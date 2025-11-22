use crate::{zaps::cache::PayCache, ZapError};
use enostr::NoteId;
use poll_promise::Promise;
use tokio::task::JoinError;
use url::Url;

pub struct FetchedInvoice {
    pub invoice: String,
    pub request_noteid: NoteId,
}

pub struct FetchedInvoiceResponse {
    pub invoice: Result<FetchedInvoice, ZapError>,
    pub url: Url,
    pub should_cache: bool,
}

pub type FetchingInvoice = Promise<Result<FetchedInvoiceResponse, JoinError>>;

/// Fetches an invoice from a URL using async HTTP
async fn fetch_invoice_async(url: &Url) -> Result<String, ZapError> {
    let (sender, promise) = Promise::new();

    let on_done = move |response: Result<ehttp::Response, String>| {
        let handle = response.map_err(ZapError::endpoint_error).and_then(|resp| {
            if !resp.ok {
                return Err(ZapError::endpoint_error(format!(
                    "bad http response: {}",
                    resp.status_text
                )));
            }

            String::from_utf8(resp.bytes).map_err(|e| ZapError::Serialization(e.to_string()))
        });

        sender.send(handle);
    };

    let request = ehttp::Request::get(url);
    ehttp::fetch(request, on_done);
    tokio::task::block_in_place(|| promise.block_and_take())
}

/// Fetches an invoice with caching support
pub fn fetch_invoice_promise(cache: &PayCache, url: Url) -> Result<FetchingInvoice, ZapError> {
    match cache.get_invoice(&url) {
        Some(cached_invoice) => {
            tracing::info!("Using cached invoice for {url}");
            let invoice = cached_invoice.clone();
            let url_clone = url.clone();
            Ok(Promise::spawn_async(tokio::spawn(async move {
                FetchedInvoiceResponse {
                    invoice: Ok(FetchedInvoice {
                        invoice,
                        request_noteid: NoteId::new([0; 32]),
                    }),
                    url: url_clone,
                    should_cache: false,
                }
            })))
        }
        None => {
            let url_clone = url.clone();
            Ok(Promise::spawn_async(tokio::spawn(async move {
                tracing::info!("Fetching invoice from: {url}");
                match fetch_invoice_async(&url).await {
                    Ok(invoice) => FetchedInvoiceResponse {
                        invoice: Ok(FetchedInvoice {
                            invoice,
                            request_noteid: NoteId::new([0; 32]),
                        }),
                        url: url_clone,
                        should_cache: true,
                    },
                    Err(e) => FetchedInvoiceResponse {
                        invoice: Err(e),
                        url: url_clone,
                        should_cache: false,
                    },
                }
            })))
        }
    }
}
