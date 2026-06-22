//! Request-id correlation middleware.
//!
//! Adopts an inbound `X-Request-Id` (when present and non-empty) or generates a
//! fresh UUID v7, injects it into request extensions (for handlers and the
//! tracing span), and echoes it on the response.
//!
//! Trust boundary: a client-supplied `X-Request-Id` is used **only** for log
//! correlation. It never participates in any security or authorization
//! decision.

use actix_web::{
    Error, HttpMessage,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    http::header::{HeaderName, HeaderValue},
    middleware::Next,
};
use uuid::Uuid;

use crate::extractors::RequestId;

/// Canonical correlation header (lowercase for `HeaderName::from_static`).
const REQUEST_ID_HEADER: &str = "x-request-id";

/// Inject/propagate a per-request correlation id.
pub async fn request_id<B: MessageBody>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, Error> {
    let inbound = req
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let id = inbound.unwrap_or_else(|| Uuid::now_v7().to_string());

    req.extensions_mut().insert(RequestId(id.clone()));

    let mut res = next.call(req).await?;
    if let Ok(value) = HeaderValue::from_str(&id) {
        res.headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
    }
    Ok(res)
}
