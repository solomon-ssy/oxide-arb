//! Unified success-response envelope.
//!
//! Every endpoint returns the same JSON shape so clients can parse one
//! contract regardless of route:
//!
//! ```json
//! { "code": 200, "message": "ok", "data": { ... } }
//! ```
//!
//! Errors share the identical envelope via [`crate::error::WebError`]'s
//! `ResponseError` implementation (`data` is `null`), eliminating the dual
//! success/error formats that plagued the ng-gateway response layer. Paginated
//! collections wrap [`quant_pivot_models::domain::Paginated`] as their `data`
//! payload, so `{code,message,data:{items,total,page,size,has_next}}` is just a
//! `WebResponse<Paginated<T>>`.

use actix_web::{HttpRequest, HttpResponse, Responder, body::BoxBody, http::StatusCode};
use serde::Serialize;

/// Canonical success envelope wrapping an arbitrary serializable payload.
#[derive(Debug, Clone, Serialize)]
pub struct WebResponse<T> {
    /// Mirror of the HTTP status code (always `200` for success envelopes).
    pub code: u16,
    /// Human-readable status message (`"ok"` for the default success case).
    pub message: String,
    /// The payload, or `null` for empty responses.
    pub data: Option<T>,
}

impl<T> WebResponse<T> {
    /// Wrap `data` in a `200 / "ok"` success envelope.
    pub fn ok(data: T) -> Self {
        Self {
            code: StatusCode::OK.as_u16(),
            message: "ok".to_owned(),
            data: Some(data),
        }
    }

    /// Build a success envelope with a custom message.
    pub fn message(message: impl Into<String>, data: T) -> Self {
        Self {
            code: StatusCode::OK.as_u16(),
            message: message.into(),
            data: Some(data),
        }
    }
}

impl<T: Serialize> Responder for WebResponse<T> {
    type Body = BoxBody;

    fn respond_to(self, _req: &HttpRequest) -> HttpResponse<Self::Body> {
        HttpResponse::Ok().json(self)
    }
}
