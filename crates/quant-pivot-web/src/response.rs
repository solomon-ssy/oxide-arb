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
///
/// `code` mirrors the HTTP status the [`Responder`] emits, so a `202 Accepted`
/// body carries `code: 202` and the response status is `202` — the two never
/// drift.
#[derive(Debug, Clone, Serialize)]
pub struct WebResponse<T> {
    /// Mirror of the HTTP status code the response is sent with.
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

    /// Wrap `data` in a `202 / "accepted"` envelope for async-enqueue endpoints.
    pub fn accepted(data: T) -> Self {
        Self {
            code: StatusCode::ACCEPTED.as_u16(),
            message: "accepted".to_owned(),
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
        let status = StatusCode::from_u16(self.code).unwrap_or(StatusCode::OK);
        HttpResponse::build(status).json(self)
    }
}
