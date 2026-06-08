//! Request extractors shared across handlers.
//!
//! - [`ValidatedJson`] — JSON body deserialization followed by
//!   `validator::Validate`, surfacing failures as `400`.
//! - [`Pagination`] — flat `?page=&size=` query window, normalized (size capped
//!   at the shared [`PageRequest`] maximum).
//! - [`AuthedActor`] — the authenticated [`Claims`] plus the actor's
//!   [`ActorRoles`], injected by the authn middleware; absence is `401`.
//! - [`RequestId`] — the correlation id injected by the request-id middleware.

use std::{
    future::{Future, Ready, ready},
    pin::Pin,
    sync::Arc,
};

use actix_web::{
    Error as ActixError, FromRequest, HttpMessage, HttpRequest,
    dev::Payload,
    web::{Json, Query},
};
use oxide_arb_error::auth::AuthError;
use oxide_arb_models::{
    domain::{PageRequest, RoleInfo},
    types::RoleId,
};
use serde::de::DeserializeOwned;
use validator::Validate;

use crate::{error::WebError, jwt::Claims};

/// A JSON body that has been deserialized **and** passed `validator::Validate`.
pub struct ValidatedJson<T>(pub T);

impl<T> ValidatedJson<T> {
    /// Consume the wrapper and return the validated value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromRequest for ValidatedJson<T>
where
    T: DeserializeOwned + Validate + 'static,
{
    type Error = ActixError;
    type Future = Pin<Box<dyn Future<Output = Result<Self, ActixError>>>>;

    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let json = Json::<T>::from_request(req, payload);
        Box::pin(async move {
            let value = json
                .await
                .map_err(|error| WebError::BadRequest(format!("invalid request body: {error}")))?
                .into_inner();
            value
                .validate()
                .map_err(|error| WebError::BadRequest(error.to_string()))?;
            Ok(Self(value))
        })
    }
}

/// A normalized pagination window parsed from the query string.
pub struct Pagination(pub PageRequest);

impl Pagination {
    /// Borrow the normalized [`PageRequest`].
    #[must_use]
    pub const fn page_request(&self) -> &PageRequest {
        &self.0
    }

    /// Consume and return the normalized [`PageRequest`].
    #[must_use]
    pub const fn into_inner(self) -> PageRequest {
        self.0
    }
}

impl FromRequest for Pagination {
    type Error = ActixError;
    type Future = Ready<Result<Self, ActixError>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        match Query::<PageRequest>::from_query(req.query_string()) {
            Ok(query) => ready(Ok(Self(query.into_inner().normalized()))),
            Err(error) => ready(Err(WebError::BadRequest(format!(
                "invalid pagination: {error}"
            ))
            .into())),
        }
    }
}

/// The set of roles bound to the authenticated actor.
///
/// Carries the full [`RoleInfo`] records (not just codes) so a single per-
/// request load serves both the `super_admin` bypass (by code) and menu
/// accessibility (`/me`, by id) without a second database round-trip.
#[derive(Debug, Clone)]
pub struct ActorRoles(Arc<[RoleInfo]>);

impl ActorRoles {
    /// Build from the roles loaded for the current user.
    #[must_use]
    pub fn new(roles: Vec<RoleInfo>) -> Self {
        Self(Arc::from(roles))
    }

    /// Borrow the underlying role records.
    #[must_use]
    pub fn as_slice(&self) -> &[RoleInfo] {
        &self.0
    }

    /// Collect the role ids (used for menu accessibility queries).
    #[must_use]
    pub fn ids(&self) -> Vec<RoleId> {
        self.0.iter().map(|role| role.id.clone()).collect()
    }

    /// Whether the actor holds the role with the given code.
    #[must_use]
    pub fn contains(&self, code: &str) -> bool {
        self.0.iter().any(|role| role.code == code)
    }
}

/// The authenticated actor: validated access-token claims plus loaded roles.
pub struct AuthedActor {
    /// The decoded access-token claims (`sub` is the stable user id).
    pub claims: Claims,
    /// The roles currently bound to the actor.
    pub roles: ActorRoles,
}

impl FromRequest for AuthedActor {
    type Error = ActixError;
    type Future = Ready<Result<Self, ActixError>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        let extensions = req.extensions();
        match (
            extensions.get::<Claims>().cloned(),
            extensions.get::<ActorRoles>().cloned(),
        ) {
            (Some(claims), Some(roles)) => ready(Ok(Self { claims, roles })),
            _ => ready(Err(WebError::from(AuthError::MissingToken).into())),
        }
    }
}

/// The per-request correlation id (from `X-Request-Id` or a generated UUID v7).
#[derive(Debug, Clone)]
pub struct RequestId(pub String);

impl RequestId {
    /// Borrow the id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromRequest for RequestId {
    type Error = ActixError;
    type Future = Ready<Result<Self, ActixError>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        req.extensions().get::<Self>().cloned().map_or_else(
            || {
                ready(Err(
                    WebError::Internal("request id missing".to_owned()).into()
                ))
            },
            |id| ready(Ok(id)),
        )
    }
}
