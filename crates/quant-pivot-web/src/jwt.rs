//! JWT issuance/validation and the Redis-backed token revocation blacklist.
//!
//! Access and refresh tokens are HS256-signed. Roles are **intentionally not
//! embedded** in the token: they are reloaded per request (see
//! [`crate::middleware::authn`]) so that authorization changes take effect
//! immediately, without forcing a re-login.
//!
//! Revocation is enforced through a [`TokenBlacklist`]: logout and refresh
//! rotation write the token's `jti` to Redis with a TTL equal to the token's
//! remaining lifetime. Authentication is **fail-closed** — if the blacklist
//! store cannot be reached, a request cannot be authenticated, because its
//! revocation status is unknown ([`AuthError::BlacklistUnavailable`] → HTTP
//! 503).

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use deadpool_redis::Pool;
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode, errors::ErrorKind,
};
use oxide_arb_error::auth::AuthError;
use oxide_arb_models::{config::JwtConfig, domain::UserInfo};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::WebError;

/// Sub-namespace for revoked token ids, appended to the platform key prefix
/// (`{key_prefix}jwt:blacklist:{jti}`).
const BLACKLIST_NAMESPACE: &str = "jwt:blacklist:";

/// Small clock-skew tolerance (seconds) applied to `exp`/`nbf` validation.
const CLOCK_SKEW_LEEWAY_SECS: u64 = 5;

/// Which kind of token a set of claims represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenType {
    /// Short-lived bearer token used to authenticate API requests.
    Access,
    /// Long-lived token used solely to mint a fresh access/refresh pair.
    Refresh,
}

impl TokenType {
    /// Stable lowercase label used in claims and error messages.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Access => "access",
            Self::Refresh => "refresh",
        }
    }
}

/// Registered + custom JWT claims.
///
/// Roles are deliberately absent — they are resolved per request from the
/// `user_role` store keyed by [`Claims::sub`], the stable user id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Unique token id; the revocation blacklist key.
    pub jti: String,
    /// Subject — the stable `UserId` (also the Casbin subject).
    pub sub: String,
    /// Issuer (`iss`), validated against the configured value.
    pub iss: String,
    /// Issued-at (Unix seconds).
    pub iat: i64,
    /// Not-before (Unix seconds).
    pub nbf: i64,
    /// Expiry (Unix seconds).
    pub exp: i64,
    /// Username snapshot for display/auditing (authoritative source is the DB).
    pub username: String,
    /// Access vs refresh discriminator, validated on decode.
    pub token_type: TokenType,
}

/// A freshly signed token plus the metadata callers need for revocation.
#[derive(Debug, Clone)]
pub struct IssuedToken {
    /// The encoded, signed JWT string.
    pub token: String,
    /// The token's unique id (`jti`).
    pub jti: String,
    /// The token's expiry (Unix seconds).
    pub exp: i64,
}

/// Revocation store for token ids.
///
/// Implementations must treat backend unavailability as
/// [`AuthError::BlacklistUnavailable`] so callers can fail closed.
#[async_trait]
pub trait TokenBlacklist: Send + Sync {
    /// Mark `jti` revoked for `ttl`. A `ttl` shorter than one second is clamped
    /// up to one second so the underlying `SET EX` stays valid.
    async fn revoke(&self, jti: &str, ttl: Duration) -> Result<(), AuthError>;

    /// Whether `jti` is currently revoked.
    async fn is_revoked(&self, jti: &str) -> Result<bool, AuthError>;

    /// Verify the revocation store is reachable (readiness probe).
    async fn health_check(&self) -> Result<(), AuthError>;
}

/// Redis implementation of [`TokenBlacklist`] over the **shared** platform
/// `deadpool` pool.
///
/// The pool is established once at the composition root and shared with the
/// cache L2 backend; only the key namespace separates the two. Revocation
/// state lives exclusively in Redis (never an in-process cache tier) so a
/// revoked `jti` is visible immediately and the fail-closed contract holds.
pub struct RedisTokenBlacklist {
    pool: Pool,
    key_prefix: String,
}

impl RedisTokenBlacklist {
    /// Wrap the shared connection pool. `key_prefix` is the platform Redis
    /// namespace (e.g. `oarb:`); revoked ids are stored under
    /// `{key_prefix}jwt:blacklist:{jti}`.
    #[must_use]
    pub fn new(pool: Pool, key_prefix: &str) -> Self {
        Self {
            pool,
            key_prefix: format!("{key_prefix}{BLACKLIST_NAMESPACE}"),
        }
    }

    /// Ping Redis to verify the revocation store is reachable.
    async fn ping(&self) -> Result<(), AuthError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        redis::cmd("PING")
            .query_async::<()>(&mut conn)
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        Ok(())
    }

    fn key(&self, jti: &str) -> String {
        format!("{}{jti}", self.key_prefix)
    }
}

#[async_trait]
impl TokenBlacklist for RedisTokenBlacklist {
    async fn revoke(&self, jti: &str, ttl: Duration) -> Result<(), AuthError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        let secs = ttl.as_secs().max(1);
        conn.set_ex::<_, _, ()>(self.key(jti), 1u8, secs)
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        Ok(())
    }

    async fn is_revoked(&self, jti: &str) -> Result<bool, AuthError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        let revoked: bool = conn
            .exists(self.key(jti))
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        Ok(revoked)
    }

    async fn health_check(&self) -> Result<(), AuthError> {
        self.ping().await
    }
}

/// Stateless JWT signer/validator paired with a revocation blacklist.
pub struct JwtService {
    encoding: EncodingKey,
    decoding: DecodingKey,
    issuer: String,
    access_ttl_secs: i64,
    refresh_ttl_secs: i64,
    blacklist: Arc<dyn TokenBlacklist>,
}

impl JwtService {
    /// Construct from the validated [`JwtConfig`] and a revocation backend.
    #[must_use]
    pub fn new(config: &JwtConfig, blacklist: Arc<dyn TokenBlacklist>) -> Self {
        Self {
            encoding: EncodingKey::from_secret(config.secret.as_bytes()),
            decoding: DecodingKey::from_secret(config.secret.as_bytes()),
            issuer: config.issuer.clone(),
            access_ttl_secs: config.access_ttl_secs,
            refresh_ttl_secs: config.refresh_ttl_secs,
            blacklist,
        }
    }

    /// Access-token lifetime in seconds (the `expires_in` of the login reply).
    #[must_use]
    pub const fn access_ttl_secs(&self) -> i64 {
        self.access_ttl_secs
    }

    /// Sign a short-lived access token for `user`.
    pub fn encode_access(&self, user: &UserInfo) -> Result<IssuedToken, WebError> {
        self.issue(user, self.access_ttl_secs, TokenType::Access)
    }

    /// Sign a long-lived refresh token for `user`.
    pub fn encode_refresh(&self, user: &UserInfo) -> Result<IssuedToken, WebError> {
        self.issue(user, self.refresh_ttl_secs, TokenType::Refresh)
    }

    fn issue(
        &self,
        user: &UserInfo,
        ttl_secs: i64,
        token_type: TokenType,
    ) -> Result<IssuedToken, WebError> {
        let now = Utc::now().timestamp();
        let exp = now + ttl_secs;
        let jti = Uuid::now_v7().to_string();
        let claims = Claims {
            jti: jti.clone(),
            sub: user.id.to_string(),
            iss: self.issuer.clone(),
            iat: now,
            nbf: now,
            exp,
            username: user.username.clone(),
            token_type,
        };
        let token = encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)
            .map_err(|error| WebError::Internal(format!("jwt encode failed: {error}")))?;
        Ok(IssuedToken { token, jti, exp })
    }

    /// Decode and validate `token`, requiring it to be of `expected` type.
    ///
    /// Returns [`AuthError::ExpiredToken`] for an expired token,
    /// [`AuthError::WrongTokenType`] on access/refresh mismatch, and
    /// [`AuthError::InvalidToken`] for any other validation failure.
    pub fn decode(&self, token: &str, expected: TokenType) -> Result<Claims, AuthError> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_required_spec_claims(&["exp", "iss", "sub"]);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.validate_aud = false;
        validation.leeway = CLOCK_SKEW_LEEWAY_SECS;

        let claims = decode::<Claims>(token, &self.decoding, &validation)
            .map_err(|error| match error.kind() {
                ErrorKind::ExpiredSignature => AuthError::ExpiredToken,
                _ => AuthError::InvalidToken,
            })?
            .claims;

        if claims.token_type != expected {
            return Err(AuthError::WrongTokenType {
                expected: expected.as_str(),
            });
        }
        Ok(claims)
    }

    /// Revoke a token by writing its `jti` to the blacklist for the token's
    /// remaining lifetime. Already-expired tokens are a no-op.
    pub async fn revoke(&self, claims: &Claims) -> Result<(), AuthError> {
        let ttl = remaining_ttl(claims.exp);
        if ttl.is_zero() {
            return Ok(());
        }
        self.blacklist.revoke(&claims.jti, ttl).await
    }

    /// Whether the given `jti` has been revoked. Backend outages surface as
    /// [`AuthError::BlacklistUnavailable`] (fail-closed).
    pub async fn is_revoked(&self, jti: &str) -> Result<bool, AuthError> {
        self.blacklist.is_revoked(jti).await
    }
}

/// Remaining lifetime of a token given its `exp` (Unix seconds), floored at
/// zero.
fn remaining_ttl(exp: i64) -> Duration {
    let now = Utc::now().timestamp();
    let secs = (exp - now).max(0);
    Duration::from_secs(secs.unsigned_abs())
}

#[cfg(test)]
mod tests {
    use super::RedisTokenBlacklist;
    use deadpool_redis::{Config, Runtime};

    #[tokio::test]
    async fn blacklist_key_derivation_matches_legacy_default_prefix() {
        // The pool is lazy — no connection is attempted until `get()`.
        let pool = Config::from_url("redis://127.0.0.1:1")
            .create_pool(Some(Runtime::Tokio1))
            .expect("lazy pool");
        let blacklist = RedisTokenBlacklist::new(pool, "oarb:");
        // Byte-identical to the previously hardcoded `oarb:jwt:blacklist:`.
        assert_eq!(blacklist.key("abc-123"), "oarb:jwt:blacklist:abc-123");
    }
}
