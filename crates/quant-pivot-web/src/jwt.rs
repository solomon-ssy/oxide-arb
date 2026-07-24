//! JWT issuance/validation and the Redis-backed token revocation blacklist.
//!
//! Access and refresh tokens are HS256-signed with an explicit media type. Roles are
//! **intentionally not
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
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
    errors::ErrorKind,
};
use quant_pivot_error::auth::AuthError;
use quant_pivot_models::{config::JwtConfig, domain::rbac::UserInfo};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::error::WebError;

/// Sub-namespace for revoked token ids, appended to the platform key prefix
/// (`{key_prefix}jwt:blacklist:{jti}`).
const BLACKLIST_NAMESPACE: &str = "jwt:blacklist:";
const REFRESH_FAMILY_NAMESPACE: &str = "jwt:session:";
const SUBJECT_SESSION_NAMESPACE: &str = "jwt:subject-sessions:";
const WS_TICKET_NAMESPACE: &str = "ws:ticket:";

const CREATE_REFRESH_FAMILY_LUA: &str = r"
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1],
  'current_refresh_jti', ARGV[1],
  'session_exp', ARGV[2],
  'generation', ARGV[3],
  'status', 'active',
  'subject', ARGV[4])
redis.call('EXPIREAT', KEYS[1], ARGV[2])
redis.call('SADD', KEYS[2], KEYS[1])
local subject_exp = redis.call('EXPIRETIME', KEYS[2])
if subject_exp < tonumber(ARGV[2]) then
  redis.call('EXPIREAT', KEYS[2], ARGV[2])
end
return 1
";

const ROTATE_REFRESH_FAMILY_LUA: &str = r"
if redis.call('EXISTS', KEYS[1]) == 0 then return -2 end
local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'active' then return -2 end
local current = redis.call('HGET', KEYS[1], 'current_refresh_jti')
local generation = redis.call('HGET', KEYS[1], 'generation')
local session_exp = redis.call('HGET', KEYS[1], 'session_exp')
if current ~= ARGV[1] or generation ~= ARGV[2] or session_exp ~= ARGV[4] then
  redis.call('HSET', KEYS[1], 'status', 'revoked')
  redis.call('EXPIREAT', KEYS[1], session_exp)
  return -1
end
redis.call('HSET', KEYS[1],
  'current_refresh_jti', ARGV[3],
  'generation', ARGV[5])
redis.call('EXPIREAT', KEYS[1], session_exp)
return 1
";

const REVOKE_REFRESH_FAMILY_LUA: &str = r"
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local session_exp = redis.call('HGET', KEYS[1], 'session_exp')
redis.call('HSET', KEYS[1], 'status', 'revoked')
redis.call('EXPIREAT', KEYS[1], session_exp)
return 1
";

const REVOKE_SUBJECT_SESSIONS_LUA: &str = r"
local families = redis.call('SMEMBERS', KEYS[1])
for _, family_key in ipairs(families) do
  if redis.call('EXISTS', family_key) == 1 then
    local session_exp = redis.call('HGET', family_key, 'session_exp')
    redis.call('HSET', family_key, 'status', 'revoked')
    if session_exp then redis.call('EXPIREAT', family_key, session_exp) end
  end
end
redis.call('DEL', KEYS[1])
return #families
";

/// Small clock-skew tolerance (seconds) applied to `exp`/`nbf` validation.
const CLOCK_SKEW_LEEWAY_SECS: u64 = 5;

/// Which kind of token a set of claims represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenUse {
    /// Short-lived bearer token used to authenticate API requests.
    Access,
    /// Long-lived token used solely to mint a fresh access/refresh pair.
    Refresh,
}

impl TokenUse {
    /// Stable lowercase label used in claims and error messages.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Access => "access",
            Self::Refresh => "refresh",
        }
    }

    #[must_use]
    pub const fn header_type(self) -> &'static str {
        match self {
            Self::Access => "at+jwt",
            Self::Refresh => "rt+jwt",
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
    /// Audience (`aud`), validated against the web API audience.
    pub aud: String,
    /// Issued-at (Unix seconds).
    pub iat: i64,
    /// Not-before (Unix seconds).
    pub nbf: i64,
    /// Expiry (Unix seconds).
    pub exp: i64,
    /// Username snapshot for display/auditing (authoritative source is the DB).
    pub username: String,
    /// Access vs refresh discriminator, validated on decode.
    pub token_use: TokenUse,
    /// Rotation family shared by one login session's access/refresh tokens.
    pub family_id: String,
    /// Absolute expiry of the login session. Refresh rotation never extends it.
    pub session_exp: i64,
    /// Monotonic refresh generation; access and refresh siblings share it.
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshFamilyRotation {
    Rotated,
    ReplayOrStale,
    RevokedOrMissing,
}

/// Deployment-time failure while validating the HS256 JWT key.
#[derive(Debug, Error)]
pub enum JwtSigningKeyError {
    #[error("web.jwt.signing_key {0}")]
    Invalid(&'static str),
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

/// Server-side WebSocket upgrade capability stored in Redis for one use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsTicketClaims {
    pub subject: String,
    pub family_id: String,
    pub access_jti: String,
    pub session_exp: i64,
    pub authorization_revision: u64,
    pub nonce: String,
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

    async fn create_refresh_family(
        &self,
        family_id: &str,
        current_refresh_jti: &str,
        subject: &str,
        session_exp: i64,
    ) -> Result<(), AuthError>;

    async fn rotate_refresh_family(
        &self,
        family_id: &str,
        presented_jti: &str,
        presented_generation: u64,
        child_jti: &str,
        child_generation: u64,
        session_exp: i64,
    ) -> Result<RefreshFamilyRotation, AuthError>;

    async fn revoke_refresh_family(&self, family_id: &str) -> Result<(), AuthError>;

    async fn revoke_subject_sessions(&self, subject: &str) -> Result<(), AuthError>;

    async fn refresh_family_active(&self, family_id: &str) -> Result<bool, AuthError>;
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
    refresh_family_prefix: String,
    subject_session_prefix: String,
    ws_ticket_prefix: String,
}

impl RedisTokenBlacklist {
    /// Wrap the shared connection pool. `key_prefix` is the platform Redis
    /// namespace (e.g. `qp:`); revoked ids are stored under
    /// `{key_prefix}jwt:blacklist:{jti}`.
    #[must_use]
    pub fn new(pool: Pool, key_prefix: &str) -> Self {
        Self {
            pool,
            key_prefix: format!("{key_prefix}{BLACKLIST_NAMESPACE}"),
            refresh_family_prefix: format!("{key_prefix}{REFRESH_FAMILY_NAMESPACE}"),
            subject_session_prefix: format!("{key_prefix}{SUBJECT_SESSION_NAMESPACE}"),
            ws_ticket_prefix: format!("{key_prefix}{WS_TICKET_NAMESPACE}"),
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

    fn refresh_family_key(&self, family_id: &str) -> String {
        format!("{}{family_id}", self.refresh_family_prefix)
    }

    fn subject_session_key(&self, subject: &str) -> String {
        format!("{}{subject}", self.subject_session_prefix)
    }

    /// Mint a random, single-use WebSocket ticket in Redis.
    pub async fn issue_ws_ticket(
        &self,
        claims: &Claims,
        authorization_revision: u64,
        ttl_secs: u64,
    ) -> Result<String, AuthError> {
        let ticket = Uuid::now_v7().to_string();
        let key = format!("{}{ticket}", self.ws_ticket_prefix);
        let payload = serde_json::to_string(&WsTicketClaims {
            subject: claims.sub.clone(),
            family_id: claims.family_id.clone(),
            access_jti: claims.jti.clone(),
            session_exp: claims.session_exp,
            authorization_revision,
            nonce: Uuid::now_v7().to_string(),
        })
        .map_err(|_| AuthError::BlacklistUnavailable)?;
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        let stored: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(payload)
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs.max(1))
            .query_async(&mut conn)
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        if stored.is_none() {
            return Err(AuthError::BlacklistUnavailable);
        }
        Ok(ticket)
    }

    /// Atomically consume a WebSocket ticket. A second use returns `None`.
    pub async fn consume_ws_ticket(
        &self,
        ticket: &str,
    ) -> Result<Option<WsTicketClaims>, AuthError> {
        let key = format!("{}{ticket}", self.ws_ticket_prefix);
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        let payload: Option<String> = redis::cmd("GETDEL")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        payload
            .map(|value| serde_json::from_str(&value).map_err(|_| AuthError::InvalidToken))
            .transpose()
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

    async fn create_refresh_family(
        &self,
        family_id: &str,
        current_refresh_jti: &str,
        subject: &str,
        session_exp: i64,
    ) -> Result<(), AuthError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        let created: i64 = redis::cmd("EVAL")
            .arg(CREATE_REFRESH_FAMILY_LUA)
            .arg(2)
            .arg(self.refresh_family_key(family_id))
            .arg(self.subject_session_key(subject))
            .arg(current_refresh_jti)
            .arg(session_exp)
            .arg(0_u64)
            .arg(subject)
            .query_async(&mut conn)
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        if created == 1 {
            Ok(())
        } else {
            Err(AuthError::BlacklistUnavailable)
        }
    }

    async fn rotate_refresh_family(
        &self,
        family_id: &str,
        presented_jti: &str,
        presented_generation: u64,
        child_jti: &str,
        child_generation: u64,
        session_exp: i64,
    ) -> Result<RefreshFamilyRotation, AuthError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        let outcome: i64 = redis::cmd("EVAL")
            .arg(ROTATE_REFRESH_FAMILY_LUA)
            .arg(1)
            .arg(self.refresh_family_key(family_id))
            .arg(presented_jti)
            .arg(presented_generation)
            .arg(child_jti)
            .arg(session_exp)
            .arg(child_generation)
            .query_async(&mut conn)
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        Ok(match outcome {
            1 => RefreshFamilyRotation::Rotated,
            -1 => RefreshFamilyRotation::ReplayOrStale,
            _ => RefreshFamilyRotation::RevokedOrMissing,
        })
    }

    async fn revoke_refresh_family(&self, family_id: &str) -> Result<(), AuthError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        redis::cmd("EVAL")
            .arg(REVOKE_REFRESH_FAMILY_LUA)
            .arg(1)
            .arg(self.refresh_family_key(family_id))
            .query_async::<i64>(&mut conn)
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        Ok(())
    }

    async fn revoke_subject_sessions(&self, subject: &str) -> Result<(), AuthError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        redis::cmd("EVAL")
            .arg(REVOKE_SUBJECT_SESSIONS_LUA)
            .arg(1)
            .arg(self.subject_session_key(subject))
            .query_async::<i64>(&mut conn)
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        Ok(())
    }

    async fn refresh_family_active(&self, family_id: &str) -> Result<bool, AuthError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        let status: Option<String> = redis::cmd("HGET")
            .arg(self.refresh_family_key(family_id))
            .arg("status")
            .query_async(&mut conn)
            .await
            .map_err(|_| AuthError::BlacklistUnavailable)?;
        Ok(status.as_deref() == Some("active"))
    }
}

/// Stateless JWT signer/validator paired with a revocation blacklist.
pub struct JwtService {
    encoding: EncodingKey,
    decoding: DecodingKey,
    issuer: String,
    audience: String,
    access_ttl_secs: i64,
    refresh_ttl_secs: i64,
    absolute_session_ttl_secs: i64,
    blacklist: Arc<dyn TokenBlacklist>,
}

impl JwtService {
    /// Construct from the validated [`JwtConfig`] and a revocation backend.
    pub fn new(
        config: &JwtConfig,
        blacklist: Arc<dyn TokenBlacklist>,
    ) -> Result<Self, JwtSigningKeyError> {
        let key = config
            .signing_key_bytes()
            .map_err(JwtSigningKeyError::Invalid)?;
        Ok(Self {
            encoding: EncodingKey::from_secret(key.as_ref()),
            decoding: DecodingKey::from_secret(key.as_ref()),
            issuer: config.issuer.clone(),
            audience: config.audience.clone(),
            access_ttl_secs: config.access_ttl_secs,
            refresh_ttl_secs: config.refresh_ttl_secs,
            absolute_session_ttl_secs: config.absolute_session_ttl_secs,
            blacklist,
        })
    }

    /// Refresh-token lifetime used for the `HttpOnly` cookie max-age.
    #[must_use]
    pub const fn refresh_ttl_secs(&self) -> i64 {
        self.refresh_ttl_secs
    }

    /// Absolute login-session lifetime. Refresh rotation never extends it.
    #[must_use]
    pub const fn absolute_session_ttl_secs(&self) -> i64 {
        self.absolute_session_ttl_secs
    }

    /// Sign a short-lived access token for `user`.
    pub fn encode_access(&self, user: &UserInfo) -> Result<IssuedToken, WebError> {
        let session_exp = Utc::now().timestamp() + self.absolute_session_ttl_secs;
        self.issue(
            user,
            self.access_ttl_secs,
            TokenUse::Access,
            &Uuid::now_v7().to_string(),
            session_exp,
            0,
        )
    }

    /// Sign a long-lived refresh token for `user`.
    pub fn encode_refresh(&self, user: &UserInfo) -> Result<IssuedToken, WebError> {
        let session_exp = Utc::now().timestamp() + self.absolute_session_ttl_secs;
        self.issue(
            user,
            self.refresh_ttl_secs,
            TokenUse::Refresh,
            &Uuid::now_v7().to_string(),
            session_exp,
            0,
        )
    }

    pub fn encode_access_in_family(
        &self,
        user: &UserInfo,
        family_id: &str,
        session_exp: i64,
        generation: u64,
    ) -> Result<IssuedToken, WebError> {
        self.issue(
            user,
            self.access_ttl_secs,
            TokenUse::Access,
            family_id,
            session_exp,
            generation,
        )
    }

    pub fn encode_refresh_in_family(
        &self,
        user: &UserInfo,
        family_id: &str,
        session_exp: i64,
        generation: u64,
    ) -> Result<IssuedToken, WebError> {
        self.issue(
            user,
            self.refresh_ttl_secs,
            TokenUse::Refresh,
            family_id,
            session_exp,
            generation,
        )
    }

    fn issue(
        &self,
        user: &UserInfo,
        ttl_secs: i64,
        token_use: TokenUse,
        family_id: &str,
        session_exp: i64,
        generation: u64,
    ) -> Result<IssuedToken, WebError> {
        let now = Utc::now().timestamp();
        let exp = (now + ttl_secs).min(session_exp);
        if exp <= now {
            return Err(WebError::from(AuthError::ExpiredToken));
        }
        let jti = Uuid::now_v7().to_string();
        let claims = Claims {
            jti: jti.clone(),
            sub: user.id.to_string(),
            iss: self.issuer.clone(),
            aud: self.audience.clone(),
            iat: now,
            nbf: now,
            exp,
            username: user.username.clone(),
            token_use,
            family_id: family_id.to_owned(),
            session_exp,
            generation,
        };
        let mut header = Header::new(Algorithm::HS256);
        header.typ = Some(token_use.header_type().to_owned());
        let token = encode(&header, &claims, &self.encoding)
            .map_err(|error| WebError::Internal(format!("jwt encode failed: {error}")))?;
        Ok(IssuedToken { token, jti, exp })
    }

    /// Decode and validate `token`, requiring it to be of `expected` type.
    ///
    /// Returns [`AuthError::ExpiredToken`] for an expired token,
    /// [`AuthError::WrongTokenType`] on access/refresh mismatch, and
    /// [`AuthError::InvalidToken`] for any other validation failure.
    pub fn decode(&self, token: &str, expected: TokenUse) -> Result<Claims, AuthError> {
        let header = decode_header(token).map_err(|_| AuthError::InvalidToken)?;
        if header.alg != Algorithm::HS256 || header.typ.as_deref() != Some(expected.header_type()) {
            return Err(AuthError::InvalidToken);
        }
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[self.audience.as_str()]);
        validation.set_required_spec_claims(&["aud", "exp", "iss", "sub"]);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.leeway = CLOCK_SKEW_LEEWAY_SECS;

        let claims = decode::<Claims>(token, &self.decoding, &validation)
            .map_err(|error| match error.kind() {
                ErrorKind::ExpiredSignature => AuthError::ExpiredToken,
                _ => AuthError::InvalidToken,
            })?
            .claims;

        if claims.token_use != expected {
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

    pub async fn create_refresh_family(
        &self,
        family_id: &str,
        current_refresh_jti: &str,
        subject: &str,
        session_exp: i64,
    ) -> Result<(), AuthError> {
        self.blacklist
            .create_refresh_family(family_id, current_refresh_jti, subject, session_exp)
            .await
    }

    pub async fn rotate_refresh_family(
        &self,
        claims: &Claims,
        child_jti: &str,
        child_generation: u64,
    ) -> Result<RefreshFamilyRotation, AuthError> {
        self.blacklist
            .rotate_refresh_family(
                &claims.family_id,
                &claims.jti,
                claims.generation,
                child_jti,
                child_generation,
                claims.session_exp,
            )
            .await
    }

    pub async fn revoke_family(&self, family_id: &str) -> Result<(), AuthError> {
        self.blacklist.revoke_refresh_family(family_id).await
    }

    pub async fn revoke_subject_sessions(&self, subject: &str) -> Result<(), AuthError> {
        self.blacklist.revoke_subject_sessions(subject).await
    }

    pub async fn family_active(&self, family_id: &str) -> Result<bool, AuthError> {
        self.blacklist.refresh_family_active(family_id).await
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
    use std::sync::Arc;

    use chrono::Utc;
    use deadpool_redis::{Config, Runtime};
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use quant_pivot_error::auth::AuthError;
    use quant_pivot_models::{
        config::JwtConfig, domain::rbac::UserInfo, enums::rbac::UserStatus, types::UserId,
    };

    use super::{Claims, JwtService, RedisTokenBlacklist, TokenUse};

    const TEST_KEY: &[u8] = &[7; 32];

    fn config(signing_key: &str) -> JwtConfig {
        JwtConfig {
            signing_key: signing_key.into(),
            issuer: "quant-pivot-test".to_owned(),
            audience: "quant-pivot-web-test".to_owned(),
            access_ttl_secs: 900,
            refresh_ttl_secs: 604_800,
            absolute_session_ttl_secs: 2_592_000,
        }
    }

    fn service(signing_key: &str) -> JwtService {
        let pool = Config::from_url("redis://127.0.0.1:1")
            .create_pool(Some(Runtime::Tokio1))
            .expect("lazy pool");
        JwtService::new(
            &config(signing_key),
            Arc::new(RedisTokenBlacklist::new(pool, "qp:")),
        )
        .expect("valid signing key")
    }

    fn user() -> UserInfo {
        let now = Utc::now();
        UserInfo {
            id: UserId::from_v7(),
            username: "operator".to_owned(),
            password_hash: "redacted".to_owned(),
            nickname: "Operator".to_owned(),
            avatar: None,
            email: None,
            phone: None,
            status: UserStatus::Active,
            created_at: now,
            updated_at: now,
        }
    }

    fn claims(audience: &str, token_use: TokenUse) -> Claims {
        let now = Utc::now().timestamp();
        Claims {
            jti: "test-jti".to_owned(),
            sub: UserId::from_v7().to_string(),
            iss: "quant-pivot-test".to_owned(),
            aud: audience.to_owned(),
            iat: now,
            nbf: now,
            exp: now + 60,
            username: "operator".to_owned(),
            token_use,
            family_id: "family".to_owned(),
            session_exp: now + 60,
            generation: 0,
        }
    }

    fn signed(claims: &Claims, algorithm: Algorithm, media_type: &str) -> String {
        let mut header = Header::new(algorithm);
        header.typ = Some(media_type.to_owned());
        encode(&header, claims, &EncodingKey::from_secret(TEST_KEY)).expect("signed test token")
    }

    #[tokio::test]
    async fn blacklist_key_uses_namespace() {
        // The pool is lazy — no connection is attempted until `get`.
        let pool = Config::from_url("redis://127.0.0.1:1")
            .create_pool(Some(Runtime::Tokio1))
            .expect("lazy pool");
        let blacklist = RedisTokenBlacklist::new(pool, "qp:");
        assert_eq!(blacklist.key("abc-123"), "qp:jwt:blacklist:abc-123");
    }

    #[test]
    fn decode_pins_algorithm_use() {
        let jwt = service("BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc");
        let access = claims("quant-pivot-web-test", TokenUse::Access);
        let valid = signed(&access, Algorithm::HS256, "at+jwt");
        assert!(jwt.decode(&valid, TokenUse::Access).is_ok());

        let wrong_algorithm = signed(&access, Algorithm::HS384, "at+jwt");
        assert!(matches!(
            jwt.decode(&wrong_algorithm, TokenUse::Access),
            Err(AuthError::InvalidToken)
        ));
        let wrong_type = signed(&access, Algorithm::HS256, "rt+jwt");
        assert!(matches!(
            jwt.decode(&wrong_type, TokenUse::Access),
            Err(AuthError::InvalidToken)
        ));
        let wrong_audience = signed(
            &claims("another-service", TokenUse::Access),
            Algorithm::HS256,
            "at+jwt",
        );
        assert!(matches!(
            jwt.decode(&wrong_audience, TokenUse::Access),
            Err(AuthError::InvalidToken)
        ));
        let refresh_claim = signed(
            &claims("quant-pivot-web-test", TokenUse::Refresh),
            Algorithm::HS256,
            "at+jwt",
        );
        assert!(matches!(
            jwt.decode(&refresh_claim, TokenUse::Access),
            Err(AuthError::WrongTokenType { .. })
        ));
    }

    #[test]
    fn key_rotation_invalidates_access() {
        let original = service("BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc");
        let rotated = service("CAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAg");
        let token = original.encode_access(&user()).expect("access token");
        assert!(rotated.decode(&token.token, TokenUse::Access).is_err());

        let session_exp = Utc::now().timestamp() + 2;
        let clipped = original
            .encode_access_in_family(&user(), "family", session_exp, 0)
            .expect("clipped access token");
        assert_eq!(clipped.exp, session_exp);
    }
}
