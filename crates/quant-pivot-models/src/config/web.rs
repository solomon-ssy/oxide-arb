//! Web server + JWT configuration (`[web]` / `[web.jwt]`).

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use zeroize::Zeroizing;

use super::secret::SecretText;

/// HTTP/WebSocket server configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebConfig {
    /// Bind address (default `0.0.0.0`).
    pub listen_host: String,
    /// Bind port (default `8080`). Also serves Prometheus `GET /metrics`.
    pub listen_port: u16,
    /// Allowed CORS origins; empty disables cross-origin requests.
    pub cors_allowed_origins: Vec<String>,
    /// Whether to serve the bundled SPA from [`Self::static_ui_dir`].
    pub serve_static_ui: bool,
    /// Directory of the built SPA assets (default `static/ui`).
    pub static_ui_dir: String,
    /// JWT signing/expiry parameters.
    pub jwt: JwtConfig,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            listen_host: default_listen_host(),
            listen_port: default_listen_port(),
            cors_allowed_origins: Vec::new(),
            serve_static_ui: false,
            static_ui_dir: default_static_ui_dir(),
            jwt: JwtConfig::default(),
        }
    }
}

impl WebConfig {
    /// Whether the HS256 signing key is exactly 256 bits after `Base64URL` decoding.
    #[must_use]
    pub fn has_jwt_signing_key(&self) -> bool {
        self.jwt.signing_key_bytes().is_ok()
    }
}

/// JWT access/refresh token configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JwtConfig {
    /// Base64URL-no-pad 32-byte HS256 signing-key source.
    pub signing_key: SecretText,
    /// Token issuer claim (default `quant-pivot`).
    pub issuer: String,
    /// Token audience claim (default `quant-pivot-web`).
    pub audience: String,
    /// Access-token lifetime in seconds (default 900 = 15m).
    pub access_ttl_secs: i64,
    /// Refresh-token lifetime in seconds (default 604800 = 7d).
    pub refresh_ttl_secs: i64,
    /// Absolute login-session lifetime in seconds (default 2592000 = 30d).
    pub absolute_session_ttl_secs: i64,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            signing_key: SecretText::default(),
            issuer: default_issuer(),
            audience: default_audience(),
            access_ttl_secs: default_access_ttl(),
            refresh_ttl_secs: default_refresh_ttl(),
            absolute_session_ttl_secs: default_absolute_session_ttl(),
        }
    }
}

impl JwtConfig {
    /// Decode the configured signing key without ever formatting its plaintext.
    pub fn signing_key_bytes(&self) -> Result<Zeroizing<[u8; 32]>, &'static str> {
        let mut decoded = Zeroizing::new([0_u8; 32]);
        let decoded_len = URL_SAFE_NO_PAD
            .decode_slice(self.signing_key.expose_secret(), decoded.as_mut())
            .map_err(|_| "must be unpadded Base64URL encoding exactly 32 bytes")?;
        if decoded_len != decoded.len() {
            return Err("must decode to exactly 32 bytes");
        }
        Ok(decoded)
    }
}

fn default_listen_host() -> String {
    "0.0.0.0".to_owned()
}

const fn default_listen_port() -> u16 {
    8080
}

fn default_static_ui_dir() -> String {
    "static/ui".to_owned()
}

fn default_issuer() -> String {
    "quant-pivot".to_owned()
}

fn default_audience() -> String {
    "quant-pivot-web".to_owned()
}

const fn default_access_ttl() -> i64 {
    900
}

const fn default_refresh_ttl() -> i64 {
    604_800
}

const fn default_absolute_session_ttl() -> i64 {
    2_592_000
}

#[cfg(test)]
mod tests {
    use super::WebConfig;

    #[test]
    fn defaults_rejects_without_key() {
        let cfg = WebConfig::default();
        assert_eq!(cfg.listen_host, "0.0.0.0");
        assert_eq!(cfg.listen_port, 8080);
        assert!(!cfg.serve_static_ui);
        assert_eq!(cfg.jwt.issuer, "quant-pivot");
        assert_eq!(cfg.jwt.audience, "quant-pivot-web");
        assert_eq!(cfg.jwt.access_ttl_secs, 900);
        assert_eq!(cfg.jwt.refresh_ttl_secs, 604_800);
        assert_eq!(cfg.jwt.absolute_session_ttl_secs, 2_592_000);
        assert!(!cfg.has_jwt_signing_key());
    }

    #[test]
    fn nested_jwt_deserializes_redacted() {
        let cfg: WebConfig = serde_json::from_str(
            r#"{ "listen_port": 9090, "jwt": { "signing_key": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "access_ttl_secs": 60 } }"#,
        )
        .expect("valid web config");
        assert_eq!(cfg.listen_port, 9090);
        assert!(cfg.has_jwt_signing_key());
        assert!(!format!("{cfg:?}").contains("AAAAAAAAAAAAAAAA"));
        assert_eq!(cfg.jwt.access_ttl_secs, 60);
        assert_eq!(cfg.jwt.refresh_ttl_secs, 604_800);
    }

    #[test]
    fn resolved_signing_key_accepted() {
        let mut cfg = WebConfig::default();
        cfg.jwt.signing_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into();
        assert!(cfg.has_jwt_signing_key());
    }

    #[test]
    fn signing_key_rejects_padding() {
        for invalid in [
            "human-password",
            "c2hvcnQ",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        ] {
            let mut cfg = WebConfig::default();
            cfg.jwt.signing_key = invalid.into();
            assert!(!cfg.has_jwt_signing_key());
        }
    }
}
