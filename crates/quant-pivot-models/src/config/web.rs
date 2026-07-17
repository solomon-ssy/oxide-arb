//! Web server + JWT configuration (`[web]` / `[web.jwt]`).
//!
//! Mounted at [`DeployConfig::web`](crate::config::DeployConfig). The JWT
//! signing private key is mounted from the deployment secret manager; public
//! verification keys remain in a rotation keyring so existing sessions survive
//! an intentional signer rotation.

use serde::Deserialize;

const DEFAULT_JWT_KEY_ID: &str = "local-dev-2026-01";
const DEFAULT_JWT_PRIVATE_KEY_FILE: &str = "var/secrets/jwt/ed25519-private.pem";
const DEFAULT_JWT_PUBLIC_KEY_FILE: &str = "var/secrets/jwt/ed25519-public.pem";

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
    /// Whether the Ed25519 signer and verification keyring are structurally complete.
    #[must_use]
    pub fn jwt_keyring_is_configured(&self) -> bool {
        self.jwt.keyring_is_configured()
    }
}

/// One public Ed25519 verification key retained for JWT rotation.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JwtVerificationKeyConfig {
    pub key_id: String,
    pub public_key_file: String,
}

/// JWT access/refresh token configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JwtConfig {
    /// `kid` emitted on newly signed access and refresh tokens.
    pub signing_key_id: String,
    /// PKCS#8 Ed25519 private PEM mounted by the deployment secret manager.
    pub signing_private_key_file: String,
    /// Public keys accepted during verification, including the active signer.
    pub verification_keys: Vec<JwtVerificationKeyConfig>,
    /// Token issuer claim (default `quant-pivot`).
    pub issuer: String,
    /// Access-token lifetime in seconds (default 900 = 15m).
    pub access_ttl_secs: i64,
    /// Refresh-token lifetime in seconds (default 604800 = 7d).
    pub refresh_ttl_secs: i64,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            signing_key_id: DEFAULT_JWT_KEY_ID.to_owned(),
            signing_private_key_file: DEFAULT_JWT_PRIVATE_KEY_FILE.to_owned(),
            verification_keys: vec![JwtVerificationKeyConfig {
                key_id: DEFAULT_JWT_KEY_ID.to_owned(),
                public_key_file: DEFAULT_JWT_PUBLIC_KEY_FILE.to_owned(),
            }],
            issuer: default_issuer(),
            access_ttl_secs: default_access_ttl(),
            refresh_ttl_secs: default_refresh_ttl(),
        }
    }
}

impl JwtConfig {
    #[must_use]
    pub fn keyring_is_configured(&self) -> bool {
        let signing_key_id = self.signing_key_id.trim();
        !signing_key_id.is_empty()
            && !self.signing_private_key_file.trim().is_empty()
            && self
                .verification_keys
                .iter()
                .any(|key| key.key_id == signing_key_id && !key.public_key_file.trim().is_empty())
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

const fn default_access_ttl() -> i64 {
    900
}

const fn default_refresh_ttl() -> i64 {
    604_800
}

#[cfg(test)]
mod tests {
    use super::WebConfig;

    #[test]
    fn defaults_are_sensible() {
        let cfg = WebConfig::default();
        assert_eq!(cfg.listen_host, "0.0.0.0");
        assert_eq!(cfg.listen_port, 8080);
        assert!(!cfg.serve_static_ui);
        assert_eq!(cfg.jwt.issuer, "quant-pivot");
        assert_eq!(cfg.jwt.access_ttl_secs, 900);
        assert_eq!(cfg.jwt.refresh_ttl_secs, 604_800);
    }

    #[test]
    fn empty_section_deserializes_to_defaults() {
        let cfg: WebConfig = serde_json::from_str("{}").expect("empty [web] is valid");
        assert_eq!(cfg.listen_port, 8080);
        assert!(cfg.jwt_keyring_is_configured());
    }

    #[test]
    fn nested_jwt_section_deserializes() {
        let cfg: WebConfig = serde_json::from_str(
            r#"{ "listen_port": 9090, "jwt": { "signing_key_id": "next", "signing_private_key_file": "/run/secrets/next.pem", "verification_keys": [{ "key_id": "next", "public_key_file": "/etc/quant-pivot/next.pub.pem" }], "access_ttl_secs": 60 } }"#,
        )
        .expect("valid web config");
        assert_eq!(cfg.listen_port, 9090);
        assert_eq!(cfg.jwt.signing_key_id, "next");
        assert_eq!(cfg.jwt.access_ttl_secs, 60);
        assert_eq!(cfg.jwt.refresh_ttl_secs, 604_800);
    }

    #[test]
    fn incomplete_keyring_detection() {
        let mut cfg = WebConfig::default();
        assert!(cfg.jwt_keyring_is_configured());
        cfg.jwt.signing_key_id = "missing".to_owned();
        assert!(!cfg.jwt_keyring_is_configured());
        cfg.jwt.verification_keys.clear();
        assert!(!cfg.jwt_keyring_is_configured());
    }
}
