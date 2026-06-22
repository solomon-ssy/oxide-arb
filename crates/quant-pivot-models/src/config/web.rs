//! Web server + JWT configuration (`[web]` / `[web.jwt]`).
//!
//! Mounted at [`DeployConfig::web`](crate::config::DeployConfig). The JWT
//! secret must be provided via environment (`OXIDE_ARB__WEB__JWT__SECRET`) in
//! production; an empty or placeholder secret is fatal in `Live` mode (see
//! `config::validation`).

use serde::Deserialize;

/// Known-insecure placeholder secret shipped in the example TOML. Live mode
/// rejects this value so a real secret must be supplied via env.
pub const JWT_SECRET_PLACEHOLDER: &str = "change-me-in-production";

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
    /// Whether the configured JWT secret is unusable for production (empty,
    /// whitespace, or the shipped placeholder).
    #[must_use]
    pub fn jwt_secret_is_weak(&self) -> bool {
        let secret = self.jwt.secret.trim();
        secret.is_empty() || secret == JWT_SECRET_PLACEHOLDER
    }
}

/// JWT access/refresh token configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct JwtConfig {
    /// HMAC signing secret. Env-only in production: `OXIDE_ARB__WEB__JWT__SECRET`.
    pub secret: String,
    /// Token issuer claim (default `oxide-arb`).
    pub issuer: String,
    /// Access-token lifetime in seconds (default 900 = 15m).
    pub access_ttl_secs: i64,
    /// Refresh-token lifetime in seconds (default 604800 = 7d).
    pub refresh_ttl_secs: i64,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            secret: String::new(),
            issuer: default_issuer(),
            access_ttl_secs: default_access_ttl(),
            refresh_ttl_secs: default_refresh_ttl(),
        }
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
    "oxide-arb".to_owned()
}

const fn default_access_ttl() -> i64 {
    900
}

const fn default_refresh_ttl() -> i64 {
    604_800
}

#[cfg(test)]
mod tests {
    use super::{JWT_SECRET_PLACEHOLDER, WebConfig};

    #[test]
    fn defaults_are_sensible() {
        let cfg = WebConfig::default();
        assert_eq!(cfg.listen_host, "0.0.0.0");
        assert_eq!(cfg.listen_port, 8080);
        assert!(!cfg.serve_static_ui);
        assert_eq!(cfg.jwt.issuer, "oxide-arb");
        assert_eq!(cfg.jwt.access_ttl_secs, 900);
        assert_eq!(cfg.jwt.refresh_ttl_secs, 604_800);
    }

    #[test]
    fn empty_section_deserializes_to_defaults() {
        let cfg: WebConfig = serde_json::from_str("{}").expect("empty [web] is valid");
        assert_eq!(cfg.listen_port, 8080);
        assert!(cfg.jwt.secret.is_empty());
    }

    #[test]
    fn nested_jwt_section_deserializes() {
        let cfg: WebConfig = serde_json::from_str(
            r#"{ "listen_port": 9090, "jwt": { "secret": "s3cret", "access_ttl_secs": 60 } }"#,
        )
        .expect("valid web config");
        assert_eq!(cfg.listen_port, 9090);
        assert_eq!(cfg.jwt.secret, "s3cret");
        assert_eq!(cfg.jwt.access_ttl_secs, 60);
        assert_eq!(cfg.jwt.refresh_ttl_secs, 604_800);
    }

    #[test]
    fn weak_secret_detection() {
        let mut cfg = WebConfig::default();
        assert!(cfg.jwt_secret_is_weak(), "empty secret is weak");
        cfg.jwt.secret = JWT_SECRET_PLACEHOLDER.to_owned();
        assert!(cfg.jwt_secret_is_weak(), "placeholder secret is weak");
        cfg.jwt.secret = "  ".to_owned();
        assert!(cfg.jwt_secret_is_weak(), "whitespace secret is weak");
        cfg.jwt.secret = "a-real-strong-secret".to_owned();
        assert!(!cfg.jwt_secret_is_weak());
    }
}
