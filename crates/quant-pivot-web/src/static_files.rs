//! Production static SPA serving with client-side-routing fallback.
//!
//! Registered as the app's `default_service`, so every explicitly registered
//! route (API, health, metrics, WebSocket) is matched first and only genuinely
//! unclaimed paths reach the UI handler. Serving rules:
//!
//! - exact file hit → serve it (with long-cache headers for hashed assets);
//! - extensionless path (a SPA deep link, e.g. `/dashboard`) → `index.html`
//!   (`no-cache`) so the Vue router can resolve it client-side;
//! - a missing path that *looks like* an asset (has an extension) → `404`
//!   (never silently returns `index.html` with the wrong content type).
//!
//! Enabled by [`WebConfig::serve_static_ui`]; if the configured directory is
//! absent the server runs API-only.

use std::path::{Path, PathBuf};

use actix_files::NamedFile;
use actix_web::{
    HttpRequest, HttpResponse,
    dev::{ServiceRequest, ServiceResponse, fn_service},
    http::header::{CACHE_CONTROL, HeaderValue, X_CONTENT_TYPE_OPTIONS},
    web::ServiceConfig,
};
use quant_pivot_models::config::WebConfig;

/// Register the static SPA service if enabled and present.
pub fn configure_static(cfg: &mut ServiceConfig, config: &WebConfig) {
    if !config.serve_static_ui {
        return;
    }
    let root = PathBuf::from(&config.static_ui_dir);
    if !root.is_dir() {
        tracing::warn!(
            static_ui_dir = %config.static_ui_dir,
            "serve_static_ui is enabled but the directory is missing; running API-only"
        );
        return;
    }
    cfg.default_service(fn_service(move |req: ServiceRequest| {
        let root = root.clone();
        async move {
            let (http_req, _payload) = req.into_parts();
            let response = serve_spa(&http_req, &root).await;
            Ok(ServiceResponse::new(http_req, response))
        }
    }));
}

/// Resolve a request path to a file response with SPA fallback.
async fn serve_spa(req: &HttpRequest, root: &Path) -> HttpResponse {
    let key = normalize_req_path(req.path());

    // Reject path traversal before touching the filesystem.
    if key.contains("..") || key.contains('\\') {
        return HttpResponse::BadRequest().finish();
    }

    if let Ok(file) = NamedFile::open_async(root.join(&key)).await {
        return finalize(req, file, &key);
    }

    if should_fallback_to_index(&key) {
        if let Ok(index) = NamedFile::open_async(root.join("index.html")).await {
            return finalize(req, index, "index.html");
        }
        tracing::warn!("SPA fallback requested but index.html is missing");
    }

    HttpResponse::NotFound().finish()
}

/// Render a [`NamedFile`] with cache + security headers.
fn finalize(req: &HttpRequest, file: NamedFile, key: &str) -> HttpResponse {
    let mut response = file.into_response(req);
    let headers = response.headers_mut();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static(cache_control_for_path(key)),
    );
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

/// Strip the leading slash; the empty path maps to `index.html`.
fn normalize_req_path(path: &str) -> String {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        "index.html".to_owned()
    } else {
        trimmed.to_owned()
    }
}

/// Only extensionless paths (SPA routes) fall back to `index.html`; a missing
/// asset (with an extension) is a genuine 404.
fn should_fallback_to_index(key: &str) -> bool {
    !key.rsplit('/').next().unwrap_or(key).contains('.')
}

/// Vite hashes asset filenames, so they are immutable and cacheable for a year;
/// `index.html` must never be cached so new deploys are picked up immediately.
fn cache_control_for_path(key: &str) -> &'static str {
    if key == "index.html" {
        return "no-cache";
    }
    match key.rsplit('.').next().unwrap_or("") {
        "js" | "css" | "png" | "jpg" | "jpeg" | "svg" | "webp" | "ico" | "woff" | "woff2" => {
            "public, max-age=31536000, immutable"
        }
        _ => "public, max-age=3600",
    }
}
