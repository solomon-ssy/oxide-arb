//! Browser-origin checks shared by cookie mutations and WebSocket upgrades.

use actix_web::{HttpRequest, http::header::ORIGIN};
use quant_pivot_models::config::DeployConfig;

use crate::error::WebError;

pub fn ensure_allowed_origin(request: &HttpRequest, deploy: &DeployConfig) -> Result<(), WebError> {
    let origin = request
        .headers()
        .get(ORIGIN)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| WebError::Unauthorized("missing browser origin".to_owned()))?;
    if deploy
        .web
        .cors_allowed_origins
        .iter()
        .any(|allowed| allowed == origin)
    {
        return Ok(());
    }
    let connection = request.connection_info();
    let same_origin = format!("{}://{}", connection.scheme(), connection.host());
    if origin == same_origin {
        Ok(())
    } else {
        Err(WebError::Unauthorized(
            "browser origin is not allowed".to_owned(),
        ))
    }
}

pub fn ensure_same_origin_mutation(
    request: &HttpRequest,
    deploy: &DeployConfig,
) -> Result<(), WebError> {
    ensure_allowed_origin(request, deploy)?;
    if let Some(fetch_site) = request
        .headers()
        .get("sec-fetch-site")
        .and_then(|value| value.to_str().ok())
        && fetch_site != "same-origin"
    {
        return Err(WebError::Unauthorized(
            "cross-site credential mutation is not allowed".to_owned(),
        ));
    }
    Ok(())
}
