//! HTTP request tracing with secret-safe targets and actionable error events.

use actix_web::{
    Error, HttpMessage,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    http::Version,
};
use tracing::Span;
use tracing_actix_web::{DefaultRootSpanBuilder, RequestId, RootSpanBuilder};

/// Root span builder that excludes query strings from `http.target`.
///
/// Query strings may contain operator-supplied filters or third-party callback
/// material, so the tracing target is always the stable path only.
pub struct HttpRootSpanBuilder;

impl RootSpanBuilder for HttpRootSpanBuilder {
    fn on_request_start(request: &ServiceRequest) -> Span {
        let user_agent = request
            .headers()
            .get("User-Agent")
            .and_then(|header| header.to_str().ok())
            .unwrap_or("");
        let http_route = request
            .match_pattern()
            .unwrap_or_else(|| "default".to_owned());
        let http_method = request.method().as_str();
        let request_id = request
            .extensions()
            .get::<RequestId>()
            .map(ToString::to_string)
            .unwrap_or_default();
        let connection_info = request.connection_info();
        let span = tracing::info_span!(
            "HTTP request",
            http.method = %http_method,
            http.route = %http_route,
            http.flavor = http_flavor(request.version()),
            http.scheme = %connection_info.scheme(),
            http.host = %connection_info.host(),
            http.client_ip = %connection_info.realip_remote_addr().unwrap_or(""),
            http.user_agent = %user_agent,
            http.target = %request_target(request),
            http.status_code = tracing::field::Empty,
            otel.name = %format!("{http_method} {http_route}"),
            otel.kind = "server",
            otel.status_code = tracing::field::Empty,
            trace_id = tracing::field::Empty,
            request_id = %request_id,
            exception.message = tracing::field::Empty,
            exception.details = tracing::field::Empty,
        );
        drop(connection_info);
        span
    }

    fn on_request_end<B: MessageBody>(span: Span, outcome: &Result<ServiceResponse<B>, Error>) {
        DefaultRootSpanBuilder::on_request_end(span.clone(), outcome);

        match outcome {
            Ok(response) if response.status().is_server_error() => {
                if let Some(error) = response.response().error() {
                    tracing::error!(parent: &span, error = ?error, "HTTP request failed");
                } else {
                    tracing::error!(
                        parent: &span,
                        status = response.status().as_u16(),
                        "HTTP request failed"
                    );
                }
            }
            Err(error) if error.as_response_error().status_code().is_server_error() => {
                tracing::error!(parent: &span, error = ?error, "HTTP request failed");
            }
            _ => {}
        }
    }
}

fn request_target(request: &ServiceRequest) -> &str {
    request.path()
}

const fn http_flavor(version: Version) -> &'static str {
    match version {
        Version::HTTP_09 => "0.9",
        Version::HTTP_10 => "1.0",
        Version::HTTP_11 => "1.1",
        Version::HTTP_2 => "2.0",
        Version::HTTP_3 => "3.0",
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use actix_web::test::TestRequest;

    use super::request_target;

    #[test]
    fn request_path_excludes_query_values() {
        let request = TestRequest::get()
            .uri("/api/markets?search=operator-input")
            .to_srv_request();

        assert_eq!(request_target(&request), "/api/markets");
        assert!(!request_target(&request).contains("operator-input"));
    }
}
