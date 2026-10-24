use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use uuid::Uuid;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");
const MAX_REQUEST_ID_LEN: usize = 128;

/// A caller-supplied request id is only trusted if it is short and made of
/// harmless characters, so it can never inject text into log lines.
fn sanitize(raw: &str) -> Option<String> {
    let ok = !raw.is_empty()
        && raw.len() <= MAX_REQUEST_ID_LEN
        && raw
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'));
    ok.then(|| raw.to_string())
}

fn request_id_of(headers: &HeaderMap) -> Option<String> {
    headers
        .get(&REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(sanitize)
}

/// Gives every request an id (keeping a safe one the caller sent, otherwise
/// generating one) and echoes it on the response. `make_span` then puts the
/// id on the request's tracing span, so every log line written while handling
/// the request carries it.
pub async fn request_id_middleware(mut request: Request, next: Next) -> Response {
    let id = request_id_of(request.headers()).unwrap_or_else(|| Uuid::new_v4().to_string());
    let value = HeaderValue::from_str(&id).expect("a sanitized id is a valid header value");

    request
        .headers_mut()
        .insert(REQUEST_ID_HEADER, value.clone());
    let mut response = next.run(request).await;
    response.headers_mut().insert(REQUEST_ID_HEADER, value);
    response
}

/// Builds the tracing span for a request. Must run after
/// `request_id_middleware`, which stores the id on the request.
pub fn make_span<B>(request: &axum::http::Request<B>) -> tracing::Span {
    let request_id = request_id_of(request.headers()).unwrap_or_default();
    tracing::info_span!(
        "http",
        method = %request.method(),
        path = %request.uri().path(),
        request_id = %request_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::get;
    use axum::Router;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use tower::{Service, ServiceBuilder, ServiceExt};
    use tower_http::trace::TraceLayer;
    use tracing_subscriber::fmt::MakeWriter;

    fn app() -> Router {
        Router::new()
            .route("/ping", get(|| async { "pong" }))
            .layer(
                ServiceBuilder::new()
                    .layer(axum::middleware::from_fn(request_id_middleware))
                    .layer(TraceLayer::new_for_http().make_span_with(make_span)),
            )
    }

    async fn request_id_for(header: Option<&str>) -> String {
        let mut builder = axum::http::Request::builder().uri("/ping");
        if let Some(h) = header {
            builder = builder.header("x-request-id", h);
        }
        let response = app()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        response
            .headers()
            .get("x-request-id")
            .expect("response carries the id")
            .to_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn keeps_a_safe_id_and_generates_one_otherwise() {
        assert_eq!(
            request_id_for(Some("trace-abc.123:x")).await,
            "trace-abc.123:x"
        );

        let generated = request_id_for(None).await;
        assert!(Uuid::parse_str(&generated).is_ok());
        assert_ne!(generated, request_id_for(None).await);
    }

    #[tokio::test]
    async fn replaces_unsafe_ids() {
        for bad in ["has space", "semi;colon", "quote\"d", &"x".repeat(129)] {
            let id = request_id_for(Some(bad)).await;
            assert!(Uuid::parse_str(&id).is_ok(), "{bad:?} should be replaced");
        }
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Capture {
        type Writer = Capture;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_request_id_appears_on_log_lines() {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(capture.clone())
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let mut service = app().into_service();
        let request = axum::http::Request::builder()
            .uri("/ping")
            .header("x-request-id", "trace-me-42")
            .body(Body::empty())
            .unwrap();
        let _ = service.call(request).await.unwrap();

        let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("request_id=trace-me-42"),
            "request id missing from log output:\n{logs}"
        );
    }
}
