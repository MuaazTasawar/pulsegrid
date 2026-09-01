use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use uuid::Uuid;

const REQUEST_ID_HEADER: &str = "x-request-id";

/// Stamps every request with a request ID (generating one if the caller
/// didn't send one), and attaches it to the tracing span for that
/// request so log lines across the coordinator's async tasks can be
/// correlated back to a single inbound HTTP call.
pub async fn request_id_middleware(mut request: Request, next: Next) -> Response {
    let request_id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    tracing::Span::current().record("request_id", &request_id);
    request
        .headers_mut()
        .insert(REQUEST_ID_HEADER, request_id.parse().unwrap());

    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(REQUEST_ID_HEADER, request_id.parse().unwrap());
    response
}