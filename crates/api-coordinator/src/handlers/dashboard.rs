use std::convert::Infallible;
use std::time::Duration;

use axum::extract::State;
use axum::http::header::{
    CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, REFERRER_POLICY, X_CONTENT_TYPE_OPTIONS,
};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use futures_util::{Stream, StreamExt};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;

use crate::AppState;

const INDEX_HTML: &str = include_str!("../../dashboard/index.html");
const APP_JS: &str = include_str!("../../dashboard/app.js");
const APP_CSS: &str = include_str!("../../dashboard/app.css");

/// The page handles an operator key, so scripts may only come from this
/// server and from the one pinned Leaflet release on cdnjs; the page may only
/// talk to this server. Styles allow inline because Leaflet positions its
/// elements that way.
const CSP: &str = "default-src 'none'; \
script-src 'self' https://cdnjs.cloudflare.com; \
style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; \
img-src 'self' data: https://tile.openstreetmap.org https://cdnjs.cloudflare.com; \
connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

/// `GET /dashboard`: the page itself. It contains no secrets; the operator
/// types the API key into it.
pub async fn page() -> impl IntoResponse {
    (
        [
            (CONTENT_TYPE, "text/html; charset=utf-8"),
            (CONTENT_SECURITY_POLICY, CSP),
            (X_CONTENT_TYPE_OPTIONS, "nosniff"),
            // OpenStreetMap's tile servers want a Referer that names the site.
            (REFERRER_POLICY, "strict-origin-when-cross-origin"),
            (CACHE_CONTROL, "no-store"),
        ],
        INDEX_HTML,
    )
}

/// `GET /dashboard/app.js`
pub async fn app_js() -> impl IntoResponse {
    (
        [
            (CONTENT_TYPE, "text/javascript; charset=utf-8"),
            (X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (CACHE_CONTROL, "no-cache"),
        ],
        APP_JS,
    )
}

/// `GET /dashboard/app.css`
pub async fn app_css() -> impl IntoResponse {
    (
        [
            (CONTENT_TYPE, "text/css; charset=utf-8"),
            (X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (CACHE_CONTROL, "no-cache"),
        ],
        APP_CSS,
    )
}

/// `GET /dashboard/stream` (operators): a Server-Sent Events stream with one
/// `delivery` event per shard per alert, as reported by the workers. A client
/// that falls behind gets a `lagged` event carrying how many it missed.
pub async fn stream(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let events = BroadcastStream::new(state.telemetry.subscribe()).filter_map(|item| {
        let event = match item {
            Ok(telemetry) => Event::default()
                .event("delivery")
                .json_data(&telemetry)
                .ok(),
            Err(BroadcastStreamRecvError::Lagged(missed)) => {
                Some(Event::default().event("lagged").data(missed.to_string()))
            }
        };
        futures_util::future::ready(event.map(Ok::<Event, Infallible>))
    });

    Sse::new(events).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_page_is_html_with_a_strict_content_security_policy() {
        let response = page().await.into_response();
        let headers = response.headers();
        assert_eq!(headers[CONTENT_TYPE], "text/html; charset=utf-8");
        let csp = headers[CONTENT_SECURITY_POLICY].to_str().unwrap();
        assert!(csp.contains("script-src 'self' https://cdnjs.cloudflare.com;"));
        assert!(!csp.contains("script-src 'self' 'unsafe-inline'"));
        assert!(csp.contains("connect-src 'self'"));
        assert!(csp.contains("frame-ancestors 'none'"));
    }

    #[test]
    fn the_embedded_assets_are_present_and_consistent() {
        assert!(INDEX_HTML.contains("/dashboard/app.js"));
        assert!(INDEX_HTML.contains("/dashboard/app.css"));
        assert!(APP_JS.contains("/dashboard/stream"));
        assert!(!APP_CSS.is_empty());
        // The only scripts allowed are our own file and the pinned Leaflet.
        assert!(!INDEX_HTML.contains("<script>"));
    }
}
