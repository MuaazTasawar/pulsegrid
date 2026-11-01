use axum::routing::get;
use axum::Router;

use crate::config::RateLimits;
use crate::extractors::operator_auth::require_operator;
use crate::handlers::dashboard::{app_css, app_js, devices, page, stream};
use crate::middleware::rate_limit::with_limit;
use crate::AppState;

/// The live dashboard. The page and its two static files hold no secrets and
/// are open; the delivery stream needs the operator key (`X-API-Key`), which
/// the page sends with `fetch` (the browser's `EventSource` cannot set headers).
pub fn router(state: &AppState, limits: &RateLimits) -> Router<AppState> {
    let public = Router::new()
        .route("/dashboard", get(page))
        .route("/dashboard/app.js", get(app_js))
        .route("/dashboard/app.css", get(app_css));

    let private = Router::new()
        .route("/dashboard/stream", get(stream))
        .route("/dashboard/devices", get(devices))
        .layer(axum::middleware::from_fn_with_state(
            state.operator_keys.clone(),
            require_operator,
        ));

    with_limit(public.merge(private), limits.location, limits)
}
