use axum::routing::post;
use axum::Router;

use crate::config::RateLimits;
use crate::extractors::operator_auth::require_operator;
use crate::handlers::alerts::dispatch_alert;
use crate::middleware::rate_limit::with_limit;
use crate::AppState;

/// `POST /alerts`: operators only (`X-API-Key`). The rate limit is the outer
/// layer on purpose, so guessing API keys is throttled too.
pub fn router(state: &AppState, limits: &RateLimits) -> Router<AppState> {
    let protected = Router::new().route("/alerts", post(dispatch_alert)).layer(
        axum::middleware::from_fn_with_state(state.operator_keys.clone(), require_operator),
    );

    with_limit(protected, limits.alerts, limits)
}
