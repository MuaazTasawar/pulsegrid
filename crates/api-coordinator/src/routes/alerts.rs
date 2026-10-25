use axum::routing::{get, post};
use axum::Router;

use crate::config::RateLimits;
use crate::extractors::operator_auth::require_operator;
use crate::handlers::alerts::{dispatch_alert, get_alert, list_alerts};
use crate::middleware::rate_limit::with_limit;
use crate::AppState;

/// Alert endpoints, operators only (`X-API-Key`). The rate limit is the
/// outer layer on purpose, so guessing API keys is throttled too. Dispatching
/// has its own tight limit; the read endpoints use the more generous one.
pub fn router(state: &AppState, limits: &RateLimits) -> Router<AppState> {
    let auth =
        || axum::middleware::from_fn_with_state(state.operator_keys.clone(), require_operator);

    let dispatch = with_limit(
        Router::new()
            .route("/alerts", post(dispatch_alert))
            .layer(auth()),
        limits.alerts,
        limits,
    );
    let reads = with_limit(
        Router::new()
            .route("/alerts", get(list_alerts))
            .route("/alerts/{id}", get(get_alert))
            .layer(auth()),
        limits.location,
        limits,
    );

    dispatch.merge(reads)
}
