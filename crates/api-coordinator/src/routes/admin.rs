use axum::routing::{delete, post};
use axum::Router;

use crate::config::RateLimits;
use crate::extractors::operator_auth::require_operator;
use crate::handlers::admin::{delete_device, revoke_device};
use crate::middleware::rate_limit::with_limit;
use crate::AppState;

/// Operator-only device administration.
pub fn router(state: &AppState, limits: &RateLimits) -> Router<AppState> {
    let routes = Router::new()
        .route("/admin/devices/{id}/revoke", post(revoke_device))
        .route("/admin/devices/{id}", delete(delete_device))
        .layer(axum::middleware::from_fn_with_state(
            state.operator_keys.clone(),
            require_operator,
        ));

    with_limit(routes, limits.location, limits)
}
