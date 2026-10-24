use axum::routing::{delete, post, put};
use axum::Router;

use crate::config::RateLimits;
use crate::handlers::devices::{delete_me, heartbeat, register, update_location, ws_ticket};
use crate::middleware::rate_limit::with_limit;
use crate::AppState;

/// Device endpoints. Registration is limited per client address (anyone can
/// call it); the authenticated endpoints get their own, more generous limit.
pub fn router(limits: &RateLimits) -> Router<AppState> {
    let register_route = with_limit(
        Router::new().route("/devices/register", post(register)),
        limits.register,
        limits,
    );

    let authenticated = with_limit(
        Router::new()
            .route("/devices/location", put(update_location))
            .route("/devices/heartbeat", post(heartbeat))
            .route("/devices/ws-ticket", post(ws_ticket))
            .route("/devices/me", delete(delete_me)),
        limits.location,
        limits,
    );

    register_route.merge(authenticated)
}
