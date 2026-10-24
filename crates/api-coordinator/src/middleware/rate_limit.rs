use std::time::Duration;

use axum::Router;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::key_extractor::SmartIpKeyExtractor;
use tower_governor::GovernorLayer;

use crate::config::{Limit, RateLimits};

/// Milliseconds between two refilled tokens for a "N per second" limit.
///
/// NOTE: tower_governor's `per_second(n)` means "one token every n SECONDS",
/// not "n tokens per second". The earlier code used `per_second(5)` and so
/// allowed one registration every five seconds instead of five per second.
/// Always build limits through this function.
pub fn refill_period_ms(limit: Limit) -> u64 {
    (1000 / u64::from(limit.per_sec.max(1))).max(1)
}

/// Wraps `route` in a per-client rate limit of `limit.per_sec` requests per
/// second with a burst of `limit.burst`, answering 429 beyond that. Does
/// nothing when limits are disabled. Clients are told apart by the socket
/// peer address, or by `X-Forwarded-For` when `trust_proxy_headers` is set.
///
/// Must be called inside a Tokio runtime (it starts a small task that
/// forgets idle clients).
pub fn with_limit<S>(route: Router<S>, limit: Limit, limits: &RateLimits) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    if limits.disabled {
        return route;
    }

    if limits.trust_proxy_headers {
        let mut builder = GovernorConfigBuilder::default().key_extractor(SmartIpKeyExtractor);
        builder
            .per_millisecond(refill_period_ms(limit))
            .burst_size(limit.burst);
        let config = builder.finish().expect("valid rate limit configuration");
        let limiter = config.limiter().clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                limiter.retain_recent();
            }
        });
        route.layer(GovernorLayer::new(config))
    } else {
        let mut builder = GovernorConfigBuilder::default();
        builder
            .per_millisecond(refill_period_ms(limit))
            .burst_size(limit.burst);
        let config = builder.finish().expect("valid rate limit configuration");
        let limiter = config.limiter().clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                limiter.retain_recent();
            }
        });
        route.layer(GovernorLayer::new(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use std::net::SocketAddr;
    use tower::ServiceExt;

    fn limits(per_sec: u32, burst: u32) -> (Limit, RateLimits) {
        let limit = Limit { per_sec, burst };
        (
            limit,
            RateLimits {
                register: limit,
                location: limit,
                alerts: limit,
                disabled: false,
                trust_proxy_headers: false,
            },
        )
    }

    fn app(limit: Limit, limits: &RateLimits) -> Router {
        with_limit(
            Router::new().route("/x", get(|| async { "ok" })),
            limit,
            limits,
        )
    }

    async fn hit(app: &Router, peer: &str, forwarded_for: Option<&str>) -> StatusCode {
        let addr: SocketAddr = format!("{peer}:4000").parse().unwrap();
        let mut builder = Request::builder().uri("/x").extension(ConnectInfo(addr));
        if let Some(xff) = forwarded_for {
            builder = builder.header("x-forwarded-for", xff);
        }
        app.clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[test]
    fn refill_period_is_per_request_not_per_second_count() {
        assert_eq!(
            refill_period_ms(Limit {
                per_sec: 5,
                burst: 1
            }),
            200
        );
        assert_eq!(
            refill_period_ms(Limit {
                per_sec: 2,
                burst: 1
            }),
            500
        );
        assert_eq!(
            refill_period_ms(Limit {
                per_sec: 1,
                burst: 1
            }),
            1000
        );
        assert_eq!(
            refill_period_ms(Limit {
                per_sec: 1000,
                burst: 1
            }),
            1
        );
    }

    #[tokio::test]
    async fn allows_a_burst_then_refills_five_per_second() {
        let (limit, all) = limits(5, 10);
        let app = app(limit, &all);

        let mut ok = 0;
        let mut limited = 0;
        for _ in 0..12 {
            match hit(&app, "10.0.0.1", None).await {
                StatusCode::OK => ok += 1,
                StatusCode::TOO_MANY_REQUESTS => limited += 1,
                other => panic!("unexpected status {other}"),
            }
        }
        assert_eq!((ok, limited), (10, 2), "burst of 10 then 429");

        // 5 per second means a token comes back every 200 ms. With the old
        // per_second(5) it took 5 seconds and this request would be refused.
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert_eq!(hit(&app, "10.0.0.1", None).await, StatusCode::OK);
        assert_eq!(
            hit(&app, "10.0.0.1", None).await,
            StatusCode::OK,
            "a second token has refilled after 400 ms"
        );
        assert_eq!(
            hit(&app, "10.0.0.1", None).await,
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[tokio::test]
    async fn clients_have_separate_buckets() {
        let (limit, all) = limits(1, 1);
        let app = app(limit, &all);
        assert_eq!(hit(&app, "10.0.0.1", None).await, StatusCode::OK);
        assert_eq!(
            hit(&app, "10.0.0.1", None).await,
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(hit(&app, "10.0.0.2", None).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn disabled_limits_never_reject() {
        let (limit, mut all) = limits(1, 1);
        all.disabled = true;
        let app = app(limit, &all);
        for _ in 0..50 {
            assert_eq!(hit(&app, "10.0.0.1", None).await, StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn forwarded_for_is_only_used_when_trusted() {
        // Not trusted: every request comes from the same peer, so the
        // forwarded header cannot be used to dodge the limit.
        let (limit, all) = limits(1, 1);
        let app_untrusted = app(limit, &all);
        assert_eq!(
            hit(&app_untrusted, "10.0.0.1", Some("1.1.1.1")).await,
            StatusCode::OK
        );
        assert_eq!(
            hit(&app_untrusted, "10.0.0.1", Some("2.2.2.2")).await,
            StatusCode::TOO_MANY_REQUESTS
        );

        // Trusted (behind our own proxy): clients are told apart by header.
        let (limit, mut all) = limits(1, 1);
        all.trust_proxy_headers = true;
        let app_trusted = app(limit, &all);
        assert_eq!(
            hit(&app_trusted, "10.0.0.1", Some("1.1.1.1")).await,
            StatusCode::OK
        );
        assert_eq!(
            hit(&app_trusted, "10.0.0.1", Some("2.2.2.2")).await,
            StatusCode::OK
        );
        assert_eq!(
            hit(&app_trusted, "10.0.0.1", Some("1.1.1.1")).await,
            StatusCode::TOO_MANY_REQUESTS
        );
    }
}
