use std::time::Duration;

use axum::Router;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::key_extractor::SmartIpKeyExtractor;
use tower_governor::GovernorLayer;

use crate::config::Config;

/// Milliseconds between two refilled tokens for a "N per second" limit.
///
/// tower_governor's `per_second(n)` means "one token every n SECONDS", not
/// "n per second", so limits are always built from a millisecond period.
pub fn refill_period_ms(per_sec: u32) -> u64 {
    (1000 / u64::from(per_sec.max(1))).max(1)
}

/// Limits how fast one client address may open new connections. Does nothing
/// when `RATE_LIMIT_DISABLED=true`. Must be called inside a Tokio runtime.
pub fn with_connect_limit<S>(route: Router<S>, config: &Config) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    if config.rate_limit_disabled {
        return route;
    }
    let period = refill_period_ms(config.connect_limit.per_sec);
    let burst = config.connect_limit.burst;

    if config.trust_proxy_headers {
        let mut builder = GovernorConfigBuilder::default().key_extractor(SmartIpKeyExtractor);
        builder.per_millisecond(period).burst_size(burst);
        let governor = builder.finish().expect("valid rate limit configuration");
        let limiter = governor.limiter().clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                limiter.retain_recent();
            }
        });
        route.layer(GovernorLayer::new(governor))
    } else {
        let mut builder = GovernorConfigBuilder::default();
        builder.per_millisecond(period).burst_size(burst);
        let governor = builder.finish().expect("valid rate limit configuration");
        let limiter = governor.limiter().clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            loop {
                tick.tick().await;
                limiter.retain_recent();
            }
        });
        route.layer(GovernorLayer::new(governor))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn period_is_per_request() {
        assert_eq!(refill_period_ms(50), 20);
        assert_eq!(refill_period_ms(1), 1000);
        assert_eq!(refill_period_ms(10_000), 1);
        assert_eq!(refill_period_ms(0), 1000);
    }
}
