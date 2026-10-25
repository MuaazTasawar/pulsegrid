use std::sync::Arc;
use std::time::Instant;

use domain::device::Location;
use domain::{Alert, AlertId, AlertSeverity, AppError};
use futures_util::future::BoxFuture;
use futures_util::stream::{self, StreamExt};
use infra::db::{AlertRepository, DeliveryRecord, DeviceRepository, ShardCounts};
use infra::redis::AlertStats;
use infra::AlertStatsStore;
use serde::Serialize;

use crate::metrics::Metrics;

/// How many shards are published to (or counted) at the same time. Below the
/// database pool size, so a large alert cannot starve other requests.
pub const MAX_PARALLEL_SHARDS: usize = 16;

/// Where alert payloads go. The real implementation writes to JetStream;
/// tests plug in a fake to exercise partial failures.
pub trait AlertPublisher: Send + Sync {
    fn publish<'a>(
        &'a self,
        subject: &'a str,
        payload: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AppError>>;
}

/// Publishes through JetStream and waits for the server to confirm the alert
/// was stored (so it can be replayed to devices that reconnect later).
pub struct JetStreamPublisher {
    client: infra::nats::Client,
}

impl JetStreamPublisher {
    pub fn new(client: infra::nats::Client) -> Self {
        Self { client }
    }
}

impl AlertPublisher for JetStreamPublisher {
    fn publish<'a>(
        &'a self,
        subject: &'a str,
        payload: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), AppError>> {
        Box::pin(async move {
            infra::nats::publish_durable(&self.client, subject, payload)
                .await
                .map(|_sequence| ())
        })
    }
}

/// Publishes `payload` to every shard, up to `MAX_PARALLEL_SHARDS` at a time.
/// One shard failing never stops the others; every shard appears exactly once
/// in the result.
pub async fn publish_to_shards(
    publisher: &dyn AlertPublisher,
    prefixes: &[String],
    payload: &[u8],
) -> Vec<(String, Result<(), AppError>)> {
    stream::iter(prefixes.iter().cloned())
        .map(|prefix| async move {
            let subject = Alert::nats_subject_for_prefix(&prefix);
            let result = publisher.publish(&subject, payload.to_vec()).await;
            (prefix, result)
        })
        .buffer_unordered(MAX_PARALLEL_SHARDS)
        .collect()
        .await
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShardDispatch {
    pub shard_prefix: String,
    /// Registered devices in the whole shard cell (about 39 x 19.5 km).
    pub devices_in_shard: i64,
    /// Registered devices inside the alert circle.
    pub devices_in_radius: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DispatchResult {
    pub alert_id: String,
    pub shards_targeted: usize,
    pub shards_published: usize,
    /// Registered devices inside the alert circle, in the shards that were
    /// published. This is an upper bound on live deliveries: devices that are
    /// offline are not reached live. See `GET /alerts/{id}` for what the
    /// workers actually delivered.
    pub devices_in_radius: i64,
    /// Registered devices in the whole of the published shard cells.
    pub devices_in_targeted_shards: i64,
    /// Deprecated alias of `devices_in_radius`, kept for existing clients.
    pub total_devices_notified: i64,
    pub per_shard: Vec<ShardDispatch>,
    /// Shards the alert could NOT be published to. When this is not empty the
    /// API answers 207: devices in those shards did not get the alert.
    pub failed_shards: Vec<String>,
}

impl DispatchResult {
    pub fn is_partial(&self) -> bool {
        !self.failed_shards.is_empty()
    }
}

/// Combines the per-shard outcomes into the response (sorted so the output
/// is stable).
pub fn summarize(
    alert_id: AlertId,
    shards_targeted: usize,
    mut published: Vec<ShardDispatch>,
    mut failed_shards: Vec<String>,
) -> DispatchResult {
    published.sort_by(|a, b| a.shard_prefix.cmp(&b.shard_prefix));
    failed_shards.sort();
    let devices_in_radius: i64 = published.iter().map(|s| s.devices_in_radius).sum();
    let devices_in_targeted_shards: i64 = published.iter().map(|s| s.devices_in_shard).sum();
    DispatchResult {
        alert_id: alert_id.0.to_string(),
        shards_targeted,
        shards_published: published.len(),
        devices_in_radius,
        devices_in_targeted_shards,
        total_devices_notified: devices_in_radius,
        per_shard: published,
        failed_shards,
    }
}

/// An alert with its audit trail and the live delivery numbers.
#[derive(Debug, Serialize)]
pub struct AlertDetails {
    pub alert: Alert,
    pub deliveries: Vec<DeliveryRecord>,
    /// What the fanout workers actually pushed to live sockets. None until a
    /// worker has reported (or if the report expired).
    pub live_stats: Option<AlertStats>,
}

#[derive(Clone)]
pub struct AlertDispatchService {
    alerts: AlertRepository,
    devices: DeviceRepository,
    stats: AlertStatsStore,
    publisher: Arc<dyn AlertPublisher>,
    metrics: Arc<Metrics>,
}

impl AlertDispatchService {
    pub fn new(
        alerts: AlertRepository,
        devices: DeviceRepository,
        stats: AlertStatsStore,
        publisher: Arc<dyn AlertPublisher>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            alerts,
            devices,
            stats,
            publisher,
            metrics,
        }
    }

    pub async fn dispatch(
        &self,
        lat: f64,
        lon: f64,
        radius_meters: f64,
        severity: AlertSeverity,
        title: String,
        message: String,
    ) -> Result<DispatchResult, AppError> {
        let started = Instant::now();
        let center = Location::new(lat, lon)?;
        let alert = Alert::new(center, radius_meters, severity, title, message)?;

        // Resolve the shards before saving anything, so a request that cannot
        // be routed (for example a huge radius next to a pole) leaves no
        // orphan alert behind.
        let shard_prefixes = alert.target_shard_prefixes()?;
        self.alerts.insert_alert(&alert).await?;

        let payload = serde_json::to_vec(&alert).map_err(|e| AppError::Internal(e.into()))?;
        let outcomes = publish_to_shards(self.publisher.as_ref(), &shard_prefixes, &payload).await;

        let mut ok_prefixes = Vec::new();
        let mut failed_shards = Vec::new();
        for (prefix, outcome) in outcomes {
            match outcome {
                Ok(()) => ok_prefixes.push(prefix),
                Err(e) => {
                    tracing::error!(
                        shard = %prefix,
                        error = ?e,
                        "failed to publish alert to shard: devices there will NOT receive it"
                    );
                    failed_shards.push(prefix);
                }
            }
        }

        let alert_ref = &alert;
        let published: Vec<ShardDispatch> = stream::iter(ok_prefixes)
            .map(|prefix| async move {
                let counts = match self
                    .devices
                    .count_in_shard_and_radius(&prefix, &alert_ref.center, alert_ref.radius_meters)
                    .await
                {
                    Ok(counts) => counts,
                    Err(e) => {
                        tracing::warn!(shard = %prefix, error = ?e, "published, but the device count failed; recording 0");
                        ShardCounts {
                            in_shard: 0,
                            in_radius: 0,
                        }
                    }
                };
                if let Err(e) = self
                    .alerts
                    .record_shard_delivery(alert_ref.id, &prefix, counts)
                    .await
                {
                    tracing::error!(shard = %prefix, error = ?e, "alert published but the audit write failed");
                }
                ShardDispatch {
                    shard_prefix: prefix,
                    devices_in_shard: counts.in_shard,
                    devices_in_radius: counts.in_radius,
                }
            })
            .buffer_unordered(MAX_PARALLEL_SHARDS)
            .collect()
            .await;

        let result = summarize(alert.id, shard_prefixes.len(), published, failed_shards);

        self.metrics.alerts_dispatched.inc();
        self.metrics
            .alert_shards_published
            .add(result.shards_published as u64);
        self.metrics
            .alert_shard_failures
            .add(result.failed_shards.len() as u64);
        if result.is_partial() {
            self.metrics.alerts_partial.inc();
        }
        self.metrics
            .alert_dispatch_duration
            .observe_duration(started.elapsed());

        Ok(result)
    }

    /// The alert, its audit trail and what workers actually delivered.
    pub async fn alert_details(&self, id: AlertId) -> Result<AlertDetails, AppError> {
        let alert = self.alerts.get_alert(id).await?;
        let deliveries = self.alerts.list_deliveries(id).await?;
        let live_stats = match self.stats.get(id.0).await {
            Ok(stats) => stats,
            Err(e) => {
                tracing::warn!(alert_id = %id.0, error = ?e, "live delivery stats unavailable");
                None
            }
        };
        Ok(AlertDetails {
            alert,
            deliveries,
            live_stats,
        })
    }

    /// Newest alerts first.
    pub async fn recent(&self, limit: i64) -> Result<Vec<Alert>, AppError> {
        self.alerts.recent_alerts(limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::time::Duration;

    /// Fails for the listed shards and takes `delay` per publish.
    struct FakePublisher {
        fail_for: HashSet<String>,
        delay: Duration,
        seen: Mutex<Vec<String>>,
    }

    impl FakePublisher {
        fn new(fail_for: &[&str], delay_ms: u64) -> Self {
            Self {
                fail_for: fail_for.iter().map(|s| s.to_string()).collect(),
                delay: Duration::from_millis(delay_ms),
                seen: Mutex::new(Vec::new()),
            }
        }
    }

    impl AlertPublisher for FakePublisher {
        fn publish<'a>(
            &'a self,
            subject: &'a str,
            payload: Vec<u8>,
        ) -> BoxFuture<'a, Result<(), AppError>> {
            Box::pin(async move {
                tokio::time::sleep(self.delay).await;
                assert_eq!(payload, b"payload");
                self.seen.lock().unwrap().push(subject.to_string());
                let shard = subject.strip_prefix("alerts.geo.").unwrap();
                if self.fail_for.contains(shard) {
                    Err(AppError::Messaging(format!("{shard} unreachable")))
                } else {
                    Ok(())
                }
            })
        }
    }

    fn prefixes(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("s{i:03}")).collect()
    }

    #[tokio::test]
    async fn one_failing_shard_does_not_stop_the_others() {
        let publisher = FakePublisher::new(&["s002", "s005"], 1);
        let targets = prefixes(8);
        let outcomes = publish_to_shards(&publisher, &targets, b"payload").await;

        assert_eq!(outcomes.len(), 8);
        let mut failed: Vec<&str> = outcomes
            .iter()
            .filter(|(_, r)| r.is_err())
            .map(|(p, _)| p.as_str())
            .collect();
        failed.sort();
        assert_eq!(failed, ["s002", "s005"]);

        // every shard was attempted exactly once
        let mut seen = publisher.seen.lock().unwrap().clone();
        seen.sort();
        let expected: Vec<String> = targets.iter().map(|p| format!("alerts.geo.{p}")).collect();
        assert_eq!(seen, expected);
    }

    #[tokio::test]
    async fn shards_are_published_in_parallel_not_one_by_one() {
        // 32 shards x 50 ms would take 1.6 s one by one. With 16 in flight it
        // takes about 100 ms.
        let publisher = FakePublisher::new(&[], 50);
        let targets = prefixes(32);
        let started = Instant::now();
        let outcomes = publish_to_shards(&publisher, &targets, b"payload").await;
        let elapsed = started.elapsed();

        assert_eq!(outcomes.len(), 32);
        assert!(outcomes.iter().all(|(_, r)| r.is_ok()));
        assert!(
            elapsed < Duration::from_millis(800),
            "dispatch took {elapsed:?}; shards are not being published concurrently"
        );
    }

    #[tokio::test]
    async fn an_outage_costs_one_timeout_not_one_per_shard() {
        // Everything fails slowly (like an unreachable NATS). The total time
        // must stay near a single delay rather than the sum.
        let all: Vec<String> = prefixes(16);
        let refs: Vec<&str> = all.iter().map(String::as_str).collect();
        let publisher = FakePublisher::new(&refs, 100);
        let started = Instant::now();
        let outcomes = publish_to_shards(&publisher, &all, b"payload").await;
        assert!(outcomes.iter().all(|(_, r)| r.is_err()));
        assert!(started.elapsed() < Duration::from_millis(600));
    }

    #[test]
    fn summarize_totals_sorts_and_keeps_the_legacy_field() {
        let shard = |p: &str, in_shard: i64, in_radius: i64| ShardDispatch {
            shard_prefix: p.into(),
            devices_in_shard: in_shard,
            devices_in_radius: in_radius,
        };
        let id = AlertId::new();
        let result = summarize(
            id,
            5,
            vec![
                shard("tkrw", 10, 3),
                shard("tkrt", 20, 7),
                shard("tkrs", 5, 0),
            ],
            vec!["tkrv".into(), "tkrr".into()],
        );

        assert_eq!(result.alert_id, id.0.to_string());
        assert_eq!(result.shards_targeted, 5);
        assert_eq!(result.shards_published, 3);
        assert_eq!(result.devices_in_radius, 10);
        assert_eq!(result.devices_in_targeted_shards, 35);
        assert_eq!(result.total_devices_notified, result.devices_in_radius);
        let order: Vec<&str> = result
            .per_shard
            .iter()
            .map(|s| s.shard_prefix.as_str())
            .collect();
        assert_eq!(order, ["tkrs", "tkrt", "tkrw"]);
        assert_eq!(result.failed_shards, ["tkrr", "tkrv"]);
        assert!(result.is_partial());
    }

    #[test]
    fn a_clean_dispatch_is_not_partial() {
        let result = summarize(AlertId::new(), 0, vec![], vec![]);
        assert!(!result.is_partial());
        assert_eq!(result.devices_in_radius, 0);
    }
}
