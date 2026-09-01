use async_nats::Client as NatsClient;
use domain::device::Location;
use domain::{Alert, AlertSeverity, AppError};
use infra::db::{AlertRepository, DeviceRepository};
use serde::Serialize;

/// Result of dispatching one alert -- per-shard breakdown plus totals.
/// The coordinator's HTTP response and the demo's "N devices notified"
/// counter both read straight off this struct.
#[derive(Debug, Serialize)]
pub struct DispatchResult {
    pub alert_id: String,
    pub shards_targeted: usize,
    pub total_devices_notified: i64,
    pub per_shard: Vec<ShardDispatch>,
}

#[derive(Debug, Serialize)]
pub struct ShardDispatch {
    pub shard_prefix: String,
    pub devices_in_shard: i64,
}

#[derive(Clone)]
pub struct AlertDispatchService {
    alert_repo: AlertRepository,
    device_repo: DeviceRepository,
    nats: NatsClient,
}

impl AlertDispatchService {
    pub fn new(alert_repo: AlertRepository, device_repo: DeviceRepository, nats: NatsClient) -> Self {
        Self {
            alert_repo,
            device_repo,
            nats,
        }
    }

    /// Core dispatch flow:
    /// 1. Build + validate the Alert domain object
    /// 2. Persist it (audit trail root row)
    /// 3. Resolve target shard prefixes via the geo routing algorithm
    /// 4. For each shard: publish the alert payload over NATS, then
    ///    record the delivery (shard + device count) for audit
    ///
    /// Publishes happen sequentially per shard rather than all at once
    /// with `join_all` -- deliberate for now, so a single slow/failing
    /// shard publish is visible in per-shard timing rather than hidden
    /// inside a batched future. Phase 6's load test will show whether
    /// this needs to become concurrent for real fanout-latency numbers;
    /// premature parallelism here would just make failures harder to
    /// attribute to a specific shard.
    pub async fn dispatch(
        &self,
        lat: f64,
        lon: f64,
        radius_meters: f64,
        severity: AlertSeverity,
        title: String,
        message: String,
    ) -> Result<DispatchResult, AppError> {
        let center = Location::new(lat, lon)?;
        let alert = Alert::new(center, radius_meters, severity, title, message)?;

        self.alert_repo.insert_alert(&alert).await?;

        let shard_prefixes = alert.target_shard_prefixes()?;
        let mut per_shard = Vec::with_capacity(shard_prefixes.len());
        let mut total_devices = 0i64;

        let payload = serde_json::to_vec(&alert)
            .map_err(|e| AppError::Internal(e.into()))?;

        for prefix in &shard_prefixes {
            let subject = Alert::nats_subject_for_prefix(prefix);
            infra::nats::publish(&self.nats, &subject, payload.clone()).await?;

            let devices_in_shard = self.device_repo.count_in_shard(prefix).await?;
            self.alert_repo
                .record_delivery(alert.id, prefix, devices_in_shard)
                .await?;

            total_devices += devices_in_shard;
            per_shard.push(ShardDispatch {
                shard_prefix: prefix.clone(),
                devices_in_shard,
            });
        }

        Ok(DispatchResult {
            alert_id: alert.id.0.to_string(),
            shards_targeted: shard_prefixes.len(),
            total_devices_notified: total_devices,
            per_shard,
        })
    }
}