use async_nats::Client as NatsClient;
use domain::device::Location;
use domain::{Alert, AlertSeverity, AppError};
use infra::db::{AlertRepository, DeviceRepository};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct DispatchResult {
    pub alert_id: String,
    pub shards_targeted: usize,
    pub total_devices_notified: i64,
    pub per_shard: Vec<ShardDispatch>,
    /// Shard prefixes the alert FAILED to reach (NATS publish error).
    /// A non-empty list means partial delivery -- some devices in the
    /// alert's radius did not receive it. Previously this was a
    /// silent, undifferentiated 500 that aborted the whole dispatch on
    /// the first failure; now every shard is attempted, and the caller
    /// can see exactly which ones failed and retry/alert on just those.
    pub failed_shards: Vec<String>,
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
        Self { alert_repo, device_repo, nats }
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
        let center = Location::new(lat, lon)?;
        let alert = Alert::new(center, radius_meters, severity, title, message)?;

        self.alert_repo.insert_alert(&alert).await?;

        let shard_prefixes = alert.target_shard_prefixes()?;
        let mut per_shard = Vec::with_capacity(shard_prefixes.len());
        let mut failed_shards = Vec::new();
        let mut total_devices = 0i64;

        let payload = serde_json::to_vec(&alert).map_err(|e| AppError::Internal(e.into()))?;

        for prefix in &shard_prefixes {
            let subject = Alert::nats_subject_for_prefix(prefix);

            match infra::nats::publish(&self.nats, &subject, payload.clone()).await {
                Ok(()) => {
                    let devices_in_shard = match self.device_repo.count_in_shard(prefix).await {
                        Ok(count) => count,
                        Err(e) => {
                            tracing::error!(shard = %prefix, error = ?e, "publish succeeded but device count failed; recording as 0");
                            0
                        }
                    };

                    if let Err(e) = self
                        .alert_repo
                        .record_delivery(alert.id, prefix, devices_in_shard)
                        .await
                    {
                        tracing::error!(shard = %prefix, error = ?e, "alert delivered but audit write failed");
                    }

                    total_devices += devices_in_shard;
                    per_shard.push(ShardDispatch {
                        shard_prefix: prefix.clone(),
                        devices_in_shard,
                    });
                }
                Err(e) => {
                    tracing::error!(shard = %prefix, error = ?e, "failed to publish alert to shard -- devices in this shard will NOT receive this alert");
                    failed_shards.push(prefix.clone());
                }
            }
        }

        Ok(DispatchResult {
            alert_id: alert.id.0.to_string(),
            shards_targeted: shard_prefixes.len(),
            total_devices_notified: total_devices,
            per_shard,
            failed_shards,
        })
    }
}