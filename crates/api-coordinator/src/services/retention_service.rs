use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use domain::AppError;
use infra::db::{AlertRepository, DeviceRepository};
use tokio::task::JoinHandle;

use crate::metrics::Metrics;

/// Rows deleted per statement, so a big backlog never holds long locks.
const BATCH_SIZE: i64 = 500;
/// A single sweep stops after this many batches and carries on at the next
/// interval, so it can never run away.
const MAX_BATCHES_PER_SWEEP: usize = 200;
/// How long after startup the first sweep runs.
const FIRST_SWEEP_DELAY: Duration = Duration::from_secs(60);

/// What one sweep removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub devices_deleted: u64,
    pub alerts_deleted: u64,
}

/// Deletes devices that have been silent for too long and alerts that are
/// past their retention period (their delivery rows go with them).
#[derive(Clone)]
pub struct RetentionService {
    devices: DeviceRepository,
    alerts: AlertRepository,
    device_retention: ChronoDuration,
    alert_retention: ChronoDuration,
    metrics: Arc<Metrics>,
}

impl RetentionService {
    pub fn new(
        devices: DeviceRepository,
        alerts: AlertRepository,
        device_retention_days: i64,
        alert_retention_days: i64,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            devices,
            alerts,
            device_retention: ChronoDuration::days(device_retention_days),
            alert_retention: ChronoDuration::days(alert_retention_days),
            metrics,
        }
    }

    /// Anything older than these timestamps is removed, as of `now`.
    pub fn cutoffs(&self, now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
        (now - self.device_retention, now - self.alert_retention)
    }

    /// Runs one sweep as of `now`.
    pub async fn sweep(&self, now: DateTime<Utc>) -> Result<SweepReport, AppError> {
        let (device_cutoff, alert_cutoff) = self.cutoffs(now);
        let mut report = SweepReport::default();

        for _ in 0..MAX_BATCHES_PER_SWEEP {
            let n = self
                .devices
                .purge_inactive(device_cutoff, BATCH_SIZE)
                .await?;
            report.devices_deleted += n;
            if n < BATCH_SIZE as u64 {
                break;
            }
        }
        for _ in 0..MAX_BATCHES_PER_SWEEP {
            let n = self
                .alerts
                .purge_older_than(alert_cutoff, BATCH_SIZE)
                .await?;
            report.alerts_deleted += n;
            if n < BATCH_SIZE as u64 {
                break;
            }
        }

        self.metrics
            .retention_devices_deleted
            .add(report.devices_deleted);
        self.metrics
            .retention_alerts_deleted
            .add(report.alerts_deleted);
        Ok(report)
    }

    /// Runs a sweep every `interval` for as long as the process lives. A
    /// failed sweep is logged and retried at the next interval.
    pub fn spawn(self, interval: Duration) -> JoinHandle<()> {
        tokio::spawn(async move {
            let first = tokio::time::Instant::now() + FIRST_SWEEP_DELAY.min(interval);
            let mut ticker = tokio::time::interval_at(first, interval);
            loop {
                ticker.tick().await;
                match self.sweep(Utc::now()).await {
                    Ok(report) if report == SweepReport::default() => {
                        tracing::debug!("retention sweep: nothing to remove");
                    }
                    Ok(report) => tracing::info!(
                        devices = report.devices_deleted,
                        alerts = report.alerts_deleted,
                        "retention sweep removed old rows"
                    ),
                    Err(e) => tracing::error!(error = ?e, "retention sweep failed"),
                }
            }
        })
    }
}
