use std::time::{Duration, Instant};

use bytes::Bytes;
use domain::{Alert, DeliveryTelemetry};
use futures_util::StreamExt;
use tokio::task::JoinHandle;

use crate::connection_registry::{BroadcastResult, ConnectionRegistry};
use crate::WorkerState;

/// How long the per-alert delivery numbers stay readable in Redis.
const STATS_TTL_SECS: u64 = 7 * 24 * 3600;

/// The outcome of fanning one alert out to the local sockets of one shard.
pub struct Fanout {
    pub alert: Alert,
    pub result: BroadcastResult,
    pub duration: Duration,
}

/// Parses an alert once and queues it to every local socket of the shard that
/// is inside the alert circle. The raw bytes are what devices receive.
pub fn fanout(
    registry: &ConnectionRegistry,
    shard_prefix: &str,
    payload: Bytes,
) -> Result<Fanout, serde_json::Error> {
    let alert: Alert = serde_json::from_slice(&payload)?;
    let started = Instant::now();
    let result = registry.broadcast_to_shard(shard_prefix, &alert, &payload);
    Ok(Fanout {
        alert,
        result,
        duration: started.elapsed(),
    })
}

/// Subscribes to `alerts.geo.<prefix>` for every shard this worker owns and
/// keeps each subscription alive (resubscribing if it ever ends).
pub fn spawn_shard_subscribers(state: &WorkerState) -> Vec<JoinHandle<()>> {
    state
        .config
        .owned_prefixes
        .iter()
        .cloned()
        .map(|prefix| tokio::spawn(run_shard(state.clone(), prefix)))
        .collect()
}

async fn run_shard(state: WorkerState, prefix: String) {
    let subject = Alert::nats_subject_for_prefix(&prefix);
    loop {
        match state.nats.subscribe(subject.clone()).await {
            Ok(mut subscription) => {
                state.health.subscription_up();
                tracing::info!(shard = %prefix, %subject, "subscribed to alerts");
                while let Some(message) = subscription.next().await {
                    on_alert(&state, &prefix, message.payload);
                }
                state.health.subscription_down();
                tracing::warn!(shard = %prefix, "alert subscription ended; resubscribing");
            }
            Err(e) => {
                tracing::warn!(shard = %prefix, error = ?e, "could not subscribe; retrying");
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn on_alert(state: &WorkerState, prefix: &str, payload: Bytes) {
    let metrics = &state.metrics;
    metrics.alerts_received.inc(prefix);

    let fanout = match fanout(&state.registry, prefix, payload) {
        Ok(f) => f,
        Err(e) => {
            metrics.alerts_invalid.inc();
            tracing::warn!(shard = %prefix, error = %e, "ignoring an alert message that is not valid JSON");
            return;
        }
    };

    let r = fanout.result;
    metrics.alerts_delivered.add(prefix, r.delivered as u64);
    metrics.alerts_dropped.add(prefix, r.dropped as u64);
    metrics
        .alerts_filtered_out
        .add(prefix, r.filtered_out as u64);
    metrics.broadcast_duration.observe_duration(fanout.duration);
    tracing::info!(
        shard = %prefix,
        alert_id = %fanout.alert.id.0,
        delivered = r.delivered,
        dropped = r.dropped,
        filtered_out = r.filtered_out,
        broadcast_duration_us = fanout.duration.as_micros() as u64,
        "alert fanned out to shard"
    );

    // Reporting must never slow the fanout loop down.
    let state = state.clone();
    let prefix = prefix.to_string();
    tokio::spawn(async move {
        if let Err(e) = state
            .stats
            .record(
                fanout.alert.id.0,
                &prefix,
                r.delivered as u64,
                r.dropped as u64,
                r.filtered_out as u64,
                STATS_TTL_SECS,
            )
            .await
        {
            tracing::warn!(error = ?e, "could not record delivery stats");
        }
        let telemetry = DeliveryTelemetry {
            alert_id: fanout.alert.id.0,
            shard_prefix: prefix,
            delivered: r.delivered,
            dropped: r.dropped,
            filtered_out: r.filtered_out,
            broadcast_duration_us: fanout.duration.as_micros() as u64,
            worker_id: state.config.instance_id.clone(),
            at: chrono::Utc::now(),
        };
        if let Err(e) = infra::nats::publish_telemetry(&state.nats, &telemetry).await {
            tracing::debug!(error = ?e, "could not publish delivery telemetry");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_registry::Outbound;
    use domain::device::Location;
    use domain::AlertSeverity;
    use uuid::Uuid;

    fn alert_bytes(radius: f64) -> Bytes {
        let alert = Alert::new(
            Location::new(24.8607, 67.0011).unwrap(),
            radius,
            AlertSeverity::Warning,
            "t".into(),
            "m".into(),
        )
        .unwrap();
        Bytes::from(serde_json::to_vec(&alert).unwrap())
    }

    #[test]
    fn a_valid_alert_is_fanned_out_with_counts() {
        let reg = ConnectionRegistry::new();
        let mut inside = reg
            .register(
                Uuid::new_v4(),
                "tkrt",
                Location::new(24.8607, 67.0011).unwrap(),
            )
            .receiver;
        let mut outside = reg
            .register(Uuid::new_v4(), "tkrt", Location::new(25.5, 67.0).unwrap())
            .receiver;

        let payload = alert_bytes(5_000.0);
        let f = fanout(&reg, "tkrt", payload.clone()).unwrap();

        assert_eq!(
            f.result,
            BroadcastResult {
                delivered: 1,
                dropped: 0,
                filtered_out: 1
            }
        );
        // devices receive the original bytes, untouched
        assert_eq!(inside.try_recv().unwrap(), Outbound::Alert(payload));
        assert!(outside.try_recv().is_err());
    }

    #[test]
    fn garbage_is_rejected_without_touching_any_socket() {
        let reg = ConnectionRegistry::new();
        let mut rx = reg
            .register(
                Uuid::new_v4(),
                "tkrt",
                Location::new(24.8607, 67.0011).unwrap(),
            )
            .receiver;
        assert!(fanout(&reg, "tkrt", Bytes::from_static(b"not json")).is_err());
        assert!(fanout(&reg, "tkrt", Bytes::from_static(b"{}")).is_err());
        assert!(rx.try_recv().is_err());
    }
}
