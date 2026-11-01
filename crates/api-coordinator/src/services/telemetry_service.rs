//! Delivery telemetry for the live dashboard.
//!
//! Workers publish one `DeliveryTelemetry` per shard per alert on the NATS
//! subject `telemetry.delivery` (fire and forget). This service subscribes
//! once and re-broadcasts every message in-process, so any number of
//! dashboard streams can follow along without each one touching NATS.
//!
//! A dashboard that reads too slowly only loses its own events (it is told
//! how many); it can never slow down the coordinator or other dashboards.

use std::time::Duration;

use domain::protocol::TELEMETRY_DELIVERY_SUBJECT;
use domain::DeliveryTelemetry;
use futures_util::StreamExt;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// How many events a slow subscriber may fall behind before it starts losing
/// the oldest ones.
const CHANNEL_CAPACITY: usize = 1024;
/// Pause before the NATS subscription is attempted again after it failed or ended.
const RESUBSCRIBE_DELAY: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct TelemetryService {
    tx: broadcast::Sender<DeliveryTelemetry>,
}

impl Default for TelemetryService {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self { tx }
    }
}

impl TelemetryService {
    pub fn new() -> Self {
        Self::default()
    }

    /// A new receiver that sees every event published from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<DeliveryTelemetry> {
        self.tx.subscribe()
    }

    /// Hands an event to every current subscriber. Returns how many there
    /// were; having none is normal.
    pub fn publish(&self, telemetry: DeliveryTelemetry) -> usize {
        self.tx.send(telemetry).unwrap_or(0)
    }

    /// Follows `telemetry.delivery` on NATS until the process ends,
    /// resubscribing if the subscription fails or ends.
    pub fn spawn(&self, client: infra::nats::Client) -> JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move {
            loop {
                match client
                    .subscribe(TELEMETRY_DELIVERY_SUBJECT.to_string())
                    .await
                {
                    Ok(mut subscription) => {
                        tracing::info!("subscribed to delivery telemetry");
                        while let Some(message) = subscription.next().await {
                            match serde_json::from_slice::<DeliveryTelemetry>(&message.payload) {
                                Ok(telemetry) => {
                                    service.publish(telemetry);
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, "ignoring unparseable telemetry");
                                }
                            }
                        }
                        tracing::warn!("delivery telemetry subscription ended");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "could not subscribe to delivery telemetry");
                    }
                }
                tokio::time::sleep(RESUBSCRIBE_DELAY).await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn sample(delivered: usize) -> DeliveryTelemetry {
        DeliveryTelemetry {
            alert_id: Uuid::new_v4(),
            shard_prefix: "tkrt".into(),
            delivered,
            dropped: 0,
            filtered_out: 1,
            broadcast_duration_us: 120,
            worker_id: "worker-1".into(),
            at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn every_subscriber_receives_what_is_published() {
        let service = TelemetryService::new();
        let mut a = service.subscribe();
        let mut b = service.subscribe();

        assert_eq!(service.publish(sample(3)), 2);

        assert_eq!(a.recv().await.unwrap().delivered, 3);
        assert_eq!(b.recv().await.unwrap().delivered, 3);
    }

    #[tokio::test]
    async fn publishing_without_subscribers_is_harmless() {
        let service = TelemetryService::new();
        assert_eq!(service.publish(sample(1)), 0);
    }

    #[tokio::test]
    async fn a_slow_subscriber_lags_but_publishers_never_block() {
        let service = TelemetryService::new();
        let mut slow = service.subscribe();

        for n in 0..(CHANNEL_CAPACITY + 10) {
            service.publish(sample(n));
        }

        assert!(matches!(
            slow.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        // After being told, it carries on with the events that are still there.
        assert!(slow.recv().await.is_ok());
    }
}
