use async_nats::Client as NatsClient;
use domain::Alert;

use crate::connection_registry::ConnectionRegistry;

/// Spawns one subscriber task per owned shard prefix. Each task
/// subscribes to `alerts.geo.<prefix>` (the exact subject naming
/// Alert::nats_subject_for_prefix defines in Phase 1, so the
/// coordinator's publisher and this subscriber can never drift on the
/// string format independently) and fans every received alert out to
/// this instance's locally-held connections for that shard.
pub fn spawn_shard_subscribers(nats: NatsClient, registry: ConnectionRegistry, owned_prefixes: Vec<String>) {
    for prefix in owned_prefixes {
        let nats = nats.clone();
        let registry = registry.clone();
        let subject = Alert::nats_subject_for_prefix(&prefix);

        tokio::spawn(async move {
            tracing::info!(%subject, "subscribing to shard");
            let result = infra::nats::subscribe_and_handle(&nats, &subject, |payload| {
                match serde_json::from_slice::<Alert>(&payload) {
                    Ok(_alert) => {
                        let (delivered, dropped) = registry.broadcast_to_shard(&prefix, &payload);
                        tracing::info!(shard = %prefix, delivered, dropped, "alert fanned out to shard");
                    }
                    Err(e) => {
                        tracing::error!(shard = %prefix, error = ?e, "failed to deserialize alert payload");
                    }
                }
            })
            .await;

            if let Err(e) = result {
                tracing::error!(%subject, error = ?e, "shard subscriber loop exited");
            }
        });
    }
}