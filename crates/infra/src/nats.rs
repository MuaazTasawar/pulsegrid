use std::time::Duration;

use async_nats::jetstream::consumer::{pull, AckPolicy, DeliverPolicy};
use async_nats::jetstream::stream::{Config as StreamConfig, DiscardPolicy, StorageType};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use domain::protocol::{
    alert_subject, control_subject, ALERT_STREAM_NAME, ALERT_SUBJECT_WILDCARD,
    TELEMETRY_DELIVERY_SUBJECT,
};
use domain::{Alert, AppError, DeliveryTelemetry, ServerMessage};
use futures_util::StreamExt;
use uuid::Uuid;

pub use async_nats::jetstream;
pub use async_nats::Client;

/// How long a single publish attempt (including the flush) is allowed
/// to take before we give up and treat it as a failure. Without this,
/// a genuine NATS outage causes publish() to hang on the client's own
/// internal reconnect/retry logic indefinitely -- unacceptable for a
/// time-critical alert system, where a fast, visible failure is far
/// better than a silent hang. Discovered via manual outage testing
/// (stopping the NATS container mid-request) rather than assumed.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(3);

/// Upper bound on how many alerts one replay request may fetch.
const MAX_REPLAY_FETCH: usize = 500;

fn msg_err<E: std::fmt::Display>(e: E) -> AppError {
    AppError::Messaging(e.to_string())
}

pub async fn connect(nats_url: &str) -> Result<Client, AppError> {
    async_nats::connect(nats_url).await.map_err(msg_err)
}

/// True while the client currently has a live connection to the server.
pub fn is_connected(client: &Client) -> bool {
    matches!(
        client.connection_state(),
        async_nats::connection::State::Connected
    )
}

/// Publishes raw bytes to a subject, bounded by PUBLISH_TIMEOUT. If the
/// timeout elapses (e.g. NATS is unreachable), returns an error rather
/// than hanging -- the caller (alert_dispatch_service) already treats
/// any Err here as a failed shard and continues to the next one, so
/// this timeout is what actually makes that partial-failure handling
/// meaningful under a real outage instead of just correct on paper.
pub async fn publish(client: &Client, subject: &str, payload: Vec<u8>) -> Result<(), AppError> {
    let publish_and_flush = async {
        client
            .publish(subject.to_string(), payload.into())
            .await
            .map_err(msg_err)?;
        client.flush().await.map_err(msg_err)?;
        Ok::<(), AppError>(())
    };

    tokio::time::timeout(PUBLISH_TIMEOUT, publish_and_flush)
        .await
        .map_err(|_| {
            AppError::Messaging(format!(
                "publish to subject '{subject}' timed out after {}s (NATS may be unreachable)",
                PUBLISH_TIMEOUT.as_secs()
            ))
        })?
}

pub async fn subscribe_and_handle<F>(
    client: &Client,
    subject: &str,
    mut handler: F,
) -> Result<(), AppError>
where
    F: FnMut(Vec<u8>) + Send,
{
    let mut sub = client
        .subscribe(subject.to_string())
        .await
        .map_err(msg_err)?;

    while let Some(message) = sub.next().await {
        handler(message.payload.to_vec());
    }
    Ok(())
}

// ---------------------------------------------------------------------
// JetStream: durable alerts and replay
// ---------------------------------------------------------------------

/// Creates (or updates) the `ALERTS` stream that durably stores every
/// published alert for `max_age`. Safe to call from every process at
/// startup. Publishing to `alerts.geo.<prefix>` through JetStream still
/// reaches plain core subscribers, so live fanout latency is unchanged.
pub async fn ensure_alert_stream(client: &Client, max_age: Duration) -> Result<(), AppError> {
    let js = jetstream::new(client.clone());
    js.create_or_update_stream(StreamConfig {
        name: ALERT_STREAM_NAME.to_string(),
        subjects: vec![ALERT_SUBJECT_WILDCARD.to_string()],
        max_age,
        storage: StorageType::File,
        discard: DiscardPolicy::Old,
        num_replicas: 1,
        ..Default::default()
    })
    .await
    .map_err(|e| {
        AppError::Messaging(format!(
            "could not create the alert stream (is JetStream enabled on the NATS server?): {e}"
        ))
    })?;
    Ok(())
}

/// Publishes an alert payload to JetStream and waits for the server's
/// acknowledgement that it was persisted, bounded by the same timeout as
/// plain publishes. Returns the stream sequence number.
pub async fn publish_durable(
    client: &Client,
    subject: &str,
    payload: Vec<u8>,
) -> Result<u64, AppError> {
    let js = jetstream::new(client.clone());
    let publish = async {
        let ack = js
            .publish(subject.to_string(), Bytes::from(payload))
            .await
            .map_err(msg_err)?;
        let ack = ack.await.map_err(msg_err)?;
        Ok::<u64, AppError>(ack.sequence)
    };
    tokio::time::timeout(PUBLISH_TIMEOUT, publish)
        .await
        .map_err(|_| {
            AppError::Messaging(format!(
                "durable publish to '{subject}' timed out after {}s (NATS may be unreachable)",
                PUBLISH_TIMEOUT.as_secs()
            ))
        })?
}

/// Reads the alerts stored for one shard since `since` (oldest first, at
/// most `max`). Uses a short-lived ephemeral consumer that is deleted
/// again afterwards. The caller is responsible for filtering by the
/// device's own cursor and location.
pub async fn replay_alerts(
    client: &Client,
    shard_prefix: &str,
    since: DateTime<Utc>,
    max: usize,
) -> Result<Vec<Alert>, AppError> {
    let max = max.clamp(1, MAX_REPLAY_FETCH);
    let js = jetstream::new(client.clone());
    let stream = js.get_stream(ALERT_STREAM_NAME).await.map_err(msg_err)?;

    let start_nanos = since.timestamp_nanos_opt().unwrap_or(0).max(0) as i128;
    let start_time =
        time::OffsetDateTime::from_unix_timestamp_nanos(start_nanos).map_err(msg_err)?;

    let consumer = stream
        .create_consumer(pull::Config {
            filter_subject: alert_subject(shard_prefix),
            deliver_policy: DeliverPolicy::ByStartTime { start_time },
            ack_policy: AckPolicy::None,
            inactive_threshold: Duration::from_secs(10),
            memory_storage: true,
            ..Default::default()
        })
        .await
        .map_err(msg_err)?;
    let consumer_name = consumer.cached_info().name.clone();

    let mut alerts = Vec::new();
    let mut batch = consumer
        .fetch()
        .max_messages(max)
        .expires(Duration::from_millis(750))
        .messages()
        .await
        .map_err(msg_err)?;
    while let Some(message) = batch.next().await {
        let message = message.map_err(msg_err)?;
        match serde_json::from_slice::<Alert>(&message.payload) {
            Ok(alert) => alerts.push(alert),
            Err(e) => tracing::warn!(error = %e, "skipping unparseable stored alert"),
        }
    }
    drop(batch);

    // Best effort: the consumer would also expire by itself.
    let _ = stream.delete_consumer(&consumer_name).await;
    Ok(alerts)
}

// ---------------------------------------------------------------------
// Control and telemetry messages
// ---------------------------------------------------------------------

/// Tells whichever worker holds `device_id`'s socket to forward `message`
/// to the device (for example "your shard changed" or "you were revoked").
pub async fn publish_control(
    client: &Client,
    device_id: Uuid,
    message: &ServerMessage,
) -> Result<(), AppError> {
    publish(
        client,
        &control_subject(device_id),
        message.to_json().into_bytes(),
    )
    .await
}

/// Fire-and-forget delivery telemetry for the live dashboard. Failures are
/// returned but callers normally just log them.
pub async fn publish_telemetry(
    client: &Client,
    telemetry: &DeliveryTelemetry,
) -> Result<(), AppError> {
    let payload = serde_json::to_vec(telemetry).map_err(msg_err)?;
    client
        .publish(TELEMETRY_DELIVERY_SUBJECT.to_string(), payload.into())
        .await
        .map_err(msg_err)
}
