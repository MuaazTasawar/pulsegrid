use async_nats::Client;
use domain::AppError;
use futures_util::StreamExt;
use std::time::Duration;

/// How long a single publish attempt (including the flush) is allowed
/// to take before we give up and treat it as a failure. Without this,
/// a genuine NATS outage causes publish() to hang on the client's own
/// internal reconnect/retry logic indefinitely -- unacceptable for a
/// time-critical alert system, where a fast, visible failure is far
/// better than a silent hang. Discovered via manual outage testing
/// (stopping the NATS container mid-request) rather than assumed.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(3);

pub async fn connect(nats_url: &str) -> Result<Client, AppError> {
    async_nats::connect(nats_url)
        .await
        .map_err(|e| AppError::Messaging(e.to_string()))
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
            .map_err(|e| AppError::Messaging(e.to_string()))?;
        client
            .flush()
            .await
            .map_err(|e| AppError::Messaging(e.to_string()))?;
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
        .map_err(|e| AppError::Messaging(e.to_string()))?;

    while let Some(message) = sub.next().await {
        handler(message.payload.to_vec());
    }
    Ok(())
}