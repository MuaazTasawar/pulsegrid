use async_nats::Client;
use domain::AppError;
use futures_util::StreamExt;

pub async fn connect(nats_url: &str) -> Result<Client, AppError> {
    async_nats::connect(nats_url)
        .await
        .map_err(|e| AppError::Messaging(e.to_string()))
}

/// Publishes raw bytes to a subject. Subject naming itself lives in
/// domain::Alert::nats_subject_for_prefix -- this stays a thin transport
/// wrapper so api-coordinator and fanout-worker can't drift on how they
/// construct subject strings.
pub async fn publish(client: &Client, subject: &str, payload: Vec<u8>) -> Result<(), AppError> {
    client
        .publish(subject.to_string(), payload.into())
        .await
        .map_err(|e| AppError::Messaging(e.to_string()))?;
    client
        .flush()
        .await
        .map_err(|e| AppError::Messaging(e.to_string()))?;
    Ok(())
}

/// Subscribes to a subject and invokes `handler` for every message
/// received, until the subscription ends or the process shuts down.
/// fanout-worker (Phase 5) calls this once per shard prefix it owns, with
/// a handler that pushes the payload out to every locally-held WebSocket
/// connection for that shard.
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