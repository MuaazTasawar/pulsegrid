use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use uuid::Uuid;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct ConnectParams {
    pub shard_prefix: String,
    /// JWT issued at /devices/register. WebSocket upgrade requests can't
    /// carry a normal Authorization header reliably across all clients,
    /// so the token travels as a query parameter instead -- a common,
    /// accepted pattern for WS auth (same approach Slack/Discord use).
    pub token: String,
}

pub async fn ws_upgrade_handler(
    ws: WebSocketUpgrade,
    Path(device_id): Path<Uuid>,
    Query(params): Query<ConnectParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let verified_device_id = match domain::verify_token(&params.token, &state.jwt_secret) {
        Ok(id) => id,
        Err(_) => {
            return (StatusCode::UNAUTHORIZED, "invalid or expired token").into_response();
        }
    };

    // The token must belong to the SAME device_id the client is trying
    // to connect as -- this is what closes the impersonation gap: a
    // valid token for device A can no longer be used to open a
    // connection (and receive alerts) as device B.
    if verified_device_id != device_id {
        return (
            StatusCode::FORBIDDEN,
            "token does not match the requested device_id",
        )
            .into_response();
    }

    if !state.owned_prefixes.contains(&params.shard_prefix) {
        return (
            axum::http::StatusCode::MISDIRECTED_REQUEST,
            format!(
                "this worker does not own shard '{}'; owned shards: {:?}",
                params.shard_prefix, state.owned_prefixes
            ),
        )
            .into_response();
    }

    ws.on_upgrade(move |socket| handle_socket(socket, device_id, params.shard_prefix, state))
}

async fn handle_socket(socket: WebSocket, device_id: Uuid, shard_prefix: String, state: AppState) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let mut rx = state.registry.register(device_id, shard_prefix.clone());

    if let Err(e) = state.presence.register(&device_id.to_string(), &state.instance_id).await {
        tracing::warn!(%device_id, error = ?e, "failed to register presence; connection still proceeds");
    }
    tracing::info!(%device_id, %shard_prefix, connections = state.registry.connection_count(), "device connected");

    let presence_for_heartbeat = state.presence.clone();
    let device_id_for_heartbeat = device_id;
    let heartbeat_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(20));
        loop {
            interval.tick().await;
            if let Err(e) = presence_for_heartbeat
                .heartbeat(&device_id_for_heartbeat.to_string())
                .await
            {
                tracing::warn!(device_id = %device_id_for_heartbeat, error = ?e, "presence heartbeat failed");
            }
        }
    });

    let forward_handle = tokio::spawn(async move {
        while let Some(payload) = rx.recv().await {
            if ws_sender.send(Message::Binary(payload.into())).await.is_err() {
                break;
            }
        }
    });

    while let Some(msg) = ws_receiver.next().await {
        if msg.is_err() {
            break;
        }
    }

    heartbeat_handle.abort();
    forward_handle.abort();
    state.registry.deregister(&device_id);
    if let Err(e) = state.presence.deregister(&device_id.to_string()).await {
        tracing::warn!(%device_id, error = ?e, "failed to deregister presence on disconnect");
    }
    tracing::info!(%device_id, connections = state.registry.connection_count(), "device disconnected");
}