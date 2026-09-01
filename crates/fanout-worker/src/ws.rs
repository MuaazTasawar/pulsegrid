use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;
use uuid::Uuid;

use crate::AppState;

#[derive(Debug, Deserialize)]
pub struct ConnectParams {
    pub shard_prefix: String,
}

pub async fn ws_upgrade_handler(
    ws: WebSocketUpgrade,
    Path(device_id): Path<Uuid>,
    Query(params): Query<ConnectParams>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Reject up front if this worker doesn't own the claimed shard --
    // fail fast rather than accept a connection that will never
    // receive anything, matching the design note in config.rs.
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

    // Forwarding task: anything the connection_registry broadcasts into
    // this device's channel gets written out to the actual socket.
    let forward_handle = tokio::spawn(async move {
        while let Some(payload) = rx.recv().await {
            if ws_sender.send(Message::Binary(payload.into())).await.is_err() {
                break;
            }
        }
    });

    // Read loop: mainly exists to detect disconnects (client close frame
    // or socket error) -- this app doesn't expect meaningful inbound
    // messages from the client beyond the implicit ping/pong axum
    // already handles.
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

use futures_util::{SinkExt, StreamExt};