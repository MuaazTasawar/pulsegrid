use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use bytes::Bytes;
use domain::device::Location;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::connection_registry::{Outbound, Registered};
use crate::WorkerState;

/// A WebSocket protocol ping is sent this often to find dead connections.
const PING_INTERVAL: Duration = Duration::from_secs(25);
/// A single write to a device may take at most this long.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the presence record is refreshed.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(20);
/// A connection that has sent nothing (not even a pong) for this long is closed.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Debug, Deserialize)]
pub struct ConnectParams {
    /// A one-time ticket from `POST /devices/ws-ticket` on the coordinator.
    pub ticket: String,
    /// Optional hint of the shard the client believes it belongs to. If it
    /// disagrees with where the device really is, the connection is refused.
    pub shard_prefix: Option<String>,
}

/// Why a connection was refused. Turned into a JSON error response.
#[derive(Debug)]
pub struct Rejection {
    pub status: StatusCode,
    /// Short machine-readable reason (also the metrics label).
    pub reason: &'static str,
    pub message: String,
    /// For a wrong-shard rejection: where the device should connect instead.
    pub ws_url: Option<String>,
}

impl Rejection {
    fn new(status: StatusCode, reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            reason,
            message: message.into(),
            ws_url: None,
        }
    }
}

impl IntoResponse for Rejection {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": self.message,
                "reason": self.reason,
                "ws_url": self.ws_url,
            })),
        )
            .into_response()
    }
}

/// What a successfully authorized connection knows about its device.
pub struct Authorized {
    pub shard_prefix: String,
    pub location: Location,
}

/// Decides whether `device_id` may open a WebSocket here, before upgrading.
///
/// The one-time ticket is the only credential; it proves the coordinator
/// authenticated the device a moment ago. The device's real shard and position
/// come from the coordinator's own record in Redis, never from the client.
pub async fn authorize(
    state: &WorkerState,
    device_id: Uuid,
    params: &ConnectParams,
) -> Result<Authorized, Rejection> {
    let unavailable = |what: &str, e: domain::AppError| {
        tracing::error!(error = ?e, "{what} unavailable while authorizing a connection");
        Rejection::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "backend_unavailable",
            "authentication backend unavailable",
        )
    };

    let ticket_device = state
        .tickets
        .consume(&params.ticket)
        .await
        .map_err(|e| unavailable("ticket store", e))?
        .ok_or_else(|| {
            Rejection::new(
                StatusCode::UNAUTHORIZED,
                "invalid_ticket",
                "the ticket is invalid, expired or already used",
            )
        })?;

    if ticket_device != device_id {
        return Err(Rejection::new(
            StatusCode::FORBIDDEN,
            "ticket_device_mismatch",
            "the ticket was issued for a different device",
        ));
    }

    if state
        .revocations
        .is_revoked(device_id)
        .await
        .map_err(|e| unavailable("revocation list", e))?
    {
        return Err(Rejection::new(
            StatusCode::UNAUTHORIZED,
            "revoked",
            "this device has been revoked",
        ));
    }

    let record = state
        .locations
        .get(device_id)
        .await
        .map_err(|e| unavailable("location store", e))?
        .ok_or_else(|| {
            Rejection::new(
                StatusCode::CONFLICT,
                "location_unknown",
                "the device's location is not known; request a new ticket",
            )
        })?;
    let location = record.location().map_err(|_| {
        Rejection::new(
            StatusCode::CONFLICT,
            "location_unknown",
            "the device's stored location is invalid; request a new ticket",
        )
    })?;

    if !state.owned.contains(&record.shard_prefix) {
        let mut rejection = Rejection::new(
            StatusCode::MISDIRECTED_REQUEST,
            "wrong_shard",
            format!(
                "this worker does not own shard {}; connect to the worker that does",
                record.shard_prefix
            ),
        );
        rejection.ws_url = state
            .shards
            .lookup_shard(&record.shard_prefix)
            .await
            .ok()
            .flatten();
        return Err(rejection);
    }

    if let Some(claimed) = &params.shard_prefix {
        if claimed != &record.shard_prefix {
            return Err(Rejection::new(
                StatusCode::MISDIRECTED_REQUEST,
                "stale_shard_hint",
                format!(
                    "the device is in shard {}, not {claimed}; request a new ticket",
                    record.shard_prefix
                ),
            ));
        }
    }

    Ok(Authorized {
        shard_prefix: record.shard_prefix,
        location,
    })
}

/// `GET /connect/{device_id}?ticket=...`
pub async fn ws_upgrade_handler(
    ws: WebSocketUpgrade,
    Path(device_id): Path<Uuid>,
    Query(params): Query<ConnectParams>,
    State(state): State<WorkerState>,
) -> Response {
    match authorize(&state, device_id, &params).await {
        Ok(auth) => ws.on_upgrade(move |socket| async move {
            handle_socket(socket, state, device_id, auth).await
        }),
        Err(rejection) => {
            state.metrics.rejects.inc(rejection.reason);
            tracing::info!(%device_id, reason = rejection.reason, "connection refused");
            rejection.into_response()
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Writes one frame, giving up (and so closing the connection) if the client
/// does not take it within `SEND_TIMEOUT`.
async fn send(sink: &mut SplitSink<WebSocket, Message>, message: Message) -> bool {
    matches!(
        tokio::time::timeout(SEND_TIMEOUT, sink.send(message)).await,
        Ok(Ok(()))
    )
}

async fn handle_socket(socket: WebSocket, state: WorkerState, device_id: Uuid, auth: Authorized) {
    let (sink, mut stream) = socket.split();
    let Registered {
        conn_id,
        mut receiver,
    } = state
        .registry
        .register(device_id, &auth.shard_prefix, auth.location);
    state.metrics.connects.inc();
    state.metrics.connections.inc();

    if let Err(e) = state
        .presence
        .register(&device_id.to_string(), &state.config.instance_id)
        .await
    {
        // Presence is for visibility only; the connection still works.
        tracing::warn!(%device_id, error = ?e, "could not register presence");
    }
    tracing::info!(
        %device_id,
        shard = %auth.shard_prefix,
        connections = state.registry.connection_count(),
        "device connected"
    );

    let last_activity = Arc::new(AtomicU64::new(now_ms()));

    // Writer: everything the device is sent goes through this one task.
    let mut writer = tokio::spawn(async move {
        let mut sink = sink;
        let mut ping = tokio::time::interval(PING_INTERVAL);
        ping.tick().await; // the first tick fires immediately
        loop {
            tokio::select! {
                outbound = receiver.recv() => {
                    let Some(outbound) = outbound else { break };
                    match outbound {
                        Outbound::Alert(payload) => {
                            if !send(&mut sink, Message::Binary(payload)).await {
                                break;
                            }
                        }
                        Outbound::Control { text, close } => {
                            let sent = send(&mut sink, Message::Text(text.into())).await;
                            if close || !sent {
                                let _ = send(&mut sink, Message::Close(None)).await;
                                break;
                            }
                        }
                        Outbound::Close => {
                            let _ = send(&mut sink, Message::Close(None)).await;
                            break;
                        }
                    }
                }
                _ = ping.tick() => {
                    if !send(&mut sink, Message::Ping(Bytes::new())).await {
                        break;
                    }
                }
            }
        }
        let _ = sink.close().await;
    });

    // Reader: only used to notice the client leaving or going silent.
    let mut reader = {
        let last_activity = last_activity.clone();
        tokio::spawn(async move {
            while let Some(frame) = stream.next().await {
                match frame {
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => last_activity.store(now_ms(), Ordering::Relaxed),
                }
            }
        })
    };

    // Keeps the presence record fresh and drops connections that went silent.
    let mut heartbeat = {
        let state = state.clone();
        let last_activity = last_activity.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let idle = now_ms().saturating_sub(last_activity.load(Ordering::Relaxed));
                if idle > IDLE_TIMEOUT.as_millis() as u64 {
                    tracing::info!(%device_id, idle_ms = idle, "closing a silent connection");
                    return;
                }
                if let Err(e) = state.presence.heartbeat(&device_id.to_string()).await {
                    tracing::warn!(%device_id, error = ?e, "presence heartbeat failed");
                }
            }
        })
    };

    tokio::select! {
        _ = &mut writer => {}
        _ = &mut reader => {}
        _ = &mut heartbeat => {}
    }
    writer.abort();
    reader.abort();
    heartbeat.abort();

    // Only clean up what is still ours. If a newer connection of the same
    // device has replaced this one, the registry entry and the presence
    // record now belong to it and must be left alone.
    let was_current = state.registry.deregister(&device_id, conn_id);
    state.metrics.connections.dec();
    if was_current {
        let id = device_id.to_string();
        if let Ok(Some(owner)) = state.presence.lookup(&id).await {
            // ...and if the device has since connected to another worker,
            // that worker's presence record is not ours to delete either.
            if owner == state.config.instance_id {
                let _ = state.presence.deregister(&id).await;
            }
        }
    }
    tracing::info!(
        %device_id,
        connections = state.registry.connection_count(),
        "device disconnected"
    );
}
