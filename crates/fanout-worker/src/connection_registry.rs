use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

/// Bounded channel capacity per connection. A slow/stalled client can
/// only ever back up 64 pending alert messages before we start
/// shedding -- this is the deliberate backpressure boundary: a single
/// slow WebSocket write must never be allowed to block the shared
/// broadcast loop for every other device in the shard.
const CONNECTION_CHANNEL_CAPACITY: usize = 64;

pub struct ConnectionHandle {
    pub shard_prefix: String,
    pub sender: mpsc::Sender<Vec<u8>>,
}

/// In-memory registry of every WebSocket connection this worker
/// instance currently holds, keyed by device ID. This is the "hot"
/// state the ripple-effect demo reads from -- purely local to this
/// process, rebuilt from nothing on restart (Redis's PresenceRegistry
/// from Phase 2 is the cross-instance lookup, this is the local fanout
/// target list).
#[derive(Clone)]
pub struct ConnectionRegistry {
    connections: Arc<DashMap<Uuid, ConnectionHandle>>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
        }
    }

    pub fn register(&self, device_id: Uuid, shard_prefix: String) -> mpsc::Receiver<Vec<u8>> {
        let (tx, rx) = mpsc::channel(CONNECTION_CHANNEL_CAPACITY);
        self.connections.insert(
            device_id,
            ConnectionHandle {
                shard_prefix,
                sender: tx,
            },
        );
        rx
    }

    pub fn deregister(&self, device_id: &Uuid) {
        self.connections.remove(device_id);
    }

    /// Fans a payload out to every connection currently registered under
    /// the given shard prefix. Uses `try_send` rather than `send().await`
    /// deliberately: this runs inside the NATS subscriber loop
    /// (subscriber.rs) shared across every device in the shard, so it
    /// must never await backpressure from one slow connection -- a full
    /// channel means that one device drops this alert rather than
    /// stalling delivery to everyone else. Returns how many connections
    /// were actually reached vs. how many were dropped, for the demo's
    /// live counter and for logging.
    pub fn broadcast_to_shard(&self, shard_prefix: &str, payload: &[u8]) -> (usize, usize) {
        let mut delivered = 0usize;
        let mut dropped = 0usize;

        for entry in self.connections.iter() {
            if entry.value().shard_prefix == shard_prefix {
                match entry.value().sender.try_send(payload.to_vec()) {
                    Ok(()) => delivered += 1,
                    Err(_) => dropped += 1,
                }
            }
        }

        (delivered, dropped)
    }

    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }
}