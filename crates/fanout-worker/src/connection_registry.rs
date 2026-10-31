use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use dashmap::DashMap;
use domain::device::Location;
use domain::Alert;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use uuid::Uuid;

/// Bounded channel capacity per connection. A slow or stalled client can only
/// ever back up 64 pending messages before we start shedding -- the deliberate
/// backpressure boundary: one slow WebSocket write must never be allowed to
/// block the shared broadcast loop for every other device in the shard.
pub const CONNECTION_CHANNEL_CAPACITY: usize = 64;

/// What a connection's writer task should send to its device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outbound {
    /// A serialized alert (a binary frame). The `Bytes` is shared by every
    /// connection it is sent to, so fanning out does not copy the payload.
    Alert(Bytes),
    /// A text frame. When `close` is true the connection is closed after it.
    Control { text: String, close: bool },
    /// Close the connection.
    Close,
}

/// What a broadcast did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BroadcastResult {
    /// Sockets the alert was queued to.
    pub delivered: usize,
    /// Sockets skipped because their queue was full (or already closing).
    pub dropped: usize,
    /// Sockets in the shard that are outside the alert circle.
    pub filtered_out: usize,
}

struct Connection {
    id: u64,
    shard: String,
    location: RwLock<Location>,
    sender: mpsc::Sender<Outbound>,
}

impl Connection {
    fn location(&self) -> Location {
        *self.location.read().unwrap_or_else(|e| e.into_inner())
    }
}

/// The receiving half handed to a connection's writer task.
pub struct Registered {
    /// Identifies this particular connection of the device. Pass it to
    /// `deregister`, so an old connection closing can never remove a newer
    /// connection of the same device.
    pub conn_id: u64,
    pub receiver: mpsc::Receiver<Outbound>,
}

#[derive(Default)]
struct Inner {
    by_device: DashMap<Uuid, Arc<Connection>>,
    by_shard: DashMap<String, DashMap<Uuid, Arc<Connection>>>,
    next_id: AtomicU64,
}

/// Every WebSocket connection this worker holds, indexed by device and by
/// shard, so an alert only touches the sockets of the shard it targets.
/// Purely local to this process; Redis holds the cross-worker view.
#[derive(Clone, Default)]
pub struct ConnectionRegistry {
    inner: Arc<Inner>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a connection. If the device already has one, the old one is
    /// told to close and replaced.
    pub fn register(&self, device_id: Uuid, shard_prefix: &str, location: Location) -> Registered {
        let (sender, receiver) = mpsc::channel(CONNECTION_CHANNEL_CAPACITY);
        let conn = Arc::new(Connection {
            id: self.inner.next_id.fetch_add(1, Ordering::Relaxed) + 1,
            shard: shard_prefix.to_string(),
            location: RwLock::new(location),
            sender,
        });
        let conn_id = conn.id;

        if let Some(previous) = self.inner.by_device.insert(device_id, conn.clone()) {
            let _ = previous.sender.try_send(Outbound::Close);
            if let Some(shard) = self.inner.by_shard.get(&previous.shard) {
                shard.remove_if(&device_id, |_, c| c.id == previous.id);
            }
        }
        self.inner
            .by_shard
            .entry(shard_prefix.to_string())
            .or_default()
            .insert(device_id, conn);

        Registered { conn_id, receiver }
    }

    /// Removes the connection, but only if it is still the one identified by
    /// `conn_id`. Returns whether anything was removed.
    pub fn deregister(&self, device_id: &Uuid, conn_id: u64) -> bool {
        let removed = self
            .inner
            .by_device
            .remove_if(device_id, |_, c| c.id == conn_id);
        match removed {
            Some((_, conn)) => {
                if let Some(shard) = self.inner.by_shard.get(&conn.shard) {
                    shard.remove_if(device_id, |_, c| c.id == conn_id);
                }
                true
            }
            None => false,
        }
    }

    /// Queues `payload` to every connection of `shard_prefix` whose device is
    /// inside the alert circle. Uses `try_send` on purpose: this runs inside
    /// the shared NATS subscriber loop, so it must never wait for one slow
    /// connection -- a full queue means that one device misses this alert.
    pub fn broadcast_to_shard(
        &self,
        shard_prefix: &str,
        alert: &Alert,
        payload: &Bytes,
    ) -> BroadcastResult {
        let mut result = BroadcastResult::default();
        let Some(shard) = self.inner.by_shard.get(shard_prefix) else {
            return result;
        };
        for entry in shard.iter() {
            let conn = entry.value();
            if !alert.contains(&conn.location()) {
                result.filtered_out += 1;
                continue;
            }
            match conn.sender.try_send(Outbound::Alert(payload.clone())) {
                Ok(()) => result.delivered += 1,
                Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => result.dropped += 1,
            }
        }
        result
    }

    /// Records where a connected device now is. Returns whether the device is
    /// connected to this worker.
    pub fn update_location(&self, device_id: &Uuid, location: Location) -> bool {
        match self.inner.by_device.get(device_id) {
            Some(conn) => {
                *conn.location.write().unwrap_or_else(|e| e.into_inner()) = location;
                true
            }
            None => false,
        }
    }

    /// Sends a text frame to a connected device, optionally closing it
    /// afterwards. If its queue is full a closing request still takes effect
    /// (the connection is dropped). Returns whether the device is here.
    pub fn send_control(&self, device_id: &Uuid, text: String, close: bool) -> bool {
        let Some(conn) = self.inner.by_device.get(device_id).map(|c| c.clone()) else {
            return false;
        };
        let sent = conn.sender.try_send(Outbound::Control { text, close });
        if sent.is_err() && close {
            self.deregister(device_id, conn.id);
        }
        true
    }

    /// A sender for the connection identified by `conn_id`, for tasks that
    /// need to write to it from outside its writer task (the replay task
    /// waits for room instead of dropping messages). None if that connection
    /// has since gone away or been replaced.
    pub fn sender_for(&self, device_id: &Uuid, conn_id: u64) -> Option<mpsc::Sender<Outbound>> {
        self.inner
            .by_device
            .get(device_id)
            .filter(|c| c.id == conn_id)
            .map(|c| c.sender.clone())
    }

    /// Closes every connection (used when the worker shuts down).
    pub fn close_all(&self) {
        for entry in self.inner.by_device.iter() {
            let _ = entry.value().sender.try_send(Outbound::Close);
        }
        self.inner.by_device.clear();
        self.inner.by_shard.clear();
    }

    pub fn connection_count(&self) -> usize {
        self.inner.by_device.len()
    }

    pub fn shard_connection_count(&self, shard_prefix: &str) -> usize {
        self.inner
            .by_shard
            .get(shard_prefix)
            .map(|s| s.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::AlertSeverity;

    const KARACHI: (f64, f64) = (24.8607, 67.0011);

    fn loc(lat: f64, lon: f64) -> Location {
        Location::new(lat, lon).unwrap()
    }

    fn alert(radius: f64) -> (Alert, Bytes) {
        let alert = Alert::new(
            loc(KARACHI.0, KARACHI.1),
            radius,
            AlertSeverity::Critical,
            "t".into(),
            "m".into(),
        )
        .unwrap();
        let bytes = Bytes::from(serde_json::to_vec(&alert).unwrap());
        (alert, bytes)
    }

    fn drain(rx: &mut mpsc::Receiver<Outbound>) -> Vec<Outbound> {
        let mut out = Vec::new();
        while let Ok(m) = rx.try_recv() {
            out.push(m);
        }
        out
    }

    #[test]
    fn broadcast_reaches_only_the_shard_and_only_inside_the_circle() {
        let reg = ConnectionRegistry::new();
        let (near_a, near_b, far, other_shard) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        let mut rx_near_a = reg.register(near_a, "tkrt", loc(24.8607, 67.0011)).receiver;
        let mut rx_near_b = reg.register(near_b, "tkrt", loc(24.8700, 67.0100)).receiver;
        // ~28 km north: same shard, outside an 8 km circle
        let mut rx_far = reg.register(far, "tkrt", loc(25.1100, 67.0011)).receiver;
        let mut rx_other = reg
            .register(other_shard, "tkrw", loc(24.8607, 67.0011))
            .receiver;

        let (alert, payload) = alert(8_000.0);
        let result = reg.broadcast_to_shard("tkrt", &alert, &payload);

        assert_eq!(
            result,
            BroadcastResult {
                delivered: 2,
                dropped: 0,
                filtered_out: 1
            }
        );
        assert_eq!(drain(&mut rx_near_a), [Outbound::Alert(payload.clone())]);
        assert_eq!(drain(&mut rx_near_b), [Outbound::Alert(payload.clone())]);
        assert!(drain(&mut rx_far).is_empty(), "outside the circle");
        assert!(drain(&mut rx_other).is_empty(), "other shard is untouched");

        // an unknown shard is simply empty
        assert_eq!(
            reg.broadcast_to_shard("zzzz", &alert, &payload),
            BroadcastResult::default()
        );
    }

    #[test]
    fn the_payload_is_shared_not_copied() {
        let reg = ConnectionRegistry::new();
        let mut a = reg
            .register(Uuid::new_v4(), "tkrt", loc(KARACHI.0, KARACHI.1))
            .receiver;
        let mut b = reg
            .register(Uuid::new_v4(), "tkrt", loc(KARACHI.0, KARACHI.1))
            .receiver;
        let (alert, payload) = alert(5_000.0);
        reg.broadcast_to_shard("tkrt", &alert, &payload);

        let (Some(Outbound::Alert(pa)), Some(Outbound::Alert(pb))) =
            (a.try_recv().ok(), b.try_recv().ok())
        else {
            panic!("both devices should have received the alert");
        };
        assert_eq!(pa.as_ptr(), pb.as_ptr());
        assert_eq!(pa.as_ptr(), payload.as_ptr());
    }

    #[test]
    fn a_full_queue_drops_for_that_device_only() {
        let reg = ConnectionRegistry::new();
        let _slow = reg.register(Uuid::new_v4(), "tkrt", loc(KARACHI.0, KARACHI.1));
        let mut healthy = reg
            .register(Uuid::new_v4(), "tkrt", loc(KARACHI.0, KARACHI.1))
            .receiver;
        let (alert, payload) = alert(5_000.0);

        let mut total = BroadcastResult::default();
        for _ in 0..CONNECTION_CHANNEL_CAPACITY + 1 {
            let r = reg.broadcast_to_shard("tkrt", &alert, &payload);
            total.delivered += r.delivered;
            total.dropped += r.dropped;
            // the healthy device keeps reading
            drain(&mut healthy);
        }
        // the slow one accepted 64 and then dropped the 65th; the healthy one got all 65
        assert_eq!(total.dropped, 1);
        assert_eq!(total.delivered, 2 * (CONNECTION_CHANNEL_CAPACITY + 1) - 1);
    }

    #[test]
    fn a_new_connection_replaces_the_old_one_and_the_old_cleanup_is_harmless() {
        let reg = ConnectionRegistry::new();
        let device = Uuid::new_v4();
        let mut first = reg.register(device, "tkrt", loc(KARACHI.0, KARACHI.1));
        let mut second = reg.register(device, "tkrt", loc(KARACHI.0, KARACHI.1));

        // the old connection was told to close
        assert_eq!(drain(&mut first.receiver), [Outbound::Close]);
        assert_eq!(reg.connection_count(), 1);
        assert_eq!(reg.shard_connection_count("tkrt"), 1);

        // the OLD connection finishing its cleanup must not remove the NEW one
        assert!(!reg.deregister(&device, first.conn_id));
        assert_eq!(reg.connection_count(), 1);

        let (alert, payload) = alert(5_000.0);
        assert_eq!(
            reg.broadcast_to_shard("tkrt", &alert, &payload).delivered,
            1
        );
        assert_eq!(drain(&mut second.receiver), [Outbound::Alert(payload)]);

        // the new one cleans up normally
        assert!(reg.deregister(&device, second.conn_id));
        assert_eq!(reg.connection_count(), 0);
        assert_eq!(reg.shard_connection_count("tkrt"), 0);
    }

    #[test]
    fn replacing_across_shards_cleans_the_old_index() {
        let reg = ConnectionRegistry::new();
        let device = Uuid::new_v4();
        reg.register(device, "tkrt", loc(KARACHI.0, KARACHI.1));
        reg.register(device, "tkrw", loc(24.9, 67.5));
        assert_eq!(reg.shard_connection_count("tkrt"), 0);
        assert_eq!(reg.shard_connection_count("tkrw"), 1);
        assert_eq!(reg.connection_count(), 1);
    }

    #[test]
    fn moving_a_device_changes_who_gets_the_alert() {
        let reg = ConnectionRegistry::new();
        let device = Uuid::new_v4();
        let mut rx = reg
            .register(device, "tkrt", loc(25.1100, 67.0011)) // ~28 km away
            .receiver;
        let (alert, payload) = alert(8_000.0);

        assert_eq!(
            reg.broadcast_to_shard("tkrt", &alert, &payload).delivered,
            0
        );
        assert!(reg.update_location(&device, loc(KARACHI.0, KARACHI.1)));
        assert_eq!(
            reg.broadcast_to_shard("tkrt", &alert, &payload).delivered,
            1
        );
        assert_eq!(drain(&mut rx).len(), 1);
        assert!(!reg.update_location(&Uuid::new_v4(), loc(1.0, 1.0)));
    }

    #[test]
    fn control_messages_reach_the_device_and_can_close_it() {
        let reg = ConnectionRegistry::new();
        let device = Uuid::new_v4();
        let mut rx = reg
            .register(device, "tkrt", loc(KARACHI.0, KARACHI.1))
            .receiver;

        assert!(reg.send_control(&device, "{\"type\":\"revoked\"}".into(), true));
        assert_eq!(
            drain(&mut rx),
            [Outbound::Control {
                text: "{\"type\":\"revoked\"}".into(),
                close: true
            }]
        );
        assert!(!reg.send_control(&Uuid::new_v4(), "x".into(), true));
    }

    #[test]
    fn closing_a_device_with_a_full_queue_still_closes_it() {
        let reg = ConnectionRegistry::new();
        let device = Uuid::new_v4();
        let mut stuck = reg.register(device, "tkrt", loc(KARACHI.0, KARACHI.1));
        let (alert, payload) = alert(5_000.0);
        for _ in 0..CONNECTION_CHANNEL_CAPACITY {
            reg.broadcast_to_shard("tkrt", &alert, &payload);
        }

        assert!(reg.send_control(&device, "bye".into(), true));
        // the connection was dropped from the registry, so its queue ends
        assert_eq!(reg.connection_count(), 0);
        let mut seen = 0;
        while stuck.receiver.try_recv().is_ok() {
            seen += 1;
        }
        assert_eq!(seen, CONNECTION_CHANNEL_CAPACITY);
        assert!(matches!(
            stuck.receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn close_all_closes_everything() {
        let reg = ConnectionRegistry::new();
        let mut a = reg
            .register(Uuid::new_v4(), "tkrt", loc(KARACHI.0, KARACHI.1))
            .receiver;
        let mut b = reg
            .register(Uuid::new_v4(), "tkrw", loc(24.9, 67.5))
            .receiver;
        reg.close_all();
        assert_eq!(reg.connection_count(), 0);
        assert_eq!(drain(&mut a), [Outbound::Close]);
        assert_eq!(drain(&mut b), [Outbound::Close]);
    }
}
