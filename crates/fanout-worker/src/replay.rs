//! Durable delivery for devices that were offline.
//!
//! Alerts are stored in JetStream. When a device connects, the worker looks
//! up the device's *cursor* (the newest alert it acknowledged), reads the
//! alerts stored since then for the device's shard, keeps the ones whose
//! circle contains the device, and sends them. The device moves its cursor
//! forward by sending `{"type":"ack","alert_id":...,"created_at":...}`.
//!
//! Delivery is therefore at-least-once: during a reconnect an alert can
//! arrive both live and by replay, so clients must de-duplicate by `id`.
//! The overlap is deliberate: with clocks that disagree, a duplicate is
//! acceptable and a lost alert is not.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use domain::device::Location;
use domain::{Alert, ServerMessage};
use infra::metrics::{Counter, Registry};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::connection_registry::Outbound;
use crate::WorkerState;

/// How long a device's cursor is remembered.
pub const ACK_CURSOR_TTL_SECS: u64 = 7 * 24 * 3600;
/// At most one cursor write per connection per this interval.
pub const ACK_MIN_INTERVAL: Duration = Duration::from_secs(1);
/// How many stored alerts one replay reads from the stream.
const MAX_STREAM_READ: usize = 500;
/// How long the replay task waits for room in a device's queue.
const SEND_WAIT: Duration = Duration::from_secs(10);

/// A device with no cursor (new, or one that never acknowledges) is sent the
/// alerts of the last five minutes.
fn no_cursor_window() -> ChronoDuration {
    ChronoDuration::seconds(300)
}

/// Alerts created up to this long after the connection was made are still
/// replayed, in case the worker's and the coordinator's clocks disagree.
fn replay_overlap() -> ChronoDuration {
    ChronoDuration::seconds(5)
}

/// An acknowledgement claiming to be from further in the future than this is
/// ignored, so a device cannot push its own cursor past real alerts.
fn future_ack_tolerance() -> ChronoDuration {
    ChronoDuration::seconds(60)
}

/// Counters for replay and acknowledgements.
pub struct ReplayMetrics {
    pub replays: Arc<Counter>,
    pub replayed_alerts: Arc<Counter>,
    pub replay_failures: Arc<Counter>,
    pub acks: Arc<Counter>,
    pub acks_ignored: Arc<Counter>,
    pub bad_messages: Arc<Counter>,
}

impl ReplayMetrics {
    pub fn new(registry: &Registry) -> Arc<Self> {
        Arc::new(Self {
            replays: registry.counter(
                "pulsegrid_worker_replays_total",
                "Reconnect replays that ran to the end.",
            ),
            replayed_alerts: registry.counter(
                "pulsegrid_worker_replayed_alerts_total",
                "Missed alerts sent to reconnecting devices.",
            ),
            replay_failures: registry.counter(
                "pulsegrid_worker_replay_failures_total",
                "Reconnect replays that could not complete.",
            ),
            acks: registry.counter(
                "pulsegrid_worker_acks_total",
                "Acknowledgements accepted from devices.",
            ),
            acks_ignored: registry.counter(
                "pulsegrid_worker_acks_ignored_total",
                "Acknowledgements ignored (timestamp too far in the future).",
            ),
            bad_messages: registry.counter(
                "pulsegrid_worker_bad_client_messages_total",
                "Text frames from devices that were not understood.",
            ),
        })
    }
}

/// What to do with one acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    /// Not acceptable (timestamp too far in the future).
    Ignored,
    /// Accepted and remembered; it will be written with a later one.
    Buffered,
    /// Accepted: write this value as the device's cursor now.
    Write(DateTime<Utc>),
}

/// Limits how often a connection writes its cursor to Redis. A chatty client
/// that acknowledges every alert costs one write per interval, not one per
/// alert; the newest value always wins and the cursor never moves backwards.
pub struct AckBatcher {
    min_interval: Duration,
    last_write: Option<Instant>,
    pending: Option<DateTime<Utc>>,
}

impl AckBatcher {
    pub fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            last_write: None,
            pending: None,
        }
    }

    /// `ts` is the acknowledged alert's creation time, `wall_now` the current
    /// time and `now` a monotonic clock reading.
    pub fn offer(
        &mut self,
        ts: DateTime<Utc>,
        wall_now: DateTime<Utc>,
        now: Instant,
    ) -> AckOutcome {
        if ts > wall_now + future_ack_tolerance() {
            return AckOutcome::Ignored;
        }
        let best = match self.pending {
            Some(pending) if pending >= ts => pending,
            _ => ts,
        };
        self.pending = Some(best);

        let due = match self.last_write {
            Some(last) => now.duration_since(last) >= self.min_interval,
            None => true,
        };
        if due {
            self.last_write = Some(now);
            match self.pending.take() {
                Some(value) => AckOutcome::Write(value),
                None => AckOutcome::Buffered,
            }
        } else {
            AckOutcome::Buffered
        }
    }

    /// What is still waiting to be written (call when the connection ends).
    pub fn take_pending(&mut self) -> Option<DateTime<Utc>> {
        self.pending.take()
    }
}

/// Chooses what to send from the stored alerts (oldest first): only alerts
/// whose circle contains the device and that were not created long after the
/// connection was made (those arrive live). If more than `limit` remain, the
/// newest `limit` are kept. Returns the serialized alerts, oldest first.
pub fn plan_replay(
    alerts: Vec<Alert>,
    location: &Location,
    connected_at: DateTime<Utc>,
    limit: usize,
) -> Vec<Bytes> {
    let upper = connected_at + replay_overlap();
    let mut payloads: Vec<Bytes> = alerts
        .into_iter()
        .filter(|alert| alert.created_at < upper && alert.contains(location))
        .filter_map(|alert| serde_json::to_vec(&alert).ok().map(Bytes::from))
        .collect();
    let skip = payloads.len().saturating_sub(limit);
    payloads.drain(..skip);
    payloads
}

/// Replays the alerts a device missed and tells it when that is done. Runs as
/// its own task; it ends quietly if the connection goes away.
pub async fn run_replay(
    state: WorkerState,
    device_id: Uuid,
    shard_prefix: String,
    location: Location,
    connected_at: DateTime<Utc>,
    sender: mpsc::Sender<Outbound>,
) {
    let since = match state.ack_cursors.get(device_id).await {
        Ok(Some(cursor)) => cursor,
        Ok(None) => connected_at - no_cursor_window(),
        Err(e) => {
            state.replay_metrics.replay_failures.inc();
            tracing::warn!(%device_id, error = ?e, "could not read the replay cursor");
            return;
        }
    };

    let stored = match infra::nats::replay_alerts(
        &state.nats,
        &shard_prefix,
        since,
        MAX_STREAM_READ,
    )
    .await
    {
        Ok(alerts) => alerts,
        Err(e) => {
            state.replay_metrics.replay_failures.inc();
            tracing::warn!(%device_id, error = ?e, "could not read stored alerts for replay");
            return;
        }
    };

    let payloads = plan_replay(
        stored,
        &location,
        connected_at,
        state.config.replay_max_alerts,
    );
    let mut replayed = 0usize;
    for payload in payloads {
        match tokio::time::timeout(SEND_WAIT, sender.send(Outbound::Alert(payload))).await {
            Ok(Ok(())) => replayed += 1,
            // the connection is gone, or the device is not reading
            _ => {
                state.replay_metrics.replay_failures.inc();
                return;
            }
        }
    }

    let done = ServerMessage::ReplayComplete { replayed }.to_json();
    let _ = sender
        .send(Outbound::Control {
            text: done,
            close: false,
        })
        .await;
    state.replay_metrics.replays.inc();
    state.replay_metrics.replayed_alerts.add(replayed as u64);
    if replayed > 0 {
        tracing::info!(%device_id, replayed, "replayed missed alerts");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_registry::ConnectionRegistry;
    use domain::AlertSeverity;

    fn loc(lat: f64, lon: f64) -> Location {
        Location::new(lat, lon).unwrap()
    }

    fn alert_at(lat: f64, lon: f64, created_at: DateTime<Utc>, title: &str) -> Alert {
        let mut alert = Alert::new(
            loc(lat, lon),
            8_000.0,
            AlertSeverity::Critical,
            title.into(),
            "m".into(),
        )
        .unwrap();
        alert.created_at = created_at;
        alert
    }

    fn titles(payloads: &[Bytes]) -> Vec<String> {
        payloads
            .iter()
            .map(|p| serde_json::from_slice::<Alert>(p).unwrap().title)
            .collect()
    }

    #[test]
    fn replay_keeps_alerts_inside_the_circle_and_not_from_the_future() {
        let connected_at = Utc::now();
        let device = loc(24.8607, 67.0011);
        let alerts = vec![
            alert_at(
                24.8607,
                67.0011,
                connected_at - ChronoDuration::minutes(3),
                "inside-old",
            ),
            // ~28 km away: outside an 8 km circle
            alert_at(
                25.1100,
                67.0011,
                connected_at - ChronoDuration::minutes(2),
                "outside",
            ),
            alert_at(
                24.8650,
                67.0050,
                connected_at + ChronoDuration::seconds(3),
                "inside-overlap",
            ),
            // created well after connecting: it arrives live, not by replay
            alert_at(
                24.8607,
                67.0011,
                connected_at + ChronoDuration::seconds(60),
                "inside-live",
            ),
        ];

        let payloads = plan_replay(alerts, &device, connected_at, 50);
        assert_eq!(titles(&payloads), ["inside-old", "inside-overlap"]);
    }

    #[test]
    fn replay_keeps_the_newest_when_there_are_too_many() {
        let connected_at = Utc::now();
        let alerts: Vec<Alert> = (0..5)
            .map(|i| {
                alert_at(
                    24.8607,
                    67.0011,
                    connected_at - ChronoDuration::minutes(10 - i),
                    &format!("a{i}"),
                )
            })
            .collect();

        let payloads = plan_replay(alerts, &loc(24.8607, 67.0011), connected_at, 2);
        assert_eq!(
            titles(&payloads),
            ["a3", "a4"],
            "the newest two, oldest first"
        );
    }

    #[test]
    fn replayed_payloads_are_the_alerts_own_json() {
        let connected_at = Utc::now();
        let alert = alert_at(
            24.8607,
            67.0011,
            connected_at - ChronoDuration::minutes(1),
            "x",
        );
        let expected = serde_json::to_vec(&alert).unwrap();
        let payloads = plan_replay(vec![alert], &loc(24.8607, 67.0011), connected_at, 10);
        assert_eq!(payloads, [Bytes::from(expected)]);
    }

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_800_000_000 + secs, 0).unwrap()
    }

    #[test]
    fn the_first_ack_is_written_immediately() {
        let mut b = AckBatcher::new(Duration::from_secs(1));
        let now = Instant::now();
        assert_eq!(b.offer(t(10), t(20), now), AckOutcome::Write(t(10)));
        assert_eq!(b.take_pending(), None);
    }

    #[test]
    fn acks_inside_the_interval_are_buffered_and_the_latest_wins() {
        let mut b = AckBatcher::new(Duration::from_secs(1));
        let start = Instant::now();
        assert_eq!(b.offer(t(10), t(20), start), AckOutcome::Write(t(10)));
        assert_eq!(
            b.offer(t(11), t(20), start + Duration::from_millis(100)),
            AckOutcome::Buffered
        );
        assert_eq!(
            b.offer(t(13), t(20), start + Duration::from_millis(200)),
            AckOutcome::Buffered
        );
        assert_eq!(
            b.offer(t(12), t(20), start + Duration::from_millis(300)),
            AckOutcome::Buffered
        );
        // after the interval the highest buffered value is written
        assert_eq!(
            b.offer(t(14), t(20), start + Duration::from_millis(1_200)),
            AckOutcome::Write(t(14))
        );
    }

    #[test]
    fn an_older_ack_never_moves_the_pending_value_backwards() {
        let mut b = AckBatcher::new(Duration::from_secs(1));
        let start = Instant::now();
        b.offer(t(10), t(20), start);
        b.offer(t(15), t(20), start + Duration::from_millis(10));
        b.offer(t(12), t(20), start + Duration::from_millis(20));
        assert_eq!(b.take_pending(), Some(t(15)));
    }

    #[test]
    fn an_ack_from_the_future_is_ignored() {
        let mut b = AckBatcher::new(Duration::from_secs(1));
        let now = Instant::now();
        assert_eq!(b.offer(t(1_000), t(20), now), AckOutcome::Ignored);
        assert_eq!(b.take_pending(), None);
        // slightly ahead (clock drift) is fine
        assert_eq!(b.offer(t(50), t(20), now), AckOutcome::Write(t(50)));
    }

    #[test]
    fn what_is_still_pending_can_be_taken_when_the_connection_ends() {
        let mut b = AckBatcher::new(Duration::from_secs(1));
        let start = Instant::now();
        b.offer(t(10), t(20), start);
        assert_eq!(
            b.offer(t(11), t(20), start + Duration::from_millis(10)),
            AckOutcome::Buffered
        );
        assert_eq!(b.take_pending(), Some(t(11)));
        assert_eq!(b.take_pending(), None);
    }

    #[test]
    fn a_sender_is_only_given_for_the_current_connection() {
        let registry = ConnectionRegistry::new();
        let device = Uuid::new_v4();
        let first = registry.register(device, "tkrt", loc(24.86, 67.0));
        assert!(registry.sender_for(&device, first.conn_id).is_some());
        assert!(registry.sender_for(&device, first.conn_id + 100).is_none());
        assert!(registry
            .sender_for(&Uuid::new_v4(), first.conn_id)
            .is_none());

        let second = registry.register(device, "tkrt", loc(24.86, 67.0));
        assert!(
            registry.sender_for(&device, first.conn_id).is_none(),
            "the replaced connection no longer gets one"
        );
        assert!(registry.sender_for(&device, second.conn_id).is_some());

        registry.deregister(&device, second.conn_id);
        assert!(registry.sender_for(&device, second.conn_id).is_none());
    }
}
