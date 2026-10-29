//! Wire-level contracts shared by the coordinator and the fanout worker:
//! NATS subject names, WebSocket control frames and telemetry payloads.
//! Keeping them in one place means publisher and subscriber can never
//! drift on a subject name or a JSON shape.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Name of the JetStream stream that durably stores published alerts.
pub const ALERT_STREAM_NAME: &str = "ALERTS";
/// Prefix of the per-shard alert subjects: `alerts.geo.<shard_prefix>`.
pub const ALERT_SUBJECT_PREFIX: &str = "alerts.geo.";
/// Wildcard covering every alert subject (what the stream captures).
pub const ALERT_SUBJECT_WILDCARD: &str = "alerts.geo.>";
/// Prefix of per-device control subjects: `control.device.<device_id>`.
pub const CONTROL_SUBJECT_PREFIX: &str = "control.device.";
/// Wildcard every worker subscribes to for control messages.
pub const CONTROL_SUBJECT_WILDCARD: &str = "control.device.*";
/// Subject workers publish per-shard delivery telemetry on.
pub const TELEMETRY_DELIVERY_SUBJECT: &str = "telemetry.delivery";

pub fn alert_subject(shard_prefix: &str) -> String {
    format!("{ALERT_SUBJECT_PREFIX}{shard_prefix}")
}

pub fn control_subject(device_id: Uuid) -> String {
    format!("{CONTROL_SUBJECT_PREFIX}{device_id}")
}

/// Extracts the device id from a `control.device.<uuid>` subject.
pub fn device_id_from_control_subject(subject: &str) -> Option<Uuid> {
    subject
        .strip_prefix(CONTROL_SUBJECT_PREFIX)
        .and_then(|rest| Uuid::parse_str(rest).ok())
}

/// Control frames pushed to a device. They travel over NATS
/// (`control.device.<id>`) from the coordinator to the worker and are then
/// forwarded unchanged to the device as a WebSocket **text** frame.
/// (Alerts themselves are **binary** frames.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// The device moved into another shard. It must reconnect to `ws_url`
    /// (None if no worker has claimed the new shard yet).
    ShardChanged {
        shard_prefix: String,
        ws_url: Option<String>,
    },
    /// The device's access was revoked or the device was deregistered.
    Revoked,
    /// The device moved but is still in the same shard. The worker applies
    /// this to its own record of where the device is (so alerts are filtered
    /// by its real position) and does NOT forward it to the device.
    LocationUpdated { lat: f64, lon: f64 },
    /// Sent once after a reconnect replay finished.
    ReplayComplete { replayed: usize },
}

impl ServerMessage {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ServerMessage is always serializable")
    }
}

/// Frames a device may send to the worker (WebSocket text frames).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Acknowledges an alert. Echo the alert's `id` and `created_at`; the
    /// worker stores `created_at` as the device's replay cursor.
    Ack {
        alert_id: Uuid,
        created_at: DateTime<Utc>,
    },
}

/// What a worker reports after fanning one alert out to one shard.
/// Feeds the live dashboard and the per-alert stats.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeliveryTelemetry {
    pub alert_id: Uuid,
    pub shard_prefix: String,
    /// Sockets the alert was queued to.
    pub delivered: usize,
    /// Sockets skipped because their outbound queue was full.
    pub dropped: usize,
    /// Sockets in the shard that were outside the alert circle.
    pub filtered_out: usize,
    pub broadcast_duration_us: u64,
    pub worker_id: String,
    pub at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subjects_are_consistent() {
        assert_eq!(alert_subject("tkrt"), "alerts.geo.tkrt");
        let id = Uuid::new_v4();
        let subject = control_subject(id);
        assert_eq!(device_id_from_control_subject(&subject), Some(id));
        assert_eq!(device_id_from_control_subject("alerts.geo.tkrt"), None);
        assert_eq!(
            device_id_from_control_subject("control.device.not-a-uuid"),
            None
        );
    }

    #[test]
    fn server_message_json_shape() {
        let m = ServerMessage::ShardChanged {
            shard_prefix: "tkrw".into(),
            ws_url: Some("ws://h:1".into()),
        };
        let v: serde_json::Value = serde_json::from_str(&m.to_json()).unwrap();
        assert_eq!(v["type"], "shard_changed");
        assert_eq!(v["shard_prefix"], "tkrw");
        assert_eq!(ServerMessage::Revoked.to_json(), r#"{"type":"revoked"}"#);
        let back: ServerMessage = serde_json::from_str(&m.to_json()).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn location_updated_json_shape() {
        let m = ServerMessage::LocationUpdated {
            lat: 24.9,
            lon: 67.1,
        };
        let v: serde_json::Value = serde_json::from_str(&m.to_json()).unwrap();
        assert_eq!(v["type"], "location_updated");
        assert_eq!(v["lat"], 24.9);
        let back: ServerMessage = serde_json::from_str(&m.to_json()).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn client_ack_parses() {
        let raw = r#"{"type":"ack","alert_id":"29be0c7d-3a52-4b0e-9d1e-7f1f0c6b9a11","created_at":"2026-10-21T10:15:30Z"}"#;
        let m: ClientMessage = serde_json::from_str(raw).unwrap();
        assert!(matches!(m, ClientMessage::Ack { .. }));
        assert!(serde_json::from_str::<ClientMessage>(r#"{"type":"nope"}"#).is_err());
    }
}
