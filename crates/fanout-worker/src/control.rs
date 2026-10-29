use domain::device::Location;
use domain::protocol::{device_id_from_control_subject, CONTROL_SUBJECT_WILDCARD};
use domain::ServerMessage;
use futures_util::StreamExt;
use tokio::task::JoinHandle;

use crate::connection_registry::ConnectionRegistry;
use crate::WorkerState;
use std::time::Duration;

/// What happened to one control message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handled {
    /// Not for us, malformed, or not meant for devices.
    Ignored,
    /// A same-shard move: the worker's record of the device's position was updated.
    LocationApplied,
    /// Sent to the device (and the device was closed afterwards).
    Forwarded,
    /// The device is not connected to this worker.
    NotConnectedHere,
}

impl Handled {
    pub fn label(self) -> &'static str {
        match self {
            Handled::Ignored => "ignored",
            Handled::LocationApplied => "location_applied",
            Handled::Forwarded => "forwarded",
            Handled::NotConnectedHere => "not_connected_here",
        }
    }
}

/// Applies one `control.device.<id>` message. Every worker receives every
/// control message, but only the one holding the device's socket acts on it.
pub fn handle_control(registry: &ConnectionRegistry, subject: &str, payload: &[u8]) -> Handled {
    let Some(device_id) = device_id_from_control_subject(subject) else {
        return Handled::Ignored;
    };
    let Ok(message) = serde_json::from_slice::<ServerMessage>(payload) else {
        return Handled::Ignored;
    };

    match message {
        ServerMessage::LocationUpdated { lat, lon } => match Location::new(lat, lon) {
            Ok(location) if registry.update_location(&device_id, location) => {
                Handled::LocationApplied
            }
            Ok(_) => Handled::NotConnectedHere,
            Err(_) => Handled::Ignored,
        },
        // The device must act on these (reconnect elsewhere / stop), so send
        // the message to it and close the connection.
        ServerMessage::ShardChanged { .. } | ServerMessage::Revoked => {
            if registry.send_control(&device_id, message.to_json(), true) {
                Handled::Forwarded
            } else {
                Handled::NotConnectedHere
            }
        }
        // Produced by workers for devices, never meant to arrive here.
        ServerMessage::ReplayComplete { .. } => Handled::Ignored,
    }
}

/// Listens to every device's control subject and applies the messages.
pub fn spawn_control_subscriber(state: &WorkerState) -> JoinHandle<()> {
    let state = state.clone();
    tokio::spawn(async move {
        loop {
            match state
                .nats
                .subscribe(CONTROL_SUBJECT_WILDCARD.to_string())
                .await
            {
                Ok(mut subscription) => {
                    tracing::info!("subscribed to device control messages");
                    while let Some(message) = subscription.next().await {
                        let outcome =
                            handle_control(&state.registry, &message.subject, &message.payload);
                        state.metrics.control_messages.inc(outcome.label());
                    }
                    tracing::warn!("control subscription ended; resubscribing");
                }
                Err(e) => tracing::warn!(error = ?e, "could not subscribe to control messages"),
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_registry::Outbound;
    use domain::protocol::control_subject;
    use domain::{Alert, AlertSeverity};
    use uuid::Uuid;

    fn loc(lat: f64, lon: f64) -> Location {
        Location::new(lat, lon).unwrap()
    }

    #[test]
    fn revoked_and_shard_changed_are_forwarded_and_close_the_socket() {
        let reg = ConnectionRegistry::new();
        let device = Uuid::new_v4();
        let mut rx = reg.register(device, "tkrt", loc(24.86, 67.0)).receiver;
        let subject = control_subject(device);

        assert_eq!(
            handle_control(&reg, &subject, br#"{"type":"revoked"}"#),
            Handled::Forwarded
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            Outbound::Control {
                text: r#"{"type":"revoked"}"#.into(),
                close: true
            }
        );

        let moved = br#"{"type":"shard_changed","shard_prefix":"tkrw","ws_url":"ws://b:1"}"#;
        assert_eq!(handle_control(&reg, &subject, moved), Handled::Forwarded);
        match rx.try_recv().unwrap() {
            Outbound::Control { text, close } => {
                assert!(close);
                let v: serde_json::Value = serde_json::from_str(&text).unwrap();
                assert_eq!(v["type"], "shard_changed");
                assert_eq!(v["ws_url"], "ws://b:1");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn a_location_update_is_applied_but_never_sent_to_the_device() {
        let reg = ConnectionRegistry::new();
        let device = Uuid::new_v4();
        // ~28 km from the alert centre, so outside an 8 km circle at first
        let mut rx = reg.register(device, "tkrt", loc(25.11, 67.0011)).receiver;
        let alert = Alert::new(
            loc(24.8607, 67.0011),
            8_000.0,
            AlertSeverity::Info,
            "t".into(),
            "m".into(),
        )
        .unwrap();
        let payload = bytes::Bytes::from(serde_json::to_vec(&alert).unwrap());
        assert_eq!(
            reg.broadcast_to_shard("tkrt", &alert, &payload).delivered,
            0
        );

        let outcome = handle_control(
            &reg,
            &control_subject(device),
            br#"{"type":"location_updated","lat":24.8607,"lon":67.0011}"#,
        );
        assert_eq!(outcome, Handled::LocationApplied);
        assert!(rx.try_recv().is_err(), "nothing is sent to the device");
        assert_eq!(
            reg.broadcast_to_shard("tkrt", &alert, &payload).delivered,
            1
        );
    }

    #[test]
    fn messages_for_devices_elsewhere_or_malformed_are_harmless() {
        let reg = ConnectionRegistry::new();
        let elsewhere = control_subject(Uuid::new_v4());
        assert_eq!(
            handle_control(&reg, &elsewhere, br#"{"type":"revoked"}"#),
            Handled::NotConnectedHere
        );
        assert_eq!(
            handle_control(
                &reg,
                &elsewhere,
                br#"{"type":"location_updated","lat":1.0,"lon":2.0}"#
            ),
            Handled::NotConnectedHere
        );
        assert_eq!(
            handle_control(&reg, &elsewhere, b"garbage"),
            Handled::Ignored
        );
        assert_eq!(
            handle_control(&reg, "control.device.not-a-uuid", br#"{"type":"revoked"}"#),
            Handled::Ignored
        );
        assert_eq!(
            handle_control(
                &reg,
                &elsewhere,
                br#"{"type":"replay_complete","replayed":1}"#
            ),
            Handled::Ignored
        );
        // an impossible position is ignored, not applied
        let device = Uuid::new_v4();
        reg.register(device, "tkrt", loc(24.86, 67.0));
        assert_eq!(
            handle_control(
                &reg,
                &control_subject(device),
                br#"{"type":"location_updated","lat":99.0,"lon":0.0}"#
            ),
            Handled::Ignored
        );
    }
}
