use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::device::Location;
use crate::errors::AppError;
use crate::geo;
use crate::protocol;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AlertId(pub Uuid);

impl AlertId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for AlertId {
    fn default() -> Self {
        Self::new()
    }
}

/// Serialized in lowercase everywhere (API request, NATS payload,
/// WebSocket push) so clients see one consistent spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl AlertSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertSeverity::Info => "info",
            AlertSeverity::Warning => "warning",
            AlertSeverity::Critical => "critical",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "info" => Some(AlertSeverity::Info),
            "warning" => Some(AlertSeverity::Warning),
            "critical" => Some(AlertSeverity::Critical),
            _ => None,
        }
    }
}

/// A dispatchable alert: a center point, a radius, and a payload. This is
/// the domain-level shape -- api-coordinator turns this into one NATS
/// publish per shard prefix returned by `target_shard_prefixes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub id: AlertId,
    pub center: Location,
    pub radius_meters: f64,
    pub severity: AlertSeverity,
    pub title: String,
    pub message: String,
    pub created_at: DateTime<Utc>,
}

impl Alert {
    pub fn new(
        center: Location,
        radius_meters: f64,
        severity: AlertSeverity,
        title: String,
        message: String,
    ) -> Result<Self, AppError> {
        if radius_meters <= 0.0 || radius_meters > 500_000.0 {
            return Err(AppError::InvalidRadius(radius_meters));
        }
        if title.trim().is_empty() {
            return Err(AppError::Validation("alert title cannot be empty".into()));
        }
        Ok(Self {
            id: AlertId::new(),
            center,
            radius_meters,
            severity,
            title,
            message,
            created_at: Utc::now(),
        })
    }

    /// Resolves this alert to the concrete set of shard prefixes it needs
    /// to be published to.
    pub fn target_shard_prefixes(&self) -> Result<Vec<String>, AppError> {
        geo::shard_prefixes_for_radius(&self.center, self.radius_meters)
    }

    /// Whether a point lies inside the alert circle (great-circle distance
    /// <= radius). The fanout worker uses this to deliver to the circle
    /// itself instead of the whole shard cell.
    pub fn contains(&self, point: &Location) -> bool {
        geo::distance_meters(&self.center, point) <= self.radius_meters
    }

    /// The NATS subject an alert is published to for a given shard prefix.
    pub fn nats_subject_for_prefix(prefix: &str) -> String {
        protocol::alert_subject(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn karachi() -> Location {
        Location::new(24.8607, 67.0011).unwrap()
    }

    #[test]
    fn severity_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&AlertSeverity::Critical).unwrap(),
            "\"critical\""
        );
        let parsed: AlertSeverity = serde_json::from_str("\"warning\"").unwrap();
        assert_eq!(parsed, AlertSeverity::Warning);
        assert!(serde_json::from_str::<AlertSeverity>("\"Critical\"").is_err());
    }

    #[test]
    fn severity_string_helpers_roundtrip() {
        for s in [
            AlertSeverity::Info,
            AlertSeverity::Warning,
            AlertSeverity::Critical,
        ] {
            assert_eq!(AlertSeverity::parse(s.as_str()), Some(s));
        }
        assert_eq!(AlertSeverity::parse("nope"), None);
    }

    #[test]
    fn new_validates_radius_and_title() {
        let c = karachi();
        assert!(Alert::new(c, 0.0, AlertSeverity::Info, "t".into(), "m".into()).is_err());
        assert!(Alert::new(c, 500_001.0, AlertSeverity::Info, "t".into(), "m".into()).is_err());
        assert!(Alert::new(c, 100.0, AlertSeverity::Info, "  ".into(), "m".into()).is_err());
        assert!(Alert::new(c, 100.0, AlertSeverity::Info, "t".into(), "m".into()).is_ok());
    }

    #[test]
    fn contains_respects_radius() {
        let alert = Alert::new(
            karachi(),
            5_000.0,
            AlertSeverity::Warning,
            "t".into(),
            "m".into(),
        )
        .unwrap();
        assert!(alert.contains(&karachi()));
        // ~2.2 km north
        assert!(alert.contains(&Location::new(24.8807, 67.0011).unwrap()));
        // ~11 km north
        assert!(!alert.contains(&Location::new(24.9607, 67.0011).unwrap()));
    }

    #[test]
    fn payload_json_shape() {
        let alert = Alert::new(
            karachi(),
            5_000.0,
            AlertSeverity::Critical,
            "t".into(),
            "m".into(),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::to_value(&alert).unwrap();
        assert_eq!(v["severity"], "critical");
        assert!(v["id"].is_string());
        assert_eq!(v["radius_meters"], 5000.0);
        assert_eq!(Alert::nats_subject_for_prefix("tkrt"), "alerts.geo.tkrt");
    }
}
