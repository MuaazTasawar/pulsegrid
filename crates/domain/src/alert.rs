use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::device::Location;
use crate::errors::AppError;
use crate::geo;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
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
    /// to be published to. Kept on the Alert type so callers in
    /// api-coordinator don't need to import `geo` directly for the common
    /// case.
    pub fn target_shard_prefixes(&self) -> Result<Vec<String>, AppError> {
        geo::shard_prefixes_for_radius(&self.center, self.radius_meters)
    }

    /// The NATS subject an alert is published to for a given shard prefix.
    /// Centralized here so the coordinator (publisher, Phase 4) and
    /// fanout-worker (subscriber, Phase 5) cannot drift on the naming
    /// scheme independently.
    pub fn nats_subject_for_prefix(prefix: &str) -> String {
        format!("alerts.geo.{prefix}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEvent {
    pub alert: Alert,
    pub dispatched_at: DateTime<Utc>,
}