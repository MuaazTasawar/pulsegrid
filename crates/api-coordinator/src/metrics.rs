use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::Response;
use infra::metrics::{Counter, CounterVec, Histogram, Registry, LATENCY_BUCKETS_SECONDS};

/// Everything the coordinator exposes on `/metrics`.
pub struct Metrics {
    pub registry: Registry,
    pub http_requests: Arc<CounterVec>,
    pub http_duration: Arc<Histogram>,
    pub devices_registered: Arc<Counter>,
    pub devices_deleted: Arc<Counter>,
    pub ws_tickets_issued: Arc<Counter>,
    pub shard_changes: Arc<Counter>,
    pub alerts_dispatched: Arc<Counter>,
    pub alerts_partial: Arc<Counter>,
    pub alert_shards_published: Arc<Counter>,
    pub alert_shard_failures: Arc<Counter>,
    pub alert_dispatch_duration: Arc<Histogram>,
    pub admin_revocations: Arc<Counter>,
    pub retention_devices_deleted: Arc<Counter>,
    pub retention_alerts_deleted: Arc<Counter>,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();
        registry
            .gauge(
                "pulsegrid_coordinator_up",
                "Coordinator process is running.",
            )
            .set(1);
        Arc::new(Self {
            http_requests: registry.counter_vec(
                "pulsegrid_http_requests_total",
                "HTTP responses by status class.",
                "status",
            ),
            http_duration: registry.histogram(
                "pulsegrid_http_request_duration_seconds",
                "Time to produce an HTTP response.",
                LATENCY_BUCKETS_SECONDS,
            ),
            devices_registered: registry
                .counter("pulsegrid_devices_registered_total", "Devices registered."),
            devices_deleted: registry.counter(
                "pulsegrid_devices_deleted_total",
                "Devices deregistered by their owner or an operator.",
            ),
            ws_tickets_issued: registry.counter(
                "pulsegrid_ws_tickets_issued_total",
                "One-time WebSocket tickets issued.",
            ),
            shard_changes: registry.counter(
                "pulsegrid_shard_changes_total",
                "Location updates that moved a device to another shard.",
            ),
            alerts_dispatched: registry.counter(
                "pulsegrid_alerts_dispatched_total",
                "Alerts accepted and published.",
            ),
            alerts_partial: registry.counter(
                "pulsegrid_alerts_partial_total",
                "Alerts that failed to publish to at least one shard.",
            ),
            alert_shards_published: registry.counter(
                "pulsegrid_alert_shards_published_total",
                "Shard publishes that JetStream acknowledged.",
            ),
            alert_shard_failures: registry.counter(
                "pulsegrid_alert_shard_failures_total",
                "Shard publishes that failed or timed out.",
            ),
            alert_dispatch_duration: registry.histogram(
                "pulsegrid_alert_dispatch_duration_seconds",
                "Time from accepting an alert to publishing it to every shard.",
                LATENCY_BUCKETS_SECONDS,
            ),
            admin_revocations: registry.counter(
                "pulsegrid_admin_revocations_total",
                "Devices revoked by an operator.",
            ),
            retention_devices_deleted: registry.counter(
                "pulsegrid_retention_devices_deleted_total",
                "Inactive devices removed by the retention sweep.",
            ),
            retention_alerts_deleted: registry.counter(
                "pulsegrid_retention_alerts_deleted_total",
                "Old alerts removed by the retention sweep.",
            ),
            registry,
        })
    }

    pub fn render(&self) -> String {
        self.registry.render()
    }
}

/// Counts every response by status class and records how long it took.
pub async fn track_http(
    State(metrics): State<Arc<Metrics>>,
    request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let response = next.run(request).await;
    metrics.http_duration.observe_duration(started.elapsed());
    metrics
        .http_requests
        .inc(&format!("{}xx", response.status().as_u16() / 100));
    response
}
