use std::sync::Arc;

use infra::metrics::{Counter, CounterVec, Gauge, Histogram, Registry, LATENCY_BUCKETS_SECONDS};

/// Everything the worker exposes on `/metrics`.
pub struct WorkerMetrics {
    pub registry: Registry,
    pub connections: Arc<Gauge>,
    pub connects: Arc<Counter>,
    pub rejects: Arc<CounterVec>,
    pub alerts_received: Arc<CounterVec>,
    pub alerts_delivered: Arc<CounterVec>,
    pub alerts_dropped: Arc<CounterVec>,
    pub alerts_filtered_out: Arc<CounterVec>,
    pub alerts_invalid: Arc<Counter>,
    pub broadcast_duration: Arc<Histogram>,
    pub control_messages: Arc<CounterVec>,
    pub lease_healthy: Arc<Gauge>,
    pub subscriptions_up: Arc<Gauge>,
}

impl WorkerMetrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();
        Arc::new(Self {
            connections: registry.gauge(
                "pulsegrid_ws_connections_current",
                "WebSocket connections currently held by this worker.",
            ),
            connects: registry.counter(
                "pulsegrid_ws_connects_total",
                "WebSocket connections accepted.",
            ),
            rejects: registry.counter_vec(
                "pulsegrid_ws_rejects_total",
                "WebSocket connections refused, by reason.",
                "reason",
            ),
            alerts_received: registry.counter_vec(
                "pulsegrid_worker_alerts_received_total",
                "Alerts received from NATS, by shard.",
                "shard",
            ),
            alerts_delivered: registry.counter_vec(
                "pulsegrid_worker_alerts_delivered_total",
                "Alerts queued to a device socket, by shard.",
                "shard",
            ),
            alerts_dropped: registry.counter_vec(
                "pulsegrid_worker_alerts_dropped_total",
                "Alerts skipped because a device's queue was full, by shard.",
                "shard",
            ),
            alerts_filtered_out: registry.counter_vec(
                "pulsegrid_worker_alerts_filtered_out_total",
                "Connected devices skipped because they were outside the alert circle, by shard.",
                "shard",
            ),
            alerts_invalid: registry.counter(
                "pulsegrid_worker_alerts_invalid_total",
                "Alert messages that could not be parsed.",
            ),
            broadcast_duration: registry.histogram(
                "pulsegrid_worker_broadcast_duration_seconds",
                "Time to fan one alert out to the local sockets of one shard.",
                LATENCY_BUCKETS_SECONDS,
            ),
            control_messages: registry.counter_vec(
                "pulsegrid_worker_control_messages_total",
                "Control messages handled, by outcome.",
                "outcome",
            ),
            lease_healthy: registry.gauge(
                "pulsegrid_worker_lease_healthy",
                "1 while this worker holds the lease on every shard it owns.",
            ),
            subscriptions_up: registry.gauge(
                "pulsegrid_worker_subscriptions_up",
                "Shard subscriptions currently active.",
            ),
            registry,
        })
    }

    pub fn render(&self) -> String {
        self.registry.render()
    }
}
