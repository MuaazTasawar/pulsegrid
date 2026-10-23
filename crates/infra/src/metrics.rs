//! A tiny, dependency-free Prometheus metrics registry: counters, gauges,
//! single-label counters and fixed-bucket histograms, rendered in the
//! Prometheus text exposition format (version 0.0.4).
//!
//! Metrics are registered once at startup and then updated lock-free with
//! atomics from any thread. Metric names must match
//! `[a-zA-Z_:][a-zA-Z0-9_:]*` (checked at registration, which panics on a
//! programmer error rather than emitting an invalid exposition).

use std::fmt::Write as _;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;

/// Latency buckets in seconds, from 0.5 ms to 5 s.
pub const LATENCY_BUCKETS_SECONDS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

trait Collect: Send + Sync {
    fn kind(&self) -> &'static str;
    fn render(&self, name: &str, out: &mut String);
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

fn escape_label_value(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn escape_help(v: &str) -> String {
    v.replace('\\', "\\\\").replace('\n', "\\n")
}

/// A monotonically increasing counter.
#[derive(Default)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Collect for Counter {
    fn kind(&self) -> &'static str {
        "counter"
    }
    fn render(&self, name: &str, out: &mut String) {
        let _ = writeln!(out, "{name} {}", self.get());
    }
}

/// A value that can go up and down.
#[derive(Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    pub fn set(&self, v: i64) {
        self.0.store(v, Ordering::Relaxed);
    }
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn add(&self, n: i64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Collect for Gauge {
    fn kind(&self) -> &'static str {
        "gauge"
    }
    fn render(&self, name: &str, out: &mut String) {
        let _ = writeln!(out, "{name} {}", self.get());
    }
}

/// A family of counters distinguished by one label (for example `shard`).
/// Keep the label's cardinality bounded.
pub struct CounterVec {
    label: String,
    values: DashMap<String, AtomicU64>,
}

impl CounterVec {
    pub fn inc(&self, label_value: &str) {
        self.add(label_value, 1);
    }

    pub fn add(&self, label_value: &str, n: u64) {
        if let Some(c) = self.values.get(label_value) {
            c.fetch_add(n, Ordering::Relaxed);
            return;
        }
        self.values
            .entry(label_value.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self, label_value: &str) -> u64 {
        self.values
            .get(label_value)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

impl Collect for CounterVec {
    fn kind(&self) -> &'static str {
        "counter"
    }
    fn render(&self, name: &str, out: &mut String) {
        let mut rows: Vec<(String, u64)> = self
            .values
            .iter()
            .map(|e| (e.key().clone(), e.value().load(Ordering::Relaxed)))
            .collect();
        rows.sort();
        for (value, count) in rows {
            let _ = writeln!(
                out,
                "{name}{{{}=\"{}\"}} {count}",
                self.label,
                escape_label_value(&value)
            );
        }
    }
}

/// A fixed-bucket histogram. Observe values in the unit of the metric
/// (seconds for latencies).
pub struct Histogram {
    bounds: Vec<f64>,
    // One slot per bound plus a final slot for values above every bound.
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Histogram {
    fn new(bounds: &[f64]) -> Self {
        let mut bounds = bounds.to_vec();
        bounds.sort_by(|a, b| a.partial_cmp(b).expect("histogram bounds must not be NaN"));
        let buckets = (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            bounds,
            buckets,
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, value: f64) {
        let idx = self
            .bounds
            .iter()
            .position(|b| value <= *b)
            .unwrap_or(self.bounds.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add((value * 1e6).max(0.0) as u64, Ordering::Relaxed);
    }

    pub fn observe_duration(&self, d: Duration) {
        self.observe(d.as_secs_f64());
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    pub fn sum(&self) -> f64 {
        self.sum_micros.load(Ordering::Relaxed) as f64 / 1e6
    }
}

impl Collect for Histogram {
    fn kind(&self) -> &'static str {
        "histogram"
    }
    fn render(&self, name: &str, out: &mut String) {
        let mut cumulative = 0u64;
        for (i, bound) in self.bounds.iter().enumerate() {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        cumulative += self.buckets[self.bounds.len()].load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cumulative}");
        let _ = writeln!(out, "{name}_sum {}", self.sum());
        let _ = writeln!(out, "{name}_count {}", self.count());
    }
}

struct Entry {
    name: String,
    help: String,
    collector: Arc<dyn Collect>,
}

/// Holds every registered metric and renders them.
#[derive(Clone, Default)]
pub struct Registry {
    entries: Arc<Mutex<Vec<Entry>>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    fn register<T: Collect + 'static>(&self, name: &str, help: &str, collector: Arc<T>) -> Arc<T> {
        assert!(valid_name(name), "invalid metric name: {name}");
        let mut entries = self.entries.lock().expect("metrics registry poisoned");
        assert!(
            !entries.iter().any(|e| e.name == name),
            "metric registered twice: {name}"
        );
        entries.push(Entry {
            name: name.to_string(),
            help: help.to_string(),
            collector: collector.clone(),
        });
        collector
    }

    pub fn counter(&self, name: &str, help: &str) -> Arc<Counter> {
        self.register(name, help, Arc::new(Counter::default()))
    }

    pub fn gauge(&self, name: &str, help: &str) -> Arc<Gauge> {
        self.register(name, help, Arc::new(Gauge::default()))
    }

    pub fn counter_vec(&self, name: &str, help: &str, label: &str) -> Arc<CounterVec> {
        assert!(valid_name(label), "invalid label name: {label}");
        self.register(
            name,
            help,
            Arc::new(CounterVec {
                label: label.to_string(),
                values: DashMap::new(),
            }),
        )
    }

    pub fn histogram(&self, name: &str, help: &str, bounds: &[f64]) -> Arc<Histogram> {
        self.register(name, help, Arc::new(Histogram::new(bounds)))
    }

    /// Renders every metric in the Prometheus text format.
    pub fn render(&self) -> String {
        let entries = self.entries.lock().expect("metrics registry poisoned");
        let mut out = String::new();
        for e in entries.iter() {
            let _ = writeln!(out, "# HELP {} {}", e.name, escape_help(&e.help));
            let _ = writeln!(out, "# TYPE {} {}", e.name, e.collector.kind());
            e.collector.render(&e.name, &mut out);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_gauges_render() {
        let r = Registry::new();
        let c = r.counter("pulsegrid_alerts_total", "Alerts dispatched.");
        let g = r.gauge("pulsegrid_ws_connections_current", "Open sockets.");
        c.inc();
        c.add(4);
        g.set(10);
        g.dec();
        g.add(-2);
        assert_eq!(c.get(), 5);
        assert_eq!(g.get(), 7);

        let text = r.render();
        assert!(text.contains("# HELP pulsegrid_alerts_total Alerts dispatched.\n"));
        assert!(text.contains("# TYPE pulsegrid_alerts_total counter\n"));
        assert!(text.contains("pulsegrid_alerts_total 5\n"));
        assert!(text.contains("# TYPE pulsegrid_ws_connections_current gauge\n"));
        assert!(text.contains("pulsegrid_ws_connections_current 7\n"));
    }

    #[test]
    fn counter_vec_is_sorted_and_escaped() {
        let r = Registry::new();
        let v = r.counter_vec("pulsegrid_delivered_total", "Per shard.", "shard");
        v.inc("tkrw");
        v.add("tkrt", 3);
        v.inc("tkrt");
        v.inc("we\"ird\\\nvalue");
        assert_eq!(v.get("tkrt"), 4);
        assert_eq!(v.get("missing"), 0);

        let text = r.render();
        let a = text.find("shard=\"tkrt\"} 4").expect("tkrt row");
        let b = text.find("shard=\"tkrw\"} 1").expect("tkrw row");
        assert!(a < b, "rows must be sorted by label value");
        assert!(text.contains(r#"shard="we\"ird\\\nvalue"} 1"#));
    }

    #[test]
    fn histogram_buckets_are_cumulative() {
        let r = Registry::new();
        let h = r.histogram(
            "pulsegrid_broadcast_seconds",
            "Broadcast time.",
            &[0.01, 0.1, 1.0],
        );
        h.observe(0.005); // <= 0.01
        h.observe(0.05); //  <= 0.1
        h.observe(0.05); //  <= 0.1
        h.observe(5.0); //   > every bound
        h.observe_duration(Duration::from_millis(500)); // <= 1.0
        assert_eq!(h.count(), 5);
        assert!((h.sum() - 5.605).abs() < 1e-6);

        let text = r.render();
        assert!(text.contains("pulsegrid_broadcast_seconds_bucket{le=\"0.01\"} 1\n"));
        assert!(text.contains("pulsegrid_broadcast_seconds_bucket{le=\"0.1\"} 3\n"));
        assert!(text.contains("pulsegrid_broadcast_seconds_bucket{le=\"1\"} 4\n"));
        assert!(text.contains("pulsegrid_broadcast_seconds_bucket{le=\"+Inf\"} 5\n"));
        assert!(text.contains("pulsegrid_broadcast_seconds_count 5\n"));
        assert!(text.contains("# TYPE pulsegrid_broadcast_seconds histogram\n"));
    }

    #[test]
    fn counters_are_safe_across_threads() {
        let r = Registry::new();
        let c = r.counter("c_total", "c");
        let v = r.counter_vec("v_total", "v", "k");
        let h = r.histogram("h_seconds", "h", LATENCY_BUCKETS_SECONDS);
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (c, v, h) = (c.clone(), v.clone(), h.clone());
                std::thread::spawn(move || {
                    for _ in 0..1_000 {
                        c.inc();
                        v.inc("same");
                        h.observe(0.002);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(c.get(), 8_000);
        assert_eq!(v.get("same"), 8_000);
        assert_eq!(h.count(), 8_000);
    }

    #[test]
    #[should_panic(expected = "invalid metric name")]
    fn rejects_invalid_names() {
        Registry::new().counter("9-bad name", "x");
    }

    #[test]
    #[should_panic(expected = "registered twice")]
    fn rejects_duplicate_names() {
        let r = Registry::new();
        r.counter("dup_total", "a");
        r.counter("dup_total", "b");
    }
}
