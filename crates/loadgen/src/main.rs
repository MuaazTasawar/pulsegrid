//! PulseGrid load generator.
//!
//! What one run does:
//!
//! 1. Registers `--devices` devices, scattered around a centre point.
//! 2. For each: asks the coordinator for a one-time ticket and connects to the
//!    `ws_url` the coordinator returns, so several workers are used correctly.
//! 3. Waits for every device to finish its connect-time replay (the server
//!    announces it with `replay_complete`), so old alerts do not pollute the run.
//! 4. Sends `--warmup-alerts` alerts that are not measured, then `--alerts`
//!    measured ones, one every `--interval-ms`.
//! 5. Reports delivery counts and latency percentiles.
//!
//! How latency is measured: the clock starts immediately BEFORE the
//! `POST /alerts` request is sent and stops when the binary frame is decoded
//! on the device's socket. So it covers the HTTP call, database work, NATS
//! publish and the worker's fan-out. Everything runs in this one process and
//! uses the monotonic clock, so there is no clock skew between sender and
//! receivers. It does NOT include real mobile networks.
//!
//! Needs `RATE_LIMIT_DISABLED=true` on the coordinator (and the workers'
//! connect rate limit raised), or registration and connects are throttled.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use futures_util::{stream, StreamExt};
use hdrhistogram::Histogram;
use rand::Rng;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(
    name = "loadgen",
    about = "Delivery-latency load generator for PulseGrid"
)]
struct Args {
    /// Base URL of the api-coordinator.
    #[arg(long, default_value = "http://localhost:8090")]
    coordinator: String,
    /// Operator API key. Default: OPERATOR_API_KEYS from the environment or ./.env.
    #[arg(long)]
    operator_key: Option<String>,
    /// Devices to register and keep connected.
    #[arg(long, default_value_t = 200)]
    devices: usize,
    /// Measured alerts to send.
    #[arg(long, default_value_t = 20)]
    alerts: usize,
    /// Alerts sent first and excluded from the statistics (cold caches, JIT-like effects).
    #[arg(long, default_value_t = 2)]
    warmup_alerts: usize,
    /// Pause between alerts.
    #[arg(long, default_value_t = 500)]
    interval_ms: u64,
    /// Latitude of the centre point.
    #[arg(long, default_value_t = 24.8607, allow_negative_numbers = true)]
    lat: f64,
    /// Longitude of the centre point.
    #[arg(long, default_value_t = 67.0011, allow_negative_numbers = true)]
    lon: f64,
    /// Devices are scattered uniformly inside this radius around the centre.
    #[arg(long, default_value_t = 3000.0)]
    spread_meters: f64,
    /// Alert radius. Must comfortably exceed the spread so every device is inside.
    #[arg(long, default_value_t = 5000.0)]
    radius_meters: f64,
    /// How many devices are set up at the same time.
    #[arg(long, default_value_t = 64)]
    setup_concurrency: usize,
    /// How long to keep listening after the last alert.
    #[arg(long, default_value_t = 5)]
    drain_secs: u64,
    /// Longest wait for all devices to finish their connect-time replay.
    #[arg(long, default_value_t = 60)]
    replay_wait_secs: u64,
    /// Do not delete the devices afterwards.
    #[arg(long)]
    keep_devices: bool,
    /// Print one JSON object on stdout instead of the text report.
    #[arg(long)]
    json: bool,
}

/// One alert frame arriving on one device's socket.
struct Arrival {
    alert_id: Uuid,
    device: usize,
    at: Instant,
}

/// An alert that was sent.
struct Sent {
    t0: Instant,
    measured: bool,
}

struct Connected {
    token: String,
    ws_url: String,
    task: JoinHandle<()>,
}

struct Posted {
    alert_id: Uuid,
    t0: Instant,
    took: Duration,
}

fn new_hist() -> Result<Histogram<u64>> {
    // Microseconds, up to two minutes, three significant digits.
    Ok(Histogram::new_with_bounds(1, 120_000_000, 3)?)
}

fn first_key(value: &str) -> Option<String> {
    value
        .split(',')
        .next()
        .map(|k| k.trim().trim_matches('"').to_owned())
        .filter(|k| !k.is_empty())
}

fn find_operator_key(arg: Option<String>) -> Result<String> {
    if let Some(key) = arg {
        return Ok(key);
    }
    if let Some(key) = std::env::var("OPERATOR_API_KEYS")
        .ok()
        .as_deref()
        .and_then(first_key)
    {
        return Ok(key);
    }
    if let Ok(text) = std::fs::read_to_string(".env") {
        for line in text.lines() {
            if let Some(key) = line
                .trim()
                .strip_prefix("OPERATOR_API_KEYS=")
                .and_then(first_key)
            {
                return Ok(key);
            }
        }
    }
    bail!("no operator key: pass --operator-key or set OPERATOR_API_KEYS (env or ./.env)")
}

/// A random point inside a circle of `spread_m` metres around (lat, lon).
fn scatter(rng: &mut impl Rng, lat: f64, lon: f64, spread_m: f64) -> (f64, f64) {
    let r = spread_m * rng.gen::<f64>().sqrt();
    let theta = rng.gen::<f64>() * std::f64::consts::TAU;
    let dlat = r * theta.cos() / 111_320.0;
    let dlon = r * theta.sin() / (111_320.0 * lat.to_radians().cos());
    (lat + dlat, lon + dlon)
}

fn alert_id_of(payload: &[u8]) -> Option<Uuid> {
    serde_json::from_slice::<Value>(payload)
        .ok()?
        .get("id")?
        .as_str()?
        .parse()
        .ok()
}

fn is_replay_complete(text: &str) -> bool {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|v| v.get("type").and_then(Value::as_str).map(str::to_owned))
        .is_some_and(|t| t == "replay_complete")
}

/// Registers one device, gets a ticket, connects to the worker the coordinator
/// names, and starts a task that reads frames until the socket ends.
async fn connect_device(
    http: reqwest::Client,
    base: String,
    idx: usize,
    point: (f64, f64),
    arrivals: mpsc::UnboundedSender<Arrival>,
    ready: mpsc::UnboundedSender<()>,
) -> Result<Connected> {
    let registered: Value = http
        .post(format!("{base}/devices/register"))
        .json(&json!({ "lat": point.0, "lon": point.1 }))
        .send()
        .await
        .context("POST /devices/register")?
        .error_for_status()
        .context("register was refused")?
        .json()
        .await
        .context("register response")?;
    let device_id: Uuid = registered["device_id"]
        .as_str()
        .context("register response has no device_id")?
        .parse()
        .context("device_id is not a uuid")?;
    let token = registered["token"]
        .as_str()
        .context("register response has no token")?
        .to_owned();

    let ticket: Value = http
        .post(format!("{base}/devices/ws-ticket"))
        .bearer_auth(&token)
        .send()
        .await
        .context("POST /devices/ws-ticket")?
        .error_for_status()
        .context("ws-ticket was refused")?
        .json()
        .await
        .context("ws-ticket response")?;
    let ws_url = ticket["ws_url"]
        .as_str()
        .context("no worker owns this device's shard")?
        .trim_end_matches('/')
        .to_owned();
    let ticket_value = ticket["ticket"]
        .as_str()
        .context("ws-ticket response has no ticket")?;

    let url = format!("{ws_url}/connect/{device_id}?ticket={ticket_value}");
    let (socket, _) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .context("websocket connect")?;

    let task = tokio::spawn(async move {
        let mut socket = socket;
        while let Some(frame) = socket.next().await {
            match frame {
                Ok(Message::Binary(bytes)) => {
                    let at = Instant::now();
                    if let Some(alert_id) = alert_id_of(&bytes) {
                        let _ = arrivals.send(Arrival {
                            alert_id,
                            device: idx,
                            at,
                        });
                    }
                }
                Ok(Message::Text(text)) => {
                    if is_replay_complete(text.as_str()) {
                        let _ = ready.send(());
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });

    Ok(Connected {
        token,
        ws_url,
        task,
    })
}

async fn post_alert(http: &reqwest::Client, base: &str, key: &str, body: &Value) -> Result<Posted> {
    let t0 = Instant::now();
    let response = http
        .post(format!("{base}/alerts"))
        .header("X-API-Key", key)
        .json(body)
        .send()
        .await
        .context("POST /alerts")?;
    let took = t0.elapsed();
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        bail!("POST /alerts returned {status}: {text}");
    }
    let value: Value = response.json().await.context("alert response")?;
    let alert_id = value["alert_id"]
        .as_str()
        .context("alert response has no alert_id")?
        .parse()
        .context("alert_id is not a uuid")?;
    Ok(Posted { alert_id, t0, took })
}

fn summarize(hist: &Histogram<u64>) -> Value {
    let ms = |us: u64| us as f64 / 1000.0;
    json!({
        "p50": ms(hist.value_at_quantile(0.50)),
        "p90": ms(hist.value_at_quantile(0.90)),
        "p99": ms(hist.value_at_quantile(0.99)),
        "p99_9": ms(hist.value_at_quantile(0.999)),
        "max": ms(hist.max()),
        "mean": hist.mean() / 1000.0,
        "samples": hist.len(),
    })
}

fn print_latency(label: &str, summary: &Value) {
    let get = |k: &str| summary[k].as_f64().unwrap_or(0.0);
    println!(
        "  {label:<22} p50 {:>8.2}  p90 {:>8.2}  p99 {:>8.2}  p99.9 {:>8.2}  max {:>8.2}  (ms)",
        get("p50"),
        get("p90"),
        get("p99"),
        get("p99_9"),
        get("max"),
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.devices == 0 || args.alerts == 0 {
        bail!("--devices and --alerts must be at least 1");
    }
    if args.spread_meters > args.radius_meters * 0.9 {
        bail!(
            "--spread-meters ({}) must be at most 90% of --radius-meters ({}), \
             otherwise some devices are outside the alert circle and the expected \
             delivery count is wrong",
            args.spread_meters,
            args.radius_meters
        );
    }
    let operator_key = find_operator_key(args.operator_key.clone())?;
    let base = args.coordinator.trim_end_matches('/').to_owned();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("building the HTTP client")?;

    // 1. Register and connect every device.
    let points: Vec<(f64, f64)> = {
        let mut rng = rand::thread_rng();
        (0..args.devices)
            .map(|_| scatter(&mut rng, args.lat, args.lon, args.spread_meters))
            .collect()
    };
    let (arrival_tx, mut arrival_rx) = mpsc::unbounded_channel::<Arrival>();
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel::<()>();

    eprintln!(
        "-> registering and connecting {} devices ({} at a time)",
        args.devices, args.setup_concurrency
    );
    let results: Vec<Result<Connected>> = stream::iter(points.into_iter().enumerate())
        .map(|(idx, point)| {
            connect_device(
                http.clone(),
                base.clone(),
                idx,
                point,
                arrival_tx.clone(),
                ready_tx.clone(),
            )
        })
        .buffer_unordered(args.setup_concurrency.max(1))
        .collect()
        .await;
    drop(arrival_tx);
    drop(ready_tx);

    let mut connected: Vec<Connected> = Vec::new();
    let mut failed_devices = 0usize;
    let mut error_samples: Vec<String> = Vec::new();
    for result in results {
        match result {
            Ok(c) => connected.push(c),
            Err(e) => {
                failed_devices += 1;
                let message = format!("{e:#}");
                if error_samples.len() < 3 && !error_samples.contains(&message) {
                    error_samples.push(message);
                }
            }
        }
    }
    let connected_count = connected.len();
    eprintln!("   connected {connected_count}, failed {failed_devices}");
    for message in &error_samples {
        eprintln!("   e.g. {message}");
    }
    if connected_count == 0 {
        bail!("no device could connect; is RATE_LIMIT_DISABLED=true set, and is a worker running?");
    }

    let mut per_worker: BTreeMap<String, usize> = BTreeMap::new();
    for c in &connected {
        *per_worker.entry(c.ws_url.clone()).or_default() += 1;
    }
    for (url, count) in &per_worker {
        eprintln!("   {url}: {count} devices");
    }

    // 2. Let every device finish its connect-time replay.
    eprintln!("-> waiting for the connect-time replay to finish on every device");
    let deadline = tokio::time::sleep(Duration::from_secs(args.replay_wait_secs));
    tokio::pin!(deadline);
    let mut replay_done = 0usize;
    while replay_done < connected_count {
        tokio::select! {
            signal = ready_rx.recv() => {
                if signal.is_none() {
                    break;
                }
                replay_done += 1;
            }
            _ = &mut deadline => break,
        }
    }
    eprintln!("   {replay_done}/{connected_count} devices finished the replay");

    // 3. Send the alerts.
    let total = args.warmup_alerts + args.alerts;
    eprintln!(
        "-> sending {} warm-up + {} measured alerts, one every {} ms",
        args.warmup_alerts, args.alerts, args.interval_ms
    );
    let mut sent: HashMap<Uuid, Sent> = HashMap::new();
    let mut post_hist = new_hist()?;
    let mut failed_alerts = 0usize;
    let mut alert_errors: Vec<String> = Vec::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms.max(1)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    for n in 0..total {
        ticker.tick().await;
        let measured = n >= args.warmup_alerts;
        let body = json!({
            "lat": args.lat,
            "lon": args.lon,
            "radius_meters": args.radius_meters,
            "severity": "warning",
            "title": format!("loadgen {}", n + 1),
            "message": "load generator",
        });
        match post_alert(&http, &base, &operator_key, &body).await {
            Ok(posted) => {
                if measured {
                    post_hist.saturating_record(posted.took.as_micros() as u64);
                }
                sent.insert(
                    posted.alert_id,
                    Sent {
                        t0: posted.t0,
                        measured,
                    },
                );
            }
            Err(e) => {
                failed_alerts += 1;
                let message = format!("{e:#}");
                if alert_errors.len() < 3 && !alert_errors.contains(&message) {
                    alert_errors.push(message);
                }
            }
        }
        if (n + 1).is_multiple_of(10) || n + 1 == total {
            eprintln!("   {}/{} alerts sent", n + 1, total);
        }
    }
    for message in &alert_errors {
        eprintln!("   alert error, e.g. {message}");
    }

    eprintln!("-> listening for {} more seconds", args.drain_secs);
    tokio::time::sleep(Duration::from_secs(args.drain_secs)).await;

    // 4. Stop the devices and collect what arrived.
    for c in &connected {
        c.task.abort();
    }
    arrival_rx.close();
    let mut arrivals: Vec<Arrival> = Vec::new();
    while let Some(arrival) = arrival_rx.recv().await {
        arrivals.push(arrival);
    }

    let tokens: Vec<String> = connected.iter().map(|c| c.token.clone()).collect();
    if !args.keep_devices {
        eprintln!("-> deleting the devices");
        let deleted: Vec<bool> = stream::iter(tokens.iter())
            .map(|token| {
                let http = http.clone();
                let url = format!("{base}/devices/me");
                async move {
                    http.delete(url)
                        .bearer_auth(token)
                        .send()
                        .await
                        .map(|r| r.status().is_success())
                        .unwrap_or(false)
                }
            })
            .buffer_unordered(args.setup_concurrency.max(1))
            .collect()
            .await;
        let ok = deleted.iter().filter(|d| **d).count();
        eprintln!("   deleted {ok}/{}", tokens.len());
    }

    // 5. Work out the numbers.
    let mut latency = new_hist()?;
    let mut seen: HashSet<(Uuid, usize)> = HashSet::new();
    let mut duplicates = 0u64;
    let mut last_arrival: Option<Instant> = None;
    for arrival in &arrivals {
        let Some(alert) = sent.get(&arrival.alert_id) else {
            continue; // a replayed older alert, not ours
        };
        if !alert.measured {
            continue;
        }
        if !seen.insert((arrival.alert_id, arrival.device)) {
            duplicates += 1;
            continue;
        }
        latency
            .saturating_record(arrival.at.saturating_duration_since(alert.t0).as_micros() as u64);
        last_arrival = Some(last_arrival.map_or(arrival.at, |l| l.max(arrival.at)));
    }

    let measured_sent = sent.values().filter(|s| s.measured).count();
    let expected = measured_sent * connected_count;
    let unique = seen.len();
    let ratio = if expected > 0 {
        unique as f64 / expected as f64
    } else {
        0.0
    };
    let first_t0 = sent.values().filter(|s| s.measured).map(|s| s.t0).min();
    let window_secs = match (first_t0, last_arrival) {
        (Some(first), Some(last)) => last.saturating_duration_since(first).as_secs_f64(),
        _ => 0.0,
    };
    let per_second = if window_secs > 0.0 {
        unique as f64 / window_secs
    } else {
        0.0
    };
    let delivery = summarize(&latency);
    let post = summarize(&post_hist);

    if args.json {
        let report = json!({
            "devices_requested": args.devices,
            "devices_connected": connected_count,
            "devices_failed": failed_devices,
            "devices_per_worker": per_worker,
            "alerts_measured": measured_sent,
            "alerts_failed": failed_alerts,
            "warmup_alerts": args.warmup_alerts,
            "interval_ms": args.interval_ms,
            "expected_deliveries": expected,
            "unique_deliveries": unique,
            "duplicate_deliveries": duplicates,
            "delivery_ratio": ratio,
            "window_secs": window_secs,
            "deliveries_per_sec": per_second,
            "delivery_latency_ms": delivery,
            "post_alerts_ms": post,
        });
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!();
        println!("=== PulseGrid load test ===");
        println!(
            "devices:    {connected_count} connected, {failed_devices} failed (of {})",
            args.devices
        );
        for (url, count) in &per_worker {
            println!("            {url}: {count}");
        }
        println!("alerts:     {measured_sent} measured, {failed_alerts} failed to send");
        println!(
            "deliveries: {unique} of {expected} expected ({:.2}%)",
            ratio * 100.0
        );
        println!("duplicates: {duplicates} (delivery is at-least-once)");
        println!(
            "rate:       {per_second:.0} deliveries/s over {window_secs:.1} s (offered load, not a ceiling)"
        );
        println!("latency, request sent -> frame received on the device:");
        print_latency("delivery", &delivery);
        print_latency("POST /alerts returns", &post);
        println!();
        println!("Note: loadgen, coordinator, workers and databases share this machine.");
    }

    if unique == 0 {
        bail!("no measured delivery arrived");
    }
    Ok(())
}
