use clap::Parser;
use domain::device::Location;
use domain::geo;
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

const METERS_PER_DEGREE_LAT: f64 = 111_320.0;

#[derive(Parser, Debug)]
#[command(about = "PulseGrid connection-storm load generator: spawns N simulated devices, fires one alert, measures fanout latency")]
struct Args {
    /// Number of simulated devices to connect concurrently
    #[arg(long, default_value_t = 1000)]
    devices: usize,

    /// Center latitude devices are scattered around
    #[arg(long, default_value_t = 24.8607)]
    center_lat: f64,

    /// Center longitude devices are scattered around
    #[arg(long, default_value_t = 67.0011)]
    center_lon: f64,

    /// Radius (meters) within which devices are randomly scattered around the center
    #[arg(long, default_value_t = 3000.0)]
    scatter_radius_meters: f64,

    /// Radius (meters) of the alert fired at the end of the test
    #[arg(long, default_value_t = 5000.0)]
    alert_radius_meters: f64,

    #[arg(long, default_value = "ws://localhost:8081")]
    fanout_worker_url: String,

    #[arg(long, default_value = "http://localhost:8090")]
    coordinator_url: String,

    /// How long to wait for devices to finish connecting before firing the alert
    #[arg(long, default_value_t = 30)]
    connect_timeout_secs: u64,

    /// How long each device waits for the alert to arrive before giving up
    #[arg(long, default_value_t = 20)]
    delivery_timeout_secs: u64,
}

enum Outcome {
    Delivered(Duration),
    ConnectFailed,
    DeliveryTimeout,
}

fn jittered_location(center_lat: f64, center_lon: f64, max_radius_m: f64) -> Location {
    let radius = max_radius_m * rand::random::<f64>().sqrt();
    let angle = rand::random::<f64>() * std::f64::consts::TAU;
    let dx = radius * angle.cos();
    let dy = radius * angle.sin();

    let dlat = dy / METERS_PER_DEGREE_LAT;
    let dlon = dx / (METERS_PER_DEGREE_LAT * center_lat.to_radians().cos());

    // Falls back to the exact center point on the rare chance jitter
    // pushes past a pole/date-line boundary -- good enough for a load
    // test, not meant to be globally correct.
    Location::new(center_lat + dlat, center_lon + dlon)
        .unwrap_or_else(|_| Location::new(center_lat, center_lon).unwrap())
}

async fn run_device(
    idx: usize,
    args_fanout_url: String,
    center_lat: f64,
    center_lon: f64,
    scatter_radius_m: f64,
    delivery_timeout_secs: u64,
    connected_counter: Arc<AtomicUsize>,
    t0: Arc<StdMutex<Option<Instant>>>,
    tx: mpsc::Sender<Outcome>,
) {
    let device_id = Uuid::new_v4();
    let location = jittered_location(center_lat, center_lon, scatter_radius_m);

    let geohash = match geo::encode_location(&location) {
        Ok(h) => h,
        Err(_) => {
            let _ = tx.send(Outcome::ConnectFailed).await;
            return;
        }
    };
    let shard_prefix = geo::shard_prefix_of(&geohash);
    let url = format!("{args_fanout_url}/connect/{device_id}?shard_prefix={shard_prefix}");

    let ws_stream = match connect_async(&url).await {
        Ok((stream, _response)) => stream,
        Err(_) => {
            let _ = tx.send(Outcome::ConnectFailed).await;
            return;
        }
    };

    connected_counter.fetch_add(1, Ordering::Relaxed);
    if idx % 200 == 0 {
        tracing_lite_progress(connected_counter.load(Ordering::Relaxed));
    }

    let (_, mut read) = ws_stream.split();

    let wait_result = tokio::time::timeout(
        Duration::from_secs(delivery_timeout_secs),
        read.next(),
    )
    .await;

    match wait_result {
        Ok(Some(Ok(Message::Binary(_)))) => {
            let recv_instant = Instant::now();
            let t0_snapshot = { *t0.lock().unwrap() };
            match t0_snapshot {
                Some(start) => {
                    let _ = tx.send(Outcome::Delivered(recv_instant.duration_since(start))).await;
                }
                None => {
                    // Message arrived before the alert was actually fired --
                    // shouldn't happen given the ordering in main(), but
                    // fail safe rather than panic on the unwrap.
                    let _ = tx.send(Outcome::DeliveryTimeout).await;
                }
            }
        }
        _ => {
            let _ = tx.send(Outcome::DeliveryTimeout).await;
        }
    }
}

fn tracing_lite_progress(connected: usize) {
    println!("  ...{connected} connected so far");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!(
        "spawning {} simulated devices around ({}, {}), scatter radius {}m",
        args.devices, args.center_lat, args.center_lon, args.scatter_radius_meters
    );

    let connected_counter = Arc::new(AtomicUsize::new(0));
    let t0: Arc<StdMutex<Option<Instant>>> = Arc::new(StdMutex::new(None));
    let (tx, mut rx) = mpsc::channel::<Outcome>(args.devices.max(16));

    // Stagger connection launches instead of firing all N at once --
    // real devices connecting to a real alert network don't dial in
    // simultaneously, and on Windows, hundreds of near-simultaneous
    // outbound TCP connections to localhost from one process can hit
    // client-side ephemeral-port/handshake contention that has nothing
    // to do with the server''s actual capacity. Batches of 50 with a
    // short stagger keep this a realistic ramp-up rather than a
    // self-inflicted client bottleneck.
    const BATCH_SIZE: usize = 50;
    const BATCH_STAGGER_MS: u64 = 100;

    for idx in 0..args.devices {
        let fanout_url = args.fanout_worker_url.clone();
        let connected_counter = connected_counter.clone();
        let t0 = t0.clone();
        let tx = tx.clone();
        let center_lat = args.center_lat;
        let center_lon = args.center_lon;
        let scatter_radius = args.scatter_radius_meters;
        let delivery_timeout = args.delivery_timeout_secs;

        tokio::spawn(async move {
            run_device(
                idx,
                fanout_url,
                center_lat,
                center_lon,
                scatter_radius,
                delivery_timeout,
                connected_counter,
                t0,
                tx,
            )
            .await;
        });

        if idx % BATCH_SIZE == 0 && idx > 0 {
            tokio::time::sleep(Duration::from_millis(BATCH_STAGGER_MS)).await;
        }
    }
    drop(tx); // the original sender; tasks each hold their own clone

    println!("waiting up to {}s for devices to connect...", args.connect_timeout_secs);
    let connect_deadline = Instant::now() + Duration::from_secs(args.connect_timeout_secs);
    loop {
        let connected = connected_counter.load(Ordering::Relaxed);
        if connected >= args.devices || Instant::now() >= connect_deadline {
            println!("{connected}/{} devices connected, firing alert", args.devices);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Store t0 BEFORE firing the HTTP request -- this ordering guarantees
    // every device's t0 read (which happens only after it receives a
    // message, i.e. strictly after the alert has propagated through the
    // network) will see a Some value, never a race against an unset t0.
    {
        let mut guard = t0.lock().unwrap();
        *guard = Some(Instant::now());
    }

    let client = reqwest::Client::new();
    let alert_body = serde_json::json!({
        "lat": args.center_lat,
        "lon": args.center_lon,
        "radius_meters": args.alert_radius_meters,
        "severity": "critical",
        "title": "Load Test Alert",
        "message": "PulseGrid connection-storm benchmark"
    });

    let dispatch_url = format!("{}/alerts", args.coordinator_url);
    let response = client.post(&dispatch_url).json(&alert_body).send().await?;
    let status = response.status();
    let body_text = response.text().await.unwrap_or_default();
    println!("alert dispatch response ({status}): {body_text}");

    println!("collecting delivery results (up to {}s)...", args.delivery_timeout_secs + 5);
    let mut histogram = Histogram::<u64>::new(3)?;
    let mut delivered = 0usize;
    let mut connect_failed = 0usize;
    let mut delivery_timed_out = 0usize;

    let collect_deadline = Instant::now() + Duration::from_secs(args.delivery_timeout_secs + 5);
    let mut received = 0usize;
    while received < args.devices && Instant::now() < collect_deadline {
        match tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(outcome)) => {
                received += 1;
                match outcome {
                    Outcome::Delivered(d) => {
                        delivered += 1;
                        let micros = d.as_micros() as u64;
                        let _ = histogram.record(micros);
                    }
                    Outcome::ConnectFailed => connect_failed += 1,
                    Outcome::DeliveryTimeout => delivery_timed_out += 1,
                }
            }
            Ok(None) => break, // channel closed, all senders dropped
            Err(_) => continue, // 500ms poll tick, keep checking the deadline
        }
    }

    println!("\n=== PulseGrid Load Test Results ===");
    println!("devices requested:     {}", args.devices);
    println!("connect failed:        {connect_failed}");
    println!("delivered:             {delivered}");
    println!("delivery timed out:    {delivery_timed_out}");

    if delivered > 0 {
        let to_ms = |micros: u64| micros as f64 / 1000.0;
        println!("\n--- fanout latency (ms) ---");
        println!("p50:  {:.2}", to_ms(histogram.value_at_quantile(0.50)));
        println!("p90:  {:.2}", to_ms(histogram.value_at_quantile(0.90)));
        println!("p99:  {:.2}", to_ms(histogram.value_at_quantile(0.99)));
        println!("max:  {:.2}", to_ms(histogram.max()));
    } else {
        println!("\nno deliveries recorded -- check fanout-worker logs");
    }

    Ok(())
}