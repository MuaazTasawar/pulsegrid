use clap::Parser;
use domain::device::Location;
use domain::geo;
use futures_util::StreamExt;
use hdrhistogram::Histogram;
use serde::Deserialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const METERS_PER_DEGREE_LAT: f64 = 111_320.0;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = 1000)]
    devices: usize,
    #[arg(long, default_value_t = 24.8607)]
    center_lat: f64,
    #[arg(long, default_value_t = 67.0011)]
    center_lon: f64,
    #[arg(long, default_value_t = 3000.0)]
    scatter_radius_meters: f64,
    #[arg(long, default_value_t = 5000.0)]
    alert_radius_meters: f64,
    #[arg(long, default_value = "ws://localhost:8081")]
    fanout_worker_url: String,
    #[arg(long, default_value = "http://localhost:8090")]
    coordinator_url: String,
    #[arg(long, default_value_t = 30)]
    connect_timeout_secs: u64,
    #[arg(long, default_value_t = 20)]
    delivery_timeout_secs: u64,
}

#[derive(Deserialize)]
struct RegisterResponse {
    device_id: String,
    token: String,
}

enum Outcome {
    Delivered(Duration),
    RegisterFailed,
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
    Location::new(center_lat + dlat, center_lon + dlon)
        .unwrap_or_else(|_| Location::new(center_lat, center_lon).unwrap())
}

#[allow(clippy::too_many_arguments)]
async fn run_device(
    idx: usize,
    coordinator_url: String,
    fanout_url: String,
    center_lat: f64,
    center_lon: f64,
    scatter_radius_m: f64,
    delivery_timeout_secs: u64,
    connected_counter: Arc<AtomicUsize>,
    t0: Arc<StdMutex<Option<Instant>>>,
    tx: mpsc::Sender<Outcome>,
) {
    let location = jittered_location(center_lat, center_lon, scatter_radius_m);

    // Register the device for real this time -- this is what fixes the
    // security gap: fanout-worker now requires a valid token whose sub
    // matches the device_id, so loadgen has to actually go through the
    // registration flow like a real client would, not just invent a
    // random UUID and connect.
    let client = reqwest::Client::new();
    let register_body = serde_json::json!({ "lat": location.lat, "lon": location.lon });
    let register_result = client
        .post(format!("{coordinator_url}/devices/register"))
        .json(&register_body)
        .send()
        .await;

    let reg: RegisterResponse = match register_result {
        Ok(resp) => match resp.json().await {
            Ok(r) => r,
            Err(_) => {
                let _ = tx.send(Outcome::RegisterFailed).await;
                return;
            }
        },
        Err(_) => {
            let _ = tx.send(Outcome::RegisterFailed).await;
            return;
        }
    };

    let geohash = match geo::encode_location(&location) {
        Ok(h) => h,
        Err(_) => {
            let _ = tx.send(Outcome::ConnectFailed).await;
            return;
        }
    };
    let shard_prefix = geo::shard_prefix_of(&geohash);
    let url = format!(
        "{fanout_url}/connect/{}?shard_prefix={shard_prefix}&token={}",
        reg.device_id, reg.token
    );

    let ws_stream = match connect_async(&url).await {
        Ok((stream, _)) => stream,
        Err(_) => {
            let _ = tx.send(Outcome::ConnectFailed).await;
            return;
        }
    };

    connected_counter.fetch_add(1, Ordering::Relaxed);
    if idx % 200 == 0 {
        println!("  ...{} connected so far", connected_counter.load(Ordering::Relaxed));
    }

    let (_, mut read) = ws_stream.split();
    let wait_result = tokio::time::timeout(Duration::from_secs(delivery_timeout_secs), read.next()).await;

    match wait_result {
        Ok(Some(Ok(Message::Binary(_)))) => {
            let recv_instant = Instant::now();
            let t0_snapshot = { *t0.lock().unwrap() };
            match t0_snapshot {
                Some(start) => {
                    let _ = tx.send(Outcome::Delivered(recv_instant.duration_since(start))).await;
                }
                None => {
                    let _ = tx.send(Outcome::DeliveryTimeout).await;
                }
            }
        }
        _ => {
            let _ = tx.send(Outcome::DeliveryTimeout).await;
        }
    }
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

    const BATCH_SIZE: usize = 50;
    const BATCH_STAGGER_MS: u64 = 100;

    for idx in 0..args.devices {
        let coordinator_url = args.coordinator_url.clone();
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
                idx, coordinator_url, fanout_url, center_lat, center_lon,
                scatter_radius, delivery_timeout, connected_counter, t0, tx,
            ).await;
        });

        if idx % BATCH_SIZE == 0 && idx > 0 {
            tokio::time::sleep(Duration::from_millis(BATCH_STAGGER_MS)).await;
        }
    }
    drop(tx);

    println!("waiting up to {}s for devices to register + connect...", args.connect_timeout_secs);
    let connect_deadline = Instant::now() + Duration::from_secs(args.connect_timeout_secs);
    loop {
        let connected = connected_counter.load(Ordering::Relaxed);
        if connected >= args.devices || Instant::now() >= connect_deadline {
            println!("{connected}/{} devices connected, firing alert", args.devices);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    {
        let mut guard = t0.lock().unwrap();
        *guard = Some(Instant::now());
    }

    let client = reqwest::Client::new();
    let alert_body = serde_json::json!({
        "lat": args.center_lat, "lon": args.center_lon,
        "radius_meters": args.alert_radius_meters,
        "severity": "critical", "title": "Load Test Alert",
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
    let mut register_failed = 0usize;
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
                        let _ = histogram.record(d.as_micros() as u64);
                    }
                    Outcome::RegisterFailed => register_failed += 1,
                    Outcome::ConnectFailed => connect_failed += 1,
                    Outcome::DeliveryTimeout => delivery_timed_out += 1,
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    println!("\n=== PulseGrid Load Test Results ===");
    println!("devices requested:     {}", args.devices);
    println!("register failed:       {register_failed}");
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