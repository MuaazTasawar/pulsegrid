#![allow(dead_code)]
//! Shared harness for the end-to-end tests.
//!
//! `Stack::start()` gives a test its own world: throwaway Postgres, Redis and
//! NATS (JetStream) containers, the real coordinator and a real fanout-worker
//! running inside the test process on random ports, and helpers that talk to
//! them over real HTTP, WebSocket and Server-Sent Events connections.
//!
//! Needs Docker, like the other integration tests. At most two stacks run at
//! the same time so a laptop is not asked to start fifteen containers at once.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::redis::{Redis, REDIS_PORT};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

pub const OPERATOR_KEY: &str = "e2e-operator-key";
pub const SHARD_PREFIXES: &str = "tkrt,tkrw,tkrs,tkrv";

static STACK_SLOTS: Semaphore = Semaphore::const_new(2);

// ------------------------------------------------------------------ stack

pub struct Stack {
    // Dropped first to last: the services stop before their containers do,
    // and the slot is handed back last.
    pub coordinator: Coordinator,
    pub worker: Option<Worker>,
    pub http: reqwest::Client,
    redis_url: String,
    nats_url: String,
    _postgres: ContainerAsync<Postgres>,
    _redis: ContainerAsync<Redis>,
    _nats: ContainerAsync<Nats>,
    _slot: SemaphorePermit<'static>,
}

impl Stack {
    pub async fn start() -> Stack {
        if std::env::var("PULSEGRID_TEST_LOG").is_ok() {
            api_coordinator::init_tracing(false);
        }
        let slot = STACK_SLOTS.acquire().await.expect("stack semaphore");

        let (database_url, postgres) = start_postgres().await;
        let (redis_url, redis) = start_redis().await;
        let (nats_url, nats) = start_nats().await;

        let coordinator = spawn_coordinator(&database_url, &redis_url, &nats_url).await;
        let worker = spawn_worker(&redis_url, &nats_url).await;

        Stack {
            coordinator,
            worker: Some(worker),
            http: reqwest::Client::new(),
            redis_url,
            nats_url,
            _postgres: postgres,
            _redis: redis,
            _nats: nats,
            _slot: slot,
        }
    }

    /// Stops the worker the way a deploy would: sockets are closed and the
    /// shard leases are released.
    pub async fn stop_worker(&mut self) {
        if let Some(mut worker) = self.worker.take() {
            worker.shutdown().await;
        }
    }

    /// Starts a fresh worker (new port) that claims the same shards.
    pub async fn start_worker(&mut self) {
        assert!(self.worker.is_none(), "a worker is already running");
        self.worker = Some(spawn_worker(&self.redis_url, &self.nats_url).await);
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{}", self.coordinator.base, path)
    }

    pub fn operator_get(&self, path: &str) -> reqwest::RequestBuilder {
        self.http
            .get(self.url(path))
            .header("X-API-Key", OPERATOR_KEY)
    }

    pub async fn register(&self, lat: f64, lon: f64) -> Device {
        let body: Value = self
            .http
            .post(self.url("/devices/register"))
            .json(&json!({ "lat": lat, "lon": lon }))
            .send()
            .await
            .expect("register request")
            .error_for_status()
            .expect("register status")
            .json()
            .await
            .expect("register json");
        Device {
            id: body["device_id"]
                .as_str()
                .expect("device_id")
                .parse()
                .expect("device_id is a uuid"),
            token: body["token"].as_str().expect("token").to_owned(),
        }
    }

    /// The full `POST /devices/ws-ticket` answer: `ws_url`, `ticket`, `shard_prefix`.
    pub async fn ticket(&self, device: &Device) -> Value {
        self.http
            .post(self.url("/devices/ws-ticket"))
            .bearer_auth(&device.token)
            .send()
            .await
            .expect("ticket request")
            .error_for_status()
            .expect("ticket status")
            .json()
            .await
            .expect("ticket json")
    }

    /// Asks for a ticket and opens the WebSocket on the worker it names.
    pub async fn connect(&self, device: &Device) -> Socket {
        let ticket = self.ticket(device).await;
        let ws_url = ticket["ws_url"]
            .as_str()
            .expect("a worker owns this device's shard");
        let value = ticket["ticket"].as_str().expect("ticket value");
        match connect_with_ticket(ws_url, device.id, value).await {
            Ok(socket) => socket,
            Err(e) => panic!("websocket connect failed: {e}"),
        }
    }

    /// Dispatches an alert as an operator and returns the coordinator's answer.
    pub async fn send_alert(&self, lat: f64, lon: f64, radius_meters: f64) -> Value {
        let response = self
            .http
            .post(self.url("/alerts"))
            .header("X-API-Key", OPERATOR_KEY)
            .json(&json!({
                "lat": lat,
                "lon": lon,
                "radius_meters": radius_meters,
                "severity": "warning",
                "title": "end-to-end test",
                "message": "sent by the test suite",
            }))
            .send()
            .await
            .expect("alert request");
        assert!(
            response.status().is_success(),
            "alert dispatch answered {}",
            response.status()
        );
        response.json().await.expect("alert json")
    }

    /// Opens a Server-Sent Events stream as an operator.
    pub async fn open_stream(&self, path: &str) -> SseStream {
        let response = self
            .operator_get(path)
            .send()
            .await
            .expect("stream request");
        assert_eq!(response.status().as_u16(), 200, "stream status");
        SseStream {
            response,
            buffer: String::new(),
        }
    }
}

#[derive(Clone)]
pub struct Device {
    pub id: Uuid,
    pub token: String,
}

// -------------------------------------------------------------- services

pub struct Coordinator {
    pub base: String,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

pub struct Worker {
    pub ws_url: String,
    pub state: fanout_worker::WorkerState,
    tasks: Vec<JoinHandle<()>>,
}

impl Worker {
    async fn shutdown(&mut self) {
        self.state.registry.close_all();
        for task in &self.tasks {
            task.abort();
        }
        self.state.lease.release_all().await;
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn lookup(pairs: &[(&str, String)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

async fn spawn_coordinator(database_url: &str, redis_url: &str, nats_url: &str) -> Coordinator {
    api_coordinator::install_crypto_provider();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let vars = lookup(&[
        ("DATABASE_URL", database_url.to_owned()),
        ("REDIS_URL", redis_url.to_owned()),
        ("NATS_URL", nats_url.to_owned()),
        ("JWT_SECRET", "e2e-jwt-secret-only-for-tests".to_owned()),
        ("OPERATOR_API_KEYS", OPERATOR_KEY.to_owned()),
        ("RATE_LIMIT_DISABLED", "true".to_owned()),
        ("COORDINATOR_PORT", addr.port().to_string()),
    ]);
    let config = api_coordinator::Config::from_lookup(&|k| vars.get(k).cloned())
        .expect("coordinator config");
    let state = api_coordinator::build_state(&config)
        .await
        .expect("coordinator state");
    let mut tasks = api_coordinator::start_background_tasks(&state, &config);
    let app = api_coordinator::build_router(state, &config.rate_limits);
    tasks.push(tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    }));

    Coordinator {
        base: format!("http://127.0.0.1:{}", addr.port()),
        tasks,
    }
}

async fn spawn_worker(redis_url: &str, nats_url: &str) -> Worker {
    fanout_worker::install_crypto_provider();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let ws_url = format!("ws://127.0.0.1:{}", addr.port());

    let vars = lookup(&[
        ("REDIS_URL", redis_url.to_owned()),
        ("NATS_URL", nats_url.to_owned()),
        ("FANOUT_WORKER_SHARD_PREFIXES", SHARD_PREFIXES.to_owned()),
        ("FANOUT_WORKER_PORT", addr.port().to_string()),
        ("WORKER_PUBLIC_URL", ws_url.clone()),
        ("OPERATOR_API_KEYS", OPERATOR_KEY.to_owned()),
        ("RATE_LIMIT_DISABLED", "true".to_owned()),
    ]);
    let config =
        fanout_worker::Config::from_lookup(&|k| vars.get(k).cloned()).expect("worker config");
    let state = fanout_worker::build_state(config)
        .await
        .expect("worker state (is another worker still holding the shards?)");
    let mut tasks = fanout_worker::start_background_tasks(&state);
    let app = fanout_worker::build_router(state.clone());
    tasks.push(tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await;
    }));

    // Wait until every shard subscription is live, or alerts could be missed.
    let deadline = Instant::now() + Duration::from_secs(15);
    while state.health.subscriptions_up.load(Ordering::Relaxed)
        < state.health.expected_subscriptions
    {
        assert!(
            Instant::now() < deadline,
            "the worker's shard subscriptions never came up"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Worker {
        ws_url,
        state,
        tasks,
    }
}

// ------------------------------------------------------------ containers

async fn start_postgres() -> (String, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres testcontainer (is Docker running?)");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    // The port accepts connections a little before Postgres accepts logins.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match infra::db::connect(&url).await {
            Ok(pool) => {
                drop(pool);
                break;
            }
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "postgres never became ready: {e:?}"
                );
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    (url, container)
}

async fn start_redis() -> (String, ContainerAsync<Redis>) {
    let container = Redis::default()
        .with_tag("7-alpine")
        .start()
        .await
        .expect("start redis testcontainer (is Docker running?)");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("mapped redis port");
    (format!("redis://127.0.0.1:{port}"), container)
}

async fn start_nats() -> (String, ContainerAsync<Nats>) {
    let container = Nats::default()
        .with_cmd(&NatsServerCmd::default().with_jetstream())
        .start()
        .await
        .expect("start nats testcontainer (is Docker running?)");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("mapped nats port");
    (format!("nats://127.0.0.1:{port}"), container)
}

// ------------------------------------------------------------- websocket

type WsStreamInner = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// What arrived on a device's socket.
#[derive(Debug)]
pub enum Frame {
    /// A binary frame: an alert.
    Alert(Value),
    /// A text frame: a control message such as `replay_complete`.
    Control(Value),
    /// The server closed the connection (or it broke).
    Closed,
}

pub struct Replay {
    pub alert_ids: Vec<String>,
    pub reported: u64,
}

pub struct Socket {
    inner: WsStreamInner,
}

pub async fn connect_with_ticket(
    ws_url: &str,
    device_id: Uuid,
    ticket: &str,
) -> Result<Socket, WsError> {
    let url = format!("{ws_url}/connect/{device_id}?ticket={ticket}");
    let (inner, _) = tokio_tungstenite::connect_async(url.as_str()).await?;
    Ok(Socket { inner })
}

/// The HTTP status the server refused the WebSocket upgrade with, if it did.
pub fn http_status(error: &WsError) -> Option<u16> {
    if let WsError::Http(response) = error {
        Some(response.status().as_u16())
    } else {
        None
    }
}

impl Socket {
    /// The next frame, or `None` if nothing arrives within `within`.
    pub async fn next_frame(&mut self, within: Duration) -> Option<Frame> {
        let deadline = Instant::now() + within;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let next = tokio::time::timeout(remaining, self.inner.next())
                .await
                .ok()?;
            match next {
                Some(Ok(Message::Binary(bytes))) => {
                    return Some(Frame::Alert(
                        serde_json::from_slice(&bytes).expect("alert frames are JSON"),
                    ));
                }
                Some(Ok(Message::Text(text))) => {
                    return Some(Frame::Control(
                        serde_json::from_str(text.as_str()).expect("control frames are JSON"),
                    ));
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return Some(Frame::Closed),
                Some(Ok(_)) => {} // ping, pong
            }
        }
    }

    /// The next alert, skipping control messages; `None` on timeout or close.
    pub async fn next_alert(&mut self, within: Duration) -> Option<Value> {
        let deadline = Instant::now() + within;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.next_frame(remaining).await {
                Some(Frame::Alert(alert)) => return Some(alert),
                Some(Frame::Control(_)) => {}
                Some(Frame::Closed) | None => return None,
            }
        }
    }

    /// Reads until the server announces the connect-time replay is done.
    pub async fn finish_replay(&mut self) -> Replay {
        let mut alert_ids = Vec::new();
        loop {
            match self.next_frame(Duration::from_secs(15)).await {
                Some(Frame::Alert(alert)) => {
                    alert_ids.push(alert["id"].as_str().expect("alert id").to_owned());
                }
                Some(Frame::Control(control)) if control["type"] == "replay_complete" => {
                    return Replay {
                        alert_ids,
                        reported: control["replayed"].as_u64().expect("replayed count"),
                    };
                }
                Some(Frame::Control(_)) => {}
                Some(Frame::Closed) => panic!("the server closed the socket during the replay"),
                None => panic!("the replay did not finish within 15 seconds"),
            }
        }
    }

    /// Acknowledges an alert the way a device does.
    pub async fn ack(&mut self, alert: &Value) {
        let message = json!({
            "type": "ack",
            "alert_id": alert["id"],
            "created_at": alert["created_at"],
        });
        self.inner
            .send(Message::Text(message.to_string().into()))
            .await
            .expect("send ack");
    }

    /// Starts the close handshake and checks the server answers it properly
    /// (a reset without a close reply is an error).
    pub async fn close_cleanly(&mut self) -> Result<(), String> {
        self.inner
            .send(Message::Close(None))
            .await
            .map_err(|e| format!("sending the close frame: {e}"))?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, self.inner.next()).await {
                Err(_) => return Err("the server never answered the close".to_owned()),
                Ok(None) | Ok(Some(Ok(Message::Close(_)))) => return Ok(()),
                Ok(Some(Err(e))) => return Err(format!("the close handshake failed: {e}")),
                Ok(Some(Ok(_))) => {}
            }
        }
    }
}

// ------------------------------------------------------------------- sse

pub struct SseStream {
    response: reqwest::Response,
    buffer: String,
}

impl SseStream {
    /// The data of the next event with this name, or `None` on timeout.
    pub async fn next_event(&mut self, name: &str, within: Duration) -> Option<Value> {
        let deadline = Instant::now() + within;
        loop {
            while let Some(end) = self.buffer.find("\n\n") {
                let block: String = self.buffer.drain(..end + 2).collect();
                if let Some(data) = event_data(&block, name) {
                    return Some(data);
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, self.response.chunk()).await {
                Ok(Ok(Some(chunk))) => self.buffer.push_str(&String::from_utf8_lossy(&chunk)),
                _ => return None,
            }
        }
    }
}

fn event_data(block: &str, name: &str) -> Option<Value> {
    let mut event = "";
    let mut data = String::new();
    for line in block.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            event = value.trim();
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push_str(value.trim_start());
        }
    }
    if event == name && !data.is_empty() {
        serde_json::from_str(&data).ok()
    } else {
        None
    }
}
