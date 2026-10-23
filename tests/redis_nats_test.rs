//! Tests for the Redis stores and the NATS / JetStream helpers added in
//! Phase 12: shard leases, device locations, one-time WebSocket tickets,
//! revocation, delivery stats, replay cursors, durable alert replay,
//! control and telemetry messages.
//!
//! By default each test starts its own Redis and/or NATS (JetStream
//! enabled) container (needs Docker). To use servers you already run, set
//! `PULSEGRID_TEST_REDIS_URL` (e.g. `redis://localhost:6379`) and/or
//! `PULSEGRID_TEST_NATS_URL` (e.g. `nats://localhost:4222`, started with
//! `-js`). All keys and subjects are unique per test, so sharing a server
//! is safe.
//!
//! The replay tests compare timestamps taken on this machine with the
//! NATS server's clock, so host and server clocks must be roughly in sync
//! (true for local servers and normal Docker setups).

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use domain::device::Location;
use domain::protocol::{alert_subject, control_subject, TELEMETRY_DELIVERY_SUBJECT};
use domain::{new_ws_ticket, Alert, AlertSeverity, DeliveryTelemetry, ServerMessage};
use futures_util::StreamExt;
use infra::redis::{DeviceLocationRecord, Pool};
use infra::{
    AckCursorStore, AlertStatsStore, DeviceLocationStore, PresenceRegistry, RevocationStore,
    ShardRegistry, WsTicketStore,
};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::redis::{Redis, REDIS_PORT};
use uuid::Uuid;

struct TestRedis {
    pool: Pool,
    _container: Option<ContainerAsync<Redis>>,
}

async fn test_redis() -> TestRedis {
    let (url, container) = match std::env::var("PULSEGRID_TEST_REDIS_URL") {
        Ok(url) => (url, None),
        Err(_) => {
            let container = Redis::default()
                .start()
                .await
                .expect("start redis testcontainer (is Docker running?)");
            let port = container
                .get_host_port_ipv4(REDIS_PORT)
                .await
                .expect("mapped redis port");
            (format!("redis://127.0.0.1:{port}"), Some(container))
        }
    };
    let pool = infra::redis::build_pool(&url).expect("build redis pool");
    infra::redis::ping_with_timeout(&pool, Duration::from_secs(30))
        .await
        .expect("redis reachable");
    TestRedis {
        pool,
        _container: container,
    }
}

struct TestNats {
    client: infra::nats::Client,
    _container: Option<ContainerAsync<Nats>>,
}

async fn test_nats() -> TestNats {
    let (url, container) = match std::env::var("PULSEGRID_TEST_NATS_URL") {
        Ok(url) => (url, None),
        Err(_) => {
            let container = Nats::default()
                .with_cmd(&NatsServerCmd::default().with_jetstream())
                .start()
                .await
                .expect("start nats testcontainer (is Docker running?)");
            let port = container
                .get_host_port_ipv4(4222)
                .await
                .expect("mapped nats port");
            (format!("nats://127.0.0.1:{port}"), Some(container))
        }
    };
    let client = infra::nats::connect(&url).await.expect("connect to nats");
    infra::nats::ensure_alert_stream(&client, Duration::from_secs(3600))
        .await
        .expect("create alert stream (JetStream must be enabled)");
    TestNats {
        client,
        _container: container,
    }
}

/// A shard-prefix-like string that never collides with another test.
fn uniq() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

fn alert_at(lat: f64, lon: f64, title: &str) -> Alert {
    Alert::new(
        Location::new(lat, lon).unwrap(),
        5_000.0,
        AlertSeverity::Critical,
        title.into(),
        "message".into(),
    )
    .unwrap()
}

// ----------------------------- Redis ---------------------------------

#[tokio::test]
async fn shard_lease_is_exclusive_refreshable_and_expires() {
    let r = test_redis().await;
    let shards = ShardRegistry::new(r.pool.clone());
    let shard = uniq();
    let (a, b) = ("ws://worker-a:8081", "ws://worker-b:8082");

    assert!(shards.claim_shard(&shard, a, 30).await.unwrap());
    assert_eq!(
        shards.lookup_shard(&shard).await.unwrap().as_deref(),
        Some(a)
    );
    // The same owner can refresh; a second worker cannot take it.
    assert!(shards.claim_shard(&shard, a, 30).await.unwrap());
    assert!(!shards.claim_shard(&shard, b, 30).await.unwrap());
    assert_eq!(
        shards.lookup_shard(&shard).await.unwrap().as_deref(),
        Some(a)
    );

    // Only the owner can release.
    assert!(!shards.release_shard(&shard, b).await.unwrap());
    assert!(shards.release_shard(&shard, a).await.unwrap());
    assert_eq!(shards.lookup_shard(&shard).await.unwrap(), None);
    assert!(shards.claim_shard(&shard, b, 30).await.unwrap());

    // A lease that is not refreshed expires, freeing the shard.
    let short = uniq();
    assert!(shards.claim_shard(&short, a, 1).await.unwrap());
    tokio::time::sleep(Duration::from_millis(1_600)).await;
    assert_eq!(shards.lookup_shard(&short).await.unwrap(), None);
    assert!(shards.claim_shard(&short, b, 30).await.unwrap());
}

#[tokio::test]
async fn legacy_shard_registration_and_presence_still_work() {
    let r = test_redis().await;
    let shards = ShardRegistry::new(r.pool.clone());
    let shard = uniq();
    shards
        .register_shard(&shard, "ws://legacy:1")
        .await
        .unwrap();
    assert_eq!(
        shards.lookup_shard(&shard).await.unwrap().as_deref(),
        Some("ws://legacy:1")
    );
    // A legacy (TTL-less) entry for the same URL can be adopted as a lease.
    assert!(shards
        .claim_shard(&shard, "ws://legacy:1", 30)
        .await
        .unwrap());
    assert!(!shards
        .claim_shard(&shard, "ws://other:2", 30)
        .await
        .unwrap());

    let presence = PresenceRegistry::new(r.pool.clone());
    let device = Uuid::new_v4().to_string();
    presence.register(&device, "worker-1").await.unwrap();
    presence.heartbeat(&device).await.unwrap();
    assert_eq!(
        presence.lookup(&device).await.unwrap().as_deref(),
        Some("worker-1")
    );
    presence.deregister(&device).await.unwrap();
    assert_eq!(presence.lookup(&device).await.unwrap(), None);
}

#[tokio::test]
async fn device_location_roundtrip_delete_and_expiry() {
    let r = test_redis().await;
    let store = DeviceLocationStore::new(r.pool.clone());
    let id = Uuid::new_v4();
    let record = DeviceLocationRecord {
        lat: 24.8607,
        lon: 67.0011,
        shard_prefix: "tkrt".into(),
    };

    assert_eq!(store.get(id).await.unwrap(), None);
    store.set(id, &record, 60).await.unwrap();
    let got = store.get(id).await.unwrap().unwrap();
    assert_eq!(got, record);
    assert_eq!(got.location().unwrap().lat, 24.8607);
    store.delete(id).await.unwrap();
    assert_eq!(store.get(id).await.unwrap(), None);

    store.set(id, &record, 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_600)).await;
    assert_eq!(store.get(id).await.unwrap(), None);
}

#[tokio::test]
async fn ws_tickets_are_single_use_and_expire() {
    let r = test_redis().await;
    let tickets = WsTicketStore::new(r.pool.clone());
    let device = Uuid::new_v4();

    let ticket = new_ws_ticket();
    tickets.issue(&ticket, device, 30).await.unwrap();
    assert_eq!(tickets.consume(&ticket).await.unwrap(), Some(device));
    assert_eq!(tickets.consume(&ticket).await.unwrap(), None, "single use");

    let short = new_ws_ticket();
    tickets.issue(&short, device, 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1_600)).await;
    assert_eq!(tickets.consume(&short).await.unwrap(), None, "expired");

    // Unknown and malformed tickets never touch Redis state.
    assert_eq!(tickets.consume(&new_ws_ticket()).await.unwrap(), None);
    assert_eq!(tickets.consume("short").await.unwrap(), None);
    assert_eq!(tickets.consume(&"z".repeat(64)).await.unwrap(), None);
    assert_eq!(tickets.consume("").await.unwrap(), None);
}

#[tokio::test]
async fn revocation_marks_a_device_until_it_expires() {
    let r = test_redis().await;
    let store = RevocationStore::new(r.pool.clone());
    let device = Uuid::new_v4();

    assert!(!store.is_revoked(device).await.unwrap());
    store.revoke(device, 1).await.unwrap();
    assert!(store.is_revoked(device).await.unwrap());
    assert!(!store.is_revoked(Uuid::new_v4()).await.unwrap());
    tokio::time::sleep(Duration::from_millis(1_600)).await;
    assert!(!store.is_revoked(device).await.unwrap());
}

#[tokio::test]
async fn alert_stats_accumulate_per_shard_and_in_total() {
    let r = test_redis().await;
    let stats = AlertStatsStore::new(r.pool.clone());
    let alert = Uuid::new_v4();

    assert_eq!(stats.get(alert).await.unwrap(), None);
    stats.record(alert, "tkrt", 10, 1, 4, 60).await.unwrap();
    stats.record(alert, "tkrw", 5, 0, 2, 60).await.unwrap();
    stats.record(alert, "tkrt", 3, 2, 0, 60).await.unwrap(); // second worker report

    let got = stats.get(alert).await.unwrap().unwrap();
    assert_eq!((got.delivered, got.dropped, got.filtered_out), (18, 3, 6));
    assert_eq!(got.per_shard.len(), 2);
    let tkrt = got.per_shard["tkrt"];
    assert_eq!(
        (tkrt.delivered, tkrt.dropped, tkrt.filtered_out),
        (13, 3, 4)
    );
    let tkrw = got.per_shard["tkrw"];
    assert_eq!((tkrw.delivered, tkrw.dropped, tkrw.filtered_out), (5, 0, 2));
}

#[tokio::test]
async fn ack_cursor_only_moves_forward() {
    let r = test_redis().await;
    let cursors = AckCursorStore::new(r.pool.clone());
    let device = Uuid::new_v4();
    let t0 = Utc::now();

    assert_eq!(cursors.get(device).await.unwrap(), None);
    assert!(cursors.advance(device, t0, 60).await.unwrap());
    // An older (out-of-order) ack must not move it back.
    assert!(!cursors
        .advance(device, t0 - ChronoDuration::seconds(30), 60)
        .await
        .unwrap());
    let stored = cursors.get(device).await.unwrap().unwrap();
    assert!((stored - t0).num_microseconds().unwrap().abs() <= 1);

    let t1 = t0 + ChronoDuration::seconds(5);
    assert!(cursors.advance(device, t1, 60).await.unwrap());
    let stored = cursors.get(device).await.unwrap().unwrap();
    assert!((stored - t1).num_microseconds().unwrap().abs() <= 1);
}

#[tokio::test]
async fn redis_ping_works() {
    let r = test_redis().await;
    infra::redis::ping_with_timeout(&r.pool, Duration::from_secs(30))
        .await
        .unwrap();
}

// ----------------------------- NATS ----------------------------------

#[tokio::test]
async fn connection_state_and_stream_setup_are_idempotent() {
    let n = test_nats().await;
    assert!(infra::nats::is_connected(&n.client));
    // Calling it again, even with another retention, is fine (update).
    infra::nats::ensure_alert_stream(&n.client, Duration::from_secs(7200))
        .await
        .unwrap();
    infra::nats::ensure_alert_stream(&n.client, Duration::from_secs(3600))
        .await
        .unwrap();
}

#[tokio::test]
async fn durable_publish_and_replay_since_a_timestamp() {
    let n = test_nats().await;
    let shard = uniq();
    let subject = alert_subject(&shard);

    let first = alert_at(24.86, 67.00, "first");
    let seq1 =
        infra::nats::publish_durable(&n.client, &subject, serde_json::to_vec(&first).unwrap())
            .await
            .unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let cutoff = Utc::now();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let second = alert_at(24.87, 67.01, "second");
    let seq2 =
        infra::nats::publish_durable(&n.client, &subject, serde_json::to_vec(&second).unwrap())
            .await
            .unwrap();
    assert!(seq2 > seq1);

    let all =
        infra::nats::replay_alerts(&n.client, &shard, cutoff - ChronoDuration::seconds(60), 50)
            .await
            .unwrap();
    let titles: Vec<&str> = all.iter().map(|a| a.title.as_str()).collect();
    assert_eq!(titles, ["first", "second"], "oldest first");
    assert_eq!(all[0].id, first.id);
    assert_eq!(all[1].severity, AlertSeverity::Critical);

    let recent = infra::nats::replay_alerts(&n.client, &shard, cutoff, 50)
        .await
        .unwrap();
    let titles: Vec<&str> = recent.iter().map(|a| a.title.as_str()).collect();
    assert_eq!(titles, ["second"]);

    // Nothing newer than "now".
    let none = infra::nats::replay_alerts(
        &n.client,
        &shard,
        Utc::now() + ChronoDuration::seconds(5),
        50,
    )
    .await
    .unwrap();
    assert!(none.is_empty());
}

#[tokio::test]
async fn replay_respects_the_limit_and_isolates_shards() {
    let n = test_nats().await;
    let (shard_a, shard_b) = (uniq(), uniq());
    let since = Utc::now() - ChronoDuration::seconds(60);

    for i in 0..5 {
        let a = alert_at(24.86, 67.00, &format!("a{i}"));
        infra::nats::publish_durable(
            &n.client,
            &alert_subject(&shard_a),
            serde_json::to_vec(&a).unwrap(),
        )
        .await
        .unwrap();
    }
    let b = alert_at(24.86, 67.00, "b0");
    infra::nats::publish_durable(
        &n.client,
        &alert_subject(&shard_b),
        serde_json::to_vec(&b).unwrap(),
    )
    .await
    .unwrap();

    let limited = infra::nats::replay_alerts(&n.client, &shard_a, since, 3)
        .await
        .unwrap();
    assert_eq!(limited.len(), 3);
    assert!(limited.iter().all(|a| a.title.starts_with('a')));

    let only_b = infra::nats::replay_alerts(&n.client, &shard_b, since, 50)
        .await
        .unwrap();
    assert_eq!(only_b.len(), 1);
    assert_eq!(only_b[0].title, "b0");
}

#[tokio::test]
async fn durable_alerts_still_reach_plain_subscribers() {
    // The live fanout path keeps using ordinary subscriptions; this proves
    // publishing through JetStream does not change that.
    let n = test_nats().await;
    let shard = uniq();
    let subject = alert_subject(&shard);
    let mut sub = n.client.subscribe(subject.clone()).await.unwrap();

    let alert = alert_at(24.86, 67.00, "live");
    infra::nats::publish_durable(&n.client, &subject, serde_json::to_vec(&alert).unwrap())
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("live delivery within 5s")
        .expect("subscription open");
    let got: Alert = serde_json::from_slice(&msg.payload).unwrap();
    assert_eq!(got.id, alert.id);
}

#[tokio::test]
async fn durable_publish_fails_fast_when_no_stream_captures_the_subject() {
    let n = test_nats().await;
    let started = std::time::Instant::now();
    let result = infra::nats::publish_durable(
        &n.client,
        &format!("not.an.alert.subject.{}", uniq()),
        b"x".to_vec(),
    )
    .await;
    assert!(result.is_err());
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn control_and_telemetry_messages_are_delivered() {
    let n = test_nats().await;

    let device = Uuid::new_v4();
    let mut control = n.client.subscribe(control_subject(device)).await.unwrap();
    let message = ServerMessage::ShardChanged {
        shard_prefix: "tkrw".into(),
        ws_url: Some("ws://worker-b:8082".into()),
    };
    infra::nats::publish_control(&n.client, device, &message)
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), control.next())
        .await
        .expect("control message within 5s")
        .unwrap();
    let parsed: ServerMessage = serde_json::from_slice(&got.payload).unwrap();
    assert_eq!(parsed, message);

    let mut telemetry = n
        .client
        .subscribe(TELEMETRY_DELIVERY_SUBJECT.to_string())
        .await
        .unwrap();
    let report = DeliveryTelemetry {
        alert_id: Uuid::new_v4(),
        shard_prefix: "tkrt".into(),
        delivered: 12,
        dropped: 1,
        filtered_out: 30,
        broadcast_duration_us: 87,
        worker_id: "worker-a".into(),
        at: Utc::now(),
    };
    infra::nats::publish_telemetry(&n.client, &report)
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), telemetry.next())
        .await
        .expect("telemetry within 5s")
        .unwrap();
    let parsed: DeliveryTelemetry = serde_json::from_slice(&got.payload).unwrap();
    assert_eq!(parsed.alert_id, report.alert_id);
    assert_eq!(parsed.delivered, 12);
}
