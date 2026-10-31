//! Tests for the Postgres layer added in Phase 11: radius-aware counts,
//! circle queries (including across the antimeridian), device heartbeat /
//! deletion / retention purge, and the alert audit trail with cascading
//! deletes.
//!
//! By default each test starts its own Postgres 16 container (needs
//! Docker). To reuse a running server instead, set
//! `PULSEGRID_TEST_DATABASE_URL` (for example
//! `postgres://postgres:postgres@localhost:5432/postgres`); every test then
//! creates its own throw-away database on that server.

use std::collections::HashSet;

use chrono::{Duration, Utc};
use domain::device::Location;
use domain::{geo, Alert, AlertSeverity, AppError, Device, DeviceId};
use infra::db::{AlertRepository, DeviceRepository, ShardCounts};
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

struct TestDb {
    pool: PgPool,
    // Keeps the container alive for the duration of the test.
    _container: Option<ContainerAsync<Postgres>>,
}

async fn test_db() -> TestDb {
    let (url, container) = match std::env::var("PULSEGRID_TEST_DATABASE_URL") {
        Ok(admin_url) => {
            let admin = PgPool::connect(&admin_url)
                .await
                .expect("connect to PULSEGRID_TEST_DATABASE_URL");
            let name = format!("pg_test_{}", Uuid::new_v4().simple());
            sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
                .execute(&admin)
                .await
                .expect("create throw-away database");
            let (base, _) = admin_url
                .rsplit_once('/')
                .expect("database url must end with /<dbname>");
            (format!("{base}/{name}"), None)
        }
        Err(_) => {
            let container = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .await
                .expect("start postgres testcontainer (is Docker running?)");
            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("mapped postgres port");
            (
                format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
                Some(container),
            )
        }
    };

    let pool = connect_with_retry(&url).await;
    infra::db::run_migrations(&pool).await.expect("migrate");
    TestDb {
        pool,
        _container: container,
    }
}

/// A freshly started container can take a while to accept connections on a
/// busy machine. Production code keeps its short connect timeout; tests wait.
async fn connect_with_retry(url: &str) -> PgPool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
    loop {
        match infra::db::connect(url).await {
            Ok(pool) => return pool,
            Err(e) if std::time::Instant::now() < deadline => {
                eprintln!("postgres is not accepting connections yet ({e}); retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Err(e) => panic!("could not connect to postgres within 90 s: {e}"),
        }
    }
}

fn device_at(lat: f64, lon: f64) -> Device {
    Device::register(Location::new(lat, lon).unwrap()).unwrap()
}

fn alert_at(lat: f64, lon: f64, radius: f64) -> Alert {
    Alert::new(
        Location::new(lat, lon).unwrap(),
        radius,
        AlertSeverity::Warning,
        "Test alert".into(),
        "Test message".into(),
    )
    .unwrap()
}

#[tokio::test]
async fn radius_counts_and_circle_listing_match_the_rust_geometry() {
    let db = test_db().await;
    let repo = DeviceRepository::new(db.pool.clone());

    // A 21 x 21 grid of devices (0.02 degree spacing) around Karachi.
    let center = Location::new(24.8607, 67.0011).unwrap();
    let radius = 8_000.0;
    let alert = alert_at(center.lat, center.lon, radius);
    let center_prefix = geo::shard_prefix_of(&geo::encode_location(&center).unwrap());

    let mut devices = Vec::new();
    for i in 0..21 {
        for j in 0..21 {
            let d = device_at(24.66 + i as f64 * 0.02, 66.80 + j as f64 * 0.02);
            repo.insert(&d).await.unwrap();
            devices.push(d);
        }
    }

    let in_shard: Vec<&Device> = devices
        .iter()
        .filter(|d| d.shard_prefix == center_prefix)
        .collect();
    let expected_ids: HashSet<Uuid> = in_shard
        .iter()
        .filter(|d| alert.contains(&d.location))
        .map(|d| d.id.0)
        .collect();

    // The test must have teeth: some devices inside, some outside.
    assert!(!expected_ids.is_empty());
    assert!(expected_ids.len() < in_shard.len());

    let counts = repo
        .count_in_shard_and_radius(&center_prefix, &center, radius)
        .await
        .unwrap();
    assert_eq!(counts.in_shard, in_shard.len() as i64);
    assert_eq!(counts.in_radius, expected_ids.len() as i64);
    assert_eq!(
        repo.count_in_shard(&center_prefix).await.unwrap(),
        in_shard.len() as i64
    );

    let listed = repo
        .list_in_circle(
            std::slice::from_ref(&center_prefix),
            &center,
            radius,
            10_000,
        )
        .await
        .unwrap();
    let listed_ids: HashSet<Uuid> = listed.iter().map(|d| d.id).collect();
    assert_eq!(listed_ids, expected_ids);
    assert!(listed.iter().all(|d| d.shard_prefix == center_prefix));

    let limited = repo
        .list_in_circle(&[center_prefix], &center, radius, 5)
        .await
        .unwrap();
    assert_eq!(limited.len(), 5);

    assert_eq!(repo.count_all().await.unwrap(), devices.len() as i64);
}

#[tokio::test]
async fn circle_queries_work_across_the_antimeridian() {
    let db = test_db().await;
    let repo = DeviceRepository::new(db.pool.clone());

    let east = device_at(0.0, 179.99);
    let west = device_at(0.0, -179.99);
    let far = device_at(0.0, 178.0);
    for d in [&east, &west, &far] {
        repo.insert(d).await.unwrap();
    }

    let center = Location::new(0.0, 179.995).unwrap();
    let prefixes: Vec<String> = [&east, &west, &far]
        .iter()
        .map(|d| d.shard_prefix.clone())
        .collect();

    let listed = repo
        .list_in_circle(&prefixes, &center, 5_000.0, 100)
        .await
        .unwrap();
    let ids: HashSet<Uuid> = listed.iter().map(|d| d.id).collect();
    assert_eq!(ids, HashSet::from([east.id.0, west.id.0]));

    let counts = repo
        .count_in_shard_and_radius(&west.shard_prefix, &center, 5_000.0)
        .await
        .unwrap();
    assert_eq!(counts.in_radius, 1);
}

#[tokio::test]
async fn device_heartbeat_delete_and_retention_purge() {
    let db = test_db().await;
    let repo = DeviceRepository::new(db.pool.clone());

    let mut stale_a = device_at(24.86, 67.00);
    stale_a.last_seen = Utc::now() - Duration::days(120);
    let mut stale_b = device_at(24.87, 67.01);
    stale_b.last_seen = Utc::now() - Duration::days(100);
    let fresh = device_at(24.88, 67.02);
    for d in [&stale_a, &stale_b, &fresh] {
        repo.insert(d).await.unwrap();
    }

    // Purge in batches of one: oldest first, then the next, then nothing.
    let cutoff = Utc::now() - Duration::days(90);
    assert_eq!(repo.purge_inactive(cutoff, 1).await.unwrap(), 1);
    assert!(matches!(
        repo.find_by_id(stale_a.id).await,
        Err(AppError::DeviceNotFound(_))
    ));
    assert_eq!(repo.purge_inactive(cutoff, 1).await.unwrap(), 1);
    assert_eq!(repo.purge_inactive(cutoff, 1).await.unwrap(), 0);
    assert_eq!(repo.count_all().await.unwrap(), 1);

    // touch refreshes last_seen and rejects unknown devices.
    let before = repo.find_by_id(fresh.id).await.unwrap().last_seen;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    repo.touch(fresh.id).await.unwrap();
    let after = repo.find_by_id(fresh.id).await.unwrap().last_seen;
    assert!(after > before);
    assert!(matches!(
        repo.touch(DeviceId(Uuid::new_v4())).await,
        Err(AppError::DeviceNotFound(_))
    ));

    // delete reports whether a row existed.
    assert!(repo.delete(fresh.id).await.unwrap());
    assert!(!repo.delete(fresh.id).await.unwrap());
    assert_eq!(repo.count_all().await.unwrap(), 0);
}

#[tokio::test]
async fn alert_audit_trail_is_idempotent_and_retention_cascades() {
    let db = test_db().await;
    let alerts = AlertRepository::new(db.pool.clone());

    let mut old = alert_at(24.86, 67.00, 5_000.0);
    old.created_at = Utc::now() - Duration::days(400);
    let recent = alert_at(24.86, 67.00, 5_000.0);
    alerts.insert_alert(&old).await.unwrap();
    alerts.insert_alert(&recent).await.unwrap();

    // Round trip through Postgres (timestamptz keeps microseconds).
    let fetched = alerts.get_alert(recent.id).await.unwrap();
    assert_eq!(fetched.id, recent.id);
    assert_eq!(fetched.severity, AlertSeverity::Warning);
    assert_eq!(fetched.title, "Test alert");
    assert_eq!(fetched.radius_meters, 5_000.0);
    assert!(
        (fetched.created_at - recent.created_at)
            .num_milliseconds()
            .abs()
            < 2
    );
    assert!(matches!(
        alerts.get_alert(domain::AlertId::new()).await,
        Err(AppError::NotFound(_))
    ));

    // Newest first.
    let listed = alerts.recent_alerts(10).await.unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].id, recent.id);
    assert_eq!(listed[1].id, old.id);

    // The second write for the same (alert, shard) is ignored.
    let first = ShardCounts {
        in_shard: 10,
        in_radius: 4,
    };
    let second = ShardCounts {
        in_shard: 99,
        in_radius: 99,
    };
    alerts
        .record_shard_delivery(old.id, "tkrt", first)
        .await
        .unwrap();
    alerts
        .record_shard_delivery(old.id, "tkrt", second)
        .await
        .unwrap();
    // The legacy form still works and stores in_radius = 0.
    alerts.record_delivery(old.id, "tkrw", 7).await.unwrap();

    let deliveries = alerts.list_deliveries(old.id).await.unwrap();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].shard_prefix, "tkrt");
    assert_eq!(deliveries[0].devices_in_shard, 10);
    assert_eq!(deliveries[0].devices_in_radius, 4);
    assert_eq!(deliveries[1].shard_prefix, "tkrw");
    assert_eq!(deliveries[1].devices_in_shard, 7);
    assert_eq!(deliveries[1].devices_in_radius, 0);

    // Retention: the old alert goes, and its delivery rows cascade.
    let cutoff = Utc::now() - Duration::days(365);
    assert_eq!(alerts.purge_older_than(cutoff, 100).await.unwrap(), 1);
    assert!(alerts.get_alert(old.id).await.is_err());
    assert!(alerts.get_alert(recent.id).await.is_ok());
    let (orphans,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM alert_deliveries")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(orphans, 0);
}

#[tokio::test]
async fn ping_succeeds_against_a_live_database() {
    let db = test_db().await;
    infra::db::ping_with_timeout(&db.pool, std::time::Duration::from_secs(30))
        .await
        .unwrap();
}
