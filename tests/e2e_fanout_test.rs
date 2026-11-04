//! End to end: real coordinator, real fanout-worker, real sockets.
//!
//! Each test starts its own Postgres, Redis and NATS (needs Docker) and talks
//! to the services over HTTP, WebSocket and Server-Sent Events.

mod common;

use std::time::Duration;

use common::Stack;
use serde_json::Value;

/// Karachi, inside shard tkrt.
const CENTER: (f64, f64) = (24.8607, 67.0011);
/// About 6.7 km south of CENTER: the same shard, but outside a 3 km circle.
const OUTSIDE: (f64, f64) = (24.8000, 67.0011);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_alert_reaches_the_devices_inside_the_circle_and_only_those() {
    let stack = Stack::start().await;
    let inside = stack.register(CENTER.0, CENTER.1).await;
    let outside = stack.register(OUTSIDE.0, OUTSIDE.1).await;
    let mut inside_socket = stack.connect(&inside).await;
    let mut outside_socket = stack.connect(&outside).await;
    inside_socket.finish_replay().await;
    outside_socket.finish_replay().await;

    let sent = stack.send_alert(CENTER.0, CENTER.1, 3000.0).await;
    let alert_id = sent["alert_id"].as_str().expect("alert_id");

    let received = inside_socket
        .next_alert(Duration::from_secs(10))
        .await
        .expect("the device inside the circle must receive the alert");
    assert_eq!(received["id"], alert_id);

    match outside_socket.next_frame(Duration::from_secs(2)).await {
        None => {}
        other => panic!("the device outside the circle received {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivery_reports_reach_the_dashboard_stream() {
    let stack = Stack::start().await;
    let device = stack.register(CENTER.0, CENTER.1).await;
    let mut socket = stack.connect(&device).await;
    socket.finish_replay().await;

    let mut stream = stack.open_stream("/dashboard/stream").await;
    let sent = stack.send_alert(CENTER.0, CENTER.1, 3000.0).await;
    let alert_id = sent["alert_id"].as_str().expect("alert_id");

    // One report per shard arrives; add up what they delivered to live sockets.
    let mut delivered = 0u64;
    while let Some(event) = stream.next_event("delivery", Duration::from_secs(10)).await {
        if event["alert_id"] == alert_id {
            delivered += event["delivered"].as_u64().unwrap_or(0);
            if delivered >= 1 {
                break;
            }
        }
    }
    assert!(
        delivered >= 1,
        "the dashboard stream never reported a delivery for the alert"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_dashboard_lists_the_registered_devices_inside_a_circle() {
    let stack = Stack::start().await;
    let inside = stack.register(CENTER.0, CENTER.1).await;
    let outside = stack.register(OUTSIDE.0, OUTSIDE.1).await;

    let body: Value = stack
        .operator_get(&format!(
            "/dashboard/devices?lat={}&lon={}&radius_meters=3000",
            CENTER.0, CENTER.1
        ))
        .send()
        .await
        .expect("devices request")
        .error_for_status()
        .expect("devices status")
        .json()
        .await
        .expect("devices json");

    let ids: Vec<&str> = body["devices"]
        .as_array()
        .expect("a devices array")
        .iter()
        .filter_map(|device| device["id"].as_str())
        .collect();
    let inside_id = inside.id.to_string();
    let outside_id = outside.id.to_string();
    assert!(ids.contains(&inside_id.as_str()), "{ids:?}");
    assert!(!ids.contains(&outside_id.as_str()), "{ids:?}");
    assert_eq!(body["truncated"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_dashboard_page_and_its_assets_are_served_without_a_key() {
    let stack = Stack::start().await;

    let page = stack
        .http
        .get(stack.url("/dashboard"))
        .send()
        .await
        .expect("page request");
    assert_eq!(page.status().as_u16(), 200);
    assert!(page
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("text/html")));
    assert!(page.headers().contains_key("content-security-policy"));

    for path in ["/dashboard/app.js", "/dashboard/app.css"] {
        let asset = stack
            .http
            .get(stack.url(path))
            .send()
            .await
            .expect("asset request");
        assert_eq!(asset.status().as_u16(), 200, "{path}");
    }
}
