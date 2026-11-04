//! Durable delivery: a device that was away gets what it missed, and only that.

mod common;

use std::time::Duration;

use common::Stack;

const CENTER: (f64, f64) = (24.8607, 67.0011);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_returning_device_gets_only_the_alerts_it_missed() {
    let stack = Stack::start().await;
    let device = stack.register(CENTER.0, CENTER.1).await;

    // Online: receives and acknowledges alert A.
    let mut first = stack.connect(&device).await;
    assert!(first.finish_replay().await.alert_ids.is_empty());
    let a = stack.send_alert(CENTER.0, CENTER.1, 3000.0).await;
    let a_id = a["alert_id"].as_str().expect("alert_id").to_owned();
    let received = first
        .next_alert(Duration::from_secs(10))
        .await
        .expect("the online device receives A");
    assert_eq!(received["id"], a_id.as_str());
    first.ack(&received).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The worker must answer the close handshake (the Phase 16 fix).
    first
        .close_cleanly()
        .await
        .expect("the worker answers the close handshake");

    // Offline: alert B is sent.
    let b = stack.send_alert(CENTER.0, CENTER.1, 3000.0).await;
    let b_id = b["alert_id"].as_str().expect("alert_id").to_owned();

    // Back online: exactly B is replayed, not A.
    let mut second = stack.connect(&device).await;
    let replay = second.finish_replay().await;
    assert_eq!(replay.alert_ids, vec![b_id]);
    assert_eq!(replay.reported, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_device_is_sent_the_recent_alerts_of_its_area() {
    let stack = Stack::start().await;

    // The alert goes out before the device even exists.
    let sent = stack.send_alert(CENTER.0, CENTER.1, 3000.0).await;
    let alert_id = sent["alert_id"].as_str().expect("alert_id").to_owned();

    let device = stack.register(CENTER.0, CENTER.1).await;
    let mut socket = stack.connect(&device).await;
    let replay = socket.finish_replay().await;

    assert_eq!(replay.alert_ids, vec![alert_id]);
    assert_eq!(replay.reported, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_device_is_not_sent_alerts_for_other_places() {
    let stack = Stack::start().await;

    // 3 km circle centred about 6.7 km away from where the device registers.
    stack.send_alert(24.8000, 67.0011, 3000.0).await;

    let device = stack.register(CENTER.0, CENTER.1).await;
    let mut socket = stack.connect(&device).await;
    let replay = socket.finish_replay().await;

    assert!(replay.alert_ids.is_empty(), "{:?}", replay.alert_ids);
    assert_eq!(replay.reported, 0);
}
