//! What happens when the worker goes away.

mod common;

use std::time::Duration;

use common::{Frame, Stack};

const CENTER: (f64, f64) = (24.8607, 67.0011);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_alert_sent_while_the_worker_is_down_is_replayed_by_the_next_one() {
    let mut stack = Stack::start().await;
    let device = stack.register(CENTER.0, CENTER.1).await;
    let mut socket = stack.connect(&device).await;
    socket.finish_replay().await;

    // The worker is stopped, as in a deploy: sockets are closed.
    stack.stop_worker().await;
    loop {
        match socket.next_frame(Duration::from_secs(10)).await {
            Some(Frame::Closed) => break,
            Some(_) => {}
            None => panic!("the socket stayed open after the worker stopped"),
        }
    }

    // An alert goes out with nobody to deliver it live. The coordinator still
    // stores it durably and says so.
    let sent = stack.send_alert(CENTER.0, CENTER.1, 3000.0).await;
    let alert_id = sent["alert_id"].as_str().expect("alert_id").to_owned();

    // A new worker starts on another port and claims the shards. The device
    // asks for a ticket again and is sent to the new address.
    stack.start_worker().await;
    let mut again = stack.connect(&device).await;
    let replay = again.finish_replay().await;

    assert_eq!(replay.alert_ids, vec![alert_id]);
    assert_eq!(replay.reported, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_worker_a_device_is_told_there_is_no_address() {
    let mut stack = Stack::start().await;
    let device = stack.register(CENTER.0, CENTER.1).await;

    stack.stop_worker().await;
    let ticket = stack.ticket(&device).await;
    assert!(
        ticket["ws_url"].is_null(),
        "a ticket named a worker that is gone: {ticket}"
    );

    // Once a worker is back, the same device gets an address again.
    stack.start_worker().await;
    let ticket = stack.ticket(&device).await;
    assert!(ticket["ws_url"].is_string(), "{ticket}");
}
