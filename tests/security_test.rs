//! Who may do what: operator keys, device tokens and one-time tickets.

mod common;

use std::time::Duration;

use common::{connect_with_ticket, http_status, Frame, Stack, OPERATOR_KEY};
use reqwest::Method;
use serde_json::json;
use uuid::Uuid;

const CENTER: (f64, f64) = (24.8607, 67.0011);

fn refused(status: u16) -> bool {
    matches!(status, 401 | 403)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operator_endpoints_refuse_requests_without_a_valid_key() {
    let stack = Stack::start().await;
    let id = Uuid::new_v4();
    let alert = json!({
        "lat": 24.86, "lon": 67.0, "radius_meters": 1000,
        "severity": "info", "title": "t", "message": "m",
    });
    let cases: Vec<(Method, String)> = vec![
        (Method::POST, "/alerts".to_owned()),
        (Method::GET, "/alerts".to_owned()),
        (Method::GET, format!("/alerts/{id}")),
        (Method::GET, "/dashboard/stream".to_owned()),
        (
            Method::GET,
            "/dashboard/devices?lat=1&lon=1&radius_meters=1000".to_owned(),
        ),
        (Method::POST, format!("/admin/devices/{id}/revoke")),
        (Method::DELETE, format!("/admin/devices/{id}")),
    ];

    for (method, path) in &cases {
        for key in [None, Some("not-the-key")] {
            let mut request = stack
                .http
                .request(method.clone(), stack.url(path))
                .json(&alert);
            if let Some(key) = key {
                request = request.header("X-API-Key", key);
            }
            let status = request.send().await.expect("request").status().as_u16();
            assert!(
                refused(status),
                "{method} {path} with key {key:?} answered {status}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn device_endpoints_refuse_missing_or_forged_tokens() {
    let stack = Stack::start().await;

    let no_token = stack
        .http
        .post(stack.url("/devices/ws-ticket"))
        .send()
        .await
        .expect("request");
    assert!(refused(no_token.status().as_u16()));

    let forged = stack
        .http
        .post(stack.url("/devices/ws-ticket"))
        .bearer_auth("not.a.token")
        .send()
        .await
        .expect("request");
    assert!(refused(forged.status().as_u16()));

    let delete = stack
        .http
        .delete(stack.url("/devices/me"))
        .send()
        .await
        .expect("request");
    assert!(refused(delete.status().as_u16()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ticket_works_once_and_only_for_its_own_device() {
    let stack = Stack::start().await;
    let a = stack.register(CENTER.0, CENTER.1).await;
    let b = stack.register(CENTER.0, CENTER.1).await;

    let first = stack.ticket(&a).await;
    let ws_url = first["ws_url"].as_str().expect("ws_url").to_owned();
    let ticket = first["ticket"].as_str().expect("ticket").to_owned();

    let _connected = match connect_with_ticket(&ws_url, a.id, &ticket).await {
        Ok(socket) => socket,
        Err(e) => panic!("the first use of a ticket must work: {e}"),
    };

    let reuse = connect_with_ticket(&ws_url, a.id, &ticket)
        .await
        .err()
        .expect("a ticket must not work twice");
    assert_eq!(http_status(&reuse), Some(401), "{reuse:?}");

    let other = stack.ticket(&a).await;
    let borrowed = connect_with_ticket(&ws_url, b.id, other["ticket"].as_str().expect("ticket"))
        .await
        .err()
        .expect("a ticket must not work for another device");
    assert_eq!(http_status(&borrowed), Some(403), "{borrowed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoking_a_device_closes_its_socket_and_blocks_new_tickets() {
    let stack = Stack::start().await;
    let device = stack.register(CENTER.0, CENTER.1).await;
    let mut socket = stack.connect(&device).await;
    socket.finish_replay().await;

    let revoke = stack
        .http
        .post(stack.url(&format!("/admin/devices/{}/revoke", device.id)))
        .header("X-API-Key", OPERATOR_KEY)
        .send()
        .await
        .expect("revoke request");
    assert_eq!(revoke.status().as_u16(), 204);

    // The device is told, then disconnected.
    let mut told = false;
    loop {
        match socket.next_frame(Duration::from_secs(10)).await {
            Some(Frame::Control(control)) if control["type"] == "revoked" => told = true,
            Some(Frame::Control(_)) | Some(Frame::Alert(_)) => {}
            Some(Frame::Closed) => break,
            None => panic!("the socket stayed open after the device was revoked"),
        }
    }
    assert!(
        told,
        "the device was closed without being told it was revoked"
    );

    // And its token no longer buys a ticket.
    let again = stack
        .http
        .post(stack.url("/devices/ws-ticket"))
        .bearer_auth(&device.token)
        .send()
        .await
        .expect("ticket request");
    assert!(refused(again.status().as_u16()), "{}", again.status());
}
