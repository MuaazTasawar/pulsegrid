use futures_util::StreamExt;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let device_id = args.get(1).cloned().unwrap_or_else(|| {
        eprintln!("usage: loadgen <device_id> <shard_prefix> [fanout_worker_url]");
        std::process::exit(1);
    });
    let shard_prefix = args.get(2).cloned().unwrap_or_else(|| {
        eprintln!("usage: loadgen <device_id> <shard_prefix> [fanout_worker_url]");
        std::process::exit(1);
    });
    let base_url = args
        .get(3)
        .cloned()
        .unwrap_or_else(|| "ws://localhost:8081".to_string());

    let url = format!("{base_url}/connect/{device_id}?shard_prefix={shard_prefix}");
    println!("connecting to {url}");

    let (ws_stream, response) = connect_async(&url).await?;
    println!("connected, http status: {}", response.status());

    let (_, mut read) = ws_stream.split();

    println!("listening for alerts... (Ctrl+C to stop)");
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Binary(payload)) => {
                let text = String::from_utf8_lossy(&payload);
                println!("--- ALERT RECEIVED ---\n{text}\n----------------------");
            }
            Ok(Message::Close(_)) => {
                println!("server closed connection");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("websocket error: {e}");
                break;
            }
        }
    }

    Ok(())
}