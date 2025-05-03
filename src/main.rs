mod storage;
mod types;
mod wal;
mod parser;

use axum::{body::Bytes, extract::State, routing::{get, post}, Router};
use std::sync::{Arc, Mutex};
use storage::Storage;
use tokio::net::TcpListener;
use wal::WAL;
use parser::parse_line_protocol;

#[tokio::main]
async fn main() {
    let wal = Arc::new(Mutex::new(WAL::new("data.wal")));
    let storage = Arc::new(Storage);

    // Replay existing data
    let replayed = wal.lock().unwrap().replay();
    for point in &replayed {
        storage.insert(point.clone());
    }
    // Set up router
    let app = Router::new()
        .route("/", get(root))
        .route("/ingest", post(ingest_handler))
        .with_state((wal.clone(), storage.clone()));

    // Run server
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Listening on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> &'static str {
    "Welcome to the Time Series API"
}

async fn ingest_handler(
    State((wal, storage)): State<(Arc<Mutex<WAL>>, Arc<Storage>)>,
    data: Bytes,
) -> String {
    let input = String::from_utf8_lossy(&data);

    match parse_line_protocol(&input) {
        Ok(point) => {
            wal.lock().unwrap().append(&point);
            storage.insert(point);
            "OK".into()
        }
        Err(e) => format!("Error: {e}"),
    }
}