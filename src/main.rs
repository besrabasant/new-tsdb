mod parser;
mod storage;
mod types;
mod wal;

use axum::{
    body::Bytes,
    extract::State,
    routing::{get, post},
    Router,
};
use parser::{parse_batch, ParsedLine};
use std::sync::{Arc, Mutex};
use storage::Storage;
use tokio::net::TcpListener;
use wal::WAL;

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

    let parsed_lines = parse_batch(&input);

    let mut success_count = 0;
    let mut error_lines = vec![];

    let mut wal = wal.lock().unwrap();

    for parsed_line in parsed_lines {
        match parsed_line {
            ParsedLine::Ok(point) => {
                wal.append(&point);
                storage.insert(point);
                success_count += 1;
            }
            ParsedLine::Err { line, error } => {
                error_lines.push(format!("Line {}: {}", line, error));
            }
        }
    }
    
    wal.flush();

    if success_count == 0 {
        return format!("Error: No valid lines found.\n{}", error_lines.join("\n"));
    }

    if !error_lines.is_empty() {
        return format!(
            "OK ({} lines inserted)\n{}",
            success_count,
            error_lines.join("\n")
        );
    }


    "OK".into()
}
