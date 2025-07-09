mod parser;
mod replicator;
mod storage;
mod types;
mod wal;

mod app_config;

use anyhow::{Ok, Result};
use axum::{
    body::Bytes,
    extract::State,
    routing::{get, post},
    Json, Router,
};
use parser::{parse_batch, ParsedLine};
use replicator::Replicator;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use storage::Storage;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing_subscriber::{fmt, EnvFilter};
use types::{AppState, TimeSeriesPoint};
use wal::WAL;

#[tokio::main]
async fn main() -> Result<()> {
    // let mut filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    // // disable logs from that crate
    // filter = filter.add_directive("swim-rs=off".parse().unwrap());

    // fmt().with_env_filter(filter).with_target(false).init();

    // Load config file
    let config = app_config::load_config();

    let data_dir = Path::new(&config.data_dir);

    // Create data_dir if it doesn't exist
    if !data_dir.exists() {
        fs::create_dir_all(data_dir).expect("Failed to create data directory");
    }

    let config = Arc::new(config);
    let wal_path = Path::new(&config.data_dir).join("data.wal");
    let wal = Arc::new(Mutex::new(WAL::new(wal_path)));
    let storage = Arc::new(Storage {
        config: Arc::clone(&config),
    });
    let addr = format!("{}:{}", config.addr, config.port);

    // Replay existing data
    let replayed = wal.lock().await.replay();
    for point in &replayed {
        storage.insert(point.clone());
    }

    let state = AppState {
        wal: wal.clone(),
        storage: storage.clone(),
        node_id: config.node_id.clone(),
    };

    println!("▶ about to spawn replicator task");
    let replicator = Replicator::new(&state, Arc::clone(&config))
        .await
        .expect("failed to start replicator");

    tokio::spawn(async move {
        println!("🌀 replicator task has started!");
        replicator.run().await;
    });

    tracing::info!("✔ spawned replicator; now starting HTTP server");

    // Set up router
    let app = Router::new()
        .route("/", get(root))
        .route("/ingest", post(ingest_handler))
        .route("/replicate", post(replicate_handler))
        .with_state(state);

    // Run server
    let listener = TcpListener::bind(&addr).await.unwrap();
    tracing::info!("Listening on http://{}", &addr);
    axum::serve(listener, app).await.unwrap();

    Ok({})
}

async fn root() -> &'static str {
    "Welcome to the Time Series API"
}

async fn ingest_handler(State(state): State<AppState>, data: Bytes) -> String {
    let input = String::from_utf8_lossy(&data);

    let parsed_lines = parse_batch(&input, &state.node_id);

    let mut success_count = 0;
    let mut error_lines = vec![];

    let mut wal = state.wal.lock().await;

    for parsed_line in parsed_lines {
        match parsed_line {
            ParsedLine::Ok(point) => {
                wal.append(&point);
                state.storage.insert(point.clone());
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

async fn replicate_handler(
    State(state): State<AppState>,
    Json(points): Json<Vec<TimeSeriesPoint>>,
) -> String {
    let mut wal = state.wal.lock().await;

    for point in points {
        wal.append(&point);
        state.storage.insert(point);
    }

    wal.flush();
    "Replication received and applied".to_string()
}
