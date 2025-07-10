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
use std::time::Duration;

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
    let wal_inner = WAL::new(wal_path)?; // WAL, not Result<_,_>
    let wal = Arc::new(Mutex::new(wal_inner)); // now Mutex<WAL>
    let storage = Arc::new(Storage {
        config: Arc::clone(&config),
    });
    let addr = format!("{}:{}", config.addr, config.port);

    let replayed: Vec<TimeSeriesPoint> = wal.lock().await.replay()?;

    // 2) Move out each point (owned), not a &Vec or &TimeSeriesPoint:
    for point in replayed {
        storage.insert(point);
    }

    let state = AppState {
        wal: wal.clone(),
        storage: storage.clone(),
        node_id: config.node_id.clone(),
    };

     // Spawn a background “drainer” that takes everything new from the WAL
    // and writes it into storage, every 100ms.
    {
        let state = state.clone();
        tokio::spawn(async move {
            let mut last_idx = 0;
            loop {
                // grab everything in the WAL since `last_idx`
                let new_points = {
                    let mut w = state.wal.lock().await;
                    w.read_since(last_idx)
                };
                if !new_points.is_empty() {
                    for pt in new_points.iter() {
                        state.storage.insert(pt.clone());
                    }
                    last_idx += new_points.len();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }

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

    let wal = state.wal.lock().await;

    for parsed_line in parsed_lines {
        match parsed_line {
            ParsedLine::Ok(point) => {
                wal.append(&point);
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
    let  wal = state.wal.lock().await;

    for point in points {
        wal.append(&point);
    }

    wal.flush();
    "Replication received and applied".to_string()
}
