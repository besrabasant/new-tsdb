use crate::types::AppState;
use crate::wal::WAL;
use dashmap::DashMap;
use reqwest::Client;
use std::fs::{File, OpenOptions};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use std::io::Write;

#[derive(Clone)]
pub struct Replicator {
    pub node_id: String,
    pub peers: Arc<Vec<String>>, // list of peer URLs
    pub wal: Arc<Mutex<WAL>>,
    pub offsets: Arc<DashMap<String, usize>>,
    pub log_file: Arc<Mutex<File>>, // <- for replication log
}

impl Replicator {
    pub fn new(state: &AppState) -> Self {
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("replication.log")
            .expect("Failed to open replication log");

        Replicator {
            node_id: state.node_id.clone(),
            peers: state.peer_urls.clone(),
            wal: Arc::clone(&state.wal),
            offsets: Arc::new(DashMap::new()),
            log_file: Arc::new(Mutex::new(log_file)), // <- for replication log
        }
    }

    pub async fn run(self) {
        let client = Client::new();

        let peers = self.peers.clone();
        let wal = Arc::clone(&self.wal);
        let offsets = Arc::clone(&self.offsets);
        let node_id = self.node_id.clone();
        let log_file = Arc::clone(&self.log_file);

        loop {
            let mut handles = Vec::new();

            for peer in peers.iter() {
                if peer.contains(&node_id) {
                    continue; // don't replicate to self
                }

                let client = client.clone();
                let peer = peer.clone();
                let wal = Arc::clone(&wal);
                let offsets = Arc::clone(&offsets);
                let log_file = Arc::clone(&log_file);

                let handle = tokio::spawn(async move {
                    let start = offsets.get(&peer).map(|entry| *entry).unwrap_or(0);

                    let wal_guard = wal.lock().await;
                    let new_points = wal_guard.read_since(start);
                    drop(wal_guard);

                    if new_points.is_empty() {
                        return;
                    }

                    let timestamp = chrono::Utc::now().to_rfc3339();
                    let point_count = new_points.len();

                    let resp = client
                        .post(&format!("{}/replicate", peer))
                        .json(&new_points)
                        .send()
                        .await;

                    let result_log = match resp {
                        Ok(res) if res.status().is_success() => {
                            offsets.insert(peer.clone(), start + new_points.len());
                            tracing::info!(
                                "[Replicator] Sent {} points to {}",
                                new_points.len(),
                                peer
                            );

                            format!(
                                "{} [SUCCESS] Replicated {} points to {} from offset {}\n",
                                timestamp, point_count, peer, start
                            )
                        }
                        Ok(res) => {
                            tracing::warn!(
                                "[Replicator] Peer {} responded with error: {}",
                                peer,
                                res.status()
                            );

                            format!(
                                "{} [FAILURE] Peer {} responded with HTTP {}\n",
                                timestamp,
                                peer,
                                res.status()
                            )
                        }
                        Err(e) => {
                            tracing::error!("[Replicator] Failed to replicate to {}: {}", peer, e);

                            format!(
                                "{} [FAILURE] Error replicating to {}: {}\n",
                                timestamp, peer, e
                            )
                        }
                    };

                    // Append the result to the replication log file
                    let mut log = log_file.lock().await;
                    let _ = log.write_all(result_log.as_bytes());
                });

                handles.push(handle);
            }

            for handle in handles {
                let _ = handle.await;
            }

            sleep(Duration::from_secs(5)).await;
        }
    }
}
