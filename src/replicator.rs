use crate::types::AppState;
use crate::wal::WAL;
use dashmap::DashMap;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Clone)]
pub struct Replicator {
    pub node_id: String,
    pub peers: Arc<Vec<String>>, // list of peer URLs
    pub wal: Arc<Mutex<WAL>>,
    pub offsets: Arc<DashMap<String, usize>>,
}

impl Replicator {
    pub fn new(state: &AppState) -> Self {
        Replicator {
            node_id: state.node_id.clone(),
            peers: state.peer_urls.clone(),
            wal: Arc::clone(&state.wal),
            offsets: Arc::new(DashMap::new()),
        }
    }
    pub async fn run(self) {
        let client = Client::new();
    
        let peers = self.peers.clone();
        let wal = Arc::clone(&self.wal);
        let offsets = Arc::clone(&self.offsets);
        let node_id = self.node_id.clone();
    
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
    
                let handle = tokio::spawn(async move {
                    let start = offsets.get(&peer).map(|entry| *entry).unwrap_or(0);
    
                    let wal_guard = wal.lock().await;
                    let new_points = wal_guard.read_since(start);
                    drop(wal_guard);
    
                    if new_points.is_empty() {
                        return;
                    }
    
                    let resp = client
                        .post(&format!("{}/replicate", peer))
                        .json(&new_points)
                        .send()
                        .await;
    
                    match resp {
                        Ok(res) if res.status().is_success() => {
                            offsets.insert(peer.clone(), start + new_points.len());
                            tracing::info!(
                                "[Replicator] Sent {} points to {}",
                                new_points.len(),
                                peer
                            );
                        }
                        Ok(res) => {
                            tracing::warn!(
                                "[Replicator] Peer {} responded with error: {}",
                                peer,
                                res.status()
                            );
                        }
                        Err(e) => {
                            tracing::error!("[Replicator] Failed to replicate to {}: {}", peer, e);
                        }
                    }
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
