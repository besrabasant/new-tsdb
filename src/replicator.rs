use crate::app_config;
use crate::types::AppState;
use crate::wal::WAL;
use dashmap::DashMap;
use reqwest::Client;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;

// SWIM gossip library for peer discovery and failure detection
use swim_rs::api::{config::SwimConfig, swim::SwimCluster};
use swim_rs::Event;

/// `Replicator` handles WAL replication to dynamic peers discovered via SWIM gossip.
#[derive(Clone)]
pub struct Replicator {
    /// Unique identifier for this node (used to skip self)
    node_id: String,

    self_gossip_addr: String,

    /// Write-Ahead Log, wrapped in a Tokio mutex for safe concurrent access
    wal: Arc<Mutex<WAL>>,

    /// Tracks the last replicated offset per peer (in-memory)
    offsets: Arc<DashMap<String, usize>>,

    /// Path to JSON file for persisting offsets across restarts
    offset_file_path: String,

    /// Log file for appending replication status messages
    log_file: Arc<Mutex<File>>,

    /// Discovery handle: maps peer node IDs to their HTTP URLs
    members: Arc<RwLock<HashMap<String, String>>>,

    swim: Arc<SwimCluster>,

    config: Arc<app_config::AppConfig>,
}

impl Replicator {
    /// Constructs a new `Replicator`, bootstraps SWIM gossip, and loads any saved offsets.
    pub async fn new(state: &AppState, config: Arc<app_config::AppConfig>) -> anyhow::Result<Self> {
        // Ensure the data directory exists
        fs::create_dir_all(&config.data_dir)?;

        // Build paths for replication log and offsets file
        let log_path = Path::new(&config.data_dir).join("replication.log");
        let offset_path = Path::new(&config.data_dir).join("offsets.json");

        // Open (or create) the replication log for appending
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;

        // Load any previously saved offsets from disk
        let offsets_map = Self::load_offsets(&offset_path);
        let offsets = Arc::new(offsets_map);

        // but for SWIM, build a pure host:udp_port string:
        let self_gossip_addr = format!("{}:{}", config.addr, config.gossip_port);
        let bootstrap = config.bootstrap_peers.clone();

        // Initialize SWIM cluster for peer discovery
        let swim = Arc::new(
            // Determine the raw bind address (no "http://")
            SwimCluster::try_new(
                &self_gossip_addr,
                SwimConfig::builder()
                    // Provide initial peers to bootstrap the gossip mesh
                    .with_known_peers(bootstrap)
                    .build(),
            )
            .await?,
        );

        // Shared map to hold live peers (node_id -> peer URL)
        let members = Arc::new(RwLock::new(HashMap::new()));

        Ok(Replicator {
            config,
            node_id: state.node_id.clone(),
            self_gossip_addr,
            wal: Arc::clone(&state.wal),
            offsets,
            offset_file_path: offset_path.to_string_lossy().into(),
            log_file: Arc::new(Mutex::new(log_file)),
            members,
            swim: swim,
        })
    }

    /// Main replication loop: every 5 seconds, send new WAL entries to each live peer.
    pub async fn run(self) {
        {
            let swim_bg = Arc::clone(&self.swim);
            tokio::spawn(async move {
                swim_bg.run().await;
            });
        }
        println!(
            "[Replicator] SWIM gossip loop started on {}",
            self.self_gossip_addr
        );

        {
            let swim_sub = Arc::clone(&self.swim);
            let members_sub = Arc::clone(&self.members);
            let http_port = self.config.port.clone();
            let self_addr = self.self_gossip_addr.clone();

            tokio::spawn(async move {
                let mut rx = swim_sub.subscribe();
                while let Ok(event) = rx.recv().await {
                    match event {
                        Event::NodeJoined(info) => {
                             println!("[Replicator] Node info: {:#?}", info);
                            let gossip = info.new_member.to_string();
                            if gossip != self_addr {
                                let host = gossip.split(':').next().unwrap();
                                let http_url = format!("http://{}:{}", host, http_port);
                                println!("[Replicator] Node joined: {} → {}", gossip, http_url);
                                members_sub.write().await.insert(gossip, http_url);
                            }
                        }
                        Event::NodeRecovered(info) => {
                            let gossip = info.recovered.to_string();
                            if gossip != self_addr {
                                let host = gossip.split(':').next().unwrap();
                                let http_url = format!("http://{}:{}", host, http_port);
                                println!("[Replicator] Node recovered: {} → {}", gossip, http_url);
                                members_sub.write().await.insert(gossip, http_url);
                            }
                        }
                        Event::NodeSuspected(info) => {
                            let gossip = info.suspect.to_string();
                            println!("[Replicator] Node suspected (remove): {}", gossip);
                            members_sub.write().await.remove(&gossip);
                        }
                        Event::NodeDeceased(info) => {
                            let gossip = info.deceased.to_string();
                            println!("[Replicator] Node deceased (remove): {}", gossip);
                            members_sub.write().await.remove(&gossip);
                        }
                    }
                }
            });
        }

        println!("[Replicator] Entering replication loop");

        let client = Client::new();

        loop {
            // Take a snapshot of the current live peers
            let snapshot = { self.members.read().await.clone() };
            let mut handles = Vec::new();
            let self_addr = self.self_gossip_addr.clone();


            // Log how many peers are currently known
            println!(
                "[Replicator] Found {} peers to replicate to",
                snapshot.len()
            );

            // Spawn a task for each peer to replicate in parallel
            for (peer_id, peer_url) in snapshot {
                println!("[Replicator] process peer {} -> {} {}", peer_id, peer_url, self.self_gossip_addr.clone());

                // Skip replicating to ourself
                if peer_id == self_addr.clone() {
                    continue;
                }

                let client = client.clone();
                let wal = Arc::clone(&self.wal);
                let offsets = Arc::clone(&self.offsets);
                let log_file = Arc::clone(&self.log_file);
                let peer = peer_url.clone();

                let handle = tokio::spawn(async move {
                    // Determine where we left off (default to 0)
                    let start = offsets.get(&peer).map(|e| *e).unwrap_or(0);

                    // Read new points from the WAL since that offset
                    let new_points = {
                        let guard = wal.lock().await;
                        guard.read_since(start)
                    };

                    // If there's nothing new, skip
                    if new_points.is_empty() {
                        return;
                    }

                    // Attempt HTTP POST to peer /replicate endpoint
                    let timestamp = chrono::Utc::now().to_rfc3339();
                    let count = new_points.len();
                    let resp = client
                        .post(&format!("{}/replicate", peer))
                        .json(&new_points)
                        .send()
                        .await;

                    // Build log entry depending on success or failure
                    let entry = match resp {
                        Ok(r) if r.status().is_success() => {
                            // Update in-memory offset on success
                            offsets.insert(peer.clone(), start + count);
                            format!(
                                "{} [SUCCESS] Replicated {} points to {} from offset {}\n",
                                timestamp, count, peer, start
                            )
                        }
                        Ok(r) => format!(
                            "{} [FAILURE] {} responded HTTP {}\n",
                            timestamp,
                            peer,
                            r.status()
                        ),
                        Err(e) => format!(
                            "{} [FAILURE] Error replicating to {}: {}\n",
                            timestamp, peer, e
                        ),
                    };

                    // Append the result to the replication log
                    let mut log = log_file.lock().await;
                    let _ = log.write_all(entry.as_bytes());
                });

                handles.push(handle);
            }

            // Wait for all peer tasks to complete
            for h in handles {
                let _ = h.await;
            }

            // Persist offsets so we don't re-send on restart
            self.save_offsets();

            // Wait before starting the next round
            sleep(Duration::from_secs(5)).await;
        }
    }

    /// Load offsets from disk into a DashMap, or return empty if file missing.
    fn load_offsets(path: &Path) -> DashMap<String, usize> {
        let map = DashMap::new();
        if let Ok(mut f) = File::open(path) {
            let mut buf = String::new();
            if f.read_to_string(&mut buf).is_ok() {
                if let Ok(raw) = serde_json::from_str::<HashMap<String, usize>>(&buf) {
                    for (k, v) in raw {
                        map.insert(k, v);
                    }
                }
            }
        }
        map
    }

    /// Persist the current offsets map to disk as indented JSON.
    pub fn save_offsets(&self) {
        let mut raw = HashMap::new();
        for entry in self.offsets.iter() {
            raw.insert(entry.key().clone(), *entry.value());
        }
        if let Ok(data) = serde_json::to_string_pretty(&raw) {
            let _ = fs::write(&self.offset_file_path, data);
        }
    }
}
