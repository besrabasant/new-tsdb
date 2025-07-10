// src/replicator.rs
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
use tokio::task::JoinHandle;
use tokio::time::sleep;

// SWIM gossip library
use swim_rs::api::{config::SwimConfig, swim::SwimCluster};
use swim_rs::Event;

#[derive(Clone)]
pub struct Replicator {
    self_http_addr: String,
    wal: Arc<Mutex<WAL>>,
    offsets: Arc<DashMap<String, usize>>,
    offset_file_path: String,
    log_file: Arc<Mutex<File>>,
    members: Arc<RwLock<HashMap<String, String>>>,
    swim: Arc<SwimCluster>,
    config: Arc<app_config::AppConfig>,
}

impl Replicator {
    pub async fn new(
        state: &AppState,
        config: Arc<app_config::AppConfig>,
    ) -> anyhow::Result<Self> {
        // ensure data dir
        fs::create_dir_all(&config.data_dir)?;

        // replication log & offsets
        let log_path = Path::new(&config.data_dir).join("replication.log");
        let offset_path = Path::new(&config.data_dir).join("offsets.json");
        let log_file = OpenOptions::new().create(true).append(true).open(&log_path)?;
        let offsets_map = Self::load_offsets(&offset_path);

        // SWIM gossip
        let gossip_addr = format!("{}:{}", config.addr, config.gossip_port);
        let swim = Arc::new(
            SwimCluster::try_new(
                &gossip_addr,
                SwimConfig::builder()
                    .with_known_peers(config.bootstrap_peers.clone())
                    .build(),
            )
            .await?,
        );

        // seed HTTP peers from bootstrap_peers
        let mut init = HashMap::new();
        for gossip in &config.bootstrap_peers {
            let host = gossip.split(':').next().unwrap();
            let url = format!("http://{}:{}", host, config.port);
            init.insert(gossip.clone(), url);
        }

        Ok(Replicator {
            self_http_addr: format!("http://{}:{}", config.addr, config.port),
            wal: Arc::clone(&state.wal),
            offsets: Arc::new(offsets_map),
            offset_file_path: offset_path.to_string_lossy().into(),
            log_file: Arc::new(Mutex::new(log_file)),
            members: Arc::new(RwLock::new(init)),
            swim,
            config,
        })
    }

    pub async fn run(self) {
        // run SWIM background
        {
            let s = Arc::clone(&self.swim);
            tokio::spawn(async move { s.run().await });
        }

        // watch SWIM events
        {
            let s = Arc::clone(&self.swim);
            let members = Arc::clone(&self.members);
            let self_http = self.self_http_addr.clone();
            let port = self.config.port;
            tokio::spawn(async move {
                let mut rx = s.subscribe();
                while let Ok(event) = rx.recv().await {
                    match event {
                        Event::NodeJoined(info) => {
                            let gossip = info.new_member.to_string();
                            if gossip != self_http {
                                let host = gossip.split(':').next().unwrap();
                                let url = format!("http://{}:{}", host, port);
                                members.write().await.insert(gossip.clone(), url.clone());
                                println!("[GOSSIP] joined: {} → {}", gossip, url);
                            }
                        }
                        Event::NodeRecovered(info) => {
                            let gossip = info.recovered.to_string();
                            if gossip != self_http {
                                let host = gossip.split(':').next().unwrap();
                                let url = format!("http://{}:{}", host, port);
                                members.write().await.insert(gossip.clone(), url.clone());
                                println!("[GOSSIP] recovered: {} → {}", gossip, url);
                            }
                        }
                        Event::NodeSuspected(info) => {
                            let gossip = info.suspect.to_string();
                            members.write().await.remove(&gossip);
                            println!("[GOSSIP] suspected (removed): {}", gossip);
                        }
                        Event::NodeDeceased(info) => {
                            let gossip = info.deceased.to_string();
                            members.write().await.remove(&gossip);
                            println!("[GOSSIP] deceased (removed): {}", gossip);
                        }
                        _ => {}
                    }
                }
            });
        }

        println!("[Replicator] starting replication loop");
        let client = Client::new();

        loop {
            // snapshot peers
            let peers = { self.members.read().await.clone() };
            println!("[Replicator] peers = {:#?}", peers);

            // tasks
            let mut handles: Vec<JoinHandle<()>> = Vec::new();

            for (_gossip, peer_url) in peers {
                if peer_url == self.self_http_addr {
                    continue;
                }

                let client = client.clone();
                let wal = Arc::clone(&self.wal);
                let offsets = Arc::clone(&self.offsets);
                let logf = Arc::clone(&self.log_file);
                let peer = peer_url.clone();

                let h = tokio::spawn(async move {
                    // fetch index
                    let start = offsets.get(&peer).map(|e| *e).unwrap_or(0);

                    // read all points
                    let all = {
                        let mut w = wal.lock().await;
                        w.read_all().unwrap_or_default()
                    };
                    println!("[Replicator] {} has total {}", peer, all.len());

                    // slice new
                    let new = if start < all.len() {
                        all[start..].to_vec()
                    } else {
                        Vec::new()
                    };
                    println!("[Replicator] {} new {}", peer, new.len());
                    if new.is_empty() {
                        return;
                    }

                    // post
                    let ts = chrono::Utc::now().to_rfc3339();
                    let cnt = new.len();
                    let resp = client
                        .post(&format!("{}/replicate", peer))
                        .json(&new)
                        .send()
                        .await;

                    let entry = match resp {
                        Ok(r) if r.status().is_success() => {
                            offsets.insert(peer.clone(), start + cnt);
                            format!("[{}] SUCCESS {}→{} idx {}\n", ts, cnt, peer, start)
                        }
                        Ok(r) => format!("[{}] FAIL {} returned {}\n", ts, peer, r.status()),
                        Err(e) => format!("[{}] ERROR sending to {}: {}\n", ts, peer, e),
                    };

                    let mut f = logf.lock().await;
                    let _ = f.write_all(entry.as_bytes());
                });

                handles.push(h);
            }

            // await tasks
            for h in handles {
                let _ = h.await;
            }

            // persist offsets
            self.save_offsets();
            sleep(Duration::from_secs(5)).await;
        }
    }

    fn load_offsets(path: &Path) -> DashMap<String, usize> {
        let m = DashMap::new();
        if let Ok(mut f) = File::open(path) {
            let mut s = String::new();
            if f.read_to_string(&mut s).is_ok() {
                if let Ok(raw) = serde_json::from_str::<HashMap<String, usize>>(&s) {
                    for (k, v) in raw {
                        m.insert(k, v);
                    }
                }
            }
        }
        m
    }

    pub fn save_offsets(&self) {
        let mut raw = HashMap::new();
        for e in self.offsets.iter() {
            raw.insert(e.key().clone(), *e.value());
        }
        if let Ok(s) = serde_json::to_string_pretty(&raw) {
            let _ = fs::write(&self.offset_file_path, s);
        }
    }
}
