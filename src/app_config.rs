use std::{fs, path::PathBuf};

use clap::Parser;
use config::Config;
use serde::{Deserialize, Serialize};
use tsdb_configdoc_derive::ConfigDoc;

/// Command-line arguments
#[derive(Parser, Debug)]
#[command(author, version, about = "Time Series API")]
pub struct Args {
    /// Path to config file (TOML)
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,

    #[arg(long)]
    addr: Option<String>,

    /// Override port
    #[arg(long)]
    port: Option<u16>,

    /// Override node ID
    #[arg(long)]
    node_id: Option<String>,

    /// Generate a sample config.toml and exit
    #[arg(long)]
    generate_config: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Standalone,
    Replicaset,
}
/// Combined config struct
#[derive(Debug, Deserialize, Serialize, ConfigDoc)]
pub struct AppConfig {
    #[configdoc(
        description = "The IP address or hostname the server should listen on (e.g. 127.0.0.1 or 0.0.0.0)"
    )]
    pub addr: String,

    #[configdoc(
        description = "The port number on which the server should accept requests",
        long_description = "This is the public-facing port used by browsers or other apps to connect to your server.\n\
                            Make sure this port is not blocked by your system's firewall.\n\
                            Avoid using ports below 1024 unless your app runs as root (e.g. 80 or 443)."
    )]
    pub port: u16,

    #[configdoc(description = "Port used for SWIM gossip between nodes")]
    pub gossip_port: u16,

    #[configdoc(description = "A unique identifier for this node in the cluster")]
    pub node_id: String,

    #[configdoc(
        description = "Addresses of other nodes for initial gossip discovery",
        long_description = "Provide a list of `host:port` strings for existing cluster members.\n\
                            SWIM will use these to bootstrap the mesh; once connected,\n\
                            membership updates are discovered dynamically."
    )]
    pub bootstrap_peers: Vec<String>,

    #[configdoc(
        description = "Directory where all data files (WAL, offsets, logs) are stored",
        long_description = "This folder will contain your write-ahead log, replication offsets,\n\
                            and any snapshots. Ensure the process has write permissions. (Default: \"./tsdb\")"
    )]
    pub data_dir: String,

    #[configdoc(
        description = "Operation mode: standalone or replicaset",
        long_description = "Choose `standalone` for a single-node deployment with local WAL-based replication.\n\
                           Choose `replicaset` to run as a multi-node cluster using Raft for consensus.\n\
                           In `replicaset` mode, this node will use SWIM for peer discovery,\n\
                           then form or join a Raft group with the discovered members."
    )]
    pub mode: Mode,
}

impl Default for AppConfig {
    fn default() -> Self {
        let addr = "0.0.0.0".to_string();
        let port = 3000;

        AppConfig {
            addr,
            port,
            gossip_port: 5000,
            node_id: "node-a".to_string(),
            bootstrap_peers: vec!["localhost:3001".to_string(), "localhost:3002".to_string()],
            data_dir: "./tsdb".to_string(),
            mode: Mode::Standalone,
        }
    }
}

/// Load config from file + CLI
pub fn load_config() -> AppConfig {
    let args = Args::parse();

    if args.generate_config {
        write_sample_config(&args.config);
        std::process::exit(0);
    }

    let config_path = &args.config;

    let file_config_result = Config::builder()
        .add_source(config::File::from(config_path.clone()))
        .build()
        .and_then(|c| c.try_deserialize::<AppConfig>());

    let file_config = match file_config_result {
        Ok(cfg) => cfg,
        Err(err) => {
            if config_path != &PathBuf::from("config.toml") {
                eprintln!("Failed to load config from {:?}: {}", config_path, err);
                std::process::exit(1);
            }
            AppConfig::default()
        }
    };

    // Determine final values, allowing CLI overrides, and compute self_url if not provided
    let addr = args.addr.unwrap_or_else(|| file_config.addr.clone());
    let port = args.port.unwrap_or(file_config.port);
    let gossip_port = file_config.gossip_port;
    let node_id = args.node_id.unwrap_or_else(|| file_config.node_id.clone());
    let data_dir = file_config.data_dir.clone();
    let bootstrap_peers = file_config.bootstrap_peers.clone();
    let mode = file_config.mode.clone();

    AppConfig {
        addr,
        port,
        gossip_port,
        node_id,
        bootstrap_peers,
        data_dir,
        mode,
    }
}

pub fn write_sample_config(path: &PathBuf) {
    let cfg = AppConfig::default();

    fs::write(path, cfg.to_documented_toml()).expect("❌ Failed to write sample config file");
    println!("✅ Sample config written to {}", path.display());
}
