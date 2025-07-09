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

/// Combined config struct
#[derive(Debug, Deserialize, Serialize, ConfigDoc)]
pub struct AppConfig {
    #[configdoc(description = "Address to bind the API server to")]
    pub addr: String,

    #[configdoc(
        description = "Port to run the API server on",
        long_description = "This port is used by external services to connect to your app.\n\
                            Make sure this port is open in your firewall.\n\
                            Avoid using privileged ports (like 80 or 443) without root access."
    )]
    pub port: u16,

    #[configdoc(description = "Unique node identifier")]
    pub node_id: String,

    #[configdoc(description = "List of peer node URLs")]
    pub peers: Vec<String>,

    #[configdoc(description = "Data directory")]
    pub data_dir: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            addr: "0.0.0.0".to_string(),
            port: 3000,
            node_id: "node-a".to_string(),
            peers: vec![
                "http://localhost:4001".to_string(),
                "http://localhost:4002".to_string(),
            ],
            data_dir: "./tsdb".to_string(),
        }
    }
}

/// Load config from file + CLI
pub fn load_config() -> AppConfig {
    let args = Args::parse();

    println!("Using config file: {:?}", args.config);

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

    AppConfig {
        addr: args.addr.unwrap_or(file_config.addr),
        port: args.port.unwrap_or(file_config.port),
        node_id: args.node_id.unwrap_or(file_config.node_id),
        peers: file_config.peers,
        data_dir: file_config.data_dir,
    }
}

pub fn write_sample_config(path: &PathBuf) {
    let cfg = AppConfig::default();

    fs::write(path, cfg.to_documented_toml()).expect("❌ Failed to write sample config file");
    println!("✅ Sample config written to {}", path.display());
}
