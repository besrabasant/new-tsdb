use std::{fs, path::PathBuf};

use serde::{Deserialize, Serialize};
use clap::Parser;


/// Command-line arguments
#[derive(Parser, Debug)]
#[command(author, version, about = "Time Series API")]
struct Args {
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
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct AppConfig {
    pub port: u16,
    pub addr: String,
    pub node_id: String,
    pub peers: Vec<String>,
    pub data_dir: String,
}


/// Load config from file + CLI
pub fn load_config() -> AppConfig {
    let args = Args::parse();

    if args.generate_config {
        write_sample_config(&args.config);
    }

    // Try loading the config file
    let file_config: AppConfig = config::Config::builder()
        .add_source(config::File::from(args.config).required(false))
        .build()
        .and_then(|c| c.try_deserialize())
        .unwrap_or(AppConfig {
            addr: "0.0.0.0".to_string(),
            port: 3000,
            node_id: "node-a".to_string(),
            peers: vec![],
            data_dir: "./tsdb".to_string(),
        });

    // Merge CLI overrides
    AppConfig {
        addr: args.addr.unwrap_or(file_config.addr),
        port: args.port.unwrap_or(file_config.port),
        node_id: args.node_id.unwrap_or(file_config.node_id),
        peers: file_config.peers,
        data_dir: file_config.data_dir,
    }
}



pub fn write_sample_config(path: &PathBuf) {
    let doc = r#"# Sample Configuration for new TSDB

# Address to bind the API server to
addr = "0.0.0.0"

# Port to run the API server on
port = 3000

# Unique node identifier
node_id = "node-a"

# List of peer node URLs
peers = ["http://localhost:4001", "http://localhost:4002"]
"#;

    fs::write(path, doc).expect("❌ Failed to write sample config file");
    println!("✅ Sample config written to {}", path.display());
    std::process::exit(0);
}
