use crate::app_config;
use crate::types::TimeSeriesPoint;
use chrono::{TimeZone, Utc};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

pub struct Storage {
    pub config: Arc<app_config::AppConfig>,
}

impl Storage {
    pub fn insert(&self, point: TimeSeriesPoint) {
        // Segment file by date
        let date = Utc
            .timestamp_opt(point.timestamp, 0)
            .single()
            .expect("Invalid timestamp")
            .format("%Y-%m-%d")
            .to_string();

        let segments_dir = Path::new(&self.config.data_dir).join("segments");

        std::fs::create_dir_all(&segments_dir).expect("Failed to create segments directory");
        
        let filename = segments_dir.join(format!("{}.tsdata", date));
        
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&filename)
            .expect("Unable to open segment file");

        let serialized = serde_json::to_string(&point).unwrap(); // Use JSON for simplicity here
        writeln!(file, "{}", serialized).expect("Failed to write point to segment");
    }
}
