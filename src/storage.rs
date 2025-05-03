use crate::types::TimeSeriesPoint;
use chrono::{TimeZone, Utc};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

pub struct Storage;

impl Storage {
    pub fn insert(&self, point: TimeSeriesPoint) {
        // Segment file by date
        let date = Utc
            .timestamp_opt(point.timestamp, 0)
            .single()
            .expect("Invalid timestamp")
            .format("%Y-%m-%d")
            .to_string();

        let filename = format!("segments/{}.tsdata", date);

        std::fs::create_dir_all("segments").unwrap();

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(Path::new(&filename))
            .expect("Unable to open segment file");

        let serialized = serde_json::to_string(&point).unwrap(); // Use JSON for simplicity here
        writeln!(file, "{}", serialized).expect("Failed to write point to segment");
    }
}
