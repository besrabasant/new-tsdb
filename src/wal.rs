use std::fs::{OpenOptions, File};
use std::io::{BufReader, BufWriter, Read, SeekFrom, Write, Seek};
use std::path::Path;
use crate::types::TimeSeriesPoint;
use bincode;

pub struct WAL {
    writer: BufWriter<File>,
    path: String,
}

impl WAL {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let path_str = path.as_ref().to_string_lossy().to_string();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("Failed to open WAL file");

        WAL {
            writer: BufWriter::new(file),
            path: path_str,
        }
    }

    pub fn append(&mut self, point: &TimeSeriesPoint) {
        let encoded = bincode::serialize(point).expect("Failed to serialize point");
        self.writer.write_all(&encoded).expect("Failed to write");
    }

    pub fn flush(&mut self) {
        self.writer.flush().expect("Flush failed");
    }

    pub fn read_since(&self, offset: usize) -> Vec<TimeSeriesPoint> {
        let file = File::open(&self.path).expect("Failed to open WAL file for read_since");
        let mut reader = BufReader::new(file);
        let mut points = Vec::new();

        // Seek to the byte offset
        reader.seek(SeekFrom::Start(offset as u64)).expect("Failed to seek WAL");

        // Read and deserialize entries one by one
        while let Ok(point) = bincode::deserialize_from(&mut reader) {
            points.push(point);
        }

        points
    }

    pub fn replay(&self) -> Vec<TimeSeriesPoint> {
        let file = File::open(&self.path).expect("Failed to open WAL for replay");
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();

        let mut cursor = std::io::Cursor::new(&buffer);
        let mut points = Vec::new();

        while let Ok(point) = bincode::deserialize_from(&mut cursor) {
            points.push(point);
        }

        points
    }
}
