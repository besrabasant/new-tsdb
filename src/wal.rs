// ============ WAL Wrapper Using walcraft ============
use crate::types::TimeSeriesPoint;
use anyhow::{anyhow, Result};
use bincode::Serializer;
use std::path::PathBuf;

// WALcraft imports
use walcraft::{Size, Wal, WalBuilder};

/// A Write-Ahead Log wrapper around WALcraft for time-series points
pub struct WAL {
    inner: Wal<TimeSeriesPoint>,
    path: PathBuf, // store the path for cloning
}

impl WAL {
      // Create or open the WAL at `path` with custom buffer and storage sizes.
    /// Create or open the WAL at `path` with custom buffer and storage sizes.
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        // WALcraft expects a directory for storage, not a file. Use the parent directory.
        let dir = if file_path.is_file() {
            file_path.parent().unwrap().to_path_buf()
        } else {
            file_path.clone()
        };
        // ensure the directory exists
        std::fs::create_dir_all(&dir).map_err(|e| anyhow!("failed to create wal dir: {}", e))?;
        let loc = dir.to_string_lossy();
        let inner: Wal<TimeSeriesPoint> = WalBuilder::new()
            .location(&loc)
            .buffer_size(Size::Kb(4))     // 4 KiB in-memory buffer
            .storage_size(Size::Mb(200))  // 200 MiB max storage
            .enable_fsync()  
            .build()
            .map_err(|e| anyhow!("walcraft build error: {}", e))?;

        Ok(WAL { inner, path: dir })
    }

    /// Append a point to the log (syncs automatically).
    pub fn append(&self, point: &TimeSeriesPoint) {
        // clone the point since WALcraft requires owned T
        self.inner.write(point.clone());
    }

    /// Read all payloads from the WAL as a sequence of TimeSeriesPoint.
    pub fn read_all(&self) -> Result<Vec<TimeSeriesPoint>> {
        let reader = self
            .inner
            .read()
            .map_err(|e| anyhow!("wal read error: {}", e))?;
        Ok(reader.collect())
    }

    /// Read all payloads starting at index `start`.
    pub fn read_since(&self, start: usize) -> Vec<TimeSeriesPoint> {
        match self.read_all() {
            Ok(vec) if vec.len() > start => vec.into_iter().skip(start).collect(),
            Ok(_) | Err(_) => Vec::new(),
        }
    }

    /// Replay just the payloads from the beginning.
    pub fn replay(&self) -> Result<Vec<TimeSeriesPoint>> {
        self.read_all()
    }

    /// Returns true if there are no records.
    pub fn is_empty(&self) -> bool {
        match self.read_all() {
            Ok(vec) => vec.is_empty(),
            Err(_) => true,
        }
    }

    /// Returns the number of entries in the log.
    pub fn len(&self) -> usize {
        match self.read_all() {
            Ok(vec) => vec.len(),
            Err(_) => 0,
        }
    }

    /// (No-op) Truncate the WAL up to (and excluding) `upto_seq`.
    /// walcraft does not currently support truncate directly.
    pub fn truncate(&self, _upto_seq: u64) -> Result<()> {
        Ok(())
    }

    /// Flush any buffered data to disk (no-op for walcraft).
    pub fn flush(&self) {
        // WALcraft syncs on write by default; no explicit flush needed.
    }
}

impl Clone for WAL {
    fn clone(&self) -> Self {
        // Re-open on same path
        WAL::new(self.path.clone()).expect("failed to clone WAL")
    }
}
