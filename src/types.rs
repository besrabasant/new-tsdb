use std::sync::Arc;
use tokio::sync::Mutex;
use serde::{Serialize, Deserialize};

use crate::{storage::Storage, wal::WAL};


#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FieldValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TimeSeriesPoint {
    pub metric: String,
    pub tags: Vec<(String, String)>,
    pub fields: Vec<(String, FieldValue)>,
    pub timestamp: i64, // UNIX timestamp
    pub node_id: String, // Node ID for conflict detection
}

#[derive(Clone)]
pub struct AppState {
    pub wal: Arc<Mutex<WAL>>,
    pub storage: Arc<Storage>,
    pub node_id: String,
}