use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
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
    pub timestamp: i64 // UNIX timestamp
}