use crate::types::{FieldValue, TimeSeriesPoint};
use influxdb_line_protocol::{parse_lines, FieldValue as InfluxFieldValue};

#[derive(Debug)]
pub enum ParsedLine {
    Ok(TimeSeriesPoint),
    Err { line: usize, error: String },
}

pub fn parse_line_protocol(line: &str) -> Result<TimeSeriesPoint, String> {
    let parsed = parse_lines(line)
        .next()
        .ok_or("No line found")?
        .map_err(|e| e.to_string())?;

    let metric = parsed.series.measurement.to_string();

    let tags: Vec<(String, String)> = parsed
        .series
        .tag_set
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    let fields: Vec<(String, FieldValue)> = parsed
        .field_set
        .into_iter()
        .filter_map(|(k, v)| {
            let key = k.to_string();
            let value = match v {
                InfluxFieldValue::F64(f) => FieldValue::Float(f),
                InfluxFieldValue::I64(i) => FieldValue::Int(i),
                InfluxFieldValue::U64(u) => FieldValue::Int(u as i64),
                InfluxFieldValue::Boolean(b) => FieldValue::Bool(b),
                InfluxFieldValue::String(s) => FieldValue::Str(s.to_string()),
            };
            Some((key, value))
        })
        .collect();

    let timestamp = parsed
        .timestamp
        .map(|ns| ns / 1_000_000_000)
        .unwrap_or_else(|| chrono::Utc::now().timestamp());

    Ok(TimeSeriesPoint {
        metric,
        tags,
        fields,
        timestamp,
    })
}

pub fn parse_batch(input: &str) -> Vec<ParsedLine> {
    input
        .lines()
        .enumerate()
        .map(|(i, line)| match parse_line_protocol(line) {
            Ok(point) => ParsedLine::Ok(point),
            Err(e) => ParsedLine::Err {
                line: i + 1,
                error: e,
            },
        })
        .collect()
}
