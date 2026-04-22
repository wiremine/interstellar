//! JSON output formatter.

use interstellar::Value;
use serde_json;

use super::{Formatter, QueryResult};

/// Formatter for JSON output.
pub struct JsonFormatter;

impl Formatter for JsonFormatter {
    fn format(result: &QueryResult) -> String {
        let json_values: Vec<serde_json::Value> = result.values.iter().map(value_to_json).collect();

        serde_json::to_string_pretty(&json_values)
            .unwrap_or_else(|e| format!("Error serializing JSON: {}", e))
    }
}

impl JsonFormatter {
    pub fn format(result: &QueryResult) -> String {
        <Self as Formatter>::format(result)
    }

    /// Format a single value as JSON.
    #[allow(dead_code)] // Will be used in Phase 3 for Gremlin output
    pub fn format_value(value: &Value) -> String {
        serde_json::to_string_pretty(&value_to_json(value))
            .unwrap_or_else(|e| format!("Error serializing JSON: {}", e))
    }
}

/// Convert an interstellar Value to serde_json Value.
pub fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::Vertex(id) => {
            serde_json::json!({
                "type": "vertex",
                "id": id.0,
            })
        }
        Value::Edge(id) => {
            serde_json::json!({
                "type": "edge",
                "id": id.0,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use interstellar::value::ValueMap;

    #[test]
    fn test_format_empty() {
        let result = QueryResult {
            values: vec![],
            row_count: 0,
            elapsed: None,
        };
        let output = JsonFormatter::format(&result);
        assert_eq!(output.trim(), "[]");
    }

    #[test]
    fn test_format_primitives() {
        let result = QueryResult {
            values: vec![
                Value::Int(42),
                Value::String("hello".to_string()),
                Value::Bool(true),
            ],
            row_count: 3,
            elapsed: None,
        };
        let output = JsonFormatter::format(&result);
        assert!(output.contains("42"));
        assert!(output.contains("\"hello\""));
        assert!(output.contains("true"));
    }

    #[test]
    fn test_format_map() {
        let mut map = ValueMap::new();
        map.insert("name".to_string(), Value::String("Alice".to_string()));
        map.insert("age".to_string(), Value::Int(30));

        let result = QueryResult {
            values: vec![Value::Map(map)],
            row_count: 1,
            elapsed: None,
        };
        let output = JsonFormatter::format(&result);
        assert!(output.contains("\"name\""));
        assert!(output.contains("\"Alice\""));
        assert!(output.contains("\"age\""));
        assert!(output.contains("30"));
    }

    #[test]
    fn test_value_to_json_vertex() {
        let vertex = Value::Vertex(interstellar::VertexId(123));
        let json = value_to_json(&vertex);
        assert_eq!(json["type"], "vertex");
        assert_eq!(json["id"], 123);
    }
}
