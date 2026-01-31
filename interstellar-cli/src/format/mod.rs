//! Output formatting for query results.
//!
//! Supports:
//! - Table format (ASCII tables using comfy-table)
//! - JSON format (pretty-printed)
//! - CSV format (streaming output)

mod csv;
mod json;
mod table;

use std::time::Duration;

use interstellar::Value;

pub use self::csv::CsvFormatter;
pub use self::json::JsonFormatter;
pub use self::table::TableFormatter;

/// Output format options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
    Csv,
}

impl From<crate::OutputFormat> for OutputFormat {
    fn from(f: crate::OutputFormat) -> Self {
        match f {
            crate::OutputFormat::Table => OutputFormat::Table,
            crate::OutputFormat::Json => OutputFormat::Json,
            crate::OutputFormat::Csv => OutputFormat::Csv,
        }
    }
}

/// Represents the result of a query.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub values: Vec<Value>,
    pub row_count: usize,
    pub elapsed: Option<Duration>,
}

impl QueryResult {
    /// Create a new query result.
    pub fn new(values: Vec<Value>, elapsed: Option<Duration>) -> Self {
        let row_count = values.len();
        Self {
            values,
            row_count,
            elapsed,
        }
    }
}

/// Format query results as a string.
pub fn format_results(result: &QueryResult, format: OutputFormat, limit: usize) -> String {
    // Apply limit
    let values: Vec<_> = if limit > 0 && result.values.len() > limit {
        result.values.iter().take(limit).cloned().collect()
    } else {
        result.values.clone()
    };

    let limited_result = QueryResult {
        values,
        row_count: result.row_count,
        elapsed: result.elapsed,
    };

    match format {
        OutputFormat::Table => TableFormatter::format(&limited_result),
        OutputFormat::Json => JsonFormatter::format(&limited_result),
        OutputFormat::Csv => CsvFormatter::format(&limited_result),
    }
}

/// Trait for output formatters.
pub trait Formatter {
    /// Format the query result as a string.
    fn format(result: &QueryResult) -> String;
}

/// Format a Value for display.
#[allow(dead_code)] // Will be used in Phase 3 for Gremlin output
pub fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => {
            if f.fract() == 0.0 {
                format!("{:.1}", f)
            } else {
                format!("{:.2}", f)
            }
        }
        Value::String(s) => s.clone(),
        Value::List(items) => {
            if items.len() <= 3 {
                let formatted: Vec<String> = items.iter().map(format_value).collect();
                format!("[{}]", formatted.join(", "))
            } else {
                format!("[{} items]", items.len())
            }
        }
        Value::Map(map) => {
            if map.len() <= 2 {
                let formatted: Vec<String> = map
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                    .collect();
                format!("{{{}}}", formatted.join(", "))
            } else {
                format!("{{{} keys}}", map.len())
            }
        }
        Value::Vertex(id) => format!("v[{}]", id.0),
        Value::Edge(id) => format!("e[{}]", id.0),
    }
}

/// Format a Value for short display (used in tables).
pub fn format_value_short(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => format!("{:.2}", f),
        Value::String(s) => {
            if s.len() > 30 {
                format!("{}...", &s[..27])
            } else {
                s.clone()
            }
        }
        Value::List(items) => format!("[{} items]", items.len()),
        Value::Map(map) => format!("{{{} keys}}", map.len()),
        Value::Vertex(id) => format!("v[{}]", id.0),
        Value::Edge(id) => format!("e[{}]", id.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_value_short() {
        assert_eq!(format_value_short(&Value::Null), "null");
        assert_eq!(format_value_short(&Value::Bool(true)), "true");
        assert_eq!(format_value_short(&Value::Int(42)), "42");
        assert_eq!(format_value_short(&Value::Float(3.14)), "3.14");
        assert_eq!(
            format_value_short(&Value::String("hello".to_string())),
            "hello"
        );
    }

    #[test]
    fn test_format_long_string() {
        let long_string = "This is a very long string that should be truncated".to_string();
        let result = format_value_short(&Value::String(long_string));
        assert!(result.ends_with("..."));
        assert!(result.len() <= 33);
    }
}
