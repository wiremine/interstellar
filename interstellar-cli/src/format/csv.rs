//! CSV output formatter.

use interstellar::Value;

use super::{format_value_short, Formatter, QueryResult};

/// Formatter for CSV output.
pub struct CsvFormatter;

impl Formatter for CsvFormatter {
    fn format(result: &QueryResult) -> String {
        if result.values.is_empty() {
            return String::new();
        }

        // Check if results are maps (typical for RETURN with multiple columns)
        let is_map_result = result
            .values
            .first()
            .map(|v| matches!(v, Value::Map(_)))
            .unwrap_or(false);

        if is_map_result {
            format_map_csv(result)
        } else {
            format_simple_csv(result)
        }
    }
}

impl CsvFormatter {
    pub fn format(result: &QueryResult) -> String {
        <Self as Formatter>::format(result)
    }
}

/// Format map-based results as CSV.
fn format_map_csv(result: &QueryResult) -> String {
    // Collect all column names from all rows
    let mut columns: Vec<String> = Vec::new();
    for value in &result.values {
        if let Value::Map(map) = value {
            for key in map.keys() {
                if !columns.contains(key) {
                    columns.push(key.clone());
                }
            }
        }
    }
    columns.sort();

    let mut output = String::new();

    // Header
    output.push_str(&columns.join(","));
    output.push('\n');

    // Rows
    for value in &result.values {
        if let Value::Map(map) = value {
            let values: Vec<String> = columns
                .iter()
                .map(|col| {
                    map.get(col)
                        .map(|v| escape_csv(&format_value_short(v)))
                        .unwrap_or_default()
                })
                .collect();
            output.push_str(&values.join(","));
            output.push('\n');
        }
    }

    output
}

/// Format simple single-column results as CSV.
fn format_simple_csv(result: &QueryResult) -> String {
    let mut output = String::from("value\n");

    for value in &result.values {
        output.push_str(&escape_csv(&format_value_short(value)));
        output.push('\n');
    }

    output
}

/// Escape a string for CSV output.
fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_format_empty() {
        let result = QueryResult {
            values: vec![],
            row_count: 0,
            elapsed: None,
        };
        let output = CsvFormatter::format(&result);
        assert!(output.is_empty());
    }

    #[test]
    fn test_format_simple_values() {
        let result = QueryResult {
            values: vec![Value::Int(1), Value::Int(2), Value::Int(3)],
            row_count: 3,
            elapsed: None,
        };
        let output = CsvFormatter::format(&result);
        assert_eq!(output, "value\n1\n2\n3\n");
    }

    #[test]
    fn test_format_map_values() {
        let mut map1 = HashMap::new();
        map1.insert("name".to_string(), Value::String("Alice".to_string()));
        map1.insert("age".to_string(), Value::Int(30));

        let mut map2 = HashMap::new();
        map2.insert("name".to_string(), Value::String("Bob".to_string()));
        map2.insert("age".to_string(), Value::Int(25));

        let result = QueryResult {
            values: vec![Value::Map(map1), Value::Map(map2)],
            row_count: 2,
            elapsed: None,
        };
        let output = CsvFormatter::format(&result);
        assert!(output.contains("age,name"));
        assert!(output.contains("30,Alice"));
        assert!(output.contains("25,Bob"));
    }

    #[test]
    fn test_escape_csv() {
        assert_eq!(escape_csv("simple"), "simple");
        assert_eq!(escape_csv("with,comma"), "\"with,comma\"");
        assert_eq!(escape_csv("with\"quote"), "\"with\"\"quote\"");
        assert_eq!(escape_csv("with\nnewline"), "\"with\nnewline\"");
    }
}
