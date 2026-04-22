//! Table output formatter using comfy-table.

use comfy_table::{Cell, ContentArrangement, Table};
use interstellar::Value;

use super::{format_value_short, Formatter, QueryResult};

/// Formatter for ASCII table output.
pub struct TableFormatter;

impl Formatter for TableFormatter {
    fn format(result: &QueryResult) -> String {
        if result.values.is_empty() {
            return format_empty_result(result.elapsed);
        }

        // Check if results are maps (typical for RETURN with multiple columns)
        let is_map_result = result
            .values
            .first()
            .map(|v| matches!(v, Value::Map(_)))
            .unwrap_or(false);

        if is_map_result {
            format_map_table(result)
        } else {
            format_simple_table(result)
        }
    }
}

impl TableFormatter {
    pub fn format(result: &QueryResult) -> String {
        <Self as Formatter>::format(result)
    }
}

/// Format an empty result.
fn format_empty_result(elapsed: Option<std::time::Duration>) -> String {
    let mut output = String::from("(no results)\n");
    if let Some(duration) = elapsed {
        output.push_str(&format!(
            "0 rows ({:.2}ms)\n",
            duration.as_secs_f64() * 1000.0
        ));
    }
    output
}

/// Format map-based results as a table.
fn format_map_table(result: &QueryResult) -> String {
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

    if columns.is_empty() {
        return "(no columns)\n".to_string();
    }

    // Create table
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);

    // Set header
    table.set_header(columns.iter().map(|c| Cell::new(c)));

    // Add rows
    for value in &result.values {
        if let Value::Map(map) = value {
            let row: Vec<Cell> = columns
                .iter()
                .map(|col| {
                    let cell_value = map.get(col).map(format_value_short).unwrap_or_default();
                    Cell::new(cell_value)
                })
                .collect();
            table.add_row(row);
        }
    }

    // Build output
    let mut output = table.to_string();
    output.push('\n');

    // Add footer
    output.push_str(&format_footer(result.values.len(), result.elapsed));

    output
}

/// Format simple single-column results.
fn format_simple_table(result: &QueryResult) -> String {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec![Cell::new("value")]);

    for value in &result.values {
        table.add_row(vec![Cell::new(format_value_short(value))]);
    }

    let mut output = table.to_string();
    output.push('\n');
    output.push_str(&format_footer(result.values.len(), result.elapsed));

    output
}

/// Format the footer with row count and timing.
fn format_footer(row_count: usize, elapsed: Option<std::time::Duration>) -> String {
    match elapsed {
        Some(duration) => {
            format!(
                "{} rows ({:.2}ms)\n",
                row_count,
                duration.as_secs_f64() * 1000.0
            )
        }
        None => {
            format!("{} rows\n", row_count)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use interstellar::value::ValueMap;

    #[test]
    fn test_format_empty_result() {
        let result = QueryResult {
            values: vec![],
            row_count: 0,
            elapsed: None,
        };
        let output = TableFormatter::format(&result);
        assert!(output.contains("no results"));
    }

    #[test]
    fn test_format_simple_values() {
        let result = QueryResult {
            values: vec![Value::Int(1), Value::Int(2), Value::Int(3)],
            row_count: 3,
            elapsed: None,
        };
        let output = TableFormatter::format(&result);
        assert!(output.contains("value"));
        assert!(output.contains("3 rows"));
    }

    #[test]
    fn test_format_map_values() {
        let mut map1 = ValueMap::new();
        map1.insert("name".to_string(), Value::String("Alice".to_string()));
        map1.insert("age".to_string(), Value::Int(30));

        let mut map2 = ValueMap::new();
        map2.insert("name".to_string(), Value::String("Bob".to_string()));
        map2.insert("age".to_string(), Value::Int(25));

        let result = QueryResult {
            values: vec![Value::Map(map1), Value::Map(map2)],
            row_count: 2,
            elapsed: None,
        };
        let output = TableFormatter::format(&result);
        assert!(output.contains("name"));
        assert!(output.contains("age"));
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
    }
}
