//! Stats command - displays database statistics.

use std::collections::HashMap;
use std::path::PathBuf;

use interstellar::storage::{GraphStorage, MmapGraph};

use crate::error::{CliError, Result};
use crate::OutputFormat;

/// Statistics about the database.
#[derive(Debug)]
pub struct DatabaseStats {
    pub path: PathBuf,
    pub total_vertices: u64,
    pub total_edges: u64,
    pub vertex_labels: HashMap<String, usize>,
    pub edge_labels: HashMap<String, usize>,
}

/// Execute the stats command.
pub fn execute(path: PathBuf, format: OutputFormat) -> Result<()> {
    // Check if database exists
    if !path.exists() {
        return Err(CliError::DatabaseNotFound { path });
    }

    // Open database
    let graph = MmapGraph::open(&path).map_err(|e| {
        CliError::database_with_source(
            format!("Failed to open database: {}", path.display()),
            anyhow::anyhow!("{}", e),
        )
    })?;

    // Collect statistics
    let stats = collect_stats(&path, &graph)?;

    // Format and display output
    match format {
        OutputFormat::Table => print_table(&stats),
        OutputFormat::Json => print_json(&stats)?,
        OutputFormat::Csv => print_csv(&stats),
    }

    Ok(())
}

/// Collect statistics from the database.
fn collect_stats(path: &PathBuf, graph: &MmapGraph) -> Result<DatabaseStats> {
    let mut vertex_labels: HashMap<String, usize> = HashMap::new();
    let mut edge_labels: HashMap<String, usize> = HashMap::new();

    // Get total counts
    let total_vertices = graph.vertex_count();
    let total_edges = graph.edge_count();

    // Count vertices by label
    for vertex in graph.all_vertices() {
        *vertex_labels.entry(vertex.label.clone()).or_insert(0) += 1;
    }

    // Count edges by label
    for edge in graph.all_edges() {
        *edge_labels.entry(edge.label.clone()).or_insert(0) += 1;
    }

    Ok(DatabaseStats {
        path: path.clone(),
        total_vertices,
        total_edges,
        vertex_labels,
        edge_labels,
    })
}

/// Print statistics in table format.
fn print_table(stats: &DatabaseStats) {
    println!("Database: {}", stats.path.display());
    println!("Storage: MmapGraph (persistent)");
    println!();
    println!(
        "Vertices: {:>10}",
        format_number(stats.total_vertices as usize)
    );
    println!(
        "Edges:    {:>10}",
        format_number(stats.total_edges as usize)
    );
    println!();

    if !stats.vertex_labels.is_empty() {
        println!("Vertex Labels:");
        let mut labels: Vec<_> = stats.vertex_labels.iter().collect();
        labels.sort_by(|a, b| b.1.cmp(a.1)); // Sort by count descending
        for (label, count) in labels {
            println!("  {}: {}", label, format_number(*count));
        }
        println!();
    }

    if !stats.edge_labels.is_empty() {
        println!("Edge Labels:");
        let mut labels: Vec<_> = stats.edge_labels.iter().collect();
        labels.sort_by(|a, b| b.1.cmp(a.1)); // Sort by count descending
        for (label, count) in labels {
            println!("  {}: {}", label, format_number(*count));
        }
    }
}

/// Print statistics in JSON format.
fn print_json(stats: &DatabaseStats) -> Result<()> {
    let json = serde_json::json!({
        "database": stats.path.display().to_string(),
        "storage": "MmapGraph",
        "vertices": {
            "total": stats.total_vertices,
            "by_label": stats.vertex_labels,
        },
        "edges": {
            "total": stats.total_edges,
            "by_label": stats.edge_labels,
        }
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&json)
            .map_err(|e| { CliError::general(format!("Failed to serialize JSON: {}", e)) })?
    );

    Ok(())
}

/// Print statistics in CSV format.
fn print_csv(stats: &DatabaseStats) {
    println!("type,label,count");

    // Total row
    println!("total,vertices,{}", stats.total_vertices);
    println!("total,edges,{}", stats.total_edges);

    // Vertex labels
    for (label, count) in &stats.vertex_labels {
        println!("vertex,{},{}", label, count);
    }

    // Edge labels
    for (label, count) in &stats.edge_labels {
        println!("edge,{},{}", label, count);
    }
}

/// Format a number with thousand separators.
fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(100), "100");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1234567), "1,234,567");
    }
}
