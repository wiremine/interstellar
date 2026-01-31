//! Export command - export data to GraphSON files.

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::time::Instant;

use interstellar::graphson;
use interstellar::storage::{GraphStorage, PersistentGraph};

use crate::error::{CliError, Result};

/// Execute the export command.
///
/// Exports data from a database to a GraphSON file.
pub fn execute(db_path: PathBuf, output_path: PathBuf, pretty: bool) -> Result<()> {
    // Check that the database exists
    if !db_path.exists() {
        return Err(CliError::DatabaseNotFound { path: db_path });
    }

    let start = Instant::now();

    // Open the database
    println!("Opening database: {}", db_path.display());
    let graph = PersistentGraph::open(&db_path).map_err(|e| {
        CliError::database_with_source(
            format!("Failed to open database: {}", db_path.display()),
            anyhow::anyhow!("{}", e),
        )
    })?;

    let snapshot = graph.snapshot();
    let vertex_count = snapshot.vertex_count();
    let edge_count = snapshot.edge_count();

    println!("Found {} vertices and {} edges", vertex_count, edge_count);

    // Create output file
    println!("Writing to {}...", output_path.display());
    let file = File::create(&output_path).map_err(|e| {
        CliError::io_with_source(
            format!("Failed to create output file: {}", output_path.display()),
            e,
        )
    })?;
    let writer = BufWriter::new(file);

    // Export to GraphSON
    if pretty {
        graphson::to_writer_pretty(&snapshot, writer)
            .map_err(|e| CliError::io(format!("Failed to write GraphSON: {}", e)))?;
    } else {
        graphson::to_writer(&snapshot, writer)
            .map_err(|e| CliError::io(format!("Failed to write GraphSON: {}", e)))?;
    }

    let elapsed = start.elapsed();

    println!();
    println!("Export complete!");
    println!("  Vertices exported: {}", vertex_count);
    println!("  Edges exported:    {}", edge_count);
    println!("  Output file:       {}", output_path.display());
    println!("  Time:              {:.2}s", elapsed.as_secs_f64());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use interstellar::storage::Graph;
    use interstellar::Value;
    use std::collections::HashMap;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_db(path: &PathBuf) {
        let graph = PersistentGraph::open(path).unwrap();
        graph
            .batch(|ctx| {
                let alice = ctx.add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
                );
                let bob = ctx.add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
                );
                ctx.add_edge(alice, bob, "knows", HashMap::new())?;
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_export_graphson() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let output_path = temp_dir.path().join("output.json");

        create_test_db(&db_path);

        let result = execute(db_path, output_path.clone(), true);
        assert!(result.is_ok());

        // Verify output file exists and is valid JSON
        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("tinker:graph"));
        assert!(content.contains("Alice"));
        assert!(content.contains("Bob"));
        assert!(content.contains("knows"));
    }

    #[test]
    fn test_export_compact() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let output_path = temp_dir.path().join("output.json");

        create_test_db(&db_path);

        let result = execute(db_path, output_path.clone(), false);
        assert!(result.is_ok());

        // Compact output should have minimal whitespace
        let content = fs::read_to_string(&output_path).unwrap();
        assert!(!content.contains("  ")); // No indentation
    }

    #[test]
    fn test_export_empty_db() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let output_path = temp_dir.path().join("output.json");

        // Create empty database
        PersistentGraph::open(&db_path).unwrap();

        let result = execute(db_path, output_path.clone(), true);
        assert!(result.is_ok());

        let content = fs::read_to_string(&output_path).unwrap();
        assert!(content.contains("tinker:graph"));
    }

    #[test]
    fn test_export_db_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("nonexistent.db");
        let output_path = temp_dir.path().join("output.json");

        let result = execute(db_path, output_path, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_export_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let output_path = temp_dir.path().join("output.json");

        create_test_db(&db_path);

        // Export
        execute(db_path.clone(), output_path.clone(), true).unwrap();

        // Import into new database
        let _db_path2 = temp_dir.path().join("test2.db");
        let json = fs::read_to_string(&output_path).unwrap();
        let imported = graphson::from_str(&json).unwrap();

        // Verify counts match
        let original = PersistentGraph::open(&db_path).unwrap();
        assert_eq!(imported.snapshot().vertex_count(), original.vertex_count());
        assert_eq!(imported.snapshot().edge_count(), original.edge_count());
    }
}
