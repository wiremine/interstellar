//! Import command - import data from GraphSON files.

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use interstellar::graphson;
use interstellar::storage::{GraphStorage, PersistentGraph};

use crate::error::{CliError, Result};

/// Execute the import command.
///
/// Imports data from a GraphSON file into an existing database.
/// The file format is automatically detected from the extension.
pub fn execute(db_path: PathBuf, file_path: PathBuf, merge: bool) -> Result<()> {
    // Check that the file exists
    if !file_path.exists() {
        return Err(CliError::io(format!(
            "Import file not found: {}",
            file_path.display()
        )));
    }

    // Detect format from extension
    let extension = file_path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_lowercase());

    match extension.as_deref() {
        Some("json") | Some("graphson") => import_graphson(db_path, file_path, merge),
        Some(ext) => Err(CliError::invalid_argument(format!(
            "Unsupported import format: .{}. Supported: .json, .graphson",
            ext
        ))),
        None => Err(CliError::invalid_argument(
            "Cannot determine file format. Use .json or .graphson extension.",
        )),
    }
}

/// Import data from a GraphSON file.
fn import_graphson(db_path: PathBuf, file_path: PathBuf, merge: bool) -> Result<()> {
    let start = Instant::now();

    // Read the GraphSON file
    println!("Reading {}...", file_path.display());
    let json = fs::read_to_string(&file_path).map_err(|e| {
        CliError::io_with_source(format!("Failed to read file: {}", file_path.display()), e)
    })?;

    // Parse the GraphSON data into an in-memory graph
    println!("Parsing GraphSON data...");
    let source_graph = graphson::from_str(&json)
        .map_err(|e| CliError::database(format!("Failed to parse GraphSON: {}", e)))?;

    let source_snapshot = source_graph.snapshot();
    let vertex_count = source_snapshot.vertex_count();
    let edge_count = source_snapshot.edge_count();

    println!("Found {} vertices and {} edges", vertex_count, edge_count);

    if vertex_count == 0 && edge_count == 0 {
        println!("Nothing to import.");
        return Ok(());
    }

    // Open or create the target database
    let db_exists = db_path.exists();

    if !db_exists {
        println!("Creating new database: {}", db_path.display());
    } else if merge {
        println!("Merging into existing database: {}", db_path.display());
    } else {
        return Err(CliError::DatabaseExists { path: db_path });
    }

    let target_graph = PersistentGraph::open(&db_path).map_err(|e| {
        CliError::database_with_source(
            format!("Failed to open database: {}", db_path.display()),
            anyhow::anyhow!("{}", e),
        )
    })?;

    // Import using batch operation
    println!("Importing data...");

    // Collect vertices and edges from source
    let vertices: Vec<_> = source_snapshot.all_vertices().collect();
    let edges: Vec<_> = source_snapshot.all_edges().collect();

    // Use batch to import everything
    target_graph
        .batch(|ctx| {
            use interstellar::error::StorageError;
            use std::collections::HashMap;

            // Map old vertex IDs to new vertex IDs
            let mut id_map: HashMap<u64, interstellar::value::VertexId> = HashMap::new();

            // Add vertices
            for vertex in &vertices {
                let new_id = ctx.add_vertex(&vertex.label, vertex.properties.clone());
                id_map.insert(vertex.id.0, new_id);
            }

            // Add edges
            for edge in &edges {
                let from_id = id_map
                    .get(&edge.src.0)
                    .copied()
                    .ok_or_else(|| StorageError::VertexNotFound(edge.src))?;
                let to_id = id_map
                    .get(&edge.dst.0)
                    .copied()
                    .ok_or_else(|| StorageError::VertexNotFound(edge.dst))?;

                ctx.add_edge(from_id, to_id, &edge.label, edge.properties.clone())?;
            }

            Ok(())
        })
        .map_err(|e| CliError::database(format!("Import failed: {}", e)))?;

    let elapsed = start.elapsed();

    println!();
    println!("Import complete!");
    println!("  Vertices imported: {}", vertex_count);
    println!("  Edges imported:    {}", edge_count);
    println!("  Time:              {:.2}s", elapsed.as_secs_f64());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_import_empty_graphson() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let file_path = temp_dir.path().join("empty.json");

        // Create empty GraphSON file
        fs::write(
            &file_path,
            r#"{"@type": "tinker:graph", "@value": {"vertices": [], "edges": []}}"#,
        )
        .unwrap();

        // Import should succeed but do nothing
        let result = execute(db_path, file_path, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_import_simple_graphson() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let file_path = temp_dir.path().join("simple.json");

        // Create simple GraphSON file
        let json = r#"{
            "@type": "tinker:graph",
            "@value": {
                "vertices": [
                    {
                        "@type": "g:Vertex",
                        "@value": {
                            "id": {"@type": "g:Int64", "@value": 1},
                            "label": "person",
                            "properties": {
                                "name": [{"@type": "g:VertexProperty", "@value": {"id": {"@type": "g:Int64", "@value": 0}, "label": "name", "value": "Alice"}}]
                            }
                        }
                    }
                ],
                "edges": []
            }
        }"#;
        fs::write(&file_path, json).unwrap();

        let result = execute(db_path.clone(), file_path, false);
        assert!(result.is_ok());

        // Verify import
        let graph = PersistentGraph::open(&db_path).unwrap();
        assert_eq!(graph.vertex_count(), 1);
    }

    #[test]
    fn test_import_file_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let file_path = temp_dir.path().join("nonexistent.json");

        let result = execute(db_path, file_path, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_import_unsupported_format() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let file_path = temp_dir.path().join("data.xml");

        fs::write(&file_path, "<graph></graph>").unwrap();

        let result = execute(db_path, file_path, false);
        assert!(result.is_err());
    }
}
