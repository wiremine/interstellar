//! Saved query commands - manage stored queries in the database.

use std::path::PathBuf;

use interstellar::query::{QueryType, SavedQuery};
use interstellar::storage::MmapGraph;

use crate::error::{CliError, Result};
use crate::OutputFormat;

/// Save a new query to the database.
pub fn save(
    path: PathBuf,
    name: String,
    query_text: String,
    description: Option<String>,
    query_type: QueryType,
) -> Result<()> {
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

    // Save the query
    let desc = description.unwrap_or_default();
    let query_id = graph
        .save_query(&name, query_type, &desc, &query_text)
        .map_err(|e| CliError::general(format!("Failed to save query: {}", e)))?;

    // Checkpoint to ensure durability
    graph
        .checkpoint()
        .map_err(|e| CliError::general(format!("Failed to checkpoint: {}", e)))?;

    println!("Saved query '{}' (id: {})", name, query_id);

    Ok(())
}

/// Get a saved query by name.
pub fn get(path: PathBuf, name: String, format: OutputFormat) -> Result<()> {
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

    // Get the query
    let query = graph
        .get_query(&name)
        .ok_or_else(|| CliError::general(format!("Query '{}' not found", name)))?;

    // Format and display output
    match format {
        OutputFormat::Table => print_query_table(&query),
        OutputFormat::Json => print_query_json(&query)?,
        OutputFormat::Csv => print_query_csv(&query),
    }

    Ok(())
}

/// List all saved queries.
pub fn list(path: PathBuf, format: OutputFormat) -> Result<()> {
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

    // Get all queries
    let queries = graph.list_queries();

    // Format and display output
    match format {
        OutputFormat::Table => print_list_table(&queries),
        OutputFormat::Json => print_list_json(&queries)?,
        OutputFormat::Csv => print_list_csv(&queries),
    }

    Ok(())
}

/// Delete a saved query by name.
pub fn delete(path: PathBuf, name: String) -> Result<()> {
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

    // Delete the query
    graph
        .delete_query(&name)
        .map_err(|e| CliError::general(format!("Failed to delete query: {}", e)))?;

    // Checkpoint to ensure durability
    graph
        .checkpoint()
        .map_err(|e| CliError::general(format!("Failed to checkpoint: {}", e)))?;

    println!("Deleted query '{}'", name);

    Ok(())
}

/// Run a saved query by name.
pub fn run(
    path: PathBuf,
    name: String,
    _params: Vec<(String, String)>,
    format: OutputFormat,
    limit: usize,
    timing: bool,
) -> Result<()> {
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

    // Get the query
    let query = graph
        .get_query(&name)
        .ok_or_else(|| CliError::general(format!("Query '{}' not found", name)))?;

    // Execute the query using the existing query command infrastructure
    // For now, we just run it directly without parameter substitution
    // TODO: Implement parameter substitution
    let query_text = query.query.clone();
    let use_gremlin = query.query_type == QueryType::Gremlin;

    // Close the graph before calling query execute (which opens its own)
    drop(graph);

    // Use the query command to execute
    crate::commands::query::execute(
        Some(path),
        Some(query_text),
        None,
        use_gremlin,
        format,
        limit,
        timing,
        false, // not memory
        false, // not readonly
    )
}

// =============================================================================
// Output Formatting
// =============================================================================

/// Print a single query in table format.
fn print_query_table(query: &SavedQuery) {
    println!("Name:        {}", query.name);
    println!("ID:          {}", query.id);
    println!("Type:        {}", query.query_type);
    if !query.description.is_empty() {
        println!("Description: {}", query.description);
    }
    println!("Query:       {}", query.query);
    if !query.parameters.is_empty() {
        let params: Vec<_> = query
            .parameters
            .iter()
            .map(|p| format!("${}", p.name))
            .collect();
        println!("Parameters:  {}", params.join(", "));
    }
}

/// Print a single query in JSON format.
fn print_query_json(query: &SavedQuery) -> Result<()> {
    let params: Vec<_> = query.parameters.iter().map(|p| &p.name).collect();
    let json = serde_json::json!({
        "name": query.name,
        "id": query.id,
        "type": format!("{}", query.query_type),
        "description": query.description,
        "query": query.query,
        "parameters": params,
    });

    println!(
        "{}",
        serde_json::to_string_pretty(&json)
            .map_err(|e| CliError::general(format!("Failed to serialize JSON: {}", e)))?
    );

    Ok(())
}

/// Print a single query in CSV format.
fn print_query_csv(query: &SavedQuery) {
    let params: Vec<&str> = query.parameters.iter().map(|p| p.name.as_str()).collect();
    println!("name,id,type,description,query,parameters");
    println!(
        "{},{},{},\"{}\",\"{}\",\"{}\"",
        query.name,
        query.id,
        query.query_type,
        query.description.replace('"', "\"\""),
        query.query.replace('"', "\"\""),
        params.join(";")
    );
}

/// Print query list in table format.
fn print_list_table(queries: &[SavedQuery]) {
    if queries.is_empty() {
        println!("No saved queries.");
        return;
    }

    println!("{:<4} {:<20} {:<8} {}", "ID", "Name", "Type", "Description");
    println!("{:-<4} {:-<20} {:-<8} {:-<40}", "", "", "", "");

    for query in queries {
        let desc = if query.description.len() > 40 {
            format!("{}...", &query.description[..37])
        } else {
            query.description.clone()
        };
        println!(
            "{:<4} {:<20} {:<8} {}",
            query.id, query.name, query.query_type, desc
        );
    }

    println!();
    println!("Total: {} queries", queries.len());
}

/// Print query list in JSON format.
fn print_list_json(queries: &[SavedQuery]) -> Result<()> {
    let list: Vec<_> = queries
        .iter()
        .map(|q| {
            let params: Vec<_> = q.parameters.iter().map(|p| &p.name).collect();
            serde_json::json!({
                "name": q.name,
                "id": q.id,
                "type": format!("{}", q.query_type),
                "description": q.description,
                "query": q.query,
                "parameters": params,
            })
        })
        .collect();

    println!(
        "{}",
        serde_json::to_string_pretty(&list)
            .map_err(|e| CliError::general(format!("Failed to serialize JSON: {}", e)))?
    );

    Ok(())
}

/// Print query list in CSV format.
fn print_list_csv(queries: &[SavedQuery]) {
    println!("name,id,type,description,parameters");
    for query in queries {
        let params: Vec<&str> = query.parameters.iter().map(|p| p.name.as_str()).collect();
        println!(
            "{},{},{},\"{}\",\"{}\"",
            query.name,
            query.id,
            query.query_type,
            query.description.replace('"', "\"\""),
            params.join(";")
        );
    }
}
