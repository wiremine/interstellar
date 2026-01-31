//! Create command - creates a new database.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use interstellar::storage::PersistentGraph;
use interstellar::Value;

use crate::config::Config;
use crate::error::{CliError, Result};
use crate::repl::{self, RuntimeSettings};
use crate::OutputFormat;

/// Execute the create command.
///
/// Creates a new database at the specified path, optionally loading sample data.
pub fn execute(
    path: PathBuf,
    force: bool,
    with_sample: Option<String>,
    no_repl: bool,
    ui: bool,
) -> Result<()> {
    // Check if database already exists
    if path.exists() {
        if force {
            // Remove existing database files
            remove_database_files(&path)?;
        } else {
            return Err(CliError::DatabaseExists { path });
        }
    }

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).map_err(|e| {
                CliError::io_with_source(
                    format!("Failed to create parent directory: {}", parent.display()),
                    e,
                )
            })?;
        }
    }

    // Create the database
    let graph = PersistentGraph::open(&path).map_err(|e| {
        CliError::database_with_source(
            format!("Failed to create database: {}", path.display()),
            anyhow::anyhow!("{}", e),
        )
    })?;

    println!("Created database: {}", path.display());

    // Load sample data if requested
    if let Some(sample_name) = with_sample {
        load_sample_data(&graph, &sample_name)?;
        println!("Loaded sample dataset: {}", sample_name);
    }

    // Handle post-creation behavior
    if ui {
        // Web UI is Phase 6
        eprintln!("Web UI not yet implemented (Phase 6)");
        eprintln!("Would open UI for database: {}", path.display());
        return Ok(());
    }

    if no_repl {
        // Just exit after creation
        return Ok(());
    }

    // Enter REPL
    drop(graph); // Close the graph, reopen for REPL

    let graph = PersistentGraph::open(&path).map_err(|e| {
        CliError::database_with_source(
            format!("Failed to reopen database: {}", path.display()),
            anyhow::anyhow!("{}", e),
        )
    })?;

    let config = Config::load()?;
    let settings = RuntimeSettings::new(OutputFormat::Table, config.limit, config.timing);
    repl::start_persistent(graph, config.repl, settings)
}

/// Remove database files (main db and WAL).
fn remove_database_files(path: &PathBuf) -> Result<()> {
    // Remove main database file
    if path.exists() {
        fs::remove_file(path).map_err(|e| {
            CliError::io_with_source(
                format!("Failed to remove existing database: {}", path.display()),
                e,
            )
        })?;
    }

    // Remove WAL file if it exists
    let wal_path = path.with_extension("wal");
    if wal_path.exists() {
        fs::remove_file(&wal_path).map_err(|e| {
            CliError::io_with_source(
                format!("Failed to remove WAL file: {}", wal_path.display()),
                e,
            )
        })?;
    }

    Ok(())
}

/// Load sample data into the database.
fn load_sample_data(graph: &PersistentGraph, sample_name: &str) -> Result<()> {
    match sample_name.to_lowercase().as_str() {
        "marvel" => load_marvel_sample(graph),
        "british_royals" | "royals" => load_royals_sample(graph),
        _ => Err(CliError::invalid_argument(format!(
            "Unknown sample dataset: {}. Available: marvel, british_royals",
            sample_name
        ))),
    }
}

/// Load Marvel Universe sample data.
fn load_marvel_sample(graph: &PersistentGraph) -> Result<()> {
    graph
        .batch(|ctx| {
            // Characters
            let iron_man = ctx.add_vertex(
                "Character",
                HashMap::from([
                    ("name".to_string(), Value::String("Tony Stark".to_string())),
                    ("alias".to_string(), Value::String("Iron Man".to_string())),
                    (
                        "powers".to_string(),
                        Value::String("Genius intellect, Powered armor".to_string()),
                    ),
                ]),
            );

            let captain = ctx.add_vertex(
                "Character",
                HashMap::from([
                    (
                        "name".to_string(),
                        Value::String("Steve Rogers".to_string()),
                    ),
                    (
                        "alias".to_string(),
                        Value::String("Captain America".to_string()),
                    ),
                    (
                        "powers".to_string(),
                        Value::String("Super soldier serum, Shield".to_string()),
                    ),
                ]),
            );

            let thor = ctx.add_vertex(
                "Character",
                HashMap::from([
                    (
                        "name".to_string(),
                        Value::String("Thor Odinson".to_string()),
                    ),
                    ("alias".to_string(), Value::String("Thor".to_string())),
                    (
                        "powers".to_string(),
                        Value::String("God of Thunder, Mjolnir".to_string()),
                    ),
                ]),
            );

            let hulk = ctx.add_vertex(
                "Character",
                HashMap::from([
                    (
                        "name".to_string(),
                        Value::String("Bruce Banner".to_string()),
                    ),
                    ("alias".to_string(), Value::String("Hulk".to_string())),
                    (
                        "powers".to_string(),
                        Value::String("Super strength, Invulnerability".to_string()),
                    ),
                ]),
            );

            let black_widow = ctx.add_vertex(
                "Character",
                HashMap::from([
                    (
                        "name".to_string(),
                        Value::String("Natasha Romanoff".to_string()),
                    ),
                    (
                        "alias".to_string(),
                        Value::String("Black Widow".to_string()),
                    ),
                    (
                        "powers".to_string(),
                        Value::String("Espionage, Combat expert".to_string()),
                    ),
                ]),
            );

            let spider_man = ctx.add_vertex(
                "Character",
                HashMap::from([
                    (
                        "name".to_string(),
                        Value::String("Peter Parker".to_string()),
                    ),
                    ("alias".to_string(), Value::String("Spider-Man".to_string())),
                    (
                        "powers".to_string(),
                        Value::String("Spider abilities, Web-slinging".to_string()),
                    ),
                ]),
            );

            // Teams
            let avengers = ctx.add_vertex(
                "Team",
                HashMap::from([
                    ("name".to_string(), Value::String("Avengers".to_string())),
                    ("founded".to_string(), Value::String("2012".to_string())),
                ]),
            );

            // Member relationships
            ctx.add_edge(iron_man, avengers, "member_of", HashMap::new())?;
            ctx.add_edge(captain, avengers, "member_of", HashMap::new())?;
            ctx.add_edge(thor, avengers, "member_of", HashMap::new())?;
            ctx.add_edge(hulk, avengers, "member_of", HashMap::new())?;
            ctx.add_edge(black_widow, avengers, "member_of", HashMap::new())?;
            ctx.add_edge(spider_man, avengers, "member_of", HashMap::new())?;

            // Other relationships
            ctx.add_edge(iron_man, captain, "ally", HashMap::new())?;
            ctx.add_edge(spider_man, iron_man, "mentored_by", HashMap::new())?;
            ctx.add_edge(thor, hulk, "ally", HashMap::new())?;

            Ok(())
        })
        .map_err(|e| CliError::database(e.to_string()))?;

    Ok(())
}

/// Load British Royals sample data.
fn load_royals_sample(graph: &PersistentGraph) -> Result<()> {
    graph
        .batch(|ctx| {
            // Royals
            let elizabeth = ctx.add_vertex(
                "Royal",
                HashMap::from([
                    (
                        "name".to_string(),
                        Value::String("Elizabeth II".to_string()),
                    ),
                    ("title".to_string(), Value::String("Queen".to_string())),
                    ("birth_year".to_string(), Value::Int(1926)),
                ]),
            );

            let philip = ctx.add_vertex(
                "Royal",
                HashMap::from([
                    (
                        "name".to_string(),
                        Value::String("Prince Philip".to_string()),
                    ),
                    (
                        "title".to_string(),
                        Value::String("Duke of Edinburgh".to_string()),
                    ),
                    ("birth_year".to_string(), Value::Int(1921)),
                ]),
            );

            let charles = ctx.add_vertex(
                "Royal",
                HashMap::from([
                    ("name".to_string(), Value::String("Charles III".to_string())),
                    ("title".to_string(), Value::String("King".to_string())),
                    ("birth_year".to_string(), Value::Int(1948)),
                ]),
            );

            let william = ctx.add_vertex(
                "Royal",
                HashMap::from([
                    (
                        "name".to_string(),
                        Value::String("Prince William".to_string()),
                    ),
                    (
                        "title".to_string(),
                        Value::String("Prince of Wales".to_string()),
                    ),
                    ("birth_year".to_string(), Value::Int(1982)),
                ]),
            );

            let harry = ctx.add_vertex(
                "Royal",
                HashMap::from([
                    (
                        "name".to_string(),
                        Value::String("Prince Harry".to_string()),
                    ),
                    (
                        "title".to_string(),
                        Value::String("Duke of Sussex".to_string()),
                    ),
                    ("birth_year".to_string(), Value::Int(1984)),
                ]),
            );

            let kate = ctx.add_vertex(
                "Royal",
                HashMap::from([
                    ("name".to_string(), Value::String("Catherine".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Princess of Wales".to_string()),
                    ),
                    ("birth_year".to_string(), Value::Int(1982)),
                ]),
            );

            // Relationships
            ctx.add_edge(elizabeth, philip, "married_to", HashMap::new())?;
            ctx.add_edge(elizabeth, charles, "parent_of", HashMap::new())?;
            ctx.add_edge(philip, charles, "parent_of", HashMap::new())?;
            ctx.add_edge(charles, william, "parent_of", HashMap::new())?;
            ctx.add_edge(charles, harry, "parent_of", HashMap::new())?;
            ctx.add_edge(william, harry, "sibling_of", HashMap::new())?;
            ctx.add_edge(harry, william, "sibling_of", HashMap::new())?;
            ctx.add_edge(william, kate, "married_to", HashMap::new())?;

            Ok(())
        })
        .map_err(|e| CliError::database(e.to_string()))?;

    Ok(())
}
