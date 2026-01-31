//! Query command - execute queries (interactive REPL or one-shot).

use std::fs;
use std::io::{self, IsTerminal, Read};
use std::path::PathBuf;
use std::time::Instant;

use interstellar::storage::{Graph, PersistentGraph};

use crate::config::Config;
use crate::error::{CliError, Result};
use crate::format::{self, QueryResult};
use crate::gremlin::{format_result, GremlinEngine, PersistentGremlinEngine};
use crate::repl::{self, RuntimeSettings};
use crate::OutputFormat;
use interstellar::gremlin::ExecutionResult;

/// Execute the query command.
#[allow(clippy::too_many_arguments)]
pub fn execute(
    path: Option<PathBuf>,
    query: Option<String>,
    file: Option<PathBuf>,
    use_gremlin: bool,
    format: OutputFormat,
    limit: usize,
    timing: bool,
    memory: bool,
    _readonly: bool, // MmapGraph doesn't have a separate readonly mode
) -> Result<()> {
    // Determine query source (inline, file, or stdin)
    let query_text = match (&query, &file) {
        (Some(q), None) => Some(q.clone()),
        (None, Some(f)) => Some(read_query_file(f)?),
        (Some(_), Some(_)) => {
            return Err(CliError::invalid_argument(
                "Cannot specify both inline query and --file",
            ));
        }
        (None, None) => {
            // Check if stdin has data (piped input)
            if !io::stdin().is_terminal() {
                Some(read_stdin()?)
            } else {
                None // Interactive mode
            }
        }
    };

    // Infer language from file extension if applicable
    let use_gremlin = if let Some(f) = &file {
        infer_language(f, use_gremlin)
    } else {
        use_gremlin
    };

    // Open or create the database
    let graph = if memory {
        Graph::new()
    } else {
        let path = path.ok_or_else(|| {
            CliError::invalid_argument(
                "Database path required. Provide path or set INTERSTELLAR_DB.",
            )
        })?;

        if !path.exists() {
            return Err(CliError::DatabaseNotFound { path });
        }

        let persistent = PersistentGraph::open(&path).map_err(|e| {
            CliError::database_with_source(
                format!("Failed to open database: {}", path.display()),
                anyhow::anyhow!("{}", e),
            )
        })?;
        return execute_with_persistent_graph(
            persistent,
            query_text,
            use_gremlin,
            format,
            limit,
            timing,
        );
    };

    execute_with_graph(graph, query_text, use_gremlin, format, limit, timing)
}

/// Execute query with a Graph.
fn execute_with_graph(
    graph: Graph,
    query_text: Option<String>,
    use_gremlin: bool,
    format: OutputFormat,
    limit: usize,
    timing: bool,
) -> Result<()> {
    match query_text {
        Some(query) => {
            // One-shot query execution
            execute_one_shot_graph(graph, &query, use_gremlin, format, limit, timing)
        }
        None => {
            // Interactive REPL
            let config = Config::load()?;
            let mut repl_config = config.repl;

            // Override with command-line gremlin flag
            if use_gremlin {
                repl_config.default_mode = crate::config::QueryMode::Gremlin;
            }

            let settings = RuntimeSettings::new(format, limit, timing);
            repl::start(graph, repl_config, settings)
        }
    }
}

/// Execute query with a PersistentGraph.
fn execute_with_persistent_graph(
    graph: PersistentGraph,
    query_text: Option<String>,
    use_gremlin: bool,
    format: OutputFormat,
    limit: usize,
    timing: bool,
) -> Result<()> {
    match query_text {
        Some(query) => {
            // One-shot query execution
            execute_one_shot_persistent(graph, &query, use_gremlin, format, limit, timing)
        }
        None => {
            // Interactive REPL
            let config = Config::load()?;
            let mut repl_config = config.repl;

            // Override with command-line gremlin flag
            if use_gremlin {
                repl_config.default_mode = crate::config::QueryMode::Gremlin;
            }

            let settings = RuntimeSettings::new(format, limit, timing);
            repl::start_persistent(graph, repl_config, settings)
        }
    }
}

/// Execute a one-shot query with Graph.
fn execute_one_shot_graph(
    graph: Graph,
    query: &str,
    use_gremlin: bool,
    format: OutputFormat,
    limit: usize,
    timing: bool,
) -> Result<()> {
    if use_gremlin {
        return execute_gremlin_one_shot_graph(graph, query, timing);
    }

    // Execute GQL query (may contain multiple statements)
    let start = Instant::now();

    // Split into statements and execute each one
    // Skip comment lines (-- or //) and empty lines
    let statements: Vec<&str> = query
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty() && !trimmed.starts_with("--") && !trimmed.starts_with("//")
        })
        .collect();

    // Join remaining lines and split by semicolons
    let joined = statements.join(" ");
    let queries: Vec<&str> = joined
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if queries.is_empty() {
        return Err(CliError::query_execution(
            "No statements to execute".to_string(),
        ));
    }

    let mut all_results = Vec::new();

    for q in queries {
        let results = graph.gql(q).map_err(|e| {
            let msg = e.to_string();
            if msg.contains("parse") || msg.contains("Parse") || msg.contains("syntax") {
                CliError::query_syntax(msg)
            } else {
                CliError::query_execution(msg)
            }
        })?;
        all_results.extend(results);
    }

    let elapsed = start.elapsed();

    // Apply limit
    let results: Vec<_> = if limit > 0 {
        all_results.into_iter().take(limit).collect()
    } else {
        all_results
    };

    // Create query result
    let query_result = QueryResult::new(results, if timing { Some(elapsed) } else { None });

    // Format and print output
    let output = format::format_results(&query_result, format.into(), 0);
    print!("{}", output);

    Ok(())
}

/// Execute Gremlin query with Graph.
fn execute_gremlin_one_shot_graph(graph: Graph, script: &str, timing: bool) -> Result<()> {
    let engine = GremlinEngine::new(graph);

    // Preprocess: remove comment-only lines (# and //)
    let processed: String = script
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.starts_with('#') && !trimmed.starts_with("//")
        })
        .collect::<Vec<_>>()
        .join("\n");

    if processed.trim().is_empty() {
        return Err(CliError::query_execution(
            "No statements to execute".to_string(),
        ));
    }

    let start = Instant::now();

    // Execute the entire script as one unit
    let result = engine.execute(&processed)?;

    let elapsed = start.elapsed();

    // Print the final result if non-unit
    if !matches!(result, ExecutionResult::Unit) {
        let output = format_result(&result);
        println!("{}", output);
    }

    if timing {
        println!("Time: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
    }

    Ok(())
}

/// Execute a one-shot query with PersistentGraph.
fn execute_one_shot_persistent(
    graph: PersistentGraph,
    query: &str,
    use_gremlin: bool,
    format: OutputFormat,
    limit: usize,
    timing: bool,
) -> Result<()> {
    if use_gremlin {
        return execute_gremlin_one_shot_persistent(graph, query, timing);
    }

    // Execute GQL query (may contain multiple statements)
    let start = Instant::now();

    // Split into statements and execute each one
    // Skip comment lines (-- or //) and empty lines
    let statements: Vec<&str> = query
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.is_empty() && !trimmed.starts_with("--") && !trimmed.starts_with("//")
        })
        .collect();

    // Join remaining lines and split by semicolons
    let joined = statements.join(" ");
    let queries: Vec<&str> = joined
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    if queries.is_empty() {
        return Err(CliError::query_execution(
            "No statements to execute".to_string(),
        ));
    }

    let mut all_results = Vec::new();

    for q in queries {
        let results = graph.gql(q).map_err(|e| {
            let msg = e.to_string();
            if msg.contains("parse") || msg.contains("Parse") || msg.contains("syntax") {
                CliError::query_syntax(msg)
            } else {
                CliError::query_execution(msg)
            }
        })?;
        all_results.extend(results);
    }

    let elapsed = start.elapsed();

    // Apply limit
    let results: Vec<_> = if limit > 0 {
        all_results.into_iter().take(limit).collect()
    } else {
        all_results
    };

    // Create query result
    let query_result = QueryResult::new(results, if timing { Some(elapsed) } else { None });

    // Format and print output
    let output = format::format_results(&query_result, format.into(), 0);
    print!("{}", output);

    Ok(())
}

/// Execute Gremlin query with PersistentGraph.
fn execute_gremlin_one_shot_persistent(
    graph: PersistentGraph,
    script: &str,
    timing: bool,
) -> Result<()> {
    let engine = PersistentGremlinEngine::new(graph);

    // Preprocess: remove comment-only lines (# and //)
    let processed: String = script
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.starts_with('#') && !trimmed.starts_with("//")
        })
        .collect::<Vec<_>>()
        .join("\n");

    if processed.trim().is_empty() {
        return Err(CliError::query_execution(
            "No statements to execute".to_string(),
        ));
    }

    let start = Instant::now();

    // Execute the entire script as one unit
    let result = engine.execute(&processed)?;

    let elapsed = start.elapsed();

    // Print the final result if non-unit
    if !matches!(result, ExecutionResult::Unit) {
        let output = format_result(&result);
        println!("{}", output);
    }

    if timing {
        println!("Time: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
    }

    Ok(())
}

/// Read query from file.
fn read_query_file(path: &PathBuf) -> Result<String> {
    fs::read_to_string(path).map_err(|e| {
        CliError::io_with_source(format!("Failed to read query file: {}", path.display()), e)
    })
}

/// Read query from stdin.
fn read_stdin() -> Result<String> {
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .map_err(|e| CliError::io_with_source("Failed to read from stdin", e))?;
    Ok(input)
}

/// Infer query language from file extension.
fn infer_language(path: &PathBuf, default_gremlin: bool) -> bool {
    match path.extension().and_then(|e| e.to_str()) {
        Some("gql") => false,
        Some("gremlin") => true,
        _ => default_gremlin,
    }
}
