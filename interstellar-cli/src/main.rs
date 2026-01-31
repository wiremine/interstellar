//! Interstellar CLI - Command-line interface for the Interstellar graph database.

mod commands;
mod config;
mod error;
mod format;
mod gremlin;
mod repl;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand, ValueEnum};

use crate::config::Config;
use crate::error::CliError;

/// Interstellar CLI - A command-line interface for the Interstellar graph database.
#[derive(Parser, Debug)]
#[command(name = "interstellar", version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Database path (opens REPL if no subcommand given)
    #[arg(env = "INTERSTELLAR_DB")]
    path: Option<PathBuf>,

    /// Output format
    #[arg(long, short, value_enum, env = "INTERSTELLAR_FORMAT")]
    format: Option<OutputFormat>,

    /// Limit number of results (0 = unlimited)
    #[arg(long, env = "INTERSTELLAR_LIMIT")]
    limit: Option<usize>,

    /// Show query execution time
    #[arg(long)]
    timing: bool,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Create a new database
    Create {
        /// Path for the new database
        path: PathBuf,

        /// Overwrite existing database if present
        #[arg(long, short)]
        force: bool,

        /// Initialize with sample dataset (marvel, british_royals)
        #[arg(long)]
        with_sample: Option<String>,

        /// Exit after creation without opening REPL
        #[arg(long)]
        no_repl: bool,

        /// Open web UI instead of REPL (future)
        #[arg(long)]
        ui: bool,
    },

    /// Execute queries (interactive REPL or one-shot)
    Query {
        /// Database path
        #[arg(env = "INTERSTELLAR_DB")]
        path: Option<PathBuf>,

        /// Use GQL parser (default)
        #[arg(long, group = "language")]
        gql: bool,

        /// Use Gremlin parser
        #[arg(long, group = "language")]
        gremlin: bool,

        /// Inline query to execute
        #[arg(name = "QUERY")]
        query: Option<String>,

        /// Read query from file
        #[arg(long, short)]
        file: Option<PathBuf>,

        /// Output format
        #[arg(long, value_enum)]
        format: Option<OutputFormat>,

        /// Limit number of results
        #[arg(long)]
        limit: Option<usize>,

        /// Show query execution time
        #[arg(long)]
        timing: bool,

        /// Use in-memory database
        #[arg(long)]
        memory: bool,

        /// Open database in read-only mode
        #[arg(long)]
        readonly: bool,
    },

    /// Import data from GraphSON file
    Import {
        /// Database path (will be created if it doesn't exist)
        path: PathBuf,

        /// GraphSON file to import (.json or .graphson)
        file: PathBuf,

        /// Merge into existing database instead of failing if it exists
        #[arg(long)]
        merge: bool,
    },

    /// Export data to GraphSON file
    Export {
        /// Database path
        path: PathBuf,

        /// Output file (.json or .graphson)
        output: PathBuf,

        /// Pretty-print JSON output
        #[arg(long)]
        pretty: bool,
    },

    /// Display database statistics
    Stats {
        /// Database path
        #[arg(env = "INTERSTELLAR_DB")]
        path: Option<PathBuf>,

        /// Output format
        #[arg(long, value_enum)]
        format: Option<OutputFormat>,
    },

    /// Display inferred schema
    Schema {
        /// Database path
        #[arg(env = "INTERSTELLAR_DB")]
        path: Option<PathBuf>,

        /// Output format
        #[arg(long, value_enum)]
        format: Option<OutputFormat>,
    },

    /// Start web UI server (future)
    Serve {
        /// Database path
        path: PathBuf,

        /// Port to listen on
        #[arg(long, short, default_value = "8080")]
        port: u16,

        /// Open browser automatically
        #[arg(long)]
        open: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
    Csv,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Table => write!(f, "table"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::Csv => write!(f, "csv"),
        }
    }
}

fn run() -> Result<(), CliError> {
    let cli = Cli::parse();
    let config = Config::load()?;

    // Merge CLI args with config
    let format = cli.format.unwrap_or(config.format);
    let limit = cli.limit.unwrap_or(config.limit);
    let timing = cli.timing || config.timing;

    match cli.command {
        Some(Commands::Create {
            path,
            force,
            with_sample,
            no_repl,
            ui,
        }) => {
            commands::create::execute(path, force, with_sample, no_repl, ui)?;
        }

        Some(Commands::Query {
            path,
            gql,
            gremlin,
            query,
            file,
            format: query_format,
            limit: query_limit,
            timing: query_timing,
            memory,
            readonly,
        }) => {
            let format = query_format.unwrap_or(format);
            let limit = query_limit.unwrap_or(limit);
            let timing = query_timing || timing;
            let use_gremlin = gremlin && !gql;

            commands::query::execute(
                path.or(cli.path),
                query,
                file,
                use_gremlin,
                format,
                limit,
                timing,
                memory,
                readonly,
            )?;
        }

        Some(Commands::Stats {
            path,
            format: stats_format,
        }) => {
            let format = stats_format.unwrap_or(format);
            let db_path = path.or(cli.path).ok_or_else(|| {
                CliError::invalid_argument(
                    "Database path required. Provide path or set INTERSTELLAR_DB.",
                )
            })?;
            commands::stats::execute(db_path, format)?;
        }

        Some(Commands::Schema {
            path,
            format: schema_format,
        }) => {
            let format = schema_format.unwrap_or(format);
            let db_path = path.or(cli.path).ok_or_else(|| {
                CliError::invalid_argument(
                    "Database path required. Provide path or set INTERSTELLAR_DB.",
                )
            })?;
            // Schema command is Phase 5, stub for now
            eprintln!("Schema command not yet implemented (Phase 5)");
            eprintln!("Database: {:?}, Format: {}", db_path, format);
        }

        Some(Commands::Import { path, file, merge }) => {
            commands::import::execute(path, file, merge)?;
        }

        Some(Commands::Export {
            path,
            output,
            pretty,
        }) => {
            commands::export::execute(path, output, pretty)?;
        }

        Some(Commands::Serve { path, port, open }) => {
            // Serve command is Phase 6, stub for now
            eprintln!("Serve command not yet implemented (Phase 6)");
            eprintln!("Database: {:?}, Port: {}, Open: {}", path, port, open);
        }

        None => {
            // No subcommand: open REPL with database if path provided
            if let Some(path) = cli.path {
                commands::query::execute(
                    Some(path),
                    None,
                    None,
                    false, // default to GQL
                    format,
                    limit,
                    timing,
                    false, // not memory
                    false, // not readonly
                )?;
            } else {
                // No path, show help
                eprintln!("No database path provided. Use --help for usage.");
                return Err(CliError::invalid_argument(
                    "Database path required. Provide path or set INTERSTELLAR_DB.",
                ));
            }
        }
    }

    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {}", e);
            ExitCode::from(e.exit_code() as u8)
        }
    }
}
