//! Error types for Interstellar CLI with mapped exit codes.

use std::path::PathBuf;
use thiserror::Error;

/// Exit codes as specified in the project brief.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    /// Success
    Success = 0,
    /// General error
    GeneralError = 1,
    /// Connection/database error
    DatabaseError = 2,
    /// Query syntax error
    QuerySyntaxError = 3,
    /// Query execution error
    QueryExecutionError = 4,
    /// File I/O error
    FileIoError = 5,
}

impl From<ExitCode> for i32 {
    fn from(code: ExitCode) -> Self {
        code as i32
    }
}

/// Main error type for the CLI.
#[derive(Debug, Error)]
pub enum CliError {
    #[error("Database error: {message}")]
    Database {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("Database not found: {path}")]
    DatabaseNotFound { path: PathBuf },

    #[error("Database already exists: {path}")]
    DatabaseExists { path: PathBuf },

    #[error("Query syntax error: {message}")]
    QuerySyntax { message: String },

    #[error("Query execution error: {message}")]
    QueryExecution {
        message: String,
        #[source]
        source: Option<anyhow::Error>,
    },

    #[error("I/O error: {message}")]
    Io {
        message: String,
        #[source]
        source: Option<std::io::Error>,
    },

    #[error("Configuration error: {message}")]
    Config { message: String },

    #[error("Invalid argument: {message}")]
    InvalidArgument { message: String },

    #[error("{message}")]
    General { message: String },
}

impl CliError {
    /// Get the exit code for this error.
    pub fn exit_code(&self) -> ExitCode {
        match self {
            CliError::Database { .. }
            | CliError::DatabaseNotFound { .. }
            | CliError::DatabaseExists { .. } => ExitCode::DatabaseError,
            CliError::QuerySyntax { .. } => ExitCode::QuerySyntaxError,
            CliError::QueryExecution { .. } => ExitCode::QueryExecutionError,
            CliError::Io { .. } => ExitCode::FileIoError,
            CliError::Config { .. }
            | CliError::InvalidArgument { .. }
            | CliError::General { .. } => ExitCode::GeneralError,
        }
    }

    /// Create a database error.
    pub fn database(message: impl Into<String>) -> Self {
        CliError::Database {
            message: message.into(),
            source: None,
        }
    }

    /// Create a database error with source.
    pub fn database_with_source(
        message: impl Into<String>,
        source: impl Into<anyhow::Error>,
    ) -> Self {
        CliError::Database {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    /// Create a query syntax error.
    pub fn query_syntax(message: impl Into<String>) -> Self {
        CliError::QuerySyntax {
            message: message.into(),
        }
    }

    /// Create a query execution error.
    pub fn query_execution(message: impl Into<String>) -> Self {
        CliError::QueryExecution {
            message: message.into(),
            source: None,
        }
    }

    /// Create an I/O error.
    #[allow(dead_code)] // Will be used in Phase 4 for import/export
    pub fn io(message: impl Into<String>) -> Self {
        CliError::Io {
            message: message.into(),
            source: None,
        }
    }

    /// Create an I/O error with source.
    pub fn io_with_source(message: impl Into<String>, source: std::io::Error) -> Self {
        CliError::Io {
            message: message.into(),
            source: Some(source),
        }
    }

    /// Create a config error.
    pub fn config(message: impl Into<String>) -> Self {
        CliError::Config {
            message: message.into(),
        }
    }

    /// Create an invalid argument error.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        CliError::InvalidArgument {
            message: message.into(),
        }
    }

    /// Create a general error.
    pub fn general(message: impl Into<String>) -> Self {
        CliError::General {
            message: message.into(),
        }
    }
}

impl From<std::io::Error> for CliError {
    fn from(err: std::io::Error) -> Self {
        CliError::Io {
            message: err.to_string(),
            source: Some(err),
        }
    }
}

/// Result type alias for CLI operations.
pub type Result<T> = std::result::Result<T, CliError>;
