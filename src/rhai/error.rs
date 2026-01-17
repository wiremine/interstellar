//! Error types for Rhai scripting integration.

use rhai::EvalAltResult;
use thiserror::Error;

/// Errors that can occur during Rhai script execution.
#[derive(Debug, Error)]
pub enum RhaiError {
    /// Script compilation/parsing failed.
    #[error("script compilation failed: {0}")]
    Compile(String),

    /// Script execution failed.
    #[error("script execution failed: {0}")]
    Execution(String),

    /// A traversal operation failed.
    #[error("traversal error: {0}")]
    Traversal(#[from] crate::error::TraversalError),

    /// A storage operation failed.
    #[error("storage error: {0}")]
    Storage(#[from] crate::error::StorageError),

    /// Type conversion error.
    #[error("type error: expected {expected}, got {actual}")]
    Type {
        /// The expected type name.
        expected: String,
        /// The actual type name.
        actual: String,
    },

    /// A required argument was not provided.
    #[error("missing argument: {0}")]
    MissingArgument(String),

    /// The traversal has already been consumed.
    #[error("traversal already consumed")]
    TraversalConsumed,
}

impl From<Box<EvalAltResult>> for RhaiError {
    fn from(err: Box<EvalAltResult>) -> Self {
        RhaiError::Execution(err.to_string())
    }
}

impl From<rhai::ParseError> for RhaiError {
    fn from(err: rhai::ParseError) -> Self {
        RhaiError::Compile(err.to_string())
    }
}

/// Result type alias for Rhai operations.
pub type RhaiResult<T> = Result<T, RhaiError>;
