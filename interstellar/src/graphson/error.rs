//! Error types for GraphSON operations.

use thiserror::Error;

/// Errors that can occur during GraphSON operations.
#[derive(Debug, Error)]
pub enum GraphSONError {
    /// JSON parsing failed
    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    /// Unknown type tag encountered
    #[error("unknown type tag: {0}")]
    UnknownTypeTag(String),

    /// Missing required field
    #[error("missing required field: {0}")]
    MissingField(String),

    /// Invalid value for type
    #[error("invalid value for type {type_tag}: {message}")]
    InvalidValue {
        /// The GraphSON type tag
        type_tag: String,
        /// Description of the error
        message: String,
    },

    /// Duplicate vertex ID
    #[error("duplicate vertex ID: {0}")]
    DuplicateVertexId(u64),

    /// Duplicate edge ID
    #[error("duplicate edge ID: {0}")]
    DuplicateEdgeId(u64),

    /// Referenced vertex not found
    #[error("vertex not found: {0}")]
    VertexNotFound(u64),

    /// Map key is not a string
    #[error("map keys must be strings, found: {0}")]
    NonStringMapKey(String),

    /// Schema validation failed
    #[error("schema validation error: {0}")]
    SchemaValidation(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// A specialized Result type for GraphSON operations.
pub type Result<T> = std::result::Result<T, GraphSONError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = GraphSONError::UnknownTypeTag("g:Unknown".to_string());
        assert_eq!(err.to_string(), "unknown type tag: g:Unknown");

        let err = GraphSONError::InvalidValue {
            type_tag: "g:Int64".to_string(),
            message: "expected integer".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "invalid value for type g:Int64: expected integer"
        );

        let err = GraphSONError::VertexNotFound(42);
        assert_eq!(err.to_string(), "vertex not found: 42");
    }

    #[test]
    fn test_json_error_conversion() {
        let json_err: std::result::Result<serde_json::Value, _> =
            serde_json::from_str("not valid json");
        let err: GraphSONError = json_err.unwrap_err().into();
        assert!(matches!(err, GraphSONError::JsonParse(_)));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: GraphSONError = io_err.into();
        assert!(matches!(err, GraphSONError::Io(_)));
    }
}
