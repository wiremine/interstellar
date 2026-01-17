//! Schema error types.
//!
//! This module defines errors that can occur during schema validation
//! and schema manipulation operations.

use thiserror::Error;

use super::PropertyType;

/// Errors that can occur during schema validation or manipulation.
#[derive(Debug, Error, Clone)]
pub enum SchemaError {
    /// Unknown vertex label in CLOSED validation mode.
    #[error("unknown vertex label: {label}")]
    UnknownVertexLabel {
        /// The unrecognized label.
        label: String,
    },

    /// Unknown edge label in CLOSED validation mode.
    #[error("unknown edge label: {label}")]
    UnknownEdgeLabel {
        /// The unrecognized label.
        label: String,
    },

    /// A required property was not provided.
    #[error("missing required property '{property}' on {element_type} '{label}'")]
    MissingRequired {
        /// "vertex" or "edge".
        element_type: &'static str,
        /// The element's label.
        label: String,
        /// The missing property key.
        property: String,
    },

    /// A property value has the wrong type.
    #[error("type mismatch for property '{property}': expected {expected}, got {actual}")]
    TypeMismatch {
        /// The property key with wrong type.
        property: String,
        /// Expected type.
        expected: PropertyType,
        /// Actual type name.
        actual: String,
    },

    /// A property was provided that is not in the schema (when additional properties are disallowed).
    #[error("unexpected property '{property}' on {element_type} '{label}'")]
    UnexpectedProperty {
        /// "vertex" or "edge".
        element_type: &'static str,
        /// The element's label.
        label: String,
        /// The unexpected property key.
        property: String,
    },

    /// Edge source vertex has invalid label.
    #[error("invalid edge endpoint: '{edge_label}' cannot connect from '{from_label}' (allowed: {allowed:?})")]
    InvalidSourceLabel {
        /// The edge type label.
        edge_label: String,
        /// The source vertex label that was used.
        from_label: String,
        /// List of allowed source labels.
        allowed: Vec<String>,
    },

    /// Edge target vertex has invalid label.
    #[error("invalid edge endpoint: '{edge_label}' cannot connect to '{to_label}' (allowed: {allowed:?})")]
    InvalidTargetLabel {
        /// The edge type label.
        edge_label: String,
        /// The target vertex label that was used.
        to_label: String,
        /// List of allowed target labels.
        allowed: Vec<String>,
    },

    /// A required property was explicitly set to null.
    #[error("null value for required property '{property}' on {element_type} '{label}'")]
    NullRequired {
        /// "vertex" or "edge".
        element_type: &'static str,
        /// The element's label.
        label: String,
        /// The property that was set to null.
        property: String,
    },

    /// Attempted to create a type that already exists.
    #[error("type '{name}' already exists")]
    TypeAlreadyExists {
        /// The duplicate type name.
        name: String,
    },

    /// Referenced type was not found.
    #[error("type '{name}' not found")]
    TypeNotFound {
        /// The missing type name.
        name: String,
    },

    /// Attempted to add a property that already exists on the type.
    #[error("property '{property}' already exists on type '{type_name}'")]
    PropertyAlreadyExists {
        /// The type name.
        type_name: String,
        /// The duplicate property name.
        property: String,
    },

    /// Referenced property was not found on the type.
    #[error("property '{property}' not found on type '{type_name}'")]
    PropertyNotFound {
        /// The type name.
        type_name: String,
        /// The missing property name.
        property: String,
    },

    /// Edge type definition missing FROM and TO constraints.
    #[error("edge type must specify FROM and TO endpoint constraints")]
    MissingEndpointConstraints,

    /// Index DDL cannot be executed through schema DDL execution.
    /// Index operations require graph storage access.
    #[error("index DDL must be executed via graph.create_index() or graph.drop_index()")]
    IndexDdlNotSupported,
}

/// Result type for schema operations.
pub type SchemaResult<T> = Result<T, SchemaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_messages_are_descriptive() {
        let err = SchemaError::MissingRequired {
            element_type: "vertex",
            label: "Person".to_string(),
            property: "name".to_string(),
        };
        assert!(err.to_string().contains("name"));
        assert!(err.to_string().contains("Person"));
        assert!(err.to_string().contains("vertex"));

        let err = SchemaError::TypeMismatch {
            property: "age".to_string(),
            expected: PropertyType::Int,
            actual: "STRING".to_string(),
        };
        assert!(err.to_string().contains("age"));
        assert!(err.to_string().contains("INT"));
        assert!(err.to_string().contains("STRING"));
    }

    #[test]
    fn error_is_clone() {
        let err = SchemaError::UnknownVertexLabel {
            label: "Test".to_string(),
        };
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }
}
