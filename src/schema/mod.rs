//! Graph schema definition and validation.
//!
//! This module provides types and functions for defining and enforcing graph schemas.
//! Schemas enable type-safe graph operations by validating vertices and edges at
//! mutation time.
//!
//! # Overview
//!
//! A [`GraphSchema`] consists of:
//! - **Vertex schemas**: Property constraints for vertices with specific labels
//! - **Edge schemas**: Endpoint constraints and property constraints for edges
//! - **Validation mode**: How strictly to enforce the schema
//!
//! # Design Principles
//!
//! 1. **Opt-in validation**: Schemas are optional; unschemaed graphs work as before
//! 2. **Fail-fast**: Validation errors surface at mutation time, not query time
//! 3. **Query-time defaults**: Default values are applied at query time, not stored
//! 4. **Required endpoints**: Edge types must specify source and target constraints
//!
//! # Usage
//!
//! ## Building Schemas Programmatically
//!
//! Use the [`SchemaBuilder`] fluent API:
//!
//! ```
//! use intersteller::schema::{SchemaBuilder, PropertyType, ValidationMode};
//! use intersteller::value::Value;
//!
//! let schema = SchemaBuilder::new()
//!     .mode(ValidationMode::Strict)
//!     .vertex("Person")
//!         .property("name", PropertyType::String)
//!         .optional("age", PropertyType::Int)
//!         .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
//!         .done()
//!     .edge("KNOWS")
//!         .from(&["Person"])
//!         .to(&["Person"])
//!         .optional("since", PropertyType::Int)
//!         .done()
//!     .build();
//! ```
//!
//! ## Validation Modes
//!
//! The [`ValidationMode`] determines how strictly schemas are enforced:
//!
//! | Mode | Unknown Label | Schema Violation | Additional Properties |
//! |------|---------------|------------------|----------------------|
//! | `None` | Allowed | Allowed | Allowed |
//! | `Warn` | Warning | Warning | Warning |
//! | `Strict` | Allowed | Error | Error (unless allowed) |
//! | `Closed` | Error | Error | Error (unless allowed) |
//!
//! ## Validating Data
//!
//! Use the validation functions to check data against a schema:
//!
//! ```
//! use intersteller::schema::{
//!     SchemaBuilder, PropertyType, ValidationMode,
//!     validate_vertex, validate_edge, apply_defaults,
//! };
//! use intersteller::value::Value;
//! use std::collections::HashMap;
//!
//! let schema = SchemaBuilder::new()
//!     .mode(ValidationMode::Strict)
//!     .vertex("Person")
//!         .property("name", PropertyType::String)
//!         .optional_with_default("active", PropertyType::Bool, Value::Bool(true))
//!         .done()
//!     .build();
//!
//! let mut props = HashMap::new();
//! props.insert("name".to_string(), Value::String("Alice".to_string()));
//!
//! // Validate
//! let results = validate_vertex(&schema, "Person", &props).unwrap();
//!
//! // Apply defaults
//! let props_with_defaults = apply_defaults(&schema, "Person", &props, true);
//! assert_eq!(props_with_defaults.get("active"), Some(&Value::Bool(true)));
//! ```
//!
//! # Module Structure
//!
//! - [`types`]: Core schema types ([`VertexSchema`], [`EdgeSchema`], [`PropertyDef`], [`PropertyType`])
//! - [`builder`]: Fluent builder API ([`SchemaBuilder`], [`VertexSchemaBuilder`], [`EdgeSchemaBuilder`])
//! - [`validation`]: Validation functions ([`validate_vertex`], [`validate_edge`], [`apply_defaults`])
//! - [`error`]: Error types ([`SchemaError`], [`SchemaResult`])

use std::collections::HashMap;

pub mod builder;
pub mod error;
pub mod serialize;
pub mod types;
pub mod validation;

// Re-export all public types for convenience
pub use builder::{EdgeSchemaBuilder, SchemaBuilder, VertexSchemaBuilder};
pub use error::{SchemaError, SchemaResult};
pub use serialize::{
    deserialize_schema, serialize_schema, SchemaSerializeError, SCHEMA_FORMAT_VERSION,
    SCHEMA_HEADER_SIZE, SCHEMA_MAGIC,
};
pub use types::{EdgeSchema, PropertyDef, PropertyType, VertexSchema};
pub use validation::{
    apply_defaults, validate_edge, validate_property_update, validate_vertex, ValidationResult,
};

/// Complete schema for a graph.
///
/// Contains type definitions for vertices and edges, along with the validation
/// mode that determines how strictly the schema is enforced.
///
/// # Example
///
/// ```
/// use intersteller::schema::{GraphSchema, ValidationMode, SchemaBuilder, PropertyType};
///
/// // Create schema using builder
/// let schema = SchemaBuilder::new()
///     .mode(ValidationMode::Strict)
///     .vertex("Person")
///         .property("name", PropertyType::String)
///         .done()
///     .build();
///
/// assert!(schema.has_vertex_schema("Person"));
/// assert_eq!(schema.mode, ValidationMode::Strict);
/// ```
#[derive(Clone, Debug, Default)]
pub struct GraphSchema {
    /// Vertex schemas keyed by label.
    pub vertex_schemas: HashMap<String, VertexSchema>,

    /// Edge schemas keyed by label.
    pub edge_schemas: HashMap<String, EdgeSchema>,

    /// Validation mode for schema enforcement.
    pub mode: ValidationMode,
}

impl GraphSchema {
    /// Create a new empty schema with default validation mode (None).
    ///
    /// For more control, use [`SchemaBuilder::new()`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new schema with the specified validation mode.
    ///
    /// # Example
    ///
    /// ```
    /// use intersteller::schema::{GraphSchema, ValidationMode};
    ///
    /// let schema = GraphSchema::with_mode(ValidationMode::Strict);
    /// assert_eq!(schema.mode, ValidationMode::Strict);
    /// ```
    pub fn with_mode(mode: ValidationMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }

    /// Get all defined vertex labels.
    ///
    /// # Example
    ///
    /// ```
    /// use intersteller::schema::{SchemaBuilder, PropertyType};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .vertex("Person").done()
    ///     .vertex("Company").done()
    ///     .build();
    ///
    /// let labels: Vec<_> = schema.vertex_labels().collect();
    /// assert_eq!(labels.len(), 2);
    /// ```
    pub fn vertex_labels(&self) -> impl Iterator<Item = &str> {
        self.vertex_schemas.keys().map(|s| s.as_str())
    }

    /// Get all defined edge labels.
    pub fn edge_labels(&self) -> impl Iterator<Item = &str> {
        self.edge_schemas.keys().map(|s| s.as_str())
    }

    /// Get the schema for a vertex label.
    ///
    /// Returns `None` if no schema is defined for the label.
    pub fn vertex_schema(&self, label: &str) -> Option<&VertexSchema> {
        self.vertex_schemas.get(label)
    }

    /// Get the schema for an edge label.
    ///
    /// Returns `None` if no schema is defined for the label.
    pub fn edge_schema(&self, label: &str) -> Option<&EdgeSchema> {
        self.edge_schemas.get(label)
    }

    /// Check if a vertex label has a schema defined.
    pub fn has_vertex_schema(&self, label: &str) -> bool {
        self.vertex_schemas.contains_key(label)
    }

    /// Check if an edge label has a schema defined.
    pub fn has_edge_schema(&self, label: &str) -> bool {
        self.edge_schemas.contains_key(label)
    }

    /// Get all edge labels that can connect from a vertex label.
    ///
    /// Returns edge labels where the specified vertex label is in the
    /// `from_labels` list, or where `from_labels` is empty (any source allowed).
    ///
    /// # Example
    ///
    /// ```
    /// use intersteller::schema::{SchemaBuilder, PropertyType};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .vertex("Person").done()
    ///     .vertex("Company").done()
    ///     .edge("WORKS_AT")
    ///         .from(&["Person"])
    ///         .to(&["Company"])
    ///         .done()
    ///     .edge("KNOWS")
    ///         .from(&["Person"])
    ///         .to(&["Person"])
    ///         .done()
    ///     .build();
    ///
    /// let edges = schema.edges_from("Person");
    /// assert!(edges.contains(&"WORKS_AT"));
    /// assert!(edges.contains(&"KNOWS"));
    /// ```
    pub fn edges_from(&self, vertex_label: &str) -> Vec<&str> {
        self.edge_schemas
            .iter()
            .filter(|(_, schema)| {
                schema.from_labels.is_empty()
                    || schema.from_labels.iter().any(|l| l == vertex_label)
            })
            .map(|(label, _)| label.as_str())
            .collect()
    }

    /// Get all edge labels that can connect to a vertex label.
    ///
    /// Returns edge labels where the specified vertex label is in the
    /// `to_labels` list, or where `to_labels` is empty (any target allowed).
    pub fn edges_to(&self, vertex_label: &str) -> Vec<&str> {
        self.edge_schemas
            .iter()
            .filter(|(_, schema)| {
                schema.to_labels.is_empty() || schema.to_labels.iter().any(|l| l == vertex_label)
            })
            .map(|(label, _)| label.as_str())
            .collect()
    }

    /// Check if the schema is empty (no vertex or edge schemas defined).
    pub fn is_empty(&self) -> bool {
        self.vertex_schemas.is_empty() && self.edge_schemas.is_empty()
    }

    /// Get the total number of defined types (vertex + edge schemas).
    pub fn type_count(&self) -> usize {
        self.vertex_schemas.len() + self.edge_schemas.len()
    }
}

/// How strictly to enforce schema validation.
///
/// The validation mode determines what happens when data doesn't match
/// the schema during mutation operations.
///
/// # Modes
///
/// - **None**: Schema is documentation only; no validation is performed
/// - **Warn**: Validation warnings are logged but mutations proceed
/// - **Strict**: Validation is enforced for known types; unknown types are allowed
/// - **Closed**: All types must have schemas; unknown types cause errors
///
/// # Example
///
/// ```
/// use intersteller::schema::{ValidationMode, GraphSchema};
///
/// let mut schema = GraphSchema::new();
/// schema.mode = ValidationMode::Strict;
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ValidationMode {
    /// No validation (schema is documentation only).
    ///
    /// All mutations are allowed regardless of schema definitions.
    /// This is useful for development or when you want to define schemas
    /// for documentation without enforcing them.
    #[default]
    None,

    /// Log warnings but allow invalid data.
    ///
    /// Schema violations produce warnings that can be logged or collected,
    /// but mutations still proceed. This is useful for migration periods
    /// when you want to identify non-conforming data without breaking changes.
    Warn,

    /// Validate labels with schemas, allow unknown labels.
    ///
    /// If a vertex or edge has a label with a defined schema, the schema
    /// is enforced. Labels without schemas are allowed without validation.
    /// This is the recommended mode for most production use cases.
    Strict,

    /// Require all labels to have schemas defined.
    ///
    /// All vertices and edges must have labels that correspond to defined
    /// schemas. Creating an element with an unknown label is an error.
    /// This provides the strictest type safety.
    Closed,
}

impl std::fmt::Display for ValidationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationMode::None => write!(f, "NONE"),
            ValidationMode::Warn => write!(f, "WARN"),
            ValidationMode::Strict => write!(f, "STRICT"),
            ValidationMode::Closed => write!(f, "CLOSED"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_schema_new() {
        let schema = GraphSchema::new();
        assert_eq!(schema.mode, ValidationMode::None);
        assert!(schema.is_empty());
        assert_eq!(schema.type_count(), 0);
    }

    #[test]
    fn graph_schema_with_mode() {
        let schema = GraphSchema::with_mode(ValidationMode::Closed);
        assert_eq!(schema.mode, ValidationMode::Closed);
    }

    #[test]
    fn graph_schema_vertex_labels() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .done()
            .vertex("Company")
            .done()
            .build();

        let labels: Vec<_> = schema.vertex_labels().collect();
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"Person"));
        assert!(labels.contains(&"Company"));
    }

    #[test]
    fn graph_schema_edge_labels() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .done()
            .edge("KNOWS")
            .from(&["Person"])
            .to(&["Person"])
            .done()
            .edge("FOLLOWS")
            .from(&["Person"])
            .to(&["Person"])
            .done()
            .build();

        let labels: Vec<_> = schema.edge_labels().collect();
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"KNOWS"));
        assert!(labels.contains(&"FOLLOWS"));
    }

    #[test]
    fn graph_schema_edges_from_to() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .done()
            .vertex("Company")
            .done()
            .edge("WORKS_AT")
            .from(&["Person"])
            .to(&["Company"])
            .done()
            .edge("KNOWS")
            .from(&["Person"])
            .to(&["Person"])
            .done()
            .edge("OWNS")
            .from(&["Person", "Company"])
            .to(&["Company"])
            .done()
            .build();

        let from_person = schema.edges_from("Person");
        assert!(from_person.contains(&"WORKS_AT"));
        assert!(from_person.contains(&"KNOWS"));
        assert!(from_person.contains(&"OWNS"));

        let from_company = schema.edges_from("Company");
        assert!(from_company.contains(&"OWNS"));
        assert!(!from_company.contains(&"WORKS_AT"));

        let to_company = schema.edges_to("Company");
        assert!(to_company.contains(&"WORKS_AT"));
        assert!(to_company.contains(&"OWNS"));
        assert!(!to_company.contains(&"KNOWS"));
    }

    #[test]
    fn graph_schema_type_count() {
        let schema = SchemaBuilder::new()
            .vertex("Person")
            .done()
            .vertex("Company")
            .done()
            .edge("WORKS_AT")
            .from(&["Person"])
            .to(&["Company"])
            .done()
            .build();

        assert!(!schema.is_empty());
        assert_eq!(schema.type_count(), 3);
        assert_eq!(schema.vertex_schemas.len(), 2);
        assert_eq!(schema.edge_schemas.len(), 1);
    }

    #[test]
    fn validation_mode_display() {
        assert_eq!(format!("{}", ValidationMode::None), "NONE");
        assert_eq!(format!("{}", ValidationMode::Warn), "WARN");
        assert_eq!(format!("{}", ValidationMode::Strict), "STRICT");
        assert_eq!(format!("{}", ValidationMode::Closed), "CLOSED");
    }

    #[test]
    fn validation_mode_default() {
        let mode: ValidationMode = Default::default();
        assert_eq!(mode, ValidationMode::None);
    }

    #[test]
    fn validation_mode_equality() {
        assert_eq!(ValidationMode::Strict, ValidationMode::Strict);
        assert_ne!(ValidationMode::Strict, ValidationMode::Closed);
    }
}
