//! Query storage and execution types.
//!
//! This module provides types for storing, retrieving, and executing named queries
//! in persistent graph storage. Queries support typed parameters with `$param` syntax.
//!
//! # Overview
//!
//! The query library allows you to:
//! - Save named queries with descriptions
//! - Validate query syntax on save (Gremlin or GQL)
//! - Extract and type parameters automatically
//! - Execute queries with parameter bindings
//!
//! # Example
//!
//! ```rust,ignore
//! use interstellar::query::{QueryType, SavedQuery, QueryParameter, ParameterType};
//! use interstellar::Value;
//! use std::collections::HashMap;
//!
//! // Define a query with parameters
//! let query = SavedQuery {
//!     id: 1,
//!     name: "find_person".to_string(),
//!     query_type: QueryType::Gremlin,
//!     description: "Find a person by name".to_string(),
//!     query: "g.V().has('person', 'name', $name)".to_string(),
//!     parameters: vec![
//!         QueryParameter {
//!             name: "name".to_string(),
//!             param_type: ParameterType::String,
//!         },
//!     ],
//! };
//!
//! // Execute with parameter bindings
//! let params: HashMap<String, Value> = HashMap::from([
//!     ("name".to_string(), Value::String("Alice".to_string())),
//! ]);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::value::Value;

// =============================================================================
// QueryType
// =============================================================================

/// Query language type.
///
/// Specifies whether a query is written in Gremlin or GQL syntax.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum QueryType {
    /// Gremlin traversal language
    Gremlin = 1,
    /// Graph Query Language (GQL/Cypher-like)
    Gql = 2,
}

impl QueryType {
    /// Convert from u16 flags value (as stored in QueryRecord)
    pub fn from_flags(flags: u16) -> Option<Self> {
        match flags {
            1 => Some(QueryType::Gremlin),
            2 => Some(QueryType::Gql),
            _ => None,
        }
    }

    /// Convert to u16 for storage in QueryRecord flags
    pub fn to_flags(self) -> u16 {
        self as u16
    }
}

impl std::fmt::Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryType::Gremlin => write!(f, "Gremlin"),
            QueryType::Gql => write!(f, "GQL"),
        }
    }
}

// =============================================================================
// ParameterType
// =============================================================================

/// Expected parameter type.
///
/// Specifies the expected type for a query parameter. This is used for
/// validation and documentation purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ParameterType {
    /// Type not constrained (any Value)
    Any = 0xFF,
    /// Expects null value
    Null = 0x00,
    /// Expects a boolean
    Boolean = 0x01,
    /// Expects an integer (i64)
    Integer = 0x02,
    /// Expects a float (f64)
    Float = 0x03,
    /// Expects a string value
    String = 0x04,
    /// Expects a list
    List = 0x05,
    /// Expects a map
    Map = 0x06,
    /// Expects a vertex ID
    VertexId = 0x07,
    /// Expects an edge ID
    EdgeId = 0x08,
}

impl ParameterType {
    /// Convert from u8 discriminant (as stored in ParameterEntry)
    pub fn from_discriminant(d: u8) -> Self {
        match d {
            0x00 => ParameterType::Null,
            0x01 => ParameterType::Boolean,
            0x02 => ParameterType::Integer,
            0x03 => ParameterType::Float,
            0x04 => ParameterType::String,
            0x05 => ParameterType::List,
            0x06 => ParameterType::Map,
            0x07 => ParameterType::VertexId,
            0x08 => ParameterType::EdgeId,
            _ => ParameterType::Any,
        }
    }

    /// Convert to u8 for storage in ParameterEntry
    pub fn to_discriminant(self) -> u8 {
        self as u8
    }

    /// Check if a Value matches this parameter type
    pub fn matches(&self, value: &Value) -> bool {
        match self {
            ParameterType::Any => true,
            ParameterType::Null => matches!(value, Value::Null),
            ParameterType::Boolean => matches!(value, Value::Bool(_)),
            ParameterType::Integer => matches!(value, Value::Int(_)),
            ParameterType::Float => matches!(value, Value::Float(_)),
            ParameterType::String => matches!(value, Value::String(_)),
            ParameterType::List => matches!(value, Value::List(_)),
            ParameterType::Map => matches!(value, Value::Map(_)),
            ParameterType::VertexId => matches!(value, Value::Vertex(_)),
            ParameterType::EdgeId => matches!(value, Value::Edge(_)),
        }
    }

    /// Get the type name for error messages
    pub fn type_name(&self) -> &'static str {
        match self {
            ParameterType::Any => "any",
            ParameterType::Null => "null",
            ParameterType::Boolean => "boolean",
            ParameterType::Integer => "integer",
            ParameterType::Float => "float",
            ParameterType::String => "string",
            ParameterType::List => "list",
            ParameterType::Map => "map",
            ParameterType::VertexId => "vertex_id",
            ParameterType::EdgeId => "edge_id",
        }
    }
}

impl std::fmt::Display for ParameterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.type_name())
    }
}

// =============================================================================
// QueryParameter
// =============================================================================

/// A query parameter definition.
///
/// Represents a parameter extracted from a query, including its name
/// and expected type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryParameter {
    /// Parameter name (without $ prefix)
    pub name: String,
    /// Expected value type
    pub param_type: ParameterType,
}

impl QueryParameter {
    /// Create a new query parameter
    pub fn new(name: impl Into<String>, param_type: ParameterType) -> Self {
        Self {
            name: name.into(),
            param_type,
        }
    }

    /// Create a new query parameter with Any type
    pub fn any(name: impl Into<String>) -> Self {
        Self::new(name, ParameterType::Any)
    }
}

impl std::fmt::Display for QueryParameter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}: {}", self.name, self.param_type)
    }
}

// =============================================================================
// SavedQuery
// =============================================================================

/// A saved query entry.
///
/// Represents a complete query stored in the query library, including
/// metadata, the query text, and extracted parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQuery {
    /// Unique query ID
    pub id: u32,
    /// Query name (unique across all queries)
    pub name: String,
    /// Query language type
    pub query_type: QueryType,
    /// Human-readable description
    pub description: String,
    /// Query text (may contain $param placeholders)
    pub query: String,
    /// Declared/inferred parameters
    pub parameters: Vec<QueryParameter>,
}

impl SavedQuery {
    /// Create a new saved query
    pub fn new(
        id: u32,
        name: impl Into<String>,
        query_type: QueryType,
        description: impl Into<String>,
        query: impl Into<String>,
        parameters: Vec<QueryParameter>,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            query_type,
            description: description.into(),
            query: query.into(),
            parameters,
        }
    }

    /// Get the parameter names as a set
    pub fn parameter_names(&self) -> std::collections::HashSet<&str> {
        self.parameters.iter().map(|p| p.name.as_str()).collect()
    }

    /// Find a parameter by name
    pub fn get_parameter(&self, name: &str) -> Option<&QueryParameter> {
        self.parameters.iter().find(|p| p.name == name)
    }

    /// Check if this query has any parameters
    pub fn has_parameters(&self) -> bool {
        !self.parameters.is_empty()
    }
}

impl std::fmt::Display for SavedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} ({}): {}",
            self.id, self.name, self.query_type, self.description
        )
    }
}

// =============================================================================
// QueryParams
// =============================================================================

/// Parameter bindings for query execution.
///
/// Maps parameter names to their values for substitution into query text.
pub type QueryParams = HashMap<String, Value>;

// =============================================================================
// Query Name Validation
// =============================================================================

/// Maximum length for query names
pub const MAX_QUERY_NAME_LENGTH: usize = 64;

/// Validate a query name.
///
/// Valid names:
/// - Must start with letter or underscore
/// - May contain letters, digits, underscores, hyphens
/// - Maximum 64 characters
/// - Case-sensitive
///
/// Returns `Ok(())` if valid, or an error message if invalid.
pub fn validate_query_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("query name cannot be empty".to_string());
    }

    if name.len() > MAX_QUERY_NAME_LENGTH {
        return Err(format!(
            "query name too long: {} characters (max {})",
            name.len(),
            MAX_QUERY_NAME_LENGTH
        ));
    }

    let mut chars = name.chars();

    // First character must be letter or underscore
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        Some(c) => {
            return Err(format!(
                "query name must start with letter or underscore, got '{}'",
                c
            ))
        }
        None => return Err("query name cannot be empty".to_string()),
    }

    // Remaining characters: letters, digits, underscores, hyphens
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' && c != '-' {
            return Err(format!("invalid character in query name: '{}'", c));
        }
    }

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_type_conversion() {
        assert_eq!(QueryType::from_flags(1), Some(QueryType::Gremlin));
        assert_eq!(QueryType::from_flags(2), Some(QueryType::Gql));
        assert_eq!(QueryType::from_flags(0), None);
        assert_eq!(QueryType::from_flags(3), None);

        assert_eq!(QueryType::Gremlin.to_flags(), 1);
        assert_eq!(QueryType::Gql.to_flags(), 2);
    }

    #[test]
    fn test_query_type_display() {
        assert_eq!(format!("{}", QueryType::Gremlin), "Gremlin");
        assert_eq!(format!("{}", QueryType::Gql), "GQL");
    }

    #[test]
    fn test_parameter_type_conversion() {
        assert_eq!(ParameterType::from_discriminant(0xFF), ParameterType::Any);
        assert_eq!(ParameterType::from_discriminant(0x00), ParameterType::Null);
        assert_eq!(
            ParameterType::from_discriminant(0x02),
            ParameterType::Integer
        );
        assert_eq!(
            ParameterType::from_discriminant(0x04),
            ParameterType::String
        );

        assert_eq!(ParameterType::Any.to_discriminant(), 0xFF);
        assert_eq!(ParameterType::Integer.to_discriminant(), 0x02);
        assert_eq!(ParameterType::String.to_discriminant(), 0x04);
    }

    #[test]
    fn test_parameter_type_matches() {
        assert!(ParameterType::Any.matches(&Value::Int(42)));
        assert!(ParameterType::Any.matches(&Value::String("test".to_string())));

        assert!(ParameterType::Integer.matches(&Value::Int(42)));
        assert!(!ParameterType::Integer.matches(&Value::String("test".to_string())));

        assert!(ParameterType::String.matches(&Value::String("test".to_string())));
        assert!(!ParameterType::String.matches(&Value::Int(42)));

        assert!(ParameterType::Boolean.matches(&Value::Bool(true)));
        assert!(!ParameterType::Boolean.matches(&Value::Int(1)));
    }

    #[test]
    fn test_parameter_type_display() {
        assert_eq!(format!("{}", ParameterType::Any), "any");
        assert_eq!(format!("{}", ParameterType::String), "string");
        assert_eq!(format!("{}", ParameterType::Integer), "integer");
    }

    #[test]
    fn test_query_parameter() {
        let param = QueryParameter::new("name", ParameterType::String);
        assert_eq!(param.name, "name");
        assert_eq!(param.param_type, ParameterType::String);
        assert_eq!(format!("{}", param), "$name: string");

        let any_param = QueryParameter::any("value");
        assert_eq!(any_param.param_type, ParameterType::Any);
    }

    #[test]
    fn test_saved_query() {
        let query = SavedQuery::new(
            1,
            "test_query",
            QueryType::Gremlin,
            "A test query",
            "g.V().has('name', $name)",
            vec![QueryParameter::new("name", ParameterType::String)],
        );

        assert_eq!(query.id, 1);
        assert_eq!(query.name, "test_query");
        assert_eq!(query.query_type, QueryType::Gremlin);
        assert!(query.has_parameters());
        assert!(query.parameter_names().contains("name"));
        assert!(query.get_parameter("name").is_some());
        assert!(query.get_parameter("unknown").is_none());
    }

    #[test]
    fn test_validate_query_name_valid() {
        assert!(validate_query_name("test").is_ok());
        assert!(validate_query_name("test_query").is_ok());
        assert!(validate_query_name("test-query").is_ok());
        assert!(validate_query_name("_private").is_ok());
        assert!(validate_query_name("query123").is_ok());
        assert!(validate_query_name("Query_123_test").is_ok());
    }

    #[test]
    fn test_validate_query_name_invalid() {
        assert!(validate_query_name("").is_err());
        assert!(validate_query_name("123test").is_err());
        assert!(validate_query_name("-test").is_err());
        assert!(validate_query_name("test query").is_err());
        assert!(validate_query_name("test.query").is_err());
        assert!(validate_query_name("test@query").is_err());

        // Too long
        let long_name = "a".repeat(MAX_QUERY_NAME_LENGTH + 1);
        assert!(validate_query_name(&long_name).is_err());

        // Max length should be ok
        let max_name = "a".repeat(MAX_QUERY_NAME_LENGTH);
        assert!(validate_query_name(&max_name).is_ok());
    }
}
