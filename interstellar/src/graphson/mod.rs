//! GraphSON 3.0 import/export support.
//!
//! This module provides functions to serialize and deserialize graphs
//! in the GraphSON 3.0 format used by Apache TinkerPop.
//!
//! GraphSON is the JSON-based format used by TinkerPop-compatible graph databases
//! like Neo4j, JanusGraph, and Amazon Neptune. Supporting GraphSON enables:
//!
//! - **Interoperability** - Import/export with other graph databases
//! - **Human readability** - JSON format is inspectable and debuggable
//! - **Tooling ecosystem** - Leverage existing visualization and analysis tools
//! - **Migration paths** - Easy data migration from/to other graph databases
//!
//! # Quick Start
//!
//! ## Export a graph
//!
//! ```rust
//! use interstellar::graphson;
//! use interstellar::storage::Graph;
//! use interstellar::value::Value;
//! use std::collections::HashMap;
//!
//! let graph = Graph::new();
//! let alice = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), Value::String("Alice".to_string())),
//!     ("age".to_string(), Value::Int(30)),
//! ]));
//!
//! let snapshot = graph.snapshot();
//! let json = graphson::to_string(&snapshot).unwrap();
//! println!("{}", json);
//! ```
//!
//! ## Import a graph
//!
//! ```rust
//! use interstellar::graphson;
//!
//! let json = r#"{"@type": "tinker:graph", "@value": {"vertices": [], "edges": []}}"#;
//! let graph = graphson::from_str(json).unwrap();
//! ```
//!
//! ## Export with schema
//!
//! ```rust
//! use interstellar::graphson;
//! use interstellar::storage::Graph;
//! use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
//!
//! let graph = Graph::new();
//! let snapshot = graph.snapshot();
//!
//! let schema = SchemaBuilder::new()
//!     .mode(ValidationMode::Strict)
//!     .vertex("person")
//!         .property("name", PropertyType::String)
//!         .done()
//!     .build();
//!
//! let json = graphson::to_string_with_schema(&snapshot, &schema).unwrap();
//! ```
//!
//! # GraphSON 3.0 Format
//!
//! GraphSON 3.0 uses explicit type wrappers for all non-string values:
//!
//! ```json
//! {"@type": "g:Int64", "@value": 42}
//! {"@type": "g:Double", "@value": 3.14}
//! {"@type": "g:List", "@value": [1, 2, 3]}
//! ```
//!
//! Strings, booleans, and null are untyped:
//!
//! ```json
//! "hello"
//! true
//! null
//! ```
//!
//! # Type Mappings
//!
//! | Interstellar Type | GraphSON Type |
//! |-------------------|---------------|
//! | `Value::Null` | JSON `null` |
//! | `Value::Bool` | JSON boolean |
//! | `Value::Int` | `g:Int64` |
//! | `Value::Float` | `g:Double` |
//! | `Value::String` | JSON string |
//! | `Value::List` | `g:List` |
//! | `Value::Map` | `g:Map` |

mod deserialize;
mod error;
mod schema_ext;
mod serialize;
mod types;

// Re-export error types
pub use error::{GraphSONError, Result};

// Re-export serialization functions
pub use serialize::{
    to_json_string, to_json_string_pretty, to_writer, to_writer_pretty, SerializeOptions,
};

// Re-export deserialization functions
pub use deserialize::{from_json_str, from_reader, DeserializeOptions, IdMode};

// Re-export schema extension
pub use schema_ext::{
    graphson_to_schema, schema_to_graphson, string_to_property_type, GraphSONSchema,
};

// Re-export types for advanced usage
pub use types::{
    GraphSONEdge, GraphSONGraph, GraphSONProperty, GraphSONValue, GraphSONVertex,
    GraphSONVertexProperty, TypedEdge, TypedGraph, TypedProperty, TypedVertex, TypedVertexProperty,
};

use crate::schema::GraphSchema;
use crate::storage::GraphStorage;
#[cfg(not(target_arch = "wasm32"))]
use std::fs::File;
#[cfg(not(target_arch = "wasm32"))]
use std::io::{BufReader, BufWriter};
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

/// Serialize a graph to a JSON string.
///
/// This is the simplest way to export a graph. For pretty-printed output,
/// use [`to_string_pretty`].
///
/// # Example
///
/// ```rust
/// use interstellar::graphson;
/// use interstellar::storage::Graph;
///
/// let graph = Graph::new();
/// let snapshot = graph.snapshot();
/// let json = graphson::to_string(&snapshot).unwrap();
/// ```
pub fn to_string<S: GraphStorage>(storage: &S) -> std::result::Result<String, serde_json::Error> {
    to_json_string(storage)
}

/// Serialize a graph to a pretty-printed JSON string.
///
/// # Example
///
/// ```rust
/// use interstellar::graphson;
/// use interstellar::storage::Graph;
///
/// let graph = Graph::new();
/// let snapshot = graph.snapshot();
/// let json = graphson::to_string_pretty(&snapshot).unwrap();
/// assert!(json.contains('\n')); // Pretty-printed
/// ```
pub fn to_string_pretty<S: GraphStorage>(
    storage: &S,
) -> std::result::Result<String, serde_json::Error> {
    to_json_string_pretty(storage)
}

/// Deserialize a graph from a JSON string.
///
/// # Example
///
/// ```rust
/// use interstellar::graphson;
///
/// let json = r#"{"@type": "tinker:graph", "@value": {"vertices": [], "edges": []}}"#;
/// let graph = graphson::from_str(json).unwrap();
/// ```
pub fn from_str(json: &str) -> Result<crate::storage::Graph> {
    from_json_str(json)
}

/// Export a graph with schema metadata.
///
/// The output includes both the graph data and schema in the
/// `interstellar:GraphWithSchema` format.
///
/// # Example
///
/// ```rust
/// use interstellar::graphson;
/// use interstellar::storage::Graph;
/// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
///
/// let graph = Graph::new();
/// let snapshot = graph.snapshot();
///
/// let schema = SchemaBuilder::new()
///     .mode(ValidationMode::Strict)
///     .vertex("person")
///         .property("name", PropertyType::String)
///         .done()
///     .build();
///
/// let json = graphson::to_string_with_schema(&snapshot, &schema).unwrap();
/// assert!(json.contains("interstellar:Schema"));
/// ```
pub fn to_string_with_schema<S: GraphStorage>(
    storage: &S,
    schema: &GraphSchema,
) -> std::result::Result<String, serde_json::Error> {
    let graph = serialize::serialize_graph(storage);
    let schema_json = schema_to_graphson(schema);

    let output = serde_json::json!({
        "@type": "interstellar:GraphWithSchema",
        "@value": {
            "schema": schema_json,
            "graph": graph
        }
    });

    serde_json::to_string_pretty(&output)
}

/// Export a graph to a file.
///
/// This function is not available on WASM targets.
/// Use [`to_string`] or [`to_string_pretty`] instead.
///
/// # Example
///
/// ```no_run
/// use interstellar::graphson;
/// use interstellar::storage::Graph;
///
/// let graph = Graph::new();
/// let snapshot = graph.snapshot();
/// graphson::export_to_file(&snapshot, "graph.json").unwrap();
/// ```
#[cfg(not(target_arch = "wasm32"))]
pub fn export_to_file<S: GraphStorage, P: AsRef<Path>>(storage: &S, path: P) -> Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    to_writer_pretty(storage, writer)?;
    Ok(())
}

/// Import a graph from a file.
///
/// This function is not available on WASM targets.
/// Use [`from_str`] instead.
///
/// # Example
///
/// ```no_run
/// use interstellar::graphson;
///
/// let graph = graphson::import_from_file("graph.json").unwrap();
/// ```
#[cfg(not(target_arch = "wasm32"))]
pub fn import_from_file<P: AsRef<Path>>(path: P) -> Result<crate::storage::Graph> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    from_reader(reader)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{PropertyType, SchemaBuilder, ValidationMode};
    use crate::storage::Graph;
    use crate::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_empty_graph_roundtrip() {
        let graph = Graph::new();
        let snapshot = graph.snapshot();
        let json = to_string(&snapshot).unwrap();
        let imported = from_str(&json).unwrap();
        assert_eq!(imported.snapshot().vertex_count(), 0);
        assert_eq!(imported.snapshot().edge_count(), 0);
    }

    #[test]
    fn test_simple_graph_roundtrip() {
        let graph = Graph::new();
        let v1 = graph.add_vertex(
            "person",
            HashMap::from([("name".into(), Value::String("Alice".into()))]),
        );
        let v2 = graph.add_vertex(
            "person",
            HashMap::from([("name".into(), Value::String("Bob".into()))]),
        );
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        let snapshot = graph.snapshot();
        let json = to_string(&snapshot).unwrap();
        let imported = from_str(&json).unwrap();

        let imported_snapshot = imported.snapshot();
        assert_eq!(imported_snapshot.vertex_count(), 2);
        assert_eq!(imported_snapshot.edge_count(), 1);
    }

    #[test]
    fn test_value_roundtrip() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(-42),
            Value::Int(0),
            Value::Int(i64::MAX),
            Value::Float(3.14159),
            Value::String("test".into()),
            Value::String("".into()),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        ];

        for original in values {
            let graph = Graph::new();
            graph.add_vertex("test", HashMap::from([("value".into(), original.clone())]));

            let snapshot = graph.snapshot();
            let json = to_string(&snapshot).unwrap();
            let imported = from_str(&json).unwrap();
            let imported_snapshot = imported.snapshot();

            let vertices: Vec<_> = imported_snapshot.all_vertices().collect();
            assert_eq!(vertices.len(), 1);
            assert_eq!(vertices[0].properties.get("value"), Some(&original));
        }
    }

    #[test]
    fn test_to_string_with_schema() {
        let graph = Graph::new();
        graph.add_vertex(
            "person",
            HashMap::from([("name".into(), Value::String("Alice".into()))]),
        );

        let snapshot = graph.snapshot();
        let schema = SchemaBuilder::new()
            .mode(ValidationMode::Strict)
            .vertex("person")
            .property("name", PropertyType::String)
            .done()
            .build();

        let json = to_string_with_schema(&snapshot, &schema).unwrap();

        assert!(json.contains("interstellar:GraphWithSchema"));
        assert!(json.contains("interstellar:Schema"));
        assert!(json.contains("person"));
        assert!(json.contains("strict"));
    }

    #[test]
    fn test_pretty_print_has_newlines() {
        let graph = Graph::new();
        graph.add_vertex("test", HashMap::new());

        let snapshot = graph.snapshot();
        let json = to_string_pretty(&snapshot).unwrap();

        assert!(json.contains('\n'));
    }

    #[test]
    fn test_invalid_json_error() {
        let result = from_str("not valid json");
        assert!(matches!(result, Err(GraphSONError::JsonParse(_))));
    }

    #[test]
    fn test_missing_vertex_error() {
        let json = r#"{
            "@type": "tinker:graph",
            "@value": {
                "vertices": [],
                "edges": [{
                    "@type": "g:Edge",
                    "@value": {
                        "id": {"@type": "g:Int64", "@value": 1},
                        "label": "knows",
                        "outV": {"@type": "g:Int64", "@value": 999},
                        "outVLabel": "person",
                        "inV": {"@type": "g:Int64", "@value": 1000},
                        "inVLabel": "person",
                        "properties": {}
                    }
                }]
            }
        }"#;
        let result = from_str(json);
        assert!(matches!(result, Err(GraphSONError::VertexNotFound(999))));
    }

    #[test]
    fn test_edge_properties_roundtrip() {
        let graph = Graph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        graph
            .add_edge(
                v1,
                v2,
                "knows",
                HashMap::from([
                    ("since".into(), Value::Int(2020)),
                    ("weight".into(), Value::Float(0.95)),
                ]),
            )
            .unwrap();

        let snapshot = graph.snapshot();
        let json = to_string(&snapshot).unwrap();
        let imported = from_str(&json).unwrap();
        let imported_snapshot = imported.snapshot();

        let edges: Vec<_> = imported_snapshot.all_edges().collect();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].properties.get("since"), Some(&Value::Int(2020)));
        assert!(edges[0].properties.contains_key("weight"));
    }

    #[test]
    fn test_multiple_edges_roundtrip() {
        let graph = Graph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());

        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        graph.add_edge(v2, v3, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v3, "likes", HashMap::new()).unwrap();

        let snapshot = graph.snapshot();
        let json = to_string(&snapshot).unwrap();
        let imported = from_str(&json).unwrap();
        let imported_snapshot = imported.snapshot();

        assert_eq!(imported_snapshot.vertex_count(), 3);
        assert_eq!(imported_snapshot.edge_count(), 3);
    }

    #[test]
    fn test_complex_properties_roundtrip() {
        let mut map = HashMap::new();
        map.insert("nested".to_string(), Value::Int(42));

        let graph = Graph::new();
        graph.add_vertex(
            "test",
            HashMap::from([
                (
                    "list".into(),
                    Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                ),
                ("map".into(), Value::Map(map)),
            ]),
        );

        let snapshot = graph.snapshot();
        let json = to_string(&snapshot).unwrap();
        let imported = from_str(&json).unwrap();
        let imported_snapshot = imported.snapshot();

        let vertices: Vec<_> = imported_snapshot.all_vertices().collect();
        assert_eq!(vertices.len(), 1);

        match vertices[0].properties.get("list") {
            Some(Value::List(items)) => {
                assert_eq!(items.len(), 3);
            }
            _ => panic!("Expected list property"),
        }

        match vertices[0].properties.get("map") {
            Some(Value::Map(m)) => {
                assert_eq!(m.get("nested"), Some(&Value::Int(42)));
            }
            _ => panic!("Expected map property"),
        }
    }
}
