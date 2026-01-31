//! GraphSON 3.0 type definitions.
//!
//! This module contains Rust types representing GraphSON 3.0 structures.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// A GraphSON typed value wrapper.
///
/// Represents the `{"@type": "...", "@value": ...}` pattern used in GraphSON 3.0
/// for explicit type annotations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GraphSONValue {
    /// Typed value with explicit type tag
    Typed {
        /// The GraphSON type tag (e.g., "g:Int64", "g:Double")
        #[serde(rename = "@type")]
        type_tag: String,
        /// The wrapped value
        #[serde(rename = "@value")]
        value: Box<JsonValue>,
    },
    /// Untyped value (strings, booleans, null)
    Untyped(JsonValue),
}

impl GraphSONValue {
    /// Create a typed Int64 value.
    pub fn int64(n: i64) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:Int64".to_string(),
            value: Box::new(JsonValue::Number(n.into())),
        }
    }

    /// Create a typed Int32 value.
    pub fn int32(n: i32) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:Int32".to_string(),
            value: Box::new(JsonValue::Number(n.into())),
        }
    }

    /// Create a typed Double value.
    pub fn double(f: f64) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:Double".to_string(),
            value: Box::new(
                serde_json::Number::from_f64(f)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null),
            ),
        }
    }

    /// Create a typed Float value.
    pub fn float(f: f32) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:Float".to_string(),
            value: Box::new(
                serde_json::Number::from_f64(f as f64)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null),
            ),
        }
    }

    /// Create a typed List value.
    pub fn list(items: Vec<GraphSONValue>) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:List".to_string(),
            value: Box::new(JsonValue::Array(
                items
                    .into_iter()
                    .map(|v| serde_json::to_value(v).unwrap_or(JsonValue::Null))
                    .collect(),
            )),
        }
    }

    /// Create a typed Set value.
    pub fn set(items: Vec<GraphSONValue>) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:Set".to_string(),
            value: Box::new(JsonValue::Array(
                items
                    .into_iter()
                    .map(|v| serde_json::to_value(v).unwrap_or(JsonValue::Null))
                    .collect(),
            )),
        }
    }

    /// Create a typed Map value (flattened key-value pairs).
    ///
    /// GraphSON 3.0 represents maps as `[k1, v1, k2, v2, ...]`.
    pub fn map(pairs: Vec<(GraphSONValue, GraphSONValue)>) -> Self {
        let flattened: Vec<JsonValue> = pairs
            .into_iter()
            .flat_map(|(k, v)| {
                vec![
                    serde_json::to_value(k).unwrap_or(JsonValue::Null),
                    serde_json::to_value(v).unwrap_or(JsonValue::Null),
                ]
            })
            .collect();

        GraphSONValue::Typed {
            type_tag: "g:Map".to_string(),
            value: Box::new(JsonValue::Array(flattened)),
        }
    }

    /// Create an untyped string value.
    pub fn string(s: impl Into<String>) -> Self {
        GraphSONValue::Untyped(JsonValue::String(s.into()))
    }

    /// Create an untyped boolean value.
    pub fn boolean(b: bool) -> Self {
        GraphSONValue::Untyped(JsonValue::Bool(b))
    }

    /// Create an untyped null value.
    pub fn null() -> Self {
        GraphSONValue::Untyped(JsonValue::Null)
    }

    /// Create a typed UUID value.
    pub fn uuid(s: impl Into<String>) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:UUID".to_string(),
            value: Box::new(JsonValue::String(s.into())),
        }
    }

    /// Create a typed Date value (milliseconds since epoch).
    pub fn date(ms: i64) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:Date".to_string(),
            value: Box::new(JsonValue::Number(ms.into())),
        }
    }

    /// Get the type tag if this is a typed value.
    pub fn type_tag(&self) -> Option<&str> {
        match self {
            GraphSONValue::Typed { type_tag, .. } => Some(type_tag),
            GraphSONValue::Untyped(_) => None,
        }
    }

    /// Get the inner JSON value.
    pub fn inner_value(&self) -> &JsonValue {
        match self {
            GraphSONValue::Typed { value, .. } => value,
            GraphSONValue::Untyped(v) => v,
        }
    }
}

/// A GraphSON vertex property.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSONVertexProperty {
    /// Property ID (unique per property instance)
    pub id: GraphSONValue,
    /// Property label (same as key)
    pub label: String,
    /// Property value
    pub value: GraphSONValue,
    /// Meta-properties (optional)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, GraphSONValue>,
}

/// A GraphSON vertex structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSONVertex {
    /// Vertex ID (g:Int64)
    pub id: GraphSONValue,
    /// Vertex label
    pub label: String,
    /// Properties map: property_key -> list of VertexProperty
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, Vec<TypedVertexProperty>>,
}

/// Wrapper for serializing a vertex property with type annotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedVertexProperty {
    /// Always "g:VertexProperty"
    #[serde(rename = "@type")]
    pub type_tag: String,
    /// The property value
    #[serde(rename = "@value")]
    pub value: GraphSONVertexProperty,
}

impl TypedVertexProperty {
    /// Create a new typed vertex property.
    pub fn new(prop: GraphSONVertexProperty) -> Self {
        TypedVertexProperty {
            type_tag: "g:VertexProperty".to_string(),
            value: prop,
        }
    }
}

/// A GraphSON edge property.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSONProperty {
    /// Property key
    pub key: String,
    /// Property value
    pub value: GraphSONValue,
}

/// Wrapper for serializing an edge property with type annotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedProperty {
    /// Always "g:Property"
    #[serde(rename = "@type")]
    pub type_tag: String,
    /// The property value
    #[serde(rename = "@value")]
    pub value: GraphSONProperty,
}

impl TypedProperty {
    /// Create a new typed property.
    pub fn new(prop: GraphSONProperty) -> Self {
        TypedProperty {
            type_tag: "g:Property".to_string(),
            value: prop,
        }
    }
}

/// A GraphSON edge structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSONEdge {
    /// Edge ID
    pub id: GraphSONValue,
    /// Edge label
    pub label: String,
    /// Source vertex ID
    #[serde(rename = "outV")]
    pub out_v: GraphSONValue,
    /// Source vertex label
    #[serde(rename = "outVLabel")]
    pub out_v_label: String,
    /// Target vertex ID
    #[serde(rename = "inV")]
    pub in_v: GraphSONValue,
    /// Target vertex label
    #[serde(rename = "inVLabel")]
    pub in_v_label: String,
    /// Properties map
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, TypedProperty>,
}

/// A complete GraphSON graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSONGraph {
    /// All vertices
    #[serde(default)]
    pub vertices: Vec<TypedVertex>,
    /// All edges
    #[serde(default)]
    pub edges: Vec<TypedEdge>,
}

/// Wrapper for serializing a vertex with type annotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedVertex {
    /// Always "g:Vertex"
    #[serde(rename = "@type")]
    pub type_tag: String,
    /// The vertex value
    #[serde(rename = "@value")]
    pub value: GraphSONVertex,
}

impl TypedVertex {
    /// Create a new typed vertex.
    pub fn new(vertex: GraphSONVertex) -> Self {
        TypedVertex {
            type_tag: "g:Vertex".to_string(),
            value: vertex,
        }
    }
}

/// Wrapper for serializing an edge with type annotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedEdge {
    /// Always "g:Edge"
    #[serde(rename = "@type")]
    pub type_tag: String,
    /// The edge value
    #[serde(rename = "@value")]
    pub value: GraphSONEdge,
}

impl TypedEdge {
    /// Create a new typed edge.
    pub fn new(edge: GraphSONEdge) -> Self {
        TypedEdge {
            type_tag: "g:Edge".to_string(),
            value: edge,
        }
    }
}

/// Wrapper for a complete graph with type annotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedGraph {
    /// "tinker:graph"
    #[serde(rename = "@type")]
    pub type_tag: String,
    /// The graph value
    #[serde(rename = "@value")]
    pub value: GraphSONGraph,
}

impl TypedGraph {
    /// Create a new typed graph.
    pub fn new(graph: GraphSONGraph) -> Self {
        TypedGraph {
            type_tag: "tinker:graph".to_string(),
            value: graph,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int64_creation() {
        let v = GraphSONValue::int64(42);
        assert_eq!(v.type_tag(), Some("g:Int64"));
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, r#"{"@type":"g:Int64","@value":42}"#);
    }

    #[test]
    fn test_double_creation() {
        let v = GraphSONValue::double(3.14);
        assert_eq!(v.type_tag(), Some("g:Double"));
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, r#"{"@type":"g:Double","@value":3.14}"#);
    }

    #[test]
    fn test_string_creation() {
        let v = GraphSONValue::string("hello");
        assert_eq!(v.type_tag(), None);
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, r#""hello""#);
    }

    #[test]
    fn test_boolean_creation() {
        let v = GraphSONValue::boolean(true);
        assert_eq!(v.type_tag(), None);
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, "true");
    }

    #[test]
    fn test_null_creation() {
        let v = GraphSONValue::null();
        assert_eq!(v.type_tag(), None);
        let json = serde_json::to_string(&v).unwrap();
        assert_eq!(json, "null");
    }

    #[test]
    fn test_list_creation() {
        let v = GraphSONValue::list(vec![
            GraphSONValue::int64(1),
            GraphSONValue::int64(2),
            GraphSONValue::int64(3),
        ]);
        assert_eq!(v.type_tag(), Some("g:List"));
        let json = serde_json::to_string(&v).unwrap();
        assert!(json.contains("g:List"));
    }

    #[test]
    fn test_map_creation() {
        let v = GraphSONValue::map(vec![
            (GraphSONValue::string("key1"), GraphSONValue::int64(1)),
            (GraphSONValue::string("key2"), GraphSONValue::int64(2)),
        ]);
        assert_eq!(v.type_tag(), Some("g:Map"));
        let json = serde_json::to_string(&v).unwrap();
        assert!(json.contains("g:Map"));
    }

    #[test]
    fn test_typed_vertex_serialization() {
        let vertex = TypedVertex::new(GraphSONVertex {
            id: GraphSONValue::int64(1),
            label: "person".to_string(),
            properties: HashMap::new(),
        });
        let json = serde_json::to_string(&vertex).unwrap();
        assert!(json.contains("g:Vertex"));
        assert!(json.contains("person"));
    }

    #[test]
    fn test_typed_edge_serialization() {
        let edge = TypedEdge::new(GraphSONEdge {
            id: GraphSONValue::int64(100),
            label: "knows".to_string(),
            out_v: GraphSONValue::int64(1),
            out_v_label: "person".to_string(),
            in_v: GraphSONValue::int64(2),
            in_v_label: "person".to_string(),
            properties: HashMap::new(),
        });
        let json = serde_json::to_string(&edge).unwrap();
        assert!(json.contains("g:Edge"));
        assert!(json.contains("knows"));
        assert!(json.contains("outV"));
        assert!(json.contains("inV"));
    }

    #[test]
    fn test_typed_graph_serialization() {
        let graph = TypedGraph::new(GraphSONGraph {
            vertices: vec![],
            edges: vec![],
        });
        let json = serde_json::to_string(&graph).unwrap();
        assert!(json.contains("tinker:graph"));
    }

    #[test]
    fn test_deserialize_typed_value() {
        let json = r#"{"@type":"g:Int64","@value":42}"#;
        let v: GraphSONValue = serde_json::from_str(json).unwrap();
        assert_eq!(v.type_tag(), Some("g:Int64"));
    }

    #[test]
    fn test_deserialize_untyped_value() {
        let json = r#""hello""#;
        let v: GraphSONValue = serde_json::from_str(json).unwrap();
        assert_eq!(v.type_tag(), None);
        assert_eq!(v.inner_value(), &JsonValue::String("hello".to_string()));
    }
}
