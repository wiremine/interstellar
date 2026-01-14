# Spec 25: GraphSON 3.0 Import/Export

## 1. Overview

### 1.1 Motivation

Intersteller needs a standard format for graph data interchange. GraphSON is the JSON-based format
used by Apache TinkerPop, the de facto standard for graph databases. Supporting GraphSON enables:

1. **Interoperability** - Import/export with Neo4j, JanusGraph, Amazon Neptune, and other TinkerPop-compatible databases
2. **Human readability** - JSON format is inspectable and debuggable
3. **Tooling ecosystem** - Leverage existing visualization and analysis tools
4. **Migration paths** - Easy data migration from/to other graph databases

### 1.2 Scope

This specification covers:

- GraphSON 3.0 format support (the latest and most complete version)
- Serialization of Intersteller graphs to GraphSON
- Deserialization of GraphSON to Intersteller graphs
- Custom extension for schema metadata (`intersteller:Schema`)
- Public API for import/export operations

Out of scope:

- GraphSON 1.0 and 2.0 (legacy formats with limitations)
- Streaming serialization (future enhancement)
- GraphML, GEXF, or other formats (separate specs)

### 1.3 Design Principles

1. **Lossless roundtrip** - Export then import should preserve all data
2. **Type preservation** - GraphSON's explicit typing prevents data loss
3. **Schema-aware** - Optionally include schema in exports
4. **Incremental adoption** - Works with or without schemas
5. **Error transparency** - Clear error messages for malformed input

## 2. GraphSON 3.0 Format

### 2.1 Type System

GraphSON 3.0 uses explicit type wrappers for all non-string values:

```json
{"@type": "g:Int32", "@value": 42}
{"@type": "g:Int64", "@value": 9223372036854775807}
{"@type": "g:Float", "@value": 3.14}
{"@type": "g:Double", "@value": 2.718281828}
{"@type": "g:List", "@value": [1, 2, 3]}
{"@type": "g:Map", "@value": ["key1", "value1", "key2", "value2"]}
```

Strings and `null` are untyped:
```json
"hello"
null
```

### 2.2 Core TinkerPop Types

| Type Tag | Description | Example |
|----------|-------------|---------|
| `g:Int32` | 32-bit signed integer | `{"@type": "g:Int32", "@value": 42}` |
| `g:Int64` | 64-bit signed integer | `{"@type": "g:Int64", "@value": 9223372036854775807}` |
| `g:Float` | 32-bit float | `{"@type": "g:Float", "@value": 3.14}` |
| `g:Double` | 64-bit double | `{"@type": "g:Double", "@value": 3.14159265359}` |
| `g:List` | Ordered collection | `{"@type": "g:List", "@value": [...]}` |
| `g:Set` | Unique collection | `{"@type": "g:Set", "@value": [...]}` |
| `g:Map` | Key-value pairs | `{"@type": "g:Map", "@value": [k1, v1, k2, v2]}` |
| `g:UUID` | UUID string | `{"@type": "g:UUID", "@value": "..."}` |
| `g:Date` | Milliseconds since epoch | `{"@type": "g:Date", "@value": 1609459200000}` |
| `g:Vertex` | Vertex structure | See 2.3 |
| `g:Edge` | Edge structure | See 2.3 |
| `g:VertexProperty` | Vertex property | See 2.3 |
| `g:Property` | Edge property | See 2.3 |

### 2.3 Graph Structure

**Vertex:**
```json
{
  "@type": "g:Vertex",
  "@value": {
    "id": {"@type": "g:Int64", "@value": 1},
    "label": "person",
    "properties": {
      "name": [
        {
          "@type": "g:VertexProperty",
          "@value": {
            "id": {"@type": "g:Int64", "@value": 100},
            "label": "name",
            "value": "Alice"
          }
        }
      ],
      "age": [
        {
          "@type": "g:VertexProperty",
          "@value": {
            "id": {"@type": "g:Int64", "@value": 101},
            "label": "age",
            "value": {"@type": "g:Int32", "@value": 30}
          }
        }
      ]
    }
  }
}
```

**Edge:**
```json
{
  "@type": "g:Edge",
  "@value": {
    "id": {"@type": "g:Int64", "@value": 1000},
    "label": "knows",
    "inV": {"@type": "g:Int64", "@value": 2},
    "inVLabel": "person",
    "outV": {"@type": "g:Int64", "@value": 1},
    "outVLabel": "person",
    "properties": {
      "since": {
        "@type": "g:Property",
        "@value": {
          "key": "since",
          "value": {"@type": "g:Int32", "@value": 2020}
        }
      }
    }
  }
}
```

### 2.4 Full Graph Format

A complete graph export uses a top-level object:

```json
{
  "@type": "tinker:graph",
  "@value": {
    "vertices": [
      {"@type": "g:Vertex", "@value": {...}},
      {"@type": "g:Vertex", "@value": {...}}
    ],
    "edges": [
      {"@type": "g:Edge", "@value": {...}},
      {"@type": "g:Edge", "@value": {...}}
    ]
  }
}
```

## 3. Type Mappings

### 3.1 Intersteller to GraphSON

| Intersteller Type | GraphSON Type | Notes |
|-------------------|---------------|-------|
| `Value::Null` | JSON `null` | Untyped |
| `Value::Bool` | JSON `true`/`false` | Untyped |
| `Value::Int(i64)` | `g:Int64` | Always 64-bit for consistency |
| `Value::Float(f64)` | `g:Double` | Always 64-bit |
| `Value::String` | JSON string | Untyped |
| `Value::List` | `g:List` | Recursive |
| `Value::Map` | `g:Map` | Keys serialized as strings |
| `Value::Vertex(VertexId)` | `g:Int64` | ID only, used in references |
| `Value::Edge(EdgeId)` | `g:Int64` | ID only, used in references |
| `VertexId` | `g:Int64` | Vertex identifier |
| `EdgeId` | `g:Int64` | Edge identifier |

### 3.2 GraphSON to Intersteller

| GraphSON Type | Intersteller Type | Notes |
|---------------|-------------------|-------|
| JSON `null` | `Value::Null` | |
| JSON `true`/`false` | `Value::Bool` | |
| `g:Int32` | `Value::Int` | Widened to i64 |
| `g:Int64` | `Value::Int` | Direct |
| `g:Float` | `Value::Float` | Widened to f64 |
| `g:Double` | `Value::Float` | Direct |
| JSON string | `Value::String` | |
| `g:List` | `Value::List` | Recursive |
| `g:Set` | `Value::List` | Converted to list |
| `g:Map` | `Value::Map` | Keys must be strings |
| `g:UUID` | `Value::String` | Stored as string |
| `g:Date` | `Value::Int` | Milliseconds since epoch |

### 3.3 Property Type Handling

For schema-aware imports, property types are validated:

| PropertyType | Expected GraphSON |
|--------------|-------------------|
| `PropertyType::Bool` | JSON boolean |
| `PropertyType::Int` | `g:Int32` or `g:Int64` |
| `PropertyType::Float` | `g:Float` or `g:Double` |
| `PropertyType::String` | JSON string |
| `PropertyType::List(T)` | `g:List` with elements of type T |
| `PropertyType::Map(T)` | `g:Map` with values of type T |
| `PropertyType::Any` | Any valid type |

## 4. Module Structure

```
src/graphson/
├── mod.rs              # Public API and re-exports
├── types.rs            # Rust types for GraphSON structures
├── serialize.rs        # Intersteller -> GraphSON
├── deserialize.rs      # GraphSON -> Intersteller
├── error.rs            # Error types
└── schema_ext.rs       # Schema extension (intersteller:Schema)
```

### 4.1 Dependency Changes

Add `serde_json` as a main dependency (currently dev-only):

```toml
[dependencies]
serde_json = "1.0"

# serde is already present with "derive" feature
serde = { version = "1.0", features = ["derive"] }
```

### 4.2 Feature Flag

GraphSON support is enabled by default but can be disabled:

```toml
[features]
default = ["inmemory", "graphson"]
graphson = ["serde_json"]
```

## 5. Core Types

### 5.1 GraphSON Value Wrapper (`types.rs`)

The central type for representing GraphSON's typed values:

```rust
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// A GraphSON typed value wrapper.
/// 
/// Represents the `{"@type": "...", "@value": ...}` pattern.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GraphSONValue {
    /// Typed value with explicit type tag
    Typed {
        #[serde(rename = "@type")]
        type_tag: String,
        #[serde(rename = "@value")]
        value: Box<JsonValue>,
    },
    /// Untyped value (strings, booleans, null)
    Untyped(JsonValue),
}

impl GraphSONValue {
    /// Create a typed Int64 value
    pub fn int64(n: i64) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:Int64".to_string(),
            value: Box::new(JsonValue::Number(n.into())),
        }
    }

    /// Create a typed Double value
    pub fn double(f: f64) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:Double".to_string(),
            value: Box::new(serde_json::Number::from_f64(f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)),
        }
    }

    /// Create a typed List value
    pub fn list(items: Vec<GraphSONValue>) -> Self {
        GraphSONValue::Typed {
            type_tag: "g:List".to_string(),
            value: Box::new(JsonValue::Array(
                items.into_iter()
                    .map(|v| serde_json::to_value(v).unwrap_or(JsonValue::Null))
                    .collect()
            )),
        }
    }

    /// Create a typed Map value (flattened key-value pairs)
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

    /// Create an untyped string value
    pub fn string(s: impl Into<String>) -> Self {
        GraphSONValue::Untyped(JsonValue::String(s.into()))
    }

    /// Create an untyped boolean value
    pub fn boolean(b: bool) -> Self {
        GraphSONValue::Untyped(JsonValue::Bool(b))
    }

    /// Create an untyped null value
    pub fn null() -> Self {
        GraphSONValue::Untyped(JsonValue::Null)
    }

    /// Get the type tag if this is a typed value
    pub fn type_tag(&self) -> Option<&str> {
        match self {
            GraphSONValue::Typed { type_tag, .. } => Some(type_tag),
            GraphSONValue::Untyped(_) => None,
        }
    }
}
```

### 5.2 Vertex and Edge Types

```rust
/// A GraphSON vertex structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSONVertex {
    /// Vertex ID (g:Int64)
    pub id: GraphSONValue,
    /// Vertex label
    pub label: String,
    /// Properties map: property_key -> list of VertexProperty
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, Vec<GraphSONVertexProperty>>,
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
    pub properties: HashMap<String, GraphSONProperty>,
}

/// A GraphSON edge property.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSONProperty {
    /// Property key
    pub key: String,
    /// Property value
    pub value: GraphSONValue,
}

/// A complete GraphSON graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSONGraph {
    /// All vertices
    #[serde(default)]
    pub vertices: Vec<GraphSONVertex>,
    /// All edges
    #[serde(default)]
    pub edges: Vec<GraphSONEdge>,
}
```

### 5.3 Typed Wrappers for Serialization

```rust
/// Wrapper for serializing a vertex with type annotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedVertex {
    #[serde(rename = "@type")]
    pub type_tag: String, // Always "g:Vertex"
    #[serde(rename = "@value")]
    pub value: GraphSONVertex,
}

impl TypedVertex {
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
    #[serde(rename = "@type")]
    pub type_tag: String, // Always "g:Edge"
    #[serde(rename = "@value")]
    pub value: GraphSONEdge,
}

impl TypedEdge {
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
    #[serde(rename = "@type")]
    pub type_tag: String, // "tinker:graph"
    #[serde(rename = "@value")]
    pub value: GraphSONGraph,
}

impl TypedGraph {
    pub fn new(graph: GraphSONGraph) -> Self {
        TypedGraph {
            type_tag: "tinker:graph".to_string(),
            value: graph,
        }
    }
}
```

## 6. Serialization

### 6.1 Value Conversion (`serialize.rs`)

```rust
use crate::value::{Value, VertexId, EdgeId};
use super::types::GraphSONValue;

/// Convert an Intersteller Value to a GraphSON value.
pub fn value_to_graphson(value: &Value) -> GraphSONValue {
    match value {
        Value::Null => GraphSONValue::null(),
        Value::Bool(b) => GraphSONValue::boolean(*b),
        Value::Int(n) => GraphSONValue::int64(*n),
        Value::Float(f) => GraphSONValue::double(*f),
        Value::String(s) => GraphSONValue::string(s.clone()),
        Value::List(items) => {
            let converted: Vec<GraphSONValue> = items
                .iter()
                .map(value_to_graphson)
                .collect();
            GraphSONValue::list(converted)
        }
        Value::Map(map) => {
            let pairs: Vec<(GraphSONValue, GraphSONValue)> = map
                .iter()
                .map(|(k, v)| (GraphSONValue::string(k.clone()), value_to_graphson(v)))
                .collect();
            GraphSONValue::map(pairs)
        }
        Value::Vertex(id) => GraphSONValue::int64(id.0 as i64),
        Value::Edge(id) => GraphSONValue::int64(id.0 as i64),
    }
}

/// Convert a VertexId to GraphSON.
pub fn vertex_id_to_graphson(id: VertexId) -> GraphSONValue {
    GraphSONValue::int64(id.0 as i64)
}

/// Convert an EdgeId to GraphSON.
pub fn edge_id_to_graphson(id: EdgeId) -> GraphSONValue {
    GraphSONValue::int64(id.0 as i64)
}
```

### 6.2 Graph Serialization

```rust
use crate::storage::GraphStorage;
use super::types::*;
use std::collections::HashMap;

/// Property ID counter for vertex properties.
/// Each vertex property gets a unique ID.
struct PropertyIdGenerator {
    next_id: u64,
}

impl PropertyIdGenerator {
    fn new() -> Self {
        PropertyIdGenerator { next_id: 1 }
    }

    fn next(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

/// Serialize a graph to GraphSON format.
pub fn serialize_graph<S: GraphStorage>(storage: &S) -> TypedGraph {
    let mut prop_id_gen = PropertyIdGenerator::new();
    let mut vertices = Vec::new();
    let mut edges = Vec::new();

    // Serialize all vertices
    for vertex in storage.vertices() {
        let vertex_id = vertex_id_to_graphson(vertex.id);
        
        let mut properties: HashMap<String, Vec<GraphSONVertexProperty>> = HashMap::new();
        for (key, value) in vertex.properties.iter() {
            let prop = GraphSONVertexProperty {
                id: GraphSONValue::int64(prop_id_gen.next() as i64),
                label: key.clone(),
                value: value_to_graphson(value),
                properties: HashMap::new(),
            };
            properties.entry(key.clone()).or_default().push(prop);
        }

        vertices.push(GraphSONVertex {
            id: vertex_id,
            label: vertex.label.clone(),
            properties,
        });
    }

    // Serialize all edges
    for edge in storage.edges() {
        let out_vertex = storage.get_vertex(edge.out_v)
            .expect("Edge source vertex must exist");
        let in_vertex = storage.get_vertex(edge.in_v)
            .expect("Edge target vertex must exist");

        let mut properties: HashMap<String, GraphSONProperty> = HashMap::new();
        for (key, value) in edge.properties.iter() {
            properties.insert(key.clone(), GraphSONProperty {
                key: key.clone(),
                value: value_to_graphson(value),
            });
        }

        edges.push(GraphSONEdge {
            id: edge_id_to_graphson(edge.id),
            label: edge.label.clone(),
            out_v: vertex_id_to_graphson(edge.out_v),
            out_v_label: out_vertex.label.clone(),
            in_v: vertex_id_to_graphson(edge.in_v),
            in_v_label: in_vertex.label.clone(),
            properties,
        });
    }

    TypedGraph::new(GraphSONGraph { vertices, edges })
}

/// Serialize a graph to a JSON string.
pub fn to_json_string<S: GraphStorage>(storage: &S) -> Result<String, serde_json::Error> {
    let typed_graph = serialize_graph(storage);
    serde_json::to_string(&typed_graph)
}

/// Serialize a graph to a pretty-printed JSON string.
pub fn to_json_string_pretty<S: GraphStorage>(storage: &S) -> Result<String, serde_json::Error> {
    let typed_graph = serialize_graph(storage);
    serde_json::to_string_pretty(&typed_graph)
}

/// Serialize a graph to a writer.
pub fn to_writer<S: GraphStorage, W: std::io::Write>(
    storage: &S,
    writer: W,
) -> Result<(), serde_json::Error> {
    let typed_graph = serialize_graph(storage);
    serde_json::to_writer(writer, &typed_graph)
}

/// Serialize a graph to a pretty-printed writer.
pub fn to_writer_pretty<S: GraphStorage, W: std::io::Write>(
    storage: &S,
    writer: W,
) -> Result<(), serde_json::Error> {
    let typed_graph = serialize_graph(storage);
    serde_json::to_writer_pretty(writer, &typed_graph)
}
```

### 6.3 Serialization Options

```rust
/// Options for controlling GraphSON serialization.
#[derive(Debug, Clone, Default)]
pub struct SerializeOptions {
    /// Include schema metadata in output
    pub include_schema: bool,
    /// Pretty-print the JSON output
    pub pretty: bool,
    /// Filter vertices by label (None = all)
    pub vertex_labels: Option<Vec<String>>,
    /// Filter edges by label (None = all)
    pub edge_labels: Option<Vec<String>>,
}

impl SerializeOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_schema(mut self, include: bool) -> Self {
        self.include_schema = include;
        self
    }

    pub fn pretty(mut self, enabled: bool) -> Self {
        self.pretty = enabled;
        self
    }

    pub fn vertex_labels(mut self, labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.vertex_labels = Some(labels.into_iter().map(Into::into).collect());
        self
    }

    pub fn edge_labels(mut self, labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.edge_labels = Some(labels.into_iter().map(Into::into).collect());
        self
    }
}
```

## 7. Deserialization

### 7.1 Error Types (`error.rs`)

```rust
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
    InvalidValue { type_tag: String, message: String },

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

pub type Result<T> = std::result::Result<T, GraphSONError>;
```

### 7.2 Value Conversion (`deserialize.rs`)

```rust
use crate::value::{Value, VertexId, EdgeId};
use super::types::GraphSONValue;
use super::error::{GraphSONError, Result};
use serde_json::Value as JsonValue;

/// Convert a GraphSON value to an Intersteller Value.
pub fn graphson_to_value(gs_value: &GraphSONValue) -> Result<Value> {
    match gs_value {
        GraphSONValue::Untyped(json) => json_to_value(json),
        GraphSONValue::Typed { type_tag, value } => {
            match type_tag.as_str() {
                "g:Int32" | "g:Int64" => {
                    let n = value.as_i64().ok_or_else(|| GraphSONError::InvalidValue {
                        type_tag: type_tag.clone(),
                        message: "expected integer".to_string(),
                    })?;
                    Ok(Value::Int(n))
                }
                "g:Float" | "g:Double" => {
                    let f = value.as_f64().ok_or_else(|| GraphSONError::InvalidValue {
                        type_tag: type_tag.clone(),
                        message: "expected number".to_string(),
                    })?;
                    Ok(Value::Float(f))
                }
                "g:List" | "g:Set" => {
                    let arr = value.as_array().ok_or_else(|| GraphSONError::InvalidValue {
                        type_tag: type_tag.clone(),
                        message: "expected array".to_string(),
                    })?;
                    let items: Result<Vec<Value>> = arr
                        .iter()
                        .map(|v| json_to_value(v))
                        .collect();
                    Ok(Value::List(items?))
                }
                "g:Map" => {
                    // GraphSON Map is flattened: [k1, v1, k2, v2, ...]
                    let arr = value.as_array().ok_or_else(|| GraphSONError::InvalidValue {
                        type_tag: type_tag.clone(),
                        message: "expected array of key-value pairs".to_string(),
                    })?;
                    
                    if arr.len() % 2 != 0 {
                        return Err(GraphSONError::InvalidValue {
                            type_tag: type_tag.clone(),
                            message: "map array must have even length".to_string(),
                        });
                    }

                    let mut map = std::collections::HashMap::new();
                    for chunk in arr.chunks(2) {
                        let key = chunk[0].as_str().ok_or_else(|| {
                            GraphSONError::NonStringMapKey(format!("{:?}", chunk[0]))
                        })?;
                        let val = json_to_value(&chunk[1])?;
                        map.insert(key.to_string(), val);
                    }
                    Ok(Value::Map(map))
                }
                "g:UUID" => {
                    // Store UUID as string
                    let s = value.as_str().ok_or_else(|| GraphSONError::InvalidValue {
                        type_tag: type_tag.clone(),
                        message: "expected string".to_string(),
                    })?;
                    Ok(Value::String(s.to_string()))
                }
                "g:Date" => {
                    // Store as milliseconds since epoch
                    let ms = value.as_i64().ok_or_else(|| GraphSONError::InvalidValue {
                        type_tag: type_tag.clone(),
                        message: "expected integer timestamp".to_string(),
                    })?;
                    Ok(Value::Int(ms))
                }
                _ => Err(GraphSONError::UnknownTypeTag(type_tag.clone())),
            }
        }
    }
}

/// Convert a plain JSON value to an Intersteller Value.
fn json_to_value(json: &JsonValue) -> Result<Value> {
    match json {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(b) => Ok(Value::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(GraphSONError::InvalidValue {
                    type_tag: "number".to_string(),
                    message: "cannot convert number".to_string(),
                })
            }
        }
        JsonValue::String(s) => Ok(Value::String(s.clone())),
        JsonValue::Array(arr) => {
            let items: Result<Vec<Value>> = arr.iter().map(json_to_value).collect();
            Ok(Value::List(items?))
        }
        JsonValue::Object(obj) => {
            // Check if this is a typed value
            if let (Some(type_tag), Some(value)) = (obj.get("@type"), obj.get("@value")) {
                let gs_value = GraphSONValue::Typed {
                    type_tag: type_tag.as_str()
                        .ok_or_else(|| GraphSONError::InvalidValue {
                            type_tag: "@type".to_string(),
                            message: "must be string".to_string(),
                        })?
                        .to_string(),
                    value: Box::new(value.clone()),
                };
                graphson_to_value(&gs_value)
            } else {
                // Plain object -> Map
                let mut map = std::collections::HashMap::new();
                for (k, v) in obj {
                    map.insert(k.clone(), json_to_value(v)?);
                }
                Ok(Value::Map(map))
            }
        }
    }
}
```

### 7.3 Graph Deserialization

```rust
use crate::storage::InMemoryGraph;
use crate::value::{VertexId, EdgeId};
use super::types::*;
use super::error::{GraphSONError, Result};
use std::collections::HashMap;

/// Deserialize a GraphSON string into an InMemoryGraph.
pub fn from_json_str(json: &str) -> Result<InMemoryGraph> {
    let typed_graph: TypedGraph = serde_json::from_str(json)?;
    deserialize_graph(typed_graph.value)
}

/// Deserialize from a reader.
pub fn from_reader<R: std::io::Read>(reader: R) -> Result<InMemoryGraph> {
    let typed_graph: TypedGraph = serde_json::from_reader(reader)?;
    deserialize_graph(typed_graph.value)
}

/// Deserialize a GraphSONGraph into an InMemoryGraph.
fn deserialize_graph(gs_graph: GraphSONGraph) -> Result<InMemoryGraph> {
    let mut graph = InMemoryGraph::new();
    
    // Map from GraphSON vertex ID to our VertexId
    let mut vertex_id_map: HashMap<u64, VertexId> = HashMap::new();

    // First pass: create all vertices
    for gs_vertex in &gs_graph.vertices {
        let gs_id = extract_int64(&gs_vertex.id)?;
        
        // Convert properties
        let mut properties = HashMap::new();
        for (key, prop_list) in &gs_vertex.properties {
            // Take the first (and usually only) property value
            if let Some(prop) = prop_list.first() {
                let value = graphson_to_value(&prop.value)?;
                properties.insert(key.clone(), value);
            }
        }

        let vertex_id = graph.add_vertex(&gs_vertex.label, properties);
        
        if vertex_id_map.insert(gs_id, vertex_id).is_some() {
            return Err(GraphSONError::DuplicateVertexId(gs_id));
        }
    }

    // Second pass: create all edges
    for gs_edge in &gs_graph.edges {
        let out_v_gs = extract_int64(&gs_edge.out_v)?;
        let in_v_gs = extract_int64(&gs_edge.in_v)?;

        let out_v = *vertex_id_map.get(&out_v_gs)
            .ok_or(GraphSONError::VertexNotFound(out_v_gs))?;
        let in_v = *vertex_id_map.get(&in_v_gs)
            .ok_or(GraphSONError::VertexNotFound(in_v_gs))?;

        // Convert properties
        let mut properties = HashMap::new();
        for (key, prop) in &gs_edge.properties {
            let value = graphson_to_value(&prop.value)?;
            properties.insert(key.clone(), value);
        }

        graph.add_edge(out_v, in_v, &gs_edge.label, properties)
            .map_err(|e| GraphSONError::SchemaValidation(e.to_string()))?;
    }

    Ok(graph)
}

/// Extract an i64 from a GraphSON value.
fn extract_int64(value: &GraphSONValue) -> Result<u64> {
    match value {
        GraphSONValue::Typed { type_tag, value } if type_tag == "g:Int64" || type_tag == "g:Int32" => {
            value.as_i64()
                .map(|n| n as u64)
                .ok_or_else(|| GraphSONError::InvalidValue {
                    type_tag: type_tag.clone(),
                    message: "expected integer".to_string(),
                })
        }
        GraphSONValue::Untyped(JsonValue::Number(n)) => {
            n.as_u64().ok_or_else(|| GraphSONError::InvalidValue {
                type_tag: "number".to_string(),
                message: "expected unsigned integer".to_string(),
            })
        }
        _ => Err(GraphSONError::InvalidValue {
            type_tag: "id".to_string(),
            message: "expected integer ID".to_string(),
        }),
    }
}
```

### 7.4 Deserialization Options

```rust
/// Options for controlling GraphSON deserialization.
#[derive(Debug, Clone, Default)]
pub struct DeserializeOptions {
    /// Validate against schema if present
    pub validate_schema: bool,
    /// Strict mode: fail on unknown properties
    pub strict: bool,
    /// ID mapping mode
    pub id_mode: IdMode,
}

/// How to handle IDs during import.
#[derive(Debug, Clone, Default)]
pub enum IdMode {
    /// Preserve original IDs (may fail if conflicts)
    Preserve,
    /// Generate new IDs, ignore originals
    #[default]
    Generate,
    /// Map original IDs to new IDs
    Map,
}

impl DeserializeOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn validate_schema(mut self, enabled: bool) -> Self {
        self.validate_schema = enabled;
        self
    }

    pub fn strict(mut self, enabled: bool) -> Self {
        self.strict = enabled;
        self
    }

    pub fn id_mode(mut self, mode: IdMode) -> Self {
        self.id_mode = mode;
        self
    }
}
```

## 8. Schema Extension

GraphSON does not have native schema support, so we define a custom extension type
`intersteller:Schema` to embed schema metadata in exports.

### 8.1 Schema Type Format

```json
{
  "@type": "intersteller:Schema",
  "@value": {
    "mode": "strict",
    "vertexSchemas": [
      {
        "label": "Person",
        "additionalProperties": false,
        "properties": [
          {
            "key": "name",
            "type": "STRING",
            "required": true,
            "default": null
          },
          {
            "key": "age",
            "type": "INT",
            "required": false,
            "default": {"@type": "g:Int32", "@value": 0}
          }
        ]
      }
    ],
    "edgeSchemas": [
      {
        "label": "KNOWS",
        "fromLabels": ["Person"],
        "toLabels": ["Person"],
        "additionalProperties": false,
        "properties": [
          {
            "key": "since",
            "type": "INT",
            "required": true,
            "default": null
          }
        ]
      }
    ]
  }
}
```

### 8.2 Schema Serialization (`schema_ext.rs`)

```rust
use crate::schema::{GraphSchema, VertexSchema, EdgeSchema, PropertyDef, PropertyType, ValidationMode};
use super::types::GraphSONValue;
use serde::{Serialize, Deserialize};

/// GraphSON representation of a property type.
fn property_type_to_string(pt: &PropertyType) -> String {
    match pt {
        PropertyType::Any => "ANY".to_string(),
        PropertyType::Bool => "BOOL".to_string(),
        PropertyType::Int => "INT".to_string(),
        PropertyType::Float => "FLOAT".to_string(),
        PropertyType::String => "STRING".to_string(),
        PropertyType::List(None) => "LIST".to_string(),
        PropertyType::List(Some(inner)) => format!("LIST<{}>", property_type_to_string(inner)),
        PropertyType::Map(None) => "MAP".to_string(),
        PropertyType::Map(Some(inner)) => format!("MAP<{}>", property_type_to_string(inner)),
    }
}

/// Parse a property type string.
fn string_to_property_type(s: &str) -> Option<PropertyType> {
    match s {
        "ANY" => Some(PropertyType::Any),
        "BOOL" => Some(PropertyType::Bool),
        "INT" => Some(PropertyType::Int),
        "FLOAT" => Some(PropertyType::Float),
        "STRING" => Some(PropertyType::String),
        "LIST" => Some(PropertyType::List(None)),
        "MAP" => Some(PropertyType::Map(None)),
        s if s.starts_with("LIST<") && s.ends_with(">") => {
            let inner = &s[5..s.len()-1];
            string_to_property_type(inner).map(|t| PropertyType::List(Some(Box::new(t))))
        }
        s if s.starts_with("MAP<") && s.ends_with(">") => {
            let inner = &s[4..s.len()-1];
            string_to_property_type(inner).map(|t| PropertyType::Map(Some(Box::new(t))))
        }
        _ => None,
    }
}

/// JSON-serializable schema representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphSONSchema {
    pub mode: String,
    pub vertex_schemas: Vec<GraphSONVertexSchema>,
    pub edge_schemas: Vec<GraphSONEdgeSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphSONVertexSchema {
    pub label: String,
    pub additional_properties: bool,
    pub properties: Vec<GraphSONPropertyDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphSONEdgeSchema {
    pub label: String,
    pub from_labels: Vec<String>,
    pub to_labels: Vec<String>,
    pub additional_properties: bool,
    pub properties: Vec<GraphSONPropertyDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphSONPropertyDef {
    pub key: String,
    #[serde(rename = "type")]
    pub value_type: String,
    pub required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

/// Convert a GraphSchema to GraphSON format.
pub fn schema_to_graphson(schema: &GraphSchema) -> serde_json::Value {
    let mode = match schema.mode {
        ValidationMode::None => "none",
        ValidationMode::Warn => "warn",
        ValidationMode::Strict => "strict",
        ValidationMode::Closed => "closed",
    };

    let vertex_schemas: Vec<GraphSONVertexSchema> = schema.vertex_schemas
        .values()
        .map(|vs| GraphSONVertexSchema {
            label: vs.label.clone(),
            additional_properties: vs.additional_properties,
            properties: vs.properties.values().map(property_def_to_graphson).collect(),
        })
        .collect();

    let edge_schemas: Vec<GraphSONEdgeSchema> = schema.edge_schemas
        .values()
        .map(|es| GraphSONEdgeSchema {
            label: es.label.clone(),
            from_labels: es.from_labels.clone(),
            to_labels: es.to_labels.clone(),
            additional_properties: es.additional_properties,
            properties: es.properties.values().map(property_def_to_graphson).collect(),
        })
        .collect();

    let gs_schema = GraphSONSchema {
        mode: mode.to_string(),
        vertex_schemas,
        edge_schemas,
    };

    serde_json::json!({
        "@type": "intersteller:Schema",
        "@value": gs_schema
    })
}

fn property_def_to_graphson(prop: &PropertyDef) -> GraphSONPropertyDef {
    GraphSONPropertyDef {
        key: prop.key.clone(),
        value_type: property_type_to_string(&prop.value_type),
        required: prop.required,
        default: prop.default.as_ref().map(|v| value_to_json(v)),
    }
}

fn value_to_json(value: &crate::value::Value) -> serde_json::Value {
    // Convert Intersteller Value to JSON for default values
    match value {
        crate::value::Value::Null => serde_json::Value::Null,
        crate::value::Value::Bool(b) => serde_json::Value::Bool(*b),
        crate::value::Value::Int(n) => serde_json::json!({"@type": "g:Int64", "@value": n}),
        crate::value::Value::Float(f) => serde_json::json!({"@type": "g:Double", "@value": f}),
        crate::value::Value::String(s) => serde_json::Value::String(s.clone()),
        crate::value::Value::List(items) => {
            serde_json::json!({
                "@type": "g:List",
                "@value": items.iter().map(value_to_json).collect::<Vec<_>>()
            })
        }
        crate::value::Value::Map(map) => {
            let pairs: Vec<serde_json::Value> = map.iter()
                .flat_map(|(k, v)| vec![serde_json::Value::String(k.clone()), value_to_json(v)])
                .collect();
            serde_json::json!({"@type": "g:Map", "@value": pairs})
        }
        crate::value::Value::Vertex(id) => serde_json::json!({"@type": "g:Int64", "@value": id.0}),
        crate::value::Value::Edge(id) => serde_json::json!({"@type": "g:Int64", "@value": id.0}),
    }
}
```

### 8.3 Graph with Schema Format

When `include_schema: true`, the output includes schema:

```json
{
  "@type": "intersteller:GraphWithSchema",
  "@value": {
    "schema": {"@type": "intersteller:Schema", "@value": {...}},
    "graph": {"@type": "tinker:graph", "@value": {...}}
  }
}
```

## 9. Public API

### 9.1 Module Exports (`mod.rs`)

```rust
//! GraphSON 3.0 import/export support.
//!
//! This module provides functions to serialize and deserialize graphs
//! in the GraphSON 3.0 format used by Apache TinkerPop.
//!
//! # Example: Export a graph
//!
//! ```rust
//! use intersteller::graphson;
//! use intersteller::storage::InMemoryGraph;
//!
//! let graph = InMemoryGraph::new();
//! // ... populate graph ...
//!
//! let json = graphson::to_string(&graph).unwrap();
//! println!("{}", json);
//! ```
//!
//! # Example: Import a graph
//!
//! ```rust
//! use intersteller::graphson;
//!
//! let json = r#"{"@type": "tinker:graph", "@value": {"vertices": [], "edges": []}}"#;
//! let graph = graphson::from_str(json).unwrap();
//! ```

mod types;
mod serialize;
mod deserialize;
mod error;
mod schema_ext;

pub use error::{GraphSONError, Result};
pub use serialize::{SerializeOptions, to_json_string, to_json_string_pretty, to_writer, to_writer_pretty};
pub use deserialize::{DeserializeOptions, IdMode, from_json_str, from_reader};
pub use schema_ext::schema_to_graphson;
pub use types::{GraphSONValue, GraphSONVertex, GraphSONEdge, GraphSONGraph, TypedGraph};

use crate::storage::GraphStorage;

// Convenience re-exports with simpler names

/// Serialize a graph to a JSON string.
pub fn to_string<S: GraphStorage>(storage: &S) -> std::result::Result<String, serde_json::Error> {
    to_json_string(storage)
}

/// Serialize a graph to a pretty-printed JSON string.
pub fn to_string_pretty<S: GraphStorage>(storage: &S) -> std::result::Result<String, serde_json::Error> {
    to_json_string_pretty(storage)
}

/// Deserialize a graph from a JSON string.
pub fn from_str(json: &str) -> Result<crate::storage::InMemoryGraph> {
    from_json_str(json)
}

/// Export a graph with schema metadata.
pub fn to_string_with_schema<S: GraphStorage>(
    storage: &S, 
    schema: &crate::schema::GraphSchema
) -> std::result::Result<String, serde_json::Error> {
    use serialize::serialize_graph;
    
    let graph = serialize_graph(storage);
    let schema_json = schema_to_graphson(schema);
    
    let output = serde_json::json!({
        "@type": "intersteller:GraphWithSchema",
        "@value": {
            "schema": schema_json,
            "graph": graph
        }
    });
    
    serde_json::to_string_pretty(&output)
}
```

### 9.2 Usage Examples

```rust
use intersteller::prelude::*;
use intersteller::graphson;
use intersteller::storage::InMemoryGraph;
use intersteller::schema::{SchemaBuilder, PropertyType, ValidationMode};
use std::collections::HashMap;

// Create a graph
let mut graph = InMemoryGraph::new();
let alice = graph.add_vertex("Person", HashMap::from([
    ("name".to_string(), Value::String("Alice".to_string())),
    ("age".to_string(), Value::Int(30)),
]));
let bob = graph.add_vertex("Person", HashMap::from([
    ("name".to_string(), Value::String("Bob".to_string())),
    ("age".to_string(), Value::Int(25)),
]));
graph.add_edge(alice, bob, "knows", HashMap::from([
    ("since".to_string(), Value::Int(2020)),
])).unwrap();

// Export to GraphSON
let json = graphson::to_string_pretty(&graph).unwrap();
println!("{}", json);

// Export with schema
let schema = SchemaBuilder::new()
    .mode(ValidationMode::Strict)
    .vertex("Person")
        .property("name", PropertyType::String)
        .property("age", PropertyType::Int)
        .done()
    .edge("knows")
        .from(&["Person"])
        .to(&["Person"])
        .property("since", PropertyType::Int)
        .done()
    .build();

let json_with_schema = graphson::to_string_with_schema(&graph, &schema).unwrap();

// Import from GraphSON
let imported = graphson::from_str(&json).unwrap();
assert_eq!(imported.vertex_count(), 2);
assert_eq!(imported.edge_count(), 1);
```

## 10. Integration

### 10.1 Graph Trait Extensions

Add convenience methods to `Graph` types:

```rust
// In src/graph.rs or as extension trait

impl InMemoryGraph {
    /// Export this graph to GraphSON format.
    #[cfg(feature = "graphson")]
    pub fn to_graphson(&self) -> Result<String, serde_json::Error> {
        crate::graphson::to_string(self)
    }

    /// Export this graph to GraphSON format (pretty-printed).
    #[cfg(feature = "graphson")]
    pub fn to_graphson_pretty(&self) -> Result<String, serde_json::Error> {
        crate::graphson::to_string_pretty(self)
    }

    /// Create a graph from GraphSON data.
    #[cfg(feature = "graphson")]
    pub fn from_graphson(json: &str) -> crate::graphson::Result<Self> {
        crate::graphson::from_str(json)
    }
}
```

### 10.2 File I/O Helpers

```rust
use std::path::Path;
use std::fs::File;
use std::io::{BufReader, BufWriter};

/// Export a graph to a file.
pub fn export_to_file<S: GraphStorage, P: AsRef<Path>>(
    storage: &S,
    path: P,
) -> Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    to_writer_pretty(storage, writer)?;
    Ok(())
}

/// Import a graph from a file.
pub fn import_from_file<P: AsRef<Path>>(path: P) -> Result<crate::storage::InMemoryGraph> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    from_reader(reader)
}
```

### 10.3 CLI Integration (Future)

```bash
# Export
intersteller export --format graphson --output graph.json

# Export with schema
intersteller export --format graphson --include-schema --output graph.json

# Import
intersteller import --format graphson --input graph.json

# Validate without importing
intersteller validate --format graphson --input graph.json
```

## 11. Testing Strategy

### 11.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // Value conversion tests
    #[test]
    fn test_value_to_graphson_primitives() {
        assert_eq!(
            value_to_graphson(&Value::Int(42)),
            GraphSONValue::int64(42)
        );
        assert_eq!(
            value_to_graphson(&Value::String("hello".into())),
            GraphSONValue::string("hello")
        );
    }

    #[test]
    fn test_graphson_to_value_primitives() {
        let gs = GraphSONValue::int64(42);
        assert_eq!(graphson_to_value(&gs).unwrap(), Value::Int(42));
    }

    // Roundtrip tests
    #[test]
    fn test_value_roundtrip() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(-42),
            Value::Float(3.14),
            Value::String("test".into()),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        ];

        for original in values {
            let gs = value_to_graphson(&original);
            let recovered = graphson_to_value(&gs).unwrap();
            assert_eq!(original, recovered);
        }
    }

    // Graph serialization tests
    #[test]
    fn test_empty_graph_roundtrip() {
        let graph = InMemoryGraph::new();
        let json = to_string(&graph).unwrap();
        let imported = from_str(&json).unwrap();
        assert_eq!(imported.vertex_count(), 0);
        assert_eq!(imported.edge_count(), 0);
    }

    #[test]
    fn test_simple_graph_roundtrip() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::from([
            ("name".into(), Value::String("Alice".into())),
        ]));
        let v2 = graph.add_vertex("person", HashMap::from([
            ("name".into(), Value::String("Bob".into())),
        ]));
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        let json = to_string(&graph).unwrap();
        let imported = from_str(&json).unwrap();
        
        assert_eq!(imported.vertex_count(), 2);
        assert_eq!(imported.edge_count(), 1);
    }

    // Error handling tests
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
                        "inVLabel": "person"
                    }
                }]
            }
        }"#;
        let result = from_str(json);
        assert!(matches!(result, Err(GraphSONError::VertexNotFound(999))));
    }
}
```

### 11.2 Integration Tests

```rust
// tests/graphson.rs

use intersteller::graphson;
use intersteller::storage::InMemoryGraph;
use std::fs;

#[test]
fn test_export_import_file() {
    let mut graph = InMemoryGraph::new();
    // ... populate ...
    
    let temp_path = "/tmp/test_graph.json";
    graphson::export_to_file(&graph, temp_path).unwrap();
    
    let imported = graphson::import_from_file(temp_path).unwrap();
    fs::remove_file(temp_path).unwrap();
    
    assert_eq!(graph.vertex_count(), imported.vertex_count());
}

#[test]
fn test_tinkerpop_compatibility() {
    // Test against official TinkerPop examples
    let tinkerpop_json = include_str!("fixtures/tinkerpop_modern.json");
    let graph = graphson::from_str(tinkerpop_json).unwrap();
    
    // Verify expected structure
    assert!(graph.vertex_count() > 0);
}
```

### 11.3 Property-Based Tests

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn value_roundtrip_proptest(value in arb_value()) {
        let gs = value_to_graphson(&value);
        let recovered = graphson_to_value(&gs).unwrap();
        prop_assert_eq!(value, recovered);
    }
}
```

## 12. Implementation Plan

### Phase 1: Core Types and Value Conversion (1-2 days)

1. Create `src/graphson/` module structure
2. Implement `types.rs` with GraphSON structs
3. Implement `error.rs` with error types
4. Implement value conversion (both directions)
5. Add basic unit tests

### Phase 2: Graph Serialization (1-2 days)

1. Implement `serialize.rs`
2. Add vertex/edge serialization
3. Implement `SerializeOptions`
4. Add serialization tests

### Phase 3: Graph Deserialization (1-2 days)

1. Implement `deserialize.rs`
2. Add vertex/edge deserialization with ID mapping
3. Implement `DeserializeOptions`
4. Add deserialization tests

### Phase 4: Schema Extension (1 day)

1. Implement `schema_ext.rs`
2. Add schema serialization
3. Add `to_string_with_schema` function
4. Add schema deserialization (optional)

### Phase 5: Integration and Polish (1 day)

1. Add convenience methods to `InMemoryGraph`
2. Add file I/O helpers
3. Update `Cargo.toml` with feature flag
4. Add integration tests
5. Documentation and examples

### Deliverables

- [ ] `src/graphson/mod.rs` - Public API
- [ ] `src/graphson/types.rs` - Type definitions
- [ ] `src/graphson/serialize.rs` - Export
- [ ] `src/graphson/deserialize.rs` - Import
- [ ] `src/graphson/error.rs` - Error types
- [ ] `src/graphson/schema_ext.rs` - Schema support
- [ ] `tests/graphson.rs` - Integration tests
- [ ] `examples/graphson.rs` - Usage example
- [ ] Update `Cargo.toml` with `serde_json` dependency
- [ ] Update `src/lib.rs` to expose `graphson` module

### Success Criteria

1. Lossless roundtrip for all supported types
2. Compatible with TinkerPop GraphSON 3.0 format
3. Schema can be embedded and extracted
4. Clear error messages for malformed input
5. ≥90% test coverage for graphson module

