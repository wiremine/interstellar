//! Deserialization of GraphSON 3.0 format to Interstellar graphs.

use super::error::{GraphSONError, Result};
use super::types::*;
use crate::storage::Graph;
use crate::value::{Value, VertexId};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Convert a GraphSON value to an Interstellar Value.
pub fn graphson_to_value(gs_value: &GraphSONValue) -> Result<Value> {
    match gs_value {
        GraphSONValue::Untyped(json) => json_to_value(json),
        GraphSONValue::Typed { type_tag, value } => match type_tag.as_str() {
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
                let arr = value
                    .as_array()
                    .ok_or_else(|| GraphSONError::InvalidValue {
                        type_tag: type_tag.clone(),
                        message: "expected array".to_string(),
                    })?;
                let items: Result<Vec<Value>> = arr.iter().map(json_to_value).collect();
                Ok(Value::List(items?))
            }
            "g:Map" => {
                // GraphSON Map is flattened: [k1, v1, k2, v2, ...]
                let arr = value
                    .as_array()
                    .ok_or_else(|| GraphSONError::InvalidValue {
                        type_tag: type_tag.clone(),
                        message: "expected array of key-value pairs".to_string(),
                    })?;

                if arr.len() % 2 != 0 {
                    return Err(GraphSONError::InvalidValue {
                        type_tag: type_tag.clone(),
                        message: "map array must have even length".to_string(),
                    });
                }

                let mut map = HashMap::new();
                for chunk in arr.chunks(2) {
                    let key = chunk[0]
                        .as_str()
                        .ok_or_else(|| GraphSONError::NonStringMapKey(format!("{:?}", chunk[0])))?;
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
        },
    }
}

/// Convert a plain JSON value to an Interstellar Value.
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
                    type_tag: type_tag
                        .as_str()
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
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k.clone(), json_to_value(v)?);
                }
                Ok(Value::Map(map))
            }
        }
    }
}

/// Deserialize a GraphSON string into a Graph.
///
/// This function handles both plain `tinker:graph` format and the
/// `interstellar:GraphWithSchema` wrapper format.
pub fn from_json_str(json: &str) -> Result<Graph> {
    // First, parse as generic JSON to check the type tag
    let json_value: JsonValue = serde_json::from_str(json)?;

    let typed_graph = extract_graph_from_json(&json_value)?;
    deserialize_graph(typed_graph)
}

/// Deserialize from a reader.
///
/// This function handles both plain `tinker:graph` format and the
/// `interstellar:GraphWithSchema` wrapper format.
pub fn from_reader<R: std::io::Read>(reader: R) -> Result<Graph> {
    // First, parse as generic JSON to check the type tag
    let json_value: JsonValue = serde_json::from_reader(reader)?;

    let typed_graph = extract_graph_from_json(&json_value)?;
    deserialize_graph(typed_graph)
}

/// Extract the graph from JSON, handling both `tinker:graph` and
/// `interstellar:GraphWithSchema` wrapper formats.
fn extract_graph_from_json(json_value: &JsonValue) -> Result<GraphSONGraph> {
    let type_tag = json_value
        .get("@type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| GraphSONError::MissingField("@type".to_string()))?;

    match type_tag {
        "tinker:graph" => {
            // Direct graph format
            let typed_graph: TypedGraph = serde_json::from_value(json_value.clone())?;
            Ok(typed_graph.value)
        }
        "interstellar:GraphWithSchema" => {
            // Wrapper format with schema - extract the graph portion
            let graph_json = json_value
                .get("@value")
                .and_then(|v| v.get("graph"))
                .ok_or_else(|| GraphSONError::MissingField("@value.graph".to_string()))?;

            let typed_graph: TypedGraph = serde_json::from_value(graph_json.clone())?;
            Ok(typed_graph.value)
        }
        other => Err(GraphSONError::UnknownTypeTag(other.to_string())),
    }
}

/// Deserialize a GraphSONGraph into a Graph.
fn deserialize_graph(gs_graph: GraphSONGraph) -> Result<Graph> {
    let graph = Graph::new();

    // Map from GraphSON vertex ID to our VertexId
    let mut vertex_id_map: HashMap<u64, VertexId> = HashMap::new();

    // First pass: create all vertices
    for gs_vertex in &gs_graph.vertices {
        let gs_id = extract_int64(&gs_vertex.value.id)?;

        // Convert properties
        let mut properties = HashMap::new();
        for (key, prop_list) in &gs_vertex.value.properties {
            // Take the first (and usually only) property value
            if let Some(typed_prop) = prop_list.first() {
                let value = graphson_to_value(&typed_prop.value.value)?;
                properties.insert(key.clone(), value);
            }
        }

        let vertex_id = graph.add_vertex(&gs_vertex.value.label, properties);

        if vertex_id_map.insert(gs_id, vertex_id).is_some() {
            return Err(GraphSONError::DuplicateVertexId(gs_id));
        }
    }

    // Second pass: create all edges
    for gs_edge in &gs_graph.edges {
        let out_v_gs = extract_int64(&gs_edge.value.out_v)?;
        let in_v_gs = extract_int64(&gs_edge.value.in_v)?;

        let out_v = *vertex_id_map
            .get(&out_v_gs)
            .ok_or(GraphSONError::VertexNotFound(out_v_gs))?;
        let in_v = *vertex_id_map
            .get(&in_v_gs)
            .ok_or(GraphSONError::VertexNotFound(in_v_gs))?;

        // Convert properties
        let mut properties = HashMap::new();
        for (key, typed_prop) in &gs_edge.value.properties {
            let value = graphson_to_value(&typed_prop.value.value)?;
            properties.insert(key.clone(), value);
        }

        graph
            .add_edge(out_v, in_v, &gs_edge.value.label, properties)
            .map_err(|e| GraphSONError::SchemaValidation(e.to_string()))?;
    }

    Ok(graph)
}

/// Extract an i64 from a GraphSON value.
fn extract_int64(value: &GraphSONValue) -> Result<u64> {
    match value {
        GraphSONValue::Typed { type_tag, value }
            if type_tag == "g:Int64" || type_tag == "g:Int32" =>
        {
            value
                .as_i64()
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
    /// Create new default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to validate against schema.
    pub fn validate_schema(mut self, enabled: bool) -> Self {
        self.validate_schema = enabled;
        self
    }

    /// Set strict mode.
    pub fn strict(mut self, enabled: bool) -> Self {
        self.strict = enabled;
        self
    }

    /// Set ID handling mode.
    pub fn id_mode(mut self, mode: IdMode) -> Self {
        self.id_mode = mode;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::GraphStorage;

    #[test]
    fn test_graphson_to_value_primitives() {
        assert_eq!(
            graphson_to_value(&GraphSONValue::null()).unwrap(),
            Value::Null
        );
        assert_eq!(
            graphson_to_value(&GraphSONValue::boolean(true)).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            graphson_to_value(&GraphSONValue::int64(42)).unwrap(),
            Value::Int(42)
        );
        assert_eq!(
            graphson_to_value(&GraphSONValue::string("hello")).unwrap(),
            Value::String("hello".to_string())
        );
    }

    #[test]
    fn test_graphson_to_value_double() {
        let gs = GraphSONValue::double(3.14);
        let value = graphson_to_value(&gs).unwrap();
        match value {
            Value::Float(f) => assert!((f - 3.14).abs() < 0.001),
            _ => panic!("Expected Float"),
        }
    }

    #[test]
    fn test_graphson_to_value_list() {
        let gs = GraphSONValue::list(vec![
            GraphSONValue::int64(1),
            GraphSONValue::int64(2),
            GraphSONValue::int64(3),
        ]);
        let value = graphson_to_value(&gs).unwrap();
        assert_eq!(
            value,
            Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
    }

    #[test]
    fn test_graphson_to_value_map() {
        let gs = GraphSONValue::map(vec![
            (GraphSONValue::string("key1"), GraphSONValue::int64(1)),
            (GraphSONValue::string("key2"), GraphSONValue::int64(2)),
        ]);
        let value = graphson_to_value(&gs).unwrap();
        match value {
            Value::Map(map) => {
                assert_eq!(map.get("key1"), Some(&Value::Int(1)));
                assert_eq!(map.get("key2"), Some(&Value::Int(2)));
            }
            _ => panic!("Expected Map"),
        }
    }

    #[test]
    fn test_unknown_type_tag_error() {
        let gs = GraphSONValue::Typed {
            type_tag: "g:Unknown".to_string(),
            value: Box::new(JsonValue::Null),
        };
        let result = graphson_to_value(&gs);
        assert!(matches!(result, Err(GraphSONError::UnknownTypeTag(_))));
    }

    #[test]
    fn test_from_json_str_empty_graph() {
        let json = r#"{
            "@type": "tinker:graph",
            "@value": {
                "vertices": [],
                "edges": []
            }
        }"#;
        let graph = from_json_str(json).unwrap();
        assert_eq!(graph.snapshot().vertex_count(), 0);
        assert_eq!(graph.snapshot().edge_count(), 0);
    }

    #[test]
    fn test_from_json_str_with_vertices() {
        let json = r#"{
            "@type": "tinker:graph",
            "@value": {
                "vertices": [
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
                                ]
                            }
                        }
                    }
                ],
                "edges": []
            }
        }"#;
        let graph = from_json_str(json).unwrap();
        let snapshot = graph.snapshot();
        assert_eq!(snapshot.vertex_count(), 1);

        let vertices: Vec<_> = snapshot.all_vertices().collect();
        assert_eq!(vertices[0].label, "person");
        assert_eq!(
            vertices[0].properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
    }

    #[test]
    fn test_from_json_str_with_edges() {
        let json = r#"{
            "@type": "tinker:graph",
            "@value": {
                "vertices": [
                    {
                        "@type": "g:Vertex",
                        "@value": {
                            "id": {"@type": "g:Int64", "@value": 1},
                            "label": "person",
                            "properties": {}
                        }
                    },
                    {
                        "@type": "g:Vertex",
                        "@value": {
                            "id": {"@type": "g:Int64", "@value": 2},
                            "label": "person",
                            "properties": {}
                        }
                    }
                ],
                "edges": [
                    {
                        "@type": "g:Edge",
                        "@value": {
                            "id": {"@type": "g:Int64", "@value": 100},
                            "label": "knows",
                            "outV": {"@type": "g:Int64", "@value": 1},
                            "outVLabel": "person",
                            "inV": {"@type": "g:Int64", "@value": 2},
                            "inVLabel": "person",
                            "properties": {}
                        }
                    }
                ]
            }
        }"#;
        let graph = from_json_str(json).unwrap();
        let snapshot = graph.snapshot();
        assert_eq!(snapshot.vertex_count(), 2);
        assert_eq!(snapshot.edge_count(), 1);

        let edges: Vec<_> = snapshot.all_edges().collect();
        assert_eq!(edges[0].label, "knows");
    }

    #[test]
    fn test_vertex_not_found_error() {
        let json = r#"{
            "@type": "tinker:graph",
            "@value": {
                "vertices": [],
                "edges": [
                    {
                        "@type": "g:Edge",
                        "@value": {
                            "id": {"@type": "g:Int64", "@value": 100},
                            "label": "knows",
                            "outV": {"@type": "g:Int64", "@value": 999},
                            "outVLabel": "person",
                            "inV": {"@type": "g:Int64", "@value": 1000},
                            "inVLabel": "person",
                            "properties": {}
                        }
                    }
                ]
            }
        }"#;
        let result = from_json_str(json);
        assert!(matches!(result, Err(GraphSONError::VertexNotFound(999))));
    }

    #[test]
    fn test_invalid_json_error() {
        let result = from_json_str("not valid json");
        assert!(matches!(result, Err(GraphSONError::JsonParse(_))));
    }

    #[test]
    fn test_deserialize_options() {
        let opts = DeserializeOptions::new()
            .validate_schema(true)
            .strict(true)
            .id_mode(IdMode::Preserve);

        assert!(opts.validate_schema);
        assert!(opts.strict);
        assert!(matches!(opts.id_mode, IdMode::Preserve));
    }
}
