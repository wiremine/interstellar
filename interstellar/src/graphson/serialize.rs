//! Serialization of Interstellar graphs to GraphSON 3.0 format.

use super::types::*;
use crate::storage::GraphStorage;
use crate::value::{EdgeId, Value, VertexId};
use std::collections::HashMap;

/// Convert an Interstellar Value to a GraphSON value.
pub fn value_to_graphson(value: &Value) -> GraphSONValue {
    match value {
        Value::Null => GraphSONValue::null(),
        Value::Bool(b) => GraphSONValue::boolean(*b),
        Value::Int(n) => GraphSONValue::int64(*n),
        Value::Float(f) => GraphSONValue::double(*f),
        Value::String(s) => GraphSONValue::string(s.clone()),
        Value::List(items) => {
            let converted: Vec<GraphSONValue> = items.iter().map(value_to_graphson).collect();
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
        Value::Point(p) => GraphSONValue::Typed {
            type_tag: "g:Point".to_string(),
            value: Box::new(serde_json::json!({"longitude": p.lon, "latitude": p.lat})),
        },
        Value::Polygon(p) => GraphSONValue::Typed {
            type_tag: "is:Polygon".to_string(),
            value: Box::new(
                serde_json::json!({"ring": p.ring.iter().map(|&(lon, lat)| vec![lon, lat]).collect::<Vec<_>>()}),
            ),
        },
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
    for vertex in storage.all_vertices() {
        let vertex_id = vertex_id_to_graphson(vertex.id);

        let mut properties: HashMap<String, Vec<TypedVertexProperty>> = HashMap::new();
        for (key, value) in vertex.properties.iter() {
            let prop = GraphSONVertexProperty {
                id: GraphSONValue::int64(prop_id_gen.next() as i64),
                label: key.clone(),
                value: value_to_graphson(value),
                properties: HashMap::new(),
            };
            properties
                .entry(key.clone())
                .or_default()
                .push(TypedVertexProperty::new(prop));
        }

        vertices.push(TypedVertex::new(GraphSONVertex {
            id: vertex_id,
            label: vertex.label.clone(),
            properties,
        }));
    }

    // Serialize all edges
    for edge in storage.all_edges() {
        let out_vertex = storage
            .get_vertex(edge.src)
            .expect("Edge source vertex must exist");
        let in_vertex = storage
            .get_vertex(edge.dst)
            .expect("Edge target vertex must exist");

        let mut properties: HashMap<String, TypedProperty> = HashMap::new();
        for (key, value) in edge.properties.iter() {
            properties.insert(
                key.clone(),
                TypedProperty::new(GraphSONProperty {
                    key: key.clone(),
                    value: value_to_graphson(value),
                }),
            );
        }

        edges.push(TypedEdge::new(GraphSONEdge {
            id: edge_id_to_graphson(edge.id),
            label: edge.label.clone(),
            out_v: vertex_id_to_graphson(edge.src),
            out_v_label: out_vertex.label.clone(),
            in_v: vertex_id_to_graphson(edge.dst),
            in_v_label: in_vertex.label.clone(),
            properties,
        }));
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
    /// Create new default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to include schema metadata.
    pub fn with_schema(mut self, include: bool) -> Self {
        self.include_schema = include;
        self
    }

    /// Set whether to pretty-print the output.
    pub fn pretty(mut self, enabled: bool) -> Self {
        self.pretty = enabled;
        self
    }

    /// Filter to specific vertex labels.
    pub fn vertex_labels(mut self, labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.vertex_labels = Some(labels.into_iter().map(Into::into).collect());
        self
    }

    /// Filter to specific edge labels.
    pub fn edge_labels(mut self, labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.edge_labels = Some(labels.into_iter().map(Into::into).collect());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use std::collections::HashMap;

    #[test]
    fn test_value_to_graphson_primitives() {
        assert_eq!(value_to_graphson(&Value::Null), GraphSONValue::null());
        assert_eq!(
            value_to_graphson(&Value::Bool(true)),
            GraphSONValue::boolean(true)
        );
        assert_eq!(value_to_graphson(&Value::Int(42)), GraphSONValue::int64(42));
        assert_eq!(
            value_to_graphson(&Value::String("hello".into())),
            GraphSONValue::string("hello")
        );
    }

    #[test]
    fn test_value_to_graphson_list() {
        let list = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        let gs = value_to_graphson(&list);
        assert_eq!(gs.type_tag(), Some("g:List"));
    }

    #[test]
    fn test_value_to_graphson_map() {
        let mut map = crate::value::ValueMap::new();
        map.insert("key".to_string(), Value::Int(42));
        let gs = value_to_graphson(&Value::Map(map));
        assert_eq!(gs.type_tag(), Some("g:Map"));
    }

    #[test]
    fn test_serialize_empty_graph() {
        let graph = Graph::new();
        let snapshot = graph.snapshot();
        let typed_graph = serialize_graph(&snapshot);
        assert!(typed_graph.value.vertices.is_empty());
        assert!(typed_graph.value.edges.is_empty());
    }

    #[test]
    fn test_serialize_graph_with_vertices() {
        let graph = Graph::new();
        graph.add_vertex(
            "person",
            HashMap::from([
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
            ]),
        );

        let snapshot = graph.snapshot();
        let typed_graph = serialize_graph(&snapshot);

        assert_eq!(typed_graph.value.vertices.len(), 1);
        let vertex = &typed_graph.value.vertices[0];
        assert_eq!(vertex.type_tag, "g:Vertex");
        assert_eq!(vertex.value.label, "person");
        assert!(vertex.value.properties.contains_key("name"));
        assert!(vertex.value.properties.contains_key("age"));
    }

    #[test]
    fn test_serialize_graph_with_edges() {
        let graph = Graph::new();
        let alice = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
        );
        let bob = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
        );
        graph
            .add_edge(
                alice,
                bob,
                "knows",
                HashMap::from([("since".to_string(), Value::Int(2020))]),
            )
            .unwrap();

        let snapshot = graph.snapshot();
        let typed_graph = serialize_graph(&snapshot);

        assert_eq!(typed_graph.value.vertices.len(), 2);
        assert_eq!(typed_graph.value.edges.len(), 1);

        let edge = &typed_graph.value.edges[0];
        assert_eq!(edge.type_tag, "g:Edge");
        assert_eq!(edge.value.label, "knows");
        assert_eq!(edge.value.out_v_label, "person");
        assert_eq!(edge.value.in_v_label, "person");
        assert!(edge.value.properties.contains_key("since"));
    }

    #[test]
    fn test_to_json_string() {
        let graph = Graph::new();
        graph.add_vertex("test", HashMap::new());

        let snapshot = graph.snapshot();
        let json = to_json_string(&snapshot).unwrap();

        assert!(json.contains("tinker:graph"));
        assert!(json.contains("g:Vertex"));
        assert!(json.contains("test"));
    }

    #[test]
    fn test_to_json_string_pretty() {
        let graph = Graph::new();
        graph.add_vertex("test", HashMap::new());

        let snapshot = graph.snapshot();
        let json = to_json_string_pretty(&snapshot).unwrap();

        // Pretty print should have newlines
        assert!(json.contains('\n'));
        assert!(json.contains("tinker:graph"));
    }

    #[test]
    fn test_serialize_options() {
        let opts = SerializeOptions::new()
            .with_schema(true)
            .pretty(true)
            .vertex_labels(["person", "software"])
            .edge_labels(["knows"]);

        assert!(opts.include_schema);
        assert!(opts.pretty);
        assert_eq!(
            opts.vertex_labels,
            Some(vec!["person".to_string(), "software".to_string()])
        );
        assert_eq!(opts.edge_labels, Some(vec!["knows".to_string()]));
    }
}
