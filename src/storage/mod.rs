use std::collections::HashMap;

pub mod interner;

use crate::value::{EdgeId, Value, VertexId};

#[derive(Clone, Debug)]
pub struct Vertex {
    pub id: VertexId,
    pub label: String,
    pub properties: HashMap<String, Value>,
}

#[derive(Clone, Debug)]
pub struct Edge {
    pub id: EdgeId,
    pub label: String,
    pub src: VertexId,
    pub dst: VertexId,
    pub properties: HashMap<String, Value>,
}

pub trait GraphStorage: Send + Sync {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex>;
    fn vertex_count(&self) -> u64;

    fn get_edge(&self, id: EdgeId) -> Option<Edge>;
    fn edge_count(&self) -> u64;

    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;
    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;

    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_>;
    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_>;

    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_>;
    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_>;
}
