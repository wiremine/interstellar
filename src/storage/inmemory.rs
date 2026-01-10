//! In-memory graph storage backend.
//!
//! This module provides [`InMemoryGraph`], a HashMap-based graph storage
//! implementation suitable for development, testing, and small to medium
//! sized graphs that fit in memory.
//!
//! # Features
//!
//! - **O(1) lookups**: Vertex and edge retrieval by ID
//! - **Indexed labels**: RoaringBitmap indices for fast label filtering
//! - **String interning**: Compact label storage via [`StringInterner`]
//! - **Adjacency lists**: Efficient edge traversal
//!
//! # Example
//!
//! ```
//! use rustgremlin::storage::{GraphStorage, InMemoryGraph};
//! use std::collections::HashMap;
//!
//! // Create a graph
//! let mut graph = InMemoryGraph::new();
//!
//! // Add vertices
//! let alice = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), "Alice".into()),
//!     ("age".to_string(), 30.into()),
//! ]));
//! let bob = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), "Bob".into()),
//! ]));
//! let rust = graph.add_vertex("software", HashMap::from([
//!     ("name".to_string(), "Rust".into()),
//! ]));
//!
//! // Add edges
//! graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
//! graph.add_edge(alice, rust, "uses", HashMap::new()).unwrap();
//!
//! // Query the graph
//! assert_eq!(graph.vertex_count(), 3);
//! assert_eq!(graph.out_edges(alice).count(), 2);
//! assert_eq!(graph.vertices_with_label("person").count(), 2);
//! ```
//!
//! # Thread Safety
//!
//! `InMemoryGraph` implements `Send + Sync` but does **not** provide interior
//! mutability. For concurrent access, wrap it in the [`Graph`](crate::Graph)
//! type which provides readers-writer lock synchronization.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use roaring::RoaringBitmap;

use crate::error::StorageError;
use crate::storage::interner::StringInterner;
use crate::storage::{Edge, GraphStorage, GraphStorageMut, Vertex};
use crate::value::{EdgeId, Value, VertexId};

/// In-memory graph storage with HashMap-based lookups.
///
/// This is the primary storage backend for development and testing. It stores
/// all graph data in memory using HashMaps for O(1) vertex/edge lookups and
/// RoaringBitmaps for efficient label indexing.
///
/// # Data Structure
///
/// ```text
/// InMemoryGraph
/// ├── nodes: HashMap<VertexId, NodeData>     # Vertex storage
/// ├── edges: HashMap<EdgeId, EdgeData>       # Edge storage
/// ├── vertex_labels: HashMap<u32, Bitmap>    # Label → vertex IDs
/// ├── edge_labels: HashMap<u32, Bitmap>      # Label → edge IDs
/// └── string_table: StringInterner           # Label string interning
/// ```
///
/// # Performance Characteristics
///
/// | Operation | Complexity |
/// |-----------|------------|
/// | `get_vertex` | O(1) |
/// | `get_edge` | O(1) |
/// | `add_vertex` | O(1) amortized |
/// | `add_edge` | O(1) |
/// | `remove_vertex` | O(degree) |
/// | `remove_edge` | O(degree) |
/// | `out_edges` | O(out_degree) |
/// | `in_edges` | O(in_degree) |
/// | `vertices_with_label` | O(n) where n = matching vertices |
///
/// # Example
///
/// ```
/// use rustgremlin::storage::{GraphStorage, InMemoryGraph};
/// use std::collections::HashMap;
///
/// let mut graph = InMemoryGraph::new();
///
/// // Build a small social network
/// let alice = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), "Alice".into()),
/// ]));
/// let bob = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), "Bob".into()),
/// ]));
///
/// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
///
/// // Vertices and edges can be removed
/// graph.remove_edge(edge_id).unwrap();
/// graph.remove_vertex(bob).unwrap();
/// ```
pub struct InMemoryGraph {
    /// Vertex data keyed by ID
    nodes: HashMap<VertexId, NodeData>,

    /// Edge data keyed by ID
    edges: HashMap<EdgeId, EdgeData>,

    /// Next vertex ID (atomic for future thread-safety)
    next_vertex_id: AtomicU64,

    /// Next edge ID (atomic for future thread-safety)
    next_edge_id: AtomicU64,

    /// Label ID -> set of vertex IDs with that label
    vertex_labels: HashMap<u32, RoaringBitmap>,

    /// Label ID -> set of edge IDs with that label
    edge_labels: HashMap<u32, RoaringBitmap>,

    /// String interning for labels
    string_table: StringInterner,
}

/// Internal vertex representation
#[derive(Clone, Debug)]
struct NodeData {
    /// Vertex identifier
    id: VertexId,

    /// Interned label string ID
    label_id: u32,

    /// Property key-value pairs
    properties: HashMap<String, Value>,

    /// Outgoing edge IDs (adjacency list)
    out_edges: Vec<EdgeId>,

    /// Incoming edge IDs (adjacency list)
    in_edges: Vec<EdgeId>,
}

/// Internal edge representation
#[derive(Clone, Debug)]
struct EdgeData {
    /// Edge identifier
    id: EdgeId,

    /// Interned label string ID
    label_id: u32,

    /// Source vertex ID
    src: VertexId,

    /// Destination vertex ID
    dst: VertexId,

    /// Property key-value pairs
    properties: HashMap<String, Value>,
}

impl InMemoryGraph {
    /// Creates a new empty in-memory graph.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, InMemoryGraph};
    ///
    /// let graph = InMemoryGraph::new();
    /// assert_eq!(graph.vertex_count(), 0);
    /// assert_eq!(graph.edge_count(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            next_vertex_id: AtomicU64::new(0),
            next_edge_id: AtomicU64::new(0),
            vertex_labels: HashMap::new(),
            edge_labels: HashMap::new(),
            string_table: StringInterner::new(),
        }
    }

    /// Adds a vertex with the given label and properties.
    ///
    /// Returns the new vertex's unique ID. Vertex IDs are assigned sequentially
    /// starting from 0 and are never reused within a graph instance.
    ///
    /// # Arguments
    ///
    /// * `label` - The vertex label (e.g., "person", "software")
    /// * `properties` - Key-value pairs of vertex properties
    ///
    /// # Returns
    ///
    /// The [`VertexId`] of the newly created vertex.
    ///
    /// # Complexity
    ///
    /// O(1) amortized (HashMap insertion).
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, InMemoryGraph};
    /// use rustgremlin::Value;
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    ///
    /// let id = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), Value::String("Alice".to_string())),
    ///     ("age".to_string(), Value::Int(30)),
    /// ]));
    ///
    /// let vertex = graph.get_vertex(id).unwrap();
    /// assert_eq!(vertex.label, "person");
    /// ```
    pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        let id = VertexId(self.next_vertex_id.fetch_add(1, Ordering::Relaxed));
        let label_id = self.string_table.intern(label);

        let node = NodeData {
            id,
            label_id,
            properties,
            out_edges: Vec::new(),
            in_edges: Vec::new(),
        };

        self.nodes.insert(id, node);

        // Update label index
        self.vertex_labels
            .entry(label_id)
            .or_default()
            .insert(id.0 as u32);

        id
    }

    /// Adds an edge between two vertices with the given label and properties.
    ///
    /// Edges are directed: they go from `src` (source) to `dst` (destination).
    /// Self-loops (edges where `src == dst`) are allowed.
    ///
    /// # Arguments
    ///
    /// * `src` - Source vertex ID (where the edge starts)
    /// * `dst` - Destination vertex ID (where the edge ends)
    /// * `label` - The edge label (e.g., "knows", "created")
    /// * `properties` - Key-value pairs of edge properties
    ///
    /// # Returns
    ///
    /// The [`EdgeId`] of the newly created edge, or an error if either
    /// vertex doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::VertexNotFound`] if `src` or `dst` doesn't exist.
    ///
    /// # Complexity
    ///
    /// O(1).
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, InMemoryGraph};
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    ///
    /// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::from([
    ///     ("since".to_string(), 2020.into()),
    /// ])).unwrap();
    ///
    /// let edge = graph.get_edge(edge_id).unwrap();
    /// assert_eq!(edge.src, alice);
    /// assert_eq!(edge.dst, bob);
    /// ```
    pub fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        // Validate vertices exist
        if !self.nodes.contains_key(&src) {
            return Err(StorageError::VertexNotFound(src));
        }
        if !self.nodes.contains_key(&dst) {
            return Err(StorageError::VertexNotFound(dst));
        }

        let id = EdgeId(self.next_edge_id.fetch_add(1, Ordering::Relaxed));
        let label_id = self.string_table.intern(label);

        let edge = EdgeData {
            id,
            label_id,
            src,
            dst,
            properties,
        };

        self.edges.insert(id, edge);

        // Update adjacency lists
        self.nodes.get_mut(&src).unwrap().out_edges.push(id);
        self.nodes.get_mut(&dst).unwrap().in_edges.push(id);

        // Update label index
        self.edge_labels
            .entry(label_id)
            .or_default()
            .insert(id.0 as u32);

        Ok(id)
    }

    /// Removes a vertex and all its incident edges.
    ///
    /// When a vertex is removed, all edges that connect to it (both incoming
    /// and outgoing) are also removed.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the vertex to remove
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::VertexNotFound`] if the vertex doesn't exist.
    ///
    /// # Complexity
    ///
    /// O(degree) where degree = in_degree + out_degree, due to edge removal.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, InMemoryGraph};
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// // Removing alice also removes the "knows" edge
    /// graph.remove_vertex(alice).unwrap();
    ///
    /// assert_eq!(graph.vertex_count(), 1);
    /// assert_eq!(graph.edge_count(), 0);
    /// ```
    pub fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError> {
        let node = self
            .nodes
            .remove(&id)
            .ok_or(StorageError::VertexNotFound(id))?;

        // Remove from label index
        if let Some(bitmap) = self.vertex_labels.get_mut(&node.label_id) {
            bitmap.remove(id.0 as u32);
        }

        // Collect incident edges to remove
        let edges_to_remove: Vec<EdgeId> = node
            .out_edges
            .iter()
            .chain(node.in_edges.iter())
            .copied()
            .collect();

        // Remove all incident edges
        for edge_id in edges_to_remove {
            // Ignore errors (edge may already be processed if self-loop)
            let _ = self.remove_edge_internal(edge_id, Some(id));
        }

        Ok(())
    }

    /// Removes an edge from the graph.
    ///
    /// The source and destination vertices are updated to remove this edge
    /// from their adjacency lists.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the edge to remove
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::EdgeNotFound`] if the edge doesn't exist.
    ///
    /// # Complexity
    ///
    /// O(degree) due to adjacency list removal.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, InMemoryGraph};
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    /// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// graph.remove_edge(edge_id).unwrap();
    ///
    /// assert_eq!(graph.edge_count(), 0);
    /// assert!(graph.get_edge(edge_id).is_none());
    /// ```
    pub fn remove_edge(&mut self, id: EdgeId) -> Result<(), StorageError> {
        self.remove_edge_internal(id, None)
    }

    /// Internal edge removal, optionally skipping a vertex being deleted
    fn remove_edge_internal(
        &mut self,
        id: EdgeId,
        skip_vertex: Option<VertexId>,
    ) -> Result<(), StorageError> {
        let edge = self
            .edges
            .remove(&id)
            .ok_or(StorageError::EdgeNotFound(id))?;

        // Remove from label index
        if let Some(bitmap) = self.edge_labels.get_mut(&edge.label_id) {
            bitmap.remove(id.0 as u32);
        }

        // Remove from source vertex's out_edges (if not being deleted)
        if skip_vertex != Some(edge.src) {
            if let Some(src_node) = self.nodes.get_mut(&edge.src) {
                src_node.out_edges.retain(|&e| e != id);
            }
        }

        // Remove from destination vertex's in_edges (if not being deleted)
        if skip_vertex != Some(edge.dst) {
            if let Some(dst_node) = self.nodes.get_mut(&edge.dst) {
                dst_node.in_edges.retain(|&e| e != id);
            }
        }

        Ok(())
    }

    /// Sets or updates a property on a vertex.
    ///
    /// If the property already exists, its value is replaced.
    /// If it doesn't exist, it is created.
    ///
    /// # Arguments
    ///
    /// * `id` - The vertex ID
    /// * `key` - The property key
    /// * `value` - The new property value
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::VertexNotFound`] if the vertex doesn't exist.
    ///
    /// # Complexity
    ///
    /// O(1) amortized.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, InMemoryGraph};
    /// use rustgremlin::Value;
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// let id = graph.add_vertex("person", HashMap::new());
    ///
    /// graph.set_vertex_property(id, "name", Value::String("Alice".into())).unwrap();
    ///
    /// let vertex = graph.get_vertex(id).unwrap();
    /// assert_eq!(vertex.properties.get("name"), Some(&Value::String("Alice".into())));
    /// ```
    pub fn set_vertex_property(
        &mut self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let node = self
            .nodes
            .get_mut(&id)
            .ok_or(StorageError::VertexNotFound(id))?;

        node.properties.insert(key.to_string(), value);
        Ok(())
    }

    /// Sets or updates a property on an edge.
    ///
    /// If the property already exists, its value is replaced.
    /// If it doesn't exist, it is created.
    ///
    /// # Arguments
    ///
    /// * `id` - The edge ID
    /// * `key` - The property key
    /// * `value` - The new property value
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::EdgeNotFound`] if the edge doesn't exist.
    ///
    /// # Complexity
    ///
    /// O(1) amortized.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, InMemoryGraph};
    /// use rustgremlin::Value;
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    /// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// graph.set_edge_property(edge_id, "since", Value::Int(2020)).unwrap();
    ///
    /// let edge = graph.get_edge(edge_id).unwrap();
    /// assert_eq!(edge.properties.get("since"), Some(&Value::Int(2020)));
    /// ```
    pub fn set_edge_property(
        &mut self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let edge = self
            .edges
            .get_mut(&id)
            .ok_or(StorageError::EdgeNotFound(id))?;

        edge.properties.insert(key.to_string(), value);
        Ok(())
    }
}

impl Default for InMemoryGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphStorage for InMemoryGraph {
    /// O(1) vertex lookup
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        let node = self.nodes.get(&id)?;
        let label = self.string_table.resolve(node.label_id)?;

        Some(Vertex {
            id: node.id,
            label: label.to_string(),
            properties: node.properties.clone(),
        })
    }

    /// O(1) count
    fn vertex_count(&self) -> u64 {
        self.nodes.len() as u64
    }

    /// O(1) edge lookup
    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        let edge = self.edges.get(&id)?;
        let label = self.string_table.resolve(edge.label_id)?;

        Some(Edge {
            id: edge.id,
            label: label.to_string(),
            src: edge.src,
            dst: edge.dst,
            properties: edge.properties.clone(),
        })
    }

    /// O(1) count
    fn edge_count(&self) -> u64 {
        self.edges.len() as u64
    }

    /// O(degree) iteration over outgoing edges
    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let iter = self
            .nodes
            .get(&vertex)
            .into_iter()
            .flat_map(|node| node.out_edges.iter())
            .filter_map(|&edge_id| self.get_edge(edge_id));

        Box::new(iter)
    }

    /// O(degree) iteration over incoming edges
    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let iter = self
            .nodes
            .get(&vertex)
            .into_iter()
            .flat_map(|node| node.in_edges.iter())
            .filter_map(|&edge_id| self.get_edge(edge_id));

        Box::new(iter)
    }

    /// O(n) where n = vertices with label (uses RoaringBitmap)
    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_> {
        // Look up label ID without interning (read-only)
        let label_id = self.string_table.lookup(label);

        let iter = label_id
            .and_then(|id| self.vertex_labels.get(&id))
            .into_iter()
            .flat_map(|bitmap| bitmap.iter())
            .filter_map(|id| self.get_vertex(VertexId(id as u64)));

        Box::new(iter)
    }

    /// O(n) where n = edges with label (uses RoaringBitmap)
    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_> {
        // Look up label ID without interning (read-only)
        let label_id = self.string_table.lookup(label);

        let iter = label_id
            .and_then(|id| self.edge_labels.get(&id))
            .into_iter()
            .flat_map(|bitmap| bitmap.iter())
            .filter_map(|id| self.get_edge(EdgeId(id as u64)));

        Box::new(iter)
    }

    /// O(n) full vertex scan
    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let iter = self.nodes.keys().filter_map(|&id| self.get_vertex(id));

        Box::new(iter)
    }

    /// O(m) full edge scan
    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let iter = self.edges.keys().filter_map(|&id| self.get_edge(id));

        Box::new(iter)
    }

    /// Get the string interner for label resolution
    fn interner(&self) -> &StringInterner {
        &self.string_table
    }
}

impl GraphStorageMut for InMemoryGraph {
    fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        InMemoryGraph::add_vertex(self, label, properties)
    }

    fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        InMemoryGraph::add_edge(self, src, dst, label, properties)
    }

    fn set_vertex_property(
        &mut self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        InMemoryGraph::set_vertex_property(self, id, key, value)
    }

    fn set_edge_property(
        &mut self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        InMemoryGraph::set_edge_property(self, id, key, value)
    }

    fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError> {
        InMemoryGraph::remove_vertex(self, id)
    }

    fn remove_edge(&mut self, id: EdgeId) -> Result<(), StorageError> {
        InMemoryGraph::remove_edge(self, id)
    }
}

// SAFETY: InMemoryGraph is Send + Sync because:
// - HashMap is Send + Sync when K, V are Send + Sync
// - AtomicU64 is Send + Sync
// - RoaringBitmap is Send + Sync
// - StringInterner is Send + Sync (HashMap-based)
//
// Note: InMemoryGraph itself does NOT provide interior mutability.
// Thread-safe mutation requires external synchronization (via Graph wrapper).
unsafe impl Send for InMemoryGraph {}
unsafe impl Sync for InMemoryGraph {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_graph_is_empty() {
        let graph = InMemoryGraph::new();
        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn add_vertex_returns_unique_ids() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("software", HashMap::new());

        assert_ne!(v1, v2);
        assert_ne!(v2, v3);
        assert_eq!(graph.vertex_count(), 3);
    }

    #[test]
    fn add_vertex_with_properties() {
        let mut graph = InMemoryGraph::new();
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));

        let id = graph.add_vertex("person", props);
        let vertex = graph.get_vertex(id).unwrap();

        assert_eq!(vertex.label, "person");
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(vertex.properties.get("age"), Some(&Value::Int(30)));
    }

    #[test]
    fn get_vertex_returns_none_for_missing() {
        let graph = InMemoryGraph::new();
        assert!(graph.get_vertex(VertexId(999)).is_none());
    }

    #[test]
    fn add_edge_connects_vertices() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());

        let edge_id = graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        let edge = graph.get_edge(edge_id).unwrap();

        assert_eq!(edge.src, v1);
        assert_eq!(edge.dst, v2);
        assert_eq!(edge.label, "knows");
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn add_edge_fails_for_missing_source() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());

        let result = graph.add_edge(VertexId(999), v1, "knows", HashMap::new());
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
    }

    #[test]
    fn add_edge_fails_for_missing_destination() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());

        let result = graph.add_edge(v1, VertexId(999), "knows", HashMap::new());
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
    }

    #[test]
    fn out_edges_returns_outgoing() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());

        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v3, "knows", HashMap::new()).unwrap();
        graph.add_edge(v2, v1, "knows", HashMap::new()).unwrap();

        let out: Vec<Edge> = graph.out_edges(v1).collect();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|e| e.src == v1));
    }

    #[test]
    fn in_edges_returns_incoming() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());

        graph.add_edge(v2, v1, "knows", HashMap::new()).unwrap();
        graph.add_edge(v3, v1, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        let incoming: Vec<Edge> = graph.in_edges(v1).collect();
        assert_eq!(incoming.len(), 2);
        assert!(incoming.iter().all(|e| e.dst == v1));
    }

    #[test]
    fn vertices_with_label_filters_correctly() {
        let mut graph = InMemoryGraph::new();
        graph.add_vertex("person", HashMap::new());
        graph.add_vertex("person", HashMap::new());
        graph.add_vertex("software", HashMap::new());

        let people: Vec<Vertex> = graph.vertices_with_label("person").collect();
        let software: Vec<Vertex> = graph.vertices_with_label("software").collect();
        let unknown: Vec<Vertex> = graph.vertices_with_label("unknown").collect();

        assert_eq!(people.len(), 2);
        assert_eq!(software.len(), 1);
        assert_eq!(unknown.len(), 0);
    }

    #[test]
    fn edges_with_label_filters_correctly() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("software", HashMap::new());

        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v3, "created", HashMap::new()).unwrap();

        let knows: Vec<Edge> = graph.edges_with_label("knows").collect();
        let created: Vec<Edge> = graph.edges_with_label("created").collect();

        assert_eq!(knows.len(), 1);
        assert_eq!(created.len(), 1);
    }

    #[test]
    fn remove_vertex_removes_incident_edges() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());

        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        graph.add_edge(v2, v3, "knows", HashMap::new()).unwrap();
        graph.add_edge(v3, v1, "knows", HashMap::new()).unwrap();

        graph.remove_vertex(v1).unwrap();

        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 1); // Only v2->v3 remains
        assert!(graph.get_vertex(v1).is_none());
    }

    #[test]
    fn remove_edge_updates_adjacency() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());

        let e1 = graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        let e2 = graph.add_edge(v1, v2, "likes", HashMap::new()).unwrap();

        graph.remove_edge(e1).unwrap();

        assert_eq!(graph.edge_count(), 1);
        assert!(graph.get_edge(e1).is_none());
        assert!(graph.get_edge(e2).is_some());

        let out: Vec<Edge> = graph.out_edges(v1).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].label, "likes");
    }

    #[test]
    fn all_vertices_iterates_all() {
        let mut graph = InMemoryGraph::new();
        graph.add_vertex("a", HashMap::new());
        graph.add_vertex("b", HashMap::new());
        graph.add_vertex("c", HashMap::new());

        let all: Vec<Vertex> = graph.all_vertices().collect();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn all_edges_iterates_all() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("a", HashMap::new());
        let v2 = graph.add_vertex("b", HashMap::new());

        graph.add_edge(v1, v2, "e1", HashMap::new()).unwrap();
        graph.add_edge(v2, v1, "e2", HashMap::new()).unwrap();

        let all: Vec<Edge> = graph.all_edges().collect();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn self_loop_edge() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());

        let e = graph.add_edge(v1, v1, "self", HashMap::new()).unwrap();

        let out: Vec<Edge> = graph.out_edges(v1).collect();
        let in_edges: Vec<Edge> = graph.in_edges(v1).collect();

        assert_eq!(out.len(), 1);
        assert_eq!(in_edges.len(), 1);
        assert_eq!(out[0].id, e);
        assert_eq!(in_edges[0].id, e);
    }

    #[test]
    fn remove_vertex_with_self_loop() {
        let mut graph = InMemoryGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        graph.add_edge(v1, v1, "self", HashMap::new()).unwrap();

        graph.remove_vertex(v1).unwrap();

        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }
}
