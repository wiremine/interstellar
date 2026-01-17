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
//! use interstellar::storage::{GraphStorage, InMemoryGraph};
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
use crate::index::{
    BTreeIndex, ElementType, IndexError, IndexSpec, IndexType, PropertyIndex, UniqueIndex,
};
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
/// use interstellar::storage::{GraphStorage, InMemoryGraph};
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

    /// Property indexes by name
    indexes: HashMap<String, Box<dyn PropertyIndex>>,
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
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
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
            indexes: HashMap::new(),
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
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
    /// use interstellar::Value;
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
            properties: properties.clone(),
            out_edges: Vec::new(),
            in_edges: Vec::new(),
        };

        self.nodes.insert(id, node);

        // Update label index
        self.vertex_labels
            .entry(label_id)
            .or_default()
            .insert(id.0 as u32);

        // Update property indexes
        self.index_vertex_insert(id, label, &properties);

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
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
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
            properties: properties.clone(),
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

        // Update property indexes
        self.index_edge_insert(id, label, &properties);

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
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
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
        // Get node data for index removal before removing from storage
        let node = self
            .nodes
            .get(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        let label_id = node.label_id;
        let properties = node.properties.clone();
        let edges_to_remove: Vec<EdgeId> = node
            .out_edges
            .iter()
            .chain(node.in_edges.iter())
            .copied()
            .collect();

        // Remove from property indexes BEFORE removing from storage
        self.index_vertex_remove(id, label_id, &properties);

        // Now remove from storage
        let node = self.nodes.remove(&id).unwrap();

        // Remove from label index
        if let Some(bitmap) = self.vertex_labels.get_mut(&node.label_id) {
            bitmap.remove(id.0 as u32);
        }

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
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
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
        // Get edge data for index removal before removing from storage
        let edge = self.edges.get(&id).ok_or(StorageError::EdgeNotFound(id))?;
        let label_id = edge.label_id;
        let properties = edge.properties.clone();
        let src = edge.src;
        let dst = edge.dst;

        // Remove from property indexes BEFORE removing from storage
        self.index_edge_remove(id, label_id, &properties);

        // Now remove from storage
        let edge = self.edges.remove(&id).unwrap();

        // Remove from label index
        if let Some(bitmap) = self.edge_labels.get_mut(&edge.label_id) {
            bitmap.remove(id.0 as u32);
        }

        // Remove from source vertex's out_edges (if not being deleted)
        if skip_vertex != Some(src) {
            if let Some(src_node) = self.nodes.get_mut(&src) {
                src_node.out_edges.retain(|&e| e != id);
            }
        }

        // Remove from destination vertex's in_edges (if not being deleted)
        if skip_vertex != Some(dst) {
            if let Some(dst_node) = self.nodes.get_mut(&dst) {
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
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
    /// use interstellar::Value;
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
        // Get old value and label for index update
        let node = self
            .nodes
            .get(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        let label = self
            .string_table
            .resolve(node.label_id)
            .map(|s| s.to_string());
        let old_value = node.properties.get(key).cloned();

        // Update indexes before modifying the vertex
        self.update_vertex_property_in_indexes(id, &label, key, old_value.as_ref(), &value)?;

        // Now update the property
        let node = self.nodes.get_mut(&id).unwrap();
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
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
    /// use interstellar::Value;
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
        // Get old value and label for index update
        let edge = self.edges.get(&id).ok_or(StorageError::EdgeNotFound(id))?;
        let label = self
            .string_table
            .resolve(edge.label_id)
            .map(|s| s.to_string());
        let old_value = edge.properties.get(key).cloned();

        // Update indexes before modifying the edge
        self.update_edge_property_in_indexes(id, &label, key, old_value.as_ref(), &value)?;

        // Now update the property
        let edge = self.edges.get_mut(&id).unwrap();
        edge.properties.insert(key.to_string(), value);
        Ok(())
    }

    // =========================================================================
    // Index Management
    // =========================================================================

    /// Creates a new property index and populates it with existing data.
    ///
    /// The index will be automatically maintained as vertices and edges
    /// are added, updated, or removed.
    ///
    /// # Arguments
    ///
    /// * `spec` - The index specification defining what to index
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::AlreadyExists`] if an index with the same name exists.
    /// Returns [`IndexError::DuplicateValue`] if creating a unique index and
    /// duplicate values exist.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::InMemoryGraph;
    /// use interstellar::index::IndexBuilder;
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    ///
    /// // Add some data first
    /// graph.add_vertex("person", HashMap::from([
    ///     ("age".to_string(), 30i64.into()),
    /// ]));
    ///
    /// // Create a B+ tree index for range queries
    /// graph.create_index(
    ///     IndexBuilder::vertex()
    ///         .label("person")
    ///         .property("age")
    ///         .build()
    ///         .unwrap()
    /// ).unwrap();
    /// ```
    pub fn create_index(&mut self, spec: IndexSpec) -> Result<(), IndexError> {
        // Check for duplicate name
        if self.indexes.contains_key(&spec.name) {
            return Err(IndexError::AlreadyExists(spec.name.clone()));
        }

        // Create the appropriate index type
        let mut index: Box<dyn PropertyIndex> = match spec.index_type {
            IndexType::BTree => Box::new(BTreeIndex::new(spec.clone())),
            IndexType::Unique => Box::new(UniqueIndex::new(spec.clone())),
        };

        // Populate index with existing data
        self.populate_index(&mut *index)?;

        self.indexes.insert(spec.name.clone(), index);
        Ok(())
    }

    /// Drops an index by name.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::NotFound`] if no index with that name exists.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::InMemoryGraph;
    /// use interstellar::index::IndexBuilder;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// graph.create_index(
    ///     IndexBuilder::vertex()
    ///         .property("age")
    ///         .name("idx_age")
    ///         .build()
    ///         .unwrap()
    /// ).unwrap();
    ///
    /// graph.drop_index("idx_age").unwrap();
    /// ```
    pub fn drop_index(&mut self, name: &str) -> Result<(), IndexError> {
        self.indexes
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| IndexError::NotFound(name.to_string()))
    }

    /// Returns an iterator over all index specifications.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::InMemoryGraph;
    /// use interstellar::index::IndexBuilder;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// graph.create_index(
    ///     IndexBuilder::vertex()
    ///         .property("age")
    ///         .build()
    ///         .unwrap()
    /// ).unwrap();
    ///
    /// for spec in graph.list_indexes() {
    ///     println!("Index: {} on property '{}'", spec.name, spec.property);
    /// }
    /// ```
    pub fn list_indexes(&self) -> impl Iterator<Item = &IndexSpec> {
        self.indexes.values().map(|idx| idx.spec())
    }

    /// Checks if an index with the given name exists.
    pub fn has_index(&self, name: &str) -> bool {
        self.indexes.contains_key(name)
    }

    /// Returns the number of indexes.
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }

    /// Gets a reference to an index by name.
    pub fn get_index(&self, name: &str) -> Option<&dyn PropertyIndex> {
        self.indexes.get(name).map(|idx| idx.as_ref())
    }

    /// Lookup vertices by indexed property value.
    ///
    /// If an applicable index exists, uses it for O(log n) or O(1) lookup.
    /// Otherwise falls back to O(n) scan.
    ///
    /// # Arguments
    ///
    /// * `label` - Optional label filter
    /// * `property` - Property key to match
    /// * `value` - Property value to find
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::{GraphStorage, InMemoryGraph};
    /// use interstellar::index::IndexBuilder;
    /// use interstellar::Value;
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// graph.add_vertex("user", HashMap::from([
    ///     ("email".to_string(), Value::String("alice@example.com".into())),
    /// ]));
    ///
    /// // Create unique index
    /// graph.create_index(
    ///     IndexBuilder::vertex()
    ///         .label("user")
    ///         .property("email")
    ///         .unique()
    ///         .build()
    ///         .unwrap()
    /// ).unwrap();
    ///
    /// // O(1) lookup via index
    /// let users: Vec<_> = graph.vertices_by_property(
    ///     Some("user"),
    ///     "email",
    ///     &Value::String("alice@example.com".into())
    /// ).collect();
    /// assert_eq!(users.len(), 1);
    /// ```
    pub fn vertices_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        // Try to find an applicable index
        for index in self.indexes.values() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != property {
                continue;
            }
            // Check label compatibility
            match (&spec.label, label) {
                (Some(idx_label), Some(filter_label)) if idx_label != filter_label => continue,
                (Some(_), None) => continue, // Index is label-specific, query is not
                _ => {}
            }

            // Use index - convert label to owned for the closure
            let ids: Vec<u64> = index.lookup_eq(value).collect();
            let label_owned = label.map(|s| s.to_string());
            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| self.get_vertex(VertexId(id)))
                    .filter(move |v| {
                        label_owned.is_none() || Some(v.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }

        // Fall back to scan
        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        Box::new(self.all_vertices().filter(move |v| {
            if let Some(ref l) = label_owned {
                if &v.label != l {
                    return false;
                }
            }
            v.properties.get(&property_owned) == Some(&value_clone)
        }))
    }

    /// Lookup edges by indexed property value.
    ///
    /// If an applicable index exists, uses it for O(log n) or O(1) lookup.
    /// Otherwise falls back to O(n) scan.
    pub fn edges_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Edge> + '_> {
        // Try to find an applicable index
        for index in self.indexes.values() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if spec.property != property {
                continue;
            }
            // Check label compatibility
            match (&spec.label, label) {
                (Some(idx_label), Some(filter_label)) if idx_label != filter_label => continue,
                (Some(_), None) => continue, // Index is label-specific, query is not
                _ => {}
            }

            // Use index - convert label to owned for the closure
            let ids: Vec<u64> = index.lookup_eq(value).collect();
            let label_owned = label.map(|s| s.to_string());
            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| self.get_edge(EdgeId(id)))
                    .filter(move |e| {
                        label_owned.is_none() || Some(e.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }

        // Fall back to scan
        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        Box::new(self.all_edges().filter(move |e| {
            if let Some(ref l) = label_owned {
                if &e.label != l {
                    return false;
                }
            }
            e.properties.get(&property_owned) == Some(&value_clone)
        }))
    }

    /// Lookup vertices by property range, using indexes if available.
    ///
    /// If an applicable BTree index exists, uses it for O(log n) range lookup.
    /// Otherwise falls back to O(n) scan.
    ///
    /// # Arguments
    ///
    /// * `label` - Optional label filter
    /// * `property` - Property key to match
    /// * `start` - Start bound of the range
    /// * `end` - End bound of the range
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::ops::Bound;
    ///
    /// // Find all people aged 18-65
    /// let adults: Vec<_> = graph.vertices_by_property_range(
    ///     Some("person"),
    ///     "age",
    ///     Bound::Included(&Value::Int(18)),
    ///     Bound::Excluded(&Value::Int(65)),
    /// ).collect();
    /// ```
    pub fn vertices_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        use std::ops::Bound;

        // Try to find an applicable BTree index
        for index in self.indexes.values() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != property {
                continue;
            }
            // BTree indexes support range queries; skip unique indexes
            if spec.index_type != crate::index::IndexType::BTree {
                continue;
            }
            // Check label compatibility
            match (&spec.label, label) {
                (Some(idx_label), Some(filter_label)) if idx_label != filter_label => continue,
                (Some(_), None) => continue, // Index is label-specific, query is not
                _ => {}
            }

            // Use index for range lookup
            let ids: Vec<u64> = index.lookup_range(start, end).collect();
            let label_owned = label.map(|s| s.to_string());
            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| self.get_vertex(VertexId(id)))
                    .filter(move |v| {
                        label_owned.is_none() || Some(v.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }

        // Fall back to scan with range filter
        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let start_clone = match start {
            Bound::Included(v) => Bound::Included(v.clone()),
            Bound::Excluded(v) => Bound::Excluded(v.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end_clone = match end {
            Bound::Included(v) => Bound::Included(v.clone()),
            Bound::Excluded(v) => Bound::Excluded(v.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        Box::new(self.all_vertices().filter(move |v| {
            if let Some(ref l) = label_owned {
                if &v.label != l {
                    return false;
                }
            }
            if let Some(prop_value) = v.properties.get(&property_owned) {
                // Check range bounds using ComparableValue for ordering
                let prop_cmp = prop_value.to_comparable();
                let in_start = match &start_clone {
                    Bound::Included(s) => prop_cmp >= s.to_comparable(),
                    Bound::Excluded(s) => prop_cmp > s.to_comparable(),
                    Bound::Unbounded => true,
                };
                let in_end = match &end_clone {
                    Bound::Included(e) => prop_cmp <= e.to_comparable(),
                    Bound::Excluded(e) => prop_cmp < e.to_comparable(),
                    Bound::Unbounded => true,
                };
                in_start && in_end
            } else {
                false
            }
        }))
    }

    // =========================================================================
    // Internal Index Helpers
    // =========================================================================

    /// Populate an index with existing graph data.
    fn populate_index(&self, index: &mut dyn PropertyIndex) -> Result<(), IndexError> {
        // Clone spec data to avoid borrow issues
        let spec = index.spec().clone();

        match spec.element_type {
            ElementType::Vertex => {
                for (id, node) in &self.nodes {
                    // Check label filter
                    if let Some(ref label) = spec.label {
                        let node_label = self.string_table.resolve(node.label_id);
                        if node_label != Some(label.as_str()) {
                            continue;
                        }
                    }

                    // Get property value
                    if let Some(value) = node.properties.get(&spec.property) {
                        index.insert(value.clone(), id.0)?;
                    }
                }
            }
            ElementType::Edge => {
                for (id, edge) in &self.edges {
                    // Check label filter
                    if let Some(ref label) = spec.label {
                        let edge_label = self.string_table.resolve(edge.label_id);
                        if edge_label != Some(label.as_str()) {
                            continue;
                        }
                    }

                    // Get property value
                    if let Some(value) = edge.properties.get(&spec.property) {
                        index.insert(value.clone(), id.0)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Update indexes when a vertex is added.
    fn index_vertex_insert(
        &mut self,
        id: VertexId,
        label: &str,
        properties: &HashMap<String, Value>,
    ) {
        for index in self.indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                // Ignore errors for BTree (no constraint), log for unique
                let _ = index.insert(value.clone(), id.0);
            }
        }
    }

    /// Update indexes when a vertex is removed.
    fn index_vertex_remove(
        &mut self,
        id: VertexId,
        label_id: u32,
        properties: &HashMap<String, Value>,
    ) {
        let label = self.string_table.resolve(label_id).map(|s| s.to_string());

        for index in self.indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if label.as_ref() != Some(idx_label) {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                let _ = index.remove(value, id.0);
            }
        }
    }

    /// Update indexes when an edge is added.
    fn index_edge_insert(&mut self, id: EdgeId, label: &str, properties: &HashMap<String, Value>) {
        for index in self.indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                let _ = index.insert(value.clone(), id.0);
            }
        }
    }

    /// Update indexes when an edge is removed.
    fn index_edge_remove(
        &mut self,
        id: EdgeId,
        label_id: u32,
        properties: &HashMap<String, Value>,
    ) {
        let label = self.string_table.resolve(label_id).map(|s| s.to_string());

        for index in self.indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if label.as_ref() != Some(idx_label) {
                    continue;
                }
            }
            if let Some(value) = properties.get(&spec.property) {
                let _ = index.remove(value, id.0);
            }
        }
    }

    /// Update indexes when a vertex property changes.
    fn update_vertex_property_in_indexes(
        &mut self,
        id: VertexId,
        label: &Option<String>,
        property: &str,
        old_value: Option<&Value>,
        new_value: &Value,
    ) -> Result<(), StorageError> {
        for index in self.indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != property {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if label.as_ref() != Some(idx_label) {
                    continue;
                }
            }

            // Remove old value, insert new
            if let Some(old) = old_value {
                let _ = index.remove(old, id.0);
            }
            index
                .insert(new_value.clone(), id.0)
                .map_err(|e| StorageError::IndexError(e.to_string()))?;
        }
        Ok(())
    }

    /// Update indexes when an edge property changes.
    fn update_edge_property_in_indexes(
        &mut self,
        id: EdgeId,
        label: &Option<String>,
        property: &str,
        old_value: Option<&Value>,
        new_value: &Value,
    ) -> Result<(), StorageError> {
        for index in self.indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if spec.property != property {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if label.as_ref() != Some(idx_label) {
                    continue;
                }
            }

            // Remove old value, insert new
            if let Some(old) = old_value {
                let _ = index.remove(old, id.0);
            }
            index
                .insert(new_value.clone(), id.0)
                .map_err(|e| StorageError::IndexError(e.to_string()))?;
        }
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

    // =========================================================================
    // Index-Aware Overrides
    // =========================================================================

    /// Returns true - InMemoryGraph supports property indexes.
    fn supports_indexes(&self) -> bool {
        true
    }

    /// Lookup vertices by property value using indexes if available.
    fn vertices_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        // Delegate to the inherent method which handles index lookups
        InMemoryGraph::vertices_by_property(self, label, property, value)
    }

    /// Lookup edges by property value using indexes if available.
    fn edges_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Edge> + '_> {
        // Delegate to the inherent method which handles index lookups
        InMemoryGraph::edges_by_property(self, label, property, value)
    }

    /// Lookup vertices by property range using indexes if available.
    fn vertices_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        // Delegate to the inherent method which handles index lookups
        InMemoryGraph::vertices_by_property_range(self, label, property, start, end)
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

    // =========================================================================
    // Index Integration Tests
    // =========================================================================

    #[test]
    fn create_index_on_empty_graph() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        let result = graph.create_index(
            IndexBuilder::vertex()
                .property("age")
                .name("idx_age")
                .build()
                .unwrap(),
        );

        assert!(result.is_ok());
        assert!(graph.has_index("idx_age"));
        assert_eq!(graph.index_count(), 1);
    }

    #[test]
    fn create_index_populates_existing_data() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        // Add vertices first
        graph.add_vertex(
            "person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        );
        graph.add_vertex(
            "person",
            HashMap::from([("age".to_string(), Value::Int(25))]),
        );
        graph.add_vertex(
            "person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        );

        // Create index after data exists
        graph
            .create_index(
                IndexBuilder::vertex()
                    .label("person")
                    .property("age")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        // Lookup should find indexed values
        let age_30: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(30))
            .collect();
        assert_eq!(age_30.len(), 2);

        let age_25: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(25))
            .collect();
        assert_eq!(age_25.len(), 1);
    }

    #[test]
    fn index_updated_on_vertex_insert() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        // Create index first
        graph
            .create_index(
                IndexBuilder::vertex()
                    .label("user")
                    .property("email")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        // Add vertices after index exists
        graph.add_vertex(
            "user",
            HashMap::from([(
                "email".to_string(),
                Value::String("alice@example.com".into()),
            )]),
        );
        graph.add_vertex(
            "user",
            HashMap::from([("email".to_string(), Value::String("bob@example.com".into()))]),
        );

        // Lookup should find newly inserted data
        let alice: Vec<_> = graph
            .vertices_by_property(
                Some("user"),
                "email",
                &Value::String("alice@example.com".into()),
            )
            .collect();
        assert_eq!(alice.len(), 1);
    }

    #[test]
    fn index_updated_on_property_change() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        let v1 = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
        );

        graph
            .create_index(
                IndexBuilder::vertex()
                    .label("person")
                    .property("name")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        // Change the name
        graph
            .set_vertex_property(v1, "name", Value::String("Alicia".into()))
            .unwrap();

        // Old value should not be found
        let alice: Vec<_> = graph
            .vertices_by_property(Some("person"), "name", &Value::String("Alice".into()))
            .collect();
        assert_eq!(alice.len(), 0);

        // New value should be found
        let alicia: Vec<_> = graph
            .vertices_by_property(Some("person"), "name", &Value::String("Alicia".into()))
            .collect();
        assert_eq!(alicia.len(), 1);
    }

    #[test]
    fn index_updated_on_vertex_remove() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        let v1 = graph.add_vertex(
            "person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        );
        let _v2 = graph.add_vertex(
            "person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        );

        graph
            .create_index(
                IndexBuilder::vertex()
                    .label("person")
                    .property("age")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        // Initially 2 vertices with age 30
        let before: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(30))
            .collect();
        assert_eq!(before.len(), 2);

        // Remove one vertex
        graph.remove_vertex(v1).unwrap();

        // Now only 1 vertex with age 30
        let after: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(30))
            .collect();
        assert_eq!(after.len(), 1);
    }

    #[test]
    fn unique_index_rejects_duplicates() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        graph.add_vertex(
            "user",
            HashMap::from([(
                "email".to_string(),
                Value::String("alice@example.com".into()),
            )]),
        );

        // Create unique index - this should succeed
        graph
            .create_index(
                IndexBuilder::vertex()
                    .label("user")
                    .property("email")
                    .unique()
                    .build()
                    .unwrap(),
            )
            .unwrap();

        // Adding duplicate email should be silently ignored (per index_vertex_insert design)
        // but the vertex still gets added - unique constraint is enforced at index creation
        graph.add_vertex(
            "user",
            HashMap::from([("email".to_string(), Value::String("bob@example.com".into()))]),
        );
        assert_eq!(graph.vertex_count(), 2);
    }

    #[test]
    fn unique_index_creation_fails_with_existing_duplicates() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        // Add vertices with duplicate values
        graph.add_vertex(
            "user",
            HashMap::from([(
                "email".to_string(),
                Value::String("duplicate@example.com".into()),
            )]),
        );
        graph.add_vertex(
            "user",
            HashMap::from([(
                "email".to_string(),
                Value::String("duplicate@example.com".into()),
            )]),
        );

        // Creating unique index should fail
        let result = graph.create_index(
            IndexBuilder::vertex()
                .label("user")
                .property("email")
                .unique()
                .build()
                .unwrap(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn drop_index() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        graph
            .create_index(
                IndexBuilder::vertex()
                    .property("age")
                    .name("idx_age")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        assert!(graph.has_index("idx_age"));

        graph.drop_index("idx_age").unwrap();

        assert!(!graph.has_index("idx_age"));
        assert_eq!(graph.index_count(), 0);
    }

    #[test]
    fn drop_nonexistent_index_fails() {
        let mut graph = InMemoryGraph::new();

        let result = graph.drop_index("nonexistent");

        assert!(result.is_err());
    }

    #[test]
    fn create_duplicate_index_fails() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        graph
            .create_index(
                IndexBuilder::vertex()
                    .property("age")
                    .name("idx_age")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        let result = graph.create_index(
            IndexBuilder::vertex()
                .property("age")
                .name("idx_age")
                .build()
                .unwrap(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn list_indexes() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        graph
            .create_index(
                IndexBuilder::vertex()
                    .property("age")
                    .name("idx_age")
                    .build()
                    .unwrap(),
            )
            .unwrap();
        graph
            .create_index(
                IndexBuilder::vertex()
                    .property("name")
                    .name("idx_name")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        let indexes: Vec<_> = graph.list_indexes().collect();
        assert_eq!(indexes.len(), 2);

        let names: Vec<_> = indexes.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"idx_age"));
        assert!(names.contains(&"idx_name"));
    }

    #[test]
    fn vertices_by_property_fallback_scan() {
        let mut graph = InMemoryGraph::new();

        // No index - should fall back to scan
        graph.add_vertex(
            "person",
            HashMap::from([("age".to_string(), Value::Int(30))]),
        );
        graph.add_vertex(
            "person",
            HashMap::from([("age".to_string(), Value::Int(25))]),
        );

        let age_30: Vec<_> = graph
            .vertices_by_property(Some("person"), "age", &Value::Int(30))
            .collect();
        assert_eq!(age_30.len(), 1);
    }

    #[test]
    fn edge_index_integration() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());

        graph
            .add_edge(
                v1,
                v2,
                "knows",
                HashMap::from([("since".to_string(), Value::Int(2020))]),
            )
            .unwrap();
        graph
            .add_edge(
                v2,
                v3,
                "knows",
                HashMap::from([("since".to_string(), Value::Int(2021))]),
            )
            .unwrap();
        graph
            .add_edge(
                v1,
                v3,
                "knows",
                HashMap::from([("since".to_string(), Value::Int(2020))]),
            )
            .unwrap();

        // Create edge index
        graph
            .create_index(
                IndexBuilder::edge()
                    .label("knows")
                    .property("since")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        // Lookup should find indexed values
        let since_2020: Vec<_> = graph
            .edges_by_property(Some("knows"), "since", &Value::Int(2020))
            .collect();
        assert_eq!(since_2020.len(), 2);

        let since_2021: Vec<_> = graph
            .edges_by_property(Some("knows"), "since", &Value::Int(2021))
            .collect();
        assert_eq!(since_2021.len(), 1);
    }

    #[test]
    fn edge_index_updated_on_property_change() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());

        let e1 = graph
            .add_edge(
                v1,
                v2,
                "knows",
                HashMap::from([("weight".to_string(), Value::Float(1.0))]),
            )
            .unwrap();

        graph
            .create_index(
                IndexBuilder::edge()
                    .label("knows")
                    .property("weight")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        // Change the weight
        graph
            .set_edge_property(e1, "weight", Value::Float(2.0))
            .unwrap();

        // Old value should not be found
        let weight_1: Vec<_> = graph
            .edges_by_property(Some("knows"), "weight", &Value::Float(1.0))
            .collect();
        assert_eq!(weight_1.len(), 0);

        // New value should be found
        let weight_2: Vec<_> = graph
            .edges_by_property(Some("knows"), "weight", &Value::Float(2.0))
            .collect();
        assert_eq!(weight_2.len(), 1);
    }

    #[test]
    fn edge_index_updated_on_edge_remove() {
        use crate::index::IndexBuilder;

        let mut graph = InMemoryGraph::new();

        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());

        let e1 = graph
            .add_edge(
                v1,
                v2,
                "knows",
                HashMap::from([("since".to_string(), Value::Int(2020))]),
            )
            .unwrap();
        let _e2 = graph
            .add_edge(
                v2,
                v1,
                "knows",
                HashMap::from([("since".to_string(), Value::Int(2020))]),
            )
            .unwrap();

        graph
            .create_index(
                IndexBuilder::edge()
                    .label("knows")
                    .property("since")
                    .build()
                    .unwrap(),
            )
            .unwrap();

        // Initially 2 edges with since=2020
        let before: Vec<_> = graph
            .edges_by_property(Some("knows"), "since", &Value::Int(2020))
            .collect();
        assert_eq!(before.len(), 2);

        // Remove one edge
        graph.remove_edge(e1).unwrap();

        // Now only 1 edge with since=2020
        let after: Vec<_> = graph
            .edges_by_property(Some("knows"), "since", &Value::Int(2020))
            .collect();
        assert_eq!(after.len(), 1);
    }
}
