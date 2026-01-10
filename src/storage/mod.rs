//! Storage backends for graph data.
//!
//! This module defines the [`GraphStorage`] trait and provides concrete implementations
//! for storing graph data. The storage layer is responsible for:
//!
//! - Storing vertices and edges with their labels and properties
//! - Providing efficient lookups by ID
//! - Supporting adjacency traversal (outgoing/incoming edges)
//! - Label-based filtering using indexed lookups
//!
//! # Available Backends
//!
//! | Backend | Description | Use Case |
//! |---------|-------------|----------|
//! | [`InMemoryGraph`] | HashMap-based storage | Development, small graphs |
//! | `MmapGraph` | Memory-mapped persistent storage | Production, large graphs (requires `mmap` feature) |
//!
//! # Architecture
//!
//! All storage backends implement the [`GraphStorage`] trait, which provides a unified
//! interface for the traversal engine. This allows the same traversal code to work
//! with any backend.
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │              Traversal Engine               │
//! └─────────────────────────────────────────────┘
//!                       │
//!                       ▼
//! ┌─────────────────────────────────────────────┐
//! │            GraphStorage trait               │
//! └─────────────────────────────────────────────┘
//!                       │
//!          ┌────────────┴────────────┐
//!          ▼                         ▼
//! ┌─────────────────┐      ┌─────────────────┐
//! │  InMemoryGraph  │      │   MmapGraph     │
//! └─────────────────┘      └─────────────────┘
//! ```
//!
//! # Example
//!
//! ```
//! use rustgremlin::storage::{GraphStorage, InMemoryGraph};
//! use std::collections::HashMap;
//!
//! let mut graph = InMemoryGraph::new();
//!
//! // Add vertices
//! let alice = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), "Alice".into()),
//! ]));
//! let bob = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), "Bob".into()),
//! ]));
//!
//! // Add edge
//! graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
//!
//! // Query via GraphStorage trait
//! assert_eq!(graph.vertex_count(), 2);
//! assert_eq!(graph.edge_count(), 1);
//! ```

use std::collections::HashMap;

pub mod inmemory;
pub mod interner;

#[cfg(feature = "mmap")]
pub mod mmap;

pub use inmemory::InMemoryGraph;
pub use interner::StringInterner;

#[cfg(feature = "mmap")]
pub use mmap::MmapGraph;

use crate::error::StorageError;
use crate::value::{EdgeId, Value, VertexId};

/// A vertex in the graph with its label and properties.
///
/// This is the external representation of a vertex returned by [`GraphStorage`] methods.
/// It contains resolved (non-interned) strings for ease of use.
///
/// # Fields
///
/// - `id`: Unique identifier for this vertex
/// - `label`: The vertex's label (e.g., "person", "software")
/// - `properties`: Key-value pairs of vertex properties
///
/// # Example
///
/// ```
/// use rustgremlin::storage::{GraphStorage, InMemoryGraph, Vertex};
/// use std::collections::HashMap;
///
/// let mut graph = InMemoryGraph::new();
/// let id = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), "Alice".into()),
///     ("age".to_string(), 30.into()),
/// ]));
///
/// let vertex: Vertex = graph.get_vertex(id).unwrap();
/// assert_eq!(vertex.label, "person");
/// assert_eq!(vertex.properties.get("name").unwrap().as_str(), Some("Alice"));
/// ```
#[derive(Clone, Debug)]
pub struct Vertex {
    /// Unique identifier for this vertex.
    pub id: VertexId,
    /// The vertex's label (e.g., "person", "software").
    pub label: String,
    /// Key-value pairs of vertex properties.
    pub properties: HashMap<String, Value>,
}

/// An edge in the graph connecting two vertices.
///
/// This is the external representation of an edge returned by [`GraphStorage`] methods.
/// Edges are directed: they have a source vertex (`src`) and destination vertex (`dst`).
///
/// # Fields
///
/// - `id`: Unique identifier for this edge
/// - `label`: The edge's label (e.g., "knows", "created")
/// - `src`: Source vertex ID (where the edge starts)
/// - `dst`: Destination vertex ID (where the edge ends)
/// - `properties`: Key-value pairs of edge properties
///
/// # Example
///
/// ```
/// use rustgremlin::storage::{GraphStorage, InMemoryGraph, Edge};
/// use std::collections::HashMap;
///
/// let mut graph = InMemoryGraph::new();
/// let alice = graph.add_vertex("person", HashMap::new());
/// let bob = graph.add_vertex("person", HashMap::new());
/// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::from([
///     ("since".to_string(), 2020.into()),
/// ])).unwrap();
///
/// let edge: Edge = graph.get_edge(edge_id).unwrap();
/// assert_eq!(edge.label, "knows");
/// assert_eq!(edge.src, alice);
/// assert_eq!(edge.dst, bob);
/// ```
#[derive(Clone, Debug)]
pub struct Edge {
    /// Unique identifier for this edge.
    pub id: EdgeId,
    /// The edge's label (e.g., "knows", "created").
    pub label: String,
    /// Source vertex ID (where the edge originates).
    pub src: VertexId,
    /// Destination vertex ID (where the edge points to).
    pub dst: VertexId,
    /// Key-value pairs of edge properties.
    pub properties: HashMap<String, Value>,
}

/// Trait for graph storage backends.
///
/// This trait defines the interface that all storage backends must implement.
/// It provides read-only access to graph data; mutation is handled separately
/// by each backend's specific methods (e.g., [`InMemoryGraph::add_vertex`]).
///
/// # Thread Safety
///
/// All implementations must be `Send + Sync` to allow concurrent read access.
/// Write access requires external synchronization (provided by [`Graph`](crate::Graph)).
///
/// # Implementors
///
/// - [`InMemoryGraph`]: HashMap-based in-memory storage
/// - `MmapGraph`: Memory-mapped persistent storage (requires `mmap` feature)
///
/// # Example
///
/// ```
/// use rustgremlin::storage::{GraphStorage, InMemoryGraph};
/// use std::collections::HashMap;
///
/// fn count_friends<S: GraphStorage>(storage: &S, person_label: &str) -> usize {
///     storage.vertices_with_label(person_label).count()
/// }
///
/// let mut graph = InMemoryGraph::new();
/// graph.add_vertex("person", HashMap::new());
/// graph.add_vertex("person", HashMap::new());
/// graph.add_vertex("software", HashMap::new());
///
/// assert_eq!(count_friends(&graph, "person"), 2);
/// ```
pub trait GraphStorage: Send + Sync {
    /// Retrieves a vertex by its ID.
    ///
    /// Returns `None` if no vertex with the given ID exists.
    ///
    /// # Complexity
    ///
    /// O(1) for [`InMemoryGraph`].
    fn get_vertex(&self, id: VertexId) -> Option<Vertex>;

    /// Returns the total number of vertices in the graph.
    ///
    /// # Complexity
    ///
    /// O(1) for all backends.
    fn vertex_count(&self) -> u64;

    /// Retrieves an edge by its ID.
    ///
    /// Returns `None` if no edge with the given ID exists.
    ///
    /// # Complexity
    ///
    /// O(1) for [`InMemoryGraph`].
    fn get_edge(&self, id: EdgeId) -> Option<Edge>;

    /// Returns the total number of edges in the graph.
    ///
    /// # Complexity
    ///
    /// O(1) for all backends.
    fn edge_count(&self) -> u64;

    /// Returns an iterator over all outgoing edges from a vertex.
    ///
    /// Outgoing edges are edges where the given vertex is the source (`src`).
    ///
    /// # Arguments
    ///
    /// * `vertex` - The source vertex ID
    ///
    /// # Returns
    ///
    /// An iterator yielding edges. Returns an empty iterator if the vertex
    /// doesn't exist or has no outgoing edges.
    ///
    /// # Complexity
    ///
    /// O(out_degree) for [`InMemoryGraph`].
    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;

    /// Returns an iterator over all incoming edges to a vertex.
    ///
    /// Incoming edges are edges where the given vertex is the destination (`dst`).
    ///
    /// # Arguments
    ///
    /// * `vertex` - The destination vertex ID
    ///
    /// # Returns
    ///
    /// An iterator yielding edges. Returns an empty iterator if the vertex
    /// doesn't exist or has no incoming edges.
    ///
    /// # Complexity
    ///
    /// O(in_degree) for [`InMemoryGraph`].
    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;

    /// Returns an iterator over all vertices with a given label.
    ///
    /// # Arguments
    ///
    /// * `label` - The label to filter by (e.g., "person", "software")
    ///
    /// # Returns
    ///
    /// An iterator yielding vertices with the specified label.
    /// Returns an empty iterator if no vertices have this label.
    ///
    /// # Complexity
    ///
    /// O(n) where n = number of vertices with the label.
    /// Uses RoaringBitmap for efficient label indexing.
    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_>;

    /// Returns an iterator over all edges with a given label.
    ///
    /// # Arguments
    ///
    /// * `label` - The label to filter by (e.g., "knows", "created")
    ///
    /// # Returns
    ///
    /// An iterator yielding edges with the specified label.
    /// Returns an empty iterator if no edges have this label.
    ///
    /// # Complexity
    ///
    /// O(m) where m = number of edges with the label.
    /// Uses RoaringBitmap for efficient label indexing.
    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_>;

    /// Returns an iterator over all vertices in the graph.
    ///
    /// The iteration order is not guaranteed to be stable.
    ///
    /// # Complexity
    ///
    /// O(V) where V = total number of vertices.
    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_>;

    /// Returns an iterator over all edges in the graph.
    ///
    /// The iteration order is not guaranteed to be stable.
    ///
    /// # Complexity
    ///
    /// O(E) where E = total number of edges.
    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_>;

    /// Returns a reference to the string interner for label resolution.
    ///
    /// The string interner maps label strings to compact integer IDs for
    /// efficient storage and comparison. This is primarily used internally
    /// by the traversal engine for label filtering.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, InMemoryGraph};
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// graph.add_vertex("person", HashMap::new());
    ///
    /// let interner = graph.interner();
    /// assert_eq!(interner.lookup("person"), Some(0));
    /// assert_eq!(interner.lookup("unknown"), None);
    /// ```
    fn interner(&self) -> &StringInterner;
}

/// Trait for mutable graph storage operations.
///
/// This trait extends storage backends with mutation capabilities, allowing
/// creation, modification, and deletion of vertices and edges.
///
/// # Separation from GraphStorage
///
/// `GraphStorageMut` is a separate trait from [`GraphStorage`] to allow
/// read-only access patterns (using `GraphStorage` alone) while still
/// supporting mutation when needed. Most traversal operations only need
/// read access.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync`. However, mutation methods require
/// `&mut self`, so external synchronization (via [`Graph`](crate::Graph))
/// is needed for concurrent write access.
///
/// # Example
///
/// ```
/// use rustgremlin::storage::{GraphStorage, GraphStorageMut, InMemoryGraph};
/// use rustgremlin::Value;
/// use std::collections::HashMap;
///
/// let mut graph = InMemoryGraph::new();
///
/// // Create a vertex
/// let id = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), Value::String("Alice".into())),
/// ]));
///
/// // Update a property
/// graph.set_vertex_property(id, "age", Value::Int(30)).unwrap();
///
/// // Verify the update
/// let vertex = graph.get_vertex(id).unwrap();
/// assert_eq!(vertex.properties.get("age"), Some(&Value::Int(30)));
/// ```
pub trait GraphStorageMut: GraphStorage {
    /// Adds a new vertex with the given label and properties.
    ///
    /// # Arguments
    ///
    /// * `label` - The vertex label (e.g., "person", "software")
    /// * `properties` - Initial property key-value pairs
    ///
    /// # Returns
    ///
    /// The [`VertexId`] of the newly created vertex.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, GraphStorageMut, InMemoryGraph};
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// let id = graph.add_vertex("person", HashMap::new());
    /// assert!(graph.get_vertex(id).is_some());
    /// ```
    fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId;

    /// Adds a new edge connecting two vertices.
    ///
    /// # Arguments
    ///
    /// * `src` - Source vertex ID (where the edge starts)
    /// * `dst` - Destination vertex ID (where the edge ends)
    /// * `label` - The edge label (e.g., "knows", "created")
    /// * `properties` - Initial property key-value pairs
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
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorageMut, InMemoryGraph};
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    /// let edge = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    /// ```
    fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError>;

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
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, GraphStorageMut, InMemoryGraph};
    /// use rustgremlin::Value;
    /// use std::collections::HashMap;
    ///
    /// let mut graph = InMemoryGraph::new();
    /// let id = graph.add_vertex("person", HashMap::new());
    ///
    /// // Add a new property
    /// graph.set_vertex_property(id, "name", Value::String("Alice".into())).unwrap();
    ///
    /// // Update existing property
    /// graph.set_vertex_property(id, "name", Value::String("Alicia".into())).unwrap();
    ///
    /// let vertex = graph.get_vertex(id).unwrap();
    /// assert_eq!(vertex.properties.get("name"), Some(&Value::String("Alicia".into())));
    /// ```
    fn set_vertex_property(
        &mut self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError>;

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
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, GraphStorageMut, InMemoryGraph};
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
    fn set_edge_property(
        &mut self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError>;

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
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, GraphStorageMut, InMemoryGraph};
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
    fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError>;

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
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::{GraphStorage, GraphStorageMut, InMemoryGraph};
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
    /// ```
    fn remove_edge(&mut self, id: EdgeId) -> Result<(), StorageError>;
}
