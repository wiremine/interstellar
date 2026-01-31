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
//! | [`Graph`] | COW graph with structural sharing | Development, production in-memory |
//! | [`PersistentGraph`] | Memory-mapped persistent storage | Production, large graphs (requires `mmap` feature) |
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
//! │ GraphSnapshot   │      │ PersistentGraph │
//! └─────────────────┘      └─────────────────┘
//! ```
//!
//! # Example
//!
//! ```
//! use interstellar::storage::{Graph, GraphStorage};
//! use std::collections::HashMap;
//!
//! let graph = Graph::new();
//!
//! // Add vertices via Graph methods
//! let alice = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), "Alice".into()),
//! ]));
//! let bob = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), "Bob".into()),
//! ]));
//! graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
//!
//! // Query via snapshot
//! let snapshot = graph.snapshot();
//! assert_eq!(snapshot.vertex_count(), 2);
//! assert_eq!(snapshot.edge_count(), 1);
//! ```

use std::collections::HashMap;
use std::sync::Arc;

pub mod cow;
pub mod interner;

#[cfg(feature = "mmap")]
pub mod mmap;

#[cfg(feature = "mmap")]
pub mod cow_mmap;

// Re-export primary types (new names)
pub use cow::{BatchContext, BatchError, Graph, GraphMutWrapper, GraphSnapshot, GraphState};

pub use interner::StringInterner;

#[cfg(feature = "mmap")]
pub use mmap::MmapGraph;

#[cfg(feature = "mmap")]
pub use cow_mmap::{
    BatchError as CowMmapBatchError, CowMmapBatchContext, CowMmapGraph, CowMmapSnapshot,
};

/// Persistent graph with Copy-on-Write semantics and mmap storage.
///
/// This is the recommended graph type for persistent storage. It provides:
/// - Durable storage to disk via memory-mapped files
/// - O(1) snapshot creation via COW layer
/// - Full traversal and GQL API support
///
/// Requires the `mmap` feature.
#[cfg(feature = "mmap")]
pub type PersistentGraph = CowMmapGraph;

/// Immutable snapshot of a persistent graph.
///
/// Snapshots are created via [`PersistentGraph::snapshot()`](CowMmapGraph::snapshot) and
/// provide a consistent, point-in-time view of the graph. They support the full
/// traversal and GQL API.
///
/// Requires the `mmap` feature.
#[cfg(feature = "mmap")]
pub type PersistentSnapshot = CowMmapSnapshot;

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
/// use interstellar::storage::{Graph, GraphStorage, Vertex};
/// use std::collections::HashMap;
///
/// let graph = Graph::new();
/// let id = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), "Alice".into()),
///     ("age".to_string(), 30i64.into()),
/// ]));
///
/// let snapshot = graph.snapshot();
/// let vertex: Vertex = snapshot.get_vertex(id).unwrap();
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
/// use interstellar::storage::{Graph, GraphStorage, Edge};
/// use std::collections::HashMap;
///
/// let graph = Graph::new();
/// let alice = graph.add_vertex("person", HashMap::new());
/// let bob = graph.add_vertex("person", HashMap::new());
/// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::from([
///     ("since".to_string(), 2020i64.into()),
/// ])).unwrap();
///
/// let snapshot = graph.snapshot();
/// let edge: Edge = snapshot.get_edge(edge_id).unwrap();
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
/// It provides read-only access to graph data; mutation is handled through
/// the [`Graph`] container's mutation API.
///
/// # Thread Safety
///
/// All implementations must be `Send + Sync` to allow concurrent read access.
/// Write access requires using [`Graph`] methods directly or [`GraphMutWrapper`].
///
/// # Implementors
///
/// - [`GraphSnapshot`]: In-memory COW snapshot
/// - `PersistentSnapshot`: Memory-mapped persistent storage (requires `mmap` feature)
///
/// # Example
///
/// ```
/// use interstellar::storage::{Graph, GraphStorage};
/// use std::collections::HashMap;
///
/// fn count_friends<S: GraphStorage>(storage: &S, person_label: &str) -> usize {
///     storage.vertices_with_label(person_label).count()
/// }
///
/// let graph = Graph::new();
/// graph.add_vertex("person", HashMap::new());
/// graph.add_vertex("person", HashMap::new());
/// graph.add_vertex("software", HashMap::new());
///
/// let snapshot = graph.snapshot();
/// assert_eq!(count_friends(&snapshot, "person"), 2);
/// ```
pub trait GraphStorage: Send + Sync {
    /// Retrieves a vertex by its ID.
    ///
    /// Returns `None` if no vertex with the given ID exists.
    ///
    /// # Complexity
    ///
    /// O(1) for all backends.
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
    /// O(1) for all backends.
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
    /// O(out_degree) for all backends.
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
    /// O(in_degree) for all backends.
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
    /// Uses RoaringTreemap for efficient label indexing.
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
    /// Uses RoaringTreemap for efficient label indexing.
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
    /// use interstellar::storage::{Graph, GraphStorage};
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// graph.add_vertex("person", HashMap::new());
    ///
    /// let snapshot = graph.snapshot();
    /// let interner = snapshot.interner();
    /// assert_eq!(interner.lookup("person"), Some(0));
    /// assert_eq!(interner.lookup("unknown"), None);
    /// ```
    fn interner(&self) -> &StringInterner;

    // =========================================================================
    // Index-Aware Methods (Optional - Default to Full Scans)
    // =========================================================================

    /// Returns whether this storage supports property indexes.
    ///
    /// Override this in storage backends that support indexing.
    fn supports_indexes(&self) -> bool {
        false
    }

    /// Lookup vertices by property value, using indexes if available.
    ///
    /// This method attempts to use an index for O(log n) or O(1) lookup.
    /// If no applicable index exists, it falls back to O(n) full scan.
    ///
    /// # Arguments
    ///
    /// * `label` - Optional label filter
    /// * `property` - Property key to match
    /// * `value` - Property value to find
    ///
    /// # Returns
    ///
    /// Iterator of matching vertices.
    fn vertices_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        // Default implementation: full scan with filter
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

    /// Lookup edges by property value, using indexes if available.
    ///
    /// This method attempts to use an index for O(log n) or O(1) lookup.
    /// If no applicable index exists, it falls back to O(n) full scan.
    ///
    /// # Arguments
    ///
    /// * `label` - Optional label filter
    /// * `property` - Property key to match
    /// * `value` - Property value to find
    ///
    /// # Returns
    ///
    /// Iterator of matching edges.
    fn edges_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Edge> + '_> {
        // Default implementation: full scan with filter
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
    /// This method attempts to use a B+ tree index for O(log n) range lookup.
    /// If no applicable index exists, it falls back to O(n) full scan.
    ///
    /// # Arguments
    ///
    /// * `label` - Optional label filter
    /// * `property` - Property key to match
    /// * `start` - Start bound of the range
    /// * `end` - End bound of the range
    ///
    /// # Returns
    ///
    /// Iterator of matching vertices.
    fn vertices_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        use std::ops::Bound;

        // Default implementation: full scan with range filter
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
/// use interstellar::storage::{Graph, GraphStorage, GraphStorageMut};
/// use interstellar::Value;
/// use std::collections::HashMap;
///
/// let graph = Graph::new();
/// let mut storage = graph.as_storage_mut();
///
/// // Create a vertex
/// let id = storage.add_vertex("person", HashMap::from([
///     ("name".to_string(), Value::String("Alice".into())),
/// ]));
///
/// // Update a property
/// storage.set_vertex_property(id, "age", Value::Int(30)).unwrap();
///
/// // Verify the update
/// let vertex = storage.get_vertex(id).unwrap();
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
    /// use interstellar::storage::{Graph, GraphStorage, GraphStorageMut};
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let mut storage = graph.as_storage_mut();
    /// let id = storage.add_vertex("person", HashMap::new());
    /// assert!(storage.get_vertex(id).is_some());
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
    /// use interstellar::storage::{Graph, GraphStorageMut};
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let mut storage = graph.as_storage_mut();
    /// let alice = storage.add_vertex("person", HashMap::new());
    /// let bob = storage.add_vertex("person", HashMap::new());
    /// let edge = storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
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
    /// use interstellar::storage::{Graph, GraphStorage, GraphStorageMut};
    /// use interstellar::Value;
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let mut storage = graph.as_storage_mut();
    /// let id = storage.add_vertex("person", HashMap::new());
    ///
    /// // Add a new property
    /// storage.set_vertex_property(id, "name", Value::String("Alice".into())).unwrap();
    ///
    /// // Update existing property
    /// storage.set_vertex_property(id, "name", Value::String("Alicia".into())).unwrap();
    ///
    /// let vertex = storage.get_vertex(id).unwrap();
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
    /// use interstellar::storage::{Graph, GraphStorage, GraphStorageMut};
    /// use interstellar::Value;
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let mut storage = graph.as_storage_mut();
    /// let alice = storage.add_vertex("person", HashMap::new());
    /// let bob = storage.add_vertex("person", HashMap::new());
    /// let edge_id = storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// storage.set_edge_property(edge_id, "since", Value::Int(2020)).unwrap();
    ///
    /// let edge = storage.get_edge(edge_id).unwrap();
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
    /// use interstellar::storage::{Graph, GraphStorage, GraphStorageMut};
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let mut storage = graph.as_storage_mut();
    /// let alice = storage.add_vertex("person", HashMap::new());
    /// let bob = storage.add_vertex("person", HashMap::new());
    /// storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// // Removing alice also removes the "knows" edge
    /// storage.remove_vertex(alice).unwrap();
    ///
    /// assert_eq!(storage.vertex_count(), 1);
    /// assert_eq!(storage.edge_count(), 0);
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
    /// use interstellar::storage::{Graph, GraphStorage, GraphStorageMut};
    /// use std::collections::HashMap;
    ///
    /// let graph = Graph::new();
    /// let mut storage = graph.as_storage_mut();
    /// let alice = storage.add_vertex("person", HashMap::new());
    /// let bob = storage.add_vertex("person", HashMap::new());
    /// let edge_id = storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// storage.remove_edge(edge_id).unwrap();
    ///
    /// assert_eq!(storage.edge_count(), 0);
    /// ```
    fn remove_edge(&mut self, id: EdgeId) -> Result<(), StorageError>;
}

// =============================================================================
// Blanket impl: GraphStorage for Arc<T> where T: GraphStorage
// =============================================================================

/// Blanket implementation for Arc-wrapped storage types.
///
/// This enables `Arc<dyn GraphStorage>` and `Arc<T>` where `T: GraphStorage`
/// to be used anywhere `GraphStorage` is required.
impl<T: GraphStorage + ?Sized> GraphStorage for Arc<T> {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        (**self).get_vertex(id)
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        (**self).get_edge(id)
    }

    fn vertex_count(&self) -> u64 {
        (**self).vertex_count()
    }

    fn edge_count(&self) -> u64 {
        (**self).edge_count()
    }

    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        (**self).all_vertices()
    }

    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        (**self).all_edges()
    }

    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        (**self).out_edges(vertex)
    }

    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        (**self).in_edges(vertex)
    }

    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_> {
        (**self).vertices_with_label(label)
    }

    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_> {
        (**self).edges_with_label(label)
    }

    fn interner(&self) -> &StringInterner {
        (**self).interner()
    }

    fn vertices_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        (**self).vertices_by_property(label, property, value)
    }

    fn edges_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Edge> + '_> {
        (**self).edges_by_property(label, property, value)
    }

    fn vertices_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: std::ops::Bound<&Value>,
        end: std::ops::Bound<&Value>,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        (**self).vertices_by_property_range(label, property, start, end)
    }
}

// =============================================================================
// StreamableStorage - Owned iterator methods for true O(1) streaming
// =============================================================================

/// Extension trait for storage backends that support streaming iteration.
///
/// Unlike [`GraphStorage`] which returns borrowed iterators tied to `&self`,
/// `StreamableStorage` returns owned (`'static`) iterators by cloning internal
/// Arc-wrapped state. This enables true streaming in [`StreamingExecutor`](crate::traversal::StreamingExecutor)
/// without upfront collection.
///
/// # Problem Solved
///
/// `GraphStorage::all_vertices()` returns `Box<dyn Iterator<Item = Vertex> + '_>`,
/// which is tied to the `&self` lifetime. To return an iterator from a function
/// (like `StreamingExecutor::build_source`), we need `'static`, which requires ownership.
///
/// Without this trait, the streaming executor must collect all IDs upfront:
///
/// ```ignore
/// // Current problem: O(V) memory even for `take(1)`
/// let ids: Vec<_> = storage.all_vertices().map(|v| v.id).collect();
/// Box::new(ids.into_iter().map(...))
/// ```
///
/// With `StreamableStorage`, the API supports true streaming iteration (though the
/// initial implementation may still collect internally):
///
/// ```ignore
/// // True O(1) streaming API
/// storage.stream_all_vertices()
/// ```
///
/// # Implementation Notes
///
/// Methods use `&self` and return `'static` iterators. Implementations should
/// clone internal Arc-wrapped state into the returned iterator. For example,
/// `GraphSnapshot` clones its `Arc<GraphState>` which is O(1).
///
/// # Default Implementation
///
/// The default implementations fall back to collecting from `GraphStorage` methods.
/// Backends can override these for true streaming behavior.
///
/// # Example
///
/// ```ignore
/// use interstellar::storage::StreamableStorage;
///
/// let snapshot = graph.snapshot();
///
/// // Streaming iteration
/// let first_10: Vec<_> = snapshot.stream_all_vertices()
///     .take(10)
///     .collect();
/// ```
pub trait StreamableStorage: GraphStorage + 'static {
    /// Stream all vertex IDs without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects all vertex IDs upfront (O(V) memory). Override for true streaming.
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let ids: Vec<_> = self.all_vertices().map(|v| v.id).collect();
        Box::new(ids.into_iter())
    }

    /// Stream all edge IDs without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects all edge IDs upfront (O(E) memory). Override for true streaming.
    fn stream_all_edges(&self) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let ids: Vec<_> = self.all_edges().map(|e| e.id).collect();
        Box::new(ids.into_iter())
    }

    /// Stream vertex IDs with a given label without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects matching vertex IDs upfront. Override for true streaming.
    fn stream_vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let ids: Vec<_> = self.vertices_with_label(label).map(|v| v.id).collect();
        Box::new(ids.into_iter())
    }

    /// Stream edge IDs with a given label without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects matching edge IDs upfront. Override for true streaming.
    fn stream_edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let ids: Vec<_> = self.edges_with_label(label).map(|e| e.id).collect();
        Box::new(ids.into_iter())
    }

    /// Stream outgoing edge IDs from a vertex without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects edge IDs upfront. Override for true streaming.
    fn stream_out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let ids: Vec<_> = self.out_edges(vertex).map(|e| e.id).collect();
        Box::new(ids.into_iter())
    }

    /// Stream incoming edge IDs to a vertex without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects edge IDs upfront. Override for true streaming.
    fn stream_in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let ids: Vec<_> = self.in_edges(vertex).map(|e| e.id).collect();
        Box::new(ids.into_iter())
    }

    // =========================================================================
    // Neighbor Streaming (for navigation steps)
    // =========================================================================

    /// Stream outgoing neighbor vertex IDs without collecting.
    ///
    /// This is the primary method used by navigation steps (`out()`, `out("label")`).
    /// Returns target vertex IDs for outgoing edges, optionally filtered by label.
    ///
    /// # Arguments
    ///
    /// * `vertex` - Source vertex ID
    /// * `label_ids` - Label IDs to filter by (empty = all labels)
    ///
    /// # Default Implementation
    ///
    /// Collects neighbor IDs upfront. Override for true streaming.
    fn stream_out_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let label_ids_owned: Vec<u32> = label_ids.to_vec();
        let interner = self.interner().clone();
        let neighbors: Vec<_> = self
            .out_edges(vertex)
            .filter(move |e| {
                if label_ids_owned.is_empty() {
                    true
                } else {
                    label_ids_owned
                        .iter()
                        .any(|&lid| interner.lookup(&e.label) == Some(lid))
                }
            })
            .map(|e| e.dst)
            .collect();
        Box::new(neighbors.into_iter())
    }

    /// Stream incoming neighbor vertex IDs without collecting.
    ///
    /// This is the primary method used by navigation steps (`in_()`, `in_("label")`).
    /// Returns source vertex IDs for incoming edges, optionally filtered by label.
    ///
    /// # Default Implementation
    ///
    /// Collects neighbor IDs upfront. Override for true streaming.
    fn stream_in_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let label_ids_owned: Vec<u32> = label_ids.to_vec();
        let interner = self.interner().clone();
        let neighbors: Vec<_> = self
            .in_edges(vertex)
            .filter(move |e| {
                if label_ids_owned.is_empty() {
                    true
                } else {
                    label_ids_owned
                        .iter()
                        .any(|&lid| interner.lookup(&e.label) == Some(lid))
                }
            })
            .map(|e| e.src)
            .collect();
        Box::new(neighbors.into_iter())
    }

    /// Stream both incoming and outgoing neighbor vertex IDs.
    ///
    /// Used by `both()` navigation step.
    ///
    /// # Default Implementation
    ///
    /// Chains `stream_out_neighbors` and `stream_in_neighbors`.
    fn stream_both_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let out_iter = self.stream_out_neighbors(vertex, label_ids);
        let in_iter = self.stream_in_neighbors(vertex, label_ids);
        Box::new(out_iter.chain(in_iter))
    }
}

// Blanket implementation: any Arc<T> where T: StreamableStorage is also StreamableStorage
impl<T: StreamableStorage + ?Sized> StreamableStorage for Arc<T> {
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        (**self).stream_all_vertices()
    }

    fn stream_all_edges(&self) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        (**self).stream_all_edges()
    }

    fn stream_vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = VertexId> + Send> {
        (**self).stream_vertices_with_label(label)
    }

    fn stream_edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        (**self).stream_edges_with_label(label)
    }

    fn stream_out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        (**self).stream_out_edges(vertex)
    }

    fn stream_in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        (**self).stream_in_edges(vertex)
    }

    fn stream_out_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        (**self).stream_out_neighbors(vertex, label_ids)
    }

    fn stream_in_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        (**self).stream_in_neighbors(vertex, label_ids)
    }

    fn stream_both_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        (**self).stream_both_neighbors(vertex, label_ids)
    }
}
