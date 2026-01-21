//! Rich graph element types with graph references.
//!
//! This module provides TinkerPop-style `GraphVertex` and `GraphEdge` types
//! that carry a reference to the graph, enabling property access and traversal
//! directly from returned elements.
//!
//! # Overview
//!
//! Unlike the lightweight [`VertexId`] and [`EdgeId`] types, `GraphVertex` and
//! `GraphEdge` are "live" objects that can:
//!
//! - Access their properties without a separate graph lookup
//! - Spawn traversals from the element
//! - Mutate properties directly
//!
//! # Generic Design
//!
//! Both `GraphVertex<G>` and `GraphEdge<G>` are generic over the graph type `G`,
//! which must implement [`GraphAccess`]. This allows the same types to work with
//! both in-memory (`Arc<Graph>`) and persistent (`Arc<CowMmapGraph>`) storage.
//!
//! For convenience, type aliases are provided:
//!
//! - [`InMemoryVertex`] = `GraphVertex<Arc<Graph>>`
//! - [`InMemoryEdge`] = `GraphEdge<Arc<Graph>>`
//! - [`PersistentVertex`] = `GraphVertex<Arc<CowMmapGraph>>` (requires `mmap` feature)
//! - [`PersistentEdge`] = `GraphEdge<Arc<CowMmapGraph>>` (requires `mmap` feature)
//!
//! # Example
//!
//! ```rust
//! use interstellar::prelude::*;
//! use std::sync::Arc;
//! use std::collections::HashMap;
//!
//! let graph = Arc::new(Graph::new());
//! let alice_id = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), Value::from("Alice")),
//!     ("age".to_string(), Value::from(30i64)),
//! ]));
//!
//! // Create a GraphVertex from an ID
//! use interstellar::graph_elements::GraphVertex;
//! let alice = GraphVertex::new(alice_id, graph.clone());
//!
//! // Access properties directly
//! assert_eq!(alice.label(), Some("person".to_string()));
//! assert_eq!(alice.property("name"), Some(Value::String("Alice".to_string())));
//!
//! // Mutate properties
//! alice.property_set("age", 31i64).unwrap();
//! ```
//!
//! # Thread Safety
//!
//! Both `GraphVertex` and `GraphEdge` are `Clone`, `Send`, and `Sync` when `G` is.
//! Multiple elements can reference the same graph concurrently.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::StorageError;
use crate::graph_access::GraphAccess;
use crate::storage::cow::Graph;
use crate::value::{EdgeId, Value, VertexId};

#[cfg(feature = "mmap")]
use crate::storage::CowMmapGraph;

// =============================================================================
// Type Aliases for Convenience
// =============================================================================

/// A vertex reference for in-memory graphs.
///
/// This is the most common type when using `Graph` (in-memory storage).
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::graph_elements::InMemoryVertex;
/// use std::sync::Arc;
/// use std::collections::HashMap;
///
/// let graph = Arc::new(Graph::new());
/// let id = graph.add_vertex("person", HashMap::new());
///
/// let v: InMemoryVertex = InMemoryVertex::new(id, graph);
/// assert!(v.exists());
/// ```
pub type InMemoryVertex = GraphVertex<Arc<Graph>>;

/// An edge reference for in-memory graphs.
///
/// This is the most common type when using `Graph` (in-memory storage).
pub type InMemoryEdge = GraphEdge<Arc<Graph>>;

/// A vertex reference for persistent mmap graphs.
///
/// Requires the `mmap` feature.
#[cfg(feature = "mmap")]
pub type PersistentVertex = GraphVertex<Arc<CowMmapGraph>>;

/// An edge reference for persistent mmap graphs.
///
/// Requires the `mmap` feature.
#[cfg(feature = "mmap")]
pub type PersistentEdge = GraphEdge<Arc<CowMmapGraph>>;

// =============================================================================
// GraphVertex
// =============================================================================

/// A vertex reference with access to the graph.
///
/// `GraphVertex<G>` provides TinkerPop-style vertex semantics where a vertex
/// object can access its properties and spawn traversals directly.
///
/// Unlike [`VertexId`], which is a lightweight identifier, `GraphVertex`
/// carries a reference to the graph enabling:
///
/// - Direct property access without separate graph lookups
/// - Mutation through the vertex object
/// - Spawning traversals from the vertex
///
/// # Type Parameters
///
/// - `G`: The graph type, typically `Arc<Graph>` or `Arc<CowMmapGraph>`
///
/// # Thread Safety
///
/// `GraphVertex<G>` is `Clone`, `Send`, and `Sync` when `G` is.
/// Multiple vertices can reference the same graph concurrently.
///
/// # Current State vs Snapshot
///
/// `GraphVertex` accesses the **current** graph state, not a snapshot.
/// This means:
///
/// - Property reads see the latest committed values
/// - Mutations are immediately visible to other `GraphVertex` objects
/// - Concurrent modifications are possible (thread-safe)
///
/// If you need snapshot isolation, use `GraphSnapshot` directly.
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::graph_elements::GraphVertex;
/// use std::sync::Arc;
/// use std::collections::HashMap;
///
/// let graph = Arc::new(Graph::new());
/// let id = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), "Alice".into()),
/// ]));
///
/// let v = GraphVertex::new(id, graph.clone());
/// assert_eq!(v.label(), Some("person".to_string()));
/// assert_eq!(v.property("name"), Some(Value::String("Alice".to_string())));
/// ```
#[derive(Clone)]
pub struct GraphVertex<G: GraphAccess> {
    id: VertexId,
    graph: G,
}

impl<G: GraphAccess> GraphVertex<G> {
    /// Create a new GraphVertex.
    ///
    /// This is typically called internally by terminal methods, but can
    /// be used directly when you have a `VertexId` and graph reference.
    ///
    /// # Arguments
    ///
    /// * `id` - The vertex ID
    /// * `graph` - A graph reference implementing `GraphAccess`
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::new());
    /// let v = GraphVertex::new(id, graph.clone());
    /// ```
    pub fn new(id: VertexId, graph: G) -> Self {
        Self { id, graph }
    }

    /// Get the vertex ID.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::new());
    /// let v = GraphVertex::new(id, graph.clone());
    /// assert_eq!(v.id(), id);
    /// ```
    #[inline]
    pub fn id(&self) -> VertexId {
        self.id
    }

    /// Get the vertex label.
    ///
    /// Returns `None` if the vertex no longer exists in the graph.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::new());
    /// let v = GraphVertex::new(id, graph.clone());
    /// assert_eq!(v.label(), Some("person".to_string()));
    /// ```
    pub fn label(&self) -> Option<String> {
        self.graph.get_vertex(self.id).map(|v| v.label)
    }

    /// Get a property value by key.
    ///
    /// Returns `None` if:
    /// - The vertex no longer exists
    /// - The property key doesn't exist
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    /// let v = GraphVertex::new(id, graph.clone());
    ///
    /// assert_eq!(v.property("name"), Some(Value::String("Alice".to_string())));
    /// assert_eq!(v.property("nonexistent"), None);
    /// ```
    pub fn property(&self, key: &str) -> Option<Value> {
        self.graph
            .get_vertex(self.id)
            .and_then(|v| v.properties.get(key).cloned())
    }

    /// Get all properties as a map.
    ///
    /// Returns an empty map if the vertex no longer exists.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    ///     ("age".to_string(), 30i64.into()),
    /// ]));
    /// let v = GraphVertex::new(id, graph.clone());
    ///
    /// let props = v.properties();
    /// assert_eq!(props.len(), 2);
    /// assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
    /// ```
    pub fn properties(&self) -> HashMap<String, Value> {
        self.graph
            .get_vertex(self.id)
            .map(|v| v.properties)
            .unwrap_or_default()
    }

    /// Check if the vertex still exists in the graph.
    ///
    /// A vertex may no longer exist if it was deleted after the `GraphVertex`
    /// was created.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::new());
    /// let v = GraphVertex::new(id, graph.clone());
    ///
    /// assert!(v.exists());
    ///
    /// // After removal, exists() returns false
    /// graph.remove_vertex(id).unwrap();
    /// assert!(!v.exists());
    /// ```
    pub fn exists(&self) -> bool {
        self.graph.get_vertex(self.id).is_some()
    }

    /// Set a property value.
    ///
    /// This mutates the graph directly. The change is immediately visible
    /// to other `GraphVertex` objects and new snapshots.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if the vertex no longer exists.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::from([
    ///     ("age".to_string(), 30i64.into()),
    /// ]));
    /// let v = GraphVertex::new(id, graph.clone());
    ///
    /// // Update the property
    /// v.property_set("age", 31i64).unwrap();
    ///
    /// // Change is immediately visible
    /// assert_eq!(v.property("age"), Some(Value::Int(31)));
    /// ```
    pub fn property_set(&self, key: &str, value: impl Into<Value>) -> Result<(), StorageError> {
        self.graph.set_vertex_property(self.id, key, value.into())
    }

    /// Get a reference to the graph.
    ///
    /// This can be useful for creating new vertices/edges or spawning
    /// new traversals.
    #[inline]
    pub fn graph(&self) -> &G {
        &self.graph
    }

    /// Convert to a lightweight `Value` for serialization or storage.
    ///
    /// This returns `Value::Vertex(id)`, which is just the ID without
    /// the graph reference.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::new());
    /// let v = GraphVertex::new(id, graph.clone());
    ///
    /// assert_eq!(v.to_value(), Value::Vertex(id));
    /// ```
    #[inline]
    pub fn to_value(&self) -> Value {
        Value::Vertex(self.id)
    }

    /// Add an outgoing edge to another vertex.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    ///
    /// let alice_v = GraphVertex::new(alice, graph.clone());
    /// let bob_v = GraphVertex::new(bob, graph.clone());
    ///
    /// let edge = alice_v.add_edge("knows", &bob_v).unwrap();
    /// assert_eq!(edge.label(), Some("knows".to_string()));
    ///
    /// // Verify traversal works
    /// let friends = alice_v.out("knows").to_list();
    /// assert_eq!(friends.len(), 1);
    /// ```
    pub fn add_edge(&self, label: &str, to: &GraphVertex<G>) -> Result<GraphEdge<G>, StorageError> {
        self.add_edge_to_id(label, to.id)
    }

    /// Add an outgoing edge to a vertex by ID.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if either vertex doesn't exist.
    pub fn add_edge_to_id(&self, label: &str, to: VertexId) -> Result<GraphEdge<G>, StorageError> {
        let edge_id = self.graph.add_edge(self.id, to, label, HashMap::new())?;
        Ok(GraphEdge::new(edge_id, self.graph.clone()))
    }

    /// Add an outgoing edge with properties to another vertex.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    ///
    /// let alice_v = GraphVertex::new(alice, graph.clone());
    /// let bob_v = GraphVertex::new(bob, graph.clone());
    ///
    /// let edge = alice_v.add_edge_with_props(
    ///     "knows",
    ///     &bob_v,
    ///     HashMap::from([("since".to_string(), 2020i64.into())])
    /// ).unwrap();
    ///
    /// assert_eq!(edge.property("since"), Some(Value::Int(2020)));
    /// ```
    pub fn add_edge_with_props(
        &self,
        label: &str,
        to: &GraphVertex<G>,
        properties: HashMap<String, Value>,
    ) -> Result<GraphEdge<G>, StorageError> {
        let edge_id = self.graph.add_edge(self.id, to.id, label, properties)?;
        Ok(GraphEdge::new(edge_id, self.graph.clone()))
    }

    /// Remove this vertex from the graph.
    ///
    /// This also removes all incident edges (both incoming and outgoing).
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if the vertex doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let id = graph.add_vertex("person", HashMap::new());
    /// let v = GraphVertex::new(id, graph.clone());
    ///
    /// assert!(v.exists());
    /// v.remove().unwrap();
    /// assert!(!v.exists());
    /// ```
    pub fn remove(&self) -> Result<(), StorageError> {
        self.graph.remove_vertex(self.id)
    }

    /// Traverse to outgoing adjacent vertices with a specific edge label.
    ///
    /// This is the TinkerPop-style `v.out(label)` pattern.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    /// let bob = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Bob".into()),
    /// ]));
    /// graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// let alice_v = GraphVertex::new(alice, graph.clone());
    /// let friends = alice_v.out("knows").to_list();
    /// assert_eq!(friends.len(), 1);
    /// assert_eq!(friends[0].property("name"), Some(Value::String("Bob".to_string())));
    /// ```
    pub fn out(&self, label: &str) -> GraphVertexTraversal<G> {
        GraphVertexTraversal::new(self.graph.clone(), self.id).out_label(label)
    }

    /// Traverse to outgoing adjacent vertices (all labels).
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// let v = GraphVertex::new(a, graph.clone());
    /// let neighbors = v.out_all().to_list();
    /// assert_eq!(neighbors.len(), 1);
    /// ```
    pub fn out_all(&self) -> GraphVertexTraversal<G> {
        GraphVertexTraversal::new(self.graph.clone(), self.id).out()
    }

    /// Traverse to incoming adjacent vertices with a specific edge label.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// let bob_v = GraphVertex::new(bob, graph.clone());
    /// let knowers = bob_v.in_("knows").to_list();
    /// assert_eq!(knowers.len(), 1);
    /// ```
    pub fn in_(&self, label: &str) -> GraphVertexTraversal<G> {
        GraphVertexTraversal::new(self.graph.clone(), self.id).in_label(label)
    }

    /// Traverse to incoming adjacent vertices (all labels).
    pub fn in_all(&self) -> GraphVertexTraversal<G> {
        GraphVertexTraversal::new(self.graph.clone(), self.id).in_step()
    }

    /// Traverse to adjacent vertices in both directions with a specific edge label.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// let alice_v = GraphVertex::new(alice, graph.clone());
    /// let both_neighbors = alice_v.both("knows").to_list();
    /// assert_eq!(both_neighbors.len(), 1);
    /// ```
    pub fn both(&self, label: &str) -> GraphVertexTraversal<G> {
        GraphVertexTraversal::new(self.graph.clone(), self.id).both_label(label)
    }

    /// Traverse to adjacent vertices in both directions (all labels).
    pub fn both_all(&self) -> GraphVertexTraversal<G> {
        GraphVertexTraversal::new(self.graph.clone(), self.id).both_step()
    }
}

impl<G: GraphAccess> std::fmt::Debug for GraphVertex<G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphVertex")
            .field("id", &self.id)
            .field("label", &self.label())
            .finish()
    }
}

impl<G: GraphAccess> PartialEq for GraphVertex<G> {
    fn eq(&self, other: &Self) -> bool {
        // Two GraphVertex are equal if they have the same ID
        // (we don't compare graph references since they might be
        // cloned Arcs pointing to the same graph)
        self.id == other.id
    }
}

impl<G: GraphAccess> Eq for GraphVertex<G> {}

impl<G: GraphAccess> std::hash::Hash for GraphVertex<G> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

// =============================================================================
// GraphEdge
// =============================================================================

/// An edge reference with access to the graph.
///
/// `GraphEdge<G>` provides TinkerPop-style edge semantics where an edge
/// object can access its properties, endpoints, and spawn traversals directly.
///
/// Unlike [`EdgeId`], which is a lightweight identifier, `GraphEdge`
/// carries a reference to the graph enabling:
///
/// - Direct property access without separate graph lookups
/// - Access to source and destination vertices as `GraphVertex<G>` objects
/// - Mutation through the edge object
///
/// # Type Parameters
///
/// - `G`: The graph type, typically `Arc<Graph>` or `Arc<CowMmapGraph>`
///
/// # Thread Safety
///
/// `GraphEdge<G>` is `Clone`, `Send`, and `Sync` when `G` is.
/// Multiple edges can reference the same graph concurrently.
///
/// # Example
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::graph_elements::{GraphVertex, GraphEdge};
/// use std::sync::Arc;
/// use std::collections::HashMap;
///
/// let graph = Arc::new(Graph::new());
/// let alice = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), "Alice".into()),
/// ]));
/// let bob = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), "Bob".into()),
/// ]));
/// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
///
/// let e = GraphEdge::new(edge_id, graph.clone());
/// assert_eq!(e.label(), Some("knows".to_string()));
///
/// // Access endpoints as GraphVertex objects
/// let src = e.out_v().unwrap();
/// assert_eq!(src.property("name"), Some(Value::String("Alice".to_string())));
/// ```
#[derive(Clone)]
pub struct GraphEdge<G: GraphAccess> {
    id: EdgeId,
    graph: G,
}

impl<G: GraphAccess> GraphEdge<G> {
    /// Create a new GraphEdge.
    ///
    /// This is typically called internally by terminal methods, but can
    /// be used directly when you have an `EdgeId` and graph reference.
    pub fn new(id: EdgeId, graph: G) -> Self {
        Self { id, graph }
    }

    /// Get the edge ID.
    #[inline]
    pub fn id(&self) -> EdgeId {
        self.id
    }

    /// Get the edge label.
    ///
    /// Returns `None` if the edge no longer exists in the graph.
    pub fn label(&self) -> Option<String> {
        self.graph.get_edge(self.id).map(|e| e.label)
    }

    /// Get the source (outgoing) vertex.
    ///
    /// In Gremlin terminology, this is the "out" vertex - the vertex
    /// from which the edge originates.
    ///
    /// Returns `None` if the edge no longer exists.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphEdge;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    /// let bob = graph.add_vertex("person", HashMap::new());
    /// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// let e = GraphEdge::new(edge_id, graph.clone());
    /// let src = e.out_v().unwrap();
    /// assert_eq!(src.property("name"), Some(Value::String("Alice".to_string())));
    /// ```
    pub fn out_v(&self) -> Option<GraphVertex<G>> {
        self.graph
            .get_edge(self.id)
            .map(|e| GraphVertex::new(e.src, self.graph.clone()))
    }

    /// Get the destination (incoming) vertex.
    ///
    /// In Gremlin terminology, this is the "in" vertex - the vertex
    /// to which the edge points.
    ///
    /// Returns `None` if the edge no longer exists.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphEdge;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Bob".into()),
    /// ]));
    /// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// let e = GraphEdge::new(edge_id, graph.clone());
    /// let dst = e.in_v().unwrap();
    /// assert_eq!(dst.property("name"), Some(Value::String("Bob".to_string())));
    /// ```
    pub fn in_v(&self) -> Option<GraphVertex<G>> {
        self.graph
            .get_edge(self.id)
            .map(|e| GraphVertex::new(e.dst, self.graph.clone()))
    }

    /// Get both endpoint vertices as (out, in) tuple.
    ///
    /// Returns `None` if the edge no longer exists.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphEdge;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    /// let bob = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Bob".into()),
    /// ]));
    /// let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// let e = GraphEdge::new(edge_id, graph.clone());
    /// let (src, dst) = e.both_v().unwrap();
    /// assert_eq!(src.property("name"), Some(Value::String("Alice".to_string())));
    /// assert_eq!(dst.property("name"), Some(Value::String("Bob".to_string())));
    /// ```
    pub fn both_v(&self) -> Option<(GraphVertex<G>, GraphVertex<G>)> {
        self.graph.get_edge(self.id).map(|e| {
            (
                GraphVertex::new(e.src, self.graph.clone()),
                GraphVertex::new(e.dst, self.graph.clone()),
            )
        })
    }

    /// Get a property value by key.
    ///
    /// Returns `None` if:
    /// - The edge no longer exists
    /// - The property key doesn't exist
    pub fn property(&self, key: &str) -> Option<Value> {
        self.graph
            .get_edge(self.id)
            .and_then(|e| e.properties.get(key).cloned())
    }

    /// Get all properties as a map.
    ///
    /// Returns an empty map if the edge no longer exists.
    pub fn properties(&self) -> HashMap<String, Value> {
        self.graph
            .get_edge(self.id)
            .map(|e| e.properties)
            .unwrap_or_default()
    }

    /// Set a property value.
    ///
    /// This mutates the graph directly. The change is immediately visible
    /// to other `GraphEdge` objects and new snapshots.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::EdgeNotFound` if the edge no longer exists.
    pub fn property_set(&self, key: &str, value: impl Into<Value>) -> Result<(), StorageError> {
        self.graph.set_edge_property(self.id, key, value.into())
    }

    /// Check if the edge still exists in the graph.
    ///
    /// An edge may no longer exist if it was deleted after the `GraphEdge`
    /// was created, or if either endpoint vertex was deleted.
    pub fn exists(&self) -> bool {
        self.graph.get_edge(self.id).is_some()
    }

    /// Get a reference to the graph.
    #[inline]
    pub fn graph(&self) -> &G {
        &self.graph
    }

    /// Convert to a lightweight `Value` for serialization or storage.
    ///
    /// This returns `Value::Edge(id)`, which is just the ID without
    /// the graph reference.
    #[inline]
    pub fn to_value(&self) -> Value {
        Value::Edge(self.id)
    }

    /// Remove this edge from the graph.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::EdgeNotFound` if the edge doesn't exist.
    pub fn remove(&self) -> Result<(), StorageError> {
        self.graph.remove_edge(self.id)
    }
}

impl<G: GraphAccess> std::fmt::Debug for GraphEdge<G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphEdge")
            .field("id", &self.id)
            .field("label", &self.label())
            .finish()
    }
}

impl<G: GraphAccess> PartialEq for GraphEdge<G> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<G: GraphAccess> Eq for GraphEdge<G> {}

impl<G: GraphAccess> std::hash::Hash for GraphEdge<G> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

// =============================================================================
// GraphVertexTraversal
// =============================================================================

/// Traversal step types for `GraphVertexTraversal`.
#[derive(Clone, Debug)]
enum TraversalStep {
    /// Navigate to outgoing neighbors (optionally filtered by label)
    Out(Option<String>),
    /// Navigate to incoming neighbors (optionally filtered by label)
    In(Option<String>),
    /// Navigate to neighbors in both directions (optionally filtered by label)
    Both(Option<String>),
    /// Filter by vertex label
    HasLabel(String),
    /// Filter by property value
    HasValue(String, Value),
}

/// A traversal builder starting from a specific vertex.
///
/// `GraphVertexTraversal<G>` provides a fluent API for traversing the graph
/// starting from a `GraphVertex<G>`. This enables the TinkerPop-style pattern:
///
/// ```rust
/// use interstellar::prelude::*;
/// use interstellar::graph_elements::GraphVertex;
/// use std::sync::Arc;
/// use std::collections::HashMap;
///
/// let graph = Arc::new(Graph::new());
/// let alice = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), "Alice".into()),
/// ]));
/// let bob = graph.add_vertex("person", HashMap::from([
///     ("name".to_string(), "Bob".into()),
/// ]));
/// graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
///
/// let alice_v = GraphVertex::new(alice, graph.clone());
///
/// // Traverse from Alice to her friends
/// let friends = alice_v.out("knows").to_list();
/// assert_eq!(friends.len(), 1);
/// assert_eq!(friends[0].property("name"), Some(Value::String("Bob".to_string())));
/// ```
///
/// # Lazy Evaluation
///
/// Steps are accumulated lazily and only executed when a terminal method
/// (`to_list()`, `first()`, `count()`, `exists()`) is called.
#[derive(Clone)]
pub struct GraphVertexTraversal<G: GraphAccess> {
    graph: G,
    start_id: VertexId,
    steps: Vec<TraversalStep>,
}

impl<G: GraphAccess> GraphVertexTraversal<G> {
    /// Create a new traversal starting from a specific vertex.
    pub(crate) fn new(graph: G, start_id: VertexId) -> Self {
        Self {
            graph,
            start_id,
            steps: Vec::new(),
        }
    }

    /// Navigate to outgoing adjacent vertices (all labels).
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let a = graph.add_vertex("person", HashMap::new());
    /// let b = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(a, b, "knows", HashMap::new()).unwrap();
    ///
    /// let v = GraphVertex::new(a, graph.clone());
    /// let neighbors = v.out_all().to_list();
    /// assert_eq!(neighbors.len(), 1);
    /// ```
    pub fn out(mut self) -> Self {
        self.steps.push(TraversalStep::Out(None));
        self
    }

    /// Navigate to outgoing adjacent vertices with label filter.
    pub fn out_label(mut self, label: &str) -> Self {
        self.steps.push(TraversalStep::Out(Some(label.to_string())));
        self
    }

    /// Navigate to incoming adjacent vertices (all labels).
    pub fn in_step(mut self) -> Self {
        self.steps.push(TraversalStep::In(None));
        self
    }

    /// Navigate to incoming adjacent vertices with label filter.
    pub fn in_label(mut self, label: &str) -> Self {
        self.steps.push(TraversalStep::In(Some(label.to_string())));
        self
    }

    /// Navigate to adjacent vertices in both directions (all labels).
    pub fn both_step(mut self) -> Self {
        self.steps.push(TraversalStep::Both(None));
        self
    }

    /// Navigate to adjacent vertices in both directions with label filter.
    pub fn both_label(mut self, label: &str) -> Self {
        self.steps
            .push(TraversalStep::Both(Some(label.to_string())));
        self
    }

    /// Filter vertices by label.
    pub fn has_label(mut self, label: &str) -> Self {
        self.steps.push(TraversalStep::HasLabel(label.to_string()));
        self
    }

    /// Filter vertices by property value.
    pub fn has_value(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.steps
            .push(TraversalStep::HasValue(key.to_string(), value.into()));
        self
    }

    /// Execute and return all vertices.
    ///
    /// This method executes the traversal by manually following edges using
    /// the `GraphAccess` trait methods.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    /// let bob = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Bob".into()),
    /// ]));
    /// let charlie = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Charlie".into()),
    /// ]));
    /// graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    /// graph.add_edge(alice, charlie, "knows", HashMap::new()).unwrap();
    ///
    /// let alice_v = GraphVertex::new(alice, graph.clone());
    /// let friends = alice_v.out("knows").to_list();
    /// assert_eq!(friends.len(), 2);
    /// ```
    pub fn to_list(self) -> Vec<GraphVertex<G>> {
        // Start with the initial vertex
        let mut current_ids: Vec<VertexId> = vec![self.start_id];

        // Execute each step
        for step in &self.steps {
            let mut next_ids: Vec<VertexId> = Vec::new();

            for vertex_id in &current_ids {
                match step {
                    TraversalStep::Out(label_filter) => {
                        // Get outgoing edges
                        for edge_id in self.graph.out_edge_ids(*vertex_id) {
                            if let Some(edge) = self.graph.get_edge(edge_id) {
                                // Apply label filter if present
                                if let Some(label) = label_filter {
                                    if &edge.label != label {
                                        continue;
                                    }
                                }
                                next_ids.push(edge.dst);
                            }
                        }
                    }
                    TraversalStep::In(label_filter) => {
                        // Get incoming edges
                        for edge_id in self.graph.in_edge_ids(*vertex_id) {
                            if let Some(edge) = self.graph.get_edge(edge_id) {
                                // Apply label filter if present
                                if let Some(label) = label_filter {
                                    if &edge.label != label {
                                        continue;
                                    }
                                }
                                next_ids.push(edge.src);
                            }
                        }
                    }
                    TraversalStep::Both(label_filter) => {
                        // Get both outgoing and incoming edges
                        for edge_id in self.graph.out_edge_ids(*vertex_id) {
                            if let Some(edge) = self.graph.get_edge(edge_id) {
                                if let Some(label) = label_filter {
                                    if &edge.label != label {
                                        continue;
                                    }
                                }
                                next_ids.push(edge.dst);
                            }
                        }
                        for edge_id in self.graph.in_edge_ids(*vertex_id) {
                            if let Some(edge) = self.graph.get_edge(edge_id) {
                                if let Some(label) = label_filter {
                                    if &edge.label != label {
                                        continue;
                                    }
                                }
                                next_ids.push(edge.src);
                            }
                        }
                    }
                    TraversalStep::HasLabel(label) => {
                        // Filter current vertex by label
                        if let Some(vertex) = self.graph.get_vertex(*vertex_id) {
                            if &vertex.label == label {
                                next_ids.push(*vertex_id);
                            }
                        }
                    }
                    TraversalStep::HasValue(key, value) => {
                        // Filter current vertex by property value
                        if let Some(vertex) = self.graph.get_vertex(*vertex_id) {
                            if vertex.properties.get(key) == Some(value) {
                                next_ids.push(*vertex_id);
                            }
                        }
                    }
                }
            }

            current_ids = next_ids;
        }

        // Convert final vertex IDs to GraphVertex objects
        current_ids
            .into_iter()
            .map(|id| GraphVertex::new(id, self.graph.clone()))
            .collect()
    }

    /// Execute and return the first vertex, if any.
    ///
    /// # Example
    ///
    /// ```rust
    /// use interstellar::prelude::*;
    /// use interstellar::graph_elements::GraphVertex;
    /// use std::sync::Arc;
    /// use std::collections::HashMap;
    ///
    /// let graph = Arc::new(Graph::new());
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    /// graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// let alice_v = GraphVertex::new(alice, graph.clone());
    /// let friend = alice_v.out("knows").first();
    /// assert!(friend.is_some());
    /// ```
    pub fn first(self) -> Option<GraphVertex<G>> {
        self.to_list().into_iter().next()
    }

    /// Execute and count results.
    pub fn count(self) -> usize {
        self.to_list().len()
    }

    /// Check if any results exist.
    pub fn exists(self) -> bool {
        self.first().is_some()
    }
}

impl<G: GraphAccess> std::fmt::Debug for GraphVertexTraversal<G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphVertexTraversal")
            .field("start_id", &self.start_id)
            .field("steps", &self.steps)
            .finish()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_graph() -> Arc<Graph> {
        let graph = Graph::new();
        let alice = graph.add_vertex(
            "person",
            HashMap::from([
                ("name".to_string(), "Alice".into()),
                ("age".to_string(), 30i64.into()),
            ]),
        );
        let bob = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Bob".into())]),
        );
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        Arc::new(graph)
    }

    // =========================================================================
    // GraphVertex Tests
    // =========================================================================

    #[test]
    fn graph_vertex_id() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let alice_val = g.v().has_value("name", "Alice").next().unwrap();
        let alice_id = alice_val.as_vertex_id().unwrap();

        let alice = GraphVertex::new(alice_id, graph.clone());
        assert_eq!(alice.id(), alice_id);
    }

    #[test]
    fn graph_vertex_label() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let alice_val = g.v().has_value("name", "Alice").next().unwrap();
        let alice_id = alice_val.as_vertex_id().unwrap();

        let alice = GraphVertex::new(alice_id, graph.clone());
        assert_eq!(alice.label(), Some("person".to_string()));
    }

    #[test]
    fn graph_vertex_property_access() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let alice_val = g.v().has_value("name", "Alice").next().unwrap();
        let alice_id = alice_val.as_vertex_id().unwrap();

        let alice = GraphVertex::new(alice_id, graph.clone());

        assert_eq!(
            alice.property("name"),
            Some(Value::String("Alice".to_string()))
        );
        assert_eq!(alice.property("age"), Some(Value::Int(30)));
        assert_eq!(alice.property("nonexistent"), None);
    }

    #[test]
    fn graph_vertex_properties() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let alice_val = g.v().has_value("name", "Alice").next().unwrap();
        let alice_id = alice_val.as_vertex_id().unwrap();

        let alice = GraphVertex::new(alice_id, graph.clone());
        let props = alice.properties();

        assert_eq!(props.len(), 2);
        assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(props.get("age"), Some(&Value::Int(30)));
    }

    #[test]
    fn graph_vertex_exists() {
        let graph = Arc::new(Graph::new());
        let id = graph.add_vertex("person", HashMap::new());
        let v = GraphVertex::new(id, graph.clone());

        assert!(v.exists());

        graph.remove_vertex(id).unwrap();
        assert!(!v.exists());
    }

    #[test]
    fn graph_vertex_mutation() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let alice_val = g.v().has_value("name", "Alice").next().unwrap();
        let alice_id = alice_val.as_vertex_id().unwrap();

        let alice = GraphVertex::new(alice_id, graph.clone());

        // Original age
        assert_eq!(alice.property("age"), Some(Value::Int(30)));

        // Mutate
        alice.property_set("age", 31i64).unwrap();

        // Change is visible immediately
        assert_eq!(alice.property("age"), Some(Value::Int(31)));

        // New snapshot also sees the change
        let snapshot2 = graph.snapshot();
        let g2 = snapshot2.gremlin();
        let age = g2.v().has_value("name", "Alice").values("age").next();
        assert_eq!(age, Some(Value::Int(31)));
    }

    #[test]
    fn graph_vertex_to_value() {
        let graph = Arc::new(Graph::new());
        let id = graph.add_vertex("person", HashMap::new());
        let v = GraphVertex::new(id, graph.clone());

        assert_eq!(v.to_value(), Value::Vertex(id));
    }

    #[test]
    fn graph_vertex_equality() {
        let graph = Arc::new(Graph::new());
        let id1 = graph.add_vertex("person", HashMap::new());
        let id2 = graph.add_vertex("person", HashMap::new());

        let v1a = GraphVertex::new(id1, graph.clone());
        let v1b = GraphVertex::new(id1, graph.clone());
        let v2 = GraphVertex::new(id2, graph.clone());

        assert_eq!(v1a, v1b);
        assert_ne!(v1a, v2);
    }

    #[test]
    fn graph_vertex_hash() {
        use std::collections::HashSet;

        let graph = Arc::new(Graph::new());
        let id1 = graph.add_vertex("person", HashMap::new());
        let id2 = graph.add_vertex("person", HashMap::new());

        let v1a = GraphVertex::new(id1, graph.clone());
        let v1b = GraphVertex::new(id1, graph.clone());
        let v2 = GraphVertex::new(id2, graph.clone());

        let mut set = HashSet::new();
        set.insert(v1a);
        set.insert(v1b); // Duplicate, should not increase size
        set.insert(v2);

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn graph_vertex_debug() {
        let graph = Arc::new(Graph::new());
        let id = graph.add_vertex("person", HashMap::new());
        let v = GraphVertex::new(id, graph.clone());

        let debug_str = format!("{:?}", v);
        assert!(debug_str.contains("GraphVertex"));
        assert!(debug_str.contains("person"));
    }

    // =========================================================================
    // GraphEdge Tests
    // =========================================================================

    #[test]
    fn graph_edge_id() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let edge_val = g.e().next().unwrap();
        let edge_id = edge_val.as_edge_id().unwrap();

        let edge = GraphEdge::new(edge_id, graph.clone());
        assert_eq!(edge.id(), edge_id);
    }

    #[test]
    fn graph_edge_label() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let edge_val = g.e().next().unwrap();
        let edge_id = edge_val.as_edge_id().unwrap();

        let edge = GraphEdge::new(edge_id, graph.clone());
        assert_eq!(edge.label(), Some("knows".to_string()));
    }

    #[test]
    fn graph_edge_endpoints() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let edge_val = g.e().next().unwrap();
        let edge_id = edge_val.as_edge_id().unwrap();

        let edge = GraphEdge::new(edge_id, graph.clone());

        // out_v() returns Alice (source)
        let src = edge.out_v().unwrap();
        assert_eq!(
            src.property("name"),
            Some(Value::String("Alice".to_string()))
        );

        // in_v() returns Bob (destination)
        let dst = edge.in_v().unwrap();
        assert_eq!(dst.property("name"), Some(Value::String("Bob".to_string())));

        // both_v() returns tuple
        let (src2, dst2) = edge.both_v().unwrap();
        assert_eq!(src2.id(), src.id());
        assert_eq!(dst2.id(), dst.id());
    }

    #[test]
    fn graph_edge_property_access() {
        let graph = Arc::new(Graph::new());
        let alice = graph.add_vertex("person", HashMap::new());
        let bob = graph.add_vertex("person", HashMap::new());
        let edge_id = graph
            .add_edge(
                alice,
                bob,
                "knows",
                HashMap::from([("since".to_string(), 2020i64.into())]),
            )
            .unwrap();

        let edge = GraphEdge::new(edge_id, graph.clone());

        assert_eq!(edge.property("since"), Some(Value::Int(2020)));
        assert_eq!(edge.property("nonexistent"), None);
    }

    #[test]
    fn graph_edge_properties() {
        let graph = Arc::new(Graph::new());
        let alice = graph.add_vertex("person", HashMap::new());
        let bob = graph.add_vertex("person", HashMap::new());
        let edge_id = graph
            .add_edge(
                alice,
                bob,
                "knows",
                HashMap::from([
                    ("since".to_string(), 2020i64.into()),
                    ("weight".to_string(), 0.95f64.into()),
                ]),
            )
            .unwrap();

        let edge = GraphEdge::new(edge_id, graph.clone());
        let props = edge.properties();

        assert_eq!(props.len(), 2);
        assert_eq!(props.get("since"), Some(&Value::Int(2020)));
    }

    #[test]
    fn graph_edge_mutation() {
        let graph = Arc::new(Graph::new());
        let alice = graph.add_vertex("person", HashMap::new());
        let bob = graph.add_vertex("person", HashMap::new());
        let edge_id = graph
            .add_edge(
                alice,
                bob,
                "knows",
                HashMap::from([("since".to_string(), 2020i64.into())]),
            )
            .unwrap();

        let edge = GraphEdge::new(edge_id, graph.clone());

        // Original value
        assert_eq!(edge.property("since"), Some(Value::Int(2020)));

        // Mutate
        edge.property_set("since", 2021i64).unwrap();

        // Change is visible immediately
        assert_eq!(edge.property("since"), Some(Value::Int(2021)));
    }

    #[test]
    fn graph_edge_exists() {
        let graph = Arc::new(Graph::new());
        let alice = graph.add_vertex("person", HashMap::new());
        let bob = graph.add_vertex("person", HashMap::new());
        let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        let edge = GraphEdge::new(edge_id, graph.clone());
        assert!(edge.exists());

        graph.remove_edge(edge_id).unwrap();
        assert!(!edge.exists());
    }

    #[test]
    fn graph_edge_to_value() {
        let graph = Arc::new(Graph::new());
        let alice = graph.add_vertex("person", HashMap::new());
        let bob = graph.add_vertex("person", HashMap::new());
        let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        let edge = GraphEdge::new(edge_id, graph.clone());
        assert_eq!(edge.to_value(), Value::Edge(edge_id));
    }

    #[test]
    fn graph_edge_equality() {
        let graph = Arc::new(Graph::new());
        let alice = graph.add_vertex("person", HashMap::new());
        let bob = graph.add_vertex("person", HashMap::new());
        let charlie = graph.add_vertex("person", HashMap::new());

        let edge1 = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        let edge2 = graph
            .add_edge(bob, charlie, "knows", HashMap::new())
            .unwrap();

        let e1a = GraphEdge::new(edge1, graph.clone());
        let e1b = GraphEdge::new(edge1, graph.clone());
        let e2 = GraphEdge::new(edge2, graph.clone());

        assert_eq!(e1a, e1b);
        assert_ne!(e1a, e2);
    }

    // =========================================================================
    // GraphVertexTraversal Tests
    // =========================================================================

    fn test_graph_chain() -> Arc<Graph> {
        // Create Alice -> Bob -> Charlie chain
        let graph = Graph::new();
        let alice = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Alice".into())]),
        );
        let bob = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Bob".into())]),
        );
        let charlie = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), "Charlie".into())]),
        );
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        graph
            .add_edge(bob, charlie, "knows", HashMap::new())
            .unwrap();
        Arc::new(graph)
    }

    #[test]
    fn vertex_out_traversal() {
        let graph = test_graph_chain();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Get Alice
        let alice_val = g.v().has_value("name", "Alice").next().unwrap();
        let alice_id = alice_val.as_vertex_id().unwrap();
        let alice = GraphVertex::new(alice_id, graph.clone());

        // Traverse from Alice -> Bob
        let friends = alice.out("knows").to_list();
        assert_eq!(friends.len(), 1);
        assert_eq!(
            friends[0].property("name"),
            Some(Value::String("Bob".to_string()))
        );
    }

    #[test]
    fn vertex_out_all_traversal() {
        let graph = test_graph_chain();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let alice_val = g.v().has_value("name", "Alice").next().unwrap();
        let alice_id = alice_val.as_vertex_id().unwrap();
        let alice = GraphVertex::new(alice_id, graph.clone());

        let friends = alice.out_all().to_list();
        assert_eq!(friends.len(), 1);
    }

    #[test]
    fn vertex_in_traversal() {
        let graph = test_graph_chain();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Get Bob
        let bob_val = g.v().has_value("name", "Bob").next().unwrap();
        let bob_id = bob_val.as_vertex_id().unwrap();
        let bob = GraphVertex::new(bob_id, graph.clone());

        // Traverse from Bob <- Alice
        let knowers = bob.in_("knows").to_list();
        assert_eq!(knowers.len(), 1);
        assert_eq!(
            knowers[0].property("name"),
            Some(Value::String("Alice".to_string()))
        );
    }

    #[test]
    fn vertex_in_all_traversal() {
        let graph = test_graph_chain();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let bob_val = g.v().has_value("name", "Bob").next().unwrap();
        let bob_id = bob_val.as_vertex_id().unwrap();
        let bob = GraphVertex::new(bob_id, graph.clone());

        let knowers = bob.in_all().to_list();
        assert_eq!(knowers.len(), 1);
    }

    #[test]
    fn vertex_both_traversal() {
        let graph = test_graph_chain();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Get Bob (middle of chain)
        let bob_val = g.v().has_value("name", "Bob").next().unwrap();
        let bob_id = bob_val.as_vertex_id().unwrap();
        let bob = GraphVertex::new(bob_id, graph.clone());

        // Bob has both Alice (incoming) and Charlie (outgoing)
        let neighbors = bob.both("knows").to_list();
        assert_eq!(neighbors.len(), 2);
    }

    #[test]
    fn vertex_chained_traversal() {
        let graph = test_graph_chain();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Get Alice
        let alice_val = g.v().has_value("name", "Alice").next().unwrap();
        let alice_id = alice_val.as_vertex_id().unwrap();
        let alice = GraphVertex::new(alice_id, graph.clone());

        // Alice -> Bob -> Charlie (friends of friends)
        let fof = alice.out("knows").out_label("knows").to_list();
        assert_eq!(fof.len(), 1);
        assert_eq!(
            fof[0].property("name"),
            Some(Value::String("Charlie".to_string()))
        );
    }

    #[test]
    fn vertex_traversal_first() {
        let graph = test_graph_chain();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let alice_val = g.v().has_value("name", "Alice").next().unwrap();
        let alice_id = alice_val.as_vertex_id().unwrap();
        let alice = GraphVertex::new(alice_id, graph.clone());

        let friend = alice.out("knows").first();
        assert!(friend.is_some());
        assert_eq!(
            friend.unwrap().property("name"),
            Some(Value::String("Bob".to_string()))
        );
    }

    #[test]
    fn vertex_traversal_count() {
        let graph = test_graph_chain();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let bob_val = g.v().has_value("name", "Bob").next().unwrap();
        let bob_id = bob_val.as_vertex_id().unwrap();
        let bob = GraphVertex::new(bob_id, graph.clone());

        assert_eq!(bob.both("knows").count(), 2); // Alice and Charlie
    }

    #[test]
    fn vertex_traversal_exists() {
        let graph = test_graph_chain();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let alice_val = g.v().has_value("name", "Alice").next().unwrap();
        let alice_id = alice_val.as_vertex_id().unwrap();
        let alice = GraphVertex::new(alice_id, graph.clone());

        assert!(alice.out("knows").exists());
        assert!(!alice.out("created").exists());
    }

    #[test]
    fn vertex_add_edge() {
        let graph = Arc::new(Graph::new());
        let alice = graph.add_vertex("person", HashMap::new());
        let bob = graph.add_vertex("person", HashMap::new());

        let alice_v = GraphVertex::new(alice, graph.clone());
        let bob_v = GraphVertex::new(bob, graph.clone());

        let edge = alice_v.add_edge("knows", &bob_v).unwrap();
        assert_eq!(edge.label(), Some("knows".to_string()));

        // Verify traversal works
        let friends = alice_v.out("knows").to_list();
        assert_eq!(friends.len(), 1);
        assert_eq!(friends[0].id(), bob);
    }

    #[test]
    fn vertex_add_edge_with_props() {
        let graph = Arc::new(Graph::new());
        let alice = graph.add_vertex("person", HashMap::new());
        let bob = graph.add_vertex("person", HashMap::new());

        let alice_v = GraphVertex::new(alice, graph.clone());
        let bob_v = GraphVertex::new(bob, graph.clone());

        let edge = alice_v
            .add_edge_with_props(
                "knows",
                &bob_v,
                HashMap::from([("since".to_string(), 2020i64.into())]),
            )
            .unwrap();

        assert_eq!(edge.property("since"), Some(Value::Int(2020)));
    }

    #[test]
    fn vertex_remove() {
        let graph = Arc::new(Graph::new());
        let id = graph.add_vertex("person", HashMap::new());
        let v = GraphVertex::new(id, graph.clone());

        assert!(v.exists());
        v.remove().unwrap();
        assert!(!v.exists());
    }

    #[test]
    fn edge_remove() {
        let graph = Arc::new(Graph::new());
        let alice = graph.add_vertex("person", HashMap::new());
        let bob = graph.add_vertex("person", HashMap::new());
        let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        let edge = GraphEdge::new(edge_id, graph.clone());
        assert!(edge.exists());
        edge.remove().unwrap();
        assert!(!edge.exists());
    }

    // =========================================================================
    // Type Alias Tests
    // =========================================================================

    #[test]
    fn type_alias_inmemory_vertex() {
        let graph = Arc::new(Graph::new());
        let id = graph.add_vertex("person", HashMap::new());

        // InMemoryVertex is just GraphVertex<Arc<Graph>>
        let v: InMemoryVertex = InMemoryVertex::new(id, graph);
        assert!(v.exists());
    }

    #[test]
    fn type_alias_inmemory_edge() {
        let graph = Arc::new(Graph::new());
        let a = graph.add_vertex("person", HashMap::new());
        let b = graph.add_vertex("person", HashMap::new());
        let edge_id = graph.add_edge(a, b, "knows", HashMap::new()).unwrap();

        // InMemoryEdge is just GraphEdge<Arc<Graph>>
        let e: InMemoryEdge = InMemoryEdge::new(edge_id, graph);
        assert!(e.exists());
    }

    // =========================================================================
    // Mmap Tests (feature-gated)
    // =========================================================================

    #[cfg(feature = "mmap")]
    mod mmap_tests {
        use super::*;
        use crate::storage::CowMmapGraph;
        use tempfile::tempdir;

        fn temp_db_path() -> (tempfile::TempDir, std::path::PathBuf) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("test.db");
            (dir, path)
        }

        #[test]
        fn persistent_vertex_basic() {
            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
            let id = graph
                .add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), "Alice".into())]),
                )
                .unwrap();

            let v: PersistentVertex = PersistentVertex::new(id, graph.clone());
            assert_eq!(v.label(), Some("person".to_string()));
            assert_eq!(v.property("name"), Some(Value::String("Alice".to_string())));
        }

        #[test]
        fn persistent_edge_basic() {
            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
            let a = graph.add_vertex("person", HashMap::new()).unwrap();
            let b = graph.add_vertex("person", HashMap::new()).unwrap();
            let edge_id = graph.add_edge(a, b, "knows", HashMap::new()).unwrap();

            let e: PersistentEdge = PersistentEdge::new(edge_id, graph.clone());
            assert_eq!(e.label(), Some("knows".to_string()));
        }

        #[test]
        fn persistent_vertex_traversal() {
            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
            let alice = graph
                .add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), "Alice".into())]),
                )
                .unwrap();
            let bob = graph
                .add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), "Bob".into())]),
                )
                .unwrap();
            graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

            let alice_v: PersistentVertex = PersistentVertex::new(alice, graph.clone());
            let friends = alice_v.out("knows").to_list();

            assert_eq!(friends.len(), 1);
            assert_eq!(
                friends[0].property("name"),
                Some(Value::String("Bob".to_string()))
            );
        }

        #[test]
        fn persistent_vertex_mutation() {
            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
            let id = graph
                .add_vertex("person", HashMap::from([("age".to_string(), 30i64.into())]))
                .unwrap();

            let v: PersistentVertex = PersistentVertex::new(id, graph.clone());
            assert_eq!(v.property("age"), Some(Value::Int(30)));

            v.property_set("age", 31i64).unwrap();
            assert_eq!(v.property("age"), Some(Value::Int(31)));
        }

        #[test]
        fn persistent_edge_endpoints_return_persistent_vertex() {
            let (_dir, path) = temp_db_path();
            let graph = Arc::new(CowMmapGraph::open(&path).unwrap());
            let a = graph
                .add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), "Alice".into())]),
                )
                .unwrap();
            let b = graph
                .add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), "Bob".into())]),
                )
                .unwrap();
            let edge_id = graph.add_edge(a, b, "knows", HashMap::new()).unwrap();

            let e: PersistentEdge = PersistentEdge::new(edge_id, graph.clone());

            // Endpoints should be PersistentVertex, not InMemoryVertex
            let src = e.out_v().unwrap();
            let dst = e.in_v().unwrap();

            assert_eq!(
                src.property("name"),
                Some(Value::String("Alice".to_string()))
            );
            assert_eq!(dst.property("name"), Some(Value::String("Bob".to_string())));
        }
    }
}
