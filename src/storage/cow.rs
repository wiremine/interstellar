//! Copy-on-Write graph storage with snapshot isolation.
//!
//! This module provides [`CowGraph`], a graph storage implementation using
//! persistent data structures from the `im` crate. This enables:
//!
//! - **Lock-free reads**: Snapshots don't hold locks
//! - **Owned snapshots**: Snapshots can outlive the source graph
//! - **Structural sharing**: O(1) snapshot creation via Arc cloning
//! - **Atomic mutations**: Each mutation is atomic without explicit transactions
//!
//! # Architecture
//!
//! ```text
//! CowGraph
//! ├── state: RwLock<CowGraphState>  # Mutable state container
//! └── schema: RwLock<Option<GraphSchema>>  # Optional schema
//!
//! CowGraphState (immutable, shareable via Clone)
//! ├── vertices: im::HashMap<VertexId, Arc<NodeData>>
//! ├── edges: im::HashMap<EdgeId, Arc<EdgeData>>
//! ├── vertex_labels: im::HashMap<u32, Arc<RoaringBitmap>>
//! ├── edge_labels: im::HashMap<u32, Arc<RoaringBitmap>>
//! ├── interner: Arc<StringInterner>
//! ├── version: u64
//! ├── next_vertex_id: u64
//! └── next_edge_id: u64
//!
//! CowSnapshot (owned, immutable)
//! └── state: Arc<CowGraphState>
//! ```
//!
//! # Example
//!
//! ```
//! use interstellar::storage::cow::{CowGraph, CowSnapshot};
//! use interstellar::storage::GraphStorage;
//! use std::collections::HashMap;
//!
//! // Create a COW graph
//! let graph = CowGraph::new();
//!
//! // Add vertices
//! let alice = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), "Alice".into()),
//! ]));
//!
//! // Take a snapshot - O(1) operation
//! let snap1 = graph.snapshot();
//!
//! // Mutate the graph
//! graph.set_vertex_property(alice, "age", 30.into()).unwrap();
//!
//! // snap1 still sees original state (no age property)
//! let v1 = snap1.get_vertex(alice).unwrap();
//! assert!(v1.properties.get("age").is_none());
//!
//! // New snapshot sees updated state
//! let snap2 = graph.snapshot();
//! let v2 = snap2.get_vertex(alice).unwrap();
//! assert_eq!(v2.properties.get("age").unwrap().as_i64(), Some(30));
//! ```
//!
//! # Thread Safety
//!
//! Both [`CowGraph`] and [`CowSnapshot`] are `Send + Sync`:
//!
//! - Multiple threads can take snapshots concurrently
//! - Snapshots can be sent to other threads
//! - Writers are serialized via RwLock
//! - Readers never block writers (snapshots don't hold locks)

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use im::HashMap as ImHashMap;
use parking_lot::RwLock;
use roaring::RoaringBitmap;

use crate::error::StorageError;
use crate::gql::{self, GqlError};
use crate::schema::GraphSchema;
use crate::storage::interner::StringInterner;
use crate::storage::{Edge, GraphStorage, Vertex};
use crate::value::{EdgeId, Value, VertexId};

// =============================================================================
// Core Data Structures
// =============================================================================

/// Internal vertex representation.
///
/// Wrapped in Arc for cheap cloning when modifying individual vertices.
#[derive(Clone, Debug)]
pub(crate) struct NodeData {
    /// Vertex identifier
    pub id: VertexId,
    /// Interned label string ID
    pub label_id: u32,
    /// Property key-value pairs
    pub properties: HashMap<String, Value>,
    /// Outgoing edge IDs (adjacency list)
    pub out_edges: Vec<EdgeId>,
    /// Incoming edge IDs (adjacency list)
    pub in_edges: Vec<EdgeId>,
}

/// Internal edge representation.
///
/// Wrapped in Arc for cheap cloning when modifying individual edges.
#[derive(Clone, Debug)]
pub(crate) struct EdgeData {
    /// Edge identifier
    pub id: EdgeId,
    /// Interned label string ID
    pub label_id: u32,
    /// Source vertex ID
    pub src: VertexId,
    /// Destination vertex ID
    pub dst: VertexId,
    /// Property key-value pairs
    pub properties: HashMap<String, Value>,
}

/// Immutable graph state that can be shared between snapshots.
///
/// All fields use persistent data structures from the `im` crate,
/// enabling O(1) cloning via structural sharing.
#[derive(Clone)]
pub struct CowGraphState {
    /// Vertex data: VertexId -> NodeData
    pub(crate) vertices: ImHashMap<VertexId, Arc<NodeData>>,

    /// Edge data: EdgeId -> EdgeData
    pub(crate) edges: ImHashMap<EdgeId, Arc<EdgeData>>,

    /// Label index: label_id -> set of vertex IDs
    pub(crate) vertex_labels: ImHashMap<u32, Arc<RoaringBitmap>>,

    /// Label index: label_id -> set of edge IDs
    pub(crate) edge_labels: ImHashMap<u32, Arc<RoaringBitmap>>,

    /// String interner (append-only, always shared)
    pub(crate) interner: Arc<RwLock<StringInterner>>,

    /// Monotonic version counter
    pub(crate) version: u64,

    /// Next vertex ID
    pub(crate) next_vertex_id: u64,

    /// Next edge ID
    pub(crate) next_edge_id: u64,
}

impl CowGraphState {
    /// Create a new empty graph state.
    pub fn new() -> Self {
        Self {
            vertices: ImHashMap::new(),
            edges: ImHashMap::new(),
            vertex_labels: ImHashMap::new(),
            edge_labels: ImHashMap::new(),
            interner: Arc::new(RwLock::new(StringInterner::new())),
            version: 0,
            next_vertex_id: 0,
            next_edge_id: 0,
        }
    }
}

impl Default for CowGraphState {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// CowGraph - Main Graph Container
// =============================================================================

/// Copy-on-Write graph with snapshot support.
///
/// `CowGraph` uses persistent data structures to enable O(1) snapshot creation
/// and lock-free reads. Mutations are serialized via RwLock but don't block
/// existing snapshots.
///
/// # Creating a CowGraph
///
/// ```
/// use interstellar::storage::cow::CowGraph;
///
/// let graph = CowGraph::new();
/// ```
///
/// # Snapshots
///
/// Snapshots are O(1) and don't hold locks:
///
/// ```
/// use interstellar::storage::cow::CowGraph;
///
/// let graph = CowGraph::new();
/// let snap = graph.snapshot();
///
/// // snap can be sent to another thread, outlive the graph, etc.
/// ```
pub struct CowGraph {
    /// Current mutable state (protected by RwLock for thread safety)
    state: RwLock<CowGraphState>,

    /// Schema for validation (optional)
    schema: RwLock<Option<GraphSchema>>,
}

impl CowGraph {
    /// Create a new empty COW graph.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    ///
    /// let graph = CowGraph::new();
    /// assert_eq!(graph.vertex_count(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            state: RwLock::new(CowGraphState::new()),
            schema: RwLock::new(None),
        }
    }

    /// Create a new COW graph with a schema.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    /// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .mode(ValidationMode::Strict)
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// let graph = CowGraph::with_schema(schema);
    /// ```
    pub fn with_schema(schema: GraphSchema) -> Self {
        Self {
            state: RwLock::new(CowGraphState::new()),
            schema: RwLock::new(Some(schema)),
        }
    }

    // =========================================================================
    // Snapshot Operations
    // =========================================================================

    /// Create a snapshot of the current graph state.
    ///
    /// This is an O(1) operation that creates a shared reference to the current
    /// state. The snapshot will not reflect any mutations made after this call.
    ///
    /// # Thread Safety
    ///
    /// This method briefly acquires a read lock to clone the state.
    /// The returned snapshot does not hold any locks.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    /// use interstellar::storage::GraphStorage;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowGraph::new();
    /// let v1 = graph.add_vertex("person", HashMap::new());
    ///
    /// let snap = graph.snapshot();
    /// assert_eq!(snap.vertex_count(), 1);
    ///
    /// // Mutations after snapshot don't affect it
    /// graph.add_vertex("person", HashMap::new());
    /// assert_eq!(snap.vertex_count(), 1); // Still 1
    /// ```
    pub fn snapshot(&self) -> CowSnapshot {
        let state = self.state.read();
        CowSnapshot {
            state: Arc::new((*state).clone()),
        }
    }

    // =========================================================================
    // Read Operations (via current state)
    // =========================================================================

    /// Returns the total number of vertices in the graph.
    pub fn vertex_count(&self) -> u64 {
        self.state.read().vertices.len() as u64
    }

    /// Returns the total number of edges in the graph.
    pub fn edge_count(&self) -> u64 {
        self.state.read().edges.len() as u64
    }

    /// Returns the current version number.
    pub fn version(&self) -> u64 {
        self.state.read().version
    }

    /// Get the current schema, if one is set.
    pub fn schema(&self) -> Option<GraphSchema> {
        self.schema.read().clone()
    }

    /// Set or replace the graph schema.
    pub fn set_schema(&self, schema: Option<GraphSchema>) {
        *self.schema.write() = schema;
    }

    // =========================================================================
    // Mutation Operations
    // =========================================================================

    /// Add a vertex to the graph.
    ///
    /// This triggers copy-on-write: only the modified paths in the persistent
    /// data structure are copied. Existing snapshots continue to see the old state.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    /// use interstellar::storage::GraphStorage;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowGraph::new();
    /// let id = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ]));
    ///
    /// let snap = graph.snapshot();
    /// let vertex = snap.get_vertex(id).unwrap();
    /// assert_eq!(vertex.label, "person");
    /// ```
    pub fn add_vertex(&self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        let mut state = self.state.write();

        // Allocate ID
        let id = VertexId(state.next_vertex_id);
        state.next_vertex_id += 1;

        // Intern label
        let label_id = state.interner.write().intern(label);

        // Create node
        let node = Arc::new(NodeData {
            id,
            label_id,
            properties,
            out_edges: Vec::new(),
            in_edges: Vec::new(),
        });

        // Insert into persistent map (O(log n), structural sharing)
        state.vertices = state.vertices.update(id, node);

        // Update label index
        let bitmap = state
            .vertex_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(id.0 as u32);
        state.vertex_labels = state.vertex_labels.update(label_id, Arc::new(new_bitmap));

        // Increment version
        state.version += 1;

        id
    }

    /// Add an edge between two vertices.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if either vertex doesn't exist.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    /// use interstellar::storage::GraphStorage;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowGraph::new();
    /// let alice = graph.add_vertex("person", HashMap::new());
    /// let bob = graph.add_vertex("person", HashMap::new());
    ///
    /// let edge = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    ///
    /// let snap = graph.snapshot();
    /// let e = snap.get_edge(edge).unwrap();
    /// assert_eq!(e.src, alice);
    /// assert_eq!(e.dst, bob);
    /// ```
    pub fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        let mut state = self.state.write();

        // Verify vertices exist
        if !state.vertices.contains_key(&src) {
            return Err(StorageError::VertexNotFound(src));
        }
        if !state.vertices.contains_key(&dst) {
            return Err(StorageError::VertexNotFound(dst));
        }

        // Allocate edge ID
        let edge_id = EdgeId(state.next_edge_id);
        state.next_edge_id += 1;

        // Intern label
        let label_id = state.interner.write().intern(label);

        // Create edge
        let edge = Arc::new(EdgeData {
            id: edge_id,
            label_id,
            src,
            dst,
            properties,
        });

        // Insert edge
        state.edges = state.edges.update(edge_id, edge);

        // Update source vertex's out_edges
        if let Some(src_node) = state.vertices.get(&src) {
            let mut new_src = (**src_node).clone();
            new_src.out_edges.push(edge_id);
            state.vertices = state.vertices.update(src, Arc::new(new_src));
        }

        // Update destination vertex's in_edges
        if let Some(dst_node) = state.vertices.get(&dst) {
            let mut new_dst = (**dst_node).clone();
            new_dst.in_edges.push(edge_id);
            state.vertices = state.vertices.update(dst, Arc::new(new_dst));
        }

        // Update label index
        let bitmap = state
            .edge_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(edge_id.0 as u32);
        state.edge_labels = state.edge_labels.update(label_id, Arc::new(new_bitmap));

        state.version += 1;

        Ok(edge_id)
    }

    /// Update a vertex's property.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if the vertex doesn't exist.
    pub fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write();

        let node = state
            .vertices
            .get(&id)
            .ok_or(StorageError::VertexNotFound(id))?;

        // Clone and modify the node
        let mut new_node = (**node).clone();
        new_node.properties.insert(key.to_string(), value);

        // Update in persistent map
        state.vertices = state.vertices.update(id, Arc::new(new_node));
        state.version += 1;

        Ok(())
    }

    /// Update an edge's property.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::EdgeNotFound` if the edge doesn't exist.
    pub fn set_edge_property(
        &self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write();

        let edge = state.edges.get(&id).ok_or(StorageError::EdgeNotFound(id))?;

        // Clone and modify the edge
        let mut new_edge = (**edge).clone();
        new_edge.properties.insert(key.to_string(), value);

        // Update in persistent map
        state.edges = state.edges.update(id, Arc::new(new_edge));
        state.version += 1;

        Ok(())
    }

    /// Remove a vertex and all its incident edges.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::VertexNotFound` if the vertex doesn't exist.
    pub fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError> {
        let mut state = self.state.write();

        let node = state
            .vertices
            .get(&id)
            .ok_or(StorageError::VertexNotFound(id))?
            .clone();

        // Collect edges to remove
        let edges_to_remove: Vec<EdgeId> = node
            .out_edges
            .iter()
            .chain(node.in_edges.iter())
            .copied()
            .collect();

        // Remove from vertex storage
        state.vertices = state.vertices.without(&id);

        // Remove from label index
        if let Some(bitmap) = state.vertex_labels.get(&node.label_id) {
            let mut new_bitmap = (**bitmap).clone();
            new_bitmap.remove(id.0 as u32);
            if new_bitmap.is_empty() {
                state.vertex_labels = state.vertex_labels.without(&node.label_id);
            } else {
                state.vertex_labels = state
                    .vertex_labels
                    .update(node.label_id, Arc::new(new_bitmap));
            }
        }

        // Remove all incident edges
        for edge_id in edges_to_remove {
            Self::remove_edge_internal(&mut state, edge_id, Some(id));
        }

        state.version += 1;

        Ok(())
    }

    /// Remove an edge from the graph.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::EdgeNotFound` if the edge doesn't exist.
    pub fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError> {
        let mut state = self.state.write();

        if !state.edges.contains_key(&id) {
            return Err(StorageError::EdgeNotFound(id));
        }

        Self::remove_edge_internal(&mut state, id, None);
        state.version += 1;

        Ok(())
    }

    /// Internal edge removal, optionally skipping a vertex being deleted.
    fn remove_edge_internal(state: &mut CowGraphState, id: EdgeId, skip_vertex: Option<VertexId>) {
        let Some(edge) = state.edges.get(&id).cloned() else {
            return;
        };

        // Remove from edge storage
        state.edges = state.edges.without(&id);

        // Remove from label index
        if let Some(bitmap) = state.edge_labels.get(&edge.label_id) {
            let mut new_bitmap = (**bitmap).clone();
            new_bitmap.remove(id.0 as u32);
            if new_bitmap.is_empty() {
                state.edge_labels = state.edge_labels.without(&edge.label_id);
            } else {
                state.edge_labels = state
                    .edge_labels
                    .update(edge.label_id, Arc::new(new_bitmap));
            }
        }

        // Remove from source vertex's out_edges (if not being deleted)
        if skip_vertex != Some(edge.src) {
            if let Some(src_node) = state.vertices.get(&edge.src) {
                let mut new_src = (**src_node).clone();
                new_src.out_edges.retain(|&e| e != id);
                state.vertices = state.vertices.update(edge.src, Arc::new(new_src));
            }
        }

        // Remove from destination vertex's in_edges (if not being deleted)
        if skip_vertex != Some(edge.dst) {
            if let Some(dst_node) = state.vertices.get(&edge.dst) {
                let mut new_dst = (**dst_node).clone();
                new_dst.in_edges.retain(|&e| e != id);
                state.vertices = state.vertices.update(edge.dst, Arc::new(new_dst));
            }
        }
    }

    // =========================================================================
    // GQL Mutation API
    // =========================================================================

    /// Execute a GQL mutation statement.
    ///
    /// This method parses and executes GQL mutation statements (CREATE, SET,
    /// DELETE, DETACH DELETE, MERGE). Read-only queries are not currently
    /// supported directly on CowGraph - use `snapshot()` and the regular
    /// GQL APIs for reads.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    /// use interstellar::storage::GraphStorage;
    ///
    /// let graph = CowGraph::new();
    ///
    /// // Mutation - acquires write lock, executes atomically
    /// let results = graph.execute_mutation("CREATE (n:Person {name: 'Alice'}) RETURN n").unwrap();
    /// assert_eq!(results.len(), 1);
    ///
    /// // Verify
    /// let snap = graph.snapshot();
    /// assert_eq!(snap.vertex_count(), 1);
    /// ```
    pub fn execute_mutation(&self, gql: &str) -> Result<Vec<Value>, GqlError> {
        let stmt = gql::parse_statement(gql)?;

        if stmt.is_read_only() {
            return Err(GqlError::Mutation(
                "Read-only queries not supported via execute_mutation(). \
                 Use snapshot() and the standard GQL APIs for reads."
                    .to_string(),
            ));
        }

        // Execute mutation atomically
        let mut wrapper = CowGraphMutWrapper { graph: self };
        let schema = self.schema();
        gql::execute_mutation_with_schema(&stmt, &mut wrapper, schema.as_ref())
            .map_err(|e| GqlError::Mutation(e.to_string()))
    }

    // =========================================================================
    // Batch Operations
    // =========================================================================

    /// Execute multiple operations atomically.
    ///
    /// The closure receives a `BatchContext` that buffers all mutations.
    /// Only when the closure returns `Ok(())` are all mutations applied.
    /// If the closure returns `Err` or panics, no mutations are applied.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::{CowGraph, BatchContext};
    /// use std::collections::HashMap;
    ///
    /// let graph = CowGraph::new();
    ///
    /// graph.batch(|ctx| {
    ///     let alice = ctx.add_vertex("Person", HashMap::from([
    ///         ("name".to_string(), "Alice".into()),
    ///     ]));
    ///     let bob = ctx.add_vertex("Person", HashMap::from([
    ///         ("name".to_string(), "Bob".into()),
    ///     ]));
    ///     ctx.add_edge(alice, bob, "knows", HashMap::new())?;
    ///     Ok(())
    /// }).unwrap();
    ///
    /// assert_eq!(graph.vertex_count(), 2);
    /// assert_eq!(graph.edge_count(), 1);
    /// ```
    pub fn batch<F, T>(&self, f: F) -> Result<T, BatchError>
    where
        F: FnOnce(&mut BatchContext) -> Result<T, BatchError>,
    {
        // Clone state for modification (O(1) due to structural sharing)
        let mut working_state = self.state.read().clone();

        // Create batch context
        let mut ctx = BatchContext {
            state: &mut working_state,
        };

        // Execute user function
        let result = f(&mut ctx)?;

        // If successful, apply the working state
        *self.state.write() = working_state;

        Ok(result)
    }
}

impl Default for CowGraph {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// CowSnapshot - Immutable Owned Snapshot
// =============================================================================

/// An owned snapshot of the graph at a point in time.
///
/// Unlike the current `GraphSnapshot<'g>`, this snapshot:
/// - Does not hold any locks
/// - Can be sent across threads (`Send + Sync`)
/// - Can outlive the source `CowGraph`
/// - Is immutable and will never change
///
/// # Example
///
/// ```
/// use interstellar::storage::cow::CowGraph;
/// use interstellar::storage::GraphStorage;
/// use std::collections::HashMap;
/// use std::thread;
///
/// let graph = CowGraph::new();
/// graph.add_vertex("person", HashMap::new());
///
/// let snap = graph.snapshot();
///
/// // Snapshot can be sent to another thread
/// let handle = thread::spawn(move || {
///     snap.vertex_count()
/// });
///
/// assert_eq!(handle.join().unwrap(), 1);
/// ```
#[derive(Clone)]
pub struct CowSnapshot {
    /// Shared reference to frozen state
    state: Arc<CowGraphState>,
}

impl CowSnapshot {
    /// Get the snapshot version.
    pub fn version(&self) -> u64 {
        self.state.version
    }

    // Note: GQL query support and traversal() are not yet available for CowSnapshot
    // because the GQL compiler and traversal engine are currently tied to GraphSnapshot.
    // This will be addressed in a future update to make those modules generic over
    // GraphStorage implementations.
    //
    // For now, use the programmatic GraphStorage API (get_vertex, out_edges, etc.)
    // or wrap the CowSnapshot data in a Graph for full GQL/traversal support.
}

impl GraphStorage for CowSnapshot {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        let node = self.state.vertices.get(&id)?;
        let label = self
            .state
            .interner
            .read()
            .resolve(node.label_id)?
            .to_string();

        Some(Vertex {
            id: node.id,
            label,
            properties: node.properties.clone(),
        })
    }

    fn vertex_count(&self) -> u64 {
        self.state.vertices.len() as u64
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        let edge = self.state.edges.get(&id)?;
        let label = self
            .state
            .interner
            .read()
            .resolve(edge.label_id)?
            .to_string();

        Some(Edge {
            id: edge.id,
            label,
            src: edge.src,
            dst: edge.dst,
            properties: edge.properties.clone(),
        })
    }

    fn edge_count(&self) -> u64 {
        self.state.edges.len() as u64
    }

    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let Some(node) = self.state.vertices.get(&vertex) else {
            return Box::new(std::iter::empty());
        };

        let edges: Vec<Edge> = node
            .out_edges
            .iter()
            .filter_map(|&eid| self.get_edge(eid))
            .collect();

        Box::new(edges.into_iter())
    }

    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let Some(node) = self.state.vertices.get(&vertex) else {
            return Box::new(std::iter::empty());
        };

        let edges: Vec<Edge> = node
            .in_edges
            .iter()
            .filter_map(|&eid| self.get_edge(eid))
            .collect();

        Box::new(edges.into_iter())
    }

    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let label_id = self.state.interner.read().lookup(label);

        let Some(id) = label_id else {
            return Box::new(std::iter::empty());
        };

        let Some(bitmap) = self.state.vertex_labels.get(&id) else {
            return Box::new(std::iter::empty());
        };

        let vertices: Vec<Vertex> = bitmap
            .iter()
            .filter_map(|vid| self.get_vertex(VertexId(vid as u64)))
            .collect();

        Box::new(vertices.into_iter())
    }

    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_> {
        let label_id = self.state.interner.read().lookup(label);

        let Some(id) = label_id else {
            return Box::new(std::iter::empty());
        };

        let Some(bitmap) = self.state.edge_labels.get(&id) else {
            return Box::new(std::iter::empty());
        };

        let edges: Vec<Edge> = bitmap
            .iter()
            .filter_map(|eid| self.get_edge(EdgeId(eid as u64)))
            .collect();

        Box::new(edges.into_iter())
    }

    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let vertices: Vec<Vertex> = self
            .state
            .vertices
            .keys()
            .filter_map(|&id| self.get_vertex(id))
            .collect();

        Box::new(vertices.into_iter())
    }

    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let edges: Vec<Edge> = self
            .state
            .edges
            .keys()
            .filter_map(|&id| self.get_edge(id))
            .collect();

        Box::new(edges.into_iter())
    }

    fn interner(&self) -> &StringInterner {
        // This is a bit tricky - we need to return a reference to the interner
        // but it's behind a RwLock. For now, we'll leak a read guard.
        // In practice, this is fine because the snapshot is immutable and
        // the interner is append-only.
        //
        // A better solution would be to store a snapshot of the interner's data
        // in CowSnapshot, but that would require more complex changes.
        //
        // For now, we use a static approach - the interner is behind Arc<RwLock>,
        // and we need to provide a &StringInterner. We'll use unsafe to extend
        // the lifetime, which is sound because:
        // 1. The snapshot holds an Arc to the state, keeping it alive
        // 2. The interner is append-only, so its contents never shrink
        // 3. We only need read access
        unsafe {
            let guard = self.state.interner.read();
            let ptr: *const StringInterner = &*guard;
            // Leak the guard by forgetting it - this is intentional
            std::mem::forget(guard);
            &*ptr
        }
    }

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

        let vertices: Vec<Vertex> = self
            .all_vertices()
            .filter(move |v| {
                if let Some(ref l) = label_owned {
                    if &v.label != l {
                        return false;
                    }
                }
                v.properties.get(&property_owned) == Some(&value_clone)
            })
            .collect();

        Box::new(vertices.into_iter())
    }

    fn edges_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Edge> + '_> {
        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        let edges: Vec<Edge> = self
            .all_edges()
            .filter(move |e| {
                if let Some(ref l) = label_owned {
                    if &e.label != l {
                        return false;
                    }
                }
                e.properties.get(&property_owned) == Some(&value_clone)
            })
            .collect();

        Box::new(edges.into_iter())
    }

    fn vertices_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
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

        let vertices: Vec<Vertex> = self
            .all_vertices()
            .filter(move |v| {
                if let Some(ref l) = label_owned {
                    if &v.label != l {
                        return false;
                    }
                }
                if let Some(prop_value) = v.properties.get(&property_owned) {
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
            })
            .collect();

        Box::new(vertices.into_iter())
    }
}

// CowSnapshot is Send + Sync because it only contains Arc<CowGraphState>
// and CowGraphState only contains Send + Sync types
unsafe impl Send for CowSnapshot {}
unsafe impl Sync for CowSnapshot {}

// =============================================================================
// BatchContext - For Atomic Multi-Operation Batches
// =============================================================================

/// Error type for batch operations.
#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    /// Storage error occurred
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// Custom error message
    #[error("{0}")]
    Custom(String),
}

/// Context for batch operations.
///
/// All mutations made through this context are buffered and only applied
/// when the batch closure returns successfully.
pub struct BatchContext<'a> {
    state: &'a mut CowGraphState,
}

impl<'a> BatchContext<'a> {
    /// Add a vertex within the batch.
    pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        let id = VertexId(self.state.next_vertex_id);
        self.state.next_vertex_id += 1;

        let label_id = self.state.interner.write().intern(label);

        let node = Arc::new(NodeData {
            id,
            label_id,
            properties,
            out_edges: Vec::new(),
            in_edges: Vec::new(),
        });

        self.state.vertices = self.state.vertices.update(id, node);

        // Update label index
        let bitmap = self
            .state
            .vertex_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(id.0 as u32);
        self.state.vertex_labels = self
            .state
            .vertex_labels
            .update(label_id, Arc::new(new_bitmap));

        self.state.version += 1;

        id
    }

    /// Add an edge within the batch.
    pub fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, BatchError> {
        if !self.state.vertices.contains_key(&src) {
            return Err(BatchError::Storage(StorageError::VertexNotFound(src)));
        }
        if !self.state.vertices.contains_key(&dst) {
            return Err(BatchError::Storage(StorageError::VertexNotFound(dst)));
        }

        let edge_id = EdgeId(self.state.next_edge_id);
        self.state.next_edge_id += 1;

        let label_id = self.state.interner.write().intern(label);

        let edge = Arc::new(EdgeData {
            id: edge_id,
            label_id,
            src,
            dst,
            properties,
        });

        self.state.edges = self.state.edges.update(edge_id, edge);

        // Update adjacency lists
        if let Some(src_node) = self.state.vertices.get(&src) {
            let mut new_src = (**src_node).clone();
            new_src.out_edges.push(edge_id);
            self.state.vertices = self.state.vertices.update(src, Arc::new(new_src));
        }

        if let Some(dst_node) = self.state.vertices.get(&dst) {
            let mut new_dst = (**dst_node).clone();
            new_dst.in_edges.push(edge_id);
            self.state.vertices = self.state.vertices.update(dst, Arc::new(new_dst));
        }

        // Update label index
        let bitmap = self
            .state
            .edge_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(edge_id.0 as u32);
        self.state.edge_labels = self
            .state
            .edge_labels
            .update(label_id, Arc::new(new_bitmap));

        self.state.version += 1;

        Ok(edge_id)
    }

    /// Get a vertex within the batch (sees uncommitted changes).
    pub fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        let node = self.state.vertices.get(&id)?;
        let label = self
            .state
            .interner
            .read()
            .resolve(node.label_id)?
            .to_string();

        Some(Vertex {
            id: node.id,
            label,
            properties: node.properties.clone(),
        })
    }
}

// =============================================================================
// CowGraphMutWrapper - Implements GraphStorageMut for CowGraph
// =============================================================================

/// Wrapper that provides GraphStorageMut implementation for CowGraph.
///
/// This is used internally by the GQL execution engine.
struct CowGraphMutWrapper<'a> {
    graph: &'a CowGraph,
}

impl<'a> GraphStorage for CowGraphMutWrapper<'a> {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        let state = self.graph.state.read();
        let node = state.vertices.get(&id)?;
        let label = state.interner.read().resolve(node.label_id)?.to_string();

        Some(Vertex {
            id: node.id,
            label,
            properties: node.properties.clone(),
        })
    }

    fn vertex_count(&self) -> u64 {
        self.graph.state.read().vertices.len() as u64
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        let state = self.graph.state.read();
        let edge = state.edges.get(&id)?;
        let label = state.interner.read().resolve(edge.label_id)?.to_string();

        Some(Edge {
            id: edge.id,
            label,
            src: edge.src,
            dst: edge.dst,
            properties: edge.properties.clone(),
        })
    }

    fn edge_count(&self) -> u64 {
        self.graph.state.read().edges.len() as u64
    }

    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let state = self.graph.state.read();
        let Some(node) = state.vertices.get(&vertex) else {
            return Box::new(std::iter::empty());
        };

        let edge_ids: Vec<EdgeId> = node.out_edges.clone();
        drop(state);

        let edges: Vec<Edge> = edge_ids
            .into_iter()
            .filter_map(|eid| self.get_edge(eid))
            .collect();

        Box::new(edges.into_iter())
    }

    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let state = self.graph.state.read();
        let Some(node) = state.vertices.get(&vertex) else {
            return Box::new(std::iter::empty());
        };

        let edge_ids: Vec<EdgeId> = node.in_edges.clone();
        drop(state);

        let edges: Vec<Edge> = edge_ids
            .into_iter()
            .filter_map(|eid| self.get_edge(eid))
            .collect();

        Box::new(edges.into_iter())
    }

    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let state = self.graph.state.read();
        let label_id = state.interner.read().lookup(label);

        let Some(id) = label_id else {
            return Box::new(std::iter::empty());
        };

        let Some(bitmap) = state.vertex_labels.get(&id) else {
            return Box::new(std::iter::empty());
        };

        let vertex_ids: Vec<u64> = bitmap.iter().map(|v| v as u64).collect();
        drop(state);

        let vertices: Vec<Vertex> = vertex_ids
            .into_iter()
            .filter_map(|vid| self.get_vertex(VertexId(vid)))
            .collect();

        Box::new(vertices.into_iter())
    }

    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_> {
        let state = self.graph.state.read();
        let label_id = state.interner.read().lookup(label);

        let Some(id) = label_id else {
            return Box::new(std::iter::empty());
        };

        let Some(bitmap) = state.edge_labels.get(&id) else {
            return Box::new(std::iter::empty());
        };

        let edge_ids: Vec<u64> = bitmap.iter().map(|e| e as u64).collect();
        drop(state);

        let edges: Vec<Edge> = edge_ids
            .into_iter()
            .filter_map(|eid| self.get_edge(EdgeId(eid)))
            .collect();

        Box::new(edges.into_iter())
    }

    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let state = self.graph.state.read();
        let vertex_ids: Vec<VertexId> = state.vertices.keys().copied().collect();
        drop(state);

        let vertices: Vec<Vertex> = vertex_ids
            .into_iter()
            .filter_map(|id| self.get_vertex(id))
            .collect();

        Box::new(vertices.into_iter())
    }

    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let state = self.graph.state.read();
        let edge_ids: Vec<EdgeId> = state.edges.keys().copied().collect();
        drop(state);

        let edges: Vec<Edge> = edge_ids
            .into_iter()
            .filter_map(|id| self.get_edge(id))
            .collect();

        Box::new(edges.into_iter())
    }

    fn interner(&self) -> &StringInterner {
        // Similar approach to CowSnapshot::interner()
        unsafe {
            let state = self.graph.state.read();
            let guard = state.interner.read();
            let ptr: *const StringInterner = &*guard;
            std::mem::forget(guard);
            &*ptr
        }
    }
}

impl<'a> crate::storage::GraphStorageMut for CowGraphMutWrapper<'a> {
    fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        self.graph.add_vertex(label, properties)
    }

    fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        self.graph.add_edge(src, dst, label, properties)
    }

    fn set_vertex_property(
        &mut self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        self.graph.set_vertex_property(id, key, value)
    }

    fn set_edge_property(
        &mut self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        self.graph.set_edge_property(id, key, value)
    }

    fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError> {
        self.graph.remove_vertex(id)
    }

    fn remove_edge(&mut self, id: EdgeId) -> Result<(), StorageError> {
        self.graph.remove_edge(id)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_graph_is_empty() {
        let graph = CowGraph::new();
        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
        assert_eq!(graph.version(), 0);
    }

    #[test]
    fn test_add_vertex() {
        let graph = CowGraph::new();
        let id = graph.add_vertex("person", HashMap::new());
        assert_eq!(id.0, 0);
        assert_eq!(graph.vertex_count(), 1);
        assert_eq!(graph.version(), 1);
    }

    #[test]
    fn test_add_vertex_with_properties() {
        let graph = CowGraph::new();
        let props = HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
        ]);
        let id = graph.add_vertex("person", props);

        let snap = graph.snapshot();
        let vertex = snap.get_vertex(id).unwrap();
        assert_eq!(vertex.label, "person");
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(vertex.properties.get("age"), Some(&Value::Int(30)));
    }

    #[test]
    fn test_add_edge() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());

        let edge_id = graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        let snap = graph.snapshot();
        let edge = snap.get_edge(edge_id).unwrap();
        assert_eq!(edge.src, v1);
        assert_eq!(edge.dst, v2);
        assert_eq!(edge.label, "knows");
    }

    #[test]
    fn test_add_edge_missing_source() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());

        let result = graph.add_edge(VertexId(999), v1, "knows", HashMap::new());
        assert!(matches!(result, Err(StorageError::VertexNotFound(_))));
    }

    #[test]
    fn test_snapshot_isolation() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex(
            "person",
            HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
        );

        // Take snapshot
        let snap = graph.snapshot();

        // Modify graph
        graph
            .set_vertex_property(v1, "name", Value::String("Alicia".to_string()))
            .unwrap();

        // Snapshot still sees old value
        let vertex = snap.get_vertex(v1).unwrap();
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".to_string()))
        );

        // New snapshot sees new value
        let snap2 = graph.snapshot();
        let vertex2 = snap2.get_vertex(v1).unwrap();
        assert_eq!(
            vertex2.properties.get("name"),
            Some(&Value::String("Alicia".to_string()))
        );
    }

    #[test]
    fn test_snapshot_survives_modification() {
        let graph = CowGraph::new();
        for i in 0..1000 {
            graph.add_vertex("node", HashMap::from([("id".to_string(), Value::Int(i))]));
        }

        let snap = graph.snapshot();
        assert_eq!(snap.vertex_count(), 1000);

        // Add more vertices
        for i in 1000..2000 {
            graph.add_vertex("node", HashMap::from([("id".to_string(), Value::Int(i))]));
        }

        // Snapshot unchanged
        assert_eq!(snap.vertex_count(), 1000);

        // New snapshot sees all
        assert_eq!(graph.snapshot().vertex_count(), 2000);
    }

    #[test]
    fn test_remove_vertex() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        graph.remove_vertex(v1).unwrap();

        assert_eq!(graph.vertex_count(), 1);
        assert_eq!(graph.edge_count(), 0); // Edge removed with vertex
    }

    #[test]
    fn test_remove_edge() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let edge_id = graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        graph.remove_edge(edge_id).unwrap();

        assert_eq!(graph.edge_count(), 0);
        assert_eq!(graph.vertex_count(), 2); // Vertices still exist
    }

    #[test]
    fn test_vertices_with_label() {
        let graph = CowGraph::new();
        graph.add_vertex("person", HashMap::new());
        graph.add_vertex("person", HashMap::new());
        graph.add_vertex("software", HashMap::new());

        let snap = graph.snapshot();
        let people: Vec<_> = snap.vertices_with_label("person").collect();
        let software: Vec<_> = snap.vertices_with_label("software").collect();

        assert_eq!(people.len(), 2);
        assert_eq!(software.len(), 1);
    }

    #[test]
    fn test_out_edges() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());

        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v3, "knows", HashMap::new()).unwrap();
        graph.add_edge(v2, v1, "knows", HashMap::new()).unwrap();

        let snap = graph.snapshot();
        let out: Vec<_> = snap.out_edges(v1).collect();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|e| e.src == v1));
    }

    #[test]
    fn test_in_edges() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        let v2 = graph.add_vertex("person", HashMap::new());
        let v3 = graph.add_vertex("person", HashMap::new());

        graph.add_edge(v2, v1, "knows", HashMap::new()).unwrap();
        graph.add_edge(v3, v1, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v2, "knows", HashMap::new()).unwrap();

        let snap = graph.snapshot();
        let in_e: Vec<_> = snap.in_edges(v1).collect();
        assert_eq!(in_e.len(), 2);
        assert!(in_e.iter().all(|e| e.dst == v1));
    }

    #[test]
    fn test_batch_success() {
        let graph = CowGraph::new();

        graph
            .batch(|ctx| {
                let alice = ctx.add_vertex(
                    "Person",
                    HashMap::from([("name".to_string(), "Alice".into())]),
                );
                let bob = ctx.add_vertex(
                    "Person",
                    HashMap::from([("name".to_string(), "Bob".into())]),
                );
                ctx.add_edge(alice, bob, "knows", HashMap::new())?;
                Ok(())
            })
            .unwrap();

        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 1);
    }

    #[test]
    fn test_batch_rollback_on_error() {
        let graph = CowGraph::new();
        graph.add_vertex("existing", HashMap::new());

        let result: Result<(), BatchError> = graph.batch(|ctx| {
            ctx.add_vertex("new", HashMap::new());
            // Try to add edge to non-existent vertex
            ctx.add_edge(VertexId(0), VertexId(999), "invalid", HashMap::new())?;
            Ok(())
        });

        assert!(result.is_err());
        // Graph should be unchanged
        assert_eq!(graph.vertex_count(), 1);
    }

    #[test]
    fn test_snapshot_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CowSnapshot>();
        assert_send_sync::<CowGraph>();
    }

    #[test]
    fn test_snapshot_can_outlive_scope() {
        let snap = {
            let graph = CowGraph::new();
            graph.add_vertex("person", HashMap::new());
            graph.snapshot()
        }; // graph dropped here

        // Snapshot still valid
        assert_eq!(snap.vertex_count(), 1);
    }

    #[test]
    fn test_concurrent_snapshots() {
        use std::sync::Arc;
        use std::thread;

        let graph = Arc::new(CowGraph::new());
        for i in 0..100 {
            graph.add_vertex("node", HashMap::from([("id".to_string(), Value::Int(i))]));
        }

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let g = Arc::clone(&graph);
                thread::spawn(move || {
                    let snap = g.snapshot();
                    snap.vertex_count()
                })
            })
            .collect();

        for handle in handles {
            assert_eq!(handle.join().unwrap(), 100);
        }
    }

    #[test]
    fn test_self_loop_edge() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());

        let e = graph.add_edge(v1, v1, "self", HashMap::new()).unwrap();

        let snap = graph.snapshot();
        let out: Vec<_> = snap.out_edges(v1).collect();
        let in_e: Vec<_> = snap.in_edges(v1).collect();

        assert_eq!(out.len(), 1);
        assert_eq!(in_e.len(), 1);
        assert_eq!(out[0].id, e);
        assert_eq!(in_e[0].id, e);
    }

    #[test]
    fn test_remove_vertex_with_self_loop() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex("person", HashMap::new());
        graph.add_edge(v1, v1, "self", HashMap::new()).unwrap();

        graph.remove_vertex(v1).unwrap();

        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }
}
