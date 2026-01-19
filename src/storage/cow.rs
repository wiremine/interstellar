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
use crate::index::{
    BTreeIndex, ElementType, IndexError, IndexSpec, IndexType, PropertyIndex, UniqueIndex,
};
use crate::schema::GraphSchema;
use crate::storage::interner::StringInterner;
use crate::storage::{Edge, GraphStorage, Vertex};
use crate::traversal::mutation::{DropStep, PendingMutation, PropertyStep};
use crate::traversal::{
    ExecutionContext, HasLabelStep, HasStep, HasValueStep, IdStep, InEStep, InStep, InVStep,
    LabelStep, LimitStep, OutEStep, OutStep, OutVStep, SkipStep, Traversal, TraversalSource,
    Traverser, ValuesStep,
};
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

    /// Property indexes for efficient lookups.
    /// Indexes are stored separately from state because they are mutable
    /// and don't need snapshot isolation (they always reflect current state).
    indexes: RwLock<HashMap<String, Box<dyn PropertyIndex>>>,
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
            indexes: RwLock::new(HashMap::new()),
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
            indexes: RwLock::new(HashMap::new()),
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
        // Clone the interner to avoid shared lock issues
        let interner_snapshot = Arc::new(state.interner.read().clone());
        CowSnapshot {
            state: Arc::new((*state).clone()),
            interner_snapshot,
        }
    }

    /// Create a traversal source for this graph.
    ///
    /// The returned [`CowTraversalSource`] provides a unified API for both
    /// reads and mutations. Any mutations in the traversal are automatically
    /// executed when terminal steps are called.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    ///
    /// let graph = CowGraph::new();
    /// let g = graph.traversal();
    ///
    /// // Create vertices
    /// let alice = g.add_v("Person").property("name", "Alice").next();
    /// let bob = g.add_v("Person").property("name", "Bob").next();
    ///
    /// // Read
    /// assert_eq!(g.v().count(), 2);
    /// ```
    pub fn traversal(&self) -> CowTraversalSource<'_> {
        CowTraversalSource::new(self)
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
    /// use interstellar::storage::cow::CowGraph;
    /// use interstellar::index::IndexBuilder;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowGraph::new();
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
    pub fn create_index(&self, spec: IndexSpec) -> Result<(), IndexError> {
        let mut indexes = self.indexes.write();

        // Check for duplicate name
        if indexes.contains_key(&spec.name) {
            return Err(IndexError::AlreadyExists(spec.name.clone()));
        }

        // Create the appropriate index type
        let mut index: Box<dyn PropertyIndex> = match spec.index_type {
            IndexType::BTree => Box::new(BTreeIndex::new(spec.clone())?),
            IndexType::Unique => Box::new(UniqueIndex::new(spec.clone())?),
        };

        // Populate index with existing data
        let state = self.state.read();
        Self::populate_index_internal(&state, &mut *index)?;

        indexes.insert(spec.name.clone(), index);
        Ok(())
    }

    /// Drops an index by name.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::NotFound`] if no index with that name exists.
    pub fn drop_index(&self, name: &str) -> Result<(), IndexError> {
        self.indexes
            .write()
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| IndexError::NotFound(name.to_string()))
    }

    /// Returns a vector of all index specifications.
    pub fn list_indexes(&self) -> Vec<IndexSpec> {
        self.indexes
            .read()
            .values()
            .map(|idx| idx.spec().clone())
            .collect()
    }

    /// Checks if an index with the given name exists.
    pub fn has_index(&self, name: &str) -> bool {
        self.indexes.read().contains_key(name)
    }

    /// Returns the number of indexes.
    pub fn index_count(&self) -> usize {
        self.indexes.read().len()
    }

    /// Returns whether this storage supports indexes.
    pub fn supports_indexes(&self) -> bool {
        true
    }

    // =========================================================================
    // Index Query Methods
    // =========================================================================

    /// Lookup vertices by indexed property value.
    ///
    /// If an applicable index exists, uses it for O(log n) or O(1) lookup.
    /// Otherwise falls back to O(n) scan.
    pub fn vertices_by_property(
        &self,
        label: Option<&str>,
        property: &str,
        value: &Value,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let indexes = self.indexes.read();
        let state = self.state.read();

        // Try to find an applicable index
        for index in indexes.values() {
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

            // Use index
            let ids: Vec<u64> = index.lookup_eq(value).collect();
            let label_owned = label.map(|s| s.to_string());

            // Need to release locks and collect results
            drop(indexes);
            drop(state);

            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| {
                        let state = self.state.read();
                        let node = state.vertices.get(&VertexId(id))?;
                        let label = state.interner.read().resolve(node.label_id)?.to_string();
                        Some(Vertex {
                            id: node.id,
                            label,
                            properties: node.properties.clone(),
                        })
                    })
                    .filter(move |v| {
                        label_owned.is_none() || Some(v.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }

        // Fall back to scan - need to release locks first
        drop(indexes);
        drop(state);

        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        // Collect to avoid lifetime issues with snapshot
        let vertices: Vec<Vertex> = self
            .snapshot()
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
        let indexes = self.indexes.read();
        let state = self.state.read();

        // Try to find an applicable index
        for index in indexes.values() {
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

            // Use index
            let ids: Vec<u64> = index.lookup_eq(value).collect();
            let label_owned = label.map(|s| s.to_string());

            // Need to release locks and collect results
            drop(indexes);
            drop(state);

            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| {
                        let state = self.state.read();
                        let edge = state.edges.get(&EdgeId(id))?;
                        let label = state.interner.read().resolve(edge.label_id)?.to_string();
                        Some(Edge {
                            id: edge.id,
                            label,
                            src: edge.src,
                            dst: edge.dst,
                            properties: edge.properties.clone(),
                        })
                    })
                    .filter(move |e| {
                        label_owned.is_none() || Some(e.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }

        // Fall back to scan - need to release locks first
        drop(indexes);
        drop(state);

        let label_owned = label.map(|s| s.to_string());
        let property_owned = property.to_string();
        let value_clone = value.clone();

        // Collect to avoid lifetime issues with snapshot
        let edges: Vec<Edge> = self
            .snapshot()
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

    /// Lookup vertices by property range, using indexes if available.
    pub fn vertices_by_property_range(
        &self,
        label: Option<&str>,
        property: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let indexes = self.indexes.read();

        // Try to find an applicable BTree index
        for index in indexes.values() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != property {
                continue;
            }
            // BTree indexes support range queries; skip unique indexes
            if spec.index_type != IndexType::BTree {
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

            drop(indexes);

            return Box::new(
                ids.into_iter()
                    .filter_map(move |id| {
                        let state = self.state.read();
                        let node = state.vertices.get(&VertexId(id))?;
                        let label = state.interner.read().resolve(node.label_id)?.to_string();
                        Some(Vertex {
                            id: node.id,
                            label,
                            properties: node.properties.clone(),
                        })
                    })
                    .filter(move |v| {
                        label_owned.is_none() || Some(v.label.as_str()) == label_owned.as_deref()
                    }),
            );
        }

        // Fall back to scan with range filter
        drop(indexes);

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

        // Collect to avoid lifetime issues with snapshot
        let vertices: Vec<Vertex> = self
            .snapshot()
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

    // =========================================================================
    // Internal Index Helpers
    // =========================================================================

    /// Populate an index with existing graph data.
    fn populate_index_internal(
        state: &CowGraphState,
        index: &mut dyn PropertyIndex,
    ) -> Result<(), IndexError> {
        let spec = index.spec().clone();

        match spec.element_type {
            ElementType::Vertex => {
                for (id, node) in state.vertices.iter() {
                    // Check label filter
                    if let Some(ref label) = spec.label {
                        let node_label = state
                            .interner
                            .read()
                            .resolve(node.label_id)
                            .map(|s| s.to_string());
                        if node_label.as_deref() != Some(label.as_str()) {
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
                for (id, edge) in state.edges.iter() {
                    // Check label filter
                    if let Some(ref label) = spec.label {
                        let edge_label = state
                            .interner
                            .read()
                            .resolve(edge.label_id)
                            .map(|s| s.to_string());
                        if edge_label.as_deref() != Some(label.as_str()) {
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
    fn index_vertex_insert(&self, id: VertexId, label: &str, properties: &HashMap<String, Value>) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
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
    fn index_vertex_remove(&self, id: VertexId, label: &str, properties: &HashMap<String, Value>) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
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
                let _ = index.remove(value, id.0);
            }
        }
    }

    /// Update indexes when an edge is added.
    fn index_edge_insert(&self, id: EdgeId, label: &str, properties: &HashMap<String, Value>) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
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
    fn index_edge_remove(&self, id: EdgeId, label: &str, properties: &HashMap<String, Value>) {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
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
                let _ = index.remove(value, id.0);
            }
        }
    }

    /// Update indexes when a vertex property changes.
    fn update_vertex_property_in_indexes(
        &self,
        id: VertexId,
        label: &str,
        property: &str,
        old_value: Option<&Value>,
        new_value: &Value,
    ) -> Result<(), StorageError> {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Vertex {
                continue;
            }
            if spec.property != property {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
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
        &self,
        id: EdgeId,
        label: &str,
        property: &str,
        old_value: Option<&Value>,
        new_value: &Value,
    ) -> Result<(), StorageError> {
        let mut indexes = self.indexes.write();
        for index in indexes.values_mut() {
            let spec = index.spec();
            if spec.element_type != ElementType::Edge {
                continue;
            }
            if spec.property != property {
                continue;
            }
            if let Some(ref idx_label) = spec.label {
                if idx_label != label {
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
            properties: properties.clone(),
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

        // Release state lock before updating indexes (to avoid deadlock)
        drop(state);

        // Update property indexes
        self.index_vertex_insert(id, label, &properties);

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
            properties: properties.clone(),
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

        // Release state lock before updating indexes (to avoid deadlock)
        drop(state);

        // Update property indexes
        self.index_edge_insert(edge_id, label, &properties);

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

        // Get label and old value for index update
        let label = state
            .interner
            .read()
            .resolve(node.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        let old_value = node.properties.get(key).cloned();

        // Clone and modify the node
        let mut new_node = (**node).clone();
        new_node.properties.insert(key.to_string(), value.clone());

        // Update in persistent map
        state.vertices = state.vertices.update(id, Arc::new(new_node));
        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes
        self.update_vertex_property_in_indexes(id, &label, key, old_value.as_ref(), &value)?;

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

        // Get label and old value for index update
        let label = state
            .interner
            .read()
            .resolve(edge.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        let old_value = edge.properties.get(key).cloned();

        // Clone and modify the edge
        let mut new_edge = (**edge).clone();
        new_edge.properties.insert(key.to_string(), value.clone());

        // Update in persistent map
        state.edges = state.edges.update(id, Arc::new(new_edge));
        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes
        self.update_edge_property_in_indexes(id, &label, key, old_value.as_ref(), &value)?;

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

        // Get label for index removal
        let label = state
            .interner
            .read()
            .resolve(node.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        let properties = node.properties.clone();

        // Collect edges to remove with their info for index updates
        let edges_to_remove: Vec<(EdgeId, String, HashMap<String, Value>)> = node
            .out_edges
            .iter()
            .chain(node.in_edges.iter())
            .filter_map(|&edge_id| {
                state.edges.get(&edge_id).map(|e| {
                    let edge_label = state
                        .interner
                        .read()
                        .resolve(e.label_id)
                        .map(|s| s.to_string())
                        .unwrap_or_default();
                    (edge_id, edge_label, e.properties.clone())
                })
            })
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
        for (edge_id, _, _) in &edges_to_remove {
            Self::remove_edge_internal(&mut state, *edge_id, Some(id));
        }

        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes - remove vertex
        self.index_vertex_remove(id, &label, &properties);

        // Update property indexes - remove edges
        for (edge_id, edge_label, edge_props) in edges_to_remove {
            self.index_edge_remove(edge_id, &edge_label, &edge_props);
        }

        Ok(())
    }

    /// Remove an edge from the graph.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::EdgeNotFound` if the edge doesn't exist.
    pub fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError> {
        let mut state = self.state.write();

        let edge = state.edges.get(&id).ok_or(StorageError::EdgeNotFound(id))?;

        // Get label and properties for index removal
        let label = state
            .interner
            .read()
            .resolve(edge.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        let properties = edge.properties.clone();

        Self::remove_edge_internal(&mut state, id, None);
        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes
        self.index_edge_remove(id, &label, &properties);

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
    // GQL API
    // =========================================================================

    /// Execute a GQL mutation statement.
    ///
    /// This method parses and executes GQL mutation statements (CREATE, SET,
    /// DELETE, DETACH DELETE, MERGE).
    ///
    /// For read-only queries, use [`snapshot().gql()`](crate::graph::GraphSnapshot::gql)
    /// instead.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    /// use interstellar::storage::GraphStorage;
    ///
    /// let graph = CowGraph::new();
    ///
    /// // Mutations via gql()
    /// graph.gql("CREATE (n:Person {name: 'Alice'}) RETURN n").unwrap();
    /// graph.gql("MATCH (n:Person) SET n.age = 30").unwrap();
    ///
    /// // Verify the mutation worked
    /// assert_eq!(graph.snapshot().vertex_count(), 1);
    /// ```
    pub fn gql(&self, query: &str) -> Result<Vec<Value>, GqlError> {
        let stmt = gql::parse_statement(query)?;

        if stmt.is_read_only() {
            return Err(GqlError::Mutation(
                "Read-only queries not supported via gql(). \
                 Use snapshot().gql() for reads."
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
// CowTraversalSource - Unified Traversal API with Auto-Mutation
// =============================================================================

/// Entry point for traversals on a [`CowGraph`] with automatic mutation execution.
///
/// Unlike the read-only [`GraphTraversalSource`](crate::traversal::GraphTraversalSource),
/// this traversal source has access to the underlying `CowGraph` and will automatically
/// execute any mutations when terminal steps are called.
///
/// # Unified API
///
/// Both reads and writes use the same API - no separate "mutation mode":
///
/// ```
/// use interstellar::storage::cow::CowGraph;
/// use std::collections::HashMap;
///
/// let graph = CowGraph::new();
/// let g = graph.traversal();
///
/// // Mutations are executed automatically
/// let alice = g.add_v("Person").property("name", "Alice").next();
/// let bob = g.add_v("Person").property("name", "Bob").next();
///
/// // Reads work normally
/// let count = g.v().count();  // 2
/// ```
pub struct CowTraversalSource<'g> {
    graph: &'g CowGraph,
}

impl<'g> CowTraversalSource<'g> {
    /// Create a new traversal source for the given graph.
    pub fn new(graph: &'g CowGraph) -> Self {
        Self { graph }
    }

    /// Start traversal from all vertices.
    pub fn v(&self) -> CowBoundTraversal<'g, (), Value> {
        CowBoundTraversal::new(
            self.graph,
            Traversal::with_source(TraversalSource::AllVertices),
        )
    }

    /// Start traversal from specific vertex IDs.
    pub fn v_ids<I>(&self, ids: I) -> CowBoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = VertexId>,
    {
        CowBoundTraversal::new(
            self.graph,
            Traversal::with_source(TraversalSource::Vertices(ids.into_iter().collect())),
        )
    }

    /// Start traversal from a single vertex ID.
    pub fn v_id(&self, id: VertexId) -> CowBoundTraversal<'g, (), Value> {
        self.v_ids([id])
    }

    /// Start traversal from all edges.
    pub fn e(&self) -> CowBoundTraversal<'g, (), Value> {
        CowBoundTraversal::new(
            self.graph,
            Traversal::with_source(TraversalSource::AllEdges),
        )
    }

    /// Start traversal from specific edge IDs.
    pub fn e_ids<I>(&self, ids: I) -> CowBoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = EdgeId>,
    {
        CowBoundTraversal::new(
            self.graph,
            Traversal::with_source(TraversalSource::Edges(ids.into_iter().collect())),
        )
    }

    /// Start a traversal that creates a new vertex.
    ///
    /// The vertex is created when a terminal step is called.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    ///
    /// let graph = CowGraph::new();
    /// let g = graph.traversal();
    ///
    /// let vertex = g.add_v("Person").property("name", "Alice").next();
    /// assert!(vertex.is_some());
    /// assert_eq!(graph.vertex_count(), 1);
    /// ```
    pub fn add_v(&self, label: impl Into<String>) -> CowBoundTraversal<'g, (), Value> {
        use crate::traversal::mutation::AddVStep;

        let mut traversal = Traversal::<(), Value>::with_source(TraversalSource::Inject(vec![]));
        traversal = traversal.add_step(AddVStep::new(label));
        CowBoundTraversal::new(self.graph, traversal)
    }

    /// Start a traversal that creates a new edge.
    ///
    /// Must specify `from` and `to` vertices before calling a terminal step.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowGraph::new();
    /// let alice = graph.add_vertex("Person", HashMap::new());
    /// let bob = graph.add_vertex("Person", HashMap::new());
    ///
    /// let g = graph.traversal();
    /// let edge = g.add_e("KNOWS").from_id(alice).to_id(bob).next();
    /// assert!(edge.is_some());
    /// assert_eq!(graph.edge_count(), 1);
    /// ```
    pub fn add_e(&self, label: impl Into<String>) -> CowAddEdgeBuilder<'g> {
        CowAddEdgeBuilder::new(self.graph, label.into())
    }

    /// Inject arbitrary values into the traversal stream.
    pub fn inject<I>(&self, values: I) -> CowBoundTraversal<'g, (), Value>
    where
        I: IntoIterator<Item = Value>,
    {
        CowBoundTraversal::new(
            self.graph,
            Traversal::with_source(TraversalSource::Inject(values.into_iter().collect())),
        )
    }
}

// =============================================================================
// CowBoundTraversal - Traversal with Auto-Mutation Execution
// =============================================================================

/// A traversal bound to a [`CowGraph`] with automatic mutation execution.
///
/// When terminal steps (`to_list()`, `next()`, `iterate()`, etc.) are called,
/// any pending mutations in the traversal results are automatically executed
/// against the graph.
pub struct CowBoundTraversal<'g, In, Out> {
    graph: &'g CowGraph,
    traversal: Traversal<In, Out>,
    track_paths: bool,
}

impl<'g, In, Out> CowBoundTraversal<'g, In, Out> {
    /// Create a new bound traversal.
    pub(crate) fn new(graph: &'g CowGraph, traversal: Traversal<In, Out>) -> Self {
        Self {
            graph,
            traversal,
            track_paths: false,
        }
    }

    /// Enable automatic path tracking for this traversal.
    pub fn with_path(mut self) -> Self {
        self.track_paths = true;
        self
    }

    /// Add a step to the traversal.
    pub fn add_step<NewOut>(
        self,
        step: impl crate::traversal::step::AnyStep + 'static,
    ) -> CowBoundTraversal<'g, In, NewOut> {
        CowBoundTraversal {
            graph: self.graph,
            traversal: self.traversal.add_step(step),
            track_paths: self.track_paths,
        }
    }

    /// Append an anonymous traversal's steps.
    pub fn append<Mid>(self, anon: Traversal<Out, Mid>) -> CowBoundTraversal<'g, In, Mid> {
        CowBoundTraversal {
            graph: self.graph,
            traversal: self.traversal.append(anon),
            track_paths: self.track_paths,
        }
    }

    /// Execute the traversal and process any pending mutations.
    ///
    /// Returns an iterator over the results with mutations applied.
    fn execute_with_mutations(self) -> Vec<Value> {
        use crate::traversal::step::{AnyStep, StartStep};
        use crate::traversal::traverser::TraversalSource;

        // For mutation-only traversals (add_v, add_e, property, drop),
        // we can execute without a full ExecutionContext since these steps
        // don't need graph access - they just produce pending mutation markers.
        //
        // This avoids the deadlock issue where CowSnapshot::interner() leaks
        // a read guard that blocks later write operations.

        // Decompose traversal into source and steps
        let (source, steps) = self.traversal.into_steps();

        // Check if this is a mutation-only traversal (source is Inject([]))
        let is_mutation_only = match &source {
            Some(TraversalSource::Inject(values)) if values.is_empty() => true,
            _ => false,
        };

        let results: Vec<Traverser> = if is_mutation_only {
            // For mutation-only traversals, execute steps directly without
            // needing a full ExecutionContext. We create a minimal dummy context.
            let dummy_storage = crate::storage::InMemoryGraph::new();
            let dummy_interner = dummy_storage.interner();
            let storage_ref: &dyn GraphStorage = &dummy_storage;

            let ctx = if self.track_paths {
                ExecutionContext::with_path_tracking(storage_ref, dummy_interner)
            } else {
                ExecutionContext::new(storage_ref, dummy_interner)
            };

            // Start with empty input (since source is Inject([]))
            let mut current: Vec<Traverser> = Vec::new();

            // For add_v, we need to inject a single empty value to trigger the step
            // The add_v step ignores input and produces one pending mutation
            if !steps.is_empty() {
                // Inject a single traverser to trigger the mutation step
                current = vec![Traverser::new(Value::Null)];
            }

            // Apply each step in sequence
            for step in &steps {
                current = step.apply(&ctx, Box::new(current.into_iter())).collect();
            }

            current
        } else {
            // For read traversals that need graph access, use the full path
            // Create a snapshot for read operations
            let snapshot = self.graph.snapshot();
            let interner = snapshot.interner();
            let storage_ref: &dyn GraphStorage = &snapshot;

            // Create execution context - CowSnapshot implements GraphStorage
            let ctx = if self.track_paths {
                ExecutionContext::with_path_tracking(storage_ref, interner)
            } else {
                ExecutionContext::new(storage_ref, interner)
            };

            // Start with source traversers
            let mut current: Vec<Traverser> = match source {
                Some(src) => {
                    let start_step = StartStep::new(src);
                    start_step
                        .apply(&ctx, Box::new(std::iter::empty()))
                        .collect()
                }
                None => Vec::new(),
            };

            // Apply each step in sequence
            for step in &steps {
                current = step.apply(&ctx, Box::new(current.into_iter())).collect();
            }

            current
            // Note: For read-only traversals, we don't need to mutate after,
            // so the leaked guard in interner() is not a problem
        };

        // Process results, executing any pending mutations
        let mut wrapper = CowGraphMutWrapper { graph: self.graph };
        let mut final_results = Vec::with_capacity(results.len());

        for traverser in results {
            if let Some(mutation) = PendingMutation::from_value(&traverser.value) {
                // Execute the mutation and get the result
                if let Some(result) = Self::execute_mutation(&mut wrapper, mutation) {
                    final_results.push(result);
                }
            } else {
                // Not a mutation, pass through
                final_results.push(traverser.value);
            }
        }

        final_results
    }

    /// Execute a single pending mutation.
    fn execute_mutation(
        wrapper: &mut CowGraphMutWrapper<'_>,
        mutation: PendingMutation,
    ) -> Option<Value> {
        use crate::storage::GraphStorageMut;

        match mutation {
            PendingMutation::AddVertex { label, properties } => {
                let id = wrapper.add_vertex(&label, properties);
                Some(Value::Vertex(id))
            }
            PendingMutation::AddEdge {
                label,
                from,
                to,
                properties,
            } => match wrapper.add_edge(from, to, &label, properties) {
                Ok(id) => Some(Value::Edge(id)),
                Err(_) => None,
            },
            PendingMutation::SetVertexProperty { id, key, value } => {
                wrapper.set_vertex_property(id, &key, value).ok()?;
                Some(Value::Vertex(id))
            }
            PendingMutation::SetEdgeProperty { id, key, value } => {
                wrapper.set_edge_property(id, &key, value).ok()?;
                Some(Value::Edge(id))
            }
            PendingMutation::DropVertex { id } => {
                wrapper.remove_vertex(id).ok()?;
                None
            }
            PendingMutation::DropEdge { id } => {
                wrapper.remove_edge(id).ok()?;
                None
            }
        }
    }
}

// Terminal methods on CowBoundTraversal
impl<'g, In, Out> CowBoundTraversal<'g, In, Out> {
    /// Execute and collect all values into a list.
    ///
    /// Any pending mutations in the results are automatically executed.
    pub fn to_list(self) -> Vec<Value> {
        self.execute_with_mutations()
    }

    /// Execute and return the first value, if any.
    ///
    /// Any pending mutations are automatically executed.
    pub fn next(self) -> Option<Value> {
        self.execute_with_mutations().into_iter().next()
    }

    /// Execute and consume the traversal, discarding results.
    ///
    /// Any pending mutations are automatically executed.
    pub fn iterate(self) {
        let _ = self.execute_with_mutations();
    }

    /// Execute and count the number of results.
    pub fn count(self) -> u64 {
        self.execute_with_mutations().len() as u64
    }

    /// Execute and collect unique values into a set.
    pub fn to_set(self) -> std::collections::HashSet<Value> {
        self.execute_with_mutations().into_iter().collect()
    }

    /// Check if the traversal produces any results.
    pub fn has_next(self) -> bool {
        !self.execute_with_mutations().is_empty()
    }
}

// Step methods for CowBoundTraversal<Value>
impl<'g, In> CowBoundTraversal<'g, In, Value> {
    /// Filter elements by label.
    pub fn has_label(self, label: impl Into<String>) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(HasLabelStep::single(label))
    }

    /// Filter to elements that have a specific property key.
    pub fn has(self, key: impl Into<String>) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(HasStep::new(key.into()))
    }

    /// Filter to elements where a property equals a value.
    pub fn has_value(
        self,
        key: impl Into<String>,
        value: Value,
    ) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(HasValueStep::new(key.into(), value))
    }

    /// Traverse to outgoing adjacent vertices.
    pub fn out(self) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(OutStep::new())
    }

    /// Traverse to outgoing adjacent vertices via edges with label.
    pub fn out_label(self, label: impl Into<String>) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(OutStep::with_labels(vec![label.into()]))
    }

    /// Traverse to incoming adjacent vertices.
    pub fn in_(self) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(InStep::new())
    }

    /// Traverse to incoming adjacent vertices via edges with label.
    pub fn in_label(self, label: impl Into<String>) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(InStep::with_labels(vec![label.into()]))
    }

    /// Traverse to outgoing edges.
    pub fn out_e(self) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(OutEStep::new())
    }

    /// Traverse to incoming edges.
    pub fn in_e(self) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(InEStep::new())
    }

    /// Traverse to the target vertex of an edge.
    pub fn in_v(self) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(InVStep)
    }

    /// Traverse to the source vertex of an edge.
    pub fn out_v(self) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(OutVStep)
    }

    /// Get property values by key.
    pub fn values(self, key: impl Into<String>) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(ValuesStep::new(key.into()))
    }

    /// Add a property to the current element (for mutation traversals).
    pub fn property(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(PropertyStep::new(key.into(), value.into()))
    }

    /// Drop (delete) the current element.
    pub fn drop(self) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(DropStep)
    }

    /// Add an edge from the current vertex.
    pub fn add_e(self, label: impl Into<String>) -> CowBoundAddEdgeBuilder<'g, In> {
        CowBoundAddEdgeBuilder::new(self.graph, self.traversal, label.into(), self.track_paths)
    }

    /// Limit results to first n.
    pub fn limit(self, n: usize) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(LimitStep::new(n))
    }

    /// Skip first n results.
    pub fn skip(self, n: usize) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(SkipStep::new(n))
    }

    /// Get element IDs.
    pub fn id(self) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(IdStep)
    }

    /// Get element labels.
    pub fn label(self) -> CowBoundTraversal<'g, In, Value> {
        self.add_step(LabelStep)
    }
}

// =============================================================================
// CowAddEdgeBuilder - Builder for add_e() from traversal source
// =============================================================================

/// Builder for creating edges from the traversal source.
pub struct CowAddEdgeBuilder<'g> {
    graph: &'g CowGraph,
    label: String,
    from: Option<VertexId>,
    to: Option<VertexId>,
    properties: HashMap<String, Value>,
}

impl<'g> CowAddEdgeBuilder<'g> {
    fn new(graph: &'g CowGraph, label: String) -> Self {
        Self {
            graph,
            label,
            from: None,
            to: None,
            properties: HashMap::new(),
        }
    }

    /// Set the source vertex by ID.
    pub fn from_id(mut self, id: VertexId) -> Self {
        self.from = Some(id);
        self
    }

    /// Set the destination vertex by ID.
    pub fn to_id(mut self, id: VertexId) -> Self {
        self.to = Some(id);
        self
    }

    /// Add a property to the edge.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Execute and return the created edge.
    pub fn next(self) -> Option<Value> {
        let from = self.from?;
        let to = self.to?;

        match self.graph.add_edge(from, to, &self.label, self.properties) {
            Ok(id) => Some(Value::Edge(id)),
            Err(_) => None,
        }
    }

    /// Execute, discarding the result.
    pub fn iterate(self) {
        let _ = self.next();
    }

    /// Execute and return results as a list.
    pub fn to_list(self) -> Vec<Value> {
        self.next().into_iter().collect()
    }
}

// =============================================================================
// CowBoundAddEdgeBuilder - Builder for add_e() from traversal
// =============================================================================

/// Builder for creating edges from an existing traversal.
pub struct CowBoundAddEdgeBuilder<'g, In> {
    graph: &'g CowGraph,
    traversal: Traversal<In, Value>,
    label: String,
    to: Option<VertexId>,
    properties: HashMap<String, Value>,
    track_paths: bool,
}

impl<'g, In> CowBoundAddEdgeBuilder<'g, In> {
    fn new(
        graph: &'g CowGraph,
        traversal: Traversal<In, Value>,
        label: String,
        track_paths: bool,
    ) -> Self {
        Self {
            graph,
            traversal,
            label,
            to: None,
            properties: HashMap::new(),
            track_paths,
        }
    }

    /// Set the destination vertex by ID.
    pub fn to_id(mut self, id: VertexId) -> Self {
        self.to = Some(id);
        self
    }

    /// Add a property to the edge.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Build and execute the traversal, creating edges.
    pub fn to_list(self) -> Vec<Value> {
        use crate::traversal::mutation::AddEStep;

        let to_id = match self.to {
            Some(id) => id,
            None => return vec![],
        };

        let mut step = AddEStep::new(&self.label);
        step = step.to_vertex(to_id);
        for (k, v) in self.properties {
            step = step.property(k, v);
        }

        let traversal: Traversal<In, Value> = self.traversal.add_step(step);
        let bound = CowBoundTraversal {
            graph: self.graph,
            traversal,
            track_paths: self.track_paths,
        };

        bound.to_list()
    }

    /// Execute and return the first edge created.
    pub fn next(self) -> Option<Value> {
        self.to_list().into_iter().next()
    }

    /// Execute, discarding results.
    pub fn iterate(self) {
        let _ = self.to_list();
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
    /// Cloned interner - snapshot-local, no shared lock
    interner_snapshot: Arc<StringInterner>,
}

impl CowSnapshot {
    /// Get the snapshot version.
    pub fn version(&self) -> u64 {
        self.state.version
    }

    /// Get the string interner for this snapshot.
    ///
    /// This returns a reference to the snapshot-local cloned interner.
    pub fn interner(&self) -> &StringInterner {
        &self.interner_snapshot
    }

    /// Create a traversal source for this snapshot.
    ///
    /// This provides the full Gremlin-style fluent API for querying the graph.
    /// Since `CowSnapshot` is immutable, only read operations are available.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    /// use interstellar::value::Value;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowGraph::new();
    /// graph.add_vertex("Person", HashMap::from([
    ///     ("name".to_string(), Value::String("Alice".to_string())),
    /// ]));
    ///
    /// let snapshot = graph.snapshot();
    /// let g = snapshot.traversal();
    ///
    /// let count = g.v().has_label("Person").count();
    /// assert_eq!(count, 1);
    /// ```
    pub fn traversal(&self) -> crate::traversal::GraphTraversalSource<'_> {
        crate::traversal::GraphTraversalSource::from_snapshot(self)
    }

    /// Execute a GQL query against this snapshot.
    ///
    /// This provides the full GQL query language for pattern matching
    /// and data retrieval. Since `CowSnapshot` is immutable, only read
    /// queries (MATCH/RETURN) are supported.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    /// use interstellar::value::Value;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowGraph::new();
    /// graph.add_vertex("Person", HashMap::from([
    ///     ("name".to_string(), Value::String("Alice".to_string())),
    /// ]));
    ///
    /// let snapshot = graph.snapshot();
    /// let results = snapshot.gql("MATCH (n:Person) RETURN n.name").unwrap();
    /// assert_eq!(results.len(), 1);
    /// ```
    pub fn gql(&self, query: &str) -> Result<Vec<crate::value::Value>, crate::gql::GqlError> {
        let stmt = crate::gql::parse_statement(query)?;
        let results = crate::gql::compile_statement(&stmt, self)?;
        Ok(results)
    }

    /// Execute a parameterized GQL query against this snapshot.
    ///
    /// Parameters provide a safe way to inject values into queries
    /// without string concatenation, preventing injection attacks.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::cow::CowGraph;
    /// use interstellar::value::Value;
    /// use interstellar::gql::Parameters;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowGraph::new();
    /// graph.add_vertex("Person", HashMap::from([
    ///     ("name".to_string(), Value::String("Alice".to_string())),
    ///     ("age".to_string(), Value::Int(30)),
    /// ]));
    ///
    /// let snapshot = graph.snapshot();
    ///
    /// let mut params = Parameters::new();
    /// params.insert("minAge".to_string(), Value::Int(25));
    ///
    /// let results = snapshot.gql_with_params(
    ///     "MATCH (n:Person) WHERE n.age >= $minAge RETURN n.name",
    ///     &params,
    /// ).unwrap();
    /// assert_eq!(results.len(), 1);
    /// ```
    pub fn gql_with_params(
        &self,
        query: &str,
        params: &crate::gql::Parameters,
    ) -> Result<Vec<crate::value::Value>, crate::gql::GqlError> {
        let stmt = crate::gql::parse_statement(query)?;
        let results = crate::gql::compile_statement_with_params(&stmt, self, params)?;
        Ok(results)
    }
}

// Implement SnapshotLike for CowSnapshot to enable generic traversal/GQL usage.
// Since CowSnapshot implements GraphStorage directly, storage() returns self.
impl crate::traversal::SnapshotLike for CowSnapshot {
    fn storage(&self) -> &dyn GraphStorage {
        self
    }

    fn interner(&self) -> &StringInterner {
        &self.interner_snapshot
    }
}

impl GraphStorage for CowSnapshot {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        let node = self.state.vertices.get(&id)?;
        let label = self.interner_snapshot.resolve(node.label_id)?.to_string();

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
        let label = self.interner_snapshot.resolve(edge.label_id)?.to_string();

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
        let label_id = self.interner_snapshot.lookup(label);

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
        let label_id = self.interner_snapshot.lookup(label);

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
        // Return a reference to the snapshot-local cloned interner.
        // No locking needed since it's owned by this snapshot.
        &self.interner_snapshot
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
