//! Copy-on-Write wrapper for memory-mapped persistent graph storage.
//!
//! This module provides [`CowMmapGraph`], which combines the persistence of
//! [`MmapGraph`] with the snapshot isolation of [`CowGraph`].
//!
//! # Architecture
//!
//! `CowMmapGraph` uses a hybrid architecture:
//!
//! - **Disk layer**: `MmapGraph` handles persistence, WAL, and crash recovery
//! - **COW layer**: `CowGraphState` provides in-memory snapshot isolation
//! - **Sync protocol**: Mutations apply to both layers atomically
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        CowMmapGraph                              │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌─────────────────┐    ┌─────────────────────────────────┐    │
//! │  │   MmapGraph     │◄──►│   RwLock<CowGraphState>         │    │
//! │  │  (persistence)  │    │   (in-memory COW layer)         │    │
//! │  └─────────────────┘    └─────────────────────────────────┘    │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```no_run
//! use interstellar::storage::cow_mmap::CowMmapGraph;
//! use interstellar::storage::GraphStorage;
//! use std::collections::HashMap;
//!
//! // Open or create a persistent COW graph
//! let graph = CowMmapGraph::open("my_graph.db").unwrap();
//!
//! // Add data (persisted to disk)
//! let alice = graph.add_vertex("person", HashMap::from([
//!     ("name".to_string(), "Alice".into()),
//! ])).unwrap();
//!
//! // Take a snapshot (O(1), lock-free)
//! let snapshot = graph.snapshot();
//!
//! // Mutations don't affect the snapshot
//! graph.add_vertex("person", HashMap::new()).unwrap();
//! assert_eq!(snapshot.vertex_count(), 1);
//! ```
//!
//! # Thread Safety
//!
//! Both `CowMmapGraph` and `CowMmapSnapshot` are `Send + Sync`:
//!
//! - Writers are serialized via RwLock
//! - Snapshots are fully independent (no locks held)
//! - Snapshots can be sent to other threads and can outlive the source graph

use std::collections::HashMap;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use roaring::RoaringBitmap;

use crate::error::StorageError;
use crate::gql::{self, GqlError};
use crate::index::{
    BTreeIndex, ElementType, IndexError, IndexSpec, IndexType, PropertyIndex, UniqueIndex,
};
use crate::schema::GraphSchema;
use crate::storage::cow::{CowGraphState, EdgeData, NodeData};
use crate::storage::interner::StringInterner;
use crate::storage::mmap::MmapGraph;
use crate::storage::{Edge, GraphStorage, Vertex};
use crate::value::{EdgeId, Value, VertexId};

// =============================================================================
// CowMmapGraph
// =============================================================================

/// Persistent graph with Copy-on-Write snapshot support.
///
/// Combines `MmapGraph` persistence with `CowGraph` snapshot semantics.
/// Data is stored durably on disk while providing O(1) snapshot creation.
///
/// # Creating a Graph
///
/// ```no_run
/// use interstellar::storage::cow_mmap::CowMmapGraph;
///
/// // Open or create a database
/// let graph = CowMmapGraph::open("my_graph.db").unwrap();
/// ```
///
/// # Snapshots
///
/// Snapshots are O(1) and independent of the source graph:
///
/// ```no_run
/// use interstellar::storage::cow_mmap::CowMmapGraph;
/// use interstellar::storage::GraphStorage;
///
/// let graph = CowMmapGraph::open("my_graph.db").unwrap();
/// let snapshot = graph.snapshot();
///
/// // snapshot can be sent to another thread, outlive the graph, etc.
/// std::thread::spawn(move || {
///     for vertex in snapshot.all_vertices() {
///         println!("{:?}", vertex);
///     }
/// });
/// ```
pub struct CowMmapGraph {
    /// Underlying persistent storage
    mmap: MmapGraph,

    /// COW state for snapshot isolation
    state: RwLock<CowGraphState>,

    /// Optional schema for validation
    schema: RwLock<Option<GraphSchema>>,

    /// Property indexes for efficient lookups.
    /// Indexes are stored separately from state because they are mutable
    /// and don't need snapshot isolation (they always reflect current state).
    indexes: RwLock<HashMap<String, Box<dyn PropertyIndex>>>,
}

impl CowMmapGraph {
    /// Open or create a database file.
    ///
    /// If the file exists, it is opened and all data is loaded into the
    /// COW layer. If the file doesn't exist, a new database is created.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file (`.db` extension recommended)
    ///
    /// # Errors
    ///
    /// - [`StorageError::InvalidFormat`] - File has invalid header
    /// - [`StorageError::Io`] - I/O error opening file
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    ///
    /// let graph = CowMmapGraph::open("my_graph.db").unwrap();
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let mmap = MmapGraph::open(path)?;
        let state = Self::load_state_from_mmap(&mmap);

        Ok(Self {
            mmap,
            state: RwLock::new(state),
            schema: RwLock::new(None),
            indexes: RwLock::new(HashMap::new()),
        })
    }

    /// Open or create a database file with a schema.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file
    /// * `schema` - Schema for validation
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use interstellar::schema::{SchemaBuilder, PropertyType, ValidationMode};
    ///
    /// let schema = SchemaBuilder::new()
    ///     .mode(ValidationMode::Strict)
    ///     .vertex("Person")
    ///         .property("name", PropertyType::String)
    ///         .done()
    ///     .build();
    ///
    /// let graph = CowMmapGraph::open_with_schema("my_graph.db", schema).unwrap();
    /// ```
    pub fn open_with_schema<P: AsRef<Path>>(
        path: P,
        schema: GraphSchema,
    ) -> Result<Self, StorageError> {
        let mmap = MmapGraph::open(path)?;
        let state = Self::load_state_from_mmap(&mmap);

        Ok(Self {
            mmap,
            state: RwLock::new(state),
            schema: RwLock::new(Some(schema)),
            indexes: RwLock::new(HashMap::new()),
        })
    }

    /// Load all data from MmapGraph into a CowGraphState.
    ///
    /// This scans all vertices and edges from disk and builds the in-memory
    /// COW representation with proper adjacency lists.
    fn load_state_from_mmap(mmap: &MmapGraph) -> CowGraphState {
        let mut state = CowGraphState::new();

        // Track max IDs to set next_*_id counters
        let mut max_vertex_id: u64 = 0;
        let mut max_edge_id: u64 = 0;

        // Load all vertices - intern labels as we go
        for vertex in mmap.all_vertices() {
            let label_id = state.interner.write().intern(&vertex.label);

            let node = Arc::new(NodeData {
                id: vertex.id,
                label_id,
                properties: vertex.properties,
                out_edges: Vec::new(), // Will be populated from edges
                in_edges: Vec::new(),
            });

            state.vertices = state.vertices.update(vertex.id, node);

            // Track max ID
            if vertex.id.0 >= max_vertex_id {
                max_vertex_id = vertex.id.0 + 1;
            }

            // Update label index
            let bitmap = state
                .vertex_labels
                .get(&label_id)
                .cloned()
                .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
            let mut new_bitmap = (*bitmap).clone();
            new_bitmap.insert(vertex.id.0 as u32);
            state.vertex_labels = state.vertex_labels.update(label_id, Arc::new(new_bitmap));
        }

        // Load all edges and build adjacency lists
        for edge in mmap.all_edges() {
            let label_id = state.interner.write().intern(&edge.label);

            let edge_data = Arc::new(EdgeData {
                id: edge.id,
                label_id,
                src: edge.src,
                dst: edge.dst,
                properties: edge.properties,
            });

            state.edges = state.edges.update(edge.id, edge_data);

            // Track max ID
            if edge.id.0 >= max_edge_id {
                max_edge_id = edge.id.0 + 1;
            }

            // Update edge label index
            let bitmap = state
                .edge_labels
                .get(&label_id)
                .cloned()
                .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
            let mut new_bitmap = (*bitmap).clone();
            new_bitmap.insert(edge.id.0 as u32);
            state.edge_labels = state.edge_labels.update(label_id, Arc::new(new_bitmap));

            // Update source vertex's out_edges
            if let Some(src_node) = state.vertices.get(&edge.src) {
                let mut new_node = (**src_node).clone();
                new_node.out_edges.push(edge.id);
                state.vertices = state.vertices.update(edge.src, Arc::new(new_node));
            }

            // Update destination vertex's in_edges
            if let Some(dst_node) = state.vertices.get(&edge.dst) {
                let mut new_node = (**dst_node).clone();
                new_node.in_edges.push(edge.id);
                state.vertices = state.vertices.update(edge.dst, Arc::new(new_node));
            }
        }

        // Set ID counters based on max IDs seen
        state.next_vertex_id = max_vertex_id;
        state.next_edge_id = max_edge_id;
        state.version = 0; // Fresh load

        state
    }

    // =========================================================================
    // Snapshot Operations
    // =========================================================================

    /// Create an immutable snapshot of the current graph state.
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
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use interstellar::storage::GraphStorage;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowMmapGraph::open("test.db").unwrap();
    /// graph.add_vertex("person", HashMap::new()).unwrap();
    ///
    /// let snap = graph.snapshot();
    /// assert_eq!(snap.vertex_count(), 1);
    ///
    /// // Mutations after snapshot don't affect it
    /// graph.add_vertex("person", HashMap::new()).unwrap();
    /// assert_eq!(snap.vertex_count(), 1); // Still 1
    /// ```
    pub fn snapshot(&self) -> CowMmapSnapshot {
        let state = self.state.read();
        CowMmapSnapshot {
            state: Arc::new((*state).clone()),
        }
    }

    /// Get the current version number.
    ///
    /// The version increments with each mutation.
    pub fn version(&self) -> u64 {
        self.state.read().version
    }

    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Returns the total number of vertices in the graph.
    pub fn vertex_count(&self) -> u64 {
        self.state.read().vertices.len() as u64
    }

    /// Returns the total number of edges in the graph.
    pub fn edge_count(&self) -> u64 {
        self.state.read().edges.len() as u64
    }

    /// Get a vertex by ID.
    pub fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        let state = self.state.read();
        state.vertices.get(&id).map(|node| {
            let label = state
                .interner
                .read()
                .resolve(node.label_id)
                .unwrap_or("")
                .to_string();
            Vertex {
                id: node.id,
                label,
                properties: node.properties.clone(),
            }
        })
    }

    /// Get an edge by ID.
    pub fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        let state = self.state.read();
        state.edges.get(&id).map(|edge| {
            let label = state
                .interner
                .read()
                .resolve(edge.label_id)
                .unwrap_or("")
                .to_string();
            Edge {
                id: edge.id,
                label,
                src: edge.src,
                dst: edge.dst,
                properties: edge.properties.clone(),
            }
        })
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
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use interstellar::index::IndexBuilder;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowMmapGraph::open("test.db").unwrap();
    ///
    /// // Add some data first
    /// graph.add_vertex("person", HashMap::from([
    ///     ("age".to_string(), 30i64.into()),
    /// ])).unwrap();
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
    /// The vertex is added to both the COW layer (for immediate visibility)
    /// and the disk layer (for persistence).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowMmapGraph::open("test.db").unwrap();
    /// let id = graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ])).unwrap();
    /// ```
    pub fn add_vertex(
        &self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<VertexId, StorageError> {
        // Write to disk FIRST to get the actual ID
        // This ensures COW state stays consistent with disk
        let id = self.mmap.add_vertex(label, properties.clone())?;

        let mut state = self.state.write();

        // Update next_vertex_id if needed (for future COW-only operations)
        if id.0 >= state.next_vertex_id {
            state.next_vertex_id = id.0 + 1;
        }

        // Intern label
        let label_id = state.interner.write().intern(label);

        // Create node in COW state
        let node = Arc::new(NodeData {
            id,
            label_id,
            properties: properties.clone(),
            out_edges: Vec::new(),
            in_edges: Vec::new(),
        });

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

        Ok(id)
    }

    /// Add an edge connecting two vertices.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::VertexNotFound`] if either endpoint doesn't exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use std::collections::HashMap;
    ///
    /// let graph = CowMmapGraph::open("test.db").unwrap();
    /// let alice = graph.add_vertex("person", HashMap::new()).unwrap();
    /// let bob = graph.add_vertex("person", HashMap::new()).unwrap();
    /// let edge = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    /// ```
    pub fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        // Check COW state first for endpoint existence
        {
            let state = self.state.read();
            if !state.vertices.contains_key(&src) {
                return Err(StorageError::VertexNotFound(src));
            }
            if !state.vertices.contains_key(&dst) {
                return Err(StorageError::VertexNotFound(dst));
            }
        }

        // Write to disk FIRST to get the actual ID
        // This ensures COW state stays consistent with disk
        let id = self.mmap.add_edge(src, dst, label, properties.clone())?;

        let mut state = self.state.write();

        // Update next_edge_id if needed
        if id.0 >= state.next_edge_id {
            state.next_edge_id = id.0 + 1;
        }

        // Intern label
        let label_id = state.interner.write().intern(label);

        // Create edge
        let edge = Arc::new(EdgeData {
            id,
            label_id,
            src,
            dst,
            properties: properties.clone(),
        });

        state.edges = state.edges.update(id, edge);

        // Update edge label index
        let bitmap = state
            .edge_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(id.0 as u32);
        state.edge_labels = state.edge_labels.update(label_id, Arc::new(new_bitmap));

        // Update source vertex's out_edges
        if let Some(src_node) = state.vertices.get(&src) {
            let mut new_node = (**src_node).clone();
            new_node.out_edges.push(id);
            state.vertices = state.vertices.update(src, Arc::new(new_node));
        }

        // Update destination vertex's in_edges
        if let Some(dst_node) = state.vertices.get(&dst) {
            let mut new_node = (**dst_node).clone();
            new_node.in_edges.push(id);
            state.vertices = state.vertices.update(dst, Arc::new(new_node));
        }

        // Increment version
        state.version += 1;

        // Release state lock before updating indexes (to avoid deadlock)
        drop(state);

        // Update property indexes
        self.index_edge_insert(id, label, &properties);

        Ok(id)
    }

    /// Set or update a property on a vertex.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::VertexNotFound`] if the vertex doesn't exist.
    pub fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write();

        // Update COW state
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

        let mut new_node = (**node).clone();
        new_node.properties.insert(key.to_string(), value.clone());
        state.vertices = state.vertices.update(id, Arc::new(new_node));

        // Increment version
        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes
        self.update_vertex_property_in_indexes(id, &label, key, old_value.as_ref(), &value)?;

        // Write to disk
        self.mmap.set_vertex_property(id, key, value)?;

        Ok(())
    }

    /// Set or update a property on an edge.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::EdgeNotFound`] if the edge doesn't exist.
    pub fn set_edge_property(
        &self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write();

        // Update COW state
        let edge = state.edges.get(&id).ok_or(StorageError::EdgeNotFound(id))?;

        // Get label and old value for index update
        let label = state
            .interner
            .read()
            .resolve(edge.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        let old_value = edge.properties.get(key).cloned();

        let mut new_edge = (**edge).clone();
        new_edge.properties.insert(key.to_string(), value.clone());
        state.edges = state.edges.update(id, Arc::new(new_edge));

        // Increment version
        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes
        self.update_edge_property_in_indexes(id, &label, key, old_value.as_ref(), &value)?;

        // Write to disk
        self.mmap.set_edge_property(id, key, value)?;

        Ok(())
    }

    /// Remove a vertex and all its incident edges.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::VertexNotFound`] if the vertex doesn't exist.
    pub fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError> {
        let mut state = self.state.write();

        // Get the node to find incident edges
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

        // Remove incident edges from COW state
        for (edge_id, _, _) in &edges_to_remove {
            // Clone the edge data we need before mutating state
            let edge_info = state.edges.get(edge_id).map(|e| (e.label_id, e.src, e.dst));

            if let Some((label_id, src, dst)) = edge_info {
                // Update edge label index
                if let Some(bitmap) = state.edge_labels.get(&label_id) {
                    let mut new_bitmap = (**bitmap).clone();
                    new_bitmap.remove(edge_id.0 as u32);
                    state.edge_labels = state.edge_labels.update(label_id, Arc::new(new_bitmap));
                }

                // Update other endpoint's adjacency list
                let other_vertex = if src == id { dst } else { src };
                if let Some(other_node) = state.vertices.get(&other_vertex) {
                    let mut new_node = (**other_node).clone();
                    new_node.out_edges.retain(|e| e != edge_id);
                    new_node.in_edges.retain(|e| e != edge_id);
                    state.vertices = state.vertices.update(other_vertex, Arc::new(new_node));
                }

                state.edges = state.edges.without(edge_id);
            }
        }

        // Remove vertex from label index
        if let Some(bitmap) = state.vertex_labels.get(&node.label_id) {
            let mut new_bitmap = (**bitmap).clone();
            new_bitmap.remove(id.0 as u32);
            state.vertex_labels = state
                .vertex_labels
                .update(node.label_id, Arc::new(new_bitmap));
        }

        // Remove vertex from COW state
        state.vertices = state.vertices.without(&id);

        // Increment version
        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes - remove vertex
        self.index_vertex_remove(id, &label, &properties);

        // Update property indexes - remove edges
        for (edge_id, edge_label, edge_props) in edges_to_remove {
            self.index_edge_remove(edge_id, &edge_label, &edge_props);
        }

        // Write to disk
        self.mmap.remove_vertex(id)?;

        Ok(())
    }

    /// Remove an edge from the graph.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::EdgeNotFound`] if the edge doesn't exist.
    pub fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError> {
        let mut state = self.state.write();

        // Get edge info
        let edge = state
            .edges
            .get(&id)
            .ok_or(StorageError::EdgeNotFound(id))?
            .clone();

        // Get label and properties for index removal
        let label = state
            .interner
            .read()
            .resolve(edge.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        let properties = edge.properties.clone();

        // Update source vertex's out_edges
        if let Some(src_node) = state.vertices.get(&edge.src) {
            let mut new_node = (**src_node).clone();
            new_node.out_edges.retain(|e| *e != id);
            state.vertices = state.vertices.update(edge.src, Arc::new(new_node));
        }

        // Update destination vertex's in_edges
        if let Some(dst_node) = state.vertices.get(&edge.dst) {
            let mut new_node = (**dst_node).clone();
            new_node.in_edges.retain(|e| *e != id);
            state.vertices = state.vertices.update(edge.dst, Arc::new(new_node));
        }

        // Update edge label index
        if let Some(bitmap) = state.edge_labels.get(&edge.label_id) {
            let mut new_bitmap = (**bitmap).clone();
            new_bitmap.remove(id.0 as u32);
            state.edge_labels = state
                .edge_labels
                .update(edge.label_id, Arc::new(new_bitmap));
        }

        // Remove edge from COW state
        state.edges = state.edges.without(&id);

        // Increment version
        state.version += 1;

        // Release state lock before updating indexes
        drop(state);

        // Update property indexes
        self.index_edge_remove(id, &label, &properties);

        // Write to disk
        self.mmap.remove_edge(id)?;

        Ok(())
    }

    // =========================================================================
    // Batch Operations
    // =========================================================================

    /// Execute multiple operations atomically.
    ///
    /// All operations in the batch either commit together or none do.
    /// Uses MmapGraph's batch mode for efficient disk writes.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::{CowMmapGraph, BatchError};
    /// use std::collections::HashMap;
    ///
    /// let graph = CowMmapGraph::open("test.db").unwrap();
    ///
    /// let (alice, bob) = graph.batch(|ctx| {
    ///     let alice = ctx.add_vertex("person", HashMap::from([
    ///         ("name".to_string(), "Alice".into()),
    ///     ]));
    ///     let bob = ctx.add_vertex("person", HashMap::from([
    ///         ("name".to_string(), "Bob".into()),
    ///     ]));
    ///     ctx.add_edge(alice, bob, "knows", HashMap::new())?;
    ///     Ok((alice, bob))
    /// }).unwrap();
    /// ```
    pub fn batch<F, T>(&self, f: F) -> Result<T, BatchError>
    where
        F: FnOnce(&mut CowMmapBatchContext) -> Result<T, BatchError>,
    {
        // Start MmapGraph batch mode
        self.mmap.begin_batch().map_err(BatchError::Storage)?;

        // Clone current COW state for the batch
        let pending_state = self.state.read().clone();

        let mut ctx = CowMmapBatchContext {
            graph: self,
            pending_state,
            operations: Vec::new(),
        };

        // Execute user's batch function
        match f(&mut ctx) {
            Ok(result) => {
                // Apply all operations to MmapGraph
                for op in &ctx.operations {
                    self.apply_operation_to_mmap(op)
                        .map_err(BatchError::Storage)?;
                }

                // Commit batch (single fsync)
                self.mmap.commit_batch().map_err(BatchError::Storage)?;

                // Update COW state atomically
                *self.state.write() = ctx.pending_state;

                Ok(result)
            }
            Err(e) => {
                // Rollback: abort MmapGraph batch, discard pending state
                let _ = self.mmap.abort_batch();
                Err(e)
            }
        }
    }

    /// Apply a batch operation to the underlying MmapGraph.
    fn apply_operation_to_mmap(&self, op: &BatchOperation) -> Result<(), StorageError> {
        match op {
            BatchOperation::AddVertex { label, properties } => {
                self.mmap.add_vertex(label, properties.clone())?;
            }
            BatchOperation::AddEdge {
                src,
                dst,
                label,
                properties,
            } => {
                self.mmap.add_edge(*src, *dst, label, properties.clone())?;
            }
            BatchOperation::SetVertexProperty { id, key, value } => {
                self.mmap.set_vertex_property(*id, key, value.clone())?;
            }
            BatchOperation::SetEdgeProperty { id, key, value } => {
                self.mmap.set_edge_property(*id, key, value.clone())?;
            }
            BatchOperation::RemoveVertex { id } => {
                self.mmap.remove_vertex(*id)?;
            }
            BatchOperation::RemoveEdge { id } => {
                self.mmap.remove_edge(*id)?;
            }
        }
        Ok(())
    }

    // =========================================================================
    // GQL Mutations
    // =========================================================================

    /// Execute a GQL mutation (CREATE, SET, DELETE, REMOVE).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    ///
    /// let graph = CowMmapGraph::open("test.db").unwrap();
    /// graph.execute_mutation("CREATE (:Person {name: 'Alice'})").unwrap();
    /// ```
    pub fn execute_mutation(&self, gql: &str) -> Result<Vec<Value>, GqlError> {
        self.execute_mutation_with_params(gql, &HashMap::new())
    }

    /// Execute a GQL mutation with parameters.
    pub fn execute_mutation_with_params(
        &self,
        gql: &str,
        _params: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, GqlError> {
        let stmt = gql::parse_statement(gql)?;

        if stmt.is_read_only() {
            return Err(GqlError::Mutation(
                "Read-only queries not supported via execute_mutation(). \
                 Use snapshot() and the standard GQL APIs for reads."
                    .to_string(),
            ));
        }

        // Execute mutation atomically
        let mut wrapper = CowMmapGraphMutWrapper { graph: self };
        let schema = self.schema();
        gql::execute_mutation_with_schema(&stmt, &mut wrapper, schema.as_ref())
            .map_err(|e| GqlError::Mutation(e.to_string()))
    }

    // =========================================================================
    // Persistence Operations
    // =========================================================================

    /// Force a checkpoint, ensuring all data is durably written to disk.
    ///
    /// This syncs the data file and truncates the WAL.
    pub fn checkpoint(&self) -> Result<(), StorageError> {
        self.mmap.checkpoint()
    }

    /// Check if the graph is currently in batch mode.
    pub fn is_batch_mode(&self) -> bool {
        self.mmap.is_batch_mode()
    }

    /// Access the underlying MmapGraph for advanced operations.
    ///
    /// # Warning
    ///
    /// Direct mutations to the MmapGraph will not be reflected in the COW layer.
    /// Use this only for read operations or when you know what you're doing.
    pub fn mmap_graph(&self) -> &MmapGraph {
        &self.mmap
    }
}

// =============================================================================
// CowMmapSnapshot
// =============================================================================

/// Immutable, owned snapshot of a persistent graph.
///
/// Snapshots are cheap to create (O(1)) and can be used independently
/// of the source graph. They implement [`GraphStorage`] for compatibility
/// with the traversal engine.
///
/// # Thread Safety
///
/// `CowMmapSnapshot` is `Send + Sync` and can be freely shared across threads.
pub struct CowMmapSnapshot {
    state: Arc<CowGraphState>,
}

impl CowMmapSnapshot {
    /// Get the version at which this snapshot was taken.
    pub fn version(&self) -> u64 {
        self.state.version
    }
}

impl Clone for CowMmapSnapshot {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

// SAFETY: CowMmapSnapshot only contains Arc<CowGraphState> which is Send + Sync
unsafe impl Send for CowMmapSnapshot {}
unsafe impl Sync for CowMmapSnapshot {}

impl GraphStorage for CowMmapSnapshot {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.state.vertices.get(&id).map(|node| {
            let label = self
                .state
                .interner
                .read()
                .resolve(node.label_id)
                .unwrap_or("")
                .to_string();
            Vertex {
                id: node.id,
                label,
                properties: node.properties.clone(),
            }
        })
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        self.state.edges.get(&id).map(|edge| {
            let label = self
                .state
                .interner
                .read()
                .resolve(edge.label_id)
                .unwrap_or("")
                .to_string();
            Edge {
                id: edge.id,
                label,
                src: edge.src,
                dst: edge.dst,
                properties: edge.properties.clone(),
            }
        })
    }

    fn vertex_count(&self) -> u64 {
        self.state.vertices.len() as u64
    }

    fn edge_count(&self) -> u64 {
        self.state.edges.len() as u64
    }

    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        match self.state.vertices.get(&vertex) {
            Some(node) => {
                let edge_ids = node.out_edges.clone();
                Box::new(edge_ids.into_iter().filter_map(|id| self.get_edge(id)))
            }
            None => Box::new(std::iter::empty()),
        }
    }

    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        match self.state.vertices.get(&vertex) {
            Some(node) => {
                let edge_ids = node.in_edges.clone();
                Box::new(edge_ids.into_iter().filter_map(|id| self.get_edge(id)))
            }
            None => Box::new(std::iter::empty()),
        }
    }

    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let label_id = self.state.interner.read().lookup(label);

        match label_id.and_then(|id| self.state.vertex_labels.get(&id).cloned()) {
            Some(bitmap) => {
                // Collect IDs first to avoid lifetime issues with bitmap
                let vertex_ids: Vec<u64> = bitmap.iter().map(|id| id as u64).collect();
                Box::new(
                    vertex_ids
                        .into_iter()
                        .filter_map(move |id| self.get_vertex(VertexId(id))),
                )
            }
            None => Box::new(std::iter::empty()),
        }
    }

    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_> {
        let label_id = self.state.interner.read().lookup(label);

        match label_id.and_then(|id| self.state.edge_labels.get(&id).cloned()) {
            Some(bitmap) => {
                // Collect IDs first to avoid lifetime issues with bitmap
                let edge_ids: Vec<u64> = bitmap.iter().map(|id| id as u64).collect();
                Box::new(
                    edge_ids
                        .into_iter()
                        .filter_map(move |id| self.get_edge(EdgeId(id))),
                )
            }
            None => Box::new(std::iter::empty()),
        }
    }

    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let vertex_ids: Vec<VertexId> = self.state.vertices.keys().copied().collect();
        Box::new(
            vertex_ids
                .into_iter()
                .filter_map(move |id| self.get_vertex(id)),
        )
    }

    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let edge_ids: Vec<EdgeId> = self.state.edges.keys().copied().collect();
        Box::new(edge_ids.into_iter().filter_map(move |id| self.get_edge(id)))
    }

    fn interner(&self) -> &StringInterner {
        // SAFETY: We leak the read guard to get a 'static lifetime reference.
        // This is safe because the StringInterner lives as long as the snapshot.
        let guard = self.state.interner.read();
        let ptr = &*guard as *const StringInterner;
        std::mem::forget(guard);
        unsafe { &*ptr }
    }
}

// =============================================================================
// Batch Context
// =============================================================================

/// Error type for batch operations.
#[derive(Debug)]
pub enum BatchError {
    /// Storage layer error
    Storage(StorageError),
    /// Vertex not found
    VertexNotFound(VertexId),
    /// Edge not found
    EdgeNotFound(EdgeId),
    /// Custom error message
    Custom(String),
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchError::Storage(e) => write!(f, "storage error: {}", e),
            BatchError::VertexNotFound(id) => write!(f, "vertex not found: {:?}", id),
            BatchError::EdgeNotFound(id) => write!(f, "edge not found: {:?}", id),
            BatchError::Custom(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for BatchError {}

impl From<StorageError> for BatchError {
    fn from(e: StorageError) -> Self {
        BatchError::Storage(e)
    }
}

/// Recorded batch operation for replay to MmapGraph.
enum BatchOperation {
    AddVertex {
        label: String,
        properties: HashMap<String, Value>,
    },
    AddEdge {
        src: VertexId,
        dst: VertexId,
        label: String,
        properties: HashMap<String, Value>,
    },
    SetVertexProperty {
        id: VertexId,
        key: String,
        value: Value,
    },
    SetEdgeProperty {
        id: EdgeId,
        key: String,
        value: Value,
    },
    RemoveVertex {
        id: VertexId,
    },
    RemoveEdge {
        id: EdgeId,
    },
}

/// Context for atomic batch operations on CowMmapGraph.
pub struct CowMmapBatchContext<'g> {
    #[allow(dead_code)] // Reserved for future use (e.g., reading during batch)
    graph: &'g CowMmapGraph,
    pending_state: CowGraphState,
    operations: Vec<BatchOperation>,
}

impl<'g> CowMmapBatchContext<'g> {
    /// Add a vertex to the batch.
    pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        // Allocate ID
        let id = VertexId(self.pending_state.next_vertex_id);
        self.pending_state.next_vertex_id += 1;

        // Intern label
        let label_id = self.pending_state.interner.write().intern(label);

        // Create node
        let node = Arc::new(NodeData {
            id,
            label_id,
            properties: properties.clone(),
            out_edges: Vec::new(),
            in_edges: Vec::new(),
        });

        self.pending_state.vertices = self.pending_state.vertices.update(id, node);

        // Update label index
        let bitmap = self
            .pending_state
            .vertex_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(id.0 as u32);
        self.pending_state.vertex_labels = self
            .pending_state
            .vertex_labels
            .update(label_id, Arc::new(new_bitmap));

        // Increment version
        self.pending_state.version += 1;

        // Record operation
        self.operations.push(BatchOperation::AddVertex {
            label: label.to_string(),
            properties,
        });

        id
    }

    /// Add an edge to the batch.
    pub fn add_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, BatchError> {
        // Verify endpoints exist
        if !self.pending_state.vertices.contains_key(&src) {
            return Err(BatchError::VertexNotFound(src));
        }
        if !self.pending_state.vertices.contains_key(&dst) {
            return Err(BatchError::VertexNotFound(dst));
        }

        // Allocate ID
        let id = EdgeId(self.pending_state.next_edge_id);
        self.pending_state.next_edge_id += 1;

        // Intern label
        let label_id = self.pending_state.interner.write().intern(label);

        // Create edge
        let edge = Arc::new(EdgeData {
            id,
            label_id,
            src,
            dst,
            properties: properties.clone(),
        });

        self.pending_state.edges = self.pending_state.edges.update(id, edge);

        // Update edge label index
        let bitmap = self
            .pending_state
            .edge_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(id.0 as u32);
        self.pending_state.edge_labels = self
            .pending_state
            .edge_labels
            .update(label_id, Arc::new(new_bitmap));

        // Update adjacency lists
        if let Some(src_node) = self.pending_state.vertices.get(&src) {
            let mut new_node = (**src_node).clone();
            new_node.out_edges.push(id);
            self.pending_state.vertices =
                self.pending_state.vertices.update(src, Arc::new(new_node));
        }
        if let Some(dst_node) = self.pending_state.vertices.get(&dst) {
            let mut new_node = (**dst_node).clone();
            new_node.in_edges.push(id);
            self.pending_state.vertices =
                self.pending_state.vertices.update(dst, Arc::new(new_node));
        }

        // Increment version
        self.pending_state.version += 1;

        // Record operation
        self.operations.push(BatchOperation::AddEdge {
            src,
            dst,
            label: label.to_string(),
            properties,
        });

        Ok(id)
    }

    /// Set a vertex property in the batch.
    pub fn set_vertex_property(
        &mut self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), BatchError> {
        let node = self
            .pending_state
            .vertices
            .get(&id)
            .ok_or(BatchError::VertexNotFound(id))?;

        let mut new_node = (**node).clone();
        new_node.properties.insert(key.to_string(), value.clone());
        self.pending_state.vertices = self.pending_state.vertices.update(id, Arc::new(new_node));

        self.pending_state.version += 1;

        self.operations.push(BatchOperation::SetVertexProperty {
            id,
            key: key.to_string(),
            value,
        });

        Ok(())
    }

    /// Set an edge property in the batch.
    pub fn set_edge_property(
        &mut self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), BatchError> {
        let edge = self
            .pending_state
            .edges
            .get(&id)
            .ok_or(BatchError::EdgeNotFound(id))?;

        let mut new_edge = (**edge).clone();
        new_edge.properties.insert(key.to_string(), value.clone());
        self.pending_state.edges = self.pending_state.edges.update(id, Arc::new(new_edge));

        self.pending_state.version += 1;

        self.operations.push(BatchOperation::SetEdgeProperty {
            id,
            key: key.to_string(),
            value,
        });

        Ok(())
    }

    /// Remove a vertex in the batch.
    pub fn remove_vertex(&mut self, id: VertexId) -> Result<(), BatchError> {
        let node = self
            .pending_state
            .vertices
            .get(&id)
            .ok_or(BatchError::VertexNotFound(id))?
            .clone();

        // Collect edges to remove
        let edges_to_remove: Vec<EdgeId> = node
            .out_edges
            .iter()
            .chain(node.in_edges.iter())
            .copied()
            .collect();

        // Remove incident edges
        for edge_id in &edges_to_remove {
            if let Some(edge) = self.pending_state.edges.get(edge_id) {
                let label_id = edge.label_id;

                // Update edge label index
                if let Some(bitmap) = self.pending_state.edge_labels.get(&label_id) {
                    let mut new_bitmap = (**bitmap).clone();
                    new_bitmap.remove(edge_id.0 as u32);
                    self.pending_state.edge_labels = self
                        .pending_state
                        .edge_labels
                        .update(label_id, Arc::new(new_bitmap));
                }

                // Update other endpoint's adjacency list
                let other_vertex = if edge.src == id { edge.dst } else { edge.src };
                if let Some(other_node) = self.pending_state.vertices.get(&other_vertex) {
                    let mut new_node = (**other_node).clone();
                    new_node.out_edges.retain(|e| e != edge_id);
                    new_node.in_edges.retain(|e| e != edge_id);
                    self.pending_state.vertices = self
                        .pending_state
                        .vertices
                        .update(other_vertex, Arc::new(new_node));
                }

                self.pending_state.edges = self.pending_state.edges.without(edge_id);
            }
        }

        // Remove vertex from label index
        if let Some(bitmap) = self.pending_state.vertex_labels.get(&node.label_id) {
            let mut new_bitmap = (**bitmap).clone();
            new_bitmap.remove(id.0 as u32);
            self.pending_state.vertex_labels = self
                .pending_state
                .vertex_labels
                .update(node.label_id, Arc::new(new_bitmap));
        }

        // Remove vertex
        self.pending_state.vertices = self.pending_state.vertices.without(&id);

        self.pending_state.version += 1;

        self.operations.push(BatchOperation::RemoveVertex { id });

        Ok(())
    }

    /// Remove an edge in the batch.
    pub fn remove_edge(&mut self, id: EdgeId) -> Result<(), BatchError> {
        let edge = self
            .pending_state
            .edges
            .get(&id)
            .ok_or(BatchError::EdgeNotFound(id))?
            .clone();

        // Update source vertex's out_edges
        if let Some(src_node) = self.pending_state.vertices.get(&edge.src) {
            let mut new_node = (**src_node).clone();
            new_node.out_edges.retain(|e| *e != id);
            self.pending_state.vertices = self
                .pending_state
                .vertices
                .update(edge.src, Arc::new(new_node));
        }

        // Update destination vertex's in_edges
        if let Some(dst_node) = self.pending_state.vertices.get(&edge.dst) {
            let mut new_node = (**dst_node).clone();
            new_node.in_edges.retain(|e| *e != id);
            self.pending_state.vertices = self
                .pending_state
                .vertices
                .update(edge.dst, Arc::new(new_node));
        }

        // Update edge label index
        if let Some(bitmap) = self.pending_state.edge_labels.get(&edge.label_id) {
            let mut new_bitmap = (**bitmap).clone();
            new_bitmap.remove(id.0 as u32);
            self.pending_state.edge_labels = self
                .pending_state
                .edge_labels
                .update(edge.label_id, Arc::new(new_bitmap));
        }

        // Remove edge
        self.pending_state.edges = self.pending_state.edges.without(&id);

        self.pending_state.version += 1;

        self.operations.push(BatchOperation::RemoveEdge { id });

        Ok(())
    }
}

// =============================================================================
// Internal: GQL Mutation Wrapper
// =============================================================================

/// Internal wrapper to provide mutable storage interface for GQL mutations.
struct CowMmapGraphMutWrapper<'a> {
    graph: &'a CowMmapGraph,
}

impl<'a> GraphStorage for CowMmapGraphMutWrapper<'a> {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.graph.get_vertex(id)
    }

    fn vertex_count(&self) -> u64 {
        self.graph.vertex_count()
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        self.graph.get_edge(id)
    }

    fn edge_count(&self) -> u64 {
        self.graph.edge_count()
    }

    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let snapshot = self.graph.snapshot();
        let edges: Vec<Edge> = snapshot.out_edges(vertex).collect();
        Box::new(edges.into_iter())
    }

    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let snapshot = self.graph.snapshot();
        let edges: Vec<Edge> = snapshot.in_edges(vertex).collect();
        Box::new(edges.into_iter())
    }

    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let snapshot = self.graph.snapshot();
        let vertices: Vec<Vertex> = snapshot.vertices_with_label(label).collect();
        Box::new(vertices.into_iter())
    }

    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_> {
        let snapshot = self.graph.snapshot();
        let edges: Vec<Edge> = snapshot.edges_with_label(label).collect();
        Box::new(edges.into_iter())
    }

    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_> {
        let snapshot = self.graph.snapshot();
        let vertices: Vec<Vertex> = snapshot.all_vertices().collect();
        Box::new(vertices.into_iter())
    }

    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_> {
        let snapshot = self.graph.snapshot();
        let edges: Vec<Edge> = snapshot.all_edges().collect();
        Box::new(edges.into_iter())
    }

    fn interner(&self) -> &StringInterner {
        // This is a bit tricky - we need to return a reference that lives long enough
        // We'll use the snapshot's interner
        let state = self.graph.state.read();
        let guard = state.interner.read();
        let ptr = &*guard as *const StringInterner;
        std::mem::forget(guard);
        unsafe { &*ptr }
    }
}

impl<'a> crate::storage::GraphStorageMut for CowMmapGraphMutWrapper<'a> {
    fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        self.graph
            .add_vertex(label, properties)
            .expect("add_vertex failed during GQL mutation")
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
    use tempfile::tempdir;

    fn temp_db_path() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        (dir, path)
    }

    #[test]
    fn test_open_new_database() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();
        assert_eq!(graph.vertex_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }

    #[test]
    fn test_add_vertex() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();

        let id = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
            )
            .unwrap();

        assert_eq!(graph.vertex_count(), 1);
        let vertex = graph.get_vertex(id).unwrap();
        assert_eq!(vertex.label, "person");
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn test_add_edge() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();

        let alice = graph.add_vertex("person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("person", HashMap::new()).unwrap();
        let edge = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        assert_eq!(graph.edge_count(), 1);
        let e = graph.get_edge(edge).unwrap();
        assert_eq!(e.label, "knows");
        assert_eq!(e.src, alice);
        assert_eq!(e.dst, bob);
    }

    #[test]
    fn test_snapshot_isolation() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();

        // Add first vertex
        graph.add_vertex("person", HashMap::new()).unwrap();
        let snap1 = graph.snapshot();

        // Add second vertex
        graph.add_vertex("person", HashMap::new()).unwrap();
        let snap2 = graph.snapshot();

        // Snapshots should see different states
        assert_eq!(snap1.vertex_count(), 1);
        assert_eq!(snap2.vertex_count(), 2);
        assert_eq!(graph.vertex_count(), 2);
    }

    #[test]
    fn test_snapshot_send_sync() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();
        graph.add_vertex("person", HashMap::new()).unwrap();

        let snapshot = graph.snapshot();

        // Spawn thread to verify Send + Sync
        let handle = std::thread::spawn(move || {
            assert_eq!(snapshot.vertex_count(), 1);
        });

        handle.join().unwrap();
    }

    #[test]
    fn test_persistence() {
        let (_dir, path) = temp_db_path();

        // Create and populate graph
        {
            let graph = CowMmapGraph::open(&path).unwrap();
            graph
                .add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
                )
                .unwrap();
            graph.checkpoint().unwrap();
        }

        // Reopen and verify
        {
            let graph = CowMmapGraph::open(&path).unwrap();
            assert_eq!(graph.vertex_count(), 1);
            let vertex = graph.get_vertex(VertexId(0)).unwrap();
            assert_eq!(vertex.label, "person");
            assert_eq!(
                vertex.properties.get("name"),
                Some(&Value::String("Alice".into()))
            );
        }
    }

    #[test]
    fn test_batch_operations() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();

        let (alice, bob, edge) = graph
            .batch(|ctx| {
                let alice = ctx.add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
                );
                let bob = ctx.add_vertex(
                    "person",
                    HashMap::from([("name".to_string(), Value::String("Bob".into()))]),
                );
                let edge = ctx.add_edge(alice, bob, "knows", HashMap::new())?;
                Ok((alice, bob, edge))
            })
            .unwrap();

        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 1);
        assert!(graph.get_vertex(alice).is_some());
        assert!(graph.get_vertex(bob).is_some());
        assert!(graph.get_edge(edge).is_some());
    }

    #[test]
    fn test_batch_rollback() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();

        // Add initial vertex
        graph.add_vertex("person", HashMap::new()).unwrap();
        assert_eq!(graph.vertex_count(), 1);

        // Failed batch should rollback
        let result: Result<(), BatchError> = graph.batch(|ctx| {
            ctx.add_vertex("person", HashMap::new());
            // Try to add edge to non-existent vertex
            ctx.add_edge(VertexId(100), VertexId(101), "knows", HashMap::new())?;
            Ok(())
        });

        assert!(result.is_err());
        // Should still have only 1 vertex
        assert_eq!(graph.vertex_count(), 1);
    }

    #[test]
    fn test_remove_vertex() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();

        let alice = graph.add_vertex("person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("person", HashMap::new()).unwrap();
        let _edge = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        assert_eq!(graph.vertex_count(), 2);
        assert_eq!(graph.edge_count(), 1);

        // Remove Alice (should also remove the edge)
        graph.remove_vertex(alice).unwrap();

        assert_eq!(graph.vertex_count(), 1);
        assert_eq!(graph.edge_count(), 0);
        assert!(graph.get_vertex(alice).is_none());
        assert!(graph.get_vertex(bob).is_some());
    }

    #[test]
    fn test_remove_edge() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();

        let alice = graph.add_vertex("person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("person", HashMap::new()).unwrap();
        let edge = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        assert_eq!(graph.edge_count(), 1);

        graph.remove_edge(edge).unwrap();

        assert_eq!(graph.edge_count(), 0);
        assert_eq!(graph.vertex_count(), 2);
    }

    #[test]
    fn test_set_vertex_property() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();

        let id = graph.add_vertex("person", HashMap::new()).unwrap();
        graph
            .set_vertex_property(id, "name", Value::String("Alice".into()))
            .unwrap();

        let vertex = graph.get_vertex(id).unwrap();
        assert_eq!(
            vertex.properties.get("name"),
            Some(&Value::String("Alice".into()))
        );
    }

    #[test]
    fn test_adjacency_via_snapshot() {
        let (_dir, path) = temp_db_path();
        let graph = CowMmapGraph::open(&path).unwrap();

        let alice = graph.add_vertex("person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("person", HashMap::new()).unwrap();
        let charlie = graph.add_vertex("person", HashMap::new()).unwrap();

        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        graph
            .add_edge(alice, charlie, "knows", HashMap::new())
            .unwrap();

        let snapshot = graph.snapshot();

        let out_edges: Vec<_> = snapshot.out_edges(alice).collect();
        assert_eq!(out_edges.len(), 2);

        let in_edges: Vec<_> = snapshot.in_edges(bob).collect();
        assert_eq!(in_edges.len(), 1);
        assert_eq!(in_edges[0].src, alice);
    }
}
