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
//! - **COW layer**: `GraphState` provides in-memory snapshot isolation
//! - **Sync protocol**: Mutations apply to both layers atomically
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        CowMmapGraph                              │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌─────────────────┐    ┌─────────────────────────────────┐    │
//! │  │   MmapGraph     │◄──►│   RwLock<GraphState>         │    │
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
use std::marker::PhantomData;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;
use roaring::RoaringBitmap;

use crate::error::StorageError;
#[cfg(feature = "gql")]
use crate::gql::{self, GqlError};
use crate::graph_elements::{GraphEdge, GraphVertex, PersistentEdge, PersistentVertex};
use crate::index::{
    BTreeIndex, ElementType, IndexError, IndexSpec, IndexType, PropertyIndex, RTreeIndex,
    UniqueIndex,
};
use crate::schema::GraphSchema;
use crate::storage::cow::{EdgeData, GraphState, NodeData};
use crate::storage::interner::StringInterner;
use crate::storage::mmap::MmapGraph;
use crate::storage::{Edge, GraphStorage, StreamableStorage, Vertex};
use crate::traversal::markers::{Edge as EdgeMarker, OutputMarker, Scalar, Vertex as VertexMarker};
use crate::traversal::mutation::{DropStep, PendingMutation, PropertyStep};
use crate::traversal::step::Step;
use crate::traversal::{
    ExecutionContext, HasLabelStep, HasStep, HasValueStep, IdStep, InEStep, InStep, InVStep,
    LabelStep, LimitStep, OutEStep, OutStep, OutVStep, SkipStep, Traversal, TraversalSource,
    Traverser, ValuesStep,
};
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

    /// COW state for snapshot isolation.
    /// Wrapped in Arc to allow reactive snapshot factory closures to
    /// capture a reference to the live graph state.
    state: Arc<RwLock<GraphState>>,

    /// Optional schema for validation
    schema: RwLock<Option<GraphSchema>>,

    /// Property indexes for efficient lookups.
    /// Indexes are stored separately from state because they are mutable
    /// and don't need snapshot isolation (they always reflect current state).
    indexes: RwLock<HashMap<String, Box<dyn PropertyIndex>>>,

    /// Event bus for reactive streaming queries.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    event_bus: std::sync::Arc<crate::storage::events::EventBus>,

    /// Subscription manager for reactive streaming queries.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    subscription_manager: std::sync::Arc<crate::traversal::reactive::SubscriptionManager>,
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

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let event_bus = std::sync::Arc::new(crate::storage::events::EventBus::new());
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let subscription_manager = {
            let eb = event_bus.clone();
            std::sync::Arc::new(crate::traversal::reactive::SubscriptionManager::new(
                std::sync::Arc::new(move || eb.subscribe()),
            ))
        };

        Ok(Self {
            mmap,
            state: Arc::new(RwLock::new(state)),
            schema: RwLock::new(None),
            indexes: RwLock::new(HashMap::new()),
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            event_bus,
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            subscription_manager,
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

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let event_bus = std::sync::Arc::new(crate::storage::events::EventBus::new());
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let subscription_manager = {
            let eb = event_bus.clone();
            std::sync::Arc::new(crate::traversal::reactive::SubscriptionManager::new(
                std::sync::Arc::new(move || eb.subscribe()),
            ))
        };

        Ok(Self {
            mmap,
            state: Arc::new(RwLock::new(state)),
            schema: RwLock::new(Some(schema)),
            indexes: RwLock::new(HashMap::new()),
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            event_bus,
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            subscription_manager,
        })
    }

    /// Load all data from MmapGraph into a GraphState.
    ///
    /// This scans all vertices and edges from disk and builds the in-memory
    /// COW representation with proper adjacency lists.
    fn load_state_from_mmap(mmap: &MmapGraph) -> GraphState {
        let mut state = GraphState::new();

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
        // Clone the interner to avoid shared lock issues
        let interner_snapshot = Arc::new(state.interner.read().clone());
        CowMmapSnapshot {
            state: Arc::new((*state).clone()),
            interner_snapshot,
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            subscription_manager: self.subscription_manager.clone(),
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            snapshot_fn: {
                let state_arc = self.state.clone();
                std::sync::Arc::new(move || {
                    let state = state_arc.read();
                    let interner_snapshot = Arc::new(state.interner.read().clone());
                    let snap = CowMmapSnapshot {
                        state: Arc::new((*state).clone()),
                        interner_snapshot,
                        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
                        subscription_manager: std::sync::Arc::new(
                            crate::traversal::reactive::SubscriptionManager::placeholder(),
                        ),
                        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
                        snapshot_fn: std::sync::Arc::new(|| {
                            panic!("nested snapshot_fn not supported")
                        }),
                    };
                    Box::new(snap)
                })
            },
        }
    }

    /// Create a traversal source for this graph.
    ///
    /// The returned [`CowMmapTraversalSource`] provides a unified API for both
    /// reads and mutations. Any mutations in the traversal are automatically
    /// executed when terminal steps are called.
    ///
    /// Terminal methods like `next()` and `to_list()` return rich types:
    /// - `g.v()` returns `PersistentVertex` objects
    /// - `g.e()` returns `PersistentEdge` objects
    ///
    /// # Arguments
    ///
    /// * `graph_arc` - An `Arc<CowMmapGraph>` for the returned rich element types
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use std::sync::Arc;
    ///
    /// let graph = Arc::new(CowMmapGraph::open("test.db").unwrap());
    /// let g = graph.gremlin(Arc::clone(&graph));
    ///
    /// // Create vertices - returns PersistentVertex
    /// let alice = g.add_v("Person").property("name", "Alice").next();
    /// let bob = g.add_v("Person").property("name", "Bob").next();
    ///
    /// // Read - returns PersistentVertex objects
    /// let vertices = g.v().to_list();
    /// assert_eq!(vertices.len(), 2);
    /// ```
    pub fn gremlin(&self, graph_arc: Arc<CowMmapGraph>) -> CowMmapTraversalSource<'_> {
        CowMmapTraversalSource::new_with_arc(self, graph_arc)
    }

    /// Get the current version number.
    ///
    /// The version increments with each mutation.
    pub fn version(&self) -> u64 {
        self.state.read().version
    }

    /// Get a reference to the event bus for subscribing to mutation events.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    pub fn event_bus(&self) -> &crate::storage::events::EventBus {
        &self.event_bus
    }

    /// Returns the subscription manager for reactive streaming queries.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    pub fn subscription_manager(
        &self,
    ) -> &std::sync::Arc<crate::traversal::reactive::SubscriptionManager> {
        &self.subscription_manager
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
            IndexType::RTree => Box::new(RTreeIndex::new(spec.clone())?),
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
        state: &GraphState,
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

        // Emit reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::VertexAdded {
                id,
                label: label.to_string(),
                properties,
            });
        }

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

        // Emit reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::EdgeAdded {
                id,
                src,
                dst,
                label: label.to_string(),
                properties,
            });
        }

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
        self.mmap.set_vertex_property(id, key, value.clone())?;

        // Emit reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::VertexPropertyChanged {
                id,
                key: key.to_string(),
                old_value,
                new_value: value,
            });
        }

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
        self.mmap.set_edge_property(id, key, value.clone())?;

        // Emit reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::EdgePropertyChanged {
                id,
                key: key.to_string(),
                old_value,
                new_value: value,
            });
        }

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
        #[allow(clippy::type_complexity)]
        let edges_to_remove: Vec<(EdgeId, VertexId, VertexId, String, HashMap<String, Value>)> = node
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
                    (edge_id, e.src, e.dst, edge_label, e.properties.clone())
                })
            })
            .collect();

        // Remove incident edges from COW state
        for (edge_id, _, _, _, _) in &edges_to_remove {
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
        for (edge_id, _edge_src, _edge_dst, edge_label, edge_props) in &edges_to_remove {
            self.index_edge_remove(*edge_id, edge_label, edge_props);
        }

        // Write to disk
        self.mmap.remove_vertex(id)?;

        // Emit reactive events
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            for (edge_id, edge_src, edge_dst, edge_label, _) in edges_to_remove {
                self.event_bus.emit(crate::storage::events::GraphEvent::EdgeRemoved {
                    id: edge_id,
                    src: edge_src,
                    dst: edge_dst,
                    label: edge_label,
                });
            }
            self.event_bus.emit(crate::storage::events::GraphEvent::VertexRemoved {
                id,
                label,
            });
        }

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

        // Get label, endpoints, and properties for index removal / reactive events
        let label = state
            .interner
            .read()
            .resolve(edge.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let (src, dst) = (edge.src, edge.dst);
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

        // Emit reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        if self.event_bus.subscriber_count() > 0 {
            self.event_bus.emit(crate::storage::events::GraphEvent::EdgeRemoved {
                id, src, dst, label,
            });
        }

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
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            pending_events: Vec::new(),
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

                // Emit batch event AFTER successful commit
                #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
                if !ctx.pending_events.is_empty() && self.event_bus.subscriber_count() > 0 {
                    self.event_bus.emit(crate::storage::events::GraphEvent::Batch(ctx.pending_events));
                }

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
    // GQL API
    // =========================================================================

    /// Execute a GQL statement (both reads and mutations).
    ///
    /// This method parses and executes any GQL statement:
    /// - **Read queries** (MATCH...RETURN): Executed against a snapshot
    /// - **Mutations** (CREATE, SET, DELETE, MERGE): Executed against the graph
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    ///
    /// let graph = CowMmapGraph::open("test.db").unwrap();
    ///
    /// // Mutations
    /// graph.gql("CREATE (:Person {name: 'Alice'})").unwrap();
    /// graph.gql("MATCH (n:Person) SET n.age = 30").unwrap();
    ///
    /// // Reads
    /// let results = graph.gql("MATCH (n:Person) RETURN n.name").unwrap();
    /// ```
    #[cfg(feature = "gql")]
    pub fn gql(&self, query: &str) -> Result<Vec<Value>, GqlError> {
        let stmt = gql::parse_statement(query)?;

        if stmt.is_read_only() {
            // Execute reads against a snapshot
            let snapshot = self.snapshot();
            gql::compile_statement(&stmt, &snapshot).map_err(GqlError::Compile)
        } else {
            // Execute mutations against the graph
            let mut wrapper = CowMmapGraphMutWrapper { graph: self };
            let schema = self.schema();
            gql::execute_mutation_with_schema(&stmt, &mut wrapper, schema.as_ref())
                .map_err(|e| GqlError::Mutation(e.to_string()))
        }
    }

    /// Execute a GQL query with parameters.
    ///
    /// This is a convenience method for executing parameterized queries.
    /// Parameters can be referenced in the query using `$paramName` syntax.
    #[cfg(feature = "gql")]
    pub fn gql_with_params(
        &self,
        query: &str,
        params: &gql::Parameters,
    ) -> Result<Vec<Value>, GqlError> {
        let stmt = gql::parse_statement(query)?;

        if stmt.is_read_only() {
            // Execute reads against a snapshot
            let snapshot = self.snapshot();
            gql::compile_statement_with_params(&stmt, &snapshot, params).map_err(GqlError::Compile)
        } else {
            // Mutations with parameters not yet supported
            Err(GqlError::Mutation(
                "Parameterized mutations are not yet supported".into(),
            ))
        }
    }

    // =========================================================================
    // Gremlin Query API
    // =========================================================================

    /// Execute a Gremlin query string.
    ///
    /// This is a convenience method that takes a snapshot, parses the query,
    /// compiles it, and executes it in one call. For mutation queries, use
    /// [`mutate()`](Self::mutate) instead.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use interstellar::gremlin::ExecutionResult;
    ///
    /// let graph = CowMmapGraph::open("my_graph.db").unwrap();
    ///
    /// // Execute a read query
    /// let result = graph.query("g.V().hasLabel('person').values('name').toList()").unwrap();
    ///
    /// if let ExecutionResult::List(names) = result {
    ///     for name in names {
    ///         println!("{:?}", name);
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GremlinError`](crate::gremlin::GremlinError) if the query fails to parse or compile.
    #[cfg(feature = "gremlin")]
    pub fn query(
        &self,
        query: &str,
    ) -> Result<crate::gremlin::ExecutionResult, crate::gremlin::GremlinError> {
        self.snapshot().query(query)
    }

    /// Execute a Gremlin query string with mutation support.
    ///
    /// Unlike [`query()`](Self::query), this method actually executes mutations
    /// (`addV`, `addE`, `property`, `drop`) against the graph and persists them to disk.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use interstellar::gremlin::ExecutionResult;
    ///
    /// let graph = CowMmapGraph::open("my_graph.db").unwrap();
    ///
    /// // Create a vertex with mutations
    /// let result = graph.mutate("g.addV('person').property('name', 'Alice')").unwrap();
    ///
    /// // Verify the vertex was created
    /// assert_eq!(graph.vertex_count(), 1);
    ///
    /// // Create an edge
    /// graph.mutate("g.addE('knows').from(0).to(1)").unwrap();
    ///
    /// // Read queries also work
    /// let result = graph.mutate("g.V().hasLabel('person').values('name').toList()").unwrap();
    /// if let ExecutionResult::List(names) = result {
    ///     println!("Found {} people", names.len());
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GremlinError`](crate::gremlin::GremlinError) if the query fails to parse, compile, or execute.
    #[cfg(feature = "gremlin")]
    pub fn mutate(
        &self,
        query: &str,
    ) -> Result<crate::gremlin::ExecutionResult, crate::gremlin::GremlinError> {
        use crate::gremlin::{compile, parse, ExecutionResult};
        use crate::traversal::mutation::PendingMutation;

        // Parse the query
        let ast = parse(query)?;

        // Compile using a snapshot (read-only compilation)
        let snapshot = self.snapshot();
        let g = snapshot.gremlin();
        let compiled = compile(&ast, &g)?;

        // Get terminal step before consuming traversal
        let terminal = compiled.terminal().cloned();

        // Execute the traversal to get raw results (may include pending mutations)
        let raw_values = compiled.traversal.to_list();

        // Process results, executing any pending mutations
        let mut final_results = Vec::with_capacity(raw_values.len());

        for value in raw_values {
            if let Some(mutation) = PendingMutation::from_value(&value) {
                // Check if ID extraction was requested
                let extract_id = value
                    .as_map()
                    .map(|m| m.contains_key("__extract_id"))
                    .unwrap_or(false);

                // Execute the mutation
                let result = match mutation {
                    PendingMutation::AddVertex { label, properties } => {
                        match self.add_vertex(&label, properties) {
                            Ok(id) => Some(Value::Vertex(id)),
                            Err(_) => None,
                        }
                    }
                    PendingMutation::AddEdge {
                        label,
                        from,
                        to,
                        properties,
                    } => match self.add_edge(from, to, &label, properties) {
                        Ok(id) => Some(Value::Edge(id)),
                        Err(_) => None,
                    },
                    PendingMutation::SetVertexProperty { id, key, value } => {
                        if self.set_vertex_property(id, &key, value).is_ok() {
                            Some(Value::Vertex(id))
                        } else {
                            None
                        }
                    }
                    PendingMutation::SetEdgeProperty { id, key, value } => {
                        if self.set_edge_property(id, &key, value).is_ok() {
                            Some(Value::Edge(id))
                        } else {
                            None
                        }
                    }
                    PendingMutation::DropVertex { id } => {
                        let _ = self.remove_vertex(id);
                        None
                    }
                    PendingMutation::DropEdge { id } => {
                        let _ = self.remove_edge(id);
                        None
                    }
                };

                if let Some(result) = result {
                    if extract_id {
                        let id_value = match result {
                            Value::Vertex(vid) => Value::Int(vid.0 as i64),
                            Value::Edge(eid) => Value::Int(eid.0 as i64),
                            other => other,
                        };
                        final_results.push(id_value);
                    } else {
                        final_results.push(result);
                    }
                }
            } else {
                // Not a mutation, pass through
                final_results.push(value);
            }
        }

        // Return based on terminal step
        Ok(match terminal {
            None | Some(crate::gremlin::TerminalStep::ToList { .. }) => {
                ExecutionResult::List(final_results)
            }
            Some(crate::gremlin::TerminalStep::Next { count: None, .. }) => {
                ExecutionResult::Single(final_results.into_iter().next())
            }
            Some(crate::gremlin::TerminalStep::Next { count: Some(n), .. }) => {
                ExecutionResult::List(final_results.into_iter().take(n as usize).collect())
            }
            Some(crate::gremlin::TerminalStep::ToSet { .. }) => {
                ExecutionResult::Set(final_results.into_iter().collect())
            }
            Some(crate::gremlin::TerminalStep::Iterate { .. }) => ExecutionResult::Unit,
            Some(crate::gremlin::TerminalStep::HasNext { .. }) => {
                ExecutionResult::Bool(!final_results.is_empty())
            }
            Some(crate::gremlin::TerminalStep::Explain { .. }) => {
                ExecutionResult::Explain("explain not supported in mmap backend".to_string())
            }
        })
    }

    /// Execute a multi-statement Gremlin script with variable assignment support.
    ///
    /// This is a convenience wrapper around [`execute_script_with_context`] that
    /// starts with an empty variable context.
    ///
    /// This method supports the full Gremlin script syntax including:
    /// - Variable assignment: `alice = g.addV('person').property('name', 'Alice').next()`
    /// - Variable references in V(): `g.V(alice)`
    /// - Variable references in from/to: `g.addE('knows').from(alice).to(bob).next()`
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use interstellar::gremlin::{ExecutionResult, ScriptResult};
    ///
    /// let graph = CowMmapGraph::open("my_graph.db").unwrap();
    ///
    /// // Execute a multi-statement script with variable assignments
    /// let result = graph.execute_script(r#"
    ///     alice = g.addV('person').property('name', 'Alice').next()
    ///     bob = g.addV('person').property('name', 'Bob').next()
    ///     g.addE('knows').from(alice).to(bob).next()
    ///     g.V(alice).out('knows').values('name').toList()
    /// "#).unwrap();
    ///
    /// // The result is from the last statement
    /// if let ExecutionResult::List(names) = result.result {
    ///     println!("Found: {:?}", names);
    /// }
    ///
    /// // Variables are available in result.variables
    /// assert!(result.variables.contains("alice"));
    /// assert!(result.variables.contains("bob"));
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GremlinError`](crate::gremlin::GremlinError) if:
    /// - The script fails to parse
    /// - A traversal fails to compile
    /// - An assignment doesn't return a single value
    /// - A variable reference cannot be resolved
    #[cfg(feature = "gremlin")]
    pub fn execute_script(
        &self,
        script: &str,
    ) -> Result<crate::gremlin::ScriptResult, crate::gremlin::GremlinError> {
        self.execute_script_with_context(script, crate::gremlin::VariableContext::new())
    }

    /// Execute a multi-statement Gremlin script with an existing variable context.
    ///
    /// This enables REPL-style workflows where variables persist across calls:
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use interstellar::gremlin::{ExecutionResult, ScriptResult, VariableContext};
    ///
    /// let graph = CowMmapGraph::open("my_graph.db").unwrap();
    /// let mut ctx = VariableContext::new();
    ///
    /// // First REPL command
    /// let result = graph.execute_script_with_context(
    ///     "alice = g.addV('person').property('name', 'Alice').next()",
    ///     ctx
    /// ).unwrap();
    /// ctx = result.variables;  // alice is now bound
    ///
    /// // Second REPL command (can reference alice from previous)
    /// let result = graph.execute_script_with_context(
    ///     "g.V(alice).values('name').toList()",
    ///     ctx
    /// ).unwrap();
    ///
    /// if let ExecutionResult::List(names) = result.result {
    ///     println!("Found: {:?}", names);
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GremlinError`](crate::gremlin::GremlinError) if:
    /// - The script fails to parse
    /// - A traversal fails to compile
    /// - An assignment doesn't return a single value
    /// - A variable reference cannot be resolved
    #[cfg(feature = "gremlin")]
    pub fn execute_script_with_context(
        &self,
        script: &str,
        context: crate::gremlin::VariableContext,
    ) -> Result<crate::gremlin::ScriptResult, crate::gremlin::GremlinError> {
        use crate::gremlin::{parse_script, ExecutionResult, ScriptResult, Statement};

        // Parse the script
        let ast = parse_script(script)?;

        // Execute using our own mutation-aware execution
        let mut ctx = context;
        let mut last_result = ExecutionResult::Unit;

        for statement in &ast.statements {
            match statement {
                Statement::Assignment {
                    name, traversal, ..
                } => {
                    // Compile and execute the traversal
                    let snapshot = self.snapshot();
                    let g = snapshot.gremlin();
                    let compiled = crate::gremlin::compile_with_vars(traversal, &g, &ctx)?;
                    let terminal = compiled.terminal().cloned();
                    let raw_values = compiled.traversal.to_list();

                    // Process mutations and get results
                    let final_results = self.process_script_mutations(raw_values);

                    // Apply terminal semantics
                    let result = match terminal {
                        Some(crate::gremlin::TerminalStep::Next { count: None, .. }) => {
                            ExecutionResult::Single(final_results.into_iter().next())
                        }
                        _ => ExecutionResult::List(final_results),
                    };

                    // Bind the result to the variable
                    match result {
                        ExecutionResult::Single(Some(value)) => {
                            ctx.bind(name.clone(), value);
                        }
                        ExecutionResult::List(values) if values.len() == 1 => {
                            ctx.bind(name.clone(), values.into_iter().next().unwrap());
                        }
                        _ => {
                            return Err(crate::gremlin::GremlinError::Compile(
                                crate::gremlin::CompileError::InvalidArguments {
                                    step: "assignment".to_string(),
                                    message: format!(
                                        "assignment to '{}' requires single value from .next()",
                                        name
                                    ),
                                },
                            ));
                        }
                    }
                    last_result = ExecutionResult::Unit;
                }
                Statement::Traversal { traversal, .. } => {
                    // Compile and execute the traversal
                    let snapshot = self.snapshot();
                    let g = snapshot.gremlin();
                    let compiled = crate::gremlin::compile_with_vars(traversal, &g, &ctx)?;
                    let terminal = compiled.terminal().cloned();
                    let raw_values = compiled.traversal.to_list();

                    // Process mutations and get results
                    let final_results = self.process_script_mutations(raw_values);

                    // Return based on terminal step
                    last_result = match terminal {
                        None | Some(crate::gremlin::TerminalStep::ToList { .. }) => {
                            ExecutionResult::List(final_results)
                        }
                        Some(crate::gremlin::TerminalStep::Next { count: None, .. }) => {
                            ExecutionResult::Single(final_results.into_iter().next())
                        }
                        Some(crate::gremlin::TerminalStep::Next { count: Some(n), .. }) => {
                            ExecutionResult::List(
                                final_results.into_iter().take(n as usize).collect(),
                            )
                        }
                        Some(crate::gremlin::TerminalStep::ToSet { .. }) => {
                            ExecutionResult::Set(final_results.into_iter().collect())
                        }
                        Some(crate::gremlin::TerminalStep::Iterate { .. }) => ExecutionResult::Unit,
                        Some(crate::gremlin::TerminalStep::HasNext { .. }) => {
                            ExecutionResult::Bool(!final_results.is_empty())
                        }
                        Some(crate::gremlin::TerminalStep::Explain { .. }) => {
                            ExecutionResult::Explain(
                                "explain not supported in mmap backend".to_string(),
                            )
                        }
                    };
                }
            }
        }

        Ok(ScriptResult {
            result: last_result,
            variables: ctx,
        })
    }

    /// Process raw traversal values, executing any pending mutations.
    #[cfg(feature = "gremlin")]
    fn process_script_mutations(&self, raw_values: Vec<Value>) -> Vec<Value> {
        use crate::traversal::mutation::PendingMutation;

        let mut final_results = Vec::with_capacity(raw_values.len());

        for value in raw_values {
            if let Some(mutation) = PendingMutation::from_value(&value) {
                let extract_id = value
                    .as_map()
                    .map(|m| m.contains_key("__extract_id"))
                    .unwrap_or(false);

                let result = match mutation {
                    PendingMutation::AddVertex { label, properties } => {
                        match self.add_vertex(&label, properties) {
                            Ok(id) => Some(Value::Vertex(id)),
                            Err(_) => None,
                        }
                    }
                    PendingMutation::AddEdge {
                        label,
                        from,
                        to,
                        properties,
                    } => match self.add_edge(from, to, &label, properties) {
                        Ok(id) => Some(Value::Edge(id)),
                        Err(_) => None,
                    },
                    PendingMutation::SetVertexProperty { id, key, value } => {
                        if self.set_vertex_property(id, &key, value).is_ok() {
                            Some(Value::Vertex(id))
                        } else {
                            None
                        }
                    }
                    PendingMutation::SetEdgeProperty { id, key, value } => {
                        if self.set_edge_property(id, &key, value).is_ok() {
                            Some(Value::Edge(id))
                        } else {
                            None
                        }
                    }
                    PendingMutation::DropVertex { id } => {
                        let _ = self.remove_vertex(id);
                        None
                    }
                    PendingMutation::DropEdge { id } => {
                        let _ = self.remove_edge(id);
                        None
                    }
                };

                if let Some(result) = result {
                    if extract_id {
                        let id_value = match result {
                            Value::Vertex(vid) => Value::Int(vid.0 as i64),
                            Value::Edge(eid) => Value::Int(eid.0 as i64),
                            other => other,
                        };
                        final_results.push(id_value);
                    } else {
                        final_results.push(result);
                    }
                }
            } else {
                final_results.push(value);
            }
        }

        final_results
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

    // =========================================================================
    // Query Storage Methods
    // =========================================================================

    /// Save a named query to persistent storage.
    ///
    /// Queries can be Gremlin or GQL, with optional parameters using `$param` syntax.
    /// Parameters are automatically extracted from the query text.
    ///
    /// # Arguments
    ///
    /// * `name` - Unique query name (letters, digits, underscores, hyphens)
    /// * `query_type` - [`QueryType::Gremlin`] or [`QueryType::Gql`]
    /// * `description` - Human-readable description
    /// * `query_text` - The query text (may contain `$param` placeholders)
    ///
    /// # Returns
    ///
    /// The assigned query ID on success.
    ///
    /// # Errors
    ///
    /// - [`QueryError::AlreadyExists`] - Name already in use
    /// - [`QueryError::InvalidName`] - Name contains invalid characters
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use interstellar::query::QueryType;
    ///
    /// let graph = CowMmapGraph::open("my_graph.db").unwrap();
    ///
    /// let query_id = graph.save_query(
    ///     "find_person",
    ///     QueryType::Gremlin,
    ///     "Find a person by name",
    ///     "g.V().hasLabel('person').has('name', $name)",
    /// ).unwrap();
    /// ```
    pub fn save_query(
        &self,
        name: &str,
        query_type: crate::query::QueryType,
        description: &str,
        query_text: &str,
    ) -> Result<u32, crate::error::QueryError> {
        self.mmap
            .save_query(name, query_type, description, query_text)
    }

    /// Get a saved query by name.
    ///
    /// Returns `None` if no query exists with the given name.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    ///
    /// let graph = CowMmapGraph::open("my_graph.db").unwrap();
    ///
    /// if let Some(query) = graph.get_query("find_person") {
    ///     println!("Query: {}", query.query);
    /// }
    /// ```
    pub fn get_query(&self, name: &str) -> Option<crate::query::SavedQuery> {
        self.mmap.get_query(name)
    }

    /// Get a saved query by ID.
    ///
    /// Returns `None` if no query exists with the given ID.
    pub fn get_query_by_id(&self, id: u32) -> Option<crate::query::SavedQuery> {
        self.mmap.get_query_by_id(id)
    }

    /// List all saved queries.
    ///
    /// Returns queries in no particular order.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    ///
    /// let graph = CowMmapGraph::open("my_graph.db").unwrap();
    ///
    /// for query in graph.list_queries() {
    ///     println!("{}: {}", query.name, query.description);
    /// }
    /// ```
    pub fn list_queries(&self) -> Vec<crate::query::SavedQuery> {
        self.mmap.list_queries()
    }

    /// Delete a saved query by name.
    ///
    /// This performs a soft delete - the space is not immediately reclaimed.
    ///
    /// # Errors
    ///
    /// - [`QueryError::NotFound`] - Query does not exist
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    ///
    /// let graph = CowMmapGraph::open("my_graph.db").unwrap();
    /// graph.delete_query("old_query").unwrap();
    /// ```
    pub fn delete_query(&self, name: &str) -> Result<(), crate::error::QueryError> {
        self.mmap.delete_query(name)
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
    state: Arc<GraphState>,
    /// Cloned interner - snapshot-local, no shared lock
    interner_snapshot: Arc<StringInterner>,
    /// Subscription manager reference for reactive queries.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    subscription_manager: std::sync::Arc<crate::traversal::reactive::SubscriptionManager>,
    /// Factory that creates fresh snapshots for reactive re-evaluation.
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    snapshot_fn: std::sync::Arc<
        dyn Fn() -> Box<dyn crate::traversal::context::SnapshotLike + Send> + Send + Sync,
    >,
}

impl CowMmapSnapshot {
    /// Get the version at which this snapshot was taken.
    pub fn version(&self) -> u64 {
        self.state.version
    }

    /// Get the string interner for this snapshot.
    pub fn interner(&self) -> &StringInterner {
        &self.interner_snapshot
    }

    /// Create a Gremlin traversal source for this snapshot.
    ///
    /// This provides the full Gremlin-style fluent API for querying the graph.
    /// Since `CowMmapSnapshot` is immutable, only read operations are available.
    pub fn gremlin(&self) -> crate::traversal::GraphTraversalSource<'_> {
        crate::traversal::GraphTraversalSource::from_snapshot(self)
    }

    /// Execute a Gremlin query string.
    ///
    /// This is a convenience method that parses, compiles, and executes a Gremlin
    /// query against this snapshot.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use interstellar::gremlin::ExecutionResult;
    ///
    /// let graph = CowMmapGraph::open("my_graph.db").unwrap();
    /// let snapshot = graph.snapshot();
    ///
    /// // Execute a read query
    /// let result = snapshot.query("g.V().hasLabel('person').values('name').toList()").unwrap();
    ///
    /// if let ExecutionResult::List(names) = result {
    ///     for name in names {
    ///         println!("{:?}", name);
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns a [`GremlinError`](crate::gremlin::GremlinError) if the query fails to parse or compile.
    #[cfg(feature = "gremlin")]
    pub fn query(
        &self,
        query: &str,
    ) -> Result<crate::gremlin::ExecutionResult, crate::gremlin::GremlinError> {
        let ast = crate::gremlin::parse(query)?;
        let g = self.gremlin();
        let compiled = crate::gremlin::compile(&ast, &g)?;
        Ok(compiled.execute())
    }
}

// Implement SnapshotLike for CowMmapSnapshot to enable generic traversal/GQL usage.
impl crate::traversal::SnapshotLike for CowMmapSnapshot {
    fn storage(&self) -> &dyn GraphStorage {
        self
    }

    fn interner(&self) -> &StringInterner {
        &self.interner_snapshot
    }

    fn as_dyn(&self) -> &dyn crate::traversal::SnapshotLike {
        self
    }

    fn arc_storage(&self) -> std::sync::Arc<dyn GraphStorage + Send + Sync> {
        // CowMmapSnapshot is Clone, Send, Sync, and implements GraphStorage
        std::sync::Arc::new(self.clone())
    }

    fn arc_interner(&self) -> std::sync::Arc<StringInterner> {
        std::sync::Arc::clone(&self.interner_snapshot)
    }

    fn arc_streamable(&self) -> std::sync::Arc<dyn StreamableStorage> {
        self.arc_streamable()
    }

    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    fn subscription_manager(&self) -> Option<&crate::traversal::reactive::SubscriptionManager> {
        Some(&self.subscription_manager)
    }

    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    fn reactive_snapshot_fn(
        &self,
    ) -> Option<
        std::sync::Arc<
            dyn Fn() -> Box<dyn crate::traversal::context::SnapshotLike + Send> + Send + Sync,
        >,
    > {
        Some(self.snapshot_fn.clone())
    }
}

impl Clone for CowMmapSnapshot {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            interner_snapshot: Arc::clone(&self.interner_snapshot),
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            subscription_manager: self.subscription_manager.clone(),
            #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
            snapshot_fn: self.snapshot_fn.clone(),
        }
    }
}

// SAFETY: CowMmapSnapshot only contains Arc<GraphState> which is Send + Sync
unsafe impl Send for CowMmapSnapshot {}
unsafe impl Sync for CowMmapSnapshot {}

// StreamableStorage implementation for CowMmapSnapshot.
//
// CowMmapSnapshot uses the same GraphState as GraphSnapshot, so we can use
// the same streaming implementation. The im::HashMap clones in O(1) via
// structural sharing, enabling true lazy iteration.
impl StreamableStorage for CowMmapSnapshot {
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Clone the im::HashMap - O(1) due to structural sharing
        let vertices = self.state.vertices.clone();
        Box::new(vertices.into_iter().map(|(id, _)| id))
    }

    fn stream_all_edges(&self) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        // Clone the im::HashMap - O(1) due to structural sharing
        let edges = self.state.edges.clone();
        Box::new(edges.into_iter().map(|(id, _)| id))
    }

    fn stream_vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Look up label ID and get the RoaringBitmap
        let label_id = self.interner_snapshot.lookup(label);
        if let Some(lid) = label_id {
            if let Some(bitmap) = self.state.vertex_labels.get(&lid) {
                // Clone the RoaringBitmap to get owned iteration
                let bitmap_owned: RoaringBitmap = (**bitmap).clone();
                return Box::new(bitmap_owned.into_iter().map(|id| VertexId(id as u64)));
            }
        }
        Box::new(std::iter::empty())
    }

    fn stream_edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        // Look up label ID and get the RoaringBitmap
        let label_id = self.interner_snapshot.lookup(label);
        if let Some(lid) = label_id {
            if let Some(bitmap) = self.state.edge_labels.get(&lid) {
                // Clone the RoaringBitmap to get owned iteration
                let bitmap_owned: RoaringBitmap = (**bitmap).clone();
                return Box::new(bitmap_owned.into_iter().map(|id| EdgeId(id as u64)));
            }
        }
        Box::new(std::iter::empty())
    }

    fn stream_out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        if let Some(node) = self.state.vertices.get(&vertex) {
            // Clone the adjacency list - O(degree)
            let out_edges = node.out_edges.clone();
            Box::new(out_edges.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn stream_in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        if let Some(node) = self.state.vertices.get(&vertex) {
            // Clone the adjacency list - O(degree)
            let in_edges = node.in_edges.clone();
            Box::new(in_edges.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn stream_out_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let state = Arc::clone(&self.state);
        let label_ids_owned: Vec<u32> = label_ids.to_vec();

        if let Some(node) = state.vertices.get(&vertex) {
            let out_edges = node.out_edges.clone();
            let state_for_iter = Arc::clone(&state);

            Box::new(out_edges.into_iter().filter_map(move |edge_id| {
                state_for_iter.edges.get(&edge_id).and_then(|edge| {
                    if label_ids_owned.is_empty() || label_ids_owned.contains(&edge.label_id) {
                        Some(edge.dst)
                    } else {
                        None
                    }
                })
            }))
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn stream_in_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let state = Arc::clone(&self.state);
        let label_ids_owned: Vec<u32> = label_ids.to_vec();

        if let Some(node) = state.vertices.get(&vertex) {
            let in_edges = node.in_edges.clone();
            let state_for_iter = Arc::clone(&state);

            Box::new(in_edges.into_iter().filter_map(move |edge_id| {
                state_for_iter.edges.get(&edge_id).and_then(|edge| {
                    if label_ids_owned.is_empty() || label_ids_owned.contains(&edge.label_id) {
                        Some(edge.src)
                    } else {
                        None
                    }
                })
            }))
        } else {
            Box::new(std::iter::empty())
        }
    }
}

impl CowMmapSnapshot {
    /// Returns an Arc<dyn StreamableStorage> for use with StreamingExecutor.
    ///
    /// This enables the traversal engine to hold an owned reference to the
    /// storage that can be used to create streaming iterators. The clone is
    /// cheap since `CowMmapSnapshot` is internally Arc-based.
    #[inline]
    pub fn arc_streamable(&self) -> Arc<dyn StreamableStorage> {
        Arc::new(self.clone())
    }
}

impl GraphStorage for CowMmapSnapshot {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.state.vertices.get(&id).map(|node| {
            let label = self
                .interner_snapshot
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
                .interner_snapshot
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
        let label_id = self.interner_snapshot.lookup(label);

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
        let label_id = self.interner_snapshot.lookup(label);

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
        // Return a reference to the snapshot-local cloned interner.
        // No locking needed since it's owned by this snapshot.
        &self.interner_snapshot
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
    pending_state: GraphState,
    operations: Vec<BatchOperation>,
    #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
    pub(crate) pending_events: Vec<crate::storage::events::GraphEvent>,
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
            properties: properties.clone(),
        });

        // Record reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        self.pending_events.push(crate::storage::events::GraphEvent::VertexAdded {
            id,
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
            properties: properties.clone(),
        });

        // Record reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        self.pending_events.push(crate::storage::events::GraphEvent::EdgeAdded {
            id,
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

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let old_value = node.properties.get(key).cloned();

        let mut new_node = (**node).clone();
        new_node.properties.insert(key.to_string(), value.clone());
        self.pending_state.vertices = self.pending_state.vertices.update(id, Arc::new(new_node));

        self.pending_state.version += 1;

        self.operations.push(BatchOperation::SetVertexProperty {
            id,
            key: key.to_string(),
            value: value.clone(),
        });

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        self.pending_events.push(crate::storage::events::GraphEvent::VertexPropertyChanged {
            id,
            key: key.to_string(),
            old_value,
            new_value: value,
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

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let old_value = edge.properties.get(key).cloned();

        let mut new_edge = (**edge).clone();
        new_edge.properties.insert(key.to_string(), value.clone());
        self.pending_state.edges = self.pending_state.edges.update(id, Arc::new(new_edge));

        self.pending_state.version += 1;

        self.operations.push(BatchOperation::SetEdgeProperty {
            id,
            key: key.to_string(),
            value: value.clone(),
        });

        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        self.pending_events.push(crate::storage::events::GraphEvent::EdgePropertyChanged {
            id,
            key: key.to_string(),
            old_value,
            new_value: value,
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

        // Capture info for reactive events before mutation
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let vertex_label = self
            .pending_state
            .interner
            .read()
            .resolve(node.label_id)
            .map(|s| s.to_string())
            .unwrap_or_default();

        // Collect edges to remove
        let edges_to_remove: Vec<EdgeId> = node
            .out_edges
            .iter()
            .chain(node.in_edges.iter())
            .copied()
            .collect();

        // Capture edge info for reactive events before removal
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        let edge_infos: Vec<(EdgeId, VertexId, VertexId, String)> = edges_to_remove
            .iter()
            .filter_map(|&edge_id| {
                self.pending_state.edges.get(&edge_id).map(|e| {
                    let elabel = self
                        .pending_state
                        .interner
                        .read()
                        .resolve(e.label_id)
                        .map(|s| s.to_string())
                        .unwrap_or_default();
                    (edge_id, e.src, e.dst, elabel)
                })
            })
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

        // Record reactive events
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        {
            for (eid, src, dst, elabel) in edge_infos {
                self.pending_events.push(crate::storage::events::GraphEvent::EdgeRemoved {
                    id: eid, src, dst, label: elabel,
                });
            }
            self.pending_events.push(crate::storage::events::GraphEvent::VertexRemoved {
                id,
                label: vertex_label,
            });
        }

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

        // Record reactive event
        #[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
        {
            let elabel = self
                .pending_state
                .interner
                .read()
                .resolve(edge.label_id)
                .map(|s| s.to_string())
                .unwrap_or_default();
            self.pending_events.push(crate::storage::events::GraphEvent::EdgeRemoved {
                id,
                src: edge.src,
                dst: edge.dst,
                label: elabel,
            });
        }

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
// CowMmapTraversalSource - Unified Traversal API with Auto-Mutation
// =============================================================================

/// Entry point for traversals on a [`CowMmapGraph`] with automatic mutation execution.
///
/// Unlike the read-only [`GraphTraversalSource`](crate::traversal::GraphTraversalSource),
/// this traversal source has access to the underlying `CowMmapGraph` and will automatically
/// execute any mutations when terminal steps are called.
///
/// # Unified API
///
/// Both reads and writes use the same API - no separate "mutation mode":
///
/// ```no_run
/// use interstellar::storage::cow_mmap::CowMmapGraph;
/// use std::collections::HashMap;
/// use std::sync::Arc;
///
/// let graph = Arc::new(CowMmapGraph::open("test.db").unwrap());
/// let g = graph.gremlin(Arc::clone(&graph));
///
/// // Mutations are executed automatically - returns PersistentVertex
/// let alice = g.add_v("Person").property("name", "Alice").next();
/// let bob = g.add_v("Person").property("name", "Bob").next();
///
/// // Reads work normally - returns PersistentVertex objects
/// let vertices = g.v().to_list();
/// assert_eq!(vertices.len(), 2);
/// ```
pub struct CowMmapTraversalSource<'g> {
    graph: &'g CowMmapGraph,
    graph_arc: Arc<CowMmapGraph>,
}

impl<'g> CowMmapTraversalSource<'g> {
    /// Create a new traversal source with an Arc<CowMmapGraph>.
    ///
    /// This is the primary constructor for `CowMmapTraversalSource`.
    pub fn new_with_arc(graph: &'g CowMmapGraph, graph_arc: Arc<CowMmapGraph>) -> Self {
        Self { graph, graph_arc }
    }

    /// Start traversal from all vertices.
    ///
    /// Returns `PersistentVertex` objects from terminal methods like `next()` and `to_list()`.
    pub fn v(&self) -> CowMmapBoundTraversal<'g, (), Value, VertexMarker> {
        CowMmapBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::AllVertices),
        )
    }

    /// Start traversal from specific vertex IDs.
    ///
    /// Returns `PersistentVertex` objects from terminal methods.
    pub fn v_ids<I>(&self, ids: I) -> CowMmapBoundTraversal<'g, (), Value, VertexMarker>
    where
        I: IntoIterator<Item = VertexId>,
    {
        CowMmapBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::Vertices(ids.into_iter().collect())),
        )
    }

    /// Start traversal from a single vertex ID.
    ///
    /// Returns `PersistentVertex` objects from terminal methods.
    pub fn v_id(&self, id: VertexId) -> CowMmapBoundTraversal<'g, (), Value, VertexMarker> {
        self.v_ids([id])
    }

    /// Start traversal from all edges.
    ///
    /// Returns `PersistentEdge` objects from terminal methods like `next()` and `to_list()`.
    pub fn e(&self) -> CowMmapBoundTraversal<'g, (), Value, EdgeMarker> {
        CowMmapBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::AllEdges),
        )
    }

    /// Start traversal from specific edge IDs.
    ///
    /// Returns `PersistentEdge` objects from terminal methods.
    pub fn e_ids<I>(&self, ids: I) -> CowMmapBoundTraversal<'g, (), Value, EdgeMarker>
    where
        I: IntoIterator<Item = EdgeId>,
    {
        CowMmapBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::Edges(ids.into_iter().collect())),
        )
    }

    /// Start a traversal that creates a new vertex.
    ///
    /// The vertex is created when a terminal step is called.
    /// Returns `PersistentVertex` from terminal methods.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use std::sync::Arc;
    ///
    /// let graph = Arc::new(CowMmapGraph::open("test.db").unwrap());
    /// let g = graph.gremlin(Arc::clone(&graph));
    ///
    /// let vertex = g.add_v("Person").property("name", "Alice").next();
    /// assert!(vertex.is_some());
    /// assert_eq!(graph.vertex_count(), 1);
    /// ```
    pub fn add_v(
        &self,
        label: impl Into<String>,
    ) -> CowMmapBoundTraversal<'g, (), Value, VertexMarker> {
        use crate::traversal::mutation::AddVStep;

        let mut traversal = Traversal::<(), Value>::with_source(TraversalSource::Inject(vec![]));
        traversal = traversal.add_step(AddVStep::new(label));
        CowMmapBoundTraversal::new_typed(self.graph, Arc::clone(&self.graph_arc), traversal)
    }

    /// Start a traversal that creates a new edge.
    ///
    /// Must specify `from` and `to` vertices before calling a terminal step.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    ///
    /// let graph = Arc::new(CowMmapGraph::open("test.db").unwrap());
    /// let alice = graph.add_vertex("Person", HashMap::new()).unwrap();
    /// let bob = graph.add_vertex("Person", HashMap::new()).unwrap();
    ///
    /// let g = graph.gremlin(Arc::clone(&graph));
    /// let edge = g.add_e("KNOWS").from_id(alice).to_id(bob).next();
    /// assert!(edge.is_some());
    /// assert_eq!(graph.edge_count(), 1);
    /// ```
    pub fn add_e(&self, label: impl Into<String>) -> CowMmapAddEdgeBuilder<'g> {
        CowMmapAddEdgeBuilder::new(self.graph, Arc::clone(&self.graph_arc), label.into())
    }

    /// Inject arbitrary values into the traversal stream.
    pub fn inject<I>(&self, values: I) -> CowMmapBoundTraversal<'g, (), Value, Scalar>
    where
        I: IntoIterator<Item = Value>,
    {
        CowMmapBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::Inject(values.into_iter().collect())),
        )
    }

    /// Start untyped traversal from all vertices.
    ///
    /// Unlike `v()`, this returns `Value` from terminal methods instead of `PersistentVertex`.
    /// Useful for dynamic scenarios.
    pub fn v_untyped(&self) -> CowMmapBoundTraversal<'g, (), Value, Scalar> {
        CowMmapBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::AllVertices),
        )
    }

    /// Start untyped traversal from specific vertex IDs.
    ///
    /// Returns `Value` from terminal methods.
    pub fn v_ids_untyped<I>(&self, ids: I) -> CowMmapBoundTraversal<'g, (), Value, Scalar>
    where
        I: IntoIterator<Item = VertexId>,
    {
        CowMmapBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::Vertices(ids.into_iter().collect())),
        )
    }

    /// Start untyped traversal from all edges.
    ///
    /// Unlike `e()`, this returns `Value` from terminal methods instead of `PersistentEdge`.
    pub fn e_untyped(&self) -> CowMmapBoundTraversal<'g, (), Value, Scalar> {
        CowMmapBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::AllEdges),
        )
    }

    /// Start untyped traversal from specific edge IDs.
    ///
    /// Returns `Value` from terminal methods.
    pub fn e_ids_untyped<I>(&self, ids: I) -> CowMmapBoundTraversal<'g, (), Value, Scalar>
    where
        I: IntoIterator<Item = EdgeId>,
    {
        CowMmapBoundTraversal::new_typed(
            self.graph,
            Arc::clone(&self.graph_arc),
            Traversal::with_source(TraversalSource::Edges(ids.into_iter().collect())),
        )
    }
}

// =============================================================================
// CowMmapBoundTraversal - Traversal with Auto-Mutation Execution
// =============================================================================

/// A traversal bound to a [`CowMmapGraph`] with automatic mutation execution.
///
/// When terminal steps (`to_list()`, `next()`, `iterate()`, etc.) are called,
/// any pending mutations in the traversal results are automatically executed
/// against the graph.
///
/// The `Marker` type determines what terminal methods return:
/// - `VertexMarker` → `next()` returns `Option<PersistentVertex>`
/// - `EdgeMarker` → `next()` returns `Option<PersistentEdge>`
/// - `Scalar` → `next()` returns `Option<Value>`
pub struct CowMmapBoundTraversal<'g, In, Out, Marker: OutputMarker = Scalar> {
    graph: &'g CowMmapGraph,
    graph_arc: Arc<CowMmapGraph>,
    traversal: Traversal<In, Out>,
    track_paths: bool,
    _marker: PhantomData<Marker>,
}

impl<'g, In, Out, Marker: OutputMarker> CowMmapBoundTraversal<'g, In, Out, Marker> {
    /// Create a new typed bound traversal.
    pub(crate) fn new_typed(
        graph: &'g CowMmapGraph,
        graph_arc: Arc<CowMmapGraph>,
        traversal: Traversal<In, Out>,
    ) -> Self {
        Self {
            graph,
            graph_arc,
            traversal,
            track_paths: false,
            _marker: PhantomData,
        }
    }

    /// Enable automatic path tracking for this traversal.
    pub fn with_path(mut self) -> Self {
        self.track_paths = true;
        self
    }

    /// Add a step to the traversal, preserving the marker type.
    pub fn add_step_same<NewOut>(
        self,
        step: impl crate::traversal::step::Step,
    ) -> CowMmapBoundTraversal<'g, In, NewOut, Marker> {
        CowMmapBoundTraversal {
            graph: self.graph,
            graph_arc: self.graph_arc,
            traversal: self.traversal.add_step(step),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Add a step to the traversal, preserving the marker type.
    ///
    /// Alias for `add_step_same` for backward compatibility.
    pub fn add_step<NewOut>(
        self,
        step: impl crate::traversal::step::Step,
    ) -> CowMmapBoundTraversal<'g, In, NewOut, Marker> {
        self.add_step_same(step)
    }

    /// Add a step to the traversal with a new marker type.
    pub fn add_step_with_marker<NewOut, NewMarker: OutputMarker>(
        self,
        step: impl crate::traversal::step::Step,
    ) -> CowMmapBoundTraversal<'g, In, NewOut, NewMarker> {
        CowMmapBoundTraversal {
            graph: self.graph,
            graph_arc: self.graph_arc,
            traversal: self.traversal.add_step(step),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Append an anonymous traversal's steps.
    pub fn append<Mid>(
        self,
        anon: Traversal<Out, Mid>,
    ) -> CowMmapBoundTraversal<'g, In, Mid, Marker> {
        CowMmapBoundTraversal {
            graph: self.graph,
            graph_arc: self.graph_arc,
            traversal: self.traversal.append(anon),
            track_paths: self.track_paths,
            _marker: PhantomData,
        }
    }

    /// Execute the traversal and process any pending mutations.
    ///
    /// Returns an iterator over the results with mutations applied.
    fn execute_with_mutations(&self) -> Vec<Value> {
        use crate::traversal::step::StartStep;
        use crate::traversal::traverser::TraversalSource;

        // Clone the traversal so we can decompose it
        let traversal_clone = self.traversal.clone();

        // Decompose traversal into source and steps
        let (source, steps) = traversal_clone.into_steps();

        // Check if this is a mutation-only traversal (source is Inject([]))
        let is_mutation_only =
            matches!(&source, Some(TraversalSource::Inject(values)) if values.is_empty());

        let results: Vec<Traverser> = if is_mutation_only {
            // For mutation-only traversals, we still need an ExecutionContext.
            // Use a snapshot from the actual graph (O(1) clone due to structural sharing).
            let snapshot = self.graph.snapshot();
            let interner = snapshot.interner();
            let storage_ref: &dyn GraphStorage = &snapshot;

            let ctx = if self.track_paths {
                ExecutionContext::with_path_tracking(storage_ref, interner)
            } else {
                ExecutionContext::new(storage_ref, interner)
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
                current = step
                    .apply_dyn(&ctx, Box::new(current.into_iter()))
                    .collect();
            }

            current
        } else {
            // For read traversals that need graph access, use the full path
            // Create a snapshot for read operations - CowMmapSnapshot implements GraphStorage
            let snapshot = self.graph.snapshot();
            let interner = snapshot.interner();
            let storage_ref: &dyn GraphStorage = &snapshot;

            // Create execution context
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
                current = step
                    .apply_dyn(&ctx, Box::new(current.into_iter()))
                    .collect();
            }

            current
        };

        // Process results, executing any pending mutations
        let mut wrapper = CowMmapGraphMutWrapper { graph: self.graph };
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
        wrapper: &mut CowMmapGraphMutWrapper<'_>,
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

// Terminal methods on CowMmapBoundTraversal
impl<'g, In, Out, Marker: OutputMarker> CowMmapBoundTraversal<'g, In, Out, Marker> {
    /// Execute and consume the traversal, discarding results.
    ///
    /// Any pending mutations are automatically executed.
    pub fn iterate(self) {
        let _ = self.execute_with_mutations();
    }

    /// Check if the traversal produces any results.
    pub fn has_next(&self) -> bool {
        !self.execute_with_mutations().is_empty()
    }
}

// =============================================================================
// Terminal Methods for VertexMarker
// =============================================================================

/// Terminal methods when traversal produces vertices.
impl<'g, In, Out> CowMmapBoundTraversal<'g, In, Out, VertexMarker> {
    /// Execute and return the first vertex, if any.
    pub fn next(self) -> Option<PersistentVertex> {
        let graph_arc = Arc::clone(&self.graph_arc);
        self.execute_with_mutations()
            .into_iter()
            .find_map(|v| match v {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph_arc))),
                _ => None,
            })
    }

    /// Execute and collect all vertices into a list.
    pub fn to_list(self) -> Vec<PersistentVertex> {
        let graph_arc = Arc::clone(&self.graph_arc);
        self.execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph_arc))),
                _ => None,
            })
            .collect()
    }

    /// Execute and return exactly one vertex.
    ///
    /// # Errors
    ///
    /// Returns `TraversalError::NotOne` if zero or more than one vertex is found.
    pub fn one(self) -> Result<PersistentVertex, crate::error::TraversalError> {
        let graph_arc = Arc::clone(&self.graph_arc);
        let ids: Vec<_> = self
            .execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Vertex(id) => Some(id),
                _ => None,
            })
            .take(2)
            .collect();
        match ids.len() {
            1 => Ok(GraphVertex::new(ids[0], graph_arc)),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }

    /// Execute and count the number of vertices.
    pub fn count(self) -> u64 {
        self.execute_with_mutations()
            .into_iter()
            .filter(|v| matches!(v, Value::Vertex(_)))
            .count() as u64
    }

    /// Execute and collect unique vertices into a set.
    #[allow(clippy::mutable_key_type)]
    pub fn to_set(self) -> std::collections::HashSet<PersistentVertex> {
        self.to_list().into_iter().collect()
    }
}

// =============================================================================
// Terminal Methods for EdgeMarker
// =============================================================================

/// Terminal methods when traversal produces edges.
impl<'g, In, Out> CowMmapBoundTraversal<'g, In, Out, EdgeMarker> {
    /// Execute and return the first edge, if any.
    pub fn next(self) -> Option<PersistentEdge> {
        let graph_arc = Arc::clone(&self.graph_arc);
        self.execute_with_mutations()
            .into_iter()
            .find_map(|v| match v {
                Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph_arc))),
                _ => None,
            })
    }

    /// Execute and collect all edges into a list.
    pub fn to_list(self) -> Vec<PersistentEdge> {
        let graph_arc = Arc::clone(&self.graph_arc);
        self.execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph_arc))),
                _ => None,
            })
            .collect()
    }

    /// Execute and collect all edges into a list of raw Values.
    ///
    /// This is an internal helper used by `CowMmapBoundAddEdgeBuilder`.
    pub(crate) fn into_list_values(self) -> Vec<Value> {
        self.execute_with_mutations()
    }

    /// Execute and return exactly one edge.
    ///
    /// # Errors
    ///
    /// Returns `TraversalError::NotOne` if zero or more than one edge is found.
    pub fn one(self) -> Result<PersistentEdge, crate::error::TraversalError> {
        let graph_arc = Arc::clone(&self.graph_arc);
        let ids: Vec<_> = self
            .execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Edge(id) => Some(id),
                _ => None,
            })
            .take(2)
            .collect();
        match ids.len() {
            1 => Ok(GraphEdge::new(ids[0], graph_arc)),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }

    /// Execute and count the number of edges.
    pub fn count(self) -> u64 {
        self.execute_with_mutations()
            .into_iter()
            .filter(|v| matches!(v, Value::Edge(_)))
            .count() as u64
    }

    /// Execute and collect unique edges into a set.
    #[allow(clippy::mutable_key_type)]
    pub fn to_set(self) -> std::collections::HashSet<PersistentEdge> {
        self.to_list().into_iter().collect()
    }
}

// =============================================================================
// Terminal Methods for Scalar
// =============================================================================

/// Terminal methods when traversal produces scalar values.
impl<'g, In, Out> CowMmapBoundTraversal<'g, In, Out, Scalar> {
    /// Execute and return the first value, if any.
    ///
    /// Any pending mutations are automatically executed.
    pub fn next(self) -> Option<Value> {
        self.execute_with_mutations().into_iter().next()
    }

    /// Execute and collect all values into a list.
    ///
    /// Any pending mutations in the results are automatically executed.
    pub fn to_list(self) -> Vec<Value> {
        self.execute_with_mutations()
    }

    /// Execute and return exactly one value.
    ///
    /// # Errors
    ///
    /// Returns `TraversalError::NotOne` if zero or more than one value is found.
    pub fn one(self) -> Result<Value, crate::error::TraversalError> {
        let results: Vec<_> = self.execute_with_mutations().into_iter().take(2).collect();
        match results.len() {
            1 => Ok(results.into_iter().next().unwrap()),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }

    /// Execute and count the number of results.
    pub fn count(self) -> u64 {
        self.execute_with_mutations().len() as u64
    }

    /// Execute and collect unique values into a set.
    pub fn to_set(self) -> std::collections::HashSet<Value> {
        self.execute_with_mutations().into_iter().collect()
    }

    // =========================================================================
    // Rich Type Terminal Methods (for backwards compatibility)
    // =========================================================================

    /// Execute and collect all vertices into a list of `PersistentVertex`.
    ///
    /// Returns vertices as rich [`PersistentVertex`] objects with access to
    /// the graph for property lookups and traversal.
    ///
    /// Non-vertex values are silently filtered.
    ///
    /// # Arguments
    ///
    /// * `graph` - An `Arc<CowMmapGraph>` to associate with the returned vertices
    ///
    /// # Note
    ///
    /// For typed traversals (from `v()`, `v_id()`, etc.), prefer using `to_list()`
    /// directly which returns `Vec<PersistentVertex>` without needing an Arc parameter.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::cow_mmap::CowMmapGraph;
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    ///
    /// let graph = Arc::new(CowMmapGraph::open("test.db").unwrap());
    /// graph.add_vertex("person", HashMap::from([
    ///     ("name".to_string(), "Alice".into()),
    /// ])).unwrap();
    ///
    /// // Preferred: use typed traversal
    /// let g = graph.gremlin(Arc::clone(&graph));
    /// let vertices = g.v().to_list();  // Returns Vec<PersistentVertex> directly
    /// assert_eq!(vertices.len(), 1);
    /// ```
    pub fn to_vertex_list(self, graph: Arc<CowMmapGraph>) -> Vec<PersistentVertex> {
        self.execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph))),
                _ => None,
            })
            .collect()
    }

    /// Execute and return the first vertex as a `PersistentVertex`.
    ///
    /// Returns `None` if the traversal produces no vertices.
    /// Non-vertex values are silently filtered.
    ///
    /// # Note
    ///
    /// For typed traversals (from `v()`, `v_id()`, etc.), prefer using `next()`
    /// directly which returns `Option<PersistentVertex>` without needing an Arc parameter.
    #[deprecated(note = "Use typed traversal with `next()` instead")]
    pub fn next_vertex(self, graph: Arc<CowMmapGraph>) -> Option<PersistentVertex> {
        self.execute_with_mutations()
            .into_iter()
            .find_map(|v| match v {
                Value::Vertex(id) => Some(GraphVertex::new(id, Arc::clone(&graph))),
                _ => None,
            })
    }

    /// Execute and return exactly one vertex as a `PersistentVertex`.
    ///
    /// # Errors
    ///
    /// Returns `TraversalError::NotOne` if zero or more than one vertex is found.
    ///
    /// # Note
    ///
    /// For typed traversals (from `v()`, `v_id()`, etc.), prefer using `one()`
    /// directly which returns `Result<PersistentVertex, _>` without needing an Arc parameter.
    #[deprecated(note = "Use typed traversal with `one()` instead")]
    pub fn one_vertex(
        self,
        graph: Arc<CowMmapGraph>,
    ) -> Result<PersistentVertex, crate::error::TraversalError> {
        let ids: Vec<_> = self
            .execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Vertex(id) => Some(id),
                _ => None,
            })
            .take(2)
            .collect();
        match ids.len() {
            1 => Ok(GraphVertex::new(ids[0], graph)),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }

    /// Execute and collect all edges into a list of `PersistentEdge`.
    ///
    /// Returns edges as rich [`PersistentEdge`] objects with access to
    /// the graph for property lookups and endpoint access.
    ///
    /// Non-edge values are silently filtered.
    ///
    /// # Note
    ///
    /// For typed traversals (from `e()`, `e_ids()`, etc.), prefer using `to_list()`
    /// directly which returns `Vec<PersistentEdge>` without needing an Arc parameter.
    #[deprecated(note = "Use typed traversal with `to_list()` instead")]
    pub fn to_edge_list(self, graph: Arc<CowMmapGraph>) -> Vec<PersistentEdge> {
        self.execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph))),
                _ => None,
            })
            .collect()
    }

    /// Execute and return the first edge as a `PersistentEdge`.
    ///
    /// Returns `None` if the traversal produces no edges.
    /// Non-edge values are silently filtered.
    ///
    /// # Note
    ///
    /// For typed traversals (from `e()`, `e_ids()`, etc.), prefer using `next()`
    /// directly which returns `Option<PersistentEdge>` without needing an Arc parameter.
    #[deprecated(note = "Use typed traversal with `next()` instead")]
    pub fn next_edge(self, graph: Arc<CowMmapGraph>) -> Option<PersistentEdge> {
        self.execute_with_mutations()
            .into_iter()
            .find_map(|v| match v {
                Value::Edge(id) => Some(GraphEdge::new(id, Arc::clone(&graph))),
                _ => None,
            })
    }

    /// Execute and return exactly one edge as a `PersistentEdge`.
    ///
    /// # Errors
    ///
    /// Returns `TraversalError::NotOne` if zero or more than one edge is found.
    ///
    /// # Arguments
    ///
    /// * `graph` - An `Arc<CowMmapGraph>` to associate with the returned edge
    pub fn one_edge(
        self,
        graph: Arc<CowMmapGraph>,
    ) -> Result<PersistentEdge, crate::error::TraversalError> {
        let ids: Vec<_> = self
            .execute_with_mutations()
            .into_iter()
            .filter_map(|v| match v {
                Value::Edge(id) => Some(id),
                _ => None,
            })
            .take(2)
            .collect();
        match ids.len() {
            1 => Ok(GraphEdge::new(ids[0], graph)),
            n => Err(crate::error::TraversalError::NotOne(n)),
        }
    }
}

// Step methods for CowMmapBoundTraversal<Value>
impl<'g, In> CowMmapBoundTraversal<'g, In, Value> {
    /// Filter elements by label.
    pub fn has_label(self, label: impl Into<String>) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(HasLabelStep::single(label))
    }

    /// Filter to elements that have a specific property key.
    pub fn has(self, key: impl Into<String>) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(HasStep::new(key.into()))
    }

    /// Filter to elements where a property equals a value.
    pub fn has_value(
        self,
        key: impl Into<String>,
        value: Value,
    ) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(HasValueStep::new(key.into(), value))
    }

    /// Traverse to outgoing adjacent vertices.
    pub fn out(self) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(OutStep::new())
    }

    /// Traverse to outgoing adjacent vertices via edges with label.
    pub fn out_label(self, label: impl Into<String>) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(OutStep::with_labels(vec![label.into()]))
    }

    /// Traverse to incoming adjacent vertices.
    pub fn in_(self) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(InStep::new())
    }

    /// Traverse to incoming adjacent vertices via edges with label.
    pub fn in_label(self, label: impl Into<String>) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(InStep::with_labels(vec![label.into()]))
    }

    /// Traverse to outgoing edges.
    pub fn out_e(self) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(OutEStep::new())
    }

    /// Traverse to incoming edges.
    pub fn in_e(self) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(InEStep::new())
    }

    /// Traverse to the target vertex of an edge.
    pub fn in_v(self) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(InVStep)
    }

    /// Traverse to the source vertex of an edge.
    pub fn out_v(self) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(OutVStep)
    }

    /// Get property values by key.
    pub fn values(self, key: impl Into<String>) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(ValuesStep::new(key.into()))
    }

    /// Add a property to the current element (for mutation traversals).
    pub fn property(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(PropertyStep::new(key.into(), value.into()))
    }

    /// Drop (delete) the current element.
    pub fn drop(self) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(DropStep)
    }

    /// Add an edge from the current vertex.
    pub fn add_e(self, label: impl Into<String>) -> CowMmapBoundAddEdgeBuilder<'g, In> {
        CowMmapBoundAddEdgeBuilder::new(
            self.graph,
            self.graph_arc,
            self.traversal,
            label.into(),
            self.track_paths,
        )
    }

    /// Limit results to first n.
    pub fn limit(self, n: usize) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(LimitStep::new(n))
    }

    /// Skip first n results.
    pub fn skip(self, n: usize) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(SkipStep::new(n))
    }

    /// Get element IDs.
    pub fn id(self) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(IdStep)
    }

    /// Get element labels.
    pub fn label(self) -> CowMmapBoundTraversal<'g, In, Value> {
        self.add_step(LabelStep)
    }
}

// =============================================================================
// Step methods for VertexMarker traversals
// =============================================================================

impl<'g, In> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
    /// Filter vertices by label (preserves VertexMarker).
    pub fn has_label(
        self,
        label: impl Into<String>,
    ) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(HasLabelStep::single(label))
    }

    /// Filter to vertices that have a specific property key (preserves VertexMarker).
    pub fn has(self, key: impl Into<String>) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(HasStep::new(key.into()))
    }

    /// Filter to vertices where a property equals a value (preserves VertexMarker).
    pub fn has_value(
        self,
        key: impl Into<String>,
        value: Value,
    ) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(HasValueStep::new(key.into(), value))
    }

    /// Traverse to outgoing adjacent vertices (preserves VertexMarker).
    pub fn out(self) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(OutStep::new())
    }

    /// Traverse to outgoing adjacent vertices via edges with label (preserves VertexMarker).
    pub fn out_label(
        self,
        label: impl Into<String>,
    ) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(OutStep::with_labels(vec![label.into()]))
    }

    /// Traverse to incoming adjacent vertices (preserves VertexMarker).
    pub fn in_(self) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(InStep::new())
    }

    /// Traverse to incoming adjacent vertices via edges with label (preserves VertexMarker).
    pub fn in_label(
        self,
        label: impl Into<String>,
    ) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(InStep::with_labels(vec![label.into()]))
    }

    /// Traverse to adjacent vertices in both directions (preserves VertexMarker).
    pub fn both(self) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(crate::traversal::BothStep::new())
    }

    /// Traverse to outgoing edges (transforms to EdgeMarker).
    pub fn out_e(self) -> CowMmapBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_with_marker(OutEStep::new())
    }

    /// Traverse to incoming edges (transforms to EdgeMarker).
    pub fn in_e(self) -> CowMmapBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_with_marker(InEStep::new())
    }

    /// Add a property to the current vertex (for mutation traversals, preserves VertexMarker).
    pub fn property(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(PropertyStep::new(key.into(), value.into()))
    }

    /// Drop (delete) the current vertex (preserves VertexMarker).
    pub fn drop(self) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(DropStep)
    }

    /// Get property values by key (transforms to Scalar).
    pub fn values(self, key: impl Into<String>) -> CowMmapBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(ValuesStep::new(key.into()))
    }

    /// Get element IDs (transforms to Scalar).
    pub fn id(self) -> CowMmapBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(IdStep)
    }

    /// Get element labels (transforms to Scalar).
    pub fn label(self) -> CowMmapBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(LabelStep)
    }

    /// Limit results to first n (preserves VertexMarker).
    pub fn limit(self, n: usize) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(LimitStep::new(n))
    }

    /// Skip first n results (preserves VertexMarker).
    pub fn skip(self, n: usize) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_same(SkipStep::new(n))
    }

    /// Add an edge from the current vertex.
    pub fn add_e(self, label: impl Into<String>) -> CowMmapBoundAddEdgeBuilder<'g, In> {
        CowMmapBoundAddEdgeBuilder::new(
            self.graph,
            self.graph_arc,
            self.traversal,
            label.into(),
            self.track_paths,
        )
    }
}

// =============================================================================
// Step methods for EdgeMarker traversals
// =============================================================================

impl<'g, In> CowMmapBoundTraversal<'g, In, Value, EdgeMarker> {
    /// Filter edges by label (preserves EdgeMarker).
    pub fn has_label(
        self,
        label: impl Into<String>,
    ) -> CowMmapBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(HasLabelStep::single(label))
    }

    /// Filter to edges that have a specific property key (preserves EdgeMarker).
    pub fn has(self, key: impl Into<String>) -> CowMmapBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(HasStep::new(key.into()))
    }

    /// Filter to edges where a property equals a value (preserves EdgeMarker).
    pub fn has_value(
        self,
        key: impl Into<String>,
        value: Value,
    ) -> CowMmapBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(HasValueStep::new(key.into(), value))
    }

    /// Traverse to the target vertex of an edge (transforms to VertexMarker).
    pub fn in_v(self) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_with_marker(InVStep)
    }

    /// Traverse to the source vertex of an edge (transforms to VertexMarker).
    pub fn out_v(self) -> CowMmapBoundTraversal<'g, In, Value, VertexMarker> {
        self.add_step_with_marker(OutVStep)
    }

    /// Add a property to the current edge (for mutation traversals, preserves EdgeMarker).
    pub fn property(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> CowMmapBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(PropertyStep::new(key.into(), value.into()))
    }

    /// Drop (delete) the current edge (preserves EdgeMarker).
    pub fn drop(self) -> CowMmapBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(DropStep)
    }

    /// Get property values by key (transforms to Scalar).
    pub fn values(self, key: impl Into<String>) -> CowMmapBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(ValuesStep::new(key.into()))
    }

    /// Get element IDs (transforms to Scalar).
    pub fn id(self) -> CowMmapBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(IdStep)
    }

    /// Get element labels (transforms to Scalar).
    pub fn label(self) -> CowMmapBoundTraversal<'g, In, Value, Scalar> {
        self.add_step_with_marker(LabelStep)
    }

    /// Limit results to first n (preserves EdgeMarker).
    pub fn limit(self, n: usize) -> CowMmapBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(LimitStep::new(n))
    }

    /// Skip first n results (preserves EdgeMarker).
    pub fn skip(self, n: usize) -> CowMmapBoundTraversal<'g, In, Value, EdgeMarker> {
        self.add_step_same(SkipStep::new(n))
    }
}

// =============================================================================
// CowMmapAddEdgeBuilder - Builder for add_e() from traversal source
// =============================================================================

/// Builder for creating edges from the traversal source.
pub struct CowMmapAddEdgeBuilder<'g> {
    graph: &'g CowMmapGraph,
    graph_arc: Arc<CowMmapGraph>,
    label: String,
    from: Option<VertexId>,
    to: Option<VertexId>,
    properties: HashMap<String, Value>,
}

impl<'g> CowMmapAddEdgeBuilder<'g> {
    fn new(graph: &'g CowMmapGraph, graph_arc: Arc<CowMmapGraph>, label: String) -> Self {
        Self {
            graph,
            graph_arc,
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
    pub fn next(self) -> Option<PersistentEdge> {
        let from = self.from?;
        let to = self.to?;

        match self.graph.add_edge(from, to, &self.label, self.properties) {
            Ok(id) => Some(GraphEdge::new(id, self.graph_arc)),
            Err(_) => None,
        }
    }

    /// Execute, discarding the result.
    pub fn iterate(self) {
        let _ = self.next();
    }

    /// Execute and return results as a list.
    pub fn to_list(self) -> Vec<PersistentEdge> {
        self.next().into_iter().collect()
    }
}

// =============================================================================
// CowMmapBoundAddEdgeBuilder - Builder for add_e() from traversal
// =============================================================================

/// Builder for creating edges from an existing traversal.
pub struct CowMmapBoundAddEdgeBuilder<'g, In> {
    graph: &'g CowMmapGraph,
    graph_arc: Arc<CowMmapGraph>,
    traversal: Traversal<In, Value>,
    label: String,
    to: Option<VertexId>,
    properties: HashMap<String, Value>,
    track_paths: bool,
}

impl<'g, In> CowMmapBoundAddEdgeBuilder<'g, In> {
    fn new(
        graph: &'g CowMmapGraph,
        graph_arc: Arc<CowMmapGraph>,
        traversal: Traversal<In, Value>,
        label: String,
        track_paths: bool,
    ) -> Self {
        Self {
            graph,
            graph_arc,
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
        let bound: CowMmapBoundTraversal<'_, In, Value, EdgeMarker> = CowMmapBoundTraversal {
            graph: self.graph,
            graph_arc: self.graph_arc,
            traversal,
            track_paths: self.track_paths,
            _marker: PhantomData,
        };

        bound.into_list_values()
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
// GraphAccess Implementation for Arc<CowMmapGraph>
// =============================================================================

impl crate::graph_access::GraphAccess for Arc<CowMmapGraph> {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.snapshot().get_vertex(id)
    }

    fn get_edge(&self, id: EdgeId) -> Option<Edge> {
        self.snapshot().get_edge(id)
    }

    fn out_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId> {
        self.snapshot().out_edges(vertex).map(|e| e.id).collect()
    }

    fn in_edge_ids(&self, vertex: VertexId) -> Vec<EdgeId> {
        self.snapshot().in_edges(vertex).map(|e| e.id).collect()
    }

    fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        CowMmapGraph::set_vertex_property(self, id, key, value)
    }

    fn set_edge_property(&self, id: EdgeId, key: &str, value: Value) -> Result<(), StorageError> {
        CowMmapGraph::set_edge_property(self, id, key, value)
    }

    fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError> {
        CowMmapGraph::add_edge(self, src, dst, label, properties)
    }

    fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError> {
        CowMmapGraph::remove_vertex(self, id)
    }

    fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError> {
        CowMmapGraph::remove_edge(self, id)
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

    // =========================================================================
    // Rich Type Terminal Method Tests
    // =========================================================================

    #[test]
    fn test_to_vertex_list_returns_persistent_vertices() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
            )
            .unwrap();
        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Bob".into()))]),
            )
            .unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let vertices = g.v().to_list();

        assert_eq!(vertices.len(), 2);
        // Verify we get PersistentVertex with full functionality
        for v in &vertices {
            assert_eq!(v.label(), Some("person".to_string()));
            assert!(v.property("name").is_some());
        }
    }

    #[test]
    fn test_next_vertex_returns_persistent_vertex() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
            )
            .unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let v = g.v().next().unwrap();

        assert_eq!(v.label(), Some("person".to_string()));
        assert_eq!(v.property("name"), Some(Value::String("Alice".into())));
    }

    #[test]
    fn test_next_vertex_returns_none_when_empty() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        let g = graph.gremlin(Arc::clone(&graph));
        let v = g.v().next();

        assert!(v.is_none());
    }

    #[test]
    fn test_one_vertex_success() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Alice".into()))]),
            )
            .unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let v = g.v().one().unwrap();

        assert_eq!(v.label(), Some("person".to_string()));
    }

    #[test]
    fn test_one_vertex_fails_when_multiple() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        graph.add_vertex("person", HashMap::new()).unwrap();
        graph.add_vertex("person", HashMap::new()).unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let result = g.v().one();

        assert!(result.is_err());
    }

    #[test]
    fn test_one_vertex_fails_when_empty() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        let g = graph.gremlin(Arc::clone(&graph));
        let result = g.v().one();

        assert!(result.is_err());
    }

    #[test]
    fn test_to_edge_list_returns_persistent_edges() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        let alice = graph.add_vertex("person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("person", HashMap::new()).unwrap();
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        graph
            .add_edge(bob, alice, "knows_too", HashMap::new())
            .unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let edges = g.e().to_list();

        assert_eq!(edges.len(), 2);
        // Verify we get PersistentEdge with full functionality
        for e in &edges {
            assert!(e.label().is_some());
            assert!(e.out_v().is_some());
            assert!(e.in_v().is_some());
        }
    }

    #[test]
    fn test_next_edge_returns_persistent_edge() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        let alice = graph.add_vertex("person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("person", HashMap::new()).unwrap();
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let e = g.e().next().unwrap();

        assert_eq!(e.label(), Some("knows".to_string()));
        // Verify edge endpoints work
        let src = e.out_v().unwrap();
        let dst = e.in_v().unwrap();
        assert_eq!(src.id(), alice);
        assert_eq!(dst.id(), bob);
    }

    #[test]
    fn test_next_edge_returns_none_when_empty() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        let g = graph.gremlin(Arc::clone(&graph));
        let e = g.e().next();

        assert!(e.is_none());
    }

    #[test]
    fn test_one_edge_success() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        let alice = graph.add_vertex("person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("person", HashMap::new()).unwrap();
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let e = g.e().one().unwrap();

        assert_eq!(e.label(), Some("knows".to_string()));
    }

    #[test]
    fn test_one_edge_fails_when_multiple() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        let alice = graph.add_vertex("person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("person", HashMap::new()).unwrap();
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        graph.add_edge(bob, alice, "knows", HashMap::new()).unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let result = g.e().one();

        assert!(result.is_err());
    }

    #[test]
    fn test_persistent_vertex_traversal() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        let alice = graph.add_vertex("person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("person", HashMap::new()).unwrap();
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let v = g.v_id(alice).next().unwrap();

        // Use PersistentVertex to traverse
        let neighbors: Vec<_> = v.out("knows").to_list();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].id(), bob);
    }

    #[test]
    fn test_persistent_vertex_mutation() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        let alice = graph
            .add_vertex("person", HashMap::from([("name".into(), "Alice".into())]))
            .unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let v = g.v_id(alice).next().unwrap();

        // Mutate through PersistentVertex
        v.property_set("age", 30i64).unwrap();

        // Verify mutation persisted
        let updated = graph.get_vertex(alice).unwrap();
        assert_eq!(updated.properties.get("age"), Some(&Value::Int(30)));
    }

    #[test]
    fn test_persistent_edge_mutation() {
        let (_dir, path) = temp_db_path();
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        let alice = graph.add_vertex("person", HashMap::new()).unwrap();
        let bob = graph.add_vertex("person", HashMap::new()).unwrap();
        let edge_id = graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

        let g = graph.gremlin(Arc::clone(&graph));
        let e = g.e().next().unwrap();

        // Mutate through PersistentEdge
        e.property_set("since", 2020i64).unwrap();

        // Verify mutation persisted
        let updated = graph.get_edge(edge_id).unwrap();
        assert_eq!(updated.properties.get("since"), Some(&Value::Int(2020)));
    }

    // =========================================================================
    // Gremlin Query/Mutate Tests
    // =========================================================================

    #[test]
    #[cfg(feature = "gremlin")]
    fn test_gremlin_query() {
        use crate::gremlin::ExecutionResult;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gremlin_query.db");
        let graph = CowMmapGraph::open(&path).unwrap();

        // Add some test data
        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
            )
            .unwrap();
        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
            )
            .unwrap();

        // Query using Gremlin text parser
        let result = graph
            .query("g.V().hasLabel('person').values('name').toList()")
            .unwrap();

        if let ExecutionResult::List(names) = result {
            assert_eq!(names.len(), 2);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    #[cfg(feature = "gremlin")]
    fn test_gremlin_mutate_add_vertex() {
        use crate::gremlin::ExecutionResult;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gremlin_mutate.db");
        let graph = CowMmapGraph::open(&path).unwrap();

        assert_eq!(graph.vertex_count(), 0);

        // Add a vertex using Gremlin mutation
        let result = graph
            .mutate("g.addV('person').property('name', 'Alice')")
            .unwrap();

        // Verify mutation returned a vertex
        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 1);
            assert!(
                values[0].is_vertex(),
                "Expected Vertex, got: {:?}",
                values[0]
            );
        } else {
            panic!("Expected List result");
        }

        // Verify the vertex was actually created
        assert_eq!(graph.vertex_count(), 1, "Vertex was not created");

        // Verify we can query the vertex
        let result = graph
            .query("g.V().hasLabel('person').values('name').toList()")
            .unwrap();
        if let ExecutionResult::List(names) = result {
            assert_eq!(names.len(), 1);
            assert_eq!(names[0], Value::String("Alice".to_string()));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    #[cfg(feature = "gremlin")]
    fn test_gremlin_mutate_add_edge() {
        use crate::gremlin::ExecutionResult;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gremlin_edge.db");
        let graph = CowMmapGraph::open(&path).unwrap();

        // Create two vertices
        let alice = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
            )
            .unwrap();
        let bob = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
            )
            .unwrap();

        assert_eq!(graph.edge_count(), 0);

        // Add edge using Gremlin mutation
        let result = graph
            .mutate(&format!("g.addE('knows').from({}).to({})", alice.0, bob.0))
            .unwrap();

        // Verify mutation returned an edge
        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 1);
            assert!(values[0].is_edge(), "Expected Edge, got: {:?}", values[0]);
        } else {
            panic!("Expected List result");
        }

        // Verify the edge was actually created
        assert_eq!(graph.edge_count(), 1, "Edge was not created");
    }

    #[test]
    #[cfg(feature = "gremlin")]
    fn test_gremlin_mutate_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gremlin_drop.db");
        let graph = CowMmapGraph::open(&path).unwrap();

        // Create vertices
        let alice = graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
            )
            .unwrap();
        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
            )
            .unwrap();

        assert_eq!(graph.vertex_count(), 2);

        // Drop using Gremlin mutation
        graph.mutate(&format!("g.V({}).drop()", alice.0)).unwrap();

        // Verify the vertex was deleted
        assert_eq!(graph.vertex_count(), 1, "Vertex was not dropped");
    }

    #[test]
    #[cfg(feature = "gremlin")]
    fn test_gremlin_snapshot_query() {
        use crate::gremlin::ExecutionResult;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot_query.db");
        let graph = CowMmapGraph::open(&path).unwrap();

        // Add some test data
        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
            )
            .unwrap();

        // Take a snapshot
        let snapshot = graph.snapshot();

        // Query the snapshot
        let result = snapshot
            .query("g.V().hasLabel('person').values('name').toList()")
            .unwrap();

        if let ExecutionResult::List(names) = result {
            assert_eq!(names.len(), 1);
            assert_eq!(names[0], Value::String("Alice".to_string()));
        } else {
            panic!("Expected List result");
        }

        // Add another vertex to the graph
        graph
            .add_vertex(
                "person",
                HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
            )
            .unwrap();

        // Snapshot should still show only 1 vertex (isolation)
        let result = snapshot.query("g.V().hasLabel('person').toList()").unwrap();
        if let ExecutionResult::List(vertices) = result {
            assert_eq!(vertices.len(), 1, "Snapshot should be isolated");
        } else {
            panic!("Expected List result");
        }

        // Graph query should show 2 vertices
        let result = graph.query("g.V().hasLabel('person').toList()").unwrap();
        if let ExecutionResult::List(vertices) = result {
            assert_eq!(vertices.len(), 2, "Graph should have both vertices");
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_gremlin_fluent_api_mutations() {
        // Test that gremlin() fluent API supports mutations (not just query/mutate text API)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fluent_mutations.db");
        let graph = Arc::new(CowMmapGraph::open(&path).unwrap());

        // Create traversal source
        let g = graph.gremlin(Arc::clone(&graph));

        // Test add_v mutation via fluent API
        let alice = g.add_v("Person").property("name", "Alice").next();
        assert!(alice.is_some(), "Should return a PersistentVertex");
        assert_eq!(graph.vertex_count(), 1, "Vertex should be created");

        // Create another vertex
        let bob = g.add_v("Person").property("name", "Bob").next();
        assert!(bob.is_some());
        assert_eq!(graph.vertex_count(), 2);

        // Get IDs for edge creation
        let alice_id = alice.unwrap().id();
        let bob_id = bob.unwrap().id();

        // Test add_e mutation via fluent API
        let edge = g.add_e("KNOWS").from_id(alice_id).to_id(bob_id).next();
        assert!(edge.is_some(), "Should return a PersistentEdge");
        assert_eq!(graph.edge_count(), 1, "Edge should be created");

        // Test read traversal via fluent API
        let people = g.v().has_label("Person").to_list();
        assert_eq!(people.len(), 2, "Should find 2 people");

        // Test traversal + navigation
        let friends = g.v_id(alice_id).out_label("KNOWS").to_list();
        assert_eq!(friends.len(), 1, "Alice should know 1 person");
        assert_eq!(friends[0].id(), bob_id);

        // Test property mutation on existing vertex via fluent API
        g.v_id(alice_id).property("age", 30i64).iterate();
        let alice_vertex = graph.get_vertex(alice_id).unwrap();
        assert_eq!(alice_vertex.properties.get("age"), Some(&Value::Int(30)));

        // Test drop mutation via fluent API
        g.v_id(bob_id).drop().iterate();
        assert_eq!(graph.vertex_count(), 1, "Bob should be deleted");
        assert_eq!(graph.edge_count(), 0, "Edge should be cascade deleted");
    }
}
