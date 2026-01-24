# Spec 32: CowMmapGraph - Copy-on-Write for Persistent Storage

## 1. Overview

This specification extends Copy-on-Write (COW) snapshot support to the memory-mapped persistent storage backend (`MmapGraph`). The goal is to provide the same lock-free snapshot semantics as `CowGraph` while maintaining persistence and durability.

### 1.1 Goals

1. **Unified COW semantics**: Both in-memory and persistent graphs support O(1) snapshots
2. **Lock-free reads**: Snapshots don't hold locks, can outlive the source
3. **Persistence**: All mutations are durable (written to disk via WAL)
4. **Thread safety**: `Send + Sync` for both graph and snapshots
5. **Backward compatibility**: Existing `MmapGraph` API preserved

### 1.2 Non-Goals

1. Full MVCC with concurrent writers (single-writer model preserved)
2. Historical queries (snapshots represent point-in-time, not queryable history)
3. Snapshot persistence (snapshots are in-memory views of persistent data)

---

## 2. Architecture

### 2.1 Current State

```
┌─────────────────────────────────────────────────────────────────┐
│                      GraphStorage trait                          │
└─────────────────────────────────────────────────────────────────┘
          │                    │                    │
          ▼                    ▼                    ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│  Graph  │   │   MmapGraph     │   │    CowGraph     │
│  (HashMap)      │   │  (mmap files)   │   │  (im crate)     │
│  No snapshots   │   │  WAL + batch    │   │  O(1) snapshots │
│  No persistence │   │  No snapshots   │   │  No persistence │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

### 2.2 Target State

```
┌─────────────────────────────────────────────────────────────────┐
│                      GraphStorage trait                          │
└─────────────────────────────────────────────────────────────────┘
          │                    │                    │
          ▼                    ▼                    ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│ Graph   │   │  CowMmapGraph   │   │    CowGraph     │
│ (deprecated)    │   │  (mmap + COW)   │   │  (im crate)     │
│                 │   │  O(1) snapshots │   │  O(1) snapshots │
│                 │   │  Persistent     │   │  No persistence │
└─────────────────┘   └─────────────────────────────────────────┘
                              │
                              ▼
                      ┌───────────────┐
                      │ CowMmapSnapshot│
                      │ (owned, lock- │
                      │  free reads)  │
                      └───────────────┘
```

### 2.3 Design Approach

`CowMmapGraph` uses a **hybrid architecture**:

1. **Disk layer**: `MmapGraph` handles persistence, WAL, and crash recovery
2. **COW layer**: `CowGraphState` (from `im` crate) provides snapshot isolation
3. **Sync protocol**: Changes flow disk → COW state on read, COW state → disk on write

```
┌─────────────────────────────────────────────────────────────────┐
│                        CowMmapGraph                              │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────────────────────┐    │
│  │   MmapGraph     │◄──►│   RwLock<CowGraphState>         │    │
│  │  (persistence)  │    │   (in-memory COW layer)         │    │
│  └─────────────────┘    └─────────────────────────────────┘    │
│          │                            │                         │
│          │                            ▼                         │
│          │              ┌─────────────────────────────────┐    │
│          │              │   CowMmapSnapshot               │    │
│          ▼              │   (owned Arc<CowGraphState>)    │    │
│    ┌──────────┐         └─────────────────────────────────┘    │
│    │  .db     │                                                 │
│    │  .wal    │                                                 │
│    └──────────┘                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Flow

### 3.1 Opening a Database

```rust
let graph = CowMmapGraph::open("my_graph.db")?;
```

1. Open `MmapGraph` from disk (handles WAL recovery if needed)
2. Load all vertices and edges into `CowGraphState`
3. Wrap in `RwLock` for thread-safe access

### 3.2 Taking a Snapshot

```rust
let snapshot = graph.snapshot();
```

1. Read-lock the `CowGraphState`
2. Clone the state (O(1) via `im` crate's structural sharing)
3. Return `CowMmapSnapshot` wrapping `Arc<CowGraphState>`
4. Release the lock

### 3.3 Mutations

```rust
graph.add_vertex("person", props);
```

1. Write-lock the `CowGraphState`
2. Apply mutation to COW state (returns new state)
3. Write mutation to `MmapGraph` (WAL + data file)
4. Replace state with new state
5. Release lock

### 3.4 Snapshot Usage

```rust
// Snapshot is independent of graph
let name = snapshot.get_vertex(id)?.properties.get("name");

// Can be used across threads
std::thread::spawn(move || {
    for v in snapshot.all_vertices() { /* ... */ }
});
```

---

## 4. API Design

### 4.1 CowMmapGraph

```rust
/// Persistent graph with Copy-on-Write snapshot support.
///
/// Combines `MmapGraph` persistence with `CowGraph` snapshot semantics.
pub struct CowMmapGraph {
    /// Underlying persistent storage
    mmap: MmapGraph,
    /// COW state for snapshot isolation
    state: RwLock<CowGraphState>,
}

impl CowMmapGraph {
    // === Construction ===
    
    /// Open or create a database file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError>;
    
    /// Open with a schema for validation.
    pub fn open_with_schema<P: AsRef<Path>>(
        path: P, 
        schema: GraphSchema
    ) -> Result<Self, StorageError>;
    
    // === Snapshots ===
    
    /// Create an immutable snapshot (O(1) operation).
    pub fn snapshot(&self) -> CowMmapSnapshot;
    
    /// Get the current version number.
    pub fn version(&self) -> u64;
    
    // === Mutations (via &self due to interior mutability) ===
    
    /// Add a vertex.
    pub fn add_vertex(
        &self, 
        label: &str, 
        properties: HashMap<String, Value>
    ) -> Result<VertexId, StorageError>;
    
    /// Add an edge.
    pub fn add_edge(
        &self,
        src: VertexId,
        dst: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<EdgeId, StorageError>;
    
    /// Set a vertex property.
    pub fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError>;
    
    /// Set an edge property.
    pub fn set_edge_property(
        &self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError>;
    
    /// Remove a vertex and its incident edges.
    pub fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError>;
    
    /// Remove an edge.
    pub fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError>;
    
    // === Batch Operations ===
    
    /// Execute multiple operations atomically.
    pub fn batch<F, T>(&self, f: F) -> Result<T, BatchError>
    where
        F: FnOnce(&mut CowMmapBatchContext) -> Result<T, BatchError>;
    
    // === GQL Mutations ===
    
    /// Execute a GQL mutation (CREATE, SET, DELETE, REMOVE).
    pub fn execute_mutation(&self, gql: &str) -> Result<Vec<Value>, GqlError>;
    
    /// Execute a GQL mutation with parameters.
    pub fn execute_mutation_with_params(
        &self,
        gql: &str,
        params: &Parameters,
    ) -> Result<Vec<Value>, GqlError>;
    
    // === Persistence ===
    
    /// Force a checkpoint (sync all data to disk).
    pub fn checkpoint(&self) -> Result<(), StorageError>;
    
    /// Check if in batch mode.
    pub fn is_batch_mode(&self) -> bool;
    
    // === Read Access (delegates to snapshot) ===
    
    /// Get vertex count.
    pub fn vertex_count(&self) -> u64;
    
    /// Get edge count.
    pub fn edge_count(&self) -> u64;
    
    /// Get a vertex by ID.
    pub fn get_vertex(&self, id: VertexId) -> Option<Vertex>;
    
    /// Get an edge by ID.
    pub fn get_edge(&self, id: EdgeId) -> Option<Edge>;
}
```

### 4.2 CowMmapSnapshot

```rust
/// Immutable, owned snapshot of a persistent graph.
///
/// Snapshots are cheap to create (O(1)) and can be used independently
/// of the source graph. They implement `GraphStorage` for compatibility
/// with the traversal engine.
pub struct CowMmapSnapshot {
    state: Arc<CowGraphState>,
}

impl CowMmapSnapshot {
    /// Get the version at which this snapshot was taken.
    pub fn version(&self) -> u64;
    
    /// Get reference to the string interner.
    pub fn interner(&self) -> &StringInterner;
}

impl GraphStorage for CowMmapSnapshot {
    // All read methods delegate to CowGraphState
    fn get_vertex(&self, id: VertexId) -> Option<Vertex>;
    fn get_edge(&self, id: EdgeId) -> Option<Edge>;
    fn vertex_count(&self) -> u64;
    fn edge_count(&self) -> u64;
    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;
    fn in_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_>;
    fn vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Vertex> + '_>;
    fn edges_with_label(&self, label: &str) -> Box<dyn Iterator<Item = Edge> + '_>;
    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_>;
    fn all_edges(&self) -> Box<dyn Iterator<Item = Edge> + '_>;
    fn interner(&self) -> &StringInterner;
}

impl Clone for CowMmapSnapshot {
    // O(1) clone via Arc
}

unsafe impl Send for CowMmapSnapshot {}
unsafe impl Sync for CowMmapSnapshot {}
```

### 4.3 CowMmapBatchContext

```rust
/// Context for atomic batch operations on CowMmapGraph.
pub struct CowMmapBatchContext<'g> {
    graph: &'g CowMmapGraph,
    // Pending changes to COW state (not yet committed)
    pending_state: CowGraphState,
    // Track operations for potential rollback
    operations: Vec<BatchOperation>,
}

impl<'g> CowMmapBatchContext<'g> {
    pub fn add_vertex(&mut self, label: &str, props: HashMap<String, Value>) -> VertexId;
    pub fn add_edge(&mut self, src: VertexId, dst: VertexId, label: &str, props: HashMap<String, Value>) -> Result<EdgeId, BatchError>;
    pub fn set_vertex_property(&mut self, id: VertexId, key: &str, value: Value) -> Result<(), BatchError>;
    pub fn set_edge_property(&mut self, id: EdgeId, key: &str, value: Value) -> Result<(), BatchError>;
    pub fn remove_vertex(&mut self, id: VertexId) -> Result<(), BatchError>;
    pub fn remove_edge(&mut self, id: EdgeId) -> Result<(), BatchError>;
}
```

---

## 5. Implementation Details

### 5.1 State Synchronization

The key challenge is keeping the COW state synchronized with the disk state.

#### Strategy: Write-Through

Every mutation:
1. Applies to COW state first (for immediate visibility)
2. Writes to disk via MmapGraph (for durability)

```rust
pub fn add_vertex(&self, label: &str, properties: HashMap<String, Value>) -> Result<VertexId, StorageError> {
    let mut state = self.state.write();
    
    // 1. Apply to COW state
    let id = state.add_vertex(label, properties.clone());
    
    // 2. Write to disk (MmapGraph handles WAL)
    let disk_id = self.mmap.add_vertex(label, properties)?;
    
    // IDs must match (both use sequential allocation)
    debug_assert_eq!(id, disk_id);
    
    Ok(id)
}
```

#### ID Allocation Synchronization

Both `CowGraphState` and `MmapGraph` allocate sequential IDs. To keep them synchronized:

1. On open: Initialize COW state's `next_vertex_id` / `next_edge_id` from MmapGraph header
2. On mutation: Both allocate the same ID (verified with debug_assert)

### 5.2 Loading from Disk

When opening a database, we need to populate the COW state from disk:

```rust
fn load_state_from_mmap(mmap: &MmapGraph) -> CowGraphState {
    let mut state = CowGraphState::new();
    
    // Load all vertices
    for vertex in mmap.all_vertices() {
        state.vertices.insert(vertex.id, Arc::new(NodeData {
            id: vertex.id,
            label_id: state.interner.intern(&vertex.label),
            properties: vertex.properties.clone(),
            out_edges: Vec::new(),  // Will be populated from edges
            in_edges: Vec::new(),
        }));
    }
    
    // Load all edges
    for edge in mmap.all_edges() {
        state.edges.insert(edge.id, Arc::new(EdgeData {
            id: edge.id,
            label_id: state.interner.intern(&edge.label),
            src: edge.src,
            dst: edge.dst,
            properties: edge.properties.clone(),
        }));
        
        // Update adjacency lists
        if let Some(src_node) = state.vertices.get_mut(&edge.src) {
            Arc::make_mut(src_node).out_edges.push(edge.id);
        }
        if let Some(dst_node) = state.vertices.get_mut(&edge.dst) {
            Arc::make_mut(dst_node).in_edges.push(edge.id);
        }
    }
    
    // Sync ID counters
    state.next_vertex_id = mmap.get_header().next_node_id;
    state.next_edge_id = mmap.get_header().next_edge_id;
    state.version = 0;  // Fresh load
    
    state
}
```

### 5.3 Batch Mode Integration

`CowMmapGraph` integrates with `MmapGraph`'s batch mode for atomic multi-operation writes:

```rust
pub fn batch<F, T>(&self, f: F) -> Result<T, BatchError>
where
    F: FnOnce(&mut CowMmapBatchContext) -> Result<T, BatchError>,
{
    // 1. Start MmapGraph batch mode
    self.mmap.begin_batch()?;
    
    // 2. Clone current COW state for the batch
    let pending_state = self.state.read().clone();
    
    let mut ctx = CowMmapBatchContext {
        graph: self,
        pending_state,
        operations: Vec::new(),
    };
    
    // 3. Execute user's batch function
    match f(&mut ctx) {
        Ok(result) => {
            // 4. Apply all operations to MmapGraph
            for op in &ctx.operations {
                self.apply_operation_to_mmap(op)?;
            }
            
            // 5. Commit batch (single fsync)
            self.mmap.commit_batch()?;
            
            // 6. Update COW state atomically
            *self.state.write() = ctx.pending_state;
            
            Ok(result)
        }
        Err(e) => {
            // Rollback: abort MmapGraph batch, discard pending state
            self.mmap.abort_batch()?;
            Err(e)
        }
    }
}
```

### 5.4 Memory Considerations

Loading the entire graph into memory doubles memory usage:
- Disk: mmap'd file
- Memory: COW state (`im::HashMap`)

For very large graphs, consider:
1. Lazy loading (load on first access)
2. LRU eviction of cold vertices
3. Hybrid mode (hot data in COW, cold data from mmap)

For Phase 1, we accept the memory overhead in favor of simpler implementation.

---

## 6. Thread Safety

### 6.1 Guarantees

| Type | Send | Sync | Notes |
|------|------|------|-------|
| `CowMmapGraph` | Yes | Yes | Single writer via RwLock |
| `CowMmapSnapshot` | Yes | Yes | Immutable, Arc-wrapped |
| `CowMmapBatchContext` | No | No | Borrows graph, single-threaded |

### 6.2 Concurrency Model

```
Writer Thread           Reader Threads
     │                       │
     ▼                       ▼
┌─────────┐            ┌───────────┐
│ RwLock  │◄───────────│ snapshot()│
│ (write) │            └───────────┘
└────┬────┘                  │
     │                       ▼
     │               ┌───────────────┐
     ▼               │ CowMmapSnapshot│
┌─────────┐          │ (lock-free)   │
│MmapGraph│          └───────────────┘
│ (WAL)   │
└─────────┘
```

- Writers serialize via RwLock
- Readers take a snapshot and release the lock immediately
- Snapshots are fully independent (no locks held)

---

## 7. Error Handling

### 7.1 Error Types

```rust
#[derive(Debug, Error)]
pub enum CowMmapError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    
    #[error("state synchronization failed: disk ID {disk_id:?} != COW ID {cow_id:?}")]
    IdMismatch {
        disk_id: VertexId,
        cow_id: VertexId,
    },
    
    #[error("batch error: {0}")]
    Batch(#[from] BatchError),
}
```

### 7.2 Recovery

On open, if WAL recovery is needed:
1. `MmapGraph::open` performs WAL replay
2. `CowMmapGraph` loads the recovered state into COW layer
3. Both layers are consistent

---

## 8. Performance Characteristics

### 8.1 Time Complexity

| Operation | CowMmapGraph | Notes |
|-----------|--------------|-------|
| `snapshot()` | O(1) | Clone COW state via Arc |
| `add_vertex()` | O(log n) | COW update + disk write |
| `get_vertex()` | O(log n) | From COW state |
| `open()` | O(V + E) | Load all data into COW |
| `batch()` | O(k log n) | k = operations in batch |

### 8.2 Space Complexity

- **Disk**: Same as `MmapGraph`
- **Memory**: O(V + E) for COW state (roughly 2x memory usage)
- **Snapshots**: O(1) per snapshot (structural sharing)

### 8.3 Performance Targets

| Metric | Target |
|--------|--------|
| Snapshot creation | < 100ns |
| Vertex lookup | < 500ns |
| Open (1M vertices) | < 5 seconds |
| Batch commit | Same as MmapGraph |

---

## 9. Migration Guide

### 9.1 From MmapGraph

```rust
// Before
let graph = MmapGraph::open("my_graph.db")?;
graph.add_vertex("person", props)?;  // &mut self

// After
let graph = CowMmapGraph::open("my_graph.db")?;
graph.add_vertex("person", props)?;  // &self (interior mutability)

// New: Snapshots!
let snapshot = graph.snapshot();
std::thread::spawn(move || {
    for v in snapshot.all_vertices() { /* ... */ }
});
```

### 9.2 From CowGraph

```rust
// Before (in-memory only)
let graph = CowGraph::new();
graph.add_vertex("person", props);
let snapshot = graph.snapshot();

// After (persistent)
let graph = CowMmapGraph::open("my_graph.db")?;
graph.add_vertex("person", props)?;  // Now returns Result
let snapshot = graph.snapshot();  // Same API!
```

---

## 10. Testing Strategy

### 10.1 Unit Tests

1. **Basic operations**: Add/get/remove vertices and edges
2. **Snapshot isolation**: Mutations don't affect existing snapshots
3. **Thread safety**: Concurrent readers with writer
4. **Batch operations**: Atomic commit and rollback
5. **Persistence**: Data survives close/reopen
6. **ID synchronization**: COW and disk IDs match

### 10.2 Integration Tests

1. **WAL recovery**: Crash simulation with uncommitted batch
2. **Large graph loading**: 100K+ vertices load time
3. **GQL mutations**: CREATE/SET/DELETE via `execute_mutation()`
4. **Traversal compatibility**: Existing traversal tests pass

### 10.3 Property-Based Tests

```rust
proptest! {
    #[test]
    fn snapshot_isolation(ops: Vec<GraphOperation>) {
        let graph = CowMmapGraph::open(temp_path())?;
        
        // Apply half the operations
        for op in &ops[..ops.len()/2] {
            apply_op(&graph, op);
        }
        
        let snapshot = graph.snapshot();
        let snapshot_count = snapshot.vertex_count();
        
        // Apply remaining operations
        for op in &ops[ops.len()/2..] {
            apply_op(&graph, op);
        }
        
        // Snapshot should be unchanged
        assert_eq!(snapshot.vertex_count(), snapshot_count);
    }
}
```

---

## 11. File Structure

```
src/storage/
├── mod.rs                 # Update exports
├── cow.rs                 # Existing CowGraph (in-memory)
├── cow_mmap.rs            # NEW: CowMmapGraph
├── inmemory.rs            # Mark as deprecated
└── mmap/
    └── mod.rs             # Existing MmapGraph (unchanged)

tests/storage/
├── cow_mmap.rs            # NEW: CowMmapGraph tests
└── ...
```

---

## 12. Implementation Phases

### Phase 1: Core Implementation
1. Create `CowMmapGraph` struct
2. Implement `open()` with state loading
3. Implement `snapshot()` returning `CowMmapSnapshot`
4. Implement basic mutations (add_vertex, add_edge)

### Phase 2: Full Mutation Support
1. Implement set_vertex_property, set_edge_property
2. Implement remove_vertex, remove_edge
3. Implement batch operations

### Phase 3: GQL Integration
1. Implement `execute_mutation()`
2. Wire up GQL mutation compiler

### Phase 4: Testing & Polish
1. Comprehensive test suite
2. Performance benchmarks
3. Documentation

---

## 13. Success Criteria

| Criterion | Target |
|-----------|--------|
| All existing MmapGraph tests pass | 100% |
| Snapshot creation | < 100ns |
| No ID desync between COW and disk | Verified via debug_assert |
| Thread-safe snapshots | Send + Sync |
| Batch atomicity | All-or-nothing |
| Persistence across restart | Data survives |

---

## 14. Future Considerations

### 14.1 Lazy Loading

For very large graphs, load data on demand:
```rust
struct LazyNode {
    loaded: Option<Arc<NodeData>>,
    disk_id: VertexId,
}
```

### 14.2 Snapshot Persistence

Allow saving snapshots to disk:
```rust
impl CowMmapSnapshot {
    pub fn save(&self, path: &Path) -> Result<(), StorageError>;
    pub fn load(path: &Path) -> Result<Self, StorageError>;
}
```

### 14.3 Deprecation of Graph

Once `CowMmapGraph` is stable:
1. Add `#[deprecated]` to `Graph`
2. Update all examples to use `CowGraph` or `CowMmapGraph`
3. Remove in next major version
