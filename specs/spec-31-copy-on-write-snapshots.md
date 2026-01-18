# Interstellar: Copy-on-Write Snapshots

This document specifies the Copy-on-Write (COW) snapshot implementation for Interstellar using persistent data structures, enabling lock-free reads with consistent snapshots.

---

## 1. Overview and Motivation

### 1.1 Current State

Interstellar Phase 1 uses `parking_lot::RwLock` for concurrency control:

```
┌─────────────────────────────────────────────────────────────────┐
│              Current: RwLock Concurrency                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Reader 1 ──┐                                                  │
│   Reader 2 ──┼──▶ RwLock::read()  ──▶ Shared Access             │
│   Reader 3 ──┘                                                  │
│                                                                 │
│   Writer   ────▶ RwLock::write() ──▶ Exclusive Access           │
│                  (blocks all readers)                           │
│                                                                 │
│   Problems:                                                     │
│   • Writers block all readers                                   │
│   • Long traversals block writes                                │
│   • Snapshot lifetime tied to lock guard                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 Proposed Solution

Copy-on-Write snapshots using the `im` crate's persistent data structures:

```
┌─────────────────────────────────────────────────────────────────┐
│              Proposed: Copy-on-Write Snapshots                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Snapshot 1 ──▶ Arc<State v1> ──▶ Reads frozen state           │
│   Snapshot 2 ──▶ Arc<State v2> ──▶ Reads different frozen state │
│                                                                 │
│   Writer ──▶ Clone-on-write ──▶ Creates new state version       │
│              (O(log n) structural sharing)                      │
│                                                                 │
│   Benefits:                                                     │
│   • Readers never block writers                                 │
│   • Writers never block readers                                 │
│   • Snapshots are owned, not borrowed                           │
│   • Automatic cleanup via Arc refcounting                       │
│   • No garbage collection needed                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 Design Goals

| Goal | Description |
|------|-------------|
| Lock-free reads | Snapshots don't hold locks |
| Owned snapshots | Snapshots can outlive the Graph reference |
| Structural sharing | Minimize memory copying via persistent structures |
| Backward compatible | Existing traversal API unchanged |
| Simple implementation | No manual garbage collection |
| Incremental adoption | Can coexist with current RwLock model |

### 1.4 Non-Goals

| Non-Goal | Rationale |
|----------|-----------|
| Concurrent writers | Single-writer model retained; use external lock |
| Transaction support | COW provides snapshots, not transactions |
| Historical queries | Only current + active snapshot states retained |
| Serializable isolation | Snapshot isolation only |

---

## 2. Dependencies

### 2.1 The `im` Crate

Add the `im` crate for persistent/immutable data structures:

```toml
[dependencies]
im = "15.1"
```

The `im` crate provides:

- `im::HashMap<K, V>` - Persistent hash map with O(log₃₂ n) operations
- `im::Vector<T>` - Persistent vector with O(log₃₂ n) operations
- Structural sharing - Clones share unmodified subtrees
- `Clone` is O(1) - Just increments reference counts

### 2.2 Performance Characteristics

| Operation | `std::HashMap` | `im::HashMap` |
|-----------|----------------|---------------|
| Get | O(1) | O(log₃₂ n) ≈ O(1) |
| Insert | O(1) amortized | O(log₃₂ n) |
| Clone | O(n) | O(1) |
| Memory | Contiguous | Tree nodes |

For a graph with 1 million vertices:
- `log₃₂(1,000,000)` ≈ 4 operations per lookup
- Clone is instant regardless of size

---

## 3. Core Data Structures

### 3.1 CowGraphState

The immutable, shareable graph state:

```rust
use im::HashMap as ImHashMap;
use std::sync::Arc;

/// Immutable graph state that can be shared between snapshots
#[derive(Clone)]
pub struct CowGraphState {
    /// Vertex data: VertexId → NodeData
    /// Using Arc<NodeData> for cheap cloning of individual nodes
    pub(crate) vertices: ImHashMap<VertexId, Arc<NodeData>>,
    
    /// Edge data: EdgeId → EdgeData
    pub(crate) edges: ImHashMap<EdgeId, Arc<EdgeData>>,
    
    /// Label index: label_id → set of vertex IDs
    pub(crate) vertex_labels: ImHashMap<u32, Arc<RoaringBitmap>>,
    
    /// Label index: label_id → set of edge IDs
    pub(crate) edge_labels: ImHashMap<u32, Arc<RoaringBitmap>>,
    
    /// String interner (append-only, always shared)
    pub(crate) interner: Arc<StringInterner>,
    
    /// Monotonic version counter
    pub(crate) version: u64,
    
    /// Next vertex ID
    pub(crate) next_vertex_id: u64,
    
    /// Next edge ID
    pub(crate) next_edge_id: u64,
}
```

### 3.2 NodeData and EdgeData

Internal representations remain largely unchanged but wrapped in `Arc`:

```rust
/// Internal vertex representation (unchanged from current)
#[derive(Clone, Debug)]
pub(crate) struct NodeData {
    pub id: VertexId,
    pub label_id: u32,
    pub properties: HashMap<String, Value>,
    pub out_edges: Vec<EdgeId>,
    pub in_edges: Vec<EdgeId>,
}

/// Internal edge representation (unchanged from current)
#[derive(Clone, Debug)]
pub(crate) struct EdgeData {
    pub id: EdgeId,
    pub label_id: u32,
    pub src: VertexId,
    pub dst: VertexId,
    pub properties: HashMap<String, Value>,
}
```

### 3.3 CowGraph

The mutable graph container:

```rust
use parking_lot::RwLock;

/// Copy-on-Write graph with snapshot support
pub struct CowGraph {
    /// Current mutable state (protected by RwLock for thread safety)
    state: RwLock<CowGraphState>,
    
    /// Schema for validation (optional)
    schema: RwLock<Option<GraphSchema>>,
}
```

### 3.4 CowSnapshot

An owned, immutable snapshot:

```rust
/// An owned snapshot of the graph at a point in time.
/// 
/// Unlike the current `GraphSnapshot<'g>`, this snapshot:
/// - Does not hold any locks
/// - Can be sent across threads (`Send + Sync`)
/// - Can outlive the source `CowGraph`
/// - Is immutable and will never change
#[derive(Clone)]
pub struct CowSnapshot {
    /// Shared reference to frozen state
    state: Arc<CowGraphState>,
}
```

---

## 4. Operations

### 4.1 Snapshot Creation

Taking a snapshot is O(1) - just clone the Arc:

```rust
impl CowGraph {
    /// Create a snapshot of the current graph state.
    /// 
    /// This is an O(1) operation that creates a shared reference
    /// to the current state. The snapshot will not reflect any
    /// mutations made after this call.
    /// 
    /// # Thread Safety
    /// 
    /// This method briefly acquires a read lock to clone the state Arc.
    /// The returned snapshot does not hold any locks.
    pub fn snapshot(&self) -> CowSnapshot {
        let state = self.state.read();
        CowSnapshot {
            state: Arc::new((*state).clone()),
        }
    }
}
```

**Note:** The `im::HashMap::clone()` is O(1) due to structural sharing.

### 4.2 Read Operations

Snapshots implement `GraphStorage` for seamless integration:

```rust
impl GraphStorage for CowSnapshot {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.state.vertices.get(&id).map(|node| {
            let label = self.state.interner.resolve(node.label_id)
                .unwrap_or_default()
                .to_string();
            Vertex {
                id: node.id,
                label,
                properties: node.properties.clone(),
            }
        })
    }
    
    fn vertex_count(&self) -> u64 {
        self.state.vertices.len() as u64
    }
    
    fn out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        let node = match self.state.vertices.get(&vertex) {
            Some(n) => n,
            None => return Box::new(std::iter::empty()),
        };
        
        let edges: Vec<_> = node.out_edges.iter()
            .filter_map(|eid| self.get_edge(*eid))
            .collect();
        
        Box::new(edges.into_iter())
    }
    
    // ... other GraphStorage methods
}
```

### 4.3 Write Operations

Writes modify the mutable state, triggering copy-on-write:

```rust
impl CowGraph {
    /// Add a vertex to the graph.
    /// 
    /// This triggers copy-on-write: only the modified paths in the
    /// persistent data structure are copied. Existing snapshots
    /// continue to see the old state.
    pub fn add_vertex(
        &self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> VertexId {
        let mut state = self.state.write();
        
        // Allocate ID
        let id = VertexId(state.next_vertex_id);
        state.next_vertex_id += 1;
        
        // Intern label
        let label_id = state.interner_mut().intern(label);
        
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
        let bitmap = state.vertex_labels
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
    
    /// Update a vertex's properties.
    pub fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> Result<(), StorageError> {
        let mut state = self.state.write();
        
        let node = state.vertices.get(&id)
            .ok_or(StorageError::VertexNotFound(id))?;
        
        // Clone and modify the node
        let mut new_node = (**node).clone();
        new_node.properties.insert(key.to_string(), value);
        
        // Update in persistent map
        state.vertices = state.vertices.update(id, Arc::new(new_node));
        state.version += 1;
        
        Ok(())
    }
    
    /// Add an edge between two vertices.
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
        let label_id = state.interner_mut().intern(label);
        
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
        let bitmap = state.edge_labels
            .get(&label_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(RoaringBitmap::new()));
        let mut new_bitmap = (*bitmap).clone();
        new_bitmap.insert(edge_id.0 as u32);
        state.edge_labels = state.edge_labels.update(label_id, Arc::new(new_bitmap));
        
        state.version += 1;
        
        Ok(edge_id)
    }
}
```

### 4.4 Interner Handling

The `StringInterner` is append-only and can be shared:

```rust
impl CowGraphState {
    /// Get shared reference to interner (for reads)
    pub fn interner(&self) -> &StringInterner {
        &self.interner
    }
    
    /// Get mutable access to interner (for writes)
    /// Uses Arc::make_mut for copy-on-write semantics
    pub fn interner_mut(&mut self) -> &mut StringInterner {
        Arc::make_mut(&mut self.interner)
    }
}
```

---

## 5. Integration with Existing API

### 5.1 Graph Wrapper

Maintain backward compatibility with the existing `Graph` type:

```rust
/// The main Graph type - now backed by CowGraph internally
pub struct Graph {
    inner: CowGraph,
}

impl Graph {
    /// Create a new in-memory graph
    pub fn in_memory() -> Self {
        Self {
            inner: CowGraph::new(),
        }
    }
    
    /// Create a snapshot for read-only traversals
    /// 
    /// The returned snapshot is owned and does not hold any locks.
    pub fn snapshot(&self) -> GraphSnapshot {
        GraphSnapshot {
            inner: self.inner.snapshot(),
        }
    }
    
    /// Get mutable access for writes
    /// 
    /// Note: For backward compatibility, this still uses a guard pattern,
    /// but internally delegates to CowGraph's write methods.
    pub fn mutate(&self) -> GraphMut<'_> {
        GraphMut {
            graph: &self.inner,
        }
    }
}
```

### 5.2 GraphSnapshot Wrapper

```rust
/// A snapshot of the graph for read-only traversals.
/// 
/// This is now an owned type that does not borrow from Graph.
#[derive(Clone)]
pub struct GraphSnapshot {
    inner: CowSnapshot,
}

impl GraphSnapshot {
    /// Create a traversal source
    pub fn traversal(&self) -> GraphTraversalSource<'_> {
        GraphTraversalSource::new(self, self.inner.interner())
    }
    
    /// Get the snapshot version
    pub fn version(&self) -> u64 {
        self.inner.state.version
    }
}

impl GraphStorage for GraphSnapshot {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.inner.get_vertex(id)
    }
    
    // ... delegate all methods to inner
}
```

### 5.3 Traversal Integration

The traversal engine needs minimal changes:

```rust
impl<'g> GraphTraversalSource<'g> {
    /// Create traversal source from a snapshot
    pub fn new(snapshot: &'g GraphSnapshot, interner: &'g StringInterner) -> Self {
        Self {
            storage: snapshot,
            interner,
        }
    }
    
    // All existing methods work unchanged since GraphSnapshot
    // implements GraphStorage
}
```

---

## 6. Memory Management

### 6.1 Automatic Cleanup

Memory is automatically reclaimed via `Arc` reference counting:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Memory Lifecycle                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  T=0: Graph created                                             │
│       State v1 (refcount = 1, owned by Graph)                   │
│                                                                 │
│  T=1: Snapshot A taken                                          │
│       State v1 (refcount = 2: Graph + Snapshot A)               │
│                                                                 │
│  T=2: Write occurs, new state created                           │
│       State v1 (refcount = 1: Snapshot A only)                  │
│       State v2 (refcount = 1: Graph only)                       │
│       [Structural sharing: unchanged nodes shared]              │
│                                                                 │
│  T=3: Snapshot A dropped                                        │
│       State v1 (refcount = 0, FREED)                            │
│       State v2 (refcount = 1: Graph only)                       │
│       [Only nodes unique to v1 are freed]                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 Structural Sharing

The `im` crate uses structural sharing to minimize copying:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Structural Sharing                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Before mutation (State v1):                                    │
│                                                                 │
│         Root                                                    │
│        /    \                                                   │
│     Node A   Node B                                             │
│      / \       |                                                │
│    V1  V2     V3                                                │
│                                                                 │
│  After updating V1 (State v2):                                  │
│                                                                 │
│    Root'              Root (v1, still valid)                    │
│    /    \            /    \                                     │
│ Node A'  Node B   Node A   Node B                               │
│   / \      |        / \      |                                  │
│ V1'  V2   V3      V1  V2    V3                                  │
│       ↑    ↑           ↑     ↑                                  │
│       └────┴───────────┴─────┘                                  │
│              (shared)                                           │
│                                                                 │
│  Memory: Only Root', Node A', V1' are new allocations           │
│  Cost: O(log₃₂ n) nodes copied, not O(n)                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 6.3 Memory Overhead

| Component | Current | COW |
|-----------|---------|-----|
| HashMap entry | 1 pointer | 1 pointer + tree overhead |
| Node/Edge | Direct | Arc wrapper (16 bytes) |
| Snapshot | Lock guard | Arc (16 bytes) |
| Overall | Lower | ~20-30% higher base |

Trade-off: Higher base memory for O(1) snapshots and no lock contention.

---

## 7. Thread Safety

### 7.1 Guarantees

| Type | Send | Sync | Notes |
|------|------|------|-------|
| `CowGraph` | Yes | Yes | RwLock provides synchronization |
| `CowSnapshot` | Yes | Yes | Immutable, no synchronization needed |
| `CowGraphState` | Yes | Yes | Immutable after creation |
| `Arc<NodeData>` | Yes | Yes | Shared ownership |

### 7.2 Concurrency Model

```rust
// Multiple readers - no blocking
let snap1 = graph.snapshot();  // O(1)
let snap2 = graph.snapshot();  // O(1)

// Can use snapshots across threads
std::thread::spawn(move || {
    let count = snap1.vertex_count();
});

std::thread::spawn(move || {
    let count = snap2.vertex_count();
});

// Writer doesn't block readers
graph.add_vertex("person", props);  // Creates new state
// snap1 and snap2 still see old state
```

### 7.3 Write Serialization

Writes are still serialized via `RwLock::write()`:

```rust
// These will be serialized (one blocks the other)
thread::spawn(|| graph.add_vertex("a", props1));
thread::spawn(|| graph.add_vertex("b", props2));
```

For concurrent writes, external coordination is needed (out of scope for this spec).

---

## 8. Performance Characteristics

### 8.1 Operation Complexity

| Operation | Current | COW | Notes |
|-----------|---------|-----|-------|
| `snapshot()` | O(1) | O(1) | Arc clone |
| `get_vertex()` | O(1) | O(log₃₂ n) | Tree traversal |
| `add_vertex()` | O(1)* | O(log₃₂ n) | Path copy |
| `add_edge()` | O(1)* | O(log₃₂ n) | Path copy |
| `vertex_count()` | O(1) | O(1) | Cached |
| Clone graph | O(n) | O(1) | Structural sharing |

*Amortized for HashMap resizing

### 8.2 Benchmarks to Implement

```rust
#[bench]
fn bench_snapshot_creation(b: &mut Bencher) {
    let graph = build_graph(1_000_000);  // 1M vertices
    b.iter(|| graph.snapshot());
    // Target: < 100ns
}

#[bench]
fn bench_vertex_lookup(b: &mut Bencher) {
    let graph = build_graph(1_000_000);
    let snap = graph.snapshot();
    b.iter(|| snap.get_vertex(VertexId(500_000)));
    // Target: < 200ns
}

#[bench]
fn bench_write_during_read(b: &mut Bencher) {
    let graph = Arc::new(build_graph(1_000_000));
    let snap = graph.snapshot();
    
    // Measure write latency while snapshot exists
    b.iter(|| graph.add_vertex("test", HashMap::new()));
    // Target: No degradation vs. no active snapshot
}

#[bench]
fn bench_concurrent_reads(b: &mut Bencher) {
    let graph = Arc::new(build_graph(1_000_000));
    
    // Multiple threads taking snapshots and reading
    b.iter(|| {
        let handles: Vec<_> = (0..8).map(|_| {
            let g = Arc::clone(&graph);
            thread::spawn(move || {
                let snap = g.snapshot();
                snap.vertex_count()
            })
        }).collect();
        
        handles.into_iter().map(|h| h.join().unwrap()).sum::<u64>()
    });
    // Target: Linear scaling with threads
}
```

---

## 9. Migration Path

### 9.1 Phase 1: Add CowGraph (Non-Breaking)

1. Add `im` dependency
2. Create `src/storage/cow.rs` with `CowGraph`, `CowSnapshot`
3. Implement `GraphStorage` for `CowSnapshot`
4. Add comprehensive tests

```rust
// New module structure
src/storage/
├── mod.rs
├── inmemory.rs      // Existing - unchanged
├── cow.rs           // New - CowGraph implementation
└── interner.rs      // Existing - unchanged
```

### 9.2 Phase 2: Integrate with Graph (Optional Breaking)

Option A: New constructors (non-breaking):
```rust
impl Graph {
    pub fn in_memory() -> Self { /* current impl */ }
    pub fn in_memory_cow() -> Self { /* new COW impl */ }
}
```

Option B: Replace default (breaking):
```rust
impl Graph {
    pub fn in_memory() -> Self { /* new COW impl */ }
    pub fn in_memory_legacy() -> Self { /* old impl */ }
}
```

### 9.3 Phase 3: Update Documentation

- Update README with new concurrency model
- Add migration guide for users
- Update benchmarks

---

## 10. Testing Strategy

### 10.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_snapshot_isolation() {
        let graph = CowGraph::new();
        let v1 = graph.add_vertex("person", props("name", "Alice"));
        
        // Take snapshot
        let snap = graph.snapshot();
        
        // Modify graph
        graph.set_vertex_property(v1, "name", "Alicia".into()).unwrap();
        
        // Snapshot still sees old value
        let vertex = snap.get_vertex(v1).unwrap();
        assert_eq!(vertex.properties.get("name"), Some(&Value::from("Alice")));
        
        // New snapshot sees new value
        let snap2 = graph.snapshot();
        let vertex2 = snap2.get_vertex(v1).unwrap();
        assert_eq!(vertex2.properties.get("name"), Some(&Value::from("Alicia")));
    }
    
    #[test]
    fn test_snapshot_survives_graph_modification() {
        let graph = CowGraph::new();
        for i in 0..1000 {
            graph.add_vertex("node", props("id", i));
        }
        
        let snap = graph.snapshot();
        assert_eq!(snap.vertex_count(), 1000);
        
        // Add more vertices
        for i in 1000..2000 {
            graph.add_vertex("node", props("id", i));
        }
        
        // Snapshot unchanged
        assert_eq!(snap.vertex_count(), 1000);
        
        // New snapshot sees all
        assert_eq!(graph.snapshot().vertex_count(), 2000);
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
        };  // graph dropped here
        
        // Snapshot still valid
        assert_eq!(snap.vertex_count(), 1);
    }
    
    #[test]
    fn test_concurrent_snapshots() {
        let graph = Arc::new(CowGraph::new());
        for i in 0..100 {
            graph.add_vertex("node", props("id", i));
        }
        
        let handles: Vec<_> = (0..10).map(|_| {
            let g = Arc::clone(&graph);
            thread::spawn(move || {
                let snap = g.snapshot();
                snap.vertex_count()
            })
        }).collect();
        
        for handle in handles {
            assert_eq!(handle.join().unwrap(), 100);
        }
    }
}
```

### 10.2 Property-Based Tests

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn snapshot_always_consistent(ops in prop::collection::vec(graph_op(), 0..100)) {
        let graph = CowGraph::new();
        let mut snapshots = Vec::new();
        
        for op in ops {
            match op {
                GraphOp::AddVertex(label) => {
                    graph.add_vertex(&label, HashMap::new());
                }
                GraphOp::TakeSnapshot => {
                    snapshots.push((graph.snapshot(), graph.vertex_count()));
                }
            }
        }
        
        // All snapshots should still have their original counts
        for (snap, expected_count) in snapshots {
            assert_eq!(snap.vertex_count(), expected_count);
        }
    }
}
```

### 10.3 Integration Tests

```rust
#[test]
fn test_traversal_on_cow_snapshot() {
    let graph = CowGraph::new();
    
    let alice = graph.add_vertex("person", props("name", "Alice"));
    let bob = graph.add_vertex("person", props("name", "Bob"));
    graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    
    let snap = graph.snapshot();
    let g = GraphTraversalSource::new(&snap, snap.interner());
    
    let friends: Vec<_> = g.v_id(alice)
        .out("knows")
        .values("name")
        .to_list();
    
    assert_eq!(friends, vec![Value::from("Bob")]);
}
```

---

## 11. File Structure

```
src/
├── storage/
│   ├── mod.rs              # Add: pub mod cow;
│   ├── cow.rs              # New: CowGraph, CowGraphState, CowSnapshot
│   ├── inmemory.rs         # Unchanged (or deprecated later)
│   └── interner.rs         # Unchanged
├── graph.rs                # Update: Use CowGraph internally (Phase 2)
└── lib.rs                  # Update: Re-export CowGraph

tests/
└── storage/
    └── cow.rs              # New: COW-specific tests
```

---

## 12. API Summary

### 12.1 Public Types

| Type | Description |
|------|-------------|
| `CowGraph` | Mutable graph with COW semantics |
| `CowSnapshot` | Immutable, owned snapshot |

### 12.2 Public Methods

```rust
impl CowGraph {
    pub fn new() -> Self;
    pub fn snapshot(&self) -> CowSnapshot;
    pub fn add_vertex(&self, label: &str, properties: HashMap<String, Value>) -> VertexId;
    pub fn add_edge(&self, src: VertexId, dst: VertexId, label: &str, properties: HashMap<String, Value>) -> Result<EdgeId, StorageError>;
    pub fn set_vertex_property(&self, id: VertexId, key: &str, value: Value) -> Result<(), StorageError>;
    pub fn set_edge_property(&self, id: EdgeId, key: &str, value: Value) -> Result<(), StorageError>;
    pub fn remove_vertex(&self, id: VertexId) -> Result<(), StorageError>;
    pub fn remove_edge(&self, id: EdgeId) -> Result<(), StorageError>;
    pub fn vertex_count(&self) -> u64;
    pub fn edge_count(&self) -> u64;
}

impl CowSnapshot {
    pub fn version(&self) -> u64;
    pub fn interner(&self) -> &StringInterner;
}

impl GraphStorage for CowSnapshot {
    // All GraphStorage methods
}

impl Clone for CowSnapshot {
    // O(1) clone via Arc
}
```

---

## 13. Success Criteria

| Criterion | Target |
|-----------|--------|
| Snapshot creation | < 100ns |
| Vertex lookup | < 500ns (vs ~50ns current) |
| No lock contention | Readers never block |
| Memory overhead | < 50% increase |
| All existing tests pass | 100% |
| Thread-safe snapshots | Send + Sync |
| Backward compatible API | Minimal breaking changes |

---

## 14. Future Considerations

### 14.1 Potential Enhancements

1. **Batch mutations** - Collect writes and apply atomically
2. **Snapshot compaction** - Merge old snapshots to reduce memory
3. **Persistent storage** - Serialize COW state to disk
4. **Write coalescing** - Buffer rapid writes

### 14.2 Path to Full MVCC

COW snapshots are a stepping stone to full MVCC:

```
Current          COW Snapshots       Full MVCC
────────────────────────────────────────────────►
RwLock           Immutable snapshots  Transaction support
Blocking reads   Lock-free reads      Lock-free reads
No history       Current + active     Historical queries
                 snapshots            Concurrent writers
```

The COW implementation provides:
- Foundation for snapshot isolation
- Experience with persistent data structures  
- Benchmark baseline for MVCC comparison
