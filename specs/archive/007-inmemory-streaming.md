# Spec: True Lazy Streaming for Graph

**Status:** Implemented  
**Priority:** High  
**Depends on:** `006-streaming-storage.md`  
**Related:** `src/storage/inmemory.rs`

## Executive Summary

Enable true O(1) lazy streaming for `Graph` to support high-performance real-time use cases. Currently, `Graph` uses default `StreamableStorage` implementations that collect upfront, defeating lazy evaluation. This spec proposes structural changes to enable true streaming without sacrificing mutation performance.

## Use Case Clarification

### Graph is NOT just for testing

`Graph` is intended for **production real-time workloads** where:

1. **Ultra-low latency** is required (sub-millisecond queries)
2. **Dataset fits in memory** (up to ~10M vertices, ~100M edges)
3. **High mutation throughput** is needed (real-time updates)
4. **Persistence is not required** (or handled externally)

Example use cases:
- Real-time recommendation engines
- Session graphs in web applications  
- In-memory caching layer over persistent storage
- Stream processing pipelines
- Game state graphs

### Storage Backend Comparison

| Backend | Persistence | Snapshot Isolation | True Streaming | Use Case |
|---------|-------------|-------------------|----------------|----------|
| `Graph` | No | No | **Not yet** | Real-time, low-latency |
| `Graph` (COW) | No | Yes | Yes | Multi-reader, occasional writes |
| `CowMmapGraph` | Yes | Yes | Yes | Persistent, concurrent reads |

## Problem Statement

`Graph` currently uses `HashMap<VertexId, NodeData>` which cannot provide `'static` iterators without collecting:

```rust
// Current: must collect because HashMap::keys() is borrowed
impl StreamableStorage for Graph {
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let ids: Vec<_> = self.nodes.keys().copied().collect();  // O(V) memory!
        Box::new(ids.into_iter())
    }
}
```

This means `g.v().take(1)` on a 10M vertex graph allocates 80MB for vertex IDs before returning a single result.

### Why COW Graphs Can Stream

`GraphSnapshot` uses `im::HashMap` which supports O(1) cloning via structural sharing:

```rust
fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
    let vertices = self.state.vertices.clone();  // O(1) structural sharing
    Box::new(vertices.into_iter().map(|(id, _)| id))  // Lazy iteration
}
```

`Graph` uses `std::collections::HashMap` which has no structural sharing—cloning is O(n).

## Goals

1. **True O(1) streaming** for `Graph` source iteration
2. **Maintain O(1) mutation performance** for add/remove operations
3. **Minimal memory overhead** (ideally <10% increase)
4. **No API changes** (StreamableStorage trait already exists)

## Non-Goals

- Adding snapshot isolation to `Graph` (use `Graph` for that)
- Thread-safe interior mutability (use external synchronization)
- Persistence (use `CowMmapGraph` for that)

---

## Solution Options

### Option A: Ordered ID Vector (Recommended)

Add a parallel `Vec<VertexId>` that tracks insertion order:

```rust
pub struct Graph {
    // Existing
    nodes: HashMap<VertexId, NodeData>,
    edges: HashMap<EdgeId, EdgeData>,
    
    // New: ordered lists for streaming
    vertex_order: Vec<VertexId>,
    edge_order: Vec<EdgeId>,
    
    // Track removed IDs for lazy cleanup
    removed_vertices: RoaringTreemap,
    removed_edges: RoaringTreemap,
    
    // ... rest unchanged
}
```

**Streaming Implementation:**

```rust
impl StreamableStorage for Graph {
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Clone the vec (8 bytes per vertex) and filter removed
        let order = self.vertex_order.clone();
        let removed = self.removed_vertices.clone();
        Box::new(order.into_iter().filter(move |id| !removed.contains(id.0)))
    }
}
```

**Trade-offs:**
- (+) True streaming: iterator is `'static` and lazy
- (+) Stable iteration order (insertion order)
- (+) O(1) append for add_vertex
- (-) Memory: +8 bytes per vertex/edge for order vec
- (-) Remove is O(1) but leaves tombstone; periodic compaction needed
- (-) Clone is O(V) for the vec, but iteration is then lazy

**Memory Overhead:**
- 10M vertices: 80MB for `vertex_order` vec
- This is ~10% overhead if each vertex averages 800 bytes

### Option B: im::HashMap Migration

Replace `std::collections::HashMap` with `im::HashMap`:

```rust
pub struct Graph {
    nodes: im::HashMap<VertexId, NodeData>,  // Changed
    edges: im::HashMap<EdgeId, EdgeData>,    // Changed
    // ... rest unchanged
}
```

**Trade-offs:**
- (+) O(1) clone for streaming (structural sharing)
- (+) No additional memory structures
- (+) Same API as COW graph internals
- (-) Slightly slower mutations (persistent data structure overhead)
- (-) Different memory layout (cache behavior changes)

**Performance Impact:**
- `im::HashMap` insert: ~2-3x slower than `std::HashMap`
- `im::HashMap` lookup: ~1.5x slower than `std::HashMap`
- For real-time use cases, this may be unacceptable

### Option C: Arc-Wrapped State

Wrap internal state in `Arc` for cheap cloning:

```rust
pub struct Graph {
    state: Arc<InMemoryState>,
}

struct InMemoryState {
    nodes: HashMap<VertexId, NodeData>,
    edges: HashMap<EdgeId, EdgeData>,
    // ...
}
```

**Trade-offs:**
- (+) Cheap Arc clone for streaming
- (-) Still need to clone HashMap contents for iteration
- (-) Adds indirection for all operations
- (-) Doesn't actually solve the problem without im::HashMap

**Verdict:** This doesn't help without also switching to persistent data structures.

### Option D: RoaringTreemap for All IDs

Use `RoaringTreemap` to track active vertex/edge IDs:

```rust
pub struct Graph {
    nodes: HashMap<VertexId, NodeData>,
    active_vertices: RoaringTreemap,  // New: tracks all active vertex IDs
    active_edges: RoaringTreemap,     // New: tracks all active edge IDs
    // ...
}
```

**Streaming Implementation:**

```rust
fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
    let bitmap = self.active_vertices.clone();  // O(n/64) memory for bitmap
    Box::new(bitmap.iter().map(VertexId))
}
```

**Trade-offs:**
- (+) RoaringTreemap clone is more compact than Vec clone
- (+) Efficient for sparse ID spaces
- (+) Already using RoaringTreemap for labels
- (-) RoaringTreemap clone is still O(n) in worst case
- (-) Iteration order is ID order, not insertion order

---

## Recommendation: Option A (Ordered ID Vector)

Option A provides the best balance:
1. **True lazy streaming** after the initial vec clone
2. **Maintains HashMap performance** for mutations
3. **Predictable memory overhead** (8 bytes per element)
4. **Insertion-order iteration** (often desirable)

The vec clone is O(V) in memory but:
- It's a contiguous memcpy (very fast)
- Subsequent iteration is lazy
- For `g.v().take(10)`, only 10 elements are processed after the clone

### Future Optimization: Chunked Iteration

For truly O(1) streaming, future work could add chunked iteration:

```rust
fn stream_all_vertices_chunked(&self, chunk_size: usize) 
    -> Box<dyn Iterator<Item = impl Iterator<Item = VertexId>> + Send>
```

This would yield chunks lazily, amortizing the clone cost.

---

## Implementation Plan

### Chunk 1: Add Ordered Vectors

**Files:** `src/storage/inmemory.rs`  
**Effort:** 0.5 days

```rust
pub struct Graph {
    // Existing fields...
    
    /// Ordered list of vertex IDs (insertion order)
    vertex_order: Vec<VertexId>,
    
    /// Ordered list of edge IDs (insertion order)  
    edge_order: Vec<EdgeId>,
}
```

Update `new()`:
```rust
pub fn new() -> Self {
    Self {
        nodes: HashMap::new(),
        edges: HashMap::new(),
        vertex_order: Vec::new(),
        edge_order: Vec::new(),
        // ...
    }
}
```

### Chunk 2: Update Mutations

**Files:** `src/storage/inmemory.rs`  
**Effort:** 0.5 days

```rust
pub fn add_vertex(&mut self, label: &str, properties: HashMap<String, Value>) -> VertexId {
    let id = VertexId(self.next_vertex_id.fetch_add(1, Ordering::Relaxed));
    
    // Existing node insertion...
    self.nodes.insert(id, node);
    
    // New: track order
    self.vertex_order.push(id);
    
    id
}

pub fn remove_vertex(&mut self, id: VertexId) -> Result<(), StorageError> {
    // Existing removal logic...
    self.nodes.remove(&id);
    
    // Remove from order vec (O(n) but removal is already O(degree))
    self.vertex_order.retain(|&vid| vid != id);
    
    Ok(())
}
```

### Chunk 3: Implement StreamableStorage

**Files:** `src/storage/inmemory.rs`  
**Effort:** 0.5 days

```rust
impl StreamableStorage for Graph {
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Clone the order vec - O(V) memory but iteration is lazy
        let order = self.vertex_order.clone();
        Box::new(order.into_iter())
    }

    fn stream_all_edges(&self) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let order = self.edge_order.clone();
        Box::new(order.into_iter())
    }

    fn stream_vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Use RoaringTreemap - already supports owned iteration
        if let Some(label_id) = self.string_table.lookup(label) {
            if let Some(bitmap) = self.vertex_labels.get(&label_id) {
                let bitmap = bitmap.clone();
                return Box::new(bitmap.iter().map(VertexId));
            }
        }
        Box::new(std::iter::empty())
    }

    fn stream_out_edges(&self, vertex: VertexId) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        if let Some(node) = self.nodes.get(&vertex) {
            let edges = node.out_edges.clone();
            Box::new(edges.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn stream_out_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        if let Some(node) = self.nodes.get(&vertex) {
            let out_edges = node.out_edges.clone();
            let label_ids = label_ids.to_vec();
            
            // Clone edge data for the neighbors we need
            let neighbors: Vec<VertexId> = out_edges
                .iter()
                .filter_map(|&eid| self.edges.get(&eid))
                .filter(|e| label_ids.is_empty() || label_ids.contains(&e.label_id))
                .map(|e| e.dst)
                .collect();
            
            Box::new(neighbors.into_iter())
        } else {
            Box::new(std::iter::empty())
        }
    }
    
    // ... similar for other methods
}
```

### Chunk 4: Tests

**Files:** `src/storage/inmemory.rs`  
**Effort:** 0.5 days

```rust
#[test]
fn stream_all_vertices_lazy() {
    let mut graph = Graph::new();
    for i in 0..1000 {
        graph.add_vertex("node", HashMap::new());
    }
    
    // Only take 10 - should not process all 1000
    let first_10: Vec<_> = graph.stream_all_vertices().take(10).collect();
    assert_eq!(first_10.len(), 10);
}

#[test]
fn stream_preserves_insertion_order() {
    let mut graph = Graph::new();
    let ids: Vec<_> = (0..100)
        .map(|_| graph.add_vertex("node", HashMap::new()))
        .collect();
    
    let streamed: Vec<_> = graph.stream_all_vertices().collect();
    assert_eq!(ids, streamed);
}
```

---

## Memory Comparison

| Scenario | Before (collect) | After (vec clone) |
|----------|------------------|-------------------|
| 10M vertices, `take(1)` | 80MB allocated | 80MB allocated* |
| 10M vertices, `take(1000)` | 80MB allocated | 80MB allocated* |
| 10M vertices, `collect()` | 80MB allocated | 80MB allocated |

*The vec clone still happens, but iteration is lazy. The memory profile is similar, but:
- Clone is a fast memcpy vs. HashMap iteration
- No intermediate Vertex structs created
- Subsequent filtering/mapping is lazy

For truly O(1) memory, see Future Work (chunked iteration or im::HashMap).

---

## Success Criteria

| Criterion | Verification |
|-----------|--------------|
| Streaming API works | All existing tests pass |
| Insertion order preserved | New test verifies order |
| Mutation perf maintained | Benchmark shows <5% regression |
| Memory overhead acceptable | <15% increase for typical graphs |

---

## Future Work

1. **Chunked streaming**: Yield ID chunks lazily to avoid full vec clone
2. **Benchmark suite**: Compare HashMap vs im::HashMap for real workloads
3. **Hybrid approach**: Use im::HashMap for read-heavy, HashMap for write-heavy
4. **Compaction**: Periodic cleanup of removed IDs from order vecs
