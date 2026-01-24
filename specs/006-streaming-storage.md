# Spec: Streaming Storage API for True O(1) Sources

**Status:** Draft  
**Priority:** Medium  
**Depends on:** `005-streaming-executor.md`  
**Related:** `src/storage/mod.rs` (GraphStorage trait)

## Executive Summary

Extend `GraphStorage` with owned-iterator methods that enable true O(1) streaming from source steps. Currently, `StreamingExecutor::build_source` must collect all vertex IDs upfront because `GraphStorage::all_vertices()` returns a borrowed iterator (`+ '_`). This spec adds `StreamableStorage` trait methods that return `'static` iterators by capturing `Arc<Self>`.

## Problem Statement

From `005-streaming-executor.md`, the source iterator forces collection:

```rust
fn build_source(...) -> Box<dyn Iterator<Item = Traverser> + Send> {
    match source {
        Some(TraversalSource::AllVertices) => {
            // Must collect because all_vertices() returns borrowed iterator
            let ids: Vec<_> = storage.all_vertices().map(|v| v.id).collect();
            Box::new(ids.into_iter().map(...))
        }
        // ...
    }
}
```

This defeats streaming for the initial scan: a graph with 10M vertices allocates a 10M-element `Vec<VertexId>` even if only 1 result is needed.

### Root Cause

```rust
pub trait GraphStorage: Send + Sync {
    fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + '_>;
    //                                                         ^^^ 
    //                                          Tied to &self lifetime
}
```

The `'_` lifetime binds the iterator to the borrow of `&self`. To return an iterator from a function, we need `'static`, which requires ownership.

## Goals

1. **True O(1) source streaming**: Iterate vertices/edges without upfront collection
2. **Backward compatible**: Existing `GraphStorage` methods unchanged
3. **Opt-in**: Backends implement streaming methods only if beneficial
4. **Zero overhead when unused**: No Arc wrapping unless streaming is requested

## Non-Goals

- Modifying existing `GraphStorage` method signatures
- Requiring all backends to implement streaming (provide default fallback)
- Parallel/concurrent iteration (future work)

---

## Solution Overview

Add a new `StreamableStorage` trait that extends `GraphStorage` with owned-iterator methods. These methods use `&self` receivers and return `'static` iterators by cloning internal Arc-wrapped state.

```rust
pub trait StreamableStorage: GraphStorage + 'static {
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send>;
    fn stream_all_edges(&self) -> Box<dyn Iterator<Item = EdgeId> + Send>;
    // ... additional streaming methods
}
```

### Design Rationale

The original spec proposed using `Arc<dyn StreamableStorage>` as a parameter instead of `self`. However, this approach requires `where Self: Sized` bounds which **breaks trait object compatibility**. You cannot call `StreamableStorage::stream_all_vertices(arc_storage)` when `arc_storage` is `Arc<dyn StreamableStorage>`.

The revised design uses `&self` methods because:
1. It works with trait objects (`dyn StreamableStorage`)
2. Implementations like `GraphSnapshot` are cheap to clone (internally just `Arc<GraphState>`)
3. The returned iterator captures a clone of `self` or relevant internal data
4. It's idiomatic Rust

### Why `VertexId`/`EdgeId` Instead of `Vertex`/`Edge`?

Returning IDs instead of full elements:
1. **Smaller memory footprint**: `VertexId` is 8 bytes vs `Vertex` with String + HashMap
2. **Lazy property loading**: Caller fetches full vertex only when needed
3. **Matches traverser model**: `Traverser` typically holds `Value::VertexId`, not full vertex

---

## Detailed Design

### Chunk 1: StreamableStorage Trait

**Files:** `src/storage/mod.rs`  
**Effort:** 0.5 days

```rust
/// Extension trait for storage backends that support streaming iteration.
///
/// Unlike [`GraphStorage`] which returns borrowed iterators tied to `&self`,
/// `StreamableStorage` returns owned (`'static`) iterators by cloning internal
/// Arc-wrapped state. This enables true streaming in [`StreamingExecutor`] 
/// without upfront collection.
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
/// Backends should override these for true streaming behavior.
pub trait StreamableStorage: GraphStorage + 'static {
    /// Stream all vertex IDs without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects all vertex IDs upfront (O(V) memory). Override for true streaming.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let storage: &dyn StreamableStorage = &snapshot;
    /// let first_10: Vec<_> = storage.stream_all_vertices()
    ///     .take(10)
    ///     .collect();
    /// ```
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Default: collect all IDs (fallback for backends that don't override)
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
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Used by OutStep::apply_streaming
    /// let neighbors = storage.stream_out_neighbors(vertex_id, &label_ids);
    /// for target_id in neighbors.take(10) {
    ///     // Process only first 10 neighbors
    /// }
    /// ```
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
                    label_ids_owned.iter().any(|&lid| {
                        interner.lookup(&e.label) == Some(lid)
                    })
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
                    label_ids_owned.iter().any(|&lid| {
                        interner.lookup(&e.label) == Some(lid)
                    })
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
```

### Chunk 2: InMemoryGraph Implementation

**Files:** `src/storage/inmemory.rs`  
**Effort:** 1 day

```rust
impl StreamableStorage for InMemoryGraph {
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Clone the vertex ID list (just u64s, cheap)
        let ids: Vec<VertexId> = self.nodes.keys().copied().collect();
        Box::new(ids.into_iter())
    }
    
    fn stream_all_edges(&self) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let ids: Vec<EdgeId> = self.edges.keys().copied().collect();
        Box::new(ids.into_iter())
    }
    
    fn stream_vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Use RoaringTreemap for efficient iteration
        let label_id = self.string_table.lookup(label);
        let ids: Vec<VertexId> = label_id
            .and_then(|id| self.vertex_labels.get(&id))
            .into_iter()
            .flat_map(|bitmap| bitmap.iter())
            .map(VertexId)
            .collect();
        Box::new(ids.into_iter())
    }
    
    fn stream_out_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let label_ids_owned: Vec<u32> = label_ids.to_vec();
        let neighbors: Vec<VertexId> = self
            .nodes
            .get(&vertex)
            .into_iter()
            .flat_map(|node| node.out_edges.iter())
            .filter_map(|&edge_id| self.edges.get(&edge_id))
            .filter(move |edge| {
                label_ids_owned.is_empty() || label_ids_owned.contains(&edge.label_id)
            })
            .map(|edge| edge.dst)
            .collect();
        Box::new(neighbors.into_iter())
    }
    
    // ... similar for other methods
}
```

### Note on True O(1) Streaming

The current `InMemoryGraph` implementation collects IDs from HashMap keys, which is still O(V).
For true O(1) streaming, `InMemoryGraph` would need an ordered list of vertex IDs:

```rust
pub struct InMemoryGraph {
    // Existing
    nodes: HashMap<VertexId, NodeData>,
    edges: HashMap<EdgeId, EdgeData>,
    
    // New: ordered list for streaming iteration
    vertex_order: Vec<VertexId>,  // Append-only, supports streaming
    edge_order: Vec<EdgeId>,
    
    // ... rest unchanged
}
```

**Trade-offs:**
- (+) True O(1) streaming without collecting
- (+) Stable iteration order
- (-) Extra memory: 8 bytes per vertex/edge
- (-) Slightly slower removal (need to update vec or use tombstones)

This optimization is deferred to future work. The initial implementation provides
the correct API with a collecting fallback.

### Alternative: RoaringTreemap Iteration

If `InMemoryGraph` uses `RoaringTreemap` for vertex ID tracking (future optimization):

```rust
fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
    // RoaringTreemap iter is owned after clone
    let bitmap = self.active_vertices.clone();
    Box::new(bitmap.iter().map(VertexId))
}
```

This provides O(active_vertices / 64) memory for the bitmap clone, much better than O(V) for IDs.

---

### Chunk 3: GraphSnapshot Implementation

**Files:** `src/storage/cow.rs`  
**Effort:** 0.5 days

`GraphSnapshot` is an ideal candidate because it's already cheap to clone (just `Arc<GraphState>`):

```rust
impl StreamableStorage for GraphSnapshot {
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Clone is cheap - just Arc increment
        let ids: Vec<VertexId> = self.state.vertices.keys().copied().collect();
        Box::new(ids.into_iter())
    }
    
    fn stream_all_edges(&self) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let ids: Vec<EdgeId> = self.state.edges.keys().copied().collect();
        Box::new(ids.into_iter())
    }
    
    fn stream_vertices_with_label(&self, label: &str) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let label_id = self.interner_snapshot.lookup(label);
        let ids: Vec<VertexId> = label_id
            .and_then(|id| self.state.vertex_labels.get(&id))
            .into_iter()
            .flat_map(|bitmap| bitmap.iter())
            .map(|id| VertexId(id as u64))
            .collect();
        Box::new(ids.into_iter())
    }
    
    fn stream_out_neighbors(
        &self,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let label_ids_owned: Vec<u32> = label_ids.to_vec();
        let neighbors: Vec<VertexId> = self
            .state
            .vertices
            .get(&vertex)
            .into_iter()
            .flat_map(|node| node.out_edges.iter())
            .filter_map(|&edge_id| self.state.edges.get(&edge_id))
            .filter(move |edge| {
                label_ids_owned.is_empty() || label_ids_owned.contains(&edge.label_id)
            })
            .map(|edge| edge.dst)
            .collect();
        Box::new(neighbors.into_iter())
    }
    
    // ... similar for other methods
}
```

---

### Chunk 4: MmapGraph Implementation (Optional)

**Files:** `src/storage/mmap.rs`  
**Effort:** 1 day

MmapGraph can use cursor-based iteration over file positions:

```rust
impl StreamableStorage for MmapGraph {
    fn stream_all_vertices(&self) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Collect vertex IDs - mmap can iterate sequentially through file
        let count = self.vertex_count();
        let ids: Vec<VertexId> = (0..count)
            .map(VertexId)
            .filter(|&id| self.get_vertex(id).is_some())
            .collect();
        Box::new(ids.into_iter())
    }
}
```

For true streaming without collection, a cursor-based iterator can be added
in future work.

---

### Chunk 5: StreamingExecutor Integration

**Files:** `src/traversal/streaming.rs`  
**Effort:** 0.5 days

Update `StreamingExecutor` to accept `Arc<dyn StreamableStorage>` and use streaming methods:

```rust
impl StreamingExecutor {
    /// Create a new streaming executor with true O(1) streaming source iteration.
    pub fn new_streaming(
        storage: Arc<dyn StreamableStorage>,
        interner: Arc<StringInterner>,
        steps: Vec<Box<dyn DynStep>>,
        source: Option<TraversalSource>,
        track_paths: bool,
    ) -> Self {
        let side_effects = SideEffects::new();
        let ctx = StreamingContext::new(
            storage.clone() as Arc<dyn GraphStorage + Send + Sync>,
            interner.clone(),
        )
        .with_side_effects(side_effects.clone())
        .with_path_tracking(track_paths);

        // Build streaming source iterator
        let source_iter = Self::build_streaming_source(storage, source, track_paths);

        // Chain adapters
        let iter = steps.into_iter().fold(
            source_iter,
            |input, step| -> Box<dyn Iterator<Item = Traverser> + Send> {
                Box::new(StreamingAdapter::new(step, ctx.clone(), input))
            },
        );

        Self { iter, side_effects }
    }

    fn build_streaming_source(
        storage: Arc<dyn StreamableStorage>,
        source: Option<TraversalSource>,
        track_paths: bool,
    ) -> Box<dyn Iterator<Item = Traverser> + Send> {
        match source {
            Some(TraversalSource::AllVertices) => {
                // True streaming via StreamableStorage
                let iter = storage.stream_all_vertices();
                Box::new(iter.map(move |id| {
                    let mut t = Traverser::new(Value::Vertex(id));
                    if track_paths { t.extend_path_unlabeled(); }
                    t
                }))
            }
            Some(TraversalSource::AllEdges) => {
                let iter = storage.stream_all_edges();
                Box::new(iter.map(move |id| {
                    let mut t = Traverser::new(Value::Edge(id));
                    if track_paths { t.extend_path_unlabeled(); }
                    t
                }))
            }
            Some(TraversalSource::Vertices(ids)) => {
                // Already owned, just validate existence
                Box::new(ids.into_iter().filter_map(move |id| {
                    storage.get_vertex(id).map(|_| {
                        let mut t = Traverser::new(Value::Vertex(id));
                        if track_paths { t.extend_path_unlabeled(); }
                        t
                    })
                }))
            }
            // ... other sources
            None => Box::new(std::iter::empty()),
        }
    }
}
```

---

### Chunk 6: GraphSnapshot arc_streamable Method

**Files:** `src/storage/cow.rs`  
**Effort:** 0.5 days

Add `arc_streamable()` method to `GraphSnapshot`:

```rust
impl GraphSnapshot {
    /// Get Arc-wrapped streamable storage for true O(1) streaming execution.
    ///
    /// Returns a clone of self wrapped in Arc as `dyn StreamableStorage`.
    pub fn arc_streamable(&self) -> Arc<dyn StreamableStorage> {
        Arc::new(self.clone())
    }
}
```

Since `GraphSnapshot` implements `StreamableStorage` and is cheap to clone
(internally just `Arc<GraphState>`), this provides an efficient way to get
owned storage for streaming pipelines.

---

## Implementation Plan

| Chunk | Effort | Description |
|-------|--------|-------------|
| 1. StreamableStorage trait | 0.5 days | New trait with default impls |
| 2. InMemoryGraph impl | 0.5 days | StreamableStorage for inmemory backend |
| 3. GraphSnapshot impl | 0.5 days | StreamableStorage for COW snapshot |
| 4. StreamingExecutor update | 0.5 days | Add new_streaming constructor |
| 5. arc_streamable method | 0.25 days | Helper on GraphSnapshot |
| 6. Tests | 0.5 days | Verify streaming behavior |

**Total: ~2.75 days**

---

## Memory Comparison

### Source Iteration

| Scenario | Current | With StreamableStorage |
|----------|---------|------------------------|
| `g.v().iter().next()` on 10M vertices | O(10M) - collect all IDs | O(1) - single ID |
| `g.v().iter().take(100).collect()` | O(10M) | O(100) |
| `g.v().has_label("person").iter()` on 1M persons | O(1M) | O(1) per iteration |

### Navigation Steps (with neighbor streaming)

| Scenario | Without 006 | With 006 |
|----------|-------------|----------|
| `g.v(id).out().iter().next()` on vertex with 10K edges | O(10K) - collect all edges | O(1) - single neighbor |
| `g.v(id).out().out().iter().next()` | O(degree^2) worst case | O(1) per step |
| `g.v().out("knows").iter().take(10)` | O(V) source + O(max_degree) per vertex | O(10) total |

---

## Alternatives Considered

### 1. Modify GraphStorage to return owned iterators

```rust
fn all_vertices(&self) -> Box<dyn Iterator<Item = Vertex> + Send + 'static>;
```

**Rejected:** Breaking change to all existing implementations. Would require every backend to wrap storage in Arc internally.

### 2. Use `self: Arc<Self>` receiver

```rust
fn stream_all_vertices(self: Arc<Self>) -> Box<dyn Iterator<Item = VertexId> + Send>;
```

**Rejected:** Requires `#![feature(arbitrary_self_types)]` which is unstable. Also doesn't work with trait objects without additional complexity.

### 3. Cursor trait with explicit state

```rust
pub trait VertexCursor: Send {
    fn next(&mut self, storage: &dyn GraphStorage) -> Option<VertexId>;
}
```

**Rejected:** More complex API, requires caller to manage cursor lifetime and storage reference separately.

### 4. Channel-based streaming

Use `crossbeam` or `tokio` channels to decouple producer/consumer.

**Rejected:** Adds async complexity, thread overhead for simple use cases. Could be added later for parallel iteration.

---

## Testing

### Unit Tests

```rust
#[test]
fn stream_all_vertices_returns_all() {
    let graph = test_graph_with_1000_vertices();
    let storage: Arc<dyn StreamableStorage> = Arc::new(graph);
    
    let streamed: Vec<_> = StreamableStorage::stream_all_vertices(storage.clone()).collect();
    let eager: Vec<_> = storage.all_vertices().map(|v| v.id).collect();
    
    assert_eq!(streamed.len(), eager.len());
    assert_eq!(streamed.into_iter().collect::<HashSet<_>>(), 
               eager.into_iter().collect::<HashSet<_>>());
}

#[test]
fn stream_early_termination() {
    let graph = test_graph_with_1000_vertices();
    let storage: Arc<dyn StreamableStorage> = Arc::new(graph);
    
    let first_10: Vec<_> = StreamableStorage::stream_all_vertices(storage)
        .take(10)
        .collect();
    
    assert_eq!(first_10.len(), 10);
}
```

### Memory Tests

Use a memory tracking allocator to verify O(1) streaming:

```rust
#[test]
fn stream_constant_memory() {
    let graph = test_graph_with_100k_vertices();
    let storage: Arc<dyn StreamableStorage> = Arc::new(graph);
    
    let baseline = current_memory_usage();
    
    // Iterate but don't collect
    let mut count = 0;
    for _id in StreamableStorage::stream_all_vertices(storage).take(1000) {
        count += 1;
    }
    
    let after = current_memory_usage();
    
    // Should not have allocated 100k IDs
    assert!(after - baseline < 1_000_000, "Memory grew too much: {}", after - baseline);
}
```

---

## Success Criteria

| Criterion | Verification |
|-----------|--------------|
| No behavior change | Existing tests pass |
| Streaming works | `iter().next()` on large graph is O(1) memory |
| Early termination | `take(n)` processes only n items from source |
| Fallback works | Backends without override still function (collect) |

---

## Future Work

1. **Parallel streaming**: Add `par_stream_all_vertices()` returning `rayon::ParallelIterator`
2. **Async streaming**: Add `async_stream_all_vertices()` returning `Stream` for async contexts
3. **Filtered streaming**: Push filters into storage layer for index-aware streaming
