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

Add a new `StreamableStorage` trait that extends `GraphStorage` with owned-iterator methods. These methods take `Arc<Self>` as a parameter (not receiver) to enable cloning the Arc into the returned iterator.

```rust
pub trait StreamableStorage: GraphStorage + 'static {
    fn stream_all_vertices(
        storage: Arc<dyn StreamableStorage>,
    ) -> Box<dyn Iterator<Item = VertexId> + Send>;
    
    fn stream_all_edges(
        storage: Arc<dyn StreamableStorage>,
    ) -> Box<dyn Iterator<Item = EdgeId> + Send>;
    
    // ... additional streaming methods
}
```

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
use std::sync::Arc;

/// Extension trait for storage backends that support streaming iteration.
///
/// Unlike [`GraphStorage`] which returns borrowed iterators tied to `&self`,
/// `StreamableStorage` returns owned iterators that capture `Arc<Self>`.
/// This enables true streaming in [`StreamingExecutor`] without upfront collection.
///
/// # Implementation Notes
///
/// Methods take `Arc<dyn StreamableStorage>` as a regular parameter (not `self`)
/// because Rust doesn't support `self: Arc<Self>` receivers on trait objects
/// without `#![feature(arbitrary_self_types)]`.
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
    /// let storage: Arc<dyn StreamableStorage> = ...;
    /// let first_10: Vec<_> = StreamableStorage::stream_all_vertices(storage)
    ///     .take(10)
    ///     .collect();
    /// ```
    fn stream_all_vertices(
        storage: Arc<dyn StreamableStorage>,
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Default: collect all IDs (fallback for backends that don't override)
        let ids: Vec<_> = storage.all_vertices().map(|v| v.id).collect();
        Box::new(ids.into_iter())
    }
    
    /// Stream all edge IDs without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects all edge IDs upfront (O(E) memory). Override for true streaming.
    fn stream_all_edges(
        storage: Arc<dyn StreamableStorage>,
    ) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let ids: Vec<_> = storage.all_edges().map(|e| e.id).collect();
        Box::new(ids.into_iter())
    }
    
    /// Stream vertex IDs with a given label without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects matching vertex IDs upfront. Override for true streaming.
    fn stream_vertices_with_label(
        storage: Arc<dyn StreamableStorage>,
        label: String,
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let ids: Vec<_> = storage.vertices_with_label(&label).map(|v| v.id).collect();
        Box::new(ids.into_iter())
    }
    
    /// Stream edge IDs with a given label without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects matching edge IDs upfront. Override for true streaming.
    fn stream_edges_with_label(
        storage: Arc<dyn StreamableStorage>,
        label: String,
    ) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let ids: Vec<_> = storage.edges_with_label(&label).map(|e| e.id).collect();
        Box::new(ids.into_iter())
    }
    
    /// Stream outgoing edge IDs from a vertex without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects edge IDs upfront. Override for true streaming.
    fn stream_out_edges(
        storage: Arc<dyn StreamableStorage>,
        vertex: VertexId,
    ) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let ids: Vec<_> = storage.out_edges(vertex).map(|e| e.id).collect();
        Box::new(ids.into_iter())
    }
    
    /// Stream incoming edge IDs to a vertex without collecting.
    ///
    /// # Default Implementation
    ///
    /// Collects edge IDs upfront. Override for true streaming.
    fn stream_in_edges(
        storage: Arc<dyn StreamableStorage>,
        vertex: VertexId,
    ) -> Box<dyn Iterator<Item = EdgeId> + Send> {
        let ids: Vec<_> = storage.in_edges(vertex).map(|e| e.id).collect();
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
    /// * `storage` - Arc-wrapped storage
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
    /// let neighbors = StreamableStorage::stream_out_neighbors(
    ///     storage.clone(),
    ///     vertex_id,
    ///     &label_ids,
    /// );
    /// for target_id in neighbors.take(10) {
    ///     // Process only first 10 neighbors
    /// }
    /// ```
    fn stream_out_neighbors(
        storage: Arc<dyn StreamableStorage>,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let label_ids_owned: Vec<u32> = label_ids.to_vec();
        let neighbors: Vec<_> = storage
            .out_edges(vertex)
            .filter(move |e| {
                if label_ids_owned.is_empty() {
                    true
                } else {
                    // Note: Edge.label is String, need to resolve to label_id
                    // This requires interner access - see implementation notes
                    label_ids_owned.iter().any(|&lid| {
                        storage.interner().lookup(&e.label) == Some(lid)
                    })
                }
            })
            .map(|e| e.dst)
            .collect();
        Box::new(neighbors.into_iter())
    }
    
    /// Stream incoming neighbor vertex IDs without collecting.
    ///
    /// This is the primary method used by navigation steps (`in()`, `in("label")`).
    /// Returns source vertex IDs for incoming edges, optionally filtered by label.
    ///
    /// # Default Implementation
    ///
    /// Collects neighbor IDs upfront. Override for true streaming.
    fn stream_in_neighbors(
        storage: Arc<dyn StreamableStorage>,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let label_ids_owned: Vec<u32> = label_ids.to_vec();
        let neighbors: Vec<_> = storage
            .in_edges(vertex)
            .filter(move |e| {
                if label_ids_owned.is_empty() {
                    true
                } else {
                    label_ids_owned.iter().any(|&lid| {
                        storage.interner().lookup(&e.label) == Some(lid)
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
        storage: Arc<dyn StreamableStorage>,
        vertex: VertexId,
        label_ids: &[u32],
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let out_iter = Self::stream_out_neighbors(storage.clone(), vertex, label_ids);
        let in_iter = Self::stream_in_neighbors(storage, vertex, label_ids);
        Box::new(out_iter.chain(in_iter))
    }
}
```

### Chunk 2: InMemoryGraph Implementation

**Files:** `src/storage/inmemory.rs`  
**Effort:** 1 day

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

impl StreamableStorage for InMemoryGraph {
    fn stream_all_vertices(
        storage: Arc<dyn StreamableStorage>,
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        // Downcast to concrete type for access to internal structure
        // This is safe because we know the concrete type at registration
        let storage = storage
            .as_any()
            .downcast_ref::<InMemoryGraph>()
            .expect("StreamableStorage impl only valid for InMemoryGraph");
        
        // Clone the vertex ID list (just u64s, cheap)
        let ids: Vec<VertexId> = storage.vertices.keys().copied().collect();
        Box::new(ids.into_iter())
    }
    
    // Alternative: cursor-based streaming using atomic index
    fn stream_all_vertices_cursor(
        storage: Arc<dyn StreamableStorage>,
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        Box::new(VertexCursor {
            storage,
            index: AtomicU64::new(0),
        })
    }
}

/// Cursor-based vertex iterator for true O(1) streaming.
struct VertexCursor {
    storage: Arc<dyn StreamableStorage>,
    // Using Vec index rather than HashMap iteration
    // Requires InMemoryGraph to maintain a Vec<VertexId> alongside HashMap
    index: AtomicU64,
}

impl Iterator for VertexCursor {
    type Item = VertexId;
    
    fn next(&mut self) -> Option<VertexId> {
        // Implementation depends on InMemoryGraph internal structure
        // See "InMemoryGraph Structure Changes" below
        todo!()
    }
}
```

### InMemoryGraph Structure Changes

To support true cursor-based streaming, `InMemoryGraph` needs an ordered list of vertex IDs:

```rust
pub struct InMemoryGraph {
    // Existing
    vertices: HashMap<VertexId, VertexData>,
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

### Alternative: RoaringTreemap Iteration

If `InMemoryGraph` uses `RoaringTreemap` for vertex ID allocation:

```rust
fn stream_all_vertices(
    storage: Arc<dyn StreamableStorage>,
) -> Box<dyn Iterator<Item = VertexId> + Send> {
    let inmem = storage.as_any().downcast_ref::<InMemoryGraph>().unwrap();
    
    // RoaringTreemap iter is owned after clone
    let bitmap = inmem.active_vertices.clone();
    Box::new(bitmap.iter().map(VertexId))
}
```

This provides O(active_vertices / 64) memory for the bitmap clone, much better than O(V) for IDs.

---

### Chunk 3: MmapGraph Implementation

**Files:** `src/storage/mmap.rs`  
**Effort:** 1 day

```rust
impl StreamableStorage for MmapGraph {
    fn stream_all_vertices(
        storage: Arc<dyn StreamableStorage>,
    ) -> Box<dyn Iterator<Item = VertexId> + Send> {
        let mmap = storage.as_any().downcast_ref::<MmapGraph>().unwrap();
        
        // Mmap can iterate by file position
        Box::new(MmapVertexIter {
            storage: storage.clone(),
            position: 0,
            count: mmap.vertex_count(),
        })
    }
}

struct MmapVertexIter {
    storage: Arc<dyn StreamableStorage>,
    position: u64,
    count: u64,
}

impl Iterator for MmapVertexIter {
    type Item = VertexId;
    
    fn next(&mut self) -> Option<VertexId> {
        if self.position >= self.count {
            return None;
        }
        
        let mmap = self.storage.as_any().downcast_ref::<MmapGraph>().unwrap();
        
        // Read vertex ID at current file position
        // Skip deleted records (tombstones)
        loop {
            if self.position >= self.count {
                return None;
            }
            
            let id = VertexId(self.position);
            self.position += 1;
            
            if mmap.get_vertex(id).is_some() {
                return Some(id);
            }
        }
    }
}
```

---

### Chunk 4: as_any() Support

**Files:** `src/storage/mod.rs`  
**Effort:** 0.5 days

Add `as_any()` method to enable downcasting:

```rust
pub trait GraphStorage: Send + Sync {
    // ... existing methods ...
    
    /// Returns self as `Any` for downcasting.
    ///
    /// Used by `StreamableStorage` implementations to access backend-specific
    /// internals for optimized streaming.
    fn as_any(&self) -> &dyn std::any::Any;
}

impl GraphStorage for InMemoryGraph {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    // ... rest unchanged
}
```

---

### Chunk 5: StreamingExecutor Integration

**Files:** `src/traversal/streaming.rs`  
**Effort:** 0.5 days

Update `StreamingExecutor::build_source` to use streaming methods:

```rust
impl StreamingExecutor {
    fn build_source(
        storage: Arc<dyn StreamableStorage>,
        source: Option<TraversalSource>,
        track_paths: bool,
    ) -> Box<dyn Iterator<Item = Traverser> + Send> {
        match source {
            Some(TraversalSource::AllVertices) => {
                // True streaming - no collection!
                let iter = StreamableStorage::stream_all_vertices(storage);
                Box::new(iter.map(move |id| {
                    let mut t = Traverser::new(Value::VertexId(id));
                    if track_paths { t.init_path(); }
                    t
                }))
            }
            Some(TraversalSource::AllEdges) => {
                let iter = StreamableStorage::stream_all_edges(storage);
                Box::new(iter.map(move |id| {
                    let mut t = Traverser::new(Value::EdgeId(id));
                    if track_paths { t.init_path(); }
                    t
                }))
            }
            Some(TraversalSource::VerticesWithLabel(labels)) => {
                // Stream each label, chain results
                let iters: Vec<_> = labels.into_iter()
                    .map(|label| {
                        StreamableStorage::stream_vertices_with_label(
                            storage.clone(), 
                            label
                        )
                    })
                    .collect();
                
                let iter = iters.into_iter().flatten();
                Box::new(iter.map(move |id| {
                    let mut t = Traverser::new(Value::VertexId(id));
                    if track_paths { t.init_path(); }
                    t
                }))
            }
            Some(TraversalSource::Vertices(ids)) => {
                // Already owned, just validate existence
                let storage_clone = storage.clone();
                Box::new(ids.into_iter().filter_map(move |id| {
                    storage_clone.get_vertex(id).map(|_| {
                        let mut t = Traverser::new(Value::VertexId(id));
                        if track_paths { t.init_path(); }
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

### Chunk 6: GraphSnapshot with StreamableStorage

**Files:** `src/graph.rs`  
**Effort:** 0.5 days

Ensure `GraphSnapshot` can provide `Arc<dyn StreamableStorage>`:

```rust
impl GraphSnapshot {
    /// Get Arc-wrapped storage for streaming execution.
    pub fn arc_streamable(&self) -> Arc<dyn StreamableStorage> {
        // Storage must implement StreamableStorage
        self.storage.clone() as Arc<dyn StreamableStorage>
    }
}
```

This requires `GraphSnapshot` to store `Arc<dyn StreamableStorage>` instead of `Arc<dyn GraphStorage>`, or use a separate field.

---

## Implementation Plan

| Chunk | Effort | Description |
|-------|--------|-------------|
| 1. StreamableStorage trait | 0.5 days | New trait with default impls (including neighbor streaming) |
| 2. InMemoryGraph impl | 1 day | True streaming for inmemory backend |
| 3. MmapGraph impl | 1 day | Cursor-based mmap iteration |
| 4. as_any() support | 0.5 days | Enable downcasting in trait |
| 5. StreamingExecutor update | 0.5 days | Use new streaming methods |
| 6. Navigation steps update | 0.5 days | Use `stream_*_neighbors` methods |
| 7. GraphSnapshot update | 0.5 days | Provide Arc<dyn StreamableStorage> |
| 8. Tests | 0.5 days | Verify streaming behavior |

**Total: ~5 days**

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
