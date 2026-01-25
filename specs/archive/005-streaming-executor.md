# Spec: StreamingExecutor for True Lazy Evaluation

**Status:** Draft  
**Priority:** High  
**Depends on:** `004-gat-step-refactor.md` (implemented), `006-streaming-storage.md` (for true edge streaming)  
**Related:** `code-reviews/traversal.md` (Section 1: Navigation Steps Collect to Vec)

## Executive Summary

Add true O(1) streaming execution by introducing `apply_streaming` to the `Step` trait. This method returns an owned iterator (`Box<dyn Iterator + Send + 'static>`) that captures all its data, eliminating lifetime constraints that force eager collection.

## Problem Statement

`TraversalExecutor` collects all results eagerly due to a lifetime constraint:

```rust
fn apply_dyn<'a>(
    &'a self,
    ctx: &'a ExecutionContext<'a>,
    input: Box<dyn Iterator<Item = Traverser> + 'a>,
) -> Box<dyn Iterator<Item = Traverser> + 'a>;  // Tied to 'a
```

The returned iterator borrows both `&self` and `&ctx`. When building a streaming pipeline, we can't return an iterator that outlives the function creating it.

## Goals

1. **True O(1) streaming**: Pull one traverser through entire pipeline at a time
2. **No collect() anywhere**: Purely pull-based evaluation
3. **Early termination**: `iter().take(1)` processes exactly one item per step
4. **Memory**: O(1) per step, regardless of total results or fan-out
5. **API compatibility**: Existing code unchanged

---

## Solution Overview

Add `apply_streaming` as a **required** method on `Step` that:
1. Takes a `StreamingContext` (Arc-wrapped storage/interner)
2. Takes a single input `Traverser` 
3. Returns `Box<dyn Iterator<Item = Traverser> + Send + 'static>`

The returned iterator **owns all its data** via cloning/Arc, enabling true streaming.

```
BoundTraversal::iter()
    |
    v
StreamingExecutor
    |
    +-- holds Arc<dyn GraphStorage>
    +-- holds Arc<StringInterner>
    +-- holds SideEffects (Arc internally)
    |
    +-- owns Box<dyn Iterator + 'static>
            |
            v
        StreamingAdapter [step N]
            +-- owns cloned step
            +-- owns StreamingContext (Arc refs)
            +-- owns input iterator
            +-- owns Option<current output iterator>
            |
            v
        StreamingAdapter [step N-1]
            ...
            |
            v
        Source Iterator ('static, owns data)
```

---

## Detailed Design

### Chunk 1: StreamingContext

**Files:** `src/traversal/context.rs`  
**Effort:** 0.5 days

An owned context that can be cloned into iterators.

```rust
use std::sync::Arc;

/// Owned execution context for streaming pipelines.
/// 
/// Unlike `ExecutionContext` which borrows storage/interner,
/// `StreamingContext` owns Arc references that can be cloned
/// into iterator closures for `'static` lifetimes.
#[derive(Clone)]
pub struct StreamingContext {
    /// Graph storage (shared ownership)
    pub storage: Arc<dyn GraphStorage + Send + Sync>,
    /// String interner (shared ownership)
    pub interner: Arc<StringInterner>,
    /// Side effects (already Arc-wrapped internally)
    pub side_effects: SideEffects,
    /// Whether to track traversal paths
    pub track_paths: bool,
}

impl StreamingContext {
    pub fn new(
        storage: Arc<dyn GraphStorage + Send + Sync>,
        interner: Arc<StringInterner>,
    ) -> Self {
        Self {
            storage,
            interner,
            side_effects: SideEffects::new(),
            track_paths: false,
        }
    }
    
    pub fn with_path_tracking(mut self, enabled: bool) -> Self {
        self.track_paths = enabled;
        self
    }
    
    pub fn with_side_effects(mut self, side_effects: SideEffects) -> Self {
        self.side_effects = side_effects;
        self
    }
    
    /// Resolve a label string to its interned ID.
    #[inline]
    pub fn resolve_label(&self, label: &str) -> Option<u32> {
        self.interner.lookup(label)
    }
    
    /// Get the storage reference.
    #[inline]
    pub fn storage(&self) -> &dyn GraphStorage {
        &*self.storage
    }
}
```

### SideEffects Update

Ensure `SideEffects` is cheaply cloneable (already uses internal Arc):

```rust
#[derive(Clone, Default)]
pub struct SideEffects {
    inner: Arc<RwLock<SideEffectsInner>>,
}
```

---

### Chunk 2: Step Trait Extension

**Files:** `src/traversal/step.rs`  
**Effort:** 1 day

Add `apply_streaming` as a **required** method on `Step`:

```rust
pub trait Step: Clone + Send + Sync + 'static {
    type Iter<'a>: Iterator<Item = Traverser> + 'a where Self: 'a;
    
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a>;
    
    /// Apply step to single input, returning owned iterator.
    /// 
    /// The returned iterator must own all its data (via cloning/Arc)
    /// to enable `'static` lifetime for streaming pipelines.
    fn apply_streaming(
        &self,
        ctx: StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static>;
    
    fn name(&self) -> &'static str;
}
```

All steps must implement `apply_streaming`. This ensures true streaming from the start with no fallback collection.

---

### Chunk 3: DynStep Extension

**Files:** `src/traversal/step.rs`  
**Effort:** 0.5 days

Add `apply_streaming` to `DynStep` with blanket impl:

```rust
pub trait DynStep: Send + Sync {
    /// Apply step with borrowed context (existing).
    fn apply_dyn<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a>;
    
    /// Apply step to single traverser, returning owned iterator.
    fn apply_streaming(
        &self,
        ctx: StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static>;
    
    fn clone_box(&self) -> Box<dyn DynStep>;
    fn dyn_name(&self) -> &'static str;
}

impl<S: Step> DynStep for S {
    fn apply_dyn<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(self.apply(ctx, input))
    }
    
    fn apply_streaming(
        &self,
        ctx: StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        <Self as Step>::apply_streaming(self, ctx, input)
    }
    
    fn clone_box(&self) -> Box<dyn DynStep> {
        Box::new(self.clone())
    }
    
    fn dyn_name(&self) -> &'static str {
        <Self as Step>::name(self)
    }
}
```

---

### Chunk 4: StreamingAdapter

**Files:** `src/traversal/streaming.rs`  
**Effort:** 1 day

The iterator adapter that chains steps:

```rust
/// Adapter that streams one step's outputs lazily.
pub struct StreamingAdapter {
    /// Owned step
    step: Box<dyn DynStep>,
    /// Streaming context (cloneable)
    ctx: StreamingContext,
    /// Input iterator (previous adapter or source)
    input: Box<dyn Iterator<Item = Traverser> + Send>,
    /// Current output iterator from one input traverser
    current: Option<Box<dyn Iterator<Item = Traverser> + Send>>,
}

impl StreamingAdapter {
    pub fn new(
        step: Box<dyn DynStep>,
        ctx: StreamingContext,
        input: Box<dyn Iterator<Item = Traverser> + Send>,
    ) -> Self {
        Self {
            step,
            ctx,
            input,
            current: None,
        }
    }
}

impl Iterator for StreamingAdapter {
    type Item = Traverser;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // 1. Yield from current output
            if let Some(ref mut current) = self.current {
                if let Some(t) = current.next() {
                    return Some(t);
                }
                self.current = None;
            }
            
            // 2. Pull next input
            let input = self.input.next()?;
            
            // 3. Apply step (returns owned iterator)
            self.current = Some(self.step.apply_streaming(self.ctx.clone(), input));
        }
    }
}
```

Note: `StreamingAdapter` is automatically `Send` because all fields are `Send`:
- `step: Box<dyn DynStep>` - `DynStep: Send`
- `ctx: StreamingContext` - `Clone + Send`
- `input: Box<dyn Iterator + Send>`
- `current: Option<Box<dyn Iterator + Send>>`

---

### Chunk 5: StreamingExecutor

**Files:** `src/traversal/streaming.rs`  
**Effort:** 0.5 days

```rust
/// Executor that streams results with O(1) memory per step.
pub struct StreamingExecutor {
    iter: Box<dyn Iterator<Item = Traverser> + Send>,
    side_effects: SideEffects,
}

impl StreamingExecutor {
    pub fn new(
        storage: Arc<dyn GraphStorage + Send + Sync>,
        interner: Arc<StringInterner>,
        steps: Vec<Box<dyn DynStep>>,
        source: Option<TraversalSource>,
        track_paths: bool,
    ) -> Self {
        let side_effects = SideEffects::new();
        let ctx = StreamingContext::new(storage.clone(), interner.clone())
            .with_side_effects(side_effects.clone())
            .with_path_tracking(track_paths);
        
        // Build source iterator
        let source_iter = Self::build_source(storage, source, track_paths);
        
        // Chain adapters
        let iter = steps.into_iter().fold(
            source_iter,
            |input, step| {
                Box::new(StreamingAdapter::new(step, ctx.clone(), input))
                    as Box<dyn Iterator<Item = Traverser> + Send>
            },
        );
        
        Self { iter, side_effects }
    }
    
    fn build_source(
        storage: Arc<dyn GraphStorage + Send + Sync>,
        source: Option<TraversalSource>,
        track_paths: bool,
    ) -> Box<dyn Iterator<Item = Traverser> + Send> {
        match source {
            Some(TraversalSource::AllVertices) => {
                // Need to collect vertex IDs to own them
                let ids: Vec<_> = storage.all_vertices().map(|v| v.id).collect();
                Box::new(ids.into_iter().map(move |id| {
                    let mut t = Traverser::new(Value::VertexId(id));
                    if track_paths { t.init_path(); }
                    t
                }))
            }
            Some(TraversalSource::Vertices(ids)) => {
                let storage = storage.clone();
                Box::new(ids.into_iter().filter_map(move |id| {
                    storage.get_vertex(id).map(|_| {
                        let mut t = Traverser::new(Value::VertexId(id));
                        if track_paths { t.init_path(); }
                        t
                    })
                }))
            }
            // ... other sources similar
            None => Box::new(std::iter::empty()),
        }
    }
    
    pub fn side_effects(&self) -> &SideEffects {
        &self.side_effects
    }
}

impl Iterator for StreamingExecutor {
    type Item = Traverser;
    
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
```

---

### Chunk 6: GraphSnapshot with Arc Storage

**Files:** `src/graph.rs`  
**Effort:** 0.5 days

Store Arc internally in `GraphSnapshot` for zero-cost streaming access:

```rust
pub struct GraphSnapshot {
    storage: Arc<dyn GraphStorage + Send + Sync>,
    interner: Arc<StringInterner>,
}

impl GraphSnapshot {
    pub fn new(
        storage: Arc<dyn GraphStorage + Send + Sync>,
        interner: Arc<StringInterner>,
    ) -> Self {
        Self { storage, interner }
    }
    
    /// Get Arc-wrapped storage for streaming execution.
    #[inline]
    pub fn arc_storage(&self) -> Arc<dyn GraphStorage + Send + Sync> {
        Arc::clone(&self.storage)
    }
    
    /// Get Arc-wrapped interner for streaming execution.
    #[inline]
    pub fn arc_interner(&self) -> Arc<StringInterner> {
        Arc::clone(&self.interner)
    }
    
    /// Get storage reference for borrowed access.
    #[inline]
    pub fn storage(&self) -> &dyn GraphStorage {
        &*self.storage
    }
    
    /// Get interner reference for borrowed access.
    #[inline]
    pub fn interner(&self) -> &StringInterner {
        &self.interner
    }
}
```

---

### Chunk 7: BoundTraversal Integration

**Files:** `src/traversal/source.rs`  
**Effort:** 0.5 days

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Stream results lazily with O(1) memory per step.
    /// 
    /// # Example
    /// 
    /// ```ignore
    /// // Stops after finding first match
    /// let first = g.v().has_label("person").iter().next();
    /// 
    /// // Only processes ~10 items through pipeline
    /// let sample: Vec<_> = g.v().out("knows").iter().take(10).collect();
    /// ```
    pub fn iter(self) -> impl Iterator<Item = Value> + Send {
        self.streaming_execute().map(|t| t.value)
    }
    
    /// Stream traversers with metadata.
    pub fn traversers(self) -> impl Iterator<Item = Traverser> + Send {
        self.streaming_execute()
    }
    
    /// Create streaming executor.
    pub fn streaming_execute(self) -> StreamingExecutor {
        StreamingExecutor::new(
            self.snapshot.arc_storage(),
            self.snapshot.arc_interner(),
            self.traversal.steps,
            self.traversal.source,
            self.track_paths,
        )
    }
}
```

---

### Chunk 8: Implement apply_streaming for All Steps

**Files:** Various in `src/traversal/`  
**Effort:** 3-4 days

All steps must implement `apply_streaming`. Example for `OutStep`:

```rust
impl Step for OutStep {
    type Iter<'a> = OutIter<'a> where Self: 'a;
    
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        // ... existing impl (borrowed, for eager evaluation) ...
    }
    
    fn apply_streaming(
        &self,
        ctx: StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Pre-resolve label strings to IDs once
        let label_ids: Vec<u32> = self.labels.iter()
            .filter_map(|l| ctx.interner.lookup(l))
            .collect();
        
        Box::new(OutStreamingIter {
            label_ids,
            storage: ctx.storage.clone(),
            track_paths: ctx.track_paths,
            input: Some(input),
            current_neighbors: None,
        })
    }
    
    fn name(&self) -> &'static str { "out" }
}

struct OutStreamingIter {
    label_ids: Vec<u32>,
    storage: Arc<dyn StreamableStorage>,
    track_paths: bool,
    input: Option<Traverser>,
    current_neighbors: Option<Box<dyn Iterator<Item = VertexId> + Send>>,
}

impl Iterator for OutStreamingIter {
    type Item = Traverser;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Yield from current neighbors
            if let Some(ref mut neighbors) = self.current_neighbors {
                if let Some(target_id) = neighbors.next() {
                    let mut t = Traverser::new(Value::VertexId(target_id));
                    if self.track_paths {
                        // Copy path from input, extend
                    }
                    return Some(t);
                }
                self.current_neighbors = None;
            }
            
            // Get input vertex
            let input = self.input.take()?;
            let vertex_id = input.value.as_vertex_id()?;
            
            // Use stream_out_neighbors from StreamableStorage (006)
            // Returns owned iterator - true O(1) streaming!
            let neighbors = StreamableStorage::stream_out_neighbors(
                self.storage.clone(),
                vertex_id,
                &self.label_ids,
            );
            
            self.current_neighbors = Some(neighbors);
        }
    }
}
```

The `OutStep::apply_streaming` method pre-resolves labels to IDs:

```rust
fn apply_streaming(
    &self,
    ctx: StreamingContext,
    input: Traverser,
) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
    // Pre-resolve label strings to IDs once
    let label_ids: Vec<u32> = self.labels.iter()
        .filter_map(|l| ctx.interner.lookup(l))
        .collect();
    
    Box::new(OutStreamingIter {
        label_ids,
        storage: ctx.storage.clone(),
        track_paths: ctx.track_paths,
        input: Some(input),
        current_neighbors: None,
    })
}
```

**Memory model with 006 integration:**
- Without 006: O(max_degree) per navigation step (collect edges to Vec)
- With 006: O(1) per navigation step (stream neighbors lazily)

### Steps to Implement

| Category | Steps |
|----------|-------|
| Navigation | `OutStep`, `InStep`, `BothStep`, `OutEStep`, `InEStep`, `BothEStep`, `OutVStep`, `InVStep`, `BothVStep` |
| Filters | `HasLabelStep`, `HasStep`, `HasValueStep`, `HasIdStep`, `WhereStep`, `IsStep`, `NotStep`, `AndStep`, `OrStep`, `DedupStep`, `RangeStep`, `LimitStep`, `TailStep`, `CoinStep`, `SampleStep` |
| Transforms | `IdStep`, `LabelStep`, `ValuesStep`, `PropertiesStep`, `PropertyMapStep`, `ValueMapStep`, `ElementMapStep`, `SelectStep`, `ProjectStep`, `UnfoldStep`, `FoldStep`, `CountStep`, `SumStep`, `MinStep`, `MaxStep`, `MeanStep`, `PathStep`, `ConstantStep`, `IdentityStep` |
| Side Effects | `StoreStep`, `AggregateStep`, `GroupStep`, `GroupCountStep` |
| Branch | `UnionStep`, `CoalesceStep`, `ChooseStep`, `OptionalStep`, `RepeatStep`, `EmitStep`, `UntilStep`, `LocalStep` |
| Barrier | `OrderStep`, `BarrierStep` |

---

## Implementation Plan

| Chunk | Effort | Description |
|-------|--------|-------------|
| 1. StreamingContext | 0.5 days | Arc-wrapped owned context |
| 2. Step trait extension | 1 day | Add required apply_streaming |
| 3. DynStep extension | 0.5 days | Blanket impl for apply_streaming |
| 4. StreamingAdapter | 1 day | Core streaming iterator |
| 5. StreamingExecutor | 0.5 days | Build adapter chain |
| 6. GraphSnapshot refactor | 0.5 days | Arc storage internally |
| 7. BoundTraversal integration | 0.5 days | iter() method |
| 8. Implement all steps | 3-4 days | apply_streaming for every step |
| 9. Tests | 1 day | Correctness + streaming verification |

**Total: ~9-10 days**

---

## Memory Analysis

| Scenario | Memory Model |
|----------|--------------|
| Current (`to_list()`) | O(total_results) - all at once |
| StreamingExecutor | O(steps + max_degree) constant |

For `g.v().out().out().out().iter().take(10)` on a graph with 1M vertices:

| Approach | Memory |
|----------|--------|
| Current | ~1M * degree^3 traversers |
| Streaming | ~10 traversers (only what's yielded) + O(max_degree) per step |

---

## Success Criteria

| Criterion | Verification |
|-----------|--------------|
| Correctness | `iter().collect() == to_list()` |
| Streaming | `iter().next()` returns without full scan |
| Early termination | `iter().take(10)` processes O(10) items |
| Memory | O(steps + max_degree) constant |

---

## Testing

### Correctness Tests

```rust
#[test]
fn streaming_matches_eager() {
    let graph = test_graph();
    let g = graph.traversal();
    
    let eager: Vec<_> = g.v().out("knows").out("created").to_list();
    let streaming: Vec<_> = g.v().out("knows").out("created").iter().collect();
    
    assert_eq!(eager, streaming);
}
```

### Streaming Verification Test

Use a counting storage wrapper to verify only required items are fetched:

```rust
/// Storage wrapper that counts method calls for testing.
struct CountingStorage {
    inner: Arc<dyn GraphStorage + Send + Sync>,
    vertex_fetch_count: AtomicUsize,
    edge_fetch_count: AtomicUsize,
}

impl CountingStorage {
    fn new(inner: Arc<dyn GraphStorage + Send + Sync>) -> Self {
        Self {
            inner,
            vertex_fetch_count: AtomicUsize::new(0),
            edge_fetch_count: AtomicUsize::new(0),
        }
    }
    
    fn vertex_fetches(&self) -> usize {
        self.vertex_fetch_count.load(Ordering::SeqCst)
    }
    
    fn edge_fetches(&self) -> usize {
        self.edge_fetch_count.load(Ordering::SeqCst)
    }
    
    fn reset_counts(&self) {
        self.vertex_fetch_count.store(0, Ordering::SeqCst);
        self.edge_fetch_count.store(0, Ordering::SeqCst);
    }
}

impl GraphStorage for CountingStorage {
    fn get_vertex(&self, id: VertexId) -> Option<Vertex> {
        self.vertex_fetch_count.fetch_add(1, Ordering::SeqCst);
        self.inner.get_vertex(id)
    }
    
    fn out_edges(&self, id: VertexId) -> Box<dyn Iterator<Item = Edge> + '_> {
        self.edge_fetch_count.fetch_add(1, Ordering::SeqCst);
        self.inner.out_edges(id)
    }
    
    // ... delegate other methods ...
}

#[test]
fn streaming_early_termination() {
    // Build a graph with 1000 vertices, each with 10 outgoing edges
    let inner = build_large_test_graph(1000, 10);
    let counting = Arc::new(CountingStorage::new(inner));
    let graph = Graph::with_storage(counting.clone());
    let g = graph.traversal();
    
    // Take only 5 results from a traversal that could return 10,000
    let results: Vec<_> = g.v().out("edge").iter().take(5).collect();
    
    assert_eq!(results.len(), 5);
    
    // Should have fetched far fewer than all vertices/edges
    // Exact count depends on graph structure, but should be O(5), not O(1000)
    let fetches = counting.vertex_fetches() + counting.edge_fetches();
    assert!(fetches < 100, "Expected O(n) fetches for take(n), got {}", fetches);
}

#[test]
fn streaming_single_item() {
    let inner = build_large_test_graph(1000, 10);
    let counting = Arc::new(CountingStorage::new(inner));
    let graph = Graph::with_storage(counting.clone());
    let g = graph.traversal();
    
    // Get just the first result
    let first = g.v().out("edge").out("edge").iter().next();
    
    assert!(first.is_some());
    
    // Should fetch minimal data - roughly 1 vertex + 1 edge lookup per step
    let fetches = counting.vertex_fetches() + counting.edge_fetches();
    assert!(fetches < 20, "Expected minimal fetches for next(), got {}", fetches);
}
```

---

## Alternatives Considered

### 1. Incremental migration with fallback collection
Default `apply_streaming` collects, steps override for true streaming.

**Rejected:** Adds complexity, delays full streaming benefits, and the fallback still has poor memory characteristics. Better to implement streaming for all steps upfront.

### 2. Modify Step trait to require owned iterators
Change `Iter<'a>` to `Iter: Iterator + Send + 'static`.

**Rejected:** Breaking change to all step implementations and loses ability to have zero-allocation borrowed iterators for eager evaluation.

### 3. Use generators (unstable)
Wait for `gen` blocks.

**Rejected:** Unknown timeline, need solution now.

### 4. Use ouroboros for self-referential structs
**Rejected:** Added complexity, dependency. The `apply_streaming` approach is cleaner.
