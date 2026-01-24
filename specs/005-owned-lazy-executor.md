# Spec: Owned LazyExecutor for True Streaming

**Status:** Draft  
**Priority:** High  
**Depends on:** `specs/004-gat-step-refactor.md` (completed)  
**Related:** GAT-based Step trait refactor

## Executive Summary

Refactor `LazyExecutor` to own traversal steps, enabling true streaming execution where results are produced on-demand without upfront collection. This completes the lazy evaluation work started in spec 004.

## Problem Statement

The GAT refactor (spec 004) achieved lazy iterator chaining between steps, but `TraversalExecutor` still collects all results upfront:

```rust
// Current: TraversalExecutor::new()
let lazy_iter = execute_traversal(&ctx, &steps, start);
let results: Vec<Traverser> = lazy_iter.collect();  // <-- Defeats laziness!
```

This means:
- `g.v().limit(1).next()` still iterates all vertices before returning one
- `count()` materializes all results just to count them
- Memory usage is O(n) even for queries that could stream in O(1)

## Goals

1. **True O(1) first-result latency**: `g.v().limit(1).next()` returns immediately
2. **Streaming terminal operations**: `count()`, `next()`, `has_next()` don't materialize
3. **Zero-copy execution**: Steps move into executor, no cloning required
4. **API compatibility**: No changes to user-facing fluent API signatures

## Non-Goals

- Reusable traversals (users can clone before executing if needed)
- Parallel execution (future work)
- Async streaming (future work)

---

## Current Architecture

### TraversalExecutor (Eager Collection)

```rust
pub struct TraversalExecutor<'g> {
    results: std::vec::IntoIter<Traverser>,  // Pre-collected!
    _phantom: PhantomData<&'g ()>,
}

impl<'g> TraversalExecutor<'g> {
    fn new<In, Out>(..., traversal: Traversal<In, Out>, ...) -> Self {
        // ... build lazy iterator ...
        let results: Vec<Traverser> = lazy_iter.collect();  // Eager!
        Self { results: results.into_iter(), _phantom: PhantomData }
    }
}
```

### Terminal Methods (Use Eager Executor)

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    pub fn to_list(self) -> Vec<Value> {
        self.execute().map(|t| t.value).collect()
    }
    
    pub fn count(self) -> u64 {
        self.execute().len() as u64  // Requires full materialization
    }
    
    pub fn next(self) -> Option<Value> {
        self.execute().next().map(|t| t.value)  // Collects all, returns one
    }
}
```

---

## Proposed Architecture

### Phase 1: Owned LazyExecutor

Replace `TraversalExecutor` with an owned `LazyExecutor` that holds steps and streams lazily:

```rust
/// Lazy traversal executor that owns steps and streams results on-demand.
///
/// Unlike the previous `TraversalExecutor`, this does not collect results
/// upfront. Each call to `next()` pulls one traverser through the pipeline.
pub struct LazyExecutor<'g> {
    /// Execution context (references graph storage and interner)
    ctx: ExecutionContext<'g>,
    
    /// Owned steps - moved from the traversal
    steps: Vec<Box<dyn DynStep>>,
    
    /// The lazy iterator chain - built once, consumed incrementally
    iter: Option<Box<dyn Iterator<Item = Traverser> + 'g>>,
    
    /// Tracks if we've started iteration (for deferred chain building)
    started: bool,
}
```

#### Key Design Decisions

1. **Steps are owned**: `Vec<Box<dyn DynStep>>` moves from `Traversal` into `LazyExecutor`
2. **Deferred chain building**: Iterator chain built on first `next()` call, not at construction
3. **No upfront collection**: Results stream through as `next()` is called

### Phase 2: Updated Constructor

```rust
impl<'g> LazyExecutor<'g> {
    /// Create a new lazy executor from a bound traversal.
    ///
    /// This consumes the traversal, taking ownership of its steps.
    /// No work is done until `next()` is called.
    pub fn new(
        storage: &'g dyn GraphStorage,
        interner: &'g StringInterner,
        traversal: Traversal<impl Any, impl Any>,
        track_paths: bool,
    ) -> Self {
        let ctx = if track_paths {
            ExecutionContext::with_path_tracking(storage, interner)
        } else {
            ExecutionContext::new(storage, interner)
        };
        
        let (source, steps) = traversal.into_steps();
        
        Self {
            ctx,
            steps,
            source,
            iter: None,
            started: false,
        }
    }
    
    /// Build the iterator chain on first access.
    fn ensure_started(&mut self) {
        if self.started {
            return;
        }
        self.started = true;
        
        // Build source iterator
        let start = self.build_source_iter();
        
        // Chain through all steps lazily
        // Note: self.steps borrowed here, lifetime tied to self
        let iter = execute_traversal(&self.ctx, &self.steps, start);
        self.iter = Some(iter);
    }
}
```

### Phase 3: Iterator Implementation

```rust
impl<'g> Iterator for LazyExecutor<'g> {
    type Item = Traverser;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.ensure_started();
        self.iter.as_mut().and_then(|it| it.next())
    }
    
    fn size_hint(&self) -> (usize, Option<usize>) {
        // Unknown size for lazy execution
        (0, None)
    }
}

// Not ExactSizeIterator - we don't know the size upfront!
impl<'g> std::iter::FusedIterator for LazyExecutor<'g> {}
```

### Phase 4: Updated BoundTraversal Methods

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Execute lazily, returning a streaming iterator.
    ///
    /// Results are computed on-demand as the iterator is consumed.
    /// This is the primary execution method - all other terminal
    /// methods are implemented in terms of this.
    pub fn iter(self) -> LazyExecutor<'g> {
        LazyExecutor::new(
            self.storage,
            self.interner,
            self.traversal,
            self.track_paths,
        )
    }
    
    /// Execute and collect all results into a list.
    pub fn to_list(self) -> Vec<Value> {
        self.iter().map(|t| t.value).collect()
    }
    
    /// Execute and return the first result.
    ///
    /// Only evaluates the pipeline until one result is found.
    pub fn next(self) -> Option<Value> {
        self.iter().next().map(|t| t.value)
    }
    
    /// Check if traversal produces any results.
    ///
    /// Only evaluates until first result is found.
    pub fn has_next(self) -> bool {
        self.iter().next().is_some()
    }
    
    /// Count results by streaming through the pipeline.
    ///
    /// Memory usage is O(1) - results are counted but not stored.
    pub fn count(self) -> u64 {
        self.iter().map(|t| t.bulk).sum()
    }
    
    /// Consume traversal, discarding results.
    ///
    /// Useful for side-effect-only traversals.
    pub fn iterate(self) {
        for _ in self.iter() {}
    }
}
```

---

## Implementation Plan

### Chunk 1: LazyExecutor Restructure
**Files:** `src/traversal/step.rs`  
**Effort:** 0.5 days

1. Update `LazyExecutor` struct to own steps
2. Add `source: Option<TraversalSource>` field
3. Implement deferred initialization with `ensure_started()`
4. Update `Iterator` impl to use deferred init

### Chunk 2: Remove TraversalExecutor
**Files:** `src/traversal/source.rs`  
**Effort:** 0.5 days

1. Delete `TraversalExecutor` struct
2. Update `BoundTraversal::execute()` to return `LazyExecutor`
3. Or: rename `execute()` to `iter()` and remove `execute()`

### Chunk 3: Update Terminal Methods
**Files:** `src/traversal/source.rs`  
**Effort:** 0.5 days

1. Rewrite `to_list()` as `self.iter().map(|t| t.value).collect()`
2. Rewrite `next()` as `self.iter().next().map(|t| t.value)`
3. Rewrite `has_next()` as `self.iter().next().is_some()`
4. Rewrite `count()` as `self.iter().map(|t| t.bulk).sum()`
5. Remove `len()` and `is_empty()` from executor (unknown size)

### Chunk 4: Update Typed Traversal
**Files:** `src/traversal/typed.rs`  
**Effort:** 0.25 days

1. Update `TypedTraversal` terminal methods to use new executor
2. Ensure type-safe wrappers work with streaming

### Chunk 5: Fix Lifetime Challenges
**Files:** `src/traversal/step.rs`, `src/traversal/source.rs`  
**Effort:** 1 day

The main challenge: `LazyExecutor` owns `steps: Vec<Box<dyn DynStep>>` but the iterator borrows from them. This is a self-referential struct.

Options:
1. **Pin + unsafe**: Pin the steps, use unsafe to create self-referential borrow
2. **Ouroboros crate**: Use `ouroboros` for safe self-referential structs
3. **Arc the steps**: `Arc<[Box<dyn DynStep>]>` shared between executor and iterator
4. **Two-phase API**: Return a guard that borrows from owned steps

Recommended: **Option 3 (Arc)** for simplicity and safety:

```rust
pub struct LazyExecutor<'g> {
    ctx: ExecutionContext<'g>,
    steps: Arc<[Box<dyn DynStep>]>,  // Shared ownership
    iter: Option<Box<dyn Iterator<Item = Traverser> + 'g>>,
}
```

### Chunk 6: Testing
**Files:** `tests/`  
**Effort:** 0.5 days

1. Add test: `limit(1).next()` only processes one element
2. Add test: `count()` doesn't allocate O(n) memory
3. Add test: Side effects happen in streaming order
4. Verify all existing tests still pass

### Chunk 7: Benchmarks
**Files:** `benches/`  
**Effort:** 0.25 days

1. Benchmark: Time to first result for `g.v().limit(1).next()`
2. Benchmark: Memory usage for `g.v().out().out().count()` on large graph
3. Compare before/after for streaming vs materialized

---

## Detailed Design: Self-Referential Solution

The core challenge is that `LazyExecutor` needs to:
1. Own the steps (`Vec<Box<dyn DynStep>>`)
2. Hold an iterator that borrows from those steps

### Recommended: Arc-based Sharing

```rust
use std::sync::Arc;

pub struct LazyExecutor<'g> {
    /// Shared ownership of steps - iterator holds a reference
    steps: Arc<[Box<dyn DynStep>]>,
    
    /// Source for initial traversers
    source: Option<TraversalSource>,
    
    /// Execution context
    ctx: ExecutionContext<'g>,
    
    /// The lazy iterator chain
    /// 
    /// Safety: This borrows from `steps` which is Arc'd, so it remains
    /// valid as long as this struct exists. We use transmute to extend
    /// the lifetime, which is safe because we control the drop order.
    iter: Option<Box<dyn Iterator<Item = Traverser> + 'g>>,
    
    /// Whether iteration has started
    started: bool,
}

impl<'g> LazyExecutor<'g> {
    pub fn new(
        storage: &'g dyn GraphStorage,
        interner: &'g StringInterner,
        source: Option<TraversalSource>,
        steps: Vec<Box<dyn DynStep>>,
        track_paths: bool,
    ) -> Self {
        let ctx = if track_paths {
            ExecutionContext::with_path_tracking(storage, interner)
        } else {
            ExecutionContext::new(storage, interner)
        };
        
        Self {
            steps: Arc::from(steps),
            source,
            ctx,
            iter: None,
            started: false,
        }
    }
    
    fn ensure_started(&mut self) {
        if self.started {
            return;
        }
        self.started = true;
        
        let start = self.build_source_iter();
        
        // Build iterator chain
        // The steps are Arc'd so they outlive the iterator
        let steps_ref: &[Box<dyn DynStep>] = &self.steps;
        
        // Safety: steps_ref lives as long as self.steps (Arc), and we
        // ensure the iterator is dropped before steps by field order
        let iter = execute_traversal(&self.ctx, steps_ref, start);
        
        self.iter = Some(iter);
    }
}
```

### Alternative: Ouroboros Crate

```rust
use ouroboros::self_referencing;

#[self_referencing]
pub struct LazyExecutor<'g> {
    steps: Vec<Box<dyn DynStep>>,
    ctx: ExecutionContext<'g>,
    source: Option<TraversalSource>,
    
    #[borrows(steps, ctx)]
    #[covariant]
    iter: Option<Box<dyn Iterator<Item = Traverser> + 'this>>,
}
```

This is safer but adds a dependency.

---

## Migration Strategy

Since this is an internal refactor with no public API changes:

1. Implement new `LazyExecutor` alongside existing code
2. Update `BoundTraversal::iter()` to use new executor
3. Delete `TraversalExecutor`
4. Run full test suite

**Total estimated effort: ~3.5 days**

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Self-referential lifetime issues | High | High | Use Arc or ouroboros, extensive testing |
| Performance regression from Arc | Low | Low | Arc clone is cheap, benchmark to verify |
| Breaking side-effect ordering | Medium | Medium | Add specific tests for streaming order |
| Subtle behavioral changes | Medium | Medium | Comprehensive test coverage |

---

## Success Criteria

1. **Latency**: `g.v().limit(1).next()` on 1M vertex graph returns in <1ms
2. **Memory**: `g.v().out().out().count()` uses O(1) memory, not O(n)
3. **Correctness**: All existing tests pass
4. **Streaming**: `store()` before `limit()` only stores limited count

### Verification Tests

```rust
#[test]
fn test_limit_short_circuits() {
    let graph = create_graph_with_1000_vertices();
    let g = graph.snapshot().gremlin();
    
    // This should NOT iterate all 1000 vertices
    let counter = AtomicUsize::new(0);
    let result = g.v()
        .side_effect(|_| { counter.fetch_add(1, Ordering::SeqCst); })
        .limit(5)
        .to_list();
    
    assert_eq!(result.len(), 5);
    assert_eq!(counter.load(Ordering::SeqCst), 5);  // Only 5, not 1000!
}

#[test]
fn test_count_streams() {
    // Memory usage should be O(1), not O(n)
    let graph = create_large_graph();
    let g = graph.snapshot().gremlin();
    
    let count = g.v().out().out().count();
    // If this doesn't OOM on a large graph, streaming works
    assert!(count > 0);
}
```

---

## References

- `specs/004-gat-step-refactor.md` - Prerequisite GAT refactor
- [Ouroboros crate](https://docs.rs/ouroboros/) - Self-referential struct helper
- [Pin documentation](https://doc.rust-lang.org/std/pin/) - For unsafe self-referential approach
