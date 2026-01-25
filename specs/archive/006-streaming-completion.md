# Spec 006: Streaming Executor Completion

## Status: Draft

## Summary

Complete the streaming execution implementation for remaining Transform and Side Effect steps that can feasibly support O(1) memory streaming.

## Background

The streaming executor (spec 005) is ~77% complete for transform steps and ~20% for side effect steps. This spec addresses the remaining steps that **can** be implemented with true streaming semantics.

### Steps Excluded from This Spec

The following steps are **intentionally not streamable** due to their semantics:

| Step | Category | Reason |
|------|----------|--------|
| `OrderStep` | Transform | Barrier - must sort all inputs before producing output |
| `MeanStep` | Transform | Barrier - must sum all values and count them |
| `GroupStep` | Aggregate | Barrier - must collect all inputs to group |
| `GroupCountStep` | Aggregate | Barrier - must count all inputs |
| `TailStep` | Filter | Barrier - must see all inputs to find last N |
| `SampleStep` | Filter | Barrier - requires reservoir sampling |
| `MapStep` | Transform | Closure requires `ExecutionContext` |
| `FlatMapStep` | Transform | Closure requires `ExecutionContext` |
| `FilterStep` | Filter | Closure requires `ExecutionContext` |
| `RepeatStep` | Repeat | Complex BFS iteration model |
| Mutation steps | Mutation | Require graph write access |

## Implementation Plan

### Phase 1: Transform Steps

#### 1.1 IndexStep (Medium Priority)

**Current State**: Pass-through stub  
**File**: `src/traversal/transform/metadata.rs:525-535`

**Problem**: Needs sequential counter to assign indices 0, 1, 2, ...

**Solution**: Use `Arc<AtomicUsize>` shared across the streaming pipeline.

```rust
fn apply_streaming(
    &self,
    _ctx: crate::traversal::context::StreamingContext,
    input: Traverser,
) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
    // IndexStep needs a shared counter across all traversers in the pipeline.
    // This requires adding an Arc<AtomicUsize> field to IndexStep.
    let index = self.counter.fetch_add(1, Ordering::SeqCst);
    let indexed_value = Value::List(vec![
        input.value.clone(),
        Value::Int(index as i64),
    ]);
    Box::new(std::iter::once(input.with_value(indexed_value)))
}
```

**Changes Required**:
1. Add `counter: Arc<AtomicUsize>` field to `IndexStep`
2. Initialize counter to 0 in constructor
3. Implement `apply_streaming` using atomic increment

**Complexity**: Low  
**Risk**: Low - atomic operations are well-understood

---

### Phase 2: Side Effect Steps

#### 2.1 StoreStep (Medium Priority)

**Current State**: Pass-through stub  
**File**: `src/traversal/sideeffect.rs`

**Problem**: Needs to store values in side effects collection.

**Solution**: Use `StreamingContext.side_effects()` which is already `Arc<SideEffects>`.

```rust
fn apply_streaming(
    &self,
    ctx: crate::traversal::context::StreamingContext,
    input: Traverser,
) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
    // Store current value in side effects (lazy - stores as traverser passes)
    ctx.side_effects().store(&self.key, input.value.clone());
    Box::new(std::iter::once(input))
}
```

**Changes Required**:
1. Implement `apply_streaming` using `ctx.side_effects().store()`

**Complexity**: Low  
**Risk**: Low - side effects already support concurrent access

---

#### 2.2 AggregateStep (Medium Priority)

**Current State**: Pass-through stub  
**File**: `src/traversal/sideeffect.rs`

**Problem**: Similar to StoreStep but uses eager aggregation semantics.

**Note**: In Gremlin, `aggregate()` is a barrier that collects all values before continuing. However, for streaming we can implement it as lazy (same as `store()`), since the barrier behavior is only observable at `cap()` time.

```rust
fn apply_streaming(
    &self,
    ctx: crate::traversal::context::StreamingContext,
    input: Traverser,
) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
    // Aggregate current value (lazy streaming version)
    ctx.side_effects().store(&self.key, input.value.clone());
    Box::new(std::iter::once(input))
}
```

**Complexity**: Low  
**Risk**: Low - semantic difference from Gremlin's eager aggregate is acceptable

---

#### 2.3 SideEffectStep (Low Priority)

**Current State**: Pass-through stub  
**File**: `src/traversal/sideeffect.rs`

**Problem**: Executes a sub-traversal for side effects only.

**Solution**: Execute sub-traversal using `execute_traversal_streaming`, discard results.

```rust
fn apply_streaming(
    &self,
    ctx: crate::traversal::context::StreamingContext,
    input: Traverser,
) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
    use crate::traversal::step::execute_traversal_streaming;
    
    // Execute sub-traversal for side effects, consume all results
    let _ = execute_traversal_streaming(&ctx, &self.sub, input.clone()).count();
    
    // Pass through original input
    Box::new(std::iter::once(input))
}
```

**Complexity**: Low  
**Risk**: Low

---

#### 2.4 ProfileStep (Low Priority)

**Current State**: Pass-through stub  
**File**: `src/traversal/sideeffect.rs`

**Problem**: Needs to record timing/count metrics.

**Solution**: Use atomic counters and `Instant::now()` for timing.

```rust
fn apply_streaming(
    &self,
    ctx: crate::traversal::context::StreamingContext,
    input: Traverser,
) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
    // Increment traverser count
    self.count.fetch_add(1, Ordering::Relaxed);
    
    // Record timing if enabled
    if let Some(ref start) = self.start_time {
        // Update elapsed time atomically
    }
    
    Box::new(std::iter::once(input))
}
```

**Changes Required**:
1. Add atomic counters to `ProfileStep`
2. Add timing support with `std::time::Instant`

**Complexity**: Medium  
**Risk**: Low

---

### Phase 3: Filter Steps (Optional)

These steps could stream but require shared mutable state:

#### 3.1 LimitStep / SkipStep / RangeStep

**Problem**: Need counters to track position in stream.

**Solution**: Use `Arc<AtomicUsize>` for position tracking.

```rust
// LimitStep
fn apply_streaming(
    &self,
    _ctx: crate::traversal::context::StreamingContext,
    input: Traverser,
) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
    let count = self.seen.fetch_add(1, Ordering::SeqCst);
    if count < self.limit {
        Box::new(std::iter::once(input))
    } else {
        Box::new(std::iter::empty())
    }
}
```

**Note**: This doesn't support early termination signaling - the pipeline will continue processing even after limit is reached. True early termination requires iterator protocol changes.

---

#### 3.2 DedupStep / DedupByKeyStep / DedupByLabelStep

**Problem**: Need shared `HashSet` to track seen values.

**Solution**: Use `Arc<RwLock<HashSet<Value>>>` or `dashmap::DashSet`.

```rust
fn apply_streaming(
    &self,
    _ctx: crate::traversal::context::StreamingContext,
    input: Traverser,
) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
    let key = input.value.clone(); // or extract key based on step type
    
    // Try to insert, returns false if already present
    if self.seen.write().insert(key) {
        Box::new(std::iter::once(input))
    } else {
        Box::new(std::iter::empty())
    }
}
```

**Complexity**: Medium  
**Risk**: Medium - lock contention in parallel scenarios

---

## Implementation Order

| Priority | Step | Effort | Impact |
|----------|------|--------|--------|
| 1 | StoreStep | Low | High - enables side effect collection |
| 2 | AggregateStep | Low | High - common pattern |
| 3 | SideEffectStep | Low | Medium - sub-traversal execution |
| 4 | IndexStep | Low | Medium - useful for result ordering |
| 5 | ProfileStep | Medium | Low - debugging only |
| 6 | LimitStep | Medium | Medium - requires state |
| 7 | DedupStep variants | Medium | Medium - requires shared set |

## Testing Strategy

Each implemented step should have:

1. **Unit test**: Verify streaming output matches eager execution
2. **Integration test**: Test in multi-step pipeline
3. **Concurrency test**: Verify thread-safety of shared state

Example test pattern:

```rust
#[test]
fn streaming_store_matches_eager() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();
    
    // Eager execution
    let eager_result = g.v().store("x").cap("x").to_list();
    
    // Streaming execution
    let streaming_result: Vec<_> = g.v().store("x").cap("x").iter().collect();
    
    assert_eq!(eager_result, streaming_result);
}
```

## Success Criteria

1. All Phase 1-2 steps implemented with true streaming
2. All existing tests continue to pass
3. New streaming-specific tests for each step
4. Documentation updated to indicate streaming support

## Future Considerations

### MapStep / FlatMapStep Streaming

These could be made streamable by introducing a new closure signature:

```rust
// Current (cannot stream)
Fn(&ExecutionContext, &Value) -> Value

// Streaming-compatible (future)
Fn(&StreamingContext, &Value) -> Value
```

This would require API additions but not breaking changes.

### Early Termination

True streaming `limit()` requires the ability to signal "stop producing" upstream. This could be implemented via:

1. Return type that includes termination signal
2. Shared atomic "cancelled" flag
3. Iterator that checks termination before each `next()`

This is out of scope for this spec but worth considering for future optimization.
