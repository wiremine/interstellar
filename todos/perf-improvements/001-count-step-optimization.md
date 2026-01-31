# Performance Issue: count() Eagerly Materializes All Traversers

## Problem

The `count()` terminal step currently has O(n) memory complexity when it should be O(1).

**Location:** `src/traversal/source.rs:3006-3007`

```rust
pub fn count(self) -> u64 {
    self.execute().len() as u64
}
```

The `execute()` method in `TraversalExecutor::new()` eagerly collects ALL traversers into a `Vec`:

```rust
// src/traversal/source.rs:2874-2877
for step in &steps {
    current = step.apply(&ctx, Box::new(current.into_iter())).collect();
}
```

## Impact

For `g.v().count()` on a graph with 10,000 vertices:

1. Creates 10,000 `Traverser` objects (each containing `Value`, `Path`, sack, loops, etc.)
2. Allocates a `Vec<Traverser>` with 10,000 elements
3. Then just returns `vec.len()`

**Benchmark results:**
- `v().count()` on 10K vertices: ~2.0 ms
- Expected with streaming: ~0.1-0.2 ms (10-20x faster)

## Root Cause

The `TraversalExecutor` architecture collects after each step to avoid lifetime issues:

```rust
// "collecting after each to avoid lifetime issues"
for step in &steps {
    current = step.apply(&ctx, Box::new(current.into_iter())).collect();
}
```

This design choice trades performance for simpler lifetime management.

## Recommended Solutions

### Option 1: Add CountStep (Minimal Change)

Add a dedicated `CountStep` that counts without materializing:

```rust
pub struct CountStep;

impl AnyStep for CountStep {
    fn apply<'a>(
        &self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let count = input.count() as i64;
        Box::new(std::iter::once(Traverser::new(Value::Int(count))))
    }
    
    fn name(&self) -> &'static str { "count" }
}
```

Then modify `count()` to add this step and extract the result:

```rust
pub fn count(self) -> u64 {
    self.add_step(CountStep).next().unwrap().as_i64().unwrap() as u64
}
```

**Pros:** Simple, surgical change
**Cons:** Still collects intermediate steps; only optimizes final count

### Option 2: Lazy Executor with Fused Count (Medium Change)

Create a lazy execution path that detects `count()` as the terminal and uses iterator counting:

```rust
pub fn count(self) -> u64 {
    // Build a lazy iterator chain instead of collecting
    let iter = self.execute_lazy();
    iter.count() as u64
}
```

This requires a parallel `execute_lazy()` path that doesn't collect.

**Pros:** True streaming for count
**Cons:** More invasive change, needs careful lifetime handling

### Option 3: Step Fusion / Query Optimization (Large Change)

Implement a query optimizer that recognizes patterns like:
- `v().count()` → `storage.vertex_count()`
- `v().has_label("X").count()` → `storage.vertices_with_label("X").count()`

```rust
fn optimize_traversal(&self) -> OptimizedTraversal {
    if self.is_simple_vertex_count() {
        return OptimizedTraversal::VertexCount;
    }
    // ... more patterns
}
```

**Pros:** Dramatic speedups for common patterns
**Cons:** Significant complexity, maintenance burden

## Recommendation

**Start with Option 1 (CountStep)**, then consider Option 2 for a more complete solution.

Option 1 is a low-risk change that immediately fixes the `count()` terminal step. It can be implemented in ~50 lines of code and tested in isolation.

## Benchmarks to Add

```rust
// Compare current vs optimized count
fn bench_count_comparison(c: &mut Criterion) {
    let graph = create_benchmark_graph(100_000, 0);
    
    let mut group = c.benchmark_group("count_optimization");
    
    // Current: collects all traversers
    group.bench_function("current_count", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().count())
        })
    });
    
    // Direct storage call (optimal)
    group.bench_function("storage_vertex_count", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            black_box(snapshot.vertex_count())
        })
    });
    
    group.finish();
}
```

## Related Optimizations

Other terminal steps that could benefit from similar treatment:
- `sum()` - currently collects then sums
- `min()` / `max()` - currently collects then finds min/max
- `mean()` - currently collects then computes average
- `fold()` - could be streaming

## Priority

**High** - This affects every `count()` call and is a common operation in graph queries.
