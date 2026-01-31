# Performance Issue: Eager Collection Defeats Lazy Evaluation (CRITICAL)

## Problem

The `TraversalExecutor` eagerly collects traversers after **every step**, defeating the lazy iterator-based design and causing O(n) memory usage at each step in the pipeline.

**Location:** `src/traversal/source.rs:2874-2877`

```rust
// Apply each step in sequence, collecting after each to avoid lifetime issues
for step in &steps {
    current = step.apply(&ctx, Box::new(current.into_iter())).collect();
}
```

## Impact

For a traversal like `g.v().out().has_label("person").values("name")` on a graph with 10K vertices averaging 10 edges each:

| Step | Memory Allocated | Notes |
|------|-----------------|-------|
| `v()` | 10K Traversers | All vertices collected |
| `out()` | 100K Traversers | All neighbors collected |
| `has_label("person")` | ~50K Traversers | Filtered, but still collected |
| `values("name")` | ~50K Traversers | Property values collected |

**Total peak memory**: Proportional to the largest intermediate result (100K traversers)

**Expected with streaming**: O(1) memory - only one traverser in flight at a time

## Root Cause

The comment "collecting after each to avoid lifetime issues" reveals this is a deliberate trade-off. The challenge is that:

1. Steps return `Box<dyn Iterator<Item = Traverser> + 'a>` where `'a` is tied to the step
2. Building a chain of iterators where each borrows from the previous creates complex lifetime dependencies
3. The current design sidesteps this by collecting into owned `Vec<Traverser>` between steps

## Technical Analysis

### Current Architecture (Eager)

```rust
fn execute(traversal) -> Vec<Traverser> {
    let mut current: Vec<Traverser> = start_step.apply().collect();
    
    for step in steps {
        // Each step gets owned data, returns iterator, which is collected
        current = step.apply(Box::new(current.into_iter())).collect();
    }
    
    current
}
```

### Desired Architecture (Lazy)

```rust
fn execute_lazy(traversal) -> impl Iterator<Item = Traverser> {
    let iter = start_step.apply();
    
    // Build an iterator chain without collecting
    steps.fold(iter, |acc, step| step.apply(Box::new(acc)))
}
```

The problem: Rust's borrow checker struggles with this because each step's iterator may reference the step itself, creating a self-referential structure.

## Proposed Solutions

### Option A: Arena-Based Step Ownership

Store steps in an arena that outlives the execution:

```rust
pub struct LazyExecutor<'arena> {
    arena: &'arena StepArena,
    current: Box<dyn Iterator<Item = Traverser> + 'arena>,
}

impl<'arena> LazyExecutor<'arena> {
    pub fn new(arena: &'arena StepArena, traversal: Traversal) -> Self {
        // Steps are allocated in arena, iterators can reference them
        let steps = arena.alloc_steps(traversal.steps);
        let iter = steps.iter().fold(
            Box::new(start_iter) as Box<dyn Iterator<Item = Traverser> + 'arena>,
            |acc, step| step.apply(acc)
        );
        Self { arena, current: iter }
    }
}
```

**Pros**: True lazy evaluation, minimal API changes
**Cons**: Requires arena allocator, added complexity

### Option B: Streaming Step Trait

Introduce a separate trait for steps that can produce streaming iterators:

```rust
pub trait StreamingStep: AnyStep {
    fn apply_streaming<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: impl Iterator<Item = Traverser> + 'a,
    ) -> impl Iterator<Item = Traverser> + 'a;
}
```

**Pros**: Opt-in for steps that benefit most (navigation, filters)
**Cons**: Two execution paths to maintain, not all steps can implement

### Option C: GAT-Based Iterator Design

Use Generic Associated Types (stabilized in Rust 1.65) for proper lifetime handling:

```rust
pub trait Step {
    type Iter<'a>: Iterator<Item = Traverser> + 'a where Self: 'a;
    
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: impl Iterator<Item = Traverser> + 'a,
    ) -> Self::Iter<'a>;
}
```

**Pros**: Clean, idiomatic Rust, full type safety
**Cons**: Major refactor of step trait hierarchy, breaking changes

### Option D: Fold-Based Terminals (Pragmatic)

For terminal operations that reduce to a single value, add specialized execution:

```rust
impl<'g> BoundTraversal<'g, In, Out> {
    pub fn fold_lazy<A, F>(self, init: A, f: F) -> A
    where
        F: FnMut(A, Traverser) -> A,
    {
        // Execute lazily, folding as we go
        let ctx = self.create_context();
        let iter = self.build_lazy_iterator(&ctx);
        iter.fold(init, f)
    }
    
    pub fn count(self) -> u64 {
        self.fold_lazy(0u64, |acc, t| acc + t.bulk as u64)
    }
    
    pub fn sum(self) -> Option<f64> {
        self.fold_lazy(None, |acc, t| {
            let val = t.value.as_f64()?;
            Some(acc.unwrap_or(0.0) + val)
        })
    }
}
```

**Pros**: Targeted fix for common operations, less invasive
**Cons**: Doesn't fix non-terminal intermediate collection

## Recommendation

### Short-term (Implemented)

**CountStep** (see `001-count-step-optimization.md`): Add a step-based count that at least avoids the final `.len()` overhead and correctly handles bulk.

### Medium-term

**Option D (Fold-Based Terminals)**: Implement lazy execution paths for terminal operations (`count`, `sum`, `min`, `max`, `fold`). This provides significant wins for the most common reduction patterns without architectural upheaval.

### Long-term

**Option C (GAT-Based Design)**: Plan a major refactor to use GATs for the step trait hierarchy. This enables true lazy evaluation throughout and aligns with modern Rust idioms.

## Benchmarks to Track

```rust
fn bench_lazy_vs_eager(c: &mut Criterion) {
    let graph = create_benchmark_graph(100_000, 500_000); // 100K vertices, 500K edges
    
    let mut group = c.benchmark_group("lazy_evaluation");
    
    // Heavy intermediate results, simple terminal
    group.bench_function("out_out_count_current", |b| {
        b.iter(|| {
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();
            black_box(g.v().out().out().count())
        })
    });
    
    // Memory profiling would show peak allocation
    group.finish();
}
```

## Related Issues

- `001-count-step-optimization.md` - Immediate fix for count() terminal
- Future: `003-streaming-terminals.md` - Fold-based terminal operations
- Future: `004-gat-step-refactor.md` - Full lazy evaluation architecture

## Priority

**Critical** - This is the fundamental performance issue in the traversal engine. The current design works correctly but has O(n) memory complexity at each step, making it unsuitable for large graphs or deep traversals.
