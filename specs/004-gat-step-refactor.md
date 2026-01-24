# Spec: GAT-Based Step Trait Refactor

**Status:** Draft  
**Priority:** High (Long-term architectural fix)  
**Depends on:** Rust 1.65+ (GATs stabilized)  
**Related:** `perf-improvements/002-eager-collection-lazy-evaluation.md`

## Executive Summary

Refactor the `AnyStep` trait to use Generic Associated Types (GATs), enabling true lazy evaluation throughout the traversal pipeline. This eliminates the O(n) memory overhead at each step caused by eager collection.

## Problem Statement

The current `AnyStep` trait returns `Box<dyn Iterator<Item = Traverser> + 'a>`, which works for lazy evaluation within a single step but forces eager collection between steps due to lifetime constraints. The executor must call `.collect()` after each step to break the lifetime chain:

```rust
// Current: O(n) memory at each step
for step in &steps {
    current = step.apply(&ctx, Box::new(current.into_iter())).collect();
}
```

With GATs, we can express that each step's iterator lifetime is tied to the step itself, allowing the compiler to verify the full chain without intermediate collection.

## Goals

1. **True O(1) streaming**: Only one traverser in flight at a time for non-branching traversals
2. **Type-safe iterator chains**: Compile-time verification of lifetime correctness
3. **Backward compatibility**: Maintain existing public fluent API for users

## Non-Goals

- Changing the user-facing traversal API (`g.v().out().has_label(...)`)
- Modifying the `Value` or `Traverser` types
- Changing storage backend interfaces

---

## Current Architecture

### AnyStep Trait (Type-Erased)

```rust
// src/traversal/step.rs:55-85
pub trait AnyStep: Send + Sync {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a>;

    fn clone_box(&self) -> Box<dyn AnyStep>;
    fn name(&self) -> &'static str;
}
```

### Step Storage

```rust
// src/traversal/pipeline.rs:43-50
pub struct Traversal<In, Out> {
    pub(crate) steps: Vec<Box<dyn AnyStep>>,
    pub(crate) source: Option<TraversalSource>,
    pub(crate) _phantom: PhantomData<fn(In) -> Out>,
}
```

### Execution (Eager)

```rust
// src/traversal/source.rs:2874-2877
for step in &steps {
    current = step.apply(&ctx, Box::new(current.into_iter())).collect();
}
```

---

## Proposed Architecture

### Phase 1: GAT-Based Step Trait

#### New `Step` Trait with GAT

```rust
// src/traversal/step.rs (new)

/// A traversal step that transforms an input iterator into an output iterator.
/// 
/// The associated type `Iter<'a>` uses a GAT to express that the returned
/// iterator's lifetime is tied to both `self` and the input iterator.
pub trait Step: Send + Sync + Clone + 'static {
    /// The iterator type returned by this step.
    /// 
    /// The lifetime `'a` is tied to:
    /// - The step itself (`&'a self`)
    /// - The execution context (`&'a ExecutionContext`)
    /// - The input iterator (must live for `'a`)
    type Iter<'a>: Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    /// Apply this step to the input iterator, producing the output iterator.
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: impl Iterator<Item = Traverser> + 'a,
    ) -> Self::Iter<'a>;

    /// Step name for debugging and profiling.
    fn name(&self) -> &'static str;
}
```

#### Key Differences from Current Design

| Aspect | Current (`AnyStep`) | New (`Step` with GAT) |
|--------|---------------------|----------------------|
| Return type | `Box<dyn Iterator + 'a>` | `Self::Iter<'a>` (concrete) |
| Input type | `Box<dyn Iterator + 'a>` | `impl Iterator + 'a` |
| Clone | `clone_box()` -> `Box<dyn AnyStep>` | `Clone` trait bound |
| Type erasure | At trait level | Separate `DynStep` wrapper |
| Allocation | Box per step invocation | Zero-cost for concrete types |

### Phase 2: Type-Erased Wrapper for Dynamic Dispatch

For storage in `Vec<Box<dyn ...>>`, we need a type-erased wrapper:

```rust
/// Type-erased step trait for dynamic dispatch.
/// 
/// This is automatically implemented for all `Step` implementors via blanket impl.
/// Use this for storing heterogeneous steps in a `Traversal`.
pub trait DynStep: Send + Sync {
    fn apply_dyn<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a>;

    fn clone_box(&self) -> Box<dyn DynStep>;
    fn name(&self) -> &'static str;
}

// Blanket implementation: every Step is also a DynStep
impl<S: Step> DynStep for S {
    fn apply_dyn<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(self.apply(ctx, input))
    }

    fn clone_box(&self) -> Box<dyn DynStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        <Self as Step>::name(self)
    }
}

impl Clone for Box<dyn DynStep> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
```

### Phase 3: Lazy Executor

```rust
/// Lazy traversal executor that streams results without intermediate collection.
pub struct LazyExecutor<'g> {
    /// The iterator chain (type-erased for storage)
    iter: Box<dyn Iterator<Item = Traverser> + 'g>,
}

impl<'g> LazyExecutor<'g> {
    pub fn new<In, Out>(
        ctx: &'g ExecutionContext<'g>,
        steps: &'g [Box<dyn DynStep>],
        start: impl Iterator<Item = Traverser> + 'g,
    ) -> Self {
        // Build iterator chain lazily - no collection!
        let iter = steps.iter().fold(
            Box::new(start) as Box<dyn Iterator<Item = Traverser> + 'g>,
            |acc, step| step.apply_dyn(ctx, acc),
        );

        Self { iter }
    }
}

impl Iterator for LazyExecutor<'_> {
    type Item = Traverser;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
```

---

## Implementation Plan

### Chunk 1: Core Trait Definitions
**Files:** `src/traversal/step.rs`  
**Effort:** 1 day

1. Replace `AnyStep` trait with new `Step` trait (GAT-based)
2. Add `DynStep` trait with blanket implementation
3. Update all internal references from `AnyStep` to `DynStep`

### Chunk 2: Update Helper Macros
**Files:** `src/traversal/step.rs`  
**Effort:** 0.5 days

Update `impl_filter_step!` and `impl_flatmap_step!` macros to generate `Step` implementations:

```rust
#[macro_export]
macro_rules! impl_filter_step {
    ($step:ty, $name:literal) => {
        impl $crate::traversal::Step for $step {
            type Iter<'a> = impl Iterator<Item = $crate::traversal::Traverser> + 'a
            where
                Self: 'a;

            fn apply<'a>(
                &'a self,
                ctx: &'a $crate::traversal::ExecutionContext<'a>,
                input: impl Iterator<Item = $crate::traversal::Traverser> + 'a,
            ) -> Self::Iter<'a> {
                let step = self.clone();
                input.filter(move |t| step.matches(ctx, t))
            }

            fn name(&self) -> &'static str {
                $name
            }
        }
    };
}
```

**Note:** This requires `#![feature(impl_trait_in_assoc_type)]` or explicit iterator types. See [Alternatives](#alternative-explicit-iterator-types) for stable Rust approach.

### Chunk 3: Migrate Filter Steps
**Files:** `src/traversal/filter.rs`  
**Effort:** 1 day

Migrate all filter steps to new trait:
- `HasLabelStep`
- `HasStep`
- `HasIdStep`
- `WhereStep`
- `IsStep`
- `AndStep`
- `OrStep`
- `NotStep`
- `DedupStep`
- `LimitStep`
- `TailStep`
- `RangeStep`
- `CoinStep`
- `SampleStep`

### Chunk 4: Migrate Navigation Steps
**Files:** `src/traversal/navigation.rs`  
**Effort:** 1.5 days

Migrate navigation steps (1:N expansion):
- `OutStep`
- `InStep`
- `BothStep`
- `OutEStep`
- `InEStep`
- `BothEStep`
- `OutVStep`
- `InVStep`
- `BothVStep`
- `OtherVStep`

### Chunk 5: Migrate Transform Steps
**Files:** `src/traversal/transform.rs`  
**Effort:** 1 day

Migrate transform steps:
- `IdStep`
- `LabelStep`
- `PropertiesStep`
- `PropertyMapStep`
- `ValuesStep`
- `ValueMapStep`
- `PathStep`
- `SelectStep`
- `UnfoldStep`
- `FoldStep`
- `GroupStep`
- `GroupCountStep`
- `OrderStep`
- `ProjectStep`

### Chunk 6: Migrate Branch Steps
**Files:** `src/traversal/branch.rs`  
**Effort:** 1.5 days

Migrate branching steps (most complex):
- `UnionStep`
- `CoalesceStep`
- `ChooseStep`
- `RepeatStep`
- `OptionalStep`
- `LocalStep`
- `BranchStep`

These require careful handling as they compose multiple sub-traversals.

### Chunk 7: Update Pipeline and Executor
**Files:** `src/traversal/pipeline.rs`, `src/traversal/source.rs`  
**Effort:** 1 day

1. Update `Traversal` to use `Box<dyn DynStep>`
2. Replace eager executor with `LazyExecutor`
3. Add `iter()` method to `BoundTraversal`
4. Implement `to_list()` as `self.iter().collect()`

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    /// Execute lazily, streaming results one at a time.
    pub fn iter(self) -> LazyExecutor<'g> {
        LazyExecutor::new(...)
    }

    /// Execute and collect all results.
    pub fn to_list(self) -> Vec<Traverser> {
        self.iter().collect()
    }
}
```

### Chunk 8: Update Terminal Steps
**Files:** `src/traversal/source.rs`  
**Effort:** 0.5 days

Update terminal methods to use lazy execution internally:

```rust
impl<'g, In, Out> BoundTraversal<'g, In, Out> {
    pub fn count(self) -> u64 {
        self.iter().map(|t| t.bulk).sum()
    }

    pub fn has_next(self) -> bool {
        self.iter().next().is_some()
    }

    pub fn next(self) -> Option<Traverser> {
        self.iter().next()
    }
}
```

### Chunk 9: Testing and Benchmarks
**Files:** `tests/`, `benches/`  
**Effort:** 1 day

1. Update all existing tests to pass
2. Add memory usage benchmarks
3. Add streaming-specific tests
4. Verify no regressions in traversal results

### Chunk 10: Documentation and Cleanup
**Files:** Throughout  
**Effort:** 0.5 days

1. Update doc comments
2. Update `AGENTS.md` with new patterns

---

## Detailed Type Signatures

### Filter Step Example (HasLabelStep)

```rust
#[derive(Clone)]
pub struct HasLabelStep {
    labels: SmallVec<[String; 2]>,
}

pub struct HasLabelIter<'a, I> {
    inner: I,
    step: &'a HasLabelStep,
    ctx: &'a ExecutionContext<'a>,
    label_ids: SmallVec<[u32; 2]>,
}

impl<'a, I: Iterator<Item = Traverser>> Iterator for HasLabelIter<'a, I> {
    type Item = Traverser;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.find(|t| {
            // Check if traverser's label matches any of our labels
            t.as_element()
                .and_then(|e| e.label_id())
                .map(|id| self.label_ids.contains(&id))
                .unwrap_or(false)
        })
    }
}

impl Step for HasLabelStep {
    type Iter<'a> = HasLabelIter<'a, Box<dyn Iterator<Item = Traverser> + 'a>>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: impl Iterator<Item = Traverser> + 'a,
    ) -> Self::Iter<'a> {
        let label_ids = self.labels
            .iter()
            .filter_map(|l| ctx.resolve_label(l))
            .collect();

        HasLabelIter {
            inner: Box::new(input),
            step: self,
            ctx,
            label_ids,
        }
    }

    fn name(&self) -> &'static str {
        "hasLabel"
    }
}
```

### Navigation Step Example (OutStep)

```rust
#[derive(Clone)]
pub struct OutStep {
    edge_labels: SmallVec<[String; 2]>,
}

pub struct OutIter<'a> {
    /// Current vertex's outgoing neighbors (flattened from edges)
    current_neighbors: Box<dyn Iterator<Item = Traverser> + 'a>,
    /// Input iterator of traversers
    input: Box<dyn Iterator<Item = Traverser> + 'a>,
    /// Resolved edge label IDs
    label_ids: SmallVec<[u32; 2]>,
    /// Execution context
    ctx: &'a ExecutionContext<'a>,
    /// Whether to track paths
    track_paths: bool,
}

impl<'a> Iterator for OutIter<'a> {
    type Item = Traverser;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get next neighbor from current expansion
            if let Some(neighbor) = self.current_neighbors.next() {
                return Some(neighbor);
            }

            // Get next input traverser and expand
            let t = self.input.next()?;
            let vertex_id = t.as_vertex_id()?;

            // Expand to neighbors
            self.current_neighbors = Box::new(
                self.ctx.storage()
                    .out_edges(vertex_id)
                    .filter(|e| {
                        self.label_ids.is_empty() || 
                        self.label_ids.contains(&e.label_id())
                    })
                    .map(move |e| {
                        if self.track_paths {
                            t.clone().advance_with_path(Value::vertex_id(e.in_v()))
                        } else {
                            t.clone().advance(Value::vertex_id(e.in_v()))
                        }
                    })
            );
        }
    }
}

impl Step for OutStep {
    type Iter<'a> = OutIter<'a> where Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: impl Iterator<Item = Traverser> + 'a,
    ) -> Self::Iter<'a> {
        let label_ids = self.edge_labels
            .iter()
            .filter_map(|l| ctx.resolve_label(l))
            .collect();

        OutIter {
            current_neighbors: Box::new(std::iter::empty()),
            input: Box::new(input),
            label_ids,
            ctx,
            track_paths: ctx.is_tracking_paths(),
        }
    }

    fn name(&self) -> &'static str {
        "out"
    }
}
```

---

## Migration Strategy

Since this project is early-stage with no external consumers of internal APIs, we can perform a direct replacement without deprecation scaffolding:

1. Replace `AnyStep` with `Step` + `DynStep` traits directly
2. Migrate all step implementations in a single pass
3. Replace eager executor with lazy executor
4. Run full test suite to verify correctness

**Total estimated effort: ~7.5 days** (reduced from ~10 days by eliminating parallel implementation overhead)

### Public API Compatibility

The user-facing API remains unchanged:

```rust
// Before and after - same API
let names: Vec<String> = g.v()
    .has_label("person")
    .out("knows")
    .values("name")
    .to_list()
    .into_iter()
    .filter_map(|t| t.into_string())
    .collect();

// New: explicit lazy iteration
let mut iter = g.v().has_label("person").iter();
while let Some(person) = iter.next() {
    println!("{:?}", person);
}
```

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Complex lifetime errors during migration | High | Medium | Extensive testing, start with simple filter steps |
| Performance regression from extra indirection | Low | Medium | Benchmark each step, profile hot paths |
| Increased compile times | Medium | Low | Monitor CI build times, consider trait object fallback |

---

## Success Criteria

1. **Memory**: Peak memory usage for `g.v().out().out().count()` on 100K vertex graph drops from O(n²) to O(1)
2. **Latency**: First result from `g.v().limit(1)` returns in O(1) time regardless of graph size
3. **Correctness**: All existing tests pass without modification
4. **API Stability**: No changes to user-facing fluent API

---

## Appendix: Rust Version Requirements

| Feature | Required Version | Status |
|---------|-----------------|--------|
| Generic Associated Types | Rust 1.65 | Stable |
| `impl Trait` in associated types | Rust 1.75 | Stable |
| `async` in traits (future) | Rust 1.75 | Stable |

Recommend targeting Rust 1.75+ for cleanest implementation with RPITIT (Return Position Impl Trait In Trait).

---

## References

- [Rust RFC 1598: GATs](https://rust-lang.github.io/rfcs/1598-generic_associated_types.html)
- [Rust 1.65 Release Notes](https://blog.rust-lang.org/2022/11/03/Rust-1.65.0.html)
- [Streaming Iterator Crate](https://docs.rs/streaming-iterator/) (alternative approach)
- Original issue: `perf-improvements/002-eager-collection-lazy-evaluation.md`
