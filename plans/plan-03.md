# Plan 03: Traversal Engine Core Implementation

**Phase 3 of RustGremlin Implementation**

Based on: `specs/spec-03-traversal-engine-core.md`

---

## Overview

This plan breaks down the Traversal Engine Core implementation into granular, testable s. Each Phase represents approximately 1-2 hours of focused work and includes specific acceptance criteria.

**Total Duration**: 4-5 weeks  
**Current State**: Existing stub types in `src/traversal/mod.rs` need to be replaced with the new architecture.

---

## Implementation Order

### Week 1: Prerequisites and Core Types

#### Phase 1.1: Extend Value Enum with Graph Element Variants
**File**: `src/value.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Add `Vertex(VertexId)` variant to `Value` enum
2. Add `Edge(EdgeId)` variant to `Value` enum
3. Add `Vertex(VertexId)` and `Edge(EdgeId)` variants to `ComparableValue`
4. Update `to_comparable()` method to handle new variants

**Code Changes**:
```rust
// In Value enum, add:
Vertex(VertexId),
Edge(EdgeId),

// In ComparableValue enum, add:
Vertex(VertexId),
Edge(EdgeId),
```

**Acceptance Criteria**:
- [x] `Value::Vertex(VertexId(1))` compiles and pattern matches
- [x] `Value::Edge(EdgeId(1))` compiles and pattern matches
- [x] `to_comparable()` handles new variants
- [x] Existing tests pass

---

#### Phase 1.2: Implement Hash for Value and OrderedFloat
**File**: `src/value.rs`  
**Duration**: 1-2 hours

**Status**: `Hash` for `OrderedFloat` completed in Phase 1.1 (required for `ComparableValue` to derive `Hash`). `Hash` for `Value` still needed.

**Tasks**:
1. ~~Implement `Hash` for `OrderedFloat` using `to_bits()`~~ (done in 1.1)
2. Implement `Hash` for `Value` (all variants including new ones)
3. Implement `Eq` for `Value` (required for Hash)

**Code Changes**:
```rust
impl Hash for OrderedFloat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int(n) => n.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::List(items) => items.hash(state),
            Value::Map(map) => {
                let mut entries: Vec<_> = map.iter().collect();
                entries.sort_by_key(|(k, _)| *k);
                for (k, v) in entries {
                    k.hash(state);
                    v.hash(state);
                }
            }
            Value::Vertex(id) => id.hash(state),
            Value::Edge(id) => id.hash(state),
        }
    }
}

impl Eq for Value {}
```

**Acceptance Criteria**:
- [x] `OrderedFloat` implements `Hash` (done in 1.1)
- [x] `Value` can be used as `HashMap` key
- [x] `Value` can be inserted into `HashSet`
- [x] Hash is consistent (same value = same hash)
- [x] Unit tests for hash consistency pass

---

#### Phase 1.3: Value Serialization for New Variants
**File**: `src/value.rs`  
**Duration**: 1 hour

**Status**: Completed in Phase 1.1

**Tasks**:
1. ~~Add serialization for `Value::Vertex` (tag 0x08)~~ (done in 1.1)
2. ~~Add serialization for `Value::Edge` (tag 0x09)~~ (done in 1.1)
3. ~~Add deserialization for both variants~~ (done in 1.1)
4. ~~Add roundtrip tests~~ (done in 1.1)

**Acceptance Criteria**:
- [x] `Value::Vertex` serializes to tag 0x08 + u64
- [x] `Value::Edge` serializes to tag 0x09 + u64
- [x] Roundtrip test passes for new variants
- [x] Existing serialization tests pass

---

#### Phase 1.4: Value From Implementations and Accessor Methods
**File**: `src/value.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Implement `From<VertexId>` for `Value`
2. Implement `From<EdgeId>` for `Value`
3. Add `as_vertex_id()` method
4. Add `as_edge_id()` method
5. Add `is_vertex()` and `is_edge()` methods

**Acceptance Criteria**:
- [x] `Value::from(VertexId(1))` returns `Value::Vertex(VertexId(1))`
- [x] `Value::Vertex(id).as_vertex_id()` returns `Some(id)`
- [x] `Value::Int(1).as_vertex_id()` returns `None`
- [x] Unit tests for all accessors pass

---

#### Phase 1.5: ExecutionContext and SideEffects
**File**: `src/traversal/context.rs` (new file)  
**Duration**: 2 hours

**Tasks**:
1. Create `src/traversal/context.rs`
2. Implement `ExecutionContext` struct with snapshot and interner references
3. Implement `SideEffects` struct with RwLock-based collections
4. Add label resolution methods to `ExecutionContext`
5. Add to `mod.rs` exports

**Code Structure**:
```rust
pub struct ExecutionContext<'g> {
    pub snapshot: &'g GraphSnapshot<'g>,
    pub interner: &'g StringInterner,
    pub side_effects: SideEffects,
}

pub struct SideEffects {
    collections: RwLock<HashMap<String, Vec<Value>>>,
    data: RwLock<HashMap<String, Box<dyn Any + Send + Sync>>>,
}
```

**Acceptance Criteria**:
- [x] `ExecutionContext::new()` compiles
- [x] `resolve_label()` returns Option<u32>
- [x] `SideEffects::store()` and `get()` work correctly
- [x] Unit tests pass

---

#### Phase 1.6: Traverser with Value-based Design
**File**: `src/traversal/mod.rs`  
**Duration**: 2 hours

**Tasks**:
1. Replace existing generic `Traverser<E>` with non-generic `Traverser`
2. Implement `Traverser::new()`, `from_vertex()`, `from_edge()`
3. Implement `split()` and `with_value()` methods
4. Implement `CloneSack` trait for sack values
5. Add accessor methods `as_vertex_id()`, `as_edge_id()`

**Acceptance Criteria**:
- [ ] `Traverser::new(Value::Int(1))` creates traverser
- [ ] `Traverser::from_vertex(VertexId(1))` works
- [ ] `split()` preserves path and metadata
- [ ] Clone works correctly

---

#### Phase 1.7: Path and PathElement Types
**File**: `src/traversal/mod.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Update `PathElement` to use `PathValue` instead of `Value`
2. Create `PathValue` enum (Vertex, Edge, Property variants)
3. Implement `Path::push()`, `get()`, `objects()`, `contains_vertex()`
4. Implement `From<&Value>` for `PathValue`

**Acceptance Criteria**:
- [ ] `Path::push()` adds elements with labels
- [ ] `Path::get("label")` returns elements by label
- [ ] `Path::contains_vertex()` detects vertices in path
- [ ] Unit tests pass

---

#### Phase 1.8: AnyStep Trait Definition
**File**: `src/traversal/step.rs` (new file)  
**Duration**: 1-2 hours

**Tasks**:
1. Create `src/traversal/step.rs`
2. Define `AnyStep` trait with `apply()`, `clone_box()`, `name()` methods
3. Implement `Clone` for `Box<dyn AnyStep>`
4. Create `impl_filter_step!` macro
5. Create `impl_flatmap_step!` macro

**Acceptance Criteria**:
- [ ] `AnyStep` trait compiles with correct signatures
- [ ] `Box<dyn AnyStep>` is clonable
- [ ] Macros expand correctly (test with dummy step)

---

#### Phase 1.9: Traversal Type with Type Erasure
**File**: `src/traversal/mod.rs`  
**Duration**: 2 hours

**Tasks**:
1. Replace existing `Traversal<S, E, T>` with `Traversal<In, Out>`
2. Implement `Traversal::new()`, `with_source()`, `add_step()`, `append()`
3. Define `TraversalSource` enum (AllVertices, Vertices, AllEdges, Edges, Inject)
4. Implement `Clone` for `Traversal`
5. Implement `into_steps()` method

**Acceptance Criteria**:
- [ ] `Traversal::<Value, Value>::new()` creates empty traversal
- [ ] `add_step()` changes output type parameter correctly
- [ ] `append()` merges steps from another traversal
- [ ] Clone works (steps are cloned via `clone_box`)

---

#### Phase 1.10: Basic Step Implementations (Identity, Start)
**File**: `src/traversal/step.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `IdentityStep` (pass-through)
2. Implement `StartStep` for source expansion
3. Test both steps compile and implement `AnyStep`

**Acceptance Criteria**:
- [ ] `IdentityStep.apply()` returns input unchanged
- [ ] `StartStep` expands source to traversers
- [ ] Both implement `AnyStep` correctly

---

### Week 2: Source and Terminal Steps

#### Phase 2.1: GraphTraversalSource Implementation
**File**: `src/traversal/source.rs` (new file)  
**Duration**: 2 hours

**Tasks**:
1. Create `src/traversal/source.rs`
2. Implement `GraphTraversalSource` struct with snapshot/interner references
3. Implement `v()` method returning `BoundTraversal`
4. Implement `e()` method returning `BoundTraversal`
5. Implement `v_ids()` and `e_ids()` for specific IDs
6. Implement `inject()` for arbitrary values

**Acceptance Criteria**:
- [ ] `g.v()` creates traversal from all vertices
- [ ] `g.v_ids([id1, id2])` creates traversal from specific vertices
- [ ] `g.e()` creates traversal from all edges
- [ ] `g.inject([1, 2, 3])` injects values

---

#### Phase 2.2: BoundTraversal Wrapper
**File**: `src/traversal/source.rs`  
**Duration**: 2 hours

**Tasks**:
1. Implement `BoundTraversal<'g, In, Out>` struct
2. Implement `add_step()` method
3. Implement `append()` for anonymous traversal merging
4. Implement `create_context()` private method
5. Implement `interner()` accessor

**Acceptance Criteria**:
- [ ] `BoundTraversal` holds snapshot, interner, and inner Traversal
- [ ] `add_step()` returns new `BoundTraversal` with updated type
- [ ] `append()` merges anonymous traversal steps
- [ ] Clone works for `BoundTraversal`

---

#### Phase 2.3: TraversalExecutor for Execution
**File**: `src/traversal/source.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Implement `TraversalExecutor<'g>` struct
2. Implement `TraversalExecutor::new()` that executes steps eagerly
3. Implement `Iterator` for `TraversalExecutor`
4. Implement `BoundTraversal::execute()` returning `TraversalExecutor`

**Acceptance Criteria**:
- [ ] `TraversalExecutor` collects results from step pipeline
- [ ] Iterator implementation returns traversers
- [ ] Execution works end-to-end (source -> steps -> results)

---

#### Phase 2.4: GraphSnapshot Integration
**File**: `src/graph.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add `traversal()` method to `GraphSnapshot`
2. Ensure `interner()` accessor exists on `GraphSnapshot`
3. Update imports in graph module

**Acceptance Criteria**:
- [ ] `snapshot.traversal()` returns `GraphTraversalSource`
- [ ] Basic traversal `g.v().count()` works end-to-end

---

#### Phase 2.5: Terminal Steps - Basic Collection
**File**: `src/traversal/terminal.rs` (new file)  
**Duration**: 2 hours

**Tasks**:
1. Create `src/traversal/terminal.rs`
2. Implement `to_list()` returning `Vec<Value>`
3. Implement `to_set()` returning `HashSet<Value>`
4. Implement `next()` returning `Option<Value>`
5. Implement `has_next()` returning `bool`
6. Implement `iterate()` for side-effect-only execution

**Acceptance Criteria**:
- [ ] `to_list()` collects all values
- [ ] `to_set()` deduplicates values
- [ ] `next()` returns first value or None
- [ ] `iterate()` consumes traversal without collecting

---

#### Phase 2.6: Terminal Steps - Aggregation
**File**: `src/traversal/terminal.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `count()` returning `u64`
2. Implement `one()` returning `Result<Value, TraversalError>`
3. Implement `take(n)` returning `Vec<Value>`
4. Implement `fold()`

**Acceptance Criteria**:
- [ ] `count()` returns correct count
- [ ] `one()` errors on 0 or 2+ results
- [ ] `take(n)` returns first n values
- [ ] `fold()` works with custom accumulator

---

#### Phase 2.7: Terminal Steps - Numeric Aggregation
**File**: `src/traversal/terminal.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `sum()` returning `Value`
2. Implement `min()` returning `Option<Value>`
3. Implement `max()` returning `Option<Value>`
4. Implement `iter()` returning iterator
5. Implement `traversers()` returning iterator with metadata

**Acceptance Criteria**:
- [ ] `sum()` adds numeric values
- [ ] `min()`/`max()` find extremes
- [ ] `iter()` allows custom iteration

---

### Week 3: Filter Steps

#### Phase 3.1: HasLabelStep Implementation
**File**: `src/traversal/filter.rs` (new file)  
**Duration**: 2 hours

**Tasks**:
1. Create `src/traversal/filter.rs`
2. Implement `HasLabelStep` struct with labels vector
3. Implement `AnyStep` for `HasLabelStep`
4. Add `has_label()` method to `BoundTraversal`
5. Add `has_label()` method to `Traversal` (for anonymous)
6. Add `has_label_any()` for multiple labels

**Acceptance Criteria**:
- [ ] `has_label("person")` filters to person vertices
- [ ] `has_label_any(&["person", "company"])` matches either
- [ ] Works with both vertices and edges
- [ ] Returns empty for non-vertex/edge values

---

#### Phase 3.2: HasStep and HasValueStep
**File**: `src/traversal/filter.rs`  
**Duration**: 2 hours

**Tasks**:
1. Implement `HasStep` (property existence check)
2. Implement `HasValueStep` (property value equality)
3. Add `has()` method to BoundTraversal and Traversal
4. Add `has_value()` method to BoundTraversal and Traversal

**Acceptance Criteria**:
- [ ] `has("age")` filters to elements with "age" property
- [ ] `has_value("name", "Alice")` matches exact value
- [ ] Works for both vertices and edges

---

#### Phase 3.3: Generic FilterStep
**File**: `src/traversal/filter.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `FilterStep<F>` with closure predicate
2. Implement `AnyStep` for `FilterStep<F>` with appropriate bounds
3. Add `filter()` method to BoundTraversal and Traversal

**Acceptance Criteria**:
- [ ] Custom predicates work: `filter(|ctx, v| ...)`
- [ ] Closure can access ExecutionContext
- [ ] FilterStep is Clone (closure must be Clone)

---

#### Phase 3.4: DedupStep
**File**: `src/traversal/filter.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `DedupStep` using HashSet for seen values
2. Implement `AnyStep` for `DedupStep`
3. Add `dedup()` method to BoundTraversal and Traversal

**Acceptance Criteria**:
- [ ] `dedup()` removes duplicate values
- [ ] Uses Value's Hash implementation
- [ ] Preserves first occurrence order

---

#### Phase 3.5: LimitStep, SkipStep, RangeStep
**File**: `src/traversal/filter.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `LimitStep` with count
2. Implement `SkipStep` with count
3. Implement `RangeStep` with start/end
4. Add methods to BoundTraversal and Traversal

**Acceptance Criteria**:
- [ ] `limit(5)` returns at most 5 results
- [ ] `skip(3)` skips first 3 results
- [ ] `range(2, 5)` returns elements 2, 3, 4

---

#### Phase 3.6: HasIdStep
**File**: `src/traversal/filter.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `HasIdStep` for filtering by element ID
2. Support both single and multiple IDs
3. Add static constructors for vertex/edge variants

**Acceptance Criteria**:
- [ ] `HasIdStep::vertex(VertexId(1))` matches specific vertex
- [ ] `HasIdStep::vertices(vec![id1, id2])` matches multiple
- [ ] Works for edges too

---

### Week 4: Navigation Steps

#### Phase 4.1: OutStep Implementation
**File**: `src/traversal/navigation.rs` (new file)  
**Duration**: 2-3 hours

**Tasks**:
1. Create `src/traversal/navigation.rs`
2. Implement `OutStep` with optional edge labels
3. Implement `AnyStep` for `OutStep`
4. Add `out()` and `out_labels()` to BoundTraversal and Traversal

**Acceptance Criteria**:
- [ ] `out()` traverses all outgoing edges to target vertices
- [ ] `out_labels(&["knows"])` filters by edge label
- [ ] Non-vertex values produce no results
- [ ] Preserves traverser metadata (split)

---

#### Phase 4.2: InStep Implementation
**File**: `src/traversal/navigation.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `InStep` with optional edge labels
2. Implement `AnyStep` for `InStep`
3. Add `in_()` and `in_labels()` methods

**Acceptance Criteria**:
- [ ] `in_()` traverses incoming edges to source vertices
- [ ] `in_labels(&["knows"])` filters by edge label
- [ ] Correctly navigates reverse direction

---

#### Phase 4.3: BothStep Implementation
**File**: `src/traversal/navigation.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `BothStep` combining out and in traversals
2. Implement `AnyStep` for `BothStep`
3. Add `both()` and `both_labels()` methods

**Acceptance Criteria**:
- [ ] `both()` includes neighbors from both directions
- [ ] Results from out and in are chained
- [ ] Edge label filtering works

---

#### Phase 4.4: OutEStep, InEStep, BothEStep
**File**: `src/traversal/navigation.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Implement `OutEStep` - traverse to outgoing edges
2. Implement `InEStep` - traverse to incoming edges
3. Implement `BothEStep` - traverse to all incident edges
4. Add corresponding methods to BoundTraversal and Traversal

**Acceptance Criteria**:
- [ ] `out_e()` returns edges, not vertices
- [ ] `in_e()` returns incoming edges
- [ ] `both_e()` returns all incident edges
- [ ] Edge label filtering works

---

#### Phase 4.5: OutVStep, InVStep, BothVStep
**File**: `src/traversal/navigation.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `OutVStep` - get source vertex of edge
2. Implement `InVStep` - get target vertex of edge
3. Implement `BothVStep` - get both vertices of edge
4. Add corresponding methods

**Acceptance Criteria**:
- [ ] `out_v()` returns source vertex from edge
- [ ] `in_v()` returns target vertex from edge
- [ ] `both_v()` returns both vertices (2 per edge)
- [ ] Non-edge values produce no results

---

### Week 5: Transform Steps and Polish

#### Phase 5.1: ValuesStep Implementation
**File**: `src/traversal/transform.rs` (new file)  
**Duration**: 2 hours

**Tasks**:
1. Create `src/traversal/transform.rs`
2. Implement `ValuesStep` with single or multiple keys
3. Implement `AnyStep` for `ValuesStep`
4. Add `values()` and `values_multi()` methods

**Acceptance Criteria**:
- [ ] `values("name")` extracts name property
- [ ] `values_multi(&["name", "age"])` extracts multiple properties
- [ ] Works for both vertices and edges
- [ ] Missing properties are skipped

---

#### Phase 5.2: IdStep and LabelStep
**File**: `src/traversal/transform.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `IdStep` - extract element ID as Int
2. Implement `LabelStep` - extract element label as String
3. Add `id()` and `label()` methods

**Acceptance Criteria**:
- [ ] `id()` returns `Value::Int` with element ID
- [ ] `label()` returns `Value::String` with label name
- [ ] Both work for vertices and edges
- [ ] Non-element values pass through or filter

---

#### Phase 5.3: MapStep and FlatMapStep
**File**: `src/traversal/transform.rs`  
**Duration**: 2 hours

**Tasks**:
1. Implement `MapStep<F>` with closure
2. Implement `FlatMapStep<F>` with closure returning Vec
3. Add `map()` and `flat_map()` methods

**Acceptance Criteria**:
- [ ] `map(|ctx, v| ...)` transforms each value
- [ ] `flat_map(|ctx, v| vec![...])` expands to multiple values
- [ ] Closures can access ExecutionContext

---

#### Phase 5.4: ConstantStep and PathStep
**File**: `src/traversal/transform.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `ConstantStep` - emit constant value
2. Implement `PathStep` - convert path to Value::List
3. Add `constant()` and `path()` methods

**Acceptance Criteria**:
- [ ] `constant("x")` replaces all values with "x"
- [ ] `path()` returns list of path elements
- [ ] Path labels are preserved

---

#### Phase 5.5: execute_traversal Helper Function
**File**: `src/traversal/mod.rs` or `src/traversal/step.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `execute_traversal()` function for sub-traversal execution
2. Takes ExecutionContext, Traversal, and input iterator
3. Returns boxed iterator over output traversers

**Acceptance Criteria**:
- [ ] Can execute anonymous traversal steps with provided context
- [ ] Ignores traversal source, uses provided input
- [ ] Works with lazy iteration (no eager collection)

---

#### Phase 5.6: Anonymous Traversal Factory Module (`__`)
**File**: `src/traversal/mod.rs`  
**Duration**: 2 hours

**Tasks**:
1. Expand `__` module with factory functions
2. Add `__::out()`, `__::in_()`, `__::has_label()`, etc.
3. Add `__::identity()`, `__::constant()`, `__::values()`
4. Ensure all return `Traversal<Value, Value>`

**Acceptance Criteria**:
- [ ] `__::out()` creates anonymous out traversal
- [ ] `__::has_label("person")` creates anonymous filter
- [ ] Anonymous traversals can be appended to bound traversals

---

#### Phase 5.7: Module Re-exports and Prelude
**File**: `src/traversal/mod.rs`, `src/lib.rs`  
**Duration**: 1 hour

**Tasks**:
1. Update `mod.rs` to re-export all public types
2. Update `src/lib.rs` prelude with traversal types
3. Ensure clean public API

**Acceptance Criteria**:
- [ ] `use rustgremlin::prelude::*` imports traversal types
- [ ] Public API is clean and documented

---

#### Phase 5.8: Integration Tests
**File**: `tests/traversal.rs` (new file)  
**Duration**: 2-3 hours

**Tasks**:
1. Create integration test file
2. Implement `create_test_graph()` helper
3. Add tests for all major traversal patterns:
   - Basic `v()`, `e()`, `count()`
   - Filter chains (`has_label`, `has_value`, `dedup`, `limit`)
   - Navigation (`out`, `in_`, `both`, edge variants)
   - Transforms (`values`, `id`, `label`, `map`)
   - Terminals (`to_list`, `one`, `count`, `sum`)
4. Add test for anonymous traversal `append()`

**Acceptance Criteria**:
- [ ] All tests pass with test graph (4 vertices, 5 edges)
- [ ] Tests cover both success and error cases
- [ ] 100% branch coverage for critical paths

---

#### Phase 5.9: Benchmarks
**File**: `benches/traversal.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Update existing benchmark file
2. Add `create_benchmark_graph()` helper (10K vertices, 100K edges)
3. Add benchmarks:
   - `v().has_label().count()`
   - `v().out().limit().count()`
   - `v().out().out().dedup().count()`

**Acceptance Criteria**:
- [ ] Benchmarks run successfully
- [ ] Results are reasonable (no obvious performance bugs)

---

#### Phase 5.10: Documentation and Cleanup
**Duration**: 2-3 hours

**Tasks**:
1. Add doc comments to all public types and methods
2. Add module-level documentation
3. Clean up any TODOs or placeholder code
4. Run `cargo clippy` and fix warnings
5. Run `cargo fmt`
6. Verify all tests pass

**Acceptance Criteria**:
- [ ] All public items have doc comments
- [ ] No clippy warnings (with `-D warnings`)
- [ ] Code is properly formatted
- [ ] All tests pass
- [ ] `cargo doc` builds without errors

---

## Exit Criteria Checklist

From spec section "Exit Criteria":

### Value Type Changes
- [ ] `Value` enum extended with `Vertex(VertexId)` and `Edge(EdgeId)` variants
- [ ] `Value` implements `Hash` and `Eq` (for `DedupStep`)
- [ ] `OrderedFloat` implements `Hash`

### Core Types
- [ ] All core types compile (`Traversal`, `Traverser`, `Path`, `ExecutionContext`)
- [ ] `AnyStep` trait works with type erasure
- [ ] `GraphTraversalSource` with `v()` and `e()` starting points
- [ ] `BoundTraversal` wrapper correctly manages execution context

### Navigation Steps
- [ ] `out()`, `in_()`, `both()` work
- [ ] `out_e()`, `in_e()`, `both_e()` work
- [ ] `out_v()`, `in_v()`, `both_v()` work

### Filter Steps
- [ ] `has_label()`, `has()`, `has_value()` work
- [ ] `filter()`, `dedup()`, `limit()`, `skip()`, `range()` work

### Transform Steps
- [ ] `values()`, `id()`, `label()` work
- [ ] `map()`, `flat_map()`, `constant()` work
- [ ] `path()` works

### Terminal Steps
- [ ] `to_list()`, `to_set()`, `next()`, `one()` work
- [ ] `has_next()`, `iterate()`, `count()` work
- [ ] `sum()`, `min()`, `max()` work

### Behavior
- [ ] Lazy evaluation verified (no work until terminal step)
- [ ] Path tracking works correctly
- [ ] Label resolution works via ExecutionContext
- [ ] Anonymous traversals can be appended to bound traversals

### Testing
- [ ] All unit tests pass
- [ ] All integration tests pass with 10K vertex, 100K edge graph
- [ ] Benchmarks run successfully

---

## File Summary

New files to create:
- `src/traversal/context.rs` - ExecutionContext, SideEffects
- `src/traversal/step.rs` - AnyStep trait, IdentityStep, StartStep
- `src/traversal/source.rs` - GraphTraversalSource, BoundTraversal, TraversalExecutor
- `src/traversal/filter.rs` - All filter steps
- `src/traversal/navigation.rs` - All navigation steps
- `src/traversal/transform.rs` - All transform steps
- `src/traversal/terminal.rs` - Terminal step implementations (methods on BoundTraversal)
- `tests/traversal.rs` - Integration tests

Files to modify:
- `src/value.rs` - Add Vertex/Edge variants, Hash, accessors
- `src/traversal/mod.rs` - Replace existing types, add re-exports, `__` module
- `src/graph.rs` - Add `traversal()` method to GraphSnapshot
- `src/lib.rs` - Update prelude exports
- `benches/traversal.rs` - Add traversal benchmarks

---

## Dependencies

No new crate dependencies required for Phase 3. The spec mentions `regex = "1.10"` for Phase 4 predicates, but this can be added in Phase 4.

Existing dependencies used:
- `parking_lot` - RwLock for SideEffects
- `smallvec` - Path labels
- `thiserror` - TraversalError (already exists)
