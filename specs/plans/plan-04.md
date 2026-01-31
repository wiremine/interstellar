# Plan 04: Anonymous Traversals and Predicates Implementation

**Phase 4 of Interstellar Implementation**

Based on: `specs/spec-04-anonymous-traversal.md`

---

## Overview

This plan breaks down the Anonymous Traversals and Predicates implementation into granular, testable phases. Each phase represents approximately 1-2 hours of focused work and includes specific acceptance criteria.

**Total Duration**: 2-3 weeks  
**Current State**: Phase 3 (Traversal Engine Core) is complete. The `__` module and `Traversal<In, Out>` types exist but need extension.

**Key Architectural Point**: Anonymous traversals use the **same `Traversal<In, Out>` type** as bound traversals. The difference is that anonymous traversals have no source—they're pure step pipelines that receive their `ExecutionContext` when spliced into a parent traversal.

---

## Dependencies

New crate dependency required:

```toml
[dependencies]
regex = "1.10"
```

---

## Implementation Order

### Week 1: Predicate System and Core Infrastructure

#### Phase 1.1: Create Predicate Module and Trait
**File**: `src/traversal/predicate.rs` (new file)  
**Duration**: 1-2 hours

**Tasks**:
1. Create `src/traversal/predicate.rs`
2. Define `Predicate` trait with `test()`, `clone_box()` methods
3. Implement `Clone` for `Box<dyn Predicate>`
4. Add module to `src/traversal/mod.rs`

**Code Structure**:
```rust
pub trait Predicate: Send + Sync {
    fn test(&self, value: &Value) -> bool;
    fn clone_box(&self) -> Box<dyn Predicate>;
}

impl Clone for Box<dyn Predicate> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
```

**Acceptance Criteria**:
- [x] `Predicate` trait compiles with correct signatures
- [x] `Box<dyn Predicate>` is clonable
- [x] Module exports are correct

---

#### Phase 1.2: Comparison Predicates (eq, neq, lt, lte, gt, gte)
**File**: `src/traversal/predicate.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `Eq` predicate struct and factory function `p::eq()`
2. Implement `Neq` predicate struct and factory function `p::neq()`
3. Implement `Lt` predicate with cross-type numeric comparison
4. Implement `Lte` predicate with cross-type numeric comparison
5. Implement `Gt` predicate with cross-type numeric comparison
6. Implement `Gte` predicate with cross-type numeric comparison
7. Handle Int/Float cross-comparison correctly

**Code Changes**:
```rust
pub mod p {
    #[derive(Clone)]
    pub struct Eq(Value);
    
    impl Predicate for Eq {
        fn test(&self, value: &Value) -> bool {
            value == &self.0
        }
        fn clone_box(&self) -> Box<dyn Predicate> {
            Box::new(self.clone())
        }
    }
    
    pub fn eq<T: Into<Value>>(value: T) -> impl Predicate {
        Eq(value.into())
    }
    // ... similar for neq, lt, lte, gt, gte
}
```

**Acceptance Criteria**:
- [x] `p::eq(42).test(&Value::Int(42))` returns true
- [x] `p::lt(50).test(&Value::Int(30))` returns true
- [x] `p::gt(50).test(&Value::Float(60.0))` returns true (cross-type)
- [x] All comparison predicates implement `Clone`
- [x] Unit tests pass

---

#### Phase 1.3: Range Predicates (between, inside, outside)
**File**: `src/traversal/predicate.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `Between` predicate (inclusive start, exclusive end)
2. Implement `Inside` predicate (exclusive both)
3. Implement `Outside` predicate (value < start OR value > end)
4. Add factory functions `p::between()`, `p::inside()`, `p::outside()`

**Acceptance Criteria**:
- [x] `p::between(10, 20).test(&Value::Int(10))` returns true (inclusive start)
- [x] `p::between(10, 20).test(&Value::Int(20))` returns false (exclusive end)
- [x] `p::inside(10, 20).test(&Value::Int(10))` returns false (exclusive)
- [x] `p::outside(10, 20).test(&Value::Int(5))` returns true
- [x] Unit tests pass

---

#### Phase 1.4: Collection Predicates (within, without)
**File**: `src/traversal/predicate.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Implement `Within` predicate (value is in set)
2. Implement `Without` predicate (value is NOT in set)
3. Add factory functions `p::within()`, `p::without()`

**Acceptance Criteria**:
- [x] `p::within([1, 2, 3]).test(&Value::Int(2))` returns true
- [x] `p::within([1, 2, 3]).test(&Value::Int(4))` returns false
- [x] `p::without([1, 2, 3]).test(&Value::Int(4))` returns true
- [x] Unit tests pass

---

#### Phase 1.5: String Predicates (containing, starting_with, ending_with)
**File**: `src/traversal/predicate.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `Containing` predicate
2. Implement `StartingWith` predicate
3. Implement `EndingWith` predicate
4. Add factory functions

**Acceptance Criteria**:
- [x] `p::containing("foo").test(&Value::String("foobar".into()))` returns true
- [x] `p::starting_with("foo").test(&Value::String("foobar".into()))` returns true
- [x] `p::ending_with("bar").test(&Value::String("foobar".into()))` returns true
- [x] Non-string values return false
- [x] Unit tests pass

---

#### Phase 1.6: Regex Predicate
**File**: `src/traversal/predicate.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add `regex = "1.10"` dependency to `Cargo.toml`
2. Implement `Regex` predicate struct with compiled regex
3. Implement `p::regex()` factory (panics on invalid pattern)
4. Implement `p::try_regex()` factory (returns Option)

**Acceptance Criteria**:
- [x] `p::regex(r"^\d{3}-\d{4}$").test(&Value::String("123-4567".into()))` returns true
- [x] `p::try_regex(r"[invalid")` returns `None`
- [x] Non-string values return false
- [x] Unit tests pass

---

#### Phase 1.7: Logical Predicate Composition (and, or, not)
**File**: `src/traversal/predicate.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `And<P1, P2>` predicate
2. Implement `Or<P1, P2>` predicate
3. Implement `Not<P>` predicate
4. Add factory functions `p::and()`, `p::or()`, `p::not()`

**Code Structure**:
```rust
#[derive(Clone)]
pub struct And<P1, P2>(P1, P2);

impl<P1: Predicate + Clone + 'static, P2: Predicate + Clone + 'static> Predicate for And<P1, P2> {
    fn test(&self, value: &Value) -> bool {
        self.0.test(value) && self.1.test(value)
    }
    fn clone_box(&self) -> Box<dyn Predicate> {
        Box::new(self.clone())
    }
}
```

**Acceptance Criteria**:
- [x] `p::and(p::gte(18), p::lt(65)).test(&Value::Int(30))` returns true
- [x] `p::or(p::eq("a"), p::eq("b")).test(&Value::String("a".into()))` returns true
- [x] `p::not(p::eq(42)).test(&Value::Int(41))` returns true
- [x] Composed predicates are clonable
- [x] Unit tests pass

---

#### Phase 1.8: HasWhereStep Implementation
**File**: `src/traversal/predicate.rs`  
**Duration**: 2 hours

**Tasks**:
1. Implement `HasWhereStep<P>` struct
2. Implement `AnyStep` for `HasWhereStep<P>`
3. Implement property extraction from `Value::Vertex` and `Value::Edge`
4. Implement `HasWhereStepDyn` for type-erased predicates
5. Add `has_where()` method to `BoundTraversal`
6. Add `has_where()` method to `Traversal`

**Acceptance Criteria**:
- [x] `has_where("age", p::gte(18))` filters vertices by property
- [x] Works for both vertices and edges
- [x] Missing properties cause the traverser to be filtered out
- [x] Non-element values are filtered out
- [x] Unit tests pass

---

### Week 2: Filter Steps with Anonymous Traversals

#### Phase 2.1: Create Branch Module
**File**: `src/traversal/branch.rs` (new file)  
**Duration**: 30 minutes

**Tasks**:
1. Create `src/traversal/branch.rs`
2. Add necessary imports (`ExecutionContext`, `Traversal`, `Traverser`, `AnyStep`, `execute_traversal_from`)
3. Add module to `src/traversal/mod.rs`

**Acceptance Criteria**:
- [x] Module compiles
- [x] Exports are accessible from `src/traversal/mod.rs`

---

#### Phase 2.2: WhereStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `WhereStep` struct holding `Traversal<Value, Value>`
2. Implement `AnyStep` for `WhereStep`
3. Execute sub-traversal for each input traverser
4. Emit traverser only if sub-traversal produces results
5. Add `where_()` method to `BoundTraversal` and `Traversal`

**Code Structure**:
```rust
#[derive(Clone)]
pub struct WhereStep {
    sub: Traversal<Value, Value>,
}

impl AnyStep for WhereStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let sub = self.sub.clone();
        Box::new(input.filter(move |t| {
            let sub_input = Box::new(std::iter::once(t.clone()));
            let mut results = execute_traversal_from(ctx, &sub, sub_input);
            results.next().is_some()
        }))
    }
    // ...
}
```

**Acceptance Criteria**:
- [x] `where_(__.out())` filters to traversers with outgoing edges
- [x] `where_(__.out().has_label("person"))` filters to traversers with person neighbors
- [x] Empty sub-traversal results filter out the traverser
- [x] Unit tests pass

---

#### Phase 2.3: NotStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `NotStep` struct holding `Traversal<Value, Value>`
2. Implement `AnyStep` for `NotStep`
3. Emit traverser only if sub-traversal produces NO results
4. Add `not()` method to `BoundTraversal` and `Traversal`

**Acceptance Criteria**:
- [x] `not(__.out())` filters to traversers WITHOUT outgoing edges
- [x] Inverse of `where_()` behavior
- [x] Unit tests pass

---

#### Phase 2.4: AndStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `AndStep` struct holding `Vec<Traversal<Value, Value>>`
2. Implement `AnyStep` for `AndStep`
3. Emit traverser only if ALL sub-traversals produce results
4. Add `and_()` method to `BoundTraversal` and `Traversal`

**Acceptance Criteria**:
- [x] `and_(vec![__.out(), __.in_()])` requires both directions
- [x] Short-circuits on first failing condition
- [x] Unit tests pass

---

#### Phase 2.5: OrStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `OrStep` struct holding `Vec<Traversal<Value, Value>>`
2. Implement `AnyStep` for `OrStep`
3. Emit traverser if ANY sub-traversal produces results
4. Add `or_()` method to `BoundTraversal` and `Traversal`

**Acceptance Criteria**:
- [x] `or_(vec![__.has_label("a"), __.has_label("b")])` matches either label
- [x] Short-circuits on first successful condition
- [x] Unit tests pass

---

#### Phase 2.6: Extend __ Factory Module with Filter Steps
**File**: `src/traversal/mod.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add `__::where_()` factory function
2. Add `__::not()` factory function
3. Add `__::and_()` factory function
4. Add `__::or_()` factory function
5. Ensure all return `Traversal<Value, Value>`

**Acceptance Criteria**:
- [x] `__::where_(__.out())` creates anonymous where traversal
- [x] `__::not(__.out())` creates anonymous not traversal
- [x] Anonymous traversals can be nested
- [x] Unit tests pass

---

### Week 2-3: Branch Steps

#### Phase 3.1: UnionStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 2 hours

**Tasks**:
1. Implement `UnionStep` struct holding `Vec<Traversal<Value, Value>>`
2. Implement `AnyStep` for `UnionStep`
3. For each input, execute ALL branches and merge results
4. Maintain traverser-major order (all results from one input before next)
5. Add `union()` method to `BoundTraversal` and `Traversal`

**Acceptance Criteria**:
- [x] `union(vec![__.out(), __.in_()])` returns neighbors from both directions
- [x] Results are merged in traverser-major order
- [x] Works with empty branches
- [x] Unit tests pass

---

#### Phase 3.2: CoalesceStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `CoalesceStep` struct holding `Vec<Traversal<Value, Value>>`
2. Implement `AnyStep` for `CoalesceStep`
3. Try branches in order, return results from first non-empty
4. Short-circuit on first success
5. Add `coalesce()` method to `BoundTraversal` and `Traversal`

**Acceptance Criteria**:
- [x] `coalesce(vec![__.values("nickname"), __.values("name")])` tries nickname first
- [x] Falls back to subsequent branches if prior is empty
- [x] Returns empty if all branches are empty
- [x] Unit tests pass

---

#### Phase 3.3: ChooseStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 2 hours

**Tasks**:
1. Implement `ChooseStep` struct with condition, if_true, if_false traversals
2. Implement `AnyStep` for `ChooseStep`
3. Evaluate condition; if results, execute if_true, else if_false
4. Add `choose()` method to `BoundTraversal` and `Traversal`

**Acceptance Criteria**:
- [x] `choose(__.has_label("a"), __.out(), __.in_())` branches based on label
- [x] Both branches execute correctly
- [x] Condition is evaluated per-traverser
- [x] Unit tests pass

---

#### Phase 3.4: OptionalStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `OptionalStep` struct holding `Traversal<Value, Value>`
2. Implement `AnyStep` for `OptionalStep`
3. If sub-traversal produces results, emit those; else emit original
4. Add `optional()` method to `BoundTraversal` and `Traversal`

**Acceptance Criteria**:
- [x] `optional(__.out())` returns neighbors if any, else keeps original
- [x] Traversers without matching neighbors pass through unchanged
- [x] Unit tests pass

---

#### Phase 3.5: LocalStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `LocalStep` struct holding `Traversal<Value, Value>`
2. Implement `AnyStep` for `LocalStep`
3. Execute sub-traversal in isolated scope per-traverser
4. Add `local()` method to `BoundTraversal` and `Traversal`

**Acceptance Criteria**:
- [x] `local(__.out().count())` counts per-traverser, not globally
- [x] Aggregations operate per-input
- [x] Unit tests pass

---

#### Phase 3.6: Extend __ Factory Module with Branch Steps
**File**: `src/traversal/mod.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add `__::union()` factory function
2. Add `__::coalesce()` factory function
3. Add `__::choose()` factory function
4. Add `__::optional()` factory function
5. Add `__::local()` factory function

**Acceptance Criteria**:
- [x] All factory functions return `Traversal<Value, Value>`
- [x] Can be used as sub-traversals in filter/branch steps
- [x] Unit tests pass

---

### Week 3: Repeat Step

#### Phase 4.1: Create Repeat Module and RepeatConfig
**File**: `src/traversal/repeat.rs` (new file)  
**Duration**: 1 hour

**Tasks**:
1. Create `src/traversal/repeat.rs`
2. Define `RepeatConfig` struct with fields:
   - `times: Option<usize>`
   - `until: Option<Traversal<Value, Value>>`
   - `emit: bool`
   - `emit_if: Option<Traversal<Value, Value>>`
   - `emit_first: bool`
3. Implement `Default` for `RepeatConfig`
4. Add module to `src/traversal/mod.rs`

**Acceptance Criteria**:
- [x] `RepeatConfig::default()` has all fields as None/false
- [x] All fields are accessible
- [x] Module exports correctly

---

#### Phase 4.2: RepeatStep Core Implementation
**File**: `src/traversal/repeat.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Implement `RepeatStep` struct with sub-traversal and config
2. Implement helper methods:
   - `satisfies_until()` - check termination condition
   - `should_emit()` - check emission condition
3. Implement `Clone` for `RepeatStep`
4. Implement `AnyStep` trait skeleton

**Code Structure**:
```rust
#[derive(Clone)]
pub struct RepeatStep {
    sub: Traversal<Value, Value>,
    config: RepeatConfig,
}

impl RepeatStep {
    pub fn new(sub: Traversal<Value, Value>) -> Self;
    pub fn with_config(sub: Traversal<Value, Value>, config: RepeatConfig) -> Self;
    fn satisfies_until(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool;
    fn should_emit(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool;
}
```

**Acceptance Criteria**:
- [x] `RepeatStep::new()` creates step with default config
- [x] `satisfies_until()` correctly evaluates until traversal
- [x] `should_emit()` correctly evaluates emit_if traversal
- [x] Step is clonable

---

#### Phase 4.3: RepeatIterator with BFS Frontier
**File**: `src/traversal/repeat.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Implement `RepeatIterator` struct with:
   - BFS frontier queue: `VecDeque<(Traverser, usize)>`
   - Emit buffer for results
   - Initialization state
2. Implement `process_frontier()` method for one BFS level
3. Implement `Iterator` for `RepeatIterator`
4. Wire up `AnyStep::apply()` to return `RepeatIterator`

**Code Structure**:
```rust
struct RepeatIterator<'a> {
    ctx: &'a ExecutionContext<'a>,
    frontier: VecDeque<(Traverser, usize)>,
    sub: Traversal<Value, Value>,
    config: RepeatConfig,
    step: RepeatStep,
    emit_buffer: VecDeque<Traverser>,
    initialized: bool,
    input: Option<Box<dyn Iterator<Item = Traverser> + 'a>>,
}
```

**Acceptance Criteria**:
- [x] BFS processes one level at a time
- [x] `times` limit is respected
- [x] `until` condition terminates correctly
- [x] Results are emitted in correct order

---

#### Phase 4.4: RepeatTraversal Builder
**File**: `src/traversal/repeat.rs`  
**Duration**: 2 hours

**Tasks**:
1. Implement `RepeatTraversal<'g, In>` builder struct
2. Implement builder methods:
   - `times(n)` - set max iterations
   - `until(condition)` - set termination condition
   - `emit()` - enable intermediate emission
   - `emit_if(condition)` - conditional emission
   - `emit_first()` - emit initial input
3. Implement `finalize()` to create `BoundTraversal`
4. Implement terminal methods directly on builder (`to_list()`, `count()`, `next()`)
5. Implement continuation methods (`has_label()`, `dedup()`, `values()`, etc.)

**Acceptance Criteria**:
- [x] `repeat(__.out()).times(2)` configures correctly
- [x] `repeat(__.out()).until(__.has_label("x"))` configures correctly
- [x] `repeat(__.out()).emit()` enables emission
- [x] Terminal methods work directly on builder
- [x] Continuation methods return `BoundTraversal`

---

#### Phase 4.5: Integrate RepeatStep with BoundTraversal
**File**: `src/traversal/source.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add `repeat()` method to `BoundTraversal`
2. Return `RepeatTraversal` builder
3. Ensure proper type transitions

**Acceptance Criteria**:
- [x] `g.v().repeat(__.out())` compiles
- [x] Returns `RepeatTraversal` for configuration
- [x] Type parameters are correct

---

#### Phase 4.6: Traverser Loop Count Support
**File**: `src/traversal/mod.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Add `loops: u32` field to `Traverser` if not present
2. Add `inc_loops()` method to increment loop count
3. Add `loops()` accessor method
4. Update `split()` to preserve loop count

**Acceptance Criteria**:
- [x] `traverser.loops()` returns current loop count
- [x] `inc_loops()` increments correctly
- [x] `split()` preserves loop count

---

### Week 3+: Integration and Polish

#### Phase 5.1: Filter Steps Integration Tests
**File**: `tests/traversal.rs` or `tests/anonymous.rs` (new)  
**Duration**: 2 hours

**Tasks**:
1. Create test graph helper with vertices and edges
2. Add integration tests for `where_()` step
3. Add integration tests for `not()` step
4. Add integration tests for `and_()` step
5. Add integration tests for `or_()` step

**Acceptance Criteria**:
- [x] `where_(__.out().has_label("person"))` filters correctly
- [x] `not(__.out())` finds leaf vertices
- [x] `and_()` requires all conditions
- [x] `or_()` accepts any condition
- [x] All tests pass

---

#### Phase 5.2: Branch Steps Integration Tests
**File**: `tests/traversal.rs` or `tests/anonymous.rs`  
**Duration**: 2 hours

**Tasks**:
1. Add integration tests for `union()` step
2. Add integration tests for `coalesce()` step
3. Add integration tests for `choose()` step
4. Add integration tests for `optional()` step
5. Add integration tests for `local()` step

**Acceptance Criteria**:
- [x] `union()` merges results correctly
- [x] `coalesce()` short-circuits correctly
- [x] `choose()` branches correctly
- [x] `optional()` falls back correctly
- [x] `local()` isolates aggregations
- [x] All tests pass

---

#### Phase 5.3: Repeat Step Integration Tests
**File**: `tests/traversal.rs` or `tests/anonymous.rs`  
**Duration**: 2 hours

**Tasks**:
1. Add integration tests for `repeat().times(n)`
2. Add integration tests for `repeat().until()`
3. Add integration tests for `repeat().emit()`
4. Add integration tests for `repeat().emit_if()`
5. Add integration tests for combined modifiers

**Acceptance Criteria**:
- [x] `repeat(__.out()).times(2)` traverses exactly 2 hops
- [x] `repeat(__.out()).until(__.has_label("company"))` terminates correctly
- [x] `repeat(__.out()).emit()` includes intermediate results
- [x] `repeat(__.out()).times(3).emit()` works together
- [x] All tests pass

---

#### Phase 5.4: Predicate Integration Tests
**File**: `tests/traversal.rs` or `tests/anonymous.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Add integration tests for `has_where()` with comparison predicates
2. Add integration tests for `has_where()` with range predicates
3. Add integration tests for `has_where()` with string predicates
4. Add integration tests for `has_where()` with composed predicates

**Acceptance Criteria**:
- [x] `has_where("age", p::gte(18))` filters correctly
- [x] `has_where("age", p::between(25, 35))` filters range
- [x] `has_where("name", p::starting_with("A"))` filters strings
- [x] `has_where("age", p::and(p::gte(18), p::lt(65)))` composes
- [x] All tests pass

---

#### Phase 5.5: Benchmarks
**File**: `benches/anonymous_traversal.rs` (new file)  
**Duration**: 1-2 hours

**Tasks**:
1. Create benchmark file with criterion setup
2. Implement `create_benchmark_graph()` helper (10K vertices)
3. Add benchmark for `where_()` step
4. Add benchmark for `union()` step
5. Add benchmark for `repeat().times(3)`
6. Add benchmark for `has_where()` with predicate

**Acceptance Criteria**:
- [ ] Benchmarks compile and run
- [ ] Results are reasonable (no performance regressions)
- [ ] Each benchmark exercises realistic traversal patterns

---

#### Phase 5.6: Module Re-exports and Prelude
**File**: `src/traversal/mod.rs`, `src/lib.rs`  
**Duration**: 1 hour

**Tasks**:
1. Re-export `p` module from `predicate.rs`
2. Re-export filter steps from `branch.rs`
3. Re-export branch steps from `branch.rs`
4. Re-export `RepeatStep`, `RepeatConfig`, `RepeatTraversal` from `repeat.rs`
5. Update prelude in `src/lib.rs`

**Acceptance Criteria**:
- [x] `use interstellar::prelude::*` imports `p`, `__`, all step types
- [x] `use interstellar::p;` works
- [x] Public API is clean

---

#### Phase 5.7: Documentation and Cleanup
**Duration**: 2-3 hours

**Tasks**:
1. Add doc comments to all public types in `predicate.rs`
2. Add doc comments to all public types in `branch.rs`
3. Add doc comments to all public types in `repeat.rs`
4. Add module-level documentation with examples
5. Run `cargo clippy` and fix warnings
6. Run `cargo fmt`
7. Verify all tests pass

**Acceptance Criteria**:
- [x] All public items have doc comments
- [x] No clippy warnings (with `-D warnings`)
- [x] Code is properly formatted
- [x] All tests pass
- [x] `cargo doc` builds without errors

---

## Exit Criteria Checklist

From spec section "Exit Criteria":

### Predicate System
- [x] `Predicate` trait compiles with `test()`, `clone_box()` methods
- [x] `Box<dyn Predicate>` is clonable
- [x] **Comparison predicates**: `eq`, `neq`, `lt`, `lte`, `gt`, `gte`
- [x] **Range predicates**: `between`, `inside`, `outside`
- [x] **Collection predicates**: `within`, `without`
- [x] **String predicates**: `containing`, `starting_with`, `ending_with`, `regex`
- [x] **Logical predicates**: `and`, `or`, `not`
- [x] All predicates are `Clone + Send + Sync`

### HasWhere Step
- [x] `has_where(key, predicate)` filters by property with predicate
- [x] Works for both vertices and edges
- [x] Missing properties filter out traverser

### Filter Steps
- [x] `where_(sub)` - filter by sub-traversal producing results
- [x] `not(sub)` - filter by sub-traversal NOT producing results
- [x] `and_(subs)` - all sub-traversals must produce results
- [x] `or_(subs)` - at least one sub-traversal must produce results

### Branch Steps
- [x] `union(branches)` - merge results from multiple branches
- [x] `coalesce(branches)` - first branch with results wins
- [x] `choose(cond, if_true, if_false)` - conditional branching
- [x] `optional(sub)` - try sub-traversal, keep original if empty
- [x] `local(sub)` - execute in isolated scope

### Repeat Step
- [x] `repeat(sub).times(n)` - fixed iterations
- [x] `repeat(sub).until(cond)` - conditional termination
- [x] `repeat(sub).emit()` - emit intermediate results
- [x] `repeat(sub).emit_if(cond)` - conditional emission
- [x] `repeat(sub).emit_first()` - emit initial input
- [x] BFS frontier processing for level-order traversal

### Anonymous Traversal Extensions
- [x] `__` module extended with filter step factories
- [x] `__` module extended with branch step factories
- [x] Anonymous traversals chain correctly
- [x] `Traversal<In, Out>` is cloneable for branching operations

### Testing
- [x] All unit tests pass
- [x] All integration tests pass
- [ ] Benchmarks run successfully on 10K vertex graph

---

## File Summary

New files to create:
- `src/traversal/predicate.rs` - `Predicate` trait, `p::` module, `HasWhereStep`
- `src/traversal/branch.rs` - Filter steps (`WhereStep`, `NotStep`, `AndStep`, `OrStep`) and branch steps (`UnionStep`, `CoalesceStep`, `ChooseStep`, `OptionalStep`, `LocalStep`)
- `src/traversal/repeat.rs` - `RepeatStep`, `RepeatConfig`, `RepeatTraversal` builder
- `benches/anonymous_traversal.rs` - Performance benchmarks

Files to modify:
- `Cargo.toml` - Add `regex = "1.10"` dependency
- `src/traversal/mod.rs` - Add module declarations, extend `__` factory, re-exports
- `src/traversal/source.rs` - Add `repeat()` method to `BoundTraversal`
- `src/lib.rs` - Update prelude exports
- `tests/traversal.rs` - Add integration tests (or create `tests/anonymous.rs`)

---

## Dependencies

```toml
[dependencies]
regex = "1.10"  # NEW - for string predicate regex matching
```

Existing dependencies used:
- `parking_lot` - RwLock (already present)
- `thiserror` - Error types (already present)

---

## Implementation Notes

### Cloneability Requirement

All steps must be `Clone` because branching operations need to clone sub-traversals for each input:

```rust
// UnionStep clones each branch for each input traverser
for branch in self.branches.iter() {
    let sub_input = std::iter::once(t.clone());
    execute_traversal_from(ctx, branch, sub_input)  // branch is borrowed, not cloned here
}
```

### Iterator Lifetime Management

The type-erased architecture requires cloning step data to avoid lifetime issues:

```rust
// GOOD: Clone the step data needed
fn apply<'a>(&'a self, ctx: &'a ExecutionContext<'a>, input: ...) -> ... {
    let step = self.clone();  // Owned copy
    Box::new(input.filter(move |t| step.matches(ctx, t)))  // OK!
}
```

### Predicate Type Erasure

The `clone_box()` pattern enables storing predicates as trait objects while supporting cloning:

```rust
impl Clone for Box<dyn Predicate> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}
```
