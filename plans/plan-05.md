# Plan 05: Query IR and Predicate System Implementation

**Phase 5 of Interstellar Implementation**

Based on: `specs/spec-05-ir.md`

---

## Overview

This plan breaks down the Query IR and Predicate System implementation into granular, testable phases. Each phase represents approximately 1-2 hours of focused work and includes specific acceptance criteria.

**Total Duration**: 3-4 days  
**Current State**: Phase 3 (Traversal Engine Core) and Phase 4 (Anonymous Traversals) are complete. The predicate system from Phase 4 provides a foundation, but this spec introduces a richer enum-based `Predicate` type and the Query IR for unified Gremlin/GQL compilation.

**Key Architectural Points**:
1. **Unified IR**: Both Gremlin bytecode and GQL AST compile to the same `QueryPlan` format
2. **Step-oriented IR**: Each `QueryOp` maps directly to traversal steps
3. **Value-based predicates**: The new `Predicate` enum replaces the trait-based approach for IR compatibility

---

## Dependencies

New crate dependency required (if not already present from Phase 4):

```toml
[dependencies]
regex = "1.10"  # For Predicate::Regex
```

---

## Implementation Order

### Day 1: Foundation - Core Types and Predicate System

#### Phase 1.1: Create Query Module Structure
**File**: `src/query/mod.rs` (new directory and file)  
**Duration**: 30 minutes

**Tasks**:
1. Create `src/query/` directory
2. Create `src/query/mod.rs` with module declarations
3. Add `pub mod query;` to `src/lib.rs`
4. Set up initial re-exports

**Code Structure**:
```rust
//! Query IR and predicate system for unified Gremlin/GQL compilation.

pub mod types;
pub mod predicate;
pub mod ir;
pub mod compiler;

pub use types::{Direction, SortOrder, Scope, T};
pub use predicate::{Predicate, p};
pub use ir::{QueryOp, QueryPlan};
pub use compiler::CompileError;
```

**Acceptance Criteria**:
- [ ] `src/query/` directory exists
- [ ] Module compiles with empty submodules
- [ ] `use crate::query::*` works from other modules

---

#### Phase 1.2: Core Types (Direction, SortOrder, Scope, T)
**File**: `src/query/types.rs` (new file)  
**Duration**: 30 minutes

**Tasks**:
1. Create `src/query/types.rs`
2. Define `Direction` enum (Out, In, Both)
3. Define `SortOrder` enum (Asc, Desc, Shuffle)
4. Define `Scope` enum (Local, Global)
5. Define `T` enum (Id, Label, Key, Value)
6. Derive standard traits (`Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`)

**Acceptance Criteria**:
- [ ] All four enums compile with correct variants
- [ ] All enums derive required traits
- [ ] Enums are re-exported from `mod.rs`

---

#### Phase 1.3: Predicate Enum - Comparison Variants
**File**: `src/query/predicate.rs` (new file)  
**Duration**: 1-2 hours

**Tasks**:
1. Create `src/query/predicate.rs`
2. Define `Predicate` enum with comparison variants:
   - `Eq(Value)`, `Neq(Value)`
   - `Lt(Value)`, `Lte(Value)`, `Gt(Value)`, `Gte(Value)`
3. Implement `evaluate()` method stub
4. Implement `value_eq()` helper function with type coercion
5. Implement `value_cmp()` helper function with type coercion
6. Add unit tests for comparison predicates

**Code Structure**:
```rust
use crate::value::Value;
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    // Comparison
    Eq(Value),
    Neq(Value),
    Lt(Value),
    Lte(Value),
    Gt(Value),
    Gte(Value),
    // ... more variants added in subsequent phases
}

impl Predicate {
    pub fn evaluate(&self, value: &Value) -> bool {
        match self {
            Predicate::Eq(target) => value_eq(value, target),
            // ...
        }
    }
}
```

**Acceptance Criteria**:
- [ ] `Predicate::Eq(Value::Int(42)).evaluate(&Value::Int(42))` returns true
- [ ] `Predicate::Lt(Value::Int(50)).evaluate(&Value::Int(30))` returns true
- [ ] Int/Float cross-comparison works correctly
- [ ] Unit tests for all comparison predicates pass

---

#### Phase 1.4: Predicate Enum - Range Variants
**File**: `src/query/predicate.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add range variants to `Predicate` enum:
   - `Between(Value, Value)` - inclusive bounds
   - `Inside(Value, Value)` - exclusive bounds
   - `Outside(Value, Value)` - outside range
2. Implement `evaluate()` logic for range predicates
3. Add unit tests

**Acceptance Criteria**:
- [ ] `Predicate::Between(Value::Int(10), Value::Int(20)).evaluate(&Value::Int(10))` returns true (inclusive)
- [ ] `Predicate::Between(Value::Int(10), Value::Int(20)).evaluate(&Value::Int(20))` returns true (inclusive)
- [ ] `Predicate::Inside(Value::Int(10), Value::Int(20)).evaluate(&Value::Int(10))` returns false (exclusive)
- [ ] `Predicate::Outside(Value::Int(10), Value::Int(20)).evaluate(&Value::Int(5))` returns true
- [ ] Unit tests pass

---

#### Phase 1.5: Predicate Enum - Collection Variants
**File**: `src/query/predicate.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Add collection variants to `Predicate` enum:
   - `Within(Vec<Value>)` - value is in collection
   - `Without(Vec<Value>)` - value is not in collection
2. Implement `evaluate()` logic
3. Add unit tests

**Acceptance Criteria**:
- [ ] `Predicate::Within(vec![...]).evaluate(&Value::Int(2))` returns true
- [ ] `Predicate::Without(vec![...]).evaluate(&Value::Int(4))` returns true
- [ ] Unit tests pass

---

#### Phase 1.6: Predicate Enum - String Variants
**File**: `src/query/predicate.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add string variants to `Predicate` enum:
   - `Containing(String)`
   - `StartingWith(String)`
   - `EndingWith(String)`
   - `Regex(String)`
2. Implement `evaluate()` logic for string predicates
3. Handle non-string values (return false)
4. Add unit tests

**Acceptance Criteria**:
- [ ] `Predicate::Containing("ello").evaluate(&Value::String("Hello"))` returns true
- [ ] `Predicate::StartingWith("He").evaluate(&Value::String("Hello"))` returns true
- [ ] `Predicate::EndingWith("lo").evaluate(&Value::String("Hello"))` returns true
- [ ] `Predicate::Regex(r"^\d+$").evaluate(&Value::String("123"))` returns true
- [ ] Non-string values return false
- [ ] Unit tests pass

---

#### Phase 1.7: Predicate Enum - Logical Variants
**File**: `src/query/predicate.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add logical variants to `Predicate` enum:
   - `And(Box<Predicate>, Box<Predicate>)`
   - `Or(Box<Predicate>, Box<Predicate>)`
   - `Not(Box<Predicate>)`
2. Implement `evaluate()` logic with short-circuit evaluation
3. Add `and()` and `or()` methods to `Predicate` for chaining
4. Add unit tests

**Acceptance Criteria**:
- [ ] `And` predicates require both to match
- [ ] `Or` predicates require at least one to match
- [ ] `Not` inverts the result
- [ ] Short-circuit evaluation works (And stops on first false, Or stops on first true)
- [ ] Unit tests pass

---

#### Phase 1.8: Predicate Builder Module (p)
**File**: `src/query/predicate.rs`  
**Duration**: 1 hour

**Tasks**:
1. Create `pub mod p` inside `predicate.rs`
2. Implement factory functions:
   - `eq()`, `neq()`, `lt()`, `lte()`, `gt()`, `gte()`
   - `between()`, `inside()`, `outside()`
   - `within()`, `without()`
   - `containing()`, `starting_with()`, `ending_with()`, `regex()`
   - `not()`
3. Use `impl Into<Value>` for ergonomic API
4. Add usage tests

**Code Structure**:
```rust
pub mod p {
    use super::Predicate;
    use crate::value::Value;

    pub fn eq(value: impl Into<Value>) -> Predicate {
        Predicate::Eq(value.into())
    }
    // ... etc
}
```

**Acceptance Criteria**:
- [ ] `p::eq(42)` creates `Predicate::Eq(Value::Int(42))`
- [ ] `p::between(10i64, 20i64)` works with type inference
- [ ] `p::within([1, 2, 3])` accepts iterables
- [ ] All factory functions return `Predicate`

---

### Day 2: Query IR Structure

#### Phase 2.1: QueryOp Enum - Source Operations
**File**: `src/query/ir.rs` (new file)  
**Duration**: 1 hour

**Tasks**:
1. Create `src/query/ir.rs`
2. Define `QueryOp` enum with source operation variants:
   - `AllVertices`
   - `Vertices(Vec<VertexId>)`
   - `AllEdges`
   - `Edges(Vec<EdgeId>)`
   - `Inject(Vec<Value>)`
3. Add necessary imports

**Acceptance Criteria**:
- [ ] All source variants compile
- [ ] `QueryOp` derives `Debug`, `Clone`
- [ ] Module compiles with correct imports

---

#### Phase 2.2: QueryOp Enum - Navigation Operations
**File**: `src/query/ir.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add navigation variants to `QueryOp`:
   - `ToVertex { direction: Direction, labels: Vec<String> }`
   - `ToEdge { direction: Direction, labels: Vec<String> }`
   - `OutVertex`
   - `InVertex`
   - `BothVertices`
2. Ensure `Direction` is imported from `types.rs`

**Acceptance Criteria**:
- [ ] Navigation variants compile with struct syntax
- [ ] Direction enum is used correctly
- [ ] Labels are optional (empty vec = all labels)

---

#### Phase 2.3: QueryOp Enum - Filter Operations
**File**: `src/query/ir.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Add filter variants to `QueryOp`:
   - `HasLabel(Vec<String>)`
   - `HasProperty(String)`
   - `HasValue { key: String, predicate: Predicate }`
   - `HasId(Vec<Value>)`
   - `Filter(Box<QueryPlan>)` (forward declaration needed)
   - `Dedup`, `DedupBy(Box<QueryPlan>)`
   - `Limit(usize)`, `Skip(usize)`, `Range(usize, usize)`
   - `Not(Box<QueryPlan>)`
   - `And(Vec<QueryPlan>)`, `Or(Vec<QueryPlan>)`
   - `Where(Box<QueryPlan>)`
   - `Is(Predicate)`

**Acceptance Criteria**:
- [ ] All filter variants compile
- [ ] `Predicate` is used for `HasValue` and `Is`
- [ ] Nested `QueryPlan` supported via `Box`

---

#### Phase 2.4: QueryOp Enum - Transform Operations
**File**: `src/query/ir.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add transform variants to `QueryOp`:
   - `Values(Vec<String>)`
   - `Id`, `Label`
   - `Constant(Value)`
   - `Path`
   - `As(String)`, `Select(Vec<String>)`
   - `Project { keys: Vec<String>, traversals: Vec<QueryPlan> }`
   - `Unfold`, `Fold`
   - `GroupBy { key_traversal: Box<QueryPlan>, value_traversal: Option<Box<QueryPlan>> }`
   - `Order { comparators: Vec<(QueryPlan, SortOrder)> }`

**Acceptance Criteria**:
- [ ] All transform variants compile
- [ ] Complex nested structures work (Project, GroupBy, Order)

---

#### Phase 2.5: QueryOp Enum - Branching and Terminal Operations
**File**: `src/query/ir.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add branching variants to `QueryOp`:
   - `Union(Vec<QueryPlan>)`
   - `Coalesce(Vec<QueryPlan>)`
   - `Optional(Box<QueryPlan>)`
   - `Repeat { traversal: Box<QueryPlan>, until: Option<Box<QueryPlan>>, emit: Option<Box<QueryPlan>>, times: Option<usize> }`
   - `Local(Box<QueryPlan>)`
2. Add side effect variants:
   - `Store(String)`, `Aggregate(String)`, `Cap(String)`
3. Add terminal variants:
   - `Count`, `Sum`, `Min`, `Max`, `Mean`

**Acceptance Criteria**:
- [ ] All branching variants compile
- [ ] All side effect variants compile
- [ ] All terminal variants compile

---

#### Phase 2.6: QueryPlan Structure
**File**: `src/query/ir.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Define `QueryPlan` struct with `ops: Vec<QueryOp>`
2. Implement `QueryPlan::new()` constructor
3. Implement `QueryPlan::single(op)` constructor
4. Implement `push()` method
5. Implement `with(op)` fluent builder method
6. Implement `is_empty()` and `len()` methods
7. Derive/implement `Default`, `Clone`, `Debug`

**Acceptance Criteria**:
- [ ] `QueryPlan::new()` creates empty plan
- [ ] `QueryPlan::single(op)` creates single-op plan
- [ ] Fluent building with `.with()` chains correctly
- [ ] `Clone` and `Default` work

---

### Day 3: IR Compiler and New Steps

#### Phase 3.1: CompileError Definition
**File**: `src/query/compiler.rs` (new file)  
**Duration**: 30 minutes

**Tasks**:
1. Create `src/query/compiler.rs`
2. Define `CompileError` enum using `thiserror`:
   - `UnsupportedOp(String)`
   - `InvalidRegex { pattern: String, source: regex::Error }`
   - `EmptyTraversal { context: String }`
   - `InvalidOp(String)`
3. Add necessary imports

**Acceptance Criteria**:
- [ ] All error variants compile
- [ ] Error messages are descriptive
- [ ] `#[error(...)]` attributes provide good formatting

---

#### Phase 3.2: IR Compiler - Source Operations
**File**: `src/query/compiler.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `QueryPlan::compile()` method returning `Result<Vec<Box<dyn AnyStep>>, CompileError>`
2. Implement `compile_op()` helper method
3. Compile source operations:
   - `AllVertices` → `StartStep::all_vertices()`
   - `Vertices(ids)` → `StartStep::vertices(ids)`
   - `AllEdges` → `StartStep::all_edges()`
   - `Edges(ids)` → `StartStep::edges(ids)`
   - `Inject(values)` → `StartStep::inject(values)`

**Acceptance Criteria**:
- [ ] `QueryPlan::single(QueryOp::AllVertices).compile()` returns one step
- [ ] Step names match expected values
- [ ] Empty plan compiles to empty vec

---

#### Phase 3.3: IR Compiler - Navigation Operations
**File**: `src/query/compiler.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Compile navigation operations:
   - `ToVertex { Out, labels }` → `OutStep`
   - `ToVertex { In, labels }` → `InStep`
   - `ToVertex { Both, labels }` → `BothStep`
   - `ToEdge { Out, labels }` → `OutEStep`
   - `ToEdge { In, labels }` → `InEStep`
   - `ToEdge { Both, labels }` → `BothEStep`
   - `OutVertex` → `OutVStep`
   - `InVertex` → `InVStep`
   - `BothVertices` → `BothVStep`

**Acceptance Criteria**:
- [ ] Direction correctly maps to Out/In/Both steps
- [ ] Labels are passed through correctly
- [ ] All nine navigation ops compile

---

#### Phase 3.4: IR Compiler - Basic Filter Operations
**File**: `src/query/compiler.rs`  
**Duration**: 1 hour

**Tasks**:
1. Compile basic filter operations:
   - `HasLabel(labels)` → `HasLabelStep`
   - `HasProperty(key)` → `HasStep`
   - `HasId(ids)` → `HasIdStep`
   - `Dedup` → `DedupStep`
   - `Limit(n)` → `LimitStep`
   - `Skip(n)` → `SkipStep`
   - `Range(start, end)` → `RangeStep`

**Acceptance Criteria**:
- [ ] All basic filter ops compile to correct steps
- [ ] Step names match expected values

---

#### Phase 3.5: HasPredicateStep Implementation
**File**: `src/traversal/filter.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `HasPredicateStep` struct with `key` and `predicate` fields
2. Implement `AnyStep` for `HasPredicateStep`
3. Extract property from vertex/edge and evaluate predicate
4. Handle missing properties (return false)
5. Add `has_predicate()` method to `BoundTraversal`

**Code Structure**:
```rust
#[derive(Clone, Debug)]
pub struct HasPredicateStep {
    key: String,
    predicate: crate::query::Predicate,
}

impl HasPredicateStep {
    pub fn new(key: impl Into<String>, predicate: crate::query::Predicate) -> Self {
        Self { key: key.into(), predicate }
    }
}
```

**Acceptance Criteria**:
- [ ] `HasPredicateStep::new("age", p::gte(18))` creates step
- [ ] Step filters vertices by property predicate
- [ ] Missing properties filter out traverser
- [ ] Non-element values filter out traverser
- [ ] Unit tests pass

---

#### Phase 3.6: IsStep Implementation
**File**: `src/traversal/filter.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `IsStep` struct with `predicate` field
2. Implement `AnyStep` for `IsStep`
3. Evaluate predicate against traverser's current value
4. Add `is()` method to `BoundTraversal`

**Acceptance Criteria**:
- [ ] `IsStep::new(p::gt(10))` creates step
- [ ] Step filters by evaluating predicate on current value
- [ ] Works with any `Value` type
- [ ] Unit tests pass

---

#### Phase 3.7: IR Compiler - Predicate Filter Operations
**File**: `src/query/compiler.rs`  
**Duration**: 1 hour

**Tasks**:
1. Compile predicate filter operations:
   - `HasValue { key, predicate }` → `HasPredicateStep`
   - `Is(predicate)` → `IsStep`
2. Integrate with existing basic filter compilation

**Acceptance Criteria**:
- [ ] `HasValue` compiles to `HasPredicateStep`
- [ ] `Is` compiles to `IsStep`
- [ ] Predicates are passed through correctly

---

### Day 3-4: Sub-traversal Steps and Branching

#### Phase 4.1: FilterTraversalStep Implementation
**File**: `src/traversal/filter.rs` or `src/traversal/branch.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `FilterTraversalStep` struct with compiled sub-steps
2. Implement `AnyStep` for `FilterTraversalStep`
3. Execute sub-traversal; emit traverser if any results produced
4. Implement `Clone` via `clone_box` pattern for sub-steps

**Acceptance Criteria**:
- [ ] Step keeps traversers where sub-traversal produces results
- [ ] Sub-traversal receives cloned input traverser
- [ ] Step is clonable
- [ ] Unit tests pass

---

#### Phase 4.2: NotStep Implementation
**File**: `src/traversal/filter.rs` or `src/traversal/branch.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `NotStep` struct with compiled sub-steps
2. Implement `AnyStep` for `NotStep`
3. Execute sub-traversal; emit traverser if NO results produced
4. Implement `Clone`

**Acceptance Criteria**:
- [ ] Step keeps traversers where sub-traversal produces NO results
- [ ] Inverse of FilterTraversalStep behavior
- [ ] Unit tests pass

---

#### Phase 4.3: AndStep and OrStep Implementation
**File**: `src/traversal/filter.rs` or `src/traversal/branch.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `AndStep` struct with `Vec<Vec<Box<dyn AnyStep>>>`
2. Implement `AnyStep` for `AndStep` (all sub-traversals must produce results)
3. Implement `OrStep` struct with `Vec<Vec<Box<dyn AnyStep>>>`
4. Implement `AnyStep` for `OrStep` (any sub-traversal must produce results)
5. Implement short-circuit evaluation

**Acceptance Criteria**:
- [ ] `AndStep` requires all sub-traversals to produce results
- [ ] `OrStep` requires any sub-traversal to produce results
- [ ] Short-circuit evaluation works correctly
- [ ] Unit tests pass

---

#### Phase 4.4: IR Compiler - Sub-traversal Filter Operations
**File**: `src/query/compiler.rs`  
**Duration**: 1 hour

**Tasks**:
1. Compile sub-traversal filter operations:
   - `Filter(sub)` → `FilterTraversalStep`
   - `Not(sub)` → `NotStep`
   - `And(subs)` → `AndStep`
   - `Or(subs)` → `OrStep`
   - `Where(sub)` → `FilterTraversalStep` (semantic alias)
2. Recursively compile sub-plans

**Acceptance Criteria**:
- [ ] Nested `QueryPlan` compiles recursively
- [ ] All sub-traversal filter ops compile correctly
- [ ] `Where` reuses `FilterTraversalStep`

---

#### Phase 4.5: UnionStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `UnionStep` struct with `Vec<Vec<Box<dyn AnyStep>>>`
2. Implement `AnyStep` for `UnionStep`
3. Collect input, execute all branches, merge all results
4. Implement `Clone`

**Acceptance Criteria**:
- [ ] Step merges results from all sub-traversals
- [ ] Input is shared across all branches
- [ ] Results maintain correct order
- [ ] Unit tests pass

---

#### Phase 4.6: CoalesceStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `CoalesceStep` struct with `Vec<Vec<Box<dyn AnyStep>>>`
2. Implement `AnyStep` for `CoalesceStep`
3. Try branches in order, return first non-empty result
4. Implement `Clone`

**Acceptance Criteria**:
- [ ] Step returns results from first non-empty branch
- [ ] Short-circuits after finding results
- [ ] Returns empty if all branches empty
- [ ] Unit tests pass

---

#### Phase 4.7: OptionalStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `OptionalStep` struct with `Vec<Box<dyn AnyStep>>`
2. Implement `AnyStep` for `OptionalStep`
3. Execute sub-traversal; return results or original if empty
4. Implement `Clone`

**Acceptance Criteria**:
- [ ] Step returns sub-traversal results if non-empty
- [ ] Step returns identity (original traverser) if sub-traversal empty
- [ ] Unit tests pass

---

#### Phase 4.8: LocalStep Implementation
**File**: `src/traversal/branch.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `LocalStep` struct with `Vec<Box<dyn AnyStep>>`
2. Implement `AnyStep` for `LocalStep`
3. Execute sub-traversal in isolated per-traverser scope
4. Implement `Clone`

**Acceptance Criteria**:
- [ ] Step executes sub-traversal per input traverser
- [ ] Results from each input are concatenated
- [ ] Unit tests pass

---

#### Phase 4.9: IR Compiler - Branching Operations
**File**: `src/query/compiler.rs`  
**Duration**: 1 hour

**Tasks**:
1. Compile branching operations:
   - `Union(subs)` → `UnionStep`
   - `Coalesce(subs)` → `CoalesceStep`
   - `Optional(sub)` → `OptionalStep`
   - `Local(sub)` → `LocalStep`
2. Recursively compile sub-plans

**Acceptance Criteria**:
- [ ] All branching ops compile correctly
- [ ] Sub-plans compile recursively

---

#### Phase 4.10: IR Compiler - Transform Operations
**File**: `src/query/compiler.rs`  
**Duration**: 1 hour

**Tasks**:
1. Compile transform operations:
   - `Values(keys)` → `ValuesStep`
   - `Id` → `IdStep`
   - `Label` → `LabelStep`
   - `Constant(value)` → `ConstantStep`
   - `Path` → `PathStep`
   - `As(label)` → `AsStep`
   - `Select(labels)` → `SelectStep`

**Acceptance Criteria**:
- [ ] All supported transform ops compile
- [ ] Step parameters are correct

---

#### Phase 4.11: IR Compiler - Unsupported Operations
**File**: `src/query/compiler.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Ensure unsupported ops return `CompileError::UnsupportedOp`:
   - `DedupBy`, `Project`, `Unfold`, `Fold`, `GroupBy`, `Order`
   - `Repeat`, `Store`, `Aggregate`, `Cap`
   - `Count`, `Sum`, `Min`, `Max`, `Mean`
2. Format error message with op debug representation

**Acceptance Criteria**:
- [ ] All Phase 2 ops return clear error messages
- [ ] Error messages indicate the unsupported operation

---

### Day 4: Integration, Testing, and Documentation

#### Phase 5.1: Predicate Unit Tests
**File**: `src/query/predicate.rs` (tests module)  
**Duration**: 1-2 hours

**Tasks**:
1. Add `#[cfg(test)] mod tests` to predicate.rs
2. Implement all predicate unit tests from spec section 7.1:
   - Equality tests (same value, null, neq)
   - Comparison tests (gt, lt, gte, lte)
   - Type coercion tests (Int/Float promotion)
   - Range tests (between, inside, outside)
   - Collection tests (within, without)
   - String tests (containing, starting_with, ending_with, regex)
   - Logical tests (and, or, not)
   - Type mismatch tests

**Acceptance Criteria**:
- [ ] All 20+ test cases from spec pass
- [ ] Edge cases covered (null, type mismatches, boundary values)
- [ ] 100% branch coverage on predicate evaluation

---

#### Phase 5.2: IR Compilation Unit Tests
**File**: `src/query/compiler.rs` (tests module)  
**Duration**: 1-2 hours

**Tasks**:
1. Add `#[cfg(test)] mod tests` to compiler.rs
2. Implement IR compilation tests from spec section 7.2:
   - `compile_empty_plan`
   - `compile_all_vertices`
   - `compile_navigation_chain`
   - `compile_predicate_filter`
   - `compile_nested_filter`
   - `compile_unsupported_op_returns_error`

**Acceptance Criteria**:
- [ ] All compilation tests pass
- [ ] Step names are verified
- [ ] Error cases are tested

---

#### Phase 5.3: Integration Tests
**File**: `tests/ir.rs` (new file) or `tests/traversal.rs`  
**Duration**: 2 hours

**Tasks**:
1. Create integration test file
2. Implement `create_test_graph()` helper
3. Add integration tests from spec section 7.3:
   - Build QueryPlan for `g.V().hasLabel("person").has("age", gte(30)).values("name")`
   - Compile to steps
   - Execute against test graph
   - Verify correct results
4. Add predicate builder API tests
5. Add combined predicate tests

**Acceptance Criteria**:
- [ ] End-to-end compilation and execution works
- [ ] Results match expected Gremlin semantics
- [ ] Predicate builder API is ergonomic

---

#### Phase 5.4: Module Re-exports and Prelude
**File**: `src/query/mod.rs`, `src/lib.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Verify all public types exported from `query` module:
   - `Direction`, `SortOrder`, `Scope`, `T`
   - `Predicate`, `p` module
   - `QueryOp`, `QueryPlan`
   - `CompileError`
2. Add query types to prelude in `src/lib.rs`
3. Document module with examples

**Acceptance Criteria**:
- [ ] `use interstellar::query::*` imports all types
- [ ] `use interstellar::query::p` works for predicates
- [ ] `use interstellar::prelude::*` includes query types

---

#### Phase 5.5: Documentation and Cleanup
**Duration**: 1-2 hours

**Tasks**:
1. Add doc comments to all public types in `types.rs`
2. Add doc comments to `Predicate` enum and all variants
3. Add doc comments to `QueryOp` enum and all variants
4. Add doc comments to `QueryPlan` struct and methods
5. Add doc comments to `CompileError`
6. Add module-level documentation with examples
7. Run `cargo clippy -- -D warnings` and fix issues
8. Run `cargo fmt`
9. Run `cargo test` and verify all pass
10. Run `cargo doc` and verify it builds

**Acceptance Criteria**:
- [ ] All public items have doc comments
- [ ] No clippy warnings
- [ ] Code is properly formatted
- [ ] All tests pass
- [ ] `cargo doc` builds without errors

---

## Exit Criteria Checklist

From spec section 10 "Acceptance Criteria":

### Predicates
- [ ] All 17 predicate variants implemented with `evaluate()`
- [ ] Int/Float type coercion works correctly
- [ ] Null handling follows documented semantics
- [ ] Logical predicates short-circuit correctly
- [ ] Builder module `p` provides ergonomic construction
- [ ] 100% branch coverage on predicate evaluation

### IR
- [ ] All QueryOp variants defined
- [ ] QueryPlan supports fluent building
- [ ] Compilation maps to existing steps where possible
- [ ] Nested traversals compile recursively
- [ ] Unsupported ops return clear errors

### New Steps
- [ ] HasPredicateStep filters by property predicate
- [ ] IsStep filters current value by predicate
- [ ] FilterTraversalStep/NotStep/AndStep/OrStep handle sub-traversals
- [ ] UnionStep merges multiple traversal results
- [ ] CoalesceStep returns first non-empty
- [ ] OptionalStep returns identity when empty
- [ ] All new steps are cloneable and thread-safe

### Integration
- [ ] Compiled QueryPlan executes correctly
- [ ] Results match expected Gremlin semantics
- [ ] Performance comparable to direct step construction

---

## File Summary

New files to create:
- `src/query/mod.rs` - Module entry, re-exports
- `src/query/types.rs` - Direction, SortOrder, Scope, T enums
- `src/query/predicate.rs` - Predicate enum, evaluate(), p module
- `src/query/ir.rs` - QueryOp enum, QueryPlan struct
- `src/query/compiler.rs` - CompileError, QueryPlan::compile()
- `tests/ir.rs` - Integration tests (optional, could use tests/traversal.rs)

Files to modify:
- `src/lib.rs` - Add `pub mod query;`, update prelude
- `src/traversal/filter.rs` - Add HasPredicateStep, IsStep
- `src/traversal/branch.rs` - Add FilterTraversalStep, NotStep, AndStep, OrStep, UnionStep, CoalesceStep, OptionalStep, LocalStep (some may exist from Phase 4)
- `src/traversal/mod.rs` - Re-export new step types
- `Cargo.toml` - Ensure `regex = "1.10"` dependency

---

## Dependencies

```toml
[dependencies]
regex = "1.10"  # For Predicate::Regex (may already exist from Phase 4)
```

Existing dependencies used:
- `thiserror` - CompileError
- All Phase 3/4 traversal infrastructure

---

## Implementation Notes

### Predicate vs Trait-based Predicates

Phase 4 introduced a `Predicate` trait for runtime predicate evaluation. This spec introduces a `Predicate` enum which:
1. Is easier to serialize/deserialize for IR storage
2. Has explicit variants for all supported operations
3. Can be pattern-matched for optimization
4. Is simpler to clone and store in IR structures

The two approaches can coexist:
- The `Predicate` enum is used in `QueryOp` IR nodes
- The existing `Predicate` trait (if still needed) can wrap the enum
- `HasPredicateStep` uses the enum directly

### Step Clone Pattern

All new steps must implement `Clone`. For steps holding `Vec<Box<dyn AnyStep>>`, implement clone via:

```rust
impl Clone for MyStep {
    fn clone(&self) -> Self {
        Self {
            sub_steps: self.sub_steps.iter().map(|s| s.clone_box()).collect(),
        }
    }
}
```

### execute_traversal Helper

The `execute_traversal` function from Phase 3 is essential for sub-traversal execution:

```rust
fn execute_traversal<'a>(
    ctx: &'a ExecutionContext<'a>,
    steps: &[Box<dyn AnyStep>],
    input: Box<dyn Iterator<Item = Traverser> + 'a>,
) -> Box<dyn Iterator<Item = Traverser> + 'a>
```

This function is used extensively in FilterTraversalStep, NotStep, AndStep, OrStep, and all branching steps.
