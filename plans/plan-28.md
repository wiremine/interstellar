# Plan 28: Rhai Scripting Integration

## Overview

This plan implements Rhai scripting integration as specified in `specs/spec-28-rhai-scripting.md`. Rhai is an embedded scripting language that will expose Intersteller's Gremlin-style traversal API to scripts, enabling interactive exploration and dynamic query construction.

**Estimated effort**: 3-4 days  
**Complexity**: Medium (new module, no changes to existing code)  
**Dependencies**: Spec 03 (Traversal Engine), Spec 07 (Math), Spec 10 (Mutations)

## Implementation Phases

### Phase 1: Feature Flag & Dependencies (1 hour)

**Goal**: Add Rhai as an optional dependency with feature flag.

#### Tasks

1. **Update Cargo.toml** with rhai dependency
   ```toml
   [features]
   rhai = ["dep:rhai"]

   [dependencies]
   rhai = { version = "1.19", optional = true, features = ["sync"] }
   ```

2. **Add conditional module** in `src/lib.rs`
   ```rust
   #[cfg(feature = "rhai")]
   pub mod rhai;
   ```

3. **Create module structure**
   ```
   src/rhai/
   ├── mod.rs
   ├── engine.rs
   ├── types.rs
   ├── traversal.rs
   ├── predicates.rs
   ├── anonymous.rs
   └── error.rs
   ```

#### Files Created/Modified
- `Cargo.toml`
- `src/lib.rs`
- `src/rhai/mod.rs` (stub)

#### Verification
- `cargo check --features rhai` passes
- `cargo check` (without feature) still passes

---

### Phase 2: Error Types (1-2 hours)

**Goal**: Define error types for Rhai integration.

#### Tasks

1. **Create `src/rhai/error.rs`**
   ```rust
   use rhai::EvalAltResult;
   use thiserror::Error;

   #[derive(Debug, Error)]
   pub enum RhaiError {
       #[error("script compilation failed: {0}")]
       Compile(String),

       #[error("script execution failed: {0}")]
       Execution(String),

       #[error("traversal error: {0}")]
       Traversal(#[from] crate::error::TraversalError),

       #[error("storage error: {0}")]
       Storage(#[from] crate::error::StorageError),

       #[error("type error: expected {expected}, got {actual}")]
       Type { expected: String, actual: String },

       #[error("missing argument: {0}")]
       MissingArgument(String),
   }

   impl From<Box<EvalAltResult>> for RhaiError { ... }
   impl From<rhai::ParseError> for RhaiError { ... }

   pub type RhaiResult<T> = Result<T, RhaiError>;
   ```

#### Files Created
- `src/rhai/error.rs`

#### Verification
- `cargo check --features rhai` passes

---

### Phase 3: Core Type Registrations (2-3 hours)

**Goal**: Register `Value`, `VertexId`, `EdgeId` with Rhai.

#### Tasks

1. **Create `src/rhai/types.rs`**

2. **Register VertexId**
   - Constructor: `vertex_id(123)`
   - Getter: `.id`
   - Display: `to_string()`

3. **Register EdgeId**
   - Constructor: `edge_id(123)`
   - Getter: `.id`
   - Display: `to_string()`

4. **Register Value**
   - Constructors: `value_int()`, `value_float()`, `value_string()`, `value_bool()`, `value_null()`
   - Type checks: `is_int()`, `is_float()`, `is_string()`, `is_bool()`, `is_null()`, `is_list()`, `is_map()`
   - Extractors: `as_int()`, `as_float()`, `as_string()`, `as_bool()`

5. **Add dynamic_to_value helper** (public for use in other modules)
   ```rust
   pub fn dynamic_to_value(d: Dynamic) -> Value { ... }
   ```

6. **Add value_to_dynamic helper**
   ```rust
   pub fn value_to_dynamic(v: Value) -> Dynamic { ... }
   ```

#### Files Created
- `src/rhai/types.rs`

#### Verification
- Unit tests for type conversions
- Round-trip tests (Value -> Dynamic -> Value)

---

### Phase 4: Predicate Bindings (2-3 hours)

**Goal**: Register predicate functions as global functions.

#### Tasks

1. **Create `src/rhai/predicates.rs`**

2. **Register comparison predicates**
   - `eq(value)`, `neq(value)`
   - `lt(value)`, `lte(value)`
   - `gt(value)`, `gte(value)`

3. **Register range predicates**
   - `between(low, high)`
   - `inside(low, high)`
   - `outside(low, high)`

4. **Register collection predicates**
   - `within([values])`
   - `without([values])`

5. **Register text predicates**
   - `containing(s)`
   - `starting_with(s)`
   - `ending_with(s)`
   - `regex(pattern)`

6. **Register logical combinators** (with `pred_` prefix to avoid Rhai conflicts)
   - `pred_not(pred)`
   - `pred_and(p1, p2)`
   - `pred_or(p1, p2)`

7. **Export dynamic_to_value** for use by other modules

#### Files Created
- `src/rhai/predicates.rs`

#### Verification
- Test each predicate type
- Test logical combinators
- Test with traversal `has_where()`

---

### Phase 5: Traversal Wrapper Types (3-4 hours)

**Goal**: Create wrapper types for traversals that work with Rhai's type system.

#### Tasks

1. **Create `src/rhai/traversal.rs`**

2. **Create RhaiTraversal wrapper**
   ```rust
   #[derive(Clone)]
   pub struct RhaiTraversal {
       inner: Arc<parking_lot::Mutex<Option<BoundTraversal<Value>>>>,
   }
   ```
   - `new(traversal)` - wrap a traversal
   - `take()` - consume the traversal (returns error if already consumed)

3. **Create RhaiTraversalSource wrapper**
   ```rust
   #[derive(Clone)]
   pub struct RhaiTraversalSource<S: GraphStorage> {
       snapshot: Arc<GraphSnapshot<S>>,
   }
   ```
   - `new(snapshot)` - wrap a snapshot
   - `source()` - create new GraphTraversalSource

4. **Register source steps**
   - `v()` - all vertices
   - `v(id)` - single vertex by ID
   - `v_ids([ids])` - multiple vertices by ID
   - `e()` - all edges

#### Files Created
- `src/rhai/traversal.rs`

#### Verification
- Test source step creation
- Test traversal consumption error

---

### Phase 6: Navigation Step Bindings (2-3 hours)

**Goal**: Register navigation steps on RhaiTraversal.

#### Tasks

1. **Register outbound navigation**
   - `out()`, `out(label)`
   - `out_e()`, `out_e(label)`
   - `out_v()`

2. **Register inbound navigation**
   - `in_()`, `in_(label)`
   - `in_e()`, `in_e(label)`
   - `in_v()`

3. **Register bidirectional navigation**
   - `both()`, `both(label)`
   - `both_e()`, `both_e(label)`
   - `other_v()`

#### Files Modified
- `src/rhai/traversal.rs`

#### Verification
- Test each navigation step
- Test with labels
- Test chained navigation

---

### Phase 7: Filter Step Bindings (2-3 hours)

**Goal**: Register filter steps on RhaiTraversal.

#### Tasks

1. **Register label filters**
   - `has_label(label)`
   - `has_label_any([labels])`

2. **Register property filters**
   - `has(key)` - property exists
   - `has_value(key, value)` - property equals value
   - `has_where(key, predicate)` - property matches predicate
   - `has_not(key)` - property doesn't exist
   - `has_id(id)` - element has specific ID

3. **Register deduplication/limiting**
   - `dedup()`
   - `limit(n)`
   - `skip(n)`
   - `range(start, end)`

4. **Register value filters**
   - `is_eq(value)` - value equals
   - `is_(predicate)` - value matches predicate

5. **Register path filters**
   - `simple_path()`
   - `cyclic_path()`

#### Files Modified
- `src/rhai/traversal.rs`

#### Verification
- Test each filter step
- Test filters with predicates
- Test combined filters

---

### Phase 8: Transform Step Bindings (2-3 hours)

**Goal**: Register transform steps on RhaiTraversal.

#### Tasks

1. **Register element accessors**
   - `id()` - get element ID
   - `label()` - get element label
   - `values(key)` - get property value
   - `values_multi([keys])` - get multiple property values

2. **Register map steps**
   - `value_map()` - all properties as map
   - `element_map()` - element as map (with id, label)
   - `path()` - traversal path

3. **Register value transforms**
   - `constant(value)` - replace with constant
   - `identity()` - pass through unchanged

4. **Register collection transforms**
   - `fold()` - collect to list
   - `unfold()` - expand list to elements

#### Files Modified
- `src/rhai/traversal.rs`

#### Verification
- Test each transform step
- Test property extraction
- Test fold/unfold round-trip

---

### Phase 9: Modulator & Terminal Steps (2-3 hours)

**Goal**: Register modulator and terminal steps.

#### Tasks

1. **Register modulator steps**
   - `as_(label)` - label current position
   - `select([labels])` - select labeled positions
   - `select_one(label)` - select single labeled position

2. **Register terminal steps**
   - `to_list()` / `list()` - collect all results
   - `next()` - get first result (or unit)
   - `one()` - get exactly one result (or unit)
   - `count()` - count results
   - `has_next()` - check if results exist
   - `iterate()` - consume without returning

3. **Implement value_to_dynamic conversion**
   - Handle all Value variants
   - Convert lists and maps recursively
   - Format Vertex/Edge as strings

#### Files Modified
- `src/rhai/traversal.rs`

#### Verification
- Test each terminal step
- Test as_/select round-trip
- Test empty traversal behavior

---

### Phase 10: Anonymous Traversal Factory (3-4 hours)

**Goal**: Create the `__` factory object for anonymous traversals.

#### Tasks

1. **Create `src/rhai/anonymous.rs`**

2. **Create AnonymousTraversalFactory struct**
   ```rust
   #[derive(Clone)]
   pub struct AnonymousTraversalFactory;
   ```

3. **Create RhaiAnonymousTraversal wrapper**
   ```rust
   #[derive(Clone)]
   pub struct RhaiAnonymousTraversal {
       factory: Arc<dyn Fn() -> Traversal<Value, Value> + Send + Sync>,
   }
   ```

4. **Register navigation methods on factory**
   - `__.out()`, `__.out(label)`
   - `__.in_()`, `__.in_(label)`
   - `__.both()`
   - `__.out_e()`, `__.in_e()`
   - `__.out_v()`, `__.in_v()`

5. **Register filter methods on factory**
   - `__.has_label(label)`
   - `__.has(key)`, `__.has_value(key, value)`, `__.has_not(key)`
   - `__.limit(n)`, `__.dedup()`

6. **Register transform methods on factory**
   - `__.id()`, `__.label()`, `__.values(key)`
   - `__.identity()`, `__.constant(value)`
   - `__.path()`, `__.fold()`, `__.unfold()`

7. **Register modulator methods**
   - `__.as_(label)`

8. **Create factory function**
   ```rust
   pub fn create_anonymous_factory() -> AnonymousTraversalFactory
   ```

#### Files Created
- `src/rhai/anonymous.rs`

#### Verification
- Test each factory method
- Test anonymous traversal in `where_()` step
- Test chained anonymous traversals

---

### Phase 11: RhaiEngine Builder (2-3 hours)

**Goal**: Create the main RhaiEngine that ties everything together.

#### Tasks

1. **Create `src/rhai/engine.rs`**

2. **Create RhaiEngine struct**
   ```rust
   pub struct RhaiEngine {
       engine: Engine,
   }
   ```

3. **Implement RhaiEngine::new()**
   - Create Rhai Engine
   - Register all types
   - Register all predicates
   - Register anonymous traversal methods

4. **Implement RhaiEngine::with_engine()**
   - Accept pre-configured Engine
   - Add Intersteller bindings

5. **Implement eval_with_graph()**
   - Clone engine
   - Register storage-specific traversal bindings
   - Create scope with `graph` and `__` bindings
   - Execute script

6. **Implement eval_ast_with_graph()**
   - Same as above but with pre-compiled AST

7. **Implement compile()**
   - Compile script to AST for caching

8. **Implement eval()**
   - Execute without graph (for testing predicates)

9. **Implement Clone and Default traits**

#### Files Created
- `src/rhai/engine.rs`

#### Verification
- Test engine creation
- Test script compilation
- Test script execution with graph
- Test pre-compiled AST execution

---

### Phase 12: Module Root & Public API (1-2 hours)

**Goal**: Finalize module exports and documentation.

#### Tasks

1. **Update `src/rhai/mod.rs`**
   - Add module documentation
   - Export public types:
     - `RhaiEngine`
     - `RhaiError`, `RhaiResult`
     - `RhaiTraversal`, `RhaiTraversalSource`
     - `RhaiAnonymousTraversal`

2. **Add inline documentation**
   - Quick start example
   - Predicate usage examples
   - Anonymous traversal examples

3. **Verify public API surface**
   - Only necessary types are public
   - Internal helpers are crate-private

#### Files Modified
- `src/rhai/mod.rs`

#### Verification
- Documentation renders correctly
- Only intended types are public

---

### Phase 13: Integration Testing (4-6 hours)

**Goal**: Comprehensive test coverage.

#### Tasks

1. **Create test file structure**
   ```
   tests/rhai/
   ├── mod.rs
   ├── types.rs
   ├── predicates.rs
   ├── traversal.rs
   ├── anonymous.rs
   └── errors.rs
   ```

2. **Type conversion tests**
   - Value creation and extraction
   - VertexId/EdgeId creation
   - Round-trip conversions

3. **Predicate tests**
   - Each predicate type
   - Logical combinators
   - Integration with has_where()

4. **Traversal tests**
   - Source steps (v, e)
   - Navigation (out, in_, both)
   - Filters (has_label, has, dedup, limit)
   - Transforms (values, value_map, path)
   - Terminals (list, count, next)

5. **Anonymous traversal tests**
   - Factory methods
   - Use in where_()
   - Chained anonymous traversals

6. **Error handling tests**
   - Compile errors
   - Runtime errors
   - Type errors
   - Traversal consumed errors

7. **Integration tests**
   - Full query scenarios
   - Social graph traversals
   - Property graph queries

#### Files Created
- `tests/rhai/*.rs`

#### Verification
- All tests pass
- Coverage >= 80%

---

### Phase 14: Documentation & Examples (2-3 hours)

**Goal**: User-facing documentation and examples.

#### Tasks

1. **Create example file** `examples/rhai_scripting.rs`
   - Basic script execution
   - Predicate usage
   - Anonymous traversals
   - Pre-compiled scripts

2. **Update README** (if needed)
   - Mention Rhai feature flag
   - Link to examples

3. **Rustdoc cleanup**
   - Ensure all public items documented
   - Add usage examples in doc comments

#### Files Created/Modified
- `examples/rhai_scripting.rs`
- `README.md` (optional)

#### Verification
- Example compiles and runs
- `cargo doc --features rhai` succeeds

---

## Test Plan

### Unit Tests

| Test | Description |
|------|-------------|
| `type_value_int` | Create and extract integer Value |
| `type_value_string` | Create and extract string Value |
| `type_vertex_id` | Create VertexId, access .id |
| `type_round_trip` | Value -> Dynamic -> Value |
| `pred_eq` | Equality predicate |
| `pred_gt_lt` | Greater/less than predicates |
| `pred_within` | Collection membership |
| `pred_between` | Range predicate |
| `pred_and_or` | Logical combinators |

### Integration Tests

| Test | Description | Expected |
|------|-------------|----------|
| `traversal_v_count` | Count all vertices | Integer count |
| `traversal_out_in` | Navigate out then in | Original vertices |
| `traversal_has_label` | Filter by label | Matching vertices |
| `traversal_has_where` | Filter with predicate | Matching vertices |
| `traversal_values` | Extract property values | Property values |
| `traversal_path` | Get traversal path | Path objects |
| `anonymous_where` | where_(__.out()) | Filtered vertices |
| `anonymous_chain` | __.out().has_label() | Combined filter |
| `script_social` | Full social graph query | Expected results |

### Error Tests

| Test | Description | Expected Error |
|------|-------------|----------------|
| `compile_error` | Invalid syntax | RhaiError::Compile |
| `runtime_error` | Undefined function | RhaiError::Execution |
| `traversal_consumed` | Reuse consumed traversal | Execution error |
| `type_mismatch` | Wrong type extraction | Execution error |

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Rhai version compatibility | Low | Medium | Pin to specific version |
| Performance overhead | Medium | Low | Document as scripting layer |
| Type conversion edge cases | Medium | Medium | Comprehensive round-trip tests |
| Clone overhead for traversals | Low | Low | Arc wrapper minimizes copies |

---

## Success Criteria

- [ ] Feature flag `rhai` compiles and tests pass
- [ ] All core types registered: `Value`, `VertexId`, `EdgeId`
- [ ] All predicates available as global functions
- [ ] All anonymous traversal factories available via `__` object
- [ ] Navigation steps work: `out`, `in_`, `both`, etc.
- [ ] Filter steps work: `has_label`, `has`, `has_where`, `dedup`, `limit`, `skip`, `range`
- [ ] Transform steps work: `id`, `label`, `values`, `value_map`, `element_map`, `path`, `fold`, `unfold`
- [ ] Terminal steps work: `to_list`/`list`, `next`, `one`, `count`, `has_next`, `iterate`
- [ ] Modulator steps work: `as_`, `select`, `select_one`
- [ ] Error messages are clear and actionable
- [ ] Documentation includes examples for common use cases
- [ ] Test coverage >= 80% for the rhai module
- [ ] Example file runs successfully

---

## Future Work (Out of Scope)

1. **REPL Implementation**: Interactive shell with history
2. **Mutation Support**: `add_v()`, `add_e()`, `property()`, `drop()`
3. **Custom Functions**: User-registered Rhai functions
4. **Script Caching**: LRU cache for compiled ASTs
5. **GQL Interop**: `graph.gql("MATCH ...")` from scripts
6. **Async Execution**: Non-blocking script execution
