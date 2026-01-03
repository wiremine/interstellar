# Plan 07: Completing the Gremlin API

**Phase 7 of RustGremlin Implementation**

Based on: `specs/spec-07-finish-api.md`

---

## Overview

This plan breaks down the implementation of the remaining 15 Gremlin API steps into granular, testable phases. Each phase represents approximately 1-2 hours of focused work and includes specific acceptance criteria.

**Total Duration**: 2-3 weeks  
**Current State**: Phases 1-6 are complete. The traversal engine, anonymous traversals, predicates, and branch steps are fully implemented.

**Key Architectural Points**:
1. All steps implement the `AnyStep` trait with type-erased execution
2. Filter steps use the `impl_filter_step!` macro where applicable
3. FlatMap steps use the `impl_flatmap_step!` macro where applicable
4. Barrier steps collect all input before producing output
5. Builder patterns are used for complex steps with modulators

---

## Dependencies

Optional dependency for `math()` step expression parsing:

```toml
[dependencies]
meval = "0.2"  # Optional - for math() expression parsing
```

---

## Implementation Order

### Week 1: Filter Steps and Navigation

#### Phase 1.1: HasNotStep Implementation
**File**: `src/traversal/filter.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `HasNotStep` struct with `key: String` field
2. Implement `matches()` method checking property absence
3. Handle vertices, edges, and non-element values
4. Use `impl_filter_step!` macro for `AnyStep` implementation
5. Add `has_not()` method to `Traversal`
6. Add `__::has_not()` factory function

**Code Structure**:
```rust
#[derive(Clone, Debug)]
pub struct HasNotStep {
    key: String,
}

impl HasNotStep {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                ctx.snapshot()
                    .storage()
                    .get_vertex(*id)
                    .map(|v| !v.properties.contains_key(&self.key))
                    .unwrap_or(true)
            }
            Value::Edge(id) => {
                ctx.snapshot()
                    .storage()
                    .get_edge(*id)
                    .map(|e| !e.properties.contains_key(&self.key))
                    .unwrap_or(true)
            }
            _ => true, // Non-elements pass through
        }
    }
}

impl_filter_step!(HasNotStep, "hasNot");
```

**Acceptance Criteria**:
- [x] `has_not("email")` filters vertices WITH email property
- [x] `has_not("weight")` filters edges WITH weight property
- [x] Non-element values (strings, integers) pass through
- [x] Vertices/edges without the property pass through
- [x] Unit tests pass

---

#### Phase 1.2: IsStep Implementation
**File**: `src/traversal/filter.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `IsStep` struct holding `Box<dyn Predicate>`
2. Implement `matches()` method evaluating predicate against current value
3. Implement `IsStep::eq()` convenience constructor
4. Use `impl_filter_step!` macro for `AnyStep` implementation
5. Add `is_eq()` and `is_()` methods to `Traversal`
6. Add `__::is_eq()` and `__::is_()` factory functions

**Code Structure**:
```rust
#[derive(Clone)]
pub struct IsStep {
    predicate: Box<dyn Predicate>,
}

impl IsStep {
    pub fn new(predicate: impl Predicate + Clone + Send + Sync + 'static) -> Self {
        Self { predicate: Box::new(predicate) }
    }

    pub fn eq(value: impl Into<Value>) -> Self {
        Self::new(p::eq(value))
    }

    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        self.predicate.test(&traverser.value)
    }
}

impl_filter_step!(IsStep, "is");
```

**Acceptance Criteria**:
- [x] `is_eq(29)` filters to value == 29
- [x] `is_(p::gt(25))` filters to values > 25
- [x] `is_(p::between(20, 40))` filters range correctly
- [x] Works with Integer, Float, String values
- [x] Unit tests pass

---

#### Phase 1.3: SimplePathStep Implementation
**File**: `src/traversal/filter.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `SimplePathStep` struct (no fields needed)
2. Implement `matches()` method checking for unique path elements
3. Use `HashSet` to detect duplicates in path
4. Handle `PathValue` comparison/hashing
5. Use `impl_filter_step!` macro for `AnyStep` implementation
6. Add `simple_path()` method to `Traversal`
7. Add `__::simple_path()` factory function

**Note**: May require implementing `Hash` and `Eq` for `Value` if not already present.

**Code Structure**:
```rust
#[derive(Clone, Debug, Default)]
pub struct SimplePathStep;

impl SimplePathStep {
    pub fn new() -> Self { Self }

    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        let elements = traverser.path.elements();
        let mut seen = std::collections::HashSet::new();
        for element in elements {
            if !seen.insert(element.value.clone()) {
                return false; // Duplicate found
            }
        }
        true
    }
}

impl_filter_step!(SimplePathStep, "simplePath");
```

**Acceptance Criteria**:
- [x] `simple_path()` filters out paths with repeated vertices
- [x] Linear paths (A -> B -> C -> D) pass through
- [x] Cyclic paths (A -> B -> C -> A) are filtered out
- [x] Works correctly with `repeat()` step
- [x] Unit tests pass

---

#### Phase 1.4: CyclicPathStep Implementation
**File**: `src/traversal/filter.rs`  
**Duration**: 30 minutes

**Tasks**:
1. Implement `CyclicPathStep` struct (inverse of SimplePathStep)
2. Implement `matches()` method detecting duplicate path elements
3. Use `impl_filter_step!` macro for `AnyStep` implementation
4. Add `cyclic_path()` method to `Traversal`
5. Add `__::cyclic_path()` factory function

**Acceptance Criteria**:
- [x] `cyclic_path()` keeps paths with repeated vertices
- [x] Linear paths are filtered out
- [x] Cyclic paths pass through
- [x] Inverse behavior of `simple_path()`
- [x] Unit tests pass

---

#### Phase 1.5: OtherVStep Implementation
**File**: `src/traversal/navigation.rs`  
**Duration**: 2 hours

**Tasks**:
1. Implement `OtherVStep` struct (no fields needed)
2. Implement `AnyStep::apply()` method manually (not a simple filter or flatmap)
3. Look up edge endpoints from storage
4. Inspect path to find previous vertex (the one we came from)
5. Return the "other" endpoint of the edge
6. Handle edge cases (not on edge, path too short)
7. Add `other_v()` method to `Traversal`
8. Add `__::other_v()` factory function

**Code Structure**:
```rust
#[derive(Clone, Debug, Default)]
pub struct OtherVStep;

impl OtherVStep {
    pub fn new() -> Self { Self }

    fn get_other_vertex(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Option<VertexId> {
        let edge_id = match &traverser.value {
            Value::Edge(id) => *id,
            _ => return None,
        };
        
        let edge = ctx.snapshot().storage().get_edge(edge_id)?;
        
        // Find source vertex from path
        let path_elements = traverser.path.elements();
        if path_elements.len() < 2 {
            return Some(edge.out_vertex); // Fallback
        }
        
        let prev_element = &path_elements[path_elements.len() - 2];
        match &prev_element.value {
            Value::Vertex(prev_id) => {
                if *prev_id == edge.out_vertex {
                    Some(edge.in_vertex)
                } else if *prev_id == edge.in_vertex {
                    Some(edge.out_vertex)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl AnyStep for OtherVStep {
    fn apply<'a>(...) -> ... {
        Box::new(input.filter_map(move |t| {
            self.get_other_vertex(ctx, &t).map(|vid| t.with_value(Value::Vertex(vid)))
        }))
    }
    // ...
}
```

**Acceptance Criteria**:
- [x] `outE().other_v()` returns the in-vertex
- [x] `inE().other_v()` returns the out-vertex
- [x] `bothE().other_v()` returns the opposite vertex
- [x] Non-edge values are filtered out
- [x] Unit tests pass

---

### Week 1-2: Transform Steps

#### Phase 2.1: PropertiesStep Implementation
**File**: `src/traversal/transform.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `PropertiesStep` struct with `keys: Option<Vec<String>>`
2. Implement `get_properties()` method returning `Vec<Value>`
3. Create property values as `Value::Map` with "key" and "value" entries
4. Use `impl_flatmap_step!` macro for `AnyStep` implementation
5. Add `properties()` and `properties_keys()` methods to `Traversal`
6. Add `__::properties()` factory function

**Code Structure**:
```rust
#[derive(Clone, Debug)]
pub struct PropertiesStep {
    keys: Option<Vec<String>>,
}

impl PropertiesStep {
    pub fn new() -> Self { Self { keys: None } }
    pub fn with_keys(keys: Vec<String>) -> Self { Self { keys: Some(keys) } }

    fn get_properties(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Vec<Value> {
        // Extract properties from vertex/edge, return as Value::Map entries
    }
}

impl_flatmap_step!(PropertiesStep, "properties", get_properties);
```

**Acceptance Criteria**:
- [ ] `properties()` returns all properties as key-value maps
- [ ] `properties_keys(&["name"])` returns only specified properties
- [ ] Each property is `Value::Map { key: String, value: Value }`
- [ ] Non-elements produce no output
- [ ] Unit tests pass

---

#### Phase 2.2: ValueMapStep Implementation
**File**: `src/traversal/transform.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `ValueMapStep` struct with `keys: Option<Vec<String>>` and `include_tokens: bool`
2. Implement `transform()` method returning `Value::Map`
3. Wrap property values in `Value::List` for multi-property compatibility
4. Optionally include "id" and "label" tokens
5. Implement `AnyStep` manually (1:1 map)
6. Add `value_map()`, `value_map_keys()`, `value_map_with_tokens()` methods to `Traversal`
7. Add `__::value_map()` factory function

**Acceptance Criteria**:
- [ ] `value_map()` returns `{name: [value], age: [value]}`
- [ ] `value_map_keys(&["name"])` returns only specified keys
- [ ] `value_map_with_tokens()` includes "id" and "label"
- [ ] Values are wrapped in lists
- [ ] Unit tests pass

---

#### Phase 2.3: ElementMapStep Implementation
**File**: `src/traversal/transform.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `ElementMapStep` struct with `keys: Option<Vec<String>>`
2. Implement `transform()` method returning `Value::Map`
3. Always include "id" and "label"
4. For edges, include "IN" and "OUT" vertex references
5. Property values are NOT wrapped in lists
6. Implement `AnyStep` manually (1:1 map)
7. Add `element_map()` and `element_map_keys()` methods to `Traversal`
8. Add `__::element_map()` factory function

**Acceptance Criteria**:
- [ ] `element_map()` for vertices includes id, label, and properties
- [ ] `element_map()` for edges includes id, label, IN, OUT, and properties
- [ ] IN/OUT are vertex reference maps with id and label
- [ ] Property values are NOT wrapped in lists
- [ ] Unit tests pass

---

#### Phase 2.4: UnfoldStep Implementation
**File**: `src/traversal/transform.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `UnfoldStep` struct (no fields needed)
2. Implement `unfold()` method returning `Vec<Value>`
3. Handle `Value::List` - return each element
4. Handle `Value::Map` - return each entry as single-entry map
5. Non-collections pass through unchanged
6. Use `impl_flatmap_step!` macro for `AnyStep` implementation
7. Add `unfold()` method to `Traversal`
8. Add `__::unfold()` factory function

**Acceptance Criteria**:
- [ ] `fold().unfold()` returns original elements
- [ ] `value_map().unfold()` returns individual property entries
- [ ] Non-collection values pass through unchanged
- [ ] Unit tests pass

---

#### Phase 2.5: MeanStep Implementation
**File**: `src/traversal/transform.rs`  
**Duration**: 1 hour

**Tasks**:
1. Implement `MeanStep` struct (no fields needed)
2. Implement `AnyStep::apply()` as a barrier step
3. Collect all input, sum numeric values, calculate average
4. Ignore non-numeric values
5. Return empty if no numeric values
6. Add `mean()` method to `Traversal`
7. Add `__::mean()` factory function

**Code Structure**:
```rust
#[derive(Clone, Debug, Default)]
pub struct MeanStep;

impl AnyStep for MeanStep {
    fn apply<'a>(&'a self, _ctx: &'a ExecutionContext<'a>, input: Box<dyn Iterator<Item = Traverser> + 'a>) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let mut sum = 0.0_f64;
        let mut count = 0_u64;
        let mut last_path = None;

        for t in input {
            last_path = Some(t.path.clone());
            match &t.value {
                Value::Integer(n) => { sum += *n as f64; count += 1; }
                Value::Float(f) => { sum += *f; count += 1; }
                _ => {}
            }
        }

        if count == 0 {
            Box::new(std::iter::empty())
        } else {
            let mean = sum / count as f64;
            Box::new(std::iter::once(Traverser {
                value: Value::Float(mean),
                path: last_path.unwrap_or_default(),
                ..Default::default()
            }))
        }
    }
    // ...
}
```

**Acceptance Criteria**:
- [ ] `values("age").mean()` returns average age
- [ ] Non-numeric values are ignored
- [ ] Empty input returns no results
- [ ] Result is `Value::Float`
- [ ] Unit tests pass

---

#### Phase 2.6: OrderStep Implementation
**File**: `src/traversal/transform.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Define `Order` enum (Asc, Desc)
2. Define `OrderKey` enum (Natural, Property, Traversal)
3. Implement `OrderStep` struct with `keys: Vec<OrderKey>`
4. Implement comparison helper methods
5. Implement `AnyStep::apply()` as a barrier step (collect, sort, emit)
6. Create `OrderBuilder` for fluent configuration
7. Add `order()` method to `Traversal` returning `OrderBuilder`

**Code Structure**:
```rust
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Order { Asc, Desc }

#[derive(Clone)]
pub enum OrderKey {
    Natural(Order),
    Property(String, Order),
    Traversal(Traversal<Value, Value>, Order),
}

#[derive(Clone)]
pub struct OrderStep {
    keys: Vec<OrderKey>,
}

pub struct OrderBuilder<In> {
    steps: Vec<Box<dyn AnyStep>>,
    order_keys: Vec<OrderKey>,
    _phantom: PhantomData<In>,
}

impl<In> OrderBuilder<In> {
    pub fn by_asc(mut self) -> Self { ... }
    pub fn by_desc(mut self) -> Self { ... }
    pub fn by_key_asc(mut self, key: &str) -> Self { ... }
    pub fn by_key_desc(mut self, key: &str) -> Self { ... }
    pub fn build(self) -> Traversal<In, Value> { ... }
}
```

**Acceptance Criteria**:
- [ ] `order().build()` sorts by natural order ascending
- [ ] `order().by_desc().build()` sorts descending
- [ ] `order().by_key_asc("age").build()` sorts by property
- [ ] `order().by_key_desc("name").build()` sorts by property descending
- [ ] Multiple sort keys work correctly
- [ ] Unit tests pass

---

### Week 2: Complex Transform Steps

#### Phase 3.1: ProjectStep Implementation
**File**: `src/traversal/transform.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Define `Projection` enum (Key, Traversal)
2. Implement `ProjectStep` struct with `keys: Vec<String>` and `projections: Vec<Projection>`
3. Implement `transform()` method creating result map
4. Execute sub-traversals for Traversal projections
5. Create `ProjectBuilder` for fluent configuration
6. Add `project()` method to `Traversal` returning `ProjectBuilder`

**Code Structure**:
```rust
#[derive(Clone)]
pub enum Projection {
    Key(String),
    Traversal(Traversal<Value, Value>),
}

#[derive(Clone)]
pub struct ProjectStep {
    keys: Vec<String>,
    projections: Vec<Projection>,
}

pub struct ProjectBuilder<In> {
    steps: Vec<Box<dyn AnyStep>>,
    keys: Vec<String>,
    projections: Vec<Projection>,
    _phantom: PhantomData<In>,
}

impl<In> ProjectBuilder<In> {
    pub fn by_key(mut self, key: &str) -> Self { ... }
    pub fn by(mut self, traversal: Traversal<Value, Value>) -> Self { ... }
    pub fn build(self) -> Traversal<In, Value> { ... }
}
```

**Acceptance Criteria**:
- [ ] `project(&["name", "age"]).by_key("name").by_key("age").build()` creates projection
- [ ] `project(&["name", "friends"]).by_key("name").by(__::out("knows").count()).build()` works with traversals
- [ ] Missing keys produce `Value::Null`
- [ ] Multiple traversal results produce `Value::List`
- [ ] Unit tests pass

---

#### Phase 3.2: MathStep Implementation (Basic)
**File**: `src/traversal/transform.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Implement `MathStep` struct with expression and variable bindings
2. Implement basic expression parsing (or use `meval` crate)
3. Support `_` variable for current value
4. Support labeled variables from path
5. Create `MathBuilder` for `by()` modulators
6. Add `math()` method to `Traversal` returning `MathBuilder`

**Note**: For initial implementation, support simple operations (+, -, *, /) with a basic parser. Consider `meval` crate for full expression support.

**Acceptance Criteria**:
- [ ] `values("age").math("_ * 2").build()` doubles values
- [ ] `as_("a").out().as_("b").math("a - b").by("age").by("age").build()` calculates difference
- [ ] Basic arithmetic operations work
- [ ] Unit tests pass

---

### Week 2-3: Aggregation Steps

#### Phase 4.1: Create Aggregate Module
**File**: `src/traversal/aggregate.rs` (new file)  
**Duration**: 30 minutes

**Tasks**:
1. Create `src/traversal/aggregate.rs`
2. Add necessary imports
3. Define `GroupKey` enum (Label, Property, Traversal)
4. Define `GroupValue` enum (Identity, Property, Traversal)
5. Add module to `src/traversal/mod.rs`

**Acceptance Criteria**:
- [ ] Module compiles
- [ ] Enums are defined and clonable
- [ ] Exports are accessible

---

#### Phase 4.2: GroupStep Implementation
**File**: `src/traversal/aggregate.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Implement `GroupStep` struct with `key_selector: GroupKey` and `value_collector: GroupValue`
2. Implement `get_key()` method to extract grouping key
3. Implement `get_value()` method to extract value for each group
4. Implement `AnyStep::apply()` as a barrier step
5. Create `GroupBuilder` for fluent configuration
6. Add `group()` method to `Traversal` returning `GroupBuilder`

**Code Structure**:
```rust
#[derive(Clone)]
pub struct GroupStep {
    key_selector: GroupKey,
    value_collector: GroupValue,
}

pub struct GroupBuilder<In> {
    steps: Vec<Box<dyn AnyStep>>,
    key_selector: Option<GroupKey>,
    value_collector: Option<GroupValue>,
    _phantom: PhantomData<In>,
}

impl<In> GroupBuilder<In> {
    pub fn by_label(mut self) -> Self { ... }
    pub fn by_key(mut self, key: &str) -> Self { ... }
    pub fn by_traversal(mut self, t: Traversal<Value, Value>) -> Self { ... }
    pub fn by_value(mut self) -> Self { ... }
    pub fn by_value_key(mut self, key: &str) -> Self { ... }
    pub fn by_value_traversal(mut self, t: Traversal<Value, Value>) -> Self { ... }
    pub fn build(self) -> Traversal<In, Value> { ... }
}
```

**Acceptance Criteria**:
- [ ] `group().by_label().by_value().build()` groups by label
- [ ] `group().by_key("age").by_value_key("name").build()` groups by property
- [ ] Result is `Value::Map` with lists as values
- [ ] Traversal-based grouping works
- [ ] Unit tests pass

---

#### Phase 4.3: GroupCountStep Implementation
**File**: `src/traversal/aggregate.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `GroupCountStep` struct with `key_selector: GroupKey`
2. Implement `get_key()` method (can share with GroupStep)
3. Implement `AnyStep::apply()` as a barrier step (count per key)
4. Create `GroupCountBuilder` for fluent configuration
5. Add `group_count()` method to `Traversal` returning `GroupCountBuilder`

**Acceptance Criteria**:
- [ ] `group_count().by_label().build()` counts by label
- [ ] `group_count().by_key("age").build()` counts by property
- [ ] Result is `Value::Map` with integer counts as values
- [ ] Respects `bulk` field on traversers
- [ ] Unit tests pass

---

### Week 3: API Integration and Testing

#### Phase 5.1: Update Traversal Methods
**File**: `src/traversal/mod.rs`  
**Duration**: 2 hours

**Tasks**:
1. Add all new methods to `Traversal<In, Out>`:
   - Filter: `has_not()`, `is_eq()`, `is_()`, `simple_path()`, `cyclic_path()`
   - Navigation: `other_v()`
   - Transform: `properties()`, `properties_keys()`, `value_map()`, `value_map_keys()`, `value_map_with_tokens()`, `element_map()`, `element_map_keys()`, `unfold()`, `mean()`
   - Builder: `project()`, `math()`, `order()`, `group()`, `group_count()`
2. Ensure proper type transitions
3. Update `__` module with factory functions

**Acceptance Criteria**:
- [ ] All methods compile correctly
- [ ] Type inference works as expected
- [ ] Methods chain correctly
- [ ] `__` module has all factory functions

---

#### Phase 5.2: Update __ Factory Module
**File**: `src/traversal/mod.rs`  
**Duration**: 1 hour

**Tasks**:
1. Add `__::has_not()` factory function
2. Add `__::is_eq()` and `__::is_()` factory functions
3. Add `__::simple_path()` and `__::cyclic_path()` factory functions
4. Add `__::other_v()` factory function
5. Add `__::properties()`, `__::value_map()`, `__::element_map()` factory functions
6. Add `__::unfold()` and `__::mean()` factory functions

**Acceptance Criteria**:
- [ ] All factory functions return `Traversal<Value, Value>`
- [ ] Can be used in sub-traversals (where, repeat, etc.)
- [ ] Unit tests pass

---

#### Phase 5.3: PathValue Hash Implementation
**File**: `src/traversal/context.rs` or appropriate location  
**Duration**: 1-2 hours

**Tasks**:
1. Implement `Hash` for `Value` (or create hash helper)
2. Ensure path elements can be compared for SimplePathStep/CyclicPathStep
3. Handle Float hashing (use `to_bits()`)
4. Handle Map hashing (sort keys first)

**Acceptance Criteria**:
- [ ] `Value` can be used in `HashSet`
- [ ] Float values hash correctly
- [ ] Map values hash deterministically
- [ ] Unit tests pass

---

#### Phase 5.4: Unit Tests for Filter Steps
**File**: `src/traversal/filter.rs` (tests module)  
**Duration**: 2 hours

**Tasks**:
1. Add tests for `HasNotStep`
2. Add tests for `IsStep` with various predicates
3. Add tests for `SimplePathStep`
4. Add tests for `CyclicPathStep`

**Acceptance Criteria**:
- [ ] All filter step unit tests pass
- [ ] Edge cases covered
- [ ] 100% branch coverage on new code

---

#### Phase 5.5: Unit Tests for Transform Steps
**File**: `src/traversal/transform.rs` (tests module)  
**Duration**: 2-3 hours

**Tasks**:
1. Add tests for `PropertiesStep`
2. Add tests for `ValueMapStep`
3. Add tests for `ElementMapStep`
4. Add tests for `UnfoldStep`
5. Add tests for `OrderStep`
6. Add tests for `MeanStep`
7. Add tests for `ProjectStep`

**Acceptance Criteria**:
- [ ] All transform step unit tests pass
- [ ] Edge cases covered
- [ ] Barrier steps tested with various input sizes

---

#### Phase 5.6: Unit Tests for Aggregation Steps
**File**: `src/traversal/aggregate.rs` (tests module)  
**Duration**: 2 hours

**Tasks**:
1. Add tests for `GroupStep` with various key/value selectors
2. Add tests for `GroupCountStep`
3. Test with traversal-based selectors
4. Test with bulk traversers

**Acceptance Criteria**:
- [ ] All aggregation step unit tests pass
- [ ] Grouping by label, property, and traversal works
- [ ] Count accumulation is correct

---

#### Phase 5.7: Integration Tests
**File**: `tests/traversal.rs`  
**Duration**: 2-3 hours

**Tasks**:
1. Create test graph fixtures (modern graph, cycle graph, linear graph)
2. Add integration tests for filter step combinations
3. Add integration tests for transform step combinations
4. Add integration tests for aggregation step combinations
5. Test step combinations with repeat()

**Acceptance Criteria**:
- [ ] `g.v().has_label("person").value_map().unfold()` works
- [ ] `g.v().group().by_label().by(__::count())` works
- [ ] `g.v().order().by_key_desc("age").limit(10)` works
- [ ] `g.v().repeat(__::out()).times(3).simple_path().path()` works
- [ ] All integration tests pass

---

#### Phase 5.8: Benchmarks
**File**: `benches/traversal.rs`  
**Duration**: 1-2 hours

**Tasks**:
1. Add benchmark for `order()` with 1000+ elements
2. Add benchmark for `group()` by label
3. Add benchmark for `group_count()`
4. Add benchmark for `value_map()`

**Acceptance Criteria**:
- [ ] Benchmarks compile and run
- [ ] No performance regressions from Phase 6
- [ ] Barrier steps handle large inputs efficiently

---

#### Phase 5.9: Documentation and Cleanup
**Duration**: 2-3 hours

**Tasks**:
1. Add doc comments to all new public types
2. Add doc comments to all new methods
3. Add module-level documentation with examples
4. Run `cargo clippy -- -D warnings` and fix issues
5. Run `cargo fmt`
6. Run `cargo test` and verify all pass
7. Run `cargo doc` and verify it builds

**Acceptance Criteria**:
- [ ] All public items have doc comments
- [ ] No clippy warnings
- [ ] Code is properly formatted
- [ ] All tests pass
- [ ] `cargo doc` builds without errors

---

## Exit Criteria Checklist

From spec section "Goals":

### Filter Steps
- [ ] `HasNotStep` - filters elements WITHOUT property
- [ ] `IsStep` - filters by predicate on current value
- [ ] `SimplePathStep` - filters to non-cyclic paths
- [ ] `CyclicPathStep` - filters to cyclic paths

### Navigation Steps
- [ ] `OtherVStep` - returns opposite vertex from edge

### Transform Steps
- [ ] `PropertiesStep` - returns property key-value pairs
- [ ] `ValueMapStep` - returns property map with list values
- [ ] `ElementMapStep` - returns complete element representation
- [ ] `UnfoldStep` - unrolls collections
- [ ] `ProjectStep` - creates named projections
- [ ] `MathStep` - evaluates mathematical expressions
- [ ] `OrderStep` - sorts traversers
- [ ] `MeanStep` - calculates arithmetic mean

### Aggregation Steps
- [ ] `GroupStep` - groups traversers by key
- [ ] `GroupCountStep` - counts traversers by key

### API Integration
- [ ] All 15 steps have methods on `Traversal<In, Out>`
- [ ] All applicable steps have `__` factory functions
- [ ] Builder types work correctly for complex steps
- [ ] Type transitions are correct

### Testing
- [ ] All unit tests pass (100% branch coverage on new code)
- [ ] All integration tests pass
- [ ] Benchmarks run without regression

---

## File Summary

New files to create:
- `src/traversal/aggregate.rs` - `GroupStep`, `GroupCountStep`, builders

Files to modify:
- `src/traversal/filter.rs` - Add `HasNotStep`, `IsStep`, `SimplePathStep`, `CyclicPathStep`
- `src/traversal/navigation.rs` - Add `OtherVStep`
- `src/traversal/transform.rs` - Add `PropertiesStep`, `ValueMapStep`, `ElementMapStep`, `UnfoldStep`, `ProjectStep`, `MathStep`, `OrderStep`, `MeanStep`
- `src/traversal/mod.rs` - Update exports, add `__` methods, add `Traversal` methods
- `src/traversal/context.rs` - May need `Hash` implementation for `Value`
- `tests/traversal.rs` - Add integration tests
- `benches/traversal.rs` - Add benchmarks
- `Cargo.toml` - Optionally add `meval = "0.2"` for math expressions

---

## Dependencies

```toml
[dependencies]
meval = "0.2"  # Optional - for full math() expression support
```

Existing dependencies used:
- `regex` - Already present from Phase 4
- `thiserror` - Error types
- All Phase 3-6 traversal infrastructure

---

## Implementation Notes

### Barrier Step Pattern

Barrier steps (order, group, groupCount, mean) collect all input before producing output:

```rust
impl AnyStep for BarrierStep {
    fn apply<'a>(&'a self, ctx: &'a ExecutionContext<'a>, input: Box<dyn Iterator<Item = Traverser> + 'a>) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Collect all input
        let all: Vec<_> = input.collect();
        
        // Process
        let result = self.process(ctx, &all);
        
        // Emit result(s)
        Box::new(std::iter::once(result))
    }
}
```

### Builder Pattern for Complex Steps

Steps with multiple `by()` modulators use the builder pattern:

```rust
// Start with step keys
g.v().project(&["name", "age"])
    // Configure projections
    .by_key("name")
    .by_key("age")
    // Finalize
    .build()
    .to_list()
```

### Value Hash Implementation

For `SimplePathStep` and `CyclicPathStep`, `Value` needs to be hashable. Use the pattern from the spec:

```rust
impl Value {
    fn hash_value<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Float(f) => f.to_bits().hash(state),
            Value::Map(map) => {
                let mut keys: Vec<_> = map.keys().collect();
                keys.sort();
                for key in keys { ... }
            }
            // ... other variants
        }
    }
}
```

### OtherVStep Path Inspection

`OtherVStep` needs to find the vertex we came from by inspecting the path:

```rust
// Path: [Vertex(A), Edge(e), ...]
// Previous element is at index len - 2
let path_elements = traverser.path.elements();
let prev = &path_elements[path_elements.len() - 2];
```

This assumes proper path tracking is enabled. If path is empty or has only one element, fall back to a default behavior (e.g., return out_vertex).
