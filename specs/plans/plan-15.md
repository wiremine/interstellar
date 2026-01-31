# Plan 15: Implement Metadata Transform Steps

**Spec Reference:** `specs/spec-13-metadata-steps.md`

**Goal:** Implement Gremlin metadata/transform steps (`propertyMap`, `key`, `value`, `index`, `loops`) to provide access to property metadata, stream position, and loop depth.

**Estimated Duration:** 3-5 days

---

## Overview

This plan implements the metadata transform steps defined in Spec 13. These steps extract metadata from property objects, annotate stream position, and expose loop depth information from repeat operations.

---

## Phase 1: Key and Value Steps (Day 1)

The `key()` and `value()` steps are simple transforms that work on property objects from the `properties()` step.

### 1.1 Implement KeyStep

**File:** `src/traversal/transform/metadata.rs` (add to existing file)

**Tasks:**
- [ ] Implement `KeyStep` struct (unit struct, no fields)
- [ ] Implement `AnyStep` trait for `KeyStep`
- [ ] Extract "key" from `Value::Map` property objects
- [ ] Filter out non-map and invalid map inputs
- [ ] Add step name "key"

**Implementation:**
```rust
#[derive(Clone, Copy, Debug, Default)]
pub struct KeyStep;

impl AnyStep for KeyStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(|t| {
            match &t.value {
                Value::Map(map) => {
                    map.get("key").cloned().map(|key| t.with_value(key))
                }
                _ => None,
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> { Box::new(*self) }
    fn name(&self) -> &'static str { "key" }
}
```

### 1.2 Implement ValueStep

**File:** `src/traversal/transform/metadata.rs`

**Tasks:**
- [ ] Implement `ValueStep` struct (unit struct, no fields)
- [ ] Implement `AnyStep` trait for `ValueStep`
- [ ] Extract "value" from `Value::Map` property objects
- [ ] Filter out non-map and invalid map inputs
- [ ] Add step name "value"

**Note:** Named `ValueStep` internally, method will be `.value_()` to avoid conflict with `Value` type, or just `.value()` if no conflict exists.

### 1.3 Add Traversal Methods

**File:** `src/traversal/source.rs` (BoundTraversal impl)

**Tasks:**
- [ ] Add `.key()` method returning `BoundTraversal<..., Value>`
- [ ] Add `.value_()` or `.value()` method returning `BoundTraversal<..., Value>`

### 1.4 Add to Anonymous Traversal

**File:** `src/traversal/source.rs` (`__` module)

**Tasks:**
- [ ] Add `__::key()`
- [ ] Add `__::value()` (or `__::value_()`)

### 1.5 Update Module Exports

**File:** `src/traversal/transform/mod.rs`

**Tasks:**
- [ ] Export `KeyStep` and `ValueStep` from transform module

**File:** `src/traversal/mod.rs`

**Tasks:**
- [ ] Re-export `KeyStep` and `ValueStep` in traversal prelude

### 1.6 Write Tests

**File:** `src/traversal/transform/metadata.rs` (tests module)

**Tasks:**
- [ ] Test `key()` extracts key from property map
- [ ] Test `key()` filters non-map values
- [ ] Test `key()` filters maps without "key"
- [ ] Test `value()` extracts value from property map
- [ ] Test `value()` filters non-map values
- [ ] Test `value()` filters maps without "value"
- [ ] Test both preserve traverser path and loops
- [ ] Test pipeline: `properties().key().dedup()`
- [ ] Test pipeline: `properties().value()` equals `values()`

---

## Phase 2: LoopsStep Implementation (Day 1-2)

The `loops()` step is trivial since loop count is already tracked in the `Traverser`.

### 2.1 Implement LoopsStep

**File:** `src/traversal/transform/metadata.rs`

**Tasks:**
- [ ] Implement `LoopsStep` struct (unit struct, no fields)
- [ ] Implement `AnyStep` trait for `LoopsStep`
- [ ] Read `traverser.loops` and convert to `Value::Int`
- [ ] Add step name "loops"

**Implementation:**
```rust
#[derive(Clone, Copy, Debug, Default)]
pub struct LoopsStep;

impl AnyStep for LoopsStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(|t| {
            let loops = t.loops as i64;
            t.with_value(Value::Int(loops))
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> { Box::new(*self) }
    fn name(&self) -> &'static str { "loops" }
}
```

### 2.2 Add Traversal Method

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Add `.loops()` method returning `BoundTraversal<..., Value>`

### 2.3 Add to Anonymous Traversal

**File:** `src/traversal/source.rs` (`__` module)

**Tasks:**
- [ ] Add `__::loops()`

### 2.4 Write Tests

**Tasks:**
- [ ] Test `loops()` returns 0 outside repeat
- [ ] Test `loops()` returns correct depth inside repeat with `times(n)`
- [ ] Test `loops()` with emit shows correct depth at each emission
- [ ] Test `loops()` in until condition: `until(__::loops().is_(p::eq(3)))`
- [ ] Test preserves path metadata

---

## Phase 3: IndexStep Implementation (Day 2)

The `index()` step requires stateful iteration to track stream position.

### 3.1 Implement IndexStep

**File:** `src/traversal/transform/metadata.rs`

**Tasks:**
- [ ] Implement `IndexStep` struct (unit struct)
- [ ] Implement `AnyStep` trait for `IndexStep`
- [ ] Use `Cell<usize>` for thread-local counter
- [ ] Wrap each value as `[value, index]` list
- [ ] Add step name "index"

**Implementation:**
```rust
use std::cell::Cell;

#[derive(Clone, Debug, Default)]
pub struct IndexStep;

impl AnyStep for IndexStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let counter = Cell::new(0usize);
        
        Box::new(input.map(move |t| {
            let idx = counter.get();
            counter.set(idx + 1);
            
            let indexed = Value::List(vec![
                t.value.clone(),
                Value::Int(idx as i64),
            ]);
            t.with_value(indexed)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> { Box::new(Self) }
    fn name(&self) -> &'static str { "index" }
}
```

**Design Notes:**
- `Cell<usize>` is safe because the iterator is consumed single-threaded
- Counter resets each time `apply()` is called (new traversal execution)
- Uses `t.with_value()` to preserve path/loops metadata

### 3.2 Add Traversal Method

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Add `.index()` method returning `BoundTraversal<..., Value>`

### 3.3 Add to Anonymous Traversal

**File:** `src/traversal/source.rs` (`__` module)

**Tasks:**
- [ ] Add `__::index()`

### 3.4 Write Tests

**Tasks:**
- [ ] Test first element gets index 0
- [ ] Test indices increment sequentially
- [ ] Test empty input returns empty output
- [ ] Test output format is `[value, index]` list
- [ ] Test preserves traverser metadata (path, loops)
- [ ] Test works with unfold: `index().unfold()`
- [ ] Test index resets on new traversal execution

---

## Phase 4: PropertyMapStep Implementation (Day 3)

The `propertyMap()` step is similar to `valueMap()` but returns property objects instead of raw values.

### 4.1 Implement PropertyMapStep

**File:** `src/traversal/transform/properties.rs` (add to existing file)

**Tasks:**
- [ ] Implement `PropertyMapStep` struct with `keys: Option<Vec<String>>`
- [ ] Implement `PropertyMapStep::new()` and `PropertyMapStep::with_keys()`
- [ ] Implement `AnyStep` trait
- [ ] Build map where values are lists of property objects `{key, value}`
- [ ] Add step name "propertyMap"

**Implementation:**
```rust
#[derive(Clone, Debug)]
pub struct PropertyMapStep {
    keys: Option<Vec<String>>,
}

impl PropertyMapStep {
    pub fn new() -> Self {
        Self { keys: None }
    }

    pub fn with_keys(keys: Vec<String>) -> Self {
        Self { keys: Some(keys) }
    }

    fn transform(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        let mut map = HashMap::new();

        match &traverser.value {
            Value::Vertex(id) => {
                if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                    let iter: Box<dyn Iterator<Item = (&String, &Value)>> = match &self.keys {
                        None => Box::new(vertex.properties.iter()),
                        Some(keys) => Box::new(
                            keys.iter()
                                .filter_map(|k| vertex.properties.get(k).map(|v| (k, v)))
                        ),
                    };
                    
                    for (key, value) in iter {
                        let prop_obj = PropertiesStep::make_property_map(key.clone(), value.clone());
                        map.insert(key.clone(), Value::List(vec![prop_obj]));
                    }
                }
            }
            Value::Edge(id) => {
                // Similar implementation for edges
            }
            _ => {}
        }

        Value::Map(map)
    }
}
```

### 4.2 Add Traversal Methods

**File:** `src/traversal/source.rs`

**Tasks:**
- [ ] Add `.property_map()` method
- [ ] Add `.property_map_keys(keys)` method

### 4.3 Add to Anonymous Traversal

**File:** `src/traversal/source.rs` (`__` module)

**Tasks:**
- [ ] Add `__::property_map()`
- [ ] Add `__::property_map_keys(keys)`

### 4.4 Update Module Exports

**File:** `src/traversal/transform/mod.rs`

**Tasks:**
- [ ] Export `PropertyMapStep` from transform module

### 4.5 Write Tests

**Tasks:**
- [ ] Test returns property objects for all properties
- [ ] Test returns property objects for specific keys only
- [ ] Test property object format `{key: "name", value: "Alice"}`
- [ ] Test values are wrapped in lists
- [ ] Test works with vertices
- [ ] Test works with edges
- [ ] Test returns empty map for non-elements
- [ ] Test preserves traverser metadata

---

## Phase 5: Integration and Documentation (Day 4)

### 5.1 Integration Tests

**File:** `tests/traversal.rs` (add to existing or create new section)

**Tasks:**
- [ ] Test `properties().key().dedup()` pipeline
- [ ] Test `properties().value()` equals `values()`
- [ ] Test `index().unfold()` pipeline
- [ ] Test `repeat().emit().loops()` pipeline
- [ ] Test `repeat().until(__::loops().is_(p::eq(n)))` pattern
- [ ] Test `project().by(__::loops())` for depth tracking

### 5.2 Example File

**File:** `examples/metadata_steps.rs`

**Tasks:**
- [ ] Create example demonstrating all new steps
- [ ] Show key/value extraction from properties
- [ ] Show index usage for stream position
- [ ] Show loops usage in repeat patterns
- [ ] Show propertyMap vs valueMap difference
- [ ] Add detailed comments

### 5.3 Update API Documentation

**File:** `Gremlin_api.md`

**Tasks:**
- [ ] Update Transform/Map Steps table
- [ ] Change `propertyMap()` from `-` to `property_map()`
- [ ] Change `key()` from `-` to `key()`
- [ ] Change `value()` from `-` to `value()`
- [ ] Change `index()` from `-` to `index()`
- [ ] Change `loops()` from `-` to `loops()`
- [ ] Update Implementation Summary counts

### 5.4 Add to Anonymous Traversal Documentation

**File:** `Gremlin_api.md` (Anonymous Traversal section)

**Tasks:**
- [ ] Add `__::property_map()`, `__::property_map_keys()`
- [ ] Add `__::key()`, `__::value()`
- [ ] Add `__::index()`
- [ ] Add `__::loops()`

---

## Testing Checklist

### Unit Tests

**KeyStep:**
- [ ] Extracts key from property map `{key: "name", value: "Alice"}`
- [ ] Filters non-map values (Int, String, etc.)
- [ ] Filters maps without "key" entry
- [ ] Preserves traverser path
- [ ] Preserves traverser loops count

**ValueStep:**
- [ ] Extracts value from property map
- [ ] Filters non-map values
- [ ] Filters maps without "value" entry
- [ ] Handles all value types (String, Int, Float, Bool, List, Map)
- [ ] Preserves traverser metadata

**LoopsStep:**
- [ ] Returns 0 outside repeat
- [ ] Returns 1 after first iteration
- [ ] Returns correct count with times(n)
- [ ] Works in emit condition
- [ ] Works in until condition

**IndexStep:**
- [ ] First element index is 0
- [ ] Sequential indices (0, 1, 2, ...)
- [ ] Empty input returns empty
- [ ] Output is `[value, index]` list
- [ ] Preserves path metadata

**PropertyMapStep:**
- [ ] Returns map of property objects
- [ ] Property objects have "key" and "value"
- [ ] Values wrapped in lists (multi-property support)
- [ ] Filters to specific keys when provided
- [ ] Works for vertices and edges
- [ ] Returns empty map for non-elements

### Integration Tests

- [ ] `g.v().properties().key().dedup().to_list()`
- [ ] `g.v().properties("name").value().to_list()` equals `g.v().values("name").to_list()`
- [ ] `g.v().index().to_list()` returns `[[v[0], 0], [v[1], 1], ...]`
- [ ] `g.v().repeat(__::out()).times(3).emit().loops().to_list()`
- [ ] `g.v().repeat(__::out()).until(__::loops().is_(p::eq(2))).to_list()`

---

## Dependencies

- Existing transform module (`src/traversal/transform/`)
- Existing properties step (`src/traversal/transform/properties.rs`)
- Existing repeat step (`src/traversal/repeat.rs`)
- Existing predicate module (`src/traversal/predicate.rs`)

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| `index()` thread safety | Low | Use `Cell<usize>` which is safe for single-threaded iteration |
| `value()` name conflict with `Value` type | Low | Use `value_()` if needed, or rely on method context |
| `loops()` 0-based vs 1-based | Medium | Document difference from Gremlin; 0-based is idiomatic Rust |
| `propertyMap()` vs `valueMap()` confusion | Low | Clear documentation distinguishing the two |

---

## Success Criteria

1. All 5 steps are implemented and pass unit tests
2. All steps available in `__` anonymous traversal factory
3. Integration tests pass for common patterns
4. Tests achieve >90% branch coverage on new code
5. `Gremlin_api.md` updated with new implementations
6. Example file demonstrates all new functionality

---

## File Changes Summary

| File | Changes |
|------|---------|
| `src/traversal/transform/metadata.rs` | Add `KeyStep`, `ValueStep`, `LoopsStep`, `IndexStep` |
| `src/traversal/transform/properties.rs` | Add `PropertyMapStep` |
| `src/traversal/transform/mod.rs` | Export new steps |
| `src/traversal/mod.rs` | Re-export new steps |
| `src/traversal/source.rs` | Add traversal methods, add to `__` module |
| `tests/traversal.rs` | Add integration tests |
| `examples/metadata_steps.rs` | New example file |
| `Gremlin_api.md` | Update documentation |

---

## Future Work (Out of Scope)

- `loops(loopName)` - Named loop tracking for nested repeats
- `index().with(Indexer.map)` - Custom indexer options
- First-class `Property` type in `Value` enum
- `withIndex()` modifier pattern
