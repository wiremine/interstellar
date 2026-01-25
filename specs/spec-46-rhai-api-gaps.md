# Spec 46: Rhai API Gaps

This specification identifies functions implemented in Rust but missing from the Rhai scripting API, based on the API reference in `docs/api/gremlin.md`.

---

## 1. Overview

### 1.1 Motivation

The Rhai scripting API provides a powerful way to query graphs without compiling Rust code. However, several Rust traversal steps and anonymous traversal factory functions are not yet exposed to Rhai. This creates an inconsistent experience where scripts cannot access the full power of the traversal engine.

### 1.2 Scope

This specification:
- Catalogs all missing Rhai functions by category
- Defines the expected Rhai function signatures
- Prioritizes implementation based on usage patterns

### 1.3 Non-Goals

- Implementation details (covered in implementation phase)
- Performance optimizations
- New features not already in Rust API

---

## 2. Missing Traversal Step Functions

### 2.1 Filter Steps (6 missing)

| Rust | Proposed Rhai | Description |
|------|---------------|-------------|
| `has_key(key)` | `has_key("key")` | Filter properties that have a specific key |
| `has_key_any(&[keys])` | `has_key_any(["keys"])` | Filter properties with any of the specified keys |
| `has_prop_value(value)` | `has_prop_value(value)` | Filter properties with a specific value |
| `has_prop_value_any(&[values])` | `has_prop_value_any([values])` | Filter properties with any of the specified values |
| `filter(closure)` | `-` | **Skip**: Requires closure support |
| `where_p(predicate)` | `where_p(predicate)` | Filter using a predicate on current value |

### 2.2 Transform/Map Steps (6 missing)

| Rust | Proposed Rhai | Description |
|------|---------------|-------------|
| `property_map()` | `property_map()` | Get map of property name to Property objects |
| `property_map_keys(&[keys])` | `property_map_keys(["keys"])` | Get property map with specific keys |
| `element_map_keys(&[keys])` | `element_map_keys(["keys"])` | Get element map with specific keys |
| `loops()` | `loops()` | Get current loop iteration count in repeat |
| `map(closure)` | `-` | **Skip**: Requires closure support |
| `flat_map(closure)` | `-` | **Skip**: Requires closure support |

### 2.3 Repeat Steps (2 missing)

| Rust | Proposed Rhai | Description |
|------|---------------|-------------|
| `repeat(traversal)` | `repeat(traversal)` | Start a repeat loop (base function) |
| `repeat().emit_first()` | `repeat_emit_first(traversal, n)` | Emit before each iteration |

### 2.4 Branch Steps (1 missing)

| Rust | Proposed Rhai | Description |
|------|---------------|-------------|
| `branch(traversal).option()` | `branch(traversal, options)` | Multi-way branching with options |

### 2.5 Side Effect Steps (1 missing)

| Rust | Proposed Rhai | Description |
|------|---------------|-------------|
| `profile()`, `profile_as(key)` | `profile()`, `profile_as("key")` | Execution profiling |

### 2.6 Modulator Steps (1 missing)

| Rust | Proposed Rhai | Description |
|------|---------------|-------------|
| `with_path()` | Already implemented | Path tracking (confirmed present) |

---

## 3. Missing Anonymous Traversal Factory Functions (`A.`)

The anonymous traversal factory (`A` in Rhai, `__` in Rust) is missing many functions that are available via `__.` in Rust. These are critical for composing complex traversals.

### 3.1 Missing Filter Functions (19 missing)

| Rust `__.` | Proposed Rhai `A.` | Description |
|------------|-------------------|-------------|
| `has_label_any(&[labels])` | `A.has_label_any(["labels"])` | Filter by any of the labels |
| `has_id(id)` | `A.has_id(id)` | Filter by element ID |
| `has_ids(&[ids])` | `A.has_ids([ids])` | Filter by any of the IDs |
| `has_key(key)` | `A.has_key("key")` | Filter properties by key existence |
| `has_key_any(&[keys])` | `A.has_key_any(["keys"])` | Filter properties by any key |
| `has_prop_value(value)` | `A.has_prop_value(value)` | Filter properties by value |
| `has_prop_value_any(&[values])` | `A.has_prop_value_any([values])` | Filter properties by any value |
| `is_(predicate)` | `A.is_(predicate)` | Filter by predicate |
| `is_eq(value)` | `A.is_eq(value)` | Filter by equality |
| `filter(closure)` | `-` | **Skip**: Requires closure |
| `skip(n)` | `A.skip(n)` | Skip first n elements |
| `range(start, end)` | `A.range(start, end)` | Take elements in range |
| `tail()` | `A.tail()` | Get last element |
| `tail_n(n)` | `A.tail_n(n)` | Get last n elements |
| `coin(probability)` | `A.coin(probability)` | Random filter |
| `sample(n)` | `A.sample(n)` | Random sample |
| `simple_path()` | `A.simple_path()` | Non-repeating path filter |
| `cyclic_path()` | `A.cyclic_path()` | Repeating path filter |
| `dedup_by_key(key)` | `A.dedup_by_key("key")` | Dedup by property |
| `dedup_by_label()` | `A.dedup_by_label()` | Dedup by label |
| `dedup_by(traversal)` | `A.dedup_by(traversal)` | Dedup by traversal result |

### 3.2 Missing Transform Functions (18 missing)

| Rust `__.` | Proposed Rhai `A.` | Description |
|------------|-------------------|-------------|
| `values_multi(&[keys])` | `A.values_multi(["keys"])` | Get multiple property values |
| `properties()` | `A.properties()` | Get all properties |
| `properties_keys(&[keys])` | `A.properties_keys(["keys"])` | Get specific properties |
| `value_map_keys(&[keys])` | `A.value_map_keys(["keys"])` | Get value map with keys |
| `value_map_with_tokens()` | `A.value_map_with_tokens()` | Value map with id/label |
| `element_map()` | `A.element_map()` | Get complete element map |
| `element_map_keys(&[keys])` | `A.element_map_keys(["keys"])` | Element map with keys |
| `property_map()` | `A.property_map()` | Get property map |
| `property_map_keys(&[keys])` | `A.property_map_keys(["keys"])` | Property map with keys |
| `key()` | `A.key()` | Get property key |
| `value()` | `A.prop_value()` | Get property value |
| `index()` | `A.index()` | Get index in stream |
| `loops()` | `A.loops()` | Get repeat loop count |
| `mean()` | `A.mean()` | Calculate mean |
| `order()` | `A.order_asc()` / `A.order_desc()` | Order elements |
| `math(expr)` | `A.math("expr")` | Math expression |
| `project(&[keys])` | `A.project(keys, projections)` | Project to map |
| `map(closure)` | `-` | **Skip**: Requires closure |
| `flat_map(closure)` | `-` | **Skip**: Requires closure |

### 3.3 Missing Aggregation Functions (2 missing)

| Rust `__.` | Proposed Rhai `A.` | Description |
|------------|-------------------|-------------|
| `group()` | `A.group(key_selector, value_collector)` | Group elements |
| `group_count()` | `A.group_count(key_selector)` | Count by group |

### 3.4 Missing Side Effect Functions (6 missing)

| Rust `__.` | Proposed Rhai `A.` | Description |
|------------|-------------------|-------------|
| `select(&[labels])` | `A.select(["labels"])` | Select labeled steps |
| `select_one(label)` | `A.select_one("label")` | Select single label |
| `store(key)` | `A.store("key")` | Store to side effect |
| `aggregate(key)` | `A.aggregate("key")` | Aggregate to side effect |
| `cap(key)` | `A.cap("key")` | Retrieve side effect |
| `side_effect(traversal)` | `A.side_effect(traversal)` | Execute side effect |
| `profile()` | `A.profile()` | Execution profiling |

### 3.5 Missing Branch Functions (10 missing)

| Rust `__.` | Proposed Rhai `A.` | Description |
|------------|-------------------|-------------|
| `where_(traversal)` | `A.where_(traversal)` | Filter by traversal result |
| `where_p(predicate)` | `A.where_p(predicate)` | Filter by predicate |
| `not(traversal)` | `A.not(traversal)` | Negated filter |
| `and_(&[traversals])` | `A.and_([traversals])` | AND filter |
| `or_(&[traversals])` | `A.or_([traversals])` | OR filter |
| `union(&[traversals])` | `A.union([traversals])` | Union of traversals |
| `coalesce(&[traversals])` | `A.coalesce([traversals])` | First non-empty result |
| `choose(cond, if_true, if_false)` | `A.choose_binary(cond, t, f)` | Conditional branch |
| `optional(traversal)` | `A.optional(traversal)` | Optional traversal |
| `local(traversal)` | `A.local(traversal)` | Local scope |
| `branch(traversal)` | `A.branch(traversal, options)` | Multi-way branch |

### 3.6 Missing Mutation Functions (4 missing)

| Rust `__.` | Proposed Rhai `A.` | Description |
|------------|-------------------|-------------|
| `add_v(label)` | `A.add_v("label")` | Add vertex |
| `add_e(label)` | `A.add_e("label")` | Add edge |
| `property(key, value)` | `A.property("key", value)` | Set property |
| `drop()` | `A.drop_()` | Remove element |

---

## 4. Summary Statistics

### 4.1 Traversal Steps

| Category | Total in Rust | Implemented in Rhai | Missing | Coverage |
|----------|---------------|---------------------|---------|----------|
| Filter Steps | 34 | 28 | 6 | 82% |
| Transform/Map Steps | 30 | 26 | 4 | 87% |
| Repeat Steps | 7 | 4 | 3 | 57% |
| Branch Steps | 8 | 6 | 2 | 75% |
| Side Effect Steps | 7 | 6 | 1 | 86% |
| **Total** | **86** | **70** | **16** | **81%** |

*Note: 3 closure-based functions (filter, map, flat_map) are excluded as they require language-level closure support.*

### 4.2 Anonymous Traversal Factory (`A.`)

| Category | Total in Rust `__.` | Implemented in Rhai `A.` | Missing | Coverage |
|----------|---------------------|--------------------------|---------|----------|
| Filter | 21+ | 6 | 15+ | ~29% |
| Transform | 20+ | 10 | 10+ | ~50% |
| Aggregation | 2 | 0 | 2 | 0% |
| Side Effect | 7 | 0 | 7 | 0% |
| Branch | 11 | 0 | 11 | 0% |
| Mutation | 4 | 0 | 4 | 0% |
| **Total** | **65+** | **16** | **49+** | **~25%** |

---

## 5. Implementation Priority

### 5.1 Priority 1: High-Value Anonymous Traversal Functions

These enable complex query patterns that are currently impossible in Rhai:

1. **Branch functions**: `A.where_()`, `A.not()`, `A.and_()`, `A.or_()`, `A.union()`, `A.coalesce()`, `A.optional()`
2. **Filter functions**: `A.has_id()`, `A.has_ids()`, `A.is_()`, `A.is_eq()`, `A.simple_path()`, `A.cyclic_path()`
3. **Transform functions**: `A.select()`, `A.select_one()`, `A.mean()`, `A.element_map()`

### 5.2 Priority 2: Completeness Functions

These round out the API for consistency:

1. **Filter**: `A.has_label_any()`, `A.skip()`, `A.range()`, `A.tail()`, `A.tail_n()`
2. **Transform**: `A.values_multi()`, `A.properties()`, `A.properties_keys()`, `A.value_map_keys()`, `A.key()`, `A.prop_value()`
3. **Side Effect**: `A.store()`, `A.aggregate()`, `A.cap()`, `A.side_effect()`

### 5.3 Priority 3: Advanced Functions

These support advanced use cases:

1. **Filter**: `A.has_key()`, `A.has_key_any()`, `A.has_prop_value()`, `A.has_prop_value_any()`, `A.coin()`, `A.sample()`, `A.dedup_by*`
2. **Transform**: `A.property_map()`, `A.property_map_keys()`, `A.index()`, `A.loops()`, `A.math()`, `A.project()`
3. **Aggregation**: `A.group()`, `A.group_count()`
4. **Mutation**: `A.add_v()`, `A.add_e()`, `A.property()`, `A.drop_()`

### 5.4 Priority 4: Traversal Step Gaps

These are less critical since the main traversal API is more complete:

1. `has_key()`, `has_key_any()`, `has_prop_value()`, `has_prop_value_any()`
2. `property_map()`, `property_map_keys()`, `element_map_keys()`
3. `loops()`, `where_p()`
4. `repeat()` base function, `repeat_emit_first()`
5. `branch()`, `profile()`

---

## 6. Implementation Approach

### 6.1 File Structure

The Rhai API is implemented in:
- `src/rhai/` - Rhai module root
- `src/rhai/traversal.rs` - Traversal step bindings
- `src/rhai/anonymous.rs` - Anonymous traversal factory (`A`)

### 6.2 Pattern for Adding Functions

Each function follows this pattern:

```rust
// In src/rhai/anonymous.rs
fn register_anonymous_functions(engine: &mut Engine) {
    // Existing pattern for A.has_label
    engine.register_fn("has_label", |label: &str| -> DynamicTraversal {
        DynamicTraversal::new(__.has_label(label))
    });
    
    // New function following same pattern
    engine.register_fn("has_id", |id: i64| -> DynamicTraversal {
        DynamicTraversal::new(__.has_id(VertexId(id as u64)))
    });
}
```

### 6.3 Testing Strategy

Each new function needs:
1. Unit test in `src/rhai/tests/` verifying correct behavior
2. Integration test with a sample graph
3. Documentation example in `docs/api/gremlin.md`

---

## 7. Implementation Checklist

### 7.1 Priority 1 (Branch & Core Filter)

- [ ] `A.where_(traversal)`
- [ ] `A.not(traversal)`
- [ ] `A.and_([traversals])`
- [ ] `A.or_([traversals])`
- [ ] `A.union([traversals])`
- [ ] `A.coalesce([traversals])`
- [ ] `A.optional(traversal)`
- [ ] `A.has_id(id)`
- [ ] `A.has_ids([ids])`
- [ ] `A.is_(predicate)`
- [ ] `A.is_eq(value)`
- [ ] `A.simple_path()`
- [ ] `A.cyclic_path()`
- [ ] `A.select(["labels"])`
- [ ] `A.select_one("label")`
- [ ] `A.mean()`
- [ ] `A.element_map()`

### 7.2 Priority 2 (Completeness)

- [ ] `A.has_label_any(["labels"])`
- [ ] `A.skip(n)`
- [ ] `A.range(start, end)`
- [ ] `A.tail()`
- [ ] `A.tail_n(n)`
- [ ] `A.values_multi(["keys"])`
- [ ] `A.properties()`
- [ ] `A.properties_keys(["keys"])`
- [ ] `A.value_map_keys(["keys"])`
- [ ] `A.key()`
- [ ] `A.prop_value()`
- [ ] `A.store("key")`
- [ ] `A.aggregate("key")`
- [ ] `A.cap("key")`
- [ ] `A.side_effect(traversal)`

### 7.3 Priority 3 (Advanced)

- [ ] `A.has_key("key")`
- [ ] `A.has_key_any(["keys"])`
- [ ] `A.has_prop_value(value)`
- [ ] `A.has_prop_value_any([values])`
- [ ] `A.coin(probability)`
- [ ] `A.sample(n)`
- [ ] `A.dedup_by_key("key")`
- [ ] `A.dedup_by_label()`
- [ ] `A.dedup_by(traversal)`
- [ ] `A.property_map()`
- [ ] `A.property_map_keys(["keys"])`
- [ ] `A.index()`
- [ ] `A.loops()`
- [ ] `A.math("expr")`
- [ ] `A.project(keys, projections)`
- [ ] `A.group(key_selector, value_collector)`
- [ ] `A.group_count(key_selector)`
- [ ] `A.add_v("label")`
- [ ] `A.add_e("label")`
- [ ] `A.property("key", value)`
- [ ] `A.drop_()`
- [ ] `A.local(traversal)`
- [ ] `A.choose_binary(cond, true_t, false_t)`
- [ ] `A.branch(traversal, options)`

### 7.4 Priority 4 (Traversal Steps)

- [ ] `has_key("key")`
- [ ] `has_key_any(["keys"])`
- [ ] `has_prop_value(value)`
- [ ] `has_prop_value_any([values])`
- [ ] `where_p(predicate)`
- [ ] `property_map()`
- [ ] `property_map_keys(["keys"])`
- [ ] `element_map_keys(["keys"])`
- [ ] `loops()`
- [ ] `repeat(traversal)` (base)
- [ ] `repeat_emit_first(traversal, n)`
- [ ] `branch(traversal, options)`
- [ ] `profile()`
- [ ] `profile_as("key")`

---

## 8. Documentation Updates

After implementation, update `docs/api/gremlin.md`:

1. Change `-` entries to implemented function names
2. Update the Implementation Summary table counts
3. Add examples for newly implemented functions
4. Update the "Additional Rust-only Anonymous Functions" section

---

## 9. Acceptance Criteria

1. All Priority 1 functions implemented and tested
2. All Priority 2 functions implemented and tested
3. `docs/api/gremlin.md` updated with new function mappings
4. Anonymous traversal factory (`A.`) coverage reaches 80%+
5. All existing tests continue to pass
