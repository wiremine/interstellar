# Plan 14: Implement Missing Filter Steps

**Spec Reference:** `specs/spec-12-filter-steps.md`

**Goal:** Implement missing Gremlin filter steps (`tail`, `dedup(by)`, `coin`, `sample`, `hasKey`, `hasValue`, `where(predicate)`) to complete the filter step coverage.

**Estimated Duration:** 1-2 weeks

---

## Overview

This plan implements the missing filter steps defined in Spec 12. These steps extend the existing filter functionality with tail operations, deduplication by key, probabilistic filtering, random sampling, and property-based filters.

---

## Phase 1: Dependencies and Setup (Day 1)

### 1.1 Add `rand` Crate Dependency

**File:** `Cargo.toml`

Add the `rand` crate for probabilistic operations:

```toml
[dependencies]
rand = "0.8"
```

**Tasks:**
- [ ] Add `rand = "0.8"` to dependencies
- [ ] Run `cargo build` to verify

### 1.2 Create Test Infrastructure

**Tasks:**
- [ ] Add helper for seeded RNG in tests (reproducibility)
- [ ] Create statistical test helpers for probabilistic steps

---

## Phase 2: TailStep Implementation (Day 2)

### 2.1 Implement TailStep

**File:** `src/traversal/filter.rs`

**Tasks:**
- [ ] Implement `TailStep` struct with `count: usize`
- [ ] Implement `TailStep::new(count)` and `TailStep::last()`
- [ ] Implement `AnyStep` trait for `TailStep`
- [ ] Add step name "tail"

**Implementation Notes:**
- Barrier step: must collect all elements
- Use `Vec::collect()` then slice from end
- Handle edge case: count > input length

### 2.2 Add Traversal Methods

**File:** `src/traversal/mod.rs` (or `context.rs`)

**Tasks:**
- [ ] Add `.tail()` method (returns last element)
- [ ] Add `.tail_n(n)` method (returns last n elements)
- [ ] Ensure proper type inference

### 2.3 Add to Anonymous Traversal

**File:** `src/traversal/mod.rs` (`__` module)

**Tasks:**
- [ ] Add `__::tail()`
- [ ] Add `__::tail_n(n)`

### 2.4 Write Tests

**Tasks:**
- [ ] Test `tail()` returns last element
- [ ] Test `tail_n(3)` returns last 3 elements
- [ ] Test `tail_n(10)` on 5 elements returns all 5
- [ ] Test empty input returns empty output
- [ ] Test preserves traverser metadata (path, labels, bulk)

---

## Phase 3: DedupByKey Implementation (Days 3-4)

### 3.1 Implement DedupByKeyStep

**File:** `src/traversal/filter.rs`

**Tasks:**
- [ ] Implement `DedupByKeyStep` struct with `key: String`
- [ ] Implement `DedupByKeyStep::new(key)`
- [ ] Implement `AnyStep` trait
- [ ] Extract property value from vertices/edges
- [ ] Use `HashSet<Value>` to track seen keys
- [ ] Add step name "dedup"

### 3.2 Implement DedupByLabelStep

**File:** `src/traversal/filter.rs`

**Tasks:**
- [ ] Implement `DedupByLabelStep` struct
- [ ] Implement `AnyStep` trait
- [ ] Extract label from vertices/edges
- [ ] Add step name "dedup"

### 3.3 Implement DedupByTraversalStep

**File:** `src/traversal/filter.rs`

**Tasks:**
- [ ] Implement `DedupByTraversalStep` struct with traversal
- [ ] Implement `AnyStep` trait
- [ ] Execute nested traversal for each element
- [ ] Use result as dedup key
- [ ] Add step name "dedup"

### 3.4 Add Traversal Methods

**Tasks:**
- [ ] Add `.dedup_by_key(key)` method
- [ ] Add `.dedup_by_label()` method
- [ ] Add `.dedup_by(traversal)` method

### 3.5 Add to Anonymous Traversal

**Tasks:**
- [ ] Add `__::dedup_by_key(key)`
- [ ] Add `__::dedup_by_label()`
- [ ] Add `__::dedup_by(traversal)`

### 3.6 Write Tests

**Tasks:**
- [ ] Test dedup by property key keeps first occurrence
- [ ] Test missing property treated as null
- [ ] Test dedup by label works correctly
- [ ] Test dedup by traversal (e.g., out-degree)
- [ ] Test works with both vertices and edges
- [ ] Test empty input returns empty output

---

## Phase 4: CoinStep Implementation (Day 5)

### 4.1 Implement CoinStep

**File:** `src/traversal/filter.rs`

**Tasks:**
- [ ] Implement `CoinStep` struct with `probability: f64`
- [ ] Implement `CoinStep::new(probability)` with validation
- [ ] Implement `AnyStep` trait
- [ ] Use `rand::thread_rng()` for randomness
- [ ] Add step name "coin"

### 4.2 Add Traversal Method

**Tasks:**
- [ ] Add `.coin(probability)` method
- [ ] Validate probability in range [0.0, 1.0]

### 4.3 Add to Anonymous Traversal

**Tasks:**
- [ ] Add `__::coin(probability)`

### 4.4 Write Tests

**Tasks:**
- [ ] Test `coin(0.0)` returns empty
- [ ] Test `coin(1.0)` returns all elements
- [ ] Test `coin(0.5)` returns approximately half (statistical)
- [ ] Test preserves traverser metadata

---

## Phase 5: SampleStep Implementation (Day 6)

### 5.1 Implement SampleStep (Reservoir Sampling)

**File:** `src/traversal/filter.rs`

**Tasks:**
- [ ] Implement `SampleStep` struct with `count: usize`
- [ ] Implement `SampleStep::new(count)`
- [ ] Implement reservoir sampling algorithm
- [ ] Implement `AnyStep` trait
- [ ] Add step name "sample"

**Reservoir Sampling Algorithm:**
```
1. Fill reservoir with first n elements
2. For each subsequent element k (k > n):
   - Generate random j in [0, k]
   - If j < n, replace reservoir[j] with element k
3. Return reservoir
```

### 5.2 Add Traversal Method

**Tasks:**
- [ ] Add `.sample(n)` method

### 5.3 Add to Anonymous Traversal

**Tasks:**
- [ ] Add `__::sample(n)`

### 5.4 Write Tests

**Tasks:**
- [ ] Test `sample(n)` on m < n elements returns all m
- [ ] Test `sample(n)` on m > n elements returns exactly n
- [ ] Test distribution is approximately uniform (statistical)
- [ ] Test empty input returns empty output

---

## Phase 6: HasKey and HasPropValue Steps (Day 7)

### 6.1 Implement HasKeyStep

**File:** `src/traversal/filter.rs`

**Tasks:**
- [ ] Implement `HasKeyStep` struct with `keys: Vec<String>`
- [ ] Implement `HasKeyStep::new(key)` and `HasKeyStep::any(keys)`
- [ ] Implement `AnyStep` trait
- [ ] Works on property values from `.properties()` step
- [ ] Add step name "hasKey"

**Note:** This step filters `Value::Map` entries or property traversers. May need to evaluate if current `properties()` step output format supports this.

### 6.2 Implement HasPropValueStep

**File:** `src/traversal/filter.rs`

**Tasks:**
- [ ] Implement `HasPropValueStep` struct with `values: Vec<Value>`
- [ ] Implement `HasPropValueStep::new(value)` and `HasPropValueStep::any(values)`
- [ ] Implement `AnyStep` trait
- [ ] Add step name "hasValue"

### 6.3 Add Traversal Methods

**Tasks:**
- [ ] Add `.has_key(key)` method (for property traversers)
- [ ] Add `.has_key_any(keys)` method
- [ ] Add `.has_prop_value(value)` method
- [ ] Add `.has_prop_value_any(values)` method

### 6.4 Add to Anonymous Traversal

**Tasks:**
- [ ] Add `__::has_key(key)`
- [ ] Add `__::has_key_any(keys)`
- [ ] Add `__::has_prop_value(value)`
- [ ] Add `__::has_prop_value_any(values)`

### 6.5 Write Tests

**Tasks:**
- [ ] Test `has_key("name")` filters to name properties
- [ ] Test `has_key_any` with multiple keys
- [ ] Test `has_prop_value("Alice")` filters correctly
- [ ] Test `has_prop_value_any` with multiple values

---

## Phase 7: WherePStep Implementation (Day 8)

### 7.1 Implement WherePStep

**File:** `src/traversal/filter.rs`

**Tasks:**
- [ ] Implement `WherePStep` struct with `predicate: Box<dyn Predicate>`
- [ ] Implement `WherePStep::new(predicate)`
- [ ] Implement `AnyStep` trait
- [ ] Test current value against predicate
- [ ] Add step name "where"

**Note:** This is similar to `IsStep` but named for Gremlin alignment.

### 7.2 Add Traversal Method

**Tasks:**
- [ ] Add `.where_p(predicate)` method
- [ ] Consider if this should merge with or complement `is_()`

### 7.3 Add to Anonymous Traversal

**Tasks:**
- [ ] Add `__::where_p(predicate)`

### 7.4 Write Tests

**Tasks:**
- [ ] Test with comparison predicates (gt, lt, eq, etc.)
- [ ] Test with set predicates (within, without)
- [ ] Test with combined predicates (and, or)

---

## Phase 8: Integration and Documentation (Days 9-10)

### 8.1 Integration Tests

**File:** `tests/filter_steps.rs` (new file or add to existing)

**Tasks:**
- [ ] Test chaining: `g.v().out().dedup_by_key("name").tail_n(5)`
- [ ] Test with ordering: `g.v().values("age").order().tail_n(3)`
- [ ] Test with other filters: `g.v().has_label("person").sample(10)`
- [ ] Test complex queries combining multiple new steps

### 8.2 Update API Documentation

**File:** `Gremlin_api.md`

**Tasks:**
- [ ] Update Filter Steps table with new implementations
- [ ] Change `-` to function names for implemented steps
- [ ] Update Implementation Summary counts

### 8.3 Add Example

**File:** `examples/filter_steps.rs`

**Tasks:**
- [ ] Create example demonstrating all new filter steps
- [ ] Show practical use cases
- [ ] Add comments explaining each step

### 8.4 Update Module Documentation

**File:** `src/traversal/filter.rs`

**Tasks:**
- [ ] Add rustdoc for all new structs
- [ ] Add module-level documentation updates
- [ ] Add usage examples in doc comments

---

## Testing Checklist

### Unit Tests

**TailStep:**
- [ ] `tail()` returns last element
- [ ] `tail_n(3)` returns last 3 elements  
- [ ] `tail_n(10)` on 5 elements returns all 5
- [ ] Empty input returns empty output
- [ ] Preserves traverser metadata

**DedupByKeyStep:**
- [ ] Dedup by property keeps first occurrence
- [ ] Missing property treated as null
- [ ] Works with vertices and edges

**DedupByLabelStep:**
- [ ] Dedup by label works correctly
- [ ] Mixed element types handled

**DedupByTraversalStep:**
- [ ] Dedup by nested traversal result

**CoinStep:**
- [ ] `coin(0.0)` returns empty
- [ ] `coin(1.0)` returns all
- [ ] `coin(0.5)` returns approximately half

**SampleStep:**
- [ ] `sample(n)` on m < n returns all m
- [ ] `sample(n)` on m > n returns exactly n
- [ ] Distribution is approximately uniform

**HasKeyStep:**
- [ ] Filters properties by single key
- [ ] Filters properties by multiple keys

**HasPropValueStep:**
- [ ] Filters properties by single value
- [ ] Filters properties by multiple values

**WherePStep:**
- [ ] Works with comparison predicates
- [ ] Works with set predicates

### Integration Tests
- [ ] Chaining new steps together
- [ ] Combining with existing steps
- [ ] Complex queries with branches

---

## Dependencies

- Existing filter module (`src/traversal/filter.rs`)
- Existing predicate module (`src/traversal/predicate.rs`)
- `rand` crate (new dependency)

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Non-deterministic tests for `coin`/`sample` | Medium | Use seeded RNG in tests, statistical tolerance |
| `tail()` memory usage on large datasets | Medium | Document barrier step behavior, suggest `limit()` first |
| `hasKey`/`hasValue` type compatibility | Low | Evaluate `properties()` output format first |
| Performance of reservoir sampling | Low | Standard algorithm, O(n) time, O(k) space |

---

## Success Criteria

1. All missing filter steps are implemented and tested
2. Tests pass with good branch coverage (>90% on new code)
3. API is consistent with existing filter step patterns
4. Documentation is complete with examples
5. `Gremlin_api.md` is updated to reflect new implementations

---

## Future Work (Out of Scope)

- `timeLimit(ms)` - Requires async runtime
- `sample(n).by(weight)` - Weighted sampling
- `dedup(scope)` - Scoped deduplication
- Seeded RNG configuration for reproducible results
