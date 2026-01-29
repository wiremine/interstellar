# Integration Test Strategy Specification

**Status**: Implemented  
**Goal**: Expand integration test coverage to expose edge cases and implementation issues in the Gremlin API

## Overview

This specification defines a systematic approach to expanding integration tests for the Interstellar Gremlin API. The strategy focuses on:

1. **Real-world query patterns** - Common graph queries users actually write
2. **Edge cases and boundary conditions** - Empty results, single elements, type mismatches
3. **Step composition complexity** - Multi-step chains that stress the traversal engine
4. **Correctness verification** - Ensuring results match expected Gremlin semantics

## Current State

### Existing Test Coverage

| Category | Files | Focus |
|----------|-------|-------|
| Source steps | `basic.rs` | `v()`, `e()`, `inject()`, `v_ids()`, `e_ids()` |
| Navigation | `navigation.rs` | `out()`, `in_()`, `both()`, edge traversals |
| Filtering | `filter.rs` | `has_*`, `filter()`, `dedup()`, `limit()`, etc. |
| Transform | `transform.rs` | `values()`, `map()`, `path()`, `select()` |
| Terminal | `terminal.rs` | `count()`, `sum()`, `fold()`, `to_list()` |
| Branch | `branch.rs` | `union()`, `coalesce()`, `choose()`, `optional()` |
| Repeat | `repeat.rs` | `repeat()` with all configurations |
| Predicates | `predicates.rs` | `p::eq()`, `p::gt()`, range predicates, etc. |
| Anonymous | `anonymous.rs` | `__` factory patterns |
| Complex | `complex.rs` | Multi-step traversal patterns |

### Coverage Gaps Identified

1. **Composition depth** - Most tests use 2-4 steps; need deeper chains
2. **Empty/null handling** - Limited tests for missing properties, empty traversals
3. **Type coercion edge cases** - Mixed types in comparisons and aggregations
4. **Barrier step interactions** - `order()`, `group()`, `sample()` with large data
5. **Path tracking in branches** - Complex path scenarios with `union()`, `repeat()`
6. **Side effect correctness** - `store()`, `aggregate()`, `cap()` semantics
7. **Streaming execution** - `streaming_execute()` for memory efficiency
8. **Concurrent snapshot isolation** - COW semantics under parallel access

---

## Test Strategy

### 1. Query Pattern Categories

Organize new tests into categories that reflect real-world usage:

#### 1.1 Neighborhood Queries
Queries that explore local graph structure around vertices.

```rust
// Direct neighbors with property filtering
g.v_ids([start]).out_labels(&["knows"]).has_where("age", p::gt(25)).to_list()

// Multi-hop with deduplication
g.v_ids([start]).out().out().out().dedup().to_list()

// Bidirectional exploration
g.v_ids([start]).both().both().dedup().limit(100).to_list()
```

#### 1.2 Path Queries
Queries that track or analyze traversal paths.

```rust
// Labeled path with select
g.v_ids([start])
    .as_("a")
    .out_labels(&["knows"])
    .as_("b")
    .out_labels(&["created"])
    .as_("c")
    .select(&["a", "b", "c"])
    .to_list()

// Path length analysis
g.v_ids([start])
    .with_path()
    .repeat(__.out())
    .times(5)
    .path()
    .to_list()

// Simple path (no revisits)
g.v_ids([start])
    .repeat(__.out().simple_path())
    .until(__.has_label("target"))
    .limit(10)
    .path()
    .to_list()
```

#### 1.3 Aggregation Queries
Queries that collect and summarize graph data.

```rust
// Group by label with counts
g.v().group_count_by(__.label()).to_list()

// Nested grouping
g.v().has_label("person")
    .group()
    .by_key(__.values("city"))
    .by_value(__.values("age").mean())
    .to_list()

// Min/max with transforms
g.v().has_label("person").values("age").max()
```

#### 1.4 Conditional Queries
Queries with branching logic.

```rust
// Choose with fallback
g.v().choose(
    __.has_label("person"),
    __.out_labels(&["knows"]),
    __.out_labels(&["contains"])
).to_list()

// Coalesce for defaults
g.v().coalesce(vec![
    __.values("nickname"),
    __.values("firstName"),
    __.constant(Value::String("Unknown".into()))
]).to_list()

// Complex and/or filters
g.v().has_label("person")
    .or_(vec![
        __.has_where("age", p::lt(25)),
        __.and_(vec![
            __.has_where("status", p::eq("active")),
            __.out_labels(&["knows"]).count().is_(p::gt(5))
        ])
    ])
    .to_list()
```

#### 1.5 Recursive Queries
Queries using repeat for variable-depth traversal.

```rust
// Fixed depth with emit
g.v_ids([start])
    .repeat(__.out())
    .times(3)
    .emit()
    .to_list()

// Until with path tracking
g.v_ids([start])
    .repeat(__.out_labels(&["reports_to"]))
    .until(__.has_value("level", "executive"))
    .emit()
    .path()
    .to_list()

// Conditional emit
g.v_ids([start])
    .repeat(__.out())
    .times(5)
    .emit_if(__.has_where("importance", p::gt(0.8)))
    .to_list()
```

### 2. Edge Case Testing

Create dedicated tests for boundary conditions:

#### 2.1 Empty Result Handling

```rust
#[test]
fn empty_start_set() {
    let g = create_empty_graph().snapshot().gremlin();
    assert_eq!(g.v().to_list().len(), 0);
    assert_eq!(g.v().out().count(), 0);
    assert_eq!(g.v().values("name").to_list().len(), 0);
}

#[test]
fn filter_removes_all() {
    let tg = create_small_graph();
    let g = tg.snapshot().gremlin();
    
    // Filter that matches nothing
    let results = g.v().has_value("nonexistent", "value").to_list();
    assert!(results.is_empty());
    
    // Chained steps after empty
    let results = g.v()
        .has_value("nonexistent", "value")
        .out()
        .values("name")
        .to_list();
    assert!(results.is_empty());
}

#[test]
fn empty_traversal_in_where() {
    let tg = create_small_graph();
    let g = tg.snapshot().gremlin();
    
    // where_() with non-matching sub-traversal
    let results = g.v()
        .has_label("person")
        .where_(__.out_labels(&["manages"]))  // No "manages" edges exist
        .to_list();
    assert!(results.is_empty());
}
```

#### 2.2 Single Element Handling

```rust
#[test]
fn single_vertex_operations() {
    let tg = create_small_graph();
    let g = tg.snapshot().gremlin();
    
    // one() on single result
    let result = g.v_ids([tg.alice]).one();
    assert!(result.is_ok());
    
    // one() fails on multiple
    let result = g.v().has_label("person").one();
    assert!(result.is_err());
    
    // one() fails on empty
    let result = g.v().has_value("name", "Nobody").one();
    assert!(result.is_err());
}

#[test]
fn single_element_aggregations() {
    let tg = create_small_graph();
    let g = tg.snapshot().gremlin();
    
    // min/max on single value
    let min = g.v_ids([tg.alice]).values("age").min();
    let max = g.v_ids([tg.alice]).values("age").max();
    assert_eq!(min, max);  // Single element: min == max
}
```

#### 2.3 Type Handling

```rust
#[test]
fn mixed_type_comparisons() {
    // Graph with mixed property types
    let graph = Graph::new();
    let v1 = graph.add_vertex("test", [("value", Value::Int(42))].into());
    let v2 = graph.add_vertex("test", [("value", Value::Float(42.0))].into());
    let v3 = graph.add_vertex("test", [("value", Value::String("42"))].into());
    
    let g = graph.snapshot().gremlin();
    
    // Numeric comparison should handle Int vs Float
    let results = g.v().has_where("value", p::eq(Value::Int(42))).to_list();
    // Document expected behavior: strict type matching or coercion
}

#[test]
fn missing_property_handling() {
    let graph = Graph::new();
    let v1 = graph.add_vertex("test", [("name", Value::String("A"))].into());
    let v2 = graph.add_vertex("test", [].into());  // No "name" property
    
    let g = graph.snapshot().gremlin();
    
    // values() should skip vertices without property
    let names = g.v().values("name").to_list();
    assert_eq!(names.len(), 1);
    
    // has() filters out vertices without property
    let with_name = g.v().has("name").to_list();
    assert_eq!(with_name.len(), 1);
}
```

#### 2.4 Large Result Sets

```rust
#[test]
fn barrier_step_memory_behavior() {
    // Create graph with 10,000 vertices
    let graph = Graph::new();
    for i in 0..10_000 {
        graph.add_vertex("node", [("index", Value::Int(i))].into());
    }
    
    let g = graph.snapshot().gremlin();
    
    // order() is a barrier - must buffer all results
    let ordered = g.v().values("index").order().by_asc().limit(10).to_list();
    assert_eq!(ordered.len(), 10);
    
    // Verify order correctness
    for i in 0..10 {
        assert_eq!(ordered[i], Value::Int(i as i64));
    }
}

#[test]
fn streaming_vs_barrier() {
    let graph = create_large_graph(1000);  // Helper to create test data
    let g = graph.snapshot().gremlin();
    
    // Non-barrier chain should stream
    let count = g.v().out().has_label("target").count();
    
    // Barrier forces collection
    let count_with_order = g.v().out().order().by_asc().has_label("target").count();
    
    // Results should match
    assert_eq!(count, count_with_order);
}
```

### 3. Composition Depth Testing

Test increasingly complex step chains:

#### 3.1 Deep Navigation Chains

```rust
#[test]
fn deep_traversal_chain() {
    let tg = create_social_graph();
    let g = tg.snapshot().gremlin();
    
    // 6-step navigation chain
    let results = g.v_ids([tg.alice])
        .out_labels(&["knows"])      // 1
        .out_labels(&["knows"])      // 2
        .in_labels(&["knows"])       // 3
        .out_labels(&["created"])    // 4
        .in_labels(&["uses"])        // 5
        .out_labels(&["knows"])      // 6
        .dedup()
        .to_list();
    
    // Verify path makes sense
    assert!(!results.is_empty());
}
```

#### 3.2 Mixed Step Types

```rust
#[test]
fn mixed_step_chain() {
    let tg = create_social_graph();
    let g = tg.snapshot().gremlin();
    
    let results = g.v()
        .has_label("person")                           // filter
        .where_(__.out_labels(&["knows"]).count().is_(p::gte(1)))  // branch + aggregate
        .out_labels(&["knows"])                        // navigation
        .has_where("age", p::gt(25))                   // filter with predicate
        .as_("friend")                                 // transform (label)
        .out_labels(&["created"])                      // navigation
        .has_label("software")                         // filter
        .as_("software")                               // transform (label)
        .select(&["friend", "software"])               // transform (select)
        .to_list();
    
    for result in &results {
        if let Value::Map(map) = result {
            assert!(map.contains_key("friend"));
            assert!(map.contains_key("software"));
        }
    }
}
```

#### 3.3 Nested Anonymous Traversals

```rust
#[test]
fn deeply_nested_anonymous() {
    let tg = create_social_graph();
    let g = tg.snapshot().gremlin();
    
    // Nested where clauses
    let results = g.v()
        .has_label("person")
        .where_(
            __.out_labels(&["knows"])
              .where_(
                  __.out_labels(&["created"])
                    .has_label("software")
              )
        )
        .values("name")
        .to_list();
    
    // People who know someone who created software
    assert!(!results.is_empty());
}

#[test]
fn nested_branching() {
    let tg = create_medium_graph();
    let g = tg.snapshot().gremlin();
    
    let results = g.v()
        .has_label("person")
        .choose(
            __.has_where("status", p::eq("active")),
            __.union(vec![
                __.out_labels(&["knows"]),
                __.out_labels(&["created"])
            ]),
            __.constant(Value::Null)
        )
        .to_list();
    
    // Active people get union results, inactive get null
    assert!(!results.is_empty());
}
```

### 4. Semantic Correctness Tests

Verify results match expected Gremlin semantics:

#### 4.1 Order Preservation

```rust
#[test]
fn navigation_order_consistency() {
    let tg = create_small_graph();
    let g = tg.snapshot().gremlin();
    
    // Multiple runs should return same order (deterministic iteration)
    let run1 = g.v_ids([tg.alice]).out().to_list();
    let run2 = g.v_ids([tg.alice]).out().to_list();
    
    assert_eq!(run1.len(), run2.len());
    for (a, b) in run1.iter().zip(run2.iter()) {
        assert_eq!(a, b);
    }
}

#[test]
fn order_step_correctness() {
    let tg = create_small_graph();
    let g = tg.snapshot().gremlin();
    
    let ages = g.v()
        .has_label("person")
        .values("age")
        .order().by_asc()
        .to_list();
    
    // Verify sorted order
    let age_vals: Vec<i64> = ages.iter().filter_map(|v| v.as_i64()).collect();
    for i in 1..age_vals.len() {
        assert!(age_vals[i-1] <= age_vals[i]);
    }
}
```

#### 4.2 Dedup Semantics

```rust
#[test]
fn dedup_preserves_first() {
    let tg = create_small_graph();
    let g = tg.snapshot().gremlin();
    
    // In cycle: Alice -> Bob -> Charlie -> Alice
    // dedup should keep first occurrence
    let path = g.v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(4)
        .emit()
        .to_list();
    
    // Contains duplicates
    let has_alice = path.iter().filter(|v| v.as_vertex_id() == Some(tg.alice)).count();
    assert!(has_alice >= 2);
    
    let deduped = g.v_ids([tg.alice])
        .repeat(__.out_labels(&["knows"]))
        .times(4)
        .emit()
        .dedup()
        .to_list();
    
    // No duplicates
    let alice_count = deduped.iter().filter(|v| v.as_vertex_id() == Some(tg.alice)).count();
    assert_eq!(alice_count, 1);
}
```

#### 4.3 Group Semantics

```rust
#[test]
fn group_by_preserves_all_values() {
    let tg = create_social_graph();
    let g = tg.snapshot().gremlin();
    
    let grouped = g.v()
        .has_label("person")
        .group()
        .by_key(__.values("city"))
        .by_value(__.values("name").fold())
        .to_list();
    
    // Verify all people are accounted for
    let total_people: usize = grouped.iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                Some(map.values()
                    .filter_map(|v| if let Value::List(l) = v { Some(l.len()) } else { None })
                    .sum::<usize>())
            } else { None }
        })
        .sum();
    
    assert_eq!(total_people, 5);  // All 5 people in social graph
}
```

### 5. Test Graph Fixtures

Add new specialized test graphs:

#### 5.1 Hierarchical Graph (Org Chart)

```rust
/// Creates an organizational hierarchy for testing recursive patterns.
///
/// Structure:
///   CEO
///   ├── CTO
///   │   ├── Engineering Manager 1
///   │   │   ├── Developer 1
///   │   │   └── Developer 2
///   │   └── Engineering Manager 2
///   │       └── Developer 3
///   └── CFO
///       └── Finance Manager
///           └── Accountant
pub fn create_org_graph() -> OrgTestGraph {
    // Implementation
}
```

#### 5.2 Dense Graph (High Connectivity)

```rust
/// Creates a densely connected graph for stress testing.
///
/// - 100 vertices
/// - Each vertex connected to ~20% of other vertices
/// - Tests traversal performance and dedup efficiency
pub fn create_dense_graph(vertex_count: usize, edge_probability: f64) -> Graph {
    // Implementation
}
```

#### 5.3 Property-Rich Graph

```rust
/// Creates a graph with diverse property types for type handling tests.
///
/// Properties include:
/// - Strings, Integers, Floats, Booleans
/// - Null values
/// - Nested maps and lists (if supported)
pub fn create_property_test_graph() -> Graph {
    // Implementation
}
```

---

## Implementation Plan

### Phase 1: Edge Case Tests (Priority: High)

Create `tests/traversal/edge_cases.rs`:

1. Empty traversal handling (10+ tests)
2. Single element operations (5+ tests)
3. Missing property handling (5+ tests)
4. Type coercion scenarios (5+ tests)

### Phase 2: Query Pattern Tests (Priority: High)

Create `tests/traversal/patterns/`:

1. `neighborhood.rs` - Neighborhood exploration patterns (10+ tests)
2. `paths.rs` - Path tracking and analysis (10+ tests)
3. `aggregations.rs` - Grouping and summarization (10+ tests)
4. `conditionals.rs` - Branching and conditional logic (10+ tests)
5. `recursive.rs` - Repeat and variable-depth patterns (10+ tests)

### Phase 3: Composition Tests (Priority: Medium)

Create `tests/traversal/composition.rs`:

1. Deep navigation chains (5+ tests)
2. Mixed step type chains (5+ tests)
3. Nested anonymous traversals (5+ tests)
4. Complex real-world queries (10+ tests)

### Phase 4: Semantic Correctness (Priority: Medium)

Create `tests/traversal/semantics.rs`:

1. Order preservation tests (5+ tests)
2. Dedup semantics (5+ tests)
3. Group/reduce semantics (5+ tests)
4. Path tracking correctness (5+ tests)

### Phase 5: Performance Edge Cases (Priority: Low)

Create `tests/traversal/performance.rs`:

1. Large result set handling (5+ tests)
2. Barrier step memory behavior (5+ tests)
3. Streaming vs buffered execution (5+ tests)

### Phase 6: Additional Test Fixtures (Priority: Medium)

Update `tests/common/graphs.rs`:

1. `create_org_graph()` - Hierarchical structure
2. `create_dense_graph()` - High connectivity
3. `create_property_test_graph()` - Type diversity
4. `create_large_graph()` - Parameterized size

---

## Test Naming Conventions

Follow consistent naming:

```rust
// Pattern: {step_or_feature}_{scenario}_{expected_outcome}
fn out_with_label_filter_returns_matching_vertices()
fn where_with_empty_subtraversal_filters_all()
fn group_by_label_preserves_all_values()
fn repeat_until_with_path_tracks_all_steps()
```

## Success Criteria

1. **Coverage**: 90%+ branch coverage on traversal modules
2. **Breadth**: Every implemented step has at least 3 integration tests
3. **Depth**: At least 20 tests with 5+ step chains
4. **Edge cases**: Every step tested with empty inputs
5. **Semantics**: Core Gremlin semantics verified against TinkerPop documentation

## Appendix: Query Patterns Reference

### Gremlin Recipe Patterns to Test

| Pattern | Description | Steps Involved |
|---------|-------------|----------------|
| Friends of Friends | 2-hop neighborhood | `out().out().dedup()` |
| Shortest Path | BFS with limit | `repeat().until().limit().path()` |
| Centrality | Degree counting | `both().count()` per vertex |
| Common Neighbors | Set intersection | `where()` with subtraversal |
| Recommendations | Similar items | `in().out().dedup().limit()` |
| Hierarchy Traversal | Tree walking | `repeat().emit().until()` |
| Property Search | Index usage | `v_by_property()` |
| Aggregated Stats | Grouping | `group().by_key().by_value()` |

### Anonymous Traversal Patterns

| Pattern | Example |
|---------|---------|
| Filter condition | `where_(__.out().has_label("x"))` |
| Negation | `not(__.out())` |
| Conjunction | `and_(vec![__.has("a"), __.has("b")])` |
| Disjunction | `or_(vec![__.has("a"), __.has("b")])` |
| Branching | `choose(__.has("x"), __.out(), __.in_())` |
| Merging | `union(vec![__.out("a"), __.out("b")])` |
| Fallback | `coalesce(vec![__.values("x"), __.constant(default)])` |
| Iteration | `repeat(__.out()).times(n)` |
