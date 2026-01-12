# Plan 03 Phase 5.8: Integration Tests

**Phase 5.8 of Traversal Engine Core Implementation**

Based on: `specs/spec-03-traversal-engine-core.md` and `plans/plan-03.md`

---

## Overview

This document specifies the integration tests required for Phase 5.8 of the Traversal Engine Core. These tests validate the complete traversal system working end-to-end with a realistic test graph.

**File**: `tests/traversal.rs`

---

## Test Graph Structure

All tests use a standardized test graph with the following structure:

### Vertices (4 total)
| ID | Label | Properties |
|----|-------|------------|
| alice | person | name: "Alice", age: 30 |
| bob | person | name: "Bob", age: 35 |
| carol | person | name: "Carol", age: 25 |
| acme | company | name: "Acme Corp" |

### Edges (5 total)
| Source | Target | Label | Properties |
|--------|--------|-------|------------|
| alice | bob | knows | weight: 1.0 |
| alice | carol | knows | weight: 0.5 |
| bob | carol | knows | weight: 0.8 |
| alice | acme | works_at | since: 2020 |
| bob | acme | works_at | since: 2018 |

---

## Test Categories

### 1. Basic Source Tests (`v()`, `e()`, `count()`)

```rust
#[test]
fn test_v_all_vertices() {
    // g.v() should return all 4 vertices
    let count = g.v().count();
    assert_eq!(count, 4);
}

#[test]
fn test_e_all_edges() {
    // g.e() should return all 5 edges
    let count = g.e().count();
    assert_eq!(count, 5);
}

#[test]
fn test_v_ids_specific_vertices() {
    // g.v_ids([alice, bob]) should return 2 vertices
    let count = g.v_ids([alice, bob]).count();
    assert_eq!(count, 2);
}

#[test]
fn test_v_ids_nonexistent_filtered() {
    // Non-existent IDs should be filtered out silently
    let fake_id = VertexId(999999);
    let count = g.v_ids([alice, fake_id]).count();
    assert_eq!(count, 1);
}

#[test]
fn test_e_ids_specific_edges() {
    // g.e_ids() should return only specified edges
    let first_edge = g.e().next().unwrap().as_edge_id().unwrap();
    let count = g.e_ids([first_edge]).count();
    assert_eq!(count, 1);
}

#[test]
fn test_inject_values() {
    // g.inject() should inject arbitrary values
    let results = g.inject([1i64, 2i64, 3i64]).to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(1));
    assert_eq!(results[1], Value::Int(2));
    assert_eq!(results[2], Value::Int(3));
}
```

### 2. Filter Chain Tests (`has_label`, `has_value`, `dedup`, `limit`)

```rust
#[test]
fn test_has_label_person() {
    // g.v().has_label("person") should return 3 vertices
    let count = g.v().has_label("person").count();
    assert_eq!(count, 3);
}

#[test]
fn test_has_label_company() {
    // g.v().has_label("company") should return 1 vertex
    let count = g.v().has_label("company").count();
    assert_eq!(count, 1);
}

#[test]
fn test_has_label_nonexistent() {
    // g.v().has_label("robot") should return 0 vertices
    let count = g.v().has_label("robot").count();
    assert_eq!(count, 0);
}

#[test]
fn test_has_label_any() {
    // g.v().has_label_any(["person", "company"]) should return 4 vertices
    let count = g.v().has_label_any(["person", "company"]).count();
    assert_eq!(count, 4);
}

#[test]
fn test_has_label_edge() {
    // g.e().has_label("knows") should return 3 edges
    let count = g.e().has_label("knows").count();
    assert_eq!(count, 3);
}

#[test]
fn test_has_property_exists() {
    // g.v().has("age") should return 3 person vertices
    let count = g.v().has("age").count();
    assert_eq!(count, 3);
}

#[test]
fn test_has_property_not_exists() {
    // g.v().has("salary") should return 0 vertices
    let count = g.v().has("salary").count();
    assert_eq!(count, 0);
}

#[test]
fn test_has_value_string() {
    // g.v().has_value("name", "Alice") should return 1 vertex
    let count = g.v().has_value("name", "Alice").count();
    assert_eq!(count, 1);
}

#[test]
fn test_has_value_int() {
    // g.v().has_value("age", 30) should return Alice
    let count = g.v().has_value("age", 30i64).count();
    assert_eq!(count, 1);
}

#[test]
fn test_has_value_no_match() {
    // g.v().has_value("name", "Nobody") should return 0
    let count = g.v().has_value("name", "Nobody").count();
    assert_eq!(count, 0);
}

#[test]
fn test_has_value_chained() {
    // g.v().has_label("person").has_value("age", 30) should return Alice
    let count = g.v()
        .has_label("person")
        .has_value("age", 30i64)
        .count();
    assert_eq!(count, 1);
}

#[test]
fn test_has_id_vertex() {
    // g.v().has_id(alice) should return 1 vertex
    let count = g.v().has_id(alice).count();
    assert_eq!(count, 1);
}

#[test]
fn test_has_ids_multiple() {
    // g.v().has_ids([alice, bob]) should return 2 vertices
    let count = g.v().has_ids([alice, bob]).count();
    assert_eq!(count, 2);
}

#[test]
fn test_filter_custom_predicate() {
    // Custom filter on integer values
    let results = g
        .inject([1i64, 2i64, 3i64, 4i64, 5i64])
        .filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 2))
        .to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results, vec![Value::Int(3), Value::Int(4), Value::Int(5)]);
}

#[test]
fn test_filter_vertex_property() {
    // Filter vertices by property value using context
    let count = g.v()
        .has_label("person")
        .filter(|ctx, v| {
            if let Some(id) = v.as_vertex_id() {
                if let Some(vertex) = ctx.snapshot.get_vertex(id) {
                    if let Some(Value::Int(age)) = vertex.property("age") {
                        return *age >= 30;
                    }
                }
            }
            false
        })
        .count();
    assert_eq!(count, 2); // Alice (30) and Bob (35)
}

#[test]
fn test_dedup_basic() {
    // Dedup on duplicate values
    let results = g.inject([1i64, 2i64, 1i64, 3i64, 2i64]).dedup().to_list();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_dedup_preserves_order() {
    // Dedup should preserve first occurrence order
    let results = g.inject([1i64, 2i64, 1i64, 3i64, 2i64]).dedup().to_list();
    assert_eq!(results[0], Value::Int(1));
    assert_eq!(results[1], Value::Int(2));
    assert_eq!(results[2], Value::Int(3));
}

#[test]
fn test_dedup_vertices() {
    // Dedup vertices after navigation (may have duplicates)
    let count_without_dedup = g.v()
        .has_label("person")
        .out()
        .out()
        .count();
    let count_with_dedup = g.v()
        .has_label("person")
        .out()
        .out()
        .dedup()
        .count();
    assert!(count_with_dedup <= count_without_dedup);
}

#[test]
fn test_limit_basic() {
    // limit(2) should return at most 2 results
    let results = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).limit(2).to_list();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::Int(1));
    assert_eq!(results[1], Value::Int(2));
}

#[test]
fn test_limit_exceeds_count() {
    // limit(10) on 5 elements returns 5
    let results = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).limit(10).to_list();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_limit_zero() {
    // limit(0) should return empty
    let results = g.inject([1i64, 2i64, 3i64]).limit(0).to_list();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_skip_basic() {
    // skip(2) should skip first 2
    let results = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).skip(2).to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(3));
}

#[test]
fn test_skip_exceeds_count() {
    // skip(10) on 5 elements returns empty
    let results = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).skip(10).to_list();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_range_basic() {
    // range(2, 5) should return elements at indices 2, 3, 4
    let results = g.inject([0i64, 1i64, 2i64, 3i64, 4i64, 5i64]).range(2, 5).to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(2));
    assert_eq!(results[1], Value::Int(3));
    assert_eq!(results[2], Value::Int(4));
}

#[test]
fn test_range_pagination() {
    // Pagination: page 1 with page_size 2
    let page_size = 2;
    let page = 1;
    let results = g
        .inject([0i64, 1i64, 2i64, 3i64, 4i64, 5i64])
        .range(page * page_size, (page + 1) * page_size)
        .to_list();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::Int(2));
    assert_eq!(results[1], Value::Int(3));
}
```

### 3. Navigation Tests (`out`, `in_`, `both`, edge variants)

```rust
#[test]
fn test_out_all_edges() {
    // Alice has 3 outgoing edges (bob, carol, acme)
    let count = g.v_ids([alice]).out().count();
    assert_eq!(count, 3);
}

#[test]
fn test_out_with_label() {
    // Alice --knows--> 2 people (bob, carol)
    let count = g.v_ids([alice]).out_labels(&["knows"]).count();
    assert_eq!(count, 2);
}

#[test]
fn test_out_multiple_labels() {
    // Alice --knows|works_at--> 3 vertices
    let count = g.v_ids([alice]).out_labels(&["knows", "works_at"]).count();
    assert_eq!(count, 3);
}

#[test]
fn test_out_no_matches() {
    // Alice has no "likes" edges
    let count = g.v_ids([alice]).out_labels(&["likes"]).count();
    assert_eq!(count, 0);
}

#[test]
fn test_out_non_vertex_produces_nothing() {
    // Calling out() on non-vertex values produces no results
    let count = g.inject([42i64]).out().count();
    assert_eq!(count, 0);
}

#[test]
fn test_in_all_edges() {
    // Carol has 2 incoming "knows" edges (from alice, bob)
    let count = g.v_ids([carol]).in_().count();
    assert_eq!(count, 2);
}

#[test]
fn test_in_with_label() {
    // Acme has 2 incoming "works_at" edges
    let count = g.v_ids([acme]).in_labels(&["works_at"]).count();
    assert_eq!(count, 2);
}

#[test]
fn test_in_no_matches() {
    // Alice has no incoming "works_at" edges
    let count = g.v_ids([alice]).in_labels(&["works_at"]).count();
    assert_eq!(count, 0);
}

#[test]
fn test_both_all_edges() {
    // Bob's neighbors: alice (in), carol (out), acme (out), carol (in from alice->carol? no that's wrong)
    // Actually: bob.in() = [alice], bob.out() = [carol, acme]
    // So bob.both() = [alice, carol, acme] = 3
    let count = g.v_ids([bob]).both().count();
    assert_eq!(count, 3);
}

#[test]
fn test_both_with_label() {
    // Bob <--knows--> neighbors: alice (in), carol (out) = 2
    let count = g.v_ids([bob]).both_labels(&["knows"]).count();
    assert_eq!(count, 2);
}

#[test]
fn test_out_e_all_edges() {
    // Alice has 3 outgoing edges
    let count = g.v_ids([alice]).out_e().count();
    assert_eq!(count, 3);
}

#[test]
fn test_out_e_with_label() {
    // Alice has 2 outgoing "knows" edges
    let count = g.v_ids([alice]).out_e_labels(&["knows"]).count();
    assert_eq!(count, 2);
}

#[test]
fn test_in_e_all_edges() {
    // Acme has 2 incoming edges
    let count = g.v_ids([acme]).in_e().count();
    assert_eq!(count, 2);
}

#[test]
fn test_in_e_with_label() {
    // Acme has 2 incoming "works_at" edges
    let count = g.v_ids([acme]).in_e_labels(&["works_at"]).count();
    assert_eq!(count, 2);
}

#[test]
fn test_both_e_all_edges() {
    // Bob has 3 incident edges: 1 in + 2 out
    let count = g.v_ids([bob]).both_e().count();
    assert_eq!(count, 3);
}

#[test]
fn test_both_e_with_label() {
    // Bob has 2 incident "knows" edges
    let count = g.v_ids([bob]).both_e_labels(&["knows"]).count();
    assert_eq!(count, 2);
}

#[test]
fn test_out_v_from_edge() {
    // out_v() returns source vertex of edge
    // All "knows" edges have persons as source
    let count = g.e().has_label("knows").out_v().has_label("person").count();
    assert_eq!(count, 3);
}

#[test]
fn test_in_v_from_edge() {
    // in_v() returns target vertex of edge
    // "works_at" edges target acme
    let targets = g.e().has_label("works_at").in_v().to_list();
    assert_eq!(targets.len(), 2);
    // Both should be the same company vertex
    let ids: std::collections::HashSet<_> = targets.iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();
    assert_eq!(ids.len(), 1); // All point to same company
}

#[test]
fn test_both_v_from_edge() {
    // both_v() returns both vertices of edge (2 per edge)
    let count = g.e().has_label("works_at").both_v().count();
    assert_eq!(count, 4); // 2 edges * 2 vertices each
}

#[test]
fn test_out_v_non_edge_produces_nothing() {
    // out_v() on non-edge values produces no results
    let count = g.v().out_v().count();
    assert_eq!(count, 0);
}

#[test]
fn test_two_hop_navigation() {
    // Alice -> friends -> friends of friends
    let count = g.v_ids([alice])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .count();
    // Alice -> bob -> carol, Alice -> carol -> (none)
    assert!(count >= 1);
}

#[test]
fn test_two_hop_with_dedup() {
    // Deduplicated friends of friends
    let count = g.v_ids([alice])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .dedup()
        .count();
    // Should be unique vertices only
    let vertices = g.v_ids([alice])
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .dedup()
        .to_list();
    let ids: std::collections::HashSet<_> = vertices.iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();
    assert_eq!(vertices.len(), ids.len());
}
```

### 4. Transform Tests (`values`, `id`, `label`, `map`)

```rust
#[test]
fn test_values_single_property() {
    // Extract "name" property from all persons
    let names = g.v().has_label("person").values("name").to_list();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&Value::String("Alice".to_string())));
    assert!(names.contains(&Value::String("Bob".to_string())));
    assert!(names.contains(&Value::String("Carol".to_string())));
}

#[test]
fn test_values_missing_property() {
    // Extract "salary" property (doesn't exist) - returns empty
    let salaries = g.v().has_label("person").values("salary").to_list();
    assert_eq!(salaries.len(), 0);
}

#[test]
fn test_values_multi() {
    // Extract multiple properties
    let props = g.v_ids([alice]).values_multi(&["name", "age"]).to_list();
    assert_eq!(props.len(), 2);
    assert!(props.contains(&Value::String("Alice".to_string())));
    assert!(props.contains(&Value::Int(30)));
}

#[test]
fn test_values_from_edge() {
    // Extract "weight" from "knows" edges
    let weights = g.e().has_label("knows").values("weight").to_list();
    assert_eq!(weights.len(), 3);
}

#[test]
fn test_id_from_vertex() {
    // id() extracts vertex ID as Int
    let ids = g.v().has_label("person").id().to_list();
    assert_eq!(ids.len(), 3);
    for id_val in &ids {
        assert!(matches!(id_val, Value::Int(_)));
    }
}

#[test]
fn test_id_from_edge() {
    // id() extracts edge ID as Int
    let ids = g.e().id().to_list();
    assert_eq!(ids.len(), 5);
    for id_val in &ids {
        assert!(matches!(id_val, Value::Int(_)));
    }
}

#[test]
fn test_id_from_non_element() {
    // id() on non-element values passes through unchanged
    let results = g.inject([42i64]).id().to_list();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(42));
}

#[test]
fn test_label_from_vertex() {
    // label() extracts vertex label as String
    let labels = g.v().has_label("person").label().to_list();
    assert_eq!(labels.len(), 3);
    for label_val in &labels {
        assert_eq!(label_val, &Value::String("person".to_string()));
    }
}

#[test]
fn test_label_from_edge() {
    // label() extracts edge label as String
    let labels = g.e().has_label("knows").label().to_list();
    assert_eq!(labels.len(), 3);
    for label_val in &labels {
        assert_eq!(label_val, &Value::String("knows".to_string()));
    }
}

#[test]
fn test_label_from_non_element() {
    // label() on non-element values produces nothing (filtered out)
    let results = g.inject([42i64]).label().to_list();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_map_transform() {
    // map() transforms each value
    let results = g
        .inject([1i64, 2i64, 3i64])
        .map(|_ctx, v| {
            match v {
                Value::Int(n) => Value::Int(n * 2),
                other => other.clone(),
            }
        })
        .to_list();
    assert_eq!(results, vec![Value::Int(2), Value::Int(4), Value::Int(6)]);
}

#[test]
fn test_map_with_context() {
    // map() can access ExecutionContext
    let count = g.v()
        .map(|ctx, v| {
            if let Some(id) = v.as_vertex_id() {
                if ctx.snapshot.get_vertex(id).is_some() {
                    return Value::Bool(true);
                }
            }
            Value::Bool(false)
        })
        .filter(|_ctx, v| matches!(v, Value::Bool(true)))
        .count();
    assert_eq!(count, 4); // All 4 vertices exist
}

#[test]
fn test_flat_map_expand() {
    // flat_map() expands to multiple values
    let results = g
        .inject([1i64, 2i64])
        .flat_map(|_ctx, v| {
            match v {
                Value::Int(n) => vec![Value::Int(*n), Value::Int(*n * 10)],
                _ => vec![],
            }
        })
        .to_list();
    assert_eq!(results.len(), 4);
    assert_eq!(results, vec![Value::Int(1), Value::Int(10), Value::Int(2), Value::Int(20)]);
}

#[test]
fn test_constant_replace() {
    // constant() replaces all values with constant
    let results = g.inject([1i64, 2i64, 3i64]).constant("X").to_list();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::String("X".to_string()));
    assert_eq!(results[1], Value::String("X".to_string()));
    assert_eq!(results[2], Value::String("X".to_string()));
}

#[test]
fn test_path_basic() {
    // path() returns the traversal path
    let paths = g.v_ids([alice]).out_labels(&["knows"]).path().to_list();
    assert_eq!(paths.len(), 2); // Alice -> bob, Alice -> carol
    
    // Each path should be a list
    for path in &paths {
        assert!(matches!(path, Value::List(_)));
    }
}
```

### 5. Terminal Tests (`to_list`, `one`, `count`, `sum`)

```rust
#[test]
fn test_to_list_basic() {
    let results = g.v().to_list();
    assert_eq!(results.len(), 4);
}

#[test]
fn test_to_list_empty() {
    let results = g.v().has_label("robot").to_list();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_to_set_deduplicates() {
    let results = g.inject([1i64, 2i64, 1i64, 3i64, 2i64]).to_set();
    assert_eq!(results.len(), 3);
    assert!(results.contains(&Value::Int(1)));
    assert!(results.contains(&Value::Int(2)));
    assert!(results.contains(&Value::Int(3)));
}

#[test]
fn test_next_returns_first() {
    let result = g.inject([10i64, 20i64, 30i64]).next();
    assert_eq!(result, Some(Value::Int(10)));
}

#[test]
fn test_next_empty_returns_none() {
    let result = g.v().has_label("robot").next();
    assert_eq!(result, None);
}

#[test]
fn test_one_success() {
    let result = g.inject([42i64]).one();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::Int(42));
}

#[test]
fn test_one_empty_error() {
    let result = g.v().has_label("robot").one();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), TraversalError::NotOne(0)));
}

#[test]
fn test_one_multiple_error() {
    let result = g.inject([1i64, 2i64]).one();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), TraversalError::NotOne(2)));
}

#[test]
fn test_one_many_error() {
    let result = g.v().has_label("person").one();
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), TraversalError::NotOne(3)));
}

#[test]
fn test_has_next_true() {
    assert!(g.v().has_next());
}

#[test]
fn test_has_next_false() {
    assert!(!g.v().has_label("robot").has_next());
}

#[test]
fn test_iterate_consumes() {
    // iterate() should consume traversal without error
    g.v().iterate();
    // If we get here without panic, test passes
}

#[test]
fn test_count_basic() {
    assert_eq!(g.v().count(), 4);
    assert_eq!(g.e().count(), 5);
}

#[test]
fn test_count_filtered() {
    assert_eq!(g.v().has_label("person").count(), 3);
    assert_eq!(g.e().has_label("knows").count(), 3);
}

#[test]
fn test_count_empty() {
    assert_eq!(g.v().has_label("robot").count(), 0);
}

#[test]
fn test_take_basic() {
    let results = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).take(3);
    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Int(1));
}

#[test]
fn test_take_exceeds() {
    let results = g.inject([1i64, 2i64]).take(10);
    assert_eq!(results.len(), 2);
}

#[test]
fn test_sum_integers() {
    let result = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).sum();
    assert_eq!(result, Value::Float(15.0));
}

#[test]
fn test_sum_mixed() {
    let values: Vec<Value> = vec![Value::Int(10), Value::Float(2.5), Value::Int(7)];
    let result = g.inject(values).sum();
    assert_eq!(result, Value::Float(19.5));
}

#[test]
fn test_sum_empty() {
    let result = g.v().has_label("robot").sum();
    assert_eq!(result, Value::Float(0.0));
}

#[test]
fn test_sum_ignores_non_numeric() {
    let values: Vec<Value> = vec![
        Value::Int(5),
        Value::String("ignored".to_string()),
        Value::Int(3),
    ];
    let result = g.inject(values).sum();
    assert_eq!(result, Value::Float(8.0));
}

#[test]
fn test_min_integers() {
    let result = g.inject([5i64, 2i64, 8i64, 1i64, 9i64]).min();
    assert_eq!(result, Some(Value::Int(1)));
}

#[test]
fn test_min_strings() {
    let result = g.inject(["banana", "apple", "cherry"]).min();
    assert_eq!(result, Some(Value::String("apple".to_string())));
}

#[test]
fn test_min_empty() {
    let result = g.v().has_label("robot").min();
    assert_eq!(result, None);
}

#[test]
fn test_max_integers() {
    let result = g.inject([5i64, 2i64, 8i64, 1i64, 9i64]).max();
    assert_eq!(result, Some(Value::Int(9)));
}

#[test]
fn test_max_strings() {
    let result = g.inject(["banana", "apple", "cherry"]).max();
    assert_eq!(result, Some(Value::String("cherry".to_string())));
}

#[test]
fn test_max_empty() {
    let result = g.v().has_label("robot").max();
    assert_eq!(result, None);
}

#[test]
fn test_fold_sum() {
    let result = g
        .inject([1i64, 2i64, 3i64, 4i64])
        .fold(0i64, |acc, v| acc + v.as_i64().unwrap_or(0));
    assert_eq!(result, 10);
}

#[test]
fn test_fold_product() {
    let result = g
        .inject([2i64, 3i64, 4i64])
        .fold(1i64, |acc, v| acc * v.as_i64().unwrap_or(1));
    assert_eq!(result, 24);
}

#[test]
fn test_fold_concat() {
    let result = g
        .inject(["Hello", " ", "World"])
        .fold(String::new(), |mut acc, v| {
            if let Some(s) = v.as_str() {
                acc.push_str(s);
            }
            acc
        });
    assert_eq!(result, "Hello World");
}

#[test]
fn test_iter_usage() {
    let doubled: Vec<i64> = g
        .inject([1i64, 2i64, 3i64])
        .iter()
        .filter_map(|v| v.as_i64())
        .map(|n| n * 2)
        .collect();
    assert_eq!(doubled, vec![2, 4, 6]);
}

#[test]
fn test_traversers_metadata() {
    // traversers() returns Traversers with metadata
    for traverser in g.v().has_label("person").traversers() {
        assert!(traverser.value.is_vertex());
    }
}
```

### 6. Anonymous Traversal `append()` Tests

```rust
#[test]
fn test_append_out_step() {
    // Append anonymous out() traversal
    let anon = Traversal::<Value, Value>::new()
        .add_step(OutStep::new());
    
    let count = g.v_ids([alice])
        .append(anon)
        .count();
    
    assert_eq!(count, 3); // Alice's 3 outgoing neighbors
}

#[test]
fn test_append_filter_step() {
    // Append anonymous filter traversal
    let anon = Traversal::<Value, Value>::new()
        .add_step(HasLabelStep::single("person"));
    
    let count = g.v()
        .append(anon)
        .count();
    
    assert_eq!(count, 3); // 3 person vertices
}

#[test]
fn test_append_chained_steps() {
    // Append anonymous traversal with multiple steps
    let anon = Traversal::<Value, Value>::new()
        .add_step(OutStep::new())
        .add_step(HasLabelStep::single("person"));
    
    let count = g.v_ids([alice])
        .append(anon)
        .count();
    
    assert_eq!(count, 2); // Alice -> bob, carol (both persons)
}

#[test]
fn test_append_preserves_prior_steps() {
    // Prior steps should be preserved
    let anon = Traversal::<Value, Value>::new()
        .add_step(ValuesStep::new("name"));
    
    let names = g.v()
        .has_label("person")
        .append(anon)
        .to_list();
    
    assert_eq!(names.len(), 3);
    assert!(names.contains(&Value::String("Alice".to_string())));
}

#[test]
fn test_append_using_factory_out() {
    // Using __ factory for anonymous traversal (when available)
    // This tests the __.out() pattern
    let count = g.v_ids([alice])
        .append(__::out())
        .count();
    
    assert_eq!(count, 3);
}

#[test]
fn test_append_using_factory_has_label() {
    // Using __ factory for has_label
    let count = g.v()
        .append(__::has_label("person"))
        .count();
    
    assert_eq!(count, 3);
}

#[test]
fn test_append_using_factory_chained() {
    // Chain multiple __ factory calls
    let anon = __::out().has_label("person");
    
    let count = g.v_ids([alice])
        .append(anon)
        .count();
    
    assert_eq!(count, 2); // bob, carol
}
```

### 7. Error Case Tests

```rust
#[test]
fn test_nonexistent_label_returns_empty() {
    let count = g.v().has_label("nonexistent").count();
    assert_eq!(count, 0);
}

#[test]
fn test_nonexistent_property_returns_empty() {
    let count = g.v().has("nonexistent").count();
    assert_eq!(count, 0);
}

#[test]
fn test_navigation_on_empty_produces_empty() {
    let count = g.v().has_label("robot").out().count();
    assert_eq!(count, 0);
}

#[test]
fn test_values_on_empty_produces_empty() {
    let results = g.v().has_label("robot").values("name").to_list();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_nonexistent_vertex_id_filtered() {
    let fake_id = VertexId(999999);
    let count = g.v_ids([fake_id]).count();
    assert_eq!(count, 0);
}

#[test]
fn test_nonexistent_edge_id_filtered() {
    let fake_id = EdgeId(999999);
    let count = g.e_ids([fake_id]).count();
    assert_eq!(count, 0);
}
```

### 8. Complex Integration Tests

```rust
#[test]
fn test_complete_traversal_chain() {
    // Complex multi-step traversal
    let results = g.v()
        .has_label("person")
        .has_value("age", 30i64)  // Alice
        .out_labels(&["knows"])    // bob, carol
        .has_label("person")
        .values("name")
        .to_list();
    
    assert_eq!(results.len(), 2);
    assert!(results.contains(&Value::String("Bob".to_string())));
    assert!(results.contains(&Value::String("Carol".to_string())));
}

#[test]
fn test_bidirectional_traversal() {
    // Find all people connected to Bob (in or out)
    let results = g.v_ids([bob])
        .both_labels(&["knows"])
        .dedup()
        .values("name")
        .to_list();
    
    // Bob knows carol (out), alice knows bob (in)
    assert!(results.len() >= 2);
}

#[test]
fn test_vertex_edge_vertex_navigation() {
    // Navigate vertex -> edge -> vertex
    let results = g.v_ids([alice])
        .out_e_labels(&["knows"])  // Get "knows" edges from Alice
        .in_v()                     // Get target vertices
        .values("name")
        .to_list();
    
    assert_eq!(results.len(), 2);
    assert!(results.contains(&Value::String("Bob".to_string())));
    assert!(results.contains(&Value::String("Carol".to_string())));
}

#[test]
fn test_multi_hop_with_filters() {
    // Friends of friends who are also connected to software
    let results = g.v()
        .has_label("person")
        .out_labels(&["knows"])
        .out_labels(&["uses"])
        .has_label("software")
        .dedup()
        .values("name")
        .to_list();
    
    // Only GraphDB should be reachable via knows->uses
    assert!(results.len() <= 1);
}

#[test]
fn test_reverse_lookup() {
    // Find who works at Acme Corp
    let results = g.v()
        .has_label("company")
        .has_value("name", "Acme Corp")
        .in_labels(&["works_at"])
        .values("name")
        .to_list();
    
    assert_eq!(results.len(), 2);
    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::String("Bob".to_string())));
}

#[test]
fn test_aggregation_on_property() {
    // Sum of ages of all people
    let result = g.v()
        .has_label("person")
        .values("age")
        .sum();
    
    // Alice (30) + Bob (35) + Carol (25) = 90
    assert_eq!(result, Value::Float(90.0));
}

#[test]
fn test_min_max_property() {
    // Find youngest and oldest person
    let youngest = g.v()
        .has_label("person")
        .values("age")
        .min();
    
    let oldest = g.v()
        .has_label("person")
        .values("age")
        .max();
    
    assert_eq!(youngest, Some(Value::Int(25))); // Carol
    assert_eq!(oldest, Some(Value::Int(35)));   // Bob
}

#[test]
fn test_property_extraction_and_filtering() {
    // Get names of people older than 28
    let results = g.v()
        .has_label("person")
        .filter(|ctx, v| {
            if let Some(id) = v.as_vertex_id() {
                if let Some(vertex) = ctx.snapshot.get_vertex(id) {
                    if let Some(Value::Int(age)) = vertex.property("age") {
                        return *age > 28;
                    }
                }
            }
            false
        })
        .values("name")
        .to_list();
    
    assert_eq!(results.len(), 2);
    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::String("Bob".to_string())));
}

#[test]
fn test_edge_property_access() {
    // Get weights of all "knows" edges
    let weights = g.e()
        .has_label("knows")
        .values("weight")
        .to_list();
    
    assert_eq!(weights.len(), 3);
    // All should be Float values
    for w in &weights {
        assert!(matches!(w, Value::Float(_)));
    }
}

#[test]
fn test_coexistence_vertex_edge_traversal() {
    // Mix vertex and edge operations in same chain
    let count = g.v()
        .has_label("person")
        .out_e_labels(&["knows"])
        .in_v()
        .has_label("person")
        .dedup()
        .count();
    
    assert!(count >= 1);
}
```

---

## Test Helper Functions

```rust
use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::value::{Value, VertexId, EdgeId};
use intersteller::error::TraversalError;
use intersteller::traversal::{Traversal, __};
use intersteller::traversal::filter::HasLabelStep;
use intersteller::traversal::navigation::OutStep;
use intersteller::traversal::transform::ValuesStep;
use std::collections::HashMap;
use std::sync::Arc;

/// Test vertex IDs for easy reference
struct TestGraph {
    graph: Graph,
    alice: VertexId,
    bob: VertexId,
    carol: VertexId,
    acme: VertexId,
}

/// Create the standard test graph
fn create_test_graph() -> TestGraph {
    let mut storage = InMemoryGraph::new();

    // Add person vertices
    let alice = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    let bob = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    let carol = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Carol".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    // Add company vertex
    let acme = storage.add_vertex("company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Acme Corp".to_string()));
        props
    });

    // Add edges with properties
    storage.add_edge(alice, bob, "knows", {
        let mut props = HashMap::new();
        props.insert("weight".to_string(), Value::Float(1.0));
        props
    }).unwrap();

    storage.add_edge(alice, carol, "knows", {
        let mut props = HashMap::new();
        props.insert("weight".to_string(), Value::Float(0.5));
        props
    }).unwrap();

    storage.add_edge(bob, carol, "knows", {
        let mut props = HashMap::new();
        props.insert("weight".to_string(), Value::Float(0.8));
        props
    }).unwrap();

    storage.add_edge(alice, acme, "works_at", {
        let mut props = HashMap::new();
        props.insert("since".to_string(), Value::Int(2020));
        props
    }).unwrap();

    storage.add_edge(bob, acme, "works_at", {
        let mut props = HashMap::new();
        props.insert("since".to_string(), Value::Int(2018));
        props
    }).unwrap();

    TestGraph {
        graph: Graph::new(Arc::new(storage)),
        alice,
        bob,
        carol,
        acme,
    }
}
```

---

## Acceptance Criteria

Phase 5.8 is complete when:

1. **All tests pass** with the standard test graph (4 vertices, 5 edges)
2. **Tests cover both success and error cases** for each step type
3. **100% branch coverage** for critical paths:
   - Source steps: `v()`, `e()`, `v_ids()`, `e_ids()`, `inject()`
   - Filter steps: `has_label`, `has`, `has_value`, `has_id`, `filter`, `dedup`, `limit`, `skip`, `range`
   - Navigation steps: `out`, `in_`, `both`, `out_e`, `in_e`, `both_e`, `out_v`, `in_v`, `both_v`
   - Transform steps: `values`, `id`, `label`, `map`, `flat_map`, `constant`, `path`
   - Terminal steps: `to_list`, `to_set`, `next`, `one`, `has_next`, `iterate`, `count`, `take`, `sum`, `min`, `max`, `fold`, `iter`, `traversers`
4. **Anonymous traversal `append()`** works correctly
5. **Edge cases handled**: empty results, non-existent IDs, type mismatches

---

## Notes

- Tests use the `InMemoryGraph` storage backend
- All tests should be independent and not rely on external state
- Test names follow the pattern `test_<category>_<specific_behavior>`
- Each test validates a single behavior where possible
- Complex integration tests validate realistic multi-step traversal patterns
