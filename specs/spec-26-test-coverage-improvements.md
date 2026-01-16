# Spec 26: Test Coverage Improvements

## Overview

This specification outlines tasks to improve test coverage across the Intersteller codebase. The current overall line coverage is **86.70%** with branch coverage at **54.34%**. The goal is to achieve **95%+ line coverage** and **75%+ branch coverage** across all modules.

## Current Coverage Summary

| File | Line Coverage | Branch Coverage | Priority |
|------|---------------|-----------------|----------|
| `traversal/builder.rs` | 14.29% | - | **Critical** |
| `traversal/anonymous.rs` | 36.81% | - | **Critical** |
| `gql/compiler.rs` | 64.31% | 43.88% | **High** |
| `gql/mutation.rs` | 76.67% | 52.82% | **High** |
| `traversal/mutation.rs` | 79.38% | 48.28% | **High** |
| `gql/error.rs` | 79.41% | 100% | Medium |
| `gql/parser.rs` | 82.69% | 56.29% | Medium |
| `traversal/transform/order.rs` | 82.76% | 65.00% | Medium |
| `traversal/aggregate.rs` | 83.57% | 52.27% | Medium |
| `traversal/transform/properties.rs` | 86.77% | 50.00% | Medium |
| `traversal/repeat.rs` | 89.68% | 90.62% | Low |
| `traversal/source.rs` | 89.27% | 61.11% | Low |

---

## Phase 1: Critical Coverage Gaps (builder.rs, anonymous.rs)

### 1.1 traversal/builder.rs (14.29% -> 95%)

The unbound `Traversal` builder has almost no integration test coverage since most tests use `BoundTraversal` via `snapshot.traversal()`.

#### Tests to Add

```rust
// tests/traversal/builder.rs

// === Source Steps ===
#[test]
fn unbound_traversal_v_source();
#[test]
fn unbound_traversal_e_source();
#[test]
fn unbound_traversal_v_ids_source();
#[test]
fn unbound_traversal_e_ids_source();
#[test]
fn unbound_traversal_inject_source();

// === Filter Steps ===
#[test]
fn unbound_has_label();
#[test]
fn unbound_has_label_any();
#[test]
fn unbound_has_key();
#[test]
fn unbound_has_value();
#[test]
fn unbound_has_not();
#[test]
fn unbound_has_id();
#[test]
fn unbound_dedup();
#[test]
fn unbound_limit();
#[test]
fn unbound_skip();
#[test]
fn unbound_range();
#[test]
fn unbound_filter_closure();
#[test]
fn unbound_where_traversal();
#[test]
fn unbound_where_predicate();
#[test]
fn unbound_is_predicate();
#[test]
fn unbound_and_filter();
#[test]
fn unbound_or_filter();
#[test]
fn unbound_not_filter();

// === Navigation Steps ===
#[test]
fn unbound_out();
#[test]
fn unbound_out_labels();
#[test]
fn unbound_in_();
#[test]
fn unbound_in_labels();
#[test]
fn unbound_both();
#[test]
fn unbound_both_labels();
#[test]
fn unbound_out_e();
#[test]
fn unbound_in_e();
#[test]
fn unbound_both_e();
#[test]
fn unbound_out_v();
#[test]
fn unbound_in_v();
#[test]
fn unbound_both_v();
#[test]
fn unbound_other_v();

// === Transform Steps ===
#[test]
fn unbound_values();
#[test]
fn unbound_values_multi();
#[test]
fn unbound_id();
#[test]
fn unbound_label();
#[test]
fn unbound_constant();
#[test]
fn unbound_map_closure();
#[test]
fn unbound_flat_map();
#[test]
fn unbound_unfold();
#[test]
fn unbound_fold();
#[test]
fn unbound_path();
#[test]
fn unbound_as_step();
#[test]
fn unbound_select();
#[test]
fn unbound_select_one();
#[test]
fn unbound_count_step();
#[test]
fn unbound_sum_step();
#[test]
fn unbound_min_step();
#[test]
fn unbound_max_step();
#[test]
fn unbound_mean_step();
#[test]
fn unbound_order();
#[test]
fn unbound_project();
#[test]
fn unbound_group();
#[test]
fn unbound_group_count();
#[test]
fn unbound_value_map();
#[test]
fn unbound_element_map();
#[test]
fn unbound_properties();
#[test]
fn unbound_property_map();

// === Branch Steps ===
#[test]
fn unbound_union();
#[test]
fn unbound_choose();
#[test]
fn unbound_coalesce();
#[test]
fn unbound_optional();
#[test]
fn unbound_local();

// === Repeat Step ===
#[test]
fn unbound_repeat_times();
#[test]
fn unbound_repeat_until();
#[test]
fn unbound_repeat_emit();

// === Composition ===
#[test]
fn unbound_append_traversal();
#[test]
fn unbound_chained_multiple_steps();
#[test]
fn unbound_bind_to_snapshot();
```

### 1.2 traversal/anonymous.rs (36.81% -> 95%)

The `__` module provides anonymous traversal factories. Many functions are untested.

#### Tests to Add

```rust
// tests/traversal/anonymous_extended.rs

// === Filter Steps ===
#[test]
fn anon_has_label_any();
#[test]
fn anon_has_key();
#[test]
fn anon_has_value();
#[test]
fn anon_has_not();
#[test]
fn anon_has_id();
#[test]
fn anon_skip();
#[test]
fn anon_range();
#[test]
fn anon_where_predicate();
#[test]
fn anon_where_traversal();
#[test]
fn anon_is_predicate();
#[test]
fn anon_and_filter();
#[test]
fn anon_or_filter();
#[test]
fn anon_not_filter();

// === Navigation Steps ===
#[test]
fn anon_out_labels();
#[test]
fn anon_in_();
#[test]
fn anon_in_labels();
#[test]
fn anon_both();
#[test]
fn anon_both_labels();
#[test]
fn anon_out_e();
#[test]
fn anon_out_e_labels();
#[test]
fn anon_in_e();
#[test]
fn anon_in_e_labels();
#[test]
fn anon_both_e();
#[test]
fn anon_both_e_labels();
#[test]
fn anon_out_v();
#[test]
fn anon_in_v();
#[test]
fn anon_both_v();
#[test]
fn anon_other_v();

// === Transform Steps ===
#[test]
fn anon_values_multi();
#[test]
fn anon_id();
#[test]
fn anon_label();
#[test]
fn anon_flat_map();
#[test]
fn anon_unfold();
#[test]
fn anon_fold();
#[test]
fn anon_path();
#[test]
fn anon_as_step();
#[test]
fn anon_select();
#[test]
fn anon_select_one();
#[test]
fn anon_count_step();
#[test]
fn anon_sum_step();
#[test]
fn anon_min_step();
#[test]
fn anon_max_step();
#[test]
fn anon_mean_step();
#[test]
fn anon_order();
#[test]
fn anon_project();
#[test]
fn anon_group();
#[test]
fn anon_group_count();
#[test]
fn anon_value_map();
#[test]
fn anon_element_map();
#[test]
fn anon_properties();
#[test]
fn anon_property_map();
#[test]
fn anon_math();

// === Branch Steps ===
#[test]
fn anon_union();
#[test]
fn anon_choose();
#[test]
fn anon_coalesce();
#[test]
fn anon_optional();
#[test]
fn anon_local();

// === Repeat Step ===
#[test]
fn anon_repeat();
#[test]
fn anon_loops();

// === Side Effect Steps ===
#[test]
fn anon_store();
#[test]
fn anon_aggregate();
#[test]
fn anon_inject();
```

---

## Phase 2: High Priority (GQL compiler, mutations)

### 2.1 gql/compiler.rs (64.31% -> 90%)

The GQL compiler has significant untested code paths, particularly around:
- Complex MATCH patterns
- Variable-length path patterns
- Edge cases in expression evaluation
- Error handling paths

#### Tests to Add

```rust
// tests/gql/compiler_extended.rs

// === Pattern Matching ===
#[test]
fn test_variable_length_path_star();
#[test]
fn test_variable_length_path_range();
#[test]
fn test_variable_length_path_minimum();
#[test]
fn test_variable_length_path_with_filter();
#[test]
fn test_bidirectional_edge_pattern();
#[test]
fn test_multiple_edge_labels();
#[test]
fn test_inline_where_on_node();
#[test]
fn test_inline_where_on_edge();
#[test]
fn test_pattern_with_properties();

// === Expressions ===
#[test]
fn test_nested_property_access();
#[test]
fn test_list_literal_in_expression();
#[test]
fn test_map_literal_in_expression();
#[test]
fn test_string_concatenation();
#[test]
fn test_null_handling_in_expressions();
#[test]
fn test_type_coercion_in_comparisons();

// === Aggregations ===
#[test]
fn test_collect_with_distinct();
#[test]
fn test_nested_aggregation();
#[test]
fn test_aggregation_with_null_values();
#[test]
fn test_group_by_with_aggregation();
#[test]
fn test_having_clause();

// === CALL Subqueries ===
#[test]
fn test_call_correlated_with_aggregation();
#[test]
fn test_call_nested_subqueries();
#[test]
fn test_call_with_multiple_importing();

// === List/Pattern Comprehensions ===
#[test]
fn test_list_comprehension_basic();
#[test]
fn test_list_comprehension_with_filter();
#[test]
fn test_list_comprehension_nested();
#[test]
fn test_pattern_comprehension_basic();
#[test]
fn test_pattern_comprehension_with_filter();

// === Error Cases ===
#[test]
fn test_undefined_variable_in_return();
#[test]
fn test_undefined_variable_in_where();
#[test]
fn test_type_error_in_aggregation();
#[test]
fn test_invalid_property_access();
```

### 2.2 gql/mutation.rs (76.67% -> 95%)

#### Tests to Add

```rust
// tests/gql/mutations_extended.rs

// === CREATE ===
#[test]
fn test_create_vertex_with_null_property();
#[test]
fn test_create_edge_with_properties();
#[test]
fn test_create_multiple_edges_in_pattern();
#[test]
fn test_create_with_expression_properties();

// === MERGE ===
#[test]
fn test_merge_with_on_create();
#[test]
fn test_merge_with_on_match();
#[test]
fn test_merge_edge_creates_when_not_exists();
#[test]
fn test_merge_edge_matches_when_exists();

// === SET ===
#[test]
fn test_set_multiple_properties();
#[test]
fn test_set_property_to_null();
#[test]
fn test_set_with_expression();
#[test]
fn test_set_labels();
#[test]
fn test_set_properties_from_map();

// === REMOVE ===
#[test]
fn test_remove_multiple_properties();
#[test]
fn test_remove_nonexistent_property();
#[test]
fn test_remove_labels();

// === DELETE ===
#[test]
fn test_delete_edge();
#[test]
fn test_delete_multiple_elements();
#[test]
fn test_delete_vertex_cascade_error();

// === FOREACH ===
#[test]
fn test_foreach_with_create();
#[test]
fn test_foreach_with_delete();
#[test]
fn test_foreach_with_merge();
#[test]
fn test_foreach_deeply_nested();
#[test]
fn test_foreach_with_expression_list();

// === Error Handling ===
#[test]
fn test_mutation_invalid_vertex_reference();
#[test]
fn test_mutation_constraint_violation();
```

### 2.3 traversal/mutation.rs (79.38% -> 95%)

#### Tests to Add

```rust
// tests/traversal/mutations_extended.rs

// === AddVertex ===
#[test]
fn test_add_v_with_multiple_properties();
#[test]
fn test_add_v_chained_property_calls();
#[test]
fn test_add_v_with_null_property();

// === AddEdge ===
#[test]
fn test_add_e_from_to_vertices();
#[test]
fn test_add_e_with_properties();
#[test]
fn test_add_e_from_labeled_path();
#[test]
fn test_add_e_to_labeled_path();
#[test]
fn test_add_e_self_loop();

// === Property Mutations ===
#[test]
fn test_property_set_on_vertex();
#[test]
fn test_property_set_on_edge();
#[test]
fn test_property_set_overwrites();
#[test]
fn test_property_set_with_closure();

// === Drop ===
#[test]
fn test_drop_vertices();
#[test]
fn test_drop_edges();
#[test]
fn test_drop_vertex_with_incident_edges();
#[test]
fn test_drop_empty_stream();
```

---

## Phase 3: Medium Priority

### 3.1 gql/parser.rs (82.69% -> 95%)

Focus on edge cases and error recovery paths.

#### Tests to Add

```rust
// tests/gql/parser_extended.rs

// === Error Cases ===
#[test]
fn test_parse_error_unclosed_parenthesis();
#[test]
fn test_parse_error_unclosed_bracket();
#[test]
fn test_parse_error_unclosed_brace();
#[test]
fn test_parse_error_invalid_operator();
#[test]
fn test_parse_error_unexpected_keyword();
#[test]
fn test_parse_error_invalid_number();
#[test]
fn test_parse_error_unterminated_string();

// === Edge Cases ===
#[test]
fn test_parse_empty_list_literal();
#[test]
fn test_parse_empty_map_literal();
#[test]
fn test_parse_nested_function_calls();
#[test]
fn test_parse_deeply_nested_expression();
#[test]
fn test_parse_unicode_identifiers();
#[test]
fn test_parse_escaped_strings();
#[test]
fn test_parse_multiline_query();
```

### 3.2 traversal/transform/order.rs (82.76% -> 95%)

#### Tests to Add

```rust
// tests/traversal/order_extended.rs

#[test]
fn test_order_by_multiple_keys();
#[test]
fn test_order_with_nulls_first();
#[test]
fn test_order_with_nulls_last();
#[test]
fn test_order_by_traversal();
#[test]
fn test_order_shuffle();
#[test]
fn test_order_preserves_metadata();
#[test]
fn test_order_empty_input();
#[test]
fn test_order_single_element();
#[test]
fn test_order_stable_sort();
```

### 3.3 traversal/aggregate.rs (83.57% -> 95%)

#### Tests to Add

```rust
// tests/traversal/aggregate_extended.rs

// === Group ===
#[test]
fn test_group_by_multiple_keys();
#[test]
fn test_group_with_sum_value();
#[test]
fn test_group_with_mean_value();
#[test]
fn test_group_with_min_value();
#[test]
fn test_group_with_max_value();
#[test]
fn test_group_nested_traversals();

// === GroupCount ===
#[test]
fn test_group_count_with_traversal_key();
#[test]
fn test_group_count_null_keys();

// === Edge Cases ===
#[test]
fn test_aggregate_empty_groups();
#[test]
fn test_aggregate_single_item_groups();
#[test]
fn test_aggregate_large_dataset();
```

### 3.4 traversal/transform/properties.rs (86.77% -> 95%)

#### Tests to Add

```rust
// tests/traversal/properties_extended.rs

#[test]
fn test_value_map_with_tokens();
#[test]
fn test_value_map_empty_properties();
#[test]
fn test_element_map_vertices();
#[test]
fn test_element_map_edges();
#[test]
fn test_properties_with_keys();
#[test]
fn test_properties_no_keys();
#[test]
fn test_property_map_full();
#[test]
fn test_property_map_selected_keys();
```

---

## Phase 4: Branch Coverage Focus

Files with low branch coverage need targeted tests for conditional paths.

### 4.1 Branch Coverage Targets

| File | Current | Target |
|------|---------|--------|
| `gql/compiler.rs` | 43.88% | 70% |
| `traversal/mutation.rs` | 48.28% | 70% |
| `traversal/transform/metadata.rs` | 50.00% | 75% |
| `traversal/transform/properties.rs` | 50.00% | 75% |
| `traversal/aggregate.rs` | 52.27% | 70% |
| `gql/mutation.rs` | 52.82% | 70% |
| `gql/parser.rs` | 56.29% | 70% |

### 4.2 Branch Coverage Test Patterns

For each conditional, ensure both true and false branches are tested:

```rust
// Pattern: Test both branches of Option handling
#[test]
fn test_some_case() { ... }
#[test]
fn test_none_case() { ... }

// Pattern: Test all match arms
#[test]
fn test_variant_a() { ... }
#[test]
fn test_variant_b() { ... }
#[test]
fn test_variant_default() { ... }

// Pattern: Test error vs success paths
#[test]
fn test_success_path() { ... }
#[test]
fn test_error_path() { ... }
```

---

## Implementation Order

1. **Week 1**: Phase 1 - builder.rs and anonymous.rs (Critical)
2. **Week 2**: Phase 2.1 - gql/compiler.rs 
3. **Week 3**: Phase 2.2-2.3 - mutation modules
4. **Week 4**: Phase 3 - Medium priority files
5. **Week 5**: Phase 4 - Branch coverage focus

## Success Criteria

- [ ] Overall line coverage >= 95%
- [ ] Overall branch coverage >= 75%
- [ ] No file below 90% line coverage
- [ ] No file below 60% branch coverage
- [ ] All new tests pass
- [ ] No regression in existing tests

## Verification

Run coverage after each phase:

```bash
cargo +nightly llvm-cov --branch --html --open
```

Check specific file:

```bash
cargo +nightly llvm-cov --branch 2>&1 | grep "filename.rs"
```
