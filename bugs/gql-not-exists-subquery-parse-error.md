# GQL NOT EXISTS { MATCH ... } Subquery Syntax Not Supported

> **Status: FIXED.** The parser now accepts the `EXISTS { MATCH pattern [WHERE expression] }`
> subquery form (with or without `NOT`). Inner WHERE may reference variables
> introduced inside the subquery.
>
> See `interstellar/tests/gql/expressions.rs::test_gql_exists_subquery_match_form`,
> `test_gql_exists_subquery_with_where_filters_inner_pattern`,
> `test_gql_not_exists_subquery_with_where`, and
> `test_gql_exists_subquery_where_no_matches`.

## Summary

The standard GQL/Cypher `EXISTS { MATCH ... }` subquery syntax is not recognized by the parser. Queries using `NOT EXISTS { MATCH (pattern) }` for negative pattern matching (antijoin) fail with a syntax error.

## Reproduction

```gql
MATCH (p:Person)
WHERE NOT EXISTS { MATCH (p)-[:PARENT_OF]->() }
RETURN p.displayName
```

### Actual Behavior

```
Syntax error at position 37
```

The parser does not recognize the `{ MATCH ... }` block syntax after `EXISTS`.

### Expected Behavior

Return all `Person` vertices that have no outgoing `PARENT_OF` edges (i.e., persons who are not parents / leaf nodes in the family tree).

## Impact

Without `EXISTS { ... }` subquery support, "find vertices that lack relationship X" queries cannot be expressed in GQL. This is a very common pattern in graph queries — e.g., finding childless persons, unlinked records, or orphan nodes.

## Workaround

Use a full Gremlin traversal scan with a filter:

```rust
g.V()
  .has_label("Person")
  .filter(__.out("PARENT_OF").count().is(0))
  .value_map(true)
```

This works but requires scanning all vertices rather than leveraging the GQL planner.

## Environment

- interstellar 0.1.1
- Rust nightly
- In-memory storage backend
