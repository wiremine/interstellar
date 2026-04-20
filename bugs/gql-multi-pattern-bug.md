# GQL Multi-Pattern Comma Syntax: Undefined Variable in Second Pattern

> **Status: FIXED** — Comma-separated patterns in `MATCH` now work. The compiler
> pre-registers variables from all patterns, anchors subsequent patterns on shared
> variables (or performs Cartesian expansion when fully disjoint), and applies
> equality constraints when a shared variable appears at a non-anchor position.
> Coverage: see `interstellar/tests/gql/patterns.rs::test_gql_multi_pattern_*`.

## Summary

When using comma-separated patterns in a single `MATCH` clause, variables bound in the second pattern are not recognized by the compiler. The query fails with `Undefined variable '<var>'. Did you forget to bind it in MATCH?`

## Reproduction

```gql
MATCH (parent:Person)-[:PARENT_OF]->(p:Person),
      (parent)-[:PARENT_OF]->(sibling:Person)-[:HAS_NAME]->(n:Name)
WHERE ID(p) = 41 AND ID(sibling) <> 41 AND n.sortOrder = 0
RETURN DISTINCT n.given, n.surname
```

**Error:**
```
Compile error: Undefined variable 'n'. Did you forget to bind it in MATCH?
```

The variable `n` is bound in the second comma-separated pattern `(parent)-[:PARENT_OF]->(sibling:Person)-[:HAS_NAME]->(n:Name)`, but the compiler doesn't see it.

## Additional Examples

This also affects other multi-pattern queries. For example, a duplicate detection query:

```gql
MATCH (p1:Person)-[:HAS_NAME]->(n1:Name),
      (p2:Person)-[:HAS_NAME]->(n2:Name)
WHERE n1.given = n2.given
  AND n1.surname = n2.surname
  AND ID(p1) < ID(p2)
  AND n1.sortOrder = 0
  AND n2.sortOrder = 0
RETURN n1.given, n1.surname, ID(p1) AS id1, ID(p2) AS id2
```

**Error:**
```
Compile error: Undefined variable 'p2'. Did you forget to bind it in MATCH?
```

Here `p2`, `n2`, and any variables introduced in the second pattern are unresolvable.

## Expected Behavior

Variables bound in any comma-separated pattern within a `MATCH` clause should be visible to `WHERE` and `RETURN`.

## Workaround

Use the Gremlin traversal API instead for queries that require multiple patterns.

## Additional Reproduction: Same Bound Variable in Second Pattern

The bug also fires when the second pattern references a variable that was already bound in the first pattern (not just newly introduced variables):

```gql
MATCH (m:Marriage)-[:HAS_SPOUSE]->(p:Person),
      (m)-[:HAS_SPOUSE]->(s:Person)
WHERE ID(p) <> ID(s)
RETURN ID(p) AS pid, ID(s) AS sid
```

**Error:**
```
Compile error: Undefined variable 's'. Did you forget to bind it in MATCH?
```

Here `m` is bound in the first pattern and reused in the second — this is the standard way to express "find two edges from the same node" in GQL. The variable `s` (newly bound in the second pattern) is still unresolvable.

## Environment

- interstellar 0.1.1
- Rust nightly
- In-memory storage backend
