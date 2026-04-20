# GQL HAVING Clause Is Silently Ignored (No-Op)

## Summary

The `HAVING` clause parses and executes without error but has no effect on query results. All rows are returned unfiltered, including those that should be excluded by the `HAVING` condition. This is a **silent correctness bug** — worse than a parse error because it produces wrong results without any indication of failure.

## Reproduction

Seed a graph with 78 `Person` vertices having a `gender` property (values: `"M"`, `"F"`, `"Unknown"`).

```gql
MATCH (p:Person)
RETURN p.gender, COUNT(*) AS c
HAVING c > 1
```

### Actual Behavior

Returns **all 78 rows** — one row per person, each with `c = 1`. The `HAVING c > 1` predicate is completely ignored. No error or warning is emitted.

Example output (truncated):
```
{"c": 1, "p.gender": "M"}
{"c": 1, "p.gender": "F"}
{"c": 1, "p.gender": "M"}
... (78 rows total)
```

### Expected Behavior

Either:
1. **Correct behavior:** Return only groups where `c > 1` (i.e., `{"c": 35, "p.gender": "M"}`, `{"c": 42, "p.gender": "F"}`, `{"c": 1, "p.gender": "Unknown"}` → only M and F rows after filtering), OR
2. **If HAVING is not yet implemented:** Return a parse or compile error so the caller knows the clause is unsupported.

## Impact

Any query relying on `HAVING` for post-aggregation filtering will silently return incorrect results. In our case, a duplicate-detection query (Demo 12) returned all 78 persons as "duplicates" instead of the actual duplicates.

## Workaround

Collect all rows and filter in application code (Rust):

```rust
let rows: Vec<_> = results.into_iter().filter(|r| count > 1).collect();
```

## Environment

- interstellar 0.1.1
- Rust nightly
- In-memory storage backend
