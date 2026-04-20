# GQL Reserved Keywords Cannot Be Used as Identifiers

## Summary

GQL reserves common English words like `desc`, `asc`, `as`, `not`, `and`, `or`, `true`,
`false`, `null` as keywords. There is no escape hatch (e.g. backtick-quoted identifiers)
to use these words as variable / property names. The parse error shows up at the
identifier position with a misleading "expected ..." message.

## Original (Misdiagnosed) Reproduction

The following query was originally reported as a "variable-length path followed by a
labeled node pattern" parse failure:

```gql
MATCH (p:Person)-[:PARENT_OF *1..4]->(desc:Person)
WHERE ID(p) = 44
RETURN DISTINCT ID(desc) AS did
```

The parse error is **not** caused by the variable-length quantifier `*1..4`. Renaming
the variable to anything other than a reserved keyword parses cleanly:

```gql
-- OK
MATCH (p:Person)-[:PARENT_OF *1..4]->(d:Person)
RETURN ID(p), ID(d)
```

The actual cause is that `desc` is the reserved `DESC` keyword used by `ORDER BY ... DESC`.
See `interstellar/src/gql/grammar.pest`:

- `DESC = @{ ^"desc" ~ !ASCII_ALPHANUMERIC }`
- `keyword = { ... ASC | DESC | ... }`
- `variable = @{ !(keyword ~ !ASCII_ALPHANUMERIC) ~ ASCII_ALPHA ~ (ASCII_ALPHANUMERIC | "_")* }`

The negative lookahead in the `variable` rule rejects any keyword as a variable name.

## Impact

Users cannot use natural identifiers like `desc` (descendants), `asc` (ancestors), `as`,
`not`, etc. as variable or property names. There is currently no workaround other than
renaming.

## Resolution

Backtick-quoted identifiers are now supported as an escape hatch. Wrap any identifier in
backticks to bypass the keyword check:

```gql
MATCH (p:Person)-[:PARENT_OF *1..4]->(`desc`:Person)
WHERE ID(p) = 44
RETURN DISTINCT ID(`desc`) AS did
```

Backticks may be used anywhere a variable name is expected (node bindings, edge bindings,
RETURN aliases, WITH aliases, ORDER BY targets, expression references). Property names
inside `{ ... }` filters, label names, and edge labels are unaffected.

## Environment

- interstellar 0.1.1
- Rust nightly
- In-memory storage backend
