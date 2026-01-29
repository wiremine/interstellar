# emit_first() Gremlin Compatibility Issue

## Summary

The current `emit_first()` API deviates from standard Gremlin semantics for controlling whether the starting vertex is emitted.

## Current Interstellar API

```rust
// Emit starting vertex + all intermediates
g.v().has_value("name", "Alice")
    .repeat(__.out())
    .times(2)
    .emit()
    .emit_first()
    .to_list()

// Emit only intermediates (not starting vertex)
g.v().has_value("name", "Alice")
    .repeat(__.out())
    .times(2)
    .emit()
    .to_list()
```

## Standard Gremlin API

In Gremlin, the **position** of `emit()` relative to `repeat()` controls this behavior:

```groovy
// Emit starting vertex first, then intermediates (emit BEFORE repeat)
g.V().has("name", "Alice")
    .emit()
    .repeat(__.out())
    .times(2)
    .toList()

// Emit only intermediates, not starting vertex (emit AFTER repeat)
g.V().has("name", "Alice")
    .repeat(__.out())
    .times(2)
    .emit()
    .toList()
```

## Issue

`emit_first()` is an Interstellar-specific method that doesn't exist in standard Gremlin. Users familiar with Gremlin would expect to use emit positioning instead.

## Recommendation

Consider supporting Gremlin-style emit positioning:

1. Add an `emit()` method on `BoundTraversal` that can be called **before** `repeat()`
2. When `emit()` precedes `repeat()`, set `emit_first = true` internally
3. Deprecate or keep `emit_first()` as an alias for explicit configuration

This would allow both syntaxes:
```rust
// Gremlin-compatible syntax
g.v().emit().repeat(__.out()).times(2).to_list()

// Explicit Interstellar syntax (could keep for clarity)
g.v().repeat(__.out()).times(2).emit().emit_first().to_list()
```

## Affected Code

- `src/traversal/repeat.rs` - `RepeatConfig` and `RepeatTraversal`
- `src/traversal/source.rs` - Would need `emit()` step before `repeat()`
- Tests in `tests/traversal/patterns/recursive.rs` use current API

## Priority

Medium - API works correctly but differs from Gremlin convention.
