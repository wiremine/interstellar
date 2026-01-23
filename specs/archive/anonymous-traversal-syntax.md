# Spec: Value-Style Anonymous Traversal Syntax (`__.method()`)

**Status: IMPLEMENTED**

## Summary

Changed anonymous traversal syntax from module-style `__::out_labels()` to Gremlin-style `__.out_labels()`.

## Motivation

Gremlin uses `__.out()` (value/method style) for anonymous traversals. Our previous implementation used `__::out()` (module/path style). The new syntax is more familiar to Gremlin users and more idiomatic.

## Implementation

### Approach: Static Struct Instance

Created a zero-sized `AnonymousTraversal` struct with methods that delegate to module functions:

```rust
// src/traversal/anonymous.rs

#[derive(Debug, Clone, Copy, Default)]
pub struct AnonymousTraversal;

#[allow(non_upper_case_globals)]
pub static __: AnonymousTraversal = AnonymousTraversal;

impl AnonymousTraversal {
    #[inline]
    pub fn out(&self) -> Traversal<Value, Value> {
        out()  // delegates to module function
    }
    
    pub fn out_labels(&self, labels: &[&str]) -> Traversal<Value, Value> {
        out_labels(labels)
    }
    
    // ... 95 more methods
}
```

### Changes Made

| Area | Files Changed | Description |
|------|---------------|-------------|
| Core implementation | `src/traversal/anonymous.rs` | Added `AnonymousTraversal` struct with 97 methods |
| Re-exports | `src/traversal/mod.rs` | Export `__` static instead of module alias |
| Codebase-wide | 31 files | Replaced `__::` with `__.` (656 occurrences) |

## Usage

```rust
use interstellar::traversal::__;

// Gremlin-style syntax
let friends = __.out_labels(&["knows"]);

// Chain anonymous traversals
let complex = __.out().has_label("person").values("name");

// Use in parent traversals
let results = g.v()
    .has_label("person")
    .where_(__.out_labels(&["knows"]))
    .to_list();
```

## Verification

- All 177 tests pass
- Clippy passes with no warnings
- Marvel example runs successfully

## Notes

- The module functions still exist in `anonymous.rs` for internal use
- The `AnonymousTraversal` struct is zero-sized (no runtime cost)
- All methods are `#[inline]` for zero-cost abstraction
