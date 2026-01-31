# WASM API Issues Spec

This document identifies issues discovered during Node.js WASM binding testing and their resolutions.

## Issue 1: `V_()` and `E_()` Require Array Argument (RESOLVED)

**Severity**: High (breaking)  
**Status**: Fixed  
**Location**: `src/wasm/types.rs:305-336`

### Description

The `V_()` method originally expected an array of vertex IDs, but users naturally want to pass a single ID:

```javascript
// Before fix - required array
graph.V_([alice]).values('name').toList();  // Works
graph.V_(alice).values('name').toList();    // Failed silently (returned [])

// After fix - both work
graph.V_(alice).values('name').toList();    // Works
graph.V_([alice, bob]).values('name').toList();  // Also works
```

### Root Cause

`js_array_to_vertex_ids()` used `js_sys::Array::from(&js)` which creates an empty array when given a non-array value (BigInt), causing the traversal to start with zero vertices.

### Resolution

Modified `js_array_to_vertex_ids()` and `js_array_to_edge_ids()` to accept either:
- A single ID (BigInt or number)
- An array of IDs

```rust
pub fn js_array_to_vertex_ids(js: JsValue) -> Result<Vec<VertexId>, JsError> {
    if js.is_undefined() || js.is_null() {
        return Ok(Vec::new());
    }

    // Handle single ID (BigInt or number) - allows graph.V_(id) syntax
    if js.dyn_ref::<js_sys::BigInt>().is_some() || js.as_f64().is_some() {
        return Ok(vec![js_to_vertex_id(js)?]);
    }

    // Handle array of IDs - allows graph.V_([id1, id2]) syntax
    if js_sys::Array::is_array(&js) {
        let array = js_sys::Array::from(&js);
        // ... iterate and collect IDs
    }

    Err(JsError::new("Expected vertex ID (bigint/number) or array of IDs"))
}
```

---

## Issue 2: `valueMap()` Returns Arrays for Property Values (NOT A BUG)

**Severity**: Low (documentation/expectation mismatch)  
**Status**: Tests updated  
**Location**: `src/traversal/transform/values.rs`

### Description

`valueMap()` wraps each property value in an array:

```javascript
graph.V_(alice).valueMap().first();
// Returns: { name: ['Alice'], age: [30n] }
```

### Root Cause

This is **correct Gremlin behavior**. In Gremlin/TinkerPop, properties can be multi-valued (a vertex can have multiple values for the same property key), so `valueMap()` returns lists.

### Resolution

Tests were updated to expect the correct Gremlin-compliant behavior:

```javascript
// valueMap() - Gremlin-compliant, returns arrays for each property
const map = graph.V_(alice).valueMap().first();
assert.deepEqual(map.name, ['Alice']);   // Array of values
assert.deepEqual(map.age, [30n]);        // Array of values

// elementMap() - convenience method, unwraps single values + includes id/label
const elemMap = graph.V_(alice).elementMap().first();
assert.equal(elemMap.name, 'Alice');     // Unwrapped value
assert.equal(elemMap.age, 30n);          // Unwrapped value
assert.equal(elemMap.id, alice);         // Includes ID
assert.equal(elemMap.label, 'person');   // Includes label
```

---

## Summary Table

| Issue | Severity | Type | Resolution |
|-------|----------|------|------------|
| `V_()` array requirement | High | API Bug | Fixed - now accepts single ID or array |
| `E_()` array requirement | High | API Bug | Fixed - now accepts single ID or array |
| `valueMap()` returns arrays | Low | Expected Behavior | Tests updated |

---

## Test Results After Fixes

All tests now pass:

| Target | Tests | Pass | Fail |
|--------|-------|------|------|
| Rust (`cargo test --lib`) | 1856 | 1856 | 0 |
| Browser (Playwright) | 11 | 11 | 0 |
| Node.js (`node --test`) | 58 | 58 | 0 |

### Test Commands

```bash
# Rust tests
cargo test --lib

# Build WASM packages
./scripts/build-wasm.sh

# Node.js tests
cd examples/wasm-node && node --test tests/*.mjs

# Browser tests  
cd examples/wasm-web && npm test
```
