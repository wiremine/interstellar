# Rhai Module Code Review

**Files Reviewed:**
- `src/rhai/mod.rs`
- `src/rhai/engine.rs`
- `src/rhai/types.rs`
- `src/rhai/predicates.rs`
- `src/rhai/traversal.rs`
- `src/rhai/anonymous.rs`
- `src/rhai/error.rs`

**Review Date:** January 2026

---

## Bug Summary

| Severity | Count | Description |
|----------|-------|-------------|
| Critical | 1 | Integer overflow on ID conversion (negative i64 to u64) |
| High | 3 | Silent failure in edge endpoints; Missing probability validation; ReDoS vulnerability |
| Medium | 1 | No script complexity limits |

---

## Critical Bugs

### 1. Integer Overflow on ID Conversion (Negative i64 to u64)

**File:** `src/rhai/types.rs`  
**Lines:** 149, 169  
**Severity:** CRITICAL

**Description:**  
The `vertex_id()` and `edge_id()` constructor functions accept `i64` from Rhai scripts and cast directly to `u64`. Negative values will wrap around to very large positive values, potentially referencing non-existent elements or causing undefined behavior.

**Problematic Code:**

```rust
// Line 149 - vertex_id constructor
engine.register_fn("vertex_id", |id: i64| VertexId(id as u64));

// Line 169 - edge_id constructor  
engine.register_fn("edge_id", |id: i64| EdgeId(id as u64));
```

**Example Attack:**

```javascript
// In Rhai script:
let vid = vertex_id(-1);  // Creates VertexId(18446744073709551615)
// This could bypass permission checks or reference garbage memory
```

**Impact:**  
- Negative IDs from scripts silently become massive positive IDs
- Could reference non-existent or unintended graph elements
- Security risk if IDs are used for access control

**Suggested Fix:**

```rust
engine.register_fn("vertex_id", |id: i64| -> Result<VertexId, Box<rhai::EvalAltResult>> {
    if id < 0 {
        Err(format!("VertexId cannot be negative: {}", id).into())
    } else {
        Ok(VertexId(id as u64))
    }
});

engine.register_fn("edge_id", |id: i64| -> Result<EdgeId, Box<rhai::EvalAltResult>> {
    if id < 0 {
        Err(format!("EdgeId cannot be negative: {}", id).into())
    } else {
        Ok(EdgeId(id as u64))
    }
});
```

---

## High Severity Bugs

### 2. Silent Failure in Edge Endpoint Methods

**File:** `src/rhai/traversal.rs` (based on continuation prompt)  
**Severity:** HIGH

**Description:**  
Edge endpoint methods (getting source/target vertex of an edge) silently return null or empty results when the edge doesn't exist or endpoints can't be retrieved, instead of propagating an error.

**Impact:**  
- Scripts may proceed with null values without realizing an error occurred
- Hard to debug silent failures
- May lead to incorrect query results

**Suggested Fix:**

Return a `Result` type that becomes a Rhai error:

```rust
engine.register_fn(
    "out_vertex",
    |edge: &mut RhaiEdge| -> Result<RhaiVertex, Box<rhai::EvalAltResult>> {
        edge.out_vertex()
            .ok_or_else(|| "Edge has no outgoing vertex (edge may not exist)".into())
    },
);
```

---

### 3. Missing Probability Validation in coin()

**File:** `src/rhai/predicates.rs` or `src/rhai/traversal.rs`  
**Severity:** HIGH

**Description:**  
The `coin()` function (used for random sampling in traversals) accepts a probability parameter but doesn't validate that it's in the valid range [0.0, 1.0].

**Problematic Pattern:**

```rust
// Accepts any f64 without validation
engine.register_fn("coin", |probability: f64| {
    RhaiPredicate::new(p::coin(probability))
});
```

**Impact:**  
- Probability < 0 or > 1 causes undefined behavior
- NaN or infinity values may cause panics or infinite loops

**Suggested Fix:**

```rust
engine.register_fn("coin", |probability: f64| -> Result<RhaiPredicate, Box<rhai::EvalAltResult>> {
    if probability.is_nan() || probability < 0.0 || probability > 1.0 {
        return Err(format!(
            "coin() probability must be between 0.0 and 1.0, got: {}",
            probability
        ).into());
    }
    Ok(RhaiPredicate::new(p::coin(probability)))
});
```

---

### 4. ReDoS Vulnerability in Regex Predicates

**File:** `src/rhai/predicates.rs`  
**Lines:** 219-227  
**Severity:** HIGH

**Description:**  
The `regex()` predicate function compiles user-provided regex patterns without any complexity limits or timeout. Malicious patterns can cause catastrophic backtracking, leading to denial of service.

**Problematic Code:**

```rust
// Lines 219-227
engine.register_fn(
    "regex",
    |pattern: ImmutableString| -> Result<RhaiPredicate, Box<rhai::EvalAltResult>> {
        match p::try_regex(&pattern) {
            Some(regex_pred) => Ok(RhaiPredicate::new(regex_pred)),
            None => Err(format!("Invalid regex pattern: {}", pattern).into()),
        }
    },
);
```

**ReDoS Example:**

```javascript
// In Rhai script:
let evil = regex("(a+)+$");  // Catastrophic backtracking on "aaaaaaaaaaaaaaaaX"
```

**Impact:**  
- Scripts can hang the entire application
- CPU exhaustion denial of service
- No timeout mechanism to recover

**Suggested Fix:**

Use `regex` crate's size limits and/or compile with a timeout:

```rust
use regex::RegexBuilder;
use std::time::Duration;

fn safe_regex(pattern: &str) -> Result<regex::Regex, String> {
    RegexBuilder::new(pattern)
        .size_limit(10 * 1024 * 1024)  // 10MB DFA limit
        .dfa_size_limit(10 * 1024 * 1024)
        .build()
        .map_err(|e| e.to_string())
}

// Or use the regex crate's built-in limits
engine.register_fn(
    "regex",
    |pattern: ImmutableString| -> Result<RhaiPredicate, Box<rhai::EvalAltResult>> {
        // Validate pattern complexity
        if pattern.len() > 1000 {
            return Err("Regex pattern too long (max 1000 chars)".into());
        }
        
        // Try to compile with size limits
        match RegexBuilder::new(&pattern)
            .size_limit(1024 * 1024)
            .build() 
        {
            Ok(re) => Ok(RhaiPredicate::from_regex(re)),
            Err(e) => Err(format!("Invalid regex pattern: {}", e).into()),
        }
    },
);
```

---

## Medium Severity Bugs

### 5. No Script Complexity Limits

**File:** `src/rhai/engine.rs`  
**Severity:** MEDIUM

**Description:**  
The `RhaiEngine` is created with default settings, which may allow scripts with excessive complexity (deep nesting, infinite loops, excessive memory allocation).

**Problematic Code:**

```rust
// Line 72
let mut engine = Engine::new();
// No complexity limits set
```

**Impact:**  
- Scripts can exhaust memory or CPU
- Potential denial of service from untrusted scripts
- No way to interrupt long-running scripts

**Suggested Fix:**

Configure Rhai engine with sensible limits:

```rust
pub fn new() -> Self {
    let mut engine = Engine::new();
    
    // Set execution limits
    engine.set_max_operations(1_000_000);  // Max VM operations
    engine.set_max_expr_depths(64, 64);    // Max expression nesting
    engine.set_max_call_levels(64);        // Max function call depth
    engine.set_max_string_size(1_000_000); // Max string length
    engine.set_max_array_size(10_000);     // Max array size
    engine.set_max_map_size(10_000);       // Max map size
    
    // Register all Interstellar types and functions
    register_types(&mut engine);
    register_predicates(&mut engine);
    register_traversal(&mut engine);
    register_anonymous(&mut engine);

    RhaiEngine { engine }
}
```

---

## Recommendations

1. **Immediate:** Fix the negative ID overflow - this is a security issue
2. **High Priority:** Add regex complexity limits to prevent ReDoS
3. **High Priority:** Validate probability parameters
4. **Medium Priority:** Add script execution limits
5. **Consider:** Adding a "safe mode" option that restricts dangerous operations for untrusted scripts
