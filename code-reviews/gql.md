# GQL Module Code Review

**Files Reviewed:**
- `src/gql/mod.rs`
- `src/gql/ast.rs`
- `src/gql/parser.rs`
- `src/gql/error.rs`
- `src/gql/mutation.rs`
- `src/gql/ddl.rs`
- `src/gql/compiler/mod.rs`
- `src/gql/compiler/helpers.rs`
- `src/gql/compiler/math.rs`
- `src/gql/compiler_legacy.rs`

**Review Date:** January 2026

---

## Bug Summary

| Severity | Count | Description |
|----------|-------|-------------|
| High | 2 | ReDoS vulnerability; Integer overflow in power operation |
| Medium | 4 | u64→i64 casting; HashMap hash non-determinism; Saturating add; No query limits |
| Low | 2 | Silent null on unknown function; Pattern comprehension limited |

---

## High Severity Bugs

### 1. ReDoS Vulnerability in Regex Matching

**File:** `src/gql/compiler/helpers.rs`  
**Lines:** 140-152  
**Severity:** HIGH

**Description:**  
The `apply_comparison()` function compiles user-provided regex patterns with `regex::Regex::new()` without any complexity limits. This is the same class of vulnerability as in the Rhai module.

**Problematic Code:**

```rust
// Lines 140-152
BinaryOperator::RegexMatch => match (left, right) {
    (Value::String(s), Value::String(pattern)) => {
        // Compile and match the regex pattern
        match regex::Regex::new(pattern) {
            Ok(re) => re.is_match(s),
            Err(_) => false, // Invalid regex pattern returns false
        }
    }
    // ...
},
```

**ReDoS Example (GQL Query):**

```sql
MATCH (n) WHERE n.name =~ "(a+)+$" RETURN n
```

**Impact:**  
- GQL queries can hang the database
- CPU exhaustion denial of service
- No timeout or recovery mechanism

**Suggested Fix:**

```rust
use regex::RegexBuilder;

BinaryOperator::RegexMatch => match (left, right) {
    (Value::String(s), Value::String(pattern)) => {
        // Compile with size limits to prevent ReDoS
        match RegexBuilder::new(pattern)
            .size_limit(1024 * 1024)  // 1MB DFA limit
            .dfa_size_limit(1024 * 1024)
            .build()
        {
            Ok(re) => re.is_match(s),
            Err(_) => false,
        }
    }
    // ...
},
```

---

### 2. Integer Overflow in Power Operation

**File:** `src/gql/compiler/helpers.rs`  
**Lines:** 194-206  
**Severity:** HIGH

**Description:**  
The power operation (`^`) casts the exponent to `u32` without bounds checking. Exponents larger than `u32::MAX` or large base/exponent combinations can cause panics or overflow.

**Problematic Code:**

```rust
// Lines 194-206
BinaryOperator::Pow => match (left, right) {
    // Integer to non-negative integer power
    (Value::Int(a), Value::Int(b)) if b >= 0 => Value::Int(a.pow(b as u32)),  // BUG: b cast to u32
    // Integer to negative power becomes float
    (Value::Int(a), Value::Int(b)) => Value::Float((a as f64).powi(b as i32)),  // BUG: b cast to i32
    // Float to integer power
    (Value::Float(a), Value::Int(b)) => Value::Float(a.powi(b as i32)),  // BUG: b cast to i32
    // ...
},
```

**Problematic Cases:**

1. `b >= u32::MAX as i64` - casting `b as u32` truncates silently
2. `a.pow(b as u32)` where result overflows i64 - causes panic in debug or wraps in release
3. Large exponents > i32::MAX for `powi()` - truncation

**Example Attack (GQL Query):**

```sql
RETURN 2 ^ 9999999999  -- Exponent exceeds u32::MAX
```

**Impact:**  
- Panics in debug builds
- Silent overflow/incorrect results in release builds
- Potential for DoS via expensive calculations

**Suggested Fix:**

```rust
BinaryOperator::Pow => match (left, right) {
    (Value::Int(a), Value::Int(b)) if b >= 0 => {
        // Validate exponent fits in u32
        if b > u32::MAX as i64 {
            return Value::Null;  // Or return error
        }
        // Use checked arithmetic to prevent overflow
        match a.checked_pow(b as u32) {
            Some(result) => Value::Int(result),
            None => Value::Float((a as f64).powf(b as f64)),  // Fallback to float
        }
    }
    (Value::Int(a), Value::Int(b)) => {
        // Negative exponent - use float
        Value::Float((a as f64).powi(b.clamp(i32::MIN as i64, i32::MAX as i64) as i32))
    }
    (Value::Float(a), Value::Int(b)) => {
        Value::Float(a.powi(b.clamp(i32::MIN as i64, i32::MAX as i64) as i32))
    }
    // ...
},
```

---

## Medium Severity Bugs

### 3. u64 to i64 Casting Loses High Bit for Large IDs

**File:** `src/gql/compiler_legacy.rs`  
**Lines:** ~2654-2655 (approximate from continuation prompt)  
**Severity:** MEDIUM

**Description:**  
When converting `VertexId` and `EdgeId` to Rhai-compatible values, the code casts `u64` to `i64`. IDs >= 2^63 will become negative, causing confusion or incorrect behavior.

**Pattern:**

```rust
// Converting IDs for return values
Value::Vertex(vid) => vid.0 as i64,
Value::Edge(eid) => eid.0 as i64,
```

**Impact:**  
- IDs >= 2^63 appear as negative numbers in query results
- May cause issues if results are round-tripped back as parameters
- Less severe than truncation since no data loss, just representation

**Suggested Fix:**

Document that IDs > i64::MAX are not supported, or use a different representation:

```rust
// Option A: Return as string for large IDs
fn id_to_value(id: u64) -> Value {
    if id > i64::MAX as u64 {
        Value::String(id.to_string())
    } else {
        Value::Int(id as i64)
    }
}

// Option B: Always return IDs as their wrapper types
Value::Vertex(vid) => Value::Vertex(vid),  // Keep as VertexId
```

---

### 4. HashMap Hash Non-Determinism

**File:** `src/gql/compiler/helpers.rs`  
**Lines:** 79-88  
**Severity:** MEDIUM

**Description:**  
The `ComparableValue::hash()` implementation iterates over HashMap entries, but HashMap iteration order is non-deterministic. This means identical maps may hash differently in different runs, breaking hash-based grouping.

**Problematic Code:**

```rust
// Lines 79-88
Value::Map(map) => {
    map.len().hash(state);
    // Note: HashMap order is not deterministic, but we still hash for consistency
    for (k, v) in map {
        k.hash(state);
        ComparableValue(v.clone()).hash(state);
    }
}
```

**Impact:**  
- `GROUP BY` on map values may produce inconsistent results across runs
- Two identical maps may end up in different hash buckets
- Very difficult to reproduce bugs

**Suggested Fix:**

Sort entries before hashing:

```rust
Value::Map(map) => {
    map.len().hash(state);
    // Sort keys for deterministic ordering
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    for (k, v) in entries {
        k.hash(state);
        ComparableValue(v.clone()).hash(state);
    }
}
```

---

### 5. Saturating Add Silently Loses Precision

**File:** `src/gql/compiler_legacy.rs`  
**Line:** 1194  
**Severity:** MEDIUM

**Description:**  
The `compute_sum()` function uses `saturating_add()` for integer sums, which silently caps at `i64::MAX` instead of returning an error or switching to float representation.

**Problematic Code:**

```rust
// Line 1194
int_sum = int_sum.saturating_add(*i);
```

**Example:**

```sql
-- Sum that would overflow
MATCH (n) RETURN sum(n.big_number)  -- If sum exceeds i64::MAX, result is silently capped
```

**Impact:**  
- Incorrect aggregate results for large datasets
- Silent data loss without any warning
- Users may not realize their sums are truncated

**Suggested Fix:**

Use checked arithmetic and fall back to float:

```rust
fn compute_sum(&self, values: &[Value]) -> Value {
    let mut sum = 0.0f64;
    let mut is_int = true;
    let mut int_sum: i64 = 0;
    let mut overflow = false;

    for v in values {
        match v {
            Value::Int(i) => {
                if is_int && !overflow {
                    match int_sum.checked_add(*i) {
                        Some(s) => int_sum = s,
                        None => overflow = true,  // Switch to float
                    }
                }
                sum += *i as f64;
            }
            Value::Float(f) => {
                is_int = false;
                sum += f;
            }
            _ => {}
        }
    }

    if is_int && !overflow {
        Value::Int(int_sum)
    } else {
        Value::Float(sum)
    }
}
```

---

### 6. No Query Complexity Limits

**File:** `src/gql/compiler_legacy.rs`  
**Severity:** MEDIUM

**Description:**  
The GQL compiler has no limits on query complexity, including:
- Maximum pattern length
- Maximum number of OPTIONAL MATCH clauses
- Maximum depth of nested subqueries
- Maximum number of UNION clauses

**Impact:**  
- Complex queries can exhaust memory during compilation
- Deeply nested patterns may cause stack overflow
- Potential for denial of service via expensive queries

**Suggested Fix:**

Add configurable complexity limits:

```rust
pub struct CompilerConfig {
    pub max_pattern_length: usize,      // Default: 100
    pub max_optional_matches: usize,    // Default: 50
    pub max_subquery_depth: usize,      // Default: 10
    pub max_union_clauses: usize,       // Default: 50
}

impl Compiler {
    pub fn compile_with_limits(
        &self, 
        query: &str, 
        limits: &CompilerConfig
    ) -> Result<...> {
        // Check limits during compilation
    }
}
```

---

## Low Severity Bugs

### 7. Silent Null on Unknown Function

**File:** `src/gql/compiler_legacy.rs`  
**Line:** ~6074 (approximate from continuation prompt)  
**Severity:** LOW

**Description:**  
Unknown function calls in GQL return `Value::Null` silently instead of raising an error.

**Example:**

```sql
RETURN unknownFunction(1, 2, 3)  -- Returns null instead of error
```

**Impact:**  
- Typos in function names are not caught
- Confusing debugging experience
- May lead to incorrect query results

**Suggested Fix:**

Return an error for unknown functions:

```rust
fn evaluate_function_call(&self, name: &str, args: &[Expression]) -> Result<Value, GqlError> {
    match name.to_lowercase().as_str() {
        "count" => self.eval_count(args),
        "sum" => self.eval_sum(args),
        // ... known functions ...
        _ => Err(GqlError::UnknownFunction { name: name.to_string() }),
    }
}
```

---

### 8. Pattern Comprehension Only Binds Last Node Variable

**File:** `src/gql/compiler_legacy.rs`  
**Lines:** ~3007-3035 (approximate)  
**Severity:** LOW

**Description:**  
The `bind_pattern_variables_from_match` function only binds the last node variable in a pattern, not intermediate nodes. This limits what can be accessed in pattern comprehensions.

**Example:**

```sql
-- In this pattern, only 'c' is accessible, not 'a' or 'b'
RETURN [(a)-[r]->(b)-[s]->(c) | c.name]
```

**Impact:**  
- Reduced expressiveness for pattern comprehensions
- Workaround required for accessing intermediate nodes

**Suggested Fix:**

Bind all node and relationship variables in the pattern:

```rust
fn bind_pattern_variables_from_match(&mut self, pattern: &Pattern) {
    for element in &pattern.elements {
        match element {
            PatternElement::Node(node) => {
                if let Some(var) = &node.variable {
                    self.bind_variable(var, node.id);
                }
            }
            PatternElement::Relationship(rel) => {
                if let Some(var) = &rel.variable {
                    self.bind_variable(var, rel.id);
                }
            }
        }
    }
}
```

---

## Recommendations

1. **Immediate:** Fix ReDoS vulnerability with regex size limits
2. **Immediate:** Add bounds checking to power operation
3. **High Priority:** Make HashMap hashing deterministic
4. **High Priority:** Handle integer overflow in aggregations gracefully
5. **Medium Priority:** Add query complexity limits
6. **Low Priority:** Return errors for unknown functions instead of null
7. **Testing:** Add fuzzing tests for regex patterns and arithmetic edge cases
