# Schema Module Code Review

**Files Reviewed:**
- `src/schema/mod.rs`
- `src/schema/types.rs`
- `src/schema/builder.rs`
- `src/schema/validation.rs`
- `src/schema/serialize.rs`
- `src/schema/error.rs`

**Review Date:** January 2026

---

## Bug Summary

| Severity | Count | Description |
|----------|-------|-------------|
| Medium | 2 | Empty list semantics; Default value type not validated |
| Low | 1 | u16 overflow risk in serialization |

---

## Medium Severity Bugs

### 1. EdgeSchema allows_from/to Returns False for Empty Lists

**File:** `src/schema/types.rs`  
**Lines:** 143-151  
**Severity:** MEDIUM

**Description:**  
The `allows_from()` and `allows_to()` methods return `false` for any label when the corresponding list is empty. However, per the design documented in `src/schema/mod.rs` lines 248-257 and 263-271, an empty list should mean "any label is allowed" (wildcard behavior).

**Problematic Code:**

```rust
// Line 143-145
pub fn allows_from(&self, label: &str) -> bool {
    self.from_labels.iter().any(|l| l == label)
}

// Line 150-152
pub fn allows_to(&self, label: &str) -> bool {
    self.to_labels.iter().any(|l| l == label)
}
```

**Contrast with GraphSchema (mod.rs lines 248-257):**

```rust
pub fn edges_from(&self, vertex_label: &str) -> Vec<&str> {
    self.edge_schemas
        .iter()
        .filter(|(_, schema)| {
            schema.from_labels.is_empty()  // Empty = allow all
                || schema.from_labels.iter().any(|l| l == vertex_label)
        })
        // ...
}
```

**Impact:**  
- Inconsistent behavior between `GraphSchema::edges_from()` and `EdgeSchema::allows_from()`
- Edge creation validation may incorrectly reject valid edges when no constraints are specified

**Suggested Fix:**

```rust
/// Check if a source label is allowed.
///
/// Returns `true` if the given label is in the list of allowed source labels,
/// or if the list is empty (meaning any source is allowed).
pub fn allows_from(&self, label: &str) -> bool {
    self.from_labels.is_empty() || self.from_labels.iter().any(|l| l == label)
}

/// Check if a target label is allowed.
///
/// Returns `true` if the given label is in the list of allowed target labels,
/// or if the list is empty (meaning any target is allowed).
pub fn allows_to(&self, label: &str) -> bool {
    self.to_labels.is_empty() || self.to_labels.iter().any(|l| l == label)
}
```

---

### 2. Default Value Type Not Validated in Builder

**File:** `src/schema/builder.rs`  
**Severity:** MEDIUM

**Description:**  
The `optional_with_default()` method in schema builders does not validate that the default value matches the declared property type. This allows construction of invalid schemas.

**Problematic Pattern:**

```rust
// This should fail but doesn't:
SchemaBuilder::new()
    .vertex("Person")
        .optional_with_default("age", PropertyType::Int, Value::String("not an int"))
        .done()
    .build()
```

**Impact:**  
- Invalid schemas can be created and stored
- Runtime errors when defaults are applied with mismatched types
- Confusing behavior for users

**Suggested Fix:**

Add validation in the builder:

```rust
pub fn optional_with_default(
    mut self,
    name: &str,
    prop_type: PropertyType,
    default: Value,
) -> Self {
    // Validate that default matches the declared type
    if !prop_type.matches(&default) {
        // Either panic (for programming errors) or store validation error
        panic!(
            "Default value {:?} does not match declared type {:?} for property '{}'",
            default, prop_type, name
        );
    }
    
    self.properties.insert(
        name.to_string(),
        PropertyDef {
            key: name.to_string(),
            value_type: prop_type,
            required: false,
            default: Some(default),
        },
    );
    self
}
```

Or use a fallible builder pattern:

```rust
pub fn optional_with_default(
    mut self,
    name: &str,
    prop_type: PropertyType,
    default: Value,
) -> Result<Self, SchemaError> {
    if !prop_type.matches(&default) {
        return Err(SchemaError::DefaultTypeMismatch {
            property: name.to_string(),
            expected_type: prop_type,
            actual_value: default,
        });
    }
    // ...
}
```

---

## Low Severity Bugs

### 3. Potential u16 Overflow in Serialization

**File:** `src/schema/serialize.rs`  
**Severity:** LOW

**Description:**  
The serialization format uses `u16` for counts of vertex schemas, edge schemas, properties, and from/to labels. While unlikely to be exceeded in practice, extremely large schemas could overflow.

**Impact:**  
- Schemas with > 65,535 vertex types, edge types, properties per type, or endpoint labels will fail or corrupt
- Very unlikely in practice but violates robustness

**Suggested Fix:**

Document the limitation clearly, or use variable-length encoding:

```rust
// Document limitation
const MAX_VERTEX_SCHEMAS: usize = u16::MAX as usize;
const MAX_EDGE_SCHEMAS: usize = u16::MAX as usize;
const MAX_PROPERTIES_PER_TYPE: usize = u16::MAX as usize;

// Or use variable-length encoding for future-proofing
fn write_varint(writer: &mut impl Write, value: usize) -> io::Result<()> {
    // LEB128 or similar encoding
}
```

---

## Recommendations

1. **High Priority:** Fix the `allows_from()`/`allows_to()` empty list semantics to match `GraphSchema::edges_from()`/`edges_to()`
2. **Medium Priority:** Add type validation for default values in builders
3. **Low Priority:** Document or address serialization limits
4. **Testing:** Add test cases for edge schemas with empty from/to lists to ensure wildcard behavior works correctly
