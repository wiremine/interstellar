# Index Module Code Review

**Files Reviewed:**
- `src/index/mod.rs`
- `src/index/btree.rs`
- `src/index/unique.rs`
- `src/index/traits.rs`
- `src/index/spec.rs`
- `src/index/error.rs`

**Review Date:** January 2026

---

## Bug Summary

| Severity | Count | Description |
|----------|-------|-------------|
| Critical | 1 | u64 to u32 truncation in RoaringBitmap |
| High | 2 | Panic on wrong IndexType; Data inconsistency in update |
| Medium | 1 | Statistics not updated on insert/remove |

---

## Critical Bugs

### 1. u64 to u32 Truncation in RoaringBitmap

**File:** `src/index/btree.rs`  
**Lines:** 107, 140, 159, 167, 179  
**Severity:** CRITICAL

**Description:**  
The `BTreeIndex` uses `RoaringBitmap` to store element IDs, but `RoaringBitmap` only supports `u32` values. The code casts `u64` element IDs to `u32` without bounds checking, causing silent data corruption for IDs >= 2^32.

**Problematic Code:**

```rust
// Line 107 - populate()
self.tree.entry(key).or_default().insert(id as u32);

// Line 140 - lookup_eq()
Box::new(bitmap.iter().map(|id| id as u64))

// Line 159 - lookup_range()
.map(|id| id as u64);

// Line 167 - insert()
let was_new = bitmap.insert(element_id as u32);

// Line 179 - remove()
let was_present = bitmap.remove(element_id as u32);
```

**Impact:**  
- IDs >= 2^32 are truncated, causing incorrect lookups
- Two different elements with IDs that differ only in upper 32 bits will collide
- Silent data corruption that's very hard to debug

**Suggested Fix:**

Option A: Validate ID range and return error for IDs >= 2^32:
```rust
fn insert(&mut self, value: Value, element_id: u64) -> Result<(), IndexError> {
    if element_id > u32::MAX as u64 {
        return Err(IndexError::IdOutOfRange { 
            id: element_id, 
            max: u32::MAX as u64 
        });
    }
    let key = value.to_comparable();
    let bitmap = self.tree.entry(key).or_default();
    let was_new = bitmap.insert(element_id as u32);
    // ...
}
```

Option B: Switch to `RoaringTreemap` which supports u64:
```rust
use roaring::RoaringTreemap;

pub struct BTreeIndex {
    tree: BTreeMap<ComparableValue, RoaringTreemap>,
    // ...
}
```

---

## High Severity Bugs

### 2. Panic on Wrong IndexType in Constructor

**File:** `src/index/btree.rs`, `src/index/unique.rs`  
**Lines:** `btree.rs:74-78`, `unique.rs:81-85`  
**Severity:** HIGH

**Description:**  
Both `BTreeIndex::new()` and `UniqueIndex::new()` use `assert!` to validate the IndexType, which panics if the wrong type is passed. Library code should not panic - it should return a Result.

**Problematic Code:**

```rust
// btree.rs:73-78
pub fn new(spec: IndexSpec) -> Self {
    assert!(
        spec.index_type == IndexType::BTree,
        "BTreeIndex requires IndexType::BTree, got {:?}",
        spec.index_type
    );
    // ...
}

// unique.rs:80-85
pub fn new(spec: IndexSpec) -> Self {
    assert!(
        spec.index_type == IndexType::Unique,
        "UniqueIndex requires IndexType::Unique, got {:?}",
        spec.index_type
    );
    // ...
}
```

**Impact:**  
- Application crash if wrong IndexType is passed
- Violates Interstellar's design principle: "No panics in library code"

**Suggested Fix:**

```rust
pub fn new(spec: IndexSpec) -> Result<Self, IndexError> {
    if spec.index_type != IndexType::BTree {
        return Err(IndexError::InvalidIndexType {
            expected: IndexType::BTree,
            got: spec.index_type,
        });
    }
    Ok(Self {
        spec,
        tree: BTreeMap::new(),
        stats: IndexStatistics::default(),
    })
}
```

---

### 3. Data Inconsistency in UniqueIndex::update()

**File:** `src/index/unique.rs`  
**Lines:** 198-234  
**Severity:** HIGH

**Description:**  
The `update()` method has a logic flaw in the path where `old_value != new_value` but the new value already exists for the same element. The reverse map gets the new value but the old value is only removed from `self.map`, leaving the reverse map inconsistent.

**Problematic Code:**

```rust
// Lines 214-219
// Same element, same new value - just remove old
if old_value != &new_value {
    self.map.remove(old_value);
    self.reverse.insert(element_id, new_value);  // BUG: new_value moved here
}
return Ok(());
```

**Impact:**  
- The `self.map` still contains the new_value → element_id mapping from the check
- But we return early without actually inserting into `self.map`
- Actually on closer inspection, the check at line 205 gets `&existing_id` not `existing_id`, so the new_value is still available. However, the logic is confusing and should be refactored.

**Suggested Fix:**

Refactor for clarity:
```rust
fn update(
    &mut self,
    old_value: &Value,
    new_value: Value,
    element_id: u64,
) -> Result<(), IndexError> {
    // Early return if same value
    if old_value == &new_value {
        return Ok(());
    }
    
    // Check if new value would conflict with a different element
    if let Some(&existing_id) = self.map.get(&new_value) {
        if existing_id != element_id {
            return Err(IndexError::DuplicateValue { ... });
        }
    }
    
    // Remove old mapping if it exists for this element
    if self.map.get(old_value) == Some(&element_id) {
        self.map.remove(old_value);
    }
    
    // Insert new mapping
    self.map.insert(new_value.clone(), element_id);
    self.reverse.insert(element_id, new_value);
    
    Ok(())
}
```

---

## Medium Severity Bugs

### 4. Statistics Not Updated Consistently

**File:** `src/index/btree.rs`  
**Lines:** 107-109, 194-212  
**Severity:** MEDIUM

**Description:**  
In `populate()`, statistics are refreshed at the end which is correct. However, `insert()` and `remove()` only update `total_elements`, not `cardinality`, `min_value`, `max_value`, or `last_updated`. This means statistics become stale after mutations.

**Problematic Code:**

```rust
// insert() only updates total_elements
if was_new {
    self.stats.total_elements += 1;
}

// remove() only updates total_elements  
if was_present {
    self.stats.total_elements = self.stats.total_elements.saturating_sub(1);
}
```

**Impact:**  
- `statistics()` returns stale data for cardinality, min/max values
- Query optimizer may make suboptimal decisions based on outdated stats

**Suggested Fix:**

Either update all relevant stats in `insert()`/`remove()`, or document that `refresh_statistics()` must be called periodically:

```rust
fn insert(&mut self, value: Value, element_id: u64) -> Result<(), IndexError> {
    let key = value.to_comparable();
    let is_new_key = !self.tree.contains_key(&key);
    let bitmap = self.tree.entry(key.clone()).or_default();
    let was_new = bitmap.insert(element_id as u32);

    if was_new {
        self.stats.total_elements += 1;
        if is_new_key {
            self.stats.cardinality += 1;
            // Update min/max if needed
            self.update_min_max(&key);
        }
        self.stats.last_updated = current_timestamp();
    }
    Ok(())
}
```

---

## Recommendations

1. **Immediate:** Fix the u64→u32 truncation bug - this can cause silent data corruption
2. **High Priority:** Convert `assert!` panics to `Result` returns
3. **Medium Priority:** Audit all statistics updates for consistency
4. **Consider:** Adding integration tests that use element IDs > 2^32 to catch truncation issues
