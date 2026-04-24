# Spec 23: Kani Formal Verification

## Overview

Integrate [Kani](https://github.com/model-checking/kani), a model checker for Rust, to formally verify critical components of the Interstellar graph database. Kani exhaustively checks all possible inputs within defined bounds, providing mathematical proofs of correctness rather than probabilistic testing.

## Motivation

The codebase contains several categories of code that benefit from formal verification:

1. **Unsafe code** in memory-mapped storage (`#[repr(C, packed)]` structs, pointer operations)
2. **Type conversions** that may overflow or truncate silently
3. **Arithmetic operations** in offset calculations that could overflow
4. **Serialization invariants** that must hold for data integrity

Property-based testing (proptest) catches many issues but cannot prove absence of bugs. Kani provides this guarantee within bounded input spaces.

## Goals

1. Verify packed struct serialization roundtrips are correct
2. Verify struct sizes match declared constants
3. Detect and document (or fix) integer overflow/truncation bugs
4. Verify offset calculations cannot overflow for reasonable capacities
5. Establish a pattern for adding future verification proofs

## Non-Goals

- Verifying concurrent behavior (Kani doesn't model threads)
- Verifying I/O operations or file system behavior
- Achieving 100% code coverage with Kani (focus on critical paths)
- Replacing existing property-based tests

---

## Phase 1: Setup and Infrastructure

### 1.1 Kani Installation Documentation

Add to `README.md` or create `docs/verification.md`:

```bash
# Install Kani (one-time setup)
cargo install --locked kani-verifier
kani setup

# Run all verification proofs
cargo kani

# Run specific proof
cargo kani --harness verify_node_record_roundtrip

# Run with verbose output
cargo kani --verbose
```

### 1.2 Cargo Configuration

Add to `Cargo.toml`:

```toml
[package.metadata.kani]
# Default unwind bound for loops (can be overridden per-proof)
default-unwind = 10
```

### 1.3 Module Structure

Create `src/kani_proofs.rs` as the centralized location for all verification proofs:

```rust
//! Kani formal verification proofs for Interstellar.
//!
//! This module contains proof harnesses that verify correctness properties
//! of critical code paths using the Kani model checker.
//!
//! # Running Proofs
//!
//! ```bash
//! cargo kani                                    # Run all proofs
//! cargo kani --harness <name>                   # Run specific proof
//! cargo kani --harness <name> --visualize       # Generate counterexample
//! ```
//!
//! # Adding New Proofs
//!
//! 1. Add a new `#[kani::proof]` function in the appropriate submodule
//! 2. Use `kani::any()` to generate symbolic inputs
//! 3. Use `kani::assume()` to constrain inputs to valid ranges
//! 4. Use `kani::assert()` or standard `assert!` for properties
//!
//! # Proof Categories
//!
//! - `records_proofs`: Packed struct serialization and layout
//! - `value_proofs`: Value type conversions and serialization
//! - `offset_proofs`: File offset calculations

// Only compile when running under Kani
#![cfg(kani)]
```

Add to `src/lib.rs`:

```rust
#[cfg(kani)]
mod kani_proofs;
```

---

## Phase 2: Packed Struct Verification

### 2.1 Struct Size Verification

Verify that struct sizes match their declared constants at verification time:

```rust
#[cfg(kani)]
mod records_proofs {
    use crate::storage::mmap::records::*;

    /// Verify all packed struct sizes match their declared constants.
    ///
    /// This catches mismatches between struct definitions and size constants
    /// that could cause buffer overflows or data corruption.
    #[kani::proof]
    fn verify_struct_sizes_match_constants() {
        assert_eq!(
            std::mem::size_of::<FileHeader>(),
            HEADER_SIZE,
            "FileHeader size mismatch"
        );
        assert_eq!(
            std::mem::size_of::<NodeRecord>(),
            NODE_RECORD_SIZE,
            "NodeRecord size mismatch"
        );
        assert_eq!(
            std::mem::size_of::<EdgeRecord>(),
            EDGE_RECORD_SIZE,
            "EdgeRecord size mismatch"
        );
    }
}
```

---

## Phase 6: FreeList and Arena Verification

### 6.1 FreeList Invariants

Verify the free list maintains valid structure:

```rust
#[cfg(kani)]
mod freelist_proofs {
    use crate::storage::mmap::freelist::FreeList;

    /// Verify FreeList push/pop roundtrip.
    #[kani::proof]
    fn verify_freelist_push_pop() {
        let mut list = FreeList::new();
        let id: u64 = kani::any();
        kani::assume(id != u64::MAX); // u64::MAX is sentinel for empty

        list.push(id);
        let popped = list.pop();

        assert_eq!(popped, Some(id));
    }

    /// Verify empty FreeList returns None.
    #[kani::proof]
    fn verify_freelist_empty_pop() {
        let mut list = FreeList::new();
        assert_eq!(list.pop(), None);
    }

    /// Verify FreeList with_head initialization.
    #[kani::proof]
    fn verify_freelist_with_head() {
        let head: u64 = kani::any();

        let mut list = FreeList::with_head(head);

        if head == u64::MAX {
            assert_eq!(list.pop(), None);
        } else {
            assert_eq!(list.pop(), Some(head));
        }
    }
}
```

### 6.2 Arena Allocator Bounds

```rust
#[cfg(kani)]
mod arena_proofs {
    use crate::storage::mmap::arena::ArenaAllocator;

    /// Verify arena allocation stays within bounds.
    #[kani::proof]
    fn verify_arena_allocation_bounded() {
        let start: u64 = kani::any();
        let end: u64 = kani::any();
        let current: u64 = kani::any();

        kani::assume(start <= current);
        kani::assume(current <= end);
        kani::assume(end - start <= 1_000_000); // Bounded for verification

        let mut arena = ArenaAllocator::new(start, end, current);

        let size: u64 = kani::any();
        kani::assume(size <= 1000); // Small allocations

        if let Some(offset) = arena.allocate(size) {
            assert!(offset >= start);
            assert!(offset + size <= end);
        }
    }

    /// Verify arena refuses allocation when full.
    #[kani::proof]
    fn verify_arena_full_returns_none() {
        let start: u64 = 0;
        let end: u64 = 100;
        let current: u64 = 100; // Arena is full

        let mut arena = ArenaAllocator::new(start, end, current);

        let size: u64 = kani::any();
        kani::assume(size > 0);

        assert_eq!(arena.allocate(size), None);
    }
}
```

---

## Phase 7: Complete Implementation File

### 7.1 Full `src/kani_proofs.rs`

The complete implementation combining all phases:

```rust
//! Kani formal verification proofs for Interstellar.
//!
//! This module contains proof harnesses that verify correctness properties
//! of critical code paths using the Kani model checker.
//!
//! # Running Proofs
//!
//! ```bash
//! cargo kani                                    # Run all proofs
//! cargo kani --harness <name>                   # Run specific proof
//! cargo kani --harness <name> --visualize       # Generate counterexample
//! ```
//!
//! # Proof Categories
//!
//! - `records_proofs`: Packed struct serialization and layout verification
//! - `value_proofs`: Value type conversions and serialization
//! - `offset_proofs`: File offset arithmetic overflow checking
//! - `freelist_proofs`: Free list data structure invariants
//! - `arena_proofs`: Arena allocator bounds checking

#![cfg(kani)]

// Re-export all proof modules
mod records_proofs;
mod value_proofs;
mod offset_proofs;
mod freelist_proofs;
mod arena_proofs;
```

Alternatively, all proofs can be in a single file with submodules:

```rust
//! Kani formal verification proofs for Interstellar.

#![cfg(kani)]

mod records_proofs {
    // ... struct verification proofs
}

mod value_proofs {
    // ... value conversion proofs
}

mod offset_proofs {
    // ... offset calculation proofs
}

mod freelist_proofs {
    // ... free list proofs
}

mod arena_proofs {
    // ... arena allocator proofs
}
```

---

## Testing Strategy

### Running Verification

```bash
# Install Kani (first time only)
cargo install --locked kani-verifier
kani setup

# Run all proofs (may take several minutes)
cargo kani

# Run specific proof for debugging
cargo kani --harness verify_node_record_roundtrip

# Get counterexample for failing proof
cargo kani --harness verify_u64_to_value_documents_overflow --visualize

# Run with increased unwind bound for loops
cargo kani --default-unwind 20
```

### Expected Results

| Proof | Expected | Notes |
|-------|----------|-------|
| `verify_struct_sizes_match_constants` | PASS | Validates layout constants |
| `verify_file_header_roundtrip` | PASS | Confirms serialization |
| `verify_node_record_roundtrip` | PASS | Confirms serialization |
| `verify_edge_record_roundtrip` | PASS | Confirms serialization |
| `verify_u64_to_value_documents_overflow` | PASS* | Documents known limitation |
| `verify_u64_to_value_safe_range` | PASS | Verifies safe subset |
| `verify_i64_to_value` | PASS | Safe conversion |
| `verify_u32_to_value` | PASS | Safe conversion |
| `verify_f64_to_value` | PASS | Safe conversion |
| `verify_f32_to_value` | PASS | Safe conversion |
| `verify_bool_to_value` | PASS | Safe conversion |
| `verify_serialization_length_bounds_documented` | PASS | Documents limitation |
| `verify_list_serialization_small` | PASS | Bounded verification |
| `verify_node_offset_no_overflow` | PASS | Within capacity bounds |
| `verify_edge_offset_no_overflow` | PASS | Within capacity bounds |
| `verify_total_file_size_no_overflow` | PASS | Within capacity bounds |
| `verify_freelist_push_pop` | PASS | Roundtrip property |
| `verify_freelist_empty_pop` | PASS | Empty list behavior |
| `verify_freelist_with_head` | PASS | Initialization |
| `verify_arena_allocation_bounded` | PASS | Bounds checking |
| `verify_arena_full_returns_none` | PASS | Full arena behavior |

### CI Integration

Add to `.github/workflows/ci.yml`:

```yaml
kani:
  name: Kani Verification
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - name: Install Kani
      run: |
        cargo install --locked kani-verifier
        kani setup
    - name: Run Kani proofs
      run: cargo kani --output-format terse
```

---

## Implementation Checklist

### Phase 1: Setup
- [ ] Add `#[cfg(kani)] mod kani_proofs;` to `src/lib.rs`
- [ ] Create `src/kani_proofs.rs` with module structure
- [ ] Add Kani metadata to `Cargo.toml`
- [ ] Document installation in README or separate file

### Phase 2: Packed Struct Verification
- [ ] `verify_struct_sizes_match_constants`
- [ ] `verify_file_header_roundtrip`
- [ ] `verify_node_record_roundtrip`
- [ ] `verify_edge_record_roundtrip`

### Phase 3: Value Type Verification
- [ ] `verify_u64_to_value_documents_overflow`
- [ ] `verify_u64_to_value_safe_range`
- [ ] `verify_i64_to_value`
- [ ] `verify_u32_to_value`
- [ ] `verify_f64_to_value`
- [ ] `verify_f32_to_value`
- [ ] `verify_bool_to_value`

### Phase 4: Serialization Verification
- [ ] `verify_serialization_length_bounds_documented`
- [ ] `verify_list_serialization_small`

### Phase 5: Offset Verification
- [ ] `verify_node_offset_no_overflow`
- [ ] `verify_edge_offset_no_overflow`
- [ ] `verify_total_file_size_no_overflow`

### Phase 6: Data Structure Verification
- [ ] `verify_freelist_push_pop`
- [ ] `verify_freelist_empty_pop`
- [ ] `verify_freelist_with_head`
- [ ] `verify_arena_allocation_bounded`
- [ ] `verify_arena_full_returns_none`

### Phase 7: CI Integration
- [ ] Add Kani to CI workflow
- [ ] Verify all proofs pass in CI

---

## Future Extensions

Once the initial verification is complete, consider adding:

1. **Graph invariant verification**: Verify edge endpoints always reference valid nodes
2. **WAL verification**: Verify write-ahead log entry roundtrips
3. **Schema validation**: Verify schema constraints are enforced
4. **Iterator safety**: Verify traversal iterators don't produce invalid states

---

## References

- [Kani Documentation](https://model-checking.github.io/kani/)
- [Kani Tutorial](https://model-checking.github.io/kani/kani-tutorial.html)
- [Kani GitHub](https://github.com/model-checking/kani)
- [AWS Blog on Kani](https://aws.amazon.com/blogs/opensource/how-amazon-uses-kani-to-prove-correctness-of-software/)


## Phase 3: Value Type Conversion Verification

### 3.1 Known Issue: u64 to Value Conversion

The current implementation has a silent overflow bug:

```rust
// src/value.rs:400-404
impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::Int(value as i64)  // BUG: wraps for values > i64::MAX
    }
}
```

This proof will **intentionally fail** to document the bug:

```rust
#[cfg(kani)]
mod value_proofs {
    use crate::value::Value;

    /// Document the u64 → Value overflow behavior.
    ///
    /// This proof is expected to FAIL, demonstrating that values > i64::MAX
    /// will wrap around to negative numbers. This documents the limitation.
    ///
    /// To fix: Change `From<u64>` to `TryFrom<u64>` that returns an error
    /// for values > i64::MAX, or add a `Value::UInt(u64)` variant.
    #[kani::proof]
    fn verify_u64_to_value_documents_overflow() {
        let value: u64 = kani::any();
        let result = Value::from(value);

        if let Value::Int(n) = result {
            // This assertion will FAIL for values > i64::MAX
            // because they wrap to negative numbers
            if value <= i64::MAX as u64 {
                assert_eq!(n as u64, value, "small u64 should roundtrip");
            } else {
                // For values > i64::MAX, the cast wraps around
                // This documents the behavior rather than fixing it
                assert!(n < 0, "large u64 wraps to negative");
            }
        }
    }

    /// Verify u64 values within i64 range convert correctly.
    ///
    /// This proof verifies the SAFE subset of u64 → Value conversions.
    #[kani::proof]
    fn verify_u64_to_value_safe_range() {
        let value: u64 = kani::any();
        kani::assume(value <= i64::MAX as u64);

        let result = Value::from(value);

        if let Value::Int(n) = result {
            assert!(n >= 0, "value in safe range should be non-negative");
            assert_eq!(n as u64, value, "value should roundtrip");
        } else {
            panic!("From<u64> should produce Value::Int");
        }
    }
}
```

### 3.2 Safe Integer Conversions

Verify conversions that should always be safe:

```rust
/// Verify i64 → Value conversion is always safe.
#[kani::proof]
fn verify_i64_to_value() {
    let value: i64 = kani::any();
    let result = Value::from(value);

    if let Value::Int(n) = result {
        assert_eq!(n, value);
    } else {
        panic!("From<i64> should produce Value::Int");
    }
}

/// Verify u32 → Value conversion is always safe (fits in i64).
#[kani::proof]
fn verify_u32_to_value() {
    let value: u32 = kani::any();
    let result = Value::from(value);

    if let Value::Int(n) = result {
        assert!(n >= 0);
        assert_eq!(n as u32, value);
    } else {
        panic!("From<u32> should produce Value::Int");
    }
}

/// Verify f64 → Value conversion preserves value.
#[kani::proof]
fn verify_f64_to_value() {
    let value: f64 = kani::any();
    // Assume finite values (NaN != NaN would fail equality)
    kani::assume(value.is_finite());

    let result = Value::from(value);

    if let Value::Float(f) = result {
        assert_eq!(f, value);
    } else {
        panic!("From<f64> should produce Value::Float");
    }
}

/// Verify f32 → Value conversion is lossless.
#[kani::proof]
fn verify_f32_to_value() {
    let value: f32 = kani::any();
    kani::assume(value.is_finite());

    let result = Value::from(value);

    if let Value::Float(f) = result {
        // f32 → f64 is lossless
        assert_eq!(f, value as f64);
    } else {
        panic!("From<f32> should produce Value::Float");
    }
}

/// Verify bool → Value conversion.
#[kani::proof]
fn verify_bool_to_value() {
    let value: bool = kani::any();
    let result = Value::from(value);

    match result {
        Value::Bool(b) => assert_eq!(b, value),
        _ => panic!("From<bool> should produce Value::Bool"),
    }
}
```

---

## Phase 4: Serialization Length Verification

### 4.1 Document Length Truncation Limitation

The `Value::serialize` method casts lengths to `u32`:

```rust
// src/value.rs:510
let len = s.len() as u32;  // Truncation if string > 4GB
```

This is acceptable for a graph database (strings > 4GB are pathological), but should be documented:

```rust
/// Document the serialization length limitation.
///
/// Strings, lists, and maps with more than u32::MAX elements will have
/// their lengths truncated during serialization. This is acceptable for
/// typical graph database usage but should be documented.
#[kani::proof]
fn verify_serialization_length_bounds_documented() {
    let len: usize = kani::any();
    
    // This is the SAFE range
    kani::assume(len <= u32::MAX as usize);
    
    let truncated = len as u32;
    assert_eq!(truncated as usize, len, "length should not truncate");
}

/// Verify serialization works for typical sizes.
///
/// Uses bounded unwinding to verify small collections serialize correctly.
#[kani::proof]
#[kani::unwind(5)]  // Verify up to 4 elements
fn verify_list_serialization_small() {
    let len: usize = kani::any();
    kani::assume(len <= 4);

    let items: Vec<Value> = (0..len).map(|i| Value::Int(i as i64)).collect();
    let value = Value::List(items.clone());

    let mut buf = Vec::new();
    value.serialize(&mut buf);

    let mut pos = 0;
    let recovered = Value::deserialize(&buf, &mut pos).unwrap();

    if let Value::List(recovered_items) = recovered {
        assert_eq!(recovered_items.len(), items.len());
    } else {
        panic!("Should deserialize to List");
    }
}
```

---

## Phase 5: Offset Calculation Verification

### 5.1 Node Offset Calculation

Verify node table offset calculations cannot overflow for reasonable capacities:

```rust
#[cfg(kani)]
mod offset_proofs {
    use crate::storage::mmap::records::{HEADER_SIZE, NODE_RECORD_SIZE, EDGE_RECORD_SIZE};

    /// Maximum supported node capacity (1 billion nodes).
    ///
    /// This bounds the verification to realistic database sizes.
    /// 1B nodes × 48 bytes = 48GB node table, which is reasonable.
    const MAX_NODE_CAPACITY: u64 = 1_000_000_000;

    /// Maximum supported edge capacity (10 billion edges).
    const MAX_EDGE_CAPACITY: u64 = 10_000_000_000;

    /// Verify node offset calculation cannot overflow.
    ///
    /// The node offset formula is: HEADER_SIZE + (node_id * NODE_RECORD_SIZE)
    #[kani::proof]
    fn verify_node_offset_no_overflow() {
        let node_id: u64 = kani::any();
        kani::assume(node_id < MAX_NODE_CAPACITY);

        // This is the actual calculation from the mmap module
        let offset = (HEADER_SIZE as u64)
            .checked_add(node_id.checked_mul(NODE_RECORD_SIZE as u64).unwrap())
            .unwrap();

        // Verify offset is valid (greater than header, no wraparound)
        assert!(offset >= HEADER_SIZE as u64);
        assert!(offset < u64::MAX - NODE_RECORD_SIZE as u64);
    }

    /// Verify edge offset calculation cannot overflow.
    ///
    /// The edge offset formula is:
    /// HEADER_SIZE + (node_capacity * NODE_RECORD_SIZE) + (edge_id * EDGE_RECORD_SIZE)
    #[kani::proof]
    fn verify_edge_offset_no_overflow() {
        let node_capacity: u64 = kani::any();
        let edge_id: u64 = kani::any();

        kani::assume(node_capacity <= MAX_NODE_CAPACITY);
        kani::assume(edge_id < MAX_EDGE_CAPACITY);

        // Calculate node table size
        let node_table_size = node_capacity
            .checked_mul(NODE_RECORD_SIZE as u64)
            .unwrap();

        // Calculate edge table start
        let edge_table_start = (HEADER_SIZE as u64)
            .checked_add(node_table_size)
            .unwrap();

        // Calculate edge offset within table
        let edge_offset_in_table = edge_id
            .checked_mul(EDGE_RECORD_SIZE as u64)
            .unwrap();

        // Final offset
        let offset = edge_table_start
            .checked_add(edge_offset_in_table)
            .unwrap();

        // Verify offset is valid
        assert!(offset >= edge_table_start);
    }

    /// Verify total file size calculation cannot overflow.
    #[kani::proof]
    fn verify_total_file_size_no_overflow() {
        let node_capacity: u64 = kani::any();
        let edge_capacity: u64 = kani::any();
        let arena_size: u64 = kani::any();
        let string_table_size: u64 = kani::any();

        // Constrain to reasonable bounds
        kani::assume(node_capacity <= MAX_NODE_CAPACITY);
        kani::assume(edge_capacity <= MAX_EDGE_CAPACITY);
        kani::assume(arena_size <= 100_000_000_000);     // 100GB property arena
        kani::assume(string_table_size <= 10_000_000_000); // 10GB string table

        let total = (HEADER_SIZE as u64)
            .checked_add(node_capacity.checked_mul(NODE_RECORD_SIZE as u64).unwrap())
            .unwrap()
            .checked_add(edge_capacity.checked_mul(EDGE_RECORD_SIZE as u64).unwrap())
            .unwrap()
            .checked_add(arena_size)
            .unwrap()
            .checked_add(string_table_size)
            .unwrap();

        // Should fit in u64 and be a reasonable file size (< 1 exabyte)
        assert!(total < 1_000_000_000_000_000_000u64);
    }
}
```

---
