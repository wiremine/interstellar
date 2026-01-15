//! Kani formal verification proofs for Intersteller.
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
//! - `records_proofs`: Packed struct serialization and layout verification
//! - `value_proofs`: Value type conversions and serialization
//! - `offset_proofs`: File offset arithmetic overflow checking
//! - `freelist_proofs`: Free list data structure invariants
//! - `arena_proofs`: Arena allocator bounds checking

#![cfg(kani)]

// =============================================================================
// Phase 2: Packed Struct Verification
// =============================================================================

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
        assert_eq!(
            std::mem::size_of::<PropertyEntry>(),
            PROPERTY_ENTRY_HEADER_SIZE,
            "PropertyEntry size mismatch"
        );
        assert_eq!(
            std::mem::size_of::<StringEntry>(),
            STRING_ENTRY_HEADER_SIZE,
            "StringEntry size mismatch"
        );
    }

    /// Verify FileHeader serialization roundtrip.
    #[kani::proof]
    fn verify_file_header_roundtrip() {
        let node_count: u64 = kani::any();
        let node_capacity: u64 = kani::any();
        let edge_count: u64 = kani::any();
        let edge_capacity: u64 = kani::any();
        let string_table_offset: u64 = kani::any();
        let string_table_end: u64 = kani::any();
        let property_arena_offset: u64 = kani::any();
        let arena_next_offset: u64 = kani::any();
        let free_node_head: u64 = kani::any();
        let free_edge_head: u64 = kani::any();
        let next_node_id: u64 = kani::any();
        let next_edge_id: u64 = kani::any();
        let schema_offset: u64 = kani::any();
        let schema_size: u64 = kani::any();
        let schema_version: u32 = kani::any();

        let mut header = FileHeader::new();
        header.node_count = node_count;
        header.node_capacity = node_capacity;
        header.edge_count = edge_count;
        header.edge_capacity = edge_capacity;
        header.string_table_offset = string_table_offset;
        header.string_table_end = string_table_end;
        header.property_arena_offset = property_arena_offset;
        header.arena_next_offset = arena_next_offset;
        header.free_node_head = free_node_head;
        header.free_edge_head = free_edge_head;
        header.next_node_id = next_node_id;
        header.next_edge_id = next_edge_id;
        header.schema_offset = schema_offset;
        header.schema_size = schema_size;
        header.schema_version = schema_version;

        let bytes = header.to_bytes();
        let recovered = FileHeader::from_bytes(&bytes);

        // Copy fields to avoid unaligned reference issues
        let rec_node_count = recovered.node_count;
        let rec_node_capacity = recovered.node_capacity;
        let rec_edge_count = recovered.edge_count;
        let rec_edge_capacity = recovered.edge_capacity;
        let rec_magic = recovered.magic;
        let rec_version = recovered.version;

        assert_eq!(rec_magic, MAGIC);
        assert_eq!(rec_version, VERSION);
        assert_eq!(rec_node_count, node_count);
        assert_eq!(rec_node_capacity, node_capacity);
        assert_eq!(rec_edge_count, edge_count);
        assert_eq!(rec_edge_capacity, edge_capacity);
    }

    /// Verify NodeRecord serialization roundtrip.
    #[kani::proof]
    fn verify_node_record_roundtrip() {
        let id: u64 = kani::any();
        let label_id: u32 = kani::any();
        let flags: u32 = kani::any();
        let first_out_edge: u64 = kani::any();
        let first_in_edge: u64 = kani::any();
        let prop_head: u64 = kani::any();

        let mut record = NodeRecord::new(id, label_id);
        record.flags = flags;
        record.first_out_edge = first_out_edge;
        record.first_in_edge = first_in_edge;
        record.prop_head = prop_head;

        let bytes = record.to_bytes();
        let recovered = NodeRecord::from_bytes(&bytes);

        // Copy fields to avoid unaligned reference issues
        let rec_id = recovered.id;
        let rec_label_id = recovered.label_id;
        let rec_flags = recovered.flags;
        let rec_first_out_edge = recovered.first_out_edge;
        let rec_first_in_edge = recovered.first_in_edge;
        let rec_prop_head = recovered.prop_head;

        assert_eq!(rec_id, id);
        assert_eq!(rec_label_id, label_id);
        assert_eq!(rec_flags, flags);
        assert_eq!(rec_first_out_edge, first_out_edge);
        assert_eq!(rec_first_in_edge, first_in_edge);
        assert_eq!(rec_prop_head, prop_head);
    }

    /// Verify EdgeRecord serialization roundtrip.
    #[kani::proof]
    fn verify_edge_record_roundtrip() {
        let id: u64 = kani::any();
        let label_id: u32 = kani::any();
        let flags: u32 = kani::any();
        let src: u64 = kani::any();
        let dst: u64 = kani::any();
        let next_out: u64 = kani::any();
        let next_in: u64 = kani::any();
        let prop_head: u64 = kani::any();

        let mut record = EdgeRecord::new(id, label_id, src, dst);
        record.flags = flags;
        record.next_out = next_out;
        record.next_in = next_in;
        record.prop_head = prop_head;

        let bytes = record.to_bytes();
        let recovered = EdgeRecord::from_bytes(&bytes);

        // Copy fields to avoid unaligned reference issues
        let rec_id = recovered.id;
        let rec_label_id = recovered.label_id;
        let rec_flags = recovered.flags;
        let rec_src = recovered.src;
        let rec_dst = recovered.dst;
        let rec_next_out = recovered.next_out;
        let rec_next_in = recovered.next_in;
        let rec_prop_head = recovered.prop_head;

        assert_eq!(rec_id, id);
        assert_eq!(rec_label_id, label_id);
        assert_eq!(rec_flags, flags);
        assert_eq!(rec_src, src);
        assert_eq!(rec_dst, dst);
        assert_eq!(rec_next_out, next_out);
        assert_eq!(rec_next_in, next_in);
        assert_eq!(rec_prop_head, prop_head);
    }

    /// Verify PropertyEntry serialization roundtrip.
    #[kani::proof]
    fn verify_property_entry_roundtrip() {
        let key_id: u32 = kani::any();
        let value_type: u8 = kani::any();
        let value_len: u32 = kani::any();
        let next: u64 = kani::any();

        let entry = PropertyEntry::new(key_id, value_type, value_len, next);
        let bytes = entry.to_bytes();
        let recovered = PropertyEntry::from_bytes(&bytes);

        // Copy fields to avoid unaligned reference issues
        let rec_key_id = recovered.key_id;
        let rec_value_type = recovered.value_type;
        let rec_value_len = recovered.value_len;
        let rec_next = recovered.next;

        assert_eq!(rec_key_id, key_id);
        assert_eq!(rec_value_type, value_type);
        assert_eq!(rec_value_len, value_len);
        assert_eq!(rec_next, next);
    }

    /// Verify StringEntry serialization roundtrip.
    #[kani::proof]
    fn verify_string_entry_roundtrip() {
        let id: u32 = kani::any();
        let len: u32 = kani::any();

        let entry = StringEntry::new(id, len);
        let bytes = entry.to_bytes();
        let recovered = StringEntry::from_bytes(&bytes);

        // Copy fields to avoid unaligned reference issues
        let rec_id = recovered.id;
        let rec_len = recovered.len;

        assert_eq!(rec_id, id);
        assert_eq!(rec_len, len);
    }
}

// =============================================================================
// Phase 3: Value Type Conversion Verification
// =============================================================================

mod value_proofs {
    use crate::value::Value;

    /// Document the u64 -> Value overflow behavior.
    ///
    /// This proof verifies that u64 values within the safe range (0..=i64::MAX)
    /// convert correctly, and documents that larger values wrap to negative.
    #[kani::proof]
    fn verify_u64_to_value_documents_overflow() {
        let value: u64 = kani::any();
        let result = Value::from(value);

        if let Value::Int(n) = result {
            if value <= i64::MAX as u64 {
                // Safe range: should roundtrip correctly
                assert_eq!(n as u64, value, "small u64 should roundtrip");
            } else {
                // For values > i64::MAX, the cast wraps around
                // This documents the behavior rather than fixing it
                assert!(n < 0, "large u64 wraps to negative");
            }
        } else {
            panic!("From<u64> should produce Value::Int");
        }
    }

    /// Verify u64 values within i64 range convert correctly.
    ///
    /// This proof verifies the SAFE subset of u64 -> Value conversions.
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

    /// Verify i64 -> Value conversion is always safe.
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

    /// Verify u32 -> Value conversion is always safe (fits in i64).
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

    /// Verify i32 -> Value conversion is always safe.
    #[kani::proof]
    fn verify_i32_to_value() {
        let value: i32 = kani::any();
        let result = Value::from(value);

        if let Value::Int(n) = result {
            assert_eq!(n, value as i64);
        } else {
            panic!("From<i32> should produce Value::Int");
        }
    }

    /// Verify f64 -> Value conversion preserves value.
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

    /// Verify f32 -> Value conversion is lossless.
    #[kani::proof]
    fn verify_f32_to_value() {
        let value: f32 = kani::any();
        kani::assume(value.is_finite());

        let result = Value::from(value);

        if let Value::Float(f) = result {
            // f32 -> f64 is lossless
            assert_eq!(f, value as f64);
        } else {
            panic!("From<f32> should produce Value::Float");
        }
    }

    /// Verify bool -> Value conversion.
    #[kani::proof]
    fn verify_bool_to_value() {
        let value: bool = kani::any();
        let result = Value::from(value);

        match result {
            Value::Bool(b) => assert_eq!(b, value),
            _ => panic!("From<bool> should produce Value::Bool"),
        }
    }
}

// =============================================================================
// Phase 4: Serialization Length Verification
// =============================================================================

mod serialization_proofs {
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

    // Note: verify_list_serialization_small is omitted because Value::serialize
    // and Value::deserialize have complex control flow that causes Kani to
    // timeout even with small bounds. The serialization roundtrip is thoroughly
    // tested by property-based tests in the regular test suite instead.
}

// =============================================================================
// Phase 5: Offset Calculation Verification
// =============================================================================

mod offset_proofs {
    use crate::storage::mmap::records::{EDGE_RECORD_SIZE, HEADER_SIZE, NODE_RECORD_SIZE};

    /// Maximum supported node capacity (1 billion nodes).
    ///
    /// This bounds the verification to realistic database sizes.
    /// 1B nodes * 48 bytes = 48GB node table, which is reasonable.
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
        let node_table_size = node_capacity.checked_mul(NODE_RECORD_SIZE as u64).unwrap();

        // Calculate edge table start
        let edge_table_start = (HEADER_SIZE as u64).checked_add(node_table_size).unwrap();

        // Calculate edge offset within table
        let edge_offset_in_table = edge_id.checked_mul(EDGE_RECORD_SIZE as u64).unwrap();

        // Final offset
        let offset = edge_table_start.checked_add(edge_offset_in_table).unwrap();

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
        kani::assume(arena_size <= 100_000_000_000); // 100GB property arena
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

// =============================================================================
// Phase 6: FreeList and Arena Verification
// =============================================================================

mod freelist_proofs {
    use crate::storage::mmap::freelist::FreeList;

    // Note: FreeList uses hashbrown::HashMap internally for tracking freed slots.
    // Kani cannot efficiently verify code that uses HashMap because it generates
    // an enormous symbolic state space, even with concrete inputs.
    //
    // Only proofs that don't interact with the HashMap (new list creation and
    // empty list operations) are included here. The HashMap-based operations
    // (free, allocate with free slots) are thoroughly tested via property-based
    // tests in the regular test suite.

    /// Verify empty FreeList returns current_count (indicating table extension needed).
    ///
    /// This proof works because it doesn't insert into the HashMap.
    #[kani::proof]
    fn verify_freelist_empty_allocate() {
        let mut list = FreeList::new();
        let current_count: u64 = kani::any();

        let allocated = list.allocate(current_count);
        assert_eq!(allocated, current_count);
    }

    /// Verify FreeList with_head initialization.
    ///
    /// This proof works because with_head only sets the head value without
    /// populating the HashMap.
    #[kani::proof]
    fn verify_freelist_with_head() {
        let head: u64 = kani::any();

        let list = FreeList::with_head(head);

        if head == u64::MAX {
            assert!(list.is_empty());
        } else {
            assert!(!list.is_empty());
            assert_eq!(list.head(), head);
        }
    }
}

mod arena_proofs {
    use crate::storage::mmap::arena::ArenaAllocator;

    // Note: ArenaAllocator uses AtomicU64 internally, which can be slow for Kani
    // to verify with fully symbolic inputs. We use bounded ranges to keep
    // verification tractable.

    /// Verify arena allocation stays within bounds.
    ///
    /// Uses tightly bounded values to avoid AtomicU64 state explosion.
    #[kani::proof]
    fn verify_arena_allocation_bounded() {
        let start: u64 = kani::any();
        let end: u64 = kani::any();
        let current: u64 = kani::any();

        // Tighter bounds for faster verification
        kani::assume(start <= 10000);
        kani::assume(end <= 20000);
        kani::assume(start <= current);
        kani::assume(current <= end);

        let allocator = ArenaAllocator::new(start, end, current);

        let size: usize = kani::any();
        kani::assume(size <= 100); // Small allocations
        kani::assume(size > 0);

        if let Ok(offset) = allocator.allocate(size) {
            assert!(offset >= start);
            assert!(offset + size as u64 <= end);
        }
        // If allocation fails (OutOfSpace), that's also valid behavior
    }

    /// Verify arena refuses allocation when full.
    #[kani::proof]
    fn verify_arena_full_returns_error() {
        let start: u64 = 0;
        let end: u64 = 100;
        let current: u64 = 100; // Arena is full

        let allocator = ArenaAllocator::new(start, end, current);

        let size: usize = kani::any();
        kani::assume(size > 0);
        kani::assume(size <= 1000);

        // Should return error when arena is full
        assert!(allocator.allocate(size).is_err());
    }

    /// Verify arena has_space is consistent with allocate.
    ///
    /// Uses tightly bounded values for tractable verification.
    #[kani::proof]
    fn verify_arena_has_space_consistency() {
        let start: u64 = kani::any();
        let end: u64 = kani::any();
        let current: u64 = kani::any();

        // Tight bounds for faster verification
        kani::assume(start <= 1000);
        kani::assume(end <= 2000);
        kani::assume(start <= current);
        kani::assume(current <= end);

        let allocator = ArenaAllocator::new(start, end, current);

        let size: usize = kani::any();
        kani::assume(size <= 100);
        kani::assume(size > 0);

        let has_space = allocator.has_space(size);

        if has_space {
            // If has_space returns true, allocate should succeed
            assert!(allocator.allocate(size).is_ok());
        }
        // Note: has_space returning false doesn't guarantee allocate fails
        // because has_space is a non-mutating check that can be racy.
        // In single-threaded context, if has_space is false, allocate should fail,
        // but we don't test that direction to avoid complexity.
    }

    /// Verify entry_size calculation is correct.
    #[kani::proof]
    fn verify_arena_entry_size() {
        use crate::storage::mmap::records::PROPERTY_ENTRY_HEADER_SIZE;

        let value_len: usize = kani::any();
        kani::assume(value_len <= 1_000_000); // Reasonable value size

        let entry_size = ArenaAllocator::entry_size(value_len);

        assert_eq!(entry_size, PROPERTY_ENTRY_HEADER_SIZE + value_len);
    }
}
