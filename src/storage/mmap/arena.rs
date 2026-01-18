//! Property arena allocator for variable-length property storage.
//!
//! Properties are stored as linked lists in a separate arena section of the file.
//! This module provides allocation and writing functions for property data.
//!
//! # Arena Layout
//!
//! The property arena is a contiguous region of the file that grows upward
//! (toward higher offsets). Properties are stored as linked lists:
//!
//! ```text
//! [PropertyEntry header (17 bytes)][value data (N bytes)]
//! [PropertyEntry header (17 bytes)][value data (M bytes)]
//! ...
//! ```
//!
//! Each property entry contains:
//! - `key_id` (4 bytes): String table ID for the property key
//! - `value_type` (1 byte): Value discriminant
//! - `value_len` (4 bytes): Length of serialized value
//! - `next` (8 bytes): Offset to next property, or `u64::MAX` if last
//!
//! # Allocation Strategy
//!
//! The arena uses a simple bump allocator. New allocations are appended at the
//! current arena end position. There is no compaction or free list for the arena;
//! deleted properties simply leave gaps that are not reclaimed.

use crate::error::StorageError;
use crate::storage::mmap::records::{PropertyEntry, PROPERTY_ENTRY_HEADER_SIZE};
use crate::value::Value;
use hashbrown::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks the current allocation position in the property arena.
///
/// This is a simple bump allocator that appends new property entries
/// at the end of the arena.
pub struct ArenaAllocator {
    /// Current write position (absolute file offset)
    current_offset: AtomicU64,
    /// Start of the arena (from header.property_arena_offset)
    arena_start: u64,
    /// End of the arena (start of string table or end of arena space)
    arena_end: u64,
}

impl ArenaAllocator {
    /// Create a new arena allocator.
    ///
    /// # Arguments
    ///
    /// * `arena_start` - Absolute file offset where arena begins
    /// * `arena_end` - Absolute file offset where arena ends (e.g., string table start)
    /// * `current_offset` - Current write position (arena_start for new database)
    pub fn new(arena_start: u64, arena_end: u64, current_offset: u64) -> Self {
        Self {
            current_offset: AtomicU64::new(current_offset),
            arena_start,
            arena_end,
        }
    }

    /// Get the current allocation offset.
    #[inline]
    pub fn current_offset(&self) -> u64 {
        self.current_offset.load(Ordering::SeqCst)
    }

    /// Get the arena start offset.
    #[inline]
    pub fn arena_start(&self) -> u64 {
        self.arena_start
    }

    /// Get the arena end offset.
    #[inline]
    pub fn arena_end(&self) -> u64 {
        self.arena_end
    }

    /// Calculate the total size needed for a property entry.
    ///
    /// Returns the size of the header plus the serialized value data.
    #[inline]
    pub fn entry_size(value_len: usize) -> usize {
        PROPERTY_ENTRY_HEADER_SIZE + value_len
    }

    /// Check if there's enough space for an allocation.
    ///
    /// # Arguments
    ///
    /// * `size` - Number of bytes needed
    ///
    /// # Returns
    ///
    /// `true` if the allocation can fit, `false` otherwise.
    pub fn has_space(&self, size: usize) -> bool {
        let current = self.current_offset.load(Ordering::SeqCst);
        current + size as u64 <= self.arena_end
    }

    /// Reserve space in the arena.
    ///
    /// Atomically allocates `size` bytes and returns the starting offset.
    /// Uses compare-and-swap to ensure atomic allocation without race conditions.
    ///
    /// # Arguments
    ///
    /// * `size` - Number of bytes to allocate
    ///
    /// # Returns
    ///
    /// The absolute file offset where the allocation starts.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::OutOfSpace`] if there isn't enough room.
    ///
    /// # Thread Safety
    ///
    /// This method is safe to call concurrently from multiple threads.
    /// Allocation is performed atomically using compare-and-swap.
    pub fn allocate(&self, size: usize) -> Result<u64, StorageError> {
        let size = size as u64;

        // Use compare-and-swap loop for atomic allocation without TOCTOU race
        loop {
            let current = self.current_offset.load(Ordering::SeqCst);
            let new_offset = current + size;

            // Check if we would exceed the arena
            if new_offset > self.arena_end {
                return Err(StorageError::OutOfSpace);
            }

            // Atomically try to update the offset
            match self.current_offset.compare_exchange(
                current,
                new_offset,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(current), // Successfully allocated
                Err(_) => continue,          // Contention, retry
            }
        }
    }

    /// Update the arena end (e.g., after file growth).
    pub fn set_arena_end(&mut self, new_end: u64) {
        self.arena_end = new_end;
    }
}

/// Serialize properties into arena format.
///
/// Converts a property map into a series of bytes ready to be written
/// to the arena. Returns the serialized data and a list of entry offsets
/// for linking.
///
/// # Arguments
///
/// * `properties` - The properties to serialize
/// * `intern_key` - Function to intern a property key and get its ID
///
/// # Returns
///
/// A tuple of:
/// - `Vec<u8>` - The serialized property data
/// - `Vec<usize>` - Offsets within the data where each entry's `next` field is located
///
/// # Example
///
/// ```ignore
/// let props = HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]);
/// let (data, next_offsets) = serialize_properties(&props, |key| interner.intern(key));
/// ```
pub fn serialize_properties<F>(
    properties: &HashMap<String, Value>,
    mut intern_key: F,
) -> (Vec<u8>, Vec<usize>)
where
    F: FnMut(&str) -> u32,
{
    let mut data = Vec::new();
    let mut next_offsets = Vec::new();

    let entries: Vec<_> = properties.iter().collect();

    for (key, value) in entries.iter() {
        // Serialize the value
        let mut value_data = Vec::new();
        value.serialize(&mut value_data);

        // Get key ID from interner
        let key_id = intern_key(key);

        // Determine the next pointer (will be patched later with actual offsets)
        // For now, use u64::MAX to indicate "not yet linked"
        let next = u64::MAX;

        // Create property entry header
        let entry = PropertyEntry::new(key_id, value.discriminant(), value_data.len() as u32, next);

        // Record where the `next` field is in our data buffer
        // The `next` field is at offset 9 within PropertyEntry (after key_id + value_type + value_len)
        let next_field_offset = data.len() + 9;
        next_offsets.push(next_field_offset);

        // Write entry header
        data.extend_from_slice(&entry.to_bytes());

        // Write value data
        data.extend_from_slice(&value_data);
    }

    (data, next_offsets)
}

/// Link property entries by patching their `next` pointers.
///
/// After writing property entries to the arena, this function patches
/// the `next` fields to create a linked list.
///
/// # Arguments
///
/// * `data` - Mutable reference to the serialized property data
/// * `next_offsets` - Offsets within `data` where `next` fields are located
/// * `base_offset` - The absolute file offset where `data` will be written
/// * `entry_sizes` - Size of each entry (header + value data)
///
/// The last entry's `next` is set to `u64::MAX` to indicate end of list.
pub fn link_property_entries(
    data: &mut [u8],
    next_offsets: &[usize],
    base_offset: u64,
    entry_sizes: &[usize],
) {
    if next_offsets.is_empty() {
        return;
    }

    // Calculate the absolute offset of each entry
    let mut entry_start = base_offset;
    let mut entry_offsets = Vec::with_capacity(next_offsets.len());

    for size in entry_sizes {
        entry_offsets.push(entry_start);
        entry_start += *size as u64;
    }

    // Link each entry to the next
    for i in 0..next_offsets.len() {
        let next_value = if i + 1 < entry_offsets.len() {
            entry_offsets[i + 1]
        } else {
            u64::MAX // Last entry
        };

        let offset = next_offsets[i];
        data[offset..offset + 8].copy_from_slice(&next_value.to_le_bytes());
    }
}

/// Calculate entry sizes for a list of properties.
///
/// # Arguments
///
/// * `properties` - The properties to measure
///
/// # Returns
///
/// A vector of sizes, one per property entry (header + value data).
pub fn calculate_entry_sizes(properties: &HashMap<String, Value>) -> Vec<usize> {
    properties
        .values()
        .map(|value| {
            let mut buf = Vec::new();
            value.serialize(&mut buf);
            PROPERTY_ENTRY_HEADER_SIZE + buf.len()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_allocator_new() {
        let allocator = ArenaAllocator::new(1000, 2000, 1000);

        assert_eq!(allocator.arena_start(), 1000);
        assert_eq!(allocator.arena_end(), 2000);
        assert_eq!(allocator.current_offset(), 1000);
    }

    #[test]
    fn test_arena_allocator_has_space() {
        let allocator = ArenaAllocator::new(1000, 2000, 1000);

        // Should have space for 1000 bytes
        assert!(allocator.has_space(1000));
        assert!(allocator.has_space(999));

        // Should not have space for more than 1000 bytes
        assert!(!allocator.has_space(1001));
    }

    #[test]
    fn test_arena_allocator_allocate() {
        let allocator = ArenaAllocator::new(1000, 2000, 1000);

        // First allocation
        let offset1 = allocator.allocate(100).unwrap();
        assert_eq!(offset1, 1000);
        assert_eq!(allocator.current_offset(), 1100);

        // Second allocation
        let offset2 = allocator.allocate(200).unwrap();
        assert_eq!(offset2, 1100);
        assert_eq!(allocator.current_offset(), 1300);
    }

    #[test]
    fn test_arena_allocator_out_of_space() {
        let allocator = ArenaAllocator::new(1000, 1100, 1000);

        // First allocation succeeds
        let offset1 = allocator.allocate(50).unwrap();
        assert_eq!(offset1, 1000);

        // Second allocation that would exceed arena fails
        let result = allocator.allocate(100);
        assert!(matches!(result, Err(StorageError::OutOfSpace)));

        // Offset should be rolled back
        assert_eq!(allocator.current_offset(), 1050);
    }

    #[test]
    fn test_serialize_properties_empty() {
        let props: HashMap<String, Value> = HashMap::new();
        let (data, next_offsets) = serialize_properties(&props, |_| 0);

        assert!(data.is_empty());
        assert!(next_offsets.is_empty());
    }

    #[test]
    fn test_serialize_properties_single() {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));

        let (data, next_offsets) = serialize_properties(&props, |_| 42);

        // Should have one entry
        assert_eq!(next_offsets.len(), 1);

        // Data should contain header + value
        // Header: 17 bytes
        // Value: 1 (discriminant) + 4 (length) + 5 ("Alice") = 10 bytes
        assert_eq!(data.len(), 17 + 10);

        // Verify the key_id is at the start (little-endian u32 = 42)
        let key_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        assert_eq!(key_id, 42);
    }

    #[test]
    fn test_serialize_properties_multiple() {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(30));

        let (data, next_offsets) = serialize_properties(&props, |key| match key {
            "name" => 1,
            "age" => 2,
            _ => 0,
        });

        // Should have two entries
        assert_eq!(next_offsets.len(), 2);

        // Data should contain both entries
        assert!(!data.is_empty());
    }

    #[test]
    fn test_link_property_entries_single() {
        let mut data = vec![0u8; 30]; // Dummy data
        let next_offsets = vec![9]; // next field at offset 9
        let entry_sizes = vec![30];

        link_property_entries(&mut data, &next_offsets, 1000, &entry_sizes);

        // Single entry should have next = u64::MAX
        let next = u64::from_le_bytes([
            data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16],
        ]);
        assert_eq!(next, u64::MAX);
    }

    #[test]
    fn test_link_property_entries_multiple() {
        // Create data for two entries
        let mut data = vec![0u8; 60]; // Two 30-byte entries
        let next_offsets = vec![9, 39]; // next fields at offset 9 and 39
        let entry_sizes = vec![30, 30];

        link_property_entries(&mut data, &next_offsets, 1000, &entry_sizes);

        // First entry's next should point to second entry (offset 1030)
        let next1 = u64::from_le_bytes([
            data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16],
        ]);
        assert_eq!(next1, 1030);

        // Second entry's next should be u64::MAX
        let next2 = u64::from_le_bytes([
            data[39], data[40], data[41], data[42], data[43], data[44], data[45], data[46],
        ]);
        assert_eq!(next2, u64::MAX);
    }

    #[test]
    fn test_link_property_entries_empty() {
        let mut data = vec![];
        let next_offsets = vec![];
        let entry_sizes = vec![];

        // Should not panic
        link_property_entries(&mut data, &next_offsets, 1000, &entry_sizes);
    }

    #[test]
    fn test_calculate_entry_sizes() {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));

        let sizes = calculate_entry_sizes(&props);

        assert_eq!(sizes.len(), 2);

        // Each size should be header (17) + value size
        for size in &sizes {
            assert!(*size > PROPERTY_ENTRY_HEADER_SIZE);
        }
    }

    #[test]
    fn test_entry_size() {
        assert_eq!(ArenaAllocator::entry_size(0), PROPERTY_ENTRY_HEADER_SIZE);
        assert_eq!(
            ArenaAllocator::entry_size(10),
            PROPERTY_ENTRY_HEADER_SIZE + 10
        );
        assert_eq!(
            ArenaAllocator::entry_size(100),
            PROPERTY_ENTRY_HEADER_SIZE + 100
        );
    }

    #[test]
    fn test_serialize_all_value_types() {
        let mut props = HashMap::new();
        props.insert("null".to_string(), Value::Null);
        props.insert("bool_true".to_string(), Value::Bool(true));
        props.insert("bool_false".to_string(), Value::Bool(false));
        props.insert("int".to_string(), Value::Int(42));
        props.insert("float".to_string(), Value::Float(3.14));
        props.insert("string".to_string(), Value::String("hello".to_string()));

        let (data, next_offsets) = serialize_properties(&props, |_| 0);

        assert_eq!(next_offsets.len(), 6);
        assert!(!data.is_empty());
    }
}
