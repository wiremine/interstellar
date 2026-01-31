//! String interning for efficient label storage.
//!
//! This module provides [`StringInterner`], a bidirectional mapping between
//! strings and compact integer IDs. It's used internally to store vertex and
//! edge labels efficiently.
//!
//! # Why String Interning?
//!
//! Graph labels (like "person", "knows") are repeated many times across
//! vertices and edges. Instead of storing the full string for each element:
//!
//! - Each unique string is stored once
//! - Elements store a compact `u32` ID instead of the string
//! - Comparisons become integer comparisons (fast)
//! - Memory usage is reduced for graphs with many repeated labels
//!
//! # Example
//!
//! ```
//! use interstellar::storage::StringInterner;
//!
//! let mut interner = StringInterner::new();
//!
//! // First intern assigns ID 0
//! let id1 = interner.intern("person");
//! assert_eq!(id1, 0);
//!
//! // Same string returns same ID
//! let id2 = interner.intern("person");
//! assert_eq!(id1, id2);
//!
//! // Different string gets new ID
//! let id3 = interner.intern("software");
//! assert_eq!(id3, 1);
//!
//! // Resolve ID back to string
//! assert_eq!(interner.resolve(id1), Some("person"));
//! ```

use crate::error::StorageError;
use std::collections::HashMap;
use std::io::Write;

/// A bidirectional string interner for efficient label storage.
///
/// Maps strings to compact `u32` IDs and vice versa. IDs are assigned
/// sequentially starting from 0. Each unique string is stored exactly once.
///
/// # Thread Safety
///
/// `StringInterner` is `Send + Sync` when used with external synchronization.
/// Mutation requires `&mut self`, so concurrent reads are safe but writes
/// need exclusive access.
///
/// # Capacity
///
/// The interner can store up to 2^32 unique strings. Attempting to intern
/// more will panic with "string interner id overflow".
///
/// # Example
///
/// ```
/// use interstellar::storage::StringInterner;
///
/// let mut interner = StringInterner::new();
///
/// // Intern some labels
/// let person_id = interner.intern("person");
/// let knows_id = interner.intern("knows");
///
/// // Read-only lookup (doesn't intern)
/// assert_eq!(interner.lookup("person"), Some(person_id));
/// assert_eq!(interner.lookup("missing"), None);
///
/// // Resolve back to strings
/// assert_eq!(interner.resolve(person_id), Some("person"));
/// assert_eq!(interner.resolve(knows_id), Some("knows"));
/// ```
#[derive(Debug, Clone)]
pub struct StringInterner {
    forward: HashMap<String, u32>,
    reverse: HashMap<u32, String>,
    next_id: u32,
}

impl StringInterner {
    /// Creates a new empty string interner.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::StringInterner;
    ///
    /// let interner = StringInterner::new();
    /// assert!(interner.is_empty());
    /// ```
    pub fn new() -> Self {
        StringInterner {
            forward: HashMap::new(),
            reverse: HashMap::new(),
            next_id: 0,
        }
    }

    /// Interns a string, returning its ID.
    ///
    /// If the string has already been interned, returns its existing ID.
    /// Otherwise, assigns a new ID and stores the string.
    ///
    /// # Arguments
    ///
    /// * `value` - The string to intern
    ///
    /// # Returns
    ///
    /// The `u32` ID for this string.
    ///
    /// # Panics
    ///
    /// Panics if more than 2^32 unique strings are interned.
    ///
    /// # Complexity
    ///
    /// O(1) amortized.
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::StringInterner;
    ///
    /// let mut interner = StringInterner::new();
    ///
    /// let id1 = interner.intern("hello");
    /// let id2 = interner.intern("hello");  // Same string
    /// let id3 = interner.intern("world");  // Different string
    ///
    /// assert_eq!(id1, id2);  // Same ID
    /// assert_ne!(id1, id3);  // Different IDs
    /// ```
    pub fn intern(&mut self, value: &str) -> u32 {
        if let Some(id) = self.forward.get(value) {
            return *id;
        }
        let id = self.next_id;
        self.forward.insert(value.to_owned(), id);
        self.reverse.insert(id, value.to_owned());
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("string interner id overflow");
        id
    }

    /// Resolves an ID back to its string.
    ///
    /// Returns `None` if the ID hasn't been assigned.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID to resolve
    ///
    /// # Returns
    ///
    /// The string for this ID, or `None` if the ID is invalid.
    ///
    /// # Complexity
    ///
    /// O(1).
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::StringInterner;
    ///
    /// let mut interner = StringInterner::new();
    /// let id = interner.intern("greeting");
    ///
    /// assert_eq!(interner.resolve(id), Some("greeting"));
    /// assert_eq!(interner.resolve(999), None);
    /// ```
    pub fn resolve(&self, id: u32) -> Option<&str> {
        self.reverse.get(&id).map(|s| s.as_str())
    }

    /// Looks up a string's ID without interning it.
    ///
    /// This is a read-only operation that won't modify the interner.
    /// Use this when you need to check if a string has been interned
    /// without potentially adding it.
    ///
    /// # Arguments
    ///
    /// * `value` - The string to look up
    ///
    /// # Returns
    ///
    /// The ID if the string has been interned, or `None` otherwise.
    ///
    /// # Complexity
    ///
    /// O(1).
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::StringInterner;
    ///
    /// let mut interner = StringInterner::new();
    /// interner.intern("exists");
    ///
    /// // lookup doesn't modify the interner
    /// assert_eq!(interner.lookup("exists"), Some(0));
    /// assert_eq!(interner.lookup("missing"), None);
    /// assert_eq!(interner.len(), 1);  // Still only 1 string
    /// ```
    pub fn lookup(&self, value: &str) -> Option<u32> {
        self.forward.get(value).copied()
    }

    /// Returns the number of interned strings.
    ///
    /// # Complexity
    ///
    /// O(1).
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::StringInterner;
    ///
    /// let mut interner = StringInterner::new();
    /// assert_eq!(interner.len(), 0);
    ///
    /// interner.intern("one");
    /// interner.intern("two");
    /// interner.intern("one");  // Duplicate, not counted
    ///
    /// assert_eq!(interner.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        self.forward.len()
    }

    /// Returns `true` if no strings have been interned.
    ///
    /// # Complexity
    ///
    /// O(1).
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::StringInterner;
    ///
    /// let mut interner = StringInterner::new();
    /// assert!(interner.is_empty());
    ///
    /// interner.intern("hello");
    /// assert!(!interner.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }

    /// Loads string table from memory-mapped file.
    ///
    /// Reads string entries from the memory-mapped region starting at the
    /// given offset. Each entry consists of an 8-byte header (u32 id, u32 len)
    /// followed by the UTF-8 string bytes.
    ///
    /// # Arguments
    ///
    /// * `mmap` - The memory-mapped file contents
    /// * `offset` - Byte offset to start of string table
    /// * `end` - Byte offset to end of string table (exclusive)
    ///
    /// # Returns
    ///
    /// A new `StringInterner` populated with all strings from the table.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::CorruptedData` if:
    /// - String table extends beyond `end` offset
    /// - String data is not valid UTF-8
    /// - Entry header is incomplete
    ///
    /// # Example
    ///
    /// ```
    /// use interstellar::storage::StringInterner;
    ///
    /// // Load string table from a byte slice (e.g., from memory-mapped file)
    /// let data = vec![
    ///     0, 0, 0, 0,  // id = 0
    ///     6, 0, 0, 0,  // len = 6
    ///     b'p', b'e', b'r', b's', b'o', b'n',  // "person"
    /// ];
    ///
    /// let interner = StringInterner::load_from_mmap(&data, 0, data.len() as u64).unwrap();
    /// assert_eq!(interner.resolve(0), Some("person"));
    /// ```
    pub fn load_from_mmap(mmap: &[u8], offset: u64, end: u64) -> Result<Self, StorageError> {
        let mut interner = StringInterner::new();
        let mut pos = offset as usize;
        let end_pos = end as usize;

        if end_pos > mmap.len() {
            return Err(StorageError::CorruptedData);
        }

        while pos < end_pos {
            // Read string entry header (8 bytes: u32 id + u32 len)
            if pos + 8 > end_pos {
                return Err(StorageError::CorruptedData);
            }

            let id = u32::from_le_bytes([mmap[pos], mmap[pos + 1], mmap[pos + 2], mmap[pos + 3]]);
            let len =
                u32::from_le_bytes([mmap[pos + 4], mmap[pos + 5], mmap[pos + 6], mmap[pos + 7]])
                    as usize;
            pos += 8;

            // Read string bytes
            if pos + len > end_pos {
                return Err(StorageError::CorruptedData);
            }

            let string_bytes = &mmap[pos..pos + len];
            let string = std::str::from_utf8(string_bytes)
                .map_err(|_| StorageError::CorruptedData)?
                .to_string();
            pos += len;

            // Insert into interner
            interner.forward.insert(string.clone(), id);
            interner.reverse.insert(id, string);

            // Update next_id to be one past the highest ID seen
            if id >= interner.next_id {
                interner.next_id = id + 1;
            }
        }

        Ok(interner)
    }

    /// Writes string table to a file.
    ///
    /// Serializes all interned strings to the file in ID order. Each entry
    /// consists of an 8-byte header (u32 id, u32 len) followed by the UTF-8
    /// string bytes.
    ///
    /// # Arguments
    ///
    /// * `file` - The file to write to (must be opened with write permissions)
    ///
    /// # Returns
    ///
    /// The total number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::Io` if any write operation fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use interstellar::storage::StringInterner;
    /// use std::fs::OpenOptions;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut interner = StringInterner::new();
    /// interner.intern("person");
    /// interner.intern("knows");
    ///
    /// let mut file = OpenOptions::new()
    ///     .create(true)
    ///     .write(true)
    ///     .open("strings.dat")?;
    ///
    /// let bytes_written = interner.write_to_file(&mut file)?;
    /// println!("Wrote {} bytes", bytes_written);
    /// # Ok(())
    /// # }
    /// ```
    pub fn write_to_file<W: Write>(&self, writer: &mut W) -> Result<u64, StorageError> {
        let mut total_bytes = 0u64;

        // Collect and sort entries by ID for deterministic output
        let mut entries: Vec<_> = self.reverse.iter().collect();
        entries.sort_by_key(|(id, _)| *id);

        for (id, string) in entries {
            let string_bytes = string.as_bytes();
            let len = string_bytes.len() as u32;

            // Write header (8 bytes: u32 id + u32 len)
            writer.write_all(&id.to_le_bytes())?;
            writer.write_all(&len.to_le_bytes())?;
            total_bytes += 8;

            // Write string bytes
            writer.write_all(string_bytes)?;
            total_bytes += len as u64;
        }

        Ok(total_bytes)
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interns_and_resolves_strings() {
        let mut interner = StringInterner::new();
        let first = interner.intern("person");
        let second = interner.intern("person");
        let third = interner.intern("knows");

        assert_eq!(first, second);
        assert_ne!(first, third);
        assert_eq!(interner.resolve(first), Some("person"));
        assert_eq!(interner.resolve(third), Some("knows"));
    }

    #[test]
    fn lookup_without_interning() {
        let mut interner = StringInterner::new();
        interner.intern("exists");

        assert_eq!(interner.lookup("exists"), Some(0));
        assert_eq!(interner.lookup("missing"), None);
    }

    #[test]
    fn len_and_is_empty() {
        let mut interner = StringInterner::new();
        assert!(interner.is_empty());
        assert_eq!(interner.len(), 0);

        interner.intern("one");
        interner.intern("two");
        interner.intern("one"); // duplicate

        assert!(!interner.is_empty());
        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn test_write_and_load_empty_string_table() {
        let interner = StringInterner::new();
        let mut buffer = Vec::new();

        let bytes_written = interner.write_to_file(&mut buffer).unwrap();
        assert_eq!(bytes_written, 0);
        assert_eq!(buffer.len(), 0);

        let loaded = StringInterner::load_from_mmap(&buffer, 0, 0).unwrap();
        assert!(loaded.is_empty());
        assert_eq!(loaded.len(), 0);
    }

    #[test]
    fn test_write_and_load_single_string() {
        let mut interner = StringInterner::new();
        interner.intern("person");

        let mut buffer = Vec::new();
        let bytes_written = interner.write_to_file(&mut buffer).unwrap();

        // Header (8 bytes) + string (6 bytes) = 14 bytes
        assert_eq!(bytes_written, 14);
        assert_eq!(buffer.len(), 14);

        let loaded = StringInterner::load_from_mmap(&buffer, 0, buffer.len() as u64).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded.resolve(0), Some("person"));
        assert_eq!(loaded.lookup("person"), Some(0));
    }

    #[test]
    fn test_write_and_load_multiple_strings() {
        let mut interner = StringInterner::new();
        let id1 = interner.intern("person");
        let id2 = interner.intern("software");
        let id3 = interner.intern("knows");

        let mut buffer = Vec::new();
        interner.write_to_file(&mut buffer).unwrap();

        let loaded = StringInterner::load_from_mmap(&buffer, 0, buffer.len() as u64).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.resolve(id1), Some("person"));
        assert_eq!(loaded.resolve(id2), Some("software"));
        assert_eq!(loaded.resolve(id3), Some("knows"));
        assert_eq!(loaded.lookup("person"), Some(id1));
        assert_eq!(loaded.lookup("software"), Some(id2));
        assert_eq!(loaded.lookup("knows"), Some(id3));
    }

    #[test]
    fn test_write_preserves_id_order() {
        let mut interner = StringInterner::new();
        interner.intern("zzz"); // ID 0
        interner.intern("aaa"); // ID 1
        interner.intern("mmm"); // ID 2

        let mut buffer = Vec::new();
        interner.write_to_file(&mut buffer).unwrap();

        let loaded = StringInterner::load_from_mmap(&buffer, 0, buffer.len() as u64).unwrap();
        assert_eq!(loaded.resolve(0), Some("zzz"));
        assert_eq!(loaded.resolve(1), Some("aaa"));
        assert_eq!(loaded.resolve(2), Some("mmm"));
    }

    #[test]
    fn test_load_from_offset() {
        let mut interner = StringInterner::new();
        interner.intern("first");
        interner.intern("second");

        let mut buffer = vec![0u8; 100]; // Prefix with 100 bytes
        interner.write_to_file(&mut buffer).unwrap();

        let offset = 100u64;
        let end = buffer.len() as u64;
        let loaded = StringInterner::load_from_mmap(&buffer, offset, end).unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded.resolve(0), Some("first"));
        assert_eq!(loaded.resolve(1), Some("second"));
    }

    #[test]
    fn test_load_with_utf8_strings() {
        let mut interner = StringInterner::new();
        interner.intern("hello");
        interner.intern("世界"); // UTF-8 multibyte
        interner.intern("🦀"); // UTF-8 emoji

        let mut buffer = Vec::new();
        interner.write_to_file(&mut buffer).unwrap();

        let loaded = StringInterner::load_from_mmap(&buffer, 0, buffer.len() as u64).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.resolve(0), Some("hello"));
        assert_eq!(loaded.resolve(1), Some("世界"));
        assert_eq!(loaded.resolve(2), Some("🦀"));
    }

    #[test]
    fn test_load_corrupted_truncated_header() {
        let mut interner = StringInterner::new();
        interner.intern("test");

        let mut buffer = Vec::new();
        interner.write_to_file(&mut buffer).unwrap();

        // Truncate to only 4 bytes (incomplete header)
        buffer.truncate(4);

        let result = StringInterner::load_from_mmap(&buffer, 0, buffer.len() as u64);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::CorruptedData));
    }

    #[test]
    fn test_load_corrupted_truncated_string() {
        let mut interner = StringInterner::new();
        interner.intern("test");

        let mut buffer = Vec::new();
        interner.write_to_file(&mut buffer).unwrap();

        // Truncate to only header (missing string bytes)
        buffer.truncate(8);

        let result = StringInterner::load_from_mmap(&buffer, 0, buffer.len() as u64);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::CorruptedData));
    }

    #[test]
    fn test_load_corrupted_invalid_utf8() {
        // Manually create a buffer with invalid UTF-8
        let mut buffer = Vec::new();

        // Valid header: id=0, len=2
        buffer.extend_from_slice(&0u32.to_le_bytes());
        buffer.extend_from_slice(&2u32.to_le_bytes());

        // Invalid UTF-8 bytes
        buffer.push(0xFF);
        buffer.push(0xFF);

        let result = StringInterner::load_from_mmap(&buffer, 0, buffer.len() as u64);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::CorruptedData));
    }

    #[test]
    fn test_load_corrupted_offset_beyond_end() {
        let buffer = vec![0u8; 100];

        // Try to read beyond buffer
        let result = StringInterner::load_from_mmap(&buffer, 0, 200);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), StorageError::CorruptedData));
    }

    #[test]
    fn test_next_id_updated_after_load() {
        let mut interner = StringInterner::new();
        interner.intern("first"); // ID 0
        interner.intern("second"); // ID 1
        interner.intern("third"); // ID 2

        let mut buffer = Vec::new();
        interner.write_to_file(&mut buffer).unwrap();

        let mut loaded = StringInterner::load_from_mmap(&buffer, 0, buffer.len() as u64).unwrap();

        // Next ID should be 3
        let new_id = loaded.intern("fourth");
        assert_eq!(new_id, 3);
    }

    #[test]
    fn test_deduplication_after_load() {
        let mut interner = StringInterner::new();
        interner.intern("person");

        let mut buffer = Vec::new();
        interner.write_to_file(&mut buffer).unwrap();

        let mut loaded = StringInterner::load_from_mmap(&buffer, 0, buffer.len() as u64).unwrap();

        // Interning same string should return existing ID
        let id = loaded.intern("person");
        assert_eq!(id, 0);
        assert_eq!(loaded.len(), 1); // No new entry added
    }
}
