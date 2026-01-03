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
//! use rustgremlin::storage::StringInterner;
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

use std::collections::HashMap;

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
/// use rustgremlin::storage::StringInterner;
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
    /// use rustgremlin::storage::StringInterner;
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
    /// use rustgremlin::storage::StringInterner;
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
    /// use rustgremlin::storage::StringInterner;
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
    /// use rustgremlin::storage::StringInterner;
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
    /// use rustgremlin::storage::StringInterner;
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
    /// use rustgremlin::storage::StringInterner;
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
}
