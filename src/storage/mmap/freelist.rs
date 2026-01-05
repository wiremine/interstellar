//! Free list management for deleted node/edge slot reuse.
//!
//! Tracks deleted slots that can be reused for new elements, avoiding fragmentation
//! and enabling efficient slot allocation.
//!
//! # Design
//!
//! The free list is an in-memory linked list of slot IDs. When a node or edge is
//! deleted, its slot ID is added to the free list. When allocating a new slot,
//! we first check the free list for a reusable slot before extending the table.
//!
//! The free list head is persisted in the file header (`free_node_head` for nodes,
//! with edge free list tracking to be added in future phases if needed).
//!
//! # Linked List Structure
//!
//! On disk, deleted records store the "next free" pointer in a repurposed field:
//! - For nodes: `first_out_edge` field stores the next free node ID
//! - For edges: `next_out` field stores the next free edge ID
//!
//! The end of the list is marked by `u64::MAX`.
//!
//! # Example
//!
//! ```text
//! Initial state:
//!   free_list.head = u64::MAX (empty)
//!
//! After deleting slots 3, 7, 5 (in that order):
//!   free_list.head = 5
//!   slot 5 -> next = 7
//!   slot 7 -> next = 3
//!   slot 3 -> next = u64::MAX (end)
//!
//! After allocating:
//!   Returns slot 5 (head)
//!   free_list.head = 7
//! ```

/// Free list for managing deleted slots in node/edge tables.
///
/// This structure tracks available slots from deleted elements, allowing
/// efficient reuse without table compaction.
///
/// # Thread Safety
///
/// This structure is not thread-safe on its own. Callers must ensure proper
/// synchronization (typically via `RwLock<FreeList>`).
#[derive(Debug, Clone)]
pub struct FreeList {
    /// First free slot ID, or `u64::MAX` if the list is empty.
    ///
    /// This value should be persisted to the file header on modification.
    head: u64,

    /// Tracks the next free slot for each freed slot.
    ///
    /// This is an in-memory representation of the on-disk linked list.
    /// When a slot is freed, we store its "next" pointer here.
    /// When a slot is allocated, we remove it from this map.
    ///
    /// Note: This design keeps the linked list structure in memory to avoid
    /// requiring disk reads during allocation. The disk representation uses
    /// the same linked list (stored in repurposed record fields), but we
    /// maintain this parallel structure for fast access.
    next_pointers: hashbrown::HashMap<u64, u64>,
}

impl FreeList {
    /// Create a new empty free list.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::mmap::freelist::FreeList;
    ///
    /// let free_list = FreeList::new();
    /// assert!(free_list.is_empty());
    /// ```
    pub fn new() -> Self {
        Self {
            head: u64::MAX,
            next_pointers: hashbrown::HashMap::new(),
        }
    }

    /// Create a free list with an existing head.
    ///
    /// This is used when loading from a persisted header. The caller must
    /// also rebuild the `next_pointers` map by scanning the deleted records.
    ///
    /// # Arguments
    ///
    /// * `head` - The first free slot ID from the file header
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::mmap::freelist::FreeList;
    ///
    /// // Load from header with head = 42
    /// let free_list = FreeList::with_head(42);
    /// assert!(!free_list.is_empty());
    /// assert_eq!(free_list.head(), 42);
    /// ```
    pub fn with_head(head: u64) -> Self {
        Self {
            head,
            next_pointers: hashbrown::HashMap::new(),
        }
    }

    /// Check if the free list is empty.
    ///
    /// Returns `true` if there are no free slots available.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::mmap::freelist::FreeList;
    ///
    /// let mut free_list = FreeList::new();
    /// assert!(free_list.is_empty());
    ///
    /// free_list.free(5);
    /// assert!(!free_list.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head == u64::MAX
    }

    /// Get the current head of the free list.
    ///
    /// Returns `u64::MAX` if the list is empty.
    #[inline]
    pub fn head(&self) -> u64 {
        self.head
    }

    /// Allocate a slot from the free list or request table extension.
    ///
    /// If the free list has available slots, returns a reused slot ID.
    /// Otherwise, returns `current_count` to indicate that the table should
    /// be extended (the new slot will be at index `current_count`).
    ///
    /// # Arguments
    ///
    /// * `current_count` - The current number of active (non-deleted) elements.
    ///   This is used as the new slot ID if no free slots are available.
    ///
    /// # Returns
    ///
    /// A slot ID to use for the new element. If this equals `current_count`,
    /// the caller may need to grow the table if `current_count >= capacity`.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::mmap::freelist::FreeList;
    ///
    /// let mut free_list = FreeList::new();
    ///
    /// // Empty list - allocate extends table
    /// let slot = free_list.allocate(0);
    /// assert_eq!(slot, 0); // Returns current_count
    ///
    /// // Free a slot and reallocate
    /// free_list.free(0);
    /// let slot = free_list.allocate(1); // current_count is now 1
    /// assert_eq!(slot, 0); // Reuses freed slot
    /// ```
    pub fn allocate(&mut self, current_count: u64) -> u64 {
        if self.head != u64::MAX {
            // Reuse a deleted slot
            let slot_id = self.head;

            // Update head to point to the next free slot
            self.head = self.next_pointers.remove(&slot_id).unwrap_or(u64::MAX);

            slot_id
        } else {
            // No free slots - extend table by using the next sequential ID
            current_count
        }
    }

    /// Free a slot, adding it to the free list.
    ///
    /// The freed slot becomes the new head of the free list, making it
    /// the first slot to be reused on the next allocation.
    ///
    /// # Arguments
    ///
    /// * `slot_id` - The ID of the slot being freed
    ///
    /// # Panics
    ///
    /// Does not panic, but freeing the same slot twice will cause corruption.
    /// Callers should ensure slots are only freed once.
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::mmap::freelist::FreeList;
    ///
    /// let mut free_list = FreeList::new();
    /// assert!(free_list.is_empty());
    ///
    /// free_list.free(5);
    /// assert!(!free_list.is_empty());
    /// assert_eq!(free_list.head(), 5);
    ///
    /// free_list.free(3);
    /// assert_eq!(free_list.head(), 3); // Most recently freed is head
    /// ```
    pub fn free(&mut self, slot_id: u64) {
        // The freed slot points to the current head
        self.next_pointers.insert(slot_id, self.head);

        // The freed slot becomes the new head
        self.head = slot_id;
    }

    /// Get the next free slot ID after the given slot.
    ///
    /// This is useful for debugging and testing the linked list structure.
    ///
    /// # Arguments
    ///
    /// * `slot_id` - The slot ID to query
    ///
    /// # Returns
    ///
    /// The next free slot ID, or `None` if the slot is not in the free list.
    #[inline]
    pub fn next(&self, slot_id: u64) -> Option<u64> {
        self.next_pointers.get(&slot_id).copied()
    }

    /// Get the number of free slots in the list.
    ///
    /// This is an O(1) operation as we track pointers in a HashMap.
    ///
    /// # Returns
    ///
    /// The number of slots currently in the free list.
    #[inline]
    pub fn len(&self) -> usize {
        if self.head == u64::MAX {
            0
        } else {
            // The head is always in the list, plus all entries in next_pointers
            // But actually next_pointers.len() == number of items in list
            // because each freed slot has an entry pointing to its successor
            self.next_pointers.len()
        }
    }

    /// Clear the free list, removing all free slots.
    ///
    /// This does not affect the actual records on disk.
    pub fn clear(&mut self) {
        self.head = u64::MAX;
        self.next_pointers.clear();
    }

    /// Rebuild the free list by providing the linked list structure.
    ///
    /// This is used when loading a database to reconstruct the in-memory
    /// representation from the on-disk linked list.
    ///
    /// # Arguments
    ///
    /// * `head` - The first free slot ID
    /// * `links` - Iterator of (slot_id, next_slot_id) pairs representing the linked list
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::mmap::freelist::FreeList;
    ///
    /// let mut free_list = FreeList::new();
    ///
    /// // Rebuild from on-disk structure: 5 -> 7 -> 3 -> MAX
    /// let links = vec![(5u64, 7u64), (7, 3), (3, u64::MAX)];
    /// free_list.rebuild(5, links.into_iter());
    ///
    /// assert_eq!(free_list.head(), 5);
    /// assert_eq!(free_list.next(5), Some(7));
    /// assert_eq!(free_list.next(7), Some(3));
    /// assert_eq!(free_list.next(3), Some(u64::MAX));
    /// ```
    pub fn rebuild(&mut self, head: u64, links: impl Iterator<Item = (u64, u64)>) {
        self.head = head;
        self.next_pointers.clear();

        for (slot_id, next_id) in links {
            self.next_pointers.insert(slot_id, next_id);
        }
    }

    /// Iterate over all free slot IDs in order (head to tail).
    ///
    /// # Example
    ///
    /// ```
    /// use rustgremlin::storage::mmap::freelist::FreeList;
    ///
    /// let mut free_list = FreeList::new();
    /// free_list.free(3);
    /// free_list.free(7);
    /// free_list.free(5);
    ///
    /// let slots: Vec<u64> = free_list.iter().collect();
    /// assert_eq!(slots, vec![5, 7, 3]); // LIFO order
    /// ```
    pub fn iter(&self) -> FreeListIter<'_> {
        FreeListIter {
            free_list: self,
            current: self.head,
        }
    }
}

impl Default for FreeList {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator over free slot IDs in the free list.
pub struct FreeListIter<'a> {
    free_list: &'a FreeList,
    current: u64,
}

impl<'a> Iterator for FreeListIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == u64::MAX {
            return None;
        }

        let slot_id = self.current;
        self.current = self
            .free_list
            .next_pointers
            .get(&slot_id)
            .copied()
            .unwrap_or(u64::MAX);
        Some(slot_id)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_free_list_is_empty() {
        let free_list = FreeList::new();
        assert!(free_list.is_empty());
        assert_eq!(free_list.head(), u64::MAX);
        assert_eq!(free_list.len(), 0);
    }

    #[test]
    fn test_with_head() {
        let free_list = FreeList::with_head(42);
        assert!(!free_list.is_empty());
        assert_eq!(free_list.head(), 42);
    }

    #[test]
    fn test_with_head_max_is_empty() {
        let free_list = FreeList::with_head(u64::MAX);
        assert!(free_list.is_empty());
    }

    #[test]
    fn test_allocate_empty_list_returns_current_count() {
        let mut free_list = FreeList::new();

        // With empty list, allocate returns current_count
        assert_eq!(free_list.allocate(0), 0);
        assert_eq!(free_list.allocate(5), 5);
        assert_eq!(free_list.allocate(100), 100);

        // List remains empty after these "allocations"
        assert!(free_list.is_empty());
    }

    #[test]
    fn test_free_adds_to_list() {
        let mut free_list = FreeList::new();

        free_list.free(5);
        assert!(!free_list.is_empty());
        assert_eq!(free_list.head(), 5);
        assert_eq!(free_list.len(), 1);
    }

    #[test]
    fn test_free_multiple_slots_lifo_order() {
        let mut free_list = FreeList::new();

        // Free slots in order: 3, 7, 5
        free_list.free(3);
        free_list.free(7);
        free_list.free(5);

        // Head should be the last freed (LIFO)
        assert_eq!(free_list.head(), 5);

        // Linked list should be: 5 -> 7 -> 3 -> MAX
        assert_eq!(free_list.next(5), Some(7));
        assert_eq!(free_list.next(7), Some(3));
        assert_eq!(free_list.next(3), Some(u64::MAX));

        assert_eq!(free_list.len(), 3);
    }

    #[test]
    fn test_allocate_reuses_freed_slot() {
        let mut free_list = FreeList::new();

        // Free slot 5
        free_list.free(5);
        assert_eq!(free_list.head(), 5);

        // Allocate should return the freed slot
        let slot = free_list.allocate(10); // current_count doesn't matter
        assert_eq!(slot, 5);

        // Free list should be empty now
        assert!(free_list.is_empty());
        assert_eq!(free_list.head(), u64::MAX);
    }

    #[test]
    fn test_allocate_reuses_slots_in_lifo_order() {
        let mut free_list = FreeList::new();

        // Free slots: 3, 7, 5 (5 is last freed)
        free_list.free(3);
        free_list.free(7);
        free_list.free(5);

        // Allocations should return in LIFO order: 5, 7, 3
        assert_eq!(free_list.allocate(100), 5);
        assert_eq!(free_list.allocate(100), 7);
        assert_eq!(free_list.allocate(100), 3);

        // Now empty
        assert!(free_list.is_empty());
        assert_eq!(free_list.allocate(100), 100); // Falls back to current_count
    }

    #[test]
    fn test_multiple_allocate_free_cycles() {
        let mut free_list = FreeList::new();

        // Cycle 1: Free and allocate
        free_list.free(10);
        assert_eq!(free_list.allocate(5), 10);
        assert!(free_list.is_empty());

        // Cycle 2: Free multiple, allocate some
        free_list.free(20);
        free_list.free(30);
        assert_eq!(free_list.allocate(5), 30);
        assert_eq!(free_list.head(), 20);

        // Cycle 3: Free more, interleave allocations
        free_list.free(40);
        assert_eq!(free_list.head(), 40);
        assert_eq!(free_list.allocate(5), 40);
        assert_eq!(free_list.allocate(5), 20);
        assert!(free_list.is_empty());
    }

    #[test]
    fn test_free_after_allocate() {
        let mut free_list = FreeList::new();

        // Free slot 5, allocate it back, then free it again
        free_list.free(5);
        let slot = free_list.allocate(10);
        assert_eq!(slot, 5);
        assert!(free_list.is_empty());

        // Free slot 5 again (simulating re-deletion)
        free_list.free(5);
        assert_eq!(free_list.head(), 5);
        assert!(!free_list.is_empty());
    }

    #[test]
    fn test_clear() {
        let mut free_list = FreeList::new();

        free_list.free(1);
        free_list.free(2);
        free_list.free(3);
        assert!(!free_list.is_empty());
        assert_eq!(free_list.len(), 3);

        free_list.clear();
        assert!(free_list.is_empty());
        assert_eq!(free_list.head(), u64::MAX);
        assert_eq!(free_list.len(), 0);
    }

    #[test]
    fn test_rebuild() {
        let mut free_list = FreeList::new();

        // Rebuild from on-disk structure: 5 -> 7 -> 3 -> MAX
        let links = vec![(5u64, 7u64), (7, 3), (3, u64::MAX)];
        free_list.rebuild(5, links.into_iter());

        assert_eq!(free_list.head(), 5);
        assert_eq!(free_list.next(5), Some(7));
        assert_eq!(free_list.next(7), Some(3));
        assert_eq!(free_list.next(3), Some(u64::MAX));
        assert_eq!(free_list.len(), 3);

        // Allocations should work correctly
        assert_eq!(free_list.allocate(100), 5);
        assert_eq!(free_list.allocate(100), 7);
        assert_eq!(free_list.allocate(100), 3);
        assert!(free_list.is_empty());
    }

    #[test]
    fn test_rebuild_empty() {
        let mut free_list = FreeList::new();

        // Start with some data
        free_list.free(1);
        free_list.free(2);

        // Rebuild with empty list
        free_list.rebuild(u64::MAX, std::iter::empty());
        assert!(free_list.is_empty());
        assert_eq!(free_list.len(), 0);
    }

    #[test]
    fn test_iter() {
        let mut free_list = FreeList::new();

        free_list.free(3);
        free_list.free(7);
        free_list.free(5);

        let slots: Vec<u64> = free_list.iter().collect();
        assert_eq!(slots, vec![5, 7, 3]); // LIFO order
    }

    #[test]
    fn test_iter_empty() {
        let free_list = FreeList::new();
        let slots: Vec<u64> = free_list.iter().collect();
        assert!(slots.is_empty());
    }

    #[test]
    fn test_default() {
        let free_list = FreeList::default();
        assert!(free_list.is_empty());
        assert_eq!(free_list.head(), u64::MAX);
    }

    #[test]
    fn test_clone() {
        let mut free_list = FreeList::new();
        free_list.free(1);
        free_list.free(2);
        free_list.free(3);

        let cloned = free_list.clone();

        // Both should have same state
        assert_eq!(free_list.head(), cloned.head());
        assert_eq!(free_list.len(), cloned.len());

        // Modifying original shouldn't affect clone
        free_list.allocate(10);
        assert_ne!(free_list.head(), cloned.head());
    }

    #[test]
    fn test_large_number_of_slots() {
        let mut free_list = FreeList::new();

        // Free 1000 slots
        for i in 0..1000 {
            free_list.free(i);
        }

        assert_eq!(free_list.len(), 1000);
        assert_eq!(free_list.head(), 999); // Last freed is head

        // Allocate them all back
        for i in (0..1000).rev() {
            assert_eq!(free_list.allocate(2000), i);
        }

        assert!(free_list.is_empty());
    }

    #[test]
    fn test_non_sequential_slot_ids() {
        let mut free_list = FreeList::new();

        // Free non-sequential slots
        free_list.free(1000);
        free_list.free(5);
        free_list.free(999999);
        free_list.free(42);

        // Allocate in LIFO order
        assert_eq!(free_list.allocate(0), 42);
        assert_eq!(free_list.allocate(0), 999999);
        assert_eq!(free_list.allocate(0), 5);
        assert_eq!(free_list.allocate(0), 1000);
        assert!(free_list.is_empty());
    }

    #[test]
    fn test_next_returns_none_for_non_free_slot() {
        let mut free_list = FreeList::new();
        free_list.free(5);

        // Slot 5 is in the list
        assert!(free_list.next(5).is_some());

        // Slot 10 is not in the list
        assert!(free_list.next(10).is_none());
    }

    #[test]
    fn test_next_returns_max_for_tail() {
        let mut free_list = FreeList::new();
        free_list.free(3); // This is the tail (first freed)
        free_list.free(5); // This becomes head

        // Tail should point to MAX
        assert_eq!(free_list.next(3), Some(u64::MAX));

        // Head should point to tail
        assert_eq!(free_list.next(5), Some(3));
    }
}
