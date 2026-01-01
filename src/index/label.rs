use std::collections::HashMap;

use roaring::RoaringBitmap;

use super::LabelIndexTrait;

/// Label index using RoaringBitmap for efficient set operations
///
/// Maps label_id -> set of element IDs (as u32, fitting RoaringBitmap)
///
/// ## Complexity
/// - add: O(1) amortized
/// - remove: O(1)
/// - get: O(1)
/// - contains: O(1)
/// - count: O(1)
/// - iteration: O(n) where n = elements with label
pub struct LabelIndex {
    /// label_id -> bitmap of element IDs
    index: HashMap<u32, RoaringBitmap>,
}

impl LabelIndex {
    /// Create a new empty label index
    pub fn new() -> Self {
        Self {
            index: HashMap::new(),
        }
    }

    /// Get iterator over all (label_id, bitmap) pairs
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &RoaringBitmap)> {
        self.index.iter()
    }

    /// Clear all index entries
    pub fn clear(&mut self) {
        self.index.clear();
    }
}

impl Default for LabelIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl LabelIndexTrait for LabelIndex {
    fn add(&mut self, label_id: u32, element_id: u64) {
        self.index
            .entry(label_id)
            .or_default()
            .insert(element_id as u32);
    }

    fn remove(&mut self, label_id: u32, element_id: u64) {
        if let Some(bitmap) = self.index.get_mut(&label_id) {
            bitmap.remove(element_id as u32);

            // Optionally remove empty bitmaps to save memory
            // if bitmap.is_empty() {
            //     self.index.remove(&label_id);
            // }
        }
    }

    fn get(&self, label_id: u32) -> Option<&RoaringBitmap> {
        self.index.get(&label_id)
    }

    fn contains(&self, label_id: u32, element_id: u64) -> bool {
        self.index
            .get(&label_id)
            .map(|b| b.contains(element_id as u32))
            .unwrap_or(false)
    }

    fn count(&self, label_id: u32) -> u64 {
        self.index.get(&label_id).map(|b| b.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_lookup() {
        let mut index = LabelIndex::new();
        index.add(1, 100);
        index.add(1, 200);
        index.add(2, 100);

        assert!(index.contains(1, 100));
        assert!(index.contains(1, 200));
        assert!(index.contains(2, 100));
        assert!(!index.contains(2, 200));

        assert_eq!(index.count(1), 2);
        assert_eq!(index.count(2), 1);
        assert_eq!(index.count(3), 0);
    }

    #[test]
    fn remove_element() {
        let mut index = LabelIndex::new();
        index.add(1, 100);
        index.add(1, 200);

        index.remove(1, 100);

        assert!(!index.contains(1, 100));
        assert!(index.contains(1, 200));
        assert_eq!(index.count(1), 1);
    }

    #[test]
    fn get_bitmap_iteration() {
        let mut index = LabelIndex::new();
        index.add(1, 100);
        index.add(1, 200);
        index.add(1, 300);

        let bitmap = index.get(1).unwrap();
        let ids: Vec<u32> = bitmap.iter().collect();

        assert_eq!(ids, vec![100, 200, 300]);
    }
}
