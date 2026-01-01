mod label;

pub use label::LabelIndex;

use roaring::RoaringBitmap;

/// Trait for label-based indexing
pub trait LabelIndexTrait {
    /// Add an element ID to the index for the given label
    fn add(&mut self, label_id: u32, element_id: u64);

    /// Remove an element ID from the index
    fn remove(&mut self, label_id: u32, element_id: u64);

    /// Get all element IDs for a label
    fn get(&self, label_id: u32) -> Option<&RoaringBitmap>;

    /// Check if an element exists for a label
    fn contains(&self, label_id: u32, element_id: u64) -> bool;

    /// Count elements for a label
    fn count(&self, label_id: u32) -> u64;
}
