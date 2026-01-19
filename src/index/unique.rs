//! Unique index implementation.
//!
//! This module provides [`UniqueIndex`], a hash-based property index that
//! enforces uniqueness and provides O(1) exact match lookups.

use std::collections::HashMap;
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::index::error::IndexError;
use crate::index::spec::{IndexSpec, IndexType};
use crate::index::traits::{index_covers_filter, IndexFilter, IndexStatistics, PropertyIndex};
use crate::value::Value;

/// Hash-based unique index with O(1) lookup.
///
/// This index enforces a uniqueness constraint on the indexed property,
/// ensuring no two elements can have the same value. It provides O(1)
/// average-case lookup performance for exact matches.
///
/// # Use Cases
///
/// - Unique identifiers: email, username, UUID
/// - Foreign keys that must be unique
/// - Any property where duplicates are not allowed
///
/// # Uniqueness Constraint
///
/// When inserting a value that already exists for a different element,
/// the insert will fail with [`IndexError::DuplicateValue`].
///
/// # Range Queries
///
/// Unique indexes do not support efficient range queries. If you need
/// range queries, use [`BTreeIndex`](crate::index::BTreeIndex) instead.
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::index::{UniqueIndex, IndexBuilder};
///
/// let spec = IndexBuilder::vertex()
///     .label("user")
///     .property("email")
///     .unique()
///     .build()?;
///
/// let mut index = UniqueIndex::new(spec);
/// index.insert(Value::String("alice@example.com".into()), 1)?;
/// index.insert(Value::String("bob@example.com".into()), 2)?;
///
/// // This would fail with DuplicateValue error:
/// // index.insert(Value::String("alice@example.com".into()), 3)?;
///
/// // O(1) lookup
/// let ids: Vec<_> = index.lookup_eq(&Value::String("alice@example.com".into())).collect();
/// assert_eq!(ids, vec![1]);
/// ```
pub struct UniqueIndex {
    /// Index specification.
    spec: IndexSpec,

    /// Maps property values to single element IDs.
    /// Enforces uniqueness constraint.
    map: HashMap<Value, u64>,

    /// Reverse map for efficient removal by element ID.
    reverse: HashMap<u64, Value>,

    /// Index statistics.
    stats: IndexStatistics,
}

impl UniqueIndex {
    /// Create a new empty unique index.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::InvalidIndexType`] if the spec's index_type
    /// is not `IndexType::Unique`.
    pub fn new(spec: IndexSpec) -> Result<Self, IndexError> {
        if spec.index_type != IndexType::Unique {
            return Err(IndexError::InvalidIndexType {
                expected: IndexType::Unique,
                got: spec.index_type,
            });
        }
        Ok(Self {
            spec,
            map: HashMap::new(),
            reverse: HashMap::new(),
            stats: IndexStatistics::default(),
        })
    }

    /// Build index from an iterator of (element_id, property_value) pairs.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::DuplicateValue`] if any values are duplicated.
    pub fn populate<I>(&mut self, elements: I) -> Result<(), IndexError>
    where
        I: Iterator<Item = (u64, Value)>,
    {
        for (id, value) in elements {
            self.insert(value, id)?;
        }
        self.refresh_statistics();
        Ok(())
    }

    /// Get the number of indexed elements.
    pub fn count(&self) -> usize {
        self.map.len()
    }

    /// Check if a value exists in the index.
    pub fn contains(&self, value: &Value) -> bool {
        self.map.contains_key(value)
    }

    /// Get the element ID for a value, if it exists.
    pub fn get(&self, value: &Value) -> Option<u64> {
        self.map.get(value).copied()
    }
}

impl PropertyIndex for UniqueIndex {
    fn spec(&self) -> &IndexSpec {
        &self.spec
    }

    fn covers(&self, filter: &IndexFilter) -> bool {
        // UniqueIndex does not support range queries efficiently
        index_covers_filter(&self.spec, filter, false)
    }

    fn lookup_eq(&self, value: &Value) -> Box<dyn Iterator<Item = u64> + '_> {
        match self.map.get(value) {
            Some(&id) => Box::new(std::iter::once(id)),
            None => Box::new(std::iter::empty()),
        }
    }

    fn lookup_range(
        &self,
        _start: Bound<&Value>,
        _end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = u64> + '_> {
        // Unique indexes don't support efficient range queries.
        // Return empty - the query planner should not use this index for ranges.
        Box::new(std::iter::empty())
    }

    fn insert(&mut self, value: Value, element_id: u64) -> Result<(), IndexError> {
        // Check for duplicate
        if let Some(&existing_id) = self.map.get(&value) {
            if existing_id != element_id {
                return Err(IndexError::DuplicateValue {
                    index_name: self.spec.name.clone(),
                    value,
                    existing_id,
                    new_id: element_id,
                });
            }
            // Same element, same value - no-op
            return Ok(());
        }

        // Remove old value if this element already had a different value
        if let Some(old_value) = self.reverse.remove(&element_id) {
            self.map.remove(&old_value);
        }

        // Insert new mapping
        self.map.insert(value.clone(), element_id);
        self.reverse.insert(element_id, value);

        self.stats.total_elements = self.map.len() as u64;
        self.stats.cardinality = self.map.len() as u64;

        Ok(())
    }

    fn remove(&mut self, value: &Value, element_id: u64) -> Result<(), IndexError> {
        // Only remove if the value maps to this specific element
        if let Some(&stored_id) = self.map.get(value) {
            if stored_id == element_id {
                self.map.remove(value);
                self.reverse.remove(&element_id);

                self.stats.total_elements = self.map.len() as u64;
                self.stats.cardinality = self.map.len() as u64;
            }
        }

        Ok(())
    }

    fn update(
        &mut self,
        old_value: &Value,
        new_value: Value,
        element_id: u64,
    ) -> Result<(), IndexError> {
        // Early return if same value - nothing to do
        if old_value == &new_value {
            return Ok(());
        }

        // Check if new value would conflict with a different element
        if let Some(&existing_id) = self.map.get(&new_value) {
            if existing_id != element_id {
                return Err(IndexError::DuplicateValue {
                    index_name: self.spec.name.clone(),
                    value: new_value,
                    existing_id,
                    new_id: element_id,
                });
            }
            // new_value already maps to this element - still need to clean up old_value
        }

        // Remove old mapping if it exists for this element
        if self.map.get(old_value) == Some(&element_id) {
            self.map.remove(old_value);
        }

        // Insert new mapping (always, since we know old_value != new_value)
        self.map.insert(new_value.clone(), element_id);
        self.reverse.insert(element_id, new_value);

        Ok(())
    }

    fn statistics(&self) -> &IndexStatistics {
        &self.stats
    }

    fn refresh_statistics(&mut self) {
        self.stats.cardinality = self.map.len() as u64;
        self.stats.total_elements = self.map.len() as u64;
        self.stats.min_value = None; // No ordering for hash index
        self.stats.max_value = None;

        self.stats.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
    }

    fn clear(&mut self) {
        self.map.clear();
        self.reverse.clear();
        self.stats = IndexStatistics::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::spec::{ElementType, IndexBuilder, IndexPredicate};

    fn create_test_index() -> UniqueIndex {
        let spec = IndexBuilder::vertex()
            .label("user")
            .property("email")
            .unique()
            .build()
            .unwrap();
        UniqueIndex::new(spec).unwrap()
    }

    #[test]
    fn new_creates_empty_index() {
        let index = create_test_index();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert_eq!(index.count(), 0);
    }

    #[test]
    fn new_returns_error_for_btree_type() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();
        let result = UniqueIndex::new(spec);
        assert!(matches!(result, Err(IndexError::InvalidIndexType { .. })));
    }

    #[test]
    fn insert_single() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        assert_eq!(index.len(), 1);
        assert!(index.contains(&Value::String("alice@example.com".into())));
        assert_eq!(
            index.get(&Value::String("alice@example.com".into())),
            Some(1)
        );
    }

    #[test]
    fn insert_multiple_different_values() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();
        index
            .insert(Value::String("bob@example.com".into()), 2)
            .unwrap();
        index
            .insert(Value::String("charlie@example.com".into()), 3)
            .unwrap();

        assert_eq!(index.len(), 3);
    }

    #[test]
    fn insert_duplicate_fails() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        let result = index.insert(Value::String("alice@example.com".into()), 2);

        assert!(matches!(result, Err(IndexError::DuplicateValue { .. })));
    }

    #[test]
    fn insert_duplicate_same_element_ok() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        // Same element, same value - should be no-op
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        assert_eq!(index.len(), 1);
    }

    #[test]
    fn insert_replaces_old_value_for_same_element() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        // Same element, different value - replaces old
        index
            .insert(Value::String("alice.new@example.com".into()), 1)
            .unwrap();

        assert_eq!(index.len(), 1);
        assert!(!index.contains(&Value::String("alice@example.com".into())));
        assert!(index.contains(&Value::String("alice.new@example.com".into())));
    }

    #[test]
    fn lookup_eq_found() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();
        index
            .insert(Value::String("bob@example.com".into()), 2)
            .unwrap();

        let ids: Vec<_> = index
            .lookup_eq(&Value::String("alice@example.com".into()))
            .collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn lookup_eq_not_found() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        let ids: Vec<_> = index
            .lookup_eq(&Value::String("notfound@example.com".into()))
            .collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn lookup_range_returns_empty() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        // Range queries not supported
        let ids: Vec<_> = index
            .lookup_range(Bound::Unbounded, Bound::Unbounded)
            .collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn remove_existing() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        index
            .remove(&Value::String("alice@example.com".into()), 1)
            .unwrap();

        assert!(index.is_empty());
        assert!(!index.contains(&Value::String("alice@example.com".into())));
    }

    #[test]
    fn remove_wrong_element_id() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        // Wrong element ID - should not remove
        index
            .remove(&Value::String("alice@example.com".into()), 999)
            .unwrap();

        assert_eq!(index.len(), 1);
        assert!(index.contains(&Value::String("alice@example.com".into())));
    }

    #[test]
    fn remove_nonexistent_value() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        // Nonexistent value - should not error
        index
            .remove(&Value::String("notfound@example.com".into()), 1)
            .unwrap();

        assert_eq!(index.len(), 1);
    }

    #[test]
    fn update_value() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        index
            .update(
                &Value::String("alice@example.com".into()),
                Value::String("alice.new@example.com".into()),
                1,
            )
            .unwrap();

        assert_eq!(index.len(), 1);
        assert!(!index.contains(&Value::String("alice@example.com".into())));
        assert!(index.contains(&Value::String("alice.new@example.com".into())));
    }

    #[test]
    fn update_to_existing_value_fails() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();
        index
            .insert(Value::String("bob@example.com".into()), 2)
            .unwrap();

        // Try to update element 2 to use element 1's value
        let result = index.update(
            &Value::String("bob@example.com".into()),
            Value::String("alice@example.com".into()),
            2,
        );

        assert!(matches!(result, Err(IndexError::DuplicateValue { .. })));
    }

    #[test]
    fn update_to_same_value_ok() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        // Update to same value - should be no-op
        index
            .update(
                &Value::String("alice@example.com".into()),
                Value::String("alice@example.com".into()),
                1,
            )
            .unwrap();

        assert_eq!(index.len(), 1);
    }

    #[test]
    fn update_with_wrong_old_value() {
        // Test update when old_value doesn't match what's in the index
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();

        // Update with wrong old_value - should still work since we're
        // updating to a new value that doesn't conflict
        index
            .update(
                &Value::String("wrong@example.com".into()), // wrong old value
                Value::String("newalice@example.com".into()),
                1,
            )
            .unwrap();

        // New value should be in the index
        assert!(index.contains(&Value::String("newalice@example.com".into())));
        // Old value that was actually there should still be there (wasn't removed)
        assert!(index.contains(&Value::String("alice@example.com".into())));
        // The element now maps to newalice (reverse map updated)
        assert_eq!(
            index.get(&Value::String("newalice@example.com".into())),
            Some(1)
        );
    }

    #[test]
    fn update_maintains_map_consistency() {
        // Verify both map and reverse map stay consistent after update
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();
        index
            .insert(Value::String("bob@example.com".into()), 2)
            .unwrap();

        // Update element 1's value
        index
            .update(
                &Value::String("alice@example.com".into()),
                Value::String("alice.new@example.com".into()),
                1,
            )
            .unwrap();

        // Verify map consistency
        assert!(!index.contains(&Value::String("alice@example.com".into())));
        assert!(index.contains(&Value::String("alice.new@example.com".into())));
        assert!(index.contains(&Value::String("bob@example.com".into())));
        assert_eq!(index.len(), 2);

        // Verify lookups work correctly
        assert_eq!(
            index.get(&Value::String("alice.new@example.com".into())),
            Some(1)
        );
        assert_eq!(index.get(&Value::String("bob@example.com".into())), Some(2));
    }

    #[test]
    fn clear_removes_all() {
        let mut index = create_test_index();
        for i in 0..100 {
            index
                .insert(Value::String(format!("user{}@example.com", i)), i)
                .unwrap();
        }

        index.clear();

        assert!(index.is_empty());
        assert_eq!(index.count(), 0);
    }

    #[test]
    fn populate_batch() {
        let mut index = create_test_index();
        let elements = vec![
            (1, Value::String("alice@example.com".into())),
            (2, Value::String("bob@example.com".into())),
            (3, Value::String("charlie@example.com".into())),
        ];

        index.populate(elements.into_iter()).unwrap();

        assert_eq!(index.len(), 3);
    }

    #[test]
    fn populate_with_duplicate_fails() {
        let mut index = create_test_index();
        let elements = vec![
            (1, Value::String("alice@example.com".into())),
            (2, Value::String("alice@example.com".into())), // Duplicate!
        ];

        let result = index.populate(elements.into_iter());

        assert!(matches!(result, Err(IndexError::DuplicateValue { .. })));
    }

    #[test]
    fn statistics_updated() {
        let mut index = create_test_index();
        index
            .insert(Value::String("alice@example.com".into()), 1)
            .unwrap();
        index
            .insert(Value::String("bob@example.com".into()), 2)
            .unwrap();

        let stats = index.statistics();
        assert_eq!(stats.cardinality, 2);
        assert_eq!(stats.total_elements, 2);
    }

    #[test]
    fn covers_eq_filter() {
        let index = create_test_index();

        let filter = IndexFilter::eq(
            ElementType::Vertex,
            Some("user".to_string()),
            "email",
            Value::String("alice@example.com".into()),
        );

        assert!(index.covers(&filter));
    }

    #[test]
    fn does_not_cover_range_filter() {
        let index = create_test_index();

        let filter = IndexFilter::gte(
            ElementType::Vertex,
            Some("user".to_string()),
            "email",
            Value::String("a".into()),
        );

        assert!(!index.covers(&filter));
    }

    #[test]
    fn covers_wrong_property() {
        let index = create_test_index();

        let filter = IndexFilter::eq(
            ElementType::Vertex,
            Some("user".to_string()),
            "username", // Wrong property
            Value::String("alice".into()),
        );

        assert!(!index.covers(&filter));
    }

    #[test]
    fn integer_values() {
        let spec = IndexBuilder::vertex()
            .label("item")
            .property("sku")
            .unique()
            .build()
            .unwrap();
        let mut index = UniqueIndex::new(spec).unwrap();

        index.insert(Value::Int(1001), 1).unwrap();
        index.insert(Value::Int(1002), 2).unwrap();
        index.insert(Value::Int(1003), 3).unwrap();

        assert_eq!(index.get(&Value::Int(1002)), Some(2));
    }

    #[test]
    fn covers_within_filter() {
        let index = create_test_index();

        let filter = IndexFilter {
            element_type: ElementType::Vertex,
            label: Some("user".to_string()),
            property: "email".to_string(),
            predicate: IndexPredicate::Within(vec![
                Value::String("a@x.com".into()),
                Value::String("b@x.com".into()),
            ]),
        };

        // Within can be handled by multiple eq lookups
        assert!(index.covers(&filter));
    }
}
