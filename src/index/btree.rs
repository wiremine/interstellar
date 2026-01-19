//! B+ tree index implementation.
//!
//! This module provides [`BTreeIndex`], a property index based on Rust's
//! [`std::collections::BTreeMap`] that supports efficient range queries.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::time::{SystemTime, UNIX_EPOCH};

use roaring::RoaringTreemap;

use crate::index::error::IndexError;
use crate::index::spec::{IndexSpec, IndexType};
use crate::index::traits::{index_covers_filter, IndexFilter, IndexStatistics, PropertyIndex};
use crate::value::{ComparableValue, Value};

/// B+ tree index for range queries.
///
/// This index uses Rust's [`BTreeMap`] internally, which provides O(log n)
/// lookup and range query performance. Each unique property value maps to
/// a [`RoaringTreemap`] containing the IDs of elements with that value.
///
/// # Use Cases
///
/// - Range queries: `age >= 18 AND age < 65`
/// - Ordered iteration: Get all values in sorted order
/// - Prefix matching (for string values)
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::index::{BTreeIndex, IndexBuilder};
///
/// let spec = IndexBuilder::vertex()
///     .label("person")
///     .property("age")
///     .build()?;
///
/// let mut index = BTreeIndex::new(spec);
/// index.insert(Value::Int(25), 1)?;
/// index.insert(Value::Int(30), 2)?;
/// index.insert(Value::Int(25), 3)?;
///
/// // Exact lookup
/// let ids: Vec<_> = index.lookup_eq(&Value::Int(25)).collect();
/// assert_eq!(ids, vec![1, 3]);
///
/// // Range lookup
/// use std::ops::Bound;
/// let ids: Vec<_> = index.lookup_range(
///     Bound::Included(&Value::Int(25)),
///     Bound::Excluded(&Value::Int(35)),
/// ).collect();
/// ```
pub struct BTreeIndex {
    /// Index specification.
    spec: IndexSpec,

    /// Underlying B-tree structure.
    /// Maps property values to sets of element IDs.
    tree: BTreeMap<ComparableValue, RoaringTreemap>,

    /// Index statistics.
    stats: IndexStatistics,
}

impl BTreeIndex {
    /// Create a new empty B+ tree index.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::InvalidIndexType`] if the spec's index_type
    /// is not `IndexType::BTree`.
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

    /// Build index from an iterator of (element_id, property_value) pairs.
    ///
    /// This is more efficient than calling `insert` repeatedly because
    /// it can batch the insertions and update statistics once at the end.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let elements = vec![
    ///     (1, Value::Int(25)),
    ///     (2, Value::Int(30)),
    ///     (3, Value::Int(25)),
    /// ];
    /// index.populate(elements.into_iter());
    /// ```
    pub fn populate<I>(&mut self, elements: I)
    where
        I: Iterator<Item = (u64, Value)>,
    {
        for (id, value) in elements {
            let key = value.to_comparable();
            self.tree.entry(key).or_default().insert(id);
        }
        self.refresh_statistics();
    }

    /// Get the number of distinct values in the index.
    pub fn distinct_values(&self) -> usize {
        self.tree.len()
    }

    /// Convert a Value bound to a ComparableValue bound.
    fn convert_bound(bound: Bound<&Value>) -> Bound<ComparableValue> {
        match bound {
            Bound::Included(v) => Bound::Included(v.to_comparable()),
            Bound::Excluded(v) => Bound::Excluded(v.to_comparable()),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

impl PropertyIndex for BTreeIndex {
    fn spec(&self) -> &IndexSpec {
        &self.spec
    }

    fn covers(&self, filter: &IndexFilter) -> bool {
        // BTreeIndex supports range queries
        index_covers_filter(&self.spec, filter, true)
    }

    fn lookup_eq(&self, value: &Value) -> Box<dyn Iterator<Item = u64> + '_> {
        let key = value.to_comparable();
        match self.tree.get(&key) {
            Some(bitmap) => Box::new(bitmap.iter()),
            None => Box::new(std::iter::empty()),
        }
    }

    fn lookup_range(
        &self,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = u64> + '_> {
        let start_bound = Self::convert_bound(start);
        let end_bound = Self::convert_bound(end);

        // Use range() to get matching entries
        // We need owned bounds for the range, so we use a workaround
        let iter = self
            .tree
            .range((start_bound, end_bound))
            .flat_map(|(_, bitmap)| bitmap.iter());

        Box::new(iter)
    }

    fn insert(&mut self, value: Value, element_id: u64) -> Result<(), IndexError> {
        let key = value.to_comparable();
        let bitmap = self.tree.entry(key).or_default();
        let was_new = bitmap.insert(element_id);

        if was_new {
            self.stats.total_elements += 1;
        }

        Ok(())
    }

    fn remove(&mut self, value: &Value, element_id: u64) -> Result<(), IndexError> {
        let key = value.to_comparable();
        if let Some(bitmap) = self.tree.get_mut(&key) {
            let was_present = bitmap.remove(element_id);
            if was_present {
                self.stats.total_elements = self.stats.total_elements.saturating_sub(1);
            }
            if bitmap.is_empty() {
                self.tree.remove(&key);
            }
        }
        Ok(())
    }

    fn statistics(&self) -> &IndexStatistics {
        &self.stats
    }

    fn refresh_statistics(&mut self) {
        let mut total = 0u64;
        for bitmap in self.tree.values() {
            total += bitmap.len();
        }

        self.stats.cardinality = self.tree.len() as u64;
        self.stats.total_elements = total;

        // Update min/max values
        self.stats.min_value = self.tree.keys().next().map(|k| k.to_value());
        self.stats.max_value = self.tree.keys().next_back().map(|k| k.to_value());

        // Update timestamp
        self.stats.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
    }

    fn clear(&mut self) {
        self.tree.clear();
        self.stats = IndexStatistics::default();
    }
}

// Extension trait for ComparableValue to convert back to Value
impl ComparableValue {
    /// Convert back to a Value.
    pub fn to_value(&self) -> Value {
        match self {
            ComparableValue::Null => Value::Null,
            ComparableValue::Bool(b) => Value::Bool(*b),
            ComparableValue::Int(n) => Value::Int(*n),
            ComparableValue::Float(f) => Value::Float(f.0),
            ComparableValue::String(s) => Value::String(s.clone()),
            ComparableValue::List(items) => {
                Value::List(items.iter().map(|v| v.to_value()).collect())
            }
            ComparableValue::Map(map) => {
                Value::Map(map.iter().map(|(k, v)| (k.clone(), v.to_value())).collect())
            }
            ComparableValue::Vertex(id) => Value::Vertex(*id),
            ComparableValue::Edge(id) => Value::Edge(*id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::spec::IndexBuilder;

    fn create_test_index() -> BTreeIndex {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();
        BTreeIndex::new(spec).unwrap()
    }

    #[test]
    fn new_creates_empty_index() {
        let index = create_test_index();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
        assert_eq!(index.distinct_values(), 0);
    }

    #[test]
    fn new_returns_error_for_unique_type() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("email")
            .unique()
            .build()
            .unwrap();
        let result = BTreeIndex::new(spec);
        assert!(matches!(result, Err(IndexError::InvalidIndexType { .. })));
    }

    #[test]
    fn insert_single() {
        let mut index = create_test_index();
        index.insert(Value::Int(30), 1).unwrap();

        assert_eq!(index.len(), 1);
        assert_eq!(index.distinct_values(), 1);

        let ids: Vec<_> = index.lookup_eq(&Value::Int(30)).collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn insert_multiple_same_value() {
        let mut index = create_test_index();
        index.insert(Value::Int(30), 1).unwrap();
        index.insert(Value::Int(30), 2).unwrap();
        index.insert(Value::Int(30), 3).unwrap();

        assert_eq!(index.len(), 3);
        assert_eq!(index.distinct_values(), 1);

        let ids: Vec<_> = index.lookup_eq(&Value::Int(30)).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn insert_multiple_different_values() {
        let mut index = create_test_index();
        index.insert(Value::Int(25), 1).unwrap();
        index.insert(Value::Int(30), 2).unwrap();
        index.insert(Value::Int(35), 3).unwrap();

        assert_eq!(index.len(), 3);
        assert_eq!(index.distinct_values(), 3);
    }

    #[test]
    fn insert_duplicate_id_same_value() {
        let mut index = create_test_index();
        index.insert(Value::Int(30), 1).unwrap();
        index.insert(Value::Int(30), 1).unwrap(); // Duplicate

        assert_eq!(index.len(), 1); // Should not double-count
    }

    #[test]
    fn lookup_eq_found() {
        let mut index = create_test_index();
        index.insert(Value::Int(25), 1).unwrap();
        index.insert(Value::Int(30), 2).unwrap();
        index.insert(Value::Int(25), 3).unwrap();

        let ids: Vec<_> = index.lookup_eq(&Value::Int(25)).collect();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn lookup_eq_not_found() {
        let mut index = create_test_index();
        index.insert(Value::Int(25), 1).unwrap();

        let ids: Vec<_> = index.lookup_eq(&Value::Int(30)).collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn lookup_range_inclusive() {
        let mut index = create_test_index();
        for i in 0..10 {
            index.insert(Value::Int(i * 10), i as u64).unwrap();
        }

        // [30, 60] should include 30, 40, 50, 60
        let ids: Vec<_> = index
            .lookup_range(
                Bound::Included(&Value::Int(30)),
                Bound::Included(&Value::Int(60)),
            )
            .collect();
        assert_eq!(ids, vec![3, 4, 5, 6]);
    }

    #[test]
    fn lookup_range_exclusive() {
        let mut index = create_test_index();
        for i in 0..10 {
            index.insert(Value::Int(i * 10), i as u64).unwrap();
        }

        // (30, 60) should include 40, 50
        let ids: Vec<_> = index
            .lookup_range(
                Bound::Excluded(&Value::Int(30)),
                Bound::Excluded(&Value::Int(60)),
            )
            .collect();
        assert_eq!(ids, vec![4, 5]);
    }

    #[test]
    fn lookup_range_unbounded_start() {
        let mut index = create_test_index();
        for i in 0..5 {
            index.insert(Value::Int(i * 10), i as u64).unwrap();
        }

        // [.., 25) should include 0, 10, 20
        let ids: Vec<_> = index
            .lookup_range(Bound::Unbounded, Bound::Excluded(&Value::Int(25)))
            .collect();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[test]
    fn lookup_range_unbounded_end() {
        let mut index = create_test_index();
        for i in 0..5 {
            index.insert(Value::Int(i * 10), i as u64).unwrap();
        }

        // [25, ..] should include 30, 40
        let ids: Vec<_> = index
            .lookup_range(Bound::Excluded(&Value::Int(20)), Bound::Unbounded)
            .collect();
        assert_eq!(ids, vec![3, 4]);
    }

    #[test]
    fn lookup_range_fully_unbounded() {
        let mut index = create_test_index();
        for i in 0..5 {
            index.insert(Value::Int(i), i as u64).unwrap();
        }

        let ids: Vec<_> = index
            .lookup_range(Bound::Unbounded, Bound::Unbounded)
            .collect();
        assert_eq!(ids, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn remove_single() {
        let mut index = create_test_index();
        index.insert(Value::Int(30), 1).unwrap();
        index.remove(&Value::Int(30), 1).unwrap();

        assert!(index.is_empty());
        let ids: Vec<_> = index.lookup_eq(&Value::Int(30)).collect();
        assert!(ids.is_empty());
    }

    #[test]
    fn remove_from_multiple() {
        let mut index = create_test_index();
        index.insert(Value::Int(30), 1).unwrap();
        index.insert(Value::Int(30), 2).unwrap();
        index.insert(Value::Int(30), 3).unwrap();

        index.remove(&Value::Int(30), 2).unwrap();

        assert_eq!(index.len(), 2);
        let ids: Vec<_> = index.lookup_eq(&Value::Int(30)).collect();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn remove_nonexistent() {
        let mut index = create_test_index();
        index.insert(Value::Int(30), 1).unwrap();

        // Should not error
        index.remove(&Value::Int(30), 999).unwrap();
        index.remove(&Value::Int(999), 1).unwrap();

        assert_eq!(index.len(), 1);
    }

    #[test]
    fn update_value() {
        let mut index = create_test_index();
        index.insert(Value::Int(25), 1).unwrap();

        index.update(&Value::Int(25), Value::Int(30), 1).unwrap();

        let old_ids: Vec<_> = index.lookup_eq(&Value::Int(25)).collect();
        assert!(old_ids.is_empty());

        let new_ids: Vec<_> = index.lookup_eq(&Value::Int(30)).collect();
        assert_eq!(new_ids, vec![1]);
    }

    #[test]
    fn clear_removes_all() {
        let mut index = create_test_index();
        for i in 0..100 {
            index.insert(Value::Int(i), i as u64).unwrap();
        }

        index.clear();

        assert!(index.is_empty());
        assert_eq!(index.distinct_values(), 0);
    }

    #[test]
    fn populate_batch() {
        let mut index = create_test_index();
        let elements = vec![
            (1, Value::Int(25)),
            (2, Value::Int(30)),
            (3, Value::Int(25)),
            (4, Value::Int(35)),
        ];

        index.populate(elements.into_iter());

        assert_eq!(index.len(), 4);
        assert_eq!(index.distinct_values(), 3);

        let ids: Vec<_> = index.lookup_eq(&Value::Int(25)).collect();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn statistics_updated() {
        let mut index = create_test_index();
        index.insert(Value::Int(10), 1).unwrap();
        index.insert(Value::Int(30), 2).unwrap();
        index.insert(Value::Int(20), 3).unwrap();

        index.refresh_statistics();

        let stats = index.statistics();
        assert_eq!(stats.cardinality, 3);
        assert_eq!(stats.total_elements, 3);
        assert_eq!(stats.min_value, Some(Value::Int(10)));
        assert_eq!(stats.max_value, Some(Value::Int(30)));
    }

    #[test]
    fn covers_matching_filter() {
        let index = create_test_index();

        let filter = IndexFilter::eq(
            crate::index::spec::ElementType::Vertex,
            Some("person".to_string()),
            "age",
            Value::Int(30),
        );

        assert!(index.covers(&filter));
    }

    #[test]
    fn covers_range_filter() {
        let index = create_test_index();

        let filter = IndexFilter::gte(
            crate::index::spec::ElementType::Vertex,
            Some("person".to_string()),
            "age",
            Value::Int(18),
        );

        assert!(index.covers(&filter));
    }

    #[test]
    fn covers_wrong_property() {
        let index = create_test_index();

        let filter = IndexFilter::eq(
            crate::index::spec::ElementType::Vertex,
            Some("person".to_string()),
            "name", // Wrong property
            Value::String("Alice".to_string()),
        );

        assert!(!index.covers(&filter));
    }

    #[test]
    fn string_values() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("name")
            .build()
            .unwrap();
        let mut index = BTreeIndex::new(spec).unwrap();

        index.insert(Value::String("Alice".to_string()), 1).unwrap();
        index.insert(Value::String("Bob".to_string()), 2).unwrap();
        index
            .insert(Value::String("Charlie".to_string()), 3)
            .unwrap();

        // Range query on strings
        let ids: Vec<_> = index
            .lookup_range(
                Bound::Included(&Value::String("Alice".to_string())),
                Bound::Excluded(&Value::String("Charlie".to_string())),
            )
            .collect();
        assert_eq!(ids, vec![1, 2]); // Alice, Bob
    }

    #[test]
    fn float_values() {
        let spec = IndexBuilder::vertex()
            .label("product")
            .property("price")
            .build()
            .unwrap();
        let mut index = BTreeIndex::new(spec).unwrap();

        index.insert(Value::Float(9.99), 1).unwrap();
        index.insert(Value::Float(19.99), 2).unwrap();
        index.insert(Value::Float(29.99), 3).unwrap();

        let ids: Vec<_> = index
            .lookup_range(
                Bound::Included(&Value::Float(10.0)),
                Bound::Included(&Value::Float(25.0)),
            )
            .collect();
        assert_eq!(ids, vec![2]); // Only 19.99
    }

    #[test]
    fn comparable_value_to_value_roundtrip() {
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(42),
            Value::Float(3.14),
            Value::String("test".to_string()),
        ];

        for val in values {
            let comparable = val.to_comparable();
            let back = comparable.to_value();
            assert_eq!(val, back);
        }
    }
}
