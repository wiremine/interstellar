//! Property index trait and supporting types.
//!
//! This module defines the [`PropertyIndex`] trait that all index implementations
//! must implement, along with supporting types for query planning and statistics.

use std::ops::Bound;

use crate::index::error::IndexError;
use crate::index::spec::{ElementType, IndexPredicate, IndexSpec};
use crate::value::Value;

/// A filter that can potentially use an index.
///
/// Used by the query planner to find applicable indexes for a filter operation.
#[derive(Clone, Debug)]
pub struct IndexFilter {
    /// Element type (Vertex or Edge).
    pub element_type: ElementType,

    /// Label constraint (None = any label).
    pub label: Option<String>,

    /// Property being filtered.
    pub property: String,

    /// The predicate to evaluate.
    pub predicate: IndexPredicate,
}

impl IndexFilter {
    /// Create a new index filter for an equality check.
    pub fn eq(
        element_type: ElementType,
        label: Option<String>,
        property: impl Into<String>,
        value: Value,
    ) -> Self {
        Self {
            element_type,
            label,
            property: property.into(),
            predicate: IndexPredicate::Eq(value),
        }
    }

    /// Create a new index filter for a range check (>=).
    pub fn gte(
        element_type: ElementType,
        label: Option<String>,
        property: impl Into<String>,
        value: Value,
    ) -> Self {
        Self {
            element_type,
            label,
            property: property.into(),
            predicate: IndexPredicate::Gte(value),
        }
    }

    /// Create a new index filter for a range check (>).
    pub fn gt(
        element_type: ElementType,
        label: Option<String>,
        property: impl Into<String>,
        value: Value,
    ) -> Self {
        Self {
            element_type,
            label,
            property: property.into(),
            predicate: IndexPredicate::Gt(value),
        }
    }

    /// Create a new index filter for a range check (<=).
    pub fn lte(
        element_type: ElementType,
        label: Option<String>,
        property: impl Into<String>,
        value: Value,
    ) -> Self {
        Self {
            element_type,
            label,
            property: property.into(),
            predicate: IndexPredicate::Lte(value),
        }
    }

    /// Create a new index filter for a range check (<).
    pub fn lt(
        element_type: ElementType,
        label: Option<String>,
        property: impl Into<String>,
        value: Value,
    ) -> Self {
        Self {
            element_type,
            label,
            property: property.into(),
            predicate: IndexPredicate::Lt(value),
        }
    }
}

/// Statistics for query optimization.
///
/// Index statistics help the query planner estimate the cost of using
/// different indexes and choose the most efficient execution plan.
#[derive(Clone, Debug, Default)]
pub struct IndexStatistics {
    /// Number of distinct values in the index.
    pub cardinality: u64,

    /// Total number of indexed elements.
    pub total_elements: u64,

    /// Minimum value (if values are comparable).
    pub min_value: Option<Value>,

    /// Maximum value (if values are comparable).
    pub max_value: Option<Value>,

    /// Last time statistics were updated (Unix timestamp).
    pub last_updated: u64,
}

impl IndexStatistics {
    /// Create empty statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Estimate the selectivity of an equality predicate.
    ///
    /// Returns a value between 0.0 and 1.0 representing the fraction
    /// of elements expected to match.
    pub fn estimate_eq_selectivity(&self) -> f64 {
        if self.cardinality == 0 || self.total_elements == 0 {
            1.0
        } else {
            1.0 / self.cardinality as f64
        }
    }

    /// Estimate the selectivity of a range predicate.
    ///
    /// This is a rough estimate assuming uniform distribution.
    pub fn estimate_range_selectivity(&self, _start: Option<&Value>, _end: Option<&Value>) -> f64 {
        // Simple heuristic: assume 10% selectivity for range queries
        // A more sophisticated implementation would use value histograms
        0.1
    }
}

/// Trait for property index implementations.
///
/// All property indexes (B+ tree, unique, etc.) must implement this trait.
/// The trait provides methods for:
/// - Query planning (checking if an index covers a filter)
/// - Lookups (exact match and range queries)
/// - Modifications (insert, remove, update)
/// - Statistics for query optimization
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to support concurrent access
/// from multiple threads.
pub trait PropertyIndex: Send + Sync {
    /// Returns the index specification.
    fn spec(&self) -> &IndexSpec;

    /// Check if this index covers the given filter.
    ///
    /// Returns `true` if this index can efficiently satisfy the filter's
    /// predicate on the specified property and label.
    fn covers(&self, filter: &IndexFilter) -> bool;

    /// Look up elements with exact value match.
    ///
    /// Returns an iterator over element IDs (vertex or edge) that have
    /// the specified value for the indexed property.
    fn lookup_eq(&self, value: &Value) -> Box<dyn Iterator<Item = u64> + '_>;

    /// Look up elements in a range.
    ///
    /// Returns an iterator over element IDs where the indexed property
    /// value falls within the specified bounds.
    ///
    /// # Arguments
    ///
    /// * `start` - Lower bound (Unbounded, Included, or Excluded)
    /// * `end` - Upper bound (Unbounded, Included, or Excluded)
    fn lookup_range(
        &self,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = u64> + '_>;

    /// Insert an element into the index.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::DuplicateValue`] if this is a unique index
    /// and the value already exists for a different element.
    fn insert(&mut self, value: Value, element_id: u64) -> Result<(), IndexError>;

    /// Remove an element from the index.
    ///
    /// If the element is not in the index, this is a no-op.
    fn remove(&mut self, value: &Value, element_id: u64) -> Result<(), IndexError>;

    /// Update an element's indexed value.
    ///
    /// This is equivalent to `remove(old_value, id)` followed by
    /// `insert(new_value, id)`, but may be more efficient.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::DuplicateValue`] if this is a unique index
    /// and the new value already exists for a different element.
    fn update(
        &mut self,
        old_value: &Value,
        new_value: Value,
        element_id: u64,
    ) -> Result<(), IndexError> {
        self.remove(old_value, element_id)?;
        self.insert(new_value, element_id)
    }

    /// Return index statistics.
    fn statistics(&self) -> &IndexStatistics;

    /// Rebuild statistics from current data.
    fn refresh_statistics(&mut self);

    /// Clear all entries from the index.
    fn clear(&mut self);

    /// Returns true if the index has no entries.
    fn is_empty(&self) -> bool {
        self.statistics().total_elements == 0
    }

    /// Returns the number of indexed elements.
    fn len(&self) -> u64 {
        self.statistics().total_elements
    }
}

/// Helper function to check if an index covers a filter.
///
/// This is used by both BTreeIndex and UniqueIndex to implement
/// the `covers` method.
pub fn index_covers_filter(spec: &IndexSpec, filter: &IndexFilter, supports_range: bool) -> bool {
    // Check element type matches
    if filter.element_type != spec.element_type {
        return false;
    }

    // Check property matches
    if filter.property != spec.property {
        return false;
    }

    // Check label matches
    match (&spec.label, &filter.label) {
        (Some(idx_label), Some(filter_label)) => {
            if idx_label != filter_label {
                return false;
            }
        }
        (Some(_), None) => {
            // Index is label-specific, filter is not - index only covers subset
            return false;
        }
        (None, _) => {
            // Index covers all labels
        }
    }

    // Check predicate is supported
    match &filter.predicate {
        IndexPredicate::Eq(_) => true,
        IndexPredicate::Neq(_) => false, // Negation requires full scan
        IndexPredicate::Lt(_)
        | IndexPredicate::Lte(_)
        | IndexPredicate::Gt(_)
        | IndexPredicate::Gte(_)
        | IndexPredicate::Between { .. } => supports_range,
        IndexPredicate::Within(values) => {
            // Can use index if we can do multiple equality lookups
            !values.is_empty()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::spec::IndexBuilder;

    #[test]
    fn index_filter_eq() {
        let filter = IndexFilter::eq(
            ElementType::Vertex,
            Some("person".to_string()),
            "age",
            Value::Int(30),
        );

        assert_eq!(filter.element_type, ElementType::Vertex);
        assert_eq!(filter.label, Some("person".to_string()));
        assert_eq!(filter.property, "age");
        assert!(matches!(
            filter.predicate,
            IndexPredicate::Eq(Value::Int(30))
        ));
    }

    #[test]
    fn index_filter_gte() {
        let filter = IndexFilter::gte(
            ElementType::Vertex,
            Some("person".to_string()),
            "age",
            Value::Int(18),
        );

        assert!(matches!(
            filter.predicate,
            IndexPredicate::Gte(Value::Int(18))
        ));
    }

    #[test]
    fn index_statistics_default() {
        let stats = IndexStatistics::default();
        assert_eq!(stats.cardinality, 0);
        assert_eq!(stats.total_elements, 0);
        assert!(stats.min_value.is_none());
        assert!(stats.max_value.is_none());
    }

    #[test]
    fn index_statistics_eq_selectivity() {
        let stats = IndexStatistics {
            cardinality: 100,
            total_elements: 1000,
            ..Default::default()
        };

        let selectivity = stats.estimate_eq_selectivity();
        assert!((selectivity - 0.01).abs() < 0.001);
    }

    #[test]
    fn index_statistics_eq_selectivity_empty() {
        let stats = IndexStatistics::default();
        assert_eq!(stats.estimate_eq_selectivity(), 1.0);
    }

    #[test]
    fn index_covers_filter_basic() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();

        let filter = IndexFilter::eq(
            ElementType::Vertex,
            Some("person".to_string()),
            "age",
            Value::Int(30),
        );

        assert!(index_covers_filter(&spec, &filter, true));
    }

    #[test]
    fn index_covers_filter_wrong_element_type() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();

        let filter = IndexFilter::eq(
            ElementType::Edge, // Wrong!
            Some("person".to_string()),
            "age",
            Value::Int(30),
        );

        assert!(!index_covers_filter(&spec, &filter, true));
    }

    #[test]
    fn index_covers_filter_wrong_property() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();

        let filter = IndexFilter::eq(
            ElementType::Vertex,
            Some("person".to_string()),
            "name", // Wrong!
            Value::String("Alice".to_string()),
        );

        assert!(!index_covers_filter(&spec, &filter, true));
    }

    #[test]
    fn index_covers_filter_wrong_label() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();

        let filter = IndexFilter::eq(
            ElementType::Vertex,
            Some("user".to_string()), // Wrong!
            "age",
            Value::Int(30),
        );

        assert!(!index_covers_filter(&spec, &filter, true));
    }

    #[test]
    fn index_covers_filter_no_label_index() {
        let spec = IndexBuilder::vertex()
            .property("created_at")
            .build()
            .unwrap();

        // Filter with label should still be covered
        let filter = IndexFilter::eq(
            ElementType::Vertex,
            Some("person".to_string()),
            "created_at",
            Value::Int(12345),
        );

        assert!(index_covers_filter(&spec, &filter, true));
    }

    #[test]
    fn index_covers_filter_label_index_no_filter_label() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();

        // Filter without label - index only covers person, not all vertices
        let filter = IndexFilter::eq(
            ElementType::Vertex,
            None, // No label filter
            "age",
            Value::Int(30),
        );

        assert!(!index_covers_filter(&spec, &filter, true));
    }

    #[test]
    fn index_covers_filter_range_with_support() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();

        let filter = IndexFilter::gte(
            ElementType::Vertex,
            Some("person".to_string()),
            "age",
            Value::Int(18),
        );

        assert!(index_covers_filter(&spec, &filter, true));
    }

    #[test]
    fn index_covers_filter_range_without_support() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .unique()
            .build()
            .unwrap();

        let filter = IndexFilter::gte(
            ElementType::Vertex,
            Some("person".to_string()),
            "age",
            Value::Int(18),
        );

        // Unique index doesn't support range queries efficiently
        assert!(!index_covers_filter(&spec, &filter, false));
    }

    #[test]
    fn index_covers_filter_neq_not_supported() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();

        let filter = IndexFilter {
            element_type: ElementType::Vertex,
            label: Some("person".to_string()),
            property: "age".to_string(),
            predicate: IndexPredicate::Neq(Value::Int(30)),
        };

        // Neq requires full scan, index doesn't help
        assert!(!index_covers_filter(&spec, &filter, true));
    }

    #[test]
    fn index_covers_filter_within() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("status")
            .build()
            .unwrap();

        let filter = IndexFilter {
            element_type: ElementType::Vertex,
            label: Some("person".to_string()),
            property: "status".to_string(),
            predicate: IndexPredicate::Within(vec![
                Value::String("active".to_string()),
                Value::String("pending".to_string()),
            ]),
        };

        assert!(index_covers_filter(&spec, &filter, true));
    }

    #[test]
    fn index_covers_filter_within_empty() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("status")
            .build()
            .unwrap();

        let filter = IndexFilter {
            element_type: ElementType::Vertex,
            label: Some("person".to_string()),
            property: "status".to_string(),
            predicate: IndexPredicate::Within(vec![]),
        };

        // Empty Within is useless
        assert!(!index_covers_filter(&spec, &filter, true));
    }
}
