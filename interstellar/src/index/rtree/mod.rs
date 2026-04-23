//! R-tree spatial index for geospatial queries.
//!
//! Provides O(log n + k) lookup for geospatial predicates on `Value::Point`
//! and `Value::Polygon` properties, backed by [`rstar::RTree`].

use std::ops::Bound;

use rstar::{RTree, RTreeObject, AABB};

use crate::geo::BoundingBox;
use crate::index::error::IndexError;
use crate::index::spec::{IndexSpec, IndexType};
use crate::index::traits::{IndexFilter, IndexStatistics, PropertyIndex};
use crate::time::{SystemTime, UNIX_EPOCH};
use crate::value::Value;

// ---------------------------------------------------------------------------
// Entry — the element stored in the R-tree
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Entry {
    /// Element ID (vertex or edge).
    id: u64,
    /// Bounding box: [min_lon, min_lat, max_lon, max_lat].
    /// For points, min == max.
    bbox: [f64; 4],
}

impl RTreeObject for Entry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners([self.bbox[0], self.bbox[1]], [self.bbox[2], self.bbox[3]])
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

// rstar requires Eq for remove operations
impl Eq for Entry {}

// ---------------------------------------------------------------------------
// RTreeIndex
// ---------------------------------------------------------------------------

/// R-tree spatial index for geospatial property lookups.
///
/// Supports `Value::Point` and `Value::Polygon` values. Non-geometry values
/// are silently skipped during insert.
///
/// The `lookup_eq` and `lookup_range` methods from `PropertyIndex` are not
/// meaningful for spatial data; they return empty iterators. Spatial queries
/// go through the dedicated `lookup_bbox` method, which the traversal planner
/// calls when a geo predicate is paired with an RTree index.
pub struct RTreeIndex {
    spec: IndexSpec,
    tree: RTree<Entry>,
    stats: IndexStatistics,
}

impl RTreeIndex {
    /// Create a new empty R-tree index.
    pub fn new(spec: IndexSpec) -> Result<Self, IndexError> {
        if spec.index_type != IndexType::RTree {
            return Err(IndexError::InvalidIndexType {
                expected: IndexType::RTree,
                got: spec.index_type,
            });
        }
        Ok(Self {
            spec,
            tree: RTree::new(),
            stats: IndexStatistics::default(),
        })
    }

    /// Query the R-tree for all entries whose bounding box intersects the
    /// given bounding box. Returns element IDs.
    pub fn lookup_bbox(&self, bbox: &BoundingBox) -> Vec<u64> {
        let envelope =
            AABB::from_corners([bbox.min_lon, bbox.min_lat], [bbox.max_lon, bbox.max_lat]);
        self.tree
            .locate_in_envelope_intersecting(&envelope)
            .map(|e| e.id)
            .collect()
    }

    /// Extract a bounding box from a Value, if it's a geo type.
    fn value_to_bbox(value: &Value) -> Option<[f64; 4]> {
        match value {
            Value::Point(p) => Some([p.lon, p.lat, p.lon, p.lat]),
            Value::Polygon(poly) => {
                let bb = poly.bbox();
                Some([bb.min_lon, bb.min_lat, bb.max_lon, bb.max_lat])
            }
            _ => None,
        }
    }
}

impl PropertyIndex for RTreeIndex {
    fn spec(&self) -> &IndexSpec {
        &self.spec
    }

    fn covers(&self, filter: &IndexFilter) -> bool {
        // RTree only covers spatial predicates, not standard eq/range.
        // The traversal planner handles this via IndexType matching,
        // but we still check element type, label, and property.
        if filter.element_type != self.spec.element_type {
            return false;
        }
        if filter.property != self.spec.property {
            return false;
        }
        match (&self.spec.label, &filter.label) {
            (Some(idx_label), Some(filter_label)) if idx_label != filter_label => false,
            (Some(_), None) => false,
            _ => true,
        }
    }

    fn lookup_eq(&self, value: &Value) -> Box<dyn Iterator<Item = u64> + '_> {
        // For points, we can do a bbox lookup with a zero-area bbox
        if let Some(bbox) = Self::value_to_bbox(value) {
            let envelope = AABB::from_corners([bbox[0], bbox[1]], [bbox[2], bbox[3]]);
            Box::new(
                self.tree
                    .locate_in_envelope_intersecting(&envelope)
                    .map(|e| e.id)
                    .collect::<Vec<_>>()
                    .into_iter(),
            )
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn lookup_range(
        &self,
        _start: Bound<&Value>,
        _end: Bound<&Value>,
    ) -> Box<dyn Iterator<Item = u64> + '_> {
        // Range queries don't apply to spatial data
        Box::new(std::iter::empty())
    }

    fn insert(&mut self, value: Value, element_id: u64) -> Result<(), IndexError> {
        if let Some(bbox) = Self::value_to_bbox(&value) {
            self.tree.insert(Entry {
                id: element_id,
                bbox,
            });
            self.stats.total_elements += 1;
            self.stats.cardinality = self.stats.total_elements; // each geo value is unique-ish
        }
        // Non-geometry values are silently skipped
        Ok(())
    }

    fn remove(&mut self, value: &Value, element_id: u64) -> Result<(), IndexError> {
        if let Some(bbox) = Self::value_to_bbox(value) {
            let entry = Entry {
                id: element_id,
                bbox,
            };
            self.tree.remove(&entry);
            self.stats.total_elements = self.stats.total_elements.saturating_sub(1);
            self.stats.cardinality = self.stats.total_elements;
        }
        Ok(())
    }

    fn statistics(&self) -> &IndexStatistics {
        &self.stats
    }

    fn refresh_statistics(&mut self) {
        self.stats.total_elements = self.tree.size() as u64;
        self.stats.cardinality = self.stats.total_elements;
        self.stats.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    fn clear(&mut self) {
        self.tree = RTree::new();
        self.stats = IndexStatistics::default();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geo::{Point, Polygon};
    use crate::index::spec::IndexBuilder;

    fn rtree_spec() -> IndexSpec {
        IndexBuilder::vertex()
            .label("place")
            .property("location")
            .rtree()
            .build()
            .unwrap()
    }

    #[test]
    fn new_rejects_wrong_type() {
        let spec = IndexBuilder::vertex().property("x").build().unwrap(); // BTree
        assert!(RTreeIndex::new(spec).is_err());
    }

    #[test]
    fn insert_and_lookup_point() {
        let mut idx = RTreeIndex::new(rtree_spec()).unwrap();
        let sf = Point::new(-122.4194, 37.7749).unwrap();
        idx.insert(Value::Point(sf), 1).unwrap();

        let bb = BoundingBox {
            min_lon: -123.0,
            min_lat: 37.0,
            max_lon: -122.0,
            max_lat: 38.0,
        };
        let results = idx.lookup_bbox(&bb);
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn insert_and_lookup_polygon() {
        let mut idx = RTreeIndex::new(rtree_spec()).unwrap();
        let poly = Polygon::new(vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]).unwrap();
        idx.insert(Value::Polygon(poly), 2).unwrap();

        let bb = BoundingBox {
            min_lon: -0.5,
            min_lat: -0.5,
            max_lon: 0.5,
            max_lat: 0.5,
        };
        let results = idx.lookup_bbox(&bb);
        assert_eq!(results, vec![2]);
    }

    #[test]
    fn non_geo_value_skipped() {
        let mut idx = RTreeIndex::new(rtree_spec()).unwrap();
        idx.insert(Value::String("hello".into()), 3).unwrap();
        assert_eq!(idx.statistics().total_elements, 0);
    }

    #[test]
    fn remove_entry() {
        let mut idx = RTreeIndex::new(rtree_spec()).unwrap();
        let pt = Point::new(10.0, 20.0).unwrap();
        idx.insert(Value::Point(pt), 5).unwrap();
        assert_eq!(idx.statistics().total_elements, 1);

        idx.remove(&Value::Point(pt), 5).unwrap();
        assert_eq!(idx.statistics().total_elements, 0);
    }

    #[test]
    fn lookup_bbox_no_match() {
        let mut idx = RTreeIndex::new(rtree_spec()).unwrap();
        let pt = Point::new(10.0, 20.0).unwrap();
        idx.insert(Value::Point(pt), 1).unwrap();

        let bb = BoundingBox {
            min_lon: 50.0,
            min_lat: 50.0,
            max_lon: 60.0,
            max_lat: 60.0,
        };
        assert!(idx.lookup_bbox(&bb).is_empty());
    }

    #[test]
    fn clear_empties_index() {
        let mut idx = RTreeIndex::new(rtree_spec()).unwrap();
        let pt = Point::new(0.0, 0.0).unwrap();
        idx.insert(Value::Point(pt), 1).unwrap();
        idx.insert(Value::Point(Point::new(1.0, 1.0).unwrap()), 2)
            .unwrap();
        assert_eq!(idx.statistics().total_elements, 2);

        idx.clear();
        assert_eq!(idx.statistics().total_elements, 0);
        assert!(idx.tree.size() == 0);
    }

    #[test]
    fn lookup_eq_for_point() {
        let mut idx = RTreeIndex::new(rtree_spec()).unwrap();
        let pt = Point::new(5.0, 10.0).unwrap();
        idx.insert(Value::Point(pt), 42).unwrap();

        let results: Vec<_> = idx.lookup_eq(&Value::Point(pt)).collect();
        assert_eq!(results, vec![42]);
    }

    #[test]
    fn lookup_range_returns_empty() {
        let idx = RTreeIndex::new(rtree_spec()).unwrap();
        let results: Vec<_> = idx
            .lookup_range(Bound::Unbounded, Bound::Unbounded)
            .collect();
        assert!(results.is_empty());
    }

    #[test]
    fn refresh_statistics() {
        let mut idx = RTreeIndex::new(rtree_spec()).unwrap();
        idx.insert(Value::Point(Point::new(0.0, 0.0).unwrap()), 1)
            .unwrap();
        idx.insert(Value::Point(Point::new(1.0, 1.0).unwrap()), 2)
            .unwrap();
        idx.refresh_statistics();
        assert_eq!(idx.statistics().total_elements, 2);
        assert!(idx.statistics().last_updated > 0);
    }

    #[test]
    fn multiple_points_in_same_bbox() {
        let mut idx = RTreeIndex::new(rtree_spec()).unwrap();
        idx.insert(Value::Point(Point::new(1.0, 1.0).unwrap()), 1)
            .unwrap();
        idx.insert(Value::Point(Point::new(1.5, 1.5).unwrap()), 2)
            .unwrap();
        idx.insert(Value::Point(Point::new(50.0, 50.0).unwrap()), 3)
            .unwrap();

        let bb = BoundingBox {
            min_lon: 0.0,
            min_lat: 0.0,
            max_lon: 2.0,
            max_lat: 2.0,
        };
        let mut results = idx.lookup_bbox(&bb);
        results.sort();
        assert_eq!(results, vec![1, 2]);
    }
}
