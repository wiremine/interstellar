//! Traversal engine core types.
//!
//! This module provides the core types for the graph traversal engine:
//! - `Traverser`: Carries a `Value` through the pipeline with metadata
//! - `Path`: Tracks traversal history
//! - `PathElement`: A single element in the path
//! - `PathValue`: Values that can be stored in a path
//!
//! The design uses `Value` internally for type erasure while maintaining
//! compile-time type safety at API boundaries through phantom type parameters.

use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;

use smallvec::SmallVec;

use crate::graph::GraphSnapshot;
use crate::storage::{Edge, GraphStorage, Vertex};
use crate::value::{EdgeId, Value, VertexId};

pub mod context;

pub use context::{ExecutionContext, SideEffects};

// -----------------------------------------------------------------------------
// CloneSack trait - enables cloning of boxed sack values
// -----------------------------------------------------------------------------

/// Trait for clonable sack values.
///
/// Sacks are used to carry data alongside traversers through the pipeline.
/// This trait enables cloning of boxed sack values while maintaining type safety.
///
/// # Implementation
///
/// This trait uses a sealed pattern to prevent external implementations while
/// allowing any `Clone + Any + Send + 'static` type to be used as a sack value
/// through the `SackValue` wrapper.
pub trait CloneSack: Send {
    /// Clone this sack value into a boxed trait object.
    fn clone_box(&self) -> Box<dyn CloneSack>;

    /// Get a reference to the underlying value as `Any`.
    fn as_any(&self) -> &dyn Any;
}

/// Wrapper type for sack values that implements `CloneSack`.
///
/// This wrapper is used internally to store arbitrary cloneable values
/// in the traverser's sack.
#[derive(Clone)]
struct SackValue<T>(T);

impl<T: Clone + Any + Send + 'static> CloneSack for SackValue<T> {
    fn clone_box(&self) -> Box<dyn CloneSack> {
        Box::new(SackValue(self.0.clone()))
    }

    fn as_any(&self) -> &dyn Any {
        &self.0
    }
}

/// Create a boxed sack value from any cloneable type.
fn box_sack<T: Clone + Any + Send + 'static>(value: T) -> Box<dyn CloneSack> {
    Box::new(SackValue(value))
}

impl Clone for Box<dyn CloneSack> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// -----------------------------------------------------------------------------
// PathValue - values that can be stored in a path
// -----------------------------------------------------------------------------

/// Values that can be stored in a path.
///
/// Path values represent the elements encountered during traversal.
/// They are categorized into vertices, edges, and other property values.
#[derive(Clone, Debug, PartialEq)]
pub enum PathValue {
    /// A vertex in the path
    Vertex(VertexId),
    /// An edge in the path
    Edge(EdgeId),
    /// A property or other value in the path
    Property(Value),
}

impl From<&Value> for PathValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Vertex(id) => PathValue::Vertex(*id),
            Value::Edge(id) => PathValue::Edge(*id),
            other => PathValue::Property(other.clone()),
        }
    }
}

impl From<Value> for PathValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Vertex(id) => PathValue::Vertex(id),
            Value::Edge(id) => PathValue::Edge(id),
            other => PathValue::Property(other),
        }
    }
}

impl From<VertexId> for PathValue {
    fn from(id: VertexId) -> Self {
        PathValue::Vertex(id)
    }
}

impl From<EdgeId> for PathValue {
    fn from(id: EdgeId) -> Self {
        PathValue::Edge(id)
    }
}

impl PathValue {
    /// Check if this path value is a vertex.
    #[inline]
    pub fn is_vertex(&self) -> bool {
        matches!(self, PathValue::Vertex(_))
    }

    /// Check if this path value is an edge.
    #[inline]
    pub fn is_edge(&self) -> bool {
        matches!(self, PathValue::Edge(_))
    }

    /// Get the vertex ID if this is a vertex.
    pub fn as_vertex_id(&self) -> Option<VertexId> {
        match self {
            PathValue::Vertex(id) => Some(*id),
            _ => None,
        }
    }

    /// Get the edge ID if this is an edge.
    pub fn as_edge_id(&self) -> Option<EdgeId> {
        match self {
            PathValue::Edge(id) => Some(*id),
            _ => None,
        }
    }

    /// Convert to a `Value`.
    pub fn to_value(&self) -> Value {
        match self {
            PathValue::Vertex(id) => Value::Vertex(*id),
            PathValue::Edge(id) => Value::Edge(*id),
            PathValue::Property(v) => v.clone(),
        }
    }
}

// -----------------------------------------------------------------------------
// PathElement - a single element in the path with labels
// -----------------------------------------------------------------------------

/// A single element in the path.
///
/// Each path element contains a value and optional labels that were assigned
/// to it via `as()` step during traversal.
#[derive(Clone, Debug)]
pub struct PathElement {
    /// The value at this position in the path.
    pub value: PathValue,
    /// Labels assigned to this path position.
    pub labels: SmallVec<[String; 2]>,
}

impl PathElement {
    /// Create a new path element with no labels.
    pub fn new(value: PathValue) -> Self {
        Self {
            value,
            labels: SmallVec::new(),
        }
    }

    /// Create a new path element with labels.
    pub fn with_labels(value: PathValue, labels: impl IntoIterator<Item = String>) -> Self {
        Self {
            value,
            labels: labels.into_iter().collect(),
        }
    }
}

// -----------------------------------------------------------------------------
// Path - tracks traversal history
// -----------------------------------------------------------------------------

/// Path tracks traversal history.
///
/// The path records every element visited during traversal, along with any
/// labels that were assigned via `as()` steps. This enables path-based
/// queries and cycle detection.
///
/// # Example
///
/// ```ignore
/// let mut path = Path::default();
/// path.push(PathValue::Vertex(VertexId(1)), &["start".to_string()]);
/// path.push(PathValue::Edge(EdgeId(1)), &[]);
/// path.push(PathValue::Vertex(VertexId(2)), &["end".to_string()]);
///
/// assert_eq!(path.len(), 3);
/// assert!(path.contains_vertex(VertexId(1)));
/// ```
#[derive(Clone, Default, Debug)]
pub struct Path {
    /// Ordered list of path elements.
    objects: Vec<PathElement>,
    /// Label to indices mapping for quick lookups.
    labels: HashMap<String, Vec<usize>>,
}

impl Path {
    /// Create a new empty path.
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a new element onto the path.
    ///
    /// # Arguments
    ///
    /// * `value` - The path value to add
    /// * `labels` - Labels to assign to this path position
    pub fn push(&mut self, value: PathValue, labels: &[String]) {
        let idx = self.objects.len();

        // Update label index
        for label in labels {
            self.labels.entry(label.clone()).or_default().push(idx);
        }

        self.objects.push(PathElement {
            value,
            labels: labels.iter().cloned().collect(),
        });
    }

    /// Push a new element with a single label.
    pub fn push_labeled(&mut self, value: PathValue, label: &str) {
        self.push(value, &[label.to_string()]);
    }

    /// Push a new element without labels.
    pub fn push_unlabeled(&mut self, value: PathValue) {
        self.push(value, &[]);
    }

    /// Get elements by label.
    ///
    /// Returns `None` if the label doesn't exist in the path.
    pub fn get(&self, label: &str) -> Option<Vec<&PathValue>> {
        self.labels
            .get(label)
            .map(|indices| indices.iter().map(|&i| &self.objects[i].value).collect())
    }

    /// Get all objects in order.
    pub fn objects(&self) -> impl Iterator<Item = &PathValue> {
        self.objects.iter().map(|e| &e.value)
    }

    /// Get all path elements in order.
    pub fn elements(&self) -> impl Iterator<Item = &PathElement> {
        self.objects.iter()
    }

    /// Check if path contains a vertex (for cycle detection).
    pub fn contains_vertex(&self, id: VertexId) -> bool {
        self.objects
            .iter()
            .any(|e| matches!(&e.value, PathValue::Vertex(v) if *v == id))
    }

    /// Check if path contains an edge.
    pub fn contains_edge(&self, id: EdgeId) -> bool {
        self.objects
            .iter()
            .any(|e| matches!(&e.value, PathValue::Edge(e) if *e == id))
    }

    /// Check if a label exists in the path.
    pub fn has_label(&self, label: &str) -> bool {
        self.labels.contains_key(label)
    }

    /// Get all labels used in this path.
    pub fn all_labels(&self) -> impl Iterator<Item = &String> {
        self.labels.keys()
    }

    /// Length of the path.
    #[inline]
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    /// Check if path is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// Get the last element in the path.
    pub fn last(&self) -> Option<&PathValue> {
        self.objects.last().map(|e| &e.value)
    }

    /// Get the first element in the path.
    pub fn first(&self) -> Option<&PathValue> {
        self.objects.first().map(|e| &e.value)
    }

    /// Convert path to a list of values.
    pub fn to_list(&self) -> Vec<Value> {
        self.objects.iter().map(|e| e.value.to_value()).collect()
    }
}

// -----------------------------------------------------------------------------
// Traverser - carries a Value through the pipeline with metadata
// -----------------------------------------------------------------------------

/// Traverser carries a `Value` through the pipeline with metadata.
///
/// Unlike a monomorphic design, we use a single concrete type with `Value`
/// to enable type erasure in steps. This allows heterogeneous steps to be
/// stored in a `Vec<Box<dyn AnyStep>>`.
///
/// # Metadata
///
/// - `path`: History of elements visited
/// - `loops`: Counter for `repeat()` step
/// - `sack`: Optional data carried alongside the traverser
/// - `bulk`: Optimization for identical traversers
///
/// # Example
///
/// ```ignore
/// let t = Traverser::from_vertex(VertexId(1));
/// assert_eq!(t.as_vertex_id(), Some(VertexId(1)));
///
/// // Split preserves metadata
/// let t2 = t.split(Value::Vertex(VertexId(2)));
/// assert_eq!(t2.path.len(), t.path.len());
/// ```
#[derive(Clone)]
pub struct Traverser {
    /// The current element (always a Value).
    pub value: Value,
    /// Path history.
    pub path: Path,
    /// Loop counter for `repeat()`.
    pub loops: usize,
    /// Optional sack value (for future use).
    pub sack: Option<Box<dyn CloneSack>>,
    /// Bulk count (optimization for identical traversers).
    pub bulk: u64,
}

impl Traverser {
    /// Create a new traverser with default metadata.
    ///
    /// # Arguments
    ///
    /// * `value` - The initial value for the traverser
    pub fn new(value: impl Into<Value>) -> Self {
        Self {
            value: value.into(),
            path: Path::default(),
            loops: 0,
            sack: None,
            bulk: 1,
        }
    }

    /// Create traverser for a vertex.
    ///
    /// # Arguments
    ///
    /// * `id` - The vertex ID
    pub fn from_vertex(id: VertexId) -> Self {
        Self::new(Value::Vertex(id))
    }

    /// Create traverser for an edge.
    ///
    /// # Arguments
    ///
    /// * `id` - The edge ID
    pub fn from_edge(id: EdgeId) -> Self {
        Self::new(Value::Edge(id))
    }

    /// Split traverser for branching (preserves path and metadata).
    ///
    /// Creates a new traverser with a different value but the same
    /// path, loops, sack, and bulk. Used when a single traverser
    /// branches into multiple paths.
    ///
    /// # Arguments
    ///
    /// * `new_value` - The value for the new traverser
    pub fn split(&self, new_value: impl Into<Value>) -> Traverser {
        Traverser {
            value: new_value.into(),
            path: self.path.clone(),
            loops: self.loops,
            sack: self.sack.clone(),
            bulk: self.bulk,
        }
    }

    /// Replace the value while preserving metadata.
    ///
    /// Consumes self and returns a new traverser with the updated value.
    /// More efficient than `split()` when you don't need to keep the original.
    ///
    /// # Arguments
    ///
    /// * `new_value` - The new value for the traverser
    pub fn with_value(self, new_value: impl Into<Value>) -> Traverser {
        Traverser {
            value: new_value.into(),
            path: self.path,
            loops: self.loops,
            sack: self.sack,
            bulk: self.bulk,
        }
    }

    /// Increment loop counter.
    ///
    /// Called by the `repeat()` step each time the traverser loops.
    pub fn inc_loops(&mut self) {
        self.loops += 1;
    }

    /// Extend path with current value.
    ///
    /// Adds the current value to the path with the given labels.
    ///
    /// # Arguments
    ///
    /// * `labels` - Labels to assign to this path position
    pub fn extend_path(&mut self, labels: &[String]) {
        let path_value = PathValue::from(&self.value);
        self.path.push(path_value, labels);
    }

    /// Extend path with current value using a single label.
    pub fn extend_path_labeled(&mut self, label: &str) {
        self.extend_path(&[label.to_string()]);
    }

    /// Extend path with current value without labels.
    pub fn extend_path_unlabeled(&mut self) {
        self.extend_path(&[]);
    }

    /// Get the value as a vertex ID (if it is one).
    #[inline]
    pub fn as_vertex_id(&self) -> Option<VertexId> {
        self.value.as_vertex_id()
    }

    /// Get the value as an edge ID (if it is one).
    #[inline]
    pub fn as_edge_id(&self) -> Option<EdgeId> {
        self.value.as_edge_id()
    }

    /// Check if the current value is a vertex.
    #[inline]
    pub fn is_vertex(&self) -> bool {
        self.value.is_vertex()
    }

    /// Check if the current value is an edge.
    #[inline]
    pub fn is_edge(&self) -> bool {
        self.value.is_edge()
    }

    /// Get a reference to the sack value, downcasted to type T.
    pub fn get_sack<T: Clone + Any + Send + 'static>(&self) -> Option<&T> {
        self.sack.as_ref().and_then(|s| s.as_any().downcast_ref())
    }

    /// Set the sack value.
    pub fn set_sack<T: Clone + Any + Send + 'static>(&mut self, value: T) {
        self.sack = Some(box_sack(value));
    }

    /// Clear the sack value.
    pub fn clear_sack(&mut self) {
        self.sack = None;
    }

    /// Get the bulk count.
    #[inline]
    pub fn bulk(&self) -> u64 {
        self.bulk
    }

    /// Set the bulk count.
    #[inline]
    pub fn set_bulk(&mut self, bulk: u64) {
        self.bulk = bulk;
    }
}

impl std::fmt::Debug for Traverser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Traverser")
            .field("value", &self.value)
            .field("path", &self.path)
            .field("loops", &self.loops)
            .field("sack", &self.sack.is_some())
            .field("bulk", &self.bulk)
            .finish()
    }
}

// -----------------------------------------------------------------------------
// Stub types (to be replaced in later phases)
// -----------------------------------------------------------------------------

/// Main traversal type - stub to be replaced in Phase 1.9.
///
/// This is a temporary placeholder. The real implementation will use
/// type-erased steps with phantom type parameters.
pub struct Traversal<S, E, T> {
    pub source: S,
    pub _phantom: PhantomData<(E, T)>,
}

/// Graph traversal source - stub to be replaced in Phase 2.1.
///
/// Entry point for all bound traversals. Created from a `GraphSnapshot`
/// via `snapshot.traversal()`.
#[allow(dead_code)]
pub struct GraphTraversalSource<'s> {
    pub(crate) snapshot: &'s GraphSnapshot<'s>,
}

#[allow(dead_code)]
impl<'s> GraphTraversalSource<'s> {
    fn storage(&self) -> &dyn GraphStorage {
        self.snapshot.graph.storage.as_ref()
    }

    pub fn v(self) -> Traversal<Self, Vertex, Traverser> {
        Traversal {
            source: self,
            _phantom: PhantomData,
        }
    }

    pub fn e(self) -> Traversal<Self, Edge, Traverser> {
        Traversal {
            source: self,
            _phantom: PhantomData,
        }
    }
}

/// Predicate module - stub for Phase 4.
pub mod p {}

/// Anonymous traversal factory module - stub for Phase 4.
pub mod __ {}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    mod path_value_tests {
        use super::*;

        #[test]
        fn from_value_vertex() {
            let v = Value::Vertex(VertexId(42));
            let pv = PathValue::from(&v);
            assert_eq!(pv, PathValue::Vertex(VertexId(42)));
        }

        #[test]
        fn from_value_edge() {
            let v = Value::Edge(EdgeId(99));
            let pv = PathValue::from(&v);
            assert_eq!(pv, PathValue::Edge(EdgeId(99)));
        }

        #[test]
        fn from_value_property() {
            let v = Value::Int(42);
            let pv = PathValue::from(&v);
            assert_eq!(pv, PathValue::Property(Value::Int(42)));

            let v2 = Value::String("hello".to_string());
            let pv2 = PathValue::from(&v2);
            assert_eq!(pv2, PathValue::Property(Value::String("hello".to_string())));
        }

        #[test]
        fn is_vertex() {
            assert!(PathValue::Vertex(VertexId(1)).is_vertex());
            assert!(!PathValue::Edge(EdgeId(1)).is_vertex());
            assert!(!PathValue::Property(Value::Int(1)).is_vertex());
        }

        #[test]
        fn is_edge() {
            assert!(PathValue::Edge(EdgeId(1)).is_edge());
            assert!(!PathValue::Vertex(VertexId(1)).is_edge());
            assert!(!PathValue::Property(Value::Int(1)).is_edge());
        }

        #[test]
        fn as_vertex_id() {
            assert_eq!(
                PathValue::Vertex(VertexId(42)).as_vertex_id(),
                Some(VertexId(42))
            );
            assert_eq!(PathValue::Edge(EdgeId(42)).as_vertex_id(), None);
            assert_eq!(PathValue::Property(Value::Int(42)).as_vertex_id(), None);
        }

        #[test]
        fn as_edge_id() {
            assert_eq!(PathValue::Edge(EdgeId(99)).as_edge_id(), Some(EdgeId(99)));
            assert_eq!(PathValue::Vertex(VertexId(99)).as_edge_id(), None);
            assert_eq!(PathValue::Property(Value::Int(99)).as_edge_id(), None);
        }

        #[test]
        fn to_value() {
            assert_eq!(
                PathValue::Vertex(VertexId(1)).to_value(),
                Value::Vertex(VertexId(1))
            );
            assert_eq!(
                PathValue::Edge(EdgeId(2)).to_value(),
                Value::Edge(EdgeId(2))
            );
            assert_eq!(
                PathValue::Property(Value::String("test".to_string())).to_value(),
                Value::String("test".to_string())
            );
        }
    }

    mod path_tests {
        use super::*;

        #[test]
        fn new_path_is_empty() {
            let path = Path::new();
            assert!(path.is_empty());
            assert_eq!(path.len(), 0);
        }

        #[test]
        fn push_adds_elements() {
            let mut path = Path::new();
            path.push(PathValue::Vertex(VertexId(1)), &[]);
            assert_eq!(path.len(), 1);
            assert!(!path.is_empty());

            path.push(PathValue::Edge(EdgeId(1)), &[]);
            assert_eq!(path.len(), 2);

            path.push(PathValue::Vertex(VertexId(2)), &[]);
            assert_eq!(path.len(), 3);
        }

        #[test]
        fn push_with_labels() {
            let mut path = Path::new();
            path.push(
                PathValue::Vertex(VertexId(1)),
                &["start".to_string(), "source".to_string()],
            );
            path.push(PathValue::Vertex(VertexId(2)), &["end".to_string()]);

            assert!(path.has_label("start"));
            assert!(path.has_label("source"));
            assert!(path.has_label("end"));
            assert!(!path.has_label("middle"));
        }

        #[test]
        fn get_by_label() {
            let mut path = Path::new();
            path.push(PathValue::Vertex(VertexId(1)), &["a".to_string()]);
            path.push(PathValue::Vertex(VertexId(2)), &["b".to_string()]);
            path.push(PathValue::Vertex(VertexId(3)), &["a".to_string()]); // Duplicate label

            let a_values = path.get("a").unwrap();
            assert_eq!(a_values.len(), 2);
            assert_eq!(a_values[0].as_vertex_id(), Some(VertexId(1)));
            assert_eq!(a_values[1].as_vertex_id(), Some(VertexId(3)));

            let b_values = path.get("b").unwrap();
            assert_eq!(b_values.len(), 1);
            assert_eq!(b_values[0].as_vertex_id(), Some(VertexId(2)));

            assert!(path.get("nonexistent").is_none());
        }

        #[test]
        fn objects_iterator() {
            let mut path = Path::new();
            path.push(PathValue::Vertex(VertexId(1)), &[]);
            path.push(PathValue::Edge(EdgeId(2)), &[]);
            path.push(PathValue::Vertex(VertexId(3)), &[]);

            let objects: Vec<_> = path.objects().collect();
            assert_eq!(objects.len(), 3);
            assert_eq!(objects[0], &PathValue::Vertex(VertexId(1)));
            assert_eq!(objects[1], &PathValue::Edge(EdgeId(2)));
            assert_eq!(objects[2], &PathValue::Vertex(VertexId(3)));
        }

        #[test]
        fn contains_vertex() {
            let mut path = Path::new();
            path.push(PathValue::Vertex(VertexId(1)), &[]);
            path.push(PathValue::Edge(EdgeId(2)), &[]);
            path.push(PathValue::Vertex(VertexId(3)), &[]);

            assert!(path.contains_vertex(VertexId(1)));
            assert!(path.contains_vertex(VertexId(3)));
            assert!(!path.contains_vertex(VertexId(2)));
            assert!(!path.contains_vertex(VertexId(99)));
        }

        #[test]
        fn contains_edge() {
            let mut path = Path::new();
            path.push(PathValue::Vertex(VertexId(1)), &[]);
            path.push(PathValue::Edge(EdgeId(2)), &[]);
            path.push(PathValue::Vertex(VertexId(3)), &[]);

            assert!(path.contains_edge(EdgeId(2)));
            assert!(!path.contains_edge(EdgeId(1)));
            assert!(!path.contains_edge(EdgeId(99)));
        }

        #[test]
        fn first_and_last() {
            let mut path = Path::new();
            assert!(path.first().is_none());
            assert!(path.last().is_none());

            path.push(PathValue::Vertex(VertexId(1)), &[]);
            assert_eq!(path.first(), Some(&PathValue::Vertex(VertexId(1))));
            assert_eq!(path.last(), Some(&PathValue::Vertex(VertexId(1))));

            path.push(PathValue::Vertex(VertexId(2)), &[]);
            path.push(PathValue::Vertex(VertexId(3)), &[]);
            assert_eq!(path.first(), Some(&PathValue::Vertex(VertexId(1))));
            assert_eq!(path.last(), Some(&PathValue::Vertex(VertexId(3))));
        }

        #[test]
        fn to_list() {
            let mut path = Path::new();
            path.push(PathValue::Vertex(VertexId(1)), &[]);
            path.push(PathValue::Edge(EdgeId(2)), &[]);
            path.push(PathValue::Property(Value::Int(42)), &[]);

            let list = path.to_list();
            assert_eq!(list.len(), 3);
            assert_eq!(list[0], Value::Vertex(VertexId(1)));
            assert_eq!(list[1], Value::Edge(EdgeId(2)));
            assert_eq!(list[2], Value::Int(42));
        }

        #[test]
        fn clone_preserves_data() {
            let mut path = Path::new();
            path.push(PathValue::Vertex(VertexId(1)), &["start".to_string()]);
            path.push(PathValue::Vertex(VertexId(2)), &["end".to_string()]);

            let cloned = path.clone();
            assert_eq!(cloned.len(), 2);
            assert!(cloned.has_label("start"));
            assert!(cloned.has_label("end"));
            assert!(cloned.contains_vertex(VertexId(1)));
            assert!(cloned.contains_vertex(VertexId(2)));
        }
    }

    mod traverser_tests {
        use super::*;

        #[test]
        fn new_creates_traverser_with_value() {
            let t = Traverser::new(Value::Int(42));
            assert_eq!(t.value, Value::Int(42));
            assert!(t.path.is_empty());
            assert_eq!(t.loops, 0);
            assert!(t.sack.is_none());
            assert_eq!(t.bulk, 1);
        }

        #[test]
        fn new_with_into_value() {
            // Test with types that implement Into<Value>
            let t1 = Traverser::new(42i64);
            assert_eq!(t1.value, Value::Int(42));

            let t2 = Traverser::new("hello");
            assert_eq!(t2.value, Value::String("hello".to_string()));

            let t3 = Traverser::new(true);
            assert_eq!(t3.value, Value::Bool(true));
        }

        #[test]
        fn from_vertex_creates_vertex_traverser() {
            let t = Traverser::from_vertex(VertexId(123));
            assert_eq!(t.value, Value::Vertex(VertexId(123)));
            assert_eq!(t.as_vertex_id(), Some(VertexId(123)));
            assert!(t.is_vertex());
            assert!(!t.is_edge());
        }

        #[test]
        fn from_edge_creates_edge_traverser() {
            let t = Traverser::from_edge(EdgeId(456));
            assert_eq!(t.value, Value::Edge(EdgeId(456)));
            assert_eq!(t.as_edge_id(), Some(EdgeId(456)));
            assert!(t.is_edge());
            assert!(!t.is_vertex());
        }

        #[test]
        fn split_preserves_path_and_metadata() {
            let mut t = Traverser::from_vertex(VertexId(1));
            t.extend_path_labeled("start");
            t.loops = 5;
            t.bulk = 10;

            let t2 = t.split(Value::Vertex(VertexId(2)));

            // New value
            assert_eq!(t2.value, Value::Vertex(VertexId(2)));

            // Preserved metadata
            assert_eq!(t2.path.len(), t.path.len());
            assert!(t2.path.has_label("start"));
            assert_eq!(t2.loops, 5);
            assert_eq!(t2.bulk, 10);
        }

        #[test]
        fn with_value_replaces_value() {
            let t = Traverser::from_vertex(VertexId(1));
            let t2 = t.with_value(Value::Int(42));

            assert_eq!(t2.value, Value::Int(42));
        }

        #[test]
        fn inc_loops_increments() {
            let mut t = Traverser::new(Value::Null);
            assert_eq!(t.loops, 0);

            t.inc_loops();
            assert_eq!(t.loops, 1);

            t.inc_loops();
            assert_eq!(t.loops, 2);
        }

        #[test]
        fn extend_path_adds_to_path() {
            let mut t = Traverser::from_vertex(VertexId(1));
            assert!(t.path.is_empty());

            t.extend_path_labeled("a");
            assert_eq!(t.path.len(), 1);
            assert!(t.path.has_label("a"));

            t.value = Value::Vertex(VertexId(2));
            t.extend_path(&["b".to_string(), "c".to_string()]);
            assert_eq!(t.path.len(), 2);
            assert!(t.path.has_label("b"));
            assert!(t.path.has_label("c"));
        }

        #[test]
        fn as_vertex_id_returns_vertex_id() {
            let t = Traverser::from_vertex(VertexId(42));
            assert_eq!(t.as_vertex_id(), Some(VertexId(42)));

            let t2 = Traverser::from_edge(EdgeId(42));
            assert_eq!(t2.as_vertex_id(), None);

            let t3 = Traverser::new(Value::Int(42));
            assert_eq!(t3.as_vertex_id(), None);
        }

        #[test]
        fn as_edge_id_returns_edge_id() {
            let t = Traverser::from_edge(EdgeId(99));
            assert_eq!(t.as_edge_id(), Some(EdgeId(99)));

            let t2 = Traverser::from_vertex(VertexId(99));
            assert_eq!(t2.as_edge_id(), None);

            let t3 = Traverser::new(Value::Int(99));
            assert_eq!(t3.as_edge_id(), None);
        }

        #[test]
        fn clone_works_correctly() {
            let mut t = Traverser::from_vertex(VertexId(1));
            t.extend_path_labeled("start");
            t.loops = 3;
            t.bulk = 5;

            let cloned = t.clone();

            assert_eq!(cloned.value, t.value);
            assert_eq!(cloned.path.len(), t.path.len());
            assert_eq!(cloned.loops, t.loops);
            assert_eq!(cloned.bulk, t.bulk);
        }

        #[test]
        fn sack_operations() {
            let mut t = Traverser::new(Value::Null);

            // Initially no sack
            assert!(t.get_sack::<i32>().is_none());

            // Set sack
            t.set_sack(42i32);
            assert_eq!(t.get_sack::<i32>(), Some(&42));

            // Wrong type returns None
            assert!(t.get_sack::<String>().is_none());

            // Clear sack
            t.clear_sack();
            assert!(t.get_sack::<i32>().is_none());
        }

        #[test]
        fn bulk_operations() {
            let mut t = Traverser::new(Value::Null);
            assert_eq!(t.bulk(), 1);

            t.set_bulk(100);
            assert_eq!(t.bulk(), 100);
        }

        #[test]
        fn debug_output() {
            let t = Traverser::from_vertex(VertexId(1));
            let debug_str = format!("{:?}", t);
            assert!(debug_str.contains("Traverser"));
            assert!(debug_str.contains("value"));
            assert!(debug_str.contains("path"));
        }
    }

    mod clone_sack_tests {
        use super::*;

        #[test]
        fn clone_box_works() {
            let boxed: Box<dyn CloneSack> = box_sack(42i32);
            let cloned = boxed.clone_box();
            assert_eq!(cloned.as_any().downcast_ref::<i32>(), Some(&42));
        }

        #[test]
        fn clone_trait_impl_works() {
            let boxed: Box<dyn CloneSack> = box_sack("hello".to_string());
            let cloned = boxed.clone();
            assert_eq!(
                cloned.as_any().downcast_ref::<String>(),
                Some(&"hello".to_string())
            );
        }

        #[test]
        fn sack_preserves_on_split() {
            let mut t = Traverser::new(Value::Int(1));
            t.set_sack(vec![1, 2, 3]);

            let t2 = t.split(Value::Int(2));

            // Sack should be cloned
            assert_eq!(t2.get_sack::<Vec<i32>>(), Some(&vec![1, 2, 3]));
        }
    }
}
