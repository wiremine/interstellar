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

use crate::value::{EdgeId, Value, VertexId};

pub mod context;
pub mod filter;
pub mod navigation;
pub mod source;
pub mod step;
pub mod transform;

pub use context::{ExecutionContext, SideEffects};
pub use filter::{
    DedupStep, FilterStep, HasIdStep, HasLabelStep, HasStep, HasValueStep, LimitStep, RangeStep,
    SkipStep,
};
pub use navigation::{
    BothEStep, BothStep, BothVStep, InEStep, InStep, InVStep, OutEStep, OutStep, OutVStep,
};
pub use source::{BoundTraversal, GraphTraversalSource, TraversalExecutor};
pub use step::{AnyStep, IdentityStep, StartStep};
pub use transform::{ConstantStep, FlatMapStep, IdStep, LabelStep, MapStep, PathStep, ValuesStep};

// Re-export macros
pub use crate::{impl_filter_step, impl_flatmap_step};

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
// TraversalSource - source information for bound traversals
// -----------------------------------------------------------------------------

/// Source information for bound traversals.
///
/// This enum describes where a traversal starts - from all vertices,
/// specific vertices, all edges, specific edges, or injected values.
#[derive(Clone, Debug)]
pub enum TraversalSource {
    /// Start from all vertices
    AllVertices,
    /// Start from specific vertex IDs
    Vertices(Vec<VertexId>),
    /// Start from all edges
    AllEdges,
    /// Start from specific edge IDs
    Edges(Vec<EdgeId>),
    /// Inject arbitrary values
    Inject(Vec<Value>),
}

// -----------------------------------------------------------------------------
// Traversal - main traversal type with type erasure
// -----------------------------------------------------------------------------

/// Main traversal type - type-erased internally, type-safe externally.
///
/// # Type Parameters
///
/// - `In`: The input type this traversal expects (phantom)
/// - `Out`: The output type this traversal produces (phantom)
///
/// Both parameters are "phantom" - used only for compile-time checking.
/// Internally, all values flow as `Value` enum through `Box<dyn AnyStep>`.
///
/// # Design Notes
///
/// - Same type for bound and anonymous traversals
/// - Steps are stored as `Vec<Box<dyn AnyStep>>` for type erasure
/// - `In = ()` for traversals that start from a source (bound)
/// - `In = SomeType` for traversals that expect input (anonymous)
///
/// # Example
///
/// ```ignore
/// // Create an anonymous traversal
/// let anon: Traversal<Value, Value> = Traversal::new()
///     .add_step(HasLabelStep::single("person"));
///
/// // Anonymous traversals can be appended to bound traversals
/// let bound = g.v().append(anon);
/// ```
pub struct Traversal<In, Out> {
    /// The steps in this traversal (type-erased)
    steps: Vec<Box<dyn AnyStep>>,
    /// Optional reference to source (for bound traversals)
    source: Option<TraversalSource>,
    /// Phantom data for input/output types
    _phantom: PhantomData<fn(In) -> Out>,
}

impl<In, Out> Clone for Traversal<In, Out> {
    fn clone(&self) -> Self {
        Self {
            steps: self.steps.iter().map(|s| s.clone_box()).collect(),
            source: self.source.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<In, Out> std::fmt::Debug for Traversal<In, Out> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Traversal")
            .field("source", &self.source)
            .field("steps_count", &self.steps.len())
            .field(
                "step_names",
                &self.steps.iter().map(|s| s.name()).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl<In, Out> Default for Traversal<In, Out> {
    fn default() -> Self {
        Self::new()
    }
}

impl<In, Out> Traversal<In, Out> {
    /// Create a new empty traversal (for anonymous traversals).
    ///
    /// Anonymous traversals have no source - they expect input from
    /// the traversal they are appended to.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon: Traversal<Value, Value> = Traversal::new();
    /// ```
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
            source: None,
            _phantom: PhantomData,
        }
    }

    /// Create a traversal with a source (for bound traversals).
    ///
    /// This is typically called by `GraphTraversalSource` methods like
    /// `v()` and `e()`.
    pub(crate) fn with_source(source: TraversalSource) -> Self {
        Self {
            steps: Vec::new(),
            source: Some(source),
            _phantom: PhantomData,
        }
    }

    /// Add a step to the traversal, returning a new traversal with updated output type.
    ///
    /// This method consumes self and returns a new `Traversal` with the output
    /// type changed to `NewOut`. The phantom type parameters ensure compile-time
    /// safety even though the steps are type-erased internally.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let t: Traversal<(), Value> = Traversal::with_source(TraversalSource::AllVertices)
    ///     .add_step(HasLabelStep::single("person"));
    /// ```
    pub fn add_step<NewOut>(mut self, step: impl AnyStep + 'static) -> Traversal<In, NewOut> {
        self.steps.push(Box::new(step));
        Traversal {
            steps: self.steps,
            source: self.source,
            _phantom: PhantomData,
        }
    }

    /// Append another traversal's steps to this one.
    ///
    /// This is used to merge anonymous traversals into bound traversals.
    /// The output type becomes the output type of the appended traversal.
    ///
    /// # Type Safety
    ///
    /// The type system ensures that `other` expects `Out` as input
    /// and produces `Mid` as output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon: Traversal<Value, Value> = __::out().has_label("person");
    /// let bound = g.v().append(anon);
    /// ```
    pub fn append<Mid>(mut self, other: Traversal<Out, Mid>) -> Traversal<In, Mid> {
        self.steps.extend(other.steps);
        Traversal {
            steps: self.steps,
            source: self.source,
            _phantom: PhantomData,
        }
    }

    /// Get the steps for execution, consuming the traversal.
    ///
    /// Returns the optional source and the list of steps. This is used
    /// by `TraversalExecutor` to execute the traversal.
    #[allow(dead_code)] // Will be used by TraversalExecutor in upcoming phases
    pub(crate) fn into_steps(self) -> (Option<TraversalSource>, Vec<Box<dyn AnyStep>>) {
        (self.source, self.steps)
    }

    /// Get the number of steps in this traversal (for testing/debugging).
    #[inline]
    pub fn step_count(&self) -> usize {
        self.steps.len()
    }

    /// Check if this traversal has a source.
    #[inline]
    pub fn has_source(&self) -> bool {
        self.source.is_some()
    }

    /// Get a reference to the source (for debugging/testing).
    pub fn source(&self) -> Option<&TraversalSource> {
        self.source.as_ref()
    }

    /// Get step names for debugging/profiling.
    pub fn step_names(&self) -> Vec<&'static str> {
        self.steps.iter().map(|s| s.name()).collect()
    }
}

// -----------------------------------------------------------------------------
// Traversal Step Methods for Anonymous Traversals
// -----------------------------------------------------------------------------

impl<In> Traversal<In, Value> {
    /// Filter elements by label (for anonymous traversals).
    ///
    /// Keeps only vertices/edges whose label matches the given label.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to person vertices
    /// let anon = __::has_label("person");
    /// let people = g.v().append(anon).to_list();
    /// ```
    pub fn has_label(self, label: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(filter::HasLabelStep::single(label))
    }

    /// Filter elements by any of the given labels (for anonymous traversals).
    ///
    /// Keeps only vertices/edges whose label matches any of the given labels.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to person or company vertices
    /// let anon = __::has_label_any(&["person", "company"]);
    /// let entities = g.v().append(anon).to_list();
    /// ```
    pub fn has_label_any<I, S>(self, labels: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(filter::HasLabelStep::any(labels))
    }

    /// Filter elements by property existence (for anonymous traversals).
    ///
    /// Keeps only vertices/edges that have the specified property.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to vertices with "age" property
    /// let anon = Traversal::<Value, Value>::new().has("age");
    /// let with_age = g.v().append(anon).to_list();
    /// ```
    pub fn has(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(filter::HasStep::new(key))
    }

    /// Filter elements by property value equality (for anonymous traversals).
    ///
    /// Keeps only vertices/edges where the specified property equals the given value.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to vertices where name == "Alice"
    /// let anon = Traversal::<Value, Value>::new().has_value("name", "Alice");
    /// let alice = g.v().append(anon).to_list();
    /// ```
    pub fn has_value(
        self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Traversal<In, Value> {
        self.add_step(filter::HasValueStep::new(key, value))
    }

    /// Filter elements using a custom predicate (for anonymous traversals).
    ///
    /// The predicate receives the execution context and the value, returning
    /// `true` to keep the traverser or `false` to filter it out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to positive integers
    /// let anon = Traversal::<Value, Value>::new()
    ///     .filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 0));
    /// let positives = g.inject([1i64, -2i64, 3i64]).append(anon).to_list();
    /// ```
    pub fn filter<F>(self, predicate: F) -> Traversal<In, Value>
    where
        F: Fn(&context::ExecutionContext, &Value) -> bool + Clone + Send + Sync + 'static,
    {
        self.add_step(filter::FilterStep::new(predicate))
    }

    /// Deduplicate traversers by value (for anonymous traversals).
    ///
    /// Removes duplicate values from the traversal, keeping only the first
    /// occurrence of each value. Uses `Value`'s `Hash` implementation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that deduplicates values
    /// let anon = Traversal::<Value, Value>::new().dedup();
    /// let unique = g.v().out().append(anon).to_list();
    /// ```
    pub fn dedup(self) -> Traversal<In, Value> {
        self.add_step(filter::DedupStep::new())
    }

    /// Limit the number of traversers passing through (for anonymous traversals).
    ///
    /// Returns at most the specified number of traversers, stopping iteration
    /// after the limit is reached.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that limits to 5 elements
    /// let anon = Traversal::<Value, Value>::new().limit(5);
    /// let first_five = g.v().append(anon).to_list();
    /// ```
    pub fn limit(self, count: usize) -> Traversal<In, Value> {
        self.add_step(filter::LimitStep::new(count))
    }

    /// Skip the first n traversers (for anonymous traversals).
    ///
    /// Discards the first n traversers and passes through all remaining ones.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that skips 10 elements
    /// let anon = Traversal::<Value, Value>::new().skip(10);
    /// let after_skip = g.v().append(anon).to_list();
    /// ```
    pub fn skip(self, count: usize) -> Traversal<In, Value> {
        self.add_step(filter::SkipStep::new(count))
    }

    /// Select traversers within a given range (for anonymous traversals).
    ///
    /// Equivalent to `skip(start).limit(end - start)`. Returns traversers
    /// from index `start` (inclusive) to index `end` (exclusive).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that selects elements 10-19
    /// let anon = Traversal::<Value, Value>::new().range(10, 20);
    /// let page = g.v().append(anon).to_list();
    /// ```
    pub fn range(self, start: usize, end: usize) -> Traversal<In, Value> {
        self.add_step(filter::RangeStep::new(start, end))
    }

    /// Filter elements by a single ID (for anonymous traversals).
    ///
    /// Keeps only vertices/edges whose ID matches the given ID.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to a specific vertex
    /// let anon = Traversal::<Value, Value>::new().has_id(VertexId(1));
    /// let vertex = g.v().append(anon).to_list();
    /// ```
    pub fn has_id(self, id: impl Into<Value>) -> Traversal<In, Value> {
        self.add_step(filter::HasIdStep::from_value(id))
    }

    /// Filter elements by multiple IDs (for anonymous traversals).
    ///
    /// Keeps only vertices/edges whose ID matches any of the given IDs.
    /// Non-element values (integers, strings, etc.) are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to multiple vertices
    /// let anon = Traversal::<Value, Value>::new().has_ids([VertexId(1), VertexId(2)]);
    /// let vertices = g.v().append(anon).to_list();
    /// ```
    pub fn has_ids<I, T>(self, ids: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>,
    {
        self.add_step(filter::HasIdStep::from_values(
            ids.into_iter().map(Into::into).collect(),
        ))
    }

    // -------------------------------------------------------------------------
    // Navigation steps (for anonymous traversals)
    // -------------------------------------------------------------------------

    /// Traverse to outgoing adjacent vertices (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out();
    /// let neighbors = g.v().append(anon).to_list();
    /// ```
    pub fn out(self) -> Traversal<In, Value> {
        self.add_step(navigation::OutStep::new())
    }

    /// Traverse to outgoing adjacent vertices via edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out_labels(&["knows"]);
    /// let friends = g.v().append(anon).to_list();
    /// ```
    pub fn out_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::OutStep::with_labels(labels))
    }

    /// Traverse to incoming adjacent vertices (for anonymous traversals).
    ///
    /// Note: Named `in_` to avoid conflict with Rust's `in` keyword.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().in_();
    /// let known_by = g.v().append(anon).to_list();
    /// ```
    pub fn in_(self) -> Traversal<In, Value> {
        self.add_step(navigation::InStep::new())
    }

    /// Traverse to incoming adjacent vertices via edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().in_labels(&["knows"]);
    /// let known_by = g.v().append(anon).to_list();
    /// ```
    pub fn in_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::InStep::with_labels(labels))
    }

    /// Traverse to adjacent vertices in both directions (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().both();
    /// let neighbors = g.v().append(anon).to_list();
    /// ```
    pub fn both(self) -> Traversal<In, Value> {
        self.add_step(navigation::BothStep::new())
    }

    /// Traverse to adjacent vertices in both directions via edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().both_labels(&["knows"]);
    /// let connected = g.v().append(anon).to_list();
    /// ```
    pub fn both_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::BothStep::with_labels(labels))
    }

    /// Traverse to outgoing edges (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out_e();
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn out_e(self) -> Traversal<In, Value> {
        self.add_step(navigation::OutEStep::new())
    }

    /// Traverse to outgoing edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out_e_labels(&["knows"]);
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn out_e_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::OutEStep::with_labels(labels))
    }

    /// Traverse to incoming edges (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().in_e();
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn in_e(self) -> Traversal<In, Value> {
        self.add_step(navigation::InEStep::new())
    }

    /// Traverse to incoming edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().in_e_labels(&["knows"]);
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn in_e_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::InEStep::with_labels(labels))
    }

    /// Traverse to all incident edges (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().both_e();
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn both_e(self) -> Traversal<In, Value> {
        self.add_step(navigation::BothEStep::new())
    }

    /// Traverse to all incident edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().both_e_labels(&["knows"]);
    /// let edges = g.v().append(anon).to_list();
    /// ```
    pub fn both_e_labels(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(navigation::BothEStep::with_labels(labels))
    }

    /// Get the source vertex of an edge (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out_v();
    /// let sources = g.e().append(anon).to_list();
    /// ```
    pub fn out_v(self) -> Traversal<In, Value> {
        self.add_step(navigation::OutVStep::new())
    }

    /// Get the target vertex of an edge (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().in_v();
    /// let targets = g.e().append(anon).to_list();
    /// ```
    pub fn in_v(self) -> Traversal<In, Value> {
        self.add_step(navigation::InVStep::new())
    }

    /// Get both vertices of an edge (for anonymous traversals).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().both_v();
    /// let vertices = g.e().append(anon).to_list();
    /// ```
    pub fn both_v(self) -> Traversal<In, Value> {
        self.add_step(navigation::BothVStep::new())
    }

    // -------------------------------------------------------------------------
    // Transform steps (for anonymous traversals)
    // -------------------------------------------------------------------------

    /// Extract property values from vertices/edges (for anonymous traversals).
    ///
    /// For each input element, extracts the value of the specified property.
    /// Missing properties are silently skipped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().values("name");
    /// let names = g.v().has_label("person").append(anon).to_list();
    /// ```
    pub fn values(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(transform::ValuesStep::new(key))
    }

    /// Extract multiple property values from vertices/edges (for anonymous traversals).
    ///
    /// For each input element, extracts the values of the specified properties.
    /// Missing properties are silently skipped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().values_multi(["name", "age"]);
    /// let data = g.v().append(anon).to_list();
    /// ```
    pub fn values_multi<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(transform::ValuesStep::from_keys(keys))
    }

    /// Extract the ID from vertices/edges (for anonymous traversals).
    ///
    /// For each input element, extracts its ID as a `Value::Int`.
    /// Non-element values are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().id();
    /// let ids = g.v().has_label("person").append(anon).to_list();
    /// ```
    pub fn id(self) -> Traversal<In, Value> {
        self.add_step(transform::IdStep::new())
    }

    /// Extract the label from vertices/edges (for anonymous traversals).
    ///
    /// For each input element, extracts its label as a `Value::String`.
    /// Non-element values are filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().label();
    /// let labels = g.v().append(anon).to_list();
    /// ```
    pub fn label(self) -> Traversal<In, Value> {
        self.add_step(transform::LabelStep::new())
    }

    /// Transform each value using a closure (for anonymous traversals).
    ///
    /// The closure receives the execution context and the current value,
    /// returning a new value. This is a 1:1 mapping.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that doubles integer values
    /// let anon = Traversal::<Value, Value>::new()
    ///     .map(|_ctx, v| {
    ///         if let Value::Int(n) = v {
    ///             Value::Int(n * 2)
    ///         } else {
    ///             v.clone()
    ///         }
    ///     });
    /// let doubled = g.inject([1i64, 2i64]).append(anon).to_list();
    /// ```
    pub fn map<F>(self, f: F) -> Traversal<In, Value>
    where
        F: Fn(&context::ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static,
    {
        self.add_step(transform::MapStep::new(f))
    }

    /// Transform each value to multiple values using a closure (for anonymous traversals).
    ///
    /// The closure receives the execution context and the current value,
    /// returning a `Vec<Value>`. This is a 1:N mapping - each input can
    /// produce zero or more outputs.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that generates ranges
    /// let anon = Traversal::<Value, Value>::new()
    ///     .flat_map(|_ctx, v| {
    ///         if let Value::Int(n) = v {
    ///             (0..*n).map(|i| Value::Int(i)).collect()
    ///         } else {
    ///             vec![]
    ///         }
    ///     });
    /// let expanded = g.inject([3i64]).append(anon).to_list();
    /// // Results: [0, 1, 2]
    /// ```
    pub fn flat_map<F>(self, f: F) -> Traversal<In, Value>
    where
        F: Fn(&context::ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync + 'static,
    {
        self.add_step(transform::FlatMapStep::new(f))
    }

    /// Replace each traverser's value with a constant (for anonymous traversals).
    ///
    /// For each input traverser, replaces the value with the specified constant.
    /// All traverser metadata (path, loops, bulk, sack) is preserved.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that replaces values with "found"
    /// let anon = Traversal::<Value, Value>::new().constant("found");
    /// let results = g.v().append(anon).to_list();
    /// // All results: Value::String("found")
    ///
    /// // With numeric constant
    /// let anon = Traversal::<Value, Value>::new().constant(42i64);
    /// ```
    pub fn constant(self, value: impl Into<Value>) -> Traversal<In, Value> {
        self.add_step(transform::ConstantStep::new(value))
    }

    /// Convert the traverser's path to a Value::List (for anonymous traversals).
    ///
    /// Replaces the traverser's value with a list containing all elements
    /// from its path history. Each path element is converted to its
    /// corresponding Value representation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that returns the path
    /// let anon = Traversal::<Value, Value>::new().out().path();
    /// let paths = g.v().append(anon).to_list();
    /// // Each result is a Value::List of path elements
    /// ```
    pub fn path(self) -> Traversal<In, Value> {
        self.add_step(transform::PathStep::new())
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

    mod traversal_source_tests {
        use super::*;

        #[test]
        fn all_vertices_source() {
            let source = TraversalSource::AllVertices;
            assert!(matches!(source, TraversalSource::AllVertices));
        }

        #[test]
        fn specific_vertices_source() {
            let source = TraversalSource::Vertices(vec![VertexId(1), VertexId(2)]);
            match source {
                TraversalSource::Vertices(ids) => {
                    assert_eq!(ids.len(), 2);
                    assert_eq!(ids[0], VertexId(1));
                    assert_eq!(ids[1], VertexId(2));
                }
                _ => panic!("Expected Vertices variant"),
            }
        }

        #[test]
        fn all_edges_source() {
            let source = TraversalSource::AllEdges;
            assert!(matches!(source, TraversalSource::AllEdges));
        }

        #[test]
        fn specific_edges_source() {
            let source = TraversalSource::Edges(vec![EdgeId(10), EdgeId(20)]);
            match source {
                TraversalSource::Edges(ids) => {
                    assert_eq!(ids.len(), 2);
                    assert_eq!(ids[0], EdgeId(10));
                    assert_eq!(ids[1], EdgeId(20));
                }
                _ => panic!("Expected Edges variant"),
            }
        }

        #[test]
        fn inject_source() {
            let source =
                TraversalSource::Inject(vec![Value::Int(1), Value::String("test".to_string())]);
            match source {
                TraversalSource::Inject(values) => {
                    assert_eq!(values.len(), 2);
                    assert_eq!(values[0], Value::Int(1));
                    assert_eq!(values[1], Value::String("test".to_string()));
                }
                _ => panic!("Expected Inject variant"),
            }
        }

        #[test]
        fn source_is_clonable() {
            let source1 = TraversalSource::AllVertices;
            let source2 = TraversalSource::Vertices(vec![VertexId(1)]);
            let source3 = TraversalSource::Inject(vec![Value::Int(42)]);

            let _ = source1.clone();
            let _ = source2.clone();
            let _ = source3.clone();
        }
    }

    mod traversal_tests {
        use super::*;

        #[test]
        fn new_creates_empty_traversal() {
            let t: Traversal<Value, Value> = Traversal::new();
            assert_eq!(t.step_count(), 0);
            assert!(!t.has_source());
            assert!(t.source().is_none());
        }

        #[test]
        fn default_creates_empty_traversal() {
            let t: Traversal<Value, Value> = Traversal::default();
            assert_eq!(t.step_count(), 0);
            assert!(!t.has_source());
        }

        #[test]
        fn with_source_creates_sourced_traversal() {
            let t: Traversal<(), Value> = Traversal::with_source(TraversalSource::AllVertices);
            assert!(t.has_source());
            assert!(matches!(t.source(), Some(TraversalSource::AllVertices)));
            assert_eq!(t.step_count(), 0);
        }

        #[test]
        fn add_step_increments_count() {
            let t: Traversal<Value, Value> = Traversal::new();
            assert_eq!(t.step_count(), 0);

            let t: Traversal<Value, Value> = t.add_step(IdentityStep::new());
            assert_eq!(t.step_count(), 1);

            let t: Traversal<Value, Value> = t.add_step(IdentityStep::new());
            assert_eq!(t.step_count(), 2);
        }

        #[test]
        fn add_step_preserves_source() {
            let t: Traversal<(), Value> = Traversal::with_source(TraversalSource::AllVertices);
            let t: Traversal<(), Value> = t.add_step(IdentityStep::new());

            assert!(t.has_source());
            assert!(matches!(t.source(), Some(TraversalSource::AllVertices)));
        }

        #[test]
        fn step_names_returns_step_names() {
            let t: Traversal<Value, Value> =
                Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let t: Traversal<Value, Value> = t.add_step(IdentityStep::new());

            let names = t.step_names();
            assert_eq!(names.len(), 2);
            assert_eq!(names[0], "identity");
            assert_eq!(names[1], "identity");
        }

        #[test]
        fn append_merges_steps() {
            let t1: Traversal<(), Value> =
                Traversal::<(), Value>::with_source(TraversalSource::AllVertices)
                    .add_step(IdentityStep::new());
            let t2: Traversal<Value, Value> =
                Traversal::<Value, Value>::new().add_step(IdentityStep::new());
            let t2: Traversal<Value, Value> = t2.add_step(IdentityStep::new());

            let merged = t1.append(t2);
            assert_eq!(merged.step_count(), 3);
            assert!(merged.has_source());
        }

        #[test]
        fn append_drops_second_source() {
            // Even if the second traversal has a source, it should be ignored
            // (anonymous traversals shouldn't have sources in normal usage)
            let t1: Traversal<(), Value> = Traversal::with_source(TraversalSource::AllVertices);
            let t2: Traversal<Value, Value> = Traversal::with_source(TraversalSource::AllEdges);

            // Note: this is unusual usage but the behavior should be defined
            let merged = t1.append(t2);
            assert!(merged.has_source());
            // Source should be from t1, not t2
            assert!(matches!(
                merged.source(),
                Some(TraversalSource::AllVertices)
            ));
        }

        #[test]
        fn clone_creates_independent_copy() {
            let t1: Traversal<Value, Value> =
                Traversal::<Value, Value>::new().add_step(IdentityStep::new());

            let t2 = t1.clone();

            // Both should have same step count
            assert_eq!(t1.step_count(), t2.step_count());

            // They should be independent (adding to one doesn't affect other)
            let t1_modified: Traversal<Value, Value> = t1.add_step(IdentityStep::new());
            assert_eq!(t1_modified.step_count(), 2);
            assert_eq!(t2.step_count(), 1);
        }

        #[test]
        fn clone_preserves_source() {
            let t1: Traversal<(), Value> =
                Traversal::with_source(TraversalSource::Vertices(vec![VertexId(1), VertexId(2)]));
            let t2 = t1.clone();

            assert!(t2.has_source());
            match t2.source() {
                Some(TraversalSource::Vertices(ids)) => {
                    assert_eq!(ids.len(), 2);
                    assert_eq!(ids[0], VertexId(1));
                }
                _ => panic!("Expected Vertices source"),
            }
        }

        #[test]
        fn into_steps_returns_source_and_steps() {
            let t: Traversal<(), Value> =
                Traversal::<(), Value>::with_source(TraversalSource::AllVertices)
                    .add_step(IdentityStep::new());
            let t: Traversal<(), Value> = t.add_step(IdentityStep::new());

            let (source, steps) = t.into_steps();

            assert!(source.is_some());
            assert!(matches!(source, Some(TraversalSource::AllVertices)));
            assert_eq!(steps.len(), 2);
            assert_eq!(steps[0].name(), "identity");
        }

        #[test]
        fn into_steps_returns_none_source_for_anonymous() {
            let t: Traversal<Value, Value> =
                Traversal::<Value, Value>::new().add_step(IdentityStep::new());

            let (source, steps) = t.into_steps();

            assert!(source.is_none());
            assert_eq!(steps.len(), 1);
        }

        #[test]
        fn debug_format_shows_info() {
            let t: Traversal<(), Value> =
                Traversal::<(), Value>::with_source(TraversalSource::AllVertices)
                    .add_step(IdentityStep::new());

            let debug_str = format!("{:?}", t);
            assert!(debug_str.contains("Traversal"));
            assert!(debug_str.contains("steps_count"));
            assert!(debug_str.contains("step_names"));
        }

        #[test]
        fn steps_can_be_executed_from_into_steps() {
            use crate::graph::Graph;
            use crate::storage::InMemoryGraph;
            use std::sync::Arc;

            let storage = InMemoryGraph::new();
            let graph = Graph::new(Arc::new(storage));
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Create a simple traversal with identity step
            let t: Traversal<Value, Value> =
                Traversal::<Value, Value>::new().add_step(IdentityStep::new());

            let (_source, steps) = t.into_steps();

            // Execute the steps manually
            let input: Vec<Traverser> =
                vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let mut current: Box<dyn Iterator<Item = Traverser> + '_> = Box::new(input.into_iter());
            for step in &steps {
                current = step.apply(&ctx, current);
            }

            let results: Vec<Traverser> = current.collect();
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].value, Value::Int(1));
            assert_eq!(results[1].value, Value::Int(2));
        }
    }

    mod graph_traversal_source_tests {
        use super::*;
        use crate::graph::Graph;
        use crate::storage::InMemoryGraph;
        use std::sync::Arc;

        fn create_test_graph() -> Graph {
            let storage = InMemoryGraph::new();
            Graph::new(Arc::new(storage))
        }

        #[test]
        fn v_creates_all_vertices_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Test that v() works by calling a terminal step
            let count = g.v().count();
            // Empty graph should have 0 vertices
            assert_eq!(count, 0);
        }

        #[test]
        fn e_creates_all_edges_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Test that e() works by calling a terminal step
            let count = g.e().count();
            // Empty graph should have 0 edges
            assert_eq!(count, 0);
        }
    }
}
