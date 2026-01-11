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

pub mod aggregate;
pub mod branch;
pub mod context;
pub mod filter;
pub mod mutation;
pub mod navigation;
pub mod predicate;
pub mod repeat;
pub mod source;
pub mod step;
pub mod transform;

pub use aggregate::{
    BoundGroupBuilder, BoundGroupCountBuilder, GroupBuilder, GroupCountBuilder, GroupCountStep,
    GroupKey, GroupStep, GroupValue,
};
pub use branch::{
    AndStep, ChooseStep, CoalesceStep, LocalStep, NotStep, OptionalStep, OrStep, UnionStep,
    WhereStep,
};
pub use context::{ExecutionContext, SideEffects};
pub use filter::{
    CoinStep, CyclicPathStep, DedupByKeyStep, DedupByLabelStep, DedupByTraversalStep, DedupStep,
    FilterStep, HasIdStep, HasKeyStep, HasLabelStep, HasNotStep, HasPropValueStep, HasStep,
    HasValueStep, HasWhereStep, IsStep, LimitStep, RangeStep, SampleStep, SimplePathStep, SkipStep,
    TailStep,
};
pub use mutation::{
    AddEStep, AddVStep, DropStep, EdgeEndpoint, MutationExecutor, MutationResult, PendingMutation,
    PropertyStep,
};
pub use navigation::{
    BothEStep, BothStep, BothVStep, InEStep, InStep, InVStep, OtherVStep, OutEStep, OutStep,
    OutVStep,
};
pub use repeat::{RepeatConfig, RepeatStep, RepeatTraversal};
pub use source::{BoundTraversal, GraphTraversalSource, TraversalExecutor};
pub use step::{execute_traversal, execute_traversal_from, AnyStep, IdentityStep, StartStep};
pub use transform::{
    AsStep, BoundProjectBuilder, ConstantStep, ElementMapStep, FlatMapStep, IdStep, LabelStep,
    MapStep, MeanStep, Order, OrderBuilder, OrderKey, OrderStep, PathStep, ProjectBuilder,
    ProjectStep, Projection, PropertiesStep, SelectStep, UnfoldStep, ValueMapStep, ValuesStep,
};

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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathValue {
    /// A vertex in the path
    Vertex(VertexId),
    /// An edge in the path
    Edge(EdgeId),
    /// A property or other value in the path
    Property(Value),
}

impl std::hash::Hash for PathValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            PathValue::Vertex(id) => {
                0u8.hash(state);
                id.hash(state);
            }
            PathValue::Edge(id) => {
                1u8.hash(state);
                id.hash(state);
            }
            PathValue::Property(v) => {
                2u8.hash(state);
                v.hash(state);
            }
        }
    }
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

    /// Add a label to the last path element if it matches the given value,
    /// or push a new element with the label if not.
    ///
    /// This is used by `as_()` to label the current position without
    /// adding a duplicate entry when path tracking has already added it.
    ///
    /// # Arguments
    ///
    /// * `label` - The label to assign
    /// * `current_value` - The current traverser value to check against/add
    pub fn label_or_push(&mut self, label: &str, current_value: PathValue) {
        // Check if the last element matches the current value
        if let Some(last_idx) = self.objects.len().checked_sub(1) {
            if self.objects[last_idx].value == current_value {
                // Last element matches, just add the label
                self.objects[last_idx].labels.push(label.to_string());
                self.labels
                    .entry(label.to_string())
                    .or_default()
                    .push(last_idx);
                return;
            }
        }
        // Either path is empty or last element doesn't match - push new
        self.push_labeled(current_value, label);
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

    /// Get the current loop count.
    ///
    /// Returns the number of times this traverser has been through a `repeat()` loop.
    #[inline]
    pub fn loops(&self) -> usize {
        self.loops
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

    /// Label the current path position or add a new labeled entry.
    ///
    /// Used by `as_()` step. If the last path element matches the current
    /// value (e.g., when path tracking already added it), adds the label
    /// to that element. Otherwise, pushes a new entry with the label.
    pub fn label_path_position(&mut self, label: &str) {
        let current = PathValue::from(&self.value);
        self.path.label_or_push(label, current);
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

    /// Get a reference to the steps (for sub-traversal execution).
    ///
    /// This method provides read-only access to the traversal's steps,
    /// enabling the `execute_traversal` helper to apply steps without
    /// consuming the traversal.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().out().has_label("person");
    /// let steps = anon.steps();
    ///
    /// // Use with execute_traversal
    /// let output = execute_traversal(&ctx, steps, input);
    /// ```
    #[inline]
    pub fn steps(&self) -> &[Box<dyn AnyStep>] {
        &self.steps
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

    /// Filter elements by property absence (for anonymous traversals).
    ///
    /// Keeps only vertices/edges that do NOT have the specified property.
    /// Non-element values (integers, strings, etc.) pass through since they
    /// don't have properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to vertices without "email" property
    /// let anon = Traversal::<Value, Value>::new().has_not("email");
    /// let without_email = g.v().append(anon).to_list();
    /// ```
    pub fn has_not(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(filter::HasNotStep::new(key))
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

    /// Filter elements by property value using a predicate (for anonymous traversals).
    ///
    /// Keeps only vertices/edges where the specified property satisfies the predicate.
    /// Non-element values (integers, strings, etc.) are filtered out.
    /// Elements without the specified property are also filtered out.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Create an anonymous traversal that filters to adults
    /// let anon = Traversal::<Value, Value>::new().has_where("age", p::gte(18));
    /// let adults = g.v().append(anon).to_list();
    ///
    /// // With string predicates
    /// let anon = Traversal::<Value, Value>::new().has_where("name", p::starting_with("A"));
    /// let a_names = g.v().append(anon).to_list();
    /// ```
    pub fn has_where(
        self,
        key: impl Into<String>,
        predicate: impl predicate::Predicate + 'static,
    ) -> Traversal<In, Value> {
        self.add_step(filter::HasWhereStep::new(key, predicate))
    }

    /// Filter by testing the current value against a predicate (for anonymous traversals).
    ///
    /// Unlike `has_where()` which tests a property of vertices/edges, `is_()` tests
    /// the traverser's current value directly. This is useful after extracting
    /// property values with `values()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Filter ages greater than 25
    /// let anon = Traversal::<Value, Value>::new().is_(p::gt(25));
    /// let adults = g.v().values("age").append(anon).to_list();
    ///
    /// // Filter ages in a range
    /// let anon = Traversal::<Value, Value>::new().is_(p::between(20, 40));
    /// let in_range = g.v().values("age").append(anon).to_list();
    /// ```
    pub fn is_(self, predicate: impl predicate::Predicate + 'static) -> Traversal<In, Value> {
        self.add_step(filter::IsStep::new(predicate))
    }

    /// Filter by testing the current value for equality (for anonymous traversals).
    ///
    /// This is a convenience method equivalent to `is_(p::eq(value))`.
    /// Useful after extracting property values with `values()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Filter to ages equal to 29
    /// let anon = Traversal::<Value, Value>::new().is_eq(29);
    /// let age_29 = g.v().values("age").append(anon).to_list();
    ///
    /// // Filter to a specific name
    /// let anon = Traversal::<Value, Value>::new().is_eq("Alice");
    /// let alice = g.v().values("name").append(anon).to_list();
    /// ```
    pub fn is_eq(self, value: impl Into<Value>) -> Traversal<In, Value> {
        self.add_step(filter::IsStep::eq(value))
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

    /// Deduplicate traversers by property value (for anonymous traversals).
    ///
    /// Removes duplicates based on a property value extracted from elements.
    /// Only the first occurrence of each unique property value passes through.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to use for deduplication
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that deduplicates by age
    /// let anon = Traversal::<Value, Value>::new().dedup_by_key("age");
    /// let unique_ages = g.v().has_label("person").append(anon).to_list();
    /// ```
    pub fn dedup_by_key(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(filter::DedupByKeyStep::new(key))
    }

    /// Deduplicate traversers by element label (for anonymous traversals).
    ///
    /// Removes duplicates based on element label. Only the first occurrence
    /// of each unique label passes through.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that keeps one per label
    /// let anon = Traversal::<Value, Value>::new().dedup_by_label();
    /// let one_per_label = g.v().append(anon).to_list();
    /// ```
    pub fn dedup_by_label(self) -> Traversal<In, Value> {
        self.add_step(filter::DedupByLabelStep::new())
    }

    /// Deduplicate traversers by sub-traversal result (for anonymous traversals).
    ///
    /// Executes the given sub-traversal for each element and uses the first
    /// result as the deduplication key.
    ///
    /// # Arguments
    ///
    /// * `sub` - The sub-traversal to execute for each element
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that deduplicates by out-degree
    /// let anon = Traversal::<Value, Value>::new()
    ///     .dedup_by(__::out().count());
    /// let unique_outdegree = g.v().append(anon).to_list();
    /// ```
    pub fn dedup_by(self, sub: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(filter::DedupByTraversalStep::new(sub))
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

    /// Filter to only paths with no repeated elements (simple paths).
    ///
    /// A simple path visits each element at most once. This is useful
    /// for preventing cycles during traversal and finding unique paths.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().repeat(out()).until(hasLabel("target")).simplePath()
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find all simple paths of length 3
    /// let simple = g.v()
    ///     .repeat(__::out())
    ///     .times(3)
    ///     .simple_path()
    ///     .to_list();
    /// ```
    pub fn simple_path(self) -> Traversal<In, Value> {
        self.add_step(filter::SimplePathStep::new())
    }

    /// Filter to only paths with at least one repeated element (cyclic paths).
    ///
    /// A cyclic path contains at least one element that appears more than once.
    /// This is the inverse of `simple_path()`.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().repeat(out()).until(hasLabel("target")).cyclicPath()
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Find all cyclic paths
    /// let cyclic = g.v()
    ///     .repeat(__::out())
    ///     .times(4)
    ///     .cyclic_path()
    ///     .to_list();
    /// ```
    pub fn cyclic_path(self) -> Traversal<In, Value> {
        self.add_step(filter::CyclicPathStep::new())
    }

    /// Return only the last element (for anonymous traversals).
    ///
    /// This is a **barrier step** - it collects ALL input before returning
    /// only the last element. Equivalent to `tail_n(1)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that returns only the last element
    /// let anon = Traversal::<Value, Value>::new().tail();
    /// let last = g.v().append(anon).to_list();
    /// ```
    pub fn tail(self) -> Traversal<In, Value> {
        self.add_step(filter::TailStep::last())
    }

    /// Return only the last n elements (for anonymous traversals).
    ///
    /// This is a **barrier step** - it collects ALL input before returning
    /// the last n elements. Elements are returned in their original order.
    ///
    /// # Behavior
    ///
    /// - If fewer than n elements exist, all elements are returned
    /// - Empty traversal returns empty result
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that returns the last 5 elements
    /// let anon = Traversal::<Value, Value>::new().tail_n(5);
    /// let last_five = g.v().append(anon).to_list();
    /// ```
    pub fn tail_n(self, count: usize) -> Traversal<In, Value> {
        self.add_step(filter::TailStep::new(count))
    }

    /// Probabilistic filter using random coin flip (for anonymous traversals).
    ///
    /// Each traverser has a probability `p` of passing through. Useful for
    /// random sampling or probabilistic traversals.
    ///
    /// # Arguments
    ///
    /// * `probability` - Probability of passing (0.0 to 1.0, clamped)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that randomly samples ~50%
    /// let anon = Traversal::<Value, Value>::new().coin(0.5);
    /// let sample = g.v().append(anon).to_list();
    /// ```
    pub fn coin(self, probability: f64) -> Traversal<In, Value> {
        self.add_step(filter::CoinStep::new(probability))
    }

    /// Randomly sample n elements using reservoir sampling (for anonymous traversals).
    ///
    /// This is a **barrier step** that collects all input elements and returns
    /// a random sample of exactly n elements. If the input has fewer than n
    /// elements, all elements are returned.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of elements to sample
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that samples 5 random elements
    /// let anon = Traversal::<Value, Value>::new().sample(5);
    /// let sampled = g.v().append(anon).to_list();
    /// ```
    ///
    /// # Note
    ///
    /// Results are non-deterministic. For reproducible results in tests,
    /// use statistical tolerances.
    pub fn sample(self, count: usize) -> Traversal<In, Value> {
        self.add_step(filter::SampleStep::new(count))
    }

    /// Filter property objects by key name (for anonymous traversals).
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a matching "key" field.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to "name" properties
    /// let anon = Traversal::<Value, Value>::new().has_key("name");
    /// let names = g.v().properties().append(anon).to_list();
    /// ```
    pub fn has_key(self, key: impl Into<String>) -> Traversal<In, Value> {
        self.add_step(filter::HasKeyStep::new(key))
    }

    /// Filter property objects by any of the specified key names (for anonymous traversals).
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a "key" field matching any of the specified keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - The property keys to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to "name" or "age" properties
    /// let anon = Traversal::<Value, Value>::new().has_key_any(["name", "age"]);
    /// let props = g.v().properties().append(anon).to_list();
    /// ```
    pub fn has_key_any<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(filter::HasKeyStep::any(keys))
    }

    /// Filter property objects by value (for anonymous traversals).
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a matching "value" field.
    ///
    /// # Arguments
    ///
    /// * `value` - The property value to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to properties with value "Alice"
    /// let anon = Traversal::<Value, Value>::new().has_prop_value("Alice");
    /// let alice_props = g.v().properties().append(anon).to_list();
    /// ```
    pub fn has_prop_value(self, value: impl Into<Value>) -> Traversal<In, Value> {
        self.add_step(filter::HasPropValueStep::new(value))
    }

    /// Filter property objects by any of the specified values (for anonymous traversals).
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a "value" field matching any of the specified values.
    ///
    /// # Arguments
    ///
    /// * `values` - The property values to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters to properties with value "Alice" or "Bob"
    /// let anon = Traversal::<Value, Value>::new().has_prop_value_any(["Alice", "Bob"]);
    /// let props = g.v().properties().append(anon).to_list();
    /// ```
    pub fn has_prop_value_any<I, V>(self, values: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        self.add_step(filter::HasPropValueStep::any(values))
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

    /// Get the "other" vertex of an edge (for anonymous traversals).
    ///
    /// When traversing from a vertex to an edge, `other_v()` returns the
    /// vertex at the opposite end from where the traverser came from.
    /// Requires path tracking to be enabled.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().other_v();
    /// let others = g.v().out_e().append(anon).to_list();
    /// ```
    pub fn other_v(self) -> Traversal<In, Value> {
        self.add_step(navigation::OtherVStep::new())
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

    /// Extract all property objects from vertices/edges (for anonymous traversals).
    ///
    /// Unlike `values()` which returns just property values, `properties()` returns
    /// the full property including its key as a Map with "key" and "value" entries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().properties();
    /// let props = g.v().has_label("person").append(anon).to_list();
    /// // Each result is Value::Map { "key": "name", "value": "Alice" } etc.
    /// ```
    pub fn properties(self) -> Traversal<In, Value> {
        self.add_step(transform::PropertiesStep::new())
    }

    /// Extract specific property objects from vertices/edges (for anonymous traversals).
    ///
    /// Unlike `values()` which returns just property values, `properties_keys()` returns
    /// the full property including its key as a Map with "key" and "value" entries.
    /// Only the specified property keys are extracted.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().properties_keys(&["name", "age"]);
    /// let props = g.v().append(anon).to_list();
    /// ```
    pub fn properties_keys(self, keys: &[&str]) -> Traversal<In, Value> {
        let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
        self.add_step(transform::PropertiesStep::with_keys(keys))
    }

    /// Get all properties as a map (for anonymous traversals).
    ///
    /// Transforms each element into a `Value::Map` containing all its properties.
    /// Property values are wrapped in `Value::List` for multi-property compatibility.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().value_map();
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{"name": ["Alice"], "age": [30]}, ...]
    /// ```
    pub fn value_map(self) -> Traversal<In, Value> {
        self.add_step(transform::ValueMapStep::new())
    }

    /// Get specific properties as a map (for anonymous traversals).
    ///
    /// Transforms each element into a `Value::Map` containing only the
    /// specified properties. Property values are wrapped in `Value::List`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().value_map_keys(&["name"]);
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{"name": ["Alice"]}, {"name": ["Bob"]}]
    /// ```
    pub fn value_map_keys<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(transform::ValueMapStep::from_keys(keys))
    }

    /// Get all properties as a map including id and label tokens (for anonymous traversals).
    ///
    /// Like `value_map()`, but also includes "id" and "label" entries.
    /// The id and label are NOT wrapped in lists, but property values are.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().value_map_with_tokens();
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{"id": 0, "label": "person", "name": ["Alice"], "age": [30]}]
    /// ```
    pub fn value_map_with_tokens(self) -> Traversal<In, Value> {
        self.add_step(transform::ValueMapStep::new().with_tokens())
    }

    /// Get complete element representation as a map (for anonymous traversals).
    ///
    /// Transforms each element into a `Value::Map` with id, label, and all
    /// properties. Unlike `value_map()`, property values are NOT wrapped in lists.
    /// For edges, also includes "IN" and "OUT" vertex references.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().element_map();
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{"id": 0, "label": "person", "name": "Alice", "age": 30}]
    /// ```
    pub fn element_map(self) -> Traversal<In, Value> {
        self.add_step(transform::ElementMapStep::new())
    }

    /// Get element representation with specific properties (for anonymous traversals).
    ///
    /// Like `element_map()`, but includes only the specified properties
    /// along with the id, label, and (for edges) IN/OUT references.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = Traversal::<Value, Value>::new().element_map_keys(&["name"]);
    /// let maps = g.v().append(anon).to_list();
    /// // Returns: [{"id": 0, "label": "person", "name": "Alice"}]
    /// ```
    pub fn element_map_keys<I, S>(self, keys: I) -> Traversal<In, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.add_step(transform::ElementMapStep::from_keys(keys))
    }

    /// Unroll collections into individual elements (for anonymous traversals).
    ///
    /// This step expands `Value::List` and `Value::Map` into separate traversers:
    /// - `Value::List`: Each element becomes a separate traverser
    /// - `Value::Map`: Each key-value pair becomes a single-entry map traverser
    /// - Non-collection values pass through unchanged
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Unfold a list
    /// let anon = Traversal::<Value, Value>::new().unfold();
    /// let items = g.inject([Value::List(vec![Value::Int(1), Value::Int(2)])])
    ///     .append(anon)
    ///     .to_list();
    /// // Results: [Value::Int(1), Value::Int(2)]
    ///
    /// // Round-trip: fold then unfold
    /// let original = g.v().fold().unfold().to_list();
    /// // Returns original vertices
    /// ```
    pub fn unfold(self) -> Traversal<In, Value> {
        self.add_step(transform::UnfoldStep::new())
    }

    /// Calculate the arithmetic mean (average) of numeric values.
    ///
    /// This is a **barrier step** - it collects ALL input values before producing
    /// a single output. Only numeric values (`Value::Int` and `Value::Float`) are
    /// included in the calculation; non-numeric values are silently ignored.
    ///
    /// # Behavior
    ///
    /// - Collects all numeric values from input traversers
    /// - `Value::Int` values are converted to `f64` for calculation
    /// - `Value::Float` values are used directly
    /// - Non-numeric values (strings, booleans, vertices, etc.) are ignored
    /// - Returns `Value::Float` with the mean if any numeric values exist
    /// - Returns empty (no output) if no numeric values are found
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Calculate average age of all people
    /// let avg_age = g.v().has_label("person").values("age").mean().next();
    ///
    /// // Mixed values - non-numeric ignored
    /// let avg = g.inject(vec![Value::Int(1), Value::Int(2), Value::String("three".into())])
    ///     .mean().next(); // Returns Some(Value::Float(1.5))
    /// ```
    pub fn mean(self) -> Traversal<In, Value> {
        self.add_step(transform::MeanStep::new())
    }

    /// Sort traversers using a fluent builder.
    ///
    /// This is a **barrier step** - it collects ALL input before producing sorted output.
    /// Returns an `OrderBuilder` that allows chaining multiple sort keys using `by` methods.
    ///
    /// # Behavior
    ///
    /// - Collects all input traversers (barrier)
    /// - Sorts according to configured keys
    /// - Multiple `by` clauses create multi-level sorts
    /// - Supports sorting by:
    ///   - Natural order of current value
    ///   - Property values from vertices/edges
    ///   - Results of sub-traversals
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Sort by natural order ascending (default)
    /// let sorted = g.v().values("name").order().build().to_list();
    ///
    /// // Sort by property descending
    /// let sorted = g.v().has_label("person")
    ///     .order().by_key_desc("age").build()
    ///     .to_list();
    ///
    /// // Multi-level sort: by age desc, then name asc
    /// let sorted = g.v().has_label("person")
    ///     .order()
    ///     .by_key_desc("age")
    ///     .by_key_asc("name")
    ///     .build()
    ///     .to_list();
    /// ```
    pub fn order(self) -> transform::OrderBuilder<In> {
        let (_, steps) = self.into_steps();
        transform::OrderBuilder::new(steps)
    }

    /// Evaluate a mathematical expression (for anonymous traversals).
    ///
    /// The expression can reference the current value using `_` and labeled
    /// path values using their label names. Use `by()` to specify which
    /// property to extract from labeled elements.
    ///
    /// Uses the `mathexpr` crate for full expression parsing and evaluation,
    /// supporting:
    /// - Operators: `+`, `-`, `*`, `/`, `%`, `^`
    /// - Functions: `sqrt`, `abs`, `sin`, `cos`, `tan`, `log`, `exp`, `pow`, `min`, `max`, etc.
    /// - Constants: `pi`, `e`
    /// - Parentheses for grouping
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Double current values
    /// g.v().values("age").math("_ * 2").build()
    ///
    /// // Calculate difference between labeled values
    /// g.v().as_("a").out("knows").as_("b")
    ///     .math("a - b")
    ///     .by("a", "age")
    ///     .by("b", "age")
    ///     .build()
    ///
    /// // Complex expression with functions
    /// g.v().values("x").math("sqrt(_ ^ 2 + 1)").build()
    /// ```
    pub fn math(self, expression: &str) -> transform::MathBuilder<In> {
        let (_, steps) = self.into_steps();
        transform::MathBuilder::new(steps, expression)
    }

    /// Create a projection with named keys (for anonymous traversals).
    ///
    /// The `project()` step creates a map with specific named keys. Each key's value
    /// is defined by a `by()` modulator, which can extract a property or execute
    /// a sub-traversal.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().hasLabel('person')
    ///     .project('name', 'age', 'friends')
    ///     .by('name')
    ///     .by('age')
    ///     .by(out('knows').count())
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// use __; // Anonymous traversal module
    ///
    /// let results = g.v().has_label("person")
    ///     .project(&["name", "friend_count"])
    ///     .by_key("name")
    ///     .by(__::out("knows").count())
    ///     .build()
    ///     .to_list();
    /// // Results: [{name: "Alice", friend_count: 2}, ...]
    /// ```
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys for the projection map
    ///
    /// # Returns
    ///
    /// A `ProjectBuilder` that requires `by()` clauses to be added for each key.
    pub fn project(self, keys: &[&str]) -> transform::ProjectBuilder<In> {
        let (_, steps) = self.into_steps();
        let key_strings: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        transform::ProjectBuilder::new(steps, key_strings)
    }

    /// Group traversers by a key and collect values (for anonymous traversals).
    ///
    /// The `group()` step is a **barrier step** that collects all input traversers,
    /// groups them by a key, and produces a single `Value::Map` output where:
    /// - Keys are the grouping keys (converted to strings)
    /// - Values are lists of collected values for each group
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// g.V().group().by(label)  // Group by label
    /// g.V().group().by("age").by("name")  // Group by age, collect names
    /// g.V().group().by(label).by(out().count())  // Group by label, count outgoing
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// use __; // Anonymous traversal module
    ///
    /// // Group vertices by label
    /// let groups = g.v()
    ///     .group().by_label().by_value().build()
    ///     .next();
    /// // Returns: Map { "person" -> [v1, v2], "software" -> [v3] }
    ///
    /// // Group by property, collect other property
    /// let groups = g.v().has_label("person")
    ///     .group().by_key("age").by_value_key("name").build()
    ///     .next();
    /// // Returns: Map { "29" -> ["Alice", "Bob"], "30" -> ["Charlie"] }
    /// ```
    ///
    /// # Returns
    ///
    /// A `GroupBuilder` that allows configuring the grouping key and value collector.
    pub fn group(self) -> aggregate::GroupBuilder<In> {
        let (_, steps) = self.into_steps();
        aggregate::GroupBuilder::new(steps)
    }

    /// Count traversers grouped by a key (for anonymous traversals).
    ///
    /// Creates a `GroupCountBuilder` that allows specifying how to group and count
    /// the traversers. The result is a single `Value::Map` where keys are the
    /// grouping keys and values are integer counts.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::*;
    /// use rustgremlin::traversal::__;
    ///
    /// // Count vertices by label
    /// let t = __::v().group_count().by_label().build();
    ///
    /// // Count vertices by a property
    /// let t2 = __::v().group_count().by_key("age").build();
    /// ```
    ///
    /// # Returns
    ///
    /// A `GroupCountBuilder` that allows configuring the grouping key.
    pub fn group_count(self) -> aggregate::GroupCountBuilder<In> {
        let (_, steps) = self.into_steps();
        aggregate::GroupCountBuilder::new(steps)
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

    /// Label the current position in the traversal path (for anonymous traversals).
    ///
    /// Records the current traverser's value in the path with the specified label.
    /// This enables later retrieval via `select()` or `select_one()`.
    ///
    /// Unlike automatic path tracking, `as_()` labels are always recorded
    /// regardless of whether `with_path()` was called.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal with labeled positions
    /// let anon = Traversal::<Value, Value>::new()
    ///     .as_("start").out().as_("end").select(&["start", "end"]);
    /// let results = g.v().append(anon).to_list();
    /// ```
    pub fn as_(self, label: &str) -> Traversal<In, Value> {
        self.add_step(transform::AsStep::new(label))
    }

    /// Select multiple labeled values from the path (for anonymous traversals).
    ///
    /// Retrieves values that were labeled with `as_()` and returns them as a Map.
    /// Traversers without any of the requested labels are filtered out.
    ///
    /// # Arguments
    ///
    /// * `labels` - The labels to select from the path
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that selects labeled values
    /// let anon = Traversal::<Value, Value>::new()
    ///     .as_("a").out().as_("b").select(&["a", "b"]);
    /// let results = g.v().append(anon).to_list();
    /// // Returns Map { "a" -> vertex1, "b" -> vertex2 }
    /// ```
    pub fn select(self, labels: &[&str]) -> Traversal<In, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        self.add_step(transform::SelectStep::new(labels))
    }

    /// Select a single labeled value from the path (for anonymous traversals).
    ///
    /// Retrieves the value that was labeled with `as_()` and returns it directly
    /// (not wrapped in a Map). Traversers without the requested label are filtered out.
    ///
    /// # Arguments
    ///
    /// * `label` - The label to select from the path
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that selects a single labeled value
    /// let anon = Traversal::<Value, Value>::new()
    ///     .as_("x").out().select_one("x");
    /// let results = g.v().append(anon).to_list();
    /// // Returns the labeled vertex directly (not a Map)
    /// ```
    pub fn select_one(self, label: &str) -> Traversal<In, Value> {
        self.add_step(transform::SelectStep::single(label))
    }

    // -------------------------------------------------------------------------
    // Filter steps using anonymous traversals
    // -------------------------------------------------------------------------

    /// Filter by sub-traversal existence (for anonymous traversals).
    ///
    /// Emits input traverser only if the sub-traversal produces at least one result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters by sub-traversal
    /// let anon = Traversal::<Value, Value>::new()
    ///     .where_(__.out());
    /// let with_out = g.v().append(anon).to_list();
    /// ```
    pub fn where_(self, sub: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(branch::WhereStep::new(sub))
    }

    /// Filter by sub-traversal non-existence (for anonymous traversals).
    ///
    /// Emits input traverser only if the sub-traversal produces NO results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that filters out vertices with outgoing edges
    /// let anon = Traversal::<Value, Value>::new()
    ///     .not(__.out());
    /// let leaves = g.v().append(anon).to_list();
    /// ```
    pub fn not(self, sub: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(branch::NotStep::new(sub))
    }

    /// Filter by multiple sub-traversals (AND logic) (for anonymous traversals).
    ///
    /// Emits input traverser only if ALL sub-traversals produce at least one result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that requires both conditions
    /// let anon = Traversal::<Value, Value>::new()
    ///     .and_(vec![__.out(), __.in_()]);
    /// let connected = g.v().append(anon).to_list();
    /// ```
    pub fn and_(self, subs: Vec<Traversal<Value, Value>>) -> Traversal<In, Value> {
        self.add_step(branch::AndStep::new(subs))
    }

    /// Filter by multiple sub-traversals (OR logic) (for anonymous traversals).
    ///
    /// Emits input traverser if ANY sub-traversal produces at least one result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that accepts either condition
    /// let anon = Traversal::<Value, Value>::new()
    ///     .or_(vec![__.has_label("person"), __.has_label("software")]);
    /// let entities = g.v().append(anon).to_list();
    /// ```
    pub fn or_(self, subs: Vec<Traversal<Value, Value>>) -> Traversal<In, Value> {
        self.add_step(branch::OrStep::new(subs))
    }

    // -------------------------------------------------------------------------
    // Branch steps using anonymous traversals
    // -------------------------------------------------------------------------

    /// Execute multiple branches and merge results (for anonymous traversals).
    ///
    /// All branches receive each input traverser; results are merged.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that executes multiple branches
    /// let anon = Traversal::<Value, Value>::new()
    ///     .union(vec![__.out(), __.in_()]);
    /// let neighbors = g.v().append(anon).to_list();
    /// ```
    pub fn union(self, branches: Vec<Traversal<Value, Value>>) -> Traversal<In, Value> {
        self.add_step(branch::UnionStep::new(branches))
    }

    /// Try branches in order, return first non-empty result (for anonymous traversals).
    ///
    /// Short-circuits on first successful branch.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal that tries branches in order
    /// let anon = Traversal::<Value, Value>::new()
    ///     .coalesce(vec![__.values("nickname"), __.values("name")]);
    /// let names = g.v().append(anon).to_list();
    /// ```
    pub fn coalesce(self, branches: Vec<Traversal<Value, Value>>) -> Traversal<In, Value> {
        self.add_step(branch::CoalesceStep::new(branches))
    }

    /// Conditional branching (for anonymous traversals).
    ///
    /// Evaluates condition; if it produces results, executes if_true, otherwise if_false.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal with conditional branching
    /// let anon = Traversal::<Value, Value>::new()
    ///     .choose(__.has_label("person"), __.out_labels(&["knows"]), __.out());
    /// let results = g.v().append(anon).to_list();
    /// ```
    pub fn choose(
        self,
        condition: Traversal<Value, Value>,
        if_true: Traversal<Value, Value>,
        if_false: Traversal<Value, Value>,
    ) -> Traversal<In, Value> {
        self.add_step(branch::ChooseStep::new(condition, if_true, if_false))
    }

    /// Optional traversal with fallback to input (for anonymous traversals).
    ///
    /// If sub-traversal produces results, emit those; otherwise emit input.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal with optional step
    /// let anon = Traversal::<Value, Value>::new()
    ///     .optional(__.out_labels(&["knows"]));
    /// let results = g.v().append(anon).to_list();
    /// ```
    pub fn optional(self, sub: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(branch::OptionalStep::new(sub))
    }

    /// Execute sub-traversal in isolated scope (for anonymous traversals).
    ///
    /// Aggregations operate independently for each input traverser.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create an anonymous traversal with local scope
    /// let anon = Traversal::<Value, Value>::new()
    ///     .local(__.out().limit(1));
    /// let results = g.v().append(anon).to_list();
    /// ```
    pub fn local(self, sub: Traversal<Value, Value>) -> Traversal<In, Value> {
        self.add_step(branch::LocalStep::new(sub))
    }

    // -------------------------------------------------------------------------
    // Mutation Steps
    // -------------------------------------------------------------------------

    /// Add or update a property on the current element.
    ///
    /// This step modifies the current traverser's element (vertex or edge)
    /// by setting a property value. For pending vertex/edge creations,
    /// the property is accumulated. For existing elements, a pending
    /// mutation is created.
    ///
    /// The actual property update happens when the traversal is executed
    /// via `MutationExecutor`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Chain properties after add_v
    /// let vertex = g.add_v("person")
    ///     .property("name", "Alice")
    ///     .property("age", 30);
    ///
    /// // Update properties on existing vertices
    /// let updated = g.v_id(id).property("status", "active");
    /// ```
    pub fn property(self, key: impl Into<String>, value: impl Into<Value>) -> Traversal<In, Value> {
        self.add_step(mutation::PropertyStep::new(key, value))
    }

    /// Delete the current element (vertex or edge).
    ///
    /// When a vertex is dropped, all its incident edges are also dropped.
    /// The actual deletion happens when the traversal is executed via
    /// `MutationExecutor`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Drop specific vertices
    /// let deleted = g.v_id(id).drop();
    ///
    /// // Drop vertices matching criteria
    /// let deleted = g.v().has_label("temp").drop();
    /// ```
    pub fn drop(self) -> Traversal<In, Value> {
        self.add_step(mutation::DropStep::new())
    }
}

// Re-export Predicate trait and p module
pub use predicate::p;
pub use predicate::Predicate;

/// Anonymous traversal factory module.
///
/// The `__` module provides factory functions for creating anonymous traversals.
/// Anonymous traversals are unbound traversal fragments that receive their input
/// at execution time when spliced into a parent traversal.
///
/// # Naming Convention
///
/// The double underscore `__` is a Gremlin convention that clearly distinguishes
/// anonymous traversal fragments from bound traversals that start from `g.v()` or `g.e()`.
///
/// # Usage
///
/// Anonymous traversals are used with steps that accept sub-traversals:
/// - `where_()` - Filter based on sub-traversal existence
/// - `union()` - Execute multiple branches and merge results
/// - `coalesce()` - Try branches until one succeeds
/// - `choose()` - Conditional branching
/// - `repeat()` - Iterative traversal
///
/// # Example
///
/// ```ignore
/// use rustgremlin::traversal::__;
///
/// // Create an anonymous traversal
/// let knows_bob = __::out_labels(&["knows"]).has_value("name", "Bob");
///
/// // Use in a parent traversal
/// let people_who_know_bob = g.v()
///     .has_label("person")
///     .where_(knows_bob)
///     .to_list();
///
/// // Factory functions can also be chained
/// let complex = __::out()
///     .has_label("person")
///     .values("name");
/// ```
///
/// # Return Type
///
/// All factory functions return `Traversal<Value, Value>`, making them
/// composable with any parent traversal expecting `Value` input.
pub mod __ {
    use crate::traversal::context::ExecutionContext;
    use crate::traversal::filter::{
        CoinStep, DedupByKeyStep, DedupByLabelStep, DedupByTraversalStep, DedupStep, FilterStep,
        HasIdStep, HasKeyStep, HasLabelStep, HasNotStep, HasPropValueStep, HasStep, HasValueStep,
        HasWhereStep, LimitStep, RangeStep, SampleStep, SkipStep, TailStep,
    };
    use crate::traversal::navigation::{
        BothEStep, BothStep, BothVStep, InEStep, InStep, InVStep, OtherVStep, OutEStep, OutStep,
        OutVStep,
    };
    use crate::traversal::predicate::Predicate;
    use crate::traversal::step::IdentityStep;
    use crate::traversal::transform::{
        AsStep, ConstantStep, ElementMapStep, FlatMapStep, IdStep, LabelStep, MapStep,
        OrderBuilder, PathStep, ProjectBuilder, PropertiesStep, SelectStep, UnfoldStep,
        ValueMapStep, ValuesStep,
    };
    use crate::traversal::Traversal;
    use crate::value::Value;

    // -------------------------------------------------------------------------
    // Identity
    // -------------------------------------------------------------------------

    /// Create an identity traversal that passes input through unchanged.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let anon = __::identity();
    /// // Equivalent to no-op, but useful as a placeholder or in union branches
    /// ```
    #[inline]
    pub fn identity() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(IdentityStep::new())
    }

    // -------------------------------------------------------------------------
    // Navigation - Vertex to Vertex
    // -------------------------------------------------------------------------

    /// Traverse to outgoing adjacent vertices.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let friends = __::out();
    /// ```
    #[inline]
    pub fn out() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(OutStep::new())
    }

    /// Traverse to outgoing adjacent vertices via edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let friends = __::out_labels(&["knows", "likes"]);
    /// ```
    pub fn out_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(OutStep::with_labels(labels))
    }

    /// Traverse to incoming adjacent vertices.
    ///
    /// Note: Named `in_` to avoid conflict with Rust's `in` keyword.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let known_by = __::in_();
    /// ```
    #[inline]
    pub fn in_() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(InStep::new())
    }

    /// Traverse to incoming adjacent vertices via edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let known_by = __::in_labels(&["knows"]);
    /// ```
    pub fn in_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(InStep::with_labels(labels))
    }

    /// Traverse to adjacent vertices in both directions.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let neighbors = __::both();
    /// ```
    #[inline]
    pub fn both() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(BothStep::new())
    }

    /// Traverse to adjacent vertices in both directions via edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let connected = __::both_labels(&["knows"]);
    /// ```
    pub fn both_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(BothStep::with_labels(labels))
    }

    // -------------------------------------------------------------------------
    // Navigation - Vertex to Edge
    // -------------------------------------------------------------------------

    /// Traverse to outgoing edges.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let edges = __::out_e();
    /// ```
    #[inline]
    pub fn out_e() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(OutEStep::new())
    }

    /// Traverse to outgoing edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let knows_edges = __::out_e_labels(&["knows"]);
    /// ```
    pub fn out_e_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(OutEStep::with_labels(labels))
    }

    /// Traverse to incoming edges.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let edges = __::in_e();
    /// ```
    #[inline]
    pub fn in_e() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(InEStep::new())
    }

    /// Traverse to incoming edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let known_by_edges = __::in_e_labels(&["knows"]);
    /// ```
    pub fn in_e_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(InEStep::with_labels(labels))
    }

    /// Traverse to all incident edges (both directions).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let all_edges = __::both_e();
    /// ```
    #[inline]
    pub fn both_e() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(BothEStep::new())
    }

    /// Traverse to all incident edges with given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let knows_edges = __::both_e_labels(&["knows"]);
    /// ```
    pub fn both_e_labels(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(BothEStep::with_labels(labels))
    }

    // -------------------------------------------------------------------------
    // Navigation - Edge to Vertex
    // -------------------------------------------------------------------------

    /// Get the source (outgoing) vertex of an edge.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sources = __::out_v();
    /// ```
    #[inline]
    pub fn out_v() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(OutVStep::new())
    }

    /// Get the target (incoming) vertex of an edge.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let targets = __::in_v();
    /// ```
    #[inline]
    pub fn in_v() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(InVStep::new())
    }

    /// Get both vertices of an edge.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let endpoints = __::both_v();
    /// ```
    #[inline]
    pub fn both_v() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(BothVStep::new())
    }

    /// Get the "other" vertex of an edge.
    ///
    /// When traversing from a vertex to an edge, `other_v()` returns the
    /// vertex at the opposite end from where the traverser came from.
    /// Requires path tracking to be enabled.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let others = __::other_v();
    /// ```
    #[inline]
    pub fn other_v() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(OtherVStep::new())
    }

    // -------------------------------------------------------------------------
    // Filter Steps
    // -------------------------------------------------------------------------

    /// Filter elements by label.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let people = __::has_label("person");
    /// ```
    pub fn has_label(label: impl Into<String>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(HasLabelStep::single(label))
    }

    /// Filter elements by any of the given labels.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let entities = __::has_label_any(&["person", "company"]);
    /// ```
    pub fn has_label_any(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(HasLabelStep::new(labels))
    }

    /// Filter elements by property existence.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let with_age = __::has("age");
    /// ```
    pub fn has(key: impl Into<String>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(HasStep::new(key))
    }

    /// Filter elements by property absence.
    ///
    /// Keeps only vertices/edges that do NOT have the specified property.
    /// Non-element values pass through since they don't have properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let without_email = __::has_not("email");
    /// ```
    pub fn has_not(key: impl Into<String>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(HasNotStep::new(key))
    }

    /// Filter elements by property value equality.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let alice = __::has_value("name", "Alice");
    /// ```
    pub fn has_value(key: impl Into<String>, value: impl Into<Value>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(HasValueStep::new(key, value))
    }

    /// Filter elements by ID.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let specific = __::has_id(VertexId(1));
    /// ```
    pub fn has_id(id: impl Into<Value>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(HasIdStep::from_value(id))
    }

    /// Filter elements by multiple IDs.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let specific = __::has_ids([VertexId(1), VertexId(2)]);
    /// ```
    pub fn has_ids<I, T>(ids: I) -> Traversal<Value, Value>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>,
    {
        Traversal::<Value, Value>::new().add_step(HasIdStep::from_values(
            ids.into_iter().map(Into::into).collect(),
        ))
    }

    /// Filter elements by property value using a predicate.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::p;
    ///
    /// // Filter to adults
    /// let adults = __::has_where("age", p::gte(18));
    ///
    /// // Filter names starting with "A"
    /// let a_names = __::has_where("name", p::starting_with("A"));
    ///
    /// // Combine predicates
    /// let working_age = __::has_where("age", p::and(p::gte(18), p::lt(65)));
    /// ```
    pub fn has_where(
        key: impl Into<String>,
        predicate: impl Predicate + 'static,
    ) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(HasWhereStep::new(key, predicate))
    }

    /// Filter by testing the current value against a predicate.
    ///
    /// Unlike `has_where()` which tests a property of vertices/edges, `is_()` tests
    /// the traverser's current value directly. This is useful after extracting
    /// property values with `values()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::{__, p};
    ///
    /// // Filter ages greater than 25
    /// let gt_25 = __::is_(p::gt(25));
    /// let adults = g.v().values("age").append(gt_25).to_list();
    ///
    /// // Filter ages in a range
    /// let in_range = __::is_(p::between(20, 40));
    /// ```
    pub fn is_(predicate: impl Predicate + 'static) -> Traversal<Value, Value> {
        use crate::traversal::filter::IsStep;
        Traversal::<Value, Value>::new().add_step(IsStep::new(predicate))
    }

    /// Filter by testing the current value for equality.
    ///
    /// This is a convenience method equivalent to `is_(p::eq(value))`.
    /// Useful after extracting property values with `values()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Filter to ages equal to 29
    /// let age_29 = __::is_eq(29);
    /// let results = g.v().values("age").append(age_29).to_list();
    ///
    /// // Filter to a specific name
    /// let alice = __::is_eq("Alice");
    /// ```
    pub fn is_eq(value: impl Into<Value>) -> Traversal<Value, Value> {
        use crate::traversal::filter::IsStep;
        Traversal::<Value, Value>::new().add_step(IsStep::eq(value))
    }

    /// Filter elements using a custom predicate.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let positive = __::filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 0));
    /// ```
    pub fn filter<F>(predicate: F) -> Traversal<Value, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync + 'static,
    {
        Traversal::<Value, Value>::new().add_step(FilterStep::new(predicate))
    }

    /// Deduplicate traversers by value.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let unique = __::dedup();
    /// ```
    #[inline]
    pub fn dedup() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(DedupStep::new())
    }

    /// Deduplicate traversers by property value.
    ///
    /// Removes duplicates based on a property value extracted from elements.
    /// Only the first occurrence of each unique property value passes through.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let unique_ages = __::dedup_by_key("age");
    /// ```
    #[inline]
    pub fn dedup_by_key(key: impl Into<String>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(DedupByKeyStep::new(key))
    }

    /// Deduplicate traversers by element label.
    ///
    /// Removes duplicates based on element label. Only the first occurrence
    /// of each unique label passes through.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let one_per_label = __::dedup_by_label();
    /// ```
    #[inline]
    pub fn dedup_by_label() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(DedupByLabelStep::new())
    }

    /// Deduplicate traversers by sub-traversal result.
    ///
    /// Executes the given sub-traversal for each element and uses the first
    /// result as the deduplication key.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Dedup by out-degree
    /// let unique_outdegree = __::dedup_by(__::out().count());
    /// ```
    #[inline]
    pub fn dedup_by(sub: Traversal<Value, Value>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(DedupByTraversalStep::new(sub))
    }

    /// Limit the number of traversers.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let first_ten = __::limit(10);
    /// ```
    #[inline]
    pub fn limit(count: usize) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(LimitStep::new(count))
    }

    /// Skip the first n traversers.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let after_ten = __::skip(10);
    /// ```
    #[inline]
    pub fn skip(count: usize) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(SkipStep::new(count))
    }

    /// Select traversers within a range.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let page = __::range(10, 20);
    /// ```
    #[inline]
    pub fn range(start: usize, end: usize) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(RangeStep::new(start, end))
    }

    /// Filter to only simple paths (no repeated elements).
    ///
    /// A simple path visits each element at most once.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let simple = __::simple_path();
    /// ```
    #[inline]
    pub fn simple_path() -> Traversal<Value, Value> {
        use crate::traversal::filter::SimplePathStep;
        Traversal::<Value, Value>::new().add_step(SimplePathStep::new())
    }

    /// Filter to only cyclic paths (at least one repeated element).
    ///
    /// A cyclic path contains at least one element that appears more than once.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cyclic = __::cyclic_path();
    /// ```
    #[inline]
    pub fn cyclic_path() -> Traversal<Value, Value> {
        use crate::traversal::filter::CyclicPathStep;
        Traversal::<Value, Value>::new().add_step(CyclicPathStep::new())
    }

    /// Return only the last element from the traversal.
    ///
    /// This is a **barrier step** - it must collect all elements to determine
    /// which is the last. Equivalent to `tail_n(1)`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let last = __::tail();
    /// ```
    #[inline]
    pub fn tail() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(TailStep::last())
    }

    /// Return only the last n elements from the traversal.
    ///
    /// This is a **barrier step** - it must collect all elements to determine
    /// which are the last n. Elements are returned in their original order.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let last_three = __::tail_n(3);
    /// ```
    #[inline]
    pub fn tail_n(count: usize) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(TailStep::new(count))
    }

    /// Probabilistic filter using random coin flip.
    ///
    /// Each traverser has a probability `p` of passing through. Useful for
    /// random sampling or probabilistic traversals.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Random sample of approximately 50%
    /// let sample = __::coin(0.5);
    /// ```
    #[inline]
    pub fn coin(probability: f64) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(CoinStep::new(probability))
    }

    /// Randomly sample n elements using reservoir sampling.
    ///
    /// This is a **barrier step** that collects all input elements and returns
    /// a random sample of exactly n elements. If the input has fewer than n
    /// elements, all elements are returned.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Sample 5 random elements
    /// let sampled = __::sample(5);
    /// ```
    #[inline]
    pub fn sample(count: usize) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(SampleStep::new(count))
    }

    /// Filter property objects by key name.
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a matching "key" field.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Filter to only "name" properties
    /// let names = __::has_key("name");
    /// ```
    #[inline]
    pub fn has_key(key: impl Into<String>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(HasKeyStep::new(key))
    }

    /// Filter property objects by any of the specified key names.
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a "key" field matching any of the specified keys.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Filter to "name" or "age" properties
    /// let props = __::has_key_any(["name", "age"]);
    /// ```
    #[inline]
    pub fn has_key_any<I, S>(keys: I) -> Traversal<Value, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Traversal::<Value, Value>::new().add_step(HasKeyStep::any(keys))
    }

    /// Filter property objects by value.
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a matching "value" field.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Filter to properties with value "Alice"
    /// let alice_props = __::has_prop_value("Alice");
    /// ```
    #[inline]
    pub fn has_prop_value(value: impl Into<Value>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(HasPropValueStep::new(value))
    }

    /// Filter property objects by any of the specified values.
    ///
    /// This step filters property maps (from `properties()`) to keep only those
    /// with a "value" field matching any of the specified values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Filter to properties with value "Alice" or "Bob"
    /// let props = __::has_prop_value_any(["Alice", "Bob"]);
    /// ```
    #[inline]
    pub fn has_prop_value_any<I, V>(values: I) -> Traversal<Value, Value>
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        Traversal::<Value, Value>::new().add_step(HasPropValueStep::any(values))
    }

    // -------------------------------------------------------------------------
    // Transform Steps
    // -------------------------------------------------------------------------

    /// Extract property values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let names = __::values("name");
    /// ```
    pub fn values(key: impl Into<String>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(ValuesStep::new(key))
    }

    /// Extract multiple property values.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let data = __::values_multi(["name", "age"]);
    /// ```
    pub fn values_multi<I, S>(keys: I) -> Traversal<Value, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Traversal::<Value, Value>::new().add_step(ValuesStep::from_keys(keys))
    }

    /// Extract all property objects.
    ///
    /// Unlike `values()` which returns just property values, `properties()` returns
    /// the full property including its key as a Map with "key" and "value" entries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let props = __::properties();
    /// // Each result is Value::Map { "key": "name", "value": "Alice" } etc.
    /// ```
    pub fn properties() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(PropertiesStep::new())
    }

    /// Extract specific property objects.
    ///
    /// Unlike `values()` which returns just property values, `properties_keys()` returns
    /// the full property including its key as a Map with "key" and "value" entries.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let props = __::properties_keys(&["name", "age"]);
    /// ```
    pub fn properties_keys(keys: &[&str]) -> Traversal<Value, Value> {
        let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(PropertiesStep::with_keys(keys))
    }

    /// Get all properties as a map with list-wrapped values.
    ///
    /// Transforms each element into a `Value::Map` containing all properties.
    /// Property values are wrapped in `Value::List` for multi-property compatibility.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = __::value_map();
    /// // Returns: {"name": ["Alice"], "age": [30]}
    /// ```
    #[inline]
    pub fn value_map() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(ValueMapStep::new())
    }

    /// Get specific properties as a map with list-wrapped values.
    ///
    /// Transforms each element into a `Value::Map` containing only the
    /// specified properties. Property values are wrapped in `Value::List`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = __::value_map_keys(&["name"]);
    /// // Returns: {"name": ["Alice"]}
    /// ```
    pub fn value_map_keys(keys: &[&str]) -> Traversal<Value, Value> {
        let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(ValueMapStep::with_keys(keys))
    }

    /// Get all properties as a map including id and label tokens.
    ///
    /// Returns a `Value::Map` containing all properties plus "id" and "label".
    /// Property values are wrapped in `Value::List`, but tokens are not.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = __::value_map_with_tokens();
    /// // Returns: {"id": 0, "label": "person", "name": ["Alice"], "age": [30]}
    /// ```
    #[inline]
    pub fn value_map_with_tokens() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(ValueMapStep::new().with_tokens())
    }

    /// Get complete element representation as a map.
    ///
    /// Transforms each element into a `Value::Map` with id, label, and all
    /// properties. Unlike `value_map()`, property values are NOT wrapped in lists.
    /// For edges, also includes "IN" and "OUT" vertex references.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = __::element_map();
    /// // Vertex: {"id": 0, "label": "person", "name": "Alice", "age": 30}
    /// // Edge: {"id": 0, "label": "knows", "IN": {...}, "OUT": {...}, "since": 2020}
    /// ```
    #[inline]
    pub fn element_map() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(ElementMapStep::new())
    }

    /// Get element representation with specific properties.
    ///
    /// Like `element_map()`, but includes only the specified properties
    /// along with the id, label, and (for edges) IN/OUT references.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let maps = __::element_map_keys(&["name"]);
    /// // Returns: {"id": 0, "label": "person", "name": "Alice"}
    /// ```
    pub fn element_map_keys(keys: &[&str]) -> Traversal<Value, Value> {
        let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(ElementMapStep::with_keys(keys))
    }

    /// Unroll collections into individual elements.
    ///
    /// - `Value::List`: Each element becomes a separate traverser
    /// - `Value::Map`: Each key-value pair becomes a single-entry map traverser
    /// - Non-collection values pass through unchanged
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Unfold a list
    /// let unfolded = __::unfold();
    ///
    /// // Use in pipeline
    /// let entries = g.v().value_map().unfold().to_list();
    /// // Each property entry becomes a separate traverser
    /// ```
    #[inline]
    pub fn unfold() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(UnfoldStep::new())
    }

    /// Calculate the arithmetic mean (average) of numeric values.
    ///
    /// This is a **barrier step** - it collects ALL input values before producing
    /// a single output. Only numeric values (`Value::Int` and `Value::Float`) are
    /// included in the calculation; non-numeric values are silently ignored.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Use in branch to calculate average
    /// let avg = __::mean();
    ///
    /// // As part of a larger traversal
    /// let avg_ages = g.v().has_label("person")
    ///     .values("age")
    ///     .append(__::mean())
    ///     .to_list();
    /// ```
    #[inline]
    pub fn mean() -> Traversal<Value, Value> {
        use crate::traversal::transform::MeanStep;
        Traversal::<Value, Value>::new().add_step(MeanStep::new())
    }

    /// Sort traversers using a fluent builder.
    ///
    /// This is a **barrier step** - it collects ALL input before producing sorted output.
    /// Returns an `OrderBuilder` for configuring sort keys.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Sort by natural order
    /// let sorted = __::order().build();
    ///
    /// // Sort by property
    /// let sorted = __::order().by_key_desc("age").build();
    /// ```
    pub fn order() -> OrderBuilder<Value> {
        OrderBuilder::new(vec![])
    }

    /// Evaluate a mathematical expression.
    ///
    /// The expression can reference the current value using `_` and labeled
    /// path values using their label names. Use `by()` to specify which
    /// property to extract from labeled elements.
    ///
    /// Uses the `mathexpr` crate for full expression parsing and evaluation,
    /// supporting:
    /// - Operators: `+`, `-`, `*`, `/`, `%`, `^`
    /// - Functions: `sqrt`, `abs`, `sin`, `cos`, `tan`, `log`, `exp`, `pow`, `min`, `max`, etc.
    /// - Constants: `pi`, `e`
    /// - Parentheses for grouping
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Double current values
    /// let doubled = __::math("_ * 2").build();
    ///
    /// // Calculate square root of sum
    /// let sqrt = __::math("sqrt(_ ^ 2 + 1)").build();
    ///
    /// // With labeled path values (requires by() for each variable)
    /// let diff = __::math("a - b")
    ///     .by("a", "age")
    ///     .by("b", "age")
    ///     .build();
    /// ```
    pub fn math(expression: &str) -> crate::traversal::transform::MathBuilder<Value> {
        crate::traversal::transform::MathBuilder::new(vec![], expression)
    }

    /// Create a projection with named keys.
    ///
    /// The `project()` step creates a map with specific named keys. Each key's value
    /// is defined by a `by()` modulator, which can extract a property or execute
    /// a sub-traversal.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// .project('name', 'age', 'friends')
    ///   .by('name')
    ///   .by('age')
    ///   .by(out('knows').count())
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// use __; // Anonymous traversal module
    ///
    /// // Use in a where clause to project data
    /// let projection = __::project(&["name", "friend_count"])
    ///     .by_key("name")
    ///     .by(__::out("knows").count())
    ///     .build();
    /// ```
    ///
    /// # Arguments
    ///
    /// * `keys` - The keys for the projection map
    ///
    /// # Returns
    ///
    /// A `ProjectBuilder` that requires `by()` clauses to be added for each key.
    pub fn project(keys: &[&str]) -> ProjectBuilder<Value> {
        let key_strings: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        ProjectBuilder::new(vec![], key_strings)
    }

    /// Group traversers by a key and collect values.
    ///
    /// The `group()` step is a **barrier step** that collects all input traversers,
    /// groups them by a key, and produces a single `Value::Map` output.
    ///
    /// # Gremlin Equivalent
    ///
    /// ```groovy
    /// .group().by(label)  // Group by label
    /// .group().by("age").by("name")  // Group by age, collect names
    /// ```
    ///
    /// # Example
    ///
    /// ```ignore
    /// use __; // Anonymous traversal module
    ///
    /// // Group by label
    /// let groups = __::group().by_label().by_value().build();
    ///
    /// // Group by property
    /// let groups = __::group().by_key("age").by_value_key("name").build();
    /// ```
    ///
    /// # Returns
    ///
    /// A `GroupBuilder` that allows configuring the grouping key and value collector.
    pub fn group() -> crate::traversal::aggregate::GroupBuilder<Value> {
        use crate::traversal::aggregate::GroupBuilder;
        GroupBuilder::new(vec![])
    }

    /// Count traversers grouped by a key (anonymous traversal factory).
    ///
    /// Creates a `GroupCountBuilder` for use in anonymous traversals.
    /// The result is a single `Value::Map` where keys are the grouping keys
    /// and values are integer counts.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Count by label
    /// let count_step = __::group_count().by_label().build();
    ///
    /// // Count by property
    /// let age_count_step = __::group_count().by_key("age").build();
    /// ```
    ///
    /// # Returns
    ///
    /// A `GroupCountBuilder` that allows configuring the grouping key.
    pub fn group_count() -> crate::traversal::aggregate::GroupCountBuilder<Value> {
        use crate::traversal::aggregate::GroupCountBuilder;
        GroupCountBuilder::new(vec![])
    }

    /// Extract the element ID.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let ids = __::id();
    /// ```
    #[inline]
    pub fn id() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(IdStep::new())
    }

    /// Extract the element label.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let labels = __::label();
    /// ```
    #[inline]
    pub fn label() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(LabelStep::new())
    }

    /// Replace values with a constant.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let markers = __::constant("found");
    /// ```
    pub fn constant(value: impl Into<Value>) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(ConstantStep::new(value))
    }

    /// Convert the path to a list.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let paths = __::path();
    /// ```
    #[inline]
    pub fn path() -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(PathStep::new())
    }

    /// Transform values using a closure.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let doubled = __::map(|_ctx, v| {
    ///     if let Value::Int(n) = v {
    ///         Value::Int(n * 2)
    ///     } else {
    ///         v.clone()
    ///     }
    /// });
    /// ```
    pub fn map<F>(f: F) -> Traversal<Value, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static,
    {
        Traversal::<Value, Value>::new().add_step(MapStep::new(f))
    }

    /// Transform values to multiple values using a closure.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let expanded = __::flat_map(|_ctx, v| {
    ///     if let Value::Int(n) = v {
    ///         (0..*n).map(Value::Int).collect()
    ///     } else {
    ///         vec![]
    ///     }
    /// });
    /// ```
    pub fn flat_map<F>(f: F) -> Traversal<Value, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync + 'static,
    {
        Traversal::<Value, Value>::new().add_step(FlatMapStep::new(f))
    }

    /// Label the current position in the path.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let labeled = __::as_("start");
    /// ```
    pub fn as_(label: &str) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(AsStep::new(label))
    }

    /// Select multiple labeled values from the path.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let selected = __::select(&["a", "b"]);
    /// ```
    pub fn select(labels: &[&str]) -> Traversal<Value, Value> {
        let labels: Vec<String> = labels.iter().map(|s| s.to_string()).collect();
        Traversal::<Value, Value>::new().add_step(SelectStep::new(labels))
    }

    /// Select a single labeled value from the path.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let selected = __::select_one("start");
    /// ```
    pub fn select_one(label: &str) -> Traversal<Value, Value> {
        Traversal::<Value, Value>::new().add_step(SelectStep::single(label))
    }

    // -------------------------------------------------------------------------
    // Filter Steps using Anonymous Traversals
    // -------------------------------------------------------------------------

    /// Filter by sub-traversal existence.
    ///
    /// Emits input traverser only if the sub-traversal produces at least one result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Keep only vertices that have outgoing edges
    /// let with_out = __::where_(__.out());
    /// ```
    pub fn where_(sub: Traversal<Value, Value>) -> Traversal<Value, Value> {
        use crate::traversal::branch::WhereStep;
        Traversal::<Value, Value>::new().add_step(WhereStep::new(sub))
    }

    /// Filter by sub-traversal non-existence.
    ///
    /// Emits input traverser only if the sub-traversal produces NO results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Keep only leaf vertices (no outgoing edges)
    /// let leaves = __::not(__.out());
    /// ```
    pub fn not(sub: Traversal<Value, Value>) -> Traversal<Value, Value> {
        use crate::traversal::branch::NotStep;
        Traversal::<Value, Value>::new().add_step(NotStep::new(sub))
    }

    /// Filter by multiple sub-traversals (AND logic).
    ///
    /// Emits input traverser only if ALL sub-traversals produce at least one result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Keep vertices that have both outgoing AND incoming edges
    /// let connected = __::and_(vec![__.out(), __.in_()]);
    /// ```
    pub fn and_(subs: Vec<Traversal<Value, Value>>) -> Traversal<Value, Value> {
        use crate::traversal::branch::AndStep;
        Traversal::<Value, Value>::new().add_step(AndStep::new(subs))
    }

    /// Filter by multiple sub-traversals (OR logic).
    ///
    /// Emits input traverser if ANY sub-traversal produces at least one result.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Keep vertices that are either "person" OR "software"
    /// let entities = __::or_(vec![__.has_label("person"), __.has_label("software")]);
    /// ```
    pub fn or_(subs: Vec<Traversal<Value, Value>>) -> Traversal<Value, Value> {
        use crate::traversal::branch::OrStep;
        Traversal::<Value, Value>::new().add_step(OrStep::new(subs))
    }

    // -------------------------------------------------------------------------
    // Branch Steps using Anonymous Traversals
    // -------------------------------------------------------------------------

    /// Execute multiple branches and merge results.
    ///
    /// All branches receive each input traverser; results are merged.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Get neighbors in both directions
    /// let neighbors = __::union(vec![__.out(), __.in_()]);
    /// ```
    pub fn union(branches: Vec<Traversal<Value, Value>>) -> Traversal<Value, Value> {
        use crate::traversal::branch::UnionStep;
        Traversal::<Value, Value>::new().add_step(UnionStep::new(branches))
    }

    /// Try branches in order, return first non-empty result.
    ///
    /// Short-circuits on first successful branch.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Try to get nickname, fall back to name
    /// let names = __::coalesce(vec![__.values("nickname"), __.values("name")]);
    /// ```
    pub fn coalesce(branches: Vec<Traversal<Value, Value>>) -> Traversal<Value, Value> {
        use crate::traversal::branch::CoalesceStep;
        Traversal::<Value, Value>::new().add_step(CoalesceStep::new(branches))
    }

    /// Conditional branching.
    ///
    /// Evaluates condition; if it produces results, executes if_true, otherwise if_false.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // If person, get friends; otherwise get all neighbors
    /// let results = __::choose(__.has_label("person"), __.out_labels(&["knows"]), __.out());
    /// ```
    pub fn choose(
        condition: Traversal<Value, Value>,
        if_true: Traversal<Value, Value>,
        if_false: Traversal<Value, Value>,
    ) -> Traversal<Value, Value> {
        use crate::traversal::branch::ChooseStep;
        Traversal::<Value, Value>::new().add_step(ChooseStep::new(condition, if_true, if_false))
    }

    /// Optional traversal with fallback to input.
    ///
    /// If sub-traversal produces results, emit those; otherwise emit input.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Try to traverse to friends, keep original if none found
    /// let results = __::optional(__.out_labels(&["knows"]));
    /// ```
    pub fn optional(sub: Traversal<Value, Value>) -> Traversal<Value, Value> {
        use crate::traversal::branch::OptionalStep;
        Traversal::<Value, Value>::new().add_step(OptionalStep::new(sub))
    }

    /// Execute sub-traversal in isolated scope.
    ///
    /// Aggregations operate independently for each input traverser.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Count neighbors per vertex
    /// let counts = __::local(__.out().limit(1));
    /// ```
    pub fn local(sub: Traversal<Value, Value>) -> Traversal<Value, Value> {
        use crate::traversal::branch::LocalStep;
        Traversal::<Value, Value>::new().add_step(LocalStep::new(sub))
    }

    // -------------------------------------------------------------------------
    // Mutation Steps
    // -------------------------------------------------------------------------

    /// Create a new vertex with the specified label.
    ///
    /// This is a **spawning step** - it produces a traverser for the newly
    /// created vertex, ignoring any input traversers. The actual vertex
    /// creation happens when the traversal is executed via `MutationExecutor`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Create a pending vertex (actual creation happens at execution time)
    /// let vertex_traversal = __::add_v("person")
    ///     .property("name", "Alice")
    ///     .property("age", 30);
    /// ```
    pub fn add_v(label: impl Into<String>) -> Traversal<Value, Value> {
        use crate::traversal::mutation::AddVStep;
        Traversal::<Value, Value>::new().add_step(AddVStep::new(label))
    }

    /// Create a new edge with the specified label.
    ///
    /// This step requires both `from` and `to` endpoints to be specified
    /// using the builder methods on the returned step. The actual edge
    /// creation happens when the traversal is executed via `MutationExecutor`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    /// use rustgremlin::value::VertexId;
    ///
    /// // Create a pending edge between two vertices
    /// let edge_step = __::add_e("knows")
    ///     .from_vertex(VertexId(1))
    ///     .to_vertex(VertexId(2))
    ///     .property("since", 2020);
    /// ```
    pub fn add_e(label: impl Into<String>) -> crate::traversal::mutation::AddEStep {
        crate::traversal::mutation::AddEStep::new(label)
    }

    /// Add or update a property on the current element.
    ///
    /// This step modifies the current traverser's element (vertex or edge)
    /// by setting a property value. The actual property update happens
    /// when the traversal is executed via `MutationExecutor`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Add a property to current element
    /// let with_name = __::property("name", "Alice");
    /// ```
    pub fn property(key: impl Into<String>, value: impl Into<Value>) -> Traversal<Value, Value> {
        use crate::traversal::mutation::PropertyStep;
        Traversal::<Value, Value>::new().add_step(PropertyStep::new(key, value))
    }

    /// Delete the current element (vertex or edge).
    ///
    /// When a vertex is dropped, all its incident edges are also dropped.
    /// The actual deletion happens when the traversal is executed via
    /// `MutationExecutor`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use rustgremlin::traversal::__;
    ///
    /// // Mark elements for deletion
    /// let deleted = __::drop();
    /// ```
    pub fn drop() -> Traversal<Value, Value> {
        use crate::traversal::mutation::DropStep;
        Traversal::<Value, Value>::new().add_step(DropStep::new())
    }
}

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
        fn loops_returns_loop_count() {
            let mut t = Traverser::new(Value::Null);
            assert_eq!(t.loops(), 0);

            t.inc_loops();
            assert_eq!(t.loops(), 1);

            t.inc_loops();
            t.inc_loops();
            assert_eq!(t.loops(), 3);
        }

        #[test]
        fn split_preserves_loop_count() {
            let mut t = Traverser::from_vertex(VertexId(1));
            t.inc_loops();
            t.inc_loops();
            t.inc_loops();
            assert_eq!(t.loops(), 3);

            let t2 = t.split(Value::Vertex(VertexId(2)));
            assert_eq!(t2.loops(), 3);
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

    mod anonymous_traversal_factory_tests {
        use super::*;
        use crate::graph::Graph;
        use crate::storage::InMemoryGraph;
        use crate::traversal::step::execute_traversal_from;
        use std::collections::HashMap;
        use std::sync::Arc;

        fn create_test_graph() -> Graph {
            let mut storage = InMemoryGraph::new();

            // Add vertices
            let alice_id = storage.add_vertex(
                "person",
                HashMap::from([
                    ("name".to_string(), Value::String("Alice".to_string())),
                    ("age".to_string(), Value::Int(30)),
                ]),
            );
            let bob_id = storage.add_vertex(
                "person",
                HashMap::from([
                    ("name".to_string(), Value::String("Bob".to_string())),
                    ("age".to_string(), Value::Int(25)),
                ]),
            );
            let company_id = storage.add_vertex(
                "company",
                HashMap::from([("name".to_string(), Value::String("Acme".to_string()))]),
            );

            // Add edges
            let _ = storage.add_edge(alice_id, bob_id, "knows", HashMap::new());
            let _ = storage.add_edge(alice_id, company_id, "works_at", HashMap::new());

            Graph::new(Arc::new(storage))
        }

        // -------------------------------------------------------------------------
        // Identity tests
        // -------------------------------------------------------------------------

        #[test]
        fn identity_creates_traversal_with_identity_step() {
            let t = __::identity();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["identity"]);
            assert!(!t.has_source());
        }

        #[test]
        fn identity_passes_through_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let anon = __::identity();
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let results: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            assert_eq!(results.len(), 3);
            assert_eq!(results[0].value, Value::Int(1));
            assert_eq!(results[1].value, Value::Int(2));
            assert_eq!(results[2].value, Value::Int(3));
        }

        // -------------------------------------------------------------------------
        // Navigation tests
        // -------------------------------------------------------------------------

        #[test]
        fn out_creates_traversal_with_out_step() {
            let t = __::out();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["out"]);
        }

        #[test]
        fn out_labels_creates_traversal_with_labels() {
            let t = __::out_labels(&["knows", "likes"]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["out"]);
        }

        #[test]
        fn in_creates_traversal_with_in_step() {
            let t = __::in_();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["in"]);
        }

        #[test]
        fn in_labels_creates_traversal_with_labels() {
            let t = __::in_labels(&["knows"]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["in"]);
        }

        #[test]
        fn both_creates_traversal_with_both_step() {
            let t = __::both();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["both"]);
        }

        #[test]
        fn both_labels_creates_traversal_with_labels() {
            let t = __::both_labels(&["knows"]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["both"]);
        }

        #[test]
        fn out_e_creates_traversal_with_out_e_step() {
            let t = __::out_e();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["outE"]);
        }

        #[test]
        fn out_e_labels_creates_traversal_with_labels() {
            let t = __::out_e_labels(&["knows"]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["outE"]);
        }

        #[test]
        fn in_e_creates_traversal_with_in_e_step() {
            let t = __::in_e();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["inE"]);
        }

        #[test]
        fn in_e_labels_creates_traversal_with_labels() {
            let t = __::in_e_labels(&["knows"]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["inE"]);
        }

        #[test]
        fn both_e_creates_traversal_with_both_e_step() {
            let t = __::both_e();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["bothE"]);
        }

        #[test]
        fn both_e_labels_creates_traversal_with_labels() {
            let t = __::both_e_labels(&["knows"]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["bothE"]);
        }

        #[test]
        fn out_v_creates_traversal_with_out_v_step() {
            let t = __::out_v();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["outV"]);
        }

        #[test]
        fn in_v_creates_traversal_with_in_v_step() {
            let t = __::in_v();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["inV"]);
        }

        #[test]
        fn both_v_creates_traversal_with_both_v_step() {
            let t = __::both_v();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["bothV"]);
        }

        // -------------------------------------------------------------------------
        // Filter tests
        // -------------------------------------------------------------------------

        #[test]
        fn has_label_creates_traversal_with_has_label_step() {
            let t = __::has_label("person");
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["hasLabel"]);
        }

        #[test]
        fn has_label_any_creates_traversal_with_multiple_labels() {
            let t = __::has_label_any(&["person", "company"]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["hasLabel"]);
        }

        #[test]
        fn has_creates_traversal_with_has_step() {
            let t = __::has("name");
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["has"]);
        }

        #[test]
        fn has_value_creates_traversal_with_has_value_step() {
            let t = __::has_value("name", "Alice");
            assert_eq!(t.step_count(), 1);
            // HasValueStep reports as "has" since it's the has(key, value) variant
            assert_eq!(t.step_names(), vec!["has"]);
        }

        #[test]
        fn has_id_creates_traversal_with_has_id_step() {
            let t = __::has_id(VertexId(1));
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["hasId"]);
        }

        #[test]
        fn has_ids_creates_traversal_with_multiple_ids() {
            let t = __::has_ids([VertexId(1), VertexId(2)]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["hasId"]);
        }

        #[test]
        fn filter_creates_traversal_with_filter_step() {
            let t = __::filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 0));
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["filter"]);
        }

        #[test]
        fn dedup_creates_traversal_with_dedup_step() {
            let t = __::dedup();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["dedup"]);
        }

        #[test]
        fn limit_creates_traversal_with_limit_step() {
            let t = __::limit(10);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["limit"]);
        }

        #[test]
        fn skip_creates_traversal_with_skip_step() {
            let t = __::skip(5);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["skip"]);
        }

        #[test]
        fn range_creates_traversal_with_range_step() {
            let t = __::range(10, 20);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["range"]);
        }

        // -------------------------------------------------------------------------
        // Transform tests
        // -------------------------------------------------------------------------

        #[test]
        fn values_creates_traversal_with_values_step() {
            let t = __::values("name");
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["values"]);
        }

        #[test]
        fn values_multi_creates_traversal_with_multiple_keys() {
            let t = __::values_multi(["name", "age"]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["values"]);
        }

        #[test]
        fn id_creates_traversal_with_id_step() {
            let t = __::id();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["id"]);
        }

        #[test]
        fn label_creates_traversal_with_label_step() {
            let t = __::label();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["label"]);
        }

        #[test]
        fn constant_creates_traversal_with_constant_step() {
            let t = __::constant("found");
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["constant"]);
        }

        #[test]
        fn path_creates_traversal_with_path_step() {
            let t = __::path();
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["path"]);
        }

        #[test]
        fn map_creates_traversal_with_map_step() {
            let t = __::map(|_ctx, v| v.clone());
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["map"]);
        }

        #[test]
        fn flat_map_creates_traversal_with_flat_map_step() {
            let t = __::flat_map(|_ctx, _v| vec![]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["flatMap"]);
        }

        #[test]
        fn as_creates_traversal_with_as_step() {
            let t = __::as_("start");
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["as"]);
        }

        #[test]
        fn select_creates_traversal_with_select_step() {
            let t = __::select(&["a", "b"]);
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["select"]);
        }

        #[test]
        fn select_one_creates_traversal_with_select_step() {
            let t = __::select_one("start");
            assert_eq!(t.step_count(), 1);
            assert_eq!(t.step_names(), vec!["select"]);
        }

        // -------------------------------------------------------------------------
        // Chaining tests
        // -------------------------------------------------------------------------

        #[test]
        fn anonymous_traversals_can_be_chained() {
            // Start with a factory function and chain additional steps
            let t = __::out().has_label("person").values("name");
            assert_eq!(t.step_count(), 3);
            assert_eq!(t.step_names(), vec!["out", "hasLabel", "values"]);
        }

        #[test]
        fn chained_traversal_has_no_source() {
            let t = __::out().has_label("person").limit(10);
            assert!(!t.has_source());
        }

        #[test]
        fn anonymous_traversal_can_be_appended_to_bound() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Create anonymous traversal
            let anon = __::has_label("person").values("name");

            // Append to bound traversal
            let bound = g.v().append(anon);

            // Execute
            let results = bound.to_list();
            assert_eq!(results.len(), 2); // Alice and Bob
        }

        // -------------------------------------------------------------------------
        // Execution tests
        // -------------------------------------------------------------------------

        #[test]
        fn anonymous_out_traverses_outgoing_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Get Alice's vertex ID
            let alice_id = snapshot
                .storage()
                .all_vertices()
                .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
                .map(|v| v.id)
                .unwrap();

            let anon = __::out();
            let input = vec![Traverser::from_vertex(alice_id)];
            let results: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            // Alice has 2 outgoing edges (knows Bob, works_at Acme)
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn anonymous_has_label_filters_by_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Get all vertices as input
            let input: Vec<_> = snapshot
                .storage()
                .all_vertices()
                .map(|v| Traverser::from_vertex(v.id))
                .collect();
            assert_eq!(input.len(), 3); // Alice, Bob, Acme

            let anon = __::has_label("person");
            let results: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            // Should only have Alice and Bob (persons)
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn anonymous_values_extracts_property() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Get Alice's vertex
            let alice_id = snapshot
                .storage()
                .all_vertices()
                .find(|v| v.properties.get("name") == Some(&Value::String("Alice".to_string())))
                .map(|v| v.id)
                .unwrap();

            let anon = __::values("name");
            let input = vec![Traverser::from_vertex(alice_id)];
            let results: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            assert_eq!(results.len(), 1);
            assert_eq!(results[0].value, Value::String("Alice".to_string()));
        }

        #[test]
        fn anonymous_constant_replaces_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let anon = __::constant("found");
            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];
            let results: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            assert_eq!(results.len(), 2);
            assert_eq!(results[0].value, Value::String("found".to_string()));
            assert_eq!(results[1].value, Value::String("found".to_string()));
        }

        #[test]
        fn anonymous_limit_restricts_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let anon = __::limit(2);
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
            ];
            let results: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            assert_eq!(results.len(), 2);
            assert_eq!(results[0].value, Value::Int(1));
            assert_eq!(results[1].value, Value::Int(2));
        }

        #[test]
        fn anonymous_dedup_removes_duplicates() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let anon = __::dedup();
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(2)),
            ];
            let results: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            assert_eq!(results.len(), 3);
            assert_eq!(results[0].value, Value::Int(1));
            assert_eq!(results[1].value, Value::Int(2));
            assert_eq!(results[2].value, Value::Int(3));
        }

        #[test]
        fn anonymous_filter_applies_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let anon = __::filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 2));
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
            ];
            let results: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            assert_eq!(results.len(), 2);
            assert_eq!(results[0].value, Value::Int(3));
            assert_eq!(results[1].value, Value::Int(4));
        }

        #[test]
        fn anonymous_map_transforms_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let anon = __::map(|_ctx, v| {
                if let Value::Int(n) = v {
                    Value::Int(n * 2)
                } else {
                    v.clone()
                }
            });
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];
            let results: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            assert_eq!(results.len(), 3);
            assert_eq!(results[0].value, Value::Int(2));
            assert_eq!(results[1].value, Value::Int(4));
            assert_eq!(results[2].value, Value::Int(6));
        }

        #[test]
        fn complex_anonymous_traversal_chains_correctly() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = GraphTraversalSource::new(&snapshot, snapshot.interner());

            // Complex chain: start from all vertices, filter to persons,
            // traverse out, get names
            let anon = __::has_label("person")
                .out()
                .has_label("person")
                .values("name");

            // Get Alice who knows Bob
            let results = g.v().append(anon).to_list();

            // Alice -> knows -> Bob, so we should get "Bob"
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::String("Bob".to_string()));
        }

        #[test]
        fn anonymous_traversals_are_reusable() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Create anonymous traversal once
            let anon = __::limit(1);

            // Use it multiple times
            let input1 = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];
            let results1: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input1.into_iter())).collect();
            assert_eq!(results1.len(), 1);

            let input2 = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Int(20)),
            ];
            let results2: Vec<_> =
                execute_traversal_from(&ctx, &anon, Box::new(input2.into_iter())).collect();
            assert_eq!(results2.len(), 1);
            assert_eq!(results2[0].value, Value::Int(10));
        }

        #[test]
        fn anonymous_traversals_are_clonable() {
            let anon = __::out().has_label("person").values("name");
            let cloned = anon.clone();

            assert_eq!(anon.step_count(), cloned.step_count());
            assert_eq!(anon.step_names(), cloned.step_names());
        }

        // ---------------------------------------------------------------------
        // Mutation Factory Function Tests
        // ---------------------------------------------------------------------

        #[test]
        fn add_v_creates_traversal_with_add_v_step() {
            let anon = __::add_v("person");
            assert_eq!(anon.step_count(), 1);
            assert_eq!(anon.step_names(), vec!["addV"]);
        }

        #[test]
        fn add_v_can_chain_property() {
            let anon = __::add_v("person").property("name", "Alice");
            assert_eq!(anon.step_count(), 2);
            assert_eq!(anon.step_names(), vec!["addV", "property"]);
        }

        #[test]
        fn add_v_can_chain_multiple_properties() {
            let anon = __::add_v("person")
                .property("name", "Alice")
                .property("age", 30i64);
            assert_eq!(anon.step_count(), 3);
            assert_eq!(anon.step_names(), vec!["addV", "property", "property"]);
        }

        #[test]
        fn add_e_creates_step_with_correct_label() {
            let step = __::add_e("knows");
            assert_eq!(step.label(), "knows");
            assert!(step.from_endpoint().is_none());
            assert!(step.to_endpoint().is_none());
        }

        #[test]
        fn add_e_builder_chain() {
            use crate::traversal::mutation::EdgeEndpoint;
            use crate::value::VertexId;

            let step = __::add_e("knows")
                .from_vertex(VertexId(1))
                .to_vertex(VertexId(2))
                .property("since", 2020i64);

            assert_eq!(step.label(), "knows");
            assert!(matches!(
                step.from_endpoint(),
                Some(EdgeEndpoint::VertexId(VertexId(1)))
            ));
            assert!(matches!(
                step.to_endpoint(),
                Some(EdgeEndpoint::VertexId(VertexId(2)))
            ));
        }

        #[test]
        fn property_creates_traversal_with_property_step() {
            let anon = __::property("name", "Alice");
            assert_eq!(anon.step_count(), 1);
            assert_eq!(anon.step_names(), vec!["property"]);
        }

        #[test]
        fn drop_creates_traversal_with_drop_step() {
            let anon = __::drop();
            assert_eq!(anon.step_count(), 1);
            assert_eq!(anon.step_names(), vec!["drop"]);
        }

        #[test]
        fn mutation_traversals_can_be_used_in_combinations() {
            // Ensure mutation traversals can be cloned and reused
            let create_vertex = __::add_v("person").property("name", "test");
            let cloned = create_vertex.clone();

            assert_eq!(create_vertex.step_count(), cloned.step_count());
            assert_eq!(create_vertex.step_names(), cloned.step_names());
        }
    }
}
