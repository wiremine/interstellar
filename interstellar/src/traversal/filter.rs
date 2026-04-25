//! Filter steps for graph traversal.
//!
//! This module provides filter steps that pass through or reject traversers
//! based on various predicates. Filter steps are 1:1 operations - each input
//! traverser produces at most one output traverser.
//!
//! # Steps
//!
//! - `HasLabelStep`: Filters elements by label
//! - `HasStep`: Filters elements by property existence
//! - `HasValueStep`: Filters elements by property value equality
//! - `HasIdStep`: Filters elements by ID
//! - `FilterStep`: Generic filter with custom predicate

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::impl_filter_step;
use crate::traversal::step::Step;
use crate::traversal::{ExecutionContext, StreamingContext, Traverser};
use crate::value::{EdgeId, Value, VertexId};

// -----------------------------------------------------------------------------
// HasLabelStep - filter by element label
// -----------------------------------------------------------------------------

/// Filter step that keeps only elements with matching labels.
///
/// Works with both vertices and edges. Non-element values (integers, strings, etc.)
/// are filtered out.
///
/// # Example
///
/// ```ignore
/// // Filter to only "person" vertices
/// let people = g.v().has_label("person").to_list();
///
/// // Filter to vertices with any of the given labels
/// let entities = g.v().has_label_any(&["person", "company"]).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct HasLabelStep {
    /// Labels to match against (element must match any one)
    labels: Vec<String>,
}

impl HasLabelStep {
    /// Create a new HasLabelStep that matches any of the given labels.
    ///
    /// # Arguments
    ///
    /// * `labels` - Labels to match against
    pub fn new(labels: Vec<String>) -> Self {
        Self { labels }
    }

    /// Create a HasLabelStep for a single label.
    ///
    /// # Arguments
    ///
    /// * `label` - The label to match
    pub fn single(label: impl Into<String>) -> Self {
        Self {
            labels: vec![label.into()],
        }
    }

    /// Create a HasLabelStep for multiple labels.
    ///
    /// # Arguments
    ///
    /// * `labels` - Labels to match (element must match any one)
    pub fn any<I, S>(labels: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            labels: labels.into_iter().map(Into::into).collect(),
        }
    }

    /// Check if a traverser's element has a matching label.
    ///
    /// Returns `false` for non-element values (integers, strings, etc.).
    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                // Get the vertex from the snapshot
                if let Some(vertex) = ctx.storage().get_vertex(*id) {
                    self.labels.iter().any(|l| l == &vertex.label)
                } else {
                    false
                }
            }
            Value::Edge(id) => {
                // Get the edge from the snapshot
                if let Some(edge) = ctx.storage().get_edge(*id) {
                    self.labels.iter().any(|l| l == &edge.label)
                } else {
                    false
                }
            }
            // Non-element values don't have labels
            _ => false,
        }
    }

    /// Streaming version of matches for StreamingContext.
    fn matches_streaming(&self, ctx: &StreamingContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                if let Some(vertex) = ctx.storage().get_vertex(*id) {
                    self.labels.iter().any(|l| l == &vertex.label)
                } else {
                    false
                }
            }
            Value::Edge(id) => {
                if let Some(edge) = ctx.storage().get_edge(*id) {
                    self.labels.iter().any(|l| l == &edge.label)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

// Use the macro to implement Step for HasLabelStep
impl_filter_step!(HasLabelStep, "hasLabel", category = crate::traversal::explain::StepCategory::Filter);

// Reactive introspection: expose label constraints for fast-rejection filtering.
#[cfg(feature = "reactive")]
impl crate::traversal::reactive::StepIntrospect for HasLabelStep {
    fn label_constraints(&self) -> Option<Vec<String>> {
        Some(self.labels.clone())
    }
}

// -----------------------------------------------------------------------------
// HasStep - filter by property existence
// -----------------------------------------------------------------------------

/// Filter step that keeps only elements with a specific property.
///
/// Works with both vertices and edges. Non-element values (integers, strings, etc.)
/// are filtered out since they don't have properties.
///
/// # Example
///
/// ```ignore
/// // Filter to only vertices that have an "age" property
/// let with_age = g.v().has("age").to_list();
/// ```
#[derive(Clone, Debug)]
pub struct HasStep {
    /// The property key to check for existence
    key: String,
}

impl HasStep {
    /// Create a new HasStep that checks for property existence.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to check
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    /// Check if a traverser's element has the property.
    ///
    /// Returns `false` for non-element values (integers, strings, etc.).
    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                // Get the vertex from storage and check property existence
                ctx.storage()
                    .get_vertex(*id)
                    .map(|v| v.properties.contains_key(&self.key))
                    .unwrap_or(false)
            }
            Value::Edge(id) => {
                // Get the edge from storage and check property existence
                ctx.storage()
                    .get_edge(*id)
                    .map(|e| e.properties.contains_key(&self.key))
                    .unwrap_or(false)
            }
            // Non-element values don't have properties
            _ => false,
        }
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, ctx: &StreamingContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => ctx
                .storage()
                .get_vertex(*id)
                .map(|v| v.properties.contains_key(&self.key))
                .unwrap_or(false),
            Value::Edge(id) => ctx
                .storage()
                .get_edge(*id)
                .map(|e| e.properties.contains_key(&self.key))
                .unwrap_or(false),
            _ => false,
        }
    }
}

// Use the macro to implement Step for HasStep
impl_filter_step!(HasStep, "has", category = crate::traversal::explain::StepCategory::Filter);

// Reactive introspection: expose property key constraint.
#[cfg(feature = "reactive")]
impl crate::traversal::reactive::StepIntrospect for HasStep {
    fn property_constraints(&self) -> Option<Vec<String>> {
        Some(vec![self.key.clone()])
    }
}

// -----------------------------------------------------------------------------
// HasNotStep - filter by property absence
// -----------------------------------------------------------------------------

/// Filter step that keeps only elements WITHOUT a specific property.
///
/// This is the inverse of `HasStep`. Works with both vertices and edges.
/// Non-element values (integers, strings, etc.) pass through since they
/// don't have properties.
///
/// # Example
///
/// ```ignore
/// // Filter to only vertices that do NOT have an "email" property
/// let without_email = g.v().has_not("email").to_list();
/// ```
#[derive(Clone, Debug)]
pub struct HasNotStep {
    /// The property key to check for absence
    key: String,
}

impl HasNotStep {
    /// Create a new HasNotStep that checks for property absence.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to check for absence
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    /// Check if a traverser's element does NOT have the property.
    ///
    /// Returns `true` for:
    /// - Vertices/edges that do NOT have the property
    /// - Non-existent vertices/edges (they don't have the property)
    /// - Non-element values (integers, strings, etc.) - they pass through
    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                // Get the vertex from storage and check property absence
                ctx.storage()
                    .get_vertex(*id)
                    .map(|v| !v.properties.contains_key(&self.key))
                    .unwrap_or(true) // Vertex not found = no property
            }
            Value::Edge(id) => {
                // Get the edge from storage and check property absence
                ctx.storage()
                    .get_edge(*id)
                    .map(|e| !e.properties.contains_key(&self.key))
                    .unwrap_or(true) // Edge not found = no property
            }
            // Non-element values pass through (they don't have properties)
            _ => true,
        }
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, ctx: &StreamingContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => ctx
                .storage()
                .get_vertex(*id)
                .map(|v| !v.properties.contains_key(&self.key))
                .unwrap_or(true),
            Value::Edge(id) => ctx
                .storage()
                .get_edge(*id)
                .map(|e| !e.properties.contains_key(&self.key))
                .unwrap_or(true),
            _ => true,
        }
    }
}

// Use the macro to implement Step for HasNotStep
impl_filter_step!(HasNotStep, "hasNot", category = crate::traversal::explain::StepCategory::Filter);

// -----------------------------------------------------------------------------
// HasValueStep - filter by property value equality
// -----------------------------------------------------------------------------

/// Filter step that keeps only elements with a specific property value.
///
/// Works with both vertices and edges. Non-element values (integers, strings, etc.)
/// are filtered out since they don't have properties.
///
/// # Example
///
/// ```ignore
/// // Filter to only vertices where name == "Alice"
/// let alice = g.v().has_value("name", "Alice").to_list();
///
/// // Filter to vertices where age == 30
/// let age_30 = g.v().has_value("age", 30i64).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct HasValueStep {
    /// The property key to check
    key: String,
    /// The expected value
    value: Value,
}

impl HasValueStep {
    /// Create a new HasValueStep that checks for property value equality.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to check
    /// * `value` - The expected value
    pub fn new(key: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Check if a traverser's element has the property with the expected value.
    ///
    /// Returns `false` for non-element values (integers, strings, etc.).
    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                // Get the vertex from the snapshot and check property value
                if let Some(vertex) = ctx.storage().get_vertex(*id) {
                    vertex
                        .properties
                        .get(&self.key)
                        .map(|pv| pv == &self.value)
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            Value::Edge(id) => {
                // Get the edge from the snapshot and check property value
                if let Some(edge) = ctx.storage().get_edge(*id) {
                    edge.properties
                        .get(&self.key)
                        .map(|pv| pv == &self.value)
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            // Non-element values don't have properties
            _ => false,
        }
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, ctx: &StreamingContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => ctx
                .storage()
                .get_vertex(*id)
                .and_then(|v| v.properties.get(&self.key).cloned())
                .map(|pv| pv == self.value)
                .unwrap_or(false),
            Value::Edge(id) => ctx
                .storage()
                .get_edge(*id)
                .and_then(|e| e.properties.get(&self.key).cloned())
                .map(|pv| pv == self.value)
                .unwrap_or(false),
            _ => false,
        }
    }
}

// Use the macro to implement Step for HasValueStep
impl_filter_step!(HasValueStep, "has", category = crate::traversal::explain::StepCategory::Filter);

// Reactive introspection: expose property key constraint.
#[cfg(feature = "reactive")]
impl crate::traversal::reactive::StepIntrospect for HasValueStep {
    fn property_constraints(&self) -> Option<Vec<String>> {
        Some(vec![self.key.clone()])
    }
}

// -----------------------------------------------------------------------------
// FilterStep - generic filter with custom predicate
// -----------------------------------------------------------------------------

/// Generic filter step that uses a custom predicate closure.
///
/// The predicate receives the execution context and the value, returning
/// `true` to keep the traverser or `false` to filter it out.
///
/// # Type Requirements
///
/// The predicate closure must be:
/// - `Fn(&ExecutionContext, &Value) -> bool` - takes context and value
/// - `Clone` - required for step cloning in branching operations
/// - `Send + Sync` - required for thread safety
/// - `'static` - required for storage in boxed trait objects
///
/// # Example
///
/// ```ignore
/// // Filter to only positive integers
/// let positives = g.inject([1i64, -2i64, 3i64])
///     .filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 0))
///     .to_list();
///
/// // Filter using graph context
/// let connected = g.v()
///     .filter(|ctx, v| {
///         if let Some(id) = v.as_vertex_id() {
///             ctx.storage().get_vertex(id)
///                 .map(|_| true)
///                 .unwrap_or(false)
///         } else {
///             false
///         }
///     })
///     .to_list();
/// ```
#[derive(Clone)]
pub struct FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync,
{
    /// The predicate closure
    predicate: F,
}

impl<F> FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync,
{
    /// Create a new FilterStep with the given predicate.
    ///
    /// # Arguments
    ///
    /// * `predicate` - A closure that returns `true` to keep the traverser
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> std::fmt::Debug for FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterStep")
            .field("predicate", &"<closure>")
            .finish()
    }
}

impl<F> Step for FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync + 'static,
{
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let predicate = self.predicate.clone();
        input.filter(move |t| predicate(ctx, &t.value))
    }

    fn name(&self) -> &'static str {
        "filter"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // CLOSURE STEP: FilterStep holds a closure that requires ExecutionContext (not StreamingContext).
        // The closure signature is: Fn(&ExecutionContext, &Value) -> bool
        // StreamingContext cannot be converted to ExecutionContext without graph mutation access.
        // To truly stream, users should use predicate-based steps (has, hasLabel, etc.) instead.
        // Current behavior: pass-through (all values pass).
        Box::new(std::iter::once(input))
    }
}

// -----------------------------------------------------------------------------
// DedupStep - deduplicate traversers by value
// -----------------------------------------------------------------------------

/// Deduplication step that removes duplicate values.
///
/// Uses `Value`'s `Hash` and `Eq` implementations to track seen values.
/// Only the first occurrence of each value passes through; subsequent
/// duplicates are filtered out.
///
/// # Example
///
/// ```ignore
/// // Remove duplicate vertices from a traversal
/// let unique = g.v().out().dedup().to_list();
///
/// // Dedup injected values
/// let unique = g.inject([1i64, 2i64, 1i64, 3i64, 2i64]).dedup().to_list();
/// // Results: [1, 2, 3]
/// ```
///
/// # Implementation Note
///
/// This step maintains internal state (a `HashSet` of seen values) which is
/// created fresh each time the step is applied. This means cloning a traversal
/// with a `DedupStep` will result in independent deduplication state.
#[derive(Clone, Debug)]
pub struct DedupStep {
    /// Shared set for streaming execution (shared across clones)
    seen: Arc<RwLock<HashSet<Value>>>,
}

impl DedupStep {
    /// Create a new DedupStep.
    pub fn new() -> Self {
        Self {
            seen: Arc::new(RwLock::new(HashSet::new())),
        }
    }
}

impl Default for DedupStep {
    fn default() -> Self {
        Self::new()
    }
}

impl Step for DedupStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        // Use a stateful iterator with HashSet to track seen values
        // The HashSet is created fresh for each apply() call
        //
        // Optimization: We use a two-phase check to avoid cloning values that
        // are already present. First we check if the value exists (no clone),
        // then only clone if it's actually new.
        let mut seen = std::collections::HashSet::new();
        input.filter(move |t| {
            // Check if already seen without cloning - this is the fast path
            // for duplicates which are discarded anyway
            if seen.contains(&t.value) {
                false
            } else {
                // Only clone if this is a new value
                seen.insert(t.value.clone());
                true
            }
        })
    }

    fn name(&self) -> &'static str {
        "dedup"
    }

    fn is_barrier(&self) -> bool {
        true
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Use shared RwLock<HashSet> for streaming deduplication
        // Check if value is already seen, if not, insert and pass through
        let mut seen = self.seen.write();
        if seen.contains(&input.value) {
            // Already seen, filter out
            Box::new(std::iter::empty())
        } else {
            // New value, insert and pass through
            seen.insert(input.value.clone());
            Box::new(std::iter::once(input))
        }
    }
}

// -----------------------------------------------------------------------------
// DedupByKeyStep - deduplicate traversers by property value
// -----------------------------------------------------------------------------

/// Deduplication step that removes duplicates based on a property value.
///
/// Extracts the specified property from vertices/edges and uses it as the
/// deduplication key. Only the first occurrence of each unique property value
/// passes through; subsequent duplicates are filtered out.
///
/// For elements without the property, `Value::Null` is used as the key,
/// so only one element without the property will pass.
///
/// # Example
///
/// ```ignore
/// // Keep only one person per age
/// let unique_ages = g.v().has_label("person").dedup_by_key("age").to_list();
///
/// // Keep only one edge per weight
/// let unique_weights = g.e().dedup_by_key("weight").to_list();
/// ```
#[derive(Clone, Debug)]
pub struct DedupByKeyStep {
    /// The property key to use for deduplication
    key: String,
    /// Shared set for streaming execution (shared across clones)
    seen: Arc<RwLock<HashSet<Value>>>,
}

impl DedupByKeyStep {
    /// Create a new DedupByKeyStep with the given property key.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to extract and use for deduplication
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            seen: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Extract the property value from a traverser's element.
    ///
    /// Returns `Value::Null` if the element doesn't have the property
    /// or if the traverser value is not an element.
    fn extract_key(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        match &traverser.value {
            Value::Vertex(id) => ctx
                .storage()
                .get_vertex(*id)
                .and_then(|v| v.properties.get(&self.key).cloned())
                .unwrap_or(Value::Null),
            Value::Edge(id) => ctx
                .storage()
                .get_edge(*id)
                .and_then(|e| e.properties.get(&self.key).cloned())
                .unwrap_or(Value::Null),
            // Non-element values don't have properties, use Null
            _ => Value::Null,
        }
    }

    /// Extract the property value using StreamingContext.
    fn extract_key_streaming(&self, ctx: &StreamingContext, traverser: &Traverser) -> Value {
        match &traverser.value {
            Value::Vertex(id) => ctx
                .storage()
                .get_vertex(*id)
                .and_then(|v| v.properties.get(&self.key).cloned())
                .unwrap_or(Value::Null),
            Value::Edge(id) => ctx
                .storage()
                .get_edge(*id)
                .and_then(|e| e.properties.get(&self.key).cloned())
                .unwrap_or(Value::Null),
            // Non-element values don't have properties, use Null
            _ => Value::Null,
        }
    }
}

impl Step for DedupByKeyStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let mut seen = std::collections::HashSet::new();
        input.filter(move |t| {
            let key = self.extract_key(ctx, t);
            seen.insert(key)
        })
    }

    fn name(&self) -> &'static str {
        "dedup"
    }

    fn is_barrier(&self) -> bool {
        true
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Extract the key using streaming context
        let key = self.extract_key_streaming(&ctx, &input);

        // Use shared RwLock<HashSet> for streaming deduplication
        let mut seen = self.seen.write();
        if seen.contains(&key) {
            // Already seen this key, filter out
            Box::new(std::iter::empty())
        } else {
            // New key, insert and pass through
            seen.insert(key);
            Box::new(std::iter::once(input))
        }
    }
}

// -----------------------------------------------------------------------------
// DedupByLabelStep - deduplicate traversers by element label
// -----------------------------------------------------------------------------

/// Deduplication step that removes duplicates based on element label.
///
/// Extracts the label from vertices/edges and uses it as the deduplication key.
/// Only the first occurrence of each unique label passes through.
///
/// For non-element values, an empty string is used as the key.
///
/// # Example
///
/// ```ignore
/// // Keep only one element per label
/// let one_per_label = g.v().dedup_by_label().to_list();
/// ```
#[derive(Clone, Debug)]
pub struct DedupByLabelStep {
    /// Shared set for streaming execution (shared across clones)
    seen: Arc<RwLock<HashSet<String>>>,
}

impl DedupByLabelStep {
    /// Create a new DedupByLabelStep.
    pub fn new() -> Self {
        Self {
            seen: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Extract the label from a traverser's element.
    ///
    /// Returns an empty string if the traverser value is not an element.
    fn extract_label(&self, ctx: &ExecutionContext, traverser: &Traverser) -> String {
        match &traverser.value {
            Value::Vertex(id) => ctx
                .storage()
                .get_vertex(*id)
                .map(|v| v.label.clone())
                .unwrap_or_default(),
            Value::Edge(id) => ctx
                .storage()
                .get_edge(*id)
                .map(|e| e.label.clone())
                .unwrap_or_default(),
            // Non-element values don't have labels
            _ => String::new(),
        }
    }

    /// Extract the label using StreamingContext.
    fn extract_label_streaming(&self, ctx: &StreamingContext, traverser: &Traverser) -> String {
        match &traverser.value {
            Value::Vertex(id) => ctx
                .storage()
                .get_vertex(*id)
                .map(|v| v.label.clone())
                .unwrap_or_default(),
            Value::Edge(id) => ctx
                .storage()
                .get_edge(*id)
                .map(|e| e.label.clone())
                .unwrap_or_default(),
            // Non-element values don't have labels
            _ => String::new(),
        }
    }
}

impl Default for DedupByLabelStep {
    fn default() -> Self {
        Self::new()
    }
}

impl Step for DedupByLabelStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let mut seen = std::collections::HashSet::new();
        input.filter(move |t| {
            let label = self.extract_label(ctx, t);
            seen.insert(label)
        })
    }

    fn name(&self) -> &'static str {
        "dedup"
    }

    fn is_barrier(&self) -> bool {
        true
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Extract the label using streaming context
        let label = self.extract_label_streaming(&ctx, &input);

        // Use shared RwLock<HashSet> for streaming deduplication
        let mut seen = self.seen.write();
        if seen.contains(&label) {
            // Already seen this label, filter out
            Box::new(std::iter::empty())
        } else {
            // New label, insert and pass through
            seen.insert(label);
            Box::new(std::iter::once(input))
        }
    }
}

// -----------------------------------------------------------------------------
// DedupByTraversalStep - deduplicate traversers by sub-traversal result
// -----------------------------------------------------------------------------

/// Deduplication step that removes duplicates based on sub-traversal result.
///
/// Executes the given sub-traversal for each element and uses the first result
/// as the deduplication key. Only the first occurrence of each unique key
/// passes through; subsequent duplicates are filtered out.
///
/// If the sub-traversal produces no results for an element, `Value::Null`
/// is used as the key.
///
/// # Example
///
/// ```ignore
/// // Keep only one vertex per out-degree (number of outgoing edges)
/// let unique_outdegree = g.v()
///     .dedup_by(__.out().count())
///     .to_list();
///
/// // Keep only one person per first known friend's name
/// let unique_friend_name = g.v()
///     .has_label("person")
///     .dedup_by(__.out_labels(&["knows"]).limit(1).values("name"))
///     .to_list();
/// ```
#[derive(Clone)]
pub struct DedupByTraversalStep {
    /// The sub-traversal to execute for each element to get the dedup key
    sub: crate::traversal::Traversal<Value, Value>,
    /// Shared set for streaming execution (shared across clones)
    seen: Arc<RwLock<HashSet<Value>>>,
}

impl DedupByTraversalStep {
    /// Create a new DedupByTraversalStep with the given sub-traversal.
    ///
    /// # Arguments
    ///
    /// * `sub` - The sub-traversal to execute for each element
    pub fn new(sub: crate::traversal::Traversal<Value, Value>) -> Self {
        Self {
            sub,
            seen: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Execute the sub-traversal and get the first result as the dedup key.
    ///
    /// Returns `Value::Null` if the sub-traversal produces no results.
    fn extract_key(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        use crate::traversal::step::execute_traversal_from;

        let sub_input = Box::new(std::iter::once(traverser.clone()));
        let mut results = execute_traversal_from(ctx, &self.sub, sub_input);
        results.next().map(|t| t.value).unwrap_or(Value::Null)
    }

    /// Execute the sub-traversal using StreamingContext and get the first result as the dedup key.
    fn extract_key_streaming(&self, ctx: &StreamingContext, traverser: &Traverser) -> Value {
        use crate::traversal::step::execute_traversal_streaming;

        let mut results = execute_traversal_streaming(ctx, &self.sub, traverser.clone());
        results.next().map(|t| t.value).unwrap_or(Value::Null)
    }
}

impl std::fmt::Debug for DedupByTraversalStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DedupByTraversalStep")
            .field("sub", &"<traversal>")
            .finish()
    }
}

impl Step for DedupByTraversalStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let mut seen = std::collections::HashSet::new();
        input.filter(move |t| {
            let key = self.extract_key(ctx, t);
            seen.insert(key)
        })
    }

    fn name(&self) -> &'static str {
        "dedup"
    }

    fn is_barrier(&self) -> bool {
        true
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Extract the dedup key using streaming sub-traversal execution
        let key = self.extract_key_streaming(&ctx, &input);

        // Use the shared HashSet to check for duplicates
        let mut seen = self.seen.write();
        if seen.contains(&key) {
            Box::new(std::iter::empty())
        } else {
            seen.insert(key);
            Box::new(std::iter::once(input))
        }
    }
}

// -----------------------------------------------------------------------------
// LimitStep - limit the number of traversers
// -----------------------------------------------------------------------------

/// Limit step that restricts the number of traversers passing through.
///
/// Returns at most the specified number of traversers, stopping iteration
/// after the limit is reached.
///
/// # Example
///
/// ```ignore
/// // Get only the first 5 vertices
/// let first_five = g.v().limit(5).to_list();
///
/// // Limit injected values
/// let limited = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).limit(3).to_list();
/// // Results: [1, 2, 3]
/// ```
#[derive(Clone, Debug)]
pub struct LimitStep {
    /// Maximum number of traversers to pass through
    limit: usize,
    /// Counter for streaming execution (shared across clones)
    seen: Arc<AtomicUsize>,
}

impl LimitStep {
    /// Create a new LimitStep with the given limit.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of traversers to pass through
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            seen: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Step for LimitStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.take(self.limit)
    }

    fn name(&self) -> &'static str {
        "limit"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn describe(&self) -> Option<String> {
        Some(format!("limit: {}", self.limit))
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Atomically increment and get the previous count
        let count = self.seen.fetch_add(1, Ordering::SeqCst);
        if count < self.limit {
            Box::new(std::iter::once(input))
        } else {
            // Already at or past limit, filter out
            Box::new(std::iter::empty())
        }
    }
}

// -----------------------------------------------------------------------------
// SkipStep - skip a number of traversers
// -----------------------------------------------------------------------------

/// Skip step that skips the first n traversers.
///
/// Discards the first n traversers and passes through all remaining ones.
///
/// # Example
///
/// ```ignore
/// // Skip the first 3 vertices
/// let after_skip = g.v().skip(3).to_list();
///
/// // Skip injected values
/// let skipped = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).skip(2).to_list();
/// // Results: [3, 4, 5]
/// ```
#[derive(Clone, Debug)]
pub struct SkipStep {
    /// Number of traversers to skip
    count: usize,
    /// Counter for streaming execution (shared across clones)
    seen: Arc<AtomicUsize>,
}

impl SkipStep {
    /// Create a new SkipStep that skips n traversers.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of traversers to skip
    pub fn new(count: usize) -> Self {
        Self {
            count,
            seen: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Step for SkipStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.skip(self.count)
    }

    fn name(&self) -> &'static str {
        "skip"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn describe(&self) -> Option<String> {
        Some(format!("skip: {}", self.count))
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Atomically increment and get the previous count
        let index = self.seen.fetch_add(1, Ordering::SeqCst);
        if index < self.count {
            // Still in skip range, filter out
            Box::new(std::iter::empty())
        } else {
            // Past skip range, pass through
            Box::new(std::iter::once(input))
        }
    }
}

// -----------------------------------------------------------------------------
// RangeStep - select a range of traversers
// -----------------------------------------------------------------------------

/// Range step that selects traversers within a given range.
///
/// Equivalent to `skip(start).limit(end - start)`. Returns traversers
/// from index `start` (inclusive) to index `end` (exclusive).
///
/// # Example
///
/// ```ignore
/// // Get vertices 2, 3, 4 (indices 2-5)
/// let range = g.v().range(2, 5).to_list();
///
/// // Range of injected values
/// let ranged = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).range(1, 4).to_list();
/// // Results: [2, 3, 4]
/// ```
///
/// # Panics
///
/// Does not panic. If `end <= start`, returns an empty iterator.
/// If `end` exceeds the number of traversers, returns all traversers from `start`.
#[derive(Clone, Debug)]
pub struct RangeStep {
    /// Start index (inclusive)
    start: usize,
    /// End index (exclusive)
    end: usize,
    /// Counter for streaming execution (shared across clones)
    seen: Arc<AtomicUsize>,
}

impl RangeStep {
    /// Create a new RangeStep with the given range.
    ///
    /// # Arguments
    ///
    /// * `start` - Start index (inclusive)
    /// * `end` - End index (exclusive)
    pub fn new(start: usize, end: usize) -> Self {
        Self {
            start,
            end,
            seen: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Step for RangeStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        // Calculate how many to take after skipping
        let take_count = self.end.saturating_sub(self.start);
        input.skip(self.start).take(take_count)
    }

    fn name(&self) -> &'static str {
        "range"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn describe(&self) -> Option<String> {
        Some(format!("range: {}..{}", self.start, self.end))
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Atomically increment and get the previous count
        let index = self.seen.fetch_add(1, Ordering::SeqCst);
        // In range [start, end)
        if index >= self.start && index < self.end {
            Box::new(std::iter::once(input))
        } else {
            // Outside range, filter out
            Box::new(std::iter::empty())
        }
    }
}

// -----------------------------------------------------------------------------
// HasIdStep - filter by element ID
// -----------------------------------------------------------------------------

/// Filter step that keeps only elements with matching IDs.
///
/// Works with both vertices and edges. Non-element values (integers, strings, etc.)
/// are filtered out.
///
/// # Example
///
/// ```ignore
/// // Filter to a specific vertex by ID
/// let vertex = g.v().has_id(VertexId(1)).to_list();
///
/// // Filter to multiple vertex IDs
/// let vertices = g.v().has_ids([VertexId(1), VertexId(2), VertexId(3)]).to_list();
///
/// // Filter edges by ID
/// let edge = g.e().has_id(EdgeId(0)).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct HasIdStep {
    /// IDs to match against (stored as Values for flexibility)
    ids: Vec<Value>,
}

impl HasIdStep {
    /// Create a HasIdStep for a single vertex ID.
    ///
    /// # Arguments
    ///
    /// * `id` - The vertex ID to match
    pub fn vertex(id: VertexId) -> Self {
        Self {
            ids: vec![Value::Vertex(id)],
        }
    }

    /// Create a HasIdStep for multiple vertex IDs.
    ///
    /// # Arguments
    ///
    /// * `ids` - The vertex IDs to match (element must match any one)
    pub fn vertices(ids: Vec<VertexId>) -> Self {
        Self {
            ids: ids.into_iter().map(Value::Vertex).collect(),
        }
    }

    /// Create a HasIdStep for a single edge ID.
    ///
    /// # Arguments
    ///
    /// * `id` - The edge ID to match
    pub fn edge(id: EdgeId) -> Self {
        Self {
            ids: vec![Value::Edge(id)],
        }
    }

    /// Create a HasIdStep for multiple edge IDs.
    ///
    /// # Arguments
    ///
    /// * `ids` - The edge IDs to match (element must match any one)
    pub fn edges(ids: Vec<EdgeId>) -> Self {
        Self {
            ids: ids.into_iter().map(Value::Edge).collect(),
        }
    }

    /// Create a HasIdStep from a single Value.
    ///
    /// This is useful for dynamic ID filtering where the ID type
    /// may not be known at compile time.
    ///
    /// # Arguments
    ///
    /// * `value` - A Value that should be a Vertex or Edge ID
    pub fn from_value(value: impl Into<Value>) -> Self {
        Self {
            ids: vec![value.into()],
        }
    }

    /// Create a HasIdStep from multiple Values.
    ///
    /// This is useful for dynamic ID filtering where the ID types
    /// may not be known at compile time.
    ///
    /// # Arguments
    ///
    /// * `values` - Values that should be Vertex or Edge IDs
    pub fn from_values(values: Vec<Value>) -> Self {
        Self { ids: values }
    }

    /// Check if a traverser's element ID matches any of the target IDs.
    ///
    /// Returns `false` for non-element values (integers, strings, etc.).
    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                // Check if this vertex ID is in our target list
                self.ids.iter().any(|target| match target {
                    Value::Vertex(target_id) => target_id == id,
                    _ => false,
                })
            }
            Value::Edge(id) => {
                // Check if this edge ID is in our target list
                self.ids.iter().any(|target| match target {
                    Value::Edge(target_id) => target_id == id,
                    _ => false,
                })
            }
            // Non-element values don't have IDs
            _ => false,
        }
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, _ctx: &StreamingContext, traverser: &Traverser) -> bool {
        // HasIdStep doesn't need storage access, just checks IDs
        match &traverser.value {
            Value::Vertex(id) => self.ids.iter().any(|target| match target {
                Value::Vertex(target_id) => target_id == id,
                _ => false,
            }),
            Value::Edge(id) => self.ids.iter().any(|target| match target {
                Value::Edge(target_id) => target_id == id,
                _ => false,
            }),
            _ => false,
        }
    }
}

// Use the macro to implement Step for HasIdStep
impl_filter_step!(HasIdStep, "hasId", category = crate::traversal::explain::StepCategory::Filter);

// -----------------------------------------------------------------------------
// HasWhereStep - filter by property value using a predicate
// -----------------------------------------------------------------------------

/// Filter step that keeps only elements where a property satisfies a predicate.
///
/// Works with both vertices and edges. Non-element values (integers, strings, etc.)
/// are filtered out since they don't have properties. Elements without the
/// specified property are also filtered out.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::p;
///
/// // Filter to vertices where age >= 18
/// let adults = g.v().has_where("age", p::gte(18)).to_list();
///
/// // Filter to vertices where name starts with "A"
/// let a_names = g.v().has_where("name", p::starting_with("A")).to_list();
///
/// // Filter with logical predicates
/// let adults_under_65 = g.v().has_where("age", p::and(p::gte(18), p::lt(65))).to_list();
/// ```
#[derive(Clone)]
pub struct HasWhereStep {
    /// The property key to extract and test
    key: String,
    /// The predicate to test the property value against
    predicate: Box<dyn crate::traversal::predicate::Predicate>,
}

impl HasWhereStep {
    /// Create a new HasWhereStep with the given key and predicate.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to extract from vertices/edges
    /// * `predicate` - The predicate to test the property value against
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::p;
    ///
    /// let step = HasWhereStep::new("age", p::gte(18));
    /// ```
    pub fn new(
        key: impl Into<String>,
        predicate: impl crate::traversal::predicate::Predicate + 'static,
    ) -> Self {
        Self {
            key: key.into(),
            predicate: Box::new(predicate),
        }
    }

    /// Check if a traverser's element has a property that satisfies the predicate.
    ///
    /// Returns `false` for:
    /// - Non-element values (integers, strings, etc.)
    /// - Elements that don't have the specified property
    /// - Properties that don't satisfy the predicate
    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                // Get the vertex from the snapshot
                if let Some(vertex) = ctx.storage().get_vertex(*id) {
                    // Get the property value and test it against the predicate
                    vertex
                        .properties
                        .get(&self.key)
                        .map(|prop_value| self.predicate.test(prop_value))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            Value::Edge(id) => {
                // Get the edge from the snapshot
                if let Some(edge) = ctx.storage().get_edge(*id) {
                    // Get the property value and test it against the predicate
                    edge.properties
                        .get(&self.key)
                        .map(|prop_value| self.predicate.test(prop_value))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            // Non-element values don't have properties
            _ => false,
        }
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, ctx: &StreamingContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => ctx
                .storage()
                .get_vertex(*id)
                .and_then(|v| v.properties.get(&self.key).cloned())
                .map(|prop_value| self.predicate.test(&prop_value))
                .unwrap_or(false),
            Value::Edge(id) => ctx
                .storage()
                .get_edge(*id)
                .and_then(|e| e.properties.get(&self.key).cloned())
                .map(|prop_value| self.predicate.test(&prop_value))
                .unwrap_or(false),
            _ => false,
        }
    }
}

impl std::fmt::Debug for HasWhereStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HasWhereStep")
            .field("key", &self.key)
            .field("predicate", &"<predicate>")
            .finish()
    }
}

// Use the macro to implement Step for HasWhereStep
impl_filter_step!(HasWhereStep, "has", category = crate::traversal::explain::StepCategory::Filter);

// Reactive introspection: expose property key constraint.
#[cfg(feature = "reactive")]
impl crate::traversal::reactive::StepIntrospect for HasWhereStep {
    fn property_constraints(&self) -> Option<Vec<String>> {
        Some(vec![self.key.clone()])
    }
}

// -----------------------------------------------------------------------------
// IsStep - filter by testing the traverser's current value against a predicate
// -----------------------------------------------------------------------------

/// Filter step that tests the traverser's current value against a predicate.
///
/// Unlike `HasWhereStep` which tests a property of a vertex/edge, `IsStep`
/// tests the traverser's current value directly. This is useful after
/// extracting property values with `values()`.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::p;
///
/// // Filter to values equal to 29
/// let age_29 = g.v().values("age").is_eq(29).to_list();
///
/// // Filter to values greater than 25
/// let adults = g.v().values("age").is_(p::gt(25)).to_list();
///
/// // Filter using range predicates
/// let in_range = g.v().values("age").is_(p::between(20, 40)).to_list();
/// ```
#[derive(Clone)]
pub struct IsStep {
    /// The predicate to test the current value against
    predicate: Box<dyn crate::traversal::predicate::Predicate>,
}

impl IsStep {
    /// Create a new IsStep with the given predicate.
    ///
    /// # Arguments
    ///
    /// * `predicate` - The predicate to test the traverser's value against
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::p;
    ///
    /// let step = IsStep::new(p::gt(25));
    /// ```
    pub fn new(predicate: impl crate::traversal::predicate::Predicate + 'static) -> Self {
        Self {
            predicate: Box::new(predicate),
        }
    }

    /// Create an IsStep that tests for equality with a value.
    ///
    /// This is a convenience method equivalent to `IsStep::new(p::eq(value))`.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to compare against for equality
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = IsStep::eq(29);
    /// ```
    pub fn eq(value: impl Into<Value>) -> Self {
        Self::new(crate::traversal::predicate::p::eq(value))
    }

    /// Check if the traverser's current value satisfies the predicate.
    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        self.predicate.test(&traverser.value)
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, _ctx: &StreamingContext, traverser: &Traverser) -> bool {
        self.predicate.test(&traverser.value)
    }
}

impl std::fmt::Debug for IsStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IsStep")
            .field("predicate", &"<predicate>")
            .finish()
    }
}

// Use the macro to implement Step for IsStep
impl_filter_step!(IsStep, "is", category = crate::traversal::explain::StepCategory::Filter);

// -----------------------------------------------------------------------------
// SimplePathStep - Filter to paths with no repeated elements
// -----------------------------------------------------------------------------

/// Filter step that keeps only traversers with simple (non-repeating) paths.
///
/// A simple path is one where no element appears more than once. This is useful
/// for finding unique paths through a graph and avoiding cycles during traversal.
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().repeat(out()).until(hasLabel("target")).simplePath()
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::prelude::*;
///
/// // Find all simple paths of length 3
/// let simple_paths = g.v()
///     .repeat(__.out())
///     .times(3)
///     .simple_path()
///     .path()
///     .to_list();
/// ```
#[derive(Clone, Debug, Copy)]
pub struct SimplePathStep;

impl SimplePathStep {
    /// Create a new SimplePathStep.
    pub fn new() -> Self {
        Self
    }

    /// Check if the traverser's path contains only unique elements.
    ///
    /// Returns `true` if all path elements are unique (simple path),
    /// `false` if any element appears more than once.
    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for element in traverser.path.elements() {
            if !seen.insert(&element.value) {
                return false; // Duplicate found
            }
        }
        true // All elements unique
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, _ctx: &StreamingContext, traverser: &Traverser) -> bool {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for element in traverser.path.elements() {
            if !seen.insert(&element.value) {
                return false;
            }
        }
        true
    }
}

impl Default for SimplePathStep {
    fn default() -> Self {
        Self::new()
    }
}

// Use the macro to implement Step for SimplePathStep
impl_filter_step!(SimplePathStep, "simplePath", category = crate::traversal::explain::StepCategory::Filter);

// -----------------------------------------------------------------------------
// CyclicPathStep - Filter to paths with at least one repeated element
// -----------------------------------------------------------------------------

/// Filter step that keeps only traversers with cyclic (repeating) paths.
///
/// A cyclic path is one where at least one element appears more than once.
/// This is the inverse of `SimplePathStep`.
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().repeat(out()).until(hasLabel("target")).cyclicPath()
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::prelude::*;
///
/// // Find all cyclic paths
/// let cyclic_paths = g.v()
///     .repeat(__.out())
///     .times(4)
///     .cyclic_path()
///     .path()
///     .to_list();
/// ```
#[derive(Clone, Debug, Copy)]
pub struct CyclicPathStep;

impl CyclicPathStep {
    /// Create a new CyclicPathStep.
    pub fn new() -> Self {
        Self
    }

    /// Check if the traverser's path contains any repeated elements.
    ///
    /// Returns `true` if any element appears more than once (cyclic path),
    /// `false` if all elements are unique.
    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for element in traverser.path.elements() {
            if !seen.insert(&element.value) {
                return true; // Duplicate found - it's cyclic
            }
        }
        false // All elements unique - not cyclic
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, _ctx: &StreamingContext, traverser: &Traverser) -> bool {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        for element in traverser.path.elements() {
            if !seen.insert(&element.value) {
                return true;
            }
        }
        false
    }
}

impl Default for CyclicPathStep {
    fn default() -> Self {
        Self::new()
    }
}

// Use the macro to implement Step for CyclicPathStep
impl_filter_step!(CyclicPathStep, "cyclicPath", category = crate::traversal::explain::StepCategory::Filter);

// -----------------------------------------------------------------------------
// TailStep - returns the last n elements from the traversal
// -----------------------------------------------------------------------------

/// Filter step that returns only the last n elements from the traversal.
///
/// This is a **barrier step** - it must consume all elements to determine
/// which are the last n. The elements are returned in their original order.
///
/// # Memory Efficiency
///
/// Unlike most barrier steps, `TailStep` uses O(n) memory where n is the
/// requested count (not the total input size). It uses a ring buffer to
/// only retain the last n elements seen, making it efficient even for
/// very large traversals.
///
/// # Behavior
///
/// - `tail()` (or `tail_n(1)`) returns only the last element
/// - `tail_n(n)` returns the last n elements in order
/// - If fewer than n elements exist, all elements are returned
/// - Empty traversal returns empty result
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().values("name").tail()      // Last element only
/// g.V().values("name").tail(3)     // Last 3 elements
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::prelude::*;
///
/// // Get the last vertex
/// let last = g.v().tail().to_list();
///
/// // Get the last 5 vertices
/// let last_five = g.v().tail_n(5).to_list();
///
/// // Works with values too
/// let last_names = g.v().values("name").tail_n(3).to_list();
/// ```
#[derive(Clone, Debug, Copy)]
pub struct TailStep {
    /// Number of elements to return from the end
    count: usize,
}

impl TailStep {
    /// Create a new TailStep that returns the last n elements.
    ///
    /// # Arguments
    ///
    /// * `count` - Number of elements to return from the end
    pub fn new(count: usize) -> Self {
        Self { count }
    }

    /// Create a TailStep that returns only the last element.
    ///
    /// Equivalent to `TailStep::new(1)`.
    pub fn last() -> Self {
        Self::new(1)
    }
}

impl Default for TailStep {
    fn default() -> Self {
        Self::last()
    }
}

impl Step for TailStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        // Special case: tail(0) returns nothing
        if self.count == 0 {
            return Box::new(std::iter::empty());
        }

        // Use a ring buffer to keep only the last `count` elements.
        // This is O(count) memory instead of O(n) for the full input.
        let mut ring_buffer: VecDeque<Traverser> = VecDeque::with_capacity(self.count);

        for traverser in input {
            if ring_buffer.len() == self.count {
                ring_buffer.pop_front();
            }
            ring_buffer.push_back(traverser);
        }

        Box::new(ring_buffer.into_iter())
    }

    fn name(&self) -> &'static str {
        "tail"
    }

    fn is_barrier(&self) -> bool {
        true
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn describe(&self) -> Option<String> {
        Some(format!("tail: {}", self.count))
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // BARRIER STEP: TailStep cannot truly stream because it must see ALL inputs
        // before it can determine which are the "last N" elements. Unlike LimitStep
        // (which can stop after N), TailStep must traverse everything to find the end.
        // This is fundamentally incompatible with O(1) streaming semantics.
        // Current behavior: pass-through (incorrect but safe for pipeline compatibility).
        Box::new(std::iter::once(input))
    }
}

// -----------------------------------------------------------------------------
// CoinStep - probabilistic filter using random coin flip
// -----------------------------------------------------------------------------

/// Probabilistic filter step that randomly allows traversers to pass through.
///
/// Each traverser has a probability `p` of passing through. This is useful for
/// random sampling, statistical testing, or creating probabilistic traversals.
///
/// # Probability Semantics
///
/// - `coin(0.0)` - No traversers pass (always filter)
/// - `coin(1.0)` - All traversers pass (identity)
/// - `coin(0.5)` - Approximately 50% of traversers pass
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().coin(0.5)  // Each vertex has 50% chance of passing
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::prelude::*;
///
/// // Random sample of approximately 10% of vertices
/// let sample = g.v().coin(0.1).to_list();
///
/// // Probabilistic filtering in a complex query
/// let random_friends = g.v()
///     .has_label("person")
///     .out_labels(&["knows"])
///     .coin(0.5)
///     .to_list();
/// ```
///
/// # Note
///
/// Results are non-deterministic. For reproducible tests, use statistical
/// tolerances or seeded RNG (not currently supported).
#[derive(Clone, Debug, Copy)]
pub struct CoinStep {
    /// Probability of allowing each traverser to pass (0.0 to 1.0)
    probability: f64,
}

impl CoinStep {
    /// Create a new CoinStep with the given probability.
    ///
    /// # Arguments
    ///
    /// * `probability` - Probability of passing (clamped to 0.0..=1.0)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let step = CoinStep::new(0.5); // 50% chance of passing
    /// ```
    pub fn new(probability: f64) -> Self {
        // Clamp probability to valid range
        let probability = probability.clamp(0.0, 1.0);
        Self { probability }
    }

    /// Create a CoinStep that always passes (probability = 1.0).
    ///
    /// Equivalent to identity; useful as a default or placeholder.
    pub fn always() -> Self {
        Self::new(1.0)
    }

    /// Create a CoinStep that never passes (probability = 0.0).
    ///
    /// Equivalent to filtering everything out.
    pub fn never() -> Self {
        Self::new(0.0)
    }

    /// Get the probability value.
    pub fn probability(&self) -> f64 {
        self.probability
    }
}

impl Default for CoinStep {
    fn default() -> Self {
        Self::always()
    }
}

impl Step for CoinStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        use rand::Rng;

        let probability = self.probability;

        // Handle edge cases without RNG overhead
        if probability <= 0.0 {
            return Box::new(std::iter::empty());
        }
        if probability >= 1.0 {
            return input;
        }

        // Use thread-local RNG for each traverser
        Box::new(input.filter(move |_| rand::thread_rng().gen::<f64>() < probability))
    }

    fn name(&self) -> &'static str {
        "coin"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use rand::Rng;

        let probability = self.probability;

        // Handle edge cases without RNG overhead
        if probability <= 0.0 {
            return Box::new(std::iter::empty());
        }
        if probability >= 1.0 {
            return Box::new(std::iter::once(input));
        }

        // Use thread-local RNG
        if rand::thread_rng().gen::<f64>() < probability {
            Box::new(std::iter::once(input))
        } else {
            Box::new(std::iter::empty())
        }
    }
}

// SampleStep - randomly sample n elements using reservoir sampling
// -----------------------------------------------------------------------------

/// Randomly samples n elements from the traversal using reservoir sampling.
///
/// `SampleStep` is a **barrier step** that collects elements and returns a random
/// sample of exactly n elements. If the input has fewer than n elements, all
/// elements are returned.
///
/// # Algorithm
///
/// Uses the reservoir sampling algorithm (Algorithm R by Vitter):
/// 1. Fill reservoir with first n elements
/// 2. For each subsequent element k (k > n):
///    - Generate random j in [0, k]
///    - If j < n, replace reservoir[j] with element k
/// 3. Return reservoir
///
/// This guarantees each element has equal probability of being selected,
/// regardless of the total input size.
///
/// # Performance
///
/// - Time: O(n) where n is the input size
/// - Space: O(k) where k is the sample size
///
/// # Example
///
/// ```ignore
/// use rust_graph_database::traversal::filter::SampleStep;
/// use rust_graph_database::traversal::step::Step;
///
/// // Sample 5 random elements
/// let step = SampleStep::new(5);
/// ```
///
/// # Note
///
/// This is a **barrier step** that must consume all input before producing output.
/// Results are non-deterministic due to randomness.
#[derive(Clone, Debug, Copy)]
pub struct SampleStep {
    count: usize,
}

impl SampleStep {
    /// Create a new SampleStep that samples n elements.
    ///
    /// # Arguments
    ///
    /// * `count` - The number of elements to sample
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = SampleStep::new(10); // Sample 10 elements
    /// ```
    pub fn new(count: usize) -> Self {
        Self { count }
    }

    /// Get the sample count.
    pub fn count(&self) -> usize {
        self.count
    }
}

impl Default for SampleStep {
    fn default() -> Self {
        Self::new(1)
    }
}

impl Step for SampleStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        use rand::Rng;

        let count = self.count;

        // Handle edge case: sample(0) returns empty
        if count == 0 {
            return Box::new(std::iter::empty());
        }

        // Reservoir sampling algorithm (Algorithm R)
        let mut reservoir: Vec<Traverser> = Vec::with_capacity(count);
        let mut rng = rand::thread_rng();

        for (k, item) in input.enumerate() {
            if k < count {
                // Fill the reservoir with first n elements
                reservoir.push(item);
            } else {
                // For subsequent elements, randomly decide whether to include
                // Generate j in [0, k] (inclusive)
                let j = rng.gen_range(0..=k);
                if j < count {
                    reservoir[j] = item;
                }
            }
        }

        Box::new(reservoir.into_iter())
    }

    fn name(&self) -> &'static str {
        "sample"
    }

    fn is_barrier(&self) -> bool {
        true
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Filter
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // BARRIER STEP: SampleStep cannot truly stream because it uses reservoir sampling,
        // which requires seeing ALL inputs to ensure a statistically uniform random sample.
        // Each new input has a probability of replacing an existing sample, so the final
        // sample isn't known until all inputs are processed.
        // This is fundamentally incompatible with O(1) streaming semantics.
        // Current behavior: pass-through (incorrect but safe for pipeline compatibility).
        Box::new(std::iter::once(input))
    }
}

// HasKeyStep - filter property maps by key
// -----------------------------------------------------------------------------

/// Filter step that keeps only property objects with specific key names.
///
/// This step is designed to work with the output of `properties()`, which returns
/// `Value::Map` objects with "key" and "value" entries. It filters to keep only
/// properties whose "key" field matches one of the specified keys.
///
/// # Behavior
///
/// - For `Value::Map` with a "key" entry: passes if the key matches any specified key
/// - For other values: filters them out
///
/// # Example
///
/// ```ignore
/// use rust_graph_database::traversal::filter::HasKeyStep;
///
/// // Filter to only "name" properties
/// let step = HasKeyStep::new("name");
///
/// // Filter to properties with key "name" or "age"
/// let step = HasKeyStep::any(["name", "age"]);
///
/// // Usage in traversal
/// g.v().properties().has_key("name").to_list()
/// ```
#[derive(Clone, Debug)]
pub struct HasKeyStep {
    /// The property keys to match against
    keys: Vec<String>,
}

impl HasKeyStep {
    /// Create a new HasKeyStep that filters to a single key.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = HasKeyStep::new("name");
    /// ```
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    /// Create a HasKeyStep that filters to any of the specified keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - The property keys to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = HasKeyStep::any(["name", "age", "email"]);
    /// ```
    pub fn any<I, S>(keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }

    /// Get the keys this step filters for.
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Check if a traverser's value matches the filter criteria.
    ///
    /// Returns `true` if the value is a Map with a "key" entry that matches
    /// any of the specified keys.
    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Map(map) => {
                // Check if this is a property map with a "key" entry
                if let Some(Value::String(key)) = map.get("key") {
                    self.keys.iter().any(|k| k == key)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, _ctx: &StreamingContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Map(map) => {
                if let Some(Value::String(key)) = map.get("key") {
                    self.keys.iter().any(|k| k == key)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

// Use the macro to implement Step for HasKeyStep
impl_filter_step!(HasKeyStep, "hasKey", category = crate::traversal::explain::StepCategory::Filter);

// HasPropValueStep - filter property maps by value
// -----------------------------------------------------------------------------

/// Filter step that keeps only property objects with specific values.
///
/// This step is designed to work with the output of `properties()`, which returns
/// `Value::Map` objects with "key" and "value" entries. It filters to keep only
/// properties whose "value" field matches one of the specified values.
///
/// **Note**: This is different from `HasValueStep`, which filters vertices/edges
/// by property values. This step filters property objects themselves.
///
/// # Behavior
///
/// - For `Value::Map` with a "value" entry: passes if the value matches any specified value
/// - For other values: filters them out
///
/// # Example
///
/// ```ignore
/// use rust_graph_database::traversal::filter::HasPropValueStep;
///
/// // Filter to properties with value "Alice"
/// let step = HasPropValueStep::new("Alice");
///
/// // Filter to properties with value 30 or 40
/// let step = HasPropValueStep::any([30i64, 40i64]);
///
/// // Usage in traversal
/// g.v().properties().has_prop_value("Alice").to_list()
/// ```
#[derive(Clone, Debug)]
pub struct HasPropValueStep {
    /// The property values to match against
    values: Vec<Value>,
}

impl HasPropValueStep {
    /// Create a new HasPropValueStep that filters to a single value.
    ///
    /// # Arguments
    ///
    /// * `value` - The property value to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = HasPropValueStep::new("Alice");
    /// let step = HasPropValueStep::new(30i64);
    /// ```
    pub fn new(value: impl Into<Value>) -> Self {
        Self {
            values: vec![value.into()],
        }
    }

    /// Create a HasPropValueStep that filters to any of the specified values.
    ///
    /// # Arguments
    ///
    /// * `values` - The property values to filter for
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = HasPropValueStep::any(["Alice", "Bob"]);
    /// let step = HasPropValueStep::any([30i64, 40i64, 50i64]);
    /// ```
    pub fn any<I, V>(values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        Self {
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    /// Get the values this step filters for.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Check if a traverser's value matches the filter criteria.
    ///
    /// Returns `true` if the value is a Map with a "value" entry that matches
    /// any of the specified values.
    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Map(map) => {
                // Check if this is a property map with a "value" entry
                if let Some(value) = map.get("value") {
                    self.values.iter().any(|v| v == value)
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, _ctx: &StreamingContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Map(map) => {
                if let Some(value) = map.get("value") {
                    self.values.iter().any(|v| v == value)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

// Use the macro to implement Step for HasPropValueStep
impl_filter_step!(HasPropValueStep, "hasValue", category = crate::traversal::explain::StepCategory::Filter);

// WherePStep - filter current value against a predicate
// -----------------------------------------------------------------------------

/// Filter step that tests the current traverser value against a predicate.
///
/// `WherePStep` filters traversers by applying a predicate to their current value.
/// This is the predicate-based variant of `where()`, complementing the traversal-based
/// `where_(traversal)` step.
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().values("age").where(P.gt(25))
/// g.V().values("name").where(P.within("Alice", "Bob"))
/// ```
///
/// # Behavior
///
/// - Tests the traverser's current value against the predicate
/// - Passes traversers where the predicate returns `true`
/// - Filters out traversers where the predicate returns `false`
///
/// # Note
///
/// This step is similar to `IsStep`, but named to align with Gremlin's `where(predicate)`
/// syntax. Use `is_()` for value filtering or `where_p()` for Gremlin compatibility.
///
/// # Example
///
/// ```ignore
/// use rust_graph_database::traversal::filter::WherePStep;
/// use rust_graph_database::traversal::p;
///
/// // Filter values greater than 25
/// let step = WherePStep::new(p::gt(25));
///
/// // Filter values within a set
/// let step = WherePStep::new(p::within([1, 2, 3]));
///
/// // Usage in traversal
/// g.v().values("age").where_p(p::gt(25)).to_list()
/// ```
#[derive(Clone)]
pub struct WherePStep {
    /// The predicate to test the current value against
    predicate: Box<dyn crate::traversal::predicate::Predicate>,
}

impl WherePStep {
    /// Create a new WherePStep with the given predicate.
    ///
    /// # Arguments
    ///
    /// * `predicate` - The predicate to test the traverser's value against
    ///
    /// # Example
    ///
    /// ```ignore
    /// use interstellar::traversal::p;
    ///
    /// let step = WherePStep::new(p::gt(25));
    /// let step = WherePStep::new(p::within(["Alice", "Bob"]));
    /// ```
    pub fn new(predicate: impl crate::traversal::predicate::Predicate + 'static) -> Self {
        Self {
            predicate: Box::new(predicate),
        }
    }

    /// Check if the traverser's current value satisfies the predicate.
    fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        self.predicate.test(&traverser.value)
    }

    /// Streaming version of matches.
    fn matches_streaming(&self, _ctx: &StreamingContext, traverser: &Traverser) -> bool {
        self.predicate.test(&traverser.value)
    }
}

impl std::fmt::Debug for WherePStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WherePStep")
            .field("predicate", &"<predicate>")
            .finish()
    }
}

// Use the macro to implement Step for WherePStep
impl_filter_step!(WherePStep, "where", category = crate::traversal::explain::StepCategory::Filter);

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use crate::traversal::SnapshotLike;
    use crate::value::{EdgeId, VertexId};
    use std::collections::HashMap;

    fn create_test_graph() -> Graph {
        let graph = Graph::new();

        // Add vertices with different labels
        let v0 = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props
        });
        let v1 = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props
        });
        let v2 = graph.add_vertex("software", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Graph DB".to_string()));
            props
        });
        let v3 = graph.add_vertex("company", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("TechCorp".to_string()));
            props
        });

        // Add edges with different labels
        graph.add_edge(v0, v1, "knows", HashMap::new()).unwrap();
        graph.add_edge(v1, v2, "uses", HashMap::new()).unwrap();
        graph.add_edge(v0, v3, "works_at", HashMap::new()).unwrap();

        graph
    }

    mod has_label_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        #[test]
        fn single_creates_single_label_step() {
            let step = HasLabelStep::single("person");
            assert_eq!(step.labels, vec!["person".to_string()]);
        }

        #[test]
        fn new_creates_multi_label_step() {
            let step = HasLabelStep::new(vec!["person".to_string(), "company".to_string()]);
            assert_eq!(step.labels.len(), 2);
        }

        #[test]
        fn any_creates_multi_label_step() {
            let step = HasLabelStep::any(["person", "company", "software"]);
            assert_eq!(step.labels.len(), 3);
        }

        #[test]
        fn name_returns_has_label() {
            let step = HasLabelStep::single("person");
            assert_eq!(step.name(), "hasLabel");
        }

        #[test]
        fn clone_box_works() {
            let step = HasLabelStep::single("person");
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "hasLabel");
        }

        #[test]
        fn filters_vertices_by_single_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasLabelStep::single("person");

            // Create traversers for all vertices
            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // person (Alice)
                Traverser::from_vertex(VertexId(1)), // person (Bob)
                Traverser::from_vertex(VertexId(2)), // software
                Traverser::from_vertex(VertexId(3)), // company
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only 2 person vertices should pass
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(1)));
        }

        #[test]
        fn filters_vertices_by_multiple_labels() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasLabelStep::any(["person", "company"]);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // person
                Traverser::from_vertex(VertexId(1)), // person
                Traverser::from_vertex(VertexId(2)), // software
                Traverser::from_vertex(VertexId(3)), // company
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // 2 persons + 1 company = 3
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn filters_edges_by_single_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasLabelStep::single("knows");

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // knows
                Traverser::from_edge(EdgeId(1)), // uses
                Traverser::from_edge(EdgeId(2)), // works_at
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only 1 "knows" edge
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn filters_edges_by_multiple_labels() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasLabelStep::any(["knows", "uses"]);

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // knows
                Traverser::from_edge(EdgeId(1)), // uses
                Traverser::from_edge(EdgeId(2)), // works_at
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // "knows" + "uses" = 2
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn filters_out_non_element_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasLabelStep::single("person");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // person - should pass
                Traverser::new(Value::Int(42)),      // not an element
                Traverser::new(Value::String("hello".to_string())), // not an element
                Traverser::new(Value::Bool(true)),   // not an element
                Traverser::new(Value::Null),         // not an element
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only the person vertex should pass
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_nonexistent_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasLabelStep::single("person");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),   // exists
                Traverser::from_vertex(VertexId(999)), // doesn't exist
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only existing person vertex should pass
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_nonexistent_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasLabelStep::single("knows");

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)),   // exists
                Traverser::from_edge(EdgeId(999)), // doesn't exist
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only existing "knows" edge should pass
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn returns_empty_for_nonexistent_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasLabelStep::single("nonexistent_label");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // No vertices match "nonexistent_label"
            assert!(output.is_empty());
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasLabelStep::single("person");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasLabelStep::single("person");

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn mixed_vertices_and_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // This filter should only match "person" vertices, not edges
            let step = HasLabelStep::single("person");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // person - match
                Traverser::from_edge(EdgeId(0)),     // "knows" edge - no match
                Traverser::from_vertex(VertexId(2)), // software - no match
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn debug_format() {
            let step = HasLabelStep::any(["person", "company"]);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasLabelStep"));
            assert!(debug_str.contains("person"));
            assert!(debug_str.contains("company"));
        }
    }

    mod has_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        fn create_graph_with_properties() -> Graph {
            let graph = Graph::new();

            // Vertex 0: person with name and age
            let v0 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            // Vertex 1: person with only name
            let v1 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props
            });

            // Vertex 2: software with name and version
            let v2 = graph.add_vertex("software", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Graph DB".to_string()));
                props.insert("version".to_string(), Value::Float(1.0));
                props
            });

            // Vertex 3: company with no properties
            graph.add_vertex("company", HashMap::new());

            // Edge 0: knows with since property
            graph
                .add_edge(v0, v1, "knows", {
                    let mut props = HashMap::new();
                    props.insert("since".to_string(), Value::Int(2020));
                    props
                })
                .unwrap();

            // Edge 1: uses with no properties
            graph.add_edge(v1, v2, "uses", HashMap::new()).unwrap();

            graph
        }

        #[test]
        fn new_creates_step_with_key() {
            let step = HasStep::new("age");
            assert_eq!(step.key, "age");
        }

        #[test]
        fn name_returns_has() {
            let step = HasStep::new("age");
            assert_eq!(step.name(), "has");
        }

        #[test]
        fn clone_box_works() {
            let step = HasStep::new("age");
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "has");
        }

        #[test]
        fn filters_vertices_with_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasStep::new("age");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has age
                Traverser::from_vertex(VertexId(1)), // no age
                Traverser::from_vertex(VertexId(2)), // no age
                Traverser::from_vertex(VertexId(3)), // no properties
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only vertex 0 has "age" property
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_vertices_by_name_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has name
                Traverser::from_vertex(VertexId(1)), // has name
                Traverser::from_vertex(VertexId(2)), // has name
                Traverser::from_vertex(VertexId(3)), // no name
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Vertices 0, 1, 2 have "name" property
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn filters_edges_with_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasStep::new("since");

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // has since
                Traverser::from_edge(EdgeId(1)), // no since
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only edge 0 has "since" property
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn filters_out_non_element_values() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has name - should pass
                Traverser::new(Value::Int(42)),      // not an element
                Traverser::new(Value::String("hello".to_string())), // not an element
                Traverser::new(Value::Null),         // not an element
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_nonexistent_vertices() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),   // exists, has name
                Traverser::from_vertex(VertexId(999)), // doesn't exist
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn returns_empty_for_nonexistent_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasStep::new("nonexistent_property");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasStep::new("name");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasStep::new("name");

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn debug_format() {
            let step = HasStep::new("age");
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasStep"));
            assert!(debug_str.contains("age"));
        }
    }

    mod has_not_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        fn create_graph_with_properties() -> Graph {
            let graph = Graph::new();

            // Vertex 0: person with name and age
            let v0 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            // Vertex 1: person with only name (no age)
            let v1 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props
            });

            // Vertex 2: software with name and version
            let v2 = graph.add_vertex("software", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Graph DB".to_string()));
                props.insert("version".to_string(), Value::Float(1.0));
                props
            });

            // Vertex 3: company with no properties
            graph.add_vertex("company", HashMap::new());

            // Edge 0: knows with since property
            graph
                .add_edge(v0, v1, "knows", {
                    let mut props = HashMap::new();
                    props.insert("since".to_string(), Value::Int(2020));
                    props
                })
                .unwrap();

            // Edge 1: uses with no properties
            graph.add_edge(v1, v2, "uses", HashMap::new()).unwrap();

            graph
        }

        #[test]
        fn new_creates_step_with_key() {
            let step = HasNotStep::new("email");
            assert_eq!(step.key, "email");
        }

        #[test]
        fn name_returns_has_not() {
            let step = HasNotStep::new("email");
            assert_eq!(step.name(), "hasNot");
        }

        #[test]
        fn clone_box_works() {
            let step = HasNotStep::new("email");
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "hasNot");
        }

        #[test]
        fn filters_out_vertices_with_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasNotStep::new("age");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has age - should be filtered
                Traverser::from_vertex(VertexId(1)), // no age - should pass
                Traverser::from_vertex(VertexId(2)), // no age - should pass
                Traverser::from_vertex(VertexId(3)), // no properties - should pass
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Vertices 1, 2, 3 don't have "age" property
            assert_eq!(output.len(), 3);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(1)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2)));
            assert_eq!(output[2].as_vertex_id(), Some(VertexId(3)));
        }

        #[test]
        fn keeps_vertices_without_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasNotStep::new("version");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // no version - should pass
                Traverser::from_vertex(VertexId(1)), // no version - should pass
                Traverser::from_vertex(VertexId(2)), // has version - should be filtered
                Traverser::from_vertex(VertexId(3)), // no version - should pass
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Vertices 0, 1, 3 don't have "version" property
            assert_eq!(output.len(), 3);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(1)));
            assert_eq!(output[2].as_vertex_id(), Some(VertexId(3)));
        }

        #[test]
        fn filters_out_edges_with_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasNotStep::new("since");

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // has since - should be filtered
                Traverser::from_edge(EdgeId(1)), // no since - should pass
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only edge 1 doesn't have "since" property
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(1)));
        }

        #[test]
        fn passes_through_non_element_values() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasNotStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has name - filtered
                Traverser::new(Value::Int(42)),      // not an element - passes through
                Traverser::new(Value::String("hello".to_string())), // not an element - passes through
                Traverser::new(Value::Bool(true)), // not an element - passes through
                Traverser::new(Value::Null),       // not an element - passes through
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Vertex 0 has name (filtered), all non-elements pass through
            assert_eq!(output.len(), 4);
            assert_eq!(output[0].value, Value::Int(42));
            assert_eq!(output[1].value, Value::String("hello".to_string()));
            assert_eq!(output[2].value, Value::Bool(true));
            assert_eq!(output[3].value, Value::Null);
        }

        #[test]
        fn nonexistent_vertices_pass_through() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasNotStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // exists, has name - filtered
                Traverser::from_vertex(VertexId(999)), // doesn't exist - passes through
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Vertex 0 filtered, vertex 999 passes (doesn't exist = no property)
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(999)));
        }

        #[test]
        fn nonexistent_edges_pass_through() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasNotStep::new("since");

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)),   // exists, has since - filtered
                Traverser::from_edge(EdgeId(999)), // doesn't exist - passes through
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Edge 0 filtered, edge 999 passes (doesn't exist = no property)
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(999)));
        }

        #[test]
        fn all_pass_for_nonexistent_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasNotStep::new("nonexistent_property");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
                Traverser::from_vertex(VertexId(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All vertices pass because none have "nonexistent_property"
            assert_eq!(output.len(), 4);
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasNotStep::new("name");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Vertex 3 has no properties, so it should pass
            let step = HasNotStep::new("name");

            let mut traverser = Traverser::from_vertex(VertexId(3));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn debug_format() {
            let step = HasNotStep::new("email");
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasNotStep"));
            assert!(debug_str.contains("email"));
        }

        #[test]
        fn mixed_vertices_and_edges() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // "name" property exists on vertices 0, 1, 2 but NOT on edges
            let step = HasNotStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has name - filtered
                Traverser::from_edge(EdgeId(0)),     // no name - passes
                Traverser::from_vertex(VertexId(3)), // no name - passes
                Traverser::from_edge(EdgeId(1)),     // no name - passes
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(3)));
            assert_eq!(output[2].as_edge_id(), Some(EdgeId(1)));
        }

        #[test]
        fn inverse_of_has_step() {
            // This test verifies that HasNotStep is the inverse of HasStep
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let has_step = HasStep::new("age");
            let has_not_step = HasNotStep::new("age");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has age
                Traverser::from_vertex(VertexId(1)), // no age
                Traverser::from_vertex(VertexId(2)), // no age
                Traverser::from_vertex(VertexId(3)), // no age
            ];

            let input_clone: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
                Traverser::from_vertex(VertexId(3)),
            ];

            let has_output: Vec<Traverser> =
                has_step.apply(&ctx, Box::new(input.into_iter())).collect();
            let has_not_output: Vec<Traverser> = has_not_step
                .apply(&ctx, Box::new(input_clone.into_iter()))
                .collect();

            // HasStep keeps vertices WITH age (vertex 0)
            assert_eq!(has_output.len(), 1);
            assert_eq!(has_output[0].as_vertex_id(), Some(VertexId(0)));

            // HasNotStep keeps vertices WITHOUT age (vertices 1, 2, 3)
            assert_eq!(has_not_output.len(), 3);
            assert_eq!(has_not_output[0].as_vertex_id(), Some(VertexId(1)));
            assert_eq!(has_not_output[1].as_vertex_id(), Some(VertexId(2)));
            assert_eq!(has_not_output[2].as_vertex_id(), Some(VertexId(3)));

            // Together they should cover all vertices (set union)
            assert_eq!(has_output.len() + has_not_output.len(), 4);
        }
    }

    mod has_value_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        fn create_graph_with_properties() -> Graph {
            let graph = Graph::new();

            // Vertex 0: person Alice, age 30
            let v0 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            // Vertex 1: person Bob, age 25
            let v1 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props.insert("age".to_string(), Value::Int(25));
                props
            });

            // Vertex 2: person Charlie, age 30
            let v2 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Charlie".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            // Vertex 3: software with version 1.0
            graph.add_vertex("software", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Graph DB".to_string()));
                props.insert("version".to_string(), Value::Float(1.0));
                props
            });

            // Edge 0: knows since 2020
            graph
                .add_edge(v0, v1, "knows", {
                    let mut props = HashMap::new();
                    props.insert("since".to_string(), Value::Int(2020));
                    props
                })
                .unwrap();

            // Edge 1: knows since 2019
            graph
                .add_edge(v1, v2, "knows", {
                    let mut props = HashMap::new();
                    props.insert("since".to_string(), Value::Int(2019));
                    props
                })
                .unwrap();

            graph
        }

        #[test]
        fn new_creates_step_with_key_and_value() {
            let step = HasValueStep::new("age", 30i64);
            assert_eq!(step.key, "age");
            assert_eq!(step.value, Value::Int(30));
        }

        #[test]
        fn name_returns_has() {
            let step = HasValueStep::new("age", 30i64);
            assert_eq!(step.name(), "has");
        }

        #[test]
        fn clone_box_works() {
            let step = HasValueStep::new("age", 30i64);
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "has");
        }

        #[test]
        fn filters_vertices_by_string_value() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasValueStep::new("name", "Alice");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Alice
                Traverser::from_vertex(VertexId(1)), // Bob
                Traverser::from_vertex(VertexId(2)), // Charlie
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_vertices_by_int_value() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasValueStep::new("age", 30i64);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30
                Traverser::from_vertex(VertexId(1)), // age 25
                Traverser::from_vertex(VertexId(2)), // age 30
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Vertices 0 and 2 have age 30
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2)));
        }

        #[test]
        fn filters_vertices_by_float_value() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasValueStep::new("version", Value::Float(1.0));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // no version
                Traverser::from_vertex(VertexId(3)), // version 1.0
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(3)));
        }

        #[test]
        fn filters_edges_by_value() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasValueStep::new("since", 2020i64);

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // since 2020
                Traverser::from_edge(EdgeId(1)), // since 2019
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn filters_out_vertices_with_different_value() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasValueStep::new("age", 99i64);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30
                Traverser::from_vertex(VertexId(1)), // age 25
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_vertices_without_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasValueStep::new("age", 30i64);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has age
                Traverser::from_vertex(VertexId(3)), // software, no age
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_non_element_values() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasValueStep::new("name", "Alice");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Alice - should pass
                Traverser::new(Value::Int(42)),      // not an element
                Traverser::new(Value::String("Alice".to_string())), // not an element, even with matching value
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_nonexistent_vertices() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasValueStep::new("name", "Alice");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),   // exists
                Traverser::from_vertex(VertexId(999)), // doesn't exist
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasValueStep::new("name", "Alice");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasValueStep::new("name", "Alice");

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn debug_format() {
            let step = HasValueStep::new("age", 30i64);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasValueStep"));
            assert!(debug_str.contains("age"));
        }
    }

    mod filter_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        fn create_test_graph() -> Graph {
            let graph = Graph::new();

            graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props.insert("age".to_string(), Value::Int(25));
                props
            });

            graph
        }

        #[test]
        fn new_creates_filter_step() {
            let step = FilterStep::new(|_ctx, _v| true);
            assert_eq!(step.name(), "filter");
        }

        #[test]
        fn name_returns_filter() {
            let step = FilterStep::new(|_ctx, _v| true);
            assert_eq!(step.name(), "filter");
        }

        #[test]
        fn clone_box_works() {
            let step = FilterStep::new(|_ctx, _v| true);
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "filter");
        }

        #[test]
        fn filters_with_always_true_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FilterStep::new(|_ctx, _v| true);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
        }

        #[test]
        fn filters_with_always_false_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FilterStep::new(|_ctx, _v| false);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_positive_integers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FilterStep::new(|_ctx, v| matches!(v, Value::Int(n) if *n > 0));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(-2)),
                Traverser::new(Value::Int(0)),
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(3));
        }

        #[test]
        fn filters_by_value_type() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FilterStep::new(|_ctx, v| v.is_vertex());

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::new(Value::Int(42)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::new(Value::String("hello".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert!(output[0].is_vertex());
            assert!(output[1].is_vertex());
        }

        #[test]
        fn can_access_execution_context() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Filter that checks if vertex exists in the graph
            let step = FilterStep::new(|ctx, v| {
                if let Some(id) = v.as_vertex_id() {
                    ctx.storage().get_vertex(id).is_some()
                } else {
                    false
                }
            });

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),   // exists
                Traverser::from_vertex(VertexId(999)), // doesn't exist
                Traverser::from_vertex(VertexId(1)),   // exists
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(1)));
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FilterStep::new(|_ctx, _v| true);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FilterStep::new(|_ctx, _v| true);

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn debug_format() {
            let step = FilterStep::new(|_ctx, _v| true);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("FilterStep"));
            assert!(debug_str.contains("<closure>"));
        }

        #[test]
        fn filter_with_string_matching() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step =
                FilterStep::new(|_ctx, v| matches!(v, Value::String(s) if s.starts_with("A")));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::String("Alice".to_string())),
                Traverser::new(Value::String("Bob".to_string())),
                Traverser::new(Value::String("Anna".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn filter_step_is_cloneable() {
            let step1 = FilterStep::new(|_ctx, v| matches!(v, Value::Int(n) if *n > 0));
            let step2 = step1.clone();

            // Both should work identically
            assert_eq!(step1.name(), step2.name());
        }
    }

    mod dedup_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        #[test]
        fn new_creates_dedup_step() {
            let step = DedupStep::new();
            assert_eq!(step.name(), "dedup");
        }

        #[test]
        fn default_creates_dedup_step() {
            let step = DedupStep::new();
            assert_eq!(step.name(), "dedup");
        }

        #[test]
        fn name_returns_dedup() {
            let step = DedupStep::new();
            assert_eq!(step.name(), "dedup");
        }

        #[test]
        fn clone_box_works() {
            let step = DedupStep::new();
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "dedup");
        }

        #[test]
        fn removes_duplicate_integers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(1)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
            assert_eq!(output[2].value, Value::Int(3));
        }

        #[test]
        fn removes_duplicate_strings() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::String("alice".to_string())),
                Traverser::new(Value::String("bob".to_string())),
                Traverser::new(Value::String("alice".to_string())),
                Traverser::new(Value::String("charlie".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::String("alice".to_string()));
            assert_eq!(output[1].value, Value::String("bob".to_string()));
            assert_eq!(output[2].value, Value::String("charlie".to_string()));
        }

        #[test]
        fn removes_duplicate_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(2)),
                Traverser::from_vertex(VertexId(1)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(1)));
            assert_eq!(output[2].as_vertex_id(), Some(VertexId(2)));
        }

        #[test]
        fn removes_duplicate_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)),
                Traverser::from_edge(EdgeId(1)),
                Traverser::from_edge(EdgeId(0)),
                Traverser::from_edge(EdgeId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
            assert_eq!(output[1].as_edge_id(), Some(EdgeId(1)));
            assert_eq!(output[2].as_edge_id(), Some(EdgeId(2)));
        }

        #[test]
        fn preserves_first_occurrence_order() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            // Values: 3, 1, 2, 1, 3, 2
            // Expected output: 3, 1, 2 (first occurrences in order)
            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(3));
            assert_eq!(output[1].value, Value::Int(1));
            assert_eq!(output[2].value, Value::Int(2));
        }

        #[test]
        fn handles_mixed_types() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            // Different types are always unique
            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::String("1".to_string())),
                Traverser::new(Value::Bool(true)),
                Traverser::new(Value::Int(1)), // duplicate
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::String("1".to_string()));
            assert_eq!(output[2].value, Value::Bool(true));
        }

        #[test]
        fn handles_floats() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Float(1.0)),
                Traverser::new(Value::Float(2.0)),
                Traverser::new(Value::Float(1.0)),
                Traverser::new(Value::Float(3.0)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
        }

        #[test]
        fn handles_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Null),
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Null),
                Traverser::new(Value::Int(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Null);
            assert_eq!(output[1].value, Value::Int(1));
            assert_eq!(output[2].value, Value::Int(2));
        }

        #[test]
        fn handles_boolean_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Bool(true)),
                Traverser::new(Value::Bool(false)),
                Traverser::new(Value::Bool(true)),
                Traverser::new(Value::Bool(false)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Bool(true));
            assert_eq!(output[1].value, Value::Bool(false));
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn single_element_passes_through() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(42));
        }

        #[test]
        fn all_unique_values_pass_through() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 4);
        }

        #[test]
        fn all_same_values_reduced_to_one() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::Int(42)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(42));
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn preserves_metadata_of_first_occurrence() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let mut t1 = Traverser::new(Value::Int(42));
            t1.extend_path_labeled("first");
            t1.loops = 1;

            let mut t2 = Traverser::new(Value::Int(42));
            t2.extend_path_labeled("second");
            t2.loops = 2;

            let input = vec![t1, t2];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only first traverser should pass through with its metadata
            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("first"));
            assert!(!output[0].path.has_label("second"));
            assert_eq!(output[0].loops, 1);
        }

        #[test]
        fn dedup_step_is_clone() {
            let step1 = DedupStep::new();
            let step2 = step1.clone();
            let _step3 = step1.clone();

            assert_eq!(step2.name(), "dedup");
        }

        #[test]
        fn debug_format() {
            let step = DedupStep::new();
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("DedupStep"));
        }

        #[test]
        fn handles_list_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::List(vec![Value::Int(1), Value::Int(2)])),
                Traverser::new(Value::List(vec![Value::Int(3)])),
                Traverser::new(Value::List(vec![Value::Int(1), Value::Int(2)])), // duplicate
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn handles_map_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupStep::new();

            let mut map1 = std::collections::HashMap::new();
            map1.insert("a".to_string(), Value::Int(1));

            let mut map2 = std::collections::HashMap::new();
            map2.insert("b".to_string(), Value::Int(2));

            let mut map3 = std::collections::HashMap::new();
            map3.insert("a".to_string(), Value::Int(1)); // same as map1

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Map(map1.into_iter().collect())),
                Traverser::new(Value::Map(map2.into_iter().collect())),
                Traverser::new(Value::Map(map3.into_iter().collect())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }
    }

    mod dedup_by_key_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        fn create_test_graph_with_ages() -> Graph {
            let graph = Graph::new();

            // Add vertices with age property
            graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });
            graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props.insert("age".to_string(), Value::Int(25));
                props
            });
            graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Charlie".to_string()));
                props.insert("age".to_string(), Value::Int(30)); // Same age as Alice
                props
            });
            graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Diana".to_string()));
                // No age property
                props
            });

            graph
        }

        #[test]
        fn new_creates_dedup_by_key_step() {
            let step = DedupByKeyStep::new("age");
            assert_eq!(step.key, "age");
        }

        #[test]
        fn name_returns_dedup() {
            let step = DedupByKeyStep::new("age");
            assert_eq!(step.name(), "dedup");
        }

        #[test]
        fn clone_box_works() {
            let step = DedupByKeyStep::new("age");
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "dedup");
        }

        #[test]
        fn dedup_by_property_keeps_first_occurrence() {
            let graph = create_test_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByKeyStep::new("age");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Alice, age 30
                Traverser::from_vertex(VertexId(1)), // Bob, age 25
                Traverser::from_vertex(VertexId(2)), // Charlie, age 30 (duplicate)
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Should keep Alice (age 30) and Bob (age 25), filter out Charlie
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0))); // Alice
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(1))); // Bob
        }

        #[test]
        fn missing_property_treated_as_null() {
            let graph = create_test_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByKeyStep::new("age");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(3)), // Diana, no age
                Traverser::from_vertex(VertexId(0)), // Alice, age 30
                Traverser::from_vertex(VertexId(1)), // Bob, age 25
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All three have unique keys: Null, 30, 25
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn multiple_elements_without_property_deduplicated() {
            let graph = create_test_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByKeyStep::new("nonexistent");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All elements have Null as the key, so only first passes
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn works_with_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByKeyStep::new("weight");

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Edge(EdgeId(0))),
                Traverser::new(Value::Edge(EdgeId(1))),
                Traverser::new(Value::Edge(EdgeId(2))),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All edges have no weight property, so treated as Null
            assert_eq!(output.len(), 1);
        }

        #[test]
        fn non_element_values_use_null_key() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByKeyStep::new("age");

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All non-elements use Null key, so only first passes
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(1));
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByKeyStep::new("age");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByKeyStep::new("age");

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn debug_format() {
            let step = DedupByKeyStep::new("age");
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("DedupByKeyStep"));
            assert!(debug_str.contains("age"));
        }
    }

    mod dedup_by_label_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        #[test]
        fn new_creates_dedup_by_label_step() {
            let step = DedupByLabelStep::new();
            assert_eq!(step.name(), "dedup");
        }

        #[test]
        fn default_creates_step() {
            let step = DedupByLabelStep::new();
            assert_eq!(step.name(), "dedup");
        }

        #[test]
        fn clone_box_works() {
            let step = DedupByLabelStep::new();
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "dedup");
        }

        #[test]
        fn dedup_by_label_keeps_first_per_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByLabelStep::new();

            // Our test graph has: person(0), person(1), software(2), company(3)
            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // person
                Traverser::from_vertex(VertexId(1)), // person (duplicate label)
                Traverser::from_vertex(VertexId(2)), // software
                Traverser::from_vertex(VertexId(3)), // company
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Should keep first of each label: person(0), software(2), company(3)
            assert_eq!(output.len(), 3);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2)));
            assert_eq!(output[2].as_vertex_id(), Some(VertexId(3)));
        }

        #[test]
        fn works_with_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByLabelStep::new();

            // Test graph edges: knows(0), uses(1), works_at(2)
            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Edge(EdgeId(0))), // knows
                Traverser::new(Value::Edge(EdgeId(1))), // uses
                Traverser::new(Value::Edge(EdgeId(2))), // works_at
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All edges have different labels
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn non_element_values_use_empty_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByLabelStep::new();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::String("hello".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All non-elements use empty string as label
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(1));
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByLabelStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = DedupByLabelStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn dedup_by_label_step_is_clone() {
            let step1 = DedupByLabelStep::new();
            let step2 = step1.clone();
            let _step3 = step1.clone();

            assert_eq!(step2.name(), "dedup");
        }

        #[test]
        fn debug_format() {
            let step = DedupByLabelStep::new();
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("DedupByLabelStep"));
        }
    }

    mod dedup_by_traversal_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;
        use crate::traversal::Traversal;

        #[test]
        fn name_returns_dedup() {
            let sub = Traversal::<Value, Value>::new();
            let step = DedupByTraversalStep::new(sub);
            assert_eq!(step.name(), "dedup");
        }

        #[test]
        fn clone_box_works() {
            let sub = Traversal::<Value, Value>::new();
            let step = DedupByTraversalStep::new(sub);
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "dedup");
        }

        #[test]
        fn dedup_by_traversal_uses_first_result() {
            use crate::traversal::transform::ValuesStep;

            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Dedup by the "name" property value
            let sub = Traversal::<Value, Value>::new().add_step(ValuesStep::new("name"));
            let step = DedupByTraversalStep::new(sub);

            // All test vertices have unique names, so all should pass
            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Alice
                Traverser::from_vertex(VertexId(1)), // Bob
                Traverser::from_vertex(VertexId(2)), // Graph DB
                Traverser::from_vertex(VertexId(3)), // TechCorp
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 4);
        }

        #[test]
        fn dedup_by_traversal_with_no_results_uses_null() {
            use crate::traversal::transform::ValuesStep;

            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Dedup by non-existent property
            let sub = Traversal::<Value, Value>::new().add_step(ValuesStep::new("nonexistent"));
            let step = DedupByTraversalStep::new(sub);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All have Null as key, so only first passes
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn dedup_by_label_traversal() {
            use crate::traversal::transform::LabelStep;

            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Dedup by label using a traversal
            let sub = Traversal::<Value, Value>::new().add_step(LabelStep::new());
            let step = DedupByTraversalStep::new(sub);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // person
                Traverser::from_vertex(VertexId(1)), // person (duplicate label)
                Traverser::from_vertex(VertexId(2)), // software
                Traverser::from_vertex(VertexId(3)), // company
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Should deduplicate by label
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let sub = Traversal::<Value, Value>::new();
            let step = DedupByTraversalStep::new(sub);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let sub = Traversal::<Value, Value>::new();
            let step = DedupByTraversalStep::new(sub);

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn debug_format() {
            let sub = Traversal::<Value, Value>::new();
            let step = DedupByTraversalStep::new(sub);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("DedupByTraversalStep"));
            assert!(debug_str.contains("<traversal>"));
        }
    }

    mod limit_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        #[test]
        fn new_creates_limit_step() {
            let step = LimitStep::new(5);
            assert_eq!(step.limit, 5);
        }

        #[test]
        fn name_returns_limit() {
            let step = LimitStep::new(5);
            assert_eq!(step.name(), "limit");
        }

        #[test]
        fn clone_box_works() {
            let step = LimitStep::new(5);
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "limit");
        }

        #[test]
        fn limits_traversers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LimitStep::new(3);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
            assert_eq!(output[2].value, Value::Int(3));
        }

        #[test]
        fn limit_zero_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LimitStep::new(0);

            let input: Vec<Traverser> =
                vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn limit_greater_than_input_returns_all() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LimitStep::new(100);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
        }

        #[test]
        fn limit_one_returns_first() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LimitStep::new(1);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(10));
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LimitStep::new(5);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LimitStep::new(1);

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn limit_step_is_clone() {
            let step1 = LimitStep::new(5);
            let step2 = step1.clone();
            let _step3 = step1.clone();

            assert_eq!(step2.limit, 5);
        }

        #[test]
        fn debug_format() {
            let step = LimitStep::new(5);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("LimitStep"));
            assert!(debug_str.contains("5"));
        }

        #[test]
        fn works_with_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LimitStep::new(2);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
                Traverser::from_vertex(VertexId(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(1)));
        }

        #[test]
        fn streaming_limit_matches_eager() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Eager execution
            let eager: Vec<Value> = g.v().limit(2).to_list();

            // Streaming execution
            let streaming: Vec<Value> = g.v().limit(2).iter().collect();

            assert_eq!(eager.len(), streaming.len());
            assert_eq!(eager.len(), 2);
        }

        #[test]
        fn streaming_limit_respects_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Should only return 1 even though there are 4 vertices
            let result: Vec<Value> = g.v().limit(1).iter().collect();
            assert_eq!(result.len(), 1);

            // Should return all 4 when limit is higher
            let result2: Vec<Value> = g.v().limit(100).iter().collect();
            assert_eq!(result2.len(), 4);
        }

        #[test]
        fn streaming_limit_zero_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            let result: Vec<Value> = g.v().limit(0).iter().collect();
            assert!(result.is_empty());
        }
    }

    mod skip_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        #[test]
        fn new_creates_skip_step() {
            let step = SkipStep::new(3);
            assert_eq!(step.count, 3);
        }

        #[test]
        fn name_returns_skip() {
            let step = SkipStep::new(3);
            assert_eq!(step.name(), "skip");
        }

        #[test]
        fn clone_box_works() {
            let step = SkipStep::new(3);
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "skip");
        }

        #[test]
        fn skips_traversers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SkipStep::new(2);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(3));
            assert_eq!(output[1].value, Value::Int(4));
            assert_eq!(output[2].value, Value::Int(5));
        }

        #[test]
        fn skip_zero_returns_all() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SkipStep::new(0);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
        }

        #[test]
        fn skip_greater_than_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SkipStep::new(100);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn skip_equal_to_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SkipStep::new(3);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn skip_one_less_than_input_returns_last() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SkipStep::new(4);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(5));
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SkipStep::new(5);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SkipStep::new(1);

            let t1 = Traverser::new(Value::Int(1));
            let mut t2 = Traverser::new(Value::Int(2));
            t2.extend_path_labeled("kept");
            t2.loops = 5;
            t2.bulk = 10;

            let input = vec![t1, t2];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("kept"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn skip_step_is_clone() {
            let step1 = SkipStep::new(3);
            let step2 = step1.clone();
            let _step3 = step1.clone();

            assert_eq!(step2.count, 3);
        }

        #[test]
        fn debug_format() {
            let step = SkipStep::new(3);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("SkipStep"));
            assert!(debug_str.contains("3"));
        }

        #[test]
        fn works_with_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SkipStep::new(2);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
                Traverser::from_vertex(VertexId(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(2)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(3)));
        }

        #[test]
        fn streaming_skip_matches_eager() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Eager execution
            let eager: Vec<Value> = g.v().skip(2).to_list();

            // Streaming execution
            let streaming: Vec<Value> = g.v().skip(2).iter().collect();

            assert_eq!(eager.len(), streaming.len());
            assert_eq!(eager.len(), 2); // 4 vertices - skip 2 = 2
        }

        #[test]
        fn streaming_skip_respects_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Skip 1, should return 3
            let result: Vec<Value> = g.v().skip(1).iter().collect();
            assert_eq!(result.len(), 3);

            // Skip all, should return 0
            let result2: Vec<Value> = g.v().skip(100).iter().collect();
            assert!(result2.is_empty());
        }

        #[test]
        fn streaming_skip_zero_returns_all() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            let result: Vec<Value> = g.v().skip(0).iter().collect();
            assert_eq!(result.len(), 4);
        }
    }

    mod tail_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        #[test]
        fn new_creates_tail_step() {
            let step = TailStep::new(3);
            assert_eq!(step.count, 3);
        }

        #[test]
        fn last_creates_tail_step_with_count_one() {
            let step = TailStep::last();
            assert_eq!(step.count, 1);
        }

        #[test]
        fn default_creates_last() {
            let step = TailStep::default();
            assert_eq!(step.count, 1);
        }

        #[test]
        fn name_returns_tail() {
            let step = TailStep::new(3);
            assert_eq!(step.name(), "tail");
        }

        #[test]
        fn clone_box_works() {
            let step = TailStep::new(3);
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "tail");
        }

        #[test]
        fn tail_returns_last_n_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TailStep::new(3);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(3));
            assert_eq!(output[1].value, Value::Int(4));
            assert_eq!(output[2].value, Value::Int(5));
        }

        #[test]
        fn tail_last_returns_single_element() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TailStep::last();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(5));
        }

        #[test]
        fn tail_zero_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TailStep::new(0);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn tail_greater_than_input_returns_all() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TailStep::new(100);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
            assert_eq!(output[2].value, Value::Int(3));
        }

        #[test]
        fn tail_equal_to_input_returns_all() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TailStep::new(3);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TailStep::new(5);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TailStep::new(1);

            let t1 = Traverser::new(Value::Int(1));
            let mut t2 = Traverser::new(Value::Int(2));
            t2.extend_path_labeled("kept");
            t2.loops = 5;
            t2.bulk = 10;

            let input = vec![t1, t2];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("kept"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn tail_step_is_copy() {
            let step1 = TailStep::new(3);
            let step2 = step1; // Copy
            let _step3 = step1; // Can still use step1

            assert_eq!(step2.count, 3);
        }

        #[test]
        fn debug_format() {
            let step = TailStep::new(3);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("TailStep"));
            assert!(debug_str.contains("3"));
        }

        #[test]
        fn works_with_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TailStep::new(2);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
                Traverser::from_vertex(VertexId(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(2)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(3)));
        }

        #[test]
        fn works_with_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TailStep::new(2);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Edge(EdgeId(0))),
                Traverser::new(Value::Edge(EdgeId(1))),
                Traverser::new(Value::Edge(EdgeId(2))),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Edge(EdgeId(1)));
            assert_eq!(output[1].value, Value::Edge(EdgeId(2)));
        }

        #[test]
        fn preserves_order() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TailStep::new(4);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(30)),
                Traverser::new(Value::Int(40)),
                Traverser::new(Value::Int(50)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 4);
            // Elements should be in original order (20, 30, 40, 50)
            assert_eq!(output[0].value, Value::Int(20));
            assert_eq!(output[1].value, Value::Int(30));
            assert_eq!(output[2].value, Value::Int(40));
            assert_eq!(output[3].value, Value::Int(50));
        }
    }

    mod coin_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        #[test]
        fn new_creates_coin_step() {
            let step = CoinStep::new(0.5);
            assert!((step.probability() - 0.5).abs() < f64::EPSILON);
        }

        #[test]
        fn new_clamps_probability_above_one() {
            let step = CoinStep::new(1.5);
            assert!((step.probability() - 1.0).abs() < f64::EPSILON);
        }

        #[test]
        fn new_clamps_probability_below_zero() {
            let step = CoinStep::new(-0.5);
            assert!(step.probability().abs() < f64::EPSILON);
        }

        #[test]
        fn always_returns_probability_one() {
            let step = CoinStep::always();
            assert!((step.probability() - 1.0).abs() < f64::EPSILON);
        }

        #[test]
        fn never_returns_probability_zero() {
            let step = CoinStep::never();
            assert!(step.probability().abs() < f64::EPSILON);
        }

        #[test]
        fn default_is_always() {
            let step = CoinStep::default();
            assert!((step.probability() - 1.0).abs() < f64::EPSILON);
        }

        #[test]
        fn name_returns_coin() {
            let step = CoinStep::new(0.5);
            assert_eq!(step.name(), "coin");
        }

        #[test]
        fn clone_box_works() {
            let step = CoinStep::new(0.5);
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "coin");
        }

        #[test]
        fn coin_zero_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CoinStep::new(0.0);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn coin_one_returns_all() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CoinStep::new(1.0);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 5);
        }

        #[test]
        fn coin_never_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CoinStep::never();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn coin_always_returns_all() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CoinStep::always();

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
        }

        #[test]
        fn coin_half_returns_approximately_half_statistical() {
            // Statistical test with large sample size
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CoinStep::new(0.5);

            // Create a large input for statistical significance
            let input: Vec<Traverser> = (0..1000).map(|i| Traverser::new(Value::Int(i))).collect();

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // With 1000 samples and p=0.5, expect ~500 with stddev ~15.8
            // Allow a generous tolerance of ±100 (about 6 standard deviations)
            let count = output.len();
            assert!(
                count > 400 && count < 600,
                "Expected approximately 500 results, got {}",
                count
            );
        }

        #[test]
        fn coin_tenth_returns_approximately_tenth_statistical() {
            // Statistical test with large sample size
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CoinStep::new(0.1);

            // Create a large input for statistical significance
            let input: Vec<Traverser> = (0..1000).map(|i| Traverser::new(Value::Int(i))).collect();

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // With 1000 samples and p=0.1, expect ~100 with stddev ~9.5
            // Allow a generous tolerance of ±50
            let count = output.len();
            assert!(
                count > 50 && count < 150,
                "Expected approximately 100 results, got {}",
                count
            );
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CoinStep::new(0.5);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Use coin(1.0) to ensure the traverser passes
            let step = CoinStep::new(1.0);

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn coin_step_is_copy() {
            let step1 = CoinStep::new(0.5);
            let step2 = step1; // Copy
            let _step3 = step1; // Can still use step1

            assert!((step2.probability() - 0.5).abs() < f64::EPSILON);
        }

        #[test]
        fn debug_format() {
            let step = CoinStep::new(0.5);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("CoinStep"));
            assert!(debug_str.contains("0.5"));
        }

        #[test]
        fn works_with_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CoinStep::new(1.0);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
        }

        #[test]
        fn works_with_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CoinStep::new(1.0);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Edge(EdgeId(0))),
                Traverser::new(Value::Edge(EdgeId(1))),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn nan_probability_treated_as_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // NaN comparison with clamp should result in 0.0
            let step = CoinStep::new(f64::NAN);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // NaN.clamp(0.0, 1.0) returns NaN in Rust, which will fail < comparison
            // So effectively, no elements should pass
            assert!(output.is_empty());
        }
    }

    mod sample_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;
        use std::collections::HashSet;

        #[test]
        fn new_creates_sample_step() {
            let step = SampleStep::new(5);
            assert_eq!(step.count(), 5);
        }

        #[test]
        fn default_creates_sample_one() {
            let step = SampleStep::default();
            assert_eq!(step.count(), 1);
        }

        #[test]
        fn name_returns_sample() {
            let step = SampleStep::new(5);
            assert_eq!(step.name(), "sample");
        }

        #[test]
        fn clone_box_works() {
            let step = SampleStep::new(5);
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "sample");
        }

        #[test]
        fn sample_zero_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SampleStep::new(0);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn sample_larger_than_input_returns_all() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SampleStep::new(10);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
        }

        #[test]
        fn sample_equal_to_input_returns_all() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SampleStep::new(5);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 5);
        }

        #[test]
        fn sample_returns_exactly_n_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SampleStep::new(5);

            let input: Vec<Traverser> = (0..100).map(|i| Traverser::new(Value::Int(i))).collect();

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 5);
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SampleStep::new(5);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SampleStep::new(1);

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn sample_step_is_copy() {
            let step1 = SampleStep::new(5);
            let step2 = step1; // Copy
            let _step3 = step1; // Can still use step1

            assert_eq!(step2.count(), 5);
        }

        #[test]
        fn debug_format() {
            let step = SampleStep::new(5);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("SampleStep"));
            assert!(debug_str.contains("5"));
        }

        #[test]
        fn works_with_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SampleStep::new(2);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn works_with_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SampleStep::new(2);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Edge(EdgeId(0))),
                Traverser::new(Value::Edge(EdgeId(1))),
                Traverser::new(Value::Edge(EdgeId(2))),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn sample_elements_come_from_input() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SampleStep::new(5);

            let input_values: Vec<i64> = (0..100).collect();
            let input: Vec<Traverser> = input_values
                .iter()
                .map(|&i| Traverser::new(Value::Int(i)))
                .collect();

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All output values should be from the original input
            let input_set: HashSet<i64> = input_values.into_iter().collect();
            for t in output {
                if let Value::Int(v) = t.value {
                    assert!(
                        input_set.contains(&v),
                        "Output value {} not in input set",
                        v
                    );
                } else {
                    panic!("Expected Int value");
                }
            }
        }

        #[test]
        fn sample_returns_distinct_elements_from_distinct_input() {
            // When input has all distinct elements, output should have distinct elements
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SampleStep::new(5);

            let input: Vec<Traverser> = (0..100).map(|i| Traverser::new(Value::Int(i))).collect();

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All output values should be unique (since input had unique values)
            let output_values: Vec<i64> = output
                .iter()
                .map(|t| match &t.value {
                    Value::Int(v) => *v,
                    _ => panic!("Expected Int"),
                })
                .collect();

            let unique_values: HashSet<i64> = output_values.iter().copied().collect();
            assert_eq!(
                unique_values.len(),
                output_values.len(),
                "Sampled elements should be unique when input is unique"
            );
        }

        #[test]
        fn distribution_is_approximately_uniform_statistical() {
            // Statistical test to verify reservoir sampling gives roughly uniform distribution
            // Run multiple samples and check that each element appears with roughly equal frequency
            let graph = create_test_graph();
            let snapshot = graph.snapshot();

            // Track how often each value is sampled
            let mut counts: HashMap<i64, usize> = HashMap::new();
            let input_size = 100;
            let sample_size = 10;
            let num_trials = 1000;

            for _ in 0..num_trials {
                let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());
                let step = SampleStep::new(sample_size);

                let input: Vec<Traverser> = (0..input_size as i64)
                    .map(|i| Traverser::new(Value::Int(i)))
                    .collect();

                let output: Vec<Traverser> =
                    step.apply(&ctx, Box::new(input.into_iter())).collect();

                for t in output {
                    if let Value::Int(v) = t.value {
                        *counts.entry(v).or_insert(0) += 1;
                    }
                }
            }

            // Each element should appear approximately sample_size/input_size * num_trials times
            // = 10/100 * 1000 = 100 times on average
            let expected = (sample_size as f64 / input_size as f64) * num_trials as f64;

            // Check that each value appears within a reasonable range
            // Allow ±50% tolerance for statistical variation
            let min_expected = (expected * 0.5) as usize;
            let max_expected = (expected * 1.5) as usize;

            // Not all values need to be sampled in 1000 trials, but most should
            // Check that at least 90% of values are within expected range
            let within_range = counts
                .values()
                .filter(|&&c| c >= min_expected && c <= max_expected)
                .count();

            assert!(
                within_range >= (input_size as f64 * 0.8) as usize,
                "Expected at least 80% of values to be sampled with approximately uniform frequency"
            );
        }
    }

    mod has_key_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        fn create_property_map(key: &str, value: Value) -> Value {
            let mut map = crate::value::ValueMap::new();
            map.insert("key".to_string(), Value::String(key.to_string()));
            map.insert("value".to_string(), value);
            Value::Map(map)
        }

        #[test]
        fn new_creates_has_key_step() {
            let step = HasKeyStep::new("name");
            assert_eq!(step.keys(), &["name".to_string()]);
        }

        #[test]
        fn any_creates_multi_key_step() {
            let step = HasKeyStep::any(["name", "age", "email"]);
            assert_eq!(step.keys().len(), 3);
        }

        #[test]
        fn name_returns_has_key() {
            let step = HasKeyStep::new("name");
            assert_eq!(step.name(), "hasKey");
        }

        #[test]
        fn clone_box_works() {
            let step = HasKeyStep::new("name");
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "hasKey");
        }

        #[test]
        fn filters_property_map_by_single_key() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasKeyStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Alice".to_string()),
                )),
                Traverser::new(create_property_map("age", Value::Int(30))),
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Bob".to_string()),
                )),
                Traverser::new(create_property_map(
                    "email",
                    Value::String("test@test.com".to_string()),
                )),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            // Check that all results have key "name"
            for t in &output {
                if let Value::Map(map) = &t.value {
                    assert_eq!(map.get("key"), Some(&Value::String("name".to_string())));
                } else {
                    panic!("Expected Map value");
                }
            }
        }

        #[test]
        fn filters_property_map_by_multiple_keys() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasKeyStep::any(["name", "age"]);

            let input: Vec<Traverser> = vec![
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Alice".to_string()),
                )),
                Traverser::new(create_property_map("age", Value::Int(30))),
                Traverser::new(create_property_map(
                    "email",
                    Value::String("test@test.com".to_string()),
                )),
                Traverser::new(create_property_map(
                    "city",
                    Value::String("NYC".to_string()),
                )),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn non_map_values_filtered_out() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasKeyStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::String("hello".to_string())),
                Traverser::from_vertex(VertexId(0)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn map_without_key_field_filtered_out() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasKeyStep::new("name");

            // Create a map without a "key" field
            let mut map = crate::value::ValueMap::new();
            map.insert("value".to_string(), Value::String("Alice".to_string()));

            let input: Vec<Traverser> = vec![Traverser::new(Value::Map(map))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasKeyStep::new("name");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasKeyStep::new("name");

            let mut traverser = Traverser::new(create_property_map(
                "name",
                Value::String("Alice".to_string()),
            ));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn debug_format() {
            let step = HasKeyStep::new("name");
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasKeyStep"));
            assert!(debug_str.contains("name"));
        }
    }

    mod has_prop_value_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        fn create_property_map(key: &str, value: Value) -> Value {
            let mut map = crate::value::ValueMap::new();
            map.insert("key".to_string(), Value::String(key.to_string()));
            map.insert("value".to_string(), value);
            Value::Map(map)
        }

        #[test]
        fn new_creates_has_prop_value_step() {
            let step = HasPropValueStep::new("Alice");
            assert_eq!(step.values().len(), 1);
        }

        #[test]
        fn any_creates_multi_value_step() {
            let step = HasPropValueStep::any(["Alice", "Bob", "Carol"]);
            assert_eq!(step.values().len(), 3);
        }

        #[test]
        fn name_returns_has_value() {
            let step = HasPropValueStep::new("Alice");
            assert_eq!(step.name(), "hasValue");
        }

        #[test]
        fn clone_box_works() {
            let step = HasPropValueStep::new("Alice");
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "hasValue");
        }

        #[test]
        fn filters_property_map_by_single_string_value() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasPropValueStep::new("Alice");

            let input: Vec<Traverser> = vec![
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Alice".to_string()),
                )),
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Bob".to_string()),
                )),
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Alice".to_string()),
                )),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn filters_property_map_by_single_int_value() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasPropValueStep::new(30i64);

            let input: Vec<Traverser> = vec![
                Traverser::new(create_property_map("age", Value::Int(30))),
                Traverser::new(create_property_map("age", Value::Int(25))),
                Traverser::new(create_property_map("age", Value::Int(30))),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn filters_property_map_by_multiple_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasPropValueStep::any(["Alice", "Carol"]);

            let input: Vec<Traverser> = vec![
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Alice".to_string()),
                )),
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Bob".to_string()),
                )),
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Carol".to_string()),
                )),
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Dave".to_string()),
                )),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn non_map_values_filtered_out() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasPropValueStep::new("Alice");

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::String("Alice".to_string())),
                Traverser::from_vertex(VertexId(0)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn map_without_value_field_filtered_out() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasPropValueStep::new("Alice");

            // Create a map without a "value" field
            let mut map = crate::value::ValueMap::new();
            map.insert("key".to_string(), Value::String("name".to_string()));

            let input: Vec<Traverser> = vec![Traverser::new(Value::Map(map))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasPropValueStep::new("Alice");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasPropValueStep::new("Alice");

            let mut traverser = Traverser::new(create_property_map(
                "name",
                Value::String("Alice".to_string()),
            ));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn debug_format() {
            let step = HasPropValueStep::new("Alice");
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasPropValueStep"));
        }

        #[test]
        fn works_with_mixed_value_types() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Create step that matches Int(30)
            let step = HasPropValueStep::new(30i64);

            let input: Vec<Traverser> = vec![
                Traverser::new(create_property_map("age", Value::Int(30))),
                // This shouldn't match because "30" string != 30 int
                Traverser::new(create_property_map("id", Value::String("30".to_string()))),
                Traverser::new(create_property_map("count", Value::Int(30))),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only the Int(30) values should match
            assert_eq!(output.len(), 2);
        }
    }

    mod range_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        #[test]
        fn new_creates_range_step() {
            let step = RangeStep::new(2, 5);
            assert_eq!(step.start, 2);
            assert_eq!(step.end, 5);
        }

        #[test]
        fn name_returns_range() {
            let step = RangeStep::new(2, 5);
            assert_eq!(step.name(), "range");
        }

        #[test]
        fn clone_box_works() {
            let step = RangeStep::new(2, 5);
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "range");
        }

        #[test]
        fn range_selects_middle_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = RangeStep::new(2, 5);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(0)),
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
                Traverser::new(Value::Int(6)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(2));
            assert_eq!(output[1].value, Value::Int(3));
            assert_eq!(output[2].value, Value::Int(4));
        }

        #[test]
        fn range_from_start() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = RangeStep::new(0, 3);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
            assert_eq!(output[2].value, Value::Int(3));
        }

        #[test]
        fn range_to_end() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = RangeStep::new(3, 100);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(4));
            assert_eq!(output[1].value, Value::Int(5));
        }

        #[test]
        fn range_equal_start_end_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = RangeStep::new(3, 3);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn range_end_less_than_start_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = RangeStep::new(5, 2);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn range_start_beyond_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = RangeStep::new(10, 20);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn range_single_element() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = RangeStep::new(2, 3);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(3));
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = RangeStep::new(0, 5);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = RangeStep::new(1, 2);

            let t1 = Traverser::new(Value::Int(1));
            let mut t2 = Traverser::new(Value::Int(2));
            t2.extend_path_labeled("kept");
            t2.loops = 5;
            t2.bulk = 10;
            let t3 = Traverser::new(Value::Int(3));

            let input = vec![t1, t2, t3];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("kept"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn range_step_is_clone() {
            let step1 = RangeStep::new(2, 5);
            let step2 = step1.clone();
            let _step3 = step1.clone();

            assert_eq!(step2.start, 2);
            assert_eq!(step2.end, 5);
        }

        #[test]
        fn debug_format() {
            let step = RangeStep::new(2, 5);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("RangeStep"));
            assert!(debug_str.contains("2"));
            assert!(debug_str.contains("5"));
        }

        #[test]
        fn works_with_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = RangeStep::new(1, 3);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
                Traverser::from_vertex(VertexId(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(1)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2)));
        }

        #[test]
        fn range_equivalent_to_skip_then_limit() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // range(2, 5) should be equivalent to skip(2).limit(3)
            let range_step = RangeStep::new(2, 5);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(0)),
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(5)),
                Traverser::new(Value::Int(6)),
            ];

            let range_output: Vec<Value> = range_step
                .apply(&ctx, Box::new(input.clone().into_iter()))
                .map(|t| t.value)
                .collect();

            // Manual skip + limit
            let skip_limit_output: Vec<Value> =
                input.into_iter().skip(2).take(3).map(|t| t.value).collect();

            assert_eq!(range_output, skip_limit_output);
        }

        #[test]
        fn streaming_range_matches_eager() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Eager execution: range(1, 3) = skip(1).limit(2)
            let eager: Vec<Value> = g.v().range(1, 3).to_list();

            // Streaming execution
            let streaming: Vec<Value> = g.v().range(1, 3).iter().collect();

            assert_eq!(eager.len(), streaming.len());
            assert_eq!(eager.len(), 2); // indices 1 and 2
        }

        #[test]
        fn streaming_range_respects_bounds() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // range(0, 2) = first 2 elements
            let result: Vec<Value> = g.v().range(0, 2).iter().collect();
            assert_eq!(result.len(), 2);

            // range(2, 4) = last 2 elements
            let result2: Vec<Value> = g.v().range(2, 4).iter().collect();
            assert_eq!(result2.len(), 2);

            // range beyond bounds
            let result3: Vec<Value> = g.v().range(10, 20).iter().collect();
            assert!(result3.is_empty());
        }

        #[test]
        fn streaming_range_empty_when_start_equals_end() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            let result: Vec<Value> = g.v().range(2, 2).iter().collect();
            assert!(result.is_empty());
        }
    }

    mod has_id_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        #[test]
        fn vertex_creates_single_vertex_id_step() {
            let step = HasIdStep::vertex(VertexId(42));
            assert_eq!(step.ids.len(), 1);
            assert_eq!(step.ids[0], Value::Vertex(VertexId(42)));
        }

        #[test]
        fn vertices_creates_multi_vertex_id_step() {
            let step = HasIdStep::vertices(vec![VertexId(1), VertexId(2), VertexId(3)]);
            assert_eq!(step.ids.len(), 3);
            assert_eq!(step.ids[0], Value::Vertex(VertexId(1)));
            assert_eq!(step.ids[1], Value::Vertex(VertexId(2)));
            assert_eq!(step.ids[2], Value::Vertex(VertexId(3)));
        }

        #[test]
        fn edge_creates_single_edge_id_step() {
            let step = HasIdStep::edge(EdgeId(99));
            assert_eq!(step.ids.len(), 1);
            assert_eq!(step.ids[0], Value::Edge(EdgeId(99)));
        }

        #[test]
        fn edges_creates_multi_edge_id_step() {
            let step = HasIdStep::edges(vec![EdgeId(10), EdgeId(20)]);
            assert_eq!(step.ids.len(), 2);
            assert_eq!(step.ids[0], Value::Edge(EdgeId(10)));
            assert_eq!(step.ids[1], Value::Edge(EdgeId(20)));
        }

        #[test]
        fn from_value_creates_single_id_step() {
            let step = HasIdStep::from_value(Value::Vertex(VertexId(5)));
            assert_eq!(step.ids.len(), 1);
            assert_eq!(step.ids[0], Value::Vertex(VertexId(5)));
        }

        #[test]
        fn from_values_creates_multi_id_step() {
            let step = HasIdStep::from_values(vec![
                Value::Vertex(VertexId(1)),
                Value::Vertex(VertexId(2)),
            ]);
            assert_eq!(step.ids.len(), 2);
        }

        #[test]
        fn name_returns_has_id() {
            let step = HasIdStep::vertex(VertexId(1));
            assert_eq!(step.name(), "hasId");
        }

        #[test]
        fn clone_box_works() {
            let step = HasIdStep::vertex(VertexId(1));
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "hasId");
        }

        #[test]
        fn filters_vertices_by_single_id() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasIdStep::vertex(VertexId(1));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)), // Should match
                Traverser::from_vertex(VertexId(2)),
                Traverser::from_vertex(VertexId(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(1)));
        }

        #[test]
        fn filters_vertices_by_multiple_ids() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasIdStep::vertices(vec![VertexId(0), VertexId(2)]);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Should match
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)), // Should match
                Traverser::from_vertex(VertexId(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2)));
        }

        #[test]
        fn filters_edges_by_single_id() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasIdStep::edge(EdgeId(1));

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)),
                Traverser::from_edge(EdgeId(1)), // Should match
                Traverser::from_edge(EdgeId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(1)));
        }

        #[test]
        fn filters_edges_by_multiple_ids() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasIdStep::edges(vec![EdgeId(0), EdgeId(2)]);

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // Should match
                Traverser::from_edge(EdgeId(1)),
                Traverser::from_edge(EdgeId(2)), // Should match
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
            assert_eq!(output[1].as_edge_id(), Some(EdgeId(2)));
        }

        #[test]
        fn filters_out_non_element_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasIdStep::vertex(VertexId(0));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),            // Should match
                Traverser::new(Value::Int(0)),                  // Not a vertex, should be filtered
                Traverser::new(Value::String("0".to_string())), // Not a vertex
                Traverser::new(Value::Bool(false)),             // Not a vertex
                Traverser::new(Value::Null),                    // Not a vertex
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn vertex_id_step_does_not_match_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Looking for vertex ID 0, but passing edges
            let step = HasIdStep::vertex(VertexId(0));

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // Same numeric value, but an edge
                Traverser::from_edge(EdgeId(1)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // None should match because they're edges, not vertices
            assert!(output.is_empty());
        }

        #[test]
        fn edge_id_step_does_not_match_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Looking for edge ID 0, but passing vertices
            let step = HasIdStep::edge(EdgeId(0));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Same numeric value, but a vertex
                Traverser::from_vertex(VertexId(1)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // None should match because they're vertices, not edges
            assert!(output.is_empty());
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasIdStep::vertex(VertexId(1));
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn returns_empty_for_nonexistent_id() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasIdStep::vertex(VertexId(999));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasIdStep::vertex(VertexId(1));

            let mut traverser = Traverser::from_vertex(VertexId(1));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn mixed_vertex_and_edge_input() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Looking for vertex ID 1
            let step = HasIdStep::vertex(VertexId(1));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_edge(EdgeId(1)), // Same numeric value but edge
                Traverser::from_vertex(VertexId(1)), // Should match
                Traverser::from_edge(EdgeId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only vertex ID 1 should match
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(1)));
        }

        #[test]
        fn from_values_with_mixed_types() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Looking for either vertex 0 or edge 1
            let step =
                HasIdStep::from_values(vec![Value::Vertex(VertexId(0)), Value::Edge(EdgeId(1))]);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Should match
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_edge(EdgeId(0)),
                Traverser::from_edge(EdgeId(1)), // Should match
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_edge_id(), Some(EdgeId(1)));
        }

        #[test]
        fn debug_format() {
            let step = HasIdStep::vertices(vec![VertexId(1), VertexId(2)]);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasIdStep"));
            assert!(debug_str.contains("ids"));
        }

        #[test]
        fn clone_works() {
            let step = HasIdStep::vertex(VertexId(42));
            let cloned = step.clone();
            assert_eq!(cloned.ids.len(), 1);
            assert_eq!(cloned.ids[0], Value::Vertex(VertexId(42)));
        }
    }

    mod has_where_step_tests {
        use super::*;
        use crate::traversal::predicate::p;
        use crate::traversal::step::DynStep;

        fn create_graph_with_ages() -> Graph {
            let graph = Graph::new();

            // Vertex 0: Alice, age 30
            let v0 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            // Vertex 1: Bob, age 25
            let v1 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props.insert("age".to_string(), Value::Int(25));
                props
            });

            // Vertex 2: Charlie, age 35
            let v2 = graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Charlie".to_string()));
                props.insert("age".to_string(), Value::Int(35));
                props
            });

            // Vertex 3: Dave, no age
            graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Dave".to_string()));
                props
            });

            // Vertex 4: Software with version 1.5
            graph.add_vertex("software", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Graph DB".to_string()));
                props.insert("version".to_string(), Value::Float(1.5));
                props
            });

            // Edge 0: knows with weight
            graph
                .add_edge(v0, v1, "knows", {
                    let mut props = HashMap::new();
                    props.insert("weight".to_string(), Value::Float(0.8));
                    props
                })
                .unwrap();

            // Edge 1: knows with weight
            graph
                .add_edge(v1, v2, "knows", {
                    let mut props = HashMap::new();
                    props.insert("weight".to_string(), Value::Float(0.3));
                    props
                })
                .unwrap();

            graph
        }

        #[test]
        fn new_creates_has_where_step() {
            let step = HasWhereStep::new("age", p::gte(18));
            assert_eq!(step.key, "age");
        }

        #[test]
        fn name_returns_has() {
            let step = HasWhereStep::new("age", p::gte(18));
            assert_eq!(step.name(), "has");
        }

        #[test]
        fn clone_box_works() {
            let step = HasWhereStep::new("age", p::gte(18));
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "has");
        }

        #[test]
        fn filters_vertices_with_gte_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::gte(30i64));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30, should pass
                Traverser::from_vertex(VertexId(1)), // age 25, should fail
                Traverser::from_vertex(VertexId(2)), // age 35, should pass
                Traverser::from_vertex(VertexId(3)), // no age, should fail
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0))); // Alice, 30
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2))); // Charlie, 35
        }

        #[test]
        fn filters_vertices_with_lt_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::lt(30i64));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30, should fail
                Traverser::from_vertex(VertexId(1)), // age 25, should pass
                Traverser::from_vertex(VertexId(2)), // age 35, should fail
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(1))); // Bob, 25
        }

        #[test]
        fn filters_vertices_with_between_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::between(26i64, 34i64));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30, should pass
                Traverser::from_vertex(VertexId(1)), // age 25, should fail (below range)
                Traverser::from_vertex(VertexId(2)), // age 35, should fail (above range)
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0))); // Alice, 30
        }

        #[test]
        fn filters_vertices_with_eq_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::eq(30i64));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30, should pass
                Traverser::from_vertex(VertexId(1)), // age 25, should fail
                Traverser::from_vertex(VertexId(2)), // age 35, should fail
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0))); // Alice
        }

        #[test]
        fn filters_vertices_with_neq_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::neq(30i64));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30, should fail
                Traverser::from_vertex(VertexId(1)), // age 25, should pass
                Traverser::from_vertex(VertexId(2)), // age 35, should pass
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(1))); // Bob
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2))); // Charlie
        }

        #[test]
        fn filters_vertices_with_within_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::within([25i64, 35i64]));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30, not in set
                Traverser::from_vertex(VertexId(1)), // age 25, in set
                Traverser::from_vertex(VertexId(2)), // age 35, in set
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(1))); // Bob, 25
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2))); // Charlie, 35
        }

        #[test]
        fn filters_vertices_with_without_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::without([25i64, 35i64]));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30, not in set -> passes
                Traverser::from_vertex(VertexId(1)), // age 25, in set -> fails
                Traverser::from_vertex(VertexId(2)), // age 35, in set -> fails
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0))); // Alice, 30
        }

        #[test]
        fn filters_vertices_with_string_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("name", p::starting_with("A"));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Alice, should pass
                Traverser::from_vertex(VertexId(1)), // Bob, should fail
                Traverser::from_vertex(VertexId(2)), // Charlie, should fail
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0))); // Alice
        }

        #[test]
        fn filters_vertices_with_containing_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("name", p::containing("li"));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Alice, contains "li" -> pass
                Traverser::from_vertex(VertexId(1)), // Bob, no "li" -> fail
                Traverser::from_vertex(VertexId(2)), // Charlie, contains "li" -> pass
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0))); // Alice
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2))); // Charlie
        }

        #[test]
        fn filters_vertices_with_and_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // age >= 25 AND age < 35
            let step = HasWhereStep::new("age", p::and(p::gte(25i64), p::lt(35i64)));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30, passes both
                Traverser::from_vertex(VertexId(1)), // age 25, passes both
                Traverser::from_vertex(VertexId(2)), // age 35, fails lt
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0))); // Alice, 30
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(1))); // Bob, 25
        }

        #[test]
        fn filters_vertices_with_or_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // age == 25 OR age == 35
            let step = HasWhereStep::new("age", p::or(p::eq(25i64), p::eq(35i64)));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30, fails both
                Traverser::from_vertex(VertexId(1)), // age 25, passes
                Traverser::from_vertex(VertexId(2)), // age 35, passes
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(1))); // Bob
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2))); // Charlie
        }

        #[test]
        fn filters_vertices_with_not_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // NOT age == 30
            let step = HasWhereStep::new("age", p::not(p::eq(30i64)));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30, fails
                Traverser::from_vertex(VertexId(1)), // age 25, passes
                Traverser::from_vertex(VertexId(2)), // age 35, passes
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(1))); // Bob
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2))); // Charlie
        }

        #[test]
        fn filters_edges_with_predicate() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("weight", p::gt(0.5));

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // weight 0.8, should pass
                Traverser::from_edge(EdgeId(1)), // weight 0.3, should fail
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn filters_out_vertices_without_property() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::gte(0i64)); // Would match any age

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has age, should pass
                Traverser::from_vertex(VertexId(3)), // no age, should fail
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_non_element_values() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::gte(0i64));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),        // vertex, should pass
                Traverser::new(Value::Int(30)),             // not an element
                Traverser::new(Value::String("30".into())), // not an element
                Traverser::new(Value::Bool(true)),          // not an element
                Traverser::new(Value::Null),                // not an element
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_nonexistent_vertices() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::gte(0i64));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),   // exists
                Traverser::from_vertex(VertexId(999)), // doesn't exist
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::gte(18i64));
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("age", p::gte(18i64));

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn debug_format() {
            let step = HasWhereStep::new("age", p::gte(18i64));
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasWhereStep"));
            assert!(debug_str.contains("age"));
            assert!(debug_str.contains("<predicate>"));
        }

        #[test]
        fn clone_works() {
            let step = HasWhereStep::new("age", p::gte(18i64));
            let cloned = step.clone();
            assert_eq!(cloned.key, "age");
        }

        #[test]
        fn filters_float_property_with_gte() {
            let graph = create_graph_with_ages();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = HasWhereStep::new("version", p::gte(Value::Float(1.0)));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(4)), // version 1.5, should pass
                Traverser::from_vertex(VertexId(0)), // no version, should fail
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(4)));
        }
    }

    mod is_step_tests {
        use super::*;
        use crate::traversal::predicate::p;
        use crate::traversal::step::Step;

        // Helper to create traversers with Value directly
        fn create_value_traverser(value: Value) -> Traverser {
            Traverser::new(value)
        }

        // Helper to create a Graph for tests
        fn create_empty_graph() -> Graph {
            Graph::in_memory()
        }

        #[test]
        fn new_creates_is_step() {
            let step = IsStep::new(p::gt(25i64));
            assert_eq!(step.name(), "is");
        }

        #[test]
        fn eq_creates_equality_step() {
            let step = IsStep::eq(29i64);
            assert_eq!(step.name(), "is");
        }

        #[test]
        fn filters_integer_values_with_eq() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::eq(29i64);

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(29)),
                create_value_traverser(Value::Int(30)),
                create_value_traverser(Value::Int(25)),
                create_value_traverser(Value::Int(29)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(29));
            assert_eq!(output[1].value, Value::Int(29));
        }

        #[test]
        fn filters_integer_values_with_gt() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::gt(25i64));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(25)),
                create_value_traverser(Value::Int(26)),
                create_value_traverser(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(26));
            assert_eq!(output[1].value, Value::Int(30));
        }

        #[test]
        fn filters_integer_values_with_gte() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::gte(25i64));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(24)),
                create_value_traverser(Value::Int(25)),
                create_value_traverser(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(25));
            assert_eq!(output[1].value, Value::Int(30));
        }

        #[test]
        fn filters_integer_values_with_lt() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::lt(25i64));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(25)),
                create_value_traverser(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(20));
        }

        #[test]
        fn filters_integer_values_with_lte() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::lte(25i64));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(25)),
                create_value_traverser(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(20));
            assert_eq!(output[1].value, Value::Int(25));
        }

        #[test]
        fn filters_integer_values_with_neq() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::neq(25i64));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(25)),
                create_value_traverser(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(20));
            assert_eq!(output[1].value, Value::Int(30));
        }

        #[test]
        fn filters_integer_values_with_between() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::between(20i64, 40i64));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(10)),
                create_value_traverser(Value::Int(20)), // inclusive start
                create_value_traverser(Value::Int(30)),
                create_value_traverser(Value::Int(40)), // exclusive end
                create_value_traverser(Value::Int(50)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(20));
            assert_eq!(output[1].value, Value::Int(30));
        }

        #[test]
        fn filters_integer_values_with_inside() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::inside(20i64, 40i64));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(10)),
                create_value_traverser(Value::Int(20)), // exclusive start
                create_value_traverser(Value::Int(30)),
                create_value_traverser(Value::Int(40)), // exclusive end
                create_value_traverser(Value::Int(50)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(30));
        }

        #[test]
        fn filters_integer_values_with_outside() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::outside(20i64, 40i64));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(10)),
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(30)),
                create_value_traverser(Value::Int(40)),
                create_value_traverser(Value::Int(50)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(10));
            assert_eq!(output[1].value, Value::Int(50));
        }

        #[test]
        fn filters_integer_values_with_within() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::within(vec![
                Value::Int(20),
                Value::Int(30),
                Value::Int(40),
            ]));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(10)),
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(25)),
                create_value_traverser(Value::Int(30)),
                create_value_traverser(Value::Int(50)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(20));
            assert_eq!(output[1].value, Value::Int(30));
        }

        #[test]
        fn filters_integer_values_with_without() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::without(vec![Value::Int(20), Value::Int(30)]));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(10)),
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(25)),
                create_value_traverser(Value::Int(30)),
                create_value_traverser(Value::Int(50)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(10));
            assert_eq!(output[1].value, Value::Int(25));
            assert_eq!(output[2].value, Value::Int(50));
        }

        #[test]
        fn filters_float_values_with_gt() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::gt(Value::Float(2.5)));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Float(1.0)),
                create_value_traverser(Value::Float(2.5)),
                create_value_traverser(Value::Float(3.0)),
                create_value_traverser(Value::Float(4.5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Float(3.0));
            assert_eq!(output[1].value, Value::Float(4.5));
        }

        #[test]
        fn filters_string_values_with_eq() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::eq("alice");

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::String("alice".to_string())),
                create_value_traverser(Value::String("bob".to_string())),
                create_value_traverser(Value::String("alice".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("alice".to_string()));
            assert_eq!(output[1].value, Value::String("alice".to_string()));
        }

        #[test]
        fn filters_string_values_with_containing() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::containing("lic"));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::String("alice".to_string())),
                create_value_traverser(Value::String("bob".to_string())),
                create_value_traverser(Value::String("malice".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("alice".to_string()));
            assert_eq!(output[1].value, Value::String("malice".to_string()));
        }

        #[test]
        fn filters_string_values_with_starting_with() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::starting_with("al"));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::String("alice".to_string())),
                create_value_traverser(Value::String("bob".to_string())),
                create_value_traverser(Value::String("alex".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("alice".to_string()));
            assert_eq!(output[1].value, Value::String("alex".to_string()));
        }

        #[test]
        fn filters_string_values_with_ending_with() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::ending_with("ce"));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::String("alice".to_string())),
                create_value_traverser(Value::String("bob".to_string())),
                create_value_traverser(Value::String("grace".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("alice".to_string()));
            assert_eq!(output[1].value, Value::String("grace".to_string()));
        }

        #[test]
        fn filters_boolean_values_with_eq() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::eq(true);

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Bool(true)),
                create_value_traverser(Value::Bool(false)),
                create_value_traverser(Value::Bool(true)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Bool(true));
            assert_eq!(output[1].value, Value::Bool(true));
        }

        #[test]
        fn filters_with_and_predicate() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::and(p::gte(20i64), p::lt(30i64)));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(10)),
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(25)),
                create_value_traverser(Value::Int(30)),
                create_value_traverser(Value::Int(40)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(20));
            assert_eq!(output[1].value, Value::Int(25));
        }

        #[test]
        fn filters_with_or_predicate() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::or(p::lt(15i64), p::gt(35i64)));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(10)),
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(30)),
                create_value_traverser(Value::Int(40)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(10));
            assert_eq!(output[1].value, Value::Int(40));
        }

        #[test]
        fn filters_with_not_predicate() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::new(p::not(p::eq(25i64)));

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(25)),
                create_value_traverser(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(20));
            assert_eq!(output[1].value, Value::Int(30));
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::eq(29i64);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn no_matches_returns_empty_output() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::eq(100i64);

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(20)),
                create_value_traverser(Value::Int(30)),
                create_value_traverser(Value::Int(40)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::eq(29i64);

            let mut traverser = Traverser::new(Value::Int(29));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn debug_format() {
            let step = IsStep::eq(29i64);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("IsStep"));
            assert!(debug_str.contains("<predicate>"));
        }

        #[test]
        fn clone_works() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IsStep::eq(29i64);
            let cloned = step.clone();

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::Int(29)),
                create_value_traverser(Value::Int(30)),
            ];

            // Both original and cloned should work the same
            let output1: Vec<Traverser> = step
                .apply(&ctx, Box::new(input.clone().into_iter()))
                .collect();
            let output2: Vec<Traverser> = cloned.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output1.len(), 1);
            assert_eq!(output2.len(), 1);
            assert_eq!(output1[0].value, output2[0].value);
        }

        #[test]
        fn type_mismatch_does_not_match() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Looking for integer 29, but input has string "29"
            let step = IsStep::eq(29i64);

            let input: Vec<Traverser> = vec![
                create_value_traverser(Value::String("29".to_string())),
                create_value_traverser(Value::Float(29.0)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Neither should match due to type mismatch
            assert!(output.is_empty());
        }
    }

    mod simple_path_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        fn create_empty_graph() -> Graph {
            Graph::in_memory()
        }

        /// Helper to create a traverser with a path containing the given values.
        /// The final value becomes the traverser's current value.
        fn create_traverser_with_path(values: Vec<Value>) -> Traverser {
            let final_value = values.last().cloned().unwrap_or(Value::Null);
            let mut traverser = Traverser::new(final_value);
            // Build path by setting value and extending for each element
            for value in values {
                traverser.value = value;
                traverser.extend_path_unlabeled();
            }
            traverser
        }

        #[test]
        fn new_creates_step() {
            let step = SimplePathStep::new();
            assert_eq!(step.name(), "simplePath");
        }

        #[test]
        fn default_creates_step() {
            let step = SimplePathStep;
            assert_eq!(step.name(), "simplePath");
        }

        #[test]
        fn clone_works() {
            let step = SimplePathStep::new();
            let cloned = step;
            assert_eq!(step.name(), cloned.name());
        }

        #[test]
        fn clone_box_works() {
            let step = SimplePathStep::new();
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "simplePath");
        }

        #[test]
        fn simple_linear_path_passes() {
            // Path: A -> B -> C -> D (all unique)
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SimplePathStep::new();

            let traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(0)),
                Value::Vertex(VertexId(1)),
                Value::Vertex(VertexId(2)),
                Value::Vertex(VertexId(3)),
            ]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
        }

        #[test]
        fn cyclic_path_filtered_out() {
            // Path: A -> B -> C -> A (cycle back to A)
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SimplePathStep::new();

            let traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(0)),
                Value::Vertex(VertexId(1)),
                Value::Vertex(VertexId(2)),
                Value::Vertex(VertexId(0)), // Cycle back to 0
            ]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn single_element_path_is_simple() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SimplePathStep::new();

            let traverser = create_traverser_with_path(vec![Value::Vertex(VertexId(0))]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
        }

        #[test]
        fn empty_path_is_simple() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SimplePathStep::new();

            // Create a traverser without extending the path
            let traverser = Traverser::new(Value::Vertex(VertexId(0)));

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
        }

        #[test]
        fn consecutive_duplicates_filtered_out() {
            // Path: A -> A (immediate duplicate)
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SimplePathStep::new();

            let traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(0)),
                Value::Vertex(VertexId(0)), // Immediate repeat
            ]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn mixed_simple_and_cyclic_paths() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SimplePathStep::new();

            // Simple path: A -> B -> C
            let simple_traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(0)),
                Value::Vertex(VertexId(1)),
                Value::Vertex(VertexId(2)),
            ]);

            // Cyclic path: D -> E -> D
            let cyclic_traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(3)),
                Value::Vertex(VertexId(4)),
                Value::Vertex(VertexId(3)),
            ]);

            let input = vec![simple_traverser, cyclic_traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only the simple path should pass
            assert_eq!(output.len(), 1);
            // Verify it's the simple path (ends at vertex 2)
            assert_eq!(output[0].value, Value::Vertex(VertexId(2)));
        }

        #[test]
        fn edges_in_path_checked_for_duplicates() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SimplePathStep::new();

            // Path with repeated edge: V0 -> E0 -> V1 -> E0 -> V2
            let traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(0)),
                Value::Edge(EdgeId(0)),
                Value::Vertex(VertexId(1)),
                Value::Edge(EdgeId(0)), // Repeat edge
                Value::Vertex(VertexId(2)),
            ]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Should be filtered because edge is repeated
            assert!(output.is_empty());
        }

        #[test]
        fn properties_in_path_checked_for_duplicates() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SimplePathStep::new();

            // Path with repeated property value
            let traverser = create_traverser_with_path(vec![
                Value::String("Alice".to_string()),
                Value::String("Bob".to_string()),
                Value::String("Alice".to_string()), // Duplicate
            ]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SimplePathStep::new();

            let mut traverser = Traverser::new(Value::Vertex(VertexId(0)));
            traverser.extend_path_unlabeled();
            traverser.value = Value::Vertex(VertexId(1));
            traverser.extend_path_labeled("end");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("end"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod cyclic_path_step_tests {
        use super::*;
        use crate::traversal::step::DynStep;

        fn create_empty_graph() -> Graph {
            Graph::in_memory()
        }

        /// Helper to create a traverser with a path containing the given values.
        fn create_traverser_with_path(values: Vec<Value>) -> Traverser {
            let final_value = values.last().cloned().unwrap_or(Value::Null);
            let mut traverser = Traverser::new(final_value);
            for value in values {
                traverser.value = value;
                traverser.extend_path_unlabeled();
            }
            traverser
        }

        #[test]
        fn new_creates_step() {
            let step = CyclicPathStep::new();
            assert_eq!(step.name(), "cyclicPath");
        }

        #[test]
        fn default_creates_step() {
            let step = CyclicPathStep;
            assert_eq!(step.name(), "cyclicPath");
        }

        #[test]
        fn clone_works() {
            let step = CyclicPathStep::new();
            let cloned = step;
            assert_eq!(step.name(), cloned.name());
        }

        #[test]
        fn clone_box_works() {
            let step = CyclicPathStep::new();
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "cyclicPath");
        }

        #[test]
        fn cyclic_path_passes() {
            // Path: A -> B -> C -> A (cycle back to A)
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CyclicPathStep::new();

            let traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(0)),
                Value::Vertex(VertexId(1)),
                Value::Vertex(VertexId(2)),
                Value::Vertex(VertexId(0)), // Cycle back to 0
            ]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
        }

        #[test]
        fn simple_path_filtered_out() {
            // Path: A -> B -> C -> D (all unique)
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CyclicPathStep::new();

            let traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(0)),
                Value::Vertex(VertexId(1)),
                Value::Vertex(VertexId(2)),
                Value::Vertex(VertexId(3)),
            ]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn single_element_path_is_not_cyclic() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CyclicPathStep::new();

            let traverser = create_traverser_with_path(vec![Value::Vertex(VertexId(0))]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn empty_path_is_not_cyclic() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CyclicPathStep::new();

            let traverser = Traverser::new(Value::Vertex(VertexId(0)));

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn consecutive_duplicates_pass() {
            // Path: A -> A (immediate duplicate)
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CyclicPathStep::new();

            let traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(0)),
                Value::Vertex(VertexId(0)), // Immediate repeat
            ]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
        }

        #[test]
        fn mixed_simple_and_cyclic_paths() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CyclicPathStep::new();

            // Simple path: A -> B -> C
            let simple_traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(0)),
                Value::Vertex(VertexId(1)),
                Value::Vertex(VertexId(2)),
            ]);

            // Cyclic path: D -> E -> D
            let cyclic_traverser = create_traverser_with_path(vec![
                Value::Vertex(VertexId(3)),
                Value::Vertex(VertexId(4)),
                Value::Vertex(VertexId(3)),
            ]);

            let input = vec![simple_traverser, cyclic_traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only the cyclic path should pass
            assert_eq!(output.len(), 1);
            // Verify it's the cyclic path (ends at vertex 3)
            assert_eq!(output[0].value, Value::Vertex(VertexId(3)));
        }

        #[test]
        fn is_inverse_of_simple_path() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let simple_step = SimplePathStep::new();
            let cyclic_step = CyclicPathStep::new();

            // Create both types of paths
            let paths = vec![
                // Simple
                create_traverser_with_path(vec![
                    Value::Vertex(VertexId(0)),
                    Value::Vertex(VertexId(1)),
                ]),
                // Cyclic
                create_traverser_with_path(vec![
                    Value::Vertex(VertexId(2)),
                    Value::Vertex(VertexId(2)),
                ]),
            ];

            let simple_output: Vec<Traverser> = simple_step
                .apply(&ctx, Box::new(paths.clone().into_iter()))
                .collect();
            let cyclic_output: Vec<Traverser> = cyclic_step
                .apply(&ctx, Box::new(paths.into_iter()))
                .collect();

            // Together they should give all paths
            assert_eq!(simple_output.len() + cyclic_output.len(), 2);
            // One is simple (VertexId(1))
            assert_eq!(simple_output.len(), 1);
            assert_eq!(simple_output[0].value, Value::Vertex(VertexId(1)));
            // One is cyclic (VertexId(2))
            assert_eq!(cyclic_output.len(), 1);
            assert_eq!(cyclic_output[0].value, Value::Vertex(VertexId(2)));
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_empty_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CyclicPathStep::new();

            let mut traverser = Traverser::new(Value::Vertex(VertexId(0)));
            traverser.extend_path_unlabeled();
            // Make it cyclic by adding the same vertex again
            traverser.extend_path_labeled("end");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("end"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod where_p_step_tests {
        use super::*;
        use crate::traversal::predicate::p;
        use crate::traversal::step::DynStep;

        #[test]
        fn new_creates_where_p_step() {
            let step = WherePStep::new(p::gt(25));
            assert_eq!(step.name(), "where");
        }

        #[test]
        fn clone_box_works() {
            let step = WherePStep::new(p::gt(25));
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "where");
        }

        #[test]
        fn filters_with_gt_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::gt(25));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(30)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(25)),
                Traverser::new(Value::Int(40)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(30));
            assert_eq!(output[1].value, Value::Int(40));
        }

        #[test]
        fn filters_with_lt_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::lt(25));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(30)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(15)),
                Traverser::new(Value::Int(25)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(20));
            assert_eq!(output[1].value, Value::Int(15));
        }

        #[test]
        fn filters_with_eq_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::eq(42));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::Int(41)),
                Traverser::new(Value::Int(43)),
                Traverser::new(Value::Int(42)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(42));
            assert_eq!(output[1].value, Value::Int(42));
        }

        #[test]
        fn filters_with_neq_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::neq(42));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::Int(41)),
                Traverser::new(Value::Int(43)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(41));
            assert_eq!(output[1].value, Value::Int(43));
        }

        #[test]
        fn filters_with_gte_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::gte(25));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(30)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(25)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(30));
            assert_eq!(output[1].value, Value::Int(25));
        }

        #[test]
        fn filters_with_lte_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::lte(25));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(30)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(25)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(20));
            assert_eq!(output[1].value, Value::Int(25));
        }

        #[test]
        fn filters_with_within_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::within([1, 2, 3]));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(5)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
            assert_eq!(output[2].value, Value::Int(3));
        }

        #[test]
        fn filters_with_without_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::without([1, 2, 3]));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(5)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(4));
            assert_eq!(output[1].value, Value::Int(5));
        }

        #[test]
        fn filters_with_between_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::between(10, 20));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(5)),
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Int(15)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(25)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // between is [start, end) - inclusive start, exclusive end
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(10));
            assert_eq!(output[1].value, Value::Int(15));
        }

        #[test]
        fn filters_with_and_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Filter values > 10 AND < 30
            let step = WherePStep::new(p::and(p::gt(10), p::lt(30)));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(5)),
                Traverser::new(Value::Int(15)),
                Traverser::new(Value::Int(25)),
                Traverser::new(Value::Int(35)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(15));
            assert_eq!(output[1].value, Value::Int(25));
        }

        #[test]
        fn filters_with_or_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Filter values < 10 OR > 30
            let step = WherePStep::new(p::or(p::lt(10), p::gt(30)));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(5)),
                Traverser::new(Value::Int(15)),
                Traverser::new(Value::Int(25)),
                Traverser::new(Value::Int(35)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(5));
            assert_eq!(output[1].value, Value::Int(35));
        }

        #[test]
        fn empty_input_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::gt(25));
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::gt(25));

            let mut traverser = Traverser::new(Value::Int(30));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn works_with_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::within(["Alice", "Bob"]));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::String("Alice".to_string())),
                Traverser::new(Value::String("Carol".to_string())),
                Traverser::new(Value::String("Bob".to_string())),
                Traverser::new(Value::String("Dave".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("Alice".to_string()));
            assert_eq!(output[1].value, Value::String("Bob".to_string()));
        }

        #[test]
        fn works_with_float_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::gt(2.5));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Float(1.5)),
                Traverser::new(Value::Float(2.5)),
                Traverser::new(Value::Float(3.5)),
                Traverser::new(Value::Float(4.5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Float(3.5));
            assert_eq!(output[1].value, Value::Float(4.5));
        }

        #[test]
        fn debug_format() {
            let step = WherePStep::new(p::gt(25));
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("WherePStep"));
            assert!(debug_str.contains("predicate"));
        }

        #[test]
        fn filters_all_when_none_match() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::gt(100));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn passes_all_when_all_match() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = WherePStep::new(p::gt(0));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
        }
    }

    mod streaming_tests {
        use super::*;
        use crate::storage::GraphStorage;
        use crate::traversal::context::StreamingContext;
        use crate::traversal::step::Step;
        use crate::traversal::SnapshotLike;

        fn create_test_graph() -> Graph {
            let graph = Graph::new();

            graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });
            graph.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props.insert("age".to_string(), Value::Int(25));
                props
            });
            graph.add_vertex("software", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Graph DB".to_string()));
                props
            });

            graph
        }

        // ---------------------------------------------------------------------
        // LimitStep streaming tests
        // ---------------------------------------------------------------------

        #[test]
        fn limit_step_streaming_limits_traversers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = LimitStep::new(2);

            // Process 5 traversers, only first 2 should pass
            let mut outputs = vec![];
            for i in 0..5 {
                let result: Vec<_> =
                    Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(i)))
                        .collect();
                outputs.extend(result);
            }

            assert_eq!(outputs.len(), 2);
            assert_eq!(outputs[0].value, Value::Int(0));
            assert_eq!(outputs[1].value, Value::Int(1));
        }

        #[test]
        fn limit_step_streaming_shares_counter_across_clones() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = LimitStep::new(3);
            let step_clone = step.clone();

            // Use original for first two
            let _: Vec<_> =
                Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(1))).collect();
            let _: Vec<_> =
                Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(2))).collect();

            // Use clone for next two - only one should pass (limit=3)
            let result1: Vec<_> =
                Step::apply_streaming(&step_clone, ctx.clone(), Traverser::new(Value::Int(3)))
                    .collect();
            let result2: Vec<_> =
                Step::apply_streaming(&step_clone, ctx.clone(), Traverser::new(Value::Int(4)))
                    .collect();

            assert_eq!(result1.len(), 1); // 3rd traverser passes
            assert_eq!(result2.len(), 0); // 4th is blocked
        }

        #[test]
        fn limit_step_streaming_matches_eager() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();

            // Eager execution
            let eager_ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());
            let step = LimitStep::new(2);
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];
            let eager_output: Vec<_> =
                Step::apply(&step, &eager_ctx, Box::new(input.into_iter())).collect();

            // Streaming execution
            let streaming_ctx =
                StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());
            let step = LimitStep::new(2);
            let mut streaming_output = vec![];
            for i in 1..=3 {
                let result: Vec<_> = Step::apply_streaming(
                    &step,
                    streaming_ctx.clone(),
                    Traverser::new(Value::Int(i)),
                )
                .collect();
                streaming_output.extend(result);
            }

            assert_eq!(eager_output.len(), streaming_output.len());
            for (e, s) in eager_output.iter().zip(streaming_output.iter()) {
                assert_eq!(e.value, s.value);
            }
        }

        // ---------------------------------------------------------------------
        // SkipStep streaming tests
        // ---------------------------------------------------------------------

        #[test]
        fn skip_step_streaming_skips_traversers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = SkipStep::new(2);

            // Process 5 traversers, first 2 should be skipped
            let mut outputs = vec![];
            for i in 0..5 {
                let result: Vec<_> =
                    Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(i)))
                        .collect();
                outputs.extend(result);
            }

            assert_eq!(outputs.len(), 3);
            assert_eq!(outputs[0].value, Value::Int(2));
            assert_eq!(outputs[1].value, Value::Int(3));
            assert_eq!(outputs[2].value, Value::Int(4));
        }

        #[test]
        fn skip_step_streaming_shares_counter_across_clones() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = SkipStep::new(2);
            let step_clone = step.clone();

            // Use original for first traverser (skipped)
            let result1: Vec<_> =
                Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(1))).collect();
            assert_eq!(result1.len(), 0);

            // Use clone for second (still skipped, counter is shared)
            let result2: Vec<_> =
                Step::apply_streaming(&step_clone, ctx.clone(), Traverser::new(Value::Int(2)))
                    .collect();
            assert_eq!(result2.len(), 0);

            // Third should pass
            let result3: Vec<_> =
                Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(3))).collect();
            assert_eq!(result3.len(), 1);
            assert_eq!(result3[0].value, Value::Int(3));
        }

        // ---------------------------------------------------------------------
        // RangeStep streaming tests
        // ---------------------------------------------------------------------

        #[test]
        fn range_step_streaming_applies_range() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = RangeStep::new(1, 4); // Skip first, take next 3

            let mut outputs = vec![];
            for i in 0..6 {
                let result: Vec<_> =
                    Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(i)))
                        .collect();
                outputs.extend(result);
            }

            assert_eq!(outputs.len(), 3);
            assert_eq!(outputs[0].value, Value::Int(1));
            assert_eq!(outputs[1].value, Value::Int(2));
            assert_eq!(outputs[2].value, Value::Int(3));
        }

        #[test]
        fn range_step_streaming_shares_counter_across_clones() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = RangeStep::new(1, 3); // Skip 1, take 2
            let step_clone = step.clone();

            // First is skipped
            let r1: Vec<_> =
                Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(0))).collect();
            assert_eq!(r1.len(), 0);

            // Second passes (via clone)
            let r2: Vec<_> =
                Step::apply_streaming(&step_clone, ctx.clone(), Traverser::new(Value::Int(1)))
                    .collect();
            assert_eq!(r2.len(), 1);

            // Third passes
            let r3: Vec<_> =
                Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(2))).collect();
            assert_eq!(r3.len(), 1);

            // Fourth blocked (past end)
            let r4: Vec<_> =
                Step::apply_streaming(&step_clone, ctx.clone(), Traverser::new(Value::Int(3)))
                    .collect();
            assert_eq!(r4.len(), 0);
        }

        // ---------------------------------------------------------------------
        // DedupStep streaming tests
        // ---------------------------------------------------------------------

        #[test]
        fn dedup_step_streaming_removes_duplicates() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = DedupStep::new();

            // Process traversers with duplicates
            let inputs = vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(1), // duplicate
                Value::Int(3),
                Value::Int(2), // duplicate
            ];

            let mut outputs = vec![];
            for v in inputs {
                let result: Vec<_> =
                    Step::apply_streaming(&step, ctx.clone(), Traverser::new(v)).collect();
                outputs.extend(result);
            }

            assert_eq!(outputs.len(), 3);
            assert_eq!(outputs[0].value, Value::Int(1));
            assert_eq!(outputs[1].value, Value::Int(2));
            assert_eq!(outputs[2].value, Value::Int(3));
        }

        #[test]
        fn dedup_step_streaming_shares_hashset_across_clones() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = DedupStep::new();
            let step_clone = step.clone();

            // Insert via original
            let r1: Vec<_> =
                Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(1))).collect();
            assert_eq!(r1.len(), 1);

            // Try duplicate via clone - should be blocked
            let r2: Vec<_> =
                Step::apply_streaming(&step_clone, ctx.clone(), Traverser::new(Value::Int(1)))
                    .collect();
            assert_eq!(r2.len(), 0);

            // New value via clone should pass
            let r3: Vec<_> =
                Step::apply_streaming(&step_clone, ctx.clone(), Traverser::new(Value::Int(2)))
                    .collect();
            assert_eq!(r3.len(), 1);
        }

        #[test]
        fn dedup_step_streaming_matches_eager() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();

            let inputs = vec![Value::Int(1), Value::Int(2), Value::Int(1), Value::Int(3)];

            // Eager execution
            let eager_ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());
            let step = DedupStep::new();
            let input: Vec<_> = inputs.iter().map(|v| Traverser::new(v.clone())).collect();
            let eager_output: Vec<_> =
                Step::apply(&step, &eager_ctx, Box::new(input.into_iter())).collect();

            // Streaming execution
            let streaming_ctx =
                StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());
            let step = DedupStep::new();
            let mut streaming_output = vec![];
            for v in inputs {
                let result: Vec<_> =
                    Step::apply_streaming(&step, streaming_ctx.clone(), Traverser::new(v))
                        .collect();
                streaming_output.extend(result);
            }

            assert_eq!(eager_output.len(), streaming_output.len());
            for (e, s) in eager_output.iter().zip(streaming_output.iter()) {
                assert_eq!(e.value, s.value);
            }
        }

        // ---------------------------------------------------------------------
        // DedupByKeyStep streaming tests
        // ---------------------------------------------------------------------

        #[test]
        fn dedup_by_key_step_streaming_removes_duplicates() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = DedupByKeyStep::new("name");

            // Get vertices - there are 3 with different names
            let vertices: Vec<_> = snapshot.all_vertices().collect();

            let mut outputs = vec![];
            for v in &vertices {
                let result: Vec<_> =
                    Step::apply_streaming(&step, ctx.clone(), Traverser::from_vertex(v.id))
                        .collect();
                outputs.extend(result);
            }

            // All should pass since they have different names
            assert_eq!(outputs.len(), vertices.len());
        }

        #[test]
        fn dedup_by_key_step_streaming_shares_hashset() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = DedupByKeyStep::new("type");

            // Create map values with same "type" key
            let map1 = {
                let mut m = crate::value::ValueMap::new();
                m.insert("type".to_string(), Value::String("A".to_string()));
                m.insert("id".to_string(), Value::Int(1));
                Value::Map(m)
            };
            let map2 = {
                let mut m = crate::value::ValueMap::new();
                m.insert("type".to_string(), Value::String("A".to_string())); // Same type
                m.insert("id".to_string(), Value::Int(2));
                Value::Map(m)
            };

            let step_clone = step.clone();

            let r1: Vec<_> =
                Step::apply_streaming(&step, ctx.clone(), Traverser::new(map1)).collect();
            assert_eq!(r1.len(), 1);

            // Clone sees the same hashset, so duplicate is blocked
            let r2: Vec<_> =
                Step::apply_streaming(&step_clone, ctx.clone(), Traverser::new(map2)).collect();
            assert_eq!(r2.len(), 0);
        }

        // ---------------------------------------------------------------------
        // DedupByLabelStep streaming tests
        // ---------------------------------------------------------------------

        #[test]
        fn dedup_by_label_step_streaming_removes_duplicates() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = DedupByLabelStep::new();

            // Get vertices - there are 2 "person" and 1 "software"
            let vertices: Vec<_> = snapshot.all_vertices().collect();

            let mut outputs = vec![];
            for v in &vertices {
                let result: Vec<_> =
                    Step::apply_streaming(&step, ctx.clone(), Traverser::from_vertex(v.id))
                        .collect();
                outputs.extend(result);
            }

            // Only 2 unique labels should pass
            assert_eq!(outputs.len(), 2);
        }

        #[test]
        fn dedup_by_label_step_streaming_shares_hashset() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let step = DedupByLabelStep::new();
            let step_clone = step.clone();

            // Get person vertices
            let person_vertices: Vec<_> = snapshot
                .all_vertices()
                .filter(|v| v.label == "person")
                .collect();
            assert!(
                person_vertices.len() >= 2,
                "Need at least 2 person vertices"
            );

            // First person passes via original step
            let r1: Vec<_> = Step::apply_streaming(
                &step,
                ctx.clone(),
                Traverser::from_vertex(person_vertices[0].id),
            )
            .collect();
            assert_eq!(r1.len(), 1);

            // Second person blocked via clone (same label already seen)
            let r2: Vec<_> = Step::apply_streaming(
                &step_clone,
                ctx.clone(),
                Traverser::from_vertex(person_vertices[1].id),
            )
            .collect();
            assert_eq!(r2.len(), 0);
        }

        // ---------------------------------------------------------------------
        // DedupByTraversalStep streaming tests
        // ---------------------------------------------------------------------

        #[test]
        fn dedup_by_traversal_step_streaming_removes_duplicates() {
            use crate::traversal::IdentityStep;
            use crate::traversal::Traversal;

            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            // Sub-traversal that returns the value itself (identity)
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep);
            let step = DedupByTraversalStep::new(sub);

            let inputs = vec![
                Value::Int(1),
                Value::Int(2),
                Value::Int(1), // duplicate
                Value::Int(3),
            ];

            let mut outputs = vec![];
            for v in inputs {
                let result: Vec<_> =
                    Step::apply_streaming(&step, ctx.clone(), Traverser::new(v)).collect();
                outputs.extend(result);
            }

            assert_eq!(outputs.len(), 3);
        }

        #[test]
        fn dedup_by_traversal_step_streaming_shares_hashset() {
            use crate::traversal::IdentityStep;
            use crate::traversal::Traversal;

            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());

            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep);
            let step = DedupByTraversalStep::new(sub);
            let step_clone = step.clone();

            // Insert via original
            let r1: Vec<_> =
                Step::apply_streaming(&step, ctx.clone(), Traverser::new(Value::Int(1))).collect();
            assert_eq!(r1.len(), 1);

            // Duplicate via clone should be blocked
            let r2: Vec<_> =
                Step::apply_streaming(&step_clone, ctx.clone(), Traverser::new(Value::Int(1)))
                    .collect();
            assert_eq!(r2.len(), 0);

            // New value via clone should pass
            let r3: Vec<_> =
                Step::apply_streaming(&step_clone, ctx.clone(), Traverser::new(Value::Int(2)))
                    .collect();
            assert_eq!(r3.len(), 1);
        }

        #[test]
        fn dedup_by_traversal_step_streaming_matches_eager() {
            use crate::traversal::IdentityStep;
            use crate::traversal::Traversal;

            let graph = create_test_graph();
            let snapshot = graph.snapshot();

            let inputs = vec![Value::Int(1), Value::Int(2), Value::Int(1), Value::Int(3)];

            // Eager execution
            let eager_ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep);
            let step = DedupByTraversalStep::new(sub);
            let input: Vec<_> = inputs.iter().map(|v| Traverser::new(v.clone())).collect();
            let eager_output: Vec<_> =
                Step::apply(&step, &eager_ctx, Box::new(input.into_iter())).collect();

            // Streaming execution
            let streaming_ctx =
                StreamingContext::new(snapshot.arc_streamable(), snapshot.arc_interner());
            let sub = Traversal::<Value, Value>::new().add_step(IdentityStep);
            let step = DedupByTraversalStep::new(sub);
            let mut streaming_output = vec![];
            for v in inputs {
                let result: Vec<_> =
                    Step::apply_streaming(&step, streaming_ctx.clone(), Traverser::new(v))
                        .collect();
                streaming_output.extend(result);
            }

            assert_eq!(eager_output.len(), streaming_output.len());
            for (e, s) in eager_output.iter().zip(streaming_output.iter()) {
                assert_eq!(e.value, s.value);
            }
        }
    }
}
