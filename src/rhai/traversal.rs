//! Traversal wrapper types for Rhai.
//!
//! This module provides wrapper types that expose the traversal API to Rhai scripts.
//! The key challenge is that Rhai requires all types to be `Clone`, but traversals
//! are consumed when terminal steps are called. We solve this using:
//!
//! - `RhaiTraversal`: A cloneable wrapper around traversal state
//! - Lazy execution: Steps are collected and executed only at terminal steps

use rhai::{Dynamic, Engine, ImmutableString};
use std::collections::HashMap;
use std::sync::Arc;

use crate::storage::cow::CowBoundTraversal;
use crate::storage::Graph;
use crate::traversal::step::IdentityStep;
use crate::traversal::{Traversal, TraversalSource, __};
use crate::value::{EdgeId, Value, VertexId};

use super::error::RhaiError;
use super::predicates::RhaiPredicate;
use super::types::{dynamic_to_value, value_to_dynamic};

/// A wrapper around a graph that can be passed to Rhai scripts.
///
/// This wrapper owns an `Arc<Graph>` and provides methods to create traversals.
/// It is designed to work with Rhai's requirement that all types be `Clone`.
#[derive(Clone)]
pub struct RhaiGraph {
    inner: Arc<Graph>,
}

impl RhaiGraph {
    /// Create a new RhaiGraph from a Graph.
    pub fn new(graph: Graph) -> Self {
        RhaiGraph {
            inner: Arc::new(graph),
        }
    }

    /// Create a new RhaiGraph from an Arc<Graph>.
    pub fn from_arc(graph: Arc<Graph>) -> Self {
        RhaiGraph { inner: graph }
    }

    /// Get a reference to the underlying graph.
    pub fn graph(&self) -> &Graph {
        &self.inner
    }

    /// Create a Gremlin-style traversal source for this graph.
    ///
    /// This is the main entry point for creating traversals in Rhai scripts.
    pub fn gremlin(&self) -> RhaiTraversalSource {
        RhaiTraversalSource {
            graph: self.inner.clone(),
        }
    }
}

/// A wrapper around `GraphTraversalSource` for Rhai.
///
/// This provides the source steps (`v()`, `e()`, etc.) that start traversals.
#[derive(Clone)]
pub struct RhaiTraversalSource {
    graph: Arc<Graph>,
}

impl RhaiTraversalSource {
    /// Start traversal from all vertices.
    pub fn v(&self) -> RhaiTraversal {
        RhaiTraversal {
            graph: self.graph.clone(),
            source: TraversalSource::AllVertices,
            steps: Vec::new(),
            track_paths: false,
        }
    }

    /// Start traversal from specific vertex IDs.
    pub fn v_ids(&self, ids: Vec<VertexId>) -> RhaiTraversal {
        RhaiTraversal {
            graph: self.graph.clone(),
            source: TraversalSource::Vertices(ids),
            steps: Vec::new(),
            track_paths: false,
        }
    }

    /// Start traversal from a single vertex ID.
    pub fn v_id(&self, id: VertexId) -> RhaiTraversal {
        RhaiTraversal {
            graph: self.graph.clone(),
            source: TraversalSource::Vertices(vec![id]),
            steps: Vec::new(),
            track_paths: false,
        }
    }

    /// Start traversal from all edges.
    pub fn e(&self) -> RhaiTraversal {
        RhaiTraversal {
            graph: self.graph.clone(),
            source: TraversalSource::AllEdges,
            steps: Vec::new(),
            track_paths: false,
        }
    }

    /// Start traversal from specific edge IDs.
    pub fn e_ids(&self, ids: Vec<EdgeId>) -> RhaiTraversal {
        RhaiTraversal {
            graph: self.graph.clone(),
            source: TraversalSource::Edges(ids),
            steps: Vec::new(),
            track_paths: false,
        }
    }

    /// Inject arbitrary values into the traversal.
    pub fn inject(&self, values: Vec<Value>) -> RhaiTraversal {
        RhaiTraversal {
            graph: self.graph.clone(),
            source: TraversalSource::Inject(values),
            steps: Vec::new(),
            track_paths: false,
        }
    }

    /// Create a new vertex with the specified label.
    ///
    /// This is a source-level mutation step that creates a new vertex.
    /// Properties can be attached via chained `.property()` calls.
    ///
    /// Note: The actual vertex creation happens when the traversal is executed
    /// via MutationExecutor.
    pub fn add_v(&self, label: String) -> RhaiTraversal {
        RhaiTraversal {
            graph: self.graph.clone(),
            source: TraversalSource::Inject(vec![]), // Empty source - AddV is a spawning step
            steps: vec![RhaiStep::AddV(label)],
            track_paths: false,
        }
    }

    /// Create a new edge with the specified label.
    ///
    /// This is a source-level mutation step that creates a new edge.
    /// Both `from_v` and `to_v` endpoints must be specified.
    pub fn add_e(&self, label: String) -> RhaiTraversal {
        RhaiTraversal {
            graph: self.graph.clone(),
            source: TraversalSource::Inject(vec![]),
            steps: vec![RhaiStep::AddE {
                label,
                from: None,
                to: None,
            }],
            track_paths: false,
        }
    }
}

/// A cloneable step that can be stored and applied later.
///
/// This is necessary because `Box<dyn AnyStep>` is not Clone, but we need
/// to rebuild traversals on demand for Rhai.
#[derive(Clone, Debug)]
#[allow(dead_code)] // Some variants are prepared for future features
enum RhaiStep {
    // Navigation steps
    Out(Vec<String>),
    In(Vec<String>),
    Both(Vec<String>),
    OutE(Vec<String>),
    InE(Vec<String>),
    BothE(Vec<String>),
    OutV,
    InV,
    OtherV,
    BothV,

    // Filter steps
    HasLabel(Vec<String>),
    Has(String),
    HasNot(String),
    HasValue(String, Value),
    HasWhere(String, RhaiPredicate),
    HasId(Value),
    Dedup,
    Limit(usize),
    Skip(usize),
    Range(usize, usize),
    IsEq(Value),
    Is(RhaiPredicate),
    SimplePath,
    CyclicPath,

    // Advanced filter steps (Phase 7)
    Tail,
    TailN(usize),
    Coin(f64),
    Sample(usize),
    DedupByKey(String),
    DedupByLabel,
    DedupBy(RhaiAnonymousTraversal),
    HasIds(Vec<Value>),

    // Transform steps
    Id,
    Label,
    Values(String),
    ValuesMulti(Vec<String>),
    ValueMap,
    ElementMap,
    Path,
    Constant(Value),
    Identity,
    Fold,
    Unfold,
    Count,
    Sum,
    Mean,
    Min,
    Max,

    // Advanced transform steps (Phase 8)
    Properties,
    PropertiesKeys(Vec<String>),
    Key,
    PropValue, // Named PropValue to avoid conflict with Value type
    ValueMapKeys(Vec<String>),
    ValueMapWithTokens,
    Index,
    Local(RhaiAnonymousTraversal),

    // Modulator steps
    As(String),
    Select(Vec<String>),
    SelectOne(String),

    // Anonymous traversal
    Anonymous(RhaiAnonymousTraversal),

    // Order step
    Order(bool), // true = ascending, false = descending

    // Repeat steps
    RepeatTimes(RhaiAnonymousTraversal, usize),
    RepeatUntil(RhaiAnonymousTraversal, RhaiAnonymousTraversal),
    RepeatEmit(RhaiAnonymousTraversal, usize), // repeat with emit, n times
    RepeatEmitUntil(RhaiAnonymousTraversal, RhaiAnonymousTraversal), // repeat with emit until condition

    // Branch steps
    Union(Vec<RhaiAnonymousTraversal>),
    Coalesce(Vec<RhaiAnonymousTraversal>),
    Choose(
        RhaiAnonymousTraversal,
        RhaiAnonymousTraversal,
        RhaiAnonymousTraversal,
    ),
    ChooseOption(
        RhaiAnonymousTraversal,
        Vec<(Value, RhaiAnonymousTraversal)>,
        Option<Box<RhaiAnonymousTraversal>>,
    ),
    Optional(RhaiAnonymousTraversal),

    // Traversal-based filter steps (Phase 2)
    Where(RhaiAnonymousTraversal),
    Not(RhaiAnonymousTraversal),
    And(Vec<RhaiAnonymousTraversal>),
    Or(Vec<RhaiAnonymousTraversal>),

    // Side effect steps (Phase 5)
    Store(String),
    Aggregate(String),
    Cap(String),
    CapMulti(Vec<String>),
    SideEffect(RhaiAnonymousTraversal),

    // Mutation steps (Phase 6)
    AddV(String),
    AddE {
        label: String,
        from: Option<RhaiEdgeEndpoint>,
        to: Option<RhaiEdgeEndpoint>,
    },
    Property(String, Value),
    Drop,

    // Builder pattern steps (Phase 11)
    OrderBy(String, bool),                          // property, ascending
    OrderByTraversal(RhaiAnonymousTraversal, bool), // traversal, ascending
    Project(Vec<String>, Vec<RhaiProjection>),      // keys, projections
    Group(RhaiGroupKey, RhaiGroupValue),            // key selector, value collector
    GroupCount(RhaiGroupKey),                       // key selector
    Math(String, HashMap<String, String>),          // expression, variable bindings
}

// =============================================================================
// Builder Pattern Helper Types (Phase 11)
// =============================================================================

/// Projection type for the project step.
#[derive(Clone, Debug)]
pub enum RhaiProjection {
    /// Extract a property value by key.
    Key(String),
    /// Execute a sub-traversal.
    Traversal(RhaiAnonymousTraversal),
}

/// Group key selector for group/group_count steps.
#[derive(Clone, Debug)]
pub enum RhaiGroupKey {
    /// Group by element label.
    Label,
    /// Group by a property value.
    Property(String),
    /// Group by the result of a sub-traversal.
    Traversal(RhaiAnonymousTraversal),
}

/// Group value collector for group step.
#[derive(Clone, Debug)]
pub enum RhaiGroupValue {
    /// Collect the traverser's current value.
    Identity,
    /// Collect a property value.
    Property(String),
    /// Collect values from a sub-traversal.
    Traversal(RhaiAnonymousTraversal),
}

/// Specifies the source or target vertex for an edge in Rhai bindings.
#[derive(Clone, Debug)]
pub enum RhaiEdgeEndpoint {
    /// A specific vertex ID.
    VertexId(VertexId),
    /// A step label referencing a previously labeled vertex.
    StepLabel(String),
}

/// A cloneable traversal that can be used in Rhai scripts.
///
/// This stores the graph reference, source, and steps, allowing us to
/// rebuild the actual traversal on demand when terminal steps are called.
#[derive(Clone)]
pub struct RhaiTraversal {
    graph: Arc<Graph>,
    source: TraversalSource,
    steps: Vec<RhaiStep>,
    track_paths: bool,
}

impl RhaiTraversal {
    /// Add a step to this traversal.
    fn add_step(mut self, step: RhaiStep) -> Self {
        self.steps.push(step);
        self
    }

    /// Enable path tracking.
    pub fn with_path(mut self) -> Self {
        self.track_paths = true;
        self
    }

    /// Execute the traversal and return results as a list.
    ///
    /// Uses `graph.gremlin()` which supports both read and mutation operations.
    /// Any pending mutations (add_v, add_e, property, drop) are automatically
    /// executed against the graph.
    pub fn to_list(&self) -> Vec<Value> {
        let g = self.graph.gremlin();

        // Build the traversal from source
        let mut bound = match &self.source {
            TraversalSource::AllVertices => g.v(),
            TraversalSource::Vertices(ids) => g.v_ids(ids.clone()),
            TraversalSource::AllEdges => g.e(),
            TraversalSource::Edges(ids) => g.e_ids(ids.clone()),
            TraversalSource::Inject(values) => g.inject(values.clone()),
        };

        if self.track_paths {
            bound = bound.with_path();
        }

        // Apply each step
        for step in &self.steps {
            bound = apply_step_cow(bound, step);
        }

        bound.to_list()
    }

    /// Execute the traversal and return the count.
    pub fn count(&self) -> i64 {
        self.to_list().len() as i64
    }

    /// Execute the traversal and return the first result.
    pub fn first(&self) -> Option<Value> {
        self.to_list().into_iter().next()
    }

    /// Execute the traversal and return exactly one result.
    pub fn one(&self) -> Result<Value, RhaiError> {
        let results = self.to_list();
        match results.len() {
            1 => Ok(results.into_iter().next().unwrap()),
            n => Err(RhaiError::Traversal(crate::error::TraversalError::NotOne(
                n,
            ))),
        }
    }

    /// Check if traversal has any results.
    pub fn has_next(&self) -> bool {
        !self.to_list().is_empty()
    }

    /// Execute the traversal and return unique results as a set.
    /// Returns a Vec with duplicates removed (order preserved).
    pub fn to_set(&self) -> Vec<Value> {
        let results = self.to_list();
        let mut seen = std::collections::HashSet::new();
        results
            .into_iter()
            .filter(|v| {
                let key = format!("{:?}", v);
                seen.insert(key)
            })
            .collect()
    }

    /// Execute the traversal without collecting results (for side effects).
    pub fn iterate(&self) {
        // Simply consume the traversal by calling to_list and dropping the result
        let _ = self.to_list();
    }

    /// Execute the traversal and return the first n results.
    pub fn take(&self, n: i64) -> Vec<Value> {
        self.to_list().into_iter().take(n as usize).collect()
    }

    // =========================================================================
    // Navigation Steps
    // =========================================================================

    /// Traverse outgoing edges to adjacent vertices.
    pub fn out(self) -> Self {
        self.add_step(RhaiStep::Out(vec![]))
    }

    /// Traverse outgoing edges with label filter.
    pub fn out_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::Out(labels))
    }

    /// Traverse incoming edges to adjacent vertices.
    pub fn in_(self) -> Self {
        self.add_step(RhaiStep::In(vec![]))
    }

    /// Traverse incoming edges with label filter.
    pub fn in_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::In(labels))
    }

    /// Traverse both directions to adjacent vertices.
    pub fn both(self) -> Self {
        self.add_step(RhaiStep::Both(vec![]))
    }

    /// Traverse both directions with label filter.
    pub fn both_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::Both(labels))
    }

    /// Traverse to outgoing edges.
    pub fn out_e(self) -> Self {
        self.add_step(RhaiStep::OutE(vec![]))
    }

    /// Traverse to outgoing edges with label filter.
    pub fn out_e_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::OutE(labels))
    }

    /// Traverse to incoming edges.
    pub fn in_e(self) -> Self {
        self.add_step(RhaiStep::InE(vec![]))
    }

    /// Traverse to incoming edges with label filter.
    pub fn in_e_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::InE(labels))
    }

    /// Traverse to both edges.
    pub fn both_e(self) -> Self {
        self.add_step(RhaiStep::BothE(vec![]))
    }

    /// Traverse to both edges with label filter.
    pub fn both_e_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::BothE(labels))
    }

    /// Traverse to outgoing vertex of edge.
    pub fn out_v(self) -> Self {
        self.add_step(RhaiStep::OutV)
    }

    /// Traverse to incoming vertex of edge.
    pub fn in_v(self) -> Self {
        self.add_step(RhaiStep::InV)
    }

    /// Traverse to the other vertex of edge.
    pub fn other_v(self) -> Self {
        self.add_step(RhaiStep::OtherV)
    }

    /// Traverse to both vertices of edge.
    pub fn both_v(self) -> Self {
        self.add_step(RhaiStep::BothV)
    }

    // =========================================================================
    // Filter Steps
    // =========================================================================

    /// Filter by label.
    pub fn has_label(self, label: String) -> Self {
        self.add_step(RhaiStep::HasLabel(vec![label]))
    }

    /// Filter by any of the given labels.
    pub fn has_label_any(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::HasLabel(labels))
    }

    /// Filter by property existence.
    pub fn has(self, key: String) -> Self {
        self.add_step(RhaiStep::Has(key))
    }

    /// Filter by property absence.
    pub fn has_not(self, key: String) -> Self {
        self.add_step(RhaiStep::HasNot(key))
    }

    /// Filter by property value equality.
    pub fn has_value(self, key: String, value: Value) -> Self {
        self.add_step(RhaiStep::HasValue(key, value))
    }

    /// Filter by property predicate.
    pub fn has_where(self, key: String, pred: RhaiPredicate) -> Self {
        self.add_step(RhaiStep::HasWhere(key, pred))
    }

    /// Filter by element ID.
    pub fn has_id(self, id: Value) -> Self {
        self.add_step(RhaiStep::HasId(id))
    }

    /// Remove duplicates.
    pub fn dedup(self) -> Self {
        self.add_step(RhaiStep::Dedup)
    }

    /// Limit results.
    pub fn limit(self, n: i64) -> Self {
        self.add_step(RhaiStep::Limit(n as usize))
    }

    /// Skip results.
    pub fn skip(self, n: i64) -> Self {
        self.add_step(RhaiStep::Skip(n as usize))
    }

    /// Take a range of results.
    pub fn range(self, start: i64, end: i64) -> Self {
        self.add_step(RhaiStep::Range(start as usize, end as usize))
    }

    /// Filter by value equality.
    pub fn is_eq(self, value: Value) -> Self {
        self.add_step(RhaiStep::IsEq(value))
    }

    /// Filter by predicate.
    pub fn is_(self, pred: RhaiPredicate) -> Self {
        self.add_step(RhaiStep::Is(pred))
    }

    /// Filter for simple paths (no repeated vertices).
    pub fn simple_path(self) -> Self {
        self.add_step(RhaiStep::SimplePath)
    }

    /// Filter for cyclic paths (has repeated vertices).
    pub fn cyclic_path(self) -> Self {
        self.add_step(RhaiStep::CyclicPath)
    }

    // =========================================================================
    // Advanced Filter Steps (Phase 7)
    // =========================================================================

    /// Return the last element.
    pub fn tail(self) -> Self {
        self.add_step(RhaiStep::Tail)
    }

    /// Return the last n elements.
    pub fn tail_n(self, n: i64) -> Self {
        self.add_step(RhaiStep::TailN(n as usize))
    }

    /// Probabilistic filter - each element has probability p of passing.
    pub fn coin(self, probability: f64) -> Self {
        self.add_step(RhaiStep::Coin(probability))
    }

    /// Random sample of n elements using reservoir sampling.
    pub fn sample(self, n: i64) -> Self {
        self.add_step(RhaiStep::Sample(n as usize))
    }

    /// Deduplicate by property key.
    pub fn dedup_by_key(self, key: String) -> Self {
        self.add_step(RhaiStep::DedupByKey(key))
    }

    /// Deduplicate by element label.
    pub fn dedup_by_label(self) -> Self {
        self.add_step(RhaiStep::DedupByLabel)
    }

    /// Deduplicate by sub-traversal result.
    pub fn dedup_by(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::DedupBy(traversal))
    }

    /// Filter by multiple element IDs.
    pub fn has_ids(self, ids: Vec<Value>) -> Self {
        self.add_step(RhaiStep::HasIds(ids))
    }

    // =========================================================================
    // Transform Steps
    // =========================================================================

    /// Get element ID.
    pub fn id(self) -> Self {
        self.add_step(RhaiStep::Id)
    }

    /// Get element label.
    pub fn label(self) -> Self {
        self.add_step(RhaiStep::Label)
    }

    /// Get property value.
    pub fn values(self, key: String) -> Self {
        self.add_step(RhaiStep::Values(key))
    }

    /// Get multiple property values.
    pub fn values_multi(self, keys: Vec<String>) -> Self {
        self.add_step(RhaiStep::ValuesMulti(keys))
    }

    /// Get all properties as a map.
    pub fn value_map(self) -> Self {
        self.add_step(RhaiStep::ValueMap)
    }

    /// Get element as a map (with id, label).
    pub fn element_map(self) -> Self {
        self.add_step(RhaiStep::ElementMap)
    }

    /// Get traversal path.
    pub fn path(self) -> Self {
        self.add_step(RhaiStep::Path)
    }

    /// Replace with constant value.
    pub fn constant(self, value: Value) -> Self {
        self.add_step(RhaiStep::Constant(value))
    }

    /// Pass through unchanged.
    pub fn identity(self) -> Self {
        self.add_step(RhaiStep::Identity)
    }

    /// Collect to list.
    pub fn fold(self) -> Self {
        self.add_step(RhaiStep::Fold)
    }

    /// Expand list to elements.
    pub fn unfold(self) -> Self {
        self.add_step(RhaiStep::Unfold)
    }

    /// Count elements (as step, not terminal).
    pub fn count_step(self) -> Self {
        self.add_step(RhaiStep::Count)
    }

    /// Sum numeric values.
    pub fn sum(self) -> Self {
        self.add_step(RhaiStep::Sum)
    }

    /// Calculate mean of numeric values.
    pub fn mean(self) -> Self {
        self.add_step(RhaiStep::Mean)
    }

    /// Get minimum value.
    pub fn min(self) -> Self {
        self.add_step(RhaiStep::Min)
    }

    /// Get maximum value.
    pub fn max(self) -> Self {
        self.add_step(RhaiStep::Max)
    }

    // =========================================================================
    // Advanced Transform Steps (Phase 8)
    // =========================================================================

    /// Get all properties as property objects.
    pub fn properties(self) -> Self {
        self.add_step(RhaiStep::Properties)
    }

    /// Get specific properties as property objects.
    pub fn properties_keys(self, keys: Vec<String>) -> Self {
        self.add_step(RhaiStep::PropertiesKeys(keys))
    }

    /// Get the key from a property.
    pub fn key(self) -> Self {
        self.add_step(RhaiStep::Key)
    }

    /// Get the value from a property.
    pub fn prop_value(self) -> Self {
        self.add_step(RhaiStep::PropValue)
    }

    /// Get specific properties as a map.
    pub fn value_map_keys(self, keys: Vec<String>) -> Self {
        self.add_step(RhaiStep::ValueMapKeys(keys))
    }

    /// Get all properties as a map with id and label tokens.
    pub fn value_map_with_tokens(self) -> Self {
        self.add_step(RhaiStep::ValueMapWithTokens)
    }

    /// Add position index to each element.
    pub fn index(self) -> Self {
        self.add_step(RhaiStep::Index)
    }

    /// Execute sub-traversal in isolated scope.
    pub fn local(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::Local(traversal))
    }

    // =========================================================================
    // Modulator Steps
    // =========================================================================

    /// Label current position.
    pub fn as_(self, label: String) -> Self {
        self.add_step(RhaiStep::As(label))
    }

    /// Select labeled positions.
    pub fn select(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::Select(labels))
    }

    /// Select single labeled position.
    pub fn select_one(self, label: String) -> Self {
        self.add_step(RhaiStep::SelectOne(label))
    }

    // =========================================================================
    // Order Steps
    // =========================================================================

    /// Order results ascending.
    pub fn order_asc(self) -> Self {
        self.add_step(RhaiStep::Order(true))
    }

    /// Order results descending.
    pub fn order_desc(self) -> Self {
        self.add_step(RhaiStep::Order(false))
    }

    // =========================================================================
    // Branch Steps
    // =========================================================================

    /// Union of multiple traversals.
    pub fn union(self, traversals: Vec<RhaiAnonymousTraversal>) -> Self {
        self.add_step(RhaiStep::Union(traversals))
    }

    /// Coalesce - first non-empty result.
    pub fn coalesce(self, traversals: Vec<RhaiAnonymousTraversal>) -> Self {
        self.add_step(RhaiStep::Coalesce(traversals))
    }

    /// Optional - include original if traversal is empty.
    pub fn optional(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::Optional(traversal))
    }

    /// Binary choose - if-then-else branching.
    ///
    /// If the condition traversal produces results, execute the true branch.
    /// Otherwise, execute the false branch.
    pub fn choose_binary(
        self,
        condition: RhaiAnonymousTraversal,
        true_branch: RhaiAnonymousTraversal,
        false_branch: RhaiAnonymousTraversal,
    ) -> Self {
        self.add_step(RhaiStep::Choose(condition, true_branch, false_branch))
    }

    /// Multi-way choose - pattern matching style branching.
    ///
    /// The key_traversal produces a value that is matched against the options.
    /// Options is a vector of (match_value, branch_traversal) pairs.
    /// If no option matches and a default is provided, the default branch is used.
    pub fn choose_options(
        self,
        key_traversal: RhaiAnonymousTraversal,
        options: Vec<(Value, RhaiAnonymousTraversal)>,
        default: Option<RhaiAnonymousTraversal>,
    ) -> Self {
        self.add_step(RhaiStep::ChooseOption(
            key_traversal,
            options,
            default.map(Box::new),
        ))
    }

    // =========================================================================
    // Traversal-Based Filter Steps (Phase 2)
    // =========================================================================

    /// Filter by sub-traversal existence.
    pub fn where_(self, cond: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::Where(cond))
    }

    /// Filter by sub-traversal non-existence.
    pub fn not(self, cond: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::Not(cond))
    }

    /// All sub-traversals must produce results.
    pub fn and_(self, conds: Vec<RhaiAnonymousTraversal>) -> Self {
        self.add_step(RhaiStep::And(conds))
    }

    /// At least one sub-traversal must produce results.
    pub fn or_(self, conds: Vec<RhaiAnonymousTraversal>) -> Self {
        self.add_step(RhaiStep::Or(conds))
    }

    // =========================================================================
    // Repeat Steps
    // =========================================================================

    /// Repeat a traversal a fixed number of times.
    pub fn repeat_times(self, traversal: RhaiAnonymousTraversal, times: i64) -> Self {
        self.add_step(RhaiStep::RepeatTimes(traversal, times as usize))
    }

    /// Repeat a traversal until a condition is met.
    pub fn repeat_until(
        self,
        traversal: RhaiAnonymousTraversal,
        until: RhaiAnonymousTraversal,
    ) -> Self {
        self.add_step(RhaiStep::RepeatUntil(traversal, until))
    }

    /// Repeat a traversal n times, emitting intermediate results.
    pub fn repeat_emit(self, traversal: RhaiAnonymousTraversal, times: i64) -> Self {
        self.add_step(RhaiStep::RepeatEmit(traversal, times as usize))
    }

    /// Repeat a traversal until a condition is met, emitting intermediate results.
    pub fn repeat_emit_until(
        self,
        traversal: RhaiAnonymousTraversal,
        until: RhaiAnonymousTraversal,
    ) -> Self {
        self.add_step(RhaiStep::RepeatEmitUntil(traversal, until))
    }

    // =========================================================================
    // Side Effect Steps (Phase 5)
    // =========================================================================

    /// Store each traverser value into a named side-effect collection (lazy).
    pub fn store(self, key: String) -> Self {
        self.add_step(RhaiStep::Store(key))
    }

    /// Aggregate all traverser values into a named side-effect collection (barrier).
    pub fn aggregate(self, key: String) -> Self {
        self.add_step(RhaiStep::Aggregate(key))
    }

    /// Retrieve accumulated side-effect data (single key).
    pub fn cap(self, key: String) -> Self {
        self.add_step(RhaiStep::Cap(key))
    }

    /// Retrieve accumulated side-effect data (multiple keys).
    pub fn cap_multi(self, keys: Vec<String>) -> Self {
        self.add_step(RhaiStep::CapMulti(keys))
    }

    /// Execute a traversal for side effects only.
    pub fn side_effect(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::SideEffect(traversal))
    }

    // =========================================================================
    // Mutation Steps (Phase 6)
    // =========================================================================

    /// Create a new vertex with the specified label.
    ///
    /// This is a spawning step that creates a new vertex. Properties can be
    /// attached via chained `.property()` calls.
    ///
    /// Note: The actual vertex creation happens when the traversal is executed
    /// via MutationExecutor.
    pub fn add_v(self, label: String) -> Self {
        self.add_step(RhaiStep::AddV(label))
    }

    /// Create a new edge with the specified label.
    ///
    /// The edge requires from/to endpoints to be specified via `from_v()` and
    /// `to_v()` methods, or it will default to using the current traverser
    /// as the source vertex.
    pub fn add_e(self, label: String) -> Self {
        self.add_step(RhaiStep::AddE {
            label,
            from: None,
            to: None,
        })
    }

    /// Set the source vertex for the edge by ID.
    pub fn from_v(mut self, id: VertexId) -> Self {
        // Find the last AddE step and update its from endpoint
        if let Some(RhaiStep::AddE { from, .. }) = self.steps.last_mut() {
            *from = Some(RhaiEdgeEndpoint::VertexId(id));
        }
        self
    }

    /// Set the source vertex for the edge by step label.
    pub fn from_label(mut self, label: String) -> Self {
        if let Some(RhaiStep::AddE { from, .. }) = self.steps.last_mut() {
            *from = Some(RhaiEdgeEndpoint::StepLabel(label));
        }
        self
    }

    /// Set the target vertex for the edge by ID.
    pub fn to_v(mut self, id: VertexId) -> Self {
        if let Some(RhaiStep::AddE { to, .. }) = self.steps.last_mut() {
            *to = Some(RhaiEdgeEndpoint::VertexId(id));
        }
        self
    }

    /// Set the target vertex for the edge by step label.
    pub fn to_label(mut self, label: String) -> Self {
        if let Some(RhaiStep::AddE { to, .. }) = self.steps.last_mut() {
            *to = Some(RhaiEdgeEndpoint::StepLabel(label));
        }
        self
    }

    /// Add or update a property on the current element.
    ///
    /// Can be chained after `add_v()` or `add_e()` to set properties on
    /// newly created elements, or used on existing vertices/edges.
    pub fn property(self, key: String, value: Value) -> Self {
        self.add_step(RhaiStep::Property(key, value))
    }

    /// Delete the current element (vertex or edge).
    ///
    /// When a vertex is dropped, all its incident edges are also deleted.
    /// Note: We use `drop_` because `drop` is a Rust reserved keyword.
    pub fn drop_(self) -> Self {
        self.add_step(RhaiStep::Drop)
    }

    // =========================================================================
    // Builder Pattern Steps (Phase 11)
    // =========================================================================

    /// Order by a property in ascending order.
    ///
    /// # Example (Rhai)
    /// ```javascript
    /// g.v().has_label("person").order_by("age").values("name").to_list()
    /// ```
    pub fn order_by(self, key: String) -> Self {
        self.add_step(RhaiStep::OrderBy(key, true))
    }

    /// Order by a property in descending order.
    ///
    /// # Example (Rhai)
    /// ```javascript
    /// g.v().has_label("person").order_by_desc("age").values("name").to_list()
    /// ```
    pub fn order_by_desc(self, key: String) -> Self {
        self.add_step(RhaiStep::OrderBy(key, false))
    }

    /// Order by a sub-traversal result in ascending order.
    pub fn order_by_traversal(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::OrderByTraversal(traversal, true))
    }

    /// Order by a sub-traversal result in descending order.
    pub fn order_by_traversal_desc(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::OrderByTraversal(traversal, false))
    }

    /// Project elements into a map with the given keys.
    ///
    /// This requires a vector of keys and a vector of projections (either property
    /// keys or sub-traversals) that define how each key's value is computed.
    ///
    /// # Example (Rhai)
    /// ```javascript
    /// g.v().has_label("person").project(["name", "friends"], [
    ///     anon().values("name"),
    ///     anon().out("knows").count()
    /// ]).to_list()
    /// ```
    pub fn project(self, keys: Vec<String>, projections: Vec<RhaiProjection>) -> Self {
        self.add_step(RhaiStep::Project(keys, projections))
    }

    /// Group elements by a key.
    ///
    /// # Example (Rhai)
    /// ```javascript
    /// g.v().has_label("person").group_by_key("city").to_list()
    /// ```
    pub fn group(self, key_selector: RhaiGroupKey, value_collector: RhaiGroupValue) -> Self {
        self.add_step(RhaiStep::Group(key_selector, value_collector))
    }

    /// Group elements by label with identity values.
    pub fn group_by_label(self) -> Self {
        self.add_step(RhaiStep::Group(
            RhaiGroupKey::Label,
            RhaiGroupValue::Identity,
        ))
    }

    /// Group elements by a property key with identity values.
    pub fn group_by_key(self, key: String) -> Self {
        self.add_step(RhaiStep::Group(
            RhaiGroupKey::Property(key),
            RhaiGroupValue::Identity,
        ))
    }

    /// Count elements by group key.
    ///
    /// # Example (Rhai)
    /// ```javascript
    /// g.v().has_label("person").group_count_by_key("city").to_list()
    /// ```
    pub fn group_count(self, key_selector: RhaiGroupKey) -> Self {
        self.add_step(RhaiStep::GroupCount(key_selector))
    }

    /// Count elements by label.
    pub fn group_count_by_label(self) -> Self {
        self.add_step(RhaiStep::GroupCount(RhaiGroupKey::Label))
    }

    /// Count elements by property key.
    pub fn group_count_by_key(self, key: String) -> Self {
        self.add_step(RhaiStep::GroupCount(RhaiGroupKey::Property(key)))
    }

    /// Evaluate a mathematical expression on numeric values.
    ///
    /// The expression can use `_` to refer to the current value, or named
    /// variables that are bound to property keys via the bindings map.
    ///
    /// # Example (Rhai)
    /// ```javascript
    /// // Double each value
    /// g.v().values("age").math("_ * 2").to_list()
    ///
    /// // Use named variables (bound to labeled elements)
    /// g.v().as_("a").out().as_("b").math_with_bindings("a - b", #{ "a": "age", "b": "age" }).to_list()
    /// ```
    pub fn math(self, expression: String) -> Self {
        self.add_step(RhaiStep::Math(expression, HashMap::new()))
    }

    /// Evaluate a mathematical expression with variable bindings.
    pub fn math_with_bindings(self, expression: String, bindings: HashMap<String, String>) -> Self {
        self.add_step(RhaiStep::Math(expression, bindings))
    }
}

/// Anonymous traversal wrapper for Rhai.
///
/// This stores steps that will be applied to an anonymous traversal.
#[derive(Clone, Debug)]
pub struct RhaiAnonymousTraversal {
    steps: Vec<RhaiStep>,
}

impl Default for RhaiAnonymousTraversal {
    fn default() -> Self {
        Self::new()
    }
}

impl RhaiAnonymousTraversal {
    /// Create a new empty anonymous traversal.
    pub fn new() -> Self {
        RhaiAnonymousTraversal { steps: Vec::new() }
    }

    /// Add a step to this traversal.
    fn add_step(mut self, step: RhaiStep) -> Self {
        self.steps.push(step);
        self
    }

    /// Convert to a real anonymous traversal.
    pub fn to_traversal(&self) -> Traversal<Value, Value> {
        let mut traversal = __::identity();
        for step in &self.steps {
            traversal = apply_anonymous_step(traversal, step);
        }
        traversal
    }

    // Navigation steps
    pub fn out(self) -> Self {
        self.add_step(RhaiStep::Out(vec![]))
    }

    pub fn out_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::Out(labels))
    }

    pub fn in_(self) -> Self {
        self.add_step(RhaiStep::In(vec![]))
    }

    pub fn in_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::In(labels))
    }

    pub fn both(self) -> Self {
        self.add_step(RhaiStep::Both(vec![]))
    }

    pub fn out_e(self) -> Self {
        self.add_step(RhaiStep::OutE(vec![]))
    }

    pub fn in_e(self) -> Self {
        self.add_step(RhaiStep::InE(vec![]))
    }

    pub fn both_e(self) -> Self {
        self.add_step(RhaiStep::BothE(vec![]))
    }

    pub fn out_v(self) -> Self {
        self.add_step(RhaiStep::OutV)
    }

    pub fn in_v(self) -> Self {
        self.add_step(RhaiStep::InV)
    }

    pub fn other_v(self) -> Self {
        self.add_step(RhaiStep::OtherV)
    }

    pub fn both_v(self) -> Self {
        self.add_step(RhaiStep::BothV)
    }

    // Filter steps
    pub fn has_label(self, label: String) -> Self {
        self.add_step(RhaiStep::HasLabel(vec![label]))
    }

    pub fn has(self, key: String) -> Self {
        self.add_step(RhaiStep::Has(key))
    }

    pub fn has_not(self, key: String) -> Self {
        self.add_step(RhaiStep::HasNot(key))
    }

    pub fn has_value(self, key: String, value: Value) -> Self {
        self.add_step(RhaiStep::HasValue(key, value))
    }

    pub fn has_where(self, key: String, pred: RhaiPredicate) -> Self {
        self.add_step(RhaiStep::HasWhere(key, pred))
    }

    pub fn dedup(self) -> Self {
        self.add_step(RhaiStep::Dedup)
    }

    pub fn limit(self, n: i64) -> Self {
        self.add_step(RhaiStep::Limit(n as usize))
    }

    // Transform steps
    pub fn id(self) -> Self {
        self.add_step(RhaiStep::Id)
    }

    pub fn label(self) -> Self {
        self.add_step(RhaiStep::Label)
    }

    pub fn values(self, key: String) -> Self {
        self.add_step(RhaiStep::Values(key))
    }

    pub fn value_map(self) -> Self {
        self.add_step(RhaiStep::ValueMap)
    }

    pub fn path(self) -> Self {
        self.add_step(RhaiStep::Path)
    }

    pub fn constant(self, value: Value) -> Self {
        self.add_step(RhaiStep::Constant(value))
    }

    pub fn identity(self) -> Self {
        self.add_step(RhaiStep::Identity)
    }

    pub fn fold(self) -> Self {
        self.add_step(RhaiStep::Fold)
    }

    pub fn unfold(self) -> Self {
        self.add_step(RhaiStep::Unfold)
    }

    // Advanced transform steps (Phase 8)
    pub fn properties(self) -> Self {
        self.add_step(RhaiStep::Properties)
    }

    pub fn properties_keys(self, keys: Vec<String>) -> Self {
        self.add_step(RhaiStep::PropertiesKeys(keys))
    }

    pub fn key(self) -> Self {
        self.add_step(RhaiStep::Key)
    }

    pub fn prop_value(self) -> Self {
        self.add_step(RhaiStep::PropValue)
    }

    pub fn value_map_keys(self, keys: Vec<String>) -> Self {
        self.add_step(RhaiStep::ValueMapKeys(keys))
    }

    pub fn value_map_with_tokens(self) -> Self {
        self.add_step(RhaiStep::ValueMapWithTokens)
    }

    pub fn index(self) -> Self {
        self.add_step(RhaiStep::Index)
    }

    pub fn local(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::Local(traversal))
    }

    // Modulator steps
    pub fn as_(self, label: String) -> Self {
        self.add_step(RhaiStep::As(label))
    }

    // === Phase 1: Additional methods for parity with RhaiTraversal ===

    // Navigation with labels
    pub fn both_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::Both(labels))
    }

    pub fn out_e_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::OutE(labels))
    }

    pub fn in_e_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::InE(labels))
    }

    pub fn both_e_labels(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::BothE(labels))
    }

    // Additional filter steps
    pub fn skip(self, n: i64) -> Self {
        self.add_step(RhaiStep::Skip(n as usize))
    }

    pub fn range(self, start: i64, end: i64) -> Self {
        self.add_step(RhaiStep::Range(start as usize, end as usize))
    }

    pub fn is_(self, pred: RhaiPredicate) -> Self {
        self.add_step(RhaiStep::Is(pred))
    }

    pub fn is_eq(self, value: Value) -> Self {
        self.add_step(RhaiStep::IsEq(value))
    }

    pub fn simple_path(self) -> Self {
        self.add_step(RhaiStep::SimplePath)
    }

    pub fn cyclic_path(self) -> Self {
        self.add_step(RhaiStep::CyclicPath)
    }

    pub fn has_id(self, id: VertexId) -> Self {
        self.add_step(RhaiStep::HasId(Value::Vertex(id)))
    }

    pub fn has_label_any(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::HasLabel(labels))
    }

    // Additional transform steps
    pub fn element_map(self) -> Self {
        self.add_step(RhaiStep::ElementMap)
    }

    pub fn values_multi(self, keys: Vec<String>) -> Self {
        self.add_step(RhaiStep::ValuesMulti(keys))
    }

    pub fn sum(self) -> Self {
        self.add_step(RhaiStep::Sum)
    }

    pub fn mean(self) -> Self {
        self.add_step(RhaiStep::Mean)
    }

    pub fn min(self) -> Self {
        self.add_step(RhaiStep::Min)
    }

    pub fn max(self) -> Self {
        self.add_step(RhaiStep::Max)
    }

    pub fn count(self) -> Self {
        self.add_step(RhaiStep::Count)
    }

    // Additional modulator steps
    pub fn select(self, labels: Vec<String>) -> Self {
        self.add_step(RhaiStep::Select(labels))
    }

    pub fn select_one(self, label: String) -> Self {
        self.add_step(RhaiStep::SelectOne(label))
    }

    pub fn order_asc(self) -> Self {
        self.add_step(RhaiStep::Order(true))
    }

    pub fn order_desc(self) -> Self {
        self.add_step(RhaiStep::Order(false))
    }

    // Traversal-based filter steps (Phase 2)
    pub fn where_(self, cond: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::Where(cond))
    }

    pub fn not(self, cond: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::Not(cond))
    }

    pub fn and_(self, conds: Vec<RhaiAnonymousTraversal>) -> Self {
        self.add_step(RhaiStep::And(conds))
    }

    pub fn or_(self, conds: Vec<RhaiAnonymousTraversal>) -> Self {
        self.add_step(RhaiStep::Or(conds))
    }

    // =========================================================================
    // Advanced Filter Steps (Phase 7)
    // =========================================================================

    /// Return the last element.
    pub fn tail(self) -> Self {
        self.add_step(RhaiStep::Tail)
    }

    /// Return the last n elements.
    pub fn tail_n(self, n: i64) -> Self {
        self.add_step(RhaiStep::TailN(n as usize))
    }

    /// Probabilistic filter - each element has probability p of passing.
    pub fn coin(self, probability: f64) -> Self {
        self.add_step(RhaiStep::Coin(probability))
    }

    /// Random sample of n elements using reservoir sampling.
    pub fn sample(self, n: i64) -> Self {
        self.add_step(RhaiStep::Sample(n as usize))
    }

    /// Deduplicate by property key.
    pub fn dedup_by_key(self, key: String) -> Self {
        self.add_step(RhaiStep::DedupByKey(key))
    }

    /// Deduplicate by element label.
    pub fn dedup_by_label(self) -> Self {
        self.add_step(RhaiStep::DedupByLabel)
    }

    /// Deduplicate by sub-traversal result.
    pub fn dedup_by(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::DedupBy(traversal))
    }

    /// Filter by multiple element IDs.
    pub fn has_ids(self, ids: Vec<Value>) -> Self {
        self.add_step(RhaiStep::HasIds(ids))
    }

    // =========================================================================
    // Side Effect Steps (Phase 5)
    // =========================================================================

    /// Store each traverser value into a named side-effect collection (lazy).
    pub fn store(self, key: String) -> Self {
        self.add_step(RhaiStep::Store(key))
    }

    /// Aggregate all traverser values into a named side-effect collection (barrier).
    pub fn aggregate(self, key: String) -> Self {
        self.add_step(RhaiStep::Aggregate(key))
    }

    /// Retrieve accumulated side-effect data (single key).
    pub fn cap(self, key: String) -> Self {
        self.add_step(RhaiStep::Cap(key))
    }

    /// Retrieve accumulated side-effect data (multiple keys).
    pub fn cap_multi(self, keys: Vec<String>) -> Self {
        self.add_step(RhaiStep::CapMulti(keys))
    }

    /// Execute a traversal for side effects only.
    pub fn side_effect(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::SideEffect(traversal))
    }

    // =========================================================================
    // Mutation Steps (Phase 6)
    // =========================================================================

    /// Create a new vertex with the specified label.
    pub fn add_v(self, label: String) -> Self {
        self.add_step(RhaiStep::AddV(label))
    }

    /// Create a new edge with the specified label.
    pub fn add_e(self, label: String) -> Self {
        self.add_step(RhaiStep::AddE {
            label,
            from: None,
            to: None,
        })
    }

    /// Set the source vertex for the edge by ID.
    pub fn from_v(mut self, id: VertexId) -> Self {
        if let Some(RhaiStep::AddE { from, .. }) = self.steps.last_mut() {
            *from = Some(RhaiEdgeEndpoint::VertexId(id));
        }
        self
    }

    /// Set the source vertex for the edge by step label.
    pub fn from_label(mut self, label: String) -> Self {
        if let Some(RhaiStep::AddE { from, .. }) = self.steps.last_mut() {
            *from = Some(RhaiEdgeEndpoint::StepLabel(label));
        }
        self
    }

    /// Set the target vertex for the edge by ID.
    pub fn to_v(mut self, id: VertexId) -> Self {
        if let Some(RhaiStep::AddE { to, .. }) = self.steps.last_mut() {
            *to = Some(RhaiEdgeEndpoint::VertexId(id));
        }
        self
    }

    /// Set the target vertex for the edge by step label.
    pub fn to_label(mut self, label: String) -> Self {
        if let Some(RhaiStep::AddE { to, .. }) = self.steps.last_mut() {
            *to = Some(RhaiEdgeEndpoint::StepLabel(label));
        }
        self
    }

    /// Add or update a property on the current element.
    pub fn property(self, key: String, value: Value) -> Self {
        self.add_step(RhaiStep::Property(key, value))
    }

    /// Delete the current element (vertex or edge).
    pub fn drop_(self) -> Self {
        self.add_step(RhaiStep::Drop)
    }

    // =========================================================================
    // Branching Steps (Phase 9)
    // =========================================================================

    /// Binary choose - if-then-else branching.
    ///
    /// If the condition traversal produces results, execute the true branch.
    /// Otherwise, execute the false branch.
    pub fn choose_binary(
        self,
        condition: RhaiAnonymousTraversal,
        true_branch: RhaiAnonymousTraversal,
        false_branch: RhaiAnonymousTraversal,
    ) -> Self {
        self.add_step(RhaiStep::Choose(condition, true_branch, false_branch))
    }

    /// Multi-way choose - pattern matching style branching.
    ///
    /// The key_traversal produces a value that is matched against the options.
    /// Options is a vector of (match_value, branch_traversal) pairs.
    /// If no option matches and a default is provided, the default branch is used.
    pub fn choose_options(
        self,
        key_traversal: RhaiAnonymousTraversal,
        options: Vec<(Value, RhaiAnonymousTraversal)>,
        default: Option<RhaiAnonymousTraversal>,
    ) -> Self {
        self.add_step(RhaiStep::ChooseOption(
            key_traversal,
            options,
            default.map(Box::new),
        ))
    }

    // =========================================================================
    // Builder Pattern Steps (Phase 11)
    // =========================================================================

    /// Order by a property in ascending order.
    pub fn order_by(self, key: String) -> Self {
        self.add_step(RhaiStep::OrderBy(key, true))
    }

    /// Order by a property in descending order.
    pub fn order_by_desc(self, key: String) -> Self {
        self.add_step(RhaiStep::OrderBy(key, false))
    }

    /// Order by a sub-traversal result in ascending order.
    pub fn order_by_traversal(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::OrderByTraversal(traversal, true))
    }

    /// Order by a sub-traversal result in descending order.
    pub fn order_by_traversal_desc(self, traversal: RhaiAnonymousTraversal) -> Self {
        self.add_step(RhaiStep::OrderByTraversal(traversal, false))
    }

    /// Project elements into a map with the given keys.
    pub fn project(self, keys: Vec<String>, projections: Vec<RhaiProjection>) -> Self {
        self.add_step(RhaiStep::Project(keys, projections))
    }

    /// Group elements by a key.
    pub fn group(self, key_selector: RhaiGroupKey, value_collector: RhaiGroupValue) -> Self {
        self.add_step(RhaiStep::Group(key_selector, value_collector))
    }

    /// Group elements by label with identity values.
    pub fn group_by_label(self) -> Self {
        self.add_step(RhaiStep::Group(
            RhaiGroupKey::Label,
            RhaiGroupValue::Identity,
        ))
    }

    /// Group elements by a property key with identity values.
    pub fn group_by_key(self, key: String) -> Self {
        self.add_step(RhaiStep::Group(
            RhaiGroupKey::Property(key),
            RhaiGroupValue::Identity,
        ))
    }

    /// Count elements by group key.
    pub fn group_count(self, key_selector: RhaiGroupKey) -> Self {
        self.add_step(RhaiStep::GroupCount(key_selector))
    }

    /// Count elements by label.
    pub fn group_count_by_label(self) -> Self {
        self.add_step(RhaiStep::GroupCount(RhaiGroupKey::Label))
    }

    /// Count elements by property key.
    pub fn group_count_by_key(self, key: String) -> Self {
        self.add_step(RhaiStep::GroupCount(RhaiGroupKey::Property(key)))
    }

    /// Evaluate a mathematical expression on numeric values.
    pub fn math(self, expression: String) -> Self {
        self.add_step(RhaiStep::Math(expression, HashMap::new()))
    }

    /// Evaluate a mathematical expression with variable bindings.
    pub fn math_with_bindings(self, expression: String, bindings: HashMap<String, String>) -> Self {
        self.add_step(RhaiStep::Math(expression, bindings))
    }
}

// =============================================================================
// Step Application
// =============================================================================

/// Apply a step to a CowBoundTraversal (supports mutations).
///
/// This function uses `add_step()` with step types directly, which works with
/// `CowBoundTraversal` for mutation support via `graph.gremlin()`.
#[allow(unused_imports)]
fn apply_step_cow<'g, In>(
    bound: CowBoundTraversal<'g, In, Value>,
    step: &RhaiStep,
) -> CowBoundTraversal<'g, In, Value> {
    use crate::traversal::filter::*;
    use crate::traversal::navigation::*;
    use crate::traversal::sideeffect::*;
    use crate::traversal::transform::*;

    match step {
        // Navigation
        RhaiStep::Out(labels) if labels.is_empty() => bound.add_step(OutStep::new()),
        RhaiStep::Out(labels) => bound.add_step(OutStep::with_labels(labels.clone())),
        RhaiStep::In(labels) if labels.is_empty() => bound.add_step(InStep::new()),
        RhaiStep::In(labels) => bound.add_step(InStep::with_labels(labels.clone())),
        RhaiStep::Both(labels) if labels.is_empty() => bound.add_step(BothStep::new()),
        RhaiStep::Both(labels) => bound.add_step(BothStep::with_labels(labels.clone())),
        RhaiStep::OutE(labels) if labels.is_empty() => bound.add_step(OutEStep::new()),
        RhaiStep::OutE(labels) => bound.add_step(OutEStep::with_labels(labels.clone())),
        RhaiStep::InE(labels) if labels.is_empty() => bound.add_step(InEStep::new()),
        RhaiStep::InE(labels) => bound.add_step(InEStep::with_labels(labels.clone())),
        RhaiStep::BothE(labels) if labels.is_empty() => bound.add_step(BothEStep::new()),
        RhaiStep::BothE(labels) => bound.add_step(BothEStep::with_labels(labels.clone())),
        RhaiStep::OutV => bound.add_step(OutVStep),
        RhaiStep::InV => bound.add_step(InVStep),
        RhaiStep::OtherV => bound.add_step(OtherVStep),
        RhaiStep::BothV => bound.add_step(BothVStep),

        // Filter
        RhaiStep::HasLabel(labels) if labels.len() == 1 => {
            bound.add_step(HasLabelStep::single(labels[0].clone()))
        }
        RhaiStep::HasLabel(labels) => bound.add_step(HasLabelStep::any(labels.clone())),
        RhaiStep::Has(key) => bound.add_step(HasStep::new(key.clone())),
        RhaiStep::HasNot(key) => bound.add_step(HasNotStep::new(key.clone())),
        RhaiStep::HasValue(key, value) => {
            bound.add_step(HasValueStep::new(key.clone(), value.clone()))
        }
        RhaiStep::HasWhere(key, pred) => {
            bound.add_step(HasWhereStep::new(key.clone(), pred.clone()))
        }
        RhaiStep::Dedup => bound.add_step(DedupStep),
        RhaiStep::Limit(n) => bound.add_step(LimitStep::new(*n)),
        RhaiStep::Skip(n) => bound.add_step(SkipStep::new(*n)),
        RhaiStep::Range(start, end) => bound.add_step(RangeStep::new(*start, *end)),
        RhaiStep::IsEq(value) => bound.add_step(IsStep::eq(value.clone())),
        RhaiStep::Is(pred) => bound.add_step(IsStep::new(pred.clone())),
        RhaiStep::SimplePath => bound.add_step(SimplePathStep::new()),
        RhaiStep::CyclicPath => bound.add_step(CyclicPathStep::new()),
        RhaiStep::HasId(id) => match id {
            Value::Vertex(vid) => bound.add_step(HasIdStep::vertex(*vid)),
            Value::Edge(eid) => bound.add_step(HasIdStep::edge(*eid)),
            Value::Int(n) => bound.add_step(HasIdStep::vertex(VertexId(*n as u64))),
            _ => bound, // Ignore invalid IDs
        },

        // Advanced filter steps (Phase 7)
        RhaiStep::Tail => bound.add_step(TailStep::last()),
        RhaiStep::TailN(n) => bound.add_step(TailStep::new(*n)),
        RhaiStep::Coin(probability) => bound.add_step(CoinStep::new(*probability)),
        RhaiStep::Sample(n) => bound.add_step(SampleStep::new(*n)),
        RhaiStep::DedupByKey(key) => bound.add_step(DedupByKeyStep::new(key.clone())),
        RhaiStep::DedupByLabel => bound.add_step(DedupByLabelStep::new()),
        RhaiStep::DedupBy(t) => bound.add_step(DedupByTraversalStep::new(t.to_traversal())),
        RhaiStep::HasIds(ids) => bound.add_step(HasIdStep::from_values(ids.clone())),

        // Transform
        RhaiStep::Id => bound.add_step(IdStep),
        RhaiStep::Label => bound.add_step(LabelStep),
        RhaiStep::Values(key) => bound.add_step(ValuesStep::new(key.clone())),
        RhaiStep::ValuesMulti(keys) => bound.add_step(ValuesStep::multi(keys.clone())),
        RhaiStep::ValueMap => bound.add_step(ValueMapStep::new()),
        RhaiStep::ElementMap => bound.add_step(ElementMapStep::new()),
        RhaiStep::Path => bound.add_step(PathStep::new()),
        RhaiStep::Constant(value) => bound.add_step(ConstantStep::new(value.clone())),
        RhaiStep::Identity => bound.add_step(IdentityStep),
        RhaiStep::Unfold => bound.add_step(UnfoldStep),
        RhaiStep::Mean => bound.add_step(MeanStep),
        // Fold, Count, Sum, Min, Max are terminal operations - skip in step chains
        RhaiStep::Fold | RhaiStep::Count | RhaiStep::Sum | RhaiStep::Min | RhaiStep::Max => bound,

        // Advanced transform steps (Phase 8)
        RhaiStep::Properties => bound.add_step(PropertiesStep::new()),
        RhaiStep::PropertiesKeys(keys) => bound.add_step(PropertiesStep::with_keys(keys.clone())),
        RhaiStep::Key => bound.add_step(KeyStep),
        RhaiStep::PropValue => bound.add_step(ValueStep),
        RhaiStep::ValueMapKeys(keys) => bound.add_step(ValueMapStep::with_keys(keys.clone())),
        RhaiStep::ValueMapWithTokens => bound.add_step(ValueMapStep::new().with_tokens()),
        RhaiStep::Index => bound.add_step(IndexStep),
        RhaiStep::Local(sub) => {
            use crate::traversal::branch::LocalStep;
            bound.add_step(LocalStep::new(sub.to_traversal()))
        }

        // Modulator
        RhaiStep::As(label) => bound.add_step(AsStep::new(label.clone())),
        RhaiStep::Select(labels) => bound.add_step(SelectStep::new(labels.clone())),
        RhaiStep::SelectOne(label) => bound.add_step(SelectStep::single(label.clone())),

        // Order
        RhaiStep::Order(asc) => {
            use crate::traversal::transform::order::{Order, OrderStep};
            let order = if *asc { Order::Asc } else { Order::Desc };
            bound.add_step(OrderStep::by_natural(order))
        }

        // Branch
        RhaiStep::Union(traversals) => {
            use crate::traversal::branch::UnionStep;
            let anon_traversals: Vec<_> = traversals.iter().map(|t| t.to_traversal()).collect();
            bound.add_step(UnionStep::new(anon_traversals))
        }
        RhaiStep::Coalesce(traversals) => {
            use crate::traversal::branch::CoalesceStep;
            let anon_traversals: Vec<_> = traversals.iter().map(|t| t.to_traversal()).collect();
            bound.add_step(CoalesceStep::new(anon_traversals))
        }
        RhaiStep::Optional(traversal) => {
            use crate::traversal::branch::OptionalStep;
            bound.add_step(OptionalStep::new(traversal.to_traversal()))
        }
        RhaiStep::Choose(cond, true_branch, false_branch) => {
            use crate::traversal::branch::ChooseStep;
            bound.add_step(ChooseStep::new(
                cond.to_traversal(),
                true_branch.to_traversal(),
                false_branch.to_traversal(),
            ))
        }
        RhaiStep::ChooseOption(key_traversal, options, default) => {
            use crate::traversal::branch::{BranchStep, OptionKey};
            let mut step = BranchStep::new(key_traversal.to_traversal());
            for (value, t) in options {
                step = step.add_option(OptionKey::Value(value.clone()), t.to_traversal());
            }
            if let Some(default_traversal) = default {
                step = step.add_none_option(default_traversal.to_traversal());
            }
            bound.add_step(step)
        }

        // Repeat steps
        RhaiStep::RepeatTimes(traversal, times) => {
            use crate::traversal::repeat::{RepeatConfig, RepeatStep};
            let config = RepeatConfig::new().with_times(*times);
            bound.add_step(RepeatStep::with_config(traversal.to_traversal(), config))
        }
        RhaiStep::RepeatUntil(traversal, until) => {
            use crate::traversal::repeat::{RepeatConfig, RepeatStep};
            let config = RepeatConfig::new().with_until(until.to_traversal());
            bound.add_step(RepeatStep::with_config(traversal.to_traversal(), config))
        }
        RhaiStep::RepeatEmit(traversal, times) => {
            use crate::traversal::repeat::{RepeatConfig, RepeatStep};
            let config = RepeatConfig::new().with_times(*times).with_emit();
            bound.add_step(RepeatStep::with_config(traversal.to_traversal(), config))
        }
        RhaiStep::RepeatEmitUntil(traversal, until) => {
            use crate::traversal::repeat::{RepeatConfig, RepeatStep};
            let config = RepeatConfig::new()
                .with_until(until.to_traversal())
                .with_emit();
            bound.add_step(RepeatStep::with_config(traversal.to_traversal(), config))
        }

        // Traversal-based filter steps (Phase 2)
        RhaiStep::Where(cond) => bound.append(__::where_(cond.to_traversal())),
        RhaiStep::Not(cond) => bound.append(__::not(cond.to_traversal())),
        RhaiStep::And(conds) => {
            let anon_traversals: Vec<_> = conds.iter().map(|t| t.to_traversal()).collect();
            bound.append(__::and_(anon_traversals))
        }
        RhaiStep::Or(conds) => {
            let anon_traversals: Vec<_> = conds.iter().map(|t| t.to_traversal()).collect();
            bound.append(__::or_(anon_traversals))
        }

        // Side effect steps (Phase 5)
        RhaiStep::Store(key) => bound.add_step(StoreStep::new(key.clone())),
        RhaiStep::Aggregate(key) => bound.add_step(AggregateStep::new(key.clone())),
        RhaiStep::Cap(key) => bound.add_step(CapStep::new(key.clone())),
        RhaiStep::CapMulti(keys) => bound.add_step(CapStep::multi(keys.clone())),
        RhaiStep::SideEffect(t) => bound.add_step(SideEffectStep::new(t.to_traversal())),

        // Mutation steps (Phase 6)
        RhaiStep::AddV(label) => {
            use crate::traversal::mutation::AddVStep;
            bound.add_step(AddVStep::new(label.clone()))
        }
        RhaiStep::AddE { label, from, to } => {
            use crate::traversal::mutation::AddEStep;
            let mut step = AddEStep::new(label.clone());
            if let Some(endpoint) = from {
                step = match endpoint {
                    RhaiEdgeEndpoint::VertexId(id) => step.from_vertex(*id),
                    RhaiEdgeEndpoint::StepLabel(lbl) => step.from_label(lbl.clone()),
                };
            }
            if let Some(endpoint) = to {
                step = match endpoint {
                    RhaiEdgeEndpoint::VertexId(id) => step.to_vertex(*id),
                    RhaiEdgeEndpoint::StepLabel(lbl) => step.to_label(lbl.clone()),
                };
            }
            bound.add_step(step)
        }
        RhaiStep::Property(key, value) => {
            use crate::traversal::mutation::PropertyStep;
            bound.add_step(PropertyStep::new(key.clone(), value.clone()))
        }
        RhaiStep::Drop => {
            use crate::traversal::mutation::DropStep;
            bound.add_step(DropStep)
        }

        // Builder pattern steps (Phase 11)
        RhaiStep::OrderBy(key, asc) => {
            use crate::traversal::transform::order::{Order, OrderStep};
            let order = if *asc { Order::Asc } else { Order::Desc };
            bound.add_step(OrderStep::by_property(key.clone(), order))
        }
        RhaiStep::OrderByTraversal(sub, asc) => {
            use crate::traversal::transform::order::{Order, OrderKey, OrderStep};
            let order = if *asc { Order::Asc } else { Order::Desc };
            bound.add_step(OrderStep::with_keys(vec![OrderKey::Traversal(
                sub.to_traversal(),
                order,
            )]))
        }
        RhaiStep::Project(keys, projections) => {
            use crate::traversal::transform::{ProjectStep, Projection};
            let core_projections: Vec<Projection> = projections
                .iter()
                .map(|p| match p {
                    RhaiProjection::Key(k) => Projection::Key(k.clone()),
                    RhaiProjection::Traversal(t) => Projection::Traversal(t.to_traversal()),
                })
                .collect();
            bound.add_step(ProjectStep::new(keys.clone(), core_projections))
        }
        RhaiStep::Group(key_selector, value_collector) => {
            use crate::traversal::aggregate::{GroupKey, GroupStep, GroupValue};
            let core_key = match key_selector {
                RhaiGroupKey::Label => GroupKey::Label,
                RhaiGroupKey::Property(k) => GroupKey::Property(k.clone()),
                RhaiGroupKey::Traversal(t) => GroupKey::Traversal(Box::new(t.to_traversal())),
            };
            let core_value = match value_collector {
                RhaiGroupValue::Identity => GroupValue::Identity,
                RhaiGroupValue::Property(k) => GroupValue::Property(k.clone()),
                RhaiGroupValue::Traversal(t) => GroupValue::Traversal(Box::new(t.to_traversal())),
            };
            bound.add_step(GroupStep::with_selectors(core_key, core_value))
        }
        RhaiStep::GroupCount(key_selector) => {
            use crate::traversal::aggregate::{GroupCountStep, GroupKey};
            let core_key = match key_selector {
                RhaiGroupKey::Label => GroupKey::Label,
                RhaiGroupKey::Property(k) => GroupKey::Property(k.clone()),
                RhaiGroupKey::Traversal(t) => GroupKey::Traversal(Box::new(t.to_traversal())),
            };
            bound.add_step(GroupCountStep::new(core_key))
        }
        RhaiStep::Math(expression, bindings) => {
            use crate::traversal::transform::MathStep;
            if bindings.is_empty() {
                bound.add_step(MathStep::new(expression.clone()))
            } else {
                bound.add_step(MathStep::with_bindings(
                    expression.clone(),
                    bindings.clone(),
                ))
            }
        }

        // Anonymous traversal (for appending)
        RhaiStep::Anonymous(anon) => bound.append(anon.to_traversal()),
    }
}

/// Apply a step to an anonymous traversal.
#[allow(unused_imports)]
fn apply_anonymous_step(
    traversal: Traversal<Value, Value>,
    step: &RhaiStep,
) -> Traversal<Value, Value> {
    use crate::traversal::filter::*;
    use crate::traversal::navigation::*;
    use crate::traversal::sideeffect::*;
    use crate::traversal::step::IdentityStep;
    use crate::traversal::transform::*;

    match step {
        // Navigation
        RhaiStep::Out(labels) if labels.is_empty() => traversal.add_step(OutStep::new()),
        RhaiStep::Out(labels) => traversal.add_step(OutStep::with_labels(labels.clone())),
        RhaiStep::In(labels) if labels.is_empty() => traversal.add_step(InStep::new()),
        RhaiStep::In(labels) => traversal.add_step(InStep::with_labels(labels.clone())),
        RhaiStep::Both(labels) if labels.is_empty() => traversal.add_step(BothStep::new()),
        RhaiStep::Both(labels) => traversal.add_step(BothStep::with_labels(labels.clone())),
        RhaiStep::OutE(labels) if labels.is_empty() => traversal.add_step(OutEStep::new()),
        RhaiStep::OutE(labels) => traversal.add_step(OutEStep::with_labels(labels.clone())),
        RhaiStep::InE(labels) if labels.is_empty() => traversal.add_step(InEStep::new()),
        RhaiStep::InE(labels) => traversal.add_step(InEStep::with_labels(labels.clone())),
        RhaiStep::BothE(labels) if labels.is_empty() => traversal.add_step(BothEStep::new()),
        RhaiStep::BothE(labels) => traversal.add_step(BothEStep::with_labels(labels.clone())),
        RhaiStep::OutV => traversal.add_step(OutVStep),
        RhaiStep::InV => traversal.add_step(InVStep),
        RhaiStep::OtherV => traversal.add_step(OtherVStep),
        RhaiStep::BothV => traversal.add_step(BothVStep),

        // Filter
        RhaiStep::HasLabel(labels) if labels.len() == 1 => {
            traversal.add_step(HasLabelStep::single(labels[0].clone()))
        }
        RhaiStep::HasLabel(labels) => traversal.add_step(HasLabelStep::any(labels.clone())),
        RhaiStep::Has(key) => traversal.add_step(HasStep::new(key.clone())),
        RhaiStep::HasNot(key) => traversal.add_step(HasNotStep::new(key.clone())),
        RhaiStep::HasValue(key, value) => {
            traversal.add_step(HasValueStep::new(key.clone(), value.clone()))
        }
        RhaiStep::HasWhere(key, pred) => {
            traversal.add_step(HasWhereStep::new(key.clone(), pred.clone()))
        }
        RhaiStep::Dedup => traversal.add_step(DedupStep),
        RhaiStep::Limit(n) => traversal.add_step(LimitStep::new(*n)),
        RhaiStep::Skip(n) => traversal.add_step(SkipStep::new(*n)),
        RhaiStep::Range(start, end) => traversal.add_step(RangeStep::new(*start, *end)),
        RhaiStep::IsEq(value) => traversal.add_step(IsStep::eq(value.clone())),
        RhaiStep::Is(pred) => traversal.add_step(IsStep::new(pred.clone())),

        // Advanced filter steps (Phase 7)
        RhaiStep::Tail => traversal.add_step(TailStep::last()),
        RhaiStep::TailN(n) => traversal.add_step(TailStep::new(*n)),
        RhaiStep::Coin(probability) => traversal.add_step(CoinStep::new(*probability)),
        RhaiStep::Sample(n) => traversal.add_step(SampleStep::new(*n)),
        RhaiStep::DedupByKey(key) => traversal.add_step(DedupByKeyStep::new(key.clone())),
        RhaiStep::DedupByLabel => traversal.add_step(DedupByLabelStep::new()),
        RhaiStep::DedupBy(t) => traversal.add_step(DedupByTraversalStep::new(t.to_traversal())),
        RhaiStep::HasIds(ids) => traversal.add_step(HasIdStep::from_values(ids.clone())),
        RhaiStep::SimplePath => traversal.add_step(SimplePathStep::new()),
        RhaiStep::CyclicPath => traversal.add_step(CyclicPathStep::new()),
        RhaiStep::HasId(id) => {
            match id {
                Value::Vertex(vid) => traversal.add_step(HasIdStep::vertex(*vid)),
                Value::Edge(eid) => traversal.add_step(HasIdStep::edge(*eid)),
                Value::Int(n) => traversal.add_step(HasIdStep::vertex(VertexId(*n as u64))),
                _ => traversal, // Ignore invalid IDs
            }
        }

        // Transform
        RhaiStep::Id => traversal.add_step(IdStep),
        RhaiStep::Label => traversal.add_step(LabelStep),
        RhaiStep::Values(key) => traversal.add_step(ValuesStep::new(key.clone())),
        RhaiStep::ValuesMulti(keys) => traversal.add_step(ValuesStep::multi(keys.clone())),
        RhaiStep::ValueMap => traversal.add_step(ValueMapStep::new()),
        RhaiStep::ElementMap => traversal.add_step(ElementMapStep::new()),
        RhaiStep::Path => traversal.add_step(PathStep::new()),
        RhaiStep::Constant(value) => traversal.add_step(ConstantStep::new(value.clone())),
        RhaiStep::Identity => traversal.add_step(IdentityStep),
        RhaiStep::Unfold => traversal.add_step(UnfoldStep),
        RhaiStep::Mean => traversal.add_step(MeanStep),
        // Note: Fold, Count, Sum, Min, Max don't have step types in this codebase.
        // They are terminal operations. Skip them in anonymous traversals.
        RhaiStep::Fold | RhaiStep::Count | RhaiStep::Sum | RhaiStep::Min | RhaiStep::Max => {
            traversal
        }

        // Advanced transform steps (Phase 8)
        RhaiStep::Properties => traversal.add_step(PropertiesStep::new()),
        RhaiStep::PropertiesKeys(keys) => {
            traversal.add_step(PropertiesStep::with_keys(keys.clone()))
        }
        RhaiStep::Key => traversal.add_step(KeyStep),
        RhaiStep::PropValue => traversal.add_step(ValueStep),
        RhaiStep::ValueMapKeys(keys) => traversal.add_step(ValueMapStep::with_keys(keys.clone())),
        RhaiStep::ValueMapWithTokens => traversal.add_step(ValueMapStep::new().with_tokens()),
        RhaiStep::Index => traversal.add_step(IndexStep),
        RhaiStep::Local(sub) => {
            use crate::traversal::branch::LocalStep;
            traversal.add_step(LocalStep::new(sub.to_traversal()))
        }

        // Modulator
        RhaiStep::As(label) => traversal.add_step(AsStep::new(label.clone())),

        // Traversal-based filter steps (Phase 2)
        RhaiStep::Where(cond) => traversal.append(__::where_(cond.to_traversal())),
        RhaiStep::Not(cond) => traversal.append(__::not(cond.to_traversal())),
        RhaiStep::And(conds) => {
            let anon_traversals: Vec<_> = conds.iter().map(|t| t.to_traversal()).collect();
            traversal.append(__::and_(anon_traversals))
        }
        RhaiStep::Or(conds) => {
            let anon_traversals: Vec<_> = conds.iter().map(|t| t.to_traversal()).collect();
            traversal.append(__::or_(anon_traversals))
        }

        // Side effect steps (Phase 5)
        RhaiStep::Store(key) => traversal.add_step(StoreStep::new(key.clone())),
        RhaiStep::Aggregate(key) => traversal.add_step(AggregateStep::new(key.clone())),
        RhaiStep::Cap(key) => traversal.add_step(CapStep::new(key.clone())),
        RhaiStep::CapMulti(keys) => traversal.add_step(CapStep::multi(keys.clone())),
        RhaiStep::SideEffect(t) => traversal.add_step(SideEffectStep::new(t.to_traversal())),

        // Mutation steps (Phase 6)
        RhaiStep::AddV(label) => traversal.append(__::add_v(label.clone())),
        RhaiStep::AddE { label, from, to } => {
            use crate::traversal::mutation::{AddEStep, EdgeEndpoint};
            let mut step = AddEStep::new(label.clone());
            if let Some(endpoint) = from {
                step = match endpoint {
                    RhaiEdgeEndpoint::VertexId(id) => step.from_vertex(*id),
                    RhaiEdgeEndpoint::StepLabel(lbl) => step.from_label(lbl.clone()),
                };
            }
            if let Some(endpoint) = to {
                step = match endpoint {
                    RhaiEdgeEndpoint::VertexId(id) => step.to_vertex(*id),
                    RhaiEdgeEndpoint::StepLabel(lbl) => step.to_label(lbl.clone()),
                };
            }
            traversal.add_step(step)
        }
        RhaiStep::Property(key, value) => {
            traversal.append(__::property(key.clone(), value.clone()))
        }
        RhaiStep::Drop => traversal.append(__::drop()),

        // Branching steps (Phase 9)
        RhaiStep::Choose(cond, true_branch, false_branch) => {
            use crate::traversal::branch::ChooseStep;
            traversal.add_step(ChooseStep::new(
                cond.to_traversal(),
                true_branch.to_traversal(),
                false_branch.to_traversal(),
            ))
        }
        RhaiStep::ChooseOption(key_traversal, options, default) => {
            use crate::traversal::branch::{BranchStep, OptionKey};
            let mut step = BranchStep::new(key_traversal.to_traversal());
            for (value, t) in options {
                step = step.add_option(OptionKey::Value(value.clone()), t.to_traversal());
            }
            if let Some(default_traversal) = default {
                step = step.add_none_option(default_traversal.to_traversal());
            }
            traversal.add_step(step)
        }

        // Builder pattern steps (Phase 11)
        RhaiStep::OrderBy(key, asc) => {
            use crate::traversal::transform::order::{Order, OrderStep};
            let order = if *asc { Order::Asc } else { Order::Desc };
            traversal.add_step(OrderStep::by_property(key.clone(), order))
        }
        RhaiStep::OrderByTraversal(sub, asc) => {
            use crate::traversal::transform::order::{Order, OrderKey, OrderStep};
            let order = if *asc { Order::Asc } else { Order::Desc };
            traversal.add_step(OrderStep::with_keys(vec![OrderKey::Traversal(
                sub.to_traversal(),
                order,
            )]))
        }
        RhaiStep::Project(keys, projections) => {
            use crate::traversal::transform::{ProjectStep, Projection};
            let core_projections: Vec<Projection> = projections
                .iter()
                .map(|p| match p {
                    RhaiProjection::Key(k) => Projection::Key(k.clone()),
                    RhaiProjection::Traversal(t) => Projection::Traversal(t.to_traversal()),
                })
                .collect();
            traversal.add_step(ProjectStep::new(keys.clone(), core_projections))
        }
        RhaiStep::Group(key_selector, value_collector) => {
            use crate::traversal::aggregate::{GroupKey, GroupStep, GroupValue};
            let core_key = match key_selector {
                RhaiGroupKey::Label => GroupKey::Label,
                RhaiGroupKey::Property(k) => GroupKey::Property(k.clone()),
                RhaiGroupKey::Traversal(t) => GroupKey::Traversal(Box::new(t.to_traversal())),
            };
            let core_value = match value_collector {
                RhaiGroupValue::Identity => GroupValue::Identity,
                RhaiGroupValue::Property(k) => GroupValue::Property(k.clone()),
                RhaiGroupValue::Traversal(t) => GroupValue::Traversal(Box::new(t.to_traversal())),
            };
            traversal.add_step(GroupStep::with_selectors(core_key, core_value))
        }
        RhaiStep::GroupCount(key_selector) => {
            use crate::traversal::aggregate::{GroupCountStep, GroupKey};
            let core_key = match key_selector {
                RhaiGroupKey::Label => GroupKey::Label,
                RhaiGroupKey::Property(k) => GroupKey::Property(k.clone()),
                RhaiGroupKey::Traversal(t) => GroupKey::Traversal(Box::new(t.to_traversal())),
            };
            traversal.add_step(GroupCountStep::new(core_key))
        }
        RhaiStep::Math(expression, bindings) => {
            use crate::traversal::transform::MathStep;
            if bindings.is_empty() {
                traversal.add_step(MathStep::new(expression.clone()))
            } else {
                traversal.add_step(MathStep::with_bindings(
                    expression.clone(),
                    bindings.clone(),
                ))
            }
        }

        // Other steps - simplified handling
        _ => traversal,
    }
}

// =============================================================================
// Rhai Registration
// =============================================================================

/// Register all traversal types with the Rhai engine.
pub fn register_traversal(engine: &mut Engine) {
    // Register types
    engine.register_type_with_name::<RhaiGraph>("Graph");
    engine.register_type_with_name::<RhaiTraversalSource>("TraversalSource");
    engine.register_type_with_name::<RhaiTraversal>("Traversal");
    engine.register_type_with_name::<RhaiAnonymousTraversal>("AnonymousTraversal");

    // RhaiGraph methods - register as method that takes &mut self
    // Register both "gremlin" (new API) and "traversal" (legacy) for compatibility
    engine.register_fn("gremlin", |g: &mut RhaiGraph| g.gremlin());
    engine.register_fn("traversal", |g: &mut RhaiGraph| g.gremlin());

    // RhaiTraversalSource methods
    register_source_methods(engine);

    // RhaiTraversal methods
    register_traversal_methods(engine);

    // RhaiAnonymousTraversal factory
    register_anonymous_factory(engine);
}

fn register_source_methods(engine: &mut Engine) {
    // v() - all vertices
    engine.register_fn("v", |source: &mut RhaiTraversalSource| source.v());

    // v(id) - single vertex by ID
    engine.register_fn("v", |source: &mut RhaiTraversalSource, id: i64| {
        source.v_id(VertexId(id as u64))
    });
    engine.register_fn("v", |source: &mut RhaiTraversalSource, id: VertexId| {
        source.v_id(id)
    });

    // v_ids([ids]) - multiple vertices
    engine.register_fn(
        "v_ids",
        |source: &mut RhaiTraversalSource, ids: rhai::Array| {
            let vertex_ids: Vec<VertexId> = ids
                .into_iter()
                .filter_map(|d| {
                    if d.is::<i64>() {
                        Some(VertexId(d.cast::<i64>() as u64))
                    } else if d.is::<VertexId>() {
                        Some(d.cast::<VertexId>())
                    } else {
                        None
                    }
                })
                .collect();
            source.v_ids(vertex_ids)
        },
    );

    // e() - all edges
    engine.register_fn("e", |source: &mut RhaiTraversalSource| source.e());

    // inject([values])
    engine.register_fn(
        "inject",
        |source: &mut RhaiTraversalSource, values: rhai::Array| {
            let values: Vec<Value> = values.into_iter().map(dynamic_to_value).collect();
            source.inject(values)
        },
    );

    // Mutation source steps (Phase 6)
    engine.register_fn(
        "add_v",
        |source: &mut RhaiTraversalSource, label: ImmutableString| source.add_v(label.to_string()),
    );
    engine.register_fn(
        "add_e",
        |source: &mut RhaiTraversalSource, label: ImmutableString| source.add_e(label.to_string()),
    );
}

fn register_traversal_methods(engine: &mut Engine) {
    // Terminal steps
    engine.register_fn("to_list", |t: &mut RhaiTraversal| -> rhai::Array {
        t.to_list().into_iter().map(value_to_dynamic).collect()
    });
    engine.register_fn("list", |t: &mut RhaiTraversal| -> rhai::Array {
        t.to_list().into_iter().map(value_to_dynamic).collect()
    });
    engine.register_fn("count", |t: &mut RhaiTraversal| t.count());
    engine.register_fn("first", |t: &mut RhaiTraversal| -> Dynamic {
        t.first().map(value_to_dynamic).unwrap_or(Dynamic::UNIT)
    });
    engine.register_fn("next", |t: &mut RhaiTraversal| -> Dynamic {
        t.first().map(value_to_dynamic).unwrap_or(Dynamic::UNIT)
    });
    engine.register_fn("has_next", |t: &mut RhaiTraversal| t.has_next());
    engine.register_fn("to_set", |t: &mut RhaiTraversal| -> rhai::Array {
        t.to_set().into_iter().map(value_to_dynamic).collect()
    });
    engine.register_fn("iterate", |t: &mut RhaiTraversal| {
        t.iterate();
    });
    engine.register_fn("take", |t: &mut RhaiTraversal, n: i64| -> rhai::Array {
        t.take(n).into_iter().map(value_to_dynamic).collect()
    });

    // Path tracking
    engine.register_fn("with_path", |t: &mut RhaiTraversal| t.clone().with_path());

    // Navigation steps
    engine.register_fn("out", |t: &mut RhaiTraversal| t.clone().out());
    engine.register_fn("out", |t: &mut RhaiTraversal, label: ImmutableString| {
        t.clone().out_labels(vec![label.to_string()])
    });
    engine.register_fn("in_", |t: &mut RhaiTraversal| t.clone().in_());
    engine.register_fn("in_", |t: &mut RhaiTraversal, label: ImmutableString| {
        t.clone().in_labels(vec![label.to_string()])
    });
    engine.register_fn("both", |t: &mut RhaiTraversal| t.clone().both());
    engine.register_fn("both", |t: &mut RhaiTraversal, label: ImmutableString| {
        t.clone().both_labels(vec![label.to_string()])
    });
    engine.register_fn("out_e", |t: &mut RhaiTraversal| t.clone().out_e());
    engine.register_fn("out_e", |t: &mut RhaiTraversal, label: ImmutableString| {
        t.clone().out_e_labels(vec![label.to_string()])
    });
    engine.register_fn("in_e", |t: &mut RhaiTraversal| t.clone().in_e());
    engine.register_fn("in_e", |t: &mut RhaiTraversal, label: ImmutableString| {
        t.clone().in_e_labels(vec![label.to_string()])
    });
    engine.register_fn("both_e", |t: &mut RhaiTraversal| t.clone().both_e());
    engine.register_fn("both_e", |t: &mut RhaiTraversal, label: ImmutableString| {
        t.clone().both_e_labels(vec![label.to_string()])
    });
    engine.register_fn("out_v", |t: &mut RhaiTraversal| t.clone().out_v());
    engine.register_fn("in_v", |t: &mut RhaiTraversal| t.clone().in_v());
    engine.register_fn("other_v", |t: &mut RhaiTraversal| t.clone().other_v());
    engine.register_fn("both_v", |t: &mut RhaiTraversal| t.clone().both_v());

    // Filter steps
    engine.register_fn(
        "has_label",
        |t: &mut RhaiTraversal, label: ImmutableString| t.clone().has_label(label.to_string()),
    );
    engine.register_fn("has", |t: &mut RhaiTraversal, key: ImmutableString| {
        t.clone().has(key.to_string())
    });
    engine.register_fn("has_not", |t: &mut RhaiTraversal, key: ImmutableString| {
        t.clone().has_not(key.to_string())
    });
    engine.register_fn(
        "has_value",
        |t: &mut RhaiTraversal, key: ImmutableString, value: Dynamic| {
            t.clone()
                .has_value(key.to_string(), dynamic_to_value(value))
        },
    );
    engine.register_fn(
        "has_where",
        |t: &mut RhaiTraversal, key: ImmutableString, pred: RhaiPredicate| {
            t.clone().has_where(key.to_string(), pred)
        },
    );
    engine.register_fn("dedup", |t: &mut RhaiTraversal| t.clone().dedup());
    engine.register_fn("limit", |t: &mut RhaiTraversal, n: i64| t.clone().limit(n));
    engine.register_fn("skip", |t: &mut RhaiTraversal, n: i64| t.clone().skip(n));
    engine.register_fn("range", |t: &mut RhaiTraversal, start: i64, end: i64| {
        t.clone().range(start, end)
    });
    engine.register_fn("is_eq", |t: &mut RhaiTraversal, value: Dynamic| {
        t.clone().is_eq(dynamic_to_value(value))
    });
    engine.register_fn("is_", |t: &mut RhaiTraversal, pred: RhaiPredicate| {
        t.clone().is_(pred)
    });
    engine.register_fn("simple_path", |t: &mut RhaiTraversal| {
        t.clone().simple_path()
    });
    engine.register_fn("cyclic_path", |t: &mut RhaiTraversal| {
        t.clone().cyclic_path()
    });

    // Transform steps
    engine.register_fn("id", |t: &mut RhaiTraversal| t.clone().id());
    engine.register_fn("label", |t: &mut RhaiTraversal| t.clone().label());
    engine.register_fn("values", |t: &mut RhaiTraversal, key: ImmutableString| {
        t.clone().values(key.to_string())
    });
    engine.register_fn("value_map", |t: &mut RhaiTraversal| t.clone().value_map());
    engine.register_fn("element_map", |t: &mut RhaiTraversal| {
        t.clone().element_map()
    });
    engine.register_fn("path", |t: &mut RhaiTraversal| t.clone().path());
    engine.register_fn("constant", |t: &mut RhaiTraversal, value: Dynamic| {
        t.clone().constant(dynamic_to_value(value))
    });
    engine.register_fn("identity", |t: &mut RhaiTraversal| t.clone().identity());
    engine.register_fn("fold", |t: &mut RhaiTraversal| t.clone().fold());
    engine.register_fn("unfold", |t: &mut RhaiTraversal| t.clone().unfold());
    engine.register_fn("sum", |t: &mut RhaiTraversal| t.clone().sum());
    engine.register_fn("mean", |t: &mut RhaiTraversal| t.clone().mean());
    engine.register_fn("min", |t: &mut RhaiTraversal| t.clone().min());
    engine.register_fn("max", |t: &mut RhaiTraversal| t.clone().max());

    // Modulator steps
    engine.register_fn("as_", |t: &mut RhaiTraversal, label: ImmutableString| {
        t.clone().as_(label.to_string())
    });
    engine.register_fn("select", |t: &mut RhaiTraversal, labels: rhai::Array| {
        let labels: Vec<String> = labels
            .into_iter()
            .filter_map(|d| d.into_string().ok())
            .collect();
        t.clone().select(labels)
    });
    engine.register_fn(
        "select_one",
        |t: &mut RhaiTraversal, label: ImmutableString| t.clone().select_one(label.to_string()),
    );

    // Order steps
    engine.register_fn("order_asc", |t: &mut RhaiTraversal| t.clone().order_asc());
    engine.register_fn("order_desc", |t: &mut RhaiTraversal| t.clone().order_desc());

    // Branch steps
    engine.register_fn("union", |t: &mut RhaiTraversal, traversals: rhai::Array| {
        let anon_traversals: Vec<RhaiAnonymousTraversal> = traversals
            .into_iter()
            .filter_map(|d| d.try_cast::<RhaiAnonymousTraversal>())
            .collect();
        t.clone().union(anon_traversals)
    });
    engine.register_fn(
        "coalesce",
        |t: &mut RhaiTraversal, traversals: rhai::Array| {
            let anon_traversals: Vec<RhaiAnonymousTraversal> = traversals
                .into_iter()
                .filter_map(|d| d.try_cast::<RhaiAnonymousTraversal>())
                .collect();
            t.clone().coalesce(anon_traversals)
        },
    );
    engine.register_fn(
        "optional",
        |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal| t.clone().optional(traversal),
    );
    engine.register_fn(
        "repeat",
        |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal, times: i64| {
            t.clone().repeat_times(traversal, times)
        },
    );
    engine.register_fn(
        "repeat_until",
        |t: &mut RhaiTraversal,
         traversal: RhaiAnonymousTraversal,
         until: RhaiAnonymousTraversal| { t.clone().repeat_until(traversal, until) },
    );
    engine.register_fn(
        "repeat_emit",
        |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal, times: i64| {
            t.clone().repeat_emit(traversal, times)
        },
    );
    engine.register_fn(
        "repeat_emit_until",
        |t: &mut RhaiTraversal,
         traversal: RhaiAnonymousTraversal,
         until: RhaiAnonymousTraversal| { t.clone().repeat_emit_until(traversal, until) },
    );

    // Traversal-based filter steps (Phase 2)
    engine.register_fn(
        "where_",
        |t: &mut RhaiTraversal, cond: RhaiAnonymousTraversal| t.clone().where_(cond),
    );
    engine.register_fn(
        "not_",
        |t: &mut RhaiTraversal, cond: RhaiAnonymousTraversal| t.clone().not(cond),
    );
    engine.register_fn("and_", |t: &mut RhaiTraversal, conditions: rhai::Array| {
        let conds: Vec<RhaiAnonymousTraversal> = conditions
            .into_iter()
            .filter_map(|d| d.try_cast::<RhaiAnonymousTraversal>())
            .collect();
        t.clone().and_(conds)
    });
    engine.register_fn("or_", |t: &mut RhaiTraversal, conditions: rhai::Array| {
        let conds: Vec<RhaiAnonymousTraversal> = conditions
            .into_iter()
            .filter_map(|d| d.try_cast::<RhaiAnonymousTraversal>())
            .collect();
        t.clone().or_(conds)
    });

    // Advanced filter steps (Phase 7)
    engine.register_fn("tail", |t: &mut RhaiTraversal| t.clone().tail());
    engine.register_fn("tail_n", |t: &mut RhaiTraversal, n: i64| {
        t.clone().tail_n(n)
    });
    engine.register_fn("coin", |t: &mut RhaiTraversal, probability: f64| {
        t.clone().coin(probability)
    });
    engine.register_fn("sample", |t: &mut RhaiTraversal, n: i64| {
        t.clone().sample(n)
    });
    engine.register_fn(
        "dedup_by_key",
        |t: &mut RhaiTraversal, key: ImmutableString| t.clone().dedup_by_key(key.to_string()),
    );
    engine.register_fn("dedup_by_label", |t: &mut RhaiTraversal| {
        t.clone().dedup_by_label()
    });
    engine.register_fn(
        "dedup_by",
        |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal| t.clone().dedup_by(traversal),
    );
    engine.register_fn("has_ids", |t: &mut RhaiTraversal, ids: rhai::Array| {
        // Convert integers to VertexIds for has_ids filtering
        let values: Vec<Value> = ids
            .into_iter()
            .map(|d| {
                if d.is::<i64>() {
                    Value::Vertex(VertexId(d.cast::<i64>() as u64))
                } else if d.is::<VertexId>() {
                    Value::Vertex(d.cast::<VertexId>())
                } else {
                    dynamic_to_value(d)
                }
            })
            .collect();
        t.clone().has_ids(values)
    });

    // Advanced transform steps (Phase 8)
    engine.register_fn("properties", |t: &mut RhaiTraversal| t.clone().properties());
    engine.register_fn(
        "properties_keys",
        |t: &mut RhaiTraversal, keys: rhai::Array| {
            let keys: Vec<String> = keys
                .into_iter()
                .filter_map(|d| d.into_string().ok())
                .collect();
            t.clone().properties_keys(keys)
        },
    );
    engine.register_fn("key", |t: &mut RhaiTraversal| t.clone().key());
    engine.register_fn("value", |t: &mut RhaiTraversal| t.clone().prop_value());
    engine.register_fn(
        "value_map_keys",
        |t: &mut RhaiTraversal, keys: rhai::Array| {
            let keys: Vec<String> = keys
                .into_iter()
                .filter_map(|d| d.into_string().ok())
                .collect();
            t.clone().value_map_keys(keys)
        },
    );
    engine.register_fn("value_map_with_tokens", |t: &mut RhaiTraversal| {
        t.clone().value_map_with_tokens()
    });
    engine.register_fn("index", |t: &mut RhaiTraversal| t.clone().index());
    engine.register_fn(
        "local",
        |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal| t.clone().local(traversal),
    );

    // Side effect steps (Phase 5)
    engine.register_fn("store", |t: &mut RhaiTraversal, key: ImmutableString| {
        t.clone().store(key.to_string())
    });
    engine.register_fn(
        "aggregate",
        |t: &mut RhaiTraversal, key: ImmutableString| t.clone().aggregate(key.to_string()),
    );
    engine.register_fn("cap", |t: &mut RhaiTraversal, key: ImmutableString| {
        t.clone().cap(key.to_string())
    });
    engine.register_fn("cap_multi", |t: &mut RhaiTraversal, keys: rhai::Array| {
        let keys: Vec<String> = keys
            .into_iter()
            .filter_map(|d| d.into_string().ok())
            .collect();
        t.clone().cap_multi(keys)
    });
    engine.register_fn(
        "side_effect",
        |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal| t.clone().side_effect(traversal),
    );

    // Mutation steps (Phase 6)
    engine.register_fn("add_v", |t: &mut RhaiTraversal, label: ImmutableString| {
        t.clone().add_v(label.to_string())
    });
    engine.register_fn("add_e", |t: &mut RhaiTraversal, label: ImmutableString| {
        t.clone().add_e(label.to_string())
    });
    engine.register_fn("from_v", |t: &mut RhaiTraversal, id: i64| {
        t.clone().from_v(VertexId(id as u64))
    });
    engine.register_fn("from_v", |t: &mut RhaiTraversal, id: VertexId| {
        t.clone().from_v(id)
    });
    engine.register_fn(
        "from_label",
        |t: &mut RhaiTraversal, label: ImmutableString| t.clone().from_label(label.to_string()),
    );
    engine.register_fn("to_v", |t: &mut RhaiTraversal, id: i64| {
        t.clone().to_v(VertexId(id as u64))
    });
    engine.register_fn("to_v", |t: &mut RhaiTraversal, id: VertexId| {
        t.clone().to_v(id)
    });
    engine.register_fn(
        "to_label",
        |t: &mut RhaiTraversal, label: ImmutableString| t.clone().to_label(label.to_string()),
    );
    engine.register_fn(
        "property",
        |t: &mut RhaiTraversal, key: ImmutableString, value: Dynamic| {
            t.clone().property(key.to_string(), dynamic_to_value(value))
        },
    );
    engine.register_fn("drop", |t: &mut RhaiTraversal| t.clone().drop_());

    // Branching steps (Phase 9)
    engine.register_fn(
        "choose_binary",
        |t: &mut RhaiTraversal,
         condition: RhaiAnonymousTraversal,
         true_branch: RhaiAnonymousTraversal,
         false_branch: RhaiAnonymousTraversal| {
            t.clone()
                .choose_binary(condition, true_branch, false_branch)
        },
    );
    engine.register_fn(
        "choose",
        |t: &mut RhaiTraversal,
         condition: RhaiAnonymousTraversal,
         true_branch: RhaiAnonymousTraversal,
         false_branch: RhaiAnonymousTraversal| {
            t.clone()
                .choose_binary(condition, true_branch, false_branch)
        },
    );
    engine.register_fn(
        "choose_options",
        |t: &mut RhaiTraversal, key_traversal: RhaiAnonymousTraversal, options: rhai::Map| {
            let mut option_vec: Vec<(Value, RhaiAnonymousTraversal)> = Vec::new();
            let mut default: Option<RhaiAnonymousTraversal> = None;

            for (key, value) in options {
                let key_str = key.to_string();
                if key_str == "_default" || key_str == "none" {
                    // Special key for default branch
                    if let Some(traversal) = value.try_cast::<RhaiAnonymousTraversal>() {
                        default = Some(traversal);
                    }
                } else if let Some(traversal) = value.try_cast::<RhaiAnonymousTraversal>() {
                    // Convert key to Value
                    let option_key = Value::String(key_str);
                    option_vec.push((option_key, traversal));
                }
            }

            t.clone().choose_options(key_traversal, option_vec, default)
        },
    );

    // Builder pattern steps (Phase 11)
    engine.register_fn("order_by", |t: &mut RhaiTraversal, key: ImmutableString| {
        t.clone().order_by(key.to_string())
    });
    engine.register_fn(
        "order_by_desc",
        |t: &mut RhaiTraversal, key: ImmutableString| t.clone().order_by_desc(key.to_string()),
    );
    engine.register_fn(
        "order_by_traversal",
        |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal| {
            t.clone().order_by_traversal(traversal)
        },
    );
    engine.register_fn(
        "order_by_traversal_desc",
        |t: &mut RhaiTraversal, traversal: RhaiAnonymousTraversal| {
            t.clone().order_by_traversal_desc(traversal)
        },
    );
    engine.register_fn("group_by_label", |t: &mut RhaiTraversal| {
        t.clone().group_by_label()
    });
    engine.register_fn(
        "group_by_key",
        |t: &mut RhaiTraversal, key: ImmutableString| t.clone().group_by_key(key.to_string()),
    );
    engine.register_fn("group_count_by_label", |t: &mut RhaiTraversal| {
        t.clone().group_count_by_label()
    });
    engine.register_fn(
        "group_count_by_key",
        |t: &mut RhaiTraversal, key: ImmutableString| t.clone().group_count_by_key(key.to_string()),
    );
    engine.register_fn(
        "math",
        |t: &mut RhaiTraversal, expression: ImmutableString| t.clone().math(expression.to_string()),
    );
    engine.register_fn(
        "math_with_bindings",
        |t: &mut RhaiTraversal, expression: ImmutableString, bindings: rhai::Map| {
            let bindings_map: HashMap<String, String> = bindings
                .into_iter()
                .filter_map(|(k, v)| v.into_string().ok().map(|s| (k.to_string(), s)))
                .collect();
            t.clone()
                .math_with_bindings(expression.to_string(), bindings_map)
        },
    );
}

fn register_anonymous_factory(engine: &mut Engine) {
    // Factory function
    engine.register_fn("anon", RhaiAnonymousTraversal::new);

    // Navigation
    engine.register_fn("out", |a: &mut RhaiAnonymousTraversal| a.clone().out());
    engine.register_fn(
        "out",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
            a.clone().out_labels(vec![label.to_string()])
        },
    );
    engine.register_fn("in_", |a: &mut RhaiAnonymousTraversal| a.clone().in_());
    engine.register_fn(
        "in_",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
            a.clone().in_labels(vec![label.to_string()])
        },
    );
    engine.register_fn("both", |a: &mut RhaiAnonymousTraversal| a.clone().both());
    engine.register_fn("out_e", |a: &mut RhaiAnonymousTraversal| a.clone().out_e());
    engine.register_fn("in_e", |a: &mut RhaiAnonymousTraversal| a.clone().in_e());
    engine.register_fn("both_e", |a: &mut RhaiAnonymousTraversal| {
        a.clone().both_e()
    });
    engine.register_fn("out_v", |a: &mut RhaiAnonymousTraversal| a.clone().out_v());
    engine.register_fn("in_v", |a: &mut RhaiAnonymousTraversal| a.clone().in_v());
    engine.register_fn("other_v", |a: &mut RhaiAnonymousTraversal| {
        a.clone().other_v()
    });
    engine.register_fn("both_v", |a: &mut RhaiAnonymousTraversal| {
        a.clone().both_v()
    });

    // Filter
    engine.register_fn(
        "has_label",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
            a.clone().has_label(label.to_string())
        },
    );
    engine.register_fn(
        "has",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| a.clone().has(key.to_string()),
    );
    engine.register_fn(
        "has_value",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString, value: Dynamic| {
            a.clone()
                .has_value(key.to_string(), dynamic_to_value(value))
        },
    );
    engine.register_fn("dedup", |a: &mut RhaiAnonymousTraversal| a.clone().dedup());
    engine.register_fn("limit", |a: &mut RhaiAnonymousTraversal, n: i64| {
        a.clone().limit(n)
    });

    // Transform
    engine.register_fn("id", |a: &mut RhaiAnonymousTraversal| a.clone().id());
    engine.register_fn("label", |a: &mut RhaiAnonymousTraversal| a.clone().label());
    engine.register_fn(
        "values",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| a.clone().values(key.to_string()),
    );
    engine.register_fn("value_map", |a: &mut RhaiAnonymousTraversal| {
        a.clone().value_map()
    });
    engine.register_fn("path", |a: &mut RhaiAnonymousTraversal| a.clone().path());
    engine.register_fn(
        "constant",
        |a: &mut RhaiAnonymousTraversal, value: Dynamic| {
            a.clone().constant(dynamic_to_value(value))
        },
    );
    engine.register_fn("identity", |a: &mut RhaiAnonymousTraversal| {
        a.clone().identity()
    });
    engine.register_fn("fold", |a: &mut RhaiAnonymousTraversal| a.clone().fold());
    engine.register_fn("unfold", |a: &mut RhaiAnonymousTraversal| {
        a.clone().unfold()
    });

    // Modulator
    engine.register_fn(
        "as_",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| a.clone().as_(label.to_string()),
    );

    // === Phase 1: Additional registrations for parity ===

    // Navigation with labels
    engine.register_fn(
        "both",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
            a.clone().both_labels(vec![label.to_string()])
        },
    );
    engine.register_fn(
        "out_e",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
            a.clone().out_e_labels(vec![label.to_string()])
        },
    );
    engine.register_fn(
        "in_e",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
            a.clone().in_e_labels(vec![label.to_string()])
        },
    );
    engine.register_fn(
        "both_e",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
            a.clone().both_e_labels(vec![label.to_string()])
        },
    );

    // Additional filter steps
    engine.register_fn(
        "has_not",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| a.clone().has_not(key.to_string()),
    );
    engine.register_fn(
        "has_where",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString, pred: RhaiPredicate| {
            a.clone().has_where(key.to_string(), pred)
        },
    );
    engine.register_fn("has_id", |a: &mut RhaiAnonymousTraversal, id: i64| {
        a.clone().has_id(VertexId(id as u64))
    });
    engine.register_fn(
        "has_label_any",
        |a: &mut RhaiAnonymousTraversal, labels: rhai::Array| {
            let labels: Vec<String> = labels
                .into_iter()
                .filter_map(|d| d.into_string().ok())
                .collect();
            a.clone().has_label_any(labels)
        },
    );
    engine.register_fn("skip", |a: &mut RhaiAnonymousTraversal, n: i64| {
        a.clone().skip(n)
    });
    engine.register_fn(
        "range",
        |a: &mut RhaiAnonymousTraversal, start: i64, end: i64| a.clone().range(start, end),
    );
    engine.register_fn(
        "is_",
        |a: &mut RhaiAnonymousTraversal, pred: RhaiPredicate| a.clone().is_(pred),
    );
    engine.register_fn("is_eq", |a: &mut RhaiAnonymousTraversal, value: Dynamic| {
        a.clone().is_eq(dynamic_to_value(value))
    });
    engine.register_fn("simple_path", |a: &mut RhaiAnonymousTraversal| {
        a.clone().simple_path()
    });
    engine.register_fn("cyclic_path", |a: &mut RhaiAnonymousTraversal| {
        a.clone().cyclic_path()
    });

    // Additional transform steps
    engine.register_fn("element_map", |a: &mut RhaiAnonymousTraversal| {
        a.clone().element_map()
    });
    engine.register_fn(
        "values_multi",
        |a: &mut RhaiAnonymousTraversal, keys: rhai::Array| {
            let keys: Vec<String> = keys
                .into_iter()
                .filter_map(|d| d.into_string().ok())
                .collect();
            a.clone().values_multi(keys)
        },
    );
    engine.register_fn("sum", |a: &mut RhaiAnonymousTraversal| a.clone().sum());
    engine.register_fn("mean", |a: &mut RhaiAnonymousTraversal| a.clone().mean());
    engine.register_fn("min", |a: &mut RhaiAnonymousTraversal| a.clone().min());
    engine.register_fn("max", |a: &mut RhaiAnonymousTraversal| a.clone().max());
    engine.register_fn("count", |a: &mut RhaiAnonymousTraversal| a.clone().count());

    // Additional modulator steps
    engine.register_fn(
        "select",
        |a: &mut RhaiAnonymousTraversal, labels: rhai::Array| {
            let labels: Vec<String> = labels
                .into_iter()
                .filter_map(|d| d.into_string().ok())
                .collect();
            a.clone().select(labels)
        },
    );
    engine.register_fn(
        "select_one",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
            a.clone().select_one(label.to_string())
        },
    );
    engine.register_fn("order_asc", |a: &mut RhaiAnonymousTraversal| {
        a.clone().order_asc()
    });
    engine.register_fn("order_desc", |a: &mut RhaiAnonymousTraversal| {
        a.clone().order_desc()
    });

    // Traversal-based filter steps (Phase 2)
    engine.register_fn(
        "where_",
        |a: &mut RhaiAnonymousTraversal, cond: RhaiAnonymousTraversal| a.clone().where_(cond),
    );
    engine.register_fn(
        "not_",
        |a: &mut RhaiAnonymousTraversal, cond: RhaiAnonymousTraversal| a.clone().not(cond),
    );
    engine.register_fn(
        "and_",
        |a: &mut RhaiAnonymousTraversal, conditions: rhai::Array| {
            let conds: Vec<RhaiAnonymousTraversal> = conditions
                .into_iter()
                .filter_map(|d| d.try_cast::<RhaiAnonymousTraversal>())
                .collect();
            a.clone().and_(conds)
        },
    );
    engine.register_fn(
        "or_",
        |a: &mut RhaiAnonymousTraversal, conditions: rhai::Array| {
            let conds: Vec<RhaiAnonymousTraversal> = conditions
                .into_iter()
                .filter_map(|d| d.try_cast::<RhaiAnonymousTraversal>())
                .collect();
            a.clone().or_(conds)
        },
    );

    // Side effect steps (Phase 5)
    engine.register_fn(
        "store",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| a.clone().store(key.to_string()),
    );
    engine.register_fn(
        "aggregate",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| a.clone().aggregate(key.to_string()),
    );
    engine.register_fn(
        "cap",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| a.clone().cap(key.to_string()),
    );
    engine.register_fn(
        "cap_multi",
        |a: &mut RhaiAnonymousTraversal, keys: rhai::Array| {
            let keys: Vec<String> = keys
                .into_iter()
                .filter_map(|d| d.into_string().ok())
                .collect();
            a.clone().cap_multi(keys)
        },
    );
    engine.register_fn(
        "side_effect",
        |a: &mut RhaiAnonymousTraversal, traversal: RhaiAnonymousTraversal| {
            a.clone().side_effect(traversal)
        },
    );

    // Mutation steps (Phase 6)
    engine.register_fn(
        "add_v",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| a.clone().add_v(label.to_string()),
    );
    engine.register_fn(
        "add_e",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| a.clone().add_e(label.to_string()),
    );
    engine.register_fn("from_v", |a: &mut RhaiAnonymousTraversal, id: i64| {
        a.clone().from_v(VertexId(id as u64))
    });
    engine.register_fn("from_v", |a: &mut RhaiAnonymousTraversal, id: VertexId| {
        a.clone().from_v(id)
    });
    engine.register_fn(
        "from_label",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
            a.clone().from_label(label.to_string())
        },
    );
    engine.register_fn("to_v", |a: &mut RhaiAnonymousTraversal, id: i64| {
        a.clone().to_v(VertexId(id as u64))
    });
    engine.register_fn("to_v", |a: &mut RhaiAnonymousTraversal, id: VertexId| {
        a.clone().to_v(id)
    });
    engine.register_fn(
        "to_label",
        |a: &mut RhaiAnonymousTraversal, label: ImmutableString| {
            a.clone().to_label(label.to_string())
        },
    );
    engine.register_fn(
        "property",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString, value: Dynamic| {
            a.clone().property(key.to_string(), dynamic_to_value(value))
        },
    );
    engine.register_fn("drop", |a: &mut RhaiAnonymousTraversal| a.clone().drop_());

    // Advanced filter steps (Phase 7)
    engine.register_fn("tail", |a: &mut RhaiAnonymousTraversal| a.clone().tail());
    engine.register_fn("tail_n", |a: &mut RhaiAnonymousTraversal, n: i64| {
        a.clone().tail_n(n)
    });
    engine.register_fn(
        "coin",
        |a: &mut RhaiAnonymousTraversal, probability: f64| a.clone().coin(probability),
    );
    engine.register_fn("sample", |a: &mut RhaiAnonymousTraversal, n: i64| {
        a.clone().sample(n)
    });
    engine.register_fn(
        "dedup_by_key",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| {
            a.clone().dedup_by_key(key.to_string())
        },
    );
    engine.register_fn("dedup_by_label", |a: &mut RhaiAnonymousTraversal| {
        a.clone().dedup_by_label()
    });
    engine.register_fn(
        "dedup_by",
        |a: &mut RhaiAnonymousTraversal, traversal: RhaiAnonymousTraversal| {
            a.clone().dedup_by(traversal)
        },
    );
    engine.register_fn(
        "has_ids",
        |a: &mut RhaiAnonymousTraversal, ids: rhai::Array| {
            // Convert integers to VertexIds for has_ids filtering
            let values: Vec<Value> = ids
                .into_iter()
                .map(|d| {
                    if d.is::<i64>() {
                        Value::Vertex(VertexId(d.cast::<i64>() as u64))
                    } else if d.is::<VertexId>() {
                        Value::Vertex(d.cast::<VertexId>())
                    } else {
                        dynamic_to_value(d)
                    }
                })
                .collect();
            a.clone().has_ids(values)
        },
    );

    // Advanced transform steps (Phase 8)
    engine.register_fn("properties", |a: &mut RhaiAnonymousTraversal| {
        a.clone().properties()
    });
    engine.register_fn(
        "properties_keys",
        |a: &mut RhaiAnonymousTraversal, keys: rhai::Array| {
            let keys: Vec<String> = keys
                .into_iter()
                .filter_map(|d| d.into_string().ok())
                .collect();
            a.clone().properties_keys(keys)
        },
    );
    engine.register_fn("key", |a: &mut RhaiAnonymousTraversal| a.clone().key());
    engine.register_fn("value", |a: &mut RhaiAnonymousTraversal| {
        a.clone().prop_value()
    });
    engine.register_fn(
        "value_map_keys",
        |a: &mut RhaiAnonymousTraversal, keys: rhai::Array| {
            let keys: Vec<String> = keys
                .into_iter()
                .filter_map(|d| d.into_string().ok())
                .collect();
            a.clone().value_map_keys(keys)
        },
    );
    engine.register_fn("value_map_with_tokens", |a: &mut RhaiAnonymousTraversal| {
        a.clone().value_map_with_tokens()
    });
    engine.register_fn("index", |a: &mut RhaiAnonymousTraversal| a.clone().index());
    engine.register_fn(
        "local",
        |a: &mut RhaiAnonymousTraversal, traversal: RhaiAnonymousTraversal| {
            a.clone().local(traversal)
        },
    );

    // Branching steps (Phase 9)
    engine.register_fn(
        "choose_binary",
        |a: &mut RhaiAnonymousTraversal,
         condition: RhaiAnonymousTraversal,
         true_branch: RhaiAnonymousTraversal,
         false_branch: RhaiAnonymousTraversal| {
            a.clone()
                .choose_binary(condition, true_branch, false_branch)
        },
    );
    engine.register_fn(
        "choose",
        |a: &mut RhaiAnonymousTraversal,
         condition: RhaiAnonymousTraversal,
         true_branch: RhaiAnonymousTraversal,
         false_branch: RhaiAnonymousTraversal| {
            a.clone()
                .choose_binary(condition, true_branch, false_branch)
        },
    );
    engine.register_fn(
        "choose_options",
        |a: &mut RhaiAnonymousTraversal,
         key_traversal: RhaiAnonymousTraversal,
         options: rhai::Map| {
            let mut option_vec: Vec<(Value, RhaiAnonymousTraversal)> = Vec::new();
            let mut default: Option<RhaiAnonymousTraversal> = None;

            for (key, value) in options {
                let key_str = key.to_string();
                if key_str == "_default" || key_str == "none" {
                    // Special key for default branch
                    if let Some(traversal) = value.try_cast::<RhaiAnonymousTraversal>() {
                        default = Some(traversal);
                    }
                } else if let Some(traversal) = value.try_cast::<RhaiAnonymousTraversal>() {
                    // Convert key to Value
                    let option_key = Value::String(key_str);
                    option_vec.push((option_key, traversal));
                }
            }

            a.clone().choose_options(key_traversal, option_vec, default)
        },
    );

    // Builder pattern steps (Phase 11)
    engine.register_fn(
        "order_by",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| a.clone().order_by(key.to_string()),
    );
    engine.register_fn(
        "order_by_desc",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| {
            a.clone().order_by_desc(key.to_string())
        },
    );
    engine.register_fn(
        "order_by_traversal",
        |a: &mut RhaiAnonymousTraversal, traversal: RhaiAnonymousTraversal| {
            a.clone().order_by_traversal(traversal)
        },
    );
    engine.register_fn(
        "order_by_traversal_desc",
        |a: &mut RhaiAnonymousTraversal, traversal: RhaiAnonymousTraversal| {
            a.clone().order_by_traversal_desc(traversal)
        },
    );
    engine.register_fn("group_by_label", |a: &mut RhaiAnonymousTraversal| {
        a.clone().group_by_label()
    });
    engine.register_fn(
        "group_by_key",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| {
            a.clone().group_by_key(key.to_string())
        },
    );
    engine.register_fn("group_count_by_label", |a: &mut RhaiAnonymousTraversal| {
        a.clone().group_count_by_label()
    });
    engine.register_fn(
        "group_count_by_key",
        |a: &mut RhaiAnonymousTraversal, key: ImmutableString| {
            a.clone().group_count_by_key(key.to_string())
        },
    );
    engine.register_fn(
        "math",
        |a: &mut RhaiAnonymousTraversal, expression: ImmutableString| {
            a.clone().math(expression.to_string())
        },
    );
    engine.register_fn(
        "math_with_bindings",
        |a: &mut RhaiAnonymousTraversal, expression: ImmutableString, bindings: rhai::Map| {
            let bindings_map: HashMap<String, String> = bindings
                .into_iter()
                .filter_map(|(k, v)| v.into_string().ok().map(|s| (k.to_string(), s)))
                .collect();
            a.clone()
                .math_with_bindings(expression.to_string(), bindings_map)
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_graph() -> RhaiGraph {
        let graph = Graph::new();

        let alice = graph.add_vertex(
            "person",
            [
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
            ]
            .into_iter()
            .collect(),
        );

        let bob = graph.add_vertex(
            "person",
            [
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::Int(25)),
            ]
            .into_iter()
            .collect(),
        );

        let carol = graph.add_vertex(
            "person",
            [
                ("name".to_string(), Value::String("Carol".to_string())),
                ("age".to_string(), Value::Int(35)),
            ]
            .into_iter()
            .collect(),
        );

        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        graph
            .add_edge(alice, carol, "knows", HashMap::new())
            .unwrap();
        graph.add_edge(bob, carol, "knows", HashMap::new()).unwrap();

        RhaiGraph::from_arc(Arc::new(graph))
    }

    #[test]
    fn test_basic_traversal() {
        let rhai_graph = create_test_graph();
        let g = rhai_graph.gremlin();

        // Count all vertices
        let count = g.v().count();
        assert_eq!(count, 3);

        // Count all edges
        let count = g.e().count();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_filter_steps() {
        let rhai_graph = create_test_graph();
        let g = rhai_graph.gremlin();

        // Filter by label
        let people = g.v().has_label("person".to_string()).to_list();
        assert_eq!(people.len(), 3);

        // Filter by property value
        let alice = g
            .v()
            .has_value("name".to_string(), Value::String("Alice".to_string()))
            .to_list();
        assert_eq!(alice.len(), 1);
    }

    #[test]
    fn test_navigation_steps() {
        let rhai_graph = create_test_graph();
        let g = rhai_graph.gremlin();

        // Alice knows 2 people
        let alice_knows = g
            .v()
            .has_value("name".to_string(), Value::String("Alice".to_string()))
            .out()
            .to_list();
        assert_eq!(alice_knows.len(), 2);
    }

    #[test]
    fn test_transform_steps() {
        let rhai_graph = create_test_graph();
        let g = rhai_graph.gremlin();

        // Get all names
        let names = g.v().values("name".to_string()).to_list();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&Value::String("Alice".to_string())));
        assert!(names.contains(&Value::String("Bob".to_string())));
        assert!(names.contains(&Value::String("Carol".to_string())));
    }

    #[test]
    fn test_predicate_filter() {
        let rhai_graph = create_test_graph();
        let g = rhai_graph.gremlin();

        // Filter by age >= 30
        let pred = RhaiPredicate::new(crate::traversal::p::gte(30i64));
        let adults = g.v().has_where("age".to_string(), pred).to_list();
        assert_eq!(adults.len(), 2); // Alice (30) and Carol (35)
    }

    #[test]
    fn test_engine_registration() {
        let mut engine = Engine::new();
        super::super::types::register_types(&mut engine);
        super::super::predicates::register_predicates(&mut engine);
        register_traversal(&mut engine);

        // Create a scope with the graph
        let rhai_graph = create_test_graph();
        let mut scope = rhai::Scope::new();
        scope.push("graph", rhai_graph);

        // Execute a simple script
        let result: i64 = engine
            .eval_with_scope(
                &mut scope,
                r#"
                let g = graph.gremlin();
                g.v().count()
            "#,
            )
            .unwrap();

        assert_eq!(result, 3);
    }

    #[test]
    fn test_script_filter() {
        let mut engine = Engine::new();
        super::super::types::register_types(&mut engine);
        super::super::predicates::register_predicates(&mut engine);
        register_traversal(&mut engine);

        let rhai_graph = create_test_graph();
        let mut scope = rhai::Scope::new();
        scope.push("graph", rhai_graph);

        // Filter by predicate
        let result: i64 = engine
            .eval_with_scope(
                &mut scope,
                r#"
                let g = graph.gremlin();
                g.v().has_where("age", gte(30)).count()
            "#,
            )
            .unwrap();

        assert_eq!(result, 2);
    }

    #[test]
    fn test_script_navigation() {
        let mut engine = Engine::new();
        super::super::types::register_types(&mut engine);
        super::super::predicates::register_predicates(&mut engine);
        register_traversal(&mut engine);

        let rhai_graph = create_test_graph();
        let mut scope = rhai::Scope::new();
        scope.push("graph", rhai_graph);

        // Navigation
        let result: i64 = engine
            .eval_with_scope(
                &mut scope,
                r#"
                let g = graph.gremlin();
                g.v().has_value("name", "Alice").out().count()
            "#,
            )
            .unwrap();

        assert_eq!(result, 2);
    }
}
