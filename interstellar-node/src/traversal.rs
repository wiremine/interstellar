//! Traversal facade for napi-rs bindings.
//!
//! Provides a JavaScript-friendly traversal API with method chaining.

use std::collections::HashMap;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi::JsUnknown;
use napi_derive::napi;

use interstellar::storage::cow::Graph as InnerGraph;
use interstellar::traversal::context::SnapshotLike;
use interstellar::traversal::step::Step;
use interstellar::traversal::{self, DynStep, ExecutionContext, Traverser};
use interstellar::value::{EdgeId, Value, VertexId};

#[cfg(feature = "mmap")]
use interstellar::storage::cow_mmap::CowMmapGraph;

use crate::predicate::JsPredicate;
use crate::value::{js_array_to_strings, js_to_value, value_to_js, values_to_js_array};

// ============================================================================
// Graph Backend Abstraction
// ============================================================================

/// Enum wrapper for different graph backends.
/// This allows JsTraversal to work with both in-memory and mmap-backed graphs.
#[derive(Clone)]
pub(crate) enum GraphBackend {
    InMemory(Arc<InnerGraph>),
    #[cfg(feature = "mmap")]
    Mmap(Arc<CowMmapGraph>),
}

impl GraphBackend {
    /// Execute a function with a snapshot of the graph.
    pub(crate) fn with_snapshot<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&dyn SnapshotLike) -> R,
    {
        match self {
            GraphBackend::InMemory(graph) => {
                let snapshot = graph.snapshot();
                f(&snapshot)
            }
            #[cfg(feature = "mmap")]
            GraphBackend::Mmap(graph) => {
                let snapshot = graph.snapshot();
                f(&snapshot)
            }
        }
    }

    /// Add a vertex to the graph.
    pub(crate) fn add_vertex(&self, label: &str, properties: HashMap<String, Value>) -> VertexId {
        match self {
            GraphBackend::InMemory(graph) => graph.add_vertex(label, properties),
            #[cfg(feature = "mmap")]
            GraphBackend::Mmap(graph) => graph
                .add_vertex(label, properties)
                .expect("add_vertex failed on mmap graph"),
        }
    }

    /// Add an edge to the graph.
    pub(crate) fn add_edge(
        &self,
        from: VertexId,
        to: VertexId,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> std::result::Result<EdgeId, interstellar::error::StorageError> {
        match self {
            GraphBackend::InMemory(graph) => graph.add_edge(from, to, label, properties),
            #[cfg(feature = "mmap")]
            GraphBackend::Mmap(graph) => graph.add_edge(from, to, label, properties),
        }
    }

    /// Set a property on a vertex.
    pub(crate) fn set_vertex_property(
        &self,
        id: VertexId,
        key: &str,
        value: Value,
    ) -> std::result::Result<(), interstellar::error::StorageError> {
        match self {
            GraphBackend::InMemory(graph) => graph.set_vertex_property(id, key, value),
            #[cfg(feature = "mmap")]
            GraphBackend::Mmap(graph) => graph.set_vertex_property(id, key, value),
        }
    }

    /// Remove a vertex.
    pub(crate) fn remove_vertex(
        &self,
        id: VertexId,
    ) -> std::result::Result<(), interstellar::error::StorageError> {
        match self {
            GraphBackend::InMemory(graph) => graph.remove_vertex(id),
            #[cfg(feature = "mmap")]
            GraphBackend::Mmap(graph) => graph.remove_vertex(id),
        }
    }

    /// Remove an edge.
    pub(crate) fn remove_edge(
        &self,
        id: EdgeId,
    ) -> std::result::Result<(), interstellar::error::StorageError> {
        match self {
            GraphBackend::InMemory(graph) => graph.remove_edge(id),
            #[cfg(feature = "mmap")]
            GraphBackend::Mmap(graph) => graph.remove_edge(id),
        }
    }

    /// Set a property on an edge.
    pub(crate) fn set_edge_property(
        &self,
        id: EdgeId,
        key: &str,
        value: Value,
    ) -> std::result::Result<(), interstellar::error::StorageError> {
        match self {
            GraphBackend::InMemory(graph) => graph.set_edge_property(id, key, value),
            #[cfg(feature = "mmap")]
            GraphBackend::Mmap(graph) => graph.set_edge_property(id, key, value),
        }
    }

    /// Get the number of vertices in the graph.
    pub(crate) fn vertex_count(&self) -> u64 {
        self.with_snapshot(|snapshot| snapshot.storage().vertex_count())
    }

    /// Get the number of edges in the graph.
    pub(crate) fn edge_count(&self) -> u64 {
        self.with_snapshot(|snapshot| snapshot.storage().edge_count())
    }

    /// Get the current version/transaction ID.
    pub(crate) fn version(&self) -> u64 {
        match self {
            GraphBackend::InMemory(graph) => graph.version(),
            #[cfg(feature = "mmap")]
            GraphBackend::Mmap(graph) => graph.version(),
        }
    }

    /// Import graph data from GraphSON JSON string.
    #[cfg(feature = "graphson")]
    pub(crate) fn from_graphson(
        &self,
        json: &str,
    ) -> std::result::Result<(), interstellar::graphson::GraphsonError> {
        match self {
            GraphBackend::InMemory(graph) => interstellar::graphson::from_graphson_str(graph, json),
            #[cfg(feature = "mmap")]
            GraphBackend::Mmap(graph) => interstellar::graphson::from_graphson_str(graph, json),
        }
    }
}

// ============================================================================
// Local Step Implementations (not exported from core library)
// ============================================================================

/// Step that collects all input elements into a single list.
#[derive(Clone, Debug, Default)]
pub(crate) struct FoldStep;

impl FoldStep {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl Step for FoldStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let values: Vec<Value> = input.map(|t| t.value).collect();
        std::iter::once(Traverser::new(Value::List(values)))
    }

    fn name(&self) -> &'static str {
        "fold"
    }

    fn apply_streaming(
        &self,
        _ctx: interstellar::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // BARRIER STEP: Cannot stream - must collect all values
        Box::new(std::iter::once(input))
    }
}

/// Step that calculates the sum of numeric values.
#[derive(Clone, Debug, Default)]
pub(crate) struct SumStep;

impl SumStep {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl Step for SumStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let mut sum = 0i64;
        let mut has_float = false;
        let mut float_sum = 0.0f64;
        let mut has_values = false;

        for t in input {
            match &t.value {
                Value::Int(n) => {
                    sum += n;
                    float_sum += *n as f64;
                    has_values = true;
                }
                Value::Float(f) => {
                    float_sum += f;
                    has_float = true;
                    has_values = true;
                }
                _ => {}
            }
        }

        if !has_values {
            None.into_iter()
        } else if has_float {
            Some(Traverser::new(Value::Float(float_sum))).into_iter()
        } else {
            Some(Traverser::new(Value::Int(sum))).into_iter()
        }
    }

    fn name(&self) -> &'static str {
        "sum"
    }

    fn apply_streaming(
        &self,
        _ctx: interstellar::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // BARRIER STEP: Cannot stream - must collect all values
        Box::new(std::iter::once(input))
    }
}

/// Step that counts the number of traversers.
#[derive(Clone, Debug, Default)]
pub(crate) struct CountStep;

impl CountStep {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl Step for CountStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let count = input.count() as i64;
        std::iter::once(Traverser::new(Value::Int(count)))
    }

    fn name(&self) -> &'static str {
        "count"
    }

    fn apply_streaming(
        &self,
        _ctx: interstellar::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // BARRIER STEP: Cannot stream - must count all inputs
        Box::new(std::iter::once(input))
    }
}

// ============================================================================
// Traversal Types
// ============================================================================

/// The type of elements in the traversal stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TraversalType {
    Vertex,
    Edge,
    Value,
}

/// Source type for initial traversal start.
#[derive(Clone)]
#[allow(dead_code)] // Injected variant reserved for future inject() step
pub(crate) enum TraversalSource {
    AllVertices,
    VertexIds(Vec<VertexId>),
    AllEdges,
    EdgeIds(Vec<EdgeId>),
    Injected(Vec<Value>),
    /// Anonymous traversal - no source, just steps (used with branch steps)
    Anonymous,
}

/// A graph traversal that can be chained with various steps.
///
/// Traversals are lazy - they only execute when a terminal step is called.
#[napi(js_name = "Traversal")]
pub struct JsTraversal {
    /// The graph this traversal operates on (supports both in-memory and mmap)
    pub(crate) graph: GraphBackend,
    /// Source of initial traversers
    pub(crate) source: TraversalSource,
    /// Accumulated steps (type-erased)
    pub(crate) steps: Vec<Box<dyn DynStep>>,
    /// Current output type
    pub(crate) output_type: TraversalType,
}

impl Clone for JsTraversal {
    fn clone(&self) -> Self {
        Self {
            graph: self.graph.clone(),
            source: self.source.clone(),
            steps: self.steps.iter().map(|s| s.clone_box()).collect(),
            output_type: self.output_type,
        }
    }
}

impl JsTraversal {
    // =========================================================================
    // GraphBackend-based constructors (unified API)
    // =========================================================================

    pub(crate) fn from_all_vertices_backend(backend: GraphBackend) -> Self {
        Self {
            graph: backend,
            source: TraversalSource::AllVertices,
            steps: Vec::new(),
            output_type: TraversalType::Vertex,
        }
    }

    pub(crate) fn from_vertex_ids_backend(backend: GraphBackend, ids: Vec<VertexId>) -> Self {
        Self {
            graph: backend,
            source: TraversalSource::VertexIds(ids),
            steps: Vec::new(),
            output_type: TraversalType::Vertex,
        }
    }

    pub(crate) fn from_all_edges_backend(backend: GraphBackend) -> Self {
        Self {
            graph: backend,
            source: TraversalSource::AllEdges,
            steps: Vec::new(),
            output_type: TraversalType::Edge,
        }
    }

    pub(crate) fn from_edge_ids_backend(backend: GraphBackend, ids: Vec<EdgeId>) -> Self {
        Self {
            graph: backend,
            source: TraversalSource::EdgeIds(ids),
            steps: Vec::new(),
            output_type: TraversalType::Edge,
        }
    }

    #[allow(dead_code)] // Reserved for future inject() step
    pub(crate) fn from_injected_values_backend(backend: GraphBackend, values: Vec<Value>) -> Self {
        Self {
            graph: backend,
            source: TraversalSource::Injected(values),
            steps: Vec::new(),
            output_type: TraversalType::Value,
        }
    }

    /// Create an anonymous traversal with a single step.
    pub(crate) fn anonymous_with_step<S: DynStep + 'static>(step: S) -> Self {
        Self {
            graph: GraphBackend::InMemory(Arc::new(InnerGraph::new())),
            source: TraversalSource::Anonymous,
            steps: vec![Box::new(step)],
            output_type: TraversalType::Value,
        }
    }

    /// Create an anonymous traversal (no source, empty steps).
    #[allow(dead_code)] // Reserved for anonymous traversal factory
    pub(crate) fn anonymous() -> Self {
        Self {
            graph: GraphBackend::InMemory(Arc::new(InnerGraph::new())),
            source: TraversalSource::Anonymous,
            steps: Vec::new(),
            output_type: TraversalType::Value,
        }
    }

    /// Check if this is an anonymous traversal.
    pub(crate) fn is_anonymous(&self) -> bool {
        matches!(self.source, TraversalSource::Anonymous)
    }

    /// Add a step to the traversal pipeline.
    fn add_step<S: DynStep + 'static>(mut self, step: S) -> Self {
        self.steps.push(Box::new(step));
        self
    }

    /// Add a step and change output type.
    fn add_step_with_type<S: DynStep + 'static>(
        mut self,
        step: S,
        output_type: TraversalType,
    ) -> Self {
        self.steps.push(Box::new(step));
        self.output_type = output_type;
        self
    }

    /// Add a step to the traversal pipeline (pub(crate) for anonymous factory).
    #[allow(dead_code)] // May be used by anonymous factory
    pub(crate) fn add_step_internal<S: DynStep + 'static>(mut self, step: S) -> Self {
        self.steps.push(Box::new(step));
        self
    }

    /// Convert this traversal into a core Traversal for use in branch steps.
    pub(crate) fn into_core_traversal(self) -> interstellar::traversal::Traversal<Value, Value> {
        interstellar::traversal::Traversal::from_steps(self.steps)
    }

    /// Execute the traversal and return results.
    fn execute(&self) -> Vec<Value> {
        // Anonymous traversals cannot be executed directly
        if self.is_anonymous() {
            return Vec::new();
        }

        // Use the graph backend abstraction to execute with the appropriate snapshot
        let source = &self.source;
        let steps = &self.steps;

        let raw_results = self.graph.with_snapshot(|snapshot| {
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Create initial traversers from source
            let initial: Box<dyn Iterator<Item = Traverser> + '_> = match source {
                TraversalSource::AllVertices => Box::new(
                    snapshot
                        .storage()
                        .all_vertices()
                        .map(|v| Traverser::from_vertex(v.id)),
                ),
                TraversalSource::VertexIds(ids) => {
                    Box::new(ids.iter().cloned().map(Traverser::from_vertex))
                }
                TraversalSource::AllEdges => Box::new(
                    snapshot
                        .storage()
                        .all_edges()
                        .map(|e| Traverser::from_edge(e.id)),
                ),
                TraversalSource::EdgeIds(ids) => {
                    Box::new(ids.iter().cloned().map(Traverser::from_edge))
                }
                TraversalSource::Injected(values) => {
                    Box::new(values.iter().cloned().map(Traverser::new))
                }
                TraversalSource::Anonymous => {
                    return Vec::new();
                }
            };

            // Apply all steps using apply_dyn
            let mut current: Box<dyn Iterator<Item = Traverser> + '_> = initial;
            for step in steps {
                current = step.apply_dyn(&ctx, current);
            }

            // Collect results
            current.map(|t| t.value).collect()
        });

        // Process pending mutations and return actual values
        self.process_mutations(raw_results)
    }

    /// Process pending mutations and return actual results.
    fn process_mutations(&self, values: Vec<Value>) -> Vec<Value> {
        use interstellar::traversal::mutation::PendingMutation;

        let mut results = Vec::with_capacity(values.len());

        for value in values {
            if let Some(mutation) = PendingMutation::from_value(&value) {
                match mutation {
                    PendingMutation::AddVertex { label, properties } => {
                        let id = self.graph.add_vertex(&label, properties);
                        results.push(Value::Vertex(id));
                    }
                    PendingMutation::AddEdge {
                        label,
                        from,
                        to,
                        properties,
                    } => {
                        if let Ok(id) = self.graph.add_edge(from, to, &label, properties) {
                            results.push(Value::Edge(id));
                        }
                    }
                    PendingMutation::SetVertexProperty { id, key, value } => {
                        if self.graph.set_vertex_property(id, &key, value).is_ok() {
                            results.push(Value::Vertex(id));
                        }
                    }
                    PendingMutation::SetEdgeProperty { id, key, value } => {
                        if self.graph.set_edge_property(id, &key, value).is_ok() {
                            results.push(Value::Edge(id));
                        }
                    }
                    PendingMutation::DropVertex { id } => {
                        let _ = self.graph.remove_vertex(id);
                    }
                    PendingMutation::DropEdge { id } => {
                        let _ = self.graph.remove_edge(id);
                    }
                }
            } else {
                results.push(value);
            }
        }

        results
    }
}

#[napi]
impl JsTraversal {
    /// Create a new empty traversal (internal use only).
    ///
    /// This constructor is not meant to be called from JavaScript directly.
    /// Use graph.V() or graph.E() to create traversals.
    #[napi(constructor)]
    pub fn new() -> Self {
        // Return an anonymous traversal - this is just to satisfy napi-rs
        // Real traversals are created via JsGraph::v() etc.
        Self {
            graph: GraphBackend::InMemory(Arc::new(InnerGraph::new())),
            source: TraversalSource::Anonymous,
            steps: Vec::new(),
            output_type: TraversalType::Value,
        }
    }

    // =========================================================================
    // Filter Steps
    // =========================================================================

    /// Filter to elements with a specific label.
    ///
    /// @param label - The label to match
    #[napi(js_name = "hasLabel")]
    pub fn has_label(&self, label: String) -> JsTraversal {
        self.clone()
            .add_step(traversal::HasLabelStep::single(&label))
    }

    /// Filter to elements with any of the specified labels.
    ///
    /// @param labels - Array of labels to match (OR logic)
    #[napi(js_name = "hasLabelAny")]
    pub fn has_label_any(&self, env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        Ok(self
            .clone()
            .add_step(traversal::HasLabelStep::any(label_vec)))
    }

    /// Filter to elements that have a property (any value).
    ///
    /// @param key - Property name
    #[napi]
    pub fn has(&self, key: String) -> JsTraversal {
        self.clone().add_step(traversal::HasStep::new(&key))
    }

    /// Filter to elements that have a property with a specific value.
    ///
    /// @param key - Property name
    /// @param value - Exact value to match
    #[napi(js_name = "hasValue")]
    pub fn has_value(&self, env: Env, key: String, value: JsUnknown) -> Result<JsTraversal> {
        let val = js_to_value(env, value)?;
        Ok(self
            .clone()
            .add_step(traversal::HasValueStep::new(&key, val)))
    }

    /// Filter to elements where property matches a predicate.
    ///
    /// @param key - Property name
    /// @param predicate - Predicate to test (e.g., P.gt(10))
    #[napi(js_name = "hasWhere")]
    pub fn has_where(&self, key: String, predicate: &JsPredicate) -> JsTraversal {
        self.clone()
            .add_step(traversal::HasWhereStep::new(&key, predicate.inner.clone()))
    }

    /// Filter to elements that do NOT have a property.
    ///
    /// @param key - Property name that must be absent
    #[napi(js_name = "hasNot")]
    pub fn has_not(&self, key: String) -> JsTraversal {
        self.clone().add_step(traversal::HasNotStep::new(&key))
    }

    /// Filter to elements with specific IDs.
    ///
    /// @param ids - Element IDs to match
    #[napi(js_name = "hasId")]
    pub fn has_id(&self, env: Env, ids: JsUnknown) -> Result<JsTraversal> {
        let vertex_ids = crate::value::js_array_to_vertex_ids(env, ids)?;
        Ok(self
            .clone()
            .add_step(traversal::HasIdStep::vertices(vertex_ids)))
    }

    /// Remove duplicate elements from the traversal.
    #[napi]
    pub fn dedup(&self) -> JsTraversal {
        self.clone().add_step(traversal::DedupStep::default())
    }

    /// Limit results to the first n elements.
    ///
    /// @param n - Maximum number of elements
    #[napi]
    pub fn limit(&self, n: u32) -> JsTraversal {
        self.clone().add_step(traversal::LimitStep::new(n as usize))
    }

    /// Skip the first n elements.
    ///
    /// @param n - Number of elements to skip
    #[napi]
    pub fn skip(&self, n: u32) -> JsTraversal {
        self.clone().add_step(traversal::SkipStep::new(n as usize))
    }

    /// Take elements in a range [start, end).
    ///
    /// @param start - Start index (inclusive)
    /// @param end - End index (exclusive)
    #[napi]
    pub fn range(&self, start: u32, end: u32) -> JsTraversal {
        self.clone()
            .add_step(traversal::SkipStep::new(start as usize))
            .add_step(traversal::LimitStep::new((end - start) as usize))
    }

    // =========================================================================
    // Navigation Steps
    // =========================================================================

    /// Navigate to outgoing adjacent vertices.
    ///
    /// @param labels - Optional edge labels to traverse
    #[napi]
    pub fn out(&self, env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        if label_vec.is_empty() {
            Ok(self
                .clone()
                .add_step_with_type(traversal::OutStep::new(), TraversalType::Vertex))
        } else {
            Ok(self.clone().add_step_with_type(
                traversal::OutStep::with_labels(label_vec),
                TraversalType::Vertex,
            ))
        }
    }

    /// Navigate to incoming adjacent vertices.
    ///
    /// @param labels - Optional edge labels to traverse
    #[napi(js_name = "in")]
    pub fn in_(&self, env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        if label_vec.is_empty() {
            Ok(self
                .clone()
                .add_step_with_type(traversal::InStep::new(), TraversalType::Vertex))
        } else {
            Ok(self.clone().add_step_with_type(
                traversal::InStep::with_labels(label_vec),
                TraversalType::Vertex,
            ))
        }
    }

    /// Navigate to adjacent vertices in both directions.
    ///
    /// @param labels - Optional edge labels to traverse
    #[napi]
    pub fn both(&self, env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        if label_vec.is_empty() {
            Ok(self
                .clone()
                .add_step_with_type(traversal::BothStep::new(), TraversalType::Vertex))
        } else {
            Ok(self.clone().add_step_with_type(
                traversal::BothStep::with_labels(label_vec),
                TraversalType::Vertex,
            ))
        }
    }

    /// Navigate to outgoing edges.
    ///
    /// @param labels - Optional edge labels to match
    #[napi(js_name = "outE")]
    pub fn out_e(&self, env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        if label_vec.is_empty() {
            Ok(self
                .clone()
                .add_step_with_type(traversal::OutEStep::new(), TraversalType::Edge))
        } else {
            Ok(self.clone().add_step_with_type(
                traversal::OutEStep::with_labels(label_vec),
                TraversalType::Edge,
            ))
        }
    }

    /// Navigate to incoming edges.
    ///
    /// @param labels - Optional edge labels to match
    #[napi(js_name = "inE")]
    pub fn in_e(&self, env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        if label_vec.is_empty() {
            Ok(self
                .clone()
                .add_step_with_type(traversal::InEStep::new(), TraversalType::Edge))
        } else {
            Ok(self.clone().add_step_with_type(
                traversal::InEStep::with_labels(label_vec),
                TraversalType::Edge,
            ))
        }
    }

    /// Navigate to edges in both directions.
    ///
    /// @param labels - Optional edge labels to match
    #[napi(js_name = "bothE")]
    pub fn both_e(&self, env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        if label_vec.is_empty() {
            Ok(self
                .clone()
                .add_step_with_type(traversal::BothEStep::new(), TraversalType::Edge))
        } else {
            Ok(self.clone().add_step_with_type(
                traversal::BothEStep::with_labels(label_vec),
                TraversalType::Edge,
            ))
        }
    }

    /// Navigate from an edge to its source vertex.
    #[napi(js_name = "outV")]
    pub fn out_v(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::OutVStep::new(), TraversalType::Vertex)
    }

    /// Navigate from an edge to its target vertex.
    #[napi(js_name = "inV")]
    pub fn in_v(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::InVStep::new(), TraversalType::Vertex)
    }

    /// Navigate from an edge to both endpoints.
    #[napi(js_name = "bothV")]
    pub fn both_v(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::BothVStep::new(), TraversalType::Vertex)
    }

    /// Navigate to the vertex that was NOT the previous step.
    #[napi(js_name = "otherV")]
    pub fn other_v(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::OtherVStep::new(), TraversalType::Vertex)
    }

    // =========================================================================
    // Transform Steps
    // =========================================================================

    /// Extract property values.
    ///
    /// @param key - Property name to extract
    #[napi]
    pub fn values(&self, key: String) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::ValuesStep::new(&key), TraversalType::Value)
    }

    /// Extract the element ID.
    #[napi]
    pub fn id(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::IdStep::new(), TraversalType::Value)
    }

    /// Extract the element label.
    #[napi]
    pub fn label(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::LabelStep::new(), TraversalType::Value)
    }

    /// Get a map of property name to value.
    #[napi(js_name = "valueMap")]
    pub fn value_map(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::ValueMapStep::new(), TraversalType::Value)
    }

    /// Get a complete element map (id, label, and all properties).
    #[napi(js_name = "elementMap")]
    pub fn element_map(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::ElementMapStep::new(), TraversalType::Value)
    }

    /// Replace each element with a constant value.
    ///
    /// @param value - Constant value to emit
    #[napi]
    pub fn constant(&self, env: Env, value: JsUnknown) -> Result<JsTraversal> {
        let val = js_to_value(env, value)?;
        Ok(self
            .clone()
            .add_step_with_type(traversal::ConstantStep::new(val), TraversalType::Value))
    }

    /// Flatten lists in the stream.
    #[napi]
    pub fn unfold(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::UnfoldStep::new(), TraversalType::Value)
    }

    /// Collect all elements into a single list.
    #[napi]
    pub fn fold(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(FoldStep::new(), TraversalType::Value)
    }

    /// Get the traversal path (history of elements visited).
    #[napi]
    pub fn path(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::PathStep::new(), TraversalType::Value)
    }

    /// Label the current step for later reference.
    ///
    /// @param label - Step label
    #[napi(js_name = "as")]
    pub fn as_(&self, label: String) -> JsTraversal {
        self.clone().add_step(traversal::AsStep::new(&label))
    }

    /// Select labeled steps from the path.
    ///
    /// @param labels - Step labels to select
    #[napi]
    pub fn select(&self, env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        Ok(self
            .clone()
            .add_step_with_type(traversal::SelectStep::new(label_vec), TraversalType::Value))
    }

    /// Count the number of elements.
    #[napi(js_name = "count_")]
    pub fn count_step(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(CountStep::new(), TraversalType::Value)
    }

    /// Calculate the sum of numeric values.
    #[napi(js_name = "sum")]
    pub fn sum_step(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(SumStep::new(), TraversalType::Value)
    }

    /// Calculate the arithmetic mean of numeric values.
    #[napi(js_name = "mean")]
    pub fn mean_step(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::MeanStep::new(), TraversalType::Value)
    }

    /// Get the minimum value.
    #[napi(js_name = "min")]
    pub fn min_step(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::MinStep::new(), TraversalType::Value)
    }

    /// Get the maximum value.
    #[napi(js_name = "max")]
    pub fn max_step(&self) -> JsTraversal {
        self.clone()
            .add_step_with_type(traversal::MaxStep::new(), TraversalType::Value)
    }

    // =========================================================================
    // Order Steps
    // =========================================================================

    /// Order by natural value (ascending).
    #[napi(js_name = "orderAsc")]
    pub fn order_asc(&self) -> JsTraversal {
        self.clone()
            .add_step(traversal::OrderStep::by_natural(traversal::Order::Asc))
    }

    /// Order by natural value (descending).
    #[napi(js_name = "orderDesc")]
    pub fn order_desc(&self) -> JsTraversal {
        self.clone()
            .add_step(traversal::OrderStep::by_natural(traversal::Order::Desc))
    }

    // =========================================================================
    // Branch Steps
    // =========================================================================

    /// Filter with a sub-traversal condition.
    ///
    /// @param traversal - Traversal that must produce results
    #[napi(js_name = "where")]
    pub fn where_(&self, traversal: &JsTraversal) -> JsTraversal {
        self.clone().add_step(traversal::WhereStep::new(
            traversal.clone().into_core_traversal(),
        ))
    }

    /// Negate a filter condition.
    ///
    /// @param traversal - Traversal that must NOT produce results
    #[napi]
    pub fn not(&self, traversal: &JsTraversal) -> JsTraversal {
        self.clone().add_step(traversal::NotStep::new(
            traversal.clone().into_core_traversal(),
        ))
    }

    /// Execute multiple traversals and combine results.
    ///
    /// @param traversals - Traversals to execute in parallel
    #[napi]
    pub fn union(&self, traversals: Vec<&JsTraversal>) -> JsTraversal {
        let core_traversals: Vec<_> = traversals
            .into_iter()
            .map(|t| t.clone().into_core_traversal())
            .collect();
        self.clone()
            .add_step(traversal::UnionStep::new(core_traversals))
    }

    /// Return the result of the first traversal that produces output.
    ///
    /// @param traversals - Traversals to try in order
    #[napi]
    pub fn coalesce(&self, traversals: Vec<&JsTraversal>) -> JsTraversal {
        let core_traversals: Vec<_> = traversals
            .into_iter()
            .map(|t| t.clone().into_core_traversal())
            .collect();
        self.clone()
            .add_step(traversal::CoalesceStep::new(core_traversals))
    }

    /// Execute traversal, but pass through original if no results.
    ///
    /// @param traversal - Optional traversal
    #[napi]
    pub fn optional(&self, traversal: &JsTraversal) -> JsTraversal {
        self.clone().add_step(traversal::OptionalStep::new(
            traversal.clone().into_core_traversal(),
        ))
    }

    /// Execute traversal in local scope (per element).
    ///
    /// @param traversal - Traversal to execute locally
    #[napi]
    pub fn local(&self, traversal: &JsTraversal) -> JsTraversal {
        self.clone().add_step(traversal::LocalStep::new(
            traversal.clone().into_core_traversal(),
        ))
    }

    // =========================================================================
    // Mutation Steps
    // =========================================================================

    /// Set a property on the current element.
    ///
    /// @param key - Property name
    /// @param value - Property value
    #[napi]
    pub fn property(&self, env: Env, key: String, value: JsUnknown) -> Result<JsTraversal> {
        let val = js_to_value(env, value)?;
        Ok(self
            .clone()
            .add_step(traversal::PropertyStep::new(&key, val)))
    }

    /// Remove the current element from the graph.
    #[napi]
    pub fn drop(&self) -> JsTraversal {
        self.clone().add_step(traversal::DropStep::new())
    }

    // =========================================================================
    // Terminal Steps
    // =========================================================================

    /// Execute the traversal and return all results as an array.
    ///
    /// @returns Array of results
    #[napi(js_name = "toList")]
    pub fn to_list(&self, env: Env) -> Result<JsUnknown> {
        let results = self.execute();
        values_to_js_array(env, results)
    }

    /// Execute and return the first result, or undefined.
    ///
    /// @returns First result or undefined
    #[napi]
    pub fn first(&self, env: Env) -> Result<Option<JsUnknown>> {
        let results = self.execute();
        match results.into_iter().next() {
            Some(v) => Ok(Some(value_to_js(env, &v)?)),
            None => Ok(None),
        }
    }

    /// Execute and return the next result.
    /// Alias for first().
    #[napi]
    pub fn next(&self, env: Env) -> Result<Option<JsUnknown>> {
        self.first(env)
    }

    /// Execute and return exactly one result.
    ///
    /// @throws If zero or more than one result
    #[napi]
    pub fn one(&self, env: Env) -> Result<JsUnknown> {
        let results = self.execute();
        if results.len() != 1 {
            return Err(Error::new(
                Status::GenericFailure,
                format!("Expected exactly one result, got {}", results.len()),
            ));
        }
        value_to_js(env, &results[0])
    }

    /// Check if the traversal has any results.
    ///
    /// @returns true if at least one result exists
    #[napi(js_name = "hasNext")]
    pub fn has_next(&self) -> bool {
        !self.execute().is_empty()
    }

    /// Execute and return the count of results.
    ///
    /// @returns Number of results
    #[napi]
    pub fn count(&self) -> u32 {
        self.execute().len() as u32
    }

    /// Iterate through all results (for side effects like drop).
    #[napi]
    pub fn iterate(&self) {
        let _ = self.execute();
    }
}
