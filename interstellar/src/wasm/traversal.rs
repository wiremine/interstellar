//! Traversal facade for WASM bindings.
//!
//! Provides a JavaScript-friendly traversal API with method chaining.

use std::sync::Arc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsError;

use crate::storage::cow::Graph as InnerGraph;
use crate::storage::GraphStorage;
use crate::traversal::context::SnapshotLike;
use crate::traversal::step::Step;
use crate::traversal::{self, DynStep, ExecutionContext, Traverser};
use crate::value::{EdgeId, Value, VertexId};
use crate::wasm::predicate::Predicate;
use crate::wasm::types::{
    js_array_to_strings, js_to_u64, js_to_value, js_to_vertex_id, value_to_js, values_to_js_array,
};

// =============================================================================
// Helper Steps for Mutation Operations
// =============================================================================

/// Step that creates a pending addE marker.
///
/// This is a WASM-specific spawning step that always produces a pending
/// edge marker. The from/to endpoints are set by subsequent fromId()/toId() steps.
#[derive(Clone, Debug)]
pub(crate) struct AddESpawnStep {
    label: String,
}

impl AddESpawnStep {
    pub(crate) fn new(label: &str) -> Self {
        Self {
            label: label.to_string(),
        }
    }
}

impl Step for AddESpawnStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        _input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let label = self.label.clone();

        // Spawning step - produce one pending addE marker regardless of input
        std::iter::once_with(move || {
            Traverser::new(Value::Map(crate::value::ValueMap::from_iter([
                ("__pending_add_e".to_string(), Value::Bool(true)),
                ("label".to_string(), Value::String(label.clone())),
                (
                    "properties".to_string(),
                    Value::Map(crate::value::ValueMap::new()),
                ),
            ])))
        })
    }

    fn name(&self) -> &'static str {
        "addE_spawn"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        Box::new(std::iter::once(input))
    }
}

/// Step that sets the "from" endpoint for a pending addE by step label.
#[derive(Clone, Debug)]
struct AddEFromLabelStep {
    label: String,
}

impl AddEFromLabelStep {
    fn new(label: &str) -> Self {
        Self {
            label: label.to_string(),
        }
    }
}

impl Step for AddEFromLabelStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let label = self.label.clone();
        input.map(move |mut t| {
            if let Value::Map(ref mut map) = t.value {
                if map.get("__pending_add_e").is_some() {
                    // Resolve from step label
                    if let Some(values) = t.path.get(&label) {
                        if let Some(pv) = values.first() {
                            if let Some(vid) = pv.as_vertex_id() {
                                map.insert("from".to_string(), Value::Vertex(vid));
                            }
                        }
                    }
                }
            }
            t
        })
    }

    fn name(&self) -> &'static str {
        "addE_from_label"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        Box::new(std::iter::once(input))
    }
}

/// Step that sets the "from" endpoint for a pending addE by vertex ID.
#[derive(Clone, Debug)]
struct AddEFromIdStep {
    id: VertexId,
}

impl AddEFromIdStep {
    fn new(id: VertexId) -> Self {
        Self { id }
    }
}

impl Step for AddEFromIdStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let id = self.id;
        input.map(move |mut t| {
            if let Value::Map(ref mut map) = t.value {
                if map.get("__pending_add_e").is_some() {
                    map.insert("from".to_string(), Value::Vertex(id));
                }
            }
            t
        })
    }

    fn name(&self) -> &'static str {
        "addE_from_id"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        Box::new(std::iter::once(input))
    }
}

/// Step that sets the "to" endpoint for a pending addE by step label.
#[derive(Clone, Debug)]
struct AddEToLabelStep {
    label: String,
}

impl AddEToLabelStep {
    fn new(label: &str) -> Self {
        Self {
            label: label.to_string(),
        }
    }
}

impl Step for AddEToLabelStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let label = self.label.clone();
        input.map(move |mut t| {
            if let Value::Map(ref mut map) = t.value {
                if map.get("__pending_add_e").is_some() {
                    // Resolve to step label
                    if let Some(values) = t.path.get(&label) {
                        if let Some(pv) = values.first() {
                            if let Some(vid) = pv.as_vertex_id() {
                                map.insert("to".to_string(), Value::Vertex(vid));
                            }
                        }
                    }
                }
            }
            t
        })
    }

    fn name(&self) -> &'static str {
        "addE_to_label"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        Box::new(std::iter::once(input))
    }
}

/// Step that sets the "to" endpoint for a pending addE by vertex ID.
#[derive(Clone, Debug)]
struct AddEToIdStep {
    id: VertexId,
}

impl AddEToIdStep {
    fn new(id: VertexId) -> Self {
        Self { id }
    }
}

impl Step for AddEToIdStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let id = self.id;
        input.map(move |mut t| {
            if let Value::Map(ref mut map) = t.value {
                if map.get("__pending_add_e").is_some() {
                    map.insert("to".to_string(), Value::Vertex(id));
                }
            }
            t
        })
    }

    fn name(&self) -> &'static str {
        "addE_to_id"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        Box::new(std::iter::once(input))
    }
}

// =============================================================================
// Aggregate Steps (Fold, Sum)
// =============================================================================

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
        _ctx: crate::traversal::context::StreamingContext,
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
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // BARRIER STEP: Cannot stream - must collect all values
        Box::new(std::iter::once(input))
    }
}

/// The type of elements in the traversal stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TraversalType {
    Vertex,
    Edge,
    Value,
}

/// Source type for initial traversal start.
#[derive(Clone)]
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
#[wasm_bindgen]
pub struct Traversal {
    /// The graph this traversal operates on
    pub(crate) graph: Arc<InnerGraph>,
    /// Source of initial traversers
    pub(crate) source: TraversalSource,
    /// Accumulated steps (type-erased)
    pub(crate) steps: Vec<Box<dyn DynStep>>,
    /// Current output type
    pub(crate) output_type: TraversalType,
}

impl Clone for Traversal {
    fn clone(&self) -> Self {
        Self {
            graph: Arc::clone(&self.graph),
            source: self.source.clone(),
            steps: self.steps.iter().map(|s| s.clone_box()).collect(),
            output_type: self.output_type,
        }
    }
}

impl Traversal {
    // =========================================================================
    // Construction (internal)
    // =========================================================================

    pub(crate) fn from_all_vertices(graph: Arc<InnerGraph>) -> Self {
        Self {
            graph,
            source: TraversalSource::AllVertices,
            steps: Vec::new(),
            output_type: TraversalType::Vertex,
        }
    }

    pub(crate) fn from_vertex_ids(graph: Arc<InnerGraph>, ids: Vec<VertexId>) -> Self {
        Self {
            graph,
            source: TraversalSource::VertexIds(ids),
            steps: Vec::new(),
            output_type: TraversalType::Vertex,
        }
    }

    pub(crate) fn from_all_edges(graph: Arc<InnerGraph>) -> Self {
        Self {
            graph,
            source: TraversalSource::AllEdges,
            steps: Vec::new(),
            output_type: TraversalType::Edge,
        }
    }

    pub(crate) fn from_edge_ids(graph: Arc<InnerGraph>, ids: Vec<EdgeId>) -> Self {
        Self {
            graph,
            source: TraversalSource::EdgeIds(ids),
            steps: Vec::new(),
            output_type: TraversalType::Edge,
        }
    }

    pub(crate) fn from_injected_values(graph: Arc<InnerGraph>, values: Vec<Value>) -> Self {
        Self {
            graph,
            source: TraversalSource::Injected(values),
            steps: Vec::new(),
            output_type: TraversalType::Value,
        }
    }

    /// Create an anonymous traversal with a single step.
    ///
    /// Anonymous traversals are used with branch steps like `where()`, `union()`, etc.
    /// They don't have their own graph - they receive input from the parent traversal.
    pub(crate) fn anonymous_with_step<S: DynStep + 'static>(step: S) -> Self {
        Self {
            graph: Arc::new(InnerGraph::new()),
            source: TraversalSource::Anonymous,
            steps: vec![Box::new(step)],
            output_type: TraversalType::Value,
        }
    }

    /// Check if this is an anonymous traversal.
    pub(crate) fn is_anonymous(&self) -> bool {
        matches!(self.source, TraversalSource::Anonymous)
    }

    /// Get the accumulated steps from this traversal.
    #[allow(dead_code)]
    pub(crate) fn into_steps(self) -> Vec<Box<dyn DynStep>> {
        self.steps
    }

    /// Convert this WASM Traversal into a core Traversal<Value, Value>.
    ///
    /// This is used when passing anonymous traversals to branch steps
    /// like `where()`, `union()`, `repeat()`, etc.
    pub(crate) fn into_core_traversal(self) -> crate::traversal::Traversal<Value, Value> {
        crate::traversal::Traversal {
            steps: self.steps,
            source: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create an anonymous traversal (no source, empty steps).
    ///
    /// Used by the `__` factory for creating traversal fragments.
    #[allow(dead_code)]
    pub(crate) fn anonymous(graph: Arc<InnerGraph>) -> Self {
        Self {
            graph,
            source: TraversalSource::Anonymous,
            steps: Vec::new(),
            output_type: TraversalType::Value,
        }
    }

    /// Add a step to the traversal pipeline.
    fn add_step<S: DynStep + 'static>(mut self, step: S) -> Self {
        self.steps.push(Box::new(step));
        self
    }

    /// Add a step to the traversal pipeline (pub(crate) for anonymous factory).
    pub(crate) fn add_step_internal<S: DynStep + 'static>(mut self, step: S) -> Self {
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

    /// Execute the traversal and return results.
    fn execute(&self) -> Vec<Value> {
        // Anonymous traversals cannot be executed directly
        if self.is_anonymous() {
            return Vec::new();
        }

        let snapshot = self.graph.snapshot();
        let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

        // Create initial traversers from source
        let initial: Box<dyn Iterator<Item = Traverser> + '_> = match &self.source {
            TraversalSource::AllVertices => Box::new(
                snapshot
                    .all_vertices()
                    .map(|v| Traverser::from_vertex(v.id)),
            ),
            TraversalSource::VertexIds(ids) => {
                Box::new(ids.iter().cloned().map(Traverser::from_vertex))
            }
            TraversalSource::AllEdges => {
                Box::new(snapshot.all_edges().map(|e| Traverser::from_edge(e.id)))
            }
            TraversalSource::EdgeIds(ids) => {
                Box::new(ids.iter().cloned().map(Traverser::from_edge))
            }
            TraversalSource::Injected(values) => {
                Box::new(values.iter().cloned().map(Traverser::new))
            }
            TraversalSource::Anonymous => {
                // This shouldn't happen - anonymous traversals shouldn't be executed directly
                return Vec::new();
            }
        };

        // Apply all steps using apply_dyn
        let mut current: Box<dyn Iterator<Item = Traverser> + '_> = initial;
        for step in &self.steps {
            current = step.apply_dyn(&ctx, current);
        }

        // Collect results and process any pending mutations
        let raw_results: Vec<Value> = current.map(|t| t.value).collect();

        // Process pending mutations and return actual values
        self.process_mutations(raw_results)
    }

    /// Process pending mutations and return actual results.
    ///
    /// This method handles the "pending mutation" markers created by
    /// AddVStep, AddEStep, PropertyStep, and DropStep. It executes
    /// the mutations against the graph and returns the resulting values.
    fn process_mutations(&self, values: Vec<Value>) -> Vec<Value> {
        use crate::traversal::mutation::PendingMutation;

        let mut results = Vec::with_capacity(values.len());

        for value in values {
            if let Some(mutation) = PendingMutation::from_value(&value) {
                // Execute the mutation and get the result
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
                        match self.graph.add_edge(from, to, &label, properties) {
                            Ok(id) => results.push(Value::Edge(id)),
                            Err(_) => {
                                // Edge creation failed - skip
                            }
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
                        // Drop doesn't return a value
                    }
                    PendingMutation::DropEdge { id } => {
                        let _ = self.graph.remove_edge(id);
                        // Drop doesn't return a value
                    }
                }
            } else {
                // Not a pending mutation, just pass through
                results.push(value);
            }
        }

        results
    }
}

#[wasm_bindgen]
impl Traversal {
    // =========================================================================
    // Filter Steps
    // =========================================================================

    /// Filter to elements with a specific label.
    ///
    /// @param label - The label to match
    #[wasm_bindgen(js_name = "hasLabel")]
    pub fn has_label(self, label: &str) -> Traversal {
        self.add_step(traversal::HasLabelStep::single(label))
    }

    /// Filter to elements with any of the specified labels.
    ///
    /// @param labels - Array of labels to match (OR logic)
    #[wasm_bindgen(js_name = "hasLabelAny")]
    pub fn has_label_any(self, labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(self.add_step(traversal::HasLabelStep::any(label_vec)))
    }

    /// Filter to elements that have a property (any value).
    ///
    /// @param key - Property name
    pub fn has(self, key: &str) -> Traversal {
        self.add_step(traversal::HasStep::new(key))
    }

    /// Filter to elements that have a property with a specific value.
    ///
    /// @param key - Property name
    /// @param value - Exact value to match
    #[wasm_bindgen(js_name = "hasValue")]
    pub fn has_value(self, key: &str, value: JsValue) -> Result<Traversal, JsError> {
        let v = js_to_value(value)?;
        Ok(self.add_step(traversal::HasValueStep::new(key, v)))
    }

    /// Filter to elements where property matches a predicate.
    ///
    /// @param key - Property name
    /// @param predicate - Predicate to test (e.g., P.gt(10n))
    #[wasm_bindgen(js_name = "hasWhere")]
    pub fn has_where(self, key: &str, predicate: Predicate) -> Traversal {
        // HasWhereStep::new accepts impl Predicate, and Box<dyn Predicate> implements Predicate
        self.add_step(traversal::HasWhereStep::new(key, predicate.into_inner()))
    }

    /// Filter to elements that do NOT have a property.
    ///
    /// @param key - Property name that must be absent
    #[wasm_bindgen(js_name = "hasNot")]
    pub fn has_not(self, key: &str) -> Traversal {
        self.add_step(traversal::HasNotStep::new(key))
    }

    /// Filter to elements with a specific ID.
    ///
    /// @param id - The element ID
    #[wasm_bindgen(js_name = "hasId")]
    pub fn has_id(self, id: JsValue) -> Result<Traversal, JsError> {
        let vid = js_to_vertex_id(id)?;
        Ok(self.add_step(traversal::HasIdStep::vertex(vid)))
    }

    /// Filter to elements with any of the specified IDs.
    ///
    /// @param ids - Array of element IDs to match
    #[wasm_bindgen(js_name = "hasIds")]
    pub fn has_ids(self, ids: JsValue) -> Result<Traversal, JsError> {
        let id_vec = crate::wasm::types::js_array_to_vertex_ids(ids)?;
        Ok(self.add_step(traversal::HasIdStep::vertices(id_vec)))
    }

    /// Filter values matching a predicate.
    ///
    /// @param predicate - Predicate to test
    #[wasm_bindgen(js_name = "is")]
    pub fn is_(self, predicate: Predicate) -> Traversal {
        // IsStep::new accepts impl Predicate, and Box<dyn Predicate> implements Predicate
        self.add_step(traversal::IsStep::new(predicate.into_inner()))
    }

    /// Filter values equal to a specific value.
    ///
    /// @param value - Value to match
    #[wasm_bindgen(js_name = "isEq")]
    pub fn is_eq(self, value: JsValue) -> Result<Traversal, JsError> {
        let v = js_to_value(value)?;
        Ok(self.add_step(traversal::IsStep::eq(v)))
    }

    /// Remove duplicate elements from the traversal.
    pub fn dedup(self) -> Traversal {
        self.add_step(traversal::DedupStep::new())
    }

    /// Remove duplicates based on a property key.
    ///
    /// @param key - Property to deduplicate by
    #[wasm_bindgen(js_name = "dedupByKey")]
    pub fn dedup_by_key(self, key: &str) -> Traversal {
        self.add_step(traversal::DedupByKeyStep::new(key))
    }

    /// Remove duplicates based on element label.
    #[wasm_bindgen(js_name = "dedupByLabel")]
    pub fn dedup_by_label(self) -> Traversal {
        self.add_step(traversal::DedupByLabelStep::new())
    }

    /// Remove duplicates based on the result of a traversal.
    ///
    /// @param traversal - Anonymous traversal to compute dedup key
    #[wasm_bindgen(js_name = "dedupBy")]
    pub fn dedup_by(self, sub: Traversal) -> Traversal {
        let core_traversal = sub.into_core_traversal();
        self.add_step(traversal::DedupByTraversalStep::new(core_traversal))
    }

    /// Limit results to the first n elements.
    ///
    /// @param n - Maximum number of elements
    pub fn limit(self, n: JsValue) -> Result<Traversal, JsError> {
        let num = js_to_u64(n)?;
        Ok(self.add_step(traversal::LimitStep::new(num as usize)))
    }

    /// Skip the first n elements.
    ///
    /// @param n - Number of elements to skip
    pub fn skip(self, n: JsValue) -> Result<Traversal, JsError> {
        let num = js_to_u64(n)?;
        Ok(self.add_step(traversal::SkipStep::new(num as usize)))
    }

    /// Take elements in a range [start, end).
    ///
    /// @param start - Start index (inclusive)
    /// @param end - End index (exclusive)
    pub fn range(self, start: JsValue, end: JsValue) -> Result<Traversal, JsError> {
        let s = js_to_u64(start)? as usize;
        let e = js_to_u64(end)? as usize;
        Ok(self.add_step(traversal::RangeStep::new(s, e)))
    }

    /// Get the last element.
    pub fn tail(self) -> Traversal {
        self.add_step(traversal::TailStep::new(1))
    }

    /// Get the last n elements.
    ///
    /// @param n - Number of elements from end
    #[wasm_bindgen(js_name = "tailN")]
    pub fn tail_n(self, n: JsValue) -> Result<Traversal, JsError> {
        let num = js_to_u64(n)?;
        Ok(self.add_step(traversal::TailStep::new(num as usize)))
    }

    /// Randomly filter elements with a given probability.
    ///
    /// @param probability - Probability (0.0 to 1.0) of keeping each element
    pub fn coin(self, probability: f64) -> Traversal {
        self.add_step(traversal::CoinStep::new(probability))
    }

    /// Randomly sample n elements.
    ///
    /// @param n - Number of elements to sample
    pub fn sample(self, n: JsValue) -> Result<Traversal, JsError> {
        let num = js_to_u64(n)?;
        Ok(self.add_step(traversal::SampleStep::new(num as usize)))
    }

    /// Filter to paths that don't repeat vertices.
    #[wasm_bindgen(js_name = "simplePath")]
    pub fn simple_path(self) -> Traversal {
        self.add_step(traversal::SimplePathStep::new())
    }

    /// Filter to paths that do repeat vertices.
    #[wasm_bindgen(js_name = "cyclicPath")]
    pub fn cyclic_path(self) -> Traversal {
        self.add_step(traversal::CyclicPathStep::new())
    }

    // =========================================================================
    // Branch / Conditional Steps
    // =========================================================================

    /// Filter based on the result of a traversal (must produce results).
    ///
    /// @param traversal - Anonymous traversal to test
    #[wasm_bindgen(js_name = "where_")]
    pub fn where_(self, sub: Traversal) -> Traversal {
        let core_traversal = sub.into_core_traversal();
        self.add_step(traversal::WhereStep::new(core_traversal))
    }

    /// Filter to elements where the traversal produces NO results.
    ///
    /// @param traversal - Anonymous traversal that must be empty
    pub fn not(self, sub: Traversal) -> Traversal {
        let core_traversal = sub.into_core_traversal();
        self.add_step(traversal::NotStep::new(core_traversal))
    }

    /// Filter where ALL traversals produce results.
    ///
    /// @param traversals - Array of anonymous traversals (AND logic)
    #[wasm_bindgen(js_name = "and_")]
    pub fn and_(self, traversals: Vec<Traversal>) -> Traversal {
        let core_traversals: Vec<_> = traversals
            .into_iter()
            .map(|t| t.into_core_traversal())
            .collect();
        self.add_step(traversal::AndStep::new(core_traversals))
    }

    /// Filter where ANY traversal produces results.
    ///
    /// @param traversals - Array of anonymous traversals (OR logic)
    #[wasm_bindgen(js_name = "or_")]
    pub fn or_(self, traversals: Vec<Traversal>) -> Traversal {
        let core_traversals: Vec<_> = traversals
            .into_iter()
            .map(|t| t.into_core_traversal())
            .collect();
        self.add_step(traversal::OrStep::new(core_traversals))
    }

    /// Execute multiple traversals and combine results.
    ///
    /// @param traversals - Traversals to execute in parallel
    pub fn union(self, traversals: Vec<Traversal>) -> Traversal {
        let core_traversals: Vec<_> = traversals
            .into_iter()
            .map(|t| t.into_core_traversal())
            .collect();
        self.add_step(traversal::UnionStep::new(core_traversals))
    }

    /// Return the result of the first traversal that produces output.
    ///
    /// @param traversals - Traversals to try in order
    pub fn coalesce(self, traversals: Vec<Traversal>) -> Traversal {
        let core_traversals: Vec<_> = traversals
            .into_iter()
            .map(|t| t.into_core_traversal())
            .collect();
        self.add_step(traversal::CoalesceStep::new(core_traversals))
    }

    /// Conditional branching.
    ///
    /// @param condition - Traversal to test condition
    /// @param if_true - Traversal if condition produces results
    /// @param if_false - Traversal if condition produces no results
    pub fn choose(
        self,
        condition: Traversal,
        if_true: Traversal,
        if_false: Traversal,
    ) -> Traversal {
        let cond = condition.into_core_traversal();
        let t_branch = if_true.into_core_traversal();
        let f_branch = if_false.into_core_traversal();
        self.add_step(traversal::ChooseStep::new(cond, t_branch, f_branch))
    }

    /// Execute traversal, but pass through original if no results.
    ///
    /// @param traversal - Optional traversal
    pub fn optional(self, sub: Traversal) -> Traversal {
        let core_traversal = sub.into_core_traversal();
        self.add_step(traversal::OptionalStep::new(core_traversal))
    }

    /// Execute traversal in local scope (per element).
    ///
    /// @param traversal - Traversal to execute locally
    pub fn local(self, sub: Traversal) -> Traversal {
        let core_traversal = sub.into_core_traversal();
        self.add_step(traversal::LocalStep::new(core_traversal))
    }

    // =========================================================================
    // Navigation Steps
    // =========================================================================

    /// Navigate to outgoing adjacent vertices (via all edge labels).
    pub fn out(self) -> Traversal {
        self.add_step_with_type(traversal::OutStep::new(), TraversalType::Vertex)
    }

    /// Navigate to outgoing adjacent vertices via specific edge labels.
    ///
    /// @param labels - Array of edge labels to traverse
    #[wasm_bindgen(js_name = "outLabels")]
    pub fn out_labels(self, labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(self.add_step_with_type(
            traversal::OutStep::with_labels(label_vec),
            TraversalType::Vertex,
        ))
    }

    /// Navigate to incoming adjacent vertices (via all edge labels).
    #[wasm_bindgen(js_name = "in_")]
    pub fn in_(self) -> Traversal {
        self.add_step_with_type(traversal::InStep::new(), TraversalType::Vertex)
    }

    /// Navigate to incoming adjacent vertices via specific edge labels.
    ///
    /// @param labels - Array of edge labels to traverse
    #[wasm_bindgen(js_name = "inLabels")]
    pub fn in_labels(self, labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(self.add_step_with_type(
            traversal::InStep::with_labels(label_vec),
            TraversalType::Vertex,
        ))
    }

    /// Navigate to adjacent vertices in both directions.
    pub fn both(self) -> Traversal {
        self.add_step_with_type(traversal::BothStep::new(), TraversalType::Vertex)
    }

    /// Navigate to adjacent vertices in both directions via specific labels.
    ///
    /// @param labels - Array of edge labels to traverse
    #[wasm_bindgen(js_name = "bothLabels")]
    pub fn both_labels(self, labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(self.add_step_with_type(
            traversal::BothStep::with_labels(label_vec),
            TraversalType::Vertex,
        ))
    }

    /// Navigate to outgoing edges.
    #[wasm_bindgen(js_name = "outE")]
    pub fn out_e(self) -> Traversal {
        self.add_step_with_type(traversal::OutEStep::new(), TraversalType::Edge)
    }

    /// Navigate to outgoing edges with specific labels.
    ///
    /// @param labels - Array of edge labels to match
    #[wasm_bindgen(js_name = "outELabels")]
    pub fn out_e_labels(self, labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(self.add_step_with_type(
            traversal::OutEStep::with_labels(label_vec),
            TraversalType::Edge,
        ))
    }

    /// Navigate to incoming edges.
    #[wasm_bindgen(js_name = "inE")]
    pub fn in_e(self) -> Traversal {
        self.add_step_with_type(traversal::InEStep::new(), TraversalType::Edge)
    }

    /// Navigate to incoming edges with specific labels.
    ///
    /// @param labels - Array of edge labels to match
    #[wasm_bindgen(js_name = "inELabels")]
    pub fn in_e_labels(self, labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(self.add_step_with_type(
            traversal::InEStep::with_labels(label_vec),
            TraversalType::Edge,
        ))
    }

    /// Navigate to edges in both directions.
    #[wasm_bindgen(js_name = "bothE")]
    pub fn both_e(self) -> Traversal {
        self.add_step_with_type(traversal::BothEStep::new(), TraversalType::Edge)
    }

    /// Navigate to edges in both directions with specific labels.
    ///
    /// @param labels - Array of edge labels to match
    #[wasm_bindgen(js_name = "bothELabels")]
    pub fn both_e_labels(self, labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(self.add_step_with_type(
            traversal::BothEStep::with_labels(label_vec),
            TraversalType::Edge,
        ))
    }

    /// Navigate from an edge to its outgoing (source) vertex.
    #[wasm_bindgen(js_name = "outV")]
    pub fn out_v(self) -> Traversal {
        self.add_step_with_type(traversal::OutVStep::new(), TraversalType::Vertex)
    }

    /// Navigate from an edge to its incoming (target) vertex.
    #[wasm_bindgen(js_name = "inV")]
    pub fn in_v(self) -> Traversal {
        self.add_step_with_type(traversal::InVStep::new(), TraversalType::Vertex)
    }

    /// Navigate from an edge to both endpoints.
    #[wasm_bindgen(js_name = "bothV")]
    pub fn both_v(self) -> Traversal {
        self.add_step_with_type(traversal::BothVStep::new(), TraversalType::Vertex)
    }

    /// Navigate from an edge to the vertex that was NOT the previous step.
    #[wasm_bindgen(js_name = "otherV")]
    pub fn other_v(self) -> Traversal {
        self.add_step_with_type(traversal::OtherVStep::new(), TraversalType::Vertex)
    }

    // =========================================================================
    // Transform Steps
    // =========================================================================

    /// Extract a single property value.
    ///
    /// @param key - Property name
    pub fn values(self, key: &str) -> Traversal {
        self.add_step_with_type(traversal::ValuesStep::new(key), TraversalType::Value)
    }

    /// Extract multiple property values (as a list per element).
    ///
    /// @param keys - Array of property names
    #[wasm_bindgen(js_name = "valuesMulti")]
    pub fn values_multi(self, keys: JsValue) -> Result<Traversal, JsError> {
        let key_vec = js_array_to_strings(keys)?;
        Ok(self.add_step_with_type(traversal::ValuesStep::multi(key_vec), TraversalType::Value))
    }

    /// Get a map of property name to value.
    #[wasm_bindgen(js_name = "valueMap")]
    pub fn value_map(self) -> Traversal {
        self.add_step_with_type(traversal::ValueMapStep::new(), TraversalType::Value)
    }

    /// Get a map of specific property names to values.
    ///
    /// @param keys - Array of property names to include
    #[wasm_bindgen(js_name = "valueMapKeys")]
    pub fn value_map_keys(self, keys: JsValue) -> Result<Traversal, JsError> {
        let key_vec = js_array_to_strings(keys)?;
        Ok(self.add_step_with_type(
            traversal::ValueMapStep::with_keys(key_vec),
            TraversalType::Value,
        ))
    }

    /// Get a value map including id and label tokens.
    #[wasm_bindgen(js_name = "valueMapWithTokens")]
    pub fn value_map_with_tokens(self) -> Traversal {
        self.add_step_with_type(
            traversal::ValueMapStep::new().with_tokens(),
            TraversalType::Value,
        )
    }

    /// Get all properties as Property objects.
    pub fn properties(self) -> Traversal {
        self.add_step_with_type(traversal::PropertiesStep::new(), TraversalType::Value)
    }

    /// Get specific properties as Property objects.
    ///
    /// @param keys - Property names
    #[wasm_bindgen(js_name = "propertiesKeys")]
    pub fn properties_keys(self, keys: JsValue) -> Result<Traversal, JsError> {
        let key_vec = js_array_to_strings(keys)?;
        Ok(self.add_step_with_type(
            traversal::PropertiesStep::with_keys(key_vec),
            TraversalType::Value,
        ))
    }

    /// Get a map of property name to Property objects.
    #[wasm_bindgen(js_name = "propertyMap")]
    pub fn property_map(self) -> Traversal {
        self.add_step_with_type(traversal::PropertyMapStep::new(), TraversalType::Value)
    }

    /// Get a property map with specific keys.
    ///
    /// @param keys - Property names to include
    #[wasm_bindgen(js_name = "propertyMapKeys")]
    pub fn property_map_keys(self, keys: JsValue) -> Result<Traversal, JsError> {
        let key_vec = js_array_to_strings(keys)?;
        Ok(self.add_step_with_type(
            traversal::PropertyMapStep::with_keys(key_vec),
            TraversalType::Value,
        ))
    }

    /// Get a complete element map (id, label, and all properties).
    #[wasm_bindgen(js_name = "elementMap")]
    pub fn element_map(self) -> Traversal {
        self.add_step_with_type(traversal::ElementMapStep::new(), TraversalType::Value)
    }

    /// Get an element map with specific property keys.
    ///
    /// @param keys - Array of property names to include
    #[wasm_bindgen(js_name = "elementMapKeys")]
    pub fn element_map_keys(self, keys: JsValue) -> Result<Traversal, JsError> {
        let key_vec = js_array_to_strings(keys)?;
        Ok(self.add_step_with_type(
            traversal::ElementMapStep::with_keys(key_vec),
            TraversalType::Value,
        ))
    }

    /// Extract the element ID.
    pub fn id(self) -> Traversal {
        self.add_step_with_type(traversal::IdStep::new(), TraversalType::Value)
    }

    /// Extract the element label.
    pub fn label(self) -> Traversal {
        self.add_step_with_type(traversal::LabelStep::new(), TraversalType::Value)
    }

    /// Replace each element with a constant value.
    ///
    /// @param value - Constant value to emit
    pub fn constant(self, value: JsValue) -> Result<Traversal, JsError> {
        let v = js_to_value(value)?;
        Ok(self.add_step_with_type(traversal::ConstantStep::new(v), TraversalType::Value))
    }

    /// Flatten lists/iterables in the stream.
    pub fn unfold(self) -> Traversal {
        self.add_step_with_type(traversal::UnfoldStep::new(), TraversalType::Value)
    }

    /// Get the traversal path (history of elements visited).
    pub fn path(self) -> Traversal {
        self.add_step_with_type(traversal::PathStep::new(), TraversalType::Value)
    }

    /// Label the current step for later reference.
    ///
    /// @param label - Step label
    #[wasm_bindgen(js_name = "as")]
    pub fn as_(self, label: &str) -> Traversal {
        self.add_step(traversal::AsStep::new(label))
    }

    /// Select a single labeled step from the path.
    ///
    /// @param label - Step label
    #[wasm_bindgen(js_name = "selectOne")]
    pub fn select_one(self, label: &str) -> Traversal {
        self.add_step_with_type(traversal::SelectStep::single(label), TraversalType::Value)
    }

    /// Select labeled steps from the path.
    ///
    /// @param labels - Array of step labels to select
    pub fn select(self, labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(self.add_step_with_type(traversal::SelectStep::new(label_vec), TraversalType::Value))
    }

    /// Calculate the arithmetic mean of numeric values.
    pub fn mean(self) -> Traversal {
        self.add_step_with_type(traversal::MeanStep::new(), TraversalType::Value)
    }

    /// Get the minimum value.
    ///
    /// Finds the minimum among numeric (Int, Float) or string values.
    /// Non-comparable values are skipped.
    pub fn min(self) -> Traversal {
        self.add_step_with_type(traversal::MinStep::new(), TraversalType::Value)
    }

    /// Get the maximum value.
    ///
    /// Finds the maximum among numeric (Int, Float) or string values.
    /// Non-comparable values are skipped.
    pub fn max(self) -> Traversal {
        self.add_step_with_type(traversal::MaxStep::new(), TraversalType::Value)
    }

    /// Count the number of elements (as a step, not terminal).
    #[wasm_bindgen(js_name = "countStep")]
    pub fn count_step(self) -> Traversal {
        self.add_step_with_type(
            crate::traversal::aggregate::CountStep::new(),
            TraversalType::Value,
        )
    }

    // =========================================================================
    // Builder Steps (Order, Project, Group, Repeat)
    // =========================================================================

    /// Start an order operation.
    ///
    /// @returns OrderBuilder for specifying sort criteria
    ///
    /// @example
    /// ```typescript
    /// graph.V().order().byKeyDesc('age').build().toList();
    /// ```
    pub fn order(self) -> crate::wasm::builders::OrderBuilder {
        crate::wasm::builders::OrderBuilder::new(self)
    }

    /// Project each element into a map with named keys.
    ///
    /// @param keys - Output keys (as array of strings)
    /// @returns ProjectBuilder for specifying projections
    ///
    /// @example
    /// ```typescript
    /// graph.V()
    ///     .project(['name', 'friends'])
    ///     .byKey('name')
    ///     .byTraversal(__.out('knows').count())
    ///     .build()
    ///     .toList();
    /// ```
    pub fn project(self, keys: JsValue) -> Result<crate::wasm::builders::ProjectBuilder, JsError> {
        let key_vec = js_array_to_strings(keys)?;
        Ok(crate::wasm::builders::ProjectBuilder::new(self, key_vec))
    }

    /// Group elements into a map.
    ///
    /// @returns GroupBuilder for specifying key and value
    ///
    /// @example
    /// ```typescript
    /// graph.V().group().byKey('age').valuesByKey('name').build().toList();
    /// ```
    pub fn group(self) -> crate::wasm::builders::GroupBuilder {
        crate::wasm::builders::GroupBuilder::new(self)
    }

    /// Count elements by group.
    ///
    /// @returns GroupCountBuilder for specifying key
    ///
    /// @example
    /// ```typescript
    /// graph.V().groupCount().byLabel().build().toList();
    /// ```
    #[wasm_bindgen(js_name = "groupCount")]
    pub fn group_count(self) -> crate::wasm::builders::GroupCountBuilder {
        crate::wasm::builders::GroupCountBuilder::new(self)
    }

    /// Start a repeat loop.
    ///
    /// @param traversal - Traversal to repeat
    /// @returns RepeatBuilder for specifying termination
    ///
    /// @example
    /// ```typescript
    /// graph.V_(startId).repeat(__.out('knows')).times(3n).build().toList();
    /// ```
    pub fn repeat(self, sub: Traversal) -> crate::wasm::builders::RepeatBuilder {
        crate::wasm::builders::RepeatBuilder::new(self, sub)
    }

    // =========================================================================
    // Terminal Steps
    // =========================================================================

    /// Execute the traversal and return all results as an array.
    ///
    /// @returns Array of results
    #[wasm_bindgen(js_name = "toList")]
    pub fn to_list(&self) -> Result<JsValue, JsError> {
        let results = self.execute();
        values_to_js_array(results)
    }

    /// Execute and return the first result, or undefined.
    ///
    /// @returns First result or undefined
    pub fn first(&self) -> Result<JsValue, JsError> {
        let results = self.execute();
        match results.into_iter().next() {
            Some(v) => value_to_js(&v),
            None => Ok(JsValue::UNDEFINED),
        }
    }

    /// Execute and return exactly one result.
    ///
    /// @throws If zero or more than one result
    pub fn one(&self) -> Result<JsValue, JsError> {
        let results = self.execute();
        let count = results.len();
        if count == 1 {
            value_to_js(&results[0])
        } else {
            Err(JsError::new(&format!(
                "Expected exactly one result, got {}",
                count
            )))
        }
    }

    /// Execute and return the next result (for iteration).
    ///
    /// @returns Next result or undefined
    pub fn next(&self) -> Result<JsValue, JsError> {
        // For now, same as first() - true iteration would need state
        self.first()
    }

    /// Check if the traversal has any results.
    ///
    /// @returns true if at least one result exists
    #[wasm_bindgen(js_name = "hasNext")]
    pub fn has_next(&self) -> bool {
        !self.execute().is_empty()
    }

    /// Execute and return the count of results.
    ///
    /// @returns Number of results as bigint
    #[wasm_bindgen(js_name = "toCount")]
    pub fn to_count(&self) -> u64 {
        self.execute().len() as u64
    }

    /// Iterate through all results (for side effects).
    pub fn iterate(&self) {
        let _ = self.execute();
    }

    // =========================================================================
    // Mutation Steps
    // =========================================================================

    /// Add a vertex with a label.
    ///
    /// This is a spawning step - it creates a new vertex and returns it.
    ///
    /// @param label - The vertex label
    #[wasm_bindgen(js_name = "addV")]
    pub fn add_v(self, label: &str) -> Traversal {
        self.add_step_with_type(traversal::AddVStep::new(label), TraversalType::Vertex)
    }

    /// Add an edge with a label.
    ///
    /// Use from()/fromId() and to()/toId() to specify endpoints.
    ///
    /// @param label - The edge label
    #[wasm_bindgen(js_name = "addE")]
    pub fn add_e(self, label: &str) -> Traversal {
        // Use the WASM-specific AddESpawnStep that creates a pending marker
        // The endpoints will be filled in by fromId()/toId() steps
        self.add_step_with_type(AddESpawnStep::new(label), TraversalType::Edge)
    }

    /// Set a property on the current element.
    ///
    /// @param key - Property name
    /// @param value - Property value
    pub fn property(self, key: &str, value: JsValue) -> Result<Traversal, JsError> {
        let v = js_to_value(value)?;
        Ok(self.add_step(traversal::PropertyStep::new(key, v)))
    }

    /// Set the source vertex for an addE() traversal from a step label.
    ///
    /// @param label - Step label of source vertex
    pub fn from(self, label: &str) -> Traversal {
        // Modify the last step if it's an AddEStep
        let mut steps = self.steps;
        if let Some(last_step) = steps.pop() {
            // Reconstruct AddEStep with from_label
            // Since we can't downcast Box<dyn DynStep>, we'll use a new step that wraps it
            // For now, we'll just add a new step
            steps.push(last_step);
        }
        // Add a step that modifies pending add_e with from label
        let mut t = Self {
            graph: self.graph,
            source: self.source,
            steps,
            output_type: self.output_type,
        };
        t = t.add_step(AddEFromLabelStep::new(label));
        t
    }

    /// Set the source vertex for an addE() traversal by ID.
    ///
    /// @param id - Source vertex ID
    #[wasm_bindgen(js_name = "fromId")]
    pub fn from_id(self, id: JsValue) -> Result<Traversal, JsError> {
        let vid = js_to_vertex_id(id)?;
        Ok(self.add_step(AddEFromIdStep::new(vid)))
    }

    /// Set the target vertex for an addE() traversal from a step label.
    ///
    /// @param label - Step label of target vertex
    pub fn to(self, label: &str) -> Traversal {
        self.add_step(AddEToLabelStep::new(label))
    }

    /// Set the target vertex for an addE() traversal by ID.
    ///
    /// @param id - Target vertex ID
    #[wasm_bindgen(js_name = "toId")]
    pub fn to_id(self, id: JsValue) -> Result<Traversal, JsError> {
        let vid = js_to_vertex_id(id)?;
        Ok(self.add_step(AddEToIdStep::new(vid)))
    }

    /// Remove the current element from the graph.
    pub fn drop(self) -> Traversal {
        self.add_step(traversal::DropStep::new())
    }

    // =========================================================================
    // Aggregate Steps
    // =========================================================================

    /// Collect all elements into a single list.
    pub fn fold(self) -> Traversal {
        self.add_step_with_type(FoldStep::new(), TraversalType::Value)
    }

    /// Calculate the sum of numeric values.
    pub fn sum(self) -> Traversal {
        self.add_step_with_type(SumStep::new(), TraversalType::Value)
    }
}
