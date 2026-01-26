//! Traversal facade for WASM bindings.
//!
//! Provides a JavaScript-friendly traversal API with method chaining.

use std::sync::Arc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsError;

use crate::storage::cow::Graph as InnerGraph;
use crate::storage::GraphStorage;
use crate::traversal::context::SnapshotLike;
use crate::traversal::{self, DynStep, ExecutionContext, Traverser};
use crate::value::{EdgeId, Value, VertexId};
use crate::wasm::predicate::Predicate;
use crate::wasm::types::{
    js_array_to_strings, js_to_u64, js_to_value, js_to_vertex_id, value_to_js, values_to_js_array,
};

/// The type of elements in the traversal stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TraversalType {
    Vertex,
    Edge,
    Value,
}

/// Source type for initial traversal start.
#[derive(Clone)]
enum TraversalSource {
    AllVertices,
    VertexIds(Vec<VertexId>),
    AllEdges,
    EdgeIds(Vec<EdgeId>),
    Injected(Vec<Value>),
}

/// A graph traversal that can be chained with various steps.
///
/// Traversals are lazy - they only execute when a terminal step is called.
#[wasm_bindgen]
pub struct Traversal {
    /// The graph this traversal operates on
    graph: Arc<InnerGraph>,
    /// Source of initial traversers
    source: TraversalSource,
    /// Accumulated steps (type-erased)
    steps: Vec<Box<dyn DynStep>>,
    /// Current output type
    output_type: TraversalType,
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

    /// Execute the traversal and return results.
    fn execute(&self) -> Vec<Value> {
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
        };

        // Apply all steps using apply_dyn
        let mut current: Box<dyn Iterator<Item = Traverser> + '_> = initial;
        for step in &self.steps {
            current = step.apply_dyn(&ctx, current);
        }

        // Collect results
        current.map(|t| t.value).collect()
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
    /// Note: MinStep is not yet implemented - this returns self unchanged for now.
    pub fn min(self) -> Traversal {
        // TODO: MinStep not yet available in traversal module
        // For now, pass through unchanged
        self
    }

    /// Get the maximum value.
    ///
    /// Note: MaxStep is not yet implemented - this returns self unchanged for now.
    pub fn max(self) -> Traversal {
        // TODO: MaxStep not yet available in traversal module
        // For now, pass through unchanged
        self
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
}
