//! Anonymous traversal factory for WASM bindings.
//!
//! Provides the `__` (double underscore) namespace for creating anonymous traversals
//! that can be used with steps like `where()`, `union()`, `repeat()`, etc.
//!
//! # Example
//!
//! ```javascript
//! import { Graph, P, __ } from 'interstellar-graph';
//!
//! // Find people who have friends older than themselves
//! graph.V()
//!     .hasLabel('person')
//!     .as('p')
//!     .out('knows')
//!     .where(__.values('age').is(P.gt(__.select('p').values('age'))))
//!     .values('name')
//!     .toList();
//! ```

use wasm_bindgen::prelude::*;

use crate::traversal;
use crate::wasm::predicate::Predicate;
use crate::wasm::traversal::Traversal;
use crate::wasm::types::{js_array_to_strings, js_to_u64, js_to_value};

/// Anonymous traversal factory.
///
/// Creates traversal fragments for use in branch/filter steps.
/// The `__` namespace mirrors the Gremlin `__` class.
#[wasm_bindgen(js_name = "__")]
pub struct AnonymousFactory;

#[wasm_bindgen(js_class = "__")]
impl AnonymousFactory {
    // =========================================================================
    // Identity / Start
    // =========================================================================

    /// Start an anonymous traversal (identity - passes through input unchanged).
    #[wasm_bindgen(js_name = "identity")]
    pub fn identity() -> Traversal {
        Traversal::anonymous_with_step(traversal::IdentityStep)
    }

    /// Start an anonymous traversal (alias for identity).
    #[wasm_bindgen(js_name = "start")]
    pub fn start() -> Traversal {
        Self::identity()
    }

    // =========================================================================
    // Filter Steps
    // =========================================================================

    /// Filter to elements with a specific label.
    #[wasm_bindgen(js_name = "hasLabel")]
    pub fn has_label(label: &str) -> Traversal {
        Traversal::anonymous_with_step(traversal::HasLabelStep::single(label))
    }

    /// Filter to elements with any of the specified labels.
    #[wasm_bindgen(js_name = "hasLabelAny")]
    pub fn has_label_any(labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(Traversal::anonymous_with_step(
            traversal::HasLabelStep::any(label_vec),
        ))
    }

    /// Filter to elements that have a property (any value).
    pub fn has(key: &str) -> Traversal {
        Traversal::anonymous_with_step(traversal::HasStep::new(key))
    }

    /// Filter to elements that have a property with a specific value.
    #[wasm_bindgen(js_name = "hasValue")]
    pub fn has_value(key: &str, value: JsValue) -> Result<Traversal, JsError> {
        let v = js_to_value(value)?;
        Ok(Traversal::anonymous_with_step(
            traversal::HasValueStep::new(key, v),
        ))
    }

    /// Filter to elements where property matches a predicate.
    #[wasm_bindgen(js_name = "hasWhere")]
    pub fn has_where(key: &str, predicate: Predicate) -> Traversal {
        Traversal::anonymous_with_step(traversal::HasWhereStep::new(key, predicate.into_inner()))
    }

    /// Filter to elements that do NOT have a property.
    #[wasm_bindgen(js_name = "hasNot")]
    pub fn has_not(key: &str) -> Traversal {
        Traversal::anonymous_with_step(traversal::HasNotStep::new(key))
    }

    /// Filter values matching a predicate.
    #[wasm_bindgen(js_name = "is")]
    pub fn is_(predicate: Predicate) -> Traversal {
        Traversal::anonymous_with_step(traversal::IsStep::new(predicate.into_inner()))
    }

    /// Filter values equal to a specific value.
    #[wasm_bindgen(js_name = "isEq")]
    pub fn is_eq(value: JsValue) -> Result<Traversal, JsError> {
        let v = js_to_value(value)?;
        Ok(Traversal::anonymous_with_step(traversal::IsStep::eq(v)))
    }

    /// Remove duplicate elements from the traversal.
    pub fn dedup() -> Traversal {
        Traversal::anonymous_with_step(traversal::DedupStep::new())
    }

    /// Limit results to the first n elements.
    pub fn limit(n: JsValue) -> Result<Traversal, JsError> {
        let num = js_to_u64(n)?;
        Ok(Traversal::anonymous_with_step(traversal::LimitStep::new(
            num as usize,
        )))
    }

    /// Skip the first n elements.
    pub fn skip(n: JsValue) -> Result<Traversal, JsError> {
        let num = js_to_u64(n)?;
        Ok(Traversal::anonymous_with_step(traversal::SkipStep::new(
            num as usize,
        )))
    }

    /// Take elements in a range [start, end).
    pub fn range(start: JsValue, end: JsValue) -> Result<Traversal, JsError> {
        let s = js_to_u64(start)? as usize;
        let e = js_to_u64(end)? as usize;
        Ok(Traversal::anonymous_with_step(traversal::RangeStep::new(
            s, e,
        )))
    }

    /// Get the last element.
    pub fn tail() -> Traversal {
        Traversal::anonymous_with_step(traversal::TailStep::new(1))
    }

    /// Filter to paths that don't repeat vertices.
    #[wasm_bindgen(js_name = "simplePath")]
    pub fn simple_path() -> Traversal {
        Traversal::anonymous_with_step(traversal::SimplePathStep::new())
    }

    /// Filter to paths that do repeat vertices.
    #[wasm_bindgen(js_name = "cyclicPath")]
    pub fn cyclic_path() -> Traversal {
        Traversal::anonymous_with_step(traversal::CyclicPathStep::new())
    }

    // =========================================================================
    // Navigation Steps
    // =========================================================================

    /// Navigate to outgoing adjacent vertices.
    pub fn out() -> Traversal {
        Traversal::anonymous_with_step(traversal::OutStep::new())
    }

    /// Navigate to outgoing adjacent vertices via specific edge labels.
    #[wasm_bindgen(js_name = "outLabels")]
    pub fn out_labels(labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(Traversal::anonymous_with_step(
            traversal::OutStep::with_labels(label_vec),
        ))
    }

    /// Navigate to incoming adjacent vertices.
    #[wasm_bindgen(js_name = "in_")]
    pub fn in_() -> Traversal {
        Traversal::anonymous_with_step(traversal::InStep::new())
    }

    /// Navigate to incoming adjacent vertices via specific edge labels.
    #[wasm_bindgen(js_name = "inLabels")]
    pub fn in_labels(labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(Traversal::anonymous_with_step(
            traversal::InStep::with_labels(label_vec),
        ))
    }

    /// Navigate to adjacent vertices in both directions.
    pub fn both() -> Traversal {
        Traversal::anonymous_with_step(traversal::BothStep::new())
    }

    /// Navigate to adjacent vertices in both directions via specific labels.
    #[wasm_bindgen(js_name = "bothLabels")]
    pub fn both_labels(labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(Traversal::anonymous_with_step(
            traversal::BothStep::with_labels(label_vec),
        ))
    }

    /// Navigate to outgoing edges.
    #[wasm_bindgen(js_name = "outE")]
    pub fn out_e() -> Traversal {
        Traversal::anonymous_with_step(traversal::OutEStep::new())
    }

    /// Navigate to incoming edges.
    #[wasm_bindgen(js_name = "inE")]
    pub fn in_e() -> Traversal {
        Traversal::anonymous_with_step(traversal::InEStep::new())
    }

    /// Navigate to edges in both directions.
    #[wasm_bindgen(js_name = "bothE")]
    pub fn both_e() -> Traversal {
        Traversal::anonymous_with_step(traversal::BothEStep::new())
    }

    /// Navigate from an edge to its outgoing (source) vertex.
    #[wasm_bindgen(js_name = "outV")]
    pub fn out_v() -> Traversal {
        Traversal::anonymous_with_step(traversal::OutVStep::new())
    }

    /// Navigate from an edge to its incoming (target) vertex.
    #[wasm_bindgen(js_name = "inV")]
    pub fn in_v() -> Traversal {
        Traversal::anonymous_with_step(traversal::InVStep::new())
    }

    /// Navigate from an edge to both endpoints.
    #[wasm_bindgen(js_name = "bothV")]
    pub fn both_v() -> Traversal {
        Traversal::anonymous_with_step(traversal::BothVStep::new())
    }

    /// Navigate from an edge to the vertex that was NOT the previous step.
    #[wasm_bindgen(js_name = "otherV")]
    pub fn other_v() -> Traversal {
        Traversal::anonymous_with_step(traversal::OtherVStep::new())
    }

    // =========================================================================
    // Transform Steps
    // =========================================================================

    /// Extract a single property value.
    pub fn values(key: &str) -> Traversal {
        Traversal::anonymous_with_step(traversal::ValuesStep::new(key))
    }

    /// Get a map of property name to value.
    #[wasm_bindgen(js_name = "valueMap")]
    pub fn value_map() -> Traversal {
        Traversal::anonymous_with_step(traversal::ValueMapStep::new())
    }

    /// Get a complete element map (id, label, and all properties).
    #[wasm_bindgen(js_name = "elementMap")]
    pub fn element_map() -> Traversal {
        Traversal::anonymous_with_step(traversal::ElementMapStep::new())
    }

    /// Extract the element ID.
    pub fn id() -> Traversal {
        Traversal::anonymous_with_step(traversal::IdStep)
    }

    /// Extract the element label.
    pub fn label() -> Traversal {
        Traversal::anonymous_with_step(traversal::LabelStep)
    }

    /// Replace each element with a constant value.
    pub fn constant(value: JsValue) -> Result<Traversal, JsError> {
        let v = js_to_value(value)?;
        Ok(Traversal::anonymous_with_step(
            traversal::ConstantStep::new(v),
        ))
    }

    /// Flatten lists/iterables in the stream.
    pub fn unfold() -> Traversal {
        Traversal::anonymous_with_step(traversal::UnfoldStep::new())
    }

    /// Get the traversal path (history of elements visited).
    pub fn path() -> Traversal {
        Traversal::anonymous_with_step(traversal::PathStep::new())
    }

    /// Select a single labeled step from the path.
    #[wasm_bindgen(js_name = "selectOne")]
    pub fn select_one(label: &str) -> Traversal {
        Traversal::anonymous_with_step(traversal::SelectStep::single(label))
    }

    /// Select labeled steps from the path.
    pub fn select(labels: JsValue) -> Result<Traversal, JsError> {
        let label_vec = js_array_to_strings(labels)?;
        Ok(Traversal::anonymous_with_step(traversal::SelectStep::new(
            label_vec,
        )))
    }

    /// Calculate the arithmetic mean of numeric values.
    pub fn mean() -> Traversal {
        Traversal::anonymous_with_step(traversal::MeanStep::new())
    }

    /// Count the number of elements.
    pub fn count() -> Traversal {
        Traversal::anonymous_with_step(traversal::aggregate::CountStep::new())
    }

    // =========================================================================
    // Path/Label Steps
    // =========================================================================

    /// Label the current step for later reference.
    #[wasm_bindgen(js_name = "as")]
    pub fn as_(label: &str) -> Traversal {
        Traversal::anonymous_with_step(traversal::AsStep::new(label))
    }

    // =========================================================================
    // Aggregate Steps
    // =========================================================================

    /// Collect all elements into a single list.
    pub fn fold() -> Traversal {
        Traversal::anonymous_with_step(super::traversal::FoldStep::new())
    }

    /// Calculate the sum of numeric values.
    pub fn sum() -> Traversal {
        Traversal::anonymous_with_step(super::traversal::SumStep::new())
    }

    // =========================================================================
    // Mutation Steps
    // =========================================================================

    /// Add a vertex with a label.
    #[wasm_bindgen(js_name = "addV")]
    pub fn add_v(label: &str) -> Traversal {
        Traversal::anonymous_with_step(traversal::AddVStep::new(label))
    }

    /// Add an edge with a label.
    #[wasm_bindgen(js_name = "addE")]
    pub fn add_e(label: &str) -> Traversal {
        Traversal::anonymous_with_step(super::traversal::AddESpawnStep::new(label))
    }

    /// Set a property on the current element.
    pub fn property(key: &str, value: JsValue) -> Result<Traversal, JsError> {
        let v = js_to_value(value)?;
        Ok(Traversal::anonymous_with_step(
            traversal::PropertyStep::new(key, v),
        ))
    }

    /// Remove the current element from the graph.
    pub fn drop() -> Traversal {
        Traversal::anonymous_with_step(traversal::DropStep::new())
    }

    // =========================================================================
    // Branch Steps
    // =========================================================================

    /// Filter based on the result of a traversal.
    #[wasm_bindgen(js_name = "where_")]
    pub fn where_(sub: Traversal) -> Traversal {
        let t = Traversal::anonymous_with_step(traversal::IdentityStep);
        let core_traversal = sub.into_core_traversal();
        t.add_step_internal(traversal::WhereStep::new(core_traversal))
    }

    /// Filter to elements where the traversal produces NO results.
    pub fn not(sub: Traversal) -> Traversal {
        let t = Traversal::anonymous_with_step(traversal::IdentityStep);
        let core_traversal = sub.into_core_traversal();
        t.add_step_internal(traversal::NotStep::new(core_traversal))
    }

    /// Execute multiple traversals and combine results.
    pub fn union(traversals: Vec<Traversal>) -> Traversal {
        let t = Traversal::anonymous_with_step(traversal::IdentityStep);
        let core_traversals: Vec<_> = traversals
            .into_iter()
            .map(|tr| tr.into_core_traversal())
            .collect();
        t.add_step_internal(traversal::UnionStep::new(core_traversals))
    }

    /// Return the result of the first traversal that produces output.
    pub fn coalesce(traversals: Vec<Traversal>) -> Traversal {
        let t = Traversal::anonymous_with_step(traversal::IdentityStep);
        let core_traversals: Vec<_> = traversals
            .into_iter()
            .map(|tr| tr.into_core_traversal())
            .collect();
        t.add_step_internal(traversal::CoalesceStep::new(core_traversals))
    }

    /// Conditional branching.
    pub fn choose(condition: Traversal, if_true: Traversal, if_false: Traversal) -> Traversal {
        let t = Traversal::anonymous_with_step(traversal::IdentityStep);
        let cond = condition.into_core_traversal();
        let t_branch = if_true.into_core_traversal();
        let f_branch = if_false.into_core_traversal();
        t.add_step_internal(traversal::ChooseStep::new(cond, t_branch, f_branch))
    }

    /// Execute traversal, but pass through original if no results.
    pub fn optional(sub: Traversal) -> Traversal {
        let t = Traversal::anonymous_with_step(traversal::IdentityStep);
        let core_traversal = sub.into_core_traversal();
        t.add_step_internal(traversal::OptionalStep::new(core_traversal))
    }

    /// Execute traversal in local scope (per element).
    pub fn local(sub: Traversal) -> Traversal {
        let t = Traversal::anonymous_with_step(traversal::IdentityStep);
        let core_traversal = sub.into_core_traversal();
        t.add_step_internal(traversal::LocalStep::new(core_traversal))
    }
}
