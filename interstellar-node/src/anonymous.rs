//! Anonymous traversal factory for napi-rs bindings.
//!
//! Provides the `__` (double underscore) namespace for creating anonymous traversals
//! that can be used with steps like `where()`, `union()`, `repeat()`, etc.
//!
//! # Example
//!
//! ```javascript
//! import { Graph, P, __ } from '@interstellar/node';
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

use napi::bindgen_prelude::*;
use napi::JsUnknown;
use napi_derive::napi;

use interstellar::traversal;

use crate::predicate::JsPredicate;
use crate::traversal::{CountStep, FoldStep, JsTraversal, SumStep};
use crate::value::{js_array_to_strings, js_to_value};

/// Anonymous traversal factory.
///
/// Creates traversal fragments for use in branch/filter steps.
/// The `__` namespace mirrors the Gremlin `__` class.
#[napi(js_name = "__")]
pub struct AnonymousFactory;

#[napi]
impl AnonymousFactory {
    // =========================================================================
    // Identity / Start
    // =========================================================================

    /// Start an anonymous traversal (identity - passes through input unchanged).
    #[napi]
    pub fn identity() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::IdentityStep)
    }

    /// Start an anonymous traversal (alias for identity).
    #[napi]
    pub fn start() -> JsTraversal {
        Self::identity()
    }

    // =========================================================================
    // Filter Steps
    // =========================================================================

    /// Filter to elements with a specific label.
    #[napi(js_name = "hasLabel")]
    pub fn has_label(label: String) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::HasLabelStep::single(&label))
    }

    /// Filter to elements with any of the specified labels.
    #[napi(js_name = "hasLabelAny")]
    pub fn has_label_any(env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        Ok(JsTraversal::anonymous_with_step(
            traversal::HasLabelStep::any(label_vec),
        ))
    }

    /// Filter to elements that have a property (any value).
    #[napi]
    pub fn has(key: String) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::HasStep::new(&key))
    }

    /// Filter to elements that have a property with a specific value.
    #[napi(js_name = "hasValue")]
    pub fn has_value(env: Env, key: String, value: JsUnknown) -> Result<JsTraversal> {
        let v = js_to_value(env, value)?;
        Ok(JsTraversal::anonymous_with_step(
            traversal::HasValueStep::new(&key, v),
        ))
    }

    /// Filter to elements where property matches a predicate.
    #[napi(js_name = "hasWhere")]
    pub fn has_where(key: String, predicate: &JsPredicate) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::HasWhereStep::new(
            &key,
            predicate.inner.clone(),
        ))
    }

    /// Filter to elements that do NOT have a property.
    #[napi(js_name = "hasNot")]
    pub fn has_not(key: String) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::HasNotStep::new(&key))
    }

    /// Filter values matching a predicate.
    #[napi(js_name = "is")]
    pub fn is_(predicate: &JsPredicate) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::IsStep::new(predicate.inner.clone()))
    }

    /// Filter values equal to a specific value.
    #[napi(js_name = "isEq")]
    pub fn is_eq(env: Env, value: JsUnknown) -> Result<JsTraversal> {
        let v = js_to_value(env, value)?;
        Ok(JsTraversal::anonymous_with_step(traversal::IsStep::eq(v)))
    }

    /// Remove duplicate elements from the traversal.
    #[napi]
    pub fn dedup() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::DedupStep::new())
    }

    /// Limit results to the first n elements.
    #[napi]
    pub fn limit(n: u32) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::LimitStep::new(n as usize))
    }

    /// Skip the first n elements.
    #[napi]
    pub fn skip(n: u32) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::SkipStep::new(n as usize))
    }

    /// Take elements in a range [start, end).
    #[napi]
    pub fn range(start: u32, end: u32) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::RangeStep::new(start as usize, end as usize))
    }

    // =========================================================================
    // Navigation Steps
    // =========================================================================

    /// Navigate to outgoing adjacent vertices.
    #[napi]
    pub fn out() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::OutStep::new())
    }

    /// Navigate to outgoing adjacent vertices via specific edge labels.
    #[napi(js_name = "outLabels")]
    pub fn out_labels(env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        Ok(JsTraversal::anonymous_with_step(
            traversal::OutStep::with_labels(label_vec),
        ))
    }

    /// Navigate to incoming adjacent vertices.
    #[napi(js_name = "in_")]
    pub fn in_() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::InStep::new())
    }

    /// Navigate to incoming adjacent vertices via specific edge labels.
    #[napi(js_name = "inLabels")]
    pub fn in_labels(env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        Ok(JsTraversal::anonymous_with_step(
            traversal::InStep::with_labels(label_vec),
        ))
    }

    /// Navigate to adjacent vertices in both directions.
    #[napi]
    pub fn both() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::BothStep::new())
    }

    /// Navigate to adjacent vertices in both directions via specific labels.
    #[napi(js_name = "bothLabels")]
    pub fn both_labels(env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        Ok(JsTraversal::anonymous_with_step(
            traversal::BothStep::with_labels(label_vec),
        ))
    }

    /// Navigate to outgoing edges.
    #[napi(js_name = "outE")]
    pub fn out_e() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::OutEStep::new())
    }

    /// Navigate to incoming edges.
    #[napi(js_name = "inE")]
    pub fn in_e() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::InEStep::new())
    }

    /// Navigate to edges in both directions.
    #[napi(js_name = "bothE")]
    pub fn both_e() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::BothEStep::new())
    }

    /// Navigate from an edge to its outgoing (source) vertex.
    #[napi(js_name = "outV")]
    pub fn out_v() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::OutVStep::new())
    }

    /// Navigate from an edge to its incoming (target) vertex.
    #[napi(js_name = "inV")]
    pub fn in_v() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::InVStep::new())
    }

    /// Navigate from an edge to both endpoints.
    #[napi(js_name = "bothV")]
    pub fn both_v() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::BothVStep::new())
    }

    /// Navigate from an edge to the vertex that was NOT the previous step.
    #[napi(js_name = "otherV")]
    pub fn other_v() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::OtherVStep::new())
    }

    // =========================================================================
    // Transform Steps
    // =========================================================================

    /// Extract a single property value.
    #[napi]
    pub fn values(key: String) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::ValuesStep::new(&key))
    }

    /// Get a map of property name to value.
    #[napi(js_name = "valueMap")]
    pub fn value_map() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::ValueMapStep::new())
    }

    /// Get a complete element map (id, label, and all properties).
    #[napi(js_name = "elementMap")]
    pub fn element_map() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::ElementMapStep::new())
    }

    /// Extract the element ID.
    #[napi]
    pub fn id() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::IdStep::new())
    }

    /// Extract the element label.
    #[napi]
    pub fn label() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::LabelStep::new())
    }

    /// Replace each element with a constant value.
    #[napi]
    pub fn constant(env: Env, value: JsUnknown) -> Result<JsTraversal> {
        let v = js_to_value(env, value)?;
        Ok(JsTraversal::anonymous_with_step(
            traversal::ConstantStep::new(v),
        ))
    }

    /// Flatten lists in the stream.
    #[napi]
    pub fn unfold() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::UnfoldStep::new())
    }

    /// Collect all elements into a single list.
    #[napi]
    pub fn fold() -> JsTraversal {
        JsTraversal::anonymous_with_step(FoldStep::new())
    }

    /// Get the traversal path (history of elements visited).
    #[napi]
    pub fn path() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::PathStep::new())
    }

    /// Label the current step for later reference.
    #[napi(js_name = "as")]
    pub fn as_(label: String) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::AsStep::new(&label))
    }

    /// Select labeled steps from the path.
    #[napi]
    pub fn select(env: Env, labels: Option<JsUnknown>) -> Result<JsTraversal> {
        let label_vec = js_array_to_strings(env, labels)?;
        Ok(JsTraversal::anonymous_with_step(
            traversal::SelectStep::new(label_vec),
        ))
    }

    /// Count the number of elements.
    #[napi(js_name = "count_")]
    pub fn count_step() -> JsTraversal {
        JsTraversal::anonymous_with_step(CountStep::new())
    }

    /// Calculate the sum of numeric values.
    #[napi(js_name = "sum")]
    pub fn sum_step() -> JsTraversal {
        JsTraversal::anonymous_with_step(SumStep::new())
    }

    /// Calculate the arithmetic mean of numeric values.
    #[napi(js_name = "mean")]
    pub fn mean_step() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::MeanStep::new())
    }

    /// Get the minimum value.
    #[napi(js_name = "min")]
    pub fn min_step() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::MinStep::new())
    }

    /// Get the maximum value.
    #[napi(js_name = "max")]
    pub fn max_step() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::MaxStep::new())
    }

    // =========================================================================
    // Order Steps
    // =========================================================================

    /// Order by natural value (ascending).
    #[napi(js_name = "orderAsc")]
    pub fn order_asc() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::OrderStep::by_natural(traversal::Order::Asc))
    }

    /// Order by natural value (descending).
    #[napi(js_name = "orderDesc")]
    pub fn order_desc() -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::OrderStep::by_natural(traversal::Order::Desc))
    }

    // =========================================================================
    // Branch Steps
    // =========================================================================

    /// Filter with a sub-traversal condition.
    #[napi(js_name = "where")]
    pub fn where_(traversal: &JsTraversal) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::WhereStep::new(
            traversal.clone().into_core_traversal(),
        ))
    }

    /// Negate a filter condition.
    #[napi]
    pub fn not(traversal: &JsTraversal) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::NotStep::new(
            traversal.clone().into_core_traversal(),
        ))
    }

    /// Execute multiple traversals and combine results.
    #[napi]
    pub fn union(traversals: Vec<&JsTraversal>) -> JsTraversal {
        let core_traversals: Vec<_> = traversals
            .into_iter()
            .map(|t| t.clone().into_core_traversal())
            .collect();
        JsTraversal::anonymous_with_step(traversal::UnionStep::new(core_traversals))
    }

    /// Return the result of the first traversal that produces output.
    #[napi]
    pub fn coalesce(traversals: Vec<&JsTraversal>) -> JsTraversal {
        let core_traversals: Vec<_> = traversals
            .into_iter()
            .map(|t| t.clone().into_core_traversal())
            .collect();
        JsTraversal::anonymous_with_step(traversal::CoalesceStep::new(core_traversals))
    }

    /// Execute traversal, but pass through original if no results.
    #[napi]
    pub fn optional(traversal: &JsTraversal) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::OptionalStep::new(
            traversal.clone().into_core_traversal(),
        ))
    }

    /// Execute traversal in local scope (per element).
    #[napi]
    pub fn local(traversal: &JsTraversal) -> JsTraversal {
        JsTraversal::anonymous_with_step(traversal::LocalStep::new(
            traversal.clone().into_core_traversal(),
        ))
    }
}
