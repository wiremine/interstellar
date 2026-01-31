//! Anonymous traversal factory module.
//!
//! The `__` module provides factory functions for creating anonymous traversals.
//! Anonymous traversals are unbound traversal fragments that receive their input
//! at execution time when spliced into a parent traversal.
//!
//! # Naming Convention
//!
//! The double underscore `__` is a Gremlin convention that clearly distinguishes
//! anonymous traversal fragments from bound traversals that start from `g.v()` or `g.e()`.
//!
//! # Usage
//!
//! Anonymous traversals are used with steps that accept sub-traversals:
//! - `where_()` - Filter based on sub-traversal existence
//! - `union()` - Execute multiple branches and merge results
//! - `coalesce()` - Try branches until one succeeds
//! - `choose()` - Conditional branching
//! - `repeat()` - Iterative traversal
//!
//! # Example
//!
//! ```ignore
//! use interstellar::traversal::__;
//!
//! // Create an anonymous traversal
//! let knows_bob = __.out_labels(&["knows"]).has_value("name", "Bob");
//!
//! // Use in a parent traversal
//! let people_who_know_bob = g.v()
//!     .has_label("person")
//!     .where_(knows_bob)
//!     .to_list();
//!
//! // Factory functions can also be chained
//! let complex = __.out()
//!     .has_label("person")
//!     .values("name");
//! ```
//!
//! # Return Type
//!
//! All factory functions return `Traversal<Value, Value>`, making them
//! composable with any parent traversal expecting `Value` input.

use crate::traversal::context::ExecutionContext;
use crate::traversal::filter::{
    CoinStep, DedupByKeyStep, DedupByLabelStep, DedupByTraversalStep, DedupStep, FilterStep,
    HasIdStep, HasKeyStep, HasLabelStep, HasNotStep, HasPropValueStep, HasStep, HasValueStep,
    HasWhereStep, LimitStep, RangeStep, SampleStep, SkipStep, TailStep, WherePStep,
};
use crate::traversal::navigation::{
    BothEStep, BothStep, BothVStep, InEStep, InStep, InVStep, OtherVStep, OutEStep, OutStep,
    OutVStep,
};
use crate::traversal::pipeline::Traversal;
use crate::traversal::predicate::Predicate;
use crate::traversal::step::IdentityStep;
use crate::traversal::transform::{
    AsStep, ConstantStep, ElementMapStep, FlatMapStep, IdStep, IndexStep, KeyStep, LabelStep,
    LoopsStep, MapStep, OrderBuilder, PathStep, ProjectBuilder, PropertiesStep, PropertyMapStep,
    SelectStep, UnfoldStep, ValueMapStep, ValueStep, ValuesStep,
};
use crate::value::Value;

// -------------------------------------------------------------------------
// Identity
// -------------------------------------------------------------------------

/// Create an identity traversal that passes input through unchanged.
///
/// # Example
///
/// ```ignore
/// let anon = __.identity();
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
/// let friends = __.out();
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
/// let friends = __.out_labels(&["knows", "likes"]);
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
/// let known_by = __.in_();
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
/// let known_by = __.in_labels(&["knows"]);
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
/// let neighbors = __.both();
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
/// let connected = __.both_labels(&["knows"]);
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
/// let edges = __.out_e();
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
/// let knows_edges = __.out_e_labels(&["knows"]);
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
/// let edges = __.in_e();
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
/// let known_by_edges = __.in_e_labels(&["knows"]);
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
/// let all_edges = __.both_e();
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
/// let knows_edges = __.both_e_labels(&["knows"]);
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
/// let sources = __.out_v();
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
/// let targets = __.in_v();
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
/// let endpoints = __.both_v();
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
/// let others = __.other_v();
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
/// let people = __.has_label("person");
/// ```
pub fn has_label(label: impl Into<String>) -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(HasLabelStep::single(label))
}

/// Filter elements by any of the given labels.
///
/// # Example
///
/// ```ignore
/// let entities = __.has_label_any(&["person", "company"]);
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
/// let with_age = __.has("age");
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
/// let without_email = __.has_not("email");
/// ```
pub fn has_not(key: impl Into<String>) -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(HasNotStep::new(key))
}

/// Filter elements by property value equality.
///
/// # Example
///
/// ```ignore
/// let alice = __.has_value("name", "Alice");
/// ```
pub fn has_value(key: impl Into<String>, value: impl Into<Value>) -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(HasValueStep::new(key, value))
}

/// Filter elements by ID.
///
/// # Example
///
/// ```ignore
/// let specific = __.has_id(VertexId(1));
/// ```
pub fn has_id(id: impl Into<Value>) -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(HasIdStep::from_value(id))
}

/// Filter elements by multiple IDs.
///
/// # Example
///
/// ```ignore
/// let specific = __.has_ids([VertexId(1), VertexId(2)]);
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
/// use interstellar::traversal::p;
///
/// // Filter to adults
/// let adults = __.has_where("age", p::gte(18));
///
/// // Filter names starting with "A"
/// let a_names = __.has_where("name", p::starting_with("A"));
///
/// // Combine predicates
/// let working_age = __.has_where("age", p::and(p::gte(18), p::lt(65)));
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
/// use interstellar::traversal::{__, p};
///
/// // Filter ages greater than 25
/// let gt_25 = __.is_(p::gt(25));
/// let adults = g.v().values("age").append(gt_25).to_list();
///
/// // Filter ages in a range
/// let in_range = __.is_(p::between(20, 40));
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
/// use interstellar::traversal::__;
///
/// // Filter to ages equal to 29
/// let age_29 = __.is_eq(29);
/// let results = g.v().values("age").append(age_29).to_list();
///
/// // Filter to a specific name
/// let alice = __.is_eq("Alice");
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
/// let positive = __.filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 0));
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
/// let unique = __.dedup();
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
/// let unique_ages = __.dedup_by_key("age");
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
/// let one_per_label = __.dedup_by_label();
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
/// let unique_outdegree = __.dedup_by(__.out().count());
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
/// let first_ten = __.limit(10);
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
/// let after_ten = __.skip(10);
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
/// let page = __.range(10, 20);
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
/// let simple = __.simple_path();
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
/// let cyclic = __.cyclic_path();
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
/// let last = __.tail();
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
/// let last_three = __.tail_n(3);
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
/// let sample = __.coin(0.5);
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
/// let sampled = __.sample(5);
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
/// let names = __.has_key("name");
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
/// let props = __.has_key_any(["name", "age"]);
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
/// let alice_props = __.has_prop_value("Alice");
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
/// let props = __.has_prop_value_any(["Alice", "Bob"]);
/// ```
#[inline]
pub fn has_prop_value_any<I, V>(values: I) -> Traversal<Value, Value>
where
    I: IntoIterator<Item = V>,
    V: Into<Value>,
{
    Traversal::<Value, Value>::new().add_step(HasPropValueStep::any(values))
}

/// Filter traversers by testing their current value against a predicate.
///
/// This step is the predicate-based variant of `where()`, complementing the
/// traversal-based `where_(traversal)` step.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::p;
///
/// // Filter values greater than 25
/// let adults = __.where_p(p::gt(25));
///
/// // Filter values within a set
/// let selected = __.where_p(p::within(["Alice", "Bob"]));
/// ```
#[inline]
pub fn where_p(
    predicate: impl crate::traversal::predicate::Predicate + 'static,
) -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(WherePStep::new(predicate))
}

// -------------------------------------------------------------------------
// Transform Steps
// -------------------------------------------------------------------------

/// Extract property values.
///
/// # Example
///
/// ```ignore
/// let names = __.values("name");
/// ```
pub fn values(key: impl Into<String>) -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(ValuesStep::new(key))
}

/// Extract multiple property values.
///
/// # Example
///
/// ```ignore
/// let data = __.values_multi(["name", "age"]);
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
/// let props = __.properties();
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
/// let props = __.properties_keys(&["name", "age"]);
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
/// let maps = __.value_map();
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
/// let maps = __.value_map_keys(&["name"]);
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
/// let maps = __.value_map_with_tokens();
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
/// let maps = __.element_map();
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
/// let maps = __.element_map_keys(&["name"]);
/// // Returns: {"id": 0, "label": "person", "name": "Alice"}
/// ```
pub fn element_map_keys(keys: &[&str]) -> Traversal<Value, Value> {
    let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
    Traversal::<Value, Value>::new().add_step(ElementMapStep::with_keys(keys))
}

/// Get all properties as a map of property objects.
///
/// Transforms each element into a `Value::Map` where keys are property names
/// and values are lists of property objects (maps with "key" and "value" entries).
///
/// # Difference from valueMap
///
/// - `value_map()`: Returns `{name: ["Alice"], age: [30]}` (just values in lists)
/// - `property_map()`: Returns `{name: [{key: "name", value: "Alice"}], age: [{key: "age", value: 30}]}` (property objects in lists)
///
/// # Example
///
/// ```ignore
/// let maps = __.property_map();
/// // Returns: {name: [{key: "name", value: "Alice"}], age: [{key: "age", value: 30}]}
/// ```
#[inline]
pub fn property_map() -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(PropertyMapStep::new())
}

/// Get specific properties as a map of property objects.
///
/// Like `property_map()`, but includes only the specified properties.
///
/// # Example
///
/// ```ignore
/// let maps = __.property_map_keys(&["name"]);
/// // Returns: {name: [{key: "name", value: "Alice"}]}
/// ```
pub fn property_map_keys(keys: &[&str]) -> Traversal<Value, Value> {
    let keys: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
    Traversal::<Value, Value>::new().add_step(PropertyMapStep::with_keys(keys))
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
/// let unfolded = __.unfold();
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
/// let avg = __.mean();
///
/// // As part of a larger traversal
/// let avg_ages = g.v().has_label("person")
///     .values("age")
///     .append(__.mean())
///     .to_list();
/// ```
#[inline]
pub fn mean() -> Traversal<Value, Value> {
    use crate::traversal::transform::MeanStep;
    Traversal::<Value, Value>::new().add_step(MeanStep::new())
}

/// Collect all traversers into a single list value.
///
/// This is a **barrier step** - it collects ALL input before producing
/// a single `Value::List` containing all collected values.
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().out().fold()  // Collect all outgoing vertices into a list
/// ```
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Fold all values into a list
/// let folded = __.fold();
///
/// // Use with project to count and collect
/// let t = g.v().out().fold()
///     .project(&["count", "items"])
///     .by(__.count_local())
///     .by(__.identity())
///     .build();
/// ```
#[inline]
pub fn fold() -> Traversal<Value, Value> {
    use crate::traversal::transform::FoldStep;
    Traversal::<Value, Value>::new().add_step(FoldStep::new())
}

/// Sum all numeric input values.
///
/// This is a **barrier step** - it collects ALL input before producing
/// the sum as a single `Value::Int` or `Value::Float`.
///
/// # Behavior
///
/// - Sums all numeric values (`Value::Int` and `Value::Float`)
/// - Non-numeric values are silently ignored
/// - If all inputs are integers, returns `Value::Int`
/// - If any input is a float, returns `Value::Float`
/// - Empty input returns `Value::Int(0)`
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().values("age").sum()  // Sum all ages
/// ```
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Sum numeric values
/// let total = __.sum();
/// ```
#[inline]
pub fn sum() -> Traversal<Value, Value> {
    use crate::traversal::transform::SumStep;
    Traversal::<Value, Value>::new().add_step(SumStep::new())
}

/// Count elements within each collection value (local scope).
///
/// Unlike the global `count()` which counts traversers in the stream,
/// `count_local()` counts elements *within* each traverser's collection value.
/// This implements Gremlin's `count(local)` semantics.
///
/// # Behavior
///
/// - `Value::List`: Returns the number of elements in the list
/// - `Value::Map`: Returns the number of entries in the map
/// - `Value::String`: Returns the length of the string
/// - Other values: Returns 1
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().out().fold().count(local)  // Count items in each folded list
/// ```
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Count elements in a folded list
/// let count = __.count_local();
/// ```
#[inline]
pub fn count_local() -> Traversal<Value, Value> {
    use crate::traversal::transform::CountLocalStep;
    Traversal::<Value, Value>::new().add_step(CountLocalStep::new())
}

/// Sum elements within each collection value (local scope).
///
/// Unlike the global `sum()` which sums across all traversers,
/// `sum_local()` sums elements *within* each traverser's collection value.
/// This implements Gremlin's `sum(local)` semantics.
///
/// # Behavior
///
/// - `Value::List`: Sums all numeric elements in the list
/// - `Value::Int`/`Value::Float`: Returns the value unchanged
/// - Other values: Returns 0
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().values("scores").fold().sum(local)  // Sum scores within each list
/// ```
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Sum elements in a folded list
/// let total = __.sum_local();
/// ```
#[inline]
pub fn sum_local() -> Traversal<Value, Value> {
    use crate::traversal::transform::SumLocalStep;
    Traversal::<Value, Value>::new().add_step(SumLocalStep::new())
}

/// Extract keys from Map values.
///
/// For each traverser with a Map value, extracts the keys.
/// Single-entry maps return the key directly; multi-entry maps
/// return a List of keys. Non-Map values are filtered out.
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().group().by(label).unfold().select(keys)
/// ```
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Get group keys after grouping
/// let keys = __.select_keys();
/// ```
#[inline]
pub fn select_keys() -> Traversal<Value, Value> {
    use crate::traversal::transform::SelectKeysStep;
    Traversal::<Value, Value>::new().add_step(SelectKeysStep::new())
}

/// Extract values from Map values.
///
/// For each traverser with a Map value, extracts the values.
/// Single-entry maps return the value directly; multi-entry maps
/// return a List of values. Non-Map values are filtered out.
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().group().by(label).unfold().select(values)
/// ```
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Get group values after grouping
/// let values = __.select_values();
/// ```
#[inline]
pub fn select_values() -> Traversal<Value, Value> {
    use crate::traversal::transform::SelectValuesStep;
    Traversal::<Value, Value>::new().add_step(SelectValuesStep::new())
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
/// let sorted = __.order().build();
///
/// // Sort by property
/// let sorted = __.order().by_key_desc("age").build();
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
/// use interstellar::traversal::__;
///
/// // Double current values
/// let doubled = __.math("_ * 2").build();
///
/// // Calculate square root of sum
/// let sqrt = __.math("sqrt(_ ^ 2 + 1)").build();
///
/// // With labeled path values (requires by() for each variable)
/// let diff = __.math("a - b")
///     .by("a", "age")
///     .by("b", "age")
///     .build();
/// ```
#[cfg(feature = "gql")]
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
/// let projection = __.project(&["name", "friend_count"])
///     .by_key("name")
///     .by(__.out("knows").count())
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
/// let groups = __.group().by_label().by_value().build();
///
/// // Group by property
/// let groups = __.group().by_key("age").by_value_key("name").build();
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
/// use interstellar::traversal::__;
///
/// // Count by label
/// let count_step = __.group_count().by_label().build();
///
/// // Count by property
/// let age_count_step = __.group_count().by_key("age").build();
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
/// let ids = __.id();
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
/// let labels = __.label();
/// ```
#[inline]
pub fn label() -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(LabelStep::new())
}

/// Extract the key from property map objects.
///
/// # Example
///
/// ```ignore
/// let keys = __.key();
/// ```
#[inline]
pub fn key() -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(KeyStep::new())
}

/// Extract the value from property map objects.
///
/// # Example
///
/// ```ignore
/// let values = __.value();
/// ```
#[inline]
pub fn value() -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(ValueStep::new())
}

/// Extract the current loop depth.
///
/// # Example
///
/// ```ignore
/// // Use in until condition
/// let vertices = g.v()
///     .repeat(__.out())
///     .until(__.loops().is_(p::gte(3)))
///     .to_list();
/// ```
#[inline]
pub fn loops() -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(LoopsStep::new())
}

/// Annotate each element with its position index.
///
/// # Example
///
/// ```ignore
/// // Get elements with indices
/// let indexed = g.v()
///     .flat_map(__.index())
///     .to_list();
/// ```
#[inline]
pub fn index() -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(IndexStep::new())
}

/// Replace values with a constant.
///
/// # Example
///
/// ```ignore
/// let markers = __.constant("found");
/// ```
pub fn constant(value: impl Into<Value>) -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(ConstantStep::new(value))
}

/// Convert the path to a list.
///
/// # Example
///
/// ```ignore
/// let paths = __.path();
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
/// let doubled = __.map(|_ctx, v| {
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
/// let expanded = __.flat_map(|_ctx, v| {
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
/// let labeled = __.as_("start");
/// ```
pub fn as_(label: &str) -> Traversal<Value, Value> {
    Traversal::<Value, Value>::new().add_step(AsStep::new(label))
}

/// Select multiple labeled values from the path.
///
/// # Example
///
/// ```ignore
/// let selected = __.select(&["a", "b"]);
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
/// let selected = __.select_one("start");
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
/// use interstellar::traversal::__;
///
/// // Keep only vertices that have outgoing edges
/// let with_out = __.where_(__.out());
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
/// use interstellar::traversal::__;
///
/// // Keep only leaf vertices (no outgoing edges)
/// let leaves = __.not(__.out());
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
/// use interstellar::traversal::__;
///
/// // Keep vertices that have both outgoing AND incoming edges
/// let connected = __.and_(vec![__.out(), __.in_()]);
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
/// use interstellar::traversal::__;
///
/// // Keep vertices that are either "person" OR "software"
/// let entities = __.or_(vec![__.has_label("person"), __.has_label("software")]);
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
/// use interstellar::traversal::__;
///
/// // Get neighbors in both directions
/// let neighbors = __.union(vec![__.out(), __.in_()]);
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
/// use interstellar::traversal::__;
///
/// // Try to get nickname, fall back to name
/// let names = __.coalesce(vec![__.values("nickname"), __.values("name")]);
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
/// use interstellar::traversal::__;
///
/// // If person, get friends; otherwise get all neighbors
/// let results = __.choose(__.has_label("person"), __.out_labels(&["knows"]), __.out());
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
/// use interstellar::traversal::__;
///
/// // Try to traverse to friends, keep original if none found
/// let results = __.optional(__.out_labels(&["knows"]));
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
/// use interstellar::traversal::__;
///
/// // Count neighbors per vertex
/// let counts = __.local(__.out().limit(1));
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
/// use interstellar::traversal::__;
///
/// // Create a pending vertex (actual creation happens at execution time)
/// let vertex_traversal = __.add_v("person")
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
/// use interstellar::traversal::__;
/// use interstellar::value::VertexId;
///
/// // Create a pending edge between two vertices
/// let edge_step = __.add_e("knows")
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
/// use interstellar::traversal::__;
///
/// // Add a property to current element
/// let with_name = __.property("name", "Alice");
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
/// use interstellar::traversal::__;
///
/// // Mark elements for deletion
/// let deleted = __.drop();
/// ```
pub fn drop() -> Traversal<Value, Value> {
    use crate::traversal::mutation::DropStep;
    Traversal::<Value, Value>::new().add_step(DropStep::new())
}

// -------------------------------------------------------------------------
// Branch Steps
// -------------------------------------------------------------------------

/// Create a branch step for anonymous traversals.
///
/// This creates a `Traversal` with a `BranchStep` that evaluates the given
/// branch traversal for each input and routes to option branches based on
/// the resulting key.
///
/// Note: This returns a traversal with a BranchStep that has no options.
/// For full branch/option functionality in anonymous traversals, you typically
/// configure options when using `BoundTraversal::branch()` instead.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Create a basic branch step (options added via bound traversal)
/// let branch_traversal = __.branch(__.label());
/// ```
pub fn branch(branch_traversal: Traversal<Value, Value>) -> Traversal<Value, Value> {
    use crate::traversal::branch::BranchStep;
    Traversal::<Value, Value>::new().add_step(BranchStep::new(branch_traversal))
}

// -------------------------------------------------------------------------
// Side Effect Steps
// -------------------------------------------------------------------------

/// Store traverser values in a side-effect collection.
///
/// This is a **lazy step** - values are stored as they pass through the iterator,
/// not all at once. The traverser values pass through unchanged.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Store values as they pass through
/// let stored = __.store("x");
/// ```
pub fn store(key: impl Into<String>) -> Traversal<Value, Value> {
    use crate::traversal::sideeffect::StoreStep;
    Traversal::<Value, Value>::new().add_step(StoreStep::new(key))
}

/// Aggregate all traverser values into a side-effect collection.
///
/// This is a **barrier step** - it collects ALL values before continuing.
/// All input traversers are collected, stored, then re-emitted.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Aggregate all values
/// let aggregated = __.aggregate("all");
/// ```
pub fn aggregate(key: impl Into<String>) -> Traversal<Value, Value> {
    use crate::traversal::sideeffect::AggregateStep;
    Traversal::<Value, Value>::new().add_step(AggregateStep::new(key))
}

/// Retrieve side-effect data by key.
///
/// For a single key, returns the collection as a `Value::List`.
/// Consumes all input traversers before producing the result.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Retrieve stored data
/// let capped = __.cap("x");
/// ```
pub fn cap(key: impl Into<String>) -> Traversal<Value, Value> {
    use crate::traversal::sideeffect::CapStep;
    Traversal::<Value, Value>::new().add_step(CapStep::new(key))
}

/// Execute a sub-traversal for its side effects.
///
/// The sub-traversal is executed for each input traverser, but its output
/// is discarded. The original traverser passes through unchanged.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Execute side effect traversal
/// let with_side_effect = __.side_effect(__.out().store("neighbors"));
/// ```
pub fn side_effect(traversal: Traversal<Value, Value>) -> Traversal<Value, Value> {
    use crate::traversal::sideeffect::SideEffectStep;
    Traversal::<Value, Value>::new().add_step(SideEffectStep::new(traversal))
}

/// Profile the traversal step timing and counts.
///
/// Records the number of traversers and elapsed time in milliseconds
/// to the side-effects under the default key "~profile".
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Profile traversal step
/// let profiled = __.profile();
/// ```
pub fn profile() -> Traversal<Value, Value> {
    use crate::traversal::sideeffect::ProfileStep;
    Traversal::<Value, Value>::new().add_step(ProfileStep::new())
}

/// Profile the traversal with a custom key.
///
/// Like `profile()`, but stores data under the specified key instead
/// of the default "~profile".
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Profile with custom key
/// let profiled = __.profile_as("my_profile");
/// ```
pub fn profile_as(key: impl Into<String>) -> Traversal<Value, Value> {
    use crate::traversal::sideeffect::ProfileStep;
    Traversal::<Value, Value>::new().add_step(ProfileStep::with_key(key))
}

// =============================================================================
// AnonymousTraversal Struct - Enables `__.method()` syntax
// =============================================================================

/// Anonymous traversal factory for Gremlin-style `__.method()` syntax.
///
/// This zero-sized struct provides method-based access to all anonymous
/// traversal functions. Use the static `__` instance for fluent syntax.
///
/// # Example
///
/// ```ignore
/// use interstellar::traversal::__;
///
/// // Gremlin-style syntax
/// let friends = __.out_labels(&["knows"]);
///
/// // Chain anonymous traversals
/// let complex = __.out().has_label("person").values("name");
///
/// // Use in parent traversals
/// let results = g.v()
///     .has_label("person")
///     .where_(__.out_labels(&["knows"]))
///     .to_list();
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct AnonymousTraversal;

/// Static instance for `__.method()` syntax.
///
/// This is the primary way to create anonymous traversals using Gremlin-style syntax.
#[allow(non_upper_case_globals)]
pub static __: AnonymousTraversal = AnonymousTraversal;

impl AnonymousTraversal {
    // -------------------------------------------------------------------------
    // Identity
    // -------------------------------------------------------------------------

    /// Create an identity traversal that passes input through unchanged.
    #[inline]
    pub fn identity(&self) -> Traversal<Value, Value> {
        identity()
    }

    // -------------------------------------------------------------------------
    // Navigation - Vertex to Vertex
    // -------------------------------------------------------------------------

    /// Traverse to outgoing adjacent vertices.
    #[inline]
    pub fn out(&self) -> Traversal<Value, Value> {
        out()
    }

    /// Traverse to outgoing adjacent vertices via edges with given labels.
    #[inline]
    pub fn out_labels(&self, labels: &[&str]) -> Traversal<Value, Value> {
        out_labels(labels)
    }

    /// Traverse to incoming adjacent vertices.
    #[inline]
    pub fn in_(&self) -> Traversal<Value, Value> {
        in_()
    }

    /// Traverse to incoming adjacent vertices via edges with given labels.
    #[inline]
    pub fn in_labels(&self, labels: &[&str]) -> Traversal<Value, Value> {
        in_labels(labels)
    }

    /// Traverse to adjacent vertices in both directions.
    #[inline]
    pub fn both(&self) -> Traversal<Value, Value> {
        both()
    }

    /// Traverse to adjacent vertices in both directions via edges with given labels.
    #[inline]
    pub fn both_labels(&self, labels: &[&str]) -> Traversal<Value, Value> {
        both_labels(labels)
    }

    // -------------------------------------------------------------------------
    // Navigation - Vertex to Edge
    // -------------------------------------------------------------------------

    /// Traverse to outgoing edges.
    #[inline]
    pub fn out_e(&self) -> Traversal<Value, Value> {
        out_e()
    }

    /// Traverse to outgoing edges with given labels.
    #[inline]
    pub fn out_e_labels(&self, labels: &[&str]) -> Traversal<Value, Value> {
        out_e_labels(labels)
    }

    /// Traverse to incoming edges.
    #[inline]
    pub fn in_e(&self) -> Traversal<Value, Value> {
        in_e()
    }

    /// Traverse to incoming edges with given labels.
    #[inline]
    pub fn in_e_labels(&self, labels: &[&str]) -> Traversal<Value, Value> {
        in_e_labels(labels)
    }

    /// Traverse to all incident edges (both directions).
    #[inline]
    pub fn both_e(&self) -> Traversal<Value, Value> {
        both_e()
    }

    /// Traverse to all incident edges with given labels.
    #[inline]
    pub fn both_e_labels(&self, labels: &[&str]) -> Traversal<Value, Value> {
        both_e_labels(labels)
    }

    // -------------------------------------------------------------------------
    // Navigation - Edge to Vertex
    // -------------------------------------------------------------------------

    /// Get the source (outgoing) vertex of an edge.
    #[inline]
    pub fn out_v(&self) -> Traversal<Value, Value> {
        out_v()
    }

    /// Get the target (incoming) vertex of an edge.
    #[inline]
    pub fn in_v(&self) -> Traversal<Value, Value> {
        in_v()
    }

    /// Get both vertices of an edge.
    #[inline]
    pub fn both_v(&self) -> Traversal<Value, Value> {
        both_v()
    }

    /// Get the "other" vertex of an edge.
    #[inline]
    pub fn other_v(&self) -> Traversal<Value, Value> {
        other_v()
    }

    // -------------------------------------------------------------------------
    // Filter Steps
    // -------------------------------------------------------------------------

    /// Filter elements by label.
    #[inline]
    pub fn has_label(&self, label: impl Into<String>) -> Traversal<Value, Value> {
        has_label(label)
    }

    /// Filter elements by any of the given labels.
    #[inline]
    pub fn has_label_any(&self, labels: &[&str]) -> Traversal<Value, Value> {
        has_label_any(labels)
    }

    /// Filter elements by property existence.
    #[inline]
    pub fn has(&self, key: impl Into<String>) -> Traversal<Value, Value> {
        has(key)
    }

    /// Filter elements by property absence.
    #[inline]
    pub fn has_not(&self, key: impl Into<String>) -> Traversal<Value, Value> {
        has_not(key)
    }

    /// Filter elements by property value equality.
    #[inline]
    pub fn has_value(
        &self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Traversal<Value, Value> {
        has_value(key, value)
    }

    /// Filter elements by ID.
    #[inline]
    pub fn has_id(&self, id: impl Into<Value>) -> Traversal<Value, Value> {
        has_id(id)
    }

    /// Filter elements by multiple IDs.
    #[inline]
    pub fn has_ids<I, T>(&self, ids: I) -> Traversal<Value, Value>
    where
        I: IntoIterator<Item = T>,
        T: Into<Value>,
    {
        has_ids(ids)
    }

    /// Filter elements by property value using a predicate.
    #[inline]
    pub fn has_where(
        &self,
        key: impl Into<String>,
        predicate: impl Predicate + 'static,
    ) -> Traversal<Value, Value> {
        has_where(key, predicate)
    }

    /// Filter by testing the current value against a predicate.
    #[inline]
    pub fn is_(&self, predicate: impl Predicate + 'static) -> Traversal<Value, Value> {
        is_(predicate)
    }

    /// Filter by testing the current value for equality.
    #[inline]
    pub fn is_eq(&self, value: impl Into<Value>) -> Traversal<Value, Value> {
        is_eq(value)
    }

    /// Filter elements using a custom predicate.
    #[inline]
    pub fn filter<F>(&self, predicate: F) -> Traversal<Value, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync + 'static,
    {
        filter(predicate)
    }

    /// Deduplicate traversers by value.
    #[inline]
    pub fn dedup(&self) -> Traversal<Value, Value> {
        dedup()
    }

    /// Deduplicate traversers by property value.
    #[inline]
    pub fn dedup_by_key(&self, key: impl Into<String>) -> Traversal<Value, Value> {
        dedup_by_key(key)
    }

    /// Deduplicate traversers by element label.
    #[inline]
    pub fn dedup_by_label(&self) -> Traversal<Value, Value> {
        dedup_by_label()
    }

    /// Deduplicate traversers by sub-traversal result.
    #[inline]
    pub fn dedup_by(&self, sub: Traversal<Value, Value>) -> Traversal<Value, Value> {
        dedup_by(sub)
    }

    /// Limit the number of traversers.
    #[inline]
    pub fn limit(&self, count: usize) -> Traversal<Value, Value> {
        limit(count)
    }

    /// Skip the first n traversers.
    #[inline]
    pub fn skip(&self, count: usize) -> Traversal<Value, Value> {
        skip(count)
    }

    /// Select traversers within a range.
    #[inline]
    pub fn range(&self, start: usize, end: usize) -> Traversal<Value, Value> {
        range(start, end)
    }

    /// Filter to only simple paths (no repeated elements).
    #[inline]
    pub fn simple_path(&self) -> Traversal<Value, Value> {
        simple_path()
    }

    /// Filter to only cyclic paths (at least one repeated element).
    #[inline]
    pub fn cyclic_path(&self) -> Traversal<Value, Value> {
        cyclic_path()
    }

    /// Return only the last element from the traversal.
    #[inline]
    pub fn tail(&self) -> Traversal<Value, Value> {
        tail()
    }

    /// Return only the last n elements from the traversal.
    #[inline]
    pub fn tail_n(&self, count: usize) -> Traversal<Value, Value> {
        tail_n(count)
    }

    /// Probabilistic filter using random coin flip.
    #[inline]
    pub fn coin(&self, probability: f64) -> Traversal<Value, Value> {
        coin(probability)
    }

    /// Randomly sample n elements using reservoir sampling.
    #[inline]
    pub fn sample(&self, count: usize) -> Traversal<Value, Value> {
        sample(count)
    }

    /// Filter property objects by key name.
    #[inline]
    pub fn has_key(&self, key: impl Into<String>) -> Traversal<Value, Value> {
        has_key(key)
    }

    /// Filter property objects by any of the specified key names.
    #[inline]
    pub fn has_key_any<I, S>(&self, keys: I) -> Traversal<Value, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        has_key_any(keys)
    }

    /// Filter property objects by value.
    #[inline]
    pub fn has_prop_value(&self, value: impl Into<Value>) -> Traversal<Value, Value> {
        has_prop_value(value)
    }

    /// Filter property objects by any of the specified values.
    #[inline]
    pub fn has_prop_value_any<I, V>(&self, values: I) -> Traversal<Value, Value>
    where
        I: IntoIterator<Item = V>,
        V: Into<Value>,
    {
        has_prop_value_any(values)
    }

    /// Filter traversers by testing their current value against a predicate.
    #[inline]
    pub fn where_p(
        &self,
        predicate: impl crate::traversal::predicate::Predicate + 'static,
    ) -> Traversal<Value, Value> {
        where_p(predicate)
    }

    // -------------------------------------------------------------------------
    // Transform Steps
    // -------------------------------------------------------------------------

    /// Extract property values.
    #[inline]
    pub fn values(&self, key: impl Into<String>) -> Traversal<Value, Value> {
        values(key)
    }

    /// Extract multiple property values.
    #[inline]
    pub fn values_multi<I, S>(&self, keys: I) -> Traversal<Value, Value>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        values_multi(keys)
    }

    /// Extract all property objects.
    #[inline]
    pub fn properties(&self) -> Traversal<Value, Value> {
        properties()
    }

    /// Extract specific property objects.
    #[inline]
    pub fn properties_keys(&self, keys: &[&str]) -> Traversal<Value, Value> {
        properties_keys(keys)
    }

    /// Get all properties as a map with list-wrapped values.
    #[inline]
    pub fn value_map(&self) -> Traversal<Value, Value> {
        value_map()
    }

    /// Get specific properties as a map with list-wrapped values.
    #[inline]
    pub fn value_map_keys(&self, keys: &[&str]) -> Traversal<Value, Value> {
        value_map_keys(keys)
    }

    /// Get all properties as a map including id and label tokens.
    #[inline]
    pub fn value_map_with_tokens(&self) -> Traversal<Value, Value> {
        value_map_with_tokens()
    }

    /// Get complete element representation as a map.
    #[inline]
    pub fn element_map(&self) -> Traversal<Value, Value> {
        element_map()
    }

    /// Get element representation with specific properties.
    #[inline]
    pub fn element_map_keys(&self, keys: &[&str]) -> Traversal<Value, Value> {
        element_map_keys(keys)
    }

    /// Get all properties as a map of property objects.
    #[inline]
    pub fn property_map(&self) -> Traversal<Value, Value> {
        property_map()
    }

    /// Get specific properties as a map of property objects.
    #[inline]
    pub fn property_map_keys(&self, keys: &[&str]) -> Traversal<Value, Value> {
        property_map_keys(keys)
    }

    /// Unroll collections into individual elements.
    #[inline]
    pub fn unfold(&self) -> Traversal<Value, Value> {
        unfold()
    }

    /// Calculate the arithmetic mean (average) of numeric values.
    #[inline]
    pub fn mean(&self) -> Traversal<Value, Value> {
        mean()
    }

    /// Collect all traversers into a single list value.
    #[inline]
    pub fn fold(&self) -> Traversal<Value, Value> {
        fold()
    }

    /// Sum all numeric input values.
    #[inline]
    pub fn sum(&self) -> Traversal<Value, Value> {
        sum()
    }

    /// Count elements within each collection value (local scope).
    #[inline]
    pub fn count_local(&self) -> Traversal<Value, Value> {
        count_local()
    }

    /// Sum elements within each collection value (local scope).
    #[inline]
    pub fn sum_local(&self) -> Traversal<Value, Value> {
        sum_local()
    }

    /// Extract keys from Map values.
    #[inline]
    pub fn select_keys(&self) -> Traversal<Value, Value> {
        select_keys()
    }

    /// Extract values from Map values.
    #[inline]
    pub fn select_values(&self) -> Traversal<Value, Value> {
        select_values()
    }

    /// Sort traversers using a fluent builder.
    #[inline]
    pub fn order(&self) -> OrderBuilder<Value> {
        order()
    }

    /// Evaluate a mathematical expression.
    #[cfg(feature = "gql")]
    #[inline]
    pub fn math(&self, expression: &str) -> crate::traversal::transform::MathBuilder<Value> {
        math(expression)
    }

    /// Create a projection with named keys.
    #[inline]
    pub fn project(&self, keys: &[&str]) -> ProjectBuilder<Value> {
        project(keys)
    }

    /// Group traversers by a key and collect values.
    #[inline]
    pub fn group(&self) -> crate::traversal::aggregate::GroupBuilder<Value> {
        group()
    }

    /// Count traversers grouped by a key.
    #[inline]
    pub fn group_count(&self) -> crate::traversal::aggregate::GroupCountBuilder<Value> {
        group_count()
    }

    /// Extract the element ID.
    #[inline]
    pub fn id(&self) -> Traversal<Value, Value> {
        id()
    }

    /// Extract the element label.
    #[inline]
    pub fn label(&self) -> Traversal<Value, Value> {
        label()
    }

    /// Extract the key from property map objects.
    #[inline]
    pub fn key(&self) -> Traversal<Value, Value> {
        key()
    }

    /// Extract the value from property map objects.
    #[inline]
    pub fn value(&self) -> Traversal<Value, Value> {
        value()
    }

    /// Extract the current loop depth.
    #[inline]
    pub fn loops(&self) -> Traversal<Value, Value> {
        loops()
    }

    /// Annotate each element with its position index.
    #[inline]
    pub fn index(&self) -> Traversal<Value, Value> {
        index()
    }

    /// Replace values with a constant.
    #[inline]
    pub fn constant(&self, value: impl Into<Value>) -> Traversal<Value, Value> {
        constant(value)
    }

    /// Convert the path to a list.
    #[inline]
    pub fn path(&self) -> Traversal<Value, Value> {
        path()
    }

    /// Transform values using a closure.
    #[inline]
    pub fn map<F>(&self, f: F) -> Traversal<Value, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static,
    {
        map(f)
    }

    /// Transform values to multiple values using a closure.
    #[inline]
    pub fn flat_map<F>(&self, f: F) -> Traversal<Value, Value>
    where
        F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync + 'static,
    {
        flat_map(f)
    }

    /// Label the current position in the path.
    #[inline]
    pub fn as_(&self, label: &str) -> Traversal<Value, Value> {
        as_(label)
    }

    /// Select multiple labeled values from the path.
    #[inline]
    pub fn select(&self, labels: &[&str]) -> Traversal<Value, Value> {
        select(labels)
    }

    /// Select a single labeled value from the path.
    #[inline]
    pub fn select_one(&self, label: &str) -> Traversal<Value, Value> {
        select_one(label)
    }

    // -------------------------------------------------------------------------
    // Filter Steps using Anonymous Traversals
    // -------------------------------------------------------------------------

    /// Filter by sub-traversal existence.
    #[inline]
    pub fn where_(&self, sub: Traversal<Value, Value>) -> Traversal<Value, Value> {
        where_(sub)
    }

    /// Filter by sub-traversal non-existence.
    #[inline]
    pub fn not(&self, sub: Traversal<Value, Value>) -> Traversal<Value, Value> {
        not(sub)
    }

    /// Filter by multiple sub-traversals (AND logic).
    #[inline]
    pub fn and_(&self, subs: Vec<Traversal<Value, Value>>) -> Traversal<Value, Value> {
        and_(subs)
    }

    /// Filter by multiple sub-traversals (OR logic).
    #[inline]
    pub fn or_(&self, subs: Vec<Traversal<Value, Value>>) -> Traversal<Value, Value> {
        or_(subs)
    }

    // -------------------------------------------------------------------------
    // Branch Steps using Anonymous Traversals
    // -------------------------------------------------------------------------

    /// Execute multiple branches and merge results.
    #[inline]
    pub fn union(&self, branches: Vec<Traversal<Value, Value>>) -> Traversal<Value, Value> {
        union(branches)
    }

    /// Try branches in order, return first non-empty result.
    #[inline]
    pub fn coalesce(&self, branches: Vec<Traversal<Value, Value>>) -> Traversal<Value, Value> {
        coalesce(branches)
    }

    /// Conditional branching.
    #[inline]
    pub fn choose(
        &self,
        condition: Traversal<Value, Value>,
        if_true: Traversal<Value, Value>,
        if_false: Traversal<Value, Value>,
    ) -> Traversal<Value, Value> {
        choose(condition, if_true, if_false)
    }

    /// Optional traversal with fallback to input.
    #[inline]
    pub fn optional(&self, sub: Traversal<Value, Value>) -> Traversal<Value, Value> {
        optional(sub)
    }

    /// Execute sub-traversal in isolated scope.
    #[inline]
    pub fn local(&self, sub: Traversal<Value, Value>) -> Traversal<Value, Value> {
        local(sub)
    }

    // -------------------------------------------------------------------------
    // Mutation Steps
    // -------------------------------------------------------------------------

    /// Create a new vertex with the specified label.
    #[inline]
    pub fn add_v(&self, label: impl Into<String>) -> Traversal<Value, Value> {
        add_v(label)
    }

    /// Create a new edge with the specified label.
    #[inline]
    pub fn add_e(&self, label: impl Into<String>) -> crate::traversal::mutation::AddEStep {
        add_e(label)
    }

    /// Add or update a property on the current element.
    #[inline]
    pub fn property(
        &self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Traversal<Value, Value> {
        property(key, value)
    }

    /// Delete the current element (vertex or edge).
    #[inline]
    pub fn drop(&self) -> Traversal<Value, Value> {
        drop()
    }

    // -------------------------------------------------------------------------
    // Branch Steps
    // -------------------------------------------------------------------------

    /// Create a branch step for anonymous traversals.
    #[inline]
    pub fn branch(&self, branch_traversal: Traversal<Value, Value>) -> Traversal<Value, Value> {
        branch(branch_traversal)
    }

    // -------------------------------------------------------------------------
    // Side Effect Steps
    // -------------------------------------------------------------------------

    /// Store traverser values in a side-effect collection.
    #[inline]
    pub fn store(&self, key: impl Into<String>) -> Traversal<Value, Value> {
        store(key)
    }

    /// Aggregate all traverser values into a side-effect collection.
    #[inline]
    pub fn aggregate(&self, key: impl Into<String>) -> Traversal<Value, Value> {
        aggregate(key)
    }

    /// Retrieve side-effect data by key.
    #[inline]
    pub fn cap(&self, key: impl Into<String>) -> Traversal<Value, Value> {
        cap(key)
    }

    /// Execute a sub-traversal for its side effects.
    #[inline]
    pub fn side_effect(&self, traversal: Traversal<Value, Value>) -> Traversal<Value, Value> {
        side_effect(traversal)
    }

    /// Profile the traversal step timing and counts.
    #[inline]
    pub fn profile(&self) -> Traversal<Value, Value> {
        profile()
    }

    /// Profile the traversal with a custom key.
    #[inline]
    pub fn profile_as(&self, key: impl Into<String>) -> Traversal<Value, Value> {
        profile_as(key)
    }
}
