use crate::impl_flatmap_step;
use crate::traversal::{ExecutionContext, Traverser};
use crate::value::Value;

// -----------------------------------------------------------------------------
// FoldStep - collect all traversers into a single list
// -----------------------------------------------------------------------------

/// Barrier step that collects all input traversers into a single list.
///
/// This is a **barrier step** - it consumes ALL input before producing a single
/// output traverser containing a `Value::List` of all collected values.
///
/// # Behavior
///
/// - Collects all input traverser values into a list
/// - Produces exactly one output traverser (even for empty input)
/// - Empty input produces an empty list `[]`
/// - Preserves path from the last input traverser
/// - Bulk is respected: a traverser with bulk=3 contributes 3 copies
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().fold()              // Collect all vertices into a list
/// g.V().values("age").fold() // Collect all ages into a list
/// ```
///
/// # Example
///
/// ```ignore
/// // Collect all vertex IDs into a list
/// let all_ids = g.v().id().fold().next();
/// // Returns: Value::List([0, 1, 2, 3, ...])
///
/// // Fold is often paired with unfold for transformations
/// let doubled = g.inject([1i64, 2i64, 3i64])
///     .fold()
///     .unfold()
///     .to_list();
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct FoldStep;

impl FoldStep {
    /// Create a new FoldStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for FoldStep {
    type Iter<'a>
        = std::iter::Once<Traverser>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let mut values = Vec::new();
        let mut last_path = None;

        for t in input {
            last_path = Some(t.path.clone());
            // Respect bulk: add value `bulk` times
            for _ in 0..t.bulk {
                values.push(t.value.clone());
            }
        }

        let result = Traverser {
            value: Value::List(values),
            path: last_path.unwrap_or_default(),
            loops: 0,
            sack: None,
            bulk: 1,
        };
        std::iter::once(result)
    }

    fn name(&self) -> &'static str {
        "fold"
    }

    fn is_barrier(&self) -> bool {
        true
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // BARRIER STEP: FoldStep cannot truly stream because it must collect ALL inputs
        // before producing a single list output.
        // Current behavior: pass-through (incorrect but safe for pipeline compatibility).
        Box::new(std::iter::once(input))
    }
}

// -----------------------------------------------------------------------------
// SumStep - sum all numeric values
// -----------------------------------------------------------------------------

/// Reducing step that sums all numeric input values.
///
/// This is a **barrier step** - it consumes ALL input before producing a single
/// output traverser containing the sum as a `Value::Int` or `Value::Float`.
///
/// # Behavior
///
/// - Sums all numeric values (`Value::Int` and `Value::Float`)
/// - Non-numeric values are silently ignored
/// - If all inputs are integers, returns `Value::Int`
/// - If any input is a float, returns `Value::Float`
/// - Empty or all-non-numeric input returns `Value::Int(0)`
/// - Bulk is respected: a traverser with value 5 and bulk=3 contributes 15
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().values("age").sum()     // Sum all ages
/// g.V().out().count().sum()     // Sum neighbor counts
/// ```
///
/// # Example
///
/// ```ignore
/// // Sum all ages
/// let total_age = g.v().has_label("person").values("age").sum().next();
/// // Returns: Value::Int(total)
///
/// // Sum with floats
/// let total = g.inject([1.5, 2.5, 3.0]).sum().next();
/// // Returns: Value::Float(7.0)
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct SumStep;

impl SumStep {
    /// Create a new SumStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for SumStep {
    type Iter<'a>
        = std::iter::Once<Traverser>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let mut int_sum: i64 = 0;
        let mut float_sum: f64 = 0.0;
        let mut has_float = false;
        let mut last_path = None;

        for t in input {
            last_path = Some(t.path.clone());
            let multiplier = t.bulk as i64;

            match &t.value {
                Value::Int(n) => {
                    int_sum += n * multiplier;
                }
                Value::Float(f) => {
                    has_float = true;
                    float_sum += f * (t.bulk as f64);
                }
                _ => {} // Ignore non-numeric values
            }
        }

        let result_value = if has_float {
            Value::Float(float_sum + (int_sum as f64))
        } else {
            Value::Int(int_sum)
        };

        let result = Traverser {
            value: result_value,
            path: last_path.unwrap_or_default(),
            loops: 0,
            sack: None,
            bulk: 1,
        };
        std::iter::once(result)
    }

    fn name(&self) -> &'static str {
        "sum"
    }

    fn is_barrier(&self) -> bool {
        true
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // BARRIER STEP: SumStep cannot truly stream because it must sum ALL inputs.
        // Current behavior: pass-through (incorrect but safe for pipeline compatibility).
        Box::new(std::iter::once(input))
    }
}

// -----------------------------------------------------------------------------
// CountLocalStep - count elements in a collection (local scope)
// -----------------------------------------------------------------------------

/// Transform step that counts elements within a collection value.
///
/// Unlike the global `CountStep` which counts traversers in the stream,
/// `CountLocalStep` counts elements *within* each traverser's collection value.
/// This implements Gremlin's `count(local)` semantics.
///
/// # Behavior
///
/// - `Value::List`: Returns the number of elements in the list
/// - `Value::Map`: Returns the number of entries in the map
/// - `Value::String`: Returns the length of the string
/// - Other values: Returns 1 (the value itself counts as 1 item)
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().out().fold().count(local)  // Count items in the folded list
/// g.V().valueMap().count(local)    // Count properties per vertex
/// ```
///
/// # Example
///
/// ```ignore
/// // Count friends per person
/// let friend_counts = g.v().out("knows").fold().count_local().to_list();
///
/// // Count properties per vertex
/// let prop_counts = g.v().value_map().count_local().to_list();
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct CountLocalStep;

impl CountLocalStep {
    /// Create a new CountLocalStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for CountLocalStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.map(|t| {
            let count = match &t.value {
                Value::List(items) => items.len() as i64,
                Value::Map(map) => map.len() as i64,
                Value::String(s) => s.len() as i64,
                _ => 1, // Non-collection values count as 1
            };
            t.with_value(Value::Int(count))
        })
    }

    fn name(&self) -> &'static str {
        "count(local)"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let count = match &input.value {
            Value::List(items) => items.len() as i64,
            Value::Map(map) => map.len() as i64,
            Value::String(s) => s.len() as i64,
            _ => 1,
        };
        Box::new(std::iter::once(input.with_value(Value::Int(count))))
    }
}

// -----------------------------------------------------------------------------
// SumLocalStep - sum elements in a collection (local scope)
// -----------------------------------------------------------------------------

/// Transform step that sums numeric elements within a collection value.
///
/// Unlike the global `SumStep` which sums across all traversers,
/// `SumLocalStep` sums elements *within* each traverser's collection value.
/// This implements Gremlin's `sum(local)` semantics.
///
/// # Behavior
///
/// - `Value::List`: Sums all numeric elements in the list
/// - `Value::Int`/`Value::Float`: Returns the value unchanged
/// - Other values: Returns 0
/// - Non-numeric list elements are ignored
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().values("scores").fold().sum(local)  // Sum scores per vertex
/// ```
///
/// # Example
///
/// ```ignore
/// // Sum transaction amounts per user
/// let totals = g.v().out("made").values("amount").fold().sum_local().to_list();
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct SumLocalStep;

impl SumLocalStep {
    /// Create a new SumLocalStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for SumLocalStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.map(|t| {
            let sum = match &t.value {
                Value::List(items) => {
                    let mut int_sum: i64 = 0;
                    let mut float_sum: f64 = 0.0;
                    let mut has_float = false;

                    for item in items {
                        match item {
                            Value::Int(n) => int_sum += n,
                            Value::Float(f) => {
                                has_float = true;
                                float_sum += f;
                            }
                            _ => {}
                        }
                    }

                    if has_float {
                        Value::Float(float_sum + (int_sum as f64))
                    } else {
                        Value::Int(int_sum)
                    }
                }
                Value::Int(n) => Value::Int(*n),
                Value::Float(f) => Value::Float(*f),
                _ => Value::Int(0),
            };
            t.with_value(sum)
        })
    }

    fn name(&self) -> &'static str {
        "sum(local)"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let sum = match &input.value {
            Value::List(items) => {
                let mut int_sum: i64 = 0;
                let mut float_sum: f64 = 0.0;
                let mut has_float = false;

                for item in items {
                    match item {
                        Value::Int(n) => int_sum += n,
                        Value::Float(f) => {
                            has_float = true;
                            float_sum += f;
                        }
                        _ => {}
                    }
                }

                if has_float {
                    Value::Float(float_sum + (int_sum as f64))
                } else {
                    Value::Int(int_sum)
                }
            }
            Value::Int(n) => Value::Int(*n),
            Value::Float(f) => Value::Float(*f),
            _ => Value::Int(0),
        };
        Box::new(std::iter::once(input.with_value(sum)))
    }
}

// -----------------------------------------------------------------------------
// UnfoldStep - unroll collection into individual items
// -----------------------------------------------------------------------------

/// Transform step that unrolls collections into individual elements.
///
/// This is a **flatMap step** - each input traverser may produce zero or more
/// output traversers. Collections (`Value::List` and `Value::Map`) are expanded
/// into separate traversers for each element, while non-collection values pass
/// through unchanged.
///
/// # Behavior
///
/// - `Value::List`: Each list element becomes a separate traverser
/// - `Value::Map`: Each key-value pair becomes a single-entry map traverser
/// - Other values: Pass through as-is
/// - Preserves path information for all output traversers
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().fold().unfold()  // Collect into list, then expand back
/// ```
///
/// # Example
///
/// ```ignore
/// // Unfold a list back into individual elements
/// let items = g.inject([Value::List(vec![1.into(), 2.into(), 3.into()])]).unfold().to_list();
/// // Returns: [1, 2, 3]
///
/// // Unfold map entries
/// let entries = g.v().value_map().unfold().to_list();
/// // Each entry is a single-key map like {"name": ["Alice"]}
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct UnfoldStep;

impl UnfoldStep {
    /// Create a new UnfoldStep.
    pub fn new() -> Self {
        Self
    }

    /// Expand a traverser by unfolding its value.
    ///
    /// Returns an iterator of values produced by unfolding the input.
    fn expand<'a>(
        &self,
        _ctx: &'a ExecutionContext<'a>,
        traverser: Traverser,
    ) -> impl Iterator<Item = Traverser> + 'a {
        let values = match &traverser.value {
            Value::List(items) => {
                // Each list element becomes a separate traverser
                items.clone()
            }
            Value::Map(map) => {
                // Each map entry becomes a single-entry map
                map.iter()
                    .map(|(k, v)| {
                        let mut entry = std::collections::HashMap::new();
                        entry.insert(k.clone(), v.clone());
                        Value::Map(entry)
                    })
                    .collect()
            }
            // Non-collections pass through unchanged
            other => vec![other.clone()],
        };

        // Create new traversers for each value
        values
            .into_iter()
            .map(move |value| traverser.split(value))
            .collect::<Vec<_>>()
            .into_iter()
    }

    /// Streaming version of expand.
    fn expand_streaming(
        &self,
        _ctx: &crate::traversal::context::StreamingContext,
        traverser: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let values: Vec<crate::value::Value> = match &traverser.value {
            crate::value::Value::List(items) => items.clone(),
            crate::value::Value::Map(map) => map
                .iter()
                .map(|(k, v)| {
                    let mut entry = std::collections::HashMap::new();
                    entry.insert(k.clone(), v.clone());
                    crate::value::Value::Map(entry)
                })
                .collect(),
            other => vec![other.clone()],
        };

        Box::new(
            values
                .into_iter()
                .map(move |value| traverser.split(value))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }
}

// Use the macro to implement Step for UnfoldStep
impl_flatmap_step!(UnfoldStep, "unfold");

// -----------------------------------------------------------------------------
// MeanStep - calculate arithmetic mean of numeric values
// -----------------------------------------------------------------------------

/// Reducing step that calculates the arithmetic mean (average) of numeric values.
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
/// - Path is preserved from the last input traverser
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().values("age").mean()  // Calculate average age
/// ```
///
/// # Example
///
/// ```ignore
/// // Calculate average age of all people
/// let avg_age = g.v().has_label("person").values("age").mean();
///
/// // Mixed values - non-numeric ignored
/// let avg = g.inject([1i64, 2i64, "three"]).mean(); // Returns 1.5
///
/// // Empty numeric input
/// let empty = g.inject(["a", "b", "c"]).mean(); // Returns nothing
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct MeanStep;

impl MeanStep {
    /// Create a new MeanStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for MeanStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let mut sum = 0.0_f64;
        let mut count = 0_u64;
        let mut last_path = None;

        for t in input {
            last_path = Some(t.path.clone());
            match &t.value {
                Value::Int(n) => {
                    sum += *n as f64;
                    count += 1;
                }
                Value::Float(f) => {
                    sum += *f;
                    count += 1;
                }
                _ => {} // Ignore non-numeric values
            }
        }

        let result = if count == 0 {
            None
        } else {
            let mean = sum / count as f64;
            Some(Traverser {
                value: Value::Float(mean),
                path: last_path.unwrap_or_default(),
                loops: 0,
                sack: None,
                bulk: 1,
            })
        };
        result.into_iter()
    }

    fn name(&self) -> &'static str {
        "mean"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // BARRIER STEP: MeanStep cannot truly stream because computing the arithmetic mean
        // requires summing ALL values and dividing by the count. The result is a single
        // value that depends on every input element.
        // This is fundamentally incompatible with O(1) streaming semantics.
        // Current behavior: pass-through (incorrect but safe for pipeline compatibility).
        Box::new(std::iter::once(input))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use crate::traversal::step::Step;
    use crate::traversal::SnapshotLike;
    use crate::value::{EdgeId, VertexId};
    use std::collections::HashMap;

    fn create_test_graph() -> Graph {
        // Add minimal dummy data if needed by tests, though many here use Traverser::new() directly
        Graph::new()
    }

    mod unfold_step_list {
        use super::*;

        #[test]
        fn unfolds_list_into_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = UnfoldStep::new();
            let list = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
            let input = vec![Traverser::new(list)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
            assert_eq!(output[2].value, Value::Int(3));
        }

        #[test]
        fn unfolds_empty_list_into_nothing() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = UnfoldStep::new();
            let list = Value::List(vec![]);
            let input = vec![Traverser::new(list)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn unfolds_nested_lists_one_level() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = UnfoldStep::new();
            let nested = Value::List(vec![
                Value::List(vec![Value::Int(1)]),
                Value::List(vec![Value::Int(2)]),
            ]);
            let input = vec![Traverser::new(nested)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::List(vec![Value::Int(1)]));
            assert_eq!(output[1].value, Value::List(vec![Value::Int(2)]));
        }
    }

    mod unfold_step_map {
        use super::*;

        #[test]
        fn unfolds_map_into_entries() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = UnfoldStep::new();
            let mut map = HashMap::new();
            map.insert("a".to_string(), Value::Int(1));
            map.insert("b".to_string(), Value::Int(2));
            let input = vec![Traverser::new(Value::Map(map))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            // Output order is not guaranteed for map, so verify content
            let values: Vec<Value> = output.into_iter().map(|t| t.value).collect();

            // Check that we have two maps, each with one entry
            for val in values {
                if let Value::Map(m) = val {
                    assert_eq!(m.len(), 1);
                    if m.contains_key("a") {
                        assert_eq!(m.get("a"), Some(&Value::Int(1)));
                    } else if m.contains_key("b") {
                        assert_eq!(m.get("b"), Some(&Value::Int(2)));
                    } else {
                        panic!("Unexpected key in map");
                    }
                } else {
                    panic!("Expected Value::Map");
                }
            }
        }

        #[test]
        fn unfolds_empty_map_into_nothing() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = UnfoldStep::new();
            let map = HashMap::new();
            let input = vec![Traverser::new(Value::Map(map))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod unfold_step_non_collection {
        use super::*;

        #[test]
        fn passes_single_value_through() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = UnfoldStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(42));
        }

        #[test]
        fn passes_vertex_through() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = UnfoldStep::new();
            let input = vec![Traverser::from_vertex(VertexId(1))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(1)));
        }
    }

    mod unfold_step_metadata {
        use super::*;

        #[test]
        fn preserves_path_for_each_unfolded_item() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = UnfoldStep::new();
            let mut traverser = Traverser::new(Value::List(vec![Value::Int(1), Value::Int(2)]));
            traverser.extend_path_labeled("start");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert!(output[0].path.has_label("start"));
            assert!(output[1].path.has_label("start"));
        }

        #[test]
        fn preserves_other_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = UnfoldStep::new();
            let mut traverser = Traverser::new(Value::List(vec![Value::Int(1)]));
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod unfold_step_integration {
        use super::*;

        #[test]
        fn handles_multiple_input_traversers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = UnfoldStep::new();
            let input = vec![
                Traverser::new(Value::List(vec![Value::Int(1), Value::Int(2)])),
                Traverser::new(Value::Int(3)),       // Non-collection
                Traverser::new(Value::List(vec![])), // Empty collection
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
            assert_eq!(output[2].value, Value::Int(3));
        }
    }

    mod mean_step_construction {
        use super::*;

        #[test]
        fn test_new() {
            let step = MeanStep::new();
            assert_eq!(step.name(), "mean");
        }
    }

    mod mean_step_numeric_tests {
        use super::*;

        #[test]
        fn calculates_mean_of_integers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();
            let input = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(20.0));
        }

        #[test]
        fn calculates_mean_of_floats() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();
            let input = vec![
                Traverser::new(Value::Float(1.5)),
                Traverser::new(Value::Float(2.5)),
                Traverser::new(Value::Float(3.5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(2.5));
        }

        #[test]
        fn calculates_mean_of_mixed_numbers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();
            let input = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Float(20.5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(15.25));
        }

        #[test]
        fn handles_negative_numbers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();
            let input = vec![
                Traverser::new(Value::Int(-10)),
                Traverser::new(Value::Int(10)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(0.0));
        }
    }

    mod mean_step_non_numeric_tests {
        use super::*;

        #[test]
        fn ignores_non_numeric_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();
            let input = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::String("skip me".to_string())),
                Traverser::new(Value::Bool(true)),
                Traverser::new(Value::Int(20)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(15.0)); // Mean of 10 and 20
        }

        #[test]
        fn ignores_vertices_and_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();
            let input = vec![
                Traverser::new(Value::Int(10)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_edge(EdgeId(1)),
                Traverser::new(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(20.0));
        }
    }

    mod mean_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn all_non_numeric_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();
            let input = vec![
                Traverser::new(Value::String("a".to_string())),
                Traverser::new(Value::Bool(false)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod mean_step_path_tests {
        use super::*;

        #[test]
        fn preserves_path_from_last_element() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();

            let mut t1 = Traverser::new(Value::Int(10));
            t1.extend_path_labeled("p1");

            let mut t2 = Traverser::new(Value::Int(20));
            t2.extend_path_labeled("p2");

            let input = vec![t1, t2];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("p2")); // Should have path from t2
        }
    }

    mod mean_step_traverser_fields {
        use super::*;

        #[test]
        fn resets_loops_and_bulk() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();

            let mut t1 = Traverser::new(Value::Int(10));
            t1.loops = 5;
            t1.bulk = 10;

            let input = vec![t1];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].loops, 0); // Reset loops
            assert_eq!(output[0].bulk, 1); // Reset bulk
        }
    }

    mod mean_step_integration {
        use super::*;

        #[test]
        fn calculates_mean_of_large_set() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MeanStep::new();
            // Mean of 1..100 is 50.5
            let input: Vec<Traverser> = (1..=100).map(|i| Traverser::new(Value::Int(i))).collect();

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(50.5));
        }
    }

    // =========================================================================
    // FoldStep Tests
    // =========================================================================

    mod fold_step_construction {
        use super::*;

        #[test]
        fn test_new() {
            let step = FoldStep::new();
            assert_eq!(step.name(), "fold");
        }

        #[test]
        fn is_barrier_returns_true() {
            let step = FoldStep::new();
            assert!(step.is_barrier());
        }

        #[test]
        fn is_clonable() {
            let step = FoldStep::new();
            let _cloned = step.clone();
        }
    }

    mod fold_step_basic {
        use super::*;

        #[test]
        fn folds_integers_into_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FoldStep::new();
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list.len(), 3);
                assert_eq!(list[0], Value::Int(1));
                assert_eq!(list[1], Value::Int(2));
                assert_eq!(list[2], Value::Int(3));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn folds_mixed_types_into_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FoldStep::new();
            let input = vec![
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::String("hello".to_string())),
                Traverser::new(Value::Bool(true)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list.len(), 3);
                assert_eq!(list[0], Value::Int(42));
                assert_eq!(list[1], Value::String("hello".to_string()));
                assert_eq!(list[2], Value::Bool(true));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn folds_empty_input_to_empty_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FoldStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert!(list.is_empty());
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn folds_single_value_to_single_element_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FoldStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list.len(), 1);
                assert_eq!(list[0], Value::Int(42));
            } else {
                panic!("Expected Value::List");
            }
        }
    }

    mod fold_step_vertices_edges {
        use super::*;

        #[test]
        fn folds_vertices_into_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FoldStep::new();
            let input = vec![
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list.len(), 2);
                assert_eq!(list[0], Value::Vertex(VertexId(1)));
                assert_eq!(list[1], Value::Vertex(VertexId(2)));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn folds_edges_into_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FoldStep::new();
            let input = vec![
                Traverser::from_edge(EdgeId(1)),
                Traverser::from_edge(EdgeId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list.len(), 2);
                assert_eq!(list[0], Value::Edge(EdgeId(1)));
                assert_eq!(list[1], Value::Edge(EdgeId(2)));
            } else {
                panic!("Expected Value::List");
            }
        }
    }

    // =========================================================================
    // SumStep Tests
    // =========================================================================

    mod sum_step_construction {
        use super::*;

        #[test]
        fn test_new() {
            let step = SumStep::new();
            assert_eq!(step.name(), "sum");
        }

        #[test]
        fn is_barrier_returns_true() {
            let step = SumStep::new();
            assert!(step.is_barrier());
        }

        #[test]
        fn is_clonable() {
            let step = SumStep::new();
            let _cloned = step.clone();
        }
    }

    mod sum_step_integers {
        use super::*;

        #[test]
        fn sums_integers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumStep::new();
            let input = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(30)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(60));
        }

        #[test]
        fn sums_negative_integers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumStep::new();
            let input = vec![
                Traverser::new(Value::Int(-10)),
                Traverser::new(Value::Int(20)),
                Traverser::new(Value::Int(-5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(5));
        }
    }

    mod sum_step_floats {
        use super::*;

        #[test]
        fn sums_floats() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumStep::new();
            let input = vec![
                Traverser::new(Value::Float(1.5)),
                Traverser::new(Value::Float(2.5)),
                Traverser::new(Value::Float(3.0)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(7.0));
        }
    }

    mod sum_step_mixed {
        use super::*;

        #[test]
        fn sums_mixed_int_and_float_returns_float() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumStep::new();
            let input = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Float(5.5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(15.5));
        }

        #[test]
        fn ignores_non_numeric_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumStep::new();
            let input = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::String("skip".to_string())),
                Traverser::new(Value::Bool(true)),
                Traverser::new(Value::Int(20)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(30));
        }
    }

    mod sum_step_empty {
        use super::*;

        #[test]
        fn empty_input_returns_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(0));
        }

        #[test]
        fn all_non_numeric_returns_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumStep::new();
            let input = vec![
                Traverser::new(Value::String("a".to_string())),
                Traverser::new(Value::Bool(false)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(0));
        }
    }

    // =========================================================================
    // CountLocalStep Tests
    // =========================================================================

    mod count_local_step_construction {
        use super::*;

        #[test]
        fn test_new() {
            let step = CountLocalStep::new();
            assert_eq!(step.name(), "count(local)");
        }

        #[test]
        fn is_not_barrier() {
            let step = CountLocalStep::new();
            assert!(!step.is_barrier());
        }

        #[test]
        fn is_clonable() {
            let step = CountLocalStep::new();
            let _cloned = step.clone();
        }
    }

    mod count_local_step_list {
        use super::*;

        #[test]
        fn counts_list_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CountLocalStep::new();
            let list = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
            let input = vec![Traverser::new(list)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(3));
        }

        #[test]
        fn counts_empty_list_as_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CountLocalStep::new();
            let input = vec![Traverser::new(Value::List(vec![]))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(0));
        }
    }

    mod count_local_step_map {
        use super::*;

        #[test]
        fn counts_map_entries() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CountLocalStep::new();
            let mut map = HashMap::new();
            map.insert("a".to_string(), Value::Int(1));
            map.insert("b".to_string(), Value::Int(2));
            let input = vec![Traverser::new(Value::Map(map))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(2));
        }

        #[test]
        fn counts_empty_map_as_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CountLocalStep::new();
            let input = vec![Traverser::new(Value::Map(HashMap::new()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(0));
        }
    }

    mod count_local_step_string {
        use super::*;

        #[test]
        fn counts_string_length() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CountLocalStep::new();
            let input = vec![Traverser::new(Value::String("hello".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(5));
        }

        #[test]
        fn counts_empty_string_as_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CountLocalStep::new();
            let input = vec![Traverser::new(Value::String("".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(0));
        }
    }

    mod count_local_step_other {
        use super::*;

        #[test]
        fn counts_non_collection_as_one() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CountLocalStep::new();
            let input = vec![
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::Bool(true)),
                Traverser::new(Value::Float(3.14)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(1));
            assert_eq!(output[2].value, Value::Int(1));
        }

        #[test]
        fn counts_vertex_as_one() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = CountLocalStep::new();
            let input = vec![Traverser::from_vertex(VertexId(1))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(1));
        }
    }

    // =========================================================================
    // SumLocalStep Tests
    // =========================================================================

    mod sum_local_step_construction {
        use super::*;

        #[test]
        fn test_new() {
            let step = SumLocalStep::new();
            assert_eq!(step.name(), "sum(local)");
        }

        #[test]
        fn is_not_barrier() {
            let step = SumLocalStep::new();
            assert!(!step.is_barrier());
        }

        #[test]
        fn is_clonable() {
            let step = SumLocalStep::new();
            let _cloned = step.clone();
        }
    }

    mod sum_local_step_list {
        use super::*;

        #[test]
        fn sums_integer_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumLocalStep::new();
            let list = Value::List(vec![Value::Int(10), Value::Int(20), Value::Int(30)]);
            let input = vec![Traverser::new(list)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(60));
        }

        #[test]
        fn sums_float_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumLocalStep::new();
            let list = Value::List(vec![Value::Float(1.5), Value::Float(2.5)]);
            let input = vec![Traverser::new(list)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(4.0));
        }

        #[test]
        fn sums_mixed_list_returns_float() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumLocalStep::new();
            let list = Value::List(vec![Value::Int(10), Value::Float(5.5)]);
            let input = vec![Traverser::new(list)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(15.5));
        }

        #[test]
        fn sums_empty_list_returns_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumLocalStep::new();
            let input = vec![Traverser::new(Value::List(vec![]))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(0));
        }

        #[test]
        fn ignores_non_numeric_in_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumLocalStep::new();
            let list = Value::List(vec![
                Value::Int(10),
                Value::String("skip".to_string()),
                Value::Int(20),
            ]);
            let input = vec![Traverser::new(list)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(30));
        }
    }

    mod sum_local_step_scalar {
        use super::*;

        #[test]
        fn returns_int_unchanged() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumLocalStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(42));
        }

        #[test]
        fn returns_float_unchanged() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumLocalStep::new();
            let input = vec![Traverser::new(Value::Float(3.14))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(3.14));
        }
    }

    mod sum_local_step_other {
        use super::*;

        #[test]
        fn non_numeric_non_list_returns_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = SumLocalStep::new();
            let input = vec![
                Traverser::new(Value::String("hello".to_string())),
                Traverser::new(Value::Bool(true)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(0));
            assert_eq!(output[1].value, Value::Int(0));
        }
    }
}
