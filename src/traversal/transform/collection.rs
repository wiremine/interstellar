use crate::impl_flatmap_step;
use crate::traversal::{ExecutionContext, Traverser};
use crate::value::Value;

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
}

// Use the macro to implement AnyStep for UnfoldStep
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

impl crate::traversal::step::AnyStep for MeanStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
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

        if count == 0 {
            Box::new(std::iter::empty())
        } else {
            let mean = sum / count as f64;
            Box::new(std::iter::once(Traverser {
                value: Value::Float(mean),
                path: last_path.unwrap_or_default(),
                loops: 0,
                sack: None,
                bulk: 1,
            }))
        }
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "mean"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use crate::traversal::step::AnyStep;
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
}
