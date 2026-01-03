use crate::traversal::{ExecutionContext, Traverser};
use crate::value::Value;

// -----------------------------------------------------------------------------
// MapStep - transform each value with a closure
// -----------------------------------------------------------------------------

/// Transform step that applies a closure to each value.
///
/// This step transforms each traverser's value using a user-provided function.
/// The closure receives the execution context and the current value, returning
/// a new value. This is a 1:1 mapping - each input produces exactly one output.
///
/// # Type Parameters
///
/// - `F`: The closure type that transforms values
///
/// # Example
///
/// ```ignore
/// // Double all integer values
/// let doubled = g.inject([1i64, 2i64, 3i64])
///     .map(|_ctx, v| {
///         if let Value::Int(n) = v {
///             Value::Int(n * 2)
///         } else {
///             v.clone()
///         }
///     })
///     .to_list();
/// // Results: [2, 4, 6]
/// ```
#[derive(Clone)]
pub struct MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync,
{
    f: F,
}

impl<F> MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync,
{
    /// Create a new MapStep with the given transformation function.
    ///
    /// # Arguments
    ///
    /// * `f` - The function to apply to each value
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = MapStep::new(|_ctx, v| {
    ///     if let Value::Int(n) = v {
    ///         Value::Int(n * 2)
    ///     } else {
    ///         v.clone()
    ///     }
    /// });
    /// ```
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> crate::traversal::step::AnyStep for MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static,
{
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let f = self.f.clone();
        Box::new(input.map(move |t| {
            let new_value = f(ctx, &t.value);
            t.with_value(new_value)
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "map"
    }
}

// Implement Debug manually since we can't derive it for closures
impl<F> std::fmt::Debug for MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapStep").finish_non_exhaustive()
    }
}

// -----------------------------------------------------------------------------
// FlatMapStep - transform each value to multiple values with a closure
// -----------------------------------------------------------------------------

/// Transform step that applies a closure returning multiple values.
///
/// This step transforms each traverser's value using a user-provided function
/// that returns a `Vec<Value>`. This is a 1:N mapping - each input can produce
/// zero or more outputs.
///
/// # Type Parameters
///
/// - `F`: The closure type that transforms values to a vector
///
/// # Example
///
/// ```ignore
/// // Generate a range of values from each integer
/// let expanded = g.inject([3i64, 5i64])
///     .flat_map(|_ctx, v| {
///         if let Value::Int(n) = v {
///             (0..*n).map(|i| Value::Int(i)).collect()
///         } else {
///             vec![]
///         }
///     })
///     .to_list();
/// // Results: [0, 1, 2, 0, 1, 2, 3, 4]
/// ```
#[derive(Clone)]
pub struct FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync,
{
    f: F,
}

impl<F> FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync,
{
    /// Create a new FlatMapStep with the given transformation function.
    ///
    /// # Arguments
    ///
    /// * `f` - The function to apply to each value, returning a Vec of new values
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = FlatMapStep::new(|_ctx, v| {
    ///     if let Value::Int(n) = v {
    ///         (0..*n).map(|i| Value::Int(i)).collect()
    ///     } else {
    ///         vec![]
    ///     }
    /// });
    /// ```
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> crate::traversal::step::AnyStep for FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync + 'static,
{
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let f = self.f.clone();
        Box::new(input.flat_map(move |t| {
            let values = f(ctx, &t.value);
            values.into_iter().map(move |v| t.split(v))
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "flatMap"
    }
}

// Implement Debug manually since we can't derive it for closures
impl<F> std::fmt::Debug for FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatMapStep").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    use crate::traversal::step::AnyStep;
    use crate::value::VertexId;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();

        // Vertex 0: person with name and age
        storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });

        Graph::new(Arc::new(storage))
    }

    // =========================================================================
    // MapStep Tests
    // =========================================================================

    mod map_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = MapStep::new(|_ctx, v| v.clone());
            assert_eq!(step.name(), "map");
        }

        #[test]
        fn clone_box_works() {
            let step = MapStep::new(|_ctx, v| v.clone());
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "map");
        }

        #[test]
        fn debug_format() {
            let step = MapStep::new(|_ctx, v| v.clone());
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("MapStep"));
        }
    }

    mod map_step_transform_tests {
        use super::*;

        #[test]
        fn identity_map_preserves_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| v.clone());
            let input = vec![
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
        fn doubles_integer_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| {
                if let Value::Int(n) = v {
                    Value::Int(n * 2)
                } else {
                    v.clone()
                }
            });
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(2));
            assert_eq!(output[1].value, Value::Int(4));
            assert_eq!(output[2].value, Value::Int(6));
        }

        #[test]
        fn converts_to_string() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| {
                let s = match v {
                    Value::Int(n) => format!("num:{}", n),
                    Value::String(s) => format!("str:{}", s),
                    _ => "other".to_string(),
                };
                Value::String(s)
            });
            let input = vec![
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::String("hello".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("num:42".to_string()));
            assert_eq!(output[1].value, Value::String("str:hello".to_string()));
        }

        #[test]
        fn can_access_execution_context() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Use context to get a vertex (context should be accessible)
            let step = MapStep::new(|ctx, v| {
                if let Value::Vertex(id) = v {
                    if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                        vertex
                            .properties
                            .get("name")
                            .cloned()
                            .unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                } else {
                    v.clone()
                }
            });
            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("Alice".to_string()));
        }
    }

    mod map_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| v.clone());

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.extend_path_labeled("start");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
        }

        #[test]
        fn preserves_loops_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| v.clone());

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.loops = 7;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].loops, 7);
        }

        #[test]
        fn preserves_bulk_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| v.clone());

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.bulk = 15;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 15);
        }
    }

    mod map_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| v.clone());
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    // =========================================================================
    // FlatMapStep Tests
    // =========================================================================

    mod flatmap_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            assert_eq!(step.name(), "flatMap");
        }

        #[test]
        fn clone_box_works() {
            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "flatMap");
        }

        #[test]
        fn debug_format() {
            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("FlatMapStep"));
        }
    }

    mod flatmap_step_transform_tests {
        use super::*;

        #[test]
        fn identity_flat_map_preserves_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
        }

        #[test]
        fn duplicates_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone(), v.clone()]);
            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 4);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(1));
            assert_eq!(output[2].value, Value::Int(2));
            assert_eq!(output[3].value, Value::Int(2));
        }

        #[test]
        fn generates_range_from_integer() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| {
                if let Value::Int(n) = v {
                    (0..*n).map(|i| Value::Int(i)).collect()
                } else {
                    vec![]
                }
            });
            let input = vec![Traverser::new(Value::Int(3))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(0));
            assert_eq!(output[1].value, Value::Int(1));
            assert_eq!(output[2].value, Value::Int(2));
        }

        #[test]
        fn can_filter_out_values_by_returning_empty_vec() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Only keep positive integers, filter out others
            let step = FlatMapStep::new(|_ctx, v| {
                if let Value::Int(n) = v {
                    if *n > 0 {
                        vec![v.clone()]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            });
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(-2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::String("hello".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(3));
        }

        #[test]
        fn can_access_execution_context() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Get all properties of a vertex as separate values
            let step = FlatMapStep::new(|ctx, v| {
                if let Value::Vertex(id) = v {
                    if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                        vertex.properties.values().cloned().collect()
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            });
            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice with name and age

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            let values: Vec<Value> = output.iter().map(|t| t.value.clone()).collect();
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::Int(30)));
        }
    }

    mod flatmap_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_on_split() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone(), v.clone()]);

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.extend_path_labeled("start");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert!(output[0].path.has_label("start"));
            assert!(output[1].path.has_label("start"));
        }

        #[test]
        fn preserves_loops_count_on_split() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone(), v.clone()]);

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.loops = 7;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].loops, 7);
            assert_eq!(output[1].loops, 7);
        }

        #[test]
        fn preserves_bulk_count_on_split() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone(), v.clone()]);

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.bulk = 15;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].bulk, 15);
            assert_eq!(output[1].bulk, 15);
        }
    }

    mod flatmap_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn empty_vec_result_produces_no_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, _v| vec![]);
            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }
}
