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

impl<F> crate::traversal::step::Step for MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static,
{
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let f = self.f.clone();
        input.map(move |t| {
            let new_value = f(ctx, &t.value);
            t.with_value(new_value)
        })
    }

    fn name(&self) -> &'static str {
        "map"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Transform
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // CLOSURE STEP: MapStep holds a closure that requires ExecutionContext (not StreamingContext).
        // The closure signature is: Fn(&ExecutionContext, &Value) -> Value
        // StreamingContext cannot be converted to ExecutionContext without graph mutation access.
        // For streaming transforms, use built-in steps (values, properties, etc.) instead.
        // Current behavior: pass-through (no transformation).
        Box::new(std::iter::once(input))
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

impl<F> crate::traversal::step::Step for FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync + 'static,
{
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let f = self.f.clone();
        input.flat_map(move |t| {
            let values = f(ctx, &t.value);
            values.into_iter().map(move |v| t.split(v))
        })
    }

    fn name(&self) -> &'static str {
        "flatMap"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Transform
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // CLOSURE STEP: FlatMapStep holds a closure that requires ExecutionContext (not StreamingContext).
        // The closure signature is: Fn(&ExecutionContext, &Value) -> Vec<Value>
        // StreamingContext cannot be converted to ExecutionContext without graph mutation access.
        // For streaming transforms, use built-in steps (out, in_, values, etc.) instead.
        // Current behavior: pass-through (no transformation).
        Box::new(std::iter::once(input))
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
    use crate::storage::Graph;
    use crate::traversal::step::{DynStep, Step};
    use crate::traversal::SnapshotLike;
    use crate::value::VertexId;
    use std::collections::HashMap;

    fn create_test_graph() -> Graph {
        let graph = Graph::new();

        // Vertex 0: person with name and age
        graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });

        graph
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
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "map");
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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Use context to get a vertex (context should be accessible)
            let step = MapStep::new(|ctx, v| {
                if let Value::Vertex(id) = v {
                    if let Some(vertex) = ctx.storage().get_vertex(*id) {
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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "flatMap");
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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| {
                if let Value::Int(n) = v {
                    (0..*n).map(Value::Int).collect()
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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Get all properties of a vertex as separate values
            let step = FlatMapStep::new(|ctx, v| {
                if let Value::Vertex(id) = v {
                    if let Some(vertex) = ctx.storage().get_vertex(*id) {
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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn empty_vec_result_produces_no_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = FlatMapStep::new(|_ctx, _v| vec![]);
            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }
}

// -----------------------------------------------------------------------------
// ProjectStep - creates named projections
// -----------------------------------------------------------------------------

use crate::traversal::step::execute_traversal_from;
use crate::traversal::Traversal;
use std::collections::HashMap;

/// Projection specification for a single key in project().
///
/// Each projection defines how to compute the value for a key in the
/// result map. It can either extract a property value directly or execute
/// a sub-traversal.
#[derive(Clone)]
pub enum Projection {
    /// Extract a property value by key from vertices/edges
    Key(String),
    /// Execute a sub-traversal to compute the value
    Traversal(Traversal<crate::value::Value, crate::value::Value>),
}

impl From<&str> for Projection {
    fn from(key: &str) -> Self {
        Projection::Key(key.to_string())
    }
}

impl From<String> for Projection {
    fn from(key: String) -> Self {
        Projection::Key(key)
    }
}

/// Transform step that creates named projections.
///
/// Project creates a map with specific named keys, where each key's value
/// is computed either from a property or by executing a sub-traversal.
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().hasLabel('person')
///     .project('name', 'age', 'friends')
///     .by('name')
///     .by('age')
///     .by(out('knows').count())
/// ```
///
/// # Example
///
/// ```ignore
/// let results = g.v().has_label("person")
///     .project(&["name", "friend_count"])
///     .by_key("name")
///     .by(__.out("knows").count())
///     .build()
///     .to_list();
/// // Results: [{name: "Alice", friend_count: 2}, ...]
/// ```
#[derive(Clone)]
pub struct ProjectStep {
    keys: Vec<String>,
    projections: Vec<Projection>,
}

impl ProjectStep {
    /// Create a new ProjectStep with keys and projections.
    ///
    /// # Arguments
    ///
    /// * `keys` - The output map keys
    /// * `projections` - How to compute each key's value
    ///
    /// # Panics
    ///
    /// Panics if keys and projections have different lengths.
    pub fn new(keys: Vec<String>, projections: Vec<Projection>) -> Self {
        assert_eq!(
            keys.len(),
            projections.len(),
            "ProjectStep: keys and projections must have the same length"
        );
        Self { keys, projections }
    }

    /// Transform a traverser into a projected map.
    fn transform(&self, ctx: &ExecutionContext, traverser: &Traverser) -> crate::value::Value {
        let mut result = HashMap::new();

        for (key, proj) in self.keys.iter().zip(self.projections.iter()) {
            let value = match proj {
                Projection::Key(prop_key) => {
                    // Get property value from element
                    self.get_property(ctx, traverser, prop_key)
                }
                Projection::Traversal(sub) => {
                    // Execute sub-traversal and collect results
                    let results: Vec<_> = execute_traversal_from(
                        ctx,
                        sub,
                        Box::new(std::iter::once(traverser.clone())),
                    )
                    .collect();

                    if results.is_empty() {
                        None
                    } else if results.len() == 1 {
                        // Single result - return the value directly
                        Some(results.into_iter().next().unwrap().value)
                    } else {
                        // Multiple results - return as list
                        Some(crate::value::Value::List(
                            results.into_iter().map(|t| t.value).collect(),
                        ))
                    }
                }
            };

            result.insert(key.clone(), value.unwrap_or(crate::value::Value::Null));
        }

        crate::value::Value::Map(result.into_iter().collect())
    }

    /// Get a property value from a traverser's element.
    fn get_property(
        &self,
        ctx: &ExecutionContext,
        t: &Traverser,
        key: &str,
    ) -> Option<crate::value::Value> {
        match &t.value {
            crate::value::Value::Vertex(id) => ctx
                .storage()
                .get_vertex(*id)
                .and_then(|v| v.properties.get(key).cloned()),
            crate::value::Value::Edge(id) => ctx
                .storage()
                .get_edge(*id)
                .and_then(|e| e.properties.get(key).cloned()),
            _ => None,
        }
    }
}

impl crate::traversal::step::Step for ProjectStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.map(move |t| {
            let value = self.transform(ctx, &t);
            t.with_value(value)
        })
    }

    fn name(&self) -> &'static str {
        "project"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Transform
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        use crate::traversal::step::execute_traversal_streaming;

        // Build the projected map
        let mut result = HashMap::new();

        for (key, proj) in self.keys.iter().zip(self.projections.iter()) {
            let value = match proj {
                Projection::Key(prop_key) => {
                    // Get property value from element using streaming context
                    self.get_property_streaming(&ctx, &input, prop_key)
                }
                Projection::Traversal(sub) => {
                    // Execute sub-traversal and collect results
                    let results: Vec<_> =
                        execute_traversal_streaming(&ctx, sub, input.clone()).collect();

                    if results.is_empty() {
                        None
                    } else if results.len() == 1 {
                        // Single result - return the value directly
                        Some(results.into_iter().next().unwrap().value)
                    } else {
                        // Multiple results - return as list
                        Some(crate::value::Value::List(
                            results.into_iter().map(|t| t.value).collect(),
                        ))
                    }
                }
            };

            result.insert(key.clone(), value.unwrap_or(crate::value::Value::Null));
        }

        let projected_value = crate::value::Value::Map(result.into_iter().collect());
        Box::new(std::iter::once(input.with_value(projected_value)))
    }
}

impl ProjectStep {
    /// Get a property value from a traverser's element using streaming context.
    fn get_property_streaming(
        &self,
        ctx: &crate::traversal::context::StreamingContext,
        t: &Traverser,
        key: &str,
    ) -> Option<crate::value::Value> {
        match &t.value {
            crate::value::Value::Vertex(id) => ctx
                .storage()
                .get_vertex(*id)
                .and_then(|v| v.properties.get(key).cloned()),
            crate::value::Value::Edge(id) => ctx
                .storage()
                .get_edge(*id)
                .and_then(|e| e.properties.get(key).cloned()),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ProjectBuilder - fluent API for building ProjectStep
// -----------------------------------------------------------------------------

use std::marker::PhantomData;

/// Fluent builder for creating ProjectStep with multiple projections.
///
/// The builder allows chaining multiple `by` clauses to define how each
/// projection key is computed.
///
/// # Example
///
/// ```ignore
/// // Project name and friend count
/// let results = g.v().has_label("person")
///     .project(&["name", "friends"])
///     .by_key("name")
///     .by(__.out("knows").count())
///     .build()
///     .to_list();
/// ```
pub struct ProjectBuilder<In> {
    steps: Vec<Box<dyn crate::traversal::step::DynStep>>,
    keys: Vec<String>,
    projections: Vec<Projection>,
    _phantom: PhantomData<In>,
}

impl<In> ProjectBuilder<In> {
    /// Create a new ProjectBuilder with existing steps and projection keys.
    ///
    /// # Arguments
    ///
    /// * `steps` - Existing traversal steps
    /// * `keys` - The keys for the projection map
    pub(crate) fn new(
        steps: Vec<Box<dyn crate::traversal::step::DynStep>>,
        keys: Vec<String>,
    ) -> Self {
        Self {
            steps,
            keys,
            projections: vec![],
            _phantom: PhantomData,
        }
    }

    /// Project using a property key.
    ///
    /// This is a shorthand for getting a property value from vertices/edges.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to extract
    pub fn by_key(mut self, key: &str) -> Self {
        self.projections.push(Projection::Key(key.to_string()));
        self
    }

    /// Project using a sub-traversal.
    ///
    /// The sub-traversal is executed for each input traverser, and the result(s)
    /// become the value for this projection key.
    ///
    /// # Arguments
    ///
    /// * `traversal` - The sub-traversal to execute
    pub fn by(mut self, traversal: Traversal<crate::value::Value, crate::value::Value>) -> Self {
        self.projections.push(Projection::Traversal(traversal));
        self
    }

    /// Build the final traversal with the ProjectStep.
    ///
    /// # Panics
    ///
    /// Panics if the number of `by` clauses doesn't match the number of keys.
    pub fn build(mut self) -> Traversal<In, crate::value::Value> {
        if self.projections.len() != self.keys.len() {
            panic!(
                "ProjectBuilder: expected {} by clauses, got {}",
                self.keys.len(),
                self.projections.len()
            );
        }

        let project_step = ProjectStep::new(self.keys, self.projections);
        self.steps.push(Box::new(project_step));

        Traversal {
            steps: self.steps,
            source: None,
            _phantom: PhantomData,
        }
    }
}

// -----------------------------------------------------------------------------
// BoundProjectBuilder - fluent API for bound traversals
// -----------------------------------------------------------------------------

/// Fluent builder for creating ProjectStep for bound traversals.
///
/// This builder is returned from `BoundTraversal::project()` and allows chaining
/// multiple `by` clauses before calling `build()` to get back a `BoundTraversal`.
///
/// # Example
///
/// ```ignore
/// // Project name and friend count
/// let results = g.v().has_label("person")
///     .project(&["name", "friends"])
///     .by_key("name")
///     .by(__.out("knows").count())
///     .build()
///     .to_list();
/// ```
pub struct BoundProjectBuilder<'g, In> {
    snapshot: &'g dyn crate::traversal::SnapshotLike,
    source: Option<crate::traversal::TraversalSource>,
    steps: Vec<Box<dyn crate::traversal::step::DynStep>>,
    keys: Vec<String>,
    projections: Vec<Projection>,
    track_paths: bool,
    _phantom: PhantomData<In>,
}

impl<'g, In> BoundProjectBuilder<'g, In> {
    /// Create a new BoundProjectBuilder with existing steps, graph references, and keys.
    ///
    /// # Arguments
    ///
    /// * `snapshot` - Snapshot reference for graph access
    /// * `source` - Optional traversal source
    /// * `steps` - Existing traversal steps
    /// * `keys` - The keys for the projection map
    /// * `track_paths` - Whether path tracking is enabled
    pub(crate) fn new(
        snapshot: &'g dyn crate::traversal::SnapshotLike,
        source: Option<crate::traversal::TraversalSource>,
        steps: Vec<Box<dyn crate::traversal::step::DynStep>>,
        keys: Vec<String>,
        track_paths: bool,
    ) -> Self {
        Self {
            snapshot,
            source,
            steps,
            keys,
            projections: vec![],
            track_paths,
            _phantom: PhantomData,
        }
    }

    /// Project using a property key.
    ///
    /// This is a shorthand for getting a property value from vertices/edges.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to extract
    pub fn by_key(mut self, key: &str) -> Self {
        self.projections.push(Projection::Key(key.to_string()));
        self
    }

    /// Project using a sub-traversal.
    ///
    /// The sub-traversal is executed for each input traverser, and the result(s)
    /// become the value for this projection key.
    ///
    /// # Arguments
    ///
    /// * `traversal` - The sub-traversal to execute
    pub fn by(mut self, traversal: Traversal<crate::value::Value, crate::value::Value>) -> Self {
        self.projections.push(Projection::Traversal(traversal));
        self
    }

    /// Build the final bound traversal with the ProjectStep.
    ///
    /// # Panics
    ///
    /// Panics if the number of `by` clauses doesn't match the number of keys.
    pub fn build(
        mut self,
    ) -> crate::traversal::source::BoundTraversal<'g, In, crate::value::Value> {
        if self.projections.len() != self.keys.len() {
            panic!(
                "BoundProjectBuilder: expected {} by clauses, got {}",
                self.keys.len(),
                self.projections.len()
            );
        }

        let project_step = ProjectStep::new(self.keys, self.projections);
        self.steps.push(Box::new(project_step));

        let traversal = Traversal {
            steps: self.steps,
            source: self.source,
            _phantom: PhantomData,
        };

        let mut bound = crate::traversal::source::BoundTraversal::new(self.snapshot, traversal);

        if self.track_paths {
            bound = bound.with_path();
        }

        bound
    }
}

// -----------------------------------------------------------------------------
// MathStep - mathematical expression evaluator
// -----------------------------------------------------------------------------

#[cfg(feature = "gql")]
use mathexpr::Expression;

/// Mathematical expression evaluator step.
///
/// Evaluates arithmetic expressions with variables from the traversal path.
/// The special variable `_` represents the current traverser value.
/// Other variables reference labeled path values from `as()` steps.
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
/// // Double the current value
/// g.v().values("age").math("_ * 2").build().to_list()
///
/// // Calculate age difference between labeled vertices
/// g.v().as_("a").out("knows").as_("b")
///     .math("a - b")
///     .by("a", "age")
///     .by("b", "age")
///     .build()
///     .to_list()
///
/// // Complex expression with functions
/// g.v().values("x").math("sqrt(_ ^ 2 + 1)").build().to_list()
/// ```
#[cfg(feature = "gql")]
#[derive(Clone)]
pub struct MathStep {
    /// The mathematical expression string
    expression: String,
    /// Variable name to property key mapping for labeled path values
    variable_keys: HashMap<String, String>,
}

#[cfg(feature = "gql")]
impl MathStep {
    /// Create a new MathStep with the given expression.
    ///
    /// # Arguments
    ///
    /// * `expression` - The mathematical expression to evaluate
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = MathStep::new("_ * 2");
    /// ```
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
            variable_keys: HashMap::new(),
        }
    }

    /// Create a MathStep with pre-configured variable bindings.
    ///
    /// # Arguments
    ///
    /// * `expression` - The mathematical expression to evaluate
    /// * `bindings` - Map of variable names to property keys
    pub fn with_bindings(expression: impl Into<String>, bindings: HashMap<String, String>) -> Self {
        Self {
            expression: expression.into(),
            variable_keys: bindings,
        }
    }

    /// Evaluate the expression for a given traverser.
    fn evaluate(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Option<Value> {
        // Collect variable names and values in order
        let mut var_names: Vec<String> = Vec::new();
        let mut var_values: Vec<f64> = Vec::new();

        for (var, prop_key) in &self.variable_keys {
            var_names.push(var.clone());
            let value = self.get_labeled_value(ctx, traverser, var, prop_key)?;
            var_values.push(value);
        }

        // Try to get current value as f64 (may be None if current value is not numeric)
        let current_value = self.value_to_f64(&traverser.value);

        // Evaluate the expression
        let result = self.evaluate_expression(current_value, &var_names, &var_values)?;

        Some(Value::Float(result))
    }

    /// Helper to evaluate the expression with given variable bindings.
    fn evaluate_expression(
        &self,
        current: Option<f64>,
        var_names: &[String],
        var_values: &[f64],
    ) -> Option<f64> {
        // Convert var_names to &str for mathexpr
        let var_name_refs: Vec<&str> = var_names.iter().map(|s| s.as_str()).collect();

        let parsed = Expression::parse(&self.expression).ok()?;
        let compiled = parsed.compile(&var_name_refs).ok()?;

        let result = if compiled.uses_current_value() {
            // If expression uses current value, we need a valid numeric current value
            let current_val = current?;
            compiled.eval_with_current(current_val, var_values).ok()?
        } else {
            compiled.eval(var_values).ok()?
        };

        // Check for NaN/Inf which indicate domain errors
        if result.is_nan() || result.is_infinite() {
            return None;
        }

        Some(result)
    }

    /// Convert a Value to f64.
    fn value_to_f64(&self, value: &Value) -> Option<f64> {
        match value {
            Value::Int(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            _ => None, // Non-numeric values cannot be used in math
        }
    }

    /// Get a labeled value from the path, extracting the specified property.
    fn get_labeled_value(
        &self,
        ctx: &ExecutionContext,
        traverser: &Traverser,
        label: &str,
        prop_key: &str,
    ) -> Option<f64> {
        // Get the first value with this label from the path
        let path_values = traverser.path.get(label)?;
        let path_value = path_values.first()?;

        // Convert PathValue to Value
        let value = path_value.to_value();

        // Extract the property from the element
        self.extract_number(ctx, &value, prop_key)
    }

    /// Extract a numeric value from an element's property.
    fn extract_number(&self, ctx: &ExecutionContext, value: &Value, key: &str) -> Option<f64> {
        match value {
            Value::Int(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            Value::Vertex(id) => {
                let vertex = ctx.storage().get_vertex(*id)?;
                match vertex.properties.get(key)? {
                    Value::Int(n) => Some(*n as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                }
            }
            Value::Edge(id) => {
                let edge = ctx.storage().get_edge(*id)?;
                match edge.properties.get(key)? {
                    Value::Int(n) => Some(*n as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

#[cfg(feature = "gql")]
impl crate::traversal::step::Step for MathStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.filter_map(move |t| self.evaluate(ctx, &t).map(|value| t.with_value(value)))
    }

    fn name(&self) -> &'static str {
        "math"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Transform
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Evaluate the expression using streaming context
        match self.evaluate_streaming(&ctx, &input) {
            Some(value) => Box::new(std::iter::once(input.with_value(value))),
            None => Box::new(std::iter::empty()),
        }
    }
}

#[cfg(feature = "gql")]
impl MathStep {
    /// Evaluate the expression for a given traverser using streaming context.
    fn evaluate_streaming(
        &self,
        ctx: &crate::traversal::context::StreamingContext,
        traverser: &Traverser,
    ) -> Option<Value> {
        // Collect variable names and values in order
        let mut var_names: Vec<String> = Vec::new();
        let mut var_values: Vec<f64> = Vec::new();

        for (var, prop_key) in &self.variable_keys {
            var_names.push(var.clone());
            let value = self.get_labeled_value_streaming(ctx, traverser, var, prop_key)?;
            var_values.push(value);
        }

        // Try to get current value as f64 (may be None if current value is not numeric)
        let current_value = self.value_to_f64(&traverser.value);

        // Evaluate using the existing helper (it doesn't need ExecutionContext)
        let result = self.evaluate_expression(current_value, &var_names, &var_values)?;

        Some(Value::Float(result))
    }

    /// Get a labeled value from the path using streaming context.
    fn get_labeled_value_streaming(
        &self,
        ctx: &crate::traversal::context::StreamingContext,
        traverser: &Traverser,
        label: &str,
        prop_key: &str,
    ) -> Option<f64> {
        // Get the first value with this label from the path
        let path_values = traverser.path.get(label)?;
        let path_value = path_values.first()?;

        // Convert PathValue to Value
        let value = path_value.to_value();

        // Extract the property from the element
        self.extract_number_streaming(ctx, &value, prop_key)
    }

    /// Extract a numeric value from an element's property using streaming context.
    fn extract_number_streaming(
        &self,
        ctx: &crate::traversal::context::StreamingContext,
        value: &Value,
        key: &str,
    ) -> Option<f64> {
        match value {
            Value::Int(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            Value::Vertex(id) => {
                let vertex = ctx.storage().get_vertex(*id)?;
                match vertex.properties.get(key)? {
                    Value::Int(n) => Some(*n as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                }
            }
            Value::Edge(id) => {
                let edge = ctx.storage().get_edge(*id)?;
                match edge.properties.get(key)? {
                    Value::Int(n) => Some(*n as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

#[cfg(feature = "gql")]
impl std::fmt::Debug for MathStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MathStep")
            .field("expression", &self.expression)
            .field("variable_keys", &self.variable_keys)
            .finish()
    }
}

// -----------------------------------------------------------------------------
// MathBuilder - fluent API for building MathStep
// -----------------------------------------------------------------------------

/// Builder for configuring math() step with by() modulators.
///
/// Each `by()` call binds a variable in the expression to a property key.
/// Variables are bound explicitly by name.
///
/// # Example
///
/// ```ignore
/// g.v().as_("a").out().as_("b")
///     .math("a - b")
///     .by("a", "age")  // Extract age from labeled "a"
///     .by("b", "age")  // Extract age from labeled "b"
///     .build()
/// ```
#[cfg(feature = "gql")]
pub struct MathBuilder<In> {
    steps: Vec<Box<dyn crate::traversal::step::DynStep>>,
    expression: String,
    variable_bindings: HashMap<String, String>,
    _phantom: PhantomData<In>,
}

#[cfg(feature = "gql")]
impl<In> MathBuilder<In> {
    /// Create a new MathBuilder with existing steps and expression.
    ///
    /// # Arguments
    ///
    /// * `steps` - Existing traversal steps
    /// * `expression` - The mathematical expression to evaluate
    pub(crate) fn new(
        steps: Vec<Box<dyn crate::traversal::step::DynStep>>,
        expression: impl Into<String>,
    ) -> Self {
        Self {
            steps,
            expression: expression.into(),
            variable_bindings: HashMap::new(),
            _phantom: PhantomData,
        }
    }

    /// Bind a variable to a property key.
    ///
    /// # Arguments
    ///
    /// * `variable` - Variable name from expression (e.g., "a", "b")
    /// * `key` - Property key to extract from labeled element
    ///
    /// # Examples
    ///
    /// ```ignore
    /// g.v().as_("a").out().as_("b")
    ///     .math("a - b")
    ///     .by("a", "age")  // Extract age from labeled "a"
    ///     .by("b", "age")  // Extract age from labeled "b"
    ///     .build()
    /// ```
    pub fn by(mut self, variable: &str, key: &str) -> Self {
        self.variable_bindings
            .insert(variable.to_string(), key.to_string());
        self
    }

    /// Finalize the math() step and return the traversal.
    pub fn build(mut self) -> Traversal<In, Value> {
        let step = MathStep::with_bindings(self.expression, self.variable_bindings);
        self.steps.push(Box::new(step));
        Traversal {
            steps: self.steps,
            source: None,
            _phantom: PhantomData,
        }
    }
}

// -----------------------------------------------------------------------------
// BoundMathBuilder - fluent API for bound traversals
// -----------------------------------------------------------------------------

/// Builder for configuring math() step for bound traversals.
///
/// This builder is returned from `BoundTraversal::math()` and allows chaining
/// `by()` clauses before calling `build()` to get back a `BoundTraversal`.
#[cfg(feature = "gql")]
pub struct BoundMathBuilder<'g, In> {
    snapshot: &'g dyn crate::traversal::SnapshotLike,
    source: Option<crate::traversal::TraversalSource>,
    steps: Vec<Box<dyn crate::traversal::step::DynStep>>,
    expression: String,
    variable_bindings: HashMap<String, String>,
    track_paths: bool,
    _phantom: PhantomData<In>,
}

#[cfg(feature = "gql")]
impl<'g, In> BoundMathBuilder<'g, In> {
    /// Create a new BoundMathBuilder with existing steps, graph references, and expression.
    pub(crate) fn new(
        snapshot: &'g dyn crate::traversal::SnapshotLike,
        source: Option<crate::traversal::TraversalSource>,
        steps: Vec<Box<dyn crate::traversal::step::DynStep>>,
        expression: impl Into<String>,
        track_paths: bool,
    ) -> Self {
        Self {
            snapshot,
            source,
            steps,
            expression: expression.into(),
            variable_bindings: HashMap::new(),
            track_paths,
            _phantom: PhantomData,
        }
    }

    /// Bind a variable to a property key.
    ///
    /// # Arguments
    ///
    /// * `variable` - Variable name from expression (e.g., "a", "b")
    /// * `key` - Property key to extract from labeled element
    pub fn by(mut self, variable: &str, key: &str) -> Self {
        self.variable_bindings
            .insert(variable.to_string(), key.to_string());
        self
    }

    /// Finalize the math() step and return the bound traversal.
    pub fn build(mut self) -> crate::traversal::source::BoundTraversal<'g, In, Value> {
        let step = MathStep::with_bindings(self.expression, self.variable_bindings);
        self.steps.push(Box::new(step));

        let traversal = Traversal {
            steps: self.steps,
            source: self.source,
            _phantom: PhantomData,
        };

        let mut bound = crate::traversal::source::BoundTraversal::new(self.snapshot, traversal);

        if self.track_paths {
            bound = bound.with_path();
        }

        bound
    }
}

#[cfg(test)]
mod project_tests {
    use super::*;
    use crate::storage::Graph;
    use crate::traversal::context::ExecutionContext;
    use crate::traversal::step::{DynStep, Step};
    use crate::traversal::SnapshotLike;
    use crate::traversal::{Traversal, Traverser};
    use crate::value::{Value, VertexId};
    use std::collections::HashMap;

    fn create_projection_test_graph() -> Graph {
        let graph = Graph::new();

        // Vertex 0: Alice, age 30, 2 friends
        let mut props0 = HashMap::new();
        props0.insert("name".to_string(), Value::String("Alice".to_string()));
        props0.insert("age".to_string(), Value::Int(30));
        let alice = graph.add_vertex("person", props0);

        // Vertex 1: Bob, age 25, 1 friend
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), Value::String("Bob".to_string()));
        props1.insert("age".to_string(), Value::Int(25));
        let bob = graph.add_vertex("person", props1);

        // Vertex 2: Charlie, age 35, 0 friends
        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), Value::String("Charlie".to_string()));
        props2.insert("age".to_string(), Value::Int(35));
        let charlie = graph.add_vertex("person", props2);

        // Alice knows Bob and Charlie
        let _ = graph.add_edge(alice, bob, "knows", HashMap::new());
        let _ = graph.add_edge(alice, charlie, "knows", HashMap::new());

        // Bob knows Alice
        let _ = graph.add_edge(bob, alice, "knows", HashMap::new());

        graph
    }

    mod project_step_construction {
        use super::*;

        #[test]
        fn new_creates_step_with_keys_and_projections() {
            let keys = vec!["name".to_string(), "age".to_string()];
            let projections = vec![
                Projection::Key("name".to_string()),
                Projection::Key("age".to_string()),
            ];
            let step = ProjectStep::new(keys, projections);
            assert_eq!(step.name(), "project");
        }

        #[test]
        #[should_panic(expected = "keys and projections must have the same length")]
        fn new_panics_with_mismatched_lengths() {
            let keys = vec!["name".to_string()];
            let projections = vec![
                Projection::Key("name".to_string()),
                Projection::Key("age".to_string()),
            ];
            ProjectStep::new(keys, projections);
        }

        #[test]
        fn clone_box_works() {
            let keys = vec!["name".to_string()];
            let projections = vec![Projection::Key("name".to_string())];
            let step = ProjectStep::new(keys, projections);
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "project");
        }
    }

    mod project_step_property_tests {
        use super::*;

        #[test]
        fn projects_single_property() {
            let graph = create_projection_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ProjectStep::new(
                vec!["name".to_string()],
                vec![Projection::Key("name".to_string())],
            );

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.len(), 1);
                assert_eq!(map.get("name"), Some(&Value::String("Alice".to_string())));
            } else {
                panic!("Expected Map value");
            }
        }

        #[test]
        fn projects_multiple_properties() {
            let graph = create_projection_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ProjectStep::new(
                vec!["name".to_string(), "age".to_string()],
                vec![
                    Projection::Key("name".to_string()),
                    Projection::Key("age".to_string()),
                ],
            );

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.len(), 2);
                assert_eq!(map.get("name"), Some(&Value::String("Alice".to_string())));
                assert_eq!(map.get("age"), Some(&Value::Int(30)));
            } else {
                panic!("Expected Map value");
            }
        }

        #[test]
        fn missing_property_produces_null() {
            let graph = create_projection_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ProjectStep::new(
                vec!["name".to_string(), "missing".to_string()],
                vec![
                    Projection::Key("name".to_string()),
                    Projection::Key("missing".to_string()),
                ],
            );

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.len(), 2);
                assert_eq!(map.get("name"), Some(&Value::String("Alice".to_string())));
                assert_eq!(map.get("missing"), Some(&Value::Null));
            } else {
                panic!("Expected Map value");
            }
        }
    }

    mod project_step_traversal_tests {
        // Note: More complex traversal tests should be done as integration tests
        // to avoid type inference issues in unit tests
    }

    mod project_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path() {
            let graph = create_projection_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ProjectStep::new(
                vec!["name".to_string()],
                vec![Projection::Key("name".to_string())],
            );

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
        }

        #[test]
        fn preserves_loops_count() {
            let graph = create_projection_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ProjectStep::new(
                vec!["name".to_string()],
                vec![Projection::Key("name".to_string())],
            );

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.loops = 7;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].loops, 7);
        }

        #[test]
        fn preserves_bulk_count() {
            let graph = create_projection_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ProjectStep::new(
                vec!["name".to_string()],
                vec![Projection::Key("name".to_string())],
            );

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.bulk = 15;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 15);
        }
    }

    mod project_step_non_element_tests {
        use super::*;

        #[test]
        fn non_element_produces_empty_projection() {
            let graph = create_projection_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ProjectStep::new(
                vec!["name".to_string(), "age".to_string()],
                vec![
                    Projection::Key("name".to_string()),
                    Projection::Key("age".to_string()),
                ],
            );

            let input = vec![Traverser::new(Value::String("not a vertex".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.len(), 2);
                // Both should be null since we can't get properties from a string
                assert_eq!(map.get("name"), Some(&Value::Null));
                assert_eq!(map.get("age"), Some(&Value::Null));
            } else {
                panic!("Expected Map value");
            }
        }
    }

    mod project_builder_tests {
        use super::*;

        #[test]
        fn builder_constructs_with_by_key() {
            let builder = ProjectBuilder::<Value>::new(vec![], vec!["name".to_string()]);
            let traversal = builder.by_key("name").build();
            assert_eq!(traversal.steps.len(), 1);
        }

        #[test]
        fn builder_constructs_with_multiple_by() {
            let builder =
                ProjectBuilder::<Value>::new(vec![], vec!["name".to_string(), "age".to_string()]);
            let traversal = builder.by_key("name").by_key("age").build();
            assert_eq!(traversal.steps.len(), 1);
        }

        #[test]
        #[should_panic(expected = "expected 2 by clauses, got 1")]
        fn builder_panics_with_too_few_by_clauses() {
            let builder =
                ProjectBuilder::<Value>::new(vec![], vec!["name".to_string(), "age".to_string()]);
            builder.by_key("name").build(); // Missing second by clause
        }

        #[test]
        fn builder_supports_mixed_projections() {
            let sub_traversal = Traversal::<Value, Value>::new();
            let builder =
                ProjectBuilder::<Value>::new(vec![], vec!["name".to_string(), "count".to_string()]);
            let traversal = builder.by_key("name").by(sub_traversal).build();
            assert_eq!(traversal.steps.len(), 1);
        }
    }
}

#[cfg(all(test, feature = "gql"))]
mod math_tests {
    use super::*;
    use crate::storage::Graph;
    use crate::traversal::context::{ExecutionContext, SnapshotLike};
    use crate::traversal::step::{DynStep, Step};
    use crate::traversal::Traverser;
    use crate::value::Value;
    use std::collections::HashMap;

    fn create_math_test_graph() -> Graph {
        let graph = Graph::new();

        // Vertex 0: Alice, age 30
        let mut props0 = HashMap::new();
        props0.insert("name".to_string(), Value::String("Alice".to_string()));
        props0.insert("age".to_string(), Value::Int(30));
        props0.insert("score".to_string(), Value::Float(85.5));
        let alice = graph.add_vertex("person", props0);

        // Vertex 1: Bob, age 25
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), Value::String("Bob".to_string()));
        props1.insert("age".to_string(), Value::Int(25));
        props1.insert("score".to_string(), Value::Float(92.0));
        let bob = graph.add_vertex("person", props1);

        // Alice knows Bob
        let _ = graph.add_edge(alice, bob, "knows", HashMap::new());

        graph
    }

    mod math_step_construction {
        use super::*;

        #[test]
        fn new_creates_math_step() {
            let step = MathStep::new("_ * 2");
            assert_eq!(step.name(), "math");
        }

        #[test]
        fn with_bindings_creates_step_with_variables() {
            let mut bindings = HashMap::new();
            bindings.insert("a".to_string(), "age".to_string());
            let step = MathStep::with_bindings("a + 10", bindings);
            assert_eq!(step.name(), "math");
        }

        #[test]
        fn clone_box_works() {
            let step = MathStep::new("_ + 1");
            let cloned = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "math");
        }

        #[test]
        fn debug_format_works() {
            let step = MathStep::new("_ * 3");
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("MathStep"));
            assert!(debug_str.contains("_ * 3"));
        }
    }

    mod math_step_basic_arithmetic {
        use super::*;

        #[test]
        fn multiply_current_value() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ * 2");
            let input = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Int(5)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Float(20.0));
            assert_eq!(output[1].value, Value::Float(10.0));
        }

        #[test]
        fn add_to_current_value() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ + 100");
            let input = vec![Traverser::new(Value::Int(50))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(150.0));
        }

        #[test]
        fn subtract_from_current_value() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ - 5");
            let input = vec![Traverser::new(Value::Int(20))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(15.0));
        }

        #[test]
        fn divide_current_value() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ / 2");
            let input = vec![Traverser::new(Value::Int(10))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(5.0));
        }

        #[test]
        fn modulo_current_value() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ % 7");
            let input = vec![Traverser::new(Value::Int(15))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(1.0));
        }

        #[test]
        fn power_current_value() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ ^ 2");
            let input = vec![Traverser::new(Value::Int(5))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(25.0));
        }

        #[test]
        fn works_with_float_input() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ * 2");
            let input = vec![Traverser::new(Value::Float(3.5))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(7.0));
        }
    }

    mod math_step_functions {
        use super::*;

        #[test]
        fn sqrt_function() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("sqrt(_)");
            let input = vec![Traverser::new(Value::Int(16))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(4.0));
        }

        #[test]
        fn abs_function() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("abs(_)");
            let input = vec![Traverser::new(Value::Int(-42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(42.0));
        }

        #[test]
        fn complex_expression_with_sqrt() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // sqrt(3^2 + 4^2) = sqrt(9 + 16) = sqrt(25) = 5
            let step = MathStep::new("sqrt(_ ^ 2 + 16)");
            let input = vec![Traverser::new(Value::Int(3))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(5.0));
        }

        #[test]
        fn pi_constant() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ * pi");
            let input = vec![Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Float(f) = output[0].value {
                assert!((f - 2.0 * std::f64::consts::PI).abs() < 1e-10);
            } else {
                panic!("Expected Float value");
            }
        }

        #[test]
        fn e_constant() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ * e");
            let input = vec![Traverser::new(Value::Int(1))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Float(f) = output[0].value {
                assert!((f - std::f64::consts::E).abs() < 1e-10);
            } else {
                panic!("Expected Float value");
            }
        }
    }

    mod math_step_filtering {
        use super::*;

        #[test]
        fn non_numeric_values_filtered_out() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ * 2");
            let input = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::String("hello".to_string())),
                Traverser::new(Value::Int(5)),
                Traverser::new(Value::Bool(true)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only numeric values should remain
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Float(20.0));
            assert_eq!(output[1].value, Value::Float(10.0));
        }

        #[test]
        fn division_by_zero_filtered_out() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("1 / _");
            let input = vec![
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(0)), // Division by zero
                Traverser::new(Value::Int(4)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Division by zero should be filtered (produces infinity)
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Float(0.5));
            assert_eq!(output[1].value, Value::Float(0.25));
        }

        #[test]
        fn sqrt_of_negative_filtered_out() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("sqrt(_)");
            let input = vec![
                Traverser::new(Value::Int(4)),
                Traverser::new(Value::Int(-1)), // sqrt of negative
                Traverser::new(Value::Int(9)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // sqrt of negative produces NaN, should be filtered
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Float(2.0));
            assert_eq!(output[1].value, Value::Float(3.0));
        }
    }

    mod math_step_metadata {
        use super::*;

        #[test]
        fn preserves_path() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ * 2");
            let mut traverser = Traverser::new(Value::Int(10));
            traverser.extend_path_labeled("start");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
        }

        #[test]
        fn preserves_loops_count() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ + 1");
            let mut traverser = Traverser::new(Value::Int(5));
            traverser.loops = 7;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].loops, 7);
        }

        #[test]
        fn preserves_bulk() {
            let graph = create_math_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = MathStep::new("_ * 3");
            let mut traverser = Traverser::new(Value::Int(2));
            traverser.bulk = 15;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 15);
        }
    }

    mod math_builder_tests {
        use super::*;

        #[test]
        fn builder_creates_basic_traversal() {
            let builder = MathBuilder::<Value>::new(vec![], "_ * 2");
            let traversal = builder.build();
            assert_eq!(traversal.steps.len(), 1);
        }

        #[test]
        fn builder_with_by_adds_variable_binding() {
            let builder = MathBuilder::<Value>::new(vec![], "a + b");
            let traversal = builder.by("a", "age").by("b", "score").build();
            assert_eq!(traversal.steps.len(), 1);
        }
    }
}
