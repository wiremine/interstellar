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
///     .by(__::out("knows").count())
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

        crate::value::Value::Map(result)
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
                .snapshot()
                .storage()
                .get_vertex(*id)
                .and_then(|v| v.properties.get(key).cloned()),
            crate::value::Value::Edge(id) => ctx
                .snapshot()
                .storage()
                .get_edge(*id)
                .and_then(|e| e.properties.get(key).cloned()),
            _ => None,
        }
    }
}

impl crate::traversal::step::AnyStep for ProjectStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(move |t| {
            let value = self.transform(ctx, &t);
            t.with_value(value)
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "project"
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
///     .by(__::out("knows").count())
///     .build()
///     .to_list();
/// ```
pub struct ProjectBuilder<In> {
    steps: Vec<Box<dyn crate::traversal::step::AnyStep>>,
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
        steps: Vec<Box<dyn crate::traversal::step::AnyStep>>,
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
///     .by(__::out("knows").count())
///     .build()
///     .to_list();
/// ```
pub struct BoundProjectBuilder<'g, In> {
    snapshot: &'g crate::graph::GraphSnapshot<'g>,
    interner: &'g crate::storage::interner::StringInterner,
    source: Option<crate::traversal::TraversalSource>,
    steps: Vec<Box<dyn crate::traversal::step::AnyStep>>,
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
    /// * `snapshot` - Graph snapshot reference
    /// * `interner` - String interner reference
    /// * `source` - Optional traversal source
    /// * `steps` - Existing traversal steps
    /// * `keys` - The keys for the projection map
    /// * `track_paths` - Whether path tracking is enabled
    pub(crate) fn new(
        snapshot: &'g crate::graph::GraphSnapshot<'g>,
        interner: &'g crate::storage::interner::StringInterner,
        source: Option<crate::traversal::TraversalSource>,
        steps: Vec<Box<dyn crate::traversal::step::AnyStep>>,
        keys: Vec<String>,
        track_paths: bool,
    ) -> Self {
        Self {
            snapshot,
            interner,
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

        let mut bound =
            crate::traversal::source::BoundTraversal::new(self.snapshot, self.interner, traversal);

        if self.track_paths {
            bound = bound.with_path();
        }

        bound
    }
}

#[cfg(test)]
mod project_tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    use crate::traversal::context::ExecutionContext;
    use crate::traversal::step::AnyStep;
    use crate::traversal::{Traversal, Traverser};
    use crate::value::{Value, VertexId};
    use std::collections::HashMap;

    fn create_projection_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();

        // Vertex 0: Alice, age 30, 2 friends
        let mut props0 = HashMap::new();
        props0.insert("name".to_string(), Value::String("Alice".to_string()));
        props0.insert("age".to_string(), Value::Int(30));
        let alice = storage.add_vertex("person", props0);

        // Vertex 1: Bob, age 25, 1 friend
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), Value::String("Bob".to_string()));
        props1.insert("age".to_string(), Value::Int(25));
        let bob = storage.add_vertex("person", props1);

        // Vertex 2: Charlie, age 35, 0 friends
        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), Value::String("Charlie".to_string()));
        props2.insert("age".to_string(), Value::Int(35));
        let charlie = storage.add_vertex("person", props2);

        // Alice knows Bob and Charlie
        let _ = storage.add_edge(alice, bob, "knows", HashMap::new());
        let _ = storage.add_edge(alice, charlie, "knows", HashMap::new());

        // Bob knows Alice
        let _ = storage.add_edge(bob, alice, "knows", HashMap::new());

        Graph::new(std::sync::Arc::new(storage))
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
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "project");
        }
    }

    mod project_step_property_tests {
        use super::*;

        #[test]
        fn projects_single_property() {
            let graph = create_projection_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
