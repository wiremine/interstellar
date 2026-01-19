//! Step trait and basic step implementations.
//!
//! The `AnyStep` trait provides type-erased step execution, enabling:
//! - Storing heterogeneous steps in `Vec<Box<dyn AnyStep>>`
//! - Anonymous traversals without graph binding at construction
//! - Cloning traversals for branching operations
//!
//! This module also provides helper macros for implementing common step patterns:
//! - `impl_filter_step!` for 1:1 filter steps
//! - `impl_flatmap_step!` for 1:N expansion steps

use crate::traversal::{ExecutionContext, Traverser};

/// Type-erased step trait.
///
/// This is the core abstraction that enables:
/// - Storing heterogeneous steps in `Vec<Box<dyn AnyStep>>`
/// - Anonymous traversals without graph binding at construction
/// - Cloning traversals for branching operations
///
/// # Design Notes
///
/// - Input and output are both `Iterator<Item = Traverser>` (using `Value` internally)
/// - Steps receive `ExecutionContext` to access graph data
/// - Steps must be cloneable (`clone_box`) for traversal cloning
///
/// # Example
///
/// ```ignore
/// struct MyFilterStep {
///     threshold: i64,
/// }
///
/// impl AnyStep for MyFilterStep {
///     fn apply<'a>(
///         &'a self,
///         _ctx: &'a ExecutionContext<'a>,
///         input: Box<dyn Iterator<Item = Traverser> + 'a>,
///     ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
///         let threshold = self.threshold;
///         Box::new(input.filter(move |t| {
///             matches!(&t.value, Value::Int(n) if *n > threshold)
///         }))
///     }
///
///     fn clone_box(&self) -> Box<dyn AnyStep> {
///         Box::new(self.clone())
///     }
///
///     fn name(&self) -> &'static str {
///         "myFilter"
///     }
/// }
/// ```
pub trait AnyStep: Send + Sync {
    /// Apply this step to input traversers, producing output traversers.
    ///
    /// The returned iterator is boxed to enable type erasure.
    /// Steps that need graph access use the provided `ExecutionContext`.
    ///
    /// # Arguments
    ///
    /// * `ctx` - Execution context providing graph access and side effects
    /// * `input` - Iterator of input traversers
    ///
    /// # Returns
    ///
    /// A boxed iterator of output traversers
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a>;

    /// Clone this step into a boxed trait object.
    ///
    /// This is required for cloning traversals (e.g., for branching operations
    /// like `union()` or `coalesce()`).
    fn clone_box(&self) -> Box<dyn AnyStep>;

    /// Get step name for debugging and profiling.
    ///
    /// Returns a static string identifying the step type.
    fn name(&self) -> &'static str;
}

// Enable cloning of Box<dyn AnyStep>
impl Clone for Box<dyn AnyStep> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Helper macro to implement `AnyStep` for simple filter steps.
///
/// Filter steps pass through or reject traversers based on a predicate.
/// The step struct must:
/// - Implement `Clone`
/// - Have a `matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool` method
///
/// # Example
///
/// ```ignore
/// #[derive(Clone)]
/// struct IsPositiveStep;
///
/// impl IsPositiveStep {
///     fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
///         matches!(&traverser.value, Value::Int(n) if *n > 0)
///     }
/// }
///
/// impl_filter_step!(IsPositiveStep, "isPositive");
/// ```
#[macro_export]
macro_rules! impl_filter_step {
    ($step:ty, $name:literal) => {
        impl $crate::traversal::step::AnyStep for $step {
            fn apply<'a>(
                &'a self,
                ctx: &'a $crate::traversal::ExecutionContext<'a>,
                input: Box<dyn Iterator<Item = $crate::traversal::Traverser> + 'a>,
            ) -> Box<dyn Iterator<Item = $crate::traversal::Traverser> + 'a> {
                let step = self.clone();
                Box::new(input.filter(move |t| step.matches(ctx, t)))
            }

            fn clone_box(&self) -> Box<dyn $crate::traversal::step::AnyStep> {
                Box::new(self.clone())
            }

            fn name(&self) -> &'static str {
                $name
            }
        }
    };
}

/// Helper macro to implement `AnyStep` for flatmap steps (1:N mappings).
///
/// Flatmap steps expand each input traverser into zero or more output traversers.
/// The step struct must:
/// - Implement `Clone`
/// - Have an `expand(&self, ctx: &ExecutionContext, traverser: Traverser) -> impl Iterator<Item = Traverser>` method
///
/// # Example
///
/// ```ignore
/// #[derive(Clone)]
/// struct DuplicateStep {
///     count: usize,
/// }
///
/// impl DuplicateStep {
///     fn expand(&self, _ctx: &ExecutionContext, traverser: Traverser) -> impl Iterator<Item = Traverser> {
///         (0..self.count).map(move |_| traverser.clone())
///     }
/// }
///
/// impl_flatmap_step!(DuplicateStep, "duplicate");
/// ```
#[macro_export]
macro_rules! impl_flatmap_step {
    ($step:ty, $name:literal) => {
        impl $crate::traversal::step::AnyStep for $step {
            fn apply<'a>(
                &'a self,
                ctx: &'a $crate::traversal::ExecutionContext<'a>,
                input: Box<dyn Iterator<Item = $crate::traversal::Traverser> + 'a>,
            ) -> Box<dyn Iterator<Item = $crate::traversal::Traverser> + 'a> {
                let step = self.clone();
                Box::new(input.flat_map(move |t| step.expand(ctx, t)))
            }

            fn clone_box(&self) -> Box<dyn $crate::traversal::step::AnyStep> {
                Box::new(self.clone())
            }

            fn name(&self) -> &'static str {
                $name
            }
        }
    };
}

// Re-export macros at crate level for convenient access
pub use crate::impl_filter_step;
pub use crate::impl_flatmap_step;

// -----------------------------------------------------------------------------
// Basic Step Implementations
// -----------------------------------------------------------------------------

use crate::traversal::TraversalSource;
use crate::value::Value;

/// Identity step - passes input through unchanged.
///
/// This step is useful as a placeholder or for testing.
/// It simply returns all input traversers without modification.
///
/// # Example
///
/// ```ignore
/// let step = IdentityStep;
/// // input traversers pass through unchanged
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct IdentityStep;

impl IdentityStep {
    /// Create a new identity step.
    pub fn new() -> Self {
        Self
    }
}

impl AnyStep for IdentityStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        input // Pass through unchanged
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "identity"
    }
}

// -----------------------------------------------------------------------------
// StartStep - produces initial traversers from source
// -----------------------------------------------------------------------------

/// Start step - produces initial traversers from a traversal source.
///
/// This step is used internally by the traversal executor to expand
/// `TraversalSource` into an iterator of `Traverser` objects.
///
/// # Behavior
///
/// - `AllVertices`: Iterates all vertices in the graph
/// - `Vertices(ids)`: Iterates specific vertices by ID (filters non-existent)
/// - `AllEdges`: Iterates all edges in the graph
/// - `Edges(ids)`: Iterates specific edges by ID (filters non-existent)
/// - `Inject(values)`: Creates traversers from arbitrary values
///
/// # Example
///
/// ```ignore
/// let step = StartStep::new(TraversalSource::AllVertices);
/// let traversers = step.apply(&ctx, Box::new(std::iter::empty()));
/// // Produces one Traverser per vertex in the graph
/// ```
#[derive(Clone, Debug)]
pub struct StartStep {
    source: TraversalSource,
}

impl StartStep {
    /// Create a new start step with the given source.
    pub fn new(source: TraversalSource) -> Self {
        Self { source }
    }

    /// Create a start step for all vertices.
    pub fn all_vertices() -> Self {
        Self::new(TraversalSource::AllVertices)
    }

    /// Create a start step for specific vertex IDs.
    pub fn vertices(ids: Vec<crate::value::VertexId>) -> Self {
        Self::new(TraversalSource::Vertices(ids))
    }

    /// Create a start step for all edges.
    pub fn all_edges() -> Self {
        Self::new(TraversalSource::AllEdges)
    }

    /// Create a start step for specific edge IDs.
    pub fn edges(ids: Vec<crate::value::EdgeId>) -> Self {
        Self::new(TraversalSource::Edges(ids))
    }

    /// Create a start step that injects arbitrary values.
    pub fn inject(values: Vec<Value>) -> Self {
        Self::new(TraversalSource::Inject(values))
    }

    /// Get the source for this start step.
    pub fn source(&self) -> &TraversalSource {
        &self.source
    }
}

impl AnyStep for StartStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        _input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let track_paths = ctx.is_tracking_paths();

        match &self.source {
            TraversalSource::AllVertices => {
                // Iterate all vertices and create traversers
                Box::new(ctx.storage().all_vertices().map(move |v| {
                    let mut t = Traverser::from_vertex(v.id);
                    if track_paths {
                        t.extend_path_unlabeled();
                    }
                    t
                }))
            }
            TraversalSource::Vertices(ids) => {
                // Iterate specific vertices, filtering out non-existent ones
                let ids = ids.clone();
                Box::new(ids.into_iter().filter_map(move |id| {
                    ctx.storage().get_vertex(id).map(|_| {
                        let mut t = Traverser::from_vertex(id);
                        if track_paths {
                            t.extend_path_unlabeled();
                        }
                        t
                    })
                }))
            }
            TraversalSource::AllEdges => {
                // Iterate all edges and create traversers
                Box::new(ctx.storage().all_edges().map(move |e| {
                    let mut t = Traverser::from_edge(e.id);
                    if track_paths {
                        t.extend_path_unlabeled();
                    }
                    t
                }))
            }
            TraversalSource::Edges(ids) => {
                // Iterate specific edges, filtering out non-existent ones
                let ids = ids.clone();
                Box::new(ids.into_iter().filter_map(move |id| {
                    ctx.storage().get_edge(id).map(|_| {
                        let mut t = Traverser::from_edge(id);
                        if track_paths {
                            t.extend_path_unlabeled();
                        }
                        t
                    })
                }))
            }
            TraversalSource::Inject(values) => {
                // Create traversers from arbitrary values
                let values = values.clone();
                Box::new(values.into_iter().map(move |v| {
                    let mut t = Traverser::new(v);
                    if track_paths {
                        t.extend_path_unlabeled();
                    }
                    t
                }))
            }
        }
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "start"
    }
}

// -----------------------------------------------------------------------------
// execute_traversal - Helper for sub-traversal execution
// -----------------------------------------------------------------------------

/// Execute an anonymous traversal's steps on provided input.
///
/// This function is the core helper for sub-traversal execution, used by
/// branching steps like `union()`, `choose()`, and `coalesce()`. It applies
/// a traversal's steps to an input iterator of traversers, ignoring any
/// source the traversal might have.
///
/// # Key Features
///
/// - **Lazy evaluation**: Uses iterator chaining, no eager collection
/// - **Source-independent**: Ignores the traversal's source, uses provided input
/// - **Context sharing**: Uses the same execution context as the parent traversal
///
/// # Arguments
///
/// * `ctx` - The execution context providing graph access and side effects
/// * `steps` - The steps to apply (extracted from a traversal)
/// * `input` - Iterator of input traversers to process
///
/// # Returns
///
/// A boxed iterator over the output traversers.
///
/// # Example
///
/// ```ignore
/// // Execute an anonymous traversal's steps
/// let anon: Traversal<Value, Value> = Traversal::new()
///     .has_label("person")
///     .out();
///
/// let (_, steps) = anon.into_steps();
/// let input = vec![Traverser::from_vertex(VertexId(1))];
///
/// let output = execute_traversal(&ctx, &steps, Box::new(input.into_iter()));
/// for traverser in output {
///     println!("{:?}", traverser.value);
/// }
/// ```
///
/// # Design Notes
///
/// This function uses a fold over the steps to build an iterator chain.
/// Each step's `apply` method wraps the previous iterator, creating a
/// lazy pipeline that only executes when the returned iterator is consumed.
///
/// The lifetime bound ensures the returned iterator can reference both
/// the context and the steps for the duration of its use.
pub fn execute_traversal<'a>(
    ctx: &'a ExecutionContext<'a>,
    steps: &'a [Box<dyn AnyStep>],
    input: Box<dyn Iterator<Item = Traverser> + 'a>,
) -> Box<dyn Iterator<Item = Traverser> + 'a> {
    // Fold over steps, building an iterator chain
    // Each step wraps the previous iterator, maintaining lazy evaluation
    steps
        .iter()
        .fold(input, |current, step| step.apply(ctx, current))
}

/// Execute a traversal on provided input, extracting steps automatically.
///
/// This is a convenience wrapper that accesses a traversal's steps
/// and calls `execute_traversal`. The traversal's source is ignored.
///
/// # Arguments
///
/// * `ctx` - The execution context
/// * `traversal` - The traversal whose steps to execute
/// * `input` - Iterator of input traversers
///
/// # Returns
///
/// A boxed iterator over output traversers.
///
/// # Example
///
/// ```ignore
/// let anon = Traversal::<Value, Value>::new().out().has_label("person");
/// let input = vec![Traverser::from_vertex(VertexId(1))];
///
/// let output = execute_traversal_from(&ctx, &anon, Box::new(input.into_iter()));
/// for traverser in output {
///     println!("{:?}", traverser.value);
/// }
/// ```
pub fn execute_traversal_from<'a, In, Out>(
    ctx: &'a ExecutionContext<'a>,
    traversal: &'a crate::traversal::Traversal<In, Out>,
    input: Box<dyn Iterator<Item = Traverser> + 'a>,
) -> Box<dyn Iterator<Item = Traverser> + 'a> {
    execute_traversal(ctx, traversal.steps(), input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    use crate::value::{Value, VertexId};
    use std::collections::HashMap;

    fn create_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();
        storage.add_vertex("person", HashMap::new());
        Graph::new(storage)
    }

    mod any_step_tests {
        use super::*;

        #[test]
        fn any_step_trait_compiles() {
            // Verify the trait can be used as a trait object
            let step: Box<dyn AnyStep> = Box::new(IdentityStep);
            assert_eq!(step.name(), "identity");
        }

        #[test]
        fn box_dyn_any_step_is_clonable() {
            let step: Box<dyn AnyStep> = Box::new(IdentityStep);
            let cloned = step.clone();
            assert_eq!(cloned.name(), "identity");
        }

        #[test]
        fn any_step_can_be_stored_in_vec() {
            let steps: Vec<Box<dyn AnyStep>> = vec![
                Box::new(IdentityStep),
                Box::new(IdentityStep),
                Box::new(IdentityStep),
            ];
            assert_eq!(steps.len(), 3);
            for step in &steps {
                assert_eq!(step.name(), "identity");
            }
        }

        #[test]
        fn vec_of_steps_is_clonable() {
            let steps: Vec<Box<dyn AnyStep>> = vec![Box::new(IdentityStep), Box::new(IdentityStep)];
            let cloned: Vec<Box<dyn AnyStep>> = steps.iter().map(|s| s.clone_box()).collect();
            assert_eq!(cloned.len(), 2);
        }
    }

    mod identity_step_tests {
        use super::*;

        #[test]
        fn identity_step_new() {
            let step = IdentityStep::new();
            assert_eq!(step.name(), "identity");
        }

        #[test]
        fn identity_step_default() {
            let step = IdentityStep;
            assert_eq!(step.name(), "identity");
        }

        #[test]
        fn identity_step_passes_through() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdentityStep;

            let input: Vec<Traverser> = vec![
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
        fn identity_step_empty_input() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdentityStep;
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn identity_step_preserves_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdentityStep;

            let mut traverser = Traverser::from_vertex(VertexId(42));
            traverser.extend_path_labeled("test");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Vertex(VertexId(42)));
            assert_eq!(output[0].path.len(), 1);
            assert!(output[0].path.has_label("test"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn identity_step_clone_box() {
            let step = IdentityStep;
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "identity");
        }
    }

    mod macro_tests {
        use super::*;

        // Test filter step using the macro
        #[derive(Clone)]
        struct TestFilterStep {
            min_value: i64,
        }

        impl TestFilterStep {
            fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
                match &traverser.value {
                    Value::Int(n) => *n >= self.min_value,
                    _ => false,
                }
            }
        }

        impl_filter_step!(TestFilterStep, "testFilter");

        #[test]
        fn filter_step_macro_compiles() {
            let step = TestFilterStep { min_value: 5 };
            assert_eq!(step.name(), "testFilter");
        }

        #[test]
        fn filter_step_macro_filters() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TestFilterStep { min_value: 5 };

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::Int(5)),
                Traverser::new(Value::Int(7)),
                Traverser::new(Value::Int(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(5));
            assert_eq!(output[1].value, Value::Int(7));
        }

        #[test]
        fn filter_step_macro_clone_box() {
            let step = TestFilterStep { min_value: 10 };
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "testFilter");
        }

        #[test]
        fn filter_step_macro_is_any_step() {
            let step: Box<dyn AnyStep> = Box::new(TestFilterStep { min_value: 0 });
            assert_eq!(step.name(), "testFilter");
        }

        // Test flatmap step using the macro
        #[derive(Clone)]
        struct TestFlatMapStep {
            repeat_count: usize,
        }

        impl TestFlatMapStep {
            fn expand(
                &self,
                _ctx: &ExecutionContext,
                traverser: Traverser,
            ) -> impl Iterator<Item = Traverser> {
                let count = self.repeat_count;
                (0..count).map(move |i| traverser.split(Value::Int(i as i64)))
            }
        }

        impl_flatmap_step!(TestFlatMapStep, "testFlatMap");

        #[test]
        fn flatmap_step_macro_compiles() {
            let step = TestFlatMapStep { repeat_count: 3 };
            assert_eq!(step.name(), "testFlatMap");
        }

        #[test]
        fn flatmap_step_macro_expands() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TestFlatMapStep { repeat_count: 3 };

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::String("a".to_string())),
                Traverser::new(Value::String("b".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // 2 inputs * 3 repeats = 6 outputs
            assert_eq!(output.len(), 6);
            // First input expanded to 0, 1, 2
            assert_eq!(output[0].value, Value::Int(0));
            assert_eq!(output[1].value, Value::Int(1));
            assert_eq!(output[2].value, Value::Int(2));
            // Second input expanded to 0, 1, 2
            assert_eq!(output[3].value, Value::Int(0));
            assert_eq!(output[4].value, Value::Int(1));
            assert_eq!(output[5].value, Value::Int(2));
        }

        #[test]
        fn flatmap_step_macro_zero_expansion() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TestFlatMapStep { repeat_count: 0 };

            let input: Vec<Traverser> =
                vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // 0 expansion = no outputs
            assert!(output.is_empty());
        }

        #[test]
        fn flatmap_step_macro_clone_box() {
            let step = TestFlatMapStep { repeat_count: 5 };
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "testFlatMap");
        }

        #[test]
        fn flatmap_step_macro_is_any_step() {
            let step: Box<dyn AnyStep> = Box::new(TestFlatMapStep { repeat_count: 1 });
            assert_eq!(step.name(), "testFlatMap");
        }

        #[test]
        fn flatmap_step_preserves_path() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = TestFlatMapStep { repeat_count: 2 };

            let mut traverser = Traverser::from_vertex(VertexId(1));
            traverser.extend_path_labeled("start");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Both outputs should preserve the path
            assert_eq!(output.len(), 2);
            assert!(output[0].path.has_label("start"));
            assert!(output[1].path.has_label("start"));
        }
    }

    mod step_composition_tests {
        use super::*;

        #[derive(Clone)]
        struct MultiplyStep {
            factor: i64,
        }

        impl MultiplyStep {
            fn expand(
                &self,
                _ctx: &ExecutionContext,
                traverser: Traverser,
            ) -> impl Iterator<Item = Traverser> {
                let factor = self.factor;
                let result = match traverser.value {
                    Value::Int(n) => Some(traverser.with_value(Value::Int(n * factor))),
                    _ => None,
                };
                result.into_iter()
            }
        }

        impl_flatmap_step!(MultiplyStep, "multiply");

        #[derive(Clone)]
        struct IsEvenStep;

        impl IsEvenStep {
            fn matches(&self, _ctx: &ExecutionContext, traverser: &Traverser) -> bool {
                matches!(&traverser.value, Value::Int(n) if n % 2 == 0)
            }
        }

        impl_filter_step!(IsEvenStep, "isEven");

        #[test]
        fn steps_can_be_composed() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Create a pipeline: identity -> multiply by 2 -> filter even
            let steps: Vec<Box<dyn AnyStep>> = vec![
                Box::new(IdentityStep),
                Box::new(MultiplyStep { factor: 2 }),
                Box::new(IsEvenStep),
            ];

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            // Apply steps in sequence
            let mut current: Box<dyn Iterator<Item = Traverser>> = Box::new(input.into_iter());
            for step in &steps {
                current = step.apply(&ctx, current);
            }

            let output: Vec<Traverser> = current.collect();

            // All values doubled (2, 4, 6) should be even
            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(2));
            assert_eq!(output[1].value, Value::Int(4));
            assert_eq!(output[2].value, Value::Int(6));
        }

        #[test]
        fn step_vec_can_be_cloned() {
            let steps: Vec<Box<dyn AnyStep>> = vec![
                Box::new(IdentityStep),
                Box::new(MultiplyStep { factor: 3 }),
                Box::new(IsEvenStep),
            ];

            let cloned: Vec<Box<dyn AnyStep>> = steps.iter().map(|s| s.clone_box()).collect();

            assert_eq!(cloned.len(), 3);
            assert_eq!(cloned[0].name(), "identity");
            assert_eq!(cloned[1].name(), "multiply");
            assert_eq!(cloned[2].name(), "isEven");
        }
    }

    mod start_step_tests {
        use super::*;
        use crate::value::EdgeId;

        fn create_populated_graph() -> Graph {
            let mut storage = InMemoryGraph::new();

            // Add 3 vertices
            let v1 = storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props
            });
            let v2 = storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props
            });
            let v3 = storage.add_vertex("software", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Graph DB".to_string()));
                props
            });

            // Add 2 edges
            storage.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
            storage.add_edge(v2, v3, "uses", HashMap::new()).unwrap();

            Graph::new(storage)
        }

        #[test]
        fn start_step_new() {
            let step = StartStep::new(TraversalSource::AllVertices);
            assert_eq!(step.name(), "start");
        }

        #[test]
        fn start_step_all_vertices_constructor() {
            let step = StartStep::all_vertices();
            assert!(matches!(step.source(), TraversalSource::AllVertices));
        }

        #[test]
        fn start_step_all_edges_constructor() {
            let step = StartStep::all_edges();
            assert!(matches!(step.source(), TraversalSource::AllEdges));
        }

        #[test]
        fn start_step_vertices_constructor() {
            let ids = vec![VertexId(1), VertexId(2)];
            let step = StartStep::vertices(ids);
            match step.source() {
                TraversalSource::Vertices(v) => {
                    assert_eq!(v.len(), 2);
                    assert_eq!(v[0], VertexId(1));
                    assert_eq!(v[1], VertexId(2));
                }
                _ => panic!("Expected Vertices source"),
            }
        }

        #[test]
        fn start_step_edges_constructor() {
            let ids = vec![EdgeId(10), EdgeId(20)];
            let step = StartStep::edges(ids);
            match step.source() {
                TraversalSource::Edges(e) => {
                    assert_eq!(e.len(), 2);
                    assert_eq!(e[0], EdgeId(10));
                    assert_eq!(e[1], EdgeId(20));
                }
                _ => panic!("Expected Edges source"),
            }
        }

        #[test]
        fn start_step_inject_constructor() {
            let values = vec![Value::Int(1), Value::String("test".to_string())];
            let step = StartStep::inject(values);
            match step.source() {
                TraversalSource::Inject(v) => {
                    assert_eq!(v.len(), 2);
                    assert_eq!(v[0], Value::Int(1));
                    assert_eq!(v[1], Value::String("test".to_string()));
                }
                _ => panic!("Expected Inject source"),
            }
        }

        #[test]
        fn start_step_all_vertices_returns_all_vertices() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = StartStep::all_vertices();
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            // Should return 3 vertices
            assert_eq!(output.len(), 3);

            // All should be vertex traversers
            for t in &output {
                assert!(t.is_vertex());
                assert!(t.as_vertex_id().is_some());
            }
        }

        #[test]
        fn start_step_all_edges_returns_all_edges() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = StartStep::all_edges();
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            // Should return 2 edges
            assert_eq!(output.len(), 2);

            // All should be edge traversers
            for t in &output {
                assert!(t.is_edge());
                assert!(t.as_edge_id().is_some());
            }
        }

        #[test]
        fn start_step_specific_vertices() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Request vertices 0 and 2 (which exist)
            let step = StartStep::vertices(vec![VertexId(0), VertexId(2)]);
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            // Should return 2 vertices
            assert_eq!(output.len(), 2);

            // Check specific IDs
            let ids: Vec<VertexId> = output.iter().map(|t| t.as_vertex_id().unwrap()).collect();
            assert!(ids.contains(&VertexId(0)));
            assert!(ids.contains(&VertexId(2)));
        }

        #[test]
        fn start_step_specific_vertices_filters_nonexistent() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Request vertices including non-existent ID 999
            let step = StartStep::vertices(vec![VertexId(0), VertexId(999)]);
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            // Should only return 1 vertex (999 doesn't exist)
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn start_step_specific_edges() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Request edge 0 (which exists)
            let step = StartStep::edges(vec![EdgeId(0)]);
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            // Should return 1 edge
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn start_step_specific_edges_filters_nonexistent() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Request edges including non-existent ID 999
            let step = StartStep::edges(vec![EdgeId(0), EdgeId(999)]);
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            // Should only return 1 edge (999 doesn't exist)
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn start_step_inject_creates_traversers() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let values = vec![
                Value::Int(1),
                Value::String("hello".to_string()),
                Value::Bool(true),
            ];
            let step = StartStep::inject(values);
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::String("hello".to_string()));
            assert_eq!(output[2].value, Value::Bool(true));
        }

        #[test]
        fn start_step_inject_empty() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = StartStep::inject(vec![]);
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn start_step_traversers_have_empty_path() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = StartStep::all_vertices();
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            // All traversers should start with empty path
            for t in &output {
                assert!(t.path.is_empty());
            }
        }

        #[test]
        fn start_step_traversers_have_default_metadata() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = StartStep::all_vertices();
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            // All traversers should have default metadata
            for t in &output {
                assert_eq!(t.loops, 0);
                assert_eq!(t.bulk, 1);
                assert!(t.sack.is_none());
            }
        }

        #[test]
        fn start_step_ignores_input() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Create input traversers (should be ignored)
            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(999)),
                Traverser::new(Value::Int(888)),
            ];

            let step = StartStep::all_vertices();
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Should return all vertices, ignoring input
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn start_step_empty_graph_returns_empty() {
            // Create empty graph
            let storage = InMemoryGraph::new();
            let graph = Graph::new(storage);
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = StartStep::all_vertices();
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::empty())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn start_step_clone_box() {
            let step = StartStep::all_vertices();
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "start");
        }

        #[test]
        fn start_step_is_clonable() {
            let step = StartStep::vertices(vec![VertexId(1), VertexId(2)]);
            let cloned = step.clone();
            match cloned.source() {
                TraversalSource::Vertices(v) => {
                    assert_eq!(v.len(), 2);
                }
                _ => panic!("Expected Vertices source"),
            }
        }

        #[test]
        fn start_step_is_any_step() {
            let step: Box<dyn AnyStep> = Box::new(StartStep::all_vertices());
            assert_eq!(step.name(), "start");
        }

        #[test]
        fn start_step_debug_output() {
            let step = StartStep::all_vertices();
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("StartStep"));
            assert!(debug_str.contains("AllVertices"));
        }

        #[test]
        fn start_step_can_be_stored_with_other_steps() {
            let steps: Vec<Box<dyn AnyStep>> =
                vec![Box::new(StartStep::all_vertices()), Box::new(IdentityStep)];

            assert_eq!(steps.len(), 2);
            assert_eq!(steps[0].name(), "start");
            assert_eq!(steps[1].name(), "identity");
        }
    }

    mod execute_traversal_tests {
        use super::*;
        use crate::traversal::Traversal;

        fn create_populated_graph() -> Graph {
            let mut storage = InMemoryGraph::new();

            // Add 3 vertices
            let v1 = storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });
            let v2 = storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props.insert("age".to_string(), Value::Int(25));
                props
            });
            let v3 = storage.add_vertex("software", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Graph DB".to_string()));
                props
            });

            // Add edges
            storage.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
            storage.add_edge(v2, v3, "uses", HashMap::new()).unwrap();

            Graph::new(storage)
        }

        #[test]
        fn execute_traversal_with_empty_steps() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let steps: Vec<Box<dyn AnyStep>> = vec![];
            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> =
                execute_traversal(&ctx, &steps, Box::new(input.into_iter())).collect();

            // With no steps, output should match input
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
        }

        #[test]
        fn execute_traversal_with_identity_step() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let steps: Vec<Box<dyn AnyStep>> = vec![Box::new(IdentityStep::new())];
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> =
                execute_traversal(&ctx, &steps, Box::new(input.into_iter())).collect();

            // Identity should pass through all values
            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
            assert_eq!(output[2].value, Value::Int(3));
        }

        #[test]
        fn execute_traversal_with_multiple_identity_steps() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let steps: Vec<Box<dyn AnyStep>> = vec![
                Box::new(IdentityStep::new()),
                Box::new(IdentityStep::new()),
                Box::new(IdentityStep::new()),
            ];
            let input = vec![Traverser::new(Value::String("test".to_string()))];

            let output: Vec<Traverser> =
                execute_traversal(&ctx, &steps, Box::new(input.into_iter())).collect();

            // Multiple identity steps should still pass through
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("test".to_string()));
        }

        #[test]
        fn execute_traversal_with_empty_input() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let steps: Vec<Box<dyn AnyStep>> = vec![Box::new(IdentityStep::new())];
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> =
                execute_traversal(&ctx, &steps, Box::new(input.into_iter())).collect();

            // Empty input should produce empty output
            assert!(output.is_empty());
        }

        #[test]
        fn execute_traversal_preserves_metadata() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let steps: Vec<Box<dyn AnyStep>> = vec![Box::new(IdentityStep::new())];

            let mut traverser = Traverser::from_vertex(VertexId(1));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> =
                execute_traversal(&ctx, &steps, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].path.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn execute_traversal_is_lazy() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let steps: Vec<Box<dyn AnyStep>> = vec![Box::new(IdentityStep::new())];
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            // Create the iterator but don't consume it fully
            let mut iter = execute_traversal(&ctx, &steps, Box::new(input.into_iter()));

            // Take only first element
            let first = iter.next();
            assert!(first.is_some());
            assert_eq!(first.unwrap().value, Value::Int(1));

            // Take second element
            let second = iter.next();
            assert!(second.is_some());
            assert_eq!(second.unwrap().value, Value::Int(2));

            // Third should still be available
            let third = iter.next();
            assert!(third.is_some());

            // Now exhausted
            assert!(iter.next().is_none());
        }

        #[test]
        fn execute_traversal_from_with_anonymous_traversal() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Create an anonymous traversal with identity step
            let anon: Traversal<Value, Value> =
                Traversal::<Value, Value>::new().add_step(IdentityStep::new());

            let input = vec![
                Traverser::new(Value::Int(10)),
                Traverser::new(Value::Int(20)),
            ];

            let output: Vec<Traverser> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(10));
            assert_eq!(output[1].value, Value::Int(20));
        }

        #[test]
        fn execute_traversal_from_ignores_source() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Create a traversal WITH a source (normally used for bound traversals)
            let traversal: Traversal<(), Value> =
                Traversal::<(), Value>::with_source(TraversalSource::AllVertices)
                    .add_step(IdentityStep::new());

            // execute_traversal_from should ignore the source and use our input
            let input = vec![Traverser::new(Value::String("custom".to_string()))];

            let output: Vec<Traverser> =
                execute_traversal_from(&ctx, &traversal, Box::new(input.into_iter())).collect();

            // Should get our custom input, not all vertices
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("custom".to_string()));
        }

        #[test]
        fn execute_traversal_from_empty_traversal() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Empty traversal (no steps)
            let anon: Traversal<Value, Value> = Traversal::new();

            let input = vec![
                Traverser::new(Value::Bool(true)),
                Traverser::new(Value::Bool(false)),
            ];

            let output: Vec<Traverser> =
                execute_traversal_from(&ctx, &anon, Box::new(input.into_iter())).collect();

            // With no steps, output should match input
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Bool(true));
            assert_eq!(output[1].value, Value::Bool(false));
        }

        #[test]
        fn execute_traversal_with_filter_step() {
            use crate::traversal::filter::HasLabelStep;

            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Create steps that filter to "person" label
            let steps: Vec<Box<dyn AnyStep>> = vec![Box::new(HasLabelStep::single("person"))];

            // Input: vertex IDs 0, 1, 2 (person, person, software)
            let input = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
            ];

            let output: Vec<Traverser> =
                execute_traversal(&ctx, &steps, Box::new(input.into_iter())).collect();

            // Should only return person vertices (IDs 0 and 1)
            assert_eq!(output.len(), 2);
            assert!(output.iter().all(|t| t.is_vertex()));
        }

        #[test]
        fn execute_traversal_chained_steps() {
            use crate::traversal::filter::HasLabelStep;

            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Chain: identity -> filter to person
            let steps: Vec<Box<dyn AnyStep>> = vec![
                Box::new(IdentityStep::new()),
                Box::new(HasLabelStep::single("person")),
                Box::new(IdentityStep::new()),
            ];

            let input = vec![
                Traverser::from_vertex(VertexId(0)), // person
                Traverser::from_vertex(VertexId(1)), // person
                Traverser::from_vertex(VertexId(2)), // software
            ];

            let output: Vec<Traverser> =
                execute_traversal(&ctx, &steps, Box::new(input.into_iter())).collect();

            // Should only return person vertices
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn execute_traversal_steps_access() {
            // Test that we can access steps from a traversal
            let anon: Traversal<Value, Value> = {
                let t = Traversal::<Value, Value>::new();
                let t: Traversal<Value, Value> = t.add_step(IdentityStep::new());
                let t: Traversal<Value, Value> = t.add_step(IdentityStep::new());
                t
            };

            let steps = anon.steps();
            assert_eq!(steps.len(), 2);
            assert_eq!(steps[0].name(), "identity");
            assert_eq!(steps[1].name(), "identity");
        }

        #[test]
        fn execute_traversal_reusable() {
            let graph = create_populated_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Same steps can be reused multiple times
            let steps: Vec<Box<dyn AnyStep>> = vec![Box::new(IdentityStep::new())];

            let input1 = vec![Traverser::new(Value::Int(1))];
            let output1: Vec<Traverser> =
                execute_traversal(&ctx, &steps, Box::new(input1.into_iter())).collect();

            let input2 = vec![Traverser::new(Value::Int(2))];
            let output2: Vec<Traverser> =
                execute_traversal(&ctx, &steps, Box::new(input2.into_iter())).collect();

            // Both should work independently
            assert_eq!(output1.len(), 1);
            assert_eq!(output1[0].value, Value::Int(1));
            assert_eq!(output2.len(), 1);
            assert_eq!(output2[0].value, Value::Int(2));
        }
    }
}
