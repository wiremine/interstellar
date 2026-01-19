use crate::traversal::{ExecutionContext, Traverser};
use crate::value::Value;

/// Transform step that replaces the traverser's value with a constant.
///
/// This step ignores the current value of the traverser and replaces it
/// with a fixed constant value provided at construction time.
///
/// # Behavior
///
/// - Each input traverser produces exactly one output traverser
/// - The value of the output traverser is the constant value
/// - Path history and other metadata are preserved
///
/// # Example
///
/// ```ignore
/// // Replace all values with the string "hello"
/// g.v().constant("hello")
/// // All results will be Value::String("hello")
///
/// // Replace with integer
/// g.v().constant(42)
/// // All results will be Value::Int(42)
/// ```
#[derive(Clone, Debug)]
pub struct ConstantStep {
    /// The constant value to emit for each traverser.
    value: Value,
}

impl ConstantStep {
    /// Create a new ConstantStep with the given value.
    ///
    /// # Arguments
    ///
    /// * `value` - The constant value to emit for each traverser
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = ConstantStep::new("constant_value");
    /// let step = ConstantStep::new(42i64);
    /// let step = ConstantStep::new(Value::Bool(true));
    /// ```
    pub fn new(value: impl Into<Value>) -> Self {
        Self {
            value: value.into(),
        }
    }

    /// Get the constant value.
    #[inline]
    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl crate::traversal::step::AnyStep for ConstantStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let value = self.value.clone();
        Box::new(input.map(move |t| t.with_value(value.clone())))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "constant"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    use crate::traversal::step::AnyStep;
    use crate::value::{EdgeId, VertexId};
    use std::collections::HashMap;

    fn create_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();

        // Vertex 0: person with name and age
        storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });

        // Vertex 1: software with name and lang
        storage.add_vertex("software", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Ripple".to_string()));
            props.insert("lang".to_string(), Value::String("Java".to_string()));
            props
        });

        // Edge 0: 0 -> 1 (created)
        storage
            .add_edge(VertexId(0), VertexId(1), "created", {
                let mut props = HashMap::new();
                props.insert("weight".to_string(), Value::Float(1.0));
                props
            })
            .unwrap();

        Graph::new(storage)
    }

    mod constant_step_transform_tests {
        use super::*;

        #[test]
        fn replaces_single_value_with_constant() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ConstantStep::new("replaced");
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("replaced".to_string()));
        }

        #[test]
        fn replaces_multiple_values_with_same_constant() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ConstantStep::new(100i64);
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::String("hello".to_string())),
                Traverser::new(Value::Bool(true)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(100));
            assert_eq!(output[1].value, Value::Int(100));
            assert_eq!(output[2].value, Value::Int(100));
        }

        #[test]
        fn replaces_vertex_values_with_constant() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ConstantStep::new("vertex_found");
            let input = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("vertex_found".to_string()));
            assert_eq!(output[1].value, Value::String("vertex_found".to_string()));
        }

        #[test]
        fn replaces_edge_values_with_constant() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ConstantStep::new("edge_found");
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("edge_found".to_string()));
        }

        #[test]
        fn works_with_null_constant() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ConstantStep::new(Value::Null);
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Null);
        }
    }

    mod constant_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ConstantStep::new("constant");

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.extend_path_labeled("start");
            traverser.extend_path_labeled("middle");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert!(output[0].path.has_label("middle"));
            assert_eq!(output[0].path.len(), 2);
        }

        #[test]
        fn preserves_loops_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ConstantStep::new("constant");

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.loops = 5;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].loops, 5);
        }

        #[test]
        fn preserves_bulk_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ConstantStep::new("constant");

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn preserves_all_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ConstantStep::new("constant");

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.extend_path_labeled("labeled");
            traverser.loops = 3;
            traverser.bulk = 7;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("labeled"));
            assert_eq!(output[0].loops, 3);
            assert_eq!(output[0].bulk, 7);
            assert_eq!(output[0].value, Value::String("constant".to_string()));
        }
    }

    mod constant_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ConstantStep::new("constant");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }
}
