use crate::impl_flatmap_step;
use crate::traversal::{ExecutionContext, Traverser};
use crate::value::Value;

// -----------------------------------------------------------------------------
// ValuesStep - extract property values from elements
// -----------------------------------------------------------------------------

/// Transform step that extracts property values from vertices and edges.
///
/// This step extracts the value(s) of specified properties from graph elements.
/// For each input element, it produces one output value per matching property key.
///
/// # Behavior
///
/// - For vertices: extracts property values from vertex properties
/// - For edges: extracts property values from edge properties  
/// - For non-element values: filtered out (produces no output)
/// - Missing properties: skipped (no error, just filtered out)
///
/// # Example
///
/// ```ignore
/// // Extract the "name" property from all person vertices
/// let names = g.v().has_label("person").values("name").to_list();
///
/// // Extract multiple properties
/// let data = g.v().values_multi(&["name", "age"]).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct ValuesStep {
    /// Property keys to extract
    keys: Vec<String>,
}

impl ValuesStep {
    /// Create a ValuesStep for a single property key.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to extract
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = ValuesStep::new("name");
    /// ```
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    /// Create a ValuesStep for multiple property keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - The property keys to extract
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = ValuesStep::multi(vec!["name".to_string(), "age".to_string()]);
    /// ```
    pub fn multi(keys: Vec<String>) -> Self {
        Self { keys }
    }

    /// Create a ValuesStep from an iterator of keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - Iterator of property keys to extract
    pub fn from_keys<I, S>(keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }

    /// Expand a traverser by extracting property values.
    ///
    /// Returns an iterator of new traversers, one for each property value found.
    /// Missing properties are silently skipped.
    fn expand<'a>(
        &self,
        ctx: &'a ExecutionContext<'a>,
        traverser: Traverser,
    ) -> impl Iterator<Item = Traverser> + 'a {
        let keys = self.keys.clone();

        match &traverser.value {
            Value::Vertex(id) => {
                // Get vertex properties
                let props: Vec<Value> = ctx
                    .storage()
                    .get_vertex(*id)
                    .map(|vertex| {
                        keys.iter()
                            .filter_map(|key| vertex.properties.get(key).cloned())
                            .collect()
                    })
                    .unwrap_or_default();

                // Create new traversers for each property value
                let traverser_for_split = traverser;
                props
                    .into_iter()
                    .map(move |value| traverser_for_split.split(value))
                    .collect::<Vec<_>>()
                    .into_iter()
            }
            Value::Edge(id) => {
                // Get edge properties
                let props: Vec<Value> = ctx
                    .storage()
                    .get_edge(*id)
                    .map(|edge| {
                        keys.iter()
                            .filter_map(|key| edge.properties.get(key).cloned())
                            .collect()
                    })
                    .unwrap_or_default();

                // Create new traversers for each property value
                let traverser_for_split = traverser;
                props
                    .into_iter()
                    .map(move |value| traverser_for_split.split(value))
                    .collect::<Vec<_>>()
                    .into_iter()
            }
            // Non-element values don't have properties
            _ => Vec::new().into_iter(),
        }
    }

    /// Streaming version of expand.
    fn expand_streaming(
        &self,
        ctx: &crate::traversal::context::StreamingContext,
        traverser: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let keys = self.keys.clone();

        match &traverser.value {
            Value::Vertex(id) => {
                let props: Vec<Value> = ctx
                    .storage()
                    .get_vertex(*id)
                    .map(|vertex| {
                        keys.iter()
                            .filter_map(|key| vertex.properties.get(key).cloned())
                            .collect()
                    })
                    .unwrap_or_default();

                Box::new(
                    props
                        .into_iter()
                        .map(move |value| traverser.split(value))
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            }
            Value::Edge(id) => {
                let props: Vec<Value> = ctx
                    .storage()
                    .get_edge(*id)
                    .map(|edge| {
                        keys.iter()
                            .filter_map(|key| edge.properties.get(key).cloned())
                            .collect()
                    })
                    .unwrap_or_default();

                Box::new(
                    props
                        .into_iter()
                        .map(move |value| traverser.split(value))
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            }
            _ => Box::new(std::iter::empty()),
        }
    }
}

// Use the macro to implement Step for ValuesStep (DynStep is provided via blanket impl)
impl_flatmap_step!(ValuesStep, "values", category = crate::traversal::explain::StepCategory::Transform, describe = |s: &ValuesStep| Some(s.keys.iter().map(|k| format!("\"{k}\"")).collect::<Vec<_>>().join(", ")));

// Reactive introspection: expose property key constraints.
#[cfg(feature = "reactive")]
impl crate::traversal::reactive::StepIntrospect for ValuesStep {
    fn property_constraints(&self) -> Option<Vec<String>> {
        Some(self.keys.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use crate::traversal::step::{DynStep, Step};
    use crate::traversal::SnapshotLike;
    use crate::value::{EdgeId, VertexId};
    use std::collections::HashMap;

    fn create_test_graph() -> Graph {
        let graph = Graph::new();

        // Vertex 0: person with name and age
        let v0 = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });

        // Vertex 1: person with name only
        let v1 = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props
        });

        // Vertex 2: software with name and version
        let v2 = graph.add_vertex("software", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Graph DB".to_string()));
            props.insert("version".to_string(), Value::Float(1.0));
            props
        });

        // Vertex 3: company with no properties
        graph.add_vertex("company", HashMap::new());

        // Edge 0: knows with since property
        graph
            .add_edge(v0, v1, "knows", {
                let mut props = HashMap::new();
                props.insert("since".to_string(), Value::Int(2020));
                props.insert("weight".to_string(), Value::Float(0.8));
                props
            })
            .unwrap();

        // Edge 1: uses with no properties
        graph.add_edge(v1, v2, "uses", HashMap::new()).unwrap();

        graph
    }

    mod values_step_construction {
        use super::*;

        #[test]
        fn new_creates_single_key_step() {
            let step = ValuesStep::new("name");
            assert_eq!(step.keys, vec!["name".to_string()]);
        }

        #[test]
        fn multi_creates_multi_key_step() {
            let step = ValuesStep::multi(vec!["name".to_string(), "age".to_string()]);
            assert_eq!(step.keys.len(), 2);
            assert_eq!(step.keys[0], "name");
            assert_eq!(step.keys[1], "age");
        }

        #[test]
        fn from_keys_creates_step_from_iterator() {
            let step = ValuesStep::from_keys(["name", "age", "email"]);
            assert_eq!(step.keys.len(), 3);
            assert_eq!(step.keys[0], "name");
            assert_eq!(step.keys[1], "age");
            assert_eq!(step.keys[2], "email");
        }

        #[test]
        fn name_returns_values() {
            let step = ValuesStep::new("name");
            assert_eq!(step.name(), "values");
        }

        #[test]
        fn clone_box_works() {
            let step = ValuesStep::new("name");
            let cloned: Box<dyn DynStep> = DynStep::clone_box(&step);
            assert_eq!(cloned.dyn_name(), "values");
        }

        #[test]
        fn debug_format() {
            let step = ValuesStep::multi(vec!["name".to_string(), "age".to_string()]);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("ValuesStep"));
            assert!(debug_str.contains("name"));
            assert!(debug_str.contains("age"));
        }
    }

    mod values_step_vertex_tests {
        use super::*;

        #[test]
        fn extracts_single_property_from_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("Alice".to_string()));
        }

        #[test]
        fn extracts_multiple_properties_from_single_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::multi(vec!["name".to_string(), "age".to_string()]);

            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice with name and age

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            // Order depends on property iteration order, so check both exist
            let values: Vec<Value> = output.iter().map(|t| t.value.clone()).collect();
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::Int(30)));
        }

        #[test]
        fn extracts_properties_from_multiple_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![
                Traverser::from_vertex(VertexId(0)), // Alice
                Traverser::from_vertex(VertexId(1)), // Bob
                Traverser::from_vertex(VertexId(2)), // Graph DB
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            let values: Vec<Value> = output.iter().map(|t| t.value.clone()).collect();
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Bob".to_string())));
            assert!(values.contains(&Value::String("Graph DB".to_string())));
        }

        #[test]
        fn skips_missing_properties() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("age");

            let input = vec![
                Traverser::from_vertex(VertexId(0)), // Alice has age
                Traverser::from_vertex(VertexId(1)), // Bob has no age
                Traverser::from_vertex(VertexId(2)), // Software has no age
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only Alice has "age" property
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(30));
        }

        #[test]
        fn vertex_with_no_properties_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::from_vertex(VertexId(3))]; // Company with no properties

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn nonexistent_property_key_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("nonexistent_property");

            let input = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn nonexistent_vertex_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::from_vertex(VertexId(999))]; // Non-existent vertex

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn extracts_different_value_types() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            // Extract string property
            let step_name = ValuesStep::new("name");
            let input = vec![Traverser::from_vertex(VertexId(0))];
            let output: Vec<Traverser> =
                step_name.apply(&ctx, Box::new(input.into_iter())).collect();
            assert!(matches!(&output[0].value, Value::String(_)));

            // Extract int property
            let step_age = ValuesStep::new("age");
            let input = vec![Traverser::from_vertex(VertexId(0))];
            let output: Vec<Traverser> =
                step_age.apply(&ctx, Box::new(input.into_iter())).collect();
            assert!(matches!(&output[0].value, Value::Int(_)));

            // Extract float property
            let step_version = ValuesStep::new("version");
            let input = vec![Traverser::from_vertex(VertexId(2))];
            let output: Vec<Traverser> = step_version
                .apply(&ctx, Box::new(input.into_iter()))
                .collect();
            assert!(matches!(&output[0].value, Value::Float(_)));
        }
    }

    mod values_step_edge_tests {
        use super::*;

        #[test]
        fn extracts_single_property_from_edge() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("since");

            let input = vec![Traverser::from_edge(EdgeId(0))]; // knows edge with since

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(2020));
        }

        #[test]
        fn extracts_multiple_properties_from_edge() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::multi(vec!["since".to_string(), "weight".to_string()]);

            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            let values: Vec<Value> = output.iter().map(|t| t.value.clone()).collect();
            assert!(values.contains(&Value::Int(2020)));
            assert!(values.contains(&Value::Float(0.8)));
        }

        #[test]
        fn edge_with_no_properties_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("since");

            let input = vec![Traverser::from_edge(EdgeId(1))]; // uses edge with no properties

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn nonexistent_edge_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("since");

            let input = vec![Traverser::from_edge(EdgeId(999))]; // Non-existent edge

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod values_step_non_element_tests {
        use super::*;

        #[test]
        fn filters_out_integer_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::new(Value::String("hello".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::new(Value::Null)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_boolean_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::new(Value::Bool(true))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn mixed_elements_and_non_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![
                Traverser::from_vertex(VertexId(0)), // Alice - has name
                Traverser::new(Value::Int(42)),      // filtered out
                Traverser::from_vertex(VertexId(1)), // Bob - has name
                Traverser::new(Value::String("hello".to_string())), // filtered out
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            let values: Vec<Value> = output.iter().map(|t| t.value.clone()).collect();
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Bob".to_string())));
        }
    }

    mod values_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_from_input_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");

            let mut traverser = Traverser::from_vertex(VertexId(0));
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

            let step = ValuesStep::new("name");

            let mut traverser = Traverser::from_vertex(VertexId(0));
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

            let step = ValuesStep::new("name");

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn multiple_outputs_preserve_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::multi(vec!["name".to_string(), "age".to_string()]);

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");
            traverser.loops = 3;
            traverser.bulk = 7;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Both outputs should have the same metadata
            assert_eq!(output.len(), 2);
            for t in &output {
                assert!(t.path.has_label("start"));
                assert_eq!(t.loops, 3);
                assert_eq!(t.bulk, 7);
            }
        }
    }

    mod values_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::new("name");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn empty_keys_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValuesStep::multi(vec![]);

            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }
}
