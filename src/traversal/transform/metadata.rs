use crate::traversal::{ExecutionContext, Traverser};
use crate::value::Value;

// -----------------------------------------------------------------------------
// IdStep - extract element ID from vertices/edges
// -----------------------------------------------------------------------------

/// Transform step that extracts the ID from vertices and edges.
///
/// This step extracts the ID of a graph element and converts it to a `Value::Int`.
/// For each vertex, returns `Value::Int(vertex_id.0)`.
/// For each edge, returns `Value::Int(edge_id.0)`.
///
/// # Behavior
///
/// - For vertices: returns `Value::Int(id)` where id is the vertex's internal ID
/// - For edges: returns `Value::Int(id)` where id is the edge's internal ID
/// - For non-element values: filtered out (produces no output)
///
/// # Example
///
/// ```ignore
/// // Get IDs of all person vertices
/// let ids = g.v().has_label("person").id().to_list();
///
/// // Get IDs of all edges
/// let edge_ids = g.e().id().to_list();
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct IdStep;

impl IdStep {
    /// Create a new IdStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::AnyStep for IdStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(|traverser| {
            match &traverser.value {
                Value::Vertex(id) => {
                    // Return the vertex ID as an integer
                    Some(traverser.split(Value::Int(id.0 as i64)))
                }
                Value::Edge(id) => {
                    // Return the edge ID as an integer
                    Some(traverser.split(Value::Int(id.0 as i64)))
                }
                // Non-element values are filtered out
                _ => None,
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "id"
    }
}

// -----------------------------------------------------------------------------
// LabelStep - extract element label from vertices/edges
// -----------------------------------------------------------------------------

/// Transform step that extracts the label from vertices and edges.
///
/// This step extracts the label of a graph element and converts it to a `Value::String`.
/// The label is resolved from the string interner.
///
/// # Behavior
///
/// - For vertices: returns `Value::String(label)` with the vertex's label
/// - For edges: returns `Value::String(label)` with the edge's label
/// - For non-element values: filtered out (produces no output)
/// - If the label cannot be resolved (shouldn't happen in normal use): filtered out
///
/// # Example
///
/// ```ignore
/// // Get labels of all vertices
/// let labels = g.v().label().to_list();
///
/// // Get unique labels
/// let unique_labels = g.v().label().dedup().to_list();
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct LabelStep;

impl LabelStep {
    /// Create a new LabelStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::AnyStep for LabelStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(move |traverser| {
            match &traverser.value {
                Value::Vertex(id) => {
                    // Get the vertex and its label (already resolved by storage)
                    let vertex = ctx.snapshot().storage().get_vertex(*id)?;
                    Some(traverser.split(Value::String(vertex.label.clone())))
                }
                Value::Edge(id) => {
                    // Get the edge and its label (already resolved by storage)
                    let edge = ctx.snapshot().storage().get_edge(*id)?;
                    Some(traverser.split(Value::String(edge.label.clone())))
                }
                // Non-element values are filtered out
                _ => None,
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "label"
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

        // Vertex 1: person with name only
        storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props
        });

        // Vertex 2: software with name and version
        storage.add_vertex("software", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Graph DB".to_string()));
            props.insert("version".to_string(), Value::Float(1.0));
            props
        });

        // Vertex 3: company with no properties
        storage.add_vertex("company", HashMap::new());

        // Edge 0: knows with since property
        storage
            .add_edge(VertexId(0), VertexId(1), "knows", {
                let mut props = HashMap::new();
                props.insert("since".to_string(), Value::Int(2020));
                props.insert("weight".to_string(), Value::Float(0.8));
                props
            })
            .unwrap();

        // Edge 1: uses with no properties
        storage
            .add_edge(VertexId(1), VertexId(2), "uses", HashMap::new())
            .unwrap();

        Graph::new(Arc::new(storage))
    }

    // =========================================================================
    // IdStep Tests
    // =========================================================================

    mod id_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = IdStep::new();
            assert_eq!(step.name(), "id");
        }

        #[test]
        fn default_creates_step() {
            let step = IdStep::default();
            assert_eq!(step.name(), "id");
        }

        #[test]
        fn clone_box_works() {
            let step = IdStep::new();
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "id");
        }

        #[test]
        fn debug_format() {
            let step = IdStep::new();
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("IdStep"));
        }
    }

    mod id_step_vertex_tests {
        use super::*;

        #[test]
        fn extracts_id_from_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(0));
        }

        #[test]
        fn extracts_ids_from_multiple_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(0));
            assert_eq!(output[1].value, Value::Int(1));
            assert_eq!(output[2].value, Value::Int(2));
        }

        #[test]
        fn extracts_id_from_vertex_with_large_id() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            // Test with a large ID to verify u64 -> i64 conversion
            let input = vec![Traverser::from_vertex(VertexId(1_000_000))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(1_000_000));
        }
    }

    mod id_step_edge_tests {
        use super::*;

        #[test]
        fn extracts_id_from_edge() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(0));
        }

        #[test]
        fn extracts_ids_from_multiple_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input = vec![
                Traverser::from_edge(EdgeId(0)),
                Traverser::from_edge(EdgeId(1)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(0));
            assert_eq!(output[1].value, Value::Int(1));
        }
    }

    mod id_step_non_element_tests {
        use super::*;

        #[test]
        fn filters_out_integer_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::new(Value::String("hello".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::new(Value::Null)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_boolean_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::new(Value::Bool(true))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_float_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::new(Value::Float(3.14))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn mixed_elements_and_non_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::new(Value::Int(42)), // filtered out
                Traverser::from_edge(EdgeId(1)),
                Traverser::new(Value::String("hello".to_string())), // filtered out
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(0)); // from vertex
            assert_eq!(output[1].value, Value::Int(1)); // from edge
        }
    }

    mod id_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_from_input_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod id_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = IdStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    // =========================================================================
    // LabelStep Tests
    // =========================================================================

    mod label_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = LabelStep::new();
            assert_eq!(step.name(), "label");
        }

        #[test]
        fn default_creates_step() {
            let step = LabelStep::default();
            assert_eq!(step.name(), "label");
        }

        #[test]
        fn clone_box_works() {
            let step = LabelStep::new();
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "label");
        }

        #[test]
        fn debug_format() {
            let step = LabelStep::new();
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("LabelStep"));
        }
    }

    mod label_step_vertex_tests {
        use super::*;

        #[test]
        fn extracts_label_from_person_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice (person)

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("person".to_string()));
        }

        #[test]
        fn extracts_label_from_software_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::from_vertex(VertexId(2))]; // Graph DB (software)

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("software".to_string()));
        }

        #[test]
        fn extracts_labels_from_multiple_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![
                Traverser::from_vertex(VertexId(0)), // person
                Traverser::from_vertex(VertexId(1)), // person
                Traverser::from_vertex(VertexId(2)), // software
                Traverser::from_vertex(VertexId(3)), // company
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 4);
            let labels: Vec<&Value> = output.iter().map(|t| &t.value).collect();
            assert_eq!(labels[0], &Value::String("person".to_string()));
            assert_eq!(labels[1], &Value::String("person".to_string()));
            assert_eq!(labels[2], &Value::String("software".to_string()));
            assert_eq!(labels[3], &Value::String("company".to_string()));
        }

        #[test]
        fn nonexistent_vertex_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::from_vertex(VertexId(999))]; // Non-existent

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod label_step_edge_tests {
        use super::*;

        #[test]
        fn extracts_label_from_knows_edge() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::from_edge(EdgeId(0))]; // knows edge

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("knows".to_string()));
        }

        #[test]
        fn extracts_label_from_uses_edge() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::from_edge(EdgeId(1))]; // uses edge

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("uses".to_string()));
        }

        #[test]
        fn extracts_labels_from_multiple_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![
                Traverser::from_edge(EdgeId(0)), // knows
                Traverser::from_edge(EdgeId(1)), // uses
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("knows".to_string()));
            assert_eq!(output[1].value, Value::String("uses".to_string()));
        }

        #[test]
        fn nonexistent_edge_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::from_edge(EdgeId(999))]; // Non-existent

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod label_step_non_element_tests {
        use super::*;

        #[test]
        fn filters_out_integer_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::new(Value::String("hello".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::new(Value::Null)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_boolean_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::new(Value::Bool(true))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_float_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::new(Value::Float(3.14))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn mixed_elements_and_non_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input = vec![
                Traverser::from_vertex(VertexId(0)),                // person
                Traverser::new(Value::Int(42)),                     // filtered out
                Traverser::from_edge(EdgeId(0)),                    // knows
                Traverser::new(Value::String("hello".to_string())), // filtered out
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("person".to_string()));
            assert_eq!(output[1].value, Value::String("knows".to_string()));
        }
    }

    mod label_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_from_input_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod label_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = LabelStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }
}
