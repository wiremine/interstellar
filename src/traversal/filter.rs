//! Filter steps for graph traversal.
//!
//! This module provides filter steps that pass through or reject traversers
//! based on various predicates. Filter steps are 1:1 operations - each input
//! traverser produces at most one output traverser.
//!
//! # Steps
//!
//! - `HasLabelStep`: Filters elements by label

use crate::impl_filter_step;
use crate::traversal::{ExecutionContext, Traverser};
use crate::value::Value;

// -----------------------------------------------------------------------------
// HasLabelStep - filter by element label
// -----------------------------------------------------------------------------

/// Filter step that keeps only elements with matching labels.
///
/// Works with both vertices and edges. Non-element values (integers, strings, etc.)
/// are filtered out.
///
/// # Example
///
/// ```ignore
/// // Filter to only "person" vertices
/// let people = g.v().has_label("person").to_list();
///
/// // Filter to vertices with any of the given labels
/// let entities = g.v().has_label_any(&["person", "company"]).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct HasLabelStep {
    /// Labels to match against (element must match any one)
    labels: Vec<String>,
}

impl HasLabelStep {
    /// Create a new HasLabelStep that matches any of the given labels.
    ///
    /// # Arguments
    ///
    /// * `labels` - Labels to match against
    pub fn new(labels: Vec<String>) -> Self {
        Self { labels }
    }

    /// Create a HasLabelStep for a single label.
    ///
    /// # Arguments
    ///
    /// * `label` - The label to match
    pub fn single(label: impl Into<String>) -> Self {
        Self {
            labels: vec![label.into()],
        }
    }

    /// Create a HasLabelStep for multiple labels.
    ///
    /// # Arguments
    ///
    /// * `labels` - Labels to match (element must match any one)
    pub fn any<I, S>(labels: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            labels: labels.into_iter().map(Into::into).collect(),
        }
    }

    /// Check if a traverser's element has a matching label.
    ///
    /// Returns `false` for non-element values (integers, strings, etc.).
    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                // Get the vertex from the snapshot
                if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                    self.labels.iter().any(|l| l == &vertex.label)
                } else {
                    false
                }
            }
            Value::Edge(id) => {
                // Get the edge from the snapshot
                if let Some(edge) = ctx.snapshot().storage().get_edge(*id) {
                    self.labels.iter().any(|l| l == &edge.label)
                } else {
                    false
                }
            }
            // Non-element values don't have labels
            _ => false,
        }
    }
}

// Use the macro to implement AnyStep for HasLabelStep
impl_filter_step!(HasLabelStep, "hasLabel");

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    use crate::value::{EdgeId, VertexId};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();

        // Add vertices with different labels
        storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props
        });
        storage.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props
        });
        storage.add_vertex("software", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Graph DB".to_string()));
            props
        });
        storage.add_vertex("company", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("TechCorp".to_string()));
            props
        });

        // Add edges with different labels
        storage
            .add_edge(VertexId(0), VertexId(1), "knows", HashMap::new())
            .unwrap();
        storage
            .add_edge(VertexId(1), VertexId(2), "uses", HashMap::new())
            .unwrap();
        storage
            .add_edge(VertexId(0), VertexId(3), "works_at", HashMap::new())
            .unwrap();

        Graph::new(Arc::new(storage))
    }

    mod has_label_step_tests {
        use super::*;
        use crate::traversal::step::AnyStep;

        #[test]
        fn single_creates_single_label_step() {
            let step = HasLabelStep::single("person");
            assert_eq!(step.labels, vec!["person".to_string()]);
        }

        #[test]
        fn new_creates_multi_label_step() {
            let step = HasLabelStep::new(vec!["person".to_string(), "company".to_string()]);
            assert_eq!(step.labels.len(), 2);
        }

        #[test]
        fn any_creates_multi_label_step() {
            let step = HasLabelStep::any(["person", "company", "software"]);
            assert_eq!(step.labels.len(), 3);
        }

        #[test]
        fn name_returns_has_label() {
            let step = HasLabelStep::single("person");
            assert_eq!(step.name(), "hasLabel");
        }

        #[test]
        fn clone_box_works() {
            let step = HasLabelStep::single("person");
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "hasLabel");
        }

        #[test]
        fn filters_vertices_by_single_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasLabelStep::single("person");

            // Create traversers for all vertices
            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // person (Alice)
                Traverser::from_vertex(VertexId(1)), // person (Bob)
                Traverser::from_vertex(VertexId(2)), // software
                Traverser::from_vertex(VertexId(3)), // company
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only 2 person vertices should pass
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(1)));
        }

        #[test]
        fn filters_vertices_by_multiple_labels() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasLabelStep::any(["person", "company"]);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // person
                Traverser::from_vertex(VertexId(1)), // person
                Traverser::from_vertex(VertexId(2)), // software
                Traverser::from_vertex(VertexId(3)), // company
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // 2 persons + 1 company = 3
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn filters_edges_by_single_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasLabelStep::single("knows");

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // knows
                Traverser::from_edge(EdgeId(1)), // uses
                Traverser::from_edge(EdgeId(2)), // works_at
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only 1 "knows" edge
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn filters_edges_by_multiple_labels() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasLabelStep::any(["knows", "uses"]);

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // knows
                Traverser::from_edge(EdgeId(1)), // uses
                Traverser::from_edge(EdgeId(2)), // works_at
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // "knows" + "uses" = 2
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn filters_out_non_element_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasLabelStep::single("person");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // person - should pass
                Traverser::new(Value::Int(42)),      // not an element
                Traverser::new(Value::String("hello".to_string())), // not an element
                Traverser::new(Value::Bool(true)),   // not an element
                Traverser::new(Value::Null),         // not an element
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only the person vertex should pass
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_nonexistent_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasLabelStep::single("person");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),   // exists
                Traverser::from_vertex(VertexId(999)), // doesn't exist
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only existing person vertex should pass
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_nonexistent_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasLabelStep::single("knows");

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)),   // exists
                Traverser::from_edge(EdgeId(999)), // doesn't exist
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only existing "knows" edge should pass
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn returns_empty_for_nonexistent_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasLabelStep::single("nonexistent_label");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // No vertices match "nonexistent_label"
            assert!(output.is_empty());
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasLabelStep::single("person");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasLabelStep::single("person");

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");
            traverser.loops = 5;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 5);
            assert_eq!(output[0].bulk, 10);
        }

        #[test]
        fn mixed_vertices_and_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // This filter should only match "person" vertices, not edges
            let step = HasLabelStep::single("person");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // person - match
                Traverser::from_edge(EdgeId(0)),     // "knows" edge - no match
                Traverser::from_vertex(VertexId(2)), // software - no match
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn debug_format() {
            let step = HasLabelStep::any(["person", "company"]);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasLabelStep"));
            assert!(debug_str.contains("person"));
            assert!(debug_str.contains("company"));
        }
    }
}
