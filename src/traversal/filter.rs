//! Filter steps for graph traversal.
//!
//! This module provides filter steps that pass through or reject traversers
//! based on various predicates. Filter steps are 1:1 operations - each input
//! traverser produces at most one output traverser.
//!
//! # Steps
//!
//! - `HasLabelStep`: Filters elements by label
//! - `HasStep`: Filters elements by property existence
//! - `HasValueStep`: Filters elements by property value equality
//! - `FilterStep`: Generic filter with custom predicate

use crate::impl_filter_step;
use crate::traversal::step::AnyStep;
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
// HasStep - filter by property existence
// -----------------------------------------------------------------------------

/// Filter step that keeps only elements with a specific property.
///
/// Works with both vertices and edges. Non-element values (integers, strings, etc.)
/// are filtered out since they don't have properties.
///
/// # Example
///
/// ```ignore
/// // Filter to only vertices that have an "age" property
/// let with_age = g.v().has("age").to_list();
/// ```
#[derive(Clone, Debug)]
pub struct HasStep {
    /// The property key to check for existence
    key: String,
}

impl HasStep {
    /// Create a new HasStep that checks for property existence.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to check
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    /// Check if a traverser's element has the property.
    ///
    /// Returns `false` for non-element values (integers, strings, etc.).
    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                // Get the vertex from the snapshot and check property existence
                ctx.snapshot()
                    .storage()
                    .get_vertex(*id)
                    .map(|v| v.properties.contains_key(&self.key))
                    .unwrap_or(false)
            }
            Value::Edge(id) => {
                // Get the edge from the snapshot and check property existence
                ctx.snapshot()
                    .storage()
                    .get_edge(*id)
                    .map(|e| e.properties.contains_key(&self.key))
                    .unwrap_or(false)
            }
            // Non-element values don't have properties
            _ => false,
        }
    }
}

// Use the macro to implement AnyStep for HasStep
impl_filter_step!(HasStep, "has");

// -----------------------------------------------------------------------------
// HasValueStep - filter by property value equality
// -----------------------------------------------------------------------------

/// Filter step that keeps only elements with a specific property value.
///
/// Works with both vertices and edges. Non-element values (integers, strings, etc.)
/// are filtered out since they don't have properties.
///
/// # Example
///
/// ```ignore
/// // Filter to only vertices where name == "Alice"
/// let alice = g.v().has_value("name", "Alice").to_list();
///
/// // Filter to vertices where age == 30
/// let age_30 = g.v().has_value("age", 30i64).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct HasValueStep {
    /// The property key to check
    key: String,
    /// The expected value
    value: Value,
}

impl HasValueStep {
    /// Create a new HasValueStep that checks for property value equality.
    ///
    /// # Arguments
    ///
    /// * `key` - The property key to check
    /// * `value` - The expected value
    pub fn new(key: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Check if a traverser's element has the property with the expected value.
    ///
    /// Returns `false` for non-element values (integers, strings, etc.).
    fn matches(&self, ctx: &ExecutionContext, traverser: &Traverser) -> bool {
        match &traverser.value {
            Value::Vertex(id) => {
                // Get the vertex from the snapshot and check property value
                if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                    vertex
                        .properties
                        .get(&self.key)
                        .map(|pv| pv == &self.value)
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            Value::Edge(id) => {
                // Get the edge from the snapshot and check property value
                if let Some(edge) = ctx.snapshot().storage().get_edge(*id) {
                    edge.properties
                        .get(&self.key)
                        .map(|pv| pv == &self.value)
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            // Non-element values don't have properties
            _ => false,
        }
    }
}

// Use the macro to implement AnyStep for HasValueStep
impl_filter_step!(HasValueStep, "has");

// -----------------------------------------------------------------------------
// FilterStep - generic filter with custom predicate
// -----------------------------------------------------------------------------

/// Generic filter step that uses a custom predicate closure.
///
/// The predicate receives the execution context and the value, returning
/// `true` to keep the traverser or `false` to filter it out.
///
/// # Type Requirements
///
/// The predicate closure must be:
/// - `Fn(&ExecutionContext, &Value) -> bool` - takes context and value
/// - `Clone` - required for step cloning in branching operations
/// - `Send + Sync` - required for thread safety
/// - `'static` - required for storage in boxed trait objects
///
/// # Example
///
/// ```ignore
/// // Filter to only positive integers
/// let positives = g.inject([1i64, -2i64, 3i64])
///     .filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 0))
///     .to_list();
///
/// // Filter using graph context
/// let connected = g.v()
///     .filter(|ctx, v| {
///         if let Some(id) = v.as_vertex_id() {
///             ctx.snapshot().storage().get_vertex(id)
///                 .map(|_| true)
///                 .unwrap_or(false)
///         } else {
///             false
///         }
///     })
///     .to_list();
/// ```
#[derive(Clone)]
pub struct FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync,
{
    /// The predicate closure
    predicate: F,
}

impl<F> FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync,
{
    /// Create a new FilterStep with the given predicate.
    ///
    /// # Arguments
    ///
    /// * `predicate` - A closure that returns `true` to keep the traverser
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> std::fmt::Debug for FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterStep")
            .field("predicate", &"<closure>")
            .finish()
    }
}

impl<F> AnyStep for FilterStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> bool + Clone + Send + Sync + 'static,
{
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let predicate = self.predicate.clone();
        Box::new(input.filter(move |t| predicate(ctx, &t.value)))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "filter"
    }
}

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

    mod has_step_tests {
        use super::*;
        use crate::traversal::step::AnyStep;

        fn create_graph_with_properties() -> Graph {
            let mut storage = InMemoryGraph::new();

            // Vertex 0: person with name and age
            storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            // Vertex 1: person with only name
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
                    props
                })
                .unwrap();

            // Edge 1: uses with no properties
            storage
                .add_edge(VertexId(1), VertexId(2), "uses", HashMap::new())
                .unwrap();

            Graph::new(Arc::new(storage))
        }

        #[test]
        fn new_creates_step_with_key() {
            let step = HasStep::new("age");
            assert_eq!(step.key, "age");
        }

        #[test]
        fn name_returns_has() {
            let step = HasStep::new("age");
            assert_eq!(step.name(), "has");
        }

        #[test]
        fn clone_box_works() {
            let step = HasStep::new("age");
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "has");
        }

        #[test]
        fn filters_vertices_with_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasStep::new("age");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has age
                Traverser::from_vertex(VertexId(1)), // no age
                Traverser::from_vertex(VertexId(2)), // no age
                Traverser::from_vertex(VertexId(3)), // no properties
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only vertex 0 has "age" property
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_vertices_by_name_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has name
                Traverser::from_vertex(VertexId(1)), // has name
                Traverser::from_vertex(VertexId(2)), // has name
                Traverser::from_vertex(VertexId(3)), // no name
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Vertices 0, 1, 2 have "name" property
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn filters_edges_with_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasStep::new("since");

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // has since
                Traverser::from_edge(EdgeId(1)), // no since
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only edge 0 has "since" property
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn filters_out_non_element_values() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has name - should pass
                Traverser::new(Value::Int(42)),      // not an element
                Traverser::new(Value::String("hello".to_string())), // not an element
                Traverser::new(Value::Null),         // not an element
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_nonexistent_vertices() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasStep::new("name");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),   // exists, has name
                Traverser::from_vertex(VertexId(999)), // doesn't exist
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn returns_empty_for_nonexistent_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasStep::new("nonexistent_property");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasStep::new("name");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasStep::new("name");

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
        fn debug_format() {
            let step = HasStep::new("age");
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasStep"));
            assert!(debug_str.contains("age"));
        }
    }

    mod has_value_step_tests {
        use super::*;
        use crate::traversal::step::AnyStep;

        fn create_graph_with_properties() -> Graph {
            let mut storage = InMemoryGraph::new();

            // Vertex 0: person Alice, age 30
            storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            // Vertex 1: person Bob, age 25
            storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props.insert("age".to_string(), Value::Int(25));
                props
            });

            // Vertex 2: person Charlie, age 30
            storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Charlie".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            // Vertex 3: software with version 1.0
            storage.add_vertex("software", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Graph DB".to_string()));
                props.insert("version".to_string(), Value::Float(1.0));
                props
            });

            // Edge 0: knows since 2020
            storage
                .add_edge(VertexId(0), VertexId(1), "knows", {
                    let mut props = HashMap::new();
                    props.insert("since".to_string(), Value::Int(2020));
                    props
                })
                .unwrap();

            // Edge 1: knows since 2019
            storage
                .add_edge(VertexId(1), VertexId(2), "knows", {
                    let mut props = HashMap::new();
                    props.insert("since".to_string(), Value::Int(2019));
                    props
                })
                .unwrap();

            Graph::new(Arc::new(storage))
        }

        #[test]
        fn new_creates_step_with_key_and_value() {
            let step = HasValueStep::new("age", 30i64);
            assert_eq!(step.key, "age");
            assert_eq!(step.value, Value::Int(30));
        }

        #[test]
        fn name_returns_has() {
            let step = HasValueStep::new("age", 30i64);
            assert_eq!(step.name(), "has");
        }

        #[test]
        fn clone_box_works() {
            let step = HasValueStep::new("age", 30i64);
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "has");
        }

        #[test]
        fn filters_vertices_by_string_value() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasValueStep::new("name", "Alice");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Alice
                Traverser::from_vertex(VertexId(1)), // Bob
                Traverser::from_vertex(VertexId(2)), // Charlie
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_vertices_by_int_value() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasValueStep::new("age", 30i64);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30
                Traverser::from_vertex(VertexId(1)), // age 25
                Traverser::from_vertex(VertexId(2)), // age 30
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Vertices 0 and 2 have age 30
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(2)));
        }

        #[test]
        fn filters_vertices_by_float_value() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasValueStep::new("version", Value::Float(1.0));

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // no version
                Traverser::from_vertex(VertexId(3)), // version 1.0
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(3)));
        }

        #[test]
        fn filters_edges_by_value() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasValueStep::new("since", 2020i64);

            let input: Vec<Traverser> = vec![
                Traverser::from_edge(EdgeId(0)), // since 2020
                Traverser::from_edge(EdgeId(1)), // since 2019
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_edge_id(), Some(EdgeId(0)));
        }

        #[test]
        fn filters_out_vertices_with_different_value() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasValueStep::new("age", 99i64);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // age 30
                Traverser::from_vertex(VertexId(1)), // age 25
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_vertices_without_property() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasValueStep::new("age", 30i64);

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // has age
                Traverser::from_vertex(VertexId(3)), // software, no age
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_non_element_values() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasValueStep::new("name", "Alice");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)), // Alice - should pass
                Traverser::new(Value::Int(42)),      // not an element
                Traverser::new(Value::String("Alice".to_string())), // not an element, even with matching value
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
        }

        #[test]
        fn filters_out_nonexistent_vertices() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasValueStep::new("name", "Alice");

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),   // exists
                Traverser::from_vertex(VertexId(999)), // doesn't exist
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasValueStep::new("name", "Alice");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_graph_with_properties();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = HasValueStep::new("name", "Alice");

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
        fn debug_format() {
            let step = HasValueStep::new("age", 30i64);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("HasValueStep"));
            assert!(debug_str.contains("age"));
        }
    }

    mod filter_step_tests {
        use super::*;
        use crate::traversal::step::AnyStep;

        fn create_test_graph() -> Graph {
            let mut storage = InMemoryGraph::new();

            storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Alice".to_string()));
                props.insert("age".to_string(), Value::Int(30));
                props
            });

            storage.add_vertex("person", {
                let mut props = HashMap::new();
                props.insert("name".to_string(), Value::String("Bob".to_string()));
                props.insert("age".to_string(), Value::Int(25));
                props
            });

            Graph::new(Arc::new(storage))
        }

        #[test]
        fn new_creates_filter_step() {
            let step = FilterStep::new(|_ctx, _v| true);
            assert_eq!(step.name(), "filter");
        }

        #[test]
        fn name_returns_filter() {
            let step = FilterStep::new(|_ctx, _v| true);
            assert_eq!(step.name(), "filter");
        }

        #[test]
        fn clone_box_works() {
            let step = FilterStep::new(|_ctx, _v| true);
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "filter");
        }

        #[test]
        fn filters_with_always_true_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FilterStep::new(|_ctx, _v| true);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
        }

        #[test]
        fn filters_with_always_false_predicate() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FilterStep::new(|_ctx, _v| false);

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_positive_integers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FilterStep::new(|_ctx, v| matches!(v, Value::Int(n) if *n > 0));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::Int(-2)),
                Traverser::new(Value::Int(0)),
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(3));
        }

        #[test]
        fn filters_by_value_type() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FilterStep::new(|_ctx, v| v.is_vertex());

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::new(Value::Int(42)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::new(Value::String("hello".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert!(output[0].is_vertex());
            assert!(output[1].is_vertex());
        }

        #[test]
        fn can_access_execution_context() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Filter that checks if vertex exists in the graph
            let step = FilterStep::new(|ctx, v| {
                if let Some(id) = v.as_vertex_id() {
                    ctx.snapshot().storage().get_vertex(id).is_some()
                } else {
                    false
                }
            });

            let input: Vec<Traverser> = vec![
                Traverser::from_vertex(VertexId(0)),   // exists
                Traverser::from_vertex(VertexId(999)), // doesn't exist
                Traverser::from_vertex(VertexId(1)),   // exists
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].as_vertex_id(), Some(VertexId(0)));
            assert_eq!(output[1].as_vertex_id(), Some(VertexId(1)));
        }

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FilterStep::new(|_ctx, _v| true);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn preserves_traverser_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FilterStep::new(|_ctx, _v| true);

            let mut traverser = Traverser::new(Value::Int(42));
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
        fn debug_format() {
            let step = FilterStep::new(|_ctx, _v| true);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("FilterStep"));
            assert!(debug_str.contains("<closure>"));
        }

        #[test]
        fn filter_with_string_matching() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step =
                FilterStep::new(|_ctx, v| matches!(v, Value::String(s) if s.starts_with("A")));

            let input: Vec<Traverser> = vec![
                Traverser::new(Value::String("Alice".to_string())),
                Traverser::new(Value::String("Bob".to_string())),
                Traverser::new(Value::String("Anna".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn filter_step_is_cloneable() {
            let step1 = FilterStep::new(|_ctx, v| matches!(v, Value::Int(n) if *n > 0));
            let step2 = step1.clone();

            // Both should work identically
            assert_eq!(step1.name(), step2.name());
        }
    }
}
