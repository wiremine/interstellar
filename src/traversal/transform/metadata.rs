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

impl crate::traversal::step::Step for IdStep {
    type Iter<'a>
        = impl Iterator<Item = crate::traversal::Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.filter_map(|traverser| {
            match &traverser.value {
                Value::Vertex(id) => {
                    // Return the vertex ID as an integer
                    Some(traverser.split(Value::Int(id.0 as i64)))
                }
                Value::Edge(id) => {
                    // Return the edge ID as an integer
                    Some(traverser.split(Value::Int(id.0 as i64)))
                }
                // Handle pending mutations: mark for ID extraction after mutation execution
                Value::Map(map)
                    if map.contains_key("__pending_add_v")
                        || map.contains_key("__pending_add_e") =>
                {
                    let mut new_map = map.clone();
                    new_map.insert("__extract_id".to_string(), Value::Bool(true));
                    Some(traverser.split(Value::Map(new_map)))
                }
                // Non-element values are filtered out
                _ => None,
            }
        })
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

impl crate::traversal::step::Step for LabelStep {
    type Iter<'a>
        = impl Iterator<Item = crate::traversal::Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.filter_map(move |traverser| {
            match &traverser.value {
                Value::Vertex(id) => {
                    // Get the vertex and its label (already resolved by storage)
                    let vertex = ctx.storage().get_vertex(*id)?;
                    Some(traverser.split(Value::String(vertex.label.clone())))
                }
                Value::Edge(id) => {
                    // Get the edge and its label (already resolved by storage)
                    let edge = ctx.storage().get_edge(*id)?;
                    Some(traverser.split(Value::String(edge.label.clone())))
                }
                // Non-element values are filtered out
                _ => None,
            }
        })
    }

    fn name(&self) -> &'static str {
        "label"
    }
}

// -----------------------------------------------------------------------------
// KeyStep - extract property key from property objects
// -----------------------------------------------------------------------------

/// Transform step that extracts the key from property map objects.
///
/// This step is designed to work with the output of `properties()`, which produces
/// `Value::Map` objects containing "key" and "value" entries. `KeyStep` extracts
/// the "key" field from these property objects.
///
/// # Behavior
///
/// - For `Value::Map` with a "key" field: returns the value of the "key" field
/// - For all other values: filtered out (produces no output)
///
/// # Example
///
/// ```ignore
/// // Get all property keys for person vertices
/// let keys = g.v().has_label("person").properties().key().to_list();
/// // Returns: ["name", "age", "name", "age", ...]
///
/// // Get unique property keys
/// let unique_keys = g.v().properties().key().dedup().to_list();
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct KeyStep;

impl KeyStep {
    /// Create a new KeyStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for KeyStep {
    type Iter<'a>
        = impl Iterator<Item = crate::traversal::Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.filter_map(|traverser| {
            match &traverser.value {
                Value::Map(map) => {
                    // Extract the "key" field from property objects
                    map.get("key").cloned().map(|key| traverser.split(key))
                }
                // Non-map values are filtered out
                _ => None,
            }
        })
    }

    fn name(&self) -> &'static str {
        "key"
    }
}

// -----------------------------------------------------------------------------
// ValueStep - extract property value from property objects
// -----------------------------------------------------------------------------

/// Transform step that extracts the value from property map objects.
///
/// This step is designed to work with the output of `properties()`, which produces
/// `Value::Map` objects containing "key" and "value" entries. `ValueStep` extracts
/// the "value" field from these property objects.
///
/// # Behavior
///
/// - For `Value::Map` with a "value" field: returns the value of the "value" field
/// - For all other values: filtered out (produces no output)
///
/// # Example
///
/// ```ignore
/// // Get all property values for person vertices
/// let values = g.v().has_label("person").properties().value().to_list();
/// // Returns: ["Alice", 30, "Bob", 25, ...]
///
/// // Get property values for specific keys
/// let ages = g.v().properties_keys(&["age"]).value().to_list();
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct ValueStep;

impl ValueStep {
    /// Create a new ValueStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for ValueStep {
    type Iter<'a>
        = impl Iterator<Item = crate::traversal::Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.filter_map(|traverser| {
            match &traverser.value {
                Value::Map(map) => {
                    // Extract the "value" field from property objects
                    map.get("value").cloned().map(|val| traverser.split(val))
                }
                // Non-map values are filtered out
                _ => None,
            }
        })
    }

    fn name(&self) -> &'static str {
        "value"
    }
}

// -----------------------------------------------------------------------------
// LoopsStep - get current loop depth from traverser
// -----------------------------------------------------------------------------

/// Transform step that extracts the current loop depth from traversers.
///
/// This step returns the loop count stored in each traverser, which is incremented
/// by `repeat()` operations. Outside of a repeat loop, the value is 0.
///
/// # Behavior
///
/// - Returns `Value::Int` representing the current loop iteration count
/// - Uses 0-based indexing (first iteration = 0, second = 1, etc.)
/// - Outside of repeat: returns 0
/// - 1:1 mapping, no filtering - every input produces exactly one output
///
/// # Example
///
/// ```ignore
/// // Get loop depth at each emit
/// let depths = g.v()
///     .has_label("person")
///     .repeat(__.out())
///     .times(3)
///     .emit()
///     .loops()
///     .to_list();
/// // Returns: [0, 0, 0, 1, 1, 2, ...] (loop depths when emitted)
///
/// // Use in until condition
/// let vertices = g.v()
///     .repeat(__.out())
///     .until(__.loops().is_(p::gte(3)))
///     .to_list();
/// ```
///
/// # Note
///
/// This uses 0-based indexing which differs from Gremlin's 1-based indexing.
/// In Gremlin, `loops()` returns 1 on the first iteration, while here it returns 0.
#[derive(Clone, Copy, Debug, Default)]
pub struct LoopsStep;

impl LoopsStep {
    /// Create a new LoopsStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for LoopsStep {
    type Iter<'a>
        = impl Iterator<Item = crate::traversal::Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.map(|traverser| {
            // Extract the loops count and convert to Value::Int
            let loops = traverser.loops as i64;
            traverser.split(Value::Int(loops))
        })
    }

    fn name(&self) -> &'static str {
        "loops"
    }
}

// -----------------------------------------------------------------------------
// IndexStep - annotate stream with position index
// -----------------------------------------------------------------------------

use std::cell::Cell;

/// Transform step that annotates each traverser with its position in the stream.
///
/// This step wraps each value in a `[value, index]` list, where the index is
/// the 0-based position of the element in the stream.
///
/// # Behavior
///
/// - Wraps each value in a `Value::List` with `[original_value, index]`
/// - Index is 0-based `Value::Int` (first element = 0, second = 1, etc.)
/// - Stateful step: tracks position counter across iteration
/// - Preserves all traverser metadata (path, loops, bulk)
/// - 1:1 mapping - every input produces exactly one output
///
/// # Example
///
/// ```ignore
/// // Get elements with their indices
/// let indexed = g.v()
///     .index()
///     .to_list();
/// // Returns: [[v[0], 0], [v[1], 1], [v[2], 2], ...]
///
/// // Get names with indices
/// let indexed_names = g.v()
///     .values("name")
///     .index()
///     .to_list();
/// // Returns: [["Alice", 0], ["Bob", 1], ...]
///
/// // Extract just values or indices with unfold
/// let with_indices = g.v()
///     .index()
///     .unfold()
///     .to_list();
/// // Returns: [v[0], 0, v[1], 1, v[2], 2, ...]
/// ```
#[derive(Clone, Debug, Default)]
pub struct IndexStep;

impl IndexStep {
    /// Create a new IndexStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for IndexStep {
    type Iter<'a>
        = impl Iterator<Item = crate::traversal::Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let counter = Cell::new(0usize);

        input.map(move |traverser| {
            let idx = counter.get();
            counter.set(idx + 1);

            // Wrap the value in a [value, index] list
            let indexed = Value::List(vec![traverser.value.clone(), Value::Int(idx as i64)]);
            traverser.split(indexed)
        })
    }

    fn name(&self) -> &'static str {
        "index"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use crate::traversal::step::Step;
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
            let step = IdStep;
            assert_eq!(step.name(), "id");
        }

        #[test]
        fn clone_works() {
            let step = IdStep::new();
            let cloned = step.clone();
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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::new(Value::String("hello".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::new(Value::Null)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_boolean_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::new(Value::Bool(true))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_float_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();
            let input = vec![Traverser::new(Value::Float(3.15))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn mixed_elements_and_non_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod id_step_pending_mutation_tests {
        use super::*;

        /// Helper to create a pending add_v mutation marker
        fn create_pending_add_v(label: &str) -> Value {
            let mut map = HashMap::new();
            map.insert("__pending_add_v".to_string(), Value::Bool(true));
            map.insert("label".to_string(), Value::String(label.to_string()));
            map.insert("properties".to_string(), Value::Map(HashMap::new()));
            Value::Map(map)
        }

        /// Helper to create a pending add_e mutation marker
        fn create_pending_add_e(label: &str, from: VertexId, to: VertexId) -> Value {
            let mut map = HashMap::new();
            map.insert("__pending_add_e".to_string(), Value::Bool(true));
            map.insert("label".to_string(), Value::String(label.to_string()));
            map.insert("from".to_string(), Value::Vertex(from));
            map.insert("to".to_string(), Value::Vertex(to));
            map.insert("properties".to_string(), Value::Map(HashMap::new()));
            Value::Map(map)
        }

        #[test]
        fn preserves_pending_add_v_with_extract_id_flag() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();
            let pending = create_pending_add_v("person");
            let input = vec![Traverser::new(pending)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert!(map.contains_key("__pending_add_v"));
                assert!(map.contains_key("__extract_id"));
                assert_eq!(map.get("__extract_id"), Some(&Value::Bool(true)));
            } else {
                panic!("Expected Map, got {:?}", output[0].value);
            }
        }

        #[test]
        fn preserves_pending_add_e_with_extract_id_flag() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();
            let pending = create_pending_add_e("knows", VertexId(0), VertexId(1));
            let input = vec![Traverser::new(pending)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert!(map.contains_key("__pending_add_e"));
                assert!(map.contains_key("__extract_id"));
                assert_eq!(map.get("__extract_id"), Some(&Value::Bool(true)));
            } else {
                panic!("Expected Map, got {:?}", output[0].value);
            }
        }

        #[test]
        fn preserves_all_pending_mutation_fields() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();

            // Create a pending add_v with properties
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));

            let mut pending_map = HashMap::new();
            pending_map.insert("__pending_add_v".to_string(), Value::Bool(true));
            pending_map.insert("label".to_string(), Value::String("person".to_string()));
            pending_map.insert("properties".to_string(), Value::Map(props));

            let input = vec![Traverser::new(Value::Map(pending_map))];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                // Original fields preserved
                assert!(map.contains_key("__pending_add_v"));
                assert_eq!(map.get("label"), Some(&Value::String("person".to_string())));

                // Properties preserved
                if let Some(Value::Map(props)) = map.get("properties") {
                    assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
                    assert_eq!(props.get("age"), Some(&Value::Int(30)));
                } else {
                    panic!("Properties not preserved");
                }

                // New extract_id flag added
                assert_eq!(map.get("__extract_id"), Some(&Value::Bool(true)));
            } else {
                panic!("Expected Map");
            }
        }

        #[test]
        fn filters_out_regular_maps_without_pending_marker() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();

            // Regular map without pending mutation marker
            let mut regular_map = HashMap::new();
            regular_map.insert("name".to_string(), Value::String("Alice".to_string()));

            let input = vec![Traverser::new(Value::Map(regular_map))];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn handles_mixed_elements_and_pending_mutations() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();
            let pending_v = create_pending_add_v("person");
            let pending_e = create_pending_add_e("knows", VertexId(0), VertexId(1));

            let input = vec![
                Traverser::from_vertex(VertexId(5)), // Real vertex -> extract ID
                Traverser::new(pending_v),           // Pending vertex -> add flag
                Traverser::new(Value::Int(42)),      // Filtered out
                Traverser::new(pending_e),           // Pending edge -> add flag
                Traverser::from_edge(EdgeId(10)),    // Real edge -> extract ID
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 4);

            // First: real vertex ID extracted
            assert_eq!(output[0].value, Value::Int(5));

            // Second: pending add_v with extract_id flag
            if let Value::Map(map) = &output[1].value {
                assert!(map.contains_key("__pending_add_v"));
                assert!(map.contains_key("__extract_id"));
            } else {
                panic!("Expected pending add_v map");
            }

            // Third: pending add_e with extract_id flag
            if let Value::Map(map) = &output[2].value {
                assert!(map.contains_key("__pending_add_e"));
                assert!(map.contains_key("__extract_id"));
            } else {
                panic!("Expected pending add_e map");
            }

            // Fourth: real edge ID extracted
            assert_eq!(output[3].value, Value::Int(10));
        }

        #[test]
        fn preserves_traverser_metadata_for_pending_mutations() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IdStep::new();

            let pending = create_pending_add_v("person");
            let mut traverser = Traverser::new(pending);
            traverser.extend_path_labeled("start");
            traverser.loops = 3;
            traverser.bulk = 5;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].loops, 3);
            assert_eq!(output[0].bulk, 5);
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
            let step = LabelStep;
            assert_eq!(step.name(), "label");
        }

        #[test]
        fn clone_works() {
            let step = LabelStep::new();
            let cloned = step.clone();
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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::new(Value::String("hello".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::new(Value::Null)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_boolean_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::new(Value::Bool(true))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_float_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LabelStep::new();
            let input = vec![Traverser::new(Value::Float(3.15))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn mixed_elements_and_non_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

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
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LabelStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    // =========================================================================
    // KeyStep Tests
    // =========================================================================

    /// Helper to create a property map object (mimics properties() step output)
    fn create_property_map(key: &str, value: Value) -> Value {
        let mut map = std::collections::HashMap::new();
        map.insert("key".to_string(), Value::String(key.to_string()));
        map.insert("value".to_string(), value);
        Value::Map(map)
    }

    mod key_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = KeyStep::new();
            assert_eq!(step.name(), "key");
        }

        #[test]
        fn default_creates_step() {
            let step = KeyStep;
            assert_eq!(step.name(), "key");
        }

        #[test]
        fn clone_works() {
            let step = KeyStep::new();
            let cloned = step.clone();
            assert_eq!(cloned.name(), "key");
        }

        #[test]
        fn debug_format() {
            let step = KeyStep::new();
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("KeyStep"));
        }
    }

    mod key_step_property_map_tests {
        use super::*;

        #[test]
        fn extracts_key_from_property_map() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let prop_map = create_property_map("name", Value::String("Alice".to_string()));
            let input = vec![Traverser::new(prop_map)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("name".to_string()));
        }

        #[test]
        fn extracts_keys_from_multiple_property_maps() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let input = vec![
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Alice".to_string()),
                )),
                Traverser::new(create_property_map("age", Value::Int(30))),
                Traverser::new(create_property_map("active", Value::Bool(true))),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::String("name".to_string()));
            assert_eq!(output[1].value, Value::String("age".to_string()));
            assert_eq!(output[2].value, Value::String("active".to_string()));
        }

        #[test]
        fn handles_map_without_key_field() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            // Map without "key" field
            let mut map = std::collections::HashMap::new();
            map.insert("value".to_string(), Value::String("test".to_string()));
            let input = vec![Traverser::new(Value::Map(map))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn handles_empty_map() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let input = vec![Traverser::new(Value::Map(std::collections::HashMap::new()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod key_step_non_map_tests {
        use super::*;

        #[test]
        fn filters_out_vertex_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_edge_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_integer_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let input = vec![Traverser::new(Value::String("hello".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let input = vec![Traverser::new(Value::Null)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_list_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let input = vec![Traverser::new(Value::List(vec![
                Value::Int(1),
                Value::Int(2),
            ]))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn mixed_maps_and_non_maps() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let input = vec![
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Alice".to_string()),
                )),
                Traverser::new(Value::Int(42)), // filtered out
                Traverser::new(create_property_map("age", Value::Int(30))),
                Traverser::from_vertex(VertexId(0)), // filtered out
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("name".to_string()));
            assert_eq!(output[1].value, Value::String("age".to_string()));
        }
    }

    mod key_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_from_input_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();

            let mut traverser = Traverser::new(create_property_map(
                "name",
                Value::String("Alice".to_string()),
            ));
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

            let step = KeyStep::new();

            let mut traverser = Traverser::new(create_property_map(
                "name",
                Value::String("Alice".to_string()),
            ));
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

            let step = KeyStep::new();

            let mut traverser = Traverser::new(create_property_map(
                "name",
                Value::String("Alice".to_string()),
            ));
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod key_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = KeyStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    // =========================================================================
    // ValueStep Tests
    // =========================================================================

    mod value_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = ValueStep::new();
            assert_eq!(step.name(), "value");
        }

        #[test]
        fn default_creates_step() {
            let step = ValueStep;
            assert_eq!(step.name(), "value");
        }

        #[test]
        fn clone_works() {
            let step = ValueStep::new();
            let cloned = step.clone();
            assert_eq!(cloned.name(), "value");
        }

        #[test]
        fn debug_format() {
            let step = ValueStep::new();
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("ValueStep"));
        }
    }

    mod value_step_property_map_tests {
        use super::*;

        #[test]
        fn extracts_value_from_property_map() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let prop_map = create_property_map("name", Value::String("Alice".to_string()));
            let input = vec![Traverser::new(prop_map)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("Alice".to_string()));
        }

        #[test]
        fn extracts_integer_value() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let prop_map = create_property_map("age", Value::Int(30));
            let input = vec![Traverser::new(prop_map)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(30));
        }

        #[test]
        fn extracts_float_value() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let prop_map = create_property_map("weight", Value::Float(0.8));
            let input = vec![Traverser::new(prop_map)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Float(0.8));
        }

        #[test]
        fn extracts_boolean_value() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let prop_map = create_property_map("active", Value::Bool(true));
            let input = vec![Traverser::new(prop_map)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Bool(true));
        }

        #[test]
        fn extracts_values_from_multiple_property_maps() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let input = vec![
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Alice".to_string()),
                )),
                Traverser::new(create_property_map("age", Value::Int(30))),
                Traverser::new(create_property_map("active", Value::Bool(true))),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::String("Alice".to_string()));
            assert_eq!(output[1].value, Value::Int(30));
            assert_eq!(output[2].value, Value::Bool(true));
        }

        #[test]
        fn handles_null_value_in_property_map() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let prop_map = create_property_map("nothing", Value::Null);
            let input = vec![Traverser::new(prop_map)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Null);
        }

        #[test]
        fn handles_map_without_value_field() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            // Map without "value" field
            let mut map = std::collections::HashMap::new();
            map.insert("key".to_string(), Value::String("test".to_string()));
            let input = vec![Traverser::new(Value::Map(map))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn handles_empty_map() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let input = vec![Traverser::new(Value::Map(std::collections::HashMap::new()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod value_step_non_map_tests {
        use super::*;

        #[test]
        fn filters_out_vertex_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_edge_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_integer_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let input = vec![Traverser::new(Value::String("hello".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let input = vec![Traverser::new(Value::Null)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_list_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let input = vec![Traverser::new(Value::List(vec![
                Value::Int(1),
                Value::Int(2),
            ]))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn mixed_maps_and_non_maps() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let input = vec![
                Traverser::new(create_property_map(
                    "name",
                    Value::String("Alice".to_string()),
                )),
                Traverser::new(Value::Int(42)), // filtered out
                Traverser::new(create_property_map("age", Value::Int(30))),
                Traverser::from_vertex(VertexId(0)), // filtered out
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("Alice".to_string()));
            assert_eq!(output[1].value, Value::Int(30));
        }
    }

    mod value_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_from_input_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();

            let mut traverser = Traverser::new(create_property_map(
                "name",
                Value::String("Alice".to_string()),
            ));
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

            let step = ValueStep::new();

            let mut traverser = Traverser::new(create_property_map(
                "name",
                Value::String("Alice".to_string()),
            ));
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

            let step = ValueStep::new();

            let mut traverser = Traverser::new(create_property_map(
                "name",
                Value::String("Alice".to_string()),
            ));
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod value_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = ValueStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    // =========================================================================
    // LoopsStep Tests
    // =========================================================================

    mod loops_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = LoopsStep::new();
            assert_eq!(step.name(), "loops");
        }

        #[test]
        fn default_creates_step() {
            let step = LoopsStep;
            assert_eq!(step.name(), "loops");
        }

        #[test]
        fn clone_works() {
            let step = LoopsStep::new();
            let cloned = step.clone();
            assert_eq!(cloned.name(), "loops");
        }

        #[test]
        fn debug_format() {
            let step = LoopsStep::new();
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("LoopsStep"));
        }
    }

    mod loops_step_basic_tests {
        use super::*;

        #[test]
        fn returns_zero_for_default_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(0));
        }

        #[test]
        fn returns_loops_count_from_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.loops = 5;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(5));
        }

        #[test]
        fn handles_multiple_traversers_with_different_loops() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut t1 = Traverser::from_vertex(VertexId(0));
            t1.loops = 0;

            let mut t2 = Traverser::from_vertex(VertexId(1));
            t2.loops = 3;

            let mut t3 = Traverser::from_vertex(VertexId(2));
            t3.loops = 10;

            let input = vec![t1, t2, t3];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(0));
            assert_eq!(output[1].value, Value::Int(3));
            assert_eq!(output[2].value, Value::Int(10));
        }

        #[test]
        fn handles_large_loop_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.loops = 1_000_000;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(1_000_000));
        }
    }

    mod loops_step_value_type_tests {
        use super::*;

        #[test]
        fn works_with_vertex_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.loops = 2;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(2));
        }

        #[test]
        fn works_with_edge_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut traverser = Traverser::from_edge(EdgeId(0));
            traverser.loops = 4;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(4));
        }

        #[test]
        fn works_with_integer_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.loops = 1;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(1));
        }

        #[test]
        fn works_with_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut traverser = Traverser::new(Value::String("test".to_string()));
            traverser.loops = 7;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(7));
        }

        #[test]
        fn works_with_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut traverser = Traverser::new(Value::Null);
            traverser.loops = 3;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::Int(3));
        }
    }

    mod loops_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_from_input_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.loops = 2;
            traverser.extend_path_labeled("start");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
        }

        #[test]
        fn preserves_loops_count_in_metadata() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.loops = 5;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            // The loops metadata should be preserved even though value is now the loops count
            assert_eq!(output[0].loops, 5);
        }

        #[test]
        fn preserves_bulk_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.loops = 3;
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod loops_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod loops_step_one_to_one_tests {
        use super::*;

        #[test]
        fn produces_exactly_one_output_per_input() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            // Create 5 traversers with different loop counts
            let input: Vec<Traverser> = (0..5usize)
                .map(|i| {
                    let mut t = Traverser::from_vertex(VertexId((i % 4) as u64));
                    t.loops = i;
                    t
                })
                .collect();

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Should have exactly 5 outputs
            assert_eq!(output.len(), 5);

            // Verify each output has correct loops value
            for (i, t) in output.iter().enumerate() {
                assert_eq!(t.value, Value::Int(i as i64));
            }
        }

        #[test]
        fn no_filtering_occurs() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = LoopsStep::new();

            // Mix of different value types - all should produce output
            let mut t1 = Traverser::from_vertex(VertexId(0));
            t1.loops = 1;

            let mut t2 = Traverser::from_edge(EdgeId(0));
            t2.loops = 2;

            let mut t3 = Traverser::new(Value::Int(42));
            t3.loops = 3;

            let mut t4 = Traverser::new(Value::Null);
            t4.loops = 4;

            let mut t5 = Traverser::new(Value::List(vec![]));
            t5.loops = 5;

            let input = vec![t1, t2, t3, t4, t5];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // All 5 inputs should produce outputs
            assert_eq!(output.len(), 5);
        }
    }

    // =========================================================================
    // IndexStep Tests
    // =========================================================================

    mod index_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = IndexStep::new();
            assert_eq!(step.name(), "index");
        }

        #[test]
        fn default_creates_step() {
            let step = IndexStep;
            assert_eq!(step.name(), "index");
        }

        #[test]
        fn clone_works() {
            let step = IndexStep::new();
            let cloned = step.clone();
            assert_eq!(cloned.name(), "index");
        }

        #[test]
        fn debug_format() {
            let step = IndexStep::new();
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("IndexStep"));
        }
    }

    mod index_step_basic_tests {
        use super::*;

        #[test]
        fn first_element_gets_index_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list.len(), 2);
                assert_eq!(list[0], Value::Vertex(VertexId(0)));
                assert_eq!(list[1], Value::Int(0));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn indices_increment_sequentially() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();
            let input = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_vertex(VertexId(1)),
                Traverser::from_vertex(VertexId(2)),
                Traverser::from_vertex(VertexId(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 4);
            for (i, t) in output.iter().enumerate() {
                if let Value::List(list) = &t.value {
                    assert_eq!(list[1], Value::Int(i as i64));
                } else {
                    panic!("Expected Value::List at index {}", i);
                }
            }
        }

        #[test]
        fn output_format_is_value_index_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();
            let input = vec![Traverser::new(Value::String("test".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list.len(), 2);
                assert_eq!(list[0], Value::String("test".to_string()));
                assert_eq!(list[1], Value::Int(0));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn preserves_original_value_in_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();

            // Test with various value types
            let input = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_edge(EdgeId(0)),
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::String("hello".to_string())),
                Traverser::new(Value::Float(3.15)),
                Traverser::new(Value::Bool(true)),
                Traverser::new(Value::Null),
            ];

            let expected_values = [
                Value::Vertex(VertexId(0)),
                Value::Edge(EdgeId(0)),
                Value::Int(42),
                Value::String("hello".to_string()),
                Value::Float(3.15),
                Value::Bool(true),
                Value::Null,
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 7);
            for (i, t) in output.iter().enumerate() {
                if let Value::List(list) = &t.value {
                    assert_eq!(list[0], expected_values[i]);
                    assert_eq!(list[1], Value::Int(i as i64));
                } else {
                    panic!("Expected Value::List at index {}", i);
                }
            }
        }
    }

    mod index_step_value_type_tests {
        use super::*;

        #[test]
        fn works_with_vertex_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list[0], Value::Vertex(VertexId(0)));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn works_with_edge_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list[0], Value::Edge(EdgeId(0)));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn works_with_list_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();
            let inner_list = Value::List(vec![Value::Int(1), Value::Int(2)]);
            let input = vec![Traverser::new(inner_list.clone())];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list[0], inner_list);
                assert_eq!(list[1], Value::Int(0));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn works_with_map_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();
            let mut inner_map = std::collections::HashMap::new();
            inner_map.insert("key".to_string(), Value::String("value".to_string()));
            let map_value = Value::Map(inner_map.clone());
            let input = vec![Traverser::new(map_value.clone())];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(list) = &output[0].value {
                assert_eq!(list[0], map_value);
                assert_eq!(list[1], Value::Int(0));
            } else {
                panic!("Expected Value::List");
            }
        }
    }

    mod index_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_from_input_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();

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

            let step = IndexStep::new();

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

            let step = IndexStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod index_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod index_step_one_to_one_tests {
        use super::*;

        #[test]
        fn produces_exactly_one_output_per_input() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();

            let input: Vec<Traverser> = (0..10).map(|i| Traverser::new(Value::Int(i))).collect();

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 10);
        }

        #[test]
        fn no_filtering_occurs() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();

            // Mix of different value types - all should produce output
            let input = vec![
                Traverser::from_vertex(VertexId(0)),
                Traverser::from_edge(EdgeId(0)),
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::Null),
                Traverser::new(Value::List(vec![])),
                Traverser::new(Value::Map(std::collections::HashMap::new())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 6);

            // Verify indices are sequential
            for (i, t) in output.iter().enumerate() {
                if let Value::List(list) = &t.value {
                    assert_eq!(list[1], Value::Int(i as i64));
                } else {
                    panic!("Expected Value::List at index {}", i);
                }
            }
        }
    }

    mod index_step_counter_tests {
        use super::*;

        #[test]
        fn counter_starts_at_zero() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();
            let input = vec![Traverser::new(Value::Int(100))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            if let Value::List(list) = &output[0].value {
                assert_eq!(list[1], Value::Int(0));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn counter_handles_large_streams() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();

            // Create a large input stream
            let input: Vec<Traverser> = (0..1000).map(|i| Traverser::new(Value::Int(i))).collect();

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1000);

            // Verify first and last indices
            if let Value::List(list) = &output[0].value {
                assert_eq!(list[1], Value::Int(0));
            }
            if let Value::List(list) = &output[999].value {
                assert_eq!(list[1], Value::Int(999));
            }
        }

        #[test]
        fn new_apply_resets_counter() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = IndexStep::new();

            // First traversal
            let input1 = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];
            let output1: Vec<Traverser> = step.apply(&ctx, Box::new(input1.into_iter())).collect();

            // Second traversal with same step instance
            let input2 = vec![Traverser::new(Value::Int(3)), Traverser::new(Value::Int(4))];
            let output2: Vec<Traverser> = step.apply(&ctx, Box::new(input2.into_iter())).collect();

            // Both should start at index 0
            if let Value::List(list) = &output1[0].value {
                assert_eq!(list[1], Value::Int(0));
            }
            if let Value::List(list) = &output2[0].value {
                assert_eq!(list[1], Value::Int(0));
            }
        }
    }
}
