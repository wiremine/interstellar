use crate::traversal::{ExecutionContext, Traverser};
use crate::value::Value;

// -----------------------------------------------------------------------------
// PathStep - convert traverser path to Value::List
// -----------------------------------------------------------------------------

/// Transform step that converts the traverser's path to a Value::List.
///
/// This step replaces the traverser's value with a list containing all
/// elements from its path history. Each path element is converted to
/// its corresponding Value representation.
///
/// # Behavior
///
/// - Each input traverser produces exactly one output traverser
/// - The output value is a `Value::List` containing path elements
/// - Empty paths produce empty lists
/// - Path labels are preserved in the path structure (accessible via traverser.path)
/// - Vertices become `Value::Vertex(id)`, edges become `Value::Edge(id)`
/// - Property values remain as their original `Value` type
///
/// # Example
///
/// ```ignore
/// // Get the path of a multi-hop traversal
/// let paths = g.v().out().out().path().to_list();
/// // Each result is a Value::List of [vertex, vertex, vertex]
///
/// // With labeled steps
/// let paths = g.v().as("start").out().as("end").path().to_list();
/// // Path labels are preserved in traverser.path
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct PathStep;

impl PathStep {
    /// Create a new PathStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for PathStep {
    type Iter<'a>
        = impl Iterator<Item = crate::traversal::Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.map(|t| {
            let path_values = t.path.to_list();
            t.with_value(path_values)
        })
    }

    fn name(&self) -> &'static str {
        "path"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Convert the path to a list value
        let path_values = input.path.to_list();
        Box::new(std::iter::once(input.with_value(path_values)))
    }
}

// -----------------------------------------------------------------------------
// AsStep - label current position in path
// -----------------------------------------------------------------------------

/// Transform step that assigns a label to the current path position.
///
/// This step doesn't change the traverser's value, but adds a label
/// to the current position in the path history. This allows later steps
/// (like `select`) to refer back to this value.
///
/// # Behavior
///
/// - Pass-through step (value is unchanged)
/// - Adds the specified label to the current path element
/// - Multiple labels can be assigned to the same position
///
/// # Example
///
/// ```ignore
/// // Label the start vertex
/// g.v().as("start").out().as("end")
///
/// // Multiple labels at same position
/// g.v().as_("a").as_("b").select(&["a", "b"])  // Both return same vertex
/// ```
#[derive(Clone, Debug)]
pub struct AsStep {
    /// The label to assign to this path position.
    label: String,
}

impl AsStep {
    /// Create a new AsStep with the given label.
    ///
    /// # Arguments
    ///
    /// * `label` - The label to assign to this path position
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = AsStep::new("start");
    /// ```
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
        }
    }

    /// Get the label for this step.
    #[inline]
    pub fn label(&self) -> &str {
        &self.label
    }
}

impl crate::traversal::step::Step for AsStep {
    type Iter<'a>
        = impl Iterator<Item = crate::traversal::Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let label = self.label.clone();
        input.map(move |mut t| {
            // Label the current path position (don't add duplicate entry)
            t.label_path_position(&label);
            t
        })
    }

    fn name(&self) -> &'static str {
        "as"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        mut input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // Label the current path position
        input.label_path_position(&self.label);
        Box::new(std::iter::once(input))
    }
}

// -----------------------------------------------------------------------------
// SelectStep - retrieve labeled values from path
// -----------------------------------------------------------------------------

/// Step that retrieves labeled values from the traversal path.
///
/// The `select()` step looks up values in the path by their labels
/// (assigned via `as_()` steps) and returns them.
///
/// # Behavior
///
/// - **Single label**: Returns the value directly
/// - **Multiple labels**: Returns a `Value::Map` with label keys
/// - **Missing labels**: Traversers with no matching labels are filtered out
/// - **Multiple values per label**: Returns the *last* value for each label
///
/// # Example
///
/// ```ignore
/// // Single label - returns value directly
/// g.v().as_("x").out().select_one("x")  // Returns vertices
///
/// // Multiple labels - returns Map
/// g.v().as_("a").out().as_("b").select(&["a", "b"])
/// // Returns Map { "a" -> vertex1, "b" -> vertex2 }
///
/// // Missing label - filtered out
/// g.v().as_("x").select_one("y")  // Returns nothing (no "y" label)
/// ```
#[derive(Clone, Debug)]
pub struct SelectStep {
    /// Labels to select from the path.
    labels: Vec<String>,
}

impl SelectStep {
    /// Create a SelectStep for multiple labels.
    ///
    /// Returns a `Value::Map` with the labeled values.
    ///
    /// # Arguments
    ///
    /// * `labels` - The labels to select from the path
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = SelectStep::new(["start", "end"]);
    /// ```
    pub fn new(labels: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            labels: labels.into_iter().map(Into::into).collect(),
        }
    }

    /// Create a SelectStep for a single label.
    ///
    /// Returns the value directly (unwrapped).
    ///
    /// # Arguments
    ///
    /// * `label` - The label to select
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = SelectStep::single("start");
    /// ```
    pub fn single(label: impl Into<String>) -> Self {
        Self {
            labels: vec![label.into()],
        }
    }
}

impl crate::traversal::step::Step for SelectStep {
    type Iter<'a>
        = impl Iterator<Item = crate::traversal::Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let labels = self.labels.clone();
        input.filter_map(move |t| {
            if labels.len() == 1 {
                // Single label: return value directly (get last value for label)
                let val = t
                    .path
                    .get(&labels[0])
                    .and_then(|values| values.last().cloned())
                    .map(|pv| pv.to_value());

                val.map(|v| t.with_value(v))
            } else {
                // Multiple labels: return Map
                let mut map = std::collections::HashMap::new();
                let mut found_any = false;
                let mut missing_any = false;

                for label in &labels {
                    if let Some(values) = t.path.get(label) {
                        if let Some(last_val) = values.last() {
                            map.insert(label.clone(), last_val.to_value());
                            found_any = true;
                        } else {
                            missing_any = true;
                        }
                    } else {
                        missing_any = true;
                    }
                }

                // Gremlin behavior: if ANY selected label is missing, the traverser is filtered out.
                // UNLESS we are using optional selection (not implemented here yet).
                // The test `select_missing_label_filters_out` expects the result to be empty
                // if "nonexistent" label is missing.

                if !missing_any && found_any {
                    Some(t.with_value(Value::Map(map.into_iter().collect())))
                } else {
                    None
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "select"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        if self.labels.len() == 1 {
            // Single label: return value directly (get last value for label)
            let val = input
                .path
                .get(&self.labels[0])
                .and_then(|values| values.last().cloned())
                .map(|pv| pv.to_value());

            match val {
                Some(v) => Box::new(std::iter::once(input.with_value(v))),
                None => Box::new(std::iter::empty()),
            }
        } else {
            // Multiple labels: return Map
            let mut map = std::collections::HashMap::new();
            let mut found_any = false;
            let mut missing_any = false;

            for label in &self.labels {
                if let Some(values) = input.path.get(label) {
                    if let Some(last_val) = values.last() {
                        map.insert(label.clone(), last_val.to_value());
                        found_any = true;
                    } else {
                        missing_any = true;
                    }
                } else {
                    missing_any = true;
                }
            }

            if !missing_any && found_any {
                Box::new(std::iter::once(input.with_value(Value::Map(map.into_iter().collect()))))
            } else {
                Box::new(std::iter::empty())
            }
        }
    }
}

// -----------------------------------------------------------------------------
// SelectKeysStep - extract keys from a Map value
// -----------------------------------------------------------------------------

/// Transform step that extracts keys from Map values.
///
/// For each traverser with a Map value, this step emits the keys.
/// If the Map has a single entry, emits that key directly.
/// If the Map has multiple entries, emits a List of keys.
/// Non-Map values are filtered out.
///
/// # Example
///
/// ```ignore
/// // After unfold() on a grouped result, get just the label keys
/// g.v().group().by_label().unfold().select_keys().to_list()
/// // Returns: ["person", "software", ...]
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct SelectKeysStep;

impl SelectKeysStep {
    /// Create a new SelectKeysStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for SelectKeysStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.filter_map(|t| match &t.value {
            Value::Map(map) => {
                let keys: Vec<Value> = map.keys().map(|k| Value::String(k.clone())).collect();
                if keys.len() == 1 {
                    Some(t.with_value(keys.into_iter().next().unwrap()))
                } else {
                    Some(t.with_value(Value::List(keys)))
                }
            }
            _ => None, // Filter out non-Map values
        })
    }

    fn name(&self) -> &'static str {
        "select_keys"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        match &input.value {
            Value::Map(map) => {
                let keys: Vec<Value> = map.keys().map(|k| Value::String(k.clone())).collect();
                if keys.len() == 1 {
                    Box::new(std::iter::once(
                        input.with_value(keys.into_iter().next().unwrap()),
                    ))
                } else {
                    Box::new(std::iter::once(input.with_value(Value::List(keys))))
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }
}

// -----------------------------------------------------------------------------
// SelectValuesStep - extract values from a Map value
// -----------------------------------------------------------------------------

/// Transform step that extracts values from Map values.
///
/// For each traverser with a Map value, this step emits the values.
/// If the Map has a single entry, emits that value directly.
/// If the Map has multiple entries, emits a List of values.
/// Non-Map values are filtered out.
///
/// # Example
///
/// ```ignore
/// // After unfold() on a grouped result, get just the grouped values
/// g.v().group().by_label().unfold().select_values().to_list()
/// // Returns: [[v1, v2], [v3], ...]  (lists of vertices per label)
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct SelectValuesStep;

impl SelectValuesStep {
    /// Create a new SelectValuesStep.
    pub fn new() -> Self {
        Self
    }
}

impl crate::traversal::step::Step for SelectValuesStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.filter_map(|t| match &t.value {
            Value::Map(map) => {
                let values: Vec<Value> = map.values().cloned().collect();
                if values.len() == 1 {
                    Some(t.with_value(values.into_iter().next().unwrap()))
                } else {
                    Some(t.with_value(Value::List(values)))
                }
            }
            _ => None, // Filter out non-Map values
        })
    }

    fn name(&self) -> &'static str {
        "select_values"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        match &input.value {
            Value::Map(map) => {
                let values: Vec<Value> = map.values().cloned().collect();
                if values.len() == 1 {
                    Box::new(std::iter::once(
                        input.with_value(values.into_iter().next().unwrap()),
                    ))
                } else {
                    Box::new(std::iter::once(input.with_value(Value::List(values))))
                }
            }
            _ => Box::new(std::iter::empty()),
        }
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
        graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props.insert("age".to_string(), Value::Int(30));
            props
        });

        graph
    }

    mod path_step_construction {
        use super::*;

        #[test]
        fn test_new() {
            let step = PathStep::new();
            assert_eq!(step.name(), "path");
        }
    }

    mod path_step_empty_path_tests {
        use super::*;

        #[test]
        fn empty_path_returns_empty_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = PathStep::new();
            let input = vec![Traverser::new(Value::Int(42))]; // Empty path

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(elements) = &output[0].value {
                assert!(elements.is_empty());
            } else {
                panic!("Expected Value::List");
            }
        }
    }

    mod path_step_with_elements_tests {
        use super::*;
        use crate::traversal::PathValue;

        #[test]
        fn path_with_vertices_and_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = PathStep::new();

            // Traverser::from_vertex creates a new Traverser with an empty path, but Traverser::new()
            // does NOT automatically add the initial value to the path.
            // The tests manually push items to the path to simulate history.
            // However, the PathStep logic just calls `t.path.to_list()`.

            // In `path_with_vertices_and_edges`:
            // traverser created from Vertex(0). Path is empty.
            // push Edge(0). Path: [Edge(0)]
            // push Vertex(1). Path: [Edge(0), Vertex(1)]
            // Result length: 2. Expected: 3.

            // The issue is that the tests expect the *initial* value of the traverser to be part of the path,
            // or they assume `from_vertex` adds it, or they assume `PathStep` includes the current value?

            // Let's look at `PathStep` implementation:
            // Box::new(input.map(|t| {
            //    let path_values = t.path.to_list();
            //    t.with_value(path_values)
            // }))

            // It ONLY converts the `path` object to a list. It does not include the current `value` unless it's already in the path.
            // In a real traversal, steps like `V()` or `out()` would add items to the path.

            // In these unit tests, we are manually constructing the path.
            // If we want the initial vertex to be in the path, we must add it.

            let mut traverser = Traverser::from_vertex(VertexId(0));
            // Add the initial vertex to the path manually as if it was visited
            traverser.path.push(PathValue::Vertex(VertexId(0)), &[]);
            traverser.path.push(PathValue::Edge(EdgeId(0)), &[]);
            traverser.path.push(PathValue::Vertex(VertexId(1)), &[]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(elements) = &output[0].value {
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0], Value::Vertex(VertexId(0)));
                assert_eq!(elements[1], Value::Edge(EdgeId(0)));
                assert_eq!(elements[2], Value::Vertex(VertexId(1)));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn path_with_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = PathStep::new();

            let mut traverser = Traverser::new(Value::Int(1));
            // Add initial value to path
            traverser.path.push(PathValue::Property(Value::Int(1)), &[]);
            traverser
                .path
                .push(PathValue::Property(Value::String("step2".to_string())), &[]);
            traverser
                .path
                .push(PathValue::Property(Value::Bool(true)), &[]);

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(elements) = &output[0].value {
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0], Value::Int(1));
                assert_eq!(elements[1], Value::String("step2".to_string()));
                assert_eq!(elements[2], Value::Bool(true));
            } else {
                panic!("Expected Value::List");
            }
        }
    }

    mod path_step_with_labels_tests {
        use super::*;
        use crate::traversal::PathValue;

        #[test]
        fn path_preserves_labels_in_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = PathStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(1));
            traverser
                .path
                .push_labeled(PathValue::Vertex(VertexId(0)), "start");
            traverser
                .path
                .push_labeled(PathValue::Vertex(VertexId(1)), "end");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            // The path should still have its labels
            assert!(output[0].path.has_label("start"));
            assert!(output[0].path.has_label("end"));
            // And the value should be a list
            if let Value::List(elements) = &output[0].value {
                assert_eq!(elements.len(), 2);
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn path_with_multiple_labels_on_same_element() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = PathStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.path.push(
                PathValue::Vertex(VertexId(0)),
                &["a".to_string(), "b".to_string()],
            );

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("a"));
            assert!(output[0].path.has_label("b"));
        }
    }

    mod path_step_metadata_tests {
        use super::*;
        use crate::traversal::PathValue;

        #[test]
        fn preserves_path_structure() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = PathStep::new();

            let mut traverser = Traverser::new(Value::Int(42));
            traverser
                .path
                .push_labeled(PathValue::Vertex(VertexId(0)), "start");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            // Path should still be intact
            assert!(output[0].path.has_label("start"));
            assert_eq!(output[0].path.len(), 1);
        }

        #[test]
        fn preserves_loops_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = PathStep::new();

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

            let step = PathStep::new();

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.bulk = 10;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod path_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = PathStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod path_step_multiple_traversers_tests {
        use super::*;
        use crate::traversal::PathValue;

        #[test]
        fn handles_multiple_traversers() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(snapshot.storage(), snapshot.interner());

            let step = PathStep::new();

            // Traverser 1
            let mut t1 = Traverser::from_vertex(VertexId(0));
            t1.path.push_labeled(PathValue::Vertex(VertexId(0)), "a");

            // Traverser 2
            let mut t2 = Traverser::from_vertex(VertexId(1));
            t2.path.push_labeled(PathValue::Vertex(VertexId(1)), "b");

            let input = vec![t1, t2];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);

            // Verify first traverser output
            if let Value::List(elements) = &output[0].value {
                assert_eq!(elements[0], Value::Vertex(VertexId(0)));
            } else {
                panic!("Expected Value::List");
            }

            // Verify second traverser output
            if let Value::List(elements) = &output[1].value {
                assert_eq!(elements[0], Value::Vertex(VertexId(1)));
            } else {
                panic!("Expected Value::List");
            }
        }
    }
}
