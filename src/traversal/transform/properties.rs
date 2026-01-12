use crate::traversal::{ExecutionContext, Traverser};
use crate::value::Value;

// -----------------------------------------------------------------------------
// PropertiesStep - extract properties map
// -----------------------------------------------------------------------------

/// Transform step that extracts property objects from elements.
///
/// This step converts elements into property objects. It's similar to `ValuesStep`,
/// but instead of extracting just the value, it can extract property metadata
/// or the property as a structured object.
///
/// **Note**: In this implementation, since we don't have first-class Property objects
/// in the Value enum yet, this step behaves like `valueMap` but returns individual
/// property entries as maps `{key: "name", value: "alice"}`.
///
/// # Behavior
///
/// - Expands each input element into multiple property objects
/// - Each output is a `Value::Map` with "key" and "value" entries
/// - Can filter by property keys
///
/// # Example
///
/// ```ignore
/// // Get properties of a vertex
/// g.v().properties()
/// // Returns maps like {key: "name", value: "alice"}, {key: "age", value: 30}
/// ```
#[derive(Clone, Debug)]
pub struct PropertiesStep {
    /// Property keys to extract. None means all properties.
    keys: Option<Vec<String>>,
}

impl Default for PropertiesStep {
    fn default() -> Self {
        Self::new()
    }
}

impl PropertiesStep {
    /// Create a PropertiesStep that extracts all properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = PropertiesStep::new();
    /// ```
    pub fn new() -> Self {
        Self { keys: None }
    }

    /// Create a PropertiesStep for specific property keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - The property keys to extract
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = PropertiesStep::with_keys(vec!["name".to_string(), "age".to_string()]);
    /// ```
    pub fn with_keys(keys: Vec<String>) -> Self {
        Self { keys: Some(keys) }
    }

    /// Create a PropertiesStep from an iterator of keys.
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
            keys: Some(keys.into_iter().map(Into::into).collect()),
        }
    }

    /// Create a property value map with "key" and "value" entries.
    #[inline]
    pub(crate) fn make_property_map(key: String, value: Value) -> Value {
        let mut map = std::collections::HashMap::new();
        map.insert("key".to_string(), Value::String(key));
        map.insert("value".to_string(), value);
        Value::Map(map)
    }

    /// Expand a traverser by extracting property objects.
    ///
    /// Returns an iterator of new traversers, one for each property found.
    /// Each property is represented as a `Value::Map` with "key" and "value" entries.
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
                    .snapshot()
                    .storage()
                    .get_vertex(*id)
                    .map(|vertex| {
                        match &keys {
                            None => {
                                // Return all properties
                                vertex
                                    .properties
                                    .iter()
                                    .map(|(k, v)| Self::make_property_map(k.clone(), v.clone()))
                                    .collect()
                            }
                            Some(key_list) => {
                                // Return only specified properties
                                key_list
                                    .iter()
                                    .filter_map(|key| {
                                        vertex.properties.get(key).map(|v| {
                                            Self::make_property_map(key.clone(), v.clone())
                                        })
                                    })
                                    .collect()
                            }
                        }
                    })
                    .unwrap_or_default();

                // Create new traversers for each property
                props
                    .into_iter()
                    .map(move |val| traverser.split(val))
                    .collect::<Vec<_>>()
                    .into_iter()
            }
            Value::Edge(id) => {
                // Get edge properties
                let props: Vec<Value> = ctx
                    .snapshot()
                    .storage()
                    .get_edge(*id)
                    .map(|edge| {
                        match &keys {
                            None => {
                                // Return all properties
                                edge.properties
                                    .iter()
                                    .map(|(k, v)| Self::make_property_map(k.clone(), v.clone()))
                                    .collect()
                            }
                            Some(key_list) => {
                                // Return only specified properties
                                key_list
                                    .iter()
                                    .filter_map(|key| {
                                        edge.properties.get(key).map(|v| {
                                            Self::make_property_map(key.clone(), v.clone())
                                        })
                                    })
                                    .collect()
                            }
                        }
                    })
                    .unwrap_or_default();

                // Create new traversers for each property
                props
                    .into_iter()
                    .map(move |val| traverser.split(val))
                    .collect::<Vec<_>>()
                    .into_iter()
            }
            _ => {
                // Non-elements have no properties
                Vec::new().into_iter()
            }
        }
    }
}

impl crate::traversal::step::AnyStep for PropertiesStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.flat_map(move |traverser| self.expand(ctx, traverser)))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "properties"
    }
}

// -----------------------------------------------------------------------------
// ValueMapStep - extract property map
// -----------------------------------------------------------------------------

/// Transform step that extracts a map of properties from elements.
///
/// This step converts each element into a `Value::Map` containing its properties.
///
/// # Behavior
///
/// - Each input element produces exactly one output `Value::Map`
/// - Property values are wrapped in lists (as per Gremlin standard for multi-properties)
/// - Can filter to specific keys
/// - Can optionally include tokens (id, label)
///
/// # Example
///
/// ```ignore
/// // Get all properties as a map
/// g.v().value_map()
/// // Returns: [{"name": ["Alice"], "age": [30]}]
///
/// // Get specific properties with tokens
/// g.v().value_map(&["name", "age"]).with_tokens()
/// // Returns: [{"id": 0, "label": "person", "name": ["Alice"], "age": [30]}]
/// ```
#[derive(Clone, Debug)]
pub struct ValueMapStep {
    /// Property keys to include. None means all properties.
    keys: Option<Vec<String>>,
    /// Whether to include id and label tokens.
    include_tokens: bool,
}

impl Default for ValueMapStep {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueMapStep {
    /// Create a ValueMapStep that extracts all properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = ValueMapStep::new();
    /// ```
    pub fn new() -> Self {
        Self {
            keys: None,
            include_tokens: false,
        }
    }

    /// Create a ValueMapStep for specific property keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - The property keys to extract
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = ValueMapStep::with_keys(vec!["name".to_string(), "age".to_string()]);
    /// ```
    pub fn with_keys(keys: Vec<String>) -> Self {
        Self {
            keys: Some(keys),
            include_tokens: false,
        }
    }

    /// Create a ValueMapStep from an iterator of keys.
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
            keys: Some(keys.into_iter().map(Into::into).collect()),
            include_tokens: false,
        }
    }

    /// Enable including id and label tokens in the output.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = ValueMapStep::new().with_tokens();
    /// ```
    pub fn with_tokens(mut self) -> Self {
        self.include_tokens = true;
        self
    }

    /// Transform a traverser's value into a property map.
    fn transform(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        let mut map = std::collections::HashMap::new();

        match &traverser.value {
            Value::Vertex(id) => {
                if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                    // Optionally include id and label tokens (NOT wrapped in lists)
                    if self.include_tokens {
                        map.insert("id".to_string(), Value::Int(id.0 as i64));
                        map.insert("label".to_string(), Value::String(vertex.label.clone()));
                    }

                    // Add properties (wrapped in lists)
                    match &self.keys {
                        None => {
                            // Include all properties
                            for (key, value) in &vertex.properties {
                                map.insert(key.clone(), Value::List(vec![value.clone()]));
                            }
                        }
                        Some(key_list) => {
                            // Include only specified properties
                            for key in key_list {
                                if let Some(value) = vertex.properties.get(key) {
                                    map.insert(key.clone(), Value::List(vec![value.clone()]));
                                }
                            }
                        }
                    }
                }
            }
            Value::Edge(id) => {
                if let Some(edge) = ctx.snapshot().storage().get_edge(*id) {
                    // Optionally include id and label tokens (NOT wrapped in lists)
                    if self.include_tokens {
                        map.insert("id".to_string(), Value::Int(id.0 as i64));
                        map.insert("label".to_string(), Value::String(edge.label.clone()));
                    }

                    // Add properties (wrapped in lists)
                    match &self.keys {
                        None => {
                            // Include all properties
                            for (key, value) in &edge.properties {
                                map.insert(key.clone(), Value::List(vec![value.clone()]));
                            }
                        }
                        Some(key_list) => {
                            // Include only specified properties
                            for key in key_list {
                                if let Some(value) = edge.properties.get(key) {
                                    map.insert(key.clone(), Value::List(vec![value.clone()]));
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                // Non-elements return empty map (or just throw error? Gremlin returns empty)
            }
        }

        Value::Map(map)
    }
}

impl crate::traversal::step::AnyStep for ValueMapStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(move |t| {
            let result = self.transform(ctx, &t);
            t.with_value(result)
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "valueMap"
    }
}

// -----------------------------------------------------------------------------
// ElementMapStep - extract complete element map
// -----------------------------------------------------------------------------

/// Transform step that extracts a map of element properties including ID, label, and incident vertices.
///
/// This step is similar to `valueMap` but always includes:
/// - ID and Label
/// - For edges: IN and OUT vertex references
/// - Properties are NOT wrapped in lists (single cardinality assumed)
///
/// # Behavior
///
/// - Each input element produces exactly one output `Value::Map`
/// - Properties are returned directly (not wrapped in lists)
/// - Edges include "IN" and "OUT" keys pointing to vertices
///
/// # Example
///
/// ```ignore
/// // Get complete element map
/// g.v().element_map()
/// // Returns: [{"id": 1, "label": "person", "name": "Alice", "age": 30}]
///
/// // Get element map for specific keys
/// g.e().element_map(&["weight"])
/// // Returns: [{"id": 0, "label": "created", "weight": 0.4, "IN": {...}, "OUT": {...}}]
/// ```
#[derive(Clone, Debug)]
pub struct ElementMapStep {
    /// Property keys to include. None means all properties.
    keys: Option<Vec<String>>,
}

impl Default for ElementMapStep {
    fn default() -> Self {
        Self::new()
    }
}

impl ElementMapStep {
    /// Create an ElementMapStep that includes all properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = ElementMapStep::new();
    /// ```
    pub fn new() -> Self {
        Self { keys: None }
    }

    /// Create an ElementMapStep for specific property keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - The property keys to include
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = ElementMapStep::with_keys(vec!["name".to_string()]);
    /// ```
    pub fn with_keys(keys: Vec<String>) -> Self {
        Self { keys: Some(keys) }
    }

    /// Create an ElementMapStep from an iterator of keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - Iterator of property keys to include
    pub fn from_keys<I, S>(keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            keys: Some(keys.into_iter().map(Into::into).collect()),
        }
    }

    /// Transform a traverser's value into an element map.
    fn transform(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        let mut map = std::collections::HashMap::new();

        match &traverser.value {
            Value::Vertex(id) => {
                if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                    // Always include id and label
                    map.insert("id".to_string(), Value::Int(id.0 as i64));
                    map.insert("label".to_string(), Value::String(vertex.label.clone()));

                    // Add properties (NOT wrapped in lists)
                    match &self.keys {
                        None => {
                            // Include all properties
                            for (key, value) in &vertex.properties {
                                map.insert(key.clone(), value.clone());
                            }
                        }
                        Some(key_list) => {
                            // Include only specified properties
                            for key in key_list {
                                if let Some(value) = vertex.properties.get(key) {
                                    map.insert(key.clone(), value.clone());
                                }
                            }
                        }
                    }
                }
            }
            Value::Edge(id) => {
                if let Some(edge) = ctx.snapshot().storage().get_edge(*id) {
                    // Always include id and label
                    map.insert("id".to_string(), Value::Int(id.0 as i64));
                    map.insert("label".to_string(), Value::String(edge.label.clone()));

                    // Include IN vertex reference (the destination vertex)
                    let in_ref = self.make_vertex_reference(ctx, edge.dst);
                    map.insert("IN".to_string(), in_ref);

                    // Include OUT vertex reference (the source vertex)
                    let out_ref = self.make_vertex_reference(ctx, edge.src);
                    map.insert("OUT".to_string(), out_ref);

                    // Add properties (NOT wrapped in lists)
                    match &self.keys {
                        None => {
                            // Include all properties
                            for (key, value) in &edge.properties {
                                map.insert(key.clone(), value.clone());
                            }
                        }
                        Some(key_list) => {
                            // Include only specified properties
                            for key in key_list {
                                if let Some(value) = edge.properties.get(key) {
                                    map.insert(key.clone(), value.clone());
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                // Non-elements return empty map
            }
        }

        Value::Map(map)
    }

    /// Helper to create a small map reference for a vertex {id, label}
    fn make_vertex_reference(&self, ctx: &ExecutionContext, id: crate::value::VertexId) -> Value {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), Value::Int(id.0 as i64));

        if let Some(vertex) = ctx.snapshot().storage().get_vertex(id) {
            map.insert("label".to_string(), Value::String(vertex.label.clone()));
        }

        Value::Map(map)
    }
}

impl crate::traversal::step::AnyStep for ElementMapStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(move |t| {
            let result = self.transform(ctx, &t);
            t.with_value(result)
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "elementMap"
    }
}

// -----------------------------------------------------------------------------
// PropertyMapStep - extract properties as map of property objects
// -----------------------------------------------------------------------------

/// Transform step that extracts a map of property objects from elements.
///
/// This step is similar to `valueMap`, but instead of returning just the values,
/// it returns property objects with "key" and "value" entries wrapped in lists.
///
/// # Difference from valueMap
///
/// - `valueMap()`: Returns `{name: ["Alice"], age: [30]}` (just values in lists)
/// - `propertyMap()`: Returns `{name: [{key: "name", value: "Alice"}], age: [{key: "age", value: 30}]}` (property objects in lists)
///
/// # Behavior
///
/// - Each input element produces exactly one output `Value::Map`
/// - Keys are property names
/// - Values are lists of property objects (for multi-property compatibility)
/// - Each property object is a map with "key" and "value" entries
/// - Non-elements return empty map
///
/// # Example
///
/// ```ignore
/// // Get all properties as property objects
/// let props = g.v().property_map().to_list();
/// // Returns: [{"name": [{"key": "name", "value": "Alice"}], "age": [{"key": "age", "value": 30}]}]
///
/// // Get specific properties only
/// let props = g.v().property_map_keys(&["name"]).to_list();
/// // Returns: [{"name": [{"key": "name", "value": "Alice"}]}]
/// ```
#[derive(Clone, Debug)]
pub struct PropertyMapStep {
    /// Property keys to include. None means all properties.
    keys: Option<Vec<String>>,
}

impl Default for PropertyMapStep {
    fn default() -> Self {
        Self::new()
    }
}

impl PropertyMapStep {
    /// Create a PropertyMapStep that extracts all properties.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = PropertyMapStep::new();
    /// ```
    pub fn new() -> Self {
        Self { keys: None }
    }

    /// Create a PropertyMapStep for specific property keys.
    ///
    /// # Arguments
    ///
    /// * `keys` - The property keys to extract
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = PropertyMapStep::with_keys(vec!["name".to_string(), "age".to_string()]);
    /// ```
    pub fn with_keys(keys: Vec<String>) -> Self {
        Self { keys: Some(keys) }
    }

    /// Create a PropertyMapStep from an iterator of keys.
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
            keys: Some(keys.into_iter().map(Into::into).collect()),
        }
    }

    /// Transform a traverser's value into a property map.
    fn transform(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Value {
        let mut map = std::collections::HashMap::new();

        match &traverser.value {
            Value::Vertex(id) => {
                if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                    match &self.keys {
                        None => {
                            // Include all properties
                            for (key, value) in &vertex.properties {
                                let prop_obj =
                                    PropertiesStep::make_property_map(key.clone(), value.clone());
                                map.insert(key.clone(), Value::List(vec![prop_obj]));
                            }
                        }
                        Some(key_list) => {
                            // Include only specified properties
                            for key in key_list {
                                if let Some(value) = vertex.properties.get(key) {
                                    let prop_obj = PropertiesStep::make_property_map(
                                        key.clone(),
                                        value.clone(),
                                    );
                                    map.insert(key.clone(), Value::List(vec![prop_obj]));
                                }
                            }
                        }
                    }
                }
            }
            Value::Edge(id) => {
                if let Some(edge) = ctx.snapshot().storage().get_edge(*id) {
                    match &self.keys {
                        None => {
                            // Include all properties
                            for (key, value) in &edge.properties {
                                let prop_obj =
                                    PropertiesStep::make_property_map(key.clone(), value.clone());
                                map.insert(key.clone(), Value::List(vec![prop_obj]));
                            }
                        }
                        Some(key_list) => {
                            // Include only specified properties
                            for key in key_list {
                                if let Some(value) = edge.properties.get(key) {
                                    let prop_obj = PropertiesStep::make_property_map(
                                        key.clone(),
                                        value.clone(),
                                    );
                                    map.insert(key.clone(), Value::List(vec![prop_obj]));
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                // Non-elements return empty map
            }
        }

        Value::Map(map)
    }
}

impl crate::traversal::step::AnyStep for PropertyMapStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(move |t| {
            let result = self.transform(ctx, &t);
            t.with_value(result)
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "propertyMap"
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

    mod properties_step_vertex_tests {
        use super::*;

        #[test]
        fn extracts_all_properties() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Vertex 0 has "name" and "age"
            assert_eq!(output.len(), 2);

            let mut keys = Vec::new();
            for t in output {
                if let Value::Map(map) = t.value {
                    if let Some(Value::String(k)) = map.get("key") {
                        keys.push(k.clone());
                    }
                }
            }
            keys.sort();
            assert_eq!(keys, vec!["age", "name"]);
        }

        #[test]
        fn extracts_specific_properties() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec!["name".to_string()]);
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.get("key"), Some(&Value::String("name".to_string())));
                assert_eq!(map.get("value"), Some(&Value::String("Alice".to_string())));
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    mod properties_step_edge_tests {
        use super::*;

        #[test]
        fn extracts_edge_properties() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Edge 0 has "weight"
            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.get("key"), Some(&Value::String("weight".to_string())));
                assert_eq!(map.get("value"), Some(&Value::Float(1.0)));
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    mod properties_step_non_element_tests {
        use super::*;

        #[test]
        fn ignores_non_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod properties_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_for_each_property() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser.extend_path_labeled("start");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Should produce multiple outputs, all sharing the same path
            assert_eq!(output.len(), 2);
            assert!(output[0].path.has_label("start"));
            assert!(output[1].path.has_label("start"));
        }
    }

    mod properties_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod value_map_step_construction {
        use super::*;

        #[test]
        fn test_new() {
            let step = ValueMapStep::new();
            assert_eq!(step.name(), "valueMap");
        }
    }

    mod value_map_step_vertex_transform {
        use super::*;

        #[test]
        fn extracts_all_properties_as_map() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValueMapStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.len(), 2);
                assert_eq!(
                    map.get("name"),
                    Some(&Value::List(vec![Value::String("Alice".to_string())]))
                );
                assert_eq!(map.get("age"), Some(&Value::List(vec![Value::Int(30)])));
            } else {
                panic!("Expected Value::Map");
            }
        }

        #[test]
        fn includes_tokens_when_requested() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValueMapStep::new().with_tokens();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.get("id"), Some(&Value::Int(0)));
                assert_eq!(map.get("label"), Some(&Value::String("person".to_string())));
                assert!(map.contains_key("name"));
                assert!(map.contains_key("age"));
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    mod value_map_step_edge_transform {
        use super::*;

        #[test]
        fn extracts_edge_properties_as_map() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValueMapStep::new();
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(
                    map.get("weight"),
                    Some(&Value::List(vec![Value::Float(1.0)]))
                );
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    mod value_map_step_non_element {
        use super::*;

        #[test]
        fn returns_empty_map_for_non_element() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValueMapStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert!(map.is_empty());
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    mod element_map_step_construction {
        use super::*;

        #[test]
        fn test_new() {
            let step = ElementMapStep::new();
            assert_eq!(step.name(), "elementMap");
        }
    }

    mod element_map_step_vertex_transform {
        use super::*;

        #[test]
        fn extracts_vertex_element_map() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ElementMapStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.get("id"), Some(&Value::Int(0)));
                assert_eq!(map.get("label"), Some(&Value::String("person".to_string())));
                assert_eq!(map.get("name"), Some(&Value::String("Alice".to_string()))); // Not wrapped in list
                assert_eq!(map.get("age"), Some(&Value::Int(30)));
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    mod element_map_step_edge_transform {
        use super::*;

        #[test]
        fn extracts_edge_element_map_with_refs() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ElementMapStep::new();
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.get("id"), Some(&Value::Int(0)));
                assert_eq!(
                    map.get("label"),
                    Some(&Value::String("created".to_string()))
                );
                assert_eq!(map.get("weight"), Some(&Value::Float(1.0)));

                // Check refs
                if let Some(Value::Map(in_ref)) = map.get("IN") {
                    assert_eq!(in_ref.get("id"), Some(&Value::Int(1)));
                    assert_eq!(
                        in_ref.get("label"),
                        Some(&Value::String("software".to_string()))
                    );
                } else {
                    panic!("Expected IN ref map");
                }

                if let Some(Value::Map(out_ref)) = map.get("OUT") {
                    assert_eq!(out_ref.get("id"), Some(&Value::Int(0)));
                    assert_eq!(
                        out_ref.get("label"),
                        Some(&Value::String("person".to_string()))
                    );
                } else {
                    panic!("Expected OUT ref map");
                }
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    mod element_map_step_non_element {
        use super::*;

        #[test]
        fn returns_empty_map_for_non_element() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ElementMapStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert!(map.is_empty());
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    // =========================================================================
    // PropertyMapStep Tests
    // =========================================================================

    mod property_map_step_construction {
        use super::*;

        #[test]
        fn new_creates_step_with_no_keys() {
            let step = PropertyMapStep::new();
            assert_eq!(step.name(), "propertyMap");
        }

        #[test]
        fn default_creates_step_with_no_keys() {
            let step = PropertyMapStep::default();
            assert_eq!(step.name(), "propertyMap");
        }

        #[test]
        fn with_keys_creates_step_with_specified_keys() {
            let step = PropertyMapStep::with_keys(vec!["name".to_string(), "age".to_string()]);
            assert_eq!(step.name(), "propertyMap");
        }

        #[test]
        fn from_keys_creates_step_from_iterator() {
            let step = PropertyMapStep::from_keys(["name", "age"]);
            assert_eq!(step.name(), "propertyMap");
        }

        #[test]
        fn clone_works() {
            let step = PropertyMapStep::with_keys(vec!["name".to_string()]);
            let cloned = step.clone();
            assert_eq!(cloned.name(), "propertyMap");
        }

        #[test]
        fn clone_box_works() {
            let step = PropertyMapStep::new();
            let boxed = step.clone_box();
            assert_eq!(boxed.name(), "propertyMap");
        }
    }

    mod property_map_step_vertex_transform {
        use super::*;

        #[test]
        fn extracts_all_vertex_properties_as_property_objects() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                // Check "name" property
                let name_list = map.get("name").expect("name key should exist");
                if let Value::List(list) = name_list {
                    assert_eq!(list.len(), 1);
                    if let Value::Map(prop_obj) = &list[0] {
                        assert_eq!(
                            prop_obj.get("key"),
                            Some(&Value::String("name".to_string()))
                        );
                        assert_eq!(
                            prop_obj.get("value"),
                            Some(&Value::String("Alice".to_string()))
                        );
                    } else {
                        panic!("Expected property object to be a Map");
                    }
                } else {
                    panic!("Expected name value to be a List");
                }

                // Check "age" property
                let age_list = map.get("age").expect("age key should exist");
                if let Value::List(list) = age_list {
                    assert_eq!(list.len(), 1);
                    if let Value::Map(prop_obj) = &list[0] {
                        assert_eq!(prop_obj.get("key"), Some(&Value::String("age".to_string())));
                        assert_eq!(prop_obj.get("value"), Some(&Value::Int(30)));
                    } else {
                        panic!("Expected property object to be a Map");
                    }
                } else {
                    panic!("Expected age value to be a List");
                }
            } else {
                panic!("Expected Value::Map");
            }
        }

        #[test]
        fn extracts_specific_vertex_properties() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::with_keys(vec!["name".to_string()]);
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                // Only "name" should be present
                assert!(map.contains_key("name"));
                assert!(!map.contains_key("age"));
                assert_eq!(map.len(), 1);
            } else {
                panic!("Expected Value::Map");
            }
        }

        #[test]
        fn missing_keys_are_omitted() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::with_keys(vec!["name".to_string(), "missing".to_string()]);
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert!(map.contains_key("name"));
                assert!(!map.contains_key("missing"));
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    mod property_map_step_edge_transform {
        use super::*;

        #[test]
        fn extracts_edge_properties_as_property_objects() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::new();
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                let weight_list = map.get("weight").expect("weight key should exist");
                if let Value::List(list) = weight_list {
                    assert_eq!(list.len(), 1);
                    if let Value::Map(prop_obj) = &list[0] {
                        assert_eq!(
                            prop_obj.get("key"),
                            Some(&Value::String("weight".to_string()))
                        );
                        assert_eq!(prop_obj.get("value"), Some(&Value::Float(1.0)));
                    } else {
                        panic!("Expected property object to be a Map");
                    }
                } else {
                    panic!("Expected weight value to be a List");
                }
            } else {
                panic!("Expected Value::Map");
            }
        }

        #[test]
        fn extracts_specific_edge_properties() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::with_keys(vec!["weight".to_string()]);
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert!(map.contains_key("weight"));
                assert_eq!(map.len(), 1);
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    mod property_map_step_non_element {
        use super::*;

        #[test]
        fn returns_empty_map_for_non_element() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert!(map.is_empty());
            } else {
                panic!("Expected Value::Map");
            }
        }

        #[test]
        fn returns_empty_map_for_string() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::new();
            let input = vec![Traverser::new(Value::String("test".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert!(map.is_empty());
            } else {
                panic!("Expected Value::Map");
            }
        }
    }

    mod property_map_step_metadata {
        use super::*;

        #[test]
        fn preserves_path() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::new();
            let mut t = Traverser::from_vertex(VertexId(0));
            t.path
                .push_labeled(crate::traversal::PathValue::Vertex(VertexId(99)), "start");

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::once(t))).collect();

            assert_eq!(output.len(), 1);
            assert!(output[0].path.has_label("start"));
        }

        #[test]
        fn preserves_loops() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::new();
            let mut t = Traverser::from_vertex(VertexId(0));
            t.loops = 5;

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::once(t))).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].loops, 5);
        }

        #[test]
        fn preserves_bulk() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::new();
            let mut t = Traverser::from_vertex(VertexId(0));
            t.bulk = 10;

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(std::iter::once(t))).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 10);
        }
    }

    mod property_map_step_missing_element {
        use super::*;

        #[test]
        fn returns_empty_map_for_missing_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::new();
            let input = vec![Traverser::from_vertex(VertexId(999))]; // Non-existent vertex

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert!(map.is_empty());
            } else {
                panic!("Expected Value::Map");
            }
        }

        #[test]
        fn returns_empty_map_for_missing_edge() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertyMapStep::new();
            let input = vec![Traverser::from_edge(EdgeId(999))]; // Non-existent edge

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert!(map.is_empty());
            } else {
                panic!("Expected Value::Map");
            }
        }
    }
}
