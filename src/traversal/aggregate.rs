//! Aggregation steps for grouping and counting traversers.
//!
//! This module provides steps for aggregating traversers into groups:
//! - `GroupStep` - groups traversers by key and collects values
//! - `GroupCountStep` - counts traversers by group key
//!
//! Both steps are "barrier" steps that collect all input before producing output.

use std::collections::HashMap;
use std::marker::PhantomData;

use crate::traversal::step::{execute_traversal_from, AnyStep};
use crate::traversal::{ExecutionContext, Traversal, Traverser};
use crate::value::Value;

/// Specification for how to extract the grouping key from a traverser.
///
/// Used by `GroupStep` and `GroupCountStep` to determine which group
/// a traverser belongs to.
#[derive(Clone, Debug)]
pub enum GroupKey {
    /// Group by element label (vertex or edge label).
    Label,

    /// Group by a property value.
    Property(String),

    /// Group by the result of a traversal.
    Traversal(Box<Traversal<crate::Value, crate::Value>>),
}

impl GroupKey {
    /// Create a GroupKey that groups by label.
    pub fn by_label() -> Self {
        GroupKey::Label
    }

    /// Create a GroupKey that groups by a property.
    pub fn by_property(key: impl Into<String>) -> Self {
        GroupKey::Property(key.into())
    }

    /// Create a GroupKey that groups by a traversal result.
    pub fn by_traversal(traversal: Traversal<crate::Value, crate::Value>) -> Self {
        GroupKey::Traversal(Box::new(traversal))
    }
}

/// Specification for how to collect values within a group.
///
/// Used by `GroupStep` to determine what value to store for each
/// traverser in a group.
#[derive(Clone, Debug)]
pub enum GroupValue {
    /// Use the traverser's current value directly (identity).
    Identity,

    /// Extract a property value from the traverser.
    Property(String),

    /// Apply a traversal to compute the group value.
    Traversal(Box<Traversal<crate::Value, crate::Value>>),
}

impl GroupValue {
    /// Create a GroupValue that uses the identity (current value).
    pub fn identity() -> Self {
        GroupValue::Identity
    }

    /// Create a GroupValue that extracts a property.
    pub fn by_property(key: impl Into<String>) -> Self {
        GroupValue::Property(key.into())
    }

    /// Create a GroupValue that applies a traversal.
    pub fn by_traversal(traversal: Traversal<crate::Value, crate::Value>) -> Self {
        GroupValue::Traversal(Box::new(traversal))
    }
}

// -----------------------------------------------------------------------------
// GroupStep - barrier step that groups traversers
// -----------------------------------------------------------------------------

/// Barrier step that groups traversers by a key and collects values.
///
/// This is a **barrier step** - it collects ALL input before producing grouped output.
/// The result is a single traverser containing a `Value::Map` where:
/// - Keys are the grouping keys (as Values)
/// - Values are lists of collected values for each group
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().group().by(label).by(values("name"))  // Group by label, collect names
/// g.V().group().by("age")  // Group by age property, collect vertices (identity)
/// ```
///
/// # Example
///
/// ```ignore
/// // Group vertices by label
/// let groups = g.v()
///     .group().by_label().by_value().build()
///     .next();
/// // Returns: Map { "person" -> [vertex1, vertex2], "software" -> [vertex3] }
///
/// // Group by property
/// let groups = g.v().has_label("person")
///     .group().by_key("age").by_value_key("name").build()
///     .next();
/// // Returns: Map { 29 -> ["Alice", "Bob"], 30 -> ["Charlie"] }
/// ```
#[derive(Clone)]
pub struct GroupStep {
    key_selector: GroupKey,
    value_collector: GroupValue,
}

impl GroupStep {
    /// Create a new GroupStep with default selectors (identity key, identity value).
    pub fn new() -> Self {
        Self {
            key_selector: GroupKey::Label,
            value_collector: GroupValue::Identity,
        }
    }

    /// Create a GroupStep with custom selectors.
    pub fn with_selectors(key_selector: GroupKey, value_collector: GroupValue) -> Self {
        Self {
            key_selector,
            value_collector,
        }
    }

    /// Extract the grouping key from a traverser.
    fn get_key(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Option<Value> {
        match &self.key_selector {
            GroupKey::Label => {
                // Extract label from vertex or edge
                match &traverser.value {
                    Value::Vertex(id) => ctx
                        .snapshot()
                        .storage()
                        .get_vertex(*id)
                        .map(|v| Value::String(v.label.clone())),
                    Value::Edge(id) => ctx
                        .snapshot()
                        .storage()
                        .get_edge(*id)
                        .map(|e| Value::String(e.label.clone())),
                    _ => None,
                }
            }
            GroupKey::Property(key) => {
                // Extract property value
                match &traverser.value {
                    Value::Vertex(id) => ctx
                        .snapshot()
                        .storage()
                        .get_vertex(*id)
                        .and_then(|v| v.properties.get(key).cloned()),
                    Value::Edge(id) => ctx
                        .snapshot()
                        .storage()
                        .get_edge(*id)
                        .and_then(|e| e.properties.get(key).cloned()),
                    _ => None,
                }
            }
            GroupKey::Traversal(sub) => {
                // Execute sub-traversal and get first result
                execute_traversal_from(ctx, sub, Box::new(std::iter::once(traverser.clone())))
                    .next()
                    .map(|t| t.value)
            }
        }
    }

    /// Extract the value to collect from a traverser.
    fn get_value(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Option<Value> {
        match &self.value_collector {
            GroupValue::Identity => {
                // Use the current value directly
                Some(traverser.value.clone())
            }
            GroupValue::Property(key) => {
                // Extract property value
                match &traverser.value {
                    Value::Vertex(id) => ctx
                        .snapshot()
                        .storage()
                        .get_vertex(*id)
                        .and_then(|v| v.properties.get(key).cloned()),
                    Value::Edge(id) => ctx
                        .snapshot()
                        .storage()
                        .get_edge(*id)
                        .and_then(|e| e.properties.get(key).cloned()),
                    _ => None,
                }
            }
            GroupValue::Traversal(sub) => {
                // Execute sub-traversal and collect all results
                let results: Vec<Value> =
                    execute_traversal_from(ctx, sub, Box::new(std::iter::once(traverser.clone())))
                        .map(|t| t.value)
                        .collect();

                if results.is_empty() {
                    None
                } else if results.len() == 1 {
                    Some(results.into_iter().next().unwrap())
                } else {
                    Some(Value::List(results))
                }
            }
        }
    }
}

impl Default for GroupStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for GroupStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Collect all input into groups (barrier)
        let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
        let mut last_path = None;

        for traverser in input {
            last_path = Some(traverser.path.clone());

            // Get the grouping key
            if let Some(key) = self.get_key(ctx, &traverser) {
                // Get the value to collect
                if let Some(value) = self.get_value(ctx, &traverser) {
                    // Convert key to string for map key
                    let key_str = match &key {
                        Value::String(s) => s.clone(),
                        Value::Int(n) => n.to_string(),
                        Value::Float(f) => f.to_string(),
                        Value::Bool(b) => b.to_string(),
                        Value::Vertex(id) => format!("v[{}]", id.0),
                        Value::Edge(id) => format!("e[{}]", id.0),
                        Value::Null => "null".to_string(),
                        Value::List(_) => continue, // Skip non-hashable keys
                        Value::Map(_) => continue,  // Skip non-hashable keys
                    };

                    groups.entry(key_str).or_default().push(value);
                }
            }
        }

        // Convert groups to a single Value::Map
        let mut result_map: HashMap<String, Value> = HashMap::new();
        for (key, values) in groups {
            result_map.insert(key, Value::List(values));
        }

        // Emit a single traverser with the grouped result
        let result_value = Value::Map(result_map);
        let result_traverser = Traverser {
            value: result_value,
            path: last_path.unwrap_or_default(),
            loops: 0,
            sack: None,
            bulk: 1,
        };

        Box::new(std::iter::once(result_traverser))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "group"
    }
}

// -----------------------------------------------------------------------------
// GroupBuilder - fluent API for building GroupStep
// -----------------------------------------------------------------------------

/// Fluent builder for creating GroupStep.
///
/// The builder allows configuring both the grouping key and the value collector
/// using `by` methods.
///
/// # Example
///
/// ```ignore
/// // Group by label, collect identity values
/// let groups = g.v()
///     .group().by_label().by_value().build()
///     .next();
///
/// // Group by property, collect other property
/// let groups = g.v().has_label("person")
///     .group().by_key("age").by_value_key("name").build()
///     .next();
/// ```
pub struct GroupBuilder<In> {
    steps: Vec<Box<dyn AnyStep>>,
    key_selector: Option<GroupKey>,
    value_collector: Option<GroupValue>,
    _phantom: PhantomData<In>,
}

impl<In> GroupBuilder<In> {
    /// Create a new GroupBuilder with existing steps.
    pub(crate) fn new(steps: Vec<Box<dyn AnyStep>>) -> Self {
        Self {
            steps,
            key_selector: None,
            value_collector: None,
            _phantom: PhantomData,
        }
    }

    /// Group by element label.
    pub fn by_label(mut self) -> Self {
        self.key_selector = Some(GroupKey::Label);
        self
    }

    /// Group by a property value.
    pub fn by_key(mut self, key: &str) -> Self {
        self.key_selector = Some(GroupKey::Property(key.to_string()));
        self
    }

    /// Group by the result of a sub-traversal.
    pub fn by_traversal(mut self, t: Traversal<Value, Value>) -> Self {
        self.key_selector = Some(GroupKey::Traversal(Box::new(t)));
        self
    }

    /// Collect identity values (the traverser's current value).
    pub fn by_value(mut self) -> Self {
        self.value_collector = Some(GroupValue::Identity);
        self
    }

    /// Collect property values.
    pub fn by_value_key(mut self, key: &str) -> Self {
        self.value_collector = Some(GroupValue::Property(key.to_string()));
        self
    }

    /// Collect values from a sub-traversal.
    pub fn by_value_traversal(mut self, t: Traversal<Value, Value>) -> Self {
        self.value_collector = Some(GroupValue::Traversal(Box::new(t)));
        self
    }

    /// Build the final traversal with the GroupStep.
    pub fn build(mut self) -> Traversal<In, Value> {
        // Default to label for key, identity for value
        let key_selector = self.key_selector.unwrap_or(GroupKey::Label);
        let value_collector = self.value_collector.unwrap_or(GroupValue::Identity);

        let group_step = GroupStep::with_selectors(key_selector, value_collector);
        self.steps.push(Box::new(group_step));

        Traversal {
            steps: self.steps,
            source: None,
            _phantom: PhantomData,
        }
    }
}

// -----------------------------------------------------------------------------
// BoundGroupBuilder - fluent API for bound traversals
// -----------------------------------------------------------------------------

/// Fluent builder for creating GroupStep for bound traversals.
///
/// This builder is returned from `BoundTraversal::group()` and allows chaining
/// configuration methods before calling `build()` to get back a `BoundTraversal`.
///
/// # Example
///
/// ```ignore
/// let groups = g.v().has_label("person")
///     .group()
///     .by_key("age")
///     .by_value_key("name")
///     .build()
///     .next();
/// ```
pub struct BoundGroupBuilder<'g, In> {
    snapshot: &'g crate::graph::GraphSnapshot<'g>,
    interner: &'g crate::storage::interner::StringInterner,
    source: Option<crate::traversal::TraversalSource>,
    steps: Vec<Box<dyn AnyStep>>,
    key_selector: Option<GroupKey>,
    value_collector: Option<GroupValue>,
    track_paths: bool,
    _phantom: PhantomData<In>,
}

impl<'g, In> BoundGroupBuilder<'g, In> {
    /// Create a new BoundGroupBuilder with existing steps and graph references.
    pub(crate) fn new(
        snapshot: &'g crate::graph::GraphSnapshot<'g>,
        interner: &'g crate::storage::interner::StringInterner,
        source: Option<crate::traversal::TraversalSource>,
        steps: Vec<Box<dyn AnyStep>>,
        track_paths: bool,
    ) -> Self {
        Self {
            snapshot,
            interner,
            source,
            steps,
            key_selector: None,
            value_collector: None,
            track_paths,
            _phantom: PhantomData,
        }
    }

    /// Group by element label.
    pub fn by_label(mut self) -> Self {
        self.key_selector = Some(GroupKey::Label);
        self
    }

    /// Group by a property value.
    pub fn by_key(mut self, key: &str) -> Self {
        self.key_selector = Some(GroupKey::Property(key.to_string()));
        self
    }

    /// Group by the result of a sub-traversal.
    pub fn by_traversal(mut self, t: Traversal<Value, Value>) -> Self {
        self.key_selector = Some(GroupKey::Traversal(Box::new(t)));
        self
    }

    /// Collect identity values (the traverser's current value).
    pub fn by_value(mut self) -> Self {
        self.value_collector = Some(GroupValue::Identity);
        self
    }

    /// Collect property values.
    pub fn by_value_key(mut self, key: &str) -> Self {
        self.value_collector = Some(GroupValue::Property(key.to_string()));
        self
    }

    /// Collect values from a sub-traversal.
    pub fn by_value_traversal(mut self, t: Traversal<Value, Value>) -> Self {
        self.value_collector = Some(GroupValue::Traversal(Box::new(t)));
        self
    }

    /// Build the final bound traversal with the GroupStep.
    pub fn build(mut self) -> crate::traversal::source::BoundTraversal<'g, In, Value> {
        // Default to label for key, identity for value
        let key_selector = self.key_selector.unwrap_or(GroupKey::Label);
        let value_collector = self.value_collector.unwrap_or(GroupValue::Identity);

        let group_step = GroupStep::with_selectors(key_selector, value_collector);
        self.steps.push(Box::new(group_step));

        let traversal = Traversal {
            steps: self.steps,
            source: self.source,
            _phantom: PhantomData,
        };

        let mut bound =
            crate::traversal::source::BoundTraversal::new(self.snapshot, self.interner, traversal);

        // Preserve track_paths by conditionally calling with_path()
        if self.track_paths {
            bound = bound.with_path();
        }

        bound
    }
}

// -----------------------------------------------------------------------------
// GroupCountStep - barrier step that counts traversers by key
// -----------------------------------------------------------------------------

/// Barrier step that counts traversers grouped by a key.
///
/// This is a **barrier step** - it collects ALL input before producing counted output.
/// The result is a single traverser containing a `Value::Map` where:
/// - Keys are the grouping keys (as Values)
/// - Values are integer counts (respecting traverser bulk)
///
/// # Gremlin Equivalent
///
/// ```groovy
/// g.V().groupCount().by(label)      // Count vertices by label
/// g.V().groupCount().by("age")      // Count vertices by age property
/// g.E().groupCount().by(label)      // Count edges by label
/// ```
///
/// # Example
///
/// ```ignore
/// use intersteller::*;
/// use intersteller::traversal::__; // Anonymous traversal factory
///
/// let graph = Graph::new(/* ... */);
/// let snapshot = graph.snapshot();
/// let g = snapshot.traversal();
///
/// // Count vertices by label
/// let counts = g.v().group_count().by_label().build().to_list();
/// // => [Map {"person" => 3, "software" => 1}]
///
/// // Count vertices by age property
/// let age_counts = g.v().has_label("person").group_count().by_key("age").build().to_list();
/// // => [Map {29 => 2, 30 => 1}]
/// ```
#[derive(Clone, Debug)]
pub struct GroupCountStep {
    key_selector: GroupKey,
}

impl GroupCountStep {
    /// Create a new GroupCountStep with the specified key selector.
    pub fn new(key_selector: GroupKey) -> Self {
        GroupCountStep { key_selector }
    }

    /// Extract the grouping key from a traverser.
    ///
    /// Returns `None` if the key cannot be extracted or is not hashable.
    fn get_key(&self, ctx: &ExecutionContext, traverser: &Traverser) -> Option<Value> {
        match &self.key_selector {
            GroupKey::Label => match &traverser.value {
                Value::Vertex(id) => ctx
                    .snapshot()
                    .storage()
                    .get_vertex(*id)
                    .map(|v| Value::String(v.label.clone())),
                Value::Edge(id) => ctx
                    .snapshot()
                    .storage()
                    .get_edge(*id)
                    .map(|e| Value::String(e.label.clone())),
                _ => None,
            },
            GroupKey::Property(key) => match &traverser.value {
                Value::Vertex(id) => ctx
                    .snapshot()
                    .storage()
                    .get_vertex(*id)
                    .and_then(|v| v.properties.get(key).cloned()),
                Value::Edge(id) => ctx
                    .snapshot()
                    .storage()
                    .get_edge(*id)
                    .and_then(|e| e.properties.get(key).cloned()),
                _ => None,
            },
            GroupKey::Traversal(t) => {
                // Execute the traversal on the current traverser
                let results =
                    execute_traversal_from(ctx, t, Box::new(std::iter::once(traverser.clone())));
                results.into_iter().next().map(|t| t.value)
            }
        }
    }
}

impl AnyStep for GroupCountStep {
    fn apply<'a>(
        &self,
        ctx: &'a ExecutionContext,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Collect all input and count by group key
        let mut counts: HashMap<String, i64> = HashMap::new();
        let mut last_path = None;

        for traverser in input {
            last_path = Some(traverser.path.clone());

            if let Some(key_value) = self.get_key(ctx, &traverser) {
                // Convert value to string for hashmap key
                // Skip non-hashable types (List, Map)
                let key_str = match key_value {
                    Value::String(s) => s,
                    Value::Int(i) => i.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Vertex(id) => format!("v[{}]", id.0),
                    Value::Edge(id) => format!("e[{}]", id.0),
                    Value::Null => "null".to_string(),
                    Value::List(_) => continue, // Skip non-hashable keys
                    Value::Map(_) => continue,  // Skip non-hashable keys
                };

                // Increment count by traverser bulk
                *counts.entry(key_str).or_insert(0) += traverser.bulk as i64;
            }
        }

        // Convert counts to a single Value::Map
        let mut result_map: HashMap<String, Value> = HashMap::new();
        for (key, count) in counts {
            result_map.insert(key, Value::Int(count));
        }

        // Emit a single traverser with the counted result
        let result_value = Value::Map(result_map);
        let result_traverser = Traverser {
            value: result_value,
            path: last_path.unwrap_or_default(),
            loops: 0,
            sack: None,
            bulk: 1,
        };

        Box::new(std::iter::once(result_traverser))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "groupCount"
    }
}

// -----------------------------------------------------------------------------
// GroupCountBuilder - fluent API for building GroupCountStep
// -----------------------------------------------------------------------------

/// Fluent builder for creating GroupCountStep.
///
/// The builder allows configuring the grouping key using `by` methods.
/// Unlike `GroupBuilder`, there is no value collector since we always count.
///
/// # Example
///
/// ```ignore
/// use intersteller::*;
///
/// let graph = Graph::new(/* ... */);
/// let snapshot = graph.snapshot();
/// let g = snapshot.traversal();
///
/// // Count by label
/// g.v().group_count().by_label().build();
///
/// // Count by property
/// g.v().group_count().by_key("age").build();
/// ```
pub struct GroupCountBuilder<In> {
    steps: Vec<Box<dyn AnyStep>>,
    key_selector: Option<GroupKey>,
    _phantom: PhantomData<In>,
}

impl<In> GroupCountBuilder<In> {
    /// Create a new GroupCountBuilder with existing steps.
    pub(crate) fn new(steps: Vec<Box<dyn AnyStep>>) -> Self {
        GroupCountBuilder {
            steps,
            key_selector: None,
            _phantom: PhantomData,
        }
    }

    /// Group by element label (vertex or edge label).
    pub fn by_label(mut self) -> Self {
        self.key_selector = Some(GroupKey::Label);
        self
    }

    /// Group by a property value.
    pub fn by_key(mut self, key: &str) -> Self {
        self.key_selector = Some(GroupKey::Property(key.to_string()));
        self
    }

    /// Group by the result of a traversal.
    pub fn by_traversal(mut self, traversal: Traversal<Value, Value>) -> Self {
        self.key_selector = Some(GroupKey::Traversal(Box::new(traversal)));
        self
    }

    /// Build the final traversal with the GroupCountStep.
    ///
    /// If no key selector is specified, defaults to grouping by identity (the traverser value itself).
    pub fn build(self) -> Traversal<In, Value> {
        let key_selector = self.key_selector.unwrap_or(GroupKey::Label);
        let mut steps = self.steps;
        steps.push(Box::new(GroupCountStep::new(key_selector)));
        Traversal {
            steps,
            source: None,
            _phantom: PhantomData,
        }
    }
}

// -----------------------------------------------------------------------------
// BoundGroupCountBuilder - for bound traversals
// -----------------------------------------------------------------------------

/// Builder for `group_count()` on `BoundTraversal`.
///
/// This builder preserves the graph snapshot reference and path tracking state
/// when building the final `BoundTraversal`.
pub struct BoundGroupCountBuilder<'g, In> {
    snapshot: &'g crate::graph::GraphSnapshot<'g>,
    interner: &'g crate::storage::interner::StringInterner,
    source: Option<crate::traversal::TraversalSource>,
    steps: Vec<Box<dyn AnyStep>>,
    key_selector: Option<GroupKey>,
    track_paths: bool,
    _phantom: PhantomData<In>,
}

impl<'g, In> BoundGroupCountBuilder<'g, In> {
    /// Create a new BoundGroupCountBuilder.
    pub(crate) fn new(
        snapshot: &'g crate::graph::GraphSnapshot<'g>,
        interner: &'g crate::storage::interner::StringInterner,
        source: Option<crate::traversal::TraversalSource>,
        steps: Vec<Box<dyn AnyStep>>,
        track_paths: bool,
    ) -> Self {
        BoundGroupCountBuilder {
            snapshot,
            interner,
            source,
            steps,
            key_selector: None,
            track_paths,
            _phantom: PhantomData,
        }
    }

    /// Group by element label.
    pub fn by_label(mut self) -> Self {
        self.key_selector = Some(GroupKey::Label);
        self
    }

    /// Group by a property value.
    pub fn by_key(mut self, key: &str) -> Self {
        self.key_selector = Some(GroupKey::Property(key.to_string()));
        self
    }

    /// Group by the result of a traversal.
    pub fn by_traversal(mut self, traversal: Traversal<Value, Value>) -> Self {
        self.key_selector = Some(GroupKey::Traversal(Box::new(traversal)));
        self
    }

    /// Build the final BoundTraversal with the GroupCountStep.
    pub fn build(self) -> crate::traversal::BoundTraversal<'g, In, Value> {
        let key_selector = self.key_selector.unwrap_or(GroupKey::Label);
        let mut steps = self.steps;
        steps.push(Box::new(GroupCountStep::new(key_selector)));

        let traversal = Traversal {
            steps,
            source: self.source,
            _phantom: PhantomData,
        };

        let mut bound =
            crate::traversal::source::BoundTraversal::new(self.snapshot, self.interner, traversal);

        // Preserve track_paths by conditionally calling with_path()
        if self.track_paths {
            bound = bound.with_path();
        }

        bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Graph;
    use crate::storage::InMemoryGraph;
    use std::collections::HashMap as StdHashMap;

    fn create_test_graph() -> Graph {
        let mut storage = InMemoryGraph::new();

        // Add person vertices with different ages
        let mut props1 = StdHashMap::new();
        props1.insert("name".to_string(), Value::String("Alice".to_string()));
        props1.insert("age".to_string(), Value::Int(29));
        storage.add_vertex("person", props1);

        let mut props2 = StdHashMap::new();
        props2.insert("name".to_string(), Value::String("Bob".to_string()));
        props2.insert("age".to_string(), Value::Int(29));
        storage.add_vertex("person", props2);

        let mut props3 = StdHashMap::new();
        props3.insert("name".to_string(), Value::String("Charlie".to_string()));
        props3.insert("age".to_string(), Value::Int(30));
        storage.add_vertex("person", props3);

        // Add software vertices
        let mut props4 = StdHashMap::new();
        props4.insert("name".to_string(), Value::String("lop".to_string()));
        storage.add_vertex("software", props4);

        Graph::new(storage)
    }

    #[test]
    fn test_group_by_label_identity() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group all vertices by label
        let result = g.v().group().by_label().by_value().build().next();

        assert!(result.is_some());
        let result = result.unwrap();

        // Should be a Map
        if let Value::Map(map) = result {
            // Should have "person" and "software" keys
            assert!(map.contains_key("person"));
            assert!(map.contains_key("software"));

            // Person group should have 3 vertices
            if let Some(Value::List(persons)) = map.get("person") {
                assert_eq!(persons.len(), 3);
                // All should be vertices
                for val in persons {
                    assert!(matches!(val, Value::Vertex(_)));
                }
            } else {
                panic!("Expected person group to be a list");
            }

            // Software group should have 1 vertex
            if let Some(Value::List(softwares)) = map.get("software") {
                assert_eq!(softwares.len(), 1);
                assert!(matches!(softwares[0], Value::Vertex(_)));
            } else {
                panic!("Expected software group to be a list");
            }
        } else {
            panic!("Expected Map value, got: {:?}", result);
        }
    }

    #[test]
    fn test_group_by_property_collect_property() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group people by age, collect names
        let result = g
            .v()
            .has_label("person")
            .group()
            .by_key("age")
            .by_value_key("name")
            .build()
            .next();

        assert!(result.is_some());
        let result = result.unwrap();

        if let Value::Map(map) = result {
            // Should have "29" and "30" keys
            assert!(map.contains_key("29") || map.contains_key("30"));

            // Age 29 should have Alice and Bob
            if let Some(Value::List(names)) = map.get("29") {
                assert_eq!(names.len(), 2);
                assert!(names.contains(&Value::String("Alice".to_string())));
                assert!(names.contains(&Value::String("Bob".to_string())));
            }

            // Age 30 should have Charlie
            if let Some(Value::List(names)) = map.get("30") {
                assert_eq!(names.len(), 1);
                assert!(names.contains(&Value::String("Charlie".to_string())));
            }
        } else {
            panic!("Expected Map value");
        }
    }

    #[test]
    fn test_group_default_selectors() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Use default selectors (by label, identity value)
        let result = g.v().group().build().next();

        assert!(result.is_some());
        let result = result.unwrap();

        // Should still be a Map grouped by label
        if let Value::Map(map) = result {
            assert!(map.contains_key("person"));
            assert!(map.contains_key("software"));
        } else {
            panic!("Expected Map value");
        }
    }

    #[test]
    fn test_group_builder_fluent_api() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Test fluent API chaining
        let result = g
            .v()
            .has_label("person")
            .group()
            .by_key("age")
            .by_value()
            .build()
            .next();

        assert!(result.is_some());

        if let Some(Value::Map(map)) = result {
            // Each group should have vertex values (identity)
            for (_, value) in map {
                if let Value::List(values) = value {
                    for val in values {
                        assert!(matches!(val, Value::Vertex(_)));
                    }
                }
            }
        }
    }

    #[test]
    fn test_group_empty_input() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Filter to no vertices, then group
        let result = g
            .v()
            .has_label("nonexistent")
            .group()
            .by_label()
            .by_value()
            .build()
            .next();

        // Should return empty map
        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            assert!(map.is_empty());
        }
    }

    // -------------------------------------------------------------------------
    // GroupCountStep Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_group_count_by_label() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Count all vertices by label
        let result = g.v().group_count().by_label().build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should have 2 labels: "person" and "software"
            assert_eq!(map.len(), 2);
            assert_eq!(map.get("person"), Some(&Value::Int(3))); // 3 person vertices
            assert_eq!(map.get("software"), Some(&Value::Int(1))); // 1 software vertex
        } else {
            panic!("Expected Value::Map, got {:?}", result);
        }
    }

    #[test]
    fn test_group_count_by_property() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Count person vertices by age property
        let result = g
            .v()
            .has_label("person")
            .group_count()
            .by_key("age")
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should have 2 ages: 29 and 30
            assert_eq!(map.len(), 2);
            assert_eq!(map.get("29"), Some(&Value::Int(2))); // Alice and Bob are 29
            assert_eq!(map.get("30"), Some(&Value::Int(1))); // Charlie is 30
        } else {
            panic!("Expected Value::Map, got {:?}", result);
        }
    }

    #[test]
    fn test_group_count_default_selector() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group count without specifying selector (should default to label)
        let result = g.v().group_count().build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should group by label by default
            assert_eq!(map.len(), 2);
            assert_eq!(map.get("person"), Some(&Value::Int(3)));
            assert_eq!(map.get("software"), Some(&Value::Int(1)));
        } else {
            panic!("Expected Value::Map, got {:?}", result);
        }
    }

    #[test]
    fn test_group_count_empty_input() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Filter to no vertices, then group count
        let result = g
            .v()
            .has_label("nonexistent")
            .group_count()
            .by_label()
            .build()
            .next();

        // Should return empty map
        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            assert!(map.is_empty());
        } else {
            panic!("Expected Value::Map, got {:?}", result);
        }
    }

    #[test]
    fn test_group_count_respects_bulk() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

        let step = GroupCountStep::new(GroupKey::Label);

        // Create traversers with different bulk values
        let mut t1 = Traverser::from_vertex(crate::value::VertexId(0)); // person
        t1.bulk = 5;

        let mut t2 = Traverser::from_vertex(crate::value::VertexId(1)); // person
        t2.bulk = 3;

        let mut t3 = Traverser::from_vertex(crate::value::VertexId(3)); // software
        t3.bulk = 2;

        let input = vec![t1, t2, t3];

        let result: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

        assert_eq!(result.len(), 1);
        if let Value::Map(map) = &result[0].value {
            // person count should be 5 + 3 = 8
            assert_eq!(map.get("person"), Some(&Value::Int(8)));
            // software count should be 2
            assert_eq!(map.get("software"), Some(&Value::Int(2)));
        } else {
            panic!("Expected Value::Map, got {:?}", result[0].value);
        }
    }

    // -------------------------------------------------------------------------
    // GroupStep - Advanced Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_group_by_traversal_key() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group by out-degree (count of outgoing edges)
        // Since our test graph has no edges, we'll use a simpler traversal
        // Group by the first character of name property
        let key_traversal = crate::traversal::__::values("name");

        let result = g
            .v()
            .has_label("person")
            .group()
            .by_traversal(key_traversal)
            .by_value()
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should group by name values
            assert!(!map.is_empty());
            // Each group should contain vertices
            for (_key, value) in map {
                if let Value::List(vertices) = value {
                    for v in vertices {
                        assert!(matches!(v, Value::Vertex(_)));
                    }
                }
            }
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_by_value_traversal() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group by label, collect age property values
        let value_traversal = crate::traversal::__::values("age");

        let result = g
            .v()
            .has_label("person")
            .group()
            .by_label()
            .by_value_traversal(value_traversal)
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should have "person" key
            if let Some(Value::List(ages)) = map.get("person") {
                // Should have 3 age values
                assert_eq!(ages.len(), 3);
                // All should be integers
                for age in ages {
                    assert!(matches!(age, Value::Int(_)));
                }
            } else {
                panic!("Expected person group with list of ages");
            }
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_edges_by_label() {
        let mut storage = InMemoryGraph::new();

        // Create vertices
        let mut props1 = StdHashMap::new();
        props1.insert("name".to_string(), Value::String("v1".to_string()));
        storage.add_vertex("person", props1);

        let mut props2 = StdHashMap::new();
        props2.insert("name".to_string(), Value::String("v2".to_string()));
        storage.add_vertex("person", props2);

        let mut props3 = StdHashMap::new();
        props3.insert("name".to_string(), Value::String("v3".to_string()));
        storage.add_vertex("software", props3);

        // Create edges
        storage
            .add_edge(
                crate::value::VertexId(0),
                crate::value::VertexId(1),
                "knows",
                StdHashMap::new(),
            )
            .unwrap();

        storage
            .add_edge(
                crate::value::VertexId(0),
                crate::value::VertexId(2),
                "created",
                StdHashMap::new(),
            )
            .unwrap();

        storage
            .add_edge(
                crate::value::VertexId(1),
                crate::value::VertexId(2),
                "created",
                StdHashMap::new(),
            )
            .unwrap();

        let graph = Graph::new(storage);
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group all edges by label
        let result = g.e().group().by_label().by_value().build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should have "knows" and "created" groups
            assert_eq!(map.len(), 2);

            if let Some(Value::List(knows_edges)) = map.get("knows") {
                assert_eq!(knows_edges.len(), 1);
                assert!(matches!(knows_edges[0], Value::Edge(_)));
            } else {
                panic!("Expected knows edges");
            }

            if let Some(Value::List(created_edges)) = map.get("created") {
                assert_eq!(created_edges.len(), 2);
                for edge in created_edges {
                    assert!(matches!(edge, Value::Edge(_)));
                }
            } else {
                panic!("Expected created edges");
            }
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_edges_by_property() {
        let mut storage = InMemoryGraph::new();

        storage.add_vertex("person", StdHashMap::new());
        storage.add_vertex("person", StdHashMap::new());

        // Create edges with weight property
        let mut edge1_props = StdHashMap::new();
        edge1_props.insert("weight".to_string(), Value::Float(0.5));
        storage
            .add_edge(
                crate::value::VertexId(0),
                crate::value::VertexId(1),
                "knows",
                edge1_props,
            )
            .unwrap();

        let mut edge2_props = StdHashMap::new();
        edge2_props.insert("weight".to_string(), Value::Float(0.8));
        storage
            .add_edge(
                crate::value::VertexId(1),
                crate::value::VertexId(0),
                "knows",
                edge2_props,
            )
            .unwrap();

        let graph = Graph::new(storage);
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group edges by weight property
        let result = g.e().group().by_key("weight").by_value().build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should have two groups by weight
            assert_eq!(map.len(), 2);
            assert!(map.contains_key("0.5") || map.contains_key("0.8"));
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_preserves_path() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Create traversal with path tracking
        let result = g
            .v()
            .has_label("person")
            .with_path()
            .group()
            .by_label()
            .by_value()
            .build()
            .next();

        assert!(result.is_some());
        // The result should exist - path tracking doesn't affect grouping
        if let Some(Value::Map(_map)) = result {
            // Success - grouping worked with path tracking enabled
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_with_bulk_traversers() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

        let step = GroupStep::with_selectors(GroupKey::Label, GroupValue::Identity);

        // Create traversers with bulk > 1
        let mut t1 = Traverser::from_vertex(crate::value::VertexId(0)); // person
        t1.bulk = 3;

        let mut t2 = Traverser::from_vertex(crate::value::VertexId(1)); // person
        t2.bulk = 2;

        let input = vec![t1, t2];

        let result: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

        assert_eq!(result.len(), 1);
        if let Value::Map(map) = &result[0].value {
            if let Some(Value::List(persons)) = map.get("person") {
                // Should have 2 vertex values (one per input traverser)
                // Note: GroupStep doesn't expand by bulk, it just collects values
                assert_eq!(persons.len(), 2);
            } else {
                panic!("Expected person group");
            }
        } else {
            panic!("Expected Value::Map");
        }
    }

    // -------------------------------------------------------------------------
    // GroupCountStep - Advanced Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_group_count_by_traversal() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Count by name property value
        let key_traversal = crate::traversal::__::values("name");

        let result = g
            .v()
            .has_label("person")
            .group_count()
            .by_traversal(key_traversal)
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should have 3 entries (Alice, Bob, Charlie)
            assert_eq!(map.len(), 3);
            // Each name should have count of 1
            assert_eq!(map.get("Alice"), Some(&Value::Int(1)));
            assert_eq!(map.get("Bob"), Some(&Value::Int(1)));
            assert_eq!(map.get("Charlie"), Some(&Value::Int(1)));
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_count_edges_by_label() {
        let mut storage = InMemoryGraph::new();

        storage.add_vertex("person", StdHashMap::new());
        storage.add_vertex("person", StdHashMap::new());
        storage.add_vertex("software", StdHashMap::new());

        // Create edges
        storage
            .add_edge(
                crate::value::VertexId(0),
                crate::value::VertexId(1),
                "knows",
                StdHashMap::new(),
            )
            .unwrap();

        storage
            .add_edge(
                crate::value::VertexId(0),
                crate::value::VertexId(2),
                "created",
                StdHashMap::new(),
            )
            .unwrap();

        storage
            .add_edge(
                crate::value::VertexId(1),
                crate::value::VertexId(2),
                "created",
                StdHashMap::new(),
            )
            .unwrap();

        let graph = Graph::new(storage);
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Count edges by label
        let result = g.e().group_count().by_label().build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            assert_eq!(map.len(), 2);
            assert_eq!(map.get("knows"), Some(&Value::Int(1)));
            assert_eq!(map.get("created"), Some(&Value::Int(2)));
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_count_edges_by_property() {
        let mut storage = InMemoryGraph::new();

        storage.add_vertex("person", StdHashMap::new());
        storage.add_vertex("person", StdHashMap::new());

        // Create edges with weight property
        let mut edge1_props = StdHashMap::new();
        edge1_props.insert("weight".to_string(), Value::Float(0.5));
        storage
            .add_edge(
                crate::value::VertexId(0),
                crate::value::VertexId(1),
                "knows",
                edge1_props,
            )
            .unwrap();

        let mut edge2_props = StdHashMap::new();
        edge2_props.insert("weight".to_string(), Value::Float(0.5));
        storage
            .add_edge(
                crate::value::VertexId(1),
                crate::value::VertexId(0),
                "knows",
                edge2_props,
            )
            .unwrap();

        let mut edge3_props = StdHashMap::new();
        edge3_props.insert("weight".to_string(), Value::Float(0.8));
        storage
            .add_edge(
                crate::value::VertexId(0),
                crate::value::VertexId(1),
                "likes",
                edge3_props,
            )
            .unwrap();

        let graph = Graph::new(storage);
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Count edges by weight property
        let result = g.e().group_count().by_key("weight").build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            assert_eq!(map.len(), 2);
            assert_eq!(map.get("0.5"), Some(&Value::Int(2)));
            assert_eq!(map.get("0.8"), Some(&Value::Int(1)));
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_count_preserves_path() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Create traversal with path tracking
        let result = g
            .v()
            .has_label("person")
            .with_path()
            .group_count()
            .by_label()
            .build()
            .next();

        assert!(result.is_some());
        // The result should exist - path tracking doesn't affect counting
        if let Some(Value::Map(map)) = result {
            assert_eq!(map.get("person"), Some(&Value::Int(3)));
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_count_multiple_bulk_values() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

        let step = GroupCountStep::new(GroupKey::Property("age".to_string()));

        // Create traversers with different bulk values
        let mut t1 = Traverser::from_vertex(crate::value::VertexId(0)); // Alice, age 29
        t1.bulk = 10;

        let mut t2 = Traverser::from_vertex(crate::value::VertexId(1)); // Bob, age 29
        t2.bulk = 5;

        let mut t3 = Traverser::from_vertex(crate::value::VertexId(2)); // Charlie, age 30
        t3.bulk = 3;

        let input = vec![t1, t2, t3];

        let result: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

        assert_eq!(result.len(), 1);
        if let Value::Map(map) = &result[0].value {
            // Age 29 should have bulk 10 + 5 = 15
            assert_eq!(map.get("29"), Some(&Value::Int(15)));
            // Age 30 should have bulk 3
            assert_eq!(map.get("30"), Some(&Value::Int(3)));
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_count_with_missing_property() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Count by a property that doesn't exist on all vertices
        let result = g.v().group_count().by_key("nonexistent").build().next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should return empty map since no vertices have this property
            assert!(map.is_empty());
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_with_missing_property() {
        let graph = create_test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.traversal();

        // Group by a property that doesn't exist on all vertices
        let result = g
            .v()
            .group()
            .by_key("nonexistent")
            .by_value()
            .build()
            .next();

        assert!(result.is_some());
        if let Some(Value::Map(map)) = result {
            // Should return empty map since no vertices have this property
            assert!(map.is_empty());
        } else {
            panic!("Expected Value::Map");
        }
    }

    #[test]
    fn test_group_step_construction() {
        let step = GroupStep::new();
        assert_eq!(step.name(), "group");

        let step2 = GroupStep::with_selectors(
            GroupKey::Property("age".to_string()),
            GroupValue::Property("name".to_string()),
        );
        assert_eq!(step2.name(), "group");
    }

    #[test]
    fn test_group_count_step_construction() {
        let step = GroupCountStep::new(GroupKey::Label);
        assert_eq!(step.name(), "groupCount");

        let step2 = GroupCountStep::new(GroupKey::Property("age".to_string()));
        assert_eq!(step2.name(), "groupCount");
    }
}
