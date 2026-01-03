//! Transform steps for graph traversal.
//!
//! This module provides transform steps that convert traverser values
//! into different values. Transform steps map input values to output values,
//! potentially changing the type of the traverser.
//!
//! # Steps
//!
//! - `ValuesStep`: Extract property values from vertices/edges

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
                    .snapshot()
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
                    .snapshot()
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
}

// Use the macro to implement AnyStep for ValuesStep
impl_flatmap_step!(ValuesStep, "values");

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

// -----------------------------------------------------------------------------
// MapStep - transform each value with a closure
// -----------------------------------------------------------------------------

/// Transform step that applies a closure to each value.
///
/// This step transforms each traverser's value using a user-provided function.
/// The closure receives the execution context and the current value, returning
/// a new value. This is a 1:1 mapping - each input produces exactly one output.
///
/// # Type Parameters
///
/// - `F`: The closure type that transforms values
///
/// # Example
///
/// ```ignore
/// // Double all integer values
/// let doubled = g.inject([1i64, 2i64, 3i64])
///     .map(|_ctx, v| {
///         if let Value::Int(n) = v {
///             Value::Int(n * 2)
///         } else {
///             v.clone()
///         }
///     })
///     .to_list();
/// // Results: [2, 4, 6]
/// ```
#[derive(Clone)]
pub struct MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync,
{
    f: F,
}

impl<F> MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync,
{
    /// Create a new MapStep with the given transformation function.
    ///
    /// # Arguments
    ///
    /// * `f` - The function to apply to each value
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = MapStep::new(|_ctx, v| {
    ///     if let Value::Int(n) = v {
    ///         Value::Int(n * 2)
    ///     } else {
    ///         v.clone()
    ///     }
    /// });
    /// ```
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> crate::traversal::step::AnyStep for MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync + 'static,
{
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let f = self.f.clone();
        Box::new(input.map(move |t| {
            let new_value = f(ctx, &t.value);
            t.with_value(new_value)
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "map"
    }
}

// Implement Debug manually since we can't derive it for closures
impl<F> std::fmt::Debug for MapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Value + Clone + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MapStep").finish_non_exhaustive()
    }
}

// -----------------------------------------------------------------------------
// FlatMapStep - transform each value to multiple values with a closure
// -----------------------------------------------------------------------------

/// Transform step that applies a closure returning multiple values.
///
/// This step transforms each traverser's value using a user-provided function
/// that returns a `Vec<Value>`. This is a 1:N mapping - each input can produce
/// zero or more outputs.
///
/// # Type Parameters
///
/// - `F`: The closure type that transforms values to a vector
///
/// # Example
///
/// ```ignore
/// // Generate a range of values from each integer
/// let expanded = g.inject([3i64, 5i64])
///     .flat_map(|_ctx, v| {
///         if let Value::Int(n) = v {
///             (0..*n).map(|i| Value::Int(i)).collect()
///         } else {
///             vec![]
///         }
///     })
///     .to_list();
/// // Results: [0, 1, 2, 0, 1, 2, 3, 4]
/// ```
#[derive(Clone)]
pub struct FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync,
{
    f: F,
}

impl<F> FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync,
{
    /// Create a new FlatMapStep with the given transformation function.
    ///
    /// # Arguments
    ///
    /// * `f` - The function to apply to each value, returning a Vec of new values
    ///
    /// # Example
    ///
    /// ```ignore
    /// let step = FlatMapStep::new(|_ctx, v| {
    ///     if let Value::Int(n) = v {
    ///         (0..*n).map(|i| Value::Int(i)).collect()
    ///     } else {
    ///         vec![]
    ///     }
    /// });
    /// ```
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> crate::traversal::step::AnyStep for FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync + 'static,
{
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let f = self.f.clone();
        Box::new(input.flat_map(move |t| {
            let values = f(ctx, &t.value);
            values.into_iter().map(move |v| t.split(v))
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "flatMap"
    }
}

// Implement Debug manually since we can't derive it for closures
impl<F> std::fmt::Debug for FlatMapStep<F>
where
    F: Fn(&ExecutionContext, &Value) -> Vec<Value> + Clone + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlatMapStep").finish_non_exhaustive()
    }
}

// -----------------------------------------------------------------------------
// ConstantStep - emit a constant value for each traverser
// -----------------------------------------------------------------------------

/// Transform step that replaces each traverser's value with a constant.
///
/// This step replaces the value of each traverser with the specified constant
/// value, preserving all traverser metadata (path, loops, bulk, sack).
///
/// # Behavior
///
/// - Each input traverser produces exactly one output traverser
/// - The output value is always the constant, regardless of input
/// - Path history, loop count, bulk, and sack are preserved
///
/// # Example
///
/// ```ignore
/// // Replace all values with the string "found"
/// let results = g.v().constant("found").to_list();
/// // All results will be Value::String("found")
///
/// // Replace with a number
/// let results = g.inject([1, 2, 3]).constant(42i64).to_list();
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

impl crate::traversal::step::AnyStep for PathStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.map(|t| {
            // Convert path elements to a Value::List
            let path_values: Vec<Value> = t.path.objects().map(|pv| pv.to_value()).collect();
            t.with_value(Value::List(path_values))
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "path"
    }
}

// -----------------------------------------------------------------------------
// AsStep - label current position in path
// -----------------------------------------------------------------------------

/// Step that labels the current position in the traversal path.
///
/// The `as_()` step records the current traverser's value in the path
/// with the specified label. This enables later retrieval via `select()`.
///
/// Unlike automatic path tracking, `as_()` labels are always recorded
/// regardless of whether `with_path()` was called.
///
/// # Behavior
///
/// - Passes traversers through unchanged (identity behavior)
/// - Adds the current value to the path with the specified label
/// - Multiple `as_()` calls with the same label create multiple entries
///
/// # Example
///
/// ```ignore
/// // Label positions for later selection
/// g.v().as_("start").out().as_("end").select(&["start", "end"])
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

impl crate::traversal::step::AnyStep for AsStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let label = self.label.clone();
        Box::new(input.map(move |mut t| {
            // Label the current path position (don't add duplicate entry)
            t.label_path_position(&label);
            t
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "as"
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
    /// Returns the value directly (not wrapped in a Map).
    ///
    /// # Arguments
    ///
    /// * `label` - The label to select from the path
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

    /// Get the labels for this step.
    #[inline]
    pub fn labels(&self) -> &[String] {
        &self.labels
    }

    /// Check if this is a single-label select.
    #[inline]
    pub fn is_single(&self) -> bool {
        self.labels.len() == 1
    }
}

impl crate::traversal::step::AnyStep for SelectStep {
    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        let labels = self.labels.clone();
        let is_single = self.labels.len() == 1;

        Box::new(input.filter_map(move |t| {
            if is_single {
                // Single label: return value directly
                let label = &labels[0];
                let value = t
                    .path
                    .get(label)
                    .and_then(|values| values.last().cloned())
                    .map(|pv| pv.to_value());
                value.map(|v| t.with_value(v))
            } else {
                // Multiple labels: return Map
                // ALL labels must be present, otherwise filter out
                let mut map = std::collections::HashMap::new();
                for label in &labels {
                    if let Some(values) = t.path.get(label) {
                        if let Some(last) = values.last() {
                            map.insert(label.clone(), last.to_value());
                        } else {
                            return None; // Label exists but no values
                        }
                    } else {
                        return None; // Label doesn't exist
                    }
                }
                Some(t.with_value(Value::Map(map)))
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn crate::traversal::step::AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "select"
    }
}

// -----------------------------------------------------------------------------
// PropertiesStep - extract property objects from elements
// -----------------------------------------------------------------------------

/// Transform step that extracts property objects from vertices and edges.
///
/// Unlike `values()` which returns just property values, `properties()` returns
/// the full property including its key as a Map with "key" and "value" entries.
///
/// # Behavior
///
/// - For vertices: extracts properties as `{key: "name", value: <value>}` maps
/// - For edges: extracts properties as `{key: "name", value: <value>}` maps
/// - For non-element values: filtered out (produces no output)
/// - `keys: None` returns all properties
/// - `keys: Some([...])` returns only specified properties
///
/// # Example
///
/// ```ignore
/// // Extract all properties from person vertices as key-value maps
/// let props = g.v().has_label("person").properties().to_list();
/// // Each result is Value::Map { "key": "name", "value": "Alice" } etc.
///
/// // Extract specific properties
/// let props = g.v().properties_keys(&["name", "age"]).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct PropertiesStep {
    /// Property keys to extract. None means all properties.
    keys: Option<Vec<String>>,
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
    fn make_property_map(key: String, value: Value) -> Value {
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
}

impl Default for PropertiesStep {
    fn default() -> Self {
        Self::new()
    }
}

// Use the macro to implement AnyStep for PropertiesStep
impl_flatmap_step!(PropertiesStep, "properties");

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

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
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "values");
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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::from_vertex(VertexId(3))]; // Company with no properties

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn nonexistent_property_key_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::from_vertex(VertexId(999))]; // Non-existent vertex

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn extracts_different_value_types() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValuesStep::new("since");

            let input = vec![Traverser::from_edge(EdgeId(1))]; // uses edge with no properties

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn nonexistent_edge_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::new(Value::String("hello".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::new(Value::Null)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_boolean_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValuesStep::new("name");

            let input = vec![Traverser::new(Value::Bool(true))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn mixed_elements_and_non_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValuesStep::new("name");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn empty_keys_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ValuesStep::multi(vec![]);

            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
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

    // =========================================================================
    // MapStep Tests
    // =========================================================================

    mod map_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = MapStep::new(|_ctx, v| v.clone());
            assert_eq!(step.name(), "map");
        }

        #[test]
        fn clone_box_works() {
            let step = MapStep::new(|_ctx, v| v.clone());
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "map");
        }

        #[test]
        fn debug_format() {
            let step = MapStep::new(|_ctx, v| v.clone());
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("MapStep"));
        }
    }

    mod map_step_transform_tests {
        use super::*;

        #[test]
        fn identity_map_preserves_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| v.clone());
            let input = vec![
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
        fn doubles_integer_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| {
                if let Value::Int(n) = v {
                    Value::Int(n * 2)
                } else {
                    v.clone()
                }
            });
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(2)),
                Traverser::new(Value::Int(3)),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(2));
            assert_eq!(output[1].value, Value::Int(4));
            assert_eq!(output[2].value, Value::Int(6));
        }

        #[test]
        fn converts_to_string() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| {
                let s = match v {
                    Value::Int(n) => format!("num:{}", n),
                    Value::String(s) => format!("str:{}", s),
                    _ => "other".to_string(),
                };
                Value::String(s)
            });
            let input = vec![
                Traverser::new(Value::Int(42)),
                Traverser::new(Value::String("hello".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::String("num:42".to_string()));
            assert_eq!(output[1].value, Value::String("str:hello".to_string()));
        }

        #[test]
        fn can_access_execution_context() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Use context to get a vertex (context should be accessible)
            let step = MapStep::new(|ctx, v| {
                if let Value::Vertex(id) = v {
                    if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                        vertex
                            .properties
                            .get("name")
                            .cloned()
                            .unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                } else {
                    v.clone()
                }
            });
            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::String("Alice".to_string()));
        }
    }

    mod map_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| v.clone());

            let mut traverser = Traverser::new(Value::Int(42));
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

            let step = MapStep::new(|_ctx, v| v.clone());

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.loops = 7;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].loops, 7);
        }

        #[test]
        fn preserves_bulk_count() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| v.clone());

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.bulk = 15;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].bulk, 15);
        }
    }

    mod map_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = MapStep::new(|_ctx, v| v.clone());
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    // =========================================================================
    // FlatMapStep Tests
    // =========================================================================

    mod flatmap_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            assert_eq!(step.name(), "flatMap");
        }

        #[test]
        fn clone_box_works() {
            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "flatMap");
        }

        #[test]
        fn debug_format() {
            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("FlatMapStep"));
        }
    }

    mod flatmap_step_transform_tests {
        use super::*;

        #[test]
        fn identity_flat_map_preserves_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(2));
        }

        #[test]
        fn duplicates_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone(), v.clone()]);
            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 4);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(1));
            assert_eq!(output[2].value, Value::Int(2));
            assert_eq!(output[3].value, Value::Int(2));
        }

        #[test]
        fn generates_range_from_integer() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| {
                if let Value::Int(n) = v {
                    (0..*n).map(|i| Value::Int(i)).collect()
                } else {
                    vec![]
                }
            });
            let input = vec![Traverser::new(Value::Int(3))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
            assert_eq!(output[0].value, Value::Int(0));
            assert_eq!(output[1].value, Value::Int(1));
            assert_eq!(output[2].value, Value::Int(2));
        }

        #[test]
        fn can_filter_out_values_by_returning_empty_vec() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Only keep positive integers, filter out others
            let step = FlatMapStep::new(|_ctx, v| {
                if let Value::Int(n) = v {
                    if *n > 0 {
                        vec![v.clone()]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            });
            let input = vec![
                Traverser::new(Value::Int(1)),
                Traverser::new(Value::Int(-2)),
                Traverser::new(Value::Int(3)),
                Traverser::new(Value::String("hello".to_string())),
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::Int(1));
            assert_eq!(output[1].value, Value::Int(3));
        }

        #[test]
        fn can_access_execution_context() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Get all properties of a vertex as separate values
            let step = FlatMapStep::new(|ctx, v| {
                if let Value::Vertex(id) = v {
                    if let Some(vertex) = ctx.snapshot().storage().get_vertex(*id) {
                        vertex.properties.values().cloned().collect()
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            });
            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice with name and age

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            let values: Vec<Value> = output.iter().map(|t| t.value.clone()).collect();
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::Int(30)));
        }
    }

    mod flatmap_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_on_split() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone(), v.clone()]);

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.extend_path_labeled("start");

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert!(output[0].path.has_label("start"));
            assert!(output[1].path.has_label("start"));
        }

        #[test]
        fn preserves_loops_count_on_split() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone(), v.clone()]);

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.loops = 7;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].loops, 7);
            assert_eq!(output[1].loops, 7);
        }

        #[test]
        fn preserves_bulk_count_on_split() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone(), v.clone()]);

            let mut traverser = Traverser::new(Value::Int(42));
            traverser.bulk = 15;

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].bulk, 15);
            assert_eq!(output[1].bulk, 15);
        }
    }

    mod flatmap_step_empty_tests {
        use super::*;

        #[test]
        fn empty_input_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, v| vec![v.clone()]);
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn empty_vec_result_produces_no_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = FlatMapStep::new(|_ctx, _v| vec![]);
            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    // =========================================================================
    // ConstantStep Tests
    // =========================================================================

    mod constant_step_construction {
        use super::*;

        #[test]
        fn new_creates_step_with_string() {
            let step = ConstantStep::new("hello");
            assert_eq!(step.value(), &Value::String("hello".to_string()));
            assert_eq!(step.name(), "constant");
        }

        #[test]
        fn new_creates_step_with_int() {
            let step = ConstantStep::new(42i64);
            assert_eq!(step.value(), &Value::Int(42));
        }

        #[test]
        fn new_creates_step_with_float() {
            let step = ConstantStep::new(3.14f64);
            assert_eq!(step.value(), &Value::Float(3.14));
        }

        #[test]
        fn new_creates_step_with_bool() {
            let step = ConstantStep::new(true);
            assert_eq!(step.value(), &Value::Bool(true));
        }

        #[test]
        fn new_creates_step_with_value() {
            let step = ConstantStep::new(Value::Null);
            assert_eq!(step.value(), &Value::Null);
        }

        #[test]
        fn clone_box_works() {
            let step = ConstantStep::new("test");
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "constant");
        }

        #[test]
        fn debug_format() {
            let step = ConstantStep::new("debug_value");
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("ConstantStep"));
            assert!(debug_str.contains("debug_value"));
        }
    }

    mod constant_step_transform_tests {
        use super::*;

        #[test]
        fn replaces_single_value_with_constant() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = ConstantStep::new("constant");
            let input: Vec<Traverser> = vec![];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    // =========================================================================
    // PathStep Tests
    // =========================================================================

    mod path_step_construction {
        use super::*;

        #[test]
        fn new_creates_step() {
            let step = PathStep::new();
            assert_eq!(step.name(), "path");
        }

        #[test]
        fn default_creates_step() {
            let step = PathStep::default();
            assert_eq!(step.name(), "path");
        }

        #[test]
        fn clone_box_works() {
            let step = PathStep::new();
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "path");
        }

        #[test]
        fn debug_format() {
            let step = PathStep::new();
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("PathStep"));
        }
    }

    mod path_step_empty_path_tests {
        use super::*;

        #[test]
        fn empty_path_produces_empty_list() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PathStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert_eq!(output[0].value, Value::List(vec![]));
        }

        #[test]
        fn multiple_traversers_with_empty_paths() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PathStep::new();
            let input = vec![Traverser::new(Value::Int(1)), Traverser::new(Value::Int(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
            assert_eq!(output[0].value, Value::List(vec![]));
            assert_eq!(output[1].value, Value::List(vec![]));
        }
    }

    mod path_step_with_elements_tests {
        use super::*;
        use crate::traversal::PathValue;

        #[test]
        fn path_with_single_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PathStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(0));
            traverser
                .path
                .push_unlabeled(PathValue::Vertex(VertexId(0)));

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            let path_list = &output[0].value;
            if let Value::List(elements) = path_list {
                assert_eq!(elements.len(), 1);
                assert_eq!(elements[0], Value::Vertex(VertexId(0)));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn path_with_multiple_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PathStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(2));
            traverser
                .path
                .push_unlabeled(PathValue::Vertex(VertexId(0)));
            traverser
                .path
                .push_unlabeled(PathValue::Vertex(VertexId(1)));
            traverser
                .path
                .push_unlabeled(PathValue::Vertex(VertexId(2)));

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(elements) = &output[0].value {
                assert_eq!(elements.len(), 3);
                assert_eq!(elements[0], Value::Vertex(VertexId(0)));
                assert_eq!(elements[1], Value::Vertex(VertexId(1)));
                assert_eq!(elements[2], Value::Vertex(VertexId(2)));
            } else {
                panic!("Expected Value::List");
            }
        }

        #[test]
        fn path_with_mixed_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PathStep::new();

            let mut traverser = Traverser::from_vertex(VertexId(1));
            traverser
                .path
                .push_unlabeled(PathValue::Vertex(VertexId(0)));
            traverser.path.push_unlabeled(PathValue::Edge(EdgeId(0)));
            traverser
                .path
                .push_unlabeled(PathValue::Vertex(VertexId(1)));

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
        fn path_with_property_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PathStep::new();

            let mut traverser = Traverser::new(Value::String("name".to_string()));
            traverser
                .path
                .push_unlabeled(PathValue::Vertex(VertexId(0)));
            traverser
                .path
                .push_unlabeled(PathValue::Property(Value::String("Alice".to_string())));

            let input = vec![traverser];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::List(elements) = &output[0].value {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], Value::Vertex(VertexId(0)));
                assert_eq!(elements[1], Value::String("Alice".to_string()));
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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

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
        fn multiple_traversers_with_different_paths() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PathStep::new();

            let mut t1 = Traverser::from_vertex(VertexId(0));
            t1.path.push_unlabeled(PathValue::Vertex(VertexId(0)));

            let mut t2 = Traverser::from_vertex(VertexId(1));
            t2.path.push_unlabeled(PathValue::Vertex(VertexId(0)));
            t2.path.push_unlabeled(PathValue::Vertex(VertexId(1)));

            let t3 = Traverser::from_vertex(VertexId(2));
            // t3 has an empty path

            let input = vec![t1, t2, t3];
            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);

            // First traverser: path with 1 element
            if let Value::List(elements) = &output[0].value {
                assert_eq!(elements.len(), 1);
            } else {
                panic!("Expected Value::List for first traverser");
            }

            // Second traverser: path with 2 elements
            if let Value::List(elements) = &output[1].value {
                assert_eq!(elements.len(), 2);
            } else {
                panic!("Expected Value::List for second traverser");
            }

            // Third traverser: empty path
            if let Value::List(elements) = &output[2].value {
                assert_eq!(elements.len(), 0);
            } else {
                panic!("Expected Value::List for third traverser");
            }
        }
    }

    // =========================================================================
    // PropertiesStep Tests
    // =========================================================================

    mod properties_step_construction {
        use super::*;

        #[test]
        fn new_creates_step_for_all_properties() {
            let step = PropertiesStep::new();
            assert_eq!(step.name(), "properties");
        }

        #[test]
        fn default_creates_step_for_all_properties() {
            let step = PropertiesStep::default();
            assert_eq!(step.name(), "properties");
        }

        #[test]
        fn with_keys_creates_step_for_specific_keys() {
            let step = PropertiesStep::with_keys(vec!["name".to_string(), "age".to_string()]);
            assert_eq!(step.name(), "properties");
        }

        #[test]
        fn from_keys_creates_step_from_iterator() {
            let step = PropertiesStep::from_keys(["name", "age", "email"]);
            assert_eq!(step.name(), "properties");
        }

        #[test]
        fn clone_box_works() {
            let step = PropertiesStep::new();
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "properties");
        }

        #[test]
        fn debug_format() {
            let step = PropertiesStep::with_keys(vec!["name".to_string()]);
            let debug_str = format!("{:?}", step);
            assert!(debug_str.contains("PropertiesStep"));
            assert!(debug_str.contains("name"));
        }
    }

    mod properties_step_vertex_tests {
        use super::*;

        fn is_property_map(value: &Value, expected_key: &str, expected_value: &Value) -> bool {
            if let Value::Map(map) = value {
                let key_matches = map.get("key") == Some(&Value::String(expected_key.to_string()));
                let value_matches = map.get("value") == Some(expected_value);
                key_matches && value_matches
            } else {
                false
            }
        }

        #[test]
        fn extracts_all_properties_from_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice with name and age

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Alice has 2 properties: name and age
            assert_eq!(output.len(), 2);

            // Check that we got both properties as maps
            let values: Vec<&Value> = output.iter().map(|t| &t.value).collect();

            let has_name = values
                .iter()
                .any(|v| is_property_map(v, "name", &Value::String("Alice".to_string())));
            let has_age = values
                .iter()
                .any(|v| is_property_map(v, "age", &Value::Int(30)));

            assert!(has_name, "Expected to find name property");
            assert!(has_age, "Expected to find age property");
        }

        #[test]
        fn extracts_specific_property_from_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec!["name".to_string()]);
            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(
                is_property_map(
                    &output[0].value,
                    "name",
                    &Value::String("Alice".to_string())
                ),
                "Expected property map with key='name' and value='Alice'"
            );
        }

        #[test]
        fn extracts_multiple_specific_properties() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec!["name".to_string(), "age".to_string()]);
            let input = vec![Traverser::from_vertex(VertexId(0))]; // Alice

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 2);
        }

        #[test]
        fn skips_missing_properties() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec!["age".to_string()]);
            let input = vec![
                Traverser::from_vertex(VertexId(0)), // Alice has age
                Traverser::from_vertex(VertexId(1)), // Bob has no age
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only Alice has "age" property
            assert_eq!(output.len(), 1);
            assert!(is_property_map(&output[0].value, "age", &Value::Int(30)));
        }

        #[test]
        fn vertex_with_no_properties_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::from_vertex(VertexId(3))]; // Company with no properties

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn nonexistent_property_key_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec!["nonexistent".to_string()]);
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn nonexistent_vertex_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::from_vertex(VertexId(999))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn properties_from_multiple_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec!["name".to_string()]);
            let input = vec![
                Traverser::from_vertex(VertexId(0)), // Alice
                Traverser::from_vertex(VertexId(1)), // Bob
                Traverser::from_vertex(VertexId(2)), // Graph DB
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 3);
        }
    }

    mod properties_step_edge_tests {
        use super::*;

        fn is_property_map(value: &Value, expected_key: &str, expected_value: &Value) -> bool {
            if let Value::Map(map) = value {
                let key_matches = map.get("key") == Some(&Value::String(expected_key.to_string()));
                let value_matches = map.get("value") == Some(expected_value);
                key_matches && value_matches
            } else {
                false
            }
        }

        #[test]
        fn extracts_all_properties_from_edge() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::from_edge(EdgeId(0))]; // knows edge with since and weight

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Edge has 2 properties: since and weight
            assert_eq!(output.len(), 2);

            let values: Vec<&Value> = output.iter().map(|t| &t.value).collect();
            let has_since = values
                .iter()
                .any(|v| is_property_map(v, "since", &Value::Int(2020)));
            let has_weight = values
                .iter()
                .any(|v| is_property_map(v, "weight", &Value::Float(0.8)));

            assert!(has_since, "Expected to find since property");
            assert!(has_weight, "Expected to find weight property");
        }

        #[test]
        fn extracts_specific_property_from_edge() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec!["since".to_string()]);
            let input = vec![Traverser::from_edge(EdgeId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            assert!(is_property_map(
                &output[0].value,
                "since",
                &Value::Int(2020)
            ));
        }

        #[test]
        fn edge_with_no_properties_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::from_edge(EdgeId(1))]; // uses edge with no properties

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn nonexistent_edge_returns_empty() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::from_edge(EdgeId(999))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod properties_step_non_element_tests {
        use super::*;

        #[test]
        fn filters_out_integer_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::new(Value::Int(42))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_string_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::new(Value::String("hello".to_string()))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn filters_out_null_values() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new();
            let input = vec![Traverser::new(Value::Null)];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }

        #[test]
        fn mixed_elements_and_non_elements() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec!["name".to_string()]);
            let input = vec![
                Traverser::from_vertex(VertexId(0)), // Alice - has name
                Traverser::new(Value::Int(42)),      // filtered out
                Traverser::from_vertex(VertexId(1)), // Bob - has name
            ];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            // Only Alice and Bob should produce output
            assert_eq!(output.len(), 2);
        }
    }

    mod properties_step_metadata_tests {
        use super::*;

        #[test]
        fn preserves_path_from_input_traverser() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec!["name".to_string()]);

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

            let step = PropertiesStep::with_keys(vec!["name".to_string()]);

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

            let step = PropertiesStep::with_keys(vec!["name".to_string()]);

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
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::new(); // All properties

            let mut traverser = Traverser::from_vertex(VertexId(0)); // Alice: name and age
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

        #[test]
        fn empty_keys_returns_empty_output() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec![]);
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert!(output.is_empty());
        }
    }

    mod properties_step_property_map_structure {
        use super::*;

        #[test]
        fn property_map_has_correct_structure() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            let step = PropertiesStep::with_keys(vec!["name".to_string()]);
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                // Should have exactly 2 keys: "key" and "value"
                assert_eq!(map.len(), 2);
                assert!(map.contains_key("key"));
                assert!(map.contains_key("value"));

                // "key" should be a string with the property key name
                assert_eq!(map.get("key"), Some(&Value::String("name".to_string())));

                // "value" should be the actual property value
                assert_eq!(map.get("value"), Some(&Value::String("Alice".to_string())));
            } else {
                panic!("Expected Value::Map");
            }
        }

        #[test]
        fn property_map_works_with_different_value_types() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let ctx = ExecutionContext::new(&snapshot, snapshot.interner());

            // Test with integer value (age)
            let step = PropertiesStep::with_keys(vec!["age".to_string()]);
            let input = vec![Traverser::from_vertex(VertexId(0))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.get("key"), Some(&Value::String("age".to_string())));
                assert_eq!(map.get("value"), Some(&Value::Int(30)));
            } else {
                panic!("Expected Value::Map");
            }

            // Test with float value (version from software vertex)
            let step = PropertiesStep::with_keys(vec!["version".to_string()]);
            let input = vec![Traverser::from_vertex(VertexId(2))];

            let output: Vec<Traverser> = step.apply(&ctx, Box::new(input.into_iter())).collect();

            assert_eq!(output.len(), 1);
            if let Value::Map(map) = &output[0].value {
                assert_eq!(map.get("key"), Some(&Value::String("version".to_string())));
                assert_eq!(map.get("value"), Some(&Value::Float(1.0)));
            } else {
                panic!("Expected Value::Map");
            }
        }
    }
}
