//! Mutation steps for graph modification.
//!
//! This module provides traversal steps for mutating the graph:
//!
//! - [`AddVStep`]: Create new vertices
//! - [`AddEStep`]: Create new edges
//! - [`PropertyStep`]: Add/update properties on vertices and edges
//! - [`DropStep`]: Delete vertices and edges
//!
//! # Mutation Execution Model
//!
//! Unlike read-only traversal steps that work with lazy iterators,
//! mutation steps collect pending changes and execute them when
//! terminal steps like `iterate()` or `next()` are called.
//!
//! # Example
//!
//! ```ignore
//! use interstellar::prelude::*;
//!
//! // Create a vertex with properties
//! let vertex = g.add_v("person")
//!     .property("name", "Alice")
//!     .property("age", 30)
//!     .next()?;
//!
//! // Create an edge
//! let edge = g.v_id(alice_id)
//!     .add_e("knows")
//!     .to_vertex(bob_id)
//!     .property("since", 2020)
//!     .next()?;
//!
//! // Delete elements
//! g.v_id(alice_id).drop().iterate();
//! ```

use std::collections::HashMap;

use crate::error::MutationError;
use crate::traversal::step::Step;
use crate::traversal::{ExecutionContext, Traverser};
use crate::value::{EdgeId, Value, VertexId};

// -----------------------------------------------------------------------------
// AddVStep - Create new vertex
// -----------------------------------------------------------------------------

/// Step that creates a new vertex with the specified label.
///
/// This is a **spawning step** - it produces a traverser for the newly
/// created vertex, ignoring any input traversers.
///
/// # Properties
///
/// Properties can be attached via the [`PropertyStep`] after this step.
///
/// # Example
///
/// ```ignore
/// // In the API, this is called via g.add_v()
/// let step = AddVStep::new("person");
/// ```
#[derive(Clone, Debug)]
pub struct AddVStep {
    label: String,
    properties: HashMap<String, Value>,
}

impl AddVStep {
    /// Create a new AddVStep with the given label.
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            properties: HashMap::new(),
        }
    }

    /// Create a new AddVStep with label and initial properties.
    pub fn with_properties(label: impl Into<String>, properties: HashMap<String, Value>) -> Self {
        Self {
            label: label.into(),
            properties,
        }
    }

    /// Get the label for the new vertex.
    #[inline]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Get the properties for the new vertex.
    #[inline]
    pub fn properties(&self) -> &HashMap<String, Value> {
        &self.properties
    }
}

impl Step for AddVStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        _input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        // Note: This is a placeholder. In a real implementation,
        // we need mutable access to the graph storage.
        // For now, we'll create a traverser that represents the "pending" vertex.
        //
        // The actual mutation happens at the MutationTraversalExecutor level
        // when terminal steps are called.

        // Get the vertex that would be created (for preview purposes)
        // In actual execution, the MutationContext handles the creation
        let label = self.label.clone();
        let properties = self.properties.clone();
        let track_paths = ctx.is_tracking_paths();

        // Create a placeholder traverser with the intent to create a vertex
        // The actual vertex ID will be assigned during execution
        std::iter::once_with(move || {
            // Create a traverser with a placeholder value indicating pending vertex
            // This will be replaced with the actual vertex ID during mutation execution
            let mut t = Traverser::new(Value::Map(HashMap::from([
                ("__pending_add_v".to_string(), Value::Bool(true)),
                ("label".to_string(), Value::String(label.clone())),
                (
                    "properties".to_string(),
                    Value::Map(
                        properties
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    ),
                ),
            ])));
            if track_paths {
                t.extend_path_unlabeled();
            }
            t
        })
    }

    fn name(&self) -> &'static str {
        "addV"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // MUTATION STEP: AddVStep requires mutable graph access to create vertices.
        // StreamingContext only provides read-only GraphSnapshot access.
        // Mutations must be deferred and executed via BoundTraversal with graph lock.
        // Current behavior: pass-through (no mutation).
        Box::new(std::iter::once(input))
    }
}

// -----------------------------------------------------------------------------
// PropertyStep - Add/update property on current element
// -----------------------------------------------------------------------------

/// Step that adds or updates a property on the current element.
///
/// This step modifies the current traverser's element (vertex or edge)
/// by setting a property value.
///
/// # Behavior
///
/// - If the property exists, its value is updated
/// - If the property doesn't exist, it is created
/// - Non-element values pass through unchanged (no-op)
///
/// # Example
///
/// ```ignore
/// // In the API, this is called via .property()
/// let step = PropertyStep::new("name", Value::String("Alice".into()));
/// ```
#[derive(Clone, Debug)]
pub struct PropertyStep {
    key: String,
    value: Value,
}

impl PropertyStep {
    /// Create a new PropertyStep with the given key and value.
    pub fn new(key: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Get the property key.
    #[inline]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the property value.
    #[inline]
    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl Step for PropertyStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let key = self.key.clone();
        let value = self.value.clone();

        input.map(move |mut t| {
            // Check if this is a pending add_v operation
            if let Value::Map(ref mut map) = t.value {
                if map.get("__pending_add_v").is_some() {
                    // Add property to the pending vertex's properties
                    if let Some(Value::Map(props)) = map.get_mut("properties") {
                        props.insert(key.clone(), value.clone());
                    }
                    return t;
                }
                if map.get("__pending_add_e").is_some() {
                    // Add property to the pending edge's properties
                    if let Some(Value::Map(props)) = map.get_mut("properties") {
                        props.insert(key.clone(), value.clone());
                    }
                    return t;
                }
            }

            // For existing elements, we mark the property update as pending
            // The actual update happens at the MutationContext level
            match &t.value {
                Value::Vertex(id) => {
                    // Mark as pending property update for vertex
                    t.value = Value::Map(HashMap::from([
                        ("__pending_property_vertex".to_string(), Value::Bool(true)),
                        ("id".to_string(), Value::Vertex(*id)),
                        ("key".to_string(), Value::String(key.clone())),
                        ("value".to_string(), value.clone()),
                    ]));
                }
                Value::Edge(id) => {
                    // Mark as pending property update for edge
                    t.value = Value::Map(HashMap::from([
                        ("__pending_property_edge".to_string(), Value::Bool(true)),
                        ("id".to_string(), Value::Edge(*id)),
                        ("key".to_string(), Value::String(key.clone())),
                        ("value".to_string(), value.clone()),
                    ]));
                }
                _ => {
                    // Non-element values pass through (no-op)
                }
            }
            t
        })
    }

    fn name(&self) -> &'static str {
        "property"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // MUTATION STEP: PropertyStep requires mutable graph access to update properties.
        // StreamingContext only provides read-only GraphSnapshot access.
        // Mutations must be deferred and executed via BoundTraversal with graph lock.
        // Current behavior: pass-through (no mutation).
        Box::new(std::iter::once(input))
    }
}

// -----------------------------------------------------------------------------
// DropStep - Delete current element
// -----------------------------------------------------------------------------

/// Step that deletes the current element (vertex or edge).
///
/// When a vertex is dropped, all its incident edges are also dropped.
///
/// # Behavior
///
/// - The step consumes the traverser (produces no output)
/// - Vertex deletion cascades to edge deletion
/// - Non-element values are silently ignored
///
/// # Example
///
/// ```ignore
/// // In the API, this is called via .drop()
/// let step = DropStep::new();
/// ```
#[derive(Clone, Debug, Default)]
pub struct DropStep;

impl DropStep {
    /// Create a new DropStep.
    pub fn new() -> Self {
        Self
    }
}

impl Step for DropStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        // Drop step marks elements for deletion and produces no output
        input.filter_map(move |t| {
            match &t.value {
                Value::Vertex(id) => {
                    // Mark as pending vertex deletion
                    Some(Traverser::new(Value::Map(HashMap::from([
                        ("__pending_drop_vertex".to_string(), Value::Bool(true)),
                        ("id".to_string(), Value::Vertex(*id)),
                    ]))))
                }
                Value::Edge(id) => {
                    // Mark as pending edge deletion
                    Some(Traverser::new(Value::Map(HashMap::from([
                        ("__pending_drop_edge".to_string(), Value::Bool(true)),
                        ("id".to_string(), Value::Edge(*id)),
                    ]))))
                }
                _ => {
                    // Non-element values are ignored
                    None
                }
            }
        })
    }

    fn name(&self) -> &'static str {
        "drop"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // MUTATION STEP: DropStep requires mutable graph access to delete elements.
        // StreamingContext only provides read-only GraphSnapshot access.
        // Mutations must be deferred and executed via BoundTraversal with graph lock.
        // Current behavior: pass-through (no mutation).
        Box::new(std::iter::once(input))
    }
}

// -----------------------------------------------------------------------------
// AddEStep - Create new edge
// -----------------------------------------------------------------------------

/// Specifies the source or target vertex for an edge.
#[derive(Clone, Debug)]
pub enum EdgeEndpoint {
    /// A specific vertex ID.
    VertexId(VertexId),
    /// The current traverser (implicit from context).
    Traverser,
    /// A step label referencing a previously labeled vertex.
    StepLabel(String),
}

/// Step that creates a new edge with the specified label.
///
/// This step requires both `from` and `to` endpoints to be specified.
/// The edge is created connecting these two vertices.
///
/// # Example
///
/// ```ignore
/// // In the API, this is called via .add_e()
/// let step = AddEStep::new("knows")
///     .from_vertex(VertexId(1))
///     .to_vertex(VertexId(2));
/// ```
#[derive(Clone, Debug)]
pub struct AddEStep {
    label: String,
    from: Option<EdgeEndpoint>,
    to: Option<EdgeEndpoint>,
    properties: HashMap<String, Value>,
}

impl AddEStep {
    /// Create a new AddEStep with the given label.
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            from: None,
            to: None,
            properties: HashMap::new(),
        }
    }

    /// Set the source vertex by ID.
    pub fn from_vertex(mut self, id: VertexId) -> Self {
        self.from = Some(EdgeEndpoint::VertexId(id));
        self
    }

    /// Set the source vertex from the current traverser.
    pub fn from_traverser(mut self) -> Self {
        self.from = Some(EdgeEndpoint::Traverser);
        self
    }

    /// Set the source vertex from a step label.
    pub fn from_label(mut self, label: impl Into<String>) -> Self {
        self.from = Some(EdgeEndpoint::StepLabel(label.into()));
        self
    }

    /// Set the target vertex by ID.
    pub fn to_vertex(mut self, id: VertexId) -> Self {
        self.to = Some(EdgeEndpoint::VertexId(id));
        self
    }

    /// Set the target vertex from the current traverser.
    pub fn to_traverser(mut self) -> Self {
        self.to = Some(EdgeEndpoint::Traverser);
        self
    }

    /// Set the target vertex from a step label.
    pub fn to_label(mut self, label: impl Into<String>) -> Self {
        self.to = Some(EdgeEndpoint::StepLabel(label.into()));
        self
    }

    /// Add a property to the edge.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Get the edge label.
    #[inline]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Get the from endpoint.
    #[inline]
    pub fn from_endpoint(&self) -> Option<&EdgeEndpoint> {
        self.from.as_ref()
    }

    /// Get the to endpoint.
    #[inline]
    pub fn to_endpoint(&self) -> Option<&EdgeEndpoint> {
        self.to.as_ref()
    }

    /// Resolve an endpoint to a vertex ID.
    fn resolve_endpoint(
        endpoint: &EdgeEndpoint,
        traverser: &Traverser,
    ) -> Result<VertexId, MutationError> {
        match endpoint {
            EdgeEndpoint::VertexId(id) => Ok(*id),
            EdgeEndpoint::Traverser => {
                traverser
                    .as_vertex_id()
                    .ok_or(MutationError::MissingEdgeEndpoint(
                        "traverser is not a vertex",
                    ))
            }
            EdgeEndpoint::StepLabel(label) => {
                // Look up the labeled value in the path
                if let Some(values) = traverser.path.get(label) {
                    values
                        .first()
                        .and_then(|pv| pv.as_vertex_id())
                        .ok_or_else(|| MutationError::StepLabelNotVertex(label.clone()))
                } else {
                    Err(MutationError::StepLabelNotFound(label.clone()))
                }
            }
        }
    }
}

impl Step for AddEStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let label = self.label.clone();
        let from = self.from.clone();
        let to = self.to.clone();
        let properties = self.properties.clone();
        let track_paths = ctx.is_tracking_paths();

        // Check if both endpoints are explicit VertexIds - in this case we don't need input traversers
        let explicit_endpoints = matches!(
            (&from, &to),
            (
                Some(EdgeEndpoint::VertexId(_)),
                Some(EdgeEndpoint::VertexId(_))
            )
        );

        if explicit_endpoints {
            let from_id = match &from {
                Some(EdgeEndpoint::VertexId(id)) => *id,
                _ => unreachable!(),
            };
            let to_id = match &to {
                Some(EdgeEndpoint::VertexId(id)) => *id,
                _ => unreachable!(),
            };
            // Return iterator for explicit endpoints case
            let iter: Box<dyn Iterator<Item = Traverser> + 'a> =
                Box::new(std::iter::once_with(move || {
                    let mut new_t = Traverser::new(Value::Map(HashMap::from([
                        ("__pending_add_e".to_string(), Value::Bool(true)),
                        ("label".to_string(), Value::String(label.clone())),
                        ("from".to_string(), Value::Vertex(from_id)),
                        ("to".to_string(), Value::Vertex(to_id)),
                        (
                            "properties".to_string(),
                            Value::Map(
                                properties
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect(),
                            ),
                        ),
                    ])));
                    if track_paths {
                        new_t.extend_path_unlabeled();
                    }
                    new_t
                }));
            return iter;
        }

        // Otherwise, we need input traversers to resolve endpoints
        Box::new(input.filter_map(move |t| {
            // Resolve from endpoint (default to current traverser if not set)
            let from_endpoint = from.as_ref().unwrap_or(&EdgeEndpoint::Traverser);
            let to_endpoint = to.as_ref()?;

            let from_id = Self::resolve_endpoint(from_endpoint, &t).ok()?;
            let to_id = Self::resolve_endpoint(to_endpoint, &t).ok()?;

            // Create a pending edge marker
            let mut new_t = Traverser::new(Value::Map(HashMap::from([
                ("__pending_add_e".to_string(), Value::Bool(true)),
                ("label".to_string(), Value::String(label.clone())),
                ("from".to_string(), Value::Vertex(from_id)),
                ("to".to_string(), Value::Vertex(to_id)),
                (
                    "properties".to_string(),
                    Value::Map(
                        properties
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    ),
                ),
            ])));
            if track_paths {
                new_t.extend_path_unlabeled();
            }
            Some(new_t)
        }))
    }

    fn name(&self) -> &'static str {
        "addE"
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        // MUTATION STEP: AddEStep requires mutable graph access to create edges.
        // StreamingContext only provides read-only GraphSnapshot access.
        // Mutations must be deferred and executed via BoundTraversal with graph lock.
        // Current behavior: pass-through (no mutation).
        Box::new(std::iter::once(input))
    }
}

// -----------------------------------------------------------------------------
// PendingMutation - Represents a pending mutation operation
// -----------------------------------------------------------------------------

/// Represents a pending mutation that will be executed at terminal step.
#[derive(Clone, Debug)]
pub enum PendingMutation {
    /// Add a new vertex.
    AddVertex {
        label: String,
        properties: HashMap<String, Value>,
    },
    /// Add a new edge.
    AddEdge {
        label: String,
        from: VertexId,
        to: VertexId,
        properties: HashMap<String, Value>,
    },
    /// Set a property on a vertex.
    SetVertexProperty {
        id: VertexId,
        key: String,
        value: Value,
    },
    /// Set a property on an edge.
    SetEdgeProperty {
        id: EdgeId,
        key: String,
        value: Value,
    },
    /// Drop a vertex.
    DropVertex { id: VertexId },
    /// Drop an edge.
    DropEdge { id: EdgeId },
}

impl PendingMutation {
    /// Parse a pending mutation from a traverser value.
    ///
    /// Returns `None` if the value doesn't represent a pending mutation.
    pub fn from_value(value: &Value) -> Option<Self> {
        let map = match value {
            Value::Map(m) => m,
            _ => return None,
        };

        // Check for pending add_v
        if map.get("__pending_add_v").is_some() {
            let label = map
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let properties = map
                .get("properties")
                .and_then(|v| match v {
                    Value::Map(m) => Some(m.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            return Some(PendingMutation::AddVertex { label, properties });
        }

        // Check for pending add_e
        if map.get("__pending_add_e").is_some() {
            let label = map
                .get("label")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let from = map.get("from").and_then(|v| v.as_vertex_id())?;
            let to = map.get("to").and_then(|v| v.as_vertex_id())?;
            let properties = map
                .get("properties")
                .and_then(|v| match v {
                    Value::Map(m) => Some(m.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            return Some(PendingMutation::AddEdge {
                label,
                from,
                to,
                properties,
            });
        }

        // Check for pending property on vertex
        if map.get("__pending_property_vertex").is_some() {
            let id = map.get("id").and_then(|v| v.as_vertex_id())?;
            let key = map
                .get("key")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let value = map.get("value").cloned().unwrap_or(Value::Null);
            return Some(PendingMutation::SetVertexProperty { id, key, value });
        }

        // Check for pending property on edge
        if map.get("__pending_property_edge").is_some() {
            let id = map.get("id").and_then(|v| v.as_edge_id())?;
            let key = map
                .get("key")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let value = map.get("value").cloned().unwrap_or(Value::Null);
            return Some(PendingMutation::SetEdgeProperty { id, key, value });
        }

        // Check for pending drop vertex
        if map.get("__pending_drop_vertex").is_some() {
            let id = map.get("id").and_then(|v| v.as_vertex_id())?;
            return Some(PendingMutation::DropVertex { id });
        }

        // Check for pending drop edge
        if map.get("__pending_drop_edge").is_some() {
            let id = map.get("id").and_then(|v| v.as_edge_id())?;
            return Some(PendingMutation::DropEdge { id });
        }

        None
    }
}

// -----------------------------------------------------------------------------
// MutationExecutor - Executes pending mutations
// -----------------------------------------------------------------------------

/// Result of executing a mutation traversal.
///
/// Contains both the executed mutation results and any values that should
/// be returned to the user (e.g., newly created vertex/edge IDs).
#[derive(Debug)]
pub struct MutationResult {
    /// Values returned by the traversal (e.g., new VertexIds, EdgeIds)
    pub values: Vec<Value>,
    /// Count of vertices added
    pub vertices_added: usize,
    /// Count of edges added
    pub edges_added: usize,
    /// Count of vertices removed
    pub vertices_removed: usize,
    /// Count of edges removed
    pub edges_removed: usize,
    /// Count of properties set
    pub properties_set: usize,
}

impl MutationResult {
    /// Create a new empty mutation result.
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            vertices_added: 0,
            edges_added: 0,
            vertices_removed: 0,
            edges_removed: 0,
            properties_set: 0,
        }
    }
}

impl Default for MutationResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Executes pending mutations against a mutable graph storage.
///
/// This struct processes `PendingMutation` markers from traversal results
/// and applies them to the underlying storage.
///
/// # Example
///
/// ```ignore
/// use interstellar::storage::Graph;
/// use interstellar::traversal::mutation::MutationExecutor;
///
/// let graph = Graph::new();
/// let mut storage = graph.as_storage_mut();
/// let mut executor = MutationExecutor::new(&mut storage);
///
/// // Execute pending mutations from traversal
/// let result = executor.execute(traversers);
/// ```
pub struct MutationExecutor<'s, S: crate::storage::GraphStorageMut> {
    storage: &'s mut S,
}

impl<'s, S: crate::storage::GraphStorageMut> MutationExecutor<'s, S> {
    /// Create a new mutation executor with the given mutable storage.
    pub fn new(storage: &'s mut S) -> Self {
        Self { storage }
    }

    /// Execute pending mutations from a list of traversers.
    ///
    /// Processes each traverser's value, detecting pending mutation markers
    /// and applying the corresponding mutations to storage.
    ///
    /// Returns a `MutationResult` with the created elements and statistics.
    pub fn execute(
        &mut self,
        traversers: impl Iterator<Item = crate::traversal::Traverser>,
    ) -> MutationResult {
        let mut result = MutationResult::new();

        for traverser in traversers {
            if let Some(mutation) = PendingMutation::from_value(&traverser.value) {
                match mutation {
                    PendingMutation::AddVertex { label, properties } => {
                        let id = self.storage.add_vertex(&label, properties);
                        result.values.push(Value::Vertex(id));
                        result.vertices_added += 1;
                    }
                    PendingMutation::AddEdge {
                        label,
                        from,
                        to,
                        properties,
                    } => {
                        match self.storage.add_edge(from, to, &label, properties) {
                            Ok(id) => {
                                result.values.push(Value::Edge(id));
                                result.edges_added += 1;
                            }
                            Err(_) => {
                                // Edge creation failed (e.g., vertex not found)
                                // Silently skip for now
                            }
                        }
                    }
                    PendingMutation::SetVertexProperty { id, key, value } => {
                        if self.storage.set_vertex_property(id, &key, value).is_ok() {
                            result.properties_set += 1;
                            result.values.push(Value::Vertex(id));
                        }
                    }
                    PendingMutation::SetEdgeProperty { id, key, value } => {
                        if self.storage.set_edge_property(id, &key, value).is_ok() {
                            result.properties_set += 1;
                            result.values.push(Value::Edge(id));
                        }
                    }
                    PendingMutation::DropVertex { id } => {
                        if self.storage.remove_vertex(id).is_ok() {
                            result.vertices_removed += 1;
                        }
                    }
                    PendingMutation::DropEdge { id } => {
                        if self.storage.remove_edge(id).is_ok() {
                            result.edges_removed += 1;
                        }
                    }
                }
            } else {
                // Not a pending mutation, just pass through the value
                result.values.push(traverser.value);
            }
        }

        result
    }

    /// Execute a single pending mutation.
    pub fn execute_mutation(&mut self, mutation: PendingMutation) -> Option<Value> {
        match mutation {
            PendingMutation::AddVertex { label, properties } => {
                let id = self.storage.add_vertex(&label, properties);
                Some(Value::Vertex(id))
            }
            PendingMutation::AddEdge {
                label,
                from,
                to,
                properties,
            } => match self.storage.add_edge(from, to, &label, properties) {
                Ok(id) => Some(Value::Edge(id)),
                Err(_) => None,
            },
            PendingMutation::SetVertexProperty { id, key, value } => {
                self.storage.set_vertex_property(id, &key, value).ok()?;
                Some(Value::Vertex(id))
            }
            PendingMutation::SetEdgeProperty { id, key, value } => {
                self.storage.set_edge_property(id, &key, value).ok()?;
                Some(Value::Edge(id))
            }
            PendingMutation::DropVertex { id } => {
                self.storage.remove_vertex(id).ok()?;
                None // Drop doesn't return a value
            }
            PendingMutation::DropEdge { id } => {
                self.storage.remove_edge(id).ok()?;
                None // Drop doesn't return a value
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traversal::step::DynStep;

    #[test]
    fn add_v_step_new() {
        let step = AddVStep::new("person");
        assert_eq!(step.label(), "person");
        assert!(step.properties().is_empty());
        assert_eq!(step.name(), "addV");
    }

    #[test]
    fn add_v_step_with_properties() {
        let props = HashMap::from([
            ("name".to_string(), Value::String("Alice".into())),
            ("age".to_string(), Value::Int(30)),
        ]);
        let step = AddVStep::with_properties("person", props.clone());
        assert_eq!(step.label(), "person");
        assert_eq!(step.properties().len(), 2);
    }

    #[test]
    fn add_v_step_clone_box() {
        let step = AddVStep::new("person");
        let cloned = DynStep::clone_box(&step);
        assert_eq!(cloned.dyn_name(), "addV");
    }

    #[test]
    fn property_step_new() {
        let step = PropertyStep::new("name", "Alice");
        assert_eq!(step.key(), "name");
        assert_eq!(step.value(), &Value::String("Alice".to_string()));
        assert_eq!(step.name(), "property");
    }

    #[test]
    fn property_step_clone_box() {
        let step = PropertyStep::new("name", "Alice");
        let cloned = DynStep::clone_box(&step);
        assert_eq!(cloned.dyn_name(), "property");
    }

    #[test]
    fn drop_step_new() {
        let step = DropStep::new();
        assert_eq!(step.name(), "drop");
    }

    #[test]
    fn drop_step_clone_box() {
        let step = DropStep::new();
        let cloned = DynStep::clone_box(&step);
        assert_eq!(cloned.dyn_name(), "drop");
    }

    #[test]
    fn add_e_step_builder() {
        let step = AddEStep::new("knows")
            .from_vertex(VertexId(1))
            .to_vertex(VertexId(2))
            .property("since", 2020i64);

        assert_eq!(step.label(), "knows");
        assert!(matches!(
            step.from_endpoint(),
            Some(EdgeEndpoint::VertexId(VertexId(1)))
        ));
        assert!(matches!(
            step.to_endpoint(),
            Some(EdgeEndpoint::VertexId(VertexId(2)))
        ));
        assert_eq!(step.name(), "addE");
    }

    #[test]
    fn add_e_step_from_traverser() {
        let step = AddEStep::new("knows")
            .from_traverser()
            .to_vertex(VertexId(2));

        assert!(matches!(
            step.from_endpoint(),
            Some(EdgeEndpoint::Traverser)
        ));
    }

    #[test]
    fn add_e_step_from_label() {
        let step = AddEStep::new("knows").from_label("start").to_label("end");

        assert!(matches!(
            step.from_endpoint(),
            Some(EdgeEndpoint::StepLabel(ref s)) if s == "start"
        ));
        assert!(matches!(
            step.to_endpoint(),
            Some(EdgeEndpoint::StepLabel(ref s)) if s == "end"
        ));
    }

    #[test]
    fn add_e_step_clone_box() {
        let step = AddEStep::new("knows");
        let cloned = DynStep::clone_box(&step);
        assert_eq!(cloned.dyn_name(), "addE");
    }

    #[test]
    fn pending_mutation_from_add_v() {
        let value = Value::Map(HashMap::from([
            ("__pending_add_v".to_string(), Value::Bool(true)),
            ("label".to_string(), Value::String("person".to_string())),
            (
                "properties".to_string(),
                Value::Map(HashMap::from([(
                    "name".to_string(),
                    Value::String("Alice".to_string()),
                )])),
            ),
        ]));

        let mutation = PendingMutation::from_value(&value);
        assert!(matches!(
            mutation,
            Some(PendingMutation::AddVertex { label, properties })
            if label == "person" && properties.len() == 1
        ));
    }

    #[test]
    fn pending_mutation_from_add_e() {
        let value = Value::Map(HashMap::from([
            ("__pending_add_e".to_string(), Value::Bool(true)),
            ("label".to_string(), Value::String("knows".to_string())),
            ("from".to_string(), Value::Vertex(VertexId(1))),
            ("to".to_string(), Value::Vertex(VertexId(2))),
            ("properties".to_string(), Value::Map(HashMap::new())),
        ]));

        let mutation = PendingMutation::from_value(&value);
        assert!(matches!(
            mutation,
            Some(PendingMutation::AddEdge { label, from, to, .. })
            if label == "knows" && from == VertexId(1) && to == VertexId(2)
        ));
    }

    #[test]
    fn pending_mutation_from_drop_vertex() {
        let value = Value::Map(HashMap::from([
            ("__pending_drop_vertex".to_string(), Value::Bool(true)),
            ("id".to_string(), Value::Vertex(VertexId(42))),
        ]));

        let mutation = PendingMutation::from_value(&value);
        assert!(matches!(
            mutation,
            Some(PendingMutation::DropVertex { id })
            if id == VertexId(42)
        ));
    }

    #[test]
    fn pending_mutation_from_regular_value() {
        // Regular values should not be parsed as mutations
        assert!(PendingMutation::from_value(&Value::Int(42)).is_none());
        assert!(PendingMutation::from_value(&Value::String("test".into())).is_none());
        assert!(PendingMutation::from_value(&Value::Vertex(VertexId(1))).is_none());
    }
}
