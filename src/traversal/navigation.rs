//! Navigation steps for graph traversal.
//!
//! This module provides steps for navigating the graph structure:
//! - `OutStep`, `InStep`, `BothStep` - traverse to adjacent vertices
//! - `OutEStep`, `InEStep`, `BothEStep` - traverse to incident edges
//! - `OutVStep`, `InVStep`, `BothVStep` - traverse from edges to vertices
//!
//! Navigation steps are "flatmap" operations - they can produce zero or more
//! output traversers for each input traverser.
//!
//! # Example
//!
//! ```ignore
//! // Traverse to outgoing neighbors
//! let neighbors = g.v_ids([VertexId(1)]).out().to_list();
//!
//! // Traverse with edge label filter
//! let knows = g.v().out_labels(&["knows"]).to_list();
//!
//! // Traverse to edges, then to target vertices
//! let targets = g.v().out_e().in_v().to_list();
//! ```

use crate::traversal::context::ExecutionContext;
use crate::traversal::step::AnyStep;
use crate::traversal::Traverser;
use crate::value::Value;

// -----------------------------------------------------------------------------
// OutStep - traverse to outgoing adjacent vertices
// -----------------------------------------------------------------------------

/// Traverse to outgoing adjacent vertices.
///
/// From a vertex, follows all outgoing edges and returns the target vertices.
/// Optionally filters by edge label.
///
/// Non-vertex traversers produce no output (empty iterator).
///
/// # Example
///
/// ```ignore
/// // All outgoing neighbors
/// let neighbors = g.v().out().to_list();
///
/// // Filter by edge label
/// let knows = g.v().out_labels(&["knows"]).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct OutStep {
    /// Optional edge labels to filter by
    labels: Vec<String>,
}

impl OutStep {
    /// Create a new OutStep with no label filter.
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    /// Create a new OutStep with label filtering.
    ///
    /// Only edges with one of the given labels will be traversed.
    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }
}

impl Default for OutStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for OutStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Pre-resolve labels once (not per-traverser) for efficiency
        let label_ids: Vec<u32> = if self.labels.is_empty() {
            Vec::new()
        } else {
            ctx.resolve_labels(&self.labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        };

        // If labels were specified but none resolved, return empty iterator
        if !self.labels.is_empty() && label_ids.is_empty() {
            return Box::new(std::iter::empty());
        }

        let track_paths = ctx.is_tracking_paths();

        // Return lazy iterator - no .collect()!
        Box::new(input.flat_map(move |t| {
            let vertex_id = match t.as_vertex_id() {
                Some(id) => id,
                None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            };

            let label_ids = label_ids.clone();
            Box::new(ctx.storage().out_edges(vertex_id).filter_map(move |edge| {
                // Filter by label if specified
                if !label_ids.is_empty() {
                    let edge_label_id = ctx.interner().lookup(&edge.label)?;
                    if !label_ids.contains(&edge_label_id) {
                        return None;
                    }
                }
                // Get target vertex
                let mut new_t = t.split(Value::Vertex(edge.dst));
                if track_paths {
                    new_t.extend_path_unlabeled();
                }
                Some(new_t)
            })) as Box<dyn Iterator<Item = Traverser>>
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "out"
    }
}

// -----------------------------------------------------------------------------
// InStep - traverse to incoming adjacent vertices
// -----------------------------------------------------------------------------

/// Traverse to incoming adjacent vertices.
///
/// From a vertex, follows all incoming edges and returns the source vertices.
/// Optionally filters by edge label.
///
/// Non-vertex traversers produce no output (empty iterator).
///
/// # Example
///
/// ```ignore
/// // All incoming neighbors
/// let neighbors = g.v().in_().to_list();
///
/// // Filter by edge label
/// let known_by = g.v().in_labels(&["knows"]).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct InStep {
    /// Optional edge labels to filter by
    labels: Vec<String>,
}

impl InStep {
    /// Create a new InStep with no label filter.
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    /// Create a new InStep with label filtering.
    ///
    /// Only edges with one of the given labels will be traversed.
    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }
}

impl Default for InStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for InStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Pre-resolve labels once (not per-traverser) for efficiency
        let label_ids: Vec<u32> = if self.labels.is_empty() {
            Vec::new()
        } else {
            ctx.resolve_labels(&self.labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        };

        // If labels were specified but none resolved, return empty iterator
        if !self.labels.is_empty() && label_ids.is_empty() {
            return Box::new(std::iter::empty());
        }

        let track_paths = ctx.is_tracking_paths();

        // Return lazy iterator - no .collect()!
        Box::new(input.flat_map(move |t| {
            let vertex_id = match t.as_vertex_id() {
                Some(id) => id,
                None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            };

            let label_ids = label_ids.clone();
            Box::new(ctx.storage().in_edges(vertex_id).filter_map(move |edge| {
                // Filter by label if specified
                if !label_ids.is_empty() {
                    let edge_label_id = ctx.interner().lookup(&edge.label)?;
                    if !label_ids.contains(&edge_label_id) {
                        return None;
                    }
                }
                // Get source vertex
                let mut new_t = t.split(Value::Vertex(edge.src));
                if track_paths {
                    new_t.extend_path_unlabeled();
                }
                Some(new_t)
            })) as Box<dyn Iterator<Item = Traverser>>
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "in"
    }
}

// -----------------------------------------------------------------------------
// BothStep - traverse to adjacent vertices in both directions
// -----------------------------------------------------------------------------

/// Traverse to adjacent vertices in both directions.
///
/// From a vertex, follows all edges (both outgoing and incoming) and returns
/// the adjacent vertices. Optionally filters by edge label.
///
/// Non-vertex traversers produce no output (empty iterator).
///
/// # Example
///
/// ```ignore
/// // All neighbors (both directions)
/// let neighbors = g.v().both().to_list();
///
/// // Filter by edge label
/// let connected = g.v().both_labels(&["knows"]).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct BothStep {
    /// Optional edge labels to filter by
    labels: Vec<String>,
}

impl BothStep {
    /// Create a new BothStep with no label filter.
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    /// Create a new BothStep with label filtering.
    ///
    /// Only edges with one of the given labels will be traversed.
    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }
}

impl Default for BothStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for BothStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Pre-resolve labels once (not per-traverser) for efficiency
        let label_ids: Vec<u32> = if self.labels.is_empty() {
            Vec::new()
        } else {
            ctx.resolve_labels(&self.labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        };

        // If labels were specified but none resolved, return empty iterator
        if !self.labels.is_empty() && label_ids.is_empty() {
            return Box::new(std::iter::empty());
        }

        let track_paths = ctx.is_tracking_paths();

        // Return lazy iterator - no .collect()!
        Box::new(input.flat_map(move |t| {
            let vertex_id = match t.as_vertex_id() {
                Some(id) => id,
                None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            };

            let label_ids_out = label_ids.clone();
            let label_ids_in = label_ids.clone();
            let t_for_in = t.clone();

            // Get outgoing neighbors
            let out_iter = ctx.storage().out_edges(vertex_id).filter_map(move |edge| {
                if !label_ids_out.is_empty() {
                    let edge_label_id = ctx.interner().lookup(&edge.label)?;
                    if !label_ids_out.contains(&edge_label_id) {
                        return None;
                    }
                }
                let mut new_t = t.split(Value::Vertex(edge.dst));
                if track_paths {
                    new_t.extend_path_unlabeled();
                }
                Some(new_t)
            });

            // Get incoming neighbors
            let in_iter = ctx.storage().in_edges(vertex_id).filter_map(move |edge| {
                if !label_ids_in.is_empty() {
                    let edge_label_id = ctx.interner().lookup(&edge.label)?;
                    if !label_ids_in.contains(&edge_label_id) {
                        return None;
                    }
                }
                let mut new_t = t_for_in.split(Value::Vertex(edge.src));
                if track_paths {
                    new_t.extend_path_unlabeled();
                }
                Some(new_t)
            });

            Box::new(out_iter.chain(in_iter)) as Box<dyn Iterator<Item = Traverser>>
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "both"
    }
}

// -----------------------------------------------------------------------------
// OutEStep - traverse to outgoing edges
// -----------------------------------------------------------------------------

/// Traverse to outgoing edges.
///
/// From a vertex, returns all outgoing edges (as edge elements).
/// Optionally filters by edge label.
///
/// Non-vertex traversers produce no output (empty iterator).
///
/// # Example
///
/// ```ignore
/// // All outgoing edges
/// let edges = g.v().out_e().to_list();
///
/// // Filter by edge label
/// let knows_edges = g.v().out_e_labels(&["knows"]).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct OutEStep {
    /// Optional edge labels to filter by
    labels: Vec<String>,
}

impl OutEStep {
    /// Create a new OutEStep with no label filter.
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    /// Create a new OutEStep with label filtering.
    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }
}

impl Default for OutEStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for OutEStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Pre-resolve labels once (not per-traverser) for efficiency
        let label_ids: Vec<u32> = if self.labels.is_empty() {
            Vec::new()
        } else {
            ctx.resolve_labels(&self.labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        };

        // If labels were specified but none resolved, return empty iterator
        if !self.labels.is_empty() && label_ids.is_empty() {
            return Box::new(std::iter::empty());
        }

        let track_paths = ctx.is_tracking_paths();

        // Return lazy iterator - no .collect()!
        Box::new(input.flat_map(move |t| {
            let vertex_id = match t.as_vertex_id() {
                Some(id) => id,
                None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            };

            let label_ids = label_ids.clone();
            Box::new(ctx.storage().out_edges(vertex_id).filter_map(move |edge| {
                if !label_ids.is_empty() {
                    let edge_label_id = ctx.interner().lookup(&edge.label)?;
                    if !label_ids.contains(&edge_label_id) {
                        return None;
                    }
                }
                let mut new_t = t.split(Value::Edge(edge.id));
                if track_paths {
                    new_t.extend_path_unlabeled();
                }
                Some(new_t)
            })) as Box<dyn Iterator<Item = Traverser>>
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "outE"
    }
}

// -----------------------------------------------------------------------------
// InEStep - traverse to incoming edges
// -----------------------------------------------------------------------------

/// Traverse to incoming edges.
///
/// From a vertex, returns all incoming edges (as edge elements).
/// Optionally filters by edge label.
///
/// Non-vertex traversers produce no output (empty iterator).
///
/// # Example
///
/// ```ignore
/// // All incoming edges
/// let edges = g.v().in_e().to_list();
///
/// // Filter by edge label
/// let known_by_edges = g.v().in_e_labels(&["knows"]).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct InEStep {
    /// Optional edge labels to filter by
    labels: Vec<String>,
}

impl InEStep {
    /// Create a new InEStep with no label filter.
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    /// Create a new InEStep with label filtering.
    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }
}

impl Default for InEStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for InEStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Pre-resolve labels once (not per-traverser) for efficiency
        let label_ids: Vec<u32> = if self.labels.is_empty() {
            Vec::new()
        } else {
            ctx.resolve_labels(&self.labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        };

        // If labels were specified but none resolved, return empty iterator
        if !self.labels.is_empty() && label_ids.is_empty() {
            return Box::new(std::iter::empty());
        }

        let track_paths = ctx.is_tracking_paths();

        // Return lazy iterator - no .collect()!
        Box::new(input.flat_map(move |t| {
            let vertex_id = match t.as_vertex_id() {
                Some(id) => id,
                None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            };

            let label_ids = label_ids.clone();
            Box::new(ctx.storage().in_edges(vertex_id).filter_map(move |edge| {
                if !label_ids.is_empty() {
                    let edge_label_id = ctx.interner().lookup(&edge.label)?;
                    if !label_ids.contains(&edge_label_id) {
                        return None;
                    }
                }
                let mut new_t = t.split(Value::Edge(edge.id));
                if track_paths {
                    new_t.extend_path_unlabeled();
                }
                Some(new_t)
            })) as Box<dyn Iterator<Item = Traverser>>
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "inE"
    }
}

// -----------------------------------------------------------------------------
// BothEStep - traverse to all incident edges
// -----------------------------------------------------------------------------

/// Traverse to all incident edges (both outgoing and incoming).
///
/// From a vertex, returns all incident edges (as edge elements).
/// Optionally filters by edge label.
///
/// Non-vertex traversers produce no output (empty iterator).
///
/// # Example
///
/// ```ignore
/// // All incident edges
/// let edges = g.v().both_e().to_list();
///
/// // Filter by edge label
/// let knows_edges = g.v().both_e_labels(&["knows"]).to_list();
/// ```
#[derive(Clone, Debug)]
pub struct BothEStep {
    /// Optional edge labels to filter by
    labels: Vec<String>,
}

impl BothEStep {
    /// Create a new BothEStep with no label filter.
    pub fn new() -> Self {
        Self { labels: Vec::new() }
    }

    /// Create a new BothEStep with label filtering.
    pub fn with_labels(labels: Vec<String>) -> Self {
        Self { labels }
    }
}

impl Default for BothEStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for BothEStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        // Pre-resolve labels once (not per-traverser) for efficiency
        let label_ids: Vec<u32> = if self.labels.is_empty() {
            Vec::new()
        } else {
            ctx.resolve_labels(&self.labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())
        };

        // If labels were specified but none resolved, return empty iterator
        if !self.labels.is_empty() && label_ids.is_empty() {
            return Box::new(std::iter::empty());
        }

        let track_paths = ctx.is_tracking_paths();

        // Return lazy iterator - no .collect()!
        Box::new(input.flat_map(move |t| {
            let vertex_id = match t.as_vertex_id() {
                Some(id) => id,
                None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            };

            let label_ids_out = label_ids.clone();
            let label_ids_in = label_ids.clone();
            let t_for_in = t.clone();

            // Get outgoing edges
            let out_iter = ctx.storage().out_edges(vertex_id).filter_map(move |edge| {
                if !label_ids_out.is_empty() {
                    let edge_label_id = ctx.interner().lookup(&edge.label)?;
                    if !label_ids_out.contains(&edge_label_id) {
                        return None;
                    }
                }
                let mut new_t = t.split(Value::Edge(edge.id));
                if track_paths {
                    new_t.extend_path_unlabeled();
                }
                Some(new_t)
            });

            // Get incoming edges
            let in_iter = ctx.storage().in_edges(vertex_id).filter_map(move |edge| {
                if !label_ids_in.is_empty() {
                    let edge_label_id = ctx.interner().lookup(&edge.label)?;
                    if !label_ids_in.contains(&edge_label_id) {
                        return None;
                    }
                }
                let mut new_t = t_for_in.split(Value::Edge(edge.id));
                if track_paths {
                    new_t.extend_path_unlabeled();
                }
                Some(new_t)
            });

            Box::new(out_iter.chain(in_iter)) as Box<dyn Iterator<Item = Traverser>>
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(self.clone())
    }

    fn name(&self) -> &'static str {
        "bothE"
    }
}

// -----------------------------------------------------------------------------
// OutVStep - get source vertex of edge
// -----------------------------------------------------------------------------

/// Get the source (outgoing) vertex of an edge.
///
/// From an edge, returns the source vertex (the vertex the edge originates from).
/// This is the opposite of `InVStep`.
///
/// Non-edge traversers produce no output (empty iterator).
///
/// # Example
///
/// ```ignore
/// // Get source vertices of all edges
/// let sources = g.e().out_v().to_list();
///
/// // Navigate: vertex -> edges -> back to source vertices
/// let sources = g.v().out_e().out_v().to_list();
/// ```
#[derive(Clone, Copy, Debug)]
pub struct OutVStep;

impl OutVStep {
    /// Create a new OutVStep.
    pub fn new() -> Self {
        Self
    }
}

impl Default for OutVStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for OutVStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(move |t| {
            let edge_id = t.as_edge_id()?;
            let edge = ctx.storage().get_edge(edge_id)?;
            let mut new_t = t.split(Value::Vertex(edge.src));
            if ctx.is_tracking_paths() {
                new_t.extend_path_unlabeled();
            }
            Some(new_t)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "outV"
    }
}

// -----------------------------------------------------------------------------
// InVStep - get target vertex of edge
// -----------------------------------------------------------------------------

/// Get the target (incoming) vertex of an edge.
///
/// From an edge, returns the target vertex (the vertex the edge points to).
/// This is the opposite of `OutVStep`.
///
/// Non-edge traversers produce no output (empty iterator).
///
/// # Example
///
/// ```ignore
/// // Get target vertices of all edges
/// let targets = g.e().in_v().to_list();
///
/// // Navigate: vertex -> edges -> to target vertices
/// let targets = g.v().out_e().in_v().to_list();
/// ```
#[derive(Clone, Copy, Debug)]
pub struct InVStep;

impl InVStep {
    /// Create a new InVStep.
    pub fn new() -> Self {
        Self
    }
}

impl Default for InVStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for InVStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(move |t| {
            let edge_id = t.as_edge_id()?;
            let edge = ctx.storage().get_edge(edge_id)?;
            let mut new_t = t.split(Value::Vertex(edge.dst));
            if ctx.is_tracking_paths() {
                new_t.extend_path_unlabeled();
            }
            Some(new_t)
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "inV"
    }
}

// -----------------------------------------------------------------------------
// BothVStep - get both vertices of edge
// -----------------------------------------------------------------------------

/// Get both vertices of an edge.
///
/// From an edge, returns both the source and target vertices (2 per edge).
/// Source vertex is returned first, then target vertex.
///
/// Non-edge traversers produce no output (empty iterator).
///
/// # Example
///
/// ```ignore
/// // Get both vertices of all edges (2 results per edge)
/// let vertices = g.e().both_v().to_list();
///
/// // Navigate: vertex -> edges -> all connected vertices
/// let connected = g.v().out_e().both_v().to_list();
/// ```
#[derive(Clone, Copy, Debug)]
pub struct BothVStep;

impl BothVStep {
    /// Create a new BothVStep.
    pub fn new() -> Self {
        Self
    }
}

impl Default for BothVStep {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyStep for BothVStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.flat_map(move |t| {
            let edge_id = match t.as_edge_id() {
                Some(id) => id,
                None => return Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            };

            match ctx.storage().get_edge(edge_id) {
                Some(edge) => {
                    let track_paths = ctx.is_tracking_paths();
                    let mut src = t.split(Value::Vertex(edge.src));
                    let mut dst = t.split(Value::Vertex(edge.dst));
                    if track_paths {
                        src.extend_path_unlabeled();
                        dst.extend_path_unlabeled();
                    }
                    // Use array iterator instead of Vec for fixed-size output
                    Box::new([src, dst].into_iter()) as Box<dyn Iterator<Item = Traverser>>
                }
                None => Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Traverser>>,
            }
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "bothV"
    }
}

// -----------------------------------------------------------------------------
// OtherVStep - get the "other" vertex of an edge
// -----------------------------------------------------------------------------

/// Get the "other" vertex of an edge.
///
/// When traversing from a vertex to an edge and then to another vertex,
/// `otherV()` returns the vertex at the opposite end from where the traverser
/// came from. This requires path tracking to determine the source vertex.
///
/// If the current value is not an edge, or if the previous path element
/// cannot be determined, the traverser is filtered out.
///
/// # Example
///
/// ```ignore
/// // From vertex -> edge -> other vertex
/// // This is similar to out() but works after explicitly stepping to edges
/// let others = g.v().has("name", "marko").out_e("knows").other_v().to_list();
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct OtherVStep;

impl OtherVStep {
    /// Create a new OtherVStep.
    pub fn new() -> Self {
        Self
    }

    /// Get the "other" vertex for the given edge traverser.
    ///
    /// Examines the traverser's path to find which vertex we came from,
    /// then returns the opposite vertex of the edge.
    fn get_other_vertex(
        &self,
        ctx: &ExecutionContext<'_>,
        traverser: &Traverser,
    ) -> Option<crate::value::VertexId> {
        use crate::traversal::PathValue;

        // Current value must be an edge
        let edge_id = match &traverser.value {
            Value::Edge(id) => *id,
            _ => return None,
        };

        // Get the edge to find its endpoints
        let edge = ctx.storage().get_edge(edge_id)?;

        // Find the vertex we came from by looking at the path
        // We need at least 2 elements: the previous vertex and the current edge
        let path_len = traverser.path.len();
        if path_len < 2 {
            // No previous element in path, can't determine source
            // Fall back: arbitrarily return dst (or could filter out)
            return Some(edge.dst);
        }

        // Collect path elements to access by index
        let path_elements: Vec<_> = traverser.path.elements().collect();
        let prev_element = &path_elements[path_len - 2];

        match &prev_element.value {
            PathValue::Vertex(prev_id) => {
                // Return the OTHER vertex
                if *prev_id == edge.src {
                    Some(edge.dst)
                } else if *prev_id == edge.dst {
                    Some(edge.src)
                } else {
                    // Previous vertex isn't an endpoint of this edge
                    // This shouldn't happen in normal traversals
                    None
                }
            }
            _ => None,
        }
    }
}

impl AnyStep for OtherVStep {
    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Box<dyn Iterator<Item = Traverser> + 'a> {
        Box::new(input.filter_map(move |t| {
            self.get_other_vertex(ctx, &t).map(|vid| {
                let mut new_t = t.with_value(Value::Vertex(vid));
                if ctx.is_tracking_paths() {
                    new_t.extend_path_unlabeled();
                }
                new_t
            })
        }))
    }

    fn clone_box(&self) -> Box<dyn AnyStep> {
        Box::new(*self)
    }

    fn name(&self) -> &'static str {
        "otherV"
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use crate::value::{EdgeId, VertexId};
    use std::collections::HashMap;

    /// Create a test graph with the following structure:
    ///
    /// ```text
    /// Alice --knows--> Bob --knows--> Charlie
    ///   |               |
    ///   +--uses--> GraphDB <--uses--+
    /// ```
    fn create_test_graph() -> Graph {
        let graph = Graph::new();

        // Add vertices
        let alice = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Alice".to_string()));
            props
        });

        let bob = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Bob".to_string()));
            props
        });

        let charlie = graph.add_vertex("person", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("Charlie".to_string()));
            props
        });

        let graphdb = graph.add_vertex("software", {
            let mut props = HashMap::new();
            props.insert("name".to_string(), Value::String("GraphDB".to_string()));
            props
        });

        // Add edges
        // Alice -> Bob (knows)
        graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
        // Bob -> Charlie (knows)
        graph
            .add_edge(bob, charlie, "knows", HashMap::new())
            .unwrap();
        // Alice -> GraphDB (uses)
        graph
            .add_edge(alice, graphdb, "uses", HashMap::new())
            .unwrap();
        // Bob -> GraphDB (uses)
        graph
            .add_edge(bob, graphdb, "uses", HashMap::new())
            .unwrap();

        graph
    }

    mod out_step_tests {
        use super::*;

        #[test]
        fn out_step_new() {
            let step = OutStep::new();
            assert!(step.labels.is_empty());
        }

        #[test]
        fn out_step_with_labels() {
            let step = OutStep::with_labels(vec!["knows".to_string()]);
            assert_eq!(step.labels, vec!["knows"]);
        }

        #[test]
        fn out_step_name() {
            let step = OutStep::new();
            assert_eq!(step.name(), "out");
        }

        #[test]
        fn out_step_clone_box() {
            let step = OutStep::with_labels(vec!["test".to_string()]);
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "out");
        }

        #[test]
        fn out_traverses_all_outgoing_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice has 2 outgoing edges (knows->Bob, uses->GraphDB)
            let results = g.v_ids([VertexId(0)]).out().to_list();
            assert_eq!(results.len(), 2);

            // Check that we got Bob and GraphDB
            let vertex_ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
            assert!(vertex_ids.contains(&VertexId(1))); // Bob
            assert!(vertex_ids.contains(&VertexId(3))); // GraphDB
        }

        #[test]
        fn out_with_label_filter() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice has 1 "knows" edge (to Bob)
            let results = g.v_ids([VertexId(0)]).out_labels(&["knows"]).to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(1)));
        }

        #[test]
        fn out_from_non_vertex_produces_nothing() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Inject a non-vertex value and try to navigate
            let results = g.inject([42i64]).out().to_list();
            assert!(results.is_empty());
        }

        #[test]
        fn out_from_vertex_with_no_out_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Charlie has no outgoing edges
            let results = g.v_ids([VertexId(2)]).out().to_list();
            assert!(results.is_empty());
        }

        #[test]
        fn out_chained() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice -> Bob -> Charlie (2 hops via knows)
            let results = g
                .v_ids([VertexId(0)])
                .out_labels(&["knows"])
                .out_labels(&["knows"])
                .to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(2))); // Charlie
        }

        #[test]
        fn out_with_multiple_labels() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice -> both knows and uses edges
            let results = g
                .v_ids([VertexId(0)])
                .out_labels(&["knows", "uses"])
                .to_list();
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn out_with_nonexistent_label() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // No "likes" edges exist
            let results = g.v_ids([VertexId(0)]).out_labels(&["likes"]).to_list();
            assert!(results.is_empty());
        }
    }

    mod in_step_tests {
        use super::*;

        #[test]
        fn in_step_new() {
            let step = InStep::new();
            assert!(step.labels.is_empty());
        }

        #[test]
        fn in_step_name() {
            let step = InStep::new();
            assert_eq!(step.name(), "in");
        }

        #[test]
        fn in_traverses_all_incoming_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Bob has 1 incoming "knows" edge (from Alice)
            let results = g.v_ids([VertexId(1)]).in_().to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(0))); // Alice
        }

        #[test]
        fn in_with_label_filter() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // GraphDB has 2 incoming "uses" edges (from Alice and Bob)
            let results = g.v_ids([VertexId(3)]).in_labels(&["uses"]).to_list();
            assert_eq!(results.len(), 2);
        }

        #[test]
        fn in_from_vertex_with_no_in_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice has no incoming edges
            let results = g.v_ids([VertexId(0)]).in_().to_list();
            assert!(results.is_empty());
        }
    }

    mod both_step_tests {
        use super::*;

        #[test]
        fn both_step_name() {
            let step = BothStep::new();
            assert_eq!(step.name(), "both");
        }

        #[test]
        fn both_traverses_all_directions() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Bob has 1 incoming (Alice), 2 outgoing (Charlie, GraphDB)
            let results = g.v_ids([VertexId(1)]).both().to_list();
            assert_eq!(results.len(), 3);
        }

        #[test]
        fn both_with_label_filter() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Bob: knows->Charlie (out), knows<-Alice (in) = 2 "knows" neighbors
            let results = g.v_ids([VertexId(1)]).both_labels(&["knows"]).to_list();
            assert_eq!(results.len(), 2);
        }
    }

    mod out_e_step_tests {
        use super::*;

        #[test]
        fn out_e_step_name() {
            let step = OutEStep::new();
            assert_eq!(step.name(), "outE");
        }

        #[test]
        fn out_e_returns_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice has 2 outgoing edges
            let results = g.v_ids([VertexId(0)]).out_e().to_list();
            assert_eq!(results.len(), 2);

            // Verify they are edge values
            for result in &results {
                assert!(result.as_edge_id().is_some());
            }
        }

        #[test]
        fn out_e_with_label_filter() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice has 1 "knows" edge
            let results = g.v_ids([VertexId(0)]).out_e_labels(&["knows"]).to_list();
            assert_eq!(results.len(), 1);
        }
    }

    mod in_e_step_tests {
        use super::*;

        #[test]
        fn in_e_step_name() {
            let step = InEStep::new();
            assert_eq!(step.name(), "inE");
        }

        #[test]
        fn in_e_returns_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // GraphDB has 2 incoming edges
            let results = g.v_ids([VertexId(3)]).in_e().to_list();
            assert_eq!(results.len(), 2);

            // Verify they are edge values
            for result in &results {
                assert!(result.as_edge_id().is_some());
            }
        }
    }

    mod both_e_step_tests {
        use super::*;

        #[test]
        fn both_e_step_name() {
            let step = BothEStep::new();
            assert_eq!(step.name(), "bothE");
        }

        #[test]
        fn both_e_returns_all_edges() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Bob: 1 incoming (knows from Alice), 2 outgoing (knows to Charlie, uses to GraphDB)
            let results = g.v_ids([VertexId(1)]).both_e().to_list();
            assert_eq!(results.len(), 3);

            // Verify they are edge values
            for result in &results {
                assert!(result.as_edge_id().is_some());
            }
        }
    }

    mod out_v_step_tests {
        use super::*;

        #[test]
        fn out_v_step_name() {
            let step = OutVStep::new();
            assert_eq!(step.name(), "outV");
        }

        #[test]
        fn out_v_returns_source_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Get edges, then source vertices
            let results = g.e().out_v().to_list();
            // 4 edges total
            assert_eq!(results.len(), 4);

            // All should be vertices
            for result in &results {
                assert!(result.as_vertex_id().is_some());
            }
        }

        #[test]
        fn out_v_from_non_edge_produces_nothing() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Inject non-edge value
            let results = g.inject([42i64]).out_v().to_list();
            assert!(results.is_empty());
        }

        #[test]
        fn out_e_in_v_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice -> edges -> target vertices (should be same as out())
            let via_out = g.v_ids([VertexId(0)]).out().to_list();
            let via_edges = g.v_ids([VertexId(0)]).out_e().in_v().to_list();

            assert_eq!(via_out.len(), via_edges.len());
        }
    }

    mod in_v_step_tests {
        use super::*;

        #[test]
        fn in_v_step_name() {
            let step = InVStep::new();
            assert_eq!(step.name(), "inV");
        }

        #[test]
        fn in_v_returns_target_vertex() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Get edges, then target vertices
            let results = g.e().in_v().to_list();
            // 4 edges total
            assert_eq!(results.len(), 4);

            // All should be vertices
            for result in &results {
                assert!(result.as_vertex_id().is_some());
            }
        }
    }

    mod both_v_step_tests {
        use super::*;

        #[test]
        fn both_v_step_name() {
            let step = BothVStep::new();
            assert_eq!(step.name(), "bothV");
        }

        #[test]
        fn both_v_returns_both_vertices() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Get first edge, then both vertices
            let results = g.e_ids([EdgeId(0)]).both_v().to_list();
            assert_eq!(results.len(), 2);

            // Should be Alice and Bob (edge 0 is Alice->Bob)
            let vertex_ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
            assert!(vertex_ids.contains(&VertexId(0))); // Alice (source)
            assert!(vertex_ids.contains(&VertexId(1))); // Bob (target)
        }

        #[test]
        fn both_v_from_non_edge_produces_nothing() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            let results = g.v_ids([VertexId(0)]).both_v().to_list();
            assert!(results.is_empty());
        }
    }

    mod other_v_step_tests {
        use super::*;

        #[test]
        fn other_v_step_new() {
            let step = OtherVStep::new();
            assert_eq!(step.name(), "otherV");
        }

        #[test]
        fn other_v_step_default() {
            let step = OtherVStep;
            assert_eq!(step.name(), "otherV");
        }

        #[test]
        fn other_v_step_clone_box() {
            let step = OtherVStep::new();
            let cloned = step.clone_box();
            assert_eq!(cloned.name(), "otherV");
        }

        #[test]
        fn other_v_from_out_edge_returns_target() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice (id=0) -> outE -> other_v should give target vertices (Bob, GraphDB)
            // Need path tracking for other_v to work correctly
            let results = g
                .v_ids([VertexId(0)])
                .with_path()
                .out_e()
                .other_v()
                .to_list();
            assert_eq!(results.len(), 2);

            let vertex_ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
            assert!(vertex_ids.contains(&VertexId(1))); // Bob
            assert!(vertex_ids.contains(&VertexId(3))); // GraphDB
        }

        #[test]
        fn other_v_from_in_edge_returns_source() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Bob (id=1) has 1 incoming "knows" edge from Alice
            // Bob -> inE -> other_v should give Alice
            let results = g
                .v_ids([VertexId(1)])
                .with_path()
                .in_e_labels(&["knows"])
                .other_v()
                .to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(0))); // Alice
        }

        #[test]
        fn other_v_from_both_e_returns_opposite() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Bob has 3 incident edges:
            // - knows from Alice (in)
            // - knows to Charlie (out)
            // - uses to GraphDB (out)
            // other_v should return: Alice, Charlie, GraphDB
            let results = g
                .v_ids([VertexId(1)])
                .with_path()
                .both_e()
                .other_v()
                .to_list();
            assert_eq!(results.len(), 3);

            let vertex_ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
            assert!(vertex_ids.contains(&VertexId(0))); // Alice
            assert!(vertex_ids.contains(&VertexId(2))); // Charlie
            assert!(vertex_ids.contains(&VertexId(3))); // GraphDB
        }

        #[test]
        fn other_v_from_non_edge_produces_nothing() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Starting from a vertex (not an edge) should produce nothing
            let results = g.v_ids([VertexId(0)]).with_path().other_v().to_list();
            assert!(results.is_empty());
        }

        #[test]
        fn other_v_with_label_filter() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice -> out_e("knows") -> other_v should give Bob only
            let results = g
                .v_ids([VertexId(0)])
                .with_path()
                .out_e_labels(&["knows"])
                .other_v()
                .to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(1))); // Bob
        }

        #[test]
        fn other_v_equivalent_to_in_v_after_out_e() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // For outE, other_v should be equivalent to in_v
            let via_other_v = g
                .v_ids([VertexId(0)])
                .with_path()
                .out_e_labels(&["knows"])
                .other_v()
                .to_list();

            let g2 = snapshot.gremlin();
            let via_in_v = g2
                .v_ids([VertexId(0)])
                .out_e_labels(&["knows"])
                .in_v()
                .to_list();

            assert_eq!(via_other_v, via_in_v);
        }

        #[test]
        fn other_v_equivalent_to_out_v_after_in_e() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // For inE, other_v should be equivalent to out_v
            let via_other_v = g
                .v_ids([VertexId(1)])
                .with_path()
                .in_e_labels(&["knows"])
                .other_v()
                .to_list();

            let g2 = snapshot.gremlin();
            let via_out_v = g2
                .v_ids([VertexId(1)])
                .in_e_labels(&["knows"])
                .out_v()
                .to_list();

            assert_eq!(via_other_v, via_out_v);
        }

        #[test]
        fn other_v_without_path_tracking_uses_fallback() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            // Deliberately NOT using with_path()
            let g = snapshot.gremlin();

            // Without path tracking, other_v will use fallback behavior
            // (returning dst since we can't determine where we came from)
            let results = g.v_ids([VertexId(0)]).out_e().other_v().to_list();

            // Should still produce results (using fallback to dst)
            assert!(!results.is_empty());
        }

        #[test]
        fn other_v_chained_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice -> out_e -> other_v -> out_e -> other_v
            // This should go: Alice -> Bob/GraphDB edges -> Bob/GraphDB -> their edges -> targets
            let results = g
                .v_ids([VertexId(0)])
                .with_path()
                .out_e_labels(&["knows"])
                .other_v()
                .out_e_labels(&["knows"])
                .other_v()
                .to_list();

            // Alice -> knows -> Bob -> knows -> Charlie
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(2))); // Charlie
        }

        #[test]
        fn other_v_with_injected_edge_no_path() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Inject an edge directly (no path context)
            // Should use fallback behavior
            let results = g
                .inject([Value::Edge(EdgeId(0))])
                .with_path()
                .other_v()
                .to_list();

            // Edge 0 is Alice->Bob, fallback returns dst (Bob)
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(1))); // Bob (dst)
        }

        #[test]
        fn other_v_values_property_access() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Alice -> out_e("knows") -> other_v -> values("name")
            let results = g
                .v_ids([VertexId(0)])
                .with_path()
                .out_e_labels(&["knows"])
                .other_v()
                .values("name")
                .to_list();

            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::String("Bob".to_string()));
        }
    }

    mod integration_tests {
        use super::*;

        #[test]
        fn complex_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Find all people who use GraphDB (in_edges->source vertices)
            let results = g
                .v_ids([VertexId(3)]) // GraphDB
                .in_labels(&["uses"])
                .to_list();
            assert_eq!(results.len(), 2); // Alice and Bob

            // Verify they're the right vertices
            let vertex_ids: Vec<_> = results.iter().filter_map(|v| v.as_vertex_id()).collect();
            assert!(vertex_ids.contains(&VertexId(0))); // Alice
            assert!(vertex_ids.contains(&VertexId(1))); // Bob
        }

        #[test]
        fn multi_hop_traversal() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // Friends of friends: Alice -> knows -> Bob -> knows -> Charlie
            let results = g
                .v_ids([VertexId(0)])
                .out_labels(&["knows"])
                .out_labels(&["knows"])
                .to_list();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], Value::Vertex(VertexId(2))); // Charlie
        }

        #[test]
        fn dedup_with_navigation() {
            let graph = create_test_graph();
            let snapshot = graph.snapshot();
            let g = snapshot.gremlin();

            // All neighbors of Alice (may have duplicates if same vertex reachable via multiple edges)
            let with_dups = g.v_ids([VertexId(0)]).out().to_list();
            let without_dups = g.v_ids([VertexId(0)]).out().dedup().to_list();

            // In this graph no duplicates, but verify dedup works
            assert_eq!(with_dups.len(), without_dups.len());
        }
    }
}
