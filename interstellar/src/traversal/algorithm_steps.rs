//! Algorithm traversal steps for Gremlin integration.
//!
//! Provides `ShortestPathStep` and `DijkstraStep` that bridge the graph algorithms
//! module with the Gremlin traversal pipeline.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use crate::storage::{Edge, GraphStorage};
use crate::traversal::context::ExecutionContext;
use crate::traversal::step::Step;
use crate::traversal::Traverser;
use crate::value::{EdgeId, OrderedFloat, Value, VertexId};

/// Direction for algorithm steps (mirrors algorithms::common::Direction).
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum StepDirection {
    Out,
    In,
    Both,
}

/// Expand neighbors from storage in the given direction.
pub(crate) fn expand_from_storage(
    storage: &dyn GraphStorage,
    vid: VertexId,
    direction: StepDirection,
) -> Vec<(VertexId, EdgeId, Edge)> {
    let mut result = Vec::new();
    if matches!(direction, StepDirection::Out | StepDirection::Both) {
        for edge in storage.out_edges(vid) {
            let eid = edge.id;
            let dst = edge.dst;
            result.push((dst, eid, edge));
        }
    }
    if matches!(direction, StepDirection::In | StepDirection::Both) {
        for edge in storage.in_edges(vid) {
            let eid = edge.id;
            let src = edge.src;
            result.push((src, eid, edge));
        }
    }
    result
}

/// Reconstruct a path from predecessor map, returning vertex IDs as a Value::List.
pub(crate) fn reconstruct_as_value_list(
    prev: &HashMap<VertexId, (VertexId, EdgeId)>,
    source: VertexId,
    target: VertexId,
) -> Value {
    let mut vertices = vec![target];
    let mut cur = target;
    while cur != source {
        if let Some((parent, _)) = prev.get(&cur) {
            vertices.push(*parent);
            cur = *parent;
        } else {
            break;
        }
    }
    vertices.reverse();
    Value::List(vertices.into_iter().map(Value::Vertex).collect())
}

// ---------------------------------------------------------------------------
// ShortestPathStep (unweighted BFS)
// ---------------------------------------------------------------------------

/// Traversal step: unweighted shortest path from current vertex to a target.
///
/// For each input traverser holding a vertex, computes the shortest unweighted
/// path to the specified target vertex using BFS. Emits a `Value::List` of
/// vertex IDs representing the path, or produces no output if no path exists.
///
/// Used via `g.v(source).shortest_path_to(target)`.
#[derive(Clone, Debug)]
pub struct ShortestPathStep {
    target: VertexId,
}

impl ShortestPathStep {
    /// Create a new shortest path step to the given target vertex.
    pub fn new(target: VertexId) -> Self {
        Self { target }
    }
}

impl Step for ShortestPathStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let target = self.target;
        Box::new(input.filter_map(move |t| {
            let source = t.as_vertex_id()?;
            let path_value = bfs_shortest_path(ctx.storage(), source, target, StepDirection::Out)?;
            Some(t.split(path_value))
        }))
    }

    fn name(&self) -> &'static str {
        "shortestPath"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Navigation
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let source = match input.as_vertex_id() {
            Some(id) => id,
            None => return Box::new(std::iter::empty()),
        };
        let target = self.target;
        let storage = ctx.arc_storage();
        let result = bfs_shortest_path_storage(&*storage, source, target, StepDirection::Out);
        match result {
            Some(path_value) => Box::new(std::iter::once(input.split(path_value))),
            None => Box::new(std::iter::empty()),
        }
    }
}

/// BFS shortest path on GraphStorage, returning Value::List of vertex path.
pub(crate) fn bfs_shortest_path(
    storage: &dyn GraphStorage,
    source: VertexId,
    target: VertexId,
    direction: StepDirection,
) -> Option<Value> {
    if source == target {
        return Some(Value::List(vec![Value::Vertex(source)]));
    }
    if storage.get_vertex(source).is_none() || storage.get_vertex(target).is_none() {
        return None;
    }

    let mut visited = HashSet::new();
    let mut prev: HashMap<VertexId, (VertexId, EdgeId)> = HashMap::new();
    let mut queue = VecDeque::new();

    visited.insert(source);
    queue.push_back(source);

    while let Some(vid) = queue.pop_front() {
        let neighbors = expand_from_storage(storage, vid, direction);
        for (neighbor, eid, _) in neighbors {
            if visited.insert(neighbor) {
                prev.insert(neighbor, (vid, eid));
                if neighbor == target {
                    return Some(reconstruct_as_value_list(&prev, source, target));
                }
                queue.push_back(neighbor);
            }
        }
    }
    None
}

/// Same as bfs_shortest_path but takes a concrete storage reference (for streaming).
fn bfs_shortest_path_storage(
    storage: &dyn GraphStorage,
    source: VertexId,
    target: VertexId,
    direction: StepDirection,
) -> Option<Value> {
    bfs_shortest_path(storage, source, target, direction)
}

// ---------------------------------------------------------------------------
// DijkstraStep (weighted shortest path)
// ---------------------------------------------------------------------------

/// Traversal step: weighted shortest path from current vertex to a target.
///
/// For each input traverser holding a vertex, computes the shortest weighted
/// path to the specified target vertex using Dijkstra's algorithm. The weight
/// is extracted from the named edge property.
///
/// Emits a `Value::Map` with keys "path" (List of vertex IDs) and "weight" (Float).
///
/// Used via `g.v(source).dijkstra_to(target, "weight")`.
#[derive(Clone, Debug)]
pub struct DijkstraStep {
    target: VertexId,
    weight_property: String,
}

impl DijkstraStep {
    /// Create a new Dijkstra step to the given target using the named weight property.
    pub fn new(target: VertexId, weight_property: String) -> Self {
        Self {
            target,
            weight_property,
        }
    }
}

impl Step for DijkstraStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let target = self.target;
        let weight_prop = self.weight_property.clone();
        Box::new(input.filter_map(move |t| {
            let source = t.as_vertex_id()?;
            let result = dijkstra_on_storage(
                ctx.storage(),
                source,
                target,
                &weight_prop,
                StepDirection::Out,
            )?;
            Some(t.split(result))
        }))
    }

    fn name(&self) -> &'static str {
        "dijkstra"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Navigation
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let source = match input.as_vertex_id() {
            Some(id) => id,
            None => return Box::new(std::iter::empty()),
        };
        let target = self.target;
        let weight_prop = self.weight_property.clone();
        let storage = ctx.arc_storage();
        let result =
            dijkstra_on_storage(&*storage, source, target, &weight_prop, StepDirection::Out);
        match result {
            Some(value) => Box::new(std::iter::once(input.split(value))),
            None => Box::new(std::iter::empty()),
        }
    }
}

/// Dijkstra on GraphStorage. Returns a Value::Map with "path" and "weight" keys.
pub(crate) fn dijkstra_on_storage(
    storage: &dyn GraphStorage,
    source: VertexId,
    target: VertexId,
    weight_property: &str,
    direction: StepDirection,
) -> Option<Value> {
    if source == target {
        let mut map = indexmap::IndexMap::new();
        map.insert("path".to_string(), Value::List(vec![Value::Vertex(source)]));
        map.insert("weight".to_string(), Value::Float(0.0));
        return Some(Value::Map(map));
    }
    if storage.get_vertex(source).is_none() || storage.get_vertex(target).is_none() {
        return None;
    }

    let mut dist: HashMap<VertexId, f64> = HashMap::new();
    let mut prev: HashMap<VertexId, (VertexId, EdgeId)> = HashMap::new();
    let mut heap: BinaryHeap<Reverse<(OrderedFloat, VertexId)>> = BinaryHeap::new();

    dist.insert(source, 0.0);
    heap.push(Reverse((OrderedFloat(0.0), source)));

    while let Some(Reverse((OrderedFloat(d), vid))) = heap.pop() {
        if vid == target {
            let path = reconstruct_as_value_list(&prev, source, target);
            let mut map = indexmap::IndexMap::new();
            map.insert("path".to_string(), path);
            map.insert("weight".to_string(), Value::Float(d));
            return Some(Value::Map(map));
        }

        if d > *dist.get(&vid).unwrap_or(&f64::INFINITY) {
            continue;
        }

        let neighbors = expand_from_storage(storage, vid, direction);
        for (neighbor, eid, edge) in neighbors {
            let w = match edge.properties.get(weight_property) {
                Some(Value::Int(n)) => *n as f64,
                Some(Value::Float(f)) => *f,
                _ => continue, // Skip edges without valid weight
            };

            let new_dist = d + w;
            let current_dist = dist.get(&neighbor).copied().unwrap_or(f64::INFINITY);
            if new_dist < current_dist {
                dist.insert(neighbor, new_dist);
                prev.insert(neighbor, (vid, eid));
                heap.push(Reverse((OrderedFloat(new_dist), neighbor)));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// A* Step (weighted shortest path with property-based heuristic)
// ---------------------------------------------------------------------------

/// Traversal step: A* shortest path from current vertex to a target.
///
/// Uses the named weight property for edge costs and a vertex property
/// as the heuristic estimate. If the heuristic property is missing on a
/// vertex, falls back to 0.0 (Dijkstra behavior).
///
/// Emits a `Value::Map` with "path" (List of vertex IDs) and "weight" (Float).
#[derive(Clone, Debug)]
pub struct AstarStep {
    target: VertexId,
    weight_property: String,
    heuristic_property: String,
}

impl AstarStep {
    pub fn new(target: VertexId, weight_property: String, heuristic_property: String) -> Self {
        Self {
            target,
            weight_property,
            heuristic_property,
        }
    }
}

impl Step for AstarStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let target = self.target;
        let weight_prop = self.weight_property.clone();
        let heuristic_prop = self.heuristic_property.clone();
        Box::new(input.filter_map(move |t| {
            let source = t.as_vertex_id()?;
            let result = astar_on_storage(
                ctx.storage(),
                source,
                target,
                &weight_prop,
                &heuristic_prop,
                StepDirection::Out,
            )?;
            Some(t.split(result))
        }))
    }

    fn name(&self) -> &'static str {
        "astar"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Navigation
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let source = match input.as_vertex_id() {
            Some(id) => id,
            None => return Box::new(std::iter::empty()),
        };
        let target = self.target;
        let weight_prop = self.weight_property.clone();
        let heuristic_prop = self.heuristic_property.clone();
        let storage = ctx.arc_storage();
        let result = astar_on_storage(
            &*storage,
            source,
            target,
            &weight_prop,
            &heuristic_prop,
            StepDirection::Out,
        );
        match result {
            Some(value) => Box::new(std::iter::once(input.split(value))),
            None => Box::new(std::iter::empty()),
        }
    }
}

/// A* on GraphStorage. Returns a Value::Map with "path" and "weight" keys.
pub(crate) fn astar_on_storage(
    storage: &dyn GraphStorage,
    source: VertexId,
    target: VertexId,
    weight_property: &str,
    heuristic_property: &str,
    direction: StepDirection,
) -> Option<Value> {
    if source == target {
        let mut map = indexmap::IndexMap::new();
        map.insert("path".to_string(), Value::List(vec![Value::Vertex(source)]));
        map.insert("weight".to_string(), Value::Float(0.0));
        return Some(Value::Map(map));
    }
    if storage.get_vertex(source).is_none() || storage.get_vertex(target).is_none() {
        return None;
    }

    // Heuristic: read vertex property, default to 0.0
    let h = |vid: VertexId| -> f64 {
        storage
            .get_vertex(vid)
            .and_then(|v| {
                v.properties.get(heuristic_property).and_then(|val| match val {
                    Value::Float(f) => Some(*f),
                    Value::Int(n) => Some(*n as f64),
                    _ => None,
                })
            })
            .unwrap_or(0.0)
    };

    let mut g_score: HashMap<VertexId, f64> = HashMap::new();
    let mut prev: HashMap<VertexId, (VertexId, EdgeId)> = HashMap::new();
    let mut heap: BinaryHeap<Reverse<(OrderedFloat, VertexId)>> = BinaryHeap::new();

    g_score.insert(source, 0.0);
    heap.push(Reverse((OrderedFloat(h(source)), source)));

    while let Some(Reverse((_, vid))) = heap.pop() {
        if vid == target {
            let g = g_score[&target];
            let path = reconstruct_as_value_list(&prev, source, target);
            let mut map = indexmap::IndexMap::new();
            map.insert("path".to_string(), path);
            map.insert("weight".to_string(), Value::Float(g));
            return Some(Value::Map(map));
        }

        let current_g = g_score[&vid];
        let neighbors = expand_from_storage(storage, vid, direction);
        for (neighbor, eid, edge) in neighbors {
            let w = match edge.properties.get(weight_property) {
                Some(Value::Int(n)) => *n as f64,
                Some(Value::Float(f)) => *f,
                _ => continue,
            };

            let tentative_g = current_g + w;
            let current_best = g_score.get(&neighbor).copied().unwrap_or(f64::INFINITY);
            if tentative_g < current_best {
                g_score.insert(neighbor, tentative_g);
                prev.insert(neighbor, (vid, eid));
                let f = tentative_g + h(neighbor);
                heap.push(Reverse((OrderedFloat(f), neighbor)));
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// K-Shortest Paths Step (Yen's algorithm)
// ---------------------------------------------------------------------------

/// Traversal step: Yen's k-shortest loopless paths.
///
/// Emits a `Value::List` of maps, each with "path" and "weight" keys.
#[derive(Clone, Debug)]
pub struct KShortestPathsStep {
    target: VertexId,
    k: usize,
    weight_property: String,
}

impl KShortestPathsStep {
    pub fn new(target: VertexId, k: usize, weight_property: String) -> Self {
        Self {
            target,
            k,
            weight_property,
        }
    }
}

impl Step for KShortestPathsStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let target = self.target;
        let k = self.k;
        let weight_prop = self.weight_property.clone();
        Box::new(input.filter_map(move |t| {
            let source = t.as_vertex_id()?;
            let result =
                k_shortest_paths_on_storage(ctx.storage(), source, target, k, &weight_prop)?;
            Some(t.split(result))
        }))
    }

    fn name(&self) -> &'static str {
        "kShortestPaths"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Navigation
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let source = match input.as_vertex_id() {
            Some(id) => id,
            None => return Box::new(std::iter::empty()),
        };
        let target = self.target;
        let k = self.k;
        let weight_prop = self.weight_property.clone();
        let storage = ctx.arc_storage();
        let result = k_shortest_paths_on_storage(&*storage, source, target, k, &weight_prop);
        match result {
            Some(value) => Box::new(std::iter::once(input.split(value))),
            None => Box::new(std::iter::empty()),
        }
    }
}

/// Yen's k-shortest paths on GraphStorage. Uses repeated Dijkstra.
/// Returns Value::List of Value::Map with "path" and "weight" keys.
fn k_shortest_paths_on_storage(
    storage: &dyn GraphStorage,
    source: VertexId,
    target: VertexId,
    k: usize,
    weight_property: &str,
) -> Option<Value> {
    // First shortest path via Dijkstra
    let first = dijkstra_on_storage(storage, source, target, weight_property, StepDirection::Out)?;
    let result_paths = vec![first];

    // For k > 1, we'd need full Yen's algorithm with edge exclusion.
    // For now, return the first path (same as the existing GQL stub).
    // TODO: Implement full Yen's algorithm with spur node iteration.
    if k > 1 {
        // Placeholder: only first path returned
    }

    Some(Value::List(result_paths))
}

// ---------------------------------------------------------------------------
// BFS Traversal Step
// ---------------------------------------------------------------------------

/// Traversal step: BFS from current vertex, yielding all reachable vertices.
///
/// Emits `Value::List` of `[vertex_id, depth]` pairs for each reachable vertex.
#[derive(Clone, Debug)]
pub struct BfsTraversalStep {
    max_depth: Option<u32>,
    edge_labels: Option<Vec<String>>,
}

impl BfsTraversalStep {
    pub fn new(max_depth: Option<u32>, edge_labels: Option<Vec<String>>) -> Self {
        Self {
            max_depth,
            edge_labels,
        }
    }
}

impl Step for BfsTraversalStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let max_depth = self.max_depth;
        let edge_labels = self.edge_labels.clone();
        Box::new(input.flat_map(move |t| {
            let source = match t.as_vertex_id() {
                Some(id) => id,
                None => return Vec::new(),
            };
            bfs_on_storage(ctx.storage(), source, max_depth, edge_labels.as_deref())
                .into_iter()
                .map(move |(vid, depth)| {
                    let mut map = indexmap::IndexMap::new();
                    map.insert("vertex".to_string(), Value::Vertex(vid));
                    map.insert("depth".to_string(), Value::Int(depth as i64));
                    t.split(Value::Map(map))
                })
                .collect::<Vec<_>>()
        }))
    }

    fn name(&self) -> &'static str {
        "bfs"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Navigation
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let source = match input.as_vertex_id() {
            Some(id) => id,
            None => return Box::new(std::iter::empty()),
        };
        let max_depth = self.max_depth;
        let edge_labels = self.edge_labels.clone();
        let storage = ctx.arc_storage();
        let results = bfs_on_storage(&*storage, source, max_depth, edge_labels.as_deref());
        Box::new(results.into_iter().map(move |(vid, depth)| {
            let mut map = indexmap::IndexMap::new();
            map.insert("vertex".to_string(), Value::Vertex(vid));
            map.insert("depth".to_string(), Value::Int(depth as i64));
            input.split(Value::Map(map))
        }))
    }
}

/// BFS on GraphStorage. Returns vec of (vertex_id, depth).
fn bfs_on_storage(
    storage: &dyn GraphStorage,
    source: VertexId,
    max_depth: Option<u32>,
    edge_labels: Option<&[String]>,
) -> Vec<(VertexId, u32)> {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    let mut result = Vec::new();

    visited.insert(source);
    queue.push_back((source, 0u32));
    result.push((source, 0));

    while let Some((vid, depth)) = queue.pop_front() {
        if let Some(max) = max_depth {
            if depth >= max {
                continue;
            }
        }
        let neighbors = expand_from_storage(storage, vid, StepDirection::Out);
        for (neighbor, _eid, edge) in neighbors {
            // Apply edge label filter
            if let Some(labels) = edge_labels {
                if !labels.iter().any(|l| l == &edge.label) {
                    continue;
                }
            }
            if visited.insert(neighbor) {
                queue.push_back((neighbor, depth + 1));
                result.push((neighbor, depth + 1));
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// DFS Traversal Step
// ---------------------------------------------------------------------------

/// Traversal step: DFS from current vertex, yielding all reachable vertices.
///
/// Emits `Value::Map` with "vertex" and "depth" keys for each reachable vertex.
#[derive(Clone, Debug)]
pub struct DfsTraversalStep {
    max_depth: Option<u32>,
    edge_labels: Option<Vec<String>>,
}

impl DfsTraversalStep {
    pub fn new(max_depth: Option<u32>, edge_labels: Option<Vec<String>>) -> Self {
        Self {
            max_depth,
            edge_labels,
        }
    }
}

impl Step for DfsTraversalStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let max_depth = self.max_depth;
        let edge_labels = self.edge_labels.clone();
        Box::new(input.flat_map(move |t| {
            let source = match t.as_vertex_id() {
                Some(id) => id,
                None => return Vec::new(),
            };
            dfs_on_storage(ctx.storage(), source, max_depth, edge_labels.as_deref())
                .into_iter()
                .map(move |(vid, depth)| {
                    let mut map = indexmap::IndexMap::new();
                    map.insert("vertex".to_string(), Value::Vertex(vid));
                    map.insert("depth".to_string(), Value::Int(depth as i64));
                    t.split(Value::Map(map))
                })
                .collect::<Vec<_>>()
        }))
    }

    fn name(&self) -> &'static str {
        "dfs"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Navigation
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let source = match input.as_vertex_id() {
            Some(id) => id,
            None => return Box::new(std::iter::empty()),
        };
        let max_depth = self.max_depth;
        let edge_labels = self.edge_labels.clone();
        let storage = ctx.arc_storage();
        let results = dfs_on_storage(&*storage, source, max_depth, edge_labels.as_deref());
        Box::new(results.into_iter().map(move |(vid, depth)| {
            let mut map = indexmap::IndexMap::new();
            map.insert("vertex".to_string(), Value::Vertex(vid));
            map.insert("depth".to_string(), Value::Int(depth as i64));
            input.split(Value::Map(map))
        }))
    }
}

/// DFS on GraphStorage. Returns vec of (vertex_id, depth) in pre-order.
fn dfs_on_storage(
    storage: &dyn GraphStorage,
    source: VertexId,
    max_depth: Option<u32>,
    edge_labels: Option<&[String]>,
) -> Vec<(VertexId, u32)> {
    let mut visited = HashSet::new();
    let mut stack = Vec::new();
    let mut result = Vec::new();

    stack.push((source, 0u32));

    while let Some((vid, depth)) = stack.pop() {
        if !visited.insert(vid) {
            continue;
        }
        result.push((vid, depth));

        if let Some(max) = max_depth {
            if depth >= max {
                continue;
            }
        }
        let neighbors = expand_from_storage(storage, vid, StepDirection::Out);
        // Push in reverse order so first neighbor is visited first
        for (neighbor, _eid, edge) in neighbors.into_iter().rev() {
            if let Some(labels) = edge_labels {
                if !labels.iter().any(|l| l == &edge.label) {
                    continue;
                }
            }
            if !visited.contains(&neighbor) {
                stack.push((neighbor, depth + 1));
            }
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Bidirectional BFS Step
// ---------------------------------------------------------------------------

/// Traversal step: bidirectional BFS shortest path.
///
/// Emits a `Value::List` of vertex IDs representing the shortest unweighted path.
#[derive(Clone, Debug)]
pub struct BidirectionalBfsStep {
    target: VertexId,
}

impl BidirectionalBfsStep {
    pub fn new(target: VertexId) -> Self {
        Self { target }
    }
}

impl Step for BidirectionalBfsStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let target = self.target;
        Box::new(input.filter_map(move |t| {
            let source = t.as_vertex_id()?;
            let result = bidirectional_bfs_on_storage(ctx.storage(), source, target)?;
            Some(t.split(result))
        }))
    }

    fn name(&self) -> &'static str {
        "bidirectionalBfs"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Navigation
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let source = match input.as_vertex_id() {
            Some(id) => id,
            None => return Box::new(std::iter::empty()),
        };
        let target = self.target;
        let storage = ctx.arc_storage();
        let result = bidirectional_bfs_on_storage(&*storage, source, target);
        match result {
            Some(value) => Box::new(std::iter::once(input.split(value))),
            None => Box::new(std::iter::empty()),
        }
    }
}

/// Bidirectional BFS on GraphStorage. Returns Value::List of vertex path.
pub(crate) fn bidirectional_bfs_on_storage(
    storage: &dyn GraphStorage,
    source: VertexId,
    target: VertexId,
) -> Option<Value> {
    if source == target {
        return Some(Value::List(vec![Value::Vertex(source)]));
    }
    if storage.get_vertex(source).is_none() || storage.get_vertex(target).is_none() {
        return None;
    }

    let mut fwd_visited: HashMap<VertexId, Option<(VertexId, EdgeId)>> = HashMap::new();
    let mut bwd_visited: HashMap<VertexId, Option<(VertexId, EdgeId)>> = HashMap::new();
    let mut fwd_queue = VecDeque::new();
    let mut bwd_queue = VecDeque::new();

    fwd_visited.insert(source, None);
    bwd_visited.insert(target, None);
    fwd_queue.push_back(source);
    bwd_queue.push_back(target);

    loop {
        // Expand forward frontier
        if fwd_queue.is_empty() && bwd_queue.is_empty() {
            return None;
        }

        if !fwd_queue.is_empty() {
            let size = fwd_queue.len();
            for _ in 0..size {
                let vid = fwd_queue.pop_front().unwrap();
                let neighbors = expand_from_storage(storage, vid, StepDirection::Out);
                for (neighbor, eid, _) in neighbors {
                    if !fwd_visited.contains_key(&neighbor) {
                        fwd_visited.insert(neighbor, Some((vid, eid)));
                        fwd_queue.push_back(neighbor);
                    }
                    if bwd_visited.contains_key(&neighbor) {
                        // Meeting point found
                        return Some(build_bidir_path(
                            &fwd_visited,
                            &bwd_visited,
                            source,
                            target,
                            neighbor,
                        ));
                    }
                }
            }
        }

        // Expand backward frontier
        if !bwd_queue.is_empty() {
            let size = bwd_queue.len();
            for _ in 0..size {
                let vid = bwd_queue.pop_front().unwrap();
                // Backward: follow incoming edges
                let neighbors = expand_from_storage(storage, vid, StepDirection::In);
                for (neighbor, eid, _) in neighbors {
                    if !bwd_visited.contains_key(&neighbor) {
                        bwd_visited.insert(neighbor, Some((vid, eid)));
                        bwd_queue.push_back(neighbor);
                    }
                    if fwd_visited.contains_key(&neighbor) {
                        return Some(build_bidir_path(
                            &fwd_visited,
                            &bwd_visited,
                            source,
                            target,
                            neighbor,
                        ));
                    }
                }
            }
        }
    }
}

/// Build path from bidirectional BFS meeting point.
fn build_bidir_path(
    fwd_visited: &HashMap<VertexId, Option<(VertexId, EdgeId)>>,
    bwd_visited: &HashMap<VertexId, Option<(VertexId, EdgeId)>>,
    source: VertexId,
    target: VertexId,
    meeting: VertexId,
) -> Value {
    // Build forward path: source -> meeting
    let mut fwd_path = vec![meeting];
    let mut cur = meeting;
    while cur != source {
        if let Some(Some((parent, _))) = fwd_visited.get(&cur) {
            fwd_path.push(*parent);
            cur = *parent;
        } else {
            break;
        }
    }
    fwd_path.reverse();

    // Build backward path: meeting -> target
    let mut bwd_path = Vec::new();
    cur = meeting;
    while cur != target {
        if let Some(Some((child, _))) = bwd_visited.get(&cur) {
            bwd_path.push(*child);
            cur = *child;
        } else {
            break;
        }
    }

    // Combine: fwd_path already includes meeting, bwd_path starts after meeting
    fwd_path.extend(bwd_path);
    Value::List(fwd_path.into_iter().map(Value::Vertex).collect())
}

// ---------------------------------------------------------------------------
// IDDFS Step (Iterative Deepening DFS)
// ---------------------------------------------------------------------------

/// Traversal step: IDDFS shortest path from current vertex to a target.
///
/// Emits a `Value::List` of vertex IDs representing the path.
#[derive(Clone, Debug)]
pub struct IddfsStep {
    target: VertexId,
    max_depth: u32,
}

impl IddfsStep {
    pub fn new(target: VertexId, max_depth: u32) -> Self {
        Self { target, max_depth }
    }
}

impl Step for IddfsStep {
    type Iter<'a>
        = Box<dyn Iterator<Item = Traverser> + 'a>
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        let target = self.target;
        let max_depth = self.max_depth;
        Box::new(input.filter_map(move |t| {
            let source = t.as_vertex_id()?;
            let result = iddfs_on_storage(ctx.storage(), source, target, max_depth)?;
            Some(t.split(result))
        }))
    }

    fn name(&self) -> &'static str {
        "iddfs"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Navigation
    }

    fn apply_streaming(
        &self,
        ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let source = match input.as_vertex_id() {
            Some(id) => id,
            None => return Box::new(std::iter::empty()),
        };
        let target = self.target;
        let max_depth = self.max_depth;
        let storage = ctx.arc_storage();
        let result = iddfs_on_storage(&*storage, source, target, max_depth);
        match result {
            Some(value) => Box::new(std::iter::once(input.split(value))),
            None => Box::new(std::iter::empty()),
        }
    }
}

/// IDDFS on GraphStorage. Returns Value::List of vertex path.
pub(crate) fn iddfs_on_storage(
    storage: &dyn GraphStorage,
    source: VertexId,
    target: VertexId,
    max_depth: u32,
) -> Option<Value> {
    if source == target {
        return Some(Value::List(vec![Value::Vertex(source)]));
    }
    if storage.get_vertex(source).is_none() || storage.get_vertex(target).is_none() {
        return None;
    }

    for depth_limit in 0..=max_depth {
        if let Some(path) = dls_on_storage(storage, source, target, depth_limit) {
            return Some(Value::List(path.into_iter().map(Value::Vertex).collect()));
        }
    }
    None
}

/// Depth-limited search helper for IDDFS.
fn dls_on_storage(
    storage: &dyn GraphStorage,
    source: VertexId,
    target: VertexId,
    limit: u32,
) -> Option<Vec<VertexId>> {
    let mut stack: Vec<(VertexId, u32, Vec<VertexId>)> = vec![(source, 0, vec![source])];

    while let Some((vid, depth, path)) = stack.pop() {
        if vid == target {
            return Some(path);
        }
        if depth >= limit {
            continue;
        }
        let neighbors = expand_from_storage(storage, vid, StepDirection::Out);
        for (neighbor, _eid, _edge) in neighbors {
            if !path.contains(&neighbor) {
                let mut new_path = path.clone();
                new_path.push(neighbor);
                stack.push((neighbor, depth + 1, new_path));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cow::Graph;
    use crate::traversal::context::{ExecutionContext, SnapshotLike};
    use std::collections::HashMap as StdHashMap;

    fn make_chain_graph(n: usize) -> (Graph, Vec<VertexId>) {
        let g = Graph::new();
        let mut ids = Vec::new();
        for _ in 0..n {
            ids.push(g.add_vertex("node", StdHashMap::new()));
        }
        for i in 0..n - 1 {
            g.add_edge(ids[i], ids[i + 1], "next", StdHashMap::new())
                .unwrap();
        }
        (g, ids)
    }

    fn make_weighted_diamond() -> (Graph, Vec<VertexId>) {
        let g = Graph::new();
        let a = g.add_vertex("node", StdHashMap::new());
        let b = g.add_vertex("node", StdHashMap::new());
        let c = g.add_vertex("node", StdHashMap::new());
        let d = g.add_vertex("node", StdHashMap::new());
        g.add_edge(
            a,
            b,
            "e",
            StdHashMap::from([("w".into(), Value::Float(1.0))]),
        )
        .unwrap();
        g.add_edge(
            a,
            c,
            "e",
            StdHashMap::from([("w".into(), Value::Float(5.0))]),
        )
        .unwrap();
        g.add_edge(
            b,
            d,
            "e",
            StdHashMap::from([("w".into(), Value::Float(1.0))]),
        )
        .unwrap();
        g.add_edge(
            c,
            d,
            "e",
            StdHashMap::from([("w".into(), Value::Float(1.0))]),
        )
        .unwrap();
        (g, vec![a, b, c, d])
    }

    #[test]
    fn shortest_path_step_chain() {
        let (g, ids) = make_chain_graph(5);
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());
        let step = ShortestPathStep::new(ids[4]);
        let input: Box<dyn Iterator<Item = Traverser>> =
            Box::new(std::iter::once(Traverser::new(Value::Vertex(ids[0]))));
        let results: Vec<_> = step.apply(&ctx, input).collect();
        assert_eq!(results.len(), 1);
        match &results[0].value {
            Value::List(path) => {
                assert_eq!(path.len(), 5);
                assert_eq!(path[0], Value::Vertex(ids[0]));
                assert_eq!(path[4], Value::Vertex(ids[4]));
            }
            other => panic!("expected List, got {:?}", other),
        }
    }

    #[test]
    fn shortest_path_step_same_vertex() {
        let (g, ids) = make_chain_graph(3);
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());
        let step = ShortestPathStep::new(ids[0]);
        let input: Box<dyn Iterator<Item = Traverser>> =
            Box::new(std::iter::once(Traverser::new(Value::Vertex(ids[0]))));
        let results: Vec<_> = step.apply(&ctx, input).collect();
        assert_eq!(results.len(), 1);
        match &results[0].value {
            Value::List(path) => assert_eq!(path.len(), 1),
            other => panic!("expected List, got {:?}", other),
        }
    }

    #[test]
    fn shortest_path_step_no_path() {
        let g = Graph::new();
        let a = g.add_vertex("node", StdHashMap::new());
        let b = g.add_vertex("node", StdHashMap::new());
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());
        let step = ShortestPathStep::new(b);
        let input: Box<dyn Iterator<Item = Traverser>> =
            Box::new(std::iter::once(Traverser::new(Value::Vertex(a))));
        let results: Vec<_> = step.apply(&ctx, input).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn shortest_path_step_non_vertex_input() {
        let g = Graph::new();
        let a = g.add_vertex("node", StdHashMap::new());
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());
        let step = ShortestPathStep::new(a);
        let input: Box<dyn Iterator<Item = Traverser>> =
            Box::new(std::iter::once(Traverser::new(Value::Int(42))));
        let results: Vec<_> = step.apply(&ctx, input).collect();
        assert!(results.is_empty());
    }

    #[test]
    fn dijkstra_step_weighted() {
        let (g, ids) = make_weighted_diamond();
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());
        let step = DijkstraStep::new(ids[3], "w".to_string());
        let input: Box<dyn Iterator<Item = Traverser>> =
            Box::new(std::iter::once(Traverser::new(Value::Vertex(ids[0]))));
        let results: Vec<_> = step.apply(&ctx, input).collect();
        assert_eq!(results.len(), 1);
        match &results[0].value {
            Value::Map(map) => {
                assert_eq!(map.get("weight"), Some(&Value::Float(2.0)));
                match map.get("path") {
                    Some(Value::List(path)) => {
                        assert_eq!(path[0], Value::Vertex(ids[0]));
                        assert_eq!(path.last().unwrap(), &Value::Vertex(ids[3]));
                    }
                    other => panic!("expected List path, got {:?}", other),
                }
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn dijkstra_step_same_vertex() {
        let (g, ids) = make_weighted_diamond();
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());
        let step = DijkstraStep::new(ids[0], "w".to_string());
        let input: Box<dyn Iterator<Item = Traverser>> =
            Box::new(std::iter::once(Traverser::new(Value::Vertex(ids[0]))));
        let results: Vec<_> = step.apply(&ctx, input).collect();
        assert_eq!(results.len(), 1);
        match &results[0].value {
            Value::Map(map) => {
                assert_eq!(map.get("weight"), Some(&Value::Float(0.0)));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn dijkstra_step_no_path() {
        let g = Graph::new();
        let a = g.add_vertex("node", StdHashMap::new());
        let b = g.add_vertex("node", StdHashMap::new());
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());
        let step = DijkstraStep::new(b, "w".to_string());
        let input: Box<dyn Iterator<Item = Traverser>> =
            Box::new(std::iter::once(Traverser::new(Value::Vertex(a))));
        let results: Vec<_> = step.apply(&ctx, input).collect();
        assert!(results.is_empty());
    }

    // --- Integration tests via Gremlin fluent API ---

    #[test]
    fn gremlin_shortest_path_to() {
        let (g, ids) = make_chain_graph(5);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin.v_ids([ids[0]]).shortest_path_to(ids[4]).to_list();
        assert_eq!(results.len(), 1);
        match &results[0] {
            Value::List(path) => {
                assert_eq!(path.len(), 5);
                assert_eq!(path[0], Value::Vertex(ids[0]));
                assert_eq!(path[4], Value::Vertex(ids[4]));
            }
            other => panic!("expected List, got {:?}", other),
        }
    }

    #[test]
    fn gremlin_dijkstra_to() {
        let (g, ids) = make_weighted_diamond();
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin.v_ids([ids[0]]).dijkstra_to(ids[3], "w").to_list();
        assert_eq!(results.len(), 1);
        match &results[0] {
            Value::Map(map) => {
                assert_eq!(map.get("weight"), Some(&Value::Float(2.0)));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    #[test]
    fn gremlin_shortest_path_no_path() {
        let g = Graph::new();
        let a = g.add_vertex("node", StdHashMap::new());
        let b = g.add_vertex("node", StdHashMap::new());
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin.v_ids([a]).shortest_path_to(b).to_list();
        assert!(results.is_empty());
    }

    #[test]
    fn gremlin_dijkstra_no_path() {
        let g = Graph::new();
        let a = g.add_vertex("node", StdHashMap::new());
        let b = g.add_vertex("node", StdHashMap::new());
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin.v_ids([a]).dijkstra_to(b, "w").to_list();
        assert!(results.is_empty());
    }

    // --- A* step tests ---

    #[test]
    fn astar_step_weighted_with_heuristic() {
        let (g, ids) = make_weighted_diamond();
        // Add heuristic properties (est. distance to target ids[3])
        // ids[0]->ids[3] est=2, ids[1]->ids[3] est=1, ids[2]->ids[3] est=1
        g.set_vertex_property(ids[0], "h", Value::Float(2.0));
        g.set_vertex_property(ids[1], "h", Value::Float(1.0));
        g.set_vertex_property(ids[2], "h", Value::Float(1.0));
        g.set_vertex_property(ids[3], "h", Value::Float(0.0));

        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin.v_ids([ids[0]]).astar_to(ids[3], "w", "h").to_list();
        assert_eq!(results.len(), 1);
        match &results[0] {
            Value::Map(map) => {
                assert_eq!(map.get("weight"), Some(&Value::Float(2.0)));
            }
            other => panic!("expected Map, got {:?}", other),
        }
    }

    // --- BFS traversal step tests ---

    #[test]
    fn bfs_traversal_step_chain() {
        let (g, ids) = make_chain_graph(4);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin.v_ids([ids[0]]).bfs_traversal(None, None).to_list();
        // Should visit all 4 vertices
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn bfs_traversal_step_max_depth() {
        let (g, ids) = make_chain_graph(5);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin
            .v_ids([ids[0]])
            .bfs_traversal(Some(2), None)
            .to_list();
        // depth 0: ids[0], depth 1: ids[1], depth 2: ids[2] — 3 vertices
        assert_eq!(results.len(), 3);
    }

    // --- DFS traversal step tests ---

    #[test]
    fn dfs_traversal_step_chain() {
        let (g, ids) = make_chain_graph(4);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin.v_ids([ids[0]]).dfs_traversal(None, None).to_list();
        assert_eq!(results.len(), 4);
    }

    // --- Bidirectional BFS step tests ---

    #[test]
    fn bidirectional_bfs_step_chain() {
        let (g, ids) = make_chain_graph(5);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin
            .v_ids([ids[0]])
            .bidirectional_bfs_to(ids[4])
            .to_list();
        assert_eq!(results.len(), 1);
        match &results[0] {
            Value::List(path) => {
                assert_eq!(path.len(), 5);
                assert_eq!(path[0], Value::Vertex(ids[0]));
                assert_eq!(path[4], Value::Vertex(ids[4]));
            }
            other => panic!("expected List, got {:?}", other),
        }
    }

    #[test]
    fn bidirectional_bfs_step_no_path() {
        let g = Graph::new();
        let a = g.add_vertex("node", StdHashMap::new());
        let b = g.add_vertex("node", StdHashMap::new());
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin.v_ids([a]).bidirectional_bfs_to(b).to_list();
        assert!(results.is_empty());
    }

    // --- IDDFS step tests ---

    #[test]
    fn iddfs_step_chain() {
        let (g, ids) = make_chain_graph(5);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin.v_ids([ids[0]]).iddfs_to(ids[4], 10).to_list();
        assert_eq!(results.len(), 1);
        match &results[0] {
            Value::List(path) => {
                assert_eq!(path.len(), 5);
                assert_eq!(path[0], Value::Vertex(ids[0]));
                assert_eq!(path[4], Value::Vertex(ids[4]));
            }
            other => panic!("expected List, got {:?}", other),
        }
    }

    #[test]
    fn iddfs_step_no_path() {
        let g = Graph::new();
        let a = g.add_vertex("node", StdHashMap::new());
        let b = g.add_vertex("node", StdHashMap::new());
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        let results = gremlin.v_ids([a]).iddfs_to(b, 5).to_list();
        assert!(results.is_empty());
    }

    #[test]
    fn iddfs_step_depth_limit() {
        let (g, ids) = make_chain_graph(5);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();
        // max_depth=2 can't reach ids[4] (needs 4 hops)
        let results = gremlin.v_ids([ids[0]]).iddfs_to(ids[4], 2).to_list();
        assert!(results.is_empty());
    }

    // --- Gremlin parser + compiler integration tests ---

    #[test]
    fn gremlin_parse_shortest_path() {
        use crate::gremlin::{compile, parse};
        let (g, ids) = make_chain_graph(5);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();

        let query = format!("g.V({}).shortestPath({})", ids[0].0, ids[4].0);
        let ast = parse(&query).unwrap();
        let compiled = compile(&ast, &gremlin).unwrap();
        let result = compiled.execute();
        match result {
            crate::gremlin::ExecutionResult::List(values) => {
                assert_eq!(values.len(), 1);
                match &values[0] {
                    Value::List(path) => assert_eq!(path.len(), 5),
                    other => panic!("expected List, got {:?}", other),
                }
            }
            other => panic!("expected List result, got {:?}", other),
        }
    }

    #[test]
    fn gremlin_parse_shortest_path_weighted() {
        use crate::gremlin::{compile, parse};
        let (g, ids) = make_weighted_diamond();
        let snap = g.snapshot();
        let gremlin = snap.gremlin();

        let query = format!(
            "g.V({}).shortestPath({}).by('w')",
            ids[0].0, ids[3].0
        );
        let ast = parse(&query).unwrap();
        let compiled = compile(&ast, &gremlin).unwrap();
        let result = compiled.execute();
        match result {
            crate::gremlin::ExecutionResult::List(values) => {
                assert_eq!(values.len(), 1);
                match &values[0] {
                    Value::Map(map) => {
                        assert_eq!(map.get("weight"), Some(&Value::Float(2.0)));
                    }
                    other => panic!("expected Map, got {:?}", other),
                }
            }
            other => panic!("expected List result, got {:?}", other),
        }
    }

    #[test]
    fn gremlin_parse_bfs() {
        use crate::gremlin::{compile, parse};
        let (g, ids) = make_chain_graph(4);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();

        let query = format!("g.V({}).bfs()", ids[0].0);
        let ast = parse(&query).unwrap();
        let compiled = compile(&ast, &gremlin).unwrap();
        let result = compiled.execute();
        match result {
            crate::gremlin::ExecutionResult::List(values) => {
                assert_eq!(values.len(), 4);
            }
            other => panic!("expected List result, got {:?}", other),
        }
    }

    #[test]
    fn gremlin_parse_dfs() {
        use crate::gremlin::{compile, parse};
        let (g, ids) = make_chain_graph(4);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();

        let query = format!("g.V({}).dfs()", ids[0].0);
        let ast = parse(&query).unwrap();
        let compiled = compile(&ast, &gremlin).unwrap();
        let result = compiled.execute();
        match result {
            crate::gremlin::ExecutionResult::List(values) => {
                assert_eq!(values.len(), 4);
            }
            other => panic!("expected List result, got {:?}", other),
        }
    }

    #[test]
    fn gremlin_parse_bidirectional_bfs() {
        use crate::gremlin::{compile, parse};
        let (g, ids) = make_chain_graph(5);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();

        let query = format!("g.V({}).bidirectionalBfs({})", ids[0].0, ids[4].0);
        let ast = parse(&query).unwrap();
        let compiled = compile(&ast, &gremlin).unwrap();
        let result = compiled.execute();
        match result {
            crate::gremlin::ExecutionResult::List(values) => {
                assert_eq!(values.len(), 1);
                match &values[0] {
                    Value::List(path) => assert_eq!(path.len(), 5),
                    other => panic!("expected List, got {:?}", other),
                }
            }
            other => panic!("expected List result, got {:?}", other),
        }
    }

    #[test]
    fn gremlin_parse_iddfs() {
        use crate::gremlin::{compile, parse};
        let (g, ids) = make_chain_graph(5);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();

        let query = format!("g.V({}).iddfs({}, 10)", ids[0].0, ids[4].0);
        let ast = parse(&query).unwrap();
        let compiled = compile(&ast, &gremlin).unwrap();
        let result = compiled.execute();
        match result {
            crate::gremlin::ExecutionResult::List(values) => {
                assert_eq!(values.len(), 1);
                match &values[0] {
                    Value::List(path) => assert_eq!(path.len(), 5),
                    other => panic!("expected List, got {:?}", other),
                }
            }
            other => panic!("expected List result, got {:?}", other),
        }
    }

    #[test]
    fn gremlin_parse_bfs_with_max_depth() {
        use crate::gremlin::{compile, parse};
        let (g, ids) = make_chain_graph(5);
        let snap = g.snapshot();
        let gremlin = snap.gremlin();

        let query = format!("g.V({}).bfs().with('maxDepth', 2)", ids[0].0);
        let ast = parse(&query).unwrap();
        let compiled = compile(&ast, &gremlin).unwrap();
        let result = compiled.execute();
        match result {
            crate::gremlin::ExecutionResult::List(values) => {
                // depth 0, 1, 2 = 3 vertices
                assert_eq!(values.len(), 3);
            }
            other => panic!("expected List result, got {:?}", other),
        }
    }
}
