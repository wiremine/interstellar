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
}
