//! Pathfinding algorithms: shortest path, Dijkstra, A*, and Yen's k-shortest paths.
//!
//! All algorithms are generic over [`GraphAccess`] and work with any storage backend.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

use crate::graph_access::GraphAccess;
use crate::value::{EdgeId, OrderedFloat, VertexId};

use super::common::{AlgorithmError, Direction, PathResult, WeightFn};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Expand neighbors in the given direction, returning (neighbor, edge_id, edge_properties).
fn expand_with_props<G: GraphAccess>(
    graph: &G,
    vid: VertexId,
    direction: Direction,
) -> Vec<(VertexId, EdgeId, HashMap<String, crate::value::Value>)> {
    let mut result = Vec::new();

    if matches!(direction, Direction::Out | Direction::Both) {
        for eid in graph.out_edge_ids(vid) {
            if let Some(edge) = graph.get_edge(eid) {
                result.push((edge.dst, eid, edge.properties.clone()));
            }
        }
    }

    if matches!(direction, Direction::In | Direction::Both) {
        for eid in graph.in_edge_ids(vid) {
            if let Some(edge) = graph.get_edge(eid) {
                result.push((edge.src, eid, edge.properties.clone()));
            }
        }
    }

    result
}

/// Reconstruct a path from a predecessor map.
fn reconstruct_path(
    prev: &HashMap<VertexId, (VertexId, EdgeId)>,
    source: VertexId,
    target: VertexId,
    weight: f64,
) -> PathResult {
    let mut vertices = vec![target];
    let mut edges = Vec::new();
    let mut cur = target;
    while cur != source {
        let (parent, eid) = prev[&cur];
        edges.push(eid);
        vertices.push(parent);
        cur = parent;
    }
    vertices.reverse();
    edges.reverse();
    PathResult {
        vertices,
        edges,
        weight,
    }
}

// ---------------------------------------------------------------------------
// Unweighted Shortest Path
// ---------------------------------------------------------------------------

/// Shortest path in an unweighted graph using BFS.
///
/// Returns the first shortest path found. The weight in the result is the hop count.
///
/// # Errors
///
/// - [`AlgorithmError::VertexNotFound`] if source or target doesn't exist
/// - [`AlgorithmError::NoPath`] if no path exists
pub fn shortest_path_unweighted<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    direction: Direction,
    label_filter: Option<&[String]>,
) -> Result<PathResult, AlgorithmError> {
    if graph.get_vertex(source).is_none() {
        return Err(AlgorithmError::VertexNotFound(source));
    }
    if graph.get_vertex(target).is_none() {
        return Err(AlgorithmError::VertexNotFound(target));
    }
    if source == target {
        return Ok(PathResult {
            vertices: vec![source],
            edges: vec![],
            weight: 0.0,
        });
    }

    let mut visited = HashSet::new();
    let mut prev: HashMap<VertexId, (VertexId, EdgeId)> = HashMap::new();
    let mut queue = VecDeque::new();

    visited.insert(source);
    queue.push_back(source);

    while let Some(vid) = queue.pop_front() {
        let neighbors = super::traversal::expand_neighbors(graph, vid, direction, label_filter);
        for (neighbor, eid) in neighbors {
            if visited.insert(neighbor) {
                prev.insert(neighbor, (vid, eid));
                if neighbor == target {
                    let weight = {
                        let mut count = 1.0;
                        let mut cur = target;
                        while let Some((p, _)) = prev.get(&cur) {
                            if *p == source {
                                break;
                            }
                            count += 1.0;
                            cur = *p;
                        }
                        count
                    };
                    return Ok(reconstruct_path(&prev, source, target, weight));
                }
                queue.push_back(neighbor);
            }
        }
    }

    Err(AlgorithmError::NoPath {
        from: source,
        to: target,
    })
}

// ---------------------------------------------------------------------------
// Dijkstra
// ---------------------------------------------------------------------------

/// Dijkstra's shortest path algorithm for non-negative weighted graphs.
///
/// # Complexity
/// - Time: O((V + E) log V) with binary heap
/// - Space: O(V)
///
/// # Errors
/// - [`AlgorithmError::VertexNotFound`] if source or target doesn't exist
/// - [`AlgorithmError::InvalidWeight`] if weight function returns `None`
/// - [`AlgorithmError::NoPath`] if target is unreachable
pub fn dijkstra<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    weight_fn: &WeightFn,
    direction: Direction,
) -> Result<PathResult, AlgorithmError> {
    if graph.get_vertex(source).is_none() {
        return Err(AlgorithmError::VertexNotFound(source));
    }
    if graph.get_vertex(target).is_none() {
        return Err(AlgorithmError::VertexNotFound(target));
    }
    if source == target {
        return Ok(PathResult {
            vertices: vec![source],
            edges: vec![],
            weight: 0.0,
        });
    }

    let mut dist: HashMap<VertexId, f64> = HashMap::new();
    let mut prev: HashMap<VertexId, (VertexId, EdgeId)> = HashMap::new();
    let mut heap: BinaryHeap<Reverse<(OrderedFloat, VertexId)>> = BinaryHeap::new();

    dist.insert(source, 0.0);
    heap.push(Reverse((OrderedFloat(0.0), source)));

    while let Some(Reverse((OrderedFloat(d), vid))) = heap.pop() {
        if vid == target {
            return Ok(reconstruct_path(&prev, source, target, d));
        }

        if d > *dist.get(&vid).unwrap_or(&f64::INFINITY) {
            continue;
        }

        let neighbors = expand_with_props(graph, vid, direction);
        for (neighbor, eid, props) in neighbors {
            let w = match weight_fn(eid, &props) {
                Some(w) => w,
                None => {
                    return Err(AlgorithmError::InvalidWeight(format!(
                        "edge {:?}",
                        eid
                    )));
                }
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

    Err(AlgorithmError::NoPath {
        from: source,
        to: target,
    })
}

/// Single-source Dijkstra returning distances to ALL reachable vertices.
///
/// Returns a map from vertex ID to `(distance, path)`.
///
/// # Errors
/// - [`AlgorithmError::VertexNotFound`] if source doesn't exist
/// - [`AlgorithmError::InvalidWeight`] if weight function returns `None`
pub fn dijkstra_all<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    weight_fn: &WeightFn,
    direction: Direction,
) -> Result<HashMap<VertexId, (f64, PathResult)>, AlgorithmError> {
    if graph.get_vertex(source).is_none() {
        return Err(AlgorithmError::VertexNotFound(source));
    }

    let mut dist: HashMap<VertexId, f64> = HashMap::new();
    let mut prev: HashMap<VertexId, (VertexId, EdgeId)> = HashMap::new();
    let mut heap: BinaryHeap<Reverse<(OrderedFloat, VertexId)>> = BinaryHeap::new();

    dist.insert(source, 0.0);
    heap.push(Reverse((OrderedFloat(0.0), source)));

    while let Some(Reverse((OrderedFloat(d), vid))) = heap.pop() {
        if d > *dist.get(&vid).unwrap_or(&f64::INFINITY) {
            continue;
        }

        let neighbors = expand_with_props(graph, vid, direction);
        for (neighbor, eid, props) in neighbors {
            let w = match weight_fn(eid, &props) {
                Some(w) => w,
                None => {
                    return Err(AlgorithmError::InvalidWeight(format!(
                        "edge {:?}",
                        eid
                    )));
                }
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

    // Reconstruct all paths
    let mut results = HashMap::new();
    for (&vid, &d) in &dist {
        let path = reconstruct_path_or_source(&prev, source, vid, d);
        results.insert(vid, (d, path));
    }

    Ok(results)
}

/// Reconstruct path, handling the source vertex case (no predecessor).
fn reconstruct_path_or_source(
    prev: &HashMap<VertexId, (VertexId, EdgeId)>,
    source: VertexId,
    target: VertexId,
    weight: f64,
) -> PathResult {
    if source == target {
        return PathResult {
            vertices: vec![source],
            edges: vec![],
            weight: 0.0,
        };
    }
    reconstruct_path(prev, source, target, weight)
}

// ---------------------------------------------------------------------------
// A*
// ---------------------------------------------------------------------------

/// A* pathfinding with a user-supplied heuristic.
///
/// The heuristic `h(v)` must be admissible (never overestimates) for optimal results.
///
/// # Complexity
/// - Time: O((V + E) log V) worst case, typically much better with a good heuristic
/// - Space: O(V)
///
/// # Errors
/// - [`AlgorithmError::VertexNotFound`] if source or target doesn't exist
/// - [`AlgorithmError::InvalidWeight`] if weight function returns `None`
/// - [`AlgorithmError::NoPath`] if target is unreachable
pub fn astar<G, H>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    weight_fn: &WeightFn,
    heuristic: H,
    direction: Direction,
) -> Result<PathResult, AlgorithmError>
where
    G: GraphAccess,
    H: Fn(VertexId) -> f64,
{
    if graph.get_vertex(source).is_none() {
        return Err(AlgorithmError::VertexNotFound(source));
    }
    if graph.get_vertex(target).is_none() {
        return Err(AlgorithmError::VertexNotFound(target));
    }
    if source == target {
        return Ok(PathResult {
            vertices: vec![source],
            edges: vec![],
            weight: 0.0,
        });
    }

    let mut g_score: HashMap<VertexId, f64> = HashMap::new();
    let mut prev: HashMap<VertexId, (VertexId, EdgeId)> = HashMap::new();
    // Heap entries: (f_score, g_score, vertex)
    let mut heap: BinaryHeap<Reverse<(OrderedFloat, OrderedFloat, VertexId)>> = BinaryHeap::new();

    g_score.insert(source, 0.0);
    let h = heuristic(source);
    heap.push(Reverse((OrderedFloat(h), OrderedFloat(0.0), source)));

    while let Some(Reverse((_, OrderedFloat(g), vid))) = heap.pop() {
        if vid == target {
            return Ok(reconstruct_path(&prev, source, target, g));
        }

        if g > *g_score.get(&vid).unwrap_or(&f64::INFINITY) {
            continue;
        }

        let neighbors = expand_with_props(graph, vid, direction);
        for (neighbor, eid, props) in neighbors {
            let w = match weight_fn(eid, &props) {
                Some(w) => w,
                None => {
                    return Err(AlgorithmError::InvalidWeight(format!(
                        "edge {:?}",
                        eid
                    )));
                }
            };

            let tentative_g = g + w;
            let current_g = g_score.get(&neighbor).copied().unwrap_or(f64::INFINITY);
            if tentative_g < current_g {
                g_score.insert(neighbor, tentative_g);
                prev.insert(neighbor, (vid, eid));
                let f = tentative_g + heuristic(neighbor);
                heap.push(Reverse((OrderedFloat(f), OrderedFloat(tentative_g), neighbor)));
            }
        }
    }

    Err(AlgorithmError::NoPath {
        from: source,
        to: target,
    })
}

// ---------------------------------------------------------------------------
// Yen's K-Shortest Paths
// ---------------------------------------------------------------------------

/// Yen's algorithm for finding the K shortest loopless paths.
///
/// # Complexity
/// - Time: O(K * V * (V + E) log V)
/// - Space: O(K * V)
///
/// # Errors
/// - [`AlgorithmError::VertexNotFound`] if source or target doesn't exist
/// - Returns fewer than `k` paths if fewer exist
pub fn k_shortest_paths<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    k: usize,
    weight_fn: &WeightFn,
    direction: Direction,
) -> Result<Vec<PathResult>, AlgorithmError> {
    if graph.get_vertex(source).is_none() {
        return Err(AlgorithmError::VertexNotFound(source));
    }
    if graph.get_vertex(target).is_none() {
        return Err(AlgorithmError::VertexNotFound(target));
    }
    if k == 0 {
        return Ok(vec![]);
    }

    // Find first shortest path
    let first = match dijkstra(graph, source, target, weight_fn, direction) {
        Ok(p) => p,
        Err(AlgorithmError::NoPath { .. }) => return Ok(vec![]),
        Err(e) => return Err(e),
    };

    let mut a: Vec<PathResult> = vec![first]; // Accepted paths
    let mut b: BinaryHeap<Reverse<(OrderedFloat, PathResult)>> = BinaryHeap::new(); // Candidates

    for k_idx in 1..k {
        let prev_path = &a[k_idx - 1];

        for spur_idx in 0..prev_path.vertices.len() - 1 {
            let spur_node = prev_path.vertices[spur_idx];
            let root_path_verts = &prev_path.vertices[..=spur_idx];
            let root_path_edges = &prev_path.edges[..spur_idx];

            // Compute root path weight
            let root_weight: f64 = if spur_idx == 0 {
                0.0
            } else {
                // Sum weights along root path
                let mut w = 0.0;
                for &eid in root_path_edges {
                    if let Some(edge) = graph.get_edge(eid) {
                        w += weight_fn(eid, &edge.properties).unwrap_or(1.0);
                    }
                }
                w
            };

            // Edges to exclude: edges from spur_node that are part of existing shortest paths
            // with the same root path prefix
            let mut excluded_edges: HashSet<EdgeId> = HashSet::new();
            let mut excluded_vertices: HashSet<VertexId> = HashSet::new();

            // Exclude root path vertices (except spur node) from spur path
            for &v in &root_path_verts[..spur_idx] {
                excluded_vertices.insert(v);
            }

            // Exclude edges from spur node that are in paths with same root
            for path in &a {
                if path.vertices.len() > spur_idx
                    && path.vertices[..=spur_idx] == *root_path_verts
                    && spur_idx < path.edges.len()
                {
                    excluded_edges.insert(path.edges[spur_idx]);
                }
            }

            // Run modified Dijkstra from spur_node avoiding excluded vertices/edges
            if let Ok(spur_path) = dijkstra_excluding(
                graph,
                spur_node,
                target,
                weight_fn,
                direction,
                &excluded_vertices,
                &excluded_edges,
            ) {
                // Combine root + spur
                let mut combined_verts: Vec<VertexId> = root_path_verts.to_vec();
                combined_verts.extend_from_slice(&spur_path.vertices[1..]);
                let mut combined_edges: Vec<EdgeId> = root_path_edges.to_vec();
                combined_edges.extend_from_slice(&spur_path.edges);
                let combined_weight = root_weight + spur_path.weight;

                let candidate = PathResult {
                    vertices: combined_verts,
                    edges: combined_edges,
                    weight: combined_weight,
                };

                // Check if this path is already in B or A
                let already_exists = a.iter().any(|p| p.vertices == candidate.vertices)
                    || b.iter()
                        .any(|Reverse((_, p))| p.vertices == candidate.vertices);

                if !already_exists {
                    b.push(Reverse((OrderedFloat(candidate.weight), candidate)));
                }
            }
        }

        if let Some(Reverse((_, best))) = b.pop() {
            a.push(best);
        } else {
            break; // No more paths
        }
    }

    Ok(a)
}

/// Dijkstra with vertex and edge exclusions (used by Yen's algorithm).
fn dijkstra_excluding<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    weight_fn: &WeightFn,
    direction: Direction,
    excluded_vertices: &HashSet<VertexId>,
    excluded_edges: &HashSet<EdgeId>,
) -> Result<PathResult, AlgorithmError> {
    if source == target {
        return Ok(PathResult {
            vertices: vec![source],
            edges: vec![],
            weight: 0.0,
        });
    }

    let mut dist: HashMap<VertexId, f64> = HashMap::new();
    let mut prev: HashMap<VertexId, (VertexId, EdgeId)> = HashMap::new();
    let mut heap: BinaryHeap<Reverse<(OrderedFloat, VertexId)>> = BinaryHeap::new();

    dist.insert(source, 0.0);
    heap.push(Reverse((OrderedFloat(0.0), source)));

    while let Some(Reverse((OrderedFloat(d), vid))) = heap.pop() {
        if vid == target {
            return Ok(reconstruct_path(&prev, source, target, d));
        }

        if d > *dist.get(&vid).unwrap_or(&f64::INFINITY) {
            continue;
        }

        let neighbors = expand_with_props(graph, vid, direction);
        for (neighbor, eid, props) in neighbors {
            if excluded_vertices.contains(&neighbor) || excluded_edges.contains(&eid) {
                continue;
            }

            let w = match weight_fn(eid, &props) {
                Some(w) => w,
                None => continue, // Skip edges with invalid weights in exclusion context
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

    Err(AlgorithmError::NoPath {
        from: source,
        to: target,
    })
}

// We need PartialEq and Eq for PathResult to work in BinaryHeap with Reverse
impl PartialOrd for PathResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for PathResult {}

impl Ord for PathResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        OrderedFloat(self.weight).cmp(&OrderedFloat(other.weight))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithms::common::unit_weight;
    use crate::storage::Graph;
    use crate::value::Value;
    use std::sync::Arc;

    fn make_chain(n: usize) -> (Arc<Graph>, Vec<VertexId>) {
        let g = Arc::new(Graph::new());
        let mut ids = Vec::new();
        for _ in 0..n {
            ids.push(g.add_vertex("node", HashMap::new()));
        }
        for i in 0..n - 1 {
            g.add_edge(ids[i], ids[i + 1], "next", HashMap::new())
                .unwrap();
        }
        (g, ids)
    }

    fn make_weighted_graph() -> (Arc<Graph>, Vec<VertexId>) {
        // 0 --1--> 1 --1--> 3
        // 0 --5--> 2 --1--> 3
        // Shortest: 0->1->3 (weight 2)
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        let c = g.add_vertex("node", HashMap::new());
        let d = g.add_vertex("node", HashMap::new());
        g.add_edge(
            a,
            b,
            "e",
            HashMap::from([("w".into(), Value::Float(1.0))]),
        )
        .unwrap();
        g.add_edge(
            a,
            c,
            "e",
            HashMap::from([("w".into(), Value::Float(5.0))]),
        )
        .unwrap();
        g.add_edge(
            b,
            d,
            "e",
            HashMap::from([("w".into(), Value::Float(1.0))]),
        )
        .unwrap();
        g.add_edge(
            c,
            d,
            "e",
            HashMap::from([("w".into(), Value::Float(1.0))]),
        )
        .unwrap();
        (g, vec![a, b, c, d])
    }

    // --- Unweighted Shortest Path ---

    #[test]
    fn shortest_unweighted_chain() {
        let (g, ids) = make_chain(5);
        
        let path =
            shortest_path_unweighted(&g, ids[0], ids[4], Direction::Out, None).unwrap();
        assert_eq!(path.vertices.first(), Some(&ids[0]));
        assert_eq!(path.vertices.last(), Some(&ids[4]));
        assert_eq!(path.vertices.len(), 5);
        assert_eq!(path.edges.len(), 4);
    }

    #[test]
    fn shortest_unweighted_same() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        
        let path = shortest_path_unweighted(&g, a, a, Direction::Out, None).unwrap();
        assert_eq!(path.vertices, vec![a]);
        assert_eq!(path.weight, 0.0);
    }

    #[test]
    fn shortest_unweighted_no_path() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        
        let result = shortest_path_unweighted(&g, a, b, Direction::Out, None);
        assert!(matches!(result, Err(AlgorithmError::NoPath { .. })));
    }

    #[test]
    fn shortest_unweighted_vertex_not_found() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        
        assert!(matches!(
            shortest_path_unweighted(&g, a, VertexId(999), Direction::Out, None),
            Err(AlgorithmError::VertexNotFound(_))
        ));
        assert!(matches!(
            shortest_path_unweighted(&g, VertexId(999), a, Direction::Out, None),
            Err(AlgorithmError::VertexNotFound(_))
        ));
    }

    // --- Dijkstra ---

    #[test]
    fn dijkstra_weighted() {
        let (g, ids) = make_weighted_graph();
        
        let wf = crate::algorithms::common::property_weight("w".into());
        let path = dijkstra(&g, ids[0], ids[3], &wf, Direction::Out).unwrap();
        assert_eq!(path.weight, 2.0); // 0->1->3
        assert_eq!(path.vertices.first(), Some(&ids[0]));
        assert_eq!(path.vertices.last(), Some(&ids[3]));
    }

    #[test]
    fn dijkstra_same_vertex() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        
        let wf = unit_weight();
        let path = dijkstra(&g, a, a, &wf, Direction::Out).unwrap();
        assert_eq!(path.vertices, vec![a]);
        assert_eq!(path.weight, 0.0);
    }

    #[test]
    fn dijkstra_no_path() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        
        let wf = unit_weight();
        assert!(matches!(
            dijkstra(&g, a, b, &wf, Direction::Out),
            Err(AlgorithmError::NoPath { .. })
        ));
    }

    #[test]
    fn dijkstra_vertex_not_found() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        
        let wf = unit_weight();
        assert!(matches!(
            dijkstra(&g, a, VertexId(999), &wf, Direction::Out),
            Err(AlgorithmError::VertexNotFound(_))
        ));
    }

    #[test]
    fn dijkstra_invalid_weight() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        g.add_edge(
            a,
            b,
            "e",
            HashMap::from([("w".into(), Value::String("bad".into()))]),
        )
        .unwrap();
        
        let wf = crate::algorithms::common::property_weight("w".into());
        assert!(matches!(
            dijkstra(&g, a, b, &wf, Direction::Out),
            Err(AlgorithmError::InvalidWeight(_))
        ));
    }

    // --- Dijkstra All ---

    #[test]
    fn dijkstra_all_basic() {
        let (g, ids) = make_chain(4);
        
        let wf = unit_weight();
        let result = dijkstra_all(&g, ids[0], &wf, Direction::Out).unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[&ids[0]].0, 0.0);
        assert_eq!(result[&ids[1]].0, 1.0);
        assert_eq!(result[&ids[2]].0, 2.0);
        assert_eq!(result[&ids[3]].0, 3.0);
    }

    #[test]
    fn dijkstra_all_vertex_not_found() {
        let g = Arc::new(Graph::new());
        
        let wf = unit_weight();
        assert!(matches!(
            dijkstra_all(&g, VertexId(999), &wf, Direction::Out),
            Err(AlgorithmError::VertexNotFound(_))
        ));
    }

    // --- A* ---

    #[test]
    fn astar_weighted() {
        let (g, ids) = make_weighted_graph();
        
        let wf = crate::algorithms::common::property_weight("w".into());
        let path = astar(
            &g,
            ids[0],
            ids[3],
            &wf,
            |_| 0.0, // trivial heuristic (becomes Dijkstra)
            Direction::Out,
        )
        .unwrap();
        assert_eq!(path.weight, 2.0);
    }

    #[test]
    fn astar_same_vertex() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        
        let wf = unit_weight();
        let path = astar(&g, a, a, &wf, |_| 0.0, Direction::Out).unwrap();
        assert_eq!(path.vertices, vec![a]);
    }

    #[test]
    fn astar_no_path() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        
        let wf = unit_weight();
        assert!(matches!(
            astar(&g, a, b, &wf, |_| 0.0, Direction::Out),
            Err(AlgorithmError::NoPath { .. })
        ));
    }

    #[test]
    fn astar_vertex_not_found() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        
        let wf = unit_weight();
        assert!(matches!(
            astar(&g, a, VertexId(999), &wf, |_| 0.0, Direction::Out),
            Err(AlgorithmError::VertexNotFound(_))
        ));
    }

    // --- K-Shortest Paths ---

    #[test]
    fn k_shortest_paths_basic() {
        // Diamond: two paths of length 2
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        let c = g.add_vertex("node", HashMap::new());
        let d = g.add_vertex("node", HashMap::new());
        g.add_edge(a, b, "e", HashMap::from([("w".into(), Value::Float(1.0))]))
            .unwrap();
        g.add_edge(a, c, "e", HashMap::from([("w".into(), Value::Float(2.0))]))
            .unwrap();
        g.add_edge(b, d, "e", HashMap::from([("w".into(), Value::Float(1.0))]))
            .unwrap();
        g.add_edge(c, d, "e", HashMap::from([("w".into(), Value::Float(1.0))]))
            .unwrap();
        
        let wf = crate::algorithms::common::property_weight("w".into());
        let paths = k_shortest_paths(&g, a, d, 3, &wf, Direction::Out).unwrap();
        assert!(paths.len() >= 2);
        // Non-decreasing weight order
        for w in paths.windows(2) {
            assert!(w[0].weight <= w[1].weight);
        }
        assert_eq!(paths[0].weight, 2.0); // a->b->d
        assert_eq!(paths[1].weight, 3.0); // a->c->d
    }

    #[test]
    fn k_shortest_paths_zero_k() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        g.add_edge(a, b, "e", HashMap::new()).unwrap();
        
        let wf = unit_weight();
        let paths = k_shortest_paths(&g, a, b, 0, &wf, Direction::Out).unwrap();
        assert!(paths.is_empty());
    }

    #[test]
    fn k_shortest_paths_no_path() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        
        let wf = unit_weight();
        let paths = k_shortest_paths(&g, a, b, 3, &wf, Direction::Out).unwrap();
        assert!(paths.is_empty());
    }

    #[test]
    fn k_shortest_paths_vertex_not_found() {
        let g = Arc::new(Graph::new());
        
        let wf = unit_weight();
        assert!(matches!(
            k_shortest_paths(&g, VertexId(1), VertexId(2), 3, &wf, Direction::Out),
            Err(AlgorithmError::VertexNotFound(_))
        ));
    }
}
