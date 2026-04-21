//! Graph traversal algorithms: BFS, DFS, bidirectional BFS, and IDDFS.
//!
//! All traversals are generic over [`GraphAccess`] and work with any storage backend.
//! BFS and DFS are lazy iterators; bidirectional BFS and IDDFS return paths directly.

use std::collections::{HashMap, HashSet, VecDeque};

use crate::graph_access::GraphAccess;
use crate::value::{EdgeId, VertexId};

use super::common::{AlgorithmError, Direction, PathResult};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Expand neighbors of `vid` in the given direction, returning (neighbor_id, edge_id).
pub(super) fn expand_neighbors<G: GraphAccess>(
    graph: &G,
    vid: VertexId,
    direction: Direction,
    label_filter: Option<&[String]>,
) -> Vec<(VertexId, EdgeId)> {
    let mut result = Vec::new();

    if matches!(direction, Direction::Out | Direction::Both) {
        for eid in graph.out_edge_ids(vid) {
            if let Some(edge) = graph.get_edge(eid) {
                if let Some(labels) = label_filter {
                    if !labels.iter().any(|l| l == &edge.label) {
                        continue;
                    }
                }
                result.push((edge.dst, eid));
            }
        }
    }

    if matches!(direction, Direction::In | Direction::Both) {
        for eid in graph.in_edge_ids(vid) {
            if let Some(edge) = graph.get_edge(eid) {
                if let Some(labels) = label_filter {
                    if !labels.iter().any(|l| l == &edge.label) {
                        continue;
                    }
                }
                result.push((edge.src, eid));
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// BFS Iterator
// ---------------------------------------------------------------------------

/// Breadth-first traversal yielding `(VertexId, depth)` lazily.
pub struct Bfs<G: GraphAccess> {
    graph: G,
    queue: VecDeque<(VertexId, u32)>,
    visited: HashSet<VertexId>,
    direction: Direction,
    max_depth: Option<u32>,
    label_filter: Option<Vec<String>>,
}

impl<G: GraphAccess> Bfs<G> {
    /// Create a new BFS starting from `start`.
    pub fn new(graph: G, start: VertexId) -> Self {
        let mut queue = VecDeque::new();
        queue.push_back((start, 0));
        Self {
            graph,
            queue,
            visited: HashSet::new(),
            direction: Direction::Out,
            max_depth: None,
            label_filter: None,
        }
    }

    /// Set the traversal direction.
    pub fn direction(mut self, dir: Direction) -> Self {
        self.direction = dir;
        self
    }

    /// Limit the maximum depth of traversal.
    pub fn max_depth(mut self, depth: u32) -> Self {
        self.max_depth = Some(depth);
        self
    }

    /// Only traverse edges with these labels.
    pub fn label_filter(mut self, labels: Vec<String>) -> Self {
        self.label_filter = Some(labels);
        self
    }
}

impl<G: GraphAccess> Iterator for Bfs<G> {
    type Item = (VertexId, u32);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((vid, depth)) = self.queue.pop_front() {
            if let Some(max) = self.max_depth {
                if depth > max {
                    continue;
                }
            }
            if !self.visited.insert(vid) {
                continue;
            }
            // Skip vertices that don't exist in the graph
            if self.graph.get_vertex(vid).is_none() {
                continue;
            }

            // Expand neighbors if we haven't reached max depth
            if self.max_depth.map_or(true, |m| depth < m) {
                let neighbors = expand_neighbors(
                    &self.graph,
                    vid,
                    self.direction,
                    self.label_filter.as_deref(),
                );
                for (neighbor, _eid) in neighbors {
                    if !self.visited.contains(&neighbor) {
                        self.queue.push_back((neighbor, depth + 1));
                    }
                }
            }

            return Some((vid, depth));
        }
        None
    }
}

// ---------------------------------------------------------------------------
// DFS Iterator
// ---------------------------------------------------------------------------

/// Depth-first traversal yielding `(VertexId, depth)` lazily (pre-order).
pub struct Dfs<G: GraphAccess> {
    graph: G,
    stack: Vec<(VertexId, u32)>,
    visited: HashSet<VertexId>,
    direction: Direction,
    max_depth: Option<u32>,
    label_filter: Option<Vec<String>>,
}

impl<G: GraphAccess> Dfs<G> {
    /// Create a new DFS starting from `start`.
    pub fn new(graph: G, start: VertexId) -> Self {
        Self {
            graph,
            stack: vec![(start, 0)],
            visited: HashSet::new(),
            direction: Direction::Out,
            max_depth: None,
            label_filter: None,
        }
    }

    /// Set the traversal direction.
    pub fn direction(mut self, dir: Direction) -> Self {
        self.direction = dir;
        self
    }

    /// Limit the maximum depth of traversal.
    pub fn max_depth(mut self, depth: u32) -> Self {
        self.max_depth = Some(depth);
        self
    }

    /// Only traverse edges with these labels.
    pub fn label_filter(mut self, labels: Vec<String>) -> Self {
        self.label_filter = Some(labels);
        self
    }
}

impl<G: GraphAccess> Iterator for Dfs<G> {
    type Item = (VertexId, u32);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((vid, depth)) = self.stack.pop() {
            if let Some(max) = self.max_depth {
                if depth > max {
                    continue;
                }
            }
            if !self.visited.insert(vid) {
                continue;
            }
            // Skip vertices that don't exist in the graph
            if self.graph.get_vertex(vid).is_none() {
                continue;
            }

            // Expand neighbors if we haven't reached max depth
            if self.max_depth.map_or(true, |m| depth < m) {
                let neighbors = expand_neighbors(
                    &self.graph,
                    vid,
                    self.direction,
                    self.label_filter.as_deref(),
                );
                for (neighbor, _eid) in neighbors {
                    if !self.visited.contains(&neighbor) {
                        self.stack.push((neighbor, depth + 1));
                    }
                }
            }

            return Some((vid, depth));
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Bidirectional BFS
// ---------------------------------------------------------------------------

/// Bidirectional BFS for finding shortest unweighted path.
///
/// Alternates expansion from source and target frontiers.
/// Returns the meeting point and reconstructed path.
///
/// # Errors
///
/// - [`AlgorithmError::VertexNotFound`] if source or target doesn't exist
/// - [`AlgorithmError::NoPath`] if no path exists
pub fn bidirectional_bfs<G: GraphAccess>(
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

    // Forward: expand Out from source; Backward: expand In from target
    // For Direction::In, reverse these. For Direction::Both, both expand Both.
    let (fwd_dir, bwd_dir) = match direction {
        Direction::Out => (Direction::Out, Direction::In),
        Direction::In => (Direction::In, Direction::Out),
        Direction::Both => (Direction::Both, Direction::Both),
    };

    let mut fwd_queue: VecDeque<VertexId> = VecDeque::new();
    let mut bwd_queue: VecDeque<VertexId> = VecDeque::new();

    // parent maps: vertex -> (parent_vertex, edge_id)
    let mut fwd_parent: HashMap<VertexId, Option<(VertexId, EdgeId)>> = HashMap::new();
    let mut bwd_parent: HashMap<VertexId, Option<(VertexId, EdgeId)>> = HashMap::new();

    fwd_queue.push_back(source);
    fwd_parent.insert(source, None);
    bwd_queue.push_back(target);
    bwd_parent.insert(target, None);

    loop {
        // Expand forward frontier
        if fwd_queue.is_empty() && bwd_queue.is_empty() {
            return Err(AlgorithmError::NoPath {
                from: source,
                to: target,
            });
        }

        if let Some(meeting) =
            expand_frontier(graph, &mut fwd_queue, &mut fwd_parent, &bwd_parent, fwd_dir, label_filter)
        {
            return Ok(reconstruct_bidir_path(
                &fwd_parent,
                &bwd_parent,
                meeting,
            ));
        }

        if let Some(meeting) =
            expand_frontier(graph, &mut bwd_queue, &mut bwd_parent, &fwd_parent, bwd_dir, label_filter)
        {
            return Ok(reconstruct_bidir_path(
                &fwd_parent,
                &bwd_parent,
                meeting,
            ));
        }

        if fwd_queue.is_empty() && bwd_queue.is_empty() {
            return Err(AlgorithmError::NoPath {
                from: source,
                to: target,
            });
        }
    }
}

/// Expand one level of a frontier. Returns meeting vertex if found.
fn expand_frontier<G: GraphAccess>(
    graph: &G,
    queue: &mut VecDeque<VertexId>,
    my_parent: &mut HashMap<VertexId, Option<(VertexId, EdgeId)>>,
    other_parent: &HashMap<VertexId, Option<(VertexId, EdgeId)>>,
    direction: Direction,
    label_filter: Option<&[String]>,
) -> Option<VertexId> {
    let level_size = queue.len();
    for _ in 0..level_size {
        let vid = queue.pop_front()?;
        let neighbors = expand_neighbors(graph, vid, direction, label_filter);
        for (neighbor, eid) in neighbors {
            if let std::collections::hash_map::Entry::Vacant(e) = my_parent.entry(neighbor) {
                e.insert(Some((vid, eid)));
                if other_parent.contains_key(&neighbor) {
                    return Some(neighbor);
                }
                queue.push_back(neighbor);
            }
        }
    }
    None
}

/// Reconstruct path from bidirectional BFS parent maps.
fn reconstruct_bidir_path(
    fwd_parent: &HashMap<VertexId, Option<(VertexId, EdgeId)>>,
    bwd_parent: &HashMap<VertexId, Option<(VertexId, EdgeId)>>,
    meeting: VertexId,
) -> PathResult {
    // Build forward half: source -> ... -> meeting
    let mut fwd_verts = vec![meeting];
    let mut fwd_edges = Vec::new();
    let mut cur = meeting;
    while let Some(Some((parent, eid))) = fwd_parent.get(&cur) {
        fwd_verts.push(*parent);
        fwd_edges.push(*eid);
        cur = *parent;
    }
    fwd_verts.reverse();
    fwd_edges.reverse();

    // Build backward half: meeting -> ... -> target
    let mut bwd_edges = Vec::new();
    cur = meeting;
    while let Some(Some((parent, eid))) = bwd_parent.get(&cur) {
        fwd_verts.push(*parent);
        bwd_edges.push(*eid);
        cur = *parent;
    }

    fwd_edges.extend(bwd_edges);
    let weight = (fwd_edges.len()) as f64;

    PathResult {
        vertices: fwd_verts,
        edges: fwd_edges,
        weight,
    }
}

// ---------------------------------------------------------------------------
// Iterative Deepening DFS (IDDFS)
// ---------------------------------------------------------------------------

/// Iterative deepening: DFS with increasing depth limit.
///
/// Combines DFS space efficiency O(d) with BFS optimality for finding
/// the shortest path in an unweighted graph.
///
/// # Errors
///
/// - [`AlgorithmError::VertexNotFound`] if source or target doesn't exist
/// - [`AlgorithmError::NoPath`] if no path found within `max_depth`
pub fn iddfs<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    max_depth: u32,
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

    for depth_limit in 0..=max_depth {
        if let Some(path) = depth_limited_dfs(graph, source, target, depth_limit, direction)? {
            return Ok(path);
        }
    }
    Err(AlgorithmError::NoPath {
        from: source,
        to: target,
    })
}

/// DFS with a fixed depth limit. Returns `Ok(Some(path))` if target is found.
fn depth_limited_dfs<G: GraphAccess>(
    graph: &G,
    source: VertexId,
    target: VertexId,
    depth_limit: u32,
    direction: Direction,
) -> Result<Option<PathResult>, AlgorithmError> {
    // Stack: (vertex, depth, path_vertices, path_edges)
    let mut stack: Vec<(VertexId, u32, Vec<VertexId>, Vec<EdgeId>)> = Vec::new();
    stack.push((source, 0, vec![source], vec![]));

    let mut visited = HashSet::new();

    while let Some((vid, depth, path_verts, path_edges)) = stack.pop() {
        if vid == target {
            let weight = path_edges.len() as f64;
            return Ok(Some(PathResult {
                vertices: path_verts,
                edges: path_edges,
                weight,
            }));
        }

        if depth >= depth_limit {
            continue;
        }

        if !visited.insert(vid) {
            continue;
        }

        let neighbors = expand_neighbors(graph, vid, direction, None);
        for (neighbor, eid) in neighbors {
            if !visited.contains(&neighbor) {
                let mut new_verts = path_verts.clone();
                new_verts.push(neighbor);
                let mut new_edges = path_edges.clone();
                new_edges.push(eid);
                stack.push((neighbor, depth + 1, new_verts, new_edges));
            }
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_chain(n: usize) -> (Arc<Graph>, Vec<VertexId>) {
        let g = Arc::new(Graph::new());
        let mut ids = Vec::new();
        for i in 0..n {
            ids.push(g.add_vertex("node", HashMap::from([("idx".into(), Value::Int(i as i64))])));
        }
        for i in 0..n - 1 {
            g.add_edge(ids[i], ids[i + 1], "next", HashMap::new())
                .unwrap();
        }
        (g, ids)
    }

    fn make_diamond() -> (Arc<Graph>, Vec<VertexId>) {
        //   0 -> 1 -> 3
        //   0 -> 2 -> 3
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        let c = g.add_vertex("node", HashMap::new());
        let d = g.add_vertex("node", HashMap::new());
        g.add_edge(a, b, "e", HashMap::new()).unwrap();
        g.add_edge(a, c, "e", HashMap::new()).unwrap();
        g.add_edge(b, d, "e", HashMap::new()).unwrap();
        g.add_edge(c, d, "e", HashMap::new()).unwrap();
        (g, vec![a, b, c, d])
    }

    use crate::value::Value;

    // --- BFS tests ---

    #[test]
    fn bfs_chain() {
        let (g, ids) = make_chain(5);
        let result: Vec<_> = Bfs::new(g, ids[0]).collect();
        assert_eq!(result.len(), 5);
        // Depth monotonically non-decreasing
        for w in result.windows(2) {
            assert!(w[0].1 <= w[1].1);
        }
    }

    #[test]
    fn bfs_max_depth() {
        let (g, ids) = make_chain(10);
        let result: Vec<_> = Bfs::new(g, ids[0]).max_depth(3).collect();
        assert!(result.iter().all(|&(_, d)| d <= 3));
        assert_eq!(result.len(), 4); // 0,1,2,3
    }

    #[test]
    fn bfs_empty_graph() {
        let g = Arc::new(Graph::new());
        let result: Vec<_> = Bfs::new(g, VertexId(999)).collect();
        assert!(result.is_empty());
    }

    #[test]
    fn bfs_single_vertex() {
        let g = Arc::new(Graph::new());
        let id = g.add_vertex("node", HashMap::new());
        let result: Vec<_> = Bfs::new(g, id).collect();
        assert_eq!(result, vec![(id, 0)]);
    }

    #[test]
    fn bfs_direction_in() {
        let (g, ids) = make_chain(3);
        // From last vertex, going In direction
        let result: Vec<_> = Bfs::new(g, ids[2]).direction(Direction::In).collect();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn bfs_direction_both() {
        let (g, ids) = make_chain(3);
        let result: Vec<_> = Bfs::new(g, ids[1])
            .direction(Direction::Both)
            .collect();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn bfs_label_filter() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        let c = g.add_vertex("node", HashMap::new());
        g.add_edge(a, b, "knows", HashMap::new()).unwrap();
        g.add_edge(a, c, "likes", HashMap::new()).unwrap();
        let result: Vec<_> = Bfs::new(g, a)
            .label_filter(vec!["knows".to_string()])
            .collect();
        assert_eq!(result.len(), 2); // a and b
    }

    // --- DFS tests ---

    #[test]
    fn dfs_chain() {
        let (g, ids) = make_chain(5);
        let result: Vec<_> = Dfs::new(g, ids[0]).collect();
        assert_eq!(result.len(), 5);
        // All vertices visited
        let visited: HashSet<_> = result.iter().map(|(v, _)| *v).collect();
        for id in &ids {
            assert!(visited.contains(id));
        }
    }

    #[test]
    fn dfs_max_depth() {
        let (g, ids) = make_chain(10);
        let result: Vec<_> = Dfs::new(g, ids[0]).max_depth(2).collect();
        assert!(result.iter().all(|&(_, d)| d <= 2));
    }

    #[test]
    fn dfs_direction_in() {
        let (g, ids) = make_chain(3);
        let result: Vec<_> = Dfs::new(g, ids[2]).direction(Direction::In).collect();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn dfs_label_filter() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        let c = g.add_vertex("node", HashMap::new());
        g.add_edge(a, b, "knows", HashMap::new()).unwrap();
        g.add_edge(a, c, "likes", HashMap::new()).unwrap();
        let result: Vec<_> = Dfs::new(g, a)
            .label_filter(vec!["knows".to_string()])
            .collect();
        assert_eq!(result.len(), 2); // a and b
    }

    // --- Bidirectional BFS tests ---

    #[test]
    fn bidir_bfs_chain() {
        let (g, ids) = make_chain(5);
        let path = bidirectional_bfs(&g, ids[0], ids[4], Direction::Out, None).unwrap();
        assert_eq!(path.vertices.first(), Some(&ids[0]));
        assert_eq!(path.vertices.last(), Some(&ids[4]));
        assert_eq!(path.vertices.len(), 5);
        assert_eq!(path.edges.len(), 4);
    }

    #[test]
    fn bidir_bfs_same_vertex() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let path = bidirectional_bfs(&g, a, a, Direction::Out, None).unwrap();
        assert_eq!(path.vertices, vec![a]);
        assert!(path.edges.is_empty());
    }

    #[test]
    fn bidir_bfs_no_path() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let b = g.add_vertex("node", HashMap::new());
        let result = bidirectional_bfs(&g, a, b, Direction::Out, None);
        assert!(matches!(result, Err(AlgorithmError::NoPath { .. })));
    }

    #[test]
    fn bidir_bfs_vertex_not_found() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let result = bidirectional_bfs(&g, a, VertexId(999), Direction::Out, None);
        assert!(matches!(result, Err(AlgorithmError::VertexNotFound(_))));
        let result = bidirectional_bfs(&g, VertexId(999), a, Direction::Out, None);
        assert!(matches!(result, Err(AlgorithmError::VertexNotFound(_))));
    }

    #[test]
    fn bidir_bfs_diamond() {
        let (g, ids) = make_diamond();
        let path = bidirectional_bfs(&g, ids[0], ids[3], Direction::Out, None).unwrap();
        assert_eq!(path.vertices.first(), Some(&ids[0]));
        assert_eq!(path.vertices.last(), Some(&ids[3]));
        assert_eq!(path.vertices.len(), 3); // shortest is 2 hops
    }

    // --- IDDFS tests ---

    #[test]
    fn iddfs_chain() {
        let (g, ids) = make_chain(5);
        let path = iddfs(&g, ids[0], ids[4], 10, Direction::Out).unwrap();
        assert_eq!(path.vertices.first(), Some(&ids[0]));
        assert_eq!(path.vertices.last(), Some(&ids[4]));
    }

    #[test]
    fn iddfs_same_vertex() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let path = iddfs(&g, a, a, 5, Direction::Out).unwrap();
        assert_eq!(path.vertices, vec![a]);
    }

    #[test]
    fn iddfs_no_path_within_depth() {
        let (g, ids) = make_chain(10);
        let result = iddfs(&g, ids[0], ids[9], 2, Direction::Out);
        assert!(matches!(result, Err(AlgorithmError::NoPath { .. })));
    }

    #[test]
    fn iddfs_vertex_not_found() {
        let g = Arc::new(Graph::new());
        let a = g.add_vertex("node", HashMap::new());
        let result = iddfs(&g, a, VertexId(999), 5, Direction::Out);
        assert!(matches!(result, Err(AlgorithmError::VertexNotFound(_))));
        let result = iddfs(&g, VertexId(999), a, 5, Direction::Out);
        assert!(matches!(result, Err(AlgorithmError::VertexNotFound(_))));
    }
}
