//! GQL CALL procedure integration tests.
//!
//! Tests for graph algorithm procedures:
//! - `interstellar.shortestPath(source, target)`
//! - `interstellar.dijkstra(source, target, weightProperty)`
//! - `interstellar.bfs(source)`
//! - `interstellar.dfs(source)` / `interstellar.dfs(source, maxDepth)`
//! - `interstellar.astar(source, target, weightProperty, heuristicProperty)`
//! - `interstellar.bidirectionalBfs(source, target)`
//! - `interstellar.iddfs(source, target, maxDepth)`

use interstellar::gql::{compile, parse};
use interstellar::prelude::*;
use interstellar::storage::Graph;
use std::collections::HashMap;
use std::sync::Arc;

/// Create a simple chain: A -> B -> C -> D with weighted edges
fn create_chain_graph() -> (Arc<Graph>, Vec<VertexId>) {
    let graph = Arc::new(Graph::new());
    let mut ids = Vec::new();
    for i in 0..4 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("n{i}")));
        ids.push(graph.add_vertex("Node", props));
    }
    for i in 0..3 {
        let mut props = HashMap::new();
        props.insert("weight".to_string(), Value::from((i + 1) as i64));
        graph.add_edge(ids[i], ids[i + 1], "LINK", props).unwrap();
    }
    (graph, ids)
}

// =============================================================================
// shortestPath
// =============================================================================

#[test]
fn test_shortest_path_procedure() {
    let (graph, ids) = create_chain_graph();
    let snapshot = graph.snapshot();

    // Use YIELD aliases to avoid keyword conflicts (path is a keyword)
    let query_str = format!(
        "MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CALL interstellar.shortestPath(a, b) YIELD path AS p, distance AS d RETURN p, d",
        ids[0].0, ids[3].0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    match &results[0] {
        Value::Map(map) => {
            let p = map.get("p").expect("missing p");
            match p {
                Value::List(vertices) => assert_eq!(vertices.len(), 4), // A->B->C->D
                other => panic!("expected list for path, got {other:?}"),
            }
            let d = map.get("d").expect("missing d");
            assert_eq!(*d, Value::Int(3)); // 3 hops
        }
        other => panic!("expected map result, got {other:?}"),
    }
}

#[test]
fn test_shortest_path_no_path() {
    let graph = Arc::new(Graph::new());
    let a = graph.add_vertex("Node", HashMap::new());
    let b = graph.add_vertex("Node", HashMap::new());
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (x), (y) WHERE id(x) = {} AND id(y) = {} CALL interstellar.shortestPath(x, y) YIELD path AS p, distance AS d RETURN p, d",
        a.0, b.0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert!(results.is_empty());
}

#[test]
fn test_shortest_path_same_vertex() {
    let (graph, ids) = create_chain_graph();
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a) WHERE id(a) = {} CALL interstellar.shortestPath(a, a) YIELD path AS p, distance AS d RETURN p, d",
        ids[0].0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    match &results[0] {
        Value::Map(map) => {
            let p = map.get("p").expect("missing p");
            match p {
                Value::List(v) => assert_eq!(v.len(), 1),
                other => panic!("expected list, got {other:?}"),
            }
            assert_eq!(*map.get("d").unwrap(), Value::Int(0));
        }
        other => panic!("expected map, got {other:?}"),
    }
}

// =============================================================================
// dijkstra
// =============================================================================

#[test]
fn test_dijkstra_procedure() {
    let (graph, ids) = create_chain_graph();
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CALL interstellar.dijkstra(a, b, 'weight') YIELD path AS p, distance AS d RETURN p, d",
        ids[0].0, ids[3].0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    match &results[0] {
        Value::Map(map) => {
            let p = map.get("p").expect("missing p");
            match p {
                Value::List(v) => assert_eq!(v.len(), 4),
                other => panic!("expected list, got {other:?}"),
            }
            let d = map.get("d").expect("missing d");
            assert_eq!(*d, Value::Float(6.0));
        }
        other => panic!("expected map, got {other:?}"),
    }
}

// =============================================================================
// bfs
// =============================================================================

#[test]
fn test_bfs_procedure() {
    let (graph, ids) = create_chain_graph();
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a) WHERE id(a) = {} CALL interstellar.bfs(a) YIELD node AS v, depth AS d RETURN v, d",
        ids[0].0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    // BFS from ids[0] should visit all 4 nodes
    assert_eq!(results.len(), 4);
}

// =============================================================================
// Error cases
// =============================================================================

// =============================================================================
// dfs
// =============================================================================

#[test]
fn test_dfs_procedure() {
    let (graph, ids) = create_chain_graph();
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a) WHERE id(a) = {} CALL interstellar.dfs(a) YIELD node AS v, depth AS d RETURN v, d",
        ids[0].0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    // DFS from ids[0] should visit all 4 nodes
    assert_eq!(results.len(), 4);
}

#[test]
fn test_dfs_procedure_with_max_depth() {
    let (graph, ids) = create_chain_graph();
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a) WHERE id(a) = {} CALL interstellar.dfs(a, 1) YIELD node AS v, depth AS d RETURN v, d",
        ids[0].0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    // DFS from ids[0] with maxDepth=1 should visit at most 2 nodes (source + 1 hop)
    assert_eq!(results.len(), 2);
}

// =============================================================================
// astar
// =============================================================================

#[test]
fn test_astar_procedure() {
    let (graph, ids) = create_chain_graph();

    // Add a heuristic property to each vertex (estimated distance to target ids[3])
    // For a chain A->B->C->D, heuristic values: A=3, B=2, C=1, D=0
    for (i, id) in ids.iter().enumerate() {
        graph.set_vertex_property(*id, "h", Value::from((3 - i) as i64)).unwrap();
    }

    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CALL interstellar.astar(a, b, 'weight', 'h') YIELD path AS p, distance AS d RETURN p, d",
        ids[0].0, ids[3].0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    match &results[0] {
        Value::Map(map) => {
            let p = map.get("p").expect("missing p");
            match p {
                Value::List(v) => assert_eq!(v.len(), 4), // A->B->C->D
                other => panic!("expected list for path, got {other:?}"),
            }
            let d = map.get("d").expect("missing d");
            assert_eq!(*d, Value::Float(6.0)); // weights: 1+2+3 = 6
        }
        other => panic!("expected map, got {other:?}"),
    }
}

#[test]
fn test_astar_procedure_no_path() {
    let graph = Arc::new(Graph::new());
    let a = graph.add_vertex("Node", HashMap::new());
    let b = graph.add_vertex("Node", HashMap::new());
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CALL interstellar.astar(a, b, 'weight', 'h') YIELD path AS p, distance AS d RETURN p, d",
        a.0, b.0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert!(results.is_empty());
}

// =============================================================================
// bidirectionalBfs
// =============================================================================

#[test]
fn test_bidirectional_bfs_procedure() {
    let (graph, ids) = create_chain_graph();
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CALL interstellar.bidirectionalBfs(a, b) YIELD path AS p, distance AS d RETURN p, d",
        ids[0].0, ids[3].0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    match &results[0] {
        Value::Map(map) => {
            let p = map.get("p").expect("missing p");
            match p {
                Value::List(v) => assert_eq!(v.len(), 4), // A->B->C->D
                other => panic!("expected list for path, got {other:?}"),
            }
            let d = map.get("d").expect("missing d");
            assert_eq!(*d, Value::Int(3)); // 3 hops
        }
        other => panic!("expected map, got {other:?}"),
    }
}

#[test]
fn test_bidirectional_bfs_procedure_no_path() {
    let graph = Arc::new(Graph::new());
    let a = graph.add_vertex("Node", HashMap::new());
    let b = graph.add_vertex("Node", HashMap::new());
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CALL interstellar.bidirectionalBfs(a, b) YIELD path AS p, distance AS d RETURN p, d",
        a.0, b.0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert!(results.is_empty());
}

// =============================================================================
// iddfs
// =============================================================================

#[test]
fn test_iddfs_procedure() {
    let (graph, ids) = create_chain_graph();
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CALL interstellar.iddfs(a, b, 10) YIELD path AS p, distance AS d RETURN p, d",
        ids[0].0, ids[3].0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    match &results[0] {
        Value::Map(map) => {
            let p = map.get("p").expect("missing p");
            match p {
                Value::List(v) => assert_eq!(v.len(), 4), // A->B->C->D
                other => panic!("expected list for path, got {other:?}"),
            }
            let d = map.get("d").expect("missing d");
            assert_eq!(*d, Value::Int(3)); // 3 hops
        }
        other => panic!("expected map, got {other:?}"),
    }
}

#[test]
fn test_iddfs_procedure_no_path() {
    let graph = Arc::new(Graph::new());
    let a = graph.add_vertex("Node", HashMap::new());
    let b = graph.add_vertex("Node", HashMap::new());
    let snapshot = graph.snapshot();

    let query_str = format!(
        "MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CALL interstellar.iddfs(a, b, 10) YIELD path AS p, distance AS d RETURN p, d",
        a.0, b.0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    assert!(results.is_empty());
}

#[test]
fn test_iddfs_procedure_depth_too_shallow() {
    let (graph, ids) = create_chain_graph();
    let snapshot = graph.snapshot();

    // Path requires 3 hops but maxDepth=1
    let query_str = format!(
        "MATCH (a), (b) WHERE id(a) = {} AND id(b) = {} CALL interstellar.iddfs(a, b, 1) YIELD path AS p, distance AS d RETURN p, d",
        ids[0].0, ids[3].0
    );
    let query = parse(&query_str).unwrap();
    let results = compile(&query, &snapshot).unwrap();

    // Should not find path with depth limit of 1 (need 3 hops)
    assert!(results.is_empty());
}

// =============================================================================
// Error cases
// =============================================================================

#[test]
fn test_unknown_procedure() {
    let graph = Arc::new(Graph::new());
    graph.add_vertex("Node", HashMap::new());
    let snapshot = graph.snapshot();

    let query_str = "MATCH (a) CALL interstellar.unknown(a) YIELD x RETURN x";
    let query = parse(query_str).unwrap();
    let result = compile(&query, &snapshot);

    assert!(result.is_err());
}
