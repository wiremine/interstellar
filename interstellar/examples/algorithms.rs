//! # Graph Algorithms Example
//!
//! Demonstrates all traversal and pathfinding algorithms in Interstellar:
//! - BFS and DFS iterators
//! - Bidirectional BFS
//! - Iterative Deepening DFS (IDDFS)
//! - Unweighted shortest path
//! - Dijkstra's weighted shortest path
//! - A* with a heuristic
//! - Yen's k-shortest paths
//!
//! Run: `cargo run --example algorithms`

use interstellar::algorithms::{
    astar, bidirectional_bfs, dijkstra, dijkstra_all, iddfs, k_shortest_paths,
    shortest_path_unweighted, Bfs, Dfs, Direction,
};
use interstellar::algorithms::common::{property_weight, unit_weight};
use interstellar::GraphAccess;
use interstellar::storage::Graph;
use interstellar::value::{Value, VertexId};
use std::collections::HashMap;
use std::sync::Arc;

/// Build a small city network with weighted "road" edges.
///
/// ```text
///              2
///   Seattle -------> Portland
///     |  \              |
///   4 |   \ 8           | 3
///     v    v            v
///   Boise   SF ------> LA
///     |          5      ^
///   6 |                 | 1
///     v                 |
///   SLC -------------> LV
///            3
/// ```
fn build_city_graph() -> (Arc<Graph>, HashMap<&'static str, VertexId>) {
    let graph = Arc::new(Graph::new());
    let mut cities = HashMap::new();

    for name in ["Seattle", "Portland", "Boise", "SF", "LA", "SLC", "LV"] {
        let id = graph.add_vertex(
            "city",
            HashMap::from([("name".to_string(), Value::from(name))]),
        );
        cities.insert(name, id);
    }

    let edges = [
        ("Seattle", "Portland", 2.0),
        ("Seattle", "Boise", 4.0),
        ("Seattle", "SF", 8.0),
        ("Portland", "LA", 3.0),
        ("SF", "LA", 5.0),
        ("Boise", "SLC", 6.0),
        ("SLC", "LV", 3.0),
        ("LV", "LA", 1.0),
    ];

    for (src, dst, weight) in edges {
        graph
            .add_edge(
                cities[src],
                cities[dst],
                "road",
                HashMap::from([("distance".to_string(), Value::Float(weight))]),
            )
            .unwrap();
    }

    (graph, cities)
}

fn main() {
    let (graph, cities) = build_city_graph();
    let seattle = cities["Seattle"];
    let la = cities["LA"];

    println!("=== Interstellar Graph Algorithms ===\n");

    // -------------------------------------------------------------------------
    // 1. BFS — Breadth-First Search
    // -------------------------------------------------------------------------
    println!("-- BFS from Seattle (outgoing, max depth 2) --\n");

    let bfs_results: Vec<_> = Bfs::new(Arc::clone(&graph), seattle)
        .direction(Direction::Out)
        .max_depth(2)
        .collect();

    for (vid, depth) in &bfs_results {
        let name = graph.get_vertex(*vid).map(|v| {
            v.properties
                .get("name")
                .cloned()
                .unwrap_or(Value::from("?"))
        });
        println!("  depth {}: {:?}", depth, name.unwrap_or(Value::from("?")));
    }
    println!("  Total vertices visited: {}\n", bfs_results.len());

    // -------------------------------------------------------------------------
    // 2. DFS — Depth-First Search
    // -------------------------------------------------------------------------
    println!("-- DFS from Seattle (outgoing) --\n");

    let dfs_results: Vec<_> = Dfs::new(Arc::clone(&graph), seattle)
        .direction(Direction::Out)
        .collect();

    for (vid, depth) in &dfs_results {
        let name = graph.get_vertex(*vid).map(|v| {
            v.properties
                .get("name")
                .cloned()
                .unwrap_or(Value::from("?"))
        });
        println!("  depth {}: {:?}", depth, name.unwrap_or(Value::from("?")));
    }
    println!("  Total vertices visited: {}\n", dfs_results.len());

    // -------------------------------------------------------------------------
    // 3. BFS with label filter
    // -------------------------------------------------------------------------
    println!("-- BFS with label filter (only 'road' edges) --\n");

    let filtered: Vec<_> = Bfs::new(Arc::clone(&graph), seattle)
        .label_filter(vec!["road".to_string()])
        .collect();

    println!("  Vertices reachable via 'road' edges: {}\n", filtered.len());

    // -------------------------------------------------------------------------
    // 4. Unweighted shortest path
    // -------------------------------------------------------------------------
    println!("-- Unweighted Shortest Path: Seattle -> LA --\n");

    match shortest_path_unweighted(&graph, seattle, la, Direction::Out, None) {
        Ok(path) => {
            print!("  Path: ");
            print_path(&graph, &path.vertices);
            println!("  Hops: {}\n", path.edges.len());
        }
        Err(e) => println!("  Error: {}\n", e),
    }

    // -------------------------------------------------------------------------
    // 5. Dijkstra — Weighted shortest path
    // -------------------------------------------------------------------------
    println!("-- Dijkstra: Seattle -> LA (weighted by distance) --\n");

    let weight_fn = property_weight("distance".to_string());
    match dijkstra(&graph, seattle, la, &weight_fn, Direction::Out) {
        Ok(path) => {
            print!("  Path: ");
            print_path(&graph, &path.vertices);
            println!("  Total distance: {}\n", path.weight);
        }
        Err(e) => println!("  Error: {}\n", e),
    }

    // -------------------------------------------------------------------------
    // 6. Dijkstra All — Distances to all reachable cities
    // -------------------------------------------------------------------------
    println!("-- Dijkstra All: distances from Seattle --\n");

    let weight_fn = property_weight("distance".to_string());
    match dijkstra_all(&graph, seattle, &weight_fn, Direction::Out) {
        Ok(results) => {
            let mut sorted: Vec<_> = results.iter().collect();
            sorted.sort_by(|a, b| a.1 .0.partial_cmp(&b.1 .0).unwrap());
            for (vid, (dist, _path)) in sorted {
                let name = vertex_name(&graph, *vid);
                println!("  {} -> {}: distance {}", vertex_name(&graph, seattle), name, dist);
            }
            println!();
        }
        Err(e) => println!("  Error: {}\n", e),
    }

    // -------------------------------------------------------------------------
    // 7. A* — Heuristic-guided search
    // -------------------------------------------------------------------------
    println!("-- A*: Seattle -> LA (with heuristic) --\n");

    // Simple heuristic: estimate remaining distance based on vertex ID.
    // In a real application, you'd use geographic coordinates.
    // Here we use a trivial admissible heuristic of 0 (degenerates to Dijkstra).
    let weight_fn = property_weight("distance".to_string());
    match astar(
        &graph,
        seattle,
        la,
        &weight_fn,
        |_vid| 0.0, // admissible heuristic (trivial)
        Direction::Out,
    ) {
        Ok(path) => {
            print!("  Path: ");
            print_path(&graph, &path.vertices);
            println!("  Total distance: {}\n", path.weight);
        }
        Err(e) => println!("  Error: {}\n", e),
    }

    // -------------------------------------------------------------------------
    // 8. Yen's K-Shortest Paths
    // -------------------------------------------------------------------------
    println!("-- K-Shortest Paths: Seattle -> LA (k=3) --\n");

    let weight_fn = property_weight("distance".to_string());
    match k_shortest_paths(&graph, seattle, la, 3, &weight_fn, Direction::Out) {
        Ok(paths) => {
            for (i, path) in paths.iter().enumerate() {
                print!("  Path {}: ", i + 1);
                print_path(&graph, &path.vertices);
                println!("    Distance: {}", path.weight);
            }
            println!();
        }
        Err(e) => println!("  Error: {}\n", e),
    }

    // -------------------------------------------------------------------------
    // 9. Bidirectional BFS
    // -------------------------------------------------------------------------
    println!("-- Bidirectional BFS: Seattle -> LA --\n");

    match bidirectional_bfs(&graph, seattle, la, Direction::Out, None) {
        Ok(path) => {
            print!("  Path: ");
            print_path(&graph, &path.vertices);
            println!("  Hops: {}\n", path.edges.len());
        }
        Err(e) => println!("  Error: {}\n", e),
    }

    // -------------------------------------------------------------------------
    // 10. IDDFS — Iterative Deepening DFS
    // -------------------------------------------------------------------------
    println!("-- IDDFS: Seattle -> LA (max depth 5) --\n");

    match iddfs(&graph, seattle, la, 5, Direction::Out) {
        Ok(path) => {
            print!("  Path: ");
            print_path(&graph, &path.vertices);
            println!("  Hops: {}\n", path.edges.len());
        }
        Err(e) => println!("  Error: {}\n", e),
    }

    // -------------------------------------------------------------------------
    // 11. Error handling
    // -------------------------------------------------------------------------
    println!("-- Error Handling --\n");

    let bad_id = VertexId(9999);
    match dijkstra(&graph, bad_id, la, &unit_weight(), Direction::Out) {
        Ok(_) => println!("  Unexpected success"),
        Err(e) => println!("  Expected error: {}", e),
    }

    // No path (reverse direction with no incoming edges to Seattle)
    match shortest_path_unweighted(&graph, la, seattle, Direction::Out, None) {
        Ok(path) => println!("  Found reverse path with {} hops", path.edges.len()),
        Err(e) => println!("  Expected error: {}", e),
    }

    println!("\n=== Done ===");
}

/// Print a path as city names joined by arrows.
fn print_path(graph: &Arc<Graph>, vertices: &[VertexId]) {
    let names: Vec<String> = vertices.iter().map(|v| vertex_name(graph, *v)).collect();
    println!("{}", names.join(" -> "));
}

/// Get the name property of a vertex.
fn vertex_name(graph: &Arc<Graph>, vid: VertexId) -> String {
    graph
        .get_vertex(vid)
        .and_then(|v| v.properties.get("name").cloned())
        .map(|v| match v {
            Value::String(s) => s,
            other => format!("{:?}", other),
        })
        .unwrap_or_else(|| format!("{:?}", vid))
}
