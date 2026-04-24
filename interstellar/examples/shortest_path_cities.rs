//! # Shortest Path Between Cities
//!
//! Demonstrates graph pathfinding combined with geospatial data:
//! - Build a European city network with coordinates and road distances
//! - Find shortest paths using Dijkstra (fluent API) and A* (with haversine heuristic)
//! - Query nearby cities using a geospatial radius search
//!
//! Run: `cargo run --example shortest_path_cities`

use interstellar::algorithms::common::property_weight;
use interstellar::algorithms::{astar, dijkstra, Direction};
use interstellar::geo::{haversine, Distance, Point};
use interstellar::index::IndexBuilder;
use interstellar::prelude::*;
use interstellar::GraphAccess;
use std::collections::HashMap;
use std::sync::Arc;

/// City definition: name, longitude, latitude.
struct City {
    name: &'static str,
    lon: f64,
    lat: f64,
}

/// Road definition: source city, destination city, distance in km.
struct Road {
    from: &'static str,
    to: &'static str,
    km: f64,
}

/// Build a European city network.
///
/// ```text
///                Amsterdam
///                 |     \
///              210|     520
///                 |       \
///   London --340-- Paris --450-- Zurich --300-- Milan
///                  |                              |
///                 450                            480
///                  |                              |
///                 Lyon                          Rome
///                  |
///                 310
///                  |
///               Marseille
/// ```
fn build_europe() -> (Arc<Graph>, HashMap<&'static str, VertexId>) {
    let cities = vec![
        City { name: "London",    lon:  -0.1278, lat: 51.5074 },
        City { name: "Paris",     lon:   2.3522, lat: 48.8566 },
        City { name: "Brussels",  lon:   4.3517, lat: 50.8503 },
        City { name: "Amsterdam", lon:   4.9041, lat: 52.3676 },
        City { name: "Lyon",      lon:   4.8357, lat: 45.7640 },
        City { name: "Marseille", lon:   5.3698, lat: 43.2965 },
        City { name: "Zurich",    lon:   8.5417, lat: 47.3769 },
        City { name: "Milan",     lon:   9.1900, lat: 45.4642 },
        City { name: "Rome",      lon:  12.4964, lat: 41.9028 },
    ];

    // Bidirectional roads (we add edges in both directions)
    let roads = vec![
        Road { from: "London",    to: "Paris",     km: 340.0 },
        Road { from: "Paris",     to: "Brussels",  km: 265.0 },
        Road { from: "Brussels",  to: "Amsterdam", km: 210.0 },
        Road { from: "Paris",     to: "Amsterdam", km: 520.0 },
        Road { from: "Paris",     to: "Lyon",      km: 450.0 },
        Road { from: "Paris",     to: "Zurich",    km: 450.0 },
        Road { from: "Lyon",      to: "Marseille", km: 310.0 },
        Road { from: "Zurich",    to: "Milan",     km: 300.0 },
        Road { from: "Milan",     to: "Rome",      km: 480.0 },
    ];

    let graph = Arc::new(Graph::new());

    // Create R-tree spatial index
    let spec = IndexBuilder::vertex()
        .label("city")
        .property("location")
        .rtree()
        .build()
        .expect("valid index spec");
    graph.create_index(spec).unwrap();

    // Add cities
    let mut ids: HashMap<&'static str, VertexId> = HashMap::new();
    for city in &cities {
        let id = graph.add_vertex(
            "city",
            HashMap::from([
                ("name".into(), Value::String(city.name.into())),
                (
                    "location".into(),
                    Value::Point(Point::new(city.lon, city.lat).unwrap()),
                ),
            ]),
        );
        ids.insert(city.name, id);
    }

    // Add bidirectional road edges
    for road in &roads {
        let props =
            HashMap::from([("distance".into(), Value::Float(road.km))]);
        graph
            .add_edge(ids[road.from], ids[road.to], "road", props.clone())
            .unwrap();
        graph
            .add_edge(ids[road.to], ids[road.from], "road", props)
            .unwrap();
    }

    (graph, ids)
}

/// Look up a city name from its VertexId.
fn city_name(graph: &Arc<Graph>, id: VertexId) -> String {
    graph
        .get_vertex(id)
        .and_then(|v| match v.properties.get("name") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .unwrap_or_else(|| format!("{id:?}"))
}

/// Format a path as "City1 -> City2 -> City3".
fn format_path(graph: &Arc<Graph>, vertices: &[VertexId]) -> String {
    vertices
        .iter()
        .map(|id| city_name(graph, *id))
        .collect::<Vec<_>>()
        .join(" -> ")
}

fn main() {
    let (graph, ids) = build_europe();

    println!("=== Shortest Path Between European Cities ===\n");
    println!(
        "Cities: {}\n",
        [
            "London", "Paris", "Brussels", "Amsterdam", "Lyon",
            "Marseille", "Zurich", "Milan", "Rome"
        ]
        .join(", ")
    );

    let london = ids["London"];
    let rome = ids["Rome"];
    let amsterdam = ids["Amsterdam"];

    // -------------------------------------------------------------------------
    // 1. Dijkstra: London -> Rome
    // -------------------------------------------------------------------------
    println!("--- Dijkstra: London -> Rome ---\n");

    let weight_fn = property_weight("distance".into());
    let result = dijkstra(&graph, london, rome, &weight_fn, Direction::Out).unwrap();

    println!("  Path:     {}", format_path(&graph, &result.vertices));
    println!("  Distance: {} km", result.weight);

    // -------------------------------------------------------------------------
    // 2. Dijkstra via fluent API: London -> Rome
    // -------------------------------------------------------------------------
    println!("\n--- Dijkstra (fluent API): London -> Rome ---\n");

    let snap = graph.snapshot();
    let g = snap.gremlin();
    let results = g.v_ids([london]).dijkstra_to(rome, "distance").to_list();

    if let Some(Value::Map(map)) = results.first() {
        if let (Some(Value::List(path)), Some(weight)) = (map.get("path"), map.get("weight")) {
            let names: Vec<String> = path
                .iter()
                .filter_map(|v| match v {
                    Value::Vertex(id) => Some(city_name(&graph, *id)),
                    _ => None,
                })
                .collect();
            let km = match weight {
                Value::Float(f) => *f,
                Value::Int(i) => *i as f64,
                _ => 0.0,
            };
            println!("  Path:     {}", names.join(" -> "));
            println!("  Distance: {} km", km);
        }
    }

    // -------------------------------------------------------------------------
    // 3. A* with haversine heuristic: Amsterdam -> Rome
    // -------------------------------------------------------------------------
    println!("\n--- A* (haversine heuristic): Amsterdam -> Rome ---\n");

    // Build a lookup from VertexId -> Point for the heuristic
    let coords: HashMap<VertexId, Point> = ids
        .values()
        .filter_map(|&vid| {
            let v = graph.get_vertex(vid)?;
            match v.properties.get("location")? {
                Value::Point(p) => Some((vid, p.clone())),
                _ => None,
            }
        })
        .collect();

    let rome_point = coords[&rome];
    let heuristic = |vid: VertexId| -> f64 {
        coords
            .get(&vid)
            .map(|p| haversine(*p, rome_point) / 1000.0) // meters -> km
            .unwrap_or(0.0)
    };

    let weight_fn2 = property_weight("distance".into());
    let result =
        astar(&graph, amsterdam, rome, &weight_fn2, heuristic, Direction::Out).unwrap();

    println!("  Path:     {}", format_path(&graph, &result.vertices));
    println!("  Distance: {} km", result.weight);

    // -------------------------------------------------------------------------
    // 4. Geospatial query: cities within 400 km of Paris
    // -------------------------------------------------------------------------
    println!("\n--- Cities within 400 km of Paris (geospatial query) ---\n");

    use interstellar::traversal::p;

    let paris_point = Point::new(2.3522, 48.8566).unwrap();
    let snap3 = graph.snapshot();
    let g3 = snap3.gremlin();

    let nearby = g3
        .v()
        .has_label("city")
        .has_where("location", p::within_distance(paris_point, Distance::Kilometers(400.0)))
        .values("name")
        .to_list();

    let names: Vec<String> = nearby
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    println!("  {}", names.join(", "));

    println!("\n=== Done ===");
}
