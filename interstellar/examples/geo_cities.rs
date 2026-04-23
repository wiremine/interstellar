//! # Interstellar Geospatial Quickstart
//!
//! Demonstrates geospatial features introduced in spec-56:
//! - Storing `Point` geometry values on vertices
//! - Creating an R-tree spatial index
//! - Running geospatial queries via Gremlin and GQL
//!   - `withinDistance` — find cities near a reference point
//!   - `bbox` — bounding-box window query
//!   - `containedBy` — polygon containment test
//!
//! Run: `cargo run --example geo_cities`

use std::collections::HashMap;
use std::sync::Arc;

use interstellar::geo::Point;
use interstellar::gremlin::ExecutionResult;
use interstellar::index::IndexBuilder;
use interstellar::storage::Graph;
use interstellar::value::Value;

/// Helper to build a vertex property map with a name and location.
fn city_props(name: &str, lon: f64, lat: f64) -> HashMap<String, Value> {
    let mut m = HashMap::new();
    m.insert("name".into(), Value::String(name.into()));
    m.insert(
        "location".into(),
        Value::Point(Point::new(lon, lat).expect("valid coord")),
    );
    m
}

fn main() {
    println!("=== Interstellar Geospatial Quickstart ===\n");

    // -------------------------------------------------------------------------
    // 1. Create graph and R-tree index on city.location
    // -------------------------------------------------------------------------
    let graph = Arc::new(Graph::new());

    let spec = IndexBuilder::vertex()
        .label("city")
        .property("location")
        .rtree()
        .build()
        .expect("valid index spec");
    graph.create_index(spec).expect("index creation failed");
    println!("Created R-tree index on city.location\n");

    // -------------------------------------------------------------------------
    // 2. Load cities (lon, lat)
    // -------------------------------------------------------------------------
    let cities = [
        ("New York", -74.006, 40.7128),
        ("Los Angeles", -118.2437, 33.9425),
        ("Chicago", -87.6298, 41.8781),
        ("Houston", -95.3698, 29.7604),
        ("Phoenix", -112.074, 33.4484),
        ("Philadelphia", -75.1652, 39.9526),
        ("San Antonio", -98.4936, 29.4241),
        ("San Diego", -117.1611, 32.7157),
        ("Dallas", -96.797, 32.7767),
        ("London", -0.1278, 51.5074),
        ("Paris", 2.3522, 48.8566),
        ("Tokyo", 139.6917, 35.6895),
        ("Sydney", 151.2093, -33.8688),
        ("São Paulo", -46.6333, -23.5505),
        ("Mumbai", 72.8777, 19.076),
    ];

    for (name, lon, lat) in &cities {
        graph.add_vertex("city", city_props(name, *lon, *lat));
    }
    println!("Loaded {} cities\n", cities.len());

    // -------------------------------------------------------------------------
    // 3. Gremlin: bbox query — cities in the western US
    // -------------------------------------------------------------------------
    println!("-- US West cities via Gremlin bbox --\n");
    let result = graph
        .query(
            "g.V().hasLabel('city')\
             .has('location', geo_bbox(-125.0, 30.0, -100.0, 50.0))\
             .values('name').toList()",
        )
        .expect("gremlin query failed");
    if let ExecutionResult::List(names) = result {
        for name in &names {
            if let Value::String(s) = name {
                println!("  - {s}");
            }
        }
    }

    // -------------------------------------------------------------------------
    // 4. Gremlin: withinDistance — cities within 1000 km of London
    // -------------------------------------------------------------------------
    println!("\n-- Cities within 1000 km of London via Gremlin --\n");
    let result = graph
        .query(
            "g.V().hasLabel('city')\
             .has('location', geo_within_distance(point(-0.1278, 51.5074), 1000km))\
             .values('name').toList()",
        )
        .expect("gremlin query failed");
    if let ExecutionResult::List(names) = result {
        for name in &names {
            if let Value::String(s) = name {
                println!("  - {s}");
            }
        }
    }

    // -------------------------------------------------------------------------
    // 5. Gremlin: containedBy — cities in a rough Europe polygon
    // -------------------------------------------------------------------------
    println!("\n-- European cities via Gremlin containedBy --\n");
    let result = graph
        .query(
            "g.V().hasLabel('city')\
             .has('location', geo_contained_by(polygon([[-10.0, 35.0], [40.0, 35.0], [40.0, 72.0], [-10.0, 72.0], [-10.0, 35.0]])))\
             .values('name').toList()",
        )
        .expect("gremlin query failed");
    if let ExecutionResult::List(names) = result {
        for name in &names {
            if let Value::String(s) = name {
                println!("  - {s}");
            }
        }
    }

    println!("\n=== Done ===");
}
