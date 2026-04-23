//! # Combining Full-Text Search with Geospatial Queries
//!
//! A realistic example that mixes both index types: full-text search on
//! restaurant descriptions and R-tree spatial queries on their locations.
//!
//! Demonstrates:
//! - Creating both a text index and an R-tree index on the same vertex label
//! - Full-text search to find restaurants by cuisine keywords
//! - Geo radius search to find restaurants near a location
//! - Chaining FTS results with geo predicates (text match + nearby)
//! - Chaining geo results with label/property filters
//!
//! Run: `cargo run --example geo_text_search --features full-text`

use std::collections::HashMap;
use std::sync::Arc;

use interstellar::geo::Point;
use interstellar::gremlin::ExecutionResult;
use interstellar::index::IndexBuilder;
use interstellar::storage::text::TextIndexConfig;
use interstellar::storage::Graph;
use interstellar::value::Value;

/// Build a restaurant vertex property map.
fn restaurant(name: &str, cuisine: &str, description: &str, lon: f64, lat: f64) -> HashMap<String, Value> {
    let mut m = HashMap::new();
    m.insert("name".into(), Value::String(name.into()));
    m.insert("cuisine".into(), Value::String(cuisine.into()));
    m.insert("description".into(), Value::String(description.into()));
    m.insert(
        "location".into(),
        Value::Point(Point::new(lon, lat).expect("valid coord")),
    );
    m
}

fn main() {
    println!("=== Geospatial + Full-Text Search ===\n");

    // -------------------------------------------------------------------------
    // 1. Create graph, text index on `description`, R-tree on `location`
    // -------------------------------------------------------------------------
    let graph = Arc::new(Graph::new());

    // Full-text index for keyword search over descriptions.
    graph
        .create_text_index_v("description", TextIndexConfig::default())
        .expect("text index creation failed");

    // R-tree spatial index for proximity queries.
    let spec = IndexBuilder::vertex()
        .label("restaurant")
        .property("location")
        .rtree()
        .build()
        .expect("valid index spec");
    graph.create_index(spec).expect("R-tree creation failed");

    println!("Created text index on `description` and R-tree on `location`\n");

    // -------------------------------------------------------------------------
    // 2. Load restaurants across Manhattan, Brooklyn, and nearby
    //
    //    Coordinates are approximate centroids for illustration.
    // -------------------------------------------------------------------------
    let data = [
        // Manhattan
        ("Sushi Nakazawa",   "japanese",  "omakase sushi with fresh seasonal fish and traditional japanese preparations", -74.0007, 40.7339),
        ("Le Bernardin",     "french",    "refined french seafood with elegant tasting menus and impeccable wine pairings", -73.9817, 40.7616),
        ("Xi'an Famous",     "chinese",   "hand-pulled noodles and spicy cumin lamb burgers from western china", -73.9938, 40.7420),
        ("Tacos El Idolo",   "mexican",   "authentic street tacos with slow-roasted pork and fresh handmade tortillas", -73.9876, 40.7261),
        ("Joe's Pizza",      "italian",   "classic new york thin-crust pizza by the slice since 1975", -74.0022, 40.7328),
        ("Russ & Daughters", "deli",      "smoked fish and bagels from a century-old lower east side institution", -73.9880, 40.7222),

        // Brooklyn
        ("Oxomoco",          "mexican",   "wood-fired mexican cuisine with creative mole sauces and mezcal cocktails", -73.9573, 40.7194),
        ("Win Son",          "taiwanese", "taiwanese american brunch with fried chicken and scallion pancakes", -73.9559, 40.7106),
        ("Lucali",           "italian",   "legendary brick-oven pizza with thin crust and fresh mozzarella in carroll gardens", -73.9977, 40.6796),
        ("Aska",             "nordic",    "scandinavian tasting menu with foraged ingredients and fermented flavors", -73.9658, 40.7119),

        // Queens
        ("Sripraphai",       "thai",      "beloved woodside thai restaurant known for crispy watercress salad and green papaya", -73.9050, 40.7455),

        // Jersey City (across the river)
        ("Razza",            "italian",   "artisan pizza with naturally leavened dough and wood-fired blistered crust", -74.0431, 40.7178),
    ];

    for (name, cuisine, desc, lon, lat) in &data {
        graph.add_vertex("restaurant", restaurant(name, cuisine, desc, *lon, *lat));
    }
    println!("Loaded {} restaurants\n", data.len());

    // -------------------------------------------------------------------------
    // 3. Full-text search: find restaurants mentioning "pizza"
    // -------------------------------------------------------------------------
    println!("-- Text search: \"pizza\" --\n");
    let g = graph.gremlin(Arc::clone(&graph));
    let hits = g
        .search_text("description", "pizza", 10)
        .expect("search failed")
        .values("name")
        .to_value_list();
    for name in &hits {
        if let Value::String(s) = name {
            println!("  - {s}");
        }
    }

    // -------------------------------------------------------------------------
    // 4. Geo search: restaurants within 3 km of Times Square
    // -------------------------------------------------------------------------
    println!("\n-- Restaurants within 3 km of Times Square --\n");
    let result = graph
        .query(
            "g.V().hasLabel('restaurant')\
             .has('location', geo_within_distance(point(-73.9855, 40.7580), 3km))\
             .values('name').toList()",
        )
        .expect("gremlin failed");
    if let ExecutionResult::List(names) = result {
        for name in &names {
            if let Value::String(s) = name {
                println!("  - {s}");
            }
        }
    }

    // -------------------------------------------------------------------------
    // 5. Combined: text search for "sushi" or "fish", then filter to
    //    within 5 km of Midtown Manhattan
    //
    //    Strategy: start with FTS hits, then apply a geo predicate via
    //    a Gremlin `has()` step to narrow by distance.
    // -------------------------------------------------------------------------
    println!("\n-- \"sushi OR fish\" within 5 km of Midtown --\n");
    let result = graph
        .query(
            "g.searchTextV('description', 'sushi OR fish', 10)\
             .has('location', geo_within_distance(point(-73.9855, 40.7580), 5km))\
             .values('name').toList()",
        )
        .expect("gremlin failed");
    if let ExecutionResult::List(names) = result {
        for name in &names {
            if let Value::String(s) = name {
                println!("  - {s}");
            }
        }
    }

    // -------------------------------------------------------------------------
    // 6. Combined: geo bbox for Brooklyn, then text search for "tacos"
    //
    //    Strategy: start with a bbox scan, chain with a text predicate.
    //    Since `has()` with a text predicate isn't available, we use
    //    `has('cuisine', 'mexican')` as a proxy filter after the bbox.
    // -------------------------------------------------------------------------
    println!("\n-- Mexican restaurants in Brooklyn (bbox + cuisine filter) --\n");
    let result = graph
        .query(
            "g.V().hasLabel('restaurant')\
             .has('location', geo_bbox(-74.05, 40.60, -73.90, 40.73))\
             .has('cuisine', 'mexican')\
             .values('name').toList()",
        )
        .expect("gremlin failed");
    if let ExecutionResult::List(names) = result {
        for name in &names {
            if let Value::String(s) = name {
                println!("  - {s}");
            }
        }
    }

    // -------------------------------------------------------------------------
    // 7. Combined: Italian restaurants within 8 km of downtown Manhattan,
    //    using text search to find mentions of "crust" or "mozzarella"
    // -------------------------------------------------------------------------
    println!("\n-- \"crust OR mozzarella\" within 8 km of downtown Manhattan --\n");
    let result = graph
        .query(
            "g.searchTextV('description', 'crust OR mozzarella', 10)\
             .has('location', geo_within_distance(point(-74.006, 40.7128), 8km))\
             .values('name').toList()",
        )
        .expect("gremlin failed");
    if let ExecutionResult::List(names) = result {
        for name in &names {
            if let Value::String(s) = name {
                println!("  - {s}");
            }
        }
    }

    // -------------------------------------------------------------------------
    // 8. FTS with scores + geo filter: which "noodle" places are nearby?
    // -------------------------------------------------------------------------
    println!("\n-- \"noodles\" near Union Square with BM25 scores --\n");
    let result = graph
        .execute_script(
            "g.searchTextV('description', 'noodles', 10)\
             .has('location', geo_within_distance(point(-73.9903, 40.7359), 5km))\
             .textScore()",
        )
        .expect("gremlin failed");
    if let ExecutionResult::List(scores) = &result.result {
        if scores.is_empty() {
            println!("  (no matches)");
        }
        for (rank, score) in scores.iter().enumerate() {
            if let Value::Float(s) = score {
                println!("  {}. score = {:.4}", rank + 1, s);
            }
        }
    }

    // Also show the names for context.
    let result = graph
        .query(
            "g.searchTextV('description', 'noodles', 10)\
             .has('location', geo_within_distance(point(-73.9903, 40.7359), 5km))\
             .values('name').toList()",
        )
        .expect("gremlin failed");
    if let ExecutionResult::List(names) = result {
        for name in &names {
            if let Value::String(s) = name {
                println!("  -> {s}");
            }
        }
    }

    println!("\n=== Done ===");
}
