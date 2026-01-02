//! Path Tracking Example
//!
//! This example demonstrates path tracking features in RustGremlin:
//! - Automatic path tracking with `with_path()`
//! - Labeling positions with `as_()`
//! - Selecting labeled values with `select()` and `select_one()`
//! - The `path()` terminal step for retrieving full traversal paths
//!
//! Run with: `cargo run --example path_tracking`

use rustgremlin::graph::Graph;
use rustgremlin::storage::InMemoryGraph;
use rustgremlin::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    println!("=== RustGremlin Path Tracking Example ===\n");

    // -------------------------------------------------------------------------
    // Step 1: Create a sample graph
    // -------------------------------------------------------------------------
    // Graph structure:
    //   alice --knows--> bob --knows--> charlie --knows--> diana
    //                     |
    //                     +--works_at--> acme
    //
    let mut storage = InMemoryGraph::new();

    let alice = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    let bob = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props
    });

    let charlie = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    let diana = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Diana".to_string()));
        props.insert("age".to_string(), Value::Int(28));
        props
    });

    let acme = storage.add_vertex("company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Acme Corp".to_string()));
        props
    });

    // Add edges
    storage
        .add_edge(alice, bob, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, diana, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, acme, "works_at", HashMap::new())
        .unwrap();

    println!("Created graph with:");
    println!("  - 5 vertices (4 people, 1 company)");
    println!("  - 4 edges (3 'knows', 1 'works_at')");
    println!("  - Chain: Alice -> Bob -> Charlie -> Diana");
    println!();

    // Wrap in Graph for traversal API
    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // -------------------------------------------------------------------------
    // Section 1: Without path tracking - paths are empty
    // -------------------------------------------------------------------------
    println!("--- Without path tracking ---");
    println!("g.v_ids([alice]).out().out().path()");
    println!();

    let paths_without_tracking = g.v_ids([alice]).out().out().path().to_list();
    println!("Results (paths are empty without with_path()):");
    for (i, p) in paths_without_tracking.iter().enumerate() {
        println!("  Path {}: {:?}", i, p);
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 2: With automatic path tracking
    // -------------------------------------------------------------------------
    println!("--- With automatic path tracking ---");
    println!("g.v_ids([alice]).with_path().out().out().path()");
    println!();

    let paths_with_tracking = g.v_ids([alice]).with_path().out().out().path().to_list();
    println!("Results (full paths recorded):");
    for (i, p) in paths_with_tracking.iter().enumerate() {
        if let Value::List(elements) = p {
            println!("  Path {}: {} elements", i, elements.len());
            for (j, elem) in elements.iter().enumerate() {
                println!("    [{}] {:?}", j, elem);
            }
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 3: Labeling with as_() - works without with_path()
    // -------------------------------------------------------------------------
    println!("--- Labeling positions with as_() ---");
    println!("g.v_ids([alice]).as_(\"start\").out().out().as_(\"end\").path()");
    println!();

    // Note: as_() labeled positions are recorded even without with_path()
    let labeled_paths = g
        .v_ids([alice])
        .as_("start")
        .out()
        .out()
        .as_("end")
        .path()
        .to_list();
    println!("Results (only labeled positions recorded):");
    for (i, p) in labeled_paths.iter().enumerate() {
        if let Value::List(elements) = p {
            println!("  Path {}: {} labeled elements", i, elements.len());
            for (j, elem) in elements.iter().enumerate() {
                println!("    [{}] {:?}", j, elem);
            }
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 4: select_one() - get single labeled value
    // -------------------------------------------------------------------------
    println!("--- select_one() - retrieve single labeled value ---");
    println!("g.v_ids([alice]).as_(\"origin\").out().out().select_one(\"origin\")");
    println!();

    let origins = g
        .v_ids([alice])
        .as_("origin")
        .out()
        .out()
        .select_one("origin")
        .to_list();
    println!("Results (the 'origin' vertex for each traverser):");
    for (i, v) in origins.iter().enumerate() {
        println!("  Result {}: {:?}", i, v);
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 5: select() - get multiple labeled values as Map
    // -------------------------------------------------------------------------
    println!("--- select() - retrieve multiple labeled values as Map ---");
    println!("g.v_ids([alice]).as_(\"a\").out().as_(\"b\").out().as_(\"c\").select(&[\"a\", \"b\", \"c\"])");
    println!();

    let multi_select = g
        .v_ids([alice])
        .as_("a")
        .out()
        .as_("b")
        .out()
        .as_("c")
        .select(&["a", "b", "c"])
        .to_list();
    println!("Results (Map with labeled positions):");
    for (i, v) in multi_select.iter().enumerate() {
        if let Value::Map(map) = v {
            println!("  Result {}:", i);
            for (key, val) in map.iter() {
                println!("    {} -> {:?}", key, val);
            }
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 6: Combining with_path() and as_()
    // -------------------------------------------------------------------------
    println!("--- Combined: with_path() + as_() ---");
    println!("g.v_ids([alice]).with_path().as_(\"start\").out().out().as_(\"end\").path()");
    println!();

    let combined = g
        .v_ids([alice])
        .with_path()
        .as_("start")
        .out()
        .out()
        .as_("end")
        .path()
        .to_list();
    println!("Results (full path with all vertices, labeled positions have labels):");
    for (i, p) in combined.iter().enumerate() {
        if let Value::List(elements) = p {
            println!("  Path {}: {} elements", i, elements.len());
            for (j, elem) in elements.iter().enumerate() {
                println!("    [{}] {:?}", j, elem);
            }
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 7: Practical example - find friends-of-friends
    // -------------------------------------------------------------------------
    println!("--- Practical: Find friends-of-friends ---");
    println!("g.v().has_label(\"person\").as_(\"person\")");
    println!("  .out_labels(&[\"knows\"]).out_labels(&[\"knows\"]).as_(\"fof\")");
    println!("  .select(&[\"person\", \"fof\"])");
    println!();

    let friends_of_friends = g
        .v()
        .has_label("person")
        .as_("person")
        .out_labels(&["knows"])
        .out_labels(&["knows"])
        .as_("fof")
        .select(&["person", "fof"])
        .to_list();

    println!("Friends-of-friends relationships:");
    for v in &friends_of_friends {
        if let Value::Map(map) = v {
            let person = map.get("person");
            let fof = map.get("fof");
            println!("  {:?} is friends-of-friends with {:?}", person, fof);
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 8: Path tracking with edge traversal
    // -------------------------------------------------------------------------
    println!("--- Path tracking through edges ---");
    println!("g.v_ids([alice]).with_path().out_e().in_v().path()");
    println!();

    let edge_paths = g.v_ids([alice]).with_path().out_e().in_v().path().to_list();
    println!("Results (includes edge in path):");
    for (i, p) in edge_paths.iter().enumerate() {
        if let Value::List(elements) = p {
            println!("  Path {}: {} elements", i, elements.len());
            for (j, elem) in elements.iter().enumerate() {
                let elem_type = match elem {
                    Value::Vertex(_) => "Vertex",
                    Value::Edge(_) => "Edge",
                    _ => "Other",
                };
                println!("    [{}] ({}) {:?}", j, elem_type, elem);
            }
        }
    }
    println!();

    println!("=== Example Complete ===");
}
