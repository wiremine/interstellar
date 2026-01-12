//! Path Tracking Example
//!
//! This example demonstrates path tracking features in Intersteller:
//! - Automatic path tracking with `with_path()`
//! - Labeling positions with `as_()`
//! - Selecting labeled values with `select()` and `select_one()`
//! - The `path()` step for retrieving full traversal paths
//!
//! Key concepts:
//! - `with_path()`: Enables automatic recording of ALL traversed elements
//! - `as_("label")`: Records the current value with a label (works without with_path)
//! - `path()`: Returns the path as a Value::List (values only, no label metadata)
//! - `select()`/`select_one()`: Retrieves labeled values by name
//!
//! Run with: `cargo run --example path_tracking`

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::value::{Value, VertexId};
use std::collections::HashMap;

/// Helper to get a vertex name from the graph for display purposes
fn get_name(snapshot: &intersteller::graph::GraphSnapshot, vid: VertexId) -> String {
    snapshot
        .storage()
        .get_vertex(vid)
        .and_then(|v| v.properties.get("name").cloned())
        .and_then(|v| {
            if let Value::String(s) = v {
                Some(s)
            } else {
                None
            }
        })
        .unwrap_or_else(|| format!("{:?}", vid))
}

/// Helper to format a value for display
fn format_value(snapshot: &intersteller::graph::GraphSnapshot, value: &Value) -> String {
    match value {
        Value::Vertex(vid) => get_name(snapshot, *vid),
        Value::Edge(eid) => format!("Edge({:?})", eid),
        other => format!("{:?}", other),
    }
}

fn main() {
    println!("=== Intersteller Path Tracking Example ===\n");

    // -------------------------------------------------------------------------
    // Step 1: Create a sample graph
    // -------------------------------------------------------------------------
    // Graph structure:
    //   Alice --knows--> Bob --knows--> Charlie --knows--> Diana
    //                     |
    //                     +--works_at--> Acme Corp
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
    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // -------------------------------------------------------------------------
    // Section 1: Without path tracking - paths are empty
    // -------------------------------------------------------------------------
    println!("--- Section 1: Without path tracking ---");
    println!("Query: g.v_ids([alice]).out().out().path()");
    println!();
    println!("By default, path tracking is disabled for performance.");
    println!("The path() step returns an empty list.");
    println!();

    let paths_without_tracking = g.v_ids([alice]).out().out().path().to_list();
    for (i, p) in paths_without_tracking.iter().enumerate() {
        println!("  Path {}: {:?}", i, p);
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 2: With automatic path tracking
    // -------------------------------------------------------------------------
    println!("--- Section 2: with_path() enables automatic tracking ---");
    println!("Query: g.v_ids([alice]).with_path().out().out().path()");
    println!();
    println!("with_path() records every element visited during traversal.");
    println!();

    let paths_with_tracking = g.v_ids([alice]).with_path().out().out().path().to_list();
    for (i, p) in paths_with_tracking.iter().enumerate() {
        if let Value::List(elements) = p {
            let names: Vec<String> = elements
                .iter()
                .map(|e| format_value(&snapshot, e))
                .collect();
            println!("  Path {}: {} -> {} -> {}", i, names[0], names[1], names[2]);
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 3: as_() labels positions for later retrieval
    // -------------------------------------------------------------------------
    println!("--- Section 3: as_() labels positions (works without with_path) ---");
    println!("Query: g.v_ids([alice]).as_(\"start\").out().out().as_(\"end\").path()");
    println!();
    println!("as_() records the current value with a label.");
    println!("Labels are stored as metadata - path() returns values only.");
    println!("Use select()/select_one() to retrieve by label name.");
    println!();

    let labeled_paths = g
        .v_ids([alice])
        .as_("start")
        .out()
        .out()
        .as_("end")
        .path()
        .to_list();

    for (i, p) in labeled_paths.iter().enumerate() {
        if let Value::List(elements) = p {
            let names: Vec<String> = elements
                .iter()
                .map(|e| format_value(&snapshot, e))
                .collect();
            // Only labeled positions are in the path (start and end)
            println!(
                "  Path {}: [start] {} ... [end] {}",
                i,
                names.first().unwrap_or(&"?".to_string()),
                names.last().unwrap_or(&"?".to_string())
            );
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 4: select_one() retrieves a labeled value
    // -------------------------------------------------------------------------
    println!("--- Section 4: select_one() retrieves a labeled value ---");
    println!("Query: g.v_ids([alice]).as_(\"origin\").out().out().select_one(\"origin\")");
    println!();
    println!("select_one() returns the labeled value directly.");
    println!();

    let origins = g
        .v_ids([alice])
        .as_("origin")
        .out()
        .out()
        .select_one("origin")
        .to_list();

    for (i, v) in origins.iter().enumerate() {
        println!("  Result {}: origin = {}", i, format_value(&snapshot, v));
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 5: select() retrieves multiple labeled values as Map
    // -------------------------------------------------------------------------
    println!("--- Section 5: select() retrieves multiple labels as Map ---");
    println!("Query: g.v_ids([alice]).as_(\"a\").out().as_(\"b\").out().as_(\"c\")");
    println!("         .select(&[\"a\", \"b\", \"c\"])");
    println!();
    println!("select() returns a Map with label -> value entries.");
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

    for (i, v) in multi_select.iter().enumerate() {
        if let Value::Map(map) = v {
            let a = map.get("a").map(|v| format_value(&snapshot, v));
            let b = map.get("b").map(|v| format_value(&snapshot, v));
            let c = map.get("c").map(|v| format_value(&snapshot, v));
            println!(
                "  Result {}: a={}, b={}, c={}",
                i,
                a.unwrap_or_default(),
                b.unwrap_or_default(),
                c.unwrap_or_default()
            );
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 6: Combining with_path() and as_()
    // -------------------------------------------------------------------------
    println!("--- Section 6: Combining with_path() + as_() ---");
    println!("Query: g.v_ids([alice]).with_path().as_(\"start\").out().out().as_(\"end\").path()");
    println!();
    println!("with_path() records ALL steps, as_() adds labels to specific ones.");
    println!("path() shows all values; select() retrieves labeled ones.");
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

    for (i, p) in combined.iter().enumerate() {
        if let Value::List(elements) = p {
            let names: Vec<String> = elements
                .iter()
                .map(|e| format_value(&snapshot, e))
                .collect();
            println!("  Path {}: {} -> {} -> {}", i, names[0], names[1], names[2]);
        }
    }

    // Show how select() retrieves the labeled positions
    println!();
    println!("  Using select() on the same traversal:");
    let combined_select = g
        .v_ids([alice])
        .with_path()
        .as_("start")
        .out()
        .out()
        .as_("end")
        .select(&["start", "end"])
        .to_list();

    for (i, v) in combined_select.iter().enumerate() {
        if let Value::Map(map) = v {
            let start = map.get("start").map(|v| format_value(&snapshot, v));
            let end = map.get("end").map(|v| format_value(&snapshot, v));
            println!(
                "    Result {}: start={}, end={}",
                i,
                start.unwrap_or_default(),
                end.unwrap_or_default()
            );
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 7: Practical example - find friends-of-friends
    // -------------------------------------------------------------------------
    println!("--- Section 7: Practical - Find friends-of-friends ---");
    println!("Query: g.v().has_label(\"person\").as_(\"person\")");
    println!("         .out_labels(&[\"knows\"]).out_labels(&[\"knows\"]).as_(\"fof\")");
    println!("         .select(&[\"person\", \"fof\"])");
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
            let person = map.get("person").map(|v| format_value(&snapshot, v));
            let fof = map.get("fof").map(|v| format_value(&snapshot, v));
            println!(
                "  {} -> (friend) -> {}",
                person.unwrap_or_default(),
                fof.unwrap_or_default()
            );
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Section 8: Path tracking with edge traversal
    // -------------------------------------------------------------------------
    println!("--- Section 8: Path tracking through edges ---");
    println!("Query: g.v_ids([alice]).with_path().out_e().in_v().path()");
    println!();
    println!("Edges are also recorded when using out_e()/in_e() steps.");
    println!();

    let edge_paths = g.v_ids([alice]).with_path().out_e().in_v().path().to_list();
    for (i, p) in edge_paths.iter().enumerate() {
        if let Value::List(elements) = p {
            print!("  Path {}: ", i);
            for (j, elem) in elements.iter().enumerate() {
                if j > 0 {
                    print!(" -> ");
                }
                match elem {
                    Value::Vertex(vid) => print!("{}", get_name(&snapshot, *vid)),
                    Value::Edge(_) => print!("[edge]"),
                    _ => print!("{:?}", elem),
                }
            }
            println!();
        }
    }
    println!();

    println!("=== Example Complete ===");
}
