//! Side Effect Steps Example
//!
//! This example demonstrates the side effect steps for collecting and analyzing
//! traversal data without affecting the main traversal stream:
//!
//! - `store(key)` - Lazily store values as they pass through
//! - `aggregate(key)` - Barrier step that collects all values before continuing
//! - `cap(key)` - Retrieve accumulated side-effect data
//! - `cap_multi(keys)` - Retrieve multiple side-effects as a map
//! - `side_effect(traversal)` - Execute sub-traversal for side effects only
//! - `profile()` / `profile_as(key)` - Collect timing and count metrics
//!
//! Run with: `cargo run --example side_effect_steps`

use intersteller::graph::Graph;
use intersteller::storage::{GraphStorage, InMemoryGraph};
use intersteller::traversal::__;
use intersteller::value::{Value, VertexId};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper to display results in a readable format
fn display_results(results: &[Value], storage: &Arc<InMemoryGraph>) -> String {
    results
        .iter()
        .map(|v| format_value(v, storage))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format a single value for display
fn format_value(value: &Value, storage: &Arc<InMemoryGraph>) -> String {
    match value {
        Value::Vertex(vid) => {
            if let Some(vertex) = storage.get_vertex(*vid) {
                if let Some(Value::String(name)) = vertex.properties.get("name") {
                    return name.clone();
                }
            }
            format!("{:?}", vid)
        }
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => format!("{:.2}", f),
        Value::List(items) => {
            let formatted: Vec<_> = items.iter().map(|v| format_value(v, storage)).collect();
            format!("[{}]", formatted.join(", "))
        }
        Value::Map(map) => {
            let formatted: Vec<_> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_value(v, storage)))
                .collect();
            format!("{{{}}}", formatted.join(", "))
        }
        other => format!("{:?}", other),
    }
}

fn main() {
    println!("=== Intersteller Side Effect Steps Example ===\n");

    // Create test graph
    let (graph, storage, alice, bob, _charlie, _david, _graph_db, _rust_lang) = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // -------------------------------------------------------------------------
    // store() - Lazily store values as they pass through
    // -------------------------------------------------------------------------
    println!("--- store() - Lazily store values as they pass through ---");

    // Store all person vertices while traversing
    let traversal_results = g
        .v()
        .has_label("person")
        .store("people")
        .values("name")
        .to_list();
    println!(
        "Names traversed: [{}]",
        display_results(&traversal_results, &storage)
    );

    // Retrieve stored values using cap()
    let stored_people = g
        .v()
        .has_label("person")
        .store("visited_people")
        .cap("visited_people")
        .to_list();
    println!(
        "Stored people (via cap): {}",
        display_results(&stored_people, &storage)
    );

    // Store values at multiple points in traversal
    let multi_store = g
        .v_ids([alice])
        .store("start")
        .out_labels(&["knows"])
        .store("friends")
        .out_labels(&["knows"])
        .store("friends_of_friends")
        .cap("friends_of_friends")
        .to_list();
    println!(
        "Friends of friends: {}",
        display_results(&multi_store, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // aggregate() - Barrier step that collects all values
    // -------------------------------------------------------------------------
    println!("--- aggregate() - Barrier step that collects all values ---");

    // Aggregate is a barrier - it collects ALL values before continuing
    // This is useful when you need to ensure all values are collected
    // before downstream processing
    let aggregated = g
        .v()
        .has_label("person")
        .aggregate("all_people")
        .cap("all_people")
        .to_list();
    println!(
        "Aggregated all people: {}",
        display_results(&aggregated, &storage)
    );

    // Aggregate property values
    let ages = g
        .v()
        .has_label("person")
        .values("age")
        .aggregate("ages")
        .cap("ages")
        .to_list();
    println!("Aggregated ages: {}", display_results(&ages, &storage));
    println!();

    // -------------------------------------------------------------------------
    // cap() - Retrieve side-effect data
    // -------------------------------------------------------------------------
    println!("--- cap() - Retrieve side-effect data ---");

    // Single key returns a List
    let single_cap = g
        .v()
        .has_label("software")
        .store("software")
        .cap("software")
        .to_list();
    println!(
        "cap(single key) returns List: {}",
        display_results(&single_cap, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // cap_multi() - Retrieve multiple side-effects as a Map
    // -------------------------------------------------------------------------
    println!("--- cap_multi() - Retrieve multiple side-effects as a Map ---");

    // Multiple keys return a Map
    let multi_cap = g
        .v()
        .has_label("person")
        .store("persons")
        .out_labels(&["uses"])
        .store("used_software")
        .cap_multi(["persons", "used_software"])
        .to_list();
    println!(
        "cap_multi returns Map: {}",
        display_results(&multi_cap, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // side_effect() - Execute sub-traversal for side effects only
    // -------------------------------------------------------------------------
    println!("--- side_effect() - Execute sub-traversal for side effects ---");

    // The side_effect step executes a sub-traversal but doesn't change
    // the main traversal stream - useful for logging, counting, etc.
    let with_side_effect = g
        .v_ids([alice])
        .side_effect(__::out_labels(&["knows"]).store("alice_friends"))
        .values("name")
        .to_list();
    println!(
        "Main traversal result (Alice's name): [{}]",
        display_results(&with_side_effect, &storage)
    );

    // The side effect captured Alice's friends
    let friends_from_side_effect = g
        .v_ids([alice])
        .side_effect(__::out_labels(&["knows"]).store("friends_side"))
        .cap("friends_side")
        .to_list();
    println!(
        "Side effect captured friends: {}",
        display_results(&friends_from_side_effect, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // profile() - Collect timing and count metrics
    // -------------------------------------------------------------------------
    println!("--- profile() - Collect timing and count metrics ---");

    // Profile captures count and timing information
    let profiled = g.v().has_label("person").profile().cap("profile").to_list();
    println!("Profile data: {}", display_results(&profiled, &storage));

    // Profile with custom key
    let custom_profile = g
        .v()
        .out_labels(&["knows"])
        .profile_as("knows_profile")
        .cap("knows_profile")
        .to_list();
    println!(
        "Custom profile key: {}",
        display_results(&custom_profile, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // Combined example - Complex pipeline with multiple side effects
    // -------------------------------------------------------------------------
    println!("--- Combined Example - Complex pipeline ---");

    // Build a traversal that:
    // 1. Starts from Alice
    // 2. Stores the starting vertex
    // 3. Follows 'knows' edges and stores friends
    // 4. Uses side_effect to count connections
    // 5. Profiles the traversal
    // 6. Retrieves all side effects
    let complex = g
        .v_ids([alice])
        .store("start_vertex")
        .out_labels(&["knows"])
        .store("direct_friends")
        .out_labels(&["knows"])
        .aggregate("fof") // friends of friends (barrier)
        .profile_as("fof_profile")
        .cap_multi(["start_vertex", "direct_friends", "fof", "fof_profile"])
        .to_list();

    println!("Complex pipeline results:");
    for result in &complex {
        if let Value::Map(map) = result {
            for (key, value) in map {
                println!("  {}: {}", key, format_value(value, &storage));
            }
        }
    }
    println!();

    // -------------------------------------------------------------------------
    // Anonymous traversal factories
    // -------------------------------------------------------------------------
    println!("--- Anonymous Traversal Factories (__::) ---");

    // Using __::store() in a union
    let anon_store = g
        .v_ids([alice, bob])
        .append(__::out_labels(&["knows"]).store("knows_targets"))
        .cap("knows_targets")
        .to_list();
    println!(
        "__::store() in pipeline: {}",
        display_results(&anon_store, &storage)
    );

    // Using __::aggregate() factory
    let anon_agg = g
        .v()
        .has_label("person")
        .append(__::aggregate("anon_people"))
        .cap("anon_people")
        .to_list();
    println!(
        "__::aggregate() factory: {}",
        display_results(&anon_agg, &storage)
    );

    // Using __::profile() in anonymous context
    let anon_profile = g
        .v()
        .append(__::out().profile_as("out_profile"))
        .cap("out_profile")
        .to_list();
    println!(
        "__::profile() factory: {}",
        display_results(&anon_profile, &storage)
    );
    println!();

    println!("=== Example Complete ===");
}

/// Create a test graph demonstrating side effect step scenarios
///
/// Graph structure:
/// ```text
///   Alice --knows--> Bob --knows--> Charlie --knows--> David
///     |               |                |
///     +--uses--> GraphDB <--uses-------+
///     |
///     +--uses--> Rust
/// ```
fn create_test_graph() -> (
    Graph,
    Arc<InMemoryGraph>,
    VertexId,
    VertexId,
    VertexId,
    VertexId,
    VertexId,
    VertexId,
) {
    let mut storage = InMemoryGraph::new();

    // Create person vertices
    let alice = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props.insert("role".to_string(), Value::String("Engineer".to_string()));
        props
    });

    let bob = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props.insert("role".to_string(), Value::String("Designer".to_string()));
        props
    });

    let charlie = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props.insert("role".to_string(), Value::String("Manager".to_string()));
        props
    });

    let david = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("David".to_string()));
        props.insert("age".to_string(), Value::Int(28));
        props.insert("role".to_string(), Value::String("Analyst".to_string()));
        props
    });

    // Create software vertices
    let graph_db = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(2.0));
        props
    });

    let rust_lang = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Rust".to_string()));
        props.insert("version".to_string(), Value::Float(1.75));
        props
    });

    // Create 'knows' edges: Alice -> Bob -> Charlie -> David
    storage
        .add_edge(alice, bob, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, david, "knows", HashMap::new())
        .unwrap();

    // Create 'uses' edges
    storage
        .add_edge(alice, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, rust_lang, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, graph_db, "uses", HashMap::new())
        .unwrap();

    let storage = Arc::new(storage);
    let graph = Graph::new(storage.clone());
    (
        graph, storage, alice, bob, charlie, david, graph_db, rust_lang,
    )
}
