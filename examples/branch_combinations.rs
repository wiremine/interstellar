//! Branch Step Combinations Example
//!
//! This example demonstrates combining branch steps for complex traversal patterns:
//! - `union` + `coalesce`: Merge results with per-branch fallbacks
//! - `choose` + `optional`: Conditional paths with safe fallbacks
//! - `coalesce` + `choose`: Priority-based conditional routing
//! - `union` + `choose`: Type-specific multi-path traversals
//! - Nested patterns for real-world query scenarios
//!
//! Run with: `cargo run --example branch_combinations`

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::traversal::__;
use intersteller::value::{Value, VertexId};
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    println!("=== Intersteller Branch Combinations Example ===\n");

    let (graph, _storage, ids) = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // =========================================================================
    // 1. union + coalesce: Merge results with per-branch fallbacks
    // =========================================================================
    // Pattern: Get display names from multiple relationship types,
    // with nickname->name fallback for each branch
    println!("--- 1. union + coalesce: Merge with per-branch fallbacks ---");

    let display_names = g
        .v_ids([ids.alice])
        .union(vec![
            // Branch 1: People Alice knows (with nickname fallback)
            __::out_labels(&["knows"]).coalesce(vec![__::values("nickname"), __::values("name")]),
            // Branch 2: People Alice manages (with nickname fallback)
            __::out_labels(&["manages"]).coalesce(vec![__::values("nickname"), __::values("name")]),
        ])
        .to_list();
    println!(
        "Alice's connections (knows + manages) with display names: {:?}",
        display_names
    );

    // =========================================================================
    // 2. choose + optional: Conditional paths with safe fallbacks
    // =========================================================================
    // Pattern: If person, try to get their manager; if no manager, keep original.
    // If not a person, just traverse out.
    println!("\n--- 2. choose + optional: Conditional with safe fallback ---");

    let results = g
        .v()
        .choose(
            __::has_label("person"),
            // If person: optionally get manager, keep self if none
            __::optional(__::in_labels(&["manages"])),
            // If not person: get any outgoing neighbors
            __::out(),
        )
        .dedup()
        .to_list();
    println!(
        "Vertices with manager (or self) / non-person neighbors: {} results",
        results.len()
    );

    // Show specific behavior for Diana (has manager) vs Bob (no manager)
    let diana_result = g
        .v_ids([ids.diana])
        .optional(__::in_labels(&["manages"]))
        .values("name")
        .to_list();
    println!(
        "Diana (has manager) -> optional(manager): {:?}",
        diana_result
    );

    let bob_result = g
        .v_ids([ids.bob])
        .optional(__::in_labels(&["manages"]))
        .values("name")
        .to_list();
    println!("Bob (no manager) -> optional(manager): {:?}", bob_result);

    // =========================================================================
    // 3. coalesce + choose: Priority-based conditional routing
    // =========================================================================
    // Pattern: Try to get preferred software first; if none, route based on
    // department (engineering->GraphDB users, others->any software users)
    println!("\n--- 3. coalesce + choose: Priority-based conditional routing ---");

    let software_connections = g
        .v_ids([ids.alice, ids.charlie])
        .coalesce(vec![
            // Priority 1: Direct "prefers" relationship
            __::out_labels(&["prefers"]),
            // Priority 2: Department-based routing
            __::choose(
                __::has_value("department", "Engineering"),
                // Engineering: find GraphDB users they know
                __::out_labels(&["knows"])
                    .out_labels(&["uses"])
                    .has_value("name", "GraphDB"),
                // Others: find any software they use
                __::out_labels(&["uses"]),
            ),
        ])
        .values("name")
        .to_list();
    println!(
        "Software for Alice & Charlie (prefers > dept-based): {:?}",
        software_connections
    );

    // =========================================================================
    // 4. union + choose: Type-specific multi-path traversal
    // =========================================================================
    // Pattern: Gather related entities differently based on vertex type
    println!("\n--- 4. union + choose: Type-specific multi-path traversal ---");

    let related_entities = g
        .v_ids([ids.alice, ids.graph_db])
        .union(vec![
            // Path 1: Type-specific "primary" relationship
            __::choose(
                __::has_label("person"),
                __::out_labels(&["works_at"]), // Person -> their company
                __::in_labels(&["created"]),   // Software -> its creator
            ),
            // Path 2: Type-specific "secondary" relationship
            __::choose(
                __::has_label("person"),
                __::out_labels(&["uses"]), // Person -> software they use
                __::in_labels(&["uses"]),  // Software -> its users
            ),
        ])
        .dedup()
        .to_list();
    println!(
        "Related entities (type-aware): {} unique results",
        related_entities.len()
    );

    // =========================================================================
    // 5. Nested combination: Robust multi-strategy search
    // =========================================================================
    // Pattern: Find the "best contact" for each person using cascading strategies
    println!("\n--- 5. Nested: Robust multi-strategy contact search ---");

    let best_contacts = g
        .v()
        .has_label("person")
        .coalesce(vec![
            // Strategy 1: Their manager (authoritative contact)
            __::in_labels(&["manages"]),
            // Strategy 2: Senior colleague they know (age > 30)
            __::out_labels(&["knows"])
                .has_label("person")
                .filter(|ctx, v| {
                    if let Some(vid) = v.as_vertex_id() {
                        if let Some(vertex) = ctx.snapshot().storage().get_vertex(vid) {
                            if let Some(Value::Int(age)) = vertex.properties.get("age") {
                                return *age > 30;
                            }
                        }
                    }
                    false
                }),
            // Strategy 3: Anyone they know (fallback)
            __::out_labels(&["knows"]).limit(1),
            // Strategy 4: Keep themselves (last resort)
            __::identity(),
        ])
        .values("name")
        .to_list();
    println!("Best contact for each person: {:?}", best_contacts);

    println!("\n=== Example Complete ===");
}

/// Create test graph with varied relationships for demonstrating combinations.
///
/// Structure:
/// - Alice (Engineering, has nickname) --knows--> Bob, Charlie
/// - Alice --manages--> Diana
/// - Alice --works_at--> Acme, --uses--> GraphDB, --prefers--> GraphDB
/// - Bob (Engineering) --knows--> Charlie, --uses--> GraphDB
/// - Charlie (Research, has nickname) --works_at--> Acme, --uses--> DataStore
/// - Diana (Engineering) --uses--> GraphDB
/// - Eve (Management) --created--> GraphDB
fn create_test_graph() -> (Graph, Arc<InMemoryGraph>, VertexIds) {
    let mut storage = InMemoryGraph::new();

    let alice = storage.add_vertex("person", {
        let mut p = HashMap::new();
        p.insert("name".into(), Value::String("Alice".into()));
        p.insert("nickname".into(), Value::String("Ali".into()));
        p.insert("age".into(), Value::Int(35));
        p.insert("department".into(), Value::String("Engineering".into()));
        p
    });

    let bob = storage.add_vertex("person", {
        let mut p = HashMap::new();
        p.insert("name".into(), Value::String("Bob".into()));
        p.insert("age".into(), Value::Int(28));
        p.insert("department".into(), Value::String("Engineering".into()));
        p
    });

    let charlie = storage.add_vertex("person", {
        let mut p = HashMap::new();
        p.insert("name".into(), Value::String("Charlie".into()));
        p.insert("nickname".into(), Value::String("Chuck".into()));
        p.insert("age".into(), Value::Int(32));
        p.insert("department".into(), Value::String("Research".into()));
        p
    });

    let diana = storage.add_vertex("person", {
        let mut p = HashMap::new();
        p.insert("name".into(), Value::String("Diana".into()));
        p.insert("age".into(), Value::Int(26));
        p.insert("department".into(), Value::String("Engineering".into()));
        p
    });

    let eve = storage.add_vertex("person", {
        let mut p = HashMap::new();
        p.insert("name".into(), Value::String("Eve".into()));
        p.insert("age".into(), Value::Int(45));
        p.insert("department".into(), Value::String("Management".into()));
        p
    });

    let acme = storage.add_vertex("company", {
        let mut p = HashMap::new();
        p.insert("name".into(), Value::String("Acme Corp".into()));
        p
    });

    let graph_db = storage.add_vertex("software", {
        let mut p = HashMap::new();
        p.insert("name".into(), Value::String("GraphDB".into()));
        p
    });

    let datastore = storage.add_vertex("software", {
        let mut p = HashMap::new();
        p.insert("name".into(), Value::String("DataStore".into()));
        p
    });

    // Relationships
    storage
        .add_edge(alice, bob, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, diana, "manages", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, acme, "works_at", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, acme, "works_at", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, graph_db, "prefers", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, datastore, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(eve, graph_db, "created", HashMap::new())
        .unwrap();

    let storage = Arc::new(storage);
    (
        Graph::from_arc(storage.clone()),
        storage,
        VertexIds {
            alice,
            bob,
            charlie,
            diana,
            eve,
            graph_db,
            datastore,
            acme,
        },
    )
}

struct VertexIds {
    alice: VertexId,
    bob: VertexId,
    charlie: VertexId,
    diana: VertexId,
    #[allow(dead_code)]
    eve: VertexId,
    graph_db: VertexId,
    #[allow(dead_code)]
    datastore: VertexId,
    #[allow(dead_code)]
    acme: VertexId,
}
