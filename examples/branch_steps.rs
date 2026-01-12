//! Branch Steps Example
//!
//! This example demonstrates the branch steps for controlling traversal flow:
//! - `union()` - Merge results from multiple branches
//! - `coalesce()` - First branch with results wins
//! - `choose()` - Conditional branching
//! - `optional()` - Try sub-traversal, keep original if empty
//! - `local()` - Execute in isolated scope per traverser
//!
//! Run with: `cargo run --example branch_steps`

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
        .map(|v| match v {
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
            Value::Float(f) => f.to_string(),
            other => format!("{:?}", other),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn main() {
    println!("=== Intersteller Branch Steps Example ===\n");

    // Create test graph
    let (graph, storage, alice, bob, charlie, graph_db) = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // -------------------------------------------------------------------------
    // union() - Merge results from multiple branches
    // -------------------------------------------------------------------------
    println!("--- union() - Merge results from multiple branches ---");

    // Get neighbors in both directions from Alice
    let union_results = g.v_ids([alice]).union(vec![__::out(), __::in_()]).to_list();
    println!(
        "Alice's neighbors (out + in): [{}]",
        display_results(&union_results, &storage)
    );

    // Union with specific edge labels
    let labeled_union = g
        .v_ids([alice])
        .union(vec![__::out_labels(&["knows"]), __::out_labels(&["uses"])])
        .to_list();
    println!(
        "Alice's 'knows' + 'uses' neighbors: [{}]",
        display_results(&labeled_union, &storage)
    );

    // Using __::union() factory for anonymous traversal
    let anon_union = __::union(vec![__::out(), __::in_()]);
    let anon_results = g.v_ids([bob]).append(anon_union).to_list();
    println!(
        "Bob's neighbors via __::union(): [{}]",
        display_results(&anon_results, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // coalesce() - First branch with results wins
    // -------------------------------------------------------------------------
    println!("--- coalesce() - First branch with results wins ---");

    // Try to get nickname, fall back to name
    // Alice has a nickname, Bob does not
    let alice_display = g
        .v_ids([alice])
        .coalesce(vec![__::values("nickname"), __::values("name")])
        .to_list();
    println!(
        "Alice display name (has nickname): [{}]",
        display_results(&alice_display, &storage)
    );

    let bob_display = g
        .v_ids([bob])
        .coalesce(vec![__::values("nickname"), __::values("name")])
        .to_list();
    println!(
        "Bob display name (no nickname, falls back): [{}]",
        display_results(&bob_display, &storage)
    );

    // Multiple fallback branches - first non-empty wins
    let fallback_chain = g
        .v_ids([charlie])
        .coalesce(vec![
            __::out_labels(&["nonexistent"]),
            __::out_labels(&["uses"]),
            __::out_labels(&["knows"]),
        ])
        .to_list();
    println!(
        "Charlie's first available edge type ('uses' wins): [{}]",
        display_results(&fallback_chain, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // choose() - Conditional branching
    // -------------------------------------------------------------------------
    println!("--- choose() - Conditional branching ---");

    // If person, follow 'knows' edges; otherwise follow all outgoing edges
    let conditional = g
        .v()
        .choose(
            __::has_label("person"),
            __::out_labels(&["knows"]),
            __::out(),
        )
        .to_list();
    println!(
        "Conditional traversal (person->knows, else->out): [{}]",
        display_results(&conditional, &storage)
    );

    // Branch based on property: age=30 follow 'uses', others follow 'knows'
    let age_branch = g
        .v_ids([alice, bob])
        .choose(
            __::has_value("age", 30i64), // Alice is 30
            __::out_labels(&["uses"]),
            __::out_labels(&["knows"]),
        )
        .to_list();
    println!(
        "Age-based branching (Alice:30->uses, Bob:25->knows): [{}]",
        display_results(&age_branch, &storage)
    );

    // Condition with chained traversal
    let chained_cond = g
        .v_ids([alice])
        .choose(
            __::out_labels(&["knows"]).has_value("name", "Bob"),
            __::values("nickname"),
            __::values("name"),
        )
        .to_list();
    println!(
        "Chained condition (knows Bob? -> nickname): [{}]",
        display_results(&chained_cond, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // optional() - Try sub-traversal, keep original if empty
    // -------------------------------------------------------------------------
    println!("--- optional() - Try sub-traversal, keep original if empty ---");

    // Alice has outgoing edges, so optional returns the traversal results
    let alice_optional = g
        .v_ids([alice])
        .optional(__::out_labels(&["knows"]))
        .to_list();
    println!(
        "Alice optional(knows): [{}] (has 'knows' edges)",
        display_results(&alice_optional, &storage)
    );

    // GraphDB has no outgoing edges, so optional returns GraphDB itself
    let graphdb_optional = g.v_ids([graph_db]).optional(__::out()).to_list();
    println!(
        "GraphDB optional(out): [{}] (no out edges, keeps original)",
        display_results(&graphdb_optional, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // local() - Execute in isolated scope per traverser
    // -------------------------------------------------------------------------
    println!("--- local() - Execute in isolated scope per traverser ---");

    // Without local: global limit takes first 1 across all traversers
    let global_limit = g
        .v_ids([alice, bob])
        .out_labels(&["knows"])
        .limit(1)
        .to_list();
    println!(
        "Global limit(1): [{}] (only 1 total)",
        display_results(&global_limit, &storage)
    );

    // With local: each traverser gets its own limit
    let local_limit = g
        .v_ids([alice, bob])
        .local(__::out_labels(&["knows"]).limit(1))
        .to_list();
    println!(
        "Local limit(1): [{}] (1 per starting vertex)",
        display_results(&local_limit, &storage)
    );

    // Local dedup: removes duplicates per-traverser, not globally
    let local_dedup = g
        .v_ids([alice, bob])
        .local(__::union(vec![__::out_labels(&["knows"]), __::out_labels(&["knows"])]).dedup())
        .to_list();
    println!(
        "Local dedup on duplicate branches: [{}]",
        display_results(&local_dedup, &storage)
    );

    // Transform within local scope
    let local_values = g
        .v_ids([alice])
        .local(__::out_labels(&["knows"]).values("name"))
        .to_list();
    println!(
        "Local values extraction: [{}]",
        display_results(&local_values, &storage)
    );
    println!();

    println!("=== Example Complete ===");
}

/// Create a test graph demonstrating branch step scenarios
///
/// Graph structure:
/// ```text
///   Alice (nickname: "Ali") --knows--> Bob --knows--> Charlie
///         |                                              |
///         +--uses--> GraphDB <--uses--------------------+
/// ```
fn create_test_graph() -> (
    Graph,
    Arc<InMemoryGraph>,
    VertexId,
    VertexId,
    VertexId,
    VertexId,
) {
    let mut storage = InMemoryGraph::new();

    // Alice has a nickname (for coalesce demo)
    let alice = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("nickname".to_string(), Value::String("Ali".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    // Bob has no nickname (for coalesce fallback demo)
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

    // GraphDB has no outgoing edges (for optional demo)
    let graph_db = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(1.0));
        props
    });

    // Edges: Alice -> Bob -> Charlie, Alice -> GraphDB, Charlie -> GraphDB
    storage
        .add_edge(alice, bob, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, graph_db, "uses", HashMap::new())
        .unwrap();

    let storage = Arc::new(storage);
    let graph = Graph::new(storage.clone());
    (graph, storage, alice, bob, charlie, graph_db)
}
