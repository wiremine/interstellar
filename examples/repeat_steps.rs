//! Repeat Steps Example
//!
//! This example demonstrates the repeat step for iterative graph exploration:
//!
//! - `repeat(sub).times(n)` - Execute exactly n iterations
//! - `repeat(sub).until(condition)` - Continue until condition is met
//! - `repeat(sub).emit()` - Emit results from all iterations
//! - `repeat(sub).emit_first()` - Also emit the starting vertex
//! - `repeat(sub).emit_if(condition)` - Selectively emit based on condition
//!
//! The repeat step processes the graph in breadth-first order, maintaining
//! a frontier of traversers at each depth level.
//!
//! Run with: `cargo run --example repeat_steps`

use intersteller::graph::Graph;
use intersteller::storage::{GraphStorage, InMemoryGraph};
use intersteller::traversal::__;
use intersteller::value::{Value, VertexId};
use std::collections::HashMap;
use std::sync::Arc;

/// Helper to display vertex results by name
fn display_vertices(results: &[Value], storage: &Arc<InMemoryGraph>) -> String {
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
            other => format!("{:?}", other),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn main() {
    println!("=== Intersteller Repeat Steps Example ===\n");

    // Create test graph
    let (graph, storage, vertices) = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // -------------------------------------------------------------------------
    // repeat().times(n) - Fixed iteration count
    // -------------------------------------------------------------------------
    println!("--- repeat().times(n) - Fixed iteration count ---");
    println!("Graph: Alice -> Bob -> Charlie -> Dave");
    println!();

    // One hop: Alice's direct friends
    let one_hop = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(1)
        .to_list();
    println!(
        "repeat(out('knows')).times(1) from Alice: [{}]",
        display_vertices(&one_hop, &storage)
    );

    // Two hops: Friends of friends
    let two_hops = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(2)
        .to_list();
    println!(
        "repeat(out('knows')).times(2) from Alice: [{}]",
        display_vertices(&two_hops, &storage)
    );

    // Three hops: Friends of friends of friends
    let three_hops = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(3)
        .to_list();
    println!(
        "repeat(out('knows')).times(3) from Alice: [{}]",
        display_vertices(&three_hops, &storage)
    );

    // times(0) - No iteration, returns starting vertex
    let zero_hops = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(0)
        .to_list();
    println!(
        "repeat(out('knows')).times(0) from Alice: [{}]",
        display_vertices(&zero_hops, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // repeat().until(condition) - Conditional termination
    // -------------------------------------------------------------------------
    println!("--- repeat().until(condition) - Conditional termination ---");

    // Traverse until reaching a company vertex
    let until_company = g
        .v_ids([vertices.alice])
        .repeat(__::out())
        .until(__::has_label("company"))
        .to_list();
    println!(
        "repeat(out()).until(has_label('company')) from Alice: [{}]",
        display_vertices(&until_company, &storage)
    );

    // Traverse until reaching a specific person
    let until_charlie = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .until(__::has_value("name", "Charlie"))
        .to_list();
    println!(
        "repeat(out('knows')).until(name='Charlie') from Alice: [{}]",
        display_vertices(&until_charlie, &storage)
    );

    // From Bob, traverse until reaching a software vertex
    let until_software = g
        .v_ids([vertices.bob])
        .repeat(__::out())
        .until(__::has_label("software"))
        .to_list();
    println!(
        "repeat(out()).until(has_label('software')) from Bob: [{}]",
        display_vertices(&until_software, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // repeat().emit() - Emit all intermediate results
    // -------------------------------------------------------------------------
    println!("--- repeat().emit() - Emit all intermediate results ---");

    // Emit all vertices along the path (friends at each level)
    let emit_all = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(3)
        .emit()
        .to_list();
    println!(
        "repeat(out('knows')).times(3).emit() from Alice: [{}]",
        display_vertices(&emit_all, &storage)
    );
    println!("  ^ Emits: Bob (1 hop), Charlie (2 hops), Dave (3 hops)");

    // Compare: without emit(), only final result
    let no_emit = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(3)
        .to_list();
    println!(
        "repeat(out('knows')).times(3) from Alice (no emit): [{}]",
        display_vertices(&no_emit, &storage)
    );
    println!("  ^ Only emits: Dave (final result)");
    println!();

    // -------------------------------------------------------------------------
    // repeat().emit().emit_first() - Include starting vertex
    // -------------------------------------------------------------------------
    println!("--- repeat().emit().emit_first() - Include starting vertex ---");

    // Include Alice in the results
    let with_first = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(2)
        .emit()
        .emit_first()
        .to_list();
    println!(
        "repeat(out('knows')).times(2).emit().emit_first() from Alice: [{}]",
        display_vertices(&with_first, &storage)
    );
    println!("  ^ Includes starting vertex Alice before Bob and Charlie");

    // Without emit_first
    let without_first = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(2)
        .emit()
        .to_list();
    println!(
        "repeat(out('knows')).times(2).emit() from Alice (no emit_first): [{}]",
        display_vertices(&without_first, &storage)
    );
    println!("  ^ Does not include starting vertex Alice");
    println!();

    // -------------------------------------------------------------------------
    // repeat().emit_if(condition) - Selective emission
    // -------------------------------------------------------------------------
    println!("--- repeat().emit_if(condition) - Selective emission ---");

    // Traverse following all outgoing edges, but only emit person vertices
    let emit_if_person = g
        .v_ids([vertices.alice])
        .repeat(__::out())
        .times(3)
        .emit_if(__::has_label("person"))
        .to_list();
    println!(
        "repeat(out()).times(3).emit_if(has_label('person')) from Alice: [{}]",
        display_vertices(&emit_if_person, &storage)
    );
    println!("  ^ Only emits person vertices, skips software/company");

    // Emit only vertices with age > 30
    let emit_if_age = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(3)
        .emit_if(__::has("age"))
        .to_list();
    println!(
        "repeat(out('knows')).times(3).emit_if(has('age')) from Alice: [{}]",
        display_vertices(&emit_if_age, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // Combined modifiers
    // -------------------------------------------------------------------------
    println!("--- Combined modifiers ---");

    // times() + until(): Use times as a safety limit with until condition
    // (until can trigger before times limit)
    let combined_times_until = g
        .v_ids([vertices.alice])
        .repeat(__::out())
        .times(10) // Safety limit
        .until(__::has_label("company"))
        .to_list();
    println!(
        "repeat(out()).times(10).until(company) from Alice: [{}]",
        display_vertices(&combined_times_until, &storage)
    );
    println!("  ^ Until triggers before times(10) limit");

    // times() + emit() + emit_first(): Full path including start
    let full_path = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(3)
        .emit()
        .emit_first()
        .to_list();
    println!(
        "Full path from Alice (emit + emit_first): [{}]",
        display_vertices(&full_path, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // Handling leaf nodes and graph exhaustion
    // -------------------------------------------------------------------------
    println!("--- Handling leaf nodes and graph exhaustion ---");

    // Dave has no outgoing 'knows' edges - traversal stops at exhaustion
    let from_dave = g
        .v_ids([vertices.dave])
        .repeat(__::out_labels(&["knows"]))
        .times(5)
        .to_list();
    println!(
        "repeat(out('knows')).times(5) from Dave: [{}]",
        display_vertices(&from_dave, &storage)
    );
    println!("  ^ Dave has no 'knows' edges, returns Dave (exhausted at step 0)");

    // TechCorp is a leaf node with no outgoing edges
    let from_company = g
        .v_ids([vertices.tech_corp])
        .repeat(__::out())
        .times(3)
        .to_list();
    println!(
        "repeat(out()).times(3) from TechCorp: [{}]",
        display_vertices(&from_company, &storage)
    );
    println!("  ^ TechCorp has no outgoing edges, returns TechCorp");
    println!();

    // -------------------------------------------------------------------------
    // Multiple starting vertices
    // -------------------------------------------------------------------------
    println!("--- Multiple starting vertices ---");

    // Start from both Alice and Bob
    let multi_start = g
        .v_ids([vertices.alice, vertices.bob])
        .repeat(__::out_labels(&["knows"]))
        .times(1)
        .to_list();
    println!(
        "repeat(out('knows')).times(1) from [Alice, Bob]: [{}]",
        display_vertices(&multi_start, &storage)
    );
    println!("  ^ Alice->Bob, Bob->Charlie");

    // With emit and dedup to get unique vertices
    let multi_start_dedup = g
        .v_ids([vertices.alice, vertices.bob])
        .repeat(__::out_labels(&["knows"]))
        .times(2)
        .emit()
        .dedup()
        .to_list();
    println!(
        "repeat(out('knows')).times(2).emit().dedup() from [Alice, Bob]: [{}]",
        display_vertices(&multi_start_dedup, &storage)
    );
    println!();

    // -------------------------------------------------------------------------
    // Using repeat() with other steps
    // -------------------------------------------------------------------------
    println!("--- Using repeat() with other steps ---");

    // Get names of all people within 2 hops
    let names = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(2)
        .emit()
        .values("name")
        .to_list();
    println!(
        "Names within 2 hops of Alice: [{}]",
        display_vertices(&names, &storage)
    );

    // Count vertices at each hop level (using intermediate collection)
    let count = g
        .v_ids([vertices.alice])
        .repeat(__::out_labels(&["knows"]))
        .times(3)
        .emit()
        .count();
    println!("Count of vertices within 3 hops: {}", count);

    // Filter after repeat
    let filtered = g
        .v_ids([vertices.alice])
        .repeat(__::out())
        .times(3)
        .emit()
        .has_label("person")
        .to_list();
    println!(
        "People within 3 hops of Alice: [{}]",
        display_vertices(&filtered, &storage)
    );
    println!();

    println!("=== Example Complete ===");
}

/// Vertex IDs for easy reference
struct VertexIds {
    alice: VertexId,
    bob: VertexId,
    #[allow(dead_code)]
    charlie: VertexId,
    dave: VertexId,
    tech_corp: VertexId,
    #[allow(dead_code)]
    startup_inc: VertexId,
    #[allow(dead_code)]
    python: VertexId,
    #[allow(dead_code)]
    rust: VertexId,
}

/// Create a test graph for demonstrating repeat steps.
///
/// Graph structure:
/// ```text
///   Alice --knows--> Bob --knows--> Charlie --knows--> Dave
///     |                |                |                |
///     |                |                |                +--works_at--> StartupInc
///     |                |                +--works_at--> TechCorp
///     |                +--uses--> Rust
///     +--uses--> Python
/// ```
///
/// This provides:
/// - A linear chain (Alice -> Bob -> Charlie -> Dave) for times(n) demos
/// - Different vertex labels (person, software, company) for until/emit_if
/// - Multiple edge types (knows, uses, works_at) for filtering
/// - Leaf nodes (TechCorp, StartupInc) for exhaustion behavior
fn create_test_graph() -> (Graph, Arc<InMemoryGraph>, VertexIds) {
    let mut storage = InMemoryGraph::new();

    // Person vertices
    let alice = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

    let bob = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(28));
        props
    });

    let charlie = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props
    });

    let dave = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Dave".to_string()));
        props.insert("age".to_string(), Value::Int(40));
        props
    });

    // Company vertices
    let tech_corp = storage.add_vertex("company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("TechCorp".to_string()));
        props.insert("size".to_string(), Value::Int(1000));
        props
    });

    let startup_inc = storage.add_vertex("company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("StartupInc".to_string()));
        props.insert("size".to_string(), Value::Int(50));
        props
    });

    // Software vertices
    let python = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Python".to_string()));
        props.insert("version".to_string(), Value::String("3.12".to_string()));
        props
    });

    let rust = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Rust".to_string()));
        props.insert("version".to_string(), Value::String("1.75".to_string()));
        props
    });

    // Create 'knows' edges: Alice -> Bob -> Charlie -> Dave
    storage
        .add_edge(alice, bob, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, dave, "knows", HashMap::new())
        .unwrap();

    // Create 'works_at' edges
    storage
        .add_edge(charlie, tech_corp, "works_at", HashMap::new())
        .unwrap();
    storage
        .add_edge(dave, startup_inc, "works_at", HashMap::new())
        .unwrap();

    // Create 'uses' edges
    storage
        .add_edge(alice, python, "uses", HashMap::new())
        .unwrap();
    storage.add_edge(bob, rust, "uses", HashMap::new()).unwrap();

    let storage = Arc::new(storage);
    let graph = Graph::new(storage.clone());

    (
        graph,
        storage,
        VertexIds {
            alice,
            bob,
            charlie,
            dave,
            tech_corp,
            startup_inc,
            python,
            rust,
        },
    )
}
