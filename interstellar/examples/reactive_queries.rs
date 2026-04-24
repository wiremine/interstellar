//! Reactive streaming queries example.
//!
//! Demonstrates subscribing to graph traversal patterns and receiving
//! live events as graph mutations match or stop matching.
//!
//! Run with:
//!   cargo run --example reactive_queries --features reactive

#[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
fn main() {
    use interstellar::prelude::*;
    use interstellar::storage::events::GraphEvent;
    use interstellar::traversal::p;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Extract a human-readable description from a subscription event.
    fn describe_event(event: &SubscriptionEvent, names: &HashMap<VertexId, String>) -> String {
        match &event.source_event {
            GraphEvent::VertexAdded {
                label, properties, ..
            } => {
                let name = properties
                    .get("name")
                    .and_then(|v| match v {
                        Value::String(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .unwrap_or("?");
                format!("{name} ({label})")
            }
            GraphEvent::VertexRemoved { id, label } => {
                let name = names.get(id).map(|s| s.as_str()).unwrap_or("?");
                format!("{name} ({label})")
            }
            other => format!("{other:?}"),
        }
    }

    println!("=== Reactive Streaming Queries Demo ===\n");

    // Create a graph
    let graph = Arc::new(Graph::new());

    // Take a snapshot and subscribe to a traversal pattern
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Subscribe to "person" vertices where age > 30
    let sub = g
        .v()
        .has_label("person")
        .has_where("age", p::gt(30i64))
        .subscribe();

    println!("Subscribed to g.V().hasLabel('person').has('age', gt(30))\n");

    // Brief pause to let the dispatcher thread start and register the subscription
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Spawn a thread to consume events
    let consumer = std::thread::spawn(move || {
        let mut count = 0;
        let mut names: HashMap<VertexId, String> = HashMap::new();
        while let Some(event) = sub.recv() {
            count += 1;

            // Track vertex names from add events so removals are descriptive
            if let GraphEvent::VertexAdded { id, properties, .. } = &event.source_event {
                if let Some(Value::String(name)) = properties.get("name") {
                    names.insert(*id, name.clone());
                }
            }

            let desc = describe_event(&event, &names);
            match event.event_type {
                SubscriptionEventType::Added => {
                    println!("  [Event {}] ADDED: {}", count, desc);
                }
                SubscriptionEventType::Removed => {
                    println!("  [Event {}] REMOVED: {}", count, desc);
                }
                SubscriptionEventType::Updated => {
                    println!("  [Event {}] UPDATED: {}", count, desc);
                }
            }
            if count >= 3 {
                break;
            }
        }
        count
    });

    // Alice (age 35) — matches the predicate
    println!("Adding Alice (person, age=35)...");
    let alice = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::from("Alice")),
            ("age".to_string(), Value::from(35i64)),
        ]),
    );

    // Bob (age 28) — does NOT match age > 30
    println!("Adding Bob (person, age=28) — should NOT trigger event...");
    let _bob = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::from("Bob")),
            ("age".to_string(), Value::from(28i64)),
        ]),
    );

    // Company vertex — wrong label, should not match
    println!("Adding Acme (company) — should NOT trigger event...");
    graph.add_vertex(
        "company",
        HashMap::from([("name".to_string(), Value::from("Acme"))]),
    );

    // Carol (age 42) — matches
    println!("Adding Carol (person, age=42)...");
    let _carol = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::from("Carol")),
            ("age".to_string(), Value::from(42i64)),
        ]),
    );

    // Remove Alice
    println!("Removing Alice...");
    graph.remove_vertex(alice).unwrap();

    // Wait for consumer
    let event_count = consumer.join().unwrap();
    println!("\nReceived {} events total", event_count);
    println!("\n=== Done ===");
}

#[cfg(not(all(feature = "reactive", not(target_arch = "wasm32"))))]
fn main() {
    eprintln!("This example requires the 'reactive' feature.");
    eprintln!("Run with: cargo run --example reactive_queries --features reactive");
}
