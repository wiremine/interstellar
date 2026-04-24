//! Reactive geospatial queries example.
//!
//! Demonstrates combining reactive streaming subscriptions with geospatial
//! predicates: subscribe to vertices that appear within a geographic radius,
//! then receive live events as cities are added, moved, or removed.
//!
//! Run with:
//!   cargo run --example reactive_geo --features reactive

#[cfg(all(feature = "reactive", not(target_arch = "wasm32")))]
fn main() {
    use interstellar::geo::{Distance, Point};
    use interstellar::index::IndexBuilder;
    use interstellar::prelude::*;
    use interstellar::storage::events::GraphEvent;
    use interstellar::traversal::p;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn props(name: &str, lon: f64, lat: f64) -> HashMap<String, Value> {
        HashMap::from([
            ("name".into(), Value::String(name.into())),
            (
                "location".into(),
                Value::Point(Point::new(lon, lat).unwrap()),
            ),
        ])
    }

    fn describe(event: &SubscriptionEvent, names: &HashMap<VertexId, String>) -> String {
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
                let loc = properties
                    .get("location")
                    .and_then(|v| match v {
                        Value::Point(p) => Some(format!("({:.4}, {:.4})", p.lat, p.lon)),
                        _ => None,
                    })
                    .unwrap_or_default();
                format!("{name} ({label}) at {loc}")
            }
            GraphEvent::VertexRemoved { id, label } => {
                let name = names.get(id).map(|s| s.as_str()).unwrap_or("?");
                format!("{name} ({label})")
            }
            GraphEvent::VertexPropertyChanged { id, key, .. } => {
                let name = names.get(id).map(|s| s.as_str()).unwrap_or("?");
                format!("{name} property '{key}' changed")
            }
            other => format!("{other:?}"),
        }
    }

    println!("=== Reactive Geospatial Queries Demo ===\n");

    // -------------------------------------------------------------------------
    // 1. Set up graph with an R-tree spatial index on city locations
    // -------------------------------------------------------------------------
    let graph = Arc::new(Graph::new());

    let spec = IndexBuilder::vertex()
        .label("city")
        .property("location")
        .rtree()
        .build()
        .expect("valid index spec");
    graph.create_index(spec).unwrap();

    println!("Created R-tree index on city.location\n");

    // -------------------------------------------------------------------------
    // 2. Subscribe: cities within 500 km of Paris (48.8566 N, 2.3522 E)
    // -------------------------------------------------------------------------
    let paris = Point::new(2.3522, 48.8566).unwrap();
    let radius = Distance::Kilometers(500.0);

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g
        .v()
        .has_label("city")
        .has_where("location", p::within_distance(paris, radius))
        .subscribe();

    println!(
        "Subscribed to: g.V().hasLabel('city')\n\
         \x20 .has('location', within_distance(Paris, 500km))\n"
    );

    // Brief pause to let the dispatcher register the subscription
    std::thread::sleep(std::time::Duration::from_millis(50));

    // -------------------------------------------------------------------------
    // 3. Consume events on a background thread
    // -------------------------------------------------------------------------
    let expected_events = 4;
    let consumer = std::thread::spawn(move || {
        let mut count = 0;
        let mut names: HashMap<VertexId, String> = HashMap::new();
        while let Some(event) = sub.recv() {
            count += 1;
            if let GraphEvent::VertexAdded { id, properties, .. } = &event.source_event {
                if let Some(Value::String(name)) = properties.get("name") {
                    names.insert(*id, name.clone());
                }
            }
            let desc = describe(&event, &names);
            match event.event_type {
                SubscriptionEventType::Added => println!("  [{count}] ADDED:   {desc}"),
                SubscriptionEventType::Removed => println!("  [{count}] REMOVED: {desc}"),
                SubscriptionEventType::Updated => println!("  [{count}] UPDATED: {desc}"),
            }
            if count >= expected_events {
                break;
            }
        }
        count
    });

    // Small delay helper to let the dispatcher process events between mutations
    let pause = || std::thread::sleep(std::time::Duration::from_millis(10));

    // -------------------------------------------------------------------------
    // 4. Mutate the graph — watch events flow
    // -------------------------------------------------------------------------

    // London: ~340 km from Paris — MATCH
    println!("Adding London (~340 km from Paris)...");
    graph.add_vertex("city", props("London", -0.1278, 51.5074));
    pause();

    // Brussels: ~265 km from Paris — MATCH
    println!("Adding Brussels (~265 km from Paris)...");
    graph.add_vertex("city", props("Brussels", 4.3517, 50.8503));
    pause();

    // Madrid: ~1050 km from Paris — NO MATCH
    println!("Adding Madrid (~1050 km from Paris) — should NOT trigger...");
    graph.add_vertex("city", props("Madrid", -3.7038, 40.4168));
    pause();

    // Berlin: ~878 km from Paris — NO MATCH
    println!("Adding Berlin (~878 km from Paris) — should NOT trigger...");
    graph.add_vertex("city", props("Berlin", 13.4050, 52.5200));
    pause();

    // Amsterdam: ~430 km from Paris — MATCH
    println!("Adding Amsterdam (~430 km from Paris)...");
    let amsterdam = graph.add_vertex("city", props("Amsterdam", 4.9041, 52.3676));
    pause();

    // Remove Amsterdam
    println!("Removing Amsterdam...");
    graph.remove_vertex(amsterdam).unwrap();

    // -------------------------------------------------------------------------
    // 5. Wait for consumer and summarise
    // -------------------------------------------------------------------------
    let event_count = consumer.join().unwrap();
    println!("\nReceived {event_count} events total");
    println!("\n=== Done ===");
}

#[cfg(not(all(feature = "reactive", not(target_arch = "wasm32"))))]
fn main() {
    eprintln!("This example requires the 'reactive' feature.");
    eprintln!("Run with: cargo run --example reactive_geo --features reactive");
}
