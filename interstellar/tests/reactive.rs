//! Integration tests for reactive streaming queries (spec-52).
//!
//! Tests the end-to-end flow: subscribe to a traversal → mutate the graph
//! → receive subscription events.

#![cfg(all(feature = "reactive", not(target_arch = "wasm32")))]

use std::collections::HashMap;
use std::sync::Arc;

use interstellar::prelude::*;
use interstellar::traversal::predicate as p;

/// Helper: create a Graph wrapped in Arc for test use.
fn test_graph() -> Arc<Graph> {
    Arc::new(Graph::new())
}

// =============================================================================
// Basic subscription lifecycle
// =============================================================================

#[test]
fn test_subscribe_receives_matching_vertex_added() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g.v().has_label("person").subscribe();

    graph.add_vertex("person", HashMap::from([("name".to_string(), Value::from("Alice"))]));

    let event = sub.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);
    assert_eq!(event.values.len(), 1);

    sub.cancel();
}

#[test]
fn test_subscribe_ignores_non_matching_vertex() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g.v().has_label("person").subscribe();

    // Add non-matching vertex
    graph.add_vertex("company", HashMap::new());

    // Add matching vertex to flush
    graph.add_vertex("person", HashMap::new());

    let event = sub.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);

    // No more events
    assert!(sub.try_recv().is_err());

    sub.cancel();
}

#[test]
fn test_subscribe_detects_vertex_removal() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g.v().has_label("person").subscribe();

    let id = graph.add_vertex("person", HashMap::new());

    let event = sub.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);

    graph.remove_vertex(id).unwrap();

    let event = sub.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Removed);

    sub.cancel();
}

// =============================================================================
// Property-based subscriptions
// =============================================================================

#[test]
fn test_subscribe_with_has_where_property_filter() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g
        .v()
        .has_label("person")
        .has_where("age", p::p::gt(30))
        .subscribe();

    // Add vertex that doesn't match the property filter
    graph.add_vertex(
        "person",
        HashMap::from([("age".to_string(), Value::from(25i64))]),
    );

    // Add vertex that matches
    let _id = graph.add_vertex(
        "person",
        HashMap::from([("age".to_string(), Value::from(35i64))]),
    );

    let event = sub.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);
    assert_eq!(event.values.len(), 1);

    sub.cancel();
}

// =============================================================================
// Multiple subscriptions
// =============================================================================

#[test]
fn test_multiple_independent_subscriptions() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub_person = g.v().has_label("person").subscribe();
    let sub_company = g.v().has_label("company").subscribe();

    graph.add_vertex("person", HashMap::new());

    // sub_person should get the event
    let event = sub_person.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);

    graph.add_vertex("company", HashMap::new());

    // sub_company should get its event
    let event = sub_company.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);

    sub_person.cancel();
    sub_company.cancel();
}

// =============================================================================
// Backpressure
// =============================================================================

#[test]
fn test_backpressure_drops_events_without_blocking() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g
        .v()
        .has_label("person")
        .subscribe_with(SubscribeOptions::new().capacity(1));

    // Flood with mutations
    for i in 0..100 {
        graph.add_vertex(
            "person",
            HashMap::from([("i".to_string(), Value::from(i as i64))]),
        );
    }

    // Should be able to receive at least one event without deadlock
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(sub.recv().is_some());

    sub.cancel();
}

// =============================================================================
// Subscription cancellation
// =============================================================================

#[test]
fn test_cancel_stops_receiving_events() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g.v().has_label("person").subscribe();
    sub.cancel();

    // Mutations after cancel shouldn't cause issues
    graph.add_vertex("person", HashMap::new());

    // Give dispatcher time
    std::thread::sleep(std::time::Duration::from_millis(50));
}

#[test]
fn test_drop_cancels_subscription() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    {
        let _sub = g.v().has_label("person").subscribe();
        // _sub dropped here
    }

    // Should not panic or deadlock
    graph.add_vertex("person", HashMap::new());
    std::thread::sleep(std::time::Duration::from_millis(50));
}

// =============================================================================
// Iterator interface
// =============================================================================

#[test]
fn test_subscription_iterator() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let mut sub = g.v().has_label("person").subscribe();

    graph.add_vertex("person", HashMap::new());
    graph.add_vertex("person", HashMap::new());

    // Use Iterator::next()
    let event = sub.next().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);

    sub.cancel();
}

// =============================================================================
// Batch mutations
// =============================================================================

#[test]
fn test_batch_mutation_events() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g.v().has_label("person").subscribe();

    // Batch add
    let _ = graph.batch(|ctx| {
        ctx.add_vertex("person", HashMap::new());
        ctx.add_vertex("person", HashMap::new());
        ctx.add_vertex("company", HashMap::new()); // non-matching
        Ok(())
    });

    // Should receive an Added event with the two person vertices
    let event = sub.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);

    sub.cancel();
}

// =============================================================================
// Edge subscriptions
// =============================================================================

#[test]
fn test_edge_subscription() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g.e().has_label("knows").subscribe();

    let v1 = graph.add_vertex("person", HashMap::new());
    let v2 = graph.add_vertex("person", HashMap::new());
    let _ = graph.add_edge(v1, v2, "knows", HashMap::new());

    let event = sub.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);

    sub.cancel();
}

// =============================================================================
// into_receiver
// =============================================================================

#[test]
fn test_into_receiver() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g.v().has_label("person").subscribe();
    let rx = sub.into_receiver();

    graph.add_vertex("person", HashMap::new());

    let event = rx.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);
}

// =============================================================================
// include_initial
// =============================================================================

#[test]
fn test_include_initial_emits_existing_matches() {
    let graph = test_graph();

    // Add vertices BEFORE subscribing
    graph.add_vertex("person", HashMap::from([("name".to_string(), Value::from("Alice"))]));
    graph.add_vertex("person", HashMap::from([("name".to_string(), Value::from("Bob"))]));
    graph.add_vertex("company", HashMap::from([("name".to_string(), Value::from("Acme"))]));

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g
        .v()
        .has_label("person")
        .subscribe_with(SubscribeOptions::new().include_initial(true));

    // Should immediately receive the two existing person vertices
    let event = sub.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);
    assert_eq!(event.values.len(), 2);

    // New additions still work after initial
    graph.add_vertex("person", HashMap::from([("name".to_string(), Value::from("Carol"))]));

    let event2 = sub.recv().unwrap();
    assert_eq!(event2.event_type, SubscriptionEventType::Added);
    assert_eq!(event2.values.len(), 1);

    sub.cancel();
}

#[test]
fn test_include_initial_empty_graph() {
    let graph = test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g
        .v()
        .has_label("person")
        .subscribe_with(SubscribeOptions::new().include_initial(true));

    // No initial events — graph is empty
    // Adding a vertex should still work
    graph.add_vertex("person", HashMap::new());

    let event = sub.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);
    assert_eq!(event.values.len(), 1);

    sub.cancel();
}

#[test]
fn test_include_initial_does_not_duplicate_on_mutation() {
    let graph = test_graph();

    // Pre-existing vertex
    graph.add_vertex("person", HashMap::from([("name".to_string(), Value::from("Alice"))]));

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    let sub = g
        .v()
        .has_label("person")
        .subscribe_with(SubscribeOptions::new().include_initial(true));

    // Initial event
    let event = sub.recv().unwrap();
    assert_eq!(event.event_type, SubscriptionEventType::Added);
    assert_eq!(event.values.len(), 1);

    // Mutate a property on the existing vertex — should emit Updated, not Added again
    let id = match &event.values[0] {
        Value::Vertex(vid) => *vid,
        _ => panic!("expected vertex"),
    };
    graph.set_vertex_property(id, "age", Value::from(30));

    let event2 = sub.recv().unwrap();
    assert_eq!(event2.event_type, SubscriptionEventType::Updated);

    sub.cancel();
}
