//! Integration tests for property indexes with traversal.
//!
//! These tests verify that property indexes work correctly with the
//! GraphStorage trait methods and can be accessed via traversal context.

use std::collections::HashMap;
use std::ops::Bound;

use interstellar::index::IndexBuilder;
use interstellar::storage::{Graph, GraphStorage, InMemoryGraph};
use interstellar::value::{Value, VertexId};

// =============================================================================
// GraphStorage Trait Method Tests
// =============================================================================

#[test]
fn supports_indexes_returns_true_for_inmemory() {
    let storage = InMemoryGraph::new();
    assert!(storage.supports_indexes());
}

#[test]
fn vertices_by_property_uses_index_for_equality() {
    let mut graph = InMemoryGraph::new();

    // Add vertices with age property
    for age in 20..30 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String(format!("Person{}", age)));
        props.insert("age".to_string(), Value::Int(age));
        graph.add_vertex("person", props);
    }

    // Create index on age property
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Use GraphStorage trait method to find vertices with age=25
    let results: Vec<_> = graph
        .vertices_by_property(Some("person"), "age", &Value::Int(25))
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].properties.get("age"), Some(&Value::Int(25)));
}

#[test]
fn vertices_by_property_without_index_falls_back_to_scan() {
    let mut graph = InMemoryGraph::new();

    // Add vertices with age property (no index)
    for age in 20..30 {
        let mut props = HashMap::new();
        props.insert("age".to_string(), Value::Int(age));
        graph.add_vertex("person", props);
    }

    // No index created - should still work via fallback scan
    let results: Vec<_> = graph
        .vertices_by_property(Some("person"), "age", &Value::Int(25))
        .collect();

    assert_eq!(results.len(), 1);
}

#[test]
fn edges_by_property_uses_index() {
    let mut graph = InMemoryGraph::new();

    // Add vertices
    let v1 = graph.add_vertex("person", HashMap::new());
    let v2 = graph.add_vertex("person", HashMap::new());
    let v3 = graph.add_vertex("person", HashMap::new());

    // Add edges with weight property
    for weight in 1..=5 {
        let mut props = HashMap::new();
        props.insert("weight".to_string(), Value::Int(weight));
        graph.add_edge(v1, v2, "knows", props.clone()).unwrap();
        graph.add_edge(v2, v3, "knows", props).unwrap();
    }

    // Create index on weight property
    graph
        .create_index(
            IndexBuilder::edge()
                .label("knows")
                .property("weight")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Find edges with weight=3
    let results: Vec<_> = graph
        .edges_by_property(Some("knows"), "weight", &Value::Int(3))
        .collect();

    assert_eq!(results.len(), 2);
    for edge in &results {
        assert_eq!(edge.properties.get("weight"), Some(&Value::Int(3)));
    }
}

#[test]
fn vertices_by_property_range_uses_btree_index() {
    let mut graph = InMemoryGraph::new();

    // Add vertices with age property
    for age in 18..65 {
        let mut props = HashMap::new();
        props.insert("age".to_string(), Value::Int(age));
        graph.add_vertex("person", props);
    }

    // Create BTree index for range queries
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Find people aged 30-39 (inclusive start, exclusive end)
    let results: Vec<_> = graph
        .vertices_by_property_range(
            Some("person"),
            "age",
            Bound::Included(&Value::Int(30)),
            Bound::Excluded(&Value::Int(40)),
        )
        .collect();

    assert_eq!(results.len(), 10); // ages 30, 31, 32, ..., 39

    for vertex in &results {
        let age = vertex.properties.get("age").unwrap().as_i64().unwrap();
        assert!(age >= 30 && age < 40);
    }
}

#[test]
fn vertices_by_property_range_without_index_falls_back_to_scan() {
    let mut graph = InMemoryGraph::new();

    // Add vertices with age property (no index)
    for age in 18..65 {
        let mut props = HashMap::new();
        props.insert("age".to_string(), Value::Int(age));
        graph.add_vertex("person", props);
    }

    // No index - should fall back to full scan
    let results: Vec<_> = graph
        .vertices_by_property_range(
            Some("person"),
            "age",
            Bound::Included(&Value::Int(30)),
            Bound::Excluded(&Value::Int(40)),
        )
        .collect();

    assert_eq!(results.len(), 10);
}

// =============================================================================
// Traversal Context Access Tests
// =============================================================================

#[test]
fn traversal_can_access_indexed_storage_methods() {
    // Build graph with COW Graph which supports indexes directly
    let graph = Graph::new();

    for age in 20..30 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String(format!("Person{}", age)));
        props.insert("age".to_string(), Value::Int(age));
        graph.add_vertex("person", props);
    }

    // Create index
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // The Graph itself supports indexes
    assert!(graph.supports_indexes());

    // Use the indexed lookup via the graph
    let results: Vec<_> = graph
        .vertices_by_property(Some("person"), "age", &Value::Int(25))
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].properties.get("name"),
        Some(&Value::String("Person25".to_string()))
    );
}

#[test]
fn index_maintained_with_graph() {
    let graph = Graph::new();

    // Create unique index first
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("user")
                .property("email")
                .unique()
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add users with unique emails
    let mut props = HashMap::new();
    props.insert(
        "email".to_string(),
        Value::String("alice@example.com".into()),
    );
    graph.add_vertex("user", props);

    let mut props = HashMap::new();
    props.insert("email".to_string(), Value::String("bob@example.com".into()));
    graph.add_vertex("user", props);

    // Verify index lookup works through Graph snapshot
    let snapshot = graph.snapshot();

    let results: Vec<_> = snapshot
        .vertices_by_property(
            Some("user"),
            "email",
            &Value::String("alice@example.com".into()),
        )
        .collect();

    assert_eq!(results.len(), 1);
}

// =============================================================================
// Range Query Bounds Tests
// =============================================================================

#[test]
fn range_query_unbounded_start() {
    let mut graph = InMemoryGraph::new();

    for value in 1..=10 {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(value));
        graph.add_vertex("item", props);
    }

    graph
        .create_index(
            IndexBuilder::vertex()
                .label("item")
                .property("value")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Get all values < 5
    let results: Vec<_> = graph
        .vertices_by_property_range(
            Some("item"),
            "value",
            Bound::Unbounded,
            Bound::Excluded(&Value::Int(5)),
        )
        .collect();

    assert_eq!(results.len(), 4); // 1, 2, 3, 4
}

#[test]
fn range_query_unbounded_end() {
    let mut graph = InMemoryGraph::new();

    for value in 1..=10 {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(value));
        graph.add_vertex("item", props);
    }

    graph
        .create_index(
            IndexBuilder::vertex()
                .label("item")
                .property("value")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Get all values >= 7
    let results: Vec<_> = graph
        .vertices_by_property_range(
            Some("item"),
            "value",
            Bound::Included(&Value::Int(7)),
            Bound::Unbounded,
        )
        .collect();

    assert_eq!(results.len(), 4); // 7, 8, 9, 10
}

#[test]
fn range_query_excluded_bounds() {
    let mut graph = InMemoryGraph::new();

    for value in 1..=10 {
        let mut props = HashMap::new();
        props.insert("value".to_string(), Value::Int(value));
        graph.add_vertex("item", props);
    }

    graph
        .create_index(
            IndexBuilder::vertex()
                .label("item")
                .property("value")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Get values where 3 < value < 8
    let results: Vec<_> = graph
        .vertices_by_property_range(
            Some("item"),
            "value",
            Bound::Excluded(&Value::Int(3)),
            Bound::Excluded(&Value::Int(8)),
        )
        .collect();

    assert_eq!(results.len(), 4); // 4, 5, 6, 7
}

// =============================================================================
// Label Filter Tests
// =============================================================================

#[test]
fn property_lookup_respects_label_filter() {
    let mut graph = InMemoryGraph::new();

    // Add people and robots with same age property
    for age in 20..25 {
        let mut props = HashMap::new();
        props.insert("age".to_string(), Value::Int(age));
        graph.add_vertex("person", props.clone());
        graph.add_vertex("robot", props);
    }

    // Create index only on person label
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Search for age=22 with person label
    let people: Vec<_> = graph
        .vertices_by_property(Some("person"), "age", &Value::Int(22))
        .collect();
    assert_eq!(people.len(), 1);
    assert_eq!(people[0].label, "person");

    // Search for age=22 with robot label (no index, falls back to scan)
    let robots: Vec<_> = graph
        .vertices_by_property(Some("robot"), "age", &Value::Int(22))
        .collect();
    assert_eq!(robots.len(), 1);
    assert_eq!(robots[0].label, "robot");
}

#[test]
fn property_lookup_without_label_uses_all_labels_index() {
    let mut graph = InMemoryGraph::new();

    // Add items with mixed labels
    for i in 1..=5 {
        let mut props = HashMap::new();
        props.insert("priority".to_string(), Value::Int(i));
        graph.add_vertex("task", props.clone());
        graph.add_vertex("bug", props);
    }

    // Create index without label filter (covers all vertices)
    graph
        .create_index(IndexBuilder::vertex().property("priority").build().unwrap())
        .unwrap();

    // Search without label filter
    let results: Vec<_> = graph
        .vertices_by_property(None, "priority", &Value::Int(3))
        .collect();

    // Should find both task and bug with priority=3
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Unique Index Integration Tests
// =============================================================================

#[test]
fn unique_index_provides_fast_single_lookup() {
    let mut graph = InMemoryGraph::new();

    // Create unique index first
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("user")
                .property("email")
                .unique()
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add users with unique emails
    for i in 1..=100 {
        let mut props = HashMap::new();
        props.insert(
            "email".to_string(),
            Value::String(format!("user{}@example.com", i)),
        );
        props.insert("name".to_string(), Value::String(format!("User {}", i)));
        graph.add_vertex("user", props);
    }

    // Lookup by email
    let results: Vec<_> = graph
        .vertices_by_property(
            Some("user"),
            "email",
            &Value::String("user50@example.com".into()),
        )
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].properties.get("name"),
        Some(&Value::String("User 50".into()))
    );
}

// =============================================================================
// Performance Verification Tests (Sanity Checks)
// =============================================================================

#[test]
fn indexed_lookup_on_large_graph() {
    let mut graph = InMemoryGraph::new();

    // Create 10,000 vertices
    for i in 0..10_000 {
        let mut props = HashMap::new();
        props.insert("index".to_string(), Value::Int(i));
        props.insert(
            "category".to_string(),
            Value::String(format!("cat{}", i % 100)),
        );
        graph.add_vertex("item", props);
    }

    // Create index
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("item")
                .property("index")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Indexed lookup should be fast
    let start = std::time::Instant::now();
    for target in [0, 5000, 9999] {
        let results: Vec<_> = graph
            .vertices_by_property(Some("item"), "index", &Value::Int(target))
            .collect();
        assert_eq!(results.len(), 1);
    }
    let elapsed = start.elapsed();

    // Should complete in under 100ms (generous threshold for CI)
    assert!(
        elapsed.as_millis() < 100,
        "Indexed lookup took too long: {:?}",
        elapsed
    );
}

#[test]
fn range_query_on_large_graph() {
    let mut graph = InMemoryGraph::new();

    // Create 10,000 vertices with timestamps
    for i in 0..10_000i64 {
        let mut props = HashMap::new();
        props.insert("timestamp".to_string(), Value::Int(i));
        graph.add_vertex("event", props);
    }

    // Create BTree index for range queries
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("event")
                .property("timestamp")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Range query should be efficient
    let start = std::time::Instant::now();
    let results: Vec<_> = graph
        .vertices_by_property_range(
            Some("event"),
            "timestamp",
            Bound::Included(&Value::Int(1000)),
            Bound::Excluded(&Value::Int(2000)),
        )
        .collect();
    let elapsed = start.elapsed();

    assert_eq!(results.len(), 1000);
    assert!(
        elapsed.as_millis() < 100,
        "Range query took too long: {:?}",
        elapsed
    );
}

// =============================================================================
// Traversal API Integration with Indexes
// =============================================================================

#[test]
fn traversal_v_by_property_uses_index() {
    let graph = Graph::new();

    // Create index first for best performance
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("person")
                .property("name")
                .unique()
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add vertices
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice".into()));
    props.insert("age".to_string(), Value::Int(30));
    graph.add_vertex("person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Bob".into()));
    props.insert("age".to_string(), Value::Int(25));
    graph.add_vertex("person", props);

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Use traversal API with indexed lookup
    let results = g
        .v_by_property(Some("person"), "name", "Alice")
        .values("age")
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(30));
}

#[test]
fn traversal_v_by_property_range_uses_index() {
    let graph = Graph::new();

    // Create BTree index for range queries
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("person")
                .property("age")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add vertices with various ages
    for (name, age) in [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 40)] {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String(name.into()));
        props.insert("age".to_string(), Value::Int(age));
        graph.add_vertex("person", props);
    }

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Range query: ages 28-36
    let results = g
        .v_by_property_range(
            Some("person"),
            "age",
            Bound::Included(&Value::Int(28)),
            Bound::Included(&Value::Int(36)),
        )
        .values("name")
        .to_list();

    // Should find Bob (30) and Charlie (35)
    assert_eq!(results.len(), 2);
    let names: Vec<_> = results.iter().filter_map(|v| v.as_str()).collect();
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Charlie"));
}

#[test]
fn traversal_e_by_property_uses_index() {
    let graph = Graph::new();

    // Create edge index
    graph
        .create_index(
            IndexBuilder::edge()
                .label("knows")
                .property("since")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add vertices
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice".into()));
    let alice = graph.add_vertex("person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Bob".into()));
    let bob = graph.add_vertex("person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Charlie".into()));
    let charlie = graph.add_vertex("person", props);

    // Add edges with "since" property
    let mut edge_props = HashMap::new();
    edge_props.insert("since".to_string(), Value::Int(2020));
    graph.add_edge(alice, bob, "knows", edge_props).unwrap();

    let mut edge_props = HashMap::new();
    edge_props.insert("since".to_string(), Value::Int(2022));
    graph.add_edge(bob, charlie, "knows", edge_props).unwrap();

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Find edges from 2020
    let results = g
        .e_by_property(Some("knows"), "since", 2020i64)
        .in_v()
        .values("name")
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".into()));
}

#[test]
fn traversal_with_index_chains_complex_queries() {
    let graph = Graph::new();

    // Create indexes
    graph
        .create_index(
            IndexBuilder::vertex()
                .label("person")
                .property("department")
                .build()
                .unwrap(),
        )
        .unwrap();

    // Add employees
    let departments = [
        "Engineering",
        "Marketing",
        "Engineering",
        "Sales",
        "Engineering",
    ];
    let names = ["Alice", "Bob", "Charlie", "Diana", "Eve"];
    let ages = [30, 25, 35, 28, 32];

    for (i, ((name, dept), age)) in names
        .iter()
        .zip(departments.iter())
        .zip(ages.iter())
        .enumerate()
    {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String((*name).into()));
        props.insert("department".to_string(), Value::String((*dept).into()));
        props.insert("age".to_string(), Value::Int(*age));
        let id = graph.add_vertex("person", props);

        // Add some edges between consecutive employees
        if i > 0 {
            let prev = VertexId((i - 1) as u64);
            graph
                .add_edge(prev, id, "works_with", HashMap::new())
                .unwrap();
        }
    }

    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Complex query: Find Engineering people, get their coworkers, filter by age > 30
    let results = g
        .v_by_property(Some("person"), "department", "Engineering")
        .out_labels(&["works_with"])
        .has_where("age", interstellar::traversal::p::gt(30))
        .values("name")
        .dedup()
        .to_list();

    // Alice (30) -> Bob, Charlie (35) -> Diana, Eve (32) works_with nobody after
    // Bob (25) is not > 30, Diana (28) is not > 30
    // So only Charlie's outgoing edge to Diana and Eve's to nobody
    // Actually: Alice -> Bob (25, fails), Charlie -> Diana (28, fails), Eve -> nobody
    // Wait, edges are: 0->1, 1->2, 2->3, 3->4
    // Engineering people: Alice(0), Charlie(2), Eve(4)
    // Alice(0).out = Bob(1) - age 25, fails
    // Charlie(2).out = Diana(3) - age 28, fails
    // Eve(4).out = nobody
    // So empty result
    assert!(results.is_empty());

    // Let's try a simpler query: just count Engineering employees
    let count = g
        .v_by_property(Some("person"), "department", "Engineering")
        .count();

    assert_eq!(count, 3); // Alice, Charlie, Eve
}
