# Testing Guide

Best practices for testing graph-based applications with Interstellar.

## Test Setup

### In-Memory Graphs for Tests

Always use `Graph` for tests - it's fast and isolated:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use interstellar::prelude::*;

    fn create_test_graph() -> Graph {
        Graph::new()
    }

    #[test]
    fn test_basic_traversal() {
        let graph = create_test_graph();
        // ... test code
    }
}
```

### Fixture Functions

Create reusable fixtures for common test data:

```rust
fn create_social_network() -> (Graph, VertexId, VertexId, VertexId) {
    let graph = Graph::new();
    
    let alice = graph.add_vertex("person", props! {
        "name" => "Alice",
        "age" => 30i64
    });
    
    let bob = graph.add_vertex("person", props! {
        "name" => "Bob",
        "age" => 25i64
    });
    
    let carol = graph.add_vertex("person", props! {
        "name" => "Carol",
        "age" => 35i64
    });
    
    graph.add_edge(alice, bob, "knows", props! {}).unwrap();
    graph.add_edge(bob, carol, "knows", props! {}).unwrap();
    
    (graph, alice, bob, carol)
}

#[test]
fn test_friend_of_friend() {
    let (graph, alice, _, carol) = create_social_network();
    let snap = graph.snapshot();
    let g = snap.traversal();
    
    let fof = g.v_ids([alice])
        .out("knows")
        .out("knows")
        .to_list();
    
    assert_eq!(fof.len(), 1);
    // Carol is friend-of-friend of Alice
}
```

---

## Testing Queries

### Test Traversal Results

```rust
#[test]
fn test_filter_by_age() {
    let (graph, _, _, _) = create_social_network();
    let snap = graph.snapshot();
    let g = snap.traversal();
    
    let adults = g.v()
        .has_label("person")
        .has_where("age", p::gte(30))
        .values("name")
        .to_list();
    
    // Convert to strings for easier assertion
    let names: Vec<String> = adults
        .into_iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        })
        .collect();
    
    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Carol".to_string()));
    assert!(!names.contains(&"Bob".to_string()));
}
```

### Test Count Results

```rust
#[test]
fn test_vertex_count() {
    let (graph, _, _, _) = create_social_network();
    let snap = graph.snapshot();
    let g = snap.traversal();
    
    let count = g.v().has_label("person").count();
    assert_eq!(count, 3);
}
```

### Test Empty Results

```rust
#[test]
fn test_no_matches() {
    let (graph, _, _, _) = create_social_network();
    let snap = graph.snapshot();
    let g = snap.traversal();
    
    let results = g.v()
        .has_label("company")  // No companies in test data
        .to_list();
    
    assert!(results.is_empty());
}
```

---

## Testing Mutations

### Test Add Vertex

```rust
#[test]
fn test_add_vertex() {
    let graph = Graph::new();
    
    let id = graph.add_vertex("person", props! {
        "name" => "Test"
    });
    
    let snapshot = graph.snapshot();
    let vertex = snapshot.get_vertex(id).unwrap();
    assert_eq!(vertex.label(), "person");
    assert_eq!(
        vertex.property("name"),
        Some(&Value::String("Test".into()))
    );
}
```

### Test Add Edge

```rust
#[test]
fn test_add_edge() {
    let graph = Graph::new();
    
    let v1 = graph.add_vertex("person", props! {});
    let v2 = graph.add_vertex("person", props! {});
    
    let edge_id = graph.add_edge(v1, v2, "knows", props! {}).unwrap();
    
    let snapshot = graph.snapshot();
    let edge = snapshot.get_edge(edge_id).unwrap();
    assert_eq!(edge.label(), "knows");
    assert_eq!(edge.from_id(), v1);
    assert_eq!(edge.to_id(), v2);
}
```

### Test GQL Mutations

```rust
#[test]
fn test_gql_create() {
    let graph = Graph::new();
    let mut storage = graph.as_storage_mut();
    
    let stmt = parse_statement(
        "CREATE (n:person {name: 'Alice', age: 30})"
    ).unwrap();
    execute_mutation(&stmt, &mut storage).unwrap();
    
    drop(storage);  // Release mutable borrow
    
    let snapshot = graph.snapshot();
    assert_eq!(snapshot.vertex_count(), 1);
    
    // Verify the created vertex
    let g = snapshot.traversal();
    let names = g.v().values("name").to_list();
    assert_eq!(names, vec![Value::String("Alice".into())]);
}
```

---

## Testing GQL Queries

```rust
#[test]
fn test_gql_query() {
    let (graph, _, _, _) = create_social_network();
    let snap = graph.snapshot();
    
    let results = snap.gql("
        MATCH (p:person)
        WHERE p.age > 25
        RETURN p.name
        ORDER BY p.name
    ").unwrap();
    
    assert_eq!(results.len(), 2);  // Alice (30) and Carol (35)
}
```

---

## Testing Error Cases

### Test Invalid Operations

```rust
#[test]
fn test_edge_to_nonexistent_vertex() {
    let graph = Graph::new();
    let v1 = graph.add_vertex("person", props! {});
    
    let result = graph.add_edge(
        v1,
        VertexId(9999),  // Doesn't exist
        "knows",
        props! {},
    );
    
    assert!(result.is_err());
}
```

### Test GQL Parse Errors

```rust
#[test]
fn test_invalid_gql() {
    let result = parse_statement("MATCH (n) RETURN");  // Incomplete
    assert!(result.is_err());
}
```

---

## Integration Tests

For larger integration tests, use the `tests/` directory:

```rust
// tests/integration_test.rs
use interstellar::prelude::*;

#[test]
fn test_full_workflow() {
    // Create graph
    let graph = Graph::new();
    
    // Load test data
    load_test_fixtures(&graph);
    
    // Run queries
    let snap = graph.snapshot();
    let results = snap.gql("...").unwrap();
    
    // Verify results
    assert!(!results.is_empty());
}
```

---

## Property-Based Testing

Use `proptest` for property-based tests:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_value_roundtrip(i: i64) {
        let value = Value::Int(i);
        let graph = Graph::new();
        
        let id = graph.add_vertex("test", props! {
            "num" => i
        });
        
        let snapshot = graph.snapshot();
        let vertex = snapshot.get_vertex(id).unwrap();
        assert_eq!(vertex.property("num"), Some(&value));
    }
}
```

---

## Test Organization

```
tests/
├── common/
│   └── mod.rs          # Shared fixtures and helpers
├── gremlin_tests.rs    # Gremlin API tests
├── gql_tests.rs        # GQL tests
└── storage_tests.rs    # Storage backend tests

src/
├── lib.rs
└── ...
    #[cfg(test)]
    mod tests {
        // Unit tests alongside code
    }
```

### Common Test Module

```rust
// tests/common/mod.rs
use interstellar::prelude::*;

pub fn create_test_graph() -> Graph {
    Graph::new()
}

pub fn load_test_fixtures(graph: &Graph) {
    // Load standard test data
}
```

---

## Best Practices

1. **Use Graph for tests** - Fast, isolated, no cleanup needed
2. **Create fixture functions** - Reusable test data setup
3. **Test both positive and negative cases** - Include error scenarios
4. **Keep tests focused** - One assertion per concept
5. **Use descriptive test names** - `test_filter_by_age_returns_correct_vertices`
6. **Clean separation** - Don't share mutable state between tests

---

## See Also

- [Quick Start](../getting-started/quick-start.md) - Basic usage patterns
- [Gremlin API](../api/gremlin.md) - Query reference
- [GQL API](../api/gql.md) - GQL reference
