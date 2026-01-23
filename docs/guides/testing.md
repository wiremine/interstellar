# Testing Guide

Best practices for testing graph-based applications with Interstellar.

## Test Setup

### In-Memory Graphs for Tests

Always use `InMemoryGraph` for tests - it's fast and isolated:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use interstellar::storage::InMemoryGraph;
    use interstellar::graph::Graph;
    use interstellar::value::Value;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn create_test_graph() -> Graph {
        let storage = InMemoryGraph::new();
        Graph::new(Arc::new(storage))
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
fn create_social_network() -> (InMemoryGraph, VertexId, VertexId, VertexId) {
    let mut storage = InMemoryGraph::new();
    
    let alice = storage.add_vertex("person", HashMap::from([
        ("name".into(), Value::String("Alice".into())),
        ("age".into(), Value::Int(30)),
    ]));
    
    let bob = storage.add_vertex("person", HashMap::from([
        ("name".into(), Value::String("Bob".into())),
        ("age".into(), Value::Int(25)),
    ]));
    
    let carol = storage.add_vertex("person", HashMap::from([
        ("name".into(), Value::String("Carol".into())),
        ("age".into(), Value::Int(35)),
    ]));
    
    storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    storage.add_edge(bob, carol, "knows", HashMap::new()).unwrap();
    
    (storage, alice, bob, carol)
}

#[test]
fn test_friend_of_friend() {
    let (storage, alice, _, carol) = create_social_network();
    let graph = Graph::new(Arc::new(storage));
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
    let (storage, _, _, _) = create_social_network();
    let graph = Graph::new(Arc::new(storage));
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
    let (storage, _, _, _) = create_social_network();
    let graph = Graph::new(Arc::new(storage));
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
    let (storage, _, _, _) = create_social_network();
    let graph = Graph::new(Arc::new(storage));
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
    let mut storage = InMemoryGraph::new();
    
    let id = storage.add_vertex("person", HashMap::from([
        ("name".into(), Value::String("Test".into())),
    ]));
    
    let vertex = storage.get_vertex(id).unwrap();
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
    let mut storage = InMemoryGraph::new();
    
    let v1 = storage.add_vertex("person", HashMap::new());
    let v2 = storage.add_vertex("person", HashMap::new());
    
    let edge_id = storage.add_edge(v1, v2, "knows", HashMap::new()).unwrap();
    
    let edge = storage.get_edge(edge_id).unwrap();
    assert_eq!(edge.label(), "knows");
    assert_eq!(edge.from_id(), v1);
    assert_eq!(edge.to_id(), v2);
}
```

### Test GQL Mutations

```rust
#[test]
fn test_gql_create() {
    let mut storage = InMemoryGraph::new();
    
    let stmt = parse_statement(
        "CREATE (n:person {name: 'Alice', age: 30})"
    ).unwrap();
    execute_mutation(&stmt, &mut storage).unwrap();
    
    assert_eq!(storage.vertex_count(), 1);
    
    // Verify the created vertex
    let graph = Graph::new(Arc::new(storage));
    let snap = graph.snapshot();
    let g = snap.traversal();
    
    let names = g.v().values("name").to_list();
    assert_eq!(names, vec![Value::String("Alice".into())]);
}
```

---

## Testing GQL Queries

```rust
#[test]
fn test_gql_query() {
    let (storage, _, _, _) = create_social_network();
    let graph = Graph::new(Arc::new(storage));
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
    let mut storage = InMemoryGraph::new();
    let v1 = storage.add_vertex("person", HashMap::new());
    
    let result = storage.add_edge(
        v1,
        VertexId(9999),  // Doesn't exist
        "knows",
        HashMap::new(),
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
    let mut storage = InMemoryGraph::new();
    
    // Load test data
    load_test_fixtures(&mut storage);
    
    // Create graph handle
    let graph = Graph::new(Arc::new(storage));
    
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
        let storage = InMemoryGraph::new();
        
        let id = storage.add_vertex("test", HashMap::from([
            ("num".into(), value.clone()),
        ]));
        
        let vertex = storage.get_vertex(id).unwrap();
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
    // Shared setup
}

pub fn load_test_fixtures(storage: &mut InMemoryGraph) {
    // Load standard test data
}
```

---

## Best Practices

1. **Use InMemoryGraph for tests** - Fast, isolated, no cleanup needed
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
