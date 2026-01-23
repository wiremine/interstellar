# Quick Start

Get up and running with Interstellar in 5 minutes. This guide covers creating a graph, adding data, and running your first queries.

## Creating a Graph

Start by creating an in-memory graph:

```rust
use interstellar::graph::Graph;
use interstellar::storage::InMemoryGraph;
use interstellar::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    // Create storage backend
    let mut storage = InMemoryGraph::new();
    
    // Add some data (we'll cover this next)
    // ...
    
    // Wrap in Graph for the traversal API
    let graph = Graph::new(Arc::new(storage));
}
```

## Adding Vertices

Vertices (nodes) represent entities in your graph:

```rust
use std::collections::HashMap;
use interstellar::value::Value;

// Create a person vertex
let alice = storage.add_vertex("person", HashMap::from([
    ("name".to_string(), Value::String("Alice".to_string())),
    ("age".to_string(), Value::Int(30)),
]));

let bob = storage.add_vertex("person", HashMap::from([
    ("name".to_string(), Value::String("Bob".to_string())),
    ("age".to_string(), Value::Int(25)),
]));

let rust_lang = storage.add_vertex("language", HashMap::from([
    ("name".to_string(), Value::String("Rust".to_string())),
]));
```

Each `add_vertex` call returns a `VertexId` that you can use to reference the vertex.

## Adding Edges

Edges represent relationships between vertices:

```rust
// Alice knows Bob
storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

// Alice and Bob both program in Rust
storage.add_edge(alice, rust_lang, "programs_in", HashMap::from([
    ("skill_level".to_string(), Value::String("expert".to_string())),
])).unwrap();

storage.add_edge(bob, rust_lang, "programs_in", HashMap::from([
    ("skill_level".to_string(), Value::String("intermediate".to_string())),
])).unwrap();
```

## Querying with Gremlin-Style API

Now let's query the graph using the fluent traversal API:

```rust
// Create a graph handle and get a snapshot
let graph = Graph::new(Arc::new(storage));
let snapshot = graph.snapshot();
let g = snapshot.traversal();

// Find all people
let people = g.v()
    .has_label("person")
    .to_list();
println!("Found {} people", people.len());

// Get names of all people
let names = g.v()
    .has_label("person")
    .values("name")
    .to_list();
println!("Names: {:?}", names);

// Find who Alice knows
let alice_friends = g.v_ids([alice])
    .out("knows")
    .values("name")
    .to_list();
println!("Alice knows: {:?}", alice_friends);

// Find all Rust programmers
let rust_devs = g.v()
    .has_label("language")
    .has_value("name", "Rust")
    .in_("programs_in")
    .values("name")
    .to_list();
println!("Rust programmers: {:?}", rust_devs);
```

## Querying with GQL

Interstellar also supports SQL-like GQL syntax:

```rust
// Simple pattern match
let results = snapshot.gql("
    MATCH (p:person)
    RETURN p.name, p.age
").unwrap();

// Find relationships
let results = snapshot.gql("
    MATCH (a:person)-[:knows]->(b:person)
    RETURN a.name AS person, b.name AS friend
").unwrap();

// Filter with WHERE
let results = snapshot.gql("
    MATCH (p:person)
    WHERE p.age > 25
    RETURN p.name
").unwrap();
```

## Complete Example

Here's a complete runnable example:

```rust
use interstellar::graph::Graph;
use interstellar::storage::InMemoryGraph;
use interstellar::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    // Create and populate graph
    let mut storage = InMemoryGraph::new();
    
    let alice = storage.add_vertex("person", HashMap::from([
        ("name".to_string(), Value::String("Alice".to_string())),
        ("age".to_string(), Value::Int(30)),
    ]));
    
    let bob = storage.add_vertex("person", HashMap::from([
        ("name".to_string(), Value::String("Bob".to_string())),
        ("age".to_string(), Value::Int(25)),
    ]));
    
    storage.add_edge(alice, bob, "knows", HashMap::new()).unwrap();
    
    // Query the graph
    let graph = Graph::new(Arc::new(storage));
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();
    
    // Find friends of Alice
    let friends = g.v_ids([alice])
        .out("knows")
        .values("name")
        .to_list();
    
    println!("Alice's friends: {:?}", friends);
    // Output: Alice's friends: [String("Bob")]
}
```

## Next Steps

- [Examples](examples.md) - More detailed example programs
- [Gremlin API](../api/gremlin.md) - Complete traversal step reference
- [GQL API](../api/gql.md) - Full GQL syntax reference
- [Graph Modeling](../guides/graph-modeling.md) - Design your graph schema

## See Also

- [Storage Backends](../concepts/storage-backends.md) - Use persistent storage
- [Predicates](../api/predicates.md) - Filter with `gt`, `lt`, `contains`, etc.
