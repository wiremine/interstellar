//! Terminal Steps Example
//!
//! This example demonstrates all available terminal steps that execute
//! traversals and produce results:
//!
//! Collection terminals:
//! - `to_list()` - Collect all values into a Vec
//! - `to_set()` - Collect unique values into a HashSet
//! - `take(n)` - Collect first n values
//!
//! Single-value terminals:
//! - `next()` - Get first value (Option)
//! - `one()` - Get exactly one value (Result)
//! - `has_next()` - Check if any results exist
//!
//! Aggregation terminals:
//! - `count()` - Count results
//! - `sum()` - Sum numeric values
//! - `min()` / `max()` - Find minimum/maximum
//! - `fold()` - Custom accumulation
//!
//! Iteration terminals:
//! - `iter()` - Get iterator over values
//! - `traversers()` - Get iterator over Traversers (with metadata)
//! - `iterate()` - Consume traversal without collecting
//!
//! Run with: `cargo run --example terminal_steps`

use rustgremlin::graph::Graph;
use rustgremlin::storage::InMemoryGraph;
use rustgremlin::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    println!("=== RustGremlin Terminal Steps Example ===\n");

    // Create test graph
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // -------------------------------------------------------------------------
    // to_list() - Collect all values into a Vec
    // -------------------------------------------------------------------------
    println!("--- to_list() - Collect to Vec ---");
    let all_vertices: Vec<Value> = g.v().to_list();
    println!("All vertices: {} items", all_vertices.len());

    let all_edges: Vec<Value> = g.e().to_list();
    println!("All edges: {} items", all_edges.len());

    let numbers: Vec<Value> = g.inject([1i64, 2i64, 3i64]).to_list();
    println!("Injected numbers: {:?}", numbers);
    println!();

    // -------------------------------------------------------------------------
    // to_set() - Collect unique values into a HashSet
    // -------------------------------------------------------------------------
    println!("--- to_set() - Collect unique to HashSet ---");
    let unique_values = g.inject([1i64, 2i64, 1i64, 3i64, 2i64]).to_set();
    println!(
        "Unique values from [1,2,1,3,2]: {} items",
        unique_values.len()
    );
    println!("Contains 1: {}", unique_values.contains(&Value::Int(1)));
    println!("Contains 4: {}", unique_values.contains(&Value::Int(4)));

    // Useful for deduplicating traversal results
    let unique_neighbors = g.v().out().to_set();
    println!(
        "Unique outgoing neighbors: {} vertices",
        unique_neighbors.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // take(n) - Collect first n values
    // -------------------------------------------------------------------------
    println!("--- take(n) - Collect first n ---");
    let first_two: Vec<Value> = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).take(2);
    println!("First 2 of [1,2,3,4,5]: {:?}", first_two);

    let first_vertex: Vec<Value> = g.v().take(1);
    println!(
        "First vertex: {:?}",
        first_vertex.first().map(|v| v.as_vertex_id())
    );

    // take() returns fewer if not enough elements
    let all_available: Vec<Value> = g.inject([1i64, 2i64]).take(10);
    println!(
        "take(10) from [1,2]: {:?} ({} items)",
        all_available,
        all_available.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // next() - Get first value as Option
    // -------------------------------------------------------------------------
    println!("--- next() - Get first value ---");
    let first: Option<Value> = g.inject([10i64, 20i64, 30i64]).next();
    println!("First of [10,20,30]: {:?}", first);

    let first_person = g.v().has_label("person").next();
    println!(
        "First person vertex: {:?}",
        first_person.map(|v| v.as_vertex_id())
    );

    // Returns None for empty traversals
    let empty: Option<Value> = g.v().has_label("nonexistent").next();
    println!("First of empty: {:?}", empty);
    println!();

    // -------------------------------------------------------------------------
    // one() - Get exactly one value (errors on 0 or >1)
    // -------------------------------------------------------------------------
    println!("--- one() - Get exactly one value ---");

    // Success case: exactly one result
    let single = g.inject([42i64]).one();
    println!("one() from [42]: {:?}", single);

    // Error case: empty traversal
    let empty_result = g.v().has_label("nonexistent").one();
    println!("one() from empty: {:?}", empty_result);

    // Error case: multiple results
    let multiple_result = g.inject([1i64, 2i64]).one();
    println!("one() from [1,2]: {:?}", multiple_result);

    // Useful for queries that should return exactly one result
    let alice = g.v().has_value("name", "Alice").one();
    println!("one() for Alice: {:?}", alice.map(|v| v.as_vertex_id()));
    println!();

    // -------------------------------------------------------------------------
    // has_next() - Check if any results exist
    // -------------------------------------------------------------------------
    println!("--- has_next() - Check existence ---");
    let has_vertices: bool = g.v().has_next();
    println!("Graph has vertices: {}", has_vertices);

    let has_people: bool = g.v().has_label("person").has_next();
    println!("Graph has people: {}", has_people);

    let has_robots: bool = g.v().has_label("robot").has_next();
    println!("Graph has robots: {}", has_robots);

    // Useful for conditional logic
    if g.v().has_label("software").has_next() {
        println!("Software exists in the graph!");
    }
    println!();

    // -------------------------------------------------------------------------
    // count() - Count results
    // -------------------------------------------------------------------------
    println!("--- count() - Count results ---");
    let vertex_count: u64 = g.v().count();
    println!("Total vertices: {}", vertex_count);

    let edge_count: u64 = g.e().count();
    println!("Total edges: {}", edge_count);

    let person_count: u64 = g.v().has_label("person").count();
    println!("Person vertices: {}", person_count);

    let knows_count: u64 = g.e().has_label("knows").count();
    println!("'knows' edges: {}", knows_count);
    println!();

    // -------------------------------------------------------------------------
    // sum() - Sum numeric values
    // -------------------------------------------------------------------------
    println!("--- sum() - Sum numeric values ---");
    let int_sum = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).sum();
    println!("Sum of [1,2,3,4,5]: {:?}", int_sum);

    // Mixed int and float produces float result
    let values: Vec<Value> = vec![Value::Int(10), Value::Float(2.5), Value::Int(7)];
    let mixed_sum = g.inject(values).sum();
    println!("Sum of [10, 2.5, 7]: {:?}", mixed_sum);

    // Non-numeric values are ignored
    let with_strings: Vec<Value> = vec![
        Value::Int(5),
        Value::String("ignored".to_string()),
        Value::Int(3),
    ];
    let partial_sum = g.inject(with_strings).sum();
    println!("Sum of [5, 'ignored', 3]: {:?}", partial_sum);

    // Empty traversal returns 0
    let empty_sum = g.v().has_label("nonexistent").sum();
    println!("Sum of empty: {:?}", empty_sum);
    println!();

    // -------------------------------------------------------------------------
    // min() / max() - Find minimum/maximum
    // -------------------------------------------------------------------------
    println!("--- min() / max() - Find extremes ---");
    let minimum = g.inject([5i64, 2i64, 8i64, 1i64, 9i64]).min();
    println!("Min of [5,2,8,1,9]: {:?}", minimum);

    let maximum = g.inject([5i64, 2i64, 8i64, 1i64, 9i64]).max();
    println!("Max of [5,2,8,1,9]: {:?}", maximum);

    // Works with strings too
    let min_string = g.inject(["banana", "apple", "cherry"]).min();
    println!("Min of ['banana','apple','cherry']: {:?}", min_string);

    let max_string = g.inject(["banana", "apple", "cherry"]).max();
    println!("Max of ['banana','apple','cherry']: {:?}", max_string);

    // Returns None for empty
    let empty_min = g.v().has_label("nonexistent").min();
    println!("Min of empty: {:?}", empty_min);
    println!();

    // -------------------------------------------------------------------------
    // fold() - Custom accumulation
    // -------------------------------------------------------------------------
    println!("--- fold() - Custom accumulation ---");

    // Sum using fold
    let fold_sum = g
        .inject([1i64, 2i64, 3i64, 4i64])
        .fold(0i64, |acc, v| acc + v.as_i64().unwrap_or(0));
    println!("Fold sum of [1,2,3,4]: {}", fold_sum);

    // Product using fold
    let fold_product = g
        .inject([2i64, 3i64, 4i64])
        .fold(1i64, |acc, v| acc * v.as_i64().unwrap_or(1));
    println!("Fold product of [2,3,4]: {}", fold_product);

    // Concatenate strings
    let concat = g
        .inject(["Hello", " ", "World", "!"])
        .fold(String::new(), |mut acc, v| {
            if let Some(s) = v.as_str() {
                acc.push_str(s);
            }
            acc
        });
    println!("Fold concat: '{}'", concat);

    // Count with fold (alternative to count())
    let fold_count = g.v().fold(0u64, |acc, _| acc + 1);
    println!("Fold count of vertices: {}", fold_count);
    println!();

    // -------------------------------------------------------------------------
    // iter() - Get iterator over values
    // -------------------------------------------------------------------------
    println!("--- iter() - Iterate over values ---");
    print!("Iterating values: ");
    for value in g.inject([1i64, 2i64, 3i64]).iter() {
        print!("{:?} ", value);
    }
    println!();

    // Can use iterator methods
    let doubled: Vec<i64> = g
        .inject([1i64, 2i64, 3i64])
        .iter()
        .filter_map(|v| v.as_i64())
        .map(|n| n * 2)
        .collect();
    println!("Doubled via iter: {:?}", doubled);
    println!();

    // -------------------------------------------------------------------------
    // traversers() - Get iterator over Traversers (with metadata)
    // -------------------------------------------------------------------------
    println!("--- traversers() - Iterate with metadata ---");
    println!("Traversers contain value and metadata:");
    for (i, traverser) in g.v().has_label("person").traversers().enumerate() {
        println!(
            "  Traverser {}: value={:?}, is_vertex={}",
            i,
            traverser.value.as_vertex_id(),
            traverser.is_vertex()
        );
    }
    println!();

    // -------------------------------------------------------------------------
    // iterate() - Consume without collecting (for side effects)
    // -------------------------------------------------------------------------
    println!("--- iterate() - Consume without collecting ---");
    println!("iterate() runs the traversal but discards results.");
    println!("Useful for side-effect-only traversals.");

    // This just consumes the traversal
    g.v().iterate();
    println!("Traversal consumed via iterate()");
    println!();

    // -------------------------------------------------------------------------
    // Combining terminals with traversal chains
    // -------------------------------------------------------------------------
    println!("--- Combined examples ---");

    // Count people who know someone
    let people_who_know = g
        .v()
        .has_label("person")
        .out_labels(&["knows"])
        .dedup()
        .count();
    println!("Unique people known by someone: {}", people_who_know);

    // Check if Alice knows anyone
    let alice_knows = g
        .v()
        .has_value("name", "Alice")
        .out_labels(&["knows"])
        .has_next();
    println!("Alice knows someone: {}", alice_knows);

    // Get first 2 software users
    let users = g.v().has_label("software").in_labels(&["uses"]).take(2);
    println!("First 2 software users: {} found", users.len());
    println!();

    println!("=== Example Complete ===");
}

/// Create a test graph with people and software
fn create_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Add person vertices
    let alice = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props
    });

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

    // Add software vertex
    let graph_db = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(1.0));
        props
    });

    // Add edges
    storage
        .add_edge(alice, bob, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, alice, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, graph_db, "uses", HashMap::new())
        .unwrap();

    Graph::new(Arc::new(storage))
}
