//! Filter Steps Example
//!
//! This example demonstrates all available filter steps:
//! - `has_label()`, `has_label_any()` - Filter by element label
//! - `has()`, `has_value()` - Filter by property existence/value
//! - `has_id()`, `has_ids()` - Filter by element ID
//! - `filter()` - Custom predicate filtering
//! - `dedup()` - Remove duplicates
//! - `dedup_by_key()`, `dedup_by_label()`, `dedup_by()` - Dedup with custom key
//! - `limit()`, `skip()`, `range()` - Pagination/slicing
//! - `tail()`, `tail_n()` - Get last elements
//! - `coin()` - Probabilistic filtering
//! - `sample()` - Random sampling
//! - `has_key()`, `has_prop_value()` - Property map filtering
//! - `where_p()` - Predicate-based filtering
//!
//! Run with: `cargo run --example filter_steps`

use intersteller::graph::Graph;
use intersteller::p;
use intersteller::storage::InMemoryGraph;
use intersteller::traversal::__;
use intersteller::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    println!("=== Intersteller Filter Steps Example ===\n");

    // Create test graph
    let graph = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // -------------------------------------------------------------------------
    // has_label() - Filter by single label
    // -------------------------------------------------------------------------
    println!("--- has_label() - Filter by label ---");
    let people = g.v().has_label("person").to_list();
    println!("Persons: {} vertices", people.len());

    let software = g.v().has_label("software").to_list();
    println!("Software: {} vertices", software.len());

    let knows_edges = g.e().has_label("knows").to_list();
    println!("'knows' edges: {}", knows_edges.len());

    let uses_edges = g.e().has_label("uses").to_list();
    println!("'uses' edges: {}", uses_edges.len());
    println!();

    // -------------------------------------------------------------------------
    // has_label_any() - Filter by multiple labels
    // -------------------------------------------------------------------------
    println!("--- has_label_any() - Filter by multiple labels ---");
    let entities = g.v().has_label_any(["person", "company"]).to_list();
    println!("Persons + Companies: {} vertices", entities.len());

    let relationship_edges = g.e().has_label_any(["knows", "works_at"]).to_list();
    println!("'knows' + 'works_at' edges: {}", relationship_edges.len());
    println!();

    // -------------------------------------------------------------------------
    // has() - Filter by property existence
    // -------------------------------------------------------------------------
    println!("--- has() - Filter by property existence ---");
    let with_age = g.v().has("age").to_list();
    println!("Vertices with 'age' property: {}", with_age.len());

    let with_version = g.v().has("version").to_list();
    println!("Vertices with 'version' property: {}", with_version.len());

    let with_title = g.v().has("title").to_list();
    println!("Vertices with 'title' property: {}", with_title.len());
    println!();

    // -------------------------------------------------------------------------
    // has_value() - Filter by property value
    // -------------------------------------------------------------------------
    println!("--- has_value() - Filter by property value ---");
    let alice = g.v().has_value("name", "Alice").to_list();
    println!("Vertices named 'Alice': {}", alice.len());

    let age_30 = g.v().has_value("age", 30i64).to_list();
    println!("Vertices with age=30: {}", age_30.len());

    let seniors = g.v().has_label("person").has_value("age", 35i64).to_list();
    println!("Persons with age=35: {}", seniors.len());
    println!();

    // -------------------------------------------------------------------------
    // has_id() / has_ids() - Filter by element ID
    // -------------------------------------------------------------------------
    println!("--- has_id() / has_ids() - Filter by ID ---");
    // Get the first vertex to use as an ID reference
    let first_vertex = g.v().next().unwrap();
    let first_id = first_vertex.as_vertex_id().unwrap();

    let vertex_by_id = g.v().has_id(first_id).to_list();
    println!("Vertex with first ID: {} found", vertex_by_id.len());

    // Get first 3 vertices and filter by their IDs
    let first_three: Vec<_> = g.v().take(3);
    let ids: Vec<_> = first_three
        .iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();
    let specific_vertices = g.v().has_ids(ids.clone()).to_list();
    println!(
        "Vertices with first 3 IDs: {} found",
        specific_vertices.len()
    );

    // Combining with other filters
    let person_by_id = g.v().has_id(first_id).has_label("person").to_list();
    println!(
        "First vertex if it's a person: {} found",
        person_by_id.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // filter() - Custom predicate
    // -------------------------------------------------------------------------
    println!("--- filter() - Custom predicate ---");

    // Filter integers > 2
    let large_nums = g
        .inject([1i64, 2i64, 3i64, 4i64, 5i64])
        .filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 2))
        .to_list();
    println!("Integers > 2: {:?}", large_nums);

    // Filter vertices (custom logic)
    let vertex_filter = g.v().filter(|_ctx, v| v.is_vertex()).to_list();
    println!("Vertices (via filter): {}", vertex_filter.len());

    // Filter strings containing 'a'
    let with_a = g
        .inject(["Alice", "Bob", "Charlie", "David"])
        .filter(|_ctx, v| matches!(v, Value::String(s) if s.to_lowercase().contains('a')))
        .to_list();
    println!("Names containing 'a': {:?}", with_a);
    println!();

    // -------------------------------------------------------------------------
    // dedup() - Remove duplicates
    // -------------------------------------------------------------------------
    println!("--- dedup() - Remove duplicates ---");
    let with_dups = g.inject([1i64, 2i64, 1i64, 3i64, 2i64, 4i64]).to_list();
    println!("With duplicates: {:?}", with_dups);

    let deduped = g
        .inject([1i64, 2i64, 1i64, 3i64, 2i64, 4i64])
        .dedup()
        .to_list();
    println!("After dedup(): {:?}", deduped);
    println!();

    // -------------------------------------------------------------------------
    // dedup_by_key(), dedup_by_label(), dedup_by() - Dedup with custom key
    // -------------------------------------------------------------------------
    println!("--- dedup_by_*() - Dedup with custom key ---");

    // Dedup by label
    let dedup_by_label = g.v().dedup_by_label().to_list();
    println!(
        "Vertices dedup by label (one per label): {} vertices",
        dedup_by_label.len()
    );

    // Dedup by property key
    let dedup_by_name = g.v().has_label("person").dedup_by_key("name").to_list();
    println!("Persons dedup by name: {} vertices", dedup_by_name.len());

    // Dedup by traversal (e.g., by age value)
    let dedup_by_age = g
        .v()
        .has_label("person")
        .dedup_by(__::values("age"))
        .to_list();
    println!("Persons dedup by age: {} vertices", dedup_by_age.len());
    println!();

    // -------------------------------------------------------------------------
    // limit() - Take first N
    // -------------------------------------------------------------------------
    println!("--- limit() - Take first N ---");
    let all = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).to_list();
    println!("All values: {:?}", all);

    let first_three = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).limit(3).to_list();
    println!("limit(3): {:?}", first_three);

    let first_person = g.v().has_label("person").limit(1).to_list();
    println!("First person vertex: {} found", first_person.len());
    println!();

    // -------------------------------------------------------------------------
    // skip() - Skip first N
    // -------------------------------------------------------------------------
    println!("--- skip() - Skip first N ---");
    let skipped = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).skip(2).to_list();
    println!("skip(2): {:?}", skipped);

    let after_first = g.v().skip(1).to_list();
    println!("Vertices after skipping 1: {} found", after_first.len());
    println!();

    // -------------------------------------------------------------------------
    // range() - Select range [start, end)
    // -------------------------------------------------------------------------
    println!("--- range() - Select range ---");
    let range_result = g
        .inject([0i64, 1i64, 2i64, 3i64, 4i64, 5i64])
        .range(2, 5)
        .to_list();
    println!("range(2, 5): {:?}", range_result);

    // Pagination example: page 2 with page_size=2
    let page_size = 2;
    let page = 1; // 0-indexed
    let page_result = g
        .inject([0i64, 1i64, 2i64, 3i64, 4i64, 5i64])
        .range(page * page_size, (page + 1) * page_size)
        .to_list();
    println!("Page 1 (size 2): {:?}", page_result);
    println!();

    // -------------------------------------------------------------------------
    // tail() / tail_n() - Get last elements
    // -------------------------------------------------------------------------
    println!("--- tail() / tail_n() - Get last elements ---");

    let last_one = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).tail().to_list();
    println!("tail(): {:?}", last_one);

    let last_three = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).tail_n(3).to_list();
    println!("tail_n(3): {:?}", last_three);

    // Tail with ordering - get oldest person
    let oldest = g
        .v()
        .has_label("person")
        .values("age")
        .order()
        .build()
        .tail()
        .to_list();
    println!("Oldest person's age: {:?}", oldest);
    println!();

    // -------------------------------------------------------------------------
    // coin() - Probabilistic filtering
    // -------------------------------------------------------------------------
    println!("--- coin() - Probabilistic filtering ---");

    // coin(0.0) filters everything
    let coin_zero = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).coin(0.0).to_list();
    println!("coin(0.0): {:?} (filters all)", coin_zero);

    // coin(1.0) passes everything
    let coin_one = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).coin(1.0).to_list();
    println!("coin(1.0): {:?} (passes all)", coin_one);

    // coin(0.5) passes approximately half (non-deterministic)
    let coin_half = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).coin(0.5).to_list();
    println!("coin(0.5): {:?} (approximately half)", coin_half);
    println!();

    // -------------------------------------------------------------------------
    // sample() - Random sampling
    // -------------------------------------------------------------------------
    println!("--- sample() - Random sampling ---");

    let sample_2 = g.inject([1i64, 2i64, 3i64, 4i64, 5i64]).sample(2).to_list();
    println!("sample(2) from 5 elements: {:?}", sample_2);

    // Sample more than available returns all
    let sample_10 = g.inject([1i64, 2i64, 3i64]).sample(10).to_list();
    println!("sample(10) from 3 elements: {:?}", sample_10);

    // Sample from graph vertices
    let sample_vertices = g.v().sample(2).to_list();
    println!("sample(2) vertices: {} vertices", sample_vertices.len());
    println!();

    // -------------------------------------------------------------------------
    // has_key() / has_prop_value() - Property map filtering
    // -------------------------------------------------------------------------
    println!("--- has_key() / has_prop_value() - Property filtering ---");

    // Filter properties by key
    let age_props = g
        .v()
        .has_label("person")
        .properties()
        .has_key("age")
        .to_list();
    println!("Properties with key 'age': {} properties", age_props.len());

    // Filter properties by multiple keys
    let name_or_age = g
        .v()
        .has_label("person")
        .properties()
        .has_key_any(["name", "age"])
        .to_list();
    println!(
        "Properties with key 'name' or 'age': {} properties",
        name_or_age.len()
    );

    // Filter properties by value
    let alice_props = g
        .v()
        .has_label("person")
        .properties()
        .has_prop_value("Alice")
        .to_list();
    println!(
        "Properties with value 'Alice': {} properties",
        alice_props.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // where_p() - Predicate-based filtering
    // -------------------------------------------------------------------------
    println!("--- where_p() - Predicate-based filtering ---");

    // Filter by comparison predicate
    let ages_gt_25 = g
        .v()
        .has_label("person")
        .values("age")
        .where_p(p::gt(25))
        .to_list();
    println!("Ages > 25: {:?}", ages_gt_25);

    // Filter by within predicate
    let ages_within = g
        .v()
        .has_label("person")
        .values("age")
        .where_p(p::within([25, 35]))
        .to_list();
    println!("Ages within [25, 35]: {:?}", ages_within);

    // Filter by between predicate
    let ages_between = g
        .v()
        .has_label("person")
        .values("age")
        .where_p(p::between(25, 35))
        .to_list();
    println!("Ages between 25 and 35 (exclusive): {:?}", ages_between);

    // Filter by combined predicate (AND)
    let ages_range = g
        .v()
        .has_label("person")
        .values("age")
        .where_p(p::and(p::gte(25), p::lte(30)))
        .to_list();
    println!("Ages >= 25 AND <= 30: {:?}", ages_range);

    // Filter by combined predicate (OR)
    let ages_or = g
        .v()
        .has_label("person")
        .values("age")
        .where_p(p::or(p::lt(26), p::gt(34)))
        .to_list();
    println!("Ages < 26 OR > 34: {:?}", ages_or);
    println!();

    // -------------------------------------------------------------------------
    // Chaining multiple filters
    // -------------------------------------------------------------------------
    println!("--- Chaining filters ---");
    let result = g.v().has_label("person").has("age").limit(2).to_list();
    println!("First 2 persons with age property: {} found", result.len());

    let complex = g
        .inject([1i64, 2i64, 3i64, 4i64, 5i64, 3i64, 2i64])
        .dedup()
        .filter(|_ctx, v| matches!(v, Value::Int(n) if *n > 1))
        .skip(1)
        .limit(2)
        .to_list();
    println!(
        "Complex chain (dedup, filter>1, skip 1, limit 2): {:?}",
        complex
    );

    // Chain with new filter steps
    let new_chain = g
        .v()
        .has_label("person")
        .values("age")
        .order()
        .build()
        .tail_n(2)
        .where_p(p::gte(30))
        .to_list();
    println!("Oldest 2 persons with age >= 30: {:?}", new_chain);
    println!();

    println!("=== Example Complete ===");
}

/// Create a test graph with people, software, and companies
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

    // Add company vertex
    let acme = storage.add_vertex("company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Acme Corp".to_string()));
        props.insert("employees".to_string(), Value::Int(100));
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
    storage
        .add_edge(alice, acme, "works_at", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, acme, "works_at", HashMap::new())
        .unwrap();

    Graph::new(storage)
}
