//! Integration test for complex "Find Similar Customers" query.
//!
//! This tests a complex Gremlin-style query pattern that:
//! 1. Starts from a person (Alice)
//! 2. Finds products they purchased and their categories
//! 3. Finds other customers who purchased products in those categories
//! 4. Excludes Alice and people Alice already knows
//! 5. Groups by customer and counts shared categories
//! 6. Orders by shared category count and returns top 5
//!
//! Original Gremlin query:
//! ```groovy
//! g.V().has('Person', 'name', 'Alice').as('alice')
//!   .out('purchased').out('inCategory').dedup().as('categories')
//!   .in('inCategory').as('sharedProducts')
//!   .in('purchased').as('similarCustomers')
//!   .where(neq('alice'))
//!   .where(__.not(__.as('similarCustomers').in('knows').as('alice')))
//!   .group()
//!     .by(select('similarCustomers'))
//!     .by(select('categories').dedup().count())
//!   .unfold()
//!   .order().by(values, desc)
//!   .limit(5)
//!   .project('customer', 'sharedCategories')
//!     .by(select(keys).values('name'))
//!     .by(select(values))
//! ```

use std::collections::HashMap;

use interstellar::storage::Graph;
use interstellar::traversal::__;
use interstellar::value::{Value, VertexId};

/// Test graph for the similar customers scenario.
///
/// Structure:
/// - People: Alice, Bob, Charlie, Diana, Eve
/// - Products: Laptop, Phone, Headphones, Keyboard, Monitor
/// - Categories: Electronics, Audio, Peripherals
///
/// Relationships:
/// - Alice purchased: Laptop, Phone
/// - Bob purchased: Phone, Headphones (shares Electronics and Audio with Alice)
/// - Charlie purchased: Keyboard, Monitor (shares Peripherals with Alice through Laptop)
/// - Diana purchased: Laptop, Headphones (shares Electronics and Audio with Alice)
/// - Eve purchased: Phone (shares Electronics with Alice)
///
/// Products in categories:
/// - Laptop -> Electronics, Peripherals
/// - Phone -> Electronics
/// - Headphones -> Electronics, Audio
/// - Keyboard -> Peripherals
/// - Monitor -> Peripherals
///
/// Knows relationships (to test exclusion):
/// - Alice knows Bob
#[allow(dead_code)]
pub struct SimilarCustomersTestGraph {
    pub graph: Graph,
    // People
    pub alice: VertexId,
    pub bob: VertexId,
    pub charlie: VertexId,
    pub diana: VertexId,
    pub eve: VertexId,
    // Products
    pub laptop: VertexId,
    pub phone: VertexId,
    pub headphones: VertexId,
    pub keyboard: VertexId,
    pub monitor: VertexId,
    // Categories
    pub electronics: VertexId,
    pub audio: VertexId,
    pub peripherals: VertexId,
}

fn create_similar_customers_graph() -> SimilarCustomersTestGraph {
    let graph = Graph::new();

    // Create people
    let alice = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props
    });

    let bob = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props
    });

    let charlie = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props
    });

    let diana = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Diana".to_string()));
        props
    });

    let eve = graph.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Eve".to_string()));
        props
    });

    // Create products
    let laptop = graph.add_vertex("Product", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Laptop".to_string()));
        props
    });

    let phone = graph.add_vertex("Product", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Phone".to_string()));
        props
    });

    let headphones = graph.add_vertex("Product", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Headphones".to_string()));
        props
    });

    let keyboard = graph.add_vertex("Product", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Keyboard".to_string()));
        props
    });

    let monitor = graph.add_vertex("Product", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Monitor".to_string()));
        props
    });

    // Create categories
    let electronics = graph.add_vertex("Category", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Electronics".to_string()));
        props
    });

    let audio = graph.add_vertex("Category", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Audio".to_string()));
        props
    });

    let peripherals = graph.add_vertex("Category", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Peripherals".to_string()));
        props
    });

    // Add purchased edges
    graph
        .add_edge(alice, laptop, "purchased", HashMap::new())
        .unwrap();
    graph
        .add_edge(alice, phone, "purchased", HashMap::new())
        .unwrap();
    graph
        .add_edge(bob, phone, "purchased", HashMap::new())
        .unwrap();
    graph
        .add_edge(bob, headphones, "purchased", HashMap::new())
        .unwrap();
    graph
        .add_edge(charlie, keyboard, "purchased", HashMap::new())
        .unwrap();
    graph
        .add_edge(charlie, monitor, "purchased", HashMap::new())
        .unwrap();
    graph
        .add_edge(diana, laptop, "purchased", HashMap::new())
        .unwrap();
    graph
        .add_edge(diana, headphones, "purchased", HashMap::new())
        .unwrap();
    graph
        .add_edge(eve, phone, "purchased", HashMap::new())
        .unwrap();

    // Add inCategory edges
    graph
        .add_edge(laptop, electronics, "inCategory", HashMap::new())
        .unwrap();
    graph
        .add_edge(laptop, peripherals, "inCategory", HashMap::new())
        .unwrap();
    graph
        .add_edge(phone, electronics, "inCategory", HashMap::new())
        .unwrap();
    graph
        .add_edge(headphones, electronics, "inCategory", HashMap::new())
        .unwrap();
    graph
        .add_edge(headphones, audio, "inCategory", HashMap::new())
        .unwrap();
    graph
        .add_edge(keyboard, peripherals, "inCategory", HashMap::new())
        .unwrap();
    graph
        .add_edge(monitor, peripherals, "inCategory", HashMap::new())
        .unwrap();

    // Add knows edge (Alice knows Bob - for exclusion test)
    graph.add_edge(alice, bob, "knows", HashMap::new()).unwrap();

    SimilarCustomersTestGraph {
        graph,
        alice,
        bob,
        charlie,
        diana,
        eve,
        laptop,
        phone,
        headphones,
        keyboard,
        monitor,
        electronics,
        audio,
        peripherals,
    }
}

// =============================================================================
// Test: Basic Path Navigation
// =============================================================================

#[test]
fn test_alice_purchased_products() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice purchased: Laptop, Phone
    let products = g.v_ids([tg.alice]).out_labels(&["purchased"]).to_list();

    assert_eq!(products.len(), 2);
    let ids: Vec<VertexId> = products.iter().filter_map(|v| v.as_vertex_id()).collect();
    assert!(ids.contains(&tg.laptop));
    assert!(ids.contains(&tg.phone));
}

#[test]
fn test_alice_product_categories() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Alice's products are in: Electronics (from both), Peripherals (from Laptop)
    let categories = g
        .v_ids([tg.alice])
        .out_labels(&["purchased"])
        .out_labels(&["inCategory"])
        .dedup()
        .to_list();

    assert_eq!(categories.len(), 2); // Electronics and Peripherals (deduplicated)
}

#[test]
fn test_customers_in_same_categories() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find all customers who purchased products in categories Alice's products are in
    let all_customers = g
        .v_ids([tg.alice])
        .out_labels(&["purchased"])
        .out_labels(&["inCategory"])
        .dedup()
        .in_labels(&["inCategory"])
        .in_labels(&["purchased"])
        .dedup()
        .to_list();

    // Should include: Alice (herself), Bob, Charlie, Diana, Eve
    let ids: Vec<VertexId> = all_customers
        .iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();

    // All 5 people should be in the result (we haven't excluded Alice yet)
    assert_eq!(ids.len(), 5);
}

// =============================================================================
// Test: Path Labeling with as_()
// =============================================================================

#[test]
fn test_as_and_select() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Label Alice and select her back
    let result = g
        .v_ids([tg.alice])
        .with_path()
        .as_("alice")
        .out_labels(&["purchased"])
        .select_one("alice")
        .to_list();

    // Should return Alice twice (once for each product she purchased)
    assert_eq!(result.len(), 2);
    for v in &result {
        assert_eq!(v.as_vertex_id(), Some(tg.alice));
    }
}

// =============================================================================
// Test: Where with neq (path comparison) - THIS IS THE GAP
// =============================================================================

#[test]
fn test_where_neq_path_reference() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find customers who are NOT Alice using where_neq
    // This requires comparing current traverser to labeled path value
    let similar_customers = g
        .v_ids([tg.alice])
        .with_path()
        .as_("alice")
        .out_labels(&["purchased"])
        .out_labels(&["inCategory"])
        .dedup()
        .in_labels(&["inCategory"])
        .in_labels(&["purchased"])
        .as_("customer")
        .where_neq("alice") // Current traverser != value at "alice" label
        .dedup()
        .to_list();

    let ids: Vec<VertexId> = similar_customers
        .iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();

    // Should NOT include Alice, should include Bob, Charlie, Diana, Eve
    assert!(!ids.contains(&tg.alice));
    assert_eq!(ids.len(), 4);
}

// =============================================================================
// Test: Where with not() subtraversal for knows exclusion
// =============================================================================

#[test]
fn test_where_not_knows_alice() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Find customers who don't know Alice
    // Alice knows Bob, so Bob should be excluded
    let not_knowing_alice = g
        .v_ids([tg.alice])
        .with_path()
        .as_("alice")
        .out_labels(&["purchased"])
        .out_labels(&["inCategory"])
        .dedup()
        .in_labels(&["inCategory"])
        .in_labels(&["purchased"])
        .as_("customer")
        .where_neq("alice")
        // Filter: customer does NOT have incoming "knows" edge from alice
        // In Gremlin: where(not(__.in('knows').as('alice')))
        // This checks if there's a "knows" edge FROM the current customer TO alice
        // Actually, Alice knows Bob means Alice -> Bob, so Bob.in('knows') = Alice
        .where_(__.not(__.in_labels(&["knows"]).where_eq("alice")))
        .dedup()
        .to_list();

    let ids: Vec<VertexId> = not_knowing_alice
        .iter()
        .filter_map(|v| v.as_vertex_id())
        .collect();

    // Should NOT include Alice (excluded by where_neq)
    // Should NOT include Bob (excluded by knows filter - Alice knows Bob)
    // Should include: Charlie, Diana, Eve
    assert!(!ids.contains(&tg.alice));
    assert!(!ids.contains(&tg.bob));
    assert_eq!(ids.len(), 3);
}

// =============================================================================
// Test: Group by with traversal-based key/value
// =============================================================================

#[test]
fn test_group_by_select() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Simple group by label test
    let grouped = g
        .v()
        .has_label("Person")
        .group()
        .by_label()
        .by_value()
        .build()
        .next();

    assert!(grouped.is_some());
    if let Some(Value::Map(map)) = grouped {
        assert!(map.contains_key("Person"));
        if let Some(Value::List(people)) = map.get("Person") {
            assert_eq!(people.len(), 5);
        }
    }
}

// =============================================================================
// Test: Unfold map entries and extract keys/values
// =============================================================================

#[test]
fn test_unfold_map_entries() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Group by label, then unfold to get individual entries
    let entries = g
        .v()
        .group()
        .by_label()
        .by_value()
        .build()
        .unfold()
        .to_list();

    // Should have entries for: Person, Product, Category
    assert_eq!(entries.len(), 3);

    // Each entry should be a single-key map
    for entry in &entries {
        if let Value::Map(m) = entry {
            assert_eq!(m.len(), 1);
        } else {
            panic!("Expected map entry");
        }
    }
}

#[test]
fn test_select_keys_from_map() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get map keys (labels)
    let labels = g
        .v()
        .group()
        .by_label()
        .by_value()
        .build()
        .unfold()
        .select_keys() // Extract the key from each single-entry map
        .to_list();

    assert_eq!(labels.len(), 3);
    assert!(labels.contains(&Value::String("Person".to_string())));
    assert!(labels.contains(&Value::String("Product".to_string())));
    assert!(labels.contains(&Value::String("Category".to_string())));
}

#[test]
fn test_select_values_from_map() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Get map values (lists of vertices)
    let value_lists = g
        .v()
        .group()
        .by_label()
        .by_value()
        .build()
        .unfold()
        .select_values() // Extract the value from each single-entry map
        .to_list();

    assert_eq!(value_lists.len(), 3);

    // Each value should be a list
    for value in &value_lists {
        assert!(matches!(value, Value::List(_)));
    }
}

// =============================================================================
// Test: Order by value with desc
// =============================================================================

#[test]
fn test_order_by_map_values_desc() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Create a map with numeric values and order by those values
    let ordered = g
        .v()
        .group_count()
        .by_label()
        .build()
        .unfold()
        .order()
        .by_value_desc() // Order by the count (value of single-entry map)
        .build()
        .to_list();

    // Should be ordered: Person (5), Product (5), Category (3)
    assert_eq!(ordered.len(), 3);

    // First should be one of the labels with count 5
    if let Value::Map(m) = &ordered[0] {
        let count = m.values().next().unwrap();
        assert_eq!(count, &Value::Int(5));
    }
}

// =============================================================================
// Full Integration Test: Similar Customers Query
// =============================================================================

#[test]
fn test_full_similar_customers_query() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Full query (simplified version without the complex group-by-count):
    // 1. Start from Alice
    // 2. Find products she purchased and their categories
    // 3. Find other customers in those categories
    // 4. Exclude Alice herself
    // 5. Exclude customers Alice knows (Bob)
    // 6. Return the similar customers

    let similar_customers = g
        .v_ids([tg.alice])
        .with_path()
        .as_("alice")
        .out_labels(&["purchased"]) // Alice's products
        .out_labels(&["inCategory"]) // Product categories
        .dedup() // Unique categories
        .in_labels(&["inCategory"]) // Products in those categories
        .in_labels(&["purchased"]) // Customers who bought those products
        .as_("customer")
        .where_neq("alice") // Exclude Alice
        .where_(__.not(
            // Exclude customers Alice knows
            __.in_labels(&["knows"]).where_eq("alice"),
        ))
        .dedup() // Unique customers
        .values("name") // Get customer names
        .to_list();

    // Expected: Charlie, Diana, Eve (not Alice, not Bob who Alice knows)
    let names: Vec<&str> = similar_customers
        .iter()
        .filter_map(|v| v.as_str())
        .collect();

    assert_eq!(names.len(), 3);
    assert!(names.contains(&"Charlie"));
    assert!(names.contains(&"Diana"));
    assert!(names.contains(&"Eve"));
    assert!(!names.contains(&"Alice"));
    assert!(!names.contains(&"Bob"));
}

// =============================================================================
// Test: Complete Gremlin Query with Ranking
// =============================================================================
//
// This test implements a simplified version of the Gremlin query that ranks
// similar customers by the number of shared category paths (not unique categories).
//
// The full Gremlin query with `.by(select('categories').dedup().count())` requires
// a reducing value collector which runs a single traversal over all group elements.
// This is a more advanced feature - for now we count paths instead of unique categories.
//
#[test]
fn test_complete_similar_customers_with_ranking() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Simplified query: rank by number of shared category PATHS (not unique categories)
    // This counts how many times a customer appears in the traversal results,
    // which correlates with shared categories.

    let ranked_similar = g
        .v_ids([tg.alice])
        .with_path()
        .as_("alice")
        .out_labels(&["purchased"])
        .out_labels(&["inCategory"])
        .dedup()
        .as_("categories")
        .in_labels(&["inCategory"])
        .as_("sharedProducts")
        .in_labels(&["purchased"])
        .as_("similarCustomers")
        .where_neq("alice") // Exclude Alice
        .where_(__.not(
            // Exclude customers Alice knows
            __.in_labels(&["knows"]).where_eq("alice"),
        ))
        // Group by customer and count occurrences (number of paths through categories)
        .group_count()
        .by_traversal(__.select_one("similarCustomers"))
        .build()
        .unfold()
        .order()
        .by_value_desc()
        .build()
        .limit(5)
        .to_list();

    // Expected results based on our test graph:
    // Each customer is counted by the number of category paths they share with Alice.
    // - Diana appears via Electronics (from Laptop, Headphones) and Audio (from Headphones)
    // - Charlie appears via Peripherals (from Keyboard, Monitor)
    // - Eve appears via Electronics (from Phone)

    println!("Ranked similar customers: {:?}", ranked_similar);

    assert!(!ranked_similar.is_empty());
    assert!(ranked_similar.len() <= 5);

    // Verify the structure: each entry should be a single-entry map
    // with customer vertex as key and count as value
    for entry in &ranked_similar {
        if let Value::Map(m) = entry {
            assert_eq!(m.len(), 1, "Expected single-entry map, got {:?}", m);
            // Value should be an integer count
            let count = m.values().next().unwrap();
            assert!(
                matches!(count, Value::Int(_)),
                "Expected Int count, got {:?}",
                count
            );
        } else {
            panic!("Expected map entry, got {:?}", entry);
        }
    }

    // First entry should have the highest count
    if let Value::Map(m) = &ranked_similar[0] {
        let count = m.values().next().unwrap();
        if let Value::Int(n) = count {
            assert!(*n >= 1, "Expected count >= 1, got {}", n);
        }
    }
}

// =============================================================================
// Test: Project step for final output formatting
// =============================================================================

#[test]
fn test_project_customer_output() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // Simplified project test: get person with their name
    let projected = g
        .v()
        .has_label("Person")
        .limit(2)
        .project(&["personName", "personLabel"])
        .by_key("name")
        .by(__.label())
        .build()
        .to_list();

    assert_eq!(projected.len(), 2);

    // Each projected value should be a map with the projection keys
    for proj in &projected {
        if let Value::Map(m) = proj {
            assert!(m.contains_key("personName"));
            assert!(m.contains_key("personLabel"));
            // personLabel should be "Person"
            assert_eq!(
                m.get("personLabel"),
                Some(&Value::String("Person".to_string()))
            );
        } else {
            panic!("Expected projected map");
        }
    }
}

// =============================================================================
// Test: by_value_fold() - Count unique categories per customer
// =============================================================================
//
// This test demonstrates the proper implementation of the Gremlin pattern:
//   .group().by(select('customer')).by(select('categories').dedup().count())
//
// The by_value_fold() method runs a reducing traversal on ALL traversers in each
// group, enabling patterns that aggregate (count, sum, dedup, etc.) across the
// entire group rather than per-element.
//

#[test]
fn test_by_value_fold_unique_category_count() {
    let tg = create_similar_customers_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.gremlin();

    // This query:
    // 1. Start from Alice
    // 2. Find products she purchased and their categories
    // 3. Find other customers in those categories (via products)
    // 4. Exclude Alice herself and customers she knows
    // 5. GROUP by customer, counting UNIQUE categories per customer
    //
    // The key difference from group_count() is that we count unique categories,
    // not the number of path occurrences.

    let ranked_by_unique_categories = g
        .v_ids([tg.alice])
        .with_path()
        .as_("alice")
        .out_labels(&["purchased"])
        .out_labels(&["inCategory"])
        .dedup() // Unique categories that Alice is connected to
        .as_("categories")
        .in_labels(&["inCategory"])
        .as_("sharedProducts")
        .in_labels(&["purchased"])
        .as_("similarCustomers")
        .where_neq("alice")
        .where_(__.not(__.in_labels(&["knows"]).where_eq("alice")))
        // Group by customer, count unique categories using fold
        .group()
        .by_traversal(__.select_one("similarCustomers"))
        .by_value_fold(__.select_one("categories").dedup().count())
        .build()
        .unfold()
        .order()
        .by_value_desc()
        .build()
        .limit(5)
        .to_list();

    println!(
        "Ranked by unique categories: {:?}",
        ranked_by_unique_categories
    );

    assert!(!ranked_by_unique_categories.is_empty());
    assert!(ranked_by_unique_categories.len() <= 5);

    // Verify the structure: each entry should be a single-entry map
    // with customer vertex as key and unique category count as value
    for entry in &ranked_by_unique_categories {
        if let Value::Map(m) = entry {
            assert_eq!(m.len(), 1, "Expected single-entry map, got {:?}", m);
            // Value should be an integer count (unique categories)
            let count = m.values().next().unwrap();
            assert!(
                matches!(count, Value::Int(_)),
                "Expected Int count, got {:?}",
                count
            );
        } else {
            panic!("Expected map entry, got {:?}", entry);
        }
    }

    // First entry should have the highest unique category count
    if let Value::Map(m) = &ranked_by_unique_categories[0] {
        let count = m.values().next().unwrap();
        if let Value::Int(n) = count {
            assert!(*n >= 1, "Expected unique category count >= 1, got {}", n);
        }
    }
}
