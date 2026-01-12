//! Anonymous Traversals and Predicates Example
//!
//! This example demonstrates two powerful features for building complex queries:
//!
//! **Predicates (`p::` module)** - Value testing functions for `has_where()`:
//! - Comparison: `eq`, `neq`, `lt`, `lte`, `gt`, `gte`
//! - Range: `between`, `inside`, `outside`
//! - Collection: `within`, `without`
//! - String: `containing`, `starting_with`, `ending_with`, `regex`
//! - Logical: `and`, `or`, `not`
//!
//! **Anonymous Traversals (`__::` module)** - Reusable traversal fragments:
//! - Navigation: `out`, `in_`, `both`, `out_e`, `in_e`, `both_e`, etc.
//! - Filtering: `has_label`, `has`, `has_value`, `has_where`, `filter`, `dedup`, `limit`
//! - Transform: `values`, `id`, `label`, `map`, `constant`
//!
//! Run with: `cargo run --example anonymous_predicates`

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::traversal::p;
use intersteller::traversal::__;
use intersteller::value::{Value, VertexId};
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    println!("=== Intersteller Anonymous Traversals and Predicates Example ===\n");

    // Create test graph
    let (graph, vertices) = create_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    println!("Graph structure:");
    println!("  People: Alice (30), Bob (25), Charlie (35), Diana (28), Eve (42)");
    println!("  Companies: Acme Corp, TechStart");
    println!("  Software: GraphDB, DataStore");
    println!("  Relationships: knows, works_at, uses, created");
    println!();

    // =========================================================================
    // PART 1: PREDICATES
    // =========================================================================
    println!("========== PART 1: PREDICATES ==========\n");

    // -------------------------------------------------------------------------
    // Comparison Predicates
    // -------------------------------------------------------------------------
    println!("--- Comparison Predicates ---");

    // p::eq() - Equal to
    let age_30 = g.v().has_where("age", p::eq(30i64)).to_list();
    println!("p::eq(30) - People aged 30: {} found", age_30.len());

    // p::neq() - Not equal to
    let not_30 = g
        .v()
        .has_label("person")
        .has_where("age", p::neq(30i64))
        .to_list();
    println!("p::neq(30) - People NOT aged 30: {} found", not_30.len());

    // p::lt() - Less than
    let under_30 = g
        .v()
        .has_label("person")
        .has_where("age", p::lt(30i64))
        .to_list();
    println!("p::lt(30) - People under 30: {} found", under_30.len());

    // p::lte() - Less than or equal
    let at_most_30 = g
        .v()
        .has_label("person")
        .has_where("age", p::lte(30i64))
        .to_list();
    println!(
        "p::lte(30) - People 30 or younger: {} found",
        at_most_30.len()
    );

    // p::gt() - Greater than
    let over_30 = g
        .v()
        .has_label("person")
        .has_where("age", p::gt(30i64))
        .to_list();
    println!("p::gt(30) - People over 30: {} found", over_30.len());

    // p::gte() - Greater than or equal
    let at_least_30 = g
        .v()
        .has_label("person")
        .has_where("age", p::gte(30i64))
        .to_list();
    println!(
        "p::gte(30) - People 30 or older: {} found",
        at_least_30.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // Range Predicates
    // -------------------------------------------------------------------------
    println!("--- Range Predicates ---");

    // p::between() - Inclusive start, exclusive end [start, end)
    let between_25_35 = g
        .v()
        .has_label("person")
        .has_where("age", p::between(25i64, 35i64))
        .to_list();
    println!(
        "p::between(25, 35) - Age in [25, 35): {} found",
        between_25_35.len()
    );

    // p::inside() - Exclusive both ends (start, end)
    let inside_25_35 = g
        .v()
        .has_label("person")
        .has_where("age", p::inside(25i64, 35i64))
        .to_list();
    println!(
        "p::inside(25, 35) - Age in (25, 35): {} found",
        inside_25_35.len()
    );

    // p::outside() - Outside the range
    let outside_28_35 = g
        .v()
        .has_label("person")
        .has_where("age", p::outside(28i64, 35i64))
        .to_list();
    println!(
        "p::outside(28, 35) - Age < 28 or > 35: {} found",
        outside_28_35.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // Collection Predicates
    // -------------------------------------------------------------------------
    println!("--- Collection Predicates ---");

    // p::within() - Value is in set
    let specific_ages = g
        .v()
        .has_label("person")
        .has_where("age", p::within([25i64, 30i64, 42i64]))
        .to_list();
    println!(
        "p::within([25, 30, 42]) - Specific ages: {} found",
        specific_ages.len()
    );

    // p::without() - Value is NOT in set
    let exclude_ages = g
        .v()
        .has_label("person")
        .has_where("age", p::without([25i64, 30i64]))
        .to_list();
    println!(
        "p::without([25, 30]) - Excluding ages: {} found",
        exclude_ages.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // String Predicates
    // -------------------------------------------------------------------------
    println!("--- String Predicates ---");

    // p::containing() - String contains substring
    let names_with_a = g
        .v()
        .has_label("person")
        .has_where("name", p::containing("a"))
        .to_list();
    println!(
        "p::containing(\"a\") - Names containing 'a': {} found",
        names_with_a.len()
    );

    // p::starting_with() - String starts with prefix
    let names_start_a = g
        .v()
        .has_label("person")
        .has_where("name", p::starting_with("A"))
        .to_list();
    println!(
        "p::starting_with(\"A\") - Names starting with 'A': {} found",
        names_start_a.len()
    );

    // p::ending_with() - String ends with suffix
    let names_end_e = g
        .v()
        .has_label("person")
        .has_where("name", p::ending_with("e"))
        .to_list();
    println!(
        "p::ending_with(\"e\") - Names ending with 'e': {} found",
        names_end_e.len()
    );

    // p::regex() - Regular expression match
    let names_regex = g
        .v()
        .has_label("person")
        .has_where("name", p::regex(r"^[A-C].*"))
        .to_list();
    println!(
        "p::regex(r\"^[A-C].*\") - Names starting with A-C: {} found",
        names_regex.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // Logical Composition
    // -------------------------------------------------------------------------
    println!("--- Logical Composition ---");

    // p::and() - Both predicates must match
    let young_adults = g
        .v()
        .has_label("person")
        .has_where("age", p::and(p::gte(25i64), p::lt(35i64)))
        .to_list();
    println!(
        "p::and(gte(25), lt(35)) - Young adults [25-35): {} found",
        young_adults.len()
    );

    // p::or() - Either predicate matches
    let young_or_senior = g
        .v()
        .has_label("person")
        .has_where("age", p::or(p::lt(28i64), p::gt(40i64)))
        .to_list();
    println!(
        "p::or(lt(28), gt(40)) - Young (<28) or senior (>40): {} found",
        young_or_senior.len()
    );

    // p::not() - Negate a predicate
    let not_thirty = g
        .v()
        .has_label("person")
        .has_where("age", p::not(p::eq(30i64)))
        .to_list();
    println!(
        "p::not(eq(30)) - Not exactly 30: {} found",
        not_thirty.len()
    );

    // Complex composition: (age >= 25 AND age <= 35) OR age == 42
    let complex = g
        .v()
        .has_label("person")
        .has_where(
            "age",
            p::or(p::and(p::gte(25i64), p::lte(35i64)), p::eq(42i64)),
        )
        .to_list();
    println!(
        "p::or(and(gte(25), lte(35)), eq(42)) - Complex: {} found",
        complex.len()
    );
    println!();

    // =========================================================================
    // PART 2: ANONYMOUS TRAVERSALS
    // =========================================================================
    println!("========== PART 2: ANONYMOUS TRAVERSALS ==========\n");

    // -------------------------------------------------------------------------
    // Creating Anonymous Traversals
    // -------------------------------------------------------------------------
    println!("--- Creating Anonymous Traversals ---");

    // Simple anonymous traversal - navigate out (inline)
    let neighbors = g.v_ids([vertices.alice]).append(__::out()).to_list();
    println!("__::out() - Alice's neighbors: {} found", neighbors.len());

    // Anonymous traversal with filter (inline)
    let people = g.v().append(__::has_label("person")).to_list();
    println!("__::has_label(\"person\") - People: {} found", people.len());

    // Anonymous traversal with transform (inline)
    let all_names = g
        .v()
        .has_label("person")
        .append(__::values("name"))
        .to_list();
    println!(
        "__::values(\"name\") - Names collected: {} values",
        all_names.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // Chaining Anonymous Traversal Steps
    // -------------------------------------------------------------------------
    println!("--- Chaining Anonymous Traversal Steps ---");

    // Chain multiple steps in anonymous traversal (inline)
    let alice_knows = g
        .v_ids([vertices.alice])
        .append(__::out_labels(&["knows"]).has_label("person"))
        .to_list();
    println!(
        "__::out_labels([\"knows\"]).has_label(\"person\") - Alice knows: {} people",
        alice_knows.len()
    );

    // Complex chain: navigate -> filter -> transform (inline)
    let alice_coworkers = g
        .v_ids([vertices.alice])
        .append(
            __::out_labels(&["works_at"])
                .in_labels(&["works_at"])
                .has_label("person")
                .dedup()
                .values("name"),
        )
        .to_list();
    println!(
        "Coworker names via anonymous traversal: {:?}",
        alice_coworkers
    );
    println!();

    // -------------------------------------------------------------------------
    // Reusable Traversal Fragments
    // -------------------------------------------------------------------------
    println!("--- Reusable Traversal Fragments ---");

    // Define a reusable fragment for "adults" (age >= 18)
    // Note: This is stored in a variable to demonstrate reuse
    let adults_fragment = __::has_label("person").has_where("age", p::gte(18i64));

    // Use the fragment from different starting points
    let all_adults = g.v().append(adults_fragment.clone()).to_list();
    println!(
        "Adults in entire graph: {} (using reusable fragment)",
        all_adults.len()
    );

    // Define a "friend of friend" fragment for reuse
    let friend_of_friend = __::out_labels(&["knows"]).out_labels(&["knows"]).dedup();

    let alice_fof = g
        .v_ids([vertices.alice])
        .append(friend_of_friend.clone())
        .to_list();
    println!(
        "Alice's friends-of-friends: {} (using reusable fragment)",
        alice_fof.len()
    );

    let bob_fof = g
        .v_ids([vertices.bob])
        .append(friend_of_friend.clone())
        .to_list();
    println!(
        "Bob's friends-of-friends: {} (reusing same fragment)",
        bob_fof.len()
    );
    println!();

    // =========================================================================
    // PART 3: COMBINING PREDICATES WITH ANONYMOUS TRAVERSALS
    // =========================================================================
    println!("========== PART 3: COMBINING BOTH ==========\n");

    // -------------------------------------------------------------------------
    // Predicates in Anonymous Traversals
    // -------------------------------------------------------------------------
    println!("--- Predicates in Anonymous Traversals ---");

    // Anonymous traversal with has_where predicate (inline)
    let alice_senior_colleagues = g
        .v_ids([vertices.alice])
        .append(
            __::out_labels(&["works_at"])
                .in_labels(&["works_at"])
                .has_label("person")
                .has_where("age", p::gte(30i64))
                .dedup(),
        )
        .to_list();
    println!(
        "Alice's senior colleagues (age >= 30): {} found",
        alice_senior_colleagues.len()
    );

    // Find experienced users of software (inline)
    let graphdb_experienced = g
        .v_ids([vertices.graph_db])
        .append(
            __::in_labels(&["uses"])
                .has_label("person")
                .has_where("age", p::gt(28i64)),
        )
        .to_list();
    println!(
        "GraphDB experienced users (age > 28): {} found",
        graphdb_experienced.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // Complex Query Patterns
    // -------------------------------------------------------------------------
    println!("--- Complex Query Patterns ---");

    // Pattern: Find people who work with other young professionals (inline)
    let charlie_young_colleagues = g
        .v_ids([vertices.charlie])
        .append(
            __::out_labels(&["works_at"])
                .in_labels(&["works_at"])
                .has_label("person")
                .has_where("age", p::and(p::gte(20i64), p::lt(30i64)))
                .dedup()
                .values("name"),
        )
        .to_list();
    println!(
        "Charlie's young colleagues (20-29): {:?}",
        charlie_young_colleagues
    );

    // Pattern: Software used by people whose names start with vowels (inline)
    let graphdb_vowel_users = g
        .v_ids([vertices.graph_db])
        .append(
            __::in_labels(&["uses"])
                .has_label("person")
                .has_where("name", p::regex(r"^[AEIOU].*")),
        )
        .to_list();
    println!(
        "GraphDB users with vowel names: {} found",
        graphdb_vowel_users.len()
    );

    // Pattern: Find companies with employees in specific age range (inline)
    let companies_with_midcareer = g
        .v()
        .has_label("company")
        .append(
            __::in_labels(&["works_at"])
                .has_label("person")
                .has_where("age", p::between(28i64, 40i64))
                .dedup(),
        )
        .to_list();
    println!(
        "Companies with mid-career employees (28-39): {} employees found",
        companies_with_midcareer.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // Building Query DSL with Anonymous Traversals
    // -------------------------------------------------------------------------
    println!("--- Building Query DSL ---");

    // Helper function pattern (inline for demo)
    fn young_adults_at_company() -> intersteller::traversal::Traversal<Value, Value> {
        __::in_labels(&["works_at"])
            .has_label("person")
            .has_where("age", p::and(p::gte(25i64), p::lt(35i64)))
    }

    let acme_young_adults = g
        .v_ids([vertices.acme])
        .append(young_adults_at_company())
        .to_list();
    println!(
        "Acme Corp young adults (25-34): {} found",
        acme_young_adults.len()
    );

    let techstart_young_adults = g
        .v_ids([vertices.techstart])
        .append(young_adults_at_company())
        .to_list();
    println!(
        "TechStart young adults (25-34): {} found",
        techstart_young_adults.len()
    );
    println!();

    // -------------------------------------------------------------------------
    // Practical Examples
    // -------------------------------------------------------------------------
    println!("--- Practical Examples ---");

    // Example 1: Find mentoring pairs (senior to junior)
    // Seniors: age >= 35, Juniors: age < 28
    let mentoring_targets = g
        .v()
        .append(
            __::has_label("person")
                .has_where("age", p::gte(35i64))
                .out_labels(&["knows"])
                .has_label("person")
                .has_where("age", p::lt(28i64)),
        )
        .to_list();
    println!(
        "Potential mentees (juniors known by seniors): {} found",
        mentoring_targets.len()
    );

    // Example 2: Find software used by multiple age groups (inline)
    let mixed_age_users = g
        .v()
        .has_label("software")
        .append(
            __::in_labels(&["uses"])
                .has_label("person")
                .has_where("age", p::or(p::lt(30i64), p::gte(35i64))),
        )
        .dedup()
        .to_list();
    println!(
        "Software users (young <30 OR senior >=35): {} found",
        mixed_age_users.len()
    );

    // Example 3: Network analysis - find well-connected people
    // (people who know at least 2 others)
    let knows_count = g
        .v()
        .has_label("person")
        .map(|ctx, v| {
            // Count outgoing "knows" edges for each person
            let vid = v.as_vertex_id().unwrap();
            let count = ctx
                .snapshot()
                .traversal()
                .v_ids([vid])
                .out_labels(&["knows"])
                .count();
            Value::Int(count as i64)
        })
        .filter(|_ctx, v| matches!(v, Value::Int(n) if *n >= 2))
        .to_list();
    println!(
        "Well-connected people (know >= 2 others): {} found",
        knows_count.len()
    );
    println!();

    println!("=== Example Complete ===");
}

/// Vertex IDs for easy reference
struct VertexIds {
    alice: VertexId,
    bob: VertexId,
    charlie: VertexId,
    #[allow(dead_code)]
    diana: VertexId,
    #[allow(dead_code)]
    eve: VertexId,
    acme: VertexId,
    techstart: VertexId,
    graph_db: VertexId,
    #[allow(dead_code)]
    datastore: VertexId,
}

/// Create a test graph with people, companies, and software
fn create_test_graph() -> (Graph, VertexIds) {
    let mut storage = InMemoryGraph::new();

    // Add person vertices with varying ages
    let alice = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        props.insert("age".to_string(), Value::Int(30));
        props.insert(
            "department".to_string(),
            Value::String("Engineering".to_string()),
        );
        props
    });

    let bob = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));
        props.insert("age".to_string(), Value::Int(25));
        props.insert(
            "department".to_string(),
            Value::String("Engineering".to_string()),
        );
        props
    });

    let charlie = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Charlie".to_string()));
        props.insert("age".to_string(), Value::Int(35));
        props.insert(
            "department".to_string(),
            Value::String("Research".to_string()),
        );
        props
    });

    let diana = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Diana".to_string()));
        props.insert("age".to_string(), Value::Int(28));
        props.insert(
            "department".to_string(),
            Value::String("Engineering".to_string()),
        );
        props
    });

    let eve = storage.add_vertex("person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Eve".to_string()));
        props.insert("age".to_string(), Value::Int(42));
        props.insert(
            "department".to_string(),
            Value::String("Management".to_string()),
        );
        props
    });

    // Add company vertices
    let acme = storage.add_vertex("company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Acme Corp".to_string()));
        props.insert("employees".to_string(), Value::Int(500));
        props.insert(
            "industry".to_string(),
            Value::String("Technology".to_string()),
        );
        props
    });

    let techstart = storage.add_vertex("company", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("TechStart".to_string()));
        props.insert("employees".to_string(), Value::Int(50));
        props.insert("industry".to_string(), Value::String("Startup".to_string()));
        props
    });

    // Add software vertices
    let graph_db = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("GraphDB".to_string()));
        props.insert("version".to_string(), Value::Float(2.0));
        props.insert("language".to_string(), Value::String("Rust".to_string()));
        props
    });

    let datastore = storage.add_vertex("software", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("DataStore".to_string()));
        props.insert("version".to_string(), Value::Float(1.5));
        props.insert("language".to_string(), Value::String("Go".to_string()));
        props
    });

    // Add "knows" relationships (social network)
    storage
        .add_edge(alice, bob, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, diana, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, eve, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, eve, "knows", HashMap::new())
        .unwrap();

    // Add "works_at" relationships
    storage
        .add_edge(alice, acme, "works_at", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, acme, "works_at", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, acme, "works_at", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, techstart, "works_at", HashMap::new())
        .unwrap();
    storage
        .add_edge(eve, techstart, "works_at", HashMap::new())
        .unwrap();

    // Add "uses" relationships (who uses what software)
    storage
        .add_edge(alice, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, datastore, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(eve, graph_db, "uses", HashMap::new())
        .unwrap();
    storage
        .add_edge(eve, datastore, "uses", HashMap::new())
        .unwrap();

    // Add "created" relationships
    storage
        .add_edge(alice, graph_db, "created", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, datastore, "created", HashMap::new())
        .unwrap();

    (
        Graph::new(Arc::new(storage)),
        VertexIds {
            alice,
            bob,
            charlie,
            diana,
            eve,
            acme,
            techstart,
            graph_db,
            datastore,
        },
    )
}
