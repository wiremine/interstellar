//! British Royal Family Graph Example - GQL Query Version
//!
//! This example demonstrates graph queries on a family tree dataset
//! using GQL (Graph Query Language) - a declarative SQL-like syntax.
//!
//! This is a companion to `british_royals.rs` which uses the programmatic
//! traversal API. Both examples use the same dataset but demonstrate
//! different query approaches.
//!
//! The dataset includes:
//! - Person vertices with properties: name, birth_date, death_date, house, is_monarch, etc.
//! - Relationship edges: parent_of, child_of, married_to
//!
//! GQL Features Demonstrated:
//! - Basic MATCH patterns with property filters
//! - WHERE clause with comparisons (<, >, =, <>, IN, CONTAINS, etc.)
//! - Edge traversal patterns (-[:label]-> and <-[:label]-)
//! - Variable-length paths for ancestry queries (*1..4)
//! - ORDER BY, LIMIT, OFFSET
//! - Aggregate functions: COUNT, COLLECT
//! - GROUP BY for grouping and summarization
//! - CASE expressions for conditional logic
//! - OPTIONAL MATCH for outer-join-like behavior
//! - UNION for combining query results
//! - Introspection: id(), labels(), properties()
//!
//! Run with: `cargo run --example british_royals_gql`

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::value::Value;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

// =============================================================================
// Data Loading (identical to british_royals.rs)
// =============================================================================

/// Load the British Royals JSON fixture and build the graph.
fn load_royals_graph() -> Graph {
    let json_str = fs::read_to_string("examples/fixtures/british_royals.json")
        .expect("Failed to read british_royals.json");
    let data: JsonValue = serde_json::from_str(&json_str).expect("Failed to parse JSON");

    let mut storage = InMemoryGraph::new();
    let mut person_ids: HashMap<String, intersteller::value::VertexId> = HashMap::new();

    // Load all persons as vertices
    if let Some(persons) = data["persons"].as_array() {
        for person in persons {
            let json_id = person["id"].as_str().unwrap_or("unknown");

            let mut props = HashMap::new();

            // Required string properties
            if let Some(v) = person["name"].as_str() {
                props.insert("name".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["full_name"].as_str() {
                props.insert("full_name".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["house"].as_str() {
                props.insert("house".to_string(), Value::String(v.to_string()));
            }

            // Date properties (stored as strings for simplicity)
            if let Some(v) = person["birth_date"].as_str() {
                props.insert("birth_date".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["birth_country"].as_str() {
                props.insert("birth_country".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["birth_city"].as_str() {
                props.insert("birth_city".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["death_date"].as_str() {
                props.insert("death_date".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["death_country"].as_str() {
                props.insert("death_country".to_string(), Value::String(v.to_string()));
            }

            // Boolean properties
            if let Some(v) = person["is_monarch"].as_bool() {
                props.insert("is_monarch".to_string(), Value::Bool(v));
            }
            if let Some(v) = person["abdicated"].as_bool() {
                props.insert("abdicated".to_string(), Value::Bool(v));
            }

            // Reign dates for monarchs
            if let Some(v) = person["reign_start"].as_str() {
                props.insert("reign_start".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = person["reign_end"].as_str() {
                props.insert("reign_end".to_string(), Value::String(v.to_string()));
            }

            // Titles as a list
            if let Some(titles) = person["titles"].as_array() {
                let title_values: Vec<Value> = titles
                    .iter()
                    .filter_map(|t| t.as_str().map(|s| Value::String(s.to_string())))
                    .collect();
                if !title_values.is_empty() {
                    props.insert("titles".to_string(), Value::List(title_values));
                }
            }

            // Store original JSON ID for lookups
            props.insert("json_id".to_string(), Value::String(json_id.to_string()));

            let vid = storage.add_vertex("person", props);
            person_ids.insert(json_id.to_string(), vid);
        }
    }

    // Load parent-child relationships
    if let Some(relations) = data["parent_child"].as_array() {
        for rel in relations {
            let parent_json_id = rel["parent_id"].as_str().unwrap_or("");
            let child_json_id = rel["child_id"].as_str().unwrap_or("");
            let relationship = rel["relationship"].as_str().unwrap_or("");

            if let (Some(&parent_vid), Some(&child_vid)) = (
                person_ids.get(parent_json_id),
                person_ids.get(child_json_id),
            ) {
                // parent_of edge from parent to child
                let mut props = HashMap::new();
                props.insert(
                    "relationship".to_string(),
                    Value::String(relationship.to_string()),
                );
                let _ = storage.add_edge(parent_vid, child_vid, "parent_of", props.clone());

                // child_of edge from child to parent (for reverse traversal)
                let _ = storage.add_edge(child_vid, parent_vid, "child_of", props);
            }
        }
    }

    // Load marriages
    if let Some(marriages) = data["marriages"].as_array() {
        for marriage in marriages {
            let p1_json_id = marriage["person1_id"].as_str().unwrap_or("");
            let p2_json_id = marriage["person2_id"].as_str().unwrap_or("");

            if let (Some(&p1_vid), Some(&p2_vid)) =
                (person_ids.get(p1_json_id), person_ids.get(p2_json_id))
            {
                let mut props = HashMap::new();
                if let Some(date) = marriage["marriage_date"].as_str() {
                    props.insert("marriage_date".to_string(), Value::String(date.to_string()));
                }
                if let Some(city) = marriage["marriage_city"].as_str() {
                    props.insert("marriage_city".to_string(), Value::String(city.to_string()));
                }
                if let Some(reason) = marriage["end_reason"].as_str() {
                    props.insert("end_reason".to_string(), Value::String(reason.to_string()));
                }
                if let Some(divorce) = marriage["divorce_date"].as_str() {
                    props.insert(
                        "divorce_date".to_string(),
                        Value::String(divorce.to_string()),
                    );
                }

                // Bidirectional marriage edges
                let _ = storage.add_edge(p1_vid, p2_vid, "married_to", props.clone());
                let _ = storage.add_edge(p2_vid, p1_vid, "married_to", props);
            }
        }
    }

    let storage = Arc::new(storage);
    Graph::new(storage)
}

// =============================================================================
// Helper Functions
// =============================================================================

fn print_section(title: &str) {
    println!("\n{}", "=".repeat(70));
    println!("{}", title);
    println!("{}", "=".repeat(70));
}

fn print_query(description: &str, gql: &str) {
    println!("\n--- {} ---", description);
    // Print the GQL query in a readable format
    let trimmed: Vec<&str> = gql
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();
    println!("GQL: {}", trimmed.join(" "));
}

fn format_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => format!("{:.1}", f),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::List(items) => {
            let formatted: Vec<String> = items.iter().map(format_value).collect();
            format!("[{}]", formatted.join(", "))
        }
        Value::Map(map) => {
            let pairs: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
        Value::Vertex(vid) => format!("v[{:?}]", vid),
        Value::Edge(eid) => format!("e[{:?}]", eid),
    }
}

fn format_names(results: &[Value]) -> String {
    if results.is_empty() {
        return "(none)".to_string();
    }
    results
        .iter()
        .map(format_value)
        .collect::<Vec<_>>()
        .join(", ")
}

fn run_query(snapshot: &intersteller::graph::GraphSnapshot, gql: &str) -> Vec<Value> {
    match snapshot.gql(gql) {
        Ok(results) => results,
        Err(e) => {
            eprintln!("Query error: {}", e);
            vec![]
        }
    }
}

// =============================================================================
// Query Demonstrations
// =============================================================================

fn main() {
    println!("=== British Royal Family Graph Database - GQL Example ===");
    println!("Loading data from examples/fixtures/british_royals.json...\n");

    let graph = load_royals_graph();
    let snapshot = graph.snapshot();

    // Report graph statistics
    let vertex_count = run_query(&snapshot, "MATCH (n) RETURN count(*)");
    let edge_count = run_query(&snapshot, "MATCH ()-[e]->() RETURN count(*)");
    println!("Graph loaded successfully!");
    println!("  Vertices: {}", format_value(&vertex_count[0]));
    println!("  Edges: {}", format_value(&edge_count[0]));

    // =========================================================================
    // SECTION 1: Basic Queries
    // =========================================================================
    print_section("1. BASIC QUERIES");

    // Query 1: Find all monarchs
    let q1 = "MATCH (p:person) WHERE p.is_monarch = true RETURN p.name";
    print_query("Find all British monarchs", q1);
    let monarchs = run_query(&snapshot, q1);
    println!("Monarchs ({}): {}", monarchs.len(), format_names(&monarchs));

    // Query 2: Find living royals (no death_date)
    let q2 = "MATCH (p:person) WHERE p.death_date IS NULL RETURN p.name";
    print_query("Find living royals (death_date IS NULL)", q2);
    let living = run_query(&snapshot, q2);
    println!(
        "Living royals ({}): {}",
        living.len(),
        format_names(&living)
    );

    // Query 3: Find members of House Windsor
    let q3 = "MATCH (p:person {house: 'Windsor'}) RETURN p.name";
    print_query("Find members of House Windsor", q3);
    let windsor = run_query(&snapshot, q3);
    println!(
        "House Windsor ({}): {}",
        windsor.len(),
        format_names(&windsor)
    );

    // =========================================================================
    // SECTION 2: Navigation Queries (Edge Traversal)
    // =========================================================================
    print_section("2. NAVIGATION QUERIES (Edge Traversal)");

    // Query 4: Find Elizabeth II's children
    let q4 = r#"
        MATCH (p:person {name: 'Elizabeth II'})-[:parent_of]->(child:person)
        RETURN child.name
    "#;
    print_query("Find Elizabeth II's children", q4);
    let elizabeth_children = run_query(&snapshot, q4);
    println!(
        "Elizabeth II's children: {}",
        format_names(&elizabeth_children)
    );

    // Query 5: Find Prince William's parents
    let q5 = r#"
        MATCH (p:person {name: 'Prince William'})-[:child_of]->(parent:person)
        RETURN parent.name
    "#;
    print_query("Find Prince William's parents", q5);
    let william_parents = run_query(&snapshot, q5);
    println!(
        "Prince William's parents: {}",
        format_names(&william_parents)
    );

    // Query 6: Find Charles III's spouses
    let q6 = r#"
        MATCH (p:person {name: 'Charles III'})-[:married_to]->(spouse:person)
        RETURN spouse.name
    "#;
    print_query("Find Charles III's spouses", q6);
    let charles_spouses = run_query(&snapshot, q6);
    println!("Charles III's spouses: {}", format_names(&charles_spouses));

    // Query 7: Find all grandchildren of Elizabeth II
    let q7 = r#"
        MATCH (p:person {name: 'Elizabeth II'})-[:parent_of]->()-[:parent_of]->(grandchild:person)
        RETURN DISTINCT grandchild.name
    "#;
    print_query("Find all grandchildren of Elizabeth II", q7);
    let grandchildren = run_query(&snapshot, q7);
    println!(
        "Elizabeth II's grandchildren ({}): {}",
        grandchildren.len(),
        format_names(&grandchildren)
    );

    // Query 7b: Get marriage edge details (edge property access)
    let q7b = r#"
        MATCH (p:person {name: 'Charles III'})-[m:married_to]->(spouse:person)
        RETURN spouse.name, m.marriage_date
    "#;
    print_query("Find Charles III's spouses with marriage dates", q7b);
    let marriages = run_query(&snapshot, q7b);
    println!("Charles III's marriages:");
    for m in &marriages {
        if let Value::Map(map) = m {
            let name = format_value(map.get("spouse.name").unwrap_or(&Value::Null));
            let date = format_value(map.get("m.marriage_date").unwrap_or(&Value::Null));
            println!("  {} (married: {})", name, date);
        }
    }

    // =========================================================================
    // SECTION 3: Predicate/WHERE Clause Queries
    // =========================================================================
    print_section("3. WHERE CLAUSE QUERIES");

    // Query 8: Find royals born before 1900
    let q8 = r#"
        MATCH (p:person)
        WHERE p.birth_date < '1900-01-01'
        RETURN p.name, p.birth_date
    "#;
    print_query("Find royals born before 1900", q8);
    let born_before_1900 = run_query(&snapshot, q8);
    println!("Born before 1900 ({}):", born_before_1900.len());
    for r in &born_before_1900 {
        if let Value::Map(map) = r {
            println!(
                "  {} (born: {})",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.birth_date").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 9: Find royals from specific houses using IN
    let q9 = r#"
        MATCH (p:person)
        WHERE p.house IN ['Hanover', 'Saxe-Coburg and Gotha']
        RETURN p.name, p.house
    "#;
    print_query(
        "Find royals from House Hanover or Saxe-Coburg and Gotha",
        q9,
    );
    let old_houses = run_query(&snapshot, q9);
    println!("Old royal houses ({}):", old_houses.len());
    for r in &old_houses {
        if let Value::Map(map) = r {
            println!(
                "  {} (house: {})",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.house").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 10: Find royals with "Elizabeth" in their name using CONTAINS
    let q10 = r#"
        MATCH (p:person)
        WHERE p.name CONTAINS 'Elizabeth'
        RETURN p.name
    "#;
    print_query("Find royals with 'Elizabeth' in their name", q10);
    let elizabeths = run_query(&snapshot, q10);
    println!(
        "Names containing 'Elizabeth': {}",
        format_names(&elizabeths)
    );

    // Query 11: Find royals NOT born in England
    let q11 = r#"
        MATCH (p:person)
        WHERE p.birth_country <> 'England' AND p.birth_country IS NOT NULL
        RETURN p.name, p.birth_country
    "#;
    print_query("Find royals NOT born in England", q11);
    let not_english_born = run_query(&snapshot, q11);
    println!("Not born in England ({}):", not_english_born.len());
    for r in &not_english_born {
        if let Value::Map(map) = r {
            println!(
                "  {} (born in: {})",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.birth_country").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 12: Count Windsor house members
    let q12 = r#"
        MATCH (p:person)
        WHERE p.house = 'Windsor'
        RETURN count(*)
    "#;
    print_query("Count Windsor house members", q12);
    let windsor_count = run_query(&snapshot, q12);
    println!(
        "Count of Windsor house members: {}",
        format_value(&windsor_count[0])
    );

    // =========================================================================
    // SECTION 4: EXISTS Subqueries (Anonymous Traversal Equivalent)
    // =========================================================================
    print_section("4. EXISTS SUBQUERIES");

    // Query 13: Find royals who have children (using EXISTS)
    let q13 = r#"
        MATCH (p:person)
        WHERE EXISTS { (p)-[:parent_of]->() }
        RETURN p.name
    "#;
    print_query(
        "Find royals who are parents (have outgoing parent_of edges)",
        q13,
    );
    let parents = run_query(&snapshot, q13);
    println!(
        "Royals with children ({}): {}",
        parents.len(),
        format_names(&parents)
    );

    // Query 14: Find royals who have NO children (using NOT EXISTS)
    let q14 = r#"
        MATCH (p:person)
        WHERE NOT EXISTS { (p)-[:parent_of]->() }
        RETURN p.name
    "#;
    print_query("Find royals without children (NOT EXISTS)", q14);
    let childless = run_query(&snapshot, q14);
    println!(
        "Royals without children ({}): {}",
        childless.len(),
        format_names(&childless)
    );

    // Query 15: Find monarchs who were married
    let q15 = r#"
        MATCH (p:person)
        WHERE p.is_monarch = true AND EXISTS { (p)-[:married_to]->() }
        RETURN p.name
    "#;
    print_query("Find monarchs who were married", q15);
    let married_monarchs = run_query(&snapshot, q15);
    println!("Married monarchs: {}", format_names(&married_monarchs));

    // =========================================================================
    // SECTION 5: UNION Queries (Branch Step Equivalent)
    // =========================================================================
    print_section("5. UNION QUERIES");

    // Query 16: Get both parents AND children of Charles III (using UNION)
    let q16 = r#"
        MATCH (p:person {name: 'Charles III'})-[:child_of]->(relative:person)
        RETURN relative.name AS name
        UNION
        MATCH (p:person {name: 'Charles III'})-[:parent_of]->(relative:person)
        RETURN relative.name AS name
    "#;
    print_query("Get Charles III's parents AND children (using UNION)", q16);
    let charles_family = run_query(&snapshot, q16);
    println!(
        "Charles III's parents and children: {}",
        format_names(&charles_family)
    );

    // =========================================================================
    // SECTION 6: CASE Expressions (Choose Step Equivalent)
    // =========================================================================
    print_section("6. CASE EXPRESSIONS");

    // Query 17: Different output based on monarch status
    let q17 = r#"
        MATCH (p:person)
        WHERE p.name = 'Elizabeth II'
        RETURN p.name,
            CASE
                WHEN p.is_monarch = true THEN p.reign_start
                ELSE p.birth_date
            END AS date_info
    "#;
    print_query(
        "Conditional: If monarch, return reign_start; else return birth_date",
        q17,
    );
    let conditional = run_query(&snapshot, q17);
    println!("Elizabeth II (monarch path): {:?}", conditional);

    let q17b = r#"
        MATCH (p:person)
        WHERE p.name = 'Prince Philip'
        RETURN p.name,
            CASE
                WHEN p.is_monarch = true THEN p.reign_start
                ELSE p.birth_date
            END AS date_info
    "#;
    print_query("Same CASE for Prince Philip (non-monarch)", q17b);
    let conditional2 = run_query(&snapshot, q17b);
    println!("Prince Philip (non-monarch path): {:?}", conditional2);

    // =========================================================================
    // SECTION 7: OPTIONAL MATCH
    // =========================================================================
    print_section("7. OPTIONAL MATCH");

    // Query 18: Get spouse if married, otherwise show person (outer join)
    let q18 = r#"
        MATCH (p:person {name: 'Princess Victoria'})
        OPTIONAL MATCH (p)-[:married_to]->(spouse:person)
        RETURN p.name AS person, spouse.name AS spouse
    "#;
    print_query(
        "Get spouse if married, otherwise null (OPTIONAL MATCH)",
        q18,
    );
    let with_optional = run_query(&snapshot, q18);
    println!("Princess Victoria with optional spouse:");
    for r in &with_optional {
        if let Value::Map(map) = r {
            let person = format_value(map.get("person").unwrap_or(&Value::Null));
            let spouse = format_value(map.get("spouse").unwrap_or(&Value::Null));
            println!("  {} -> spouse: {}", person, spouse);
        }
    }

    // =========================================================================
    // SECTION 8: Variable-Length Paths (Repeat Step Equivalent)
    // =========================================================================
    print_section("8. VARIABLE-LENGTH PATHS (Ancestry Queries)");

    // Query 19: Find ancestors of Prince George (up to 4 generations)
    let q19 = r#"
        MATCH (p:person {name: 'Prince George'})-[:child_of*1..4]->(ancestor:person)
        RETURN DISTINCT ancestor.name
    "#;
    print_query("Find Prince George's ancestors (1-4 generations)", q19);
    let george_ancestors = run_query(&snapshot, q19);
    println!(
        "Prince George's ancestors ({}): {}",
        george_ancestors.len(),
        format_names(&george_ancestors)
    );

    // Query 20: Find all descendants of Queen Victoria (up to 3 generations)
    let q20 = r#"
        MATCH (p:person {name: 'Victoria'})-[:parent_of*1..3]->(descendant:person)
        RETURN DISTINCT descendant.name
    "#;
    print_query("Find Queen Victoria's descendants (1-3 generations)", q20);
    let victoria_descendants = run_query(&snapshot, q20);
    println!(
        "Victoria's descendants - 3 gen ({}): {}",
        victoria_descendants.len(),
        format_names(&victoria_descendants)
    );

    // Query 21: Find Prince William's lineage
    let q21 = r#"
        MATCH (p:person {name: 'Prince William'})-[:child_of*1..3]->(ancestor:person)
        RETURN DISTINCT ancestor.name
    "#;
    print_query("Find Prince William's lineage (up to 3 generations)", q21);
    let william_ancestors = run_query(&snapshot, q21);
    println!(
        "William's lineage ({}): {}",
        william_ancestors.len(),
        format_names(&william_ancestors)
    );

    // =========================================================================
    // SECTION 9: Multi-Hop Patterns with Path Binding
    // =========================================================================
    print_section("9. MULTI-HOP PATTERNS");

    // Query 22: Track parent-child relationship
    let q22 = r#"
        MATCH (child:person {name: 'Prince William'})-[:child_of]->(parent:person)
        RETURN child.name AS child, parent.name AS parent
    "#;
    print_query("Track parent-child with named variables", q22);
    let labeled_path = run_query(&snapshot, q22);
    println!("William -> Parent mappings:");
    for r in &labeled_path {
        if let Value::Map(map) = r {
            let child = format_value(map.get("child").unwrap_or(&Value::Null));
            let parent = format_value(map.get("parent").unwrap_or(&Value::Null));
            println!("  {} -> {}", child, parent);
        }
    }

    // Query 23: Full path from Charles to grandchildren
    let q23 = r#"
        MATCH (g:person {name: 'Charles III'})-[:parent_of]->(c:person)-[:parent_of]->(gc:person)
        RETURN g.name AS grandparent, c.name AS child, gc.name AS grandchild
    "#;
    print_query("Full path: Charles III -> child -> grandchild", q23);
    let path_results = run_query(&snapshot, q23);
    println!("Paths from Charles III to grandchildren:");
    for (i, r) in path_results.iter().enumerate() {
        if let Value::Map(map) = r {
            let gp = format_value(map.get("grandparent").unwrap_or(&Value::Null));
            let c = format_value(map.get("child").unwrap_or(&Value::Null));
            let gc = format_value(map.get("grandchild").unwrap_or(&Value::Null));
            println!("  Path {}: {} -> {} -> {}", i + 1, gp, c, gc);
        }
    }

    // =========================================================================
    // SECTION 10: Complex Combined Queries
    // =========================================================================
    print_section("10. COMPLEX COMBINED QUERIES");

    // Query 24: Find living descendants of Elizabeth II who are NOT monarchs
    let q24 = r#"
        MATCH (e:person {name: 'Elizabeth II'})-[:parent_of*1..3]->(d:person)
        WHERE d.death_date IS NULL AND (d.is_monarch IS NULL OR d.is_monarch = false)
        RETURN DISTINCT d.name
    "#;
    print_query(
        "Living descendants of Elizabeth II who are not monarchs",
        q24,
    );
    let living_non_monarch = run_query(&snapshot, q24);
    println!(
        "Living non-monarch descendants ({}): {}",
        living_non_monarch.len(),
        format_names(&living_non_monarch)
    );

    // Query 25: Find the monarch who abdicated
    let q25 = r#"
        MATCH (p:person)
        WHERE p.is_monarch = true AND p.abdicated = true
        RETURN p.name, p.reign_end
    "#;
    print_query("Find the monarch who abdicated", q25);
    let abdicated = run_query(&snapshot, q25);
    println!("Abdicated monarch:");
    for r in &abdicated {
        if let Value::Map(map) = r {
            println!(
                "  {} (reign ended: {})",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.reign_end").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 26: Count children per monarch
    let q26 = r#"
        MATCH (m:person)-[:parent_of]->(c:person)
        WHERE m.is_monarch = true
        RETURN m.name AS monarch, count(*) AS children
        GROUP BY m.name
    "#;
    print_query("Count children per monarch", q26);
    let monarch_children = run_query(&snapshot, q26);
    println!("Monarchs and their children count:");
    for r in &monarch_children {
        if let Value::Map(map) = r {
            println!(
                "  {}: {} children",
                format_value(map.get("monarch").unwrap_or(&Value::Null)),
                format_value(map.get("children").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 11: ORDER BY and LIMIT
    // =========================================================================
    print_section("11. ORDER BY AND LIMIT");

    // Query 27: Monarchs ordered by reign start date
    let q27 = r#"
        MATCH (p:person)
        WHERE p.is_monarch = true AND p.reign_start IS NOT NULL
        RETURN p.name, p.reign_start
        ORDER BY p.reign_start
    "#;
    print_query("Monarchs ordered by reign start date (ascending)", q27);
    let monarchs_by_reign = run_query(&snapshot, q27);
    println!("Monarchs by reign start:");
    for r in &monarchs_by_reign {
        if let Value::Map(map) = r {
            println!(
                "  {} (reign started: {})",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.reign_start").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 28: Living royals ordered alphabetically (first 10)
    let q28 = r#"
        MATCH (p:person)
        WHERE p.death_date IS NULL
        RETURN p.name
        ORDER BY p.name
        LIMIT 10
    "#;
    print_query("Living royals ordered alphabetically (first 10)", q28);
    let living_ordered = run_query(&snapshot, q28);
    println!("Living royals (first 10 alphabetically):");
    for name in &living_ordered {
        println!("  {}", format_value(name));
    }

    // =========================================================================
    // SECTION 12: Aggregation Queries
    // =========================================================================
    print_section("12. AGGREGATION QUERIES");

    // Query 29: Group royals by house using GROUP BY
    let q29 = r#"
        MATCH (p:person)
        WHERE p.house IS NOT NULL
        RETURN p.house AS house, count(*) AS count
        GROUP BY p.house
        ORDER BY count DESC
    "#;
    print_query("Count royals by house using GROUP BY", q29);
    let house_counts = run_query(&snapshot, q29);
    println!("House counts:");
    for r in &house_counts {
        if let Value::Map(map) = r {
            println!(
                "  {}: {}",
                format_value(map.get("house").unwrap_or(&Value::Null)),
                format_value(map.get("count").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 30: Count royals by birth country
    let q30 = r#"
        MATCH (p:person)
        WHERE p.birth_country IS NOT NULL
        RETURN p.birth_country AS country, count(*) AS count
        GROUP BY p.birth_country
        ORDER BY count DESC
    "#;
    print_query("Count royals by birth country", q30);
    let country_counts = run_query(&snapshot, q30);
    println!("Birth country counts:");
    for r in &country_counts {
        if let Value::Map(map) = r {
            println!(
                "  {}: {}",
                format_value(map.get("country").unwrap_or(&Value::Null)),
                format_value(map.get("count").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 31: Collect names by house
    let q31 = r#"
        MATCH (p:person)
        WHERE p.house IS NOT NULL
        RETURN p.house AS house, collect(p.name) AS members
        GROUP BY p.house
    "#;
    print_query("Collect royal names by house", q31);
    let by_house = run_query(&snapshot, q31);
    println!("Royals grouped by house:");
    for r in &by_house {
        if let Value::Map(map) = r {
            let house = format_value(map.get("house").unwrap_or(&Value::Null));
            let members = format_value(map.get("members").unwrap_or(&Value::Null));
            println!("  {}: {}", house, members);
        }
    }

    // =========================================================================
    // SECTION 13: Introspection Functions
    // =========================================================================
    print_section("13. INTROSPECTION FUNCTIONS");

    // Query 32: Get vertex ID for Elizabeth II
    let q32 = r#"
        MATCH (p:person {name: 'Elizabeth II'})
        RETURN id(p) AS vertex_id, labels(p) AS vertex_labels
    "#;
    print_query("Get Elizabeth II's internal ID and labels", q32);
    let introspection = run_query(&snapshot, q32);
    println!("Elizabeth II introspection:");
    for r in &introspection {
        if let Value::Map(map) = r {
            println!(
                "  ID: {}, Labels: {}",
                format_value(map.get("vertex_id").unwrap_or(&Value::Null)),
                format_value(map.get("vertex_labels").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 33: Get properties of Victoria
    let q33 = r#"
        MATCH (p:person {name: 'Victoria'})
        RETURN properties(p) AS props
    "#;
    print_query("Get all properties of Victoria", q33);
    let props = run_query(&snapshot, q33);
    println!("Victoria's properties:");
    for r in &props {
        // The result is a Map with the properties embedded (single return value)
        if let Value::Map(map) = r {
            for (key, value) in map.iter() {
                println!("  {}: {}", key, format_value(value));
            }
        }
    }

    // Query 34: Get edge type/label
    let q34 = r#"
        MATCH (p:person {name: 'Elizabeth II'})-[e]->(other:person)
        RETURN type(e) AS edge_type, other.name AS connected_to
        LIMIT 5
    "#;
    print_query("Get edge types from Elizabeth II", q34);
    let edge_types = run_query(&snapshot, q34);
    println!("Elizabeth II's edge types:");
    for r in &edge_types {
        if let Value::Map(map) = r {
            println!(
                "  -{}- {}",
                format_value(map.get("edge_type").unwrap_or(&Value::Null)),
                format_value(map.get("connected_to").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // Summary Statistics
    // =========================================================================
    print_section("SUMMARY STATISTICS");

    let total_people = run_query(&snapshot, "MATCH (p:person) RETURN count(*)");
    println!("Total people: {}", format_value(&total_people[0]));

    let total_monarchs = run_query(
        &snapshot,
        "MATCH (p:person) WHERE p.is_monarch = true RETURN count(*)",
    );
    println!("Total monarchs: {}", format_value(&total_monarchs[0]));

    let living_count = run_query(
        &snapshot,
        "MATCH (p:person) WHERE p.death_date IS NULL RETURN count(*)",
    );
    println!("Living royals: {}", format_value(&living_count[0]));

    // Edge counts
    let parent_edges = run_query(&snapshot, "MATCH ()-[e:parent_of]->() RETURN count(*)");
    let child_edges = run_query(&snapshot, "MATCH ()-[e:child_of]->() RETURN count(*)");
    let marriage_edges = run_query(&snapshot, "MATCH ()-[e:married_to]->() RETURN count(*)");
    println!("\nRelationship counts:");
    println!("  parent_of edges: {}", format_value(&parent_edges[0]));
    println!("  child_of edges: {}", format_value(&child_edges[0]));
    // Marriages are bidirectional, so divide by 2
    if let Value::Int(n) = &marriage_edges[0] {
        println!("  marriages: {}", n / 2);
    }

    // Monarchs in chronological order
    // Note: Using COALESCE for reign_end to show 'present' for current monarch
    let monarchs_chrono = run_query(
        &snapshot,
        r#"
            MATCH (p:person)
            WHERE p.is_monarch = true AND p.reign_start IS NOT NULL
            RETURN p.name, p.reign_start, COALESCE(p.reign_end, 'present') AS reign_end
            ORDER BY p.reign_start
        "#,
    );
    println!("\nMonarchs in chronological order:");
    for r in &monarchs_chrono {
        if let Value::Map(map) = r {
            let name = format_value(map.get("p.name").unwrap_or(&Value::Null));
            let start = format_value(map.get("p.reign_start").unwrap_or(&Value::Null));
            let end = format_value(map.get("reign_end").unwrap_or(&Value::Null));
            println!("  {} ({} - {})", name, start, end);
        }
    }

    println!("\n=== GQL Example Complete ===");
    println!("\nNote: Compare this with british_royals.rs to see the difference between");
    println!("the declarative GQL approach and the programmatic traversal API.");
}
