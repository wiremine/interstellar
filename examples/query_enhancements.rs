//! Query Enhancements Example (Spec 17)
//!
//! This example demonstrates the query language enhancements from Spec 17:
//!
//! 1. **SKIP Alias** - Alternative syntax for OFFSET (Cypher compatibility)
//! 2. **HAVING Clause** - Filter aggregated results post-GROUP BY
//! 3. **Regular Expression Predicates** - Pattern matching with `=~` operator
//! 4. **REDUCE Function** - Fold/accumulate over lists
//! 5. **ALL/ANY/NONE/SINGLE Predicates** - List quantifier expressions
//! 6. **WITH Clause** - Pipe results between query parts for multi-stage processing
//!
//! Run with: `cargo run --example query_enhancements`

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::value::Value;
use std::collections::HashMap;

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║     Intersteller Query Enhancements Example (Spec 17)            ║");
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // Create the sample graph
    let graph = create_sample_graph();
    let snapshot = graph.snapshot();

    // =========================================================================
    // 1. SKIP Alias (Cypher-compatible pagination)
    // =========================================================================
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("1. SKIP ALIAS - Alternative syntax for OFFSET");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // SKIP is equivalent to OFFSET - both skip the first n results
    let query = "MATCH (p:Person) RETURN p.name ORDER BY p.name LIMIT 3 SKIP 2";
    println!("Query: {}\n", query);

    let results = snapshot.gql(query).unwrap();
    println!("Results (skipping first 2 of alphabetically sorted names):");
    for (i, result) in results.iter().enumerate() {
        println!("  {}. {:?}", i + 1, result);
    }
    println!();

    // =========================================================================
    // 2. HAVING Clause (Post-aggregation filtering)
    // =========================================================================
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("2. HAVING CLAUSE - Filter groups after aggregation");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // HAVING filters groups after GROUP BY, unlike WHERE which filters rows before
    let query = r#"
        MATCH (e:Employee)-[:WORKS_IN]->(d:Department)
        RETURN d.name AS department, COUNT(*) AS headcount, AVG(e.salary) AS avgSalary
        GROUP BY d.name
        HAVING headcount >= 2
        ORDER BY avgSalary DESC
    "#;
    println!("Query: {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Departments with 2+ employees:");
    for result in &results {
        if let Value::Map(map) = result {
            let dept = map.get("department").unwrap();
            let count = map.get("headcount").unwrap();
            let avg = map.get("avgSalary").unwrap();
            println!(
                "  • {:?}: {:?} employees, avg salary {:?}",
                dept, count, avg
            );
        }
    }
    println!();

    // =========================================================================
    // 3. Regular Expression Predicates
    // =========================================================================
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("3. REGEX PREDICATES - Pattern matching with =~ operator");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Find people with Gmail addresses
    let query = r#"MATCH (p:Person) WHERE p.email =~ '.*@gmail\.com$' RETURN p.name, p.email"#;
    println!("Query: {}\n", query);

    let results = snapshot.gql(query).unwrap();
    println!("People with Gmail addresses:");
    for result in &results {
        if let Value::Map(map) = result {
            println!(
                "  • {:?} - {:?}",
                map.get("p.name").unwrap(),
                map.get("p.email").unwrap()
            );
        }
    }
    println!();

    // Case-insensitive regex with (?i) flag
    let query = r#"MATCH (p:Person) WHERE p.name =~ '(?i)^a.*' RETURN p.name"#;
    println!("Query: {}\n", query);

    let results = snapshot.gql(query).unwrap();
    println!("People whose name starts with 'A' (case-insensitive):");
    for result in &results {
        println!("  • {:?}", result);
    }
    println!();

    // =========================================================================
    // 4. REDUCE Function (List accumulation)
    // =========================================================================
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("4. REDUCE FUNCTION - Fold/accumulate over lists");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Sum values using REDUCE
    let query = r#"
        MATCH (s:Student)
        RETURN s.name, 
               s.scores,
               REDUCE(total = 0, score IN s.scores | total + score) AS totalScore
    "#;
    println!("Query: {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Student score totals:");
    for result in &results {
        if let Value::Map(map) = result {
            let name = map.get("s.name").unwrap();
            let scores = map.get("s.scores").unwrap();
            let total = map.get("totalScore").unwrap();
            println!("  • {:?}: {:?} → total: {:?}", name, scores, total);
        }
    }
    println!();

    // REDUCE for string concatenation
    let query = r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN REDUCE(s = '', tag IN p.tags | s + tag + ', ') AS allTags
    "#;
    println!("Query: {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Alice's tags concatenated:");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();

    // =========================================================================
    // 5. ALL/ANY/NONE/SINGLE Predicates (List quantifiers)
    // =========================================================================
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("5. LIST PREDICATES - ALL/ANY/NONE/SINGLE quantifiers");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // ALL: Every element satisfies condition
    let query = r#"
        MATCH (s:Student)
        WHERE ALL(score IN s.scores WHERE score >= 70)
        RETURN s.name AS passingAll, s.scores
    "#;
    println!("Query (ALL - every score >= 70): {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Students where ALL scores are passing (≥70):");
    for result in &results {
        if let Value::Map(map) = result {
            println!(
                "  • {:?}: {:?}",
                map.get("passingAll").unwrap(),
                map.get("s.scores").unwrap()
            );
        }
    }
    println!();

    // ANY: At least one element satisfies condition
    let query = r#"
        MATCH (s:Student)
        WHERE ANY(score IN s.scores WHERE score >= 95)
        RETURN s.name AS hasExcellent, s.scores
    "#;
    println!("Query (ANY - at least one score >= 95): {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Students with ANY excellent score (≥95):");
    for result in &results {
        if let Value::Map(map) = result {
            println!(
                "  • {:?}: {:?}",
                map.get("hasExcellent").unwrap(),
                map.get("s.scores").unwrap()
            );
        }
    }
    println!();

    // NONE: No element satisfies condition
    let query = r#"
        MATCH (s:Student)
        WHERE NONE(score IN s.scores WHERE score < 60)
        RETURN s.name AS noFailures, s.scores
    "#;
    println!("Query (NONE - no score < 60): {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Students with NONE failing (all ≥60):");
    for result in &results {
        if let Value::Map(map) = result {
            println!(
                "  • {:?}: {:?}",
                map.get("noFailures").unwrap(),
                map.get("s.scores").unwrap()
            );
        }
    }
    println!();

    // SINGLE: Exactly one element satisfies condition
    let query = r#"
        MATCH (s:Student)
        WHERE SINGLE(score IN s.scores WHERE score = 100)
        RETURN s.name AS exactlyOnePerfect, s.scores
    "#;
    println!(
        "Query (SINGLE - exactly one score = 100): {}\n",
        query.trim()
    );

    let results = snapshot.gql(query).unwrap();
    println!("Students with SINGLE perfect score (exactly one 100):");
    for result in &results {
        if let Value::Map(map) = result {
            println!(
                "  • {:?}: {:?}",
                map.get("exactlyOnePerfect").unwrap(),
                map.get("s.scores").unwrap()
            );
        }
    }
    println!();

    // =========================================================================
    // 6. WITH Clause (Multi-stage query processing)
    // =========================================================================
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("6. WITH CLAUSE - Multi-stage query processing");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Basic WITH: project and rename
    let query = r#"
        MATCH (p:Person)
        WITH p.name AS name, p.age AS age
        WHERE age >= 30
        RETURN name, age
        ORDER BY age DESC
    "#;
    println!("Query (basic WITH projection + filter): {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("People aged 30+:");
    for result in &results {
        if let Value::Map(map) = result {
            println!(
                "  • {:?}, age {:?}",
                map.get("name").unwrap(),
                map.get("age").unwrap()
            );
        }
    }
    println!();

    // WITH with aggregation
    let query = r#"
        MATCH (p:Person)-[:KNOWS]->(friend)
        WITH p.name AS person, COUNT(friend) AS friendCount
        WHERE friendCount >= 2
        RETURN person, friendCount
        ORDER BY friendCount DESC
    "#;
    println!("Query (WITH aggregation + filter): {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("People with 2+ friends:");
    for result in &results {
        if let Value::Map(map) = result {
            println!(
                "  • {:?} has {:?} friends",
                map.get("person").unwrap(),
                map.get("friendCount").unwrap()
            );
        }
    }
    println!();

    // WITH DISTINCT
    let query = r#"
        MATCH (p:Person)
        WITH DISTINCT p.city AS city
        RETURN city
        ORDER BY city
    "#;
    println!("Query (WITH DISTINCT): {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Unique cities:");
    for result in &results {
        println!("  • {:?}", result);
    }
    println!();

    // WITH with ORDER BY and LIMIT
    let query = r#"
        MATCH (p:Person)
        WITH p.name AS name, p.age AS age
        ORDER BY age DESC
        LIMIT 3
        RETURN name, age
    "#;
    println!("Query (WITH ORDER BY LIMIT): {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Top 3 oldest people:");
    for (i, result) in results.iter().enumerate() {
        if let Value::Map(map) = result {
            println!(
                "  {}. {:?}, age {:?}",
                i + 1,
                map.get("name").unwrap(),
                map.get("age").unwrap()
            );
        }
    }
    println!();

    // WITH aggregation: SUM by group
    let query = r#"
        MATCH (e:Employee)-[:WORKS_IN]->(d:Department)
        WITH d.name AS dept, SUM(e.salary) AS totalSalary, COUNT(e) AS count
        RETURN dept, totalSalary, count
        ORDER BY totalSalary DESC
    "#;
    println!("Query (WITH SUM aggregation): {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Department salary totals:");
    for result in &results {
        if let Value::Map(map) = result {
            let dept = map.get("dept").unwrap();
            let total = map.get("totalSalary").unwrap();
            let count = map.get("count").unwrap();
            println!("  • {:?}: ${:?} total ({:?} employees)", dept, total, count);
        }
    }
    println!();

    // WITH COLLECT
    let query = r#"
        MATCH (p:Person)
        WITH p.city AS city, COLLECT(p.name) AS residents
        RETURN city, residents
        ORDER BY city
    "#;
    println!("Query (WITH COLLECT): {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Residents by city:");
    for result in &results {
        if let Value::Map(map) = result {
            let city = map.get("city").unwrap();
            let residents = map.get("residents").unwrap();
            println!("  • {:?}: {:?}", city, residents);
        }
    }
    println!();

    // =========================================================================
    // Complex Combined Query
    // =========================================================================
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("COMBINED EXAMPLE - Multiple features together");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    let query = r#"
        MATCH (p:Person)-[:KNOWS]->(friend)
        WITH p.name AS person, p.email AS email, COUNT(friend) AS friendCount
        WHERE friendCount >= 1 AND email =~ '.*@gmail\.com$'
        RETURN person, email, friendCount
        ORDER BY friendCount DESC
    "#;
    println!("Query: {}\n", query.trim());

    let results = snapshot.gql(query).unwrap();
    println!("Gmail users with friends (combined WITH + regex):");
    for result in &results {
        if let Value::Map(map) = result {
            let person = map.get("person").unwrap();
            let email = map.get("email").unwrap();
            let friends = map.get("friendCount").unwrap();
            println!("  • {:?} ({:?}): {:?} friends", person, email, friends);
        }
    }
    println!();

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║                    Example Complete!                             ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
}

/// Create a sample graph with various entities for demonstrating query features.
fn create_sample_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // -------------------------------------------------------------------------
    // People with emails, tags, cities
    // -------------------------------------------------------------------------
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::String("Alice".to_string()));
    alice_props.insert("age".to_string(), Value::Int(32));
    alice_props.insert(
        "email".to_string(),
        Value::String("alice@gmail.com".to_string()),
    );
    alice_props.insert("city".to_string(), Value::String("New York".to_string()));
    alice_props.insert(
        "tags".to_string(),
        Value::List(vec![
            Value::String("developer".to_string()),
            Value::String("leader".to_string()),
            Value::String("mentor".to_string()),
        ]),
    );
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::String("Bob".to_string()));
    bob_props.insert("age".to_string(), Value::Int(28));
    bob_props.insert(
        "email".to_string(),
        Value::String("bob@yahoo.com".to_string()),
    );
    bob_props.insert("city".to_string(), Value::String("New York".to_string()));
    bob_props.insert(
        "tags".to_string(),
        Value::List(vec![Value::String("analyst".to_string())]),
    );
    let bob = storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::String("Carol".to_string()));
    carol_props.insert("age".to_string(), Value::Int(35));
    carol_props.insert(
        "email".to_string(),
        Value::String("carol@gmail.com".to_string()),
    );
    carol_props.insert("city".to_string(), Value::String("Los Angeles".to_string()));
    carol_props.insert(
        "tags".to_string(),
        Value::List(vec![
            Value::String("manager".to_string()),
            Value::String("speaker".to_string()),
        ]),
    );
    let carol = storage.add_vertex("Person", carol_props);

    let mut dave_props = HashMap::new();
    dave_props.insert("name".to_string(), Value::String("Dave".to_string()));
    dave_props.insert("age".to_string(), Value::Int(45));
    dave_props.insert(
        "email".to_string(),
        Value::String("dave@company.org".to_string()),
    );
    dave_props.insert("city".to_string(), Value::String("Chicago".to_string()));
    dave_props.insert(
        "tags".to_string(),
        Value::List(vec![Value::String("executive".to_string())]),
    );
    let dave = storage.add_vertex("Person", dave_props);

    let mut eve_props = HashMap::new();
    eve_props.insert("name".to_string(), Value::String("Eve".to_string()));
    eve_props.insert("age".to_string(), Value::Int(29));
    eve_props.insert(
        "email".to_string(),
        Value::String("eve@gmail.com".to_string()),
    );
    eve_props.insert("city".to_string(), Value::String("Los Angeles".to_string()));
    eve_props.insert(
        "tags".to_string(),
        Value::List(vec![
            Value::String("designer".to_string()),
            Value::String("artist".to_string()),
        ]),
    );
    let eve = storage.add_vertex("Person", eve_props);

    // KNOWS relationships
    // Alice knows Bob, Carol, Dave (3 friends)
    // Bob knows Carol (1 friend)
    // Carol knows Dave, Eve (2 friends)
    // Dave knows Eve (1 friend)
    let _ = storage.add_edge(alice, bob, "KNOWS", HashMap::new());
    let _ = storage.add_edge(alice, carol, "KNOWS", HashMap::new());
    let _ = storage.add_edge(alice, dave, "KNOWS", HashMap::new());
    let _ = storage.add_edge(bob, carol, "KNOWS", HashMap::new());
    let _ = storage.add_edge(carol, dave, "KNOWS", HashMap::new());
    let _ = storage.add_edge(carol, eve, "KNOWS", HashMap::new());
    let _ = storage.add_edge(dave, eve, "KNOWS", HashMap::new());

    // -------------------------------------------------------------------------
    // Students with test scores (for ALL/ANY/NONE/SINGLE and REDUCE demos)
    // -------------------------------------------------------------------------
    let mut emma_props = HashMap::new();
    emma_props.insert("name".to_string(), Value::String("Emma".to_string()));
    emma_props.insert(
        "scores".to_string(),
        Value::List(vec![
            Value::Int(85),
            Value::Int(90),
            Value::Int(78),
            Value::Int(92),
        ]),
    );
    storage.add_vertex("Student", emma_props);

    let mut frank_props = HashMap::new();
    frank_props.insert("name".to_string(), Value::String("Frank".to_string()));
    frank_props.insert(
        "scores".to_string(),
        Value::List(vec![
            Value::Int(72),
            Value::Int(68),
            Value::Int(75),
            Value::Int(55), // One failing score < 60
        ]),
    );
    storage.add_vertex("Student", frank_props);

    let mut grace_props = HashMap::new();
    grace_props.insert("name".to_string(), Value::String("Grace".to_string()));
    grace_props.insert(
        "scores".to_string(),
        Value::List(vec![
            Value::Int(95),
            Value::Int(88),
            Value::Int(100),
            Value::Int(92), // One perfect score
        ]),
    );
    storage.add_vertex("Student", grace_props);

    let mut henry_props = HashMap::new();
    henry_props.insert("name".to_string(), Value::String("Henry".to_string()));
    henry_props.insert(
        "scores".to_string(),
        Value::List(vec![
            Value::Int(100),
            Value::Int(100),
            Value::Int(98),
            Value::Int(97), // Two perfect scores
        ]),
    );
    storage.add_vertex("Student", henry_props);

    // -------------------------------------------------------------------------
    // Departments and Employees (for HAVING and aggregation demos)
    // -------------------------------------------------------------------------
    let mut eng_props = HashMap::new();
    eng_props.insert("name".to_string(), Value::String("Engineering".to_string()));
    let engineering = storage.add_vertex("Department", eng_props);

    let mut sales_props = HashMap::new();
    sales_props.insert("name".to_string(), Value::String("Sales".to_string()));
    let sales = storage.add_vertex("Department", sales_props);

    let mut hr_props = HashMap::new();
    hr_props.insert("name".to_string(), Value::String("HR".to_string()));
    let hr = storage.add_vertex("Department", hr_props);

    // Employees
    let mut emp1_props = HashMap::new();
    emp1_props.insert("name".to_string(), Value::String("John".to_string()));
    emp1_props.insert("salary".to_string(), Value::Int(85000));
    let emp1 = storage.add_vertex("Employee", emp1_props);
    let _ = storage.add_edge(emp1, engineering, "WORKS_IN", HashMap::new());

    let mut emp2_props = HashMap::new();
    emp2_props.insert("name".to_string(), Value::String("Jane".to_string()));
    emp2_props.insert("salary".to_string(), Value::Int(92000));
    let emp2 = storage.add_vertex("Employee", emp2_props);
    let _ = storage.add_edge(emp2, engineering, "WORKS_IN", HashMap::new());

    let mut emp3_props = HashMap::new();
    emp3_props.insert("name".to_string(), Value::String("Mike".to_string()));
    emp3_props.insert("salary".to_string(), Value::Int(78000));
    let emp3 = storage.add_vertex("Employee", emp3_props);
    let _ = storage.add_edge(emp3, engineering, "WORKS_IN", HashMap::new());

    let mut emp4_props = HashMap::new();
    emp4_props.insert("name".to_string(), Value::String("Sarah".to_string()));
    emp4_props.insert("salary".to_string(), Value::Int(65000));
    let emp4 = storage.add_vertex("Employee", emp4_props);
    let _ = storage.add_edge(emp4, sales, "WORKS_IN", HashMap::new());

    let mut emp5_props = HashMap::new();
    emp5_props.insert("name".to_string(), Value::String("Tom".to_string()));
    emp5_props.insert("salary".to_string(), Value::Int(72000));
    let emp5 = storage.add_vertex("Employee", emp5_props);
    let _ = storage.add_edge(emp5, sales, "WORKS_IN", HashMap::new());

    let mut emp6_props = HashMap::new();
    emp6_props.insert("name".to_string(), Value::String("Lisa".to_string()));
    emp6_props.insert("salary".to_string(), Value::Int(55000));
    let emp6 = storage.add_vertex("Employee", emp6_props);
    let _ = storage.add_edge(emp6, hr, "WORKS_IN", HashMap::new());

    Graph::new(storage)
}
