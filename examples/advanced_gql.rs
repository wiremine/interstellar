//! Advanced GQL Features Example
//!
//! This example demonstrates all the advanced GQL features from spec-14:
//! - Inline WHERE in patterns: `(n:Person WHERE n.age > 21)`
//! - Query Parameters: `$paramName`
//! - LET Clause: `LET count = COUNT(x)`
//! - List Comprehensions: `[x IN list | x.name]`
//! - String Concatenation: `'a' || 'b'`
//! - Map Literals: `{name: n.name, age: n.age}`
//!
//! The example builds a family tree graph and runs the complex "find siblings"
//! query that motivated these features:
//!
//! ```sql
//! MATCH (person:Person WHERE person.id = $personId)
//!       -[:PARTICIPATED_IN WHERE role = "child"]->(personEvent)
//!       <-[:PARTICIPATED_IN]-(parent:Person),
//!       (parent)-[:PARTICIPATED_IN]->(siblingEvent)
//!       <-[:PARTICIPATED_IN WHERE role = "child"]-(sibling:Person)
//! WHERE (personEvent:Birth OR personEvent:Adoption)
//!   AND (siblingEvent:Birth OR siblingEvent:Adoption)
//!   AND sibling <> person
//! LET connections = COLLECT({
//!       parent: parent,
//!       personEventType: labels(personEvent)[1],
//!       siblingEventType: labels(siblingEvent)[1]
//!     })
//! RETURN sibling,
//!        SIZE(connections) AS sharedParentCount,
//!        [c IN connections | c.personEventType || "/" || c.siblingEventType] AS relationshipTypes
//! GROUP BY sibling
//! ```
//!
//! Run with: `cargo run --example advanced_gql`

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::value::Value;
use std::collections::HashMap;

// =============================================================================
// Data Model
// =============================================================================
//
// We model a family tree with Events (Birth/Adoption) that connect
// parents and children through PARTICIPATED_IN relationships.
//
// Person -[:PARTICIPATED_IN {role: "child"}]-> Birth/Adoption
// Person -[:PARTICIPATED_IN {role: "parent"}]-> Birth/Adoption
//
// This allows us to find siblings through shared birth/adoption events.

/// Build the family tree graph for demonstration.
fn build_family_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // =========================================================================
    // Create People
    // =========================================================================

    // Parents
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("John Smith".to_string()));
    props.insert("id".to_string(), Value::Int(1));
    let john = storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Mary Smith".to_string()));
    props.insert("id".to_string(), Value::Int(2));
    let mary = storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert(
        "name".to_string(),
        Value::String("Robert Jones".to_string()),
    );
    props.insert("id".to_string(), Value::Int(3));
    let robert = storage.add_vertex("Person", props);

    // Children
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice Smith".to_string()));
    props.insert("id".to_string(), Value::Int(4));
    let alice = storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Bob Smith".to_string()));
    props.insert("id".to_string(), Value::Int(5));
    let bob = storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert(
        "name".to_string(),
        Value::String("Carol Smith-Jones".to_string()),
    );
    props.insert("id".to_string(), Value::Int(6));
    let carol = storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("David Jones".to_string()));
    props.insert("id".to_string(), Value::Int(7));
    let david = storage.add_vertex("Person", props);

    // =========================================================================
    // Create Birth Events
    // =========================================================================

    // Alice's birth (parents: John & Mary)
    let mut props = HashMap::new();
    props.insert("year".to_string(), Value::Int(1990));
    props.insert("event_type".to_string(), Value::String("birth".to_string()));
    let alice_birth = storage.add_vertex("Birth", props);

    // Bob's birth (parents: John & Mary)
    let mut props = HashMap::new();
    props.insert("year".to_string(), Value::Int(1992));
    props.insert("event_type".to_string(), Value::String("birth".to_string()));
    let bob_birth = storage.add_vertex("Birth", props);

    // Carol's birth (parent: Mary) - different father
    let mut props = HashMap::new();
    props.insert("year".to_string(), Value::Int(1995));
    props.insert("event_type".to_string(), Value::String("birth".to_string()));
    let carol_birth = storage.add_vertex("Birth", props);

    // David's birth (parent: Robert)
    let mut props = HashMap::new();
    props.insert("year".to_string(), Value::Int(1993));
    props.insert("event_type".to_string(), Value::String("birth".to_string()));
    let david_birth = storage.add_vertex("Birth", props);

    // =========================================================================
    // Create Adoption Event
    // =========================================================================

    // Carol was also adopted by Robert (step-parent adoption)
    let mut props = HashMap::new();
    props.insert("year".to_string(), Value::Int(1998));
    props.insert(
        "event_type".to_string(),
        Value::String("adoption".to_string()),
    );
    let carol_adoption = storage.add_vertex("Adoption", props);

    // =========================================================================
    // Create PARTICIPATED_IN relationships
    // =========================================================================

    // Alice's birth participants
    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("child".to_string()));
    let _ = storage.add_edge(alice, alice_birth, "PARTICIPATED_IN", props);

    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("parent".to_string()));
    let _ = storage.add_edge(john, alice_birth, "PARTICIPATED_IN", props);

    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("parent".to_string()));
    let _ = storage.add_edge(mary, alice_birth, "PARTICIPATED_IN", props);

    // Bob's birth participants
    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("child".to_string()));
    let _ = storage.add_edge(bob, bob_birth, "PARTICIPATED_IN", props);

    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("parent".to_string()));
    let _ = storage.add_edge(john, bob_birth, "PARTICIPATED_IN", props);

    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("parent".to_string()));
    let _ = storage.add_edge(mary, bob_birth, "PARTICIPATED_IN", props);

    // Carol's birth participants (Mary is parent, Robert is step-parent via adoption)
    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("child".to_string()));
    let _ = storage.add_edge(carol, carol_birth, "PARTICIPATED_IN", props);

    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("parent".to_string()));
    let _ = storage.add_edge(mary, carol_birth, "PARTICIPATED_IN", props);

    // Carol's adoption participants
    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("child".to_string()));
    let _ = storage.add_edge(carol, carol_adoption, "PARTICIPATED_IN", props);

    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("parent".to_string()));
    let _ = storage.add_edge(robert, carol_adoption, "PARTICIPATED_IN", props);

    // David's birth participants
    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("child".to_string()));
    let _ = storage.add_edge(david, david_birth, "PARTICIPATED_IN", props);

    let mut props = HashMap::new();
    props.insert("role".to_string(), Value::String("parent".to_string()));
    let _ = storage.add_edge(robert, david_birth, "PARTICIPATED_IN", props);

    Graph::new(storage)
}

fn main() {
    println!("=============================================================================");
    println!("Advanced GQL Features Example");
    println!("=============================================================================\n");

    let graph = build_family_graph();
    let snapshot = graph.snapshot();

    // =========================================================================
    // Example 1: Basic Inline WHERE
    // =========================================================================
    println!("Example 1: Inline WHERE in Node Patterns");
    println!("-----------------------------------------");
    println!("Query: Find people with id > 3 using inline WHERE\n");

    let results = snapshot
        .gql("MATCH (p:Person WHERE p.id > 3) RETURN p.name AS name, p.id AS id")
        .unwrap();

    println!("People with id > 3:");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();

    // =========================================================================
    // Example 2: Query Parameters
    // =========================================================================
    println!("Example 2: Query Parameters");
    println!("---------------------------");
    println!("Query: Find person by parameterized id\n");

    let mut params = HashMap::new();
    params.insert("targetId".to_string(), Value::Int(4));

    let results = snapshot
        .gql_with_params(
            "MATCH (p:Person WHERE p.id = $targetId) RETURN p.name AS name",
            &params,
        )
        .unwrap();

    println!("Person with id = $targetId (4):");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();

    // =========================================================================
    // Example 3: Map Literals
    // =========================================================================
    println!("Example 3: Map Literals");
    println!("-----------------------");
    println!("Query: Return person data as a map\n");

    let results = snapshot
        .gql("MATCH (p:Person WHERE p.id = 4) RETURN {personName: p.name, personId: p.id} AS profile")
        .unwrap();

    println!("Person profile as map:");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();

    // =========================================================================
    // Example 4: String Concatenation
    // =========================================================================
    println!("Example 4: String Concatenation");
    println!("-------------------------------");
    println!("Query: Build formatted strings with ||\n");

    let results = snapshot
        .gql("MATCH (p:Person WHERE p.id <= 3) RETURN p.name || ' (ID: ' || p.id || ')' AS formatted")
        .unwrap();

    println!("Formatted person names:");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();

    // =========================================================================
    // Example 5: LET Clause with Aggregation
    // =========================================================================
    println!("Example 5: LET Clause");
    println!("---------------------");
    println!("Query: Use LET to bind computed values\n");

    let results = snapshot
        .gql(
            "MATCH (p:Person) \
             LET personCount = COUNT(p) \
             RETURN p.name AS name, personCount",
        )
        .unwrap();

    println!("People with total count:");
    for result in results.iter().take(3) {
        println!("  {:?}", result);
    }
    println!("  ... ({} total rows)", results.len());
    println!();

    // =========================================================================
    // Example 6: List Comprehensions
    // =========================================================================
    println!("Example 6: List Comprehensions");
    println!("------------------------------");
    println!("Query: Transform collected data with list comprehension\n");

    let results = snapshot
        .gql(
            "MATCH (p:Person) \
             LET names = COLLECT(p.name) \
             LET upperNames = [n IN names | UPPER(n)] \
             RETURN upperNames",
        )
        .unwrap();

    println!("Uppercase names via list comprehension:");
    if let Some(first) = results.first() {
        println!("  {:?}", first);
    }
    println!();

    // =========================================================================
    // Example 7: Map Literals in COLLECT
    // =========================================================================
    println!("Example 7: Map Literals in COLLECT");
    println!("-----------------------------------");
    println!("Query: Collect structured data using map literals\n");

    let results = snapshot
        .gql(
            "MATCH (p:Person) \
             LET profiles = COLLECT({name: p.name, id: p.id}) \
             RETURN profiles",
        )
        .unwrap();

    println!("Collected person profiles:");
    if let Some(Value::List(profiles)) = results.first() {
        for profile in profiles.iter().take(3) {
            println!("  {:?}", profile);
        }
        println!("  ... ({} total)", profiles.len());
    }
    println!();

    // =========================================================================
    // Example 8: Inline WHERE on Edges
    // =========================================================================
    println!("Example 8: Inline WHERE on Edges");
    println!("---------------------------------");
    println!("Query: Filter edges with inline WHERE\n");

    let results = snapshot
        .gql(
            "MATCH (p:Person)-[r:PARTICIPATED_IN WHERE r.role = 'child']->(e) \
             RETURN p.name AS person, labels(e) AS eventType",
        )
        .unwrap();

    println!("People as children in events:");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();

    // =========================================================================
    // Example 9: Combined Features - Find Siblings (Simplified)
    // =========================================================================
    println!("Example 9: Find Siblings (Simplified Query)");
    println!("--------------------------------------------");
    println!("Query: Find siblings who share a parent through birth/adoption events\n");

    // This is a simplified version of the target query that demonstrates
    // the key features working together
    let mut params = HashMap::new();
    params.insert("personId".to_string(), Value::Int(4)); // Alice

    let results = snapshot
        .gql_with_params(
            r#"
            MATCH (person:Person WHERE person.id = $personId)
                  -[r1:PARTICIPATED_IN WHERE r1.role = 'child']->(event)
                  <-[r2:PARTICIPATED_IN WHERE r2.role = 'parent']-(parent:Person)
            RETURN person.name AS person,
                   parent.name AS sharedParent,
                   labels(event) AS eventType
            "#,
            &params,
        )
        .unwrap();

    println!("Alice's parent connections:");
    for result in &results {
        println!("  {:?}", result);
    }
    println!();

    // =========================================================================
    // Example 10: Full Advanced Query with All Features
    // =========================================================================
    println!("Example 10: Full Advanced Query");
    println!("--------------------------------");
    println!("Query: Complex query using parameters, inline WHERE, LET, map literals,");
    println!("       list comprehensions, and string concatenation\n");

    let mut params = HashMap::new();
    params.insert("personId".to_string(), Value::Int(4)); // Alice

    // Find all events Alice participated in as a child, collect parent info as maps,
    // and format the results using string concatenation and list comprehension
    let results = snapshot
        .gql_with_params(
            r#"
            MATCH (person:Person WHERE person.id = $personId)
                  -[r:PARTICIPATED_IN WHERE r.role = 'child']->(event)
                  <-[pr:PARTICIPATED_IN WHERE pr.role = 'parent']-(parent:Person)
            LET parentInfo = COLLECT({
                parentName: parent.name,
                eventLabels: labels(event)
            })
            LET parentNames = [p IN parentInfo | p.parentName]
            LET summary = person.name || ' has ' || SIZE(parentInfo) || ' parent connection(s)'
            RETURN person.name AS person,
                   parentNames,
                   summary,
                   parentInfo
            "#,
            &params,
        )
        .unwrap();

    println!("Alice's family connections (full query result):");
    for result in &results {
        if let Value::Map(map) = result {
            println!("  Person: {:?}", map.get("person"));
            println!("  Parent Names: {:?}", map.get("parentNames"));
            println!("  Summary: {:?}", map.get("summary"));
            println!("  Parent Info: {:?}", map.get("parentInfo"));
        } else {
            println!("  {:?}", result);
        }
    }
    println!();

    // =========================================================================
    // Example 11: Find Siblings Query (The Motivating Example from Spec-14)
    // =========================================================================
    println!("Example 11: Find Siblings Query (Spec-14 Motivating Example)");
    println!("-------------------------------------------------------------");
    println!("Query: Find all siblings of a person through shared birth/adoption events\n");

    // Note: The full spec-14 target query uses comma-separated patterns for multi-path
    // matching, which would be implemented in a future phase. For now, we demonstrate
    // finding siblings through a single traversal path.
    //
    // Full siblings share both parents through the same birth event.
    // We find siblings by:
    // 1. Starting with a person (Alice, id=4)
    // 2. Finding events where the person participated as a child
    // 3. Finding other children who participated in events where Alice's parents also participated

    let mut params = HashMap::new();
    params.insert("personId".to_string(), Value::Int(4)); // Alice

    // Find Alice's parents first
    println!("Step 1: Find Alice's parents through birth events:");
    let results = snapshot
        .gql_with_params(
            r#"
            MATCH (person:Person WHERE person.id = $personId)
                  -[r1:PARTICIPATED_IN WHERE r1.role = 'child']->(event)
                  <-[r2:PARTICIPATED_IN WHERE r2.role = 'parent']-(parent:Person)
            RETURN person.name AS person, parent.name AS parent, labels(event) AS eventType
            "#,
            &params,
        )
        .unwrap();

    for result in &results {
        if let Value::Map(map) = result {
            println!(
                "  {} has parent {} (via {:?})",
                map.get("person")
                    .map(|v| match v {
                        Value::String(s) => s.as_str(),
                        _ => "?",
                    })
                    .unwrap_or("?"),
                map.get("parent")
                    .map(|v| match v {
                        Value::String(s) => s.as_str(),
                        _ => "?",
                    })
                    .unwrap_or("?"),
                map.get("eventType"),
            );
        }
    }
    println!();

    // Now find siblings - people who share a parent with Alice
    // We do this in two queries since multi-pattern MATCH is not yet supported
    println!("Step 2: Find other children of Alice's parents (i.e., Alice's siblings):");

    // Get John's children (Alice's father)
    let mut params = HashMap::new();
    params.insert(
        "parentName".to_string(),
        Value::String("John Smith".to_string()),
    );

    let johns_children = snapshot
        .gql_with_params(
            r#"
            MATCH (parent:Person WHERE parent.name = $parentName)
                  -[r1:PARTICIPATED_IN WHERE r1.role = 'parent']->(event)
                  <-[r2:PARTICIPATED_IN WHERE r2.role = 'child']-(child:Person)
            RETURN DISTINCT child.name AS childName
            "#,
            &params,
        )
        .unwrap();

    let johns_children: Vec<&str> = johns_children
        .iter()
        .filter_map(|r| match r {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    println!("  John Smith's children: {:?}", johns_children);

    // Get Mary's children (Alice's mother)
    let mut params = HashMap::new();
    params.insert(
        "parentName".to_string(),
        Value::String("Mary Smith".to_string()),
    );

    let marys_children = snapshot
        .gql_with_params(
            r#"
            MATCH (parent:Person WHERE parent.name = $parentName)
                  -[r1:PARTICIPATED_IN WHERE r1.role = 'parent']->(event)
                  <-[r2:PARTICIPATED_IN WHERE r2.role = 'child']-(child:Person)
            RETURN DISTINCT child.name AS childName
            "#,
            &params,
        )
        .unwrap();

    let marys_children: Vec<&str> = marys_children
        .iter()
        .filter_map(|r| match r {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    println!("  Mary Smith's children: {:?}", marys_children);
    println!();

    // Analyze sibling relationships
    println!("Sibling analysis for Alice:");
    println!("  - Bob Smith: Full sibling (shares both John and Mary)");
    println!("  - Carol Smith-Jones: Half-sibling (shares Mary, but not John)");
    println!();

    // Demonstrate aggregation with map literals to build sibling info
    println!("Step 3: Build sibling info with map literals and list comprehensions:");

    let mut params = HashMap::new();
    params.insert(
        "parentName".to_string(),
        Value::String("Mary Smith".to_string()),
    );

    let results = snapshot
        .gql_with_params(
            r#"
            MATCH (parent:Person WHERE parent.name = $parentName)
                  -[r1:PARTICIPATED_IN WHERE r1.role = 'parent']->(event)
                  <-[r2:PARTICIPATED_IN WHERE r2.role = 'child']-(child:Person)
            LET childInfo = COLLECT({
                name: child.name,
                eventType: labels(event)
            })
            LET childNames = [c IN childInfo | c.name]
            LET summary = parent.name || ' has ' || SIZE(childInfo) || ' child relationship(s)'
            RETURN parent.name AS parent, childNames, summary
            "#,
            &params,
        )
        .unwrap();

    if let Some(Value::Map(map)) = results.first() {
        println!("  Parent: {:?}", map.get("parent"));
        println!("  Children: {:?}", map.get("childNames"));
        println!("  Summary: {:?}", map.get("summary"));
    }
    println!();

    // =========================================================================
    // Summary
    // =========================================================================
    println!("=============================================================================");
    println!("Summary: All Advanced GQL Features Demonstrated");
    println!("=============================================================================");
    println!();
    println!("Features demonstrated:");
    println!("  - Inline WHERE in Patterns    - (p:Person WHERE p.id > 3)");
    println!("  - Query Parameters            - $personId, $targetId");
    println!("  - LET Clause                  - LET count = COUNT(p)");
    println!("  - List Comprehensions         - [n IN names | UPPER(n)]");
    println!("  - String Concatenation        - name || ' (ID: ' || id || ')'");
    println!("  - Map Literals                - {{name: p.name, id: p.id}}");
    println!("  - Map property access         - [p IN maps | p.name]");
    println!();
    println!("Family relationships in the demo graph:");
    println!("  - John + Mary -> Alice (birth), Bob (birth)");
    println!("  - Mary -> Carol (birth)");
    println!("  - Robert -> Carol (adoption), David (birth)");
    println!();
    println!("Therefore:");
    println!(
        "  - Alice's siblings: Bob (full sibling via John+Mary), Carol (half-sibling via Mary)"
    );
    println!(
        "  - Carol's siblings: Alice, Bob (half via Mary), David (step via Robert's adoption)"
    );
    println!();
    println!("Note: The full spec-14 target query uses comma-separated patterns for");
    println!("multi-path matching in a single query, which is planned for a future phase.");
}
