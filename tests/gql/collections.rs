//! Collection tests for GQL query language
//!
//! Tests for collection operations including:
//! - List comprehension
//! - String concatenation
//! - Map literals
//! - Regex predicates
//! - REDUCE expression
//! - List predicates (ALL, ANY, NONE, SINGLE)
//! - WITH clause
//! - List indexing and slicing
//! - Pattern comprehension

use interstellar::gql::parse;
use interstellar::prelude::*;
use interstellar::storage::InMemoryGraph;
use std::collections::{HashMap, HashSet};

// =============================================================================
// Test Graph Helpers
// =============================================================================

/// Helper to create a basic test graph
fn create_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("age".to_string(), Value::from(30i64));
    storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Bob"));
    props.insert("age".to_string(), Value::from(25i64));
    storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Charlie"));
    props.insert("age".to_string(), Value::from(35i64));
    storage.add_vertex("Person", props);

    Graph::new(storage)
}

/// Helper to create a graph with a single dummy vertex for testing expressions
fn create_dummy_graph() -> Graph {
    let mut storage = InMemoryGraph::new();
    storage.add_vertex("Dummy", HashMap::new());
    Graph::new(storage)
}

/// Create a test graph with email data for regex tests
fn create_regex_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("email".to_string(), Value::from("alice@gmail.com"));
    storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Bob"));
    props.insert("email".to_string(), Value::from("bob@yahoo.com"));
    storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Charlie"));
    props.insert("email".to_string(), Value::from("charlie@gmail.com"));
    storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("John Smith"));
    props.insert("email".to_string(), Value::from("john.smith@company.org"));
    storage.add_vertex("Person", props);

    Graph::new(storage)
}

/// Helper to create a graph with relationships for WITH clause testing
fn create_with_clause_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    alice_props.insert("city".to_string(), Value::from("NYC"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    bob_props.insert("city".to_string(), Value::from("NYC"));
    let bob = storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    carol_props.insert("age".to_string(), Value::from(35i64));
    carol_props.insert("city".to_string(), Value::from("LA"));
    let carol = storage.add_vertex("Person", carol_props);

    let mut dave_props = HashMap::new();
    dave_props.insert("name".to_string(), Value::from("Dave"));
    dave_props.insert("age".to_string(), Value::from(28i64));
    dave_props.insert("city".to_string(), Value::from("NYC"));
    let dave = storage.add_vertex("Person", dave_props);

    let mut eve_props = HashMap::new();
    eve_props.insert("name".to_string(), Value::from("Eve"));
    eve_props.insert("age".to_string(), Value::from(32i64));
    eve_props.insert("city".to_string(), Value::from("LA"));
    let eve = storage.add_vertex("Person", eve_props);

    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, dave, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(carol, dave, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(carol, eve, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(dave, eve, "KNOWS", HashMap::new())
        .unwrap();

    Graph::new(storage)
}

fn create_pattern_comprehension_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25));
    let bob = storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    carol_props.insert("age".to_string(), Value::from(35));
    let carol = storage.add_vertex("Person", carol_props);

    let mut dave_props = HashMap::new();
    dave_props.insert("name".to_string(), Value::from("Dave"));
    dave_props.insert("age".to_string(), Value::from(20));
    let dave = storage.add_vertex("Person", dave_props);

    storage
        .add_edge(alice, bob, "FRIEND", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, carol, "FRIEND", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, dave, "FRIEND", HashMap::new())
        .unwrap();
    storage
        .add_edge(carol, dave, "FRIEND", HashMap::new())
        .unwrap();

    Graph::new(storage)
}

// =============================================================================
// List Comprehension Tests
// =============================================================================

#[test]
fn test_gql_list_comprehension_basic_transform() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) LET names = COLLECT(n.name) LET upper = [x IN names | TOUPPER(x)] RETURN upper")
        .unwrap();

    assert_eq!(results.len(), 3);

    if let Value::Map(map) = &results[0] {
        let upper = map.get("upper").unwrap();
        if let Value::List(items) = upper {
            assert_eq!(items.len(), 3);
            let names: HashSet<String> = items
                .iter()
                .filter_map(|v| {
                    if let Value::String(s) = v {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .collect();
            assert!(names.contains("ALICE"));
            assert!(names.contains("BOB"));
            assert!(names.contains("CHARLIE"));
        } else {
            panic!("Expected list for upper");
        }
    }
}

#[test]
fn test_gql_list_comprehension_with_filter() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) LET ages = COLLECT(n.age) LET adults = [a IN ages WHERE a >= 30 | a] RETURN adults")
        .unwrap();

    assert_eq!(results.len(), 3);

    if let Value::Map(map) = &results[0] {
        let adults = map.get("adults").unwrap();
        if let Value::List(items) = adults {
            assert_eq!(items.len(), 2);
            let ages: HashSet<i64> = items
                .iter()
                .filter_map(|v| {
                    if let Value::Int(n) = v {
                        Some(*n)
                    } else {
                        None
                    }
                })
                .collect();
            assert!(ages.contains(&30));
            assert!(ages.contains(&35));
            assert!(!ages.contains(&25));
        } else {
            panic!("Expected list for adults");
        }
    }
}

#[test]
fn test_gql_list_comprehension_numeric_transform() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) LET ages = COLLECT(n.age) LET doubled = [a IN ages | a * 2] RETURN doubled")
        .unwrap();

    assert_eq!(results.len(), 3);

    if let Value::Map(map) = &results[0] {
        let doubled = map.get("doubled").unwrap();
        if let Value::List(items) = doubled {
            assert_eq!(items.len(), 3);
            let ages: HashSet<i64> = items
                .iter()
                .filter_map(|v| {
                    if let Value::Int(n) = v {
                        Some(*n)
                    } else {
                        None
                    }
                })
                .collect();
            assert!(ages.contains(&60));
            assert!(ages.contains(&50));
            assert!(ages.contains(&70));
        } else {
            panic!("Expected list for doubled");
        }
    }
}

#[test]
fn test_gql_list_comprehension_filter_and_transform() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) LET ages = COLLECT(n.age) LET result = [a IN ages WHERE a > 25 | a * 2] RETURN result")
        .unwrap();

    assert_eq!(results.len(), 3);

    if let Value::Map(map) = &results[0] {
        let result = map.get("result").unwrap();
        if let Value::List(items) = result {
            assert_eq!(items.len(), 2);
            let values: HashSet<i64> = items
                .iter()
                .filter_map(|v| {
                    if let Value::Int(n) = v {
                        Some(*n)
                    } else {
                        None
                    }
                })
                .collect();
            assert!(values.contains(&60));
            assert!(values.contains(&70));
        } else {
            panic!("Expected list for result");
        }
    }
}

#[test]
fn test_gql_list_comprehension_empty_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:NonExistent) LET names = COLLECT(n.name) LET upper = [x IN names | TOUPPER(x)] RETURN upper")
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_gql_list_comprehension_null_handling() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) LET emails = n.emails LET processed = [e IN emails | TOUPPER(e)] RETURN processed")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        let processed = map.get("processed").unwrap();
        assert!(matches!(processed, Value::Null));
    }
}

#[test]
fn test_gql_list_comprehension_property_access() {
    let mut storage = InMemoryGraph::new();

    let alice_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Alice"));
        props.insert("age".to_string(), Value::from(30i64));
        props
    });

    let bob_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bob"));
        props.insert("age".to_string(), Value::from(25i64));
        props
    });

    let _ = storage.add_edge(alice_id, bob_id, "KNOWS", HashMap::new());

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person)-[:KNOWS]->(f) LET friends = COLLECT(f) LET friendNames = [friend IN friends | friend.name] RETURN p.name, friendNames")
        .unwrap();

    assert!(!results.is_empty());
}

#[test]
fn test_gql_list_comprehension_nested() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) LET ages = COLLECT(n.age) LET doubled = [a IN ages | a * 2] LET big = [d IN doubled WHERE d > 55 | d] RETURN big")
        .unwrap();

    assert_eq!(results.len(), 3);

    if let Value::Map(map) = &results[0] {
        let big = map.get("big").unwrap();
        if let Value::List(items) = big {
            assert_eq!(items.len(), 2);
            let values: HashSet<i64> = items
                .iter()
                .filter_map(|v| {
                    if let Value::Int(n) = v {
                        Some(*n)
                    } else {
                        None
                    }
                })
                .collect();
            assert!(values.contains(&60));
            assert!(values.contains(&70));
            assert!(!values.contains(&50));
        } else {
            panic!("Expected list for big");
        }
    }
}

#[test]
fn test_gql_list_comprehension_parse_only() {
    use interstellar::gql::Expression;

    let query =
        parse("MATCH (n:Person) LET doubled = [x IN items | x * 2] RETURN doubled").unwrap();

    assert_eq!(query.let_clauses.len(), 1);
    assert_eq!(query.let_clauses[0].variable, "doubled");

    if let Expression::ListComprehension {
        variable, filter, ..
    } = &query.let_clauses[0].expression
    {
        assert_eq!(variable, "x");
        assert!(filter.is_none());
    } else {
        panic!("Expected ListComprehension expression");
    }
}

#[test]
fn test_gql_list_comprehension_parse_with_filter() {
    use interstellar::gql::Expression;

    let query =
        parse("MATCH (n:Person) LET filtered = [x IN items WHERE x > 10 | x * 2] RETURN filtered")
            .unwrap();

    assert_eq!(query.let_clauses.len(), 1);

    if let Expression::ListComprehension {
        variable, filter, ..
    } = &query.let_clauses[0].expression
    {
        assert_eq!(variable, "x");
        assert!(filter.is_some());
    } else {
        panic!("Expected ListComprehension expression");
    }
}

#[test]
fn test_gql_list_comprehension_in_return() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) LET ages = COLLECT(n.age) RETURN [a IN ages | a * 2] AS doubled")
        .unwrap();

    assert_eq!(results.len(), 3);

    if let Value::Map(map) = &results[0] {
        let doubled = map.get("doubled").unwrap();
        if let Value::List(items) = doubled {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected list for doubled");
        }
    }
}

#[test]
fn test_gql_list_comprehension_with_arithmetic() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) LET ages = COLLECT(n.age) LET computed = [a IN ages | (a * 2) + 10] RETURN computed")
        .unwrap();

    assert_eq!(results.len(), 3);

    if let Value::Map(map) = &results[0] {
        let computed = map.get("computed").unwrap();
        if let Value::List(items) = computed {
            assert_eq!(items.len(), 3);
            let values: HashSet<i64> = items
                .iter()
                .filter_map(|v| {
                    if let Value::Int(n) = v {
                        Some(*n)
                    } else {
                        None
                    }
                })
                .collect();
            assert!(values.contains(&70));
            assert!(values.contains(&60));
            assert!(values.contains(&80));
        } else {
            panic!("Expected list for computed");
        }
    }
}

// =============================================================================
// String Concatenation Tests
// =============================================================================

#[test]
fn test_gql_string_concat_basic() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) RETURN 'Hello' || ' ' || 'World' AS greeting")
        .unwrap();

    assert!(!results.is_empty());
    assert_eq!(results[0], Value::String("Hello World".to_string()));
}

#[test]
fn test_gql_string_concat_properties() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name || ' is ' || n.age || ' years old' AS description")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::String("Alice is 30 years old".to_string())
    );
}

#[test]
fn test_gql_string_concat_null_handling() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) RETURN NULL || 'text' AS result")
        .unwrap();

    assert!(!results.is_empty());
    assert_eq!(results[0], Value::Null);

    let results = snapshot
        .gql("MATCH (n:Person) RETURN 'text' || NULL AS result")
        .unwrap();

    assert!(!results.is_empty());
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_gql_string_concat_type_coercion() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) RETURN 'Count: ' || 42 AS result")
        .unwrap();

    assert!(!results.is_empty());
    assert_eq!(results[0], Value::String("Count: 42".to_string()));

    let results = snapshot
        .gql("MATCH (n:Person) RETURN 'Value: ' || 3.14 AS result")
        .unwrap();

    assert!(!results.is_empty());
    assert_eq!(results[0], Value::String("Value: 3.14".to_string()));

    let results = snapshot
        .gql("MATCH (n:Person) RETURN 'Active: ' || true AS result")
        .unwrap();

    assert!(!results.is_empty());
    assert_eq!(results[0], Value::String("Active: true".to_string()));
}

#[test]
fn test_gql_string_concat_chained() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN 'Name: ' || n.name || ', Age: ' || n.age AS info")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::String("Name: Alice, Age: 30".to_string())
    );
}

#[test]
fn test_gql_string_concat_in_where_clause() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name || '!' = 'Alice!' RETURN n.name AS name")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn test_gql_string_concat_precedence() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) RETURN 'Result: ' || 1 + 2 AS result")
        .unwrap();

    assert!(!results.is_empty());
    assert_eq!(results[0], Value::String("Result: 3".to_string()));
}

#[test]
fn test_gql_string_concat_with_let() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' LET fullDesc = n.name || ' (' || n.age || ')' RETURN fullDesc")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice (30)".to_string()));
}

#[test]
fn test_gql_string_concat_multiple_returns() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name AS name, n.name || '!' AS excited")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(
            map.get("excited"),
            Some(&Value::String("Alice!".to_string()))
        );
    } else {
        panic!("Expected map result for multiple return items");
    }
}

// =============================================================================
// Map Literal Tests
// =============================================================================

#[test]
fn test_gql_map_literal_basic() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) RETURN {name: 'test', count: 42} AS data")
        .unwrap();

    assert!(!results.is_empty());
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("name"), Some(&Value::String("test".to_string())));
        assert_eq!(map.get("count"), Some(&Value::Int(42)));
    } else {
        panic!("Expected map result, got {:?}", results[0]);
    }
}

#[test]
fn test_gql_map_literal_with_properties() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN {personName: n.name, personAge: n.age} AS info")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("personName"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(map.get("personAge"), Some(&Value::Int(30)));
    } else {
        panic!("Expected map result, got {:?}", results[0]);
    }
}

#[test]
fn test_gql_map_literal_empty() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (n:Person) RETURN {} AS empty").unwrap();

    assert!(!results.is_empty());
    if let Value::Map(map) = &results[0] {
        assert!(map.is_empty());
    } else {
        panic!("Expected empty map, got {:?}", results[0]);
    }
}

#[test]
fn test_gql_map_literal_with_expressions() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN {doubled: n.age * 2, greeting: 'Hello ' || n.name} AS computed")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("doubled"), Some(&Value::Int(60)));
        assert_eq!(
            map.get("greeting"),
            Some(&Value::String("Hello Alice".to_string()))
        );
    } else {
        panic!("Expected map result, got {:?}", results[0]);
    }
}

#[test]
fn test_gql_map_literal_nested() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN {person: {name: n.name, age: n.age}, type: 'data'} AS nested")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(outer) = &results[0] {
        assert_eq!(outer.get("type"), Some(&Value::String("data".to_string())));
        if let Some(Value::Map(inner)) = outer.get("person") {
            assert_eq!(inner.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(inner.get("age"), Some(&Value::Int(30)));
        } else {
            panic!("Expected nested map for 'person'");
        }
    } else {
        panic!("Expected map result, got {:?}", results[0]);
    }
}

#[test]
fn test_gql_map_literal_with_let() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' LET profile = {name: n.name, age: n.age} RETURN profile")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(map.get("age"), Some(&Value::Int(30)));
    } else {
        panic!("Expected map result, got {:?}", results[0]);
    }
}

#[test]
fn test_gql_map_literal_in_collect() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) LET people = COLLECT({name: n.name, age: n.age}) RETURN people")
        .unwrap();

    assert_eq!(results.len(), 3);
    if let Value::List(items) = &results[0] {
        assert_eq!(items.len(), 3);
        for item in items {
            if let Value::Map(map) = item {
                assert!(map.contains_key("name"));
                assert!(map.contains_key("age"));
            } else {
                panic!("Expected map in list, got {:?}", item);
            }
        }
    } else {
        panic!("Expected list result, got {:?}", results[0]);
    }
}

#[test]
fn test_gql_map_literal_multiple_entries() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN {a: 1, b: 2, c: 3, d: 4} AS multi")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("a"), Some(&Value::Int(1)));
        assert_eq!(map.get("b"), Some(&Value::Int(2)));
        assert_eq!(map.get("c"), Some(&Value::Int(3)));
        assert_eq!(map.get("d"), Some(&Value::Int(4)));
    } else {
        panic!("Expected map result, got {:?}", results[0]);
    }
}

#[test]
fn test_gql_map_literal_parse() {
    use interstellar::gql::Expression;

    let query = parse("MATCH (n:Person) RETURN {name: n.name, age: 30} AS data").unwrap();

    if let Expression::Map(entries) = &query.return_clause.items[0].expression {
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, "name");
        assert_eq!(entries[1].0, "age");
    } else {
        panic!("Expected Map expression in RETURN");
    }
}

#[test]
fn test_gql_map_literal_with_string_key() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN {'full-name': n.name, 'years-old': n.age} AS data")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("full-name"),
            Some(&Value::String("Alice".to_string()))
        );
        assert_eq!(map.get("years-old"), Some(&Value::Int(30)));
    } else {
        panic!("Expected map result, got {:?}", results[0]);
    }
}

// =============================================================================
// Regex Predicate Tests
// =============================================================================

#[test]
fn test_gql_regex_basic_match() {
    let graph = create_regex_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.email =~ '.*@gmail\\.com$' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 2);
    let names: HashSet<String> = results
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            _ => panic!("Expected string"),
        })
        .collect();
    assert!(names.contains("Alice"));
    assert!(names.contains("Charlie"));
}

#[test]
fn test_gql_regex_no_match() {
    let graph = create_regex_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.email =~ '.*@outlook\\.com$' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_gql_regex_case_insensitive() {
    let graph = create_regex_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name =~ '(?i)^john' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("John Smith".to_string()));
}

#[test]
fn test_gql_regex_starts_with_pattern() {
    let graph = create_regex_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name =~ '^[AC]' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 2);
    let names: HashSet<String> = results
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            _ => panic!("Expected string"),
        })
        .collect();
    assert!(names.contains("Alice"));
    assert!(names.contains("Charlie"));
}

#[test]
fn test_gql_regex_contains_pattern() {
    let graph = create_regex_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.email =~ '.*\\..*@' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("John Smith".to_string()));
}

#[test]
fn test_gql_regex_with_null() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("email".to_string(), Value::from("alice@test.com"));
    storage.add_vertex("Person", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Bob"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.email =~ '.*' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn test_gql_regex_invalid_pattern() {
    let graph = create_regex_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.email =~ '.*(' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_gql_regex_in_return_expression() {
    let graph = create_regex_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.email =~ '.*@gmail\\.com$' AS is_gmail")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_gql_regex_combined_with_and() {
    let graph = create_regex_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.email =~ '.*@gmail\\.com$' AND p.name =~ '^A' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn test_gql_regex_combined_with_or() {
    let graph = create_regex_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.email =~ '.*@gmail\\.com$' OR p.email =~ '.*@yahoo\\.com$' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 3);
    let names: HashSet<String> = results
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            _ => panic!("Expected string"),
        })
        .collect();
    assert!(names.contains("Alice"));
    assert!(names.contains("Bob"));
    assert!(names.contains("Charlie"));
}

#[test]
fn test_gql_regex_with_not() {
    let graph = create_regex_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE NOT p.email =~ '.*@gmail\\.com$' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 2);
    let names: HashSet<String> = results
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            _ => panic!("Expected string"),
        })
        .collect();
    assert!(names.contains("Bob"));
    assert!(names.contains("John Smith"));
}

#[test]
fn test_gql_regex_digit_pattern() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Product A"));
    props.insert("sku".to_string(), Value::from("SKU-12345"));
    storage.add_vertex("Product", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Product B"));
    props.insert("sku".to_string(), Value::from("SKU-99999"));
    storage.add_vertex("Product", props);

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Product C"));
    props.insert("sku".to_string(), Value::from("INVALID"));
    storage.add_vertex("Product", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Product) WHERE p.sku =~ '^SKU-\\d{5}$' RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 2);
    let names: HashSet<String> = results
        .iter()
        .map(|v| match v {
            Value::String(s) => s.clone(),
            _ => panic!("Expected string"),
        })
        .collect();
    assert!(names.contains("Product A"));
    assert!(names.contains("Product B"));
}

#[test]
fn test_gql_regex_parse() {
    use interstellar::gql::{BinaryOperator, Expression};

    let query = parse("MATCH (p:Person) WHERE p.email =~ '.*@gmail\\.com$' RETURN p").unwrap();

    if let Some(where_clause) = &query.where_clause {
        if let Expression::BinaryOp { op, .. } = &where_clause.expression {
            assert_eq!(*op, BinaryOperator::RegexMatch);
        } else {
            panic!("Expected BinaryOp expression in WHERE");
        }
    } else {
        panic!("Expected WHERE clause");
    }
}

// =============================================================================
// REDUCE Expression Tests
// =============================================================================

#[test]
fn test_gql_reduce_sum_numbers() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN REDUCE(total = 0, x IN [1, 2, 3, 4, 5] | total + x) AS sum")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(15));
}

#[test]
fn test_gql_reduce_product() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN REDUCE(product = 1, n IN [2, 3, 4] | product * n) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(24));
}

#[test]
fn test_gql_reduce_string_concat() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN REDUCE(s = '', name IN ['Alice', 'Bob', 'Carol'] | s || name || ', ') AS names")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice, Bob, Carol, ".to_string()));
}

#[test]
fn test_gql_reduce_empty_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN REDUCE(total = 100, x IN [] | total + x) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(100));
}

#[test]
fn test_gql_reduce_null_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN REDUCE(total = 0, x IN null | total + x) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_gql_reduce_with_float_initial() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN REDUCE(sum = 0.0, n IN [1.5, 2.5, 3.0] | sum + n) AS total")
        .unwrap();

    assert_eq!(results.len(), 1);
    match &results[0] {
        Value::Float(f) => assert!((f - 7.0).abs() < 0.001),
        other => panic!("Expected Float(7.0), got {:?}", other),
    }
}

#[test]
fn test_gql_reduce_nested_expression() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN REDUCE(sum = 0, n IN [1, 2, 3] | sum + n * n) AS sum_of_squares")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(14));
}

#[test]
fn test_gql_reduce_count_elements() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN REDUCE(count = 0, x IN ['a', 'b', 'c', 'd'] | count + 1) AS count")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(4));
}

#[test]
fn test_gql_reduce_max_value() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN REDUCE(max = 0, n IN [3, 7, 2, 9, 1] | CASE WHEN n > max THEN n ELSE max END) AS maximum")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(9));
}

#[test]
fn test_gql_reduce_parse() {
    use interstellar::gql::{Expression, Literal};

    let query = parse("MATCH (n) RETURN REDUCE(total = 0, x IN items | total + x) AS sum").unwrap();

    assert_eq!(query.return_clause.items.len(), 1);
    let return_item = &query.return_clause.items[0];
    match &return_item.expression {
        Expression::Reduce {
            accumulator,
            initial,
            variable,
            list,
            expression,
        } => {
            assert_eq!(accumulator, "total");
            assert_eq!(variable, "x");
            match initial.as_ref() {
                Expression::Literal(Literal::Int(0)) => {}
                other => panic!("Expected initial value 0, got {:?}", other),
            }
            match list.as_ref() {
                Expression::Variable(var) => assert_eq!(var, "items"),
                other => panic!("Expected Variable 'items', got {:?}", other),
            }
            match expression.as_ref() {
                Expression::BinaryOp { .. } => {}
                other => panic!("Expected BinaryOp, got {:?}", other),
            }
        }
        other => panic!("Expected Reduce expression, got {:?}", other),
    }
}

#[test]
fn test_gql_reduce_with_list_comprehension_input() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN REDUCE(sum = 0, n IN [x IN [1, 2, 3] | x * 2] | sum + n) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(12));
}

// =============================================================================
// List Predicate Tests (ALL, ANY, NONE, SINGLE)
// =============================================================================

#[test]
fn test_gql_all_predicate_all_match() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN ALL(x IN [2, 4, 6] WHERE x % 2 = 0) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_gql_all_predicate_one_fails() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN ALL(x IN [2, 3, 6] WHERE x % 2 = 0) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(false));
}

#[test]
fn test_gql_all_predicate_empty_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN ALL(x IN [] WHERE x > 0) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_gql_any_predicate_one_matches() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN ANY(x IN [1, 2, 3] WHERE x = 2) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_gql_any_predicate_none_match() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN ANY(x IN [1, 2, 3] WHERE x = 4) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(false));
}

#[test]
fn test_gql_any_predicate_empty_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN ANY(x IN [] WHERE x > 0) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(false));
}

#[test]
fn test_gql_none_predicate_none_match() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN NONE(x IN [1, 2, 3] WHERE x = 4) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_gql_none_predicate_one_matches() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN NONE(x IN [1, 2, 3] WHERE x = 2) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(false));
}

#[test]
fn test_gql_none_predicate_empty_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN NONE(x IN [] WHERE x > 0) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_gql_single_predicate_exactly_one() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN SINGLE(x IN [1, 2, 3] WHERE x = 2) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_gql_single_predicate_multiple_match() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN SINGLE(x IN [2, 2, 3] WHERE x = 2) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(false));
}

#[test]
fn test_gql_single_predicate_none_match() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN SINGLE(x IN [1, 2, 3] WHERE x = 4) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(false));
}

#[test]
fn test_gql_single_predicate_empty_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN SINGLE(x IN [] WHERE x > 0) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(false));
}

#[test]
fn test_gql_list_predicate_in_where_clause() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE ALL(x IN [25, 30, 35] WHERE x >= 25) RETURN n.name AS name")
        .unwrap();

    assert_eq!(results.len(), 3);
}

#[test]
fn test_gql_list_predicate_with_variable_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN ANY(x IN [20, 25, 30] WHERE x = n.age) AS has_age")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_gql_list_predicate_complex_condition() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN ALL(x IN [2, 3] WHERE x > 1 AND x < 4) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_gql_list_predicate_nested() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.name = 'Alice' RETURN ANY(sublist IN [[1, 2], [3, 4]] WHERE ALL(x IN sublist WHERE x > 0)) AS result")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_gql_all_predicate_parse() {
    use interstellar::gql::Expression;

    let query = parse("MATCH (n) RETURN ALL(x IN items WHERE x > 0)").unwrap();
    assert_eq!(query.return_clause.items.len(), 1);
    let return_item = &query.return_clause.items[0];
    match &return_item.expression {
        Expression::All {
            variable,
            list,
            condition: _,
        } => {
            assert_eq!(variable, "x");
            match list.as_ref() {
                Expression::Variable(var) => assert_eq!(var, "items"),
                other => panic!("Expected Variable 'items', got {:?}", other),
            }
        }
        other => panic!("Expected All expression, got {:?}", other),
    }
}

#[test]
fn test_gql_any_predicate_parse() {
    use interstellar::gql::Expression;

    let query = parse("MATCH (n) RETURN ANY(x IN items WHERE x = 0)").unwrap();
    assert_eq!(query.return_clause.items.len(), 1);
    let return_item = &query.return_clause.items[0];
    match &return_item.expression {
        Expression::Any {
            variable,
            list,
            condition: _,
        } => {
            assert_eq!(variable, "x");
            match list.as_ref() {
                Expression::Variable(var) => assert_eq!(var, "items"),
                other => panic!("Expected Variable 'items', got {:?}", other),
            }
        }
        other => panic!("Expected Any expression, got {:?}", other),
    }
}

#[test]
fn test_gql_none_predicate_parse() {
    use interstellar::gql::Expression;

    let query = parse("MATCH (n) RETURN NONE(x IN items WHERE x < 0)").unwrap();
    assert_eq!(query.return_clause.items.len(), 1);
    let return_item = &query.return_clause.items[0];
    match &return_item.expression {
        Expression::None {
            variable,
            list: _,
            condition: _,
        } => {
            assert_eq!(variable, "x");
        }
        other => panic!("Expected None expression, got {:?}", other),
    }
}

#[test]
fn test_gql_single_predicate_parse() {
    use interstellar::gql::Expression;

    let query = parse("MATCH (n) RETURN SINGLE(x IN items WHERE x = 1)").unwrap();
    assert_eq!(query.return_clause.items.len(), 1);
    let return_item = &query.return_clause.items[0];
    match &return_item.expression {
        Expression::Single {
            variable,
            list: _,
            condition: _,
        } => {
            assert_eq!(variable, "x");
        }
        other => panic!("Expected Single expression, got {:?}", other),
    }
}

// =============================================================================
// WITH Clause Tests
// =============================================================================

#[test]
fn test_gql_with_basic_projection() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH p.name AS name RETURN name")
        .unwrap();

    assert_eq!(results.len(), 5, "Should return 5 person names");

    for result in &results {
        assert!(matches!(result, Value::String(_)));
    }
}

#[test]
fn test_gql_with_multiple_items() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH p.name AS name, p.age AS age RETURN name, age")
        .unwrap();

    assert_eq!(results.len(), 5);

    for result in &results {
        if let Value::Map(map) = result {
            assert!(map.contains_key("name"));
            assert!(map.contains_key("age"));
        } else {
            panic!("Expected Map result");
        }
    }
}

#[test]
fn test_gql_with_where_filter() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH p.name AS name, p.age AS age WHERE age > 30 RETURN name")
        .unwrap();

    assert_eq!(
        results.len(),
        2,
        "Should find 2 people over 30 (Carol=35, Eve=32)"
    );

    let names: HashSet<_> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(names.contains("Carol"));
    assert!(names.contains("Eve"));
}

#[test]
fn test_gql_with_aggregation_count() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person)-[:KNOWS]->(friend) WITH p.name AS person, COUNT(friend) AS friendCount RETURN person, friendCount")
        .unwrap();

    assert_eq!(results.len(), 4, "Should have 4 people with friends");

    let mut counts: HashMap<String, i64> = HashMap::new();
    for result in results {
        if let Value::Map(map) = result {
            let person = match map.get("person") {
                Some(Value::String(s)) => s.clone(),
                _ => continue,
            };
            let count = match map.get("friendCount") {
                Some(Value::Int(n)) => *n,
                _ => continue,
            };
            counts.insert(person, count);
        }
    }

    assert_eq!(counts.get("Alice"), Some(&3));
    assert_eq!(counts.get("Bob"), Some(&1));
    assert_eq!(counts.get("Carol"), Some(&2));
    assert_eq!(counts.get("Dave"), Some(&1));
}

#[test]
fn test_gql_with_aggregation_where_on_count() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person)-[:KNOWS]->(friend) WITH p.name AS person, COUNT(friend) AS cnt WHERE cnt >= 2 RETURN person, cnt")
        .unwrap();

    assert_eq!(results.len(), 2, "Should find 2 people with >= 2 friends");

    let names: HashSet<_> = results
        .iter()
        .filter_map(|v| match v {
            Value::Map(map) => map.get("person").and_then(|v| match v {
                Value::String(s) => Some(s.as_str()),
                _ => None,
            }),
            _ => None,
        })
        .collect();

    assert!(names.contains("Alice"));
    assert!(names.contains("Carol"));
}

#[test]
fn test_gql_with_distinct() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH DISTINCT p.city AS city RETURN city")
        .unwrap();

    assert_eq!(results.len(), 2, "Should have 2 unique cities");

    let cities: HashSet<_> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(cities.contains("NYC"));
    assert!(cities.contains("LA"));
}

#[test]
fn test_gql_with_order_by() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH p.name AS name, p.age AS age ORDER BY age DESC RETURN name")
        .unwrap();

    assert_eq!(results.len(), 5);

    let names: Vec<_> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(names[0], "Carol");
    assert_eq!(names[1], "Eve");
    assert_eq!(names[2], "Alice");
    assert_eq!(names[3], "Dave");
    assert_eq!(names[4], "Bob");
}

#[test]
fn test_gql_with_limit() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 3 RETURN name")
        .unwrap();

    assert_eq!(results.len(), 3, "Should return only 3 results");

    let names: Vec<_> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
}

#[test]
fn test_gql_with_limit_offset() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 2 OFFSET 2 RETURN name")
        .unwrap();

    assert_eq!(results.len(), 2, "Should return 2 results after skipping 2");

    let names: Vec<_> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert_eq!(names, vec!["Carol", "Dave"]);
}

#[test]
fn test_gql_with_global_aggregation() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH COUNT(p) AS total RETURN total")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(5));
}

#[test]
fn test_gql_with_avg_aggregation() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH AVG(p.age) AS avgAge RETURN avgAge")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Float(avg) = results[0] {
        assert!((avg - 30.0).abs() < 0.001);
    } else {
        panic!("Expected Float result");
    }
}

#[test]
fn test_gql_with_sum_aggregation() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH p.city AS city, SUM(p.age) AS totalAge RETURN city, totalAge")
        .unwrap();

    assert_eq!(results.len(), 2, "Should have 2 cities");

    let mut city_ages: HashMap<String, i64> = HashMap::new();
    for result in results {
        if let Value::Map(map) = result {
            let city = match map.get("city") {
                Some(Value::String(s)) => s.clone(),
                _ => continue,
            };
            let total = match map.get("totalAge") {
                Some(Value::Int(n)) => *n,
                _ => continue,
            };
            city_ages.insert(city, total);
        }
    }

    assert_eq!(city_ages.get("NYC"), Some(&83));
    assert_eq!(city_ages.get("LA"), Some(&67));
}

#[test]
fn test_gql_with_scope_reset() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH p.name AS name WHERE name = 'Alice' RETURN name")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn test_gql_with_collect_aggregation() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH p.city AS city, COLLECT(p.name) AS names RETURN city, names")
        .unwrap();

    assert_eq!(results.len(), 2, "Should have 2 cities");

    for result in results {
        if let Value::Map(map) = result {
            let city = match map.get("city") {
                Some(Value::String(s)) => s.clone(),
                _ => continue,
            };
            let names = match map.get("names") {
                Some(Value::List(list)) => list.clone(),
                _ => continue,
            };

            if city == "NYC" {
                assert_eq!(names.len(), 3, "NYC should have 3 people");
            } else if city == "LA" {
                assert_eq!(names.len(), 2, "LA should have 2 people");
            }
        }
    }
}

#[test]
fn test_gql_with_min_max_aggregation() {
    let graph = create_with_clause_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WITH MIN(p.age) AS youngest, MAX(p.age) AS oldest RETURN youngest, oldest")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("youngest"), Some(&Value::Int(25)));
        assert_eq!(map.get("oldest"), Some(&Value::Int(35)));
    } else {
        panic!("Expected Map result");
    }
}

#[test]
fn test_gql_with_parse_only() {
    let query = parse("MATCH (p:Person) WITH p.name AS name, COUNT(p) AS cnt WHERE cnt > 1 ORDER BY name LIMIT 10 RETURN name").unwrap();

    assert_eq!(query.with_clauses.len(), 1);
    let with_clause = &query.with_clauses[0];
    assert_eq!(with_clause.items.len(), 2);
    assert!(with_clause.where_clause.is_some());
    assert!(with_clause.order_clause.is_some());
    assert!(with_clause.limit_clause.is_some());
}

#[test]
fn test_gql_with_distinct_parse() {
    let query = parse("MATCH (p:Person) WITH DISTINCT p.city AS city RETURN city").unwrap();

    assert_eq!(query.with_clauses.len(), 1);
    assert!(query.with_clauses[0].distinct);
}

// =============================================================================
// List Indexing Tests
// =============================================================================

#[test]
fn test_gql_index_access_literal_list() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][0]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(1));

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][1]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(2));

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][2]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(3));
}

#[test]
fn test_gql_index_access_negative_index() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][-1]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(3));

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][-2]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(2));

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][-3]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(1));
}

#[test]
fn test_gql_index_access_out_of_bounds() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][10]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][-10]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_gql_index_access_on_null() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN null[0]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_gql_index_access_on_non_list() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN 'hello'[0]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);

    let results = snapshot.gql("MATCH (x) RETURN 42[0]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_gql_index_access_non_integer_index() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3]['foo']").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][1.5]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_gql_index_access_empty_list() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN [][0]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_gql_chained_index_access() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (x) RETURN [[1, 2], [3, 4]][0][1]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(2));

    let results = snapshot
        .gql("MATCH (x) RETURN [[1, 2], [3, 4]][1][0]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(3));
}

#[test]
fn test_gql_index_with_expression() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (x) RETURN [10, 20, 30][1 + 1]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(30));
}

#[test]
fn test_gql_index_access_on_property() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert(
        "scores".to_string(),
        Value::List(vec![Value::Int(95), Value::Int(87), Value::Int(92)]),
    );
    storage.add_vertex("Student", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (s:Student) RETURN s.scores[0]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(95));

    let results = snapshot
        .gql("MATCH (s:Student) RETURN s.scores[-1]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(92));
}

// =============================================================================
// List Slicing Tests
// =============================================================================

#[test]
fn test_gql_slice_full_range() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (x) RETURN [1, 2, 3, 4, 5][1..3]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::List(vec![Value::Int(2), Value::Int(3)]));
}

#[test]
fn test_gql_slice_open_start() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (x) RETURN [1, 2, 3, 4, 5][..3]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
    );
}

#[test]
fn test_gql_slice_open_end() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (x) RETURN [1, 2, 3, 4, 5][2..]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::List(vec![Value::Int(3), Value::Int(4), Value::Int(5)])
    );
}

#[test]
fn test_gql_slice_fully_open() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][..]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
    );
}

#[test]
fn test_gql_slice_negative_start() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (x) RETURN [1, 2, 3, 4, 5][-3..]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::List(vec![Value::Int(3), Value::Int(4), Value::Int(5)])
    );
}

#[test]
fn test_gql_slice_negative_end() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (x) RETURN [1, 2, 3, 4, 5][..-1]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::List(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4)
        ])
    );
}

#[test]
fn test_gql_slice_negative_both() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (x) RETURN [1, 2, 3, 4, 5][-3..-1]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::List(vec![Value::Int(3), Value::Int(4)]));
}

#[test]
fn test_gql_slice_bounds_clamping() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][10..20]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::List(vec![]));

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][1..100]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::List(vec![Value::Int(2), Value::Int(3)]));

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][-100..2]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::List(vec![Value::Int(1), Value::Int(2)]));
}

#[test]
fn test_gql_slice_empty_range() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][1..1]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::List(vec![]));

    let results = snapshot.gql("MATCH (x) RETURN [1, 2, 3][2..1]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::List(vec![]));
}

#[test]
fn test_gql_slice_empty_list() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN [][0..10]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::List(vec![]));
}

#[test]
fn test_gql_slice_on_null() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN null[0..2]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_gql_slice_on_non_list() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (x) RETURN 'hello'[0..2]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);

    let results = snapshot.gql("MATCH (x) RETURN 42[0..2]").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_gql_slice_on_property() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert(
        "history".to_string(),
        Value::List(vec![
            Value::Int(100),
            Value::Int(200),
            Value::Int(300),
            Value::Int(400),
            Value::Int(500),
        ]),
    );
    storage.add_vertex("Account", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (a:Account) RETURN a.history[..3]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::List(vec![Value::Int(100), Value::Int(200), Value::Int(300)])
    );

    let results = snapshot
        .gql("MATCH (a:Account) RETURN a.history[-2..]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::List(vec![Value::Int(400), Value::Int(500)])
    );
}

#[test]
fn test_gql_index_in_where_clause() {
    let mut storage = InMemoryGraph::new();

    let mut props1 = HashMap::new();
    props1.insert(
        "tags".to_string(),
        Value::List(vec![
            Value::String("rust".to_string()),
            Value::String("graph".to_string()),
        ]),
    );
    storage.add_vertex("Project", props1);

    let mut props2 = HashMap::new();
    props2.insert(
        "tags".to_string(),
        Value::List(vec![
            Value::String("python".to_string()),
            Value::String("ml".to_string()),
        ]),
    );
    storage.add_vertex("Project", props2);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Project) WHERE p.tags[0] = 'rust' RETURN p")
        .unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_gql_slice_in_return() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert(
        "numbers".to_string(),
        Value::List(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5),
        ]),
    );
    storage.add_vertex("Data", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (d:Data) RETURN d.numbers[1..4] AS middle")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        Value::List(vec![Value::Int(2), Value::Int(3), Value::Int(4)])
    );
}

#[test]
fn test_gql_combined_index_and_slice() {
    let graph = create_dummy_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (x) RETURN [1, 2, 3, 4, 5][1..4][0]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(2));

    let results = snapshot
        .gql("MATCH (x) RETURN [[1,2], [3,4], [5,6]][1..3][0]")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::List(vec![Value::Int(3), Value::Int(4)]));
}

// =============================================================================
// Pattern Comprehension Tests
// =============================================================================

#[test]
fn test_gql_pattern_comprehension_basic() {
    let graph = create_pattern_comprehension_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person {name: 'Alice'}) RETURN p.name, [(p)-[:FRIEND]->(f) | f.name] AS friendNames")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("p.name"), Some(&Value::from("Alice")));

        if let Some(Value::List(friends)) = map.get("friendNames") {
            assert_eq!(friends.len(), 2);
            let friend_names: Vec<&str> = friends.iter().filter_map(|v| v.as_str()).collect();
            assert!(friend_names.contains(&"Bob"));
            assert!(friend_names.contains(&"Carol"));
        } else {
            panic!(
                "Expected friendNames to be a list, got: {:?}",
                map.get("friendNames")
            );
        }
    } else {
        panic!("Expected map result");
    }
}

#[test]
fn test_gql_pattern_comprehension_empty_matches() {
    let graph = create_pattern_comprehension_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person {name: 'Dave'}) RETURN p.name, [(p)-[:FRIEND]->(f) | f.name] AS friendNames")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("p.name"), Some(&Value::from("Dave")));

        if let Some(Value::List(friends)) = map.get("friendNames") {
            assert!(friends.is_empty(), "Dave should have no friends");
        } else {
            panic!("Expected friendNames to be a list");
        }
    } else {
        panic!("Expected map result");
    }
}

#[test]
fn test_gql_pattern_comprehension_with_filter() {
    let graph = create_pattern_comprehension_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person {name: 'Alice'}) RETURN p.name, [(p)-[:FRIEND]->(f) WHERE f.age > 30 | f.name] AS olderFriends")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        if let Some(Value::List(friends)) = map.get("olderFriends") {
            assert_eq!(friends.len(), 1);
            assert_eq!(friends[0], Value::from("Carol"));
        } else {
            panic!("Expected olderFriends to be a list");
        }
    } else {
        panic!("Expected map result");
    }
}

#[test]
fn test_gql_pattern_comprehension_map_transform() {
    let graph = create_pattern_comprehension_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person {name: 'Alice'}) RETURN [(p)-[:FRIEND]->(f) | {name: f.name, age: f.age}] AS friends")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::List(friends) = &results[0] {
        assert_eq!(friends.len(), 2);
        for friend in friends {
            if let Value::Map(friend_map) = friend {
                assert!(friend_map.contains_key("name"));
                assert!(friend_map.contains_key("age"));
            } else {
                panic!("Expected friend to be a map");
            }
        }
    } else {
        panic!("Expected list result");
    }
}

#[test]
fn test_gql_pattern_comprehension_multiple_people() {
    let graph = create_pattern_comprehension_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name, [(p)-[:FRIEND]->(f) | f.name] AS friendNames ORDER BY p.name")
        .unwrap();

    assert_eq!(results.len(), 4);

    if let Value::Map(alice) = &results[0] {
        assert_eq!(alice.get("p.name"), Some(&Value::from("Alice")));
        if let Some(Value::List(friends)) = alice.get("friendNames") {
            assert_eq!(friends.len(), 2);
        }
    }

    if let Value::Map(bob) = &results[1] {
        assert_eq!(bob.get("p.name"), Some(&Value::from("Bob")));
        if let Some(Value::List(friends)) = bob.get("friendNames") {
            assert_eq!(friends.len(), 1);
        }
    }

    if let Value::Map(carol) = &results[2] {
        assert_eq!(carol.get("p.name"), Some(&Value::from("Carol")));
        if let Some(Value::List(friends)) = carol.get("friendNames") {
            assert_eq!(friends.len(), 1);
        }
    }

    if let Value::Map(dave) = &results[3] {
        assert_eq!(dave.get("p.name"), Some(&Value::from("Dave")));
        if let Some(Value::List(friends)) = dave.get("friendNames") {
            assert!(friends.is_empty());
        }
    }
}

#[test]
fn test_gql_pattern_comprehension_with_labels() {
    let graph = create_pattern_comprehension_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person {name: 'Alice'}) RETURN [(p)-[:FRIEND]->(f:Person) | f.name] AS friends")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::List(friends) = &results[0] {
        assert_eq!(friends.len(), 2);
    } else {
        panic!("Expected list result");
    }
}
