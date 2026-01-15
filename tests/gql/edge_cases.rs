//! Edge case tests for GQL
//!
//! This module contains:
//! - Error message tests
//! - Unicode and special character tests
//! - Numeric boundary tests
//! - Null handling tests
//! - Boolean property tests
//! - Social network integration tests
//! - Stress tests
//! - Phase 2.6 integration tests
//! - Phase 4.7 advanced integration tests
//! - Introspection function tests
//! - Inline WHERE tests
//! - Query parameter tests
//! - LET clause tests
//! - IS predicate tests

use intersteller::gql::{parse, GqlError};
use intersteller::storage::InMemoryGraph;
use intersteller::{Graph, Value};
use std::collections::{HashMap, HashSet};

// =============================================================================
// Helper Functions
// =============================================================================

/// Helper to create a basic test graph
fn create_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    storage.add_vertex("Person", bob_props);

    let mut charlie_props = HashMap::new();
    charlie_props.insert("name".to_string(), Value::from("Charlie"));
    charlie_props.insert("age".to_string(), Value::from(35i64));
    storage.add_vertex("Person", charlie_props);

    Graph::new(storage)
}

/// Helper to create a graph with rich edge relationships for pattern tests
fn create_pattern_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    let bob = storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    let carol = storage.add_vertex("Person", carol_props);

    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "WORKS_WITH", HashMap::new())
        .unwrap();

    Graph::new(storage)
}

/// Helper to create a graph with age property for property return tests
fn create_property_return_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    storage.add_vertex("Person", bob_props);

    Graph::new(storage)
}

/// Helper to create a graph for multi-hop traversal tests
fn create_multi_hop_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    let bob = storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    let carol = storage.add_vertex("Person", carol_props);

    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "KNOWS", HashMap::new())
        .unwrap();

    Graph::new(storage)
}

/// Helper to create a more comprehensive test graph for Phase 4.7 tests
fn create_phase47_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let people = vec![
        ("Alice", 30i64, "Engineering", 100000i64),
        ("Bob", 25i64, "Engineering", 80000i64),
        ("Carol", 35i64, "Sales", 90000i64),
        ("Dave", 28i64, "Sales", 75000i64),
        ("Eve", 22i64, "Engineering", 70000i64),
        ("Frank", 40i64, "Marketing", 95000i64),
        ("Grace", 33i64, "Marketing", 85000i64),
        ("Henry", 28i64, "Engineering", 82000i64),
    ];

    for (name, age, dept, salary) in people {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(name));
        props.insert("age".to_string(), Value::from(age));
        props.insert("department".to_string(), Value::from(dept));
        props.insert("salary".to_string(), Value::from(salary));
        storage.add_vertex("Person", props);
    }

    Graph::new(storage)
}

// =============================================================================
// Phase 5.4: Improved Error Messages Tests
// =============================================================================

#[test]
fn test_gql_parse_error_includes_position() {
    let result = parse("MATCH (n:Person RETURN n");
    assert!(result.is_err());

    if let Err(e) = result {
        let error_msg = format!("{}", e);
        assert!(
            error_msg.contains("position")
                || error_msg.contains("line")
                || error_msg.contains("-->"),
            "Error message should contain position info: {}",
            error_msg
        );
    }
}

#[test]
fn test_gql_parse_error_missing_clause_position() {
    let result = parse("MATCH (n:Person)");
    assert!(result.is_err());

    if let Err(e) = result {
        let error_msg = format!("{}", e);
        assert!(
            error_msg.contains("RETURN")
                || error_msg.contains("position")
                || error_msg.contains("-->"),
            "Error message should be helpful: {}",
            error_msg
        );
    }
}

#[test]
fn test_gql_compile_error_helpful_message() {
    let graph = Graph::in_memory();
    let snapshot = graph.snapshot();

    let result = snapshot.gql("MATCH (n:Person) RETURN x");
    assert!(result.is_err());

    if let Err(e) = result {
        let error_msg = format!("{}", e);
        assert!(
            error_msg.contains("x"),
            "Error message should mention the undefined variable: {}",
            error_msg
        );
        assert!(
            error_msg.contains("MATCH")
                || error_msg.contains("bind")
                || error_msg.contains("forget"),
            "Error message should suggest binding in MATCH: {}",
            error_msg
        );
    }
}

#[test]
fn test_gql_compile_error_duplicate_variable_message() {
    use intersteller::gql::CompileError;

    let err = CompileError::duplicate_variable("n");
    let error_msg = format!("{}", err);

    assert!(
        error_msg.contains("n"),
        "Error message should mention the duplicate variable: {}",
        error_msg
    );
    assert!(
        error_msg.contains("already defined") || error_msg.contains("duplicate"),
        "Error message should indicate it's a duplicate: {}",
        error_msg
    );
}

#[test]
fn test_gql_parse_error_span_extraction() {
    use intersteller::gql::{ParseError, Span};

    let err = ParseError::invalid_literal("abc", Span::new(5, 8), "expected integer");

    let span = err.span();
    assert!(span.is_some());
    let span = span.unwrap();
    assert_eq!(span.start, 5);
    assert_eq!(span.end, 8);

    let msg = format!("{}", err);
    assert!(msg.contains("abc"));
    assert!(msg.contains("5"));
    assert!(msg.contains("expected integer"));
}

#[test]
fn test_gql_compile_error_suggestions() {
    use intersteller::gql::CompileError;

    let err = CompileError::undefined_variable("myVar");
    let msg = format!("{}", err);
    assert!(msg.contains("myVar"));
    assert!(msg.contains("Did you forget") || msg.contains("MATCH"));

    let err = CompileError::duplicate_variable("n");
    let msg = format!("{}", err);
    assert!(msg.contains("n"));
    assert!(msg.contains("already defined"));

    let err = CompileError::aggregate_in_where("COUNT");
    let msg = format!("{}", err);
    assert!(msg.contains("COUNT"));
    assert!(msg.contains("WHERE"));
}

#[test]
fn test_gql_compile_error_empty_pattern_message() {
    use intersteller::gql::CompileError;

    let err = CompileError::EmptyPattern;
    let msg = format!("{}", err);

    assert!(
        msg.contains("empty") || msg.contains("Empty"),
        "Error should mention empty pattern: {}",
        msg
    );
    assert!(
        msg.contains("MATCH") || msg.contains("node"),
        "Error should suggest how to fix: {}",
        msg
    );
}

#[test]
fn test_gql_compile_error_pattern_start_message() {
    use intersteller::gql::CompileError;

    let err = CompileError::PatternMustStartWithNode;
    let msg = format!("{}", err);

    assert!(
        msg.contains("start") || msg.contains("node"),
        "Error should explain pattern structure: {}",
        msg
    );
}

// =============================================================================
// Unicode and Special Characters Tests
// =============================================================================

#[test]
fn test_gql_unicode_japanese_characters() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("田中太郎"));
    props.insert("city".to_string(), Value::from("東京"));
    storage.add_vertex("Person", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("佐藤花子"));
    props2.insert("city".to_string(), Value::from("大阪"));
    storage.add_vertex("Person", props2);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = "MATCH (p:Person) WHERE p.name = '田中太郎' RETURN p.name, p.city";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("田中太郎")));
        assert_eq!(row.get("p.city"), Some(&Value::from("東京")));
    } else {
        panic!("Expected map result");
    }
}

#[test]
fn test_gql_unicode_german_umlauts() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Müller"));
    props.insert("city".to_string(), Value::from("München"));
    storage.add_vertex("Person", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Schröder"));
    props2.insert("city".to_string(), Value::from("Köln"));
    storage.add_vertex("Person", props2);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) RETURN p.name ORDER BY p.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 2);

    if let Value::String(name) = &results[0] {
        assert_eq!(name, "Müller");
    }
}

#[test]
fn test_gql_unicode_russian_cyrillic() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Иванов"));
    props.insert("city".to_string(), Value::from("Москва"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = "MATCH (p:Person) WHERE p.city = 'Москва' RETURN p.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Иванов")));
    }
}

#[test]
fn test_gql_unicode_arabic() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("محمد"));
    props.insert("city".to_string(), Value::from("القاهرة"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) RETURN p.name, p.city"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("محمد")));
        assert_eq!(row.get("p.city"), Some(&Value::from("القاهرة")));
    }
}

#[test]
fn test_gql_unicode_emoji() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Test User 🎉"));
    props.insert("status".to_string(), Value::from("😀👍🚀"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) RETURN p.name, p.status"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Test User 🎉")));
        assert_eq!(row.get("p.status"), Some(&Value::from("😀👍🚀")));
    }
}

#[test]
fn test_gql_unicode_mixed_scripts() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert(
        "description".to_string(),
        Value::from("Hello 世界 Привет مرحبا 🌍"),
    );
    storage.add_vertex("Item", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (i:Item) RETURN i.description"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(
            row.get("i.description"),
            Some(&Value::from("Hello 世界 Привет مرحبا 🌍"))
        );
    }
}

#[test]
fn test_gql_special_chars_whitespace() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("bio".to_string(), Value::from("Line 1\nLine 2\nLine 3"));
    props.insert("data".to_string(), Value::from("Col1\tCol2\tCol3"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) RETURN p.bio, p.data"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        let bio = row.get("p.bio").unwrap();
        if let Value::String(s) = bio {
            assert!(s.contains('\n'));
        }
    }
}

#[test]
fn test_gql_empty_string_property() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("nickname".to_string(), Value::from(""));
    storage.add_vertex("Person", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    props2.insert("nickname".to_string(), Value::from("Bobby"));
    storage.add_vertex("Person", props2);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = "MATCH (p:Person) WHERE p.nickname = '' RETURN p.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Alice")));
    }
}

// =============================================================================
// Numeric Boundary Tests
// =============================================================================

#[test]
fn test_gql_large_integer_values() {
    let mut storage = InMemoryGraph::new();

    let large_val = i64::MAX - 1000;
    let mut props = HashMap::new();
    props.insert("id".to_string(), Value::Int(large_val));
    props.insert("name".to_string(), Value::from("BigNum"));
    storage.add_vertex("Entity", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (e:Entity) RETURN e.id, e.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("e.id"), Some(&Value::Int(large_val)));
    }
}

#[test]
fn test_gql_negative_integer_values() {
    let mut storage = InMemoryGraph::new();

    let small_val = i64::MIN + 1000;
    let mut props = HashMap::new();
    props.insert("balance".to_string(), Value::Int(small_val));
    props.insert("name".to_string(), Value::from("Debt"));
    storage.add_vertex("Account", props);

    let mut props2 = HashMap::new();
    props2.insert("balance".to_string(), Value::Int(1000i64));
    props2.insert("name".to_string(), Value::from("Savings"));
    storage.add_vertex("Account", props2);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (a:Account) WHERE a.balance < 0 RETURN a.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("a.name"), Some(&Value::from("Debt")));
    }
}

#[test]
fn test_gql_zero_comparisons() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("value".to_string(), Value::Int(0i64));
    props.insert("name".to_string(), Value::from("Zero"));
    storage.add_vertex("Number", props);

    let mut props2 = HashMap::new();
    props2.insert("value".to_string(), Value::Int(1i64));
    props2.insert("name".to_string(), Value::from("One"));
    storage.add_vertex("Number", props2);

    let mut props3 = HashMap::new();
    props3.insert("value".to_string(), Value::Int(-1i64));
    props3.insert("name".to_string(), Value::from("NegOne"));
    storage.add_vertex("Number", props3);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (n:Number) WHERE n.value = 0 RETURN n.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    let query2 = r#"MATCH (n:Number) WHERE n.value >= 0 RETURN n.name ORDER BY n.value"#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    assert_eq!(results2.len(), 2);
}

#[test]
fn test_gql_float_precision() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("rate".to_string(), Value::Float(0.1 + 0.2));
    props.insert("name".to_string(), Value::from("FloatTest"));
    storage.add_vertex("Test", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (t:Test) RETURN t.rate"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        if let Some(Value::Float(f)) = row.get("t.rate") {
            assert!((f - 0.3).abs() < 0.0001);
        }
    }
}

#[test]
fn test_gql_small_float_values() {
    let mut storage = InMemoryGraph::new();

    let tiny = 1e-10f64;
    let mut props = HashMap::new();
    props.insert("epsilon".to_string(), Value::Float(tiny));
    storage.add_vertex("Math", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (m:Math) RETURN m.epsilon"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("m.epsilon"), Some(&Value::Float(tiny)));
    }
}

// =============================================================================
// Null Handling Tests
// =============================================================================

#[test]
fn test_gql_missing_property_returns_null() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    storage.add_vertex("Person", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) RETURN p.name, p.age"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();

    assert_eq!(
        results.len(),
        0,
        "Missing property should filter out result"
    );
}

#[test]
fn test_gql_is_null_missing_property() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    storage.add_vertex("Person", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    props2.insert("age".to_string(), Value::Int(30i64));
    storage.add_vertex("Person", props2);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) WHERE p.age IS NULL RETURN p.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Alice")));
    }
}

#[test]
fn test_gql_is_not_null() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    storage.add_vertex("Person", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    props2.insert("email".to_string(), Value::from("bob@example.com"));
    storage.add_vertex("Person", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Charlie"));
    props3.insert("email".to_string(), Value::from("charlie@example.com"));
    storage.add_vertex("Person", props3);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) WHERE p.email IS NOT NULL RETURN p.name ORDER BY p.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 2);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Bob")));
    }
}

#[test]
fn test_gql_explicit_null_property() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("middlename".to_string(), Value::Null);
    storage.add_vertex("Person", props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) WHERE p.middlename IS NULL RETURN p.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);
}

// =============================================================================
// Boolean Property Tests
// =============================================================================

#[test]
fn test_gql_boolean_property_true() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("active".to_string(), Value::Bool(true));
    storage.add_vertex("User", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    props2.insert("active".to_string(), Value::Bool(false));
    storage.add_vertex("User", props2);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (u:User) WHERE u.active = true RETURN u.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("u.name"), Some(&Value::from("Alice")));
    }
}

#[test]
fn test_gql_boolean_property_false() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Alice"));
    props.insert("verified".to_string(), Value::Bool(true));
    storage.add_vertex("User", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    props2.insert("verified".to_string(), Value::Bool(false));
    storage.add_vertex("User", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Charlie"));
    props3.insert("verified".to_string(), Value::Bool(false));
    storage.add_vertex("User", props3);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (u:User) WHERE u.verified = false RETURN u.name ORDER BY u.name"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 2);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("u.name"), Some(&Value::from("Bob")));
    }
}

// =============================================================================
// Social Network Integration Tests
// =============================================================================

fn create_social_network_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::Int(28i64));
    alice_props.insert("city".to_string(), Value::from("NYC"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::Int(35i64));
    bob_props.insert("city".to_string(), Value::from("LA"));
    let bob = storage.add_vertex("Person", bob_props);

    let mut charlie_props = HashMap::new();
    charlie_props.insert("name".to_string(), Value::from("Charlie"));
    charlie_props.insert("age".to_string(), Value::Int(42i64));
    charlie_props.insert("city".to_string(), Value::from("NYC"));
    let charlie = storage.add_vertex("Person", charlie_props);

    let mut diana_props = HashMap::new();
    diana_props.insert("name".to_string(), Value::from("Diana"));
    diana_props.insert("age".to_string(), Value::Int(31i64));
    diana_props.insert("city".to_string(), Value::from("Chicago"));
    let diana = storage.add_vertex("Person", diana_props);

    let mut eve_props = HashMap::new();
    eve_props.insert("name".to_string(), Value::from("Eve"));
    eve_props.insert("age".to_string(), Value::Int(25i64));
    eve_props.insert("city".to_string(), Value::from("LA"));
    let eve = storage.add_vertex("Person", eve_props);

    let mut frank_props = HashMap::new();
    frank_props.insert("name".to_string(), Value::from("Frank"));
    frank_props.insert("age".to_string(), Value::Int(55i64));
    frank_props.insert("city".to_string(), Value::from("NYC"));
    let frank = storage.add_vertex("Person", frank_props);

    let mut grace_props = HashMap::new();
    grace_props.insert("name".to_string(), Value::from("Grace"));
    grace_props.insert("age".to_string(), Value::Int(29i64));
    grace_props.insert("city".to_string(), Value::from("Boston"));
    let grace = storage.add_vertex("Person", grace_props);

    let mut henry_props = HashMap::new();
    henry_props.insert("name".to_string(), Value::from("Henry"));
    henry_props.insert("age".to_string(), Value::Int(38i64));
    henry_props.insert("city".to_string(), Value::from("Seattle"));
    let henry = storage.add_vertex("Person", henry_props);

    // KNOWS relationships
    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, charlie, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, diana, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, alice, "KNOWS", HashMap::new())
        .unwrap();
    storage.add_edge(bob, eve, "KNOWS", HashMap::new()).unwrap();
    storage
        .add_edge(bob, frank, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, alice, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, diana, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, grace, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, alice, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, charlie, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, eve, "KNOWS", HashMap::new())
        .unwrap();
    storage.add_edge(eve, bob, "KNOWS", HashMap::new()).unwrap();
    storage
        .add_edge(eve, diana, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(eve, henry, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(frank, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(frank, grace, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(grace, charlie, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(grace, frank, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(grace, henry, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(henry, eve, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(henry, grace, "KNOWS", HashMap::new())
        .unwrap();

    Graph::new(storage)
}

#[test]
fn test_gql_social_network_direct_friends() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(friend:Person) RETURN friend.name ORDER BY friend.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 3);

    let names: Vec<&str> = results
        .iter()
        .filter_map(|r| {
            if let Value::String(name) = r {
                return Some(name.as_str());
            }
            None
        })
        .collect();

    assert_eq!(names, vec!["Bob", "Charlie", "Diana"]);
}

#[test]
fn test_gql_social_network_friends_of_friends() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(:Person)-[:KNOWS]->(fof:Person) WHERE fof.name <> 'Alice' RETURN DISTINCT fof.name ORDER BY fof.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();

    let names: Vec<&str> = results
        .iter()
        .filter_map(|r| {
            if let Value::String(name) = r {
                return Some(name.as_str());
            }
            None
        })
        .collect();

    assert!(names.contains(&"Eve"));
    assert!(names.contains(&"Frank"));
    assert!(names.contains(&"Grace"));
}

#[test]
fn test_gql_social_network_filter_by_age() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name, p.age ORDER BY p.age DESC";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 5);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Frank")));
        assert_eq!(row.get("p.age"), Some(&Value::Int(55i64)));
    }
}

#[test]
fn test_gql_social_network_city_filter_with_limit() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        WHERE p.city = 'NYC'
        RETURN p.name, p.age
        ORDER BY p.age
        LIMIT 2
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 2);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Alice")));
    }
    if let Value::Map(row) = &results[1] {
        assert_eq!(row.get("p.name"), Some(&Value::from("Charlie")));
    }
}

#[test]
fn test_gql_social_network_count_friends() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)-[:KNOWS]->(friend:Person)
        RETURN p.name, COUNT(friend) AS friend_count
        ORDER BY friend_count DESC, p.name
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 8);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("friend_count"), Some(&Value::Int(3i64)));
    }
}

#[test]
fn test_gql_social_network_avg_friend_age() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = "MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(friend:Person) RETURN AVG(friend.age) AS avg_age";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        if let Some(Value::Float(avg)) = row.get("avg_age") {
            assert!(
                (avg - 36.0).abs() < 0.01,
                "Expected average ~36.0, got {}",
                avg
            );
        } else {
            panic!("Expected float avg_age, got {:?}", row.get("avg_age"));
        }
    } else {
        panic!("Expected Map result, got {:?}", results[0]);
    }
}

#[test]
fn test_gql_social_network_coworkers() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = "MATCH (p1:Person)-[:KNOWS]->(c:Person) RETURN p1.name, c.name AS friend ORDER BY p1.name, friend LIMIT 5";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert!(!results.is_empty());
}

#[test]
fn test_gql_social_network_city_strangers() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        WHERE p.city = 'NYC'
        RETURN p.name
        ORDER BY p.name
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_gql_social_network_variable_length_path() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)-[:KNOWS*2..3]->(target:Person)
        WHERE p.name = 'Alice'
        RETURN DISTINCT target.name
        ORDER BY target.name
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert!(!results.is_empty());
}

#[test]
fn test_gql_social_network_collect_names() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        WHERE p.city = 'LA'
        RETURN COLLECT(p.name) AS la_people
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        if let Some(Value::List(names)) = row.get("la_people") {
            assert_eq!(names.len(), 2);
        } else {
            panic!("Expected list");
        }
    }
}

#[test]
fn test_gql_social_network_combined_where_and() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        WHERE p.age > 25 AND p.age < 40
        RETURN p.name, p.age
        ORDER BY p.age
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_gql_social_network_combined_where_or() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        WHERE p.city = 'NYC' OR p.city = 'LA'
        RETURN p.name
        ORDER BY p.name
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_gql_social_network_min_max_age() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        RETURN MIN(p.age) AS youngest, MAX(p.age) AS oldest
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("youngest"), Some(&Value::Int(25i64)));
        assert_eq!(row.get("oldest"), Some(&Value::Int(55i64)));
    }
}

#[test]
fn test_gql_social_network_sum_ages() {
    let graph = create_social_network_graph();
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (p:Person)
        WHERE p.city = 'NYC'
        RETURN SUM(p.age) AS total_age
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("total_age"), Some(&Value::Int(125i64)));
    }
}

// =============================================================================
// Stress Tests
// =============================================================================

#[test]
fn test_gql_stress_1000_vertices() {
    let mut storage = InMemoryGraph::new();

    for i in 0..1000 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Person{}", i)));
        props.insert("index".to_string(), Value::Int(i as i64));
        props.insert("group".to_string(), Value::Int((i % 10) as i64));
        storage.add_vertex("Person", props);
    }

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (p:Person) RETURN COUNT(p) AS total"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("total"), Some(&Value::Int(1000i64)));
    }

    let query2 = r#"MATCH (p:Person) WHERE p.group = 5 RETURN COUNT(p) AS count"#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    assert_eq!(results2.len(), 1);

    if let Value::Map(row) = &results2[0] {
        assert_eq!(row.get("count"), Some(&Value::Int(100i64)));
    }
}

#[test]
fn test_gql_stress_dense_graph() {
    let mut storage = InMemoryGraph::new();

    let mut vertex_ids = Vec::new();
    for i in 0..50 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Node{}", i)));
        props.insert("tier".to_string(), Value::Int((i % 5) as i64));
        let id = storage.add_vertex("Node", props);
        vertex_ids.push(id);
    }

    for i in 0..50 {
        for j in 1..=5 {
            let target = (i + j * 7) % 50;
            if i != target {
                storage
                    .add_edge(
                        vertex_ids[i],
                        vertex_ids[target],
                        "CONNECTS",
                        HashMap::new(),
                    )
                    .unwrap();
            }
        }
    }

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (a:Node)-[:CONNECTS]->(b:Node) RETURN COUNT(*) AS edge_count"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        if let Some(Value::Int(count)) = row.get("edge_count") {
            assert!(*count > 200);
        }
    }

    let query2 = r#"
        MATCH (a:Node)-[:CONNECTS]->(b:Node)
        WHERE a.tier = 0
        RETURN COUNT(*) AS connections
    "#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    assert_eq!(results2.len(), 1);
}

#[test]
fn test_gql_stress_large_aggregation() {
    let mut storage = InMemoryGraph::new();

    for i in 0..500 {
        let mut props = HashMap::new();
        props.insert("id".to_string(), Value::Int(i as i64));
        props.insert("amount".to_string(), Value::Float((i as f64) * 10.5));
        props.insert("category".to_string(), Value::from(format!("Cat{}", i % 5)));
        storage.add_vertex("Transaction", props);
    }

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"MATCH (t:Transaction) RETURN SUM(t.amount) AS total"#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        if let Some(Value::Float(total)) = row.get("total") {
            let expected = 10.5 * 499.0 * 500.0 / 2.0;
            assert!((total - expected).abs() < 1.0);
        }
    }

    let query2 = r#"
        MATCH (t:Transaction)
        RETURN t.category, AVG(t.amount) AS avg_amount, COUNT(t) AS count
        ORDER BY t.category
    "#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    assert_eq!(results2.len(), 5);

    if let Value::Map(row) = &results2[0] {
        assert_eq!(row.get("count"), Some(&Value::Int(100i64)));
    }
}

#[test]
fn test_gql_stress_large_order_by() {
    let mut storage = InMemoryGraph::new();

    for i in 0..200 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Item{:03}", i)));
        let score = ((i * 17 + 23) % 1000) as i64;
        props.insert("score".to_string(), Value::Int(score));
        storage.add_vertex("Item", props);
    }

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (i:Item)
        RETURN i.name, i.score
        ORDER BY i.score DESC
        LIMIT 10
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 10);

    let mut prev_score = i64::MAX;
    for result in &results {
        if let Value::Map(row) = result {
            if let Some(Value::Int(score)) = row.get("i.score") {
                assert!(
                    *score <= prev_score,
                    "Results should be in descending order"
                );
                prev_score = *score;
            }
        }
    }
}

#[test]
fn test_gql_stress_large_offset() {
    let mut storage = InMemoryGraph::new();

    for i in 0..300 {
        let mut props = HashMap::new();
        props.insert("index".to_string(), Value::Int(i as i64));
        storage.add_vertex("Record", props);
    }

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = "MATCH (r:Record) RETURN r.index ORDER BY r.index LIMIT 1000 OFFSET 290";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 10);

    if let Value::Map(row) = &results[0] {
        assert_eq!(row.get("r.index"), Some(&Value::Int(290i64)));
    }
}

#[test]
fn test_gql_stress_multi_hop_traversal() {
    let mut storage = InMemoryGraph::new();

    let mut vertex_ids = Vec::new();
    for i in 0..100 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Node{}", i)));
        props.insert("depth".to_string(), Value::Int(i as i64));
        let id = storage.add_vertex("ChainNode", props);
        vertex_ids.push(id);
    }

    for i in 0..99 {
        storage
            .add_edge(vertex_ids[i], vertex_ids[i + 1], "NEXT", HashMap::new())
            .unwrap();
    }

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = "MATCH (n:ChainNode {name: 'Node0'})-[:NEXT]->(next:ChainNode) RETURN next.name";
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::String(name) = &results[0] {
        assert_eq!(name, "Node1");
    } else {
        panic!("Expected String result, got {:?}", results[0]);
    }

    let query2 =
        "MATCH (n:ChainNode {name: 'Node0'})-[:NEXT*5]->(target:ChainNode) RETURN target.name";
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    assert_eq!(results2.len(), 1);

    if let Value::String(name) = &results2[0] {
        assert_eq!(name, "Node5");
    } else {
        panic!("Expected String result, got {:?}", results2[0]);
    }
}

#[test]
fn test_gql_stress_multiple_labels() {
    let mut storage = InMemoryGraph::new();

    for i in 0..100 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Person{}", i)));
        storage.add_vertex("Person", props);
    }
    for i in 0..50 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Company{}", i)));
        storage.add_vertex("Company", props);
    }
    for i in 0..75 {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from(format!("Product{}", i)));
        storage.add_vertex("Product", props);
    }

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query1 = r#"MATCH (p:Person) RETURN COUNT(p) AS count"#;
    let results1: Vec<_> = snapshot.gql(query1).unwrap();
    if let Value::Map(row) = &results1[0] {
        assert_eq!(row.get("count"), Some(&Value::Int(100i64)));
    }

    let query2 = r#"MATCH (c:Company) RETURN COUNT(c) AS count"#;
    let results2: Vec<_> = snapshot.gql(query2).unwrap();
    if let Value::Map(row) = &results2[0] {
        assert_eq!(row.get("count"), Some(&Value::Int(50i64)));
    }

    let query3 = r#"MATCH (p:Product) RETURN COUNT(p) AS count"#;
    let results3: Vec<_> = snapshot.gql(query3).unwrap();
    if let Value::Map(row) = &results3[0] {
        assert_eq!(row.get("count"), Some(&Value::Int(75i64)));
    }
}

#[test]
fn test_gql_stress_distinct_many_duplicates() {
    let mut storage = InMemoryGraph::new();

    for i in 0..500 {
        let mut props = HashMap::new();
        props.insert("id".to_string(), Value::Int(i as i64));
        props.insert(
            "category".to_string(),
            Value::from(format!("Category{}", i % 10)),
        );
        storage.add_vertex("Item", props);
    }

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (i:Item)
        RETURN DISTINCT i.category
        ORDER BY i.category
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 10);
}

#[test]
fn test_gql_stress_complex_where() {
    let mut storage = InMemoryGraph::new();

    for i in 0..200 {
        let mut props = HashMap::new();
        props.insert("id".to_string(), Value::Int(i as i64));
        props.insert("value".to_string(), Value::Int((i * 3) as i64));
        props.insert("active".to_string(), Value::Bool(i % 2 == 0));
        props.insert("tier".to_string(), Value::from(format!("T{}", i % 4)));
        storage.add_vertex("Entity", props);
    }

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let query = r#"
        MATCH (e:Entity)
        WHERE e.active = true AND (e.tier = 'T0' OR e.tier = 'T2') AND e.value > 100
        RETURN COUNT(e) AS count
    "#;
    let results: Vec<_> = snapshot.gql(query).unwrap();
    assert_eq!(results.len(), 1);

    if let Value::Map(row) = &results[0] {
        if let Some(Value::Int(count)) = row.get("count") {
            assert!(*count > 0);
        }
    }
}

// =============================================================================
// Phase 2.6: Integration Tests - Patterns
// =============================================================================

#[test]
fn test_gql_edge_traversal_phase_2_6() {
    let graph = create_pattern_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) RETURN friend")
        .unwrap();
    assert_eq!(
        results.len(),
        2,
        "Alice should know 2 people (Bob and Carol)"
    );

    let results = snapshot
        .gql("MATCH (b:Person {name: 'Bob'})<-[:KNOWS]-(source) RETURN source")
        .unwrap();
    assert_eq!(results.len(), 1, "Bob should be known by 1 person (Alice)");

    let results = snapshot
        .gql("MATCH (b:Person {name: 'Bob'})-[:KNOWS]-(connected) RETURN connected")
        .unwrap();
    assert_eq!(
        results.len(),
        1,
        "Bob should be connected to 1 person via KNOWS (Alice)"
    );
}

#[test]
fn test_gql_property_return_phase_2_6() {
    let graph = create_property_return_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(results.len(), 2, "Should find 2 Person vertices");

    let names: Vec<&str> = results
        .iter()
        .filter_map(|v| match v {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        })
        .collect();

    assert!(
        names.contains(&"Alice"),
        "Results should contain Alice, got: {:?}",
        names
    );
    assert!(
        names.contains(&"Bob"),
        "Results should contain Bob, got: {:?}",
        names
    );
}

#[test]
fn test_gql_multi_hop_phase_2_6() {
    let graph = create_multi_hop_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name")
        .unwrap();

    assert_eq!(results.len(), 1, "Should find exactly one path to Carol");
    assert_eq!(
        results[0],
        Value::String("Carol".to_string()),
        "Should find Carol at the end of two-hop traversal"
    );
}

#[test]
fn test_gql_comprehensive_edge_traversal() {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    let bob = storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    let carol = storage.add_vertex("Person", carol_props);

    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "WORKS_WITH", HashMap::new())
        .unwrap();

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(friend) RETURN friend")
        .unwrap();
    assert_eq!(results.len(), 2, "Alice knows 2 people");

    let results = snapshot
        .gql("MATCH (b:Person {name: 'Bob'})<-[:KNOWS]-(source) RETURN source")
        .unwrap();
    assert_eq!(results.len(), 1, "Bob is known by 1 person");

    let results = snapshot
        .gql("MATCH (b:Person {name: 'Bob'})-[:KNOWS]-(connected) RETURN connected")
        .unwrap();
    assert_eq!(
        results.len(),
        1,
        "Bob connected via KNOWS to 1 person (Alice)"
    );
}

#[test]
fn test_gql_property_return_values() {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    storage.add_vertex("Person", bob_props);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN p.name").unwrap();
    assert_eq!(results.len(), 2);

    assert!(results.contains(&Value::String("Alice".to_string())));
    assert!(results.contains(&Value::String("Bob".to_string())));
}

#[test]
fn test_gql_multi_hop_with_property_filters() {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    let bob = storage.add_vertex("Person", bob_props);

    let mut carol_props = HashMap::new();
    carol_props.insert("name".to_string(), Value::from("Carol"));
    let carol = storage.add_vertex("Person", carol_props);

    let mut dave_props = HashMap::new();
    dave_props.insert("name".to_string(), Value::from("Dave"));
    let dave = storage.add_vertex("Person", dave_props);

    storage
        .add_edge(alice, bob, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, carol, "KNOWS", HashMap::new())
        .unwrap();
    storage
        .add_edge(alice, dave, "KNOWS", HashMap::new())
        .unwrap();

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Carol".to_string()));
}

// =============================================================================
// Phase 4.7: Advanced Integration Tests
// =============================================================================

#[test]
fn test_gql_order_by_multiple_columns() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.department, p.name, p.salary ORDER BY p.department, p.salary DESC")
        .unwrap();

    assert_eq!(results.len(), 8, "Should return all 8 people");

    let entries: Vec<(String, String, i64)> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                let dept = match map.get("p.department") {
                    Some(Value::String(s)) => s.clone(),
                    _ => return None,
                };
                let name = match map.get("p.name") {
                    Some(Value::String(s)) => s.clone(),
                    _ => return None,
                };
                let salary = match map.get("p.salary") {
                    Some(Value::Int(n)) => *n,
                    _ => return None,
                };
                Some((dept, name, salary))
            } else {
                None
            }
        })
        .collect();

    assert_eq!(entries[0].0, "Engineering");
    assert_eq!(entries[0].1, "Alice");
    assert_eq!(entries[0].2, 100000);
}

#[test]
fn test_gql_order_by_mixed_directions() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.department, p.name, p.age ORDER BY p.department ASC, p.age DESC")
        .unwrap();

    let entries: Vec<(String, i64)> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                let dept = match map.get("p.department") {
                    Some(Value::String(s)) => s.clone(),
                    _ => return None,
                };
                let age = match map.get("p.age") {
                    Some(Value::Int(n)) => *n,
                    _ => return None,
                };
                Some((dept, age))
            } else {
                None
            }
        })
        .collect();

    assert_eq!(entries[0].0, "Engineering");
    assert_eq!(entries[0].1, 30);

    let engineering_ages: Vec<i64> = entries
        .iter()
        .filter(|(d, _)| d == "Engineering")
        .map(|(_, a)| *a)
        .collect();
    assert_eq!(
        engineering_ages,
        vec![30, 28, 25, 22],
        "Engineering should be sorted by age DESC"
    );
}

#[test]
fn test_gql_limit_with_order_top_n() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name, p.salary ORDER BY p.salary DESC LIMIT 3")
        .unwrap();

    assert_eq!(results.len(), 3, "Should return exactly 3 results");

    let salaries: Vec<i64> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                match map.get("p.salary") {
                    Some(Value::Int(n)) => Some(*n),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        salaries,
        vec![100000, 95000, 90000],
        "Should return top 3 salaries"
    );
}

#[test]
fn test_gql_offset_pagination() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.salary ORDER BY p.salary DESC LIMIT 3 OFFSET 3")
        .unwrap();

    assert_eq!(results.len(), 3, "Should return 3 results for page 2");

    let salaries: Vec<i64> = results
        .iter()
        .filter_map(|v| match v {
            Value::Int(n) => Some(*n),
            _ => None,
        })
        .collect();

    assert_eq!(
        salaries,
        vec![85000, 82000, 80000],
        "Should return correct page of salaries"
    );
}

#[test]
fn test_gql_aggregation_with_filter() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.department = 'Engineering' RETURN count(*) AS count, sum(p.salary) AS total_salary, avg(p.salary) AS avg_salary")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("count"),
            Some(&Value::Int(4)),
            "Should have 4 engineers"
        );
        assert_eq!(
            map.get("total_salary"),
            Some(&Value::Int(332000)),
            "Total engineering salary"
        );
        if let Some(Value::Float(avg)) = map.get("avg_salary") {
            assert!(
                (avg - 83000.0).abs() < 0.01,
                "Average engineering salary should be 83000"
            );
        }
    } else {
        panic!("Expected Map result");
    }
}

#[test]
fn test_gql_aggregations_with_aliases() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN count(*) AS headcount, min(p.age) AS youngest, max(p.age) AS oldest, avg(p.age) AS avg_age")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("headcount"), Some(&Value::Int(8)));
        assert_eq!(map.get("youngest"), Some(&Value::Int(22)));
        assert_eq!(map.get("oldest"), Some(&Value::Int(40)));
        if let Some(Value::Float(avg)) = map.get("avg_age") {
            assert!((avg - 30.125).abs() < 0.01, "Average age should be ~30.125");
        }
    } else {
        panic!("Expected Map result");
    }
}

#[test]
fn test_gql_collect_all_values() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.department = 'Marketing' RETURN collect(p.name) AS names")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        if let Some(Value::List(names)) = map.get("names") {
            assert_eq!(names.len(), 2, "Marketing has 2 people");
            let name_set: std::collections::HashSet<_> = names.iter().collect();
            assert!(name_set.contains(&Value::String("Frank".to_string())));
            assert!(name_set.contains(&Value::String("Grace".to_string())));
        } else {
            panic!("Expected List for names");
        }
    } else {
        panic!("Expected Map result");
    }
}

#[test]
fn test_gql_count_distinct_departments() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN count(DISTINCT p.department) AS num_departments")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("num_departments"),
            Some(&Value::Int(3)),
            "Should have 3 unique departments"
        );
    } else {
        panic!("Expected Map result");
    }
}

#[test]
fn test_gql_single_aggregation_no_alias() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot.gql("MATCH (p:Person) RETURN count(*)").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(8), "Should count all 8 people");
}

#[test]
fn test_gql_order_by_with_nulls() {
    let mut storage = InMemoryGraph::new();

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), Value::from("Alice"));
    props1.insert("score".to_string(), Value::from(85i64));
    storage.add_vertex("Person", props1);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Bob"));
    storage.add_vertex("Person", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Carol"));
    props3.insert("score".to_string(), Value::from(90i64));
    storage.add_vertex("Person", props3);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name, p.score ORDER BY p.score")
        .unwrap();

    assert_eq!(results.len(), 2, "Should only return 2 people with scores");

    let scores: Vec<i64> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                match map.get("p.score") {
                    Some(Value::Int(n)) => Some(*n),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect();

    assert_eq!(scores, vec![85, 90], "Scores should be in ascending order");
}

#[test]
fn test_gql_limit_zero() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name LIMIT 0")
        .unwrap();

    assert_eq!(results.len(), 0, "LIMIT 0 should return empty result");
}

#[test]
fn test_gql_offset_beyond_results() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name LIMIT 10 OFFSET 100")
        .unwrap();

    assert_eq!(
        results.len(),
        0,
        "OFFSET beyond result set should return empty"
    );
}

#[test]
fn test_gql_combined_where_order_limit() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 25 RETURN p.name, p.age ORDER BY p.age ASC LIMIT 2")
        .unwrap();

    assert_eq!(results.len(), 2);

    let ages: Vec<i64> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                match map.get("p.age") {
                    Some(Value::Int(n)) => Some(*n),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect();

    assert!(ages[0] == 28, "First should be age 28");
    assert!(ages[1] == 28 || ages[1] == 30, "Second should be 28 or 30");
}

#[test]
fn test_gql_sum_floats() {
    let mut storage = InMemoryGraph::new();

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), Value::from("Product A"));
    props1.insert("price".to_string(), Value::Float(19.99));
    storage.add_vertex("Product", props1);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Product B"));
    props2.insert("price".to_string(), Value::Float(29.99));
    storage.add_vertex("Product", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("Product C"));
    props3.insert("price".to_string(), Value::Float(9.99));
    storage.add_vertex("Product", props3);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Product) RETURN sum(p.price) AS total")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        if let Some(Value::Float(total)) = map.get("total") {
            assert!(
                (total - 59.97).abs() < 0.001,
                "Sum should be ~59.97, got {}",
                total
            );
        } else {
            panic!("Expected Float for total");
        }
    } else {
        panic!("Expected Map result");
    }
}

#[test]
fn test_gql_avg_mixed_numeric() {
    let mut storage = InMemoryGraph::new();

    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), Value::from("A"));
    props1.insert("value".to_string(), Value::Int(10));
    storage.add_vertex("Item", props1);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("B"));
    props2.insert("value".to_string(), Value::Float(20.0));
    storage.add_vertex("Item", props2);

    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), Value::from("C"));
    props3.insert("value".to_string(), Value::Int(30));
    storage.add_vertex("Item", props3);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (i:Item) RETURN avg(i.value) AS average")
        .unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        if let Some(Value::Float(avg)) = map.get("average") {
            assert!(
                (avg - 20.0).abs() < 0.001,
                "Average should be 20.0, got {}",
                avg
            );
        } else {
            panic!("Expected Float for average");
        }
    } else {
        panic!("Expected Map result");
    }
}

#[test]
fn test_gql_order_by_aliased_property() {
    let graph = create_phase47_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name AS employee_name, p.salary AS pay ORDER BY p.salary DESC LIMIT 3")
        .unwrap();

    assert_eq!(results.len(), 3);

    let names: Vec<String> = results
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                match map.get("employee_name") {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect();

    assert_eq!(names[0], "Alice", "Highest paid should be Alice");
    assert_eq!(names[1], "Frank", "Second highest paid should be Frank");
    assert_eq!(names[2], "Carol", "Third highest paid should be Carol");
}

// =============================================================================
// Introspection Function Tests
// =============================================================================

fn create_introspection_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::from("Alice"));
    alice_props.insert("age".to_string(), Value::from(30i64));
    alice_props.insert("city".to_string(), Value::from("New York"));
    let alice = storage.add_vertex("Person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::from("Bob"));
    bob_props.insert("age".to_string(), Value::from(25i64));
    let bob = storage.add_vertex("Person", bob_props);

    let mut acme_props = HashMap::new();
    acme_props.insert("name".to_string(), Value::from("Acme Corp"));
    acme_props.insert("founded".to_string(), Value::from(1990i64));
    let acme = storage.add_vertex("Company", acme_props);

    let mut works_at_props = HashMap::new();
    works_at_props.insert("since".to_string(), Value::from(2020i64));
    works_at_props.insert("role".to_string(), Value::from("Engineer"));
    storage
        .add_edge(alice, acme, "works_at", works_at_props)
        .unwrap();

    let mut knows_props = HashMap::new();
    knows_props.insert("years".to_string(), Value::from(5i64));
    storage.add_edge(alice, bob, "knows", knows_props).unwrap();

    Graph::new(storage)
}

#[test]
fn test_gql_id_function_vertex() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name = 'Alice' RETURN id(p)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Int(_)));
}

#[test]
fn test_gql_id_function_edge() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person)-[e:knows]->(q:Person) RETURN id(e)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Int(_)));
}

#[test]
fn test_gql_id_function_with_alias() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN id(p) AS vertex_id, p.name")
        .unwrap();

    assert_eq!(results.len(), 2);

    for r in &results {
        if let Value::Map(map) = r {
            assert!(map.contains_key("vertex_id"));
            assert!(matches!(map.get("vertex_id"), Some(Value::Int(_))));
        } else {
            panic!("Expected Map result");
        }
    }
}

#[test]
fn test_gql_id_function_in_order_by() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) RETURN p.name, id(p) AS vid ORDER BY vid")
        .unwrap();

    assert_eq!(results.len(), 2);

    let mut prev_id = i64::MIN;
    for r in &results {
        if let Value::Map(map) = r {
            if let Some(Value::Int(id)) = map.get("vid") {
                assert!(*id > prev_id, "Results should be ordered by ID");
                prev_id = *id;
            }
        }
    }
}

#[test]
fn test_gql_labels_function_basic() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name = 'Alice' RETURN labels(p)")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::List(labels) = &results[0] {
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], Value::String("Person".to_string()));
    } else {
        panic!("Expected List result for labels()");
    }
}

#[test]
fn test_gql_labels_function_company() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (c:Company) RETURN labels(c) AS vertex_labels")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        if let Some(Value::List(labels)) = map.get("vertex_labels") {
            assert_eq!(labels.len(), 1);
            assert_eq!(labels[0], Value::String("Company".to_string()));
        } else {
            panic!("Expected List in vertex_labels");
        }
    }
}

#[test]
fn test_gql_labels_function_on_edge_returns_null() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person)-[e:knows]->(q:Person) RETURN labels(e) AS edge_labels")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("edge_labels"), Some(&Value::Null));
    }
}

#[test]
fn test_gql_type_function_basic() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person)-[e:knows]->(q:Person) RETURN type(e)")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("knows".to_string()));
}

#[test]
fn test_gql_type_function_different_edges() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)-[e]->(target)
            WHERE p.name = 'Alice'
            RETURN type(e) AS rel_type, target.name AS target_name
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 2);

    let types: HashSet<String> = results
        .iter()
        .filter_map(|r| {
            if let Value::Map(map) = r {
                if let Some(Value::String(t)) = map.get("rel_type") {
                    return Some(t.clone());
                }
            }
            None
        })
        .collect();

    assert!(types.contains("knows"));
    assert!(types.contains("works_at"));
}

#[test]
fn test_gql_type_function_with_alias() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH ()-[e:works_at]->() RETURN type(e) AS edge_type")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert_eq!(
            map.get("edge_type"),
            Some(&Value::String("works_at".to_string()))
        );
    }
}

#[test]
fn test_gql_type_function_on_vertex_returns_null() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name = 'Alice' RETURN type(p) AS vertex_type")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("vertex_type"), Some(&Value::Null));
    }
}

#[test]
fn test_gql_properties_function_vertex() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.name = 'Alice' RETURN properties(p)")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(props) = &results[0] {
        assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(props.get("age"), Some(&Value::Int(30)));
        assert_eq!(
            props.get("city"),
            Some(&Value::String("New York".to_string()))
        );
    } else {
        panic!("Expected Map result for properties()");
    }
}

#[test]
fn test_gql_properties_function_edge() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person)-[e:works_at]->(c:Company) RETURN properties(e)")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(props) = &results[0] {
        assert_eq!(props.get("since"), Some(&Value::Int(2020)));
        assert_eq!(
            props.get("role"),
            Some(&Value::String("Engineer".to_string()))
        );
    } else {
        panic!("Expected Map result for edge properties()");
    }
}

#[test]
fn test_gql_properties_function_with_alias() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (c:Company) RETURN properties(c) AS all_props, c.name")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(result_map) = &results[0] {
        if let Some(Value::Map(props)) = result_map.get("all_props") {
            assert_eq!(
                props.get("name"),
                Some(&Value::String("Acme Corp".to_string()))
            );
            assert_eq!(props.get("founded"), Some(&Value::Int(1990)));
        } else {
            panic!("Expected Map in all_props");
        }
    }
}

#[test]
fn test_gql_properties_function_empty_properties() {
    let mut storage = InMemoryGraph::new();

    storage.add_vertex("EmptyNode", HashMap::new());

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (n:EmptyNode) RETURN properties(n)")
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(props) = &results[0] {
        assert!(props.is_empty());
    } else {
        panic!("Expected Map result for properties()");
    }
}

#[test]
fn test_gql_introspection_combined() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            WHERE p.name = 'Alice'
            RETURN id(p) AS vertex_id, labels(p) AS vertex_labels, properties(p) AS props
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert!(matches!(map.get("vertex_id"), Some(Value::Int(_))));

        if let Some(Value::List(labels)) = map.get("vertex_labels") {
            assert_eq!(labels.len(), 1);
            assert_eq!(labels[0], Value::String("Person".to_string()));
        } else {
            panic!("Expected List for vertex_labels");
        }

        if let Some(Value::Map(props)) = map.get("props") {
            assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
        } else {
            panic!("Expected Map for props");
        }
    }
}

#[test]
fn test_gql_introspection_edge_combined() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)-[e:knows]->(q:Person)
            RETURN id(e) AS edge_id, type(e) AS edge_type, properties(e) AS edge_props
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert!(matches!(map.get("edge_id"), Some(Value::Int(_))));

        assert_eq!(
            map.get("edge_type"),
            Some(&Value::String("knows".to_string()))
        );

        if let Some(Value::Map(props)) = map.get("edge_props") {
            assert_eq!(props.get("years"), Some(&Value::Int(5)));
        } else {
            panic!("Expected Map for edge_props");
        }
    }
}

#[test]
fn test_gql_introspection_with_aggregation() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            RETURN count(*) AS person_count, collect(id(p)) AS person_ids
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 1);

    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("person_count"), Some(&Value::Int(2)));

        if let Some(Value::List(ids)) = map.get("person_ids") {
            assert_eq!(ids.len(), 2);
            for id in ids {
                assert!(matches!(id, Value::Int(_)));
            }
        } else {
            panic!("Expected List for person_ids");
        }
    }
}

#[test]
fn test_gql_introspection_with_where() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql(
            r#"
            MATCH (n:Person)
            WHERE id(n) >= 0
            RETURN n.name, id(n) AS vid
        "#,
        )
        .unwrap();

    assert_eq!(results.len(), 2);
}

// =============================================================================
// IS Predicate Equivalence Tests
// =============================================================================

#[test]
fn test_gql_is_predicate_equivalent_greater_than() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age > 25 RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn test_gql_is_predicate_equivalent_range() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age >= 20 AND p.age < 28 RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".to_string()));
}

#[test]
fn test_gql_is_predicate_equivalent_equality() {
    let graph = create_introspection_test_graph();
    let snapshot = graph.snapshot();

    let results = snapshot
        .gql("MATCH (p:Person) WHERE p.age = 30 RETURN p.name")
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

// =============================================================================
// Inline WHERE Tests
// =============================================================================

fn create_inline_where_test_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create people with various ages
    let alice = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Alice"));
        props.insert("age".to_string(), Value::Int(30));
        props.insert("active".to_string(), Value::Bool(true));
        props
    });

    let bob = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bob"));
        props.insert("age".to_string(), Value::Int(25));
        props.insert("active".to_string(), Value::Bool(true));
        props
    });

    let charlie = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Charlie"));
        props.insert("age".to_string(), Value::Int(17));
        props.insert("active".to_string(), Value::Bool(false));
        props
    });

    let dave = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Dave"));
        props.insert("age".to_string(), Value::Int(45));
        props.insert("active".to_string(), Value::Bool(true));
        props
    });

    // Create KNOWS relationships with 'since' property
    storage
        .add_edge(alice, bob, "KNOWS", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2020));
            props.insert("strength".to_string(), Value::Float(0.8));
            props
        })
        .unwrap();

    storage
        .add_edge(alice, charlie, "KNOWS", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2018));
            props.insert("strength".to_string(), Value::Float(0.3));
            props
        })
        .unwrap();

    storage
        .add_edge(bob, dave, "KNOWS", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2022));
            props.insert("strength".to_string(), Value::Float(0.9));
            props
        })
        .unwrap();

    storage
        .add_edge(charlie, dave, "KNOWS", {
            let mut props = HashMap::new();
            props.insert("since".to_string(), Value::Int(2015));
            props.insert("strength".to_string(), Value::Float(0.5));
            props
        })
        .unwrap();

    Graph::new(storage)
}

#[test]
fn test_gql_inline_where_node_simple() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Only adults (age > 21)
    let results = snapshot
        .gql("MATCH (n:Person WHERE n.age > 21) RETURN n.name")
        .unwrap();

    assert_eq!(results.len(), 3); // Alice (30), Bob (25), Dave (45)

    let names: HashSet<&str> = results
        .iter()
        .filter_map(|v| {
            if let Value::String(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
        .collect();
    assert!(names.contains("Alice"));
    assert!(names.contains("Bob"));
    assert!(names.contains("Dave"));
    assert!(!names.contains("Charlie")); // age 17, filtered out
}

#[test]
fn test_gql_inline_where_node_equality() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Only active people
    let results = snapshot
        .gql("MATCH (n:Person WHERE n.active = true) RETURN n.name")
        .unwrap();

    assert_eq!(results.len(), 3); // Alice, Bob, Dave - not Charlie

    let names: HashSet<&str> = results
        .iter()
        .filter_map(|v| {
            if let Value::String(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
        .collect();
    assert!(!names.contains("Charlie")); // active = false
}

#[test]
fn test_gql_inline_where_node_compound() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Adults who are active
    let results = snapshot
        .gql("MATCH (n:Person WHERE n.age >= 18 AND n.active = true) RETURN n.name")
        .unwrap();

    assert_eq!(results.len(), 3); // Alice, Bob, Dave

    let names: HashSet<&str> = results
        .iter()
        .filter_map(|v| {
            if let Value::String(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
        .collect();
    assert!(names.contains("Alice"));
    assert!(names.contains("Bob"));
    assert!(names.contains("Dave"));
}

#[test]
fn test_gql_inline_where_edge_simple() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Only relationships established since 2020
    let results = snapshot
        .gql("MATCH (a:Person)-[r:KNOWS WHERE r.since >= 2020]->(b:Person) RETURN a.name, b.name")
        .unwrap();

    // Alice->Bob (2020), Bob->Dave (2022)
    assert_eq!(results.len(), 2);
}

#[test]
fn test_gql_inline_where_edge_strength() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Only strong relationships (strength > 0.5)
    let results = snapshot
        .gql("MATCH (a:Person)-[r:KNOWS WHERE r.strength > 0.5]->(b:Person) RETURN a.name, b.name")
        .unwrap();

    // Alice->Bob (0.8), Bob->Dave (0.9)
    assert_eq!(results.len(), 2);
}

#[test]
fn test_gql_inline_where_combined_node_and_edge() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Active people who have strong recent relationships
    let results = snapshot
        .gql(
            r#"
            MATCH (a:Person WHERE a.active = true)-[r:KNOWS WHERE r.since >= 2020 AND r.strength > 0.5]->(b:Person)
            RETURN a.name, b.name
            "#,
        )
        .unwrap();

    // Alice->Bob (active, 2020, 0.8), Bob->Dave (active, 2022, 0.9)
    assert_eq!(results.len(), 2);
}

#[test]
fn test_gql_inline_where_with_global_where() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Inline WHERE on source node, global WHERE on target
    let results = snapshot
        .gql(
            r#"
            MATCH (a:Person WHERE a.age > 21)-[r:KNOWS]->(b:Person)
            WHERE b.active = true
            RETURN a.name, b.name
            "#,
        )
        .unwrap();

    // Filters: a.age > 21 AND b.active = true
    // Alice(30)->Bob(active), Alice(30)->Charlie(inactive), Bob(25)->Dave(active)
    // After filters: Alice->Bob, Bob->Dave
    assert_eq!(results.len(), 2);
}

#[test]
fn test_gql_inline_where_no_matches() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Impossible condition
    let results = snapshot
        .gql("MATCH (n:Person WHERE n.age > 100) RETURN n.name")
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_gql_inline_where_edge_no_matches() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Impossible edge condition
    let results = snapshot
        .gql("MATCH (a:Person)-[r:KNOWS WHERE r.since > 2030]->(b:Person) RETURN a.name")
        .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_gql_inline_where_equivalent_to_global() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Inline WHERE version
    let inline_results = snapshot
        .gql("MATCH (n:Person WHERE n.age > 21) RETURN n.name")
        .unwrap();

    // Global WHERE version
    let global_results = snapshot
        .gql("MATCH (n:Person) WHERE n.age > 21 RETURN n.name")
        .unwrap();

    // Should produce identical results
    assert_eq!(inline_results.len(), global_results.len());

    let inline_names: HashSet<_> = inline_results.iter().collect();
    let global_names: HashSet<_> = global_results.iter().collect();
    assert_eq!(inline_names, global_names);
}

#[test]
fn test_gql_inline_where_string_comparison() {
    let graph = create_inline_where_test_graph();
    let snapshot = graph.snapshot();

    // Filter by name starting with 'A' or 'B'
    let results = snapshot
        .gql("MATCH (n:Person WHERE n.name < 'C') RETURN n.name")
        .unwrap();

    // Alice, Bob
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Query Parameters Tests
// =============================================================================

#[test]
fn test_gql_parameter_in_where_clause() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let mut params = intersteller::gql::Parameters::new();
    params.insert("minAge".to_string(), Value::Int(30));

    let results = snapshot
        .gql_with_params(
            "MATCH (n:Person) WHERE n.age >= $minAge RETURN n.name",
            &params,
        )
        .unwrap();

    // Alice (30) and Charlie (35) should match
    assert_eq!(results.len(), 2);

    let names: HashSet<_> = results.iter().collect();
    assert!(names.contains(&Value::from("Alice")));
    assert!(names.contains(&Value::from("Charlie")));
}

#[test]
fn test_gql_parameter_equality() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let mut params = intersteller::gql::Parameters::new();
    params.insert("targetAge".to_string(), Value::Int(25));

    let results = snapshot
        .gql_with_params(
            "MATCH (n:Person) WHERE n.age = $targetAge RETURN n.name",
            &params,
        )
        .unwrap();

    // Only Bob is 25
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::from("Bob"));
}

#[test]
fn test_gql_parameter_in_list() {
    // Note: The IN clause currently requires a list literal [a, b, c]
    // rather than a parameter. This tests using parameters as list elements.
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let mut params = intersteller::gql::Parameters::new();
    params.insert("age1".to_string(), Value::Int(25));
    params.insert("age2".to_string(), Value::Int(35));

    let results = snapshot
        .gql_with_params(
            "MATCH (n:Person) WHERE n.age IN [$age1, $age2] RETURN n.name",
            &params,
        )
        .unwrap();

    // Bob (25) and Charlie (35) should match
    assert_eq!(results.len(), 2);

    let names: HashSet<_> = results.iter().collect();
    assert!(names.contains(&Value::from("Bob")));
    assert!(names.contains(&Value::from("Charlie")));
}

#[test]
fn test_gql_multiple_parameters() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let mut params = intersteller::gql::Parameters::new();
    params.insert("minAge".to_string(), Value::Int(25));
    params.insert("maxAge".to_string(), Value::Int(32));

    let results = snapshot
        .gql_with_params(
            "MATCH (n:Person) WHERE n.age >= $minAge AND n.age <= $maxAge RETURN n.name",
            &params,
        )
        .unwrap();

    // Bob (25) and Alice (30) should match
    assert_eq!(results.len(), 2);

    let names: HashSet<_> = results.iter().collect();
    assert!(names.contains(&Value::from("Alice")));
    assert!(names.contains(&Value::from("Bob")));
}

#[test]
fn test_gql_parameter_in_return_expression() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let mut params = intersteller::gql::Parameters::new();
    params.insert("multiplier".to_string(), Value::Int(2));

    let results = snapshot
        .gql_with_params(
            "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.age * $multiplier",
            &params,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    // Alice is 30, multiplied by 2 = 60
    assert_eq!(results[0], Value::Int(60));
}

#[test]
fn test_gql_unbound_parameter_error() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let params = intersteller::gql::Parameters::new(); // Empty params

    let result = snapshot.gql_with_params(
        "MATCH (n:Person) WHERE n.age >= $undefinedParam RETURN n.name",
        &params,
    );

    // Should error because $undefinedParam is not provided
    assert!(result.is_err());

    if let Err(GqlError::Compile(e)) = result {
        let error_msg = e.to_string();
        assert!(
            error_msg.contains("undefinedParam"),
            "Error should mention the unbound parameter name"
        );
    } else {
        panic!("Expected CompileError for unbound parameter");
    }
}

#[test]
fn test_gql_parameter_string_value() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let mut params = intersteller::gql::Parameters::new();
    params.insert("searchName".to_string(), Value::from("Alice"));

    let results = snapshot
        .gql_with_params(
            "MATCH (n:Person) WHERE n.name = $searchName RETURN n.age",
            &params,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(30));
}

#[test]
fn test_gql_parameter_with_null() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let mut params = intersteller::gql::Parameters::new();
    params.insert("value".to_string(), Value::Null);

    // Comparing with null should use IS NULL logic
    let results = snapshot
        .gql_with_params(
            "MATCH (n:Person) WHERE $value IS NULL RETURN n.name",
            &params,
        )
        .unwrap();

    // All persons should match since $value IS NULL is true
    assert_eq!(results.len(), 3);
}

#[test]
fn test_gql_parameter_reuse() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let mut params = intersteller::gql::Parameters::new();
    params.insert("threshold".to_string(), Value::Int(30));

    // Use same parameter twice
    let results = snapshot
        .gql_with_params(
            "MATCH (n:Person) WHERE n.age >= $threshold AND $threshold > 20 RETURN n.name",
            &params,
        )
        .unwrap();

    // Alice (30) and Charlie (35) should match
    assert_eq!(results.len(), 2);
}

#[test]
fn test_gql_empty_params_works_like_regular_query() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let params = intersteller::gql::Parameters::new();

    // Query without parameters should work with empty params
    let results_with_params = snapshot
        .gql_with_params("MATCH (n:Person) RETURN n.name", &params)
        .unwrap();

    let results_regular = snapshot.gql("MATCH (n:Person) RETURN n.name").unwrap();

    assert_eq!(results_with_params.len(), results_regular.len());

    let names_with_params: HashSet<_> = results_with_params.iter().collect();
    let names_regular: HashSet<_> = results_regular.iter().collect();
    assert_eq!(names_with_params, names_regular);
}

#[test]
fn test_gql_parameter_float_comparison() {
    let mut storage = InMemoryGraph::new();

    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::from("Item1"));
    props.insert("price".to_string(), Value::Float(19.99));
    storage.add_vertex("Product", props);

    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), Value::from("Item2"));
    props2.insert("price".to_string(), Value::Float(29.99));
    storage.add_vertex("Product", props2);

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    let mut params = intersteller::gql::Parameters::new();
    params.insert("maxPrice".to_string(), Value::Float(25.0));

    let results = snapshot
        .gql_with_params(
            "MATCH (p:Product) WHERE p.price < $maxPrice RETURN p.name",
            &params,
        )
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::from("Item1"));
}

// =============================================================================
// LET Clause Tests
// =============================================================================

#[test]
fn test_gql_let_simple_expression() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Simple LET binding a computed value
    let results = snapshot
        .gql("MATCH (n:Person) LET doubled = n.age * 2 RETURN n.name, doubled")
        .unwrap();

    assert_eq!(results.len(), 3);

    // Each result should be a map with name and doubled age
    for result in &results {
        if let Value::Map(map) = result {
            let name = map.get("n.name").unwrap();
            let doubled = map.get("doubled").unwrap();

            match name {
                Value::String(s) if s == "Alice" => {
                    assert_eq!(*doubled, Value::Int(60)); // 30 * 2
                }
                Value::String(s) if s == "Bob" => {
                    assert_eq!(*doubled, Value::Int(50)); // 25 * 2
                }
                Value::String(s) if s == "Charlie" => {
                    assert_eq!(*doubled, Value::Int(70)); // 35 * 2
                }
                _ => panic!("Unexpected name: {:?}", name),
            }
        } else {
            panic!("Expected map result");
        }
    }
}

#[test]
fn test_gql_let_count_aggregate() {
    let mut storage = InMemoryGraph::new();

    // Create people with friends
    let alice_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Alice"));
        props
    });

    let bob_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bob"));
        props
    });

    let charlie_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Charlie"));
        props
    });

    // Alice knows Bob and Charlie
    let _ = storage.add_edge(alice_id, bob_id, "KNOWS", HashMap::new());
    let _ = storage.add_edge(alice_id, charlie_id, "KNOWS", HashMap::new());

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    // Use LET to count friends
    let results = snapshot
        .gql("MATCH (p:Person)-[:KNOWS]->(f) LET friendCount = COUNT(f) RETURN p.name, friendCount")
        .unwrap();

    // Alice has 2 friends, so we should see 2 rows (one per edge), each with friendCount = 2
    assert!(!results.is_empty());

    // All rows should have the same friendCount (aggregate applied to all)
    for result in &results {
        if let Value::Map(map) = result {
            let count = map.get("friendCount").unwrap();
            assert_eq!(*count, Value::Int(2)); // Alice knows 2 people
        }
    }
}

#[test]
fn test_gql_let_collect_aggregate() {
    let mut storage = InMemoryGraph::new();

    let alice_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Alice"));
        props
    });

    let bob_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bob"));
        props
    });

    let charlie_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Charlie"));
        props
    });

    let _ = storage.add_edge(alice_id, bob_id, "KNOWS", HashMap::new());
    let _ = storage.add_edge(alice_id, charlie_id, "KNOWS", HashMap::new());

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    // Use LET to collect friend names
    let results = snapshot
        .gql("MATCH (p:Person)-[:KNOWS]->(f) LET friends = COLLECT(f.name) RETURN p.name, friends")
        .unwrap();

    // Each row should have the collected list of friend names
    for result in &results {
        if let Value::Map(map) = result {
            let friends = map.get("friends").unwrap();
            if let Value::List(list) = friends {
                // Should contain Bob and Charlie
                assert_eq!(list.len(), 2);
                let names: HashSet<_> = list.iter().collect();
                assert!(names.contains(&Value::from("Bob")));
                assert!(names.contains(&Value::from("Charlie")));
            } else {
                panic!("Expected list for friends");
            }
        }
    }
}

#[test]
fn test_gql_let_multiple_clauses() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Multiple LET clauses - later ones can reference earlier
    let results = snapshot
        .gql("MATCH (n:Person) LET doubled = n.age * 2 LET plusTen = doubled + 10 RETURN n.name, plusTen")
        .unwrap();

    assert_eq!(results.len(), 3);

    for result in &results {
        if let Value::Map(map) = result {
            let name = map.get("n.name").unwrap();
            let plus_ten = map.get("plusTen").unwrap();

            match name {
                Value::String(s) if s == "Alice" => {
                    assert_eq!(*plus_ten, Value::Int(70)); // (30 * 2) + 10
                }
                Value::String(s) if s == "Bob" => {
                    assert_eq!(*plus_ten, Value::Int(60)); // (25 * 2) + 10
                }
                Value::String(s) if s == "Charlie" => {
                    assert_eq!(*plus_ten, Value::Int(80)); // (35 * 2) + 10
                }
                _ => panic!("Unexpected name"),
            }
        }
    }
}

#[test]
fn test_gql_let_sum_aggregate() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Use LET with SUM aggregate
    let results = snapshot
        .gql("MATCH (n:Person) LET totalAge = SUM(n.age) RETURN n.name, totalAge")
        .unwrap();

    // Total age should be 30 + 25 + 35 = 90
    assert_eq!(results.len(), 3);

    for result in &results {
        if let Value::Map(map) = result {
            let total = map.get("totalAge").unwrap();
            assert_eq!(*total, Value::Int(90));
        }
    }
}

#[test]
fn test_gql_let_avg_aggregate() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Use LET with AVG aggregate
    let results = snapshot
        .gql("MATCH (n:Person) LET avgAge = AVG(n.age) RETURN n.name, avgAge")
        .unwrap();

    // Average age should be (30 + 25 + 35) / 3 = 30.0
    assert_eq!(results.len(), 3);

    for result in &results {
        if let Value::Map(map) = result {
            let avg = map.get("avgAge").unwrap();
            if let Value::Float(f) = avg {
                assert!((f - 30.0).abs() < 0.001);
            } else {
                panic!("Expected float for avgAge");
            }
        }
    }
}

#[test]
fn test_gql_let_with_where() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // LET should be evaluated after WHERE
    let results = snapshot
        .gql("MATCH (n:Person) WHERE n.age > 25 LET doubled = n.age * 2 RETURN n.name, doubled")
        .unwrap();

    // Only Alice (30) and Charlie (35) match WHERE
    assert_eq!(results.len(), 2);

    let names: HashSet<String> = results
        .iter()
        .filter_map(|r| {
            if let Value::Map(map) = r {
                if let Some(Value::String(name)) = map.get("n.name") {
                    return Some(name.clone());
                }
            }
            None
        })
        .collect();

    assert!(names.contains("Alice"));
    assert!(names.contains("Charlie"));
    assert!(!names.contains("Bob"));
}

#[test]
fn test_gql_let_size_of_collect() {
    let mut storage = InMemoryGraph::new();

    let alice_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Alice"));
        props
    });

    let bob_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Bob"));
        props
    });

    let charlie_id = storage.add_vertex("Person", {
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::from("Charlie"));
        props
    });

    let _ = storage.add_edge(alice_id, bob_id, "KNOWS", HashMap::new());
    let _ = storage.add_edge(alice_id, charlie_id, "KNOWS", HashMap::new());

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    // Chain LET clauses: collect then size
    let results = snapshot
        .gql("MATCH (p:Person)-[:KNOWS]->(f) LET friends = COLLECT(f.name) LET numFriends = SIZE(friends) RETURN p.name, numFriends")
        .unwrap();

    for result in &results {
        if let Value::Map(map) = result {
            let num = map.get("numFriends").unwrap();
            assert_eq!(*num, Value::Int(2)); // Alice knows 2 people
        }
    }
}

#[test]
fn test_gql_let_parse_only() {
    use intersteller::gql::parse;

    // Test that LET clause is properly parsed
    let query = parse("MATCH (n:Person) LET x = n.age RETURN x").unwrap();

    assert_eq!(query.let_clauses.len(), 1);
    assert_eq!(query.let_clauses[0].variable, "x");
}

#[test]
fn test_gql_let_parse_multiple() {
    use intersteller::gql::parse;

    // Test parsing multiple LET clauses
    let query = parse("MATCH (n:Person) LET x = n.age LET y = x * 2 RETURN y").unwrap();

    assert_eq!(query.let_clauses.len(), 2);
    assert_eq!(query.let_clauses[0].variable, "x");
    assert_eq!(query.let_clauses[1].variable, "y");
}
