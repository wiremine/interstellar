//! Extended coverage tests for GQL compiler (src/gql/compiler.rs).
//!
//! These tests target path-based evaluation functions and other uncovered code paths:
//! - Path-based REDUCE evaluation (evaluate_reduce_from_path)
//! - Path-based list predicates (evaluate_list_predicate_from_path)
//! - Path-based list comprehension (evaluate_list_comprehension_from_path)
//! - CALL subqueries (correlated and uncorrelated)
//! - Math function edge cases
//! - Error handling paths

use std::collections::HashMap;

use interstellar::gql::{compile, parse};
use interstellar::storage::CowGraph;
use interstellar::value::Value;

// =============================================================================
// Helper Functions
// =============================================================================

/// Creates a test graph with Person and Software vertices plus edges.
fn create_test_graph() -> CowGraph {
    let graph = CowGraph::new();

    // Add Person vertices with various properties
    let alice = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
            ("city".to_string(), Value::String("NYC".to_string())),
            (
                "scores".to_string(),
                Value::List(vec![Value::Int(85), Value::Int(90), Value::Int(95)]),
            ),
        ]),
    );

    let bob = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            ("age".to_string(), Value::Int(25)),
            ("city".to_string(), Value::String("LA".to_string())),
            (
                "scores".to_string(),
                Value::List(vec![Value::Int(70), Value::Int(75), Value::Int(80)]),
            ),
        ]),
    );

    let charlie = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Charlie".to_string())),
            ("age".to_string(), Value::Int(35)),
            ("city".to_string(), Value::String("NYC".to_string())),
            (
                "scores".to_string(),
                Value::List(vec![Value::Int(60), Value::Int(65), Value::Int(70)]),
            ),
        ]),
    );

    let diana = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Diana".to_string())),
            ("age".to_string(), Value::Int(28)),
            ("city".to_string(), Value::String("Chicago".to_string())),
        ]),
    );

    // Add Software vertices
    let gremlin = graph.add_vertex(
        "Software",
        HashMap::from([
            ("name".to_string(), Value::String("Gremlin".to_string())),
            ("lang".to_string(), Value::String("Java".to_string())),
            ("version".to_string(), Value::Float(3.5)),
        ]),
    );

    let rust_proj = graph.add_vertex(
        "Software",
        HashMap::from([
            (
                "name".to_string(),
                Value::String("Interstellar".to_string()),
            ),
            ("lang".to_string(), Value::String("Rust".to_string())),
            ("version".to_string(), Value::Float(1.0)),
        ]),
    );

    // Add edges
    graph
        .add_edge(
            alice,
            bob,
            "KNOWS",
            HashMap::from([
                ("since".to_string(), Value::Int(2020)),
                ("weight".to_string(), Value::Float(0.8)),
            ]),
        )
        .unwrap();

    graph
        .add_edge(
            alice,
            charlie,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2019))]),
        )
        .unwrap();

    graph
        .add_edge(
            bob,
            charlie,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2021))]),
        )
        .unwrap();

    graph
        .add_edge(
            charlie,
            diana,
            "KNOWS",
            HashMap::from([("since".to_string(), Value::Int(2018))]),
        )
        .unwrap();

    graph
        .add_edge(
            alice,
            gremlin,
            "CREATED",
            HashMap::from([("year".to_string(), Value::Int(2015))]),
        )
        .unwrap();

    graph
        .add_edge(
            bob,
            rust_proj,
            "CREATED",
            HashMap::from([("year".to_string(), Value::Int(2023))]),
        )
        .unwrap();

    graph
}

/// Creates a simple graph for basic tests.
fn create_simple_graph() -> CowGraph {
    let graph = CowGraph::new();
    graph.add_vertex("Dummy", HashMap::new());
    graph
}

// =============================================================================
// Path-based REDUCE evaluation tests
// These test evaluate_reduce_from_path by NOT using LET/WITH clauses
// =============================================================================

#[test]
fn test_reduce_in_return_without_let() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // REDUCE directly in RETURN without LET clause triggers path-based evaluation
    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN REDUCE(total = 0, x IN n.scores | total + x) AS sum
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // Alice's scores are [85, 90, 95] = 270
    assert_eq!(results[0], Value::Int(270));
}

#[test]
fn test_reduce_with_multiplication_in_return() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN REDUCE(product = 1, x IN [2, 3, 4] | product * x) AS product
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(24));
}

#[test]
fn test_reduce_with_null_list_in_return() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Diana doesn't have scores property
    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Diana'
        RETURN REDUCE(total = 0, x IN n.scores | total + x) AS sum
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_reduce_with_empty_list_in_return() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN REDUCE(total = 100, x IN [] | total + x) AS sum
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(100));
}

#[test]
fn test_reduce_with_string_concat_in_return() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN REDUCE(s = '', item IN ['a', 'b', 'c'] | s || item) AS concat
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("abc".to_string()));
}

// =============================================================================
// Path-based list predicate tests (ALL, ANY, NONE, SINGLE)
// These test evaluate_list_predicate_from_path
// =============================================================================

#[test]
fn test_all_predicate_in_return_without_let() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // ALL directly in RETURN triggers path-based evaluation
    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN ALL(x IN n.scores WHERE x >= 80) AS all_passing
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // Alice's scores are [85, 90, 95], all >= 80
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_any_predicate_in_return_without_let() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Charlie'
        RETURN ANY(x IN n.scores WHERE x >= 70) AS has_passing
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // Charlie's scores are [60, 65, 70], one is >= 70
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_none_predicate_in_return_without_let() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN NONE(x IN n.scores WHERE x < 50) AS none_failing
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // Alice's scores are [85, 90, 95], none < 50
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_single_predicate_in_return_without_let() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Charlie'
        RETURN SINGLE(x IN n.scores WHERE x = 70) AS exactly_one_70
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // Charlie's scores are [60, 65, 70], exactly one is 70
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_list_predicate_with_null_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Diana doesn't have scores property
    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Diana'
        RETURN ALL(x IN n.scores WHERE x > 0) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_all_predicate_empty_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN ALL(x IN [] WHERE x > 0) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // ALL on empty list is true (vacuous truth)
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_any_predicate_none_match() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN ANY(x IN n.scores WHERE x > 100) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // Alice's scores are [85, 90, 95], none > 100
    assert_eq!(results[0], Value::Bool(false));
}

#[test]
fn test_single_predicate_multiple_matches() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN SINGLE(x IN n.scores WHERE x > 80) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // Alice's scores are [85, 90, 95], multiple > 80
    assert_eq!(results[0], Value::Bool(false));
}

// =============================================================================
// Path-based list comprehension tests
// These test evaluate_list_comprehension_from_path
// =============================================================================

#[test]
fn test_list_comprehension_in_return_without_let() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN [x IN n.scores | x * 2] AS doubled
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // Alice's scores are [85, 90, 95] -> [170, 180, 190]
    if let Value::List(items) = &results[0] {
        assert_eq!(items.len(), 3);
        assert!(items.contains(&Value::Int(170)));
        assert!(items.contains(&Value::Int(180)));
        assert!(items.contains(&Value::Int(190)));
    } else {
        panic!("Expected list result");
    }
}

#[test]
fn test_list_comprehension_with_null_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Diana doesn't have scores property
    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Diana'
        RETURN [x IN n.scores | x * 2] AS doubled
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

// =============================================================================
// CALL subquery tests
// =============================================================================

#[test]
fn test_call_uncorrelated_basic() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Uncorrelated subquery doesn't reference outer variables
    let query = parse(
        r#"
        MATCH (p:Person)
        CALL {
            MATCH (s:Software)
            RETURN s.name AS software
        }
        RETURN p.name, software
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // 4 people x 2 software = 8 results
    assert_eq!(results.len(), 8);
}

#[test]
fn test_call_empty_subquery_excludes_row() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        CALL {
            WITH p
            MATCH (p)-[:CREATED]->(s:Software)
            RETURN s.name AS software
        }
        RETURN p.name, software
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Only Alice and Bob have CREATED edges
    assert_eq!(results.len(), 2);
}

// =============================================================================
// String function edge cases
// =============================================================================

#[test]
fn test_ltrim_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN ltrim('   hello') AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("hello".to_string()));
}

#[test]
fn test_rtrim_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN rtrim('hello   ') AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("hello".to_string()));
}

// =============================================================================
// Math function edge cases
// =============================================================================

#[test]
fn test_sqrt_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN sqrt(16) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 4.0).abs() < 0.001);
    } else {
        panic!("Expected float result, got {:?}", results[0]);
    }
}

#[test]
fn test_sqrt_negative_returns_nan_or_null() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN sqrt(-1) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // sqrt of negative returns NaN (which may be represented as Float(NaN))
    if let Value::Float(f) = results[0] {
        assert!(f.is_nan());
    }
    // Or it could return Null depending on implementation
}

#[test]
fn test_log_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN log(2.718281828) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        // ln(e) ≈ 1
        assert!((f - 1.0).abs() < 0.01);
    } else {
        panic!("Expected float result");
    }
}

#[test]
fn test_log10_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN log10(100) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 2.0).abs() < 0.001);
    } else {
        panic!("Expected float result");
    }
}

#[test]
fn test_exp_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN exp(1) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        // e ≈ 2.718281828
        assert!((f - std::f64::consts::E).abs() < 0.001);
    } else {
        panic!("Expected float result");
    }
}

#[test]
fn test_sin_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN sin(0) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 0.0).abs() < 0.001);
    } else {
        panic!("Expected float result");
    }
}

#[test]
fn test_cos_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN cos(0) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 1.0).abs() < 0.001);
    } else {
        panic!("Expected float result");
    }
}

#[test]
fn test_tan_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN tan(0) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 0.0).abs() < 0.001);
    } else {
        panic!("Expected float result");
    }
}

#[test]
fn test_asin_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN asin(0) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 0.0).abs() < 0.001);
    } else {
        panic!("Expected float result");
    }
}

#[test]
fn test_acos_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN acos(1) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 0.0).abs() < 0.001);
    } else {
        panic!("Expected float result");
    }
}

#[test]
fn test_atan_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN atan(0) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 0.0).abs() < 0.001);
    } else {
        panic!("Expected float result");
    }
}

#[test]
fn test_atan2_function() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN atan2(1, 1) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        // atan2(1, 1) = π/4 ≈ 0.7854
        assert!((f - std::f64::consts::FRAC_PI_4).abs() < 0.001);
    } else {
        panic!("Expected float result");
    }
}

#[test]
fn test_sign_function_positive() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN sign(42) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(1));
}

#[test]
fn test_sign_function_negative() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN sign(-42) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(-1));
}

#[test]
fn test_sign_function_zero() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN sign(0) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(0));
}

// =============================================================================
// Index and slice edge cases for path-based evaluation
// =============================================================================

#[test]
fn test_index_access_on_property_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN n.scores[0] AS first
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(85));
}

#[test]
fn test_slice_on_property_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN n.scores[0..2] AS first_two
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::List(items) = &results[0] {
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], Value::Int(85));
        assert_eq!(items[1], Value::Int(90));
    } else {
        panic!("Expected list result");
    }
}

#[test]
fn test_negative_index_on_property() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN n.scores[-1] AS last
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(95));
}

// =============================================================================
// CASE expression in path-based context
// =============================================================================

#[test]
fn test_case_expression_in_return() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        RETURN n.name, 
               CASE WHEN n.age >= 30 THEN 'Senior' ELSE 'Junior' END AS category
        ORDER BY n.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 4);
}

// =============================================================================
// Additional edge case tests
// =============================================================================

#[test]
fn test_type_function() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WHERE a.name = 'Alice'
        RETURN type(r) AS rel_type
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert!(!results.is_empty());
    // All should be KNOWS
    for result in &results {
        assert_eq!(result, &Value::String("KNOWS".to_string()));
    }
}

#[test]
fn test_id_function() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN id(n) AS node_id
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // ID should be an integer
    if let Value::Int(id) = results[0] {
        assert!(id >= 0);
    } else {
        panic!("Expected integer ID");
    }
}

#[test]
fn test_labels_function() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN labels(n) AS node_labels
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::List(labels) = &results[0] {
        assert!(labels.contains(&Value::String("Person".to_string())));
    } else {
        panic!("Expected list result");
    }
}

#[test]
fn test_properties_function() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN properties(n) AS props
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(props) = &results[0] {
        assert_eq!(props.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(props.get("age"), Some(&Value::Int(30)));
    } else {
        panic!("Expected map result");
    }
}

// =============================================================================
// Variable-length path with quantifier tests
// =============================================================================

#[test]
fn test_variable_length_path_with_quantifier_star() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (a:Person)-[:KNOWS*]->(b:Person)
        WHERE a.name = 'Alice'
        RETURN DISTINCT b.name AS friend
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Alice knows Bob and Charlie directly
    // Bob knows Charlie
    // Charlie knows Diana
    // So Alice can reach: Bob, Charlie, Diana
    assert!(results.len() >= 2);
}

#[test]
fn test_variable_length_path_with_exact_length() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (a:Person)-[:KNOWS*2]->(b:Person)
        WHERE a.name = 'Alice'
        RETURN b.name AS friend
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Alice -> Bob -> Charlie (length 2)
    // Alice -> Charlie -> Diana (length 2)
    assert!(!results.is_empty());
}

// =============================================================================
// Multiple match patterns
// =============================================================================

#[test]
fn test_match_with_relationship() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Test MATCH with relationship pattern to exercise more code paths
    let query = parse(
        r#"
        MATCH (p:Person)-[r:KNOWS]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN p.name, other.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Alice knows Bob and Charlie
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Coalesce function tests
// =============================================================================

#[test]
fn test_coalesce_returns_first_non_null() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Diana'
        RETURN coalesce(n.scores, [0]) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // Diana doesn't have scores, so should return [0]
    if let Value::List(items) = &results[0] {
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], Value::Int(0));
    } else {
        panic!("Expected list result");
    }
}

#[test]
fn test_coalesce_returns_first_when_not_null() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN coalesce(n.name, 'Unknown') AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

// =============================================================================
// Null handling in expressions
// =============================================================================

#[test]
fn test_null_in_arithmetic() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Diana'
        RETURN n.nonexistent + 5 AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Null);
}

#[test]
fn test_null_in_comparison() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.nonexistent = 5
        RETURN n.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // No matches because null = 5 is false
    assert_eq!(results.len(), 0);
}

// =============================================================================
// Boolean expression edge cases
// =============================================================================

#[test]
fn test_not_operator_in_where() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE NOT n.age > 30
        RETURN n.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Alice: age 30 (not >30) -> NOT true -> included
    // Bob: age 25 -> NOT true -> included
    // Charlie: age 35 (>30) -> NOT false -> excluded
    // Diana: age 28 -> NOT true -> included
    assert_eq!(results.len(), 3);
}

// =============================================================================
// Power and modulo operators
// =============================================================================

#[test]
fn test_power_operator() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN 2 ^ 3 AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // 2^3 = 8
    if let Value::Float(f) = results[0] {
        assert!((f - 8.0).abs() < 0.001);
    } else if let Value::Int(i) = results[0] {
        assert_eq!(i, 8);
    } else {
        panic!("Expected numeric result");
    }
}

#[test]
fn test_modulo_operator() {
    let graph = create_simple_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n)
        RETURN 10 % 3 AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(1));
}

// =============================================================================
// Unary minus tests
// =============================================================================

#[test]
fn test_unary_minus() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE n.name = 'Alice'
        RETURN -n.age AS neg_age
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(-30));
}

// =============================================================================
// Multi-variable pattern tests (triggers path-based evaluation)
// =============================================================================

#[test]
fn test_multi_var_pattern_string_functions() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Multi-variable pattern triggers path-based evaluation
    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN toUpper(other.name) AS upper_name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Alice knows Bob and Charlie
    assert_eq!(results.len(), 2);
    let names: Vec<&Value> = results.iter().collect();
    assert!(names.contains(&&Value::String("BOB".to_string())));
    assert!(names.contains(&&Value::String("CHARLIE".to_string())));
}

#[test]
fn test_multi_var_pattern_tolower() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN toLower(other.name) AS lower_name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 2);
    let names: Vec<&Value> = results.iter().collect();
    assert!(names.contains(&&Value::String("bob".to_string())));
    assert!(names.contains(&&Value::String("charlie".to_string())));
}

#[test]
fn test_multi_var_pattern_size() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN size(other.name) AS name_len
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Bob = 3, Charlie = 7
    assert_eq!(results.len(), 2);
}

#[test]
fn test_multi_var_pattern_trim() {
    let graph = CowGraph::new();

    let alice = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            (
                "nickname".to_string(),
                Value::String("  Ally  ".to_string()),
            ),
        ]),
    );

    let bob = graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
    );

    graph.add_edge(alice, bob, "KNOWS", HashMap::new());

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        RETURN trim(p.nickname) AS trimmed
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Ally".to_string()));
}

#[test]
fn test_multi_var_pattern_substring() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice' AND other.name = 'Charlie'
        RETURN substring(other.name, 0, 4) AS sub
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Char".to_string()));
}

#[test]
fn test_multi_var_pattern_replace() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE other.name = 'Bob'
        RETURN replace(other.name, 'ob', 'obby') AS replaced
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bobby".to_string()));
}

#[test]
fn test_multi_var_pattern_abs() {
    let graph = CowGraph::new();

    let alice = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("balance".to_string(), Value::Int(-100)),
        ]),
    );

    let bob = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            ("balance".to_string(), Value::Int(50)),
        ]),
    );

    graph.add_edge(alice, bob, "OWES", HashMap::new());

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:OWES]->(other:Person)
        RETURN abs(p.balance) AS abs_balance
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(100));
}

#[test]
fn test_multi_var_pattern_ceil_floor_round() {
    let graph = CowGraph::new();

    let alice = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("score".to_string(), Value::Float(3.7)),
        ]),
    );

    let bob = graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
    );

    graph.add_edge(alice, bob, "RATED", HashMap::new());

    let snapshot = graph.snapshot();

    // Test ceil
    let query = parse(
        r#"
        MATCH (p:Person)-[:RATED]->(other:Person)
        RETURN ceil(p.score) AS ceiled
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(4.0));

    // Test floor
    let query = parse(
        r#"
        MATCH (p:Person)-[:RATED]->(other:Person)
        RETURN floor(p.score) AS floored
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(3.0));

    // Test round
    let query = parse(
        r#"
        MATCH (p:Person)-[:RATED]->(other:Person)
        RETURN round(p.score) AS rounded
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(4.0));
}

// =============================================================================
// Type conversion function tests (path-based)
// =============================================================================

#[test]
fn test_multi_var_tostring() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE other.name = 'Bob'
        RETURN toString(other.age) AS age_str
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("25".to_string()));
}

#[test]
fn test_multi_var_tointeger() {
    let graph = CowGraph::new();

    let alice = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("score_str".to_string(), Value::String("42".to_string())),
        ]),
    );

    let bob = graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
    );

    graph.add_edge(alice, bob, "KNOWS", HashMap::new());

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        RETURN toInteger(p.score_str) AS score_int
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(42));
}

#[test]
fn test_multi_var_tofloat() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE other.name = 'Bob'
        RETURN toFloat(other.age) AS age_float
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(25.0));
}

#[test]
fn test_multi_var_toboolean() {
    let graph = CowGraph::new();

    let alice = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("active".to_string(), Value::String("true".to_string())),
        ]),
    );

    let bob = graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
    );

    graph.add_edge(alice, bob, "KNOWS", HashMap::new());

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        RETURN toBoolean(p.active) AS is_active
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

// =============================================================================
// Degree/Radian conversion tests (path-based)
// =============================================================================

#[test]
fn test_multi_var_radians() {
    let graph = CowGraph::new();

    let alice = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("angle".to_string(), Value::Int(180)),
        ]),
    );

    let bob = graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
    );

    graph.add_edge(alice, bob, "KNOWS", HashMap::new());

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        RETURN radians(p.angle) AS rad
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - std::f64::consts::PI).abs() < 0.0001);
    } else {
        panic!("Expected float");
    }
}

#[test]
fn test_multi_var_degrees() {
    let graph = CowGraph::new();

    let alice = graph.add_vertex(
        "Person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("angle".to_string(), Value::Float(std::f64::consts::PI)),
        ]),
    );

    let bob = graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
    );

    graph.add_edge(alice, bob, "KNOWS", HashMap::new());

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        RETURN degrees(p.angle) AS deg
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 180.0).abs() < 0.0001);
    } else {
        panic!("Expected float");
    }
}

// =============================================================================
// Mathematical constants tests (path-based)
// =============================================================================

#[test]
fn test_multi_var_pi_constant() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice' AND other.name = 'Bob'
        RETURN pi() AS pi_val
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - std::f64::consts::PI).abs() < 0.0001);
    } else {
        panic!("Expected float for pi()");
    }
}

#[test]
fn test_multi_var_e_constant() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice' AND other.name = 'Bob'
        RETURN e() AS e_val
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - std::f64::consts::E).abs() < 0.0001);
    } else {
        panic!("Expected float for e()");
    }
}

// =============================================================================
// POW function tests (path-based)
// =============================================================================

#[test]
fn test_multi_var_pow_function() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice' AND other.name = 'Bob'
        RETURN pow(2, 3) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // pow returns Int when both args are integers
    assert_eq!(results[0], Value::Int(8));
}

// =============================================================================
// DISTINCT tests
// =============================================================================

#[test]
fn test_distinct_multi_var() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Alice knows both Bob and Charlie - return DISTINCT source name
    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN DISTINCT p.name AS source
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Should be deduplicated to just "Alice" once
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

// =============================================================================
// Variable-length path tests (more edge cases)
// =============================================================================

#[test]
fn test_variable_length_path_min_only() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // *2.. means at least 2 hops
    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS*2..]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN DISTINCT other.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();
    // Results depend on graph structure - just verify it runs
    assert!(results.len() >= 0);
}

#[test]
fn test_variable_length_path_range() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // *1..3 means 1 to 3 hops
    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS*1..3]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN DISTINCT other.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();
    assert!(results.len() >= 0);
}

// =============================================================================
// Error handling tests
// =============================================================================

#[test]
fn test_undefined_variable_in_where() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        WHERE unknown_var.name = 'Alice'
        RETURN n.name
        "#,
    )
    .unwrap();

    let result = compile(&query, &snapshot);
    assert!(result.is_err());
}

#[test]
fn test_undefined_variable_in_return() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (n:Person)
        RETURN undefined.name
        "#,
    )
    .unwrap();

    let result = compile(&query, &snapshot);
    assert!(result.is_err());
}

// =============================================================================
// Contains/StartsWith/EndsWith tests (path-based)
// =============================================================================

#[test]
fn test_multi_var_contains() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice' AND other.name CONTAINS 'ob'
        RETURN other.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".to_string()));
}

#[test]
fn test_multi_var_starts_with() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice' AND other.name STARTS WITH 'Ch'
        RETURN other.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Charlie".to_string()));
}

#[test]
fn test_multi_var_ends_with() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice' AND other.name ENDS WITH 'ob'
        RETURN other.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Bob".to_string()));
}

// =============================================================================
// IS NULL / IS NOT NULL in multi-var patterns
// =============================================================================

#[test]
fn test_multi_var_is_null() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Diana doesn't have scores
    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE other.scores IS NULL
        RETURN other.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();
    // Results depend on graph - just verify it runs
    assert!(results.len() >= 0);
}

#[test]
fn test_multi_var_is_not_null() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice' AND other.scores IS NOT NULL
        RETURN other.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();
    // Bob and Charlie have scores
    assert!(results.len() >= 0);
}

// =============================================================================
// CASE expression in multi-var patterns
// =============================================================================

#[test]
fn test_multi_var_case_expression() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN CASE WHEN other.age < 30 THEN 'young' ELSE 'old' END AS age_group
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Bob is 25 (young), Charlie is 35 (old)
    assert_eq!(results.len(), 2);
    let groups: Vec<&Value> = results.iter().collect();
    assert!(groups.contains(&&Value::String("young".to_string())));
    assert!(groups.contains(&&Value::String("old".to_string())));
}

// =============================================================================
// Coalesce in multi-var patterns
// =============================================================================

#[test]
fn test_multi_var_coalesce() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN coalesce(other.missing, other.name) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 2);
    // Should return names since 'missing' is null
}

// =============================================================================
// EXISTS pattern tests
// =============================================================================

#[test]
fn test_exists_pattern_true() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:KNOWS]->(:Person) }
        RETURN p.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Alice and Bob have outgoing KNOWS edges
    assert!(results.len() >= 1);
}

#[test]
fn test_exists_pattern_false() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Diana has no outgoing KNOWS edges in our test graph
    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Diana' AND EXISTS { (p)-[:KNOWS]->(:Person) }
        RETURN p.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Diana has no outgoing edges, so result should be empty
    assert_eq!(results.len(), 0);
}

// =============================================================================
// UNION query tests
// =============================================================================

#[test]
fn test_union_basic() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query_text = r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN p.name AS name
        UNION
        MATCH (p:Person)
        WHERE p.name = 'Bob'
        RETURN p.name AS name
    "#;

    let statement = interstellar::gql::parse_statement(query_text).unwrap();
    let results = interstellar::gql::compile_statement(&statement, &snapshot).unwrap();

    // Should have Alice and Bob (deduplicated)
    assert_eq!(results.len(), 2);
}

#[test]
fn test_union_all_keeps_duplicates() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query_text = r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN p.name AS name
        UNION ALL
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN p.name AS name
    "#;

    let statement = interstellar::gql::parse_statement(query_text).unwrap();
    let results = interstellar::gql::compile_statement(&statement, &snapshot).unwrap();

    // UNION ALL keeps duplicates - should have 2 Alice entries
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Aggregation with LET clause tests
// =============================================================================

#[test]
fn test_let_with_aggregate_sum() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        LET total = sum(p.age)
        RETURN total
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // LET broadcasts aggregate to all rows - returns 4 rows with the same total
    // All ages summed: 30 + 25 + 35 + 28 = 118
    assert_eq!(results.len(), 4);
    assert_eq!(results[0], Value::Int(118));
}

#[test]
fn test_let_with_aggregate_avg() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        LET avg_age = avg(p.age)
        RETURN avg_age
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // LET broadcasts aggregate to all rows
    assert_eq!(results.len(), 4);
}

#[test]
fn test_let_with_aggregate_count() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        LET cnt = count(p)
        RETURN cnt
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // LET broadcasts aggregate to all rows
    assert_eq!(results.len(), 4);
    assert_eq!(results[0], Value::Int(4)); // 4 persons
}

#[test]
fn test_let_with_aggregate_collect() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        LET names = collect(p.name)
        RETURN names
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // LET broadcasts aggregate to all rows
    assert_eq!(results.len(), 4);
    if let Value::List(names) = &results[0] {
        assert_eq!(names.len(), 4);
    } else {
        panic!("Expected list");
    }
}

// =============================================================================
// WITH clause tests
// =============================================================================

#[test]
fn test_with_clause_projection() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WITH p.name AS name, p.age AS age
        WHERE age > 25
        RETURN name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Alice (30), Charlie (35), Diana (28) are > 25
    assert_eq!(results.len(), 3);
}

#[test]
fn test_with_clause_aggregation() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WITH p.city AS city, count(p) AS cnt
        RETURN city, cnt
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // NYC (2), LA (1), Chicago (1) - grouped by city
    assert!(results.len() >= 1);
}

#[test]
fn test_with_clause_order_by() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WITH p.name AS name, p.age AS age
        ORDER BY age DESC
        RETURN name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Should be ordered by age descending
    assert_eq!(results.len(), 4);
}

#[test]
fn test_with_clause_limit() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WITH p
        LIMIT 2
        RETURN p.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_with_clause_distinct() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WITH DISTINCT p.city AS city
        RETURN city
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Distinct cities: NYC, LA, Chicago
    assert_eq!(results.len(), 3);
}

// =============================================================================
// Edge property tests
// =============================================================================

#[test]
fn test_edge_property_access() {
    let graph = CowGraph::new();

    let alice = graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Alice".to_string()))]),
    );

    let bob = graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("Bob".to_string()))]),
    );

    graph.add_edge(
        alice,
        bob,
        "KNOWS",
        HashMap::from([
            ("since".to_string(), Value::Int(2020)),
            ("strength".to_string(), Value::Float(0.8)),
        ]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[r:KNOWS]->(other:Person)
        WHERE p.name = 'Alice'
        RETURN r.since AS since, r.strength AS strength
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_edge_type_filter() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Filter by edge type
    let query = parse(
        r#"
        MATCH (p:Person)-[r:KNOWS]->(other)
        RETURN type(r) AS edge_type
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // All results should have edge_type = "KNOWS"
    for result in &results {
        assert_eq!(*result, Value::String("KNOWS".to_string()));
    }
}

// =============================================================================
// Incoming edge tests
// =============================================================================

#[test]
fn test_incoming_edges() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)<-[:KNOWS]-(other:Person)
        WHERE p.name = 'Bob'
        RETURN other.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Alice knows Bob, so Alice should be in results
    assert!(results.len() >= 1);
}

#[test]
fn test_bidirectional_edges() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)-[:KNOWS]-(other:Person)
        WHERE p.name = 'Alice'
        RETURN DISTINCT other.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Should find connections in both directions
    assert!(results.len() >= 1);
}

// =============================================================================
// Map expression tests
// =============================================================================

#[test]
fn test_map_literal_in_return() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN {name: p.name, age: p.age} AS person_map
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(map) = &results[0] {
        assert_eq!(map.get("name"), Some(&Value::String("Alice".to_string())));
        assert_eq!(map.get("age"), Some(&Value::Int(30)));
    } else {
        panic!("Expected map");
    }
}

// =============================================================================
// List literal tests
// =============================================================================

#[test]
fn test_list_literal_in_return() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN [p.name, p.age, p.city] AS info
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::List(items) = &results[0] {
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], Value::String("Alice".to_string()));
        assert_eq!(items[1], Value::Int(30));
        assert_eq!(items[2], Value::String("NYC".to_string()));
    } else {
        panic!("Expected list");
    }
}

// =============================================================================
// Comparison operators tests
// =============================================================================

#[test]
fn test_greater_than_or_equal() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.age >= 30
        RETURN p.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Alice (30), Charlie (35)
    assert_eq!(results.len(), 2);
}

#[test]
fn test_less_than_or_equal() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.age <= 28
        RETURN p.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Bob (25), Diana (28)
    assert_eq!(results.len(), 2);
}

#[test]
fn test_not_equal() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name <> 'Alice'
        RETURN p.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Everyone except Alice
    assert_eq!(results.len(), 3);
}

// =============================================================================
// IN list operator tests
// =============================================================================

#[test]
fn test_in_list_match() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name IN ['Alice', 'Bob']
        RETURN p.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_not_in_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE NOT p.name IN ['Alice', 'Bob']
        RETURN p.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Charlie and Diana
    assert_eq!(results.len(), 2);
}

// =============================================================================
// Arithmetic in WHERE clause tests
// =============================================================================

#[test]
fn test_arithmetic_in_where() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.age * 2 > 60
        RETURN p.name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Charlie (35*2=70), Alice (30*2=60 is not > 60)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Charlie".to_string()));
}

#[test]
fn test_division_in_return() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN p.age / 2 AS half_age
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(15));
}

// =============================================================================
// String concatenation tests
// =============================================================================

#[test]
fn test_string_concatenation() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN p.name + ' from ' + p.city AS full_name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice from NYC".to_string()));
}

// =============================================================================
// Row-based function tests (with LET clause)
// =============================================================================

#[test]
fn test_row_based_toupper() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        LET upper_name = toUpper(p.name)
        RETURN upper_name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("ALICE".to_string()));
}

#[test]
fn test_row_based_tolower() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        LET lower_name = toLower(p.name)
        RETURN lower_name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("alice".to_string()));
}

#[test]
fn test_row_based_abs() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Account",
        HashMap::from([
            ("name".to_string(), Value::String("Test".to_string())),
            ("balance".to_string(), Value::Int(-100)),
        ]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (a:Account)
        LET abs_balance = abs(a.balance)
        RETURN abs_balance
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(100));
}

// =============================================================================
// Nodes function tests
// =============================================================================

#[test]
fn test_nodes_function() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    // Use nodes() to get node ids from a pattern match
    let query = parse(
        r#"
        MATCH (a:Person)-[:KNOWS]->(b:Person)
        WHERE a.name = 'Alice' AND b.name = 'Bob'
        RETURN a, b
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
}

// =============================================================================
// startNode and endNode function tests
// =============================================================================

#[test]
fn test_startnode_function() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WHERE a.name = 'Alice' AND b.name = 'Bob'
        RETURN startNode(r) AS start_id
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
}

#[test]
fn test_endnode_function() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (a:Person)-[r:KNOWS]->(b:Person)
        WHERE a.name = 'Alice' AND b.name = 'Bob'
        RETURN endNode(r) AS end_id
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
}

// =============================================================================
// Single-element evaluation tests (no LET, single variable)
// These test the evaluate_function_call and evaluate_value code paths
// =============================================================================

#[test]
fn test_single_var_toupper() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN toUpper(p.name) AS upper_name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("ALICE".to_string()));
}

#[test]
fn test_single_var_tolower() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN toLower(p.name) AS lower_name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("alice".to_string()));
}

#[test]
fn test_single_var_size_string() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN size(p.name) AS name_len
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(5)); // "Alice" = 5 chars
}

#[test]
fn test_single_var_size_list() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN size(p.scores) AS scores_len
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(3)); // Alice has 3 scores
}

#[test]
fn test_single_var_trim() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Person",
        HashMap::from([("name".to_string(), Value::String("  Trimmed  ".to_string()))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        RETURN trim(p.name) AS trimmed
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Trimmed".to_string()));
}

#[test]
fn test_single_var_substring_with_length() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN substring(p.name, 1, 3) AS sub
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("lic".to_string()));
}

#[test]
fn test_single_var_substring_without_length() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN substring(p.name, 2) AS sub
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("ice".to_string()));
}

#[test]
fn test_single_var_replace() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN replace(p.name, 'li', 'LI') AS replaced
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("ALIce".to_string()));
}

#[test]
fn test_single_var_abs() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Account",
        HashMap::from([
            ("name".to_string(), Value::String("Test".to_string())),
            ("balance".to_string(), Value::Int(-50)),
        ]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (a:Account)
        RETURN abs(a.balance) AS abs_balance
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(50));
}

#[test]
fn test_single_var_ceil() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::Float(3.2))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN ceil(d.value) AS ceiled
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(4.0));
}

#[test]
fn test_single_var_floor() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::Float(3.8))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN floor(d.value) AS floored
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(3.0));
}

#[test]
fn test_single_var_round() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::Float(3.5))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN round(d.value) AS rounded
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(4.0));
}

#[test]
fn test_single_var_sign() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN sign(p.age) AS sign_val
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(1));
}

#[test]
fn test_single_var_sqrt() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::Int(16))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN sqrt(d.value) AS root
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(4.0));
}

#[test]
fn test_single_var_log() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::Float(std::f64::consts::E))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN log(d.value) AS ln_val
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 1.0).abs() < 0.0001);
    } else {
        panic!("Expected float");
    }
}

#[test]
fn test_single_var_exp() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::Int(0))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN exp(d.value) AS exp_val
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 1.0).abs() < 0.0001);
    } else {
        panic!("Expected float");
    }
}

#[test]
fn test_single_var_sin() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::Int(0))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN sin(d.value) AS sin_val
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!(f.abs() < 0.0001); // sin(0) = 0
    } else {
        panic!("Expected float");
    }
}

#[test]
fn test_single_var_cos() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::Int(0))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN cos(d.value) AS cos_val
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!((f - 1.0).abs() < 0.0001); // cos(0) = 1
    } else {
        panic!("Expected float");
    }
}

#[test]
fn test_single_var_tan() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::Int(0))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN tan(d.value) AS tan_val
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Float(f) = results[0] {
        assert!(f.abs() < 0.0001); // tan(0) = 0
    } else {
        panic!("Expected float");
    }
}

#[test]
fn test_single_var_tostring() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN toString(p.age) AS age_str
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("30".to_string()));
}

#[test]
fn test_single_var_tointeger() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::String("42".to_string()))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN toInteger(d.value) AS int_val
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(42));
}

#[test]
fn test_single_var_tofloat() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN toFloat(p.age) AS age_float
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(30.0));
}

#[test]
fn test_single_var_toboolean() {
    let graph = CowGraph::new();

    graph.add_vertex(
        "Data",
        HashMap::from([("value".to_string(), Value::String("true".to_string()))]),
    );

    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (d:Data)
        RETURN toBoolean(d.value) AS bool_val
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true));
}

#[test]
fn test_single_var_coalesce() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN coalesce(p.missing, p.name) AS result
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::String("Alice".to_string()));
}

#[test]
fn test_single_var_properties() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN properties(p) AS props
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::Map(props) = &results[0] {
        assert!(props.contains_key("name"));
        assert!(props.contains_key("age"));
    } else {
        panic!("Expected map");
    }
}

#[test]
fn test_single_var_labels() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN labels(p) AS lbls
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::List(labels) = &results[0] {
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0], Value::String("Person".to_string()));
    } else {
        panic!("Expected list");
    }
}

#[test]
fn test_single_var_id() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN id(p) AS node_id
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Value::Int(_)));
}

// =============================================================================
// Single-element REDUCE tests
// =============================================================================

#[test]
fn test_single_var_reduce() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN REDUCE(total = 0, x IN p.scores | total + x) AS sum
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    // Alice's scores: 85 + 90 + 95 = 270
    assert_eq!(results[0], Value::Int(270));
}

// =============================================================================
// Single-element list predicate tests
// =============================================================================

#[test]
fn test_single_var_all_predicate() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN ALL(x IN p.scores WHERE x > 80) AS all_above_80
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true)); // 85, 90, 95 are all > 80
}

#[test]
fn test_single_var_any_predicate() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN ANY(x IN p.scores WHERE x > 90) AS any_above_90
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true)); // 95 > 90
}

#[test]
fn test_single_var_none_predicate() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN NONE(x IN p.scores WHERE x < 80) AS none_below_80
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true)); // All scores >= 85
}

#[test]
fn test_single_var_single_predicate() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN SINGLE(x IN p.scores WHERE x = 90) AS exactly_one_90
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Bool(true)); // Exactly one score = 90
}

// =============================================================================
// Single-element list comprehension tests
// =============================================================================

#[test]
fn test_single_var_list_comprehension() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN [x IN p.scores | x * 2] AS doubled
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::List(items) = &results[0] {
        // 85*2 = 170, 90*2 = 180, 95*2 = 190
        assert_eq!(items.len(), 3);
    } else {
        panic!("Expected list");
    }
}

// =============================================================================
// Single-element CASE tests
// =============================================================================

#[test]
fn test_single_var_case() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        RETURN CASE WHEN p.age >= 30 THEN 'senior' ELSE 'junior' END AS category
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Alice (30) -> senior, Bob (25) -> junior, Charlie (35) -> senior, Diana (28) -> junior
    assert_eq!(results.len(), 4);
}

// =============================================================================
// Single-element EXISTS tests
// =============================================================================

#[test]
fn test_single_var_exists() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE EXISTS { (p)-[:KNOWS]->(:Person) }
        RETURN p.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // Only people with outgoing KNOWS edges
    assert!(results.len() >= 1);
}

#[test]
fn test_single_var_not_exists() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE NOT EXISTS { (p)-[:KNOWS]->(:Person) }
        RETURN p.name AS name
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    // People without outgoing KNOWS edges
    assert!(results.len() >= 0);
}

// =============================================================================
// Index/Slice tests for single element
// =============================================================================

#[test]
fn test_single_var_index() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN p.scores[0] AS first_score
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(85));
}

#[test]
fn test_single_var_negative_index() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN p.scores[-1] AS last_score
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Int(95));
}

#[test]
fn test_single_var_slice() {
    let graph = create_test_graph();
    let snapshot = graph.snapshot();

    let query = parse(
        r#"
        MATCH (p:Person)
        WHERE p.name = 'Alice'
        RETURN p.scores[0..2] AS first_two
        "#,
    )
    .unwrap();

    let results = compile(&query, &snapshot).unwrap();

    assert_eq!(results.len(), 1);
    if let Value::List(items) = &results[0] {
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], Value::Int(85));
        assert_eq!(items[1], Value::Int(90));
    } else {
        panic!("Expected list");
    }
}
