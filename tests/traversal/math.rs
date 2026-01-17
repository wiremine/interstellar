//! Integration tests for the math() traversal step.
//!
//! Tests cover:
//! - Basic arithmetic operations with current value (_)
//! - Mathematical functions (sqrt, abs, sin, cos, etc.)
//! - Constants (pi, e)
//! - Anonymous traversal `__::math()`
//! - BoundTraversal math() with by() modulators
//! - Labeled path value extraction
//! - Edge cases (empty input, non-numeric values, domain errors)
//! - Metadata preservation (path, loops, bulk)

use std::collections::HashMap;

use interstellar::graph::Graph;
use interstellar::storage::InMemoryGraph;
use interstellar::traversal::__;
use interstellar::value::{Value, VertexId};

use crate::common::graphs::{create_small_graph, TestGraphBuilder};

// =============================================================================
// Helper Functions
// =============================================================================

/// Create a graph with numeric properties for math testing.
fn create_math_test_graph() -> (Graph, VertexId, VertexId, VertexId) {
    let mut storage = InMemoryGraph::new();

    // Vertex with positive values
    let v1 = storage.add_vertex("point", {
        let mut props = HashMap::new();
        props.insert("x".to_string(), Value::Int(3));
        props.insert("y".to_string(), Value::Int(4));
        props.insert("value".to_string(), Value::Float(16.0));
        props
    });

    // Vertex with negative value
    let v2 = storage.add_vertex("point", {
        let mut props = HashMap::new();
        props.insert("x".to_string(), Value::Int(-5));
        props.insert("y".to_string(), Value::Int(12));
        props.insert("value".to_string(), Value::Float(-9.0));
        props
    });

    // Vertex with float values
    let v3 = storage.add_vertex("point", {
        let mut props = HashMap::new();
        props.insert("x".to_string(), Value::Float(1.5));
        props.insert("y".to_string(), Value::Float(2.5));
        props.insert("value".to_string(), Value::Float(0.0));
        props
    });

    // Add edge between v1 and v2 with numeric property
    storage
        .add_edge(v1, v2, "connects", {
            let mut props = HashMap::new();
            props.insert("weight".to_string(), Value::Float(2.5));
            props
        })
        .unwrap();

    (Graph::new(storage), v1, v2, v3)
}

/// Helper to check if two f64 values are approximately equal.
fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-10
}

/// Extract f64 from Value, handling both Int and Float
fn extract_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Float(f) => Some(*f),
        Value::Int(i) => Some(*i as f64),
        _ => None,
    }
}

// =============================================================================
// Basic Arithmetic with Current Value (_)
// =============================================================================

#[test]
fn math_multiply_current_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([5i64, 10i64]).math("_ * 2").build().to_list();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::Float(10.0));
    assert_eq!(results[1], Value::Float(20.0));
}

#[test]
fn math_add_to_current_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .inject([1i64, 2i64, 3i64])
        .math("_ + 10")
        .build()
        .to_list();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Float(11.0));
    assert_eq!(results[1], Value::Float(12.0));
    assert_eq!(results[2], Value::Float(13.0));
}

#[test]
fn math_subtract_from_current_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([100i64]).math("_ - 42").build().to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(58.0));
}

#[test]
fn math_divide_current_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([20i64, 50i64]).math("_ / 5").build().to_list();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::Float(4.0));
    assert_eq!(results[1], Value::Float(10.0));
}

#[test]
fn math_modulo_current_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([17i64, 20i64]).math("_ % 5").build().to_list();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::Float(2.0));
    assert_eq!(results[1], Value::Float(0.0));
}

#[test]
fn math_power_current_value() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([2i64, 3i64]).math("_ ^ 3").build().to_list();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::Float(8.0));
    assert_eq!(results[1], Value::Float(27.0));
}

#[test]
fn math_with_float_input() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .inject([Value::Float(2.5), Value::Float(3.5)])
        .math("_ * 2")
        .build()
        .to_list();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::Float(5.0));
    assert_eq!(results[1], Value::Float(7.0));
}

#[test]
fn math_complex_expression() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // (5 + 3) * 2 = 16
    let results = g.inject([5i64]).math("(_ + 3) * 2").build().to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(16.0));
}

#[test]
fn math_operator_precedence() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // 2 + 3 * 4 = 14 (not 20)
    let results = g.inject([2i64]).math("_ + 3 * 4").build().to_list();

    assert_eq!(results.len(), 1);
    // This should be 2 + 12 = 14
    assert_eq!(results[0], Value::Float(14.0));
}

// =============================================================================
// Mathematical Functions
// =============================================================================

#[test]
fn math_sqrt_function() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .inject([16i64, 25i64, 100i64])
        .math("sqrt(_)")
        .build()
        .to_list();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Float(4.0));
    assert_eq!(results[1], Value::Float(5.0));
    assert_eq!(results[2], Value::Float(10.0));
}

#[test]
fn math_abs_function() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .inject([Value::Int(-5), Value::Int(5), Value::Float(-3.14)])
        .math("abs(_)")
        .build()
        .to_list();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Float(5.0));
    assert_eq!(results[1], Value::Float(5.0));
    assert!(approx_eq(extract_f64(&results[2]).unwrap(), 3.14));
}

#[test]
fn math_pow_function() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([2i64]).math("pow(_, 10)").build().to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(1024.0));
}

#[test]
fn math_min_max_functions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let min_results = g.inject([5i64]).math("min(_, 3)").build().to_list();
    assert_eq!(min_results[0], Value::Float(3.0));

    let max_results = g.inject([5i64]).math("max(_, 3)").build().to_list();
    assert_eq!(max_results[0], Value::Float(5.0));
}

#[test]
fn math_floor_ceil_round() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let floor = g
        .inject([Value::Float(3.7)])
        .math("floor(_)")
        .build()
        .to_list();
    assert_eq!(floor[0], Value::Float(3.0));

    let ceil = g
        .inject([Value::Float(3.2)])
        .math("ceil(_)")
        .build()
        .to_list();
    assert_eq!(ceil[0], Value::Float(4.0));

    let round = g
        .inject([Value::Float(3.5)])
        .math("round(_)")
        .build()
        .to_list();
    assert_eq!(round[0], Value::Float(4.0));
}

#[test]
fn math_trigonometric_functions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // sin(0) = 0
    let sin_results = g.inject([0i64]).math("sin(_)").build().to_list();
    assert!(approx_eq(extract_f64(&sin_results[0]).unwrap(), 0.0));

    // cos(0) = 1
    let cos_results = g.inject([0i64]).math("cos(_)").build().to_list();
    assert!(approx_eq(extract_f64(&cos_results[0]).unwrap(), 1.0));

    // tan(0) = 0
    let tan_results = g.inject([0i64]).math("tan(_)").build().to_list();
    assert!(approx_eq(extract_f64(&tan_results[0]).unwrap(), 0.0));
}

#[test]
fn math_log_exp_functions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // exp(0) = 1
    let exp_results = g.inject([0i64]).math("exp(_)").build().to_list();
    assert!(approx_eq(extract_f64(&exp_results[0]).unwrap(), 1.0));

    // log(e) ≈ 1
    let log_results = g.inject([1i64]).math("log(e)").build().to_list();
    assert!(approx_eq(extract_f64(&log_results[0]).unwrap(), 1.0));

    // log10(100) = 2
    let log10_results = g.inject([100i64]).math("log10(_)").build().to_list();
    assert!(approx_eq(extract_f64(&log10_results[0]).unwrap(), 2.0));
}

#[test]
fn math_nested_functions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // sqrt(abs(-16)) = 4
    let results = g.inject([-16i64]).math("sqrt(abs(_))").build().to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(4.0));
}

// =============================================================================
// Constants (pi, e)
// =============================================================================

#[test]
fn math_pi_constant() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([1i64]).math("pi").build().to_list();

    assert_eq!(results.len(), 1);
    assert!(approx_eq(
        extract_f64(&results[0]).unwrap(),
        std::f64::consts::PI
    ));
}

#[test]
fn math_e_constant() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([1i64]).math("e").build().to_list();

    assert_eq!(results.len(), 1);
    assert!(approx_eq(
        extract_f64(&results[0]).unwrap(),
        std::f64::consts::E
    ));
}

#[test]
fn math_pi_in_expression() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // sin(pi/2) = 1
    let results = g.inject([1i64]).math("sin(pi / 2)").build().to_list();

    assert_eq!(results.len(), 1);
    assert!(approx_eq(extract_f64(&results[0]).unwrap(), 1.0));
}

// =============================================================================
// Traversal API Integration (values -> math)
// =============================================================================

#[test]
fn math_on_vertex_property_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get ages and double them
    let results = g
        .v()
        .has_label("person")
        .values("age")
        .math("_ * 2")
        .build()
        .to_list();

    assert_eq!(results.len(), 3);
    // Ages are 30, 25, 35 -> doubled: 60, 50, 70
    let values: Vec<f64> = results.iter().filter_map(|v| extract_f64(v)).collect();
    assert!(values.contains(&60.0));
    assert!(values.contains(&50.0));
    assert!(values.contains(&70.0));
}

#[test]
fn math_pythagorean_on_properties() {
    let (graph, v1, _v2, _v3) = create_math_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // sqrt(3^2 + 4^2) = 5 for v1 (x=3, y=4)
    // We need to use labeled paths for this
    let results = g
        .v_ids([v1])
        .as_("p")
        .values("x")
        .math("sqrt(_ ^ 2 + 16)") // Using hardcoded y^2=16 for simplicity
        .build()
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(5.0));
}

// =============================================================================
// Anonymous Traversal __::math()
// =============================================================================

#[test]
fn anonymous_math_basic() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let anon = __::math("_ * 3").build();
    let results = g.inject([2i64, 4i64]).append(anon).to_list();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::Float(6.0));
    assert_eq!(results[1], Value::Float(12.0));
}

#[test]
fn anonymous_math_with_sqrt() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let anon = __::math("sqrt(_)").build();
    let results = g.inject([4i64, 9i64, 16i64]).append(anon).to_list();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Float(2.0));
    assert_eq!(results[1], Value::Float(3.0));
    assert_eq!(results[2], Value::Float(4.0));
}

#[test]
fn anonymous_math_chained_with_filter() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Double values then filter > 10
    let anon = __::math("_ * 2").build();
    let results = g
        .inject([3i64, 5i64, 7i64, 10i64])
        .append(anon)
        .filter(|_ctx, v| matches!(v, Value::Float(f) if *f > 10.0))
        .to_list();

    assert_eq!(results.len(), 2); // 14 and 20
    assert_eq!(results[0], Value::Float(14.0));
    assert_eq!(results[1], Value::Float(20.0));
}

#[test]
fn anonymous_math_in_complex_traversal() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Get ages from people, add 10 years
    let anon = __::values("age");
    let math_step = __::math("_ + 10").build();

    let results = g
        .v()
        .has_label("person")
        .append(anon)
        .append(math_step)
        .to_list();

    assert_eq!(results.len(), 3);
    // Ages are 30, 25, 35 -> +10: 40, 35, 45
    let values: Vec<f64> = results.iter().filter_map(|v| extract_f64(v)).collect();
    assert!(values.contains(&40.0));
    assert!(values.contains(&35.0));
    assert!(values.contains(&45.0));
}

// =============================================================================
// BoundTraversal with by() Modulators
// =============================================================================

#[test]
fn math_with_labeled_path_values() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Calculate age difference: Alice's age (30) - Bob's age (25) = 5
    // Note: with_path() is required for labeled path values
    let results = g
        .v_ids([tg.alice])
        .with_path()
        .as_("a")
        .out_labels(&["knows"])
        .has_label("person")
        .as_("b")
        .math("a - b")
        .by("a", "age")
        .by("b", "age")
        .build()
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(5.0)); // 30 - 25 = 5
}

#[test]
fn math_with_multiple_labeled_values() {
    let (graph, v1, _v2, _v3) = create_math_test_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Calculate: sqrt(a.x^2 + b.y^2) using labeled values
    // v1 has x=3, y=4, so sqrt(9+16) = 5
    // Note: with_path() is required for labeled path values
    let results = g
        .v_ids([v1])
        .with_path()
        .as_("a")
        .out() // go to v2
        .as_("b")
        .math("sqrt(a ^ 2 + b ^ 2)")
        .by("a", "x") // a = 3
        .by("b", "y") // b = 12
        .build()
        .to_list();

    assert_eq!(results.len(), 1);
    // sqrt(3^2 + 12^2) = sqrt(9 + 144) = sqrt(153) ≈ 12.369
    let result = extract_f64(&results[0]).unwrap();
    assert!(approx_eq(result, (153.0_f64).sqrt()));
}

#[test]
fn math_age_difference_chain() {
    let graph = TestGraphBuilder::new()
        .add_person("Alice", 40)
        .add_person("Bob", 30)
        .add_person("Carol", 25)
        .add_edge(0, 1, "knows")
        .add_edge(1, 2, "knows")
        .build();

    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Start from first vertex (Alice), traverse to Bob
    // Note: with_path() is required for labeled path values
    let results = g
        .v()
        .has_value("name", "Alice")
        .with_path()
        .as_("older")
        .out_labels(&["knows"])
        .as_("younger")
        .math("older - younger")
        .by("older", "age")
        .by("younger", "age")
        .build()
        .to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(10.0)); // 40 - 30 = 10
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn math_empty_input_stream() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // No vertices with label "nonexistent"
    let results = g
        .v()
        .has_label("nonexistent")
        .values("age")
        .math("_ * 2")
        .build()
        .to_list();

    assert!(results.is_empty());
}

#[test]
fn math_non_numeric_values_filtered() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Mix of numeric and string values - strings should be filtered out
    let results = g
        .inject([
            Value::Int(5),
            Value::String("hello".to_string()),
            Value::Float(3.0),
            Value::Bool(true),
        ])
        .math("_ * 2")
        .build()
        .to_list();

    assert_eq!(results.len(), 2); // Only 5 and 3.0
    assert_eq!(results[0], Value::Float(10.0));
    assert_eq!(results[1], Value::Float(6.0));
}

#[test]
fn math_division_by_zero_filtered() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // 5/0 = Inf, should be filtered
    let results = g.inject([5i64]).math("_ / 0").build().to_list();

    assert!(results.is_empty()); // Division by zero produces Inf, filtered out
}

#[test]
fn math_sqrt_negative_filtered() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // sqrt(-1) = NaN, should be filtered
    let results = g
        .inject([-1i64, 4i64, -9i64, 16i64])
        .math("sqrt(_)")
        .build()
        .to_list();

    assert_eq!(results.len(), 2); // Only 4 and 16
    assert_eq!(results[0], Value::Float(2.0));
    assert_eq!(results[1], Value::Float(4.0));
}

#[test]
fn math_log_zero_filtered() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // log(0) = -Inf, should be filtered
    let results = g.inject([0i64, 1i64]).math("log(_)").build().to_list();

    assert_eq!(results.len(), 1); // Only log(1) = 0
    assert!(approx_eq(extract_f64(&results[0]).unwrap(), 0.0));
}

#[test]
fn math_log_negative_filtered() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // log(-1) = NaN, should be filtered
    let results = g
        .inject([-1i64, 1i64, -10i64])
        .math("log(_)")
        .build()
        .to_list();

    assert_eq!(results.len(), 1); // Only log(1) = 0
}

#[test]
fn math_very_large_numbers() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Large number that doesn't overflow
    let results = g.inject([1_000_000i64]).math("_ * 1000").build().to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(1_000_000_000.0));
}

#[test]
fn math_overflow_filtered() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Extremely large exponent that overflows to Inf
    let results = g
        .inject([Value::Float(1e308)])
        .math("_ * 1e308")
        .build()
        .to_list();

    assert!(results.is_empty()); // Inf is filtered
}

// =============================================================================
// Metadata Preservation
// =============================================================================

#[test]
fn math_preserves_path() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Check that path is preserved through math step
    let results = g
        .v_ids([tg.alice])
        .as_("start")
        .values("age")
        .math("_ * 2")
        .build()
        .path()
        .to_list();

    assert_eq!(results.len(), 1);
    // Path should contain the labeled elements
    if let Value::List(path) = &results[0] {
        assert!(!path.is_empty());
    } else {
        panic!("Expected path to be a list");
    }
}

// =============================================================================
// Expression Without Current Value
// =============================================================================

#[test]
fn math_constant_expression() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Expression that doesn't use current value
    let results = g.inject([1i64, 2i64, 3i64]).math("2 + 3").build().to_list();

    assert_eq!(results.len(), 3);
    // All results should be 5
    for r in &results {
        assert_eq!(*r, Value::Float(5.0));
    }
}

#[test]
fn math_pi_times_two() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Expression using constant without current value
    let results = g.inject([1i64]).math("pi * 2").build().to_list();

    assert_eq!(results.len(), 1);
    assert!(approx_eq(
        extract_f64(&results[0]).unwrap(),
        std::f64::consts::PI * 2.0
    ));
}

// =============================================================================
// Real-World Use Cases
// =============================================================================

#[test]
fn math_percentage_calculation() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Calculate percentage: value / 100
    let results = g
        .inject([75i64, 50i64, 25i64])
        .math("_ / 100")
        .build()
        .to_list();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Float(0.75));
    assert_eq!(results[1], Value::Float(0.50));
    assert_eq!(results[2], Value::Float(0.25));
}

#[test]
fn math_temperature_conversion() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Celsius to Fahrenheit: F = C * 9/5 + 32
    let results = g
        .inject([0i64, 100i64, 37i64])
        .math("_ * 9 / 5 + 32")
        .build()
        .to_list();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Float(32.0)); // 0°C = 32°F
    assert_eq!(results[1], Value::Float(212.0)); // 100°C = 212°F
    assert!(approx_eq(extract_f64(&results[2]).unwrap(), 98.6)); // 37°C ≈ 98.6°F
}

#[test]
fn math_distance_formula() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Distance from origin: sqrt(x^2 + y^2) where x=y=current value
    // This is a simplification - same x and y
    let results = g
        .inject([3i64])
        .math("sqrt(_ ^ 2 + _ ^ 2)")
        .build()
        .to_list();

    assert_eq!(results.len(), 1);
    // sqrt(9 + 9) = sqrt(18) ≈ 4.243
    assert!(approx_eq(
        extract_f64(&results[0]).unwrap(),
        (18.0_f64).sqrt()
    ));
}

#[test]
fn math_compound_interest() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // Simple interest for 1 year at 5%: principal * (1 + 0.05)
    let results = g.inject([1000i64]).math("_ * 1.05").build().to_list();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], Value::Float(1050.0));
}

// =============================================================================
// Clamp Function Tests
// =============================================================================

#[test]
fn math_clamp_function() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // clamp(value, min, max)
    let results = g
        .inject([5i64, 15i64, 25i64])
        .math("clamp(_, 10, 20)")
        .build()
        .to_list();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Float(10.0)); // 5 clamped to 10
    assert_eq!(results[1], Value::Float(15.0)); // 15 unchanged
    assert_eq!(results[2], Value::Float(20.0)); // 25 clamped to 20
}

// =============================================================================
// Hyperbolic Functions
// =============================================================================

#[test]
fn math_hyperbolic_functions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // sinh(0) = 0
    let sinh_results = g.inject([0i64]).math("sinh(_)").build().to_list();
    assert!(approx_eq(extract_f64(&sinh_results[0]).unwrap(), 0.0));

    // cosh(0) = 1
    let cosh_results = g.inject([0i64]).math("cosh(_)").build().to_list();
    assert!(approx_eq(extract_f64(&cosh_results[0]).unwrap(), 1.0));

    // tanh(0) = 0
    let tanh_results = g.inject([0i64]).math("tanh(_)").build().to_list();
    assert!(approx_eq(extract_f64(&tanh_results[0]).unwrap(), 0.0));
}

// =============================================================================
// Additional Math Functions
// =============================================================================

#[test]
fn math_cbrt_function() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([27i64, 8i64]).math("cbrt(_)").build().to_list();

    assert_eq!(results.len(), 2);
    assert!(approx_eq(extract_f64(&results[0]).unwrap(), 3.0));
    assert!(approx_eq(extract_f64(&results[1]).unwrap(), 2.0));
}

#[test]
fn math_log2_function() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g.inject([8i64, 16i64]).math("log2(_)").build().to_list();

    assert_eq!(results.len(), 2);
    assert!(approx_eq(extract_f64(&results[0]).unwrap(), 3.0));
    assert!(approx_eq(extract_f64(&results[1]).unwrap(), 4.0));
}

#[test]
fn math_signum_function() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .inject([Value::Int(-5), Value::Int(0), Value::Int(5)])
        .math("signum(_)")
        .build()
        .to_list();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], Value::Float(-1.0));
    assert_eq!(results[1], Value::Float(0.0));
    assert_eq!(results[2], Value::Float(1.0));
}

#[test]
fn math_trunc_function() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    let results = g
        .inject([Value::Float(3.7), Value::Float(-3.7)])
        .math("trunc(_)")
        .build()
        .to_list();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Value::Float(3.0));
    assert_eq!(results[1], Value::Float(-3.0));
}

// =============================================================================
// Inverse Trigonometric Functions
// =============================================================================

#[test]
fn math_inverse_trig_functions() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // asin(0) = 0
    let asin_results = g.inject([0i64]).math("asin(_)").build().to_list();
    assert!(approx_eq(extract_f64(&asin_results[0]).unwrap(), 0.0));

    // acos(1) = 0
    let acos_results = g.inject([1i64]).math("acos(_)").build().to_list();
    assert!(approx_eq(extract_f64(&acos_results[0]).unwrap(), 0.0));

    // atan(0) = 0
    let atan_results = g.inject([0i64]).math("atan(_)").build().to_list();
    assert!(approx_eq(extract_f64(&atan_results[0]).unwrap(), 0.0));
}

#[test]
fn math_inverse_trig_domain_errors() {
    let tg = create_small_graph();
    let snapshot = tg.graph.snapshot();
    let g = snapshot.traversal();

    // asin(2) is out of domain [-1, 1], returns NaN -> filtered
    let results = g.inject([2i64]).math("asin(_)").build().to_list();
    assert!(results.is_empty());

    // acos(2) is out of domain [-1, 1], returns NaN -> filtered
    let results = g.inject([2i64]).math("acos(_)").build().to_list();
    assert!(results.is_empty());
}
