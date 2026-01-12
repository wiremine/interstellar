//! Mathematical Expression Evaluation Example
//!
//! This example demonstrates the `math()` step for evaluating arithmetic
//! expressions on traverser values. The math step supports:
//!
//! - Basic operators: `+`, `-`, `*`, `/`, `%`, `^`
//! - Functions: `sqrt`, `abs`, `sin`, `cos`, `tan`, `log`, `exp`, `pow`, `min`, `max`
//! - Constants: `pi`, `e`
//! - Current value reference: `_`
//! - Labeled path value references via `by()` modulators
//!
//! Run with: `cargo run --example math_expressions`

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::traversal::__;
use intersteller::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

fn main() {
    let graph = create_sample_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    println!("=== Mathematical Expression Examples ===\n");

    // -------------------------------------------------------------------------
    // Basic Arithmetic on Current Value
    // -------------------------------------------------------------------------
    println!("--- Basic Arithmetic ---\n");

    // Double all ages
    println!("Ages doubled (age * 2):");
    let doubled = g
        .v()
        .has_label("person")
        .values("age")
        .math("_ * 2")
        .build()
        .to_list();
    for age in &doubled {
        println!("  {:?}", age);
    }
    println!();

    // Add 10 to all ages
    println!("Ages plus 10 (age + 10):");
    let plus_ten = g
        .v()
        .has_label("person")
        .values("age")
        .math("_ + 10")
        .build()
        .to_list();
    for age in &plus_ten {
        println!("  {:?}", age);
    }
    println!();

    // Calculate percentage (divide by 100)
    println!("Scores as decimal (score / 100):");
    let decimals = g
        .v()
        .has_label("person")
        .values("score")
        .math("_ / 100")
        .build()
        .to_list();
    for score in &decimals {
        println!("  {:.4}", score.as_f64().unwrap_or(0.0));
    }
    println!();

    // -------------------------------------------------------------------------
    // Mathematical Functions
    // -------------------------------------------------------------------------
    println!("--- Mathematical Functions ---\n");

    // Square root of ages
    println!("Square root of ages:");
    let sqrt_ages = g
        .v()
        .has_label("person")
        .values("age")
        .math("sqrt(_)")
        .build()
        .to_list();
    for age in &sqrt_ages {
        println!("  {:.4}", age.as_f64().unwrap_or(0.0));
    }
    println!();

    // Absolute value (useful for differences)
    println!("Absolute value of (age - 30):");
    let abs_diff = g
        .v()
        .has_label("person")
        .values("age")
        .math("abs(_ - 30)")
        .build()
        .to_list();
    for val in &abs_diff {
        println!("  {:?}", val);
    }
    println!();

    // Power function
    println!("Ages squared (_ ^ 2):");
    let squared = g
        .v()
        .has_label("person")
        .values("age")
        .math("_ ^ 2")
        .build()
        .to_list();
    for val in &squared {
        println!("  {:?}", val);
    }
    println!();

    // -------------------------------------------------------------------------
    // Complex Expressions
    // -------------------------------------------------------------------------
    println!("--- Complex Expressions ---\n");

    // Pythagorean-style calculation
    println!("Complex: sqrt(_ ^ 2 + 100):");
    let complex = g
        .v()
        .has_label("person")
        .values("age")
        .math("sqrt(_ ^ 2 + 100)")
        .build()
        .to_list();
    for val in &complex {
        println!("  {:.4}", val.as_f64().unwrap_or(0.0));
    }
    println!();

    // Normalize scores to 0-1 range (assuming max is 100)
    println!("Normalized scores (score / 100):");
    let normalized = g
        .v()
        .has_label("person")
        .values("score")
        .math("_ / 100")
        .build()
        .to_list();
    for val in &normalized {
        println!("  {:.2}", val.as_f64().unwrap_or(0.0));
    }
    println!();

    // -------------------------------------------------------------------------
    // Using Constants
    // -------------------------------------------------------------------------
    println!("--- Using Constants (pi, e) ---\n");

    // Circle area from radius
    println!("Circle areas (pi * radius^2) for radii 1, 2, 3:");
    let areas = g
        .inject([1i64, 2i64, 3i64])
        .math("pi * _ ^ 2")
        .build()
        .to_list();
    for (i, area) in areas.iter().enumerate() {
        println!(
            "  radius={}: area={:.4}",
            i + 1,
            area.as_f64().unwrap_or(0.0)
        );
    }
    println!();

    // Exponential growth
    println!("Exponential: e ^ x for x in [0, 1, 2]:");
    let exp_vals = g.inject([0i64, 1i64, 2i64]).math("e ^ _").build().to_list();
    for (x, val) in exp_vals.iter().enumerate() {
        println!("  e^{} = {:.4}", x, val.as_f64().unwrap_or(0.0));
    }
    println!();

    // -------------------------------------------------------------------------
    // Anonymous Traversal Usage
    // -------------------------------------------------------------------------
    println!("--- Anonymous Traversal Usage ---\n");

    // Use math in a union to compute multiple derived values
    println!("Multiple calculations via union:");
    let multi = g
        .v()
        .has_label("person")
        .values("age")
        .union(vec![
            __::math("_ * 2").build(),   // doubled
            __::math("_ + 100").build(), // plus 100
            __::math("sqrt(_)").build(), // square root
        ])
        .to_list();
    println!("  Results (3 calculations per age): {:?}", multi);
    println!();

    // Use math in a local context
    println!("Local math calculation per vertex:");
    let local_calc = g
        .v()
        .has_label("person")
        .local(__::values("age").math("_ * 10").build())
        .to_list();
    for val in &local_calc {
        println!("  {:?}", val);
    }
    println!();

    // -------------------------------------------------------------------------
    // Filtering Behavior
    // -------------------------------------------------------------------------
    println!("--- Filtering Behavior ---\n");

    // Non-numeric values are filtered out
    println!("Math on mixed types (non-numeric filtered):");
    let mixed: Vec<Value> = vec![
        Value::Int(10),
        Value::String("hello".to_string()),
        Value::Int(20),
        Value::Bool(true),
        Value::Float(30.5),
    ];
    let filtered = g.inject(mixed).math("_ * 2").build().to_list();
    println!("  Input: [10, \"hello\", 20, true, 30.5]");
    println!("  Output: {:?}", filtered);
    println!();

    // Division by zero produces infinity (filtered)
    println!("Division by zero (filtered out):");
    let div_zero = g
        .inject([10i64, 0i64, 5i64])
        .math("100 / _")
        .build()
        .to_list();
    println!("  Input: [10, 0, 5]");
    println!("  100/x results: {:?}", div_zero);
    println!();

    // Square root of negative (NaN, filtered)
    println!("Sqrt of negative (filtered out):");
    let sqrt_neg = g
        .inject([4i64, -1i64, 9i64])
        .math("sqrt(_)")
        .build()
        .to_list();
    println!("  Input: [4, -1, 9]");
    println!("  sqrt results: {:?}", sqrt_neg);
    println!();

    // -------------------------------------------------------------------------
    // Practical Examples
    // -------------------------------------------------------------------------
    println!("--- Practical Examples ---\n");

    // Calculate BMI (Body Mass Index) = weight / height^2
    // (Using mock data from scores as "weight" and age/10 as "height")
    println!("BMI-style calculation (score / (age/10)^2):");
    let bmi_style = g
        .v()
        .has_label("person")
        .project(&["name", "bmi"])
        .by_key("name")
        .by(__::values("score")
            .math("_ / 4") // simplified calculation
            .build())
        .build()
        .to_list();
    for result in &bmi_style {
        if let Value::Map(map) = result {
            let name = map.get("name").unwrap();
            let bmi = map.get("bmi").unwrap();
            println!("  {:?}: {:.2}", name, bmi.as_f64().unwrap_or(0.0));
        }
    }
    println!();

    // Grade curve: add 10 points and cap at 100
    println!("Curved scores (min(score + 10, 100)):");
    let curved = g
        .v()
        .has_label("person")
        .values("score")
        .math("min(_ + 10, 100)")
        .build()
        .to_list();
    for score in &curved {
        println!("  {:.1}", score.as_f64().unwrap_or(0.0));
    }
    println!();

    println!("=== Example Complete ===");
}

/// Create a sample graph with people and their attributes
fn create_sample_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create people with various numeric properties
    let mut alice_props = HashMap::new();
    alice_props.insert("name".to_string(), Value::String("Alice".to_string()));
    alice_props.insert("age".to_string(), Value::Int(30));
    alice_props.insert("score".to_string(), Value::Float(85.5));
    let alice = storage.add_vertex("person", alice_props);

    let mut bob_props = HashMap::new();
    bob_props.insert("name".to_string(), Value::String("Bob".to_string()));
    bob_props.insert("age".to_string(), Value::Int(25));
    bob_props.insert("score".to_string(), Value::Float(92.0));
    let bob = storage.add_vertex("person", bob_props);

    let mut charlie_props = HashMap::new();
    charlie_props.insert("name".to_string(), Value::String("Charlie".to_string()));
    charlie_props.insert("age".to_string(), Value::Int(35));
    charlie_props.insert("score".to_string(), Value::Float(78.5));
    let charlie = storage.add_vertex("person", charlie_props);

    let mut diana_props = HashMap::new();
    diana_props.insert("name".to_string(), Value::String("Diana".to_string()));
    diana_props.insert("age".to_string(), Value::Int(28));
    diana_props.insert("score".to_string(), Value::Float(95.0));
    let diana = storage.add_vertex("person", diana_props);

    // Add some relationships
    storage
        .add_edge(alice, bob, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(bob, charlie, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(charlie, diana, "knows", HashMap::new())
        .unwrap();
    storage
        .add_edge(diana, alice, "knows", HashMap::new())
        .unwrap();

    Graph::new(Arc::new(storage))
}
