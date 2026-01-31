//! GraphSON 3.0 Import/Export Example
//!
//! This example demonstrates how to:
//! - Export a graph to GraphSON format
//! - Import a graph from GraphSON format
//! - Round-trip graphs through GraphSON
//! - Export with schema metadata
//!
//! Run with: cargo run --example graphson

use interstellar::graphson;
use interstellar::schema::{PropertyType, SchemaBuilder, ValidationMode};
use interstellar::storage::{Graph, GraphStorage};
use interstellar::value::Value;
use std::collections::HashMap;

fn main() {
    println!("=== GraphSON 3.0 Import/Export Example ===\n");

    // Create a sample social network graph
    let graph = create_sample_graph();
    println!("Created sample graph:");
    println!("  Vertices: {}", graph.snapshot().vertex_count());
    println!("  Edges: {}", graph.snapshot().edge_count());

    // -------------------------------------------------------------------------
    // Example 1: Basic Export
    // -------------------------------------------------------------------------
    println!("\n--- Example 1: Basic Export ---\n");

    let json = graph.to_graphson_pretty().expect("Failed to serialize");
    println!("GraphSON output (truncated):");
    print_truncated(&json, 40);

    // -------------------------------------------------------------------------
    // Example 2: Import from GraphSON
    // -------------------------------------------------------------------------
    println!("\n--- Example 2: Import from GraphSON ---\n");

    let imported = Graph::from_graphson(&json).expect("Failed to deserialize");
    println!("Imported graph:");
    println!("  Vertices: {}", imported.snapshot().vertex_count());
    println!("  Edges: {}", imported.snapshot().edge_count());

    // Verify the data is preserved
    let snapshot = imported.snapshot();
    println!("\nImported vertices:");
    for vertex in snapshot.all_vertices() {
        let name = vertex
            .properties
            .get("name")
            .map(|v| format!("{:?}", v))
            .unwrap_or_else(|| "N/A".to_string());
        println!("  {} ({}): name={}", vertex.id.0, vertex.label, name);
    }

    // -------------------------------------------------------------------------
    // Example 3: Export with Schema
    // -------------------------------------------------------------------------
    println!("\n--- Example 3: Export with Schema ---\n");

    let schema = SchemaBuilder::new()
        .mode(ValidationMode::Strict)
        .vertex("person")
        .property("name", PropertyType::String)
        .optional("age", PropertyType::Int)
        .optional("active", PropertyType::Bool)
        .done()
        .vertex("software")
        .property("name", PropertyType::String)
        .optional("lang", PropertyType::String)
        .done()
        .edge("knows")
        .from(&["person"])
        .to(&["person"])
        .optional("weight", PropertyType::Float)
        .done()
        .edge("created")
        .from(&["person"])
        .to(&["software"])
        .done()
        .build();

    let json_with_schema = graphson::to_string_with_schema(&graph.snapshot(), &schema)
        .expect("Failed to serialize with schema");

    println!("GraphSON with schema (truncated):");
    print_truncated(&json_with_schema, 50);

    // -------------------------------------------------------------------------
    // Example 4: Using graphson module directly
    // -------------------------------------------------------------------------
    println!("\n--- Example 4: Module Functions ---\n");

    // Compact output (no pretty printing)
    let compact = graphson::to_string(&graph.snapshot()).expect("Failed to serialize");
    println!("Compact JSON length: {} bytes", compact.len());

    // Pretty output
    let pretty = graphson::to_string_pretty(&graph.snapshot()).expect("Failed to serialize");
    println!("Pretty JSON length: {} bytes", pretty.len());

    // -------------------------------------------------------------------------
    // Example 5: Complex Property Types
    // -------------------------------------------------------------------------
    println!("\n--- Example 5: Complex Property Types ---\n");

    let complex_graph = Graph::new();

    // Add a vertex with various property types
    let mut tags = HashMap::new();
    tags.insert("priority".to_string(), Value::Int(1));
    tags.insert("status".to_string(), Value::String("active".to_string()));

    complex_graph.add_vertex(
        "item",
        HashMap::from([
            (
                "name".to_string(),
                Value::String("Complex Item".to_string()),
            ),
            ("count".to_string(), Value::Int(42)),
            ("score".to_string(), Value::Float(3.14159)),
            ("enabled".to_string(), Value::Bool(true)),
            (
                "tags".to_string(),
                Value::List(vec![
                    Value::String("rust".to_string()),
                    Value::String("graph".to_string()),
                    Value::String("database".to_string()),
                ]),
            ),
            ("metadata".to_string(), Value::Map(tags)),
        ]),
    );

    let complex_json = complex_graph
        .to_graphson_pretty()
        .expect("Failed to serialize");
    println!("Complex properties serialized:");
    print_truncated(&complex_json, 60);

    // Round-trip and verify
    let reimported = Graph::from_graphson(&complex_json).expect("Failed to deserialize");
    let vertices: Vec<_> = reimported.snapshot().all_vertices().collect();
    println!("\nRe-imported properties:");
    for (key, value) in &vertices[0].properties {
        println!("  {}: {:?}", key, value);
    }

    // -------------------------------------------------------------------------
    // Example 6: TinkerPop Compatibility
    // -------------------------------------------------------------------------
    println!("\n--- Example 6: TinkerPop Compatible Format ---\n");

    // This is the standard TinkerPop GraphSON 3.0 format
    let tinkerpop_json = r#"{
        "@type": "tinker:graph",
        "@value": {
            "vertices": [
                {
                    "@type": "g:Vertex",
                    "@value": {
                        "id": {"@type": "g:Int64", "@value": 1},
                        "label": "person",
                        "properties": {
                            "name": [{
                                "@type": "g:VertexProperty",
                                "@value": {
                                    "id": {"@type": "g:Int64", "@value": 0},
                                    "label": "name",
                                    "value": "marko"
                                }
                            }],
                            "age": [{
                                "@type": "g:VertexProperty",
                                "@value": {
                                    "id": {"@type": "g:Int64", "@value": 1},
                                    "label": "age",
                                    "value": {"@type": "g:Int32", "@value": 29}
                                }
                            }]
                        }
                    }
                },
                {
                    "@type": "g:Vertex",
                    "@value": {
                        "id": {"@type": "g:Int64", "@value": 2},
                        "label": "person",
                        "properties": {
                            "name": [{
                                "@type": "g:VertexProperty",
                                "@value": {
                                    "id": {"@type": "g:Int64", "@value": 2},
                                    "label": "name",
                                    "value": "vadas"
                                }
                            }],
                            "age": [{
                                "@type": "g:VertexProperty",
                                "@value": {
                                    "id": {"@type": "g:Int64", "@value": 3},
                                    "label": "age",
                                    "value": {"@type": "g:Int32", "@value": 27}
                                }
                            }]
                        }
                    }
                }
            ],
            "edges": [
                {
                    "@type": "g:Edge",
                    "@value": {
                        "id": {"@type": "g:Int64", "@value": 7},
                        "label": "knows",
                        "outV": {"@type": "g:Int64", "@value": 1},
                        "outVLabel": "person",
                        "inV": {"@type": "g:Int64", "@value": 2},
                        "inVLabel": "person",
                        "properties": {
                            "weight": {
                                "@type": "g:Property",
                                "@value": {
                                    "key": "weight",
                                    "value": {"@type": "g:Double", "@value": 0.5}
                                }
                            }
                        }
                    }
                }
            ]
        }
    }"#;

    let tinkerpop_graph =
        graphson::from_str(tinkerpop_json).expect("Failed to parse TinkerPop format");
    println!("Imported TinkerPop 'modern' graph subset:");
    println!("  Vertices: {}", tinkerpop_graph.snapshot().vertex_count());
    println!("  Edges: {}", tinkerpop_graph.snapshot().edge_count());

    for vertex in tinkerpop_graph.snapshot().all_vertices() {
        let name = vertex
            .properties
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        let age = vertex
            .properties
            .get("age")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        println!("  {} (age {})", name, age);
    }

    println!("\n=== Example Complete ===");
}

/// Create a sample social network graph
fn create_sample_graph() -> Graph {
    let graph = Graph::new();

    // Add people
    let alice = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Alice".to_string())),
            ("age".to_string(), Value::Int(30)),
            ("active".to_string(), Value::Bool(true)),
        ]),
    );

    let bob = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Bob".to_string())),
            ("age".to_string(), Value::Int(25)),
            ("active".to_string(), Value::Bool(true)),
        ]),
    );

    let charlie = graph.add_vertex(
        "person",
        HashMap::from([
            ("name".to_string(), Value::String("Charlie".to_string())),
            ("age".to_string(), Value::Int(35)),
            ("active".to_string(), Value::Bool(false)),
        ]),
    );

    // Add software
    let rust_project = graph.add_vertex(
        "software",
        HashMap::from([
            (
                "name".to_string(),
                Value::String("Interstellar".to_string()),
            ),
            ("lang".to_string(), Value::String("Rust".to_string())),
        ]),
    );

    // Add relationships
    graph
        .add_edge(
            alice,
            bob,
            "knows",
            HashMap::from([("weight".to_string(), Value::Float(0.8))]),
        )
        .unwrap();

    graph
        .add_edge(
            alice,
            charlie,
            "knows",
            HashMap::from([("weight".to_string(), Value::Float(0.6))]),
        )
        .unwrap();

    graph
        .add_edge(
            bob,
            charlie,
            "knows",
            HashMap::from([("weight".to_string(), Value::Float(0.9))]),
        )
        .unwrap();

    graph
        .add_edge(alice, rust_project, "created", HashMap::new())
        .unwrap();

    graph
        .add_edge(bob, rust_project, "created", HashMap::new())
        .unwrap();

    graph
}

/// Print truncated output with line limit
fn print_truncated(text: &str, max_lines: usize) {
    let lines: Vec<&str> = text.lines().collect();
    let show = lines.len().min(max_lines);

    for line in &lines[..show] {
        println!("{}", line);
    }

    if lines.len() > max_lines {
        println!("... ({} more lines)", lines.len() - max_lines);
    }
}
