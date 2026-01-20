//! British Royal Family Graph Database Example
//!
//! Comprehensive demonstration of Interstellar's graph database capabilities using
//! a family tree dataset with 70+ members of the British Royal Family.
//!
//! This example showcases:
//! - Data loading from JSON fixtures
//! - Fluent traversal API for ancestry/lineage queries
//! - GQL (Graph Query Language) queries
//!
//! Run: `cargo run --example british_royals`

use interstellar::storage::{Graph, GraphSnapshot, GraphStorage};
use interstellar::traversal::__;
use interstellar::value::{Value, VertexId};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;

// =============================================================================
// Data Loading
// =============================================================================

fn load_royals_graph() -> Graph {
    let json_str = fs::read_to_string("examples/fixtures/british_royals.json")
        .expect("Failed to read british_royals.json");
    let data: JsonValue = serde_json::from_str(&json_str).expect("Failed to parse JSON");

    let graph = Graph::new();
    let mut person_ids: HashMap<String, VertexId> = HashMap::new();

    // Load persons
    if let Some(persons) = data["persons"].as_array() {
        for person in persons {
            let json_id = person["id"].as_str().unwrap_or("unknown");
            let mut props = HashMap::new();

            for key in [
                "name",
                "full_name",
                "house",
                "birth_date",
                "birth_country",
                "death_date",
                "reign_start",
                "reign_end",
            ] {
                if let Some(v) = person[key].as_str() {
                    props.insert(key.to_string(), Value::String(v.to_string()));
                }
            }
            if let Some(v) = person["is_monarch"].as_bool() {
                props.insert("is_monarch".to_string(), Value::Bool(v));
            }
            if let Some(v) = person["abdicated"].as_bool() {
                props.insert("abdicated".to_string(), Value::Bool(v));
            }

            let vid = graph.add_vertex("person", props);
            person_ids.insert(json_id.to_string(), vid);
        }
    }

    // Load parent-child relationships
    if let Some(relations) = data["parent_child"].as_array() {
        for rel in relations {
            let parent_id = rel["parent_id"].as_str().unwrap_or("");
            let child_id = rel["child_id"].as_str().unwrap_or("");
            if let (Some(&p), Some(&c)) = (person_ids.get(parent_id), person_ids.get(child_id)) {
                let _ = graph.add_edge(p, c, "parent_of", HashMap::new());
                let _ = graph.add_edge(c, p, "child_of", HashMap::new());
            }
        }
    }

    // Load marriages
    if let Some(marriages) = data["marriages"].as_array() {
        for marriage in marriages {
            let p1_id = marriage["person1_id"].as_str().unwrap_or("");
            let p2_id = marriage["person2_id"].as_str().unwrap_or("");
            if let (Some(&p1), Some(&p2)) = (person_ids.get(p1_id), person_ids.get(p2_id)) {
                let mut props = HashMap::new();
                if let Some(d) = marriage["marriage_date"].as_str() {
                    props.insert("marriage_date".to_string(), Value::String(d.to_string()));
                }
                let _ = graph.add_edge(p1, p2, "married_to", props.clone());
                let _ = graph.add_edge(p2, p1, "married_to", props);
            }
        }
    }

    graph
}

// =============================================================================
// Helper Functions
// =============================================================================

fn get_name(snapshot: &GraphSnapshot, value: &Value) -> String {
    if let Some(vid) = value.as_vertex_id() {
        if let Some(vertex) = snapshot.get_vertex(vid) {
            if let Some(Value::String(name)) = vertex.properties.get("name") {
                return name.clone();
            }
        }
    }
    format!("{:?}", value)
}

fn display_names(snapshot: &GraphSnapshot, results: &[Value]) -> String {
    if results.is_empty() {
        return "(none)".to_string();
    }
    results
        .iter()
        .map(|v| get_name(snapshot, v))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::List(items) => format!(
            "[{}]",
            items
                .iter()
                .map(format_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Map(map) => format!(
            "{{{}}}",
            map.iter()
                .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => format!("{:?}", value),
    }
}

fn section(title: &str) {
    println!("\n{}", "=".repeat(60));
    println!("{}", title);
    println!("{}", "=".repeat(60));
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    println!("=== British Royal Family Graph Database Example ===\n");

    let graph = load_royals_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Graph statistics
    let vertices = g.v().count();
    let edges = g.e().count();
    println!("Graph loaded: {} persons, {} edges", vertices, edges);

    // =========================================================================
    // FLUENT API EXAMPLES
    // =========================================================================
    section("FLUENT TRAVERSAL API");

    println!("\n--- All monarchs ---");
    let monarchs = g.v().has_value("is_monarch", true).to_list();
    println!("{}", display_names(&snapshot, &monarchs));

    println!("\n--- Living royals (has_not death_date) ---");
    let living = g.v().has_label("person").has_not("death_date").to_list();
    println!(
        "{} living: {}",
        living.len(),
        display_names(&snapshot, &living)
    );

    println!("\n--- Elizabeth II's children ---");
    let children = g
        .v()
        .has_value("name", "Elizabeth II")
        .out_labels(&["parent_of"])
        .to_list();
    println!("{}", display_names(&snapshot, &children));

    println!("\n--- Charles III's spouses ---");
    let spouses = g
        .v()
        .has_value("name", "Charles III")
        .out_labels(&["married_to"])
        .to_list();
    println!("{}", display_names(&snapshot, &spouses));

    println!("\n--- Prince George's ancestors (repeat 4 generations) ---");
    let ancestors = g
        .v()
        .has_value("name", "Prince George")
        .repeat(__::out_labels(&["child_of"]))
        .times(4)
        .emit()
        .dedup()
        .to_list();
    println!("{}", display_names(&snapshot, &ancestors));

    println!("\n--- Victoria's monarch descendants ---");
    let line = g
        .v()
        .has_value("name", "Victoria")
        .repeat(__::out_labels(&["parent_of"]).has_value("is_monarch", true))
        .times(6)
        .emit()
        .to_list();
    println!("{}", display_names(&snapshot, &line));

    println!("\n--- Royals without children (not + where) ---");
    let childless = g
        .v()
        .has_label("person")
        .not(__::out_labels(&["parent_of"]))
        .to_list();
    println!("{} without children", childless.len());

    println!("\n--- Royals by house (group_count) ---");
    let by_house = g
        .v()
        .has_label("person")
        .group_count()
        .by_key("house")
        .build()
        .to_list();
    if let Some(Value::Map(map)) = by_house.first() {
        for (house, count) in map {
            println!("  {}: {}", house, format_value(count));
        }
    }

    println!("\n--- Monarchs ordered by reign start ---");
    let ordered = g
        .v()
        .has_value("is_monarch", true)
        .has("reign_start")
        .order()
        .by_key_asc("reign_start")
        .build()
        .to_list();
    for m in &ordered {
        if let Some(vid) = m.as_vertex_id() {
            if let Some(v) = snapshot.get_vertex(vid) {
                let name = v
                    .properties
                    .get("name")
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                let reign = v
                    .properties
                    .get("reign_start")
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                println!("  {} ({})", name, reign);
            }
        }
    }

    // =========================================================================
    // GQL EXAMPLES
    // =========================================================================
    section("GQL (GRAPH QUERY LANGUAGE)");

    println!("\n--- Count people (GQL) ---");
    let r = snapshot.gql("MATCH (p:person) RETURN count(*)").unwrap();
    println!("  {}", format_value(&r[0]));

    println!("\n--- Monarchs (GQL) ---");
    let r = snapshot
        .gql("MATCH (p:person) WHERE p.is_monarch = true RETURN p.name")
        .unwrap();
    let names: Vec<String> = r.iter().map(format_value).collect();
    println!("  {}", names.join(", "));

    println!("\n--- Elizabeth II's grandchildren (GQL multi-hop) ---");
    let r = snapshot
        .gql(
            r#"
            MATCH (e:person {name: 'Elizabeth II'})-[:parent_of]->()-[:parent_of]->(gc:person)
            RETURN DISTINCT gc.name
        "#,
        )
        .unwrap();
    let names: Vec<String> = r.iter().map(format_value).collect();
    println!("  {}", names.join(", "));

    println!("\n--- Royals with children (EXISTS) ---");
    let r = snapshot
        .gql(
            r#"
            MATCH (p:person)
            WHERE EXISTS { (p)-[:parent_of]->() }
            RETURN count(*)
        "#,
        )
        .unwrap();
    println!("  {} royals have children", format_value(&r[0]));

    println!("\n--- Prince George's ancestors (variable-length path) ---");
    let r = snapshot
        .gql(
            r#"
            MATCH (p:person {name: 'Prince George'})-[:child_of*1..4]->(ancestor:person)
            RETURN DISTINCT ancestor.name
        "#,
        )
        .unwrap();
    let names: Vec<String> = r.iter().map(format_value).collect();
    println!("  {}", names.join(", "));

    println!("\n--- House counts (GROUP BY) ---");
    let r = snapshot
        .gql(
            r#"
            MATCH (p:person)
            WHERE p.house IS NOT NULL
            RETURN p.house, count(*) AS cnt
            GROUP BY p.house
            ORDER BY cnt DESC
        "#,
        )
        .unwrap();
    for v in &r {
        if let Value::Map(m) = v {
            println!(
                "  {}: {}",
                format_value(m.get("p.house").unwrap_or(&Value::Null)),
                format_value(m.get("cnt").unwrap_or(&Value::Null))
            );
        }
    }

    println!("\n--- Monarch status (CASE expression) ---");
    let r = snapshot
        .gql(
            r#"
            MATCH (p:person)
            WHERE p.name IN ['Elizabeth II', 'Prince Philip']
            RETURN p.name,
                   CASE WHEN p.is_monarch = true THEN 'Monarch' ELSE 'Not Monarch' END AS status
        "#,
        )
        .unwrap();
    for v in &r {
        if let Value::Map(m) = v {
            println!(
                "  {} - {}",
                format_value(m.get("p.name").unwrap_or(&Value::Null)),
                format_value(m.get("status").unwrap_or(&Value::Null))
            );
        }
    }

    println!("\n--- Introspection: id() and labels() ---");
    let r = snapshot
        .gql(
            r#"
            MATCH (p:person {name: 'Victoria'})
            RETURN id(p) AS vid, labels(p) AS lbls
        "#,
        )
        .unwrap();
    for v in &r {
        if let Value::Map(m) = v {
            println!(
                "  ID: {}, Labels: {}",
                format_value(m.get("vid").unwrap_or(&Value::Null)),
                format_value(m.get("lbls").unwrap_or(&Value::Null))
            );
        }
    }

    println!("\n=== Example Complete ===");
}
