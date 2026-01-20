//! Marvel Universe Graph Example
//!
//! This example demonstrates graph traversal queries on the Marvel Universe dataset
//! containing 68 characters (heroes, villains, antiheroes), 14 teams/organizations,
//! and 6 locations with rich relationship data.
//!
//! The dataset includes:
//! - Character vertices with properties: name, alias, type, powers, first_appearance, affiliation, base
//! - Team vertices with properties: name, type, founded, base, purpose
//! - Location vertices with properties: name, type, notable_areas
//! - Relationship edges: member_of, rivals_with, allies_with, mentors, related_to, works_for, located_in
//!
//! This example showcases:
//! - Data loading from JSON fixtures
//! - Basic traversal with filtering (Phase 3)
//! - Navigation steps: out(), in_(), both() (Phase 3)
//! - Predicate system: p::eq, p::gt, p::within, p::containing (Phase 4)
//! - Anonymous traversals: __::out(), __::has_label() (Phase 4)
//! - Branch steps: union(), coalesce(), choose(), optional() (Phase 5)
//! - Repeat steps for relationship chain queries (Phase 5)
//! - Path tracking with as_() and select() (Phase 3)
//!
//! Run with: `cargo run --example marvel`

use interstellar::storage::{Graph, GraphSnapshot, GraphStorage};
use interstellar::traversal::{p, __};
use interstellar::value::{Value, VertexId};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;

// =============================================================================
// Data Loading
// =============================================================================

/// ID mappings for looking up vertices by their JSON IDs
struct IdMappings {
    characters: HashMap<String, VertexId>,
    teams: HashMap<String, VertexId>,
    locations: HashMap<String, VertexId>,
}

/// Load the Marvel Universe JSON fixture and build the graph.
fn load_marvel_graph() -> (Graph, IdMappings) {
    let json_str =
        fs::read_to_string("examples/fixtures/marvel.json").expect("Failed to read marvel.json");
    let data: JsonValue = serde_json::from_str(&json_str).expect("Failed to parse JSON");

    let graph = Graph::new();
    let mut mappings = IdMappings {
        characters: HashMap::new(),
        teams: HashMap::new(),
        locations: HashMap::new(),
    };

    // -------------------------------------------------------------------------
    // Load Characters
    // -------------------------------------------------------------------------
    if let Some(characters) = data["nodes"]["characters"].as_array() {
        for character in characters {
            let json_id = character["id"].as_str().unwrap_or("unknown");

            let mut props = HashMap::new();

            // String properties
            if let Some(v) = character["name"].as_str() {
                props.insert("name".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = character["alias"].as_str() {
                props.insert("alias".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = character["type"].as_str() {
                props.insert("type".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = character["base"].as_str() {
                props.insert("base".to_string(), Value::String(v.to_string()));
            }

            // Integer properties
            if let Some(v) = character["first_appearance"].as_i64() {
                props.insert("first_appearance".to_string(), Value::Int(v));
            }

            // List properties - powers
            if let Some(powers) = character["powers"].as_array() {
                let power_values: Vec<Value> = powers
                    .iter()
                    .filter_map(|p| p.as_str().map(|s| Value::String(s.to_string())))
                    .collect();
                if !power_values.is_empty() {
                    props.insert("powers".to_string(), Value::List(power_values));
                }
            }

            // List properties - affiliation
            if let Some(affiliations) = character["affiliation"].as_array() {
                let affiliation_values: Vec<Value> = affiliations
                    .iter()
                    .filter_map(|a| a.as_str().map(|s| Value::String(s.to_string())))
                    .collect();
                if !affiliation_values.is_empty() {
                    props.insert("affiliation".to_string(), Value::List(affiliation_values));
                }
            }

            // Store original JSON ID for lookups
            props.insert("json_id".to_string(), Value::String(json_id.to_string()));

            let vid = graph.add_vertex("character", props);
            mappings.characters.insert(json_id.to_string(), vid);
        }
    }

    // -------------------------------------------------------------------------
    // Load Teams
    // -------------------------------------------------------------------------
    if let Some(teams) = data["nodes"]["teams"].as_array() {
        for team in teams {
            let json_id = team["id"].as_str().unwrap_or("unknown");

            let mut props = HashMap::new();

            if let Some(v) = team["name"].as_str() {
                props.insert("name".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = team["type"].as_str() {
                props.insert("type".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = team["base"].as_str() {
                props.insert("base".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = team["purpose"].as_str() {
                props.insert("purpose".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = team["founded"].as_i64() {
                props.insert("founded".to_string(), Value::Int(v));
            }

            props.insert("json_id".to_string(), Value::String(json_id.to_string()));

            let vid = graph.add_vertex("team", props);
            mappings.teams.insert(json_id.to_string(), vid);
        }
    }

    // -------------------------------------------------------------------------
    // Load Locations
    // -------------------------------------------------------------------------
    if let Some(locations) = data["nodes"]["locations"].as_array() {
        for location in locations {
            let json_id = location["id"].as_str().unwrap_or("unknown");

            let mut props = HashMap::new();

            if let Some(v) = location["name"].as_str() {
                props.insert("name".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = location["type"].as_str() {
                props.insert("location_type".to_string(), Value::String(v.to_string()));
            }

            // Notable areas as list
            if let Some(areas) = location["notable_areas"].as_array() {
                let area_values: Vec<Value> = areas
                    .iter()
                    .filter_map(|a| a.as_str().map(|s| Value::String(s.to_string())))
                    .collect();
                if !area_values.is_empty() {
                    props.insert("notable_areas".to_string(), Value::List(area_values));
                }
            }

            props.insert("json_id".to_string(), Value::String(json_id.to_string()));

            let vid = graph.add_vertex("location", props);
            mappings.locations.insert(json_id.to_string(), vid);
        }
    }

    // -------------------------------------------------------------------------
    // Load Edges: member_of
    // -------------------------------------------------------------------------
    if let Some(edges) = data["edges"]["member_of"].as_array() {
        for edge in edges {
            let source = edge["source"].as_str().unwrap_or("");
            let target = edge["target"].as_str().unwrap_or("");

            if let (Some(&src_vid), Some(&dst_vid)) =
                (mappings.characters.get(source), mappings.teams.get(target))
            {
                let mut props = HashMap::new();
                if let Some(role) = edge["role"].as_str() {
                    props.insert("role".to_string(), Value::String(role.to_string()));
                }
                if let Some(joined) = edge["joined"].as_i64() {
                    props.insert("joined".to_string(), Value::Int(joined));
                }
                let _ = graph.add_edge(src_vid, dst_vid, "member_of", props);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Load Edges: rivals_with
    // -------------------------------------------------------------------------
    if let Some(edges) = data["edges"]["rivals_with"].as_array() {
        for edge in edges {
            let source = edge["source"].as_str().unwrap_or("");
            let target = edge["target"].as_str().unwrap_or("");

            // Try character -> character first
            let src_vid = mappings.characters.get(source).copied();
            let dst_vid = mappings
                .characters
                .get(target)
                .copied()
                .or_else(|| mappings.teams.get(target).copied());

            if let (Some(src), Some(dst)) = (src_vid, dst_vid) {
                let mut props = HashMap::new();
                if let Some(rivalry_type) = edge["rivalry_type"].as_str() {
                    props.insert(
                        "rivalry_type".to_string(),
                        Value::String(rivalry_type.to_string()),
                    );
                }
                if let Some(since) = edge["since"].as_i64() {
                    props.insert("since".to_string(), Value::Int(since));
                }
                let _ = graph.add_edge(src, dst, "rivals_with", props);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Load Edges: allies_with
    // -------------------------------------------------------------------------
    if let Some(edges) = data["edges"]["allies_with"].as_array() {
        for edge in edges {
            let source = edge["source"].as_str().unwrap_or("");
            let target = edge["target"].as_str().unwrap_or("");

            if let (Some(&src_vid), Some(&dst_vid)) = (
                mappings.characters.get(source),
                mappings.characters.get(target),
            ) {
                let mut props = HashMap::new();
                if let Some(alliance_type) = edge["alliance_type"].as_str() {
                    props.insert(
                        "alliance_type".to_string(),
                        Value::String(alliance_type.to_string()),
                    );
                }
                if let Some(strength) = edge["strength"].as_str() {
                    props.insert("strength".to_string(), Value::String(strength.to_string()));
                }
                let _ = graph.add_edge(src_vid, dst_vid, "allies_with", props);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Load Edges: mentors
    // -------------------------------------------------------------------------
    if let Some(edges) = data["edges"]["mentors"].as_array() {
        for edge in edges {
            let source = edge["source"].as_str().unwrap_or("");
            let target = edge["target"].as_str().unwrap_or("");

            if let (Some(&src_vid), Some(&dst_vid)) = (
                mappings.characters.get(source),
                mappings.characters.get(target),
            ) {
                let mut props = HashMap::new();
                if let Some(mentorship_type) = edge["mentorship_type"].as_str() {
                    props.insert(
                        "mentorship_type".to_string(),
                        Value::String(mentorship_type.to_string()),
                    );
                }
                if let Some(period) = edge["period"].as_str() {
                    props.insert("period".to_string(), Value::String(period.to_string()));
                }
                let _ = graph.add_edge(src_vid, dst_vid, "mentors", props);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Load Edges: related_to
    // -------------------------------------------------------------------------
    if let Some(edges) = data["edges"]["related_to"].as_array() {
        for edge in edges {
            let source = edge["source"].as_str().unwrap_or("");
            let target = edge["target"].as_str().unwrap_or("");

            if let (Some(&src_vid), Some(&dst_vid)) = (
                mappings.characters.get(source),
                mappings.characters.get(target),
            ) {
                let mut props = HashMap::new();
                if let Some(relationship) = edge["relationship"].as_str() {
                    props.insert(
                        "relationship".to_string(),
                        Value::String(relationship.to_string()),
                    );
                }
                if let Some(note) = edge["note"].as_str() {
                    props.insert("note".to_string(), Value::String(note.to_string()));
                }
                let _ = graph.add_edge(src_vid, dst_vid, "related_to", props);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Load Edges: works_for
    // -------------------------------------------------------------------------
    if let Some(edges) = data["edges"]["works_for"].as_array() {
        for edge in edges {
            let source = edge["source"].as_str().unwrap_or("");
            let target = edge["target"].as_str().unwrap_or("");

            // Target can be character or team
            let src_vid = mappings.characters.get(source).copied();
            let dst_vid = mappings
                .characters
                .get(target)
                .copied()
                .or_else(|| mappings.teams.get(target).copied());

            if let (Some(src), Some(dst)) = (src_vid, dst_vid) {
                let mut props = HashMap::new();
                if let Some(role) = edge["role"].as_str() {
                    props.insert("role".to_string(), Value::String(role.to_string()));
                }
                if let Some(period) = edge["period"].as_str() {
                    props.insert("period".to_string(), Value::String(period.to_string()));
                }
                let _ = graph.add_edge(src, dst, "works_for", props);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Load Edges: located_in
    // -------------------------------------------------------------------------
    if let Some(edges) = data["edges"]["located_in"].as_array() {
        for edge in edges {
            let source = edge["source"].as_str().unwrap_or("");
            let target = edge["target"].as_str().unwrap_or("");

            // Source can be team or character
            let src_vid = mappings
                .teams
                .get(source)
                .copied()
                .or_else(|| mappings.characters.get(source).copied());
            let dst_vid = mappings.locations.get(target).copied();

            if let (Some(src), Some(dst)) = (src_vid, dst_vid) {
                let _ = graph.add_edge(src, dst, "located_in", HashMap::new());
            }
        }
    }

    (graph, mappings)
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Get a property value from a vertex
fn get_property(snapshot: &GraphSnapshot, value: &Value, prop: &str) -> Option<String> {
    if let Some(vid) = value.as_vertex_id() {
        if let Some(vertex) = snapshot.get_vertex(vid) {
            if let Some(val) = vertex.properties.get(prop) {
                return Some(format_value(val));
            }
        }
    }
    None
}

/// Format a Value for display
fn format_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
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
        Value::Null => "null".to_string(),
        Value::Vertex(vid) => format!("v[{:?}]", vid),
        Value::Edge(eid) => format!("e[{:?}]", eid),
    }
}

/// Get the alias (superhero name) from a character vertex
fn get_alias(snapshot: &GraphSnapshot, value: &Value) -> String {
    get_property(snapshot, value, "alias").unwrap_or_else(|| format!("{:?}", value))
}

/// Get the name from a vertex
fn get_name(snapshot: &GraphSnapshot, value: &Value) -> String {
    get_property(snapshot, value, "name").unwrap_or_else(|| format!("{:?}", value))
}

/// Display a list of character results as aliases
fn display_aliases(snapshot: &GraphSnapshot, results: &[Value]) -> String {
    if results.is_empty() {
        return "(none)".to_string();
    }
    results
        .iter()
        .map(|v| get_alias(snapshot, v))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Display a list of vertex results as names
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

/// Get the best display name for a vertex (alias if character, otherwise name)
fn get_display_name(snapshot: &GraphSnapshot, value: &Value) -> String {
    // Try alias first (for characters), then fall back to name (for teams/locations)
    get_property(snapshot, value, "alias")
        .or_else(|| get_property(snapshot, value, "name"))
        .unwrap_or_else(|| format!("{:?}", value))
}

/// Display vertices with a specific property
fn display_with_prop(snapshot: &GraphSnapshot, results: &[Value], prop: &str) -> String {
    if results.is_empty() {
        return "(none)".to_string();
    }
    results
        .iter()
        .map(|v| {
            let name = get_display_name(snapshot, v);
            let prop_val = get_property(snapshot, v, prop).unwrap_or_else(|| "N/A".to_string());
            format!("{} ({})", name, prop_val)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn print_section(title: &str) {
    println!("\n{}", "=".repeat(70));
    println!("{}", title);
    println!("{}", "=".repeat(70));
}

fn print_query(description: &str) {
    println!("\n--- {} ---", description);
}

// =============================================================================
// Query Demonstrations
// =============================================================================

fn main() {
    println!("=== Marvel Universe Graph Database Example ===");
    println!("Loading data from examples/fixtures/marvel.json...\n");

    let (graph, _mappings) = load_marvel_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    // Report graph statistics
    let character_count = g.v().has_label("character").count();
    let team_count = g.v().has_label("team").count();
    let location_count = g.v().has_label("location").count();
    let edge_count = g.e().count();

    println!("Graph loaded successfully!");
    println!("  Characters: {}", character_count);
    println!("  Teams: {}", team_count);
    println!("  Locations: {}", location_count);
    println!("  Total edges: {}", edge_count);

    // =========================================================================
    // SECTION 1: Basic Queries
    // =========================================================================
    print_section("1. BASIC QUERIES");

    // Query 1: Find all heroes
    print_query("Find all heroes");
    let heroes = g
        .v()
        .has_label("character")
        .has_value("type", "hero")
        .to_list();
    println!(
        "Heroes ({}): {}",
        heroes.len(),
        display_aliases(&snapshot, &heroes)
    );

    // Query 2: Find all villains
    print_query("Find all villains");
    let villains = g
        .v()
        .has_label("character")
        .has_value("type", "villain")
        .to_list();
    println!(
        "Villains ({}): {}",
        villains.len(),
        display_aliases(&snapshot, &villains)
    );

    // Query 3: Find all antiheroes
    print_query("Find all antiheroes");
    let antiheroes = g
        .v()
        .has_label("character")
        .has_value("type", "antihero")
        .to_list();
    println!(
        "Antiheroes ({}): {}",
        antiheroes.len(),
        display_aliases(&snapshot, &antiheroes)
    );

    // Query 4: Count characters by type
    print_query("Count characters by type");
    let hero_count = g
        .v()
        .has_label("character")
        .has_value("type", "hero")
        .count();
    let villain_count = g
        .v()
        .has_label("character")
        .has_value("type", "villain")
        .count();
    let antihero_count = g
        .v()
        .has_label("character")
        .has_value("type", "antihero")
        .count();
    println!("  Heroes: {}", hero_count);
    println!("  Villains: {}", villain_count);
    println!("  Antiheroes: {}", antihero_count);

    // Query 5: Find hero teams
    print_query("Find hero teams");
    let hero_teams = g
        .v()
        .has_label("team")
        .has_value("type", "hero_team")
        .to_list();
    println!(
        "Hero teams ({}): {}",
        hero_teams.len(),
        display_names(&snapshot, &hero_teams)
    );

    // =========================================================================
    // SECTION 2: Navigation Queries
    // =========================================================================
    print_section("2. NAVIGATION QUERIES");

    // Query 6: Find Spider-Man's team memberships
    print_query("Find Spider-Man's team memberships");
    let spidey_teams = g
        .v()
        .has_value("alias", "Spider-Man")
        .out_labels(&["member_of"])
        .to_list();
    println!(
        "Spider-Man's teams: {}",
        display_names(&snapshot, &spidey_teams)
    );

    // Query 7: Find all Avengers members
    print_query("Find all Avengers members");
    let avengers_members = g
        .v()
        .has_value("name", "Avengers")
        .in_labels(&["member_of"])
        .to_list();
    println!(
        "Avengers ({}): {}",
        avengers_members.len(),
        display_aliases(&snapshot, &avengers_members)
    );

    // Query 8: Find Spider-Man's rivals
    print_query("Find Spider-Man's rivals");
    let spidey_rivals = g
        .v()
        .has_value("alias", "Spider-Man")
        .out_labels(&["rivals_with"])
        .to_list();
    println!(
        "Spider-Man's rivals: {}",
        display_aliases(&snapshot, &spidey_rivals)
    );

    // Query 9: Find characters who mentor others
    print_query("Find mentors (characters who mentor others)");
    let mentors = g
        .v()
        .has_label("character")
        .out_labels(&["mentors"])
        .in_labels(&["mentors"])
        .dedup()
        .to_list();
    println!("Mentors: {}", display_aliases(&snapshot, &mentors));

    // Query 10: Find Spider-Man's mentors
    print_query("Find who mentors Spider-Man");
    let spidey_mentors = g
        .v()
        .has_value("alias", "Spider-Man")
        .in_labels(&["mentors"])
        .to_list();
    println!(
        "Spider-Man's mentors: {}",
        display_aliases(&snapshot, &spidey_mentors)
    );

    // Query 11: Find Thanos's rivals
    print_query("Find Thanos's rivals");
    let thanos_rivals = g
        .v()
        .has_value("alias", "Thanos")
        .out_labels(&["rivals_with"])
        .to_list();
    println!(
        "Thanos's rivals: {}",
        display_names(&snapshot, &thanos_rivals)
    );

    // Query 12: Find members of the Sinister Six
    print_query("Find Sinister Six members");
    let sinister_six = g
        .v()
        .has_value("name", "Sinister Six")
        .in_labels(&["member_of"])
        .to_list();
    println!(
        "Sinister Six ({}): {}",
        sinister_six.len(),
        display_aliases(&snapshot, &sinister_six)
    );

    // Query 13: Find X-Men members
    print_query("Find X-Men members");
    let xmen = g
        .v()
        .has_value("name", "X-Men")
        .in_labels(&["member_of"])
        .to_list();
    println!(
        "X-Men ({}): {}",
        xmen.len(),
        display_aliases(&snapshot, &xmen)
    );

    // =========================================================================
    // SECTION 3: Predicate Queries
    // =========================================================================
    print_section("3. PREDICATE QUERIES (p:: module)");

    // Query 14: Find characters from the Golden Age (before 1960)
    print_query("Find Golden Age characters (first appearance before 1960)");
    let golden_age = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::lt(1960))
        .to_list();
    println!(
        "Golden Age characters ({}): {}",
        golden_age.len(),
        display_with_prop(&snapshot, &golden_age, "first_appearance")
    );

    // Query 15: Find Silver Age characters (1960-1970)
    print_query("Find Silver Age characters (1960-1970)");
    let silver_age = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::between(1960, 1971))
        .to_list();
    println!(
        "Silver Age characters ({}): {}",
        silver_age.len(),
        display_aliases(&snapshot, &silver_age)
    );

    // Query 16: Find characters appearing after 1990
    print_query("Find Modern Age characters (after 1990)");
    let modern = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::gte(1990))
        .to_list();
    println!(
        "Modern characters ({}): {}",
        modern.len(),
        display_with_prop(&snapshot, &modern, "first_appearance")
    );

    // Query 17: Find teams founded before 1965
    print_query("Find teams founded before 1965");
    let old_teams = g
        .v()
        .has_label("team")
        .has_where("founded", p::lt(1965))
        .to_list();
    println!(
        "Old teams: {}",
        display_with_prop(&snapshot, &old_teams, "founded")
    );

    // Query 18: Find characters based in specific locations
    print_query("Find characters based in New York City");
    let nyc_chars = g
        .v()
        .has_label("character")
        .has_value("base", "New York City")
        .to_list();
    println!(
        "NYC-based characters ({}): {}",
        nyc_chars.len(),
        display_aliases(&snapshot, &nyc_chars)
    );

    // Query 19: Find space-based characters
    print_query("Find space-based characters");
    let space_chars = g
        .v()
        .has_label("character")
        .has_value("base", "Space")
        .to_list();
    println!(
        "Space-based characters: {}",
        display_aliases(&snapshot, &space_chars)
    );

    // Query 20: Find characters NOT based in NYC
    print_query("Find characters NOT based in New York City");
    let not_nyc = g
        .v()
        .has_label("character")
        .has_where("base", p::neq("New York City"))
        .limit(15) // Limit for display
        .to_list();
    println!(
        "Non-NYC characters (first 15): {}",
        display_with_prop(&snapshot, &not_nyc, "base")
    );

    // =========================================================================
    // SECTION 4: Anonymous Traversal Queries
    // =========================================================================
    print_section("4. ANONYMOUS TRAVERSAL QUERIES (__:: module)");

    // Query 21: Find characters who are team members
    print_query("Find characters who are members of at least one team");
    let team_members = g
        .v()
        .has_label("character")
        .where_(__::out_labels(&["member_of"]))
        .to_list();
    println!(
        "Characters in teams ({}): {}",
        team_members.len(),
        display_aliases(
            &snapshot,
            &team_members.iter().take(20).cloned().collect::<Vec<_>>()
        )
    );
    if team_members.len() > 20 {
        println!("  ... and {} more", team_members.len() - 20);
    }

    // Query 22: Find lone wolf characters (not in any team)
    print_query("Find characters NOT in any team (lone wolves)");
    let lone_wolves = g
        .v()
        .has_label("character")
        .not(__::out_labels(&["member_of"]))
        .to_list();
    println!(
        "Lone wolves ({}): {}",
        lone_wolves.len(),
        display_aliases(&snapshot, &lone_wolves)
    );

    // Query 23: Find characters with both rivals AND allies
    print_query("Find characters who have both rivals AND allies");
    let complex_relations = g
        .v()
        .has_label("character")
        .and_(vec![
            __::out_labels(&["rivals_with"]),
            __::out_labels(&["allies_with"]),
        ])
        .to_list();
    println!(
        "Characters with both rivals and allies ({}): {}",
        complex_relations.len(),
        display_aliases(&snapshot, &complex_relations)
    );

    // Query 24: Find characters who have rivals OR work_for someone
    print_query("Find characters who have rivals OR work for someone");
    let rival_or_worker = g
        .v()
        .has_label("character")
        .or_(vec![
            __::out_labels(&["rivals_with"]),
            __::out_labels(&["works_for"]),
        ])
        .to_list();
    println!(
        "Characters with rivals or employers ({}): {}",
        rival_or_worker.len(),
        display_aliases(
            &snapshot,
            &rival_or_worker.iter().take(15).cloned().collect::<Vec<_>>()
        )
    );

    // Query 25: Find villains who are in teams
    print_query("Find villains who are team members");
    let villain_teams = g
        .v()
        .has_label("character")
        .has_value("type", "villain")
        .where_(__::out_labels(&["member_of"]))
        .to_list();
    println!(
        "Villains in teams ({}): {}",
        villain_teams.len(),
        display_aliases(&snapshot, &villain_teams)
    );

    // Query 26: Find heroes who mentor other characters
    print_query("Find heroes who mentor others");
    let hero_mentors = g
        .v()
        .has_label("character")
        .has_value("type", "hero")
        .where_(__::out_labels(&["mentors"]))
        .to_list();
    println!(
        "Hero mentors: {}",
        display_aliases(&snapshot, &hero_mentors)
    );

    // =========================================================================
    // SECTION 5: Branch Step Queries
    // =========================================================================
    print_section("5. BRANCH STEP QUERIES (union, coalesce, choose)");

    // Query 27: Union - Get both rivals AND allies of Spider-Man
    print_query("Get Spider-Man's rivals AND allies (using union)");
    let spidey_relationships = g
        .v()
        .has_value("alias", "Spider-Man")
        .union(vec![
            __::out_labels(&["rivals_with"]),
            __::out_labels(&["allies_with"]),
        ])
        .dedup()
        .to_list();
    println!(
        "Spider-Man's rivals and allies: {}",
        display_aliases(&snapshot, &spidey_relationships)
    );

    // Query 28: Union - Get both incoming and outgoing mentorship
    print_query("Get Iron Man's mentorship connections (both directions)");
    let ironman_mentorship = g
        .v()
        .has_value("alias", "Iron Man")
        .union(vec![
            __::out_labels(&["mentors"]), // Who Iron Man mentors
            __::in_labels(&["mentors"]),  // Who mentors Iron Man
        ])
        .dedup()
        .to_list();
    println!(
        "Iron Man's mentorship network: {}",
        display_aliases(&snapshot, &ironman_mentorship)
    );

    // Query 29: Coalesce - Get alias, or name if no alias
    print_query("Coalesce: Get alias (fallback to name)");
    // For teams which don't have alias, this will fall back to name
    let coalesce_result = g
        .v()
        .has_label("team")
        .limit(3)
        .coalesce(vec![__::values("alias"), __::values("name")])
        .to_list();
    println!("Team identifiers (coalesce): {:?}", coalesce_result);

    // Query 30: Choose - Different output based on character type
    print_query("Choose: If hero get 'allies', if villain get 'rivals'");
    let wolverine_connections = g
        .v()
        .has_value("alias", "Wolverine")
        .choose(
            __::has_value("type", "hero"),
            __::out_labels(&["allies_with"]),
            __::out_labels(&["rivals_with"]),
        )
        .to_list();
    println!(
        "Wolverine's connections (hero path -> allies): {}",
        display_aliases(&snapshot, &wolverine_connections)
    );

    let magneto_connections = g
        .v()
        .has_value("alias", "Magneto")
        .choose(
            __::has_value("type", "hero"),
            __::out_labels(&["allies_with"]),
            __::out_labels(&["rivals_with"]),
        )
        .to_list();
    println!(
        "Magneto's connections (villain path -> rivals): {}",
        display_names(&snapshot, &magneto_connections)
    );

    // Query 31: Optional - Try to get mentor, keep self if none
    print_query("Optional: Get mentor if exists, otherwise keep character");
    let optional_mentor = g
        .v()
        .has_value("alias", "Deadpool") // Deadpool has no mentor in data
        .optional(__::in_labels(&["mentors"]))
        .to_list();
    println!(
        "Deadpool with optional mentor: {}",
        display_aliases(&snapshot, &optional_mentor)
    );

    let optional_mentor2 = g
        .v()
        .has_value("alias", "Spider-Man") // Spider-Man has mentors
        .optional(__::in_labels(&["mentors"]))
        .to_list();
    println!(
        "Spider-Man with optional mentor: {}",
        display_aliases(&snapshot, &optional_mentor2)
    );

    // =========================================================================
    // SECTION 6: Repeat Step Queries
    // =========================================================================
    print_section("6. REPEAT STEP QUERIES (Relationship Chains)");

    // Query 32: Find multi-hop alliance chains
    print_query("Find friends-of-friends for Captain America (2 hops)");
    let cap_fof = g
        .v()
        .has_value("alias", "Captain America")
        .repeat(__::out_labels(&["allies_with"]))
        .times(2)
        .emit()
        .dedup()
        .to_list();
    println!(
        "Cap's alliance network ({}): {}",
        cap_fof.len(),
        display_aliases(&snapshot, &cap_fof)
    );

    // Query 33: Find all team members reachable from a character
    print_query("Find teammates of Spider-Man's teammates");
    let spidey_extended_team = g
        .v()
        .has_value("alias", "Spider-Man")
        .out_labels(&["member_of"]) // Get Spider-Man's teams
        .in_labels(&["member_of"]) // Get all members of those teams
        .dedup()
        .to_list();
    println!(
        "Spider-Man's extended team ({}): {}",
        spidey_extended_team.len(),
        display_aliases(
            &snapshot,
            &spidey_extended_team
                .iter()
                .take(15)
                .cloned()
                .collect::<Vec<_>>()
        )
    );
    if spidey_extended_team.len() > 15 {
        println!("  ... and {} more", spidey_extended_team.len() - 15);
    }

    // Query 34: Find mentorship chains
    print_query("Find mentorship chain: who mentored the mentors? (2 levels)");
    let mentor_chain = g
        .v()
        .has_value("alias", "Spider-Man")
        .in_labels(&["mentors"]) // Direct mentors
        .repeat(__::in_labels(&["mentors"]))
        .times(1)
        .emit()
        .emit_first() // Include the direct mentors too
        .dedup()
        .to_list();
    println!(
        "Spider-Man's mentorship chain: {}",
        display_aliases(&snapshot, &mentor_chain)
    );

    // Query 35: Find rivalry chains
    print_query("Find rivalry network from Spider-Man (2 hops)");
    let rivalry_network = g
        .v()
        .has_value("alias", "Spider-Man")
        .repeat(__::out_labels(&["rivals_with"]))
        .times(2)
        .emit()
        .dedup()
        .to_list();
    println!(
        "Spider-Man's rivalry network ({}): {}",
        rivalry_network.len(),
        display_aliases(&snapshot, &rivalry_network)
    );

    // Query 36: Explore Avengers membership depth
    print_query("Explore all characters connected to Avengers (via membership, 1 hop)");
    let avengers_network = g
        .v()
        .has_value("name", "Avengers")
        .repeat(__::in_labels(&["member_of"]).out_labels(&["member_of"]))
        .times(1)
        .emit_first()
        .dedup()
        .to_list();
    println!(
        "Avengers network ({}): {}",
        avengers_network.len(),
        display_names(
            &snapshot,
            &avengers_network
                .iter()
                .take(10)
                .cloned()
                .collect::<Vec<_>>()
        )
    );

    // =========================================================================
    // SECTION 7: Path Tracking Queries
    // =========================================================================
    print_section("7. PATH TRACKING QUERIES (as_, select, path)");

    // Query 37: Track character -> team with labeled positions
    print_query("Track character to team relationship with labels");
    let char_team_path = g
        .v()
        .has_value("alias", "Iron Man")
        .as_("hero")
        .out_labels(&["member_of"])
        .as_("team")
        .select(&["hero", "team"])
        .to_list();
    println!("Iron Man -> Team mappings:");
    for result in &char_team_path {
        if let Value::Map(map) = result {
            let hero = map
                .get("hero")
                .map(|v| get_alias(&snapshot, v))
                .unwrap_or_default();
            let team = map
                .get("team")
                .map(|v| get_name(&snapshot, v))
                .unwrap_or_default();
            println!("  {} -> {}", hero, team);
        }
    }

    // Query 38: Track rivalry with labels
    print_query("Track rivalry relationships");
    let rivalry_path = g
        .v()
        .has_value("alias", "Captain America")
        .as_("hero")
        .out_labels(&["rivals_with"])
        .as_("rival")
        .select(&["hero", "rival"])
        .to_list();
    println!("Captain America's rivalries:");
    for result in &rivalry_path {
        if let Value::Map(map) = result {
            let hero = map
                .get("hero")
                .map(|v| get_alias(&snapshot, v))
                .unwrap_or_default();
            let rival = map
                .get("rival")
                .map(|v| get_alias(&snapshot, v))
                .unwrap_or_default();
            println!("  {} vs {}", hero, rival);
        }
    }

    // Query 39: Full path from character -> team -> location
    print_query("Full path: Character -> Team -> Location");
    let full_path = g
        .v()
        .has_value("alias", "Black Panther")
        .with_path()
        .out_labels(&["member_of"])
        .out_labels(&["located_in"])
        .path()
        .to_list();
    println!("Black Panther -> Team -> Location paths:");
    for (i, path_value) in full_path.iter().enumerate() {
        if let Value::List(path) = path_value {
            let formatted: Vec<String> = path
                .iter()
                .map(|v| {
                    if v.as_vertex_id().is_some() {
                        // Try alias first, then name
                        get_property(&snapshot, v, "alias")
                            .or_else(|| get_property(&snapshot, v, "name"))
                            .unwrap_or_else(|| format!("{:?}", v))
                    } else {
                        format!("{:?}", v)
                    }
                })
                .collect();
            println!("  Path {}: {}", i + 1, formatted.join(" -> "));
        }
    }

    // Query 40: Track mentorship with path
    print_query("Mentorship paths: Who mentors whom? (Professor X's students)");
    let mentor_path = g
        .v()
        .has_value("alias", "Professor X")
        .with_path()
        .out_labels(&["mentors"]) // Professor X mentors many X-Men
        .path()
        .to_list();
    println!("Professor X mentorship paths:");
    for (i, path_value) in mentor_path.iter().enumerate() {
        if let Value::List(path) = path_value {
            let names: Vec<String> = path.iter().map(|v| get_alias(&snapshot, v)).collect();
            println!("  Path {}: {}", i + 1, names.join(" -> mentors -> "));
        }
    }

    // =========================================================================
    // SECTION 8: Complex Combined Queries
    // =========================================================================
    print_section("8. COMPLEX COMBINED QUERIES");

    // Query 41: Find Avengers who have rivals in the Sinister Six
    print_query("Find Avengers who have rivals in the Sinister Six");
    let avengers_vs_sinister = g
        .v()
        .has_value("name", "Avengers")
        .in_labels(&["member_of"]) // All Avengers members
        .where_(
            __::out_labels(&["rivals_with"])
                .out_labels(&["member_of"])
                .has_value("name", "Sinister Six"),
        )
        .to_list();
    println!(
        "Avengers vs Sinister Six: {}",
        display_aliases(&snapshot, &avengers_vs_sinister)
    );

    // Query 42: Find heroes who are both Avengers and X-Men members
    // Note: This returns 0 because in the fixture data, while some characters have
    // both teams in their "affiliation" property, the actual member_of edges only
    // connect them to one team. This demonstrates how graph queries depend on edge data.
    print_query("Find heroes who are members of BOTH Avengers and X-Men (via edges)");
    let dual_members = g
        .v()
        .has_label("character")
        .has_value("type", "hero")
        .and_(vec![
            __::out_labels(&["member_of"]).has_value("name", "Avengers"),
            __::out_labels(&["member_of"]).has_value("name", "X-Men"),
        ])
        .to_list();
    println!(
        "Dual Avengers/X-Men members ({}): {}",
        dual_members.len(),
        display_aliases(&snapshot, &dual_members)
    );
    println!("  (Note: fixture edges only show primary team membership)");

    // Query 43: Find villains who lead their teams
    print_query("Find villains who are team leaders");
    // We need to check edge properties, but the simple approach is to
    // find villains in teams - we can't easily filter by edge property yet
    // So let's find villains who are in villain teams
    let villain_team_members = g
        .v()
        .has_label("character")
        .has_value("type", "villain")
        .where_(__::out_labels(&["member_of"]).has_value("type", "villain_team"))
        .to_list();
    println!(
        "Villains in villain teams: {}",
        display_aliases(&snapshot, &villain_team_members)
    );

    // Query 44: Find characters with family relationships who are on opposing sides
    print_query("Find family members who might be on opposing sides");
    // Find characters with related_to edges
    let family_relations = g
        .v()
        .has_label("character")
        .where_(__::out_labels(&["related_to"]))
        .to_list();
    println!(
        "Characters with family relations: {}",
        display_aliases(&snapshot, &family_relations)
    );

    // Query 45: Find the most connected characters (have many relationship types)
    print_query("Find well-connected characters (members + allies + rivals)");
    let well_connected = g
        .v()
        .has_label("character")
        .and_(vec![
            __::out_labels(&["member_of"]),
            __::out_labels(&["allies_with"]),
            __::out_labels(&["rivals_with"]),
        ])
        .to_list();
    println!(
        "Well-connected characters (team + allies + rivals) ({}): {}",
        well_connected.len(),
        display_aliases(&snapshot, &well_connected)
    );

    // Query 46: Find Guardians of the Galaxy members
    print_query("Find Guardians of the Galaxy members");
    let guardians = g
        .v()
        .has_value("name", "Guardians of the Galaxy")
        .in_labels(&["member_of"])
        .to_list();
    println!(
        "Guardians ({}): {}",
        guardians.len(),
        display_aliases(&snapshot, &guardians)
    );

    // Query 47: Find characters who work for villains
    print_query("Find characters who work for other characters");
    let workers = g
        .v()
        .has_label("character")
        .where_(__::out_labels(&["works_for"]).has_label("character"))
        .to_list();
    println!(
        "Characters working for others: {}",
        display_aliases(&snapshot, &workers)
    );

    // Query 48: Find Wakanda-based characters
    print_query("Find characters based in Wakanda");
    let wakanda_chars = g
        .v()
        .has_label("character")
        .has_value("base", "Wakanda")
        .to_list();
    println!(
        "Wakanda-based: {}",
        display_aliases(&snapshot, &wakanda_chars)
    );

    // Query 49: Find sibling relationships
    print_query("Find characters with sibling relationships");
    // Thor and Loki are adopted_brothers, for example
    let siblings = g
        .v()
        .has_value("alias", "Thor")
        .out_labels(&["related_to"])
        .to_list();
    println!("Thor's family: {}", display_aliases(&snapshot, &siblings));

    // Query 50: Complex: Find heroes who could mentor villains' rivals
    print_query("Find hero mentors whose students rival villains");
    let hero_mentor_network = g
        .v()
        .has_label("character")
        .has_value("type", "hero")
        .where_(__::out_labels(&["mentors"])) // Must be a mentor
        .where_(
            __::out_labels(&["mentors"]) // Their students
                .out_labels(&["rivals_with"]) // Who have rivals
                .has_value("type", "villain"), // That are villains
        )
        .to_list();
    println!(
        "Hero mentors with students who rival villains: {}",
        display_aliases(&snapshot, &hero_mentor_network)
    );

    // =========================================================================
    // SECTION 9: Summary Statistics
    // =========================================================================
    print_section("9. SUMMARY STATISTICS");

    // Edge type counts
    let member_of_count = g.e().has_label("member_of").count();
    let rivals_count = g.e().has_label("rivals_with").count();
    let allies_count = g.e().has_label("allies_with").count();
    let mentors_count = g.e().has_label("mentors").count();
    let related_count = g.e().has_label("related_to").count();
    let works_for_count = g.e().has_label("works_for").count();
    let located_in_count = g.e().has_label("located_in").count();

    println!("Edge counts by type:");
    println!("  member_of: {}", member_of_count);
    println!("  rivals_with: {}", rivals_count);
    println!("  allies_with: {}", allies_count);
    println!("  mentors: {}", mentors_count);
    println!("  related_to: {}", related_count);
    println!("  works_for: {}", works_for_count);
    println!("  located_in: {}", located_in_count);

    // Team type counts
    println!("\nTeam counts by type:");
    let hero_team_count = g
        .v()
        .has_label("team")
        .has_value("type", "hero_team")
        .count();
    let villain_team_count = g
        .v()
        .has_label("team")
        .has_value("type", "villain_team")
        .count();
    let org_count = g
        .v()
        .has_label("team")
        .has_value("type", "organization")
        .count();
    println!("  Hero teams: {}", hero_team_count);
    println!("  Villain teams: {}", villain_team_count);
    println!("  Organizations: {}", org_count);

    // Location counts
    println!("\nLocations:");
    let locations = g.v().has_label("location").values("name").to_list();
    for loc in &locations {
        println!("  {}", format_value(loc));
    }

    // Character era distribution
    println!("\nCharacter era distribution:");
    let pre_1960 = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::lt(1960))
        .count();
    let era_60s = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::between(1960, 1970))
        .count();
    let era_70s = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::between(1970, 1980))
        .count();
    let era_80s = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::between(1980, 1990))
        .count();
    let era_90s_plus = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::gte(1990))
        .count();
    println!("  Pre-1960 (Golden Age): {}", pre_1960);
    println!("  1960-1969 (Silver Age): {}", era_60s);
    println!("  1970-1979 (Bronze Age): {}", era_70s);
    println!("  1980-1989 (Modern Age): {}", era_80s);
    println!("  1990+ (Contemporary): {}", era_90s_plus);

    println!("\n=== Example Complete ===");
}
