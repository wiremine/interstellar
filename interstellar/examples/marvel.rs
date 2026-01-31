//! # Marvel Universe Graph Example
//!
//! Comprehensive demonstration of Gremlin-style traversal features.
//!
//! This example loads the Marvel Universe dataset from a GraphSON 3.0 fixture
//! containing characters (heroes, villains, antiheroes), teams, and locations
//! with rich relationship data. It showcases all major Gremlin traversal features.
//!
//! Run with: `cargo run --example marvel`

use interstellar::graphson;
use interstellar::storage::{Graph, GraphSnapshot, GraphStorage};
use interstellar::traversal::{p, __};
use interstellar::value::Value;
use std::fs;

// =============================================================================
// Part 1: Data Loading (GraphSON Import)
// =============================================================================

/// Load the Marvel Universe graph from GraphSON 3.0 fixture.
///
/// The fixture includes both the graph data and schema definitions,
/// demonstrating real-world GraphSON import capabilities.
fn load_marvel_graph() -> Graph {
    let json_str = fs::read_to_string("examples/fixtures/marvel.graphson.json")
        .expect("Failed to read marvel.graphson.json");
    graphson::from_str(&json_str).expect("Failed to parse GraphSON")
}

// =============================================================================
// Helper Functions
// =============================================================================

fn get_prop(snapshot: &GraphSnapshot, value: &Value, prop: &str) -> String {
    if let Some(vid) = value.as_vertex_id() {
        if let Some(vertex) = snapshot.get_vertex(vid) {
            if let Some(val) = vertex.properties.get(prop) {
                return match val {
                    Value::String(s) => s.clone(),
                    Value::Int(n) => n.to_string(),
                    other => format!("{:?}", other),
                };
            }
        }
    }
    format!("{:?}", value)
}

fn get_alias(s: &GraphSnapshot, v: &Value) -> String {
    get_prop(s, v, "alias")
}

fn get_name(s: &GraphSnapshot, v: &Value) -> String {
    get_prop(s, v, "name")
}

fn display(s: &GraphSnapshot, results: &[Value], prop: &str) -> String {
    if results.is_empty() {
        return "(none)".to_string();
    }
    results
        .iter()
        .map(|v| get_prop(s, v, prop))
        .collect::<Vec<_>>()
        .join(", ")
}

fn print_section(title: &str) {
    println!("\n{}\n{}", "=".repeat(60), title);
}

fn print_query(desc: &str) {
    println!("\n--- {} ---", desc);
}

// =============================================================================
// Main - Query Demonstrations
// =============================================================================

fn main() {
    println!("=== Marvel Universe Graph - Gremlin Features Demo ===");
    let graph = load_marvel_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.gremlin();

    println!(
        "\nGraph: {} characters, {} teams, {} locations, {} edges",
        g.v().has_label("character").count(),
        g.v().has_label("team").count(),
        g.v().has_label("location").count(),
        g.e().count()
    );

    // =========================================================================
    // Part 2: Basic Queries - v(), has_label(), has_value(), count(), to_list()
    // =========================================================================
    print_section("Part 2: BASIC QUERIES");

    print_query("Heroes, Villains, Antiheroes");
    let heroes = g
        .v()
        .has_label("character")
        .has_value("type", "hero")
        .count();
    let villains = g
        .v()
        .has_label("character")
        .has_value("type", "villain")
        .count();
    let antiheroes = g
        .v()
        .has_label("character")
        .has_value("type", "antihero")
        .count();
    println!(
        "Heroes: {}, Villains: {}, Antiheroes: {}",
        heroes, villains, antiheroes
    );

    print_query("List hero teams");
    let teams = g
        .v()
        .has_label("team")
        .has_value("type", "hero_team")
        .to_list();
    println!("Hero teams: {}", display(&snapshot, &teams, "name"));

    // =========================================================================
    // Part 3: Navigation - out_labels(), in_labels(), dedup()
    // =========================================================================
    print_section("Part 3: NAVIGATION");

    print_query("Spider-Man's team memberships");
    let spidey_teams = g
        .v()
        .has_value("alias", "Spider-Man")
        .out_labels(&["member_of"])
        .to_list();
    println!("Teams: {}", display(&snapshot, &spidey_teams, "name"));

    print_query("Avengers members");
    let avengers = g
        .v()
        .has_value("name", "Avengers")
        .in_labels(&["member_of"])
        .to_list();
    println!(
        "Avengers ({}): {}",
        avengers.len(),
        display(&snapshot, &avengers, "alias")
    );

    print_query("Mentors (characters who mentor others)");
    let mentors = g
        .v()
        .has_label("character")
        .out_labels(&["mentors"])
        .in_labels(&["mentors"])
        .dedup()
        .to_list();
    println!("Mentors: {}", display(&snapshot, &mentors, "alias"));

    // =========================================================================
    // Part 4: Predicates - p::lt(), p::gt(), p::gte(), p::between(), p::neq()
    // =========================================================================
    print_section("Part 4: PREDICATES (p:: module)");

    print_query("Golden Age characters (before 1960)");
    let golden = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::lt(1960))
        .to_list();
    println!(
        "Golden Age ({}): {}",
        golden.len(),
        display(&snapshot, &golden, "alias")
    );

    print_query("Silver Age characters (1960-1970)");
    let silver = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::between(1960, 1970))
        .to_list();
    println!(
        "Silver Age ({}): {}",
        silver.len(),
        display(&snapshot, &silver, "alias")
    );

    print_query("Modern characters (1990+)");
    let modern = g
        .v()
        .has_label("character")
        .has_where("first_appearance", p::gte(1990))
        .to_list();
    println!(
        "Modern ({}): {}",
        modern.len(),
        display(&snapshot, &modern, "alias")
    );

    print_query("Characters NOT based in NYC");
    let not_nyc = g
        .v()
        .has_label("character")
        .has_where("base", p::neq("New York City"))
        .limit(10)
        .to_list();
    println!(
        "Non-NYC (first 10): {}",
        display(&snapshot, &not_nyc, "alias")
    );

    // =========================================================================
    // Part 5: Anonymous Traversals - where_(), not(), and_(), or_()
    // =========================================================================
    print_section("Part 5: ANONYMOUS TRAVERSALS (__. module)");

    print_query("Characters in at least one team");
    let in_teams = g
        .v()
        .has_label("character")
        .where_(__.out_labels(&["member_of"]))
        .count();
    println!("Characters in teams: {}", in_teams);

    print_query("Lone wolves (not in any team)");
    let lone = g
        .v()
        .has_label("character")
        .not(__.out_labels(&["member_of"]))
        .to_list();
    println!(
        "Lone wolves ({}): {}",
        lone.len(),
        display(&snapshot, &lone, "alias")
    );

    print_query("Characters with BOTH rivals AND allies");
    let complex = g
        .v()
        .has_label("character")
        .and_(vec![
            __.out_labels(&["rivals_with"]),
            __.out_labels(&["allies_with"]),
        ])
        .to_list();
    println!(
        "With rivals+allies ({}): {}",
        complex.len(),
        display(&snapshot, &complex, "alias")
    );

    print_query("Characters with rivals OR works_for");
    let either = g
        .v()
        .has_label("character")
        .or_(vec![
            __.out_labels(&["rivals_with"]),
            __.out_labels(&["works_for"]),
        ])
        .count();
    println!("With rivals or employer: {}", either);

    // =========================================================================
    // Part 6: Branch Steps - union(), coalesce(), choose(), optional()
    // =========================================================================
    print_section("Part 6: BRANCH STEPS");

    print_query("Spider-Man's rivals AND allies (union)");
    let both = g
        .v()
        .has_value("alias", "Spider-Man")
        .union(vec![
            __.out_labels(&["rivals_with"]),
            __.out_labels(&["allies_with"]),
        ])
        .dedup()
        .to_list();
    println!("Rivals+Allies: {}", display(&snapshot, &both, "alias"));

    print_query("Iron Man's mentorship network (union in/out)");
    let mentorship = g
        .v()
        .has_value("alias", "Iron Man")
        .union(vec![
            __.out_labels(&["mentors"]),
            __.in_labels(&["mentors"]),
        ])
        .dedup()
        .to_list();
    println!(
        "Mentorship network: {}",
        display(&snapshot, &mentorship, "alias")
    );

    print_query("Coalesce: alias or name fallback");
    let coalesce = g
        .v()
        .has_label("team")
        .limit(3)
        .coalesce(vec![__.values("alias"), __.values("name")])
        .to_list();
    println!("Team identifiers: {:?}", coalesce);

    print_query("Choose: hero->allies, villain->rivals");
    let wolverine = g
        .v()
        .has_value("alias", "Wolverine")
        .choose(
            __.has_value("type", "hero"),
            __.out_labels(&["allies_with"]),
            __.out_labels(&["rivals_with"]),
        )
        .to_list();
    println!(
        "Wolverine (hero): {}",
        display(&snapshot, &wolverine, "alias")
    );

    let magneto = g
        .v()
        .has_value("alias", "Magneto")
        .choose(
            __.has_value("type", "hero"),
            __.out_labels(&["allies_with"]),
            __.out_labels(&["rivals_with"]),
        )
        .to_list();
    println!(
        "Magneto (villain): {}",
        display(&snapshot, &magneto, "name")
    );

    print_query("Optional: mentor if exists, else self");
    let optional = g
        .v()
        .has_value("alias", "Deadpool")
        .optional(__.in_labels(&["mentors"]))
        .to_list();
    println!(
        "Deadpool with optional mentor: {}",
        display(&snapshot, &optional, "alias")
    );

    // =========================================================================
    // Part 7: Repeat Steps - repeat(), times(), emit(), emit_first()
    // =========================================================================
    print_section("Part 7: REPEAT STEPS");

    print_query("Captain America's alliance network (2 hops)");
    let alliance = g
        .v()
        .has_value("alias", "Captain America")
        .repeat(__.out_labels(&["allies_with"]))
        .times(2)
        .emit()
        .dedup()
        .to_list();
    println!(
        "Alliance network ({}): {}",
        alliance.len(),
        display(&snapshot, &alliance, "alias")
    );

    print_query("Spider-Man's extended team (teammates of teammates)");
    let extended = g
        .v()
        .has_value("alias", "Spider-Man")
        .out_labels(&["member_of"])
        .in_labels(&["member_of"])
        .dedup()
        .to_list();
    println!(
        "Extended team ({}): {}",
        extended.len(),
        display(
            &snapshot,
            &extended.iter().take(10).cloned().collect::<Vec<_>>(),
            "alias"
        )
    );

    print_query("Rivalry network from Spider-Man (2 hops)");
    let rivals = g
        .v()
        .has_value("alias", "Spider-Man")
        .repeat(__.out_labels(&["rivals_with"]))
        .times(2)
        .emit()
        .dedup()
        .to_list();
    println!(
        "Rivalry network ({}): {}",
        rivals.len(),
        display(&snapshot, &rivals, "alias")
    );

    // =========================================================================
    // Part 8: Path Tracking - as_(), select(), with_path(), path()
    // =========================================================================
    print_section("Part 8: PATH TRACKING");

    print_query("Iron Man -> Team mappings (as/select)");
    let paths = g
        .v()
        .has_value("alias", "Iron Man")
        .as_("hero")
        .out_labels(&["member_of"])
        .as_("team")
        .select(&["hero", "team"])
        .to_list();
    for result in &paths {
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

    print_query("Black Panther -> Team -> Location (full path)");
    let full = g
        .v()
        .has_value("alias", "Black Panther")
        .with_path()
        .out_labels(&["member_of"])
        .out_labels(&["located_in"])
        .path()
        .to_list();
    for (i, p) in full.iter().enumerate() {
        if let Value::List(path) = p {
            let names: Vec<String> = path
                .iter()
                .map(|v| {
                    get_prop(&snapshot, v, "alias")
                        .replace(&format!("{:?}", v), &get_prop(&snapshot, v, "name"))
                })
                .collect();
            println!("  Path {}: {}", i + 1, names.join(" -> "));
        }
    }

    print_query("Professor X mentorship paths");
    let mentor_paths = g
        .v()
        .has_value("alias", "Professor X")
        .with_path()
        .out_labels(&["mentors"])
        .path()
        .to_list();
    for (i, p) in mentor_paths.iter().take(5).enumerate() {
        if let Value::List(path) = p {
            let names: Vec<String> = path.iter().map(|v| get_alias(&snapshot, v)).collect();
            println!("  Path {}: {}", i + 1, names.join(" -> mentors -> "));
        }
    }

    // =========================================================================
    // Part 9: Summary Statistics
    // =========================================================================
    print_section("Part 9: SUMMARY STATISTICS");

    println!("Edge counts:");
    for label in [
        "member_of",
        "rivals_with",
        "allies_with",
        "mentors",
        "related_to",
        "works_for",
        "located_in",
    ] {
        println!("  {}: {}", label, g.e().has_label(label).count());
    }

    println!("\nTeam types:");
    for team_type in ["hero_team", "villain_team", "organization"] {
        println!(
            "  {}: {}",
            team_type,
            g.v().has_label("team").has_value("type", team_type).count()
        );
    }

    println!("\nEra distribution:");
    let eras = [
        ("Pre-1960", 0, 1960),
        ("1960s", 1960, 1970),
        ("1970s", 1970, 1980),
        ("1980s", 1980, 1990),
        ("1990+", 1990, 2100),
    ];
    for (name, start, end) in eras {
        let count = g
            .v()
            .has_label("character")
            .has_where("first_appearance", p::between(start, end))
            .count();
        println!("  {}: {}", name, count);
    }

    println!("\n=== Example Complete ===");
}
