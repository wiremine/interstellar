//! NBA Graph Example
//!
//! This example demonstrates graph traversal queries on an NBA dataset
//! containing 15 legendary players, 19 teams, and their relationships.
//!
//! The dataset includes:
//! - Player vertices with properties: name, position, height_inches, career stats (flattened),
//!   mvp_awards (list), finals_mvp_awards (list), all_star_selections
//! - Team vertices with properties: name, city, state, arena, founded, conference, division,
//!   championships (list), conference_titles (list), retired_numbers (list)
//! - Relationship edges: played_for (with start_year, end_year, role), won_championship_with (with years)
//!
//! This example showcases:
//! - Data loading from JSON fixtures
//! - Basic traversal with filtering (Phase 3)
//! - Navigation steps: out(), in_(), both(), other_v() (Phase 3 & 7)
//! - Predicate system: p::eq, p::gt, p::gte, p::within, p::containing (Phase 4)
//! - Filter steps: has_not(), is_(), is_eq() (Phase 7)
//! - Anonymous traversals: __::out(), __::has_label() (Phase 4)
//! - Branch steps: union(), coalesce(), choose(), optional() (Phase 5)
//! - Repeat steps for multi-hop queries (Phase 5)
//! - Path tracking with as_() and select() (Phase 3)
//! - Transform steps: value_map(), element_map(), unfold(), order(), mean() (Phase 7)
//! - Aggregation steps: group(), group_count() (Phase 7)
//!
//! Run with: `cargo run --example nba`

use intersteller::graph::Graph;
use intersteller::storage::{GraphStorage, InMemoryGraph};
use intersteller::traversal::{p, __};
use intersteller::value::{Value, VertexId};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

// =============================================================================
// Data Loading
// =============================================================================

/// ID mappings for looking up vertices by their JSON IDs
struct IdMappings {
    players: HashMap<String, VertexId>,
    teams: HashMap<String, VertexId>,
}

/// Load the NBA JSON fixture and build the graph.
fn load_nba_graph() -> (Graph, Arc<InMemoryGraph>, IdMappings) {
    let json_str =
        fs::read_to_string("examples/fixtures/nba.json").expect("Failed to read nba.json");
    let data: JsonValue = serde_json::from_str(&json_str).expect("Failed to parse JSON");

    let mut storage = InMemoryGraph::new();
    let mut mappings = IdMappings {
        players: HashMap::new(),
        teams: HashMap::new(),
    };

    // -------------------------------------------------------------------------
    // Load Teams first (so we can reference them when loading player relationships)
    // -------------------------------------------------------------------------
    if let Some(teams) = data["teams"].as_array() {
        for team in teams {
            let json_id = team["id"].as_str().unwrap_or("unknown");

            let mut props = HashMap::new();

            // String properties
            if let Some(v) = team["name"].as_str() {
                props.insert("name".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = team["city"].as_str() {
                props.insert("city".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = team["state"].as_str() {
                props.insert("state".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = team["arena"].as_str() {
                props.insert("arena".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = team["conference"].as_str() {
                props.insert("conference".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = team["division"].as_str() {
                props.insert("division".to_string(), Value::String(v.to_string()));
            }

            // Integer properties
            if let Some(v) = team["founded"].as_i64() {
                props.insert("founded".to_string(), Value::Int(v));
            }

            // Boolean properties
            if let Some(v) = team["defunct"].as_bool() {
                props.insert("defunct".to_string(), Value::Bool(v));
            }
            if let Some(v) = team["defunct_year"].as_i64() {
                props.insert("defunct_year".to_string(), Value::Int(v));
            }

            // List properties - championships
            if let Some(championships) = team["championships"].as_array() {
                let values: Vec<Value> = championships
                    .iter()
                    .filter_map(|y| y.as_i64().map(Value::Int))
                    .collect();
                props.insert("championships".to_string(), Value::List(values.clone()));
                props.insert(
                    "championship_count".to_string(),
                    Value::Int(values.len() as i64),
                );
            }

            // List properties - conference titles
            if let Some(conf_titles) = team["conference_titles"].as_array() {
                let values: Vec<Value> = conf_titles
                    .iter()
                    .filter_map(|y| y.as_i64().map(Value::Int))
                    .collect();
                props.insert("conference_titles".to_string(), Value::List(values.clone()));
                props.insert(
                    "conference_title_count".to_string(),
                    Value::Int(values.len() as i64),
                );
            }

            // List properties - retired numbers
            if let Some(retired) = team["retired_numbers"].as_array() {
                let values: Vec<Value> = retired
                    .iter()
                    .filter_map(|n| n.as_i64().map(Value::Int))
                    .collect();
                props.insert("retired_numbers".to_string(), Value::List(values));
            }

            // Store original JSON ID
            props.insert("json_id".to_string(), Value::String(json_id.to_string()));

            let vid = storage.add_vertex("team", props);
            mappings.teams.insert(json_id.to_string(), vid);
        }
    }

    // -------------------------------------------------------------------------
    // Load Players
    // -------------------------------------------------------------------------
    if let Some(players) = data["players"].as_array() {
        for player in players {
            let json_id = player["id"].as_str().unwrap_or("unknown");

            let mut props = HashMap::new();

            // String properties
            if let Some(v) = player["name"].as_str() {
                props.insert("name".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = player["position"].as_str() {
                props.insert("position".to_string(), Value::String(v.to_string()));
            }

            // Integer properties
            if let Some(v) = player["height_inches"].as_i64() {
                props.insert("height_inches".to_string(), Value::Int(v));
            }
            if let Some(v) = player["all_star_selections"].as_i64() {
                props.insert("all_star_selections".to_string(), Value::Int(v));
            }

            // Flatten career_stats into top-level properties
            if let Some(stats) = player["career_stats"].as_object() {
                if let Some(v) = stats.get("points_per_game").and_then(|v| v.as_f64()) {
                    props.insert("points_per_game".to_string(), Value::Float(v));
                }
                if let Some(v) = stats.get("rebounds_per_game").and_then(|v| v.as_f64()) {
                    props.insert("rebounds_per_game".to_string(), Value::Float(v));
                }
                if let Some(v) = stats.get("assists_per_game").and_then(|v| v.as_f64()) {
                    props.insert("assists_per_game".to_string(), Value::Float(v));
                }
                if let Some(v) = stats.get("steals_per_game").and_then(|v| v.as_f64()) {
                    props.insert("steals_per_game".to_string(), Value::Float(v));
                }
                if let Some(v) = stats.get("blocks_per_game").and_then(|v| v.as_f64()) {
                    props.insert("blocks_per_game".to_string(), Value::Float(v));
                }
                if let Some(v) = stats.get("field_goal_pct").and_then(|v| v.as_f64()) {
                    props.insert("field_goal_pct".to_string(), Value::Float(v));
                }
                if let Some(v) = stats.get("three_point_pct").and_then(|v| v.as_f64()) {
                    props.insert("three_point_pct".to_string(), Value::Float(v));
                }
                if let Some(v) = stats.get("free_throw_pct").and_then(|v| v.as_f64()) {
                    props.insert("free_throw_pct".to_string(), Value::Float(v));
                }
            }

            // List properties - MVP awards
            if let Some(mvps) = player["mvp_awards"].as_array() {
                let values: Vec<Value> = mvps
                    .iter()
                    .filter_map(|y| y.as_i64().map(Value::Int))
                    .collect();
                props.insert("mvp_count".to_string(), Value::Int(values.len() as i64));
                props.insert("mvp_awards".to_string(), Value::List(values));
            }

            // List properties - Finals MVP awards
            if let Some(fmvps) = player["finals_mvp_awards"].as_array() {
                let values: Vec<Value> = fmvps
                    .iter()
                    .filter_map(|y| y.as_i64().map(Value::Int))
                    .collect();
                props.insert(
                    "finals_mvp_count".to_string(),
                    Value::Int(values.len() as i64),
                );
                props.insert("finals_mvp_awards".to_string(), Value::List(values));
            }

            // Store original JSON ID
            props.insert("json_id".to_string(), Value::String(json_id.to_string()));

            let vid = storage.add_vertex("player", props);
            mappings.players.insert(json_id.to_string(), vid);
        }
    }

    // -------------------------------------------------------------------------
    // Load Edges: played_for
    // -------------------------------------------------------------------------
    if let Some(edges) = data["relationships"]["played_for"].as_array() {
        for edge in edges {
            let player_id = edge["player_id"].as_str().unwrap_or("");
            let team_id = edge["team_id"].as_str().unwrap_or("");

            if let (Some(&player_vid), Some(&team_vid)) =
                (mappings.players.get(player_id), mappings.teams.get(team_id))
            {
                let mut props = HashMap::new();
                if let Some(start) = edge["start_year"].as_i64() {
                    props.insert("start_year".to_string(), Value::Int(start));
                }
                if let Some(end) = edge["end_year"].as_i64() {
                    props.insert("end_year".to_string(), Value::Int(end));
                }
                if let Some(role) = edge["role"].as_str() {
                    props.insert("role".to_string(), Value::String(role.to_string()));
                }
                let _ = storage.add_edge(player_vid, team_vid, "played_for", props);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Load Edges: won_championship_with
    // -------------------------------------------------------------------------
    if let Some(edges) = data["relationships"]["won_championship_with"].as_array() {
        for edge in edges {
            let player_id = edge["player_id"].as_str().unwrap_or("");
            let team_id = edge["team_id"].as_str().unwrap_or("");

            if let (Some(&player_vid), Some(&team_vid)) =
                (mappings.players.get(player_id), mappings.teams.get(team_id))
            {
                let mut props = HashMap::new();
                if let Some(years) = edge["years"].as_array() {
                    let values: Vec<Value> = years
                        .iter()
                        .filter_map(|y| y.as_i64().map(Value::Int))
                        .collect();
                    props.insert("ring_count".to_string(), Value::Int(values.len() as i64));
                    props.insert("years".to_string(), Value::List(values));
                }
                let _ = storage.add_edge(player_vid, team_vid, "won_championship_with", props);
            }
        }
    }

    let storage = Arc::new(storage);
    let graph = Graph::new(storage.clone());

    (graph, storage, mappings)
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Get a property value from a vertex
fn get_property(storage: &Arc<InMemoryGraph>, value: &Value, prop: &str) -> Option<String> {
    if let Some(vid) = value.as_vertex_id() {
        if let Some(vertex) = storage.get_vertex(vid) {
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
        Value::Float(f) => format!("{:.1}", f),
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

/// Get the name from a vertex
fn get_name(storage: &Arc<InMemoryGraph>, value: &Value) -> String {
    get_property(storage, value, "name").unwrap_or_else(|| format!("{:?}", value))
}

/// Display a list of vertex results as names
fn display_names(storage: &Arc<InMemoryGraph>, results: &[Value]) -> String {
    if results.is_empty() {
        return "(none)".to_string();
    }
    results
        .iter()
        .map(|v| get_name(storage, v))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Display vertices with a specific property
fn display_with_prop(storage: &Arc<InMemoryGraph>, results: &[Value], prop: &str) -> String {
    if results.is_empty() {
        return "(none)".to_string();
    }
    results
        .iter()
        .map(|v| {
            let name = get_name(storage, v);
            let prop_val = get_property(storage, v, prop).unwrap_or_else(|| "N/A".to_string());
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
    println!("=== NBA Graph Database Example ===");
    println!("Loading data from examples/fixtures/nba.json...\n");

    let (graph, storage, _mappings) = load_nba_graph();
    let snapshot = graph.snapshot();
    let g = snapshot.traversal();

    // Report graph statistics
    let player_count = g.v().has_label("player").count();
    let team_count = g.v().has_label("team").count();
    let edge_count = g.e().count();

    println!("Graph loaded successfully!");
    println!("  Players: {}", player_count);
    println!("  Teams: {}", team_count);
    println!("  Total edges: {}", edge_count);

    // =========================================================================
    // SECTION 1: Basic Queries
    // =========================================================================
    print_section("1. BASIC QUERIES");

    // Query 1: Find all players
    print_query("Find all players");
    let players = g.v().has_label("player").to_list();
    println!(
        "Players ({}): {}",
        players.len(),
        display_names(&storage, &players)
    );

    // Query 2: Find all teams
    print_query("Find all teams");
    let teams = g.v().has_label("team").to_list();
    println!(
        "Teams ({}): {}",
        teams.len(),
        display_names(&storage, &teams)
    );

    // Query 3: Find Point Guards
    print_query("Find Point Guards");
    let point_guards = g
        .v()
        .has_label("player")
        .has_value("position", "Point Guard")
        .to_list();
    println!("Point Guards: {}", display_names(&storage, &point_guards));

    // Query 4: Find Centers
    print_query("Find Centers");
    let centers = g
        .v()
        .has_label("player")
        .has_value("position", "Center")
        .to_list();
    println!("Centers: {}", display_names(&storage, &centers));

    // Query 5: Find Eastern Conference teams
    print_query("Find Eastern Conference teams");
    let eastern = g
        .v()
        .has_label("team")
        .has_value("conference", "Eastern")
        .to_list();
    println!(
        "Eastern Conference ({}): {}",
        eastern.len(),
        display_names(&storage, &eastern)
    );

    // Query 6: Find Western Conference teams
    print_query("Find Western Conference teams");
    let western = g
        .v()
        .has_label("team")
        .has_value("conference", "Western")
        .to_list();
    println!(
        "Western Conference ({}): {}",
        western.len(),
        display_names(&storage, &western)
    );

    // =========================================================================
    // SECTION 2: Navigation Queries
    // =========================================================================
    print_section("2. NAVIGATION QUERIES");

    // Query 7: Find teams Michael Jordan played for
    print_query("Find teams Michael Jordan played for");
    let mj_teams = g
        .v()
        .has_value("name", "Michael Jordan")
        .out_labels(&["played_for"])
        .to_list();
    println!(
        "Michael Jordan's teams: {}",
        display_names(&storage, &mj_teams)
    );

    // Query 8: Find teams LeBron James played for
    print_query("Find teams LeBron James played for");
    let lebron_teams = g
        .v()
        .has_value("name", "LeBron James")
        .out_labels(&["played_for"])
        .to_list();
    println!(
        "LeBron James's teams: {}",
        display_names(&storage, &lebron_teams)
    );

    // Query 9: Find players who played for the Lakers
    print_query("Find players who played for the Lakers");
    let lakers_players = g
        .v()
        .has_value("name", "Los Angeles Lakers")
        .in_labels(&["played_for"])
        .dedup()
        .to_list();
    println!(
        "Lakers players ({}): {}",
        lakers_players.len(),
        display_names(&storage, &lakers_players)
    );

    // Query 10: Find players who played for the Bulls
    print_query("Find players who played for the Bulls");
    let bulls_players = g
        .v()
        .has_value("name", "Chicago Bulls")
        .in_labels(&["played_for"])
        .dedup()
        .to_list();
    println!("Bulls players: {}", display_names(&storage, &bulls_players));

    // Query 11: Find championship teams for Tim Duncan
    print_query("Find championship teams for Tim Duncan");
    let duncan_chips = g
        .v()
        .has_value("name", "Tim Duncan")
        .out_labels(&["won_championship_with"])
        .to_list();
    println!(
        "Tim Duncan's championship teams: {}",
        display_names(&storage, &duncan_chips)
    );

    // Query 12: Find players who won championships with the Lakers
    print_query("Find players who won championships with the Lakers");
    let lakers_champs = g
        .v()
        .has_value("name", "Los Angeles Lakers")
        .in_labels(&["won_championship_with"])
        .dedup()
        .to_list();
    println!(
        "Lakers champions ({}): {}",
        lakers_champs.len(),
        display_names(&storage, &lakers_champs)
    );

    // Query 12b: Using other_v() to navigate from edges to opposite vertex (Phase 7)
    print_query("Navigate from player through edges using other_v()");
    let kobe_teams_via_edge = g
        .v()
        .has_value("name", "Kobe Bryant")
        .out_e_labels(&["played_for"]) // Get edges
        .other_v() // Navigate to the other vertex (teams)
        .to_list();
    println!(
        "Kobe's teams via other_v(): {}",
        display_names(&storage, &kobe_teams_via_edge)
    );

    // =========================================================================
    // SECTION 3: Predicate Queries
    // =========================================================================
    print_section("3. PREDICATE QUERIES (p:: module)");

    // Query 13: Find players who averaged 25+ PPG
    print_query("Find players who averaged 25+ PPG");
    let high_scorers = g
        .v()
        .has_label("player")
        .has_where("points_per_game", p::gte(25.0))
        .to_list();
    println!(
        "25+ PPG scorers ({}): {}",
        high_scorers.len(),
        display_with_prop(&storage, &high_scorers, "points_per_game")
    );

    // Query 14: Find elite scorers (27+ PPG)
    print_query("Find elite scorers (27+ PPG)");
    let elite_scorers = g
        .v()
        .has_label("player")
        .has_where("points_per_game", p::gte(27.0))
        .to_list();
    println!(
        "Elite scorers: {}",
        display_with_prop(&storage, &elite_scorers, "points_per_game")
    );

    // Query 15: Find players with 10+ rebounds per game
    print_query("Find players who averaged 10+ rebounds per game");
    let rebounders = g
        .v()
        .has_label("player")
        .has_where("rebounds_per_game", p::gte(10.0))
        .to_list();
    println!(
        "10+ RPG players: {}",
        display_with_prop(&storage, &rebounders, "rebounds_per_game")
    );

    // Query 16: Find players with 3+ MVPs
    print_query("Find players with 3 or more MVP awards");
    let multi_mvps = g
        .v()
        .has_label("player")
        .has_where("mvp_count", p::gte(3))
        .to_list();
    println!(
        "3+ MVP winners: {}",
        display_with_prop(&storage, &multi_mvps, "mvp_count")
    );

    // Query 17: Find teams founded before 1970
    print_query("Find teams founded before 1970");
    let old_teams = g
        .v()
        .has_label("team")
        .has_where("founded", p::lt(1970))
        .to_list();
    println!(
        "Pre-1970 teams ({}): {}",
        old_teams.len(),
        display_with_prop(&storage, &old_teams, "founded")
    );

    // Query 18: Find teams founded between 1980 and 2000
    print_query("Find teams founded between 1980 and 2000");
    let expansion_era = g
        .v()
        .has_label("team")
        .has_where("founded", p::between(1980, 2001))
        .to_list();
    println!(
        "1980-2000 teams: {}",
        display_with_prop(&storage, &expansion_era, "founded")
    );

    // Query 19: Find guards (Point Guard or Shooting Guard)
    print_query("Find all guards (Point Guard or Shooting Guard)");
    let guards = g
        .v()
        .has_label("player")
        .has_where("position", p::within(["Point Guard", "Shooting Guard"]))
        .to_list();
    println!(
        "Guards ({}): {}",
        guards.len(),
        display_with_prop(&storage, &guards, "position")
    );

    // Query 20: Find teams with 5+ championships
    print_query("Find dynasty teams (5+ championships)");
    let dynasties = g
        .v()
        .has_label("team")
        .has_where("championship_count", p::gte(5))
        .to_list();
    println!(
        "Dynasty teams: {}",
        display_with_prop(&storage, &dynasties, "championship_count")
    );

    // Query 21: Find tall players (7 feet or taller = 84+ inches)
    print_query("Find players 7 feet or taller");
    let tall_players = g
        .v()
        .has_label("player")
        .has_where("height_inches", p::gte(84))
        .to_list();
    println!(
        "7-footers: {}",
        display_with_prop(&storage, &tall_players, "height_inches")
    );

    // Query 21b: Using is_() to filter extracted values (Phase 7)
    print_query("Filter PPG values using is_() with predicate");
    let elite_ppg = g
        .v()
        .has_label("player")
        .values("points_per_game")
        .is_(p::gte(27.0)) // Filter values >= 27.0
        .to_list();
    println!(
        "Elite PPG values (>= 27.0): {:?}",
        elite_ppg
            .iter()
            .filter_map(|v| v.as_f64())
            .collect::<Vec<_>>()
    );

    // Query 21c: Using is_eq() to filter to exact values (Phase 7)
    print_query("Filter to exact position using is_eq()");
    let center_positions = g
        .v()
        .has_label("player")
        .values("position")
        .is_eq("Center") // Filter to exact value
        .count();
    println!("Number of Centers (via is_eq): {}", center_positions);

    // =========================================================================
    // SECTION 4: Anonymous Traversal Queries
    // =========================================================================
    print_section("4. ANONYMOUS TRAVERSAL QUERIES (__:: module)");

    // Query 22: Find championship winners
    print_query("Find players who have won championships");
    let champ_winners = g
        .v()
        .has_label("player")
        .where_(__::out_labels(&["won_championship_with"]))
        .to_list();
    println!(
        "Championship winners ({}): {}",
        champ_winners.len(),
        display_names(&storage, &champ_winners)
    );

    // Query 23: Find players without championships
    print_query("Find players without championships");
    let ringless = g
        .v()
        .has_label("player")
        .not(__::out_labels(&["won_championship_with"]))
        .to_list();
    println!(
        "Players without rings: {}",
        display_names(&storage, &ringless)
    );

    // Query 24: Find MVP winners who also won Finals MVP
    print_query("Find players who won both MVP and Finals MVP");
    let both_mvps = g
        .v()
        .has_label("player")
        .and_(vec![
            __::has_where("mvp_count", p::gte(1)),
            __::has_where("finals_mvp_count", p::gte(1)),
        ])
        .to_list();
    println!("MVP + Finals MVP: {}", display_names(&storage, &both_mvps));

    // Query 25: Find active franchises using has_not() (Phase 7)
    // has_not() filters elements WITHOUT a specific property
    print_query("Find active franchises using has_not()");
    let active_teams = g
        .v()
        .has_label("team")
        .has_not("defunct") // Teams without "defunct" property are active
        .to_list();
    println!(
        "Active teams ({}): {}",
        active_teams.len(),
        display_names(&storage, &active_teams)
    );

    // Query 26: Find defunct franchises (teams WITH defunct property)
    print_query("Find defunct franchises");
    let defunct_teams = g
        .v()
        .has_label("team")
        .has("defunct") // Teams with "defunct" property
        .to_list();
    println!(
        "Defunct teams ({}): {}",
        defunct_teams.len(),
        display_names(&storage, &defunct_teams)
    );

    // Query 27: Find players who played for championship teams
    print_query("Find players who played for teams with 3+ championships");
    let champ_team_players = g
        .v()
        .has_label("player")
        .where_(__::out_labels(&["played_for"]).has_where("championship_count", p::gte(3)))
        .dedup()
        .to_list();
    println!(
        "Players on dynasty franchises: {}",
        display_names(&storage, &champ_team_players)
    );

    // =========================================================================
    // SECTION 5: Branch Step Queries
    // =========================================================================
    print_section("5. BRANCH STEP QUERIES (union, coalesce, choose)");

    // Query 28: Union - Get all team connections for Shaq
    print_query("Get Shaq's teams (played_for AND won_championship_with)");
    let shaq_teams = g
        .v()
        .has_value("name", "Shaquille O'Neal")
        .union(vec![
            __::out_labels(&["played_for"]),
            __::out_labels(&["won_championship_with"]),
        ])
        .dedup()
        .to_list();
    println!(
        "Shaq's team connections: {}",
        display_names(&storage, &shaq_teams)
    );

    // Query 29: Coalesce - Get state, fallback to country for Toronto
    print_query("Get location (state or country) for teams");
    // Toronto has "Ontario" as state and "Canada" as country
    let toronto_location = g
        .v()
        .has_value("name", "Toronto Raptors")
        .coalesce(vec![__::values("state"), __::constant("Unknown")])
        .to_list();
    println!("Toronto Raptors location: {:?}", toronto_location);

    // Query 30: Choose - Different query based on conference
    print_query("Choose: If Western, show division; else show city");
    let lakers_conditional = g
        .v()
        .has_value("name", "Los Angeles Lakers")
        .choose(
            __::has_value("conference", "Western"),
            __::values("division"),
            __::values("city"),
        )
        .to_list();
    println!("Lakers (Western -> division): {:?}", lakers_conditional);

    let celtics_conditional = g
        .v()
        .has_value("name", "Boston Celtics")
        .choose(
            __::has_value("conference", "Western"),
            __::values("division"),
            __::values("city"),
        )
        .to_list();
    println!("Celtics (Eastern -> city): {:?}", celtics_conditional);

    // Query 31: Optional - Try to get championship, keep player if none
    print_query("Optional: Get championship team if exists");
    let barkley_optional = g
        .v()
        .has_value("name", "Charles Barkley")
        .optional(__::out_labels(&["won_championship_with"]))
        .to_list();
    println!(
        "Charles Barkley with optional championship: {}",
        display_names(&storage, &barkley_optional)
    );

    let jordan_optional = g
        .v()
        .has_value("name", "Michael Jordan")
        .optional(__::out_labels(&["won_championship_with"]))
        .to_list();
    println!(
        "Michael Jordan with optional championship: {}",
        display_names(&storage, &jordan_optional)
    );

    // =========================================================================
    // SECTION 6: Repeat Step Queries
    // =========================================================================
    print_section("6. REPEAT STEP QUERIES");

    // Query 32: Find teammates (players who played for the same team)
    print_query("Find LeBron's teammates (via shared teams)");
    let lebron_teammates = g
        .v()
        .has_value("name", "LeBron James")
        .out_labels(&["played_for"]) // LeBron's teams
        .in_labels(&["played_for"]) // Other players on those teams
        .dedup()
        .to_list();
    // Filter out LeBron himself
    let lebron_teammates: Vec<Value> = lebron_teammates
        .into_iter()
        .filter(|v| get_name(&storage, v) != "LeBron James")
        .collect();
    println!(
        "LeBron's teammates ({}): {}",
        lebron_teammates.len(),
        display_names(&storage, &lebron_teammates)
    );

    // Query 33: Find extended network - teammates of teammates
    print_query("Find teammates of Kobe's teammates (2 hops)");
    let kobe_network = g
        .v()
        .has_value("name", "Kobe Bryant")
        .out_labels(&["played_for"]) // Kobe's team (Lakers)
        .in_labels(&["played_for"]) // Lakers players
        .out_labels(&["played_for"]) // Their teams
        .in_labels(&["played_for"]) // Players on those teams
        .dedup()
        .to_list();
    println!(
        "Kobe's extended network ({}): {}",
        kobe_network.len(),
        display_names(&storage, &kobe_network)
    );

    // Query 34: Find multi-team championship winners
    print_query("Find players who won championships with multiple teams");
    let multi_team_champs: Vec<Value> = players
        .iter()
        .filter(|p| {
            let champ_teams = g
                .v_ids([p.as_vertex_id().unwrap()])
                .out_labels(&["won_championship_with"])
                .dedup()
                .count();
            champ_teams > 1
        })
        .cloned()
        .collect();
    println!(
        "Multi-team champions: {}",
        display_names(&storage, &multi_team_champs)
    );

    // Query 35: Find the Spurs dynasty players using repeat
    print_query("Traverse from Spurs to find championship connections");
    let spurs_dynasty = g
        .v()
        .has_value("name", "San Antonio Spurs")
        .repeat(__::in_labels(&["won_championship_with"]))
        .times(1)
        .emit()
        .dedup()
        .to_list();
    println!(
        "Spurs dynasty players: {}",
        display_names(&storage, &spurs_dynasty)
    );

    // =========================================================================
    // SECTION 7: Path Tracking Queries
    // =========================================================================
    print_section("7. PATH TRACKING QUERIES (as_, select, path)");

    // Query 36: Track player to team relationship with labels
    print_query("Track player -> team relationship with labels");
    let player_team_path = g
        .v()
        .has_value("name", "Kevin Durant")
        .as_("player")
        .out_labels(&["played_for"])
        .as_("team")
        .select(&["player", "team"])
        .to_list();
    println!("Kevin Durant -> Team mappings:");
    for result in &player_team_path {
        if let Value::Map(map) = result {
            let player = map
                .get("player")
                .map(|v| get_name(&storage, v))
                .unwrap_or_default();
            let team = map
                .get("team")
                .map(|v| get_name(&storage, v))
                .unwrap_or_default();
            println!("  {} -> {}", player, team);
        }
    }

    // Query 37: Track championship connections
    print_query("Track player -> championship team with labels");
    let champ_path = g
        .v()
        .has_value("name", "Stephen Curry")
        .as_("player")
        .out_labels(&["won_championship_with"])
        .as_("team")
        .select(&["player", "team"])
        .to_list();
    println!("Stephen Curry's championships:");
    for result in &champ_path {
        if let Value::Map(map) = result {
            let player = map
                .get("player")
                .map(|v| get_name(&storage, v))
                .unwrap_or_default();
            let team = map
                .get("team")
                .map(|v| get_name(&storage, v))
                .unwrap_or_default();
            println!("  {} won with {}", player, team);
        }
    }

    // Query 38: Full path from player to teammates
    print_query("Full path: Player -> Team -> Teammates");
    let full_path = g
        .v()
        .has_value("name", "Magic Johnson")
        .with_path()
        .out_labels(&["played_for"])
        .in_labels(&["played_for"])
        .path()
        .limit(5)
        .to_list();
    println!("Magic Johnson -> Team -> Teammate paths (first 5):");
    for (i, path_value) in full_path.iter().enumerate() {
        if let Value::List(path) = path_value {
            let names: Vec<String> = path.iter().map(|v| get_name(&storage, v)).collect();
            println!("  Path {}: {}", i + 1, names.join(" -> "));
        }
    }

    // =========================================================================
    // SECTION 8: Complex Combined Queries
    // =========================================================================
    print_section("8. COMPLEX COMBINED QUERIES");

    // Query 39: Find MVP players who won championships with the team where they won MVP
    print_query("Find players who won MVP and championships");
    let mvp_champs = g
        .v()
        .has_label("player")
        .has("mvp_awards")
        .where_(__::out_labels(&["won_championship_with"]))
        .to_list();
    println!(
        "MVP + Championship winners ({}): {}",
        mvp_champs.len(),
        display_names(&storage, &mvp_champs)
    );

    // Query 40: Find the most decorated players (MVP + Finals MVP + Championships)
    print_query("Find most decorated players (MVP, Finals MVP, and rings)");
    let decorated = g
        .v()
        .has_label("player")
        .and_(vec![
            __::has("mvp_awards"),
            __::has("finals_mvp_awards"),
            __::out_labels(&["won_championship_with"]),
        ])
        .to_list();
    println!(
        "Triple-crown players: {}",
        display_names(&storage, &decorated)
    );

    // Query 41: Find Lakers-Celtics rivalry players (played for both)
    print_query("Find players in the Lakers-Celtics connection");
    // Players who played for Lakers and have teammates who played for Celtics
    let lakers = g
        .v()
        .has_value("name", "Los Angeles Lakers")
        .in_labels(&["played_for"])
        .dedup()
        .to_list();
    let celtics = g
        .v()
        .has_value("name", "Boston Celtics")
        .in_labels(&["played_for"])
        .dedup()
        .to_list();
    println!("Lakers players: {}", display_names(&storage, &lakers));
    println!("Celtics players: {}", display_names(&storage, &celtics));

    // Query 42: Find shooting guards who are elite scorers
    print_query("Find elite scoring shooting guards (25+ PPG)");
    let elite_sgs = g
        .v()
        .has_label("player")
        .has_value("position", "Shooting Guard")
        .has_where("points_per_game", p::gte(25.0))
        .to_list();
    println!(
        "Elite scoring SGs: {}",
        display_with_prop(&storage, &elite_sgs, "points_per_game")
    );

    // Query 43: Find big men with great passing (Centers with 3+ APG)
    print_query("Find passing big men (Centers with 3+ APG)");
    let passing_bigs = g
        .v()
        .has_label("player")
        .has_value("position", "Center")
        .has_where("assists_per_game", p::gte(3.0))
        .to_list();
    println!(
        "Passing Centers: {}",
        display_with_prop(&storage, &passing_bigs, "assists_per_game")
    );

    // Query 44: Find players with championship drought (played for non-champion teams only)
    print_query("Find championship-less players on winning franchises");
    let drought_players = g
        .v()
        .has_label("player")
        .not(__::out_labels(&["won_championship_with"]))
        .where_(__::out_labels(&["played_for"]).has_where("championship_count", p::gte(1)))
        .to_list();
    println!(
        "Ringless on winning franchises: {}",
        display_names(&storage, &drought_players)
    );

    // Query 45: Find the GOAT candidates (multiple MVPs + multiple Finals MVPs)
    print_query("Find GOAT candidates (2+ MVPs and 2+ Finals MVPs)");
    let goat_candidates = g
        .v()
        .has_label("player")
        .has_where("mvp_count", p::gte(2))
        .has_where("finals_mvp_count", p::gte(2))
        .to_list();
    for candidate in &goat_candidates {
        let name = get_name(&storage, candidate);
        let mvps = get_property(&storage, candidate, "mvp_count").unwrap_or_default();
        let fmvps = get_property(&storage, candidate, "finals_mvp_count").unwrap_or_default();
        // Sum ring_count from all won_championship_with edges
        // Each edge has a ring_count property = number of championships with that team
        let rings: i64 = g
            .v_ids([candidate.as_vertex_id().unwrap()])
            .out_e_labels(&["won_championship_with"])
            .values("ring_count")
            .to_list()
            .iter()
            .filter_map(|v| v.as_i64())
            .sum();
        println!(
            "  {}: {} MVPs, {} Finals MVPs, {} rings",
            name, mvps, fmvps, rings
        );
    }

    // =========================================================================
    // SECTION 9: Transform Steps (Phase 7)
    // =========================================================================
    print_section("9. TRANSFORM STEPS (Phase 7: value_map, element_map, order, mean, unfold)");

    // Query 46: Get player stats using value_map()
    print_query("Get Michael Jordan's stats using value_map()");
    let mj_stats = g
        .v()
        .has_value("name", "Michael Jordan")
        .value_map_keys(["name", "points_per_game", "mvp_count", "position"])
        .to_list();
    println!("MJ value_map:");
    for stat in &mj_stats {
        println!("  {:?}", stat);
    }

    // Query 47: Get complete element representation using element_map()
    print_query("Get Bulls complete element_map()");
    let bulls_element = g
        .v()
        .has_value("name", "Chicago Bulls")
        .element_map_keys(["name", "city", "championship_count", "conference"])
        .to_list();
    println!("Bulls element_map:");
    for elem in &bulls_element {
        println!("  {:?}", elem);
    }

    // Query 48: Get edge element_map (includes IN/OUT vertex references)
    print_query("Get edge element_map for MJ's played_for edges");
    let mj_edges = g
        .v()
        .has_value("name", "Michael Jordan")
        .out_e_labels(&["played_for"])
        .element_map()
        .to_list();
    println!("MJ's played_for edges (element_map):");
    for edge in &mj_edges {
        println!("  {:?}", edge);
    }

    // Query 49: Order players by PPG descending
    print_query("Top 5 scorers by PPG using order()");
    let top_scorers = g
        .v()
        .has_label("player")
        .order()
        .by_key_desc("points_per_game")
        .build()
        .limit(5)
        .to_list();
    println!("Top 5 scorers:");
    for (i, player) in top_scorers.iter().enumerate() {
        let name = get_name(&storage, player);
        let ppg = get_property(&storage, player, "points_per_game").unwrap_or_default();
        println!("  {}. {} ({} PPG)", i + 1, name, ppg);
    }

    // Query 50: Order teams by championship count ascending
    print_query("Teams ordered by championships (ascending)");
    let teams_by_chips = g
        .v()
        .has_label("team")
        .has("championship_count") // Only teams with championships
        .order()
        .by_key_asc("championship_count")
        .build()
        .to_list();
    println!("Teams by championships (asc):");
    for team in &teams_by_chips {
        let name = get_name(&storage, team);
        let chips = get_property(&storage, team, "championship_count").unwrap_or_default();
        println!("  {} ({})", name, chips);
    }

    // Query 51: Calculate average PPG using mean()
    print_query("Calculate average PPG across all players");
    let avg_ppg = g
        .v()
        .has_label("player")
        .values("points_per_game")
        .mean()
        .to_list();
    println!(
        "Average PPG: {:.2}",
        avg_ppg.first().and_then(|v| v.as_f64()).unwrap_or(0.0)
    );

    // Query 52: Calculate average rebounds for Centers
    print_query("Average rebounds per game for Centers");
    let avg_center_rpg = g
        .v()
        .has_label("player")
        .has_value("position", "Center")
        .values("rebounds_per_game")
        .mean()
        .to_list();
    println!(
        "Average Center RPG: {:.2}",
        avg_center_rpg
            .first()
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0)
    );

    // Query 53: Unfold value_map entries
    print_query("Unfold LeBron's value_map into individual entries");
    let lebron_props = g
        .v()
        .has_value("name", "LeBron James")
        .value_map_keys(["name", "position", "mvp_count"])
        .unfold()
        .to_list();
    println!("LeBron's properties (unfolded):");
    for prop in &lebron_props {
        println!("  {:?}", prop);
    }

    // Query 54: Order + limit for "Top N" queries
    print_query("Top 3 players by MVP count");
    let mvp_leaders = g
        .v()
        .has_label("player")
        .has("mvp_count")
        .order()
        .by_key_desc("mvp_count")
        .build()
        .limit(3)
        .to_list();
    println!("MVP Leaders:");
    for player in &mvp_leaders {
        let name = get_name(&storage, player);
        let mvps = get_property(&storage, player, "mvp_count").unwrap_or_default();
        println!("  {} ({} MVPs)", name, mvps);
    }

    // =========================================================================
    // SECTION 10: Aggregation Steps (Phase 7)
    // =========================================================================
    print_section("10. AGGREGATION STEPS (Phase 7: group, group_count)");

    // Query 55: Group players by position
    print_query("Group players by position using group()");
    let by_position = g
        .v()
        .has_label("player")
        .group()
        .by_key("position")
        .by_value_key("name")
        .build()
        .to_list();
    println!("Players grouped by position:");
    if let Some(Value::Map(map)) = by_position.first() {
        for (pos, players) in map {
            println!("  {}: {:?}", pos, players);
        }
    }

    // Query 56: Count players by position using group_count()
    print_query("Count players by position using group_count()");
    let position_counts = g
        .v()
        .has_label("player")
        .group_count()
        .by_key("position")
        .build()
        .to_list();
    println!("Position counts:");
    if let Some(Value::Map(map)) = position_counts.first() {
        for (pos, count) in map {
            println!("  {}: {}", pos, format_value(count));
        }
    }

    // Query 57: Count teams by conference
    print_query("Count teams by conference using group_count()");
    let conference_counts = g
        .v()
        .has_label("team")
        .group_count()
        .by_key("conference")
        .build()
        .to_list();
    println!("Conference counts:");
    if let Some(Value::Map(map)) = conference_counts.first() {
        for (conf, count) in map {
            println!("  {}: {}", conf, format_value(count));
        }
    }

    // Query 58: Group teams by conference, collect names
    print_query("Group teams by conference");
    let teams_by_conf = g
        .v()
        .has_label("team")
        .group()
        .by_key("conference")
        .by_value_key("name")
        .build()
        .to_list();
    println!("Teams by conference:");
    if let Some(Value::Map(map)) = teams_by_conf.first() {
        for (conf, teams) in map {
            println!("  {}: {:?}", conf, teams);
        }
    }

    // Query 59: Group players by position, count using group_count
    print_query("Group edges by label using group_count()");
    let edge_label_counts = g.e().group_count().by_label().build().to_list();
    println!("Edge counts by label:");
    if let Some(Value::Map(map)) = edge_label_counts.first() {
        for (label, count) in map {
            println!("  {}: {}", label, format_value(count));
        }
    }

    // Query 60: Group championship teams by dynasty era
    print_query("Count championship-winning teams by conference");
    let champ_by_conf = g
        .v()
        .has_label("team")
        .has_where("championship_count", p::gte(1))
        .group_count()
        .by_key("conference")
        .build()
        .to_list();
    println!("Championship teams by conference:");
    if let Some(Value::Map(map)) = champ_by_conf.first() {
        for (conf, count) in map {
            println!("  {}: {}", conf, format_value(count));
        }
    }

    // =========================================================================
    // Summary Statistics (Refactored with Phase 7 APIs)
    // =========================================================================
    print_section("SUMMARY STATISTICS (Using Phase 7 APIs)");

    // Position distribution using group_count() instead of manual loop
    println!("Players by position (using group_count):");
    let position_dist = g
        .v()
        .has_label("player")
        .group_count()
        .by_key("position")
        .build()
        .to_list();
    if let Some(Value::Map(map)) = position_dist.first() {
        // Sort for consistent output
        let mut positions: Vec<_> = map.iter().collect();
        positions.sort_by(|(a, _), (b, _)| a.cmp(b));
        for (pos, count) in positions {
            println!("  {}: {}", pos, format_value(count));
        }
    }

    // Conference distribution using group_count()
    println!("\nTeams by conference (using group_count):");
    let conf_dist = g
        .v()
        .has_label("team")
        .group_count()
        .by_key("conference")
        .build()
        .to_list();
    if let Some(Value::Map(map)) = conf_dist.first() {
        for (conf, count) in map {
            println!("  {}: {}", conf, format_value(count));
        }
    }

    // Edge counts using group_count() by label
    println!("\nRelationship counts (using group_count by label):");
    let edge_dist = g.e().group_count().by_label().build().to_list();
    if let Some(Value::Map(map)) = edge_dist.first() {
        for (label, count) in map {
            println!("  {}: {}", label, format_value(count));
        }
    }

    // Average stats using mean()
    println!("\nAverage stats (using mean()):");
    let avg_ppg = g
        .v()
        .has_label("player")
        .values("points_per_game")
        .mean()
        .to_list();
    println!(
        "  Average PPG: {:.2}",
        avg_ppg.first().and_then(|v| v.as_f64()).unwrap_or(0.0)
    );

    let avg_rpg = g
        .v()
        .has_label("player")
        .values("rebounds_per_game")
        .mean()
        .to_list();
    println!(
        "  Average RPG: {:.2}",
        avg_rpg.first().and_then(|v| v.as_f64()).unwrap_or(0.0)
    );

    let avg_apg = g
        .v()
        .has_label("player")
        .values("assists_per_game")
        .mean()
        .to_list();
    println!(
        "  Average APG: {:.2}",
        avg_apg.first().and_then(|v| v.as_f64()).unwrap_or(0.0)
    );

    // Championship leaders using order()
    println!("\nChampionship leaders (using order().by_key_desc()):");
    let champ_leaders = g
        .v()
        .has_label("team")
        .has_where("championship_count", p::gte(5))
        .order()
        .by_key_desc("championship_count")
        .build()
        .to_list();
    for team in &champ_leaders {
        let name = get_name(&storage, team);
        let count = get_property(&storage, team, "championship_count").unwrap_or_default();
        println!("  {}: {} championships", name, count);
    }

    // All-Star leaders using order()
    println!("\nAll-Star leaders (15+ selections, using order().by_key_desc()):");
    let allstar_leaders = g
        .v()
        .has_label("player")
        .has_where("all_star_selections", p::gte(15))
        .order()
        .by_key_desc("all_star_selections")
        .build()
        .to_list();
    for player in &allstar_leaders {
        let name = get_name(&storage, player);
        let selections = get_property(&storage, player, "all_star_selections").unwrap_or_default();
        println!("  {}: {} All-Star selections", name, selections);
    }

    // Total MVP awards using values() + sum (still needed since sum() isn't a step)
    println!("\nMVP statistics:");
    let total_mvps: i64 = g
        .v()
        .has_label("player")
        .values("mvp_count")
        .to_list()
        .iter()
        .filter_map(|v| v.as_i64())
        .sum();
    println!("  Total MVP awards represented: {}", total_mvps);

    println!("\n=== Example Complete ===");
}
