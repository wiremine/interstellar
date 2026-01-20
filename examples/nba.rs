//! NBA Graph Database Example
//!
//! Comprehensive demonstration of Interstellar's graph database capabilities using
//! an NBA dataset with players, teams, and their relationships.
//!
//! This example showcases:
//! - Data loading from JSON fixtures
//! - Fluent traversal API queries
//! - GQL (Graph Query Language) queries
//! - Memory-mapped persistent storage (optional)
//!
//! Run: `cargo run --example nba`
//! With persistence: `cargo run --features mmap --example nba`

use interstellar::prelude::*;
use interstellar::storage::{Graph, GraphSnapshot, GraphStorage};
use interstellar::traversal::{p, __};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;

// =============================================================================
// Data Loading
// =============================================================================

struct NbaGraph {
    graph: Graph,
    player_ids: HashMap<String, VertexId>,
    team_ids: HashMap<String, VertexId>,
}

fn load_nba_graph() -> NbaGraph {
    let json_str =
        fs::read_to_string("examples/fixtures/nba.json").expect("Failed to read nba.json");
    let data: JsonValue = serde_json::from_str(&json_str).expect("Failed to parse JSON");

    let graph = Graph::new();
    let mut player_ids = HashMap::new();
    let mut team_ids = HashMap::new();

    // Load Teams
    if let Some(teams) = data["teams"].as_array() {
        for team in teams {
            let json_id = team["id"].as_str().unwrap_or("unknown");
            let mut props = HashMap::new();

            for key in ["name", "city", "state", "arena", "conference", "division"] {
                if let Some(v) = team[key].as_str() {
                    props.insert(key.to_string(), Value::String(v.to_string()));
                }
            }
            if let Some(v) = team["founded"].as_i64() {
                props.insert("founded".to_string(), Value::Int(v));
            }
            if let Some(v) = team["defunct"].as_bool() {
                props.insert("defunct".to_string(), Value::Bool(v));
            }
            if let Some(championships) = team["championships"].as_array() {
                let values: Vec<Value> = championships
                    .iter()
                    .filter_map(|y| y.as_i64().map(Value::Int))
                    .collect();
                props.insert(
                    "championship_count".to_string(),
                    Value::Int(values.len() as i64),
                );
                props.insert("championships".to_string(), Value::List(values));
            }

            let vid = graph.add_vertex("team", props);
            team_ids.insert(json_id.to_string(), vid);
        }
    }

    // Load Players
    if let Some(players) = data["players"].as_array() {
        for player in players {
            let json_id = player["id"].as_str().unwrap_or("unknown");
            let mut props = HashMap::new();

            if let Some(v) = player["name"].as_str() {
                props.insert("name".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = player["position"].as_str() {
                props.insert("position".to_string(), Value::String(v.to_string()));
            }
            if let Some(v) = player["height_inches"].as_i64() {
                props.insert("height_inches".to_string(), Value::Int(v));
            }
            if let Some(v) = player["all_star_selections"].as_i64() {
                props.insert("all_star_selections".to_string(), Value::Int(v));
            }

            // Flatten career stats
            if let Some(stats) = player["career_stats"].as_object() {
                for key in ["points_per_game", "rebounds_per_game", "assists_per_game"] {
                    if let Some(v) = stats.get(key).and_then(|v| v.as_f64()) {
                        props.insert(key.to_string(), Value::Float(v));
                    }
                }
            }

            // MVP counts
            if let Some(mvps) = player["mvp_awards"].as_array() {
                props.insert("mvp_count".to_string(), Value::Int(mvps.len() as i64));
            }
            if let Some(fmvps) = player["finals_mvp_awards"].as_array() {
                props.insert(
                    "finals_mvp_count".to_string(),
                    Value::Int(fmvps.len() as i64),
                );
            }

            let vid = graph.add_vertex("player", props);
            player_ids.insert(json_id.to_string(), vid);
        }
    }

    // Load played_for edges
    if let Some(edges) = data["relationships"]["played_for"].as_array() {
        for edge in edges {
            let player_id = edge["player_id"].as_str().unwrap_or("");
            let team_id = edge["team_id"].as_str().unwrap_or("");
            if let (Some(&p), Some(&t)) = (player_ids.get(player_id), team_ids.get(team_id)) {
                let mut props = HashMap::new();
                if let Some(start) = edge["start_year"].as_i64() {
                    props.insert("start_year".to_string(), Value::Int(start));
                }
                if let Some(end) = edge["end_year"].as_i64() {
                    props.insert("end_year".to_string(), Value::Int(end));
                }
                let _ = graph.add_edge(p, t, "played_for", props);
            }
        }
    }

    // Load won_championship_with edges
    if let Some(edges) = data["relationships"]["won_championship_with"].as_array() {
        for edge in edges {
            let player_id = edge["player_id"].as_str().unwrap_or("");
            let team_id = edge["team_id"].as_str().unwrap_or("");
            if let (Some(&p), Some(&t)) = (player_ids.get(player_id), team_ids.get(team_id)) {
                let mut props = HashMap::new();
                if let Some(years) = edge["years"].as_array() {
                    props.insert("ring_count".to_string(), Value::Int(years.len() as i64));
                }
                let _ = graph.add_edge(p, t, "won_championship_with", props);
            }
        }
    }

    NbaGraph {
        graph,
        player_ids,
        team_ids,
    }
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
        Value::Float(f) => format!("{:.1}", f),
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
    println!("=== NBA Graph Database Example ===\n");

    let nba = load_nba_graph();
    let snapshot = nba.graph.snapshot();
    let g = snapshot.gremlin();

    // Graph statistics
    let players = g.v().has_label("player").count();
    let teams = g.v().has_label("team").count();
    let edges = g.e().count();
    println!(
        "Graph loaded: {} players, {} teams, {} edges",
        players, teams, edges
    );

    // =========================================================================
    // FLUENT API EXAMPLES
    // =========================================================================
    section("FLUENT TRAVERSAL API");

    // Basic navigation
    println!("\n--- Teams Michael Jordan played for ---");
    let mj_teams = g
        .v()
        .has_value("name", "Michael Jordan")
        .out_labels(&["played_for"])
        .to_list();
    println!("{}", display_names(&snapshot, &mj_teams));

    println!("\n--- Lakers players ---");
    let lakers = g
        .v()
        .has_value("name", "Los Angeles Lakers")
        .in_labels(&["played_for"])
        .dedup()
        .to_list();
    println!("{}", display_names(&snapshot, &lakers));

    // Predicate filtering
    println!("\n--- Elite scorers (27+ PPG) ---");
    let elite = g
        .v()
        .has_label("player")
        .has_where("points_per_game", p::gte(27.0))
        .to_list();
    for p in &elite {
        let name = get_name(&snapshot, p);
        if let Some(vid) = p.as_vertex_id() {
            if let Some(v) = snapshot.get_vertex(vid) {
                if let Some(Value::Float(ppg)) = v.properties.get("points_per_game") {
                    println!("  {} ({:.1} PPG)", name, ppg);
                }
            }
        }
    }

    // Anonymous traversals with where/not
    println!("\n--- Championship winners ---");
    let champs = g
        .v()
        .has_label("player")
        .where_(__::out_labels(&["won_championship_with"]))
        .to_list();
    println!("{}", display_names(&snapshot, &champs));

    println!("\n--- Players without rings ---");
    let ringless = g
        .v()
        .has_label("player")
        .not(__::out_labels(&["won_championship_with"]))
        .to_list();
    println!("{}", display_names(&snapshot, &ringless));

    // Branch steps
    println!("\n--- Shaq's team connections (union) ---");
    let shaq = g
        .v()
        .has_value("name", "Shaquille O'Neal")
        .union(vec![
            __::out_labels(&["played_for"]),
            __::out_labels(&["won_championship_with"]),
        ])
        .dedup()
        .to_list();
    println!("{}", display_names(&snapshot, &shaq));

    // Path tracking
    println!("\n--- Kevin Durant's career path ---");
    let kd_path = g
        .v()
        .has_value("name", "Kevin Durant")
        .as_("player")
        .out_labels(&["played_for"])
        .as_("team")
        .select(&["player", "team"])
        .to_list();
    for r in &kd_path {
        if let Value::Map(map) = r {
            let player = map
                .get("player")
                .map(|v| get_name(&snapshot, v))
                .unwrap_or_default();
            let team = map
                .get("team")
                .map(|v| get_name(&snapshot, v))
                .unwrap_or_default();
            println!("  {} -> {}", player, team);
        }
    }

    // Aggregation
    println!("\n--- Players by position (group_count) ---");
    let by_pos = g
        .v()
        .has_label("player")
        .group_count()
        .by_key("position")
        .build()
        .to_list();
    if let Some(Value::Map(map)) = by_pos.first() {
        for (pos, count) in map {
            println!("  {}: {}", pos, format_value(count));
        }
    }

    println!("\n--- Top 3 scorers (order + limit) ---");
    let top3 = g
        .v()
        .has_label("player")
        .order()
        .by_key_desc("points_per_game")
        .build()
        .limit(3)
        .to_list();
    for (i, p) in top3.iter().enumerate() {
        let name = get_name(&snapshot, p);
        if let Some(vid) = p.as_vertex_id() {
            if let Some(v) = snapshot.get_vertex(vid) {
                if let Some(Value::Float(ppg)) = v.properties.get("points_per_game") {
                    println!("  {}. {} ({:.1} PPG)", i + 1, name, ppg);
                }
            }
        }
    }

    println!("\n--- Average PPG (mean) ---");
    let avg = g
        .v()
        .has_label("player")
        .values("points_per_game")
        .mean()
        .to_list();
    if let Some(Value::Float(v)) = avg.first() {
        println!("  {:.2} PPG", v);
    }

    // =========================================================================
    // GQL EXAMPLES
    // =========================================================================
    section("GQL (GRAPH QUERY LANGUAGE)");

    println!("\n--- Count players (GQL) ---");
    let r = nba.graph.gql("MATCH (p:player) RETURN count(*)").unwrap();
    println!("  {}", format_value(&r[0]));

    println!("\n--- Michael Jordan's teams (GQL) ---");
    let r = nba
        .graph
        .gql(r#"MATCH (p:player {name: 'Michael Jordan'})-[:played_for]->(t:team) RETURN t.name"#)
        .unwrap();
    for v in &r {
        println!("  {}", format_value(v));
    }

    println!("\n--- Elite scorers with ORDER BY (GQL) ---");
    let r = nba
        .graph
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.points_per_game >= 27.0
            RETURN p.name, p.points_per_game
            ORDER BY p.points_per_game DESC
        "#,
        )
        .unwrap();
    for v in &r {
        if let Value::Map(m) = v {
            println!(
                "  {} ({} PPG)",
                format_value(m.get("p.name").unwrap_or(&Value::Null)),
                format_value(m.get("p.points_per_game").unwrap_or(&Value::Null))
            );
        }
    }

    println!("\n--- Championship winners (EXISTS subquery) ---");
    let r = nba
        .graph
        .gql(
            r#"
            MATCH (p:player)
            WHERE EXISTS { (p)-[:won_championship_with]->() }
            RETURN p.name
            ORDER BY p.name
        "#,
        )
        .unwrap();
    let names: Vec<String> = r.iter().map(format_value).collect();
    println!("  {}", names.join(", "));

    println!("\n--- Players by position (GROUP BY) ---");
    let r = nba
        .graph
        .gql(
            r#"
            MATCH (p:player)
            RETURN p.position, count(*) AS cnt
            GROUP BY p.position
        "#,
        )
        .unwrap();
    for v in &r {
        if let Value::Map(m) = v {
            println!(
                "  {}: {}",
                format_value(m.get("p.position").unwrap_or(&Value::Null)),
                format_value(m.get("cnt").unwrap_or(&Value::Null))
            );
        }
    }

    println!("\n--- Scoring tiers (CASE expression) ---");
    let r = nba
        .graph
        .gql(
            r#"
            MATCH (p:player)
            RETURN p.name,
                   CASE
                       WHEN p.points_per_game >= 27.0 THEN 'Elite'
                       WHEN p.points_per_game >= 20.0 THEN 'Star'
                       ELSE 'Other'
                   END AS tier
            ORDER BY p.points_per_game DESC
            LIMIT 5
        "#,
        )
        .unwrap();
    for v in &r {
        if let Value::Map(m) = v {
            println!(
                "  {} - {}",
                format_value(m.get("p.name").unwrap_or(&Value::Null)),
                format_value(m.get("tier").unwrap_or(&Value::Null))
            );
        }
    }

    println!("\n--- Introspection: id() and labels() ---");
    let r = nba
        .graph
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.name = 'LeBron James'
            RETURN id(p) AS vid, labels(p) AS lbls, p.name
        "#,
        )
        .unwrap();
    for v in &r {
        if let Value::Map(m) = v {
            println!(
                "  ID: {}, Labels: {}, Name: {}",
                format_value(m.get("vid").unwrap_or(&Value::Null)),
                format_value(m.get("lbls").unwrap_or(&Value::Null)),
                format_value(m.get("p.name").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // PERSISTENCE (optional mmap feature)
    // =========================================================================
    #[cfg(feature = "mmap")]
    {
        section("PERSISTENCE (MmapGraph)");
        demonstrate_persistence();
    }

    println!("\n=== Example Complete ===");
}

#[cfg(feature = "mmap")]
fn demonstrate_persistence() {
    use interstellar::storage::mmap::MmapGraph;

    const DB_PATH: &str = "examples/data/nba_demo.db";

    // Write
    println!("\n--- Writing to persistent storage ---");
    {
        let storage = MmapGraph::open(DB_PATH).expect("Failed to open MmapGraph");
        storage.begin_batch().unwrap();

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Demo Player".to_string()));
        props.insert("position".to_string(), Value::String("Guard".to_string()));
        let _ = storage.add_vertex("player", props);

        storage.commit_batch().unwrap();
        storage.checkpoint().unwrap();
        println!("  Written to {}", DB_PATH);
    }

    // Read back
    println!("\n--- Reading from persistent storage ---");
    {
        let storage = MmapGraph::open(DB_PATH).expect("Failed to open MmapGraph");
        // Use MmapGraph directly for reading - it implements GraphStorage
        println!("  Players in persistent store: {}", storage.vertex_count());
    }

    // Cleanup
    let _ = fs::remove_file(DB_PATH);
    let _ = fs::remove_file(format!("{}.wal", DB_PATH));
    println!("  Cleaned up demo database");
}
