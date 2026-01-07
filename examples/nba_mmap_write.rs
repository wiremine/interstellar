//! NBA Graph - Memory-Mapped Storage Write Example
//!
//! This example demonstrates how to create a persistent graph database using
//! the memory-mapped (MmapGraph) storage backend. It loads the NBA dataset
//! from JSON and writes it to a persistent file that survives process restarts.
//!
//! Features demonstrated:
//! - Opening/creating an MmapGraph database file
//! - Batch mode for efficient bulk loading (~500x faster than individual writes)
//! - Adding vertices and edges with properties
//! - Checkpointing for durability guarantees
//!
//! Run with: `cargo run --features mmap --example nba_mmap_write`
//!
//! After running this example, run `nba_mmap_read` to query the persisted data.

use rustgremlin::storage::mmap::MmapGraph;
use rustgremlin::value::{Value, VertexId};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

const DB_PATH: &str = "examples/data/nba_graph.db";

/// ID mappings for looking up vertices by their JSON IDs
struct IdMappings {
    players: HashMap<String, VertexId>,
    teams: HashMap<String, VertexId>,
}

fn main() {
    println!("=== NBA Graph Database - MmapGraph Write Example ===\n");

    // Check if database already exists
    let db_exists = Path::new(DB_PATH).exists();
    if db_exists {
        println!("Note: Database file already exists at {}", DB_PATH);
        println!("      This will add data to the existing database.\n");
        println!("      Delete the file to start fresh: rm {}\n", DB_PATH);
    }

    // =========================================================================
    // Step 1: Open/Create the MmapGraph database
    // =========================================================================
    println!("Step 1: Opening MmapGraph database at {}...", DB_PATH);

    let storage = MmapGraph::open(DB_PATH).expect("Failed to open/create MmapGraph database");
    let storage = Arc::new(storage);

    println!("        Database opened successfully!\n");

    // =========================================================================
    // Step 2: Load JSON fixture data
    // =========================================================================
    println!("Step 2: Loading NBA data from examples/fixtures/nba.json...");

    let json_str =
        fs::read_to_string("examples/fixtures/nba.json").expect("Failed to read nba.json");
    let data: JsonValue = serde_json::from_str(&json_str).expect("Failed to parse JSON");

    let team_count = data["teams"].as_array().map(|a| a.len()).unwrap_or(0);
    let player_count = data["players"].as_array().map(|a| a.len()).unwrap_or(0);
    let played_for_count = data["relationships"]["played_for"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    let won_champ_count = data["relationships"]["won_championship_with"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    println!("        Found {} teams", team_count);
    println!("        Found {} players", player_count);
    println!(
        "        Found {} played_for relationships",
        played_for_count
    );
    println!(
        "        Found {} won_championship_with relationships\n",
        won_champ_count
    );

    // =========================================================================
    // Step 3: Begin batch mode for efficient bulk loading
    // =========================================================================
    println!("Step 3: Beginning batch mode for efficient bulk loading...");
    println!("        (Batch mode buffers WAL writes for ~500x faster inserts)\n");

    storage.begin_batch().expect("Failed to begin batch mode");

    let mut mappings = IdMappings {
        players: HashMap::new(),
        teams: HashMap::new(),
    };

    // =========================================================================
    // Step 4: Load Teams
    // =========================================================================
    println!("Step 4: Loading teams...");

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

            // Store original JSON ID for reference
            props.insert("json_id".to_string(), Value::String(json_id.to_string()));

            let vid = storage
                .add_vertex("team", props)
                .expect("Failed to add team vertex");
            mappings.teams.insert(json_id.to_string(), vid);
        }
    }

    println!("        Loaded {} teams", mappings.teams.len());

    // =========================================================================
    // Step 5: Load Players
    // =========================================================================
    println!("Step 5: Loading players...");

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

            // Store original JSON ID for reference
            props.insert("json_id".to_string(), Value::String(json_id.to_string()));

            let vid = storage
                .add_vertex("player", props)
                .expect("Failed to add player vertex");
            mappings.players.insert(json_id.to_string(), vid);
        }
    }

    println!("        Loaded {} players", mappings.players.len());

    // =========================================================================
    // Step 6: Load Edges - played_for
    // =========================================================================
    println!("Step 6: Loading played_for edges...");

    let mut played_for_loaded = 0;
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
                played_for_loaded += 1;
            }
        }
    }

    println!("        Loaded {} played_for edges", played_for_loaded);

    // =========================================================================
    // Step 7: Load Edges - won_championship_with
    // =========================================================================
    println!("Step 7: Loading won_championship_with edges...");

    let mut won_champ_loaded = 0;
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
                won_champ_loaded += 1;
            }
        }
    }

    println!(
        "        Loaded {} won_championship_with edges\n",
        won_champ_loaded
    );

    // =========================================================================
    // Step 8: Commit batch and checkpoint
    // =========================================================================
    println!("Step 8: Committing batch and creating checkpoint...");

    storage.commit_batch().expect("Failed to commit batch");
    println!("        Batch committed successfully!");

    storage.checkpoint().expect("Failed to create checkpoint");
    println!("        Checkpoint created - data is now durable!\n");

    // =========================================================================
    // Summary
    // =========================================================================
    println!("=== Write Complete ===\n");
    println!("Database written to: {}", DB_PATH);
    println!("  - {} teams", mappings.teams.len());
    println!("  - {} players", mappings.players.len());
    println!("  - {} played_for edges", played_for_loaded);
    println!("  - {} won_championship_with edges", won_champ_loaded);
    println!(
        "  - Total: {} vertices, {} edges",
        mappings.teams.len() + mappings.players.len(),
        played_for_loaded + won_champ_loaded
    );

    // Get file size
    if let Ok(metadata) = fs::metadata(DB_PATH) {
        let size_bytes = metadata.len();
        let size_kb = size_bytes as f64 / 1024.0;
        println!("  - File size: {:.2} KB", size_kb);
    }

    println!("\nNext step: Run the read example to query the persisted data:");
    println!("  cargo run --features mmap --example nba_mmap_read");
}
