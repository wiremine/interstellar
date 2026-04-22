//! # NBA Graph Example
//!
//! Comprehensive demonstration of GQL (Graph Query Language) features.
//!
//! This example showcases Interstellar's GQL capabilities using NBA data
//! loaded from a GraphSON 3.0 fixture with embedded schema:
//! - Basic queries: MATCH, RETURN, WHERE, ORDER BY, LIMIT
//! - Pattern matching: single-hop, multi-hop, variable-length paths
//! - Aggregations: count(), sum(), avg(), min(), max(), collect()
//! - Grouping: GROUP BY
//! - Subqueries: EXISTS { }
//! - CASE expressions
//! - Introspection: id(), labels()
//! - Mutations: CREATE, SET, DELETE, DETACH DELETE, MERGE
//! - Parameters: $paramName, gql_with_params()
//! - Advanced: inline WHERE, LET clause, list comprehensions, map literals
//!
//! Run: `cargo run --example nba`

use interstellar::graphson;
use interstellar::prelude::*;
use interstellar::storage::Graph;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

// =============================================================================
// Data Loading (GraphSON Import)
// =============================================================================

/// Load the NBA graph from GraphSON 3.0 fixture.
///
/// The fixture includes both the graph data and schema definitions,
/// demonstrating real-world GraphSON import capabilities.
fn load_nba_graph() -> Arc<Graph> {
    let json_str = fs::read_to_string("examples/fixtures/nba.graphson.json")
        .expect("Failed to read nba.graphson.json");
    Arc::new(graphson::from_str(&json_str).expect("Failed to parse GraphSON"))
}

// =============================================================================
// Helper Functions
// =============================================================================

fn format_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => format!("{:.1}", f),
        Value::Bool(b) => b.to_string(),
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
        Value::Null => "null".to_string(),
        _ => format!("{:?}", value),
    }
}

fn get_field(row: &Value, key: &str) -> String {
    if let Value::Map(m) = row {
        m.get(key).map(format_value).unwrap_or_default()
    } else {
        format_value(row)
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
    println!("=== NBA Graph Database Example ===");
    println!("Comprehensive GQL (Graph Query Language) Demonstration\n");

    // =========================================================================
    // Part 1: Load Data from GraphSON
    // =========================================================================
    section("PART 1: LOAD DATA FROM GRAPHSON");

    println!("\n--- Loading NBA Graph from GraphSON ---");
    let graph = load_nba_graph();

    // Graph statistics using GQL
    let players: i64 = match &graph.gql("MATCH (p:player) RETURN count(*)").unwrap()[0] {
        Value::Int(n) => *n,
        Value::Map(m) => m.values().next().and_then(|v| v.as_i64()).unwrap_or(0),
        _ => 0,
    };
    let teams: i64 = match &graph.gql("MATCH (t:team) RETURN count(*)").unwrap()[0] {
        Value::Int(n) => *n,
        Value::Map(m) => m.values().next().and_then(|v| v.as_i64()).unwrap_or(0),
        _ => 0,
    };
    println!("Graph loaded: {} players, {} teams", players, teams);
    println!("(Schema embedded in GraphSON fixture)");

    // =========================================================================
    // Part 2: Basic GQL Queries
    // =========================================================================
    section("PART 2: BASIC GQL QUERIES");

    println!("\n--- MATCH and RETURN ---");
    let results = graph.gql("MATCH (p:player) RETURN p.name LIMIT 5").unwrap();
    println!("First 5 players:");
    for r in &results {
        println!("  {}", format_value(r));
    }

    println!("\n--- WHERE clause filtering ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)
               WHERE p.points_per_game >= 25.0
               RETURN p.name, p.points_per_game
               ORDER BY p.points_per_game DESC"#,
        )
        .unwrap();
    println!("Players with 25+ PPG:");
    for r in &results {
        println!(
            "  {} ({} PPG)",
            get_field(r, "p.name"),
            get_field(r, "p.points_per_game")
        );
    }

    println!("\n--- ORDER BY and LIMIT ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)
               RETURN p.name, p.all_star_selections
               ORDER BY p.all_star_selections DESC
               LIMIT 3"#,
        )
        .unwrap();
    println!("Top 3 All-Star players:");
    for (i, r) in results.iter().enumerate() {
        println!(
            "  {}. {} ({} selections)",
            i + 1,
            get_field(r, "p.name"),
            get_field(r, "p.all_star_selections")
        );
    }

    // =========================================================================
    // Part 3: Pattern Matching
    // =========================================================================
    section("PART 3: PATTERN MATCHING");

    println!("\n--- Single-hop patterns ---");
    let results = graph
        .gql(
            r#"MATCH (p:player {name: 'Michael Jordan'})-[:played_for]->(t:team)
               RETURN t.name"#,
        )
        .unwrap();
    println!("Teams Michael Jordan played for:");
    for r in &results {
        println!("  {}", format_value(r));
    }

    println!("\n--- Relationship properties ---");
    let results = graph
        .gql(
            r#"MATCH (p:player {name: 'LeBron James'})-[r:played_for]->(t:team)
               RETURN t.name, r.start_year, r.end_year
               ORDER BY r.start_year"#,
        )
        .unwrap();
    println!("LeBron's career timeline:");
    for r in &results {
        println!(
            "  {} ({}-{})",
            get_field(r, "t.name"),
            get_field(r, "r.start_year"),
            get_field(r, "r.end_year")
        );
    }

    println!("\n--- Incoming relationships ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)-[:won_championship_with]->(t:team {name: 'Los Angeles Lakers'})
               RETURN DISTINCT p.name
               ORDER BY p.name"#,
        )
        .unwrap();
    println!("Lakers championship winners:");
    for r in &results {
        println!("  {}", format_value(r));
    }

    // =========================================================================
    // Part 4: Aggregations
    // =========================================================================
    section("PART 4: AGGREGATIONS");

    println!("\n--- count(*) ---");
    let results = graph
        .gql("MATCH (p:player) WHERE p.mvp_count > 0 RETURN count(*)")
        .unwrap();
    println!("MVP winners: {}", format_value(&results[0]));

    println!("\n--- sum(), avg(), min(), max() ---");
    let results = graph
        .gql("MATCH (p:player) RETURN avg(p.points_per_game)")
        .unwrap();
    println!("Average PPG: {}", format_value(&results[0]));

    let results = graph
        .gql("MATCH (p:player) RETURN max(p.all_star_selections)")
        .unwrap();
    println!("Max All-Star selections: {}", format_value(&results[0]));

    println!("\n--- GROUP BY ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)
               RETURN p.position, count(*) AS cnt, avg(p.points_per_game) AS avg_ppg
               GROUP BY p.position
               ORDER BY cnt DESC"#,
        )
        .unwrap();
    println!("Players by position:");
    for r in &results {
        println!(
            "  {}: {} players, {:.1} avg PPG",
            get_field(r, "p.position"),
            get_field(r, "cnt"),
            get_field(r, "avg_ppg")
        );
    }

    println!("\n--- collect() ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)-[:won_championship_with]->(t:team {name: 'Golden State Warriors'})
               RETURN collect(p.name) AS warriors_champs"#,
        )
        .unwrap();
    println!("Warriors champions: {}", format_value(&results[0]));

    // =========================================================================
    // Part 5: Subqueries and EXISTS
    // =========================================================================
    section("PART 5: SUBQUERIES AND EXISTS");

    println!("\n--- EXISTS subquery ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)
               WHERE EXISTS { (p)-[:won_championship_with]->() }
               RETURN p.name
               ORDER BY p.name"#,
        )
        .unwrap();
    println!("Championship winners:");
    let names: Vec<String> = results.iter().map(format_value).collect();
    println!("  {}", names.join(", "));

    println!("\n--- NOT EXISTS (players without rings) ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)
               WHERE NOT EXISTS { (p)-[:won_championship_with]->() }
               RETURN p.name"#,
        )
        .unwrap();
    println!("Players without championships:");
    for r in &results {
        println!("  {}", format_value(r));
    }

    // =========================================================================
    // Part 6: CASE Expressions
    // =========================================================================
    section("PART 6: CASE EXPRESSIONS");

    println!("\n--- CASE WHEN for classification ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)
               RETURN p.name,
                      CASE
                          WHEN p.points_per_game >= 27.0 THEN 'Elite Scorer'
                          WHEN p.points_per_game >= 22.0 THEN 'Star'
                          WHEN p.points_per_game >= 15.0 THEN 'Starter'
                          ELSE 'Role Player'
                      END AS tier
               ORDER BY p.points_per_game DESC
               LIMIT 6"#,
        )
        .unwrap();
    println!("Player scoring tiers:");
    for r in &results {
        println!("  {} - {}", get_field(r, "p.name"), get_field(r, "tier"));
    }

    // =========================================================================
    // Part 7: Introspection Functions
    // =========================================================================
    section("PART 7: INTROSPECTION");

    println!("\n--- id() and labels() ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)
               WHERE p.name = 'Stephen Curry'
               RETURN id(p) AS vertex_id, labels(p) AS vertex_labels, p.name"#,
        )
        .unwrap();
    for r in &results {
        println!(
            "  ID: {}, Labels: {}, Name: {}",
            get_field(r, "vertex_id"),
            get_field(r, "vertex_labels"),
            get_field(r, "p.name")
        );
    }

    // =========================================================================
    // Part 8: Query Parameters
    // =========================================================================
    section("PART 8: QUERY PARAMETERS");

    println!("\n--- Using $paramName syntax ---");
    let mut params = HashMap::new();
    params.insert("minPPG".to_string(), Value::Float(27.0));
    params.insert("maxPPG".to_string(), Value::Float(30.0));

    let results = graph
        .gql_with_params(
            r#"MATCH (p:player)
               WHERE p.points_per_game >= $minPPG AND p.points_per_game < $maxPPG
               RETURN p.name, p.points_per_game
               ORDER BY p.points_per_game DESC"#,
            &params,
        )
        .unwrap();
    println!("Players with PPG between 27.0 and 30.0:");
    for r in &results {
        println!(
            "  {} ({} PPG)",
            get_field(r, "p.name"),
            get_field(r, "p.points_per_game")
        );
    }

    let mut params = HashMap::new();
    params.insert(
        "playerName".to_string(),
        Value::String("Tim Duncan".to_string()),
    );
    let results = graph
        .gql_with_params(
            r#"MATCH (p:player)-[:won_championship_with]->(t:team)
               WHERE p.name = $playerName
               RETURN t.name"#,
            &params,
        )
        .unwrap();
    println!("\nTeams {} won with:", "Tim Duncan");
    for r in &results {
        println!("  {}", format_value(r));
    }

    // =========================================================================
    // Part 9: Advanced Features
    // =========================================================================
    section("PART 9: ADVANCED FEATURES");

    println!("\n--- Inline WHERE in patterns ---");
    let results = graph
        .gql(
            r#"MATCH (p:player WHERE p.mvp_count >= 2)
               RETURN p.name, p.mvp_count
               ORDER BY p.mvp_count DESC"#,
        )
        .unwrap();
    println!("Multi-MVP winners (inline WHERE):");
    for r in &results {
        println!(
            "  {} ({} MVPs)",
            get_field(r, "p.name"),
            get_field(r, "p.mvp_count")
        );
    }

    println!("\n--- LET clause ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)
               LET totalPlayers = COUNT(p)
               RETURN p.name, totalPlayers
               LIMIT 3"#,
        )
        .unwrap();
    println!("Players with total count (LET clause):");
    for r in &results {
        println!(
            "  {} (of {} total)",
            get_field(r, "p.name"),
            get_field(r, "totalPlayers")
        );
    }

    println!("\n--- Map literals ---");
    let results = graph
        .gql(
            r#"MATCH (p:player WHERE p.name = 'Kobe Bryant')
               RETURN {name: p.name, ppg: p.points_per_game, position: p.position} AS profile"#,
        )
        .unwrap();
    println!("Player profile as map:");
    for r in &results {
        println!("  {}", format_value(r));
    }

    println!("\n--- String concatenation ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)
               WHERE p.mvp_count > 0
               RETURN p.name || ' (' || p.mvp_count || ' MVPs)' AS formatted
               LIMIT 5"#,
        )
        .unwrap();
    println!("Formatted MVP info:");
    for r in &results {
        println!("  {}", format_value(r));
    }

    println!("\n--- List comprehensions ---");
    let results = graph
        .gql(
            r#"MATCH (p:player)
               LET names = COLLECT(p.name)
               LET upperNames = [n IN names | UPPER(n)]
               RETURN upperNames
               LIMIT 1"#,
        )
        .unwrap();
    println!("Uppercase player names (first 5):");
    if let Some(Value::Map(m)) = results.first() {
        if let Some(Value::List(list)) = m.values().next() {
            for name in list.iter().take(5) {
                println!("  {}", format_value(name));
            }
        }
    }

    // =========================================================================
    // Part 10: GQL Mutations
    // =========================================================================
    section("PART 10: GQL MUTATIONS");

    // Create a separate graph for mutation demos
    let mut_graph = Arc::new(Graph::new());

    println!("\n--- CREATE vertices and edges ---");
    mut_graph
        .gql("CREATE (:player {name: 'Demo Player', position: 'Guard', points_per_game: 20.5})")
        .unwrap();
    mut_graph
        .gql("CREATE (:team {name: 'Demo Team', city: 'Demo City'})")
        .unwrap();
    println!("Created demo player and team");

    let count = mut_graph.gql("MATCH (n) RETURN count(*)").unwrap();
    println!("Vertex count: {}", format_value(&count[0]));

    println!("\n--- SET property updates ---");
    mut_graph
        .gql("MATCH (p:player {name: 'Demo Player'}) SET p.all_star_selections = 5")
        .unwrap();
    let results = mut_graph
        .gql("MATCH (p:player {name: 'Demo Player'}) RETURN p.all_star_selections")
        .unwrap();
    println!("Updated all_star_selections: {}", format_value(&results[0]));

    println!("\n--- MERGE (upsert) ---");
    mut_graph
        .gql(
            r#"MERGE (p:player {name: 'Demo Player'})
               ON MATCH SET p.status = 'existing'
               ON CREATE SET p.status = 'new'"#,
        )
        .unwrap();
    let results = mut_graph
        .gql("MATCH (p:player {name: 'Demo Player'}) RETURN p.status")
        .unwrap();
    println!(
        "MERGE result (should be 'existing'): {}",
        format_value(&results[0])
    );

    mut_graph
        .gql(
            r#"MERGE (p:player {name: 'New Player'})
               ON MATCH SET p.status = 'existing'
               ON CREATE SET p.status = 'new'"#,
        )
        .unwrap();
    let results = mut_graph
        .gql("MATCH (p:player {name: 'New Player'}) RETURN p.status")
        .unwrap();
    println!(
        "MERGE result (should be 'new'): {}",
        format_value(&results[0])
    );

    println!("\n--- DELETE ---");
    let before = mut_graph.gql("MATCH (n) RETURN count(*)").unwrap();
    mut_graph
        .gql("MATCH (p:player {name: 'New Player'}) DELETE p")
        .unwrap();
    let after = mut_graph.gql("MATCH (n) RETURN count(*)").unwrap();
    println!(
        "Vertices before: {}, after DELETE: {}",
        format_value(&before[0]),
        format_value(&after[0])
    );

    println!("\n--- DETACH DELETE (with edges) ---");
    // Create vertices with edge in one statement
    mut_graph
        .gql("CREATE (:player {name: 'Hub Player'})-[:played_for {start_year: 2020}]->(:team {name: 'Hub Team'})")
        .unwrap();
    println!("Created player with edge to team");

    let before = mut_graph.gql("MATCH (n) RETURN count(*)").unwrap();
    mut_graph
        .gql("MATCH (p:player {name: 'Hub Player'}) DETACH DELETE p")
        .unwrap();
    let after = mut_graph.gql("MATCH (n) RETURN count(*)").unwrap();
    println!(
        "Vertices before: {}, after DETACH DELETE: {} (edge also removed)",
        format_value(&before[0]),
        format_value(&after[0])
    );

    // =========================================================================
    // Summary
    // =========================================================================
    section("SUMMARY: GQL FEATURES DEMONSTRATED");

    println!("\nData Loading:");
    println!("  GraphSON 3.0 import with embedded schema");
    println!("  graphson::from_str() for deserializing graph data");

    println!("\nBasic Queries:");
    println!("  MATCH (p:player) RETURN p.name");
    println!("  WHERE p.points_per_game >= 25.0");
    println!("  ORDER BY ... DESC LIMIT n");

    println!("\nPattern Matching:");
    println!("  (p)-[:played_for]->(t)");
    println!("  (p)-[r:played_for]->(t) -- with relationship variable");
    println!("  MATCH ... RETURN DISTINCT ...");

    println!("\nAggregations:");
    println!("  count(*), sum(), avg(), min(), max()");
    println!("  collect() for lists");
    println!("  GROUP BY position");

    println!("\nSubqueries:");
    println!("  WHERE EXISTS {{ (p)-[:won_championship_with]->() }}");
    println!("  WHERE NOT EXISTS {{ ... }}");

    println!("\nCASE Expressions:");
    println!("  CASE WHEN ... THEN ... ELSE ... END AS tier");

    println!("\nIntrospection:");
    println!("  id(p), labels(p)");

    println!("\nParameters:");
    println!("  $paramName with gql_with_params()");

    println!("\nAdvanced:");
    println!("  (p:player WHERE p.mvp_count >= 2) -- inline WHERE");
    println!("  LET totalPlayers = COUNT(p)");
    println!("  {{name: p.name, ppg: p.ppg}} -- map literals");
    println!("  p.name || ' (' || p.mvp_count || ')' -- string concat");
    println!("  [n IN names | UPPER(n)] -- list comprehensions");

    println!("\nMutations:");
    println!("  CREATE (:Label {{prop: value}})");
    println!("  SET p.prop = value");
    println!("  MERGE ... ON CREATE SET ... ON MATCH SET ...");
    println!("  DELETE / DETACH DELETE");

    println!("\n=== Example Complete ===");
}
