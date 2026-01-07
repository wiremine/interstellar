//! NBA Graph - GQL Query Example
//!
//! This example demonstrates querying a persistent graph database using GQL
//! (Graph Query Language). It opens the NBA database created by `nba_mmap_write`
//! and runs queries using the declarative GQL syntax.
//!
//! GQL provides a SQL-like syntax for graph pattern matching, making queries
//! more readable and accessible compared to the programmatic traversal API.
//!
//! Run first: `cargo run --features mmap --example nba_mmap_write`
//! Then run:  `cargo run --features mmap --example nba_gql`

use rustgremlin::graph::Graph;
use rustgremlin::storage::mmap::MmapGraph;
use rustgremlin::value::Value;
use std::sync::Arc;

const DB_PATH: &str = "examples/data/nba_graph.db";

fn main() {
    println!("=== NBA Graph Database - GQL Query Example ===\n");
    println!("Opening persistent database from {}...\n", DB_PATH);

    // =========================================================================
    // Open the existing MmapGraph database
    // =========================================================================
    let storage = match MmapGraph::open(DB_PATH) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("Error: Failed to open database: {}", e);
            eprintln!("\nMake sure you've run the write example first:");
            eprintln!("  cargo run --features mmap --example nba_mmap_write");
            std::process::exit(1);
        }
    };

    let graph = Graph::new(storage);
    let snapshot = graph.snapshot();

    // =========================================================================
    // SECTION 1: Basic Queries
    // =========================================================================
    print_section("1. BASIC QUERIES");

    // Query 1: Count all players
    print_query("Count all players");
    let results = snapshot.gql("MATCH (p:player) RETURN count(*)").unwrap();
    println!("Total players: {}", format_value(&results[0]));

    // Query 2: Count all teams
    print_query("Count all teams");
    let results = snapshot.gql("MATCH (t:team) RETURN count(*)").unwrap();
    println!("Total teams: {}", format_value(&results[0]));

    // Query 3: Find all Point Guards
    print_query("Find Point Guards");
    let results = snapshot
        .gql("MATCH (p:player) WHERE p.position = 'Point Guard' RETURN p.name")
        .unwrap();
    println!("Point Guards: {}", format_names(&results));

    // Query 4: Find Centers
    print_query("Find Centers");
    let results = snapshot
        .gql("MATCH (p:player) WHERE p.position = 'Center' RETURN p.name")
        .unwrap();
    println!("Centers: {}", format_names(&results));

    // Query 5: Find Eastern Conference teams
    print_query("Find Eastern Conference teams");
    let results = snapshot
        .gql("MATCH (t:team) WHERE t.conference = 'Eastern' RETURN t.name")
        .unwrap();
    println!("Eastern Conference: {}", format_names(&results));

    // Query 6: Find Western Conference teams
    print_query("Find Western Conference teams");
    let results = snapshot
        .gql("MATCH (t:team) WHERE t.conference = 'Western' RETURN t.name")
        .unwrap();
    println!("Western Conference: {}", format_names(&results));

    // =========================================================================
    // SECTION 2: Edge Traversal Queries
    // =========================================================================
    print_section("2. EDGE TRAVERSAL QUERIES");

    // Query 7: Find teams Michael Jordan played for
    print_query("Teams Michael Jordan played for");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'Michael Jordan'})-[:played_for]->(t:team)
            RETURN t.name
        "#,
        )
        .unwrap();
    println!("MJ's teams: {}", format_names(&results));

    // Query 8: Find teams LeBron James played for
    print_query("Teams LeBron James played for");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'LeBron James'})-[:played_for]->(t:team)
            RETURN t.name
        "#,
        )
        .unwrap();
    println!("LeBron's teams: {}", format_names(&results));

    // Query 9: Find players who played for the Lakers
    print_query("Players who played for the Lakers");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team {name: 'Los Angeles Lakers'})<-[:played_for]-(p:player)
            RETURN DISTINCT p.name
        "#,
        )
        .unwrap();
    println!("Lakers players: {}", format_names(&results));

    // Query 10: Find players who played for the Bulls
    print_query("Players who played for the Bulls");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team {name: 'Chicago Bulls'})<-[:played_for]-(p:player)
            RETURN DISTINCT p.name
        "#,
        )
        .unwrap();
    println!("Bulls players: {}", format_names(&results));

    // Query 11: Find Tim Duncan's championship teams
    print_query("Tim Duncan's championship teams");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'Tim Duncan'})-[:won_championship_with]->(t:team)
            RETURN t.name
        "#,
        )
        .unwrap();
    println!(
        "Tim Duncan's championship teams: {}",
        format_names(&results)
    );

    // Query 12: Find Lakers championship winners
    print_query("Lakers championship winners");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team {name: 'Los Angeles Lakers'})<-[:won_championship_with]-(p:player)
            RETURN DISTINCT p.name
        "#,
        )
        .unwrap();
    println!("Lakers champions: {}", format_names(&results));

    // =========================================================================
    // SECTION 3: WHERE Clause with Comparisons
    // =========================================================================
    print_section("3. WHERE CLAUSE - COMPARISONS");

    // Query 13: Find high scorers (25+ PPG)
    print_query("Players averaging 25+ PPG");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.points_per_game >= 25.0
            RETURN p.name, p.points_per_game
        "#,
        )
        .unwrap();
    println!("High scorers:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} PPG)",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.points_per_game").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 14: Find elite scorers (27+ PPG)
    print_query("Elite scorers (27+ PPG)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.points_per_game >= 27.0
            RETURN p.name, p.points_per_game
            ORDER BY p.points_per_game DESC
        "#,
        )
        .unwrap();
    println!("Elite scorers:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} PPG)",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.points_per_game").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 15: Find great rebounders (10+ RPG)
    print_query("Players averaging 10+ RPG");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.rebounds_per_game >= 10.0
            RETURN p.name, p.rebounds_per_game
        "#,
        )
        .unwrap();
    println!("Great rebounders:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} RPG)",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.rebounds_per_game").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 16: Find multi-MVP winners (3+ MVPs)
    print_query("Players with 3+ MVP awards");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.mvp_count >= 3
            RETURN p.name, p.mvp_count
            ORDER BY p.mvp_count DESC
        "#,
        )
        .unwrap();
    println!("Multi-MVP winners:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} MVPs)",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.mvp_count").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 4: WHERE Clause with IN Lists
    // =========================================================================
    print_section("4. WHERE CLAUSE - IN LISTS");

    // Query 17: Find all guards
    print_query("Find all guards (Point Guard or Shooting Guard)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.position IN ['Point Guard', 'Shooting Guard']
            RETURN p.name, p.position
        "#,
        )
        .unwrap();
    println!("Guards:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({})",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.position").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 18: Find teams from specific cities
    print_query("Teams from LA or New York");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            WHERE t.city IN ['Los Angeles', 'New York']
            RETURN t.name, t.city
        "#,
        )
        .unwrap();
    println!("LA/NY teams:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({})",
                format_value(map.get("t.name").unwrap_or(&Value::Null)),
                format_value(map.get("t.city").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 5: String Operations
    // =========================================================================
    print_section("5. STRING OPERATIONS");

    // Query 19: Find players whose name starts with 'Michael'
    print_query("Players whose name starts with 'Michael'");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.name STARTS WITH 'Michael'
            RETURN p.name
        "#,
        )
        .unwrap();
    println!("Players named Michael*: {}", format_names(&results));

    // Query 20: Find teams with 'Lakers' in name
    print_query("Teams with 'Lakers' in name");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            WHERE t.name CONTAINS 'Lakers'
            RETURN t.name
        "#,
        )
        .unwrap();
    println!("Lakers teams: {}", format_names(&results));

    // =========================================================================
    // SECTION 6: Complex WHERE with AND/OR
    // =========================================================================
    print_section("6. COMPLEX WHERE CLAUSES");

    // Query 21: Find elite scoring shooting guards
    print_query("Elite scoring shooting guards (25+ PPG)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.position = 'Shooting Guard' AND p.points_per_game >= 25.0
            RETURN p.name, p.points_per_game
        "#,
        )
        .unwrap();
    println!("Elite SGs:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} PPG)",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.points_per_game").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 22: Find GOAT candidates (2+ MVPs AND 2+ Finals MVPs)
    print_query("GOAT candidates (2+ MVPs AND 2+ Finals MVPs)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.mvp_count >= 2 AND p.finals_mvp_count >= 2
            RETURN p.name, p.mvp_count, p.finals_mvp_count
        "#,
        )
        .unwrap();
    println!("GOAT candidates:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} MVPs, {} Finals MVPs)",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.mvp_count").unwrap_or(&Value::Null)),
                format_value(map.get("p.finals_mvp_count").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 7: Multi-Hop Traversals
    // =========================================================================
    print_section("7. MULTI-HOP TRAVERSALS");

    // Query 23: Find teammates (players who shared a team)
    print_query("Find LeBron's teammates (players on same teams)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'LeBron James'})-[:played_for]->(t:team)<-[:played_for]-(teammate:player)
            RETURN DISTINCT teammate.name
        "#,
        )
        .unwrap();
    // Filter out LeBron himself
    let teammates: Vec<Value> = results
        .into_iter()
        .filter(|v| {
            if let Value::String(name) = v {
                name != "LeBron James"
            } else {
                true
            }
        })
        .collect();
    println!("LeBron's teammates: {}", format_names(&teammates));

    // Query 24: Find championship teammates
    print_query("Find Shaq's championship teammates");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'Shaquille O''Neal'})-[:won_championship_with]->(t:team)<-[:won_championship_with]-(teammate:player)
            RETURN DISTINCT teammate.name
        "#,
        )
        .unwrap();
    let teammates: Vec<Value> = results
        .into_iter()
        .filter(|v| {
            if let Value::String(name) = v {
                name != "Shaquille O'Neal"
            } else {
                true
            }
        })
        .collect();
    println!(
        "Shaq's championship teammates: {}",
        format_names(&teammates)
    );

    // =========================================================================
    // SECTION 8: Aggregation Queries
    // =========================================================================
    print_section("8. AGGREGATION QUERIES");

    // Query 25: Average PPG across all players
    print_query("Average points per game");
    let results = snapshot
        .gql("MATCH (p:player) RETURN avg(p.points_per_game)")
        .unwrap();
    if let Some(Value::Float(avg)) = results.first() {
        println!("Average PPG: {:.2}", avg);
    }

    // Query 26: Average RPG for Centers
    print_query("Average rebounds for Centers");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.position = 'Center'
            RETURN avg(p.rebounds_per_game)
        "#,
        )
        .unwrap();
    if let Some(Value::Float(avg)) = results.first() {
        println!("Average Center RPG: {:.2}", avg);
    }

    // Query 27: Total MVP awards
    print_query("Total MVP awards represented");
    let results = snapshot
        .gql("MATCH (p:player) RETURN sum(p.mvp_count)")
        .unwrap();
    println!("Total MVPs: {}", format_value(&results[0]));

    // Query 28: Min and Max PPG
    print_query("Min and Max PPG");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            RETURN min(p.points_per_game) AS min_ppg, max(p.points_per_game) AS max_ppg
        "#,
        )
        .unwrap();
    if let Some(Value::Map(map)) = results.first() {
        println!(
            "PPG range: {} to {}",
            format_value(map.get("min_ppg").unwrap_or(&Value::Null)),
            format_value(map.get("max_ppg").unwrap_or(&Value::Null))
        );
    }

    // Query 29: Count players by position
    print_query("Count unique cities");
    let results = snapshot
        .gql("MATCH (p:player) RETURN count(DISTINCT p.position)")
        .unwrap();
    println!("Unique positions: {}", format_value(&results[0]));

    // Query 30: Collect all team names
    print_query("Collect all team conferences");
    let results = snapshot
        .gql("MATCH (t:team) RETURN collect(DISTINCT t.conference)")
        .unwrap();
    if let Some(Value::List(confs)) = results.first() {
        println!("Conferences: {:?}", confs);
    }

    // =========================================================================
    // SECTION 9: ORDER BY and LIMIT
    // =========================================================================
    print_section("9. ORDER BY and LIMIT");

    // Query 31: Top 5 scorers
    print_query("Top 5 scorers by PPG");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            RETURN p.name, p.points_per_game
            ORDER BY p.points_per_game DESC
            LIMIT 5
        "#,
        )
        .unwrap();
    println!("Top 5 scorers:");
    for (i, r) in results.iter().enumerate() {
        if let Value::Map(map) = r {
            println!(
                "  {}. {} ({} PPG)",
                i + 1,
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.points_per_game").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 32: Top 3 MVP winners
    print_query("Top 3 MVP winners");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.mvp_count >= 1
            RETURN p.name, p.mvp_count
            ORDER BY p.mvp_count DESC
            LIMIT 3
        "#,
        )
        .unwrap();
    println!("Top MVP winners:");
    for (i, r) in results.iter().enumerate() {
        if let Value::Map(map) = r {
            println!(
                "  {}. {} ({} MVPs)",
                i + 1,
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.mvp_count").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 33: Dynasty teams (5+ championships)
    print_query("Dynasty teams (5+ championships)");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            WHERE t.championship_count >= 5
            RETURN t.name, t.championship_count
            ORDER BY t.championship_count DESC
        "#,
        )
        .unwrap();
    println!("Dynasty teams:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} titles)",
                format_value(map.get("t.name").unwrap_or(&Value::Null)),
                format_value(map.get("t.championship_count").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 34: Oldest teams
    print_query("Oldest teams (founded before 1950)");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            WHERE t.founded < 1950
            RETURN t.name, t.founded
            ORDER BY t.founded ASC
        "#,
        )
        .unwrap();
    println!("Oldest teams:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} (founded {})",
                format_value(map.get("t.name").unwrap_or(&Value::Null)),
                format_value(map.get("t.founded").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 10: Combined Queries
    // =========================================================================
    print_section("10. COMBINED QUERIES");

    // Query 35: Lakers vs Celtics rivalry players
    print_query("Lakers players");
    let lakers = snapshot
        .gql(
            r#"
            MATCH (t:team {name: 'Los Angeles Lakers'})<-[:played_for]-(p:player)
            RETURN DISTINCT p.name
        "#,
        )
        .unwrap();
    println!("Lakers players: {}", format_names(&lakers));

    print_query("Celtics players");
    let celtics = snapshot
        .gql(
            r#"
            MATCH (t:team {name: 'Boston Celtics'})<-[:played_for]-(p:player)
            RETURN DISTINCT p.name
        "#,
        )
        .unwrap();
    println!("Celtics players: {}", format_names(&celtics));

    // Query 36: 7-footers (84+ inches)
    print_query("7-footers (84+ inches)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.height_inches >= 84
            RETURN p.name, p.height_inches
            ORDER BY p.height_inches DESC
        "#,
        )
        .unwrap();
    println!("7-footers:");
    for r in &results {
        if let Value::Map(map) = r {
            let height = map
                .get("p.height_inches")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let feet = height / 12;
            let inches = height % 12;
            println!(
                "  {} ({}'{}\") ",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                feet,
                inches
            );
        }
    }

    // Query 37: All-Star leaders
    print_query("All-Star leaders (15+ selections)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.all_star_selections >= 15
            RETURN p.name, p.all_star_selections
            ORDER BY p.all_star_selections DESC
        "#,
        )
        .unwrap();
    println!("All-Star leaders:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} selections)",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.all_star_selections").unwrap_or(&Value::Null))
            );
        }
    }

    println!("\n=== GQL Query Example Complete ===");
    println!("\nThis example demonstrated:");
    println!("  - Basic node matching with labels");
    println!("  - Property filters in MATCH and WHERE clauses");
    println!("  - Edge traversals (outgoing and incoming)");
    println!("  - Multi-hop pattern matching");
    println!("  - Comparison operators (<, >, >=, <=, =, <>)");
    println!("  - Logical operators (AND, OR)");
    println!("  - IN lists for set membership");
    println!("  - String operations (STARTS WITH, CONTAINS)");
    println!("  - Aggregations (COUNT, SUM, AVG, MIN, MAX, COLLECT)");
    println!("  - DISTINCT for deduplication");
    println!("  - ORDER BY for sorting");
    println!("  - LIMIT for result pagination");
}

// =============================================================================
// Helper Functions
// =============================================================================

fn print_section(title: &str) {
    println!("\n{}", "=".repeat(70));
    println!("{}", title);
    println!("{}", "=".repeat(70));
}

fn print_query(description: &str) {
    println!("\n--- {} ---", description);
}

fn format_value(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => format!("{:.1}", f),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
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
        Value::Vertex(vid) => format!("v[{:?}]", vid),
        Value::Edge(eid) => format!("e[{:?}]", eid),
    }
}

fn format_names(results: &[Value]) -> String {
    if results.is_empty() {
        return "(none)".to_string();
    }
    results
        .iter()
        .map(format_value)
        .collect::<Vec<_>>()
        .join(", ")
}
