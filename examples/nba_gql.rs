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

    // =========================================================================
    // SECTION 11: EXISTS Subqueries (Plan 10)
    // =========================================================================
    print_section("11. EXISTS SUBQUERIES (Plan 10)");

    // Query 38: Find championship winners using EXISTS
    print_query("Find players who have won championships (EXISTS)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE EXISTS { (p)-[:won_championship_with]->() }
            RETURN p.name
            ORDER BY p.name
        "#,
        )
        .unwrap();
    println!("Championship winners: {}", format_names(&results));

    // Query 39: Find players WITHOUT championships using NOT EXISTS
    print_query("Find players without championships (NOT EXISTS)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE NOT EXISTS { (p)-[:won_championship_with]->() }
            RETURN p.name
            ORDER BY p.name
        "#,
        )
        .unwrap();
    println!("No rings: {}", format_names(&results));

    // Query 40: EXISTS with additional filters
    print_query("Find MVP winners who also won championships");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.mvp_count >= 1 AND EXISTS { (p)-[:won_championship_with]->() }
            RETURN p.name, p.mvp_count
            ORDER BY p.mvp_count DESC
        "#,
        )
        .unwrap();
    println!("MVP + Championship winners:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} MVPs)",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.mvp_count").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 41: Find players who played for dynasty teams
    // Note: EXISTS with inner WHERE not yet supported, so we use a multi-hop pattern
    print_query("Find players who played for teams with 10+ championships");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)-[:played_for]->(t:team)
            WHERE t.championship_count >= 10
            RETURN DISTINCT p.name
            ORDER BY p.name
        "#,
        )
        .unwrap();
    println!("Players on historic franchises: {}", format_names(&results));

    // =========================================================================
    // SECTION 12: GROUP BY Aggregation (Plan 10)
    // =========================================================================
    print_section("12. GROUP BY AGGREGATION (Plan 10)");

    // Query 42: Count players by position
    print_query("Count players by position");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            RETURN p.position, count(*) AS player_count
            GROUP BY p.position
        "#,
        )
        .unwrap();
    println!("Players by position:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: {}",
                format_value(map.get("p.position").unwrap_or(&Value::Null)),
                format_value(map.get("player_count").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 43: Average PPG by position
    print_query("Average PPG by position");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            RETURN p.position, avg(p.points_per_game) AS avg_ppg
            GROUP BY p.position
        "#,
        )
        .unwrap();
    println!("Average PPG by position:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: {:.1} PPG",
                format_value(map.get("p.position").unwrap_or(&Value::Null)),
                map.get("avg_ppg").and_then(|v| v.as_f64()).unwrap_or(0.0)
            );
        }
    }

    // Query 44: Count teams by conference
    print_query("Count teams by conference");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            RETURN t.conference, count(*) AS team_count
            GROUP BY t.conference
        "#,
        )
        .unwrap();
    println!("Teams by conference:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: {}",
                format_value(map.get("t.conference").unwrap_or(&Value::Null)),
                format_value(map.get("team_count").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 45: Total championships by conference
    print_query("Total championships by conference");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            RETURN t.conference, sum(t.championship_count) AS total_titles
            GROUP BY t.conference
        "#,
        )
        .unwrap();
    println!("Championships by conference:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: {} titles",
                format_value(map.get("t.conference").unwrap_or(&Value::Null)),
                format_value(map.get("total_titles").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 13: CASE Expressions (Plan 10)
    // =========================================================================
    print_section("13. CASE EXPRESSIONS (Plan 10)");

    // Query 46: Categorize players by scoring
    print_query("Categorize players by scoring level");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            RETURN p.name,
                   p.points_per_game,
                   CASE
                       WHEN p.points_per_game >= 27.0 THEN 'Elite'
                       WHEN p.points_per_game >= 20.0 THEN 'Star'
                       WHEN p.points_per_game >= 15.0 THEN 'Starter'
                       ELSE 'Role Player'
                   END AS scoring_tier
            ORDER BY p.points_per_game DESC
            LIMIT 10
        "#,
        )
        .unwrap();
    println!("Scoring tiers:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({:.1} PPG) - {}",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                map.get("p.points_per_game")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                format_value(map.get("scoring_tier").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 47: Categorize teams by era
    print_query("Categorize teams by founding era");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            RETURN t.name,
                   t.founded,
                   CASE
                       WHEN t.founded < 1950 THEN 'Original'
                       WHEN t.founded < 1970 THEN 'Expansion Era'
                       WHEN t.founded < 1990 THEN 'Modern Era'
                       ELSE 'Recent'
                   END AS era
            ORDER BY t.founded
        "#,
        )
        .unwrap();
    println!("Teams by era:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({}) - {}",
                format_value(map.get("t.name").unwrap_or(&Value::Null)),
                format_value(map.get("t.founded").unwrap_or(&Value::Null)),
                format_value(map.get("era").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 48: Dynasty status using CASE
    print_query("Dynasty status based on championships");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            RETURN t.name,
                   t.championship_count,
                   CASE
                       WHEN t.championship_count >= 10 THEN 'Legendary Dynasty'
                       WHEN t.championship_count >= 5 THEN 'Dynasty'
                       WHEN t.championship_count >= 1 THEN 'Champion'
                       ELSE 'No Titles'
                   END AS status
            ORDER BY t.championship_count DESC
            LIMIT 10
        "#,
        )
        .unwrap();
    println!("Dynasty status:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} titles) - {}",
                format_value(map.get("t.name").unwrap_or(&Value::Null)),
                format_value(map.get("t.championship_count").unwrap_or(&Value::Null)),
                format_value(map.get("status").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 14: COALESCE and Functions (Plan 10)
    // =========================================================================
    print_section("14. COALESCE AND FUNCTIONS (Plan 10)");

    // Query 49: COALESCE for null handling
    print_query("Use COALESCE for default values");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            RETURN p.name,
                   COALESCE(p.mvp_count, 0) AS mvps,
                   COALESCE(p.finals_mvp_count, 0) AS finals_mvps
            ORDER BY p.name
            LIMIT 5
        "#,
        )
        .unwrap();
    println!("MVP counts (with COALESCE defaults):");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: {} MVPs, {} Finals MVPs",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("mvps").unwrap_or(&Value::Null)),
                format_value(map.get("finals_mvps").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 50: String functions - UPPER
    print_query("String functions: UPPER");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            RETURN upper(t.name) AS team_upper, t.city
            LIMIT 5
        "#,
        )
        .unwrap();
    println!("Uppercase team names:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({})",
                format_value(map.get("team_upper").unwrap_or(&Value::Null)),
                format_value(map.get("t.city").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 51: Numeric functions - ABS, ROUND
    print_query("Numeric functions: ROUND");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            RETURN p.name,
                   p.points_per_game,
                   round(p.points_per_game) AS ppg_rounded
            ORDER BY p.points_per_game DESC
            LIMIT 5
        "#,
        )
        .unwrap();
    println!("Rounded PPG:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: {:.1} -> {}",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                map.get("p.points_per_game")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                format_value(map.get("ppg_rounded").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 52: Type conversion - toString
    print_query("Type conversion: toString");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.mvp_count >= 3
            RETURN p.name,
                   toString(p.mvp_count) AS mvp_string
        "#,
        )
        .unwrap();
    println!("MVP count as string:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: '{}' MVPs",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("mvp_string").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 15: Multi-Variable Patterns (Plan 10)
    // =========================================================================
    print_section("15. MULTI-VARIABLE PATTERNS (Plan 10)");

    // Query 53: Return multiple variables from pattern
    print_query("Player-Team relationships with both variables");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)-[:played_for]->(t:team)
            RETURN p.name AS player, t.name AS team, t.conference
            ORDER BY p.name
            LIMIT 10
        "#,
        )
        .unwrap();
    println!("Player-Team pairs:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} -> {} ({})",
                format_value(map.get("player").unwrap_or(&Value::Null)),
                format_value(map.get("team").unwrap_or(&Value::Null)),
                format_value(map.get("t.conference").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 54: Three-node pattern (teammates)
    print_query("Find teammates via shared team (3-node pattern)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p1:player)-[:played_for]->(t:team)<-[:played_for]-(p2:player)
            WHERE p1.name = 'Kobe Bryant' AND p1.name <> p2.name
            RETURN p1.name AS player1, t.name AS team, p2.name AS player2
            LIMIT 10
        "#,
        )
        .unwrap();
    println!("Kobe's teammates:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} and {} (both on {})",
                format_value(map.get("player1").unwrap_or(&Value::Null)),
                format_value(map.get("player2").unwrap_or(&Value::Null)),
                format_value(map.get("team").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 16: Edge Variables and Properties (Plan 10)
    // =========================================================================
    print_section("16. EDGE VARIABLES AND PROPERTIES (Plan 10)");

    // Query 55: Bind edge variable and access properties
    print_query("Access edge properties via edge variable");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)-[e:won_championship_with]->(t:team)
            RETURN p.name AS player, t.name AS team, e.ring_count AS rings
            ORDER BY e.ring_count DESC
            LIMIT 10
        "#,
        )
        .unwrap();
    println!("Championship details (with ring count from edge):");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} won {} ring(s) with {}",
                format_value(map.get("player").unwrap_or(&Value::Null)),
                format_value(map.get("rings").unwrap_or(&Value::Null)),
                format_value(map.get("team").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 56: Filter by edge property
    print_query("Filter by edge property (3+ rings with one team)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)-[e:won_championship_with]->(t:team)
            WHERE e.ring_count >= 3
            RETURN p.name AS player, t.name AS team, e.ring_count AS rings
            ORDER BY e.ring_count DESC
        "#,
        )
        .unwrap();
    println!("Players with 3+ rings with single team:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} - {} rings with {}",
                format_value(map.get("player").unwrap_or(&Value::Null)),
                format_value(map.get("rings").unwrap_or(&Value::Null)),
                format_value(map.get("team").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 17: Combined Plan 10 Features
    // =========================================================================
    print_section("17. COMBINED PLAN 10 FEATURES");

    // Query 57: EXISTS + GROUP BY
    print_query("Count championship winners by position (EXISTS + GROUP BY)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE EXISTS { (p)-[:won_championship_with]->() }
            RETURN p.position, count(*) AS champions
            GROUP BY p.position
        "#,
        )
        .unwrap();
    println!("Champions by position:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: {}",
                format_value(map.get("p.position").unwrap_or(&Value::Null)),
                format_value(map.get("champions").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 58: CASE expressions with player categorization
    // Note: CASE in GROUP BY not yet supported, so we show individual categorizations
    print_query("Categorize players by scoring tier (CASE expression)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            RETURN p.name,
                   CASE
                       WHEN p.points_per_game >= 25.0 THEN 'Elite'
                       WHEN p.points_per_game >= 18.0 THEN 'Star'
                       ELSE 'Other'
                   END AS tier
            ORDER BY p.points_per_game DESC
            LIMIT 8
        "#,
        )
        .unwrap();
    println!("Players by scoring tier:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: {}",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("tier").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 59: Multi-variable + EXISTS + aggregation
    print_query("Players and their championship team count");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)-[:won_championship_with]->(t:team)
            RETURN p.name AS player, count(DISTINCT t) AS team_count
            GROUP BY p.name
            ORDER BY team_count DESC
            LIMIT 5
        "#,
        )
        .unwrap();
    println!("Players by number of championship teams:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: {} team(s)",
                format_value(map.get("player").unwrap_or(&Value::Null)),
                format_value(map.get("team_count").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 60: Full combined query
    print_query("Elite guards who won championships (all features)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.position IN ['Point Guard', 'Shooting Guard']
              AND p.points_per_game >= 20.0
              AND EXISTS { (p)-[:won_championship_with]->() }
            RETURN p.name,
                   p.position,
                   p.points_per_game,
                   CASE
                       WHEN p.mvp_count >= 1 THEN 'MVP Winner'
                       ELSE 'No MVP'
                   END AS mvp_status
            ORDER BY p.points_per_game DESC
        "#,
        )
        .unwrap();
    println!("Elite championship guards:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({}, {:.1} PPG) - {}",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.position").unwrap_or(&Value::Null)),
                map.get("p.points_per_game")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                format_value(map.get("mvp_status").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 18: Introspection Functions (Plan 11)
    // =========================================================================
    print_section("18. INTROSPECTION FUNCTIONS (Plan 11)");

    // Query 61: Get element metadata with id() and labels()
    print_query("Element introspection - id() and labels()");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.name = 'Michael Jordan'
            RETURN id(p) AS vertex_id, labels(p) AS vertex_labels, p.name
        "#,
        )
        .unwrap();
    println!("Michael Jordan vertex info:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  ID: {}, Labels: {}, Name: {}",
                format_value(map.get("vertex_id").unwrap_or(&Value::Null)),
                format_value(map.get("vertex_labels").unwrap_or(&Value::Null)),
                format_value(map.get("p.name").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 62: Get all properties of a vertex with properties()
    print_query("Get all properties of a player with properties()");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.name = 'LeBron James'
            RETURN properties(p) AS all_props
        "#,
        )
        .unwrap();
    println!("LeBron James properties:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}",
                format_value(map.get("all_props").unwrap_or(&Value::Null))
            );
        } else {
            println!("  {}", format_value(r));
        }
    }

    // Query 63: Get edge type with type()
    print_query("Edge type introspection with type()");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'Michael Jordan'})-[e]->(t:team)
            RETURN p.name AS player, type(e) AS relationship, t.name AS team
        "#,
        )
        .unwrap();
    println!("Michael Jordan's relationships:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} --[{}]--> {}",
                format_value(map.get("player").unwrap_or(&Value::Null)),
                format_value(map.get("relationship").unwrap_or(&Value::Null)),
                format_value(map.get("team").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 64: Get edge properties
    print_query("Edge properties with properties()");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)-[e:played_for]->(t:team)
            WHERE p.name = 'LeBron James'
            RETURN t.name AS team, properties(e) AS edge_props
        "#,
        )
        .unwrap();
    println!("LeBron's career history with edge properties:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} - {}",
                format_value(map.get("team").unwrap_or(&Value::Null)),
                format_value(map.get("edge_props").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 65: Combine id() with aggregation
    print_query("Vertex IDs with aggregation");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            WHERE t.conference = 'Eastern'
            RETURN id(t) AS team_id, t.name AS team_name
            ORDER BY team_id
            LIMIT 5
        "#,
        )
        .unwrap();
    println!("First 5 Eastern Conference teams by ID:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  ID {}: {}",
                format_value(map.get("team_id").unwrap_or(&Value::Null)),
                format_value(map.get("team_name").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 66: Show relationship types using type()
    print_query("Shaq's relationships with type()");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'Shaquille O''Neal'})-[e]->(t:team)
            RETURN type(e) AS rel_type, t.name AS team
            ORDER BY rel_type, t.name
        "#,
        )
        .unwrap();
    println!("Shaq's relationships:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} -> {} ({})",
                format_value(map.get("rel_type").unwrap_or(&Value::Null)),
                format_value(map.get("team").unwrap_or(&Value::Null)),
                "Shaq"
            );
        }
    }

    // =========================================================================
    // SECTION 19: Additional Predicate Queries (Parity with traversal API)
    // =========================================================================
    print_section("19. ADDITIONAL PREDICATE QUERIES");

    // Query 67: Teams founded before 1970 (equivalent to traversal Query 17)
    print_query("Teams founded before 1970");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            WHERE t.founded < 1970
            RETURN t.name, t.founded
            ORDER BY t.founded ASC
        "#,
        )
        .unwrap();
    println!("Pre-1970 teams:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} (founded {})",
                format_value(map.get("t.name").unwrap_or(&Value::Null)),
                format_value(map.get("t.founded").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 68: Teams founded between 1980 and 2000 (equivalent to traversal Query 18)
    print_query("Teams founded between 1980 and 2000");
    let results = snapshot
        .gql(
            r#"
            MATCH (t:team)
            WHERE t.founded >= 1980 AND t.founded <= 2000
            RETURN t.name, t.founded
            ORDER BY t.founded ASC
        "#,
        )
        .unwrap();
    println!("Expansion era teams (1980-2000):");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} (founded {})",
                format_value(map.get("t.name").unwrap_or(&Value::Null)),
                format_value(map.get("t.founded").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 69: Passing big men - Centers with 3+ APG (equivalent to traversal Query 43)
    print_query("Passing big men (Centers with 3+ APG)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.position = 'Center' AND p.assists_per_game >= 3.0
            RETURN p.name, p.assists_per_game
            ORDER BY p.assists_per_game DESC
        "#,
        )
        .unwrap();
    println!("Passing Centers:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({} APG)",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.assists_per_game").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 70: Most decorated players - MVP, Finals MVP, AND championships
    // (equivalent to traversal Query 40)
    print_query("Most decorated players (MVP + Finals MVP + Championships)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE p.mvp_count >= 1 
              AND p.finals_mvp_count >= 1 
              AND EXISTS { (p)-[:won_championship_with]->() }
            RETURN p.name, p.mvp_count, p.finals_mvp_count
            ORDER BY p.mvp_count DESC
        "#,
        )
        .unwrap();
    println!("Triple-crown players (MVP + Finals MVP + Ring):");
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

    // Query 71: Championship drought - ringless players on winning franchises
    // (equivalent to traversal Query 44)
    // Note: In our dataset, Charles Barkley is the only ringless player, but he
    // has no played_for edges in the fixture data, so the result should be "(none)".
    // We use a two-step approach to demonstrate the query pattern.
    print_query("Ringless players on championship franchises");

    // First find ringless players
    let ringless = snapshot
        .gql(
            r#"
            MATCH (p:player)
            WHERE NOT EXISTS { (p)-[:won_championship_with]->() }
            RETURN p.name
        "#,
        )
        .unwrap();
    println!("  Players without rings: {}", format_names(&ringless));

    // Then check if they played for championship teams
    // Note: Charles Barkley has no played_for edges in our fixture
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'Charles Barkley'})-[:played_for]->(t:team)
            RETURN t.name
        "#,
        )
        .unwrap();
    println!(
        "  Charles Barkley's teams: {}",
        if results.is_empty() {
            "(no played_for edges in fixture)".to_string()
        } else {
            format_names(&results)
        }
    );

    // Query 72: GOAT candidates with detailed stats (equivalent to traversal Query 45)
    print_query("GOAT candidates (2+ MVPs AND 2+ Finals MVPs) with ring count");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)-[e:won_championship_with]->(t:team)
            WHERE p.mvp_count >= 2 AND p.finals_mvp_count >= 2
            RETURN p.name, 
                   p.mvp_count, 
                   p.finals_mvp_count, 
                   sum(e.ring_count) AS total_rings
            GROUP BY p.name, p.mvp_count, p.finals_mvp_count
            ORDER BY total_rings DESC
        "#,
        )
        .unwrap();
    println!("GOAT candidates:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {}: {} MVPs, {} Finals MVPs, {} rings",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.mvp_count").unwrap_or(&Value::Null)),
                format_value(map.get("p.finals_mvp_count").unwrap_or(&Value::Null)),
                format_value(map.get("total_rings").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 20: Union-Style Queries
    // =========================================================================
    print_section("20. UNION-STYLE QUERIES");

    // Query 73: Shaq's all team connections (played_for + won_championship_with)
    // GQL doesn't have UNION yet, so we use two queries
    // This demonstrates how to achieve similar results to traversal API's union()
    print_query("Shaq's teams - played for");
    let played_for = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'Shaquille O''Neal'})-[:played_for]->(t:team)
            RETURN t.name AS team
        "#,
        )
        .unwrap();
    println!("Teams Shaq played for: {}", format_names(&played_for));

    print_query("Shaq's teams - won championships with");
    let won_with = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'Shaquille O''Neal'})-[:won_championship_with]->(t:team)
            RETURN t.name AS team
        "#,
        )
        .unwrap();
    println!(
        "Teams Shaq won championships with: {}",
        format_names(&won_with)
    );

    // Combine programmatically (simulating UNION)
    let mut all_shaq_teams: Vec<String> = played_for
        .iter()
        .filter_map(|v| {
            if let Value::Map(map) = v {
                map.get("team").map(format_value)
            } else if let Value::String(s) = v {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();
    for v in &won_with {
        if let Value::Map(map) = v {
            if let Some(team) = map.get("team").map(format_value) {
                if !all_shaq_teams.contains(&team) {
                    all_shaq_teams.push(team);
                }
            }
        } else if let Value::String(s) = v {
            if !all_shaq_teams.contains(s) {
                all_shaq_teams.push(s.clone());
            }
        }
    }
    println!(
        "All Shaq's team connections (union): {}",
        all_shaq_teams.join(", ")
    );

    // =========================================================================
    // SECTION 21: Complex Multi-Hop Patterns
    // =========================================================================
    print_section("21. COMPLEX MULTI-HOP PATTERNS");

    // Query 74: Extended teammate network (teammates of teammates)
    // Equivalent to traversal Query 33
    print_query("Kobe's extended network (teammates of teammates)");
    let results = snapshot
        .gql(
            r#"
            MATCH (p1:player {name: 'Kobe Bryant'})-[:played_for]->(t1:team)<-[:played_for]-(teammate:player)-[:played_for]->(t2:team)<-[:played_for]-(extended:player)
            RETURN DISTINCT extended.name
            ORDER BY extended.name
        "#,
        )
        .unwrap();
    println!(
        "Kobe's extended network ({} players): {}",
        results.len(),
        if results.len() > 10 {
            format!(
                "{} ...",
                results
                    .iter()
                    .take(10)
                    .map(format_value)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            format_names(&results)
        }
    );

    // Query 75: Multi-team championship winners
    // Players who won championships with more than one team
    print_query("Multi-team championship winners");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)-[:won_championship_with]->(t:team)
            RETURN p.name, count(DISTINCT t) AS team_count, collect(t.name) AS teams
            GROUP BY p.name
            ORDER BY team_count DESC
        "#,
        )
        .unwrap();
    println!("Players by championship teams:");
    for r in &results {
        if let Value::Map(map) = r {
            let count = map.get("team_count").and_then(|v| v.as_i64()).unwrap_or(0);
            if count > 1 {
                println!(
                    "  {} ({} teams): {}",
                    format_value(map.get("p.name").unwrap_or(&Value::Null)),
                    count,
                    format_value(map.get("teams").unwrap_or(&Value::Null))
                );
            }
        }
    }

    // Query 76: Find championship teammates (Shaq's championship co-winners)
    // Equivalent to traversal Query 24
    print_query("Shaq's championship teammates");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player {name: 'Shaquille O''Neal'})-[:won_championship_with]->(t:team)<-[:won_championship_with]-(teammate:player)
            WHERE teammate.name <> 'Shaquille O''Neal'
            RETURN DISTINCT teammate.name, t.name AS team
            ORDER BY teammate.name
        "#,
        )
        .unwrap();
    println!("Shaq's championship teammates:");
    for r in &results {
        if let Value::Map(map) = r {
            println!(
                "  {} ({})",
                format_value(map.get("teammate.name").unwrap_or(&Value::Null)),
                format_value(map.get("team").unwrap_or(&Value::Null))
            );
        }
    }

    // =========================================================================
    // SECTION 22: Summary Statistics (GQL equivalents)
    // =========================================================================
    print_section("22. SUMMARY STATISTICS");

    // Query 77: Total MVP awards in dataset
    print_query("Total MVP awards represented");
    let results = snapshot
        .gql("MATCH (p:player) RETURN sum(p.mvp_count) AS total_mvps")
        .unwrap();
    if let Some(Value::Map(map)) = results.first() {
        println!(
            "Total MVP awards: {}",
            format_value(map.get("total_mvps").unwrap_or(&Value::Null))
        );
    }

    // Query 78: Average stats across all players
    print_query("Average player stats");
    let results = snapshot
        .gql(
            r#"
            MATCH (p:player)
            RETURN avg(p.points_per_game) AS avg_ppg,
                   avg(p.rebounds_per_game) AS avg_rpg,
                   avg(p.assists_per_game) AS avg_apg
        "#,
        )
        .unwrap();
    if let Some(Value::Map(map)) = results.first() {
        println!(
            "  Average PPG: {:.2}",
            map.get("avg_ppg").and_then(|v| v.as_f64()).unwrap_or(0.0)
        );
        println!(
            "  Average RPG: {:.2}",
            map.get("avg_rpg").and_then(|v| v.as_f64()).unwrap_or(0.0)
        );
        println!(
            "  Average APG: {:.2}",
            map.get("avg_apg").and_then(|v| v.as_f64()).unwrap_or(0.0)
        );
    }

    // Query 79: All-Star leaders (15+ selections)
    // Equivalent to summary in traversal example
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
                "  {}: {} All-Star selections",
                format_value(map.get("p.name").unwrap_or(&Value::Null)),
                format_value(map.get("p.all_star_selections").unwrap_or(&Value::Null))
            );
        }
    }

    // Query 80: Edge counts by relationship type
    print_query("Relationship counts by type");
    let played_for_count = snapshot
        .gql("MATCH ()-[e:played_for]->() RETURN count(e) AS count")
        .unwrap();
    let champ_count = snapshot
        .gql("MATCH ()-[e:won_championship_with]->() RETURN count(e) AS count")
        .unwrap();
    println!("Edge counts:");
    if let Some(Value::Map(map)) = played_for_count.first() {
        println!(
            "  played_for: {}",
            format_value(map.get("count").unwrap_or(&Value::Null))
        );
    }
    if let Some(Value::Map(map)) = champ_count.first() {
        println!(
            "  won_championship_with: {}",
            format_value(map.get("count").unwrap_or(&Value::Null))
        );
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
    println!("\n  Plan 10 Features:");
    println!("  - EXISTS / NOT EXISTS subqueries");
    println!("  - GROUP BY for grouped aggregation");
    println!("  - CASE expressions for conditional logic");
    println!("  - COALESCE for null handling");
    println!("  - String functions (UPPER, LOWER, etc.)");
    println!("  - Numeric functions (ROUND, ABS, etc.)");
    println!("  - Type conversion (toString, toInteger, etc.)");
    println!("  - Multi-variable pattern binding");
    println!("  - Edge variable binding and property access");
    println!("\n  Plan 11 Features:");
    println!("  - id() for internal element IDs");
    println!("  - labels() for vertex labels");
    println!("  - type() for edge types");
    println!("  - properties() for all properties as map");
    println!("\n  Additional Features (Parity with Traversal API):");
    println!("  - Complex predicate queries (founded before/between dates)");
    println!("  - Passing big men (position + stats filter)");
    println!("  - Most decorated players (multiple criteria + EXISTS)");
    println!("  - GOAT candidates with ring count aggregation");
    println!("  - Union-style queries (combining relationship types)");
    println!("  - Multi-hop patterns (extended networks, teammates of teammates)");
    println!("  - Summary statistics (totals, averages, counts)");
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
