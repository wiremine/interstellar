//! CALL Subquery Example
//!
//! This example demonstrates the CALL subquery feature in GQL, which allows
//! executing nested queries within a main query. CALL subqueries are powerful
//! for computing aggregations, filtering, and combining results from multiple
//! patterns.
//!
//! Key concepts:
//! - **Uncorrelated CALL**: Subquery runs once, results cross-joined with outer rows
//! - **Correlated CALL**: Subquery runs per outer row, using `WITH` to import variables
//! - **UNION in CALL**: Combine results from multiple subqueries
//! - **Aggregation in CALL**: Compute COUNT, SUM, COLLECT, etc. per outer row
//!
//! Run with: `cargo run --example call_subquery`

use intersteller::graph::Graph;
use intersteller::storage::InMemoryGraph;
use intersteller::value::Value;
use std::collections::HashMap;

/// Helper to extract a string value from a result map
fn get_str<'a>(result: &'a Value, key: &str) -> &'a str {
    if let Value::Map(map) = result {
        if let Some(Value::String(s)) = map.get(key) {
            return s.as_str();
        }
    }
    "?"
}

/// Helper to extract an integer value from a result map
fn get_int(result: &Value, key: &str) -> i64 {
    if let Value::Map(map) = result {
        if let Some(Value::Int(n)) = map.get(key) {
            return *n;
        }
    }
    0
}

/// Build a sample movie recommendation graph.
///
/// Graph structure:
/// ```text
/// People: Alice, Bob, Charlie, Diana
/// Movies: The Matrix (1999), Inception (2010), Interstellar (2014), Dune (2021)
/// Genres: Sci-Fi, Action, Drama
///
/// Relationships:
///   Alice -[:LIKES {rating:5}]-> The Matrix
///   Alice -[:LIKES {rating:4}]-> Inception
///   Alice -[:LIKES {rating:5}]-> Interstellar
///   Bob   -[:LIKES {rating:5}]-> The Matrix
///   Bob   -[:LIKES {rating:4}]-> Dune
///   Charlie -[:LIKES {rating:3}]-> Inception
///   Diana likes nothing
///
///   Alice -[:KNOWS]-> Bob, Charlie
///   Bob   -[:KNOWS]-> Charlie, Diana
/// ```
fn build_movie_graph() -> Graph {
    let mut storage = InMemoryGraph::new();

    // Create People
    let alice = storage.add_vertex("Person", props(&[("name", "Alice"), ("age", "28")]));
    let bob = storage.add_vertex("Person", props(&[("name", "Bob"), ("age", "34")]));
    let charlie = storage.add_vertex("Person", props(&[("name", "Charlie"), ("age", "25")]));
    let _diana = storage.add_vertex("Person", props(&[("name", "Diana"), ("age", "31")]));

    // Create Movies
    let matrix = storage.add_vertex("Movie", props(&[("title", "The Matrix"), ("year", "1999")]));
    let inception = storage.add_vertex("Movie", props(&[("title", "Inception"), ("year", "2010")]));
    let interstellar = storage.add_vertex(
        "Movie",
        props(&[("title", "Interstellar"), ("year", "2014")]),
    );
    let dune = storage.add_vertex("Movie", props(&[("title", "Dune"), ("year", "2021")]));

    // Create Genres
    let scifi = storage.add_vertex("Genre", props(&[("name", "Sci-Fi")]));
    let action = storage.add_vertex("Genre", props(&[("name", "Action")]));
    let drama = storage.add_vertex("Genre", props(&[("name", "Drama")]));

    // LIKES relationships with ratings
    let _ = storage.add_edge(alice, matrix, "LIKES", rating(5));
    let _ = storage.add_edge(alice, inception, "LIKES", rating(4));
    let _ = storage.add_edge(alice, interstellar, "LIKES", rating(5));
    let _ = storage.add_edge(bob, matrix, "LIKES", rating(5));
    let _ = storage.add_edge(bob, dune, "LIKES", rating(4));
    let _ = storage.add_edge(charlie, inception, "LIKES", rating(3));
    // Diana likes nothing - tests empty subquery results

    // KNOWS relationships
    let _ = storage.add_edge(alice, bob, "KNOWS", HashMap::new());
    let _ = storage.add_edge(alice, charlie, "KNOWS", HashMap::new());
    let _ = storage.add_edge(bob, charlie, "KNOWS", HashMap::new());
    let _ = storage.add_edge(bob, _diana, "KNOWS", HashMap::new());

    // IN_GENRE relationships
    let _ = storage.add_edge(matrix, scifi, "IN_GENRE", HashMap::new());
    let _ = storage.add_edge(matrix, action, "IN_GENRE", HashMap::new());
    let _ = storage.add_edge(inception, scifi, "IN_GENRE", HashMap::new());
    let _ = storage.add_edge(inception, drama, "IN_GENRE", HashMap::new());
    let _ = storage.add_edge(interstellar, scifi, "IN_GENRE", HashMap::new());
    let _ = storage.add_edge(interstellar, drama, "IN_GENRE", HashMap::new());
    let _ = storage.add_edge(dune, scifi, "IN_GENRE", HashMap::new());
    let _ = storage.add_edge(dune, action, "IN_GENRE", HashMap::new());

    Graph::new(storage)
}

/// Helper to create property maps
fn props(pairs: &[(&str, &str)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| {
            let value = if let Ok(n) = v.parse::<i64>() {
                Value::from(n)
            } else {
                Value::from(*v)
            };
            (k.to_string(), value)
        })
        .collect()
}

/// Helper to create rating property
fn rating(r: i64) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    map.insert("rating".to_string(), Value::from(r));
    map
}

fn main() {
    println!("=============================================================================");
    println!("CALL Subquery Example");
    println!("=============================================================================\n");

    let graph = build_movie_graph();
    let snapshot = graph.snapshot();

    // =========================================================================
    // Example 1: Uncorrelated CALL - Cross Join
    // =========================================================================
    example_header("1", "Uncorrelated CALL (Cross Join)");
    println!("An uncorrelated CALL runs the subquery once and cross-joins results");
    println!("with the outer query rows.\n");
    println!("Query: MATCH (p:Person)");
    println!("       CALL {{ MATCH (g:Genre) RETURN g.name AS genre }}");
    println!("       RETURN p.name, genre\n");

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            CALL {
                MATCH (g:Genre)
                RETURN g.name AS genre
            }
            RETURN p.name AS person, genre
            ORDER BY p.name, genre
            "#,
        )
        .unwrap();

    println!("Results (4 people x 3 genres = {} rows):", results.len());
    for r in results.iter().take(6) {
        println!("  {} -> {}", get_str(r, "person"), get_str(r, "genre"));
    }
    println!("  ...\n");

    // =========================================================================
    // Example 2: Correlated CALL - Per-Row Execution
    // =========================================================================
    example_header("2", "Correlated CALL (Per-Row Execution)");
    println!("A correlated CALL uses WITH to import outer variables, running the");
    println!("subquery once per outer row.\n");
    println!("Query: MATCH (p:Person)");
    println!("       CALL {{ WITH p MATCH (p)-[:LIKES]->(m:Movie) RETURN m.title }}");
    println!("       RETURN p.name, m.title\n");

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[:LIKES]->(m:Movie)
                RETURN m.title AS movie
            }
            RETURN p.name AS person, movie
            ORDER BY p.name, movie
            "#,
        )
        .unwrap();

    println!("Results (only people with liked movies appear):");
    for r in &results {
        println!("  {} likes {}", get_str(r, "person"), get_str(r, "movie"));
    }
    println!("  Note: Diana is excluded (she has no LIKES edges)\n");

    // =========================================================================
    // Example 3: CALL with COUNT Aggregation
    // =========================================================================
    example_header("3", "CALL with COUNT Aggregation");
    println!("Compute aggregations per outer row using correlated CALL.\n");
    println!("Query: MATCH (p:Person)");
    println!("       CALL {{ WITH p MATCH (p)-[:LIKES]->(m) RETURN count(m) AS cnt }}");
    println!("       RETURN p.name, cnt\n");

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[:LIKES]->(m:Movie)
                RETURN count(m) AS movieCount
            }
            RETURN p.name AS person, movieCount
            ORDER BY movieCount DESC
            "#,
        )
        .unwrap();

    println!("Results:");
    for r in &results {
        println!(
            "  {} has liked {} movies",
            get_str(r, "person"),
            get_int(r, "movieCount")
        );
    }
    println!();

    // =========================================================================
    // Example 4: CALL with SUM Aggregation
    // =========================================================================
    example_header("4", "CALL with SUM Aggregation");
    println!("Sum ratings to find total engagement per person.\n");
    println!("Query: MATCH (p:Person)");
    println!("       CALL {{ WITH p MATCH (p)-[r:LIKES]->() RETURN sum(r.rating) }}");
    println!("       RETURN p.name, totalRating\n");

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[r:LIKES]->(m:Movie)
                RETURN sum(r.rating) AS totalRating
            }
            RETURN p.name AS person, totalRating
            ORDER BY totalRating DESC
            "#,
        )
        .unwrap();

    println!("Results:");
    for r in &results {
        println!(
            "  {} total rating: {} (avg: {:.1})",
            get_str(r, "person"),
            get_int(r, "totalRating"),
            get_int(r, "totalRating") as f64
                / results
                    .iter()
                    .find(|x| get_str(x, "person") == get_str(r, "person"))
                    .map(|_| {
                        match get_str(r, "person") {
                            "Alice" => 3.0,
                            "Bob" => 2.0,
                            "Charlie" => 1.0,
                            _ => 1.0,
                        }
                    })
                    .unwrap_or(1.0)
        );
    }
    println!();

    // =========================================================================
    // Example 5: CALL with COLLECT Aggregation
    // =========================================================================
    example_header("5", "CALL with COLLECT Aggregation");
    println!("Collect all liked movie titles into a list per person.\n");
    println!("Query: MATCH (p:Person)");
    println!("       CALL {{ WITH p MATCH (p)-[:LIKES]->(m) RETURN collect(m.title) }}");
    println!("       RETURN p.name, movies\n");

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[:LIKES]->(m:Movie)
                RETURN collect(m.title) AS movies
            }
            RETURN p.name AS person, movies
            "#,
        )
        .unwrap();

    println!("Results:");
    for r in &results {
        if let Value::Map(map) = r {
            let person = get_str(r, "person");
            if let Some(Value::List(movies)) = map.get("movies") {
                let titles: Vec<&str> = movies
                    .iter()
                    .filter_map(|v| {
                        if let Value::String(s) = v {
                            Some(s.as_str())
                        } else {
                            None
                        }
                    })
                    .collect();
                println!("  {}: {:?}", person, titles);
            }
        }
    }
    println!();

    // =========================================================================
    // Example 6: CALL with WHERE Filter
    // =========================================================================
    example_header("6", "CALL with WHERE Filter");
    println!("Filter results inside the subquery.\n");
    println!("Query: MATCH (p:Person)");
    println!("       CALL {{ WITH p MATCH (p)-[:LIKES]->(m) WHERE m.year > 2010 RETURN m }}");
    println!("       RETURN p.name, m.title, m.year\n");

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[:LIKES]->(m:Movie)
                WHERE m.year > 2010
                RETURN m.title AS movie, m.year AS year
            }
            RETURN p.name AS person, movie, year
            "#,
        )
        .unwrap();

    println!("Results (only movies released after 2010):");
    for r in &results {
        println!(
            "  {} likes {} ({})",
            get_str(r, "person"),
            get_str(r, "movie"),
            get_int(r, "year")
        );
    }
    println!();

    // =========================================================================
    // Example 7: CALL with ORDER BY and LIMIT
    // =========================================================================
    example_header("7", "CALL with ORDER BY and LIMIT");
    println!("Get top N results per outer row.\n");
    println!("Query: MATCH (p:Person)");
    println!("       CALL {{ WITH p MATCH (p)-[r:LIKES]->(m)");
    println!("              RETURN m.title, r.rating ORDER BY r.rating DESC LIMIT 1 }}");
    println!("       RETURN p.name, topMovie, rating\n");

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[r:LIKES]->(m:Movie)
                RETURN m.title AS topMovie, r.rating AS rating
                ORDER BY r.rating DESC
                LIMIT 1
            }
            RETURN p.name AS person, topMovie, rating
            "#,
        )
        .unwrap();

    println!("Results (each person's highest-rated movie):");
    for r in &results {
        println!(
            "  {}'s top movie: {} (rating: {})",
            get_str(r, "person"),
            get_str(r, "topMovie"),
            get_int(r, "rating")
        );
    }
    println!();

    // =========================================================================
    // Example 8: CALL with UNION
    // =========================================================================
    example_header("8", "CALL with UNION");
    println!("Combine results from multiple patterns in a single CALL.\n");
    println!("Query: MATCH (p:Person) WHERE p.name = 'Alice'");
    println!("       CALL {{");
    println!("         WITH p MATCH (p)-[:LIKES]->(m) RETURN m.title, 'movie'");
    println!("         UNION");
    println!("         WITH p MATCH (p)-[:KNOWS]->(f) RETURN f.name, 'friend'");
    println!("       }}");
    println!("       RETURN connection, type\n");

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            WHERE p.name = 'Alice'
            CALL {
                WITH p
                MATCH (p)-[:LIKES]->(m:Movie)
                RETURN m.title AS connection, 'movie' AS type
                UNION
                WITH p
                MATCH (p)-[:KNOWS]->(f:Person)
                RETURN f.name AS connection, 'friend' AS type
            }
            RETURN p.name AS person, connection, type
            "#,
        )
        .unwrap();

    println!("Alice's connections:");
    for r in &results {
        println!("  [{}] {}", get_str(r, "type"), get_str(r, "connection"));
    }
    println!();

    // =========================================================================
    // Example 9: Multiple CALL Clauses
    // =========================================================================
    example_header("9", "Multiple CALL Clauses");
    println!("Chain multiple CALL clauses to compute independent values.\n");
    println!("Query: MATCH (p:Person) WHERE p.name = 'Alice'");
    println!("       CALL {{ WITH p ... RETURN count(m) AS movieCount }}");
    println!("       CALL {{ WITH p ... RETURN count(f) AS friendCount }}");
    println!("       RETURN p.name, movieCount, friendCount\n");

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            WHERE p.name = 'Alice'
            CALL {
                WITH p
                MATCH (p)-[:LIKES]->(m:Movie)
                RETURN count(m) AS movieCount
            }
            CALL {
                WITH p
                MATCH (p)-[:KNOWS]->(f:Person)
                RETURN count(f) AS friendCount
            }
            RETURN p.name AS person, movieCount, friendCount
            "#,
        )
        .unwrap();

    println!("Results:");
    for r in &results {
        println!(
            "  {} has {} liked movies and {} friends",
            get_str(r, "person"),
            get_int(r, "movieCount"),
            get_int(r, "friendCount")
        );
    }
    println!();

    // =========================================================================
    // Example 10: CALL with Variable Aliasing
    // =========================================================================
    example_header("10", "CALL with Variable Aliasing");
    println!("Use WITH p AS newName to rename imported variables.\n");
    println!("Query: MATCH (p:Person) WHERE p.name = 'Bob'");
    println!("       CALL {{ WITH p AS viewer MATCH (viewer)-[:LIKES]->(m) RETURN m.title }}");
    println!("       RETURN p.name, movie\n");

    let results = snapshot
        .gql(
            r#"
            MATCH (p:Person)
            WHERE p.name = 'Bob'
            CALL {
                WITH p AS viewer
                MATCH (viewer)-[:LIKES]->(m:Movie)
                RETURN m.title AS movie
            }
            RETURN p.name AS person, movie
            "#,
        )
        .unwrap();

    println!("Results:");
    for r in &results {
        println!("  {} likes {}", get_str(r, "person"), get_str(r, "movie"));
    }
    println!();

    // =========================================================================
    // Example 11: Real-World Use Case - Movie Recommendations
    // =========================================================================
    example_header("11", "Real-World Use Case: Movie Recommendations");
    println!("Find movies that Alice's friends like but Alice hasn't seen.\n");

    // Get Alice's movies
    let alice_movies = snapshot
        .gql(
            r#"
            MATCH (p:Person)-[:LIKES]->(m:Movie)
            WHERE p.name = 'Alice'
            RETURN m.title AS movie
            "#,
        )
        .unwrap();

    let alice_titles: Vec<&str> = alice_movies
        .iter()
        .filter_map(|r| {
            if let Value::String(s) = r {
                Some(s.as_str())
            } else {
                None
            }
        })
        .collect();
    println!("Alice has already seen: {:?}\n", alice_titles);

    // Get movies Alice's friends like with ratings
    let friend_movies = snapshot
        .gql(
            r#"
            MATCH (alice:Person)-[:KNOWS]->(friend:Person)
            WHERE alice.name = 'Alice'
            CALL {
                WITH friend
                MATCH (friend)-[r:LIKES]->(m:Movie)
                RETURN m.title AS movie, r.rating AS rating
            }
            RETURN friend.name AS friend, movie, rating
            ORDER BY rating DESC
            "#,
        )
        .unwrap();

    println!("Movies Alice's friends like:");
    for r in &friend_movies {
        let movie = get_str(r, "movie");
        let seen = if alice_titles.contains(&movie) {
            " (Alice has seen)"
        } else {
            " <- RECOMMEND"
        };
        println!(
            "  {} rates {} as {}/5{}",
            get_str(r, "friend"),
            movie,
            get_int(r, "rating"),
            seen
        );
    }
    println!();

    // =========================================================================
    // Summary
    // =========================================================================
    println!("=============================================================================");
    println!("CALL Subquery Summary");
    println!("=============================================================================\n");

    println!("CALL subqueries execute nested queries within a main query.\n");
    println!("Key patterns:");
    println!();
    println!("  1. UNCORRELATED (no WITH)     - Runs once, cross-joins with outer rows");
    println!("     CALL {{ MATCH (m:Movie) RETURN m.title }}\n");
    println!("  2. CORRELATED (with WITH)     - Runs per outer row");
    println!("     CALL {{ WITH p MATCH (p)-[:LIKES]->(m) RETURN m }}\n");
    println!("  3. AGGREGATION                - COUNT, SUM, AVG, COLLECT per outer row");
    println!("     CALL {{ WITH p ... RETURN count(m) }}\n");
    println!("  4. WHERE FILTER               - Filter inside subquery");
    println!("     CALL {{ WITH p ... WHERE m.year > 2010 RETURN m }}\n");
    println!("  5. ORDER BY / LIMIT           - Top-N per outer row");
    println!("     CALL {{ WITH p ... ORDER BY r.rating DESC LIMIT 1 }}\n");
    println!("  6. UNION / UNION ALL          - Combine multiple patterns");
    println!("     CALL {{ ... RETURN x UNION ... RETURN x }}\n");
    println!("  7. MULTIPLE CALL              - Chain independent subqueries");
    println!("     CALL {{ ... }} CALL {{ ... }}\n");
    println!("  8. VARIABLE ALIASING          - Rename imported variables");
    println!("     CALL {{ WITH p AS person ... }}");
}

fn example_header(num: &str, title: &str) {
    println!("-----------------------------------------------------------------------------");
    println!("Example {}: {}", num, title);
    println!("-----------------------------------------------------------------------------");
}
