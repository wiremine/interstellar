//! Snapshot tests for the GQL parser.
//!
//! These tests use insta to capture the AST structure of parsed queries
//! and error messages, ensuring stability across changes.

use insta::assert_yaml_snapshot;
use rustgremlin::gql::parse;

// =============================================================================
// Basic Query Parsing Snapshots
// =============================================================================

#[test]
fn test_parse_simple_match_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_match_no_label_snapshot() {
    let ast = parse("MATCH (n) RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_match_anonymous_node_snapshot() {
    let ast = parse("MATCH (:Person) RETURN 'found'").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_multiple_labels_snapshot() {
    let ast = parse("MATCH (n:Person:Employee) RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// Property Constraints Snapshots
// =============================================================================

#[test]
fn test_parse_property_filter_string_snapshot() {
    let ast = parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_property_filter_number_snapshot() {
    let ast = parse("MATCH (n:Person {age: 30}) RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_property_filter_multiple_snapshot() {
    let ast = parse("MATCH (n:Person {name: 'Alice', age: 30, active: true}) RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// Edge Pattern Snapshots
// =============================================================================

#[test]
fn test_parse_edge_outgoing_snapshot() {
    let ast = parse("MATCH (a)-[:KNOWS]->(b) RETURN a, b").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_edge_incoming_snapshot() {
    let ast = parse("MATCH (a)<-[:KNOWS]-(b) RETURN a, b").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_edge_bidirectional_snapshot() {
    let ast = parse("MATCH (a)-[:KNOWS]-(b) RETURN a, b").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_edge_no_label_snapshot() {
    let ast = parse("MATCH (a)-[]->(b) RETURN a, b").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_edge_with_variable_snapshot() {
    let ast = parse("MATCH (a)-[r:KNOWS]->(b) RETURN a, r, b").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_multi_hop_pattern_snapshot() {
    let ast = parse("MATCH (a)-[:KNOWS]->(b)-[:WORKS_AT]->(c) RETURN a, b, c").unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// Variable-Length Path Snapshots
// =============================================================================

#[test]
fn test_parse_variable_length_any_snapshot() {
    let ast = parse("MATCH (a)-[*]->(b) RETURN b").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_variable_length_exact_snapshot() {
    let ast = parse("MATCH (a)-[*2]->(b) RETURN b").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_variable_length_range_snapshot() {
    let ast = parse("MATCH (a)-[*1..3]->(b) RETURN b").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_variable_length_min_only_snapshot() {
    let ast = parse("MATCH (a)-[*2..]->(b) RETURN b").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_variable_length_max_only_snapshot() {
    let ast = parse("MATCH (a)-[*..5]->(b) RETURN b").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_variable_length_with_label_snapshot() {
    let ast = parse("MATCH (a)-[:KNOWS*2..4]->(b) RETURN b").unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// WHERE Clause Snapshots
// =============================================================================

#[test]
fn test_parse_where_equals_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.name = 'Alice' RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_not_equals_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.status <> 'inactive' RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_greater_than_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.age > 21 RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_less_than_equals_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.age <= 65 RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_and_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.age > 21 AND n.active = true RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_or_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.city = 'NYC' OR n.city = 'LA' RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_not_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE NOT n.archived RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_is_null_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.email IS NULL RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_is_not_null_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_in_list_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.status IN ['active', 'pending'] RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_not_in_list_snapshot() {
    let ast =
        parse("MATCH (n:Person) WHERE n.status NOT IN ['banned', 'deleted'] RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_contains_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.name CONTAINS 'son' RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_starts_with_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.name STARTS WITH 'A' RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_ends_with_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE n.email ENDS WITH '.com' RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_where_complex_expression_snapshot() {
    let ast = parse("MATCH (n:Person) WHERE (n.age > 21 AND n.age < 65) OR n.vip = true RETURN n")
        .unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// RETURN Clause Snapshots
// =============================================================================

#[test]
fn test_parse_return_variable_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_return_property_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n.name").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_return_multiple_properties_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n.name, n.age, n.email").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_return_alias_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n.name AS personName").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_return_distinct_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN DISTINCT n.city").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_return_literal_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN 'constant', 42, true").unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// Aggregate Function Snapshots
// =============================================================================

#[test]
fn test_parse_count_star_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN count(*)").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_count_property_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN count(n.email)").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_count_distinct_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN count(DISTINCT n.city)").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_sum_snapshot() {
    let ast = parse("MATCH (n:Order) RETURN sum(n.total)").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_avg_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN avg(n.age)").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_min_max_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN min(n.age), max(n.age)").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_collect_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN collect(n.name)").unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// ORDER BY Clause Snapshots
// =============================================================================

#[test]
fn test_parse_order_by_single_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n ORDER BY n.age").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_order_by_desc_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n ORDER BY n.age DESC").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_order_by_multiple_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n ORDER BY n.age DESC, n.name ASC").unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// LIMIT/OFFSET Clause Snapshots
// =============================================================================

#[test]
fn test_parse_limit_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n LIMIT 10").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_limit_offset_snapshot() {
    let ast = parse("MATCH (n:Person) RETURN n LIMIT 10 OFFSET 5").unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// Complex Query Snapshots
// =============================================================================

#[test]
fn test_parse_complex_query_snapshot() {
    let ast = parse(
        r#"
        MATCH (p:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
        WHERE friend.age > 25 AND friend.city = 'NYC'
        RETURN friend.name, friend.age
        ORDER BY friend.age DESC
        LIMIT 10
    "#,
    )
    .unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_social_network_query_snapshot() {
    let ast = parse(
        r#"
        MATCH (user:User {status: 'active'})-[:FOLLOWS*1..2]->(followed:User)
        WHERE followed.verified = true AND followed.id <> user.id
        RETURN DISTINCT followed.username, followed.followers
        ORDER BY followed.followers DESC
        LIMIT 50
    "#,
    )
    .unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_aggregation_query_snapshot() {
    let ast = parse(
        r#"
        MATCH (o:Order)-[:CONTAINS]->(p:Product)
        WHERE o.date > '2024-01-01'
        RETURN p.category, count(*) AS orderCount, sum(o.total) AS revenue
        ORDER BY revenue DESC
        LIMIT 10
    "#,
    )
    .unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// Parse Error Snapshots
// =============================================================================

#[test]
fn test_parse_error_missing_return_snapshot() {
    let err = parse("MATCH (n:Person)").unwrap_err();
    assert_yaml_snapshot!("error_missing_return", format!("{}", err));
}

#[test]
fn test_parse_error_unclosed_paren_snapshot() {
    let err = parse("MATCH (n:Person RETURN n").unwrap_err();
    assert_yaml_snapshot!("error_unclosed_paren", format!("{}", err));
}

#[test]
fn test_parse_error_double_arrow_snapshot() {
    // Test with truly malformed syntax - double arrows
    let err = parse("MATCH (a)-->>>(b) RETURN a").unwrap_err();
    assert_yaml_snapshot!("error_double_arrow", format!("{}", err));
}

#[test]
fn test_parse_error_empty_query_snapshot() {
    let err = parse("").unwrap_err();
    assert_yaml_snapshot!("error_empty_query", format!("{}", err));
}

#[test]
fn test_parse_error_missing_match_snapshot() {
    let err = parse("RETURN n").unwrap_err();
    assert_yaml_snapshot!("error_missing_match", format!("{}", err));
}

#[test]
fn test_parse_error_invalid_literal_snapshot() {
    let err = parse("MATCH (n {age: @invalid}) RETURN n").unwrap_err();
    assert_yaml_snapshot!("error_invalid_literal", format!("{}", err));
}

// =============================================================================
// GROUP BY Clause Snapshots
// =============================================================================

#[test]
fn test_parse_group_by_single_snapshot() {
    let ast = parse("MATCH (p:player) RETURN p.position, count(*) GROUP BY p.position").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_group_by_multiple_snapshot() {
    let ast =
        parse("MATCH (p:player) RETURN p.position, p.team, count(*) GROUP BY p.position, p.team")
            .unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_group_by_with_avg_snapshot() {
    let ast =
        parse("MATCH (p:player) RETURN p.position, avg(p.ppg) AS avg_ppg GROUP BY p.position")
            .unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_group_by_full_query_snapshot() {
    let ast = parse(
        r#"
        MATCH (p:player)
        WHERE p.active = true
        RETURN p.position, count(*) AS cnt, avg(p.ppg) AS avg_ppg
        GROUP BY p.position
        ORDER BY cnt DESC
        LIMIT 10
    "#,
    )
    .unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// Case Insensitivity Snapshots
// =============================================================================

#[test]
fn test_parse_case_insensitive_keywords_snapshot() {
    // All keywords should be case-insensitive
    let ast = parse("match (n:Person) where n.age > 21 return n order by n.name limit 10").unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_mixed_case_keywords_snapshot() {
    let ast = parse("Match (n:Person) Where n.age > 21 Return n Order By n.name Limit 10").unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// CASE Expression Snapshots
// =============================================================================

#[test]
fn test_parse_case_simple_snapshot() {
    let ast =
        parse(r#"MATCH (p:player) RETURN CASE WHEN p.age > 30 THEN 'Senior' ELSE 'Junior' END"#)
            .unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_case_multiple_when_snapshot() {
    let ast = parse(
        r#"
        MATCH (p:player)
        RETURN p.name, CASE 
            WHEN p.ppg >= 25 THEN 'Elite'
            WHEN p.ppg >= 15 THEN 'Starter'
            WHEN p.ppg >= 5 THEN 'Role Player'
            ELSE 'Bench'
        END AS tier
    "#,
    )
    .unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_case_no_else_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN CASE WHEN p.mvp_count > 0 THEN 'MVP Winner' END"#)
        .unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_case_with_comparison_snapshot() {
    let ast = parse(
        r#"
        MATCH (p:player)
        WHERE CASE WHEN p.position = 'Center' THEN p.height > 84 ELSE true END
        RETURN p.name
    "#,
    )
    .unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// Function Call Snapshots (including COALESCE)
// =============================================================================

#[test]
fn test_parse_coalesce_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN coalesce(p.nickname, p.name)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_coalesce_multiple_args_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN coalesce(p.nickname, p.alias, p.name, 'Unknown')"#)
        .unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_upper_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN upper(p.name)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_lower_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN lower(p.name)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_trim_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN trim(p.name)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_substring_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN substring(p.name, 0, 3)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_replace_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN replace(p.name, ' ', '_')"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_tostring_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN toString(p.age)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_tointeger_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN toInteger(p.jersey_number)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_tofloat_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN toFloat(p.ppg)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_abs_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN abs(p.plus_minus)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_ceil_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN ceil(p.ppg)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_floor_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN floor(p.ppg)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_round_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN round(p.ppg)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_length_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN length(p.name)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

// =============================================================================
// Introspection Function Snapshots (Plan 11)
// =============================================================================

#[test]
fn test_parse_id_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN id(p)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_labels_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN labels(p)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_type_function_snapshot() {
    let ast = parse(r#"MATCH (p:player)-[e:played_for]->(t:team) RETURN type(e)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_properties_function_snapshot() {
    let ast = parse(r#"MATCH (p:player) RETURN properties(p)"#).unwrap();
    assert_yaml_snapshot!(ast);
}

#[test]
fn test_parse_introspection_combined_snapshot() {
    let ast = parse(
        r#"MATCH (p:player) RETURN id(p) AS vid, labels(p) AS vlabels, properties(p) AS vprops"#,
    )
    .unwrap();
    assert_yaml_snapshot!(ast);
}
