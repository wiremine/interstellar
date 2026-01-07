//! Parser for GQL queries.
//!
//! Converts GQL text into AST using pest.

use pest::Parser;
use pest_derive::Parser;

use crate::gql::ast::*;
use crate::gql::error::ParseError;

#[derive(Parser)]
#[grammar = "gql/grammar.pest"]
struct GqlParser;

/// Parse a GQL query string into an AST.
pub fn parse(input: &str) -> Result<Query, ParseError> {
    let pairs =
        GqlParser::parse(Rule::query, input).map_err(|e| ParseError::Syntax(e.to_string()))?;

    let query_pair = pairs.into_iter().next().ok_or(ParseError::Empty)?;

    build_query(query_pair)
}

fn build_query(pair: pest::iterators::Pair<Rule>) -> Result<Query, ParseError> {
    let mut match_clause = None;
    let mut return_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => match_clause = Some(build_match_clause(inner)?),
            Rule::return_clause => return_clause = Some(build_return_clause(inner)?),
            Rule::EOI => {}
            _ => {}
        }
    }

    Ok(Query {
        match_clause: match_clause.ok_or(ParseError::MissingClause("MATCH"))?,
        where_clause: None,
        return_clause: return_clause.ok_or(ParseError::MissingClause("RETURN"))?,
        order_clause: None,
        limit_clause: None,
    })
}

fn build_match_clause(pair: pest::iterators::Pair<Rule>) -> Result<MatchClause, ParseError> {
    let mut patterns = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern {
            patterns.push(build_pattern(inner)?);
        }
    }

    Ok(MatchClause { patterns })
}

fn build_pattern(pair: pest::iterators::Pair<Rule>) -> Result<Pattern, ParseError> {
    let mut elements = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::node_pattern => {
                elements.push(PatternElement::Node(build_node_pattern(inner)?));
            }
            Rule::edge_pattern => {
                elements.push(PatternElement::Edge(build_edge_pattern(inner)?));
            }
            _ => {}
        }
    }

    Ok(Pattern { elements })
}

fn build_node_pattern(pair: pest::iterators::Pair<Rule>) -> Result<NodePattern, ParseError> {
    let mut variable = None;
    let mut labels = Vec::new();
    let mut properties = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::label_filter => {
                labels = build_labels(inner)?;
            }
            Rule::property_filter => {
                properties = build_properties(inner)?;
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        variable,
        labels,
        properties,
    })
}

fn build_edge_pattern(pair: pest::iterators::Pair<Rule>) -> Result<EdgePattern, ParseError> {
    let mut variable = None;
    let mut labels = Vec::new();
    let mut quantifier = None;
    let mut properties = Vec::new();

    let mut has_left = false;
    let mut has_right = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::left_arrow => has_left = true,
            Rule::right_arrow => has_right = true,
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::label_filter => {
                labels = build_labels(inner)?;
            }
            Rule::quantifier => quantifier = Some(build_quantifier(inner)?),
            Rule::property_filter => properties = build_properties(inner)?,
            _ => {}
        }
    }

    let direction = match (has_left, has_right) {
        (false, true) => EdgeDirection::Outgoing, // -[]->
        (true, false) => EdgeDirection::Incoming, // <-[]-
        _ => EdgeDirection::Both,                 // -[]-
    };

    Ok(EdgePattern {
        variable,
        labels,
        direction,
        quantifier,
        properties,
    })
}

fn build_labels(pair: pest::iterators::Pair<Rule>) -> Result<Vec<String>, ParseError> {
    let mut labels = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::identifier {
            labels.push(inner.as_str().to_string());
        }
    }
    Ok(labels)
}

fn build_quantifier(pair: pest::iterators::Pair<Rule>) -> Result<PathQuantifier, ParseError> {
    let mut min = None;
    let mut max = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::range {
            let range_str = inner.as_str();
            if range_str.contains("..") {
                let parts: Vec<&str> = range_str.split("..").collect();
                if !parts[0].is_empty() {
                    min = Some(
                        parts[0]
                            .parse()
                            .map_err(|_| ParseError::InvalidLiteral(parts[0].to_string()))?,
                    );
                }
                if parts.len() > 1 && !parts[1].is_empty() {
                    max = Some(
                        parts[1]
                            .parse()
                            .map_err(|_| ParseError::InvalidLiteral(parts[1].to_string()))?,
                    );
                }
            } else {
                // Single integer: *2 means exactly 2 hops
                let n: u32 = range_str
                    .parse()
                    .map_err(|_| ParseError::InvalidLiteral(range_str.to_string()))?;
                min = Some(n);
                max = Some(n);
            }
        }
    }

    Ok(PathQuantifier { min, max })
}

fn build_properties(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Vec<(String, Literal)>, ParseError> {
    let mut properties = Vec::new();

    for prop in pair.into_inner() {
        if prop.as_rule() == Rule::property {
            let mut key = None;
            let mut value = None;

            for inner in prop.into_inner() {
                match inner.as_rule() {
                    Rule::identifier => key = Some(inner.as_str().to_string()),
                    Rule::literal => value = Some(build_literal(inner)?),
                    _ => {}
                }
            }

            if let (Some(k), Some(v)) = (key, value) {
                properties.push((k, v));
            }
        }
    }

    Ok(properties)
}

fn build_literal(pair: pest::iterators::Pair<Rule>) -> Result<Literal, ParseError> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::InvalidLiteral("empty".to_string()))?;

    match inner.as_rule() {
        Rule::string => {
            let s = inner.as_str();
            // Remove surrounding quotes and unescape '' -> '
            let content = &s[1..s.len() - 1];
            let unescaped = content.replace("''", "'");
            Ok(Literal::String(unescaped))
        }
        Rule::integer => {
            let n: i64 = inner
                .as_str()
                .parse()
                .map_err(|_| ParseError::InvalidLiteral(inner.as_str().to_string()))?;
            Ok(Literal::Int(n))
        }
        Rule::float => {
            let f: f64 = inner
                .as_str()
                .parse()
                .map_err(|_| ParseError::InvalidLiteral(inner.as_str().to_string()))?;
            Ok(Literal::Float(f))
        }
        Rule::boolean => {
            // Check the string content to determine true/false
            let s = inner.as_str().to_lowercase();
            if s == "true" {
                Ok(Literal::Bool(true))
            } else {
                Ok(Literal::Bool(false))
            }
        }
        Rule::TRUE => Ok(Literal::Bool(true)),
        Rule::FALSE => Ok(Literal::Bool(false)),
        Rule::NULL => Ok(Literal::Null),
        _ => Err(ParseError::InvalidLiteral(inner.as_str().to_string())),
    }
}

fn build_return_clause(pair: pest::iterators::Pair<Rule>) -> Result<ReturnClause, ParseError> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::variable {
            items.push(ReturnItem {
                expression: Expression::Variable(inner.as_str().to_string()),
                alias: None,
            });
        }
    }

    Ok(ReturnClause { items })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_match() {
        let query = parse("MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(query.match_clause.patterns.len(), 1);

        let pattern = &query.match_clause.patterns[0];
        assert_eq!(pattern.elements.len(), 1);

        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.variable, Some("n".to_string()));
            assert_eq!(node.labels, vec!["Person".to_string()]);
        } else {
            panic!("Expected node pattern");
        }

        assert_eq!(query.return_clause.items.len(), 1);
        if let Expression::Variable(v) = &query.return_clause.items[0].expression {
            assert_eq!(v, "n");
        } else {
            panic!("Expected variable expression");
        }
    }

    #[test]
    fn test_parse_no_label() {
        let query = parse("MATCH (n) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.variable, Some("n".to_string()));
            assert!(node.labels.is_empty());
        }
    }

    #[test]
    fn test_parse_case_insensitive() {
        // MATCH and RETURN are case insensitive
        let query = parse("match (n:Person) return n").unwrap();
        assert_eq!(query.match_clause.patterns.len(), 1);

        let query = parse("Match (n:Person) Return n").unwrap();
        assert_eq!(query.match_clause.patterns.len(), 1);
    }

    #[test]
    fn test_parse_error_missing_return() {
        let result = parse("MATCH (n:Person)");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_invalid_syntax() {
        let result = parse("MATCH (n:Person RETURN n");
        assert!(result.is_err());
    }

    // ============================================
    // Phase 2.1: Edge Pattern Tests
    // ============================================

    #[test]
    fn test_parse_outgoing_edge() {
        let query = parse("MATCH (a)-[:KNOWS]->(b) RETURN b").unwrap();
        assert_eq!(query.match_clause.patterns.len(), 1);

        let pattern = &query.match_clause.patterns[0];
        assert_eq!(pattern.elements.len(), 3); // node, edge, node

        // First node
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.variable, Some("a".to_string()));
        } else {
            panic!("Expected node pattern");
        }

        // Edge
        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert_eq!(edge.labels, vec!["KNOWS".to_string()]);
            assert_eq!(edge.direction, EdgeDirection::Outgoing);
            assert!(edge.variable.is_none());
        } else {
            panic!("Expected edge pattern");
        }

        // Second node
        if let PatternElement::Node(node) = &pattern.elements[2] {
            assert_eq!(node.variable, Some("b".to_string()));
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn test_parse_incoming_edge() {
        let query = parse("MATCH (a)<-[:KNOWS]-(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert_eq!(edge.labels, vec!["KNOWS".to_string()]);
            assert_eq!(edge.direction, EdgeDirection::Incoming);
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_bidirectional_edge() {
        let query = parse("MATCH (a)-[:KNOWS]-(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert_eq!(edge.labels, vec!["KNOWS".to_string()]);
            assert_eq!(edge.direction, EdgeDirection::Both);
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_edge_without_label() {
        let query = parse("MATCH (a)-[]->(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert!(edge.labels.is_empty());
            assert_eq!(edge.direction, EdgeDirection::Outgoing);
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_edge_with_variable() {
        let query = parse("MATCH (a)-[e:KNOWS]->(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert_eq!(edge.variable, Some("e".to_string()));
            assert_eq!(edge.labels, vec!["KNOWS".to_string()]);
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_node_with_properties() {
        let query = parse("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.variable, Some("n".to_string()));
            assert_eq!(node.labels, vec!["Person".to_string()]);
            assert_eq!(node.properties.len(), 1);
            assert_eq!(node.properties[0].0, "name");
            assert_eq!(node.properties[0].1, Literal::String("Alice".to_string()));
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn test_parse_node_with_multiple_properties() {
        let query = parse("MATCH (n:Person {name: 'Alice', age: 30}) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.properties.len(), 2);
            assert_eq!(node.properties[0].0, "name");
            assert_eq!(node.properties[0].1, Literal::String("Alice".to_string()));
            assert_eq!(node.properties[1].0, "age");
            assert_eq!(node.properties[1].1, Literal::Int(30));
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn test_parse_multiple_patterns() {
        let query = parse("MATCH (a), (b) RETURN a").unwrap();
        assert_eq!(query.match_clause.patterns.len(), 2);

        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.variable, Some("a".to_string()));
        }
        if let PatternElement::Node(node) = &query.match_clause.patterns[1].elements[0] {
            assert_eq!(node.variable, Some("b".to_string()));
        }
    }

    #[test]
    fn test_parse_multiple_labels() {
        let query = parse("MATCH (n:Person:Employee) RETURN n").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(
                node.labels,
                vec!["Person".to_string(), "Employee".to_string()]
            );
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn test_parse_quantifier_exact() {
        let query = parse("MATCH (a)-[:KNOWS*2]->(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            let q = edge.quantifier.as_ref().unwrap();
            assert_eq!(q.min, Some(2));
            assert_eq!(q.max, Some(2));
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_quantifier_range() {
        let query = parse("MATCH (a)-[:KNOWS*1..3]->(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            let q = edge.quantifier.as_ref().unwrap();
            assert_eq!(q.min, Some(1));
            assert_eq!(q.max, Some(3));
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_quantifier_unbounded_max() {
        let query = parse("MATCH (a)-[:KNOWS*2..]->(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            let q = edge.quantifier.as_ref().unwrap();
            assert_eq!(q.min, Some(2));
            assert_eq!(q.max, None);
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_quantifier_unbounded_min() {
        let query = parse("MATCH (a)-[:KNOWS*..3]->(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            let q = edge.quantifier.as_ref().unwrap();
            assert_eq!(q.min, None);
            assert_eq!(q.max, Some(3));
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_quantifier_star_only() {
        let query = parse("MATCH (a)-[:KNOWS*]->(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            let q = edge.quantifier.as_ref().unwrap();
            assert_eq!(q.min, None);
            assert_eq!(q.max, None);
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_literal_types() {
        // String literal
        let query = parse("MATCH (n {name: 'Alice'}) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.properties[0].1, Literal::String("Alice".to_string()));
        }

        // Integer literal
        let query = parse("MATCH (n {age: 30}) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.properties[0].1, Literal::Int(30));
        }

        // Negative integer
        let query = parse("MATCH (n {balance: -100}) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.properties[0].1, Literal::Int(-100));
        }

        // Float literal
        let query = parse("MATCH (n {score: 3.14}) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.properties[0].1, Literal::Float(3.14));
        }

        // Boolean true
        let query = parse("MATCH (n {active: true}) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.properties[0].1, Literal::Bool(true));
        }

        // Boolean false
        let query = parse("MATCH (n {active: false}) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.properties[0].1, Literal::Bool(false));
        }

        // Null
        let query = parse("MATCH (n {deleted: null}) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.properties[0].1, Literal::Null);
        }
    }

    #[test]
    fn test_parse_string_escape() {
        // Escaped single quote: '' -> '
        let query = parse("MATCH (n {name: 'O''Brien'}) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.properties[0].1, Literal::String("O'Brien".to_string()));
        }
    }

    #[test]
    fn test_parse_multi_hop_pattern() {
        let query = parse("MATCH (a)-[:KNOWS]->(b)-[:WORKS_WITH]->(c) RETURN c").unwrap();
        let pattern = &query.match_clause.patterns[0];

        assert_eq!(pattern.elements.len(), 5); // a, edge, b, edge, c

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert_eq!(edge.labels, vec!["KNOWS".to_string()]);
        }
        if let PatternElement::Edge(edge) = &pattern.elements[3] {
            assert_eq!(edge.labels, vec!["WORKS_WITH".to_string()]);
        }
    }

    #[test]
    fn test_parse_edge_with_properties() {
        let query = parse("MATCH (a)-[:KNOWS {since: 2020}]->(b) RETURN b").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert_eq!(edge.properties.len(), 1);
            assert_eq!(edge.properties[0].0, "since");
            assert_eq!(edge.properties[0].1, Literal::Int(2020));
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_anonymous_node() {
        // Node without variable (anonymous)
        let query = parse("MATCH (:Person)-[:KNOWS]->(friend) RETURN friend").unwrap();
        let pattern = &query.match_clause.patterns[0];

        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert!(node.variable.is_none());
            assert_eq!(node.labels, vec!["Person".to_string()]);
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn test_parse_complex_pattern() {
        // Complex pattern with multiple features
        let query =
            parse("MATCH (a:Person {name: 'Alice'})-[r:KNOWS*1..3]->(b:Person:Employee) RETURN b")
                .unwrap();
        let pattern = &query.match_clause.patterns[0];

        assert_eq!(pattern.elements.len(), 3);

        // First node with label and properties
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.variable, Some("a".to_string()));
            assert_eq!(node.labels, vec!["Person".to_string()]);
            assert_eq!(node.properties[0].1, Literal::String("Alice".to_string()));
        }

        // Edge with variable, label, and quantifier
        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert_eq!(edge.variable, Some("r".to_string()));
            assert_eq!(edge.labels, vec!["KNOWS".to_string()]);
            assert_eq!(edge.direction, EdgeDirection::Outgoing);
            let q = edge.quantifier.as_ref().unwrap();
            assert_eq!(q.min, Some(1));
            assert_eq!(q.max, Some(3));
        }

        // Second node with multiple labels
        if let PatternElement::Node(node) = &pattern.elements[2] {
            assert_eq!(node.variable, Some("b".to_string()));
            assert_eq!(
                node.labels,
                vec!["Person".to_string(), "Employee".to_string()]
            );
        }
    }
}
