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
        if inner.as_rule() == Rule::node_pattern {
            elements.push(PatternElement::Node(build_node_pattern(inner)?));
        }
    }

    Ok(Pattern { elements })
}

fn build_node_pattern(pair: pest::iterators::Pair<Rule>) -> Result<NodePattern, ParseError> {
    let mut variable = None;
    let mut labels = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::label_filter => {
                for label_inner in inner.into_inner() {
                    if label_inner.as_rule() == Rule::identifier {
                        labels.push(label_inner.as_str().to_string());
                    }
                }
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        variable,
        labels,
        properties: Vec::new(),
    })
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
}
