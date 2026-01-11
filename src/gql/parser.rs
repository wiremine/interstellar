//! Parser for GQL queries.
//!
//! This module converts GQL query text into a typed AST using the
//! [pest](https://pest.rs) parsing library. The grammar is defined in
//! `grammar.pest`.
//!
//! # Usage
//!
//! The primary entry point is the [`parse`] function:
//!
//! ```rust
//! use rustgremlin::gql::parse;
//!
//! let ast = parse("MATCH (n:Person) RETURN n").unwrap();
//! assert_eq!(ast.match_clause.patterns.len(), 1);
//! ```
//!
//! # Error Handling
//!
//! Parse errors include source position information for debugging:
//!
//! ```rust
//! use rustgremlin::gql::{parse, ParseError};
//!
//! match parse("MATCH (n:Person") {
//!     Ok(_) => unreachable!(),
//!     Err(e) => {
//!         // Error message includes position info
//!         println!("Parse error: {}", e);
//!     }
//! }
//! ```

use pest::Parser;
use pest_derive::Parser;

use crate::gql::ast::{
    AggregateFunc, BinaryOperator, CaseExpression, CreateClause, DeleteClause, DetachDeleteClause,
    EdgeDirection, EdgePattern, Expression, GroupByClause, LimitClause, Literal, MatchClause,
    MergeClause, MutationClause, MutationQuery, NodePattern, OptionalMatchClause, OrderClause,
    OrderItem, PathQuantifier, Pattern, PatternElement, PropertyRef, Query, RemoveClause,
    ReturnClause, ReturnItem, SetClause, SetItem, Statement, UnaryOperator, UnwindClause,
    WhereClause, WithPathClause,
};
use crate::gql::error::{ParseError, Span};

#[derive(Parser)]
#[grammar = "gql/grammar.pest"]
struct GqlParser;

/// Helper to extract a Span from a pest Pair
fn span_from_pair(pair: &pest::iterators::Pair<Rule>) -> Span {
    let span = pair.as_span();
    Span::new(span.start(), span.end())
}

/// Parse a GQL query string into an AST.
///
/// This is the main entry point for parsing GQL queries. It takes a query
/// string and returns a [`Query`] AST node that can be passed to
/// [`compile`](crate::gql::compile) for execution.
///
/// # Arguments
///
/// * `input` - A GQL query string
///
/// # Returns
///
/// Returns `Ok(Query)` on successful parse, or `Err(ParseError)` if the
/// query contains syntax errors.
///
/// # Example
///
/// ```rust
/// use rustgremlin::gql::parse;
///
/// // Simple query
/// let query = parse("MATCH (n:Person) RETURN n").unwrap();
///
/// // Query with all clauses
/// let query = parse(r#"
///     MATCH (p:Person)-[:KNOWS]->(friend:Person)
///     WHERE p.age > 25
///     RETURN p.name, friend.name
///     ORDER BY p.age DESC
///     LIMIT 10
/// "#).unwrap();
/// ```
///
/// # Errors
///
/// Returns [`ParseError`] for:
/// - Syntax errors (malformed query structure)
/// - Missing required clauses (MATCH, RETURN)
/// - Invalid literals (malformed numbers, strings)
///
/// ```rust
/// use rustgremlin::gql::parse;
///
/// // Missing RETURN clause
/// assert!(parse("MATCH (n:Person)").is_err());
///
/// // Malformed pattern
/// assert!(parse("MATCH (n:Person RETURN n").is_err());
/// ```
pub fn parse(input: &str) -> Result<Query, ParseError> {
    // Parse as a statement and extract the single query
    let stmt = parse_statement(input)?;
    match stmt {
        Statement::Query(query) => Ok(*query),
        Statement::Union { .. } => {
            // For backward compatibility, parse() returns Query
            // Use parse_statement() for UNION queries
            Err(ParseError::Syntax(
                "Use parse_statement() for UNION queries".to_string(),
            ))
        }
        Statement::Mutation(_) => {
            // For backward compatibility, parse() returns Query
            // Use parse_statement() for mutation statements
            Err(ParseError::Syntax(
                "Use parse_statement() for mutation statements".to_string(),
            ))
        }
    }
}

/// Parse a GQL statement string into an AST.
///
/// This function parses GQL statements which may be single queries or
/// UNION of multiple queries. Use this when you need to support UNION.
///
/// # Arguments
///
/// * `input` - A GQL statement string (single query or UNION)
///
/// # Returns
///
/// Returns `Ok(Statement)` on successful parse, or `Err(ParseError)` if the
/// statement contains syntax errors.
///
/// # Example
///
/// ```rust
/// use rustgremlin::gql::parse_statement;
///
/// // Single query
/// let stmt = parse_statement("MATCH (n:Person) RETURN n").unwrap();
///
/// // UNION query
/// let stmt = parse_statement(r#"
///     MATCH (p:Player)-[:played_for]->(t:Team) RETURN t.name
///     UNION
///     MATCH (p:Player)-[:won_with]->(t:Team) RETURN t.name
/// "#).unwrap();
///
/// // UNION ALL query
/// let stmt = parse_statement(r#"
///     MATCH (a:A) RETURN a.name
///     UNION ALL
///     MATCH (b:B) RETURN b.name
/// "#).unwrap();
/// ```
pub fn parse_statement(input: &str) -> Result<Statement, ParseError> {
    let pairs =
        GqlParser::parse(Rule::statement, input).map_err(|e| ParseError::Syntax(e.to_string()))?;

    let stmt_pair = pairs.into_iter().next().ok_or(ParseError::Empty)?;

    build_statement(stmt_pair)
}

/// Build a Statement from a pest pair.
fn build_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement, ParseError> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::read_statement => {
                return build_read_statement(inner);
            }
            Rule::mutation_statement => {
                return build_mutation_statement(inner);
            }
            Rule::EOI => {}
            _ => {}
        }
    }

    Err(ParseError::Empty)
}

/// Build a Statement from a read_statement pest pair.
fn build_read_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement, ParseError> {
    let mut queries = Vec::new();
    let mut union_all = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::query => {
                queries.push(build_query(inner)?);
            }
            Rule::union_clause => {
                // Check for ALL keyword in the union clause
                for clause_inner in inner.into_inner() {
                    if clause_inner.as_rule() == Rule::ALL {
                        union_all = true;
                    }
                }
            }
            _ => {}
        }
    }

    if queries.is_empty() {
        return Err(ParseError::Empty);
    }

    if queries.len() == 1 {
        Ok(Statement::Query(Box::new(queries.pop().unwrap())))
    } else {
        Ok(Statement::Union {
            queries,
            all: union_all,
        })
    }
}

/// Build a Statement from a mutation_statement pest pair.
fn build_mutation_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::Empty)?;

    match inner.as_rule() {
        Rule::create_only_statement => build_create_only_statement(inner),
        Rule::match_mutation_statement => build_match_mutation_statement(inner),
        Rule::merge_statement => build_merge_statement(inner),
        _ => Err(ParseError::Syntax(format!(
            "Unexpected mutation statement type: {:?}",
            inner.as_rule()
        ))),
    }
}

/// Build a CREATE-only statement (without MATCH).
fn build_create_only_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement, ParseError> {
    let mut mutations = Vec::new();
    let mut return_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::create_clause => {
                mutations.push(MutationClause::Create(build_create_clause(inner)?));
            }
            Rule::return_clause => {
                return_clause = Some(build_return_clause(inner)?);
            }
            _ => {}
        }
    }

    Ok(Statement::Mutation(Box::new(MutationQuery {
        match_clause: None,
        optional_match_clauses: vec![],
        where_clause: None,
        mutations,
        return_clause,
    })))
}

/// Build a MATCH + mutation statement.
fn build_match_mutation_statement(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Statement, ParseError> {
    let mut match_clause = None;
    let mut optional_match_clauses = Vec::new();
    let mut where_clause = None;
    let mut mutations = Vec::new();
    let mut return_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => {
                match_clause = Some(build_match_clause(inner)?);
            }
            Rule::optional_match_clause => {
                optional_match_clauses.push(build_optional_match_clause(inner)?);
            }
            Rule::where_clause => {
                where_clause = Some(build_where_clause(inner)?);
            }
            Rule::mutation_clause => {
                mutations.push(build_mutation_clause(inner)?);
            }
            Rule::return_clause => {
                return_clause = Some(build_return_clause(inner)?);
            }
            _ => {}
        }
    }

    Ok(Statement::Mutation(Box::new(MutationQuery {
        match_clause,
        optional_match_clauses,
        where_clause,
        mutations,
        return_clause,
    })))
}

/// Build a MERGE statement.
fn build_merge_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut pattern = None;
    let mut on_create = None;
    let mut on_match = None;
    let mut return_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::merge_clause => {
                // Extract pattern from merge_clause
                for merge_inner in inner.into_inner() {
                    if merge_inner.as_rule() == Rule::pattern {
                        pattern = Some(build_pattern(merge_inner)?);
                    }
                }
            }
            Rule::merge_action => {
                // Parse ON CREATE or ON MATCH action
                for action_inner in inner.into_inner() {
                    match action_inner.as_rule() {
                        Rule::on_create_action => {
                            on_create = Some(build_set_items_from_action(action_inner)?);
                        }
                        Rule::on_match_action => {
                            on_match = Some(build_set_items_from_action(action_inner)?);
                        }
                        _ => {}
                    }
                }
            }
            Rule::return_clause => {
                return_clause = Some(build_return_clause(inner)?);
            }
            _ => {}
        }
    }

    let pattern = pattern.ok_or_else(|| ParseError::missing_clause("MERGE pattern", pair_span))?;

    Ok(Statement::Mutation(Box::new(MutationQuery {
        match_clause: None,
        optional_match_clauses: vec![],
        where_clause: None,
        mutations: vec![MutationClause::Merge(MergeClause {
            pattern,
            on_create,
            on_match,
        })],
        return_clause,
    })))
}

/// Build a mutation clause (CREATE, SET, REMOVE, DELETE, DETACH DELETE).
fn build_mutation_clause(pair: pest::iterators::Pair<Rule>) -> Result<MutationClause, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::Empty)?;

    match inner.as_rule() {
        Rule::create_clause => Ok(MutationClause::Create(build_create_clause(inner)?)),
        Rule::set_clause => Ok(MutationClause::Set(build_set_clause(inner)?)),
        Rule::remove_clause => Ok(MutationClause::Remove(build_remove_clause(inner)?)),
        Rule::delete_clause => Ok(MutationClause::Delete(build_delete_clause(inner)?)),
        Rule::detach_delete_clause => Ok(MutationClause::DetachDelete(build_detach_delete_clause(
            inner,
        )?)),
        _ => Err(ParseError::Syntax(format!(
            "Unexpected mutation clause type: {:?}",
            inner.as_rule()
        ))),
    }
}

/// Build a CREATE clause.
fn build_create_clause(pair: pest::iterators::Pair<Rule>) -> Result<CreateClause, ParseError> {
    let mut patterns = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern {
            patterns.push(build_pattern(inner)?);
        }
    }

    Ok(CreateClause { patterns })
}

/// Build a SET clause.
fn build_set_clause(pair: pest::iterators::Pair<Rule>) -> Result<SetClause, ParseError> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::set_item {
            items.push(build_set_item(inner)?);
        }
    }

    Ok(SetClause { items })
}

/// Build a single SET item (property_access = expression).
fn build_set_item(pair: pest::iterators::Pair<Rule>) -> Result<SetItem, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut target = None;
    let mut value = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::property_access => {
                target = Some(build_property_ref(inner)?);
            }
            Rule::expression => {
                value = Some(build_expression(inner)?);
            }
            _ => {}
        }
    }

    Ok(SetItem {
        target: target.ok_or_else(|| ParseError::missing_clause("SET target", pair_span))?,
        value: value.ok_or_else(|| ParseError::missing_clause("SET value", pair_span))?,
    })
}

/// Build a PropertyRef from a property_access rule.
fn build_property_ref(pair: pest::iterators::Pair<Rule>) -> Result<PropertyRef, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut iter = pair.into_inner();

    let variable = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("variable", pair_span))?
        .as_str()
        .to_string();

    let property = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("property", pair_span))?
        .as_str()
        .to_string();

    Ok(PropertyRef { variable, property })
}

/// Build set items from an ON CREATE or ON MATCH action.
fn build_set_items_from_action(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Vec<SetItem>, ParseError> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::set_item {
            items.push(build_set_item(inner)?);
        }
    }

    Ok(items)
}

/// Build a REMOVE clause.
fn build_remove_clause(pair: pest::iterators::Pair<Rule>) -> Result<RemoveClause, ParseError> {
    let mut properties = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::remove_item {
            // remove_item contains property_access
            for item_inner in inner.into_inner() {
                if item_inner.as_rule() == Rule::property_access {
                    properties.push(build_property_ref(item_inner)?);
                }
            }
        }
    }

    Ok(RemoveClause { properties })
}

/// Build a DELETE clause.
fn build_delete_clause(pair: pest::iterators::Pair<Rule>) -> Result<DeleteClause, ParseError> {
    let mut variables = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::variable {
            variables.push(inner.as_str().to_string());
        }
    }

    Ok(DeleteClause { variables })
}

/// Build a DETACH DELETE clause.
fn build_detach_delete_clause(
    pair: pest::iterators::Pair<Rule>,
) -> Result<DetachDeleteClause, ParseError> {
    let mut variables = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::variable {
            variables.push(inner.as_str().to_string());
        }
    }

    Ok(DetachDeleteClause { variables })
}

fn build_query(pair: pest::iterators::Pair<Rule>) -> Result<Query, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut match_clause = None;
    let mut optional_match_clauses = Vec::new();
    let mut with_path_clause = None;
    let mut unwind_clauses = Vec::new();
    let mut where_clause = None;
    let mut return_clause = None;
    let mut group_by_clause = None;
    let mut order_clause = None;
    let mut limit_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::match_clause => match_clause = Some(build_match_clause(inner)?),
            Rule::optional_match_clause => {
                optional_match_clauses.push(build_optional_match_clause(inner)?);
            }
            Rule::with_path_clause => with_path_clause = Some(build_with_path_clause(inner)?),
            Rule::unwind_clause => unwind_clauses.push(build_unwind_clause(inner)?),
            Rule::where_clause => where_clause = Some(build_where_clause(inner)?),
            Rule::return_clause => return_clause = Some(build_return_clause(inner)?),
            Rule::group_by_clause => group_by_clause = Some(build_group_by_clause(inner)?),
            Rule::order_clause => order_clause = Some(build_order_clause(inner)?),
            Rule::limit_clause => limit_clause = Some(build_limit_clause(inner)?),
            Rule::EOI => {}
            _ => {}
        }
    }

    Ok(Query {
        match_clause: match_clause.ok_or_else(|| ParseError::missing_clause("MATCH", pair_span))?,
        optional_match_clauses,
        with_path_clause,
        unwind_clauses,
        where_clause,
        return_clause: return_clause
            .ok_or_else(|| ParseError::missing_clause("RETURN", pair_span))?,
        group_by_clause,
        order_clause,
        limit_clause,
    })
}

fn build_where_clause(pair: pest::iterators::Pair<Rule>) -> Result<WhereClause, ParseError> {
    let pair_span = span_from_pair(&pair);
    let expr_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::expression)
        .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?;

    Ok(WhereClause {
        expression: build_expression(expr_pair)?,
    })
}

fn build_group_by_clause(pair: pest::iterators::Pair<Rule>) -> Result<GroupByClause, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut expressions = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            expressions.push(build_expression(inner)?);
        }
    }

    if expressions.is_empty() {
        return Err(ParseError::missing_clause("GROUP BY expression", pair_span));
    }

    Ok(GroupByClause { expressions })
}

fn build_order_clause(pair: pest::iterators::Pair<Rule>) -> Result<OrderClause, ParseError> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::order_item {
            items.push(build_order_item(inner)?);
        }
    }

    Ok(OrderClause { items })
}

fn build_order_item(pair: pest::iterators::Pair<Rule>) -> Result<OrderItem, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut expression = None;
    let mut descending = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => expression = Some(build_expression(inner)?),
            Rule::DESC => descending = true,
            Rule::ASC => descending = false,
            _ => {}
        }
    }

    Ok(OrderItem {
        expression: expression
            .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?,
        descending,
    })
}

fn build_limit_clause(pair: pest::iterators::Pair<Rule>) -> Result<LimitClause, ParseError> {
    let mut limit = 0u64;
    let mut offset = None;
    let mut seen_limit = false;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::integer {
            let span = span_from_pair(&inner);
            let n: u64 = inner.as_str().parse().map_err(|_| {
                ParseError::invalid_literal(inner.as_str(), span, "expected unsigned integer")
            })?;
            if !seen_limit {
                limit = n;
                seen_limit = true;
            } else {
                offset = Some(n);
            }
        }
    }

    Ok(LimitClause { limit, offset })
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

fn build_optional_match_clause(
    pair: pest::iterators::Pair<Rule>,
) -> Result<OptionalMatchClause, ParseError> {
    let mut patterns = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::pattern {
            patterns.push(build_pattern(inner)?);
        }
    }

    Ok(OptionalMatchClause { patterns })
}

/// Build a WITH PATH clause from a pest pair.
///
/// WITH PATH or WITH PATH AS alias
fn build_with_path_clause(pair: pest::iterators::Pair<Rule>) -> Result<WithPathClause, ParseError> {
    let mut alias = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::identifier {
            alias = Some(inner.as_str().to_string());
        }
    }

    Ok(WithPathClause { alias })
}

/// Build an UNWIND clause from a pest pair.
///
/// UNWIND expression AS variable
fn build_unwind_clause(pair: pest::iterators::Pair<Rule>) -> Result<UnwindClause, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut expression = None;
    let mut alias = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => expression = Some(build_expression(inner)?),
            Rule::identifier => alias = Some(inner.as_str().to_string()),
            _ => {}
        }
    }

    Ok(UnwindClause {
        expression: expression
            .ok_or_else(|| ParseError::missing_clause("UNWIND expression", pair_span))?,
        alias: alias.ok_or_else(|| ParseError::missing_clause("UNWIND alias", pair_span))?,
    })
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
            let span = span_from_pair(&inner);
            let range_str = inner.as_str();
            if range_str.contains("..") {
                let parts: Vec<&str> = range_str.split("..").collect();
                if !parts[0].is_empty() {
                    min = Some(parts[0].parse().map_err(|_| {
                        ParseError::invalid_range(range_str, span, "invalid minimum value")
                    })?);
                }
                if parts.len() > 1 && !parts[1].is_empty() {
                    max = Some(parts[1].parse().map_err(|_| {
                        ParseError::invalid_range(range_str, span, "invalid maximum value")
                    })?);
                }
            } else {
                // Single integer: *2 means exactly 2 hops
                let n: u32 = range_str
                    .parse()
                    .map_err(|_| ParseError::invalid_range(range_str, span, "expected integer"))?;
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
    let pair_span = span_from_pair(&pair);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::invalid_literal("empty", pair_span, "expected literal value"))?;

    let span = span_from_pair(&inner);
    match inner.as_rule() {
        Rule::string => {
            let s = inner.as_str();
            // Remove surrounding quotes and unescape '' -> '
            let content = &s[1..s.len() - 1];
            let unescaped = content.replace("''", "'");
            Ok(Literal::String(unescaped))
        }
        Rule::integer => {
            let n: i64 = inner.as_str().parse().map_err(|_| {
                ParseError::invalid_literal(inner.as_str(), span, "expected integer")
            })?;
            Ok(Literal::Int(n))
        }
        Rule::float => {
            let f: f64 = inner
                .as_str()
                .parse()
                .map_err(|_| ParseError::invalid_literal(inner.as_str(), span, "expected float"))?;
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
        _ => Err(ParseError::invalid_literal(
            inner.as_str(),
            span,
            "unexpected literal type",
        )),
    }
}

fn build_return_clause(pair: pest::iterators::Pair<Rule>) -> Result<ReturnClause, ParseError> {
    let mut items = Vec::new();
    let mut distinct = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::DISTINCT => distinct = true,
            Rule::return_item => items.push(build_return_item(inner)?),
            _ => {}
        }
    }

    Ok(ReturnClause { distinct, items })
}

fn build_return_item(pair: pest::iterators::Pair<Rule>) -> Result<ReturnItem, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut expression = None;
    let mut alias = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::expression => expression = Some(build_expression(inner)?),
            Rule::identifier => alias = Some(inner.as_str().to_string()),
            _ => {}
        }
    }

    Ok(ReturnItem {
        expression: expression
            .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?,
        alias,
    })
}

fn build_expression(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?;

    // Expression always starts with or_expr in the grammar
    build_or_expr(inner)
}

fn build_or_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut children: Vec<_> = pair.into_inner().collect();

    // First child must be and_expr
    if children.is_empty() {
        return Err(ParseError::missing_clause("expression", pair_span));
    }

    let first = children.remove(0);
    let mut left = build_and_expr(first)?;

    // Remaining children are: OR, and_expr, OR, and_expr, ...
    let mut iter = children.into_iter();
    while let Some(or_token) = iter.next() {
        if or_token.as_rule() == Rule::OR {
            if let Some(right_pair) = iter.next() {
                let right = build_and_expr(right_pair)?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Or,
                    right: Box::new(right),
                };
            }
        }
    }

    Ok(left)
}

fn build_and_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut children: Vec<_> = pair.into_inner().collect();

    // First child must be not_expr
    if children.is_empty() {
        return Err(ParseError::missing_clause("expression", pair_span));
    }

    let first = children.remove(0);
    let mut left = build_not_expr(first)?;

    // Remaining children are: AND, not_expr, AND, not_expr, ...
    let mut iter = children.into_iter();
    while let Some(and_token) = iter.next() {
        if and_token.as_rule() == Rule::AND {
            if let Some(right_pair) = iter.next() {
                let right = build_not_expr(right_pair)?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(right),
                };
            }
        }
    }

    Ok(left)
}

fn build_not_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut not_count = 0;
    let mut comparison_pair = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::NOT => not_count += 1,
            Rule::comparison => comparison_pair = Some(inner),
            _ => {}
        }
    }

    let mut expr = build_comparison(
        comparison_pair.ok_or_else(|| ParseError::missing_clause("comparison", pair_span))?,
    )?;

    // Apply NOT operators (odd number = negated)
    if not_count % 2 == 1 {
        expr = Expression::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr),
        };
    }

    Ok(expr)
}

fn build_comparison(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::missing_clause("comparison", pair_span))?;

    let span = span_from_pair(&inner);
    match inner.as_rule() {
        Rule::is_null_expr => build_is_null_expr(inner),
        Rule::in_expr => build_in_expr(inner),
        Rule::comparison_expr => build_comparison_expr(inner),
        _ => Err(ParseError::unexpected_token(
            span,
            inner.as_str(),
            "comparison expression",
        )),
    }
}

fn build_is_null_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut iter = pair.into_inner();
    let additive_pair = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?;
    let expr = build_additive(additive_pair)?;

    // Check for NOT keyword
    let negated = iter.any(|p| p.as_rule() == Rule::NOT);

    Ok(Expression::IsNull {
        expr: Box::new(expr),
        negated,
    })
}

fn build_in_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut iter = pair.into_inner().peekable();
    let additive_pair = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?;
    let expr = build_additive(additive_pair)?;

    // Check for NOT keyword
    let mut negated = false;
    let mut list = Vec::new();

    for inner in iter {
        match inner.as_rule() {
            Rule::NOT => negated = true,
            Rule::list_expr => list = build_list_expr(inner)?,
            _ => {}
        }
    }

    Ok(Expression::InList {
        expr: Box::new(expr),
        list,
        negated,
    })
}

fn build_comparison_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut iter = pair.into_inner();
    let first = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?;
    let left = build_additive(first)?;

    // Check if there's a comparison operator
    if let Some(op_pair) = iter.next() {
        let op_span = span_from_pair(&op_pair);
        let op = parse_comp_op(&op_pair)?;
        let right_pair = iter
            .next()
            .ok_or_else(|| ParseError::missing_clause("right operand", op_span))?;
        let right = build_additive(right_pair)?;
        Ok(Expression::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    } else {
        Ok(left)
    }
}

fn parse_comp_op(pair: &pest::iterators::Pair<Rule>) -> Result<BinaryOperator, ParseError> {
    // The comp_op rule contains one of the operator rules
    let span = span_from_pair(pair);
    let inner =
        pair.clone().into_inner().next().ok_or_else(|| {
            ParseError::unexpected_token(span, pair.as_str(), "comparison operator")
        })?;

    match inner.as_rule() {
        Rule::eq => Ok(BinaryOperator::Eq),
        Rule::neq => Ok(BinaryOperator::Neq),
        Rule::lt => Ok(BinaryOperator::Lt),
        Rule::lte => Ok(BinaryOperator::Lte),
        Rule::gt => Ok(BinaryOperator::Gt),
        Rule::gte => Ok(BinaryOperator::Gte),
        Rule::CONTAINS => Ok(BinaryOperator::Contains),
        Rule::starts_with => Ok(BinaryOperator::StartsWith),
        Rule::ends_with => Ok(BinaryOperator::EndsWith),
        _ => Err(ParseError::unexpected_token(
            span,
            pair.as_str(),
            "comparison operator",
        )),
    }
}

fn build_additive(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut children: Vec<_> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::missing_clause("expression", pair_span));
    }

    let first = children.remove(0);
    let mut left = build_multiplicative(first)?;

    // Remaining children are: add_op, multiplicative, add_op, multiplicative, ...
    let mut iter = children.into_iter();
    while let Some(op_pair) = iter.next() {
        if op_pair.as_rule() == Rule::add_op {
            let op_span = span_from_pair(&op_pair);
            let op = match op_pair.as_str() {
                "+" => BinaryOperator::Add,
                "-" => BinaryOperator::Sub,
                _ => {
                    return Err(ParseError::unexpected_token(
                        op_span,
                        op_pair.as_str(),
                        "+ or -",
                    ))
                }
            };
            if let Some(right_pair) = iter.next() {
                let right = build_multiplicative(right_pair)?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            }
        }
    }

    Ok(left)
}

fn build_multiplicative(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut children: Vec<_> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::missing_clause("expression", pair_span));
    }

    let first = children.remove(0);
    let mut left = build_power(first)?;

    // Remaining children are: mul_op, power, mul_op, power, ...
    let mut iter = children.into_iter();
    while let Some(op_pair) = iter.next() {
        if op_pair.as_rule() == Rule::mul_op {
            let op_span = span_from_pair(&op_pair);
            let op = match op_pair.as_str() {
                "*" => BinaryOperator::Mul,
                "/" => BinaryOperator::Div,
                "%" => BinaryOperator::Mod,
                _ => {
                    return Err(ParseError::unexpected_token(
                        op_span,
                        op_pair.as_str(),
                        "*, /, or %",
                    ))
                }
            };
            if let Some(right_pair) = iter.next() {
                let right = build_power(right_pair)?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            }
        }
    }

    Ok(left)
}

fn build_power(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut children: Vec<_> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::missing_clause("expression", pair_span));
    }

    let first = children.remove(0);
    let mut left = build_unary(first)?;

    // Remaining children are: pow_op, unary, pow_op, unary, ...
    let mut iter = children.into_iter();
    while let Some(op_pair) = iter.next() {
        if op_pair.as_rule() == Rule::pow_op {
            if let Some(right_pair) = iter.next() {
                let right = build_unary(right_pair)?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Pow,
                    right: Box::new(right),
                };
            }
        }
    }

    Ok(left)
}

fn build_unary(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut negated = false;
    let mut primary_pair = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::neg_op => negated = true,
            Rule::primary => primary_pair = Some(inner),
            _ => {}
        }
    }

    let expr = build_primary(
        primary_pair.ok_or_else(|| ParseError::missing_clause("primary expression", pair_span))?,
    )?;

    if negated {
        Ok(Expression::UnaryOp {
            op: UnaryOperator::Neg,
            expr: Box::new(expr),
        })
    } else {
        Ok(expr)
    }
}

fn build_primary(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::missing_clause("primary expression", pair_span))?;

    let span = span_from_pair(&inner);
    match inner.as_rule() {
        Rule::case_expr => build_case_expr(inner),
        Rule::exists_expr => build_exists_expr(inner),
        Rule::literal => Ok(Expression::Literal(build_literal(inner)?)),
        Rule::function_call => build_function_call(inner),
        Rule::property_access => {
            let mut parts = inner.into_inner();
            let variable = parts
                .next()
                .ok_or_else(|| ParseError::missing_clause("variable", span))?
                .as_str()
                .to_string();
            let property = parts
                .next()
                .ok_or_else(|| ParseError::missing_clause("property", span))?
                .as_str()
                .to_string();
            Ok(Expression::Property { variable, property })
        }
        Rule::variable => Ok(Expression::Variable(inner.as_str().to_string())),
        Rule::paren_expr => {
            // Parenthesized expression - extract the inner expression
            let inner_expr = inner
                .into_inner()
                .next()
                .ok_or_else(|| ParseError::missing_clause("expression", span))?;
            build_expression_from_inner(inner_expr)
        }
        Rule::list_expr => Ok(Expression::List(build_list_expr(inner)?)),
        _ => Err(ParseError::unexpected_token(
            span,
            inner.as_str(),
            "literal, variable, property access, function call, CASE, or EXISTS expression",
        )),
    }
}

/// Build a CASE expression from a pest pair.
///
/// CASE WHEN condition THEN result [WHEN ... THEN ...] [ELSE default] END
fn build_case_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let mut when_clauses = Vec::new();
    let mut else_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::case_when_clause => {
                let (condition, result) = build_case_when_clause(inner)?;
                when_clauses.push((condition, result));
            }
            Rule::case_else_clause => {
                else_clause = Some(Box::new(build_case_else_clause(inner)?));
            }
            _ => {}
        }
    }

    Ok(Expression::Case(CaseExpression {
        when_clauses,
        else_clause,
    }))
}

/// Build a single WHEN/THEN clause from a pest pair.
fn build_case_when_clause(
    pair: pest::iterators::Pair<Rule>,
) -> Result<(Expression, Expression), ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut condition = None;
    let mut result = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            if condition.is_none() {
                condition = Some(build_expression(inner)?);
            } else {
                result = Some(build_expression(inner)?);
            }
        }
    }

    Ok((
        condition.ok_or_else(|| ParseError::missing_clause("WHEN condition", pair_span))?,
        result.ok_or_else(|| ParseError::missing_clause("THEN result", pair_span))?,
    ))
}

/// Build the ELSE clause from a pest pair.
fn build_case_else_clause(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            return build_expression(inner);
        }
    }
    Err(ParseError::missing_clause("ELSE expression", pair_span))
}

/// Build an EXISTS expression from a pest pair.
///
/// EXISTS { pattern } or NOT EXISTS { pattern }
fn build_exists_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut negated = false;
    let mut pattern = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::NOT => negated = true,
            Rule::pattern => pattern = Some(build_pattern(inner)?),
            _ => {}
        }
    }

    Ok(Expression::Exists {
        pattern: pattern
            .ok_or_else(|| ParseError::missing_clause("pattern in EXISTS", pair_span))?,
        negated,
    })
}

fn build_expression_from_inner(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Expression, ParseError> {
    // Handle the expression rule directly (which contains or_expr)
    let pair_span = span_from_pair(&pair);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?;
    build_or_expr(inner)
}

fn build_function_call(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut iter = pair.into_inner();
    let name = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("function name", pair_span))?
        .as_str()
        .to_string();

    let mut args = Vec::new();
    let mut distinct = false;

    // Parse function arguments if present
    if let Some(args_pair) = iter.next() {
        if args_pair.as_rule() == Rule::function_args {
            for arg in args_pair.into_inner() {
                match arg.as_rule() {
                    Rule::star => {
                        // COUNT(*) - represent as Variable("*")
                        args.push(Expression::Variable("*".to_string()));
                    }
                    Rule::DISTINCT => {
                        // DISTINCT keyword
                        distinct = true;
                    }
                    Rule::expression => {
                        args.push(build_expression(arg)?);
                    }
                    _ => {}
                }
            }
        }
    }

    // Check if this is an aggregate function
    let name_upper = name.to_uppercase();
    match name_upper.as_str() {
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "COLLECT" => {
            let func = match name_upper.as_str() {
                "COUNT" => AggregateFunc::Count,
                "SUM" => AggregateFunc::Sum,
                "AVG" => AggregateFunc::Avg,
                "MIN" => AggregateFunc::Min,
                "MAX" => AggregateFunc::Max,
                "COLLECT" => AggregateFunc::Collect,
                _ => unreachable!(),
            };
            let expr = args
                .into_iter()
                .next()
                .unwrap_or(Expression::Variable("*".to_string()));
            Ok(Expression::Aggregate {
                func,
                distinct,
                expr: Box::new(expr),
            })
        }
        _ => Ok(Expression::FunctionCall { name, args }),
    }
}

fn build_list_expr(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Expression>, ParseError> {
    let mut items = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::expression {
            items.push(build_expression(inner)?);
        }
    }
    Ok(items)
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
        let query = parse("MATCH (n {score: 3.15}) RETURN n").unwrap();
        if let PatternElement::Node(node) = &query.match_clause.patterns[0].elements[0] {
            assert_eq!(node.properties[0].1, Literal::Float(3.15));
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

    // ============================================
    // Phase 2.3: RETURN Clause Extensions Tests
    // ============================================

    #[test]
    fn test_parse_return_property_access() {
        let query = parse("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Property { variable, property } =
            &query.return_clause.items[0].expression
        {
            assert_eq!(variable, "n");
            assert_eq!(property, "name");
        } else {
            panic!("Expected property access expression");
        }
        assert!(query.return_clause.items[0].alias.is_none());
    }

    #[test]
    fn test_parse_return_with_alias() {
        let query = parse("MATCH (n:Person) RETURN n.name AS personName").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Property { variable, property } =
            &query.return_clause.items[0].expression
        {
            assert_eq!(variable, "n");
            assert_eq!(property, "name");
        } else {
            panic!("Expected property access expression");
        }
        assert_eq!(
            query.return_clause.items[0].alias,
            Some("personName".to_string())
        );
    }

    #[test]
    fn test_parse_return_multiple_items() {
        let query = parse("MATCH (n:Person) RETURN n.name, n.age").unwrap();
        assert_eq!(query.return_clause.items.len(), 2);

        // First item: n.name
        if let Expression::Property { variable, property } =
            &query.return_clause.items[0].expression
        {
            assert_eq!(variable, "n");
            assert_eq!(property, "name");
        } else {
            panic!("Expected property access expression for first item");
        }

        // Second item: n.age
        if let Expression::Property { variable, property } =
            &query.return_clause.items[1].expression
        {
            assert_eq!(variable, "n");
            assert_eq!(property, "age");
        } else {
            panic!("Expected property access expression for second item");
        }
    }

    #[test]
    fn test_parse_return_multiple_items_with_aliases() {
        let query = parse("MATCH (n:Person) RETURN n.name AS name, n.age AS years").unwrap();
        assert_eq!(query.return_clause.items.len(), 2);

        // First item: n.name AS name
        if let Expression::Property { variable, property } =
            &query.return_clause.items[0].expression
        {
            assert_eq!(variable, "n");
            assert_eq!(property, "name");
        }
        assert_eq!(query.return_clause.items[0].alias, Some("name".to_string()));

        // Second item: n.age AS years
        if let Expression::Property { variable, property } =
            &query.return_clause.items[1].expression
        {
            assert_eq!(variable, "n");
            assert_eq!(property, "age");
        }
        assert_eq!(
            query.return_clause.items[1].alias,
            Some("years".to_string())
        );
    }

    #[test]
    fn test_parse_return_variable_still_works() {
        // Ensure returning just a variable still works
        let query = parse("MATCH (n:Person) RETURN n").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Variable(v) = &query.return_clause.items[0].expression {
            assert_eq!(v, "n");
        } else {
            panic!("Expected variable expression");
        }
    }

    #[test]
    fn test_parse_return_variable_with_alias() {
        let query = parse("MATCH (n:Person) RETURN n AS person").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Variable(v) = &query.return_clause.items[0].expression {
            assert_eq!(v, "n");
        } else {
            panic!("Expected variable expression");
        }
        assert_eq!(
            query.return_clause.items[0].alias,
            Some("person".to_string())
        );
    }

    #[test]
    fn test_parse_return_literal() {
        // Return a literal value
        let query = parse("MATCH (n:Person) RETURN 'hello'").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Literal(lit) = &query.return_clause.items[0].expression {
            assert_eq!(*lit, Literal::String("hello".to_string()));
        } else {
            panic!("Expected literal expression");
        }
    }

    #[test]
    fn test_parse_return_literal_integer() {
        let query = parse("MATCH (n:Person) RETURN 42").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Literal(lit) = &query.return_clause.items[0].expression {
            assert_eq!(*lit, Literal::Int(42));
        } else {
            panic!("Expected literal expression");
        }
    }

    #[test]
    fn test_parse_return_literal_with_alias() {
        let query = parse("MATCH (n:Person) RETURN 100 AS count").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Literal(lit) = &query.return_clause.items[0].expression {
            assert_eq!(*lit, Literal::Int(100));
        } else {
            panic!("Expected literal expression");
        }
        assert_eq!(
            query.return_clause.items[0].alias,
            Some("count".to_string())
        );
    }

    #[test]
    fn test_parse_return_mixed_expressions() {
        // Mix of variable, property access, and literal
        let query = parse("MATCH (n:Person) RETURN n, n.name, 'constant' AS c").unwrap();
        assert_eq!(query.return_clause.items.len(), 3);

        // First: variable n
        if let Expression::Variable(v) = &query.return_clause.items[0].expression {
            assert_eq!(v, "n");
        } else {
            panic!("Expected variable expression");
        }

        // Second: property access n.name
        if let Expression::Property { variable, property } =
            &query.return_clause.items[1].expression
        {
            assert_eq!(variable, "n");
            assert_eq!(property, "name");
        } else {
            panic!("Expected property access expression");
        }

        // Third: literal 'constant' AS c
        if let Expression::Literal(lit) = &query.return_clause.items[2].expression {
            assert_eq!(*lit, Literal::String("constant".to_string()));
        } else {
            panic!("Expected literal expression");
        }
        assert_eq!(query.return_clause.items[2].alias, Some("c".to_string()));
    }

    #[test]
    fn test_parse_return_case_insensitive_as() {
        // AS keyword is case insensitive
        let query = parse("MATCH (n:Person) RETURN n.name as personName").unwrap();
        assert_eq!(
            query.return_clause.items[0].alias,
            Some("personName".to_string())
        );

        let query = parse("MATCH (n:Person) RETURN n.name As personName").unwrap();
        assert_eq!(
            query.return_clause.items[0].alias,
            Some("personName".to_string())
        );

        let query = parse("MATCH (n:Person) RETURN n.name AS personName").unwrap();
        assert_eq!(
            query.return_clause.items[0].alias,
            Some("personName".to_string())
        );
    }

    #[test]
    fn test_parse_return_different_variables() {
        // Return properties from different pattern variables
        let query = parse("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name").unwrap();
        assert_eq!(query.return_clause.items.len(), 2);

        if let Expression::Property { variable, property } =
            &query.return_clause.items[0].expression
        {
            assert_eq!(variable, "a");
            assert_eq!(property, "name");
        }

        if let Expression::Property { variable, property } =
            &query.return_clause.items[1].expression
        {
            assert_eq!(variable, "b");
            assert_eq!(property, "name");
        }
    }

    // ============================================
    // Phase 3.1 & 3.2: WHERE Clause and Expression Tests
    // ============================================

    #[test]
    fn test_parse_where_simple_comparison() {
        let query = parse("MATCH (p:Person) WHERE p.age > 30 RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { left, op, right } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Gt));
            if let Expression::Property { variable, property } = *left {
                assert_eq!(variable, "p");
                assert_eq!(property, "age");
            } else {
                panic!("Expected property access on left side");
            }
            if let Expression::Literal(Literal::Int(n)) = *right {
                assert_eq!(n, 30);
            } else {
                panic!("Expected integer literal on right side");
            }
        } else {
            panic!("Expected binary comparison expression");
        }
    }

    #[test]
    fn test_parse_where_equality() {
        let query = parse("MATCH (p:Person) WHERE p.name = 'Alice' RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { left, op, right } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Eq));
            if let Expression::Property { variable, property } = *left {
                assert_eq!(variable, "p");
                assert_eq!(property, "name");
            }
            if let Expression::Literal(Literal::String(s)) = *right {
                assert_eq!(s, "Alice");
            }
        } else {
            panic!("Expected binary comparison expression");
        }
    }

    #[test]
    fn test_parse_where_and() {
        let query =
            parse("MATCH (p:Person) WHERE p.age > 30 AND p.name = 'Alice' RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { left, op, right } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::And));
            // Left side: p.age > 30
            if let Expression::BinaryOp { op: left_op, .. } = *left {
                assert!(matches!(left_op, BinaryOperator::Gt));
            } else {
                panic!("Expected binary op on left side of AND");
            }
            // Right side: p.name = 'Alice'
            if let Expression::BinaryOp { op: right_op, .. } = *right {
                assert!(matches!(right_op, BinaryOperator::Eq));
            } else {
                panic!("Expected binary op on right side of AND");
            }
        } else {
            panic!("Expected AND expression");
        }
    }

    #[test]
    fn test_parse_where_or() {
        let query = parse("MATCH (p:Person) WHERE p.age < 20 OR p.age > 60 RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Or));
        } else {
            panic!("Expected OR expression");
        }
    }

    #[test]
    fn test_parse_where_not() {
        let query = parse("MATCH (p:Person) WHERE NOT p.active = true RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::UnaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, UnaryOperator::Not));
        } else {
            panic!("Expected NOT expression");
        }
    }

    #[test]
    fn test_parse_where_is_null() {
        let query = parse("MATCH (p:Person) WHERE p.email IS NULL RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::IsNull { expr, negated } = where_clause.expression {
            assert!(!negated);
            if let Expression::Property { variable, property } = *expr {
                assert_eq!(variable, "p");
                assert_eq!(property, "email");
            } else {
                panic!("Expected property access");
            }
        } else {
            panic!("Expected IS NULL expression");
        }
    }

    #[test]
    fn test_parse_where_is_not_null() {
        let query = parse("MATCH (p:Person) WHERE p.email IS NOT NULL RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::IsNull { negated, .. } = where_clause.expression {
            assert!(negated);
        } else {
            panic!("Expected IS NOT NULL expression");
        }
    }

    #[test]
    fn test_parse_where_in_list() {
        let query =
            parse("MATCH (p:Person) WHERE p.status IN ['active', 'pending'] RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::InList {
            expr,
            list,
            negated,
        } = where_clause.expression
        {
            assert!(!negated);
            if let Expression::Property { variable, property } = *expr {
                assert_eq!(variable, "p");
                assert_eq!(property, "status");
            }
            assert_eq!(list.len(), 2);
        } else {
            panic!("Expected IN list expression");
        }
    }

    #[test]
    fn test_parse_where_not_in_list() {
        let query =
            parse("MATCH (p:Person) WHERE p.status NOT IN ['inactive', 'deleted'] RETURN p")
                .unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::InList { negated, .. } = where_clause.expression {
            assert!(negated);
        } else {
            panic!("Expected NOT IN list expression");
        }
    }

    #[test]
    fn test_parse_where_contains() {
        let query = parse("MATCH (p:Person) WHERE p.name CONTAINS 'son' RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Contains));
        } else {
            panic!("Expected CONTAINS expression");
        }
    }

    #[test]
    fn test_parse_where_starts_with() {
        let query = parse("MATCH (p:Person) WHERE p.name STARTS WITH 'Al' RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::StartsWith));
        } else {
            panic!("Expected STARTS WITH expression");
        }
    }

    #[test]
    fn test_parse_where_ends_with() {
        let query = parse("MATCH (p:Person) WHERE p.name ENDS WITH 'son' RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::EndsWith));
        } else {
            panic!("Expected ENDS WITH expression");
        }
    }

    #[test]
    fn test_parse_where_arithmetic() {
        let query = parse("MATCH (p:Person) WHERE p.age + 5 > 30 RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { left, op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Gt));
            // Left side should be: p.age + 5
            if let Expression::BinaryOp {
                op: left_op,
                left: inner_left,
                ..
            } = *left
            {
                assert!(matches!(left_op, BinaryOperator::Add));
                if let Expression::Property { property, .. } = *inner_left {
                    assert_eq!(property, "age");
                }
            } else {
                panic!("Expected addition on left side");
            }
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_parse_where_precedence_and_or() {
        // AND has higher precedence than OR
        // a OR b AND c  should parse as  a OR (b AND c)
        let query =
            parse("MATCH (p:Person) WHERE p.x = 1 OR p.y = 2 AND p.z = 3 RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        // Top level should be OR
        if let Expression::BinaryOp { op, right, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Or));
            // Right side should be AND
            if let Expression::BinaryOp { op: right_op, .. } = *right {
                assert!(matches!(right_op, BinaryOperator::And));
            } else {
                panic!("Expected AND on right side of OR");
            }
        } else {
            panic!("Expected OR expression at top level");
        }
    }

    #[test]
    fn test_parse_where_parentheses() {
        // Parentheses override precedence
        let query =
            parse("MATCH (p:Person) WHERE (p.x = 1 OR p.y = 2) AND p.z = 3 RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        // Top level should be AND
        if let Expression::BinaryOp { op, left, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::And));
            // Left side should be OR (from parentheses)
            if let Expression::BinaryOp { op: left_op, .. } = *left {
                assert!(matches!(left_op, BinaryOperator::Or));
            } else {
                panic!("Expected OR on left side of AND");
            }
        } else {
            panic!("Expected AND expression at top level");
        }
    }

    #[test]
    fn test_parse_where_comparison_operators() {
        // Test all comparison operators
        let operators = vec![
            ("=", BinaryOperator::Eq),
            ("<>", BinaryOperator::Neq),
            ("!=", BinaryOperator::Neq),
            ("<", BinaryOperator::Lt),
            ("<=", BinaryOperator::Lte),
            (">", BinaryOperator::Gt),
            (">=", BinaryOperator::Gte),
        ];

        for (op_str, expected_op) in operators {
            let query_str = format!("MATCH (p:Person) WHERE p.age {} 30 RETURN p", op_str);
            let query = parse(&query_str).unwrap();
            assert!(query.where_clause.is_some());

            let where_clause = query.where_clause.unwrap();
            if let Expression::BinaryOp { op, .. } = where_clause.expression {
                assert_eq!(op, expected_op, "Failed for operator: {}", op_str);
            } else {
                panic!("Expected binary comparison for operator: {}", op_str);
            }
        }
    }

    #[test]
    fn test_parse_where_unary_negation() {
        let query = parse("MATCH (p:Person) WHERE p.balance > -100 RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { right, .. } = where_clause.expression {
            if let Expression::UnaryOp { op, expr } = *right {
                assert!(matches!(op, UnaryOperator::Neg));
                if let Expression::Literal(Literal::Int(n)) = *expr {
                    assert_eq!(n, 100);
                }
            } else {
                panic!("Expected unary negation");
            }
        }
    }

    // ============================================
    // Phase 4.1 & 4.2: ORDER BY and LIMIT Tests
    // ============================================

    #[test]
    fn test_parse_order_by_single() {
        let query = parse("MATCH (p:Person) RETURN p ORDER BY p.age").unwrap();
        assert!(query.order_clause.is_some());

        let order_clause = query.order_clause.unwrap();
        assert_eq!(order_clause.items.len(), 1);
        assert!(!order_clause.items[0].descending); // Default is ASC
    }

    #[test]
    fn test_parse_order_by_asc() {
        let query = parse("MATCH (p:Person) RETURN p ORDER BY p.age ASC").unwrap();
        assert!(query.order_clause.is_some());

        let order_clause = query.order_clause.unwrap();
        assert!(!order_clause.items[0].descending);
    }

    #[test]
    fn test_parse_order_by_desc() {
        let query = parse("MATCH (p:Person) RETURN p ORDER BY p.age DESC").unwrap();
        assert!(query.order_clause.is_some());

        let order_clause = query.order_clause.unwrap();
        assert!(order_clause.items[0].descending);
    }

    #[test]
    fn test_parse_order_by_multiple() {
        let query = parse("MATCH (p:Person) RETURN p ORDER BY p.age DESC, p.name ASC").unwrap();
        assert!(query.order_clause.is_some());

        let order_clause = query.order_clause.unwrap();
        assert_eq!(order_clause.items.len(), 2);
        assert!(order_clause.items[0].descending);
        assert!(!order_clause.items[1].descending);
    }

    #[test]
    fn test_parse_limit_simple() {
        let query = parse("MATCH (p:Person) RETURN p LIMIT 10").unwrap();
        assert!(query.limit_clause.is_some());

        let limit_clause = query.limit_clause.unwrap();
        assert_eq!(limit_clause.limit, 10);
        assert!(limit_clause.offset.is_none());
    }

    #[test]
    fn test_parse_limit_with_offset() {
        let query = parse("MATCH (p:Person) RETURN p LIMIT 10 OFFSET 5").unwrap();
        assert!(query.limit_clause.is_some());

        let limit_clause = query.limit_clause.unwrap();
        assert_eq!(limit_clause.limit, 10);
        assert_eq!(limit_clause.offset, Some(5));
    }

    #[test]
    fn test_parse_full_query() {
        // Test all clauses together
        let query = parse(
            "MATCH (p:Person) WHERE p.age > 25 RETURN p.name ORDER BY p.age DESC LIMIT 10 OFFSET 5",
        )
        .unwrap();

        assert!(query.where_clause.is_some());
        assert!(query.order_clause.is_some());
        assert!(query.limit_clause.is_some());

        let order_clause = query.order_clause.unwrap();
        assert!(order_clause.items[0].descending);

        let limit_clause = query.limit_clause.unwrap();
        assert_eq!(limit_clause.limit, 10);
        assert_eq!(limit_clause.offset, Some(5));
    }

    // ============================================
    // Phase 4.4 & 4.5: Aggregate Function Tests
    // ============================================

    #[test]
    fn test_parse_count_star() {
        let query = parse("MATCH (p:Person) RETURN count(*)").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Aggregate { func, expr, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Count));
            if let Expression::Variable(v) = expr.as_ref() {
                assert_eq!(v, "*");
            }
        } else {
            panic!("Expected aggregate expression");
        }
    }

    #[test]
    fn test_parse_count_property() {
        let query = parse("MATCH (p:Person) RETURN count(p.name)").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Aggregate { func, expr, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Count));
            if let Expression::Property { property, .. } = expr.as_ref() {
                assert_eq!(property, "name");
            }
        } else {
            panic!("Expected aggregate expression");
        }
    }

    #[test]
    fn test_parse_sum() {
        let query = parse("MATCH (p:Person) RETURN sum(p.age)").unwrap();

        if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Sum));
        } else {
            panic!("Expected SUM aggregate");
        }
    }

    #[test]
    fn test_parse_avg() {
        let query = parse("MATCH (p:Person) RETURN avg(p.age)").unwrap();

        if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Avg));
        } else {
            panic!("Expected AVG aggregate");
        }
    }

    #[test]
    fn test_parse_min_max() {
        let query = parse("MATCH (p:Person) RETURN min(p.age), max(p.age)").unwrap();
        assert_eq!(query.return_clause.items.len(), 2);

        if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Min));
        }
        if let Expression::Aggregate { func, .. } = &query.return_clause.items[1].expression {
            assert!(matches!(func, AggregateFunc::Max));
        }
    }

    #[test]
    fn test_parse_collect() {
        let query = parse("MATCH (p:Person) RETURN collect(p.name)").unwrap();

        if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
            assert!(matches!(func, AggregateFunc::Collect));
        } else {
            panic!("Expected COLLECT aggregate");
        }
    }

    #[test]
    fn test_parse_aggregate_case_insensitive() {
        // Aggregate function names are case insensitive
        let queries = vec![
            "MATCH (p:Person) RETURN COUNT(*)",
            "MATCH (p:Person) RETURN Count(*)",
            "MATCH (p:Person) RETURN count(*)",
        ];

        for query_str in queries {
            let query = parse(query_str).unwrap();
            if let Expression::Aggregate { func, .. } = &query.return_clause.items[0].expression {
                assert!(matches!(func, AggregateFunc::Count));
            } else {
                panic!("Expected COUNT aggregate for: {}", query_str);
            }
        }
    }

    #[test]
    fn test_parse_count_distinct() {
        let query = parse("MATCH (p:Person) RETURN count(DISTINCT p.city)").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Aggregate {
            func,
            distinct,
            expr,
        } = &query.return_clause.items[0].expression
        {
            assert!(matches!(func, AggregateFunc::Count));
            assert!(distinct, "DISTINCT flag should be true");
            if let Expression::Property { property, .. } = expr.as_ref() {
                assert_eq!(property, "city");
            } else {
                panic!("Expected property expression");
            }
        } else {
            panic!("Expected aggregate expression");
        }
    }

    #[test]
    fn test_parse_collect_distinct() {
        let query = parse("MATCH (p:Person) RETURN collect(DISTINCT p.city)").unwrap();

        if let Expression::Aggregate {
            func,
            distinct,
            expr,
        } = &query.return_clause.items[0].expression
        {
            assert!(matches!(func, AggregateFunc::Collect));
            assert!(distinct, "DISTINCT flag should be true");
            if let Expression::Property { property, .. } = expr.as_ref() {
                assert_eq!(property, "city");
            }
        } else {
            panic!("Expected COLLECT DISTINCT aggregate");
        }
    }

    #[test]
    fn test_parse_distinct_case_insensitive() {
        // DISTINCT keyword is case insensitive
        let queries = vec![
            "MATCH (p:Person) RETURN count(DISTINCT p.name)",
            "MATCH (p:Person) RETURN count(distinct p.name)",
            "MATCH (p:Person) RETURN count(Distinct p.name)",
        ];

        for query_str in queries {
            let query = parse(query_str).unwrap();
            if let Expression::Aggregate { distinct, .. } = &query.return_clause.items[0].expression
            {
                assert!(distinct, "DISTINCT flag should be true for: {}", query_str);
            } else {
                panic!("Expected aggregate expression for: {}", query_str);
            }
        }
    }

    #[test]
    fn test_parse_function_call_non_aggregate() {
        // Non-aggregate functions are parsed as FunctionCall
        let query = parse("MATCH (p:Person) RETURN toUpper(p.name)").unwrap();

        if let Expression::FunctionCall { name, args } = &query.return_clause.items[0].expression {
            assert_eq!(name, "toUpper");
            assert_eq!(args.len(), 1);
        } else {
            panic!("Expected function call expression");
        }
    }

    // ============================================
    // Phase 5.3: RETURN DISTINCT Tests
    // ============================================

    #[test]
    fn test_parse_return_distinct_property() {
        let query = parse("MATCH (p:Person) RETURN DISTINCT p.city").unwrap();
        assert!(query.return_clause.distinct);
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Property { variable, property } =
            &query.return_clause.items[0].expression
        {
            assert_eq!(variable, "p");
            assert_eq!(property, "city");
        } else {
            panic!("Expected property access expression");
        }
    }

    #[test]
    fn test_parse_return_distinct_variable() {
        let query = parse("MATCH (n:Person) RETURN DISTINCT n").unwrap();
        assert!(query.return_clause.distinct);

        if let Expression::Variable(v) = &query.return_clause.items[0].expression {
            assert_eq!(v, "n");
        } else {
            panic!("Expected variable expression");
        }
    }

    #[test]
    fn test_parse_return_distinct_multiple_items() {
        let query = parse("MATCH (p:Person) RETURN DISTINCT p.city, p.country").unwrap();
        assert!(query.return_clause.distinct);
        assert_eq!(query.return_clause.items.len(), 2);

        if let Expression::Property { property, .. } = &query.return_clause.items[0].expression {
            assert_eq!(property, "city");
        }
        if let Expression::Property { property, .. } = &query.return_clause.items[1].expression {
            assert_eq!(property, "country");
        }
    }

    #[test]
    fn test_parse_return_distinct_with_alias() {
        let query = parse("MATCH (p:Person) RETURN DISTINCT p.city AS location").unwrap();
        assert!(query.return_clause.distinct);
        assert_eq!(
            query.return_clause.items[0].alias,
            Some("location".to_string())
        );
    }

    #[test]
    fn test_parse_return_distinct_case_insensitive() {
        // DISTINCT keyword is case insensitive
        let queries = vec![
            "MATCH (p:Person) RETURN DISTINCT p.city",
            "MATCH (p:Person) RETURN distinct p.city",
            "MATCH (p:Person) RETURN Distinct p.city",
        ];

        for query_str in queries {
            let query = parse(query_str).unwrap();
            assert!(
                query.return_clause.distinct,
                "DISTINCT flag should be true for: {}",
                query_str
            );
        }
    }

    #[test]
    fn test_parse_return_without_distinct() {
        // Verify that queries without DISTINCT have distinct = false
        let query = parse("MATCH (p:Person) RETURN p.city").unwrap();
        assert!(!query.return_clause.distinct);
    }

    // ============================================
    // EXISTS Expression Tests
    // ============================================

    #[test]
    fn test_parse_exists_basic() {
        let query = parse("MATCH (p:player) WHERE EXISTS { (p)-[:KNOWS]->() } RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::Exists { negated, pattern } = where_clause.expression {
            assert!(!negated);
            // Pattern should have 3 elements: node, edge, node
            assert_eq!(pattern.elements.len(), 3);
        } else {
            panic!("Expected EXISTS expression");
        }
    }

    #[test]
    fn test_parse_not_exists() {
        let query = parse(
            "MATCH (p:player) WHERE NOT EXISTS { (p)-[:won_championship_with]->() } RETURN p.name",
        )
        .unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        // NOT EXISTS is parsed as NOT applied to EXISTS (which has negated=false)
        // So we get UnaryOp(Not, Exists { negated: false, ... })
        if let Expression::UnaryOp { op, expr } = where_clause.expression {
            assert!(matches!(op, UnaryOperator::Not));
            if let Expression::Exists { negated, pattern } = expr.as_ref() {
                assert!(!negated); // The inner EXISTS is not negated
                assert!(!pattern.elements.is_empty());
            } else {
                panic!("Expected EXISTS expression inside NOT");
            }
        } else if let Expression::Exists { negated, pattern } = where_clause.expression {
            // If the grammar is changed to support NOT directly in exists_expr
            assert!(negated);
            assert!(!pattern.elements.is_empty());
        } else {
            panic!("Expected NOT(EXISTS) or EXISTS(negated=true) expression");
        }
    }

    #[test]
    fn test_parse_exists_with_labels() {
        let query = parse("MATCH (p:player) WHERE EXISTS { (p)-[:played_for]->(t:team) } RETURN p")
            .unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::Exists { pattern, .. } = where_clause.expression {
            // Check the edge has the correct label
            if let PatternElement::Edge(edge) = &pattern.elements[1] {
                assert_eq!(edge.labels, vec!["played_for".to_string()]);
            } else {
                panic!("Expected edge pattern");
            }
            // Check the target node has the correct label
            if let PatternElement::Node(node) = &pattern.elements[2] {
                assert_eq!(node.labels, vec!["team".to_string()]);
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected EXISTS expression");
        }
    }

    #[test]
    fn test_parse_exists_with_properties() {
        let query = parse(
            "MATCH (p:player) WHERE EXISTS { (p)-[:played_for]->(t:team {name: 'Lakers'}) } RETURN p",
        )
        .unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::Exists { pattern, .. } = where_clause.expression {
            // Check the target node has properties
            if let PatternElement::Node(node) = &pattern.elements[2] {
                assert_eq!(node.properties.len(), 1);
                assert_eq!(node.properties[0].0, "name");
                assert_eq!(node.properties[0].1, Literal::String("Lakers".to_string()));
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected EXISTS expression");
        }
    }

    #[test]
    fn test_parse_exists_combined_with_and() {
        let query = parse(
            "MATCH (p:player) WHERE p.age > 30 AND EXISTS { (p)-[:won_championship]->() } RETURN p",
        )
        .unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        // Top level should be AND
        if let Expression::BinaryOp { op, right, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::And));
            // Right side should be EXISTS
            if let Expression::Exists { .. } = *right {
                // Good
            } else {
                panic!("Expected EXISTS on right side of AND");
            }
        } else {
            panic!("Expected AND expression");
        }
    }

    #[test]
    fn test_parse_exists_case_insensitive() {
        // EXISTS keyword is case insensitive
        let queries = vec![
            "MATCH (p:player) WHERE EXISTS { (p)-[:KNOWS]->() } RETURN p",
            "MATCH (p:player) WHERE exists { (p)-[:KNOWS]->() } RETURN p",
            "MATCH (p:player) WHERE Exists { (p)-[:KNOWS]->() } RETURN p",
        ];

        for query_str in queries {
            let query = parse(query_str).unwrap();
            assert!(query.where_clause.is_some());
            let where_clause = query.where_clause.unwrap();
            assert!(
                matches!(where_clause.expression, Expression::Exists { .. }),
                "Expected EXISTS expression for: {}",
                query_str
            );
        }
    }

    #[test]
    fn test_parse_exists_incoming_edge() {
        let query =
            parse("MATCH (t:team) WHERE EXISTS { (t)<-[:played_for]-() } RETURN t").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::Exists { pattern, .. } = where_clause.expression {
            // Check the edge direction
            if let PatternElement::Edge(edge) = &pattern.elements[1] {
                assert_eq!(edge.direction, EdgeDirection::Incoming);
            } else {
                panic!("Expected edge pattern");
            }
        } else {
            panic!("Expected EXISTS expression");
        }
    }

    #[test]
    fn test_parse_exists_bidirectional_edge() {
        let query = parse("MATCH (n) WHERE EXISTS { (n)-[:KNOWS]-() } RETURN n").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::Exists { pattern, .. } = where_clause.expression {
            // Check the edge direction
            if let PatternElement::Edge(edge) = &pattern.elements[1] {
                assert_eq!(edge.direction, EdgeDirection::Both);
            } else {
                panic!("Expected edge pattern");
            }
        } else {
            panic!("Expected EXISTS expression");
        }
    }

    // ============================================
    // GROUP BY Clause Tests
    // ============================================

    #[test]
    fn test_parse_group_by_single() {
        let query =
            parse("MATCH (p:player) RETURN p.position, count(*) GROUP BY p.position").unwrap();
        assert!(query.group_by_clause.is_some());

        let group_by = query.group_by_clause.unwrap();
        assert_eq!(group_by.expressions.len(), 1);

        if let Expression::Property { variable, property } = &group_by.expressions[0] {
            assert_eq!(variable, "p");
            assert_eq!(property, "position");
        } else {
            panic!("Expected property expression");
        }
    }

    #[test]
    fn test_parse_group_by_multiple() {
        let query = parse(
            "MATCH (p:player) RETURN p.position, p.team, count(*) GROUP BY p.position, p.team",
        )
        .unwrap();
        assert!(query.group_by_clause.is_some());

        let group_by = query.group_by_clause.unwrap();
        assert_eq!(group_by.expressions.len(), 2);

        if let Expression::Property { property, .. } = &group_by.expressions[0] {
            assert_eq!(property, "position");
        }
        if let Expression::Property { property, .. } = &group_by.expressions[1] {
            assert_eq!(property, "team");
        }
    }

    #[test]
    fn test_parse_group_by_with_avg() {
        let query =
            parse("MATCH (p:player) RETURN p.position, avg(p.points_per_game) GROUP BY p.position")
                .unwrap();
        assert!(query.group_by_clause.is_some());

        // Verify RETURN clause has both items
        assert_eq!(query.return_clause.items.len(), 2);

        // First item should be property access
        if let Expression::Property { property, .. } = &query.return_clause.items[0].expression {
            assert_eq!(property, "position");
        } else {
            panic!("Expected property expression");
        }

        // Second item should be AVG aggregate
        if let Expression::Aggregate { func, .. } = &query.return_clause.items[1].expression {
            assert!(matches!(func, AggregateFunc::Avg));
        } else {
            panic!("Expected AVG aggregate");
        }
    }

    #[test]
    fn test_parse_group_by_with_alias() {
        let query =
            parse("MATCH (p:player) RETURN p.position AS pos, count(*) AS cnt GROUP BY p.position")
                .unwrap();
        assert!(query.group_by_clause.is_some());

        // Verify aliases
        assert_eq!(query.return_clause.items[0].alias, Some("pos".to_string()));
        assert_eq!(query.return_clause.items[1].alias, Some("cnt".to_string()));
    }

    #[test]
    fn test_parse_group_by_case_insensitive() {
        // GROUP BY keywords are case insensitive
        let queries = vec![
            "MATCH (p:player) RETURN p.position, count(*) GROUP BY p.position",
            "MATCH (p:player) RETURN p.position, count(*) group by p.position",
            "MATCH (p:player) RETURN p.position, count(*) Group By p.position",
        ];

        for query_str in queries {
            let query = parse(query_str).unwrap();
            assert!(
                query.group_by_clause.is_some(),
                "Expected GROUP BY clause for: {}",
                query_str
            );
        }
    }

    #[test]
    fn test_parse_group_by_with_order_by() {
        let query = parse(
            "MATCH (p:player) RETURN p.position, count(*) AS cnt GROUP BY p.position ORDER BY cnt DESC",
        )
        .unwrap();
        assert!(query.group_by_clause.is_some());
        assert!(query.order_clause.is_some());

        let order = query.order_clause.unwrap();
        assert!(order.items[0].descending);
    }

    #[test]
    fn test_parse_group_by_with_limit() {
        let query =
            parse("MATCH (p:player) RETURN p.position, count(*) GROUP BY p.position LIMIT 5")
                .unwrap();
        assert!(query.group_by_clause.is_some());
        assert!(query.limit_clause.is_some());

        let limit = query.limit_clause.unwrap();
        assert_eq!(limit.limit, 5);
    }

    #[test]
    fn test_parse_group_by_full_query() {
        let query = parse(
            "MATCH (p:player) WHERE p.active = true RETURN p.position, count(*) AS cnt GROUP BY p.position ORDER BY cnt DESC LIMIT 10",
        )
        .unwrap();
        assert!(query.where_clause.is_some());
        assert!(query.group_by_clause.is_some());
        assert!(query.order_clause.is_some());
        assert!(query.limit_clause.is_some());
    }

    #[test]
    fn test_parse_group_by_variable() {
        // GROUP BY a variable instead of a property
        let query = parse("MATCH (p:player) RETURN p, count(*) GROUP BY p").unwrap();
        assert!(query.group_by_clause.is_some());

        let group_by = query.group_by_clause.unwrap();
        if let Expression::Variable(v) = &group_by.expressions[0] {
            assert_eq!(v, "p");
        } else {
            panic!("Expected variable expression");
        }
    }

    #[test]
    fn test_parse_without_group_by() {
        // Verify that queries without GROUP BY have group_by_clause = None
        let query = parse("MATCH (p:player) RETURN p.name").unwrap();
        assert!(query.group_by_clause.is_none());
    }
}
