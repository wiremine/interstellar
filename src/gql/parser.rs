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
//! use intersteller::gql::parse;
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
//! use intersteller::gql::{parse, ParseError};
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
    AggregateFunc, AlterEdgeType, AlterNodeType, AlterTypeAction, BinaryOperator, CallBody,
    CallClause, CallQuery, CaseExpression, CreateClause, CreateEdgeType, CreateNodeType,
    DdlStatement, DeleteClause, DetachDeleteClause, DropType, EdgeDirection, EdgePattern,
    Expression, ForeachClause, ForeachMutation, GroupByClause, HavingClause, ImportingWith,
    LetClause, LimitClause, Literal, MatchClause, MergeClause, MutationClause, MutationQuery,
    NodePattern, OptionalMatchClause, OrderClause, OrderItem, PathQuantifier, Pattern,
    PatternElement, PropertyDefinition, PropertyRef, PropertyTypeAst, Query, RemoveClause,
    ReturnClause, ReturnItem, SetClause, SetItem, SetValidation, Statement, UnaryOperator,
    UnwindClause, ValidationModeAst, WhereClause, WithClause, WithPathClause,
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
/// use intersteller::gql::parse;
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
/// use intersteller::gql::parse;
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
        Statement::Ddl(_) => {
            // For backward compatibility, parse() returns Query
            // Use parse_statement() for DDL statements
            Err(ParseError::Syntax(
                "Use parse_statement() for DDL statements".to_string(),
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
/// use intersteller::gql::parse_statement;
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
    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::read_statement => {
                return build_read_statement(inner);
            }
            Rule::mutation_statement => {
                return build_mutation_statement(inner);
            }
            Rule::ddl_statement => {
                return build_ddl_statement(inner);
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

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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
        foreach_clauses: vec![],
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
    let mut foreach_clauses = Vec::new();
    let mut return_clause = None;

    for inner in pair.clone().into_inner() {
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
            Rule::foreach_clause => {
                foreach_clauses.push(build_foreach_clause(inner)?);
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
        foreach_clauses,
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

    for inner in pair.clone().into_inner() {
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
        foreach_clauses: vec![],
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

    for inner in pair.clone().into_inner() {
        if inner.as_rule() == Rule::pattern {
            patterns.push(build_pattern(inner)?);
        }
    }

    Ok(CreateClause { patterns })
}

/// Build a SET clause.
fn build_set_clause(pair: pest::iterators::Pair<Rule>) -> Result<SetClause, ParseError> {
    let mut items = Vec::new();

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
        if inner.as_rule() == Rule::set_item {
            items.push(build_set_item(inner)?);
        }
    }

    Ok(items)
}

/// Build a REMOVE clause.
fn build_remove_clause(pair: pest::iterators::Pair<Rule>) -> Result<RemoveClause, ParseError> {
    let mut properties = Vec::new();

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
        if inner.as_rule() == Rule::variable {
            variables.push(inner.as_str().to_string());
        }
    }

    Ok(DetachDeleteClause { variables })
}

/// Build a FOREACH clause.
///
/// Grammar: `FOREACH ~ "(" ~ identifier ~ IN ~ expression ~ pipe_token ~ foreach_mutation+ ~ ")"`
///
/// Parses FOREACH clauses that iterate over a list and apply mutations.
///
/// # Examples
///
/// ```text
/// FOREACH (n IN nodes(p) | SET n.visited = true)
/// FOREACH (i IN items | SET i.x = 1 REMOVE i.y)
/// ```
fn build_foreach_clause(pair: pest::iterators::Pair<Rule>) -> Result<ForeachClause, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut variable = None;
    let mut list = None;
    let mut mutations = Vec::new();

    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::identifier => {
                if variable.is_none() {
                    variable = Some(inner.as_str().to_string());
                }
            }
            Rule::expression => {
                if list.is_none() {
                    list = Some(build_expression(inner)?);
                }
            }
            Rule::pipe_token => {
                // Skip the pipe token
            }
            Rule::foreach_mutation => {
                mutations.push(build_foreach_mutation(inner)?);
            }
            _ => {}
        }
    }

    Ok(ForeachClause {
        variable: variable
            .ok_or_else(|| ParseError::missing_clause("FOREACH variable", pair_span))?,
        list: list
            .ok_or_else(|| ParseError::missing_clause("FOREACH list expression", pair_span))?,
        mutations,
    })
}

/// Build a single FOREACH mutation (SET, REMOVE, DELETE, DETACH DELETE, CREATE, or nested FOREACH).
fn build_foreach_mutation(
    pair: pest::iterators::Pair<Rule>,
) -> Result<ForeachMutation, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::Empty)?;

    match inner.as_rule() {
        Rule::set_clause => Ok(ForeachMutation::Set(build_set_clause(inner)?)),
        Rule::remove_clause => Ok(ForeachMutation::Remove(build_remove_clause(inner)?)),
        Rule::delete_clause => Ok(ForeachMutation::Delete(build_delete_clause(inner)?)),
        Rule::detach_delete_clause => Ok(ForeachMutation::DetachDelete(
            build_detach_delete_clause(inner)?,
        )),
        Rule::create_clause => Ok(ForeachMutation::Create(build_create_clause(inner)?)),
        Rule::nested_foreach => Ok(ForeachMutation::Foreach(Box::new(build_nested_foreach(
            inner,
        )?))),
        _ => Err(ParseError::Syntax(format!(
            "Unexpected FOREACH mutation type: {:?}",
            inner.as_rule()
        ))),
    }
}

/// Build a nested FOREACH clause (same structure as regular FOREACH).
fn build_nested_foreach(pair: pest::iterators::Pair<Rule>) -> Result<ForeachClause, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut variable = None;
    let mut list = None;
    let mut mutations = Vec::new();

    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::identifier => {
                if variable.is_none() {
                    variable = Some(inner.as_str().to_string());
                }
            }
            Rule::expression => {
                if list.is_none() {
                    list = Some(build_expression(inner)?);
                }
            }
            Rule::pipe_token => {
                // Skip the pipe token
            }
            Rule::foreach_mutation => {
                mutations.push(build_foreach_mutation(inner)?);
            }
            _ => {}
        }
    }

    Ok(ForeachClause {
        variable: variable
            .ok_or_else(|| ParseError::missing_clause("FOREACH variable", pair_span))?,
        list: list
            .ok_or_else(|| ParseError::missing_clause("FOREACH list expression", pair_span))?,
        mutations,
    })
}

fn build_query(pair: pest::iterators::Pair<Rule>) -> Result<Query, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut match_clause = None;
    let mut optional_match_clauses = Vec::new();
    let mut with_path_clause = None;
    let mut unwind_clauses = Vec::new();
    let mut where_clause = None;
    let mut call_clauses = Vec::new();
    let mut let_clauses = Vec::new();
    let mut with_clauses = Vec::new();
    let mut return_clause = None;
    let mut group_by_clause = None;
    let mut having_clause = None;
    let mut order_clause = None;
    let mut limit_clause = None;

    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::match_clause => match_clause = Some(build_match_clause(inner)?),
            Rule::optional_match_clause => {
                optional_match_clauses.push(build_optional_match_clause(inner)?);
            }
            Rule::with_path_clause => with_path_clause = Some(build_with_path_clause(inner)?),
            Rule::unwind_clause => unwind_clauses.push(build_unwind_clause(inner)?),
            Rule::where_clause => where_clause = Some(build_where_clause(inner)?),
            Rule::call_clause => call_clauses.push(build_call_clause(inner)?),
            Rule::let_clause => let_clauses.push(build_let_clause(inner)?),
            Rule::with_clause => with_clauses.push(build_with_clause(inner)?),
            Rule::return_clause => return_clause = Some(build_return_clause(inner)?),
            Rule::group_by_clause => group_by_clause = Some(build_group_by_clause(inner)?),
            Rule::having_clause => having_clause = Some(build_having_clause(inner)?),
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
        call_clauses,
        let_clauses,
        with_clauses,
        return_clause: return_clause
            .ok_or_else(|| ParseError::missing_clause("RETURN", pair_span))?,
        group_by_clause,
        having_clause,
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

fn build_let_clause(pair: pest::iterators::Pair<Rule>) -> Result<LetClause, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut variable = None;
    let mut expression = None;

    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::identifier => variable = Some(inner.as_str().to_string()),
            Rule::expression => expression = Some(build_expression(inner)?),
            _ => {}
        }
    }

    Ok(LetClause {
        variable: variable.ok_or_else(|| ParseError::missing_clause("variable", pair_span))?,
        expression: expression
            .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?,
    })
}

fn build_group_by_clause(pair: pest::iterators::Pair<Rule>) -> Result<GroupByClause, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut expressions = Vec::new();

    for inner in pair.clone().into_inner() {
        if inner.as_rule() == Rule::expression {
            expressions.push(build_expression(inner)?);
        }
    }

    if expressions.is_empty() {
        return Err(ParseError::missing_clause("GROUP BY expression", pair_span));
    }

    Ok(GroupByClause { expressions })
}

fn build_having_clause(pair: pest::iterators::Pair<Rule>) -> Result<HavingClause, ParseError> {
    let pair_span = span_from_pair(&pair);
    let expr_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::expression)
        .ok_or_else(|| ParseError::missing_clause("HAVING expression", pair_span))?;

    Ok(HavingClause {
        expression: build_expression(expr_pair)?,
    })
}

fn build_order_clause(pair: pest::iterators::Pair<Rule>) -> Result<OrderClause, ParseError> {
    let mut items = Vec::new();

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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
    let mut limit = None;
    let mut offset = None;

    let children: Vec<_> = pair.clone().into_inner().collect();
    let mut i = 0;

    while i < children.len() {
        let child = &children[i];
        match child.as_rule() {
            Rule::LIMIT => {
                // Next child should be an integer for LIMIT
                if i + 1 < children.len() && children[i + 1].as_rule() == Rule::integer {
                    let span = span_from_pair(&children[i + 1]);
                    let n: u64 = children[i + 1].as_str().parse().map_err(|_| {
                        ParseError::invalid_literal(
                            children[i + 1].as_str(),
                            span,
                            "expected unsigned integer",
                        )
                    })?;
                    limit = Some(n);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            Rule::OFFSET | Rule::SKIP => {
                // Next child should be an integer for OFFSET/SKIP
                if i + 1 < children.len() && children[i + 1].as_rule() == Rule::integer {
                    let span = span_from_pair(&children[i + 1]);
                    let n: u64 = children[i + 1].as_str().parse().map_err(|_| {
                        ParseError::invalid_literal(
                            children[i + 1].as_str(),
                            span,
                            "expected unsigned integer",
                        )
                    })?;
                    offset = Some(n);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            Rule::integer => {
                // This handles the case where the grammar has already consumed the keyword
                // Just move on; we process integers after keywords
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    // Default limit to 0 if only SKIP/OFFSET was provided (edge case)
    // But grammatically we always have at least one of LIMIT or SKIP/OFFSET
    Ok(LimitClause {
        limit: limit.unwrap_or(0),
        offset,
    })
}

fn build_match_clause(pair: pest::iterators::Pair<Rule>) -> Result<MatchClause, ParseError> {
    let mut patterns = Vec::new();

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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

/// Build a WITH clause from a pest pair.
///
/// WITH [DISTINCT] items [WHERE condition] [ORDER BY ...] [LIMIT ...]
///
/// The WITH clause pipes results between query parts, projecting specified
/// columns forward. It resets variable scope - only explicitly listed variables
/// are available in subsequent clauses.
///
/// # Examples
///
/// ```text
/// WITH p, COUNT(f) AS friendCount
/// WITH DISTINCT friend.city AS city
/// WITH p ORDER BY p.score DESC LIMIT 10
/// WITH p, cnt WHERE cnt > 5
/// ```
fn build_with_clause(pair: pest::iterators::Pair<Rule>) -> Result<WithClause, ParseError> {
    let mut distinct = false;
    let mut items = Vec::new();
    let mut where_clause = None;
    let mut order_clause = None;
    let mut limit_clause = None;

    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::DISTINCT => distinct = true,
            Rule::return_item => items.push(build_return_item(inner)?),
            Rule::with_where_clause => {
                // Extract the expression from with_where_clause
                let where_span = span_from_pair(&inner);
                let expr_pair = inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::expression)
                    .ok_or_else(|| ParseError::missing_clause("WHERE expression", where_span))?;
                where_clause = Some(WhereClause {
                    expression: build_expression(expr_pair)?,
                });
            }
            Rule::order_clause => order_clause = Some(build_order_clause(inner)?),
            Rule::with_limit_clause => limit_clause = Some(build_with_limit_clause(inner)?),
            _ => {}
        }
    }

    Ok(WithClause {
        distinct,
        items,
        where_clause,
        order_clause,
        limit_clause,
    })
}

/// Build a LIMIT clause from within a WITH clause.
/// Uses with_limit_clause rule to avoid ambiguity with main limit_clause.
fn build_with_limit_clause(pair: pest::iterators::Pair<Rule>) -> Result<LimitClause, ParseError> {
    let mut limit = None;
    let mut offset = None;

    let children: Vec<_> = pair.clone().into_inner().collect();
    let mut i = 0;

    while i < children.len() {
        let child = &children[i];
        match child.as_rule() {
            Rule::LIMIT => {
                // Next child should be an integer for LIMIT
                if i + 1 < children.len() && children[i + 1].as_rule() == Rule::integer {
                    let span = span_from_pair(&children[i + 1]);
                    let n: u64 = children[i + 1].as_str().parse().map_err(|_| {
                        ParseError::invalid_literal(
                            children[i + 1].as_str(),
                            span,
                            "expected unsigned integer",
                        )
                    })?;
                    limit = Some(n);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            Rule::OFFSET | Rule::SKIP => {
                // Next child should be an integer for OFFSET/SKIP
                if i + 1 < children.len() && children[i + 1].as_rule() == Rule::integer {
                    let span = span_from_pair(&children[i + 1]);
                    let n: u64 = children[i + 1].as_str().parse().map_err(|_| {
                        ParseError::invalid_literal(
                            children[i + 1].as_str(),
                            span,
                            "expected unsigned integer",
                        )
                    })?;
                    offset = Some(n);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            Rule::integer => {
                // This handles the case where the grammar has already consumed the keyword
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    Ok(LimitClause {
        limit: limit.unwrap_or(0),
        offset,
    })
}

// ============================================================
// CALL Subquery Parser Functions
// ============================================================

/// Build a CALL clause from a pest pair.
///
/// Grammar: `CALL { call_body }`
///
/// CALL subqueries execute a nested query for each row in the outer query.
/// The subquery can import variables from the outer scope using WITH.
fn build_call_clause(pair: pest::iterators::Pair<Rule>) -> Result<CallClause, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut body = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::call_body {
            body = Some(build_call_body(inner)?);
        }
    }

    Ok(CallClause {
        body: body.ok_or_else(|| ParseError::missing_clause("CALL body", pair_span))?,
    })
}

/// Build a CALL body from a pest pair.
///
/// Grammar: `call_query ~ (union_clause ~ call_query)*`
///
/// The body can be a single query or a UNION of multiple queries.
fn build_call_body(pair: pest::iterators::Pair<Rule>) -> Result<CallBody, ParseError> {
    let mut queries = Vec::new();
    let mut union_all = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::call_query => {
                queries.push(build_call_query(inner)?);
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

    if queries.len() == 1 {
        Ok(CallBody::Single(Box::new(queries.pop().unwrap())))
    } else {
        Ok(CallBody::Union {
            queries,
            all: union_all,
        })
    }
}

/// Build a CALL query from a pest pair.
///
/// Grammar: `importing_with? ~ match_clause? ~ optional_match_clause* ~ where_clause? ~
///           call_clause* ~ with_clause* ~ return_clause ~ order_clause? ~ limit_clause?`
///
/// A query inside a CALL subquery. MATCH is optional (can just transform imported variables).
fn build_call_query(pair: pest::iterators::Pair<Rule>) -> Result<CallQuery, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut importing_with = None;
    let mut match_clause = None;
    let mut optional_match_clauses = Vec::new();
    let mut where_clause = None;
    let mut call_clauses = Vec::new();
    let mut with_clauses = Vec::new();
    let mut return_clause = None;
    let mut order_clause = None;
    let mut limit_clause = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::importing_with => importing_with = Some(build_importing_with(inner)?),
            Rule::match_clause => match_clause = Some(build_match_clause(inner)?),
            Rule::optional_match_clause => {
                optional_match_clauses.push(build_optional_match_clause(inner)?);
            }
            Rule::where_clause => where_clause = Some(build_where_clause(inner)?),
            Rule::call_clause => call_clauses.push(build_call_clause(inner)?),
            Rule::with_clause => with_clauses.push(build_with_clause(inner)?),
            Rule::return_clause => return_clause = Some(build_return_clause(inner)?),
            Rule::order_clause => order_clause = Some(build_order_clause(inner)?),
            Rule::limit_clause => limit_clause = Some(build_limit_clause(inner)?),
            _ => {}
        }
    }

    Ok(CallQuery {
        importing_with,
        match_clause,
        optional_match_clauses,
        where_clause,
        call_clauses,
        with_clauses,
        return_clause: return_clause
            .ok_or_else(|| ParseError::missing_clause("RETURN in CALL subquery", pair_span))?,
        order_clause,
        limit_clause,
    })
}

/// Build an importing WITH clause from a pest pair.
///
/// Grammar: `WITH ~ !PATH ~ return_item ~ ("," ~ return_item)*`
///
/// The importing WITH clause brings variables from the outer scope into the subquery.
/// This makes the CALL subquery "correlated" - it runs once per outer row.
fn build_importing_with(pair: pest::iterators::Pair<Rule>) -> Result<ImportingWith, ParseError> {
    let mut items = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::return_item {
            items.push(build_return_item(inner)?);
        }
    }

    Ok(ImportingWith { items })
}

fn build_pattern(pair: pest::iterators::Pair<Rule>) -> Result<Pattern, ParseError> {
    let mut elements = Vec::new();

    for inner in pair.clone().into_inner() {
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
    let mut where_clause = None;

    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::label_filter => {
                labels = build_labels(inner)?;
            }
            Rule::property_filter => {
                properties = build_properties(inner)?;
            }
            Rule::inline_where => {
                where_clause = Some(build_inline_where(inner)?);
            }
            _ => {}
        }
    }

    Ok(NodePattern {
        variable,
        labels,
        properties,
        where_clause,
    })
}

fn build_edge_pattern(pair: pest::iterators::Pair<Rule>) -> Result<EdgePattern, ParseError> {
    let mut variable = None;
    let mut labels = Vec::new();
    let mut quantifier = None;
    let mut properties = Vec::new();
    let mut where_clause = None;

    let mut has_left = false;
    let mut has_right = false;

    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::left_arrow => has_left = true,
            Rule::right_arrow => has_right = true,
            Rule::variable => variable = Some(inner.as_str().to_string()),
            Rule::label_filter => {
                labels = build_labels(inner)?;
            }
            Rule::quantifier => quantifier = Some(build_quantifier(inner)?),
            Rule::property_filter => properties = build_properties(inner)?,
            Rule::inline_where => {
                where_clause = Some(build_inline_where(inner)?);
            }
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
        where_clause,
    })
}

fn build_labels(pair: pest::iterators::Pair<Rule>) -> Result<Vec<String>, ParseError> {
    let mut labels = Vec::new();
    for inner in pair.clone().into_inner() {
        if inner.as_rule() == Rule::identifier {
            labels.push(inner.as_str().to_string());
        }
    }
    Ok(labels)
}

fn build_quantifier(pair: pest::iterators::Pair<Rule>) -> Result<PathQuantifier, ParseError> {
    let mut min = None;
    let mut max = None;

    for inner in pair.clone().into_inner() {
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

/// Build an inline WHERE expression from a pest pair.
///
/// Parses `WHERE expression` within node/edge patterns.
fn build_inline_where(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let expr_pair = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::expression)
        .ok_or_else(|| ParseError::missing_clause("inline WHERE expression", pair_span))?;

    build_expression(expr_pair)
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

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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
        Rule::regex_expr => build_regex_expr(inner),
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
    let concat_pair = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?;
    let expr = build_concat_expr(concat_pair)?;

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
    let concat_pair = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?;
    let expr = build_concat_expr(concat_pair)?;

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

/// Build a regex match expression from a pest pair.
///
/// Grammar: `concat_expr ~ regex_op ~ concat_expr`
/// Example: `p.email =~ '.*@gmail\\.com$'`
fn build_regex_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut iter = pair.into_inner();

    // First operand (the string to match against)
    let left_pair = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("left operand", pair_span))?;
    let left = build_concat_expr(left_pair)?;

    // Skip the regex_op token (=~)
    let _op = iter.next();

    // Second operand (the regex pattern)
    let right_pair = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("regex pattern", pair_span))?;
    let right = build_concat_expr(right_pair)?;

    Ok(Expression::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::RegexMatch,
        right: Box::new(right),
    })
}

fn build_comparison_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut iter = pair.into_inner();
    let first = iter
        .next()
        .ok_or_else(|| ParseError::missing_clause("expression", pair_span))?;
    let left = build_concat_expr(first)?;

    // Check if there's a comparison operator
    if let Some(op_pair) = iter.next() {
        let op_span = span_from_pair(&op_pair);
        let op = parse_comp_op(&op_pair)?;
        let right_pair = iter
            .next()
            .ok_or_else(|| ParseError::missing_clause("right operand", op_span))?;
        let right = build_concat_expr(right_pair)?;
        Ok(Expression::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    } else {
        Ok(left)
    }
}

/// Build a concatenation expression from a pest pair.
///
/// Concatenation has lower precedence than arithmetic operators.
/// Parses: `additive ~ (concat_op ~ additive)*`
fn build_concat_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut children: Vec<_> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::missing_clause("expression", pair_span));
    }

    let first = children.remove(0);
    let mut left = build_additive(first)?;

    // Remaining children are: concat_op, additive, concat_op, additive, ...
    let mut iter = children.into_iter();
    while let Some(op_pair) = iter.next() {
        if op_pair.as_rule() == Rule::concat_op {
            if let Some(right_pair) = iter.next() {
                let right = build_additive(right_pair)?;
                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::Concat,
                    right: Box::new(right),
                };
            }
        }
    }

    Ok(left)
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
    let mut postfix_pair = None;

    for inner in pair.clone().into_inner() {
        match inner.as_rule() {
            Rule::neg_op => negated = true,
            Rule::postfix_expr => postfix_pair = Some(inner),
            // Fallback for backward compatibility (in case grammar changes)
            Rule::primary => postfix_pair = Some(inner),
            _ => {}
        }
    }

    let expr = build_postfix_expr(
        postfix_pair.ok_or_else(|| ParseError::missing_clause("primary expression", pair_span))?,
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

/// Build a postfix expression from a pest pair.
///
/// Grammar: `postfix_expr = { primary ~ index_access* }`
///
/// Handles chained index and slice access on expressions:
/// - `list[0]` - single index
/// - `list[1..3]` - slice
/// - `matrix[0][1]` - chained indexing
/// - `p.scores[-1]` - negative index on property
fn build_postfix_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);

    // If this is just a primary (backward compatibility), handle it directly
    if pair.as_rule() == Rule::primary {
        return build_primary(pair);
    }

    let mut inner = pair.into_inner();

    // First element must be a primary expression
    let primary_pair = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("primary expression", pair_span))?;
    let mut expr = build_primary(primary_pair)?;

    // Apply any index/slice accesses
    for access in inner {
        if access.as_rule() == Rule::index_access {
            expr = build_index_access(expr, access)?;
        }
    }

    Ok(expr)
}

/// Build an index or slice access from a pest pair.
///
/// Grammar: `index_access = { "[" ~ (slice_range | expression) ~ "]" }`
///
/// Determines whether this is an index access `[expr]` or slice access `[start..end]`
/// and builds the appropriate AST node.
fn build_index_access(
    list: Expression,
    pair: pest::iterators::Pair<Rule>,
) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::missing_clause("index or slice", pair_span))?;

    match inner.as_rule() {
        Rule::slice_range => {
            // This is a slice: list[start..end]
            let (start, end) = build_slice_range(inner)?;
            Ok(Expression::Slice {
                list: Box::new(list),
                start,
                end,
            })
        }
        Rule::expression => {
            // This is an index: list[expr]
            let index = build_expression(inner)?;
            Ok(Expression::Index {
                list: Box::new(list),
                index: Box::new(index),
            })
        }
        _ => Err(ParseError::unexpected_token(
            span_from_pair(&inner),
            inner.as_str(),
            "slice range or expression",
        )),
    }
}

/// Build a slice range from a pest pair.
///
/// Grammar: `slice_range = { slice_start? ~ ".." ~ slice_end? }`
///
/// Returns (start, end) where each is an optional boxed expression.
#[allow(clippy::type_complexity)]
fn build_slice_range(
    pair: pest::iterators::Pair<Rule>,
) -> Result<(Option<Box<Expression>>, Option<Box<Expression>>), ParseError> {
    let mut start = None;
    let mut end = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::slice_start => {
                start = Some(Box::new(build_slice_bound(inner)?));
            }
            Rule::slice_end => {
                end = Some(Box::new(build_slice_bound(inner)?));
            }
            _ => {}
        }
    }

    Ok((start, end))
}

/// Build a slice bound expression (start or end).
///
/// Grammar: `slice_start = { slice_atom ~ (add_op ~ slice_atom)* }`
///
/// Supports arithmetic expressions like `-1`, `n + 1`, `len(list) - 2`, etc.
fn build_slice_bound(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut children: Vec<_> = pair.into_inner().collect();

    if children.is_empty() {
        return Err(ParseError::missing_clause("slice bound", pair_span));
    }

    // First element is a slice_atom
    let first = children.remove(0);
    let mut left = build_slice_atom(first)?;

    // Process remaining: add_op, slice_atom, add_op, slice_atom, ...
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
                let right = build_slice_atom(right_pair)?;
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

/// Build a slice atom (the basic units in slice bounds).
///
/// Grammar: `slice_atom = { neg_op? ~ (function_call | literal | property_access | variable | "(" ~ expression ~ ")") }`
///
/// Handles negation and the allowed primary expressions in slice bounds.
fn build_slice_atom(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut negated = false;
    let mut expr = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::neg_op => negated = true,
            Rule::function_call => expr = Some(build_function_call(inner)?),
            Rule::literal => expr = Some(Expression::Literal(build_literal(inner)?)),
            Rule::property_access => {
                let span = span_from_pair(&inner);
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
                expr = Some(Expression::Property { variable, property });
            }
            Rule::variable => expr = Some(Expression::Variable(inner.as_str().to_string())),
            Rule::expression => expr = Some(build_expression(inner)?),
            _ => {}
        }
    }

    let expr =
        expr.ok_or_else(|| ParseError::missing_clause("slice atom expression", pair_span))?;

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
        Rule::parameter => {
            // Parameter syntax: $paramName - strip the leading '$'
            let param_str = inner.as_str();
            let name = param_str
                .strip_prefix('$')
                .unwrap_or(param_str)
                .to_string();
            Ok(Expression::Parameter(name))
        }
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
        Rule::list_comprehension => build_list_comprehension(inner),
        Rule::pattern_comprehension => build_pattern_comprehension(inner),
        Rule::map_expr => build_map_expr(inner),
        Rule::reduce_expr => build_reduce_expr(inner),
        Rule::all_predicate => build_list_predicate(inner, ListPredicateKind::All),
        Rule::any_predicate => build_list_predicate(inner, ListPredicateKind::Any),
        Rule::none_predicate => build_list_predicate(inner, ListPredicateKind::None),
        Rule::single_predicate => build_list_predicate(inner, ListPredicateKind::Single),
        _ => Err(ParseError::unexpected_token(
            span,
            inner.as_str(),
            "literal, variable, property access, function call, parameter, CASE, EXISTS expression, list comprehension, pattern comprehension, map literal, REDUCE, or list predicate",
        )),
    }
}

/// Build a CASE expression from a pest pair.
///
/// CASE WHEN condition THEN result [WHEN ... THEN ...] [ELSE default] END
fn build_case_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let mut when_clauses = Vec::new();
    let mut else_clause = None;

    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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
    for inner in pair.clone().into_inner() {
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

    for inner in pair.clone().into_inner() {
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
    for inner in pair.clone().into_inner() {
        if inner.as_rule() == Rule::expression {
            items.push(build_expression(inner)?);
        }
    }
    Ok(items)
}

/// Build a map literal expression from a pest pair.
///
/// Grammar: `{key: value, key2: value2, ...}`
///
/// Keys can be identifiers or string literals.
///
/// # Examples
///
/// ```text
/// {name: 'Alice', age: 30}
/// {personName: p.name, personAge: p.age}
/// {'string-key': value, regularKey: value2}
/// ```
fn build_map_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let mut entries = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::map_entry {
            let (key, value) = build_map_entry(inner)?;
            entries.push((key, value));
        }
    }

    Ok(Expression::Map(entries))
}

/// Build a single map entry (key: value) from a pest pair.
fn build_map_entry(pair: pest::iterators::Pair<Rule>) -> Result<(String, Expression), ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut inner = pair.into_inner();

    // First element: map_key (identifier or string)
    let key_pair = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("map key", pair_span))?;
    let key = build_map_key(key_pair)?;

    // Second element: expression (the value)
    let value_pair = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("map value", pair_span))?;
    let value = build_expression(value_pair)?;

    Ok((key, value))
}

/// Build a map key from a pest pair.
/// Keys can be identifiers or string literals.
fn build_map_key(pair: pest::iterators::Pair<Rule>) -> Result<String, ParseError> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::missing_clause("map key", Span { start: 0, end: 0 }))?;

    match inner.as_rule() {
        Rule::identifier => Ok(inner.as_str().to_string()),
        Rule::string => {
            // String literal - extract inner content without quotes
            let s = inner.as_str();
            // Remove surrounding quotes and handle escaped quotes
            let inner_content = inner
                .into_inner()
                .next()
                .map(|p| p.as_str().replace("''", "'"))
                .unwrap_or_else(|| {
                    // Fallback: strip quotes manually
                    s.trim_matches('\'').replace("''", "'")
                });
            Ok(inner_content)
        }
        _ => Ok(inner.as_str().to_string()),
    }
}

/// Build a REDUCE expression from a pest pair.
///
/// Grammar: `REDUCE(accumulator = reduce_initial, variable IN list_comp_source | expression)`
///
/// # Examples
///
/// ```text
/// REDUCE(total = 0, x IN prices | total + x)     -- sum
/// REDUCE(s = '', name IN names | s || name)      -- string concatenation
/// REDUCE(product = 1, n IN numbers | product * n) -- product
/// ```
fn build_reduce_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut inner = pair.into_inner();

    // Skip the REDUCE keyword
    inner.next();

    // First element: identifier (accumulator variable name)
    let accumulator = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("accumulator variable", pair_span))?
        .as_str()
        .to_string();

    // Second element: reduce_initial (initial value) - limited grammar
    let initial_pair = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("initial value", pair_span))?;
    let initial = Box::new(build_reduce_initial(initial_pair)?);

    // Third element: identifier (loop variable name)
    let variable = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("loop variable", pair_span))?
        .as_str()
        .to_string();

    // Skip the IN keyword
    inner.next();

    // Fourth element: list_comp_source (list to iterate) - uses limited grammar to avoid | ambiguity
    let list_pair = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("list expression", pair_span))?;
    let list = Box::new(build_list_comp_expr(list_pair)?);

    // Skip the pipe_token
    inner.next();

    // Fifth element: expression (accumulator expression)
    let expr_pair = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("reduce expression", pair_span))?;
    let expression = Box::new(build_expression(expr_pair)?);

    Ok(Expression::Reduce {
        accumulator,
        initial,
        variable,
        list,
        expression,
    })
}

/// Build the initial value expression for REDUCE.
/// Uses a limited grammar to avoid consuming the comma delimiter.
fn build_reduce_initial(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::missing_clause("initial value", pair_span))?;

    match inner.as_rule() {
        Rule::function_call => build_function_call(inner),
        Rule::literal => Ok(Expression::Literal(build_literal(inner)?)),
        Rule::property_access => {
            let span = span_from_pair(&inner);
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
        Rule::expression => build_expression(inner),
        _ => Err(ParseError::unexpected_token(
            span_from_pair(&inner),
            inner.as_str(),
            "function call, literal, property access, variable, or parenthesized expression",
        )),
    }
}

/// Kind of list predicate (ALL, ANY, NONE, SINGLE).
enum ListPredicateKind {
    All,
    Any,
    None,
    Single,
}

/// Build a list predicate expression (ALL, ANY, NONE, SINGLE) from a pest pair.
///
/// Grammar: `ALL|ANY|NONE|SINGLE(variable IN list WHERE condition)`
///
/// # Examples
///
/// ```text
/// ALL(score IN s.scores WHERE score >= 60)     -- all elements satisfy
/// ANY(tag IN p.tags WHERE tag = 'vip')         -- at least one satisfies
/// NONE(review IN p.reviews WHERE review < 3)   -- none satisfy
/// SINGLE(p IN players WHERE p.captain = true)  -- exactly one satisfies
/// ```
fn build_list_predicate(
    pair: pest::iterators::Pair<Rule>,
    kind: ListPredicateKind,
) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut inner = pair.into_inner();

    // Skip the keyword (ALL, ANY, NONE, or SINGLE)
    inner.next();

    // First element: identifier (the variable name)
    let variable = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("variable", pair_span))?
        .as_str()
        .to_string();

    // Skip the IN keyword
    inner.next();

    // Third element: list_comp_source (the list to iterate over)
    let list_pair = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("list expression", pair_span))?;
    let list = Box::new(build_list_comp_expr(list_pair)?);

    // Skip the WHERE keyword
    inner.next();

    // Fourth element: expression (the condition)
    let condition_pair = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("condition expression", pair_span))?;
    let condition = Box::new(build_expression(condition_pair)?);

    // Return the appropriate expression variant
    match kind {
        ListPredicateKind::All => Ok(Expression::All {
            variable,
            list,
            condition,
        }),
        ListPredicateKind::Any => Ok(Expression::Any {
            variable,
            list,
            condition,
        }),
        ListPredicateKind::None => Ok(Expression::None {
            variable,
            list,
            condition,
        }),
        ListPredicateKind::Single => Ok(Expression::Single {
            variable,
            list,
            condition,
        }),
    }
}

/// Build a list comprehension expression from a pest pair.
///
/// Grammar: `[variable IN list_comp_source WHERE? list_comp_filter | expression]`
///
/// # Examples
///
/// ```text
/// [x IN people | x.name]               -- basic transformation
/// [x IN people WHERE x.age > 18 | x.name]  -- with filter
/// [n IN numbers | n * 2]               -- numeric transformation
/// ```
fn build_list_comprehension(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut inner = pair.into_inner();

    // First element: identifier (the variable name)
    let variable = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("variable", pair_span))?
        .as_str()
        .to_string();

    // Skip the IN keyword (it appears as an inner rule)
    let _in_keyword = inner.next();

    // Third element: list_comp_source (the list to iterate over)
    let list_pair = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("list expression", pair_span))?;
    let list = Box::new(build_list_comp_expr(list_pair)?);

    // Remaining elements: optional list_comp_where, pipe_token, and transform expression
    let mut filter = None;
    let mut transform = None;

    for item in inner {
        match item.as_rule() {
            Rule::list_comp_where => {
                // Extract the list_comp_filter from WHERE clause (skip WHERE keyword)
                for where_inner in item.into_inner() {
                    if where_inner.as_rule() == Rule::list_comp_filter {
                        filter = Some(Box::new(build_list_comp_expr(where_inner)?));
                        break;
                    }
                }
            }
            Rule::pipe_token => {
                // Skip the pipe token - it's just a delimiter
            }
            Rule::expression => {
                // This is the transform expression (after the |)
                transform = Some(Box::new(build_expression(item)?));
            }
            _ => {}
        }
    }

    let transform =
        transform.ok_or_else(|| ParseError::missing_clause("transform expression", pair_span))?;

    Ok(Expression::ListComprehension {
        variable,
        list,
        filter,
        transform,
    })
}

/// Build a pattern comprehension expression from a pest pair.
///
/// Pattern comprehensions match a pattern and transform each match into a list.
///
/// # Grammar
///
/// ```text
/// pattern_comprehension = { "[" ~ pattern ~ pattern_comp_where? ~ pipe_token ~ expression ~ "]" }
/// pattern_comp_where = { WHERE ~ expression }
/// ```
///
/// # Examples
///
/// ```text
/// [(p)-[:FRIEND]->(f) | f.name]                    -- basic pattern
/// [(p)-[:FRIEND]->(f) WHERE f.age > 21 | f.name]   -- with filter
/// [(p)-[r:KNOWS]->(other) | {name: other.name}]    -- map transform
/// ```
fn build_pattern_comprehension(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut inner = pair.into_inner();

    // First element: the pattern (required)
    let pattern_pair = inner
        .next()
        .ok_or_else(|| ParseError::missing_clause("pattern", pair_span))?;
    let pattern = build_pattern(pattern_pair)?;

    // Remaining elements: optional pattern_comp_where, pipe_token, and transform expression
    let mut filter = None;
    let mut transform = None;

    for item in inner {
        match item.as_rule() {
            Rule::pattern_comp_where => {
                // Extract the expression from WHERE clause
                for where_inner in item.into_inner() {
                    if where_inner.as_rule() == Rule::expression {
                        filter = Some(Box::new(build_expression(where_inner)?));
                        break;
                    }
                }
            }
            Rule::pipe_token => {
                // Skip the pipe token - it's just a delimiter
            }
            Rule::expression => {
                // This is the transform expression (after the |)
                transform = Some(Box::new(build_expression(item)?));
            }
            _ => {}
        }
    }

    let transform =
        transform.ok_or_else(|| ParseError::missing_clause("transform expression", pair_span))?;

    Ok(Expression::PatternComprehension {
        pattern,
        filter,
        transform,
    })
}

/// Build an expression from list comprehension source/filter rules.
///
/// These rules use a simplified grammar to avoid ambiguity with the `|` token.
fn build_list_comp_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut terms: Vec<(Option<BinaryOperator>, Expression)> = Vec::new();

    for item in pair.into_inner() {
        match item.as_rule() {
            Rule::list_comp_term => {
                let term_expr = build_list_comp_term(item)?;
                terms.push((None, term_expr));
            }
            Rule::list_comp_binop => {
                // Store the operator to apply to the next term
                let op = build_list_comp_binop(&item)?;
                if let Some(last) = terms.last_mut() {
                    if last.0.is_none() {
                        // This term doesn't have an operator yet, store it for combining with next
                        last.0 = Some(op);
                    }
                }
            }
            _ => {}
        }
    }

    // Build expression tree from terms (left-to-right for simplicity)
    if terms.is_empty() {
        return Err(ParseError::missing_clause("expression", pair_span));
    }

    let mut result = terms.remove(0).1;
    for (maybe_op, term) in terms {
        if let Some(op) = maybe_op {
            result = Expression::BinaryOp {
                left: Box::new(result),
                op,
                right: Box::new(term),
            };
        } else {
            // No operator - shouldn't happen in well-formed input
            result = term;
        }
    }

    Ok(result)
}

/// Build a term from the list comprehension grammar.
fn build_list_comp_term(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut is_not = false;
    let mut primary_expr = None;
    let mut is_null = false;
    let is_not_null = false;

    for item in pair.into_inner() {
        match item.as_rule() {
            Rule::NOT => {
                is_not = true;
            }
            Rule::list_comp_primary => {
                primary_expr = Some(build_list_comp_primary(item)?);
            }
            Rule::IS => {
                // Next will be NOT? NULL
            }
            Rule::NULL => {
                is_null = true;
            }
            _ => {}
        }
    }

    let mut expr =
        primary_expr.ok_or_else(|| ParseError::missing_clause("primary expression", pair_span))?;

    // Handle IS NULL / IS NOT NULL
    if is_null {
        expr = Expression::IsNull {
            expr: Box::new(expr),
            negated: is_not_null,
        };
        is_not = false; // IS NOT NULL is handled separately
    }

    // Handle NOT prefix
    if is_not {
        expr = Expression::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr),
        };
    }

    Ok(expr)
}

/// Build a primary expression from the list comprehension grammar.
fn build_list_comp_primary(pair: pest::iterators::Pair<Rule>) -> Result<Expression, ParseError> {
    let pair_span = span_from_pair(&pair);
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::missing_clause("primary expression", pair_span))?;

    match inner.as_rule() {
        Rule::function_call => build_function_call(inner),
        Rule::list_expr => Ok(Expression::List(build_list_expr(inner)?)),
        Rule::list_comprehension => build_list_comprehension(inner),
        Rule::pattern_comprehension => build_pattern_comprehension(inner),
        Rule::literal => Ok(Expression::Literal(build_literal(inner)?)),
        Rule::property_access => {
            let span = span_from_pair(&inner);
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
        Rule::expression => build_expression(inner),
        _ => Err(ParseError::unexpected_token(
            span_from_pair(&inner),
            inner.as_str(),
            "function call, list literal, list comprehension, pattern comprehension, literal, property access, variable, or parenthesized expression",
        )),
    }
}

/// Build a binary operator from the list comprehension grammar.
fn build_list_comp_binop(pair: &pest::iterators::Pair<Rule>) -> Result<BinaryOperator, ParseError> {
    let inner = pair.clone().into_inner().next();

    if let Some(op_pair) = inner {
        match op_pair.as_rule() {
            Rule::AND => Ok(BinaryOperator::And),
            Rule::OR => Ok(BinaryOperator::Or),
            Rule::comp_op => {
                let comp_inner = op_pair.into_inner().next();
                if let Some(comp) = comp_inner {
                    match comp.as_rule() {
                        Rule::eq => Ok(BinaryOperator::Eq),
                        Rule::neq => Ok(BinaryOperator::Neq),
                        Rule::lt => Ok(BinaryOperator::Lt),
                        Rule::lte => Ok(BinaryOperator::Lte),
                        Rule::gt => Ok(BinaryOperator::Gt),
                        Rule::gte => Ok(BinaryOperator::Gte),
                        Rule::CONTAINS => Ok(BinaryOperator::Contains),
                        Rule::starts_with => Ok(BinaryOperator::StartsWith),
                        Rule::ends_with => Ok(BinaryOperator::EndsWith),
                        _ => Ok(BinaryOperator::Eq),
                    }
                } else {
                    Ok(BinaryOperator::Eq)
                }
            }
            Rule::add_op => {
                if op_pair.as_str() == "+" {
                    Ok(BinaryOperator::Add)
                } else {
                    Ok(BinaryOperator::Sub)
                }
            }
            Rule::mul_op => match op_pair.as_str() {
                "*" => Ok(BinaryOperator::Mul),
                "/" => Ok(BinaryOperator::Div),
                "%" => Ok(BinaryOperator::Mod),
                _ => Ok(BinaryOperator::Mul),
            },
            Rule::pow_op => Ok(BinaryOperator::Pow),
            _ => Ok(BinaryOperator::Eq),
        }
    } else {
        // Direct match on the pair's string representation
        match pair.as_str().to_uppercase().as_str() {
            "AND" => Ok(BinaryOperator::And),
            "OR" => Ok(BinaryOperator::Or),
            "+" => Ok(BinaryOperator::Add),
            "-" => Ok(BinaryOperator::Sub),
            "*" => Ok(BinaryOperator::Mul),
            "/" => Ok(BinaryOperator::Div),
            "%" => Ok(BinaryOperator::Mod),
            "^" => Ok(BinaryOperator::Pow),
            "=" => Ok(BinaryOperator::Eq),
            "<>" | "!=" => Ok(BinaryOperator::Neq),
            "<" => Ok(BinaryOperator::Lt),
            "<=" => Ok(BinaryOperator::Lte),
            ">" => Ok(BinaryOperator::Gt),
            ">=" => Ok(BinaryOperator::Gte),
            _ => Ok(BinaryOperator::Eq),
        }
    }
}

// =============================================================================
// DDL Statement Parsing
// =============================================================================

/// Build a DDL statement from a pest pair.
fn build_ddl_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::Empty)?;

    let ddl = match inner.as_rule() {
        Rule::create_node_type => DdlStatement::CreateNodeType(build_create_node_type(inner)?),
        Rule::create_edge_type => DdlStatement::CreateEdgeType(build_create_edge_type(inner)?),
        Rule::alter_node_type => DdlStatement::AlterNodeType(build_alter_node_type(inner)?),
        Rule::alter_edge_type => DdlStatement::AlterEdgeType(build_alter_edge_type(inner)?),
        Rule::drop_node_type => DdlStatement::DropNodeType(build_drop_type(inner)?),
        Rule::drop_edge_type => DdlStatement::DropEdgeType(build_drop_type(inner)?),
        Rule::set_schema_validation => DdlStatement::SetValidation(build_set_validation(inner)?),
        _ => {
            return Err(ParseError::Syntax(format!(
                "Unexpected DDL statement type: {:?}",
                inner.as_rule()
            )))
        }
    };

    Ok(Statement::Ddl(Box::new(ddl)))
}

/// Build a CREATE NODE TYPE statement.
///
/// Grammar: `CREATE NODE TYPE identifier ( property_def_list? )`
fn build_create_node_type(pair: pest::iterators::Pair<Rule>) -> Result<CreateNodeType, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut name = None;
    let mut properties = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => {
                name = Some(inner.as_str().to_string());
            }
            Rule::property_def_list => {
                properties = build_property_def_list(inner)?;
            }
            Rule::CREATE | Rule::NODE | Rule::TYPE => {}
            _ => {}
        }
    }

    let name = name.ok_or_else(|| ParseError::missing_clause("type name", pair_span))?;

    Ok(CreateNodeType { name, properties })
}

/// Build a CREATE EDGE TYPE statement.
///
/// Grammar: `CREATE EDGE TYPE identifier ( property_def_list? ) FROM type_name_list TO type_name_list`
fn build_create_edge_type(pair: pest::iterators::Pair<Rule>) -> Result<CreateEdgeType, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut name = None;
    let mut properties = Vec::new();
    let mut from_types = Vec::new();
    let mut to_types = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => {
                if name.is_none() {
                    name = Some(inner.as_str().to_string());
                }
            }
            Rule::property_def_list => {
                properties = build_property_def_list(inner)?;
            }
            Rule::edge_endpoint_clause => {
                let (from, to) = build_edge_endpoint_clause(inner)?;
                from_types = from;
                to_types = to;
            }
            Rule::CREATE | Rule::EDGE | Rule::TYPE => {}
            _ => {}
        }
    }

    let name = name.ok_or_else(|| ParseError::missing_clause("type name", pair_span))?;

    Ok(CreateEdgeType {
        name,
        properties,
        from_types,
        to_types,
    })
}

/// Build edge endpoint clause (FROM ... TO ...).
fn build_edge_endpoint_clause(
    pair: pest::iterators::Pair<Rule>,
) -> Result<(Vec<String>, Vec<String>), ParseError> {
    let mut from_types = Vec::new();
    let mut to_types = Vec::new();
    let mut is_from = true;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::type_name_list => {
                let types = build_type_name_list(inner)?;
                if is_from {
                    from_types = types;
                    is_from = false;
                } else {
                    to_types = types;
                }
            }
            Rule::FROM_KW | Rule::TO_KW => {}
            _ => {}
        }
    }

    Ok((from_types, to_types))
}

/// Build a comma-separated list of type names.
fn build_type_name_list(pair: pest::iterators::Pair<Rule>) -> Result<Vec<String>, ParseError> {
    let mut types = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::identifier {
            types.push(inner.as_str().to_string());
        }
    }
    Ok(types)
}

/// Build an ALTER NODE TYPE statement.
fn build_alter_node_type(pair: pest::iterators::Pair<Rule>) -> Result<AlterNodeType, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut name = None;
    let mut action = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => {
                name = Some(inner.as_str().to_string());
            }
            Rule::alter_type_action => {
                action = Some(build_alter_type_action(inner)?);
            }
            Rule::ALTER | Rule::NODE | Rule::TYPE => {}
            _ => {}
        }
    }

    let name = name.ok_or_else(|| ParseError::missing_clause("type name", pair_span))?;
    let action = action.ok_or_else(|| ParseError::missing_clause("alter action", pair_span))?;

    Ok(AlterNodeType { name, action })
}

/// Build an ALTER EDGE TYPE statement.
fn build_alter_edge_type(pair: pest::iterators::Pair<Rule>) -> Result<AlterEdgeType, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut name = None;
    let mut action = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => {
                name = Some(inner.as_str().to_string());
            }
            Rule::alter_type_action => {
                action = Some(build_alter_type_action(inner)?);
            }
            Rule::ALTER | Rule::EDGE | Rule::TYPE => {}
            _ => {}
        }
    }

    let name = name.ok_or_else(|| ParseError::missing_clause("type name", pair_span))?;
    let action = action.ok_or_else(|| ParseError::missing_clause("alter action", pair_span))?;

    Ok(AlterEdgeType { name, action })
}

/// Build an alter type action.
fn build_alter_type_action(
    pair: pest::iterators::Pair<Rule>,
) -> Result<AlterTypeAction, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::Empty)?;

    match inner.as_rule() {
        Rule::allow_additional_properties => Ok(AlterTypeAction::AllowAdditionalProperties),
        Rule::add_property_action => {
            let prop_def = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::property_def)
                .ok_or(ParseError::Empty)?;
            Ok(AlterTypeAction::AddProperty(build_property_def(prop_def)?))
        }
        Rule::drop_property_action => {
            let prop_name = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::identifier)
                .ok_or(ParseError::Empty)?;
            Ok(AlterTypeAction::DropProperty(
                prop_name.as_str().to_string(),
            ))
        }
        _ => Err(ParseError::Syntax(format!(
            "Unexpected alter type action: {:?}",
            inner.as_rule()
        ))),
    }
}

/// Build a DROP TYPE statement (for both node and edge types).
fn build_drop_type(pair: pest::iterators::Pair<Rule>) -> Result<DropType, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut name = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::identifier {
            name = Some(inner.as_str().to_string());
        }
    }

    let name = name.ok_or_else(|| ParseError::missing_clause("type name", pair_span))?;

    Ok(DropType { name })
}

/// Build a SET SCHEMA VALIDATION statement.
fn build_set_validation(pair: pest::iterators::Pair<Rule>) -> Result<SetValidation, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut mode = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::validation_mode {
            mode = Some(build_validation_mode(inner)?);
        }
    }

    let mode = mode.ok_or_else(|| ParseError::missing_clause("validation mode", pair_span))?;

    Ok(SetValidation { mode })
}

/// Build a validation mode.
fn build_validation_mode(
    pair: pest::iterators::Pair<Rule>,
) -> Result<ValidationModeAst, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::Empty)?;

    match inner.as_rule() {
        Rule::NONE_KW => Ok(ValidationModeAst::None),
        Rule::WARN_KW => Ok(ValidationModeAst::Warn),
        Rule::STRICT => Ok(ValidationModeAst::Strict),
        Rule::CLOSED => Ok(ValidationModeAst::Closed),
        _ => Err(ParseError::Syntax(format!(
            "Unexpected validation mode: {:?}",
            inner.as_rule()
        ))),
    }
}

/// Build a list of property definitions.
fn build_property_def_list(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Vec<PropertyDefinition>, ParseError> {
    let mut properties = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::property_def {
            properties.push(build_property_def(inner)?);
        }
    }
    Ok(properties)
}

/// Build a single property definition.
///
/// Grammar: `identifier property_type not_null_modifier? default_modifier?`
fn build_property_def(pair: pest::iterators::Pair<Rule>) -> Result<PropertyDefinition, ParseError> {
    let pair_span = span_from_pair(&pair);
    let mut name = None;
    let mut prop_type = None;
    let mut required = false;
    let mut default = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => {
                name = Some(inner.as_str().to_string());
            }
            Rule::property_type => {
                prop_type = Some(build_property_type(inner)?);
            }
            Rule::not_null_modifier => {
                required = true;
            }
            Rule::default_modifier => {
                default = build_default_modifier(inner)?;
            }
            _ => {}
        }
    }

    let name = name.ok_or_else(|| ParseError::missing_clause("property name", pair_span))?;
    let prop_type =
        prop_type.ok_or_else(|| ParseError::missing_clause("property type", pair_span))?;

    Ok(PropertyDefinition {
        name,
        prop_type,
        required,
        default,
    })
}

/// Build a property type.
fn build_property_type(pair: pest::iterators::Pair<Rule>) -> Result<PropertyTypeAst, ParseError> {
    let inner = pair.into_inner().next().ok_or(ParseError::Empty)?;

    match inner.as_rule() {
        Rule::STRING_TYPE => Ok(PropertyTypeAst::String),
        Rule::INT_TYPE => Ok(PropertyTypeAst::Int),
        Rule::FLOAT_TYPE => Ok(PropertyTypeAst::Float),
        Rule::BOOL_TYPE => Ok(PropertyTypeAst::Bool),
        Rule::ANY_TYPE => Ok(PropertyTypeAst::Any),
        Rule::list_type => build_list_type(inner),
        Rule::map_type => build_map_type(inner),
        _ => Err(ParseError::Syntax(format!(
            "Unexpected property type: {:?}",
            inner.as_rule()
        ))),
    }
}

/// Build a LIST type with optional element type.
fn build_list_type(pair: pest::iterators::Pair<Rule>) -> Result<PropertyTypeAst, ParseError> {
    let mut element_type = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::property_type {
            element_type = Some(Box::new(build_property_type(inner)?));
        }
    }

    Ok(PropertyTypeAst::List(element_type))
}

/// Build a MAP type with optional value type.
fn build_map_type(pair: pest::iterators::Pair<Rule>) -> Result<PropertyTypeAst, ParseError> {
    let mut value_type = None;

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::property_type {
            value_type = Some(Box::new(build_property_type(inner)?));
        }
    }

    Ok(PropertyTypeAst::Map(value_type))
}

/// Build a default modifier value.
fn build_default_modifier(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Option<Literal>, ParseError> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::literal {
            return Ok(Some(build_literal(inner)?));
        }
    }
    Ok(None)
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

    // =========================================================================
    // Inline WHERE in Patterns Tests
    // =========================================================================

    #[test]
    fn test_parse_node_inline_where() {
        // Basic inline WHERE on a node
        let query = parse("MATCH (n:Person WHERE n.age > 21) RETURN n").unwrap();

        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert!(node.where_clause.is_some());
            let where_expr = node.where_clause.as_ref().unwrap();
            // Verify it's a comparison expression
            if let Expression::BinaryOp { op, .. } = where_expr {
                assert!(matches!(op, BinaryOperator::Gt));
            } else {
                panic!("Expected binary op expression");
            }
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn test_parse_node_inline_where_with_label_and_props() {
        // Inline WHERE combined with label and property filter
        let query =
            parse("MATCH (n:Person {status: 'active'} WHERE n.age >= 18) RETURN n").unwrap();

        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert_eq!(node.labels, vec!["Person"]);
            assert!(!node.properties.is_empty());
            assert!(node.where_clause.is_some());
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn test_parse_edge_inline_where() {
        // Inline WHERE on an edge
        let query = parse("MATCH (a)-[r:KNOWS WHERE r.since > 2020]->(b) RETURN a, b").unwrap();

        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert!(edge.where_clause.is_some());
            let where_expr = edge.where_clause.as_ref().unwrap();
            if let Expression::BinaryOp { op, .. } = where_expr {
                assert!(matches!(op, BinaryOperator::Gt));
            } else {
                panic!("Expected binary op expression");
            }
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_edge_inline_where_with_props() {
        // Inline WHERE combined with edge property filter
        let query =
            parse("MATCH (a)-[r:FOLLOWS {active: true} WHERE r.weight > 0.5]->(b) RETURN a, b")
                .unwrap();

        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert_eq!(edge.labels, vec!["FOLLOWS"]);
            assert!(!edge.properties.is_empty());
            assert!(edge.where_clause.is_some());
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_combined_inline_where() {
        // Both node and edge with inline WHERE
        let query = parse(
            "MATCH (a:Person WHERE a.active = true)-[r:FOLLOWS WHERE r.weight > 0.5]->(b) RETURN a, b",
        )
        .unwrap();

        let pattern = &query.match_clause.patterns[0];

        // Check node
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert!(node.where_clause.is_some());
        } else {
            panic!("Expected node pattern");
        }

        // Check edge
        if let PatternElement::Edge(edge) = &pattern.elements[1] {
            assert!(edge.where_clause.is_some());
        } else {
            panic!("Expected edge pattern");
        }
    }

    #[test]
    fn test_parse_inline_where_complex_expression() {
        // Inline WHERE with AND expression
        let query =
            parse("MATCH (n:Person WHERE n.age > 18 AND n.status = 'active') RETURN n").unwrap();

        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert!(node.where_clause.is_some());
            let where_expr = node.where_clause.as_ref().unwrap();
            if let Expression::BinaryOp { op, .. } = where_expr {
                assert!(matches!(op, BinaryOperator::And));
            } else {
                panic!("Expected AND expression");
            }
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn test_parse_inline_where_is_null() {
        // Inline WHERE with IS NULL check
        let query = parse("MATCH (n:Person WHERE n.email IS NOT NULL) RETURN n").unwrap();

        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert!(node.where_clause.is_some());
            let where_expr = node.where_clause.as_ref().unwrap();
            assert!(matches!(
                where_expr,
                Expression::IsNull { negated: true, .. }
            ));
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn test_parse_inline_where_in_list() {
        // Inline WHERE with IN list
        let query =
            parse("MATCH (n:Person WHERE n.status IN ['active', 'pending']) RETURN n").unwrap();

        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert!(node.where_clause.is_some());
            let where_expr = node.where_clause.as_ref().unwrap();
            assert!(matches!(
                where_expr,
                Expression::InList { negated: false, .. }
            ));
        } else {
            panic!("Expected node pattern");
        }
    }

    #[test]
    fn test_parse_inline_where_with_global_where() {
        // Inline WHERE combined with global WHERE clause
        let query = parse(
            "MATCH (a:Person WHERE a.active = true)-[r:KNOWS]->(b) WHERE b.age > 30 RETURN a, b",
        )
        .unwrap();

        // Check inline WHERE on node
        let pattern = &query.match_clause.patterns[0];
        if let PatternElement::Node(node) = &pattern.elements[0] {
            assert!(node.where_clause.is_some());
        } else {
            panic!("Expected node pattern");
        }

        // Check global WHERE exists
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn test_list_comprehension_pest_parse() {
        // Test pest parsing directly
        let input = "[x IN items | x * 2]";

        let result = GqlParser::parse(Rule::list_comprehension, input);
        assert!(result.is_ok(), "Failed to parse list comprehension");

        let pairs = result.unwrap();
        let pair = pairs.into_iter().next().unwrap();
        assert_eq!(pair.as_rule(), Rule::list_comprehension);
        assert_eq!(pair.as_str(), "[x IN items | x * 2]");
    }

    #[test]
    fn test_list_comprehension_with_filter_pest_parse() {
        let input = "[a IN ages WHERE a >= 30 | a]";

        let result = GqlParser::parse(Rule::list_comprehension, input);
        assert!(
            result.is_ok(),
            "Failed to parse list comprehension with filter"
        );

        let pairs = result.unwrap();
        let pair = pairs.into_iter().next().unwrap();
        assert_eq!(pair.as_rule(), Rule::list_comprehension);
    }

    #[test]
    fn test_list_comprehension_full_parse() {
        // Test full query parsing with list comprehension
        let input = "MATCH (n:Person) LET doubled = [x IN items | x * 2] RETURN doubled";

        let result = parse(input);
        assert!(
            result.is_ok(),
            "Failed to parse query with list comprehension"
        );

        let query = result.unwrap();
        assert_eq!(query.let_clauses.len(), 1);
        assert_eq!(query.let_clauses[0].variable, "doubled");
    }

    #[test]
    fn test_reduce_expr_pest_parse() {
        // Test basic REDUCE expression parsing
        let input = "REDUCE(total = 0, x IN items | total + x)";

        let result = GqlParser::parse(Rule::reduce_expr, input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse REDUCE expression");

        let pairs = result.unwrap();
        let pair = pairs.into_iter().next().unwrap();
        assert_eq!(pair.as_rule(), Rule::reduce_expr);

        // Debug: print inner pairs
        for inner in pair.into_inner() {
            eprintln!("Inner: {:?} = {:?}", inner.as_rule(), inner.as_str());
        }
    }

    #[test]
    fn test_reduce_full_parse() {
        // Test full query parsing with REDUCE
        let input = "MATCH (n:Person) RETURN REDUCE(total = 0, x IN [1,2,3] | total + x) AS sum";

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse query with REDUCE");
    }

    // ============================================
    // CALL Subquery Parser Tests
    // ============================================

    #[test]
    fn test_parse_call_basic_uncorrelated() {
        // Basic uncorrelated CALL (no importing WITH)
        let input = r#"
            MATCH (p:Person)
            CALL {
                MATCH (t:Team)
                RETURN count(t) AS teamCount
            }
            RETURN p.name, teamCount
        "#;

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse uncorrelated CALL subquery");

        let query = result.unwrap();
        assert_eq!(query.call_clauses.len(), 1);

        let call = &query.call_clauses[0];
        if let CallBody::Single(call_query) = &call.body {
            assert!(call_query.importing_with.is_none());
            assert!(call_query.match_clause.is_some());
        } else {
            panic!("Expected Single CallBody");
        }
    }

    #[test]
    fn test_parse_call_correlated_with_importing_with() {
        // Correlated CALL with importing WITH
        let input = r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[:FRIEND]->(f)
                RETURN count(f) AS friendCount
            }
            RETURN p.name, friendCount
        "#;

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse correlated CALL subquery");

        let query = result.unwrap();
        assert_eq!(query.call_clauses.len(), 1);

        let call = &query.call_clauses[0];
        if let CallBody::Single(call_query) = &call.body {
            assert!(call_query.importing_with.is_some());
            let importing = call_query.importing_with.as_ref().unwrap();
            assert_eq!(importing.items.len(), 1);
            if let Expression::Variable(v) = &importing.items[0].expression {
                assert_eq!(v, "p");
            } else {
                panic!("Expected variable expression in importing WITH");
            }
        } else {
            panic!("Expected Single CallBody");
        }
    }

    #[test]
    fn test_parse_call_multiple_imported_variables() {
        // CALL with multiple variables imported
        let input = r#"
            MATCH (p:Person)-[:WORKS_AT]->(c:Company)
            CALL {
                WITH p, c
                RETURN p.salary + c.bonus AS totalComp
            }
            RETURN p.name, totalComp
        "#;

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse CALL with multiple imports");

        let query = result.unwrap();
        let call = &query.call_clauses[0];
        if let CallBody::Single(call_query) = &call.body {
            let importing = call_query.importing_with.as_ref().unwrap();
            assert_eq!(importing.items.len(), 2);
        } else {
            panic!("Expected Single CallBody");
        }
    }

    #[test]
    fn test_parse_call_union() {
        // CALL with UNION inside
        let input = r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[:FRIEND]->(f)
                RETURN f.name AS name
                UNION
                WITH p
                MATCH (p)-[:COLLEAGUE]->(c)
                RETURN c.name AS name
            }
            RETURN p.name, name
        "#;

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse CALL with UNION");

        let query = result.unwrap();
        let call = &query.call_clauses[0];
        if let CallBody::Union { queries, all } = &call.body {
            assert_eq!(queries.len(), 2);
            assert!(!all); // UNION (not UNION ALL)
        } else {
            panic!("Expected Union CallBody");
        }
    }

    #[test]
    fn test_parse_call_union_all() {
        // CALL with UNION ALL inside
        let input = r#"
            MATCH (p:Person)
            CALL {
                MATCH (t:Team {sport: 'Basketball'})
                RETURN t.name AS teamName
                UNION ALL
                MATCH (t:Team {sport: 'Football'})
                RETURN t.name AS teamName
            }
            RETURN p.name, teamName
        "#;

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse CALL with UNION ALL");

        let query = result.unwrap();
        let call = &query.call_clauses[0];
        if let CallBody::Union { queries, all } = &call.body {
            assert_eq!(queries.len(), 2);
            assert!(all); // UNION ALL
        } else {
            panic!("Expected Union CallBody");
        }
    }

    #[test]
    fn test_parse_call_without_match() {
        // CALL without MATCH (just transforms imported variables)
        let input = r#"
            MATCH (p:Person)
            CALL {
                WITH p
                RETURN p.firstName || ' ' || p.lastName AS fullName
            }
            RETURN fullName
        "#;

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse CALL without MATCH");

        let query = result.unwrap();
        let call = &query.call_clauses[0];
        if let CallBody::Single(call_query) = &call.body {
            assert!(call_query.match_clause.is_none());
            assert!(call_query.importing_with.is_some());
        } else {
            panic!("Expected Single CallBody");
        }
    }

    #[test]
    fn test_parse_call_with_order_limit() {
        // CALL with ORDER BY and LIMIT in subquery
        let input = r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[:BOUGHT]->(item)
                RETURN item.name AS itemName
                ORDER BY item.price DESC
                LIMIT 5
            }
            RETURN p.name, itemName
        "#;

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(
            result.is_ok(),
            "Failed to parse CALL with ORDER BY and LIMIT"
        );

        let query = result.unwrap();
        let call = &query.call_clauses[0];
        if let CallBody::Single(call_query) = &call.body {
            assert!(call_query.order_clause.is_some());
            assert!(call_query.limit_clause.is_some());
            assert_eq!(call_query.limit_clause.as_ref().unwrap().limit, 5);
        } else {
            panic!("Expected Single CallBody");
        }
    }

    #[test]
    fn test_parse_call_with_where() {
        // CALL with WHERE clause
        let input = r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[:FRIEND]->(f)
                WHERE f.age > 21
                RETURN f.name AS friendName
            }
            RETURN p.name, friendName
        "#;

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse CALL with WHERE");

        let query = result.unwrap();
        let call = &query.call_clauses[0];
        if let CallBody::Single(call_query) = &call.body {
            assert!(call_query.where_clause.is_some());
        } else {
            panic!("Expected Single CallBody");
        }
    }

    #[test]
    fn test_parse_multiple_call_clauses() {
        // Multiple CALL clauses in a query
        let input = r#"
            MATCH (p:Person)
            CALL {
                WITH p
                MATCH (p)-[:FRIEND]->(f)
                RETURN count(f) AS friendCount
            }
            CALL {
                WITH p
                MATCH (p)-[:BOUGHT]->(i)
                RETURN count(i) AS itemCount
            }
            RETURN p.name, friendCount, itemCount
        "#;

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse multiple CALL clauses");

        let query = result.unwrap();
        assert_eq!(query.call_clauses.len(), 2);
    }

    #[test]
    fn test_parse_call_is_correlated() {
        // Test is_correlated() method on CallClause
        let correlated_input = r#"
            MATCH (p:Person)
            CALL {
                WITH p
                RETURN p.name AS name
            }
            RETURN name
        "#;

        let uncorrelated_input = r#"
            MATCH (p:Person)
            CALL {
                MATCH (t:Team)
                RETURN count(t) AS teamCount
            }
            RETURN p.name, teamCount
        "#;

        let correlated_query = parse(correlated_input).unwrap();
        let uncorrelated_query = parse(uncorrelated_input).unwrap();

        assert!(
            correlated_query.call_clauses[0].is_correlated(),
            "CALL with importing WITH should be correlated"
        );
        assert!(
            !uncorrelated_query.call_clauses[0].is_correlated(),
            "CALL without importing WITH should be uncorrelated"
        );
    }

    #[test]
    fn test_parse_call_case_insensitive() {
        // CALL keyword is case insensitive
        let inputs = [
            "MATCH (p:Person) CALL { MATCH (t:Team) RETURN t } RETURN p",
            "MATCH (p:Person) call { MATCH (t:Team) RETURN t } RETURN p",
            "MATCH (p:Person) Call { MATCH (t:Team) RETURN t } RETURN p",
        ];

        for input in &inputs {
            let result = parse(input);
            if let Err(e) = &result {
                eprintln!("Parse error for '{}': {:?}", input, e);
            }
            assert!(result.is_ok(), "Failed to parse: {}", input);
        }
    }

    #[test]
    fn test_parse_call_importing_with_alias() {
        // Importing WITH with alias (renaming variables)
        let input = r#"
            MATCH (p:Person)
            CALL {
                WITH p AS person
                RETURN person.name AS name
            }
            RETURN p.name, name
        "#;

        let result = parse(input);
        if let Err(e) = &result {
            eprintln!("Parse error: {:?}", e);
        }
        assert!(result.is_ok(), "Failed to parse CALL with aliased import");

        let query = result.unwrap();
        let call = &query.call_clauses[0];
        if let CallBody::Single(call_query) = &call.body {
            let importing = call_query.importing_with.as_ref().unwrap();
            assert_eq!(importing.items[0].alias, Some("person".to_string()));
        } else {
            panic!("Expected Single CallBody");
        }
    }

    // ============================================
    // List Index and Slice Tests
    // ============================================

    #[test]
    fn test_parse_index_access_literal() {
        // Index access on a list literal
        let query = parse("MATCH (n) RETURN [1, 2, 3][0]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Index { list, index } = &query.return_clause.items[0].expression {
            // List should be a list literal [1, 2, 3]
            if let Expression::List(items) = list.as_ref() {
                assert_eq!(items.len(), 3);
            } else {
                panic!("Expected list literal");
            }
            // Index should be integer literal 0
            if let Expression::Literal(Literal::Int(i)) = index.as_ref() {
                assert_eq!(*i, 0);
            } else {
                panic!("Expected integer literal index");
            }
        } else {
            panic!("Expected Index expression");
        }
    }

    #[test]
    fn test_parse_index_access_property() {
        // Index access on a property
        let query = parse("MATCH (p:Person) RETURN p.scores[0]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Index { list, index } = &query.return_clause.items[0].expression {
            // List should be property access p.scores
            if let Expression::Property { variable, property } = list.as_ref() {
                assert_eq!(variable, "p");
                assert_eq!(property, "scores");
            } else {
                panic!("Expected property access for list");
            }
            // Index should be integer literal 0
            if let Expression::Literal(Literal::Int(i)) = index.as_ref() {
                assert_eq!(*i, 0);
            } else {
                panic!("Expected integer literal index");
            }
        } else {
            panic!("Expected Index expression");
        }
    }

    #[test]
    fn test_parse_index_access_negative() {
        // Negative index access
        let query = parse("MATCH (n) RETURN [1, 2, 3][-1]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Index { index, .. } = &query.return_clause.items[0].expression {
            // Index should be unary negation of 1
            if let Expression::UnaryOp { op, expr } = index.as_ref() {
                assert!(matches!(op, UnaryOperator::Neg));
                if let Expression::Literal(Literal::Int(i)) = expr.as_ref() {
                    assert_eq!(*i, 1);
                } else {
                    panic!("Expected integer literal in negation");
                }
            } else {
                panic!("Expected unary negation for negative index");
            }
        } else {
            panic!("Expected Index expression");
        }
    }

    #[test]
    fn test_parse_slice_full_range() {
        // Full slice with start and end
        let query = parse("MATCH (n) RETURN [1, 2, 3, 4][1..3]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Slice { list, start, end } = &query.return_clause.items[0].expression {
            // List should be [1, 2, 3, 4]
            if let Expression::List(items) = list.as_ref() {
                assert_eq!(items.len(), 4);
            } else {
                panic!("Expected list literal");
            }
            // Start should be 1
            assert!(start.is_some());
            if let Expression::Literal(Literal::Int(s)) = start.as_ref().unwrap().as_ref() {
                assert_eq!(*s, 1);
            } else {
                panic!("Expected integer literal for start");
            }
            // End should be 3
            assert!(end.is_some());
            if let Expression::Literal(Literal::Int(e)) = end.as_ref().unwrap().as_ref() {
                assert_eq!(*e, 3);
            } else {
                panic!("Expected integer literal for end");
            }
        } else {
            panic!("Expected Slice expression");
        }
    }

    #[test]
    fn test_parse_slice_open_start() {
        // Slice with only end: [..3]
        let query = parse("MATCH (n) RETURN [1, 2, 3, 4][..3]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Slice { start, end, .. } = &query.return_clause.items[0].expression {
            assert!(start.is_none(), "Start should be None");
            assert!(end.is_some(), "End should be Some");
            if let Expression::Literal(Literal::Int(e)) = end.as_ref().unwrap().as_ref() {
                assert_eq!(*e, 3);
            }
        } else {
            panic!("Expected Slice expression");
        }
    }

    #[test]
    fn test_parse_slice_open_end() {
        // Slice with only start: [2..]
        let query = parse("MATCH (n) RETURN [1, 2, 3, 4][2..]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Slice { start, end, .. } = &query.return_clause.items[0].expression {
            assert!(start.is_some(), "Start should be Some");
            assert!(end.is_none(), "End should be None");
            if let Expression::Literal(Literal::Int(s)) = start.as_ref().unwrap().as_ref() {
                assert_eq!(*s, 2);
            }
        } else {
            panic!("Expected Slice expression");
        }
    }

    #[test]
    fn test_parse_slice_fully_open() {
        // Full copy slice: [..]
        let query = parse("MATCH (n) RETURN [1, 2, 3][..]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Slice { start, end, .. } = &query.return_clause.items[0].expression {
            assert!(start.is_none(), "Start should be None");
            assert!(end.is_none(), "End should be None");
        } else {
            panic!("Expected Slice expression");
        }
    }

    #[test]
    fn test_parse_slice_negative_indices() {
        // Slice with negative start: [-3..]
        let query = parse("MATCH (n) RETURN [1, 2, 3, 4, 5][-3..]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Slice { start, end, .. } = &query.return_clause.items[0].expression {
            assert!(start.is_some(), "Start should be Some");
            assert!(end.is_none(), "End should be None");
            // Start should be -3 (unary negation of 3)
            if let Expression::UnaryOp { op, expr } = start.as_ref().unwrap().as_ref() {
                assert!(matches!(op, UnaryOperator::Neg));
                if let Expression::Literal(Literal::Int(i)) = expr.as_ref() {
                    assert_eq!(*i, 3);
                }
            } else {
                panic!("Expected unary negation for negative start");
            }
        } else {
            panic!("Expected Slice expression");
        }
    }

    #[test]
    fn test_parse_slice_negative_end() {
        // Slice with negative end: [..-1]
        let query = parse("MATCH (n) RETURN [1, 2, 3, 4][..-1]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Slice { start, end, .. } = &query.return_clause.items[0].expression {
            assert!(start.is_none(), "Start should be None");
            assert!(end.is_some(), "End should be Some");
            // End should be -1 (unary negation of 1)
            if let Expression::UnaryOp { op, expr } = end.as_ref().unwrap().as_ref() {
                assert!(matches!(op, UnaryOperator::Neg));
                if let Expression::Literal(Literal::Int(i)) = expr.as_ref() {
                    assert_eq!(*i, 1);
                }
            } else {
                panic!("Expected unary negation for negative end");
            }
        } else {
            panic!("Expected Slice expression");
        }
    }

    #[test]
    fn test_parse_chained_index() {
        // Chained index access: matrix[0][1]
        let query = parse("MATCH (n) RETURN [[1, 2], [3, 4]][0][1]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        // Should be Index { list: Index { list: [[1,2],[3,4]], index: 0 }, index: 1 }
        if let Expression::Index { list, index } = &query.return_clause.items[0].expression {
            // Inner should also be an Index expression
            if let Expression::Index {
                list: inner_list, ..
            } = list.as_ref()
            {
                if let Expression::List(items) = inner_list.as_ref() {
                    assert_eq!(items.len(), 2);
                } else {
                    panic!("Expected nested list literal");
                }
            } else {
                panic!("Expected inner Index expression");
            }
            // Outer index should be 1
            if let Expression::Literal(Literal::Int(i)) = index.as_ref() {
                assert_eq!(*i, 1);
            } else {
                panic!("Expected integer literal for outer index");
            }
        } else {
            panic!("Expected Index expression");
        }
    }

    #[test]
    fn test_parse_index_on_variable() {
        // Index access on a variable
        let query = parse("MATCH (n) RETURN n[0]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Index { list, index } = &query.return_clause.items[0].expression {
            if let Expression::Variable(var) = list.as_ref() {
                assert_eq!(var, "n");
            } else {
                panic!("Expected variable for list");
            }
            if let Expression::Literal(Literal::Int(i)) = index.as_ref() {
                assert_eq!(*i, 0);
            }
        } else {
            panic!("Expected Index expression");
        }
    }

    #[test]
    fn test_parse_index_with_expression() {
        // Index access with expression as index: list[n + 1]
        let query = parse("MATCH (n) RETURN [1, 2, 3][1 + 1]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Index { index, .. } = &query.return_clause.items[0].expression {
            if let Expression::BinaryOp { op, .. } = index.as_ref() {
                assert!(matches!(op, BinaryOperator::Add));
            } else {
                panic!("Expected binary op in index expression");
            }
        } else {
            panic!("Expected Index expression");
        }
    }

    #[test]
    fn test_parse_slice_on_property() {
        // Slice on a property: p.history[..10]
        let query = parse("MATCH (p:Person) RETURN p.history[..10]").unwrap();
        assert_eq!(query.return_clause.items.len(), 1);

        if let Expression::Slice { list, start, end } = &query.return_clause.items[0].expression {
            if let Expression::Property { variable, property } = list.as_ref() {
                assert_eq!(variable, "p");
                assert_eq!(property, "history");
            } else {
                panic!("Expected property access for list");
            }
            assert!(start.is_none());
            assert!(end.is_some());
        } else {
            panic!("Expected Slice expression");
        }
    }

    #[test]
    fn test_parse_index_in_where() {
        // Index access in WHERE clause
        let query = parse("MATCH (p:Person) WHERE p.tags[0] = 'admin' RETURN p").unwrap();
        assert!(query.where_clause.is_some());

        let where_clause = query.where_clause.unwrap();
        if let Expression::BinaryOp { left, op, .. } = where_clause.expression {
            assert!(matches!(op, BinaryOperator::Eq));
            if let Expression::Index { list, index } = *left {
                if let Expression::Property { variable, property } = *list {
                    assert_eq!(variable, "p");
                    assert_eq!(property, "tags");
                } else {
                    panic!("Expected property access");
                }
                if let Expression::Literal(Literal::Int(i)) = *index {
                    assert_eq!(i, 0);
                }
            } else {
                panic!("Expected Index expression on left of comparison");
            }
        } else {
            panic!("Expected BinaryOp");
        }
    }

    // =========================================================================
    // Pattern Comprehension Tests
    // =========================================================================

    #[test]
    fn test_parse_pattern_comprehension_simple() {
        // Basic pattern comprehension: [(p)-[:FRIEND]->(f) | f.name]
        let query =
            parse("MATCH (p:Person) RETURN p.name, [(p)-[:FRIEND]->(f) | f.name] AS friendNames")
                .unwrap();
        assert_eq!(query.return_clause.items.len(), 2);

        // Check the second return item is a pattern comprehension
        let item = &query.return_clause.items[1];
        assert_eq!(item.alias, Some("friendNames".to_string()));

        if let Expression::PatternComprehension {
            pattern,
            filter,
            transform,
        } = &item.expression
        {
            // Check pattern has 3 elements: node, edge, node
            assert_eq!(pattern.elements.len(), 3);

            // First node is (p)
            if let PatternElement::Node(node) = &pattern.elements[0] {
                assert_eq!(node.variable, Some("p".to_string()));
            } else {
                panic!("Expected node pattern");
            }

            // Edge is [:FRIEND]
            if let PatternElement::Edge(edge) = &pattern.elements[1] {
                assert_eq!(edge.labels, vec!["FRIEND".to_string()]);
                assert!(matches!(edge.direction, EdgeDirection::Outgoing));
            } else {
                panic!("Expected edge pattern");
            }

            // Last node is (f)
            if let PatternElement::Node(node) = &pattern.elements[2] {
                assert_eq!(node.variable, Some("f".to_string()));
            } else {
                panic!("Expected node pattern");
            }

            // No filter
            assert!(filter.is_none());

            // Transform is f.name
            if let Expression::Property { variable, property } = transform.as_ref() {
                assert_eq!(variable, "f");
                assert_eq!(property, "name");
            } else {
                panic!("Expected property access in transform");
            }
        } else {
            panic!("Expected PatternComprehension expression");
        }
    }

    #[test]
    fn test_parse_pattern_comprehension_with_filter() {
        // Pattern comprehension with WHERE filter: [(p)-[:FRIEND]->(f) WHERE f.age > 21 | f.name]
        let query = parse(
            "MATCH (p:Person) RETURN [(p)-[:FRIEND]->(f) WHERE f.age > 21 | f.name] AS adultFriends",
        )
        .unwrap();

        let item = &query.return_clause.items[0];
        if let Expression::PatternComprehension {
            pattern,
            filter,
            transform,
        } = &item.expression
        {
            // Pattern should have 3 elements
            assert_eq!(pattern.elements.len(), 3);

            // Should have a filter
            assert!(filter.is_some());
            let filter_expr = filter.as_ref().unwrap();
            if let Expression::BinaryOp { left, op, right } = filter_expr.as_ref() {
                assert!(matches!(op, BinaryOperator::Gt));
                if let Expression::Property { variable, property } = left.as_ref() {
                    assert_eq!(variable, "f");
                    assert_eq!(property, "age");
                }
                if let Expression::Literal(Literal::Int(val)) = right.as_ref() {
                    assert_eq!(*val, 21);
                }
            } else {
                panic!("Expected comparison in filter");
            }

            // Transform is f.name
            if let Expression::Property { variable, property } = transform.as_ref() {
                assert_eq!(variable, "f");
                assert_eq!(property, "name");
            } else {
                panic!("Expected property access in transform");
            }
        } else {
            panic!("Expected PatternComprehension expression");
        }
    }

    #[test]
    fn test_parse_pattern_comprehension_multi_hop() {
        // Multi-hop pattern: [(p)-[:FRIEND]->()-[:FRIEND]->(fof) | fof.name]
        let query = parse(
            "MATCH (p:Person) RETURN [(p)-[:FRIEND]->()-[:FRIEND]->(fof) | fof.name] AS fofNames",
        )
        .unwrap();

        let item = &query.return_clause.items[0];
        if let Expression::PatternComprehension { pattern, .. } = &item.expression {
            // Pattern should have 5 elements: node, edge, node, edge, node
            assert_eq!(pattern.elements.len(), 5);

            // Check structure
            assert!(matches!(&pattern.elements[0], PatternElement::Node(_)));
            assert!(matches!(&pattern.elements[1], PatternElement::Edge(_)));
            assert!(matches!(&pattern.elements[2], PatternElement::Node(_)));
            assert!(matches!(&pattern.elements[3], PatternElement::Edge(_)));
            assert!(matches!(&pattern.elements[4], PatternElement::Node(_)));

            // Last node should be (fof)
            if let PatternElement::Node(node) = &pattern.elements[4] {
                assert_eq!(node.variable, Some("fof".to_string()));
            }
        } else {
            panic!("Expected PatternComprehension expression");
        }
    }

    #[test]
    fn test_parse_pattern_comprehension_map_transform() {
        // Pattern comprehension with map transform
        let query = parse(
            "MATCH (p:Person) RETURN [(p)-[r:KNOWS]->(other) | {name: other.name}] AS contacts",
        )
        .unwrap();

        let item = &query.return_clause.items[0];
        if let Expression::PatternComprehension { transform, .. } = &item.expression {
            // Transform should be a map expression
            if let Expression::Map(entries) = transform.as_ref() {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].0, "name");
                if let Expression::Property { variable, property } = &entries[0].1 {
                    assert_eq!(variable, "other");
                    assert_eq!(property, "name");
                }
            } else {
                panic!("Expected Map expression in transform");
            }
        } else {
            panic!("Expected PatternComprehension expression");
        }
    }

    #[test]
    fn test_parse_pattern_comprehension_with_labels() {
        // Pattern with labels on target node
        let query = parse(
            "MATCH (p:Person) RETURN [(p)-[:PURCHASED]->(item:Product) | item.name] AS purchases",
        )
        .unwrap();

        let item = &query.return_clause.items[0];
        if let Expression::PatternComprehension { pattern, .. } = &item.expression {
            // Last node should have label Product
            if let PatternElement::Node(node) = &pattern.elements[2] {
                assert_eq!(node.variable, Some("item".to_string()));
                assert_eq!(node.labels, vec!["Product".to_string()]);
            } else {
                panic!("Expected node pattern");
            }
        } else {
            panic!("Expected PatternComprehension expression");
        }
    }

    // =========================================================================
    // FOREACH Parser Tests
    // =========================================================================

    #[test]
    fn test_parse_foreach_single_set() {
        // Basic FOREACH with a single SET mutation (must be in a mutation statement)
        let stmt = parse_statement(
            "MATCH (p:Person) SET p.processed = false FOREACH (n IN [1, 2, 3] | SET p.count = n)",
        )
        .unwrap();

        if let Statement::Mutation(mutation) = stmt {
            assert_eq!(mutation.foreach_clauses.len(), 1);

            let foreach = &mutation.foreach_clauses[0];
            assert_eq!(foreach.variable, "n");
            assert_eq!(foreach.mutations.len(), 1);

            // Check list expression
            if let Expression::List(items) = &foreach.list {
                assert_eq!(items.len(), 3);
            } else {
                panic!("Expected list expression");
            }

            // Check mutation is SET
            assert!(matches!(&foreach.mutations[0], ForeachMutation::Set(_)));
        } else {
            panic!("Expected Mutation statement");
        }
    }

    #[test]
    fn test_parse_foreach_multiple_mutations() {
        // FOREACH with multiple mutations
        let stmt = parse_statement(
            "MATCH (p:Person) SET p.init = true FOREACH (n IN [1, 2] | SET p.x = 1 SET p.y = 2)",
        )
        .unwrap();

        if let Statement::Mutation(mutation) = stmt {
            assert_eq!(mutation.foreach_clauses.len(), 1);
            let foreach = &mutation.foreach_clauses[0];
            assert_eq!(foreach.mutations.len(), 2);

            // Both should be SET mutations
            assert!(matches!(&foreach.mutations[0], ForeachMutation::Set(_)));
            assert!(matches!(&foreach.mutations[1], ForeachMutation::Set(_)));
        } else {
            panic!("Expected Mutation statement");
        }
    }

    #[test]
    fn test_parse_foreach_with_remove() {
        // FOREACH with REMOVE mutation
        let stmt = parse_statement(
            "MATCH (p:Person) SET p.processed = true FOREACH (n IN [1] | REMOVE p.temp)",
        )
        .unwrap();

        if let Statement::Mutation(mutation) = stmt {
            let foreach = &mutation.foreach_clauses[0];
            assert!(matches!(&foreach.mutations[0], ForeachMutation::Remove(_)));
        } else {
            panic!("Expected Mutation statement");
        }
    }

    #[test]
    fn test_parse_foreach_in_mutation_statement() {
        // FOREACH inside a mutation statement (MATCH ... SET ... FOREACH ...)
        let stmt = parse_statement(
            "MATCH (p:Person)-[:KNOWS]->(f) SET p.hasKnown = true FOREACH (x IN [f] | SET x.known = true)",
        )
        .unwrap();

        if let Statement::Mutation(mutation) = stmt {
            // Should have at least one mutation and one foreach
            assert!(!mutation.mutations.is_empty());
            assert_eq!(mutation.foreach_clauses.len(), 1);
        } else {
            panic!("Expected Mutation statement");
        }
    }

    #[test]
    fn test_parse_multiple_foreach_clauses() {
        // Multiple FOREACH clauses in sequence
        let stmt = parse_statement(
            "MATCH (p:Person) SET p.init = true FOREACH (x IN [1] | SET p.a = x) FOREACH (y IN [2] | SET p.b = y)",
        )
        .unwrap();

        if let Statement::Mutation(mutation) = stmt {
            assert_eq!(mutation.foreach_clauses.len(), 2);
            assert_eq!(mutation.foreach_clauses[0].variable, "x");
            assert_eq!(mutation.foreach_clauses[1].variable, "y");
        } else {
            panic!("Expected Mutation statement");
        }
    }
}
