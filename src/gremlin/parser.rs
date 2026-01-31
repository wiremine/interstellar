//! Parser for Gremlin query strings.
//!
//! This module uses pest to parse Gremlin text into an AST.

use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

use crate::gremlin::ast::*;
use crate::gremlin::error::ParseError;

#[derive(Parser)]
#[grammar = "gremlin/grammar.pest"]
pub struct GremlinParser;

/// Parse a Gremlin query string into an AST.
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::gremlin::parse;
///
/// let ast = parse("g.V().hasLabel('person').values('name')")?;
/// ```
pub fn parse(input: &str) -> Result<GremlinTraversal, ParseError> {
    if input.trim().is_empty() {
        return Err(ParseError::Empty);
    }

    let pairs = GremlinParser::parse(Rule::traversal, input).map_err(ParseError::from_pest)?;

    let traversal_pair = pairs.into_iter().next().ok_or(ParseError::Empty)?;

    build_traversal(traversal_pair)
}

/// Parse a multi-statement Gremlin script into an AST.
///
/// Scripts support variable assignment and reference:
/// ```gremlin
/// alice = g.addV('person').property('name', 'Alice').next()
/// bob = g.addV('person').property('name', 'Bob').next()
/// g.addE('knows').from(alice).to(bob).next()
/// g.V(alice).out('knows').values('name').toList()
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::gremlin::parse_script;
///
/// let script = parse_script(r#"
///     alice = g.addV('person').property('name', 'Alice').next()
///     g.V(alice).values('name').toList()
/// "#)?;
/// ```
pub fn parse_script(input: &str) -> Result<Script, ParseError> {
    if input.trim().is_empty() {
        return Err(ParseError::Empty);
    }

    let pairs = GremlinParser::parse(Rule::script, input).map_err(ParseError::from_pest)?;

    let script_pair = pairs.into_iter().next().ok_or(ParseError::Empty)?;
    let span = Span::new(script_pair.as_span().start(), script_pair.as_span().end());

    let mut statements = Vec::new();

    for inner in script_pair.into_inner() {
        match inner.as_rule() {
            Rule::statement => {
                statements.push(build_statement(inner)?);
            }
            Rule::EOI => {}
            _ => {}
        }
    }

    Ok(Script { statements, span })
}

fn build_statement(pair: Pair<Rule>) -> Result<Statement, ParseError> {
    let span = Span::new(pair.as_span().start(), pair.as_span().end());

    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("Empty statement".to_string()))?;

    match inner.as_rule() {
        Rule::assignment => build_assignment(inner, span),
        Rule::traversal_expression => build_traversal_expression(inner, span),
        _ => Err(ParseError::Syntax(format!(
            "Unknown statement type: {:?}",
            inner.as_rule()
        ))),
    }
}

fn build_assignment(pair: Pair<Rule>, span: Span) -> Result<Statement, ParseError> {
    let mut parts = pair.into_inner();

    // Variable name
    let name_pair = parts
        .next()
        .ok_or_else(|| ParseError::Syntax("Assignment missing variable name".to_string()))?;
    let name = name_pair.as_str().to_string();

    // Traversal body
    let body_pair = parts
        .find(|p| p.as_rule() == Rule::traversal_body)
        .ok_or_else(|| ParseError::Syntax("Assignment missing traversal body".to_string()))?;

    // Terminal step
    let terminal_pair = parts.find(|p| p.as_rule() == Rule::terminal_step);

    let traversal = build_traversal_from_body(body_pair, terminal_pair)?;

    Ok(Statement::Assignment {
        name,
        traversal,
        span,
    })
}

fn build_traversal_expression(pair: Pair<Rule>, span: Span) -> Result<Statement, ParseError> {
    let mut inner_iter = pair.into_inner();

    let body_pair = inner_iter
        .find(|p| p.as_rule() == Rule::traversal_body)
        .ok_or_else(|| ParseError::Syntax("Traversal expression missing body".to_string()))?;

    let terminal_pair = inner_iter.find(|p| p.as_rule() == Rule::terminal_step);

    let traversal = build_traversal_from_body(body_pair, terminal_pair)?;

    Ok(Statement::Traversal { traversal, span })
}

fn build_traversal_from_body(
    body_pair: Pair<Rule>,
    terminal_pair: Option<Pair<Rule>>,
) -> Result<GremlinTraversal, ParseError> {
    let span = Span::new(body_pair.as_span().start(), body_pair.as_span().end());

    let mut source: Option<SourceStep> = None;
    let mut steps: Vec<Step> = Vec::new();

    for inner in body_pair.into_inner() {
        match inner.as_rule() {
            Rule::graph_source => {
                source = Some(build_source(inner)?);
            }
            Rule::step => {
                steps.push(build_step(inner)?);
            }
            _ => {}
        }
    }

    let source = source.ok_or(ParseError::MissingSource)?;

    let terminal = if let Some(term_pair) = terminal_pair {
        Some(build_terminal(term_pair)?)
    } else {
        None
    };

    Ok(GremlinTraversal {
        source,
        steps,
        terminal,
        span,
    })
}

fn build_traversal(pair: Pair<Rule>) -> Result<GremlinTraversal, ParseError> {
    let span = Span::new(pair.as_span().start(), pair.as_span().end());

    let mut source: Option<SourceStep> = None;
    let mut steps: Vec<Step> = Vec::new();
    let mut terminal: Option<TerminalStep> = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::graph_source => {
                source = Some(build_source(inner)?);
            }
            Rule::step => {
                steps.push(build_step(inner)?);
            }
            Rule::terminal_step => {
                terminal = Some(build_terminal(inner)?);
            }
            Rule::EOI => {}
            _ => {}
        }
    }

    let source = source.ok_or(ParseError::MissingSource)?;

    Ok(GremlinTraversal {
        source,
        steps,
        terminal,
        span,
    })
}

fn build_source(pair: Pair<Rule>) -> Result<SourceStep, ParseError> {
    // graph_source = { "g" ~ "." ~ source_step }
    let inner = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::source_step)
        .ok_or_else(|| ParseError::Syntax("Expected source step".to_string()))?;

    let step_pair = inner
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("Expected V, E, addV, addE, or inject".to_string()))?;

    let span = Span::new(step_pair.as_span().start(), step_pair.as_span().end());

    match step_pair.as_rule() {
        Rule::v_step => {
            let (ids, variable) = build_vertex_source_args(step_pair)?;
            Ok(SourceStep::V {
                ids,
                variable,
                span,
            })
        }
        Rule::e_step => {
            let (ids, variable) = build_vertex_source_args(step_pair)?;
            Ok(SourceStep::E {
                ids,
                variable,
                span,
            })
        }
        Rule::add_v_source_step => {
            let label = extract_string(step_pair)?;
            Ok(SourceStep::AddV { label, span })
        }
        Rule::add_e_source_step => {
            let label = extract_string(step_pair)?;
            Ok(SourceStep::AddE { label, span })
        }
        Rule::inject_step => {
            let values = build_value_list_opt(step_pair)?;
            Ok(SourceStep::Inject { values, span })
        }
        _ => Err(ParseError::Syntax(format!(
            "Unknown source step: {:?}",
            step_pair.as_rule()
        ))),
    }
}

/// Build V() or E() source arguments, which can be:
/// - Empty (all vertices/edges)
/// - Variable reference (g.V(alice))
/// - Value list (g.V(1, 2, 3))
fn build_vertex_source_args(
    pair: Pair<Rule>,
) -> Result<(Vec<Literal>, Option<String>), ParseError> {
    let mut ids = Vec::new();
    let mut variable = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable_ref => {
                // Extract the variable name from variable_ref -> variable_name
                // First capture the string in case into_inner fails
                let fallback = inner.as_str().to_string();
                let var_name = inner
                    .into_inner()
                    .next()
                    .map(|p| p.as_str().to_string())
                    .unwrap_or(fallback);
                variable = Some(var_name);
            }
            Rule::value_list => {
                ids = collect_values_from_list(inner)?;
            }
            _ => {}
        }
    }

    Ok((ids, variable))
}

fn build_step(pair: Pair<Rule>) -> Result<Step, ParseError> {
    // step = { "." ~ step_body }
    let step_body = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::step_body)
        .ok_or_else(|| ParseError::Syntax("Expected step body".to_string()))?;

    let inner = step_body
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("Expected step content".to_string()))?;

    let span = Span::new(inner.as_span().start(), inner.as_span().end());

    match inner.as_rule() {
        // Navigation
        Rule::out_step => Ok(Step::Out {
            labels: build_label_list_opt(inner)?,
            span,
        }),
        Rule::in_step => Ok(Step::In {
            labels: build_label_list_opt(inner)?,
            span,
        }),
        Rule::both_step => Ok(Step::Both {
            labels: build_label_list_opt(inner)?,
            span,
        }),
        Rule::out_e_step => Ok(Step::OutE {
            labels: build_label_list_opt(inner)?,
            span,
        }),
        Rule::in_e_step => Ok(Step::InE {
            labels: build_label_list_opt(inner)?,
            span,
        }),
        Rule::both_e_step => Ok(Step::BothE {
            labels: build_label_list_opt(inner)?,
            span,
        }),
        Rule::out_v_step => Ok(Step::OutV { span }),
        Rule::in_v_step => Ok(Step::InV { span }),
        Rule::both_v_step => Ok(Step::BothV { span }),
        Rule::other_v_step => Ok(Step::OtherV { span }),

        // Filter
        Rule::has_step => build_has_step(inner, span),
        Rule::has_label_step => {
            let labels = collect_strings(inner)?;
            Ok(Step::HasLabel { labels, span })
        }
        Rule::has_id_step => {
            let ids = collect_values(inner)?;
            Ok(Step::HasId { ids, span })
        }
        Rule::has_not_step => {
            let key = extract_string(inner)?;
            Ok(Step::HasNot { key, span })
        }
        Rule::has_key_step => {
            let keys = collect_strings(inner)?;
            Ok(Step::HasKey { keys, span })
        }
        Rule::has_value_step => {
            let values = collect_values(inner)?;
            Ok(Step::HasValue { values, span })
        }
        Rule::where_step => build_where_step(inner, span),
        Rule::is_step => build_is_step(inner, span),
        Rule::and_step => {
            let traversals = collect_anonymous_traversals(inner)?;
            Ok(Step::And { traversals, span })
        }
        Rule::or_step => {
            let traversals = collect_anonymous_traversals(inner)?;
            Ok(Step::Or { traversals, span })
        }
        Rule::not_step => {
            let traversal = build_anonymous_traversal(
                inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::anonymous_traversal)
                    .ok_or_else(|| ParseError::Syntax("not() requires traversal".to_string()))?,
            )?;
            Ok(Step::Not {
                traversal: Box::new(traversal),
                span,
            })
        }
        Rule::dedup_step => {
            let by_label = inner.into_inner().find_map(|p| {
                if p.as_rule() == Rule::string {
                    parse_string_value(p).ok()
                } else {
                    None
                }
            });
            Ok(Step::Dedup { by_label, span })
        }
        Rule::limit_step => {
            let count = extract_integer(inner)? as u64;
            Ok(Step::Limit { count, span })
        }
        Rule::skip_step => {
            let count = extract_integer(inner)? as u64;
            Ok(Step::Skip { count, span })
        }
        Rule::range_step => {
            let mut nums = inner.into_inner().filter(|p| p.as_rule() == Rule::integer);
            let start = parse_integer(
                nums.next()
                    .ok_or_else(|| ParseError::Syntax("range() requires start".to_string()))?,
            )? as u64;
            let end = parse_integer(
                nums.next()
                    .ok_or_else(|| ParseError::Syntax("range() requires end".to_string()))?,
            )? as u64;
            Ok(Step::Range { start, end, span })
        }
        Rule::tail_step => {
            let count = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::integer)
                .map(|p| parse_integer(p))
                .transpose()?
                .map(|n| n as u64);
            Ok(Step::Tail { count, span })
        }
        Rule::coin_step => {
            let probability = extract_float(inner)?;
            Ok(Step::Coin { probability, span })
        }
        Rule::sample_step => {
            let count = extract_integer(inner)? as u64;
            Ok(Step::Sample { count, span })
        }
        Rule::simple_path_step => Ok(Step::SimplePath { span }),
        Rule::cyclic_path_step => Ok(Step::CyclicPath { span }),

        // Transform
        Rule::values_step => {
            let keys = collect_strings(inner)?;
            Ok(Step::Values { keys, span })
        }
        Rule::properties_step => {
            let keys = collect_strings(inner)?;
            Ok(Step::Properties { keys, span })
        }
        Rule::value_map_step => build_value_map_step(inner, span),
        Rule::element_map_step => {
            let keys = collect_strings(inner)?;
            Ok(Step::ElementMap { keys, span })
        }
        Rule::property_map_step => {
            let keys = collect_strings(inner)?;
            Ok(Step::PropertyMap { keys, span })
        }
        Rule::id_step => Ok(Step::Id { span }),
        Rule::label_step => Ok(Step::Label { span }),
        Rule::key_step => Ok(Step::Key { span }),
        Rule::value_step => Ok(Step::Value { span }),
        Rule::path_step => Ok(Step::Path { span }),
        Rule::select_step => {
            let labels = collect_strings(inner)?;
            Ok(Step::Select { labels, span })
        }
        Rule::project_step => {
            let keys = collect_strings(inner)?;
            Ok(Step::Project { keys, span })
        }
        Rule::by_step => build_by_step(inner, span),
        Rule::unfold_step => Ok(Step::Unfold { span }),
        Rule::fold_step => Ok(Step::Fold { span }),
        Rule::count_step => Ok(Step::Count { span }),
        Rule::sum_step => Ok(Step::Sum { span }),
        Rule::max_step => Ok(Step::Max { span }),
        Rule::min_step => Ok(Step::Min { span }),
        Rule::mean_step => Ok(Step::Mean { span }),
        Rule::group_step => Ok(Step::Group { span }),
        Rule::group_count_step => Ok(Step::GroupCount { span }),
        Rule::order_step => Ok(Step::Order { span }),
        Rule::math_step => {
            let expression = extract_string(inner)?;
            Ok(Step::Math { expression, span })
        }
        Rule::constant_step => {
            let value = extract_value(inner)?;
            Ok(Step::Constant { value, span })
        }
        Rule::identity_step => Ok(Step::Identity { span }),
        Rule::index_step => Ok(Step::Index { span }),
        Rule::loops_step => Ok(Step::Loops { span }),

        // Branch
        Rule::choose_step => build_choose_step(inner, span),
        Rule::union_step => {
            let traversals = collect_anonymous_traversals(inner)?;
            Ok(Step::Union { traversals, span })
        }
        Rule::coalesce_step => {
            let traversals = collect_anonymous_traversals(inner)?;
            Ok(Step::Coalesce { traversals, span })
        }
        Rule::optional_step => {
            let traversal = build_anonymous_traversal(
                inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::anonymous_traversal)
                    .ok_or_else(|| {
                        ParseError::Syntax("optional() requires traversal".to_string())
                    })?,
            )?;
            Ok(Step::Optional {
                traversal: Box::new(traversal),
                span,
            })
        }
        Rule::local_step => {
            let traversal = build_anonymous_traversal(
                inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::anonymous_traversal)
                    .ok_or_else(|| ParseError::Syntax("local() requires traversal".to_string()))?,
            )?;
            Ok(Step::Local {
                traversal: Box::new(traversal),
                span,
            })
        }
        Rule::branch_step => {
            let traversal = build_anonymous_traversal(
                inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::anonymous_traversal)
                    .ok_or_else(|| ParseError::Syntax("branch() requires traversal".to_string()))?,
            )?;
            Ok(Step::Branch {
                traversal: Box::new(traversal),
                span,
            })
        }
        Rule::option_step => build_option_step(inner, span),

        // Repeat
        Rule::repeat_step => {
            let traversal = build_anonymous_traversal(
                inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::anonymous_traversal)
                    .ok_or_else(|| ParseError::Syntax("repeat() requires traversal".to_string()))?,
            )?;
            Ok(Step::Repeat {
                traversal: Box::new(traversal),
                span,
            })
        }
        Rule::times_step => {
            let count = extract_integer(inner)? as u32;
            Ok(Step::Times { count, span })
        }
        Rule::until_step => {
            let traversal = build_anonymous_traversal(
                inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::anonymous_traversal)
                    .ok_or_else(|| ParseError::Syntax("until() requires traversal".to_string()))?,
            )?;
            Ok(Step::Until {
                traversal: Box::new(traversal),
                span,
            })
        }
        Rule::emit_step => {
            let traversal = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::anonymous_traversal)
                .map(|p| build_anonymous_traversal(p))
                .transpose()?
                .map(Box::new);
            Ok(Step::Emit { traversal, span })
        }

        // Side Effect
        Rule::as_step => {
            let label = extract_string(inner)?;
            Ok(Step::As { label, span })
        }
        Rule::aggregate_step => {
            let key = extract_string(inner)?;
            Ok(Step::Aggregate { key, span })
        }
        Rule::store_step => {
            let key = extract_string(inner)?;
            Ok(Step::Store { key, span })
        }
        Rule::cap_step => {
            let keys = collect_strings(inner)?;
            Ok(Step::Cap { keys, span })
        }
        Rule::side_effect_step => {
            let traversal = build_anonymous_traversal(
                inner
                    .into_inner()
                    .find(|p| p.as_rule() == Rule::anonymous_traversal)
                    .ok_or_else(|| {
                        ParseError::Syntax("sideEffect() requires traversal".to_string())
                    })?,
            )?;
            Ok(Step::SideEffect {
                traversal: Box::new(traversal),
                span,
            })
        }
        Rule::profile_step => {
            let key = inner.into_inner().find_map(|p| {
                if p.as_rule() == Rule::string {
                    parse_string_value(p).ok()
                } else {
                    None
                }
            });
            Ok(Step::Profile { key, span })
        }

        // Mutation
        Rule::add_v_inline_step => {
            let label = extract_string(inner)?;
            Ok(Step::AddV { label, span })
        }
        Rule::add_e_inline_step => {
            let label = extract_string(inner)?;
            Ok(Step::AddE { label, span })
        }
        Rule::property_step => build_property_step(inner, span),
        Rule::from_step => {
            let args = build_from_to_args(inner)?;
            Ok(Step::From { args, span })
        }
        Rule::to_step => {
            let args = build_from_to_args(inner)?;
            Ok(Step::To { args, span })
        }
        Rule::drop_step => Ok(Step::Drop { span }),

        _ => Err(ParseError::Syntax(format!(
            "Unknown step: {:?}",
            inner.as_rule()
        ))),
    }
}

fn build_terminal(pair: Pair<Rule>) -> Result<TerminalStep, ParseError> {
    // terminal_step = { "." ~ terminal_body }
    let body = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::terminal_body)
        .ok_or_else(|| ParseError::Syntax("Expected terminal body".to_string()))?;

    let inner = body
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("Expected terminal step content".to_string()))?;

    let span = Span::new(inner.as_span().start(), inner.as_span().end());

    match inner.as_rule() {
        Rule::next_step => {
            let count = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::integer)
                .map(|p| parse_integer(p))
                .transpose()?
                .map(|n| n as u64);
            Ok(TerminalStep::Next { count, span })
        }
        Rule::to_list_step => Ok(TerminalStep::ToList { span }),
        Rule::to_set_step => Ok(TerminalStep::ToSet { span }),
        Rule::iterate_step => Ok(TerminalStep::Iterate { span }),
        Rule::has_next_step => Ok(TerminalStep::HasNext { span }),
        _ => Err(ParseError::Syntax(format!(
            "Unknown terminal step: {:?}",
            inner.as_rule()
        ))),
    }
}

// ============================================================
// Helper functions for building specific step types
// ============================================================

fn build_has_step(pair: Pair<Rule>, span: Span) -> Result<Step, ParseError> {
    let has_args = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::has_args)
        .ok_or_else(|| ParseError::Syntax("has() requires arguments".to_string()))?;

    let inner = has_args
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("has() arguments empty".to_string()))?;

    let args = match inner.as_rule() {
        Rule::has_key_only => {
            let key = parse_string_value(
                inner
                    .into_inner()
                    .next()
                    .ok_or_else(|| ParseError::Syntax("has() key missing".to_string()))?,
            )?;
            HasArgs::Key(key)
        }
        Rule::has_key_value => {
            let mut parts = inner.into_inner();
            let key = parse_string_value(
                parts
                    .next()
                    .ok_or_else(|| ParseError::Syntax("has() key missing".to_string()))?,
            )?;
            let value = build_value(
                parts
                    .next()
                    .ok_or_else(|| ParseError::Syntax("has() value missing".to_string()))?,
            )?;
            HasArgs::KeyValue { key, value }
        }
        Rule::has_key_predicate => {
            let mut parts = inner.into_inner();
            let key = parse_string_value(
                parts
                    .next()
                    .ok_or_else(|| ParseError::Syntax("has() key missing".to_string()))?,
            )?;
            let predicate = build_predicate(
                parts
                    .next()
                    .ok_or_else(|| ParseError::Syntax("has() predicate missing".to_string()))?,
            )?;
            HasArgs::KeyPredicate { key, predicate }
        }
        Rule::has_label_key_value => {
            let mut parts = inner
                .into_inner()
                .filter(|p| matches!(p.as_rule(), Rule::string | Rule::value));
            let label = parse_string_value(
                parts
                    .next()
                    .ok_or_else(|| ParseError::Syntax("has() label missing".to_string()))?,
            )?;
            let key = parse_string_value(
                parts
                    .next()
                    .ok_or_else(|| ParseError::Syntax("has() key missing".to_string()))?,
            )?;
            let value = build_value(
                parts
                    .next()
                    .ok_or_else(|| ParseError::Syntax("has() value missing".to_string()))?,
            )?;
            HasArgs::LabelKeyValue { label, key, value }
        }
        _ => {
            return Err(ParseError::Syntax(format!(
                "Unknown has() variant: {:?}",
                inner.as_rule()
            )))
        }
    };

    Ok(Step::Has { args, span })
}

fn build_where_step(pair: Pair<Rule>, span: Span) -> Result<Step, ParseError> {
    let where_args = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::where_args)
        .ok_or_else(|| ParseError::Syntax("where() requires arguments".to_string()))?;

    let inner = where_args
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("where() arguments empty".to_string()))?;

    let args = match inner.as_rule() {
        Rule::where_predicate => {
            let pred_pair = inner
                .into_inner()
                .next()
                .ok_or_else(|| ParseError::Syntax("where() predicate missing".to_string()))?;
            WhereArgs::Predicate(build_predicate(pred_pair)?)
        }
        Rule::where_traversal => {
            let trav_pair = inner
                .into_inner()
                .next()
                .ok_or_else(|| ParseError::Syntax("where() traversal missing".to_string()))?;
            WhereArgs::Traversal(Box::new(build_anonymous_traversal(trav_pair)?))
        }
        _ => {
            return Err(ParseError::Syntax(format!(
                "Unknown where() variant: {:?}",
                inner.as_rule()
            )))
        }
    };

    Ok(Step::Where { args, span })
}

fn build_is_step(pair: Pair<Rule>, span: Span) -> Result<Step, ParseError> {
    let is_arg = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::is_arg)
        .ok_or_else(|| ParseError::Syntax("is() requires argument".to_string()))?;

    let inner = is_arg
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("is() argument empty".to_string()))?;

    let args = match inner.as_rule() {
        Rule::predicate => IsArgs::Predicate(build_predicate(inner)?),
        _ => IsArgs::Value(build_value(inner)?),
    };

    Ok(Step::Is { args, span })
}

fn build_value_map_step(pair: Pair<Rule>, span: Span) -> Result<Step, ParseError> {
    let mut args = ValueMapArgs::default();

    if let Some(args_pair) = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::value_map_args)
    {
        for inner in args_pair.into_inner() {
            match inner.as_rule() {
                Rule::boolean => {
                    args.include_tokens = inner.as_str() == "true";
                }
                Rule::string => {
                    args.keys.push(parse_string_value(inner)?);
                }
                _ => {}
            }
        }
    }

    Ok(Step::ValueMap { args, span })
}

fn build_by_step(pair: Pair<Rule>, span: Span) -> Result<Step, ParseError> {
    let by_arg_opt = pair.into_inner().find(|p| p.as_rule() == Rule::by_arg);

    let args = if let Some(by_arg) = by_arg_opt {
        let inner = by_arg
            .into_inner()
            .next()
            .ok_or_else(|| ParseError::Syntax("by() argument empty".to_string()))?;

        match inner.as_rule() {
            Rule::order_direction => ByArgs::Order(parse_order_direction(&inner)?),
            Rule::by_key_direction => {
                let mut parts = inner.into_inner();
                let key = parse_string_value(
                    parts
                        .next()
                        .ok_or_else(|| ParseError::Syntax("by() key missing".to_string()))?,
                )?;
                let order_pair = parts
                    .next()
                    .ok_or_else(|| ParseError::Syntax("by() order missing".to_string()))?;
                let order = parse_order_direction(&order_pair)?;
                ByArgs::KeyOrder { key, order }
            }
            Rule::anonymous_traversal => {
                ByArgs::Traversal(Box::new(build_anonymous_traversal(inner)?))
            }
            Rule::string => ByArgs::Key(parse_string_value(inner)?),
            _ => {
                return Err(ParseError::Syntax(format!(
                    "Unknown by() variant: {:?}",
                    inner.as_rule()
                )))
            }
        }
    } else {
        ByArgs::Identity
    };

    Ok(Step::By { args, span })
}

fn build_choose_step(pair: Pair<Rule>, span: Span) -> Result<Step, ParseError> {
    let choose_args = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::choose_args)
        .ok_or_else(|| ParseError::Syntax("choose() requires arguments".to_string()))?;

    let inner = choose_args
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("choose() arguments empty".to_string()))?;

    let args = match inner.as_rule() {
        Rule::choose_if_then_else => {
            let travs: Vec<_> = inner
                .into_inner()
                .filter(|p| p.as_rule() == Rule::anonymous_traversal)
                .collect();
            if travs.len() != 3 {
                return Err(ParseError::Syntax(
                    "choose() if-then-else requires 3 traversals".to_string(),
                ));
            }
            let mut iter = travs.into_iter();
            ChooseArgs::IfThenElse {
                condition: Box::new(build_anonymous_traversal(iter.next().unwrap())?),
                if_true: Box::new(build_anonymous_traversal(iter.next().unwrap())?),
                if_false: Box::new(build_anonymous_traversal(iter.next().unwrap())?),
            }
        }
        Rule::choose_by_traversal => {
            let trav = inner
                .into_inner()
                .next()
                .ok_or_else(|| ParseError::Syntax("choose() traversal missing".to_string()))?;
            ChooseArgs::ByTraversal(Box::new(build_anonymous_traversal(trav)?))
        }
        Rule::choose_predicate => {
            let pred = inner
                .into_inner()
                .next()
                .ok_or_else(|| ParseError::Syntax("choose() predicate missing".to_string()))?;
            ChooseArgs::ByPredicate(build_predicate(pred)?)
        }
        _ => {
            return Err(ParseError::Syntax(format!(
                "Unknown choose() variant: {:?}",
                inner.as_rule()
            )))
        }
    };

    Ok(Step::Choose { args, span })
}

fn build_option_step(pair: Pair<Rule>, span: Span) -> Result<Step, ParseError> {
    let option_args = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::option_args)
        .ok_or_else(|| ParseError::Syntax("option() requires arguments".to_string()))?;

    let inner = option_args
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("option() arguments empty".to_string()))?;

    let args = match inner.as_rule() {
        Rule::option_none => {
            let trav = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::anonymous_traversal)
                .ok_or_else(|| ParseError::Syntax("option(none) requires traversal".to_string()))?;
            OptionArgs::None {
                traversal: Box::new(build_anonymous_traversal(trav)?),
            }
        }
        Rule::option_key_value => {
            let mut parts = inner.into_inner();
            let key = build_value(
                parts
                    .next()
                    .ok_or_else(|| ParseError::Syntax("option() key missing".to_string()))?,
            )?;
            let trav = parts
                .find(|p| p.as_rule() == Rule::anonymous_traversal)
                .ok_or_else(|| ParseError::Syntax("option() traversal missing".to_string()))?;
            OptionArgs::KeyValue {
                key,
                traversal: Box::new(build_anonymous_traversal(trav)?),
            }
        }
        _ => {
            return Err(ParseError::Syntax(format!(
                "Unknown option() variant: {:?}",
                inner.as_rule()
            )))
        }
    };

    Ok(Step::Option { args, span })
}

fn build_property_step(pair: Pair<Rule>, span: Span) -> Result<Step, ParseError> {
    let prop_args = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::property_args)
        .ok_or_else(|| ParseError::Syntax("property() requires arguments".to_string()))?;

    let inner = prop_args
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("property() arguments empty".to_string()))?;

    let args =
        match inner.as_rule() {
            Rule::property_key_value => {
                let mut parts = inner.into_inner();
                let key =
                    parse_string_value(parts.next().ok_or_else(|| {
                        ParseError::Syntax("property() key missing".to_string())
                    })?)?;
                let value =
                    build_value(parts.next().ok_or_else(|| {
                        ParseError::Syntax("property() value missing".to_string())
                    })?)?;
                PropertyArgs {
                    cardinality: None,
                    key,
                    value,
                }
            }
            Rule::property_cardinality => {
                let mut parts = inner.into_inner();
                let card_pair = parts.next().ok_or_else(|| {
                    ParseError::Syntax("property() cardinality missing".to_string())
                })?;
                let cardinality = Some(parse_cardinality(&card_pair)?);
                let key =
                    parse_string_value(parts.next().ok_or_else(|| {
                        ParseError::Syntax("property() key missing".to_string())
                    })?)?;
                let value =
                    build_value(parts.next().ok_or_else(|| {
                        ParseError::Syntax("property() value missing".to_string())
                    })?)?;
                PropertyArgs {
                    cardinality,
                    key,
                    value,
                }
            }
            _ => {
                return Err(ParseError::Syntax(format!(
                    "Unknown property() variant: {:?}",
                    inner.as_rule()
                )))
            }
        };

    Ok(Step::Property { args, span })
}

fn build_from_to_args(pair: Pair<Rule>) -> Result<FromToArgs, ParseError> {
    let from_to_arg = pair
        .into_inner()
        .find(|p| p.as_rule() == Rule::from_to_arg)
        .ok_or_else(|| ParseError::Syntax("from/to() requires argument".to_string()))?;

    let inner = from_to_arg
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("from/to() argument empty".to_string()))?;

    match inner.as_rule() {
        Rule::anonymous_traversal => Ok(FromToArgs::Traversal(Box::new(
            build_anonymous_traversal(inner)?,
        ))),
        Rule::string => Ok(FromToArgs::Label(parse_string_value(inner)?)),
        Rule::variable_ref => {
            // Extract variable name from variable_ref -> variable_name
            // Capture fallback first before consuming inner
            let fallback = inner.as_str().to_string();
            let var_name = inner
                .into_inner()
                .next()
                .map(|p| p.as_str().to_string())
                .unwrap_or(fallback);
            Ok(FromToArgs::Variable(var_name))
        }
        _ => Ok(FromToArgs::Id(build_value(inner)?)),
    }
}

// ============================================================
// Anonymous traversal and predicates
// ============================================================

fn build_anonymous_traversal(pair: Pair<Rule>) -> Result<AnonymousTraversal, ParseError> {
    let span = Span::new(pair.as_span().start(), pair.as_span().end());
    let mut steps = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::step {
            steps.push(build_step(inner)?);
        }
    }

    Ok(AnonymousTraversal { steps, span })
}

fn build_predicate(pair: Pair<Rule>) -> Result<Predicate, ParseError> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("Predicate empty".to_string()))?;

    match inner.as_rule() {
        Rule::p_predicate => {
            let method = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::p_method)
                .ok_or_else(|| ParseError::Syntax("P. method missing".to_string()))?;
            build_p_method(method)
        }
        Rule::text_p_predicate => {
            let method = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::text_p_method)
                .ok_or_else(|| ParseError::Syntax("TextP. method missing".to_string()))?;
            build_text_p_method(method)
        }
        _ => Err(ParseError::Syntax(format!(
            "Unknown predicate type: {:?}",
            inner.as_rule()
        ))),
    }
}

fn build_p_method(pair: Pair<Rule>) -> Result<Predicate, ParseError> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("P method empty".to_string()))?;

    match inner.as_rule() {
        Rule::p_eq => Ok(Predicate::Eq(extract_value(inner)?)),
        Rule::p_neq => Ok(Predicate::Neq(extract_value(inner)?)),
        Rule::p_lt => Ok(Predicate::Lt(extract_value(inner)?)),
        Rule::p_lte => Ok(Predicate::Lte(extract_value(inner)?)),
        Rule::p_gt => Ok(Predicate::Gt(extract_value(inner)?)),
        Rule::p_gte => Ok(Predicate::Gte(extract_value(inner)?)),
        Rule::p_between => {
            let values = collect_values(inner)?;
            if values.len() != 2 {
                return Err(ParseError::Syntax(
                    "between() requires 2 values".to_string(),
                ));
            }
            let mut iter = values.into_iter();
            Ok(Predicate::Between {
                start: iter.next().unwrap(),
                end: iter.next().unwrap(),
            })
        }
        Rule::p_inside => {
            let values = collect_values(inner)?;
            if values.len() != 2 {
                return Err(ParseError::Syntax("inside() requires 2 values".to_string()));
            }
            let mut iter = values.into_iter();
            Ok(Predicate::Inside {
                start: iter.next().unwrap(),
                end: iter.next().unwrap(),
            })
        }
        Rule::p_outside => {
            let values = collect_values(inner)?;
            if values.len() != 2 {
                return Err(ParseError::Syntax(
                    "outside() requires 2 values".to_string(),
                ));
            }
            let mut iter = values.into_iter();
            Ok(Predicate::Outside {
                start: iter.next().unwrap(),
                end: iter.next().unwrap(),
            })
        }
        Rule::p_within => {
            let value_list = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::value_list)
                .ok_or_else(|| ParseError::Syntax("within() requires values".to_string()))?;
            let values = collect_values_from_list(value_list)?;
            Ok(Predicate::Within(values))
        }
        Rule::p_without => {
            let value_list = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::value_list)
                .ok_or_else(|| ParseError::Syntax("without() requires values".to_string()))?;
            let values = collect_values_from_list(value_list)?;
            Ok(Predicate::Without(values))
        }
        Rule::p_and => {
            let preds: Vec<_> = inner
                .into_inner()
                .filter(|p| p.as_rule() == Rule::predicate)
                .collect();
            if preds.len() != 2 {
                return Err(ParseError::Syntax(
                    "P.and() requires 2 predicates".to_string(),
                ));
            }
            let mut iter = preds.into_iter();
            Ok(Predicate::And(
                Box::new(build_predicate(iter.next().unwrap())?),
                Box::new(build_predicate(iter.next().unwrap())?),
            ))
        }
        Rule::p_or => {
            let preds: Vec<_> = inner
                .into_inner()
                .filter(|p| p.as_rule() == Rule::predicate)
                .collect();
            if preds.len() != 2 {
                return Err(ParseError::Syntax(
                    "P.or() requires 2 predicates".to_string(),
                ));
            }
            let mut iter = preds.into_iter();
            Ok(Predicate::Or(
                Box::new(build_predicate(iter.next().unwrap())?),
                Box::new(build_predicate(iter.next().unwrap())?),
            ))
        }
        Rule::p_not => {
            let pred = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::predicate)
                .ok_or_else(|| ParseError::Syntax("P.not() requires predicate".to_string()))?;
            Ok(Predicate::Not(Box::new(build_predicate(pred)?)))
        }
        _ => Err(ParseError::Syntax(format!(
            "Unknown P method: {:?}",
            inner.as_rule()
        ))),
    }
}

fn build_text_p_method(pair: Pair<Rule>) -> Result<Predicate, ParseError> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("TextP method empty".to_string()))?;

    let s = extract_string(inner.clone())?;

    match inner.as_rule() {
        Rule::text_containing => Ok(Predicate::Containing(s)),
        Rule::text_not_containing => Ok(Predicate::NotContaining(s)),
        Rule::text_starting_with => Ok(Predicate::StartingWith(s)),
        Rule::text_not_starting_with => Ok(Predicate::NotStartingWith(s)),
        Rule::text_ending_with => Ok(Predicate::EndingWith(s)),
        Rule::text_not_ending_with => Ok(Predicate::NotEndingWith(s)),
        Rule::text_regex => Ok(Predicate::Regex(s)),
        _ => Err(ParseError::Syntax(format!(
            "Unknown TextP method: {:?}",
            inner.as_rule()
        ))),
    }
}

// ============================================================
// Value building helpers
// ============================================================

fn build_value(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    let inner = if pair.as_rule() == Rule::value {
        pair.into_inner()
            .next()
            .ok_or_else(|| ParseError::Syntax("Value empty".to_string()))?
    } else {
        pair
    };

    match inner.as_rule() {
        Rule::string => Ok(Literal::String(parse_string_value(inner)?)),
        Rule::float => Ok(Literal::Float(parse_float(inner)?)),
        Rule::integer => Ok(Literal::Int(parse_integer(inner)?)),
        Rule::boolean => Ok(Literal::Bool(inner.as_str() == "true")),
        Rule::null => Ok(Literal::Null),
        Rule::list_value => {
            let items: Result<Vec<_>, _> = inner
                .into_inner()
                .filter(|p| p.as_rule() == Rule::value)
                .map(build_value)
                .collect();
            Ok(Literal::List(items?))
        }
        Rule::map_value => {
            let entries: Result<Vec<_>, _> = inner
                .into_inner()
                .filter(|p| p.as_rule() == Rule::map_entry)
                .map(|entry| {
                    let mut parts = entry.into_inner();
                    let key_pair = parts
                        .next()
                        .ok_or_else(|| ParseError::Syntax("Map entry key missing".to_string()))?;
                    let key = match key_pair.as_rule() {
                        Rule::string => parse_string_value(key_pair)?,
                        Rule::identifier => key_pair.as_str().to_string(),
                        _ => return Err(ParseError::Syntax("Invalid map key type".to_string())),
                    };
                    let value = build_value(parts.next().ok_or_else(|| {
                        ParseError::Syntax("Map entry value missing".to_string())
                    })?)?;
                    Ok((key, value))
                })
                .collect();
            Ok(Literal::Map(entries?))
        }
        _ => Err(ParseError::Syntax(format!(
            "Unknown value type: {:?}",
            inner.as_rule()
        ))),
    }
}

fn build_value_list_opt(pair: Pair<Rule>) -> Result<Vec<Literal>, ParseError> {
    if let Some(value_list) = pair.into_inner().find(|p| p.as_rule() == Rule::value_list) {
        collect_values_from_list(value_list)
    } else {
        Ok(Vec::new())
    }
}

fn build_label_list_opt(pair: Pair<Rule>) -> Result<Vec<String>, ParseError> {
    if let Some(label_list) = pair.into_inner().find(|p| p.as_rule() == Rule::label_list) {
        collect_strings_from_list(label_list)
    } else {
        Ok(Vec::new())
    }
}

// ============================================================
// Collection helpers
// ============================================================

fn collect_anonymous_traversals(pair: Pair<Rule>) -> Result<Vec<AnonymousTraversal>, ParseError> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::anonymous_traversal)
        .map(build_anonymous_traversal)
        .collect()
}

fn collect_strings(pair: Pair<Rule>) -> Result<Vec<String>, ParseError> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::string)
        .map(parse_string_value)
        .collect()
}

fn collect_strings_from_list(pair: Pair<Rule>) -> Result<Vec<String>, ParseError> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::string)
        .map(parse_string_value)
        .collect()
}

fn collect_values(pair: Pair<Rule>) -> Result<Vec<Literal>, ParseError> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::value)
        .map(build_value)
        .collect()
}

fn collect_values_from_list(pair: Pair<Rule>) -> Result<Vec<Literal>, ParseError> {
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::value)
        .map(build_value)
        .collect()
}

// ============================================================
// Primitive extraction helpers
// ============================================================

fn extract_string(pair: Pair<Rule>) -> Result<String, ParseError> {
    pair.into_inner()
        .find(|p| p.as_rule() == Rule::string)
        .map(parse_string_value)
        .transpose()?
        .ok_or_else(|| ParseError::Syntax("Expected string".to_string()))
}

fn extract_integer(pair: Pair<Rule>) -> Result<i64, ParseError> {
    pair.into_inner()
        .find(|p| p.as_rule() == Rule::integer)
        .map(parse_integer)
        .transpose()?
        .ok_or_else(|| ParseError::Syntax("Expected integer".to_string()))
}

fn extract_float(pair: Pair<Rule>) -> Result<f64, ParseError> {
    pair.into_inner()
        .find(|p| p.as_rule() == Rule::float)
        .map(parse_float)
        .transpose()?
        .ok_or_else(|| ParseError::Syntax("Expected float".to_string()))
}

fn extract_value(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    pair.into_inner()
        .find(|p| p.as_rule() == Rule::value)
        .map(build_value)
        .transpose()?
        .ok_or_else(|| ParseError::Syntax("Expected value".to_string()))
}

fn parse_string_value(pair: Pair<Rule>) -> Result<String, ParseError> {
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| ParseError::Syntax("String literal empty".to_string()))?;

    let content = match inner.as_rule() {
        Rule::single_quoted => {
            let inner_str = inner.into_inner().next().map(|p| p.as_str()).unwrap_or("");
            inner_str.replace("\\'", "'").replace("\\\\", "\\")
        }
        Rule::double_quoted => {
            let inner_str = inner.into_inner().next().map(|p| p.as_str()).unwrap_or("");
            inner_str.replace("\\\"", "\"").replace("\\\\", "\\")
        }
        _ => inner.as_str().to_string(),
    };

    Ok(content)
}

fn parse_integer(pair: Pair<Rule>) -> Result<i64, ParseError> {
    pair.as_str()
        .parse()
        .map_err(|_| ParseError::InvalidLiteral {
            value: pair.as_str().to_string(),
            span: Span::new(pair.as_span().start(), pair.as_span().end()),
            reason: "invalid integer",
        })
}

fn parse_float(pair: Pair<Rule>) -> Result<f64, ParseError> {
    pair.as_str()
        .parse()
        .map_err(|_| ParseError::InvalidLiteral {
            value: pair.as_str().to_string(),
            span: Span::new(pair.as_span().start(), pair.as_span().end()),
            reason: "invalid float",
        })
}

fn parse_order_direction(pair: &Pair<Rule>) -> Result<OrderDirection, ParseError> {
    match pair.as_str() {
        "asc" | "Order.asc" => Ok(OrderDirection::Asc),
        "desc" | "Order.desc" => Ok(OrderDirection::Desc),
        "shuffle" | "Order.shuffle" => Ok(OrderDirection::Shuffle),
        _ => Err(ParseError::Syntax(format!(
            "Unknown order direction: {}",
            pair.as_str()
        ))),
    }
}

fn parse_cardinality(pair: &Pair<Rule>) -> Result<Cardinality, ParseError> {
    match pair.as_str() {
        "single" | "Cardinality.single" => Ok(Cardinality::Single),
        "list" | "Cardinality.list" => Ok(Cardinality::List),
        "set" | "Cardinality.set" => Ok(Cardinality::Set),
        _ => Err(ParseError::Syntax(format!(
            "Unknown cardinality: {}",
            pair.as_str()
        ))),
    }
}
