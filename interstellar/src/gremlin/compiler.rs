//! Compiler for Gremlin AST to Interstellar traversals.
//!
//! This module transforms parsed Gremlin AST into executable traversal pipelines.

use std::collections::{HashMap, HashSet};

use crate::gremlin::ast::*;
use crate::gremlin::error::CompileError;
use crate::traversal::{p, BoundTraversal, GraphTraversalSource, Traversal, __};
use crate::value::{EdgeId, Value, VertexId};

/// Result of executing a compiled traversal.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionResult {
    /// List of values
    List(Vec<Value>),
    /// Single value (from next())
    Single(Option<Value>),
    /// Set of unique values
    Set(HashSet<Value>),
    /// Boolean result (from hasNext())
    Bool(bool),
    /// No result (from iterate())
    Unit,
    /// Explanation string (from explain())
    Explain(String),
}

/// A compiled traversal ready for execution.
pub struct CompiledTraversal<'g> {
    /// The bound traversal pipeline
    pub(crate) traversal: BoundTraversal<'g, (), Value>,
    /// Terminal step information
    terminal: Option<TerminalStep>,
}

impl<'g> CompiledTraversal<'g> {
    /// Execute the traversal and return results.
    pub fn execute(self) -> ExecutionResult {
        match self.terminal {
            None | Some(TerminalStep::ToList { .. }) => {
                ExecutionResult::List(self.traversal.to_list())
            }
            Some(TerminalStep::Next { count: None, .. }) => {
                ExecutionResult::Single(self.traversal.next())
            }
            Some(TerminalStep::Next { count: Some(n), .. }) => {
                ExecutionResult::List(self.traversal.take(n as usize))
            }
            Some(TerminalStep::ToSet { .. }) => ExecutionResult::Set(self.traversal.to_set()),
            Some(TerminalStep::Iterate { .. }) => {
                self.traversal.iterate();
                ExecutionResult::Unit
            }
            Some(TerminalStep::HasNext { .. }) => ExecutionResult::Bool(self.traversal.has_next()),
            Some(TerminalStep::Explain { .. }) => {
                let explanation = self.traversal.explain();
                ExecutionResult::Explain(explanation.to_string())
            }
        }
    }

    /// Get the terminal step type (if any).
    pub fn terminal(&self) -> Option<&TerminalStep> {
        self.terminal.as_ref()
    }
}

/// Compile a Gremlin AST into an executable traversal.
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::gremlin::{parse, compile};
/// use interstellar::prelude::*;
///
/// let graph = Graph::new();
/// let snapshot = graph.snapshot();
/// let g = snapshot.gremlin();
///
/// let ast = parse("g.V().hasLabel('person').values('name')")?;
/// let compiled = compile(&ast, &g)?;
/// let results = compiled.execute();
/// ```
pub fn compile<'g>(
    ast: &GremlinTraversal,
    g: &GraphTraversalSource<'g>,
) -> Result<CompiledTraversal<'g>, CompileError> {
    // Validate the AST structure first
    validate_ast(ast)?;

    // Handle addE as source specially - it needs to look ahead for from/to/property
    if let SourceStep::AddE { label, .. } = &ast.source {
        let (traversal, consumed) = compile_add_e_source(g, label, &ast.steps)?;
        // Compile remaining steps
        let remaining = &ast.steps[consumed..];
        let traversal = compile_steps(remaining, traversal)?;
        return Ok(CompiledTraversal {
            traversal,
            terminal: ast.terminal.clone(),
        });
    }

    // Compile source step to get initial traversal
    let mut traversal = compile_source(&ast.source, g)?;

    // Compile each step and append to traversal
    traversal = compile_steps(&ast.steps, traversal)?;

    Ok(CompiledTraversal {
        traversal,
        terminal: ast.terminal.clone(),
    })
}

/// Compile the source step to create initial traversal.
fn compile_source<'g>(
    source: &SourceStep,
    g: &GraphTraversalSource<'g>,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    match source {
        SourceStep::V { ids, .. } => {
            if ids.is_empty() {
                Ok(g.v())
            } else {
                let vertex_ids: Vec<VertexId> = ids
                    .iter()
                    .map(|lit| match lit {
                        Literal::Int(n) => VertexId(*n as u64),
                        Literal::String(s) => {
                            // Try parsing string as u64
                            s.parse::<u64>().map(VertexId).unwrap_or(VertexId(0))
                            // Fallback - won't match
                        }
                        _ => VertexId(0),
                    })
                    .collect();
                Ok(g.v_ids(vertex_ids))
            }
        }
        SourceStep::E { ids, .. } => {
            if ids.is_empty() {
                Ok(g.e())
            } else {
                let edge_ids: Vec<EdgeId> = ids
                    .iter()
                    .map(|lit| match lit {
                        Literal::Int(n) => EdgeId(*n as u64),
                        Literal::String(s) => s.parse::<u64>().map(EdgeId).unwrap_or(EdgeId(0)),
                        _ => EdgeId(0),
                    })
                    .collect();
                Ok(g.e_ids(edge_ids))
            }
        }
        SourceStep::AddV { label, .. } => Ok(g.add_v(label)),
        SourceStep::AddE { label: _, .. } => {
            // addE as source requires from/to to be specified later
            // For now, start with a placeholder that will need from/to
            Err(CompileError::UnsupportedStep {
                step: "addE as source (use g.V().addE() pattern instead)".to_string(),
            })
        }
        SourceStep::Inject { values, .. } => {
            let vals: Vec<Value> = values.iter().map(literal_to_value).collect();
            Ok(g.inject(vals))
        }
        SourceStep::SearchTextV {
            property, query, k, ..
        } => compile_search_text_v(g, property, query, *k),
        SourceStep::SearchTextE {
            property, query, k, ..
        } => compile_search_text_e(g, property, query, *k),
    }
}

/// Compile `g.searchTextV(prop, query, k)` into a `GraphTraversalSource`
/// FTS source step. Score is propagated via the traverser sack and read
/// back via [`Step::TextScore`] (`__.textScore()`). (spec-55c Layer 4)
#[cfg(feature = "full-text")]
fn compile_search_text_v<'g>(
    g: &GraphTraversalSource<'g>,
    property: &str,
    query: &crate::gremlin::ast::TextQueryAst,
    k: u64,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let runtime = text_query_ast_to_runtime(query);
    g.search_text_query(property, &runtime, k as usize)
        .map_err(|e| CompileError::UnsupportedStep {
            step: format!("searchTextV: {e}"),
        })
}

/// `searchTextV` requires the `full-text` feature; without it we surface a
/// clear, actionable compile error.
#[cfg(not(feature = "full-text"))]
fn compile_search_text_v<'g>(
    _g: &GraphTraversalSource<'g>,
    _property: &str,
    _query: &crate::gremlin::ast::TextQueryAst,
    _k: u64,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    Err(CompileError::UnsupportedStep {
        step: "searchTextV (requires the `full-text` feature)".to_string(),
    })
}

/// Compile `g.searchTextE(prop, query, k)` for edge full-text search.
/// (spec-55c Layer 4)
#[cfg(feature = "full-text")]
fn compile_search_text_e<'g>(
    g: &GraphTraversalSource<'g>,
    property: &str,
    query: &crate::gremlin::ast::TextQueryAst,
    k: u64,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let runtime = text_query_ast_to_runtime(query);
    g.search_text_query_e(property, &runtime, k as usize)
        .map_err(|e| CompileError::UnsupportedStep {
            step: format!("searchTextE: {e}"),
        })
}

/// Edge variant of the no-feature stub.
#[cfg(not(feature = "full-text"))]
fn compile_search_text_e<'g>(
    _g: &GraphTraversalSource<'g>,
    _property: &str,
    _query: &crate::gremlin::ast::TextQueryAst,
    _k: u64,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    Err(CompileError::UnsupportedStep {
        step: "searchTextE (requires the `full-text` feature)".to_string(),
    })
}

/// Convert the parsed [`TextQueryAst`] into the runtime
/// `interstellar::storage::text::TextQuery` consumed by the FTS engine.
#[cfg(feature = "full-text")]
fn text_query_ast_to_runtime(
    ast: &crate::gremlin::ast::TextQueryAst,
) -> crate::storage::text::TextQuery {
    use crate::gremlin::ast::TextQueryAst;
    use crate::storage::text::TextQuery;
    match ast {
        TextQueryAst::Match(s) => TextQuery::Match(s.clone()),
        TextQueryAst::MatchAll(s) => TextQuery::MatchAll(s.clone()),
        TextQueryAst::Phrase(s) => TextQuery::Phrase {
            text: s.clone(),
            slop: 0,
        },
        TextQueryAst::Prefix(s) => TextQuery::Prefix(s.clone()),
        TextQueryAst::And(children) => {
            TextQuery::And(children.iter().map(text_query_ast_to_runtime).collect())
        }
        TextQueryAst::Or(children) => {
            TextQuery::Or(children.iter().map(text_query_ast_to_runtime).collect())
        }
        TextQueryAst::Not(inner) => TextQuery::Not(Box::new(text_query_ast_to_runtime(inner))),
    }
}

/// Compile addE as a source step with from/to/property lookahead.
///
/// Returns the traversal and the number of steps consumed.
fn compile_add_e_source<'g>(
    g: &GraphTraversalSource<'g>,
    label: &str,
    steps: &[Step],
) -> Result<(BoundTraversal<'g, (), Value>, usize), CompileError> {
    let mut from_args = None;
    let mut to_args = None;
    let mut properties = Vec::new();
    let mut consumed = 0;

    // Look ahead for from(), to(), and property() steps
    for step in steps {
        match step {
            Step::From { args, .. } => {
                from_args = Some(args.clone());
                consumed += 1;
            }
            Step::To { args, .. } => {
                to_args = Some(args.clone());
                consumed += 1;
            }
            Step::Property { args, .. } => {
                properties.push(args.clone());
                consumed += 1;
            }
            _ => break,
        }
    }

    // Build the edge using the source builder
    let mut builder = g.add_e(label);

    // Set from endpoint
    if let Some(from) = from_args {
        builder = match from {
            FromToArgs::Label(label) => builder.from_label(label),
            FromToArgs::Id(lit) => {
                let id = match lit {
                    Literal::Int(i) => VertexId(i as u64),
                    Literal::String(s) => s.parse::<u64>().map(VertexId).map_err(|_| {
                        CompileError::InvalidArguments {
                            step: "from".to_string(),
                            message: format!("Invalid vertex ID: {}", s),
                        }
                    })?,
                    _ => {
                        return Err(CompileError::InvalidArguments {
                            step: "from".to_string(),
                            message: "from() requires a label string or vertex ID".to_string(),
                        });
                    }
                };
                builder.from_vertex(id)
            }
            FromToArgs::Traversal(_) => {
                return Err(CompileError::UnsupportedStep {
                    step: "from(__.traversal) - use from('label') with as() instead".to_string(),
                });
            }
            FromToArgs::Variable(_) => {
                // Variables require compile_with_vars, not basic compile
                return Err(CompileError::UnsupportedStep {
                    step: "from(variable) - use execute_script() for variable support".to_string(),
                });
            }
        };
    }

    // Set to endpoint
    if let Some(to) = to_args {
        builder = match to {
            FromToArgs::Label(label) => builder.to_label(label),
            FromToArgs::Id(lit) => {
                let id = match lit {
                    Literal::Int(i) => VertexId(i as u64),
                    Literal::String(s) => s.parse::<u64>().map(VertexId).map_err(|_| {
                        CompileError::InvalidArguments {
                            step: "to".to_string(),
                            message: format!("Invalid vertex ID: {}", s),
                        }
                    })?,
                    _ => {
                        return Err(CompileError::InvalidArguments {
                            step: "to".to_string(),
                            message: "to() requires a label string or vertex ID".to_string(),
                        });
                    }
                };
                builder.to_vertex(id)
            }
            FromToArgs::Traversal(_) => {
                return Err(CompileError::UnsupportedStep {
                    step: "to(__.traversal) - use to('label') with as() instead".to_string(),
                });
            }
            FromToArgs::Variable(_) => {
                // Variables require compile_with_vars, not basic compile
                return Err(CompileError::UnsupportedStep {
                    step: "to(variable) - use execute_script() for variable support".to_string(),
                });
            }
        };
    }

    // Add properties
    for prop in properties {
        let val = literal_to_value(&prop.value);
        builder = builder.property(&prop.key, val);
    }

    Ok((builder.build(), consumed))
}

/// Compile a sequence of steps onto a traversal.
fn compile_steps<'g>(
    steps: &[Step],
    mut traversal: BoundTraversal<'g, (), Value>,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let mut i = 0;
    while i < steps.len() {
        let step = &steps[i];

        // Handle special cases where steps combine (e.g., order().by(), repeat().times())
        match step {
            Step::Order { .. } => {
                // Look ahead for by() steps
                let mut by_steps = Vec::new();
                let mut j = i + 1;
                while j < steps.len() {
                    if let Step::By { args, .. } = &steps[j] {
                        by_steps.push(args.clone());
                        j += 1;
                    } else {
                        break;
                    }
                }
                traversal = compile_order_with_by(traversal, &by_steps)?;
                i = j;
                continue;
            }
            Step::Project { keys, .. } => {
                // Look ahead for by() steps
                let mut by_steps = Vec::new();
                let mut j = i + 1;
                while j < steps.len() && by_steps.len() < keys.len() {
                    if let Step::By { args, .. } = &steps[j] {
                        by_steps.push(args.clone());
                        j += 1;
                    } else {
                        break;
                    }
                }
                traversal = compile_project_with_by(traversal, keys, &by_steps)?;
                i = j;
                continue;
            }
            Step::Group { .. } => {
                // Look ahead for by() steps (up to 2: key and value)
                let mut by_steps = Vec::new();
                let mut j = i + 1;
                while j < steps.len() && by_steps.len() < 2 {
                    if let Step::By { args, .. } = &steps[j] {
                        by_steps.push(args.clone());
                        j += 1;
                    } else {
                        break;
                    }
                }
                traversal = compile_group_with_by(traversal, &by_steps)?;
                i = j;
                continue;
            }
            Step::GroupCount { .. } => {
                // Look ahead for by() step (only key)
                let mut by_steps = Vec::new();
                let mut j = i + 1;
                while j < steps.len() && by_steps.is_empty() {
                    if let Step::By { args, .. } = &steps[j] {
                        by_steps.push(args.clone());
                        j += 1;
                    } else {
                        break;
                    }
                }
                traversal = compile_group_count_with_by(traversal, &by_steps)?;
                i = j;
                continue;
            }
            Step::Repeat { traversal: sub, .. } => {
                // Look ahead for times(), until(), emit()
                let mut times_count = None;
                let mut until_trav = None;
                let mut emit_trav: Option<Option<Box<AnonymousTraversal>>> = None;
                let mut j = i + 1;
                while j < steps.len() {
                    match &steps[j] {
                        Step::Times { count, .. } => {
                            times_count = Some(*count);
                            j += 1;
                        }
                        Step::Until { traversal: ut, .. } => {
                            until_trav = Some(ut.clone());
                            j += 1;
                        }
                        Step::Emit { traversal: et, .. } => {
                            emit_trav = Some(et.clone());
                            j += 1;
                        }
                        _ => break,
                    }
                }
                traversal = compile_repeat(traversal, sub, times_count, until_trav, emit_trav)?;
                i = j;
                continue;
            }
            #[cfg(feature = "gql")]
            Step::Math { expression, .. } => {
                // Look ahead for by() steps (variable bindings)
                let mut by_steps = Vec::new();
                let mut j = i + 1;
                while j < steps.len() {
                    if let Step::By { args, .. } = &steps[j] {
                        by_steps.push(args.clone());
                        j += 1;
                    } else {
                        break;
                    }
                }
                traversal = compile_math_with_by(traversal, expression, &by_steps)?;
                i = j;
                continue;
            }
            #[cfg(not(feature = "gql"))]
            Step::Math { .. } => {
                return Err(CompileError::UnsupportedStep {
                    step: "math (requires 'gql' feature)".to_string(),
                });
            }
            Step::AddE { label, .. } => {
                // Look ahead for from(), to(), and property() steps
                let mut from_args = None;
                let mut to_args = None;
                let mut properties = Vec::new();
                let mut j = i + 1;
                while j < steps.len() {
                    match &steps[j] {
                        Step::From { args, .. } => {
                            from_args = Some(args.clone());
                            j += 1;
                        }
                        Step::To { args, .. } => {
                            to_args = Some(args.clone());
                            j += 1;
                        }
                        Step::Property { args, .. } => {
                            properties.push(args.clone());
                            j += 1;
                        }
                        _ => break,
                    }
                }
                traversal = compile_add_e_with_endpoints(
                    traversal,
                    label,
                    from_args,
                    to_args,
                    &properties,
                )?;
                i = j;
                continue;
            }
            Step::By { .. } => {
                // by() should have been consumed by order/project/math/etc.
                // If we get here, it's orphaned
                return Err(CompileError::MissingPrecedingStep {
                    step: "by".to_string(),
                    required: "order, project, group, math".to_string(),
                });
            }
            Step::From { .. } => {
                // from() should have been consumed by addE
                return Err(CompileError::MissingPrecedingStep {
                    step: "from".to_string(),
                    required: "addE".to_string(),
                });
            }
            Step::To { .. } => {
                // to() should have been consumed by addE
                return Err(CompileError::MissingPrecedingStep {
                    step: "to".to_string(),
                    required: "addE".to_string(),
                });
            }
            Step::Times { .. } | Step::Until { .. } | Step::Emit { .. } => {
                // Should have been consumed by repeat
                return Err(CompileError::MissingPrecedingStep {
                    step: step.name().to_string(),
                    required: "repeat".to_string(),
                });
            }
            _ => {
                traversal = compile_step(step, traversal)?;
                i += 1;
            }
        }
    }

    Ok(traversal)
}

/// Compile a single step.
fn compile_step<'g>(
    step: &Step,
    traversal: BoundTraversal<'g, (), Value>,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    match step {
        // Navigation - Vertex to Vertex
        Step::Out { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.out())
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.out_labels(&labels_ref))
            }
        }
        Step::In { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.in_())
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.in_labels(&labels_ref))
            }
        }
        Step::Both { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.both())
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.both_labels(&labels_ref))
            }
        }

        // Navigation - Vertex to Edge
        Step::OutE { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.out_e())
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.out_e_labels(&labels_ref))
            }
        }
        Step::InE { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.in_e())
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.in_e_labels(&labels_ref))
            }
        }
        Step::BothE { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.both_e())
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.both_e_labels(&labels_ref))
            }
        }

        // Navigation - Edge to Vertex
        Step::OutV { .. } => Ok(traversal.out_v()),
        Step::InV { .. } => Ok(traversal.in_v()),
        Step::BothV { .. } => Ok(traversal.both_v()),
        Step::OtherV { .. } => Ok(traversal.other_v()),

        // Filter - Has
        Step::Has { args, .. } => compile_has(traversal, args),
        Step::HasLabel { labels, .. } => {
            if labels.len() == 1 {
                Ok(traversal.has_label(&labels[0]))
            } else {
                Ok(traversal.has_label_any(labels.clone()))
            }
        }
        Step::HasId { ids, .. } => {
            if ids.len() == 1 {
                let id = match &ids[0] {
                    Literal::Int(n) => VertexId(*n as u64),
                    _ => {
                        return Err(CompileError::TypeMismatch {
                            message: "hasId requires integer ID".to_string(),
                        })
                    }
                };
                Ok(traversal.has_id(id))
            } else {
                let vertex_ids: Result<Vec<_>, _> = ids
                    .iter()
                    .map(|lit| match lit {
                        Literal::Int(n) => Ok(VertexId(*n as u64)),
                        _ => Err(CompileError::TypeMismatch {
                            message: "hasId requires integer IDs".to_string(),
                        }),
                    })
                    .collect();
                Ok(traversal.has_ids(vertex_ids?))
            }
        }
        Step::HasNot { key, .. } => Ok(traversal.has_not(key)),
        Step::HasKey { keys, .. } => {
            // hasKey filters property maps (from properties()) by key name
            // Multiple keys means "has ANY of these keys" (OR semantics)
            if keys.len() == 1 {
                Ok(traversal.has_key(&keys[0]))
            } else {
                Ok(traversal.has_key_any(keys.clone()))
            }
        }
        Step::HasValue { values, .. } => {
            // hasValue filters property maps (from properties()) by their value
            // Multiple values means "has ANY of these values" (OR semantics)
            if values.len() == 1 {
                let val = literal_to_value(&values[0]);
                Ok(traversal.has_prop_value(val))
            } else {
                let vals: Vec<Value> = values.iter().map(literal_to_value).collect();
                Ok(traversal.has_prop_value_any(vals))
            }
        }

        // Filter - Where/Is
        Step::Where { args, .. } => match args {
            WhereArgs::Traversal(sub) => {
                let anon = compile_anonymous_traversal(sub)?;
                Ok(traversal.where_(anon))
            }
            WhereArgs::Predicate(pred) => {
                let p = compile_predicate(pred)?;
                Ok(traversal.where_p(p))
            }
        },
        Step::Is { args, .. } => match args {
            IsArgs::Value(lit) => {
                let val = literal_to_value(lit);
                Ok(traversal.is_eq(val))
            }
            IsArgs::Predicate(pred) => {
                let p = compile_predicate(pred)?;
                Ok(traversal.is_(p))
            }
        },

        // Filter - Boolean
        Step::And { traversals, .. } => {
            let anons: Result<Vec<_>, _> =
                traversals.iter().map(compile_anonymous_traversal).collect();
            Ok(traversal.and_(anons?))
        }
        Step::Or { traversals, .. } => {
            let anons: Result<Vec<_>, _> =
                traversals.iter().map(compile_anonymous_traversal).collect();
            Ok(traversal.or_(anons?))
        }
        Step::Not { traversal: sub, .. } => {
            let anon = compile_anonymous_traversal(sub)?;
            Ok(traversal.not(anon))
        }

        // Filter - Limiting
        Step::Dedup { by_label, .. } => {
            if let Some(label) = by_label {
                Ok(traversal.dedup_by_key(label))
            } else {
                Ok(traversal.dedup())
            }
        }
        Step::Limit { count, .. } => Ok(traversal.limit(*count as usize)),
        Step::Skip { count, .. } => Ok(traversal.skip(*count as usize)),
        Step::Range { start, end, .. } => Ok(traversal.range(*start as usize, *end as usize)),
        Step::Tail { count, .. } => {
            if let Some(n) = count {
                Ok(traversal.tail_n(*n as usize))
            } else {
                Ok(traversal.tail())
            }
        }
        Step::Coin { probability, .. } => Ok(traversal.coin(*probability)),
        Step::Sample { count, .. } => Ok(traversal.sample(*count as usize)),
        Step::SimplePath { .. } => Ok(traversal.simple_path()),
        Step::CyclicPath { .. } => Ok(traversal.cyclic_path()),

        // Transform - Property Access
        Step::Values { keys, .. } => {
            if keys.is_empty() {
                // values() with no args - get all property values
                Ok(traversal.values_multi(Vec::<String>::new()))
            } else if keys.len() == 1 {
                Ok(traversal.values(&keys[0]))
            } else {
                Ok(traversal.values_multi(keys.clone()))
            }
        }
        Step::Properties { keys, .. } => {
            if keys.is_empty() {
                Ok(traversal.properties())
            } else {
                Ok(traversal.properties_keys(keys.clone()))
            }
        }
        Step::ValueMap { args, .. } => {
            if args.include_tokens {
                Ok(traversal.value_map_with_tokens())
            } else if args.keys.is_empty() {
                Ok(traversal.value_map())
            } else {
                // value_map with specific keys - use value_map then filter
                // For now, just use value_map (full implementation would filter)
                Ok(traversal.value_map())
            }
        }
        Step::ElementMap { .. } => Ok(traversal.element_map()),
        Step::PropertyMap { .. } => Ok(traversal.property_map()),
        Step::Id { .. } => Ok(traversal.id()),
        Step::Label { .. } => Ok(traversal.label()),
        Step::Key { .. } => Ok(traversal.key()),
        Step::Value { .. } => Ok(traversal.value()),
        Step::Path { .. } => Ok(traversal.path()),

        // Transform - Selection
        Step::Select { labels, .. } => {
            if labels.len() == 1 {
                Ok(traversal.select_one(&labels[0]))
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.select(&labels_ref))
            }
        }
        Step::As { label, .. } => Ok(traversal.as_(label)),

        // Transform - Collection
        Step::Unfold { .. } => Ok(traversal.unfold()),
        Step::Fold { .. } => Ok(traversal.fold_step()),

        // Transform - Aggregation (barrier steps that return traversals)
        Step::Count { .. } => Ok(traversal.count_step()),
        Step::Sum { .. } => Ok(traversal.sum_step()),
        Step::Max { .. } => Ok(traversal.max_step()),
        Step::Min { .. } => Ok(traversal.min_step()),
        Step::Mean { .. } => Ok(traversal.mean()),

        // Transform - Misc
        Step::Constant { value, .. } => {
            let val = literal_to_value(value);
            Ok(traversal.constant(val))
        }
        Step::Identity { .. } => {
            // identity() is a no-op - just returns the traversal unchanged
            Ok(traversal)
        }
        Step::Index { .. } => Ok(traversal.index()),
        Step::Loops { .. } => Ok(traversal.loops()),
        // Math is handled by lookahead in compile_steps - this should not be reached
        Step::Math { .. } => unreachable!("math() should be handled by lookahead"),

        // Branch
        Step::Union { traversals, .. } => {
            let anons: Result<Vec<_>, _> =
                traversals.iter().map(compile_anonymous_traversal).collect();
            Ok(traversal.union(anons?))
        }
        Step::Coalesce { traversals, .. } => {
            let anons: Result<Vec<_>, _> =
                traversals.iter().map(compile_anonymous_traversal).collect();
            Ok(traversal.coalesce(anons?))
        }
        Step::Choose { args, .. } => compile_choose(traversal, args),
        Step::Optional { traversal: sub, .. } => {
            let anon = compile_anonymous_traversal(sub)?;
            Ok(traversal.optional(anon))
        }
        Step::Local { traversal: sub, .. } => {
            let anon = compile_anonymous_traversal(sub)?;
            Ok(traversal.local(anon))
        }
        Step::Branch { .. } => Err(CompileError::UnsupportedStep {
            step: "branch (use choose instead)".to_string(),
        }),
        Step::Option { .. } => Err(CompileError::MissingPrecedingStep {
            step: "option".to_string(),
            required: "choose or branch".to_string(),
        }),

        // Side Effect
        Step::Aggregate { key, .. } => Ok(traversal.aggregate(key)),
        Step::Store { key, .. } => Ok(traversal.store(key)),
        Step::Cap { keys, .. } => {
            if keys.len() == 1 {
                Ok(traversal.cap(&keys[0]))
            } else {
                Ok(traversal.cap_multi(keys.clone()))
            }
        }
        Step::SideEffect { traversal: sub, .. } => {
            let anon = compile_anonymous_traversal(sub)?;
            Ok(traversal.side_effect(anon))
        }
        Step::Profile { .. } => Ok(traversal.profile()),

        // Mutation
        Step::AddV { label, .. } => {
            let anon = __.add_v(label);
            Ok(traversal.append(anon))
        }
        // Property can appear standalone (after addV) or after addE (handled by lookahead)
        Step::Property { args, .. } => {
            let val = literal_to_value(&args.value);
            Ok(traversal.property(&args.key, val))
        }
        Step::Drop { .. } => Ok(traversal.drop()),

        #[cfg(feature = "full-text")]
        Step::TextScore { .. } => Ok(traversal.text_score()),
        #[cfg(not(feature = "full-text"))]
        Step::TextScore { .. } => Err(CompileError::UnsupportedStep {
            step: "textScore (requires the `full-text` feature)".to_string(),
        }),

        // These should be handled by compile_steps lookahead
        Step::Order { .. }
        | Step::Project { .. }
        | Step::Group { .. }
        | Step::GroupCount { .. }
        | Step::Repeat { .. }
        | Step::Times { .. }
        | Step::Until { .. }
        | Step::Emit { .. }
        | Step::By { .. }
        | Step::AddE { .. }
        | Step::From { .. }
        | Step::To { .. } => {
            unreachable!("Should be handled by compile_steps lookahead")
        }
    }
}

/// Compile has() step with its various argument forms.
fn compile_has<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    args: &HasArgs,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    match args {
        HasArgs::Key(key) => Ok(traversal.has(key)),
        HasArgs::KeyValue { key, value } => {
            let val = literal_to_value(value);
            Ok(traversal.has_value(key, val))
        }
        HasArgs::KeyPredicate { key, predicate } => {
            let p = compile_predicate(predicate)?;
            Ok(traversal.has_where(key, p))
        }
        HasArgs::LabelKeyValue { label, key, value } => {
            let val = literal_to_value(value);
            // has(label, key, value) = hasLabel(label).has(key, value)
            Ok(traversal.has_label(label).has_value(key, val))
        }
    }
}

/// Compile choose() step with its various argument forms.
fn compile_choose<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    args: &ChooseArgs,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    match args {
        ChooseArgs::IfThenElse {
            condition,
            if_true,
            if_false,
        } => {
            let cond = compile_anonymous_traversal(condition)?;
            let then_t = compile_anonymous_traversal(if_true)?;
            let else_t = compile_anonymous_traversal(if_false)?;
            Ok(traversal.choose(cond, then_t, else_t))
        }
        ChooseArgs::ByTraversal(_) => Err(CompileError::UnsupportedStep {
            step: "choose with branch selector (use if-then-else form)".to_string(),
        }),
        ChooseArgs::ByPredicate(_) => Err(CompileError::UnsupportedStep {
            step: "choose with predicate (use if-then-else form)".to_string(),
        }),
    }
}

/// Compile order() with following by() steps.
fn compile_order_with_by<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    by_steps: &[ByArgs],
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let mut builder = traversal.order();

    for by in by_steps {
        builder = match by {
            ByArgs::Identity => builder.by_asc(),
            ByArgs::Key(k) => builder.by_key_asc(k),
            ByArgs::Order(OrderDirection::Asc) => builder.by_asc(),
            ByArgs::Order(OrderDirection::Desc) => builder.by_desc(),
            ByArgs::Order(OrderDirection::Shuffle) => {
                // Shuffle not directly supported, use desc as fallback
                builder.by_desc()
            }
            ByArgs::KeyOrder { key, order } => match order {
                OrderDirection::Asc => builder.by_key_asc(key),
                OrderDirection::Desc => builder.by_key_desc(key),
                OrderDirection::Shuffle => builder.by_desc(),
            },
            ByArgs::Traversal(sub) => {
                let anon = compile_anonymous_traversal(sub)?;
                builder.by_traversal(anon, false)
            }
            ByArgs::TraversalOrder {
                traversal: sub,
                order,
            } => {
                let anon = compile_anonymous_traversal(sub)?;
                let desc = matches!(order, OrderDirection::Desc);
                builder.by_traversal(anon, desc)
            }
        };
    }

    // If no by() steps, default to ascending by value
    if by_steps.is_empty() {
        builder = builder.by_asc();
    }

    Ok(builder.build())
}

/// Compile project() with following by() steps.
fn compile_project_with_by<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    keys: &[String],
    by_steps: &[ByArgs],
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let keys_ref: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    let mut builder = traversal.project(&keys_ref);

    for by in by_steps {
        builder = match by {
            ByArgs::Identity => builder.by(__.identity()),
            ByArgs::Key(k) => builder.by_key(k),
            ByArgs::Traversal(sub) => {
                let anon = compile_anonymous_traversal(sub)?;
                builder.by(anon)
            }
            _ => {
                return Err(CompileError::InvalidArguments {
                    step: "project".to_string(),
                    message: "project().by() only supports key or traversal".to_string(),
                });
            }
        };
    }

    Ok(builder.build())
}

/// Compile group() with following by() steps.
///
/// In Gremlin, `group().by('key').by('value')` groups by 'key' and collects 'value'.
/// - First by() specifies the key selector
/// - Second by() specifies the value collector
fn compile_group_with_by<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    by_steps: &[ByArgs],
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let mut builder = traversal.group();

    // First by() is the key selector
    if let Some(by) = by_steps.first() {
        builder = match by {
            ByArgs::Identity => builder.by_label(), // Default to label when identity
            ByArgs::Key(k) => builder.by_key(k),
            ByArgs::Traversal(sub) => {
                let anon = compile_anonymous_traversal(sub)?;
                builder.by_traversal(anon)
            }
            _ => {
                return Err(CompileError::InvalidArguments {
                    step: "group".to_string(),
                    message: "group().by() key selector only supports key or traversal".to_string(),
                });
            }
        };
    }

    // Second by() is the value collector
    if let Some(by) = by_steps.get(1) {
        builder = match by {
            ByArgs::Identity => builder.by_value(),
            ByArgs::Key(k) => builder.by_value_key(k),
            ByArgs::Traversal(sub) => {
                let anon = compile_anonymous_traversal(sub)?;
                // Check if the traversal ends with a reducing step (count, sum, etc.)
                // If so, use by_value_fold for correct semantics
                if is_reducing_traversal(sub) {
                    builder.by_value_fold(anon)
                } else {
                    builder.by_value_traversal(anon)
                }
            }
            _ => {
                return Err(CompileError::InvalidArguments {
                    step: "group".to_string(),
                    message: "group().by() value collector only supports key or traversal"
                        .to_string(),
                });
            }
        };
    }

    Ok(builder.build())
}

/// Check if a traversal ends with a reducing step (count, sum, min, max, fold, dedup).
fn is_reducing_traversal(trav: &AnonymousTraversal) -> bool {
    if let Some(last_step) = trav.steps.last() {
        matches!(
            last_step,
            Step::Count { .. }
                | Step::Sum { .. }
                | Step::Min { .. }
                | Step::Max { .. }
                | Step::Fold { .. }
                | Step::Dedup { .. }
        )
    } else {
        false
    }
}

/// Compile groupCount() with following by() step.
///
/// In Gremlin, `groupCount().by('key')` counts occurrences grouped by 'key'.
fn compile_group_count_with_by<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    by_steps: &[ByArgs],
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let mut builder = traversal.group_count();

    // by() specifies the key selector
    if let Some(by) = by_steps.first() {
        builder = match by {
            ByArgs::Identity => builder.by_label(), // Default to label when identity
            ByArgs::Key(k) => builder.by_key(k),
            ByArgs::Traversal(sub) => {
                let anon = compile_anonymous_traversal(sub)?;
                builder.by_traversal(anon)
            }
            _ => {
                return Err(CompileError::InvalidArguments {
                    step: "groupCount".to_string(),
                    message: "groupCount().by() only supports key or traversal".to_string(),
                });
            }
        };
    }

    Ok(builder.build())
}

/// Compile math() with following by() steps for variable bindings.
///
/// In Gremlin, `math('a + b').by('a').by('b')` binds variable 'a' to property 'a'
/// and variable 'b' to property 'b'. The by() takes a single key that serves as
/// both the variable name and property key.
#[cfg(feature = "gql")]
fn compile_math_with_by<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    expression: &str,
    by_steps: &[ByArgs],
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let mut builder = traversal.math(expression);

    for by in by_steps {
        match by {
            ByArgs::Key(k) => {
                // In Gremlin, by('x') after math() means: variable 'x' maps to property 'x'
                builder = builder.by(k, k);
            }
            ByArgs::Identity => {
                // by() with no args - skip, math handles _ for current value automatically
            }
            _ => {
                return Err(CompileError::InvalidArguments {
                    step: "math".to_string(),
                    message: "math().by() only supports key names (e.g., by('x'))".to_string(),
                });
            }
        }
    }

    Ok(builder.build())
}

/// Compile addE() with following from(), to(), and property() steps.
///
/// Handles patterns like:
/// - `addE('knows').from('a').to('b')`
/// - `addE('knows').from(__.select('a')).to(__.select('b'))`
/// - `addE('knows').to('b').property('since', 2020)`
fn compile_add_e_with_endpoints<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    label: &str,
    from_args: Option<FromToArgs>,
    to_args: Option<FromToArgs>,
    properties: &[PropertyArgs],
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let mut builder = traversal.add_e(label);

    // Set from endpoint
    if let Some(from) = from_args {
        builder = match from {
            FromToArgs::Label(label) => builder.from_label(label),
            FromToArgs::Id(lit) => {
                let id = match lit {
                    Literal::Int(i) => VertexId(i as u64),
                    Literal::String(s) => s.parse::<u64>().map(VertexId).map_err(|_| {
                        CompileError::InvalidArguments {
                            step: "from".to_string(),
                            message: format!("Invalid vertex ID: {}", s),
                        }
                    })?,
                    _ => {
                        return Err(CompileError::InvalidArguments {
                            step: "from".to_string(),
                            message: "from() requires a label string or vertex ID".to_string(),
                        });
                    }
                };
                builder.from_vertex(id)
            }
            FromToArgs::Traversal(_) => {
                // Traversal-based from (e.g., from(__.select('a'))) requires more complex handling
                // For now, we only support label-based references
                return Err(CompileError::UnsupportedStep {
                    step: "from(__.traversal) - use from('label') with as() instead".to_string(),
                });
            }
            FromToArgs::Variable(_) => {
                // Variables require compile_with_vars, not basic compile
                return Err(CompileError::UnsupportedStep {
                    step: "from(variable) - use execute_script() for variable support".to_string(),
                });
            }
        };
    }

    // Set to endpoint
    if let Some(to) = to_args {
        builder = match to {
            FromToArgs::Label(label) => builder.to_label(label),
            FromToArgs::Id(lit) => {
                let id = match lit {
                    Literal::Int(i) => VertexId(i as u64),
                    Literal::String(s) => s.parse::<u64>().map(VertexId).map_err(|_| {
                        CompileError::InvalidArguments {
                            step: "to".to_string(),
                            message: format!("Invalid vertex ID: {}", s),
                        }
                    })?,
                    _ => {
                        return Err(CompileError::InvalidArguments {
                            step: "to".to_string(),
                            message: "to() requires a label string or vertex ID".to_string(),
                        });
                    }
                };
                builder.to_vertex(id)
            }
            FromToArgs::Traversal(_) => {
                // Traversal-based to (e.g., to(__.select('b'))) requires more complex handling
                return Err(CompileError::UnsupportedStep {
                    step: "to(__.traversal) - use to('label') with as() instead".to_string(),
                });
            }
            FromToArgs::Variable(_) => {
                // Variables require compile_with_vars, not basic compile
                return Err(CompileError::UnsupportedStep {
                    step: "to(variable) - use execute_script() for variable support".to_string(),
                });
            }
        };
    }

    // Add properties
    for prop in properties {
        let val = literal_to_value(&prop.value);
        builder = builder.property(&prop.key, val);
    }

    Ok(builder.build())
}

/// Compile repeat() with times/until/emit modifiers.
fn compile_repeat<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    sub: &AnonymousTraversal,
    times: Option<u32>,
    _until: Option<Box<AnonymousTraversal>>,
    _emit: Option<Option<Box<AnonymousTraversal>>>,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let anon = compile_anonymous_traversal(sub)?;

    if let Some(n) = times {
        Ok(traversal.repeat(anon).times(n as usize).identity())
    } else {
        // Without times() or until(), repeat would be infinite
        // Default to a safe limit
        Err(CompileError::InvalidArguments {
            step: "repeat".to_string(),
            message: "repeat() requires times() or until() to terminate".to_string(),
        })
    }
}

/// Compile an anonymous traversal.
fn compile_anonymous_traversal(
    ast: &AnonymousTraversal,
) -> Result<Traversal<Value, Value>, CompileError> {
    let mut traversal: Traversal<Value, Value> = __.identity();

    for step in &ast.steps {
        traversal = compile_anonymous_step(step, traversal)?;
    }

    Ok(traversal)
}

/// Compile a step for an anonymous traversal.
fn compile_anonymous_step(
    step: &Step,
    traversal: Traversal<Value, Value>,
) -> Result<Traversal<Value, Value>, CompileError> {
    match step {
        // Navigation
        Step::Out { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.append(__.out()))
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.append(__.out_labels(&labels_ref)))
            }
        }
        Step::In { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.append(__.in_()))
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.append(__.in_labels(&labels_ref)))
            }
        }
        Step::Both { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.append(__.both()))
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.append(__.both_labels(&labels_ref)))
            }
        }
        Step::OutE { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.append(__.out_e()))
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.append(__.out_e_labels(&labels_ref)))
            }
        }
        Step::InE { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.append(__.in_e()))
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.append(__.in_e_labels(&labels_ref)))
            }
        }
        Step::BothE { labels, .. } => {
            if labels.is_empty() {
                Ok(traversal.append(__.both_e()))
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.append(__.both_e_labels(&labels_ref)))
            }
        }
        Step::OutV { .. } => Ok(traversal.append(__.out_v())),
        Step::InV { .. } => Ok(traversal.append(__.in_v())),
        Step::BothV { .. } => Ok(traversal.append(__.both_v())),
        Step::OtherV { .. } => Ok(traversal.append(__.other_v())),

        // Filter
        Step::Has { args, .. } => compile_anonymous_has(traversal, args),
        Step::HasLabel { labels, .. } => {
            if labels.len() == 1 {
                Ok(traversal.append(__.has_label(&labels[0])))
            } else {
                let label_refs: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.append(__.has_label_any(&label_refs)))
            }
        }
        Step::HasId { ids, .. } => {
            if ids.len() == 1 {
                let id_val = literal_to_value(&ids[0]);
                Ok(traversal.append(__.has_id(id_val)))
            } else {
                let id_vals: Vec<Value> = ids.iter().map(literal_to_value).collect();
                Ok(traversal.append(__.has_ids(id_vals)))
            }
        }
        Step::HasNot { key, .. } => Ok(traversal.append(__.has_not(key))),
        Step::HasKey { keys, .. } => {
            if keys.len() == 1 {
                Ok(traversal.append(__.has_key(&keys[0])))
            } else {
                Ok(traversal.append(__.has_key_any(keys.clone())))
            }
        }
        Step::HasValue { values, .. } => {
            if values.len() == 1 {
                let val = literal_to_value(&values[0]);
                Ok(traversal.append(__.has_prop_value(val)))
            } else {
                let vals: Vec<Value> = values.iter().map(literal_to_value).collect();
                Ok(traversal.append(__.has_prop_value_any(vals)))
            }
        }
        Step::Where { args, .. } => match args {
            WhereArgs::Traversal(sub) => {
                let anon = compile_anonymous_traversal(sub)?;
                Ok(traversal.append(__.where_(anon)))
            }
            WhereArgs::Predicate(_) => Err(CompileError::UnsupportedStep {
                step: "where(predicate) in anonymous traversal".to_string(),
            }),
        },
        Step::Is { args, .. } => match args {
            IsArgs::Value(lit) => {
                let val = literal_to_value(lit);
                Ok(traversal.append(__.is_eq(val)))
            }
            IsArgs::Predicate(pred) => {
                let p = compile_predicate(pred)?;
                Ok(traversal.append(__.is_(p)))
            }
        },
        Step::And { traversals, .. } => {
            let anons: Result<Vec<_>, _> =
                traversals.iter().map(compile_anonymous_traversal).collect();
            Ok(traversal.append(__.and_(anons?)))
        }
        Step::Or { traversals, .. } => {
            let anons: Result<Vec<_>, _> =
                traversals.iter().map(compile_anonymous_traversal).collect();
            Ok(traversal.append(__.or_(anons?)))
        }
        Step::Not { traversal: sub, .. } => {
            let anon = compile_anonymous_traversal(sub)?;
            Ok(traversal.append(__.not(anon)))
        }
        Step::Dedup { by_label, .. } => {
            if let Some(key) = by_label {
                Ok(traversal.append(__.dedup_by_key(key)))
            } else {
                Ok(traversal.append(__.dedup()))
            }
        }
        Step::Limit { count, .. } => Ok(traversal.append(__.limit(*count as usize))),
        Step::Skip { count, .. } => Ok(traversal.append(__.skip(*count as usize))),
        Step::Range { start, end, .. } => {
            Ok(traversal.append(__.range(*start as usize, *end as usize)))
        }
        Step::Tail { count, .. } => {
            if let Some(n) = count {
                Ok(traversal.append(__.tail_n(*n as usize)))
            } else {
                Ok(traversal.append(__.tail()))
            }
        }
        Step::Coin { probability, .. } => Ok(traversal.append(__.coin(*probability))),
        Step::Sample { count, .. } => Ok(traversal.append(__.sample(*count as usize))),
        Step::SimplePath { .. } => Ok(traversal.append(__.simple_path())),
        Step::CyclicPath { .. } => Ok(traversal.append(__.cyclic_path())),

        // Transform
        Step::Values { keys, .. } => {
            if keys.len() == 1 {
                Ok(traversal.append(__.values(&keys[0])))
            } else {
                Ok(traversal.append(__.values_multi(keys.clone())))
            }
        }
        Step::Id { .. } => Ok(traversal.append(__.id())),
        Step::Label { .. } => Ok(traversal.append(__.label())),
        Step::ValueMap { .. } => Ok(traversal.append(__.value_map())),
        Step::ElementMap { .. } => Ok(traversal.append(__.element_map())),
        Step::Constant { value, .. } => {
            let val = literal_to_value(value);
            Ok(traversal.append(__.constant(val)))
        }
        Step::Identity { .. } => Ok(traversal.append(__.identity())),
        Step::As { label, .. } => Ok(traversal.append(__.as_(label))),
        Step::Select { labels, .. } => {
            if labels.len() == 1 {
                Ok(traversal.append(__.select_one(&labels[0])))
            } else {
                let labels_ref: Vec<&str> = labels.iter().map(|s| s.as_str()).collect();
                Ok(traversal.append(__.select(&labels_ref)))
            }
        }
        Step::Unfold { .. } => Ok(traversal.append(__.unfold())),
        Step::Fold { .. } => Ok(traversal.append(__.fold())),
        Step::Count { .. } => Ok(traversal.append(__.count())),
        Step::Sum { .. } => Ok(traversal.append(__.sum())),
        Step::Min { .. } => Ok(traversal.append(__.min())),
        Step::Max { .. } => Ok(traversal.append(__.max())),

        // For unsupported steps in anonymous context
        _ => Err(CompileError::UnsupportedStep {
            step: format!("{} in anonymous traversal", step.name()),
        }),
    }
}

/// Compile has() for anonymous traversal.
fn compile_anonymous_has(
    traversal: Traversal<Value, Value>,
    args: &HasArgs,
) -> Result<Traversal<Value, Value>, CompileError> {
    match args {
        HasArgs::Key(key) => Ok(traversal.append(__.has(key))),
        HasArgs::KeyValue { key, value } => {
            let val = literal_to_value(value);
            Ok(traversal.append(__.has_value(key, val)))
        }
        HasArgs::KeyPredicate { key, predicate } => {
            let p = compile_predicate(predicate)?;
            Ok(traversal.append(__.has_where(key, p)))
        }
        HasArgs::LabelKeyValue { label, key, value } => {
            let val = literal_to_value(value);
            Ok(traversal
                .append(__.has_label(label))
                .append(__.has_value(key, val)))
        }
    }
}

/// Compile a predicate.
fn compile_predicate(
    pred: &Predicate,
) -> Result<Box<dyn crate::traversal::Predicate>, CompileError> {
    match pred {
        Predicate::Eq(lit) => Ok(Box::new(p::eq(literal_to_value(lit)))),
        Predicate::Neq(lit) => Ok(Box::new(p::neq(literal_to_value(lit)))),
        Predicate::Lt(lit) => Ok(Box::new(p::lt(literal_to_value(lit)))),
        Predicate::Lte(lit) => Ok(Box::new(p::lte(literal_to_value(lit)))),
        Predicate::Gt(lit) => Ok(Box::new(p::gt(literal_to_value(lit)))),
        Predicate::Gte(lit) => Ok(Box::new(p::gte(literal_to_value(lit)))),
        Predicate::Between { start, end } => Ok(Box::new(p::between(
            literal_to_value(start),
            literal_to_value(end),
        ))),
        Predicate::Inside { start, end } => Ok(Box::new(p::inside(
            literal_to_value(start),
            literal_to_value(end),
        ))),
        Predicate::Outside { start, end } => Ok(Box::new(p::outside(
            literal_to_value(start),
            literal_to_value(end),
        ))),
        Predicate::Within(values) => {
            let vals: Vec<Value> = values.iter().map(literal_to_value).collect();
            Ok(Box::new(p::within(vals)))
        }
        Predicate::Without(values) => {
            let vals: Vec<Value> = values.iter().map(literal_to_value).collect();
            Ok(Box::new(p::without(vals)))
        }
        Predicate::And(p1, p2) => {
            let left = compile_predicate(p1)?;
            let right = compile_predicate(p2)?;
            Ok(p::and_pred(left, right))
        }
        Predicate::Or(p1, p2) => {
            let left = compile_predicate(p1)?;
            let right = compile_predicate(p2)?;
            Ok(p::or_pred(left, right))
        }
        Predicate::Not(inner) => {
            let pred = compile_predicate(inner)?;
            Ok(p::not_pred(pred))
        }
        Predicate::Containing(s) => Ok(Box::new(p::containing(s))),
        Predicate::NotContaining(s) => Ok(Box::new(p::not_containing(s))),
        Predicate::StartingWith(s) => Ok(Box::new(p::starting_with(s))),
        Predicate::NotStartingWith(s) => Ok(Box::new(p::not_starting_with(s))),
        Predicate::EndingWith(s) => Ok(Box::new(p::ending_with(s))),
        Predicate::NotEndingWith(s) => Ok(Box::new(p::not_ending_with(s))),
        Predicate::Regex(pattern) => Ok(Box::new(p::regex(pattern))),
        Predicate::GeoWithinDistance { geometry, distance } => {
            use crate::geo::Distance;
            let center = match literal_to_value(geometry) {
                Value::Point(pt) => pt,
                _ => {
                    return Err(CompileError::TypeMismatch {
                        message: "geo_within_distance requires a point geometry".to_string(),
                    })
                }
            };
            let dist = match distance {
                Literal::Distance { value, unit } => {
                    use DistanceUnit::*;
                    match unit {
                        Meters => Distance::Meters(*value),
                        Kilometers => Distance::Kilometers(*value),
                        Miles => Distance::Miles(*value),
                        NauticalMiles => Distance::NauticalMiles(*value),
                    }
                }
                _ => {
                    return Err(CompileError::TypeMismatch {
                        message: "geo_within_distance requires a distance literal (e.g., 5km)"
                            .to_string(),
                    })
                }
            };
            Ok(Box::new(p::within_distance(center, dist)))
        }
        Predicate::GeoIntersects(geometry) => {
            let val = literal_to_value(geometry);
            let geom = match &val {
                Value::Point(pt) => p::GeometryRef::Point(*pt),
                Value::Polygon(poly) => p::GeometryRef::Polygon(poly.clone()),
                _ => {
                    return Err(CompileError::TypeMismatch {
                        message: "geo_intersects requires a point or polygon geometry".to_string(),
                    })
                }
            };
            Ok(Box::new(p::intersects(geom)))
        }
        Predicate::GeoContainedBy(geometry) => {
            let val = literal_to_value(geometry);
            match val {
                Value::Polygon(poly) => Ok(Box::new(p::contained_by(poly))),
                _ => Err(CompileError::TypeMismatch {
                    message: "geo_contained_by requires a polygon geometry".to_string(),
                }),
            }
        }
        Predicate::GeoBBox {
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        } => Ok(Box::new(p::bbox(*min_lon, *min_lat, *max_lon, *max_lat))),
    }
}

/// Convert an AST Literal to a runtime Value.
pub fn literal_to_value(literal: &Literal) -> Value {
    match literal {
        Literal::String(s) => Value::String(s.clone()),
        Literal::Int(n) => Value::Int(*n),
        Literal::Float(f) => Value::Float(*f),
        Literal::Bool(b) => Value::Bool(*b),
        Literal::Null => Value::Null,
        Literal::List(items) => Value::List(items.iter().map(literal_to_value).collect()),
        Literal::Map(entries) => Value::Map(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), literal_to_value(v)))
                .collect(),
        ),
        Literal::Point { lon, lat } => {
            // Use unchecked since validation happens at query execution time
            Value::Point(crate::geo::Point::new_unchecked(*lon, *lat))
        }
        Literal::Polygon(coords) => {
            // Construct polygon from coordinate tuples; validation at execution time
            match crate::geo::Polygon::new(coords.iter().copied()) {
                Ok(poly) => Value::Polygon(poly),
                // Fallback: create with unchecked points for degenerate input
                Err(_) => Value::Null,
            }
        }
        Literal::Distance { value, unit } => {
            use crate::geo::Distance;
            use DistanceUnit::*;
            let dist = match unit {
                Meters => Distance::Meters(*value),
                Kilometers => Distance::Kilometers(*value),
                Miles => Distance::Miles(*value),
                NauticalMiles => Distance::NauticalMiles(*value),
            };
            // Distance literals are converted to meters as a float
            Value::Float(dist.meters())
        }
    }
}

// ============================================================
// Validation (kept from original)
// ============================================================

/// Validate the AST for semantic correctness.
fn validate_ast(ast: &GremlinTraversal) -> Result<(), CompileError> {
    validate_source(&ast.source)?;
    for (i, step) in ast.steps.iter().enumerate() {
        validate_step(step, i, &ast.steps)?;
    }
    Ok(())
}

fn validate_source(source: &SourceStep) -> Result<(), CompileError> {
    match source {
        SourceStep::V { ids, .. } | SourceStep::E { ids, .. } => {
            for id in ids {
                validate_id_literal(id)?;
            }
            Ok(())
        }
        SourceStep::AddV { label, .. } | SourceStep::AddE { label, .. } => {
            if label.is_empty() {
                Err(CompileError::InvalidArguments {
                    step: "addV/addE".to_string(),
                    message: "label cannot be empty".to_string(),
                })
            } else {
                Ok(())
            }
        }
        SourceStep::Inject { .. } => Ok(()),
        SourceStep::SearchTextV { property, k, .. }
        | SourceStep::SearchTextE { property, k, .. } => {
            if property.is_empty() {
                return Err(CompileError::InvalidArguments {
                    step: "searchTextV/searchTextE".to_string(),
                    message: "property name cannot be empty".to_string(),
                });
            }
            if *k == 0 {
                return Err(CompileError::InvalidArguments {
                    step: "searchTextV/searchTextE".to_string(),
                    message: "k must be > 0".to_string(),
                });
            }
            Ok(())
        }
    }
}

fn validate_id_literal(literal: &Literal) -> Result<(), CompileError> {
    match literal {
        Literal::Int(_) | Literal::String(_) => Ok(()),
        _ => Err(CompileError::TypeMismatch {
            message: "vertex/edge ID must be an integer or string".to_string(),
        }),
    }
}

fn validate_step(step: &Step, index: usize, _all_steps: &[Step]) -> Result<(), CompileError> {
    match step {
        Step::Limit { count, .. } if *count == 0 => Err(CompileError::InvalidArguments {
            step: "limit".to_string(),
            message: "count must be greater than 0".to_string(),
        }),
        Step::Range { start, end, .. } if start >= end => Err(CompileError::InvalidArguments {
            step: "range".to_string(),
            message: "start must be less than end".to_string(),
        }),
        Step::Coin { probability, .. } if !(0.0..=1.0).contains(probability) => {
            Err(CompileError::InvalidArguments {
                step: "coin".to_string(),
                message: "probability must be between 0.0 and 1.0".to_string(),
            })
        }
        Step::By { .. } if index == 0 => Err(CompileError::MissingPrecedingStep {
            step: "by".to_string(),
            required: "order, project, group".to_string(),
        }),
        Step::Times { .. } | Step::Until { .. } | Step::Emit { .. } if index == 0 => {
            Err(CompileError::MissingPrecedingStep {
                step: step.name().to_string(),
                required: "repeat".to_string(),
            })
        }
        Step::Option { .. } if index == 0 => Err(CompileError::MissingPrecedingStep {
            step: "option".to_string(),
            required: "choose or branch".to_string(),
        }),
        _ => Ok(()),
    }
}

// ============================================================
// Helper trait for Step names
// ============================================================

impl Step {
    fn name(&self) -> &'static str {
        match self {
            Step::Out { .. } => "out",
            Step::In { .. } => "in",
            Step::Both { .. } => "both",
            Step::OutE { .. } => "outE",
            Step::InE { .. } => "inE",
            Step::BothE { .. } => "bothE",
            Step::OutV { .. } => "outV",
            Step::InV { .. } => "inV",
            Step::BothV { .. } => "bothV",
            Step::OtherV { .. } => "otherV",
            Step::Has { .. } => "has",
            Step::HasLabel { .. } => "hasLabel",
            Step::HasId { .. } => "hasId",
            Step::HasNot { .. } => "hasNot",
            Step::HasKey { .. } => "hasKey",
            Step::HasValue { .. } => "hasValue",
            Step::Where { .. } => "where",
            Step::Is { .. } => "is",
            Step::And { .. } => "and",
            Step::Or { .. } => "or",
            Step::Not { .. } => "not",
            Step::Dedup { .. } => "dedup",
            Step::Limit { .. } => "limit",
            Step::Skip { .. } => "skip",
            Step::Range { .. } => "range",
            Step::Tail { .. } => "tail",
            Step::Coin { .. } => "coin",
            Step::Sample { .. } => "sample",
            Step::SimplePath { .. } => "simplePath",
            Step::CyclicPath { .. } => "cyclicPath",
            Step::Values { .. } => "values",
            Step::Properties { .. } => "properties",
            Step::ValueMap { .. } => "valueMap",
            Step::ElementMap { .. } => "elementMap",
            Step::PropertyMap { .. } => "propertyMap",
            Step::Id { .. } => "id",
            Step::Label { .. } => "label",
            Step::Key { .. } => "key",
            Step::Value { .. } => "value",
            Step::Path { .. } => "path",
            Step::Select { .. } => "select",
            Step::Project { .. } => "project",
            Step::By { .. } => "by",
            Step::Unfold { .. } => "unfold",
            Step::Fold { .. } => "fold",
            Step::Count { .. } => "count",
            Step::Sum { .. } => "sum",
            Step::Max { .. } => "max",
            Step::Min { .. } => "min",
            Step::Mean { .. } => "mean",
            Step::Group { .. } => "group",
            Step::GroupCount { .. } => "groupCount",
            Step::Order { .. } => "order",
            Step::Math { .. } => "math",
            Step::Constant { .. } => "constant",
            Step::Identity { .. } => "identity",
            Step::Index { .. } => "index",
            Step::Loops { .. } => "loops",
            Step::Choose { .. } => "choose",
            Step::Union { .. } => "union",
            Step::Coalesce { .. } => "coalesce",
            Step::Optional { .. } => "optional",
            Step::Local { .. } => "local",
            Step::Branch { .. } => "branch",
            Step::Option { .. } => "option",
            Step::Repeat { .. } => "repeat",
            Step::Times { .. } => "times",
            Step::Until { .. } => "until",
            Step::Emit { .. } => "emit",
            Step::As { .. } => "as",
            Step::Aggregate { .. } => "aggregate",
            Step::Store { .. } => "store",
            Step::Cap { .. } => "cap",
            Step::SideEffect { .. } => "sideEffect",
            Step::Profile { .. } => "profile",
            Step::AddV { .. } => "addV",
            Step::AddE { .. } => "addE",
            Step::Property { .. } => "property",
            Step::From { .. } => "from",
            Step::To { .. } => "to",
            Step::Drop { .. } => "drop",
            Step::TextScore { .. } => "textScore",
        }
    }
}

// ============================================================
// Multi-Statement Script Execution
// ============================================================

/// Variable context for tracking bindings during script execution.
///
/// Variables are bound when assignments are executed and can be referenced
/// in subsequent statements via g.V(variable), from(variable), or to(variable).
///
/// This type can be passed between `execute_script_with_context` calls to
/// maintain state in a REPL-style workflow.
#[derive(Debug, Default, Clone)]
pub struct VariableContext {
    /// Maps variable names to their bound values
    bindings: HashMap<String, Value>,
}

impl VariableContext {
    /// Create a new empty variable context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind a value to a variable name.
    pub fn bind(&mut self, name: String, value: Value) {
        self.bindings.insert(name, value);
    }

    /// Look up a variable's value.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.bindings.get(name)
    }

    /// Get a vertex ID from a variable.
    ///
    /// Returns Some(VertexId) if the variable exists and contains a vertex
    /// (either as a Value::Vertex or extracting id from vertex properties).
    pub fn get_vertex_id(&self, name: &str) -> Option<VertexId> {
        self.get(name).and_then(|v| match v {
            Value::Int(id) => Some(VertexId(*id as u64)),
            Value::Vertex(vid) => Some(*vid),
            Value::Map(map) => {
                // Try to extract id from a vertex map representation
                map.get("id").and_then(|id_val| match id_val {
                    Value::Int(id) => Some(VertexId(*id as u64)),
                    _ => None,
                })
            }
            _ => None,
        })
    }

    /// Check if a variable exists.
    pub fn contains(&self, name: &str) -> bool {
        self.bindings.contains_key(name)
    }

    /// Get all variable names.
    pub fn variables(&self) -> impl Iterator<Item = &str> {
        self.bindings.keys().map(|s| s.as_str())
    }

    /// Get all bindings as an iterator.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.bindings.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Get the number of bound variables.
    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    /// Check if the context is empty.
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

/// Result of executing a Gremlin script.
///
/// Contains both the execution result and the variable context after execution,
/// enabling REPL-style workflows where the context can be passed to subsequent calls.
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::prelude::*;
/// use interstellar::gremlin::{ScriptResult, VariableContext};
///
/// let graph = Graph::new();
/// let mut ctx = VariableContext::new();
///
/// // First command
/// let result = graph.execute_script_with_context(
///     "alice = g.addV('person').property('name', 'Alice').next()",
///     ctx
/// )?;
/// ctx = result.variables;
///
/// // Second command (uses 'alice' from previous execution)
/// let result = graph.execute_script_with_context(
///     "g.V(alice).values('name').toList()",
///     ctx
/// )?;
/// ```
#[derive(Debug, Clone)]
pub struct ScriptResult {
    /// The result of the last statement in the script.
    pub result: ExecutionResult,
    /// The variable context after execution, containing all bound variables.
    pub variables: VariableContext,
}

/// Execute a multi-statement Gremlin script with a fresh variable context.
///
/// This is a convenience wrapper around [`execute_script_with_context`] that
/// starts with an empty variable context.
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::gremlin::{parse_script, execute_script};
/// use interstellar::prelude::*;
///
/// let graph = Graph::new();
/// let script = parse_script(r#"
///     alice = g.addV('person').property('name', 'Alice').next()
///     bob = g.addV('person').property('name', 'Bob').next()
///     g.addE('knows').from(alice).to(bob).next()
///     g.V(alice).out('knows').values('name').toList()
/// "#)?;
///
/// let result = execute_script(&script, &graph)?;
/// // result.result contains the execution result
/// // result.variables contains {alice: VertexId(...), bob: VertexId(...)}
/// ```
pub fn execute_script<'g>(
    script: &Script,
    g: &GraphTraversalSource<'g>,
) -> Result<ScriptResult, CompileError> {
    execute_script_with_context(script, g, VariableContext::new())
}

/// Execute a multi-statement Gremlin script with an existing variable context.
///
/// This function executes each statement in order, tracking variable bindings.
/// Variables from the input context are available for reference, and new
/// assignments are added to the returned context.
///
/// This enables REPL-style workflows where state is maintained across calls:
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::gremlin::{parse_script, execute_script_with_context, VariableContext};
/// use interstellar::prelude::*;
///
/// let graph = Graph::new();
/// let mut ctx = VariableContext::new();
///
/// // First REPL input
/// let script1 = parse_script("alice = g.addV('person').next()")?;
/// let result1 = execute_script_with_context(&script1, &g, ctx)?;
/// ctx = result1.variables;  // alice is now bound
///
/// // Second REPL input (can reference alice)
/// let script2 = parse_script("g.V(alice).label().next()")?;
/// let result2 = execute_script_with_context(&script2, &g, ctx)?;
/// ctx = result2.variables;
/// ```
pub fn execute_script_with_context<'g>(
    script: &Script,
    g: &GraphTraversalSource<'g>,
    context: VariableContext,
) -> Result<ScriptResult, CompileError> {
    let mut ctx = context;
    let mut last_result = ExecutionResult::Unit;

    for statement in &script.statements {
        match statement {
            Statement::Assignment {
                name, traversal, ..
            } => {
                // Compile and execute the traversal with variable context
                let compiled = compile_with_vars(traversal, g, &ctx)?;
                let result = compiled.execute();

                // Bind the result to the variable
                match result {
                    ExecutionResult::Single(Some(value)) => {
                        ctx.bind(name.clone(), value);
                    }
                    ExecutionResult::List(values) if values.len() == 1 => {
                        ctx.bind(name.clone(), values.into_iter().next().unwrap());
                    }
                    ExecutionResult::List(values) if values.is_empty() => {
                        return Err(CompileError::InvalidArguments {
                            step: "assignment".to_string(),
                            message: format!(
                                "assignment to '{}' requires single value, traversal returned empty",
                                name
                            ),
                        });
                    }
                    ExecutionResult::List(values) => {
                        return Err(CompileError::InvalidArguments {
                            step: "assignment".to_string(),
                            message: format!(
                                "assignment to '{}' requires single value, got {} values",
                                name,
                                values.len()
                            ),
                        });
                    }
                    _ => {
                        return Err(CompileError::InvalidArguments {
                            step: "assignment".to_string(),
                            message: format!(
                                "assignment to '{}' requires .next() terminal to produce single value",
                                name
                            ),
                        });
                    }
                }
                last_result = ExecutionResult::Unit;
            }
            Statement::Traversal { traversal, .. } => {
                let compiled = compile_with_vars(traversal, g, &ctx)?;
                last_result = compiled.execute();
            }
        }
    }

    Ok(ScriptResult {
        result: last_result,
        variables: ctx,
    })
}

/// Compile a traversal with variable context for resolving references.
pub fn compile_with_vars<'g>(
    ast: &GremlinTraversal,
    g: &GraphTraversalSource<'g>,
    ctx: &VariableContext,
) -> Result<CompiledTraversal<'g>, CompileError> {
    // Validate the AST structure first
    validate_ast(ast)?;

    // Handle addE as source specially - it needs to look ahead for from/to/property
    if let SourceStep::AddE { label, .. } = &ast.source {
        let (traversal, consumed) = compile_add_e_source_with_vars(g, label, &ast.steps, ctx)?;
        // Compile remaining steps
        let remaining = &ast.steps[consumed..];
        let traversal = compile_steps_with_vars(remaining, traversal, ctx)?;
        return Ok(CompiledTraversal {
            traversal,
            terminal: ast.terminal.clone(),
        });
    }

    // Compile source step to get initial traversal
    let mut traversal = compile_source_with_vars(&ast.source, g, ctx)?;

    // Compile each step and append to traversal
    traversal = compile_steps_with_vars(&ast.steps, traversal, ctx)?;

    Ok(CompiledTraversal {
        traversal,
        terminal: ast.terminal.clone(),
    })
}

/// Compile the source step with variable context support.
fn compile_source_with_vars<'g>(
    source: &SourceStep,
    g: &GraphTraversalSource<'g>,
    ctx: &VariableContext,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    match source {
        SourceStep::V { ids, variable, .. } => {
            if let Some(var_name) = variable {
                // Resolve variable to vertex ID
                let id =
                    ctx.get_vertex_id(var_name)
                        .ok_or_else(|| CompileError::InvalidArguments {
                            step: "V".to_string(),
                            message: format!("variable '{}' not found or not a vertex", var_name),
                        })?;
                Ok(g.v_ids(vec![id]))
            } else if ids.is_empty() {
                Ok(g.v())
            } else {
                let vertex_ids: Vec<VertexId> = ids
                    .iter()
                    .map(|lit| match lit {
                        Literal::Int(n) => VertexId(*n as u64),
                        Literal::String(s) => s.parse::<u64>().map(VertexId).unwrap_or(VertexId(0)),
                        _ => VertexId(0),
                    })
                    .collect();
                Ok(g.v_ids(vertex_ids))
            }
        }
        SourceStep::E { ids, variable, .. } => {
            if let Some(var_name) = variable {
                // Resolve variable to edge ID
                let id_val = ctx
                    .get(var_name)
                    .ok_or_else(|| CompileError::InvalidArguments {
                        step: "E".to_string(),
                        message: format!("variable '{}' not found", var_name),
                    })?;
                let id = match id_val {
                    Value::Int(n) => EdgeId(*n as u64),
                    Value::Edge(eid) => *eid,
                    _ => {
                        return Err(CompileError::InvalidArguments {
                            step: "E".to_string(),
                            message: format!("variable '{}' is not an edge", var_name),
                        });
                    }
                };
                Ok(g.e_ids(vec![id]))
            } else if ids.is_empty() {
                Ok(g.e())
            } else {
                let edge_ids: Vec<EdgeId> = ids
                    .iter()
                    .map(|lit| match lit {
                        Literal::Int(n) => EdgeId(*n as u64),
                        Literal::String(s) => s.parse::<u64>().map(EdgeId).unwrap_or(EdgeId(0)),
                        _ => EdgeId(0),
                    })
                    .collect();
                Ok(g.e_ids(edge_ids))
            }
        }
        SourceStep::AddV { label, .. } => Ok(g.add_v(label)),
        SourceStep::AddE { label: _, .. } => Err(CompileError::UnsupportedStep {
            step: "addE as source (use g.V().addE() pattern instead)".to_string(),
        }),
        SourceStep::Inject { values, .. } => {
            let vals: Vec<Value> = values.iter().map(literal_to_value).collect();
            Ok(g.inject(vals))
        }
        SourceStep::SearchTextV {
            property, query, k, ..
        } => compile_search_text_v(g, property, query, *k),
        SourceStep::SearchTextE {
            property, query, k, ..
        } => compile_search_text_e(g, property, query, *k),
    }
}

/// Compile addE as source with variable context support.
fn compile_add_e_source_with_vars<'g>(
    g: &GraphTraversalSource<'g>,
    label: &str,
    steps: &[Step],
    ctx: &VariableContext,
) -> Result<(BoundTraversal<'g, (), Value>, usize), CompileError> {
    let mut from_args = None;
    let mut to_args = None;
    let mut properties = Vec::new();
    let mut consumed = 0;

    // Look ahead for from(), to(), and property() steps
    for step in steps {
        match step {
            Step::From { args, .. } => {
                from_args = Some(args.clone());
                consumed += 1;
            }
            Step::To { args, .. } => {
                to_args = Some(args.clone());
                consumed += 1;
            }
            Step::Property { args, .. } => {
                properties.push(args.clone());
                consumed += 1;
            }
            _ => break,
        }
    }

    // Build the edge using the source builder
    let mut builder = g.add_e(label);

    // Set from endpoint with variable support
    if let Some(from) = from_args {
        builder = match from {
            FromToArgs::Label(label) => builder.from_label(label),
            FromToArgs::Variable(var_name) => {
                let id =
                    ctx.get_vertex_id(&var_name)
                        .ok_or_else(|| CompileError::InvalidArguments {
                            step: "from".to_string(),
                            message: format!("variable '{}' not found or not a vertex", var_name),
                        })?;
                builder.from_vertex(id)
            }
            FromToArgs::Id(lit) => {
                let id = match lit {
                    Literal::Int(i) => VertexId(i as u64),
                    Literal::String(s) => s.parse::<u64>().map(VertexId).map_err(|_| {
                        CompileError::InvalidArguments {
                            step: "from".to_string(),
                            message: format!("Invalid vertex ID: {}", s),
                        }
                    })?,
                    _ => {
                        return Err(CompileError::InvalidArguments {
                            step: "from".to_string(),
                            message: "from() requires a label string or vertex ID".to_string(),
                        });
                    }
                };
                builder.from_vertex(id)
            }
            FromToArgs::Traversal(_) => {
                return Err(CompileError::UnsupportedStep {
                    step: "from(__.traversal) - use from('label') with as() instead".to_string(),
                });
            }
        };
    }

    // Set to endpoint with variable support
    if let Some(to) = to_args {
        builder = match to {
            FromToArgs::Label(label) => builder.to_label(label),
            FromToArgs::Variable(var_name) => {
                let id =
                    ctx.get_vertex_id(&var_name)
                        .ok_or_else(|| CompileError::InvalidArguments {
                            step: "to".to_string(),
                            message: format!("variable '{}' not found or not a vertex", var_name),
                        })?;
                builder.to_vertex(id)
            }
            FromToArgs::Id(lit) => {
                let id = match lit {
                    Literal::Int(i) => VertexId(i as u64),
                    Literal::String(s) => s.parse::<u64>().map(VertexId).map_err(|_| {
                        CompileError::InvalidArguments {
                            step: "to".to_string(),
                            message: format!("Invalid vertex ID: {}", s),
                        }
                    })?,
                    _ => {
                        return Err(CompileError::InvalidArguments {
                            step: "to".to_string(),
                            message: "to() requires a label string or vertex ID".to_string(),
                        });
                    }
                };
                builder.to_vertex(id)
            }
            FromToArgs::Traversal(_) => {
                return Err(CompileError::UnsupportedStep {
                    step: "to(__.traversal) - use to('label') with as() instead".to_string(),
                });
            }
        };
    }

    // Add properties
    for prop in properties {
        let val = literal_to_value(&prop.value);
        builder = builder.property(&prop.key, val);
    }

    Ok((builder.build(), consumed))
}

/// Compile steps with variable context support.
fn compile_steps_with_vars<'g>(
    steps: &[Step],
    mut traversal: BoundTraversal<'g, (), Value>,
    ctx: &VariableContext,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let mut i = 0;
    while i < steps.len() {
        let step = &steps[i];

        match step {
            Step::AddE { label, .. } => {
                // Look ahead for from(), to(), and property() steps
                let mut from_args = None;
                let mut to_args = None;
                let mut properties = Vec::new();
                let mut j = i + 1;
                while j < steps.len() {
                    match &steps[j] {
                        Step::From { args, .. } => {
                            from_args = Some(args.clone());
                            j += 1;
                        }
                        Step::To { args, .. } => {
                            to_args = Some(args.clone());
                            j += 1;
                        }
                        Step::Property { args, .. } => {
                            properties.push(args.clone());
                            j += 1;
                        }
                        _ => break,
                    }
                }
                traversal = compile_add_e_with_endpoints_and_vars(
                    traversal,
                    label,
                    from_args,
                    to_args,
                    &properties,
                    ctx,
                )?;
                i = j;
                continue;
            }
            _ => {
                // For all other steps, delegate to the regular compile_steps
                // since they don't need variable resolution
                traversal = compile_step(step, traversal)?;
                i += 1;
            }
        }
    }

    Ok(traversal)
}

/// Compile addE with endpoints and variable context support.
fn compile_add_e_with_endpoints_and_vars<'g>(
    traversal: BoundTraversal<'g, (), Value>,
    label: &str,
    from_args: Option<FromToArgs>,
    to_args: Option<FromToArgs>,
    properties: &[PropertyArgs],
    ctx: &VariableContext,
) -> Result<BoundTraversal<'g, (), Value>, CompileError> {
    let mut builder = traversal.add_e(label);

    // Set from endpoint with variable support
    if let Some(from) = from_args {
        builder = match from {
            FromToArgs::Label(label) => builder.from_label(label),
            FromToArgs::Variable(var_name) => {
                let id =
                    ctx.get_vertex_id(&var_name)
                        .ok_or_else(|| CompileError::InvalidArguments {
                            step: "from".to_string(),
                            message: format!("variable '{}' not found or not a vertex", var_name),
                        })?;
                builder.from_vertex(id)
            }
            FromToArgs::Id(lit) => {
                let id = match lit {
                    Literal::Int(i) => VertexId(i as u64),
                    Literal::String(s) => s.parse::<u64>().map(VertexId).map_err(|_| {
                        CompileError::InvalidArguments {
                            step: "from".to_string(),
                            message: format!("Invalid vertex ID: {}", s),
                        }
                    })?,
                    _ => {
                        return Err(CompileError::InvalidArguments {
                            step: "from".to_string(),
                            message: "from() requires a label string or vertex ID".to_string(),
                        });
                    }
                };
                builder.from_vertex(id)
            }
            FromToArgs::Traversal(_) => {
                return Err(CompileError::UnsupportedStep {
                    step: "from(__.traversal) - use from('label') with as() instead".to_string(),
                });
            }
        };
    }

    // Set to endpoint with variable support
    if let Some(to) = to_args {
        builder = match to {
            FromToArgs::Label(label) => builder.to_label(label),
            FromToArgs::Variable(var_name) => {
                let id =
                    ctx.get_vertex_id(&var_name)
                        .ok_or_else(|| CompileError::InvalidArguments {
                            step: "to".to_string(),
                            message: format!("variable '{}' not found or not a vertex", var_name),
                        })?;
                builder.to_vertex(id)
            }
            FromToArgs::Id(lit) => {
                let id = match lit {
                    Literal::Int(i) => VertexId(i as u64),
                    Literal::String(s) => s.parse::<u64>().map(VertexId).map_err(|_| {
                        CompileError::InvalidArguments {
                            step: "to".to_string(),
                            message: format!("Invalid vertex ID: {}", s),
                        }
                    })?,
                    _ => {
                        return Err(CompileError::InvalidArguments {
                            step: "to".to_string(),
                            message: "to() requires a label string or vertex ID".to_string(),
                        });
                    }
                };
                builder.to_vertex(id)
            }
            FromToArgs::Traversal(_) => {
                return Err(CompileError::UnsupportedStep {
                    step: "to(__.traversal) - use to('label') with as() instead".to_string(),
                });
            }
        };
    }

    // Add properties
    for prop in properties {
        let val = literal_to_value(&prop.value);
        builder = builder.property(&prop.key, val);
    }

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gremlin::parse;
    use crate::storage::Graph;

    fn test_graph() -> Graph {
        let graph = Graph::new();

        // Add some test vertices
        let alice = graph.add_vertex(
            "person",
            [
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::Int(30)),
            ]
            .into_iter()
            .collect(),
        );

        let bob = graph.add_vertex(
            "person",
            [
                ("name".to_string(), Value::String("Bob".to_string())),
                ("age".to_string(), Value::Int(25)),
            ]
            .into_iter()
            .collect(),
        );

        let charlie = graph.add_vertex(
            "person",
            [
                ("name".to_string(), Value::String("Charlie".to_string())),
                ("age".to_string(), Value::Int(35)),
            ]
            .into_iter()
            .collect(),
        );

        let software = graph.add_vertex(
            "software",
            [(
                "name".to_string(),
                Value::String("Interstellar".to_string()),
            )]
            .into_iter()
            .collect(),
        );

        // Add edges
        graph
            .add_edge(alice, bob, "knows", Default::default())
            .unwrap();
        graph
            .add_edge(bob, charlie, "knows", Default::default())
            .unwrap();
        graph
            .add_edge(alice, software, "created", Default::default())
            .unwrap();

        graph
    }

    #[test]
    fn test_compile_v_all() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V()").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 4); // 3 people + 1 software
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_has_label() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().hasLabel('person')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 3); // 3 people
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_values() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().hasLabel('person').values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 3);
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Bob".to_string())));
            assert!(values.contains(&Value::String("Charlie".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_has_predicate() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().has('age', P.gt(25)).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 2); // Alice (30) and Charlie (35)
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Charlie".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_has_key() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // hasKey filters property objects (from properties()) by key name
        let ast = parse("g.V().hasLabel('person').properties().hasKey('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Each of the 3 persons has a 'name' property
            assert_eq!(values.len(), 3);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_has_key_multiple() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // hasKey with multiple keys filters properties matching ANY of the keys
        // First get all properties, then filter to those with 'name' or 'age' key
        let ast =
            parse("g.V().hasLabel('person').limit(1).properties().hasKey('name', 'age')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // One person has 'name' and 'age' properties - both should match
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_has_value_on_properties() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // hasValue filters property objects by their value
        let ast = parse("g.V().properties('name').hasValue('Alice')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 1);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_has_value_multiple() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // hasValue with multiple values filters properties matching ANY of the values
        let ast = parse("g.V().properties('name').hasValue('Alice', 'Bob')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Should find Alice and Bob's name properties
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_out() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().hasLabel('person').out('knows').values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Alice->Bob, Bob->Charlie
            assert_eq!(values.len(), 2);
            assert!(values.contains(&Value::String("Bob".to_string())));
            assert!(values.contains(&Value::String("Charlie".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_limit() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().limit(2)").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_dedup() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().hasLabel('person').out().in().dedup()").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Should be deduplicated
            let unique: HashSet<_> = values.iter().collect();
            assert_eq!(values.len(), unique.len());
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_order_by() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().hasLabel('person').order().by('age', desc).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 3);
            // Should be sorted by age descending: Charlie(35), Alice(30), Bob(25)
            assert_eq!(values[0], Value::String("Charlie".to_string()));
            assert_eq!(values[1], Value::String("Alice".to_string()));
            assert_eq!(values[2], Value::String("Bob".to_string()));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_union() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().hasLabel('person').has('name', 'Alice').union(__.out('knows'), __.out('created')).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 2); // Bob (knows) + Interstellar (created)
            assert!(values.contains(&Value::String("Bob".to_string())));
            assert!(values.contains(&Value::String("Interstellar".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_terminal_next() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().hasLabel('person').next()").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        assert!(matches!(result, ExecutionResult::Single(Some(_))));
    }

    #[test]
    fn test_compile_terminal_has_next() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().hasLabel('person').hasNext()").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        assert!(matches!(result, ExecutionResult::Bool(true)));
    }

    #[test]
    fn test_compile_where_traversal() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().hasLabel('person').where(__.out('knows')).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Only Alice and Bob have outgoing 'knows' edges
            assert_eq!(values.len(), 2);
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Bob".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_repeat_times() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast =
            parse("g.V().has('name', 'Alice').repeat(__.out('knows')).times(2).values('name')")
                .unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Alice -> Bob -> Charlie
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], Value::String("Charlie".to_string()));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_literal_to_value() {
        assert_eq!(
            literal_to_value(&Literal::String("test".to_string())),
            Value::String("test".to_string())
        );
        assert_eq!(literal_to_value(&Literal::Int(42)), Value::Int(42));
        assert_eq!(literal_to_value(&Literal::Float(3.14)), Value::Float(3.14));
        assert_eq!(literal_to_value(&Literal::Bool(true)), Value::Bool(true));
        assert_eq!(literal_to_value(&Literal::Null), Value::Null);
    }

    // ========================================================================
    // Anonymous Traversal Tests
    // ========================================================================

    #[test]
    fn test_compile_anonymous_identity() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.identity() in where should pass through all
        let ast = parse("g.V().hasLabel('person').where(__.identity()).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 3); // All persons pass identity filter
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_has_label() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Filter with __.hasLabel in where
        let ast = parse("g.V().where(__.hasLabel('person')).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 3); // Only person vertices
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Bob".to_string())));
            assert!(values.contains(&Value::String("Charlie".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_chained() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Chained steps in anonymous traversal
        let ast = parse(
            "g.V().has('name', 'Alice').where(__.out('knows').has('name', 'Bob')).values('name')",
        )
        .unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], Value::String("Alice".to_string()));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_not() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // not(__.out('knows')) - vertices without outgoing knows edges
        let ast = parse("g.V().hasLabel('person').not(__.out('knows')).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Charlie has no outgoing 'knows' edges
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], Value::String("Charlie".to_string()));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_coalesce() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // coalesce tries first branch, falls back to second
        let ast =
            parse("g.V().hasLabel('person').coalesce(__.values('nickname'), __.values('name'))")
                .unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // No 'nickname' property, so falls back to 'name'
            assert_eq!(values.len(), 3);
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Bob".to_string())));
            assert!(values.contains(&Value::String("Charlie".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_optional() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // optional returns the result if exists, otherwise the input
        let ast =
            parse("g.V().has('name', 'Charlie').optional(__.out('knows')).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Charlie has no outgoing 'knows', so optional returns Charlie itself
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], Value::String("Charlie".to_string()));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_choose() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // choose with condition
        let ast = parse("g.V().hasLabel('person').choose(__.has('age', P.gt(28)), __.values('name'), __.constant('young'))").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 3);
            // Alice (30) and Charlie (35) are > 28, Bob (25) is not
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Charlie".to_string())));
            assert!(values.contains(&Value::String("young".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_local() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // local limits within each traverser's scope
        let ast = parse("g.V().hasLabel('person').local(__.out('knows').limit(1)).values('name')")
            .unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Alice has 1 knows (Bob), Bob has 1 knows (Charlie), Charlie has 0
            // With limit(1) local, we get at most 1 per person
            assert!(values.len() <= 2);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_nested() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Nested anonymous traversals via union containing out chains
        let ast = parse("g.V().has('name', 'Alice').union(__.out('knows').out('knows'), __.out('created')).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Alice -> Bob -> Charlie (via knows chain)
            // Alice -> Interstellar (via created)
            assert_eq!(values.len(), 2);
            assert!(values.contains(&Value::String("Charlie".to_string())));
            assert!(values.contains(&Value::String("Interstellar".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_values() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.values() in union
        let ast =
            parse("g.V().has('name', 'Alice').union(__.values('name'), __.values('age'))").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 2);
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::Int(30)));
        } else {
            panic!("Expected List result");
        }
    }

    // ========================================================================
    // Anonymous Traversal Filter Step Tests
    // ========================================================================

    #[test]
    fn test_compile_anonymous_tail() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.tail() in local scope - get last out neighbor for each vertex
        let ast = parse("g.V().hasLabel('person').local(__.out('knows').tail(1)).values('name')")
            .unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Alice has 1 out-knows (Bob), Bob has 1 (Charlie), Charlie has 0
            // tail(1) from each = at most 2 results
            assert!(values.len() <= 2);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_is() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.is() with value
        let ast = parse("g.V().hasLabel('person').values('age').where(__.is(30))").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], Value::Int(30));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_is_predicate() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.is() with predicate
        let ast = parse("g.V().hasLabel('person').values('age').where(__.is(P.gte(30)))").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 2); // Alice (30) and Charlie (35)
            assert!(values.contains(&Value::Int(30)));
            assert!(values.contains(&Value::Int(35)));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_and() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.and() combining conditions
        let ast = parse(
            "g.V().where(__.and(__.hasLabel('person'), __.has('age', P.gt(25)))).values('name')",
        )
        .unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 2); // Alice (30) and Charlie (35)
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Charlie".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_or() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.or() combining conditions
        let ast = parse(
            "g.V().where(__.or(__.has('name', 'Alice'), __.has('name', 'Charlie'))).values('name')",
        )
        .unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 2);
            assert!(values.contains(&Value::String("Alice".to_string())));
            assert!(values.contains(&Value::String("Charlie".to_string())));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_coin() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.coin() with 100% probability should return all
        let ast = parse("g.V().hasLabel('person').where(__.coin(1.0)).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 3); // All persons pass with 100% coin
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_sample() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.sample() in local scope
        let ast = parse("g.V().hasLabel('person').local(__.out('knows').sample(1)).values('name')")
            .unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Alice has 1 out-knows, Bob has 1, Charlie has 0
            // Sample(1) from each = at most 2 results
            assert!(values.len() <= 2);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_simple_path() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.simplePath() filters cyclic paths
        let ast = parse(
            "g.V().has('name', 'Alice').as('a').out().out().where(__.simplePath()).values('name')",
        )
        .unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        // Alice -> Bob -> Charlie is a simple path
        if let ExecutionResult::List(values) = result {
            assert!(values.len() <= 1); // Should only get Charlie via simple path
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_where_nested() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Nested where in anonymous traversal
        let ast =
            parse("g.V().where(__.out('knows').where(__.has('age', P.gt(30)))).values('name')")
                .unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // Bob knows Charlie (age 35 > 30)
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], Value::String("Bob".to_string()));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    fn test_compile_anonymous_has_key() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // __.has('key') checks property existence (hasKey is for property objects)
        // Use __.has('age') to filter vertices that have the 'age' property
        let ast = parse("g.V().where(__.has('age')).values('name')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            // All persons have 'age' property
            assert_eq!(values.len(), 3);
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    #[cfg(feature = "gql")]
    fn test_compile_math_simple() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // math('_ + 5') adds 5 to each age value
        // Alice=30, Bob=25, Charlie=35 -> 35, 30, 40
        let ast = parse("g.V().hasLabel('person').values('age').math('_ + 5')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 3);
            // Check all expected values are present (order may vary)
            let nums: Vec<f64> = values
                .iter()
                .filter_map(|v| match v {
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            assert!(nums.contains(&35.0));
            assert!(nums.contains(&30.0));
            assert!(nums.contains(&40.0));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    #[cfg(feature = "gql")]
    fn test_compile_math_multiplication() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // math('_ * 2') doubles each age value
        // Alice=30, Bob=25, Charlie=35 -> 60, 50, 70
        let ast = parse("g.V().hasLabel('person').values('age').math('_ * 2')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 3);
            let nums: Vec<f64> = values
                .iter()
                .filter_map(|v| match v {
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            assert!(nums.contains(&60.0));
            assert!(nums.contains(&50.0));
            assert!(nums.contains(&70.0));
        } else {
            panic!("Expected List result");
        }
    }

    #[test]
    #[cfg(feature = "gql")]
    fn test_compile_math_no_by() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // math() without by() should still compile
        let ast = parse("g.V().hasLabel('person').values('age').math('_ - 10')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 3);
            // Alice=30-10=20, Bob=25-10=15, Charlie=35-10=25
            let nums: Vec<f64> = values
                .iter()
                .filter_map(|v| match v {
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            assert!(nums.contains(&20.0));
            assert!(nums.contains(&15.0));
            assert!(nums.contains(&25.0));
        } else {
            panic!("Expected List result");
        }
    }

    // =========================================================================
    // Mutation Compilation Tests
    //
    // NOTE: These tests verify that mutations PARSE and COMPILE correctly.
    // They do NOT verify actual execution because the Gremlin text parser
    // uses the read-only GraphTraversalSource. Mutations return placeholder
    // values (maps with __pending_add_v, etc.).
    //
    // For actual mutations via Gremlin text, use the Rust API:
    //   let g = graph.gremlin(Arc::clone(&graph));  // CowTraversalSource
    //   g.add_v("Person").property("name", "Alice").next();
    // =========================================================================

    #[test]
    fn test_compile_add_e_source_with_labels() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Test g.addE('test').from('a').to('b') pattern
        // Verifies parser/compiler handles addE as source step
        let ast = parse("g.addE('test_edge').from('a').to('b')").unwrap();
        let result = compile(&ast, &g);

        // Should compile successfully
        assert!(
            result.is_ok(),
            "addE source compilation failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_compile_add_e_inline_with_labels() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Test mid-traversal addE pattern
        let ast =
            parse("g.V().hasLabel('person').as('a').out('knows').as('b').addE('friend').from('a').to('b')")
                .unwrap();
        let result = compile(&ast, &g);

        // Should compile successfully
        assert!(
            result.is_ok(),
            "addE inline compilation failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_compile_add_e_with_property() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Test addE with property
        let ast = parse("g.addE('test').from('a').to('b').property('since', 2020)").unwrap();
        let result = compile(&ast, &g);

        assert!(
            result.is_ok(),
            "addE with property compilation failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_compile_drop() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Test drop() step compiles correctly
        let ast = parse("g.V().hasLabel('software').drop()").unwrap();
        let result = compile(&ast, &g);

        assert!(
            result.is_ok(),
            "drop compilation failed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_compile_add_v_with_property() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        // Test addV with property compiles and returns placeholder
        let ast = parse("g.addV('test').property('name', 'TestNode')").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        // Returns a placeholder map since we're using read-only GraphTraversalSource.
        // For actual mutations, use Graph::gremlin(Arc::clone(&graph)) with CowTraversalSource.
        if let ExecutionResult::List(values) = result {
            assert_eq!(values.len(), 1);
            // Verify it's a pending mutation placeholder
            if let Value::Map(map) = &values[0] {
                assert!(
                    map.contains_key("__pending_add_v"),
                    "Expected pending mutation placeholder, got: {:?}",
                    map
                );
            } else {
                panic!("Expected Map placeholder, got: {:?}", values[0]);
            }
        } else {
            panic!("Expected List result");
        }
    }

    // ========================================================================
    // Explain Terminal Tests
    // ========================================================================

    #[test]
    fn test_parse_explain_terminal() {
        let ast = parse("g.V().out('knows').explain()").unwrap();
        assert!(matches!(ast.terminal, Some(TerminalStep::Explain { .. })));
    }

    #[test]
    fn test_compile_explain_basic() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().hasLabel('person').out('knows').values('name').explain()").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::Explain(text) = result {
            assert!(text.contains("Traversal Explanation"), "Output:\n{text}");
            assert!(text.contains("hasLabel"), "Output:\n{text}");
            assert!(text.contains("out"), "Output:\n{text}");
            assert!(text.contains("values"), "Output:\n{text}");
        } else {
            panic!("Expected Explain result, got: {:?}", result);
        }
    }

    #[test]
    fn test_compile_explain_empty_traversal() {
        let graph = test_graph();
        let snapshot = graph.snapshot();
        let g = snapshot.gremlin();

        let ast = parse("g.V().explain()").unwrap();
        let compiled = compile(&ast, &g).unwrap();
        let result = compiled.execute();

        if let ExecutionResult::Explain(text) = result {
            assert!(text.contains("Traversal Explanation"));
        } else {
            panic!("Expected Explain result");
        }
    }
}
