//! Error types for GQL parsing and compilation.

use thiserror::Error;

/// Top-level GQL error type
#[derive(Debug, Error)]
pub enum GqlError {
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("Compile error: {0}")]
    Compile(#[from] CompileError),
}

/// Errors during parsing
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Syntax error: {0}")]
    Syntax(String),

    #[error("Empty input")]
    Empty,

    #[error("Missing {0} clause")]
    MissingClause(&'static str),

    #[error("Invalid literal: {0}")]
    InvalidLiteral(String),
}

/// Errors during compilation to traversal
#[derive(Debug, Error)]
pub enum CompileError {
    #[error("Undefined variable: {0}")]
    UndefinedVariable(String),

    #[error("Variable already defined: {0}")]
    DuplicateVariable(String),

    #[error("Empty pattern")]
    EmptyPattern,

    #[error("Pattern must start with a node")]
    PatternMustStartWithNode,

    #[error("Unsupported expression in context")]
    UnsupportedExpression,

    #[error("Aggregates not allowed in WHERE clause")]
    AggregateInWhere,
}
