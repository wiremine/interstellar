//! Error types for the Gremlin parser and compiler.

use crate::gremlin::ast::Span;
use thiserror::Error;

/// Top-level Gremlin error encompassing parse, compile, and execution errors.
#[derive(Debug, Error)]
pub enum GremlinError {
    /// Error during parsing
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),

    /// Error during compilation
    #[error("Compile error: {0}")]
    Compile(#[from] CompileError),

    /// Error during execution
    #[error("Execution error: {0}")]
    Execution(String),
}

/// Parse errors with source location information.
#[derive(Debug, Error)]
pub enum ParseError {
    /// Syntax error at a specific position
    #[error("Syntax error at position {span:?}: {message}")]
    SyntaxAt { span: Span, message: String },

    /// General syntax error
    #[error("Syntax error: {0}")]
    Syntax(String),

    /// Empty query provided
    #[error("Empty query")]
    Empty,

    /// Invalid literal value
    #[error("Invalid literal '{value}' at {span:?}: {reason}")]
    InvalidLiteral {
        value: String,
        span: Span,
        reason: &'static str,
    },

    /// Unexpected token encountered
    #[error("Unexpected token at {span:?}: found '{found}', expected {expected}")]
    UnexpectedToken {
        span: Span,
        found: String,
        expected: String,
    },

    /// Missing source step (query must start with g.V(), g.E(), etc.)
    #[error("Missing source step (query must start with g.V(), g.E(), etc.)")]
    MissingSource,

    /// Invalid step
    #[error("Invalid step '{step}' at {span:?}: {reason}")]
    InvalidStep {
        step: String,
        span: Span,
        reason: String,
    },
}

/// Compile errors during AST to traversal compilation.
#[derive(Debug, Error)]
pub enum CompileError {
    /// Step is not yet supported
    #[error("Unsupported step: {step}")]
    UnsupportedStep { step: String },

    /// Invalid arguments for a step
    #[error("Invalid arguments for {step}: {message}")]
    InvalidArguments { step: String, message: String },

    /// Type mismatch during compilation
    #[error("Type mismatch: {message}")]
    TypeMismatch { message: String },

    /// Reference to undefined label
    #[error("Undefined label: '{label}'")]
    UndefinedLabel { label: String },

    /// Invalid predicate construction
    #[error("Invalid predicate: {message}")]
    InvalidPredicate { message: String },

    /// Step requires a preceding step that wasn't found
    #[error("Step '{step}' requires preceding '{required}'")]
    MissingPrecedingStep { step: String, required: String },
}

impl ParseError {
    /// Create a syntax error at a specific span
    pub fn at(span: Span, message: impl Into<String>) -> Self {
        ParseError::SyntaxAt {
            span,
            message: message.into(),
        }
    }

    /// Create a syntax error from a pest parsing error
    pub fn from_pest(error: pest::error::Error<crate::gremlin::parser::Rule>) -> Self {
        ParseError::Syntax(error.to_string())
    }
}
