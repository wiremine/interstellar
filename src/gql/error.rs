//! Error types for GQL parsing and compilation.
//!
//! This module defines the error types that can occur during GQL query
//! processing. Errors are separated into two categories:
//!
//! - [`ParseError`] - Errors during query parsing (syntax errors, invalid literals, etc.)
//! - [`CompileError`] - Errors during compilation to traversals (undefined variables, etc.)
//!
//! Both error types can be wrapped in the top-level [`GqlError`] type.
//!
//! # Error Location
//!
//! Parse errors include [`Span`] information indicating the position in the
//! source query where the error occurred. This helps users identify and fix
//! syntax errors.
//!
//! # Examples
//!
//! ## Handling parse errors
//!
//! ```
//! use intersteller::gql::{parse, ParseError};
//!
//! match parse("MATCH (n) RETURN") {
//!     Ok(_) => println!("Query parsed successfully"),
//!     Err(ParseError::MissingClauseLegacy(clause)) => {
//!         println!("Missing clause: {}", clause);
//!     }
//!     Err(e) => println!("Parse error: {}", e),
//! }
//! ```
//!
//! ## Handling compile errors
//!
//! ```
//! use intersteller::gql::{parse, compile, CompileError};
//! use intersteller::Graph;
//!
//! let graph = Graph::in_memory();
//! let snapshot = graph.snapshot();
//!
//! let query = parse("MATCH (n:Person) RETURN x").unwrap();
//! match compile(&query, &snapshot) {
//!     Ok(_) => println!("Query executed successfully"),
//!     Err(CompileError::UndefinedVariable { name }) => {
//!         println!("Variable '{}' is not defined in MATCH", name);
//!     }
//!     Err(e) => println!("Compile error: {}", e),
//! }
//! ```

use thiserror::Error;

/// Top-level GQL error type
#[derive(Debug, Error)]
pub enum GqlError {
    #[error("Parse error: {0}")]
    Parse(#[from] ParseError),

    #[error("Compile error: {0}")]
    Compile(#[from] CompileError),
}

/// Source span information for error locations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// Starting byte position (0-indexed)
    pub start: usize,
    /// Ending byte position (exclusive)
    pub end: usize,
}

impl Span {
    /// Create a new span
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Create a span at a single position
    pub fn at(position: usize) -> Self {
        Self {
            start: position,
            end: position,
        }
    }
}

impl std::fmt::Display for Span {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.start == self.end {
            write!(f, "position {}", self.start)
        } else {
            write!(f, "positions {}..{}", self.start, self.end)
        }
    }
}

/// Errors during parsing
#[derive(Debug, Error)]
pub enum ParseError {
    /// Syntax error with position information
    #[error("Syntax error at {span}: {message}")]
    SyntaxAt { span: Span, message: String },

    /// Legacy syntax error (for backward compatibility with pest errors)
    #[error("Syntax error: {0}")]
    Syntax(String),

    /// Empty input provided
    #[error("Empty input: query string is empty or contains only whitespace")]
    Empty,

    /// Missing required clause
    #[error("Missing {clause} clause at {span}")]
    MissingClause { clause: &'static str, span: Span },

    /// Missing clause (legacy variant without position)
    #[error("Missing {0} clause")]
    MissingClauseLegacy(&'static str),

    /// Invalid literal value
    #[error("Invalid literal '{value}' at {span}: {reason}")]
    InvalidLiteral {
        value: String,
        span: Span,
        reason: &'static str,
    },

    /// Invalid literal (legacy variant)
    #[error("Invalid literal: {0}")]
    InvalidLiteralLegacy(String),

    /// Unexpected token
    #[error("Unexpected token '{found}' at {span}, expected {expected}")]
    UnexpectedToken {
        span: Span,
        found: String,
        expected: String,
    },

    /// Unexpected end of input
    #[error("Unexpected end of input at {span}, expected {expected}")]
    UnexpectedEof { span: Span, expected: String },

    /// Invalid range specification
    #[error("Invalid range '{range}' at {span}: {reason}")]
    InvalidRange {
        range: String,
        span: Span,
        reason: &'static str,
    },
}

impl ParseError {
    /// Create a syntax error with position
    pub fn syntax_at(span: Span, message: impl Into<String>) -> Self {
        ParseError::SyntaxAt {
            span,
            message: message.into(),
        }
    }

    /// Create a missing clause error with position
    pub fn missing_clause(clause: &'static str, span: Span) -> Self {
        ParseError::MissingClause { clause, span }
    }

    /// Create an invalid literal error with position
    pub fn invalid_literal(value: impl Into<String>, span: Span, reason: &'static str) -> Self {
        ParseError::InvalidLiteral {
            value: value.into(),
            span,
            reason,
        }
    }

    /// Create an unexpected token error
    pub fn unexpected_token(
        span: Span,
        found: impl Into<String>,
        expected: impl Into<String>,
    ) -> Self {
        ParseError::UnexpectedToken {
            span,
            found: found.into(),
            expected: expected.into(),
        }
    }

    /// Create an invalid range error
    pub fn invalid_range(range: impl Into<String>, span: Span, reason: &'static str) -> Self {
        ParseError::InvalidRange {
            range: range.into(),
            span,
            reason,
        }
    }

    /// Get the span of this error, if available
    pub fn span(&self) -> Option<Span> {
        match self {
            ParseError::SyntaxAt { span, .. } => Some(*span),
            ParseError::MissingClause { span, .. } => Some(*span),
            ParseError::InvalidLiteral { span, .. } => Some(*span),
            ParseError::UnexpectedToken { span, .. } => Some(*span),
            ParseError::UnexpectedEof { span, .. } => Some(*span),
            ParseError::InvalidRange { span, .. } => Some(*span),
            ParseError::Syntax(_)
            | ParseError::Empty
            | ParseError::MissingClauseLegacy(_)
            | ParseError::InvalidLiteralLegacy(_) => None,
        }
    }
}

/// Errors during compilation to traversal
#[derive(Debug, Error)]
pub enum CompileError {
    /// Reference to undefined variable
    #[error("Undefined variable '{name}'. Did you forget to bind it in MATCH?")]
    UndefinedVariable { name: String },

    /// Duplicate variable binding
    #[error("Variable '{name}' is already defined. Use a different name or reference the existing binding.")]
    DuplicateVariable { name: String },

    /// Empty pattern in MATCH clause
    #[error("Empty pattern: MATCH clause requires at least one node pattern like (n)")]
    EmptyPattern,

    /// Pattern must start with a node
    #[error("Pattern must start with a node: found edge pattern without preceding node. Start with (n) before -[e]->")]
    PatternMustStartWithNode,

    /// Unsupported expression type
    #[error("Unsupported expression '{expr}' in this context")]
    UnsupportedExpression { expr: String },

    /// Unsupported expression (legacy variant)
    #[error("Unsupported expression in context")]
    UnsupportedExpressionLegacy,

    /// Aggregate function in WHERE clause
    #[error("Aggregate function {func}() cannot be used in WHERE clause. Use HAVING or compute in RETURN instead.")]
    AggregateInWhere { func: String },

    /// Aggregate in WHERE (legacy variant)
    #[error("Aggregates not allowed in WHERE clause")]
    AggregateInWhereLegacy,

    /// Invalid property access
    #[error("Invalid property access on '{variable}': variable is not bound to a node or edge")]
    InvalidPropertyAccess { variable: String },

    /// Unsupported aggregation
    #[error(
        "Unsupported aggregation function '{func}'. Supported: COUNT, SUM, AVG, MIN, MAX, COLLECT"
    )]
    UnsupportedAggregation { func: String },

    /// Type mismatch in expression
    #[error("Type mismatch: {message}")]
    TypeMismatch { message: String },

    /// Non-aggregated expression not in GROUP BY
    #[error(
        "Expression '{expr}' must appear in GROUP BY clause or be used in an aggregate function"
    )]
    ExpressionNotInGroupBy { expr: String },

    /// Unsupported feature (e.g., mutations on immutable snapshot)
    #[error("Unsupported: {0}")]
    UnsupportedFeature(String),

    /// Unbound parameter reference
    #[error("Unbound parameter: ${0}")]
    UnboundParameter(String),
}

impl CompileError {
    /// Create an undefined variable error
    pub fn undefined_variable(name: impl Into<String>) -> Self {
        CompileError::UndefinedVariable { name: name.into() }
    }

    /// Create a duplicate variable error
    pub fn duplicate_variable(name: impl Into<String>) -> Self {
        CompileError::DuplicateVariable { name: name.into() }
    }

    /// Create an unsupported expression error
    pub fn unsupported_expression(expr: impl Into<String>) -> Self {
        CompileError::UnsupportedExpression { expr: expr.into() }
    }

    /// Create an aggregate in WHERE error
    pub fn aggregate_in_where(func: impl Into<String>) -> Self {
        CompileError::AggregateInWhere { func: func.into() }
    }

    /// Create an invalid property access error
    pub fn invalid_property_access(variable: impl Into<String>) -> Self {
        CompileError::InvalidPropertyAccess {
            variable: variable.into(),
        }
    }

    /// Create an unsupported aggregation error
    pub fn unsupported_aggregation(func: impl Into<String>) -> Self {
        CompileError::UnsupportedAggregation { func: func.into() }
    }

    /// Create a type mismatch error
    pub fn type_mismatch(message: impl Into<String>) -> Self {
        CompileError::TypeMismatch {
            message: message.into(),
        }
    }

    /// Create an expression not in GROUP BY error
    pub fn expression_not_in_group_by(expr: impl Into<String>) -> Self {
        CompileError::ExpressionNotInGroupBy { expr: expr.into() }
    }

    /// Create an unbound parameter error
    pub fn unbound_parameter(name: impl Into<String>) -> Self {
        CompileError::UnboundParameter(name.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_span_display() {
        let span = Span::new(10, 20);
        assert_eq!(format!("{}", span), "positions 10..20");

        let span = Span::at(5);
        assert_eq!(format!("{}", span), "position 5");
    }

    #[test]
    fn test_parse_error_messages() {
        let err = ParseError::syntax_at(Span::new(0, 5), "unexpected keyword");
        assert!(format!("{}", err).contains("position"));
        assert!(format!("{}", err).contains("unexpected keyword"));

        let err = ParseError::missing_clause("RETURN", Span::at(10));
        assert!(format!("{}", err).contains("RETURN"));

        let err = ParseError::invalid_literal("abc", Span::new(5, 8), "expected integer");
        assert!(format!("{}", err).contains("abc"));
        assert!(format!("{}", err).contains("expected integer"));

        let err = ParseError::unexpected_token(Span::at(3), "}", "identifier");
        assert!(format!("{}", err).contains("}"));
        assert!(format!("{}", err).contains("identifier"));
    }

    #[test]
    fn test_compile_error_messages() {
        let err = CompileError::undefined_variable("x");
        let msg = format!("{}", err);
        assert!(msg.contains("x"));
        assert!(msg.contains("Did you forget"));

        let err = CompileError::duplicate_variable("n");
        let msg = format!("{}", err);
        assert!(msg.contains("n"));
        assert!(msg.contains("already defined"));

        let err = CompileError::aggregate_in_where("COUNT");
        let msg = format!("{}", err);
        assert!(msg.contains("COUNT"));
        assert!(msg.contains("WHERE"));
    }

    #[test]
    fn test_parse_error_span_extraction() {
        let err = ParseError::syntax_at(Span::new(5, 10), "test");
        assert_eq!(err.span(), Some(Span::new(5, 10)));

        let err = ParseError::Syntax("test".to_string());
        assert_eq!(err.span(), None);

        let err = ParseError::Empty;
        assert_eq!(err.span(), None);
    }
}
