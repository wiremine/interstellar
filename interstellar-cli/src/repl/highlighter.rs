//! Syntax highlighting for GQL and Gremlin queries in the REPL.
//!
//! Highlights:
//! - Keywords (blue)
//! - String literals (green)
//! - Numbers (yellow)
//! - Labels (cyan)
//! - Methods/functions (magenta) - Gremlin only

use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use colored::Colorize;
use rustyline::highlight::Highlighter;

use crate::config::QueryMode;

/// GQL keywords to highlight.
const GQL_KEYWORDS: &[&str] = &[
    "MATCH", "WHERE", "RETURN", "CREATE", "DELETE", "DETACH", "SET", "REMOVE", "ORDER", "BY",
    "ASC", "DESC", "LIMIT", "SKIP", "OPTIONAL", "WITH", "UNION", "ALL", "UNWIND", "AS", "DISTINCT",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "AND", "OR", "NOT", "IN", "IS", "NULL", "TRUE", "FALSE",
];

/// Gremlin traversal methods to highlight (TinkerPop-style).
const GREMLIN_METHODS: &[&str] = &[
    // Source steps
    "V",
    "E",
    "addV",
    "addE",
    "inject",
    // Navigation
    "out",
    "in",
    "both",
    "outE",
    "inE",
    "bothE",
    "outV",
    "inV",
    "bothV",
    "otherV",
    // Filter
    "has",
    "hasLabel",
    "hasId",
    "hasNot",
    "hasKey",
    "hasValue",
    "where",
    "is",
    "and",
    "or",
    "not",
    "dedup",
    "limit",
    "skip",
    "range",
    "tail",
    "coin",
    "sample",
    "simplePath",
    "cyclicPath",
    // Transform
    "values",
    "valueMap",
    "elementMap",
    "propertyMap",
    "id",
    "label",
    "key",
    "value",
    "path",
    "select",
    "project",
    "by",
    "unfold",
    "fold",
    "count",
    "sum",
    "max",
    "min",
    "mean",
    "order",
    "math",
    "constant",
    "identity",
    "index",
    "loops",
    // Branch
    "choose",
    "union",
    "coalesce",
    "optional",
    "local",
    "branch",
    "option",
    // Repeat
    "repeat",
    "times",
    "until",
    "emit",
    // Side effect
    "as",
    "aggregate",
    "store",
    "cap",
    "sideEffect",
    "profile",
    // Mutation
    "property",
    "from",
    "to",
    "drop",
    // Terminal
    "toList",
    "toSet",
    "next",
    "iterate",
    "hasNext",
];

/// Gremlin predicate functions to highlight (TinkerPop-style).
const GREMLIN_PREDICATES: &[&str] = &[
    // P predicates
    "eq",
    "neq",
    "lt",
    "lte",
    "gt",
    "gte",
    "between",
    "inside",
    "outside",
    "within",
    "without",
    // TextP predicates
    "containing",
    "notContaining",
    "startingWith",
    "notStartingWith",
    "endingWith",
    "notEndingWith",
    "regex",
];

/// Gremlin keywords and special identifiers.
const GREMLIN_KEYWORDS: &[&str] = &[
    "true", "false", "null", // Predicate namespaces
    "P", "TextP", // Anonymous traversal
    "__",
];

/// Syntax highlighter for GQL queries.
#[derive(Debug, Clone)]
pub struct GqlHighlighter {
    pub enabled: bool,
}

impl GqlHighlighter {
    /// Create a new GQL highlighter.
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }
}

impl Highlighter for GqlHighlighter {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        if !self.enabled {
            return Cow::Borrowed(line);
        }

        let mut result = String::with_capacity(line.len() * 2);
        let chars: Vec<char> = line.chars().collect();
        let len = chars.len();
        let mut i = 0;

        while i < len {
            let c = chars[i];

            // String literal (single or double quotes)
            if c == '\'' || c == '"' {
                let quote = c;
                let start = i;
                i += 1;

                // Find the end of the string
                while i < len && chars[i] != quote {
                    if chars[i] == '\\' && i + 1 < len {
                        i += 1; // Skip escaped character
                    }
                    i += 1;
                }
                if i < len {
                    i += 1; // Include closing quote
                }

                let string_literal: String = chars[start..i].iter().collect();
                result.push_str(&string_literal.green().to_string());
                continue;
            }

            // Number
            if c.is_ascii_digit() || (c == '-' && i + 1 < len && chars[i + 1].is_ascii_digit()) {
                let start = i;
                if c == '-' {
                    i += 1;
                }
                while i < len && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let number: String = chars[start..i].iter().collect();
                result.push_str(&number.yellow().to_string());
                continue;
            }

            // Label (after colon)
            if c == ':' {
                result.push(c);
                i += 1;

                // Collect the label name
                let start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }

                if i > start {
                    let label: String = chars[start..i].iter().collect();
                    result.push_str(&label.cyan().to_string());
                }
                continue;
            }

            // Word (potential keyword or identifier)
            if c.is_alphabetic() || c == '_' {
                let start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }

                let word: String = chars[start..i].iter().collect();
                let upper = word.to_uppercase();

                if GQL_KEYWORDS.contains(&upper.as_str()) {
                    result.push_str(&word.blue().bold().to_string());
                } else {
                    result.push_str(&word);
                }
                continue;
            }

            // Comment (-- or //)
            if c == '-' && i + 1 < len && chars[i + 1] == '-' {
                let rest: String = chars[i..].iter().collect();
                result.push_str(&rest.dimmed().to_string());
                break;
            }
            if c == '/' && i + 1 < len && chars[i + 1] == '/' {
                let rest: String = chars[i..].iter().collect();
                result.push_str(&rest.dimmed().to_string());
                break;
            }

            // Other characters
            result.push(c);
            i += 1;
        }

        Cow::Owned(result)
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        self.enabled
    }
}

/// Syntax highlighter for Gremlin queries.
#[derive(Debug, Clone)]
pub struct GremlinHighlighter {
    pub enabled: bool,
}

impl GremlinHighlighter {
    /// Create a new Gremlin highlighter.
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }
}

impl Highlighter for GremlinHighlighter {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        if !self.enabled {
            return Cow::Borrowed(line);
        }

        let mut result = String::with_capacity(line.len() * 2);
        let chars: Vec<char> = line.chars().collect();
        let len = chars.len();
        let mut i = 0;

        while i < len {
            let c = chars[i];

            // String literal (single or double quotes)
            if c == '\'' || c == '"' {
                let quote = c;
                let start = i;
                i += 1;

                // Find the end of the string
                while i < len && chars[i] != quote {
                    if chars[i] == '\\' && i + 1 < len {
                        i += 1; // Skip escaped character
                    }
                    i += 1;
                }
                if i < len {
                    i += 1; // Include closing quote
                }

                let string_literal: String = chars[start..i].iter().collect();
                result.push_str(&string_literal.green().to_string());
                continue;
            }

            // Number
            if c.is_ascii_digit() || (c == '-' && i + 1 < len && chars[i + 1].is_ascii_digit()) {
                let start = i;
                if c == '-' {
                    i += 1;
                }
                while i < len && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let number: String = chars[start..i].iter().collect();
                result.push_str(&number.yellow().to_string());
                continue;
            }

            // The 'g' traversal source - special case
            if c == 'g' && (i == 0 || !chars[i - 1].is_alphanumeric()) {
                // Check if followed by '.' (g.v(), g.e())
                if i + 1 < len && chars[i + 1] == '.' {
                    result.push_str(&"g".cyan().bold().to_string());
                    i += 1;
                    continue;
                }
            }

            // Word (potential keyword, method, or identifier)
            if c.is_alphabetic() || c == '_' {
                let start = i;
                while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }

                let word: String = chars[start..i].iter().collect();

                // Check if this is a method call (followed by '(')
                let is_method = i < len && chars[i] == '(';
                // Check if preceded by '.' (chained method)
                let after_dot = start > 0 && chars[start - 1] == '.';

                if GREMLIN_KEYWORDS.contains(&word.as_str()) {
                    result.push_str(&word.blue().bold().to_string());
                } else if (is_method || after_dot) && GREMLIN_METHODS.contains(&word.as_str()) {
                    result.push_str(&word.magenta().to_string());
                } else if is_method && GREMLIN_PREDICATES.contains(&word.as_str()) {
                    // Predicates like gt(), eq(), within()
                    result.push_str(&word.yellow().bold().to_string());
                } else {
                    result.push_str(&word);
                }
                continue;
            }

            // Comment (//)
            if c == '/' && i + 1 < len && chars[i + 1] == '/' {
                let rest: String = chars[i..].iter().collect();
                result.push_str(&rest.dimmed().to_string());
                break;
            }

            // Other characters
            result.push(c);
            i += 1;
        }

        Cow::Owned(result)
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        self.enabled
    }
}

/// Combined query highlighter that switches based on mode.
#[derive(Clone)]
pub struct QueryHighlighter {
    gql: GqlHighlighter,
    gremlin: GremlinHighlighter,
    is_gremlin_mode: Arc<AtomicBool>,
}

impl QueryHighlighter {
    /// Create a new query highlighter.
    pub fn new(enabled: bool, mode: QueryMode) -> Self {
        Self {
            gql: GqlHighlighter::new(enabled),
            gremlin: GremlinHighlighter::new(enabled),
            is_gremlin_mode: Arc::new(AtomicBool::new(mode == QueryMode::Gremlin)),
        }
    }

    /// Set the current query mode.
    #[allow(dead_code)]
    pub fn set_mode(&self, mode: QueryMode) {
        self.is_gremlin_mode
            .store(mode == QueryMode::Gremlin, Ordering::SeqCst);
    }

    /// Get a clone of the mode flag for sharing.
    pub fn mode_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.is_gremlin_mode)
    }

    /// Check if highlighting is enabled.
    pub fn is_enabled(&self) -> bool {
        self.gql.enabled
    }
}

impl Highlighter for QueryHighlighter {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        if self.is_gremlin_mode.load(Ordering::SeqCst) {
            self.gremlin.highlight(line, pos)
        } else {
            self.gql.highlight(line, pos)
        }
    }

    fn highlight_char(&self, line: &str, pos: usize, forced: bool) -> bool {
        if self.is_gremlin_mode.load(Ordering::SeqCst) {
            self.gremlin.highlight_char(line, pos, forced)
        } else {
            self.gql.highlight_char(line, pos, forced)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_highlighter_disabled() {
        let highlighter = GqlHighlighter::new(false);
        let line = "MATCH (n) RETURN n";
        let result = highlighter.highlight(line, 0);
        assert_eq!(result, line);
    }

    #[test]
    fn test_highlighter_enabled() {
        // Enable color output even when not a TTY (for testing)
        colored::control::set_override(true);

        let highlighter = GqlHighlighter::new(true);
        let line = "MATCH (n:Person) RETURN n";
        let result = highlighter.highlight(line, 0);
        // Result should be different due to ANSI codes
        assert_ne!(result.as_ref(), line);

        // Reset color override
        colored::control::unset_override();
    }

    #[test]
    fn test_gremlin_highlighter_disabled() {
        let highlighter = GremlinHighlighter::new(false);
        let line = "g.V().hasLabel('person').toList()";
        let result = highlighter.highlight(line, 0);
        assert_eq!(result, line);
    }

    #[test]
    fn test_gremlin_highlighter_enabled() {
        colored::control::set_override(true);

        let highlighter = GremlinHighlighter::new(true);
        let line = "g.V().hasLabel('person').toList()";
        let result = highlighter.highlight(line, 0);
        // Result should be different due to ANSI codes
        assert_ne!(result.as_ref(), line);

        colored::control::unset_override();
    }

    #[test]
    fn test_query_highlighter_mode_switch() {
        // Test that mode switching works - check mode flag is correctly set
        let highlighter = QueryHighlighter::new(true, QueryMode::Gql);

        // Should start in GQL mode
        assert!(!highlighter
            .is_gremlin_mode
            .load(std::sync::atomic::Ordering::SeqCst));

        // Switch to Gremlin mode
        highlighter.set_mode(QueryMode::Gremlin);
        assert!(highlighter
            .is_gremlin_mode
            .load(std::sync::atomic::Ordering::SeqCst));

        // Switch back to GQL mode
        highlighter.set_mode(QueryMode::Gql);
        assert!(!highlighter
            .is_gremlin_mode
            .load(std::sync::atomic::Ordering::SeqCst));
    }
}
