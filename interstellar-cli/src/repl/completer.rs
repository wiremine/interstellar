//! Tab completion for the REPL.
//!
//! Provides completion for:
//! - Dot-commands when line starts with "."
//! - GQL keywords (MATCH, RETURN, WHERE, etc.)
//! - Vertex and edge labels from the database schema

use rustyline::completion::{Completer, Pair};
use rustyline::Context;

use super::GraphLabels;

/// GQL keywords for completion.
const GQL_KEYWORDS: &[&str] = &[
    "MATCH", "WHERE", "RETURN", "CREATE", "DELETE", "DETACH", "SET", "REMOVE", "ORDER", "BY",
    "ASC", "DESC", "LIMIT", "SKIP", "OPTIONAL", "WITH", "UNION", "ALL", "UNWIND", "AS", "DISTINCT",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "AND", "OR", "NOT", "IN", "IS", "NULL", "TRUE", "FALSE",
    "CALL", "YIELD",
];

/// Gremlin traversal methods for completion (TinkerPop-style).
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
    "properties",
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
    "explain",
    // Algorithm
    "shortestPath",
    "kShortestPaths",
    "bfs",
    "dfs",
    "bidirectionalBfs",
    "iddfs",
    // Modulator
    "with",
    // Full-text search
    "searchTextV",
    "searchTextE",
    "textScore",
];

/// Gremlin predicate functions for completion (TinkerPop-style).
const GREMLIN_PREDICATES: &[&str] = &[
    // P predicates (used as P.eq(), P.gt(), etc.)
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
    // TextP predicates (used as TextP.containing(), etc.)
    "containing",
    "notContaining",
    "startingWith",
    "notStartingWith",
    "endingWith",
    "notEndingWith",
    "regex",
];

/// Gremlin predicate namespaces for completion.
const GREMLIN_PREDICATE_NAMESPACES: &[&str] = &["P", "TextP", "TextQ", "__"];

/// Dot-commands for completion.
const DOT_COMMANDS: &[&str] = &[
    ".help", ".schema", ".stats", ".history", ".vars", ".mode", ".clear", ".quit", ".exit", ".set",
    ".read", ".output",
];

/// Completer for the REPL.
#[derive(Debug, Clone)]
pub struct ReplCompleter {
    vertex_labels: Vec<String>,
    edge_labels: Vec<String>,
}

impl ReplCompleter {
    /// Create a new completer with schema information.
    pub fn new(labels: GraphLabels) -> Self {
        Self {
            vertex_labels: labels.vertex_labels,
            edge_labels: labels.edge_labels,
        }
    }

    /// Update labels from the database.
    #[allow(dead_code)] // Will be used for dynamic schema updates
    pub fn update_labels(&mut self, labels: GraphLabels) {
        self.vertex_labels = labels.vertex_labels;
        self.edge_labels = labels.edge_labels;
    }

    /// Complete dot-commands.
    fn complete_dot_commands(&self, word: &str) -> Vec<Pair> {
        DOT_COMMANDS
            .iter()
            .filter(|cmd| cmd.starts_with(word))
            .map(|cmd| Pair {
                display: cmd.to_string(),
                replacement: cmd.to_string(),
            })
            .collect()
    }

    /// Complete GQL keywords.
    fn complete_keywords(&self, word: &str) -> Vec<Pair> {
        let word_upper = word.to_uppercase();
        GQL_KEYWORDS
            .iter()
            .filter(|kw| kw.starts_with(&word_upper))
            .map(|kw| {
                // Return in the same case as input if starting with lowercase
                let replacement = if word
                    .chars()
                    .next()
                    .map(|c| c.is_lowercase())
                    .unwrap_or(false)
                {
                    kw.to_lowercase()
                } else {
                    kw.to_string()
                };
                Pair {
                    display: kw.to_string(),
                    replacement,
                }
            })
            .collect()
    }

    /// Complete labels.
    fn complete_labels(&self, word: &str) -> Vec<Pair> {
        let mut pairs = Vec::new();

        // Vertex labels
        for label in &self.vertex_labels {
            if label.to_lowercase().starts_with(&word.to_lowercase()) {
                pairs.push(Pair {
                    display: label.clone(),
                    replacement: label.clone(),
                });
            }
        }

        // Edge labels
        for label in &self.edge_labels {
            if label.to_lowercase().starts_with(&word.to_lowercase()) {
                pairs.push(Pair {
                    display: format!(":{}", label),
                    replacement: label.clone(),
                });
            }
        }

        pairs
    }

    /// Complete Gremlin methods (after a dot).
    fn complete_gremlin_methods(&self, word: &str) -> Vec<Pair> {
        let mut pairs: Vec<Pair> = GREMLIN_METHODS
            .iter()
            .filter(|m| m.starts_with(word))
            .map(|m| Pair {
                display: format!("{}()", m),
                replacement: format!("{}(", m),
            })
            .collect();

        // Deduplicate (some methods like id, label appear in multiple lists)
        pairs.sort_by(|a, b| a.display.cmp(&b.display));
        pairs.dedup_by(|a, b| a.display == b.display);

        pairs
    }

    /// Complete Gremlin predicates (P.eq, TextP.containing, etc.).
    fn complete_gremlin_predicates(&self, word: &str) -> Vec<Pair> {
        let mut pairs = Vec::new();

        // Complete predicate namespaces (P, TextP, __)
        for ns in GREMLIN_PREDICATE_NAMESPACES {
            if ns.starts_with(word) {
                pairs.push(Pair {
                    display: format!("{}.", ns),
                    replacement: format!("{}.", ns),
                });
            }
        }

        // Complete P.xxx predicates
        if let Some(after_p) = word.strip_prefix("P.") {
            for pred in GREMLIN_PREDICATES.iter().take(11) {
                // First 11 are P predicates
                if pred.starts_with(after_p) {
                    pairs.push(Pair {
                        display: format!("P.{}()", pred),
                        replacement: format!("P.{}(", pred),
                    });
                }
            }
        }

        // Complete TextP.xxx predicates
        if let Some(after_textp) = word.strip_prefix("TextP.") {
            for pred in GREMLIN_PREDICATES.iter().skip(11) {
                // After first 11 are TextP predicates
                if pred.starts_with(after_textp) {
                    pairs.push(Pair {
                        display: format!("TextP.{}()", pred),
                        replacement: format!("TextP.{}(", pred),
                    });
                }
            }
        }

        // Complete __.xxx anonymous traversals
        if let Some(after_anon) = word.strip_prefix("__.") {
            for method in GREMLIN_METHODS {
                if method.starts_with(after_anon) {
                    pairs.push(Pair {
                        display: format!("__.{}()", method),
                        replacement: format!("__.{}(", method),
                    });
                }
            }
        }

        pairs
    }
}

impl Completer for ReplCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // Get the word being completed
        let line_to_cursor = &line[..pos];
        let word_start = line_to_cursor
            .rfind(|c: char| c.is_whitespace() || c == '(' || c == ':' || c == '[')
            .map(|i| i + 1)
            .unwrap_or(0);
        let word = &line_to_cursor[word_start..];

        // Check if we're completing after a dot (Gremlin method chain)
        let after_dot = word_start > 0 && line_to_cursor.chars().nth(word_start - 1) == Some('.');

        let completions = if line.starts_with('.') && !line.contains(' ') {
            // Completing a dot-command
            self.complete_dot_commands(line)
        } else if word.is_empty() {
            // No word to complete
            Vec::new()
        } else if word.chars().next() == Some(':') {
            // Completing after a colon (label context)
            self.complete_labels(&word[1..])
        } else if after_dot {
            // Completing after a dot - Gremlin method chain
            self.complete_gremlin_methods(word)
        } else {
            // Try keywords first, then Gremlin methods/predicates, then labels
            let mut completions = self.complete_keywords(word);
            completions.extend(self.complete_gremlin_methods(word));
            completions.extend(self.complete_gremlin_predicates(word));
            completions.extend(self.complete_labels(word));
            completions
        };

        // Calculate the position where replacement should start
        let replacement_start = if line.starts_with('.') && !line.contains(' ') {
            0
        } else {
            word_start
        };

        Ok((replacement_start, completions))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_complete_dot_commands() {
        let completer = ReplCompleter::new(GraphLabels::default());

        let completions = completer.complete_dot_commands(".h");
        assert!(completions.iter().any(|p| p.replacement == ".help"));
        assert!(completions.iter().any(|p| p.replacement == ".history"));
    }

    #[test]
    fn test_complete_keywords() {
        let completer = ReplCompleter::new(GraphLabels::default());

        let completions = completer.complete_keywords("MA");
        assert!(completions.iter().any(|p| p.display == "MATCH"));
        assert!(completions.iter().any(|p| p.display == "MAX"));
    }

    #[test]
    fn test_complete_labels() {
        let labels = GraphLabels {
            vertex_labels: vec!["Person".to_string(), "Company".to_string()],
            edge_labels: vec!["knows".to_string(), "works_at".to_string()],
        };
        let completer = ReplCompleter::new(labels);

        let completions = completer.complete_labels("Per");
        assert!(completions.iter().any(|p| p.replacement == "Person"));
    }
}
