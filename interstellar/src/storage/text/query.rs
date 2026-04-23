//! Text-query AST.
//!
//! [`TextQuery`] is a backend-agnostic representation of a search query. The
//! Tantivy backend translates it into `Box<dyn tantivy::query::Query>` in
//! [`super::tantivy_index`].
//!
//! The enum mirrors only the subset of Tantivy's expressive surface that
//! interstellar guarantees to users — extending the surface is a deliberate
//! API decision, not an automatic consequence of upgrading Tantivy.

/// A backend-agnostic search query.
///
/// Construct manually for full control, or use the Tantivy mini-language
/// parser exposed by [`super::TantivyTextIndex::parse_query`] for
/// string-based queries (`"foo OR bar"`, `"+raft -paxos"`, etc.).
///
/// # Variants
///
/// | Variant | Semantics |
/// |---|---|
/// | [`Match`](Self::Match) | Free-text query: tokenize via the index analyzer, OR-of-terms |
/// | [`MatchAll`](Self::MatchAll) | Free-text query: tokenize, AND-of-terms (all required) |
/// | [`Phrase`](Self::Phrase) | Phrase query: terms in order, optional positional `slop` |
/// | [`Prefix`](Self::Prefix) | Single-term prefix match (case-folded by the analyzer) |
/// | [`And`](Self::And) | Boolean intersection of subqueries |
/// | [`Or`](Self::Or) | Boolean union of subqueries |
/// | [`Not`](Self::Not) | Boolean complement (must be combined with a positive clause at the top level) |
#[derive(Debug, Clone, PartialEq)]
pub enum TextQuery {
    /// Free-text query parsed by the configured analyzer; OR-of-terms by
    /// default.
    Match(String),

    /// All analyzer-produced terms must appear (intersection).
    MatchAll(String),

    /// Phrase query: terms in order with optional positional `slop`. Requires
    /// the index to have been built with
    /// [`store_positions = true`](super::TextIndexConfig::store_positions).
    Phrase {
        /// The phrase text. Tokenized by the index's analyzer.
        text: String,
        /// Positional slop — number of allowed swaps/insertions. `0` means
        /// strict adjacency.
        slop: u32,
    },

    /// Prefix match against a single term. The term is lowercased / stemmed
    /// first if the analyzer does so.
    Prefix(String),

    /// Boolean intersection. An empty `And` matches nothing (consistent with
    /// SQL semantics).
    And(Vec<TextQuery>),

    /// Boolean union. An empty `Or` matches nothing.
    Or(Vec<TextQuery>),

    /// Negation. At query compile time we wrap this as a Tantivy `MustNot`
    /// clause; a top-level standalone `Not` matches no documents (Tantivy
    /// requires at least one positive clause).
    Not(Box<TextQuery>),
}

impl TextQuery {
    /// Convenience: build an `And` from any iterator of subqueries.
    pub fn all<I: IntoIterator<Item = TextQuery>>(queries: I) -> Self {
        TextQuery::And(queries.into_iter().collect())
    }

    /// Convenience: build an `Or` from any iterator of subqueries.
    pub fn any<I: IntoIterator<Item = TextQuery>>(queries: I) -> Self {
        TextQuery::Or(queries.into_iter().collect())
    }

    /// Convenience: build a `Not(inner)`.
    #[allow(clippy::should_implement_trait)]
    pub fn not(inner: TextQuery) -> Self {
        TextQuery::Not(Box::new(inner))
    }

    /// `true` iff this query, considered standalone, would match no documents
    /// regardless of analyzer (e.g. a top-level `Not`, an empty boolean, or
    /// an empty match string).
    pub(super) fn is_empty(&self) -> bool {
        match self {
            TextQuery::Match(s) | TextQuery::MatchAll(s) | TextQuery::Prefix(s) => s.is_empty(),
            TextQuery::Phrase { text, .. } => text.is_empty(),
            TextQuery::And(q) | TextQuery::Or(q) => q.is_empty(),
            // A standalone Not is structurally non-empty but matches nothing
            // when compiled — see `is_purely_negative`.
            TextQuery::Not(_) => false,
        }
    }

    /// `true` iff this query consists entirely of negative clauses with no
    /// positive ones at the top level. Tantivy rejects such queries; we catch
    /// them up front to give a friendlier error.
    pub(super) fn is_purely_negative(&self) -> bool {
        match self {
            TextQuery::Not(_) => true,
            TextQuery::And(qs) | TextQuery::Or(qs) => {
                !qs.is_empty() && qs.iter().all(|q| q.is_purely_negative())
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convenience_constructors_compose() {
        let q = TextQuery::all([
            TextQuery::Match("a".into()),
            TextQuery::any([TextQuery::Match("b".into()), TextQuery::Match("c".into())]),
            TextQuery::not(TextQuery::Match("d".into())),
        ]);
        match q {
            TextQuery::And(parts) => assert_eq!(parts.len(), 3),
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn empty_match_is_empty() {
        assert!(TextQuery::Match(String::new()).is_empty());
        assert!(TextQuery::MatchAll(String::new()).is_empty());
        assert!(TextQuery::Prefix(String::new()).is_empty());
        assert!(TextQuery::Phrase {
            text: String::new(),
            slop: 0
        }
        .is_empty());
        assert!(TextQuery::And(vec![]).is_empty());
        assert!(TextQuery::Or(vec![]).is_empty());
    }

    #[test]
    fn populated_match_is_not_empty() {
        assert!(!TextQuery::Match("foo".into()).is_empty());
    }

    #[test]
    fn purely_negative_detection() {
        assert!(TextQuery::not(TextQuery::Match("a".into())).is_purely_negative());
        assert!(
            TextQuery::all([TextQuery::not(TextQuery::Match("a".into()))]).is_purely_negative()
        );
        assert!(!TextQuery::all([
            TextQuery::Match("a".into()),
            TextQuery::not(TextQuery::Match("b".into())),
        ])
        .is_purely_negative());
        assert!(!TextQuery::Match("a".into()).is_purely_negative());
        // Empty And/Or is *not* purely-negative (it's just empty).
        assert!(!TextQuery::And(vec![]).is_purely_negative());
    }
}
