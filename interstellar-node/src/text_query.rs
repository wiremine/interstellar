//! TextQuery factory for napi-rs bindings.
//!
//! Provides the `TextQ` namespace for creating structured text search queries.

use napi_derive::napi;

use interstellar::storage::text::TextQuery;

/// A structured text search query.
///
/// Use the `TextQ` factory to create instances.
#[napi(js_name = "TextQuery")]
pub struct JsTextQuery {
    pub(crate) inner: TextQuery,
}

impl Clone for JsTextQuery {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Factory for creating structured text search queries.
///
/// @example
/// ```javascript
/// import { Graph, TextQ } from '@interstellar/node';
///
/// // Simple match query
/// graph.searchTextQueryV('description', TextQ.match('graph database'), 10);
///
/// // Phrase query
/// graph.searchTextQueryV('description', TextQ.phrase('graph database', 0), 10);
///
/// // Boolean AND
/// graph.searchTextQueryV('description', TextQ.and([
///     TextQ.match('graph'),
///     TextQ.match('database'),
/// ]), 10);
/// ```
#[napi]
pub struct TextQ;

#[napi]
impl TextQ {
    /// Free-text match query (OR of terms by default).
    ///
    /// @param text - Search text
    #[napi(js_name = "match")]
    pub fn match_(text: String) -> JsTextQuery {
        JsTextQuery {
            inner: TextQuery::Match(text),
        }
    }

    /// All terms must match (AND of terms).
    ///
    /// @param text - Search text
    #[napi(js_name = "matchAll")]
    pub fn match_all(text: String) -> JsTextQuery {
        JsTextQuery {
            inner: TextQuery::MatchAll(text),
        }
    }

    /// Phrase query: terms must appear in order.
    ///
    /// @param text - Phrase text
    /// @param slop - Positional slop (0 = strict adjacency)
    #[napi]
    pub fn phrase(text: String, slop: Option<u32>) -> JsTextQuery {
        JsTextQuery {
            inner: TextQuery::Phrase {
                text,
                slop: slop.unwrap_or(0),
            },
        }
    }

    /// Prefix match on a single term.
    ///
    /// @param prefix - Term prefix
    #[napi]
    pub fn prefix(prefix: String) -> JsTextQuery {
        JsTextQuery {
            inner: TextQuery::Prefix(prefix),
        }
    }

    /// Boolean AND: all subqueries must match.
    ///
    /// @param queries - Array of TextQuery objects
    #[napi(js_name = "and")]
    pub fn and_(queries: Vec<&JsTextQuery>) -> JsTextQuery {
        JsTextQuery {
            inner: TextQuery::And(queries.into_iter().map(|q| q.inner.clone()).collect()),
        }
    }

    /// Boolean OR: any subquery can match.
    ///
    /// @param queries - Array of TextQuery objects
    #[napi(js_name = "or")]
    pub fn or_(queries: Vec<&JsTextQuery>) -> JsTextQuery {
        JsTextQuery {
            inner: TextQuery::Or(queries.into_iter().map(|q| q.inner.clone()).collect()),
        }
    }

    /// Boolean NOT: negate a query.
    ///
    /// Must be combined with a positive clause at the top level.
    ///
    /// @param query - Query to negate
    #[napi(js_name = "not")]
    pub fn not_(query: &JsTextQuery) -> JsTextQuery {
        JsTextQuery {
            inner: TextQuery::Not(Box::new(query.inner.clone())),
        }
    }
}
