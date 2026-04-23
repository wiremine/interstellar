//! Full-text search indexing.
//!
//! This module provides a full-text search (FTS) capability over string-valued
//! graph element properties. The default implementation is backed by
//! [Tantivy](https://github.com/quickwit-oss/tantivy) and uses BM25 relevance
//! scoring.
//!
//! Phase 1 of [spec-55](../../../../specs/spec-55-fulltext-search.md) introduces
//! the trait surface and an in-memory backend. Schema integration, traversal
//! steps, GQL/Gremlin surfaces, and persistence are layered on in subsequent
//! phases.
//!
//! # Quick Start
//!
//! ```
//! # #[cfg(feature = "full-text")]
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use interstellar::index::ElementType;
//! use interstellar::storage::text::{
//!     Analyzer, ElementRef, TantivyTextIndex, TextIndex, TextIndexConfig, TextQuery,
//! };
//! use interstellar::value::VertexId;
//!
//! let idx = TantivyTextIndex::in_memory(ElementType::Vertex, TextIndexConfig {
//!     analyzer: Analyzer::StandardEnglish,
//!     ..Default::default()
//! })?;
//!
//! idx.upsert(1, "the quick brown fox jumps over the lazy dog")?;
//! idx.upsert(2, "raft is a consensus algorithm for distributed systems")?;
//! idx.commit()?;
//!
//! let hits = idx.search(&TextQuery::Match("consensus".into()), 10)?;
//! assert_eq!(hits.len(), 1);
//! assert_eq!(hits[0].element, ElementRef::Vertex(VertexId(2)));
//! # Ok(())
//! # }
//! ```

use crate::error::StorageError;
use crate::value::{EdgeId, VertexId};

mod analyzer;
mod query;
mod tantivy_index;

#[cfg(test)]
mod tests;

pub use analyzer::Analyzer;
pub use query::TextQuery;
pub use tantivy_index::TantivyTextIndex;

/// Default BM25 `k1` parameter (term saturation).
///
/// Note: as of Tantivy 0.25 this is an internal constant in the scorer
/// (`tantivy::query::bm25::K1 = 1.2`) and is **not** runtime-configurable
/// through the public Tantivy API. The field is retained on
/// [`TextIndexConfig`] for forward compatibility, but a non-default value is
/// currently a no-op and surfaced via [`TextIndexError::UnsupportedConfig`]
/// when the index is built.
pub const DEFAULT_BM25_K1: f32 = 1.2;

/// Default BM25 `b` parameter (length normalization). See [`DEFAULT_BM25_K1`]
/// for the same caveat about runtime configurability.
pub const DEFAULT_BM25_B: f32 = 0.75;

// =============================================================================
// Errors
// =============================================================================

/// Errors that can occur while interacting with a text index.
///
/// All public entry points on the [`TextIndex`] trait return
/// `Result<_, TextIndexError>`. No method panics on a recoverable condition;
/// internal Tantivy panics are caught at the trait boundary and surfaced as
/// [`TextIndexError::Backend`].
#[derive(Debug, thiserror::Error)]
pub enum TextIndexError {
    /// The property name was not registered as a text index on this storage.
    #[error("property '{0}' is not registered as a text index")]
    PropertyNotIndexed(String),

    /// An attempt was made to index a non-string value.
    ///
    /// The text index only accepts `Value::String` and
    /// `Value::List(Value::String..)` payloads. The mutation path checks this
    /// before calling `upsert` and surfaces this error if the type doesn't
    /// match. The `id` is the inner `u64` of the offending element.
    #[error("element id {0} property '{1}' is not a string-valued field")]
    NonStringValue(u64, String),

    /// The query string failed to parse under the configured analyzer / query
    /// language.
    #[error("query parse error: {0}")]
    QueryParse(String),

    /// An analyzer name referenced in DDL or config is unknown.
    #[error("analyzer '{0}' is not registered")]
    UnknownAnalyzer(String),

    /// A field of [`TextIndexConfig`] holds a value the current backend cannot
    /// honor (e.g. non-default BM25 parameters in Tantivy 0.25).
    #[error("unsupported config: {0}")]
    UnsupportedConfig(String),

    /// The persistent index detected on-disk corruption (used by Phase 4).
    #[error("index corruption: {0}")]
    Corruption(String),

    /// The underlying Tantivy backend reported an error.
    #[error("backend error: {0}")]
    Backend(String),

    /// Underlying graph storage error (used when text indexes are wired into
    /// `GraphStorage` mutation paths in Phase 2).
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

impl From<tantivy::TantivyError> for TextIndexError {
    fn from(err: tantivy::TantivyError) -> Self {
        TextIndexError::Backend(err.to_string())
    }
}

impl From<tantivy::query::QueryParserError> for TextIndexError {
    fn from(err: tantivy::query::QueryParserError) -> Self {
        TextIndexError::QueryParse(err.to_string())
    }
}

// =============================================================================
// Config
// =============================================================================

/// Configuration for a single text index.
///
/// See [`Analyzer`] for the supported tokenization pipelines. The BM25 fields
/// are currently advisory — see [`DEFAULT_BM25_K1`].
#[derive(Debug, Clone, PartialEq)]
pub struct TextIndexConfig {
    /// Tokenization / normalization pipeline applied to indexed text and to
    /// the parsed portion of free-text queries.
    pub analyzer: Analyzer,

    /// Whether to record term positions (required for phrase queries).
    /// Costs ~30% additional disk in the persistent backend.
    pub store_positions: bool,

    /// BM25 `k1` parameter. Currently advisory — see [`DEFAULT_BM25_K1`].
    pub bm25_k1: f32,

    /// BM25 `b` parameter. Currently advisory — see [`DEFAULT_BM25_K1`].
    pub bm25_b: f32,

    /// Auto-commit batch size. After this many `upsert`s the writer flushes
    /// a segment so the changes become visible to subsequent searchers.
    /// Set to `usize::MAX` to disable auto-commit; callers must then invoke
    /// [`TextIndex::commit`] explicitly.
    pub commit_every: usize,

    /// Memory budget for the Tantivy `IndexWriter` (in bytes).
    /// Tantivy requires this to be at least 15 MB; defaults to 50 MB.
    pub writer_memory_bytes: usize,
}

impl Default for TextIndexConfig {
    fn default() -> Self {
        Self {
            analyzer: Analyzer::StandardEnglish,
            store_positions: true,
            bm25_k1: DEFAULT_BM25_K1,
            bm25_b: DEFAULT_BM25_B,
            commit_every: 1024,
            writer_memory_bytes: 50_000_000,
        }
    }
}

// =============================================================================
// Hits
// =============================================================================

/// Discriminator carried by every [`TextHit`].
///
/// A single text-index instance is bound to exactly one element type at
/// construction time, so the variant in a hit reflects that binding rather
/// than per-document choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElementRef {
    /// The matching element is a vertex.
    Vertex(VertexId),
    /// The matching element is an edge.
    Edge(EdgeId),
}

impl ElementRef {
    /// Returns the `VertexId` if this is a `Vertex` variant, else `None`.
    pub fn as_vertex(self) -> Option<VertexId> {
        match self {
            ElementRef::Vertex(id) => Some(id),
            ElementRef::Edge(_) => None,
        }
    }

    /// Returns the `EdgeId` if this is an `Edge` variant, else `None`.
    pub fn as_edge(self) -> Option<EdgeId> {
        match self {
            ElementRef::Edge(id) => Some(id),
            ElementRef::Vertex(_) => None,
        }
    }
}

/// A single search hit returned by [`TextIndex::search`].
#[derive(Debug, Clone, PartialEq)]
pub struct TextHit {
    /// The matching element (vertex or edge).
    pub element: ElementRef,

    /// BM25 relevance score (larger = more relevant).
    pub score: f32,
}

// =============================================================================
// Trait
// =============================================================================

/// Trait every backend's text index implements.
///
/// The trait abstracts over Tantivy specifically so the rest of interstellar
/// can be backend-agnostic, leaving room to swap the implementation (e.g. for
/// an `fst` + roaring-bitmap scorer) without breaking callers.
pub trait TextIndex: Send + Sync {
    /// Configuration this index was built with.
    fn config(&self) -> &TextIndexConfig;

    /// Insert or replace the indexed text for `id`.
    ///
    /// The `id` is the inner `u64` of either a [`VertexId`] or [`EdgeId`],
    /// depending on which element type this index instance was constructed
    /// for. The trait deliberately stays element-type-agnostic; the index
    /// instance carries the binding and uses it when assembling [`TextHit`]s.
    ///
    /// Multi-valued fields are flattened by the caller before reaching this
    /// trait (see the spec for the chosen sentinel-character convention).
    ///
    /// Takes `&self`: implementations are expected to use interior mutability
    /// so that a single shared `Arc<dyn TextIndex>` can be held by the
    /// surrounding `Graph` and shared across mutation paths and search paths.
    fn upsert(&self, id: u64, text: &str) -> Result<(), TextIndexError>;

    /// Remove the entry for `id`. Idempotent: deleting a missing element is
    /// a successful no-op.
    fn delete(&self, id: u64) -> Result<(), TextIndexError>;

    /// Top-`k` matches for `query`, ordered by descending score.
    ///
    /// Returns at most `k` hits. Calling with `k == 0` returns an empty vec
    /// without consulting the backend.
    fn search(&self, query: &TextQuery, k: usize) -> Result<Vec<TextHit>, TextIndexError>;

    /// Number of distinct documents currently visible to searchers
    /// (i.e. committed and not tombstoned).
    fn len(&self) -> usize;

    /// Returns `true` iff [`len`](Self::len) is 0.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Force a commit so all prior writes become visible to subsequent
    /// searchers.
    fn commit(&self) -> Result<(), TextIndexError>;

    /// Merge segments. May be a no-op for the in-memory backend; for the
    /// mmap backend triggers Tantivy's segment merger.
    fn merge(&self) -> Result<(), TextIndexError>;
}
