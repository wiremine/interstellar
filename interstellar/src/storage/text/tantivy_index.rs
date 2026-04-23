//! Tantivy-backed implementation of [`TextIndex`].
//!
//! Phase 1 ships the in-memory backend (Tantivy `RamDirectory`). The on-disk
//! variant lands in Phase 4 along with WAL integration.

use std::sync::Arc;

use parking_lot::RwLock;
use tantivy::collector::TopDocs;
use tantivy::query::{
    AllQuery, BooleanQuery, Occur, PhraseQuery, Query, QueryParser, RegexQuery, TermQuery,
};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, FAST, INDEXED,
    STORED,
};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};

use crate::index::ElementType;
use crate::value::{EdgeId, VertexId};

use super::query::TextQuery;
use super::{
    ElementRef, TextHit, TextIndex, TextIndexConfig, TextIndexError, DEFAULT_BM25_B,
    DEFAULT_BM25_K1,
};

/// Build an [`ElementRef`] from a raw u64 id according to the index's
/// element type.
fn make_element_ref(element_type: ElementType, id: u64) -> ElementRef {
    match element_type {
        ElementType::Vertex => ElementRef::Vertex(VertexId(id)),
        ElementType::Edge => ElementRef::Edge(EdgeId(id)),
    }
}

/// Internal Tantivy field name used to store the element id (`VertexId.0` or
/// `EdgeId.0`) so we can:
///   - delete a previously-indexed document by id (`delete_term`), and
///   - recover the id from a search hit.
const FIELD_ELEMENT_ID: &str = "_id";

/// Internal field name for the indexed text body.
const FIELD_BODY: &str = "_text";

/// Tantivy-backed [`TextIndex`].
///
/// In-memory only in Phase 1. The struct is `Send + Sync` (the underlying
/// state is held in a [`parking_lot::RwLock`]) so it can be shared across
/// threads exactly like the rest of the storage layer.
pub struct TantivyTextIndex {
    config: TextIndexConfig,
    element_type: ElementType,
    inner: RwLock<Inner>,
}

/// Mutable state. Held inside an `RwLock` so writes (`upsert`, `delete`,
/// `commit`, `merge`) take a write lock and reads (`search`, `len`) take a
/// read lock and reload the searcher.
struct Inner {
    index: Index,
    writer: IndexWriter<TantivyDocument>,
    reader: IndexReader,
    body_field: Field,
    id_field: Field,
    schema: Schema,
    /// Number of `upsert`s since the last commit. Used to drive
    /// `commit_every`.
    pending_upserts: usize,
}

impl TantivyTextIndex {
    /// Build a new in-memory text index with the given configuration.
    ///
    /// Validates `config` up-front:
    ///   - non-default BM25 parameters are rejected (Tantivy 0.25 hard-codes
    ///     them);
    ///   - `writer_memory_bytes` must satisfy Tantivy's minimum (15 MB).
    pub fn in_memory(
        element_type: ElementType,
        config: TextIndexConfig,
    ) -> Result<Self, TextIndexError> {
        Self::validate_config(&config)?;

        let analyzer = config.analyzer.build()?;
        let tokenizer_name = config.analyzer.tokenizer_name();

        let mut schema_builder = SchemaBuilder::new();
        let id_field = schema_builder.add_u64_field(FIELD_ELEMENT_ID, INDEXED | STORED | FAST);
        let record_option = if config.store_positions {
            IndexRecordOption::WithFreqsAndPositions
        } else {
            IndexRecordOption::WithFreqs
        };
        let text_indexing = TextFieldIndexing::default()
            .set_tokenizer(&tokenizer_name)
            .set_index_option(record_option);
        let text_options = TextOptions::default().set_indexing_options(text_indexing);
        let body_field = schema_builder.add_text_field(FIELD_BODY, text_options);

        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());
        index.tokenizers().register(&tokenizer_name, analyzer);

        let writer: IndexWriter<TantivyDocument> = index
            .writer(config.writer_memory_bytes)
            .map_err(TextIndexError::from)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(TextIndexError::from)?;

        Ok(Self {
            config,
            element_type,
            inner: RwLock::new(Inner {
                index,
                writer,
                reader,
                body_field,
                id_field,
                schema,
                pending_upserts: 0,
            }),
        })
    }

    /// Create a new on-disk text index at `dir`.
    ///
    /// The directory is created if it does not exist. Any existing Tantivy
    /// segments in the directory are **deleted** — use [`Self::open`] to
    /// reopen a previously-created on-disk index.
    ///
    /// The index uses Tantivy's `MmapDirectory` for persistent segment
    /// storage. Commits are flushed to disk and survive process restarts.
    pub fn on_disk(
        dir: &std::path::Path,
        element_type: ElementType,
        config: TextIndexConfig,
    ) -> Result<Self, TextIndexError> {
        Self::validate_config(&config)?;

        // Ensure directory exists; wipe stale segments.
        if dir.exists() {
            std::fs::remove_dir_all(dir).map_err(|e| {
                TextIndexError::Backend(format!("failed to clean index dir: {e}"))
            })?;
        }
        std::fs::create_dir_all(dir).map_err(|e| {
            TextIndexError::Backend(format!("failed to create index dir: {e}"))
        })?;

        // Build schema first so we can pass it to create_in_dir.
        let analyzer = config.analyzer.build()?;
        let tokenizer_name = config.analyzer.tokenizer_name();

        let mut schema_builder = SchemaBuilder::new();
        let id_field = schema_builder.add_u64_field(FIELD_ELEMENT_ID, INDEXED | STORED | FAST);
        let record_option = if config.store_positions {
            IndexRecordOption::WithFreqsAndPositions
        } else {
            IndexRecordOption::WithFreqs
        };
        let text_indexing = TextFieldIndexing::default()
            .set_tokenizer(&tokenizer_name)
            .set_index_option(record_option);
        let text_options = TextOptions::default().set_indexing_options(text_indexing);
        let body_field = schema_builder.add_text_field(FIELD_BODY, text_options);
        let schema = schema_builder.build();

        let index = Index::create_in_dir(dir, schema.clone())
            .map_err(TextIndexError::from)?;
        index.tokenizers().register(&tokenizer_name, analyzer);

        let writer: IndexWriter<TantivyDocument> = index
            .writer(config.writer_memory_bytes)
            .map_err(TextIndexError::from)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(TextIndexError::from)?;

        Ok(Self {
            config,
            element_type,
            inner: RwLock::new(Inner {
                index,
                writer,
                reader,
                body_field,
                id_field,
                schema,
                pending_upserts: 0,
            }),
        })
    }

    /// Open an existing on-disk text index at `dir`.
    ///
    /// The directory must contain a valid Tantivy index created by
    /// [`Self::on_disk`]. The analyzer is re-registered from `config`.
    pub fn open(
        dir: &std::path::Path,
        element_type: ElementType,
        config: TextIndexConfig,
    ) -> Result<Self, TextIndexError> {
        Self::validate_config(&config)?;

        let index = Index::open_in_dir(dir).map_err(TextIndexError::from)?;

        let analyzer = config.analyzer.build()?;
        let tokenizer_name = config.analyzer.tokenizer_name();
        index.tokenizers().register(&tokenizer_name, analyzer);

        // Resolve fields from the schema stored on disk.
        let schema = index.schema();
        let id_field = schema
            .get_field(FIELD_ELEMENT_ID)
            .map_err(|_| TextIndexError::Corruption(format!("missing field {FIELD_ELEMENT_ID}")))?;
        let body_field = schema
            .get_field(FIELD_BODY)
            .map_err(|_| TextIndexError::Corruption(format!("missing field {FIELD_BODY}")))?;

        let writer: IndexWriter<TantivyDocument> = index
            .writer(config.writer_memory_bytes)
            .map_err(TextIndexError::from)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(TextIndexError::from)?;

        Ok(Self {
            config,
            element_type,
            inner: RwLock::new(Inner {
                index,
                writer,
                reader,
                body_field,
                id_field,
                schema,
                pending_upserts: 0,
            }),
        })
    }

    /// Parse a Tantivy mini-language query string against this index's body
    /// field. The returned [`TextQuery`] uses [`TextQuery::Match`] /
    /// [`TextQuery::And`] etc. is **not** what is returned — instead this
    /// produces a `Box<dyn Query>` already bound to the field. For now we
    /// expose this as a private helper used by [`Self::search_str`]; the
    /// public surface accepts [`TextQuery`].
    fn compile_string_query(
        inner: &Inner,
        analyzer_field_for_query: Field,
        query_text: &str,
    ) -> Result<Box<dyn Query>, TextIndexError> {
        let parser = QueryParser::for_index(&inner.index, vec![analyzer_field_for_query]);
        parser.parse_query(query_text).map_err(TextIndexError::from)
    }

    /// Convenience: search using a Tantivy mini-language query string instead
    /// of building a [`TextQuery`].
    ///
    /// Phase 3 will route this through the GQL `MATCHES` literal.
    pub fn search_str(&self, query: &str, k: usize) -> Result<Vec<TextHit>, TextIndexError> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let inner = self.inner.read();
        let q = Self::compile_string_query(&inner, inner.body_field, query)?;
        self.run_search(&inner, q.as_ref(), k)
    }

    fn validate_config(config: &TextIndexConfig) -> Result<(), TextIndexError> {
        if (config.bm25_k1 - DEFAULT_BM25_K1).abs() > f32::EPSILON {
            return Err(TextIndexError::UnsupportedConfig(format!(
                "Tantivy 0.25 hard-codes BM25 k1=1.2; bm25_k1={} is not supported in v1",
                config.bm25_k1
            )));
        }
        if (config.bm25_b - DEFAULT_BM25_B).abs() > f32::EPSILON {
            return Err(TextIndexError::UnsupportedConfig(format!(
                "Tantivy 0.25 hard-codes BM25 b=0.75; bm25_b={} is not supported in v1",
                config.bm25_b
            )));
        }
        if config.writer_memory_bytes < 15_000_000 {
            return Err(TextIndexError::UnsupportedConfig(format!(
                "writer_memory_bytes must be >= 15_000_000 (Tantivy minimum); got {}",
                config.writer_memory_bytes
            )));
        }
        Ok(())
    }

    /// Convert a [`TextQuery`] into a `Box<dyn tantivy::query::Query>`.
    fn compile(inner: &Inner, q: &TextQuery) -> Result<Box<dyn Query>, TextIndexError> {
        if q.is_empty() {
            // Empty leaf or empty boolean — match nothing.
            return Ok(Box::new(BooleanQuery::new(Vec::new())));
        }
        // A bare top-level `Not` is allowed and compiles to
        // `AllQuery MUST_NOT inner` further down. Purely-negative *boolean*
        // queries (`And [Not, ...]` or `Or [Not, ...]`) are rejected because
        // BM25 has no positive clause to score against.
        if matches!(q, TextQuery::And(_) | TextQuery::Or(_)) && q.is_purely_negative() {
            return Err(TextIndexError::QueryParse(
                "query consists only of negations; combine with a positive clause or use \
                 `Match(*)` explicitly"
                    .to_string(),
            ));
        }
        Self::compile_inner(inner, q)
    }

    fn compile_inner(inner: &Inner, q: &TextQuery) -> Result<Box<dyn Query>, TextIndexError> {
        match q {
            TextQuery::Match(text) => {
                let parser = QueryParser::for_index(&inner.index, vec![inner.body_field]);
                // Default conjunction = false → OR-of-terms.
                parser.parse_query(text).map_err(TextIndexError::from)
            }
            TextQuery::MatchAll(text) => {
                let mut parser = QueryParser::for_index(&inner.index, vec![inner.body_field]);
                parser.set_conjunction_by_default();
                parser.parse_query(text).map_err(TextIndexError::from)
            }
            TextQuery::Phrase { text, slop } => {
                if !inner_supports_positions(inner)? {
                    return Err(TextIndexError::UnsupportedConfig(
                        "phrase queries require store_positions = true".to_string(),
                    ));
                }
                let terms = analyze_terms(inner, text)?;
                if terms.is_empty() {
                    return Ok(Box::new(BooleanQuery::new(Vec::new())));
                }
                if terms.len() == 1 {
                    return Ok(Box::new(TermQuery::new(
                        terms.into_iter().next().unwrap(),
                        IndexRecordOption::WithFreqsAndPositions,
                    )));
                }
                let mut phrase = PhraseQuery::new(terms);
                phrase.set_slop(*slop);
                Ok(Box::new(phrase))
            }
            TextQuery::Prefix(text) => {
                let terms = analyze_terms(inner, text)?;
                let term = match terms.len() {
                    0 => return Ok(Box::new(BooleanQuery::new(Vec::new()))),
                    1 => terms.into_iter().next().unwrap(),
                    _ => {
                        return Err(TextIndexError::QueryParse(format!(
                            "Prefix expects exactly one analyzer-produced term, got {}",
                            terms.len()
                        )))
                    }
                };
                // RegexQuery interprets its pattern after the analyzer has
                // already run, so we escape regex metacharacters in the term
                // text and append `.*`.
                let pattern = format!("{}.*", regex_escape(term.value().as_str().unwrap_or("")));
                let q = RegexQuery::from_pattern(&pattern, inner.body_field)
                    .map_err(TextIndexError::from)?;
                Ok(Box::new(q))
            }
            TextQuery::And(parts) => {
                let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(parts.len());
                for part in parts {
                    if let TextQuery::Not(inner_q) = part {
                        clauses.push((Occur::MustNot, Self::compile_inner(inner, inner_q)?));
                    } else {
                        clauses.push((Occur::Must, Self::compile_inner(inner, part)?));
                    }
                }
                Ok(Box::new(BooleanQuery::new(clauses)))
            }
            TextQuery::Or(parts) => {
                let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(parts.len());
                for part in parts {
                    if let TextQuery::Not(_) = part {
                        // Negation inside an Or doesn't have well-defined
                        // semantics in BM25 boolean evaluation; reject it.
                        return Err(TextIndexError::QueryParse(
                            "Not is not allowed as a direct child of Or; use And { positive, \
                             Not(...) } instead"
                                .to_string(),
                        ));
                    } else {
                        clauses.push((Occur::Should, Self::compile_inner(inner, part)?));
                    }
                }
                Ok(Box::new(BooleanQuery::new(clauses)))
            }
            TextQuery::Not(inner_q) => {
                // A bare `Not` at the top compiles into `AllQuery MUST_NOT
                // inner` to satisfy Tantivy's "needs a positive clause" rule.
                let neg = Self::compile_inner(inner, inner_q)?;
                let clauses: Vec<(Occur, Box<dyn Query>)> =
                    vec![(Occur::Must, Box::new(AllQuery)), (Occur::MustNot, neg)];
                Ok(Box::new(BooleanQuery::new(clauses)))
            }
        }
    }

    fn run_search(
        &self,
        inner: &Inner,
        query: &dyn Query,
        k: usize,
    ) -> Result<Vec<TextHit>, TextIndexError> {
        // Make recently-committed segments visible.
        inner.reader.reload().map_err(TextIndexError::from)?;
        let searcher = inner.reader.searcher();

        let top_docs = searcher
            .search(query, &TopDocs::with_limit(k))
            .map_err(TextIndexError::from)?;

        let mut out = Vec::with_capacity(top_docs.len());
        for (score, address) in top_docs {
            let doc: TantivyDocument = searcher.doc(address).map_err(TextIndexError::from)?;
            let id = extract_element_id(&doc, inner.id_field).ok_or_else(|| {
                TextIndexError::Corruption(format!(
                    "hit at {:?} missing {} field",
                    address, FIELD_ELEMENT_ID
                ))
            })?;
            out.push(TextHit {
                element: make_element_ref(self.element_type, id),
                score,
            });
        }
        Ok(out)
    }
}

/// Returns whether the index was built with positions recorded.
fn inner_supports_positions(inner: &Inner) -> Result<bool, TextIndexError> {
    // Inspect the body field's text indexing options.
    let entry = inner.schema.get_field_entry(inner.body_field);
    use tantivy::schema::FieldType;
    match entry.field_type() {
        FieldType::Str(opts) => Ok(opts
            .get_indexing_options()
            .map(|i| i.index_option() == IndexRecordOption::WithFreqsAndPositions)
            .unwrap_or(false)),
        _ => Err(TextIndexError::Corruption(format!(
            "field {} is not a text field",
            FIELD_BODY
        ))),
    }
}

/// Run the body-field analyzer over `text` and return the resulting terms.
fn analyze_terms(inner: &Inner, text: &str) -> Result<Vec<Term>, TextIndexError> {
    use tantivy::tokenizer::TokenStream;

    let tokenizer_name = match inner.schema.get_field_entry(inner.body_field).field_type() {
        tantivy::schema::FieldType::Str(opts) => opts
            .get_indexing_options()
            .map(|i| i.tokenizer().to_string())
            .ok_or_else(|| {
                TextIndexError::Corruption("body field has no tokenizer registered".to_string())
            })?,
        _ => {
            return Err(TextIndexError::Corruption(
                "body field is not a text field".to_string(),
            ))
        }
    };
    let mut analyzer = inner
        .index
        .tokenizers()
        .get(&tokenizer_name)
        .ok_or_else(|| TextIndexError::UnknownAnalyzer(tokenizer_name.clone()))?;
    let mut stream = analyzer.token_stream(text);
    let mut out = Vec::new();
    while stream.advance() {
        out.push(Term::from_field_text(
            inner.body_field,
            &stream.token().text,
        ));
    }
    Ok(out)
}

fn extract_element_id(doc: &TantivyDocument, field: Field) -> Option<u64> {
    use tantivy::schema::document::{CompactDocValue, Value};
    doc.get_first(field)
        .and_then(|v: CompactDocValue<'_>| v.as_u64())
}

/// Escape regex metacharacters so a user term can be embedded into a regex
/// pattern verbatim. Tantivy's [`RegexQuery`] uses Rust regex syntax.
fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

// =============================================================================
// TextIndex impl
// =============================================================================

impl TextIndex for TantivyTextIndex {
    fn config(&self) -> &TextIndexConfig {
        &self.config
    }

    fn upsert(&self, id: u64, text: &str) -> Result<(), TextIndexError> {
        let mut inner = self.inner.write();

        // Delete any prior version of this element.
        let term = Term::from_field_u64(inner.id_field, id);
        inner.writer.delete_term(term);

        let mut doc = TantivyDocument::default();
        doc.add_u64(inner.id_field, id);
        doc.add_text(inner.body_field, text);
        inner
            .writer
            .add_document(doc)
            .map_err(TextIndexError::from)?;

        inner.pending_upserts += 1;
        if inner.pending_upserts >= self.config.commit_every {
            inner.writer.commit().map_err(TextIndexError::from)?;
            inner.pending_upserts = 0;
        }
        Ok(())
    }

    fn delete(&self, id: u64) -> Result<(), TextIndexError> {
        let mut inner = self.inner.write();
        let term = Term::from_field_u64(inner.id_field, id);
        inner.writer.delete_term(term);
        inner.pending_upserts += 1;
        if inner.pending_upserts >= self.config.commit_every {
            inner.writer.commit().map_err(TextIndexError::from)?;
            inner.pending_upserts = 0;
        }
        Ok(())
    }

    fn search(&self, query: &TextQuery, k: usize) -> Result<Vec<TextHit>, TextIndexError> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let inner = self.inner.read();
        let q = Self::compile(&inner, query)?;
        self.run_search(&inner, q.as_ref(), k)
    }

    fn len(&self) -> usize {
        let inner = self.inner.read();
        if inner.reader.reload().is_err() {
            return 0;
        }
        inner.reader.searcher().num_docs() as usize
    }

    fn commit(&self) -> Result<(), TextIndexError> {
        let mut inner = self.inner.write();
        inner.writer.commit().map_err(TextIndexError::from)?;
        inner.pending_upserts = 0;
        inner.reader.reload().map_err(TextIndexError::from)?;
        Ok(())
    }

    fn merge(&self) -> Result<(), TextIndexError> {
        // Tantivy's RamDirectory has no scheduled merges. We can request a
        // best-effort merge of all current segments. If there are fewer than
        // 2 segments, this is a no-op.
        let inner = self.inner.read();
        let searcher = inner.reader.searcher();
        let segment_ids: Vec<_> = searcher
            .segment_readers()
            .iter()
            .map(|s| s.segment_id())
            .collect();
        drop(inner);
        if segment_ids.len() < 2 {
            return Ok(());
        }
        let mut inner = self.inner.write();
        // `merge` is async (returns a future) in Tantivy; we just fire it
        // and forget — segment merging is opportunistic, not required for
        // correctness.
        drop(inner.writer.merge(&segment_ids));
        Ok(())
    }
}

// Drop note: the IndexWriter holds a background indexing thread pool. When
// the struct is dropped Tantivy joins those threads; uncommitted writes are
// discarded. That matches the "RAM-only, no recovery" semantic for Phase 1.
//
// We use `Arc<RwLock<Inner>>` indirectly only via `RwLock<Inner>` field; the
// outer struct can be wrapped in `Arc` by callers if shared ownership is
// needed.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TantivyTextIndex>();
};

// Suppress an unused-import warning when compiled without tests.
#[allow(dead_code)]
fn _force_link_arc(_: Arc<()>) {}
