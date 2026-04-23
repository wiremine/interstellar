//! Cross-module tests for the full-text search subsystem.
//!
//! Per-module unit tests live next to their code (in `analyzer.rs`,
//! `query.rs`, `tantivy_index.rs`). This file holds end-to-end behavioural
//! tests that exercise the full path from `upsert` through `search`.

use crate::index::ElementType;
use crate::value::VertexId;

use super::{Analyzer, ElementRef, TantivyTextIndex, TextIndex, TextIndexConfig, TextQuery};

fn fresh_index(analyzer: Analyzer) -> TantivyTextIndex {
    TantivyTextIndex::in_memory(
        ElementType::Vertex,
        TextIndexConfig {
            analyzer,
            commit_every: 1, // commit on every upsert so reads see writes immediately
            ..Default::default()
        },
    )
    .expect("build in-memory index")
}

/// Helper: extract the `VertexId` from a hit, panicking if it's an edge hit.
/// Used to keep assertions terse in the vertex-only tests.
fn vid(h: &super::TextHit) -> VertexId {
    h.element
        .as_vertex()
        .expect("vertex-only test received an edge hit")
}

#[test]
fn empty_index_returns_no_hits() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    let hits = idx
        .search(&TextQuery::Match("anything".into()), 10)
        .unwrap();
    assert!(hits.is_empty());
    assert_eq!(idx.len(), 0);
    assert!(idx.is_empty());
}

#[test]
fn k_zero_returns_empty_without_querying_backend() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "hello world").unwrap();
    let hits = idx.search(&TextQuery::Match("hello".into()), 0).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn upsert_and_search_returns_matching_vertex() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "the quick brown fox").unwrap();
    idx.upsert(2, "lazy dog under the hedge").unwrap();
    let hits = idx.search(&TextQuery::Match("fox".into()), 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].element, ElementRef::Vertex(VertexId(1)));
    assert!(hits[0].score > 0.0);
}

#[test]
fn upsert_is_idempotent_replacing_prior_doc() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "old text about cats").unwrap();
    idx.upsert(1, "new text about dogs").unwrap();
    let cats = idx.search(&TextQuery::Match("cats".into()), 10).unwrap();
    assert!(cats.is_empty(), "old text should be gone");
    let dogs = idx.search(&TextQuery::Match("dogs".into()), 10).unwrap();
    assert_eq!(dogs.len(), 1);
    assert_eq!(vid(&dogs[0]), VertexId(1));
    assert_eq!(idx.len(), 1, "still exactly one logical document");
}

#[test]
fn delete_removes_document() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "text one").unwrap();
    idx.upsert(2, "text two").unwrap();
    idx.delete(1).unwrap();
    idx.commit().unwrap();
    let hits = idx.search(&TextQuery::Match("text".into()), 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(vid(&hits[0]), VertexId(2));
}

#[test]
fn delete_missing_vertex_is_noop() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.delete(999).unwrap();
    idx.commit().unwrap();
    assert_eq!(idx.len(), 0);
}

#[test]
fn delete_then_reupsert_restores_searchability() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "raft consensus algorithm").unwrap();
    idx.delete(1).unwrap();
    idx.commit().unwrap();
    let gone = idx.search(&TextQuery::Match("raft".into()), 10).unwrap();
    assert!(gone.is_empty());
    idx.upsert(1, "raft consensus algorithm").unwrap();
    let back = idx.search(&TextQuery::Match("raft".into()), 10).unwrap();
    assert_eq!(back.len(), 1);
    assert_eq!(vid(&back[0]), VertexId(1));
}

#[test]
fn bm25_ranks_more_relevant_doc_higher() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(
        1,
        "raft raft raft consensus consensus algorithm distributed",
    )
    .unwrap();
    idx.upsert(2, "this document mentions raft only once in passing")
        .unwrap();
    let hits = idx.search(&TextQuery::Match("raft".into()), 10).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(vid(&hits[0]), VertexId(1));
    assert!(
        hits[0].score > hits[1].score,
        "more frequent term should rank higher"
    );
}

#[test]
fn match_all_requires_every_term() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "raft consensus").unwrap();
    idx.upsert(2, "raft").unwrap();
    let or_hits = idx
        .search(&TextQuery::Match("raft consensus".into()), 10)
        .unwrap();
    assert_eq!(or_hits.len(), 2, "OR semantics: both should match");
    let and_hits = idx
        .search(&TextQuery::MatchAll("raft consensus".into()), 10)
        .unwrap();
    assert_eq!(and_hits.len(), 1);
    assert_eq!(vid(&and_hits[0]), VertexId(1));
}

#[test]
fn phrase_query_respects_order() {
    let idx = fresh_index(Analyzer::Standard);
    idx.upsert(1, "quick brown fox").unwrap();
    idx.upsert(2, "fox brown quick").unwrap();
    let hits = idx
        .search(
            &TextQuery::Phrase {
                text: "quick brown fox".into(),
                slop: 0,
            },
            10,
        )
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(vid(&hits[0]), VertexId(1));
}

#[test]
fn phrase_query_requires_positions() {
    let idx = TantivyTextIndex::in_memory(
        ElementType::Vertex,
        TextIndexConfig {
            store_positions: false,
            commit_every: 1,
            ..Default::default()
        },
    )
    .unwrap();
    idx.upsert(1, "the quick brown fox").unwrap();
    let err = idx
        .search(
            &TextQuery::Phrase {
                text: "quick brown".into(),
                slop: 0,
            },
            10,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::storage::text::TextIndexError::UnsupportedConfig(_)
    ));
}

#[test]
fn prefix_query_matches_term_prefix() {
    let idx = fresh_index(Analyzer::Standard);
    idx.upsert(1, "consensus").unwrap();
    idx.upsert(2, "consequence").unwrap();
    idx.upsert(3, "rooster").unwrap();
    let hits = idx.search(&TextQuery::Prefix("conse".into()), 10).unwrap();
    let ids: std::collections::HashSet<_> = hits.iter().map(vid).collect();
    assert_eq!(
        ids,
        [VertexId(1), VertexId(2)]
            .into_iter()
            .collect::<std::collections::HashSet<_>>()
    );
}

#[test]
fn boolean_and_with_negation() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "raft consensus").unwrap();
    idx.upsert(2, "raft paxos comparison").unwrap();
    idx.upsert(3, "paxos only").unwrap();
    let q = TextQuery::all([
        TextQuery::Match("raft".into()),
        TextQuery::not(TextQuery::Match("paxos".into())),
    ]);
    let hits = idx.search(&q, 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(vid(&hits[0]), VertexId(1));
}

#[test]
fn boolean_or_unions_results() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "raft").unwrap();
    idx.upsert(2, "paxos").unwrap();
    idx.upsert(3, "viewstamp").unwrap();
    let q = TextQuery::any([
        TextQuery::Match("raft".into()),
        TextQuery::Match("paxos".into()),
    ]);
    let hits = idx.search(&q, 10).unwrap();
    let ids: std::collections::HashSet<_> = hits.iter().map(vid).collect();
    assert_eq!(
        ids,
        [VertexId(1), VertexId(2)]
            .into_iter()
            .collect::<std::collections::HashSet<_>>()
    );
}

#[test]
fn purely_negative_query_rejected() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "anything").unwrap();
    // Bare top-level Not is fine via compile (it AllQuery MUST_NOTs); but a
    // purely-negative *boolean* (`And [Not]`) we explicitly reject.
    let q = TextQuery::all([TextQuery::not(TextQuery::Match("foo".into()))]);
    let err = idx.search(&q, 10).unwrap_err();
    assert!(matches!(
        err,
        crate::storage::text::TextIndexError::QueryParse(_)
    ));
}

#[test]
fn bare_top_level_not_returns_complement() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "raft").unwrap();
    idx.upsert(2, "paxos").unwrap();
    let q = TextQuery::not(TextQuery::Match("raft".into()));
    let hits = idx.search(&q, 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(vid(&hits[0]), VertexId(2));
}

#[test]
fn results_sorted_descending_by_score_with_at_most_k() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    for i in 0..20 {
        let body = format!("term {}", "raft ".repeat(i));
        idx.upsert(i as u64, &body).unwrap();
    }
    let hits = idx.search(&TextQuery::Match("raft".into()), 5).unwrap();
    assert_eq!(hits.len(), 5);
    for win in hits.windows(2) {
        assert!(
            win[0].score >= win[1].score,
            "scores must be non-increasing"
        );
        assert!(win[0].score >= 0.0);
    }
}

#[test]
fn empty_match_string_matches_nothing() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "anything").unwrap();
    let hits = idx.search(&TextQuery::Match(String::new()), 10).unwrap();
    assert!(hits.is_empty());
}

#[test]
fn analyzer_aware_match_finds_stemmed_forms() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "running runners run").unwrap();
    let hits = idx.search(&TextQuery::Match("runs".into()), 10).unwrap();
    assert_eq!(hits.len(), 1, "stemming should let 'runs' match 'run'");
}

#[test]
fn raw_analyzer_is_exact_match() {
    let idx = fresh_index(Analyzer::Raw);
    idx.upsert(1, "exact-id-123").unwrap();
    idx.upsert(2, "exact-id-456").unwrap();
    let hits = idx.search_str("\"exact-id-123\"", 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(vid(&hits[0]), VertexId(1));
}

#[test]
fn config_rejects_nondefault_bm25() {
    let result = TantivyTextIndex::in_memory(
        ElementType::Vertex,
        TextIndexConfig {
            bm25_k1: 1.5,
            ..Default::default()
        },
    );
    assert!(matches!(
        result.err(),
        Some(crate::storage::text::TextIndexError::UnsupportedConfig(_))
    ));
    let result = TantivyTextIndex::in_memory(
        ElementType::Vertex,
        TextIndexConfig {
            bm25_b: 0.5,
            ..Default::default()
        },
    );
    assert!(matches!(
        result.err(),
        Some(crate::storage::text::TextIndexError::UnsupportedConfig(_))
    ));
}

#[test]
fn config_rejects_too_small_writer_memory() {
    let result = TantivyTextIndex::in_memory(
        ElementType::Vertex,
        TextIndexConfig {
            writer_memory_bytes: 1_000_000,
            ..Default::default()
        },
    );
    assert!(matches!(
        result.err(),
        Some(crate::storage::text::TextIndexError::UnsupportedConfig(_))
    ));
}

#[test]
fn manual_commit_makes_pending_upserts_visible() {
    let idx = TantivyTextIndex::in_memory(
        ElementType::Vertex,
        TextIndexConfig {
            commit_every: usize::MAX, // disable auto-commit
            ..Default::default()
        },
    )
    .unwrap();
    idx.upsert(1, "delayed text").unwrap();
    let before = idx.search(&TextQuery::Match("delayed".into()), 10).unwrap();
    assert!(before.is_empty(), "uncommitted write should be invisible");
    idx.commit().unwrap();
    let after = idx.search(&TextQuery::Match("delayed".into()), 10).unwrap();
    assert_eq!(after.len(), 1);
}

#[test]
fn merge_is_safe_to_call_repeatedly() {
    let idx = fresh_index(Analyzer::StandardEnglish);
    idx.upsert(1, "doc one").unwrap();
    idx.upsert(2, "doc two").unwrap();
    idx.merge().unwrap();
    idx.merge().unwrap();
    // After merging, search should still work.
    let hits = idx.search(&TextQuery::Match("doc".into()), 10).unwrap();
    assert_eq!(hits.len(), 2);
}

#[test]
fn unsupported_query_string_surfaces_parse_error() {
    let idx = fresh_index(Analyzer::Standard);
    let err = idx.search_str("(((", 10).unwrap_err();
    assert!(matches!(
        err,
        crate::storage::text::TextIndexError::QueryParse(_)
    ));
}

// ---------------------------------------------------------------------------
// Property-based tests
// ---------------------------------------------------------------------------

mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn ascii_word() -> impl Strategy<Value = String> {
        proptest::string::string_regex("[a-z]{3,8}").unwrap()
    }

    fn ascii_doc() -> impl Strategy<Value = String> {
        proptest::collection::vec(ascii_word(), 1..8).prop_map(|ws| ws.join(" "))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// `upsert(v, s); upsert(v, s)` ≡ `upsert(v, s)`.
        #[test]
        fn upsert_idempotent(text in ascii_doc()) {
            let idx = fresh_index(Analyzer::Standard);
            idx.upsert(1, &text).unwrap();
            idx.upsert(1, &text).unwrap();
            prop_assert_eq!(idx.len(), 1);
        }

        /// Search results are non-increasing in score and obey the k cap.
        #[test]
        fn search_results_sorted_and_bounded(
            docs in proptest::collection::vec(ascii_doc(), 0..20),
            term in ascii_word(),
            k in 1usize..32,
        ) {
            let idx = fresh_index(Analyzer::Standard);
            for (i, d) in docs.iter().enumerate() {
                idx.upsert(i as u64, d).unwrap();
            }
            let hits = idx.search(&TextQuery::Match(term), k).unwrap();
            prop_assert!(hits.len() <= k);
            for win in hits.windows(2) {
                prop_assert!(win[0].score >= win[1].score);
                prop_assert!(win[0].score >= 0.0);
            }
        }

        /// `Match(t)` returns a non-empty result iff at least one indexed doc
        /// contains an analyzer-equivalent token of `t`.
        #[test]
        fn match_iff_analyzer_equivalent_token_exists(
            docs in proptest::collection::vec(ascii_doc(), 1..10),
            term in ascii_word(),
        ) {
            let idx = fresh_index(Analyzer::Standard);
            for (i, d) in docs.iter().enumerate() {
                idx.upsert(i as u64, d).unwrap();
            }
            let normalized: Vec<String> = Analyzer::Standard.tokens(&term).unwrap();
            let any_doc_contains = docs.iter().any(|d| {
                let toks = Analyzer::Standard.tokens(d).unwrap();
                normalized.iter().any(|t| toks.iter().any(|w| w == t))
            });
            let hits = idx.search(&TextQuery::Match(term), 50).unwrap();
            prop_assert_eq!(!hits.is_empty(), any_doc_contains);
        }

        /// Deleting then re-inserting the same text returns the document to a
        /// searchable state.
        #[test]
        fn delete_then_reupsert_round_trip(text in ascii_doc(), term in ascii_word()) {
            let idx = fresh_index(Analyzer::Standard);
            idx.upsert(7, &text).unwrap();
            let before = idx.search(&TextQuery::Match(term.clone()), 10).unwrap();
            idx.delete(7).unwrap();
            idx.commit().unwrap();
            let middle = idx.search(&TextQuery::Match(term.clone()), 10).unwrap();
            prop_assert!(middle.is_empty());
            idx.upsert(7, &text).unwrap();
            let after = idx.search(&TextQuery::Match(term), 10).unwrap();
            prop_assert_eq!(before.len(), after.len());
        }
    }
}
