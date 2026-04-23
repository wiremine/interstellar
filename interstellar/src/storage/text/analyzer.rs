//! Analyzers for the full-text index.
//!
//! An [`Analyzer`] is the spec-level enum users pick when configuring a
//! [`TextIndexConfig`](super::TextIndexConfig). At index-build time we
//! translate it into a Tantivy [`TextAnalyzer`] and register it with that
//! index's [`TokenizerManager`] under a unique name.
//!
//! See [spec-55 §Analyzers](../../../../specs/spec-55-fulltext-search.md) for
//! the canonical mapping table.

use tantivy::tokenizer::{
    Language, LowerCaser, NgramTokenizer, RawTokenizer, RemoveLongFilter, SimpleTokenizer,
    Stemmer, StopWordFilter, TextAnalyzer, WhitespaceTokenizer,
};

use super::TextIndexError;

/// Tokens longer than this are dropped to avoid pathological inputs creating
/// gigantic terms. 255 codepoints is generous for words and matches common
/// search-engine defaults.
const MAX_TOKEN_LEN: usize = 255;

/// Built-in analyzer pipelines.
///
/// `Eq` is implemented manually because [`Self::NGram`] holds a struct
/// variant; floats are not involved so derive(Eq) would be safe but we
/// prefer to keep the variant set explicit and easy to extend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Analyzer {
    /// Lowercase + Unicode word segmentation + English stopwords + Porter
    /// stemming. The recommended default for English text.
    StandardEnglish,

    /// Lowercase + Unicode word segmentation. No stopwords, no stemming.
    Standard,

    /// Whitespace-only split. Case-preserving. Useful for pre-normalized text
    /// or identifiers that must survive verbatim.
    Whitespace,

    /// No tokenization. The full string becomes a single term. Useful for
    /// indexing IDs, tags, or enums where exact-match semantics are needed.
    Raw,

    /// Character n-grams. Lowercases, then emits all overlapping windows of
    /// length `min..=max`. Good for substring / prefix-style matching at the
    /// cost of a much larger term dictionary.
    NGram { min: usize, max: usize },
}

impl Analyzer {
    /// Stable identifier used to register this analyzer with a Tantivy
    /// [`TokenizerManager`]. The same string must be used when configuring a
    /// `TextField`'s `tokenizer` so search-time and index-time pipelines line
    /// up.
    pub fn tokenizer_name(&self) -> String {
        match self {
            Analyzer::StandardEnglish => "interstellar_standard_en".to_string(),
            Analyzer::Standard => "interstellar_standard".to_string(),
            Analyzer::Whitespace => "interstellar_whitespace".to_string(),
            Analyzer::Raw => "interstellar_raw".to_string(),
            Analyzer::NGram { min, max } => format!("interstellar_ngram_{}_{}", min, max),
        }
    }

    /// Construct the Tantivy [`TextAnalyzer`] this variant represents.
    ///
    /// Returns [`TextIndexError::UnsupportedConfig`] if the variant carries
    /// invalid parameters (e.g. `NGram { min: 0, .. }` or `min > max`).
    pub fn build(&self) -> Result<TextAnalyzer, TextIndexError> {
        Ok(match self {
            Analyzer::StandardEnglish => TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
                .filter(LowerCaser)
                .filter(
                    StopWordFilter::new(Language::English).expect("English stopwords are bundled"),
                )
                .filter(Stemmer::new(Language::English))
                .build(),
            Analyzer::Standard => TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
                .filter(LowerCaser)
                .build(),
            Analyzer::Whitespace => TextAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
                .build(),
            Analyzer::Raw => TextAnalyzer::builder(RawTokenizer::default()).build(),
            Analyzer::NGram { min, max } => {
                if *min == 0 || *max == 0 {
                    return Err(TextIndexError::UnsupportedConfig(format!(
                        "NGram analyzer requires min >= 1 and max >= 1 (got min={}, max={})",
                        min, max
                    )));
                }
                if min > max {
                    return Err(TextIndexError::UnsupportedConfig(format!(
                        "NGram analyzer requires min <= max (got min={}, max={})",
                        min, max
                    )));
                }
                let ngram = NgramTokenizer::new(*min, *max, false).map_err(|e| {
                    TextIndexError::UnsupportedConfig(format!(
                        "invalid NGram parameters: {}",
                        e
                    ))
                })?;
                TextAnalyzer::builder(ngram)
                    .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
                    .filter(LowerCaser)
                    .build()
            }
        })
    }

    /// Tokenize `text` with this analyzer's pipeline and return the produced
    /// terms in order. Useful for testing and for the
    /// `analyzer-equivalent-token` proptest.
    #[cfg(test)]
    pub fn tokens(&self, text: &str) -> Result<Vec<String>, TextIndexError> {
        use tantivy::tokenizer::TokenStream;

        let mut analyzer = self.build()?;
        let mut stream = analyzer.token_stream(text);
        let mut out = Vec::new();
        while stream.advance() {
            out.push(stream.token().text.clone());
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_english_lowercases_stems_and_removes_stopwords() {
        let toks = Analyzer::StandardEnglish
            .tokens("The quick brown foxes are jumping")
            .unwrap();
        // "the", "are" removed by stopwords; "foxes" -> "fox", "jumping" -> "jump"
        assert_eq!(toks, vec!["quick", "brown", "fox", "jump"]);
    }

    #[test]
    fn standard_lowercases_but_does_not_stem() {
        let toks = Analyzer::Standard
            .tokens("The Quick Brown FOXES")
            .unwrap();
        assert_eq!(toks, vec!["the", "quick", "brown", "foxes"]);
    }

    #[test]
    fn whitespace_preserves_case_and_splits_on_whitespace_only() {
        let toks = Analyzer::Whitespace
            .tokens("Hello,  WORLD! quick-brown")
            .unwrap();
        assert_eq!(toks, vec!["Hello,", "WORLD!", "quick-brown"]);
    }

    #[test]
    fn raw_emits_a_single_token() {
        let toks = Analyzer::Raw.tokens("hello world  HELLO").unwrap();
        assert_eq!(toks, vec!["hello world  HELLO"]);
    }

    #[test]
    fn ngram_emits_all_windows() {
        let toks = Analyzer::NGram { min: 2, max: 3 }.tokens("abcd").unwrap();
        // 2-grams: ab, bc, cd; 3-grams: abc, bcd
        assert!(toks.contains(&"ab".to_string()));
        assert!(toks.contains(&"bc".to_string()));
        assert!(toks.contains(&"cd".to_string()));
        assert!(toks.contains(&"abc".to_string()));
        assert!(toks.contains(&"bcd".to_string()));
    }

    #[test]
    fn ngram_zero_min_rejected() {
        let result = Analyzer::NGram { min: 0, max: 3 }.build();
        assert!(matches!(result, Err(TextIndexError::UnsupportedConfig(_))));
    }

    #[test]
    fn ngram_min_greater_than_max_rejected() {
        let result = Analyzer::NGram { min: 5, max: 2 }.build();
        assert!(matches!(result, Err(TextIndexError::UnsupportedConfig(_))));
    }

    #[test]
    fn tokenizer_names_are_stable_and_distinct() {
        let names = [
            Analyzer::StandardEnglish.tokenizer_name(),
            Analyzer::Standard.tokenizer_name(),
            Analyzer::Whitespace.tokenizer_name(),
            Analyzer::Raw.tokenizer_name(),
            Analyzer::NGram { min: 2, max: 3 }.tokenizer_name(),
            Analyzer::NGram { min: 3, max: 4 }.tokenizer_name(),
        ];
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), names.len());
    }

    #[test]
    fn long_tokens_are_filtered() {
        let big = "a".repeat(MAX_TOKEN_LEN + 10);
        let input = format!("ok {} fine", big);
        let toks = Analyzer::Standard.tokens(&input).unwrap();
        assert_eq!(toks, vec!["ok", "fine"]);
    }
}
