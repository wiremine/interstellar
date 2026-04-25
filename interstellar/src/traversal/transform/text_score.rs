//! `text_score` step — extract the full-text-search relevance score that
//! `searchTextV` / `searchTextE` (and the corresponding Rust API) attach to
//! each emitted traverser via the sack.
//!
//! The score is stored as `f32` in the traverser sack and surfaced as
//! [`Value::Float`]. If the upstream step did not attach a score (e.g. the
//! traverser came from a non-FTS source), `text_score` emits
//! [`Value::Null`] rather than failing — this matches the spec-55c
//! decision that `__.textScore()` is a pure projection that should not
//! abort otherwise-valid pipelines.
//!
//! See `specs/spec-55c-fulltext-query-languages.md` §3.3.

use crate::traversal::{ExecutionContext, Traverser};
use crate::value::Value;

/// Transform step that projects the FTS score from the traverser's sack.
///
/// # Behavior
///
/// - Each input traverser produces exactly one output traverser (1:1).
/// - When the sack contains an `f32` score, output value is
///   `Value::Float(score as f64)`.
/// - When the sack is empty or contains a non-`f32` payload, output value
///   is `Value::Null`.
/// - All other traverser metadata (path, loops, bulk, sack) is preserved.
#[derive(Clone, Copy, Debug, Default)]
pub struct TextScoreStep;

impl TextScoreStep {
    /// Create a new `TextScoreStep`.
    pub fn new() -> Self {
        Self
    }
}

#[inline]
fn project_score(t: &Traverser) -> Value {
    match t.get_sack::<f32>() {
        Some(score) => Value::Float(*score as f64),
        None => Value::Null,
    }
}

impl crate::traversal::step::Step for TextScoreStep {
    type Iter<'a>
        = impl Iterator<Item = Traverser> + 'a
    where
        Self: 'a;

    fn apply<'a>(
        &'a self,
        _ctx: &'a ExecutionContext<'a>,
        input: Box<dyn Iterator<Item = Traverser> + 'a>,
    ) -> Self::Iter<'a> {
        input.map(|t| {
            let v = project_score(&t);
            t.with_value(v)
        })
    }

    fn name(&self) -> &'static str {
        "text_score"
    }

    fn category(&self) -> crate::traversal::explain::StepCategory {
        crate::traversal::explain::StepCategory::Transform
    }

    fn apply_streaming(
        &self,
        _ctx: crate::traversal::context::StreamingContext,
        input: Traverser,
    ) -> Box<dyn Iterator<Item = Traverser> + Send + 'static> {
        let v = project_score(&input);
        Box::new(std::iter::once(input.with_value(v)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Graph;
    use crate::traversal::step::Step;
    use crate::traversal::traverser::box_sack;
    use crate::traversal::ExecutionContext;
    use crate::traversal::SnapshotLike;

    #[test]
    fn projects_score_when_sack_has_f32() {
        let g = Graph::new();
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());

        let mut t = Traverser::new(Value::Int(1));
        t.sack = Some(box_sack(0.875f32));

        let step = TextScoreStep::new();
        let out: Vec<_> = step.apply(&ctx, Box::new(std::iter::once(t))).collect();
        assert_eq!(out.len(), 1);
        match &out[0].value {
            Value::Float(f) => assert!((f - 0.875).abs() < 1e-6),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn emits_null_when_sack_is_none() {
        let g = Graph::new();
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());

        let t = Traverser::new(Value::Int(1));
        assert!(t.sack.is_none());

        let step = TextScoreStep::new();
        let out: Vec<_> = step.apply(&ctx, Box::new(std::iter::once(t))).collect();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0].value, Value::Null));
    }

    #[test]
    fn emits_null_when_sack_has_wrong_type() {
        let g = Graph::new();
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());

        let mut t = Traverser::new(Value::Int(1));
        t.sack = Some(box_sack(42i64)); // wrong type

        let step = TextScoreStep::new();
        let out: Vec<_> = step.apply(&ctx, Box::new(std::iter::once(t))).collect();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0].value, Value::Null));
    }

    #[test]
    fn preserves_path_and_loops_and_bulk() {
        let g = Graph::new();
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());

        let mut t = Traverser::new(Value::Int(1));
        t.sack = Some(box_sack(1.0f32));
        t.extend_path_labeled("hit");
        t.loops = 3;
        t.bulk = 7;

        let step = TextScoreStep::new();
        let out: Vec<_> = step.apply(&ctx, Box::new(std::iter::once(t))).collect();
        assert_eq!(out.len(), 1);
        assert!(out[0].path.has_label("hit"));
        assert_eq!(out[0].loops, 3);
        assert_eq!(out[0].bulk, 7);
    }

    #[test]
    fn streaming_path_matches_iterator_path() {
        use crate::traversal::context::StreamingContext;
        let g = Graph::new();
        let snap = g.snapshot();

        let mut t = Traverser::new(Value::Int(1));
        t.sack = Some(box_sack(0.5f32));

        let step = TextScoreStep::new();
        let sctx = StreamingContext::new(snap.arc_streamable(), snap.arc_interner());
        let out: Vec<_> = step.apply_streaming(sctx, t).collect();
        assert_eq!(out.len(), 1);
        match &out[0].value {
            Value::Float(f) => assert!((f - 0.5).abs() < 1e-6),
            other => panic!("expected Float, got {:?}", other),
        }
    }

    #[test]
    fn empty_input_yields_empty_output() {
        let g = Graph::new();
        let snap = g.snapshot();
        let ctx = ExecutionContext::new(snap.storage(), snap.interner());

        let step = TextScoreStep::new();
        let input: Vec<Traverser> = vec![];
        let out: Vec<_> = step.apply(&ctx, Box::new(input.into_iter())).collect();
        assert!(out.is_empty());
    }

    #[test]
    fn name_is_text_score() {
        let step = TextScoreStep::new();
        assert_eq!(<TextScoreStep as Step>::name(&step), "text_score");
    }
}
