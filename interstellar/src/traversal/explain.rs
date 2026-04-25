//! Traversal explanation types for `explain()`.
//!
//! Provides structured descriptions of traversal pipelines without executing them.
//! Used for debugging, logging, and understanding query plans.

use std::fmt;

use crate::traversal::step::DynStep;
use crate::traversal::traverser::TraversalSource;

// =============================================================================
// StepCategory
// =============================================================================

/// Category of traversal step for classification in `explain()` output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepCategory {
    /// Source steps: V(), E(), inject()
    Source,
    /// Navigation: out(), in_(), both(), outE(), etc.
    Navigation,
    /// Filter: has(), hasLabel(), where_(), limit(), etc.
    Filter,
    /// Transform: values(), valueMap(), id(), label(), etc.
    Transform,
    /// Aggregation: group(), groupCount(), fold(), count(), etc.
    Aggregation,
    /// Branch: union(), coalesce(), choose(), etc.
    Branch,
    /// Side Effect: aggregate(), store(), sideEffect(), etc.
    SideEffect,
    /// Modulator: as_(), by(), etc.
    Modulator,
    /// Unknown/custom steps
    Other,
}

impl fmt::Display for StepCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Source => write!(f, "Source"),
            Self::Navigation => write!(f, "Navigation"),
            Self::Filter => write!(f, "Filter"),
            Self::Transform => write!(f, "Transform"),
            Self::Aggregation => write!(f, "Aggregation"),
            Self::Branch => write!(f, "Branch"),
            Self::SideEffect => write!(f, "SideEffect"),
            Self::Modulator => write!(f, "Modulator"),
            Self::Other => write!(f, "Other"),
        }
    }
}

// =============================================================================
// StepExplanation
// =============================================================================

/// Explanation of a single traversal step.
#[derive(Debug, Clone)]
pub struct StepExplanation {
    /// Step name (from `dyn_name()`)
    pub name: &'static str,
    /// Zero-based index in the pipeline
    pub index: usize,
    /// Whether this step blocks streaming execution
    pub is_barrier: bool,
    /// Step category
    pub category: StepCategory,
    /// Optional human-readable description of step configuration
    pub description: Option<String>,
}

// =============================================================================
// TraversalExplanation
// =============================================================================

/// Structured description of a traversal pipeline.
///
/// Returned by `explain()` on `BoundTraversal` and `Traversal`.
/// Does not execute the traversal — only inspects its structure.
///
/// # Example
///
/// ```ignore
/// let explanation = g.v().out().has_label("person").explain();
/// println!("{}", explanation);
/// ```
#[derive(Debug, Clone)]
pub struct TraversalExplanation {
    /// Source description (e.g., "V()", "V(1,2,3)", "E()")
    pub source: Option<String>,
    /// Ordered list of step explanations
    pub steps: Vec<StepExplanation>,
    /// Whether any step is a barrier
    pub has_barriers: bool,
    /// Total number of steps
    pub step_count: usize,
}

impl TraversalExplanation {
    /// Build an explanation from a traversal source and steps.
    pub fn from_steps(
        source: Option<&TraversalSource>,
        steps: &[Box<dyn DynStep>],
    ) -> Self {
        let step_explanations: Vec<StepExplanation> = steps
            .iter()
            .enumerate()
            .map(|(i, step)| StepExplanation {
                name: step.dyn_name(),
                index: i,
                is_barrier: step.is_barrier(),
                category: step.category(),
                description: step.describe(),
            })
            .collect();

        let has_barriers = step_explanations.iter().any(|s| s.is_barrier);
        let step_count = step_explanations.len();

        Self {
            source: source.map(format_source),
            steps: step_explanations,
            has_barriers,
            step_count,
        }
    }
}

/// Format a `TraversalSource` for display.
fn format_source(source: &TraversalSource) -> String {
    match source {
        TraversalSource::AllVertices => "V() [all vertices]".to_string(),
        TraversalSource::Vertices(ids) => {
            if ids.len() <= 5 {
                let id_strs: Vec<String> = ids.iter().map(|id| format!("{}", id.0)).collect();
                format!("V({}) [{} vertex/vertices]", id_strs.join(", "), ids.len())
            } else {
                format!("V(...) [{} vertices]", ids.len())
            }
        }
        TraversalSource::AllEdges => "E() [all edges]".to_string(),
        TraversalSource::Edges(ids) => {
            if ids.len() <= 5 {
                let id_strs: Vec<String> = ids.iter().map(|id| format!("{}", id.0)).collect();
                format!("E({}) [{} edge(s)]", id_strs.join(", "), ids.len())
            } else {
                format!("E(...) [{} edges]", ids.len())
            }
        }
        TraversalSource::Inject(values) => {
            format!("inject(...) [{} values]", values.len())
        }
        #[cfg(feature = "full-text")]
        TraversalSource::VerticesWithTextScore(pairs) => {
            format!("textSearch(...) [{} results]", pairs.len())
        }
    }
}

impl fmt::Display for TraversalExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Traversal Explanation")?;
        writeln!(f, "=====================")?;

        if let Some(ref source) = self.source {
            writeln!(f, "Source: {source}")?;
        } else {
            writeln!(f, "Source: (anonymous)")?;
        }

        if self.has_barriers {
            writeln!(f, "Barriers: Yes (streaming disabled)")?;
        } else {
            writeln!(f, "Barriers: No")?;
        }

        writeln!(f)?;

        if self.steps.is_empty() {
            writeln!(f, "Steps: (none)")?;
        } else {
            writeln!(f, "Steps ({}):", self.step_count)?;
            for step in &self.steps {
                let barrier_marker = if step.is_barrier { "  BARRIER" } else { "" };
                let desc = step
                    .description
                    .as_deref()
                    .map(|d| format!("  {d}"))
                    .unwrap_or_default();
                writeln!(
                    f,
                    "  [{:>2}] {:<12} {:<12}{barrier_marker}{desc}",
                    step.index, step.name, step.category,
                )?;
            }
        }

        Ok(())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::VertexId;

    #[test]
    fn step_category_display() {
        assert_eq!(format!("{}", StepCategory::Navigation), "Navigation");
        assert_eq!(format!("{}", StepCategory::Filter), "Filter");
        assert_eq!(format!("{}", StepCategory::Other), "Other");
    }

    #[test]
    fn format_source_all_vertices() {
        let s = format_source(&TraversalSource::AllVertices);
        assert_eq!(s, "V() [all vertices]");
    }

    #[test]
    fn format_source_specific_vertices() {
        let s = format_source(&TraversalSource::Vertices(vec![VertexId(1), VertexId(2)]));
        assert!(s.contains("V(1, 2)"));
        assert!(s.contains("2 vertex"));
    }

    #[test]
    fn format_source_many_vertices() {
        let ids: Vec<VertexId> = (0..10).map(VertexId).collect();
        let s = format_source(&TraversalSource::Vertices(ids));
        assert!(s.contains("V(...)"));
        assert!(s.contains("10 vertices"));
    }

    #[test]
    fn format_source_all_edges() {
        let s = format_source(&TraversalSource::AllEdges);
        assert_eq!(s, "E() [all edges]");
    }

    #[test]
    fn format_source_inject() {
        use crate::value::Value;
        let s = format_source(&TraversalSource::Inject(vec![Value::Int(1), Value::Int(2)]));
        assert!(s.contains("inject(...)"));
        assert!(s.contains("2 values"));
    }

    #[test]
    fn explanation_empty() {
        let exp = TraversalExplanation::from_steps(None, &[]);
        assert_eq!(exp.step_count, 0);
        assert!(!exp.has_barriers);
        assert!(exp.source.is_none());
    }

    #[test]
    fn explanation_display_empty() {
        let exp = TraversalExplanation::from_steps(None, &[]);
        let display = format!("{exp}");
        assert!(display.contains("(anonymous)"));
        assert!(display.contains("(none)"));
    }

    #[test]
    fn explanation_display_with_source() {
        let exp =
            TraversalExplanation::from_steps(Some(&TraversalSource::AllVertices), &[]);
        let display = format!("{exp}");
        assert!(display.contains("V() [all vertices]"));
    }
}
