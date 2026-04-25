//! Traversal explanation types for `explain()`.
//!
//! Provides structured descriptions of traversal pipelines without executing them.
//! Used for debugging, logging, and understanding query plans.

use std::fmt;

use crate::index::IndexSpec;
use crate::traversal::step::DynStep;
use crate::traversal::traverser::TraversalSource;
use crate::value::Value;

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
    /// Index hint: which index (if any) covers this step's filter key
    pub index_hint: Option<String>,
    /// Whether this step has a filter key (for showing [no index] hint)
    pub has_filter_key: bool,
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
        indexes: &[IndexSpec],
        text_indexes: &[String],
    ) -> Self {
        let step_explanations: Vec<StepExplanation> = steps
            .iter()
            .enumerate()
            .map(|(i, step)| {
                let filter_key = step.filter_key();
                let index_hint = filter_key.as_deref().and_then(|key| {
                    // Check property indexes first
                    indexes
                        .iter()
                        .find(|idx| idx.property == key)
                        .map(|idx| format!("{} ({:?})", idx.name, idx.index_type))
                        // Then check text indexes
                        .or_else(|| {
                            if text_indexes.iter().any(|t| t == key) {
                                Some("full-text".to_string())
                            } else {
                                None
                            }
                        })
                });

                StepExplanation {
                    name: step.dyn_name(),
                    index: i,
                    is_barrier: step.is_barrier(),
                    category: step.category(),
                    description: step.describe(),
                    index_hint,
                    has_filter_key: filter_key.is_some(),
                }
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

/// Format anonymous traversal steps as a compact pipeline string.
///
/// E.g. `__.out("knows").hasLabel("person")` becomes `out("knows").hasLabel("person")`.
/// Used by `describe()` on branch/repeat steps.
pub fn format_traversal_steps(steps: &[Box<dyn DynStep>]) -> String {
    if steps.is_empty() {
        return "identity".to_string();
    }
    steps
        .iter()
        .map(|s| {
            let name = s.dyn_name();
            match s.describe() {
                Some(desc) => format!("{name}({desc})"),
                None => format!("{name}()"),
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Format a `Value` concisely for explain output.
pub fn format_value(v: &Value) -> String {
    match v {
        Value::String(s) => format!("\"{s}\""),
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => format!("{v:?}"),
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
            format!("searchTextV(...) [{} results]", pairs.len())
        }
        #[cfg(feature = "full-text")]
        TraversalSource::EdgesWithTextScore(pairs) => {
            format!("searchTextE(...) [{} results]", pairs.len())
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

        if self.steps.is_empty() {
            writeln!(f, "Steps:  (none)")?;
            return Ok(());
        }

        // Compute column widths
        let name_width = self
            .steps
            .iter()
            .map(|s| s.name.len())
            .max()
            .unwrap_or(4)
            .max(4);
        let cat_width = self
            .steps
            .iter()
            .map(|s| format!("{}", s.category).len())
            .max()
            .unwrap_or(8)
            .max(8);

        // Summary line: count by category
        let mut cat_counts: Vec<(StepCategory, usize)> = Vec::new();
        for step in &self.steps {
            if let Some(entry) = cat_counts.iter_mut().find(|(c, _)| *c == step.category) {
                entry.1 += 1;
            } else {
                cat_counts.push((step.category, 1));
            }
        }
        let barrier_count = self.steps.iter().filter(|s| s.is_barrier).count();
        let summary_parts: Vec<String> = cat_counts
            .iter()
            .map(|(cat, n)| format!("{n} {cat}"))
            .collect();
        let barrier_note = if barrier_count > 0 {
            format!(", {} barrier", barrier_count)
        } else {
            String::new()
        };
        writeln!(
            f,
            "Steps:  {} ({}{})",
            self.step_count,
            summary_parts.join(", "),
            barrier_note,
        )?;

        writeln!(f)?;

        // Check if any step has index information to show
        let show_index_col = self.steps.iter().any(|s| s.has_filter_key);

        // Compute index column width
        let idx_width = if show_index_col {
            self.steps
                .iter()
                .filter_map(|s| {
                    if s.index_hint.is_some() || s.has_filter_key {
                        let text = match &s.index_hint {
                            Some(hint) => hint.len(),
                            None => 8, // "no index"
                        };
                        Some(text)
                    } else {
                        None
                    }
                })
                .max()
                .unwrap_or(5)
                .max(5)
        } else {
            0
        };

        // Column prefix width: "  0  step      Category    "
        let prefix_width = 5 + name_width + 2 + cat_width + 2;

        // Table header
        if show_index_col {
            writeln!(
                f,
                "  #  {:<name_width$}  {:<cat_width$}  {:<idx_width$}  Description",
                "Step", "Category", "Index",
                name_width = name_width,
                cat_width = cat_width,
                idx_width = idx_width,
            )?;
            let rule_len = prefix_width + idx_width + 2 + 11;
            writeln!(f, "  {}", "─".repeat(rule_len))?;
        } else {
            writeln!(
                f,
                "  #  {:<name_width$}  {:<cat_width$}  Description",
                "Step", "Category",
                name_width = name_width,
                cat_width = cat_width,
            )?;
            let rule_len = prefix_width + 11;
            writeln!(f, "  {}", "─".repeat(rule_len))?;
        }

        // Steps
        for step in &self.steps {
            // Barrier separator before barrier steps
            if step.is_barrier {
                let rule_len = if show_index_col {
                    prefix_width + idx_width + 2 + 11
                } else {
                    prefix_width + 11
                };
                writeln!(
                    f,
                    "  {0}── barrier {0}──",
                    "─".repeat((rule_len.saturating_sub(13)) / 2)
                )?;
            }

            let cat_str = format!("{}", step.category);
            let desc = step.description.as_deref().unwrap_or("");

            // Split description into first line and continuation lines
            let mut desc_lines = desc.split('\n');
            let first_line = desc_lines.next().unwrap_or("");

            if show_index_col {
                let idx_str = match &step.index_hint {
                    Some(hint) => hint.as_str(),
                    None if step.has_filter_key => "no index",
                    None => "",
                };
                writeln!(
                    f,
                    "  {:<2} {:<name_width$}  {:<cat_width$}  {:<idx_width$}  {first_line}",
                    step.index,
                    step.name,
                    cat_str,
                    idx_str,
                    name_width = name_width,
                    cat_width = cat_width,
                    idx_width = idx_width,
                    first_line = first_line,
                )?;
                // Continuation lines indented to Description column
                let indent = prefix_width + idx_width + 2;
                for line in desc_lines {
                    writeln!(f, "{:indent$}{line}", "", indent = indent, line = line)?;
                }
            } else {
                writeln!(
                    f,
                    "  {:<2} {:<name_width$}  {:<cat_width$}  {first_line}",
                    step.index,
                    step.name,
                    cat_str,
                    name_width = name_width,
                    cat_width = cat_width,
                    first_line = first_line,
                )?;
                // Continuation lines indented to Description column
                for line in desc_lines {
                    writeln!(f, "{:prefix_width$}{line}", "", prefix_width = prefix_width, line = line)?;
                }
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
        let exp = TraversalExplanation::from_steps(None, &[], &[], &[]);
        assert_eq!(exp.step_count, 0);
        assert!(!exp.has_barriers);
        assert!(exp.source.is_none());
    }

    #[test]
    fn explanation_display_empty() {
        let exp = TraversalExplanation::from_steps(None, &[], &[], &[]);
        let display = format!("{exp}");
        assert!(display.contains("(anonymous)"));
        assert!(display.contains("(none)"));
    }

    #[test]
    fn explanation_display_with_source() {
        let exp =
            TraversalExplanation::from_steps(Some(&TraversalSource::AllVertices), &[], &[], &[]);
        let display = format!("{exp}");
        assert!(display.contains("V() [all vertices]"));
    }
}
