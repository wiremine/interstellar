//! Integration tests for the traversal engine.
//!
//! These tests verify the complete traversal pipeline including:
//! - Basic traversal sources (v, e, inject)
//! - Filter steps (has_label, has, has_value, dedup, limit, skip, range, has_id)
//! - Navigation steps (out, in_, both, out_e, in_e, both_e, out_v, in_v, both_v)
//! - Transform steps (values, id, label, map, flat_map, constant, path, as_, select)
//! - Terminal steps (to_list, to_set, next, one, count, sum, min, max, fold)
//! - Anonymous traversals (__ module)
//! - Complex multi-step traversals

mod common;

// Traversal test modules - each module is in tests/traversal/<name>.rs
#[path = "traversal/basic.rs"]
mod basic;

#[path = "traversal/filter.rs"]
mod filter;

#[path = "traversal/navigation.rs"]
mod navigation;

#[path = "traversal/transform.rs"]
mod transform;

#[path = "traversal/terminal.rs"]
mod terminal;

#[path = "traversal/branch.rs"]
mod branch;

#[path = "traversal/repeat.rs"]
mod repeat;

#[path = "traversal/predicates.rs"]
mod predicates;

#[path = "traversal/anonymous.rs"]
mod anonymous;

#[path = "traversal/phase7.rs"]
mod phase7;

#[path = "traversal/metadata.rs"]
mod metadata;

#[path = "traversal/complex.rs"]
mod complex;

#[path = "traversal/errors.rs"]
mod errors;

#[path = "traversal/math.rs"]
mod math;

#[path = "traversal/builder_coverage.rs"]
mod builder_coverage;

#[path = "traversal/anonymous_coverage.rs"]
mod anonymous_coverage;
