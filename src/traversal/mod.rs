//! Traversal engine core types.
//!
//! This module provides the core types for the graph traversal engine:
//! - `Traverser`: Carries a `Value` through the pipeline with metadata
//! - `Path`: Tracks traversal history
//! - `PathElement`: A single element in the path
//! - `PathValue`: Values that can be stored in a path
//! - `Traversal`: Type-erased traversal pipeline with fluent API
//!
//! The design uses `Value` internally for type erasure while maintaining
//! compile-time type safety at API boundaries through phantom type parameters.
//!
//! # Error Handling Policy
//!
//! The traversal engine uses a **filter-out-on-error** strategy, which aligns
//! with Gremlin's standard behavior. This means:
//!
//! - **Missing elements are silently filtered out** rather than causing errors
//! - **Invalid operations yield empty results** rather than panicking
//!
//! This design choice enables robust traversals over potentially inconsistent
//! graph data without requiring explicit error handling at each step.
//!
//! ## Step-Specific Behavior
//!
//! | Step Type | Missing Element Behavior |
//! |-----------|--------------------------|
//! | Navigation (`out`, `in`, `both`) | Returns empty iterator (no neighbors found) |
//! | Filter (`has_label`, `has`, `has_where`) | Returns `false` (filters out the element) |
//! | Property extraction (`values`, `properties`) | Returns `Value::Null` or skips the property |
//! | Mutation (`add_v`, `add_e`, `property`) | Silently skips invalid targets |
//!
//! ## Rationale
//!
//! This approach was chosen because:
//!
//! 1. **Consistency with Gremlin**: Standard Gremlin implementations filter
//!    rather than error on missing elements
//! 2. **Composability**: Traversals can chain freely without null checks
//! 3. **Performance**: No exception overhead for common "not found" cases
//! 4. **Safety**: Graph data may be modified concurrently; filtering provides
//!    graceful degradation
//!
//! ## Example
//!
//! ```ignore
//! // This traversal won't error even if some vertices lack the "age" property
//! let ages = g.v()
//!     .has_label("person")
//!     .values("age")  // Missing properties become Null and are filtered
//!     .to_list();
//!
//! // Navigation from a deleted vertex returns empty
//! let neighbors = g.v_by_id(deleted_id)
//!     .out("knows")  // Returns empty if vertex doesn't exist
//!     .to_list();
//! ```
//!
//! ## When You Need Errors
//!
//! If your use case requires explicit error handling for missing elements,
//! consider these alternatives:
//!
//! 1. Check counts: `g.v_by_id(id).count()` returns 0 if missing
//! 2. Use `coalesce`: Provide fallback values for missing elements
//! 3. Pre-validate: Check element existence before traversal

// -----------------------------------------------------------------------------
// Module declarations
// -----------------------------------------------------------------------------

pub mod aggregate;
pub mod anonymous;
pub mod branch;
pub mod context;
pub mod filter;
pub mod markers;
pub mod mutation;
pub mod navigation;
pub mod predicate;
pub mod repeat;
pub mod sideeffect;
pub mod source;
pub mod step;
pub mod transform;
pub mod traverser;
pub mod typed;

// Internal modules (not re-exported directly)
mod builder;
mod pipeline;

// -----------------------------------------------------------------------------
// Re-exports from core modules
// -----------------------------------------------------------------------------

// Re-export static __ instance for Gremlin-style `__.method()` syntax
pub use anonymous::{AnonymousTraversal, __};

// Re-export core types from traverser module
pub use traverser::{CloneSack, Path, PathElement, PathValue, TraversalSource, Traverser};

// Re-export Traversal from pipeline module
pub use pipeline::Traversal;

// Re-export marker types for compile-time type tracking
pub use markers::{
    Edge as EdgeMarker, OutputMarker, Scalar as ScalarMarker, Vertex as VertexMarker,
};

// Re-export typed traversal types
pub use typed::{TypedTraversal, TypedTraversalSource};

// Re-export aggregate types
pub use aggregate::{
    BoundGroupBuilder, BoundGroupCountBuilder, GroupBuilder, GroupCountBuilder, GroupCountStep,
    GroupKey, GroupStep, GroupValue,
};

// Re-export branch types
pub use branch::{
    AndStep, BranchStep, ChooseStep, CoalesceStep, LocalStep, NotStep, OptionKey, OptionalStep,
    OrStep, UnionStep, WhereStep,
};

// Re-export context types
pub use context::{ExecutionContext, SideEffects, SnapshotLike};

// Re-export filter types
pub use filter::{
    CoinStep, CyclicPathStep, DedupByKeyStep, DedupByLabelStep, DedupByTraversalStep, DedupStep,
    FilterStep, HasIdStep, HasKeyStep, HasLabelStep, HasNotStep, HasPropValueStep, HasStep,
    HasValueStep, HasWhereStep, IsStep, LimitStep, RangeStep, SampleStep, SimplePathStep, SkipStep,
    TailStep, WherePStep,
};

// Re-export mutation types
pub use mutation::{
    AddEStep, AddVStep, DropStep, EdgeEndpoint, MutationExecutor, MutationResult, PendingMutation,
    PropertyStep,
};

// Re-export navigation types
pub use navigation::{
    BothEStep, BothStep, BothVStep, InEStep, InStep, InVStep, OtherVStep, OutEStep, OutStep,
    OutVStep,
};

// Re-export repeat types
pub use repeat::{RepeatConfig, RepeatStep, RepeatTraversal};

// Re-export side effect types
pub use sideeffect::{AggregateStep, CapStep, ProfileStep, SideEffectStep, StoreStep};

// Re-export source types
pub use source::{BoundTraversal, BranchBuilder, GraphTraversalSource, TraversalExecutor};

// Re-export step types
pub use step::{execute_traversal, execute_traversal_from, AnyStep, IdentityStep, StartStep};

// Re-export transform types
pub use transform::{
    AsStep, BoundProjectBuilder, ConstantStep, ElementMapStep, FlatMapStep, IdStep, IndexStep,
    KeyStep, LabelStep, LoopsStep, MapStep, MeanStep, Order, OrderBuilder, OrderKey, OrderStep,
    PathStep, ProjectBuilder, ProjectStep, Projection, PropertiesStep, PropertyMapStep, SelectStep,
    UnfoldStep, ValueMapStep, ValueStep, ValuesStep,
};

// Re-export predicate types
pub use predicate::p;
pub use predicate::Predicate;

// Re-export macros
pub use crate::{impl_filter_step, impl_flatmap_step};
