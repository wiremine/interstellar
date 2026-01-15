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

// -----------------------------------------------------------------------------
// Module declarations
// -----------------------------------------------------------------------------

pub mod aggregate;
pub mod anonymous;
pub mod branch;
pub mod context;
pub mod filter;
pub mod mutation;
pub mod navigation;
pub mod predicate;
pub mod repeat;
pub mod sideeffect;
pub mod source;
pub mod step;
pub mod transform;
pub mod traverser;

// Internal modules (not re-exported directly)
mod builder;
mod pipeline;

// -----------------------------------------------------------------------------
// Re-exports from core modules
// -----------------------------------------------------------------------------

// Re-export anonymous module as __ for Gremlin-style API
pub use anonymous as __;

// Re-export core types from traverser module
pub use traverser::{CloneSack, Path, PathElement, PathValue, TraversalSource, Traverser};

// Re-export Traversal from pipeline module
pub use pipeline::Traversal;

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
pub use context::{ExecutionContext, SideEffects};

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
