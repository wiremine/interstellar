//! Property index support for efficient lookups.
//!
//! This module provides property indexes that transform O(n) property scans into
//! O(log n) or O(1) lookups, dramatically improving query performance on large graphs.
//!
//! # Index Types
//!
//! | Type | Use Case | Lookup Complexity |
//! |------|----------|-------------------|
//! | [`BTreeIndex`] | Range queries, ordered iteration | O(log n) + O(k) |
//! | [`UniqueIndex`] | Exact match with uniqueness constraint | O(1) average |
//!
//! # Example
//!
//! ```rust,ignore
//! use interstellar::index::IndexBuilder;
//! use interstellar::storage::InMemoryGraph;
//!
//! let mut graph = InMemoryGraph::new();
//!
//! // Create a B+ tree index for range queries
//! graph.create_index(
//!     IndexBuilder::vertex()
//!         .label("person")
//!         .property("age")
//!         .build()
//!         .unwrap()
//! ).unwrap();
//!
//! // Create a unique index for O(1) lookups with uniqueness constraint
//! graph.create_index(
//!     IndexBuilder::vertex()
//!         .label("user")
//!         .property("email")
//!         .unique()
//!         .build()
//!         .unwrap()
//! ).unwrap();
//!
//! // Queries automatically use indexes when applicable
//! let adults = graph.gremlin().v()
//!     .has_label("person")
//!     .has_where("age", p::gte(18))
//!     .to_list();
//! ```
//!
//! # How It Works
//!
//! 1. **Index Creation**: When you create an index, existing data is scanned and
//!    indexed. The index is then maintained automatically on insert/update/delete.
//!
//! 2. **Automatic Selection**: Filter steps like `has_value()` and `has_where()`
//!    check for applicable indexes and use them when beneficial.
//!
//! 3. **Transparent Integration**: Both the Gremlin-style traversal API and GQL
//!    queries benefit from indexes without any API changes.

mod btree;
mod error;
mod spec;
mod traits;
mod unique;

pub use btree::BTreeIndex;
pub use error::IndexError;
pub use spec::{ElementType, IndexBuilder, IndexPredicate, IndexSpec, IndexType};
pub use traits::{IndexFilter, IndexStatistics, PropertyIndex};
pub use unique::UniqueIndex;
