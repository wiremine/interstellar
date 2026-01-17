//! Index specification types and builder.
//!
//! This module provides the types needed to define and create property indexes.

use crate::index::error::IndexError;
use crate::value::Value;

/// Element type for indexing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ElementType {
    /// Index applies to vertices.
    Vertex,
    /// Index applies to edges.
    Edge,
}

/// Type of index structure.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
pub enum IndexType {
    /// B+ tree for range queries and ordered iteration.
    /// Supports efficient `<`, `<=`, `>`, `>=`, `BETWEEN` predicates.
    #[default]
    BTree,
    /// Hash-based index with uniqueness constraint.
    /// Provides O(1) exact match lookup and enforces unique values.
    Unique,
}

/// Specification for creating an index.
///
/// This struct defines all the parameters needed to create a property index.
/// Use [`IndexBuilder`] for a fluent API to construct specifications.
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::index::{IndexBuilder, IndexType};
///
/// let spec = IndexBuilder::vertex()
///     .label("person")
///     .property("age")
///     .build()
///     .unwrap();
///
/// assert_eq!(spec.property, "age");
/// assert_eq!(spec.index_type, IndexType::BTree);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexSpec {
    /// Unique name for this index.
    pub name: String,

    /// What element type to index (Vertex or Edge).
    pub element_type: ElementType,

    /// Label filter - only index elements with this label.
    /// None means index all elements regardless of label.
    pub label: Option<String>,

    /// Property key to index.
    pub property: String,

    /// Index type (BTree or Unique).
    pub index_type: IndexType,
}

impl IndexSpec {
    /// Generate an automatic name for this index.
    fn auto_name(
        element_type: ElementType,
        label: Option<&str>,
        property: &str,
        index_type: IndexType,
    ) -> String {
        let prefix = match index_type {
            IndexType::BTree => "idx",
            IndexType::Unique => "uniq",
        };
        let elem = match element_type {
            ElementType::Vertex => "v",
            ElementType::Edge => "e",
        };
        match label {
            Some(l) => format!("{}_{}_{}{}", prefix, l, property, elem),
            None => format!("{}_{}{}", prefix, property, elem),
        }
    }
}

/// Fluent builder for index creation.
///
/// # Example
///
/// ```rust,ignore
/// use interstellar::index::IndexBuilder;
///
/// // B+ tree index on person.age
/// let spec = IndexBuilder::vertex()
///     .label("person")
///     .property("age")
///     .build()?;
///
/// // Unique index on user.email
/// let spec = IndexBuilder::vertex()
///     .label("user")
///     .property("email")
///     .unique()
///     .build()?;
///
/// // Edge index
/// let spec = IndexBuilder::edge()
///     .label("purchased")
///     .property("amount")
///     .build()?;
/// ```
#[derive(Clone, Debug)]
pub struct IndexBuilder {
    element_type: ElementType,
    label: Option<String>,
    property: Option<String>,
    index_type: IndexType,
    name: Option<String>,
}

impl IndexBuilder {
    /// Start building a vertex index.
    pub fn vertex() -> Self {
        Self {
            element_type: ElementType::Vertex,
            label: None,
            property: None,
            index_type: IndexType::BTree,
            name: None,
        }
    }

    /// Start building an edge index.
    pub fn edge() -> Self {
        Self {
            element_type: ElementType::Edge,
            label: None,
            property: None,
            index_type: IndexType::BTree,
            name: None,
        }
    }

    /// Set the label filter (only index elements with this label).
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Set the property to index (required).
    pub fn property(mut self, property: impl Into<String>) -> Self {
        self.property = Some(property.into());
        self
    }

    /// Make this a unique index (default is B+ tree).
    pub fn unique(mut self) -> Self {
        self.index_type = IndexType::Unique;
        self
    }

    /// Set explicit index name (default: auto-generated).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Build the index specification.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::MissingProperty`] if the property was not set.
    pub fn build(self) -> Result<IndexSpec, IndexError> {
        let property = self.property.ok_or(IndexError::MissingProperty)?;

        let name = self.name.unwrap_or_else(|| {
            IndexSpec::auto_name(
                self.element_type,
                self.label.as_deref(),
                &property,
                self.index_type,
            )
        });

        Ok(IndexSpec {
            name,
            element_type: self.element_type,
            label: self.label,
            property,
            index_type: self.index_type,
        })
    }
}

/// Predicates that can use indexes.
///
/// These predicates represent the comparison operations that can be
/// accelerated using property indexes.
#[derive(Clone, Debug, PartialEq)]
pub enum IndexPredicate {
    /// Exact equality: property = value
    Eq(Value),

    /// Inequality: property <> value
    Neq(Value),

    /// Less than: property < value
    Lt(Value),

    /// Less than or equal: property <= value
    Lte(Value),

    /// Greater than: property > value
    Gt(Value),

    /// Greater than or equal: property >= value
    Gte(Value),

    /// Range: start <= property < end (or inclusive)
    Between {
        /// Start of range.
        start: Value,
        /// End of range.
        end: Value,
        /// Whether start is inclusive.
        start_inclusive: bool,
        /// Whether end is inclusive.
        end_inclusive: bool,
    },

    /// Membership: property IN [values]
    Within(Vec<Value>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_builder_vertex_basic() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();

        assert_eq!(spec.element_type, ElementType::Vertex);
        assert_eq!(spec.label, Some("person".to_string()));
        assert_eq!(spec.property, "age");
        assert_eq!(spec.index_type, IndexType::BTree);
        assert!(spec.name.starts_with("idx_"));
    }

    #[test]
    fn index_builder_edge_basic() {
        let spec = IndexBuilder::edge()
            .label("knows")
            .property("since")
            .build()
            .unwrap();

        assert_eq!(spec.element_type, ElementType::Edge);
        assert_eq!(spec.label, Some("knows".to_string()));
        assert_eq!(spec.property, "since");
        assert_eq!(spec.index_type, IndexType::BTree);
    }

    #[test]
    fn index_builder_unique() {
        let spec = IndexBuilder::vertex()
            .label("user")
            .property("email")
            .unique()
            .build()
            .unwrap();

        assert_eq!(spec.index_type, IndexType::Unique);
        assert!(spec.name.starts_with("uniq_"));
    }

    #[test]
    fn index_builder_custom_name() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .name("my_custom_index")
            .build()
            .unwrap();

        assert_eq!(spec.name, "my_custom_index");
    }

    #[test]
    fn index_builder_no_label() {
        let spec = IndexBuilder::vertex()
            .property("created_at")
            .build()
            .unwrap();

        assert_eq!(spec.label, None);
        assert_eq!(spec.property, "created_at");
    }

    #[test]
    fn index_builder_missing_property() {
        let result = IndexBuilder::vertex().label("person").build();

        assert!(matches!(result, Err(IndexError::MissingProperty)));
    }

    #[test]
    fn index_spec_auto_name_btree_with_label() {
        let name =
            IndexSpec::auto_name(ElementType::Vertex, Some("person"), "age", IndexType::BTree);
        assert_eq!(name, "idx_person_agev");
    }

    #[test]
    fn index_spec_auto_name_unique_with_label() {
        let name = IndexSpec::auto_name(
            ElementType::Vertex,
            Some("user"),
            "email",
            IndexType::Unique,
        );
        assert_eq!(name, "uniq_user_emailv");
    }

    #[test]
    fn index_spec_auto_name_without_label() {
        let name = IndexSpec::auto_name(ElementType::Edge, None, "weight", IndexType::BTree);
        assert_eq!(name, "idx_weighte");
    }

    #[test]
    fn element_type_equality() {
        assert_eq!(ElementType::Vertex, ElementType::Vertex);
        assert_eq!(ElementType::Edge, ElementType::Edge);
        assert_ne!(ElementType::Vertex, ElementType::Edge);
    }

    #[test]
    fn index_type_equality() {
        assert_eq!(IndexType::BTree, IndexType::BTree);
        assert_eq!(IndexType::Unique, IndexType::Unique);
        assert_ne!(IndexType::BTree, IndexType::Unique);
    }

    #[test]
    fn index_type_default_is_btree() {
        assert_eq!(IndexType::default(), IndexType::BTree);
    }

    #[test]
    fn index_predicate_eq() {
        let pred = IndexPredicate::Eq(Value::Int(42));
        assert!(matches!(pred, IndexPredicate::Eq(Value::Int(42))));
    }

    #[test]
    fn index_predicate_between() {
        let pred = IndexPredicate::Between {
            start: Value::Int(10),
            end: Value::Int(20),
            start_inclusive: true,
            end_inclusive: false,
        };
        match pred {
            IndexPredicate::Between {
                start,
                end,
                start_inclusive,
                end_inclusive,
            } => {
                assert_eq!(start, Value::Int(10));
                assert_eq!(end, Value::Int(20));
                assert!(start_inclusive);
                assert!(!end_inclusive);
            }
            _ => panic!("Expected Between"),
        }
    }

    #[test]
    fn index_predicate_within() {
        let pred = IndexPredicate::Within(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        match pred {
            IndexPredicate::Within(values) => {
                assert_eq!(values.len(), 3);
            }
            _ => panic!("Expected Within"),
        }
    }

    #[test]
    fn index_spec_clone() {
        let spec = IndexBuilder::vertex()
            .label("person")
            .property("age")
            .build()
            .unwrap();

        let cloned = spec.clone();
        assert_eq!(spec, cloned);
    }

    #[test]
    fn index_builder_clone() {
        let builder = IndexBuilder::vertex().label("person").property("age");

        let cloned = builder.clone();
        let spec1 = builder.build().unwrap();
        let spec2 = cloned.build().unwrap();

        assert_eq!(spec1, spec2);
    }
}
