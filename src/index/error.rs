//! Index-related error types.

use crate::value::Value;
use thiserror::Error;

/// Errors that can occur during index operations.
#[derive(Debug, Error)]
pub enum IndexError {
    /// Index with this name already exists.
    #[error("index already exists: {0}")]
    AlreadyExists(String),

    /// Index not found.
    #[error("index not found: {0}")]
    NotFound(String),

    /// Duplicate value in unique index.
    #[error("unique constraint violation on index '{index_name}': value {value:?} already exists for element {existing_id}, cannot add element {new_id}")]
    DuplicateValue {
        /// Name of the index that rejected the value.
        index_name: String,
        /// The duplicate value.
        value: Value,
        /// The element that already has this value.
        existing_id: u64,
        /// The element that tried to use this value.
        new_id: u64,
    },

    /// Missing required property in IndexBuilder.
    #[error("index builder missing required property")]
    MissingProperty,

    /// Value type cannot be indexed.
    #[error("value type not indexable: {0:?}")]
    NotIndexable(Value),

    /// Internal error (e.g., I/O, serialization).
    #[error("internal index error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_already_exists() {
        let err = IndexError::AlreadyExists("idx_person_age".to_string());
        assert_eq!(err.to_string(), "index already exists: idx_person_age");
    }

    #[test]
    fn error_display_not_found() {
        let err = IndexError::NotFound("idx_missing".to_string());
        assert_eq!(err.to_string(), "index not found: idx_missing");
    }

    #[test]
    fn error_display_duplicate_value() {
        let err = IndexError::DuplicateValue {
            index_name: "uniq_user_email".to_string(),
            value: Value::String("alice@example.com".to_string()),
            existing_id: 1,
            new_id: 2,
        };
        let msg = err.to_string();
        assert!(msg.contains("uniq_user_email"));
        assert!(msg.contains("alice@example.com"));
        assert!(msg.contains("1"));
        assert!(msg.contains("2"));
    }

    #[test]
    fn error_display_missing_property() {
        let err = IndexError::MissingProperty;
        assert_eq!(err.to_string(), "index builder missing required property");
    }

    #[test]
    fn error_display_not_indexable() {
        let err = IndexError::NotIndexable(Value::List(vec![Value::Int(1)]));
        assert!(err.to_string().contains("not indexable"));
    }
}
