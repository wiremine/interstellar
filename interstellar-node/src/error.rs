//! Error conversion for napi-rs bindings.
//!
//! Converts Interstellar error types to napi errors.

use napi::bindgen_prelude::*;

use interstellar::error::{MutationError, StorageError, TraversalError};

/// Convert Interstellar errors to napi errors.
pub trait IntoNapiError {
    fn into_napi_error(self) -> Error;
}

impl IntoNapiError for StorageError {
    fn into_napi_error(self) -> Error {
        match &self {
            StorageError::VertexNotFound(id) => Error::new(
                Status::GenericFailure,
                format!("Vertex not found: {:?}", id),
            ),
            StorageError::EdgeNotFound(id) => {
                Error::new(Status::GenericFailure, format!("Edge not found: {:?}", id))
            }
            StorageError::Io(e) => Error::new(Status::GenericFailure, format!("I/O error: {}", e)),
            StorageError::InvalidFormat => {
                Error::new(Status::GenericFailure, "Invalid data format")
            }
            StorageError::CorruptedData => {
                Error::new(Status::GenericFailure, "Corrupted data detected")
            }
            StorageError::OutOfSpace => Error::new(Status::GenericFailure, "Storage out of space"),
            StorageError::IndexError(msg) => {
                Error::new(Status::GenericFailure, format!("Index error: {}", msg))
            }
            _ => Error::new(Status::GenericFailure, self.to_string()),
        }
    }
}

impl IntoNapiError for TraversalError {
    fn into_napi_error(self) -> Error {
        match self {
            TraversalError::NotOne(count) => Error::new(
                Status::GenericFailure,
                format!("Expected exactly one result, got {}", count),
            ),
            TraversalError::Storage(e) => e.into_napi_error(),
            TraversalError::Mutation(e) => e.into_napi_error(),
        }
    }
}

impl IntoNapiError for MutationError {
    fn into_napi_error(self) -> Error {
        Error::new(Status::GenericFailure, self.to_string())
    }
}

/// Extension trait for Result types.
pub trait ResultExt<T> {
    fn to_napi(self) -> Result<T>;
}

impl<T, E: IntoNapiError> ResultExt<T> for std::result::Result<T, E> {
    fn to_napi(self) -> Result<T> {
        self.map_err(|e| e.into_napi_error())
    }
}
