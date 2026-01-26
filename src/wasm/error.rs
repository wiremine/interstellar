//! Error conversion for WASM bindings.
//!
//! Converts Rust errors to JavaScript-friendly `JsError` types.

#![allow(dead_code)]

use wasm_bindgen::JsError;

use crate::error::{MutationError, StorageError, TraversalError};

/// Helper function to convert StorageError to JsError.
pub fn storage_error_to_js(err: StorageError) -> JsError {
    JsError::new(&err.to_string())
}

/// Helper function to convert TraversalError to JsError.
pub fn traversal_error_to_js(err: TraversalError) -> JsError {
    JsError::new(&err.to_string())
}

/// Helper function to convert MutationError to JsError.
pub fn mutation_error_to_js(err: MutationError) -> JsError {
    JsError::new(&err.to_string())
}

/// Helper trait for converting Results to JsError Results.
pub trait IntoJsResult<T> {
    fn into_js(self) -> Result<T, JsError>;
}

impl<T> IntoJsResult<T> for Result<T, StorageError> {
    fn into_js(self) -> Result<T, JsError> {
        self.map_err(storage_error_to_js)
    }
}

impl<T> IntoJsResult<T> for Result<T, TraversalError> {
    fn into_js(self) -> Result<T, JsError> {
        self.map_err(traversal_error_to_js)
    }
}

impl<T> IntoJsResult<T> for Result<T, MutationError> {
    fn into_js(self) -> Result<T, JsError> {
        self.map_err(mutation_error_to_js)
    }
}
