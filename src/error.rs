use crate::value::{EdgeId, VertexId};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("vertex not found: {0:?}")]
    VertexNotFound(VertexId),
    #[error("edge not found: {0:?}")]
    EdgeNotFound(EdgeId),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WAL corrupted: {0}")]
    WalCorrupted(String),
    #[error("invalid file format")]
    InvalidFormat,
}

#[derive(Debug, thiserror::Error)]
pub enum TraversalError {
    #[error("expected exactly one result, found {0}")]
    NotOne(usize),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_error_display_variants() {
        let v_err = StorageError::VertexNotFound(VertexId(1));
        let e_err = StorageError::EdgeNotFound(EdgeId(2));
        let wal_err = StorageError::WalCorrupted("oops".to_string());
        let fmt_err = StorageError::InvalidFormat;

        assert!(format!("{}", v_err).contains("vertex not found"));
        assert!(format!("{}", e_err).contains("edge not found"));
        assert!(format!("{}", wal_err).contains("WAL corrupted"));
        assert!(format!("{}", fmt_err).contains("invalid file format"));
    }

    #[test]
    fn traversal_error_wraps_storage() {
        let inner = StorageError::EdgeNotFound(EdgeId(3));
        let err = TraversalError::from(inner);
        assert!(format!("{}", err).contains("storage error"));
    }
}
