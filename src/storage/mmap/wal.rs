//! Write-ahead log for durability and crash recovery.
//!
//! The WAL (Write-Ahead Log) provides atomicity and durability for graph mutations.
//! All operations are logged to the WAL before being applied to the main data file,
//! ensuring that committed transactions can be recovered after a crash.
//!
//! # WAL Entry Format
//!
//! Each WAL entry on disk consists of:
//!
//! ```text
//! ┌──────────────┬──────────────┬───────────────────┐
//! │   CRC32      │    Length    │   Entry Data      │
//! │   (4 bytes)  │   (4 bytes)  │   (variable)      │
//! └──────────────┴──────────────┴───────────────────┘
//! ```
//!
//! - **CRC32**: Checksum of the entry data for corruption detection
//! - **Length**: Length of the serialized entry data in bytes
//! - **Entry Data**: Bincode-serialized [`WalEntry`]
//!
//! # Transaction Protocol
//!
//! 1. `BeginTx` - Start a new transaction
//! 2. Zero or more operation entries (InsertNode, InsertEdge, etc.)
//! 3. `CommitTx` - Mark transaction as committed
//!    - OR `AbortTx` - Mark transaction as aborted
//!
//! Only committed transactions are replayed during recovery.
//!
//! # Recovery Process
//!
//! On database open:
//! 1. Scan WAL for all transactions
//! 2. Identify committed transactions (have BeginTx + CommitTx)
//! 3. Replay committed transactions in order
//! 4. Discard aborted/incomplete transactions
//! 5. Truncate WAL after successful recovery

use serde::{Deserialize, Serialize};

use crate::value::{EdgeId, Value, VertexId};

use super::records::{EdgeRecord, NodeRecord};

// =============================================================================
// WAL Header Constants
// =============================================================================

/// Size of the WAL entry header in bytes (CRC32 + Length)
pub const WAL_ENTRY_HEADER_SIZE: usize = 8;

// =============================================================================
// WAL Entry Header
// =============================================================================

/// On-disk header for a WAL entry.
///
/// Each WAL entry is prefixed with this header containing:
/// - A CRC32 checksum of the entry data for integrity verification
/// - The length of the serialized entry data
///
/// # Layout
///
/// ```text
/// Offset | Size | Field
/// -------|------|-------
/// 0      | 4    | crc32
/// 4      | 4    | len
/// ```
#[repr(C, packed)]
#[derive(Copy, Clone, Debug)]
pub struct WalEntryHeader {
    /// CRC32 checksum of the serialized entry data
    pub crc32: u32,
    /// Length of the serialized entry data in bytes
    pub len: u32,
}

impl WalEntryHeader {
    /// Create a new WAL entry header
    pub fn new(crc32: u32, len: u32) -> Self {
        Self { crc32, len }
    }

    /// Read header from bytes
    ///
    /// # Safety
    ///
    /// Uses `read_unaligned` because the struct is `#[repr(C, packed)]`.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() >= WAL_ENTRY_HEADER_SIZE,
            "Buffer too small for WalEntryHeader"
        );

        unsafe {
            let ptr = bytes.as_ptr() as *const WalEntryHeader;
            ptr.read_unaligned()
        }
    }

    /// Write header to bytes
    ///
    /// # Safety
    ///
    /// Creates a byte slice from the packed struct.
    pub fn to_bytes(&self) -> [u8; WAL_ENTRY_HEADER_SIZE] {
        unsafe {
            let ptr = self as *const WalEntryHeader as *const u8;
            let slice = std::slice::from_raw_parts(ptr, WAL_ENTRY_HEADER_SIZE);
            let mut result = [0u8; WAL_ENTRY_HEADER_SIZE];
            result.copy_from_slice(slice);
            result
        }
    }
}

// =============================================================================
// WAL Entry Types
// =============================================================================

/// A write-ahead log entry representing a database operation.
///
/// WAL entries capture all mutations to the database in a format that can be
/// replayed during crash recovery. Each entry is serialized using bincode
/// and written with a CRC32 checksum for integrity.
///
/// # Transaction Entries
///
/// - [`WalEntry::BeginTx`] - Starts a new transaction
/// - [`WalEntry::CommitTx`] - Marks a transaction as committed
/// - [`WalEntry::AbortTx`] - Marks a transaction as aborted (rolled back)
///
/// # Data Modification Entries
///
/// - [`WalEntry::InsertNode`] - Insert a new vertex
/// - [`WalEntry::InsertEdge`] - Insert a new edge
/// - [`WalEntry::UpdateProperty`] - Modify a property value
/// - [`WalEntry::DeleteNode`] - Delete a vertex
/// - [`WalEntry::DeleteEdge`] - Delete an edge
///
/// # Checkpoint Entry
///
/// - [`WalEntry::Checkpoint`] - Marks a safe truncation point
///
/// # Serialization
///
/// All entries are serialized using bincode. The `NodeRecord` and `EdgeRecord`
/// types are converted to serializable representations for WAL storage.
///
/// # Example
///
/// ```ignore
/// use rustgremlin::storage::mmap::wal::{WalEntry, WalEntryHeader};
///
/// // Create a begin transaction entry
/// let entry = WalEntry::BeginTx {
///     tx_id: 1,
///     timestamp: 1704067200,
/// };
///
/// // Serialize with bincode
/// let data = bincode::serialize(&entry).unwrap();
///
/// // Create header
/// let crc = crc32fast::hash(&data);
/// let header = WalEntryHeader::new(crc, data.len() as u32);
/// ```
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum WalEntry {
    /// Begin a new transaction.
    ///
    /// Every transaction starts with this entry. The `tx_id` is a unique
    /// identifier that links all operations in the transaction.
    BeginTx {
        /// Unique transaction identifier
        tx_id: u64,
        /// Unix timestamp when the transaction started (seconds since epoch)
        timestamp: u64,
    },

    /// Insert a new vertex into the graph.
    ///
    /// Contains the vertex ID and a serializable copy of the node record.
    InsertNode {
        /// The ID assigned to the new vertex
        id: VertexId,
        /// The node record data
        record: SerializableNodeRecord,
    },

    /// Insert a new edge into the graph.
    ///
    /// Contains the edge ID and a serializable copy of the edge record.
    InsertEdge {
        /// The ID assigned to the new edge
        id: EdgeId,
        /// The edge record data
        record: SerializableEdgeRecord,
    },

    /// Update a property on a vertex or edge.
    ///
    /// Stores both old and new values to support undo/redo operations.
    UpdateProperty {
        /// Whether this is a vertex (true) or edge (false)
        is_vertex: bool,
        /// The element ID (vertex or edge)
        element_id: u64,
        /// String table ID for the property key
        key_id: u32,
        /// Previous value (for rollback)
        old_value: Value,
        /// New value being set
        new_value: Value,
    },

    /// Delete a vertex from the graph.
    ///
    /// The vertex is marked as deleted; its slot may be reused later.
    DeleteNode {
        /// ID of the vertex to delete
        id: VertexId,
    },

    /// Delete an edge from the graph.
    ///
    /// The edge is marked as deleted; its slot may be reused later.
    DeleteEdge {
        /// ID of the edge to delete
        id: EdgeId,
    },

    /// Commit a transaction.
    ///
    /// Marks all operations in this transaction as permanent. During recovery,
    /// only operations from committed transactions are replayed.
    CommitTx {
        /// Transaction ID to commit
        tx_id: u64,
    },

    /// Abort a transaction.
    ///
    /// Marks all operations in this transaction as rolled back. During recovery,
    /// operations from aborted transactions are discarded.
    AbortTx {
        /// Transaction ID to abort
        tx_id: u64,
    },

    /// Create a checkpoint.
    ///
    /// Indicates that all prior committed transactions have been flushed to
    /// the main data file. The WAL can be safely truncated at this point.
    Checkpoint {
        /// Monotonically increasing version number
        version: u64,
    },
}

// =============================================================================
// Serializable Record Types
// =============================================================================

/// A serializable representation of a [`NodeRecord`].
///
/// The on-disk `NodeRecord` uses `#[repr(C, packed)]` which doesn't play well
/// with serde/bincode. This type provides a serializable equivalent that can
/// be converted to/from `NodeRecord`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SerializableNodeRecord {
    /// Vertex ID (0-based)
    pub id: u64,
    /// String table ID for label
    pub label_id: u32,
    /// Status flags
    pub flags: u32,
    /// First outgoing edge ID (u64::MAX if none)
    pub first_out_edge: u64,
    /// First incoming edge ID (u64::MAX if none)
    pub first_in_edge: u64,
    /// Property list head offset (u64::MAX if none)
    pub prop_head: u64,
}

impl From<NodeRecord> for SerializableNodeRecord {
    fn from(record: NodeRecord) -> Self {
        // Copy fields to avoid unaligned reference issues with packed struct
        Self {
            id: record.id,
            label_id: record.label_id,
            flags: record.flags,
            first_out_edge: record.first_out_edge,
            first_in_edge: record.first_in_edge,
            prop_head: record.prop_head,
        }
    }
}

impl From<SerializableNodeRecord> for NodeRecord {
    fn from(ser: SerializableNodeRecord) -> Self {
        let mut record = NodeRecord::new(ser.id, ser.label_id);
        record.flags = ser.flags;
        record.first_out_edge = ser.first_out_edge;
        record.first_in_edge = ser.first_in_edge;
        record.prop_head = ser.prop_head;
        record
    }
}

/// A serializable representation of an [`EdgeRecord`].
///
/// The on-disk `EdgeRecord` uses `#[repr(C, packed)]` which doesn't play well
/// with serde/bincode. This type provides a serializable equivalent that can
/// be converted to/from `EdgeRecord`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SerializableEdgeRecord {
    /// Edge ID (0-based)
    pub id: u64,
    /// String table ID for label
    pub label_id: u32,
    /// Status flags
    pub flags: u32,
    /// Source vertex ID
    pub src: u64,
    /// Destination vertex ID
    pub dst: u64,
    /// Next outgoing edge from src (u64::MAX if last)
    pub next_out: u64,
    /// Next incoming edge to dst (u64::MAX if last)
    pub next_in: u64,
    /// Property list head offset (u64::MAX if none)
    pub prop_head: u64,
}

impl From<EdgeRecord> for SerializableEdgeRecord {
    fn from(record: EdgeRecord) -> Self {
        // Copy fields to avoid unaligned reference issues with packed struct
        Self {
            id: record.id,
            label_id: record.label_id,
            flags: record.flags,
            src: record.src,
            dst: record.dst,
            next_out: record.next_out,
            next_in: record.next_in,
            prop_head: record.prop_head,
        }
    }
}

impl From<SerializableEdgeRecord> for EdgeRecord {
    fn from(ser: SerializableEdgeRecord) -> Self {
        let mut record = EdgeRecord::new(ser.id, ser.label_id, ser.src, ser.dst);
        record.flags = ser.flags;
        record.next_out = ser.next_out;
        record.next_in = ser.next_in;
        record.prop_head = ser.prop_head;
        record
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // =========================================================================
    // WalEntryHeader Tests
    // =========================================================================

    #[test]
    fn test_wal_entry_header_size() {
        assert_eq!(
            std::mem::size_of::<WalEntryHeader>(),
            WAL_ENTRY_HEADER_SIZE,
            "WalEntryHeader size must be exactly 8 bytes"
        );
    }

    #[test]
    fn test_wal_entry_header_alignment() {
        // crc32: u32 (4 bytes) + len: u32 (4 bytes) = 8 bytes
        assert_eq!(
            std::mem::size_of::<WalEntryHeader>(),
            4 + 4,
            "WalEntryHeader fields should sum to 8 bytes"
        );
    }

    #[test]
    fn test_wal_entry_header_new() {
        let header = WalEntryHeader::new(0x12345678, 256);
        let crc32 = header.crc32;
        let len = header.len;
        assert_eq!(crc32, 0x12345678);
        assert_eq!(len, 256);
    }

    #[test]
    fn test_wal_entry_header_roundtrip() {
        let header = WalEntryHeader::new(0xDEADBEEF, 1024);
        let orig_crc32 = header.crc32;
        let orig_len = header.len;

        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), WAL_ENTRY_HEADER_SIZE);

        let recovered = WalEntryHeader::from_bytes(&bytes);
        let rec_crc32 = recovered.crc32;
        let rec_len = recovered.len;

        assert_eq!(rec_crc32, orig_crc32);
        assert_eq!(rec_len, orig_len);
    }

    #[test]
    fn test_wal_entry_header_byte_order() {
        let header = WalEntryHeader::new(0x01020304, 0x05060708);
        let bytes = header.to_bytes();

        // CRC32 at offset 0 (little-endian)
        let crc_bytes: [u8; 4] = [bytes[0], bytes[1], bytes[2], bytes[3]];
        assert_eq!(crc_bytes[0], 0x04); // LSB first
        assert_eq!(crc_bytes[3], 0x01);

        // len at offset 4 (little-endian)
        let len_bytes: [u8; 4] = [bytes[4], bytes[5], bytes[6], bytes[7]];
        assert_eq!(len_bytes[0], 0x08); // LSB first
        assert_eq!(len_bytes[3], 0x05);
    }

    // =========================================================================
    // WalEntry Serialization Tests
    // =========================================================================

    #[test]
    fn test_begin_tx_serializes() {
        let entry = WalEntry::BeginTx {
            tx_id: 42,
            timestamp: 1704067200,
        };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_commit_tx_serializes() {
        let entry = WalEntry::CommitTx { tx_id: 123 };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_abort_tx_serializes() {
        let entry = WalEntry::AbortTx { tx_id: 456 };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_checkpoint_serializes() {
        let entry = WalEntry::Checkpoint { version: 789 };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_insert_node_serializes() {
        let record = SerializableNodeRecord {
            id: 100,
            label_id: 5,
            flags: 0,
            first_out_edge: u64::MAX,
            first_in_edge: u64::MAX,
            prop_head: 1024,
        };

        let entry = WalEntry::InsertNode {
            id: VertexId(100),
            record,
        };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_insert_edge_serializes() {
        let record = SerializableEdgeRecord {
            id: 200,
            label_id: 10,
            flags: 0,
            src: 1,
            dst: 2,
            next_out: u64::MAX,
            next_in: u64::MAX,
            prop_head: 2048,
        };

        let entry = WalEntry::InsertEdge {
            id: EdgeId(200),
            record,
        };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_update_property_serializes() {
        let entry = WalEntry::UpdateProperty {
            is_vertex: true,
            element_id: 42,
            key_id: 7,
            old_value: Value::Int(10),
            new_value: Value::Int(20),
        };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_update_property_with_complex_values() {
        let mut old_map = HashMap::new();
        old_map.insert("name".to_string(), Value::String("Alice".to_string()));

        let mut new_map = HashMap::new();
        new_map.insert("name".to_string(), Value::String("Bob".to_string()));
        new_map.insert("age".to_string(), Value::Int(30));

        let entry = WalEntry::UpdateProperty {
            is_vertex: false,
            element_id: 99,
            key_id: 15,
            old_value: Value::Map(old_map),
            new_value: Value::Map(new_map),
        };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_delete_node_serializes() {
        let entry = WalEntry::DeleteNode { id: VertexId(555) };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }

    #[test]
    fn test_delete_edge_serializes() {
        let entry = WalEntry::DeleteEdge { id: EdgeId(666) };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }

    // =========================================================================
    // WalEntry Clone and Debug Tests
    // =========================================================================

    #[test]
    fn test_wal_entry_is_clone() {
        let entry = WalEntry::BeginTx {
            tx_id: 1,
            timestamp: 1000,
        };
        let cloned = entry.clone();
        assert_eq!(entry, cloned);
    }

    #[test]
    fn test_wal_entry_is_debug() {
        let entry = WalEntry::BeginTx {
            tx_id: 1,
            timestamp: 1000,
        };
        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("BeginTx"));
        assert!(debug_str.contains("tx_id"));
    }

    // =========================================================================
    // SerializableNodeRecord Tests
    // =========================================================================

    #[test]
    fn test_serializable_node_record_from_node_record() {
        let node = NodeRecord::new(42, 7);
        let ser: SerializableNodeRecord = node.into();

        assert_eq!(ser.id, 42);
        assert_eq!(ser.label_id, 7);
        assert_eq!(ser.flags, 0);
        assert_eq!(ser.first_out_edge, u64::MAX);
        assert_eq!(ser.first_in_edge, u64::MAX);
        assert_eq!(ser.prop_head, u64::MAX);
    }

    #[test]
    fn test_node_record_from_serializable() {
        let ser = SerializableNodeRecord {
            id: 100,
            label_id: 10,
            flags: 1,
            first_out_edge: 200,
            first_in_edge: 300,
            prop_head: 400,
        };

        let node: NodeRecord = ser.into();
        // Copy packed struct fields to local variables before assertions
        let id = node.id;
        let label_id = node.label_id;
        let flags = node.flags;
        let first_out_edge = node.first_out_edge;
        let first_in_edge = node.first_in_edge;
        let prop_head = node.prop_head;

        assert_eq!(id, 100);
        assert_eq!(label_id, 10);
        assert_eq!(flags, 1);
        assert_eq!(first_out_edge, 200);
        assert_eq!(first_in_edge, 300);
        assert_eq!(prop_head, 400);
    }

    #[test]
    fn test_node_record_roundtrip_through_serializable() {
        let mut original = NodeRecord::new(50, 5);
        original.flags = 3;
        original.first_out_edge = 100;
        original.first_in_edge = 200;
        original.prop_head = 300;

        // Copy original values (packed struct fields)
        let orig_id = original.id;
        let orig_label_id = original.label_id;
        let orig_flags = original.flags;
        let orig_first_out = original.first_out_edge;
        let orig_first_in = original.first_in_edge;
        let orig_prop_head = original.prop_head;

        let ser: SerializableNodeRecord = original.into();
        let recovered: NodeRecord = ser.into();

        // Copy recovered values (packed struct fields)
        let rec_id = recovered.id;
        let rec_label_id = recovered.label_id;
        let rec_flags = recovered.flags;
        let rec_first_out = recovered.first_out_edge;
        let rec_first_in = recovered.first_in_edge;
        let rec_prop_head = recovered.prop_head;

        assert_eq!(rec_id, orig_id);
        assert_eq!(rec_label_id, orig_label_id);
        assert_eq!(rec_flags, orig_flags);
        assert_eq!(rec_first_out, orig_first_out);
        assert_eq!(rec_first_in, orig_first_in);
        assert_eq!(rec_prop_head, orig_prop_head);
    }

    // =========================================================================
    // SerializableEdgeRecord Tests
    // =========================================================================

    #[test]
    fn test_serializable_edge_record_from_edge_record() {
        let edge = EdgeRecord::new(42, 7, 10, 20);
        let ser: SerializableEdgeRecord = edge.into();

        assert_eq!(ser.id, 42);
        assert_eq!(ser.label_id, 7);
        assert_eq!(ser.flags, 0);
        assert_eq!(ser.src, 10);
        assert_eq!(ser.dst, 20);
        assert_eq!(ser.next_out, u64::MAX);
        assert_eq!(ser.next_in, u64::MAX);
        assert_eq!(ser.prop_head, u64::MAX);
    }

    #[test]
    fn test_edge_record_from_serializable() {
        let ser = SerializableEdgeRecord {
            id: 100,
            label_id: 10,
            flags: 1,
            src: 5,
            dst: 15,
            next_out: 200,
            next_in: 300,
            prop_head: 400,
        };

        let edge: EdgeRecord = ser.into();
        // Copy packed struct fields to local variables before assertions
        let id = edge.id;
        let label_id = edge.label_id;
        let flags = edge.flags;
        let src = edge.src;
        let dst = edge.dst;
        let next_out = edge.next_out;
        let next_in = edge.next_in;
        let prop_head = edge.prop_head;

        assert_eq!(id, 100);
        assert_eq!(label_id, 10);
        assert_eq!(flags, 1);
        assert_eq!(src, 5);
        assert_eq!(dst, 15);
        assert_eq!(next_out, 200);
        assert_eq!(next_in, 300);
        assert_eq!(prop_head, 400);
    }

    #[test]
    fn test_edge_record_roundtrip_through_serializable() {
        let mut original = EdgeRecord::new(50, 5, 1, 2);
        original.flags = 1;
        original.next_out = 100;
        original.next_in = 200;
        original.prop_head = 300;

        // Copy original values (packed struct fields)
        let orig_id = original.id;
        let orig_label_id = original.label_id;
        let orig_flags = original.flags;
        let orig_src = original.src;
        let orig_dst = original.dst;
        let orig_next_out = original.next_out;
        let orig_next_in = original.next_in;
        let orig_prop_head = original.prop_head;

        let ser: SerializableEdgeRecord = original.into();
        let recovered: EdgeRecord = ser.into();

        // Copy recovered values (packed struct fields)
        let rec_id = recovered.id;
        let rec_label_id = recovered.label_id;
        let rec_flags = recovered.flags;
        let rec_src = recovered.src;
        let rec_dst = recovered.dst;
        let rec_next_out = recovered.next_out;
        let rec_next_in = recovered.next_in;
        let rec_prop_head = recovered.prop_head;

        assert_eq!(rec_id, orig_id);
        assert_eq!(rec_label_id, orig_label_id);
        assert_eq!(rec_flags, orig_flags);
        assert_eq!(rec_src, orig_src);
        assert_eq!(rec_dst, orig_dst);
        assert_eq!(rec_next_out, orig_next_out);
        assert_eq!(rec_next_in, orig_next_in);
        assert_eq!(rec_prop_head, orig_prop_head);
    }

    // =========================================================================
    // All Entry Types Serialize Tests
    // =========================================================================

    #[test]
    fn test_all_entry_types_serialize_with_bincode() {
        let entries = vec![
            WalEntry::BeginTx {
                tx_id: 1,
                timestamp: 1000,
            },
            WalEntry::InsertNode {
                id: VertexId(1),
                record: SerializableNodeRecord {
                    id: 1,
                    label_id: 1,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            },
            WalEntry::InsertEdge {
                id: EdgeId(1),
                record: SerializableEdgeRecord {
                    id: 1,
                    label_id: 1,
                    flags: 0,
                    src: 0,
                    dst: 1,
                    next_out: u64::MAX,
                    next_in: u64::MAX,
                    prop_head: u64::MAX,
                },
            },
            WalEntry::UpdateProperty {
                is_vertex: true,
                element_id: 0,
                key_id: 0,
                old_value: Value::Null,
                new_value: Value::Int(42),
            },
            WalEntry::DeleteNode { id: VertexId(0) },
            WalEntry::DeleteEdge { id: EdgeId(0) },
            WalEntry::CommitTx { tx_id: 1 },
            WalEntry::AbortTx { tx_id: 2 },
            WalEntry::Checkpoint { version: 1 },
        ];

        for entry in entries {
            let serialized = bincode::serialize(&entry).expect(&format!("serialize {:?}", entry));
            let deserialized: WalEntry =
                bincode::deserialize(&serialized).expect(&format!("deserialize {:?}", entry));
            assert_eq!(
                entry, deserialized,
                "Entry {:?} did not roundtrip correctly",
                entry
            );
        }
    }

    // =========================================================================
    // Constant Value Tests
    // =========================================================================

    #[test]
    fn test_wal_entry_header_size_constant() {
        assert_eq!(WAL_ENTRY_HEADER_SIZE, 8);
    }

    // =========================================================================
    // Value Serialization within WAL Tests
    // =========================================================================

    #[test]
    fn test_wal_entry_with_all_value_types() {
        let value_variants = vec![
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(i64::MIN),
            Value::Int(i64::MAX),
            Value::Float(f64::MIN),
            Value::Float(f64::MAX),
            Value::String("test string".to_string()),
            Value::String(String::new()),
            Value::List(vec![Value::Int(1), Value::Bool(true)]),
            Value::List(vec![]),
            Value::Vertex(VertexId(0)),
            Value::Vertex(VertexId(u64::MAX)),
            Value::Edge(EdgeId(0)),
            Value::Edge(EdgeId(u64::MAX)),
        ];

        for old_val in &value_variants {
            for new_val in &value_variants {
                let entry = WalEntry::UpdateProperty {
                    is_vertex: true,
                    element_id: 42,
                    key_id: 7,
                    old_value: old_val.clone(),
                    new_value: new_val.clone(),
                };

                let serialized = bincode::serialize(&entry)
                    .expect(&format!("serialize with {:?} -> {:?}", old_val, new_val));
                let deserialized: WalEntry = bincode::deserialize(&serialized)
                    .expect(&format!("deserialize with {:?} -> {:?}", old_val, new_val));

                assert_eq!(entry, deserialized);
            }
        }
    }

    #[test]
    fn test_wal_entry_with_nested_map_value() {
        let mut inner_map = HashMap::new();
        inner_map.insert("nested_key".to_string(), Value::Int(100));

        let mut outer_map = HashMap::new();
        outer_map.insert("inner".to_string(), Value::Map(inner_map));
        outer_map.insert(
            "list".to_string(),
            Value::List(vec![Value::Int(1), Value::Int(2)]),
        );

        let entry = WalEntry::UpdateProperty {
            is_vertex: false,
            element_id: 1,
            key_id: 2,
            old_value: Value::Null,
            new_value: Value::Map(outer_map),
        };

        let serialized = bincode::serialize(&entry).expect("serialize");
        let deserialized: WalEntry = bincode::deserialize(&serialized).expect("deserialize");

        assert_eq!(entry, deserialized);
    }
}
