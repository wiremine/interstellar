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
/// use intersteller::storage::mmap::wal::{WalEntry, WalEntryHeader};
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

    /// Update the schema in the database.
    ///
    /// Contains the serialized schema data and the offset where it was written.
    /// This allows schema changes to be replayed during crash recovery.
    SchemaUpdate {
        /// Byte offset where schema data was written
        offset: u64,
        /// Serialized schema data
        data: Vec<u8>,
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

use crate::error::StorageError;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// =============================================================================
// WriteAheadLog Implementation
// =============================================================================

/// Write-ahead log for transaction durability.
///
/// The `WriteAheadLog` provides atomicity and durability for graph mutations.
/// All operations are logged to the WAL before being applied to the main data file,
/// ensuring that committed transactions can be recovered after a crash.
///
/// # Usage
///
/// ```ignore
/// use intersteller::storage::mmap::wal::{WriteAheadLog, WalEntry};
///
/// let mut wal = WriteAheadLog::open("my_graph.wal")?;
///
/// // Begin a transaction
/// let tx_id = wal.begin_transaction()?;
///
/// // Log operations
/// wal.log(WalEntry::InsertNode { id: VertexId(0), record: node_record.into() })?;
///
/// // Commit the transaction
/// wal.log(WalEntry::CommitTx { tx_id })?;
/// wal.sync()?;
/// ```
///
/// # Thread Safety
///
/// `WriteAheadLog` is NOT thread-safe. It should be protected by an external
/// lock (like `RwLock<WriteAheadLog>`) when used in concurrent contexts.
///
/// # File Format
///
/// Each WAL entry on disk consists of:
/// - 4 bytes: CRC32 checksum of the entry data
/// - 4 bytes: Length of the serialized entry data  
/// - N bytes: Bincode-serialized WalEntry
pub struct WriteAheadLog {
    /// File handle for WAL writes
    file: File,

    /// Next transaction ID to assign
    next_tx_id: AtomicU64,

    /// Reusable buffer for serialization to avoid repeated allocations
    buffer: Vec<u8>,
}

impl WriteAheadLog {
    /// Open or create a WAL file at the given path.
    ///
    /// If the file doesn't exist, it will be created. If it exists, it will be
    /// opened for appending. The file is opened with read, write, and create
    /// permissions.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the WAL file (typically `<database>.wal`)
    ///
    /// # Returns
    ///
    /// A new `WriteAheadLog` instance ready for writing.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the file cannot be opened or created.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let wal = WriteAheadLog::open("my_graph.wal")?;
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        Ok(Self {
            file,
            next_tx_id: AtomicU64::new(0),
            buffer: Vec::with_capacity(4096),
        })
    }

    /// Begin a new transaction.
    ///
    /// This logs a `BeginTx` entry to the WAL and returns a unique transaction ID.
    /// All subsequent operations should use this transaction ID until either
    /// `CommitTx` or `AbortTx` is logged.
    ///
    /// # Returns
    ///
    /// The unique transaction ID assigned to this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if writing to the WAL fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let tx_id = wal.begin_transaction()?;
    /// // ... perform operations ...
    /// wal.log(WalEntry::CommitTx { tx_id })?;
    /// ```
    pub fn begin_transaction(&mut self) -> Result<u64, StorageError> {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);

        self.log(WalEntry::BeginTx {
            tx_id,
            timestamp: Self::now(),
        })?;

        Ok(tx_id)
    }

    /// Log an entry to the WAL.
    ///
    /// This serializes the entry using bincode, computes a CRC32 checksum,
    /// and writes the entry to the WAL file. The entry is appended to the
    /// end of the file.
    ///
    /// # Format
    ///
    /// Each entry is written as:
    /// ```text
    /// ┌──────────────┬──────────────┬───────────────────┐
    /// │   CRC32      │    Length    │   Entry Data      │
    /// │   (4 bytes)  │   (4 bytes)  │   (variable)      │
    /// └──────────────┴──────────────┴───────────────────┘
    /// ```
    ///
    /// # Arguments
    ///
    /// * `entry` - The WAL entry to log
    ///
    /// # Returns
    ///
    /// The byte offset where the entry was written (useful for debugging).
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] if writing to the file fails
    /// - [`StorageError::WalCorrupted`] if serialization fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// let offset = wal.log(WalEntry::InsertNode {
    ///     id: VertexId(0),
    ///     record: node_record.into(),
    /// })?;
    /// ```
    pub fn log(&mut self, entry: WalEntry) -> Result<u64, StorageError> {
        // Clear and serialize to buffer
        self.buffer.clear();
        bincode::serialize_into(&mut self.buffer, &entry)
            .map_err(|e| StorageError::WalCorrupted(format!("serialization error: {}", e)))?;

        // Calculate CRC32
        let crc = crc32fast::hash(&self.buffer);

        // Create header
        let header = WalEntryHeader::new(crc, self.buffer.len() as u32);
        let header_bytes = header.to_bytes();

        // Seek to end and get current position
        let offset = self.file.seek(SeekFrom::End(0))?;

        // Write header
        self.file.write_all(&header_bytes)?;

        // Write entry data
        self.file.write_all(&self.buffer)?;

        Ok(offset)
    }

    /// Sync the WAL to disk (fsync).
    ///
    /// This ensures all logged entries are durably written to disk. For
    /// transaction durability, `sync()` should be called after logging
    /// the `CommitTx` entry.
    ///
    /// # Performance Note
    ///
    /// `fsync` is relatively expensive (1-5ms on typical SSDs). For better
    /// performance with many small transactions, consider batching multiple
    /// transactions before calling `sync()`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the sync operation fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// wal.log(WalEntry::CommitTx { tx_id })?;
    /// wal.sync()?;  // Ensure transaction is durable
    /// ```
    pub fn sync(&mut self) -> Result<(), StorageError> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Get the current Unix timestamp in seconds.
    ///
    /// This is used for transaction timestamps in `BeginTx` entries.
    ///
    /// # Returns
    ///
    /// Seconds since Unix epoch (January 1, 1970).
    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Get the current file position (for testing).
    ///
    /// Returns the current write position in the WAL file.
    #[cfg(test)]
    fn position(&mut self) -> Result<u64, StorageError> {
        Ok(self.file.seek(SeekFrom::Current(0))?)
    }

    /// Get the current transaction ID counter (for testing).
    #[cfg(test)]
    fn current_tx_id(&self) -> u64 {
        self.next_tx_id.load(Ordering::SeqCst)
    }

    // =========================================================================
    // Reading Methods (Phase 3.4)
    // =========================================================================

    /// Read the next WAL entry from the current file position.
    ///
    /// This reads a single entry from the WAL file, verifying its CRC32 checksum.
    /// The file position is advanced past the entry after reading.
    ///
    /// # Entry Format
    ///
    /// ```text
    /// ┌──────────────┬──────────────┬───────────────────┐
    /// │   CRC32      │    Length    │   Entry Data      │
    /// │   (4 bytes)  │   (4 bytes)  │   (variable)      │
    /// └──────────────┴──────────────┴───────────────────┘
    /// ```
    ///
    /// # Returns
    ///
    /// The deserialized `WalEntry`.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] if reading from the file fails (including EOF)
    /// - [`StorageError::WalCorrupted`] if the CRC32 checksum doesn't match
    /// - [`StorageError::WalCorrupted`] if deserialization fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Seek to start of WAL
    /// wal.seek_to_start()?;
    ///
    /// // Read entries until EOF
    /// loop {
    ///     match wal.read_entry() {
    ///         Ok(entry) => println!("Read: {:?}", entry),
    ///         Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
    ///         Err(e) => return Err(e),
    ///     }
    /// }
    /// ```
    pub fn read_entry(&mut self) -> Result<WalEntry, StorageError> {
        // Read header bytes
        let mut header_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
        self.file.read_exact(&mut header_bytes)?;

        let header = WalEntryHeader::from_bytes(&header_bytes);
        let crc32 = header.crc32;
        let len = header.len;

        // Validate length to prevent excessive allocation
        if len > 100_000_000 {
            // 100MB limit
            return Err(StorageError::WalCorrupted(format!(
                "entry length {} exceeds maximum",
                len
            )));
        }

        // Read entry data
        let mut entry_data = vec![0u8; len as usize];
        self.file.read_exact(&mut entry_data)?;

        // Verify CRC32
        let computed_crc = crc32fast::hash(&entry_data);
        if computed_crc != crc32 {
            return Err(StorageError::WalCorrupted(format!(
                "CRC32 mismatch: expected {:08x}, got {:08x}",
                crc32, computed_crc
            )));
        }

        // Deserialize entry
        let entry: WalEntry = bincode::deserialize(&entry_data)
            .map_err(|e| StorageError::WalCorrupted(format!("deserialization error: {}", e)))?;

        Ok(entry)
    }

    /// Check if the WAL needs recovery.
    ///
    /// Recovery is needed when there are uncommitted transactions in the WAL.
    /// This scans the entire WAL file looking for `BeginTx` entries without
    /// matching `CommitTx` or `AbortTx` entries.
    ///
    /// # Algorithm
    ///
    /// 1. Scan all WAL entries from the beginning
    /// 2. Track transaction IDs that have started but not completed
    /// 3. Return `true` if any transactions remain open
    ///
    /// # Returns
    ///
    /// - `true` if there are uncommitted transactions requiring recovery
    /// - `false` if the WAL is empty or all transactions are complete
    ///
    /// # Note
    ///
    /// This method seeks to the beginning of the file and reads all entries,
    /// then seeks back to the end. It does not modify the file.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if wal.needs_recovery() {
    ///     println!("WAL recovery required");
    ///     // Perform recovery...
    /// }
    /// ```
    pub fn needs_recovery(&mut self) -> bool {
        // Remember current position
        let original_pos = match self.file.stream_position() {
            Ok(pos) => pos,
            Err(_) => return false,
        };

        // Seek to start
        if self.file.seek(SeekFrom::Start(0)).is_err() {
            return false;
        }

        // Track active transactions
        let mut active_transactions: HashSet<u64> = HashSet::new();

        // Read all entries
        loop {
            match self.read_entry() {
                Ok(entry) => match entry {
                    WalEntry::BeginTx { tx_id, .. } => {
                        active_transactions.insert(tx_id);
                    }
                    WalEntry::CommitTx { tx_id } | WalEntry::AbortTx { tx_id } => {
                        active_transactions.remove(&tx_id);
                    }
                    WalEntry::Checkpoint { .. } => {
                        // Checkpoint means all prior transactions are complete
                        active_transactions.clear();
                    }
                    _ => {
                        // Other entries don't affect transaction state
                    }
                },
                Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // End of file reached
                    break;
                }
                Err(_) => {
                    // Error reading entry - might indicate incomplete write (needs recovery)
                    // Restore original position
                    let _ = self.file.seek(SeekFrom::Start(original_pos));
                    return true;
                }
            }
        }

        // Restore original position
        let _ = self.file.seek(SeekFrom::Start(original_pos));

        // Recovery needed if there are uncommitted transactions
        !active_transactions.is_empty()
    }

    /// Truncate the WAL file, removing all entries.
    ///
    /// This is called after a successful checkpoint or recovery to clear the WAL.
    /// After truncation, the WAL file will be empty and the file position will
    /// be at the beginning.
    ///
    /// # Safety
    ///
    /// This is a destructive operation. Only call this after ensuring all
    /// committed transactions have been applied to the main data file.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the truncation or seek fails.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // After successful recovery or checkpoint
    /// wal.truncate()?;
    /// assert_eq!(wal.file_size()?, 0);
    /// ```
    pub fn truncate(&mut self) -> Result<(), StorageError> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    /// Seek to the start of the WAL file.
    ///
    /// This positions the file cursor at the beginning, ready to read entries
    /// from the start.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the seek fails.
    pub fn seek_to_start(&mut self) -> Result<(), StorageError> {
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    /// Get the current size of the WAL file in bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if getting the file metadata fails.
    pub fn file_size(&self) -> Result<u64, StorageError> {
        let metadata = self.file.metadata()?;
        Ok(metadata.len())
    }

    /// Read all entries from the WAL file.
    ///
    /// This seeks to the beginning of the file and reads all entries,
    /// returning them in order. The file position is left at the end
    /// after reading.
    ///
    /// # Returns
    ///
    /// A vector of all WAL entries in the file.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] if reading fails
    /// - [`StorageError::WalCorrupted`] if any entry is corrupted
    ///
    /// # Example
    ///
    /// ```ignore
    /// let entries = wal.read_all_entries()?;
    /// for entry in entries {
    ///     println!("{:?}", entry);
    /// }
    /// ```
    pub fn read_all_entries(&mut self) -> Result<Vec<WalEntry>, StorageError> {
        self.seek_to_start()?;

        let mut entries = Vec::new();

        loop {
            match self.read_entry() {
                Ok(entry) => entries.push(entry),
                Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(entries)
    }

    /// Get committed transaction entries from the WAL.
    ///
    /// This reads all entries and returns only those from committed transactions,
    /// in the order they were logged. Entries from aborted or incomplete
    /// transactions are excluded.
    ///
    /// # Returns
    ///
    /// A vector of WAL entries from committed transactions only.
    /// BeginTx and CommitTx entries are excluded from the result.
    ///
    /// # Errors
    ///
    /// - [`StorageError::Io`] if reading fails
    /// - [`StorageError::WalCorrupted`] if any entry is corrupted
    ///
    /// # Example
    ///
    /// ```ignore
    /// let committed = wal.get_committed_entries()?;
    /// for entry in committed {
    ///     // Replay this entry
    /// }
    /// ```
    pub fn get_committed_entries(&mut self) -> Result<Vec<WalEntry>, StorageError> {
        use std::collections::HashMap;

        self.seek_to_start()?;

        // Track entries for each transaction
        let mut tx_entries: HashMap<u64, Vec<WalEntry>> = HashMap::new();
        let mut committed_tx_ids: Vec<u64> = Vec::new();
        let mut current_tx_id: Option<u64> = None;

        loop {
            match self.read_entry() {
                Ok(entry) => match &entry {
                    WalEntry::BeginTx { tx_id, .. } => {
                        current_tx_id = Some(*tx_id);
                        tx_entries.insert(*tx_id, Vec::new());
                    }
                    WalEntry::CommitTx { tx_id } => {
                        committed_tx_ids.push(*tx_id);
                        current_tx_id = None;
                    }
                    WalEntry::AbortTx { tx_id } => {
                        tx_entries.remove(tx_id);
                        current_tx_id = None;
                    }
                    WalEntry::Checkpoint { .. } => {
                        // Checkpoint doesn't contain data to replay
                    }
                    _ => {
                        // Add operation to current transaction
                        if let Some(tx_id) = current_tx_id {
                            if let Some(entries) = tx_entries.get_mut(&tx_id) {
                                entries.push(entry);
                            }
                        }
                    }
                },
                Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        // Collect entries from committed transactions in order
        let mut result = Vec::new();
        for tx_id in committed_tx_ids {
            if let Some(entries) = tx_entries.remove(&tx_id) {
                result.extend(entries);
            }
        }

        Ok(result)
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

    // =========================================================================
    // WriteAheadLog Tests
    // =========================================================================

    #[test]
    fn test_wal_open_creates_new_file() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");

        assert!(!wal_path.exists(), "WAL file should not exist initially");

        let wal = WriteAheadLog::open(&wal_path).expect("open WAL");
        drop(wal);

        assert!(wal_path.exists(), "WAL file should be created");
    }

    #[test]
    fn test_wal_open_existing_file() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");

        // Create and write to WAL
        {
            let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");
            let _tx_id = wal.begin_transaction().expect("begin tx");
        }

        // Re-open and verify we can continue using it
        let mut wal = WriteAheadLog::open(&wal_path).expect("reopen WAL");
        let tx_id = wal.begin_transaction().expect("begin another tx");
        // Note: tx_id counter resets on reopen (would need recovery to restore)
        assert_eq!(tx_id, 0, "tx_id starts at 0 on fresh open");
    }

    #[test]
    fn test_wal_begin_transaction_returns_unique_ids() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        let tx1 = wal.begin_transaction().expect("begin tx 1");
        let tx2 = wal.begin_transaction().expect("begin tx 2");
        let tx3 = wal.begin_transaction().expect("begin tx 3");

        assert_eq!(tx1, 0);
        assert_eq!(tx2, 1);
        assert_eq!(tx3, 2);
    }

    #[test]
    fn test_wal_begin_transaction_increments_counter() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        assert_eq!(wal.current_tx_id(), 0);

        let _ = wal.begin_transaction().expect("begin tx");
        assert_eq!(wal.current_tx_id(), 1);

        let _ = wal.begin_transaction().expect("begin tx");
        assert_eq!(wal.current_tx_id(), 2);
    }

    #[test]
    fn test_wal_log_entry_increases_file_size() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        let pos_before = wal.position().expect("get position");
        assert_eq!(pos_before, 0, "should start at position 0");

        let _ = wal.begin_transaction().expect("begin tx");

        let pos_after = wal.position().expect("get position");
        assert!(
            pos_after > pos_before,
            "position should increase after logging"
        );
    }

    #[test]
    fn test_wal_log_returns_offset() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        let offset1 = wal
            .log(WalEntry::BeginTx {
                tx_id: 0,
                timestamp: 1000,
            })
            .expect("log entry");
        assert_eq!(offset1, 0, "first entry should be at offset 0");

        let offset2 = wal.log(WalEntry::CommitTx { tx_id: 0 }).expect("log entry");
        assert!(offset2 > offset1, "second entry should be at higher offset");
    }

    #[test]
    fn test_wal_log_multiple_entries() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Log a complete transaction
        let tx_id = wal.begin_transaction().expect("begin tx");

        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log insert node");

        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log insert node");

        wal.log(WalEntry::InsertEdge {
            id: EdgeId(0),
            record: SerializableEdgeRecord {
                id: 0,
                label_id: 2,
                flags: 0,
                src: 0,
                dst: 1,
                next_out: u64::MAX,
                next_in: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log insert edge");

        wal.log(WalEntry::CommitTx { tx_id }).expect("log commit");

        // Verify file has content
        let pos = wal.position().expect("get position");
        assert!(pos > 0, "WAL should have content");
    }

    #[test]
    fn test_wal_sync_succeeds() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        let tx_id = wal.begin_transaction().expect("begin tx");
        wal.log(WalEntry::CommitTx { tx_id }).expect("log commit");
        wal.sync().expect("sync should succeed");
    }

    #[test]
    fn test_wal_entries_are_append_only() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Log several entries and track positions
        let offsets: Vec<u64> = (0..5)
            .map(|i| {
                wal.log(WalEntry::BeginTx {
                    tx_id: i,
                    timestamp: 1000 + i,
                })
                .expect("log entry")
            })
            .collect();

        // Verify offsets are strictly increasing
        for i in 1..offsets.len() {
            assert!(
                offsets[i] > offsets[i - 1],
                "offsets should be strictly increasing"
            );
        }
    }

    #[test]
    fn test_wal_crc32_is_written_correctly() {
        use std::io::Read;

        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");

        // Write an entry
        {
            let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");
            wal.log(WalEntry::BeginTx {
                tx_id: 42,
                timestamp: 1704067200,
            })
            .expect("log entry");
            wal.sync().expect("sync");
        }

        // Read the file and verify CRC
        let mut file = File::open(&wal_path).expect("open file");
        let mut header_bytes = [0u8; WAL_ENTRY_HEADER_SIZE];
        file.read_exact(&mut header_bytes).expect("read header");

        let header = WalEntryHeader::from_bytes(&header_bytes);
        let crc = header.crc32;
        let len = header.len;

        // Read entry data
        let mut entry_data = vec![0u8; len as usize];
        file.read_exact(&mut entry_data).expect("read entry data");

        // Verify CRC
        let computed_crc = crc32fast::hash(&entry_data);
        assert_eq!(crc, computed_crc, "CRC32 should match");

        // Verify entry deserializes correctly
        let entry: WalEntry = bincode::deserialize(&entry_data).expect("deserialize");
        match entry {
            WalEntry::BeginTx { tx_id, timestamp } => {
                assert_eq!(tx_id, 42);
                assert_eq!(timestamp, 1704067200);
            }
            _ => panic!("Expected BeginTx entry"),
        }
    }

    #[test]
    fn test_wal_log_all_entry_types() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Log all entry types
        let entries = vec![
            WalEntry::BeginTx {
                tx_id: 0,
                timestamp: 1000,
            },
            WalEntry::InsertNode {
                id: VertexId(0),
                record: SerializableNodeRecord {
                    id: 0,
                    label_id: 1,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            },
            WalEntry::InsertEdge {
                id: EdgeId(0),
                record: SerializableEdgeRecord {
                    id: 0,
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
                key_id: 1,
                old_value: Value::Null,
                new_value: Value::Int(42),
            },
            WalEntry::DeleteNode { id: VertexId(0) },
            WalEntry::DeleteEdge { id: EdgeId(0) },
            WalEntry::CommitTx { tx_id: 0 },
            WalEntry::AbortTx { tx_id: 1 },
            WalEntry::Checkpoint { version: 1 },
        ];

        for entry in entries {
            wal.log(entry).expect("log entry");
        }

        // Verify all were written
        let pos = wal.position().expect("get position");
        assert!(
            pos > 0,
            "WAL should have content after logging all entry types"
        );
    }

    #[test]
    fn test_wal_log_large_property_value() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Create a large string value
        let large_string = "x".repeat(100_000);

        wal.log(WalEntry::UpdateProperty {
            is_vertex: true,
            element_id: 0,
            key_id: 1,
            old_value: Value::Null,
            new_value: Value::String(large_string),
        })
        .expect("log large property");

        wal.sync().expect("sync");

        let pos = wal.position().expect("get position");
        assert!(pos > 100_000, "WAL should contain the large value");
    }

    #[test]
    fn test_wal_multiple_transactions() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Transaction 1: committed
        let tx1 = wal.begin_transaction().expect("begin tx1");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert node");
        wal.log(WalEntry::CommitTx { tx_id: tx1 })
            .expect("commit tx1");

        // Transaction 2: aborted
        let tx2 = wal.begin_transaction().expect("begin tx2");
        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert node");
        wal.log(WalEntry::AbortTx { tx_id: tx2 })
            .expect("abort tx2");

        // Transaction 3: committed
        let tx3 = wal.begin_transaction().expect("begin tx3");
        wal.log(WalEntry::InsertNode {
            id: VertexId(2),
            record: SerializableNodeRecord {
                id: 2,
                label_id: 2,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert node");
        wal.log(WalEntry::CommitTx { tx_id: tx3 })
            .expect("commit tx3");

        wal.sync().expect("sync");

        // Verify file has expected transaction IDs
        assert_eq!(tx1, 0);
        assert_eq!(tx2, 1);
        assert_eq!(tx3, 2);
    }

    #[test]
    fn test_wal_now_returns_reasonable_timestamp() {
        // This test verifies that `now()` returns a reasonable Unix timestamp
        // We can't test the exact value, but we can verify it's in a reasonable range
        let timestamp = WriteAheadLog::now();

        // Should be after 2024-01-01 (1704067200)
        assert!(
            timestamp > 1704067200,
            "timestamp should be after 2024-01-01"
        );

        // Should be before 2100-01-01 (4102444800) - gives us plenty of runway
        assert!(
            timestamp < 4102444800,
            "timestamp should be before 2100-01-01"
        );
    }

    // =========================================================================
    // Phase 3.4: Reading and Recovery Tests
    // =========================================================================

    #[test]
    fn test_read_entry_single() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write a single entry
        wal.log(WalEntry::BeginTx {
            tx_id: 42,
            timestamp: 1704067200,
        })
        .expect("log entry");
        wal.sync().expect("sync");

        // Read it back
        wal.seek_to_start().expect("seek");
        let entry = wal.read_entry().expect("read entry");

        match entry {
            WalEntry::BeginTx { tx_id, timestamp } => {
                assert_eq!(tx_id, 42);
                assert_eq!(timestamp, 1704067200);
            }
            _ => panic!("Expected BeginTx entry"),
        }
    }

    #[test]
    fn test_read_entry_multiple() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write multiple entries
        let entries_to_write = vec![
            WalEntry::BeginTx {
                tx_id: 0,
                timestamp: 1000,
            },
            WalEntry::InsertNode {
                id: VertexId(0),
                record: SerializableNodeRecord {
                    id: 0,
                    label_id: 1,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            },
            WalEntry::CommitTx { tx_id: 0 },
        ];

        for entry in &entries_to_write {
            wal.log(entry.clone()).expect("log entry");
        }
        wal.sync().expect("sync");

        // Read all entries back
        wal.seek_to_start().expect("seek");
        let mut read_entries = Vec::new();
        loop {
            match wal.read_entry() {
                Ok(entry) => read_entries.push(entry),
                Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        assert_eq!(read_entries.len(), 3);
        assert_eq!(read_entries[0], entries_to_write[0]);
        assert_eq!(read_entries[1], entries_to_write[1]);
        assert_eq!(read_entries[2], entries_to_write[2]);
    }

    #[test]
    fn test_read_entry_crc_mismatch_detected() {
        use std::io::Write;

        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");

        // Write a valid entry
        {
            let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");
            wal.log(WalEntry::BeginTx {
                tx_id: 0,
                timestamp: 1000,
            })
            .expect("log entry");
            wal.sync().expect("sync");
        }

        // Corrupt the entry data (after the header)
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&wal_path)
                .expect("open file");

            // Corrupt byte at offset 10 (past header)
            file.seek(SeekFrom::Start(10)).expect("seek");
            file.write_all(&[0xFF]).expect("write");
            file.sync_all().expect("sync");
        }

        // Try to read - should detect CRC mismatch
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");
        wal.seek_to_start().expect("seek");

        let result = wal.read_entry();
        assert!(result.is_err(), "Should detect CRC mismatch");

        match result {
            Err(StorageError::WalCorrupted(msg)) => {
                assert!(
                    msg.contains("CRC32 mismatch"),
                    "Error should mention CRC mismatch: {}",
                    msg
                );
            }
            _ => panic!("Expected WalCorrupted error"),
        }
    }

    #[test]
    fn test_read_entry_eof_error() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Don't write anything - file is empty
        let result = wal.read_entry();

        assert!(result.is_err());
        match result {
            Err(StorageError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
            }
            _ => panic!("Expected EOF error"),
        }
    }

    #[test]
    fn test_read_all_entries() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write multiple entries
        let tx_id = wal.begin_transaction().expect("begin tx");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log insert");
        wal.log(WalEntry::CommitTx { tx_id }).expect("log commit");
        wal.sync().expect("sync");

        // Read all entries
        let entries = wal.read_all_entries().expect("read all");

        assert_eq!(entries.len(), 3);
        assert!(matches!(entries[0], WalEntry::BeginTx { .. }));
        assert!(matches!(entries[1], WalEntry::InsertNode { .. }));
        assert!(matches!(entries[2], WalEntry::CommitTx { .. }));
    }

    #[test]
    fn test_read_all_entries_empty_file() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        let entries = wal.read_all_entries().expect("read all");
        assert!(entries.is_empty(), "Empty WAL should have no entries");
    }

    #[test]
    fn test_needs_recovery_empty_wal() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        assert!(!wal.needs_recovery(), "Empty WAL should not need recovery");
    }

    #[test]
    fn test_needs_recovery_committed_transaction() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write a committed transaction
        let tx_id = wal.begin_transaction().expect("begin tx");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log insert");
        wal.log(WalEntry::CommitTx { tx_id }).expect("log commit");
        wal.sync().expect("sync");

        assert!(
            !wal.needs_recovery(),
            "Committed transaction should not need recovery"
        );
    }

    #[test]
    fn test_needs_recovery_uncommitted_transaction() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write an uncommitted transaction
        let _tx_id = wal.begin_transaction().expect("begin tx");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log insert");
        // No CommitTx!
        wal.sync().expect("sync");

        assert!(
            wal.needs_recovery(),
            "Uncommitted transaction should need recovery"
        );
    }

    #[test]
    fn test_needs_recovery_aborted_transaction() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write an aborted transaction
        let tx_id = wal.begin_transaction().expect("begin tx");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log insert");
        wal.log(WalEntry::AbortTx { tx_id }).expect("log abort");
        wal.sync().expect("sync");

        assert!(
            !wal.needs_recovery(),
            "Aborted transaction should not need recovery"
        );
    }

    #[test]
    fn test_needs_recovery_mixed_transactions() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Transaction 1: committed
        let tx1 = wal.begin_transaction().expect("begin tx1");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::CommitTx { tx_id: tx1 })
            .expect("commit tx1");

        // Transaction 2: uncommitted
        let _tx2 = wal.begin_transaction().expect("begin tx2");
        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        // No CommitTx!

        wal.sync().expect("sync");

        assert!(
            wal.needs_recovery(),
            "Mixed transactions with one uncommitted should need recovery"
        );
    }

    #[test]
    fn test_needs_recovery_checkpoint_clears_state() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write committed transaction followed by checkpoint
        let tx_id = wal.begin_transaction().expect("begin tx");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::CommitTx { tx_id }).expect("commit");
        wal.log(WalEntry::Checkpoint { version: 1 })
            .expect("checkpoint");
        wal.sync().expect("sync");

        assert!(
            !wal.needs_recovery(),
            "After checkpoint, should not need recovery"
        );
    }

    #[test]
    fn test_truncate_clears_file() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write some entries
        let tx_id = wal.begin_transaction().expect("begin tx");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::CommitTx { tx_id }).expect("commit");
        wal.sync().expect("sync");

        // Verify file has content
        let size_before = wal.file_size().expect("get size");
        assert!(size_before > 0, "WAL should have content before truncate");

        // Truncate
        wal.truncate().expect("truncate");

        // Verify file is empty
        let size_after = wal.file_size().expect("get size");
        assert_eq!(size_after, 0, "WAL should be empty after truncate");
    }

    #[test]
    fn test_truncate_allows_new_writes() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write, truncate, write again
        let tx1 = wal.begin_transaction().expect("begin tx1");
        wal.log(WalEntry::CommitTx { tx_id: tx1 })
            .expect("commit tx1");

        wal.truncate().expect("truncate");

        let tx2 = wal.begin_transaction().expect("begin tx2");
        wal.log(WalEntry::CommitTx { tx_id: tx2 })
            .expect("commit tx2");
        wal.sync().expect("sync");

        // Read back entries - should only see tx2's entries
        let entries = wal.read_all_entries().expect("read all");
        assert_eq!(entries.len(), 2, "Should have 2 entries from tx2");
    }

    #[test]
    fn test_file_size() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Empty file
        let size0 = wal.file_size().expect("get size");
        assert_eq!(size0, 0, "New WAL should be empty");

        // After one entry
        wal.log(WalEntry::BeginTx {
            tx_id: 0,
            timestamp: 1000,
        })
        .expect("log");
        wal.sync().expect("sync");

        let size1 = wal.file_size().expect("get size");
        assert!(size1 > 0, "WAL should have content after logging");
    }

    #[test]
    fn test_seek_to_start() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write an entry
        wal.log(WalEntry::BeginTx {
            tx_id: 42,
            timestamp: 1000,
        })
        .expect("log");
        wal.sync().expect("sync");

        // Seek to start and read
        wal.seek_to_start().expect("seek");
        let entry = wal.read_entry().expect("read");

        match entry {
            WalEntry::BeginTx { tx_id, .. } => assert_eq!(tx_id, 42),
            _ => panic!("Expected BeginTx"),
        }

        // Seek to start again and read again
        wal.seek_to_start().expect("seek");
        let entry2 = wal.read_entry().expect("read");

        match entry2 {
            WalEntry::BeginTx { tx_id, .. } => assert_eq!(tx_id, 42),
            _ => panic!("Expected BeginTx"),
        }
    }

    #[test]
    fn test_get_committed_entries_single_committed() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        let tx_id = wal.begin_transaction().expect("begin tx");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 2,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::CommitTx { tx_id }).expect("commit");
        wal.sync().expect("sync");

        let committed = wal.get_committed_entries().expect("get committed");

        // Should have 2 InsertNode entries (not BeginTx or CommitTx)
        assert_eq!(committed.len(), 2);
        assert!(matches!(
            committed[0],
            WalEntry::InsertNode {
                id: VertexId(0),
                ..
            }
        ));
        assert!(matches!(
            committed[1],
            WalEntry::InsertNode {
                id: VertexId(1),
                ..
            }
        ));
    }

    #[test]
    fn test_get_committed_entries_excludes_aborted() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Transaction 1: committed
        let tx1 = wal.begin_transaction().expect("begin tx1");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::CommitTx { tx_id: tx1 }).expect("commit");

        // Transaction 2: aborted
        let tx2 = wal.begin_transaction().expect("begin tx2");
        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 2,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::AbortTx { tx_id: tx2 }).expect("abort");

        wal.sync().expect("sync");

        let committed = wal.get_committed_entries().expect("get committed");

        // Should only have entries from tx1
        assert_eq!(committed.len(), 1);
        assert!(matches!(
            committed[0],
            WalEntry::InsertNode {
                id: VertexId(0),
                ..
            }
        ));
    }

    #[test]
    fn test_get_committed_entries_excludes_uncommitted() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Transaction 1: committed
        let tx1 = wal.begin_transaction().expect("begin tx1");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::CommitTx { tx_id: tx1 }).expect("commit");

        // Transaction 2: uncommitted
        let _tx2 = wal.begin_transaction().expect("begin tx2");
        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 2,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        // No commit!

        wal.sync().expect("sync");

        let committed = wal.get_committed_entries().expect("get committed");

        // Should only have entries from tx1
        assert_eq!(committed.len(), 1);
        assert!(matches!(
            committed[0],
            WalEntry::InsertNode {
                id: VertexId(0),
                ..
            }
        ));
    }

    #[test]
    fn test_get_committed_entries_preserves_order() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Transaction 1
        let tx1 = wal.begin_transaction().expect("begin tx1");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 1,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::CommitTx { tx_id: tx1 }).expect("commit");

        // Transaction 2
        let tx2 = wal.begin_transaction().expect("begin tx2");
        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 2,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::CommitTx { tx_id: tx2 }).expect("commit");

        // Transaction 3
        let tx3 = wal.begin_transaction().expect("begin tx3");
        wal.log(WalEntry::InsertNode {
            id: VertexId(2),
            record: SerializableNodeRecord {
                id: 2,
                label_id: 3,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("insert");
        wal.log(WalEntry::CommitTx { tx_id: tx3 }).expect("commit");

        wal.sync().expect("sync");

        let committed = wal.get_committed_entries().expect("get committed");

        // Should have entries in order: 0, 1, 2
        assert_eq!(committed.len(), 3);
        assert!(matches!(
            committed[0],
            WalEntry::InsertNode {
                id: VertexId(0),
                ..
            }
        ));
        assert!(matches!(
            committed[1],
            WalEntry::InsertNode {
                id: VertexId(1),
                ..
            }
        ));
        assert!(matches!(
            committed[2],
            WalEntry::InsertNode {
                id: VertexId(2),
                ..
            }
        ));
    }

    #[test]
    fn test_get_committed_entries_empty_wal() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        let committed = wal.get_committed_entries().expect("get committed");
        assert!(committed.is_empty());
    }

    #[test]
    fn test_roundtrip_write_read_all_entry_types() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let wal_path = dir.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        let entries_to_write = vec![
            WalEntry::BeginTx {
                tx_id: 0,
                timestamp: 1000,
            },
            WalEntry::InsertNode {
                id: VertexId(0),
                record: SerializableNodeRecord {
                    id: 0,
                    label_id: 1,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: 100,
                },
            },
            WalEntry::InsertEdge {
                id: EdgeId(0),
                record: SerializableEdgeRecord {
                    id: 0,
                    label_id: 2,
                    flags: 0,
                    src: 0,
                    dst: 1,
                    next_out: u64::MAX,
                    next_in: u64::MAX,
                    prop_head: 200,
                },
            },
            WalEntry::UpdateProperty {
                is_vertex: true,
                element_id: 0,
                key_id: 3,
                old_value: Value::Null,
                new_value: Value::String("hello".to_string()),
            },
            WalEntry::DeleteNode { id: VertexId(0) },
            WalEntry::DeleteEdge { id: EdgeId(0) },
            WalEntry::CommitTx { tx_id: 0 },
            WalEntry::AbortTx { tx_id: 1 },
            WalEntry::Checkpoint { version: 42 },
        ];

        // Write all entries
        for entry in &entries_to_write {
            wal.log(entry.clone()).expect("log entry");
        }
        wal.sync().expect("sync");

        // Read all entries
        let entries_read = wal.read_all_entries().expect("read all");

        // Verify they match
        assert_eq!(entries_read.len(), entries_to_write.len());
        for (i, (written, read)) in entries_to_write.iter().zip(entries_read.iter()).enumerate() {
            assert_eq!(written, read, "Entry {} mismatch", i);
        }
    }
}
