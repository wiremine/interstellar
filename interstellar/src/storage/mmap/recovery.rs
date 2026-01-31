//! Crash recovery logic for replaying write-ahead log.
//!
//! Recovers database to consistent state by replaying committed transactions from WAL.
//!
//! # Recovery Process
//!
//! When a database is opened after a crash or unclean shutdown, the recovery module:
//!
//! 1. Scans the WAL for all entries
//! 2. Identifies committed transactions (have both BeginTx and CommitTx entries)
//! 3. Replays committed transactions in order to the main data file
//! 4. Discards aborted and incomplete transactions
//! 5. Truncates the WAL after successful recovery
//!
//! # Transaction Model
//!
//! ```text
//! BeginTx { tx_id: 1 }
//! InsertNode { ... }
//! InsertEdge { ... }
//! CommitTx { tx_id: 1 }   <-- Transaction is committed
//!
//! BeginTx { tx_id: 2 }
//! InsertNode { ... }
//! AbortTx { tx_id: 2 }    <-- Transaction is aborted (discarded)
//!
//! BeginTx { tx_id: 3 }
//! InsertNode { ... }
//! <crash>                 <-- Transaction is incomplete (discarded)
//! ```
//!
//! # Idempotency
//!
//! Recovery is designed to be idempotent - running it multiple times on the same
//! WAL produces the same result. This is achieved by:
//!
//! - Always writing records at their designated positions (based on ID)
//! - Using deterministic position calculations
//! - Truncating the WAL only after successful recovery

use std::fs::File;

use crate::error::StorageError;
use crate::value::{EdgeId, VertexId};

use super::records::{EdgeRecord, NodeRecord, EDGE_RECORD_SIZE, HEADER_SIZE, NODE_RECORD_SIZE};
use super::wal::{WalEntry, WriteAheadLog};

/// Recover database from WAL.
///
/// This function reads all entries from the WAL, identifies committed transactions,
/// and replays their operations to the main data file. After successful recovery,
/// the WAL is truncated.
///
/// # Arguments
///
/// * `wal` - The write-ahead log to recover from
/// * `data_file` - The main data file to apply changes to
/// * `node_capacity` - Current node table capacity (for offset calculations)
///
/// # Recovery Algorithm
///
/// 1. Read all committed entries from the WAL using `get_committed_entries()`
/// 2. For each entry, apply the operation to the data file:
///    - `InsertNode`: Write node record at calculated offset
///    - `InsertEdge`: Write edge record at calculated offset
///    - `DeleteNode`: Mark node as deleted
///    - `DeleteEdge`: Mark edge as deleted
///    - `UpdateProperty`: (Not implemented yet - requires property arena)
/// 3. Sync the data file
/// 4. Truncate the WAL
///
/// # Errors
///
/// - [`StorageError::Io`] - I/O error reading WAL or writing to data file
/// - [`StorageError::WalCorrupted`] - WAL entry is corrupted
///
/// # Example
///
/// ```ignore
/// use interstellar::storage::mmap::recovery::recover;
/// use interstellar::storage::mmap::wal::WriteAheadLog;
/// use std::fs::File;
///
/// let mut wal = WriteAheadLog::open("my_graph.wal")?;
/// let data_file = File::options().read(true).write(true).open("my_graph.db")?;
///
/// // Check if recovery is needed
/// if wal.needs_recovery() {
///     recover(&mut wal, &data_file, 1000)?;
/// }
/// ```
pub fn recover(
    wal: &mut WriteAheadLog,
    data_file: &File,
    node_capacity: u64,
) -> Result<RecoveryStats, StorageError> {
    let mut stats = RecoveryStats::default();

    // Get all committed entries
    let entries = wal.get_committed_entries()?;

    if entries.is_empty() {
        // Nothing to recover, just truncate WAL if needed
        if wal.file_size()? > 0 {
            wal.truncate()?;
        }
        return Ok(stats);
    }

    // Replay each entry
    for entry in entries {
        match entry {
            WalEntry::InsertNode { id, record } => {
                write_node_to_file(data_file, id, &record.into())?;
                stats.nodes_recovered += 1;
            }
            WalEntry::InsertEdge { id, record } => {
                write_edge_to_file(data_file, id, &record.into(), node_capacity)?;
                stats.edges_recovered += 1;
            }
            WalEntry::DeleteNode { id } => {
                mark_node_deleted(data_file, id)?;
                stats.nodes_deleted += 1;
            }
            WalEntry::DeleteEdge { id } => {
                mark_edge_deleted(data_file, id, node_capacity)?;
                stats.edges_deleted += 1;
            }
            WalEntry::UpdateProperty { .. } => {
                // Property updates require property arena support
                // For now, skip these during recovery
                // TODO: Implement property update recovery in Phase 4
                stats.properties_updated += 1;
            }
            WalEntry::SchemaUpdate { offset, data } => {
                write_schema_to_file(data_file, offset, &data)?;
                stats.schema_updates += 1;
            }
            // Transaction markers are not replayed
            WalEntry::BeginTx { .. }
            | WalEntry::CommitTx { .. }
            | WalEntry::AbortTx { .. }
            | WalEntry::Checkpoint { .. } => {}
            // Index operations are handled via the index specs JSON file
            // during MmapGraph::open(), so we skip them during recovery
            WalEntry::CreateIndex { .. } | WalEntry::DropIndex { .. } => {}
        }
    }

    // Sync the data file to ensure all changes are persisted
    data_file.sync_data()?;

    // Truncate the WAL now that recovery is complete
    wal.truncate()?;

    Ok(stats)
}

/// Statistics from a recovery operation.
///
/// Provides information about what was recovered from the WAL.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveryStats {
    /// Number of node records recovered
    pub nodes_recovered: u64,
    /// Number of edge records recovered
    pub edges_recovered: u64,
    /// Number of nodes marked as deleted
    pub nodes_deleted: u64,
    /// Number of edges marked as deleted
    pub edges_deleted: u64,
    /// Number of property updates applied
    pub properties_updated: u64,
    /// Number of schema updates applied
    pub schema_updates: u64,
}

impl RecoveryStats {
    /// Check if any operations were performed
    pub fn is_empty(&self) -> bool {
        self.nodes_recovered == 0
            && self.edges_recovered == 0
            && self.nodes_deleted == 0
            && self.edges_deleted == 0
            && self.properties_updated == 0
            && self.schema_updates == 0
    }

    /// Total number of operations performed
    pub fn total_operations(&self) -> u64 {
        self.nodes_recovered
            + self.edges_recovered
            + self.nodes_deleted
            + self.edges_deleted
            + self.properties_updated
            + self.schema_updates
    }
}

// =============================================================================
// Helper Functions for Writing Records
// =============================================================================

/// Calculate the byte offset for a node record.
///
/// Node records are stored immediately after the header, in a contiguous array.
///
/// # Formula
///
/// `offset = HEADER_SIZE + (vertex_id * NODE_RECORD_SIZE)`
#[inline]
fn node_offset(id: VertexId) -> u64 {
    HEADER_SIZE as u64 + (id.0 * NODE_RECORD_SIZE as u64)
}

/// Calculate the byte offset for an edge record.
///
/// Edge records are stored after all node records, in a contiguous array.
///
/// # Formula
///
/// `offset = HEADER_SIZE + (node_capacity * NODE_RECORD_SIZE) + (edge_id * EDGE_RECORD_SIZE)`
#[inline]
fn edge_offset(id: EdgeId, node_capacity: u64) -> u64 {
    HEADER_SIZE as u64
        + (node_capacity * NODE_RECORD_SIZE as u64)
        + (id.0 * EDGE_RECORD_SIZE as u64)
}

/// Write a node record to the data file at the correct offset.
///
/// # Platform Notes
///
/// On Unix, uses `write_all_at` for positioned writes without seeking.
/// On other platforms, uses seek + write_all.
fn write_node_to_file(file: &File, id: VertexId, record: &NodeRecord) -> Result<(), StorageError> {
    let offset = node_offset(id);
    let bytes = record.to_bytes();

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(&bytes, offset)?;
    }

    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&bytes)?;
    }

    Ok(())
}

/// Write an edge record to the data file at the correct offset.
///
/// # Platform Notes
///
/// On Unix, uses `write_all_at` for positioned writes without seeking.
/// On other platforms, uses seek + write_all.
fn write_edge_to_file(
    file: &File,
    id: EdgeId,
    record: &EdgeRecord,
    node_capacity: u64,
) -> Result<(), StorageError> {
    let offset = edge_offset(id, node_capacity);
    let bytes = record.to_bytes();

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(&bytes, offset)?;
    }

    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&bytes)?;
    }

    Ok(())
}

/// Mark a node as deleted in the data file.
///
/// Reads the current record, sets the deleted flag, and writes it back.
fn mark_node_deleted(file: &File, id: VertexId) -> Result<(), StorageError> {
    let offset = node_offset(id);

    // Read current record
    let mut bytes = [0u8; NODE_RECORD_SIZE];

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(&mut bytes, offset)?;
    }

    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut bytes)?;
    }

    let mut record = NodeRecord::from_bytes(&bytes);
    record.mark_deleted();

    // Write back
    let updated_bytes = record.to_bytes();

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(&updated_bytes, offset)?;
    }

    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&updated_bytes)?;
    }

    Ok(())
}

/// Mark an edge as deleted in the data file.
///
/// Reads the current record, sets the deleted flag, and writes it back.
fn mark_edge_deleted(file: &File, id: EdgeId, node_capacity: u64) -> Result<(), StorageError> {
    let offset = edge_offset(id, node_capacity);

    // Read current record
    let mut bytes = [0u8; EDGE_RECORD_SIZE];

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(&mut bytes, offset)?;
    }

    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut bytes)?;
    }

    let mut record = EdgeRecord::from_bytes(&bytes);
    record.mark_deleted();

    // Write back
    let updated_bytes = record.to_bytes();

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(&updated_bytes, offset)?;
    }

    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&updated_bytes)?;
    }

    Ok(())
}

/// Write schema data to the data file at the specified offset.
///
/// Used during recovery to replay schema updates from the WAL.
fn write_schema_to_file(file: &File, offset: u64, data: &[u8]) -> Result<(), StorageError> {
    // Ensure file is large enough
    let metadata = file.metadata()?;
    let required_size = offset + data.len() as u64;
    if required_size > metadata.len() {
        file.set_len(required_size)?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(data, offset)?;
    }

    #[cfg(not(unix))]
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = file;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
    }

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mmap::records::FileHeader;
    use crate::storage::mmap::wal::{SerializableEdgeRecord, SerializableNodeRecord, WalEntry};
    use std::fs::OpenOptions;
    use tempfile::TempDir;

    /// Helper: Create a test database file with header and space for records
    fn create_test_db(dir: &TempDir, node_capacity: u64, edge_capacity: u64) -> File {
        let db_path = dir.path().join("test.db");

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&db_path)
            .expect("create db file");

        // Calculate file size
        let node_table_size = node_capacity * NODE_RECORD_SIZE as u64;
        let edge_table_size = edge_capacity * EDGE_RECORD_SIZE as u64;
        let file_size = HEADER_SIZE as u64 + node_table_size + edge_table_size;

        file.set_len(file_size).expect("set file length");

        // Write header
        let mut header = FileHeader::new();
        header.node_capacity = node_capacity;
        header.edge_capacity = edge_capacity;

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&header.to_bytes(), 0)
                .expect("write header");
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = &file;
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&header.to_bytes()).unwrap();
        }

        file.sync_all().expect("sync file");

        file
    }

    /// Helper: Read a node record from the test file
    fn read_node_record(file: &File, id: VertexId) -> NodeRecord {
        let offset = node_offset(id);
        let mut bytes = [0u8; NODE_RECORD_SIZE];

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.read_exact_at(&mut bytes, offset).expect("read node");
        }

        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = file;
            f.seek(SeekFrom::Start(offset)).unwrap();
            f.read_exact(&mut bytes).unwrap();
        }

        NodeRecord::from_bytes(&bytes)
    }

    /// Helper: Read an edge record from the test file
    fn read_edge_record(file: &File, id: EdgeId, node_capacity: u64) -> EdgeRecord {
        let offset = edge_offset(id, node_capacity);
        let mut bytes = [0u8; EDGE_RECORD_SIZE];

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.read_exact_at(&mut bytes, offset).expect("read edge");
        }

        #[cfg(not(unix))]
        {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = file;
            f.seek(SeekFrom::Start(offset)).unwrap();
            f.read_exact(&mut bytes).unwrap();
        }

        EdgeRecord::from_bytes(&bytes)
    }

    // =========================================================================
    // Offset Calculation Tests
    // =========================================================================

    #[test]
    fn test_node_offset_calculation() {
        // Node 0 should be at HEADER_SIZE
        assert_eq!(node_offset(VertexId(0)), HEADER_SIZE as u64);

        // Node 1 should be at HEADER_SIZE + NODE_RECORD_SIZE
        assert_eq!(
            node_offset(VertexId(1)),
            HEADER_SIZE as u64 + NODE_RECORD_SIZE as u64
        );

        // Node 100 should be at HEADER_SIZE + 100 * NODE_RECORD_SIZE
        assert_eq!(
            node_offset(VertexId(100)),
            HEADER_SIZE as u64 + 100 * NODE_RECORD_SIZE as u64
        );
    }

    #[test]
    fn test_edge_offset_calculation() {
        let node_capacity = 1000u64;

        // Edge 0 should be after all node records
        let expected_base = HEADER_SIZE as u64 + node_capacity * NODE_RECORD_SIZE as u64;
        assert_eq!(edge_offset(EdgeId(0), node_capacity), expected_base);

        // Edge 1 should be at base + EDGE_RECORD_SIZE
        assert_eq!(
            edge_offset(EdgeId(1), node_capacity),
            expected_base + EDGE_RECORD_SIZE as u64
        );

        // Edge 500 should be at base + 500 * EDGE_RECORD_SIZE
        assert_eq!(
            edge_offset(EdgeId(500), node_capacity),
            expected_base + 500 * EDGE_RECORD_SIZE as u64
        );
    }

    // =========================================================================
    // RecoveryStats Tests
    // =========================================================================

    #[test]
    fn test_recovery_stats_default_is_empty() {
        let stats = RecoveryStats::default();
        assert!(stats.is_empty());
        assert_eq!(stats.total_operations(), 0);
    }

    #[test]
    fn test_recovery_stats_not_empty_with_operations() {
        let mut stats = RecoveryStats::default();
        stats.nodes_recovered = 5;
        assert!(!stats.is_empty());
        assert_eq!(stats.total_operations(), 5);
    }

    #[test]
    fn test_recovery_stats_total_operations() {
        let stats = RecoveryStats {
            nodes_recovered: 10,
            edges_recovered: 20,
            nodes_deleted: 3,
            edges_deleted: 2,
            properties_updated: 5,
            schema_updates: 0,
        };
        assert_eq!(stats.total_operations(), 40);
    }

    // =========================================================================
    // Write Helper Tests
    // =========================================================================

    #[test]
    fn test_write_node_to_file() {
        let dir = TempDir::new().unwrap();
        let file = create_test_db(&dir, 100, 100);

        let node = NodeRecord::new(42, 7);
        write_node_to_file(&file, VertexId(42), &node).expect("write node");
        file.sync_all().expect("sync");

        let read_back = read_node_record(&file, VertexId(42));

        // Copy fields from packed struct
        let id = read_back.id;
        let label_id = read_back.label_id;

        assert_eq!(id, 42);
        assert_eq!(label_id, 7);
    }

    #[test]
    fn test_write_edge_to_file() {
        let dir = TempDir::new().unwrap();
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        let edge = EdgeRecord::new(10, 5, 1, 2);
        write_edge_to_file(&file, EdgeId(10), &edge, node_capacity).expect("write edge");
        file.sync_all().expect("sync");

        let read_back = read_edge_record(&file, EdgeId(10), node_capacity);

        // Copy fields from packed struct
        let id = read_back.id;
        let label_id = read_back.label_id;
        let src = read_back.src;
        let dst = read_back.dst;

        assert_eq!(id, 10);
        assert_eq!(label_id, 5);
        assert_eq!(src, 1);
        assert_eq!(dst, 2);
    }

    #[test]
    fn test_mark_node_deleted() {
        let dir = TempDir::new().unwrap();
        let file = create_test_db(&dir, 100, 100);

        // First write a node
        let node = NodeRecord::new(5, 3);
        write_node_to_file(&file, VertexId(5), &node).expect("write node");
        file.sync_all().expect("sync");

        // Verify it's not deleted
        let before = read_node_record(&file, VertexId(5));
        assert!(!before.is_deleted());

        // Mark as deleted
        mark_node_deleted(&file, VertexId(5)).expect("mark deleted");
        file.sync_all().expect("sync");

        // Verify it's deleted
        let after = read_node_record(&file, VertexId(5));
        assert!(after.is_deleted());
    }

    #[test]
    fn test_mark_edge_deleted() {
        let dir = TempDir::new().unwrap();
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        // First write an edge
        let edge = EdgeRecord::new(7, 2, 0, 1);
        write_edge_to_file(&file, EdgeId(7), &edge, node_capacity).expect("write edge");
        file.sync_all().expect("sync");

        // Verify it's not deleted
        let before = read_edge_record(&file, EdgeId(7), node_capacity);
        assert!(!before.is_deleted());

        // Mark as deleted
        mark_edge_deleted(&file, EdgeId(7), node_capacity).expect("mark deleted");
        file.sync_all().expect("sync");

        // Verify it's deleted
        let after = read_edge_record(&file, EdgeId(7), node_capacity);
        assert!(after.is_deleted());
    }

    // =========================================================================
    // Recovery Integration Tests
    // =========================================================================

    #[test]
    fn test_recover_empty_wal() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        let stats = recover(&mut wal, &file, node_capacity).expect("recover");

        assert!(stats.is_empty());
        assert_eq!(stats.total_operations(), 0);
    }

    #[test]
    fn test_recover_single_committed_node() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write a committed transaction with one node
        let tx_id = wal.begin_transaction().expect("begin tx");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 42,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log insert");
        wal.log(WalEntry::CommitTx { tx_id }).expect("commit");
        wal.sync().expect("sync");

        // Recover
        let stats = recover(&mut wal, &file, node_capacity).expect("recover");

        assert_eq!(stats.nodes_recovered, 1);
        assert_eq!(stats.edges_recovered, 0);
        assert_eq!(stats.total_operations(), 1);

        // Verify the node was written
        let node = read_node_record(&file, VertexId(0));
        let id = node.id;
        let label_id = node.label_id;

        assert_eq!(id, 0);
        assert_eq!(label_id, 42);

        // WAL should be truncated
        assert_eq!(wal.file_size().expect("file size"), 0);
    }

    #[test]
    fn test_recover_multiple_committed_transactions() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Transaction 1: Insert node 0
        let tx1 = wal.begin_transaction().expect("begin tx1");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 10,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        wal.log(WalEntry::CommitTx { tx_id: tx1 }).expect("commit");

        // Transaction 2: Insert node 1 and edge 0
        let tx2 = wal.begin_transaction().expect("begin tx2");
        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 20,
                flags: 0,
                first_out_edge: 0,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        wal.log(WalEntry::InsertEdge {
            id: EdgeId(0),
            record: SerializableEdgeRecord {
                id: 0,
                label_id: 5,
                flags: 0,
                src: 0,
                dst: 1,
                next_out: u64::MAX,
                next_in: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        wal.log(WalEntry::CommitTx { tx_id: tx2 }).expect("commit");

        wal.sync().expect("sync");

        // Recover
        let stats = recover(&mut wal, &file, node_capacity).expect("recover");

        assert_eq!(stats.nodes_recovered, 2);
        assert_eq!(stats.edges_recovered, 1);

        // Verify nodes - copy fields from packed struct to avoid unaligned reference
        let node0 = read_node_record(&file, VertexId(0));
        let (node0_id, node0_label) = (node0.id, node0.label_id);
        assert_eq!(node0_id, 0);
        assert_eq!(node0_label, 10);

        let node1 = read_node_record(&file, VertexId(1));
        let (node1_id, node1_label) = (node1.id, node1.label_id);
        assert_eq!(node1_id, 1);
        assert_eq!(node1_label, 20);

        // Verify edge - copy fields from packed struct
        let edge0 = read_edge_record(&file, EdgeId(0), node_capacity);
        let (edge0_id, edge0_label, edge0_src, edge0_dst) =
            (edge0.id, edge0.label_id, edge0.src, edge0.dst);
        assert_eq!(edge0_id, 0);
        assert_eq!(edge0_label, 5);
        assert_eq!(edge0_src, 0);
        assert_eq!(edge0_dst, 1);
    }

    #[test]
    fn test_recover_discards_uncommitted_transactions() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Transaction 1: Committed
        let tx1 = wal.begin_transaction().expect("begin tx1");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 100,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        wal.log(WalEntry::CommitTx { tx_id: tx1 }).expect("commit");

        // Transaction 2: NOT committed (simulates crash)
        let _tx2 = wal.begin_transaction().expect("begin tx2");
        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 999, // This should NOT appear in the DB
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        // No commit!

        wal.sync().expect("sync");

        // Recover
        let stats = recover(&mut wal, &file, node_capacity).expect("recover");

        // Only 1 node recovered (from committed tx)
        assert_eq!(stats.nodes_recovered, 1);

        // Verify node 0 was written (committed) - copy fields from packed struct
        let node0 = read_node_record(&file, VertexId(0));
        let (node0_id, node0_label) = (node0.id, node0.label_id);
        assert_eq!(node0_id, 0);
        assert_eq!(node0_label, 100);

        // Verify node 1 was NOT written (uncommitted)
        // The record will be all zeros since we didn't write to that slot
        let node1 = read_node_record(&file, VertexId(1));
        let node1_id = node1.id;
        let node1_label = node1.label_id;
        // Uncommitted node should not have label_id 999
        assert!(
            node1_id != 1 || node1_label != 999,
            "Uncommitted transaction should not be recovered"
        );
    }

    #[test]
    fn test_recover_discards_aborted_transactions() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Transaction 1: Committed
        let tx1 = wal.begin_transaction().expect("begin tx1");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 50,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        wal.log(WalEntry::CommitTx { tx_id: tx1 }).expect("commit");

        // Transaction 2: Aborted
        let tx2 = wal.begin_transaction().expect("begin tx2");
        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 888, // This should NOT appear
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        wal.log(WalEntry::AbortTx { tx_id: tx2 }).expect("abort");

        // Transaction 3: Committed
        let tx3 = wal.begin_transaction().expect("begin tx3");
        wal.log(WalEntry::InsertNode {
            id: VertexId(2),
            record: SerializableNodeRecord {
                id: 2,
                label_id: 60,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        wal.log(WalEntry::CommitTx { tx_id: tx3 }).expect("commit");

        wal.sync().expect("sync");

        // Recover
        let stats = recover(&mut wal, &file, node_capacity).expect("recover");

        // Only 2 nodes recovered (tx1 and tx3)
        assert_eq!(stats.nodes_recovered, 2);

        // Verify node 0 and 2 were written - copy fields from packed struct
        let node0 = read_node_record(&file, VertexId(0));
        let node0_label = node0.label_id;
        assert_eq!(node0_label, 50);

        let node2 = read_node_record(&file, VertexId(2));
        let node2_label = node2.label_id;
        assert_eq!(node2_label, 60);

        // Verify node 1 was NOT written (aborted)
        let node1 = read_node_record(&file, VertexId(1));
        assert!(
            node1.label_id != 888,
            "Aborted transaction should not be recovered"
        );
    }

    #[test]
    fn test_recover_handles_delete_operations() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Transaction 1: Insert and delete
        let tx1 = wal.begin_transaction().expect("begin tx1");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 10,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        wal.log(WalEntry::InsertNode {
            id: VertexId(1),
            record: SerializableNodeRecord {
                id: 1,
                label_id: 20,
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        wal.log(WalEntry::DeleteNode { id: VertexId(0) })
            .expect("log delete");
        wal.log(WalEntry::CommitTx { tx_id: tx1 }).expect("commit");

        wal.sync().expect("sync");

        // Recover
        let stats = recover(&mut wal, &file, node_capacity).expect("recover");

        assert_eq!(stats.nodes_recovered, 2);
        assert_eq!(stats.nodes_deleted, 1);

        // Node 0 should be deleted
        let node0 = read_node_record(&file, VertexId(0));
        assert!(node0.is_deleted(), "Node 0 should be marked as deleted");

        // Node 1 should exist
        let node1 = read_node_record(&file, VertexId(1));
        assert!(!node1.is_deleted(), "Node 1 should not be deleted");
    }

    #[test]
    fn test_recover_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        // First, write some data and recover
        {
            let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

            let tx_id = wal.begin_transaction().expect("begin tx");
            wal.log(WalEntry::InsertNode {
                id: VertexId(0),
                record: SerializableNodeRecord {
                    id: 0,
                    label_id: 123,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            })
            .expect("log");
            wal.log(WalEntry::CommitTx { tx_id }).expect("commit");
            wal.sync().expect("sync");

            let stats = recover(&mut wal, &file, node_capacity).expect("recover");
            assert_eq!(stats.nodes_recovered, 1);
        }

        // WAL should be empty now
        {
            let mut wal = WriteAheadLog::open(&wal_path).expect("reopen WAL");
            assert_eq!(wal.file_size().expect("file size"), 0);

            // Recovery on empty WAL should succeed with no operations
            let stats = recover(&mut wal, &file, node_capacity).expect("recover again");
            assert!(stats.is_empty());
        }

        // Data should still be there - copy field from packed struct
        let node = read_node_record(&file, VertexId(0));
        let node_label = node.label_id;
        assert_eq!(node_label, 123);
    }

    #[test]
    fn test_recover_truncates_wal_on_success() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Write a transaction
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
        .expect("log");
        wal.log(WalEntry::CommitTx { tx_id }).expect("commit");
        wal.sync().expect("sync");

        // WAL should have content
        assert!(wal.file_size().expect("file size") > 0);

        // Recover
        recover(&mut wal, &file, node_capacity).expect("recover");

        // WAL should be truncated
        assert_eq!(wal.file_size().expect("file size"), 0);
    }

    #[test]
    fn test_recover_preserves_order() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Transaction 1: Insert node 0 with label 1
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
        .expect("log");
        wal.log(WalEntry::CommitTx { tx_id: tx1 }).expect("commit");

        // Transaction 2: Update node 0 with label 2 (by re-inserting)
        let tx2 = wal.begin_transaction().expect("begin tx2");
        wal.log(WalEntry::InsertNode {
            id: VertexId(0),
            record: SerializableNodeRecord {
                id: 0,
                label_id: 2, // Updated label
                flags: 0,
                first_out_edge: u64::MAX,
                first_in_edge: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log");
        wal.log(WalEntry::CommitTx { tx_id: tx2 }).expect("commit");

        wal.sync().expect("sync");

        // Recover
        recover(&mut wal, &file, node_capacity).expect("recover");

        // Node should have the final label (2) since tx2 came after tx1
        // Copy field from packed struct to avoid unaligned reference
        let node = read_node_record(&file, VertexId(0));
        let node_label = node.label_id;
        assert_eq!(
            node_label, 2,
            "Later transaction should overwrite earlier one"
        );
    }

    #[test]
    fn test_recover_mixed_nodes_and_edges() {
        let dir = TempDir::new().unwrap();
        let wal_path = dir.path().join("test.wal");
        let node_capacity = 100;
        let file = create_test_db(&dir, node_capacity, 100);

        let mut wal = WriteAheadLog::open(&wal_path).expect("open WAL");

        // Complex transaction with multiple nodes and edges
        let tx_id = wal.begin_transaction().expect("begin tx");

        // Insert 3 nodes
        for i in 0..3 {
            wal.log(WalEntry::InsertNode {
                id: VertexId(i),
                record: SerializableNodeRecord {
                    id: i,
                    label_id: (i * 10) as u32,
                    flags: 0,
                    first_out_edge: u64::MAX,
                    first_in_edge: u64::MAX,
                    prop_head: u64::MAX,
                },
            })
            .expect("log node");
        }

        // Insert 2 edges
        wal.log(WalEntry::InsertEdge {
            id: EdgeId(0),
            record: SerializableEdgeRecord {
                id: 0,
                label_id: 100,
                flags: 0,
                src: 0,
                dst: 1,
                next_out: u64::MAX,
                next_in: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log edge");

        wal.log(WalEntry::InsertEdge {
            id: EdgeId(1),
            record: SerializableEdgeRecord {
                id: 1,
                label_id: 101,
                flags: 0,
                src: 1,
                dst: 2,
                next_out: u64::MAX,
                next_in: u64::MAX,
                prop_head: u64::MAX,
            },
        })
        .expect("log edge");

        wal.log(WalEntry::CommitTx { tx_id }).expect("commit");
        wal.sync().expect("sync");

        // Recover
        let stats = recover(&mut wal, &file, node_capacity).expect("recover");

        assert_eq!(stats.nodes_recovered, 3);
        assert_eq!(stats.edges_recovered, 2);

        // Verify all nodes - copy fields from packed struct
        for i in 0..3 {
            let node = read_node_record(&file, VertexId(i));
            let (node_id, node_label) = (node.id, node.label_id);
            assert_eq!(node_id, i);
            assert_eq!(node_label, (i * 10) as u32);
        }

        // Verify all edges - copy fields from packed struct
        let edge0 = read_edge_record(&file, EdgeId(0), node_capacity);
        let (edge0_id, edge0_src, edge0_dst) = (edge0.id, edge0.src, edge0.dst);
        assert_eq!(edge0_id, 0);
        assert_eq!(edge0_src, 0);
        assert_eq!(edge0_dst, 1);

        let edge1 = read_edge_record(&file, EdgeId(1), node_capacity);
        let (edge1_id, edge1_src, edge1_dst) = (edge1.id, edge1.src, edge1.dst);
        assert_eq!(edge1_id, 1);
        assert_eq!(edge1_src, 1);
        assert_eq!(edge1_dst, 2);
    }
}
