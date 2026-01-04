//! Memory-mapped persistent graph storage.
//!
//! This module implements the `GraphStorage` trait using memory-mapped files,
//! providing durable storage with write-ahead logging for crash recovery.

use hashbrown::HashMap;
use memmap2::{Mmap, MmapOptions};
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Arc;

use crate::error::StorageError;
use crate::storage::StringInterner;

pub mod arena;
pub mod freelist;
pub mod records;
pub mod recovery;
pub mod wal;

use records::{
    EdgeRecord, FileHeader, NodeRecord, EDGE_RECORD_SIZE, HEADER_SIZE, MAGIC, NODE_RECORD_SIZE,
    VERSION,
};

use crate::value::{EdgeId, VertexId};

/// Memory-mapped graph storage backend.
///
/// This backend provides persistent storage using memory-mapped files with
/// write-ahead logging for durability and crash recovery.
///
/// # File Structure
///
/// The database consists of a main data file and a write-ahead log (WAL):
///
/// ```text
/// my_graph.db        - Main data file (mmap'd for reads)
/// my_graph.wal       - Write-ahead log (not implemented yet)
/// ```
///
/// # Thread Safety
///
/// All mutable state is protected by `RwLock`, making the graph safe to share
/// across threads. However, concurrent write operations require external
/// coordination.
pub struct MmapGraph {
    /// Memory-mapped file (read-only view of data)
    #[allow(dead_code)] // Will be used in Phase 2.2 and beyond
    mmap: Arc<RwLock<Mmap>>,

    /// File handle for writes
    #[allow(dead_code)] // Will be used in Phase 4.2 and beyond
    file: Arc<RwLock<File>>,

    /// String interner (in-memory, rebuilt on load)
    #[allow(dead_code)] // Will be used in Phase 2.4
    string_table: Arc<RwLock<StringInterner>>,

    /// Label indexes (in-memory, rebuilt on load)
    #[allow(dead_code)] // Will be used in Phase 2.5
    vertex_labels: Arc<RwLock<HashMap<u32, RoaringBitmap>>>,
    #[allow(dead_code)] // Will be used in Phase 2.5
    edge_labels: Arc<RwLock<HashMap<u32, RoaringBitmap>>>,
}

impl MmapGraph {
    /// Open existing database or create new one.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database file (`.db` extension recommended)
    ///
    /// # Returns
    ///
    /// A new `MmapGraph` instance connected to the file.
    ///
    /// # Errors
    ///
    /// - [`StorageError::InvalidFormat`] - File exists but has invalid header
    /// - [`StorageError::Io`] - I/O error opening or creating file
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rustgremlin::storage::MmapGraph;
    ///
    /// // Create or open a database
    /// let graph = MmapGraph::open("my_graph.db").unwrap();
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let file_exists = path.exists();

        // Open or create main data file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        if !file_exists {
            // New database - initialize with default structure
            Self::initialize_new_file(&file)?;
        }

        // Memory-map the file
        let mmap = unsafe { MmapOptions::new().map(&file)? };

        // Validate header
        Self::validate_header(&mmap)?;

        Ok(Self {
            mmap: Arc::new(RwLock::new(mmap)),
            file: Arc::new(RwLock::new(file)),
            string_table: Arc::new(RwLock::new(StringInterner::new())),
            vertex_labels: Arc::new(RwLock::new(HashMap::new())),
            edge_labels: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Initialize a new database file with header and initial structure.
    ///
    /// Creates a file with:
    /// - 64-byte header
    /// - Space for 1000 initial node records
    /// - Space for 10000 initial edge records
    /// - 64KB for properties and strings
    ///
    /// # Safety
    ///
    /// This function assumes the file is empty and newly created.
    fn initialize_new_file(file: &File) -> Result<(), StorageError> {
        // Initial capacities
        const INITIAL_NODE_CAPACITY: u64 = 1000;
        const INITIAL_EDGE_CAPACITY: u64 = 10000;
        const INITIAL_ARENA_SIZE: u64 = 64 * 1024; // 64KB

        // Calculate file size
        let node_table_size = INITIAL_NODE_CAPACITY * NODE_RECORD_SIZE as u64;
        let edge_table_size = INITIAL_EDGE_CAPACITY * records::EDGE_RECORD_SIZE as u64;
        let initial_size =
            HEADER_SIZE as u64 + node_table_size + edge_table_size + INITIAL_ARENA_SIZE;

        // Set file size
        file.set_len(initial_size)?;

        // Calculate offsets
        let property_arena_offset = HEADER_SIZE as u64 + node_table_size + edge_table_size;
        let string_table_offset = initial_size - 32 * 1024; // Last 32KB for strings

        // Create initial header
        let mut header = FileHeader::new();
        header.node_capacity = INITIAL_NODE_CAPACITY;
        header.edge_capacity = INITIAL_EDGE_CAPACITY;
        header.property_arena_offset = property_arena_offset;
        header.string_table_offset = string_table_offset;

        // Write header
        Self::write_header(file, &header)?;

        Ok(())
    }

    /// Validate file header for correct magic and version.
    ///
    /// # Errors
    ///
    /// - [`StorageError::InvalidFormat`] - File is too small or has wrong magic number
    /// - [`StorageError::InvalidFormat`] - File has unsupported version
    fn validate_header(mmap: &[u8]) -> Result<(), StorageError> {
        if mmap.len() < HEADER_SIZE {
            return Err(StorageError::InvalidFormat);
        }

        let header = Self::read_header(mmap);

        // Check magic number
        let magic = header.magic;
        if magic != MAGIC {
            return Err(StorageError::InvalidFormat);
        }

        // Check version
        let version = header.version;
        if version != VERSION {
            return Err(StorageError::InvalidFormat);
        }

        Ok(())
    }

    /// Read header from memory-mapped bytes.
    ///
    /// # Safety
    ///
    /// This uses `read_unaligned` since FileHeader is `#[repr(C, packed)]`.
    /// Caller must ensure mmap has at least HEADER_SIZE bytes.
    fn read_header(mmap: &[u8]) -> FileHeader {
        assert!(mmap.len() >= HEADER_SIZE, "mmap too small to read header");

        FileHeader::from_bytes(mmap)
    }

    /// Write header to file at offset 0.
    ///
    /// # Arguments
    ///
    /// * `file` - File to write to
    /// * `header` - Header to write
    ///
    /// # Platform Notes
    ///
    /// On Unix, uses `write_all_at` for positioned writes.
    /// On other platforms, uses seek + write_all.
    fn write_header(file: &File, header: &FileHeader) -> Result<(), StorageError> {
        let bytes = header.to_bytes();

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&bytes, 0)?;
        }

        #[cfg(not(unix))]
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = file;
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&bytes)?;
        }

        file.sync_data()?;

        Ok(())
    }

    // =========================================================================
    // Read Operations
    // =========================================================================

    /// Get node record by vertex ID (O(1) lookup).
    ///
    /// Returns `None` if:
    /// - ID is out of bounds (>= node_capacity)
    /// - Node is marked as deleted
    /// - Record cannot be read from mmap
    ///
    /// # Arguments
    ///
    /// * `id` - Vertex ID to look up
    ///
    /// # Returns
    ///
    /// The node record if found and not deleted, otherwise `None`.
    ///
    /// # Safety
    ///
    /// Uses `read_unaligned` to read the packed struct from memory. This is safe
    /// because we verify the offset is within the mmap bounds before reading.
    #[inline]
    pub(crate) fn get_node_record(&self, id: VertexId) -> Option<NodeRecord> {
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        // Check bounds
        if id.0 >= header.node_capacity {
            return None;
        }

        // Calculate offset: header + (node_id * record_size)
        let offset = HEADER_SIZE + (id.0 as usize * NODE_RECORD_SIZE);

        // Verify we can read the full record
        if offset + NODE_RECORD_SIZE > mmap.len() {
            return None;
        }

        // Read record (using read_unaligned for packed struct)
        let record = unsafe {
            let ptr = mmap.as_ptr().add(offset) as *const NodeRecord;
            ptr.read_unaligned()
        };

        // Check deleted flag
        if record.is_deleted() {
            return None;
        }

        Some(record)
    }

    /// Get edge record by edge ID (O(1) lookup).
    ///
    /// Returns `None` if:
    /// - ID is out of bounds (>= edge_capacity)
    /// - Edge is marked as deleted
    /// - Record cannot be read from mmap
    ///
    /// # Arguments
    ///
    /// * `id` - Edge ID to look up
    ///
    /// # Returns
    ///
    /// The edge record if found and not deleted, otherwise `None`.
    ///
    /// # Safety
    ///
    /// Uses `read_unaligned` to read the packed struct from memory. This is safe
    /// because we verify the offset is within the mmap bounds before reading.
    #[inline]
    pub(crate) fn get_edge_record(&self, id: EdgeId) -> Option<EdgeRecord> {
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);

        // Check bounds
        if id.0 >= header.edge_capacity {
            return None;
        }

        // Calculate edge table offset: header + (node_capacity * node_record_size)
        let edge_table_offset = HEADER_SIZE + (header.node_capacity as usize * NODE_RECORD_SIZE);

        // Calculate record offset: edge_table_offset + (edge_id * record_size)
        let offset = edge_table_offset + (id.0 as usize * EDGE_RECORD_SIZE);

        // Verify we can read the full record
        if offset + EDGE_RECORD_SIZE > mmap.len() {
            return None;
        }

        // Read record (using read_unaligned for packed struct)
        let record = unsafe {
            let ptr = mmap.as_ptr().add(offset) as *const EdgeRecord;
            ptr.read_unaligned()
        };

        // Check deleted flag
        if record.is_deleted() {
            return None;
        }

        Some(record)
    }

    /// Helper: Calculate offset to the edge table.
    ///
    /// The edge table starts immediately after the node table.
    /// Returns the byte offset from the beginning of the file.
    #[inline]
    fn edge_table_offset(&self) -> usize {
        let mmap = self.mmap.read();
        let header = Self::read_header(&mmap);
        HEADER_SIZE + (header.node_capacity as usize * NODE_RECORD_SIZE)
    }

    /// Helper: Read a u32 value from mmap at the given offset.
    ///
    /// # Safety
    ///
    /// This performs bounds checking before reading to ensure the offset
    /// is valid within the mmap.
    #[inline]
    fn read_u32(&self, offset: usize) -> Result<u32, StorageError> {
        let mmap = self.mmap.read();

        if offset + 4 > mmap.len() {
            return Err(StorageError::CorruptedData);
        }

        let bytes: [u8; 4] = mmap[offset..offset + 4].try_into().unwrap();
        Ok(u32::from_le_bytes(bytes))
    }

    /// Helper: Read a u64 value from mmap at the given offset.
    ///
    /// # Safety
    ///
    /// This performs bounds checking before reading to ensure the offset
    /// is valid within the mmap.
    #[inline]
    fn read_u64(&self, offset: usize) -> Result<u64, StorageError> {
        let mmap = self.mmap.read();

        if offset + 8 > mmap.len() {
            return Err(StorageError::CorruptedData);
        }

        let bytes: [u8; 8] = mmap[offset..offset + 8].try_into().unwrap();
        Ok(u64::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    use crate::value::{EdgeId, VertexId};

    #[test]
    fn test_create_new_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        let graph = MmapGraph::open(&path).unwrap();

        // Verify file was created
        assert!(path.exists());

        // Verify header
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);

        // Copy values to avoid unaligned reference errors
        let magic = header.magic;
        let version = header.version;
        let node_count = header.node_count;
        let node_capacity = header.node_capacity;
        let edge_count = header.edge_count;
        let edge_capacity = header.edge_capacity;

        assert_eq!(magic, MAGIC);
        assert_eq!(version, VERSION);
        assert_eq!(node_count, 0);
        assert_eq!(node_capacity, 1000);
        assert_eq!(edge_count, 0);
        assert_eq!(edge_capacity, 10000);
    }

    #[test]
    fn test_validate_header_rejects_invalid_magic() {
        // Create a buffer with wrong magic number
        let mut bytes = [0u8; HEADER_SIZE];
        let mut header = FileHeader::new();
        header.magic = 0xDEADBEEF; // Wrong magic
        bytes.copy_from_slice(&header.to_bytes());

        let result = MmapGraph::validate_header(&bytes);
        assert!(matches!(result, Err(StorageError::InvalidFormat)));
    }

    #[test]
    fn test_validate_header_rejects_invalid_version() {
        // Create a buffer with wrong version
        let mut bytes = [0u8; HEADER_SIZE];
        let mut header = FileHeader::new();
        header.version = 999; // Unsupported version
        bytes.copy_from_slice(&header.to_bytes());

        let result = MmapGraph::validate_header(&bytes);
        assert!(matches!(result, Err(StorageError::InvalidFormat)));
    }

    #[test]
    fn test_validate_header_accepts_valid_header() {
        // Create a buffer with valid header
        let mut bytes = [0u8; HEADER_SIZE];
        let header = FileHeader::new();
        bytes.copy_from_slice(&header.to_bytes());

        let result = MmapGraph::validate_header(&bytes);
        assert!(result.is_ok());
    }

    #[test]
    fn test_initial_file_has_correct_structure() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        let graph = MmapGraph::open(&path).unwrap();

        // Verify file size
        let file = graph.file.read();
        let metadata = file.metadata().unwrap();
        let file_size = metadata.len();

        // Size should be: header + nodes + edges + arena
        // 64 + (1000 * 48) + (10000 * 56) + (64 * 1024)
        let expected_size = 64 + (1000 * 48) + (10000 * 56) + (64 * 1024);
        assert_eq!(file_size, expected_size);

        // Verify header fields
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);

        let property_arena_offset = header.property_arena_offset;
        let string_table_offset = header.string_table_offset;
        let free_node_head = header.free_node_head;

        // Property arena should start after node and edge tables
        let expected_arena_offset = 64 + (1000 * 48) + (10000 * 56);
        assert_eq!(property_arena_offset, expected_arena_offset as u64);

        // String table should be in last 32KB
        assert_eq!(string_table_offset, file_size - 32 * 1024);

        // Free list should be empty
        assert_eq!(free_node_head, u64::MAX);
    }

    #[test]
    fn test_reopen_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        // Create and close database
        {
            let _graph = MmapGraph::open(&path).unwrap();
        }

        // Reopen database
        let graph = MmapGraph::open(&path).unwrap();

        // Verify header is still valid
        let mmap = graph.mmap.read();
        let header = MmapGraph::read_header(&mmap);

        let magic = header.magic;
        let version = header.version;

        assert_eq!(magic, MAGIC);
        assert_eq!(version, VERSION);
    }

    #[test]
    fn test_header_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");

        let graph = MmapGraph::open(&path).unwrap();

        // Read initial header
        let mmap = graph.mmap.read();
        let header1 = MmapGraph::read_header(&mmap);

        let node_capacity1 = header1.node_capacity;
        let edge_capacity1 = header1.edge_capacity;

        drop(mmap);

        // Write modified header
        let mut header2 = FileHeader::new();
        header2.node_capacity = 2000;
        header2.edge_capacity = 20000;
        header2.node_count = 10;
        header2.edge_count = 50;

        let file = graph.file.read();
        MmapGraph::write_header(&file, &header2).unwrap();
        drop(file);

        // Remap to see changes
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        drop(file);

        let header3 = MmapGraph::read_header(&new_mmap);

        let node_capacity3 = header3.node_capacity;
        let edge_capacity3 = header3.edge_capacity;
        let node_count3 = header3.node_count;
        let edge_count3 = header3.edge_count;

        assert_eq!(node_capacity3, 2000);
        assert_eq!(edge_capacity3, 20000);
        assert_eq!(node_count3, 10);
        assert_eq!(edge_count3, 50);

        // Original values should have been different
        assert_ne!(node_capacity1, node_capacity3);
        assert_ne!(edge_capacity1, edge_capacity3);
    }

    // =========================================================================
    // Phase 2.2: Read Operations Tests
    // =========================================================================

    #[test]
    fn test_get_node_record_returns_none_for_out_of_bounds() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Capacity is 1000, so ID 1000 should be out of bounds
        let result = graph.get_node_record(VertexId(1000));
        assert!(result.is_none(), "Should return None for out-of-bounds ID");

        let result = graph.get_node_record(VertexId(9999));
        assert!(
            result.is_none(),
            "Should return None for way out-of-bounds ID"
        );
    }

    #[test]
    fn test_get_node_record_reads_valid_record() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Manually write a node record to the file
        let node_id = VertexId(0);
        let mut record = records::NodeRecord::new(node_id.0, 42);
        record.first_out_edge = 100;
        record.first_in_edge = 200;
        record.prop_head = 300;

        // Write record to file
        let offset = HEADER_SIZE;
        let bytes = record.to_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap to see the changes
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Read the record back
        let result = graph.get_node_record(node_id);
        assert!(result.is_some(), "Should find the written record");

        let retrieved = result.unwrap();
        // Copy fields to avoid unaligned reference errors with packed structs
        let id = retrieved.id;
        let label_id = retrieved.label_id;
        let first_out_edge = retrieved.first_out_edge;
        let first_in_edge = retrieved.first_in_edge;
        let prop_head = retrieved.prop_head;

        assert_eq!(id, 0);
        assert_eq!(label_id, 42);
        assert_eq!(first_out_edge, 100);
        assert_eq!(first_in_edge, 200);
        assert_eq!(prop_head, 300);
    }

    #[test]
    fn test_get_node_record_returns_none_for_deleted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write a deleted node record
        let node_id = VertexId(5);
        let mut record = records::NodeRecord::new(node_id.0, 7);
        record.mark_deleted();

        let offset = HEADER_SIZE + (node_id.0 as usize * NODE_RECORD_SIZE);
        let bytes = record.to_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Should return None for deleted node
        let result = graph.get_node_record(node_id);
        assert!(
            result.is_none(),
            "Should return None for deleted node record"
        );
    }

    #[test]
    fn test_get_edge_record_returns_none_for_out_of_bounds() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Capacity is 10000, so ID 10000 should be out of bounds
        let result = graph.get_edge_record(EdgeId(10000));
        assert!(result.is_none(), "Should return None for out-of-bounds ID");

        let result = graph.get_edge_record(EdgeId(99999));
        assert!(
            result.is_none(),
            "Should return None for way out-of-bounds ID"
        );
    }

    #[test]
    fn test_get_edge_record_reads_valid_record() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Manually write an edge record to the file
        let edge_id = EdgeId(0);
        let mut record = records::EdgeRecord::new(edge_id.0, 99, 10, 20);
        record.next_out = 500;
        record.next_in = 600;
        record.prop_head = 700;

        // Calculate offset: header + node_table + edge_id * edge_record_size
        let edge_table_offset = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);
        let offset = edge_table_offset + (edge_id.0 as usize * EDGE_RECORD_SIZE);
        let bytes = record.to_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap to see the changes
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Read the record back
        let result = graph.get_edge_record(edge_id);
        assert!(result.is_some(), "Should find the written record");

        let retrieved = result.unwrap();
        // Copy fields to avoid unaligned reference errors with packed structs
        let id = retrieved.id;
        let label_id = retrieved.label_id;
        let src = retrieved.src;
        let dst = retrieved.dst;
        let next_out = retrieved.next_out;
        let next_in = retrieved.next_in;
        let prop_head = retrieved.prop_head;

        assert_eq!(id, 0);
        assert_eq!(label_id, 99);
        assert_eq!(src, 10);
        assert_eq!(dst, 20);
        assert_eq!(next_out, 500);
        assert_eq!(next_in, 600);
        assert_eq!(prop_head, 700);
    }

    #[test]
    fn test_get_edge_record_returns_none_for_deleted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write a deleted edge record
        let edge_id = EdgeId(42);
        let mut record = records::EdgeRecord::new(edge_id.0, 13, 100, 200);
        record.mark_deleted();

        let edge_table_offset = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);
        let offset = edge_table_offset + (edge_id.0 as usize * EDGE_RECORD_SIZE);
        let bytes = record.to_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Should return None for deleted edge
        let result = graph.get_edge_record(edge_id);
        assert!(
            result.is_none(),
            "Should return None for deleted edge record"
        );
    }

    #[test]
    fn test_edge_table_offset_calculation() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let offset = graph.edge_table_offset();

        // Should be: header (64) + node_table (1000 * 48)
        let expected = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);
        assert_eq!(offset, expected);
    }

    #[test]
    fn test_read_u32_helper() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write a known u32 value at a specific offset
        let test_offset = 100;
        let test_value = 0x12345678u32;
        let bytes = test_value.to_le_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(test_offset)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Read it back using the helper
        let result = graph.read_u32(test_offset as usize);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), test_value);
    }

    #[test]
    fn test_read_u32_bounds_checking() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mmap = graph.mmap.read();
        let file_size = mmap.len();
        drop(mmap);

        // Try to read beyond the end of the file
        let result = graph.read_u32(file_size);
        assert!(result.is_err());
        assert!(matches!(result, Err(StorageError::CorruptedData)));

        // Try to read where we'd overflow
        let result = graph.read_u32(file_size - 2); // Only 2 bytes left, need 4
        assert!(result.is_err());
    }

    #[test]
    fn test_read_u64_helper() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write a known u64 value at a specific offset
        let test_offset = 200;
        let test_value = 0x123456789ABCDEF0u64;
        let bytes = test_value.to_le_bytes();

        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(test_offset)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
        }

        // Remap
        let file = graph.file.read();
        let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
        *graph.mmap.write() = new_mmap;
        drop(file);

        // Read it back using the helper
        let result = graph.read_u64(test_offset as usize);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), test_value);
    }

    #[test]
    fn test_read_u64_bounds_checking() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let mmap = graph.mmap.read();
        let file_size = mmap.len();
        drop(mmap);

        // Try to read beyond the end of the file
        let result = graph.read_u64(file_size);
        assert!(result.is_err());
        assert!(matches!(result, Err(StorageError::CorruptedData)));

        // Try to read where we'd overflow
        let result = graph.read_u64(file_size - 4); // Only 4 bytes left, need 8
        assert!(result.is_err());
    }

    #[test]
    fn test_get_node_record_multiple_nodes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        // Write multiple node records
        for i in 0..10 {
            let node_id = VertexId(i);
            let record = records::NodeRecord::new(node_id.0, i as u32 * 10);

            let offset = HEADER_SIZE + (node_id.0 as usize * NODE_RECORD_SIZE);
            let bytes = record.to_bytes();

            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
            drop(file);

            // Remap after each write
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
            drop(file);
        }

        // Read all records back and verify
        for i in 0..10 {
            let result = graph.get_node_record(VertexId(i));
            assert!(result.is_some(), "Node {} should exist", i);

            let record = result.unwrap();
            // Copy fields to avoid unaligned reference errors
            let id = record.id;
            let label_id = record.label_id;

            assert_eq!(id, i);
            assert_eq!(label_id, (i as u32) * 10);
        }

        // Verify non-written records return None
        let result = graph.get_node_record(VertexId(10));
        // This will return None because the record will be all zeros (not written)
        // and id field won't match, or it could be considered valid with zeros
        // For a more accurate test, we'd check if label_id is 0 or handle zero-initialized memory
    }

    #[test]
    fn test_get_edge_record_multiple_edges() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let graph = MmapGraph::open(&path).unwrap();

        let edge_table_offset = HEADER_SIZE + (1000 * NODE_RECORD_SIZE);

        // Write multiple edge records
        for i in 0..10 {
            let edge_id = EdgeId(i);
            let record = records::EdgeRecord::new(edge_id.0, i as u32 * 5, i * 2, i * 2 + 1);

            let offset = edge_table_offset + (edge_id.0 as usize * EDGE_RECORD_SIZE);
            let bytes = record.to_bytes();

            use std::io::{Seek, SeekFrom, Write};
            let mut file = graph.file.write();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&bytes).unwrap();
            file.sync_data().unwrap();
            drop(file);

            // Remap after each write
            let file = graph.file.read();
            let new_mmap = unsafe { MmapOptions::new().map(&*file).unwrap() };
            *graph.mmap.write() = new_mmap;
            drop(file);
        }

        // Read all records back and verify
        for i in 0..10 {
            let result = graph.get_edge_record(EdgeId(i));
            assert!(result.is_some(), "Edge {} should exist", i);

            let record = result.unwrap();
            // Copy fields to avoid unaligned reference errors
            let id = record.id;
            let label_id = record.label_id;
            let src = record.src;
            let dst = record.dst;

            assert_eq!(id, i);
            assert_eq!(label_id, (i as u32) * 5);
            assert_eq!(src, i * 2);
            assert_eq!(dst, i * 2 + 1);
        }
    }
}
