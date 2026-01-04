//! On-disk record formats for memory-mapped storage.
//!
//! This module defines the fixed-size record structures used in the mmap file format.

// =============================================================================
// File Format Constants
// =============================================================================

/// Magic number identifying RustGremlin database files ("GRML" in ASCII)
pub const MAGIC: u32 = 0x47524D4C;

/// File format version
pub const VERSION: u32 = 1;

/// Size of the file header in bytes
pub const HEADER_SIZE: usize = 64;

/// Size of a node record in bytes
pub const NODE_RECORD_SIZE: usize = 48;

/// Size of an edge record in bytes
pub const EDGE_RECORD_SIZE: usize = 56;

// =============================================================================
// FileHeader
// =============================================================================

/// File header at offset 0 (64 bytes total)
///
/// The header contains metadata about the database file, including counts,
/// capacities, and offsets to major file sections.
///
/// # Layout
///
/// ```text
/// Offset | Size | Field
/// -------|------|-------------------
/// 0      | 4    | magic
/// 4      | 4    | version
/// 8      | 8    | node_count
/// 16     | 8    | node_capacity
/// 24     | 8    | edge_count
/// 32     | 8    | edge_capacity
/// 40     | 8    | string_table_offset
/// 48     | 8    | property_arena_offset
/// 56     | 8    | free_node_head
/// ```
///
/// Note: This implementation follows the plan which specifies a single free_node_head.
/// Edge free list tracking will be handled separately in later phases if needed.
#[repr(C, packed)]
#[derive(Copy, Clone, Debug)]
pub struct FileHeader {
    /// Magic number (must be 0x47524D4C "GRML")
    pub magic: u32,

    /// File format version (currently 1)
    pub version: u32,

    /// Number of active (non-deleted) nodes
    pub node_count: u64,

    /// Total allocated slots in node table
    pub node_capacity: u64,

    /// Number of active (non-deleted) edges
    pub edge_count: u64,

    /// Total allocated slots in edge table
    pub edge_capacity: u64,

    /// Byte offset to start of string table
    pub string_table_offset: u64,

    /// Byte offset to start of property arena
    pub property_arena_offset: u64,

    /// First free node slot ID (u64::MAX if empty)
    pub free_node_head: u64,
}

impl FileHeader {
    /// Create a new header with default values
    pub fn new() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            node_count: 0,
            node_capacity: 0,
            edge_count: 0,
            edge_capacity: 0,
            string_table_offset: 0,
            property_arena_offset: 0,
            free_node_head: u64::MAX,
        }
    }

    /// Read header from bytes
    ///
    /// # Safety
    ///
    /// This uses `read_unaligned` because the struct is `#[repr(C, packed)]`,
    /// which means fields may not be naturally aligned.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() >= HEADER_SIZE,
            "Buffer too small for FileHeader"
        );

        unsafe {
            let ptr = bytes.as_ptr() as *const FileHeader;
            ptr.read_unaligned()
        }
    }

    /// Write header to bytes
    ///
    /// # Safety
    ///
    /// This creates a byte slice from the packed struct. Since the struct
    /// is `#[repr(C, packed)]`, we can safely interpret it as bytes.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        unsafe {
            let ptr = self as *const FileHeader as *const u8;
            let slice = std::slice::from_raw_parts(ptr, HEADER_SIZE);
            let mut result = [0u8; HEADER_SIZE];
            result.copy_from_slice(slice);
            result
        }
    }
}

impl Default for FileHeader {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// NodeRecord
// =============================================================================

/// Node (vertex) flag: marks a deleted node
pub const NODE_FLAG_DELETED: u32 = 0x0001;

/// Node (vertex) flag: has property indexes (reserved for future use)
pub const NODE_FLAG_INDEXED: u32 = 0x0002;

/// On-disk vertex record (48 bytes)
///
/// Fixed-size record for each vertex in the graph. Uses linked lists to track
/// adjacency (outgoing and incoming edges).
///
/// # Layout
///
/// ```text
/// Offset | Size | Field
/// -------|------|-------------
/// 0      | 8    | id
/// 8      | 4    | label_id
/// 12     | 4    | flags
/// 16     | 8    | first_out_edge
/// 24     | 8    | first_in_edge
/// 32     | 8    | prop_head
/// 40     | 8    | (padding to 48)
/// ```
///
/// # Fields
///
/// - **id**: Vertex ID (0-based index in node table)
/// - **label_id**: String table ID for the vertex label
/// - **flags**: Status flags (deleted, indexed, etc.)
/// - **first_out_edge**: First outgoing edge ID (u64::MAX if none)
/// - **first_in_edge**: First incoming edge ID (u64::MAX if none)
/// - **prop_head**: Property list head offset in arena (u64::MAX if none)
#[repr(C, packed)]
#[derive(Copy, Clone, Debug)]
pub struct NodeRecord {
    /// Vertex ID (0-based)
    pub id: u64,

    /// String table ID for label
    pub label_id: u32,

    /// Status flags (NODE_FLAG_*)
    pub flags: u32,

    /// First outgoing edge ID (u64::MAX if none)
    pub first_out_edge: u64,

    /// First incoming edge ID (u64::MAX if none)
    pub first_in_edge: u64,

    /// Property list head offset (u64::MAX if none)
    pub prop_head: u64,

    /// Padding to reach 48 bytes
    _padding: u64,
}

impl NodeRecord {
    /// Create a new node record
    pub fn new(id: u64, label_id: u32) -> Self {
        Self {
            id,
            label_id,
            flags: 0,
            first_out_edge: u64::MAX,
            first_in_edge: u64::MAX,
            prop_head: u64::MAX,
            _padding: 0,
        }
    }

    /// Check if this node is deleted
    pub fn is_deleted(&self) -> bool {
        self.flags & NODE_FLAG_DELETED != 0
    }

    /// Mark this node as deleted
    pub fn mark_deleted(&mut self) {
        self.flags |= NODE_FLAG_DELETED;
    }

    /// Read node record from bytes
    ///
    /// # Safety
    ///
    /// Uses `read_unaligned` because the struct is `#[repr(C, packed)]`.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() >= NODE_RECORD_SIZE,
            "Buffer too small for NodeRecord"
        );

        unsafe {
            let ptr = bytes.as_ptr() as *const NodeRecord;
            ptr.read_unaligned()
        }
    }

    /// Write node record to bytes
    ///
    /// # Safety
    ///
    /// Creates a byte slice from the packed struct.
    pub fn to_bytes(&self) -> [u8; NODE_RECORD_SIZE] {
        unsafe {
            let ptr = self as *const NodeRecord as *const u8;
            let slice = std::slice::from_raw_parts(ptr, NODE_RECORD_SIZE);
            let mut result = [0u8; NODE_RECORD_SIZE];
            result.copy_from_slice(slice);
            result
        }
    }
}

// =============================================================================
// EdgeRecord
// =============================================================================

/// Edge flag: marks a deleted edge
pub const EDGE_FLAG_DELETED: u32 = 0x0001;

/// On-disk edge record (56 bytes)
///
/// Fixed-size record for each edge in the graph. Includes linked-list pointers
/// for efficient adjacency list traversal.
///
/// # Layout
///
/// ```text
/// Offset | Size | Field
/// -------|------|-------------
/// 0      | 8    | id
/// 8      | 4    | label_id
/// 12     | 4    | flags (stores edge flags)
/// 16     | 8    | src
/// 24     | 8    | dst
/// 32     | 8    | next_out
/// 40     | 8    | next_in
/// 48     | 8    | prop_head
/// ```
///
/// # Fields
///
/// - **id**: Edge ID (0-based index in edge table)
/// - **label_id**: String table ID for the edge label
/// - **flags**: Status flags (deleted, etc.)
/// - **src**: Source vertex ID
/// - **dst**: Destination vertex ID
/// - **next_out**: Next outgoing edge from src (u64::MAX if last)
/// - **next_in**: Next incoming edge to dst (u64::MAX if last)
/// - **prop_head**: Property list head offset in arena (u64::MAX if none)
#[repr(C, packed)]
#[derive(Copy, Clone, Debug)]
pub struct EdgeRecord {
    /// Edge ID (0-based)
    pub id: u64,

    /// String table ID for label
    pub label_id: u32,

    /// Status flags (EDGE_FLAG_*)
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

impl EdgeRecord {
    /// Create a new edge record
    pub fn new(id: u64, label_id: u32, src: u64, dst: u64) -> Self {
        Self {
            id,
            label_id,
            flags: 0,
            src,
            dst,
            next_out: u64::MAX,
            next_in: u64::MAX,
            prop_head: u64::MAX,
        }
    }

    /// Check if this edge is deleted
    pub fn is_deleted(&self) -> bool {
        self.flags & EDGE_FLAG_DELETED != 0
    }

    /// Mark this edge as deleted
    pub fn mark_deleted(&mut self) {
        self.flags |= EDGE_FLAG_DELETED;
    }

    /// Read edge record from bytes
    ///
    /// # Safety
    ///
    /// Uses `read_unaligned` because the struct is `#[repr(C, packed)]`.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(
            bytes.len() >= EDGE_RECORD_SIZE,
            "Buffer too small for EdgeRecord"
        );

        unsafe {
            let ptr = bytes.as_ptr() as *const EdgeRecord;
            ptr.read_unaligned()
        }
    }

    /// Write edge record to bytes
    ///
    /// # Safety
    ///
    /// Creates a byte slice from the packed struct.
    pub fn to_bytes(&self) -> [u8; EDGE_RECORD_SIZE] {
        unsafe {
            let ptr = self as *const EdgeRecord as *const u8;
            let slice = std::slice::from_raw_parts(ptr, EDGE_RECORD_SIZE);
            let mut result = [0u8; EDGE_RECORD_SIZE];
            result.copy_from_slice(slice);
            result
        }
    }
}

// Property arena structures will be added in Phase 1.4

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_header_size() {
        // FileHeader must be exactly 64 bytes
        assert_eq!(
            std::mem::size_of::<FileHeader>(),
            HEADER_SIZE,
            "FileHeader size must be exactly 64 bytes"
        );
    }

    #[test]
    fn test_file_header_alignment() {
        // Verify the packed struct has expected layout

        // magic and version are u32 (4 bytes each) = 8 bytes
        // 7 u64 fields (7 × 8 bytes) = 56 bytes
        // Total: 8 + 56 = 64 bytes

        assert_eq!(
            std::mem::size_of::<FileHeader>(),
            4 + 4 + (7 * 8),
            "FileHeader fields should sum to 64 bytes"
        );
    }

    #[test]
    fn test_file_header_default_values() {
        let header = FileHeader::new();

        // Copy fields to avoid unaligned reference errors
        let magic = header.magic;
        let version = header.version;
        let node_count = header.node_count;
        let node_capacity = header.node_capacity;
        let edge_count = header.edge_count;
        let edge_capacity = header.edge_capacity;
        let string_table_offset = header.string_table_offset;
        let property_arena_offset = header.property_arena_offset;
        let free_node_head = header.free_node_head;

        assert_eq!(magic, MAGIC);
        assert_eq!(version, VERSION);
        assert_eq!(node_count, 0);
        assert_eq!(node_capacity, 0);
        assert_eq!(edge_count, 0);
        assert_eq!(edge_capacity, 0);
        assert_eq!(string_table_offset, 0);
        assert_eq!(property_arena_offset, 0);
        assert_eq!(free_node_head, u64::MAX);
    }

    #[test]
    fn test_file_header_roundtrip() {
        // Create a header with some values
        let mut header = FileHeader::new();
        header.node_count = 100;
        header.node_capacity = 1000;
        header.edge_count = 500;
        header.edge_capacity = 5000;
        header.string_table_offset = 123456;
        header.property_arena_offset = 789012;
        header.free_node_head = 42;

        // Copy original values
        let orig_magic = header.magic;
        let orig_version = header.version;
        let orig_node_count = header.node_count;
        let orig_node_capacity = header.node_capacity;
        let orig_edge_count = header.edge_count;
        let orig_edge_capacity = header.edge_capacity;
        let orig_string_table_offset = header.string_table_offset;
        let orig_property_arena_offset = header.property_arena_offset;
        let orig_free_node_head = header.free_node_head;

        // Convert to bytes
        let bytes = header.to_bytes();

        // Verify size
        assert_eq!(bytes.len(), HEADER_SIZE);

        // Convert back from bytes
        let recovered = FileHeader::from_bytes(&bytes);

        // Copy recovered values to avoid unaligned reference errors
        let rec_magic = recovered.magic;
        let rec_version = recovered.version;
        let rec_node_count = recovered.node_count;
        let rec_node_capacity = recovered.node_capacity;
        let rec_edge_count = recovered.edge_count;
        let rec_edge_capacity = recovered.edge_capacity;
        let rec_string_table_offset = recovered.string_table_offset;
        let rec_property_arena_offset = recovered.property_arena_offset;
        let rec_free_node_head = recovered.free_node_head;

        // Verify all fields match
        assert_eq!(rec_magic, orig_magic);
        assert_eq!(rec_version, orig_version);
        assert_eq!(rec_node_count, orig_node_count);
        assert_eq!(rec_node_capacity, orig_node_capacity);
        assert_eq!(rec_edge_count, orig_edge_count);
        assert_eq!(rec_edge_capacity, orig_edge_capacity);
        assert_eq!(rec_string_table_offset, orig_string_table_offset);
        assert_eq!(rec_property_arena_offset, orig_property_arena_offset);
        assert_eq!(rec_free_node_head, orig_free_node_head);
    }

    #[test]
    fn test_file_header_transmute_safety() {
        // Verify we can safely transmute between [u8; 64] and FileHeader
        let header = FileHeader::new();
        let bytes = header.to_bytes();

        // This should not panic
        let _ = FileHeader::from_bytes(&bytes);

        // Verify magic number is at correct offset
        let magic_bytes: [u8; 4] = [bytes[0], bytes[1], bytes[2], bytes[3]];
        let magic = u32::from_le_bytes(magic_bytes);
        assert_eq!(magic, MAGIC);

        // Verify version is at correct offset
        let version_bytes: [u8; 4] = [bytes[4], bytes[5], bytes[6], bytes[7]];
        let version = u32::from_le_bytes(version_bytes);
        assert_eq!(version, VERSION);
    }

    #[test]
    fn test_file_header_byte_order() {
        // Verify fields are stored in little-endian format
        let mut header = FileHeader::new();
        header.node_count = 0x0102030405060708u64;

        let bytes = header.to_bytes();

        // node_count starts at offset 8
        let node_count_bytes: [u8; 8] = [
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ];

        // Should be little-endian
        assert_eq!(node_count_bytes[0], 0x08);
        assert_eq!(node_count_bytes[7], 0x01);
    }

    #[test]
    fn test_constants() {
        assert_eq!(MAGIC, 0x47524D4C); // "GRML"
        assert_eq!(VERSION, 1);
        assert_eq!(HEADER_SIZE, 64);
        assert_eq!(NODE_RECORD_SIZE, 48);
        assert_eq!(EDGE_RECORD_SIZE, 56);
    }

    // =========================================================================
    // NodeRecord Tests
    // =========================================================================

    #[test]
    fn test_node_record_size() {
        // NodeRecord must be exactly 48 bytes
        assert_eq!(
            std::mem::size_of::<NodeRecord>(),
            NODE_RECORD_SIZE,
            "NodeRecord size must be exactly 48 bytes"
        );
    }

    #[test]
    fn test_node_record_alignment() {
        // Verify packed struct layout
        // id: u64 (8 bytes)
        // label_id: u32 (4 bytes)
        // flags: u32 (4 bytes)
        // first_out_edge: u64 (8 bytes)
        // first_in_edge: u64 (8 bytes)
        // prop_head: u64 (8 bytes)
        // _padding: u64 (8 bytes)
        // Total: 8 + 4 + 4 + 8 + 8 + 8 + 8 = 48 bytes

        assert_eq!(
            std::mem::size_of::<NodeRecord>(),
            8 + 4 + 4 + 8 + 8 + 8 + 8,
            "NodeRecord fields should sum to 48 bytes"
        );
    }

    #[test]
    fn test_node_record_new() {
        let record = NodeRecord::new(42, 7);

        // Copy values to avoid unaligned reference errors
        let id = record.id;
        let label_id = record.label_id;
        let flags = record.flags;
        let first_out_edge = record.first_out_edge;
        let first_in_edge = record.first_in_edge;
        let prop_head = record.prop_head;

        assert_eq!(id, 42);
        assert_eq!(label_id, 7);
        assert_eq!(flags, 0);
        assert_eq!(first_out_edge, u64::MAX);
        assert_eq!(first_in_edge, u64::MAX);
        assert_eq!(prop_head, u64::MAX);
        assert!(!record.is_deleted());
    }

    #[test]
    fn test_node_record_deleted_flag() {
        let mut record = NodeRecord::new(0, 0);

        assert!(!record.is_deleted());

        record.mark_deleted();
        assert!(record.is_deleted());

        // Verify flag is set correctly
        let flags = record.flags;
        assert_eq!(flags & NODE_FLAG_DELETED, NODE_FLAG_DELETED);
    }

    #[test]
    fn test_node_record_roundtrip() {
        let mut record = NodeRecord::new(123, 456);
        record.flags = 0x0003; // Set some flags
        record.first_out_edge = 789;
        record.first_in_edge = 101112;
        record.prop_head = 131415;

        // Copy original values
        let orig_id = record.id;
        let orig_label_id = record.label_id;
        let orig_flags = record.flags;
        let orig_first_out_edge = record.first_out_edge;
        let orig_first_in_edge = record.first_in_edge;
        let orig_prop_head = record.prop_head;

        // Convert to bytes
        let bytes = record.to_bytes();
        assert_eq!(bytes.len(), NODE_RECORD_SIZE);

        // Convert back from bytes
        let recovered = NodeRecord::from_bytes(&bytes);

        // Copy recovered values
        let rec_id = recovered.id;
        let rec_label_id = recovered.label_id;
        let rec_flags = recovered.flags;
        let rec_first_out_edge = recovered.first_out_edge;
        let rec_first_in_edge = recovered.first_in_edge;
        let rec_prop_head = recovered.prop_head;

        // Verify all fields match
        assert_eq!(rec_id, orig_id);
        assert_eq!(rec_label_id, orig_label_id);
        assert_eq!(rec_flags, orig_flags);
        assert_eq!(rec_first_out_edge, orig_first_out_edge);
        assert_eq!(rec_first_in_edge, orig_first_in_edge);
        assert_eq!(rec_prop_head, orig_prop_head);
    }

    #[test]
    fn test_node_record_byte_order() {
        // Verify fields are stored in little-endian format
        let record = NodeRecord::new(0x0102030405060708u64, 0x090A0B0Cu32);

        let bytes = record.to_bytes();

        // id starts at offset 0
        let id_bytes: [u8; 8] = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        assert_eq!(id_bytes[0], 0x08); // Little-endian: LSB first
        assert_eq!(id_bytes[7], 0x01);

        // label_id starts at offset 8
        let label_id_bytes: [u8; 4] = [bytes[8], bytes[9], bytes[10], bytes[11]];
        assert_eq!(label_id_bytes[0], 0x0C); // Little-endian: LSB first
        assert_eq!(label_id_bytes[3], 0x09);
    }

    #[test]
    fn test_node_record_no_unexpected_padding() {
        // Verify there's no unexpected padding in the struct
        // We explicitly include _padding field to reach 48 bytes

        let record = NodeRecord::new(0, 0);
        let bytes = record.to_bytes();

        // All 48 bytes should be defined (no random padding)
        assert_eq!(bytes.len(), 48);
    }

    // =========================================================================
    // EdgeRecord Tests
    // =========================================================================

    #[test]
    fn test_edge_record_size() {
        // EdgeRecord must be exactly 56 bytes
        assert_eq!(
            std::mem::size_of::<EdgeRecord>(),
            EDGE_RECORD_SIZE,
            "EdgeRecord size must be exactly 56 bytes"
        );
    }

    #[test]
    fn test_edge_record_alignment() {
        // Verify packed struct layout
        // id: u64 (8 bytes)
        // label_id: u32 (4 bytes)
        // flags: u32 (4 bytes)
        // src: u64 (8 bytes)
        // dst: u64 (8 bytes)
        // next_out: u64 (8 bytes)
        // next_in: u64 (8 bytes)
        // prop_head: u64 (8 bytes)
        // Total: 8 + 4 + 4 + 8 + 8 + 8 + 8 + 8 = 56 bytes

        assert_eq!(
            std::mem::size_of::<EdgeRecord>(),
            8 + 4 + 4 + 8 + 8 + 8 + 8 + 8,
            "EdgeRecord fields should sum to 56 bytes"
        );
    }

    #[test]
    fn test_edge_record_new() {
        let record = EdgeRecord::new(42, 7, 100, 200);

        // Copy values to avoid unaligned reference errors
        let id = record.id;
        let label_id = record.label_id;
        let flags = record.flags;
        let src = record.src;
        let dst = record.dst;
        let next_out = record.next_out;
        let next_in = record.next_in;
        let prop_head = record.prop_head;

        assert_eq!(id, 42);
        assert_eq!(label_id, 7);
        assert_eq!(flags, 0);
        assert_eq!(src, 100);
        assert_eq!(dst, 200);
        assert_eq!(next_out, u64::MAX);
        assert_eq!(next_in, u64::MAX);
        assert_eq!(prop_head, u64::MAX);
        assert!(!record.is_deleted());
    }

    #[test]
    fn test_edge_record_deleted_flag() {
        let mut record = EdgeRecord::new(0, 0, 0, 0);

        assert!(!record.is_deleted());

        record.mark_deleted();
        assert!(record.is_deleted());

        // Verify flag is set correctly
        let flags = record.flags;
        assert_eq!(flags & EDGE_FLAG_DELETED, EDGE_FLAG_DELETED);
    }

    #[test]
    fn test_edge_record_roundtrip() {
        let mut record = EdgeRecord::new(123, 456, 789, 101112);
        record.flags = 0x0001; // Set deleted flag
        record.next_out = 131415;
        record.next_in = 161718;
        record.prop_head = 192021;

        // Copy original values
        let orig_id = record.id;
        let orig_label_id = record.label_id;
        let orig_flags = record.flags;
        let orig_src = record.src;
        let orig_dst = record.dst;
        let orig_next_out = record.next_out;
        let orig_next_in = record.next_in;
        let orig_prop_head = record.prop_head;

        // Convert to bytes
        let bytes = record.to_bytes();
        assert_eq!(bytes.len(), EDGE_RECORD_SIZE);

        // Convert back from bytes
        let recovered = EdgeRecord::from_bytes(&bytes);

        // Copy recovered values
        let rec_id = recovered.id;
        let rec_label_id = recovered.label_id;
        let rec_flags = recovered.flags;
        let rec_src = recovered.src;
        let rec_dst = recovered.dst;
        let rec_next_out = recovered.next_out;
        let rec_next_in = recovered.next_in;
        let rec_prop_head = recovered.prop_head;

        // Verify all fields match
        assert_eq!(rec_id, orig_id);
        assert_eq!(rec_label_id, orig_label_id);
        assert_eq!(rec_flags, orig_flags);
        assert_eq!(rec_src, orig_src);
        assert_eq!(rec_dst, orig_dst);
        assert_eq!(rec_next_out, orig_next_out);
        assert_eq!(rec_next_in, orig_next_in);
        assert_eq!(rec_prop_head, orig_prop_head);
        assert!(recovered.is_deleted());
    }

    #[test]
    fn test_edge_record_byte_order() {
        // Verify fields are stored in little-endian format
        let record = EdgeRecord::new(
            0x0102030405060708u64,
            0x090A0B0Cu32,
            0x0D0E0F1011121314u64,
            0x1516171819202122u64,
        );

        let bytes = record.to_bytes();

        // id starts at offset 0
        let id_bytes: [u8; 8] = [
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ];
        assert_eq!(id_bytes[0], 0x08); // Little-endian: LSB first
        assert_eq!(id_bytes[7], 0x01);

        // label_id starts at offset 8
        let label_id_bytes: [u8; 4] = [bytes[8], bytes[9], bytes[10], bytes[11]];
        assert_eq!(label_id_bytes[0], 0x0C); // Little-endian: LSB first
        assert_eq!(label_id_bytes[3], 0x09);

        // src starts at offset 16
        let src_bytes: [u8; 8] = [
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
        ];
        assert_eq!(src_bytes[0], 0x14); // Little-endian: LSB first
        assert_eq!(src_bytes[7], 0x0D);
    }

    #[test]
    fn test_edge_record_no_unexpected_padding() {
        // Verify there's no unexpected padding in the struct
        let record = EdgeRecord::new(0, 0, 0, 0);
        let bytes = record.to_bytes();

        // All 56 bytes should be defined (no random padding)
        assert_eq!(bytes.len(), 56);
    }

    #[test]
    fn test_edge_record_linked_list_pointers() {
        // Test that next_out and next_in can be used for linked lists
        let mut edge1 = EdgeRecord::new(0, 0, 10, 20);
        let mut edge2 = EdgeRecord::new(1, 0, 10, 30);
        let mut edge3 = EdgeRecord::new(2, 0, 10, 40);

        // Build linked list: edge1 -> edge2 -> edge3 -> MAX
        edge1.next_out = 1; // Points to edge2
        edge2.next_out = 2; // Points to edge3
        edge3.next_out = u64::MAX; // End of list

        // Verify chain (copy values to avoid unaligned reference)
        let next_out_1 = edge1.next_out;
        let next_out_2 = edge2.next_out;
        let next_out_3 = edge3.next_out;

        assert_eq!(next_out_1, 1);
        assert_eq!(next_out_2, 2);
        assert_eq!(next_out_3, u64::MAX);

        // Verify this survives serialization
        let bytes1 = edge1.to_bytes();
        let recovered1 = EdgeRecord::from_bytes(&bytes1);
        let recovered_next_out = recovered1.next_out;
        assert_eq!(recovered_next_out, 1);
    }

    #[test]
    fn test_flag_constants() {
        // Verify flag constants are non-overlapping
        assert_eq!(NODE_FLAG_DELETED, 0x0001);
        assert_eq!(NODE_FLAG_INDEXED, 0x0002);
        assert_eq!(EDGE_FLAG_DELETED, 0x0001);

        // Verify flags can be combined
        let combined = NODE_FLAG_DELETED | NODE_FLAG_INDEXED;
        assert_eq!(combined, 0x0003);
        assert_ne!(combined & NODE_FLAG_DELETED, 0);
        assert_ne!(combined & NODE_FLAG_INDEXED, 0);
    }
}
