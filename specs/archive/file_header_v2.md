# File Header V2 Specification

This specification defines an enhanced file header for the memory-mapped storage backend, addressing gaps identified against `guiding-documents/file_versioning.md`.

---

## Motivation

The current V1 header lacks several features recommended for a production-grade versioned layout:

1. No page size declaration
2. No flags/capabilities field
3. Undifferentiated error for magic vs version mismatch
4. No explicit endianness declaration
5. No header integrity checksum
6. Limited reserved space for future expansion

This specification introduces Header V2 to address these gaps while maintaining the ability to read V1 files.

---

## Header V2 Layout

Total size: **192 bytes**

```text
Offset | Size | Field                  | Description
-------|------|------------------------|------------------------------------------
0      | 4    | magic                  | Magic number 0x47524D4C ("GRML")
4      | 4    | version                | File format version (2)
8      | 4    | min_reader_version     | Minimum library version that can read this file
12     | 4    | page_size              | Page size in bytes (default 4096)
16     | 4    | flags                  | Feature flags bitfield
20     | 1    | endianness             | Byte order indicator (1 = little, 2 = big)
21     | 3    | _padding1              | Alignment padding
24     | 8    | node_count             | Number of active (non-deleted) nodes
32     | 8    | node_capacity          | Total allocated slots in node table
40     | 8    | edge_count             | Number of active (non-deleted) edges
48     | 8    | edge_capacity          | Total allocated slots in edge table
56     | 8    | string_table_offset    | Byte offset to start of string table
64     | 8    | string_table_end       | Byte offset to end of string table data
72     | 8    | property_arena_offset  | Byte offset to start of property arena
80     | 8    | arena_next_offset      | Current write position in property arena
88     | 8    | free_node_head         | First free node slot (u64::MAX if empty)
96     | 8    | free_edge_head         | First free edge slot (u64::MAX if empty)
104    | 8    | next_node_id           | Next node ID to allocate
112    | 8    | next_edge_id           | Next edge ID to allocate
120    | 8    | schema_offset          | Byte offset to schema region (0 = none)
128    | 8    | schema_size            | Size of schema data in bytes
136    | 4    | schema_version         | Schema format version
140    | 12   | _schema_reserved       | Reserved for future schema fields
152    | 4    | header_crc32           | CRC32 of bytes 0-151 (header excluding this field)
156    | 36   | _reserved              | Reserved for future use (zero-filled)
```

---

## Field Specifications

### Identity Fields

#### `magic` (u32)
- **Value**: `0x47524D4C` (ASCII "GRML")
- **Purpose**: Identifies the file as an Interstellar database
- **Validation**: Must match exactly; reject with `InvalidFormat` if not

#### `version` (u32)
- **Value**: `2` for this specification
- **Purpose**: Indicates the physical layout version
- **Validation**: See version compatibility section

#### `min_reader_version` (u32)
- **Value**: Minimum library version required to read this file
- **Purpose**: Enables forward compatibility signaling
- **Default**: Equal to `version` for files without backward-compatible features
- **Example**: A V3 file that only uses V2 features could set `min_reader_version = 2`

### Configuration Fields

#### `page_size` (u32)
- **Value**: Page size in bytes
- **Default**: `4096`
- **Valid values**: Powers of 2 from 512 to 65536
- **Purpose**: Enables future page size tuning and documents alignment requirements

#### `flags` (u32)
- **Purpose**: Bitfield for optional features and capabilities
- **Initial flags** (all reserved for future use):

```text
Bit | Name              | Description
----|-------------------|------------------------------------------
0   | RESERVED          | Reserved
1   | RESERVED          | Reserved
2   | RESERVED          | Reserved
...
31  | RESERVED          | Reserved
```

- **Validation**: Unknown flags in a file should trigger a version mismatch error (the file uses features this library doesn't understand)

#### `endianness` (u8)
- **Values**:
  - `1` = Little-endian
  - `2` = Big-endian
- **Purpose**: Explicit byte order declaration for cross-platform safety
- **Current requirement**: Only little-endian (`1`) is supported
- **Validation**: Reject with `InvalidFormat` if not `1`

#### `_padding1` ([u8; 3])
- **Purpose**: Alignment padding after `endianness`
- **Value**: Must be zero

### Count Fields

Unchanged from V1:
- `node_count`, `node_capacity`, `edge_count`, `edge_capacity`

### Offset Fields

Unchanged from V1:
- `string_table_offset`, `string_table_end`
- `property_arena_offset`, `arena_next_offset`
- `free_node_head`, `free_edge_head`
- `next_node_id`, `next_edge_id`

### Schema Fields

Unchanged from V1:
- `schema_offset`, `schema_size`, `schema_version`, `_schema_reserved`

### Integrity Fields

#### `header_crc32` (u32)
- **Purpose**: Detects header corruption
- **Calculation**: CRC32 (IEEE polynomial) of bytes 0-151 (all header bytes before this field)
- **Validation**: Computed CRC must match stored value; reject with `CorruptedData` if not

### Reserved Fields

#### `_reserved` ([u8; 36])
- **Purpose**: Space for future header fields without requiring version bump
- **Value**: Must be zero-filled
- **Validation**: Should be ignored when reading (allows future minor additions)

---

## Rust Struct Definition

```rust
/// Header size for V2 format
pub const HEADER_SIZE_V2: usize = 192;

/// Current file format version
pub const VERSION: u32 = 2;

/// Minimum version this library can read
pub const MIN_READABLE_VERSION: u32 = 1;

/// Endianness indicator: little-endian
pub const ENDIAN_LITTLE: u8 = 1;

/// Endianness indicator: big-endian
pub const ENDIAN_BIG: u8 = 2;

/// Default page size
pub const DEFAULT_PAGE_SIZE: u32 = 4096;

/// File header at offset 0 (192 bytes)
#[repr(C, packed)]
#[derive(Copy, Clone, Debug)]
pub struct FileHeader {
    // Identity (12 bytes)
    /// Magic number (must be 0x47524D4C "GRML")
    pub magic: u32,
    /// File format version
    pub version: u32,
    /// Minimum library version required to read this file
    pub min_reader_version: u32,

    // Configuration (8 bytes)
    /// Page size in bytes
    pub page_size: u32,
    /// Feature flags bitfield
    pub flags: u32,

    // Endianness (4 bytes with padding)
    /// Byte order: 1 = little-endian, 2 = big-endian
    pub endianness: u8,
    /// Alignment padding (must be zero)
    pub _padding1: [u8; 3],

    // Counts (32 bytes)
    /// Number of active (non-deleted) nodes
    pub node_count: u64,
    /// Total allocated slots in node table
    pub node_capacity: u64,
    /// Number of active (non-deleted) edges
    pub edge_count: u64,
    /// Total allocated slots in edge table
    pub edge_capacity: u64,

    // Offsets (64 bytes)
    /// Byte offset to start of string table
    pub string_table_offset: u64,
    /// Byte offset to end of string table data
    pub string_table_end: u64,
    /// Byte offset to start of property arena
    pub property_arena_offset: u64,
    /// Current write position in property arena
    pub arena_next_offset: u64,
    /// First free node slot ID (u64::MAX if empty)
    pub free_node_head: u64,
    /// First free edge slot ID (u64::MAX if empty)
    pub free_edge_head: u64,
    /// Next node ID to allocate
    pub next_node_id: u64,
    /// Next edge ID to allocate
    pub next_edge_id: u64,

    // Schema (32 bytes)
    /// Byte offset to schema region (0 = no schema)
    pub schema_offset: u64,
    /// Size of schema data in bytes
    pub schema_size: u64,
    /// Schema format version
    pub schema_version: u32,
    /// Reserved for future schema fields
    pub _schema_reserved: [u8; 12],

    // Integrity (4 bytes)
    /// CRC32 of bytes 0-151
    pub header_crc32: u32,

    // Reserved (36 bytes)
    /// Reserved for future use (must be zero)
    pub _reserved: [u8; 36],
}
```

---

## Error Handling

### New Error Variant

Add a dedicated error for version mismatches:

```rust
#[derive(Debug, Error)]
pub enum StorageError {
    // ... existing variants ...

    /// The file version is incompatible with this library.
    ///
    /// This occurs when opening a database created with a different
    /// version of Interstellar that uses an incompatible layout.
    ///
    /// # Fields
    ///
    /// - `file_version`: Version found in the file header
    /// - `min_supported`: Minimum version this library can read
    /// - `max_supported`: Maximum version this library can read (current version)
    ///
    /// # Recovery
    ///
    /// - If `file_version > max_supported`: Upgrade the library
    /// - If `file_version < min_supported`: Use migration tools or older library
    #[error(
        "version mismatch: file version {file_version}, \
         library supports {min_supported}-{max_supported}"
    )]
    VersionMismatch {
        file_version: u32,
        min_supported: u32,
        max_supported: u32,
    },
}
```

### Validation Logic

```rust
fn validate_header(header: &FileHeader) -> Result<(), StorageError> {
    // 1. Check magic number
    if header.magic != MAGIC {
        return Err(StorageError::InvalidFormat);
    }

    // 2. Check version compatibility
    if header.version < MIN_READABLE_VERSION || header.version > VERSION {
        return Err(StorageError::VersionMismatch {
            file_version: header.version,
            min_supported: MIN_READABLE_VERSION,
            max_supported: VERSION,
        });
    }

    // 3. Check min_reader_version (for forward compat)
    if header.min_reader_version > VERSION {
        return Err(StorageError::VersionMismatch {
            file_version: header.version,
            min_supported: MIN_READABLE_VERSION,
            max_supported: VERSION,
        });
    }

    // 4. Check endianness
    if header.endianness != ENDIAN_LITTLE {
        return Err(StorageError::InvalidFormat);
    }

    // 5. Check page size validity
    if !header.page_size.is_power_of_two()
        || header.page_size < 512
        || header.page_size > 65536
    {
        return Err(StorageError::InvalidFormat);
    }

    // 6. Verify header CRC (V2+ only)
    if header.version >= 2 {
        let computed_crc = compute_header_crc(header);
        if computed_crc != header.header_crc32 {
            return Err(StorageError::CorruptedData);
        }
    }

    // 7. Check for unknown flags
    let known_flags: u32 = 0; // No flags defined yet
    if header.flags & !known_flags != 0 {
        return Err(StorageError::VersionMismatch {
            file_version: header.version,
            min_supported: MIN_READABLE_VERSION,
            max_supported: VERSION,
        });
    }

    Ok(())
}
```

---

## V1 Backward Compatibility

When reading a V1 file (136 bytes):

1. Detect V1 by `header.version == 1`
2. Synthesize missing V2 fields with defaults:
   - `min_reader_version = 1`
   - `page_size = 4096`
   - `flags = 0`
   - `endianness = 1` (little-endian)
   - `header_crc32` = skip validation
   - `_reserved` = zeroes

```rust
fn read_header(mmap: &[u8]) -> Result<FileHeader, StorageError> {
    if mmap.len() < HEADER_SIZE_V1 {
        return Err(StorageError::InvalidFormat);
    }

    let version = u32::from_le_bytes(mmap[4..8].try_into().unwrap());

    match version {
        1 => {
            let v1 = FileHeaderV1::from_bytes(&mmap[..HEADER_SIZE_V1]);
            Ok(FileHeader::from_v1(v1))
        }
        2 => {
            if mmap.len() < HEADER_SIZE_V2 {
                return Err(StorageError::InvalidFormat);
            }
            Ok(FileHeader::from_bytes(&mmap[..HEADER_SIZE_V2]))
        }
        _ => Err(StorageError::VersionMismatch {
            file_version: version,
            min_supported: MIN_READABLE_VERSION,
            max_supported: VERSION,
        }),
    }
}
```

---

## CRC32 Calculation

```rust
use crc32fast::Hasher;

fn compute_header_crc(header: &FileHeader) -> u32 {
    let bytes = header.to_bytes();
    // CRC covers bytes 0-151 (excludes header_crc32 and _reserved)
    let crc_range = &bytes[0..152];

    let mut hasher = Hasher::new();
    hasher.update(crc_range);
    hasher.finalize()
}
```

**Note**: Requires adding `crc32fast` dependency.

---

## Migration Path

### V1 → V2 Migration

Migration is a no-op for the header itself:
1. Read V1 header
2. Extend file by 56 bytes (192 - 136)
3. Write V2 header with synthesized defaults
4. Compute and write CRC32

The rest of the file format (node records, edge records, property arena) remains unchanged.

---

## Summary of Changes from V1

| Change | Bytes Added | Purpose |
|--------|-------------|---------|
| `min_reader_version` | 4 | Forward compatibility |
| `page_size` | 4 | Configuration |
| `flags` | 4 | Feature capabilities |
| `endianness` + padding | 4 | Cross-platform safety |
| `header_crc32` | 4 | Integrity verification |
| `_reserved` expansion | 36 | Future headroom |
| **Total** | **56** | 136 → 192 bytes |

---

## Implementation Checklist

- [ ] Add `VersionMismatch` error variant to `StorageError`
- [ ] Define `FileHeader` V2 struct in `records.rs`
- [ ] Keep `FileHeaderV1` for backward compatibility
- [ ] Add `crc32fast` dependency
- [ ] Update `validate_header()` with new logic
- [ ] Implement `FileHeader::from_v1()` conversion
- [ ] Update `MmapGraph::create()` to write V2 headers
- [ ] Update `MmapGraph::open()` to handle both versions
- [ ] Add tests for V1 → V2 reading
- [ ] Add tests for CRC validation
- [ ] Add tests for version mismatch errors
- [ ] Update `HEADER_SIZE` constant references
