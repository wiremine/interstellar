//! Query storage layer for mmap-backed persistent queries.
//!
//! This module provides the low-level storage operations for saving,
//! loading, and managing queries in the memory-mapped file format.

use hashbrown::HashMap;

use crate::error::QueryError;
use crate::query::{ParameterType, QueryParameter, QueryType, SavedQuery};

use super::records::{
    ParameterEntry, QueryRecord, QueryRegionHeader, PARAMETER_ENTRY_HEADER_SIZE,
    QUERY_RECORD_HEADER_SIZE, QUERY_REGION_HEADER_SIZE, QUERY_TYPE_GQL, QUERY_TYPE_GREMLIN,
};

// =============================================================================
// Constants
// =============================================================================

/// Default initial size for the query region (16KB)
pub const DEFAULT_QUERY_REGION_SIZE: u64 = 16 * 1024;

/// Minimum growth increment for the query region (16KB)
pub const MIN_QUERY_REGION_GROWTH: u64 = 16 * 1024;

// =============================================================================
// QueryStore
// =============================================================================

/// Query storage helper for serialization and deserialization.
///
/// This struct provides methods for converting between `SavedQuery` and
/// the on-disk byte format.
pub struct QueryStore;

impl QueryStore {
    /// Calculate the total record size for a query.
    ///
    /// The record size includes:
    /// - Fixed header (36 bytes)
    /// - Name (variable)
    /// - Description (variable)
    /// - Query text (variable)
    /// - Parameters (4 bytes header + name per parameter)
    pub fn calculate_record_size(
        name: &str,
        description: &str,
        query: &str,
        parameters: &[QueryParameter],
    ) -> u32 {
        let mut size = QUERY_RECORD_HEADER_SIZE;
        size += name.len();
        size += description.len();
        size += query.len();

        for param in parameters {
            size += PARAMETER_ENTRY_HEADER_SIZE + param.name.len();
        }

        size as u32
    }

    /// Serialize a query to bytes.
    ///
    /// Returns a byte vector containing the complete record (header + variable data).
    ///
    /// # Layout
    ///
    /// ```text
    /// [QueryRecord header - 36 bytes]
    /// [name - name_len bytes]
    /// [description - description_len bytes]
    /// [query - query_len bytes]
    /// [param1: ParameterEntry header + name bytes]
    /// [param2: ParameterEntry header + name bytes]
    /// ...
    /// ```
    pub fn serialize_query(
        id: u32,
        query_type: QueryType,
        name: &str,
        description: &str,
        query: &str,
        parameters: &[QueryParameter],
    ) -> Vec<u8> {
        let record_size = Self::calculate_record_size(name, description, query, parameters);

        // Create the header
        let type_flag = match query_type {
            QueryType::Gremlin => QUERY_TYPE_GREMLIN,
            QueryType::Gql => QUERY_TYPE_GQL,
        };

        let record = QueryRecord::new(
            id,
            type_flag,
            parameters.len() as u16,
            name.len() as u16,
            description.len() as u16,
            query.len() as u32,
            record_size,
        );

        let mut data = Vec::with_capacity(record_size as usize);

        // Write header
        data.extend_from_slice(&record.to_bytes());

        // Write name
        data.extend_from_slice(name.as_bytes());

        // Write description
        data.extend_from_slice(description.as_bytes());

        // Write query text
        data.extend_from_slice(query.as_bytes());

        // Write parameters
        for param in parameters {
            let entry =
                ParameterEntry::new(param.name.len() as u16, param.param_type.to_discriminant());
            data.extend_from_slice(&entry.to_bytes());
            data.extend_from_slice(param.name.as_bytes());
        }

        data
    }

    /// Deserialize a query from bytes.
    ///
    /// # Arguments
    ///
    /// * `data` - Byte slice starting at the query record
    ///
    /// # Returns
    ///
    /// The deserialized `SavedQuery` or an error if the data is malformed.
    pub fn deserialize_query(data: &[u8]) -> Result<SavedQuery, QueryError> {
        if data.len() < QUERY_RECORD_HEADER_SIZE {
            return Err(QueryError::Storage(
                crate::error::StorageError::CorruptedData,
            ));
        }

        // Read header
        let record = QueryRecord::from_bytes(data);

        // Extract header fields (copy to avoid unaligned access issues)
        let id = record.id;
        let query_type_flag = record.query_type();
        let param_count = record.param_count;
        let name_len = record.name_len as usize;
        let description_len = record.description_len as usize;
        let query_len = record.query_len as usize;
        let record_size = record.record_size as usize;

        // Validate record size
        if data.len() < record_size {
            return Err(QueryError::Storage(
                crate::error::StorageError::CorruptedData,
            ));
        }

        // Parse query type
        let query_type = match query_type_flag {
            QUERY_TYPE_GREMLIN => QueryType::Gremlin,
            QUERY_TYPE_GQL => QueryType::Gql,
            _ => {
                return Err(QueryError::Storage(
                    crate::error::StorageError::CorruptedData,
                ));
            }
        };

        // Read variable data
        let mut offset = QUERY_RECORD_HEADER_SIZE;

        // Read name
        let name = std::str::from_utf8(&data[offset..offset + name_len])
            .map_err(|_| QueryError::Storage(crate::error::StorageError::CorruptedData))?
            .to_string();
        offset += name_len;

        // Read description
        let description = std::str::from_utf8(&data[offset..offset + description_len])
            .map_err(|_| QueryError::Storage(crate::error::StorageError::CorruptedData))?
            .to_string();
        offset += description_len;

        // Read query text
        let query = std::str::from_utf8(&data[offset..offset + query_len])
            .map_err(|_| QueryError::Storage(crate::error::StorageError::CorruptedData))?
            .to_string();
        offset += query_len;

        // Read parameters
        let mut parameters = Vec::with_capacity(param_count as usize);
        for _ in 0..param_count {
            if offset + PARAMETER_ENTRY_HEADER_SIZE > data.len() {
                return Err(QueryError::Storage(
                    crate::error::StorageError::CorruptedData,
                ));
            }

            let entry = ParameterEntry::from_bytes(&data[offset..]);
            let param_name_len = entry.name_len as usize;
            let param_type = ParameterType::from_discriminant(entry.value_type);

            offset += PARAMETER_ENTRY_HEADER_SIZE;

            if offset + param_name_len > data.len() {
                return Err(QueryError::Storage(
                    crate::error::StorageError::CorruptedData,
                ));
            }

            let param_name = std::str::from_utf8(&data[offset..offset + param_name_len])
                .map_err(|_| QueryError::Storage(crate::error::StorageError::CorruptedData))?
                .to_string();
            offset += param_name_len;

            parameters.push(QueryParameter {
                name: param_name,
                param_type,
            });
        }

        Ok(SavedQuery {
            id,
            name,
            query_type,
            description,
            query,
            parameters,
        })
    }

    /// Initialize a new query region header.
    ///
    /// Returns the bytes for an empty query region header.
    pub fn create_region_header() -> [u8; QUERY_REGION_HEADER_SIZE] {
        let header = QueryRegionHeader::new();
        header.to_bytes()
    }

    /// Read the query region header from bytes.
    ///
    /// Returns `None` if the magic number doesn't match.
    pub fn read_region_header(data: &[u8]) -> Option<QueryRegionHeader> {
        if data.len() < QUERY_REGION_HEADER_SIZE {
            return None;
        }

        let header = QueryRegionHeader::from_bytes(data);
        if header.is_valid() {
            Some(header)
        } else {
            None
        }
    }
}

/// In-memory index for query lookups.
///
/// Maps query names to their IDs for O(1) name-based lookups.
#[derive(Debug, Default)]
pub struct QueryIndex {
    /// Name to ID mapping
    name_to_id: HashMap<String, u32>,
    /// ID to disk offset mapping
    id_to_offset: HashMap<u32, u64>,
}

impl QueryIndex {
    /// Create a new empty query index.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a query into the index.
    pub fn insert(&mut self, name: String, id: u32, offset: u64) {
        self.name_to_id.insert(name, id);
        self.id_to_offset.insert(id, offset);
    }

    /// Remove a query from the index by name.
    pub fn remove(&mut self, name: &str) -> Option<u32> {
        if let Some(id) = self.name_to_id.remove(name) {
            self.id_to_offset.remove(&id);
            Some(id)
        } else {
            None
        }
    }

    /// Look up a query ID by name.
    pub fn get_id(&self, name: &str) -> Option<u32> {
        self.name_to_id.get(name).copied()
    }

    /// Look up a query offset by ID.
    pub fn get_offset(&self, id: u32) -> Option<u64> {
        self.id_to_offset.get(&id).copied()
    }

    /// Look up a query offset by name.
    pub fn get_offset_by_name(&self, name: &str) -> Option<u64> {
        self.get_id(name).and_then(|id| self.get_offset(id))
    }

    /// Check if a query name exists.
    pub fn contains_name(&self, name: &str) -> bool {
        self.name_to_id.contains_key(name)
    }

    /// Check if a query ID exists.
    pub fn contains_id(&self, id: u32) -> bool {
        self.id_to_offset.contains_key(&id)
    }

    /// Get the number of indexed queries.
    pub fn len(&self) -> usize {
        self.name_to_id.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.name_to_id.is_empty()
    }

    /// Clear the index.
    pub fn clear(&mut self) {
        self.name_to_id.clear();
        self.id_to_offset.clear();
    }

    /// Iterate over all (name, id) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &u32)> {
        self.name_to_id.iter()
    }

    /// Get all query offsets for iteration.
    pub fn offsets(&self) -> impl Iterator<Item = u64> + '_ {
        self.id_to_offset.values().copied()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_record_size_no_params() {
        let size = QueryStore::calculate_record_size(
            "test_query",    // 10 bytes
            "A test query",  // 12 bytes
            "g.V().count()", // 13 bytes
            &[],
        );

        // 36 (header) + 10 (name) + 12 (desc) + 13 (query) = 71
        assert_eq!(size, 71);
    }

    #[test]
    fn test_calculate_record_size_with_params() {
        let params = vec![
            QueryParameter::new("name", ParameterType::String), // 4 + 4 = 8
            QueryParameter::new("age", ParameterType::Integer), // 4 + 3 = 7
        ];

        let size = QueryStore::calculate_record_size(
            "query", // 5 bytes
            "desc",  // 4 bytes
            "g.V()", // 5 bytes
            &params,
        );

        // 36 (header) + 5 (name) + 4 (desc) + 5 (query) + 8 + 7 = 65
        assert_eq!(size, 65);
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let params = vec![
            QueryParameter::new("name", ParameterType::String),
            QueryParameter::new("count", ParameterType::Integer),
        ];

        let data = QueryStore::serialize_query(
            42,
            QueryType::Gremlin,
            "find_person",
            "Find a person by name",
            "g.V().has('person', 'name', $name).limit($count)",
            &params,
        );

        let query = QueryStore::deserialize_query(&data).unwrap();

        assert_eq!(query.id, 42);
        assert_eq!(query.name, "find_person");
        assert_eq!(query.query_type, QueryType::Gremlin);
        assert_eq!(query.description, "Find a person by name");
        assert_eq!(
            query.query,
            "g.V().has('person', 'name', $name).limit($count)"
        );
        assert_eq!(query.parameters.len(), 2);
        assert_eq!(query.parameters[0].name, "name");
        assert_eq!(query.parameters[0].param_type, ParameterType::String);
        assert_eq!(query.parameters[1].name, "count");
        assert_eq!(query.parameters[1].param_type, ParameterType::Integer);
    }

    #[test]
    fn test_serialize_deserialize_gql() {
        let params = vec![QueryParameter::new("user_id", ParameterType::VertexId)];

        let data = QueryStore::serialize_query(
            1,
            QueryType::Gql,
            "user_posts",
            "Get posts by user",
            "MATCH (u:User {id: $user_id})-[:POSTED]->(p:Post) RETURN p",
            &params,
        );

        let query = QueryStore::deserialize_query(&data).unwrap();

        assert_eq!(query.id, 1);
        assert_eq!(query.query_type, QueryType::Gql);
        assert_eq!(query.parameters[0].param_type, ParameterType::VertexId);
    }

    #[test]
    fn test_serialize_deserialize_no_params() {
        let data = QueryStore::serialize_query(
            100,
            QueryType::Gremlin,
            "count_all",
            "Count all vertices",
            "g.V().count()",
            &[],
        );

        let query = QueryStore::deserialize_query(&data).unwrap();

        assert_eq!(query.id, 100);
        assert_eq!(query.name, "count_all");
        assert!(query.parameters.is_empty());
    }

    #[test]
    fn test_region_header_creation() {
        let header_bytes = QueryStore::create_region_header();
        assert_eq!(header_bytes.len(), QUERY_REGION_HEADER_SIZE);

        let header = QueryStore::read_region_header(&header_bytes).unwrap();
        assert!(header.is_valid());
        // Copy packed field to avoid unaligned access
        let first_query = { header.first_query };
        assert_eq!(first_query, u64::MAX);
    }

    #[test]
    fn test_region_header_invalid() {
        let bad_data = [0u8; QUERY_REGION_HEADER_SIZE];
        let result = QueryStore::read_region_header(&bad_data);
        assert!(result.is_none());
    }

    #[test]
    fn test_query_index_basic() {
        let mut index = QueryIndex::new();

        index.insert("query1".to_string(), 1, 1000);
        index.insert("query2".to_string(), 2, 2000);

        assert_eq!(index.len(), 2);
        assert!(index.contains_name("query1"));
        assert!(index.contains_id(1));
        assert_eq!(index.get_id("query1"), Some(1));
        assert_eq!(index.get_offset(1), Some(1000));
        assert_eq!(index.get_offset_by_name("query2"), Some(2000));
    }

    #[test]
    fn test_query_index_remove() {
        let mut index = QueryIndex::new();

        index.insert("query1".to_string(), 1, 1000);
        assert_eq!(index.len(), 1);

        let removed_id = index.remove("query1");
        assert_eq!(removed_id, Some(1));
        assert!(index.is_empty());
        assert!(!index.contains_name("query1"));
        assert!(!index.contains_id(1));
    }

    #[test]
    fn test_query_index_remove_nonexistent() {
        let mut index = QueryIndex::new();
        let result = index.remove("nonexistent");
        assert_eq!(result, None);
    }

    #[test]
    fn test_query_index_clear() {
        let mut index = QueryIndex::new();
        index.insert("q1".to_string(), 1, 100);
        index.insert("q2".to_string(), 2, 200);

        index.clear();

        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_deserialize_truncated_data() {
        // Too short for header
        let short_data = [0u8; 10];
        let result = QueryStore::deserialize_query(&short_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_empty_strings() {
        let data = QueryStore::serialize_query(0, QueryType::Gremlin, "", "", "g.V()", &[]);

        let query = QueryStore::deserialize_query(&data).unwrap();
        assert_eq!(query.name, "");
        assert_eq!(query.description, "");
        assert_eq!(query.query, "g.V()");
    }

    #[test]
    fn test_serialize_unicode() {
        let data = QueryStore::serialize_query(
            1,
            QueryType::Gremlin,
            "test_unicode",
            "Description with unicode: \u{1F600} emoji",
            "g.V().has('name', '\u{4E2D}\u{6587}')",
            &[QueryParameter::new(
                "\u{0391}\u{0392}",
                ParameterType::String,
            )],
        );

        let query = QueryStore::deserialize_query(&data).unwrap();
        assert!(query.description.contains('\u{1F600}'));
        assert!(query.query.contains("\u{4E2D}\u{6587}"));
        assert_eq!(query.parameters[0].name, "\u{0391}\u{0392}");
    }
}
