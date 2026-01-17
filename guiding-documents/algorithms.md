# Interstellar: Core Algorithms

This document details the fundamental algorithms powering the Interstellar graph traversal library. Interstellar supports dual storage backends (in-memory and memory-mapped), both sharing the same traversal execution engine.

---

## 1. Traversal Execution Engine

### 1.1 Iterator-Based Lazy Evaluation

The traversal engine uses a pull-based iterator model where each step is a lazy transformer. No computation occurs until a terminal step requests results.

```
┌─────────────────────────────────────────────────────────────────┐
│                   Traversal Pipeline                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Source          Step 1         Step 2         Terminal        │
│  ┌───────┐      ┌───────┐      ┌───────┐      ┌───────┐        │
│  │  V()  │─────▶│ has() │─────▶│ out() │─────▶│toList │        │
│  └───────┘      └───────┘      └───────┘      └───────┘        │
│      │              │              │              │             │
│      ▼              ▼              ▼              ▼             │
│   Iterator      Iterator       Iterator       Collect          │
│   <Vertex>      <Vertex>       <Vertex>       Vec<V>           │
│                                                                 │
│  Pull direction: ◀─────────────────────────────────────────    │
│  Data flow:      ─────────────────────────────────────────▶    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```rust
/// Core trait for traversal steps
pub trait Step<In, Out>: Sized {
    type Iter: Iterator<Item = Out>;
    
    fn apply(self, input: impl Iterator<Item = In>) -> Self::Iter;
}

/// Composed traversal - chains steps via nested iterators
pub struct ComposedTraversal<S1, S2, A, B, C>
where
    S1: Step<A, B>,
    S2: Step<B, C>,
{
    step1: S1,
    step2: S2,
    _phantom: PhantomData<(A, B, C)>,
}

impl<S1, S2, A, B, C> Step<A, C> for ComposedTraversal<S1, S2, A, B, C>
where
    S1: Step<A, B>,
    S2: Step<B, C>,
{
    type Iter = S2::Iter;
    
    fn apply(self, input: impl Iterator<Item = A>) -> Self::Iter {
        let intermediate = self.step1.apply(input);
        self.step2.apply(intermediate)
    }
}
```

### 1.2 Traverser State Machine

Each element flowing through the pipeline carries metadata via a `Traverser`:

```rust
/// Traverser carries element + metadata through pipeline
#[derive(Clone)]
pub struct Traverser<E> {
    element: E,
    path: Path,
    loops: u32,
    sack: Option<Box<dyn Any>>,
    bulk: u64,  // Optimization: represents N identical traversers
}

/// Path tracks traversal history
#[derive(Clone, Default)]
pub struct Path {
    objects: Vec<PathElement>,
    labels: HashMap<String, Vec<usize>>,  // label → indices
}

#[derive(Clone)]
pub struct PathElement {
    value: Value,
    labels: SmallVec<[String; 2]>,
}

impl<E> Traverser<E> {
    /// Split traverser for branching (preserves path)
    pub fn split<F>(&self, new_element: F) -> Traverser<F> {
        Traverser {
            element: new_element,
            path: self.path.clone(),
            loops: self.loops,
            sack: self.sack.clone(),
            bulk: self.bulk,
        }
    }
    
    /// Extend path with current element
    pub fn extend_path(&mut self, labels: &[String]) {
        self.path.push(self.element.clone().into(), labels);
    }
}
```

### 1.3 Bulk Optimization

When multiple identical traversers exist, we collapse them:

```rust
/// Bulk-aware deduplication
fn bulk_dedup<E: Eq + Hash>(
    traversers: impl Iterator<Item = Traverser<E>>
) -> impl Iterator<Item = Traverser<E>> {
    let mut seen: HashMap<E, Traverser<E>> = HashMap::new();
    
    for t in traversers {
        match seen.entry(t.element.clone()) {
            Entry::Occupied(mut e) => {
                e.get_mut().bulk += t.bulk;
            }
            Entry::Vacant(e) => {
                e.insert(t);
            }
        }
    }
    
    seen.into_values()
}
```

---

## 2. Graph Storage Algorithms

### 2.1 Memory-Mapped File Layout

```
┌─────────────────────────────────────────────────────────────────┐
│                    File Layout Algorithm                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Offset 0                                                       │
│  ┌──────────────────────────────────────┐                      │
│  │            Header (64 bytes)          │                      │
│  │  ┌────────────────────────────────┐  │                      │
│  │  │ magic: 0x47524D4C ("GRML")     │  │                      │
│  │  │ version: 1                      │  │                      │
│  │  │ node_count: u64                 │  │                      │
│  │  │ edge_count: u64                 │  │                      │
│  │  │ node_capacity: u64              │  │                      │
│  │  │ edge_capacity: u64              │  │                      │
│  │  │ string_table_offset: u64        │  │                      │
│  │  │ free_list_head: u64             │  │                      │
│  │  └────────────────────────────────┘  │                      │
│  └──────────────────────────────────────┘                      │
│                                                                 │
│  Offset 64: Node Array                                          │
│  ┌──────────────────────────────────────┐                      │
│  │ NodeRecord[0] │ NodeRecord[1] │ ...  │                      │
│  │    48 bytes   │    48 bytes   │      │                      │
│  └──────────────────────────────────────┘                      │
│  └─── node_capacity × 48 bytes ─────────┘                      │
│                                                                 │
│  Offset 64 + (node_capacity × 48): Edge Array                   │
│  ┌──────────────────────────────────────┐                      │
│  │ EdgeRecord[0] │ EdgeRecord[1] │ ...  │                      │
│  │    56 bytes   │    56 bytes   │      │                      │
│  └──────────────────────────────────────┘                      │
│                                                                 │
│  Property Arena: Variable-length allocations                    │
│  String Table: Interned strings                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Node/Edge Lookup: O(1)

```rust
impl Storage {
    const HEADER_SIZE: u64 = 64;
    const NODE_RECORD_SIZE: u64 = 48;
    const EDGE_RECORD_SIZE: u64 = 56;
    
    /// O(1) node lookup by ID
    #[inline]
    pub fn get_node(&self, id: VertexId) -> Option<&NodeRecord> {
        if id.0 >= self.header().node_count {
            return None;
        }
        
        let offset = Self::HEADER_SIZE + (id.0 * Self::NODE_RECORD_SIZE);
        let ptr = unsafe { self.mmap.as_ptr().add(offset as usize) };
        let record = unsafe { &*(ptr as *const NodeRecord) };
        
        // Check if slot is occupied (not in free list)
        if record.flags & FLAG_DELETED != 0 {
            return None;
        }
        
        Some(record)
    }
    
    /// O(1) edge lookup by ID
    #[inline]
    pub fn get_edge(&self, id: EdgeId) -> Option<&EdgeRecord> {
        if id.0 >= self.header().edge_count {
            return None;
        }
        
        let offset = self.edge_array_offset() + (id.0 * Self::EDGE_RECORD_SIZE);
        let ptr = unsafe { self.mmap.as_ptr().add(offset as usize) };
        let record = unsafe { &*(ptr as *const EdgeRecord) };
        
        if record.flags & FLAG_DELETED != 0 {
            return None;
        }
        
        Some(record)
    }
}
```

### 2.3 Adjacency List Traversal: O(degree)

Edges are stored in doubly-linked lists per vertex:

```
┌─────────────────────────────────────────────────────────────────┐
│              Adjacency List Structure                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  NodeRecord (Alice)                                             │
│  ┌─────────────────────────────────────────────────┐           │
│  │ first_out_edge ──────────────────────┐          │           │
│  │ first_in_edge ───────────────────────┼──┐       │           │
│  └─────────────────────────────────────────────────┘           │
│                                          │  │                   │
│         ┌────────────────────────────────┘  │                   │
│         ▼                                   │                   │
│  EdgeRecord (Alice→Bob)                     │                   │
│  ┌──────────────────────────┐               │                   │
│  │ src: Alice               │               │                   │
│  │ dst: Bob                 │               │                   │
│  │ next_out ────────────────┼───┐           │                   │
│  │ next_in: NULL            │   │           │                   │
│  └──────────────────────────┘   │           │                   │
│                                 │           │                   │
│         ┌───────────────────────┘           │                   │
│         ▼                                   │                   │
│  EdgeRecord (Alice→Carol)                   │                   │
│  ┌──────────────────────────┐               │                   │
│  │ src: Alice               │               │                   │
│  │ dst: Carol               │               │                   │
│  │ next_out: NULL           │               │                   │
│  │ next_in: ...             │               │                   │
│  └──────────────────────────┘               │                   │
│                                             │                   │
│         ┌───────────────────────────────────┘                   │
│         ▼                                                       │
│  EdgeRecord (Dan→Alice)  ◀── incoming edge to Alice             │
│  ┌──────────────────────────┐                                   │
│  │ src: Dan                 │                                   │
│  │ dst: Alice               │                                   │
│  │ next_in ─────────────────┼───▶ next incoming edge            │
│  └──────────────────────────┘                                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```rust
impl Storage {
    /// Iterate outgoing edges of a vertex
    pub fn out_edges(&self, vertex: VertexId) -> OutEdgeIter<'_> {
        let node = self.get_node(vertex);
        let first = node.map(|n| n.first_out_edge).unwrap_or(u64::MAX);
        
        OutEdgeIter {
            storage: self,
            current: first,
        }
    }
    
    /// Iterate incoming edges of a vertex
    pub fn in_edges(&self, vertex: VertexId) -> InEdgeIter<'_> {
        let node = self.get_node(vertex);
        let first = node.map(|n| n.first_in_edge).unwrap_or(u64::MAX);
        
        InEdgeIter {
            storage: self,
            current: first,
        }
    }
}

pub struct OutEdgeIter<'a> {
    storage: &'a Storage,
    current: u64,
}

impl<'a> Iterator for OutEdgeIter<'a> {
    type Item = &'a EdgeRecord;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == u64::MAX {
            return None;
        }
        
        let edge = self.storage.get_edge(EdgeId(self.current))?;
        self.current = edge.next_out;
        Some(edge)
    }
}
```

### 2.4 Edge Insertion Algorithm

```rust
impl StorageMut {
    /// Insert edge with O(1) complexity
    pub fn insert_edge(
        &mut self,
        src: VertexId,
        dst: VertexId,
        label_id: u32,
        properties: &[(u32, Value)],
    ) -> Result<EdgeId, StorageError> {
        // 1. Allocate edge record (from free list or append)
        let edge_id = self.allocate_edge_slot()?;
        
        // 2. Allocate properties in arena
        let prop_head = self.allocate_properties(properties)?;
        
        // 3. Get current first edges for src and dst
        let src_node = self.get_node_mut(src)?;
        let old_first_out = src_node.first_out_edge;
        
        let dst_node = self.get_node_mut(dst)?;
        let old_first_in = dst_node.first_in_edge;
        
        // 4. Create edge record
        let edge = EdgeRecord {
            id: edge_id.0,
            label_id,
            _padding: 0,
            src: src.0,
            dst: dst.0,
            next_out: old_first_out,  // Point to old first
            next_in: old_first_in,
            prop_head,
        };
        
        // 5. Write edge record
        self.write_edge(edge_id, &edge)?;
        
        // 6. Update vertex first-edge pointers (prepend to list)
        self.get_node_mut(src)?.first_out_edge = edge_id.0;
        self.get_node_mut(dst)?.first_in_edge = edge_id.0;
        
        // 7. Log to WAL for durability
        self.wal.log_edge_insert(edge_id, &edge)?;
        
        Ok(edge_id)
    }
}
```

### 2.5 Free List Management

Deleted nodes/edges are tracked in an embedded free list:

```rust
/// Free list node embedded in deleted records
#[repr(C)]
struct FreeListEntry {
    magic: u32,        // 0xDEADBEEF to identify free slot
    next_free: u64,    // Next free slot (or u64::MAX)
    _padding: [u8; N], // Fill to record size
}

impl StorageMut {
    /// Allocate slot from free list or extend array
    fn allocate_node_slot(&mut self) -> Result<VertexId, StorageError> {
        let header = self.header_mut();
        
        if header.free_node_head != u64::MAX {
            // Reuse from free list
            let slot = header.free_node_head;
            let free_entry = self.read_free_entry(slot);
            header.free_node_head = free_entry.next_free;
            Ok(VertexId(slot))
        } else if header.node_count < header.node_capacity {
            // Append to array
            let slot = header.node_count;
            header.node_count += 1;
            Ok(VertexId(slot))
        } else {
            // Need to grow file
            self.grow_node_array()?;
            self.allocate_node_slot()
        }
    }
    
    /// Return slot to free list
    fn deallocate_node(&mut self, id: VertexId) {
        let header = self.header_mut();
        let old_head = header.free_node_head;
        
        // Write free list entry in the slot
        let entry = FreeListEntry {
            magic: 0xDEADBEEF,
            next_free: old_head,
            _padding: [0; NODE_RECORD_SIZE - 12],
        };
        
        self.write_free_entry(id.0, &entry);
        header.free_node_head = id.0;
    }
}
```

---

## 3. Index Algorithms

### 3.1 Label Index: Hash-Based

```rust
/// Label → Element IDs mapping
pub struct LabelIndex {
    // label_id → RoaringBitmap of element IDs
    vertex_labels: HashMap<u32, RoaringBitmap>,
    edge_labels: HashMap<u32, RoaringBitmap>,
}

impl LabelIndex {
    /// O(1) average lookup
    pub fn vertices_with_label(&self, label_id: u32) -> impl Iterator<Item = VertexId> {
        self.vertex_labels
            .get(&label_id)
            .into_iter()
            .flat_map(|bitmap| bitmap.iter())
            .map(VertexId)
    }
    
    /// O(1) insertion
    pub fn add_vertex(&mut self, id: VertexId, label_id: u32) {
        self.vertex_labels
            .entry(label_id)
            .or_default()
            .insert(id.0 as u32);
    }
}
```

### 3.2 Property Index: B+ Tree

```
┌─────────────────────────────────────────────────────────────────┐
│                    B+ Tree Structure                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Index Key: (label_id, property_key_id, value_hash)            │
│                                                                 │
│                    ┌─────────────────┐                         │
│                    │   Root Node     │                         │
│                    │ [30] [60] [90]  │                         │
│                    └────┬───┬───┬────┘                         │
│              ┌──────────┘   │   └──────────┐                   │
│              ▼              ▼              ▼                    │
│        ┌─────────┐   ┌─────────┐   ┌─────────┐                │
│        │ [10,20] │   │ [40,50] │   │ [70,80] │  Internal      │
│        └────┬────┘   └────┬────┘   └────┬────┘                │
│             │             │             │                      │
│      ┌──────┴──────┐     ...           ...                    │
│      ▼             ▼                                           │
│  ┌───────┐    ┌───────┐                                       │
│  │ Leaf  │───▶│ Leaf  │───▶ ...   Leaf nodes linked           │
│  │[5,10] │    │[15,20]│           for range scans             │
│  │IDs:.. │    │IDs:.. │                                       │
│  └───────┘    └───────┘                                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```rust
/// B+ Tree property index
pub struct PropertyIndex {
    root: Option<NodeId>,
    order: usize,  // Max children per node (typically 128-256)
    nodes: Vec<BTreeNode>,
}

#[derive(Clone)]
enum BTreeNode {
    Internal {
        keys: Vec<IndexKey>,
        children: Vec<NodeId>,
    },
    Leaf {
        keys: Vec<IndexKey>,
        values: Vec<RoaringBitmap>,  // Element IDs per key
        next_leaf: Option<NodeId>,
    },
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
struct IndexKey {
    label_id: u32,
    property_key_id: u32,
    value: ComparableValue,
}

impl PropertyIndex {
    /// Point lookup: O(log n)
    pub fn get(&self, key: &IndexKey) -> Option<&RoaringBitmap> {
        let mut node_id = self.root?;
        
        loop {
            match &self.nodes[node_id.0] {
                BTreeNode::Internal { keys, children } => {
                    // Binary search for child
                    let idx = keys.binary_search(key).unwrap_or_else(|i| i);
                    node_id = children[idx];
                }
                BTreeNode::Leaf { keys, values, .. } => {
                    // Binary search in leaf
                    return keys.binary_search(key)
                        .ok()
                        .map(|idx| &values[idx]);
                }
            }
        }
    }
    
    /// Range scan: O(log n + k) where k = results
    pub fn range(
        &self,
        start: &IndexKey,
        end: &IndexKey,
    ) -> impl Iterator<Item = (&IndexKey, &RoaringBitmap)> {
        // Find starting leaf
        let start_leaf = self.find_leaf(start);
        
        RangeScanIter {
            index: self,
            current_leaf: start_leaf,
            current_idx: 0,
            end_key: end.clone(),
        }
    }
    
    /// Insert: O(log n) amortized
    pub fn insert(&mut self, key: IndexKey, element_id: u64) {
        // ... standard B+ tree insertion with splits
    }
}
```

### 3.3 Composite Index

```rust
/// Multi-property composite index
pub struct CompositeIndex {
    /// Concatenated key: (label, key1_value, key2_value, ...)
    btree: PropertyIndex,
    key_order: Vec<u32>,  // Property key IDs in index order
}

impl CompositeIndex {
    /// Build composite key from element properties
    fn make_key(&self, label_id: u32, props: &Properties) -> Option<IndexKey> {
        let mut value_parts = Vec::with_capacity(self.key_order.len());
        
        for &key_id in &self.key_order {
            let value = props.get(key_id)?;
            value_parts.push(value.to_comparable());
        }
        
        Some(IndexKey {
            label_id,
            property_key_id: 0,  // Unused for composite
            value: ComparableValue::Composite(value_parts),
        })
    }
    
    /// Prefix scan for partial key matches
    pub fn prefix_scan(
        &self,
        label_id: u32,
        prefix_values: &[Value],
    ) -> impl Iterator<Item = u64> {
        let start = self.make_prefix_key(label_id, prefix_values, Bound::Start);
        let end = self.make_prefix_key(label_id, prefix_values, Bound::End);
        
        self.btree.range(&start, &end)
            .flat_map(|(_, ids)| ids.iter())
    }
}
```

---

## 4. Traversal Step Algorithms

### 4.1 Filter Steps

```rust
/// has(key, value) - Property filter
pub struct HasStep {
    key_id: u32,
    predicate: Box<dyn Predicate>,
}

impl<E: Element> Step<Traverser<E>, Traverser<E>> for HasStep {
    type Iter = std::iter::Filter<...>;
    
    fn apply(self, input: impl Iterator<Item = Traverser<E>>) -> Self::Iter {
        input.filter(move |t| {
            t.element
                .property(self.key_id)
                .map(|v| self.predicate.test(&v))
                .unwrap_or(false)
        })
    }
}

/// Index-accelerated has() when index exists
pub struct IndexedHasStep {
    index: Arc<PropertyIndex>,
    key: IndexKey,
}

impl Step<GraphSource, Traverser<Vertex>> for IndexedHasStep {
    type Iter = impl Iterator<Item = Traverser<Vertex>>;
    
    fn apply(self, _input: impl Iterator<Item = GraphSource>) -> Self::Iter {
        // Bypass full scan, go directly to index
        self.index
            .get(&self.key)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .map(|id| Traverser::new(Vertex::new(VertexId(id as u64))))
    }
}
```

### 4.2 Navigation Steps

```rust
/// out() - Traverse outgoing edges to adjacent vertices
/// Accepts optional label filters (supports both single and multiple labels)
pub struct OutStep {
    label_filters: Option<Vec<u32>>,  // Multiple label support
}

impl Step<Traverser<Vertex>, Traverser<Vertex>> for OutStep {
    type Iter = impl Iterator<Item = Traverser<Vertex>>;
    
    fn apply(self, input: impl Iterator<Item = Traverser<Vertex>>) -> Self::Iter {
        input.flat_map(move |t| {
            let vertex_id = t.element.id();
            let storage = t.element.storage();
            
            storage
                .out_edges(vertex_id)
                .filter(move |e| {
                    self.label_filters
                        .as_ref()
                        .map(|labels| labels.contains(&e.label_id))
                        .unwrap_or(true)
                })
                .map(move |e| {
                    let dst = storage.get_node(VertexId(e.dst)).unwrap();
                    t.split(Vertex::from_record(dst, storage))
                })
        })
    }
}

/// Bidirectional traversal with deduplication
pub struct BothStep {
    label_filters: Option<Vec<u32>>,  // Multiple label support
}

impl Step<Traverser<Vertex>, Traverser<Vertex>> for BothStep {
    type Iter = impl Iterator<Item = Traverser<Vertex>>;
    
    fn apply(self, input: impl Iterator<Item = Traverser<Vertex>>) -> Self::Iter {
        input.flat_map(move |t| {
            let vertex_id = t.element.id();
            let storage = t.element.storage();
            
            // Chain out and in edges, track seen to avoid duplicates
            let mut seen = HashSet::new();
            
            let out_neighbors = storage.out_edges(vertex_id)
                .filter(|e| self.matches_label(e))
                .filter_map(|e| {
                    if seen.insert(e.dst) {
                        Some(VertexId(e.dst))
                    } else {
                        None
                    }
                });
            
            let in_neighbors = storage.in_edges(vertex_id)
                .filter(|e| self.matches_label(e))
                .filter_map(|e| {
                    if seen.insert(e.src) {
                        Some(VertexId(e.src))
                    } else {
                        None
                    }
                });
            
            out_neighbors.chain(in_neighbors)
                .map(move |id| {
                    let node = storage.get_node(id).unwrap();
                    t.split(Vertex::from_record(node, storage))
                })
        })
    }
}
```

### 4.3 Repeat Step (Loop Execution)

```rust
/// repeat().until().emit() execution (frontier stays streamed, no full collect)
pub struct RepeatExecutor<S: Step<T, T>, T: Clone> {
    step: S,
    until_predicate: Option<Box<dyn Fn(&T) -> bool>>,
    emit_predicate: Option<Box<dyn Fn(&T) -> bool>>,
    max_loops: Option<u32>,
    emit_first: bool,
}

impl<S: Step<T, T>, T: Clone> RepeatExecutor<S, T> {
    pub fn execute(
        self,
        input: impl Iterator<Item = Traverser<T>>,
    ) -> impl Iterator<Item = Traverser<T>> {
        RepeatIter::new(self, input)
    }

    fn should_emit(&self, t: &Traverser<T>) -> bool {
        match &self.emit_predicate {
            Some(p) => p(&t.element),
            None => false,
        }
    }
    
    fn should_stop(&self, t: &Traverser<T>) -> bool {
        match &self.until_predicate {
            Some(p) => p(&t.element),
            None => false,
        }
    }
}

/// Streaming repeat iterator: processes one frontier at a time and yields as produced
pub struct RepeatIter<S, T, I>
where
    S: Step<T, T>,
    T: Clone,
    I: Iterator<Item = Traverser<T>>,
{
    exec: RepeatExecutor<S, T>,
    frontier: Vec<Traverser<T>>,
    next_frontier: Vec<Traverser<T>>,
    pending: std::collections::VecDeque<Traverser<T>>,
    loop_count: u32,
    upstream: Option<I>,
}

impl<S, T, I> RepeatIter<S, T, I>
where
    S: Step<T, T>,
    T: Clone,
    I: Iterator<Item = Traverser<T>>,
{
    fn new(exec: RepeatExecutor<S, T>, upstream: I) -> Self {
        Self {
            exec,
            frontier: Vec::new(),
            next_frontier: Vec::new(),
            pending: std::collections::VecDeque::new(),
            loop_count: 0,
            upstream: Some(upstream),
        }
    }
}

impl<S, T, I> Iterator for RepeatIter<S, T, I>
where
    S: Step<T, T>,
    T: Clone,
    I: Iterator<Item = Traverser<T>>,
{
    type Item = Traverser<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(t) = self.pending.pop_front() {
                return Some(t);
            }

            // Seed frontier once from upstream
            if let Some(upstream) = self.upstream.take() {
                self.frontier.extend(upstream);
            }

            if self.frontier.is_empty() {
                return None;
            }

            for mut traverser in self.frontier.drain(..) {
                traverser.loops = self.loop_count;

                if self.exec.should_emit(&traverser) {
                    self.pending.push_back(traverser.clone());
                }

                if self.exec.should_stop(&traverser) {
                    if !self.exec.should_emit(&traverser) {
                        self.pending.push_back(traverser);
                    }
                    continue;
                }

                let stepped = self.exec.step.clone().apply(std::iter::once(traverser));
                self.next_frontier.extend(stepped);
            }

            std::mem::swap(&mut self.frontier, &mut self.next_frontier);
            self.loop_count += 1;
        }
    }
}
```

### 4.4 Branch Steps

```rust
/// union() - Execute multiple traversals, merge results lazily
/// Traverser-major interleaving: for each input traverser, run
/// all branches in order and emit their results before moving
/// to the next traverser, preserving Gremlin ordering semantics.
pub struct UnionStep<T> {
    branches: Vec<Box<dyn Step<T, T>>>,
}

impl<T: Clone> Step<Traverser<T>, Traverser<T>> for UnionStep<T> {
    type Iter = impl Iterator<Item = Traverser<T>>;

    fn apply(self, input: impl Iterator<Item = Traverser<T>>) -> Self::Iter {
        let branches = self.branches;
        input.flat_map(move |t| {
            branches
                .iter()
                .cloned()
                .flat_map(move |branch| branch.apply(std::iter::once(t.split(t.element.clone()))))
        })
    }
}

/// Optional replay wrapper (only when a branch truly needs to
/// re-read the same upstream stream; default union streams per
/// traverser without buffering to preserve ordering and memory).
pub struct ReplayableIter<I, T>
where
    I: Iterator<Item = T>,
{
    shared: std::sync::Arc<std::sync::Mutex<ReplayState<T>>>,
    cursor: usize,
    _marker: std::marker::PhantomData<I>,
}

struct ReplayState<T> {
    upstream: Box<dyn Iterator<Item = T> + Send>,
    buffer: Vec<T>,
    finished: bool,
}

impl<I, T> ReplayableIter<I, T>
where
    I: Iterator<Item = T> + Send + 'static,
    T: Clone,
{
    pub fn new(iter: I) -> Self {
        Self {
            shared: std::sync::Arc::new(std::sync::Mutex::new(ReplayState {
                upstream: Box::new(iter),
                buffer: Vec::new(),
                finished: false,
            })),
            cursor: 0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<I, T> Clone for ReplayableIter<I, T>
where
    I: Iterator<Item = T> + Send + 'static,
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            cursor: 0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<I, T> Iterator for ReplayableIter<I, T>
where
    I: Iterator<Item = T> + Send + 'static,
    T: Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let mut state = self.shared.lock().unwrap();

        if self.cursor < state.buffer.len() {
            let item = state.buffer[self.cursor].clone();
            self.cursor += 1;
            return Some(item);
        }

        if state.finished {
            return None;
        }

        if let Some(item) = state.upstream.next() {
            state.buffer.push(item.clone());
            self.cursor += 1;
            return Some(item);
        }

        state.finished = true;
        None
    }
}

/// coalesce() - First non-empty traversal wins
pub struct CoalesceStep<T> {
    branches: Vec<Box<dyn Step<T, T>>>,
}

impl<T: Clone> Step<Traverser<T>, Traverser<T>> for CoalesceStep<T> {
    type Iter = impl Iterator<Item = Traverser<T>>;
    
    fn apply(self, input: impl Iterator<Item = Traverser<T>>) -> Self::Iter {
        input.flat_map(move |t| {
            for branch in &self.branches {
                let mut results = branch.clone()
                    .apply(std::iter::once(t.clone()))
                    .peekable();
                
                if results.peek().is_some() {
                    return Either::Left(results);
                }
            }
            Either::Right(std::iter::empty())
        })
    }
}

/// choose() - Conditional branching
pub struct ChooseStep<T, C: Fn(&T) -> bool> {
    condition: C,
    if_true: Box<dyn Step<T, T>>,
    if_false: Box<dyn Step<T, T>>,
}

impl<T, C: Fn(&T) -> bool> Step<Traverser<T>, Traverser<T>> for ChooseStep<T, C> {
    type Iter = impl Iterator<Item = Traverser<T>>;
    
    fn apply(self, input: impl Iterator<Item = Traverser<T>>) -> Self::Iter {
        input.flat_map(move |t| {
            if (self.condition)(&t.element) {
                Either::Left(self.if_true.apply(std::iter::once(t)))
            } else {
                Either::Right(self.if_false.apply(std::iter::once(t)))
            }
        })
    }
}
```

---

## 5. Query Optimization

### 5.1 Index Selection

```rust
/// Query planner selects optimal index
pub struct QueryPlanner<'g> {
    graph: &'g Graph,
    statistics: &'g Statistics,
}

impl<'g> QueryPlanner<'g> {
    pub fn plan_has_step(
        &self,
        label: Option<u32>,
        key: u32,
        predicate: &Predicate,
    ) -> HasStepPlan {
        // Check for exact match index
        if let Some(idx) = self.graph.property_index(label, key) {
            if predicate.is_equality() {
                let selectivity = self.estimate_selectivity(idx, predicate);
                if selectivity < 0.1 {  // Less than 10% of data
                    return HasStepPlan::UseIndex(idx.clone());
                }
            }
        }
        
        // Check for composite index with this as prefix
        if let Some(comp_idx) = self.graph.composite_index_with_prefix(label, key) {
            return HasStepPlan::UseCompositePrefix(comp_idx.clone());
        }
        
        // Fall back to label scan + filter
        if let Some(label_id) = label {
            let label_size = self.statistics.label_count(label_id);
            let total_size = self.statistics.total_vertices();
            
            if label_size < total_size / 10 {
                return HasStepPlan::LabelScanThenFilter(label_id);
            }
        }
        
        // Full scan
        HasStepPlan::FullScan
    }
}

enum HasStepPlan {
    UseIndex(Arc<PropertyIndex>),
    UseCompositePrefix(Arc<CompositeIndex>),
    LabelScanThenFilter(u32),
    FullScan,
}
```

### 5.2 Traversal Rewriting

```rust
/// Optimize traversal before execution
pub fn optimize(traversal: Traversal) -> Traversal {
    let mut steps = traversal.into_steps();
    
    // Rule 1: Push filters down (filter early)
    steps = push_filters_down(steps);
    
    // Rule 2: Combine adjacent has() steps
    steps = combine_has_steps(steps);
    
    // Rule 3: Eliminate redundant dedup()
    steps = eliminate_redundant_dedup(steps);
    
    // Rule 4: Convert to index lookups where possible
    steps = convert_to_index_lookups(steps);
    
    // Rule 5: Reorder for optimal join order
    steps = optimize_join_order(steps);
    
    Traversal::from_steps(steps)
}

/// Push has() steps as early as possible
fn push_filters_down(steps: Vec<Step>) -> Vec<Step> {
    let mut result = Vec::new();
    let mut pending_filters = Vec::new();
    
    for step in steps {
        match step {
            Step::Has(h) => pending_filters.push(h),
            Step::Out(o) | Step::In(i) | Step::Both(b) => {
                // Insert filters before navigation
                result.extend(pending_filters.drain(..).map(Step::Has));
                result.push(step);
            }
            _ => result.push(step),
        }
    }
    
    result.extend(pending_filters.into_iter().map(Step::Has));
    result
}
```

### 5.3 Cost Estimation

```rust
/// Statistics for cost-based optimization
pub struct Statistics {
    vertex_count: u64,
    edge_count: u64,
    label_counts: HashMap<u32, u64>,
    property_histograms: HashMap<(u32, u32), Histogram>,
    avg_out_degree: f64,
    avg_in_degree: f64,
}

impl Statistics {
    /// Estimate result cardinality for a step
    pub fn estimate_cardinality(&self, step: &Step, input_card: f64) -> f64 {
        match step {
            Step::Has(h) => {
                let selectivity = self.property_selectivity(h.key, &h.predicate);
                input_card * selectivity
            }
            Step::HasLabel(label) => {
                let label_frac = self.label_counts.get(label)
                    .map(|&c| c as f64 / self.vertex_count as f64)
                    .unwrap_or(0.01);
                input_card * label_frac
            }
            Step::Out(_) => input_card * self.avg_out_degree,
            Step::In(_) => input_card * self.avg_in_degree,
            Step::Both(_) => input_card * (self.avg_out_degree + self.avg_in_degree),
            Step::Dedup => input_card * 0.7,  // Heuristic
            Step::Limit(n) => (*n as f64).min(input_card),
            _ => input_card,
        }
    }
    
    /// Estimate cost (I/O operations) for a plan
    pub fn estimate_cost(&self, plan: &[Step]) -> f64 {
        let mut card = self.vertex_count as f64;
        let mut cost = 0.0;
        
        for step in plan {
            cost += self.step_cost(step, card);
            card = self.estimate_cardinality(step, card);
        }
        
        cost
    }
}
```

---

## 6. Concurrency Model (Phase 1)

### 6.1 Simple RwLock-Based Concurrency

Interstellar Phase 1 uses a simple reader-writer lock model for concurrent access:

```rust
/// Thread-safe graph handle
pub struct Graph {
    storage: Arc<dyn GraphStorage>,
    lock: Arc<RwLock<()>>,
}

/// Read-only snapshot for traversals
pub struct GraphSnapshot<'g> {
    graph: &'g Graph,
    _guard: RwLockReadGuard<'g, ()>,
}

/// Mutable transaction
pub struct GraphMut<'g> {
    graph: &'g Graph,
    write_buffer: WriteBuffer,
    _guard: RwLockWriteGuard<'g, ()>,
}

impl Graph {
    /// Get read-only snapshot (multiple readers allowed)
    pub fn traversal(&self) -> GraphSnapshot<'_> {
        GraphSnapshot {
            graph: self,
            _guard: self.lock.read(),
        }
    }
    
    /// Get mutable transaction (exclusive access)
    pub fn mutate(&self) -> GraphMut<'_> {
        GraphMut {
            graph: self,
            write_buffer: WriteBuffer::new(),
            _guard: self.lock.write(),
        }
    }
}
```

**Concurrency guarantees:**
- Multiple concurrent readers via `RwLock::read()`
- Single writer with buffered writes via `RwLock::write()`
- Atomic commits via WAL (for memory-mapped storage)
- Consistent snapshots during traversal execution

### 6.2 Future: MVCC (Phase 2+)

For improved concurrent read performance, a future version will implement Multi-Version Concurrency Control (MVCC):

```rust
/// Version-stamped record for MVCC (Future)
pub struct VersionedRecord<T> {
    data: T,
    created_at: u64,
    deleted_at: Option<u64>,
}

/// Snapshot sees consistent view at specific version (Future)
pub struct Snapshot {
    version: u64,
    visible_range: Range<u64>,
}
```

This will enable:
- Lock-free reads with snapshot isolation
- Better concurrent read performance
- Time-travel queries
- Garbage collection of old versions

See roadmap in [overview.md](./overview.md) for timeline.

### 6.3 Write-Ahead Log (WAL)

```rust
/// WAL entry types
#[derive(Serialize, Deserialize)]
enum WalEntry {
    BeginTx { tx_id: u64 },
    InsertNode { id: VertexId, record: NodeRecord },
    InsertEdge { id: EdgeId, record: EdgeRecord },
    UpdateProperty { element: ElementId, key: u32, old: Value, new: Value },
    DeleteNode { id: VertexId },
    DeleteEdge { id: EdgeId },
    CommitTx { tx_id: u64 },
    AbortTx { tx_id: u64 },
    Checkpoint { version: u64 },
}

pub struct WriteAheadLog {
    file: File,
    buffer: Vec<u8>,
    last_checkpoint: u64,
}

impl WriteAheadLog {
    /// Append entry with fsync for durability
    pub fn log(&mut self, entry: WalEntry) -> io::Result<u64> {
        let offset = self.file.seek(SeekFrom::End(0))?;
        
        // Serialize with length prefix
        self.buffer.clear();
        let len = bincode::serialized_size(&entry)? as u32;
        self.buffer.extend_from_slice(&len.to_le_bytes());
        bincode::serialize_into(&mut self.buffer, &entry)?;
        
        // CRC for integrity
        let crc = crc32fast::hash(&self.buffer);
        self.buffer.extend_from_slice(&crc.to_le_bytes());
        
        self.file.write_all(&self.buffer)?;
        self.file.sync_data()?;  // Ensure durability
        
        Ok(offset)
    }
    
    /// Recover from WAL after crash
    pub fn recover(&mut self, storage: &mut Storage) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        
        let mut pending_tx: HashMap<u64, Vec<WalEntry>> = HashMap::new();
        
        while let Some(entry) = self.read_entry()? {
            match entry {
                WalEntry::BeginTx { tx_id } => {
                    pending_tx.insert(tx_id, Vec::new());
                }
                WalEntry::CommitTx { tx_id } => {
                    // Apply all entries for this transaction
                    if let Some(entries) = pending_tx.remove(&tx_id) {
                        for e in entries {
                            self.apply_entry(storage, e)?;
                        }
                    }
                }
                WalEntry::AbortTx { tx_id } => {
                    pending_tx.remove(&tx_id);
                }
                other => {
                    // Buffer until commit
                    if let Some(tx_entries) = pending_tx.values_mut().next() {
                        tx_entries.push(other);
                    }
                }
            }
        }
        
        // Uncommitted transactions are discarded
        Ok(())
    }
}
```

---

## 7. Serialization & Portability

### 7.1 String Interning

```rust
/// Deduplicated string storage
pub struct StringInterner {
    string_to_id: HashMap<String, u32>,
    id_to_offset: Vec<u32>,
    arena: Vec<u8>,
}

impl StringInterner {
    /// Intern a string, returning its ID
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.string_to_id.get(s) {
            return id;
        }
        
        let id = self.id_to_offset.len() as u32;
        let offset = self.arena.len() as u32;
        
        // Write length-prefixed string
        let len = s.len() as u32;
        self.arena.extend_from_slice(&len.to_le_bytes());
        self.arena.extend_from_slice(s.as_bytes());
        
        self.id_to_offset.push(offset);
        self.string_to_id.insert(s.to_string(), id);
        
        id
    }
    
    /// Resolve ID to string
    pub fn resolve(&self, id: u32) -> Option<&str> {
        let offset = *self.id_to_offset.get(id as usize)? as usize;
        
        let len = u32::from_le_bytes(
            self.arena[offset..offset + 4].try_into().ok()?
        ) as usize;
        
        let bytes = &self.arena[offset + 4..offset + 4 + len];
        std::str::from_utf8(bytes).ok()
    }
}
```

### 7.2 Value Serialization

```rust
/// Compact binary encoding for property values
impl Value {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Value::Null => buf.push(0x00),
            Value::Bool(false) => buf.push(0x01),
            Value::Bool(true) => buf.push(0x02),
            Value::Int(n) => {
                buf.push(0x03);
                buf.extend_from_slice(&n.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(0x04);
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Value::String(s) => {
                buf.push(0x05);
                let len = s.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::List(items) => {
                buf.push(0x06);
                let len = items.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    item.serialize(buf);
                }
            }
            Value::Map(map) => {
                buf.push(0x07);
                let len = map.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                for (k, v) in map {
                    Value::String(k.clone()).serialize(buf);
                    v.serialize(buf);
                }
            }
        }
    }
    
    pub fn deserialize(buf: &[u8], pos: &mut usize) -> Option<Value> {
        let tag = *buf.get(*pos)?;
        *pos += 1;
        
        match tag {
            0x00 => Some(Value::Null),
            0x01 => Some(Value::Bool(false)),
            0x02 => Some(Value::Bool(true)),
            0x03 => {
                let n = i64::from_le_bytes(buf[*pos..*pos + 8].try_into().ok()?);
                *pos += 8;
                Some(Value::Int(n))
            }
            // ... other cases
            _ => None,
        }
    }
}
```

---

## 8. Complexity Summary

| Operation | Time Complexity | Space Complexity |
|-----------|-----------------|------------------|
| Vertex lookup by ID | O(1) | O(1) |
| Edge lookup by ID | O(1) | O(1) |
| Adjacency iteration | O(degree) | O(1) |
| Label scan | O(n) | O(1) |
| Property index lookup | O(log n) | O(1) |
| Property range scan | O(log n + k) | O(k) |
| Add vertex | O(1) amortized | O(1) |
| Add edge | O(1) | O(1) |
| Delete vertex | O(degree) | O(1) |
| Delete edge | O(1) | O(1) |
| BFS/DFS traversal | O(V + E) | O(V) |
| Shortest path | O(V + E) | O(V) |
| WAL append | O(1) | O(entry size) |
| Snapshot creation | O(1) | O(1) |
| String interning | O(1) avg | O(string length) |