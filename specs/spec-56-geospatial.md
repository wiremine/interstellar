# Spec 56: Geospatial Index & Predicates

## Overview

Add first-class **geospatial support** to Interstellar: two new geometry-valued
property types (`Point`, `Polygon`), a set of geospatial predicates
(`within_distance`, `intersects`, `contained_by`, `bbox`), and an in-memory
**R-tree index** (backed by [`rstar`](https://crates.io/crates/rstar)) that
turns those predicates into O(log n + k) lookups instead of O(n) scans.

This spec promotes Tier-2 item #10 from `specs/feature-proposals.md:115`
("Geospatial Index & Predicates") into an implementation-ready specification,
and is positioned to land as the next storage feature after spec-54
(vector index) and spec-55 (full-text search).

### Goals

1. **Two new `Value` variants** — `Point { lon, lat }` and `Polygon(ring)` —
   carried end-to-end through storage, traversal, GraphSON, GQL, Gremlin, and
   all language bindings.
2. **WGS84 only** (EPSG:4326). Distances are great-circle (haversine) in
   meters internally; user-facing API accepts a `Distance` enum.
3. **Predicate factories** in `traversal::p` reusing the existing
   `Predicate` trait — `p::within_distance`, `p::intersects`,
   `p::contained_by`, `p::bbox`. Composable with `has_where`,
   `and`/`or`/`not`, and existing predicates.
4. **R-tree spatial index** (`RTreeIndex`) registered through the existing
   `Box<dyn PropertyIndex>` mechanism in `cow.rs`, `cow_mmap.rs`, and
   `mmap/mod.rs`. In-memory only; the `mmap` backend rebuilds it on open.
5. **Index-aware filter planning** — when a `has_where` predicate is one of
   the geo predicates and an `RTreeIndex` exists for the property, the
   traversal planner uses the index instead of the scan path. Falls back to
   scan if no index is registered. Behavior is identical either way; only
   the cost changes.
6. **Gremlin grammar** — `point(lon, lat)`, `polygon([[lon,lat], …])`, plus
   geo predicate factories `geo_within_distance`, `geo_intersects`,
   `geo_contained_by`, `geo_bbox` (snake_case to match the existing `p::*`
   convention).
7. **GQL grammar** — Cypher-flavored `point({ longitude: …, latitude: … })`
   constructor, scalar function `point.distance(a, b)`, and an
   `IN <polygon>` membership operator.
8. **GraphSON 3.0 round-trip** using the existing `g:Point` and a new
   `is:Polygon` typed-value envelope (Interstellar namespace, since GraphSON
   has no standard Polygon).
9. **Bindings** — napi-rs and WASM expose `{ type: "Point", coordinates: [lon, lat] }`
   GeoJSON-style objects. CLI/REPL prints WKT (`POINT(-122.4 37.8)`).
10. **100% branch coverage** target on new code: unit tests, proptest
    round-trips, integration tests, Criterion bench, and a Kani proof for the
    bbox-superset invariant.

### Non-Goals

- **CRS other than WGS84.** No SRID tag, no on-the-fly projection. Planar
  geometry is deferred.
- **LineString, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection,**
  and **polygons with holes.** Reserved for a follow-up.
- **3D geometry / Z coordinate / measure (M).** Strict 2D.
- **Persistent on-disk R-tree.** The mmap backend rebuilds the index in
  memory on open. A page-level persistent format is a follow-up.
- **Spatial joins as first-class steps** (`g.V().nearest(other_v, k)`).
  Achievable today by composition; a sugar step is deferred.
- **Polygon validity repair / self-intersection detection.** Polygons are
  validated for closure and minimum vertex count only; semantics for invalid
  polygons are best-effort.
- **Geographic indexing strategies other than R-tree.** S2 and H3 are
  explicitly out of scope for v1 (see Alternatives Considered).

---

## Architecture

```
interstellar/src/
├── value.rs                 # +Value::Point, +Value::Polygon, discriminants 0x0A/0x0B
├── geo/                     # NEW
│   ├── mod.rs               # Point, Polygon, Distance, BoundingBox, Geometry trait
│   ├── distance.rs          # Haversine + helper conversions
│   ├── predicates.rs        # WithinDistance, Intersects, ContainedBy, BBox impls
│   └── tests.rs
├── storage/
│   ├── rtree/               # NEW
│   │   ├── mod.rs           # RTreeIndex: PropertyIndex
│   │   └── tests.rs
│   ├── cow.rs               # +RTreeIndex registration
│   ├── cow_mmap.rs          # +rebuild-on-open path
│   └── mmap/mod.rs          # +rebuild-on-open path, +populate from arena
└── traversal/
    └── predicate.rs         # +p::within_distance, p::intersects,
                             #  p::contained_by, p::bbox
```

No new top-level crate. The `geo` crate (BSD-3-Clause) provides
`geo::Polygon`, `geo::Point`, `geo::algorithm::contains::Contains`,
`geo::algorithm::intersects::Intersects`. The `rstar` crate (MIT/Apache-2.0)
provides the index. Both are non-optional dependencies of the `interstellar`
crate; they are pure-Rust and WASM-clean.

### Cargo dependency additions

```toml
# interstellar/Cargo.toml
geo        = "0.28"
rstar      = "0.12"
```

No feature flag. Geospatial support is always on.

---

## Core Types

### `geo` module

```rust
// interstellar/src/geo/mod.rs

/// A WGS84 longitude/latitude point, stored in degrees.
///
/// Invariants: `-180.0 <= lon <= 180.0` and `-90.0 <= lat <= 90.0`.
/// Constructors validate; deserialization clamps + logs (never panics).
#[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Point {
    pub lon: f64,
    pub lat: f64,
}

impl Point {
    pub fn new(lon: f64, lat: f64) -> Result<Self, GeoError> { /* ... */ }

    /// Construct without bounds checking. Caller asserts validity.
    pub fn new_unchecked(lon: f64, lat: f64) -> Self { Self { lon, lat } }
}

/// A simple closed polygon ring in WGS84, lon/lat order, no holes.
///
/// Invariants:
///   - At least 4 points (3 distinct + closure).
///   - First point equals last point (closure). Constructor closes it
///     automatically if not already closed.
///   - No self-intersection check (best effort; documented).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Polygon {
    /// Outer ring, lon/lat order. Always closed (first == last).
    pub ring: Vec<(f64, f64)>,
}

impl Polygon {
    pub fn new<I: IntoIterator<Item = (f64, f64)>>(pts: I) -> Result<Self, GeoError>;

    /// Cached axis-aligned bounding box.
    pub fn bbox(&self) -> BoundingBox;
}

/// Axis-aligned WGS84 bounding box. lon/lat in degrees.
#[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BoundingBox {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

impl BoundingBox {
    pub fn from_point(p: Point) -> Self;
    pub fn from_radius(center: Point, radius: Distance) -> Self;
    pub fn contains_point(&self, p: Point) -> bool;
    pub fn intersects(&self, other: &BoundingBox) -> bool;
}

/// Distance with explicit units. All math internally normalizes to meters.
#[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Distance {
    Meters(f64),
    Kilometers(f64),
    Miles(f64),
    NauticalMiles(f64),
}

impl Distance {
    pub fn meters(&self) -> f64;
    pub fn km(m: f64) -> Self { Self::Kilometers(m) }
    pub fn mi(m: f64) -> Self { Self::Miles(m) }
}

#[derive(Debug, thiserror::Error)]
pub enum GeoError {
    #[error("longitude {0} out of range [-180, 180]")]
    InvalidLongitude(f64),
    #[error("latitude {0} out of range [-90, 90]")]
    InvalidLatitude(f64),
    #[error("polygon requires at least 3 distinct points, got {0}")]
    PolygonTooSmall(usize),
    #[error("polygon contains non-finite coordinate")]
    PolygonNonFinite,
    #[error("distance must be non-negative and finite, got {0}")]
    InvalidDistance(f64),
}
```

### `Value` extensions

```rust
// interstellar/src/value.rs

pub enum Value {
    // existing variants 0x00..=0x09 ...
    Point(crate::geo::Point),       // discriminant 0x0A
    Polygon(crate::geo::Polygon),   // discriminant 0x0B
}
```

#### Serialization format

Tags `0x0A` and `0x0B` are appended to the existing `Value::serialize` /
`Value::deserialize` pair. Old data files (which contain only tags
`0x00..=0x09`) load unchanged. New files containing geometry values cannot be
read by older binaries — this is a forward-compatible, backward-incompatible
change to the value codec. Per established convention this bumps the
property arena's logical-format minor version; no migration is required.

| Tag    | Type     | Payload                                                   |
|--------|----------|-----------------------------------------------------------|
| `0x0A` | Point    | 16 bytes: `lon` (LE f64) ‖ `lat` (LE f64)                 |
| `0x0B` | Polygon  | 4-byte LE u32 ring length `n`, then `n × 16` bytes of `(lon, lat)` LE f64 pairs. The ring is always serialized closed; `n >= 4`. |

#### Conversions, helpers

```rust
impl From<Point>     for Value { /* Value::Point */ }
impl From<Polygon>   for Value { /* Value::Polygon */ }

impl Value {
    pub fn as_point(&self)   -> Option<Point>;
    pub fn as_polygon(&self) -> Option<&Polygon>;
    pub fn is_point(&self)   -> bool;
    pub fn is_polygon(&self) -> bool;
}
```

`ComparableValue` mirrors both variants:
- `ComparableValue::Point(OrderedFloat, OrderedFloat)`
- `ComparableValue::Polygon(Vec<(OrderedFloat, OrderedFloat)>)`

`Value::Hash` hashes `lon` / `lat` as `f64::to_bits()`, matching the
existing pattern for `Value::Float`.

`Value::discriminant` returns `0x0A` / `0x0B` consistently with the
serialization tag.

---

## Predicates

### Trait implementations (`geo::predicates`)

All four implement `traversal::predicate::Predicate`. They are `Send + Sync`
and have a `clone_box()` matching the existing pattern.

```rust
pub struct WithinDistance { pub center: Point, pub radius: Distance }
pub struct Intersects     { pub geom:   GeometryRef }
pub struct ContainedBy    { pub region: Polygon }
pub struct BBox           { pub bbox:   BoundingBox }

pub enum GeometryRef {
    Point(Point),
    Polygon(Polygon),
    BBox(BoundingBox),
}

impl Predicate for WithinDistance {
    fn test(&self, value: &Value) -> bool {
        match value {
            Value::Point(p)    => haversine(self.center, *p) <= self.radius.meters(),
            Value::Polygon(poly) => /* min vertex distance + edge-crossing check */,
            _ => false,
        }
    }
    fn clone_box(&self) -> Box<dyn Predicate> { Box::new(self.clone()) }
}
```

`Intersects` and `ContainedBy` delegate to the `geo` crate's
`Intersects`/`Contains` traits after converting our `Point`/`Polygon` into
`geo::Point`/`geo::Polygon`. The conversion is a thin `From` impl in
`geo/mod.rs`; no allocation for points, one `Vec` for polygons.

### Predicate factory exports

```rust
// re-exported via traversal::p

pub fn within_distance(center: Point, r: Distance) -> WithinDistance;
pub fn intersects(g: impl Into<GeometryRef>)       -> Intersects;
pub fn contained_by(poly: Polygon)                  -> ContainedBy;
pub fn bbox(min_lon: f64, min_lat: f64,
            max_lon: f64, max_lat: f64)             -> BBox;
```

Naming uses snake_case to match the existing `p::*` convention
(`p::eq`, `p::lt`, `p::within`, …).

### Index-aware filter planning

The traversal builder already has a hook (introduced for the B+ tree index
in spec-30) where a `HasWhere(prop, predicate)` step can be rewritten into
an `IndexLookup(prop, predicate)` step when the storage backend reports an
index for `prop` whose `IndexType` is compatible with `predicate`.

We extend the compatibility table:

| Predicate concrete type | Compatible `IndexType`           |
|-------------------------|----------------------------------|
| `WithinDistance`        | `IndexType::RTree`               |
| `Intersects`            | `IndexType::RTree`               |
| `ContainedBy`           | `IndexType::RTree`               |
| `BBox`                  | `IndexType::RTree`               |

Without an `RTreeIndex` registered, predicates fall back to the existing
scan path. Correctness is identical; only cost changes. This means **users
can adopt geo predicates with zero schema work**, then add an index later
purely for performance.

---

## Storage Integration

### `IndexType` extension

```rust
// interstellar/src/index/mod.rs

#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    BTree,
    Unique,
    RTree,            // NEW
}
```

### `RTreeIndex`

```rust
// interstellar/src/storage/rtree/mod.rs

use rstar::{RTree, RTreeObject, AABB, PointDistance};

pub struct RTreeIndex {
    spec:  IndexSpec,
    tree:  RwLock<RTree<Entry>>,
}

#[derive(Clone, Debug)]
struct Entry {
    id:    ElementId,        // VertexId or EdgeId, depending on spec.element_type
    point: [f64; 2],         // [lon, lat] for points; representative for polygons
    bbox:  [f64; 4],         // for polygons; for points, equals the point
    kind:  EntryKind,
}

#[derive(Clone, Copy, Debug)]
enum EntryKind { Point, Polygon }

impl RTreeObject for Entry {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners([self.bbox[0], self.bbox[1]],
                           [self.bbox[2], self.bbox[3]])
    }
}

impl PropertyIndex for RTreeIndex {
    fn insert(&mut self, id: ElementId, value: &Value) -> Result<(), IndexError>;
    fn remove(&mut self, id: ElementId, value: &Value) -> Result<(), IndexError>;
    fn lookup(&self, predicate: &dyn Predicate)
        -> Result<Box<dyn Iterator<Item = ElementId> + '_>, IndexError>;
    fn spec(&self) -> &IndexSpec;
    fn len(&self) -> usize;
}
```

`lookup` downcasts the predicate via the same `Any`-based dispatch the
B+ tree index already uses for `RangeQuery`/`EqualityQuery` predicates.
Each geo predicate exposes a `bounding_envelope()` method that yields an
`AABB<[f64; 2]>`; the R-tree's `locate_in_envelope_intersecting()` returns
the candidate set, and the predicate's `test()` filters the false positives
out. This two-phase pattern is what the Kani proof targets (Section
"Verification").

`Send + Sync` is satisfied: `rstar::RTree` is `Send + Sync` for `Send + Sync`
objects, and `Entry` contains only `Copy` data plus an enum.

### Backend wiring

For each of the three backends — `cow.rs`, `cow_mmap.rs`, `mmap/mod.rs` —
the `create_index` match arm gains a third arm:

```rust
let mut index: Box<dyn PropertyIndex> = match spec.index_type {
    IndexType::BTree  => Box::new(BTreeIndex::new(spec.clone())?),
    IndexType::Unique => Box::new(UniqueIndex::new(spec.clone())?),
    IndexType::RTree  => Box::new(RTreeIndex::new(spec.clone())?),
};
```

`populate_index` is unchanged: it iterates over all elements with the
target label/property and calls `index.insert` for each non-null
`Value::Point | Value::Polygon`. Non-geometry values are skipped (consistent
with how the B+ tree index handles type mismatches).

### Mmap rebuild-on-open

`MmapGraph::open` already invokes `populate_index` for any indexes declared
in the on-disk catalog. We extend the catalog format with one optional byte
per index entry (`0x02` = RTree) and rebuild like any other index. **No
spatial data is persisted**; rebuild cost is O(n log n) in indexed elements
and runs once at startup.

A future spec can add a persistent R-tree page format; until then,
applications with very large geo datasets can defer index creation until
after open if startup latency matters.

---

## Traversal API

```rust
use interstellar::geo::{Point, Distance, Polygon};
use interstellar::traversal::p;

// Find people within 5 km of a point
let near: Vec<Vertex> = g.v()
    .has_label("person")
    .has_where("home", p::within_distance(
        Point::new(-122.4194, 37.7749).unwrap(),
        Distance::Kilometers(5.0),
    ))
    .to_list()?;

// Compose with other predicates
g.v()
    .has_label("park")
    .has_where("boundary", p::contained_by(bay_area_polygon))
    .has_where("acres",    p::gt(100))
    .count();

// Bounding box pre-filter
g.e()
    .has_label("delivery")
    .has_where("dropoff", p::bbox(-122.6, 37.6, -122.3, 37.9))
    .to_list()?;
```

No new traversal step types are required. All four geo predicates flow
through the existing `has_where` / `where_` machinery.

---

## Gremlin Surface

### Grammar additions (pest)

```
geo_point       = { "point" ~ "(" ~ number ~ "," ~ number ~ ")" }
geo_polygon     = { "polygon" ~ "(" ~ "[" ~ point_list ~ "]" ~ ")" }
point_list      = { "[" ~ number ~ "," ~ number ~ "]"
                    ~ ("," ~ "[" ~ number ~ "," ~ number ~ "]")* }
distance_expr   = { number ~ ("m" | "km" | "mi" | "nmi")? }

geo_predicate   = { ("geo_within_distance"
                    | "geo_intersects"
                    | "geo_contained_by"
                    | "geo_bbox") ~ "(" ~ predicate_args ~ ")" }
```

### Examples

```groovy
g.V().has("location",
    geo_within_distance(point(-122.4194, 37.7749), 5km))
 .toList()

g.V().hasLabel("park")
     .has("boundary", geo_contained_by(polygon([[-122.6,37.6],[-122.3,37.6],[-122.3,37.9],[-122.6,37.9]])))
     .count()

g.V().has("location", geo_bbox(-122.6, 37.6, -122.3, 37.9))
```

The compiler maps `point(...)` / `polygon(...)` to `Value::Point` /
`Value::Polygon`, and `geo_*(...)` to the corresponding `p::*` factory.

---

## GQL Surface

### Grammar additions

```
geo_constructor ::= 'point' '(' map_literal ')'
                  | 'polygon' '(' '[' point_pair (',' point_pair)* ']' ')'
point_pair      ::= '[' number ',' number ']'

geo_function    ::= 'point' '.' 'distance' '(' expr ',' expr ')'
                  | 'point' '.' 'within_bbox' '(' expr ',' expr ',' expr ',' expr ',' expr ')'
                  | 'point' '.' 'within_distance' '(' expr ',' expr ',' distance_literal ')'

distance_literal ::= number ('m' | 'km' | 'mi' | 'nmi')

membership_op   ::= expr 'IN' polygon_expr
```

### Examples

```sql
-- distance threshold
MATCH (p:person)
WHERE point.distance(p.home, point({longitude: -122.4194, latitude: 37.7749})) < 5km
RETURN p.name;

-- bbox filter (planner uses RTreeIndex if present)
MATCH (e:event)
WHERE point.within_bbox(e.location, -122.6, 37.6, -122.3, 37.9)
RETURN e.title, e.location;

-- polygon membership
MATCH (p:park)
WHERE p.boundary IN polygon([[-122.6,37.6],[-122.3,37.6],[-122.3,37.9],[-122.6,37.9]])
RETURN p.name;

-- index DDL
CREATE INDEX RTREE ON :person(home);
DROP   INDEX RTREE ON :person(home);
```

`point({...})` matches Cypher's spatial constructor. The Interstellar
compiler accepts both `longitude`/`latitude` keys and short forms `lon`/`lat`.

---

## GraphSON

### Encoding

```jsonc
// Value::Point(lon=-122.4, lat=37.8)
{
  "@type": "g:Point",
  "@value": { "longitude": -122.4, "latitude": 37.8 }
}

// Value::Polygon(ring)
{
  "@type": "is:Polygon",
  "@value": {
    "ring": [
      [-122.6, 37.6],
      [-122.3, 37.6],
      [-122.3, 37.9],
      [-122.6, 37.9],
      [-122.6, 37.6]
    ]
  }
}
```

The `is:` namespace is Interstellar's existing extension namespace for
non-standard types. `g:Point` is encoded for cross-tool friendliness even
though TinkerPop's own GraphSON does not standardize spatial types.

The decoder also accepts a GeoJSON-like fallback
(`{"type":"Point","coordinates":[lon,lat]}`) for ergonomics, but the
canonical encoder emits the typed form.

---

## Bindings

### napi-rs (`interstellar-node`)

`Value::Point` ↔ `{ type: "Point", coordinates: [lon, lat] }`
`Value::Polygon` ↔ `{ type: "Polygon", coordinates: [[lon, lat], …] }`
(GeoJSON shape, no CRS object — WGS84 is implied.)

```javascript
const sf = { type: "Point", coordinates: [-122.4194, 37.7749] };
g.V().has("home",
    geo.withinDistance(sf, { km: 5 }))
 .toList();
```

The Node API exposes `geo.point`, `geo.polygon`,
`geo.withinDistance(center, distance)`, `geo.intersects(g)`,
`geo.containedBy(poly)`, `geo.bbox(...)`. The `distance` argument accepts
either a number-of-meters or `{ km | mi | nmi | m }`.

### WASM (`interstellar-wasm`)

Same shape as napi-rs, via `serde-wasm-bindgen`. Polygons round-trip as
plain `Float64Array`-friendly nested arrays. No new feature flag needed;
`rstar` and `geo` both compile cleanly to `wasm32-unknown-unknown`.

### CLI / REPL (`interstellar-cli`)

Print as WKT for human readability:
```
POINT(-122.4194 37.7749)
POLYGON((-122.6 37.6, -122.3 37.6, -122.3 37.9, -122.6 37.9, -122.6 37.6))
```
A flag `--geo-format=geojson` switches to GeoJSON. WKT *parsing* is not in
scope; the REPL accepts the same `point(...)` / `polygon(...)` constructors
as Gremlin/GQL.

---

## Verification

### Property-based tests (`proptest`)

- **Round-trip**: any random valid `Point` / `Polygon` survives
  `serialize → deserialize` byte-for-byte.
- **Bounded coordinates**: `Point::new` rejects out-of-range lat/lon and
  accepts every value in range.
- **Polygon closure**: `Polygon::new` always returns a ring with
  `first == last`.
- **Predicate consistency**: for any random `Point p` and `Distance r`,
  `WithinDistance { center, r }.test(&Value::Point(p))` agrees with a
  reference haversine computation.

### Integration tests

- 10 000 random points; brute-force scan vs `RTreeIndex` lookup must
  return identical ID sets for each predicate (`within_distance`,
  `bbox`, `contained_by`, `intersects`).
- Mmap backend: create index, write 10 000 points + polygons, close,
  reopen, assert all queries still return identical results.
- GraphSON round-trip on a graph with mixed `Point` / `Polygon`
  properties.
- Gremlin and GQL parser round-trips for every new grammar production.

### Bench (`benches/geo.rs`)

- 10k / 100k / 1M points, `within_distance` with 1 km / 10 km / 100 km
  radii, scan vs R-tree.
- Polygon containment over 1k random polygons in a 100k-point dataset.

### Kani proof (`kani/geo_index_superset.rs`)

```rust
#[kani::proof]
fn rtree_lookup_is_a_superset_of_true_matches() {
    // For an arbitrary 8-element point set and an arbitrary BBox predicate,
    // assert: every element whose stored Value::Point satisfies the predicate
    // is contained in the candidate set returned by RTreeIndex::lookup.
}
```

This guarantees the index never *misses* a true match — the
"R-tree returns a superset, predicate filters" pattern is correct by
construction.

### Coverage target

100 % branch coverage on `interstellar/src/geo/**` and
`interstellar/src/storage/rtree/**` per the project standard.

---

## Implementation Phases

Each phase is independently shippable, lands behind no feature flag, and
keeps `cargo test --workspace` green.

### Phase 1 — Value types & serialization
- Add `geo` module with `Point`, `Polygon`, `Distance`, `BoundingBox`,
  `GeoError`.
- Add `Value::Point` (`0x0A`) and `Value::Polygon` (`0x0B`).
- Update `Value::serialize` / `deserialize` / `discriminant` /
  `to_comparable` / `Hash` / `From` impls / accessors.
- Update GraphSON encoder/decoder.
- Update napi-rs and WASM `Value` conversions.
- Update CLI WKT printer.
- Unit + proptest coverage.

**Deliverable:** users can store and round-trip geometry properties end to
end; no querying yet.

### Phase 2 — Predicates and scan path
- Add `geo::predicates::{WithinDistance, Intersects, ContainedBy, BBox}`.
- Add `traversal::p::{within_distance, intersects, contained_by, bbox}`.
- Wire into `has_where` (no special path; uses scan).
- Integration tests on small dataset; brute-force oracle.

**Deliverable:** fully functional geo queries via `has_where` at O(n).

### Phase 3 — R-tree index
- Add `IndexType::RTree`.
- Add `RTreeIndex` impl of `PropertyIndex`.
- Wire into `cow.rs`, `cow_mmap.rs`, `mmap/mod.rs` index registration.
- Extend filter-planner predicate→index compatibility table.
- Mmap rebuild-on-open path; persist `IndexType::RTree` byte in the
  on-disk catalog.
- Bench, coverage, Kani proof.

**Deliverable:** registered indexes turn O(n) geo predicates into
O(log n + k) lookups transparently.

### Phase 4 — Query language surface
- Gremlin grammar: `point`, `polygon`, `geo_*` predicate factories,
  `<n>km` / `<n>mi` distance literals.
- GQL grammar: `point({...})`, `polygon([...])`, `point.distance`,
  `point.within_bbox`, `point.within_distance`, `IN <polygon>`,
  `CREATE INDEX RTREE` DDL, distance literals.
- Compiler dispatch for both languages.
- Round-trip tests for every new production.

**Deliverable:** geo queries usable from Gremlin and GQL.

### Phase 5 — Docs, examples, polish
- `examples/geo_cities.rs` — load a city dataset, run within-distance
  and bbox queries.
- User-facing doc page under `interstellar/docs/`.
- Update `AGENTS.md`, `feature-proposals.md` (mark #10 shipped).
- Release notes.

**Deliverable:** feature is discoverable, documented, and demonstrable.

---

## Alternatives Considered

- **`s2` cell index.** Better for very large datasets and amenable to
  on-disk persistence via the existing `BTreeIndex` (cells are u64 keys).
  Rejected for v1 because R-tree gives easier geometry semantics
  (polygon-in-polygon, exact distance) and simpler integration.
- **`h3o` hex grid.** Excellent for analytics / aggregation but not a
  general-purpose primary index. Considered as a future complementary
  index type.
- **Geohash strings + existing `BTreeIndex`.** Zero new index code, but
  weak at radius-boundary queries (false negatives unless we expand to
  neighbor cells, which adds complexity without an ergonomic win).
- **Persistent on-disk R-tree.** Real but a substantial undertaking —
  page-aligned node layout, freelist integration, WAL records. Deferred
  to a follow-up; rebuild-on-open is acceptable for current targets.
- **CRS / SRID parameter.** Would allow planar geometry and other
  coordinate systems. Out of scope; everything is WGS84.

---

## Open Risks

1. **R-tree rebuild latency on open.** For a graph with tens of millions of
   indexed geometries this could add seconds to startup. Mitigation:
   parallelize rebuild across indexes; document the cost; later spec for
   persistence.
2. **Polygon predicate cost.** Polygon containment via `geo::Polygon`
   uses the Shamos-Hoey approach; pathological self-intersecting input
   yields undefined results. Mitigation: validate-on-construct and document.
3. **f64 precision near the antimeridian / poles.** Haversine is robust;
   bbox queries that straddle the antimeridian require splitting into two
   AABBs. v1 documents that bboxes must not cross longitude ±180; a
   helper `BoundingBox::split_antimeridian` is added in Phase 1 to make
   user-side splitting easy.
4. **Index<->predicate downcast brittleness.** The `Any` dispatch already
   used for B+ tree predicates is repeated here. A single registration
   table in `traversal::predicate` would be cleaner; that refactor is
   tracked as a follow-up but is not required for this spec.

---

## Out of Scope (Tracked for Follow-ups)

- Persistent R-tree page format.
- LineString / MultiPolygon / polygon-with-holes.
- S2 / H3 secondary index types.
- Spatial join steps (`g.V().nearest(other, k)`).
- WKT parsing in the REPL.
- 3D / Z / measure coordinates.
- Antimeridian-crossing bbox queries handled automatically.
