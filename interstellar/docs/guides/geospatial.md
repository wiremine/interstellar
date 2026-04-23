# Geospatial Queries

Interstellar has built-in geospatial support: `Point` and `Polygon` geometry types, an R-tree spatial index, and predicates for distance, bounding-box, intersection, and containment queries. All coordinates use WGS84 (EPSG:4326) with haversine distances in meters.

For a runnable end-to-end walkthrough, see [`examples/geo_cities.rs`](../../examples/geo_cities.rs):

```bash
cargo run --example geo_cities
```

---

## Geometry types

### Point

A `Point` is a WGS84 longitude/latitude pair. Longitude must be in `[-180, 180]` and latitude in `[-90, 90]`.

```rust
use interstellar::geo::Point;
use interstellar::value::Value;

let p = Point::new(-74.006, 40.7128).expect("valid coordinate");
let v: Value = Value::Point(p);
```

Use `Point::new_unchecked(lon, lat)` to skip validation when you know the coordinates are valid.

### Polygon

A `Polygon` is a closed ring of `(lon, lat)` coordinate pairs. The first and last coordinates must be identical (closed ring). Minimum 4 pairs (triangle + closing point).

```rust
use interstellar::geo::Polygon;

let triangle = Polygon::new(vec![
    (0.0, 0.0),
    (1.0, 0.0),
    (0.5, 1.0),
    (0.0, 0.0), // closes the ring
]).expect("valid polygon");
```

### Distance

The `Distance` enum represents a distance with units:

```rust
use interstellar::geo::Distance;

let d = Distance::Meters(5000.0);
let d = Distance::Kilometers(5.0);
let d = Distance::Miles(3.1);
let d = Distance::NauticalMiles(2.7);

// Convert to meters
assert_eq!(Distance::Kilometers(1.0).to_meters(), 1000.0);
```

### BoundingBox

A `BoundingBox` defines an axis-aligned rectangle in lon/lat space:

```rust
use interstellar::geo::BoundingBox;

let bbox = BoundingBox::new(-125.0, 30.0, -100.0, 50.0);
assert!(bbox.contains_point(-112.0, 33.4));
```

---

## Serialization

`Value::Point` and `Value::Polygon` are first-class `Value` variants. They serialize with tags `0x0A` and `0x0B` respectively, and round-trip through all storage backends.

---

## R-tree spatial index

Create an R-tree index on a vertex property that stores `Point` values:

```rust
use std::sync::Arc;
use interstellar::index::IndexBuilder;
use interstellar::storage::Graph;

let graph = Arc::new(Graph::new());

let spec = IndexBuilder::vertex()
    .label("city")
    .property("location")
    .rtree()
    .build()
    .expect("valid index spec");
graph.create_index(spec).expect("index creation failed");
```

The R-tree index accelerates spatial predicates. It is rebuilt on open for the mmap backend (no persistent on-disk R-tree yet).

---

## Gremlin predicates

Use these predicates inside `.has(property, predicate)` steps:

### `geo_bbox` — bounding-box filter

```gremlin
g.V().hasLabel('city')
  .has('location', geo_bbox(-125.0, 30.0, -100.0, 50.0))
  .values('name').toList()
```

### `geo_within_distance` — radius search

```gremlin
g.V().hasLabel('city')
  .has('location', geo_within_distance(point(-0.1278, 51.5074), 1000km))
  .values('name').toList()
```

Distance units: `m` (meters), `km` (kilometers), `mi` (miles), `nmi` (nautical miles).

### `geo_intersects` — geometry intersection

```gremlin
g.V().has('location', geo_intersects(point(2.35, 48.86)))
  .values('name').toList()
```

### `geo_contained_by` — polygon containment

```gremlin
g.V().hasLabel('city')
  .has('location', geo_contained_by(
    polygon([[-10.0, 35.0], [40.0, 35.0], [40.0, 72.0], [-10.0, 72.0], [-10.0, 35.0]])
  ))
  .values('name').toList()
```

### Geo value literals

- **Point**: `point(lon, lat)` — e.g. `point(-74.006, 40.7128)`
- **Polygon**: `polygon([[lon1, lat1], [lon2, lat2], ...])` — closed ring of coordinate pairs

---

## GQL surface (parsing only)

GQL grammar supports geospatial constructs for parsing and validation:

```sql
-- R-tree index creation
CREATE RTREE INDEX idx_loc ON city(location)

-- Point and polygon constructors
POINT(-74.006, 40.7128)
POLYGON((-10.0, 35.0), (40.0, 35.0), (40.0, 72.0), (-10.0, 72.0), (-10.0, 35.0))

-- Geo functions in WHERE clauses
MATCH (c:city)
WHERE point.within_distance(c.location, POINT(139.69, 35.69), 800km)
RETURN c.name
```

> **Note**: GQL geo expression *evaluation* is not yet implemented. The grammar parses and validates these constructs, but runtime execution is planned for a future release.

---

## Rust predicate API

The `p` module provides predicate factories for use in Rust code:

```rust
use interstellar::p;
use interstellar::geo::{Point, Distance, Polygon};

// Radius search
let center = Point::new(-74.006, 40.7128).unwrap();
let pred = p::within_distance(center, Distance::Kilometers(500.0));

// Bounding box
let pred = p::bbox(-125.0, 30.0, -100.0, 50.0);

// Polygon containment
let poly = Polygon::new(vec![
    (-10.0, 35.0), (40.0, 35.0), (40.0, 72.0),
    (-10.0, 72.0), (-10.0, 35.0),
]).unwrap();
let pred = p::contained_by(poly);

// Intersection (accepts Point or Polygon)
let pred = p::intersects(Point::new(2.35, 48.86).unwrap());
```

These predicates implement the `Predicate` trait and can be used wherever predicates are accepted (Gremlin compilation, property filtering, etc.).

---

## Limitations

- **WGS84 only** — no support for other coordinate reference systems.
- **R-tree is in-memory** — rebuilt on open for the mmap backend. Large datasets may add startup latency.
- **GQL evaluation** — geo expressions parse but don't execute yet.
- **Polygon predicates** — self-intersecting polygons yield undefined results. Validate geometry on construction.
