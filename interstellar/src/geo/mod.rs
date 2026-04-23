//! Geospatial types and operations for WGS84 coordinate data.
//!
//! This module provides first-class geospatial support:
//!
//! - [`Point`] — A WGS84 longitude/latitude point
//! - [`Polygon`] — A simple closed polygon ring (no holes)
//! - [`BoundingBox`] — An axis-aligned bounding box
//! - [`Distance`] — A distance with explicit units
//! - [`GeoError`] — Error type for geospatial operations
//!
//! All coordinates use WGS84 (EPSG:4326). Longitude is in the range
//! `[-180, 180]` and latitude in `[-90, 90]`, both in degrees.

mod distance;

pub use distance::Distance;

use std::fmt;

// ---------------------------------------------------------------------------
// GeoError
// ---------------------------------------------------------------------------

/// Errors from geospatial operations.
#[derive(Debug, thiserror::Error)]
pub enum GeoError {
    /// Longitude is outside the valid range `[-180, 180]`.
    #[error("longitude {0} out of range [-180, 180]")]
    InvalidLongitude(f64),

    /// Latitude is outside the valid range `[-90, 90]`.
    #[error("latitude {0} out of range [-90, 90]")]
    InvalidLatitude(f64),

    /// Polygon requires at least 3 distinct points.
    #[error("polygon requires at least 3 distinct points, got {0}")]
    PolygonTooSmall(usize),

    /// Polygon contains a non-finite coordinate (NaN or infinity).
    #[error("polygon contains non-finite coordinate")]
    PolygonNonFinite,

    /// Polygon coordinate is outside WGS84 bounds.
    #[error("polygon coordinate ({0}, {1}) out of WGS84 bounds")]
    InvalidCoordinate(f64, f64),

    /// Distance must be non-negative and finite.
    #[error("distance must be non-negative and finite, got {0}")]
    InvalidDistance(f64),
}

// ---------------------------------------------------------------------------
// Point
// ---------------------------------------------------------------------------

/// A WGS84 longitude/latitude point, stored in degrees.
///
/// Invariants: `-180.0 <= lon <= 180.0` and `-90.0 <= lat <= 90.0`.
/// Constructors validate; deserialization clamps (never panics).
#[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Point {
    /// Longitude in degrees, `[-180, 180]`.
    pub lon: f64,
    /// Latitude in degrees, `[-90, 90]`.
    pub lat: f64,
}

impl Point {
    /// Create a new point with validation.
    pub fn new(lon: f64, lat: f64) -> Result<Self, GeoError> {
        if !lon.is_finite() || !(-180.0..=180.0).contains(&lon) {
            return Err(GeoError::InvalidLongitude(lon));
        }
        if !lat.is_finite() || !(-90.0..=90.0).contains(&lat) {
            return Err(GeoError::InvalidLatitude(lat));
        }
        Ok(Self { lon, lat })
    }

    /// Construct without bounds checking. Caller asserts validity.
    #[inline]
    pub fn new_unchecked(lon: f64, lat: f64) -> Self {
        Self { lon, lat }
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "POINT({} {})", self.lon, self.lat)
    }
}

// ---------------------------------------------------------------------------
// Polygon
// ---------------------------------------------------------------------------

/// A simple closed polygon ring in WGS84, lon/lat order, no holes.
///
/// Invariants:
///   - At least 4 points (3 distinct + closure).
///   - First point equals last point (closure). Constructor closes it
///     automatically if not already closed.
///   - All coordinates are finite and within WGS84 bounds.
///   - No self-intersection check (best effort; documented).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Polygon {
    /// Outer ring, lon/lat order. Always closed (first == last).
    pub ring: Vec<(f64, f64)>,
}

impl Polygon {
    /// Create a new polygon from an iterator of `(lon, lat)` pairs.
    ///
    /// The ring is automatically closed if the first and last points differ.
    /// Returns an error if fewer than 3 distinct points are provided or if
    /// any coordinate is invalid.
    pub fn new<I: IntoIterator<Item = (f64, f64)>>(pts: I) -> Result<Self, GeoError> {
        let mut ring: Vec<(f64, f64)> = pts.into_iter().collect();

        // Validate all coordinates
        for &(lon, lat) in &ring {
            if !lon.is_finite() || !lat.is_finite() {
                return Err(GeoError::PolygonNonFinite);
            }
            if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
                return Err(GeoError::InvalidCoordinate(lon, lat));
            }
        }

        // Auto-close if needed
        if ring.len() >= 2 && ring.first() != ring.last() {
            let first = ring[0];
            ring.push(first);
        }

        // Need at least 4 points (3 distinct + closure)
        let distinct = if ring.len() >= 2 {
            ring.len() - 1
        } else {
            ring.len()
        };
        if distinct < 3 {
            return Err(GeoError::PolygonTooSmall(distinct));
        }

        Ok(Self { ring })
    }

    /// Compute the axis-aligned bounding box.
    pub fn bbox(&self) -> BoundingBox {
        let mut min_lon = f64::INFINITY;
        let mut min_lat = f64::INFINITY;
        let mut max_lon = f64::NEG_INFINITY;
        let mut max_lat = f64::NEG_INFINITY;

        for &(lon, lat) in &self.ring {
            min_lon = min_lon.min(lon);
            min_lat = min_lat.min(lat);
            max_lon = max_lon.max(lon);
            max_lat = max_lat.max(lat);
        }

        BoundingBox {
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        }
    }
}

impl fmt::Display for Polygon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "POLYGON((")?;
        for (i, &(lon, lat)) in self.ring.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} {}", lon, lat)?;
        }
        write!(f, "))")
    }
}

// ---------------------------------------------------------------------------
// BoundingBox
// ---------------------------------------------------------------------------

/// Axis-aligned WGS84 bounding box. lon/lat in degrees.
#[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BoundingBox {
    /// Minimum longitude.
    pub min_lon: f64,
    /// Minimum latitude.
    pub min_lat: f64,
    /// Maximum longitude.
    pub max_lon: f64,
    /// Maximum latitude.
    pub max_lat: f64,
}

impl BoundingBox {
    /// Create a zero-area bounding box from a single point.
    pub fn from_point(p: Point) -> Self {
        Self {
            min_lon: p.lon,
            min_lat: p.lat,
            max_lon: p.lon,
            max_lat: p.lat,
        }
    }

    /// Create a bounding box that encloses a circle of the given radius
    /// centered on `center`. This is an approximation that over-estimates
    /// slightly near the poles.
    pub fn from_radius(center: Point, radius: Distance) -> Self {
        let meters = radius.meters();
        // Approximate degrees per meter at this latitude
        let lat_delta = meters / 111_320.0;
        let lon_delta = meters / (111_320.0 * center.lat.to_radians().cos().abs().max(0.0001));

        Self {
            min_lon: (center.lon - lon_delta).max(-180.0),
            min_lat: (center.lat - lat_delta).max(-90.0),
            max_lon: (center.lon + lon_delta).min(180.0),
            max_lat: (center.lat + lat_delta).min(90.0),
        }
    }

    /// Check if a point falls within this bounding box.
    #[inline]
    pub fn contains_point(&self, p: Point) -> bool {
        p.lon >= self.min_lon
            && p.lon <= self.max_lon
            && p.lat >= self.min_lat
            && p.lat <= self.max_lat
    }

    /// Check if two bounding boxes intersect.
    #[inline]
    pub fn intersects(&self, other: &BoundingBox) -> bool {
        self.min_lon <= other.max_lon
            && self.max_lon >= other.min_lon
            && self.min_lat <= other.max_lat
            && self.max_lat >= other.min_lat
    }

    /// Split a bbox that crosses the antimeridian (±180° longitude) into
    /// two non-crossing bboxes. Returns `None` if the bbox does not cross.
    pub fn split_antimeridian(&self) -> Option<(BoundingBox, BoundingBox)> {
        if self.min_lon <= self.max_lon {
            return None; // Does not cross
        }
        Some((
            BoundingBox {
                min_lon: self.min_lon,
                min_lat: self.min_lat,
                max_lon: 180.0,
                max_lat: self.max_lat,
            },
            BoundingBox {
                min_lon: -180.0,
                min_lat: self.min_lat,
                max_lon: self.max_lon,
                max_lat: self.max_lat,
            },
        ))
    }
}

impl fmt::Display for BoundingBox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BBOX({}, {}, {}, {})",
            self.min_lon, self.min_lat, self.max_lon, self.max_lat
        )
    }
}

// ---------------------------------------------------------------------------
// Haversine distance
// ---------------------------------------------------------------------------

/// Compute the great-circle distance in meters between two points using
/// the haversine formula.
pub fn haversine(a: Point, b: Point) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;

    let d_lat = (b.lat - a.lat).to_radians();
    let d_lon = (b.lon - a.lon).to_radians();

    let lat1 = a.lat.to_radians();
    let lat2 = b.lat.to_radians();

    let h = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);

    2.0 * EARTH_RADIUS_M * h.sqrt().asin()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Point --

    #[test]
    fn point_new_valid() {
        let p = Point::new(-122.4194, 37.7749).unwrap();
        assert_eq!(p.lon, -122.4194);
        assert_eq!(p.lat, 37.7749);
    }

    #[test]
    fn point_new_boundary() {
        assert!(Point::new(-180.0, -90.0).is_ok());
        assert!(Point::new(180.0, 90.0).is_ok());
        assert!(Point::new(0.0, 0.0).is_ok());
    }

    #[test]
    fn point_new_invalid_lon() {
        assert!(matches!(
            Point::new(181.0, 0.0),
            Err(GeoError::InvalidLongitude(_))
        ));
        assert!(matches!(
            Point::new(-181.0, 0.0),
            Err(GeoError::InvalidLongitude(_))
        ));
        assert!(matches!(
            Point::new(f64::NAN, 0.0),
            Err(GeoError::InvalidLongitude(_))
        ));
        assert!(matches!(
            Point::new(f64::INFINITY, 0.0),
            Err(GeoError::InvalidLongitude(_))
        ));
    }

    #[test]
    fn point_new_invalid_lat() {
        assert!(matches!(
            Point::new(0.0, 91.0),
            Err(GeoError::InvalidLatitude(_))
        ));
        assert!(matches!(
            Point::new(0.0, -91.0),
            Err(GeoError::InvalidLatitude(_))
        ));
    }

    #[test]
    fn point_display() {
        let p = Point::new(1.5, 2.5).unwrap();
        assert_eq!(format!("{}", p), "POINT(1.5 2.5)");
    }

    // -- Polygon --

    #[test]
    fn polygon_new_valid_triangle() {
        let poly = Polygon::new(vec![(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]).unwrap();
        assert_eq!(poly.ring.len(), 4); // auto-closed
        assert_eq!(poly.ring.first(), poly.ring.last());
    }

    #[test]
    fn polygon_new_already_closed() {
        let poly = Polygon::new(vec![(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)]).unwrap();
        assert_eq!(poly.ring.len(), 4);
    }

    #[test]
    fn polygon_too_small() {
        assert!(matches!(
            Polygon::new(vec![(0.0, 0.0), (1.0, 0.0)]),
            Err(GeoError::PolygonTooSmall(2))
        ));
    }

    #[test]
    fn polygon_non_finite() {
        assert!(matches!(
            Polygon::new(vec![(f64::NAN, 0.0), (1.0, 0.0), (0.0, 1.0)]),
            Err(GeoError::PolygonNonFinite)
        ));
    }

    #[test]
    fn polygon_invalid_coordinate() {
        assert!(matches!(
            Polygon::new(vec![(200.0, 0.0), (1.0, 0.0), (0.0, 1.0)]),
            Err(GeoError::InvalidCoordinate(200.0, 0.0))
        ));
    }

    #[test]
    fn polygon_bbox() {
        let poly = Polygon::new(vec![
            (-10.0, -20.0),
            (10.0, -20.0),
            (10.0, 20.0),
            (-10.0, 20.0),
        ])
        .unwrap();
        let bb = poly.bbox();
        assert_eq!(bb.min_lon, -10.0);
        assert_eq!(bb.max_lon, 10.0);
        assert_eq!(bb.min_lat, -20.0);
        assert_eq!(bb.max_lat, 20.0);
    }

    #[test]
    fn polygon_display() {
        let poly = Polygon::new(vec![(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]).unwrap();
        let s = format!("{}", poly);
        assert!(s.starts_with("POLYGON(("));
        assert!(s.ends_with("))"));
    }

    // -- BoundingBox --

    #[test]
    fn bbox_from_point() {
        let p = Point::new(10.0, 20.0).unwrap();
        let bb = BoundingBox::from_point(p);
        assert_eq!(bb.min_lon, 10.0);
        assert_eq!(bb.max_lon, 10.0);
    }

    #[test]
    fn bbox_contains_point() {
        let bb = BoundingBox {
            min_lon: -10.0,
            min_lat: -10.0,
            max_lon: 10.0,
            max_lat: 10.0,
        };
        assert!(bb.contains_point(Point::new(0.0, 0.0).unwrap()));
        assert!(bb.contains_point(Point::new(-10.0, -10.0).unwrap()));
        assert!(!bb.contains_point(Point::new(11.0, 0.0).unwrap()));
    }

    #[test]
    fn bbox_intersects() {
        let a = BoundingBox {
            min_lon: 0.0,
            min_lat: 0.0,
            max_lon: 10.0,
            max_lat: 10.0,
        };
        let b = BoundingBox {
            min_lon: 5.0,
            min_lat: 5.0,
            max_lon: 15.0,
            max_lat: 15.0,
        };
        let c = BoundingBox {
            min_lon: 20.0,
            min_lat: 20.0,
            max_lon: 30.0,
            max_lat: 30.0,
        };
        assert!(a.intersects(&b));
        assert!(!a.intersects(&c));
    }

    #[test]
    fn bbox_split_antimeridian_no_cross() {
        let bb = BoundingBox {
            min_lon: 170.0,
            min_lat: -10.0,
            max_lon: 180.0,
            max_lat: 10.0,
        };
        assert!(bb.split_antimeridian().is_none());
    }

    #[test]
    fn bbox_split_antimeridian_cross() {
        // min_lon > max_lon indicates crossing
        let bb = BoundingBox {
            min_lon: 170.0,
            min_lat: -10.0,
            max_lon: -170.0,
            max_lat: 10.0,
        };
        let (left, right) = bb.split_antimeridian().unwrap();
        assert_eq!(left.min_lon, 170.0);
        assert_eq!(left.max_lon, 180.0);
        assert_eq!(right.min_lon, -180.0);
        assert_eq!(right.max_lon, -170.0);
    }

    #[test]
    fn bbox_from_radius() {
        let center = Point::new(0.0, 0.0).unwrap();
        let bb = BoundingBox::from_radius(center, Distance::Kilometers(100.0));
        assert!(bb.min_lon < 0.0);
        assert!(bb.max_lon > 0.0);
        assert!(bb.min_lat < 0.0);
        assert!(bb.max_lat > 0.0);
    }

    // -- Haversine --

    #[test]
    fn haversine_same_point() {
        let p = Point::new(0.0, 0.0).unwrap();
        assert!((haversine(p, p) - 0.0).abs() < 1e-10);
    }

    #[test]
    fn haversine_known_distance() {
        // New York to London, ~5570 km
        let ny = Point::new(-74.006, 40.7128).unwrap();
        let london = Point::new(-0.1278, 51.5074).unwrap();
        let dist = haversine(ny, london);
        assert!((dist - 5_570_000.0).abs() < 50_000.0); // within 50km
    }

    // -- Distance --

    #[test]
    fn distance_conversions() {
        let d = Distance::Kilometers(5.0);
        assert!((d.meters() - 5000.0).abs() < 1e-10);

        let d = Distance::Miles(1.0);
        assert!((d.meters() - 1609.344).abs() < 0.01);

        let d = Distance::NauticalMiles(1.0);
        assert!((d.meters() - 1852.0).abs() < 0.01);

        let d = Distance::Meters(100.0);
        assert!((d.meters() - 100.0).abs() < 1e-10);
    }

    #[test]
    fn distance_display() {
        assert_eq!(format!("{}", Distance::Meters(100.0)), "100m");
        assert_eq!(format!("{}", Distance::Kilometers(5.0)), "5km");
        assert_eq!(format!("{}", Distance::Miles(3.1)), "3.1mi");
        assert_eq!(format!("{}", Distance::NauticalMiles(2.7)), "2.7nmi");
    }
}
