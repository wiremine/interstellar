//! Distance type with explicit units.

use std::fmt;

/// Distance with explicit units. All math internally normalizes to meters.
#[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Distance {
    /// Distance in meters.
    Meters(f64),
    /// Distance in kilometers.
    Kilometers(f64),
    /// Distance in statute miles.
    Miles(f64),
    /// Distance in nautical miles.
    NauticalMiles(f64),
}

impl Distance {
    /// Convert to meters.
    #[inline]
    pub fn meters(&self) -> f64 {
        match self {
            Self::Meters(m) => *m,
            Self::Kilometers(km) => km * 1_000.0,
            Self::Miles(mi) => mi * 1_609.344,
            Self::NauticalMiles(nmi) => nmi * 1_852.0,
        }
    }

    /// Shorthand for `Distance::Kilometers`.
    #[inline]
    pub fn km(v: f64) -> Self {
        Self::Kilometers(v)
    }

    /// Shorthand for `Distance::Miles`.
    #[inline]
    pub fn mi(v: f64) -> Self {
        Self::Miles(v)
    }
}

impl fmt::Display for Distance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Meters(v) => write!(f, "{}m", v),
            Self::Kilometers(v) => write!(f, "{}km", v),
            Self::Miles(v) => write!(f, "{}mi", v),
            Self::NauticalMiles(v) => write!(f, "{}nmi", v),
        }
    }
}
