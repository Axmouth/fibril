//! The compressed day: every world paces itself against one shared clock so
//! traffic has rush hours, quiet nights, and out-of-phase rhythms instead of
//! uniform noise. A "day" defaults to a few real minutes.

use std::time::Instant;

#[derive(Clone)]
pub struct DayClock {
    start: Instant,
    day_secs: f64,
}

impl DayClock {
    pub fn new(day_secs: u64) -> Self {
        Self {
            start: Instant::now(),
            day_secs: day_secs.max(30) as f64,
        }
    }

    /// Time of day as a fraction in [0, 1). 0.0 is midnight, 0.5 is noon.
    pub fn time_of_day(&self) -> f64 {
        (self.start.elapsed().as_secs_f64() / self.day_secs).fract()
    }

    /// True while the time of day is inside [from, to).
    pub fn within(&self, from: f64, to: f64) -> bool {
        let t = self.time_of_day();
        t >= from && t < to
    }
}

/// Piecewise-linear rate curve over the day: control points of
/// (time-of-day, multiplier), interpolated and wrapped at midnight.
pub fn rhythm(curve: &[(f64, f64)], t: f64) -> f64 {
    if curve.is_empty() {
        return 1.0;
    }
    // Find the segment [a, b) that contains t, wrapping around midnight.
    for pair in curve.windows(2) {
        let (ta, va) = pair[0];
        let (tb, vb) = pair[1];
        if t >= ta && t < tb {
            let f = (t - ta) / (tb - ta).max(f64::EPSILON);
            return va + (vb - va) * f;
        }
    }
    // Wrap segment: from the last point through midnight to the first.
    let (ta, va) = curve[curve.len() - 1];
    let (tb, vb) = curve[0];
    let span = (1.0 - ta) + tb;
    let f = if t >= ta { t - ta } else { (1.0 - ta) + t } / span.max(f64::EPSILON);
    va + (vb - va) * f
}

#[cfg(test)]
mod tests {
    use super::rhythm;

    #[test]
    fn rhythm_interpolates_and_wraps() {
        let curve = [(0.25, 0.0), (0.5, 2.0), (0.75, 0.0)];
        assert!((rhythm(&curve, 0.375) - 1.0).abs() < 1e-9);
        assert!((rhythm(&curve, 0.5) - 2.0).abs() < 1e-9);
        // Between 0.75 and 0.25 (through midnight) it goes 0.0 -> 0.0.
        assert!(rhythm(&curve, 0.0).abs() < 1e-9);
    }
}
