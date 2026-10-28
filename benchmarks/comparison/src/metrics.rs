use anyhow::{Context, Result, ensure};
use hdrhistogram::Histogram;
use serde::Serialize;

// Microseconds, three significant digits. Overflow is an error, never clamped.
pub struct Latency(Histogram<u64>);
impl Default for Latency {
    fn default() -> Self {
        Self(Histogram::new_with_bounds(1, 3_600_000_000, 3).unwrap())
    }
}
impl Latency {
    pub fn record(&mut self, ns: u64) -> Result<()> {
        self.0.record(ns.div_ceil(1000).max(1))?;
        Ok(())
    }
    pub fn summary(&self) -> serde_json::Value {
        let ms = |q| self.0.value_at_quantile(q) as f64 / 1000.0;
        serde_json::json!({"samples":self.0.len(), "p50_ms":ms(0.5), "p95_ms":ms(0.95),
            "p99_ms":ms(0.99), "p999_ms":ms(0.999), "max_ms":self.0.max() as f64 / 1000.0,
            "mean_ms":self.0.mean()/1000.0})
    }
}

pub const HEADER: usize = 32;
const MAGIC: u64 = 0x3148434e45425246;
#[derive(Clone, Copy, Debug)]
pub struct Stamp {
    pub id: u64,
    pub intended: u64,
    pub admitted: u64,
}
impl Stamp {
    pub fn encode(self, size: usize) -> Vec<u8> {
        let mut bytes = vec![0xa5; size];
        for (chunk, value) in
            bytes[..HEADER]
                .chunks_exact_mut(8)
                .zip([MAGIC, self.id, self.intended, self.admitted])
        {
            chunk.copy_from_slice(&value.to_le_bytes());
        }
        bytes
    }
    pub fn decode(bytes: &[u8], size: usize) -> Result<Self> {
        ensure!(
            bytes.len() == size && size >= HEADER,
            "wrong payload length: {}",
            bytes.len()
        );
        let read = |at| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        ensure!(read(0) == MAGIC, "wrong benchmark payload magic");
        ensure!(
            bytes[HEADER..].iter().all(|b| *b == 0xa5),
            "payload corruption"
        );
        let stamp = Self {
            id: read(8),
            intended: read(16),
            admitted: read(24),
        };
        ensure!(
            stamp.intended <= stamp.admitted,
            "invalid payload timestamps"
        );
        Ok(stamp)
    }
}

#[derive(Default)]
pub struct Ids {
    words: Vec<u64>,
    pub count: u64,
    pub highest: u64,
}
impl Ids {
    pub fn insert(&mut self, id: u64, limit: u64) -> Result<()> {
        ensure!(
            id < limit,
            "delivery id {id} outside configured limit {limit}"
        );
        let word = (id / 64) as usize;
        self.words.resize(self.words.len().max(word + 1), 0);
        let bit = 1 << (id % 64);
        ensure!(self.words[word] & bit == 0, "duplicate delivery id {id}");
        self.words[word] |= bit;
        self.count += 1;
        self.highest = self.highest.max(id);
        Ok(())
    }
    /// Record a delivery that may repeat after a broker failover. Returns
    /// false for a repeat.
    pub fn insert_once(&mut self, id: u64, limit: u64) -> Result<bool> {
        ensure!(id < limit, "delivery id {id} outside configured limit {limit}");
        let word = (id / 64) as usize;
        self.words.resize(self.words.len().max(word + 1), 0);
        let bit = 1 << (id % 64);
        if self.words[word] & bit != 0 {
            return Ok(false);
        }
        self.words[word] |= bit;
        self.count += 1;
        self.highest = self.highest.max(id);
        Ok(true)
    }
    /// Ids recorded here but not in `other`, with up to `examples` of them.
    pub fn missing_from(&self, other: &Ids, examples: usize) -> (u64, Vec<u64>) {
        let mut count = 0;
        let mut ids = Vec::new();
        for (i, word) in self.words.iter().enumerate() {
            let missing = word & !other.words.get(i).copied().unwrap_or(0);
            count += u64::from(missing.count_ones());
            let mut bits = missing;
            while bits != 0 && ids.len() < examples {
                ids.push(i as u64 * 64 + u64::from(bits.trailing_zeros()));
                bits &= bits - 1;
            }
        }
        (count, ids)
    }
    pub fn finish(&self, issued: u64) -> Result<()> {
        ensure!(
            self.count == issued && (issued == 0 || self.highest + 1 == issued),
            "delivery identity mismatch: unique={}, highest={}, issued={issued}",
            self.count,
            self.highest
        );
        Ok(())
    }
}

// Every message in a burst keeps the first message's intended timestamp.
pub fn burst_bounds(id: u64, pattern: &[u64]) -> (u64, u64) {
    if pattern.is_empty() { return (id, 1); }
    let cycle: u64 = pattern.iter().sum();
    let mut first = (id / cycle) * cycle;
    for &size in pattern {
        if id - first < size { return (first, size); }
        first += size;
    }
    unreachable!("validated positive burst pattern covers its cycle")
}

#[derive(Default)]
pub struct BurstTracker {
    pending: std::collections::HashMap<u64, (u64, u64)>,
}
impl BurstTracker {
    // Caller has rejected duplicate IDs. Supports reordered arrivals across
    // connections and completes only after every record in a burst arrived.
    pub fn observe(&mut self, first: u64, size: u64, received: u64) -> Result<Option<u64>> {
        let entry = self.pending.entry(first).or_default();
        entry.0 += 1;
        entry.1 = entry.1.max(received);
        ensure!(entry.0 <= size, "burst received too many records");
        if entry.0 == size {
            let last = entry.1;
            self.pending.remove(&first);
            Ok(Some(last))
        } else { Ok(None) }
    }
    pub fn finish(&self) -> Result<()> {
        ensure!(self.pending.is_empty(), "incomplete measured bursts at drain");
        Ok(())
    }
}

// Deadline is computed from the original start, including after a missed deadline.
/// A paced rate that changes over the run: points of (seconds from workload
/// start, messages per second), linear in between and flat outside them.
#[derive(Clone, Debug, Default)]
pub struct RateProfile {
    points: Vec<(f64, f64)>,
}

impl RateProfile {
    pub fn parse(items: &[String]) -> Result<Self> {
        let mut points = Vec::new();
        for item in items {
            let (t, r) = item.split_once(':').context("rate profile points look like SECONDS:RATE")?;
            let (t, r): (f64, f64) = (t.trim().parse()?, r.trim().parse()?);
            ensure!(t >= 0.0 && r > 0.0 && r <= 10_000_000.0, "rate profile point {item} out of range");
            if let Some(&(last, _)) = points.last() {
                ensure!(t > last, "rate profile times must increase");
            }
            points.push((t, r));
        }
        Ok(Self { points })
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Pieces of the rate curve as (start, end, rate at start, rate at end),
    /// with flat pieces before the first and after the last point.
    fn pieces(&self, until: f64) -> Vec<(f64, f64, f64, f64)> {
        let mut out = Vec::new();
        let (t0, r0) = self.points[0];
        if t0 > 0.0 {
            out.push((0.0, t0, r0, r0));
        }
        for w in self.points.windows(2) {
            out.push((w[0].0, w[1].0, w[0].1, w[1].1));
        }
        let (tn, rn) = *self.points.last().unwrap();
        out.push((tn, until.max(tn) + 1.0, rn, rn));
        out
    }

    /// Messages offered by time `t` seconds.
    pub fn count_at(&self, t: f64) -> f64 {
        let mut n = 0.0;
        for (a, b, ra, rb) in self.pieces(t) {
            if t <= a {
                break;
            }
            let end = t.min(b);
            let slope = (rb - ra) / (b - a);
            let d = end - a;
            n += ra * d + slope * d * d / 2.0;
        }
        n
    }

    /// When message number `n` is due, in nanoseconds from workload start.
    pub fn due_ns(&self, n: u64, until: f64) -> u64 {
        let target = n as f64;
        let mut done = 0.0;
        for (a, b, ra, rb) in self.pieces(until) {
            let len = b - a;
            let slope = (rb - ra) / len;
            let piece = ra * len + slope * len * len / 2.0;
            if done + piece >= target {
                let need = target - done;
                // Solve ra*d + slope*d^2/2 = need for d.
                let d = if slope.abs() < 1e-12 {
                    need / ra
                } else {
                    (-ra + (ra * ra + 2.0 * slope * need).max(0.0).sqrt()) / slope
                };
                return ((a + d) * 1e9) as u64;
            }
            done += piece;
        }
        (until * 1e9) as u64
    }
}

pub fn scheduled_ns(id: u64, rate: u64) -> u64 {
    ((id as u128 * 1_000_000_000) / rate as u128) as u64
}

#[cfg(test)]
mod profile_tests {
    use super::*;
    #[test]
    fn ramp_counts_and_schedules_agree() {
        let p = RateProfile::parse(&["0:100".into(), "10:1100".into(), "20:100".into()]).unwrap();
        // Area under the ramp up and down: 2 * (100 * 10 + 1000 * 10 / 2) = 12000.
        assert!((p.count_at(20.0) - 12_000.0).abs() < 1e-6);
        // First 5 s: 100 * 5 + (100 per s per s) * 5^2 / 2 = 1750.
        assert!((p.count_at(5.0) - 1_750.0).abs() < 1e-6);
        let t = p.due_ns(1_750, 20.0) as f64 / 1e9;
        assert!((t - 5.0).abs() < 1e-6, "{t}");
        // Flat after the last point.
        assert!((p.count_at(30.0) - 13_000.0).abs() < 1e-6);
    }
}

#[cfg(test)]
mod burst_tests {
    use super::*;
    #[test]
    fn burst_schedule_preserves_average_and_original_deadlines() {
        for (id, first, size) in [(0,0,200),(199,0,200),(200,200,100),(299,200,100),(300,300,200),(599,500,100)] {
            assert_eq!(burst_bounds(id, &[200,100]), (first,size));
        }
        assert_eq!(scheduled_ns(burst_bounds(299, &[200,100]).0, 100), 2_000_000_000);
        assert_eq!(scheduled_ns(burst_bounds(300, &[200,100]).0, 100), 3_000_000_000);
        assert_eq!(burst_bounds(299, &[]), (299,1));
    }
    #[test]
    fn burst_completion_waits_for_all_records_across_reordered_bursts() {
        let mut b = BurstTracker::default();
        assert_eq!(b.observe(0, 2, 30).unwrap(), None);
        assert_eq!(b.observe(2, 1, 40).unwrap(), Some(40));
        assert!(b.finish().is_err());
        assert_eq!(b.observe(0, 2, 25).unwrap(), Some(30));
        b.finish().unwrap();
    }
}

#[derive(Default, Serialize)]
pub struct Counts {
    pub issued: u64,
    pub confirmed: u64,
    pub delivered: u64,
    pub ack_sent: u64,
}

#[cfg(test)]
mod tests {
    #[test]
    fn repeats_and_losses_after_failover() {
        let mut confirmed = Ids::default();
        let mut delivered = Ids::default();
        for id in [0, 1, 2, 63, 64, 130] {
            confirmed.insert(id, 1000).unwrap();
        }
        for id in [0, 2, 64, 130, 131] {
            assert!(delivered.insert_once(id, 1000).unwrap());
        }
        assert!(!delivered.insert_once(64, 1000).unwrap());
        assert_eq!(delivered.count, 5);
        assert_eq!(confirmed.missing_from(&delivered, 20), (2, vec![1, 63]));
        assert_eq!(confirmed.missing_from(&delivered, 1), (2, vec![1]));
    }

    use super::*;
    #[test]
    fn rejects_duplicates_holes_and_corruption() {
        let mut ids = Ids::default();
        ids.insert(0, 10).unwrap();
        ids.insert(2, 10).unwrap();
        assert!(ids.insert(2, 10).is_err());
        assert!(ids.finish(3).is_err());
        ids.insert(1, 10).unwrap();
        ids.finish(3).unwrap();
        assert!(ids.insert(10, 10).is_err());
        let mut bytes = Stamp {
            id: 2,
            intended: 10,
            admitted: 30,
        }
        .encode(64);
        assert_eq!(Stamp::decode(&bytes, 64).unwrap().admitted, 30);
        bytes[40] = 0;
        assert!(Stamp::decode(&bytes, 64).is_err());
    }
    #[test]
    fn keeps_original_schedule_and_wait_in_latency() {
        assert_eq!(scheduled_ns(100_000, 100_000), 1_000_000_000);
        let intended = scheduled_ns(1, 1000);
        let admitted = 20_000_000;
        let received = 25_000_000;
        let mut admission = Latency::default();
        admission.record(admitted - intended).unwrap();
        let mut total = Latency::default();
        total.record(received - intended).unwrap();
        assert!(admission.summary()["p99_ms"].as_f64().unwrap() >= 19.0);
        assert!(total.summary()["p99_ms"].as_f64().unwrap() >= 24.0);
    }
}
