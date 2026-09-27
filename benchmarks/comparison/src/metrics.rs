use anyhow::{Result, ensure};
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
pub fn scheduled_ns(id: u64, rate: u64) -> u64 {
    ((id as u128 * 1_000_000_000) / rate as u128) as u64
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
