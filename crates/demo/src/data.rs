//! Shared fake-data helpers: people, places, and ids. Domain-specific pools
//! (menus, hotels, cargo tags) live with their worlds.

use fake::Fake;
use fake::faker::name::en::Name;

/// A plausible person name.
pub fn person() -> String {
    Name().fake()
}

/// A short uppercase reference id like `ORD-7K3FQ2`.
pub fn reference(prefix: &str) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    let tail: String = (0..6)
        .map(|_| ALPHABET[fastrand::usize(..ALPHABET.len())] as char)
        .collect();
    format!("{prefix}-{tail}")
}

/// Pick one entry from a pool.
pub fn pick<'a, T>(pool: &'a [T]) -> &'a T {
    &pool[fastrand::usize(..pool.len())]
}

pub const CITIES: &[&str] = &[
    "Athens",
    "Lisbon",
    "Rotterdam",
    "Marseille",
    "Gdansk",
    "Valencia",
    "Thessaloniki",
    "Hamburg",
    "Genoa",
    "Bilbao",
    "Antwerp",
    "Trieste",
];
