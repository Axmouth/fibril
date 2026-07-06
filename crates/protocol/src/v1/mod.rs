//! Protocol v1. The wire vocabulary and byte-exact codec live in the
//! `fibril-wire` crate and are re-exported here. This module adds the
//! broker-side connection and replication logic on top of that vocabulary.

pub use fibril_wire::*;

pub mod client;
pub mod handler;
pub mod replication;
pub mod replication_stream;
