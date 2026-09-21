//! Protocol v1. The wire vocabulary and byte-exact codec live in the
//! `fibril-wire` crate and are re-exported here. This module adds the
//! broker-side connection and replication logic on top of that vocabulary.

pub use fibril_wire::*;

pub mod client;
mod connection_writer;
pub mod handler;
pub mod replication;
pub mod recovery_inspection;
pub mod initial_history;
pub mod history_replication;
pub mod replication_stream;
pub mod session_store;
