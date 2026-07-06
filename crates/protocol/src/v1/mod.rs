//! Protocol v1. The wire vocabulary and the byte-exact codec live in the
//! `fibril-wire` leaf crate and are re-exported here, so existing
//! `fibril_protocol::v1::*` paths (types, the `wire` and `frame` modules) keep
//! resolving unchanged. This module adds the broker-side connection and
//! replication logic on top of that vocabulary.

pub use fibril_wire::*;

pub mod client;
pub mod handler;
pub mod replication;
pub mod replication_stream;
