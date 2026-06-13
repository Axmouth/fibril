//! Value types that originate at the fibril (broker) layer and above.
//!
//! Engine-layer types (`Partition`, `Offset`, `Topic`, `Group`) live in
//! `stroma-common`; this crate holds fibril-and-up concepts. Kept
//! dependency-light (just `serde`) so the network client can use these without
//! pulling in the broker/engine stack.

use serde::{Deserialize, Serialize};

/// A broker delivery tag, used to settle (ack/nack) a delivered message.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DeliveryTag {
    pub epoch: u64,
}
