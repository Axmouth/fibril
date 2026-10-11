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

/// Message size limits shared by the startup seed, the runtime settings and
/// the broker connection decoder.
pub mod message_size {
    /// Default `max_message_bytes`.
    pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
    /// Smallest accepted `max_message_bytes`.
    pub const MIN_MAX_MESSAGE_BYTES: usize = 1024;
    /// Largest accepted `max_message_bytes`. Recovery inspection and transfer
    /// pages are capped at 16 MiB, so 15 MiB keeps one maximum message plus
    /// its record overhead inside a single page. A larger message could be
    /// confirmed and then never fit a recovery page after a failover.
    pub const MAX_MAX_MESSAGE_BYTES: usize = 15 * 1024 * 1024;
    /// Default `warn_message_bytes`. `0` disables the warning.
    pub const DEFAULT_WARN_MESSAGE_BYTES: usize = 1024 * 1024;
    /// Room on top of `max_message_bytes` for the topic, headers and encoding
    /// of one publish frame.
    pub const MESSAGE_FRAME_OVERHEAD_BYTES: usize = 64 * 1024;
    /// Frame cap before a connection authenticates. Enough for HELLO, AUTH
    /// and PING.
    pub const UNAUTHENTICATED_FRAME_BYTES: usize = 64 * 1024;

    /// Frame cap for ordinary opcodes on an authenticated connection.
    pub fn frame_limit(max_message_bytes: usize) -> usize {
        max_message_bytes.saturating_add(MESSAGE_FRAME_OVERHEAD_BYTES)
    }

    /// Checks `max_message_bytes` against its bounds. `field` names the
    /// setting in the error message.
    pub fn validate_max_message_bytes(field: &str, value: usize) -> Result<(), String> {
        if (MIN_MAX_MESSAGE_BYTES..=MAX_MAX_MESSAGE_BYTES).contains(&value) {
            Ok(())
        } else {
            Err(format!(
                "{field} must be between {MIN_MAX_MESSAGE_BYTES} and {MAX_MAX_MESSAGE_BYTES} bytes"
            ))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn max_message_bytes_bounds_are_inclusive() {
            for ok in [
                MIN_MAX_MESSAGE_BYTES,
                DEFAULT_MAX_MESSAGE_BYTES,
                MAX_MAX_MESSAGE_BYTES,
            ] {
                assert!(validate_max_message_bytes("x", ok).is_ok(), "{ok}");
            }
            for bad in [0, MIN_MAX_MESSAGE_BYTES - 1, MAX_MAX_MESSAGE_BYTES + 1] {
                let err = validate_max_message_bytes("x", bad).unwrap_err();
                assert!(err.starts_with("x must be between"), "{err}");
            }
        }

        #[test]
        fn largest_message_frame_fits_a_recovery_page() {
            assert!(frame_limit(MAX_MAX_MESSAGE_BYTES) <= 16 * 1024 * 1024);
        }
    }
}
