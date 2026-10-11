//! Message size enforcement on broker connections: 413 replies for oversized
//! requests and the rate limited warning for large payloads.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use fibril_wire::{
    ERR_TOO_LARGE, ErrorMsg, Op,
    frame::{Frame, OversizedFrame},
    helper::{ProtocolResult, error_frame, try_encode},
};

/// How often the large message warning may repeat for one topic.
const WARN_INTERVAL: Duration = Duration::from_secs(60);
/// Topics tracked by the warning limiter. Once full, expired entries are
/// pruned. If every entry is still fresh, new topics are not warned about
/// until one expires, which keeps memory bounded under many topics.
const WARN_TOPICS_MAX: usize = 1024;

const LARGE_PAYLOAD_ADVICE: &str = "Store large payloads elsewhere, for example in object \
     storage, and publish a reference to them";

/// Per broker limiter for the large message warning: at most one warning per
/// topic per minute.
#[derive(Debug, Default)]
pub struct LargeMessageWarnings {
    last_warned: Mutex<HashMap<String, Instant>>,
}

impl LargeMessageWarnings {
    /// Logs a warning when `payload_len` is above `warn_bytes`, unless this
    /// topic was warned about within the last minute. `warn_bytes == 0`
    /// disables the warning. Returns whether a warning was logged.
    pub fn check(&self, topic: &str, payload_len: usize, warn_bytes: usize) -> bool {
        if warn_bytes == 0 || payload_len <= warn_bytes {
            return false;
        }
        if !self.should_warn(topic, Instant::now()) {
            return false;
        }
        tracing::warn!(
            topic,
            payload_bytes = payload_len,
            warn_message_bytes = warn_bytes,
            "large message: a {payload_len} byte payload on topic `{topic}` is above the \
             {warn_bytes} byte warning threshold (warn_message_bytes). Large messages slow \
             replication and recovery. {LARGE_PAYLOAD_ADVICE}. Logged at most once a minute \
             per topic"
        );
        true
    }

    fn should_warn(&self, topic: &str, now: Instant) -> bool {
        let mut last = self
            .last_warned
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(at) = last.get(topic) {
            if now.saturating_duration_since(*at) < WARN_INTERVAL {
                return false;
            }
        } else if last.len() >= WARN_TOPICS_MAX {
            last.retain(|_, at| now.saturating_duration_since(*at) < WARN_INTERVAL);
            if last.len() >= WARN_TOPICS_MAX {
                return false;
            }
        }
        last.insert(topic.to_owned(), now);
        true
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.last_warned.lock().unwrap().len()
    }
}

/// Error text for a decoded publish whose payload is over the limit.
pub fn payload_too_large_message(payload_len: usize, max_message_bytes: usize) -> String {
    format!(
        "message payload of {payload_len} bytes exceeds the broker limit of {max_message_bytes} \
         bytes (max_message_bytes). {LARGE_PAYLOAD_ADVICE}"
    )
}

/// Error text for a frame the decoder skipped because it was over the cap.
pub fn frame_too_large_message(frame: &OversizedFrame, max_message_bytes: usize) -> String {
    format!(
        "request of {} bytes exceeds the broker limit of {} bytes (max_message_bytes is \
         {max_message_bytes}, plus room for the topic and headers). {LARGE_PAYLOAD_ADVICE}",
        frame.payload_len, frame.limit
    )
}

/// The 413 reply for a skipped frame, in the shape the client expects for
/// that request.
pub fn frame_too_large_reply(
    frame: &OversizedFrame,
    max_message_bytes: usize,
) -> ProtocolResult<Frame> {
    let message = frame_too_large_message(frame, max_message_bytes);
    let error_op = match Op::from_code(frame.opcode) {
        Some(Op::Subscribe | Op::SubscribeStream) => Some(Op::SubscribeErr),
        Some(Op::Auth) => Some(Op::AuthErr),
        _ => None,
    };
    match error_op {
        Some(op) => try_encode(
            op,
            frame.request_id,
            &ErrorMsg {
                code: ERR_TOO_LARGE,
                message,
            },
        ),
        None => error_frame(frame.request_id, ERR_TOO_LARGE, message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fibril_wire::helper::try_decode;

    #[test]
    fn warning_respects_threshold_and_zero_disables() {
        let warnings = LargeMessageWarnings::default();
        assert!(!warnings.check("t", 100, 0));
        assert!(!warnings.check("t", 100, 100));
        assert!(warnings.check("t", 101, 100));
    }

    #[test]
    fn warning_is_logged_once_per_topic_per_interval() {
        let warnings = LargeMessageWarnings::default();
        let start = Instant::now();
        assert!(warnings.should_warn("a", start));
        assert!(!warnings.should_warn("a", start + Duration::from_secs(59)));
        assert!(warnings.should_warn("b", start + Duration::from_secs(1)));
        assert!(warnings.should_warn("a", start + WARN_INTERVAL));
    }

    #[test]
    fn warning_limiter_stays_bounded() {
        let warnings = LargeMessageWarnings::default();
        let start = Instant::now();
        for i in 0..WARN_TOPICS_MAX {
            assert!(warnings.should_warn(&format!("t{i}"), start));
        }
        // Full of fresh entries: a new topic is skipped, not tracked.
        assert!(!warnings.should_warn("extra", start));
        assert_eq!(warnings.tracked(), WARN_TOPICS_MAX);
        // Once entries expire they are pruned and new topics warn again.
        assert!(warnings.should_warn("extra", start + WARN_INTERVAL));
        assert_eq!(warnings.tracked(), 1);
    }

    fn oversized(op: Op) -> OversizedFrame {
        OversizedFrame {
            version: 1,
            opcode: op as u16,
            request_id: 42,
            payload_len: 20_000_000,
            limit: 8 * 1024 * 1024 + 64 * 1024,
        }
    }

    #[test]
    fn too_large_reply_uses_the_op_the_client_expects() {
        for (op, reply) in [
            (Op::Publish, Op::Error),
            (Op::PublishDelayed, Op::Error),
            (Op::DeclareQueue, Op::Error),
            (Op::Subscribe, Op::SubscribeErr),
            (Op::SubscribeStream, Op::SubscribeErr),
            (Op::Auth, Op::AuthErr),
        ] {
            let frame = frame_too_large_reply(&oversized(op), 8 * 1024 * 1024).unwrap();
            assert_eq!(frame.opcode, reply as u16, "{op:?}");
            assert_eq!(frame.request_id, 42);
            let err: ErrorMsg = try_decode(&frame).unwrap();
            assert_eq!(err.code, ERR_TOO_LARGE);
            assert!(err.message.contains("20000000 bytes"), "{}", err.message);
            assert!(err.message.contains("8454144 bytes"), "{}", err.message);
            assert!(err.message.contains("reference"), "{}", err.message);
        }
    }
}
