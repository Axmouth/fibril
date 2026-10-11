use bytes::{Buf, BufMut, Bytes, BytesMut};
use fibril_common::message_size::UNAUTHENTICATED_FRAME_BYTES;
use std::{
    io,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};
use tokio_util::codec::{Decoder, Encoder};

const HEADER: usize = 4 + 2 + 2 + 4 + 8; // 20

#[derive(Debug, Clone)]
pub struct Frame {
    pub version: u16,
    pub opcode: u16,
    pub flags: u32,
    pub request_id: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
pub struct ProtoCodec;

impl Decoder for ProtoCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, io::Error> {
        if src.len() < HEADER {
            return Ok(None);
        }

        // Peek length (payload only)
        let (payload_len, opcode) = peek_header(src);
        let recovery_limit = explicit_payload_limit(opcode);
        if recovery_limit.is_some_and(|limit| payload_len > limit) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recovery frame exceeds size limit",
            ));
        }

        if src.len() < HEADER + payload_len {
            return Ok(None);
        }

        // Now consume
        let payload_len = src.get_u32() as usize;
        let version = src.get_u16();
        let opcode = src.get_u16();
        let flags = src.get_u32();
        let request_id = src.get_u64();

        let payload = src.split_to(payload_len).freeze();

        Ok(Some(Frame {
            version,
            opcode,
            flags,
            request_id,
            payload,
        }))
    }
}

/// Payload length and opcode from a buffered frame header.
fn peek_header(src: &[u8]) -> (usize, u16) {
    let mut peek = src;
    let payload_len = peek.get_u32() as usize;
    let _version = peek.get_u16();
    let opcode = peek.get_u16();
    (payload_len, opcode)
}

/// Opcodes with their own payload cap, checked from the header before the
/// body is buffered. Going over one is a protocol violation that ends the
/// connection.
pub fn explicit_payload_limit(opcode: u16) -> Option<usize> {
    match opcode {
        x if x == crate::Op::RecoveryTransfer as u16 || x == crate::Op::RecoveryTransferOk as u16 => Some(crate::MAX_RECOVERY_TRANSFER_FRAME_BYTES),
        x if x == crate::Op::HistoryReplication as u16 => Some(crate::MAX_HISTORY_REPLICATION_FRAME_BYTES),
        x if x == crate::Op::InitialHistoryPrepare as u16 || x == crate::Op::InitialHistoryPrepareOk as u16 => Some(crate::MAX_INITIAL_HISTORY_FRAME_BYTES),
        x if x == crate::Op::RecoveryRead as u16 => Some(crate::MAX_RECOVERY_READ_REQUEST_BYTES),
        x if x == crate::Op::RecoveryReadOk as u16 => Some(crate::MAX_RECOVERY_READ_REPLY_BYTES),
        _ => None,
    }
}

/// Frame size state shared by a broker connection's decoder and its handler.
///
/// Before authentication every frame is capped at
/// [`UNAUTHENTICATED_FRAME_BYTES`] and an oversized one ends the connection.
/// After authentication ordinary opcodes are capped at `max_frame_bytes` and
/// an oversized one is skipped and reported as [`ServerFrame::TooLarge`].
/// Opcodes with an [`explicit_payload_limit`] keep that limit.
#[derive(Debug, Clone)]
pub struct FrameLimit {
    inner: Arc<FrameLimitState>,
}

#[derive(Debug)]
struct FrameLimitState {
    authenticated: AtomicBool,
    max_frame_bytes: AtomicUsize,
}

impl FrameLimit {
    pub fn new(authenticated: bool, max_frame_bytes: usize) -> Self {
        Self {
            inner: Arc::new(FrameLimitState {
                authenticated: AtomicBool::new(authenticated),
                max_frame_bytes: AtomicUsize::new(max_frame_bytes),
            }),
        }
    }

    pub fn authenticate(&self) {
        self.inner.authenticated.store(true, Ordering::Release);
    }

    pub fn is_authenticated(&self) -> bool {
        self.inner.authenticated.load(Ordering::Acquire)
    }

    pub fn set_max_frame_bytes(&self, max_frame_bytes: usize) {
        self.inner
            .max_frame_bytes
            .store(max_frame_bytes, Ordering::Release);
    }

    pub fn max_frame_bytes(&self) -> usize {
        self.inner.max_frame_bytes.load(Ordering::Acquire)
    }
}

/// An ordinary frame whose declared payload was over the connection cap. The
/// payload is discarded as it arrives and never buffered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OversizedFrame {
    pub version: u16,
    pub opcode: u16,
    pub request_id: u64,
    pub payload_len: usize,
    pub limit: usize,
}

/// Item decoded by [`ServerCodec`]. Only the decoder builds `TooLarge`, so a
/// peer cannot forge it with frame flags or payload bytes.
#[derive(Debug, Clone)]
pub enum ServerFrame {
    Frame(Frame),
    TooLarge(OversizedFrame),
}

/// Broker side codec. Same wire format as [`ProtoCodec`], plus the size caps
/// described on [`FrameLimit`].
#[derive(Debug)]
pub struct ServerCodec {
    limit: FrameLimit,
    discard_remaining: usize,
}

impl ServerCodec {
    pub fn new(limit: FrameLimit) -> Self {
        Self {
            limit,
            discard_remaining: 0,
        }
    }

    fn discard(&mut self, src: &mut BytesMut) {
        let n = self.discard_remaining.min(src.len());
        src.advance(n);
        self.discard_remaining -= n;
    }
}

impl Decoder for ServerCodec {
    type Item = ServerFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<ServerFrame>, io::Error> {
        if self.discard_remaining > 0 {
            self.discard(src);
            if self.discard_remaining > 0 {
                return Ok(None);
            }
        }
        if src.len() < HEADER {
            return Ok(None);
        }
        let (payload_len, opcode) = peek_header(src);
        if !self.limit.is_authenticated() {
            if payload_len > UNAUTHENTICATED_FRAME_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "frame of {payload_len} bytes exceeds the {UNAUTHENTICATED_FRAME_BYTES} \
                         byte limit before authentication"
                    ),
                ));
            }
        } else if explicit_payload_limit(opcode).is_none() {
            let limit = self.limit.max_frame_bytes();
            if payload_len > limit {
                // Consume the header and drop the payload as it arrives.
                let _payload_len = src.get_u32();
                let version = src.get_u16();
                let _opcode = src.get_u16();
                let _flags = src.get_u32();
                let request_id = src.get_u64();
                self.discard_remaining = payload_len;
                self.discard(src);
                return Ok(Some(ServerFrame::TooLarge(OversizedFrame {
                    version,
                    opcode,
                    request_id,
                    payload_len,
                    limit,
                })));
            }
        }
        Ok(ProtoCodec.decode(src)?.map(ServerFrame::Frame))
    }
}

impl Encoder<Frame> for ServerCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), io::Error> {
        ProtoCodec.encode(item, dst)
    }
}

impl Encoder<Frame> for ProtoCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), io::Error> {
        let started = Instant::now();
        let opcode = item.opcode;
        let request_id = item.request_id;
        let payload_len = item.payload.len();
        dst.reserve(20 + payload_len);

        dst.put_u32(payload_len as u32);
        dst.put_u16(item.version);
        dst.put_u16(item.opcode);
        dst.put_u32(item.flags);
        dst.put_u64(item.request_id);
        dst.extend_from_slice(&item.payload);
        log_frame_encode_timing(opcode, request_id, payload_len, started.elapsed());

        Ok(())
    }
}

fn log_frame_encode_timing(
    opcode: u16,
    request_id: u64,
    payload_len: usize,
    elapsed: std::time::Duration,
) {
    const LARGE_FRAME_BYTES: usize = 1 << 20;
    const SLOW_FRAME_MICROS: u128 = 5_000;

    let slow = elapsed.as_micros() >= SLOW_FRAME_MICROS;
    let detailed_large = protocol_frame_timing_enabled() && payload_len >= LARGE_FRAME_BYTES;
    if !slow && !detailed_large {
        return;
    }

    tracing::info!(
        stage = "frame_encode",
        pid = std::process::id(),
        opcode,
        request_id,
        payload_len,
        elapsed_us = elapsed.as_micros(),
        "protocol frame codec timing"
    );
}

fn protocol_frame_timing_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        matches!(
            std::env::var("FIBRIL_PROTOCOL_CODEC_TIMING").as_deref(),
            Ok("1") | Ok("true") | Ok("yes") | Ok("on")
        )
    })
}

#[cfg(test)]
mod recovery_frame_tests {
    use super::*;

    #[test]
    fn oversized_recovery_frame_is_refused_before_body_arrives() {
        for (op, limit) in [
            (crate::Op::RecoveryTransfer, crate::MAX_RECOVERY_TRANSFER_FRAME_BYTES),
            (crate::Op::RecoveryTransferOk, crate::MAX_RECOVERY_TRANSFER_FRAME_BYTES),
            (crate::Op::HistoryReplication, crate::MAX_HISTORY_REPLICATION_FRAME_BYTES),
            (crate::Op::InitialHistoryPrepare, crate::MAX_INITIAL_HISTORY_FRAME_BYTES),
            (crate::Op::InitialHistoryPrepareOk, crate::MAX_INITIAL_HISTORY_FRAME_BYTES),
            (
                crate::Op::RecoveryRead,
                crate::MAX_RECOVERY_READ_REQUEST_BYTES,
            ),
            (
                crate::Op::RecoveryReadOk,
                crate::MAX_RECOVERY_READ_REPLY_BYTES,
            ),
        ] {
            let mut header = BytesMut::new();
            header.put_u32((limit + 1) as u32);
            header.put_u16(1);
            header.put_u16(op as u16);
            header.put_u32(0);
            header.put_u64(3);
            assert_eq!(header.len(), 20);
            assert_eq!(
                ProtoCodec.decode(&mut header).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            header[..4].copy_from_slice(&(limit as u32).to_be_bytes());
            assert!(ProtoCodec.decode(&mut header).unwrap().is_none());
        }
    }
}

#[cfg(test)]
mod server_codec_tests {
    use super::*;

    const CAP: usize = 1024;

    fn frame(opcode: u16, flags: u32, request_id: u64, payload: &[u8]) -> Frame {
        Frame {
            version: 1,
            opcode,
            flags,
            request_id,
            payload: Bytes::copy_from_slice(payload),
        }
    }

    fn encoded(frames: &[Frame]) -> BytesMut {
        let mut buf = BytesMut::new();
        for f in frames {
            ProtoCodec.encode(f.clone(), &mut buf).unwrap();
        }
        buf
    }

    fn header_only(opcode: u16, request_id: u64, payload_len: usize) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u32(payload_len as u32);
        buf.put_u16(1);
        buf.put_u16(opcode);
        buf.put_u32(0);
        buf.put_u64(request_id);
        buf
    }

    fn expect_frame(item: Option<ServerFrame>) -> Frame {
        match item {
            Some(ServerFrame::Frame(f)) => f,
            other => panic!("expected a frame, got {other:?}"),
        }
    }

    fn expect_too_large(item: Option<ServerFrame>) -> OversizedFrame {
        match item {
            Some(ServerFrame::TooLarge(o)) => o,
            other => panic!("expected an oversized marker, got {other:?}"),
        }
    }

    #[test]
    fn oversized_frame_before_auth_is_an_error() {
        let mut codec = ServerCodec::new(FrameLimit::new(false, CAP));
        let mut buf = header_only(crate::Op::Hello as u16, 1, UNAUTHENTICATED_FRAME_BYTES + 1);
        assert_eq!(
            codec.decode(&mut buf).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        // Frames up to the unauthenticated cap decode normally, even past the
        // post-auth cap.
        let mut codec = ServerCodec::new(FrameLimit::new(false, CAP));
        let big = vec![7u8; UNAUTHENTICATED_FRAME_BYTES];
        let mut buf = encoded(&[frame(crate::Op::Auth as u16, 0, 2, &big)]);
        let decoded = expect_frame(codec.decode(&mut buf).unwrap());
        assert_eq!(decoded.payload.len(), big.len());
    }

    #[test]
    fn unauthenticated_cap_also_applies_to_explicit_limit_opcodes() {
        let mut codec = ServerCodec::new(FrameLimit::new(false, CAP));
        let mut buf = header_only(
            crate::Op::RecoveryTransfer as u16,
            1,
            UNAUTHENTICATED_FRAME_BYTES + 1,
        );
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn oversized_frame_after_auth_yields_marker_and_skips_payload() {
        let limit = FrameLimit::new(false, CAP);
        let mut codec = ServerCodec::new(limit.clone());
        limit.authenticate();
        let big = vec![0xAB; CAP + 1];
        let mut buf = encoded(&[
            frame(crate::Op::Publish as u16, 0, 9, &big),
            frame(crate::Op::Publish as u16, 0, 10, b"next"),
        ]);

        let marker = expect_too_large(codec.decode(&mut buf).unwrap());
        assert_eq!(
            marker,
            OversizedFrame {
                version: 1,
                opcode: crate::Op::Publish as u16,
                request_id: 9,
                payload_len: CAP + 1,
                limit: CAP,
            }
        );
        let next = expect_frame(codec.decode(&mut buf).unwrap());
        assert_eq!(next.request_id, 10);
        assert_eq!(&next.payload[..], b"next");
        assert!(buf.is_empty());
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn oversized_payload_split_across_reads_is_discarded_in_bounded_memory() {
        let mut codec = ServerCodec::new(FrameLimit::new(true, CAP));
        let big = vec![0x55; CAP * 4];
        let all = encoded(&[
            frame(crate::Op::Publish as u16, 0, 1, &big),
            frame(crate::Op::Ping as u16, 0, 2, b""),
        ]);

        let mut buf = BytesMut::new();
        let mut items = Vec::new();
        for chunk in all.chunks(7) {
            buf.extend_from_slice(chunk);
            while let Some(item) = codec.decode(&mut buf).unwrap() {
                items.push(item);
            }
            // Never holds more than one header plus one read worth of bytes.
            assert!(buf.len() < HEADER + 7, "buffered {}", buf.len());
        }
        assert_eq!(items.len(), 2);
        assert_eq!(expect_too_large(Some(items[0].clone())).request_id, 1);
        let ping = expect_frame(Some(items[1].clone()));
        assert_eq!(ping.opcode, crate::Op::Ping as u16);
        assert_eq!(ping.request_id, 2);
    }

    #[test]
    fn frame_at_the_cap_decodes_normally() {
        let mut codec = ServerCodec::new(FrameLimit::new(true, CAP));
        let payload = vec![1u8; CAP];
        let mut buf = encoded(&[frame(crate::Op::Publish as u16, 0, 3, &payload)]);
        let decoded = expect_frame(codec.decode(&mut buf).unwrap());
        assert_eq!(decoded.payload.len(), CAP);
    }

    #[test]
    fn cap_changes_apply_to_the_next_frame() {
        let limit = FrameLimit::new(true, CAP);
        let mut codec = ServerCodec::new(limit.clone());
        let payload = vec![1u8; CAP + 1];
        let mut buf = encoded(&[frame(crate::Op::Publish as u16, 0, 3, &payload)]);
        limit.set_max_frame_bytes(CAP * 2);
        expect_frame(codec.decode(&mut buf).unwrap());
    }

    #[test]
    fn explicit_limit_opcodes_keep_their_limits_after_auth() {
        for (op, op_limit) in [
            (crate::Op::RecoveryTransfer, crate::MAX_RECOVERY_TRANSFER_FRAME_BYTES),
            (crate::Op::RecoveryTransferOk, crate::MAX_RECOVERY_TRANSFER_FRAME_BYTES),
            (crate::Op::HistoryReplication, crate::MAX_HISTORY_REPLICATION_FRAME_BYTES),
            (crate::Op::InitialHistoryPrepare, crate::MAX_INITIAL_HISTORY_FRAME_BYTES),
            (crate::Op::InitialHistoryPrepareOk, crate::MAX_INITIAL_HISTORY_FRAME_BYTES),
            (crate::Op::RecoveryRead, crate::MAX_RECOVERY_READ_REQUEST_BYTES),
            (crate::Op::RecoveryReadOk, crate::MAX_RECOVERY_READ_REPLY_BYTES),
        ] {
            let mut codec = ServerCodec::new(FrameLimit::new(true, CAP));
            // Over the general cap but within the opcode limit: waits for the
            // body instead of skipping it.
            let mut buf = header_only(op as u16, 4, op_limit);
            assert!(codec.decode(&mut buf).unwrap().is_none(), "{op:?}");
            assert_eq!(buf.len(), HEADER, "{op:?}");
            // Over the opcode limit: a protocol error, as with ProtoCodec.
            let mut buf = header_only(op as u16, 4, op_limit + 1);
            assert_eq!(
                codec.decode(&mut buf).unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "{op:?}"
            );
        }
    }

    #[test]
    fn client_cannot_forge_the_marker() {
        // Every flag bit set and a payload that is itself an oversized frame
        // header still decode as an ordinary frame.
        let mut codec = ServerCodec::new(FrameLimit::new(true, CAP));
        let inner = header_only(crate::Op::Publish as u16, 5, CAP * 10);
        let mut buf = encoded(&[frame(crate::Op::Publish as u16, u32::MAX, 5, &inner)]);
        let f = expect_frame(codec.decode(&mut buf).unwrap());
        assert_eq!(f.flags, u32::MAX);
        assert_eq!(&f.payload[..], &inner[..]);
    }

    #[test]
    fn server_codec_encodes_like_proto_codec() {
        let f = frame(crate::Op::PublishOk as u16, 2, 77, b"ok");
        let mut a = BytesMut::new();
        let mut b = BytesMut::new();
        ProtoCodec.encode(f.clone(), &mut a).unwrap();
        ServerCodec::new(FrameLimit::new(true, CAP))
            .encode(f, &mut b)
            .unwrap();
        assert_eq!(a, b);
    }
}
