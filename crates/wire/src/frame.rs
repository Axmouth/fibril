use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{io, sync::OnceLock, time::Instant};
use tokio_util::codec::{Decoder, Encoder};

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
        const HEADER: usize = 4 + 2 + 2 + 4 + 8; // 20
        if src.len() < HEADER {
            return Ok(None);
        }

        // Peek length (payload only)
        let mut peek = &src[..];
        let payload_len = peek.get_u32() as usize;
        let _version = peek.get_u16();
        let opcode = peek.get_u16();
        let recovery_limit = match opcode {
            x if x == crate::Op::InitialHistoryPrepare as u16 || x == crate::Op::InitialHistoryPrepareOk as u16 => Some(crate::MAX_INITIAL_HISTORY_FRAME_BYTES),
            x if x == crate::Op::RecoveryRead as u16 => Some(crate::MAX_RECOVERY_READ_REQUEST_BYTES),
            x if x == crate::Op::RecoveryReadOk as u16 => Some(crate::MAX_RECOVERY_READ_REPLY_BYTES),
            _ => None,
        };
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
