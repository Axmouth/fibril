package fibril

// frame header and TCP stream framing. The fixed 20-byte header wraps every body
// encoded by the wire codec:
//
//	u32 payload_len
//	u16 version
//	u16 opcode
//	u32 flags
//	u64 request_id
//	bytes payload[payload_len]
//
// Byte-exact with the broker's frame format (crates/protocol/src/v1/frame.rs)
// and every other client.

import "encoding/binary"

// Protocol constants carried in the handshake.
const (
	ProtocolV1       uint16 = 1
	ComplianceString        = "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md"

	frameHeaderSize = 20
)

// op is a v1 protocol opcode.
type op uint16

const (
	opHello    op = 1
	opHelloOk  op = 2
	opHelloErr op = 3

	opAuth    op = 10
	opAuthOk  op = 11
	opAuthErr op = 12

	opPublish        op = 20
	opPublishDelayed op = 21
	opPublishOk      op = 25

	opSubscribe    op = 30
	opSubscribeOk  op = 31
	opSubscribeErr op = 32

	opDeliver           op = 40
	opAck               op = 41
	opNack              op = 42
	opAssignmentChanged op = 43

	opPing op = 50
	opPong op = 51

	opDeclareQueue    op = 60
	opDeclareQueueOk  op = 61
	opDeclarePlexus   op = 62
	opDeclarePlexusOk op = 63
	opSubscribeStream op = 64

	opReconcileClient op = 70
	opReconcileServer op = 71
	opReconcileResult op = 72

	opTopology   op = 90
	opTopologyOk op = 91
	opRedirect   op = 92

	opTopologyUpdate    op = 101
	opTopologyUpdateAck op = 102
	opGoingAway         op = 103

	opError op = 255
)

// frame is a decoded protocol frame: the fixed header fields plus the body.
type frame struct {
	Version   uint16
	Opcode    op
	Flags     uint32
	RequestID uint64
	Payload   []byte
}

// buildFrame wraps an already-encoded body in a v1 frame for op.
func buildFrame(op op, requestID uint64, payload []byte) frame {
	return frame{Version: ProtocolV1, Opcode: op, RequestID: requestID, Payload: payload}
}

// encodeFrame serializes a frame to its on-wire byte representation.
func encodeFrame(f frame) []byte {
	out := make([]byte, frameHeaderSize+len(f.Payload))
	binary.BigEndian.PutUint32(out[0:], uint32(len(f.Payload)))
	binary.BigEndian.PutUint16(out[4:], f.Version)
	binary.BigEndian.PutUint16(out[6:], uint16(f.Opcode))
	binary.BigEndian.PutUint32(out[8:], f.Flags)
	binary.BigEndian.PutUint64(out[12:], f.RequestID)
	copy(out[frameHeaderSize:], f.Payload)
	return out
}

// tryDecodeFrame decodes one frame from the head of buf. It returns the frame
// and the number of bytes consumed, or ok=false when buf does not yet hold a
// full frame (the caller reads more and retries). The payload is copied, so it
// does not alias buf.
func tryDecodeFrame(buf []byte) (frame, int, bool) {
	if len(buf) < frameHeaderSize {
		return frame{}, 0, false
	}
	payloadLen := binary.BigEndian.Uint32(buf[0:])
	total := frameHeaderSize + int(payloadLen)
	if len(buf) < total {
		return frame{}, 0, false
	}
	f := frame{
		Version:   binary.BigEndian.Uint16(buf[4:]),
		Opcode:    op(binary.BigEndian.Uint16(buf[6:])),
		Flags:     binary.BigEndian.Uint32(buf[8:]),
		RequestID: binary.BigEndian.Uint64(buf[12:]),
	}
	if payloadLen > 0 {
		f.Payload = make([]byte, payloadLen)
		copy(f.Payload, buf[frameHeaderSize:total])
	}
	return f, total, true
}
