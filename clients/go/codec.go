package fibril

// Frame header and TCP stream framing. The fixed 20-byte header wraps every body
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

// Op is a v1 protocol opcode.
type Op uint16

const (
	OpHello    Op = 1
	OpHelloOk  Op = 2
	OpHelloErr Op = 3

	OpAuth    Op = 10
	OpAuthOk  Op = 11
	OpAuthErr Op = 12

	OpPublish        Op = 20
	OpPublishDelayed Op = 21
	OpPublishOk      Op = 25

	OpSubscribe    Op = 30
	OpSubscribeOk  Op = 31
	OpSubscribeErr Op = 32

	OpDeliver           Op = 40
	OpAck               Op = 41
	OpNack              Op = 42
	OpAssignmentChanged Op = 43

	OpPing Op = 50
	OpPong Op = 51

	OpDeclareQueue    Op = 60
	OpDeclareQueueOk  Op = 61
	OpDeclarePlexus   Op = 62
	OpDeclarePlexusOk Op = 63
	OpSubscribeStream Op = 64

	OpReconcileClient Op = 70
	OpReconcileServer Op = 71
	OpReconcileResult Op = 72

	OpTopology   Op = 90
	OpTopologyOk Op = 91
	OpRedirect   Op = 92

	OpTopologyUpdate    Op = 101
	OpTopologyUpdateAck Op = 102
	OpGoingAway         Op = 103

	OpError Op = 255
)

// Frame is a decoded protocol frame: the fixed header fields plus the body.
type Frame struct {
	Version   uint16
	Opcode    Op
	Flags     uint32
	RequestID uint64
	Payload   []byte
}

// buildFrame wraps an already-encoded body in a v1 frame for op.
func buildFrame(op Op, requestID uint64, payload []byte) Frame {
	return Frame{Version: ProtocolV1, Opcode: op, RequestID: requestID, Payload: payload}
}

// encodeFrame serializes a frame to its on-wire byte representation.
func encodeFrame(f Frame) []byte {
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
func tryDecodeFrame(buf []byte) (frame Frame, consumed int, ok bool) {
	if len(buf) < frameHeaderSize {
		return Frame{}, 0, false
	}
	payloadLen := binary.BigEndian.Uint32(buf[0:])
	total := frameHeaderSize + int(payloadLen)
	if len(buf) < total {
		return Frame{}, 0, false
	}
	f := Frame{
		Version:   binary.BigEndian.Uint16(buf[4:]),
		Opcode:    Op(binary.BigEndian.Uint16(buf[6:])),
		Flags:     binary.BigEndian.Uint32(buf[8:]),
		RequestID: binary.BigEndian.Uint64(buf[12:]),
	}
	if payloadLen > 0 {
		f.Payload = make([]byte, payloadLen)
		copy(f.Payload, buf[frameHeaderSize:total])
	}
	return f, total, true
}
