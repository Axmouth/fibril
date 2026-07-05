package fibril

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"
)

func TestFrameHeaderByteLayout(t *testing.T) {
	// op=PublishOk(25=0x19), flags=0, request_id=777(0x309), payload=[0xaa,0xbb].
	f := BuildFrame(OpPublishOk, 777, []byte{0xaa, 0xbb})
	got := hex.EncodeToString(EncodeFrame(f))
	// u32 len=2, u16 ver=1, u16 op=0x0019, u32 flags=0, u64 reqid=0x0309, payload.
	want := "00000002" + "0001" + "0019" + "00000000" + "0000000000000309" + "aabb"
	if got != want {
		t.Errorf("frame header layout\n got: %s\nwant: %s", got, want)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	orig := BuildFrame(OpDeliver, 0xdeadbeef, []byte("hello body"))
	dec, consumed, ok := TryDecodeFrame(EncodeFrame(orig))
	if !ok {
		t.Fatal("TryDecodeFrame reported incomplete on a whole frame")
	}
	if consumed != frameHeaderSize+len(orig.Payload) {
		t.Errorf("consumed = %d, want %d", consumed, frameHeaderSize+len(orig.Payload))
	}
	if !reflect.DeepEqual(dec, orig) {
		t.Errorf("round-trip mismatch\n got: %+v\nwant: %+v", dec, orig)
	}
}

func TestTryDecodeFramePartialAndMulti(t *testing.T) {
	a := EncodeFrame(BuildFrame(OpPing, 1, nil))
	b := EncodeFrame(BuildFrame(OpPong, 2, []byte{9}))

	// A header-length-minus-one prefix is incomplete.
	if _, _, ok := TryDecodeFrame(a[:frameHeaderSize-1]); ok {
		t.Error("expected incomplete on a short header")
	}
	// A full body minus one byte is incomplete.
	if _, _, ok := TryDecodeFrame(b[:len(b)-1]); ok {
		t.Error("expected incomplete on a truncated body")
	}

	// Two frames back to back decode one at a time from the running buffer.
	buf := append(append([]byte{}, a...), b...)
	f1, n1, ok := TryDecodeFrame(buf)
	if !ok || f1.Opcode != OpPing {
		t.Fatalf("first frame decode failed: ok=%v op=%d", ok, f1.Opcode)
	}
	buf = buf[n1:]
	f2, n2, ok := TryDecodeFrame(buf)
	if !ok || f2.Opcode != OpPong || !bytes.Equal(f2.Payload, []byte{9}) {
		t.Fatalf("second frame decode failed: ok=%v op=%d payload=%v", ok, f2.Opcode, f2.Payload)
	}
	if len(buf[n2:]) != 0 {
		t.Errorf("expected buffer fully consumed, %d bytes left", len(buf[n2:]))
	}
}
