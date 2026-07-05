// Package fibril is a Go client for the Fibril message broker.
//
// This file is the wire codec: byte-exact encode/decode of v1 frame bodies in
// the broker's custom binary format (the authoritative layout lives in the
// protocol crate, crates/protocol/src/v1/wire.rs). It is byte-for-byte identical
// across every client and pinned by the shared vectors in
// clients/wire_vectors.json. This is the only file that touches raw bytes.
//
// Layout (all big-endian): u8/u16/u32/u64 integers; length-prefixed bytes (u32
// length then raw bytes); strings are length-prefixed UTF-8; bool is a u8 (0 or
// 1); uuid is 16 raw bytes; option[T] is a u8 tag (0 = none, 1 = some) then T;
// each body starts with a 4-byte ASCII magic.
package fibril

import "encoding/binary"

// UUID is an opaque 16-byte identifier. The client only echoes it back to the
// broker and never interprets it.
type UUID [16]byte

// Headers is the user-visible header map carried on a message.
type Headers map[string]string

// ---- partition hashing (FNV-1a 64-bit) ---------------------------------

const (
	fnvOffset uint64 = 0xCBF29CE484222325
	fnvPrime  uint64 = 0x100000001B3
)

// FNV1a is the stable 64-bit FNV-1a hash used for partition selection. It must
// stay byte-for-byte identical to the broker and every other client so a given
// key always lands on the same partition (per-key ordering).
func FNV1a(data []byte) uint64 {
	h := fnvOffset
	for _, b := range data {
		h ^= uint64(b)
		h *= fnvPrime
	}
	return h
}

// ---- content type ------------------------------------------------------

// ContentKind is the wire tag for a message content type.
type ContentKind uint8

const (
	ContentNone    ContentKind = 0
	ContentMsgpack ContentKind = 1
	ContentJSON    ContentKind = 2
	ContentText    ContentKind = 3
	ContentCustom  ContentKind = 4
)

// ContentType is a message content type: one of the well-known kinds, or a
// custom MIME string when Kind is ContentCustom.
type ContentType struct {
	Kind   ContentKind
	Custom string
}

// ---- byte writer -------------------------------------------------------

// writer is a growable big-endian byte writer. Writing never fails, so its
// methods take no error; a bad uuid length is a programmer error surfaced by the
// caller before encoding.
type writer struct {
	buf []byte
}

func (w *writer) u8(v uint8)   { w.buf = append(w.buf, v) }
func (w *writer) u16(v uint16) { w.buf = binary.BigEndian.AppendUint16(w.buf, v) }
func (w *writer) u32(v uint32) { w.buf = binary.BigEndian.AppendUint32(w.buf, v) }
func (w *writer) u64(v uint64) { w.buf = binary.BigEndian.AppendUint64(w.buf, v) }
func (w *writer) raw(b []byte) { w.buf = append(w.buf, b...) }

func (w *writer) writeBytes(b []byte) {
	w.u32(uint32(len(b)))
	w.buf = append(w.buf, b...)
}

func (w *writer) writeStr(s string) {
	w.u32(uint32(len(s)))
	w.buf = append(w.buf, s...)
}

func (w *writer) writeBool(b bool) {
	if b {
		w.u8(1)
	} else {
		w.u8(0)
	}
}

// magic writes a 4-byte ASCII body marker.
func (w *writer) magic(m string) { w.buf = append(w.buf, m...) }

func (w *writer) uuid(u UUID) { w.buf = append(w.buf, u[:]...) }

func (w *writer) optionalStr(s *string) {
	if s == nil {
		w.u8(0)
		return
	}
	w.u8(1)
	w.writeStr(*s)
}

func (w *writer) optionalBytes(b []byte) {
	if b == nil {
		w.u8(0)
		return
	}
	w.u8(1)
	w.writeBytes(b)
}

func (w *writer) optionalU32(v *uint32) {
	if v == nil {
		w.u8(0)
		return
	}
	w.u8(1)
	w.u32(*v)
}

func (w *writer) optionalU64(v *uint64) {
	if v == nil {
		w.u8(0)
		return
	}
	w.u8(1)
	w.u64(*v)
}

func (w *writer) optionalUUID(u *UUID) {
	if u == nil {
		w.u8(0)
		return
	}
	w.u8(1)
	w.uuid(*u)
}

func (w *writer) contentType(ct ContentType) {
	w.u8(uint8(ct.Kind))
	if ct.Kind == ContentCustom {
		w.writeStr(ct.Custom)
	}
}

func (w *writer) headers(h Headers) {
	w.u32(uint32(len(h)))
	// Header order is not significant to the broker, but a stable order keeps
	// encoding deterministic. Callers that must match a fixed vector pass a map
	// with a single entry (multi-entry vectors are not pinned for that reason).
	for k, v := range h {
		w.writeStr(k)
		w.writeStr(v)
	}
}

// ---- byte reader -------------------------------------------------------

// reader is a big-endian byte reader with strict bounds and trailing-byte
// checks. It carries a sticky error: once a read fails, later reads are no-ops
// and the accumulated error surfaces at finish(). This keeps decode functions
// readable (no error check per field) while still being safe.
type reader struct {
	buf []byte
	pos int
	err error
}

func (r *reader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if r.pos+n > len(r.buf) {
		r.err = &WireError{Kind: WireUnexpectedEOF, Message: "wire: unexpected end of input"}
		return false
	}
	return true
}

func (r *reader) u8() uint8 {
	if !r.need(1) {
		return 0
	}
	v := r.buf[r.pos]
	r.pos++
	return v
}

func (r *reader) u16() uint16 {
	if !r.need(2) {
		return 0
	}
	v := binary.BigEndian.Uint16(r.buf[r.pos:])
	r.pos += 2
	return v
}

func (r *reader) u32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v
}

func (r *reader) u64() uint64 {
	if !r.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	return v
}

// rawN returns a fresh copy of the next n bytes, so the result does not alias
// the (possibly reused) input buffer.
func (r *reader) rawN(n int) []byte {
	if !r.need(n) {
		return nil
	}
	out := make([]byte, n)
	copy(out, r.buf[r.pos:r.pos+n])
	r.pos += n
	return out
}

func (r *reader) readBytes() []byte {
	return r.rawN(int(r.u32()))
}

func (r *reader) readStr() string {
	return string(r.readBytes())
}

func (r *reader) readBool() bool {
	return r.u8() != 0
}

func (r *reader) uuid() UUID {
	var u UUID
	if !r.need(16) {
		return u
	}
	copy(u[:], r.buf[r.pos:r.pos+16])
	r.pos += 16
	return u
}

func (r *reader) optionalStr() *string {
	if r.u8() == 1 {
		s := r.readStr()
		return &s
	}
	return nil
}

func (r *reader) optionalBytes() []byte {
	if r.u8() == 1 {
		return r.readBytes()
	}
	return nil
}

func (r *reader) optionalU32() *uint32 {
	if r.u8() == 1 {
		v := r.u32()
		return &v
	}
	return nil
}

func (r *reader) optionalU64() *uint64 {
	if r.u8() == 1 {
		v := r.u64()
		return &v
	}
	return nil
}

func (r *reader) optionalUUID() *UUID {
	if r.u8() == 1 {
		v := r.uuid()
		return &v
	}
	return nil
}

func (r *reader) contentType() ContentType {
	kind := ContentKind(r.u8())
	if kind == ContentCustom {
		return ContentType{Kind: kind, Custom: r.readStr()}
	}
	if kind > ContentCustom {
		if r.err == nil {
			r.err = &WireError{Kind: WireUnknownContentType, Message: "wire: unknown content type"}
		}
		return ContentType{}
	}
	return ContentType{Kind: kind}
}

func (r *reader) headers() Headers {
	n := r.u32()
	h := make(Headers, n)
	for i := uint32(0); i < n && r.err == nil; i++ {
		k := r.readStr()
		h[k] = r.readStr()
	}
	return h
}

// expectMagic reads a 4-byte body marker and checks it matches m.
func (r *reader) expectMagic(m string) {
	got := r.rawN(4)
	if r.err != nil {
		return
	}
	if string(got) != m {
		r.err = &WireError{Kind: WireInvalidMagic, Message: "wire: bad magic, expected " + m}
	}
}

func (r *reader) remaining() int {
	return len(r.buf) - r.pos
}

// finish reports any accumulated read error, or a trailing-bytes error if the
// body was not fully consumed.
func (r *reader) finish() error {
	if r.err != nil {
		return r.err
	}
	if r.pos != len(r.buf) {
		return &WireError{Kind: WireTrailingBytes, Message: "wire: trailing bytes"}
	}
	return nil
}
