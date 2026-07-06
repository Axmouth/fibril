using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace Fibril;

// This file is the wire codec: byte-exact encode/decode of v1 frame bodies in
// the broker's custom binary format (the authoritative layout lives in the
// protocol crate, crates/protocol/src/v1/wire.rs). It is byte-for-byte identical
// pinned by the shared vectors in clients/wire_vectors.json. Only this file and
// Frame.cs touch raw bytes.
//
// Layout (all big-endian). Integers are u8/u16/u32/u64. Length-prefixed bytes are
// a u32 length then raw bytes. Strings are length-prefixed UTF-8. A bool is a u8
// (0 or 1). A uuid is 16 raw bytes. An option[T] is a u8 tag (0 = none, 1 = some)
// then T. Each body starts with a 4-byte ASCII magic.

/// <summary>
/// An opaque 16-byte identifier. The client only echoes it back to the broker
/// and never interprets it, so it is kept as raw bytes (not a <see cref="Guid"/>,
/// whose byte layout differs) to stay byte-exact.
/// </summary>
public readonly struct Uuid : IEquatable<Uuid>
{
    private static readonly byte[] Zeros = new byte[16];
    private readonly byte[]? _bytes;

    public Uuid(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != 16)
        {
            throw new WireException(WireErrorKind.InvalidUuid, "uuid must be 16 bytes");
        }
        _bytes = bytes.ToArray();
    }

    /// <summary>The 16-byte identifier. A default (unset) value reads as all zeros.</summary>
    public ReadOnlySpan<byte> Span => _bytes ?? Zeros;

    /// <summary>An all-zero identifier.</summary>
    public static Uuid Zero => new(Zeros);

    /// <summary>A uuid whose 16 bytes are all <paramref name="b"/> (test helper for fixed ids).</summary>
    public static Uuid Fill(byte b)
    {
        Span<byte> s = stackalloc byte[16];
        s.Fill(b);
        return new Uuid(s);
    }

    public bool Equals(Uuid other) => Span.SequenceEqual(other.Span);
    public override bool Equals(object? obj) => obj is Uuid u && Equals(u);
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(Span);
        return hash.ToHashCode();
    }
}

/// <summary>The wire tag for a message content type.</summary>
public enum ContentKind : byte
{
    None = 0,
    Msgpack = 1,
    Json = 2,
    Text = 3,
    Custom = 4,
}

/// <summary>
/// A message content type: one of the well-known kinds, or a custom MIME string
/// when <see cref="Kind"/> is <see cref="ContentKind.Custom"/>.
/// </summary>
public readonly struct ContentType
{
    public ContentKind Kind { get; init; }
    public string Custom { get; init; }

    public ContentType(ContentKind kind, string custom = "")
    {
        Kind = kind;
        Custom = custom;
    }

    public static ContentType Of(ContentKind kind) => new(kind);
    public static ContentType CustomType(string mime) => new(ContentKind.Custom, mime);
}

/// <summary>Stable 64-bit FNV-1a hashing, used for partition selection.</summary>
public static class Fnv
{
    private const ulong Offset = 0xCBF29CE484222325;
    private const ulong Prime = 0x100000001B3;

    /// <summary>
    /// The 64-bit FNV-1a hash used for partition selection. Must stay
    /// byte-for-byte identical to the broker so a given key always lands on the
    /// same partition (per-key ordering).
    /// </summary>
    public static ulong Hash1a(ReadOnlySpan<byte> data)
    {
        var h = Offset;
        foreach (var b in data)
        {
            h ^= b;
            h *= Prime;
        }
        return h;
    }
}

/// <summary>
/// A growable big-endian byte writer for frame bodies. Writing never fails.
/// </summary>
public sealed class WireWriter
{
    private readonly ArrayBufferWriter<byte> _buf = new();

    public byte[] ToArray() => _buf.WrittenSpan.ToArray();

    public void U8(byte v)
    {
        var s = _buf.GetSpan(1);
        s[0] = v;
        _buf.Advance(1);
    }

    public void U16(ushort v)
    {
        var s = _buf.GetSpan(2);
        BinaryPrimitives.WriteUInt16BigEndian(s, v);
        _buf.Advance(2);
    }

    public void U32(uint v)
    {
        var s = _buf.GetSpan(4);
        BinaryPrimitives.WriteUInt32BigEndian(s, v);
        _buf.Advance(4);
    }

    public void U64(ulong v)
    {
        var s = _buf.GetSpan(8);
        BinaryPrimitives.WriteUInt64BigEndian(s, v);
        _buf.Advance(8);
    }

    public void Raw(ReadOnlySpan<byte> b)
    {
        if (b.IsEmpty)
        {
            return;
        }
        var s = _buf.GetSpan(b.Length);
        b.CopyTo(s);
        _buf.Advance(b.Length);
    }

    public void WriteBytes(ReadOnlySpan<byte> b)
    {
        U32((uint)b.Length);
        Raw(b);
    }

    public void WriteStr(string s)
    {
        // Length prefix is the UTF-8 byte count, matching the reference client.
        var byteCount = Encoding.UTF8.GetByteCount(s);
        U32((uint)byteCount);
        var dst = _buf.GetSpan(byteCount);
        Encoding.UTF8.GetBytes(s, dst);
        _buf.Advance(byteCount);
    }

    public void WriteBool(bool b) => U8(b ? (byte)1 : (byte)0);

    /// <summary>Writes a 4-byte ASCII body marker.</summary>
    public void Magic(string m)
    {
        Span<byte> s = stackalloc byte[4];
        Encoding.ASCII.GetBytes(m, s);
        Raw(s);
    }

    public void WriteUuid(Uuid u) => Raw(u.Span);

    public void OptionalStr(string? s)
    {
        if (s is null)
        {
            U8(0);
            return;
        }
        U8(1);
        WriteStr(s);
    }

    public void OptionalBytes(byte[]? b)
    {
        if (b is null)
        {
            U8(0);
            return;
        }
        U8(1);
        WriteBytes(b);
    }

    public void OptionalU32(uint? v)
    {
        if (v is null)
        {
            U8(0);
            return;
        }
        U8(1);
        U32(v.Value);
    }

    public void OptionalU64(ulong? v)
    {
        if (v is null)
        {
            U8(0);
            return;
        }
        U8(1);
        U64(v.Value);
    }

    public void OptionalUuid(Uuid? u)
    {
        if (u is null)
        {
            U8(0);
            return;
        }
        U8(1);
        WriteUuid(u.Value);
    }

    public void WriteContentType(ContentType ct)
    {
        U8((byte)ct.Kind);
        if (ct.Kind == ContentKind.Custom)
        {
            WriteStr(ct.Custom);
        }
    }

    public void WriteHeaders(IReadOnlyDictionary<string, string>? h)
    {
        var count = h?.Count ?? 0;
        U32((uint)count);
        if (h is null)
        {
            return;
        }
        // Header order is not significant to the broker, but callers that must
        // match a fixed vector pass a single-entry map (multi-entry vectors are
        // not pinned for that reason).
        foreach (var kv in h)
        {
            WriteStr(kv.Key);
            WriteStr(kv.Value);
        }
    }
}

/// <summary>
/// A big-endian byte reader with strict bounds and trailing-byte checks. Reads
/// throw <see cref="WireException"/> on malformed input.
/// </summary>
public sealed class WireReader
{
    private readonly byte[] _buf;
    private int _pos;

    public WireReader(byte[] buf)
    {
        _buf = buf;
    }

    private void Need(int n)
    {
        if (_pos + n > _buf.Length)
        {
            throw new WireException(WireErrorKind.UnexpectedEof, "wire: unexpected end of input");
        }
    }

    public byte U8()
    {
        Need(1);
        return _buf[_pos++];
    }

    public ushort U16()
    {
        Need(2);
        var v = BinaryPrimitives.ReadUInt16BigEndian(_buf.AsSpan(_pos));
        _pos += 2;
        return v;
    }

    public uint U32()
    {
        Need(4);
        var v = BinaryPrimitives.ReadUInt32BigEndian(_buf.AsSpan(_pos));
        _pos += 4;
        return v;
    }

    public ulong U64()
    {
        Need(8);
        var v = BinaryPrimitives.ReadUInt64BigEndian(_buf.AsSpan(_pos));
        _pos += 8;
        return v;
    }

    /// <summary>Returns a fresh copy of the next n bytes (does not alias the buffer).</summary>
    public byte[] RawN(int n)
    {
        Need(n);
        var outBuf = _buf.AsSpan(_pos, n).ToArray();
        _pos += n;
        return outBuf;
    }

    public byte[] ReadBytes() => RawN((int)U32());

    public string ReadStr() => Encoding.UTF8.GetString(ReadBytes());

    public bool ReadBool() => U8() != 0;

    public Uuid ReadUuid()
    {
        Need(16);
        var u = new Uuid(_buf.AsSpan(_pos, 16));
        _pos += 16;
        return u;
    }

    public string? OptionalStr() => U8() == 1 ? ReadStr() : null;

    public byte[]? OptionalBytes() => U8() == 1 ? ReadBytes() : null;

    public uint? OptionalU32() => U8() == 1 ? U32() : null;

    public ulong? OptionalU64() => U8() == 1 ? U64() : null;

    public Uuid? OptionalUuid() => U8() == 1 ? ReadUuid() : null;

    public ContentType ReadContentType()
    {
        var kind = (ContentKind)U8();
        if (kind == ContentKind.Custom)
        {
            return new ContentType(kind, ReadStr());
        }
        if ((byte)kind > (byte)ContentKind.Custom)
        {
            throw new WireException(WireErrorKind.UnknownContentType, "wire: unknown content type");
        }
        return new ContentType(kind);
    }

    public Dictionary<string, string> ReadHeaders()
    {
        var n = U32();
        var h = new Dictionary<string, string>((int)n);
        for (uint i = 0; i < n; i++)
        {
            var k = ReadStr();
            h[k] = ReadStr();
        }
        return h;
    }

    /// <summary>Reads a 4-byte body marker and checks it matches <paramref name="m"/>.</summary>
    public void ExpectMagic(string m)
    {
        var got = RawN(4);
        if (Encoding.ASCII.GetString(got) != m)
        {
            throw new WireException(WireErrorKind.InvalidMagic, "wire: bad magic, expected " + m);
        }
    }

    public int Remaining => _buf.Length - _pos;

    /// <summary>Throws a trailing-bytes error if the body was not fully consumed.</summary>
    public void Finish()
    {
        if (_pos != _buf.Length)
        {
            throw new WireException(WireErrorKind.TrailingBytes, "wire: trailing bytes");
        }
    }
}
