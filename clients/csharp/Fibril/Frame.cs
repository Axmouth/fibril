using System.Buffers.Binary;

namespace Fibril;

/// <summary>
/// A v1 protocol opcode.
/// </summary>
public enum Op : ushort
{
    Hello = 1,
    HelloOk = 2,
    HelloErr = 3,

    Auth = 10,
    AuthOk = 11,
    AuthErr = 12,

    Publish = 20,
    PublishDelayed = 21,
    PublishOk = 25,

    Subscribe = 30,
    SubscribeOk = 31,
    SubscribeErr = 32,

    Deliver = 40,
    Ack = 41,
    Nack = 42,
    AssignmentChanged = 43,

    Ping = 50,
    Pong = 51,

    DeclareQueue = 60,
    DeclareQueueOk = 61,
    DeclarePlexus = 62,
    DeclarePlexusOk = 63,
    SubscribeStream = 64,

    ReconcileClient = 70,
    ReconcileServer = 71,
    ReconcileResult = 72,

    Topology = 90,
    TopologyOk = 91,
    Redirect = 92,

    TopologyUpdate = 101,
    TopologyUpdateAck = 102,
    GoingAway = 103,
    SubscriptionClosed = 104,

    Error = 255,
}

/// <summary>
/// A decoded protocol frame: the fixed header fields plus the body payload.
/// </summary>
/// <remarks>
/// The fixed 20-byte header wraps every body encoded by the wire codec (all
/// big-endian):
/// <code>
/// u32 payload_len
/// u16 version
/// u16 opcode
/// u32 flags
/// u64 request_id
/// bytes payload[payload_len]
/// </code>
/// Byte-exact with the broker's frame format (crates/protocol/src/v1/frame.rs).
/// </remarks>
public readonly struct Frame
{
    public const int HeaderSize = 20;

    public ushort Version { get; init; }
    public Op Opcode { get; init; }
    public uint Flags { get; init; }
    public ulong RequestId { get; init; }
    public byte[] Payload { get; init; }

    /// <summary>Wraps an already-encoded body in a v1 frame for <paramref name="op"/>.</summary>
    public static Frame Build(Op op, ulong requestId, byte[]? payload) => new()
    {
        Version = Protocol.V1,
        Opcode = op,
        Flags = 0,
        RequestId = requestId,
        Payload = payload ?? Array.Empty<byte>(),
    };

    /// <summary>Serializes this frame to its on-wire byte representation.</summary>
    public byte[] Encode()
    {
        var payload = Payload ?? Array.Empty<byte>();
        var outBuf = new byte[HeaderSize + payload.Length];
        var span = outBuf.AsSpan();
        BinaryPrimitives.WriteUInt32BigEndian(span[0..], (uint)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(span[4..], Version);
        BinaryPrimitives.WriteUInt16BigEndian(span[6..], (ushort)Opcode);
        BinaryPrimitives.WriteUInt32BigEndian(span[8..], Flags);
        BinaryPrimitives.WriteUInt64BigEndian(span[12..], RequestId);
        payload.CopyTo(span[HeaderSize..]);
        return outBuf;
    }

    /// <summary>
    /// Decodes one frame from the head of <paramref name="buf"/>. Returns the frame
    /// and the number of bytes consumed, or <c>false</c> when <paramref name="buf"/>
    /// does not yet hold a full frame (the caller reads more and retries). The
    /// payload is copied, so it does not alias <paramref name="buf"/>.
    /// </summary>
    public static bool TryDecode(ReadOnlySpan<byte> buf, out Frame frame, out int consumed)
    {
        frame = default;
        consumed = 0;
        if (buf.Length < HeaderSize)
        {
            return false;
        }
        var payloadLen = BinaryPrimitives.ReadUInt32BigEndian(buf[0..]);
        var total = HeaderSize + (int)payloadLen;
        if (buf.Length < total)
        {
            return false;
        }
        var payload = payloadLen > 0 ? buf[HeaderSize..total].ToArray() : Array.Empty<byte>();
        frame = new Frame
        {
            Version = BinaryPrimitives.ReadUInt16BigEndian(buf[4..]),
            Opcode = (Op)BinaryPrimitives.ReadUInt16BigEndian(buf[6..]),
            Flags = BinaryPrimitives.ReadUInt32BigEndian(buf[8..]),
            RequestId = BinaryPrimitives.ReadUInt64BigEndian(buf[12..]),
            Payload = payload,
        };
        consumed = total;
        return true;
    }
}
