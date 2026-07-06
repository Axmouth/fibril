using System.Buffers.Binary;
using System.IO;
using System.Threading.Channels;

namespace Fibril;

// Reads frames off the socket through a growable buffer, so one ReadAsync pulls
// many frames and the common case decodes a frame with no await and no syscall.
// The unbuffered alternative awaits two socket reads per frame (header, then
// payload), which caps the delivery read path well below the write path. Only the
// read loop touches a reader, so it needs no synchronization.
internal sealed class FrameReader
{
    private readonly Stream _stream;
    private byte[] _buf;
    private int _start; // first unread byte
    private int _end;   // one past the last filled byte

    public FrameReader(Stream stream, int initialCapacity = 64 * 1024)
    {
        _stream = stream;
        _buf = new byte[initialCapacity];
    }

    public async ValueTask<Frame> ReadFrameAsync(CancellationToken ct)
    {
        await EnsureAsync(Frame.HeaderSize, ct).ConfigureAwait(false);
        var header = _buf.AsSpan(_start, Frame.HeaderSize);
        var payloadLen = (int)BinaryPrimitives.ReadUInt32BigEndian(header);
        var version = BinaryPrimitives.ReadUInt16BigEndian(header.Slice(4));
        var opcode = (Op)BinaryPrimitives.ReadUInt16BigEndian(header.Slice(6));
        var flags = BinaryPrimitives.ReadUInt32BigEndian(header.Slice(8));
        var requestId = BinaryPrimitives.ReadUInt64BigEndian(header.Slice(12));

        var total = Frame.HeaderSize + payloadLen;
        await EnsureAsync(total, ct).ConfigureAwait(false);
        var payload = payloadLen > 0
            ? _buf.AsSpan(_start + Frame.HeaderSize, payloadLen).ToArray()
            : Array.Empty<byte>();
        _start += total;
        return new Frame
        {
            Version = version,
            Opcode = opcode,
            Flags = flags,
            RequestId = requestId,
            Payload = payload,
        };
    }

    // Guarantees at least `need` contiguous bytes from _start, compacting or growing
    // the buffer and refilling from the stream as needed. One ReadAsync fills as much
    // free space as the socket has ready, so a burst of frames arrives together.
    private async ValueTask EnsureAsync(int need, CancellationToken ct)
    {
        while (_end - _start < need)
        {
            if (_start + need > _buf.Length)
            {
                var avail = _end - _start;
                if (need > _buf.Length)
                {
                    var grown = new byte[Math.Max(need, _buf.Length * 2)];
                    Array.Copy(_buf, _start, grown, 0, avail);
                    _buf = grown;
                }
                else
                {
                    Array.Copy(_buf, _start, _buf, 0, avail);
                }
                _start = 0;
                _end = avail;
            }
            var n = await _stream.ReadAsync(_buf.AsMemory(_end), ct).ConfigureAwait(false);
            if (n == 0)
            {
                throw new EndOfStreamException("connection closed by peer");
            }
            _end += n;
        }
    }
}
