using System.Text;
using Fibril;

namespace Fibril.Tests;

public class CodecTest
{
    private static string Hex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();

    [Fact]
    public void FrameHeaderByteLayout()
    {
        // op=PublishOk(25=0x19), flags=0, request_id=777(0x309), payload=[0xaa,0xbb].
        var f = Frame.Build(Op.PublishOk, 777, new byte[] { 0xaa, 0xbb });
        var got = Hex(f.Encode());
        // u32 len=2, u16 ver=1, u16 op=0x0019, u32 flags=0, u64 reqid=0x0309, payload.
        var want = "00000002" + "0001" + "0019" + "00000000" + "0000000000000309" + "aabb";
        Assert.Equal(want, got);
    }

    [Fact]
    public void FrameRoundTrip()
    {
        var orig = Frame.Build(Op.Deliver, 0xdeadbeef, Encoding.ASCII.GetBytes("hello body"));
        Assert.True(Frame.TryDecode(orig.Encode(), out var dec, out var consumed));
        Assert.Equal(Frame.HeaderSize + orig.Payload.Length, consumed);
        Assert.Equal(orig.Opcode, dec.Opcode);
        Assert.Equal(orig.RequestId, dec.RequestId);
        Assert.Equal(orig.Payload, dec.Payload);
    }

    [Fact]
    public void TryDecodePartialAndMulti()
    {
        var a = Frame.Build(Op.Ping, 1, null).Encode();
        var b = Frame.Build(Op.Pong, 2, new byte[] { 9 }).Encode();

        // A header-length-minus-one prefix is incomplete.
        Assert.False(Frame.TryDecode(a.AsSpan(0, Frame.HeaderSize - 1), out _, out _));
        // A full body minus one byte is incomplete.
        Assert.False(Frame.TryDecode(b.AsSpan(0, b.Length - 1), out _, out _));

        // Two frames back to back decode one at a time from the running buffer.
        var buf = new byte[a.Length + b.Length];
        a.CopyTo(buf, 0);
        b.CopyTo(buf, a.Length);
        Assert.True(Frame.TryDecode(buf, out var f1, out var n1));
        Assert.Equal(Op.Ping, f1.Opcode);
        Assert.True(Frame.TryDecode(buf.AsSpan(n1), out var f2, out var n2));
        Assert.Equal(Op.Pong, f2.Opcode);
        Assert.Equal(new byte[] { 9 }, f2.Payload);
        Assert.Equal(0, buf.Length - n1 - n2);
    }

    [Fact]
    public void Fnv1aCanonical()
    {
        var cases = new Dictionary<string, ulong>
        {
            [""] = 14695981039346656037,
            ["a"] = 12638187200555641996,
            ["order-42"] = 9015620992513762004,
            ["partition-key"] = 11792757095719117019,
            ["hello world"] = 8618312879776256743,
        };
        foreach (var (key, want) in cases)
        {
            Assert.Equal(want, Fnv.Hash1a(Encoding.UTF8.GetBytes(key)));
        }
    }

    [Fact]
    public void WireReaderRejectsTrailingBytes()
    {
        var r = new WireReader(new byte[] { 1, 2, 3 });
        Assert.Equal(1, r.U8());
        var ex = Assert.Throws<WireException>(() => r.Finish());
        Assert.Equal(WireErrorKind.TrailingBytes, ex.Kind);
    }

    [Fact]
    public void WireReaderRejectsShortRead()
    {
        var r = new WireReader(new byte[] { 0, 0 });
        var ex = Assert.Throws<WireException>(() => r.U32());
        Assert.Equal(WireErrorKind.UnexpectedEof, ex.Kind);
    }
}
