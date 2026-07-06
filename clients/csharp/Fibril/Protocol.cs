using System.Runtime.CompilerServices;

// The wire DTOs and codec are internal. The conformance tests reach them here.
[assembly: InternalsVisibleTo("Fibril.Tests")]

namespace Fibril;

/// <summary>
/// Protocol constants carried in the v1 handshake. The compliance marker and
/// protocol version are byte-exact with the broker (the authoritative layout lives
/// in the protocol crate, crates/protocol/src/v1).
/// </summary>
public static class Protocol
{
    /// <summary>The v1 wire protocol version.</summary>
    public const ushort V1 = 1;

    /// <summary>The compliance marker offered in the handshake.</summary>
    public const string ComplianceString = "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md";
}
