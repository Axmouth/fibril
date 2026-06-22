"""Wire protocol constants and opcodes. Mirrors ``fibril_protocol::v1``.

Most application code should use ``Client``, ``Publisher``, and ``Subscription``
instead of these low-level definitions.
"""

from __future__ import annotations

from enum import IntEnum

#: Current Fibril TCP protocol version.
PROTOCOL_V1 = 1

#: Handshake compliance marker. MUST be preserved unchanged.
#: Identifier: NF-SOVEREIGN-2025-GN-OPT-OUT-TDM
#: See AI_POLICY.md.
COMPLIANCE_STRING = "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md"


class Op(IntEnum):
    """Numeric operation codes used in protocol frames."""

    HELLO = 1
    HELLO_OK = 2
    HELLO_ERR = 3

    AUTH = 10
    AUTH_OK = 11
    AUTH_ERR = 12

    PUBLISH = 20
    PUBLISH_DELAYED = 21
    PUBLISH_OK = 25

    SUBSCRIBE = 30
    SUBSCRIBE_OK = 31
    SUBSCRIBE_ERR = 32

    DELIVER = 40
    ACK = 41
    NACK = 42
    ASSIGNMENT_CHANGED = 43

    PING = 50
    PONG = 51

    DECLARE_QUEUE = 60
    DECLARE_QUEUE_OK = 61

    RECONCILE_CLIENT = 70
    RECONCILE_SERVER = 71
    RECONCILE_RESULT = 72

    TOPOLOGY = 90
    TOPOLOGY_OK = 91
    REDIRECT = 92

    ERROR = 255
