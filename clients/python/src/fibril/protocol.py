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

    DECLARE_PLEXUS = 62
    DECLARE_PLEXUS_OK = 63
    SUBSCRIBE_STREAM = 64

    RECONCILE_CLIENT = 70
    RECONCILE_SERVER = 71
    RECONCILE_RESULT = 72

    TOPOLOGY = 90
    TOPOLOGY_OK = 91
    REDIRECT = 92

    # Unsolicited broker->client routing push (generation changed) and the
    # client's ack of the generation it applied, so the broker can fence a
    # repartition cutover on client acks.
    TOPOLOGY_UPDATE = 101
    TOPOLOGY_UPDATE_ACK = 102

    # Server-initiated drain notice: the broker is going away for a planned
    # shutdown or upgrade. The client settles in-flight work and reconnects.
    GOING_AWAY = 103

    ERROR = 255
