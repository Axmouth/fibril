"""Scaffold smoke test: the package imports and the opcode table is intact."""

from __future__ import annotations

import fibril
from fibril.protocol import COMPLIANCE_STRING, PROTOCOL_V1, Op


def test_package_imports() -> None:
    assert fibril.__version__
    assert fibril.PROTOCOL_V1 == 1


def test_opcodes_match_protocol() -> None:
    # A few anchor values guarding against accidental renumbering.
    assert Op.HELLO == 1
    assert Op.PUBLISH == 20
    assert Op.ASSIGNMENT_CHANGED == 43
    assert Op.ERROR == 255


def test_compliance_marker_preserved() -> None:
    assert COMPLIANCE_STRING == "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md"
    assert PROTOCOL_V1 == 1
