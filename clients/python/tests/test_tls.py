"""Client TLS: trust paths and the typed mismatch taxonomy.

Certificates are minted at test time with the openssl binary, so no
certificate material is ever committed to the repo.
"""

from __future__ import annotations

import asyncio
import hashlib
import ssl
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
from fake_broker import FakeBroker

from fibril import (
    Client,
    ClientOptions,
    ERR_TLS_REQUIRED,
    TlsCertificateUntrustedError,
    TlsClientCertificateRequiredError,
    TlsConfigError,
    TlsNotSupportedByBrokerError,
    TlsRequiredByBrokerError,
)
from fibril.client import _normalize_fingerprint
from fibril.codec import build_frame, encode_frame, read_frame
from fibril.frames import encode_body
from fibril.protocol import Op
from fibril import wire


def _mint_certs(tmp: Path) -> tuple[Path, Path, Path]:
    """Mint a throwaway CA and a localhost server certificate."""
    ca_key, ca_pem = tmp / "ca.key", tmp / "ca.pem"
    server_key, server_csr, server_pem = (
        tmp / "server.key",
        tmp / "server.csr",
        tmp / "server.pem",
    )
    ext = tmp / "san.cnf"
    ext.write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\n")

    def ossl(*args: str) -> None:
        subprocess.run(["openssl", *args], check=True, capture_output=True)

    ossl(
        "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
        "-keyout", str(ca_key), "-out", str(ca_pem), "-days", "2", "-nodes",
        "-subj", "/CN=Fibril Py Test CA",
    )
    ossl(
        "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
        "-keyout", str(server_key), "-out", str(server_csr), "-nodes",
        "-subj", "/CN=localhost",
    )
    ossl(
        "x509", "-req", "-in", str(server_csr), "-CA", str(ca_pem),
        "-CAkey", str(ca_key), "-CAcreateserial", "-out", str(server_pem),
        "-days", "2", "-extfile", str(ext),
    )
    return ca_pem, server_pem, server_key


def _server_context(
    server_pem: Path,
    server_key: Path,
    ca_pem: Path,
    require_client_cert: bool = False,
) -> ssl.SSLContext:
    # Serve leaf + CA like the real broker's generated mode.
    chain = server_pem.read_text() + ca_pem.read_text()
    chain_file = server_pem.parent / "chain.pem"
    chain_file.write_text(chain)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=str(chain_file), keyfile=str(server_key))
    if require_client_cert:
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cafile=str(ca_pem))
    return context


def _mint_client_cert(tmp: Path, identity: str) -> tuple[Path, Path]:
    """Mint a client certificate from the test CA, identity as CN."""
    key, csr, pem = tmp / f"{identity}.key", tmp / f"{identity}.csr", tmp / f"{identity}.pem"

    def ossl(*args: str) -> None:
        subprocess.run(["openssl", *args], check=True, capture_output=True)

    ossl(
        "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
        "-keyout", str(key), "-out", str(csr), "-nodes", "-subj", f"/CN={identity}",
    )
    ossl(
        "x509", "-req", "-in", str(csr), "-CA", str(tmp / "ca.pem"),
        "-CAkey", str(tmp / "ca.key"), "-CAcreateserial", "-out", str(pem), "-days", "2",
    )
    return pem, key


def _leaf_fingerprint(server_pem: Path) -> str:
    der = ssl.PEM_cert_to_DER_cert(server_pem.read_text())
    return hashlib.sha256(der).hexdigest()


@pytest.fixture
def certs(tmp_path: Path) -> tuple[Path, Path, Path]:
    return _mint_certs(tmp_path)


async def test_tls_ca_path_handshake(certs: tuple[Path, Path, Path]) -> None:
    ca_pem, server_pem, server_key = certs
    broker = FakeBroker(ssl_context=_server_context(server_pem, server_key, ca_pem))
    await broker.start()
    try:
        client = await Client.connect(
            ("127.0.0.1", broker.port),
            ClientOptions()
            .with_tls_ca_path(str(ca_pem))
            .with_tls_server_name("localhost"),
        )
        await client.shutdown()
    finally:
        await broker.stop()


async def test_tls_leaf_fingerprint_pin(certs: tuple[Path, Path, Path]) -> None:
    ca_pem, server_pem, server_key = certs
    broker = FakeBroker(ssl_context=_server_context(server_pem, server_key, ca_pem))
    await broker.start()
    try:
        client = await Client.connect(
            ("127.0.0.1", broker.port),
            ClientOptions().with_tls_ca_fingerprint(_leaf_fingerprint(server_pem)),
        )
        await client.shutdown()

        with pytest.raises(TlsCertificateUntrustedError):
            await Client.connect(
                ("127.0.0.1", broker.port),
                ClientOptions().with_tls_ca_fingerprint("00" * 32),
            )
    finally:
        await broker.stop()


@pytest.mark.skipif(
    sys.version_info < (3, 13),
    reason="CA pin needs the presented chain, exposed from Python 3.13",
)
async def test_tls_ca_fingerprint_pin_matches_chain(
    certs: tuple[Path, Path, Path],
) -> None:
    ca_pem, server_pem, server_key = certs
    broker = FakeBroker(ssl_context=_server_context(server_pem, server_key, ca_pem))
    await broker.start()
    try:
        der = ssl.PEM_cert_to_DER_cert(ca_pem.read_text())
        client = await Client.connect(
            ("127.0.0.1", broker.port),
            ClientOptions().with_tls_ca_fingerprint(hashlib.sha256(der).hexdigest()),
        )
        await client.shutdown()
    finally:
        await broker.stop()


async def test_untrusted_cert_is_a_trust_error(certs: tuple[Path, Path, Path]) -> None:
    ca_pem, server_pem, server_key = certs
    broker = FakeBroker(ssl_context=_server_context(server_pem, server_key, ca_pem))
    await broker.start()
    try:
        # OS roots cannot know the throwaway CA.
        with pytest.raises(TlsCertificateUntrustedError):
            await Client.connect(
                ("127.0.0.1", broker.port),
                ClientOptions().with_tls().with_tls_server_name("localhost"),
            )
    finally:
        await broker.stop()


async def test_plaintext_client_maps_the_426_reply() -> None:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        frame = await read_frame(reader)
        # What the real TLS listener does on sniffing a plaintext HELLO.
        body = encode_body(Op.ERROR, wire.ErrorMsg(ERR_TLS_REQUIRED, "requires TLS"))
        writer.write(encode_frame(build_frame(Op.ERROR, frame.request_id, body)))
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        with pytest.raises(TlsRequiredByBrokerError):
            await Client.connect(("127.0.0.1", port), ClientOptions())
    finally:
        server.close()
        await server.wait_closed()


async def test_tls_client_against_plaintext_listener_fails_fast() -> None:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        # What the real plaintext listener does on sniffing a ClientHello.
        prefix = await reader.read(2)
        if prefix[:2] == b"\x16\x03":
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        with pytest.raises(TlsNotSupportedByBrokerError):
            await Client.connect(
                ("127.0.0.1", port),
                ClientOptions().with_tls_ca_fingerprint("11" * 32),
            )
    finally:
        server.close()
        await server.wait_closed()


async def test_client_certificate_passes_a_requiring_broker(
    certs: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    ca_pem, server_pem, server_key = certs
    cert_pem, cert_key = _mint_client_cert(tmp_path, "svc-a")
    broker = FakeBroker(
        ssl_context=_server_context(server_pem, server_key, ca_pem, require_client_cert=True)
    )
    await broker.start()
    try:
        client = await Client.connect(
            ("localhost", broker.port),
            ClientOptions()
            .with_tls_ca_path(str(ca_pem))
            .with_tls_client_cert(str(cert_pem), str(cert_key)),
        )
        await client.shutdown()
    finally:
        await broker.stop()


async def test_certless_client_gets_the_required_certificate_error(
    certs: tuple[Path, Path, Path],
) -> None:
    ca_pem, server_pem, server_key = certs
    broker = FakeBroker(
        ssl_context=_server_context(server_pem, server_key, ca_pem, require_client_cert=True)
    )
    await broker.start()
    try:
        with pytest.raises(TlsClientCertificateRequiredError):
            await Client.connect(
                ("localhost", broker.port),
                ClientOptions().with_tls_ca_path(str(ca_pem)),
            )
    finally:
        await broker.stop()


def test_fingerprint_normalization() -> None:
    bare = "aabbccddeeff00112233445566778899" * 2
    colon = ":".join(bare[i : i + 2] for i in range(0, 64, 2)).upper()
    assert _normalize_fingerprint(bare) == _normalize_fingerprint(colon)
    with pytest.raises(TlsConfigError):
        _normalize_fingerprint("abcd")
    with pytest.raises(TlsConfigError):
        _normalize_fingerprint("zz" * 32)
