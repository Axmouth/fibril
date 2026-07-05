"""Cluster-aware client: connection pool, topology cache, and routing.

Mirrors the Rust client (``crates/client``): one bootstrap connection plus a
pool of lazily opened connections to non-bootstrap owners, a routing cache warmed
by topology fetches and point-updated by redirects, bounded redirect-follow, and
auto-reconnect. The engine exposes async request methods directly, so this layer
calls them without an intermediate command channel.
"""

from __future__ import annotations

import asyncio
import hashlib
import socket as _socket
import ssl
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Callable, Optional, Union

from . import wire
from .engine import (
    DEFAULT_WRITE_COALESCE_BYTES,
    DEFAULT_WRITE_COALESCE_COUNT,
    DEFAULT_WRITE_COALESCE_WINDOW_S,
    Engine,
    EngineOptions,
    SubscribeResult,
    SubscriptionRegistry,
)
from .errors import (
    BrokenPipeError,
    DisconnectionError,
    EofError,
    FibrilError,
    TlsCertificateUntrustedError,
    TlsConfigError,
    TlsClientCertificateRequiredError,
    TlsHandshakeError,
    TlsNotSupportedByBrokerError,
    retry_advice,
)
from .internal.bounded_queue import BoundedQueue
from .internal.topology import Route, RoundRobin, TopologyCache, route_partition

Address = Union[str, tuple[str, int]]
AssignmentHandler = Callable[[wire.AssignmentChanged], None]
GoingAwayHandler = Callable[[wire.GoingAway], None]

DEFAULT_CLIENT_NAME = "Fibril Python Client"
DEFAULT_CLIENT_VERSION = "0.4.0"


def normalize_group(group: Optional[str]) -> Optional[str]:
    trimmed = group.strip() if group else ""
    if not trimmed or trimmed == "default":
        return None
    return trimmed


@dataclass
class TlsOptions:
    """TLS options for connecting to a TLS-enabled broker.

    Trust resolution order: ``ca_fingerprint`` pin if set, else ``ca_path``
    roots, else the OS trust store (for brokers with publicly issued
    certificates).
    """

    #: PEM file with the CA certificate(s) to trust, e.g. the broker's
    #: generated ``<data_dir>/tls/ca.pem``.
    ca_path: Optional[str] = None
    #: SHA-256 fingerprint of the broker CA (or server) certificate, as
    #: printed in the broker startup log. Hex digits, colons optional.
    #: Pinning replaces chain-of-trust verification, the handshake still
    #: proves possession of the certificate key. On Python older than 3.13
    #: only the leaf certificate is visible to the pin check, so pin the
    #: server certificate there or use ``ca_path``.
    ca_fingerprint: Optional[str] = None
    #: Name verified against the certificate (and sent as SNI). Defaults to
    #: the host part of the connect address.
    server_name: Optional[str] = None
    #: PEM client certificate chain presented to the broker, for
    #: ``tls.client_auth`` deployments. Set together with ``client_key_path``.
    client_cert_path: Optional[str] = None
    #: PEM private key for the client certificate.
    client_key_path: Optional[str] = None


@dataclass
class ClientOptions:
    """Connection and routing settings. Use ``with_*`` for copies with overrides."""

    client_name: str = DEFAULT_CLIENT_NAME
    client_version: str = DEFAULT_CLIENT_VERSION
    auth: Optional[wire.Auth] = None
    heartbeat_interval_seconds: float = 5.0
    resume_identity: Optional[wire.ResumeIdentity] = None
    auto_reconnect_attempts: int = 1
    reconnect_reconcile_policy: wire.ReconcilePolicy = "conservative"
    max_redirects: int = 3
    publish_timeout_ms: int = 30_000
    topology_refresh_cooldown_ms: int = 1_000
    supervise_subscriptions: bool = True
    subscription_supervise_interval_ms: int = 1_000
    tls: Optional[TlsOptions] = None
    #: Fire-and-forget writes (unconfirmed publishes) coalesce into one socket
    #: write, flushed on whichever comes first: these many buffered bytes, this
    #: many buffered frames, or ``write_coalesce_window_ms`` since the last flush.
    #: Tune with :meth:`with_write_coalescing`.
    write_coalesce_bytes: int = DEFAULT_WRITE_COALESCE_BYTES
    write_coalesce_count: int = DEFAULT_WRITE_COALESCE_COUNT
    write_coalesce_window_ms: float = DEFAULT_WRITE_COALESCE_WINDOW_S * 1000

    def with_auth(self, username: str, password: str) -> "ClientOptions":
        return replace(self, auth=wire.Auth(username=username, password=password))

    def with_write_coalescing(
        self,
        *,
        max_bytes: Optional[int] = None,
        max_frames: Optional[int] = None,
        window_ms: Optional[float] = None,
    ) -> "ClientOptions":
        """Tune coalescing of fire-and-forget writes (unconfirmed publishes).

        Such frames are buffered and sent in one socket write, flushed on
        whichever limit is reached first: ``max_bytes`` buffered, ``max_frames``
        buffered, or ``window_ms`` since the last flush. Reply-bearing frames
        (confirmed publishes, acks, requests) always flush immediately. Larger
        limits trade a little latency for fewer syscalls. The defaults already sit
        at the throughput plateau, so this is mainly for tightening latency or
        memory, or disabling coalescing (``max_frames=1``). Only the limits you
        pass change.
        """
        result = self
        if max_bytes is not None:
            if max_bytes < 1:
                raise ValueError("max_bytes must be >= 1")
            result = replace(result, write_coalesce_bytes=max_bytes)
        if max_frames is not None:
            if max_frames < 1:
                raise ValueError("max_frames must be >= 1")
            result = replace(result, write_coalesce_count=max_frames)
        if window_ms is not None:
            if window_ms < 0:
                raise ValueError("window_ms must be >= 0")
            result = replace(result, write_coalesce_window_ms=window_ms)
        return result

    def with_tls(self) -> "ClientOptions":
        """TLS with the OS trust store, for publicly issued broker certificates."""
        return replace(self, tls=self.tls or TlsOptions())

    def with_tls_ca_path(self, ca_path: str) -> "ClientOptions":
        """TLS trusting the CA certificate(s) in a PEM file, e.g. the broker's
        generated ``<data_dir>/tls/ca.pem``."""
        return replace(self, tls=replace(self.tls or TlsOptions(), ca_path=str(ca_path)))

    def with_tls_ca_fingerprint(self, ca_fingerprint: str) -> "ClientOptions":
        """TLS pinning the broker certificate by the SHA-256 fingerprint printed
        in the broker startup log (colons optional)."""
        return replace(
            self, tls=replace(self.tls or TlsOptions(), ca_fingerprint=ca_fingerprint)
        )

    def with_tls_server_name(self, server_name: str) -> "ClientOptions":
        """TLS with an explicit certificate name to verify (and send as SNI),
        when the connect address is not the name on the certificate."""
        return replace(self, tls=replace(self.tls or TlsOptions(), server_name=server_name))

    def with_tls_client_cert(self, cert_path: str, key_path: str) -> "ClientOptions":
        """Present a client certificate (PEM chain and key) to the broker,
        for ``tls.client_auth`` deployments. Enables TLS if not already
        enabled."""
        return replace(
            self,
            tls=replace(
                self.tls or TlsOptions(),
                client_cert_path=str(cert_path),
                client_key_path=str(key_path),
            ),
        )

    def with_heartbeat_interval(self, seconds: float) -> "ClientOptions":
        return replace(self, heartbeat_interval_seconds=seconds)

    def with_resume_identity(self, resume_identity: wire.ResumeIdentity) -> "ClientOptions":
        return replace(self, resume_identity=resume_identity)

    def disable_auto_reconnect(self) -> "ClientOptions":
        return replace(self, auto_reconnect_attempts=0)

    def engine_options(
        self, resume_identity: Optional[wire.ResumeIdentity] = None
    ) -> EngineOptions:
        return EngineOptions(
            client_name=self.client_name,
            client_version=self.client_version,
            auth=self.auth,
            resume_identity=resume_identity if resume_identity is not None else self.resume_identity,
            reconnect_reconcile_policy=self.reconnect_reconcile_policy,
            heartbeat_interval_seconds=self.heartbeat_interval_seconds,
            write_coalesce_bytes=self.write_coalesce_bytes,
            write_coalesce_count=self.write_coalesce_count,
            write_coalesce_window_s=self.write_coalesce_window_ms / 1000,
        )

    async def connect(self, address: Address) -> "Client":
        return await Client.connect(address, self)


@dataclass
class QueueConfig:
    """Queue declaration for retry and dead-letter behavior. Immutable builder."""

    topic: str
    group_name: Optional[str] = None
    dlq_policy: Optional[wire.QueueDlqPolicy] = None
    dlq_max_retries: Optional[int] = None
    default_message_ttl_ms: Optional[int] = None

    def group(self, group: str) -> "QueueConfig":
        return replace(self, group_name=normalize_group(group))

    def max_retries(self, n: int) -> "QueueConfig":
        return replace(self, dlq_max_retries=n)

    def default_message_ttl(self, ttl: Union[float, timedelta]) -> "QueueConfig":
        """Default per-message TTL (seconds or timedelta): messages without their
        own TTL drop after this age. Per-message expiry, not queue expiration."""
        seconds = ttl.total_seconds() if isinstance(ttl, timedelta) else float(ttl)
        if seconds < 0:
            raise ValueError("ttl must be non-negative")
        return replace(self, default_message_ttl_ms=int(seconds * 1000))

    def discard_dead_letters(self) -> "QueueConfig":
        return replace(self, dlq_policy="discard")

    def use_global_dead_letter_queue(self) -> "QueueConfig":
        return replace(self, dlq_policy="global")

    def custom_dead_letter_queue(
        self, topic: str, group: Optional[str] = None
    ) -> "QueueConfig":
        return replace(
            self, dlq_policy=wire.CustomDlqPolicy(topic=topic, group=normalize_group(group))
        )

    def to_wire(self) -> wire.DeclareQueue:
        return wire.DeclareQueue(
            topic=self.topic,
            group=self.group_name,
            dlq_policy=self.dlq_policy,
            dlq_max_retries=self.dlq_max_retries,
            default_message_ttl_ms=self.default_message_ttl_ms,
        )


@dataclass
class StreamConfig:
    """Plexus (fan-out stream) declaration. Immutable builder.

    A stream delivers every record to every consumer (unlike a queue). Partitions
    buy write throughput and per-key ordering, not consumer work-sharing.

    Durability tiers trade latency for durability: ``durable`` fsyncs before
    delivering and confirming, ``speculative`` delivers early and defers the
    confirm until durable, and ``ephemeral`` delivers and confirms without an
    fsync.
    """

    topic: str
    partition_count: Optional[int] = None
    durability: wire.StreamDurability = "durable"
    retention: wire.StreamRetention = field(default_factory=wire.StreamRetention)
    replication_factor: Optional[int] = None

    def partitions(self, count: int) -> "StreamConfig":
        if count < 1:
            raise ValueError("partitions must be a positive integer")
        return replace(self, partition_count=count)

    def ephemeral(self) -> "StreamConfig":
        return replace(self, durability="ephemeral")

    def speculative(self) -> "StreamConfig":
        return replace(self, durability="speculative")

    def durable(self) -> "StreamConfig":
        return replace(self, durability="durable")

    def retain_for(self, age: Union[float, timedelta]) -> "StreamConfig":
        """Drop records older than this age (seconds or timedelta)."""
        seconds = age.total_seconds() if isinstance(age, timedelta) else float(age)
        if seconds < 0:
            raise ValueError("age must be non-negative")
        return replace(self, retention=replace(self.retention, max_age_ms=int(seconds * 1000)))

    def retain_bytes(self, max_bytes: int) -> "StreamConfig":
        return replace(self, retention=replace(self.retention, max_bytes=max_bytes))

    def retain_records(self, max_records: int) -> "StreamConfig":
        return replace(self, retention=replace(self.retention, max_records=max_records))

    def replicas(self, replication_factor: int) -> "StreamConfig":
        """Per-stream durable-tier replication factor (follower count).

        When unset, the cluster default applies. Only the durable tier
        replicates; the express tiers stay owner-only. ``0`` makes a durable
        stream owner-only (durable on disk, not highly available).
        """
        if replication_factor < 0:
            raise ValueError("replication_factor must be non-negative")
        return replace(self, replication_factor=replication_factor)

    def to_wire(self) -> wire.DeclarePlexus:
        return wire.DeclarePlexus(
            topic=self.topic,
            partition_count=self.partition_count,
            durability=self.durability,
            retention=self.retention,
            replication_factor=self.replication_factor,
        )


@dataclass
class SubscribeHandle:
    """A routed subscription connection: the engine plus its delivery queue."""

    engine: Engine
    queue: BoundedQueue[object]


@dataclass
class ReconnectOutcome:
    resume_outcome: wire.ResumeOutcome


def parse_address(address: Address) -> tuple[str, int]:
    if isinstance(address, tuple):
        return address
    if address.startswith("[") and "]:" in address:  # [ipv6]:port
        host, _, port = address[1:].partition("]:")
        return host, int(port)
    idx = address.rfind(":")
    if idx == -1:
        raise DisconnectionError(f"invalid address (no port): {address}")
    return address[:idx], int(address[idx + 1 :])


def _endpoint_key(host: str, port: int) -> str:
    return f"{host}:{port}"


def _normalize_fingerprint(raw: str) -> bytes:
    hex_digits = "".join(ch for ch in raw if ch not in ": ").lower()
    if len(hex_digits) != 64 or any(c not in "0123456789abcdef" for c in hex_digits):
        raise TlsConfigError(
            "tls ca_fingerprint must be 64 hex digits (SHA-256, colons optional)"
        )
    return bytes.fromhex(hex_digits)


def _certificate_required_error(
    err: BaseException, tls: Optional[TlsOptions]
) -> Optional[TlsClientCertificateRequiredError]:
    """With TLS 1.3 the client side of the handshake completes before the
    broker's client-certificate verdict, so a ``require`` rejection lands
    after connect instead of in the handshake itself. asyncio flattens the
    broker's certificate-required alert into a clean EOF, so a certless TLS
    connect that ends with no reply to HELLO is attributed to the
    certificate requirement - the one broker behavior that produces exactly
    this shape."""
    if tls is None:
        return None
    message = (
        "the broker likely requires a client certificate "
        "(tls.client_auth = require): provide one with "
        "with_tls_client_cert(cert_path, key_path). Deployment-CA "
        "certificates are issued with fibrilctl cert issue"
    )
    if "certificate required" in str(err).lower():
        return TlsClientCertificateRequiredError(message)
    if tls.client_cert_path is None and isinstance(err, EofError):
        return TlsClientCertificateRequiredError(message)
    return None


def _build_ssl_context(tls: TlsOptions) -> ssl.SSLContext:
    if tls.ca_fingerprint is not None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        # The pin replaces chain-of-trust verification; the match happens
        # after the handshake, which still proves key possession.
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    elif tls.ca_path is not None:
        try:
            context = ssl.create_default_context(cafile=tls.ca_path)
        except (OSError, ssl.SSLError) as err:
            raise TlsConfigError(f"failed to load tls ca_path {tls.ca_path}: {err}") from err
    else:
        context = ssl.create_default_context()
    if (tls.client_cert_path is None) != (tls.client_key_path is None):
        raise TlsConfigError(
            "client certificate options must be set together: both the "
            "certificate and its key"
        )
    if tls.client_cert_path is not None and tls.client_key_path is not None:
        try:
            context.load_cert_chain(tls.client_cert_path, tls.client_key_path)
        except (OSError, ssl.SSLError) as err:
            raise TlsConfigError(
                f"failed to load tls client certificate material: {err}"
            ) from err
    return context


def _chain_matches_pin(ssl_object: ssl.SSLObject, pin: bytes) -> bool:
    # get_unverified_chain (Python 3.13+) exposes the whole presented chain
    # so a CA pin can match. Older Pythons expose only the leaf certificate.
    certs: list[bytes] = []
    chain_getter = getattr(ssl_object, "get_unverified_chain", None)
    if chain_getter is not None:
        try:
            for cert in chain_getter() or []:
                certs.append(ssl.PEM_cert_to_DER_cert(cert.public_bytes()))
        except ssl.SSLError:
            certs = []
    if not certs:
        leaf = ssl_object.getpeercert(binary_form=True)
        if leaf:
            certs.append(leaf)
    return any(hashlib.sha256(der).digest() == pin for der in certs)


async def _open_connection(
    host: str, port: int, tls: Optional[TlsOptions] = None
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    if tls is None:
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except ConnectionRefusedError as err:
            # The most common first-run stumble: name the two checks that
            # resolve almost all of them.
            raise DisconnectionError(
                f"connection refused by {host}:{port}. Is the broker running and reachable "
                "there? Clients connect to the broker port (default 9876), not the admin API "
                "or dashboard port (default 8081)"
            ) from err
        except (ConnectionError, OSError) as err:
            raise DisconnectionError(f"failed to connect to {host}:{port}: {err}") from err
    else:
        context = _build_ssl_context(tls)
        pin = (
            _normalize_fingerprint(tls.ca_fingerprint)
            if tls.ca_fingerprint is not None
            else None
        )
        server_hostname = tls.server_name or host
        try:
            reader, writer = await asyncio.open_connection(
                host, port, ssl=context, server_hostname=server_hostname
            )
        except ssl.SSLCertVerificationError as err:
            raise TlsCertificateUntrustedError(str(err)) from err
        except ssl.SSLEOFError as err:
            # The handshake ended abruptly: almost always a plaintext broker
            # closing on sighting the ClientHello.
            raise TlsNotSupportedByBrokerError(f"{host}:{port}") from err
        except ssl.SSLError as err:
            if "certificate required" in str(err).lower():
                raise TlsClientCertificateRequiredError(
                    f"the broker at {host}:{port} requires a client certificate: "
                    "provide one with with_tls_client_cert(cert_path, key_path). "
                    "Deployment-CA certificates are issued with fibrilctl cert issue"
                ) from err
            raise TlsHandshakeError(f"TLS handshake failed: {err}") from err
        except ConnectionResetError as err:
            raise TlsNotSupportedByBrokerError(f"{host}:{port}") from err
        except (ConnectionError, OSError) as err:
            raise DisconnectionError(f"failed to connect to {host}:{port}: {err}") from err
        if pin is not None:
            ssl_object = writer.get_extra_info("ssl_object")
            if ssl_object is None or not _chain_matches_pin(ssl_object, pin):
                writer.close()
                raise TlsCertificateUntrustedError(
                    "no certificate in the presented chain matches the pinned fingerprint"
                )
    sock = writer.get_extra_info("socket")
    if sock is not None:
        # Disable Nagle for low latency on small frames, like the reference clients.
        sock.setsockopt(_socket.IPPROTO_TCP, _socket.TCP_NODELAY, 1)
    return reader, writer


class _PooledConnection:
    """A lazily opened connection to one non-bootstrap owner, its own session."""

    def __init__(
        self,
        host: str,
        port: int,
        opts: ClientOptions,
        on_assignment: Optional[AssignmentHandler],
        on_topology_update: Optional[object] = None,
        on_going_away: Optional[object] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._opts = opts
        self._on_assignment = on_assignment
        self._on_topology_update = on_topology_update
        self._on_going_away = on_going_away
        self._engine: Optional[Engine] = None
        self._lock = asyncio.Lock()

    async def engine_for_operation(self) -> Engine:
        if self._engine is not None and not self._engine.is_closed():
            return self._engine
        async with self._lock:
            if self._engine is not None and not self._engine.is_closed():
                return self._engine
            reader, writer = await _open_connection(self._host, self._port, self._opts.tls)
            try:
                engine = await Engine.start(
                    reader,
                    writer,
                    self._opts.engine_options(),
                    {},
                    self._on_assignment,
                    self._on_topology_update,
                    self._on_going_away,
                )
            except BaseException as err:
                writer.close()
                refined = _certificate_required_error(err, self._opts.tls)
                if refined is not None:
                    raise refined from err
                raise
            self._engine = engine
            return engine

    def shutdown(self) -> None:
        if self._engine is not None:
            self._engine.shutdown()


def _prune_pool_to_topology(
    cache: TopologyCache, pool: dict[str, "_PooledConnection"]
) -> None:
    """Drop pooled connections to endpoints that no longer own anything, so a
    failed-over owner's stale connection is gone. A full topology view (refresh
    or push) is authoritative about the live owner set."""
    live = cache.endpoints()
    for endpoint in list(pool):
        if endpoint not in live:
            pool.pop(endpoint).shutdown()


def _apply_pushed_topology(
    topology: wire.TopologyOk,
    cache: TopologyCache,
    pool: dict[str, "_PooledConnection"],
) -> int:
    """Apply a broker-pushed topology snapshot to the routing cache and prune the
    pool, mirroring fetch_topology's apply path. A push only moves the cache
    forward: a stale push (older generation than the cache already holds) is
    ignored so an out-of-order delivery cannot regress routing. Returns the
    generation the cache reflects after the call, which the engine acks."""
    if topology.generation > cache.generation:
        cache.replace(topology)
        cache.last_refresh_ms = asyncio.get_running_loop().time() * 1000
        _prune_pool_to_topology(cache, pool)
    return cache.generation


@dataclass(frozen=True)
class QueueInfo:
    """A declared queue as seen in the cluster :class:`Catalogue`."""

    topic: str
    #: The queue's group namespace, or ``None`` for the ungrouped default.
    group: Optional[str]
    partition_count: int


@dataclass(frozen=True)
class StreamInfo:
    """A declared Plexus stream as seen in the cluster :class:`Catalogue`."""

    topic: str
    partition_count: int


@dataclass(frozen=True)
class Catalogue:
    """A snapshot of the channels declared in the cluster: every queue and Plexus
    stream the client currently knows about, with partition counts. Derived from
    the topology and kept live by topology pushes, so it needs no extra
    round-trips. ``queues`` and ``streams`` are sorted (by topic, then group) for a
    stable order. Read the current snapshot with :meth:`Client.catalogue` or
    subscribe to changes with :meth:`Client.on_catalogue_change`."""

    queues: tuple[QueueInfo, ...]
    streams: tuple[StreamInfo, ...]
    generation: int


CatalogueHandler = Callable[[Catalogue], None]


@dataclass
class _CatalogueState:
    """Latest catalogue snapshot plus its change listeners. Created in connect()
    before the bootstrap engine starts so a push has somewhere to land with no
    wiring race (same reasoning as the routing cache and pool)."""

    current: Catalogue = field(
        default_factory=lambda: Catalogue(queues=(), streams=(), generation=0)
    )
    listeners: set[CatalogueHandler] = field(default_factory=set)


def _catalogue_from_topology(topology: wire.TopologyOk) -> Catalogue:
    """Derive the catalogue from a topology snapshot. The topology lists one entry
    per partition, so queues dedupe by (topic, group) and streams by topic; both
    come back sorted for a deterministic order."""
    queues: dict[tuple[str, Optional[str]], QueueInfo] = {}
    for q in topology.queues:
        queues[(q.topic, q.group)] = QueueInfo(q.topic, q.group, max(q.partition_count, 1))
    streams: dict[str, StreamInfo] = {}
    for s in topology.streams:
        streams[s.topic] = StreamInfo(s.topic, max(s.partition_count, 1))
    return Catalogue(
        queues=tuple(queues[k] for k in sorted(queues, key=lambda k: (k[0], k[1] or ""))),
        streams=tuple(streams[k] for k in sorted(streams)),
        generation=topology.generation,
    )


def _refresh_catalogue(topology: wire.TopologyOk, state: _CatalogueState) -> None:
    """Refresh the catalogue snapshot from a full topology and notify listeners if
    the set of declared queues or streams changed. Monotonic and self-guarding: a
    stale topology (generation not newer than the snapshot already held) is
    ignored. Owner-only churn updates the stored generation but fires no listener."""
    nxt = _catalogue_from_topology(topology)
    prev = state.current
    if nxt.generation <= prev.generation and prev.generation != 0:
        return
    changed = nxt.queues != prev.queues or nxt.streams != prev.streams
    state.current = nxt
    if changed:
        for listener in list(state.listeners):
            try:
                listener(nxt)
            except Exception:
                pass


class Client:
    """Fibril broker client: one bootstrap connection plus routed pooled owners."""

    def __init__(
        self,
        address: tuple[str, int],
        opts: ClientOptions,
        engine: Engine,
        registry: SubscriptionRegistry,
        assignment_listeners: set[AssignmentHandler],
        going_away_listeners: set[GoingAwayHandler],
        topology: TopologyCache,
        pool: dict[str, "_PooledConnection"],
        catalogue: _CatalogueState,
    ) -> None:
        self._address = address
        self._opts = opts
        self._engine = engine
        self._registry = registry
        self._assignment_listeners = assignment_listeners
        self._going_away_listeners = going_away_listeners
        self._bootstrap_endpoint = _endpoint_key(*address)
        # Created in connect() before the bootstrap engine starts and shared here,
        # so a topology push the broker sends right after HELLO has somewhere to
        # land with no wiring race.
        self._topology = topology
        self._pool = pool
        self._catalogue = catalogue
        self._round_robin = RoundRobin()
        self._cohort_member_id: Optional[wire.Uuid] = None
        self._user_shutdown = False
        self._reconnect_lock = asyncio.Lock()

    # ---- connect / lifecycle ------------------------------------------

    @classmethod
    async def connect(
        cls, address: Address, opts: Optional[ClientOptions] = None
    ) -> "Client":
        opts = opts or ClientOptions()
        addr = parse_address(address)
        registry: SubscriptionRegistry = {}
        listeners: set[AssignmentHandler] = set()
        going_away_listeners: set[GoingAwayHandler] = set()

        def emit(event: wire.AssignmentChanged) -> None:
            for listener in list(listeners):
                try:
                    listener(event)
                except Exception:
                    pass

        def emit_going_away(notice: wire.GoingAway) -> None:
            for listener in list(going_away_listeners):
                try:
                    listener(notice)
                except Exception:
                    pass

        # The routing cache and pool are created here, before the bootstrap engine
        # starts, so a topology push the broker sends right after HELLO has
        # somewhere to land with no wiring race. The Client below shares them.
        topology = TopologyCache()
        pool: dict[str, _PooledConnection] = {}
        catalogue = _CatalogueState()

        def on_topology_update(t: wire.TopologyOk) -> int:
            generation = _apply_pushed_topology(t, topology, pool)
            _refresh_catalogue(t, catalogue)
            return generation

        reader, writer = await _open_connection(*addr, tls=opts.tls)
        try:
            engine = await Engine.start(
                reader,
                writer,
                opts.engine_options(),
                registry,
                emit,
                on_topology_update,
                emit_going_away,
            )
        except BaseException as err:
            writer.close()
            refined = _certificate_required_error(err, opts.tls)
            if refined is not None:
                raise refined from err
            raise
        client = cls(
            addr,
            opts,
            engine,
            registry,
            listeners,
            going_away_listeners,
            topology,
            pool,
            catalogue,
        )
        return client

    def _emit_assignment(self, event: wire.AssignmentChanged) -> None:
        for listener in list(self._assignment_listeners):
            try:
                listener(event)
            except Exception:
                pass

    def on_assignment_change(self, handler: AssignmentHandler) -> Callable[[], None]:
        """Observe exclusive-cohort assignment changes. Returns an unsubscribe."""
        self._assignment_listeners.add(handler)

        def unsubscribe() -> None:
            self._assignment_listeners.discard(handler)

        return unsubscribe

    def _emit_going_away(self, notice: wire.GoingAway) -> None:
        for listener in list(self._going_away_listeners):
            try:
                listener(notice)
            except Exception:
                pass

    def on_going_away(self, handler: GoingAwayHandler) -> Callable[[], None]:
        """Observe broker drain notices. The handler fires when the broker
        announces it is draining for a planned shutdown or upgrade, so the app can
        stop producing or finish in-flight work before the connection drops.
        Returns an unsubscribe. The stream survives reconnects."""
        self._going_away_listeners.add(handler)

        def unsubscribe() -> None:
            self._going_away_listeners.discard(handler)

        return unsubscribe

    def catalogue(self) -> Catalogue:
        """The current cluster catalogue: every queue and Plexus stream this client
        knows about, with partition counts. Read straight from the cached topology
        with no round-trip. Empty on a cold or standalone client; kept live by
        broker topology pushes."""
        return self._catalogue.current

    def on_catalogue_change(self, handler: CatalogueHandler) -> Callable[[], None]:
        """Observe cluster catalogue changes. The handler fires with a full
        :class:`Catalogue` snapshot when the set of declared queues or streams
        changes (a channel added or removed, or a partition count change).
        Owner-only failover churn does not fire. Returns an unsubscribe."""
        self._catalogue.listeners.add(handler)

        def unsubscribe() -> None:
            self._catalogue.listeners.discard(handler)

        return unsubscribe

    async def reconnect(self) -> ReconnectOutcome:
        async with self._reconnect_lock:
            return await self._reconnect_once()

    async def _reconnect_once(self) -> ReconnectOutcome:
        old = self._engine
        reader, writer = await _open_connection(*self._address, tls=self._opts.tls)
        try:
            engine = await Engine.start(
                reader,
                writer,
                self._opts.engine_options(old.resume_identity),
                self._registry,
                self._emit_assignment,
                self._on_topology_update,
                self._emit_going_away,
            )
        except BaseException as err:
            writer.close()
            refined = _certificate_required_error(err, self._opts.tls)
            if refined is not None:
                raise refined from err
            raise
        self._engine = engine
        self._user_shutdown = False
        old.shutdown_for_reconnect()
        return ReconnectOutcome(resume_outcome=engine.resume_outcome)

    async def _reconnect_if_closed(self) -> None:
        if not self._engine.is_closed():
            return
        if self._user_shutdown or self._opts.auto_reconnect_attempts == 0:
            raise BrokenPipeError()
        # A non-retryable close (bad credentials, forbidden, malformed request)
        # will only fail again on reconnect, so surface it instead of storming the
        # broker with doomed handshakes while the real error never reaches the caller.
        reason = self._engine.close_reason()
        if reason is not None and retry_advice(reason) == "do_not_retry":
            raise reason
        async with self._reconnect_lock:
            if not self._engine.is_closed():
                return
            last: Optional[BaseException] = None
            for _ in range(self._opts.auto_reconnect_attempts):
                try:
                    await self._reconnect_once()
                    return
                except Exception as err:
                    last = err
            raise last or BrokenPipeError()

    async def shutdown(self) -> None:
        """Gracefully shut down. Pending ops fail and subscription iterators end."""
        self._user_shutdown = True
        for conn in self._pool.values():
            conn.shutdown()
        self._pool.clear()
        engine = self._engine
        engine.shutdown()
        await engine.wait_closed()

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.shutdown()

    # ---- handles -------------------------------------------------------

    def publisher(self, topic: str) -> "Publisher":
        from .publisher import Publisher

        return Publisher(self, topic, None)

    def publisher_grouped(self, topic: str, group: str) -> "Publisher":
        from .publisher import Publisher

        return Publisher(self, topic, normalize_group(group))

    def subscribe(self, topic: str) -> "SubscriptionBuilder":
        from .subscription import SubscriptionBuilder

        return SubscriptionBuilder(self, topic)

    def stream(self, topic: str) -> "StreamSubscriptionBuilder":
        """Begin a Plexus (fan-out stream) subscription. Every consumer sees every
        record; the subscription reads all partitions and fans them in."""
        from .subscription import StreamSubscriptionBuilder

        return StreamSubscriptionBuilder(self, topic)

    def routing(self) -> "RoutingClient":
        """Opt in to the routing/discovery surface. Returns a
        :class:`RoutingClient` sharing this connection; the plain client stays
        usable. Pattern subscribe and auto-pickup of matching channels live there,
        off the default surface."""
        from .routing import RoutingClient

        return RoutingClient(self)

    async def declare_queue(self, config: QueueConfig) -> None:
        engine = await self._engine_for_operation()
        await engine.declare_queue(config.to_wire())

    async def declare_plexus(self, config: StreamConfig) -> None:
        """Declare a Plexus (fan-out stream) channel."""
        engine = await self._engine_for_operation()
        await engine.declare_plexus(config.to_wire())

    # ---- topology / routing -------------------------------------------

    async def fetch_topology(
        self, topic: Optional[str] = None, group: Optional[str] = None
    ) -> wire.TopologyOk:
        engine = await self._engine_for_operation()
        topology = await engine.fetch_topology(topic, group)
        self._topology.replace(topology)
        self._topology.last_refresh_ms = asyncio.get_running_loop().time() * 1000
        _prune_pool_to_topology(self._topology, self._pool)
        _refresh_catalogue(topology, self._catalogue)
        return topology

    def _on_topology_update(self, topology: wire.TopologyOk) -> int:
        """Apply a broker-pushed topology snapshot and return the acked generation.

        Used by reconnect and pooled engines (the bootstrap engine uses the
        equivalent closure created in connect, sharing the same cache and pool).
        """
        generation = _apply_pushed_topology(topology, self._topology, self._pool)
        _refresh_catalogue(topology, self._catalogue)
        return generation

    def route(self, topic: str, group: Optional[str], key: Optional[bytes]) -> Route:
        return route_partition(self._topology, topic, group, key, self._round_robin)

    @property
    def max_redirects(self) -> int:
        return self._opts.max_redirects

    @property
    def publish_timeout_ms(self) -> int:
        return self._opts.publish_timeout_ms

    def apply_redirect(self, redirect: wire.Redirect) -> None:
        self._topology.apply_redirect(redirect)

    def is_topic_missing(self, topic: str, group: Optional[str]) -> bool:
        return self._topology.is_populated() and not self._topology.knows_topic(topic, group)

    def is_shutting_down(self) -> bool:
        return self._user_shutdown

    def supervise_subscriptions(self) -> bool:
        return self._opts.supervise_subscriptions

    def supervise_interval_ms(self) -> int:
        return self._opts.subscription_supervise_interval_ms

    def partition_set(self, topic: str, group: Optional[str]) -> list[int]:
        partitioning = self._topology.partitioning(topic, group)
        count = max(partitioning.count if partitioning is not None else 1, 1)
        return list(range(count))

    def owner_endpoint(
        self, topic: str, partition: int, group: Optional[str]
    ) -> Optional[str]:
        entry = self._topology.lookup(topic, partition, group)
        return entry.endpoint if entry is not None else None

    async def refresh_topology_throttled(self) -> bool:
        now = asyncio.get_running_loop().time() * 1000
        if now - self._topology.last_refresh_ms < self._opts.topology_refresh_cooldown_ms:
            return False
        try:
            await self.fetch_topology()
            return True
        except Exception:
            return False

    # ---- engine resolution --------------------------------------------

    async def _engine_for_operation(self) -> Engine:
        await self._reconnect_if_closed()
        return self._engine

    async def engine_for(
        self, topic: str, partition: int, group: Optional[str]
    ) -> Engine:
        owner = self._topology.lookup(topic, partition, group)
        if owner is None or owner.endpoint == self._bootstrap_endpoint:
            return await self._engine_for_operation()
        conn = self._pool.get(owner.endpoint)
        if conn is None:
            host, port = parse_address(owner.endpoint)
            conn = _PooledConnection(
                host,
                port,
                self._opts,
                self._emit_assignment,
                self._on_topology_update,
                self._emit_going_away,
            )
            self._pool[owner.endpoint] = conn
        return await conn.engine_for_operation()

    async def subscribe_once(self, req: wire.Subscribe) -> SubscribeHandle:
        """Subscribe one partition to its current owner (routed). Used by the fan-in
        supervisor to (re)attach a subscription."""
        full = self._with_cohort_member(req)
        engine = await self.engine_for(full.topic, full.partition, full.group)
        result: SubscribeResult = await engine.subscribe(
            full, supervised=self._opts.supervise_subscriptions
        )
        self._capture_cohort_member(full, result.member_id)
        return SubscribeHandle(engine=engine, queue=result.queue)

    async def subscribe_stream_once(self, req: wire.SubscribeStream) -> SubscribeHandle:
        """Subscribe one stream partition to its current owner (routed). Used by the
        stream fan-in supervisor to (re)attach a subscription."""
        engine = await self.engine_for(req.topic, req.partition, None)
        result = await engine.subscribe_stream(req)
        return SubscribeHandle(engine=engine, queue=result.queue)

    def _with_cohort_member(self, req: wire.Subscribe) -> wire.Subscribe:
        if req.consumer_group is None:
            return req
        if req.member_id is not None:
            return req
        return replace(req, member_id=self._cohort_member_id)

    def _capture_cohort_member(
        self, req: wire.Subscribe, member_id: Optional[wire.Uuid]
    ) -> None:
        if (
            req.consumer_group is not None
            and member_id is not None
            and self._cohort_member_id is None
        ):
            self._cohort_member_id = member_id


# Imported lazily in the handle factory methods to avoid an import cycle, but
# referenced in type comments above.
if False:  # pragma: no cover - typing only
    from .publisher import Publisher
    from .subscription import StreamSubscriptionBuilder, SubscriptionBuilder
