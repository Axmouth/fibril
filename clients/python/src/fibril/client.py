"""Cluster-aware client: connection pool, topology cache, and routing.

Mirrors the Rust client (``crates/client``): one bootstrap connection plus a
pool of lazily opened connections to non-bootstrap owners, a routing cache warmed
by topology fetches and point-updated by redirects, bounded redirect-follow, and
auto-reconnect. The engine exposes async request methods directly, so this layer
calls them without an intermediate command channel.
"""

from __future__ import annotations

import asyncio
import socket as _socket
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Callable, Optional, Union

from . import wire
from .engine import Engine, EngineOptions, SubscribeResult, SubscriptionRegistry
from .errors import BrokenPipeError, DisconnectionError, FibrilError
from .internal.bounded_queue import BoundedQueue
from .internal.topology import Route, RoundRobin, TopologyCache, route_partition

Address = Union[str, tuple[str, int]]
AssignmentHandler = Callable[[wire.AssignmentChanged], None]

DEFAULT_CLIENT_NAME = "Fibril Python Client"
DEFAULT_CLIENT_VERSION = "0.1.0"


def normalize_group(group: Optional[str]) -> Optional[str]:
    trimmed = group.strip() if group else ""
    if not trimmed or trimmed == "default":
        return None
    return trimmed


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

    def with_auth(self, username: str, password: str) -> "ClientOptions":
        return replace(self, auth=wire.Auth(username=username, password=password))

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


async def _open_connection(
    host: str, port: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    try:
        reader, writer = await asyncio.open_connection(host, port)
    except (ConnectionError, OSError) as err:
        raise DisconnectionError(f"failed to connect to {host}:{port}: {err}") from err
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
    ) -> None:
        self._host = host
        self._port = port
        self._opts = opts
        self._on_assignment = on_assignment
        self._on_topology_update = on_topology_update
        self._engine: Optional[Engine] = None
        self._lock = asyncio.Lock()

    async def engine_for_operation(self) -> Engine:
        if self._engine is not None and not self._engine.is_closed():
            return self._engine
        async with self._lock:
            if self._engine is not None and not self._engine.is_closed():
                return self._engine
            reader, writer = await _open_connection(self._host, self._port)
            try:
                engine = await Engine.start(
                    reader,
                    writer,
                    self._opts.engine_options(),
                    {},
                    self._on_assignment,
                    self._on_topology_update,
                )
            except BaseException:
                writer.close()
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
        topology: TopologyCache,
        pool: dict[str, "_PooledConnection"],
        catalogue: _CatalogueState,
    ) -> None:
        self._address = address
        self._opts = opts
        self._engine = engine
        self._registry = registry
        self._assignment_listeners = assignment_listeners
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

        def emit(event: wire.AssignmentChanged) -> None:
            for listener in list(listeners):
                try:
                    listener(event)
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

        reader, writer = await _open_connection(*addr)
        try:
            engine = await Engine.start(
                reader, writer, opts.engine_options(), registry, emit, on_topology_update
            )
        except BaseException:
            writer.close()
            raise
        client = cls(addr, opts, engine, registry, listeners, topology, pool, catalogue)
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
        reader, writer = await _open_connection(*self._address)
        try:
            engine = await Engine.start(
                reader,
                writer,
                self._opts.engine_options(old.resume_identity),
                self._registry,
                self._emit_assignment,
                self._on_topology_update,
            )
        except BaseException:
            writer.close()
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
                host, port, self._opts, self._emit_assignment, self._on_topology_update
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
