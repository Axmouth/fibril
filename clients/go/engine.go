package fibril

// The engine owns one live connection to one broker: the handshake, optional
// auth, heartbeats, the request-id -> reply-channel map, and the read/write
// loops. It mirrors the reference client's structure with Go primitives: a single
// actor goroutine (run) owns all mutable state and is the sole writer, a read
// goroutine feeds it decoded frames, and public methods hand it commands over a
// channel and wait on a per-request reply channel. No locks guard engine state -
// only the run goroutine touches it.
//
// It knows nothing about routing, topology, or reconnect; that is the client
// layer above it.

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
)

const defaultHeartbeatInterval = 5 * time.Second

// heartbeatTimeoutMessage carries the shared client-local guide wording (see
// clients/error_guides.json): a stalled connection, its likely cause, and that a
// reconnect will be attempted.
const heartbeatTimeoutMessage = "heartbeat timeout: no response from the broker within the timeout window. " +
	"This usually means a network stall or an overloaded or stopped broker rather than a client bug. " +
	"The client will attempt to reconnect if auto-reconnect is enabled."

// The run goroutine buffers outgoing frames and flushes once per batch, so a
// burst of fire-and-forget writes (unconfirmed publishes, acks) coalesces into
// far fewer socket writes. writeBufSize caps a single flush; maxWriteBatch caps
// how many queued commands one drain absorbs before flushing, keeping the loop
// responsive to incoming frames.
const (
	writeBufSize  = 128 * 1024
	maxWriteBatch = 256
)

// EngineOptions are the connection-level settings for one session.
type EngineOptions struct {
	ClientName        string
	ClientVersion     string
	Auth              *Auth
	HeartbeatInterval time.Duration
	Resume            *ResumeIdentity
	// TLS, if set, connects over TLS with these trust settings; nil connects
	// plaintext.
	TLS *TLSOptions
	// OnTopologyUpdate, if set, is called on the run goroutine for each broker
	// topology push; it applies the snapshot and returns the generation the
	// client now reflects, which the engine acks so the broker can fence a
	// repartition cutover.
	OnTopologyUpdate func(TopologyOk) uint64
	// OnAssignmentChanged, if set, is called on the run goroutine for each
	// exclusive-cohort assignment push.
	OnAssignmentChanged func(AssignmentChanged)
	// ReconcileRegistry, if set, remembers non-supervised subscriptions on this
	// endpoint so a reconnect can restore them. On (re)connect the engine sends
	// RECONCILE_CLIENT for any it holds and adopts the restored delivery channels.
	ReconcileRegistry *reconcileRegistry
}

// command is a request from a public method to the run goroutine: send op(body),
// and if reply/subReply is non-nil, correlate the broker's response back to it.
type command struct {
	op       Op
	body     []byte
	id       uint64         // if nonzero, use this request id instead of allocating one
	reply    chan reply     // generic request/reply; nil = fire-and-forget
	subReply chan subResult // set for a subscribe (delivers the created Subscription)
	autoAck  bool           // carried to the sub state on SUBSCRIBE_OK
	// noReconcile keeps this subscribe out of the reconnect reconcile registry
	// (supervised subscriptions and streams recover another way).
	noReconcile bool
	// sub carries the full subscribe request so SUBSCRIBE_OK can register it for
	// reconcile without re-decoding.
	sub *Subscribe
}

type reply struct {
	frame Frame
	err   error
}

type subResult struct {
	sub *Subscription
	err error
}

// waiter is a pending request awaiting its correlated reply. Exactly one of
// reply/subReply is set.
type waiter struct {
	reply       chan reply
	subReply    chan subResult
	autoAck     bool
	noReconcile bool
	sub         *Subscribe
}

func failWaiter(w *waiter, err error) {
	if w.reply != nil {
		w.reply <- reply{err: err}
	}
	if w.subReply != nil {
		w.subReply <- subResult{err: err}
	}
}

// subState is a live subscription's delivery channel, owned by the run goroutine.
// A preserve subscription's channel is not closed when the engine dies (the
// reconcile registry owns it across a reconnect).
type subState struct {
	ch       chan Delivery
	autoAck  bool
	preserve bool
}

// Engine is one live broker connection.
type Engine struct {
	conn net.Conn
	bw   *bufio.Writer // buffered write side, flushed per batch by the run goroutine
	opts EngineOptions

	cmdCh   chan command
	frameCh chan Frame    // read goroutine -> run goroutine
	stop    chan struct{} // closed once on teardown, unblocks the read goroutine
	done    chan struct{} // closed when run returns

	shutdownOnce sync.Once
	stopOnce     sync.Once

	// Owned exclusively by the run goroutine (no locks).
	nextID   uint64
	waiters  map[uint64]*waiter
	subs     map[uint64]*subState
	lastSeen time.Time
	closed   bool

	errMu    sync.Mutex
	closeErr error

	// Set during the handshake, immutable afterwards.
	ResumeIdentity ResumeIdentity
	ResumeOutcome  ResumeOutcome
}

// Connect dials addr and performs the handshake (and optional auth), returning a
// ready engine.
func Connect(addr string, opts EngineOptions) (*Engine, error) {
	conn, err := dial(addr, opts.TLS)
	if err != nil {
		return nil, err
	}
	e, err := startEngine(conn, opts)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return e, nil
}

// startEngine runs the handshake synchronously over conn, then starts the read
// and run goroutines. Split out from Connect so tests can supply any net.Conn
// (e.g. an in-memory pipe) instead of dialing.
func startEngine(conn net.Conn, opts EngineOptions) (*Engine, error) {
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = defaultHeartbeatInterval
	}
	br := bufio.NewReader(conn)

	// HELLO.
	hello := Hello{
		ClientName:      opts.ClientName,
		ClientVersion:   opts.ClientVersion,
		ProtocolVersion: ProtocolV1,
		Resume:          opts.Resume,
	}
	if _, err := conn.Write(encodeFrame(buildFrame(OpHello, 1, encodeHello(hello)))); err != nil {
		return nil, &DisconnectionError{Message: "write HELLO: " + err.Error()}
	}
	hf, err := readFrame(br)
	if err != nil {
		return nil, &DisconnectionError{Message: "read HELLO reply: " + err.Error()}
	}
	switch hf.Opcode {
	case OpHelloOk:
	case OpHelloErr, OpError:
		em, _ := decodeError(hf.Payload)
		// A plaintext connection to a TLS listener draws this definitive code.
		if em.Code == errTLSRequired {
			return nil, &TlsRequiredByBrokerError{}
		}
		return nil, &ServerError{Code: em.Code, Message: em.Message}
	default:
		return nil, &UnexpectedError{Message: "unexpected frame during HELLO"}
	}
	helloOk, err := decodeHelloOk(hf.Payload)
	if err != nil {
		return nil, err
	}
	if helloOk.ProtocolVersion != ProtocolV1 {
		return nil, &DisconnectionError{Message: "protocol version mismatch"}
	}
	if helloOk.Compliance != ComplianceString {
		return nil, &DisconnectionError{Message: "protocol compliance marker mismatch"}
	}

	// AUTH (optional).
	if opts.Auth != nil {
		if _, err := conn.Write(encodeFrame(buildFrame(OpAuth, 2, encodeAuth(*opts.Auth)))); err != nil {
			return nil, &DisconnectionError{Message: "write AUTH: " + err.Error()}
		}
		af, err := readFrame(br)
		if err != nil {
			return nil, &DisconnectionError{Message: "read AUTH reply: " + err.Error()}
		}
		switch af.Opcode {
		case OpAuthOk:
		case OpAuthErr, OpError:
			em, _ := decodeError(af.Payload)
			return nil, &ServerError{Code: em.Code, Message: em.Message}
		default:
			return nil, &UnexpectedError{Message: "unexpected frame during AUTH"}
		}
	}

	// RECONCILE on any reconnect that has remembered subscriptions: a bounced
	// owner reconnects into a fresh session that forgot them, so the client
	// re-announces them and adopts the restored delivery channels.
	subs := make(map[uint64]*subState)
	nextID := uint64(2) // 1 = HELLO, 2 = AUTH
	if reg := opts.ReconcileRegistry; reg != nil && !reg.isEmpty() {
		nextID = 3
		if _, err := conn.Write(encodeFrame(buildFrame(OpReconcileClient, 3, encodeReconcileClient(ReconcileClient{
			Policy:        reg.policy,
			Subscriptions: reg.snapshot(),
		})))); err != nil {
			return nil, &DisconnectionError{Message: "write RECONCILE_CLIENT: " + err.Error()}
		}
		rf, err := readFrame(br)
		if err != nil {
			return nil, &DisconnectionError{Message: "read RECONCILE reply: " + err.Error()}
		}
		switch rf.Opcode {
		case OpReconcileResult:
			res, derr := decodeReconcileResult(rf.Payload)
			if derr != nil {
				return nil, derr
			}
			subs = reg.applyResult(res)
		case OpError:
			em, _ := decodeError(rf.Payload)
			return nil, &ServerError{Code: em.Code, Message: em.Message}
		default:
			return nil, &UnexpectedError{Message: "unexpected frame during reconciliation"}
		}
	}

	e := &Engine{
		conn:           conn,
		bw:             bufio.NewWriterSize(conn, writeBufSize),
		opts:           opts,
		cmdCh:          make(chan command, 64),
		frameCh:        make(chan Frame, 64),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
		nextID:         nextID,
		waiters:        make(map[uint64]*waiter),
		subs:           subs,
		lastSeen:       time.Now(),
		ResumeIdentity: ResumeIdentity{OwnerID: helloOk.OwnerID, ClientID: helloOk.ClientID, ResumeToken: helloOk.ResumeToken},
		ResumeOutcome:  helloOk.ResumeOutcome,
	}
	go e.readLoop(br)
	go e.run()
	return e, nil
}

// ---- public request methods --------------------------------------------

// PublishUnconfirmed sends a fire-and-forget publish.
func (e *Engine) PublishUnconfirmed(p Publish) error {
	p.RequireConfirm = false
	return e.send(OpPublish, encodePublish(p))
}

// PublishConfirmed sends a publish and waits for the broker-assigned offset.
func (e *Engine) PublishConfirmed(p Publish) (uint64, error) {
	p.RequireConfirm = true
	f, err := e.request(OpPublish, encodePublish(p))
	if err != nil {
		return 0, err
	}
	ok, err := decodePublishOk(f.Payload)
	if err != nil {
		return 0, err
	}
	return ok.Offset, nil
}

// PublishDelayedUnconfirmed sends a fire-and-forget publish scheduled for later.
func (e *Engine) PublishDelayedUnconfirmed(p PublishDelayed) error {
	p.RequireConfirm = false
	return e.send(OpPublishDelayed, encodePublishDelayed(p))
}

// PublishDelayedConfirmed sends a delayed publish and waits for the offset.
func (e *Engine) PublishDelayedConfirmed(p PublishDelayed) (uint64, error) {
	p.RequireConfirm = true
	f, err := e.request(OpPublishDelayed, encodePublishDelayed(p))
	if err != nil {
		return 0, err
	}
	ok, err := decodePublishOk(f.Payload)
	if err != nil {
		return 0, err
	}
	return ok.Offset, nil
}

// PublishResult is the outcome of a pipelined confirmed publish.
type PublishResult struct {
	Offset uint64
	Err    error
}

// PublishPipelined sends a confirmed publish and returns a channel that yields
// its result later, without blocking. The frame is written before this returns,
// so callers can pipeline several publishes (in send order) and collect each
// offset afterward.
func (e *Engine) PublishPipelined(p Publish) (<-chan PublishResult, error) {
	p.RequireConfirm = true
	rc := make(chan reply, 1)
	select {
	case e.cmdCh <- command{op: OpPublish, body: encodePublish(p), reply: rc}:
	case <-e.done:
		return nil, e.err()
	}
	out := make(chan PublishResult, 1)
	go func() {
		select {
		case r := <-rc:
			if r.err != nil {
				out <- PublishResult{Err: r.err}
				return
			}
			ok, err := decodePublishOk(r.frame.Payload)
			out <- PublishResult{Offset: ok.Offset, Err: err}
		case <-e.done:
			out <- PublishResult{Err: e.err()}
		}
	}()
	return out, nil
}

// DeclareQueue declares a queue and waits for the broker's confirmation.
func (e *Engine) DeclareQueue(d DeclareQueue) (DeclareQueueOk, error) {
	f, err := e.request(OpDeclareQueue, encodeDeclareQueue(d))
	if err != nil {
		return DeclareQueueOk{}, err
	}
	return decodeDeclareQueueOk(f.Payload)
}

// DeclarePlexus declares a Plexus (fan-out stream) channel.
func (e *Engine) DeclarePlexus(d DeclarePlexus) (DeclarePlexusOk, error) {
	f, err := e.request(OpDeclarePlexus, encodeDeclarePlexus(d))
	if err != nil {
		return DeclarePlexusOk{}, err
	}
	return decodeDeclarePlexusOk(f.Payload)
}

// FetchTopology requests the cluster topology, optionally filtered.
func (e *Engine) FetchTopology(req TopologyRequest) (TopologyOk, error) {
	f, err := e.request(OpTopology, encodeTopologyRequest(req))
	if err != nil {
		return TopologyOk{}, err
	}
	return decodeTopologyOk(f.Payload)
}

// Shutdown tears the connection down gracefully and waits for the loops to exit.
func (e *Engine) Shutdown() {
	e.shutdownOnce.Do(func() { e.closeStop() })
	<-e.done
}

// IsClosed reports whether the engine can no longer accept operations.
func (e *Engine) IsClosed() bool {
	select {
	case <-e.done:
		return true
	default:
		return false
	}
}

// ---- command submission ------------------------------------------------

func (e *Engine) send(op Op, body []byte) error {
	select {
	case e.cmdCh <- command{op: op, body: body}:
		return nil
	case <-e.done:
		return e.err()
	}
}

func (e *Engine) request(op Op, body []byte) (Frame, error) {
	rc := make(chan reply, 1)
	select {
	case e.cmdCh <- command{op: op, body: body, reply: rc}:
	case <-e.done:
		return Frame{}, e.err()
	}
	select {
	case r := <-rc:
		return r.frame, r.err
	case <-e.done:
		return Frame{}, e.err()
	}
}

// ---- run goroutine (the actor) -----------------------------------------

func (e *Engine) run() {
	defer close(e.done)
	ticker := time.NewTicker(e.opts.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case cmd := <-e.cmdCh:
			e.handleCommand(cmd)
			e.drainCommands() // coalesce any queued commands into this batch
		case f, ok := <-e.frameCh:
			if !ok {
				e.markDead(&DisconnectionError{Message: "connection closed by peer"})
			} else {
				e.handleFrame(f)
			}
		case <-ticker.C:
			e.tick()
		case <-e.stop:
			e.flush() // push buffered fire-and-forget frames before closing
			e.markDead(&BrokenPipeError{Message: "engine shutdown"})
		}
		if e.closed {
			return
		}
		e.flush() // one socket write for everything buffered this iteration
	}
}

// drainCommands buffers any immediately-available commands (up to a cap) without
// blocking, so their writes coalesce into the batch the run loop then flushes.
func (e *Engine) drainCommands() {
	for n := 0; n < maxWriteBatch; n++ {
		select {
		case cmd := <-e.cmdCh:
			e.handleCommand(cmd)
		default:
			return
		}
	}
}

// flush writes the buffered frames to the socket in one syscall (a no-op when
// nothing is buffered).
func (e *Engine) flush() {
	if e.closed {
		return
	}
	if err := e.bw.Flush(); err != nil {
		e.markDead(&DisconnectionError{Message: "socket write failed: " + err.Error()})
	}
}

func (e *Engine) handleCommand(cmd command) {
	if e.closed {
		if cmd.reply != nil {
			cmd.reply <- reply{err: e.err()}
		}
		if cmd.subReply != nil {
			cmd.subReply <- subResult{err: e.err()}
		}
		return
	}
	id := cmd.id
	if id == 0 {
		e.nextID++
		id = e.nextID
	}
	if cmd.reply != nil || cmd.subReply != nil {
		e.waiters[id] = &waiter{reply: cmd.reply, subReply: cmd.subReply, autoAck: cmd.autoAck, noReconcile: cmd.noReconcile, sub: cmd.sub}
	}
	if err := e.write(buildFrame(cmd.op, id, cmd.body)); err != nil {
		if w, ok := e.waiters[id]; ok {
			delete(e.waiters, id)
			failWaiter(w, err)
		}
		e.markDead(err)
	}
}

func (e *Engine) handleFrame(f Frame) {
	e.lastSeen = time.Now()
	switch f.Opcode {
	case OpPong:
		return
	case OpPing:
		_ = e.write(buildFrame(OpPong, f.RequestID, nil))
	case OpPublishOk, OpDeclareQueueOk, OpDeclarePlexusOk, OpTopologyOk:
		e.resolve(f.RequestID, reply{frame: f})
	case OpSubscribeOk:
		e.handleSubscribeOk(f)
	case OpDeliver:
		e.handleDeliver(f)
	case OpTopologyUpdate:
		// Unsolicited routing refresh. Apply it and ack the generation now
		// reflected so the broker can fence a repartition cutover.
		if e.opts.OnTopologyUpdate != nil {
			if topo, err := decodeTopologyUpdate(f.Payload); err == nil {
				gen := e.opts.OnTopologyUpdate(topo)
				_ = e.write(buildFrame(OpTopologyUpdateAck, f.RequestID, encodeTopologyUpdateAck(TopologyUpdateAck{Generation: gen})))
			}
		}
	case OpAssignmentChanged:
		if e.opts.OnAssignmentChanged != nil {
			if a, err := decodeAssignmentChanged(f.Payload); err == nil {
				e.opts.OnAssignmentChanged(a)
			}
		}
	case OpRedirect:
		// The broker routed us to a different owner. Fail the waiter with a typed
		// redirect the client layer acts on; not fatal to the connection.
		w, ok := e.waiters[f.RequestID]
		if !ok {
			return
		}
		delete(e.waiters, f.RequestID)
		if rd, err := decodeRedirect(f.Payload); err != nil {
			failWaiter(w, err)
		} else {
			failWaiter(w, &RedirectError{Redirect: rd})
		}
	case OpError, OpSubscribeErr:
		em, _ := decodeError(f.Payload)
		serr := &ServerError{Code: em.Code, Message: em.Message}
		if w, ok := e.waiters[f.RequestID]; ok {
			delete(e.waiters, f.RequestID)
			failWaiter(w, serr)
		} else if f.Opcode == OpError {
			// A connection-level error with no correlated request is fatal.
			e.markDead(&DisconnectionError{Message: serr.Error()})
		}
	default:
		// Assignment/topology pushes and other frames are handled in a later brick.
	}
}

func (e *Engine) resolve(id uint64, r reply) {
	if w, ok := e.waiters[id]; ok {
		delete(e.waiters, id)
		if w.reply != nil {
			w.reply <- r
		}
	}
}

func (e *Engine) tick() {
	if e.closed {
		return
	}
	if time.Since(e.lastSeen) > 3*e.opts.HeartbeatInterval {
		e.markDead(&DisconnectionError{Message: heartbeatTimeoutMessage})
		return
	}
	e.nextID++
	if err := e.write(buildFrame(OpPing, e.nextID, nil)); err != nil {
		e.markDead(err)
	}
}

func (e *Engine) markDead(err error) {
	if e.closed {
		return
	}
	e.closed = true
	e.setErr(err)
	for id, w := range e.waiters {
		failWaiter(w, err)
		delete(e.waiters, id)
	}
	// Closing each delivery channel ends the consumer's range over it. A preserve
	// subscription's channel is left open: the reconcile registry owns it and
	// either carries it onto the reconnected engine or closes it there.
	for id, s := range e.subs {
		if !s.preserve {
			close(s.ch)
		}
		delete(e.subs, id)
	}
	e.closeStop()
	_ = e.conn.Close()
}

// write is only ever called from the run goroutine, so the engine has a single
// writer and needs no write lock.
// write buffers a frame. The run goroutine flushes the buffer once per batch;
// errors surface at flush time (or as a sticky error on the next write).
func (e *Engine) write(f Frame) error {
	_, err := e.bw.Write(encodeFrame(f))
	return err
}

// ---- read goroutine ----------------------------------------------------

func (e *Engine) readLoop(br *bufio.Reader) {
	defer close(e.frameCh)
	for {
		f, err := readFrame(br)
		if err != nil {
			return // EOF or the conn was closed by markDead; frameCh close signals run
		}
		select {
		case e.frameCh <- f:
		case <-e.stop:
			return
		}
	}
}

// ---- teardown plumbing -------------------------------------------------

func (e *Engine) closeStop() {
	e.stopOnce.Do(func() { close(e.stop) })
}

func (e *Engine) setErr(err error) {
	e.errMu.Lock()
	if e.closeErr == nil {
		e.closeErr = err
	}
	e.errMu.Unlock()
}

func (e *Engine) err() error {
	e.errMu.Lock()
	defer e.errMu.Unlock()
	if e.closeErr != nil {
		return e.closeErr
	}
	return &BrokenPipeError{}
}

// ---- frame read helper -------------------------------------------------

// readFrame reads exactly one whole frame from r (blocking until it has one, or
// returning an error on EOF/short read).
func readFrame(r io.Reader) (Frame, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}
	payloadLen := binary.BigEndian.Uint32(header[0:])
	f := Frame{
		Version:   binary.BigEndian.Uint16(header[4:]),
		Opcode:    Op(binary.BigEndian.Uint16(header[6:])),
		Flags:     binary.BigEndian.Uint32(header[8:]),
		RequestID: binary.BigEndian.Uint64(header[12:]),
	}
	if payloadLen > 0 {
		f.Payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return Frame{}, err
		}
	}
	return f, nil
}
