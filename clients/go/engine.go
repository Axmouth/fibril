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

// EngineOptions are the connection-level settings for one session.
type EngineOptions struct {
	ClientName        string
	ClientVersion     string
	Auth              *Auth
	HeartbeatInterval time.Duration
	Resume            *ResumeIdentity
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
	reply    chan reply
	subReply chan subResult
	autoAck  bool
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
type subState struct {
	ch      chan Delivery
	autoAck bool
}

// Engine is one live broker connection.
type Engine struct {
	conn net.Conn
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
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, &DisconnectionError{Message: "failed to connect to " + addr + ": " + err.Error()}
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
	if _, err := conn.Write(EncodeFrame(BuildFrame(OpHello, 1, EncodeHello(hello)))); err != nil {
		return nil, &DisconnectionError{Message: "write HELLO: " + err.Error()}
	}
	hf, err := readFrame(br)
	if err != nil {
		return nil, &DisconnectionError{Message: "read HELLO reply: " + err.Error()}
	}
	switch hf.Opcode {
	case OpHelloOk:
	case OpHelloErr, OpError:
		em, _ := DecodeError(hf.Payload)
		return nil, &ServerError{Code: em.Code, Message: em.Message}
	default:
		return nil, &UnexpectedError{Message: "unexpected frame during HELLO"}
	}
	helloOk, err := DecodeHelloOk(hf.Payload)
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
		if _, err := conn.Write(EncodeFrame(BuildFrame(OpAuth, 2, EncodeAuth(*opts.Auth)))); err != nil {
			return nil, &DisconnectionError{Message: "write AUTH: " + err.Error()}
		}
		af, err := readFrame(br)
		if err != nil {
			return nil, &DisconnectionError{Message: "read AUTH reply: " + err.Error()}
		}
		switch af.Opcode {
		case OpAuthOk:
		case OpAuthErr, OpError:
			em, _ := DecodeError(af.Payload)
			return nil, &ServerError{Code: em.Code, Message: em.Message}
		default:
			return nil, &UnexpectedError{Message: "unexpected frame during AUTH"}
		}
	}

	e := &Engine{
		conn:           conn,
		opts:           opts,
		cmdCh:          make(chan command, 64),
		frameCh:        make(chan Frame, 64),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
		nextID:         2, // 1 = HELLO, 2 = AUTH are already used
		waiters:        make(map[uint64]*waiter),
		subs:           make(map[uint64]*subState),
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
	return e.send(OpPublish, EncodePublish(p))
}

// PublishConfirmed sends a publish and waits for the broker-assigned offset.
func (e *Engine) PublishConfirmed(p Publish) (uint64, error) {
	p.RequireConfirm = true
	f, err := e.request(OpPublish, EncodePublish(p))
	if err != nil {
		return 0, err
	}
	ok, err := DecodePublishOk(f.Payload)
	if err != nil {
		return 0, err
	}
	return ok.Offset, nil
}

// DeclareQueue declares a queue and waits for the broker's confirmation.
func (e *Engine) DeclareQueue(d DeclareQueue) (DeclareQueueOk, error) {
	f, err := e.request(OpDeclareQueue, EncodeDeclareQueue(d))
	if err != nil {
		return DeclareQueueOk{}, err
	}
	return DecodeDeclareQueueOk(f.Payload)
}

// FetchTopology requests the cluster topology, optionally filtered.
func (e *Engine) FetchTopology(req TopologyRequest) (TopologyOk, error) {
	f, err := e.request(OpTopology, EncodeTopologyRequest(req))
	if err != nil {
		return TopologyOk{}, err
	}
	return DecodeTopologyOk(f.Payload)
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
		case f, ok := <-e.frameCh:
			if !ok {
				e.markDead(&DisconnectionError{Message: "connection closed by peer"})
			} else {
				e.handleFrame(f)
			}
		case <-ticker.C:
			e.tick()
		case <-e.stop:
			e.markDead(&BrokenPipeError{Message: "engine shutdown"})
		}
		if e.closed {
			return
		}
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
		e.waiters[id] = &waiter{reply: cmd.reply, subReply: cmd.subReply, autoAck: cmd.autoAck}
	}
	if err := e.write(BuildFrame(cmd.op, id, cmd.body)); err != nil {
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
		_ = e.write(BuildFrame(OpPong, f.RequestID, nil))
	case OpPublishOk, OpDeclareQueueOk, OpDeclarePlexusOk, OpTopologyOk:
		e.resolve(f.RequestID, reply{frame: f})
	case OpSubscribeOk:
		e.handleSubscribeOk(f)
	case OpDeliver:
		e.handleDeliver(f)
	case OpRedirect:
		// The broker routed us to a different owner. Fail the waiter with a typed
		// redirect the client layer acts on; not fatal to the connection.
		w, ok := e.waiters[f.RequestID]
		if !ok {
			return
		}
		delete(e.waiters, f.RequestID)
		if rd, err := DecodeRedirect(f.Payload); err != nil {
			failWaiter(w, err)
		} else {
			failWaiter(w, &RedirectError{Redirect: rd})
		}
	case OpError, OpSubscribeErr:
		em, _ := DecodeError(f.Payload)
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
		e.markDead(&DisconnectionError{Message: "heartbeat timeout: no response from the broker"})
		return
	}
	e.nextID++
	if err := e.write(BuildFrame(OpPing, e.nextID, nil)); err != nil {
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
	// Closing each delivery channel ends the consumer's range over it.
	for id, s := range e.subs {
		close(s.ch)
		delete(e.subs, id)
	}
	e.closeStop()
	_ = e.conn.Close()
}

// write is only ever called from the run goroutine, so the engine has a single
// writer and needs no write lock.
func (e *Engine) write(f Frame) error {
	_, err := e.conn.Write(EncodeFrame(f))
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
