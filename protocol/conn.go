// conn.go implements Conn, the connection dispatcher that sits directly on
// top of the framing (framing.go) and envelope (jsonrpc.go) layers: it reads
// frames one at a time via FrameReader, classifies each via ParseEnvelope,
// and routes the result to the right place — a registered request handler,
// a registered notification handler, or the pending-call table for a
// correlated response. It writes outgoing requests/responses/notifications
// through the single-writer Writer.
//
// Conn knows nothing about ACP's specific methods or domain types: it routes
// purely by the JSON-RPC method string. The typed ACP surface built on top
// of this is Task 1.7's concern.
package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// MaxInFlightHandlers bounds how many request/notification handler callbacks
// may run concurrently on one Conn. Requests and notifications beyond this
// bound queue (as goroutines parked on a semaphore) until a slot frees.
const MaxInFlightHandlers = 64

// NotifyBufferDepth bounds how many notifications for one not-yet-registered
// method a Conn will buffer before HandleNotify is called for that method.
// Once the depth is exceeded, the oldest buffered notification is dropped to
// make room for the newest, and the drop is counted (see
// Conn.DroppedNotifications).
const NotifyBufferDepth = 512

// HandlerFunc handles one incoming JSON-RPC request and returns the value to
// send back as the response result, or an error. If err is (or wraps) a
// *Fault, its Code/Message/Data are sent to the peer via ToWireError;
// any other error is reported to the peer as InternalError, with the
// original error kept only for local diagnosis (never sent on the wire).
type HandlerFunc func(ctx context.Context, method string, params json.RawMessage) (any, error)

// NotifyFunc handles one incoming JSON-RPC notification. It has no result:
// notifications never receive a response.
type NotifyFunc func(ctx context.Context, method string, params json.RawMessage)

// ConnOptions configures a Conn constructed by NewConn.
type ConnOptions struct {
	// ExtIDBase sets the starting id minted for extension-traffic calls (the
	// id space used internally for outgoing calls other than the ones Call
	// mints — see callExt). Zero means "use the default base of 1". This
	// exists so a later typed layer can mint extension-traffic ids from a
	// caller-chosen range without ever colliding with Call's id space, which
	// always starts at 1<<32 regardless of this option.
	ExtIDBase int64
}

// ConnClosedError is returned by Call and Notify once a Conn has been (or is
// being) closed, and delivered to every Call that was still in flight at the
// moment of closing. Close may have been explicit (Conn.Close) or implicit
// (the peer went away, or a read off the transport failed); Unwrap exposes
// that cause for local diagnosis, but it is never sent to a peer.
type ConnClosedError struct {
	cause error
}

func (e *ConnClosedError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("protocol: connection closed: %v", e.cause)
	}
	return "protocol: connection closed"
}

// Unwrap exposes the cause that led to closing, if any.
func (e *ConnClosedError) Unwrap() error { return e.cause }

// callResult is what a pending Call is waiting to receive: either a decoded
// success (raw json result bytes) or a failure.
type callResult struct {
	raw json.RawMessage
	err error
}

// Conn is one bidirectional JSON-RPC connection. It routes incoming requests
// to registered handlers and correlates outgoing requests with their
// responses.
//
// A single internal goroutine reads frames off r and dispatches them; Send
// calls made by that dispatch (auto-responses, and any handler's eventual
// response) and by callers of Call/Notify all funnel through the shared
// Writer, which already serializes concurrent writers onto w.
type Conn struct {
	fr     *FrameReader
	writer *Writer
	r      io.Reader
	w      io.Writer

	// mu guards closed/closeErr/pending together so that "is this Conn
	// still open" and "register/remove a pending entry" are always
	// observed atomically with respect to shutdown's fail-all sweep: no
	// interleaving of Call and shutdown can produce a pending entry nobody
	// will ever resolve, nor a fail-all sweep that misses one.
	mu       sync.Mutex
	closed   bool
	closeErr *ConnClosedError
	pending  map[ID]chan callResult

	// handlersMu guards all handler registration state, including the
	// per-method notification buffers used before HandleNotify is called.
	handlersMu     sync.Mutex
	handlers       map[string]HandlerFunc
	notifyHandlers map[string]NotifyFunc
	unknownRequest HandlerFunc
	unknownNotify  NotifyFunc
	notifyBuffers  map[string][]json.RawMessage

	droppedNotifications atomic.Uint64

	// nextCallID and nextExtID are disjoint id spaces for outgoing calls:
	// Call mints from nextCallID (starting at 1<<32), callExt mints from
	// nextExtID (starting at 1, or ConnOptions.ExtIDBase). Both are plain
	// incrementing counters; the actual collision-freedom comes from the
	// two ranges never overlapping, not from any coordination between them.
	nextCallID atomic.Int64
	nextExtID  atomic.Int64

	// sem is a counting semaphore (capacity MaxInFlightHandlers) that bounds
	// how many request handler callbacks run concurrently. A goroutine
	// spawned to run a handler blocks trying to send into sem until a slot
	// is free (or done closes), which is how "excess requests queue" is
	// implemented. Notification handlers do not use sem: see notifyMu et al.
	sem chan struct{}

	// notifyMu, notifyCond, notifyQueue, and notifyClosed implement an
	// unbounded FIFO job queue drained by a single dedicated goroutine
	// (notifyWorker), which is how notification handler *execution* (not
	// just invocation) is serialized in wire order. Both dispatchNotification
	// (live notifications) and HandleNotify (buffered-notification flush)
	// enqueue onto this same queue rather than running or spawning a handler
	// directly, so completion order always matches arrival order — including
	// across different notification methods, and including a buffered flush
	// relative to a live notification for the same method that arrives
	// concurrently with registration (see HandleNotify).
	notifyOrderMu sync.Mutex
	notifyMu      sync.Mutex
	notifyCond    *sync.Cond
	notifyQueue   []func()
	notifyClosed  bool

	done         chan struct{}
	readLoopDone chan struct{}
	closeOnce    sync.Once
}

// NewConn wraps r and w as one JSON-RPC connection and starts its internal
// read loop. The returned Conn is ready for Handle/HandleNotify registration
// and for Call/Notify immediately.
func NewConn(r io.Reader, w io.Writer, opts ConnOptions) *Conn {
	c := &Conn{
		fr:             NewFrameReader(r),
		writer:         NewWriter(w),
		r:              r,
		w:              w,
		pending:        make(map[ID]chan callResult),
		handlers:       make(map[string]HandlerFunc),
		notifyHandlers: make(map[string]NotifyFunc),
		notifyBuffers:  make(map[string][]json.RawMessage),
		sem:            make(chan struct{}, MaxInFlightHandlers),
		done:           make(chan struct{}),
		readLoopDone:   make(chan struct{}),
	}
	c.notifyCond = sync.NewCond(&c.notifyMu)

	// Call's id space starts at 1<<32 and counts up; storing base-1 lets
	// mintCallID uniformly use Add(1) to produce the first id.
	c.nextCallID.Store((int64(1) << 32) - 1)

	extBase := opts.ExtIDBase
	if extBase == 0 {
		extBase = 1
	}
	c.nextExtID.Store(extBase - 1)

	go c.readLoop()
	go c.notifyWorker()
	return c
}

// Done returns a channel that is closed once c has been (or is being)
// closed, whether by an explicit Close call or because reading from the
// peer failed.
func (c *Conn) Done() <-chan struct{} { return c.done }

// DroppedNotifications reports how many buffered early notifications have
// been dropped (oldest-first) across all methods due to NotifyBufferDepth
// overflow. Safe to call at any time, including after Close, and it reflects
// the final count once the Conn is closed.
func (c *Conn) DroppedNotifications() uint64 { return c.droppedNotifications.Load() }

// WaitForNotifications waits until all notification jobs enqueued before this
// call have finished executing in the Conn's ordered notification worker. It
// is cancellation-aware and returns *ConnClosedError if the Conn closes before
// the barrier reaches the worker. A caller should not invoke it from inside a
// notification handler, because that handler is itself ahead of the barrier.
func (c *Conn) WaitForNotifications(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	markerDone := make(chan struct{})
	c.notifyOrderMu.Lock()
	c.notifyMu.Lock()
	if c.notifyClosed {
		c.notifyMu.Unlock()
		c.notifyOrderMu.Unlock()
		return c.closedError()
	}
	c.notifyQueue = append(c.notifyQueue, func() { close(markerDone) })
	c.notifyMu.Unlock()
	c.notifyOrderMu.Unlock()
	c.notifyCond.Signal()

	select {
	case <-markerDone:
		return nil
	case <-c.done:
		return c.closedError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Conn) closedError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeErr != nil {
		return c.closeErr
	}
	return &ConnClosedError{}
}

// Handle registers h as the handler for incoming requests with the given
// method. A later call with the same method replaces the previous handler.
func (c *Conn) Handle(method string, h HandlerFunc) {
	c.handlersMu.Lock()
	c.handlers[method] = h
	c.handlersMu.Unlock()
}

// HandleNotify registers h as the handler for incoming notifications with
// the given method, then enqueues — in arrival order — any notifications for
// that method that were buffered before this call (see NotifyBufferDepth)
// for ordered execution on the same dispatch worker that runs live
// notifications. HandleNotify does not block waiting for that flush to run;
// it only guarantees the flushed notifications are queued ahead of any live
// notification for method that could possibly be dispatched after this call
// returns.
//
// The registration and the flush-enqueue happen under the same handlersMu
// critical section, which is what guarantees the ordering: dispatchNotification
// cannot observe the new handler (and thus cannot enqueue a live notification
// for method) until this call releases handlersMu, by which point every
// buffered notification for method has already been pushed onto the shared
// queue ahead of it.
func (c *Conn) HandleNotify(method string, h NotifyFunc) {
	c.notifyOrderMu.Lock()
	defer c.notifyOrderMu.Unlock()
	c.handlersMu.Lock()
	c.notifyHandlers[method] = h
	buffered := c.notifyBuffers[method]
	delete(c.notifyBuffers, method)
	for _, params := range buffered {
		params := params
		c.enqueueNotifyJob(func() { h(context.Background(), method, params) })
	}
	c.handlersMu.Unlock()
}

// HandleUnknownRequest registers a catch-all handler invoked for any
// incoming request whose method has no handler registered via Handle. This
// is meant for vendor/extension request methods (for example ACP's `_meta`
// passthrough) that this Conn does not know about by name.
func (c *Conn) HandleUnknownRequest(h HandlerFunc) {
	c.handlersMu.Lock()
	c.unknownRequest = h
	c.handlersMu.Unlock()
}

// HandleUnknownNotify registers a catch-all handler invoked for any incoming
// notification whose method has no handler registered via HandleNotify. Once
// this hook is set, it takes priority over buffering: an unrecognized
// notification method is routed to it immediately rather than being held for
// a HandleNotify registration that (by design, once this hook exists) is
// presumed never coming for that method.
func (c *Conn) HandleUnknownNotify(h NotifyFunc) {
	c.handlersMu.Lock()
	c.unknownNotify = h
	c.handlersMu.Unlock()
}

// Call sends a JSON-RPC request for method with params (marshaled to JSON;
// may be nil), blocks until the correlated response arrives, decodes its
// result into result (if non-nil and the call succeeded), and returns any
// error. It mints the request id from Call's own id space (starting at
// 1<<32), disjoint from callExt's.
//
// Call returns *ConnClosedError immediately if c is already closed, and
// every Call still blocked when c closes (for any reason) receives the same
// typed error. A canceled ctx unblocks Call with ctx.Err() and removes the
// pending entry; it never leaks.
func (c *Conn) Call(ctx context.Context, method string, params, result any) error {
	return c.call(ctx, method, params, result, c.mintCallID)
}

// callExt is Call's counterpart for extension traffic: it mints request ids
// from the ExtIDBase-configured id space instead of Call's. It is
// unexported for now — Task 1.7's typed surface is expected to expose it (or
// something built on it) for vendor/extension calls once that layer exists.
func (c *Conn) callExt(ctx context.Context, method string, params, result any) error {
	return c.call(ctx, method, params, result, c.mintExtID)
}

func (c *Conn) mintCallID() int64 { return c.nextCallID.Add(1) }
func (c *Conn) mintExtID() int64  { return c.nextExtID.Add(1) }

func (c *Conn) call(ctx context.Context, method string, params, result any, mintID func() int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	if c.closed {
		err := error(c.closeErr)
		c.mu.Unlock()
		return err
	}

	var raw json.RawMessage
	if params != nil {
		r, err := json.Marshal(params)
		if err != nil {
			c.mu.Unlock()
			return fmt.Errorf("protocol: marshal call params: %w", err)
		}
		raw = r
	}

	id := NewNumberID(mintID())
	resultCh := make(chan callResult, 1)
	c.pending[id] = resultCh
	c.mu.Unlock()

	req := &Request{ID: id, Method: method, Params: raw}
	if sendErr := c.writer.SendContext(ctx, req); sendErr != nil {
		// If our entry is still in the pending table, nobody else will ever
		// resolve it (a shutdown that already ran would have removed it as
		// part of its fail-all sweep before the writer could report
		// closed — see shutdown), so this call owns reporting sendErr and
		// must clean up its own entry. If the entry is already gone, some
		// concurrent event (shutdown's sweep, or in principle a response)
		// has already produced an authoritative result, so fall through and
		// prefer that instead of the raw send error.
		c.mu.Lock()
		_, stillPending := c.pending[id]
		if stillPending {
			delete(c.pending, id)
		}
		c.mu.Unlock()
		if stillPending {
			return sendErr
		}
	}

	select {
	case res := <-resultCh:
		return deliverCallResult(res, result)
	case <-ctx.Done():
		c.mu.Lock()
		_, stillPending := c.pending[id]
		if stillPending {
			delete(c.pending, id)
		}
		c.mu.Unlock()
		if stillPending {
			return ctx.Err()
		}
		// The entry was resolved concurrently between ctx firing and us
		// acquiring the lock (a response arrived, or shutdown swept it):
		// prefer that real result over ctx.Err() rather than discarding it.
		select {
		case res := <-resultCh:
			return deliverCallResult(res, result)
		default:
			return ctx.Err()
		}
	}
}

func deliverCallResult(res callResult, result any) error {
	if res.err != nil {
		return res.err
	}
	if result != nil && len(res.raw) > 0 {
		if err := json.Unmarshal(res.raw, result); err != nil {
			return fmt.Errorf("protocol: unmarshal call result: %w", err)
		}
	}
	return nil
}

// Notify sends a JSON-RPC notification for method with params (marshaled to
// JSON; may be nil). It does not wait for anything: notifications have no
// response. Notify returns *ConnClosedError immediately if c is already
// closed.
func (c *Conn) Notify(ctx context.Context, method string, params any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.mu.Lock()
	if c.closed {
		err := error(c.closeErr)
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()

	var raw json.RawMessage
	if params != nil {
		r, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("protocol: marshal notify params: %w", err)
		}
		raw = r
	}

	n := &Notification{Method: method, Params: raw}
	return c.writer.SendContext(ctx, n)
}

// Close stops c from accepting further Call/Notify calls (which now fail
// fast with *ConnClosedError), fails every currently in-flight Call the same
// way, stops the internal writer, and closes the underlying reader and
// writer (whichever of them implement io.Closer) so the read loop's blocked
// Read unblocks and the peer observes our side going away. It waits for the
// read loop goroutine to fully exit before returning, but does not wait for
// any handler callback that is still running in user code — a slow or stuck
// handler cannot delay Close.
//
// Close is idempotent and safe to call concurrently with itself or with any
// other Conn method.
func (c *Conn) Close() error {
	c.shutdown(nil)
	<-c.readLoopDone
	return nil
}

// shutdown performs the one-time transition to closed, however it was
// triggered: an explicit Close (cause == nil), or the read loop observing a
// transport failure (cause == that error, including plain io.EOF for a
// clean peer disconnect). It never blocks on a handler callback.
func (c *Conn) shutdown(cause error) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.closeErr = &ConnClosedError{cause: cause}
		pending := c.pending
		c.pending = make(map[ID]chan callResult)
		c.mu.Unlock()

		close(c.done)

		c.notifyMu.Lock()
		c.notifyClosed = true
		c.notifyMu.Unlock()
		c.notifyCond.Broadcast()

		for _, ch := range pending {
			ch <- callResult{err: c.closeErr}
		}

		// Interrupt raw I/O before waiting for admitted Writer sends. A
		// blocked Write must see the transport close or Writer.Close can wait
		// forever, preventing the transport and child process from being
		// interrupted at all.
		if closer, ok := c.r.(io.Closer); ok {
			_ = closer.Close()
		}
		if any(c.w) != any(c.r) {
			if closer, ok := c.w.(io.Closer); ok {
				_ = closer.Close()
			}
		}
		_ = c.writer.Close()
	})
}

// readLoop is the single goroutine that reads frames off the transport,
// classifies them, and dispatches them. It owns FrameReader exclusively (per
// FrameReader's single-reader-loop contract) and never blocks on a handler
// callback: request dispatch hands off to a spawned goroutine gated by sem,
// and notification dispatch enqueues onto the notify worker's unbounded
// queue (see enqueueNotifyJob), so a slow handler cannot stall reading
// subsequent frames (in particular, responses correlating other in-flight
// Calls).
func (c *Conn) readLoop() {
	defer close(c.readLoopDone)
	for {
		frame, err := c.fr.ReadFrame()
		if err != nil {
			// Any read failure — a clean io.EOF on peer disconnect, a
			// truncated frame, or a genuine transport error — means this
			// Conn can make no further progress: treat it exactly like an
			// explicit Close so every in-flight Call fails the same way.
			c.shutdown(err)
			return
		}

		env, err := ParseEnvelope(frame)
		if err != nil {
			// Malformed input from the peer. There is no reliable id to
			// respond to (ParseEnvelope failing means the shape itself,
			// possibly including the id, could not be trusted), so this
			// frame is dropped and reading continues rather than tearing
			// down the whole connection over one bad message.
			continue
		}

		switch env.Kind() {
		case KindRequest:
			c.dispatchRequest(env.Request)
		case KindNotification:
			c.dispatchNotification(env.Notification)
		case KindResponse:
			c.correlateResponse(env.Response)
		}
	}
}

// correlateResponse delivers resp to the pending Call waiting on its id, if
// any. A response with no matching pending entry (already resolved by a
// concurrent cancel, or simply a bogus/late id from the peer) is dropped.
func (c *Conn) correlateResponse(resp *Response) {
	c.mu.Lock()
	ch, ok := c.pending[resp.ID]
	if ok {
		delete(c.pending, resp.ID)
	}
	c.mu.Unlock()
	if !ok {
		return
	}

	if resp.Error != nil {
		ch <- callResult{err: FromWireError(resp.Error)}
		return
	}
	ch <- callResult{raw: resp.Result}
}

// dispatchRequest routes an incoming request to its registered handler, the
// unknown-request catch-all, or (if neither exists) responds immediately
// with MethodNotFound. The handler itself always runs in a spawned,
// sem-gated goroutine so the read loop is never blocked by it.
func (c *Conn) dispatchRequest(req *Request) {
	c.handlersMu.Lock()
	h, ok := c.handlers[req.Method]
	if !ok {
		h = c.unknownRequest
	}
	c.handlersMu.Unlock()

	if h == nil {
		resp := &Response{
			ID:    req.ID,
			Error: ToWireError(MethodNotFound(fmt.Sprintf("method not found: %s", req.Method), nil)),
		}
		_ = c.writer.Send(resp)
		return
	}

	c.spawnHandler(func() {
		result, err := h(context.Background(), req.Method, req.Params)
		resp := c.buildResponse(req.ID, result, err)
		_ = c.writer.Send(resp)
	})
}

// buildResponse converts a handler's (result, error) pair into the Response
// to send back. A *Fault (directly returned, or reachable via errors.As)
// carries its Code/Message/whitelisted Data to the wire; any other error is
// reported as InternalError with the original error kept only in the local
// Fault for diagnosis, never serialized.
func (c *Conn) buildResponse(id ID, result any, err error) *Response {
	if err != nil {
		var f *Fault
		if !errors.As(err, &f) {
			f = InternalError(err.Error(), err)
		}
		return &Response{ID: id, Error: ToWireError(f)}
	}

	raw, merr := json.Marshal(result)
	if merr != nil {
		return &Response{ID: id, Error: ToWireError(InternalError("marshal handler result", merr))}
	}
	return &Response{ID: id, Result: raw}
}

// dispatchNotification routes an incoming notification to its registered
// handler or the unknown-notify catch-all, both enqueued onto the shared
// notify worker queue (see notifyWorker) so that handler *completion* order
// matches wire order across all notifications on this Conn, not just
// invocation order. If neither a handler nor the catch-all exists, the
// notification is buffered (bounded, drop-oldest-on-overflow) for a
// HandleNotify registration that may come later.
func (c *Conn) dispatchNotification(n *Notification) {
	c.notifyOrderMu.Lock()
	defer c.notifyOrderMu.Unlock()
	c.handlersMu.Lock()

	if h, ok := c.notifyHandlers[n.Method]; ok {
		c.handlersMu.Unlock()
		method, params := n.Method, n.Params
		c.enqueueNotifyJob(func() { h(context.Background(), method, params) })
		return
	}

	if h := c.unknownNotify; h != nil {
		c.handlersMu.Unlock()
		method, params := n.Method, n.Params
		c.enqueueNotifyJob(func() { h(context.Background(), method, params) })
		return
	}

	buf := append(c.notifyBuffers[n.Method], n.Params)
	if drop := len(buf) - NotifyBufferDepth; drop > 0 {
		buf = buf[drop:]
		// drop is bounded above by len(buf) (an in-memory slice length,
		// never anywhere near the int64/uint64 boundary) and just proved
		// positive, so this conversion cannot wrap.
		c.droppedNotifications.Add(uint64(int64(drop)))
	}
	c.notifyBuffers[n.Method] = buf

	c.handlersMu.Unlock()
}

// enqueueNotifyJob appends job to the notify worker's queue and wakes it.
// job is abandoned (never run) if the Conn has already started shutting
// down, matching spawnHandler's "abandon rather than run" behavior for
// handler work that has not yet started. Never blocks: the queue is an
// unbounded slice guarded by notifyMu, not a fixed-capacity channel, so a
// slow (or stalled) notifyWorker can never make a caller — in particular the
// read loop, via dispatchNotification — block here.
func (c *Conn) enqueueNotifyJob(job func()) {
	c.notifyMu.Lock()
	if c.notifyClosed {
		c.notifyMu.Unlock()
		return
	}
	c.notifyQueue = append(c.notifyQueue, job)
	c.notifyMu.Unlock()
	c.notifyCond.Signal()
}

// notifyWorker is the single goroutine that drains the notify queue,
// running each queued job to completion before starting the next: this
// strict one-at-a-time draining in enqueue order is what makes notification
// handler completion order match wire order. It exits once the Conn is
// shutting down, abandoning (not running) whatever is still queued at that
// point — it never waits for a job it has already started, matching Close's
// "does not wait for any handler callback still running" contract, and it
// never blocks Close from returning.
func (c *Conn) notifyWorker() {
	for {
		job, ok := c.nextNotifyJob()
		if !ok {
			return
		}
		job()
	}
}

// nextNotifyJob blocks until a job is available or the Conn starts shutting
// down. Once notifyClosed is set, it returns (nil, false) immediately even
// if jobs remain queued, abandoning them.
func (c *Conn) nextNotifyJob() (func(), bool) {
	c.notifyMu.Lock()
	defer c.notifyMu.Unlock()
	for len(c.notifyQueue) == 0 && !c.notifyClosed {
		c.notifyCond.Wait()
	}
	if c.notifyClosed {
		return nil, false
	}
	job := c.notifyQueue[0]
	c.notifyQueue[0] = nil
	c.notifyQueue = c.notifyQueue[1:]
	return job, true
}

// spawnHandler runs fn in a new goroutine once a slot in sem is available,
// or abandons the attempt (without running fn) if c closes first. This is
// how both the MaxInFlightHandlers concurrency cap and "excess requests
// queue" are implemented: the goroutine itself is cheap to leave parked on
// the semaphore send.
func (c *Conn) spawnHandler(fn func()) {
	go func() {
		select {
		case c.sem <- struct{}{}:
			defer func() { <-c.sem }()
			fn()
		case <-c.done:
		}
	}()
}

// pendingLen reports the number of in-flight Calls currently registered in
// the pending table. Unexported: a white-box test hook only (see
// conn_internal_test.go), not part of Conn's public contract.
func (c *Conn) pendingLen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}
