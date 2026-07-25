package protocol_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
)

// assertNoGoroutineLeak captures the current goroutine count and registers a
// t.Cleanup that fails the test if the count has not returned to that
// baseline within 1s. Call it before creating any Conn under test.
func assertNoGoroutineLeak(t *testing.T) {
	t.Helper()
	// Let any prior test's goroutines finish unwinding before we sample our
	// own baseline.
	runtime.Gosched()
	baseline := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(1 * time.Second)
		for {
			n := runtime.NumGoroutine()
			if n <= baseline {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("goroutine leak: NumGoroutine() = %d, want <= %d (baseline) within 1s", n, baseline)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
}

// pipeConns returns two protocol.Conns wired together over a net.Pipe, one
// playing "client" and one "server" purely by convention of the test.
func pipeConns(t *testing.T) (client, server *protocol.Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	client = protocol.NewConn(c1, c1, protocol.ConnOptions{})
	server = protocol.NewConn(c2, c2, protocol.ConnOptions{})
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

type echoParams struct {
	Value string `json:"value"`
}

type echoResult struct {
	Value string `json:"value"`
}

// --- request -> handler -> response with same id; unknown method ---

func TestConnRequestRoutesToHandlerAndRespondsWithSameID(t *testing.T) {
	assertNoGoroutineLeak(t)

	c1, c2 := net.Pipe()
	server := protocol.NewConn(c2, c2, protocol.ConnOptions{})
	t.Cleanup(func() { server.Close() })

	server.Handle("echo", func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		var p echoParams
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, err
		}
		return echoResult{Value: "got:" + p.Value}, nil
	})

	// Drive the client side with raw wire primitives (not protocol.Conn) so
	// the response's id can be checked byte-for-byte against what was sent,
	// independent of Conn's own id-minting.
	w := protocol.NewWriter(c1)
	fr := protocol.NewFrameReader(c1)
	t.Cleanup(func() { w.Close() })

	req := &protocol.Request{
		ID:     protocol.NewNumberID(42),
		Method: "echo",
		Params: mustMarshal(t, echoParams{Value: "hi"}),
	}
	if err := w.Send(req); err != nil {
		t.Fatalf("Send(request) error = %v", err)
	}

	frame, err := fr.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}
	env, err := protocol.ParseEnvelope(frame)
	if err != nil {
		t.Fatalf("ParseEnvelope() error = %v", err)
	}
	if env.Kind() != protocol.KindResponse {
		t.Fatalf("Kind() = %v, want KindResponse", env.Kind())
	}
	resp := env.Response
	if resp.Error != nil {
		t.Fatalf("Response.Error = %v, want nil", resp.Error)
	}
	gotID, ok := resp.ID.Number()
	if !ok || gotID != 42 {
		t.Fatalf("Response.ID = (%v, %v), want (42, true)", gotID, ok)
	}
	var result echoResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatalf("Unmarshal(Result) error = %v", err)
	}
	if result.Value != "got:hi" {
		t.Fatalf("result.Value = %q, want %q", result.Value, "got:hi")
	}
}

func TestConnUnknownMethodReturnsMethodNotFound(t *testing.T) {
	assertNoGoroutineLeak(t)

	c1, c2 := net.Pipe()
	server := protocol.NewConn(c2, c2, protocol.ConnOptions{})
	t.Cleanup(func() { server.Close() })

	w := protocol.NewWriter(c1)
	fr := protocol.NewFrameReader(c1)
	t.Cleanup(func() { w.Close() })

	req := &protocol.Request{
		ID:     protocol.NewStringID("req-1"),
		Method: "nonexistent.method",
	}
	if err := w.Send(req); err != nil {
		t.Fatalf("Send(request) error = %v", err)
	}

	frame, err := fr.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}
	env, err := protocol.ParseEnvelope(frame)
	if err != nil {
		t.Fatalf("ParseEnvelope() error = %v", err)
	}
	resp := env.Response
	if resp == nil {
		t.Fatalf("Response = nil, want a response with MethodNotFound")
	}
	gotID, ok := resp.ID.String()
	if !ok || gotID != "req-1" {
		t.Fatalf("Response.ID = (%v, %v), want (\"req-1\", true)", gotID, ok)
	}
	if resp.Error == nil {
		t.Fatalf("Response.Error = nil, want MethodNotFound")
	}
	if resp.Error.Code != protocol.ErrorCodeMethodNotFound {
		t.Fatalf("Response.Error.Code = %d, want %d", resp.Error.Code, protocol.ErrorCodeMethodNotFound)
	}
}

// --- fail-all on termination ---

func TestConnFailAllInFlightCallsOnPeerClose(t *testing.T) {
	assertNoGoroutineLeak(t)

	client, server := pipeConns(t)

	blockRelease := make(chan struct{})
	var reachedCount int64
	server.Handle("block", func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		atomic.AddInt64(&reachedCount, 1)
		<-blockRelease
		return "unused", nil
	})
	defer close(blockRelease)

	const n = 5
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var result string
			errs[i] = client.Call(context.Background(), "block", nil, &result)
		}(i)
	}

	// Wait until every call has actually reached (and blocked inside) the
	// server's handler before severing the connection, so the close race
	// under test is deterministically "n calls truly in flight", not
	// however many happened to land within an arbitrary sleep window.
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&reachedCount) < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := atomic.LoadInt64(&reachedCount); got != n {
		t.Fatalf("reachedCount = %d, want %d before closing (calls never reached the server)", got, n)
	}

	if err := server.Close(); err != nil {
		t.Fatalf("server.Close() error = %v", err)
	}

	wg.Wait()
	for i, err := range errs {
		var closedErr *protocol.ConnClosedError
		if !errors.As(err, &closedErr) {
			t.Errorf("call[%d] error = %v (%T), want *ConnClosedError", i, err, err)
		}
	}
}

func TestConnCallAfterCloseFailsFast(t *testing.T) {
	assertNoGoroutineLeak(t)

	client, _ := pipeConns(t)
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	done := make(chan error, 1)
	go func() {
		var result string
		done <- client.Call(context.Background(), "whatever", nil, &result)
	}()

	select {
	case err := <-done:
		var closedErr *protocol.ConnClosedError
		if !errors.As(err, &closedErr) {
			t.Fatalf("Call() after Close error = %v (%T), want *ConnClosedError", err, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Call() after Close never returned (did not fail fast)")
	}
}

func TestConnDoneClosesOnClose(t *testing.T) {
	assertNoGoroutineLeak(t)

	client, _ := pipeConns(t)
	select {
	case <-client.Done():
		t.Fatalf("Done() closed before Close()")
	default:
	}
	client.Close()
	select {
	case <-client.Done():
	case <-time.After(time.Second):
		t.Fatalf("Done() never closed after Close()")
	}
}

// --- concurrency cap ---

func TestConnHandlerConcurrencyCap(t *testing.T) {
	assertNoGoroutineLeak(t)

	client, server := pipeConns(t)

	var current int64
	var peak int64
	release := make(chan struct{})
	var releaseOnce sync.Once

	server.Handle("work", func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		n := atomic.AddInt64(&current, 1)
		for {
			old := atomic.LoadInt64(&peak)
			if n <= old || atomic.CompareAndSwapInt64(&peak, old, n) {
				break
			}
		}
		if n == int64(protocol.MaxInFlightHandlers) {
			releaseOnce.Do(func() { close(release) })
		}
		select {
		case <-release:
		case <-time.After(5 * time.Second):
		}
		atomic.AddInt64(&current, -1)
		return "ok", nil
	})

	const total = protocol.MaxInFlightHandlers + 36
	var wg sync.WaitGroup
	errs := make([]error, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var result string
			errs[i] = client.Call(context.Background(), "work", nil, &result)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("call[%d] error = %v", i, err)
		}
	}
	if got := atomic.LoadInt64(&peak); got != int64(protocol.MaxInFlightHandlers) {
		t.Errorf("peak concurrent handlers = %d, want exactly %d", got, protocol.MaxInFlightHandlers)
	}
	if got := atomic.LoadInt64(&current); got != 0 {
		t.Errorf("current concurrent handlers after completion = %d, want 0", got)
	}
}

// --- buffered early notifications ---

func TestConnBufferedNotificationsFlushInOrderOnRegister(t *testing.T) {
	assertNoGoroutineLeak(t)

	client, server := pipeConns(t)

	// A no-op handler used purely as a synchronization point: since a single
	// Conn read loop processes frames strictly in wire order, once this
	// Call's response comes back, every notification sent before it has
	// already been buffered by the server.
	server.Handle("sync", func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		return "ok", nil
	})

	const n = 10
	for i := 0; i < n; i++ {
		if err := client.Notify(context.Background(), "buffered.thing", echoParams{Value: strconv.Itoa(i)}); err != nil {
			t.Fatalf("Notify(%d) error = %v", i, err)
		}
	}
	var syncResult string
	if err := client.Call(context.Background(), "sync", nil, &syncResult); err != nil {
		t.Fatalf("Call(sync) error = %v", err)
	}

	var got []string
	var mu sync.Mutex
	flushed := make(chan struct{})
	var flushCount int
	server.HandleNotify("buffered.thing", func(ctx context.Context, method string, params json.RawMessage) {
		var p echoParams
		if err := json.Unmarshal(params, &p); err != nil {
			t.Errorf("unmarshal notify params: %v", err)
			return
		}
		mu.Lock()
		got = append(got, p.Value)
		flushCount++
		if flushCount == n {
			close(flushed)
		}
		mu.Unlock()
	})

	select {
	case <-flushed:
	case <-time.After(2 * time.Second):
		t.Fatalf("buffered notifications never flushed (got %d of %d)", len(got), n)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != n {
		t.Fatalf("got %d flushed notifications, want %d: %v", len(got), n, got)
	}
	for i, v := range got {
		if v != strconv.Itoa(i) {
			t.Errorf("flushed[%d] = %q, want %q (order not preserved)", i, v, strconv.Itoa(i))
		}
	}
}

func TestConnBufferedNotificationsOverflowDropsOldest(t *testing.T) {
	assertNoGoroutineLeak(t)

	client, server := pipeConns(t)

	server.Handle("sync", func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		return "ok", nil
	})

	const n = protocol.NotifyBufferDepth + 20
	for i := 0; i < n; i++ {
		if err := client.Notify(context.Background(), "overflow.thing", echoParams{Value: strconv.Itoa(i)}); err != nil {
			t.Fatalf("Notify(%d) error = %v", i, err)
		}
	}
	var syncResult string
	if err := client.Call(context.Background(), "sync", nil, &syncResult); err != nil {
		t.Fatalf("Call(sync) error = %v", err)
	}

	if dropped := server.DroppedNotifications(); dropped != 20 {
		t.Fatalf("DroppedNotifications() = %d, want 20", dropped)
	}

	var got []string
	var mu sync.Mutex
	flushed := make(chan struct{})
	server.HandleNotify("overflow.thing", func(ctx context.Context, method string, params json.RawMessage) {
		var p echoParams
		if err := json.Unmarshal(params, &p); err != nil {
			t.Errorf("unmarshal notify params: %v", err)
			return
		}
		mu.Lock()
		got = append(got, p.Value)
		if len(got) == protocol.NotifyBufferDepth {
			close(flushed)
		}
		mu.Unlock()
	})

	select {
	case <-flushed:
	case <-time.After(2 * time.Second):
		t.Fatalf("buffered notifications never flushed (got %d)", len(got))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != protocol.NotifyBufferDepth {
		t.Fatalf("got %d flushed notifications, want %d", len(got), protocol.NotifyBufferDepth)
	}
	// Oldest 20 (0..19) must have been dropped: the surviving window is
	// [20, n).
	for i, v := range got {
		want := strconv.Itoa(i + 20)
		if v != want {
			t.Fatalf("flushed[%d] = %q, want %q (drop-oldest order violated)", i, v, want)
		}
	}
}

// --- typed + catch-all extension passthrough ---

func TestConnUnknownRequestAndNotifyHooks(t *testing.T) {
	assertNoGoroutineLeak(t)

	client, server := pipeConns(t)

	type captured struct {
		method string
		params string
	}
	reqCh := make(chan captured, 1)
	server.HandleUnknownRequest(func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		reqCh <- captured{method: method, params: string(params)}
		return echoResult{Value: "hook-handled"}, nil
	})

	notifyCh := make(chan captured, 1)
	server.HandleUnknownNotify(func(ctx context.Context, method string, params json.RawMessage) {
		notifyCh <- captured{method: method, params: string(params)}
	})

	var result echoResult
	if err := client.Call(context.Background(), "vendor/thing", echoParams{Value: "x"}, &result); err != nil {
		t.Fatalf("Call(vendor/thing) error = %v", err)
	}
	if result.Value != "hook-handled" {
		t.Fatalf("result.Value = %q, want %q", result.Value, "hook-handled")
	}
	select {
	case c := <-reqCh:
		if c.method != "vendor/thing" {
			t.Errorf("captured request method = %q, want %q", c.method, "vendor/thing")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("HandleUnknownRequest hook never invoked")
	}

	if err := client.Notify(context.Background(), "vendor/event", echoParams{Value: "y"}); err != nil {
		t.Fatalf("Notify(vendor/event) error = %v", err)
	}
	select {
	case c := <-notifyCh:
		if c.method != "vendor/event" {
			t.Errorf("captured notify method = %q, want %q", c.method, "vendor/event")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("HandleUnknownNotify hook never invoked")
	}
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	return raw
}
