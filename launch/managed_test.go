// managed_test.go proves Dial/ManagedClient's owned/shared proxy lifecycle
// against fake ModelProxy, HarnessAdapter, and connCloser implementations --
// no real ACP peer or subprocess is spawned anywhere in this file. dial's
// own connect step is fully substitutable (see connectFunc), so the
// ordering/unwind/close/death-observation contract is provable in isolation
// from acp/client's own already-thoroughly-tested Dial/Close behavior.
package launch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/transport/stdio"
)

// callOrder records the order fake Start/Configure/connect calls happen in,
// guarded for concurrent use even though this file's own tests drive it
// sequentially.
type callOrder struct {
	mu  sync.Mutex
	seq []string
}

func (o *callOrder) record(step string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seq = append(o.seq, step)
}

func (o *callOrder) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.seq...)
}

type fakeProxy struct {
	mu       sync.Mutex
	order    *callOrder
	starts   int
	closes   int
	startErr error
	closeErr error
	baseURL  string
	token    string
	ready    bool
	// preClose, if set, runs synchronously at the start of Close, before
	// closeErr is consulted -- used to observe ordering against another
	// fake (e.g. asserting a connCloser was already closed by this point).
	preClose func()
}

func (p *fakeProxy) Start(ctx context.Context) error {
	p.mu.Lock()
	p.starts++
	err := p.startErr
	p.mu.Unlock()
	if p.order != nil {
		p.order.record("proxy.Start")
	}
	return err
}

func (p *fakeProxy) Binding() (string, string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.baseURL, p.token, p.ready
}

func (p *fakeProxy) Close(ctx context.Context) error {
	if p.preClose != nil {
		p.preClose()
	}
	p.mu.Lock()
	p.closes++
	err := p.closeErr
	p.mu.Unlock()
	if p.order != nil {
		p.order.record("proxy.Close")
	}
	return err
}

func (p *fakeProxy) counts() (starts, closes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.starts, p.closes
}

type fakeHarness struct {
	mu    sync.Mutex
	order *callOrder
	calls int
	cmd   stdio.Command
	bind  ProxyBinding
	err   error
}

func (h *fakeHarness) Configure(cmd stdio.Command, binding ProxyBinding) (stdio.Command, error) {
	h.mu.Lock()
	h.calls++
	h.cmd, h.bind = cmd, binding
	err := h.err
	h.mu.Unlock()
	if h.order != nil {
		h.order.record("harness.Configure")
	}
	if err != nil {
		return stdio.Command{}, err
	}
	return cmd, nil
}

func (h *fakeHarness) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func (h *fakeHarness) lastBinding() ProxyBinding {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.bind
}

type fakeConn struct {
	mu       sync.Mutex
	closes   int
	closeErr error
	done     chan struct{}
}

func newFakeConn() *fakeConn { return &fakeConn{done: make(chan struct{})} }

func (c *fakeConn) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	return c.closeErr
}

func (c *fakeConn) Done() <-chan struct{} { return c.done }

func (c *fakeConn) kill() { close(c.done) }

func (c *fakeConn) closesSnapshot() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

// fakeConnect returns a connectFunc that always succeeds with conn,
// recording into order if non-nil.
func fakeConnect(conn connCloser, order *callOrder) connectFunc {
	return func(ctx context.Context, cmd stdio.Command, opts client.Options) (connCloser, error) {
		if order != nil {
			order.record("connect")
		}
		return conn, nil
	}
}

// fakeConnectFailing returns a connectFunc that always fails with err.
func fakeConnectFailing(err error) connectFunc {
	return func(ctx context.Context, cmd stdio.Command, opts client.Options) (connCloser, error) {
		return nil, err
	}
}

// failIfCalledConnect fails the test if the returned connectFunc is ever
// invoked -- used to prove dial short-circuits before ever attempting to
// connect.
func failIfCalledConnect(t *testing.T) connectFunc {
	t.Helper()
	return func(ctx context.Context, cmd stdio.Command, opts client.Options) (connCloser, error) {
		t.Fatal("connect was called, want dial to have already failed before reaching it")
		return nil, nil
	}
}

func readyProxy() *fakeProxy {
	return &fakeProxy{ready: true, baseURL: "http://proxy.local:9", token: "proxy-token"}
}

// --- exactly one of owned/shared is required ---

func TestDialRequiresExactlyOneProxy(t *testing.T) {
	ctx := context.Background()

	t.Run("neither set", func(t *testing.T) {
		harness := &fakeHarness{}
		_, err := dial(ctx, Config{Harness: harness}, failIfCalledConnect(t))
		var cfgErr *ConfigError
		if !errors.As(err, &cfgErr) {
			t.Fatalf("dial() error = %v (%T), want *ConfigError", err, err)
		}
	})

	t.Run("both set", func(t *testing.T) {
		harness := &fakeHarness{}
		proxy := readyProxy()
		shared := &ProxyBinding{BaseURL: "http://shared", Token: "shared-tok"}
		_, err := dial(ctx, Config{OwnedProxy: proxy, SharedProxy: shared, Harness: harness}, failIfCalledConnect(t))
		var cfgErr *ConfigError
		if !errors.As(err, &cfgErr) {
			t.Fatalf("dial() error = %v (%T), want *ConfigError", err, err)
		}
		if starts, closes := proxy.counts(); starts != 0 || closes != 0 {
			t.Errorf("proxy starts/closes = %d/%d, want 0/0: an invalid config must never touch either proxy", starts, closes)
		}
		if harness.callCount() != 0 {
			t.Errorf("harness.Configure called %d times, want 0", harness.callCount())
		}
	})

	t.Run("no harness", func(t *testing.T) {
		proxy := readyProxy()
		_, err := dial(ctx, Config{OwnedProxy: proxy}, failIfCalledConnect(t))
		var cfgErr *ConfigError
		if !errors.As(err, &cfgErr) {
			t.Fatalf("dial() error = %v (%T), want *ConfigError", err, err)
		}
		if starts, _ := proxy.counts(); starts != 0 {
			t.Errorf("proxy.Start called %d times, want 0", starts)
		}
	})
}

// --- owned startup precedes command configuration and ACP dial ---

func TestDialOwnedProxyOrdering(t *testing.T) {
	order := &callOrder{}
	proxy := readyProxy()
	proxy.order = order
	harness := &fakeHarness{order: order}
	conn := newFakeConn()

	mc, err := dial(context.Background(), Config{
		OwnedProxy: proxy,
		Harness:    harness,
		Command:    stdio.Command{Path: "/bin/claude-agent-acp"},
	}, fakeConnect(conn, order))
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}

	want := []string{"proxy.Start", "harness.Configure", "connect"}
	if got := order.snapshot(); !equalStrings(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}

	if got, want := harness.lastBinding(), (ProxyBinding{BaseURL: proxy.baseURL, Token: proxy.token}); got != want {
		t.Errorf("harness configured with binding %+v, want %+v (the owned proxy's own Binding(), not anything else)", got, want)
	}

	if mc.owned == nil {
		t.Error("ManagedClient.owned = nil, want the owned proxy tracked")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- failures unwind in reverse order ---

func TestDialFailureUnwindsOwnedProxy(t *testing.T) {
	bootErr := errors.New("boot failed")

	t.Run("proxy start fails: never configured, never connected, never closed", func(t *testing.T) {
		proxy := readyProxy()
		proxy.startErr = bootErr
		harness := &fakeHarness{}

		_, err := dial(context.Background(), Config{
			OwnedProxy: proxy,
			Harness:    harness,
			Command:    stdio.Command{Path: "/bin/claude-agent-acp"},
		}, failIfCalledConnect(t))
		if !errors.Is(err, bootErr) {
			t.Fatalf("dial() error = %v, want to wrap %v", err, bootErr)
		}
		if harness.callCount() != 0 {
			t.Errorf("harness.Configure called %d times, want 0", harness.callCount())
		}
		if starts, closes := proxy.counts(); starts != 1 || closes != 0 {
			t.Errorf("proxy starts/closes = %d/%d, want 1/0: a proxy whose own Start failed was never actually running, so there is nothing to close", starts, closes)
		}
	})

	t.Run("configure fails: owned proxy closed, never connected", func(t *testing.T) {
		proxy := readyProxy()
		harness := &fakeHarness{err: bootErr}

		_, err := dial(context.Background(), Config{
			OwnedProxy: proxy,
			Harness:    harness,
			Command:    stdio.Command{Path: "/bin/claude-agent-acp"},
		}, failIfCalledConnect(t))
		if !errors.Is(err, bootErr) {
			t.Fatalf("dial() error = %v, want to wrap %v", err, bootErr)
		}
		if starts, closes := proxy.counts(); starts != 1 || closes != 1 {
			t.Errorf("proxy starts/closes = %d/%d, want 1/1: Configure failing after a successful Start must close the owned proxy", starts, closes)
		}
	})

	t.Run("connect fails: owned proxy closed after a successful configure", func(t *testing.T) {
		proxy := readyProxy()
		harness := &fakeHarness{}

		_, err := dial(context.Background(), Config{
			OwnedProxy: proxy,
			Harness:    harness,
			Command:    stdio.Command{Path: "/bin/claude-agent-acp"},
		}, fakeConnectFailing(bootErr))
		if !errors.Is(err, bootErr) {
			t.Fatalf("dial() error = %v, want to wrap %v", err, bootErr)
		}
		if harness.callCount() != 1 {
			t.Errorf("harness.Configure called %d times, want 1", harness.callCount())
		}
		if starts, closes := proxy.counts(); starts != 1 || closes != 1 {
			t.Errorf("proxy starts/closes = %d/%d, want 1/1: a failed ACP dial after a successful Start must close the owned proxy", starts, closes)
		}
	})

	t.Run("proxy not ready after Start: closed, never configured", func(t *testing.T) {
		proxy := &fakeProxy{ready: false}
		harness := &fakeHarness{}

		_, err := dial(context.Background(), Config{
			OwnedProxy: proxy,
			Harness:    harness,
			Command:    stdio.Command{Path: "/bin/claude-agent-acp"},
		}, failIfCalledConnect(t))
		var notReady *ProxyNotReadyError
		if !errors.As(err, &notReady) {
			t.Fatalf("dial() error = %v (%T), want *ProxyNotReadyError", err, err)
		}
		if harness.callCount() != 0 {
			t.Errorf("harness.Configure called %d times, want 0", harness.callCount())
		}
		if starts, closes := proxy.counts(); starts != 1 || closes != 1 {
			t.Errorf("proxy starts/closes = %d/%d, want 1/1", starts, closes)
		}
	})
}

// --- shared proxies are never started or closed ---

func TestSharedProxyNeverStartedOrClosed(t *testing.T) {
	shared := &ProxyBinding{BaseURL: "http://shared", Token: "shared-tok"}
	harness := &fakeHarness{}
	conn := newFakeConn()

	mc, err := dial(context.Background(), Config{
		SharedProxy: shared,
		Harness:     harness,
		Command:     stdio.Command{Path: "/bin/claude-agent-acp"},
	}, fakeConnect(conn, nil))
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	if mc.owned != nil {
		t.Fatal("ManagedClient.owned != nil for a SharedProxy dial: nothing should be tracked to start or close")
	}
	if got := harness.lastBinding(); got != *shared {
		t.Errorf("harness configured with binding %+v, want the shared binding %+v", got, *shared)
	}

	if err := mc.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if conn.closesSnapshot() != 1 {
		t.Errorf("conn.Close called %d times, want 1", conn.closesSnapshot())
	}
}

// --- ManagedClient.Close closes ACP before the owned proxy, joining errors ---

func TestManagedClientCloseOrdersConnBeforeProxyAndJoinsErrors(t *testing.T) {
	conn := newFakeConn()
	connErr := errors.New("conn close boom")
	conn.closeErr = connErr

	proxyErr := errors.New("proxy close boom")
	proxyClosedAfterConn := false
	proxy := &fakeProxy{closeErr: proxyErr}
	proxy.preClose = func() {
		proxyClosedAfterConn = conn.closesSnapshot() >= 1
	}

	mc := newManagedClientForTest(conn, proxy)
	err := mc.Close(context.Background())

	if !errors.Is(err, connErr) {
		t.Errorf("Close() error = %v, want it to wrap conn's error %v", err, connErr)
	}
	if !errors.Is(err, proxyErr) {
		t.Errorf("Close() error = %v, want it to wrap proxy's error %v", err, proxyErr)
	}
	if !proxyClosedAfterConn {
		t.Error("owned proxy Close observed before conn.Close had run, want conn closed first")
	}
}

func TestManagedClientCloseIsIdempotent(t *testing.T) {
	conn := newFakeConn()
	proxy := &fakeProxy{}
	mc := newManagedClientForTest(conn, proxy)

	if err := mc.Close(context.Background()); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := mc.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	if conn.closesSnapshot() != 1 {
		t.Errorf("conn.Close called %d times, want 1", conn.closesSnapshot())
	}
	if _, closes := proxy.counts(); closes != 1 {
		t.Errorf("proxy.Close called %d times, want 1", closes)
	}
}

// --- child death observed through Done closes an owned proxy ---

func TestWatchOwnedDeathClosesOwnedProxyOnUnexpectedChildDeath(t *testing.T) {
	conn := newFakeConn()
	proxy := &fakeProxy{}
	mc := newManagedClientForTest(conn, proxy)

	go mc.watchOwnedDeath()
	conn.kill()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, closes := proxy.counts(); closes == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for watchOwnedDeath to close the owned proxy after conn.Done() fired")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if conn.closesSnapshot() != 0 {
		t.Errorf("conn.Close called %d times, want 0: watchOwnedDeath must not itself call Close on a connection that already died on its own", conn.closesSnapshot())
	}
}

func TestWatchOwnedDeathAndExplicitCloseRaceSafely(t *testing.T) {
	conn := newFakeConn()
	proxy := &fakeProxy{}
	mc := newManagedClientForTest(conn, proxy)

	go mc.watchOwnedDeath()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = mc.Close(context.Background())
	}()
	go func() {
		defer wg.Done()
		conn.kill()
	}()
	wg.Wait()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, closes := proxy.counts(); closes == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("proxy.Close settled at an unexpected count: %#v", proxy)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// --- multiple owners remain independent ---

func TestMultipleOwnersRemainIndependent(t *testing.T) {
	proxyA := readyProxy()
	proxyB := readyProxy()
	harness := &fakeHarness{}

	mcA, err := dial(context.Background(), Config{OwnedProxy: proxyA, Harness: harness, Command: stdio.Command{Path: "/bin/a"}}, fakeConnect(newFakeConn(), nil))
	if err != nil {
		t.Fatalf("dial() A error = %v", err)
	}
	mcB, err := dial(context.Background(), Config{OwnedProxy: proxyB, Harness: harness, Command: stdio.Command{Path: "/bin/b"}}, fakeConnect(newFakeConn(), nil))
	if err != nil {
		t.Fatalf("dial() B error = %v", err)
	}

	if err := mcA.Close(context.Background()); err != nil {
		t.Fatalf("Close() A error = %v", err)
	}
	if _, closes := proxyA.counts(); closes != 1 {
		t.Errorf("proxyA closes = %d, want 1", closes)
	}
	if _, closes := proxyB.counts(); closes != 0 {
		t.Errorf("proxyB closes = %d, want 0: closing mcA must never touch mcB's owned proxy", closes)
	}

	if err := mcB.Close(context.Background()); err != nil {
		t.Fatalf("Close() B error = %v", err)
	}
	if _, closes := proxyB.counts(); closes != 1 {
		t.Errorf("proxyB closes = %d, want 1", closes)
	}
}

// --- multiple borrowers remain independent ---

func TestMultipleBorrowersOfSameSharedProxyRemainIndependent(t *testing.T) {
	shared := &ProxyBinding{BaseURL: "http://shared", Token: "shared-tok"}
	harness := &fakeHarness{}
	conn1 := newFakeConn()
	conn2 := newFakeConn()

	mc1, err := dial(context.Background(), Config{SharedProxy: shared, Harness: harness, Command: stdio.Command{Path: "/bin/a"}}, fakeConnect(conn1, nil))
	if err != nil {
		t.Fatalf("dial() 1 error = %v", err)
	}
	if _, err := dial(context.Background(), Config{SharedProxy: shared, Harness: harness, Command: stdio.Command{Path: "/bin/b"}}, fakeConnect(conn2, nil)); err != nil {
		t.Fatalf("dial() 2 error = %v", err)
	}

	if err := mc1.Close(context.Background()); err != nil {
		t.Fatalf("Close() 1 error = %v", err)
	}
	if conn1.closesSnapshot() != 1 {
		t.Errorf("conn1 closes = %d, want 1", conn1.closesSnapshot())
	}
	if conn2.closesSnapshot() != 0 {
		t.Errorf("conn2 closes = %d, want 0: closing one borrower must never touch another's connection", conn2.closesSnapshot())
	}
}
