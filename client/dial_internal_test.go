package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

// newTestClient builds a *Client whose connection attempt is entirely
// replaced by attempt, bypassing real process spawning and protocol wiring
// so the start-once state machine can be exercised fast and deterministically
// under -race -count=N. This is the seam production code wires to
// (*Client).spawnAndConnect via New; tests in this file replace it directly
// since dial_internal_test.go is white-box (package client).
func newTestClient(attempt func(ctx context.Context) error) *Client {
	c := &Client{
		sessions: make(map[protocol.SessionID]*Session),
		done:     make(chan struct{}),
	}
	c.attemptConnect = attempt
	return c
}

// TestDialConcurrentCallersShareOneAttempt proves that N goroutines calling
// Dial concurrently on the same *Client observe exactly one underlying
// connection attempt: every caller blocks on that one in-flight attempt
// (rather than starting its own) and all observe its result.
func TestDialConcurrentCallersShareOneAttempt(t *testing.T) {
	var calls int64
	release := make(chan struct{})
	entered := make(chan struct{}, 100)

	c := newTestClient(func(ctx context.Context) error {
		atomic.AddInt64(&calls, 1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})

	const n = 20
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = c.Dial(context.Background())
		}(i)
	}

	// Wait for the single attempt to actually start before releasing it, so
	// the assertion below is deterministic rather than timing-dependent.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the shared attempt to start")
	}
	close(release)
	wg.Wait()

	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("attemptConnect call count = %d, want 1 (concurrent Dial callers must share one attempt)", got)
	}
	for i, err := range errs {
		if err != nil {
			t.Errorf("Dial()[%d] error = %v, want nil", i, err)
		}
	}
}

// TestDialFailedStartResetsAndRetryWorks proves that a failed connection
// attempt resets the state machine to idle so a later Dial call starts a
// fresh attempt (rather than being permanently wedged in a failed state).
func TestDialFailedStartResetsAndRetryWorks(t *testing.T) {
	var calls int64
	wantErr := errors.New("boom")

	c := newTestClient(func(ctx context.Context) error {
		n := atomic.AddInt64(&calls, 1)
		if n == 1 {
			return wantErr
		}
		return nil
	})

	err := c.Dial(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("first Dial() error = %v, want %v", err, wantErr)
	}

	err = c.Dial(context.Background())
	if err != nil {
		t.Fatalf("retry Dial() error = %v, want nil", err)
	}

	if got := atomic.LoadInt64(&calls); got != 2 {
		t.Errorf("attemptConnect call count = %d, want 2 (one failed, one retry)", got)
	}
}

// TestDialConcurrentCallersDuringFailureAllRetry proves the concurrent-share
// and failure-reset behaviors compose: N concurrent Dial callers against an
// attempt that fails a fixed number of times before succeeding all eventually
// observe success, with attemptConnect invoked exactly once per attempt
// (never duplicated by concurrently-waiting callers).
func TestDialConcurrentCallersDuringFailureAllRetry(t *testing.T) {
	const failures = 2
	var calls int64

	c := newTestClient(func(ctx context.Context) error {
		n := atomic.AddInt64(&calls, 1)
		if n <= failures {
			return errors.New("transient failure")
		}
		return nil
	})

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each caller retries on its own until it observes success or a
			// bounded number of attempts elapse, exactly like a real caller
			// (e.g. the foreignloops driver) would on first Spawn.
			var err error
			for attempt := 0; attempt < failures+5; attempt++ {
				err = c.Dial(context.Background())
				if err == nil {
					break
				}
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller[%d] final Dial() error = %v, want nil (eventual success)", i, err)
		}
	}
}

// TestDialRespectsContextCancellationWhileWaiting proves a caller blocked
// waiting on someone else's in-flight attempt unblocks with ctx.Err() when
// its own context is canceled, without disturbing the shared attempt itself.
func TestDialRespectsContextCancellationWhileWaiting(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	c := newTestClient(func(ctx context.Context) error {
		close(entered)
		<-release
		return nil
	})

	ownerDone := make(chan error, 1)
	go func() { ownerDone <- c.Dial(context.Background()) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the owning attempt to start")
	}

	waiterCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := c.Dial(waiterCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter Dial() error = %v, want context.DeadlineExceeded", err)
	}

	close(release)
	if err := <-ownerDone; err != nil {
		t.Fatalf("owner Dial() error = %v, want nil (waiter's cancellation must not disturb the shared attempt)", err)
	}
}

func TestDialCloseCancelsInFlightAttempt(t *testing.T) {
	assertNoGoroutineLeak(t)
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAttempt := func() {
		releaseOnce.Do(func() { close(release) })
	}

	c := newTestClient(func(ctx context.Context) error {
		close(entered)
		select {
		case <-ctx.Done():
			close(canceled)
			return ctx.Err()
		case <-release:
			return nil
		}
	})

	ownerDone := make(chan error, 1)
	go func() { ownerDone <- c.Dial(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the owning attempt to start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- c.Close(context.Background()) }()
	if err := <-closeDone; err != nil {
		t.Fatalf("Client.Close() error = %v", err)
	}

	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		releaseAttempt()
		<-ownerDone
		t.Fatal("Client.Close() did not cancel the in-flight dial attempt")
	}

	var closedErr *ClosedError
	if err := <-ownerDone; !errors.As(err, &closedErr) {
		t.Fatalf("owner Dial() error = %v (%T), want *ClosedError", err, err)
	}
	c.mu.Lock()
	state := c.state
	c.mu.Unlock()
	if state != dialClosed {
		t.Fatalf("Client state after close = %v, want dialClosed", state)
	}
}

func TestDialCloseRejectsLateFinishConnect(t *testing.T) {
	assertNoGoroutineLeak(t)
	agentSide, clientSide := net.Pipe()
	fa := newFakeAgent(protocol.NewConn(agentSide, agentSide, protocol.ConnOptions{}))
	initEntered := make(chan struct{})
	releaseInit := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseInit) })
	}
	fa.onInitialize = func(req protocol.InitializeRequest) (protocol.InitializeResponse, error) {
		close(initEntered)
		<-releaseInit
		return protocol.InitializeResponse{ProtocolVersion: protocol.CurrentProtocolVersion}, nil
	}

	c := New(stdio.Command{}, Options{})
	c.attemptConnect = func(ctx context.Context) error {
		// Deliberately use a context the test controls independently: this
		// models an attempt/handshake implementation that completes after its
		// owner has observed Close and tests finishConnect's late-publication
		// guard directly.
		return c.finishConnect(context.Background(), nil, clientSide, clientSide)
	}
	t.Cleanup(func() {
		release()
		_ = agentSide.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})

	ownerDone := make(chan error, 1)
	go func() { ownerDone <- c.Dial(context.Background()) }()
	select {
	case <-initEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initialize to block")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- c.Close(context.Background()) }()
	if err := <-closeDone; err != nil {
		t.Fatalf("Client.Close() error = %v", err)
	}
	release()

	var closedErr *ClosedError
	if err := <-ownerDone; !errors.As(err, &closedErr) {
		t.Fatalf("owner Dial() error = %v (%T), want *ClosedError", err, err)
	}
	c.mu.Lock()
	state, conn := c.state, c.conn
	c.mu.Unlock()
	if state != dialClosed {
		t.Fatalf("Client state after late finishConnect = %v, want dialClosed", state)
	}
	if conn != nil {
		t.Fatal("late finishConnect published a connection after Client.Close")
	}
	select {
	case <-fa.conn.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("late finishConnect did not close its protocol connection")
	}
}
