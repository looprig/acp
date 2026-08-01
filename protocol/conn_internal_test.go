package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

type blockedWriteTransport struct {
	writeStarted chan struct{}
	closed       chan struct{}
	closeOnce    sync.Once
	writeOnce    sync.Once
}

func newBlockedWriteTransport() *blockedWriteTransport {
	return &blockedWriteTransport{
		writeStarted: make(chan struct{}),
		closed:       make(chan struct{}),
	}
}

func (t *blockedWriteTransport) Read([]byte) (int, error) {
	<-t.closed
	return 0, io.EOF
}

func (t *blockedWriteTransport) Write([]byte) (int, error) {
	t.writeOnce.Do(func() { close(t.writeStarted) })
	<-t.closed
	return 0, io.ErrClosedPipe
}

func (t *blockedWriteTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

// assertNoGoroutineLeakInternal mirrors conn_test.go's assertNoGoroutineLeak.
// Duplicated (rather than shared) because this file lives in package
// protocol (white-box) while conn_test.go lives in package protocol_test.
func assertNoGoroutineLeakInternal(t *testing.T) {
	t.Helper()
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

// --- pending-request table ---

func TestConnPendingTableTracksInFlightCalls(t *testing.T) {
	assertNoGoroutineLeakInternal(t)

	c1, c2 := net.Pipe()
	client := NewConn(c1, c1, ConnOptions{})
	server := NewConn(c2, c2, ConnOptions{})
	t.Cleanup(func() { client.Close(); server.Close() })

	reached := make(chan struct{})
	release := make(chan struct{})
	server.Handle("slow", func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		close(reached)
		<-release
		return "done", nil
	})

	if got := client.pendingLen(); got != 0 {
		t.Fatalf("pendingLen() before Call = %d, want 0", got)
	}

	done := make(chan error, 1)
	go func() {
		var result string
		done <- client.Call(context.Background(), "slow", nil, &result)
	}()

	<-reached
	// The handler is now blocked server-side, which only happens after the
	// client's Call has sent its request and registered the pending entry.
	deadline := time.Now().Add(time.Second)
	for client.pendingLen() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := client.pendingLen(); got != 1 {
		t.Fatalf("pendingLen() while in-flight = %d, want 1", got)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if got := client.pendingLen(); got != 0 {
		t.Fatalf("pendingLen() after completion = %d, want 0", got)
	}
}

func TestConnCallContextCancelRemovesPendingEntry(t *testing.T) {
	assertNoGoroutineLeakInternal(t)

	c1, c2 := net.Pipe()
	client := NewConn(c1, c1, ConnOptions{})
	server := NewConn(c2, c2, ConnOptions{})
	t.Cleanup(func() { client.Close(); server.Close() })

	reached := make(chan struct{})
	release := make(chan struct{})
	server.Handle("slow", func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		close(reached)
		<-release
		return "done", nil
	})
	t.Cleanup(func() { close(release) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var result string
		done <- client.Call(ctx, "slow", nil, &result)
	}()

	<-reached
	deadline := time.Now().Add(time.Second)
	for client.pendingLen() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := client.pendingLen(); got != 1 {
		t.Fatalf("pendingLen() before cancel = %d, want 1", got)
	}

	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Call() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Call() never returned after ctx cancel")
	}

	deadline = time.Now().Add(time.Second)
	for client.pendingLen() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := client.pendingLen(); got != 0 {
		t.Fatalf("pendingLen() after cancel = %d, want 0 (leaked pending entry)", got)
	}
}

func TestConnWaitForNotificationsWaitsForQueuedJobs(t *testing.T) {
	assertNoGoroutineLeakInternal(t)
	c1, c2 := net.Pipe()
	client := NewConn(c1, c1, ConnOptions{})
	server := NewConn(c2, c2, ConnOptions{})
	t.Cleanup(func() { client.Close(); server.Close() })

	barrier, ok := any(client).(interface {
		WaitForNotifications(context.Context) error
	})
	if !ok {
		t.Fatal("Conn is missing additive WaitForNotifications(context.Context) error API")
	}

	started := make(chan struct{})
	release := make(chan struct{})
	secondDone := make(chan struct{})
	client.enqueueNotifyJob(func() {
		close(started)
		<-release
	})
	client.enqueueNotifyJob(func() { close(secondDone) })

	barrierDone := make(chan error, 1)
	go func() { barrierDone <- barrier.WaitForNotifications(context.Background()) }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first notification job")
	}
	select {
	case err := <-barrierDone:
		t.Fatalf("notification barrier returned before queued jobs drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-barrierDone; err != nil {
		t.Fatalf("WaitForNotifications() error = %v", err)
	}
	select {
	case <-secondDone:
	default:
		t.Fatal("notification barrier returned before the second queued job completed")
	}
}

func TestConnWaitForNotificationsIsCancellationAware(t *testing.T) {
	assertNoGoroutineLeakInternal(t)
	c1, c2 := net.Pipe()
	client := NewConn(c1, c1, ConnOptions{})
	server := NewConn(c2, c2, ConnOptions{})
	t.Cleanup(func() { client.Close(); server.Close() })

	barrier, ok := any(client).(interface {
		WaitForNotifications(context.Context) error
	})
	if !ok {
		t.Fatal("Conn is missing additive WaitForNotifications(context.Context) error API")
	}

	release := make(chan struct{})
	client.enqueueNotifyJob(func() { <-release })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := barrier.WaitForNotifications(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForNotifications() error = %v, want context.DeadlineExceeded", err)
	}
	close(release)
}

func TestConnCloseInterruptsBlockedWriterBeforeWaiting(t *testing.T) {
	assertNoGoroutineLeakInternal(t)
	transport := newBlockedWriteTransport()
	conn := NewConn(transport, transport, ConnOptions{})
	t.Cleanup(func() {
		_ = transport.Close()
		_ = conn.Close()
	})

	notifyDone := make(chan error, 1)
	go func() { notifyDone <- conn.Notify(context.Background(), "blocked", nil) }()
	select {
	case <-transport.writeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Notify to enter the blocked write")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- conn.Close() }()
	select {
	case <-transport.closed:
	case <-time.After(2 * time.Second):
		// Release the test transport so the pre-fix implementation cannot
		// strand the test process in its writer wait after this assertion.
		_ = transport.Close()
		<-closeDone
		t.Fatal("Conn.Close() did not interrupt the raw transport before waiting for the blocked writer")
	}
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Conn.Close() remained blocked after interrupting the raw transport")
	}
	select {
	case <-notifyDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Notify() remained blocked after Conn.Close()")
	}
}

func TestConnNotifyReturnsOnContextCancellationDuringWrite(t *testing.T) {
	assertNoGoroutineLeakInternal(t)
	transport := newBlockedWriteTransport()
	conn := NewConn(transport, transport, ConnOptions{})
	t.Cleanup(func() {
		_ = transport.Close()
		_ = conn.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	notifyDone := make(chan error, 1)
	go func() { notifyDone <- conn.Notify(ctx, "blocked", nil) }()
	select {
	case <-transport.writeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Notify to enter the blocked write")
	}

	cancel()
	select {
	case err := <-notifyDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Notify() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		// Release the pre-fix writer so the test process cannot be left
		// behind after recording the expected cancellation failure.
		_ = transport.Close()
		<-notifyDone
		t.Fatal("Notify() remained blocked after context cancellation")
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Conn.Close() error = %v", err)
	}
}

// --- disjoint id spaces ---

func TestConnDisjointIDSpacesNoCollisionUnderConcurrency(t *testing.T) {
	assertNoGoroutineLeakInternal(t)

	c1, c2 := net.Pipe()
	client := NewConn(c1, c1, ConnOptions{})
	server := NewConn(c2, c2, ConnOptions{})
	t.Cleanup(func() { client.Close(); server.Close() })

	server.Handle("callSpace", func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		return string(params), nil
	})
	server.Handle("extSpace", func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		return string(params), nil
	})

	const n = 200
	var wg sync.WaitGroup
	callErrs := make([]error, n)
	extErrs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var result string
			callErrs[i] = client.Call(context.Background(), "callSpace", i, &result)
			if result != jsonNum(i) {
				t.Errorf("Call[%d] result = %q, want %q (cross-talk between concurrent calls)", i, result, jsonNum(i))
			}
		}(i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var result string
			extErrs[i] = client.callExt(context.Background(), "extSpace", i, &result)
			if result != jsonNum(i) {
				t.Errorf("callExt[%d] result = %q, want %q (cross-talk between concurrent calls)", i, result, jsonNum(i))
			}
		}(i)
	}
	wg.Wait()

	for i, err := range callErrs {
		if err != nil {
			t.Errorf("Call[%d] error = %v", i, err)
		}
	}
	for i, err := range extErrs {
		if err != nil {
			t.Errorf("callExt[%d] error = %v", i, err)
		}
	}

	// The two counters must have advanced independently and without any lost
	// update (a race in the atomic increment would under-count and manifest
	// as a duplicate minted id, which the map-keyed pending table would
	// silently coalesce, producing exactly this kind of counter mismatch).
	wantCallID := (int64(1) << 32) - 1 + int64(n)
	if got := client.nextCallID.Load(); got != wantCallID {
		t.Errorf("nextCallID = %d, want %d", got, wantCallID)
	}
	wantExtID := int64(n)
	if got := client.nextExtID.Load(); got != wantExtID {
		t.Errorf("nextExtID = %d, want %d", got, wantExtID)
	}
	if wantCallID <= wantExtID {
		t.Fatalf("test setup bug: call id space (%d) does not start above ext id space (%d)", wantCallID, wantExtID)
	}
}

func TestConnExtIDBaseOptionSetsStartingValue(t *testing.T) {
	assertNoGoroutineLeakInternal(t)

	c1, c2 := net.Pipe()
	client := NewConn(c1, c1, ConnOptions{ExtIDBase: 1000})
	server := NewConn(c2, c2, ConnOptions{})
	t.Cleanup(func() { client.Close(); server.Close() })

	server.Handle("m", func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		return "ok", nil
	})

	var result string
	if err := client.callExt(context.Background(), "m", nil, &result); err != nil {
		t.Fatalf("callExt() error = %v", err)
	}
	if got := client.nextExtID.Load(); got != 1000 {
		t.Fatalf("nextExtID after first callExt = %d, want 1000 (ExtIDBase)", got)
	}
}

func jsonNum(i int) string {
	raw, _ := json.Marshal(i)
	return string(raw)
}

// pendingLen and callExt are exercised above via direct unexported access,
// confirming Task 1.5's white-box test hooks work as intended.
