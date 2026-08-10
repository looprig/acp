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

func TestConnResponsePublicationLinearizesBeforeCancellation(t *testing.T) {
	const attempts = 10000

	for i := 0; i < attempts; i++ {
		conn := &Conn{pending: make(map[ID]chan callResult)}
		id := NewNumberID(int64(i + 1))
		pending := make(chan callResult, 1)
		conn.pending[id] = pending
		response := &Response{
			ID:              id,
			Result:          json.RawMessage(`{"accepted":true}`),
			ReceiveSequence: uint64(i + 1),
		}
		published := make(chan struct{})
		go func() {
			conn.correlateResponse(response)
			close(published)
		}()

		// Observing removal is the cancellation side's linearization point. A
		// response that has acknowledged ownership must already be available
		// to the cancellation cleanup; it may not be lost in the gap between
		// removing the map entry and publishing to its channel.
		for {
			conn.mu.Lock()
			_, stillPending := conn.pending[id]
			conn.mu.Unlock()
			if !stillPending {
				break
			}
			runtime.Gosched()
		}

		var payload struct {
			Accepted bool `json:"accepted"`
		}
		facts, err := conn.abandonPendingCall(id, pending, &payload, CallResult{}, context.Canceled)
		<-published
		if err != nil {
			t.Fatalf("attempt %d: abandonPendingCall() error = %v, want acknowledged response", i, err)
		}
		if facts.ResponseSequence != uint64(i+1) || facts.ReceiveSequence != uint64(i+1) {
			t.Fatalf("attempt %d: response facts = %#v, want sequence %d", i, facts, i+1)
		}
		if !facts.WriteAdmitted {
			t.Fatalf("attempt %d: acknowledged response facts = %#v, want WriteAdmitted=true", i, facts)
		}
		if !payload.Accepted {
			t.Fatalf("attempt %d: acknowledged response payload = %#v, want accepted=true", i, payload)
		}
	}
}

func TestConnCallWithResultCarriesAdmissionAndResponseSequence(t *testing.T) {
	assertNoGoroutineLeakInternal(t)
	c1, c2 := net.Pipe()
	client := NewConn(c1, c1, ConnOptions{})
	server := NewConn(c2, c2, ConnOptions{})
	t.Cleanup(func() { client.Close(); server.Close() })
	server.Handle("ordered", func(context.Context, string, json.RawMessage) (any, error) {
		return map[string]string{"ok": "yes"}, nil
	})

	var result map[string]string
	facts, err := client.CallWithResult(context.Background(), "ordered", nil, &result)
	if err != nil {
		t.Fatalf("CallWithResult() error = %v", err)
	}
	if !facts.WriteAdmitted {
		t.Fatal("CallWithResult() reported false write admission after successful request")
	}
	if facts.ResponseSequence == 0 {
		t.Fatal("CallWithResult() response sequence = 0, want stamped inbound response")
	}
	if result["ok"] != "yes" {
		t.Fatalf("CallWithResult() result = %#v, want ok=yes", result)
	}
}

func TestConnStartCallSignalsAdmissionBeforeBlockedWrite(t *testing.T) {
	reader, readerPeer := io.Pipe()
	sink := newAdmissionGateWriter()
	c := NewConn(reader, sink, ConnOptions{})
	defer func() {
		sink.releaseWrite()
		_ = readerPeer.Close()
		_ = c.Close()
	}()

	h, err := c.StartCall(context.Background(), "blocked.write", nil, nil)
	if err != nil {
		t.Fatalf("StartCall() error = %v", err)
	}
	if cap(h.Admission()) != 1 || cap(h.Result()) != 1 {
		t.Fatalf("async channels have capacities admission=%d result=%d, want one each", cap(h.Admission()), cap(h.Result()))
	}
	select {
	case admitted, ok := <-h.Admission():
		if !ok || !admitted {
			t.Fatalf("admission = (%v, %v), want one true value", admitted, ok)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queue admission")
	}
	select {
	case completion := <-h.Result():
		t.Fatalf("result arrived while raw write/response blocked: %#v", completion)
	case <-time.After(50 * time.Millisecond):
	}
	h.Cancel()
	completion, ok := <-h.Result()
	if !ok {
		t.Fatal("Result() closed before cancellation completion")
	}
	if !completion.Facts.WriteAdmitted {
		t.Fatal("post-admission cancellation lost WriteAdmitted=true")
	}
	if !errors.Is(completion.Err, context.Canceled) {
		t.Fatalf("completion error = %v, want context.Canceled", completion.Err)
	}
}

func TestConnCallWithResultCarriesAdmissionOnPeerErrorAndEOF(t *testing.T) {
	assertNoGoroutineLeakInternal(t)

	t.Run("peer error", func(t *testing.T) {
		c1, c2 := net.Pipe()
		client := NewConn(c1, c1, ConnOptions{})
		server := NewConn(c2, c2, ConnOptions{})
		t.Cleanup(func() { client.Close(); server.Close() })
		server.Handle("error", func(context.Context, string, json.RawMessage) (any, error) {
			return nil, InvalidParams("bad steering", nil)
		})

		facts, err := client.CallWithResult(context.Background(), "error", nil, nil)
		if err == nil {
			t.Fatal("CallWithResult() error = nil, want peer error")
		}
		if !facts.WriteAdmitted || facts.ResponseSequence == 0 {
			t.Fatalf("CallWithResult() facts = %#v, want admitted response sequence", facts)
		}
	})

	t.Run("peer EOF", func(t *testing.T) {
		c1, c2 := net.Pipe()
		client := NewConn(c1, c1, ConnOptions{})
		server := NewConn(c2, c2, ConnOptions{})
		reached := make(chan struct{})
		release := make(chan struct{})
		server.Handle("eof", func(context.Context, string, json.RawMessage) (any, error) {
			close(reached)
			<-release
			return nil, nil
		})
		t.Cleanup(func() { client.Close(); server.Close() })

		done := make(chan struct{})
		var facts CallResult
		var err error
		go func() {
			facts, err = client.CallWithResult(context.Background(), "eof", nil, nil)
			close(done)
		}()
		<-reached
		_ = server.Close()
		close(release)
		<-done
		if err == nil {
			t.Fatal("CallWithResult() error = nil, want peer EOF/close")
		}
		if !facts.WriteAdmitted {
			t.Fatalf("CallWithResult() facts = %#v, want admitted request after peer close", facts)
		}
	})
}

func TestConnWaitForNotificationsThroughResponseSequence(t *testing.T) {
	assertNoGoroutineLeakInternal(t)
	c1, c2 := net.Pipe()
	client := NewConn(c1, c1, ConnOptions{})
	server := NewConn(c2, c2, ConnOptions{})
	t.Cleanup(func() { client.Close(); server.Close() })

	notifyDone := make(chan struct{})
	client.HandleNotifyWithSequence("ordered.notify", func(context.Context, string, json.RawMessage, uint64) {
		close(notifyDone)
	})
	server.Handle("ordered.call", func(context.Context, string, json.RawMessage) (any, error) {
		if err := server.Notify(context.Background(), "ordered.notify", map[string]string{"v": "1"}); err != nil {
			return nil, err
		}
		return "done", nil
	})

	facts, err := client.CallWithResult(context.Background(), "ordered.call", nil, nil)
	if err != nil {
		t.Fatalf("CallWithResult() error = %v", err)
	}
	if facts.ResponseSequence == 0 {
		t.Fatal("CallWithResult() response sequence = 0")
	}
	if err := client.WaitForNotificationsThrough(context.Background(), facts.ResponseSequence); err != nil {
		t.Fatalf("WaitForNotificationsThrough() error = %v", err)
	}
	select {
	case <-notifyDone:
	default:
		t.Fatal("notification barrier returned before registered handler completed")
	}
}

func TestConnWaitForNotificationsThroughDoesNotWaitForLaterNotification(t *testing.T) {
	assertNoGoroutineLeakInternal(t)
	c1, c2 := net.Pipe()
	client := NewConn(c1, c1, ConnOptions{})
	server := NewConn(c2, c2, ConnOptions{})
	t.Cleanup(func() { client.Close(); server.Close() })

	firstDone := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	client.HandleNotifyWithSequence("ordered.first", func(context.Context, string, json.RawMessage, uint64) {
		close(firstDone)
	})
	client.HandleNotifyWithSequence("ordered.second", func(context.Context, string, json.RawMessage, uint64) {
		close(secondStarted)
		<-releaseSecond
	})
	server.Handle("ordered.call", func(context.Context, string, json.RawMessage) (any, error) {
		if err := server.Notify(context.Background(), "ordered.first", nil); err != nil {
			return nil, err
		}
		return "done", nil
	})

	facts, err := client.CallWithResult(context.Background(), "ordered.call", nil, nil)
	if err != nil {
		t.Fatalf("CallWithResult() error = %v", err)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- server.Notify(context.Background(), "ordered.second", nil) }()
	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for later notification handler to start")
	}
	if err := client.WaitForNotificationsThrough(context.Background(), facts.ResponseSequence); err != nil {
		t.Fatalf("WaitForNotificationsThrough() error = %v", err)
	}
	select {
	case <-firstDone:
	default:
		t.Fatal("response-sequence barrier returned before the earlier notification completed")
	}
	close(releaseSecond)
	if err := <-secondDone; err != nil {
		t.Fatalf("later notification send error = %v", err)
	}
}

type dispatchReleaseContext struct {
	release func()
	once    sync.Once
}

func (c *dispatchReleaseContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *dispatchReleaseContext) Done() <-chan struct{} {
	c.once.Do(c.release)
	return nil
}

func (c *dispatchReleaseContext) Err() error { return nil }

func (c *dispatchReleaseContext) Value(any) any { return nil }

func TestConnReceiveBarrierWaitsForNotificationDispatchRegistration(t *testing.T) {
	assertNoGoroutineLeakInternal(t)
	clientReader, clientWriter := io.Pipe()
	client := NewConn(clientReader, clientWriter, ConnOptions{})
	t.Cleanup(func() { client.Close() })

	sequence := client.beginReceiveNotification()
	handlerDone := make(chan struct{})
	ctx := &dispatchReleaseContext{release: func() {
		client.enqueueTrackedNotifyJob(sequence, func() { close(handlerDone) })
		client.finishReceiveNotification(sequence)
	}}

	if err := client.WaitForNotificationsThrough(ctx, sequence); err != nil {
		t.Fatalf("WaitForNotificationsThrough() error = %v", err)
	}
	select {
	case <-handlerDone:
	default:
		t.Fatal("receive barrier returned before notification handler completion")
	}
}

func TestConnWaitForNotificationsThroughChecksContextAfterOrderingBarrier(t *testing.T) {
	client := &Conn{
		done:                   make(chan struct{}),
		receiveChanged:         make(chan struct{}),
		receiveDispatchPending: make(map[uint64]struct{}),
		notifyPending:          make(map[uint64]struct{}),
		notifyChanged:          make(chan struct{}),
		receiveThrough:         1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client.notifyOrderMu.Lock()
	waitDone := make(chan error, 1)
	go func() { waitDone <- client.WaitForNotificationsThrough(ctx, 1) }()
	// Give WaitForNotificationsThrough time to reach its ordering barrier.
	time.Sleep(10 * time.Millisecond)
	cancel()
	client.notifyOrderMu.Unlock()

	if err := <-waitDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForNotificationsThrough() error = %v, want context.Canceled", err)
	}
}

func TestConnReceiveSequenceOverflowFailsClosedWithoutZeroObservation(t *testing.T) {
	for _, test := range []struct {
		name string
		mint func(*Conn) uint64
	}{
		{name: "response", mint: func(client *Conn) uint64 { return client.stampReceive() }},
		{name: "notification", mint: func(client *Conn) uint64 { return client.beginReceiveNotification() }},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertNoGoroutineLeakInternal(t)
			clientReader, clientWriter := io.Pipe()
			client := NewConn(clientReader, clientWriter, ConnOptions{})
			t.Cleanup(func() { client.Close() })

			client.receiveMu.Lock()
			client.receiveThrough = ^uint64(0)
			client.receiveMu.Unlock()

			if sequence := test.mint(client); sequence != 0 {
				t.Fatalf("receive sequence overflow minted %d, want no sequence", sequence)
			}
			select {
			case <-client.Done():
			default:
				t.Fatal("receive sequence overflow did not fail closed")
			}
		})
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
