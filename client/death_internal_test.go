package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

// TestConnectionDeathFailsPendingPromptAndClosesUpdates proves that when the
// underlying connection dies (here: the fake agent's side of the pipe is
// closed, simulating the peer disappearing), a Prompt call already in
// flight fails with a typed *ClosedError and the session's Updates()
// channel closes rather than hanging forever.
func TestConnectionDeathFailsPendingPromptAndClosesUpdates(t *testing.T) {
	assertNoGoroutineLeak(t)
	c, fa := dialTestClient(t, Options{})

	promptEntered := make(chan struct{})
	release := make(chan struct{})
	// Conn's own contract (see protocol/conn.go's Close doc) is that Close
	// does not wait for an in-flight handler callback to finish, and
	// dispatchRequest always hands handlers context.Background() (never a
	// context tied to the connection's lifetime) — so this handler must be
	// released by an explicit signal, not ctx.Done(), or its goroutine would
	// leak for the rest of the process's life instead of just until release
	// is closed below.
	fa.onPrompt = func(ctx context.Context, fa *fakeAgent, req protocol.PromptRequest) (protocol.PromptResponse, error) {
		close(promptEntered)
		<-release
		return protocol.PromptResponse{}, errors.New("fake agent: torn down mid-prompt")
	}
	defer close(release)
	sess := newSessionForTest(t, c, fa, "sess-death")

	promptDone := make(chan error, 1)
	go func() {
		_, err := sess.Prompt(context.Background(), []protocol.ContentBlock{{Text: &protocol.TextContent{Text: "hi"}}})
		promptDone <- err
	}()

	select {
	case <-promptEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the prompt to reach the fake agent")
	}

	// Simulate the peer (subprocess) disappearing: close its side of the
	// transport, which is exactly what a dead process's closed stdout pipe
	// looks like to protocol.Conn's read loop.
	if err := fa.conn.Close(); err != nil {
		t.Fatalf("fa.conn.Close() error = %v", err)
	}

	select {
	case err := <-promptDone:
		var closedErr *ClosedError
		if !errors.As(err, &closedErr) {
			t.Fatalf("Prompt() error = %v (%T), want *ClosedError", err, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the in-flight Prompt to fail after connection death")
	}

	select {
	case _, open := <-sess.Updates():
		if open {
			t.Fatal("Updates() delivered a value instead of closing after connection death")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Updates() to close after connection death")
	}
}

// TestOperationsAfterConnectionDeathFailTyped proves that once the
// connection has died, subsequent Client operations fail fast with a typed
// *ClosedError rather than hanging or panicking.
func TestOperationsAfterConnectionDeathFailTyped(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	if err := fa.conn.Close(); err != nil {
		t.Fatalf("fa.conn.Close() error = %v", err)
	}

	// watchDeath transitions state asynchronously once conn.Done() fires;
	// poll briefly rather than assuming an arbitrary sleep is long enough.
	deadline := time.Now().Add(2 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, err = c.NewSession(ctx, NewSessionParams{Cwd: "/work"})
		cancel()
		var closedErr *ClosedError
		if errors.As(err, &closedErr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("NewSession() after connection death error = %v, want *ClosedError", err)
}

// assertDoneClosed fails the test unless c.Done() is already closed
// (non-blocking receive), and assertDoneOpen fails it unless c.Done() is
// still open. Both poll briefly rather than assuming a single immediate
// check is deterministic against watchDeath's own goroutine scheduling.
func assertDoneClosed(t *testing.T, c *Client, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		select {
		case <-c.Done():
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("Done() not closed within %s", d)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func assertDoneOpen(t *testing.T, c *Client) {
	t.Helper()
	select {
	case <-c.Done():
		t.Fatal("Done() closed, want still open")
	default:
	}
}

// TestDoneClosesOnExplicitClose proves Done() closes when Close is called
// on an otherwise healthy, successfully-dialed Client.
func TestDoneClosesOnExplicitClose(t *testing.T) {
	c, _ := dialTestClient(t, Options{})
	assertDoneOpen(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	assertDoneClosed(t, c, 2*time.Second)
}

// TestDoneClosesOnConnectionDeath proves Done() closes when the connection
// dies on its own (watchDeath observing conn.Done(), never Close called by
// this test at all) — the "child/connection death" trigger distinct from
// an explicit Close.
func TestDoneClosesOnConnectionDeath(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	assertDoneOpen(t, c)

	if err := fa.conn.Close(); err != nil {
		t.Fatalf("fa.conn.Close() error = %v", err)
	}
	assertDoneClosed(t, c, 2*time.Second)
}

// TestDoneClosesOnCloseAfterNeverSuccessfullyDialing proves two things about
// the "failed terminal transition" trigger: first, that a Dial attempt which
// merely fails (and, per the start-once state machine, resets to idle so a
// later Dial can retry) does NOT by itself close Done — a retryable Client
// is not yet terminally done. Second, that Close called from that same
// never-successfully-connected state (proc and conn both nil: attemptConnect
// never got far enough to set them) still performs a genuine terminal
// transition and closes Done, exactly as it would from a live connection.
func TestDoneClosesOnCloseAfterNeverSuccessfullyDialing(t *testing.T) {
	c := New(stdio.Command{}, Options{})
	c.attemptConnect = func(ctx context.Context) error {
		return errors.New("boom: attemptConnect always fails in this test")
	}

	if err := c.Dial(context.Background()); err == nil {
		t.Fatal("Dial() error = nil, want the injected failure")
	}
	assertDoneOpen(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	assertDoneClosed(t, c, 2*time.Second)
}

// TestDoneClosesExactlyOnceUnderConcurrentCloseAndDeath proves Done()
// survives a genuine race between an explicit Close and watchDeath's own
// connection-death observation without panicking (closing an
// already-closed channel panics, so this is the sync.Once guard's real
// job) and settles closed either way. Run with -race.
func TestDoneClosesExactlyOnceUnderConcurrentCloseAndDeath(t *testing.T) {
	assertNoGoroutineLeak(t)
	c, fa := dialTestClient(t, Options{})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = fa.conn.Close()
	}()
	wg.Wait()

	assertDoneClosed(t, c, 2*time.Second)

	// A second, independent read must also observe it closed (reading a
	// closed channel is always safe/non-blocking); this is really just
	// belt-and-suspenders on top of the -race run itself catching any
	// double-close panic.
	select {
	case <-c.Done():
	default:
		t.Fatal("Done() not closed on a second read after concurrent Close/death")
	}
}
