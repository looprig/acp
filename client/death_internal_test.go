package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
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
