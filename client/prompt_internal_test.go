package client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
)

func newSessionForTest(t *testing.T, c *Client, fa *fakeAgent, id protocol.SessionID) *Session {
	t.Helper()
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		return protocol.NewSessionResponse{SessionID: id}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := c.NewSession(ctx, NewSessionParams{Cwd: "/work"})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	return sess
}

func TestPromptReturnsStopReason(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	fa.onPrompt = func(ctx context.Context, fa *fakeAgent, req protocol.PromptRequest) (protocol.PromptResponse, error) {
		return protocol.PromptResponse{StopReason: protocol.StopReasonEndTurn}, nil
	}
	sess := newSessionForTest(t, c, fa, "sess-prompt")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := sess.Prompt(ctx, []protocol.ContentBlock{{Text: &protocol.TextContent{Text: "hi"}}})
	if err != nil {
		t.Fatalf("Prompt() error = %v", err)
	}
	if res.StopReason != protocol.StopReasonEndTurn {
		t.Errorf("StopReason = %v, want %v", res.StopReason, protocol.StopReasonEndTurn)
	}
}

// TestPromptSerializesConcurrentCallsOnSameSession proves the one
// prompt-in-flight-per-session semaphore: a second Prompt call issued while
// one is already in flight blocks until the first completes rather than
// racing two session/prompt calls onto the wire concurrently.
func TestPromptSerializesConcurrentCallsOnSameSession(t *testing.T) {
	c, fa := dialTestClient(t, Options{})

	var inFlight int32
	var maxObservedInFlight int32
	release := make(chan struct{})
	fa.onPrompt = func(ctx context.Context, fa *fakeAgent, req protocol.PromptRequest) (protocol.PromptResponse, error) {
		n := atomic.AddInt32(&inFlight, 1)
		for {
			max := atomic.LoadInt32(&maxObservedInFlight)
			if n <= max || atomic.CompareAndSwapInt32(&maxObservedInFlight, max, n) {
				break
			}
		}
		<-release
		atomic.AddInt32(&inFlight, -1)
		return protocol.PromptResponse{StopReason: protocol.StopReasonEndTurn}, nil
	}
	sess := newSessionForTest(t, c, fa, "sess-serialize")

	const n = 5
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := sess.Prompt(ctx, []protocol.ContentBlock{{Text: &protocol.TextContent{Text: "hi"}}})
			done <- err
		}()
	}

	// Give every goroutine a chance to attempt Prompt (and, if the semaphore
	// were broken, to pile into the handler concurrently) before releasing.
	time.Sleep(100 * time.Millisecond)
	close(release)

	for i := 0; i < n; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Prompt()[%d] error = %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a serialized Prompt call to complete")
		}
	}

	if got := atomic.LoadInt32(&maxObservedInFlight); got != 1 {
		t.Errorf("max concurrent session/prompt calls observed = %d, want 1 (semaphore not enforced)", got)
	}
}

// TestCancelResolvesPromptAsCancelledWithoutError proves
// cancellation-as-success: Cancel sends session/cancel, the fake agent
// resolves the in-flight prompt with StopReasonCancelled, and Session.Prompt
// returns that as a normal, non-error *PromptResult.
func TestCancelResolvesPromptAsCancelledWithoutError(t *testing.T) {
	c, fa := dialTestClient(t, Options{})

	promptEntered := make(chan struct{})
	fa.onPrompt = func(ctx context.Context, fa *fakeAgent, req protocol.PromptRequest) (protocol.PromptResponse, error) {
		close(promptEntered)
		n := fa.waitCancel(t, 5*time.Second)
		if n.SessionID != req.SessionID {
			t.Errorf("CancelNotification.SessionID = %q, want %q", n.SessionID, req.SessionID)
		}
		return protocol.PromptResponse{StopReason: protocol.StopReasonCancelled}, nil
	}
	sess := newSessionForTest(t, c, fa, "sess-cancel")

	promptDone := make(chan struct{})
	var res *PromptResult
	var promptErr error
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, promptErr = sess.Prompt(ctx, []protocol.ContentBlock{{Text: &protocol.TextContent{Text: "hi"}}})
		close(promptDone)
	}()

	select {
	case <-promptEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the prompt to reach the fake agent")
	}

	cancelCtx, cancelCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCancel()
	if err := sess.Cancel(cancelCtx); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	select {
	case <-promptDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the cancelled prompt to resolve")
	}
	if promptErr != nil {
		t.Fatalf("Prompt() error = %v, want nil (cancellation resolves as success)", promptErr)
	}
	if res.StopReason != protocol.StopReasonCancelled {
		t.Errorf("StopReason = %v, want %v", res.StopReason, protocol.StopReasonCancelled)
	}
}
