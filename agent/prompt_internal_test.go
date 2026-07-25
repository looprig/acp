package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"
)

// internalFakeSubscription is a minimal event.Subscription for drainToTerminal
// white-box tests that do not need the full wire/registry machinery
// prompt_test.go's (package agent_test) fakes provide.
type internalFakeSubscription struct {
	ch chan event.Delivery
}

func (s *internalFakeSubscription) Events() <-chan event.Delivery { return s.ch }
func (s *internalFakeSubscription) Close() error                  { return nil }
func (s *internalFakeSubscription) Err() error                    { return nil }

// erroringSender is a liveUpdateSender that always fails, so
// drainToTerminal's send-error abort path (a live update failing to reach
// the wire) can be exercised directly without tearing down a real Conn (see
// prompt.go: "sender is a real network write, and a wedged or gone
// connection means continuing to drain silently would misrepresent what the
// client actually received").
type erroringSender struct {
	err   error
	calls int
}

func (s *erroringSender) SessionUpdate(context.Context, protocol.SessionNotification) error {
	s.calls++
	return s.err
}

func TestDrainToTerminalAbortsWhenLiveUpdateSendFails(t *testing.T) {
	sessionID, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	loopID, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	turnID, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	cmdID, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	hdr := event.Header{Coordinates: identity.Coordinates{SessionID: sessionID, LoopID: loopID, TurnID: turnID}, Cause: identity.Cause{CommandID: cmdID}}

	sub := &internalFakeSubscription{ch: make(chan event.Delivery, 4)}
	sender := &erroringSender{err: errors.New("wire is gone")}

	sub.ch <- event.Delivery{Event: event.TurnStarted{Header: hdr}}
	sub.ch <- event.Delivery{Event: event.TokenDelta{Header: hdr, Chunk: &content.TextChunk{Text: "hi"}}}
	// The terminal is never sent: the live-update failure above must abort
	// the drain before it would ever be needed.

	wireSessionID := protocol.SessionID(sessionID.String())

	type result struct {
		resp protocol.PromptResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := drainToTerminal(context.Background(), sub, cmdID, wireSessionID, sender)
		done <- result{resp, err}
	}()

	select {
	case r := <-done:
		if r.err == nil {
			t.Fatal("drainToTerminal: error = nil, want a typed failure when the live-update send fails")
		}
		var f *protocol.Fault
		if !errors.As(r.err, &f) {
			t.Fatalf("drainToTerminal error = %v (%T), want *protocol.Fault", r.err, r.err)
		}
		if r.resp.StopReason != "" || len(r.resp.Meta) != 0 {
			t.Errorf("resp = %+v, want zero value on failure", r.resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for drainToTerminal to abort after a failed live-update send")
	}

	if sender.calls != 1 {
		t.Errorf("sender.SessionUpdate calls = %d, want exactly 1", sender.calls)
	}
}
