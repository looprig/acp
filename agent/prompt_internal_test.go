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

// erroringSender is a liveClient whose SessionUpdate always fails, so
// drainToTerminal's send-error abort path (a live update failing to reach
// the wire) can be exercised directly without tearing down a real Conn (see
// prompt.go: "client is a real network write, and a wedged or gone
// connection means continuing to drain silently would misrepresent what the
// client actually received"). RequestPermission is never exercised by this
// test (no event.GateOpened is ever sent), so it is a trivial stub.
type erroringSender struct {
	err   error
	calls int
}

func (s *erroringSender) SessionUpdate(context.Context, protocol.SessionNotification) error {
	s.calls++
	return s.err
}

func (s *erroringSender) RequestPermission(context.Context, protocol.RequestPermissionRequest) (*protocol.RequestPermissionResponse, error) {
	return nil, errors.New("erroringSender: RequestPermission not implemented")
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
		// live is nil: no event.GateOpened is ever sent on sub.ch in this
		// test, so drainToTerminal's gate-handling branch (the only place
		// that would dereference it) is never reached.
		resp, err := drainToTerminal(context.Background(), sub, cmdID, wireSessionID, nil, sender, newGateTracker())
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

// --- promptTracker: begin/end/markClosing/forget, white-box ----------------

// TestPromptTrackerBeginEndBasicSerialization pins the pre-existing
// begin/end contract still holds under the new promptBeginOutcome return
// type: a second begin for the same id is rejected while one is in flight,
// and released once end runs.
func TestPromptTrackerBeginEndBasicSerialization(t *testing.T) {
	tr := newPromptTracker()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	if got := tr.begin(id); got != promptBeginOK {
		t.Fatalf("first begin = %v, want promptBeginOK", got)
	}
	if got := tr.begin(id); got != promptBeginAlreadyInFlight {
		t.Fatalf("second begin while in flight = %v, want promptBeginAlreadyInFlight", got)
	}
	tr.end(id)
	if got := tr.begin(id); got != promptBeginOK {
		t.Fatalf("begin after end = %v, want promptBeginOK (tracker must release)", got)
	}
	tr.end(id)
}

// TestPromptTrackerMarkClosingTakesPrecedenceOverInFlight asserts the
// closing check runs before the in-flight check (see begin's doc): once
// markClosing has run, every future begin call reports promptBeginClosing —
// never promptBeginAlreadyInFlight — even while a prompt this same
// markClosing call observed as in-flight is still running.
func TestPromptTrackerMarkClosingTakesPrecedenceOverInFlight(t *testing.T) {
	tr := newPromptTracker()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	if got := tr.begin(id); got != promptBeginOK {
		t.Fatalf("begin = %v, want promptBeginOK", got)
	}

	done, wasInFlight := tr.markClosing(id)
	if !wasInFlight {
		t.Fatal("markClosing: wasInFlight = false, want true (a prompt is currently in flight)")
	}
	select {
	case <-done:
		t.Fatal("done channel is already closed, want open (the in-flight prompt has not ended yet)")
	default:
	}

	if got := tr.begin(id); got != promptBeginClosing {
		t.Fatalf("begin after markClosing (still in flight) = %v, want promptBeginClosing", got)
	}

	tr.end(id)
	select {
	case <-done:
	default:
		t.Fatal("done channel not closed after end, want closed")
	}

	// Still permanently closing, even though nothing is in flight anymore.
	if got := tr.begin(id); got != promptBeginClosing {
		t.Fatalf("begin after end (still closing) = %v, want promptBeginClosing", got)
	}
}

// TestPromptTrackerMarkClosingWithoutInFlightPrompt asserts markClosing
// correctly reports wasInFlight=false and a nil done channel when nothing is
// running for id at all.
func TestPromptTrackerMarkClosingWithoutInFlightPrompt(t *testing.T) {
	tr := newPromptTracker()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	done, wasInFlight := tr.markClosing(id)
	if wasInFlight {
		t.Fatal("markClosing: wasInFlight = true, want false (nothing was ever begun for this id)")
	}
	if done != nil {
		t.Fatalf("markClosing: done = %v, want nil", done)
	}
	if got := tr.begin(id); got != promptBeginClosing {
		t.Fatalf("begin after markClosing = %v, want promptBeginClosing", got)
	}
}

// TestPromptTrackerForgetClearsClosingAndInFlight asserts forget (called by
// close.go only after the session has already left the live registry) fully
// clears both maps, so a stale id never grows promptTracker's memory for the
// rest of the process's lifetime.
func TestPromptTrackerForgetClearsClosingAndInFlight(t *testing.T) {
	tr := newPromptTracker()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	if got := tr.begin(id); got != promptBeginOK {
		t.Fatalf("begin = %v, want promptBeginOK", got)
	}
	tr.markClosing(id)

	if _, ok := tr.closing[id]; !ok {
		t.Fatal("closing[id] not recorded before forget")
	}
	if _, ok := tr.inFlight[id]; !ok {
		t.Fatal("inFlight[id] not recorded before forget")
	}

	tr.forget(id)

	if _, ok := tr.closing[id]; ok {
		t.Error("closing[id] still present after forget")
	}
	if _, ok := tr.inFlight[id]; ok {
		t.Error("inFlight[id] still present after forget")
	}

	// forget must not have somehow left id in a stuck "closing" state that
	// survives past its own removal: a session id is never reused, so this
	// is mostly documenting intent, but a fresh begin should be OK were the
	// same id ever (implausibly) reused.
	if got := tr.begin(id); got != promptBeginOK {
		t.Fatalf("begin after forget = %v, want promptBeginOK", got)
	}
}
