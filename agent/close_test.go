package agent_test

// close_test.go proves session/close's orchestrated teardown state machine
// (Task 2.7 of harness/docs/plans/2026-07-23-acp-bridge-implementation.md)
// end to end, over a real protocol.Conn: marking a session closing rejects
// new prompts, an in-flight prompt is cancelled and genuinely drained (not
// fired-and-forgotten), any outstanding permission request is resolved
// (denied) via gates.go's existing CancelSession integration point, the
// optional SessionCloser.Shutdown capability is invoked with a bounded
// context, and the session is removed from the live registry only once all
// of that has actually finished — never before, proven by observing
// registry membership while Shutdown is deliberately held open.

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	coreuuid "github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
)

// recordingDeleter is a SessionDeleter that records every call it receives.
// close_test.go configures one into Options.Deleter specifically so the
// state-machine test can assert it is NEVER called by session/close: a
// capability that is configured (not merely absent) and proven unused, not
// just never wired up.
type recordingDeleter struct {
	calls int32
}

func (d *recordingDeleter) DeleteSession(context.Context, agent.SessionID) error {
	atomic.AddInt32(&d.calls, 1)
	return nil
}

func (d *recordingDeleter) callCount() int {
	return int(atomic.LoadInt32(&d.calls))
}

// closeOnlyLiveSession is a minimal LiveSession that deliberately does NOT
// implement agent.SessionCloser, so
// TestHandleSessionCloseWithoutShutdownCapability can prove
// handleSessionClose's Shutdown step is genuinely conditional on the
// optional capability rather than assumed present.
type closeOnlyLiveSession struct {
	id coreuuid.UUID

	mu            sync.Mutex
	interruptCall int
}

func (s *closeOnlyLiveSession) SessionID() coreuuid.UUID { return s.id }

func (s *closeOnlyLiveSession) Submit(context.Context, []content.Block) (coreuuid.UUID, error) {
	return coreuuid.UUID{}, errors.New("closeOnlyLiveSession: Submit not implemented")
}

func (s *closeOnlyLiveSession) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errors.New("closeOnlyLiveSession: SubscribeEvents not implemented")
}

func (s *closeOnlyLiveSession) RespondGate(context.Context, gate.GateResponse) error {
	return errors.New("closeOnlyLiveSession: RespondGate not implemented")
}

func (s *closeOnlyLiveSession) Interrupt(context.Context) (bool, error) {
	s.mu.Lock()
	s.interruptCall++
	s.mu.Unlock()
	return true, nil
}

func (s *closeOnlyLiveSession) interruptCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.interruptCall
}

func newCloseOnlyLiveSession(t *testing.T) *closeOnlyLiveSession {
	t.Helper()
	return &closeOnlyLiveSession{id: mustNewUUID(t)}
}

// TestHandleSessionCloseStateMachine is the single scripted-session test
// Task 2.7 calls for: it drives the full close ordering as one coherent
// sequence (not disconnected assertions), asserting at each synchronization
// point that later steps have NOT happened yet, then unblocking the next
// step and asserting it now has.
func TestHandleSessionCloseStateMachine(t *testing.T) {
	fake := newFakeLiveSession(t)
	fake.shutdownBlock = make(chan struct{})
	del := &recordingDeleter{}

	host := &promptHostStub{session: fake}
	a, err := agent.New(agent.Options{Host: host, Deleter: del})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	resp, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sessionID := resp.SessionID

	// The client's session/request_permission handler blocks forever
	// (never answers): the only way the in-flight permission round trip
	// below ever resolves is close's own gates.CancelSession step forcing
	// it closed, never a real client answer arriving late.
	reachedPermission := make(chan struct{}, 1)
	neverAnswered := make(chan protocol.RequestPermissionResponse)
	t.Cleanup(func() { close(neverAnswered) })
	client.Handle(string(protocol.MethodSessionRequestPermission), func(context.Context, string, json.RawMessage) (any, error) {
		select {
		case reachedPermission <- struct{}{}:
		default:
		}
		resp, ok := <-neverAnswered
		if !ok {
			return nil, errors.New("test: client permission handler channel closed without answering")
		}
		return resp, nil
	})

	type promptResult struct {
		resp *protocol.PromptResponse
		err  error
	}
	promptDone := make(chan promptResult, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello"),
		})
		promptDone <- promptResult{resp, err}
	}()

	cmdID := awaitSubmittedCommandID(t, fake)
	sessUUID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	loopID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	turnID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	hdr := turnHeader(sessUUID, loopID, turnID, cmdID)
	send(t, fake.events, event.TurnStarted{Header: hdr})

	opened, gateID := permissionGateOpened(t, hdr)
	send(t, fake.events, opened)

	select {
	case <-reachedPermission:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the client's session/request_permission handler to be reached")
	}

	// --- Step: session/close begins concurrently with the in-flight
	// prompt still parked on the open permission gate. -----------------
	type closeResult struct {
		resp *protocol.CloseSessionResponse
		err  error
	}
	closeDone := make(chan closeResult, 1)
	go func() {
		resp, err := agentConn.CloseSession(context.Background(), protocol.CloseSessionRequest{SessionID: sessionID})
		closeDone <- closeResult{resp, err}
	}()

	// Poll for the fail-closed deny this must produce: proof (without any
	// white-box access) that markClosing, Interrupt, and
	// gates.CancelSession have all already run, in that order, forcing the
	// stuck RequestPermission call to unblock via ctx cancellation rather
	// than a real client answer.
	deadline := time.Now().Add(testTimeout)
	for len(fake.gateResponses()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	responses := fake.gateResponses()
	if len(responses) != 1 {
		t.Fatalf("RespondGate calls = %d, want exactly 1 (fail-closed deny forced by session/close)", len(responses))
	}
	if responses[0].GateID != gateID {
		t.Errorf("RespondGate GateID = %v, want %v", responses[0].GateID, gateID)
	}
	if responses[0].Action != string(gate.ApprovalDeny) {
		t.Errorf("RespondGate Action = %q, want %q (outstanding permission requests must be resolved denied)", responses[0].Action, gate.ApprovalDeny)
	}
	if got := fake.interruptCalls; got != 1 {
		t.Errorf("Interrupt calls = %d, want exactly 1 (in-flight prompt cancelled per 2.4)", got)
	}

	// --- Ordering assertion 1: new prompts are rejected as "closing"
	// (not merely "already in flight") while close is still in progress,
	// and the session is still present in the registry (resolveSession
	// still finds it — otherwise this would fail ResourceNotFound instead
	// of InvalidRequest). --------------------------------------------
	_, rejectErr := agentConn.Prompt(context.Background(), protocol.PromptRequest{
		SessionID: sessionID,
		Prompt:    textPrompt("rejected while closing"),
	})
	if rejectErr == nil {
		t.Fatal("Prompt while session is closing: error = nil, want a typed rejection")
	}
	var rejectFault *protocol.Fault
	if !errors.As(rejectErr, &rejectFault) {
		t.Fatalf("Prompt while session is closing: error = %v (%T), want *protocol.Fault", rejectErr, rejectErr)
	}
	if rejectFault.Code != protocol.ErrorCodeInvalidRequest {
		t.Errorf("Prompt while session is closing: Fault.Code = %v, want %v (session still registered, just closing)", rejectFault.Code, protocol.ErrorCodeInvalidRequest)
	}

	// --- Ordering assertion 2: the in-flight prompt is now genuinely
	// drained. gates.go's own fail-closed contract
	// (TestGateRequestPermissionConnectionDeathFailsClosed) means a
	// forced-closed permission round trip aborts drainToTerminal
	// immediately with a typed fault — it does not, and structurally
	// cannot, continue on to observe the turn's real terminal afterward
	// (the loop is durably parked on that exact gate). This IS "the
	// in-flight prompt cancelled and drained" for a prompt that was parked
	// on a gate: Interrupt signaled the cancellation, and forcing the gate
	// closed is what actually lets the drain conclude and the tracker
	// release, rather than hang forever waiting on an answer that will
	// never come. -------------------------------------------------------
	select {
	case r := <-promptDone:
		if r.err == nil {
			t.Fatal("in-flight Prompt: error = nil, want a typed failure (its permission round trip was forced closed)")
		}
		var promptFault *protocol.Fault
		if !errors.As(r.err, &promptFault) {
			t.Fatalf("in-flight Prompt error = %v (%T), want *protocol.Fault", r.err, r.err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the in-flight Prompt to return once its gate was forced closed")
	}

	// --- Ordering assertion 3: the drain has now finished, so
	// handleSessionClose has moved on to SessionCloser.Shutdown, which is
	// deliberately blocked (shutdownBlock). While it is blocked, the
	// session must STILL be in the registry (removal has not happened) —
	// this is the critical "registry removal only after teardown" proof.
	deadline = time.Now().Add(testTimeout)
	for {
		if calls, _ := fake.shutdownState(); calls == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for SessionCloser.Shutdown to be called")
		}
		time.Sleep(time.Millisecond)
	}

	_, stillPresentErr := agentConn.Prompt(context.Background(), protocol.PromptRequest{
		SessionID: sessionID,
		Prompt:    textPrompt("still present during shutdown"),
	})
	if stillPresentErr == nil {
		t.Fatal("Prompt while Shutdown is blocked: error = nil, want a typed rejection")
	}
	var stillPresentFault *protocol.Fault
	if !errors.As(stillPresentErr, &stillPresentFault) {
		t.Fatalf("Prompt while Shutdown is blocked: error = %v (%T), want *protocol.Fault", stillPresentErr, stillPresentErr)
	}
	if stillPresentFault.Code != protocol.ErrorCodeInvalidRequest {
		t.Fatalf("Prompt while Shutdown is blocked: Fault.Code = %v, want %v (registry removal must not have happened yet — session must still resolve, just be rejected as closing)", stillPresentFault.Code, protocol.ErrorCodeInvalidRequest)
	}

	if del.callCount() != 0 {
		t.Errorf("SessionDeleter.DeleteSession calls = %d, want 0 (durable history must be untouched by close) while Shutdown is still in progress", del.callCount())
	}

	// Unblock Shutdown and let session/close finish.
	close(fake.shutdownBlock)

	select {
	case r := <-closeDone:
		if r.err != nil {
			t.Fatalf("CloseSession: unexpected error: %v", r.err)
		}
		if r.resp == nil {
			t.Fatal("CloseSession: resp = nil, want a non-nil CloseSessionResponse")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for CloseSession to return")
	}

	if calls, hadDeadline := fake.shutdownState(); calls != 1 {
		t.Errorf("SessionCloser.Shutdown calls = %d, want exactly 1", calls)
	} else if !hadDeadline {
		t.Error("SessionCloser.Shutdown: ctx had no deadline, want a bounded context")
	}

	// --- Ordering assertion 4: registry removal has now genuinely
	// happened (only after Shutdown returned): a session-scoped call now
	// fails ResourceNotFound, not InvalidRequest — the session is gone,
	// not merely still-closing. -----------------------------------------
	_, goneErr := agentConn.Prompt(context.Background(), protocol.PromptRequest{
		SessionID: sessionID,
		Prompt:    textPrompt("after close"),
	})
	if goneErr == nil {
		t.Fatal("Prompt after CloseSession returned: error = nil, want ResourceNotFound")
	}
	var goneFault *protocol.Fault
	if !errors.As(goneErr, &goneFault) {
		t.Fatalf("Prompt after CloseSession returned: error = %v (%T), want *protocol.Fault", goneErr, goneErr)
	}
	if goneFault.Code != protocol.ErrorCodeResourceNotFound {
		t.Errorf("Prompt after CloseSession returned: Fault.Code = %v, want %v (session must be removed from the registry)", goneFault.Code, protocol.ErrorCodeResourceNotFound)
	}

	// --- Final assertion: durable history was never touched. ------------
	if del.callCount() != 0 {
		t.Errorf("SessionDeleter.DeleteSession calls = %d, want 0 (session/close must never invoke the delete capability)", del.callCount())
	}
}

// TestHandleSessionCloseWithoutInFlightPrompt covers the quiet path: no
// prompt in flight when session/close runs. It must still succeed, still
// call Interrupt (matching session/cancel's own unconditional behavior —
// Interrupt is a safe no-op when nothing is running), call the configured
// SessionCloser.Shutdown, and remove the session from the registry.
func TestHandleSessionCloseWithoutInFlightPrompt(t *testing.T) {
	fake := newFakeLiveSession(t)
	host := &promptHostStub{session: fake}
	a, err := agent.New(agent.Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	resp, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sessionID := resp.SessionID

	closeResp, err := agentConn.CloseSession(context.Background(), protocol.CloseSessionRequest{SessionID: sessionID})
	if err != nil {
		t.Fatalf("CloseSession: unexpected error: %v", err)
	}
	if closeResp == nil {
		t.Fatal("CloseSession: resp = nil, want a non-nil CloseSessionResponse")
	}

	if got := fake.interruptCalls; got != 1 {
		t.Errorf("Interrupt calls = %d, want exactly 1 (close always signals interrupt, matching session/cancel's own unconditional behavior)", got)
	}
	if calls, _ := fake.shutdownState(); calls != 1 {
		t.Errorf("SessionCloser.Shutdown calls = %d, want exactly 1", calls)
	}

	_, err = agentConn.Prompt(context.Background(), protocol.PromptRequest{SessionID: sessionID, Prompt: textPrompt("after close")})
	if err == nil {
		t.Fatal("Prompt after close: error = nil, want ResourceNotFound")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("Prompt after close: error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeResourceNotFound {
		t.Errorf("Prompt after close: Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeResourceNotFound)
	}
}

// TestHandleSessionCloseWithoutShutdownCapability proves the Shutdown step
// is genuinely conditional on the live session implementing the optional
// SessionCloser interface, rather than assumed present: closeOnlyLiveSession
// does not implement it at all, and session/close must still succeed and
// still remove the session from the registry.
func TestHandleSessionCloseWithoutShutdownCapability(t *testing.T) {
	live := newCloseOnlyLiveSession(t)
	host := &promptHostStub{session: live}
	a, err := agent.New(agent.Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	resp, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sessionID := resp.SessionID

	closeResp, err := agentConn.CloseSession(context.Background(), protocol.CloseSessionRequest{SessionID: sessionID})
	if err != nil {
		t.Fatalf("CloseSession: unexpected error: %v", err)
	}
	if closeResp == nil {
		t.Fatal("CloseSession: resp = nil, want a non-nil CloseSessionResponse")
	}
	if got := live.interruptCalls(); got != 1 {
		t.Errorf("Interrupt calls = %d, want exactly 1", got)
	}

	_, err = agentConn.Prompt(context.Background(), protocol.PromptRequest{SessionID: sessionID, Prompt: textPrompt("after close")})
	if err == nil {
		t.Fatal("Prompt after close: error = nil, want ResourceNotFound")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("Prompt after close: error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeResourceNotFound {
		t.Errorf("Prompt after close: Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeResourceNotFound)
	}
}

// TestHandleSessionCloseRejectsMalformedOrUnknownSessionID mirrors
// session/new's own malformed/unknown id table (session_test.go): every
// session-scoped method, including session/close, must reject before ever
// touching the host or registry.
func TestHandleSessionCloseRejectsMalformedOrUnknownSessionID(t *testing.T) {
	tests := []struct {
		name     string
		id       protocol.SessionID
		wantCode protocol.ErrorCode
	}{
		{name: "malformed", id: "not-a-uuid", wantCode: protocol.ErrorCodeInvalidParams},
		{name: "well-formed but unregistered", id: protocol.SessionID(mustNewUUID(t).String()), wantCode: protocol.ErrorCodeResourceNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host := &promptHostStub{session: newFakeLiveSession(t)}
			a, err := agent.New(agent.Options{Host: host})
			if err != nil {
				t.Fatalf("agent.New: %v", err)
			}
			client, server := pipeConns(t)
			a.Register(server)
			agentConn := protocol.NewAgentConn(client)

			_, err = agentConn.CloseSession(context.Background(), protocol.CloseSessionRequest{SessionID: tt.id})
			if err == nil {
				t.Fatal("CloseSession: error = nil, want a typed fault")
			}
			var f *protocol.Fault
			if !errors.As(err, &f) {
				t.Fatalf("CloseSession: error = %v (%T), want *protocol.Fault", err, err)
			}
			if f.Code != tt.wantCode {
				t.Errorf("CloseSession: Fault.Code = %v, want %v", f.Code, tt.wantCode)
			}
		})
	}
}

func mustNewUUID(t *testing.T) coreuuid.UUID {
	t.Helper()
	id, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	return id
}
