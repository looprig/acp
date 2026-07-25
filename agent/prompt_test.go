package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	coreuuid "github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/identity"
)

// --- fakes ------------------------------------------------------------

// fakeSubscription is the scripted event.Subscription every test in this
// file feeds directly: Events() exposes the channel the test writes to, and
// closing that channel (not calling Close) is how a test simulates the hub
// losing the subscription before a terminal arrives.
type fakeSubscription struct {
	ch chan event.Delivery

	mu     sync.Mutex
	closed bool
	err    error
}

func (s *fakeSubscription) Events() <-chan event.Delivery { return s.ch }

func (s *fakeSubscription) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *fakeSubscription) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// fakeLiveSession is the scripted LiveSession the task calls for: every
// method records that it was called (in a mutex-guarded trace, so ordering
// assertions reflect the real call sequence rather than an incidental one),
// and SubscribeEvents returns a channel-backed fakeSubscription the test
// feeds by hand.
type fakeLiveSession struct {
	id coreuuid.UUID

	mu        sync.Mutex
	calls     []string
	subFilter event.EventFilter
	subErr    error
	sub       *fakeSubscription

	events chan event.Delivery
	// submitted receives the minted command id every time Submit succeeds,
	// so a test can read the exact id it must use to build a correlated
	// TurnStarted without hardcoding anything.
	submitted chan coreuuid.UUID
	submitErr error

	interruptOnce  sync.Once
	interrupted    chan struct{}
	interruptCalls int
	interruptOK    bool
	interruptErr   error

	// respondGateCalls records every gate.GateResponse RespondGate was
	// called with, in order, so gates_test.go's fidelity/fail-closed tests
	// can assert exactly what (if anything) was delivered. respondGateErr,
	// when non-nil, is returned by every call (simulating a harness-side
	// RespondGate failure).
	respondGateCalls []gate.GateResponse
	respondGateErr   error

	// Shutdown fields back the optional agent.SessionCloser capability (see
	// host.go), exercised by close_test.go's session/close orchestration
	// tests. shutdownBlock, when non-nil, makes Shutdown block until the
	// test closes it — this is how close_test.go proves registry removal
	// happens only after Shutdown returns, by observing registry state
	// while Shutdown is deliberately held open.
	shutdownCalls       int
	shutdownErr         error
	shutdownBlock       chan struct{}
	shutdownHadDeadline bool
}

// Shutdown implements agent.SessionCloser. Every fakeLiveSession therefore
// structurally satisfies SessionCloser; tests that must exercise the "live
// session does NOT implement SessionCloser" branch use a different LiveSession
// type entirely (see close_test.go's closeOnlyLiveSession).
func (f *fakeLiveSession) Shutdown(ctx context.Context) error {
	f.mu.Lock()
	f.shutdownCalls++
	_, hasDeadline := ctx.Deadline()
	f.shutdownHadDeadline = hasDeadline
	block := f.shutdownBlock
	err := f.shutdownErr
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	return err
}

// shutdownState returns a snapshot of Shutdown's call count and whether the
// most recent call observed a context deadline.
func (f *fakeLiveSession) shutdownState() (calls int, hadDeadline bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shutdownCalls, f.shutdownHadDeadline
}

func newFakeLiveSession(t *testing.T) *fakeLiveSession {
	t.Helper()
	id, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	return &fakeLiveSession{
		id:          id,
		events:      make(chan event.Delivery),
		submitted:   make(chan coreuuid.UUID, 4),
		interrupted: make(chan struct{}),
		interruptOK: true,
	}
}

func (f *fakeLiveSession) SessionID() coreuuid.UUID { return f.id }

func (f *fakeLiveSession) SubscribeEvents(filter event.EventFilter) (event.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "subscribe")
	f.subFilter = filter
	if f.subErr != nil {
		return nil, f.subErr
	}
	sub := &fakeSubscription{ch: f.events}
	f.sub = sub
	return sub, nil
}

func (f *fakeLiveSession) Submit(_ context.Context, _ []content.Block) (coreuuid.UUID, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "submit")
	err := f.submitErr
	f.mu.Unlock()
	if err != nil {
		return coreuuid.UUID{}, err
	}
	cmdID, err := coreuuid.New()
	if err != nil {
		return coreuuid.UUID{}, err
	}
	f.submitted <- cmdID
	return cmdID, nil
}

func (f *fakeLiveSession) RespondGate(_ context.Context, resp gate.GateResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "respond_gate")
	f.respondGateCalls = append(f.respondGateCalls, resp)
	return f.respondGateErr
}

// gateResponses returns a snapshot of every gate.GateResponse RespondGate has
// been called with so far, in order.
func (f *fakeLiveSession) gateResponses() []gate.GateResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]gate.GateResponse(nil), f.respondGateCalls...)
}

func (f *fakeLiveSession) Interrupt(context.Context) (bool, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "interrupt")
	f.interruptCalls++
	ok, err := f.interruptOK, f.interruptErr
	f.mu.Unlock()
	f.interruptOnce.Do(func() { close(f.interrupted) })
	return ok, err
}

func (f *fakeLiveSession) callTrace() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// promptHostStub is a SessionHost whose NewSession always hands back the
// single pre-built LiveSession it was constructed with (unlike
// sessionHostStub in session_test.go, which mints a fresh stub every call):
// prompt.go's tests need to drive one specific, scripted session.
type promptHostStub struct {
	session agent.LiveSession
}

func (h *promptHostStub) NewSession(context.Context, agent.Setup) (agent.LiveSession, error) {
	return h.session, nil
}

func (h *promptHostStub) LoadSession(context.Context, agent.SessionID, agent.Setup) (agent.LoadedSession, error) {
	return agent.LoadedSession{}, errors.New("promptHostStub: LoadSession not implemented")
}

func (h *promptHostStub) ResumeSession(context.Context, agent.SessionID, agent.Setup) (agent.LiveSession, error) {
	return nil, errors.New("promptHostStub: ResumeSession not implemented")
}

// --- test setup helpers -------------------------------------------------

const testTimeout = 5 * time.Second

// newPromptTestAgent wires a fresh agent.Agent around fake, registers it on
// a piped connection, and creates one session through the real session/new
// handshake so the returned wire sessionId is one resolveSession will
// actually find (mirroring how these ids are minted in production, rather
// than reaching into the registry directly, which package agent_test cannot
// do anyway).
func newPromptTestAgent(t *testing.T, fake *fakeLiveSession) (*protocol.AgentConn, protocol.SessionID) {
	t.Helper()
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
	return agentConn, resp.SessionID
}

func textPrompt(text string) []protocol.ContentBlock {
	return []protocol.ContentBlock{{Text: &protocol.TextContent{Text: text}}}
}

// awaitSubmittedCommandID reads the command id fakeLiveSession.Submit minted,
// failing the test if Submit is never observed within testTimeout.
func awaitSubmittedCommandID(t *testing.T, fake *fakeLiveSession) coreuuid.UUID {
	t.Helper()
	select {
	case id := <-fake.submitted:
		return id
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Submit to be called")
		return coreuuid.UUID{}
	}
}

func send(t *testing.T, ch chan event.Delivery, ev event.Event) {
	t.Helper()
	select {
	case ch <- event.Delivery{Event: ev}:
	case <-time.After(testTimeout):
		t.Fatal("timed out sending event into fake subscription channel")
	}
}

func turnHeader(sessionID, loopID, turnID coreuuid.UUID, cmdID coreuuid.UUID) event.Header {
	return event.Header{
		Coordinates: identity.Coordinates{SessionID: sessionID, LoopID: loopID, TurnID: turnID},
		Cause:       identity.Cause{CommandID: cmdID},
	}
}

// --- Behavior 1: subscribe-before-submit, correlation, decoy filtering --

func TestHandlePromptSubscribesBeforeSubmitAndCorrelatesAcrossDecoys(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, sessionID := newPromptTestAgent(t, fake)

	otherLoop, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	otherTurn, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	otherCmd, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	realSession, err := coreuuid.New() // a session id decoys use, distinct from ours
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	type result struct {
		resp *protocol.PromptResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello"),
		})
		done <- result{resp, err}
	}()

	cmdID := awaitSubmittedCommandID(t, fake)

	// Decoy 1: TurnStarted for a completely different submitted command
	// (another prompt racing on the fan-in) must not be mistaken for
	// correlation.
	send(t, fake.events, event.TurnStarted{Header: turnHeader(realSession, otherLoop, otherTurn, otherCmd)})
	// Decoy 2: a terminal from that other loop/turn, arriving before our own
	// TurnStarted is even seen, must not complete our prompt.
	send(t, fake.events, event.TurnDone{Header: turnHeader(realSession, otherLoop, otherTurn, otherCmd)})

	loopID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	turnID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	// The real TurnStarted: Cause.CommandID matches the submitted command.
	send(t, fake.events, event.TurnStarted{Header: turnHeader(realSession, loopID, turnID, cmdID)})

	// Decoy 3 & 4: interleaved activity from other loops/turns AFTER
	// correlation must still be ignored.
	send(t, fake.events, event.TurnDone{Header: turnHeader(realSession, otherLoop, otherTurn, otherCmd)})
	send(t, fake.events, event.TurnFailed{Header: turnHeader(realSession, otherLoop, turnID, otherCmd), Err: errors.New("decoy failure")})

	// The correlated terminal.
	send(t, fake.events, event.TurnDone{Header: turnHeader(realSession, loopID, turnID, cmdID)})

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Prompt: unexpected error: %v", r.err)
		}
		if r.resp.StopReason != protocol.StopReasonEndTurn {
			t.Errorf("StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonEndTurn)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt to return")
	}

	trace := fake.callTrace()
	if len(trace) < 2 || trace[0] != "subscribe" || trace[1] != "submit" {
		t.Fatalf("call trace = %v, want [subscribe submit ...] (subscribe must happen strictly before submit)", trace)
	}
	if !fake.subFilter.Enduring.All {
		t.Errorf("subscribe filter Enduring.All = false, want true (the correlated loop is unknown before TurnStarted arrives)")
	}
}

// --- Behavior 2: terminal mapping + sanitized TurnFailed -----------------

func TestHandlePromptTerminalMapping(t *testing.T) {
	sessionUUID, err := coreuuid.New()
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

	const canary = "CANARY-db-password-hunter2-must-never-cross-the-wire"

	tests := []struct {
		name           string
		terminal       func(hdr event.Header) event.Event
		wantStopReason protocol.StopReason
		wantErr        bool
		wantErrText    string // if non-empty, must appear in the wire error message
	}{
		{
			name:           "TurnDone maps to end_turn",
			terminal:       func(hdr event.Header) event.Event { return event.TurnDone{Header: hdr} },
			wantStopReason: protocol.StopReasonEndTurn,
		},
		{
			name:           "TurnInterrupted maps to cancelled",
			terminal:       func(hdr event.Header) event.Event { return event.TurnInterrupted{Header: hdr} },
			wantStopReason: protocol.StopReasonCancelled,
		},
		{
			name: "TurnFailed with EmptyResponseError surfaces the exact safe message",
			terminal: func(hdr event.Header) event.Event {
				return event.TurnFailed{Header: hdr, Err: &event.EmptyResponseError{}}
			},
			wantErr:     true,
			wantErrText: (&event.EmptyResponseError{}).Error(),
		},
		{
			name: "TurnFailed with ToolLimitError surfaces the exact safe message",
			terminal: func(hdr event.Header) event.Event {
				return event.TurnFailed{Header: hdr, Err: &event.ToolLimitError{Iterations: 5, MaxIterations: 5, Calls: 9, MaxCalls: 9}}
			},
			wantErr:     true,
			wantErrText: (&event.ToolLimitError{Iterations: 5, MaxIterations: 5, Calls: 9, MaxCalls: 9}).Error(),
		},
		{
			name: "TurnFailed with TurnPanicError is sanitized (Detail never crosses the wire)",
			terminal: func(hdr event.Header) event.Event {
				return event.TurnFailed{Header: hdr, Err: &event.TurnPanicError{Detail: canary}}
			},
			wantErr: true,
		},
		{
			name: "TurnFailed with an untyped/raw cause is sanitized",
			terminal: func(hdr event.Header) event.Event {
				return event.TurnFailed{Header: hdr, Err: errors.New(canary)}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeLiveSession(t)
			agentConn, sessionID := newPromptTestAgent(t, fake)

			type result struct {
				resp *protocol.PromptResponse
				err  error
			}
			done := make(chan result, 1)
			go func() {
				resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
					SessionID: sessionID,
					Prompt:    textPrompt("hello"),
				})
				done <- result{resp, err}
			}()

			cmdID := awaitSubmittedCommandID(t, fake)
			hdr := turnHeader(sessionUUID, loopID, turnID, cmdID)
			send(t, fake.events, event.TurnStarted{Header: hdr})
			send(t, fake.events, tt.terminal(hdr))

			select {
			case r := <-done:
				if tt.wantErr {
					if r.err == nil {
						t.Fatal("Prompt: error = nil, want a sanitized prompt error")
					}
					var f *protocol.Fault
					if !errors.As(r.err, &f) {
						t.Fatalf("Prompt error = %v (%T), want *protocol.Fault", r.err, r.err)
					}
					if strings.Contains(f.Message, canary) {
						t.Fatalf("wire error message contains the raw canary text: %q", f.Message)
					}
					if tt.wantErrText != "" && !strings.Contains(f.Message, tt.wantErrText) {
						t.Errorf("wire error message = %q, want it to contain %q (typed-cause table must not collapse to one generic string)", f.Message, tt.wantErrText)
					}
					return
				}
				if r.err != nil {
					t.Fatalf("Prompt: unexpected error: %v", r.err)
				}
				if r.resp.StopReason != tt.wantStopReason {
					t.Errorf("StopReason = %v, want %v", r.resp.StopReason, tt.wantStopReason)
				}
			case <-time.After(testTimeout):
				t.Fatal("timed out waiting for Prompt to return")
			}
		})
	}
}

// --- Behavior 3: per-session serialization ------------------------------

func TestHandlePromptRejectsConcurrentPromptOnSameSession(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, sessionID := newPromptTestAgent(t, fake)

	type result struct {
		resp *protocol.PromptResponse
		err  error
	}
	first := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("first"),
		})
		first <- result{resp, err}
	}()

	// Wait until the first prompt has actually reached Submit (i.e. it is
	// genuinely in flight, subscribed and submitted) before firing the
	// second, so this is a real concurrency assertion and not a race that
	// happens to pass.
	cmdID := awaitSubmittedCommandID(t, fake)

	secondResp, secondErr := agentConn.Prompt(context.Background(), protocol.PromptRequest{
		SessionID: sessionID,
		Prompt:    textPrompt("second"),
	})
	if secondErr == nil {
		t.Fatal("second concurrent Prompt: error = nil, want a typed rejection")
	}
	if secondResp != nil {
		t.Errorf("second concurrent Prompt: resp = %+v, want nil", secondResp)
	}
	var f *protocol.Fault
	if !errors.As(secondErr, &f) {
		t.Fatalf("second concurrent Prompt error = %v (%T), want *protocol.Fault", secondErr, secondErr)
	}
	if f.Code != protocol.ErrorCodeInvalidRequest {
		t.Errorf("second concurrent Prompt Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidRequest)
	}

	// The registry must never have seen a second Submit: the rejection must
	// happen before touching the live session a second time.
	if trace := fake.callTrace(); len(trace) != 2 {
		t.Errorf("call trace = %v, want exactly [subscribe submit] (rejected prompt must never touch the live session)", trace)
	}

	// Let the first prompt complete normally.
	loopID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	turnID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	sessUUID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	hdr := turnHeader(sessUUID, loopID, turnID, cmdID)
	send(t, fake.events, event.TurnStarted{Header: hdr})
	send(t, fake.events, event.TurnDone{Header: hdr})

	select {
	case r := <-first:
		if r.err != nil {
			t.Fatalf("first Prompt: unexpected error: %v", r.err)
		}
		if r.resp.StopReason != protocol.StopReasonEndTurn {
			t.Errorf("first Prompt StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonEndTurn)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for first Prompt to return")
	}

	// Now that the first prompt has completed, a third prompt on the same
	// session must be accepted again (the tracker must have released).
	thirdDone := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("third"),
		})
		thirdDone <- result{resp, err}
	}()
	cmdID3 := awaitSubmittedCommandID(t, fake)
	hdr3 := turnHeader(sessUUID, loopID, turnID, cmdID3)
	send(t, fake.events, event.TurnStarted{Header: hdr3})
	send(t, fake.events, event.TurnDone{Header: hdr3})
	select {
	case r := <-thirdDone:
		if r.err != nil {
			t.Fatalf("third Prompt (after first released): unexpected error: %v", r.err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for third Prompt to return")
	}
}

// TestHandlePromptSerializationUnderRace hammers a session with many
// concurrent session/prompt attempts (run with -race -count=N per the
// task's ground rules) while ONE prompt is deterministically held in flight
// (its terminal is withheld until every attempt has been observed), so this
// is a genuine concurrency assertion rather than a race that happens to
// pass: every one of the concurrent attempts MUST be rejected, and none may
// ever reach Submit, because the tracker must serialize regardless of
// however many goroutines hit it at once. Only after every attempt has
// returned is the held prompt released.
func TestHandlePromptSerializationUnderRace(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, sessionID := newPromptTestAgent(t, fake)

	// Get one prompt genuinely in flight (subscribed and submitted) before
	// hammering it: this is what makes every subsequent attempt's rejection
	// deterministic rather than a race that happens to pass.
	type result struct {
		resp *protocol.PromptResponse
		err  error
	}
	holderDone := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("holder"),
		})
		holderDone <- result{resp, err}
	}()
	cmdID := awaitSubmittedCommandID(t, fake)

	const attempts = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	var rejected int

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
				SessionID: sessionID,
				Prompt:    textPrompt("hi"),
			})
			var f *protocol.Fault
			isRejection := err != nil && errors.As(err, &f) && f.Code == protocol.ErrorCodeInvalidRequest
			mu.Lock()
			defer mu.Unlock()
			if isRejection {
				rejected++
			}
		}()
	}
	wg.Wait()

	if rejected != attempts {
		t.Errorf("rejected = %d, want exactly %d (the holder was still in flight for all of them)", rejected, attempts)
	}
	// The live session must never have seen a second Submit: only the
	// holder's [subscribe submit] pair.
	if trace := fake.callTrace(); len(trace) != 2 {
		t.Errorf("call trace = %v, want exactly [subscribe submit] (no rejected attempt may touch the live session)", trace)
	}

	// Release the holder.
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
	send(t, fake.events, event.TurnDone{Header: hdr})

	select {
	case r := <-holderDone:
		if r.err != nil {
			t.Fatalf("holder Prompt: unexpected error: %v", r.err)
		}
		if r.resp.StopReason != protocol.StopReasonEndTurn {
			t.Errorf("holder StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonEndTurn)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for holder Prompt to return")
	}
}

// --- Behavior 4: subscription loss before terminal ----------------------

func TestHandlePromptSubscriptionClosedBeforeTerminalIsTypedFailure(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, sessionID := newPromptTestAgent(t, fake)

	type result struct {
		resp *protocol.PromptResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello"),
		})
		done <- result{resp, err}
	}()

	cmdID := awaitSubmittedCommandID(t, fake)
	sessUUID, _ := coreuuid.New()
	loopID, _ := coreuuid.New()
	turnID, _ := coreuuid.New()
	hdr := turnHeader(sessUUID, loopID, turnID, cmdID)
	send(t, fake.events, event.TurnStarted{Header: hdr})

	// Simulate the hub losing the subscription before any terminal arrives.
	close(fake.events)

	select {
	case r := <-done:
		if r.err == nil {
			t.Fatal("Prompt after subscription loss: error = nil, want a typed prompt failure (never an empty success)")
		}
		if r.resp != nil {
			t.Errorf("Prompt after subscription loss: resp = %+v, want nil", r.resp)
		}
		var f *protocol.Fault
		if !errors.As(r.err, &f) {
			t.Fatalf("Prompt after subscription loss: error = %v (%T), want *protocol.Fault", r.err, r.err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt to return after subscription loss")
	}
}

// --- Behavior 5: session/cancel mid-prompt ------------------------------

func TestHandlePromptCancelMidPromptInterruptsDrainsAndSucceeds(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, sessionID := newPromptTestAgent(t, fake)

	type result struct {
		resp *protocol.PromptResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello"),
		})
		done <- result{resp, err}
	}()

	cmdID := awaitSubmittedCommandID(t, fake)
	sessUUID, _ := coreuuid.New()
	loopID, _ := coreuuid.New()
	turnID, _ := coreuuid.New()
	hdr := turnHeader(sessUUID, loopID, turnID, cmdID)
	send(t, fake.events, event.TurnStarted{Header: hdr})

	if err := agentConn.Cancel(context.Background(), protocol.CancelNotification{SessionID: sessionID}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	select {
	case <-fake.interrupted:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Interrupt to be called")
	}

	// The handler must keep draining (never short-circuit) rather than
	// completing the moment Interrupt was called: feed one more decoy from
	// an entirely different loop/turn, then the real correlated terminal.
	decoyLoop, _ := coreuuid.New()
	decoyTurn, _ := coreuuid.New()
	send(t, fake.events, event.TurnDone{Header: turnHeader(sessUUID, decoyLoop, decoyTurn, cmdID)})
	send(t, fake.events, event.TurnInterrupted{Header: hdr})

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Prompt after cancel: unexpected error (cancellation must be reported as success): %v", r.err)
		}
		if r.resp.StopReason != protocol.StopReasonCancelled {
			t.Errorf("StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonCancelled)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt to return after cancel")
	}

	if got := fake.interruptCalls; got != 1 {
		t.Errorf("Interrupt calls = %d, want 1", got)
	}
}

// --- bonus: context cancellation during drain ---------------------------

func TestHandlePromptContextCancellationDuringDrainIsTypedFailure(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, sessionID := newPromptTestAgent(t, fake)

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		resp *protocol.PromptResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(ctx, protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello"),
		})
		done <- result{resp, err}
	}()

	awaitSubmittedCommandID(t, fake)
	cancel()

	select {
	case r := <-done:
		if r.err == nil {
			t.Fatal("Prompt after ctx cancel: error = nil, want a typed failure")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt to return after context cancellation")
	}
}

// --- bonus: Submit failure releases the in-flight tracker ---------------

func TestHandlePromptSubmitErrorReleasesInFlightTracker(t *testing.T) {
	fake := newFakeLiveSession(t)
	fake.submitErr = errors.New("submit boom")
	agentConn, sessionID := newPromptTestAgent(t, fake)

	_, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
		SessionID: sessionID,
		Prompt:    textPrompt("hello"),
	})
	if err == nil {
		t.Fatal("Prompt with failing Submit: error = nil, want an error")
	}

	// The tracker must have released even though Submit failed: a
	// subsequent prompt on the same session must not be rejected as
	// "already in flight".
	fake.mu.Lock()
	fake.submitErr = nil
	fake.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello again"),
		})
		if err != nil {
			t.Errorf("second Prompt after Submit failure released tracker: unexpected error: %v", err)
			return
		}
		if resp.StopReason != protocol.StopReasonEndTurn {
			t.Errorf("StopReason = %v, want %v", resp.StopReason, protocol.StopReasonEndTurn)
		}
	}()

	cmdID := awaitSubmittedCommandID(t, fake)
	sessUUID, _ := coreuuid.New()
	loopID, _ := coreuuid.New()
	turnID, _ := coreuuid.New()
	hdr := turnHeader(sessUUID, loopID, turnID, cmdID)
	send(t, fake.events, event.TurnStarted{Header: hdr})
	send(t, fake.events, event.TurnDone{Header: hdr})

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for second Prompt to return")
	}
}

// --- Behavior 6: live progress events are actually streamed to the client
// (Task 2.5's wiring of translate.go into the drain loop) --------------------

// TestHandlePromptStreamsLiveUpdatesOverTheWire is the integration-style
// check the task calls for: a real session/prompt drain, over a real
// piped protocol.Conn, must actually emit session/update notifications the
// client can observe on the wire — not just translate events correctly in
// isolation (translate_test.go already covers that unit by unit). It sends
// one TokenDelta, one ToolCallStarted, and one ToolCallCompleted from the
// correlated loop/turn, plus one decoy TokenDelta from a different loop
// (which must never surface as a client-visible update), before the
// terminal, and asserts the client received exactly the three real updates
// in order with the correct sessionId and a stamped _meta.
func TestHandlePromptStreamsLiveUpdatesOverTheWire(t *testing.T) {
	fake := newFakeLiveSession(t)
	host := &promptHostStub{session: fake}
	a, err := agent.New(agent.Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	updates := make(chan protocol.SessionNotification, 8)
	client.HandleNotify(string(protocol.MethodSessionUpdate), func(_ context.Context, _ string, params json.RawMessage) {
		var n protocol.SessionNotification
		if err := json.Unmarshal(params, &n); err != nil {
			t.Errorf("unmarshal session/update notification: %v", err)
			return
		}
		updates <- n
	})

	resp, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sessionID := resp.SessionID

	type result struct {
		resp *protocol.PromptResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello"),
		})
		done <- result{resp, err}
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

	// Decoy: Ephemeral traffic from a completely different loop, arriving
	// after correlation, must never surface as a session/update.
	decoyLoop, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	send(t, fake.events, event.TokenDelta{
		Header: turnHeader(sessUUID, decoyLoop, turnID, cmdID),
		Chunk:  &content.TextChunk{Text: "decoy, must never surface"},
	})

	toolExecID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	send(t, fake.events, event.TokenDelta{Header: hdr, Chunk: &content.TextChunk{Text: "hi there"}})
	send(t, fake.events, event.ToolCallStarted{Header: hdr, ToolExecutionID: toolExecID, ToolName: "bash", Summary: "listing files"})
	send(t, fake.events, event.ToolCallCompleted{Header: hdr, ToolExecutionID: toolExecID, ResultPreview: "a.txt\nb.txt"})
	send(t, fake.events, event.TurnDone{Header: hdr})

	wantSessionID := sessionID
	assertUpdate := func(want string) protocol.SessionNotification {
		t.Helper()
		select {
		case n := <-updates:
			if n.SessionID != wantSessionID {
				t.Errorf("SessionID = %q, want %q", n.SessionID, wantSessionID)
			}
			if len(n.Meta) == 0 {
				t.Error("_meta was not stamped on the wire notification")
			}
			switch want {
			case "agent_message_chunk":
				if n.Update.AgentMessageChunk == nil {
					t.Errorf("update = %+v, want agent_message_chunk", n.Update)
				}
			case "tool_call":
				if n.Update.ToolCall == nil {
					t.Errorf("update = %+v, want tool_call", n.Update)
				}
			case "tool_call_update":
				if n.Update.ToolCallUpdate == nil {
					t.Errorf("update = %+v, want tool_call_update", n.Update)
				}
			}
			return n
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for a %s session/update notification", want)
			return protocol.SessionNotification{}
		}
	}

	assertUpdate("agent_message_chunk")
	assertUpdate("tool_call")
	assertUpdate("tool_call_update")

	// No fourth update (in particular, not the decoy) must ever arrive.
	select {
	case n := <-updates:
		t.Fatalf("unexpected extra session/update notification: %+v", n)
	case <-time.After(200 * time.Millisecond):
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Prompt: unexpected error: %v", r.err)
		}
		if r.resp.StopReason != protocol.StopReasonEndTurn {
			t.Errorf("StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonEndTurn)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt to return")
	}
}

// --- bonus: unsupported content blocks fail closed without touching the
// live session, and release the in-flight tracker ------------------------

func TestHandlePromptRejectsUnsupportedContentBlockWithoutTouchingLiveSession(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, sessionID := newPromptTestAgent(t, fake)

	uri := "https://example.invalid/image.png"
	_, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
		SessionID: sessionID,
		Prompt: []protocol.ContentBlock{
			{Image: &protocol.ImageContent{Data: "", MimeType: "image/png", URI: &uri}},
		},
	})
	if err == nil {
		t.Fatal("Prompt with an image block: error = nil, want InvalidParams (unsupported block type)")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("Prompt with an image block: error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
	}
	if trace := fake.callTrace(); len(trace) != 0 {
		t.Errorf("call trace = %v, want empty (rejecting an unsupported block must never touch the live session)", trace)
	}

	// The tracker must have released: a follow-up prompt on the same
	// session must not be rejected as "already in flight".
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello"),
		})
		if err != nil {
			t.Errorf("Prompt after rejected content block: unexpected error: %v", err)
			return
		}
		if resp.StopReason != protocol.StopReasonEndTurn {
			t.Errorf("StopReason = %v, want %v", resp.StopReason, protocol.StopReasonEndTurn)
		}
	}()

	cmdID := awaitSubmittedCommandID(t, fake)
	sessUUID, _ := coreuuid.New()
	loopID, _ := coreuuid.New()
	turnID, _ := coreuuid.New()
	hdr := turnHeader(sessUUID, loopID, turnID, cmdID)
	send(t, fake.events, event.TurnStarted{Header: hdr})
	send(t, fake.events, event.TurnDone{Header: hdr})

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for follow-up Prompt to return")
	}
}
