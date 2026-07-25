package agent_test

// compact_test.go tests the `/compact` internal slash command and its
// available_commands_update advertisement: Task 4.2 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// Behaviors, one test each:
//   - TestCompactRoutesToCompactorAndCorrelatesResolved: an exact "/compact"
//     prompt is routed to Compactor.Compact (never LiveSession.Submit), and
//     the response completes only once the correlated CompactWaiterResolved
//     is observed, ignoring decoys along the way.
//   - TestCompactRejectedIsSanitized: a correlated CompactWaiterRejected
//     produces a sanitized *protocol.Fault (compact_internal_test.go already
//     covers the full allowlist at the unit level; this proves the wire path
//     actually uses it).
//   - TestCompactOtherSlashTextFallsThroughAsOrdinaryPrompt: the negative
//     test — several other slash-prefixed (and plain) strings, including
//     near-misses of "/compact" itself, are never routed to Compact and are
//     submitted as ordinary prompt content instead.
//   - TestCompactNotRoutedWhenNoCompactorConfigured: the literal "/compact"
//     text falls through to ordinary prompt handling when Options.Compactor
//     is nil.
//   - TestAvailableCommandsUpdateAdvertisedOnceWhenCompactorConfigured: the
//     first session/prompt call for a session with a Compactor configured
//     sends exactly one available_commands_update naming "compact"; a second
//     prompt on the same session sends no additional one.
//   - TestAvailableCommandsUpdateNotSentWithoutCompactor: no
//     available_commands_update is ever sent when Options.Compactor is nil.
//   - TestCompactResolvesPerSessionNotConnectionWide: the regression test for
//     the Task 4.2 follow-up fix. Three DISTINCT Compactor fakes are wired
//     in: one per session (sessions A and B, each on its own fakeLiveSession)
//     plus a third standing in for the connection-level Options.Compactor
//     (present only to gate advertisement — see host.go's Compactor doc).
//     Sending "/compact" on session A, then on session B, must invoke only
//     that session's OWN compactor each time; the connection-level one must
//     never be invoked at all. Before the fix, handleCompactPrompt called
//     a.opts.Compactor.Compact directly, so this test fails against that code
//     (the connection-level fake observes the calls instead of either
//     session's own) — see the test's own comment for exactly how this was
//     confirmed.
//   - TestCompactFailsClosedWhenSessionLacksCompactor: Options.Compactor is
//     configured (so `/compact` is advertised) but the specific session's
//     LiveSession value does not implement Compactor at all
//     (liveSessionWithoutCompactor) — `/compact` on that session must fail
//     closed with a sanitized *protocol.Fault, never panic, and never fall
//     back to invoking the connection-level Options.Compactor.

import (
	"context"
	"errors"
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

// scriptedCompactor is the scripted agent.Compactor these tests drive: every
// Compact call is counted, and (unless err is set) mints and reports a fresh
// command id on minted so a test can read the exact id it must correlate
// against, exactly like fakeLiveSession.submitted does for Submit
// (prompt_test.go).
type scriptedCompactor struct {
	mu     sync.Mutex
	calls  int
	err    error
	minted chan coreuuid.UUID
}

func newScriptedCompactor() *scriptedCompactor {
	return &scriptedCompactor{minted: make(chan coreuuid.UUID, 4)}
}

func (c *scriptedCompactor) Compact(context.Context) (coreuuid.UUID, error) {
	c.mu.Lock()
	c.calls++
	err := c.err
	c.mu.Unlock()
	if err != nil {
		return coreuuid.UUID{}, err
	}
	id, genErr := coreuuid.New()
	if genErr != nil {
		return coreuuid.UUID{}, genErr
	}
	c.minted <- id
	return id, nil
}

func (c *scriptedCompactor) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// awaitCompactCommandID reads the command id scriptedCompactor.Compact
// minted, failing the test if Compact is never observed within testTimeout.
func awaitCompactCommandID(t *testing.T, c *scriptedCompactor) coreuuid.UUID {
	t.Helper()
	select {
	case id := <-c.minted:
		return id
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Compactor.Compact to be called")
		return coreuuid.UUID{}
	}
}

// compactorPresenceMarker is a Compactor whose Compact method panics if ever
// invoked. It is what these tests wire into Options.Compactor whenever a
// Compactor is configured at all: since the fix scopes actual invocation to
// a per-session live.(Compactor) type-assertion (handleCompactPrompt,
// compact.go) and Options.Compactor is repurposed as an advertisement-only
// presence signal (host.go's Compactor doc), nothing should EVER call
// Options.Compactor.Compact directly again. Wiring a panicking marker here
// — instead of a counting fake — means any regression that reintroduces a
// direct a.opts.Compactor.Compact call fails loudly and immediately, rather
// than silently invoking the wrong session's compaction.
type compactorPresenceMarker struct{}

func (compactorPresenceMarker) Compact(context.Context) (coreuuid.UUID, error) {
	panic("agent: Options.Compactor.Compact invoked directly; compaction must resolve per-session via live.(Compactor)")
}

// newCompactTestAgent wires a fresh agent.Agent around fake, registers it on
// a piped connection, and creates one session through the real session/new
// handshake, mirroring prompt_test.go's newPromptTestAgent. compactor (nil is
// a valid, meaningful choice: "no Compactor configured") is wired onto fake
// itself (fake.compactor) so per-session resolution (live.(Compactor)) finds
// it; Options.Compactor is set to compactorPresenceMarker whenever compactor
// is non-nil, purely so advertisement/routing sees a Compactor is configured
// at the connection level, without ever being the thing actually invoked.
func newCompactTestAgent(t *testing.T, fake *fakeLiveSession, compactor agent.Compactor) (*protocol.AgentConn, *protocol.Conn, protocol.SessionID) {
	t.Helper()
	fake.compactor = compactor
	opts := agent.Options{Host: &promptHostStub{session: fake}}
	if compactor != nil {
		opts.Compactor = compactorPresenceMarker{}
	}
	a, err := agent.New(opts)
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
	return agentConn, client, resp.SessionID
}

// compactHeader builds the minimal event.Header a CompactWaiterResolved/
// CompactWaiterRejected fake needs for this file's correlation tests: only
// Cause.CommandID is ever consulted by drainCompactionToTerminal.
func compactHeader(cmdID coreuuid.UUID) event.Header {
	return event.Header{Cause: identity.Cause{CommandID: cmdID}}
}

// --- Behavior 1: routing + correlation on CompactWaiterResolved ---------

func TestCompactRoutesToCompactorAndCorrelatesResolved(t *testing.T) {
	fake := newFakeLiveSession(t)
	compactor := newScriptedCompactor()
	agentConn, _, sessionID := newCompactTestAgent(t, fake, compactor)

	type result struct {
		resp *protocol.PromptResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("/compact"),
		})
		done <- result{resp, err}
	}()

	cmdID := awaitCompactCommandID(t, compactor)

	// Decoy: an unrelated CompactWaiterRejected for a different command id
	// must never be mistaken for our own outcome.
	otherCmd, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	send(t, fake.events, event.CompactWaiterRejected{Header: compactHeader(otherCmd), Reason: event.CompactRejectInternal})
	// Decoy: unrelated turn activity interleaved on the same fan-in.
	decoyTurnCmd, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	send(t, fake.events, event.TurnDone{Header: turnHeader(coreuuid.UUID{}, coreuuid.UUID{}, coreuuid.UUID{}, decoyTurnCmd)})

	// The correlated outcome.
	send(t, fake.events, event.CompactWaiterResolved{Header: compactHeader(cmdID), CommittedEventID: mustUUID(t)})

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Prompt(/compact): unexpected error: %v", r.err)
		}
		if r.resp.StopReason != protocol.StopReasonEndTurn {
			t.Errorf("StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonEndTurn)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt(/compact) to return")
	}

	if got := compactor.callCount(); got != 1 {
		t.Errorf("Compactor.Compact calls = %d, want 1", got)
	}
	// The live session's ordinary prompt path (Submit) must never have been
	// touched: /compact is routed to Compactor, not Submit.
	trace := fake.callTrace()
	for _, call := range trace {
		if call == "submit" {
			t.Fatalf("call trace = %v: /compact must never call LiveSession.Submit", trace)
		}
	}
	if len(trace) == 0 || trace[0] != "subscribe" {
		t.Fatalf("call trace = %v, want it to start with subscribe", trace)
	}
}

func mustUUID(t *testing.T) coreuuid.UUID {
	t.Helper()
	id, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	return id
}

// --- Behavior 2: rejection is sanitized on the wire ----------------------

func TestCompactRejectedIsSanitized(t *testing.T) {
	fake := newFakeLiveSession(t)
	compactor := newScriptedCompactor()
	agentConn, _, sessionID := newCompactTestAgent(t, fake, compactor)

	done := make(chan error, 1)
	go func() {
		_, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("/compact"),
		})
		done <- err
	}()

	cmdID := awaitCompactCommandID(t, compactor)
	send(t, fake.events, event.CompactWaiterRejected{Header: compactHeader(cmdID), Reason: event.CompactRejectExecutionFailed})

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Prompt(/compact) after CompactWaiterRejected: error = nil, want a sanitized rejection")
		}
		var f *protocol.Fault
		if !errors.As(err, &f) {
			t.Fatalf("Prompt(/compact) error = %v (%T), want *protocol.Fault", err, err)
		}
		if f.Code != protocol.ErrorCodeInternalError {
			t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt(/compact) to return")
	}
}

// --- Behavior 3: negative test — every other slash text (and near-misses
// of "/compact" itself) falls through as ordinary prompt content ----------

func TestCompactOtherSlashTextFallsThroughAsOrdinaryPrompt(t *testing.T) {
	texts := []string{
		"hello",
		"/help",
		"/compactness is nice",
		"/compact please",
		"/compact ",
		" /compact",
		"/compact\n",
		"//compact",
		"/Compact",
	}

	for _, text := range texts {
		t.Run(text, func(t *testing.T) {
			fake := newFakeLiveSession(t)
			compactor := newScriptedCompactor()
			agentConn, _, sessionID := newCompactTestAgent(t, fake, compactor)

			type result struct {
				resp *protocol.PromptResponse
				err  error
			}
			done := make(chan result, 1)
			go func() {
				resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
					SessionID: sessionID,
					Prompt:    textPrompt(text),
				})
				done <- result{resp, err}
			}()

			// Proves the ordinary Submit path was actually taken (not just
			// "no crash"): awaitSubmittedCommandID blocks until
			// LiveSession.Submit is called, which only ever happens on the
			// ordinary prompt path.
			cmdID := awaitSubmittedCommandID(t, fake)
			hdr := turnHeader(mustUUID(t), mustUUID(t), mustUUID(t), cmdID)
			send(t, fake.events, event.TurnStarted{Header: hdr})
			send(t, fake.events, event.TurnDone{Header: hdr})

			select {
			case r := <-done:
				if r.err != nil {
					t.Fatalf("Prompt(%q): unexpected error: %v", text, r.err)
				}
				if r.resp.StopReason != protocol.StopReasonEndTurn {
					t.Errorf("StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonEndTurn)
				}
			case <-time.After(testTimeout):
				t.Fatalf("timed out waiting for Prompt(%q) to return", text)
			}

			if got := compactor.callCount(); got != 0 {
				t.Errorf("Prompt(%q): Compactor.Compact calls = %d, want 0 (must never be specially routed)", text, got)
			}
		})
	}
}

// --- Behavior 4: "/compact" falls through when no Compactor is configured

func TestCompactNotRoutedWhenNoCompactorConfigured(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, _, sessionID := newCompactTestAgent(t, fake, nil)

	type result struct {
		resp *protocol.PromptResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("/compact"),
		})
		done <- result{resp, err}
	}()

	cmdID := awaitSubmittedCommandID(t, fake)
	hdr := turnHeader(mustUUID(t), mustUUID(t), mustUUID(t), cmdID)
	send(t, fake.events, event.TurnStarted{Header: hdr})
	send(t, fake.events, event.TurnDone{Header: hdr})

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Prompt(/compact) without a Compactor: unexpected error: %v", r.err)
		}
		if r.resp.StopReason != protocol.StopReasonEndTurn {
			t.Errorf("StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonEndTurn)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt(/compact) to return")
	}
}

// --- Behavior 5: available_commands_update advertisement gating ---------

func TestAvailableCommandsUpdateAdvertisedOnceWhenCompactorConfigured(t *testing.T) {
	fake := newFakeLiveSession(t)
	compactor := newScriptedCompactor()
	agentConn, client, sessionID := newCompactTestAgent(t, fake, compactor)
	updates := collectSessionUpdates(t, client)

	// First prompt (ordinary text): must trigger exactly one
	// available_commands_update naming "compact".
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello"),
		})
		if err != nil {
			t.Errorf("first Prompt: unexpected error: %v", err)
			return
		}
		if resp.StopReason != protocol.StopReasonEndTurn {
			t.Errorf("first Prompt StopReason = %v, want %v", resp.StopReason, protocol.StopReasonEndTurn)
		}
	}()

	cmdID := awaitSubmittedCommandID(t, fake)
	hdr := turnHeader(mustUUID(t), mustUUID(t), mustUUID(t), cmdID)
	send(t, fake.events, event.TurnStarted{Header: hdr})
	send(t, fake.events, event.TurnDone{Header: hdr})

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for first Prompt to return")
	}

	var got *protocol.SessionNotification
	select {
	case n := <-updates:
		got = &n
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for available_commands_update")
	}
	if got.Update.AvailableCommandsUpdate == nil {
		t.Fatalf("update = %+v, want available_commands_update", got.Update)
	}
	cmds := got.Update.AvailableCommandsUpdate.AvailableCommands
	if len(cmds) != 1 || cmds[0].Name != "compact" {
		t.Errorf("AvailableCommands = %+v, want exactly one named %q", cmds, "compact")
	}

	// No second available_commands_update must arrive from this same send.
	select {
	case n := <-updates:
		t.Fatalf("unexpected extra session/update: %+v", n)
	case <-time.After(200 * time.Millisecond):
	}

	// Second prompt on the SAME session: must NOT send a second
	// available_commands_update (advertised exactly once per session).
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		_, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello again"),
		})
		if err != nil {
			t.Errorf("second Prompt: unexpected error: %v", err)
		}
	}()
	cmdID2 := awaitSubmittedCommandID(t, fake)
	hdr2 := turnHeader(mustUUID(t), mustUUID(t), mustUUID(t), cmdID2)
	send(t, fake.events, event.TurnStarted{Header: hdr2})
	send(t, fake.events, event.TurnDone{Header: hdr2})
	select {
	case <-done2:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for second Prompt to return")
	}

	select {
	case n := <-updates:
		t.Fatalf("unexpected available_commands_update on second prompt: %+v", n)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestAvailableCommandsUpdateNotSentWithoutCompactor(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, client, sessionID := newCompactTestAgent(t, fake, nil)
	updates := collectSessionUpdates(t, client)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello"),
		})
		if err != nil {
			t.Errorf("Prompt: unexpected error: %v", err)
		}
	}()

	cmdID := awaitSubmittedCommandID(t, fake)
	hdr := turnHeader(mustUUID(t), mustUUID(t), mustUUID(t), cmdID)
	send(t, fake.events, event.TurnStarted{Header: hdr})
	send(t, fake.events, event.TurnDone{Header: hdr})

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt to return")
	}

	select {
	case n := <-updates:
		t.Fatalf("unexpected session/update without a Compactor configured: %+v", n)
	case <-time.After(200 * time.Millisecond):
	}
}

// --- Regression: per-session Compactor resolution (follow-up fix) --------

// multiSessionHostStub is a SessionHost whose NewSession hands back each of a
// fixed queue of pre-built LiveSession values in order, one per call. Unlike
// promptHostStub (always the identical single session) or session_test.go's
// sessionHostStub (mints a fresh session this test cannot pre-wire a
// distinguishable Compactor onto), this is what
// TestCompactResolvesPerSessionNotConnectionWide needs to put two distinct,
// independently-scripted sessions on the SAME Agent.
type multiSessionHostStub struct {
	mu    sync.Mutex
	queue []agent.LiveSession
}

func (h *multiSessionHostStub) NewSession(context.Context, agent.Setup) (agent.LiveSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.queue) == 0 {
		return nil, errors.New("multiSessionHostStub: NewSession queue exhausted")
	}
	live := h.queue[0]
	h.queue = h.queue[1:]
	return live, nil
}

func (h *multiSessionHostStub) LoadSession(context.Context, agent.SessionID, agent.Setup) (agent.LoadedSession, error) {
	return agent.LoadedSession{}, errors.New("multiSessionHostStub: LoadSession not implemented")
}

func (h *multiSessionHostStub) ResumeSession(context.Context, agent.SessionID, agent.Setup) (agent.LiveSession, error) {
	return nil, errors.New("multiSessionHostStub: ResumeSession not implemented")
}

// TestCompactResolvesPerSessionNotConnectionWide is the core regression test
// for the Task 4.2 follow-up bug: handleCompactPrompt used to call
// a.opts.Compactor.Compact(ctx) directly — a single connection-level field —
// with no session identifier passed anywhere. Two sessions on the same
// Agent sending "/compact" would therefore invoke the exact same Compact
// call on the exact same shared object, with nothing distinguishing which
// session's history should actually be compacted.
//
// This test wires in THREE distinct scriptedCompactor fakes: compactorConn
// stands in for the connection-level Options.Compactor (present only to
// gate advertisement — see host.go's Compactor doc), while compactorA and
// compactorB are each wired onto their own session's fakeLiveSession
// (session A and session B respectively) via the per-session `compactor`
// field. compactorConn is never equal to either session's own compactor, so
// any call actually reaching it (instead of the correlated session's own)
// is unambiguous evidence of the bug.
//
// Proving this fails against the pre-fix code: with the old
// `a.opts.Compactor.Compact(ctx)` body, EVERY `/compact` call — regardless
// of which session sent it — resolves to a.opts.Compactor, i.e.
// compactorConn here. Sending "/compact" on session A would therefore
// increment compactorConn.callCount() (not compactorA's), and the assertion
// below that compactorConn.callCount() stays 0 throughout would fail
// (observed 1, then 2, instead of 0). Confirmed by temporarily reverting
// compact.go's handleCompactPrompt to the old `a.opts.Compactor.Compact(ctx)`
// body and re-running this test: it fails with exactly that shape
// (compactorA/compactorB calls = 0, compactorConn calls = 1 after the first
// "/compact" and 2 after the second) rather than panicking or passing
// vacuously. Against the fixed code (live.(Compactor) resolution), each
// session's OWN compactor is the only one ever invoked, and compactorConn
// stays at 0 for the whole test.
func TestCompactResolvesPerSessionNotConnectionWide(t *testing.T) {
	fakeA := newFakeLiveSession(t)
	fakeB := newFakeLiveSession(t)
	compactorConn := newScriptedCompactor()
	compactorA := newScriptedCompactor()
	compactorB := newScriptedCompactor()
	fakeA.compactor = compactorA
	fakeB.compactor = compactorB

	host := &multiSessionHostStub{queue: []agent.LiveSession{fakeA, fakeB}}
	a, err := agent.New(agent.Options{Host: host, Compactor: compactorConn})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	respA, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace/a"})
	if err != nil {
		t.Fatalf("NewSession (A): %v", err)
	}
	respB, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace/b"})
	if err != nil {
		t.Fatalf("NewSession (B): %v", err)
	}

	type result struct {
		resp *protocol.PromptResponse
		err  error
	}

	// "/compact" on session A must invoke ONLY compactorA.
	doneA := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: respA.SessionID,
			Prompt:    textPrompt("/compact"),
		})
		doneA <- result{resp, err}
	}()
	cmdA := awaitCompactCommandID(t, compactorA)
	send(t, fakeA.events, event.CompactWaiterResolved{Header: compactHeader(cmdA), CommittedEventID: mustUUID(t)})
	select {
	case r := <-doneA:
		if r.err != nil {
			t.Fatalf("Prompt(/compact) on session A: unexpected error: %v", r.err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt(/compact) on session A to return")
	}

	if got := compactorA.callCount(); got != 1 {
		t.Errorf("after /compact on session A: compactorA calls = %d, want 1", got)
	}
	if got := compactorB.callCount(); got != 0 {
		t.Errorf("after /compact on session A: compactorB calls = %d, want 0 (a different session's compactor must never be invoked)", got)
	}
	if got := compactorConn.callCount(); got != 0 {
		t.Errorf("after /compact on session A: connection-level Options.Compactor calls = %d, want 0 (must never be invoked directly; this is the exact pre-fix bug)", got)
	}

	// "/compact" on session B must invoke ONLY compactorB; compactorA and
	// compactorConn must remain exactly as they were.
	doneB := make(chan result, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: respB.SessionID,
			Prompt:    textPrompt("/compact"),
		})
		doneB <- result{resp, err}
	}()
	cmdB := awaitCompactCommandID(t, compactorB)
	send(t, fakeB.events, event.CompactWaiterResolved{Header: compactHeader(cmdB), CommittedEventID: mustUUID(t)})
	select {
	case r := <-doneB:
		if r.err != nil {
			t.Fatalf("Prompt(/compact) on session B: unexpected error: %v", r.err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt(/compact) on session B to return")
	}

	if got := compactorB.callCount(); got != 1 {
		t.Errorf("after /compact on session B: compactorB calls = %d, want 1", got)
	}
	if got := compactorA.callCount(); got != 1 {
		t.Errorf("after /compact on session B: compactorA calls = %d, want still 1 (unaffected by session B's /compact)", got)
	}
	if got := compactorConn.callCount(); got != 0 {
		t.Errorf("after /compact on session B: connection-level Options.Compactor calls = %d, want 0 (must never be invoked directly)", got)
	}
}

// liveSessionWithoutCompactor is a minimal LiveSession that deliberately does
// NOT implement agent.Compactor, unlike fakeLiveSession (which does — see its
// Compact method in prompt_test.go). It backs
// TestCompactFailsClosedWhenSessionLacksCompactor, mirroring close_test.go's
// closeOnlyLiveSession pattern for the analogous SessionCloser case.
type liveSessionWithoutCompactor struct {
	id     coreuuid.UUID
	events chan event.Delivery
}

func newLiveSessionWithoutCompactor(t *testing.T) *liveSessionWithoutCompactor {
	t.Helper()
	return &liveSessionWithoutCompactor{id: mustUUID(t), events: make(chan event.Delivery)}
}

func (s *liveSessionWithoutCompactor) SessionID() coreuuid.UUID { return s.id }

func (s *liveSessionWithoutCompactor) Submit(context.Context, []content.Block) (coreuuid.UUID, error) {
	return coreuuid.UUID{}, errors.New("liveSessionWithoutCompactor: Submit not implemented")
}

func (s *liveSessionWithoutCompactor) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return &fakeSubscription{ch: s.events}, nil
}

func (s *liveSessionWithoutCompactor) RespondGate(context.Context, gate.GateResponse) error {
	return errors.New("liveSessionWithoutCompactor: RespondGate not implemented")
}

func (s *liveSessionWithoutCompactor) Interrupt(context.Context) (bool, error) {
	return true, nil
}

// TestCompactFailsClosedWhenSessionLacksCompactor proves handleCompactPrompt
// fails closed via a live.(Compactor) type-assertion miss, rather than
// assuming every LiveSession supports compaction just because
// Options.Compactor is configured at the connection level (which only gates
// advertisement — see host.go's Compactor doc). The connection-level
// compactorPresenceMarker would panic if ever invoked directly, so a passing
// test here also proves the not-implemented path never falls back to it.
func TestCompactFailsClosedWhenSessionLacksCompactor(t *testing.T) {
	live := newLiveSessionWithoutCompactor(t)
	host := &promptHostStub{session: live}
	a, err := agent.New(agent.Options{Host: host, Compactor: compactorPresenceMarker{}})
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

	_, err = agentConn.Prompt(context.Background(), protocol.PromptRequest{
		SessionID: resp.SessionID,
		Prompt:    textPrompt("/compact"),
	})
	if err == nil {
		t.Fatal("Prompt(/compact) on a session whose LiveSession lacks Compactor: error = nil, want a fail-closed error")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("Prompt(/compact) error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInternalError {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
	}
}
