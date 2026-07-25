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

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	coreuuid "github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
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

// newCompactTestAgent wires a fresh agent.Agent with the given Compactor
// (nil is a valid, meaningful choice: "no Compactor configured") around
// fake, registers it on a piped connection, and creates one session through
// the real session/new handshake, mirroring prompt_test.go's
// newPromptTestAgent.
func newCompactTestAgent(t *testing.T, fake *fakeLiveSession, compactor agent.Compactor) (*protocol.AgentConn, *protocol.Conn, protocol.SessionID) {
	t.Helper()
	host := &promptHostStub{session: fake}
	a, err := agent.New(agent.Options{Host: host, Compactor: compactor})
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
