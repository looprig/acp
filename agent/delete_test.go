package agent_test

// delete_test.go tests the session/delete handler: Task 3.4 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// session/delete is the exact converse of close.go's own invariant
// (close_test.go's recordingDeleter proves SessionDeleter is NEVER called by
// session/close): this handler is the ONLY path that may ever invoke
// SessionDeleter, and only when the requested sessionId does NOT currently
// name a live, registered session. Attempting to delete a LIVE session is
// rejected before the Deleter is ever consulted -- durable history must
// never be deleted out from under a live session -- so the client must
// session/close it first.
//
// The pinned schema (protocol/schema/v1/schema.json's DeleteSessionRequest/
// DeleteSessionResponse $defs) defines no dedicated error code for "session
// still active": the request carries only {sessionId, _meta} and the
// response only {_meta}. Absent a schema-documented specific code, this
// handler reports the rejection as protocol.InvalidRequest (-32600),
// matching the exact precedent prompt.go's ErrSessionClosing/
// ErrPromptAlreadyInFlight already set for an analogous "invalid given
// current session state" condition -- not a guessed or novel mapping.
import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	coreuuid "github.com/looprig/core/uuid"
)

// stubDeleter is the SessionDeleter test double every test in this file
// drives: it records every DeleteSession call (count and last id) and can be
// scripted to return an error, either a *protocol.Fault (to prove
// pass-through) or a plain error (to prove InternalError wrapping) --
// matching every other optional-capability handler's identical host-error
// convention (see e.g. resume.go/handleSessionResume).
type stubDeleter struct {
	mu     sync.Mutex
	calls  int
	lastID agent.SessionID
	err    error
}

func (d *stubDeleter) DeleteSession(_ context.Context, id agent.SessionID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	d.lastID = id
	return d.err
}

func (d *stubDeleter) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func (d *stubDeleter) lastCalledID() agent.SessionID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastID
}

// newDeleteTestAgent wires a fresh agent.Agent around a live promptHostStub
// session (reused from prompt_test.go) with del configured as Options.Deleter,
// registers it on a piped connection, and creates one session through the
// real session/new handshake -- mirroring newPromptTestAgent's own pattern
// (prompt_test.go), but parameterized on Deleter, which that helper does not
// expose.
func newDeleteTestAgent(t *testing.T, del agent.SessionDeleter) (agentConn *protocol.AgentConn, liveSessionID protocol.SessionID) {
	t.Helper()
	fake := newFakeLiveSession(t)
	host := &promptHostStub{session: fake}
	a, err := agent.New(agent.Options{Host: host, Deleter: del})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn = protocol.NewAgentConn(client)

	resp, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	return agentConn, resp.SessionID
}

// TestHandleSessionDeleteRejectsLiveSession is this task's central
// assertion: session/delete on a sessionId that currently names a LIVE
// session is rejected -- InvalidRequest, matching prompt.go's precedent for
// "invalid given current session state" -- and the Deleter is NEVER called.
// The exact wire error is golden-pinned: the pinned schema documents no
// dedicated code for this condition, so this test also fixes the concrete
// choice this handler makes.
func TestHandleSessionDeleteRejectsLiveSession(t *testing.T) {
	del := &stubDeleter{}
	agentConn, liveID := newDeleteTestAgent(t, del)

	_, err := agentConn.DeleteSession(context.Background(), protocol.DeleteSessionRequest{SessionID: liveID})
	if err == nil {
		t.Fatal("DeleteSession(live session): error = nil, want InvalidRequest fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidRequest {
		t.Errorf("Fault.Code = %v, want %v (ErrorCodeInvalidRequest)", f.Code, protocol.ErrorCodeInvalidRequest)
	}
	wantMessage := "session/delete: session is still active; close it first"
	if f.Message != wantMessage {
		t.Errorf("Fault.Message = %q, want %q", f.Message, wantMessage)
	}

	if got := del.callCount(); got != 0 {
		t.Errorf("Deleter.DeleteSession calls = %d, want 0 (must reject before ever touching the Deleter)", got)
	}
}

// TestHandleSessionDeleteInvokesDeleterWhenNotLive asserts the converse: a
// well-formed sessionId that does NOT name a currently-registered live
// session reaches SessionDeleter.DeleteSession with the resolved id, and the
// handler answers with an empty DeleteSessionResponse.
func TestHandleSessionDeleteInvokesDeleterWhenNotLive(t *testing.T) {
	del := &stubDeleter{}
	agentConn, _ := newDeleteTestAgent(t, del)

	notLiveID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	resp, err := agentConn.DeleteSession(context.Background(), protocol.DeleteSessionRequest{
		SessionID: protocol.SessionID(notLiveID.String()),
	})
	if err != nil {
		t.Fatalf("DeleteSession(not-live session): %v", err)
	}
	if resp == nil {
		t.Fatal("DeleteSession: resp = nil")
	}

	if got := del.callCount(); got != 1 {
		t.Fatalf("Deleter.DeleteSession calls = %d, want 1", got)
	}
	if got := del.lastCalledID(); got != notLiveID {
		t.Errorf("Deleter.DeleteSession id = %v, want %v", got, notLiveID)
	}
}

// TestHandleSessionDeleteSucceedsAfterClose exercises the intended real-world
// lifecycle end to end: session/close first (removing the session from the
// live registry, per close.go's own orchestration), then session/delete on
// the SAME id now succeeds and reaches the Deleter -- proving the "must
// close first" rejection is not permanent, only conditioned on current
// liveness.
func TestHandleSessionDeleteSucceedsAfterClose(t *testing.T) {
	del := &stubDeleter{}
	agentConn, liveID := newDeleteTestAgent(t, del)

	if _, err := agentConn.CloseSession(context.Background(), protocol.CloseSessionRequest{SessionID: liveID}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}

	if _, err := agentConn.DeleteSession(context.Background(), protocol.DeleteSessionRequest{SessionID: liveID}); err != nil {
		t.Fatalf("DeleteSession(after close): %v", err)
	}
	if got := del.callCount(); got != 1 {
		t.Errorf("Deleter.DeleteSession calls = %d, want 1 (after close, the session is no longer live)", got)
	}
}

// TestHandleSessionDeleteRejectsMalformedSessionID asserts a malformed wire
// sessionId is rejected by ParseSessionID before either the registry or the
// Deleter is ever touched, matching every other session-scoped handler's
// validation discipline (all external input is untrusted).
func TestHandleSessionDeleteRejectsMalformedSessionID(t *testing.T) {
	del := &stubDeleter{}
	agentConn, _ := newDeleteTestAgent(t, del)

	_, err := agentConn.DeleteSession(context.Background(), protocol.DeleteSessionRequest{SessionID: "not-a-uuid"})
	if err == nil {
		t.Fatal("DeleteSession(malformed sessionId): error = nil, want InvalidParams fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
	}
	if got := del.callCount(); got != 0 {
		t.Errorf("Deleter.DeleteSession calls = %d, want 0 (must fail closed before touching the Deleter)", got)
	}
}

// TestHandleSessionDeletePassesThroughTypedHostFault asserts a *protocol.Fault
// returned by SessionDeleter.DeleteSession is passed through unchanged,
// matching every other optional-capability handler's identical rule (see
// e.g. handleSessionResume/handleSessionList).
func TestHandleSessionDeletePassesThroughTypedHostFault(t *testing.T) {
	wantFault := protocol.ResourceNotFound("session/delete: no such durable session", nil)
	del := &stubDeleter{err: wantFault}
	agentConn, _ := newDeleteTestAgent(t, del)

	notLiveID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	_, err = agentConn.DeleteSession(context.Background(), protocol.DeleteSessionRequest{
		SessionID: protocol.SessionID(notLiveID.String()),
	})
	if err == nil {
		t.Fatal("DeleteSession: error = nil, want the Deleter's Fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeResourceNotFound {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeResourceNotFound)
	}
}

// TestHandleSessionDeleteWrapsPlainHostError asserts a plain (non-Fault)
// error returned by SessionDeleter.DeleteSession is wrapped as InternalError,
// matching every other optional-capability handler's identical rule.
func TestHandleSessionDeleteWrapsPlainHostError(t *testing.T) {
	del := &stubDeleter{err: errors.New("boom")}
	agentConn, _ := newDeleteTestAgent(t, del)

	notLiveID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	_, err = agentConn.DeleteSession(context.Background(), protocol.DeleteSessionRequest{
		SessionID: protocol.SessionID(notLiveID.String()),
	})
	if err == nil {
		t.Fatal("DeleteSession: error = nil, want InternalError fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInternalError {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
	}
}

// TestHandleSessionDeleteNotRegisteredWithoutDeleter asserts session/delete
// is rejected with MethodNotFound (not registered at all) when
// Options.Deleter is nil -- the capability-gating half of Task 3.4's
// advertisement matrix, complementing capabilities_test.go's own
// TestCapabilityAdvertisementMatrix "without capability" case for
// SessionDeleter (this test additionally proves the SAME agent instance
// never wires session/delete regardless of a live session existing).
func TestHandleSessionDeleteNotRegisteredWithoutDeleter(t *testing.T) {
	agentConn, liveID := newDeleteTestAgent(t, nil)

	_, err := agentConn.DeleteSession(context.Background(), protocol.DeleteSessionRequest{SessionID: liveID})
	if err == nil {
		t.Fatal("DeleteSession without Deleter: error = nil, want MethodNotFound fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeMethodNotFound {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeMethodNotFound)
	}
}
