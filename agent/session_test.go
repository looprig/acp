package agent_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	coreuuid "github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
)

// sessionLiveStub is a minimal LiveSession whose identity is a real,
// randomly generated UUID; nothing in this task's tests exercises the rest
// of the interface (Submit/SubscribeEvents/RespondGate/Interrupt), so those
// methods only exist to honor LiveSession's full contract.
type sessionLiveStub struct{ id coreuuid.UUID }

func (s *sessionLiveStub) SessionID() coreuuid.UUID { return s.id }

func (s *sessionLiveStub) Submit(context.Context, []content.Block) (coreuuid.UUID, error) {
	return coreuuid.UUID{}, errors.New("sessionLiveStub: Submit not implemented")
}

func (s *sessionLiveStub) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errors.New("sessionLiveStub: SubscribeEvents not implemented")
}

func (s *sessionLiveStub) RespondGate(context.Context, gate.GateResponse) error {
	return errors.New("sessionLiveStub: RespondGate not implemented")
}

func (s *sessionLiveStub) Interrupt(context.Context) (bool, error) {
	return false, errors.New("sessionLiveStub: Interrupt not implemented")
}

// sessionHostStub is a SessionHost whose NewSession records every call
// (count and the Setup it received) and mints a fresh, real-UUID-backed
// sessionLiveStub each time, unless a caller has queued a specific error via
// nextErr.
type sessionHostStub struct {
	mu        sync.Mutex
	calls     int
	lastSetup agent.Setup
	nextErr   error
}

func (h *sessionHostStub) NewSession(_ context.Context, setup agent.Setup) (agent.LiveSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	h.lastSetup = setup
	if h.nextErr != nil {
		err := h.nextErr
		h.nextErr = nil
		return nil, err
	}
	id, err := coreuuid.New()
	if err != nil {
		return nil, err
	}
	return &sessionLiveStub{id: id}, nil
}

func (h *sessionHostStub) LoadSession(context.Context, agent.SessionID, agent.Setup) (agent.LoadedSession, error) {
	return agent.LoadedSession{}, errors.New("sessionHostStub: LoadSession not implemented")
}

func (h *sessionHostStub) ResumeSession(context.Context, agent.SessionID, agent.Setup) (agent.LiveSession, error) {
	return nil, errors.New("sessionHostStub: ResumeSession not implemented")
}

func (h *sessionHostStub) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func (h *sessionHostStub) setup() agent.Setup {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastSetup
}

// TestParseSessionIDRejectsMalformed is the table test Task 2.3 calls for:
// every session-scoped method must reject a malformed wire sessionId before
// it touches the host, and this is the one shared validation function every
// such handler goes through (see resolveSession).
func TestParseSessionIDRejectsMalformed(t *testing.T) {
	tests := []struct {
		name       string
		id         string
		wantReason agent.SessionIDReason
	}{
		{name: "empty", id: "", wantReason: agent.SessionIDReasonEmpty},
		{name: "not a uuid at all", id: "not-a-uuid", wantReason: agent.SessionIDReasonMalformed},
		{name: "too short", id: "1234", wantReason: agent.SessionIDReasonMalformed},
		{name: "correct length, non-hex digit", id: "123e4567-e89b-42d3-a456-42661417400g", wantReason: agent.SessionIDReasonMalformed},
		{name: "wrong version (well-known v1 uuid)", id: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", wantReason: agent.SessionIDReasonWrongVariant},
		{name: "correct version, wrong variant bits", id: "123e4567-e89b-42d3-0456-426614174000", wantReason: agent.SessionIDReasonWrongVariant},
		{name: "nil uuid", id: "00000000-0000-0000-0000-000000000000", wantReason: agent.SessionIDReasonWrongVariant},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := agent.ParseSessionID(protocol.SessionID(tt.id))
			if err == nil {
				t.Fatalf("ParseSessionID(%q) error = nil, want error", tt.id)
			}
			var sidErr *agent.SessionIDError
			if !errors.As(err, &sidErr) {
				t.Fatalf("ParseSessionID(%q) error = %v (%T), want *agent.SessionIDError", tt.id, err, err)
			}
			if sidErr.Reason != tt.wantReason {
				t.Errorf("ParseSessionID(%q) reason = %v, want %v", tt.id, sidErr.Reason, tt.wantReason)
			}
		})
	}
}

// TestParseSessionIDAcceptsCanonicalUUID is the happy path: a UUID string
// this facade itself would mint (uuid.New()'s output) round-trips exactly.
func TestParseSessionIDAcceptsCanonicalUUID(t *testing.T) {
	valid, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	got, err := agent.ParseSessionID(protocol.SessionID(valid.String()))
	if err != nil {
		t.Fatalf("ParseSessionID(%q): unexpected error: %v", valid.String(), err)
	}
	if got != valid {
		t.Errorf("ParseSessionID(%q) = %v, want %v", valid.String(), got, valid)
	}
}

// TestHandleSessionNewHappyPath asserts the full session/new path: Setup is
// built from the request's cwd, Host.NewSession is called exactly once, and
// the ACP wire sessionId returned is exactly the created session's Harness
// UUID string form.
func TestHandleSessionNewHappyPath(t *testing.T) {
	host := &sessionHostStub{}
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
	if host.callCount() != 1 {
		t.Fatalf("Host.NewSession calls = %d, want 1", host.callCount())
	}
	if host.setup().Cwd != "/workspace" {
		t.Errorf("Setup.Cwd = %q, want /workspace", host.setup().Cwd)
	}

	sessionID, err := agent.ParseSessionID(resp.SessionID)
	if err != nil {
		t.Fatalf("ParseSessionID(response sessionId %q): %v", resp.SessionID, err)
	}
	if sessionID.String() != string(resp.SessionID) {
		t.Errorf("round-tripped SessionID = %q, want exactly %q", sessionID.String(), resp.SessionID)
	}
}

// TestHandleSessionNewRejectsWhenAuthRequired asserts session/new reuses
// Task 2.2's AuthorizeSessionCreation gate rather than reimplementing it:
// with an Authenticator configured and no successful authenticate call yet,
// session/new must fail closed with AuthenticationRequired and must never
// touch the host.
func TestHandleSessionNewRejectsWhenAuthRequired(t *testing.T) {
	host := &sessionHostStub{}
	opts := agent.Options{
		Host:          host,
		Authenticator: &fakeAuthenticator{},
		AuthMethods:   []protocol.AuthMethod{{ID: "test-method", Name: "Test"}},
	}
	a, err := agent.New(opts)
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	_, err = agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err == nil {
		t.Fatal("NewSession before authenticate: error = nil, want AuthenticationRequired fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("NewSession before authenticate: error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeAuthenticationRequired {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeAuthenticationRequired)
	}
	if host.callCount() != 0 {
		t.Errorf("Host.NewSession calls = %d, want 0 (must fail closed before touching host)", host.callCount())
	}
}

// TestHandleSessionNewRejectsInvalidSetup asserts a malformed Setup field
// (here, a non-absolute cwd) is rejected with InvalidParams before the host
// is ever called, reusing Task 2.1's NewSetup validation rather than
// duplicating it.
func TestHandleSessionNewRejectsInvalidSetup(t *testing.T) {
	host := &sessionHostStub{}
	a, err := agent.New(agent.Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	_, err = agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "relative/path"})
	if err == nil {
		t.Fatal("NewSession(relative cwd): error = nil, want InvalidParams fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("NewSession(relative cwd): error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
	}
	if host.callCount() != 0 {
		t.Errorf("Host.NewSession calls = %d, want 0 (must fail closed before touching host)", host.callCount())
	}
}

// TestHandleSessionNewRejectsMCPWhenNotAccepted asserts session/new fails
// closed on requested MCP servers, matching the design's "otherwise it
// rejects... according to ACP rather than silently ignoring requested
// servers": no Options field yet advertises MCP composition acceptance, so
// every session/new request carrying MCP servers must be rejected.
func TestHandleSessionNewRejectsMCPWhenNotAccepted(t *testing.T) {
	host := &sessionHostStub{}
	a, err := agent.New(agent.Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	req := protocol.NewSessionRequest{
		Cwd: "/workspace",
		McpServers: []protocol.McpServer{
			{Stdio: &protocol.McpServerStdio{Name: "fixture", Command: "/usr/bin/fixture-mcp"}},
		},
	}
	_, err = agentConn.NewSession(context.Background(), req)
	if err == nil {
		t.Fatal("NewSession(mcpServers, not accepted): error = nil, want InvalidParams fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("NewSession(mcpServers, not accepted): error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
	}
	if host.callCount() != 0 {
		t.Errorf("Host.NewSession calls = %d, want 0 (must fail closed before touching host)", host.callCount())
	}
}

// TestHandleSessionNewRegistryBounded pins the exact overflow boundary at
// the wire level: MaxLiveSessions successful session/new calls, then the
// next one rejected, with the host never even invoked for the rejected
// attempt (the registry's capacity pre-check must short-circuit first).
func TestHandleSessionNewRegistryBounded(t *testing.T) {
	host := &sessionHostStub{}
	a, err := agent.New(agent.Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	for i := 0; i < agent.MaxLiveSessions; i++ {
		if _, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"}); err != nil {
			t.Fatalf("NewSession #%d (within MaxLiveSessions): unexpected error: %v", i+1, err)
		}
	}
	if host.callCount() != agent.MaxLiveSessions {
		t.Fatalf("Host.NewSession calls = %d, want %d", host.callCount(), agent.MaxLiveSessions)
	}

	_, err = agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err == nil {
		t.Fatal("NewSession beyond MaxLiveSessions: error = nil, want overflow rejection")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("NewSession beyond MaxLiveSessions: error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInternalError {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
	}
	if host.callCount() != agent.MaxLiveSessions {
		t.Errorf("Host.NewSession calls = %d, want unchanged %d (capacity pre-check must short-circuit before touching host)", host.callCount(), agent.MaxLiveSessions)
	}
}
