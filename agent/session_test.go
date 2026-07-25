package agent_test

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

// TestHandleSessionNewPopulatesInitialConfigOptions is the Important fix from
// the Phase 4 follow-up review: session/new must surface the newly-created
// session's initial runtime configuration options and mode state when
// Options.ConfigCatalog is configured — without this, a real ACP client has
// no way to discover that config options or modes exist at all. The
// translation must be the exact same one applyConfigOption's own response
// uses (config.go's translateRuntimeConfigOptions), reused here via
// initialConfigState rather than duplicated.
func TestHandleSessionNewPopulatesInitialConfigOptions(t *testing.T) {
	host := &sessionHostStub{}
	catalog := &stubConfigCatalog{options: []agent.RuntimeConfigOption{
		modelOption("fast",
			agent.RuntimeConfigValue{ID: "fast", Name: "Fast"},
			agent.RuntimeConfigValue{ID: "slow", Name: "Slow"},
		),
		{
			ID:       agent.ModeConfigOptionID,
			Category: protocol.SessionConfigOptionCategoryMode,
			Name:     "Mode",
			Values: []agent.RuntimeConfigValue{
				{ID: "build", Name: "Build"},
				{ID: "plan", Name: "Plan"},
			},
			CurrentValue: "build",
		},
	}}
	a, err := agent.New(agent.Options{Host: host, ConfigCatalog: catalog})
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

	if len(resp.ConfigOptions) != 2 {
		t.Fatalf("resp.ConfigOptions has %d entries, want 2", len(resp.ConfigOptions))
	}
	if resp.Modes == nil {
		t.Fatal("resp.Modes = nil, want a populated SessionModeState (catalog offers the well-known mode option)")
	}
	if resp.Modes.CurrentModeID != "build" {
		t.Errorf("resp.Modes.CurrentModeID = %q, want %q", resp.Modes.CurrentModeID, "build")
	}
	if len(resp.Modes.AvailableModes) != 2 {
		t.Fatalf("resp.Modes.AvailableModes has %d entries, want 2", len(resp.Modes.AvailableModes))
	}
}

// TestHandleSessionNewOmitsModesWhenCatalogHasNoModeOption asserts Modes
// stays nil (never a zero-value SessionModeState) when the configured
// catalog offers no ModeConfigOptionID entry: a legitimate host shape (only
// model/effort-style options, no mode support at all).
func TestHandleSessionNewOmitsModesWhenCatalogHasNoModeOption(t *testing.T) {
	host := &sessionHostStub{}
	catalog := &stubConfigCatalog{options: []agent.RuntimeConfigOption{
		modelOption("fast", agent.RuntimeConfigValue{ID: "fast", Name: "Fast"}),
	}}
	a, err := agent.New(agent.Options{Host: host, ConfigCatalog: catalog})
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
	if resp.Modes != nil {
		t.Errorf("resp.Modes = %+v, want nil (no mode option in the catalog)", resp.Modes)
	}
	if len(resp.ConfigOptions) != 1 {
		t.Fatalf("resp.ConfigOptions has %d entries, want 1", len(resp.ConfigOptions))
	}
}

// TestHandleSessionNewNoConfigOptionsWithoutCatalog asserts nothing changes
// when Options.ConfigCatalog is not configured at all: ConfigOptions/Modes
// stay absent from the response exactly like before this fix.
func TestHandleSessionNewNoConfigOptionsWithoutCatalog(t *testing.T) {
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
	if resp.ConfigOptions != nil {
		t.Errorf("resp.ConfigOptions = %+v, want nil", resp.ConfigOptions)
	}
	if resp.Modes != nil {
		t.Errorf("resp.Modes = %+v, want nil", resp.Modes)
	}
}

// shutdownCheckHostStub is a SessionHost whose NewSession always hands back
// the same *raceLiveSession (SessionCloser-capable), used by
// TestHandleSessionNewShutsDownOrphanWhenInitialConfigFetchFails to prove the
// initial-config-state fetch failure path gets the exact same best-effort
// orphan cleanup as every other post-creation failure in handleSessionNew.
type shutdownCheckHostStub struct{ live *raceLiveSession }

func (h *shutdownCheckHostStub) NewSession(context.Context, agent.Setup) (agent.LiveSession, error) {
	return h.live, nil
}

func (h *shutdownCheckHostStub) LoadSession(context.Context, agent.SessionID, agent.Setup) (agent.LoadedSession, error) {
	return agent.LoadedSession{}, errors.New("shutdownCheckHostStub: LoadSession not implemented")
}

func (h *shutdownCheckHostStub) ResumeSession(context.Context, agent.SessionID, agent.Setup) (agent.LiveSession, error) {
	return nil, errors.New("shutdownCheckHostStub: ResumeSession not implemented")
}

// TestHandleSessionNewShutsDownOrphanWhenInitialConfigFetchFails asserts that
// when the initial config-options fetch (RuntimeConfigCatalog.RuntimeConfigOptions)
// fails AFTER the live session has already been created and registered,
// handleSessionNew reports InternalError and gives the now-orphaned session
// the same best-effort SessionCloser.Shutdown cleanup as every other
// post-registration failure (see shutdownOrphanedSession, replay.go).
func TestHandleSessionNewShutsDownOrphanWhenInitialConfigFetchFails(t *testing.T) {
	id, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	live := &raceLiveSession{id: id}
	host := &shutdownCheckHostStub{live: live}
	catalog := &stubConfigCatalog{err: errors.New("catalog unavailable")}
	a, err := agent.New(agent.Options{Host: host, ConfigCatalog: catalog})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	_, err = agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err == nil {
		t.Fatal("NewSession: error = nil, want InternalError when the initial config-options fetch fails")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInternalError {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
	}
	if calls, hadDeadline := live.shutdownState(); calls != 1 {
		t.Errorf("SessionCloser.Shutdown calls = %d, want exactly 1 (orphaned session must not be silently abandoned)", calls)
	} else if !hadDeadline {
		t.Error("SessionCloser.Shutdown: ctx had no deadline, want a bounded context")
	}
}

// raceLiveSession is a minimal LiveSession that also implements
// agent.SessionCloser with a call counter, used by
// TestHandleSessionNewOrphanedSessionShutdownOnRegistryRace to prove the
// loser of a session/new registry-capacity race has Shutdown called on the
// host-backed session that was created for it but never registered.
type raceLiveSession struct {
	id coreuuid.UUID

	mu                  sync.Mutex
	shutdownCalls       int
	shutdownHadDeadline bool
}

func (s *raceLiveSession) SessionID() coreuuid.UUID { return s.id }

func (s *raceLiveSession) Submit(context.Context, []content.Block) (coreuuid.UUID, error) {
	return coreuuid.UUID{}, errors.New("raceLiveSession: Submit not implemented")
}

func (s *raceLiveSession) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errors.New("raceLiveSession: SubscribeEvents not implemented")
}

func (s *raceLiveSession) RespondGate(context.Context, gate.GateResponse) error {
	return errors.New("raceLiveSession: RespondGate not implemented")
}

func (s *raceLiveSession) Interrupt(context.Context) (bool, error) {
	return false, errors.New("raceLiveSession: Interrupt not implemented")
}

// Shutdown implements agent.SessionCloser.
func (s *raceLiveSession) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdownCalls++
	_, hasDeadline := ctx.Deadline()
	s.shutdownHadDeadline = hasDeadline
	return nil
}

func (s *raceLiveSession) shutdownState() (calls int, hadDeadline bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shutdownCalls, s.shutdownHadDeadline
}

// gatingHostStub is a SessionHost whose NewSession mints a fresh
// raceLiveSession every call and records it, optionally gating on a
// barrier armed by arm: once armed, NewSession signals entry on entered and
// then blocks until the test closes release. This is how
// TestHandleSessionNewOrphanedSessionShutdownOnRegistryRace forces two
// concurrent session/new calls to both pass handleSessionNew's atCapacity
// pre-check and both genuinely create a real host-backed session before
// either's sessions.add call can run — the actual race window the
// registry-capacity leak lives in, not an accidental scheduling artifact.
type gatingHostStub struct {
	mu      sync.Mutex
	created []*raceLiveSession

	armed   bool
	entered chan struct{}
	release chan struct{}
}

func (h *gatingHostStub) arm(entered, release chan struct{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.armed = true
	h.entered = entered
	h.release = release
}

func (h *gatingHostStub) NewSession(_ context.Context, _ agent.Setup) (agent.LiveSession, error) {
	h.mu.Lock()
	armed := h.armed
	entered := h.entered
	release := h.release
	h.mu.Unlock()

	if armed {
		entered <- struct{}{}
		<-release
	}

	id, err := coreuuid.New()
	if err != nil {
		return nil, err
	}
	live := &raceLiveSession{id: id}
	h.mu.Lock()
	h.created = append(h.created, live)
	h.mu.Unlock()
	return live, nil
}

func (h *gatingHostStub) LoadSession(context.Context, agent.SessionID, agent.Setup) (agent.LoadedSession, error) {
	return agent.LoadedSession{}, errors.New("gatingHostStub: LoadSession not implemented")
}

func (h *gatingHostStub) ResumeSession(context.Context, agent.SessionID, agent.Setup) (agent.LiveSession, error) {
	return nil, errors.New("gatingHostStub: ResumeSession not implemented")
}

func (h *gatingHostStub) createdSessions() []*raceLiveSession {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*raceLiveSession(nil), h.created...)
}

// resetCreated clears the created-session log, so a test can discard the
// bookkeeping from an unraced fill phase and observe only what the racing
// calls themselves create.
func (h *gatingHostStub) resetCreated() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.created = nil
}

// TestHandleSessionNewOrphanedSessionShutdownOnRegistryRace reproduces the
// exact race a Phase 2 review flagged: two concurrent session/new calls at
// MaxLiveSessions-1 already registered, both passing atCapacity's advisory
// pre-check before either's Host.NewSession call completes, so both
// genuinely create a real host-backed session and only one can win the
// registry's last slot. The loser's live session must not be silently
// abandoned: handleSessionNew must best-effort call SessionCloser.Shutdown
// on it with a bounded context before returning the capacity-exceeded
// error, exactly like session/close's own optional Shutdown step
// (close.go).
func TestHandleSessionNewOrphanedSessionShutdownOnRegistryRace(t *testing.T) {
	host := &gatingHostStub{}
	a, err := agent.New(agent.Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	// Fill to MaxLiveSessions-1 with ordinary, unraced session/new calls
	// (the barrier is not armed yet, so these return immediately).
	for i := 0; i < agent.MaxLiveSessions-1; i++ {
		if _, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"}); err != nil {
			t.Fatalf("NewSession #%d (filling to MaxLiveSessions-1): unexpected error: %v", i+1, err)
		}
	}

	host.resetCreated()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	host.arm(entered, release)

	type result struct {
		sessionID protocol.SessionID
		err       error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			resp, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{sessionID: resp.SessionID}
		}()
	}

	// Wait for BOTH goroutines to be blocked inside Host.NewSession: proof
	// both already passed the atCapacity pre-check (which runs before
	// Host.NewSession in handleSessionNew) before either has a chance to
	// register or fail to register.
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(testTimeout):
			t.Fatalf("timed out waiting for both concurrent session/new calls to enter Host.NewSession (only %d entered)", i)
		}
	}
	close(release)

	var got [2]result
	for i := 0; i < 2; i++ {
		select {
		case got[i] = <-results:
		case <-time.After(testTimeout):
			t.Fatal("timed out waiting for both concurrent session/new calls to return")
		}
	}

	var successes, failures int
	var winnerID protocol.SessionID
	for _, r := range got {
		if r.err == nil {
			successes++
			winnerID = r.sessionID
			continue
		}
		failures++
		var f *protocol.Fault
		if !errors.As(r.err, &f) {
			t.Fatalf("loser NewSession: error = %v (%T), want *protocol.Fault", r.err, r.err)
		}
		if f.Code != protocol.ErrorCodeInternalError {
			t.Errorf("loser NewSession: Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
		}
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("successes = %d, failures = %d, want exactly 1 and 1", successes, failures)
	}

	created := host.createdSessions()
	if len(created) != 2 {
		t.Fatalf("Host.NewSession created %d sessions during the race, want exactly 2", len(created))
	}

	var winner, loser *raceLiveSession
	for _, s := range created {
		if s.id.String() == string(winnerID) {
			winner = s
		} else {
			loser = s
		}
	}
	if winner == nil || loser == nil {
		t.Fatalf("could not identify winner/loser among created sessions (winnerID=%s)", winnerID)
	}

	if loserCalls, loserHadDeadline := loser.shutdownState(); loserCalls != 1 {
		t.Errorf("loser SessionCloser.Shutdown calls = %d, want exactly 1 (orphaned host session must not be silently abandoned)", loserCalls)
	} else if !loserHadDeadline {
		t.Error("loser SessionCloser.Shutdown: ctx had no deadline, want a bounded context")
	}

	if winnerCalls, _ := winner.shutdownState(); winnerCalls != 0 {
		t.Errorf("winner SessionCloser.Shutdown calls = %d, want 0 (a registered session must not be torn down)", winnerCalls)
	}
}
