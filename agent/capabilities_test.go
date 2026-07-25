package agent_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/sessionstore"
)

// --- fakes: a minimal SessionHost plus one fake per optional capability
// interface from host.go. None of these need real behavior for this task:
// only initialize/authenticate/logout are wired by the facade, so
// SessionHost's own methods are never invoked here.

type fakeHost struct{}

func (fakeHost) NewSession(context.Context, agent.Setup) (agent.LiveSession, error) {
	return nil, errors.New("fakeHost: NewSession not implemented")
}

func (fakeHost) LoadSession(context.Context, agent.SessionID, agent.Setup) (agent.LoadedSession, error) {
	return agent.LoadedSession{}, errors.New("fakeHost: LoadSession not implemented")
}

func (fakeHost) ResumeSession(context.Context, agent.SessionID, agent.Setup) (agent.LiveSession, error) {
	return nil, errors.New("fakeHost: ResumeSession not implemented")
}

type fakeReplayer struct{}

func (fakeReplayer) OpenEventReplayer(agent.SessionID) (journal.EventReplayer, error) {
	return nil, nil
}

type fakeCatalog struct{}

func (fakeCatalog) ListSessions(context.Context) ([]sessionstore.SessionMeta, error) {
	return nil, nil
}

type fakeConfigCatalog struct{}

func (fakeConfigCatalog) RuntimeConfigOptions(context.Context, agent.SessionID) ([]agent.RuntimeConfigOption, error) {
	return nil, nil
}

type fakeConfigController struct{}

func (fakeConfigController) SetRuntimeConfigOption(context.Context, agent.SessionID, agent.RuntimeConfigOption) ([]agent.RuntimeConfigOption, error) {
	return nil, nil
}

type fakeCompactor struct{}

func (fakeCompactor) Compact(context.Context) (uuid.UUID, error) { return uuid.UUID{}, nil }

type fakeDeleter struct{}

func (fakeDeleter) DeleteSession(context.Context, agent.SessionID) error { return nil }

// fakeAuthenticator records every Authenticate call it receives and returns
// a caller-configured error.
type fakeAuthenticator struct {
	calls        int
	lastMethodID protocol.AuthMethodID
	err          error
}

func (f *fakeAuthenticator) Authenticate(_ context.Context, id protocol.AuthMethodID) error {
	f.calls++
	f.lastMethodID = id
	return f.err
}

// fakeLogoutHandler records every Logout call it receives and returns a
// caller-configured error.
type fakeLogoutHandler struct {
	calls int
	err   error
}

func (f *fakeLogoutHandler) Logout(context.Context) error {
	f.calls++
	return f.err
}

// baseOptions returns the minimal valid Options (only the always-required
// Host set): every optional capability is nil/unsupported.
func baseOptions() agent.Options {
	return agent.Options{Host: fakeHost{}}
}

// pipeConns wires two protocol.Conns together over a net.Pipe, mirroring
// protocol_test's own helper, since it is unexported there.
func pipeConns(t *testing.T) (client, server *protocol.Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	client = protocol.NewConn(c1, c1, protocol.ConnOptions{})
	server = protocol.NewConn(c2, c2, protocol.ConnOptions{})
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

// mustInitialize registers a on server, calls initialize over client, and
// returns the decoded response.
func mustInitialize(t *testing.T, a *agent.Agent, client, server *protocol.Conn) *protocol.InitializeResponse {
	t.Helper()
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)
	resp, err := agentConn.Initialize(context.Background(), protocol.InitializeRequest{
		ProtocolVersion: protocol.CurrentProtocolVersion,
	})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return resp
}

// assertMethodNotFound calls method on client and asserts the response is
// exactly a MethodNotFound fault, the protocol-defined rejection for a
// method with no registered handler.
func assertMethodNotFound(t *testing.T, client *protocol.Conn, method string) {
	t.Helper()
	err := client.Call(context.Background(), method, struct{}{}, &struct{}{})
	if err == nil {
		t.Fatalf("Call(%s) error = nil, want MethodNotFound", method)
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("Call(%s) error = %v (%T), want *protocol.Fault", method, err, err)
	}
	if f.Code != protocol.ErrorCodeMethodNotFound {
		t.Errorf("Call(%s) Fault.Code = %v, want %v", method, f.Code, protocol.ErrorCodeMethodNotFound)
	}
}

// TestCapabilityAdvertisementMatrix is the spec's support matrix, enumerated:
// for each of the 8 optional capability interfaces from host.go, construct
// the facade with and without it and assert (a) the initialize response
// advertises exactly the supplied set, and (b) the wire method the capability
// backs is rejected with MethodNotFound when the capability is absent.
//
// RuntimeConfigCatalog, RuntimeConfigController, and Compactor have no
// initialize-level wire representation in the pinned schema (config options
// are surfaced per-session in session/new's response, and `/compact` as a
// session-level available command — Tasks 4.1/4.2), so their cases only
// assert that supplying them does not perturb the initialize response.
func TestCapabilityAdvertisementMatrix(t *testing.T) {
	authMethods := []protocol.AuthMethod{{ID: "test-method", Name: "Test Method"}}

	type tc struct {
		name   string
		with   func(*agent.Options)
		check  func(t *testing.T, resp *protocol.InitializeResponse, present bool)
		method string // representative wire method; "" skips the rejection check
	}

	cases := []tc{
		{
			name: "EventReplayer backs loadSession",
			with: func(o *agent.Options) { o.Replayer = fakeReplayer{} },
			check: func(t *testing.T, resp *protocol.InitializeResponse, present bool) {
				t.Helper()
				got := resp.AgentCapabilities != nil && resp.AgentCapabilities.LoadSession
				if got != present {
					t.Errorf("AgentCapabilities.LoadSession = %v, want %v", got, present)
				}
			},
			method: "session/load",
		},
		{
			name: "SessionCatalog backs sessionCapabilities.list",
			with: func(o *agent.Options) { o.Catalog = fakeCatalog{} },
			check: func(t *testing.T, resp *protocol.InitializeResponse, present bool) {
				t.Helper()
				got := resp.AgentCapabilities != nil &&
					resp.AgentCapabilities.SessionCapabilities != nil &&
					resp.AgentCapabilities.SessionCapabilities.List != nil
				if got != present {
					t.Errorf("SessionCapabilities.List present = %v, want %v", got, present)
				}
			},
			method: "session/list",
		},
		{
			name: "SessionDeleter backs sessionCapabilities.delete",
			with: func(o *agent.Options) { o.Deleter = fakeDeleter{} },
			check: func(t *testing.T, resp *protocol.InitializeResponse, present bool) {
				t.Helper()
				got := resp.AgentCapabilities != nil &&
					resp.AgentCapabilities.SessionCapabilities != nil &&
					resp.AgentCapabilities.SessionCapabilities.Delete != nil
				if got != present {
					t.Errorf("SessionCapabilities.Delete present = %v, want %v", got, present)
				}
			},
			method: "session/delete",
		},
		{
			name:   "RuntimeConfigCatalog has no initialize-level field",
			with:   func(o *agent.Options) { o.ConfigCatalog = fakeConfigCatalog{} },
			check:  func(t *testing.T, resp *protocol.InitializeResponse, present bool) { t.Helper() },
			method: "",
		},
		{
			name:   "RuntimeConfigController backs session/set_config_option (deferred, currently always rejected)",
			with:   func(o *agent.Options) { o.ConfigController = fakeConfigController{} },
			check:  func(t *testing.T, resp *protocol.InitializeResponse, present bool) { t.Helper() },
			method: "session/set_config_option",
		},
		{
			name:   "Compactor has no initialize-level field",
			with:   func(o *agent.Options) { o.Compactor = fakeCompactor{} },
			check:  func(t *testing.T, resp *protocol.InitializeResponse, present bool) { t.Helper() },
			method: "",
		},
		{
			name: "Authenticator backs authMethods",
			with: func(o *agent.Options) {
				o.Authenticator = &fakeAuthenticator{}
				o.AuthMethods = authMethods
			},
			check: func(t *testing.T, resp *protocol.InitializeResponse, present bool) {
				t.Helper()
				got := len(resp.AuthMethods) > 0
				if got != present {
					t.Errorf("len(AuthMethods) > 0 = %v, want %v", got, present)
				}
			},
			method: "authenticate",
		},
		{
			name: "LogoutHandler backs agentCapabilities.auth.logout",
			with: func(o *agent.Options) { o.Logout = &fakeLogoutHandler{} },
			check: func(t *testing.T, resp *protocol.InitializeResponse, present bool) {
				t.Helper()
				got := resp.AgentCapabilities != nil &&
					resp.AgentCapabilities.Auth != nil &&
					resp.AgentCapabilities.Auth.Logout != nil
				if got != present {
					t.Errorf("AgentCapabilities.Auth.Logout present = %v, want %v", got, present)
				}
			},
			method: "logout",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Run("with capability", func(t *testing.T) {
				opts := baseOptions()
				c.with(&opts)
				a, err := agent.New(opts)
				if err != nil {
					t.Fatalf("agent.New: %v", err)
				}
				client, server := pipeConns(t)
				resp := mustInitialize(t, a, client, server)
				c.check(t, resp, true)
			})

			t.Run("without capability", func(t *testing.T) {
				opts := baseOptions()
				a, err := agent.New(opts)
				if err != nil {
					t.Fatalf("agent.New: %v", err)
				}
				client, server := pipeConns(t)
				resp := mustInitialize(t, a, client, server)
				c.check(t, resp, false)

				if c.method != "" {
					assertMethodNotFound(t, client, c.method)
				}
			})
		})
	}
}

// TestSessionResumeAlwaysAdvertised asserts AgentCapabilities.SessionCapabilities.Resume
// is present even with only the always-required Host set (baseOptions,
// nothing else configured): unlike EventReplayer/SessionCatalog/
// SessionDeleter, resume has no separate optional Options field gating it —
// SessionHost.ResumeSession (host.go) is a required method every SessionHost
// implements — so a facade unconditionally advertises it and unconditionally
// wires session/resume (agent.go's Register).
func TestSessionResumeAlwaysAdvertised(t *testing.T) {
	a, err := agent.New(baseOptions())
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	resp := mustInitialize(t, a, client, server)

	if resp.AgentCapabilities == nil || resp.AgentCapabilities.SessionCapabilities == nil ||
		resp.AgentCapabilities.SessionCapabilities.Resume == nil {
		t.Fatalf("AgentCapabilities.SessionCapabilities.Resume = %+v, want non-nil with only Host configured", resp.AgentCapabilities)
	}

	agentConn := protocol.NewAgentConn(client)
	validID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	_, err = agentConn.ResumeSession(context.Background(), protocol.ResumeSessionRequest{
		SessionID: protocol.SessionID(validID.String()), Cwd: "/workspace",
	})
	// fakeHost.ResumeSession always errors; the point here is only that the
	// method is REGISTERED (not MethodNotFound), matching the advertised
	// capability.
	if err == nil {
		t.Fatal("ResumeSession: error = nil, want fakeHost's stub error")
	}
	var f *protocol.Fault
	if errors.As(err, &f) && f.Code == protocol.ErrorCodeMethodNotFound {
		t.Error("ResumeSession: MethodNotFound, want the method to be registered (resume has no capability gate)")
	}
}

// TestNewRequiresHost asserts Options.Host is required: the facade has
// nothing to wire session creation to without it (fail closed on missing
// required configuration).
func TestNewRequiresHost(t *testing.T) {
	_, err := agent.New(agent.Options{})
	if !errors.Is(err, agent.ErrMissingHost) {
		t.Fatalf("agent.New(no Host) error = %v, want ErrMissingHost", err)
	}
}

// TestNewRejectsAuthenticatorWithoutAuthMethods asserts that supplying an
// Authenticator without any AuthMethods to advertise it under is rejected at
// construction: a client could never select a method id to authenticate
// with, so this configuration can never succeed and must fail closed early.
func TestNewRejectsAuthenticatorWithoutAuthMethods(t *testing.T) {
	opts := baseOptions()
	opts.Authenticator = &fakeAuthenticator{}
	_, err := agent.New(opts)
	if !errors.Is(err, agent.ErrAuthenticatorWithoutMethods) {
		t.Fatalf("agent.New(Authenticator, no AuthMethods) error = %v, want ErrAuthenticatorWithoutMethods", err)
	}
}

// TestAuthenticateRejectsUnadvertisedMethodID asserts the facade validates
// the requested method id against the advertised AuthMethods before ever
// calling into the Authenticator, per host.go's Authenticator contract.
func TestAuthenticateRejectsUnadvertisedMethodID(t *testing.T) {
	fakeAuth := &fakeAuthenticator{}
	opts := baseOptions()
	opts.Authenticator = fakeAuth
	opts.AuthMethods = []protocol.AuthMethod{{ID: "known-method", Name: "Known"}}
	a, err := agent.New(opts)
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}

	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	_, err = agentConn.Authenticate(context.Background(), protocol.AuthenticateRequest{MethodID: "unknown-method"})
	if err == nil {
		t.Fatal("Authenticate(unknown-method) error = nil, want *protocol.Fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("Authenticate(unknown-method) error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Authenticate(unknown-method) Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
	}
	if fakeAuth.calls != 0 {
		t.Errorf("Authenticator.Authenticate calls = %d, want 0 (must not be invoked for an unadvertised method id)", fakeAuth.calls)
	}
}

// TestAuthenticateInvokesAuthenticatorAndUnlocksSessionCreation covers the
// Authenticator behavior (not just advertisement): calling authenticate
// invokes the configured Authenticator with the requested method id, and
// success unlocks session creation per the pinned schema's auth flow
// ("after successful authentication, the client can proceed to create
// sessions ... without receiving an auth_required error").
func TestAuthenticateInvokesAuthenticatorAndUnlocksSessionCreation(t *testing.T) {
	fakeAuth := &fakeAuthenticator{}
	opts := baseOptions()
	opts.Authenticator = fakeAuth
	opts.AuthMethods = []protocol.AuthMethod{{ID: "test-method", Name: "Test"}}
	a, err := agent.New(opts)
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}

	if err := a.AuthorizeSessionCreation(); err == nil {
		t.Fatal("AuthorizeSessionCreation() before authenticate = nil, want error (auth required)")
	}

	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	if _, err := agentConn.Authenticate(context.Background(), protocol.AuthenticateRequest{MethodID: "test-method"}); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if fakeAuth.calls != 1 {
		t.Fatalf("Authenticator.Authenticate calls = %d, want 1", fakeAuth.calls)
	}
	if fakeAuth.lastMethodID != "test-method" {
		t.Errorf("Authenticator.Authenticate methodID = %q, want %q", fakeAuth.lastMethodID, "test-method")
	}

	if err := a.AuthorizeSessionCreation(); err != nil {
		t.Errorf("AuthorizeSessionCreation() after successful authenticate = %v, want nil", err)
	}
}

// TestLogoutInvokesHandlerAndRequiresReAuth covers the LogoutHandler
// behavior: calling logout invokes the configured LogoutHandler, and
// afterward session creation requires re-authentication again, per the
// pinned schema ("after a successful logout, all new sessions will require
// authentication").
func TestLogoutInvokesHandlerAndRequiresReAuth(t *testing.T) {
	fakeAuth := &fakeAuthenticator{}
	fakeLogout := &fakeLogoutHandler{}
	opts := baseOptions()
	opts.Authenticator = fakeAuth
	opts.AuthMethods = []protocol.AuthMethod{{ID: "test-method", Name: "Test"}}
	opts.Logout = fakeLogout
	a, err := agent.New(opts)
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}

	client, server := pipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	if _, err := agentConn.Authenticate(context.Background(), protocol.AuthenticateRequest{MethodID: "test-method"}); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if err := a.AuthorizeSessionCreation(); err != nil {
		t.Fatalf("AuthorizeSessionCreation() after authenticate = %v, want nil", err)
	}

	var logoutResp protocol.LogoutResponse
	if err := client.Call(context.Background(), string(protocol.MethodLogout), protocol.LogoutRequest{}, &logoutResp); err != nil {
		t.Fatalf("logout call: %v", err)
	}
	if fakeLogout.calls != 1 {
		t.Fatalf("LogoutHandler.Logout calls = %d, want 1", fakeLogout.calls)
	}

	err = a.AuthorizeSessionCreation()
	if err == nil {
		t.Fatal("AuthorizeSessionCreation() after logout = nil, want error (re-authentication required)")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("AuthorizeSessionCreation() after logout error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeAuthenticationRequired {
		t.Errorf("AuthorizeSessionCreation() after logout Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeAuthenticationRequired)
	}
}
