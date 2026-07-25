// agent.go defines the Agent facade: the struct that binds Options to real
// ACP wire methods over a *protocol.Conn (see conn.go's Handle/HandleNotify).
// This file wires only initialize, authenticate, and logout — the methods
// whose behavior does not depend on a live session (session/new and
// everything after it are later tasks; see the phase plan in
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md).
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/looprig/acp/protocol"
)

// Options configures a facade Agent. Host is the one required field; every
// other field is an optional capability interface from host.go, and nil
// means the corresponding ACP capability is unsupported (see capabilities.go
// for exactly how each maps onto the initialize response, and Register for
// which wire methods are gated on which field).
type Options struct {
	// Host is the required session factory the facade calls through for
	// session/new, session/load, and session/resume once those are wired
	// (Task 2.3 onward). It carries no capability gating of its own: every
	// product-facing agent needs it.
	Host SessionHost

	// Replayer, when supplied, backs the loadSession capability and
	// session/load (see EventReplayer; wired starting Task 3.1).
	Replayer EventReplayer
	// Catalog, when supplied, backs the session/list capability (see
	// SessionCatalog; wired starting Task 3.3).
	Catalog SessionCatalog
	// ConfigCatalog, when supplied, lets the facade enumerate a session's
	// available runtime configuration options (see RuntimeConfigCatalog;
	// wired starting Task 4.1). It has no initialize-level wire
	// representation: config options are surfaced per-session in the
	// session/new response.
	ConfigCatalog RuntimeConfigCatalog
	// ConfigController, when supplied, backs session/set_config_option and
	// session/set_mode (see RuntimeConfigController; wired starting Task
	// 4.1).
	ConfigController RuntimeConfigController
	// Compactor, when supplied, lets the facade run the `/compact` slash
	// command (see Compactor; wired starting Task 4.2). It has no
	// initialize-level wire representation: it is advertised as a
	// session-level available command.
	Compactor Compactor
	// Deleter, when supplied, backs the session/delete capability (see
	// SessionDeleter; wired starting Task 3.4).
	Deleter SessionDeleter

	// Authenticator, when supplied, backs the authenticate method. It is
	// meaningless without at least one entry in AuthMethods — a client
	// could never select a method id to authenticate with — so New rejects
	// that combination (see ErrAuthenticatorWithoutMethods).
	Authenticator Authenticator
	// AuthMethods is the set of authentication methods advertised in the
	// initialize response's authMethods field. It is only meaningful when
	// Authenticator is supplied, and must be non-empty in that case.
	AuthMethods []protocol.AuthMethod

	// Logout, when supplied, backs the logout method (see LogoutHandler).
	Logout LogoutHandler
}

// ErrMissingHost reports that Options did not supply a SessionHost, the one
// field every facade requires.
var ErrMissingHost = errors.New("agent: Options.Host is required")

// ErrAuthenticatorWithoutMethods reports that Options supplied an
// Authenticator but no AuthMethods to advertise it under. Advertising the
// authenticate capability with no selectable method id can never succeed
// from a client's perspective, so New fails closed at construction rather
// than accepting a configuration that could never work.
var ErrAuthenticatorWithoutMethods = errors.New("agent: Authenticator supplied without any AuthMethods to advertise")

// Agent is the ACP-facing facade: it registers ACP wire handlers on a
// *protocol.Conn (via Register) and consults Options to decide, per the
// pinned schema, which optional capabilities to advertise and allow.
//
// Its zero value is not usable; construct with New.
type Agent struct {
	opts Options

	authMu        sync.Mutex
	authenticated bool

	// capsMu guards clientCaps, the negotiated client capability set
	// recorded at initialize (see handleInitialize) and read back by
	// handleSessionNew when constructing each session's Setup. It defaults
	// to the full schema-declared default set so a session created before
	// any initialize call (which should not happen over a conforming
	// client, but is not this facade's to assume) still gets a valid,
	// conservative Setup rather than a zero value.
	capsMu     sync.Mutex
	clientCaps protocol.ClientCapabilities

	// sessions is the bounded registry of live ACP sessions keyed by their
	// Harness session UUID (see session.go's resolveSession and
	// handleSessionNew, and registry.go's sessionRegistry).
	sessions *sessionRegistry

	// prompts enforces "at most one session/prompt in flight per ACP
	// session" (see prompt.go's promptTracker).
	prompts *promptTracker

	// gates tracks permission-gate round trips currently in flight per ACP
	// session (see gates.go's gateTracker). It is the integration point
	// Task 2.7's session/close orchestration uses to force any gate still
	// open for a closing session.
	gates *gateTracker

	// client is the agent-calls-client method surface bound to the same Conn
	// Register was given. handlePrompt's drain loop (prompt.go) uses it to
	// send session/update notifications for the live events it observes on
	// the way to a turn's terminal (see translate.go's liveTranslator). It is
	// nil until Register runs, which happens before any wire traffic can
	// reach a handler.
	client *protocol.ClientConn
}

// New validates opts and constructs the facade.
//
// authenticated state starts unlocked when no Authenticator is configured
// (nothing ever gates on it), and locked when one is configured, matching
// the pinned schema's authenticate flow: "called when the agent requires
// authentication before allowing session creation."
func New(opts Options) (*Agent, error) {
	if opts.Host == nil {
		return nil, ErrMissingHost
	}
	if opts.Authenticator != nil && len(opts.AuthMethods) == 0 {
		return nil, ErrAuthenticatorWithoutMethods
	}
	return &Agent{
		opts:          opts,
		authenticated: opts.Authenticator == nil,
		clientCaps:    protocol.DefaultClientCapabilities(),
		sessions:      newSessionRegistry(MaxLiveSessions),
		prompts:       newPromptTracker(),
		gates:         newGateTracker(),
	}, nil
}

// Register binds the facade's currently implemented handlers onto conn:
// initialize, session/new, session/prompt, session/cancel, and session/close
// unconditionally (every product-facing agent needs Options.Host, so none of
// these have a capability gate of their own — session/new, session/prompt,
// and session/close each consult AuthorizeSessionCreation/resolveSession
// internally), and authenticate/logout only when their backing Options field
// is supplied. Every other ACP method (session/load, and later) is
// intentionally left unregistered here — Conn's own method-not-found
// fallback rejects them (see conn.go's dispatchRequest) until a later task
// wires them up, which is exactly the fail-closed behavior an unadvertised
// capability must have.
func (a *Agent) Register(conn *protocol.Conn) {
	a.client = protocol.NewClientConn(conn)
	conn.Handle(string(protocol.MethodInitialize), a.handleInitialize)
	conn.Handle(string(protocol.MethodSessionNew), a.handleSessionNew)
	conn.Handle(string(protocol.MethodSessionPrompt), a.handlePrompt)
	conn.HandleNotify(string(protocol.MethodSessionCancel), a.handleSessionCancel)
	conn.Handle(string(protocol.MethodSessionClose), a.handleSessionClose)
	if a.opts.Authenticator != nil {
		conn.Handle(string(protocol.MethodAuthenticate), a.handleAuthenticate)
	}
	if a.opts.Logout != nil {
		conn.Handle(string(protocol.MethodLogout), a.handleLogout)
	}
}

// AuthorizeSessionCreation reports whether session-creation methods
// (session/new, session/load, session/resume — wired starting Task 2.3)
// may proceed right now. Per the pinned schema's authenticate/logout flow —
// session/new's own doc says it "may return an auth_required error if the
// agent requires authentication," and logout's says "after a successful
// logout, all new sessions will require authentication" — this returns nil
// when no Authenticator is configured (authentication is never required) or
// once Authenticate has since succeeded, and a *protocol.Fault with
// ErrorCodeAuthenticationRequired otherwise. Callers implementing those
// methods must consult this before touching the host.
func (a *Agent) AuthorizeSessionCreation() error {
	if a.opts.Authenticator == nil {
		return nil
	}
	a.authMu.Lock()
	defer a.authMu.Unlock()
	if a.authenticated {
		return nil
	}
	return protocol.AuthRequired("authentication is required before session creation", nil)
}

func (a *Agent) setAuthenticated(v bool) {
	a.authMu.Lock()
	a.authenticated = v
	a.authMu.Unlock()
}

// handleInitialize answers the initialize method with the negotiated
// protocol version and the capability matrix computed from Options (see
// capabilities.go). This module speaks exactly protocol version 1, so the
// response always names it; a client that cannot speak it is expected to
// disconnect, per the pinned schema.
//
// It also records the client's negotiated capability set (with every
// schema-declared default applied to any subfield the client did not
// advertise) for handleSessionNew to read back into each session's Setup:
// client capabilities are negotiated once for the connection, not re-sent
// per session/new request (see NewSessionRequest in types_gen.go, which
// carries no client-capability field of its own).
func (a *Agent) handleInitialize(_ context.Context, _ string, params json.RawMessage) (any, error) {
	var req protocol.InitializeRequest
	if len(params) > 0 {
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, protocol.InvalidParams("initialize: decode params", err)
		}
	}
	a.setClientCapabilities(defaultedClientCapabilities(req.ClientCapabilities))

	caps := a.capabilities()
	return protocol.InitializeResponse{
		ProtocolVersion:   protocol.CurrentProtocolVersion,
		AgentCapabilities: &caps,
		AuthMethods:       a.opts.AuthMethods,
	}, nil
}

// setClientCapabilities records c as the connection's negotiated client
// capability set.
func (a *Agent) setClientCapabilities(c protocol.ClientCapabilities) {
	a.capsMu.Lock()
	a.clientCaps = c
	a.capsMu.Unlock()
}

// negotiatedClientCapabilities returns the client capability set recorded by
// the most recent initialize call (or the full schema-declared defaults, if
// initialize has not yet run).
func (a *Agent) negotiatedClientCapabilities() protocol.ClientCapabilities {
	a.capsMu.Lock()
	defer a.capsMu.Unlock()
	return a.clientCaps
}

// handleAuthenticate answers the authenticate method. It is only ever
// registered when Options.Authenticator is non-nil (see Register), so it can
// assume that capability is present. The requested method id is validated
// against the advertised AuthMethods before the Authenticator is ever
// invoked, matching host.go's Authenticator contract ("the facade validates
// methodID against the advertised authMethods before calling this").
func (a *Agent) handleAuthenticate(ctx context.Context, _ string, params json.RawMessage) (any, error) {
	var req protocol.AuthenticateRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("authenticate: decode params", err)
	}
	if !a.authMethodAdvertised(req.MethodID) {
		return nil, protocol.InvalidParams("authenticate: method id not advertised in initialize", nil)
	}

	if err := a.opts.Authenticator.Authenticate(ctx, req.MethodID); err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("authenticate: "+err.Error(), err)
	}

	a.setAuthenticated(true)
	return protocol.AuthenticateResponse{}, nil
}

// handleLogout answers the logout method. It is only ever registered when
// Options.Logout is non-nil (see Register). A successful logout re-locks
// session creation exactly like the pre-authenticate state, matching the
// pinned schema: "after a successful logout, all new sessions will require
// authentication."
func (a *Agent) handleLogout(ctx context.Context, _ string, _ json.RawMessage) (any, error) {
	if err := a.opts.Logout.Logout(ctx); err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("logout: "+err.Error(), err)
	}

	a.setAuthenticated(false)
	return protocol.LogoutResponse{}, nil
}

// authMethodAdvertised reports whether id matches one of the AuthMethods
// supplied in Options — the set the initialize response actually
// advertised.
func (a *Agent) authMethodAdvertised(id protocol.AuthMethodID) bool {
	for _, m := range a.opts.AuthMethods {
		if m.ID == id {
			return true
		}
	}
	return false
}
