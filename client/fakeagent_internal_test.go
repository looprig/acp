package client

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

// fakeAgent is a minimal, scriptable ACP agent built directly on
// protocol.Conn over one end of an in-process net.Pipe — the same
// construction mockpeer itself uses (protocol.NewConn + Conn.Handle/
// HandleNotify), but with per-test-overridable behavior instead of
// mockpeer's fixed script. It lets Task 5.1's session-lifecycle, prompt,
// cancel, replay-timeout, dedup, and capability-dispatch behaviors be
// exercised fast and deterministically without a real subprocess; the
// mockpeer subprocess itself is reserved (see client_integration_test.go)
// for genuinely process-boundary concerns: real spawning, and mockpeer's
// three fault-injection modes.
type fakeAgent struct {
	conn   *protocol.Conn
	client *protocol.ClientConn

	mu                sync.Mutex
	lastInitReq       protocol.InitializeRequest
	onInitialize      func(req protocol.InitializeRequest) (protocol.InitializeResponse, error)
	onNewSession      func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error)
	onLoadSession     func(ctx context.Context, fa *fakeAgent, req protocol.LoadSessionRequest) (protocol.LoadSessionResponse, error)
	onResume          func(req protocol.ResumeSessionRequest) (protocol.ResumeSessionResponse, error)
	onPrompt          func(ctx context.Context, fa *fakeAgent, req protocol.PromptRequest) (protocol.PromptResponse, error)
	onSetConfigOption func(req protocol.SetSessionConfigOptionRequest) (protocol.SetSessionConfigOptionResponse, error)
	onSetMode         func(req protocol.SetSessionModeRequest) (protocol.SetSessionModeResponse, error)
	onSetModel        func(req setModelRequest) (setModelResponse, error)
	cancelReceived    chan protocol.CancelNotification
}

// newFakeAgent wires default (successful, minimal) handlers for every
// agent-served method onto conn, which the caller has already built over its
// side of an in-process pipe. Every default may be overridden per test by
// setting the corresponding field before the Client dials.
func newFakeAgent(conn *protocol.Conn) *fakeAgent {
	fa := &fakeAgent{
		conn:           conn,
		client:         protocol.NewClientConn(conn),
		cancelReceived: make(chan protocol.CancelNotification, 8),
	}
	fa.onInitialize = func(req protocol.InitializeRequest) (protocol.InitializeResponse, error) {
		return protocol.InitializeResponse{ProtocolVersion: protocol.CurrentProtocolVersion}, nil
	}
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		return protocol.NewSessionResponse{SessionID: "fake-session-1"}, nil
	}
	fa.onLoadSession = func(ctx context.Context, fa *fakeAgent, req protocol.LoadSessionRequest) (protocol.LoadSessionResponse, error) {
		return protocol.LoadSessionResponse{}, nil
	}
	fa.onResume = func(req protocol.ResumeSessionRequest) (protocol.ResumeSessionResponse, error) {
		return protocol.ResumeSessionResponse{}, nil
	}
	fa.onPrompt = func(ctx context.Context, fa *fakeAgent, req protocol.PromptRequest) (protocol.PromptResponse, error) {
		return protocol.PromptResponse{StopReason: protocol.StopReasonEndTurn}, nil
	}
	fa.onSetConfigOption = func(req protocol.SetSessionConfigOptionRequest) (protocol.SetSessionConfigOptionResponse, error) {
		return protocol.SetSessionConfigOptionResponse{}, nil
	}
	fa.onSetMode = func(req protocol.SetSessionModeRequest) (protocol.SetSessionModeResponse, error) {
		return protocol.SetSessionModeResponse{}, nil
	}
	fa.onSetModel = func(req setModelRequest) (setModelResponse, error) {
		return setModelResponse{}, nil
	}

	conn.Handle(string(protocol.MethodInitialize), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
		var req protocol.InitializeRequest
		if len(params) > 0 {
			if err := json.Unmarshal(params, &req); err != nil {
				return nil, protocol.InvalidParams("initialize: decode", err)
			}
		}
		fa.mu.Lock()
		fa.lastInitReq = req
		handler := fa.onInitialize
		fa.mu.Unlock()
		return handler(req)
	})
	conn.Handle(string(protocol.MethodSessionNew), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
		var req protocol.NewSessionRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, protocol.InvalidParams("session/new: decode", err)
		}
		return fa.onNewSession(req)
	})
	conn.Handle(string(protocol.MethodSessionLoad), func(ctx context.Context, _ string, params json.RawMessage) (any, error) {
		var req protocol.LoadSessionRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, protocol.InvalidParams("session/load: decode", err)
		}
		return fa.onLoadSession(ctx, fa, req)
	})
	conn.Handle(string(protocol.MethodSessionResume), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
		var req protocol.ResumeSessionRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, protocol.InvalidParams("session/resume: decode", err)
		}
		return fa.onResume(req)
	})
	conn.Handle(string(protocol.MethodSessionPrompt), func(ctx context.Context, _ string, params json.RawMessage) (any, error) {
		var req protocol.PromptRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, protocol.InvalidParams("session/prompt: decode", err)
		}
		return fa.onPrompt(ctx, fa, req)
	})
	conn.HandleNotify(string(protocol.MethodSessionCancel), func(_ context.Context, _ string, params json.RawMessage) {
		var n protocol.CancelNotification
		if err := json.Unmarshal(params, &n); err != nil {
			return
		}
		fa.cancelReceived <- n
	})
	conn.Handle(string(protocol.MethodSessionSetConfigOption), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
		var req protocol.SetSessionConfigOptionRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, protocol.InvalidParams("session/set_config_option: decode", err)
		}
		return fa.onSetConfigOption(req)
	})
	conn.Handle(string(protocol.MethodSessionSetMode), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
		var req protocol.SetSessionModeRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, protocol.InvalidParams("session/set_mode: decode", err)
		}
		return fa.onSetMode(req)
	})
	conn.Handle(methodSessionSetModel, func(_ context.Context, _ string, params json.RawMessage) (any, error) {
		var req setModelRequest
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, protocol.InvalidParams("session/set_model: decode", err)
		}
		return fa.onSetModel(req)
	})

	return fa
}

// waitCancel blocks until fakeAgent observes a session/cancel notification,
// or fails the test after d.
func (fa *fakeAgent) waitCancel(t *testing.T, d time.Duration) protocol.CancelNotification {
	t.Helper()
	select {
	case n := <-fa.cancelReceived:
		return n
	case <-time.After(d):
		t.Fatal("timed out waiting for session/cancel notification")
		return protocol.CancelNotification{}
	}
}

// dialTestClient constructs a Client wired to a fresh in-process fakeAgent
// over a net.Pipe (no real subprocess) with the given Options, dials it, and
// registers cleanup. It returns both so a test can script fa's handlers
// before triggering Client calls that reach them.
//
// configure, if given, is applied to fa before dialing — the only way to
// script fa's onInitialize response (unlike every other handler, which a
// test can still safely reassign after dialing, onInitialize must already
// be in place before Dial's handshake happens). Tests that need a
// non-default initialize response (for example, one advertising a
// session/set_model _meta capability) pass a configure func; every existing
// caller passes none, so this is purely additive.
func dialTestClient(t *testing.T, opts Options, configure ...func(*fakeAgent)) (*Client, *fakeAgent) {
	t.Helper()
	agentSide, clientSide := net.Pipe()

	fa := newFakeAgent(protocol.NewConn(agentSide, agentSide, protocol.ConnOptions{}))
	for _, fn := range configure {
		fn(fa)
	}

	c := New(stdio.Command{}, opts)
	c.attemptConnect = func(ctx context.Context) error {
		return c.finishConnect(ctx, nil, clientSide, clientSide)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Dial(ctx); err != nil {
		t.Fatalf("Dial() over fake agent: %v", err)
	}
	return c, fa
}
