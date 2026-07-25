package agent_test

// gates_test.go is the wire-level counterpart to gates_internal_test.go
// (package agent, white-box): it proves the permission-gate round trip
// (gates.go, Task 2.6 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md) is actually
// wired into drainToTerminal's real event drain and actually issues
// session/request_permission over a real protocol.Conn, rather than only
// exercising the pure functions in isolation.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	coreuuid "github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
)

// newGateTestAgent mirrors prompt_test.go's newPromptTestAgent, but also
// returns the raw client-side *protocol.Conn: these tests need to register a
// handler for session/request_permission, a call the AGENT makes TO the
// client, which protocol.AgentConn's typed surface (client-calls-agent only)
// cannot do.
func newGateTestAgent(t *testing.T, fake *fakeLiveSession) (agentConn *protocol.AgentConn, client *protocol.Conn, sessionID protocol.SessionID) {
	t.Helper()
	host := &promptHostStub{session: fake}
	a, err := agent.New(agent.Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	var server *protocol.Conn
	client, server = pipeConns(t)
	a.Register(server)
	agentConn = protocol.NewAgentConn(client)

	resp, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	return agentConn, client, resp.SessionID
}

// permissionGateOpened builds the event.GateOpened a real
// internal/loopruntime.permissionGate would produce for hdr's turn: a
// translatable gate.KindPermission gate offering the exact three approval
// controls.
func permissionGateOpened(t *testing.T, hdr event.Header) (event.GateOpened, coreuuid.UUID) {
	t.Helper()
	gateID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	toolExecID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	return event.GateOpened{
		Header: hdr,
		Gate: gate.Gate{
			ID:       gateID,
			Kind:     gate.KindPermission,
			Resolver: gate.ResolverLoop,
			Subject:  gate.Subject{ToolExecutionID: toolExecID},
			Prompt: gate.Prompt{
				Title:    "Approve tool call",
				Body:     "bash wants to run `rm -rf /tmp/scratch`",
				Controls: gate.ApprovalControls(),
			},
		},
	}, gateID
}

// --- option-set fidelity over the real wire, both directions -------------

func TestGateRequestPermissionOptionFidelityOverTheWire(t *testing.T) {
	tests := []struct {
		name   string
		action gate.ApprovalAction
	}{
		{"approve", gate.ApprovalApprove},
		{"approve always for this workspace", gate.ApprovalApproveAlwaysWorkspace},
		{"deny", gate.ApprovalDeny},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeLiveSession(t)
			agentConn, client, sessionID := newGateTestAgent(t, fake)

			var gotReq protocol.RequestPermissionRequest
			client.Handle(string(protocol.MethodSessionRequestPermission), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
				if err := json.Unmarshal(params, &gotReq); err != nil {
					t.Errorf("unmarshal session/request_permission params: %v", err)
				}
				return protocol.RequestPermissionResponse{
					Outcome: protocol.RequestPermissionOutcome{
						Selected: &protocol.SelectedPermissionOutcome{OptionID: protocol.PermissionOptionID(tt.action)},
					},
				}, nil
			})

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

			opened, gateID := permissionGateOpened(t, hdr)
			send(t, fake.events, opened)

			// fake.events is unbuffered: this send cannot complete until
			// drainToTerminal has looped back to receive again, which only
			// happens once the GateOpened round trip above — including the
			// RequestPermission call and RespondGate — has fully run. No
			// polling wait is needed for that ordering.
			send(t, fake.events, event.TurnDone{Header: hdr})

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

			responses := fake.gateResponses()
			if len(responses) != 1 {
				t.Fatalf("RespondGate calls = %d, want 1", len(responses))
			}
			if responses[0].GateID != gateID {
				t.Errorf("RespondGate GateID = %v, want %v", responses[0].GateID, gateID)
			}
			if responses[0].Action != string(tt.action) {
				t.Errorf("RespondGate Action = %q, want %q", responses[0].Action, tt.action)
			}

			if gotReq.SessionID != sessionID {
				t.Errorf("RequestPermissionRequest.SessionID = %q, want %q", gotReq.SessionID, sessionID)
			}
			if len(gotReq.Options) != 3 {
				t.Fatalf("RequestPermissionRequest.Options len = %d, want 3", len(gotReq.Options))
			}
			for _, o := range gotReq.Options {
				if o.OptionID == protocol.PermissionOptionID(tt.action) && o.Kind == "" {
					t.Errorf("offered option %q has no Kind hint", o.OptionID)
				}
			}
			if gotReq.ToolCall.ToolCallID == "" {
				t.Error("RequestPermissionRequest.ToolCall.ToolCallID is empty")
			}
		})
	}
}

// --- an option the client was never offered: typed error, gate left open --

func TestGateRequestPermissionUnofferedOptionLeavesGateOpen(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, client, sessionID := newGateTestAgent(t, fake)

	client.Handle(string(protocol.MethodSessionRequestPermission), func(context.Context, string, json.RawMessage) (any, error) {
		return protocol.RequestPermissionResponse{
			Outcome: protocol.RequestPermissionOutcome{
				Selected: &protocol.SelectedPermissionOutcome{OptionID: "not-a-real-option-id"},
			},
		}, nil
	})

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

	opened, _ := permissionGateOpened(t, hdr)
	send(t, fake.events, opened)

	// No TurnDone is ever sent: an unoffered selection must abort the whole
	// drain with a typed error immediately, exactly like a failed
	// session/update send already does (prompt.go), never wait for a
	// terminal that RespondGate was never told to help produce.
	select {
	case r := <-done:
		if r.err == nil {
			t.Fatal("Prompt: error = nil, want a typed error for an unoffered permission option")
		}
		if r.resp != nil {
			t.Errorf("Prompt: resp = %+v, want nil on failure", r.resp)
		}
		var f *protocol.Fault
		if !errors.As(r.err, &f) {
			t.Fatalf("Prompt error = %v (%T), want *protocol.Fault", r.err, r.err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt to return")
	}

	if got := fake.gateResponses(); len(got) != 0 {
		t.Fatalf("RespondGate calls = %d, want 0 (the gate must be left open, not answered on an untrusted selection)", len(got))
	}
}

// --- connection death mid-round-trip: fail closed, never left open, never
// approved -------------------------------------------------------------

func TestGateRequestPermissionConnectionDeathFailsClosed(t *testing.T) {
	fake := newFakeLiveSession(t)
	agentConn, client, sessionID := newGateTestAgent(t, fake)

	reached := make(chan struct{}, 1)
	proceed := make(chan protocol.RequestPermissionResponse, 1)
	// The handler goroutine below blocks on proceed for as long as the test
	// runs (that block is the point: it simulates a request that never gets
	// an answer because the connection dies first). Closing proceed at
	// cleanup unblocks it deterministically instead of leaking it — see
	// protocol.Conn.Close's own doc: Close never waits for a running handler
	// callback, so nothing here relies on that goroutine exiting promptly.
	t.Cleanup(func() { close(proceed) })
	client.Handle(string(protocol.MethodSessionRequestPermission), func(context.Context, string, json.RawMessage) (any, error) {
		select {
		case reached <- struct{}{}:
		default:
		}
		resp, ok := <-proceed
		if !ok {
			return nil, errors.New("test: client handler channel closed without ever answering")
		}
		return resp, nil
	})

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

	opened, gateID := permissionGateOpened(t, hdr)
	send(t, fake.events, opened)

	select {
	case <-reached:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the client's session/request_permission handler to be reached")
	}

	// Simulate connection death: closing the client side causes the
	// server's read loop (the agent's side, which issued the in-flight
	// RequestPermission call) to observe the transport gone and fail every
	// one of its own pending calls with *protocol.ConnClosedError — see
	// protocol.Conn.readLoop's doc ("any read failure ... means this Conn
	// can make no further progress: treat it exactly like an explicit
	// Close").
	if err := client.Close(); err != nil {
		t.Fatalf("client.Close: %v", err)
	}

	select {
	case r := <-done:
		// The exact error shape is not asserted here: the client side's own
		// Close() resolves the OUTER session/prompt call's pending entry
		// immediately (a raw *protocol.ConnClosedError, per Conn.Close's own
		// documented contract), which typically wins the race against the
		// SERVER side's separate goroutine noticing the same failure and
		// building drainToTerminal's own typed Fault. Either way, an error
		// here is the only acceptable outcome — never a success.
		if r.err == nil {
			t.Fatal("Prompt: error = nil, want an error after connection death mid-gate")
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Prompt to return after connection death")
	}

	// The real fail-closed guarantee under test is this: regardless of what
	// the OUTER call observed, the agent's own drainToTerminal goroutine
	// (server side) must independently notice its RequestPermission call
	// failed and answer the gate Deny. That goroutine is not synchronized
	// with the done channel above, so poll for it rather than assuming it
	// has already run.
	deadline := time.Now().Add(testTimeout)
	for len(fake.gateResponses()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	responses := fake.gateResponses()
	if len(responses) != 1 {
		t.Fatalf("RespondGate calls = %d, want exactly 1 (fail-closed deny)", len(responses))
	}
	if responses[0].GateID != gateID {
		t.Errorf("RespondGate GateID = %v, want %v", responses[0].GateID, gateID)
	}
	if responses[0].Action != string(gate.ApprovalDeny) {
		t.Errorf("RespondGate Action = %q, want %q (fail closed: never left open, never approved)", responses[0].Action, gate.ApprovalDeny)
	}
}

// --- matrix: only a translatable permission gate is ever turned into a
// session/request_permission call; ask-user and host-owned form/open-url
// gates are not, and never block the drain either -------------------------

func TestGateOpenedKindMatrixOnlyPermissionIsExposed(t *testing.T) {
	toolExecID := func(t *testing.T) coreuuid.UUID {
		t.Helper()
		id, err := coreuuid.New()
		if err != nil {
			t.Fatalf("uuid.New: %v", err)
		}
		return id
	}

	tests := []struct {
		name        string
		gate        func(t *testing.T) gate.Gate
		wantExposed bool
	}{
		{
			name: "permission gate is exposed as session/request_permission",
			gate: func(t *testing.T) gate.Gate {
				return gate.Gate{
					Kind:     gate.KindPermission,
					Resolver: gate.ResolverLoop,
					Subject:  gate.Subject{ToolExecutionID: toolExecID(t)},
					Prompt:   gate.Prompt{Controls: gate.ApprovalControls()},
				}
			},
			wantExposed: true,
		},
		{
			name: "ask-user gate is not exposed (no ACP outcome field for its answer)",
			gate: func(t *testing.T) gate.Gate {
				return gate.Gate{
					Kind:     gate.KindAskUser,
					Resolver: gate.ResolverLoop,
					Subject:  gate.Subject{ToolExecutionID: toolExecID(t)},
					Prompt: gate.Prompt{
						Controls: []gate.Control{{Action: "answer", Label: "Answer"}},
					},
				}
			},
			wantExposed: false,
		},
		{
			name: "host-owned form gate is not exposed (no elicitation method in the pinned schema)",
			gate: func(t *testing.T) gate.Gate {
				return gate.Gate{
					Kind:     gate.KindForm,
					Resolver: gate.ResolverSession,
					Prompt: gate.Prompt{
						Controls: []gate.Control{{Action: "accept", Label: "Accept"}, {Action: "decline", Label: "Decline"}},
					},
				}
			},
			wantExposed: false,
		},
		{
			name: "host-owned open-url gate is not exposed (no faithful ACP interaction for it)",
			gate: func(t *testing.T) gate.Gate {
				return gate.Gate{
					Kind:     gate.KindOpenURL,
					Resolver: gate.ResolverSession,
					Prompt: gate.Prompt{
						Origin:   "https://example.invalid",
						Controls: []gate.Control{{Action: "accept", Label: "I opened it"}},
					},
				}
			},
			wantExposed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFakeLiveSession(t)
			agentConn, client, sessionID := newGateTestAgent(t, fake)

			requestPermissionCalls := make(chan struct{}, 1)
			client.Handle(string(protocol.MethodSessionRequestPermission), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
				requestPermissionCalls <- struct{}{}
				var req protocol.RequestPermissionRequest
				_ = json.Unmarshal(params, &req)
				return protocol.RequestPermissionResponse{
					Outcome: protocol.RequestPermissionOutcome{Selected: &protocol.SelectedPermissionOutcome{OptionID: req.Options[0].OptionID}},
				}, nil
			})

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
			send(t, fake.events, event.GateOpened{Header: hdr, Gate: tt.gate(t)})

			if tt.wantExposed {
				select {
				case <-requestPermissionCalls:
				case <-time.After(testTimeout):
					t.Fatal("timed out waiting for session/request_permission to be issued")
				}
			}

			// Either way, the drain must still reach the turn's terminal:
			// an unexposed gate kind must never wedge the prompt.
			send(t, fake.events, event.TurnDone{Header: hdr})

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

			if !tt.wantExposed {
				select {
				case <-requestPermissionCalls:
					t.Fatal("session/request_permission was issued for a gate kind that must never be exposed this way")
				default:
				}
				if got := fake.gateResponses(); len(got) != 0 {
					t.Errorf("RespondGate calls = %d, want 0 for an unexposed gate kind", len(got))
				}
			}
		})
	}
}
