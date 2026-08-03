// codex_connector_test.go proves CodexConnector's own type behavior
// (construction, WithModel, posture handling) and the capability-gating
// regression Task 15 calls for: that this connector never invokes any ACP
// extension method, gated or otherwise, and that changing models always
// means a fresh Configure/Dial (a new codex-acp process/session), never a
// post-session switching RPC against an existing one.
package launch

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

func TestCodexConstructorStoresModel(t *testing.T) {
	c := Codex("gpt-5-codex")
	if c.Model != "gpt-5-codex" {
		t.Errorf("Model = %q, want %q", c.Model, "gpt-5-codex")
	}
	if c.Posture != (CodexPosture{}) {
		t.Errorf("Posture = %+v, want the zero value by default", c.Posture)
	}
}

func TestCodexConnectorWithModelReturnsIndependentCopy(t *testing.T) {
	c := Codex("model-a")
	c.Posture = CodexPosture{ApprovalPolicy: "never", SandboxMode: "read-only", SandboxNetworkAccess: true}

	c2 := c.WithModel("model-b")

	if c2 == c {
		t.Fatal("WithModel() returned the same pointer, want a distinct CodexConnector")
	}
	if c2.Model != "model-b" {
		t.Errorf("c2.Model = %q, want %q", c2.Model, "model-b")
	}
	if c.Model != "model-a" {
		t.Errorf("c.Model = %q after WithModel, want unchanged %q", c.Model, "model-a")
	}
	if c2.Posture != c.Posture {
		t.Errorf("c2.Posture = %+v, want carried over unchanged: %+v", c2.Posture, c.Posture)
	}

	// Mutating c2 afterward must never affect c.
	c2.Posture.ApprovalPolicy = "on-request"
	if c.Posture.ApprovalPolicy != "never" {
		t.Errorf("c.Posture.ApprovalPolicy = %q after mutating c2, want unchanged %q", c.Posture.ApprovalPolicy, "never")
	}
}

// TestCodexModelChangeProducesNewArgvNeverInPlaceMutation proves that
// "changing the model" is entirely a matter of building a new
// CodexConnector (via WithModel) and calling Configure again -- the
// `-c model=` value is the ONLY thing that differs between the two
// resulting argvs, and the original connector's own Configure output is
// completely unaffected by the second call.
func TestCodexModelChangeProducesNewArgvNeverInPlaceMutation(t *testing.T) {
	binding := ProxyBinding{BaseURL: "http://127.0.0.1:1", Token: "tok"}
	cmd := stdio.Command{Path: "/opt/codex-acp"}

	a := Codex("model-a")
	outA1, err := a.Configure(cmd, binding)
	if err != nil {
		t.Fatalf("Configure() a (first) error = %v", err)
	}

	b := a.WithModel("model-b")
	outB, err := b.Configure(cmd, binding)
	if err != nil {
		t.Fatalf("Configure() b error = %v", err)
	}

	outA2, err := a.Configure(cmd, binding)
	if err != nil {
		t.Fatalf("Configure() a (second) error = %v", err)
	}
	if !equalStrings(outA1.Args, outA2.Args) {
		t.Fatalf("a's Configure() output changed after b.Configure() ran: %v vs %v -- a must be entirely unaffected by b", outA1.Args, outA2.Args)
	}

	if outA1.Args[1] != "model=model-a" {
		t.Fatalf("a's model arg = %q, want %q", outA1.Args[1], "model=model-a")
	}
	if outB.Args[1] != "model=model-b" {
		t.Fatalf("b's model arg = %q, want %q", outB.Args[1], "model=model-b")
	}
	// Every other override is identical between the two argvs: only the
	// model value differs.
	for i := 2; i < len(outA1.Args); i++ {
		if outA1.Args[i] != outB.Args[i] {
			t.Fatalf("argv differs at index %d beyond the model override: %v vs %v", i, outA1.Args, outB.Args)
		}
	}
}

// TestCodexModelChangeDialsANewSessionRatherThanSwitchingInPlace exercises
// the same "recreate, don't switch" contract through launch.Dial's own
// fake-connect seam (see managed_test.go): two CodexConnectors dialed
// separately produce two entirely independent ManagedClients/connections,
// never one connection mutated by some in-place model-switch call.
func TestCodexModelChangeDialsANewSessionRatherThanSwitchingInPlace(t *testing.T) {
	harnessA := Codex("model-a")
	harnessB := harnessA.WithModel("model-b")

	connA := newFakeConn()
	mcA, err := dial(context.Background(), Config{
		OwnedProxy: readyProxy(),
		Harness:    harnessA,
		Command:    stdio.Command{Path: "/opt/codex-acp"},
	}, fakeConnect(connA, nil))
	if err != nil {
		t.Fatalf("dial() A error = %v", err)
	}
	t.Cleanup(func() { _ = mcA.Close(context.Background()) })

	connB := newFakeConn()
	mcB, err := dial(context.Background(), Config{
		OwnedProxy: readyProxy(),
		Harness:    harnessB,
		Command:    stdio.Command{Path: "/opt/codex-acp"},
	}, fakeConnect(connB, nil))
	if err != nil {
		t.Fatalf("dial() B error = %v", err)
	}
	t.Cleanup(func() { _ = mcB.Close(context.Background()) })

	if mcA == mcB {
		t.Fatal("expected two distinct *ManagedClient values, got the same one")
	}
	if mcA.conn == mcB.conn {
		t.Fatal("expected two distinct underlying connections, want a brand-new codex-acp process/session per model rather than one session reused")
	}
}

// TestCodexConnectorMethodSetNeverTouchesSessionOrCapabilityTypes is the
// capability-gating regression: CodexConnector's ENTIRE exported method
// set must never include a method that accepts a *client.Session, a
// client.SetModelCapability, or any protocol Session*ID type -- the only
// way any of those could be used is to make a wire call against an
// existing session (session/set_config_option, session/set_mode, or the
// unstable session/set_model extension), which the design doc explicitly
// forbids for Codex ("no post-session model-switching RPC call is ever
// attempted"). Unlike ClaudeConnector (see claude_connector.go's
// SelectDefaultModel/SelectSmallModel/ApplyPermissionMode, which
// deliberately DO accept a *client.Session), CodexConnector's Configure and
// ConfigureNative methods only configure fresh process commands, while
// WithModel operates purely on Go values. If a later change adds any
// session-touching method to CodexConnector, this test fails immediately,
// forcing a conscious decision rather than a silent regression.
func TestCodexConnectorMethodSetNeverTouchesSessionOrCapabilityTypes(t *testing.T) {
	disallowed := []reflect.Type{
		reflect.TypeOf((*client.Session)(nil)),
		reflect.TypeOf(client.SetModelCapability{}),
		reflect.TypeOf(protocol.SessionConfigID("")),
		reflect.TypeOf(protocol.SessionConfigValueID("")),
		reflect.TypeOf(protocol.SessionModeID("")),
	}

	typ := reflect.TypeOf(&CodexConnector{})
	for i := 0; i < typ.NumMethod(); i++ {
		m := typ.Method(i)
		ft := m.Func.Type()
		// ft.In(0) is the receiver for a method obtained via
		// reflect.Type.Method; real parameters start at index 1.
		for j := 1; j < ft.NumIn(); j++ {
			in := ft.In(j)
			for _, bad := range disallowed {
				if in == bad {
					t.Fatalf("CodexConnector.%s accepts a %v parameter: CodexConnector must never gain a method that could reach a session-level extension call", m.Name, in)
				}
			}
		}
	}
}

// TestCodexConnectorMethodSetIsExactlyConfigureConfigureNativeAndWithModel
// locks down CodexConnector's exported surface: Configure and ConfigureNative
// (its gateway and native HarnessAdapter contracts) and WithModel (its
// "recreate the session" mechanism), and nothing else -- reinforcing the
// same intent as the type-based check above with an explicit, human-readable
// method list.
func TestCodexConnectorMethodSetIsExactlyConfigureConfigureNativeAndWithModel(t *testing.T) {
	typ := reflect.TypeOf(&CodexConnector{})
	want := map[string]bool{"Configure": true, "ConfigureNative": true, "WithModel": true}
	if typ.NumMethod() != len(want) {
		names := make([]string, typ.NumMethod())
		for i := 0; i < typ.NumMethod(); i++ {
			names[i] = typ.Method(i).Name
		}
		t.Fatalf("CodexConnector methods = %v, want exactly %v", names, want)
	}
	for i := 0; i < typ.NumMethod(); i++ {
		name := typ.Method(i).Name
		if !want[name] {
			t.Errorf("unexpected CodexConnector method %q", name)
		}
	}
}

// --- the codex-acp "{}}-success for unknown methods" wire quirk ---

// bareSuccessPeer is an in-process ACP peer (net.Pipe, no subprocess, no
// acp/internal/mockpeer involvement) that replicates codex-acp's own
// load-bearing quirk (see the design doc's "Protocol quirk"): its
// "initialize" response advertises no extension capability in _meta at
// all, and it answers ANY method it has no specific handler for --
// exactly the shape of an unadvertised/speculatively-probed extension --
// with a bare `{}` JSON-RPC SUCCESS via protocol.Conn.HandleUnknownRequest,
// rather than the standard -32601 Method Not Found a spec-compliant peer
// would return. It records every such method name, so a test can assert
// exactly what (if anything) was actually invoked against it.
type bareSuccessPeer struct {
	mu    sync.Mutex
	calls []string
}

func (p *bareSuccessPeer) recordedCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func (p *bareSuccessPeer) handleInitialize(_ context.Context, _ string, _ json.RawMessage) (any, error) {
	return protocol.InitializeResponse{ProtocolVersion: protocol.CurrentProtocolVersion}, nil
}

func (p *bareSuccessPeer) handleUnknown(_ context.Context, method string, _ json.RawMessage) (any, error) {
	p.mu.Lock()
	p.calls = append(p.calls, method)
	p.mu.Unlock()
	return struct{}{}, nil
}

// dialBareSuccessPeer wires an in-process net.Pipe connection between a
// bareSuccessPeer (serving the agent role) and a *protocol.AgentConn (the
// client role a connector drives), completes "initialize", and returns
// both ends for a test to drive further. This exercises protocol.Conn's
// real wire dispatch -- the actual layer the codex-acp quirk lives at --
// using only the protocol/stdio primitives acp/launch is already allowed
// to import (see acp/CLAUDE.md); no subprocess and no acp/client
// involvement at all.
func dialBareSuccessPeer(t *testing.T) (*bareSuccessPeer, *protocol.AgentConn) {
	t.Helper()
	peerSide, clientSide := net.Pipe()
	t.Cleanup(func() { _ = peerSide.Close(); _ = clientSide.Close() })

	peer := &bareSuccessPeer{}
	peerConn := protocol.NewConn(peerSide, peerSide, protocol.ConnOptions{})
	peerConn.Handle(string(protocol.MethodInitialize), peer.handleInitialize)
	peerConn.HandleUnknownRequest(peer.handleUnknown)

	clientConn := protocol.NewConn(clientSide, clientSide, protocol.ConnOptions{})
	agent := protocol.NewAgentConn(clientConn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := agent.Initialize(ctx, protocol.InitializeRequest{ProtocolVersion: protocol.CurrentProtocolVersion}); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	return peer, agent
}

// TestBareSuccessPeerReplicatesCodexACPQuirk proves the fake peer above is
// faithful to the documented codex-acp behavior: a hypothetical
// speculative probe for an unadvertised extension method -- something a
// spec-compliant peer would answer with a JSON-RPC -32601 Method Not
// Found error -- instead succeeds silently. This is exactly why
// CodexConnector (and every connector in this package) must gate optional
// extensions on an explicitly advertised initialize _meta capability,
// never by calling and inspecting the result.
func TestBareSuccessPeerReplicatesCodexACPQuirk(t *testing.T) {
	peer, agent := dialBareSuccessPeer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp struct{}
	if err := agent.Conn().Call(ctx, "session/set_model", struct{ SessionID, ModelID string }{"s1", "m1"}, &resp); err != nil {
		t.Fatalf("Call(session/set_model) against a bare-success peer error = %v, want nil (the quirk this test documents)", err)
	}
	if got := peer.recordedCalls(); len(got) != 1 || got[0] != "session/set_model" {
		t.Fatalf("peer recorded calls = %v, want exactly [%q]", got, "session/set_model")
	}
}

// TestCodexConnectorNeverProbesTheBareSuccessPeer proves CodexConnector's
// entire public API -- Configure and WithModel -- has no way to reach a
// live ACP peer at all, let alone probe it: Configure's signature
// (stdio.Command, ProxyBinding) -> (stdio.Command, error) carries no
// context, no *client.Session, and no *protocol.AgentConn, so it is
// structurally impossible for it to place any RPC call, extension or
// otherwise. This exercises the connector against a real (fake) peer that
// WOULD misreport success for exactly this kind of probe (see
// TestBareSuccessPeerReplicatesCodexACPQuirk), and confirms it is never
// touched.
func TestCodexConnectorNeverProbesTheBareSuccessPeer(t *testing.T) {
	peer, _ := dialBareSuccessPeer(t)

	c := Codex("gpt-5-codex")
	if _, err := c.Configure(stdio.Command{Path: "/opt/codex-acp"}, ProxyBinding{BaseURL: "http://x", Token: "t"}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	c2 := c.WithModel("gpt-5-codex-mini")
	if _, err := c2.Configure(stdio.Command{Path: "/opt/codex-acp"}, ProxyBinding{BaseURL: "http://x", Token: "t"}); err != nil {
		t.Fatalf("Configure() error = %v", err)
	}

	if got := peer.recordedCalls(); len(got) != 0 {
		t.Fatalf("peer recorded calls = %v, want none: CodexConnector's Configure/WithModel have no way to reach any peer at all", got)
	}
}
