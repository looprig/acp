//go:build integration

// exampleagent_integration_test.go is Task 6.1's automated substitute for
// what Zed's own external-agent conformance harness would otherwise check at
// the protocol level: it builds the real exampleagent binary and spawns it as
// a genuine subprocess (the same pattern acp/internal/mockpeer_test.go and
// acp/client/client_integration_test.go already use), then drives it with raw
// acp/protocol calls exactly the way ANY ACP client — Zed included — would:
// initialize, session/new, session/prompt, session/cancel, a permission round
// trip, session/list, and session/load.
//
// This proves the COMPOSITION (exampleagent's Host/liveSession bound to the
// real acp/agent facade, acp/protocol wire types, and acp/transport/stdio) is
// wire-correct end to end, over a real OS process boundary. It does not, and
// cannot, prove that the real Zed editor's own external-agent integration
// accepts this binary — that requires a human with Zed installed; see
// acp/docs/interop/zed.md for the honest split between what this file
// verifies and what remains a pending manual step.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
	"github.com/looprig/harness/pkg/gate"
)

var exampleAgentBinPath string

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	tmpDir, err := os.MkdirTemp("", "acp-exampleagent-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "exampleagent_test: create temp dir:", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	goBin, err := exec.LookPath("go")
	if err != nil {
		fmt.Fprintln(os.Stderr, "exampleagent_test: find go toolchain:", err)
		return 1
	}
	exampleAgentBinPath = filepath.Join(tmpDir, "exampleagent")

	buildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// #nosec G204 -- goBin comes from exec.LookPath (never external input),
	// and every argument is a fixed literal or a path this test itself
	// constructed; there is no shell and nothing derived from wire input.
	build := exec.CommandContext(buildCtx, goBin, "build", "-o", exampleAgentBinPath, ".")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "exampleagent_test: build exampleagent binary:", err)
		return 1
	}

	return m.Run()
}

const testTimeout = 15 * time.Second

func spawnExampleAgent(t *testing.T) *stdio.Proc {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	proc, err := stdio.Spawn(ctx, stdio.Command{
		Path: exampleAgentBinPath,
		Dir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("stdio.Spawn(exampleagent): %v", err)
	}
	t.Cleanup(func() { _ = proc.Kill() })
	return proc
}

// exampleAgentPeer bundles one spawned exampleagent subprocess's connection,
// typed agent-calling surface, and the session/update notifications it has
// sent so far.
type exampleAgentPeer struct {
	conn    *protocol.Conn
	agent   *protocol.AgentConn
	updates chan protocol.SessionNotification
}

// newExampleAgentPeer spawns a fresh exampleagent subprocess and wires a
// client-role Conn over it. permissionResponder, when non-nil, answers every
// session/request_permission call exampleagent makes; a nil responder leaves
// the method unregistered entirely, matching a real client that never
// advertised permission support.
func newExampleAgentPeer(t *testing.T, permissionResponder func(protocol.RequestPermissionRequest) protocol.RequestPermissionResponse) *exampleAgentPeer {
	t.Helper()
	proc := spawnExampleAgent(t)

	conn := protocol.NewConn(proc.Stdout, proc.Stdin, protocol.ConnOptions{})
	t.Cleanup(func() { _ = conn.Close() })

	updates := make(chan protocol.SessionNotification, 64)
	conn.HandleNotify(string(protocol.MethodSessionUpdate), func(_ context.Context, _ string, params json.RawMessage) {
		var n protocol.SessionNotification
		if err := json.Unmarshal(params, &n); err != nil {
			t.Errorf("decode session/update notification: %v", err)
			return
		}
		updates <- n
	})
	if permissionResponder != nil {
		conn.Handle(string(protocol.MethodSessionRequestPermission), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
			var req protocol.RequestPermissionRequest
			if err := json.Unmarshal(params, &req); err != nil {
				t.Errorf("decode session/request_permission params: %v", err)
				return nil, err
			}
			return permissionResponder(req), nil
		})
	}

	return &exampleAgentPeer{conn: conn, agent: protocol.NewAgentConn(conn), updates: updates}
}

func (p *exampleAgentPeer) initialize(t *testing.T, ctx context.Context) *protocol.InitializeResponse {
	t.Helper()
	resp, err := p.agent.Initialize(ctx, protocol.InitializeRequest{ProtocolVersion: protocol.CurrentProtocolVersion})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	return resp
}

func (p *exampleAgentPeer) newSession(t *testing.T, ctx context.Context, cwd string) protocol.SessionID {
	t.Helper()
	resp, err := p.agent.NewSession(ctx, protocol.NewSessionRequest{Cwd: cwd, McpServers: []protocol.McpServer{}})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	if resp.SessionID == "" {
		t.Fatal("NewSession() returned an empty session id")
	}
	return resp.SessionID
}

func textPrompt(text string) []protocol.ContentBlock {
	return []protocol.ContentBlock{{Text: &protocol.TextContent{Text: text}}}
}

// updateMeta mirrors acp/agent's private wire shape for a session/update's
// _meta object (eventId, promptId, isReplay), so tests can decode it without
// depending on the agent package's unexported type.
type updateMeta struct {
	EventID  string `json:"eventId"`
	PromptID string `json:"promptId,omitempty"`
	IsReplay bool   `json:"isReplay,omitempty"`
}

func decodeMeta(t *testing.T, raw json.RawMessage) updateMeta {
	t.Helper()
	var m updateMeta
	if len(raw) == 0 {
		return m
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode _meta: %v", err)
	}
	return m
}

func updateKind(u protocol.SessionUpdate) string {
	switch {
	case u.UserMessageChunk != nil:
		return "user_message_chunk"
	case u.AgentMessageChunk != nil:
		return "agent_message_chunk"
	case u.AgentThoughtChunk != nil:
		return "agent_thought_chunk"
	case u.ToolCall != nil:
		return "tool_call"
	case u.ToolCallUpdate != nil:
		return "tool_call_update"
	case u.Plan != nil:
		return "plan"
	case u.UsageUpdate != nil:
		return "usage_update"
	case u.SessionInfoUpdate != nil:
		return "session_info_update"
	default:
		return "unknown"
	}
}

// drainUpdateKinds collects session/update notifications from p.updates until
// every kind in want has been observed at least once (returning the last
// notification seen of each), or timeout elapses.
func drainUpdateKinds(t *testing.T, p *exampleAgentPeer, want []string, timeout time.Duration) map[string]protocol.SessionNotification {
	t.Helper()
	seen := make(map[string]protocol.SessionNotification, len(want))
	need := make(map[string]struct{}, len(want))
	for _, k := range want {
		need[k] = struct{}{}
	}
	deadline := time.After(timeout)
	for len(need) > 0 {
		select {
		case n := <-p.updates:
			k := updateKind(n.Update)
			seen[k] = n
			delete(need, k)
		case <-deadline:
			missing := make([]string, 0, len(need))
			for k := range need {
				missing = append(missing, k)
			}
			t.Fatalf("timed out waiting for update kinds %v; still missing %v", want, missing)
		}
	}
	return seen
}

// drainAvailable collects every session/update notification already queued
// (or arriving within quiet of the previous one), without requiring any
// particular kind. It is used where a test needs to assert something did NOT
// happen (e.g. no tool_call after a denied permission), where waiting the
// full testTimeout for an event that will never arrive would needlessly slow
// the suite down.
func drainAvailable(p *exampleAgentPeer, quiet time.Duration) []protocol.SessionNotification {
	var out []protocol.SessionNotification
	for {
		select {
		case n := <-p.updates:
			out = append(out, n)
		case <-time.After(quiet):
			return out
		}
	}
}

// --- initialize ------------------------------------------------------------

func TestExampleAgentInitializeAdvertisesLoadAndListCapabilities(t *testing.T) {
	t.Parallel()
	p := newExampleAgentPeer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	resp := p.initialize(t, ctx)
	if resp.ProtocolVersion != protocol.CurrentProtocolVersion {
		t.Errorf("ProtocolVersion = %v, want %v", resp.ProtocolVersion, protocol.CurrentProtocolVersion)
	}
	if resp.AgentCapabilities == nil {
		t.Fatal("AgentCapabilities = nil, want non-nil")
	}
	if !resp.AgentCapabilities.LoadSession {
		t.Error("AgentCapabilities.LoadSession = false, want true (Host supplies an EventReplayer)")
	}
	sc := resp.AgentCapabilities.SessionCapabilities
	if sc == nil || sc.List == nil {
		t.Error("AgentCapabilities.SessionCapabilities.List = nil, want non-nil (Host supplies a SessionCatalog)")
	}
}

// --- session/new + session/prompt (default flow) + a real tool call -------

func TestExampleAgentNewSessionAndDefaultPromptCompletesWithToolCall(t *testing.T) {
	t.Parallel()
	p := newExampleAgentPeer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	p.initialize(t, ctx)

	sessionID := p.newSession(t, ctx, t.TempDir())

	resp, err := p.agent.Prompt(ctx, protocol.PromptRequest{SessionID: sessionID, Prompt: textPrompt("hello")})
	if err != nil {
		t.Fatalf("Prompt() error = %v", err)
	}
	if resp.StopReason != protocol.StopReasonEndTurn {
		t.Errorf("StopReason = %v, want %v", resp.StopReason, protocol.StopReasonEndTurn)
	}

	seen := drainUpdateKinds(t, p, []string{"agent_message_chunk", "tool_call", "tool_call_update"}, testTimeout)
	upd := seen["tool_call_update"].Update.ToolCallUpdate
	if upd == nil || upd.Status == nil || *upd.Status != protocol.ToolCallStatusCompleted {
		t.Errorf("tool_call_update status = %v, want completed", upd)
	}
}

// --- session/cancel ---------------------------------------------------------

func TestExampleAgentSessionCancelStopsAnInFlightPrompt(t *testing.T) {
	t.Parallel()
	p := newExampleAgentPeer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	p.initialize(t, ctx)
	sessionID := p.newSession(t, ctx, t.TempDir())

	type result struct {
		resp *protocol.PromptResponse
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := p.agent.Prompt(ctx, protocol.PromptRequest{SessionID: sessionID, Prompt: textPrompt("please trigger-cancel this one")})
		done <- result{resp, err}
	}()

	// Wait for the turn to actually start streaming before cancelling it, so
	// this proves a real in-flight prompt was interrupted, not a race against
	// one that had not even started yet.
	drainUpdateKinds(t, p, []string{"agent_message_chunk"}, testTimeout)

	if err := p.agent.Cancel(ctx, protocol.CancelNotification{SessionID: sessionID}); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Prompt() after cancel: unexpected error (cancellation must be reported as success): %v", r.err)
		}
		if r.resp.StopReason != protocol.StopReasonCancelled {
			t.Errorf("StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonCancelled)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the cancelled session/prompt to return")
	}
}

// --- permission round trip --------------------------------------------------

func TestExampleAgentPermissionRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		optionID    string
		wantToolRun bool
	}{
		{name: "approve", optionID: string(gate.ApprovalApprove), wantToolRun: true},
		{name: "deny", optionID: string(gate.ApprovalDeny), wantToolRun: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var gotReq protocol.RequestPermissionRequest
			reqReceived := make(chan struct{})
			p := newExampleAgentPeer(t, func(req protocol.RequestPermissionRequest) protocol.RequestPermissionResponse {
				gotReq = req
				close(reqReceived)
				return protocol.RequestPermissionResponse{
					Outcome: protocol.RequestPermissionOutcome{
						Selected: &protocol.SelectedPermissionOutcome{OptionID: protocol.PermissionOptionID(tt.optionID)},
					},
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()
			p.initialize(t, ctx)
			sessionID := p.newSession(t, ctx, t.TempDir())

			resp, err := p.agent.Prompt(ctx, protocol.PromptRequest{SessionID: sessionID, Prompt: textPrompt("trigger-permission please")})
			if err != nil {
				t.Fatalf("Prompt() error = %v", err)
			}
			if resp.StopReason != protocol.StopReasonEndTurn {
				t.Errorf("StopReason = %v, want %v", resp.StopReason, protocol.StopReasonEndTurn)
			}

			select {
			case <-reqReceived:
			case <-time.After(testTimeout):
				t.Fatal("timed out waiting for session/request_permission")
			}
			if gotReq.SessionID != sessionID {
				t.Errorf("permission request sessionId = %q, want %q", gotReq.SessionID, sessionID)
			}
			if len(gotReq.Options) != 3 {
				t.Errorf("permission request options = %d, want 3 (Approve/Approve-always/Deny)", len(gotReq.Options))
			}

			gotToolCall := false
			for _, n := range drainAvailable(p, 500*time.Millisecond) {
				if updateKind(n.Update) == "tool_call" {
					gotToolCall = true
				}
			}
			if gotToolCall != tt.wantToolRun {
				t.Errorf("observed a tool_call update = %v, want %v", gotToolCall, tt.wantToolRun)
			}
		})
	}
}

// --- session/list ------------------------------------------------------------

func TestExampleAgentSessionListReportsSessionsWithTitleAndCwd(t *testing.T) {
	t.Parallel()
	p := newExampleAgentPeer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	p.initialize(t, ctx)

	titledCwd := t.TempDir()
	titledSession := p.newSession(t, ctx, titledCwd)
	if _, err := p.agent.Prompt(ctx, protocol.PromptRequest{SessionID: titledSession, Prompt: textPrompt("remember this title")}); err != nil {
		t.Fatalf("Prompt() error = %v", err)
	}

	untitledCwd := t.TempDir()
	untitledSession := p.newSession(t, ctx, untitledCwd)

	listResp, err := p.agent.ListSessions(ctx, protocol.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions() error = %v", err)
	}

	byID := make(map[protocol.SessionID]protocol.SessionInfo, len(listResp.Sessions))
	for _, info := range listResp.Sessions {
		byID[info.SessionID] = info
	}

	titled, ok := byID[titledSession]
	if !ok {
		t.Fatalf("session/list did not report the titled session %q (got %d sessions)", titledSession, len(listResp.Sessions))
	}
	if titled.Cwd != titledCwd {
		t.Errorf("titled session cwd = %q, want %q", titled.Cwd, titledCwd)
	}
	if titled.Title == nil || *titled.Title == "" {
		t.Error("titled session Title = nil/empty, want a non-empty title derived from its prompt")
	}

	untitled, ok := byID[untitledSession]
	if !ok {
		t.Fatalf("session/list did not report the untitled session %q (got %d sessions)", untitledSession, len(listResp.Sessions))
	}
	if untitled.Cwd != untitledCwd {
		t.Errorf("untitled session cwd = %q, want %q", untitled.Cwd, untitledCwd)
	}
	if untitled.Title != nil {
		t.Errorf("untitled session Title = %q, want nil (no prompt submitted yet)", *untitled.Title)
	}
}

// --- session/load ------------------------------------------------------------

func TestExampleAgentSessionLoadReplaysDurableHistoryAfterClose(t *testing.T) {
	t.Parallel()
	p := newExampleAgentPeer(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	p.initialize(t, ctx)

	cwd := t.TempDir()
	sessionID := p.newSession(t, ctx, cwd)
	const promptText = "please remember this exchange"
	if _, err := p.agent.Prompt(ctx, protocol.PromptRequest{SessionID: sessionID, Prompt: textPrompt(promptText)}); err != nil {
		t.Fatalf("Prompt() error = %v", err)
	}
	drainUpdateKinds(t, p, []string{"agent_message_chunk", "tool_call", "tool_call_update"}, testTimeout)

	if _, err := p.agent.CloseSession(ctx, protocol.CloseSessionRequest{SessionID: sessionID}); err != nil {
		t.Fatalf("CloseSession() error = %v", err)
	}

	if _, err := p.agent.LoadSession(ctx, protocol.LoadSessionRequest{SessionID: sessionID, Cwd: cwd, McpServers: []protocol.McpServer{}}); err != nil {
		t.Fatalf("LoadSession() error = %v", err)
	}

	seen := drainUpdateKinds(t, p, []string{"user_message_chunk", "agent_message_chunk", "tool_call"}, testTimeout)
	for kind, n := range seen {
		if meta := decodeMeta(t, n.Meta); !meta.IsReplay {
			t.Errorf("replayed %s update _meta.isReplay = false, want true", kind)
		}
	}

	userChunk := seen["user_message_chunk"].Update.UserMessageChunk
	if userChunk == nil || userChunk.Content.Text == nil {
		t.Fatal("replayed user_message_chunk has no text content")
	}
	if userChunk.Content.Text.Text != promptText {
		t.Errorf("replayed user message text = %q, want %q", userChunk.Content.Text.Text, promptText)
	}

	// A further prompt on the reloaded session must still work, proving the
	// live controller (not just the replay) was genuinely restored.
	resp, err := p.agent.Prompt(ctx, protocol.PromptRequest{SessionID: sessionID, Prompt: textPrompt("hello again")})
	if err != nil {
		t.Fatalf("Prompt() after session/load: error = %v", err)
	}
	if resp.StopReason != protocol.StopReasonEndTurn {
		t.Errorf("StopReason = %v, want %v", resp.StopReason, protocol.StopReasonEndTurn)
	}
}
