package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

// mockpeerBinPath is the real mockpeer binary, built once in TestMain and
// spawned as a genuine subprocess by every integration test below — the same
// way acp/client's tests (Task 5.1) and this package's own tests exercise it.
var mockpeerBinPath string

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	tmpDir, err := os.MkdirTemp("", "acp-mockpeer-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mockpeer_test: create temp dir:", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	goBin, err := exec.LookPath("go")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mockpeer_test: find go toolchain:", err)
		return 1
	}
	mockpeerBinPath = filepath.Join(tmpDir, "mockpeer")

	buildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// #nosec G204 -- goBin comes from exec.LookPath (never external input),
	// and every argument is a fixed literal or a path this test itself
	// constructed; there is no shell and nothing derived from wire input.
	build := exec.CommandContext(buildCtx, goBin, "build", "-o", mockpeerBinPath, ".")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "mockpeer_test: build mockpeer binary:", err)
		return 1
	}

	return m.Run()
}

// spawnMockpeer starts the real mockpeer binary with exactly env as its
// environment (never this test process's ambient one, per Command's
// contract) and registers cleanup that kills and reaps it unconditionally,
// so no test can leave a zombie behind regardless of how it exits.
func spawnMockpeer(t *testing.T, env []string) *stdio.Proc {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	proc, err := stdio.Spawn(ctx, stdio.Command{
		Path: mockpeerBinPath,
		Env:  env,
		Dir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("stdio.Spawn(mockpeer): %v", err)
	}
	t.Cleanup(func() { _ = proc.Kill() })
	return proc
}

// updateKind reports which SessionUpdate variant is set, by name, so test
// assertions can compare against the plain strings used on the wire.
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
	default:
		return "unknown"
	}
}

// --- default-mode behavior: a real prompt turn end to end ---

func TestMockpeerDefaultBehaviorAnswersFullPromptTurn(t *testing.T) {
	t.Parallel()
	proc := spawnMockpeer(t, nil)

	client := protocol.NewConn(proc.Stdout, proc.Stdin, protocol.ConnOptions{})
	t.Cleanup(func() { _ = client.Close() })
	agent := protocol.NewAgentConn(client)

	updates := make(chan protocol.SessionUpdate, 8)
	client.HandleNotify(string(protocol.MethodSessionUpdate), func(_ context.Context, _ string, params json.RawMessage) {
		var n protocol.SessionNotification
		if err := json.Unmarshal(params, &n); err != nil {
			t.Errorf("decode session/update notification: %v", err)
			return
		}
		updates <- n.Update
	})

	var gotPermReq protocol.RequestPermissionRequest
	permReceived := make(chan struct{})
	client.Handle(string(protocol.MethodSessionRequestPermission), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
		if err := json.Unmarshal(params, &gotPermReq); err != nil {
			t.Errorf("decode session/request_permission params: %v", err)
		}
		close(permReceived)
		return protocol.RequestPermissionResponse{
			Outcome: protocol.RequestPermissionOutcome{
				Selected: &protocol.SelectedPermissionOutcome{OptionID: "allow-once"},
			},
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	initResp, err := agent.Initialize(ctx, protocol.InitializeRequest{ProtocolVersion: protocol.CurrentProtocolVersion})
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	if initResp.ProtocolVersion != protocol.CurrentProtocolVersion {
		t.Errorf("ProtocolVersion = %v, want %v", initResp.ProtocolVersion, protocol.CurrentProtocolVersion)
	}

	sessResp, err := agent.NewSession(ctx, protocol.NewSessionRequest{Cwd: "/work", McpServers: []protocol.McpServer{}})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	if sessResp.SessionID == "" {
		t.Fatal("NewSession() returned an empty session id")
	}

	promptResp, err := agent.Prompt(ctx, protocol.PromptRequest{
		SessionID: sessResp.SessionID,
		Prompt:    []protocol.ContentBlock{{Text: &protocol.TextContent{Text: "hello"}}},
	})
	if err != nil {
		t.Fatalf("Prompt() error = %v", err)
	}
	if promptResp.StopReason != protocol.StopReasonEndTurn {
		t.Errorf("StopReason = %v, want %v", promptResp.StopReason, protocol.StopReasonEndTurn)
	}

	// Conn dispatches each notification's handler in its own goroutine (see
	// protocol/conn.go's dispatchNotification/spawnHandler), so mockpeer
	// sending the five updates in a fixed wire order does not guarantee this
	// test observes them in that same order — only that all five arrive
	// exactly once. Collect by kind rather than by arrival position.
	wantKinds := []string{"agent_message_chunk", "agent_thought_chunk", "tool_call", "tool_call_update", "plan"}
	byKind := make(map[string]protocol.SessionUpdate, len(wantKinds))
	for range wantKinds {
		select {
		case u := <-updates:
			byKind[updateKind(u)] = u
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for a session/update notification (have %d/%d)", len(byKind), len(wantKinds))
		}
	}
	for _, want := range wantKinds {
		if _, ok := byKind[want]; !ok {
			t.Errorf("missing %q update; got kinds %v", want, mapKeys(byKind))
		}
	}
	if toolCall, ok := byKind["tool_call"]; ok && toolCall.ToolCall.ToolCallID != mockToolCallID {
		t.Errorf("tool_call toolCallId = %q, want %q", toolCall.ToolCall.ToolCallID, mockToolCallID)
	}
	if toolCallUpdate, ok := byKind["tool_call_update"]; ok && toolCallUpdate.ToolCallUpdate.ToolCallID != mockToolCallID {
		t.Errorf("tool_call_update toolCallId = %q, want %q", toolCallUpdate.ToolCallUpdate.ToolCallID, mockToolCallID)
	}
	if plan, ok := byKind["plan"]; ok && len(plan.Plan.Entries) == 0 {
		t.Error("plan update has no entries")
	}

	select {
	case <-permReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for session/request_permission")
	}
	if gotPermReq.SessionID != sessResp.SessionID {
		t.Errorf("permission request sessionId = %q, want %q", gotPermReq.SessionID, sessResp.SessionID)
	}
	if gotPermReq.ToolCall.ToolCallID != mockToolCallID {
		t.Errorf("permission request toolCallId = %q, want %q", gotPermReq.ToolCall.ToolCallID, mockToolCallID)
	}
	if len(gotPermReq.Options) == 0 {
		t.Error("permission request has no options")
	}
}

func TestMockpeerPromptRejectsUnknownSession(t *testing.T) {
	t.Parallel()
	proc := spawnMockpeer(t, nil)

	client := protocol.NewConn(proc.Stdout, proc.Stdin, protocol.ConnOptions{})
	t.Cleanup(func() { _ = client.Close() })
	agent := protocol.NewAgentConn(client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := agent.Initialize(ctx, protocol.InitializeRequest{ProtocolVersion: protocol.CurrentProtocolVersion}); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}

	_, err := agent.Prompt(ctx, protocol.PromptRequest{
		SessionID: "never-created",
		Prompt:    []protocol.ContentBlock{{Text: &protocol.TextContent{Text: "hello"}}},
	})
	if err == nil {
		t.Fatal("Prompt() for an unknown session succeeded, want an error")
	}
}

// --- ACP_MOCK_MALFORMED_OUTPUT ---

func TestMockpeerMalformedOutputThenContinuesNormally(t *testing.T) {
	t.Parallel()
	proc := spawnMockpeer(t, []string{"ACP_MOCK_MALFORMED_OUTPUT=1"})

	reader := bufio.NewReader(proc.Stdout)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read first line: %v", err)
	}
	if json.Valid([]byte(line)) {
		t.Fatalf("first line = %q, want non-JSON output", line)
	}

	// The rest of the stream must still behave like an ordinary agent: build
	// a Conn on top of the same (already-partially-drained) reader and
	// complete a normal initialize call.
	client := protocol.NewConn(reader, proc.Stdin, protocol.ConnOptions{})
	t.Cleanup(func() { _ = client.Close() })
	agent := protocol.NewAgentConn(client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := agent.Initialize(ctx, protocol.InitializeRequest{ProtocolVersion: protocol.CurrentProtocolVersion}); err != nil {
		t.Fatalf("Initialize() after malformed output error = %v", err)
	}
}

// --- ACP_MOCK_EXIT_CODE ---

func TestMockpeerExitCodeSubprocess(t *testing.T) {
	t.Parallel()
	proc := spawnMockpeer(t, []string{"ACP_MOCK_EXIT_CODE=42"})

	err := proc.Wait()
	var exitErr *stdio.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("Wait() error = %v (%T), want *stdio.ExitError", err, err)
	}
	var ee *exec.ExitError
	if !errors.As(exitErr.Err, &ee) {
		t.Fatalf("ExitError.Err = %v (%T), want *exec.ExitError", exitErr.Err, exitErr.Err)
	}
	if ee.ExitCode() != 42 {
		t.Fatalf("exit code = %d, want 42", ee.ExitCode())
	}
}

func TestMockpeerExitCodeInvalidValueFailsClosed(t *testing.T) {
	t.Parallel()
	proc := spawnMockpeer(t, []string{"ACP_MOCK_EXIT_CODE=not-a-number"})

	err := proc.Wait()
	var exitErr *stdio.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("Wait() error = %v (%T), want *stdio.ExitError", err, err)
	}
	var ee *exec.ExitError
	if !errors.As(exitErr.Err, &ee) {
		t.Fatalf("ExitError.Err = %v (%T), want *exec.ExitError", exitErr.Err, exitErr.Err)
	}
	if ee.ExitCode() != exitInvalidEnv {
		t.Fatalf("exit code = %d, want %d (exitInvalidEnv)", ee.ExitCode(), exitInvalidEnv)
	}
}

// --- ACP_MOCK_DIE_AFTER_INIT ---

func TestMockpeerDieAfterInit(t *testing.T) {
	t.Parallel()
	proc := spawnMockpeer(t, []string{"ACP_MOCK_DIE_AFTER_INIT=1"})

	client := protocol.NewConn(proc.Stdout, proc.Stdin, protocol.ConnOptions{})
	t.Cleanup(func() { _ = client.Close() })
	agent := protocol.NewAgentConn(client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := agent.Initialize(ctx, protocol.InitializeRequest{ProtocolVersion: protocol.CurrentProtocolVersion}); err != nil {
		t.Fatalf("Initialize() error = %v, want success (the peer dies only after replying)", err)
	}

	sessCtx, sessCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sessCancel()
	if _, err := agent.NewSession(sessCtx, protocol.NewSessionRequest{Cwd: "/work", McpServers: []protocol.McpServer{}}); err == nil {
		t.Fatal("NewSession() after ACP_MOCK_DIE_AFTER_INIT succeeded, want failure")
	}

	waitErr := proc.Wait()
	if waitErr == nil {
		t.Fatal("Wait() = nil, want the forced post-initialize exit status")
	}
	var exitErr *stdio.ExitError
	if errors.As(waitErr, &exitErr) {
		var ee *exec.ExitError
		if errors.As(exitErr.Err, &ee) && ee.ExitCode() != exitDiedAfterInit {
			t.Errorf("exit code = %d, want %d (exitDiedAfterInit)", ee.ExitCode(), exitDiedAfterInit)
		}
	}
}

// --- env parsing: fast, in-process unit tests ---

func mapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func envLookup(vars map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

func TestEnvBool(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		want    bool
		wantErr bool
	}{
		{name: "unset", vars: map[string]string{}, want: false},
		{name: "empty", vars: map[string]string{"X": ""}, want: false},
		{name: "one", vars: map[string]string{"X": "1"}, want: true},
		{name: "zero rejected", vars: map[string]string{"X": "0"}, wantErr: true},
		{name: "word rejected", vars: map[string]string{"X": "true"}, wantErr: true},
		{name: "trailing space rejected", vars: map[string]string{"X": "1 "}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := envBool(envLookup(tt.vars), "X")
			if (err != nil) != tt.wantErr {
				t.Fatalf("envBool() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("envBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnvExitCodeValue(t *testing.T) {
	tests := []struct {
		name     string
		vars     map[string]string
		wantCode int
		wantSet  bool
		wantErr  bool
	}{
		{name: "unset", vars: map[string]string{}, wantSet: false},
		{name: "zero", vars: map[string]string{"ACP_MOCK_EXIT_CODE": "0"}, wantCode: 0, wantSet: true},
		{name: "max", vars: map[string]string{"ACP_MOCK_EXIT_CODE": "255"}, wantCode: 255, wantSet: true},
		{name: "out of range high", vars: map[string]string{"ACP_MOCK_EXIT_CODE": "256"}, wantSet: true, wantErr: true},
		{name: "negative", vars: map[string]string{"ACP_MOCK_EXIT_CODE": "-1"}, wantSet: true, wantErr: true},
		{name: "garbage", vars: map[string]string{"ACP_MOCK_EXIT_CODE": "nope"}, wantSet: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, set, err := envExitCodeValue(envLookup(tt.vars))
			if set != tt.wantSet {
				t.Fatalf("set = %v, want %v", set, tt.wantSet)
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && code != tt.wantCode {
				t.Errorf("code = %d, want %d", code, tt.wantCode)
			}
		})
	}
}

// --- run(): fast in-process coverage of the exit-code fast path ---

func TestRunExitCodeFastPathNeverTouchesStdio(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(strings.NewReader(""), &stdout, &stderr, envLookup(map[string]string{"ACP_MOCK_EXIT_CODE": "7"}))
	if code != 7 {
		t.Fatalf("run() = %d, want 7", code)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

func TestRunInvalidEnvFailsClosed(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(strings.NewReader(""), &stdout, &stderr, envLookup(map[string]string{"ACP_MOCK_EXIT_CODE": "garbage"}))
	if code != exitInvalidEnv {
		t.Fatalf("run() = %d, want %d", code, exitInvalidEnv)
	}
	if stderr.Len() == 0 {
		t.Error("expected a diagnostic on stderr for an invalid ACP_MOCK_EXIT_CODE")
	}
}

func TestRunInvalidMalformedOutputFlagFailsClosed(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(strings.NewReader(""), &stdout, &stderr, envLookup(map[string]string{"ACP_MOCK_MALFORMED_OUTPUT": "yes"}))
	if code != exitInvalidEnv {
		t.Fatalf("run() = %d, want %d", code, exitInvalidEnv)
	}
}

func TestRunInvalidDieAfterInitFlagFailsClosed(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(strings.NewReader(""), &stdout, &stderr, envLookup(map[string]string{"ACP_MOCK_DIE_AFTER_INIT": "yes"}))
	if code != exitInvalidEnv {
		t.Fatalf("run() = %d, want %d", code, exitInvalidEnv)
	}
}
