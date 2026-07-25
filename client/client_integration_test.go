//go:build integration

// client_integration_test.go exercises acp/client against the real
// acp/internal/mockpeer binary, spawned as a genuine subprocess exactly the
// way foreignloops/driver/acp will drive a real foreign agent. It is
// process-boundary-only coverage: the fast, deterministic behaviors already
// covered via the in-process fakeAgent (session lifecycle, prompt semaphore,
// cancel, dedup, capability dispatch) are not re-tested here. This file
// specifically proves: (1) Dial genuinely spawns and initializes over real
// stdio, and (2) each of mockpeer's three fault-injection modes surfaces as
// the typed client behavior Task 5.1 requires.
package client_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

var mockpeerBinPath string

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	tmpDir, err := os.MkdirTemp("", "acp-client-mockpeer-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "client_integration_test: create temp dir:", err)
		return 1
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	goBin, err := exec.LookPath("go")
	if err != nil {
		fmt.Fprintln(os.Stderr, "client_integration_test: find go toolchain:", err)
		return 1
	}
	mockpeerBinPath = filepath.Join(tmpDir, "mockpeer")

	buildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// #nosec G204 -- goBin comes from exec.LookPath (never external input);
	// every argument is a fixed literal or a path this test itself
	// constructed. There is no shell and nothing derived from wire input.
	build := exec.CommandContext(buildCtx, goBin, "build", "-o", mockpeerBinPath, "../internal/mockpeer")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "client_integration_test: build mockpeer binary:", err)
		return 1
	}

	return m.Run()
}

func mockpeerCommand(env []string) stdio.Command {
	return stdio.Command{Path: mockpeerBinPath, Env: env}
}

// --- real spawn + initialize + a full prompt turn, end to end ---

func TestDialAgainstRealMockpeerCompletesAFullPromptTurn(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	c, err := client.Dial(ctx, mockpeerCommand(nil), client.Options{})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer c.Close(context.Background())

	sess, err := c.NewSession(ctx, client.NewSessionParams{Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	res, err := sess.Prompt(ctx, []protocol.ContentBlock{{Text: &protocol.TextContent{Text: "hello"}}})
	if err != nil {
		t.Fatalf("Prompt() error = %v", err)
	}
	if res.StopReason != protocol.StopReasonEndTurn {
		t.Errorf("StopReason = %v, want %v", res.StopReason, protocol.StopReasonEndTurn)
	}

	// mockpeer's default prompt turn streams five updates before returning;
	// drain at least one to prove the update path works end to end over a
	// real subprocess (the exact sequence is already covered by
	// internal/mockpeer/mockpeer_test.go and the in-process fakeAgent
	// tests).
	select {
	case u, ok := <-sess.Updates():
		if !ok {
			t.Fatal("Updates() closed before delivering mockpeer's streamed updates")
		}
		_ = u
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a session/update from the real mockpeer subprocess")
	}
}

// --- ACP_MOCK_EXIT_CODE: the subprocess exits before any protocol activity ---

func TestDialAgainstMockpeerExitCodeFaultFailsTyped(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.Dial(ctx, mockpeerCommand([]string{"ACP_MOCK_EXIT_CODE=17"}), client.Options{})
	if err == nil {
		t.Fatal("Dial() against a mockpeer that exits before any protocol activity succeeded, want an error")
	}
	var closedErr *client.ClosedError
	if !errors.As(err, &closedErr) {
		t.Fatalf("Dial() error = %v (%T), want *client.ClosedError", err, err)
	}
}

// --- ACP_MOCK_DIE_AFTER_INIT: the subprocess dies right after answering
// "initialize" ---

func TestDialAgainstMockpeerDieAfterInitFailsSubsequentCallsTyped(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := client.Dial(ctx, mockpeerCommand([]string{"ACP_MOCK_DIE_AFTER_INIT=1"}), client.Options{})
	if err != nil {
		t.Fatalf("Dial() error = %v, want success (the peer dies only after answering initialize)", err)
	}
	defer c.Close(context.Background())

	sessCtx, sessCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sessCancel()
	if _, err := c.NewSession(sessCtx, client.NewSessionParams{Cwd: t.TempDir()}); err == nil {
		t.Fatal("NewSession() after ACP_MOCK_DIE_AFTER_INIT succeeded, want a typed failure")
	}
}

// --- ACP_MOCK_MALFORMED_OUTPUT: one bad line before normal behavior ---

func TestDialAgainstMockpeerMalformedOutputStillInitializes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, err := client.Dial(ctx, mockpeerCommand([]string{"ACP_MOCK_MALFORMED_OUTPUT=1"}), client.Options{})
	if err != nil {
		t.Fatalf("Dial() error = %v, want success (a leading malformed line must not break framing)", err)
	}
	defer c.Close(context.Background())

	if _, err := c.NewSession(ctx, client.NewSessionParams{Cwd: t.TempDir()}); err != nil {
		t.Fatalf("NewSession() after malformed leading output error = %v, want success", err)
	}
}
