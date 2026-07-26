//go:build integration

// interop_integration_test.go is Task 6.2 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md ("Client interop
// against one maintained ACP agent"): a golden probe that drives a real,
// independently-maintained ACP agent binary through acp/client — the same
// production code path foreignloops/driver/acp uses — rather than against
// acp/internal/mockpeer (a scripted, in-repo test double). Everywhere else in
// this module's test suite exercises the bridge against mockpeer, which
// proves this module's own wire behavior but can never prove this module
// actually interoperates with an agent it did not write. This file is the one
// place that tries.
//
// # Which real agent this targets
//
// The ACP ecosystem (agentclientprotocol.com, the same spec this module's
// protocol/schema/v1 artifacts are pinned from) is implemented by several
// independently-maintained coding agents that can run in an ACP "agent" mode
// over stdio. This module's own implementation plan (Task 6.2's own text)
// names three examples to pick from, whichever is installed on the machine
// running this test: a Gemini CLI ACP endpoint, a Claude Code ACP endpoint,
// or `cursor-agent acp`. This test is written generically against ANY of
// them — it asserts only wire-level protocol invariants (see below), never
// anything about what a specific agent says or does — so it is not tied to
// one particular agent's presence.
//
// IMPORTANT — honesty note: this session had no such agent binary installed
// or reachable, and this test has NEVER been run against a real external
// agent in this repository's history so far. It is written and verified to
// compile and to SKIP cleanly (see TestMain/ACP_INTEROP_AGENT_PATH below) in
// an environment with no such binary — which is the only thing that has
// actually been verified here. A human with one of the agents above (or any
// other ACP-conformant agent) installed can point ACP_INTEROP_AGENT_PATH at
// it to actually exercise the golden path; see this file's env var docs for
// exactly how, and acp/docs/interop/ for a recorded pointer to this test.
//
// # Assertions are protocol invariants only, not content
//
// Per this task's own spec, every assertion below is about the WIRE SHAPE of
// a real agent's responses — initialize succeeds and returns a well-formed
// capabilities object, a session id round-trips, a prompt eventually reaches
// one of the pinned schema's defined terminal stop reasons, and
// session/cancel does not itself error — never about what the agent actually
// said or did. A real agent's specific reply text, tool choices, or model
// behavior are none of this bridge's concern and are deliberately never
// inspected here.
package client_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

// envInteropAgentPath names the real ACP agent executable to drive. Set it
// to an absolute path, or a bare command name resolved via the OS's PATH
// (mirroring ordinary shell lookup — see resolveInteropAgentPath), e.g.:
//
//	ACP_INTEROP_AGENT_PATH=gemini ACP_INTEROP_AGENT_ARGS="--experimental-acp" \
//	  go test -tags integration -race ./client/... -run TestClientInteropAgainstRealACPAgent
//
//	ACP_INTEROP_AGENT_PATH=cursor-agent ACP_INTEROP_AGENT_ARGS=acp \
//	  go test -tags integration -race ./client/... -run TestClientInteropAgainstRealACPAgent
//
// The exact flag or subcommand that puts a given agent into ACP "agent" mode
// (served over stdio, speaking this pinned schema) is that agent's own
// choice and may change over time; consult its own documentation. This test
// deliberately does not hardcode one agent's invocation — it only reads
// whatever ACP_INTEROP_AGENT_PATH/ACP_INTEROP_AGENT_ARGS name.
const envInteropAgentPath = "ACP_INTEROP_AGENT_PATH"

// envInteropAgentArgs is an optional space-separated argument list appended
// after envInteropAgentPath's resolved executable (see resolveInteropArgs).
const envInteropAgentArgs = "ACP_INTEROP_AGENT_ARGS"

// resolveInteropAgentPath resolves raw (the value of ACP_INTEROP_AGENT_PATH)
// into the absolute, cleaned path stdio.Command requires: a bare name (no
// path separator) is looked up on PATH exactly like a shell would; anything
// else is cleaned and made absolute relative to the current working
// directory. It never invokes a shell itself.
func resolveInteropAgentPath(raw string) (string, error) {
	if !strings.ContainsRune(raw, os.PathSeparator) {
		return exec.LookPath(raw)
	}
	abs, err := filepath.Abs(raw)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

// resolveInteropArgs splits ACP_INTEROP_AGENT_ARGS on whitespace. It
// deliberately does not support shell quoting: every real invocation this
// test doc names (`--experimental-acp`, `acp`) is a single bare token, so a
// naive whitespace split is sufficient and avoids pulling in a shell-parsing
// dependency for a manual, opt-in test harness.
func resolveInteropArgs(raw string) []string {
	if raw == "" {
		return nil
	}
	return strings.Fields(raw)
}

// interopEnv is the environment passed to the real agent subprocess. Every
// other Command this module spawns (mockpeer, exampleagent) is a controlled,
// in-repo test double that needs no ambient environment at all — matching
// this module's "never inherit the parent environment wholesale" rule (see
// acp/CLAUDE.md and stdio.Command's own doc). This ONE golden probe is a
// deliberate, narrow, documented exception: it exists purely so a human can
// manually opt in (by setting ACP_INTEROP_AGENT_PATH) to run their own
// already-configured, locally-installed real agent, which may need its own
// PATH, HOME, API keys, or other credentials to authenticate and run at all
// — none of which this test can enumerate or allowlist in advance without
// hardcoding assumptions about a specific third-party agent's own
// configuration surface. Forwarding the current process's full environment
// is therefore the only viable choice for this specific, opt-in-only,
// human-operated harness; it must never be copied as a pattern for any
// caller-facing or automatically-invoked code path in this module.
func interopEnv() []string {
	return os.Environ()
}

// requireInteropAgent skips the test (with a clear, actionable message) if
// ACP_INTEROP_AGENT_PATH is unset, or fails the test outright if it is set
// but does not resolve to a runnable executable — a configuration mistake is
// not the same thing as "no agent configured," and should not be silently
// treated as a skip.
func requireInteropAgent(t *testing.T) stdio.Command {
	t.Helper()
	raw, ok := os.LookupEnv(envInteropAgentPath)
	if !ok || raw == "" {
		t.Skipf("skipping: %s is not set. This golden probe drives a real, independently-maintained ACP agent (see this file's package doc for examples and setup); it only runs when a human points %s at one. No such agent is installed/configured in this environment, so it skips rather than failing.", envInteropAgentPath, envInteropAgentPath)
	}

	path, err := resolveInteropAgentPath(raw)
	if err != nil {
		t.Fatalf("%s=%q did not resolve to a runnable executable: %v", envInteropAgentPath, raw, err)
	}

	return stdio.Command{
		Path: path,
		Args: resolveInteropArgs(os.Getenv(envInteropAgentArgs)),
		Env:  interopEnv(),
		Dir:  t.TempDir(),
	}
}

// wantStopReasons is the pinned v1.20.0 schema's complete closed set of
// StopReason values (protocol/types_gen.go) — the only values a decoded
// PromptResponse can ever carry (its UnmarshalJSON already rejects anything
// else), so membership here is a documentation aid more than a real
// decode-time guard, but it makes explicit exactly which "terminal
// stopReason" values this probe accepts, per the task's protocol-invariant
// framing.
var wantStopReasons = map[protocol.StopReason]bool{
	protocol.StopReasonEndTurn:         true,
	protocol.StopReasonMaxTokens:       true,
	protocol.StopReasonMaxTurnRequests: true,
	protocol.StopReasonRefusal:         true,
	protocol.StopReasonCancelled:       true,
}

// TestClientInteropAgainstRealACPAgent drives one real, externally-installed
// ACP agent through acp/client: Dial (which performs "initialize" internally
// — see Client.Dial), session/new, a single session/prompt, and
// session/cancel. Every assertion is a protocol invariant (see this file's
// package doc); nothing about the agent's actual reply content, tool
// choices, or behavior is ever inspected.
func TestClientInteropAgainstRealACPAgent(t *testing.T) {
	cmd := requireInteropAgent(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := client.Dial(ctx, cmd, client.Options{})
	if err != nil {
		t.Fatalf("Dial() (initialize) against the real agent: error = %v", err)
	}
	defer c.Close(context.Background())

	// --- session/new: a session id must round-trip -----------------------
	sess, err := c.NewSession(ctx, client.NewSessionParams{Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("NewSession() against the real agent: error = %v", err)
	}
	if sess.ID() == "" {
		t.Fatal("NewSession() returned an empty session id")
	}

	// --- session/prompt: must eventually reach a defined terminal
	// stopReason. The prompt text is deliberately inert (asks for no tool
	// use or file changes) since this probe has no interest in, and makes no
	// assertion about, what the agent actually does or says.
	promptCtx, promptCancel := context.WithTimeout(ctx, 120*time.Second)
	defer promptCancel()
	result, err := sess.Prompt(promptCtx, []protocol.ContentBlock{{Text: &protocol.TextContent{
		Text: "Please reply with a brief acknowledgement only. Do not run any tools, commands, or make any file changes.",
	}}})
	if err != nil {
		t.Fatalf("Prompt() against the real agent: error = %v", err)
	}
	if !wantStopReasons[result.StopReason] {
		t.Errorf("Prompt() StopReason = %q, want one of the pinned schema's defined StopReason values", result.StopReason)
	}

	// --- session/cancel: must not itself error, per the task's "cancel
	// doesn't error" invariant. By this point the prompt above has already
	// resolved, so this proves session/cancel is accepted as a valid,
	// harmless notification on a session with nothing in flight — a real
	// mid-flight cancellation race against an arbitrary, unknown-latency
	// external agent is not something this probe can reliably orchestrate
	// without guessing at that agent's own timing, so it is intentionally
	// not attempted here (see acp/internal/exampleagent's dedicated
	// mid-flight cancellation golden probe for that scenario against a
	// composition this module fully controls).
	if err := sess.Cancel(ctx); err != nil {
		t.Errorf("Cancel() against the real agent: error = %v, want nil", err)
	}

	if err := c.Close(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("Close() against the real agent: error = %v", err)
	}
}
