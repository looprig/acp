// Package main builds mockpeer: a small, real ACP agent used as a scriptable
// test peer. It is spawned as a subprocess (over acp/transport/stdio) by
// acp/client's tests and by mockpeer_test.go here; it is never imported as a
// library.
//
// mockpeer speaks the agent role of the Agent Client Protocol directly on
// protocol.Conn (via Conn.Handle/HandleNotify with the generated method-name
// constants and request/response types) rather than through a typed
// "server-side AgentConn" wrapper, because no such wrapper exists yet — that
// lands in Phase 2 as the real agent facade.
//
// Behavior is scripted entirely through environment variables, read once at
// startup and validated defensively (see envBool and envExitCode): garbage
// values fail closed with exitInvalidEnv rather than being silently ignored
// or causing a panic.
//
//   - ACP_MOCK_EXIT_CODE=n: exit immediately with code n, before any protocol
//     activity.
//   - ACP_MOCK_MALFORMED_OUTPUT=1: write one non-JSON line to stdout before
//     starting the connection, then continue serving normally.
//   - ACP_MOCK_DIE_AFTER_INIT=1: behave normally through the "initialize"
//     response, then exit the instant that response has been flushed to
//     stdout — simulating a peer that crashes right after the handshake.
//   - Default (none set): answer "initialize", "session/new", and
//     "session/prompt"; a prompt streams a fixed "session/update" sequence
//     (an agent message chunk, an agent thought chunk, a tool call, a tool
//     call update, and a plan) and issues one "session/request_permission"
//     call back to the peer.
//
// Note on elicitation: the task this package implements
// (docs/plans/2026-07-23-acp-bridge-implementation.md, Task 1.8) also calls
// for mockpeer to "issue ... one elicitation when told to." The pinned
// v1.20.0 ACP schema this module generates from (see protocol/methods_gen.go)
// has no elicitation method at all — it was already confirmed absent during
// Task 1.7 (protocol/acp.go has no ClientConn.Elicit either, for the same
// reason). Per the plan's own precedence rule ("when a generated-schema
// detail conflicts with this plan's exact constant names, the pinned
// artifact wins"), elicitation is omitted here rather than invented.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

const (
	envExitCode        = "ACP_MOCK_EXIT_CODE"
	envMalformedOutput = "ACP_MOCK_MALFORMED_OUTPUT"
	envDieAfterInit    = "ACP_MOCK_DIE_AFTER_INIT"

	// exitInvalidEnv is returned when one of mockpeer's own env vars is set
	// but cannot be parsed. It is deliberately distinct from any exit code a
	// test could legitimately request via ACP_MOCK_EXIT_CODE (which accepts
	// the full 0-255 range), so a misconfigured test fixture never gets
	// confused for a deliberately requested exit code.
	exitInvalidEnv = 78
	// exitServeFailed is returned when the stdio transport loop ends with an
	// error that was not caused by mockpeer's own shutdown (ctx cancellation).
	exitServeFailed = 1
	// exitDiedAfterInit is the process exit code forced by
	// ACP_MOCK_DIE_AFTER_INIT once the initialize response has been flushed.
	exitDiedAfterInit = 91

	// malformedLine is the exact non-JSON bytes ACP_MOCK_MALFORMED_OUTPUT
	// writes as the very first line of output, before any protocol traffic.
	malformedLine = "not-json: this line intentionally breaks NDJSON framing\n"
)

func main() {
	os.Exit(run(os.Stdin, os.Stdout, os.Stderr, os.LookupEnv))
}

// run is main's entire body, factored out so mockpeer_test.go can exercise it
// directly (fast, in-process) as well as via a real spawned subprocess. It
// never panics on bad input: every env var is validated up front and a
// parse failure fails closed with exitInvalidEnv.
func run(stdin io.Reader, stdout io.Writer, stderr io.Writer, lookupEnv func(string) (string, bool)) int {
	if code, set, err := envExitCodeValue(lookupEnv); err != nil {
		fmt.Fprintf(stderr, "mockpeer: %s: %v\n", envExitCode, err)
		return exitInvalidEnv
	} else if set {
		return code
	}

	malformed, err := envBool(lookupEnv, envMalformedOutput)
	if err != nil {
		fmt.Fprintf(stderr, "mockpeer: %v\n", err)
		return exitInvalidEnv
	}
	dieAfterInit, err := envBool(lookupEnv, envDieAfterInit)
	if err != nil {
		fmt.Fprintf(stderr, "mockpeer: %v\n", err)
		return exitInvalidEnv
	}

	if malformed {
		if _, err := io.WriteString(stdout, malformedLine); err != nil {
			fmt.Fprintf(stderr, "mockpeer: write malformed output: %v\n", err)
			return exitServeFailed
		}
	}

	out := stdout
	if dieAfterInit {
		out = &dieAfterFirstWrite{w: stdout}
	}

	conn := protocol.NewConn(stdin, out, protocol.ConnOptions{})
	newAgentPeer().register(conn)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := stdio.Serve(ctx, stdin, out, conn); err != nil && ctx.Err() == nil {
		fmt.Fprintf(stderr, "mockpeer: serve: %v\n", err)
		return exitServeFailed
	}
	return 0
}

// envBool reads name as a strict boolean flag: unset or "" means false, "1"
// means true, and anything else (including "0", "true", "yes") is rejected
// rather than guessed at, so a typo in a test's env can never be silently
// misinterpreted as either value.
func envBool(lookupEnv func(string) (string, bool), name string) (bool, error) {
	v, ok := lookupEnv(name)
	if !ok || v == "" {
		return false, nil
	}
	if v == "1" {
		return true, nil
	}
	return false, fmt.Errorf("%s: invalid value %q, want unset or %q", name, v, "1")
}

// envExitCodeValue reads ACP_MOCK_EXIT_CODE. set reports whether it was
// present at all; when present, it must parse as a whole number in the
// [0, 255] range a process exit code can actually carry.
func envExitCodeValue(lookupEnv func(string) (string, bool)) (code int, set bool, err error) {
	v, ok := lookupEnv(envExitCode)
	if !ok || v == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, true, fmt.Errorf("invalid value %q: %w", v, err)
	}
	if n < 0 || n > 255 {
		return 0, true, fmt.Errorf("value %d out of range [0, 255]", n)
	}
	return n, true, nil
}

// dieAfterFirstWrite wraps an io.Writer and terminates the process (via
// os.Exit) the instant its first Write to the underlying writer returns —
// after the real bytes have already reached the transport, so the caller of
// that Write (protocol.Writer's single internal goroutine, which never calls
// Write concurrently with itself) is guaranteed the frame was fully handed
// off before the process disappears. In the intended usage (ACP always
// initializes before anything else), that first outbound frame is the
// "initialize" response, so this is "die right after answering initialize"
// with no arbitrary sleep and no race.
type dieAfterFirstWrite struct {
	w     io.Writer
	fired bool
}

func (d *dieAfterFirstWrite) Write(p []byte) (int, error) {
	n, err := d.w.Write(p)
	if !d.fired {
		d.fired = true
		os.Exit(exitDiedAfterInit)
	}
	return n, err
}

// Close forwards to the underlying writer if it is closeable, so wrapping
// for ACP_MOCK_DIE_AFTER_INIT never changes ordinary shutdown behavior for
// the (unreachable, since the process has already exited) case where it
// would otherwise matter.
func (d *dieAfterFirstWrite) Close() error {
	if c, ok := d.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
