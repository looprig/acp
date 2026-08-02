//go:build integration && (darwin || (linux && !android))

// This file exercises Spawn across a real process boundary: it re-execs the
// test binary itself (via os.Executable, restricted to one -test.run target)
// as the child, so no separate fixture binary needs to be built. That keeps
// the tests self-contained while still spawning genuine OS processes with
// real process groups, signals, and stderr — the thing unit tests (which
// never cross a process boundary) cannot exercise.
package stdio

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
)

const helperStartupTimeout = 10 * time.Second

// selfExecCommand builds a Command that re-invokes this test binary,
// restricted to running only the named helper test function, with env as its
// complete (never-ambient) environment.
func selfExecCommand(t *testing.T, testName string, env []string, dir string) Command {
	t.Helper()
	execPath, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	execPath, err = filepath.Abs(execPath)
	if err != nil {
		t.Fatalf("absolute test binary path: %v", err)
	}
	if dir == "" {
		dir = t.TempDir()
	}
	return Command{
		Path: execPath,
		Args: []string{"-test.run=^" + testName + "$"},
		Env:  env,
		Dir:  dir,
	}
}

// --- cooperative helper: echoes NDJSON lines and exits promptly on SIGINT. ---

func TestCooperativeEchoHelper(t *testing.T) {
	if os.Getenv("GO_WANT_STDIO_COOPERATIVE_HELPER") != "1" {
		return
	}
	fmt.Fprintln(os.Stderr, "cooperative helper: diagnostic noise, never protocol bytes")
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			fmt.Fprintln(os.Stdout, scanner.Text())
		}
	}()

	select {
	case <-interrupt:
	case <-done:
	}
	os.Exit(0)
}

func TestSpawnCleanShutdownOfCooperativeChild(t *testing.T) {
	t.Parallel()
	cmd := selfExecCommand(t, "TestCooperativeEchoHelper", []string{
		"GO_WANT_STDIO_COOPERATIVE_HELPER=1",
	}, "")

	ctx, cancel := context.WithTimeout(context.Background(), helperStartupTimeout)
	defer cancel()
	proc, err := Spawn(ctx, cmd)
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}

	conn := protocol.NewConn(proc.Stdout, proc.Stdin, protocol.ConnOptions{})
	defer func() { _ = conn.Close() }()

	// Round-trip a notification through the child's echo to prove the
	// transport is carrying protocol bytes end-to-end, stdout-only (the
	// child's diagnostic stderr write above must never appear on this path).
	received := make(chan struct{})
	conn.HandleNotify("ping", func(context.Context, string, json.RawMessage) {
		close(received)
	})
	if err := conn.Notify(context.Background(), "ping", nil); err != nil {
		t.Fatalf("Notify() error = %v", err)
	}
	select {
	case <-received:
	case <-time.After(helperStartupTimeout):
		t.Fatal("timed out waiting for echoed notification round-trip")
	}

	start := time.Now()
	if err := proc.Kill(); err != nil {
		t.Fatalf("Kill() error = %v", err)
	}
	elapsed := time.Since(start)
	if elapsed >= closeGrace {
		t.Fatalf("Kill() took %v for a cooperative child, want well under the %v grace period", elapsed, closeGrace)
	}

	if err := proc.Wait(); err != nil {
		t.Fatalf("Wait() error = %v, want nil for a clean SIGINT exit", err)
	}
}

// --- stubborn helper: ignores SIGINT/SIGTERM and spawns a grandchild that
// does too, so only a process-group-wide SIGKILL reaches either of them. ---

func TestStubbornGroupHelper(t *testing.T) {
	if os.Getenv("GO_WANT_STDIO_STUBBORN_HELPER") != "1" {
		return
	}
	signal.Ignore(syscall.SIGINT, syscall.SIGTERM)

	// #nosec G204 -- /bin/sh -c with a fixed, literal script; no external input.
	child := exec.Command("/bin/sh", "-c", `trap '' INT TERM; printf ready > "$CHILD_READY_FILE"; exec /bin/sleep 300`)
	child.Stdout = nil
	child.Stderr = nil
	if err := child.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start grandchild: %v\n", err)
		os.Exit(2)
	}
	if err := os.WriteFile(os.Getenv("CHILD_PID_FILE"), []byte(strconv.Itoa(child.Process.Pid)), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "write grandchild pid: %v\n", err)
		os.Exit(2)
	}

	fmt.Fprintln(os.Stdout, `{"jsonrpc":"2.0","method":"ready","params":null}`)
	// Not select{}: with signals ignored and nothing else runnable, the Go
	// runtime's deadlock detector would kill the process itself almost
	// immediately, defeating the point of this helper (it must survive until
	// a real SIGKILL reaches it).
	time.Sleep(10 * time.Minute)
}

func TestSpawnKillEscalatesToSIGKILLOnStubbornGroup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	childPIDFile := filepath.Join(dir, "child.pid")
	childReadyFile := filepath.Join(dir, "child.ready")
	cmd := selfExecCommand(t, "TestStubbornGroupHelper", []string{
		"GO_WANT_STDIO_STUBBORN_HELPER=1",
		"CHILD_PID_FILE=" + childPIDFile,
		"CHILD_READY_FILE=" + childReadyFile,
	}, dir)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	proc, err := Spawn(ctx, cmd)
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	leaderPID := proc.cmd.Process.Pid

	waitForFile(t, childReadyFile)
	grandchildPID := readPIDFile(t, childPIDFile)

	// This leader ignores SIGINT/SIGTERM itself, so — unlike the cooperative
	// test — Kill escalates all the way to SIGKILLing the leader too: its
	// Wait outcome is genuinely "killed," not a bug. Kill and Wait must agree
	// (Kill's teardown ends by calling Wait itself).
	start := time.Now()
	killErr := proc.Kill()
	elapsed := time.Since(start)
	if elapsed < closeGrace {
		t.Fatalf("Kill() returned after %v, want at least the %v grace period for a stubborn child", elapsed, closeGrace)
	}
	var exitErr *ExitError
	if !errors.As(killErr, &exitErr) {
		t.Fatalf("Kill() error = %T %v, want *ExitError for a SIGKILLed leader", killErr, killErr)
	}

	if waitErr := proc.Wait(); waitErr.Error() != killErr.Error() {
		t.Fatalf("Wait() = %v, want the same cached result as Kill() = %v", waitErr, killErr)
	}

	// No zombie: once reaped, the leader pid must be fully gone, not merely a
	// zombie entry (which would still answer kill(pid, 0) as ESRCH-free).
	assertProcessGone(t, leaderPID)
	// The group-wide SIGKILL must reach the grandchild too, not just the
	// direct leader.
	assertProcessGone(t, grandchildPID)
}

func TestSpawnKillIsIdempotent(t *testing.T) {
	t.Parallel()
	cmd := selfExecCommand(t, "TestCooperativeEchoHelper", []string{
		"GO_WANT_STDIO_COOPERATIVE_HELPER=1",
	}, "")

	ctx, cancel := context.WithTimeout(context.Background(), helperStartupTimeout)
	defer cancel()
	proc, err := Spawn(ctx, cmd)
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = proc.Kill()
		}(i)
	}
	wg.Wait()
	for i, err := range errs[1:] {
		if !errorsEqual(err, errs[0]) {
			t.Fatalf("concurrent Kill()[%d] = %v, want the same result as Kill()[0] = %v", i+1, err, errs[0])
		}
	}

	// Wait called again after Kill must return the identical cached result,
	// never re-invoke the underlying (single-call-only) exec.Cmd.Wait.
	if err := proc.Wait(); !errorsEqual(err, errs[0]) {
		t.Fatalf("Wait() after Kill() = %v, want %v", err, errs[0])
	}
}

func TestSpawnCancellationRacesWithProcessGroupSetup(t *testing.T) {
	t.Parallel()
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		proc, err := Spawn(ctx, selfExecCommand(t, "TestCooperativeEchoHelper", []string{
			"GO_WANT_STDIO_COOPERATIVE_HELPER=1",
		}, ""))
		cancel()
		if err != nil {
			continue
		}
		if err := proc.Kill(); err != nil {
			var exitErr *ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("Kill() error = %v, want nil or *ExitError", err)
			}
		}
	}
}

func TestSpawnEnvIsNeverAmbient(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	outFile := filepath.Join(dir, "env.count")
	cmd := selfExecCommand(t, "TestEnvDumpHelper", []string{
		"GO_WANT_STDIO_ENVDUMP_HELPER=1",
		"ENV_OUT_FILE=" + outFile,
	}, dir)

	ctx, cancel := context.WithTimeout(context.Background(), helperStartupTimeout)
	defer cancel()
	proc, err := Spawn(ctx, cmd)
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	if err := proc.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	raw, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read env dump: %v", err)
	}
	count, convErr := strconv.Atoi(strings.TrimSpace(string(raw)))
	if convErr != nil {
		t.Fatalf("parse env dump %q: %v", raw, convErr)
	}
	// Exactly the two entries this test explicitly whitelisted above (plus
	// none of this test process's own, much larger, ambient environment).
	if count != 2 {
		t.Fatalf("child saw %d environment entries, want exactly 2 (never the ambient environment)", count)
	}
}

func TestEnvDumpHelper(t *testing.T) {
	if os.Getenv("GO_WANT_STDIO_ENVDUMP_HELPER") != "1" {
		return
	}
	if err := os.WriteFile(os.Getenv("ENV_OUT_FILE"), []byte(strconv.Itoa(len(os.Environ()))), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "write env dump: %v\n", err)
		os.Exit(2)
	}
	os.Exit(0)
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(helperStartupTimeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func readPIDFile(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("parse pid file %q: %v", raw, err)
	}
	return pid
}

func assertProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("pid %d still exists (zombie or alive) after Kill", pid)
}

func errorsEqual(a, b error) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Error() == b.Error()
}
