//go:build integration && (darwin || (linux && !android))

// version_integration_test.go proves the one thing version_test.go's fake
// runners cannot: that ProbeCodexVersion's *real* runner
// (defaultCodexVersionRunner, exec.CommandContext-based) actually kills and
// reaps a genuinely hung subprocess once its bounded timeout expires, rather
// than merely classifying a synthetic ctx.Done() the way the in-process fake
// blocking runner in version_test.go does.
//
// This follows transport/stdio/spawn_integration_test.go's established
// self-exec precedent -- re-exec the test binary itself as the child, no
// separate fixture binary -- with one necessary deviation: that precedent
// selects its helper via a "-test.run=^Name$" argument, because it calls
// Spawn directly and controls the child's argv. defaultCodexVersionRunner
// does not offer that seam: it always invokes exactly `<path> --version`,
// and an unrecognized "--version" flag would make the testing package's own
// flag.Parse() (inside m.Run()) abort the child immediately -- before any
// test body ever runs -- which would defeat the entire point of this test
// (it would "pass" by exiting fast, never by being killed for hanging).
// So the trigger here fires in a custom TestMain, ahead of m.Run() and thus
// ahead of flag.Parse(), gated by an env var exactly like the stdio
// precedent's helper processes (GO_WANT_STDIO_*_HELPER); see helperSleepEnv.
package launch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// helperSleepEnv, when set to "1" in the child's inherited environment,
// makes TestMain below record the child's own pid and then sleep far past
// any timeout this test applies, instead of running the package's tests.
const helperSleepEnv = "GO_WANT_LAUNCH_VERSION_SLEEP_HELPER"

// helperPIDFileEnv names the env var carrying the path the helper writes its
// own pid to, so the parent test can later prove that pid is fully gone
// (killed and reaped, not merely a zombie) once ProbeCodexVersion returns.
const helperPIDFileEnv = "LAUNCH_VERSION_SLEEP_PID_FILE"

// TestMain intercepts the self-exec'd helper invocation before the testing
// package ever parses flags. defaultCodexVersionRunner always execs the
// child as `<path> --version`; letting m.Run() see that argv would abort on
// an unrecognized "--version" flag, so the sleep helper must run and exit
// (or block) here, ahead of that parse.
func TestMain(m *testing.M) {
	if os.Getenv(helperSleepEnv) == "1" {
		if pidFile := os.Getenv(helperPIDFileEnv); pidFile != "" {
			if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
				os.Exit(2)
			}
		}
		// Not a channel/select{} deadlock trap: this process owns no other
		// goroutines, and a plain sleep survives cleanly until SIGKILL
		// arrives, which is exactly what must happen here.
		time.Sleep(2 * time.Minute)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestProbeCodexVersionKillsRealHungSubprocess proves defaultCodexVersionRunner
// -- exec.CommandContext against a genuinely hung child, not an in-process
// fake -- actually kills and reaps that child once ProbeCodexVersion's bound
// expires, and that the classification surfacing that is CodexVersionTimeout.
func TestProbeCodexVersionKillsRealHungSubprocess(t *testing.T) {
	execPath, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	execPath, err = filepath.Abs(execPath)
	if err != nil {
		t.Fatalf("absolute test binary path: %v", err)
	}

	dir := t.TempDir()
	pidFile := filepath.Join(dir, "helper.pid")

	// defaultCodexVersionRunner's signature (ctx, path) leaves no seam for
	// injecting the child's environment or args, so the self-exec trigger
	// and pid-file path have to ride the parent's own environment: cmd.Env
	// is left nil inside defaultCodexVersionRunner, and os/exec inherits the
	// full parent environment whenever Env is nil.
	for k, v := range map[string]string{
		helperSleepEnv:   "1",
		helperPIDFileEnv: pidFile,
	} {
		if err := os.Setenv(k, v); err != nil {
			t.Fatalf("Setenv(%s): %v", k, err)
		}
	}
	t.Cleanup(func() {
		_ = os.Unsetenv(helperSleepEnv)
		_ = os.Unsetenv(helperPIDFileEnv)
	})

	// Bounded, but generous enough that a freshly exec'd copy of this test
	// binary reliably starts and writes its own pid file well within it,
	// even on a loaded CI host -- this is proving a kill happens, not
	// measuring how fast one does.
	const probeTimeout = 2 * time.Second

	start := time.Now()
	result, probeErr := ProbeCodexVersion(context.Background(), execPath, probeTimeout, nil)
	elapsed := time.Since(start)

	if result.Class != CodexVersionTimeout {
		t.Fatalf("Class = %v (err = %v), want %v", result.Class, probeErr, CodexVersionTimeout)
	}
	var verErr *CodexVersionError
	if !errors.As(probeErr, &verErr) {
		t.Fatalf("ProbeCodexVersion() error = %v (%T), want *CodexVersionError", probeErr, probeErr)
	}

	// ProbeCodexVersion must return promptly once its own bound expires, not
	// merely eventually: defaultCodexVersionRunner's cmd.Output() call must
	// actually have killed and Waited on the child by the time it returns,
	// rather than leaking a goroutine that reaps it sometime later.
	if elapsed > 10*time.Second {
		t.Fatalf("ProbeCodexVersion took %v to return after a %v bound, want it to return promptly once its hung child is killed", elapsed, probeTimeout)
	}

	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("read helper pid file: %v (helper may never have started)", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("parse helper pid file %q: %v", raw, err)
	}

	// This is the actual proof of real process death, not just a
	// classification label: poll the OS itself for the helper's pid to be
	// fully gone -- ESRCH, not merely a zombie entry (which would still
	// answer kill(pid, 0) as ESRCH-free) -- so a regression that silently
	// stopped killing (or stopped reaping) the process would fail this test
	// even if classifyCodexVersion still happened to say "timeout".
	assertProcessGone(t, pid)
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
	t.Fatalf("pid %d still exists (zombie or alive) after ProbeCodexVersion's timeout killed it", pid)
}
