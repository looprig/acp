// version_test.go proves ProbeCodexVersion's classification against an
// injectable CodexVersionRunner -- no real codex-acp binary (or any other
// subprocess) is ever spawned here.
package launch

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeCodexVersionRunner returns a CodexVersionRunner that always returns
// stdout/err, optionally recording whether it was called.
func fakeCodexVersionRunner(stdout string, err error, called *bool) CodexVersionRunner {
	return func(ctx context.Context, path string) ([]byte, error) {
		if called != nil {
			*called = true
		}
		return []byte(stdout), err
	}
}

func TestProbeCodexVersionClassifiesModern(t *testing.T) {
	runner := fakeCodexVersionRunner("@agentclientprotocol/codex-acp 1.1.7", nil, nil)

	result, err := ProbeCodexVersion(context.Background(), "/opt/codex-acp", time.Second, runner)
	if err != nil {
		t.Fatalf("ProbeCodexVersion() error = %v, want nil for a modern version", err)
	}
	if result.Class != CodexVersionModern {
		t.Errorf("Class = %v, want %v", result.Class, CodexVersionModern)
	}
	if want := (CodexVersion{Major: 1, Minor: 1, Patch: 7}); result.Version != want {
		t.Errorf("Version = %+v, want %+v", result.Version, want)
	}
}

func TestProbeCodexVersionClassifiesModernAboveMinimum(t *testing.T) {
	runner := fakeCodexVersionRunner("@agentclientprotocol/codex-acp 2.0.0", nil, nil)

	result, err := ProbeCodexVersion(context.Background(), "/opt/codex-acp", time.Second, runner)
	if err != nil {
		t.Fatalf("ProbeCodexVersion() error = %v, want nil", err)
	}
	if result.Class != CodexVersionModern {
		t.Errorf("Class = %v, want %v", result.Class, CodexVersionModern)
	}
}

func TestProbeCodexVersionClassifiesBelowMinimum(t *testing.T) {
	runner := fakeCodexVersionRunner("@agentclientprotocol/codex-acp 1.1.6", nil, nil)

	result, err := ProbeCodexVersion(context.Background(), "/opt/codex-acp", time.Second, runner)
	var verErr *CodexVersionError
	if !errors.As(err, &verErr) {
		t.Fatalf("ProbeCodexVersion() error = %v (%T), want *CodexVersionError", err, err)
	}
	if result.Class != CodexVersionBelowMinimum {
		t.Errorf("Class = %v, want %v", result.Class, CodexVersionBelowMinimum)
	}
	if want := (CodexVersion{Major: 1, Minor: 1, Patch: 6}); result.Version != want {
		t.Errorf("Version = %+v, want %+v", result.Version, want)
	}
	if verErr.Result.Class != CodexVersionBelowMinimum {
		t.Errorf("CodexVersionError.Result.Class = %v, want %v", verErr.Result.Class, CodexVersionBelowMinimum)
	}
}

func TestProbeCodexVersionClassifiesLegacyNoVersion(t *testing.T) {
	runner := fakeCodexVersionRunner("", nil, nil)

	result, err := ProbeCodexVersion(context.Background(), "/opt/codex-acp", time.Second, runner)
	var verErr *CodexVersionError
	if !errors.As(err, &verErr) {
		t.Fatalf("ProbeCodexVersion() error = %v (%T), want *CodexVersionError", err, err)
	}
	if result.Class != CodexVersionLegacyNoVersion {
		t.Errorf("Class = %v, want %v", result.Class, CodexVersionLegacyNoVersion)
	}
}

func TestProbeCodexVersionClassifiesUnparseable(t *testing.T) {
	for name, raw := range map[string]string{
		"partial version":     "@agentclientprotocol/codex-acp 1.2",
		"prerelease suffix":   "@agentclientprotocol/codex-acp 1.2.0-rc1",
		"non numeric":         "@agentclientprotocol/codex-acp a.b.c",
		"unrelated text":      "codex-acp: dev build, no version info",
		"too many components": "@agentclientprotocol/codex-acp 1.2.3.4",
	} {
		t.Run(name, func(t *testing.T) {
			runner := fakeCodexVersionRunner(raw, nil, nil)
			result, err := ProbeCodexVersion(context.Background(), "/opt/codex-acp", time.Second, runner)
			var verErr *CodexVersionError
			if !errors.As(err, &verErr) {
				t.Fatalf("ProbeCodexVersion() error = %v (%T), want *CodexVersionError", err, err)
			}
			if result.Class != CodexVersionUnparseable {
				t.Errorf("Class = %v, want %v", result.Class, CodexVersionUnparseable)
			}
		})
	}
}

func TestProbeCodexVersionClassifiesNonzeroExit(t *testing.T) {
	runErr := errors.New("exit status 1")
	runner := fakeCodexVersionRunner("", runErr, nil)

	result, err := ProbeCodexVersion(context.Background(), "/opt/codex-acp", time.Second, runner)
	var verErr *CodexVersionError
	if !errors.As(err, &verErr) {
		t.Fatalf("ProbeCodexVersion() error = %v (%T), want *CodexVersionError", err, err)
	}
	if result.Class != CodexVersionNonzeroExit {
		t.Errorf("Class = %v, want %v", result.Class, CodexVersionNonzeroExit)
	}
}

func TestProbeCodexVersionClassifiesNonzeroExitEvenWithVersionLikeOutput(t *testing.T) {
	// A process that both prints something version-shaped AND fails must
	// still be rejected: a nonzero exit always dominates, regardless of
	// what happened to reach stdout first.
	runErr := errors.New("exit status 1")
	runner := fakeCodexVersionRunner("@agentclientprotocol/codex-acp 9.9.9", runErr, nil)

	result, err := ProbeCodexVersion(context.Background(), "/opt/codex-acp", time.Second, runner)
	if err == nil {
		t.Fatal("ProbeCodexVersion() error = nil, want a rejection despite version-shaped output")
	}
	if result.Class != CodexVersionNonzeroExit {
		t.Errorf("Class = %v, want %v", result.Class, CodexVersionNonzeroExit)
	}
}

func TestProbeCodexVersionClassifiesTimeout(t *testing.T) {
	blocking := func(ctx context.Context, path string) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	result, err := ProbeCodexVersion(context.Background(), "/opt/codex-acp", 20*time.Millisecond, blocking)
	var verErr *CodexVersionError
	if !errors.As(err, &verErr) {
		t.Fatalf("ProbeCodexVersion() error = %v (%T), want *CodexVersionError", err, err)
	}
	if result.Class != CodexVersionTimeout {
		t.Errorf("Class = %v, want %v", result.Class, CodexVersionTimeout)
	}
}

func TestProbeCodexVersionNonPositiveTimeoutStillRunsProbe(t *testing.T) {
	blocked := make(chan struct{})
	runner := func(ctx context.Context, path string) ([]byte, error) {
		close(blocked)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	// A non-positive timeout must fall back to
	// DefaultCodexVersionProbeTimeout (5s) rather than rejecting before
	// ever invoking the runner or hanging forever; proving the exact
	// bound numerically would make this test slow, so this only proves
	// the runner was actually invoked (the probe did not short-circuit)
	// and cancels the outer context ourselves once that is observed, to
	// keep the test fast.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-blocked
		cancel()
	}()

	_, err := ProbeCodexVersion(ctx, "/opt/codex-acp", 0, runner)
	if err == nil {
		t.Fatal("ProbeCodexVersion() error = nil, want a rejection once the context was canceled")
	}
}

func TestProbeCodexVersionRejectsNonAbsolutePath(t *testing.T) {
	called := false
	runner := fakeCodexVersionRunner("@agentclientprotocol/codex-acp 1.1.7", nil, &called)

	for name, path := range map[string]string{
		"empty":       "",
		"relative":    "codex-acp",
		"not-cleaned": "/opt/../opt/codex-acp",
	} {
		t.Run(name, func(t *testing.T) {
			called = false
			_, err := ProbeCodexVersion(context.Background(), path, time.Second, runner)
			var pathErr *PathError
			if !errors.As(err, &pathErr) {
				t.Fatalf("ProbeCodexVersion() error = %v (%T), want *PathError", err, err)
			}
			if called {
				t.Error("runner was invoked despite an invalid path, want the probe to fail closed before ever running it")
			}
		})
	}
}

func TestProbeCodexVersionNilRunnerUsesDefaultAgainstMissingBinary(t *testing.T) {
	// No fake substituted: exercises the real defaultCodexVersionRunner
	// end to end, but against a path that cannot possibly exist, so this
	// never shells out to any real adapter binary (codex-acp or
	// otherwise) -- only proves defaultCodexVersionRunner/exec wiring
	// itself fails closed rather than panicking or hanging.
	result, err := ProbeCodexVersion(context.Background(), "/nonexistent/absolute/path/codex-acp-does-not-exist", 2*time.Second, nil)
	if err == nil {
		t.Fatal("ProbeCodexVersion() error = nil, want a rejection for a nonexistent binary")
	}
	if result.Class != CodexVersionNonzeroExit {
		t.Errorf("Class = %v, want %v", result.Class, CodexVersionNonzeroExit)
	}
}

func TestCodexVersionLessOrdersMajorMinorPatch(t *testing.T) {
	cases := []struct {
		a, b CodexVersion
		want bool
	}{
		{CodexVersion{1, 1, 6}, CodexVersion{1, 1, 7}, true},
		{CodexVersion{1, 1, 7}, CodexVersion{1, 1, 7}, false},
		{CodexVersion{1, 1, 8}, CodexVersion{1, 1, 7}, false},
		{CodexVersion{1, 0, 99}, CodexVersion{1, 1, 0}, true},
		{CodexVersion{0, 16, 0}, CodexVersion{1, 1, 7}, true},
		{CodexVersion{2, 0, 0}, CodexVersion{1, 1, 7}, false},
	}
	for _, c := range cases {
		if got := c.a.Less(c.b); got != c.want {
			t.Errorf("%+v.Less(%+v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}
