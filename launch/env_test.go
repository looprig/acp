package launch

import (
	"errors"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/looprig/acp/transport/stdio"
)

func TestBuildChildCommandDeepCopiesEnvAndArgs(t *testing.T) {
	original := stdio.Command{
		Path: "/bin/x",
		Args: []string{"orig-arg"},
		Env:  []string{"PATH=/usr/bin"},
		Dir:  "/work",
	}

	out, err := buildChildCommand(original, []envOverride{{Key: "FOO", Value: "bar"}}, nil)
	if err != nil {
		t.Fatalf("buildChildCommand() error = %v", err)
	}

	// Mutating the result must never affect the original Command's slices.
	out.Env[0] = "MUTATED"
	out.Args[0] = "MUTATED"
	if original.Env[0] != "PATH=/usr/bin" {
		t.Errorf("original.Env mutated via the returned Command's backing array: %v", original.Env)
	}
	if original.Args[0] != "orig-arg" {
		t.Errorf("original.Args mutated via the returned Command's backing array: %v", original.Args)
	}

	// Mutating the original afterward must never affect a result already
	// returned.
	out2, err := buildChildCommand(original, nil, nil)
	if err != nil {
		t.Fatalf("buildChildCommand() error = %v", err)
	}
	original.Env[0] = "MUTATED-AGAIN"
	original.Args[0] = "MUTATED-AGAIN"
	if out2.Env[0] != "PATH=/usr/bin" {
		t.Errorf("out2.Env mutated via the original Command's backing array: %v", out2.Env)
	}
	if out2.Args[0] != "orig-arg" {
		t.Errorf("out2.Args mutated via the original Command's backing array: %v", out2.Args)
	}
}

func TestBuildChildCommandRejectsForbiddenPresent(t *testing.T) {
	cmd := stdio.Command{
		Path: "/bin/x",
		Env:  []string{"CLAUDECODE=1", "PATH=/usr/bin"},
	}

	_, err := buildChildCommand(cmd, []envOverride{{Key: "ANTHROPIC_BASE_URL", Value: "http://x"}}, []string{"CLAUDECODE"})
	var conflict *ConflictingEnvError
	if !errors.As(err, &conflict) {
		t.Fatalf("buildChildCommand() error = %v (%T), want *ConflictingEnvError", err, err)
	}
	if conflict.Key != "CLAUDECODE" {
		t.Errorf("ConflictingEnvError.Key = %q, want %q", conflict.Key, "CLAUDECODE")
	}
}

func TestBuildChildCommandAllowsForbiddenNameAbsent(t *testing.T) {
	cmd := stdio.Command{Path: "/bin/x", Env: []string{"PATH=/usr/bin"}}

	if _, err := buildChildCommand(cmd, nil, []string{"CLAUDECODE"}); err != nil {
		t.Fatalf("buildChildCommand() error = %v, want nil when the forbidden name is absent", err)
	}
}

func TestBuildChildCommandReplacesExistingOverrideKeyInPlace(t *testing.T) {
	cmd := stdio.Command{
		Path: "/bin/x",
		Env:  []string{"ANTHROPIC_BASE_URL=http://old", "PATH=/usr/bin"},
	}

	out, err := buildChildCommand(cmd, []envOverride{{Key: "ANTHROPIC_BASE_URL", Value: "http://new"}}, nil)
	if err != nil {
		t.Fatalf("buildChildCommand() error = %v", err)
	}

	got := sortedCopy(out.Env)
	want := sortedCopy([]string{"ANTHROPIC_BASE_URL=http://new", "PATH=/usr/bin"})
	if !reflect.DeepEqual(got, want) {
		t.Errorf("out.Env = %v, want %v (replaced in place, never duplicated)", got, want)
	}
}

func TestBuildChildCommandPreservesUnrelatedEntries(t *testing.T) {
	cmd := stdio.Command{
		Path: "/bin/x",
		Env:  []string{"PATH=/usr/bin", "HOME=/root", "LANG=C"},
	}

	out, err := buildChildCommand(cmd, []envOverride{{Key: "ANTHROPIC_BASE_URL", Value: "http://x"}, {Key: "ANTHROPIC_AUTH_TOKEN", Value: "tok"}}, nil)
	if err != nil {
		t.Fatalf("buildChildCommand() error = %v", err)
	}

	got := sortedCopy(out.Env)
	want := sortedCopy([]string{"PATH=/usr/bin", "HOME=/root", "LANG=C", "ANTHROPIC_BASE_URL=http://x", "ANTHROPIC_AUTH_TOKEN=tok"})
	if !reflect.DeepEqual(got, want) {
		t.Errorf("out.Env = %v, want %v (unrelated entries preserved verbatim)", got, want)
	}
}

func TestBuildChildCommandNeverInspectsAmbientEnvironment(t *testing.T) {
	const ambientKey = "ACP_LAUNCH_ENV_TEST_AMBIENT_ONLY"
	t.Setenv(ambientKey, "should-never-appear")

	cmd := stdio.Command{Path: "/bin/x"} // no Env at all

	out, err := buildChildCommand(cmd, []envOverride{{Key: "ANTHROPIC_BASE_URL", Value: "http://x"}}, nil)
	if err != nil {
		t.Fatalf("buildChildCommand() error = %v", err)
	}

	for _, kv := range out.Env {
		if len(kv) >= len(ambientKey) && kv[:len(ambientKey)] == ambientKey {
			t.Fatalf("out.Env contains the ambient-only variable %q: %v", ambientKey, out.Env)
		}
	}
	if len(out.Env) != 1 {
		t.Fatalf("out.Env = %v, want exactly the one override (no ambient leakage, no os.Environ() pass-through)", out.Env)
	}

	// Belt and suspenders: confirm this process really does have the
	// ambient variable set, so the assertion above is meaningful rather
	// than vacuous.
	if v, ok := os.LookupEnv(ambientKey); !ok || v != "should-never-appear" {
		t.Fatalf("test setup failed: ambient env var not actually set (%q, %v)", v, ok)
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
