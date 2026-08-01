package launch

import (
	"errors"
	"reflect"
	"testing"

	"github.com/looprig/acp/transport/stdio"
)

func TestClaudeConnectorConfigureSetsExactContract(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "primary", Small: "small"})

	cmd := stdio.Command{
		Path: "/opt/claude-agent-acp/bin/claude-agent-acp",
		Args: []string{"--should-be-discarded"},
		Env:  []string{"PATH=/usr/bin"},
		Dir:  "/work",
	}
	binding := ProxyBinding{BaseURL: "http://127.0.0.1:4141", Token: "gw-token-abc"}

	out, err := c.Configure(cmd, binding)
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}

	if out.Path != cmd.Path {
		t.Errorf("Path = %q, want the supplied absolute path %q unchanged", out.Path, cmd.Path)
	}
	if len(out.Args) != 0 {
		t.Errorf("Args = %v, want an empty argument list", out.Args)
	}
	if out.Dir != cmd.Dir {
		t.Errorf("Dir = %q, want %q unchanged", out.Dir, cmd.Dir)
	}

	env := envMap(out.Env)
	if env["ANTHROPIC_BASE_URL"] != binding.BaseURL {
		t.Errorf("ANTHROPIC_BASE_URL = %q, want %q", env["ANTHROPIC_BASE_URL"], binding.BaseURL)
	}
	if env["ANTHROPIC_AUTH_TOKEN"] != binding.Token {
		t.Errorf("ANTHROPIC_AUTH_TOKEN = %q, want %q", env["ANTHROPIC_AUTH_TOKEN"], binding.Token)
	}
	if _, present := env["CLAUDE_CODE_EXECUTABLE"]; present {
		t.Errorf("CLAUDE_CODE_EXECUTABLE present = %v, want absent when CLIPath is unset", present)
	}
	if _, present := env["CLAUDECODE"]; present {
		t.Errorf("CLAUDECODE present in configured env, want always absent")
	}
	if env["PATH"] != "/usr/bin" {
		t.Errorf("PATH = %q, want the unrelated allowlisted entry preserved", env["PATH"])
	}

	// The original Command must never be mutated by Configure.
	if cmd.Args[0] != "--should-be-discarded" {
		t.Errorf("original cmd.Args mutated: %v", cmd.Args)
	}
	if len(cmd.Env) != 1 || cmd.Env[0] != "PATH=/usr/bin" {
		t.Errorf("original cmd.Env mutated: %v", cmd.Env)
	}
}

func TestClaudeConnectorConfigureSetsOptionalCLIPath(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "primary"})
	c.CLIPath = "/usr/local/bin/claude"

	out, err := c.Configure(stdio.Command{Path: "/opt/claude-agent-acp"}, ProxyBinding{BaseURL: "http://x", Token: "t"})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}

	env := envMap(out.Env)
	if env["CLAUDE_CODE_EXECUTABLE"] != c.CLIPath {
		t.Errorf("CLAUDE_CODE_EXECUTABLE = %q, want %q", env["CLAUDE_CODE_EXECUTABLE"], c.CLIPath)
	}
}

func TestClaudeConnectorConfigureWithoutProxyOmitsGatewayOverrides(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "primary", Small: "small"})

	out, err := c.configureWithoutProxy(stdio.Command{
		Path: "/opt/claude-agent-acp",
		Env:  []string{"PATH=/usr/bin", "LANG=C"},
	})
	if err != nil {
		t.Fatalf("ConfigureWithoutProxy() error = %v", err)
	}
	if got := envMap(out.Env); len(got) != 2 || got["PATH"] != "/usr/bin" || got["LANG"] != "C" {
		t.Fatalf("native env = %#v, want only caller environment", got)
	}
	if len(out.Args) != 0 {
		t.Fatalf("native args = %#v, want empty", out.Args)
	}
}

func TestClaudeConnectorConfigureWithoutProxyRetainsValidationAndDenylist(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "primary"})

	_, err := c.configureWithoutProxy(stdio.Command{Path: "claude-agent-acp"})
	var pathErr *PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("ConfigureWithoutProxy() error = %v (%T), want *PathError", err, err)
	}

	for _, key := range []string{"CLAUDECODE", "ANTHROPIC_BASE_URL", "ANTHROPIC_AUTH_TOKEN"} {
		t.Run(key, func(t *testing.T) {
			_, err := c.configureWithoutProxy(stdio.Command{
				Path: "/opt/claude-agent-acp",
				Env:  []string{key + "=caller-value"},
			})
			var conflict *ConflictingEnvError
			if !errors.As(err, &conflict) {
				t.Fatalf("ConfigureWithoutProxy() error = %v (%T), want *ConflictingEnvError", err, err)
			}
			if conflict.Key != key {
				t.Fatalf("ConflictingEnvError.Key = %q, want %q", conflict.Key, key)
			}
		})
	}
}

func TestClaudeConnectorConfigureRejectsNonAbsolutePath(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "primary"})

	for name, cmd := range map[string]stdio.Command{
		"empty":        {Path: ""},
		"relative":     {Path: "claude-agent-acp"},
		"not-cleaned":  {Path: "/opt/../opt/claude-agent-acp"},
		"trailing-sep": {Path: "/opt/claude-agent-acp/"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := c.Configure(cmd, ProxyBinding{BaseURL: "http://x", Token: "t"})
			var pathErr *PathError
			if !errors.As(err, &pathErr) {
				t.Fatalf("Configure() error = %v (%T), want *PathError", err, err)
			}
		})
	}
}

func TestClaudeConnectorConfigureRejectsNonAbsoluteCLIPath(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "primary"})
	c.CLIPath = "claude"

	_, err := c.Configure(stdio.Command{Path: "/opt/claude-agent-acp"}, ProxyBinding{BaseURL: "http://x", Token: "t"})
	var pathErr *PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("Configure() error = %v (%T), want *PathError", err, err)
	}
	if pathErr.Field != "CLIPath" {
		t.Errorf("PathError.Field = %q, want %q", pathErr.Field, "CLIPath")
	}
}

func TestClaudeConnectorConfigureRejectsPreexistingCLAUDECODE(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "primary"})

	cmd := stdio.Command{
		Path: "/opt/claude-agent-acp",
		Env:  []string{"CLAUDECODE=1"},
	}

	_, err := c.Configure(cmd, ProxyBinding{BaseURL: "http://x", Token: "t"})
	var conflict *ConflictingEnvError
	if !errors.As(err, &conflict) {
		t.Fatalf("Configure() error = %v (%T), want *ConflictingEnvError (CLAUDECODE must be rejected, never silently stripped)", err, err)
	}
	if conflict.Key != "CLAUDECODE" {
		t.Errorf("ConflictingEnvError.Key = %q, want %q", conflict.Key, "CLAUDECODE")
	}
}

func TestClaudeCodeConstructorStoresModels(t *testing.T) {
	models := ClaudeModels{Default: "primary", Small: "small"}
	c := ClaudeCode(models)
	if !reflect.DeepEqual(c.Models, models) {
		t.Errorf("Models = %+v, want %+v", c.Models, models)
	}
	if c.CLIPath != "" {
		t.Errorf("CLIPath = %q, want empty by default", c.CLIPath)
	}
}

// envMap parses "key=value" Env entries into a map for assertions that
// don't care about entry order.
func envMap(env []string) map[string]string {
	m := make(map[string]string, len(env))
	for _, kv := range env {
		for i := 0; i < len(kv); i++ {
			if kv[i] == '=' {
				m[kv[:i]] = kv[i+1:]
				break
			}
		}
	}
	return m
}
