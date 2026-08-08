package launch

import (
	"errors"
	"testing"

	"github.com/looprig/acp/transport/stdio"
)

func TestCodexConnectorConfigureSetsExactContract(t *testing.T) {
	c := Codex("gpt-5-codex")

	cmd := stdio.Command{
		Path: "/opt/codex-acp/bin/codex-acp",
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
	if out.Dir != cmd.Dir {
		t.Errorf("Dir = %q, want %q unchanged", out.Dir, cmd.Dir)
	}

	want := []string{
		"-c", "model=gpt-5-codex",
		"-c", "model_provider=looprig",
		"-c", "model_providers.looprig.base_url=http://127.0.0.1:4141/v1",
		"-c", "model_providers.looprig.env_key=LOOPRIG_PROXY_TOKEN",
		"-c", `model_providers.looprig.wire_api="responses"`,
		"-c", "model_providers.looprig.requires_openai_auth=false",
		"-c", "approval_policy=on-request",
		"-c", "sandbox_mode=workspace-write",
		"-c", "sandbox_workspace_write.network_access=false",
	}
	if !equalStrings(out.Args, want) {
		t.Fatalf("Args = %#v, want %#v", out.Args, want)
	}

	env := envMap(out.Env)
	if env["LOOPRIG_PROXY_TOKEN"] != binding.Token {
		t.Errorf("LOOPRIG_PROXY_TOKEN = %q, want %q", env["LOOPRIG_PROXY_TOKEN"], binding.Token)
	}
	if _, present := env["CODEX_HOME"]; present {
		t.Errorf("CODEX_HOME present in configured env, want always absent")
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

func TestCodexConnectorConfigureWithoutProxyOmitsGatewayOverrides(t *testing.T) {
	c := Codex("gpt-5-codex")

	out, err := c.configureWithoutProxy(stdio.Command{
		Path: "/opt/codex-acp",
		Env:  []string{"PATH=/usr/bin", "LANG=C"},
	})
	if err != nil {
		t.Fatalf("ConfigureWithoutProxy() error = %v", err)
	}
	want := []string{
		"-c", "model=gpt-5-codex",
		"-c", "approval_policy=on-request",
		"-c", "sandbox_mode=workspace-write",
		"-c", "sandbox_workspace_write.network_access=false",
	}
	if !equalStrings(out.Args, want) {
		t.Fatalf("native args = %#v, want %#v", out.Args, want)
	}
	env := envMap(out.Env)
	if len(env) != 2 || env["PATH"] != "/usr/bin" || env["LANG"] != "C" {
		t.Fatalf("native env = %#v, want only caller environment", env)
	}
}

func TestCodexConnectorConfigureWithoutProxyAllowsEmptyModel(t *testing.T) {
	c := Codex("")

	out, err := c.configureWithoutProxy(stdio.Command{
		Path: "/opt/codex-acp",
		Env:  []string{"PATH=/usr/bin", "LANG=C"},
	})
	if err != nil {
		t.Fatalf("ConfigureWithoutProxy() error = %v, want native configuration to allow an empty model", err)
	}
	want := []string{
		"-c", "approval_policy=on-request",
		"-c", "sandbox_mode=workspace-write",
		"-c", "sandbox_workspace_write.network_access=false",
	}
	if !equalStrings(out.Args, want) {
		t.Fatalf("native args = %#v, want %#v without a model or gateway provider overrides", out.Args, want)
	}
	for _, arg := range out.Args {
		if arg == "model=" || arg == "model_provider=looprig" || len(arg) >= len("model_providers.") && arg[:len("model_providers.")] == "model_providers." {
			t.Fatalf("native args contain gateway/model override %q: %#v", arg, out.Args)
		}
	}
	env := envMap(out.Env)
	if len(env) != 2 || env["PATH"] != "/usr/bin" || env["LANG"] != "C" {
		t.Fatalf("native env = %#v, want only caller environment", env)
	}
}

func TestCodexNativeEffortEmitsSeparateModelAndEffortOverrides(t *testing.T) {
	c := Codex("").WithModelEffort("gpt-5.6-sol", "medium")

	out, err := c.ConfigureNative(stdio.Command{Path: "/opt/codex-acp"})
	if err != nil {
		t.Fatalf("ConfigureNative() error = %v", err)
	}

	wantPrefix := []string{
		"-c", "model=gpt-5.6-sol",
		"-c", "model_reasoning_effort=medium",
	}
	if len(out.Args) < len(wantPrefix) || !equalStrings(out.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("native args = %#v, want separate model and effort overrides %v", out.Args, wantPrefix)
	}
	for _, arg := range out.Args {
		if arg == "model=gpt-5.6-sol:medium" || arg == "model=gpt-5.6-sol-medium" {
			t.Fatalf("native args combined model and effort into %q: %#v", arg, out.Args)
		}
	}
}

func TestCodexNativeOmittedModelAndEffortEmitNeitherOverride(t *testing.T) {
	c := Codex("").WithModelEffort("", "")

	out, err := c.ConfigureNative(stdio.Command{Path: "/opt/codex-acp"})
	if err != nil {
		t.Fatalf("ConfigureNative() error = %v, want omitted selectors to be a no-op", err)
	}
	for i := 0; i+1 < len(out.Args); i += 2 {
		if out.Args[i+1] == "model=" || out.Args[i+1] == "model_reasoning_effort=" || out.Args[i+1] == "model_reasoning_effort" {
			t.Fatalf("native args contain an omitted selector override %q: %#v", out.Args[i+1], out.Args)
		}
	}
}

func TestCodexNativePartialModelEffortSelectionReturnsTypedConfigError(t *testing.T) {
	for name, connector := range map[string]*CodexConnector{
		"model without effort": Codex("").WithModelEffort("gpt-5.6-sol", ""),
		"effort without model": Codex("").WithModelEffort("", "medium"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := connector.ConfigureNative(stdio.Command{Path: "/opt/codex-acp"})
			var cfgErr *ConfigError
			if !errors.As(err, &cfgErr) {
				t.Fatalf("ConfigureNative() error = %v (%T), want *ConfigError", err, err)
			}
		})
	}
}

func TestCodexConnectorConfigureWithoutProxyRetainsValidationAndDenylist(t *testing.T) {
	c := Codex("gpt-5-codex")

	_, err := c.configureWithoutProxy(stdio.Command{Path: "codex-acp"})
	var pathErr *PathError
	if !errors.As(err, &pathErr) {
		t.Fatalf("ConfigureWithoutProxy() error = %v (%T), want *PathError", err, err)
	}

	for _, key := range []string{"CODEX_HOME", "LOOPRIG_PROXY_TOKEN"} {
		t.Run(key, func(t *testing.T) {
			_, err := c.configureWithoutProxy(stdio.Command{
				Path: "/opt/codex-acp",
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

func TestCodexConnectorConfigureAppliesCustomPosture(t *testing.T) {
	c := Codex("gpt-5-codex")
	c.Posture = CodexPosture{
		ApprovalPolicy:       "never",
		SandboxMode:          "danger-full-access",
		SandboxNetworkAccess: true,
	}

	out, err := c.Configure(stdio.Command{Path: "/opt/codex-acp"}, ProxyBinding{BaseURL: "http://x", Token: "t"})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}

	want := []string{"approval_policy=never", "sandbox_mode=danger-full-access", "sandbox_workspace_write.network_access=true"}
	got := lastNPairValues(out.Args, 3)
	if !equalStrings(got, want) {
		t.Fatalf("posture args = %v, want %v", got, want)
	}
}

func TestCodexConnectorConfigureDefaultsEmptyPosture(t *testing.T) {
	c := Codex("gpt-5-codex")
	// c.Posture left at its zero value.

	out, err := c.Configure(stdio.Command{Path: "/opt/codex-acp"}, ProxyBinding{BaseURL: "http://x", Token: "t"})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}

	want := []string{"approval_policy=on-request", "sandbox_mode=workspace-write", "sandbox_workspace_write.network_access=false"}
	got := lastNPairValues(out.Args, 3)
	if !equalStrings(got, want) {
		t.Fatalf("posture args = %v, want %v", got, want)
	}

	// Defaults are applied fresh at Configure time, never baked back into
	// the connector's own stored Posture field.
	if c.Posture != (CodexPosture{}) {
		t.Errorf("c.Posture = %+v after Configure, want the zero value preserved (defaults must not be written back)", c.Posture)
	}
}

func TestCodexConnectorConfigureRejectsNonAbsolutePath(t *testing.T) {
	c := Codex("gpt-5-codex")

	for name, cmd := range map[string]stdio.Command{
		"empty":        {Path: ""},
		"relative":     {Path: "codex-acp"},
		"not-cleaned":  {Path: "/opt/../opt/codex-acp"},
		"trailing-sep": {Path: "/opt/codex-acp/"},
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

func TestCodexConnectorConfigureRejectsEmptyModel(t *testing.T) {
	c := Codex("")

	_, err := c.Configure(stdio.Command{Path: "/opt/codex-acp"}, ProxyBinding{BaseURL: "http://x", Token: "t"})
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("Configure() error = %v (%T), want *ConfigError", err, err)
	}
}

func TestCodexConnectorConfigureRejectsPreexistingCODEXHOME(t *testing.T) {
	c := Codex("gpt-5-codex")

	cmd := stdio.Command{
		Path: "/opt/codex-acp",
		Env:  []string{"CODEX_HOME=/home/x/.codex"},
	}

	_, err := c.Configure(cmd, ProxyBinding{BaseURL: "http://x", Token: "t"})
	var conflict *ConflictingEnvError
	if !errors.As(err, &conflict) {
		t.Fatalf("Configure() error = %v (%T), want *ConflictingEnvError (CODEX_HOME must be rejected, never silently stripped)", err, err)
	}
	if conflict.Key != "CODEX_HOME" {
		t.Errorf("ConflictingEnvError.Key = %q, want %q", conflict.Key, "CODEX_HOME")
	}
}

func TestCodexConnectorConfigureArgsOrderIsDeterministicAcrossCalls(t *testing.T) {
	c := Codex("gpt-5-codex")
	cmd := stdio.Command{Path: "/opt/codex-acp"}
	binding := ProxyBinding{BaseURL: "http://x", Token: "t"}

	first, err := c.Configure(cmd, binding)
	if err != nil {
		t.Fatalf("Configure() [1] error = %v", err)
	}
	for i := 0; i < 5; i++ {
		again, err := c.Configure(cmd, binding)
		if err != nil {
			t.Fatalf("Configure() [%d] error = %v", i+2, err)
		}
		if !equalStrings(again.Args, first.Args) {
			t.Fatalf("Args on call %d = %v, want the same deterministic order as call 1: %v", i+2, again.Args, first.Args)
		}
	}
}

// lastNPairValues returns the trailing n "key=value" strings from a
// "-c","key=value",... argv sequence, dropping the "-c" tokens.
func lastNPairValues(args []string, n int) []string {
	var values []string
	for i := 0; i+1 < len(args); i += 2 {
		values = append(values, args[i+1])
	}
	if len(values) < n {
		return values
	}
	return values[len(values)-n:]
}
