package launch

import (
	"errors"
	"reflect"
	"testing"

	"github.com/looprig/acp/transport/stdio"
)

func TestGeminiAdapterConfigureSetsExactContract(t *testing.T) {
	g := Gemini("gemini-2.5-pro")

	cmd := stdio.Command{
		Path: "/opt/gemini/bin/gemini",
		Args: []string{"--some-arg"},
		Env:  []string{"PATH=/usr/bin"},
		Dir:  "/work",
	}
	binding := ProxyBinding{BaseURL: "http://127.0.0.1:4141", Token: "gw-token-abc"}

	out, err := g.Configure(cmd, binding)
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}

	if out.Path != cmd.Path {
		t.Errorf("Path = %q, want %q unchanged", out.Path, cmd.Path)
	}
	if out.Dir != cmd.Dir {
		t.Errorf("Dir = %q, want %q unchanged", out.Dir, cmd.Dir)
	}
	// Unlike ClaudeConnector/CodexConnector, GeminiAdapter claims no
	// executable argument contract: Args must be preserved, never
	// cleared or replaced.
	if !equalStrings(out.Args, cmd.Args) {
		t.Errorf("Args = %v, want the caller's own Args preserved: %v", out.Args, cmd.Args)
	}

	env := envMap(out.Env)
	if env["GOOGLE_GEMINI_BASE_URL"] != binding.BaseURL {
		t.Errorf("GOOGLE_GEMINI_BASE_URL = %q, want %q", env["GOOGLE_GEMINI_BASE_URL"], binding.BaseURL)
	}
	if env["GEMINI_API_KEY"] != binding.Token {
		t.Errorf("GEMINI_API_KEY = %q, want %q", env["GEMINI_API_KEY"], binding.Token)
	}
	if env["GEMINI_MODEL"] != "gemini-2.5-pro" {
		t.Errorf("GEMINI_MODEL = %q, want %q", env["GEMINI_MODEL"], "gemini-2.5-pro")
	}
	if env["PATH"] != "/usr/bin" {
		t.Errorf("PATH = %q, want the unrelated allowlisted entry preserved", env["PATH"])
	}

	// Exactly these three gateway variables -- nothing Vertex-AI- or
	// Code-Assist-related, and nothing else added at all.
	wantKeys := map[string]bool{"PATH": true, "GOOGLE_GEMINI_BASE_URL": true, "GEMINI_API_KEY": true, "GEMINI_MODEL": true}
	for k := range env {
		if !wantKeys[k] {
			t.Errorf("unexpected env var %q present, want only %v", k, wantKeys)
		}
	}

	// The original Command must never be mutated by Configure.
	if cmd.Args[0] != "--some-arg" {
		t.Errorf("original cmd.Args mutated: %v", cmd.Args)
	}
	if len(cmd.Env) != 1 || cmd.Env[0] != "PATH=/usr/bin" {
		t.Errorf("original cmd.Env mutated: %v", cmd.Env)
	}
}

func TestGeminiAdapterConfigureRejectsNonAbsolutePath(t *testing.T) {
	g := Gemini("gemini-2.5-pro")

	for name, cmd := range map[string]stdio.Command{
		"empty":        {Path: ""},
		"relative":     {Path: "gemini"},
		"not-cleaned":  {Path: "/opt/../opt/gemini"},
		"trailing-sep": {Path: "/opt/gemini/"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := g.Configure(cmd, ProxyBinding{BaseURL: "http://x", Token: "t"})
			var pathErr *PathError
			if !errors.As(err, &pathErr) {
				t.Fatalf("Configure() error = %v (%T), want *PathError", err, err)
			}
		})
	}
}

func TestGeminiAdapterConfigureRejectsEmptyModel(t *testing.T) {
	g := Gemini("")

	_, err := g.Configure(stdio.Command{Path: "/opt/gemini"}, ProxyBinding{BaseURL: "http://x", Token: "t"})
	var cfgErr *ConfigError
	if !errors.As(err, &cfgErr) {
		t.Fatalf("Configure() error = %v (%T), want *ConfigError", err, err)
	}
}

func TestGeminiConstructorStoresModel(t *testing.T) {
	g := Gemini("gemini-2.5-flash")
	if g.Model != "gemini-2.5-flash" {
		t.Errorf("Model = %q, want %q", g.Model, "gemini-2.5-flash")
	}
}

// TestGeminiAdapterIsNotAConnector is a scope-creep regression guard: the
// design doc is explicit that Gemini CLI's ACP connector is deferred until
// an adapter path is verified against a real release (see this file's own
// package doc). GeminiAdapter must therefore expose exactly one method
// (Configure, its HarnessAdapter contract) and nothing session-, config-,
// or capability-related -- if a later change adds any such method, this
// test fails, forcing a conscious decision rather than an accidental
// "complete the set."
func TestGeminiAdapterIsNotAConnector(t *testing.T) {
	typ := reflect.TypeOf(&GeminiAdapter{})
	if got, want := typ.NumMethod(), 1; got != want {
		names := make([]string, got)
		for i := 0; i < got; i++ {
			names[i] = typ.Method(i).Name
		}
		t.Fatalf("GeminiAdapter has %d exported methods %v, want exactly 1 (Configure): building more is scope creep against the design doc's deferred Gemini ACP connector", got, names)
	}
	if got := typ.Method(0).Name; got != "Configure" {
		t.Fatalf("GeminiAdapter's sole method = %q, want %q", got, "Configure")
	}
}
