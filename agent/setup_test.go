package agent_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
)

func TestNewSetupCwdValidation(t *testing.T) {
	tests := []struct {
		name       string
		cwd        string
		wantReason agent.CwdErrorReason
		wantErr    bool
	}{
		{name: "valid absolute clean path", cwd: "/workspace/project", wantErr: false},
		{name: "empty", cwd: "", wantErr: true, wantReason: agent.CwdReasonEmpty},
		{name: "relative path", cwd: "relative/project", wantErr: true, wantReason: agent.CwdReasonNotAbsolute},
		{name: "dot dot traversal", cwd: "/workspace/../etc", wantErr: true, wantReason: agent.CwdReasonTraversal},
		{name: "leading dot dot", cwd: "/../etc", wantErr: true, wantReason: agent.CwdReasonTraversal},
		{name: "double slash not canonical", cwd: "/workspace//project", wantErr: true, wantReason: agent.CwdReasonNotCanonical},
		{name: "trailing slash not canonical", cwd: "/workspace/project/", wantErr: true, wantReason: agent.CwdReasonNotCanonical},
		{name: "dot segment not canonical", cwd: "/workspace/./project", wantErr: true, wantReason: agent.CwdReasonNotCanonical},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			setup, err := agent.NewSetup(tt.cwd, nil, nil, false)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewSetup(%q) error = nil, want error", tt.cwd)
				}
				var cwdErr *agent.CwdError
				if !errors.As(err, &cwdErr) {
					t.Fatalf("NewSetup(%q) error = %v (%T), want *agent.CwdError", tt.cwd, err, err)
				}
				if cwdErr.Reason != tt.wantReason {
					t.Errorf("NewSetup(%q) reason = %v, want %v", tt.cwd, cwdErr.Reason, tt.wantReason)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewSetup(%q) unexpected error: %v", tt.cwd, err)
			}
			if setup.Cwd != tt.cwd {
				t.Errorf("Setup.Cwd = %q, want %q", setup.Cwd, tt.cwd)
			}
		})
	}
}

func TestNewSetupClientCapabilityDefaulting(t *testing.T) {
	t.Run("nil capabilities get full schema defaults", func(t *testing.T) {
		t.Parallel()
		setup, err := agent.NewSetup("/workspace", nil, nil, false)
		if err != nil {
			t.Fatalf("NewSetup: unexpected error: %v", err)
		}
		want := protocol.DefaultClientCapabilities()
		if setup.ClientCapabilities.Terminal != want.Terminal {
			t.Errorf("Terminal = %v, want %v", setup.ClientCapabilities.Terminal, want.Terminal)
		}
		if setup.ClientCapabilities.Fs == nil {
			t.Fatal("Fs = nil, want schema-defaulted *FileSystemCapabilities")
		}
		if want := protocol.DefaultFileSystemCapabilities(); !reflect.DeepEqual(*setup.ClientCapabilities.Fs, want) {
			t.Errorf("Fs = %+v, want %+v", *setup.ClientCapabilities.Fs, want)
		}
	})

	t.Run("partial capabilities fill only unset subfields", func(t *testing.T) {
		t.Parallel()
		given := &protocol.ClientCapabilities{Terminal: true}
		setup, err := agent.NewSetup("/workspace", given, nil, false)
		if err != nil {
			t.Fatalf("NewSetup: unexpected error: %v", err)
		}
		if !setup.ClientCapabilities.Terminal {
			t.Error("Terminal = false, want true (explicitly supplied value must be preserved)")
		}
		if setup.ClientCapabilities.Fs == nil {
			t.Fatal("Fs = nil, want schema-defaulted *FileSystemCapabilities filled in for the unset subfield")
		}
		if want := protocol.DefaultFileSystemCapabilities(); !reflect.DeepEqual(*setup.ClientCapabilities.Fs, want) {
			t.Errorf("Fs = %+v, want %+v", *setup.ClientCapabilities.Fs, want)
		}
	})

	t.Run("fully populated capabilities are preserved untouched", func(t *testing.T) {
		t.Parallel()
		fs := protocol.FileSystemCapabilities{ReadTextFile: true, WriteTextFile: true}
		given := &protocol.ClientCapabilities{Fs: &fs, Terminal: true}
		setup, err := agent.NewSetup("/workspace", given, nil, false)
		if err != nil {
			t.Fatalf("NewSetup: unexpected error: %v", err)
		}
		if setup.ClientCapabilities.Fs == nil || !reflect.DeepEqual(*setup.ClientCapabilities.Fs, fs) {
			t.Errorf("Fs = %+v, want preserved %+v", setup.ClientCapabilities.Fs, fs)
		}
		if !setup.ClientCapabilities.Terminal {
			t.Error("Terminal = false, want true")
		}
	})
}

func TestNewSetupMCPDescriptorRejection(t *testing.T) {
	server := protocol.McpServer{Stdio: &protocol.McpServerStdio{Name: "fixture", Command: "/usr/bin/fixture-mcp"}}

	t.Run("rejected when host does not accept MCP", func(t *testing.T) {
		t.Parallel()
		_, err := agent.NewSetup("/workspace", nil, []protocol.McpServer{server}, false)
		if err == nil {
			t.Fatal("NewSetup: error = nil, want *agent.MCPNotAcceptedError")
		}
		var mcpErr *agent.MCPNotAcceptedError
		if !errors.As(err, &mcpErr) {
			t.Fatalf("NewSetup: error = %v (%T), want *agent.MCPNotAcceptedError", err, err)
		}
		if mcpErr.Count != 1 {
			t.Errorf("MCPNotAcceptedError.Count = %d, want 1", mcpErr.Count)
		}
	})

	t.Run("accepted when host advertises acceptance", func(t *testing.T) {
		t.Parallel()
		setup, err := agent.NewSetup("/workspace", nil, []protocol.McpServer{server}, true)
		if err != nil {
			t.Fatalf("NewSetup: unexpected error: %v", err)
		}
		if len(setup.MCPServers) != 1 {
			t.Fatalf("MCPServers = %v, want 1 entry", setup.MCPServers)
		}
	})

	t.Run("empty MCP servers never rejected regardless of acceptance", func(t *testing.T) {
		t.Parallel()
		setup, err := agent.NewSetup("/workspace", nil, nil, false)
		if err != nil {
			t.Fatalf("NewSetup: unexpected error: %v", err)
		}
		if len(setup.MCPServers) != 0 {
			t.Errorf("MCPServers = %v, want empty", setup.MCPServers)
		}
	})
}
