package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
)

// stubFS is a minimal FSHandler recording whether it was invoked.
type stubFS struct{ called bool }

func (s *stubFS) ReadTextFile(_ context.Context, req protocol.ReadTextFileRequest) (protocol.ReadTextFileResponse, error) {
	s.called = true
	return protocol.ReadTextFileResponse{Content: "stub-content"}, nil
}
func (s *stubFS) WriteTextFile(_ context.Context, req protocol.WriteTextFileRequest) (protocol.WriteTextFileResponse, error) {
	s.called = true
	return protocol.WriteTextFileResponse{}, nil
}

type stubTerminal struct{ called bool }

func (s *stubTerminal) CreateTerminal(_ context.Context, req protocol.CreateTerminalRequest) (protocol.CreateTerminalResponse, error) {
	s.called = true
	return protocol.CreateTerminalResponse{TerminalID: "term-1"}, nil
}
func (s *stubTerminal) TerminalOutput(_ context.Context, req protocol.TerminalOutputRequest) (protocol.TerminalOutputResponse, error) {
	s.called = true
	return protocol.TerminalOutputResponse{}, nil
}
func (s *stubTerminal) WaitForTerminalExit(_ context.Context, req protocol.WaitForTerminalExitRequest) (protocol.WaitForTerminalExitResponse, error) {
	s.called = true
	return protocol.WaitForTerminalExitResponse{}, nil
}
func (s *stubTerminal) KillTerminal(_ context.Context, req protocol.KillTerminalRequest) (protocol.KillTerminalResponse, error) {
	s.called = true
	return protocol.KillTerminalResponse{}, nil
}
func (s *stubTerminal) ReleaseTerminal(_ context.Context, req protocol.ReleaseTerminalRequest) (protocol.ReleaseTerminalResponse, error) {
	s.called = true
	return protocol.ReleaseTerminalResponse{}, nil
}

type stubPermission struct{ called bool }

func (s *stubPermission) RequestPermission(_ context.Context, req protocol.RequestPermissionRequest) (protocol.RequestPermissionResponse, error) {
	s.called = true
	return protocol.RequestPermissionResponse{Outcome: protocol.RequestPermissionOutcome{Selected: &protocol.SelectedPermissionOutcome{OptionID: "allow-once"}}}, nil
}

// TestCapabilityMatrixAdvertisesOnlyConfiguredCapabilities proves that
// InitializeRequest.ClientCapabilities reflects exactly which Options
// handlers are configured, independent of each other.
func TestCapabilityMatrixAdvertisesOnlyConfiguredCapabilities(t *testing.T) {
	tests := []struct {
		name         string
		opts         Options
		wantFS       bool
		wantTerminal bool
	}{
		{name: "none configured", opts: Options{}},
		{name: "fs only", opts: Options{FS: &stubFS{}}, wantFS: true},
		{name: "terminal only", opts: Options{Terminal: &stubTerminal{}}, wantTerminal: true},
		{name: "permissions only (no wire bit)", opts: Options{Permissions: &stubPermission{}}},
		{name: "all configured", opts: Options{FS: &stubFS{}, Terminal: &stubTerminal{}, Permissions: &stubPermission{}}, wantFS: true, wantTerminal: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, fa := dialTestClient(t, tt.opts)
			_ = c

			fa.mu.Lock()
			gotReq := fa.lastInitReq
			fa.mu.Unlock()

			if gotReq.ClientCapabilities == nil {
				t.Fatal("ClientCapabilities = nil, want a populated struct")
			}
			gotFS := gotReq.ClientCapabilities.Fs != nil
			if gotFS != tt.wantFS {
				t.Errorf("ClientCapabilities.Fs present = %v, want %v", gotFS, tt.wantFS)
			}
			if gotReq.ClientCapabilities.Terminal != tt.wantTerminal {
				t.Errorf("ClientCapabilities.Terminal = %v, want %v", gotReq.ClientCapabilities.Terminal, tt.wantTerminal)
			}
		})
	}
}

// TestCapabilityMatrixDispatchesOnlyWhenConfigured proves the dispatch side
// of the same matrix: a configured handler is actually invoked, and calling
// an unconfigured method fails with Conn's own MethodNotFound rather than
// panicking or hanging.
func TestCapabilityMatrixDispatchesOnlyWhenConfigured(t *testing.T) {
	t.Run("fs configured: read succeeds", func(t *testing.T) {
		fs := &stubFS{}
		c, fa := dialTestClient(t, Options{FS: fs})
		sess := newSessionForTest(t, c, fa, "sess-fs")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := fa.client.ReadTextFile(ctx, protocol.ReadTextFileRequest{SessionID: sess.ID(), Path: "/abs/path"})
		if err != nil {
			t.Fatalf("ReadTextFile() error = %v", err)
		}
		if !fs.called {
			t.Error("FSHandler.ReadTextFile was not invoked")
		}
		if resp.Content != "stub-content" {
			t.Errorf("Content = %q, want stub-content", resp.Content)
		}
	})

	t.Run("fs not configured: MethodNotFound", func(t *testing.T) {
		c, fa := dialTestClient(t, Options{})
		sess := newSessionForTest(t, c, fa, "sess-no-fs")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := fa.client.ReadTextFile(ctx, protocol.ReadTextFileRequest{SessionID: sess.ID(), Path: "/abs/path"})
		assertMethodNotFound(t, err)
	})

	t.Run("terminal configured: create succeeds", func(t *testing.T) {
		term := &stubTerminal{}
		c, fa := dialTestClient(t, Options{Terminal: term})
		sess := newSessionForTest(t, c, fa, "sess-term")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := fa.client.CreateTerminal(ctx, protocol.CreateTerminalRequest{SessionID: sess.ID(), Command: "echo"})
		if err != nil {
			t.Fatalf("CreateTerminal() error = %v", err)
		}
		if !term.called {
			t.Error("TerminalHandler.CreateTerminal was not invoked")
		}
		if resp.ID() != "term-1" {
			t.Errorf("TerminalID = %q, want term-1", resp.ID())
		}
	})

	t.Run("terminal not configured: MethodNotFound", func(t *testing.T) {
		c, fa := dialTestClient(t, Options{})
		sess := newSessionForTest(t, c, fa, "sess-no-term")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := fa.client.CreateTerminal(ctx, protocol.CreateTerminalRequest{SessionID: sess.ID(), Command: "echo"})
		assertMethodNotFound(t, err)
	})

	t.Run("permissions configured: request succeeds", func(t *testing.T) {
		perm := &stubPermission{}
		c, fa := dialTestClient(t, Options{Permissions: perm})
		sess := newSessionForTest(t, c, fa, "sess-perm")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := fa.client.RequestPermission(ctx, protocol.RequestPermissionRequest{
			SessionID: sess.ID(),
			ToolCall:  protocol.ToolCallUpdate{ToolCallID: "tc-1"},
		})
		if err != nil {
			t.Fatalf("RequestPermission() error = %v", err)
		}
		if !perm.called {
			t.Error("PermissionHandler.RequestPermission was not invoked")
		}
	})

	t.Run("permissions not configured: MethodNotFound", func(t *testing.T) {
		c, fa := dialTestClient(t, Options{})
		sess := newSessionForTest(t, c, fa, "sess-no-perm")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := fa.client.RequestPermission(ctx, protocol.RequestPermissionRequest{
			SessionID: sess.ID(),
			ToolCall:  protocol.ToolCallUpdate{ToolCallID: "tc-1"},
		})
		assertMethodNotFound(t, err)
	})
}

func assertMethodNotFound(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("error = nil, want MethodNotFound (capability not configured)")
	}
	var fault *protocol.Fault
	if !errors.As(err, &fault) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if fault.Code != protocol.ErrorCodeMethodNotFound {
		t.Errorf("fault.Code = %v, want %v", fault.Code, protocol.ErrorCodeMethodNotFound)
	}
}
