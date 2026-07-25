package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
)

// TestDispatchValidatesSessionIDBeforeInvokingHandler proves that an inbound
// request naming an unknown (never registered) sessionId is rejected before
// the injected handler is ever invoked.
func TestDispatchValidatesSessionIDBeforeInvokingHandler(t *testing.T) {
	fs := &stubFS{}
	_, fa := dialTestClient(t, Options{FS: fs})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := fa.client.ReadTextFile(ctx, protocol.ReadTextFileRequest{SessionID: "never-registered", Path: "/abs/path"})
	if err == nil {
		t.Fatal("ReadTextFile() with an unknown sessionId succeeded, want an error")
	}
	if fs.called {
		t.Error("FSHandler.ReadTextFile was invoked despite an invalid sessionId")
	}
	var fault *protocol.Fault
	if !errors.As(err, &fault) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
}

func TestDispatchRejectsEmptySessionID(t *testing.T) {
	fs := &stubFS{}
	_, fa := dialTestClient(t, Options{FS: fs})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := fa.client.ReadTextFile(ctx, protocol.ReadTextFileRequest{Path: "/abs/path"})
	if err == nil {
		t.Fatal("ReadTextFile() with an empty sessionId succeeded, want an error")
	}
	if fs.called {
		t.Error("FSHandler.ReadTextFile was invoked despite an empty sessionId")
	}
}

// TestDispatchValidatesPathBeforeInvokingHandler proves a relative (or
// empty) path is rejected before the FS handler is invoked.
func TestDispatchValidatesPathBeforeInvokingHandler(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "relative path", path: "relative/path.txt"},
		{name: "empty path", path: ""},
		{name: "traversal not cleaned", path: "/abs/../etc/passwd"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &stubFS{}
			c, fa := dialTestClient(t, Options{FS: fs})
			sess := newSessionForTest(t, c, fa, protocol.SessionID("sess-path-"+tt.name))

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := fa.client.ReadTextFile(ctx, protocol.ReadTextFileRequest{SessionID: sess.ID(), Path: tt.path})
			if err == nil {
				t.Fatalf("ReadTextFile() with path %q succeeded, want an error", tt.path)
			}
			if fs.called {
				t.Errorf("FSHandler.ReadTextFile was invoked despite invalid path %q", tt.path)
			}
		})
	}
}

// TestDispatchValidatesTerminalIDBeforeInvokingHandler proves an empty
// terminalId is rejected before the Terminal handler is invoked, for every
// method that takes one (everything except terminal/create).
func TestDispatchValidatesTerminalIDBeforeInvokingHandler(t *testing.T) {
	term := &stubTerminal{}
	c, fa := dialTestClient(t, Options{Terminal: term})
	sess := newSessionForTest(t, c, fa, "sess-term-validate")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var resp protocol.TerminalOutputResponse
	err := fa.conn.Call(ctx, string(protocol.MethodTerminalOutput), protocol.TerminalOutputRequest{SessionID: sess.ID(), TerminalID: ""}, &resp)
	if err == nil {
		t.Fatal("terminal/output with an empty terminalId succeeded, want an error")
	}
	if term.called {
		t.Error("TerminalHandler.TerminalOutput was invoked despite an empty terminalId")
	}
}
