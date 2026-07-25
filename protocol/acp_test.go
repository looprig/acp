package protocol_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
)

// canonicalize decodes fixture (hand-written, human-formatted JSON) into a
// fresh T using T's own (possibly custom) UnmarshalJSON, then re-marshals it
// with T's own MarshalJSON. The result — canonical, compact wire bytes — is
// exactly what Conn.Call/Conn.Notify would produce for that value, so it is
// the basis for every byte-identity assertion below: it removes irrelevant
// formatting differences (whitespace, key order in a hand-written fixture)
// while still proving the typed surface preserves the fixture's data
// byte-for-byte through a real encode/decode cycle.
func canonicalize[T any](t *testing.T, fixture json.RawMessage) (T, []byte) {
	t.Helper()
	var v T
	if err := json.Unmarshal(fixture, &v); err != nil {
		t.Fatalf("unmarshal fixture into %T: %v\nfixture: %s", v, err, fixture)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	return v, out
}

// captureRequest registers the handler for method on server: it records the
// raw incoming params bytes (for the caller to inspect after the call
// returns) and answers with respRaw verbatim. json.RawMessage's own
// MarshalJSON returns its bytes unchanged, so respRaw reaches the wire
// exactly as given provided it is already compact (as canonicalize's output
// always is).
func captureRequest(server *protocol.Conn, method protocol.Method, respRaw json.RawMessage) *json.RawMessage {
	captured := new(json.RawMessage)
	server.Handle(string(method), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
		*captured = append(json.RawMessage(nil), params...)
		return respRaw, nil
	})
	return captured
}

// captureNotify registers the notification handler for method on server. The
// returned channel closes once exactly one notification for method has
// arrived, at which point *captured holds its raw params bytes.
func captureNotify(server *protocol.Conn, method protocol.Method) (captured *json.RawMessage, done <-chan struct{}) {
	raw := new(json.RawMessage)
	ch := make(chan struct{})
	server.HandleNotify(string(method), func(_ context.Context, _ string, params json.RawMessage) {
		*raw = append(json.RawMessage(nil), params...)
		close(ch)
	})
	return raw, ch
}

func waitNotify(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("notification never arrived")
	}
}

// runCallFixture drives one request/response ("Call"-shaped) method end to
// end over a live net.Pipe-backed Conn pair: it canonicalizes reqFixture and
// respFixture via Req/Resp's own marshaling, wires server to capture the raw
// request bytes and answer with the canonical response bytes, invokes call
// with the canonicalized request value, and asserts both directions are
// byte-identical to the canonical fixture forms.
func runCallFixture[Req any, Resp any](
	t *testing.T,
	server *protocol.Conn,
	method protocol.Method,
	reqFixture, respFixture json.RawMessage,
	call func(context.Context, Req) (*Resp, error),
) {
	t.Helper()

	reqVal, reqCanonical := canonicalize[Req](t, reqFixture)
	_, respCanonical := canonicalize[Resp](t, respFixture)

	captured := captureRequest(server, method, respCanonical)

	got, err := call(context.Background(), reqVal)
	if err != nil {
		t.Fatalf("%s call error = %v", method, err)
	}
	if got == nil {
		t.Fatalf("%s call returned nil response", method)
	}
	if !bytes.Equal(*captured, reqCanonical) {
		t.Errorf("%s request wire bytes =\n%s\nwant\n%s", method, *captured, reqCanonical)
	}
	gotBytes, err := json.Marshal(*got)
	if err != nil {
		t.Fatalf("marshal %s response: %v", method, err)
	}
	if !bytes.Equal(gotBytes, respCanonical) {
		t.Errorf("%s response wire bytes =\n%s\nwant\n%s", method, gotBytes, respCanonical)
	}
}

// runNotifyFixture is runCallFixture's counterpart for notifications: there
// is no response to check, only that the params sent equal the canonical
// fixture bytes once received.
func runNotifyFixture[Params any](
	t *testing.T,
	server *protocol.Conn,
	method protocol.Method,
	fixture json.RawMessage,
	notify func(context.Context, Params) error,
) {
	t.Helper()

	val, canonical := canonicalize[Params](t, fixture)
	captured, done := captureNotify(server, method)

	if err := notify(context.Background(), val); err != nil {
		t.Fatalf("%s notify error = %v", method, err)
	}
	waitNotify(t, done)

	if !bytes.Equal(*captured, canonical) {
		t.Errorf("%s notification wire bytes =\n%s\nwant\n%s", method, *captured, canonical)
	}
}

// --- AgentConn: methods a client calls on an agent ---

func TestAgentConnInitialize(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{
		"protocolVersion": 1,
		"clientInfo": {"name": "acp-golden-client", "version": "0.1.0"},
		"clientCapabilities": {
			"fs": {"readTextFile": true, "writeTextFile": true},
			"terminal": true
		}
	}`)
	respFixture := json.RawMessage(`{
		"protocolVersion": 1,
		"agentInfo": {"name": "acp-golden-agent", "version": "9.9.9"},
		"authMethods": [{"id": "api-key", "name": "API Key"}],
		"agentCapabilities": {
			"loadSession": true,
			"mcpCapabilities": {"http": true, "sse": false}
		}
	}`)

	runCallFixture(t, server, protocol.MethodInitialize, reqFixture, respFixture, agent.Initialize)
}

func TestAgentConnAuthenticate(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{"methodId": "api-key"}`)
	respFixture := json.RawMessage(`{}`)

	runCallFixture(t, server, protocol.MethodAuthenticate, reqFixture, respFixture, agent.Authenticate)
}

func TestAgentConnNewSession(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{
		"cwd": "/work/project",
		"mcpServers": [
			{"type": "stdio", "name": "fs-tools", "command": "/usr/bin/mcp-fs", "args": ["--stdio"], "env": []}
		]
	}`)
	respFixture := json.RawMessage(`{
		"sessionId": "sess_1",
		"modes": {
			"availableModes": [{"id": "default", "name": "Default"}],
			"currentModeId": "default"
		}
	}`)

	runCallFixture(t, server, protocol.MethodSessionNew, reqFixture, respFixture, agent.NewSession)
}

func TestAgentConnLoadSession(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{
		"sessionId": "sess_1",
		"cwd": "/work/project",
		"mcpServers": []
	}`)
	respFixture := json.RawMessage(`{
		"configOptions": []
	}`)

	runCallFixture(t, server, protocol.MethodSessionLoad, reqFixture, respFixture, agent.LoadSession)
}

func TestAgentConnResumeSession(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{
		"sessionId": "sess_1",
		"cwd": "/work/project"
	}`)
	respFixture := json.RawMessage(`{
		"modes": {
			"availableModes": [{"id": "default", "name": "Default"}],
			"currentModeId": "default"
		}
	}`)

	runCallFixture(t, server, protocol.MethodSessionResume, reqFixture, respFixture, agent.ResumeSession)
}

func TestAgentConnListSessions(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{
		"cwd": "/work/project"
	}`)
	respFixture := json.RawMessage(`{
		"sessions": [
			{"sessionId": "sess_1", "cwd": "/work/project", "title": "Fix the bug"}
		],
		"nextCursor": "cursor-2"
	}`)

	runCallFixture(t, server, protocol.MethodSessionList, reqFixture, respFixture, agent.ListSessions)
}

func TestAgentConnCloseSession(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{"sessionId": "sess_1"}`)
	respFixture := json.RawMessage(`{}`)

	runCallFixture(t, server, protocol.MethodSessionClose, reqFixture, respFixture, agent.CloseSession)
}

func TestAgentConnDeleteSession(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{"sessionId": "sess_1"}`)
	respFixture := json.RawMessage(`{}`)

	runCallFixture(t, server, protocol.MethodSessionDelete, reqFixture, respFixture, agent.DeleteSession)
}

func TestAgentConnPrompt(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{
		"sessionId": "sess_1",
		"prompt": [{"type": "text", "text": "What does this repo do?"}]
	}`)
	respFixture := json.RawMessage(`{"stopReason": "end_turn"}`)

	runCallFixture(t, server, protocol.MethodSessionPrompt, reqFixture, respFixture, agent.Prompt)
}

func TestAgentConnCancelIsNotification(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	fixture := json.RawMessage(`{"sessionId": "sess_1"}`)

	runNotifyFixture(t, server, protocol.MethodSessionCancel, fixture, agent.Cancel)
}

func TestAgentConnSetConfigOption(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{
		"sessionId": "sess_1",
		"configId": "verbose",
		"type": "boolean",
		"value": true
	}`)
	respFixture := json.RawMessage(`{"configOptions": []}`)

	runCallFixture(t, server, protocol.MethodSessionSetConfigOption, reqFixture, respFixture, agent.SetConfigOption)
}

// TestAgentConnSetConfigOptionValueIDVariant exercises
// SetSessionConfigOptionRequest's other ("value_id", the fallback when
// "type" is absent) variant, distinct enough from the boolean variant above
// to be worth its own fixture: both variants nest their scalar payload
// under the wire property "value" rather than being an object in their own
// right (see internal/gen's Variant.WrapKey), so this also covers that both
// directions of that flattening are exercised, not just one.
func TestAgentConnSetConfigOptionValueIDVariant(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{
		"sessionId": "sess_1",
		"configId": "theme",
		"value": "dark"
	}`)
	respFixture := json.RawMessage(`{"configOptions": []}`)

	runCallFixture(t, server, protocol.MethodSessionSetConfigOption, reqFixture, respFixture, agent.SetConfigOption)
}

func TestAgentConnSetMode(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	agent := protocol.NewAgentConn(client)

	reqFixture := json.RawMessage(`{"sessionId": "sess_1", "modeId": "default"}`)
	respFixture := json.RawMessage(`{}`)

	runCallFixture(t, server, protocol.MethodSessionSetMode, reqFixture, respFixture, agent.SetMode)
}

// --- ClientConn: methods an agent calls on a client ---

func TestClientConnRequestPermission(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	clientConn := protocol.NewClientConn(client)

	reqFixture := json.RawMessage(`{
		"sessionId": "sess_1",
		"toolCall": {"toolCallId": "call_1", "title": "Delete file"},
		"options": [
			{"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
			{"optionId": "reject-once", "name": "Reject once", "kind": "reject_once"}
		]
	}`)
	respFixture := json.RawMessage(`{
		"outcome": {"outcome": "selected", "optionId": "allow-once"}
	}`)

	runCallFixture(t, server, protocol.MethodSessionRequestPermission, reqFixture, respFixture, clientConn.RequestPermission)
}

func TestClientConnReadTextFile(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	clientConn := protocol.NewClientConn(client)

	reqFixture := json.RawMessage(`{
		"sessionId": "sess_1",
		"path": "/work/project/README.md",
		"line": 1,
		"limit": 100
	}`)
	respFixture := json.RawMessage(`{"content": "# Project\n\nHello.\n"}`)

	runCallFixture(t, server, protocol.MethodFsReadTextFile, reqFixture, respFixture, clientConn.ReadTextFile)
}

func TestClientConnWriteTextFile(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	clientConn := protocol.NewClientConn(client)

	reqFixture := json.RawMessage(`{
		"sessionId": "sess_1",
		"path": "/work/project/README.md",
		"content": "# Project\n\nUpdated.\n"
	}`)
	respFixture := json.RawMessage(`{}`)

	runCallFixture(t, server, protocol.MethodFsWriteTextFile, reqFixture, respFixture, clientConn.WriteTextFile)
}

// terminalFixtureSessionID and terminalFixtureTerminalID pin the ids shared
// across the CreateTerminal fixture and every TerminalHandle operation
// fixture below, so each handle-op test can assert the handle threads the
// exact ids from its own CreateTerminal call without repeating them in the
// request wrapper's Go signature.
const (
	terminalFixtureSessionID  = "sess_1"
	terminalFixtureTerminalID = "term_1"
)

func createTestTerminal(t *testing.T, server *protocol.Conn, clientConn *protocol.ClientConn) *protocol.TerminalHandle {
	t.Helper()

	reqFixture := json.RawMessage(`{
		"sessionId": "` + terminalFixtureSessionID + `",
		"command": "bash",
		"args": ["-lc", "echo hi"],
		"env": [{"name": "TERM", "value": "xterm-256color"}],
		"cwd": "/work/project",
		"outputByteLimit": 65536
	}`)
	respFixture := json.RawMessage(`{"terminalId": "` + terminalFixtureTerminalID + `"}`)

	var handle *protocol.TerminalHandle
	runCallFixture(t, server, protocol.MethodTerminalCreate, reqFixture, respFixture,
		func(ctx context.Context, req protocol.CreateTerminalRequest) (*protocol.CreateTerminalResponse, error) {
			h, err := clientConn.CreateTerminal(ctx, req)
			if err != nil {
				return nil, err
			}
			handle = h
			return &protocol.CreateTerminalResponse{TerminalID: h.ID()}, nil
		})
	if handle == nil {
		t.Fatalf("createTestTerminal: handle never assigned")
	}
	if handle.ID() != protocol.TerminalID(terminalFixtureTerminalID) {
		t.Fatalf("handle.ID() = %q, want %q", handle.ID(), terminalFixtureTerminalID)
	}
	return handle
}

func TestClientConnCreateTerminal(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	clientConn := protocol.NewClientConn(client)

	createTestTerminal(t, server, clientConn)
}

func TestTerminalHandleOutput(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	clientConn := protocol.NewClientConn(client)
	handle := createTestTerminal(t, server, clientConn)

	fixture := json.RawMessage(`{
		"sessionId": "` + terminalFixtureSessionID + `",
		"terminalId": "` + terminalFixtureTerminalID + `"
	}`)
	respFixture := json.RawMessage(`{
		"output": "hi\n",
		"truncated": false,
		"exitStatus": {"exitCode": 0}
	}`)

	runCallFixture(t, server, protocol.MethodTerminalOutput, fixture, respFixture,
		func(ctx context.Context, _ protocol.TerminalOutputRequest) (*protocol.TerminalOutputResponse, error) {
			return handle.Output(ctx)
		})
}

func TestTerminalHandleWaitForExit(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	clientConn := protocol.NewClientConn(client)
	handle := createTestTerminal(t, server, clientConn)

	fixture := json.RawMessage(`{
		"sessionId": "` + terminalFixtureSessionID + `",
		"terminalId": "` + terminalFixtureTerminalID + `"
	}`)
	respFixture := json.RawMessage(`{"exitCode": 0}`)

	runCallFixture(t, server, protocol.MethodTerminalWaitForExit, fixture, respFixture,
		func(ctx context.Context, _ protocol.WaitForTerminalExitRequest) (*protocol.WaitForTerminalExitResponse, error) {
			return handle.WaitForExit(ctx)
		})
}

func TestTerminalHandleKill(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	clientConn := protocol.NewClientConn(client)
	handle := createTestTerminal(t, server, clientConn)

	fixture := json.RawMessage(`{
		"sessionId": "` + terminalFixtureSessionID + `",
		"terminalId": "` + terminalFixtureTerminalID + `"
	}`)
	respFixture := json.RawMessage(`{}`)

	runCallFixture(t, server, protocol.MethodTerminalKill, fixture, respFixture,
		func(ctx context.Context, _ protocol.KillTerminalRequest) (*protocol.KillTerminalResponse, error) {
			return handle.Kill(ctx)
		})
}

func TestTerminalHandleRelease(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	clientConn := protocol.NewClientConn(client)
	handle := createTestTerminal(t, server, clientConn)

	fixture := json.RawMessage(`{
		"sessionId": "` + terminalFixtureSessionID + `",
		"terminalId": "` + terminalFixtureTerminalID + `"
	}`)
	respFixture := json.RawMessage(`{}`)

	runCallFixture(t, server, protocol.MethodTerminalRelease, fixture, respFixture,
		func(ctx context.Context, _ protocol.ReleaseTerminalRequest) (*protocol.ReleaseTerminalResponse, error) {
			return handle.Release(ctx)
		})
}

func TestClientConnSessionUpdateIsNotification(t *testing.T) {
	assertNoGoroutineLeak(t)
	client, server := pipeConns(t)
	clientConn := protocol.NewClientConn(client)

	fixture := json.RawMessage(`{
		"sessionId": "sess_1",
		"update": {
			"sessionUpdate": "agent_message_chunk",
			"content": {"type": "text", "text": "Working on it..."}
		}
	}`)

	runNotifyFixture(t, server, protocol.MethodSessionUpdate, fixture, clientConn.SessionUpdate)
}
