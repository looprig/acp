package agent_test

// e2e_test.go is the Phase 2 exit criterion from
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md: "an in-process
// end-to-end test wiring facade <-> protocol.Conn <-> mockclient over pipes:
// initialize -> new -> prompt -> streamed updates -> permission round-trip
// -> cancel -> close, all assertions on wire JSON."
//
// Every other test file in this package already proves each of these steps
// individually (often more thoroughly, with more edge cases). This file's
// job is different: prove the WHOLE lifecycle composes over one real
// connection, end to end, with assertions made on the literal bytes that
// crossed the wire — not just the Go-level values a typed AgentConn/
// ClientConn call decoded them into. capturingWriter (below) taps each
// Conn's underlying io.Writer to record every newline-terminated JSON-RPC
// frame (see protocol.Writer.run: exactly one full frame per Write call) in
// the exact order it was sent, so this file can assert directly on parsed
// JSON structure — method names, id correlation, result/params shapes —
// for the key messages at each lifecycle step.
//
// There is no reusable fake ACP client anywhere in this package yet (gates_
// test.go and prompt_test.go each register ad hoc client.Handle/
// client.HandleNotify callbacks inline, right where they are needed): that
// is exactly the "minimal mock client" pattern this file also uses, over the
// raw *protocol.Conn client side, mirroring newGateTestAgent.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	coreuuid "github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
)

// capturingWriter wraps an io.Writer, recording an independent copy of every
// byte slice written to it. Each Write call from protocol.Writer's internal
// goroutine is exactly one complete, newline-terminated JSON-RPC frame, so
// the recorded slice is, in order, every wire message this Conn ever sent.
type capturingWriter struct {
	w net.Conn

	mu     sync.Mutex
	frames [][]byte
}

func (c *capturingWriter) Write(p []byte) (int, error) {
	cp := append([]byte(nil), p...)
	n, err := c.w.Write(p)
	c.mu.Lock()
	c.frames = append(c.frames, cp)
	c.mu.Unlock()
	return n, err
}

func (c *capturingWriter) snapshot() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.frames...)
}

// e2ePipe bundles both directions of one wired-up connection: a real
// protocol.Conn plus the capturingWriter recording every frame it sends.
type e2ePipe struct {
	conn    *protocol.Conn
	written *capturingWriter
}

// newE2EPipes wires a client-role and an agent-role protocol.Conn together
// over a net.Pipe, each with its outbound frames captured independently:
// serverPipe.written records every frame the AGENT sends (responses to the
// client, plus its own agent-calls-client requests/notifications);
// clientPipe.written records every frame the CLIENT sends (requests/
// notifications to the agent, plus its responses to agent-calls-client
// requests).
func newE2EPipes(t *testing.T) (clientPipe, serverPipe e2ePipe) {
	t.Helper()
	c1, c2 := net.Pipe()

	clientWritten := &capturingWriter{w: c1}
	serverWritten := &capturingWriter{w: c2}

	clientConn := protocol.NewConn(c1, clientWritten, protocol.ConnOptions{})
	serverConn := protocol.NewConn(c2, serverWritten, protocol.ConnOptions{})
	t.Cleanup(func() {
		clientConn.Close()
		serverConn.Close()
	})

	return e2ePipe{conn: clientConn, written: clientWritten}, e2ePipe{conn: serverConn, written: serverWritten}
}

// wireFrame is a JSON-RPC frame decoded generically (method dispatch neutral
// on whether it is a request, response, or notification), for wire-level
// structural assertions this file makes alongside the typed Go-level ones.
type wireFrame map[string]any

// awaitWireFrame scans pipe's captured frames (from *pos onward, advancing
// pos past every frame it inspects) for the first one matching pred,
// polling until testTimeout elapses. It fails the test outright if a
// captured frame is not valid JSON — every frame this module ever writes
// must be, by construction.
func awaitWireFrame(t *testing.T, pipe *capturingWriter, pos *int, description string, pred func(wireFrame) bool) wireFrame {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for {
		frames := pipe.snapshot()
		for ; *pos < len(frames); *pos++ {
			var f wireFrame
			if err := json.Unmarshal(bytes.TrimSpace(frames[*pos]), &f); err != nil {
				t.Fatalf("captured wire frame %d is not valid JSON: %v (%s)", *pos, err, frames[*pos])
			}
			if pred(f) {
				*pos++
				return f
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for wire frame: %s", description)
		}
		time.Sleep(time.Millisecond)
	}
}

func isRequestFor(method string) func(wireFrame) bool {
	return func(f wireFrame) bool {
		_, hasID := f["id"]
		return hasID && f["method"] == method
	}
}

func isNotificationFor(method string) func(wireFrame) bool {
	return func(f wireFrame) bool {
		_, hasID := f["id"]
		return !hasID && f["method"] == method
	}
}

func isResponseTo(id float64) func(wireFrame) bool {
	return func(f wireFrame) bool {
		gotID, ok := f["id"].(float64)
		return ok && gotID == id && f["method"] == nil
	}
}

// TestEndToEndInitializeNewPromptUpdatesPermissionCancelClose is the Phase 2
// exit criterion: initialize -> new -> prompt -> streamed updates ->
// permission round-trip -> cancel -> close, wired over a real
// protocol.Conn pipe pair, with the facade on one side and a minimal mock
// ACP client (built from ad hoc client.Handle/client.HandleNotify callbacks,
// the same pattern gates_test.go and prompt_test.go already use) on the
// other. Every step asserts on the actual wire JSON captured by
// capturingWriter, not merely the Go-level value a typed call decoded.
func TestEndToEndInitializeNewPromptUpdatesPermissionCancelClose(t *testing.T) {
	fake := newFakeLiveSession(t)
	host := &promptHostStub{session: fake}
	a, err := agent.New(agent.Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}

	clientPipe, serverPipe := newE2EPipes(t)
	a.Register(serverPipe.conn)
	agentConn := protocol.NewAgentConn(clientPipe.conn)

	// The minimal mock ACP client: session/update notifications are
	// recorded for wire+Go-level assertions, and session/request_permission
	// is answered with a scripted approval.
	updates := make(chan protocol.SessionNotification, 8)
	clientPipe.conn.HandleNotify(string(protocol.MethodSessionUpdate), func(_ context.Context, _ string, params json.RawMessage) {
		var n protocol.SessionNotification
		if err := json.Unmarshal(params, &n); err != nil {
			t.Errorf("unmarshal session/update notification: %v", err)
			return
		}
		updates <- n
	})
	var gotPermissionReq protocol.RequestPermissionRequest
	clientPipe.conn.Handle(string(protocol.MethodSessionRequestPermission), func(_ context.Context, _ string, params json.RawMessage) (any, error) {
		if err := json.Unmarshal(params, &gotPermissionReq); err != nil {
			t.Errorf("unmarshal session/request_permission params: %v", err)
		}
		return protocol.RequestPermissionResponse{
			Outcome: protocol.RequestPermissionOutcome{
				Selected: &protocol.SelectedPermissionOutcome{OptionID: protocol.PermissionOptionID(gate.ApprovalApprove)},
			},
		}, nil
	})

	serverPos, clientPos := 0, 0

	// --- Step 1: initialize -------------------------------------------
	initResp, err := agentConn.Initialize(context.Background(), protocol.InitializeRequest{
		ProtocolVersion:    protocol.CurrentProtocolVersion,
		ClientCapabilities: &protocol.ClientCapabilities{},
	})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if initResp.ProtocolVersion != protocol.CurrentProtocolVersion {
		t.Errorf("InitializeResponse.ProtocolVersion = %v, want %v", initResp.ProtocolVersion, protocol.CurrentProtocolVersion)
	}
	if initResp.AgentCapabilities == nil {
		t.Fatal("InitializeResponse.AgentCapabilities = nil, want non-nil")
	}

	initReqFrame := awaitWireFrame(t, clientPipe.written, &clientPos, "initialize request", isRequestFor("initialize"))
	initReqID, ok := initReqFrame["id"].(float64)
	if !ok {
		t.Fatalf("initialize request frame has no numeric id: %+v", initReqFrame)
	}
	initRespFrame := awaitWireFrame(t, serverPipe.written, &serverPos, "initialize response", isResponseTo(initReqID))
	result, ok := initRespFrame["result"].(map[string]any)
	if !ok {
		t.Fatalf("initialize response frame has no result object: %+v", initRespFrame)
	}
	if pv, _ := result["protocolVersion"].(float64); pv != float64(protocol.CurrentProtocolVersion) {
		t.Errorf("wire initialize response protocolVersion = %v, want %v", result["protocolVersion"], protocol.CurrentProtocolVersion)
	}
	if _, ok := result["agentCapabilities"]; !ok {
		t.Error("wire initialize response missing agentCapabilities")
	}

	// --- Step 2: session/new -------------------------------------------
	newResp, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sessionID := newResp.SessionID
	if _, err := agent.ParseSessionID(sessionID); err != nil {
		t.Fatalf("NewSession returned an invalid sessionId %q: %v", sessionID, err)
	}

	newReqFrame := awaitWireFrame(t, clientPipe.written, &clientPos, "session/new request", isRequestFor("session/new"))
	newReqID, ok := newReqFrame["id"].(float64)
	if !ok {
		t.Fatalf("session/new request frame has no numeric id: %+v", newReqFrame)
	}
	newRespFrame := awaitWireFrame(t, serverPipe.written, &serverPos, "session/new response", isResponseTo(newReqID))
	newResult, ok := newRespFrame["result"].(map[string]any)
	if !ok {
		t.Fatalf("session/new response frame has no result object: %+v", newRespFrame)
	}
	if got, _ := newResult["sessionId"].(string); got != string(sessionID) {
		t.Errorf("wire session/new response sessionId = %q, want %q", got, sessionID)
	}

	// --- Step 3: session/prompt, with a streamed update and a permission
	// round trip in the middle of the drain. ---------------------------
	type promptResult struct {
		resp *protocol.PromptResponse
		err  error
	}
	promptDone := make(chan promptResult, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("hello"),
		})
		promptDone <- promptResult{resp, err}
	}()

	promptReqFrame := awaitWireFrame(t, clientPipe.written, &clientPos, "session/prompt request", isRequestFor("session/prompt"))
	promptReqID, ok := promptReqFrame["id"].(float64)
	if !ok {
		t.Fatalf("session/prompt request frame has no numeric id: %+v", promptReqFrame)
	}

	cmdID := awaitSubmittedCommandID(t, fake)
	sessUUID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	loopID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	turnID, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	hdr := turnHeader(sessUUID, loopID, turnID, cmdID)
	send(t, fake.events, event.TurnStarted{Header: hdr})

	// Streamed update.
	send(t, fake.events, event.TokenDelta{Header: hdr, Chunk: &content.TextChunk{Text: "hi there"}})

	select {
	case n := <-updates:
		if n.SessionID != sessionID {
			t.Errorf("session/update SessionID = %q, want %q", n.SessionID, sessionID)
		}
		if n.Update.AgentMessageChunk == nil {
			t.Fatalf("session/update = %+v, want agent_message_chunk", n.Update)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for the streamed session/update notification")
	}

	updateFrame := awaitWireFrame(t, serverPipe.written, &serverPos, "session/update notification", isNotificationFor("session/update"))
	updateParams, ok := updateFrame["params"].(map[string]any)
	if !ok {
		t.Fatalf("session/update notification frame has no params object: %+v", updateFrame)
	}
	if got, _ := updateParams["sessionId"].(string); got != string(sessionID) {
		t.Errorf("wire session/update sessionId = %q, want %q", got, sessionID)
	}
	updateShape, ok := updateParams["update"].(map[string]any)
	if !ok {
		t.Fatalf("session/update notification has no update object: %+v", updateParams)
	}
	if _, ok := updateShape["content"]; !ok {
		t.Errorf("wire session/update.update has no content field: %+v", updateShape)
	}

	// Permission round trip.
	opened, gateID := permissionGateOpened(t, hdr)
	send(t, fake.events, opened)

	permReqFrame := awaitWireFrame(t, serverPipe.written, &serverPos, "session/request_permission request", isRequestFor("session/request_permission"))
	permReqID, ok := permReqFrame["id"].(float64)
	if !ok {
		t.Fatalf("session/request_permission request frame has no numeric id: %+v", permReqFrame)
	}
	permReqParams, ok := permReqFrame["params"].(map[string]any)
	if !ok {
		t.Fatalf("session/request_permission request frame has no params object: %+v", permReqFrame)
	}
	if got, _ := permReqParams["sessionId"].(string); got != string(sessionID) {
		t.Errorf("wire session/request_permission sessionId = %q, want %q", got, sessionID)
	}
	if opts, ok := permReqParams["options"].([]any); !ok || len(opts) != 3 {
		t.Errorf("wire session/request_permission options = %v, want 3 options", permReqParams["options"])
	}

	permRespFrame := awaitWireFrame(t, clientPipe.written, &clientPos, "session/request_permission response", isResponseTo(permReqID))
	permResult, ok := permRespFrame["result"].(map[string]any)
	if !ok {
		t.Fatalf("session/request_permission response frame has no result object: %+v", permRespFrame)
	}
	outcome, ok := permResult["outcome"].(map[string]any)
	if !ok {
		t.Fatalf("session/request_permission response result has no outcome object: %+v", permResult)
	}
	if got, _ := outcome["optionId"].(string); got != string(gate.ApprovalApprove) {
		t.Errorf("wire session/request_permission response outcome.optionId = %q, want %q", got, gate.ApprovalApprove)
	}

	if gotPermissionReq.SessionID != sessionID {
		t.Errorf("decoded RequestPermissionRequest.SessionID = %q, want %q", gotPermissionReq.SessionID, sessionID)
	}

	// The wire response is confirmed above, but the server-side handler's
	// own call to RespondGate happens in a goroutine not synchronized with
	// the moment that response hit the wire (mirroring the same poll used by
	// gates_test.go's TestGateRequestPermissionConnectionDeathFailsClosed and
	// close_test.go's TestHandleSessionCloseStateMachine): poll for it
	// rather than assuming it has already run.
	deadline := time.Now().Add(testTimeout)
	for len(fake.gateResponses()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	responses := fake.gateResponses()
	if len(responses) != 1 || responses[0].GateID != gateID || responses[0].Action != string(gate.ApprovalApprove) {
		t.Fatalf("RespondGate calls = %+v, want exactly one Approve for gate %v", responses, gateID)
	}

	send(t, fake.events, event.TurnDone{Header: hdr})

	select {
	case r := <-promptDone:
		if r.err != nil {
			t.Fatalf("Prompt: unexpected error: %v", r.err)
		}
		if r.resp.StopReason != protocol.StopReasonEndTurn {
			t.Errorf("Prompt StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonEndTurn)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for session/prompt to return")
	}

	promptRespFrame := awaitWireFrame(t, serverPipe.written, &serverPos, "session/prompt response", isResponseTo(promptReqID))
	promptResult2, ok := promptRespFrame["result"].(map[string]any)
	if !ok {
		t.Fatalf("session/prompt response frame has no result object: %+v", promptRespFrame)
	}
	if got, _ := promptResult2["stopReason"].(string); got != string(protocol.StopReasonEndTurn) {
		t.Errorf("wire session/prompt response stopReason = %q, want %q", got, protocol.StopReasonEndTurn)
	}

	// --- Step 4: session/cancel on a second prompt, then observe the
	// interrupted terminal. ----------------------------------------------
	promptDone2 := make(chan promptResult, 1)
	go func() {
		resp, err := agentConn.Prompt(context.Background(), protocol.PromptRequest{
			SessionID: sessionID,
			Prompt:    textPrompt("second"),
		})
		promptDone2 <- promptResult{resp, err}
	}()

	promptReq2Frame := awaitWireFrame(t, clientPipe.written, &clientPos, "second session/prompt request", isRequestFor("session/prompt"))
	promptReq2ID, ok := promptReq2Frame["id"].(float64)
	if !ok {
		t.Fatalf("second session/prompt request frame has no numeric id: %+v", promptReq2Frame)
	}

	cmdID2 := awaitSubmittedCommandID(t, fake)
	hdr2 := turnHeader(sessUUID, loopID, turnID, cmdID2)
	send(t, fake.events, event.TurnStarted{Header: hdr2})

	if err := agentConn.Cancel(context.Background(), protocol.CancelNotification{SessionID: sessionID}); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	cancelFrame := awaitWireFrame(t, clientPipe.written, &clientPos, "session/cancel notification", isNotificationFor("session/cancel"))
	cancelParams, ok := cancelFrame["params"].(map[string]any)
	if !ok {
		t.Fatalf("session/cancel notification frame has no params object: %+v", cancelFrame)
	}
	if got, _ := cancelParams["sessionId"].(string); got != string(sessionID) {
		t.Errorf("wire session/cancel sessionId = %q, want %q", got, sessionID)
	}

	select {
	case <-fake.interrupted:
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Interrupt to be called after session/cancel")
	}

	send(t, fake.events, event.TurnInterrupted{Header: hdr2})

	select {
	case r := <-promptDone2:
		if r.err != nil {
			t.Fatalf("second Prompt: unexpected error (cancellation must be reported as success): %v", r.err)
		}
		if r.resp.StopReason != protocol.StopReasonCancelled {
			t.Errorf("second Prompt StopReason = %v, want %v", r.resp.StopReason, protocol.StopReasonCancelled)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for second session/prompt to return after cancellation")
	}

	promptResp2Frame := awaitWireFrame(t, serverPipe.written, &serverPos, "second session/prompt response", isResponseTo(promptReq2ID))
	promptResult3, ok := promptResp2Frame["result"].(map[string]any)
	if !ok {
		t.Fatalf("second session/prompt response frame has no result object: %+v", promptResp2Frame)
	}
	if got, _ := promptResult3["stopReason"].(string); got != string(protocol.StopReasonCancelled) {
		t.Errorf("wire second session/prompt response stopReason = %q, want %q", got, protocol.StopReasonCancelled)
	}

	// --- Step 5: session/close ------------------------------------------
	closeResp, err := agentConn.CloseSession(context.Background(), protocol.CloseSessionRequest{SessionID: sessionID})
	if err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	if closeResp == nil {
		t.Fatal("CloseSession: resp = nil, want a non-nil CloseSessionResponse")
	}

	closeReqFrame := awaitWireFrame(t, clientPipe.written, &clientPos, "session/close request", isRequestFor("session/close"))
	closeReqID, ok := closeReqFrame["id"].(float64)
	if !ok {
		t.Fatalf("session/close request frame has no numeric id: %+v", closeReqFrame)
	}
	closeReqParams, ok := closeReqFrame["params"].(map[string]any)
	if !ok {
		t.Fatalf("session/close request frame has no params object: %+v", closeReqFrame)
	}
	if got, _ := closeReqParams["sessionId"].(string); got != string(sessionID) {
		t.Errorf("wire session/close sessionId = %q, want %q", got, sessionID)
	}
	closeRespFrame := awaitWireFrame(t, serverPipe.written, &serverPos, "session/close response", isResponseTo(closeReqID))
	if _, hasError := closeRespFrame["error"]; hasError {
		t.Errorf("session/close response frame has an error: %+v", closeRespFrame)
	}

	// The session must now be gone: a further session-scoped call fails
	// ResourceNotFound, proving close's registry removal genuinely ran.
	_, err = agentConn.Prompt(context.Background(), protocol.PromptRequest{SessionID: sessionID, Prompt: textPrompt("after close")})
	if err == nil {
		t.Fatal("Prompt after close: error = nil, want ResourceNotFound")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("Prompt after close: error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeResourceNotFound {
		t.Errorf("Prompt after close: Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeResourceNotFound)
	}
}
