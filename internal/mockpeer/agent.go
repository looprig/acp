package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/looprig/acp/protocol"
)

// agentPeer holds mockpeer's default-mode session state: just enough to
// reject a prompt for a session id it never issued, and to mint session ids
// that are unique within one process lifetime.
type agentPeer struct {
	mu       sync.Mutex
	sessions map[protocol.SessionID]struct{}

	nextSessionID atomic.Uint64
}

func newAgentPeer() *agentPeer {
	return &agentPeer{sessions: make(map[protocol.SessionID]struct{})}
}

// register binds every method mockpeer answers as an agent to conn. The
// session/prompt handler additionally needs conn itself (to call back out
// for session/update notifications and the session/request_permission
// request), so it is wrapped in a closure rather than being a bound method
// value like the others.
func (a *agentPeer) register(conn *protocol.Conn) {
	conn.Handle(string(protocol.MethodInitialize), a.handleInitialize)
	conn.Handle(string(protocol.MethodSessionNew), a.handleNewSession)
	conn.Handle(string(protocol.MethodSessionPrompt), func(ctx context.Context, _ string, params json.RawMessage) (any, error) {
		return a.handlePrompt(ctx, conn, params)
	})
}

func (a *agentPeer) handleInitialize(_ context.Context, _ string, params json.RawMessage) (any, error) {
	var req protocol.InitializeRequest
	if len(params) > 0 {
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, protocol.InvalidParams("initialize: decode params", err)
		}
	}

	return protocol.InitializeResponse{
		ProtocolVersion: protocol.CurrentProtocolVersion,
		AgentInfo: &protocol.Implementation{
			Name:    "acp-mockpeer",
			Version: "0.1.0",
		},
	}, nil
}

func (a *agentPeer) handleNewSession(_ context.Context, _ string, params json.RawMessage) (any, error) {
	var req protocol.NewSessionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("session/new: decode params", err)
	}
	if req.Cwd == "" {
		return nil, protocol.InvalidParams("session/new: cwd is required", nil)
	}

	id := protocol.SessionID(fmt.Sprintf("mockpeer-session-%d", a.nextSessionID.Add(1)))
	a.mu.Lock()
	a.sessions[id] = struct{}{}
	a.mu.Unlock()

	return protocol.NewSessionResponse{SessionID: id}, nil
}

// mockToolCallID is the id shared by every notification and request in one
// prompt turn's fixed update sequence below, so a test driving mockpeer can
// correlate the tool_call, tool_call_update, and session/request_permission
// events without mockpeer needing to invent and communicate ids out of band.
const mockToolCallID protocol.ToolCallID = "mockpeer-tool-1"

// handlePrompt answers session/prompt: it streams the fixed update sequence
// (an agent message chunk, an agent thought chunk, a tool call, a tool call
// update, and a plan) as session/update notifications, issues one
// session/request_permission call back to the peer, and ends the turn.
//
// The permission outcome is advisory only: the update sequence above has
// already been sent by the time it is requested, and a peer that has not
// wired up permission handling still gets a complete, well-formed prompt
// turn — only a diagnostic is printed to stderr if that call fails.
func (a *agentPeer) handlePrompt(ctx context.Context, conn *protocol.Conn, params json.RawMessage) (any, error) {
	var req protocol.PromptRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("session/prompt: decode params", err)
	}

	a.mu.Lock()
	_, known := a.sessions[req.SessionID]
	a.mu.Unlock()
	if !known {
		return nil, protocol.InvalidParams(fmt.Sprintf("session/prompt: unknown session %q", req.SessionID), nil)
	}

	client := protocol.NewClientConn(conn)

	for _, update := range promptUpdateSequence() {
		n := protocol.SessionNotification{SessionID: req.SessionID, Update: update}
		if err := client.SessionUpdate(ctx, n); err != nil {
			return nil, protocol.InternalError("session/update: send", err)
		}
	}

	permReq := protocol.RequestPermissionRequest{
		SessionID: req.SessionID,
		ToolCall: protocol.ToolCallUpdate{
			ToolCallID: mockToolCallID,
			Title:      strPtr("Reading a file"),
		},
		Options: []protocol.PermissionOption{
			{OptionID: "allow-once", Name: "Allow once", Kind: protocol.PermissionOptionKindAllowOnce},
			{OptionID: "reject-once", Name: "Reject once", Kind: protocol.PermissionOptionKindRejectOnce},
		},
	}
	if _, err := client.RequestPermission(ctx, permReq); err != nil {
		fmt.Fprintf(os.Stderr, "mockpeer: session/request_permission: %v\n", err)
	}

	return protocol.PromptResponse{StopReason: protocol.StopReasonEndTurn}, nil
}

// promptUpdateSequence is the fixed sequence of session/update payloads one
// default-mode prompt turn streams, in order: an agent message chunk, an
// agent thought chunk, a tool call, an update to that same tool call, and a
// plan.
func promptUpdateSequence() []protocol.SessionUpdate {
	return []protocol.SessionUpdate{
		{AgentMessageChunk: &protocol.ContentChunk{Content: textBlock("Looking into that now.")}},
		{AgentThoughtChunk: &protocol.ContentChunk{Content: textBlock("The file probably needs a quick read first.")}},
		{ToolCall: &protocol.ToolCall{
			ToolCallID: mockToolCallID,
			Title:      "Reading a file",
			Kind:       toolKindPtr(protocol.ToolKindRead),
			Status:     toolStatusPtr(protocol.ToolCallStatusPending),
		}},
		{ToolCallUpdate: &protocol.ToolCallUpdate{
			ToolCallID: mockToolCallID,
			Status:     toolStatusPtr(protocol.ToolCallStatusCompleted),
			Content: []protocol.ToolCallContent{
				{Content: &protocol.Content{Content: textBlock("file contents read successfully")}},
			},
		}},
		{Plan: &protocol.Plan{
			Entries: []protocol.PlanEntry{
				{Content: "Read the relevant file", Priority: protocol.PlanEntryPriorityHigh, Status: protocol.PlanEntryStatusCompleted},
				{Content: "Report back to the user", Priority: protocol.PlanEntryPriorityMedium, Status: protocol.PlanEntryStatusInProgress},
			},
		}},
	}
}

func textBlock(text string) protocol.ContentBlock {
	return protocol.ContentBlock{Text: &protocol.TextContent{Text: text}}
}

func toolKindPtr(k protocol.ToolKind) *protocol.ToolKind { return &k }

func toolStatusPtr(s protocol.ToolCallStatus) *protocol.ToolCallStatus { return &s }

func strPtr(s string) *string { return &s }
