package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/looprig/acp/agent"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/identity"
)

// gateWaitSafetyTimeout bounds how long a turn blocks waiting for RespondGate
// before giving up and denying itself, and cancelSafetyTimeout bounds how
// long the "trigger-cancel" flow waits for Interrupt before giving up and
// completing normally. Both exist only so a test bug (forgetting to answer a
// gate, or forgetting to cancel) can never wedge exampleagent's turn
// goroutine forever; a well-behaved caller always resolves well within these
// bounds.
const (
	gateWaitSafetyTimeout   = 60 * time.Second
	cancelSafetyTimeout     = 30 * time.Second
	triggerPermissionMarker = "trigger-permission"
	triggerCancelMarker     = "trigger-cancel"
)

// subscription is the single-consumer event.Subscription liveSession hands
// back from SubscribeEvents: it wraps the session's one reused delivery
// channel (see liveSession.events's doc). Close and Err are no-ops — there is
// nothing to release and no loss cause to report, since the channel is never
// closed out from under a subscriber.
type subscription struct{ ch chan event.Delivery }

func (s *subscription) Events() <-chan event.Delivery { return s.ch }
func (s *subscription) Close() error                  { return nil }
func (s *subscription) Err() error                    { return nil }

// pendingGate is the single in-flight permission gate a liveSession's running
// turn may be waiting on. resp is buffered (depth 1) so RespondGate never
// blocks on delivering the answer even if the turn goroutine has already
// moved on (e.g. because it was interrupted first).
type pendingGate struct {
	id   uuid.UUID
	resp chan gate.GateResponse
}

// liveSession is exampleagent's agent.LiveSession (+ agent.SessionCloser): a
// single simulated Harness-shaped session. Every session/prompt call it
// serves subscribes to and publishes on the SAME reused channel (events),
// mirroring the fakeLiveSession test pattern in acp/agent's own tests —
// correct here because, exactly like there, at most one prompt is ever in
// flight per session (enforced by the facade's promptTracker), so there is
// never more than one active producer or consumer of that channel at a time.
type liveSession struct {
	id uuid.UUID
	st *sessionState

	events chan event.Delivery

	mu          sync.Mutex
	turnCancel  context.CancelFunc
	pendingGate *pendingGate
	closed      bool

	toolCallSeq atomic.Uint64
}

func newLiveSession(id uuid.UUID, st *sessionState) *liveSession {
	return &liveSession{id: id, st: st, events: make(chan event.Delivery)}
}

func (s *liveSession) SessionID() uuid.UUID { return s.id }

func (s *liveSession) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return &subscription{ch: s.events}, nil
}

// Submit implements agent.LiveSession: it mints a command id, starts the
// scripted turn in its own goroutine (so Submit itself returns immediately,
// matching Harness's own fire-and-forget Submit contract — see
// agent/host.go's LiveSession doc), and returns.
func (s *liveSession) Submit(_ context.Context, blocks []content.Block) (uuid.UUID, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return uuid.UUID{}, fmt.Errorf("exampleagent: session %s is closed", s.id)
	}
	s.mu.Unlock()

	cmdID, err := uuid.New()
	if err != nil {
		return uuid.UUID{}, err
	}
	turnCtx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.turnCancel = cancel
	s.mu.Unlock()

	go s.runTurn(turnCtx, cmdID, blocks)
	return cmdID, nil
}

// RespondGate implements agent.LiveSession: it delivers resp to the pending
// gate wait matching resp.GateID, failing closed if no gate is open at all or
// the id does not match the one actually open (an ACP facade must never be
// able to answer a stale or foreign gate id).
func (s *liveSession) RespondGate(_ context.Context, resp gate.GateResponse) error {
	s.mu.Lock()
	pg := s.pendingGate
	s.mu.Unlock()
	if pg == nil {
		return fmt.Errorf("exampleagent: RespondGate: no gate is currently open for session %s", s.id)
	}
	if pg.id != resp.GateID {
		return fmt.Errorf("exampleagent: RespondGate: gate %s is not the open gate %s", resp.GateID, pg.id)
	}
	pg.resp <- resp
	return nil
}

// Interrupt implements agent.LiveSession: it cancels the currently running
// turn (if any) and reports whether one was actually running to interrupt.
func (s *liveSession) Interrupt(context.Context) (bool, error) {
	s.mu.Lock()
	cancel := s.turnCancel
	s.mu.Unlock()
	if cancel == nil {
		return false, nil
	}
	cancel()
	return true, nil
}

// Shutdown implements agent.SessionCloser: it marks the session closed so a
// later Submit fails closed rather than silently starting a turn on a torn-
// down session. It never touches the durable log or metadata — those belong
// to sessionState and outlive Shutdown (see host.go's doc: session/close must
// never delete durable history).
func (s *liveSession) Shutdown(context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

// --- turn scripting -------------------------------------------------------

// newHeader mints a fresh event.Header for one event: a new EventID and
// CreatedAt every call (every real Harness event carries its own unique
// identity), the fixed session/loop/turn coordinates, and — only when cause
// is non-zero — the submit command id, matching real Harness semantics: only
// the turn-resolution events (TurnStarted, TurnFoldedInto, InputCancelled)
// stamp Cause.CommandID (see event.Header's doc); every other event in the
// turn leaves it zero.
func newHeader(sessionID, loopID, turnID, cause uuid.UUID) (event.Header, error) {
	eventID, err := uuid.New()
	if err != nil {
		return event.Header{}, err
	}
	hdr := event.Header{
		Coordinates: identity.Coordinates{SessionID: sessionID, LoopID: loopID, TurnID: turnID},
		EventID:     eventID,
		CreatedAt:   time.Now().UTC(),
	}
	if cause != (uuid.UUID{}) {
		hdr.Cause = identity.Cause{CommandID: cause}
	}
	return hdr, nil
}

// promptText concatenates every text block in blocks (the only block variant
// the facade's blocksFromPrompt ever produces — see acp/agent/prompt.go),
// separated by newlines, to recover the plain-text prompt this session's
// scripted turn dispatches on (see classifyPrompt).
func promptText(blocks []content.Block) string {
	var sb strings.Builder
	for _, b := range blocks {
		tb, ok := b.(*content.TextBlock)
		if !ok {
			continue
		}
		if sb.Len() > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(tb.Text)
	}
	return sb.String()
}

// turnKind classifies a prompt's text into one of exampleagent's three
// scripted flows (see this package's doc comment).
type turnKind int

const (
	turnDefault turnKind = iota
	turnPermission
	turnCancel
)

func classifyPrompt(text string) turnKind {
	switch {
	case strings.Contains(text, triggerPermissionMarker):
		return turnPermission
	case strings.Contains(text, triggerCancelMarker):
		return turnCancel
	default:
		return turnDefault
	}
}

// sessionTitle derives a session/list title from a prompt: its first line,
// truncated, mirroring sessionstore.SessionMeta.Title's own documented
// derivation ("a short, human-readable label derived from the first turn's
// user message (its first line, truncated)" — harness/pkg/sessionstore/catalog.go).
func sessionTitle(text string) string {
	line := text
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	const maxTitleLen = 60
	if len(line) > maxTitleLen {
		line = line[:maxTitleLen]
	}
	return line
}

// runTurn is the whole body of one simulated turn, run in its own goroutine
// from Submit. It always publishes a TurnStarted first and a terminal
// (TurnDone or TurnInterrupted) last; everything in between depends on
// classifyPrompt(promptText(blocks)).
func (s *liveSession) runTurn(turnCtx context.Context, cmdID uuid.UUID, blocks []content.Block) {
	turnID, err := uuid.New()
	if err != nil {
		return // no id generator available; nothing this goroutine can safely publish
	}
	loopID := s.st.loopID
	text := promptText(blocks)

	turnIndex := event.TurnIndex(s.st.log.turnCount())
	userMsg := &content.UserMessage{Message: content.Message{Role: content.RoleUser, Blocks: blocks}}

	startHdr, err := newHeader(s.id, loopID, turnID, cmdID)
	if err != nil {
		return
	}
	startHdr.EventVisibility = event.Public
	turnStarted := event.TurnStarted{Header: startHdr, TurnIndex: turnIndex, Message: userMsg}
	s.st.log.append(turnStarted)
	s.st.touch(time.Now().UTC(), sessionTitle(text))
	if !s.publish(turnStarted) {
		return
	}

	switch classifyPrompt(text) {
	case turnPermission:
		s.runPermissionTurn(turnCtx, loopID, turnID, turnIndex)
	case turnCancel:
		s.runCancelTurn(turnCtx, loopID, turnID, turnIndex)
	default:
		s.runDefaultTurn(turnCtx, loopID, turnID, turnIndex)
	}

	s.mu.Lock()
	s.turnCancel = nil
	s.mu.Unlock()
}

// publish sends ev on the session's reused delivery channel, reporting false
// (without sending) if the session has already been shut down. This is a
// best-effort guard, not an airtight one — Shutdown is only ever called after
// session/close's own drain wait has already observed this turn's terminal
// (see close.go's six-step orchestration), so in practice a running turn
// always reaches its own terminal publish before Shutdown could ever
// observe s.closed here; the check exists purely so a hypothetical future
// caller of Shutdown outside that sequencing can never wedge this goroutine
// forever on a channel nothing will ever drain again. Every other publish
// call in this file is unconditional: at most one turn is ever running per
// session, and the facade's drain loop is always either actively reading
// this channel or deliberately paused mid-gate-round-trip (during which this
// session's own turn goroutine never calls publish either — see
// runPermissionTurn).
func (s *liveSession) publish(ev event.Event) bool {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return false
	}
	s.events <- event.Delivery{Event: ev}
	return true
}

func (s *liveSession) header(loopID, turnID uuid.UUID) event.Header {
	hdr, err := newHeader(s.id, loopID, turnID, uuid.UUID{})
	if err != nil {
		// uuid.New only fails if crypto/rand itself is broken; there is no
		// safe fallback identity to mint here. Returning the zero Header
		// would carry a zero EventID, which is a real (if degenerate)
		// value the facade already tolerates (see event.Header's EventID
		// doc), so this deliberately does not panic.
		return event.Header{}
	}
	hdr.EventVisibility = event.Public
	return hdr
}

// mintToolCall returns a fresh Harness ToolExecutionID (ephemeral, in-memory
// identity) and a distinct provider tool-use id string (the durable identity
// a content.ToolUseBlock/ToolResultMessage carries) for one tool call — see
// agent/translate.go's toolCallID and agent/replay.go's replayToolCallID docs
// for why these are deliberately two different kinds of identity.
func (s *liveSession) mintToolCall() (execID uuid.UUID, providerID string, err error) {
	execID, err = uuid.New()
	if err != nil {
		return uuid.UUID{}, "", err
	}
	n := s.toolCallSeq.Add(1)
	return execID, fmt.Sprintf("tool-%d", n), nil
}

// runDefaultTurn streams a short reply plus one already-approved tool call
// (no permission gate involved) and ends in stopReason: end_turn.
func (s *liveSession) runDefaultTurn(turnCtx context.Context, loopID, turnID uuid.UUID, turnIndex event.TurnIndex) {
	hdr := s.header(loopID, turnID)
	chunk1, chunk2 := "Sure, ", "let me check that file for you."
	if !s.publish(event.TokenDelta{Header: hdr, TurnIndex: turnIndex, Chunk: &content.TextChunk{Text: chunk1}}) {
		return
	}
	hdr = s.header(loopID, turnID)
	if !s.publish(event.TokenDelta{Header: hdr, TurnIndex: turnIndex, Chunk: &content.TextChunk{Text: chunk2}}) {
		return
	}

	execID, providerID, err := s.mintToolCall()
	if err != nil {
		return
	}
	hdr = s.header(loopID, turnID)
	if !s.publish(event.ToolCallStarted{Header: hdr, ToolExecutionID: execID, ToolName: "read_file", Summary: "Reading example.txt"}) {
		return
	}
	const resultPreview = "example.txt: 42 lines"
	hdr = s.header(loopID, turnID)
	if !s.publish(event.ToolCallCompleted{Header: hdr, ToolExecutionID: execID, ResultPreview: resultPreview}) {
		return
	}

	fullText := chunk1 + chunk2
	aiMsg := &content.AIMessage{
		Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{
			&content.TextBlock{Text: fullText},
			&content.ToolUseBlock{ID: providerID, Name: "read_file", Input: json.RawMessage(`{"path":"example.txt"}`)},
		}},
		Usage: &content.Usage{InputTokens: 128, OutputTokens: 32},
	}
	toolResult := &content.ToolResultMessage{
		Message:   content.Message{Role: content.RoleTool, Blocks: []content.Block{&content.TextBlock{Text: resultPreview}}},
		ToolUseID: providerID,
	}
	hdr = s.header(loopID, turnID)
	s.st.log.append(event.StepDone{Header: hdr, Messages: content.AgenticMessages{aiMsg, toolResult}})
	if !s.publish(event.StepDone{Header: hdr, Messages: content.AgenticMessages{aiMsg, toolResult}}) {
		return
	}

	hdr = s.header(loopID, turnID)
	s.publish(event.TurnDone{Header: hdr, TurnIndex: turnIndex, Message: aiMsg, Usage: *aiMsg.Usage})
	s.st.touch(time.Now().UTC(), "")
}

// runPermissionTurn opens a gate.KindPermission gate and blocks (up to
// gateWaitSafetyTimeout, or until turnCtx is cancelled by Interrupt) for
// RespondGate before either running the tool call (approved) or replying
// that it will not run it (denied).
func (s *liveSession) runPermissionTurn(turnCtx context.Context, loopID, turnID uuid.UUID, turnIndex event.TurnIndex) {
	hdr := s.header(loopID, turnID)
	if !s.publish(event.TokenDelta{Header: hdr, TurnIndex: turnIndex, Chunk: &content.TextChunk{Text: "I need permission before running that tool."}}) {
		return
	}

	execID, providerID, err := s.mintToolCall()
	if err != nil {
		return
	}
	gateID, err := uuid.New()
	if err != nil {
		return
	}
	g := gate.Gate{
		ID:       gateID,
		Kind:     gate.KindPermission,
		Resolver: gate.ResolverLoop,
		Subject:  gate.Subject{ToolExecutionID: execID},
		Prompt: gate.Prompt{
			Title:    "Run example_tool",
			Body:     "example_tool wants to modify a file in the workspace.",
			Controls: gate.ApprovalControls(),
		},
	}

	pg := &pendingGate{id: gateID, resp: make(chan gate.GateResponse, 1)}
	s.mu.Lock()
	s.pendingGate = pg
	s.mu.Unlock()

	hdr = s.header(loopID, turnID)
	if !s.publish(event.GateOpened{Header: hdr, Gate: g}) {
		return
	}

	var resp gate.GateResponse
	select {
	case resp = <-pg.resp:
	case <-turnCtx.Done():
		s.mu.Lock()
		s.pendingGate = nil
		s.mu.Unlock()
		hdr = s.header(loopID, turnID)
		s.publish(event.TurnInterrupted{Header: hdr, TurnIndex: turnIndex})
		return
	case <-time.After(gateWaitSafetyTimeout):
		resp = gate.GateResponse{GateID: gateID, Action: string(gate.ApprovalDeny)}
	}
	s.mu.Lock()
	s.pendingGate = nil
	s.mu.Unlock()

	var aiMsg *content.AIMessage
	var messages content.AgenticMessages
	if resp.Action == string(gate.ApprovalDeny) {
		const denyText = "Okay, I will not run that tool."
		hdr = s.header(loopID, turnID)
		if !s.publish(event.TokenDelta{Header: hdr, TurnIndex: turnIndex, Chunk: &content.TextChunk{Text: denyText}}) {
			return
		}
		aiMsg = &content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{&content.TextBlock{Text: denyText}}}}
		messages = content.AgenticMessages{aiMsg}
	} else {
		hdr = s.header(loopID, turnID)
		if !s.publish(event.ToolCallStarted{Header: hdr, ToolExecutionID: execID, ToolName: "example_tool", Summary: "Running example_tool"}) {
			return
		}
		const resultPreview = "example_tool completed successfully"
		hdr = s.header(loopID, turnID)
		if !s.publish(event.ToolCallCompleted{Header: hdr, ToolExecutionID: execID, ResultPreview: resultPreview}) {
			return
		}
		const replyText = "I ran example_tool for you."
		aiMsg = &content.AIMessage{
			Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{
				&content.TextBlock{Text: replyText},
				&content.ToolUseBlock{ID: providerID, Name: "example_tool", Input: json.RawMessage(`{}`)},
			}},
		}
		toolResult := &content.ToolResultMessage{
			Message:   content.Message{Role: content.RoleTool, Blocks: []content.Block{&content.TextBlock{Text: resultPreview}}},
			ToolUseID: providerID,
		}
		messages = content.AgenticMessages{aiMsg, toolResult}
	}

	hdr = s.header(loopID, turnID)
	s.st.log.append(event.StepDone{Header: hdr, Messages: messages})
	if !s.publish(event.StepDone{Header: hdr, Messages: messages}) {
		return
	}

	hdr = s.header(loopID, turnID)
	usage := content.Usage{}
	if aiMsg.Usage != nil {
		usage = *aiMsg.Usage
	}
	s.publish(event.TurnDone{Header: hdr, TurnIndex: turnIndex, Message: aiMsg, Usage: usage})
	s.st.touch(time.Now().UTC(), "")
}

// runCancelTurn pauses mid-stream so a caller has a real window to send
// session/cancel and observe TurnInterrupted -> stopReason: cancelled. If
// nothing cancels it within cancelSafetyTimeout, it completes normally
// instead of hanging forever (see this file's constant doc).
func (s *liveSession) runCancelTurn(turnCtx context.Context, loopID, turnID uuid.UUID, turnIndex event.TurnIndex) {
	hdr := s.header(loopID, turnID)
	if !s.publish(event.TokenDelta{Header: hdr, TurnIndex: turnIndex, Chunk: &content.TextChunk{Text: "Working on a long task..."}}) {
		return
	}

	select {
	case <-turnCtx.Done():
		hdr = s.header(loopID, turnID)
		s.publish(event.TurnInterrupted{Header: hdr, TurnIndex: turnIndex})
		return
	case <-time.After(cancelSafetyTimeout):
	}

	const replyText = "All done."
	hdr = s.header(loopID, turnID)
	if !s.publish(event.TokenDelta{Header: hdr, TurnIndex: turnIndex, Chunk: &content.TextChunk{Text: replyText}}) {
		return
	}
	aiMsg := &content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{&content.TextBlock{Text: replyText}}}}
	hdr = s.header(loopID, turnID)
	s.st.log.append(event.StepDone{Header: hdr, Messages: content.AgenticMessages{aiMsg}})
	if !s.publish(event.StepDone{Header: hdr, Messages: content.AgenticMessages{aiMsg}}) {
		return
	}
	hdr = s.header(loopID, turnID)
	s.publish(event.TurnDone{Header: hdr, TurnIndex: turnIndex, Message: aiMsg})
	s.st.touch(time.Now().UTC(), "")
}

var _ agent.LiveSession = (*liveSession)(nil)
var _ agent.SessionCloser = (*liveSession)(nil)
var _ agent.SessionHost = (*Host)(nil)
var _ agent.EventReplayer = (*Host)(nil)
var _ agent.SessionCatalog = (*Host)(nil)
