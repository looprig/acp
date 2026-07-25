// translate.go implements the live event translator: Task 2.5 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// prompt.go's drainToTerminal (Task 2.4) already correlates a session/prompt
// to its exact turn (LoopID/TurnID) and drains that turn's event stream to
// its terminal. Until now it silently discarded every non-terminal event it
// saw along the way ("progress event ...: not a terminal"). This file adds
// the translator drainToTerminal uses to turn each of those in-flight,
// public events into the ACP session/update notification it forwards to the
// client, so a prompt's progress is actually streamed live rather than only
// observed internally while the client waits on the terminal response.
//
// Harness events carry two orthogonal classifications this translator must
// respect: Class (Ephemeral/Enduring — durability, irrelevant here) and
// EventVisibility (Public/Internal). Only Public events are ever translated;
// this is a hard security boundary, not a convenience filter, so it is
// checked directly by Translate rather than trusted solely to the Harness
// hub's own delivery filter (see event.ShouldDeliver) upstream.
package agent

import (
	"encoding/json"
	"fmt"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
)

// liveTranslator turns one correlated turn's public Harness events into ACP
// session/update notifications. drainToTerminal constructs one fresh
// instance per session/prompt call, once TurnStarted has established the
// turn's LoopID/TurnID, and calls Translate once per subsequent event it
// observes for that turn.
//
// It carries a small amount of mutable state: the running message-id
// sequence. ACP's ContentChunk.MessageID groups consecutive chunks
// belonging to one logical message ("a change in messageId indicates a new
// message has started"), but Harness's TokenDelta deliberately never carries
// a StepID (see harness/internal/loopruntime/header.go's stampStepID: it
// names TokenDelta explicitly as one of the events that must keep StepID
// zero). The only signal a live translator actually has for a message
// boundary is therefore a change in streamed chunk kind (text versus
// thinking) — a step that streamed thinking then switched to text, or a
// later step's stream starting after an intervening tool call, starts a new
// message id. This is a deliberate, documented design decision for Task 2.5
// (see the implementation task's ambiguity notes), not an inherited
// contract from elsewhere.
type liveTranslator struct {
	sessionID protocol.SessionID
	loopID    uuid.UUID
	turnID    uuid.UUID
	promptID  uuid.UUID

	haveMessage    bool
	messageThought bool
	messageSeq     uint64
}

// newLiveTranslator constructs the translator for one correlated turn.
// sessionID is the ACP wire session id; loopID/turnID are the coordinates
// drainToTerminal correlated from TurnStarted; promptID is the submitted
// command id (session/prompt's own correlation handle), stamped into every
// update's _meta.promptId so a client can associate a live update with the
// session/prompt call it belongs to.
func newLiveTranslator(sessionID protocol.SessionID, loopID, turnID, promptID uuid.UUID) *liveTranslator {
	return &liveTranslator{sessionID: sessionID, loopID: loopID, turnID: turnID, promptID: promptID}
}

// Translate maps one Harness event into the ACP session/update notification
// it produces, or reports ok=false when ev is not translatable into a live
// update. That happens for three distinct reasons, all deliberate:
//
//   - ev.Visibility() is not Public: internal events must never reach the
//     wire (the hard security boundary this file exists to enforce, not just
//     assume).
//   - ev is a turn terminal (TurnDone/TurnFailed/TurnInterrupted): those
//     resolve session/prompt's own response (see drainToTerminal) rather than
//     a session/update. TurnDone.Usage is inspected here too (see
//     translateTurnUsage) but, per "drop, don't guess," never actually
//     produces one — see that function's doc for why.
//   - ev's concrete type/payload has no representable ACP update: a
//     ToolUseChunk TokenDelta (ACP has no streaming tool-argument-delta
//     update), or any other event kind this translator does not know about
//     (StepDone and the rest of the Enduring bookkeeping events).
func (t *liveTranslator) Translate(ev event.Event) (protocol.SessionNotification, bool) {
	if ev.Visibility() != event.Public {
		return protocol.SessionNotification{}, false
	}

	var update protocol.SessionUpdate
	switch e := ev.(type) {
	case event.TokenDelta:
		u, ok := t.translateTokenDelta(e)
		if !ok {
			return protocol.SessionNotification{}, false
		}
		update = u
	case event.ToolCallStarted:
		update = translateToolCallStarted(e)
	case event.ToolCallCompleted:
		update = translateToolCallCompleted(e)
	case event.ContextMeasured:
		u, ok := translateContextMeasurement(e.Measurement)
		if !ok {
			return protocol.SessionNotification{}, false
		}
		update = u
	case event.ContextPressure:
		u, ok := translateContextMeasurement(e.Measurement)
		if !ok {
			return protocol.SessionNotification{}, false
		}
		update = u
	case event.TurnDone:
		u, ok := translateTurnUsage(e.Usage)
		if !ok {
			return protocol.SessionNotification{}, false
		}
		update = u
	default:
		return protocol.SessionNotification{}, false
	}

	hdr := ev.EventHeader()
	return protocol.SessionNotification{
		SessionID: t.sessionID,
		Update:    update,
		Meta:      t.meta(hdr.EventID),
	}, true
}

// translateTokenDelta classifies e.Chunk — the distinction between a text
// and thinking update is not a field on TokenDelta itself, it lives in the
// chunk's concrete type — into an agent_message_chunk or agent_thought_chunk
// update. A ToolUseChunk (or any future Chunk variant) reports ok=false: ACP
// has no streaming tool-argument-delta update to represent it as.
func (t *liveTranslator) translateTokenDelta(e event.TokenDelta) (protocol.SessionUpdate, bool) {
	switch c := e.Chunk.(type) {
	case *content.TextChunk:
		id := t.messageID(false)
		return protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{
			Content:   protocol.ContentBlock{Text: &protocol.TextContent{Text: c.Text}},
			MessageID: &id,
		}}, true
	case *content.ThinkingChunk:
		id := t.messageID(true)
		return protocol.SessionUpdate{AgentThoughtChunk: &protocol.ContentChunk{
			Content:   protocol.ContentBlock{Text: &protocol.TextContent{Text: c.Thinking}},
			MessageID: &id,
		}}, true
	default:
		return protocol.SessionUpdate{}, false
	}
}

// messageID returns the deterministic message id for the current run of
// same-kind (thought vs non-thought) chunks, minting the next sequence
// number the first time it observes a switch away from the previously seen
// kind (see liveTranslator's doc comment). The format is
// "msg:{sessionID}:{loopID}:{turnID}:{seq}": deterministic and reproducible
// given the same ordered event stream, never a random value.
func (t *liveTranslator) messageID(thought bool) protocol.MessageID {
	switch {
	case !t.haveMessage:
		t.haveMessage = true
		t.messageThought = thought
	case t.messageThought != thought:
		t.messageSeq++
		t.messageThought = thought
	}
	return protocol.MessageID(fmt.Sprintf("msg:%s:%s:%s:%d", t.sessionID, t.loopID, t.turnID, t.messageSeq))
}

// toolCallID derives the deterministic ACP tool call id from a Harness
// ToolExecutionID: its string form, verbatim. Harness already mints
// ToolExecutionID as the stable per-call identity (stamped on both
// ToolCallStarted and ToolCallCompleted for the same call), so no further
// derivation is needed or wanted.
func toolCallID(id uuid.UUID) protocol.ToolCallID {
	return protocol.ToolCallID(id.String())
}

// translateToolCallStarted maps ToolCallStarted onto ACP's tool_call update:
// the notification that a new tool call has been initiated. Title prefers
// Summary (the human-readable "what the tool is doing" text ToolCallStarted
// already caps at construction) and falls back to ToolName if Summary is
// empty. Status is in_progress: ToolCallStarted "is emitted when an approved
// tool begins executing" (tool.go), never before. RawInput/Kind/Locations
// are left unset: Harness's ToolCallStarted carries no raw arguments (by its
// own doc, "never raw args") and no structured tool-kind or location data
// this facade could map without guessing.
func translateToolCallStarted(e event.ToolCallStarted) protocol.SessionUpdate {
	title := e.Summary
	if title == "" {
		title = e.ToolName
	}
	status := protocol.ToolCallStatusInProgress
	return protocol.SessionUpdate{ToolCall: &protocol.ToolCall{
		ToolCallID: toolCallID(e.ToolExecutionID),
		Title:      title,
		Status:     &status,
	}}
}

// translateToolCallCompleted maps ToolCallCompleted onto ACP's terminal
// tool_call_update: Harness has no intermediate tool-progress event (tool
// lifecycle is exactly ToolCallStarted then ToolCallCompleted), so this is
// always the one and only update completing the tool_call this translator
// already emitted. IsError maps to ToolCallStatusFailed; otherwise the call
// completed successfully. ResultPreview — the capped tool output Harness
// already prepared for display — becomes the update's content when
// non-empty; RawOutput is left unset since Harness never hands this
// translator the raw result to put there.
func translateToolCallCompleted(e event.ToolCallCompleted) protocol.SessionUpdate {
	status := protocol.ToolCallStatusCompleted
	if e.IsError {
		status = protocol.ToolCallStatusFailed
	}
	upd := &protocol.ToolCallUpdate{
		ToolCallID: toolCallID(e.ToolExecutionID),
		Status:     &status,
	}
	if e.ResultPreview != "" {
		upd.Content = []protocol.ToolCallContent{{
			Content: &protocol.Content{
				Content: protocol.ContentBlock{Text: &protocol.TextContent{Text: e.ResultPreview}},
			},
		}}
	}
	return protocol.SessionUpdate{ToolCallUpdate: upd}
}

// translateContextMeasurement maps a Harness ContextMeasurement onto ACP's
// usage_update. UsageUpdate's Size/Used fields are context-window-occupancy
// semantics by their own doc ("Total context window size in tokens" /
// "Tokens currently in context") — exactly what ContextMeasurement's
// InputLimit/InputTokens already are, so the mapping is direct and always
// representable. Cost is never populated: Harness carries no pricing or
// currency data anywhere on this measurement (Harness deliberately has no
// model/effort catalog of its own), so fabricating one would be guessing,
// not mapping; it is always left nil.
func translateContextMeasurement(m event.ContextMeasurement) (protocol.SessionUpdate, bool) {
	return protocol.SessionUpdate{UsageUpdate: &protocol.UsageUpdate{
		Size: uint64(m.InputLimit),
		Used: uint64(m.InputTokens),
	}}, true
}

// translateTurnUsage decides whether a completed turn's accumulated
// content.Usage can be represented as ACP's usage_update, and — after
// reading Usage's actual shape — always reports it cannot.
//
// UsageUpdate's required Size field is a context-WINDOW-size concept (see
// translateContextMeasurement); content.Usage carries no window-limit
// concept anywhere — TurnDone's own doc describes it as "the checked sum of
// every completed request in this turn," a token-spend total, not a window
// occupancy snapshot. There is no non-fabricated value this translator
// could put in Size from a TurnDone alone, and synthesizing one (zero, or a
// remembered value from an unrelated earlier ContextMeasured) would be
// exactly the guess "drop, don't guess" forbids. Only
// ContextMeasured/ContextPressure — whose Measurement already carries the
// window limit and the current occupancy together, from the same
// authoritative source — ever produce a usage_update; TurnDone.Usage never
// does.
func translateTurnUsage(_ content.Usage) (protocol.SessionUpdate, bool) {
	return protocol.SessionUpdate{}, false
}

// updateMeta is the wire shape stamped into every session/update
// notification's _meta object. eventId identifies the exact Harness event
// (Header.EventID) that produced this update; promptId is the session/
// prompt command id this update belongs to, letting a client correlate a
// live update against the in-flight prompt it is currently awaiting a
// terminal response for. Replay adds isReplay in Phase 3.
type updateMeta struct {
	EventID  string `json:"eventId"`
	PromptID string `json:"promptId"`
}

// meta builds the _meta payload for one update. json.Marshal cannot fail
// here: updateMeta is exactly two plain strings (UUID.String() always
// produces the fixed-width canonical hex-and-hyphen form), so there is no
// cyclic reference, channel, or function value it could ever choke on.
func (t *liveTranslator) meta(eventID uuid.UUID) json.RawMessage {
	raw, _ := json.Marshal(updateMeta{EventID: eventID.String(), PromptID: t.promptID.String()})
	return raw
}
