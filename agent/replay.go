// replay.go implements the replay translator for session/load: Task 3.1 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// Live ephemeral token and tool-progress events are not durable (see
// translate.go's package doc and event.go's Class doc: only Enduring events
// are ever journaled). session/load therefore cannot replay the live token
// stream at all — TokenDelta, ToolCallStarted, and ToolCallCompleted are all
// Ephemeral (see event.go's TokenDelta doc and event/tool.go's
// ToolCallStarted/ToolCallCompleted docs) and never appear in durable
// history. Instead this file reconstructs, from the session's Enduring
// event history alone:
//
//   - user messages, from TurnStarted.Message;
//   - assistant messages and completed tool calls, from StepDone.Messages
//     (the step's single *content.AIMessage followed by its
//     *content.ToolResultMessages — see event.go's StepDone doc); and
//   - the session's current context-window usage, from the LAST
//     ContextMeasured seen (reusing translate.go's translateContextMeasurement
//     directly).
//
// This grouped, four-bucket order — every user message, then every assistant
// message, then every completed tool call, then one final metadata update —
// is the exact shape the design doc's "Load replay versus live streaming"
// section and this task describe, not an interleaved chronological replay:
// it deliberately does not attempt to reproduce per-turn interleaving.
//
// TurnDone/TurnFailed/TurnInterrupted are turn-boundary markers only and
// never separately translated: TurnDone.Message is the concatenation of
// content already reconstructed from this turn's StepDone events, so
// translating it too would duplicate client-visible content (see the
// no-duplication property this task requires; translate.go's live translator
// applies the identical "drop, don't guess" rule to TurnDone for the same
// reason).
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/journal"
)

// replayToolCallID derives the ACP tool call id for a durably-replayed tool
// call from its ToolUseBlock's provider-issued ID (content.ToolUseBlock.ID).
// This is DELIBERATELY a different derivation from translate.go's toolCallID,
// which uses Harness's ToolExecutionID (a uuid minted only in memory by the
// tool runner — see internal/loopruntime/runner.go's idGen): the only events
// that ever carry a ToolExecutionID, ToolCallStarted and ToolCallCompleted,
// are Ephemeral and therefore never reach durable history at all. The
// provider's own ToolUseBlock.ID, by contrast, IS durable: it rides on the
// AIMessage Harness commits via StepDone (an Enduring event), and the
// matching ToolResultMessage.ToolUseID always names the same value, so it is
// replay's only stable, available call identity.
func replayToolCallID(providerID string) protocol.ToolCallID {
	return protocol.ToolCallID(providerID)
}

// replayUserMessageID mints the single message id shared by every text block
// of one replayed user message (a TurnStarted's Message). Unlike assistant
// content, a user message carries no thought-versus-text distinction, so
// there is nothing to sequence: one turn's user message is always exactly
// one ACP message.
func replayUserMessageID(sessionID protocol.SessionID, loopID, turnID uuid.UUID) protocol.MessageID {
	return protocol.MessageID("msg:" + string(sessionID) + ":" + loopID.String() + ":" + turnID.String() + ":user")
}

// replayMeta builds the _meta payload for one replayed session/update: the
// durable EventID that produced it, and isReplay:true (see updateMeta's doc
// in translate.go). It never carries a promptId: a replayed update has no
// correlated in-flight session/prompt call.
func replayMeta(eventID uuid.UUID) json.RawMessage {
	return marshalUpdateMeta(updateMeta{EventID: eventID.String(), IsReplay: true})
}

// toolResultContent converts a ToolResultMessage's display blocks into ACP
// ToolCallContent. Only text blocks are representable without guessing
// (matches prompt.go's blocksFromPrompt, which applies the same text-only
// restriction on the inbound side); any other block variant is dropped
// rather than lossily reinterpreted.
func toolResultContent(blocks []content.Block) []protocol.ToolCallContent {
	var out []protocol.ToolCallContent
	for _, b := range blocks {
		tb, ok := b.(*content.TextBlock)
		if !ok {
			continue
		}
		out = append(out, protocol.ToolCallContent{
			Content: &protocol.Content{Content: protocol.ContentBlock{Text: &protocol.TextContent{Text: tb.Text}}},
		})
	}
	return out
}

// replayState accumulates the ordered reconstruction buckets while
// buildReplayNotifications walks a session's durable event history. See this
// file's package doc for why the final output is grouped by kind rather than
// interleaved chronologically.
type replayState struct {
	sessionID protocol.SessionID

	userMessages      []protocol.SessionNotification
	assistantMessages []protocol.SessionNotification
	toolCalls         []protocol.SessionNotification

	lastMeasurement   *event.ContextMeasurement
	lastMeasurementID uuid.UUID

	// havePrimary/primaryLoop lock in the first loop-scoped event's LoopID as
	// the loop this reconstruction follows; every other loop's traffic (a
	// subagent, for instance) is treated as a decoy and skipped, mirroring
	// prompt.go's drainToTerminal decoy-skipping rule for a live turn. Task
	// 3.1 does not attempt multi-loop replay (see LoadedSession's doc: the
	// bound is scoped "for the session's replayed loop", singular).
	havePrimary bool
	primaryLoop uuid.UUID

	// currentTurnID is the TurnID of the most recently accepted TurnStarted
	// (one within the ReplayedThrough boundary); a StepDone whose TurnID does
	// not match is a decoy (out of scope, or a step from before any
	// TurnStarted has been seen) and is skipped.
	currentTurnID uuid.UUID
	// msgSeq sequences assistant message ids across the WHOLE current turn
	// (every step's AIMessage), reset fresh on each new TurnStarted — the
	// same per-turn scoping liveTranslator uses (see translate.go), so a
	// turn's message ids follow the identical algorithm whether observed live
	// or reconstructed here.
	msgSeq messageIDSeq
}

// appendUserMessage reconstructs one TurnStarted's user message. Only text
// blocks are translated (see toolResultContent's doc for the identical
// text-only rationale); every text block of the message shares one message
// id, since a user message carries no thought/text distinction to sequence.
func (st *replayState) appendUserMessage(hdr event.Header, msg *content.UserMessage) {
	if msg == nil {
		return
	}
	id := replayUserMessageID(st.sessionID, hdr.LoopID, hdr.TurnID)
	for _, b := range msg.Blocks {
		tb, ok := b.(*content.TextBlock)
		if !ok {
			continue
		}
		mid := id
		st.userMessages = append(st.userMessages, protocol.SessionNotification{
			SessionID: st.sessionID,
			Update: protocol.SessionUpdate{UserMessageChunk: &protocol.ContentChunk{
				Content:   protocol.ContentBlock{Text: &protocol.TextContent{Text: tb.Text}},
				MessageID: &mid,
			}},
			Meta: replayMeta(hdr.EventID),
		})
	}
}

// appendAssistantChunk records one fully-formed (never fragmented) text or
// thought chunk from the current turn's assistant content, using hdr's
// EventID (the StepDone that carried it) for _meta.eventId.
func (st *replayState) appendAssistantChunk(hdr event.Header, thought bool, text string) {
	id := formatMessageID(st.sessionID, hdr.LoopID, st.currentTurnID, st.msgSeq.next(thought))
	update := protocol.SessionUpdate{}
	chunk := &protocol.ContentChunk{
		Content:   protocol.ContentBlock{Text: &protocol.TextContent{Text: text}},
		MessageID: &id,
	}
	if thought {
		update.AgentThoughtChunk = chunk
	} else {
		update.AgentMessageChunk = chunk
	}
	st.assistantMessages = append(st.assistantMessages, protocol.SessionNotification{
		SessionID: st.sessionID,
		Update:    update,
		Meta:      replayMeta(hdr.EventID),
	})
}

// appendToolCall records one already-resolved tool call: replay only ever
// observes a tool call after StepDone commits it, so — unlike the live
// translator's two-phase ToolCallStarted/ToolCallCompleted — there is no
// in-progress state to reconstruct; a single ToolCall update already showing
// the terminal status is both correct and sufficient. RawInput is the
// provider's own ToolUseBlock.Input verbatim (already a json.RawMessage):
// unlike the live translator, which never sees raw arguments at all (see
// translate.go's translateToolCallStarted doc), durable history carries them,
// so replay can represent this field where live cannot.
func (st *replayState) appendToolCall(hdr event.Header, call *content.ToolUseBlock, result *content.ToolResultMessage) {
	status := protocol.ToolCallStatusCompleted
	if result.IsError {
		status = protocol.ToolCallStatusFailed
	}
	st.toolCalls = append(st.toolCalls, protocol.SessionNotification{
		SessionID: st.sessionID,
		Update: protocol.SessionUpdate{ToolCall: &protocol.ToolCall{
			ToolCallID: replayToolCallID(call.ID),
			Title:      call.Name,
			Status:     &status,
			RawInput:   call.Input,
			Content:    toolResultContent(result.Blocks),
		}},
		Meta: replayMeta(hdr.EventID),
	})
}

// appendStepDone walks one StepDone's Messages: content.AgenticMessages
// documented as "the step's single AIMessage followed by its
// ToolResultMessages" (event.go). Each AIMessage content block becomes
// either an assistant/thought chunk (TextBlock/ThinkingBlock) or a pending
// tool call awaiting its result (ToolUseBlock); each ToolResultMessage
// resolves the pending call with the matching ToolUseID and appends the
// completed tool_call update. A ToolUseBlock with no matching
// ToolResultMessage in this same StepDone is left unresolved and dropped:
// StepDone is only ever committed once the step's tool batch has fully run
// (see event.go's StepDone doc — "emitted at step completion"), so this
// should not occur for real durable history; it fails safe rather than
// fabricating an in-progress state replay can never observe.
func (st *replayState) appendStepDone(hdr event.Header, messages content.AgenticMessages) {
	pending := make(map[string]*content.ToolUseBlock)
	for _, m := range messages {
		switch mm := m.(type) {
		case *content.AIMessage:
			for _, b := range mm.Blocks {
				switch block := b.(type) {
				case *content.TextBlock:
					st.appendAssistantChunk(hdr, false, block.Text)
				case *content.ThinkingBlock:
					st.appendAssistantChunk(hdr, true, block.Thinking)
				case *content.ToolUseBlock:
					pending[block.ID] = block
				default:
					continue // no representable ACP update for this block kind
				}
			}
		case *content.ToolResultMessage:
			call, ok := pending[mm.ToolUseID]
			if !ok {
				continue // no matching call recorded in this step (see doc)
			}
			delete(pending, mm.ToolUseID)
			st.appendToolCall(hdr, call, mm)
		default:
			continue // UserMessage/SystemMessage never appear in StepDone.Messages
		}
	}
}

// buildReplayNotifications walks cur (a durable event replay cursor opened
// from the session's Beginning) and returns the exact ordered session/update
// sequence session/load must send: every user message, then every assistant
// message, then every completed tool call, then — if any ContextMeasured was
// observed — one final usage_update reflecting the LAST measurement seen
// (see this file's package doc).
//
// Two boundaries are enforced while walking:
//
//   - Visibility defense in depth: cur is documented as public-only by
//     construction (sessionstore.Store.OpenEventReplayer's publicOnly filter),
//     but every event's Visibility() is re-checked here anyway, exactly like
//     translate.go's live translator — a violating replayer must never leak
//     an Internal event onto the wire just because it claims to be filtered.
//   - ReplayedThrough: the first TurnStarted whose TurnIndex exceeds
//     replayedThrough stops the walk entirely (every following ledger record
//     is, by the loop's monotonic turn numbering, equally out of scope) —
//     the boundary LoadedSession documents as "the point up to which durable
//     history was reconstructed before the live controller took over."
//
// A non-EOF error from cur.Next fails the whole reconstruction rather than
// silently truncating it: a corrupt or unreadable durable record must be
// surfaced, not hidden behind a partial replay that looks complete.
func buildReplayNotifications(ctx context.Context, sessionID protocol.SessionID, cur journal.EventCursor, replayedThrough event.TurnIndex) ([]protocol.SessionNotification, error) {
	st := &replayState{sessionID: sessionID}

walk:
	for {
		ev, _, err := cur.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		if ev.Visibility() != event.Public {
			continue // defense in depth: never translate a non-public event (see doc)
		}

		hdr := ev.EventHeader()
		if ev.Scope() == event.ScopeLoop {
			switch {
			case !st.havePrimary:
				st.havePrimary = true
				st.primaryLoop = hdr.LoopID
			case hdr.LoopID != st.primaryLoop:
				continue // another loop's traffic: out of scope for Task 3.1 (see replayState doc)
			}
		}

		switch e := ev.(type) {
		case event.TurnStarted:
			if e.TurnIndex > replayedThrough {
				break walk // beyond the load boundary; everything after is too (monotonic turn index)
			}
			st.currentTurnID = hdr.TurnID
			st.msgSeq = messageIDSeq{}
			st.appendUserMessage(hdr, e.Message)
		case event.StepDone:
			if hdr.TurnID != st.currentTurnID {
				continue // decoy: not part of an in-scope turn
			}
			st.appendStepDone(hdr, e.Messages)
		case event.ContextMeasured:
			m := e.Measurement
			st.lastMeasurement = &m
			st.lastMeasurementID = hdr.EventID
		default:
			continue // not part of Task 3.1's reconstruction (see design doc's replay section)
		}
	}

	out := make([]protocol.SessionNotification, 0, len(st.userMessages)+len(st.assistantMessages)+len(st.toolCalls)+1)
	out = append(out, st.userMessages...)
	out = append(out, st.assistantMessages...)
	out = append(out, st.toolCalls...)
	if st.lastMeasurement != nil {
		update, _ := translateContextMeasurement(*st.lastMeasurement) // always ok=true; see translate.go
		out = append(out, protocol.SessionNotification{
			SessionID: sessionID,
			Update:    update,
			Meta:      replayMeta(st.lastMeasurementID),
		})
	}
	return out, nil
}

// shutdownOrphanedSession makes a best-effort attempt to release a host
// session this facade can no longer track — the SAME cleanup handleSessionNew
// applies when it loses a registry-capacity race (see session.go), reused
// here rather than duplicated: a live value that never implements
// SessionCloser is left as-is, and a Shutdown call that itself errors is not
// fatal (the caller's own error is what is reported, not Shutdown's).
func shutdownOrphanedSession(live LiveSession) {
	if closer, ok := live.(SessionCloser); ok {
		ctx, cancel := context.WithTimeout(context.Background(), closeShutdownGrace)
		_ = closer.Shutdown(ctx)
		cancel()
	}
}

// handleSessionLoad answers the session/load method. It is only ever
// registered when Options.Replayer is non-nil (see Register), matching
// capabilities.go's LoadSession advertisement gate; an unregistered call
// already fails closed via Conn's own method-not-found fallback, so this
// handler can assume the capability is present.
//
// Per the design doc's "Load replay versus live streaming" and this task:
// Host.LoadSession restores the session and returns a LoadedSession (its
// live controller plus the durable-history boundary); this handler then
// opens Replayer's event cursor, reconstructs and sends the ordered
// session/update sequence (buildReplayNotifications), and registers the live
// session only once that full reconstruction has actually reached the
// client — matching handleSessionNew's registration but, unlike it, ALSO
// matching handlePrompt's correlation contract of not answering until the
// underlying work (there, a turn's drain; here, a full replay) is completely
// done. Any failure from LoadSession onward makes the live session an orphan
// this facade can no longer track, so it gets the same best-effort Shutdown
// cleanup as a session/new registry-capacity race.
func (a *Agent) handleSessionLoad(ctx context.Context, _ string, params json.RawMessage) (any, error) {
	if err := a.AuthorizeSessionCreation(); err != nil {
		return nil, err
	}

	var req protocol.LoadSessionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("session/load: decode params", err)
	}

	sessionID, err := ParseSessionID(req.SessionID)
	if err != nil {
		return nil, protocol.InvalidParams("sessionId: "+err.Error(), err)
	}

	// Advisory pre-check, mirroring handleSessionNew's: avoid the cost of
	// restoring a session the registry cannot hold anyway. sessions.add's own
	// bounded check below is still the authoritative enforcement point.
	if a.sessions.atCapacity() {
		capErr := &TooManyLiveSessionsError{Max: MaxLiveSessions}
		return nil, protocol.InternalError("session/load: "+capErr.Error(), capErr)
	}

	caps := a.negotiatedClientCapabilities()
	// MCP composition has no Options capability yet (same restriction
	// handleSessionNew applies — see its own doc), so setup fails closed
	// rather than silently accepting or dropping requested MCP servers.
	setup, err := NewSetup(req.Cwd, &caps, req.McpServers, false)
	if err != nil {
		return nil, protocol.InvalidParams("session/load: invalid setup: "+err.Error(), err)
	}

	loaded, err := a.opts.Host.LoadSession(ctx, sessionID, setup)
	if err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("session/load: "+err.Error(), err)
	}

	wireSessionID := protocol.SessionID(sessionID.String())
	if err := a.replayHistory(ctx, wireSessionID, sessionID, loaded.ReplayedThrough); err != nil {
		shutdownOrphanedSession(loaded.Live)
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("session/load: "+err.Error(), err)
	}

	if err := a.sessions.add(loaded.Live); err != nil {
		shutdownOrphanedSession(loaded.Live)
		return nil, protocol.InternalError("session/load: "+err.Error(), err)
	}

	return protocol.LoadSessionResponse{}, nil
}

// replayHistory opens the durable event replayer for sessionID and sends the
// full reconstructed session/update sequence via a.client, in order,
// returning only once every notification has actually reached the client (or
// a send/read failure aborts it) — see handleSessionLoad's doc for why this
// blocking contract matters.
func (a *Agent) replayHistory(ctx context.Context, wireSessionID protocol.SessionID, sessionID SessionID, replayedThrough event.TurnIndex) error {
	replayer, err := a.opts.Replayer.OpenEventReplayer(sessionID)
	if err != nil {
		return protocol.InternalError("session/load: open replayer: "+err.Error(), err)
	}

	cur, err := replayer.Open(ctx, journal.ReplayRequest{SessionID: sessionID, From: journal.Beginning()})
	if err != nil {
		return protocol.InternalError("session/load: open replay cursor: "+err.Error(), err)
	}
	defer cur.Close()

	notifications, err := buildReplayNotifications(ctx, wireSessionID, cur, replayedThrough)
	if err != nil {
		return protocol.InternalError("session/load: replay: "+err.Error(), err)
	}

	for _, n := range notifications {
		if err := a.client.SessionUpdate(ctx, n); err != nil {
			return protocol.InternalError("session/load: session/update: "+err.Error(), err)
		}
	}
	return nil
}
