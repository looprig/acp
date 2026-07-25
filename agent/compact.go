// compact.go implements the `/compact` internal slash command and its
// available_commands_update advertisement: Task 4.2 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// `/compact` is the one and only internal Harness command this facade ever
// exposes through session/prompt (see the design doc's "Slash commands and
// compaction": "No other internal Harness command is automatically
// exposed."). It exists only when Options.Compactor is configured
// (host.go); handlePrompt (prompt.go) checks isCompactSlashCommand on every
// incoming prompt and, only when both conditions hold, routes to
// handleCompactPrompt below instead of the ordinary Submit path — any other
// slash-prefixed (or plain) text, or "/compact" itself when no Compactor is
// configured, falls through unchanged to blocksFromPrompt/Submit exactly as
// before this task.
//
// Correlating a compaction command's outcome differs from Task 2.4's turn
// correlation (prompt.go's drainToTerminal). A submitted turn only learns
// its LoopID/TurnID from an intervening TurnStarted event, so drainToTerminal
// needs two phases: correlate the loop/turn first, then match every
// subsequent event against it. A compaction attempt's own terminal events —
// CompactWaiterResolved and CompactWaiterRejected
// (harness/pkg/event/compaction.go) — already carry Header.Cause.CommandID
// directly (they are Reply events: see event.Reply and
// event.CompactWaiterReplyID), so a single phase suffices:
// drainCompactionToTerminal matches the submitted command id straight off
// each event it observes, with no intermediate "started" event to wait for.
//
// available_commands_update is advertised lazily: the first time
// handlePrompt runs for a session (see ensureAvailableCommandsAdvertised),
// not eagerly at session/new/session/load/session/resume. This is a
// deliberate choice, not an oversight: session/resume's own documented
// contract is to send zero session/update notifications at all (see
// resume.go's package doc, an already-tested invariant), so hooking
// advertisement into session establishment would either have to special-case
// resume out again or silently break that invariant. Tying it to the first
// prompt instead reaches every session-establishment path uniformly, without
// touching session.go/resume.go/replay.go at all.
//
// Options.Compactor is a connection-level field, set once before any session
// exists, so it answers exactly one question: whether compaction is
// available at all, for the sole purpose of deciding whether to advertise
// and route `/compact` (above, and ensureAvailableCommandsAdvertised below).
// It is NEVER invoked to actually perform a compaction — a single
// connection-wide field cannot tell two different sessions' compactions
// apart, so the actual Compactor for a given `/compact` call is always
// resolved from that SPECIFIC session's live value instead
// (live.(Compactor) in handleCompactPrompt), the same per-session
// type-assertion pattern SessionCloser already uses (host.go's Compactor
// doc; close.go's live.(SessionCloser)).
package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
)

// compactCommandName is the exact literal prompt text that routes to
// compaction. Anything else — a different command, extra whitespace,
// additional content blocks, or plain prose that merely mentions
// "/compact" — is ordinary prompt content (see isCompactSlashCommand).
const compactCommandName = "/compact"

// compactAvailableCommand is the sole protocol.AvailableCommand this facade
// ever advertises. Name follows the pinned schema's own examples
// ("create_plan", "research_codebase": bare identifiers, no leading slash —
// a client is expected to prefix "/" itself when rendering a command
// palette); Input is left nil since `/compact` takes no arguments.
var compactAvailableCommand = protocol.AvailableCommand{
	Name:        "compact",
	Description: "Compact the conversation history to free up context space.",
}

// ErrCompactSubscriptionClosed is the local cause used when the event
// subscription backing a `/compact` correlation closes before the
// compaction's outcome (CompactWaiterResolved/CompactWaiterRejected) is
// observed. Mirrors prompt.go's ErrSubscriptionClosed for the turn-terminal
// case: subscription loss becomes a typed failure, never a silent success.
// It never crosses the wire itself (only Message/Code/Data do — see
// protocol.Fault); it exists so local callers can errors.Is/As it.
var ErrCompactSubscriptionClosed = errors.New("agent: event subscription closed before compaction outcome")

// ErrCompactorNotImplemented is the local cause used when a session's
// LiveSession value does not implement Compactor, even though
// Options.Compactor is configured at the connection level. Options.Compactor
// only gates whether `/compact` is advertised/routed at all (see host.go's
// Compactor doc and Options.Compactor's own doc, agent.go) — it is never the
// thing actually invoked, so it cannot substitute for a session whose own
// live value does not support compaction. A correctly implemented
// SessionHost should never produce this (every live session it hands back
// should implement Compactor exactly when compaction is generally
// available), but handleCompactPrompt still fails closed here rather than
// panicking or silently no-op-succeeding if it ever does. Never crosses the
// wire itself; exists so local callers can errors.Is/As it.
var ErrCompactorNotImplemented = errors.New("agent: session does not implement Compactor")

// isCompactSlashCommand reports whether blocks is exactly the literal
// "/compact" command: a single text content block whose entire text equals
// compactCommandName. Any other shape — zero or multiple blocks, a non-text
// block, leading/trailing whitespace, or different text entirely — is not
// the compact command and must fall through to ordinary prompt handling.
func isCompactSlashCommand(blocks []protocol.ContentBlock) bool {
	if len(blocks) != 1 {
		return false
	}
	b := blocks[0]
	return b.Text != nil && b.Text.Text == compactCommandName
}

// handleCompactPrompt runs the `/compact` slash command for one
// session/prompt call: it resolves Compactor from the SPECIFIC session's
// live value (live.(Compactor) — never a.opts.Compactor, which is only a
// connection-level advertisement signal; see host.go's Compactor doc and
// close.go's live.(SessionCloser) for the identical pattern), subscribes to
// live's Enduring event stream (compaction events are all Enduring and
// loop-scoped — see harness/pkg/event/compaction.go — and the target loop is
// not known ahead of time, so every loop is subscribed, exactly like
// prompt.go's own subscribe-before-submit rule guards against a race between
// subscribing and triggering the command), triggers Compactor.Compact, and
// drains until the correlated outcome. The caller (handlePrompt) has already
// confirmed Options.Compactor is configured and req.Prompt is exactly
// isCompactSlashCommand; neither of those facts says anything about whether
// THIS session's live value itself supports compaction, which is why the
// type-assertion below is still required and can still fail closed.
func (a *Agent) handleCompactPrompt(ctx context.Context, live LiveSession) (protocol.PromptResponse, error) {
	compactor, ok := live.(Compactor)
	if !ok {
		return protocol.PromptResponse{}, protocol.InternalError("session/prompt: compact: "+ErrCompactorNotImplemented.Error(), ErrCompactorNotImplemented)
	}

	sub, err := live.SubscribeEvents(event.EventFilter{Enduring: event.LoopScope{All: true}})
	if err != nil {
		return protocol.PromptResponse{}, protocol.InternalError("session/prompt: compact: subscribe: "+err.Error(), err)
	}
	defer sub.Close()

	cmdID, err := compactor.Compact(ctx)
	if err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return protocol.PromptResponse{}, f
		}
		return protocol.PromptResponse{}, protocol.InternalError("session/prompt: compact: "+err.Error(), err)
	}

	return drainCompactionToTerminal(ctx, sub, cmdID)
}

// drainCompactionToTerminal reads sub's event stream until it observes the
// compaction outcome caused by cmdID: event.CompactWaiterResolved (success)
// or event.CompactWaiterRejected (rejection, sanitized via
// sanitizedCompactRejection). See this file's package doc for how this
// differs from prompt.go's drainToTerminal. Any other event — decoy activity
// from an unrelated command, turn, or loop — is ignored exactly like
// drainToTerminal ignores its own decoys.
func drainCompactionToTerminal(ctx context.Context, sub event.Subscription, cmdID uuid.UUID) (protocol.PromptResponse, error) {
	for {
		select {
		case <-ctx.Done():
			return protocol.PromptResponse{}, protocol.InternalError("session/prompt: context ended before compaction completed", ctx.Err())

		case delivery, ok := <-sub.Events():
			if !ok {
				cause := sub.Err()
				if cause == nil {
					cause = ErrCompactSubscriptionClosed
				}
				return protocol.PromptResponse{}, protocol.InternalError("session/prompt: event subscription ended before compaction completed", cause)
			}

			switch e := delivery.Event.(type) {
			case event.CompactWaiterResolved:
				if e.Visibility() != event.Public || e.Header.Cause.CommandID != cmdID {
					continue // decoy: internal, or a different compaction attempt
				}
				return protocol.PromptResponse{StopReason: protocol.StopReasonEndTurn}, nil
			case event.CompactWaiterRejected:
				if e.Visibility() != event.Public || e.Header.Cause.CommandID != cmdID {
					continue // decoy: internal, or a different compaction attempt
				}
				return protocol.PromptResponse{}, sanitizedCompactRejection(e.Reason)
			default:
				continue // progress/unrelated event: not our compaction's terminal
			}
		}
	}
}

// compactRejectMessages is the fixed, human-readable allowlist
// sanitizedCompactRejection maps every valid event.CompactRejectReason
// through. Every entry here is a static string this package owns; none of
// them are ever derived from Harness's own formatting of the reason value.
var compactRejectMessages = map[event.CompactRejectReason]string{
	event.CompactRejectControlLaneFull:     "the control lane was full",
	event.CompactRejectShuttingDown:        "the session was shutting down",
	event.CompactRejectInterrupted:         "the operation was interrupted",
	event.CompactRejectCanceled:            "the request was canceled",
	event.CompactRejectStaleBasis:          "the compaction basis was stale",
	event.CompactRejectProgressPublication: "compaction progress could not be published",
	event.CompactRejectUnavailable:         "compaction is currently unavailable",
	event.CompactRejectExecutionFailed:     "compaction execution failed",
	event.CompactRejectInvalidSummary:      "the compaction summary was invalid",
	event.CompactRejectContextCountFailed:  "context token counting failed",
	event.CompactRejectSummaryTooLarge:     "the compaction summary was too large",
	event.CompactRejectInternal:            "an internal error occurred",
	event.CompactRejectContextLimitUnknown: "the context limit is unknown",
}

// sanitizedCompactRejection maps a CompactWaiterRejected.Reason to the
// *protocol.Fault returned for a rejected `/compact`. This mirrors
// prompt.go's sanitizedPromptFailure's discipline even though the source
// shape differs: CompactRejectReason (harness/pkg/event/compaction.go) is
// itself a closed, already-typed enum rather than an arbitrary error, so
// there is no raw internal error text to leak here the way TurnPanicError.Detail
// can — but this function still never lets Go's default formatting of the
// enum reach the wire (that would print an unlabelled integer, or invite a
// future Harness-side rename to silently change the wire text): every valid
// reason is mapped through an explicit, fixed allowlist of messages
// (compactRejectMessages), and anything outside that allowlist — including
// the unspecified zero value, which a validated event should never carry —
// falls back to one fixed generic message. Even in that fallback branch, the
// real reason value is still attached as the returned Fault's local
// (unexported, never-serialized) diagnostic cause — exactly like every other
// sanitized-error path in this phase (sanitizedPromptFailure in prompt.go,
// and this same function's known-allowlist branch above) — so an actual
// unmapped-reason occurrence is debuggable locally via errors.Unwrap, even
// though the wire-visible Message never varies with it.
func sanitizedCompactRejection(reason event.CompactRejectReason) *protocol.Fault {
	msg, ok := compactRejectMessages[reason]
	if !ok {
		cause := fmt.Errorf("agent: unrecognized compact reject reason: %d", reason)
		return protocol.InternalError("session/prompt: compact: the compaction was rejected", cause)
	}
	return protocol.InternalError("session/prompt: compact: "+msg, nil)
}

// ensureAvailableCommandsAdvertised sends the available_commands_update
// notification advertising `/compact`, exactly once per ACP session, at the
// first opportunity handlePrompt has to do so (the first session/prompt call
// this facade ever handles for that session — see this file's package doc
// for why advertisement is lazy rather than tied to session establishment).
// It is a no-op when Options.Compactor is nil (nothing to advertise) or when
// this session has already been advertised to.
func (a *Agent) ensureAvailableCommandsAdvertised(ctx context.Context, wireSessionID protocol.SessionID, sessionID SessionID) error {
	if a.opts.Compactor == nil {
		return nil
	}

	a.commandsMu.Lock()
	_, already := a.commandsAdvertised[sessionID]
	a.commandsMu.Unlock()
	if already {
		return nil
	}

	notification := protocol.SessionNotification{
		SessionID: wireSessionID,
		Update: protocol.SessionUpdate{
			AvailableCommandsUpdate: &protocol.AvailableCommandsUpdate{
				AvailableCommands: []protocol.AvailableCommand{compactAvailableCommand},
			},
		},
	}
	if err := a.client.SessionUpdate(ctx, notification); err != nil {
		return protocol.InternalError("session/prompt: available_commands_update: "+err.Error(), err)
	}

	a.commandsMu.Lock()
	a.commandsAdvertised[sessionID] = struct{}{}
	a.commandsMu.Unlock()
	return nil
}
