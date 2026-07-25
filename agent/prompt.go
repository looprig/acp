// prompt.go implements the session/prompt correlation engine and the
// session/cancel handler: Task 2.4 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// ACP's session/prompt is a request whose response completes only at a turn
// terminal, but Harness's Submit is fire-and-forget: it returns a command id
// and the turn's outcome arrives later, asynchronously, on the session's
// event stream. Bridging the two requires the two-phase correlation rule
// from the design doc ("Prompt correlation and event translation"):
//
//  1. Subscribe before submitting (see handlePrompt: SubscribeEvents is
//     called, and its error path returned, strictly before Submit is ever
//     called — a TurnStarted racing in immediately after Submit returns can
//     therefore never be missed).
//  2. Match TurnStarted.Header.Cause.CommandID to the submitted command id.
//  3. Capture that event's LoopID and TurnID (Header.Coordinates).
//  4. Match every following event using both captured identifiers, ignoring
//     everything else (interleaved activity from other loops, other turns,
//     or other prompts' TurnStarted events) as a decoy.
//  5. Complete the ACP response only on the correlated TurnDone, TurnFailed,
//     or TurnInterrupted.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
)

// ErrPromptAlreadyInFlight is the local cause behind the *protocol.Fault
// returned when a second session/prompt is attempted on a session that
// already has one in flight. Per the design doc: "At most one prompt per ACP
// session is in flight at a time: a second concurrent session/prompt on the
// same session is rejected... never queued behind or interleaved with the
// running one." It never crosses the wire itself (only Message/Code/Data
// do — see protocol.Fault); it exists so local callers can errors.Is/As it.
var ErrPromptAlreadyInFlight = errors.New("agent: a session/prompt is already in flight for this session")

// ErrSubscriptionClosed is the local cause used when the event subscription
// backing a session/prompt's correlation closes before the correlated
// turn's terminal is observed. Per the design doc: "Subscription loss before
// a terminal becomes a typed prompt failure rather than a successful empty
// answer" — this is never reported as if the prompt quietly produced no
// content.
var ErrSubscriptionClosed = errors.New("agent: event subscription closed before turn terminal")

// ErrSessionClosing is the local cause behind the *protocol.Fault returned
// when a session/prompt is attempted on a session that close.go's
// handleSessionClose has already marked closing (see promptTracker.markClosing).
// It is a distinct sentinel from ErrPromptAlreadyInFlight — both reject the
// same way (InvalidRequest), but a caller inspecting the cause via errors.Is
// can tell "busy" apart from "gone" — it never crosses the wire itself (only
// Message/Code/Data do — see protocol.Fault).
var ErrSessionClosing = errors.New("agent: session is closing")

// promptTracker enforces "at most one session/prompt in flight per ACP
// session" and, since Task 2.7, "no session/prompt may begin once
// session/close has started for its session". It is intentionally its own
// small type rather than a field folded into sessionRegistry: which sessions
// exist and which sessions currently have a prompt in flight (or are
// closing) are different questions with different lifetimes (a session can
// outlive many sequential prompts), and conflating them would force every
// registry reader to reason about prompt state too.
type promptTracker struct {
	mu sync.Mutex
	// inFlight maps a session with a prompt currently running to a channel
	// that end closes when that prompt concludes: close.go's orchestration
	// reads this channel (via markClosing) to wait for a real drain to
	// finish, rather than firing the interrupt and racing ahead of it.
	inFlight map[SessionID]chan struct{}
	// closing records every session for which markClosing has ever been
	// called. Entries are deliberately never removed: a session only ever
	// closes once (handleSessionClose ends by removing it from the
	// registry entirely — see close.go), so there is no scenario where a
	// session should un-close and accept prompts again.
	closing map[SessionID]struct{}
}

func newPromptTracker() *promptTracker {
	return &promptTracker{
		inFlight: make(map[SessionID]chan struct{}),
		closing:  make(map[SessionID]struct{}),
	}
}

// promptBeginOutcome classifies the result of promptTracker.begin.
type promptBeginOutcome int

const (
	// promptBeginOK: no prompt was in flight and the session was not
	// closing; the caller now owns the in-flight slot and must call end
	// exactly once (typically via defer) regardless of how the prompt
	// concludes.
	promptBeginOK promptBeginOutcome = iota
	// promptBeginAlreadyInFlight: another prompt is already running for
	// this session; the caller must not touch the live session at all.
	promptBeginAlreadyInFlight
	// promptBeginClosing: session/close has marked this session closing
	// (see markClosing); the caller must not touch the live session at
	// all, regardless of whether a prompt happens to be in flight too.
	promptBeginClosing
)

// begin reports whether a new session/prompt may start for id, checking
// closing before in-flight so a session mid-close is always reported as
// closing even if (as is normal mid-teardown) a prompt also happens to be in
// flight for it. This is the sole enforcement point: a caller must not touch
// the live session (subscribe/submit) unless begin reports promptBeginOK,
// and must call end exactly once afterward (typically via defer) regardless
// of how the prompt concludes.
func (t *promptTracker) begin(id SessionID) promptBeginOutcome {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.closing[id]; ok {
		return promptBeginClosing
	}
	if _, ok := t.inFlight[id]; ok {
		return promptBeginAlreadyInFlight
	}
	t.inFlight[id] = make(chan struct{})
	return promptBeginOK
}

// end releases id, allowing a subsequent session/prompt on the same session
// to proceed (unless the session has since been marked closing), and closes
// the channel a concurrent markClosing call may be waiting on. It is a
// no-op if id is not currently marked in flight.
func (t *promptTracker) end(id SessionID) {
	t.mu.Lock()
	ch, ok := t.inFlight[id]
	delete(t.inFlight, id)
	t.mu.Unlock()
	if ok {
		close(ch)
	}
}

// markClosing marks id as closing: every future begin call for id reports
// promptBeginClosing, permanently (see the closing field's doc). It returns
// the channel end will close when the currently in-flight prompt for id (if
// any) concludes, and whether one was in flight at the moment closing was
// recorded. Both the "mark closing" and "read in-flight state" steps happen
// under the same lock, so a concurrent begin can never race past this: it
// either observes closing already set (and reports promptBeginClosing) or
// has already recorded itself in inFlight before this runs (and is therefore
// exactly what wasInFlight/done reports).
func (t *promptTracker) markClosing(id SessionID) (done <-chan struct{}, wasInFlight bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closing[id] = struct{}{}
	ch, ok := t.inFlight[id]
	return ch, ok
}

// forget drops every trace of id from both inFlight and closing. Callers
// must only call this once id has already been removed from the live
// session registry (see close.go's handleSessionClose, the only caller):
// from that point on, resolveSession fails every session-scoped call for id
// with ResourceNotFound before it can ever reach promptTracker again, so the
// closing bookkeeping this file relies on to reject a late in-flight prompt
// attempt is no longer needed. Without this, closing would grow by one
// entry for every session ever closed over a long-running process's
// lifetime — session ids are never reused (see SessionID's doc), so nothing
// would ever naturally shrink it otherwise.
func (t *promptTracker) forget(id SessionID) {
	t.mu.Lock()
	delete(t.closing, id)
	delete(t.inFlight, id)
	t.mu.Unlock()
}

// cancelInterruptTimeout bounds how long handleSessionCancel waits for the
// live session's Interrupt call. session/cancel is a notification (see
// protocol.AgentConn.Cancel and conn.go's NotifyFunc: "notifications never
// receive a response"), so nothing downstream reads this deadline directly —
// it exists so a wedged host implementation cannot block the shared
// notification-dispatch worker forever (conn.go's notifyWorker serializes
// notification handler completion order across the whole Conn).
const cancelInterruptTimeout = 30 * time.Second

// handlePrompt answers the session/prompt method: the two-phase correlation
// engine documented at the top of this file. It resolves the session,
// enforces per-session serialization (promptTracker), converts the request's
// content blocks, subscribes, submits, and drains the event stream to the
// correlated terminal.
//
// When Options.Compactor is configured, it also lazily advertises
// `/compact` (ensureAvailableCommandsAdvertised) and, when the request's
// prompt content is exactly that literal command (isCompactSlashCommand),
// routes to handleCompactPrompt instead of the ordinary
// blocksFromPrompt/Submit path below — see compact.go's package doc for the
// full contract. Any other content, including "/compact" itself when no
// Compactor is configured, falls through unchanged.
func (a *Agent) handlePrompt(ctx context.Context, _ string, params json.RawMessage) (any, error) {
	var req protocol.PromptRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("session/prompt: decode params", err)
	}

	live, err := a.resolveSession(req.SessionID)
	if err != nil {
		return nil, err
	}
	sessionID := live.SessionID()

	switch a.prompts.begin(sessionID) {
	case promptBeginOK:
	case promptBeginClosing:
		return nil, protocol.InvalidRequest("session/prompt: session is closing", ErrSessionClosing)
	default: // promptBeginAlreadyInFlight
		return nil, protocol.InvalidRequest("session/prompt: a prompt is already in flight for this session", ErrPromptAlreadyInFlight)
	}
	defer a.prompts.end(sessionID)

	if a.opts.Compactor != nil {
		if err := a.ensureAvailableCommandsAdvertised(ctx, req.SessionID, sessionID); err != nil {
			return nil, err
		}
		if isCompactSlashCommand(req.Prompt) {
			return a.handleCompactPrompt(ctx, live)
		}
	}

	blocks, err := blocksFromPrompt(req.Prompt)
	if err != nil {
		return nil, protocol.InvalidParams("session/prompt: "+err.Error(), err)
	}

	// Phase 1: subscribe BEFORE submitting. The correlated loop is not known
	// yet (it is only learned from TurnStarted, below), so this asks for
	// every loop's Enduring events — TurnStarted/TurnDone/TurnFailed/
	// TurnInterrupted are all Enduring by construction — for correlation, AND
	// every loop's Ephemeral firehose (TokenDelta, tool progress) so the live
	// translator (translate.go) has something to forward once correlation
	// lands. Subscribing to Ephemeral from every loop — not just the eventual
	// correlated one, which is not known yet — means other loops'/prompts'
	// Ephemeral traffic is observed too until correlation happens; drainToTerminal
	// discards it exactly like it already discards Enduring decoys, via the
	// same post-correlation LoopID/TurnID check.
	sub, err := live.SubscribeEvents(event.EventFilter{
		Ephemeral: event.LoopScope{All: true},
		Enduring:  event.LoopScope{All: true},
	})
	if err != nil {
		return nil, protocol.InternalError("session/prompt: subscribe: "+err.Error(), err)
	}
	defer sub.Close()

	// Phase 2: submit. Only now does the loop that will produce our turn's
	// events get a chance to run — the subscription above is already live.
	cmdID, err := live.Submit(ctx, blocks)
	if err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("session/prompt: submit: "+err.Error(), err)
	}

	return drainToTerminal(ctx, sub, cmdID, req.SessionID, live, a.client, a.gates)
}

// liveUpdateSender is the narrow capability drainToTerminal needs to forward
// a translated live update to the ACP client: exactly protocol.ClientConn's
// SessionUpdate method, named here so this file depends on the capability it
// uses rather than the concrete type (a fake in tests never needs the rest
// of ClientConn's surface — RequestPermission, file I/O, terminals — to
// exercise this path).
type liveUpdateSender interface {
	SessionUpdate(ctx context.Context, n protocol.SessionNotification) error
}

// drainToTerminal reads sub's event stream until it observes the terminal
// (TurnDone/TurnFailed/TurnInterrupted) of the turn caused by cmdID,
// ignoring everything else: events before correlation is established
// (anything other than the matching TurnStarted), and — once correlation is
// established — any event from a different loop or turn. It never returns a
// successful response without having observed that exact terminal.
//
// Every correlated event it observes along the way — not just the terminal
// — is also offered to a liveTranslator (translate.go): a translatable,
// Public event becomes a session/update notification sent through client
// before drainToTerminal continues (or, for a terminal event, before it
// returns), so the client actually sees live progress rather than only the
// eventual PromptResponse. A send failure aborts the drain with a typed
// error: client is a real network write, and a wedged or gone connection
// means continuing to drain silently would misrepresent what the client
// actually received.
//
// A correlated event.GateOpened for a translatable permission gate (gates.go,
// Task 2.6) runs its full session/request_permission round trip inline,
// right here mid-drain, before drainToTerminal continues: the turn is
// already parked on that gate regardless, so blocking this loop on the round
// trip loses nothing, and it keeps the gate's resolution strictly ordered
// with respect to every other event this turn produces. gates is the
// tracking structure Task 2.7's session/close orchestration hooks into to
// force any gate still open when a session closes (see gateTracker).
func drainToTerminal(ctx context.Context, sub event.Subscription, cmdID uuid.UUID, wireSessionID protocol.SessionID, live LiveSession, client liveClient, gates *gateTracker) (protocol.PromptResponse, error) {
	var loopID, turnID uuid.UUID
	correlated := false
	var translator *liveTranslator

	for {
		select {
		case <-ctx.Done():
			return protocol.PromptResponse{}, protocol.InternalError("session/prompt: context ended before turn completed", ctx.Err())

		case delivery, ok := <-sub.Events():
			if !ok {
				cause := sub.Err()
				if cause == nil {
					cause = ErrSubscriptionClosed
				}
				return protocol.PromptResponse{}, protocol.InternalError("session/prompt: event subscription ended before turn completed", cause)
			}

			ev := delivery.Event
			if !correlated {
				ts, ok := ev.(event.TurnStarted)
				if !ok || ts.Header.Cause.CommandID != cmdID {
					continue // decoy: not our TurnStarted (or not a TurnStarted at all)
				}
				loopID = ts.Header.LoopID
				turnID = ts.Header.TurnID
				correlated = true
				translator = newLiveTranslator(wireSessionID, loopID, turnID, cmdID)
				continue
			}

			hdr := ev.EventHeader()
			if hdr.LoopID != loopID || hdr.TurnID != turnID {
				continue // decoy: interleaved activity from another loop/turn
			}

			if n, ok := translator.Translate(ev); ok {
				if err := client.SessionUpdate(ctx, n); err != nil {
					return protocol.PromptResponse{}, protocol.InternalError("session/prompt: session/update: "+err.Error(), err)
				}
			}

			switch e := ev.(type) {
			case event.TurnDone:
				return protocol.PromptResponse{StopReason: protocol.StopReasonEndTurn}, nil
			case event.TurnInterrupted:
				// Cancellation-as-success: session/cancel having called
				// Interrupt is reported here, never as a transport or
				// internal error.
				return protocol.PromptResponse{StopReason: protocol.StopReasonCancelled}, nil
			case event.TurnFailed:
				return protocol.PromptResponse{}, sanitizedPromptFailure(e.Err)
			case event.GateOpened:
				// Defense in depth, matching translate.go's own rule for
				// every other event kind: never act on a non-public gate
				// envelope, even though the hub's own delivery filter
				// (event.ShouldDeliver) should already have kept an Internal
				// one from ever reaching this subscription.
				if e.Visibility() != event.Public {
					continue
				}
				opts, ok := permissionOptionsFromGate(e.Gate)
				if !ok {
					// Not translatable (ask-user, a host-owned form/open-url
					// gate, or anything else) — see gates.go's package doc.
					// Falls through exactly like any other untranslatable
					// progress event: the drain continues, the gate stays
					// open until something else resolves it.
					continue
				}
				gctx, release := gates.begin(ctx, live.SessionID(), e.Gate.ID)
				fault := runPermissionGateRoundTrip(gctx, client, live, wireSessionID, e.Gate, opts)
				release()
				if fault != nil {
					return protocol.PromptResponse{}, fault
				}
			default:
				continue // progress event (StepDone, TokenDelta, ...): not a terminal
			}
		}
	}
}

// sanitizedPromptFailure maps a TurnFailed.Err cause to the *protocol.Fault
// returned for session/prompt. This is the typed-cause table the design doc
// requires ("sanitized ACP prompt error unless a typed cause maps exactly"):
// exactly the causes Harness documents as secret-free (EmptyResponseError,
// ToolLimitError — see harness/pkg/event/errors.go) are surfaced verbatim,
// because their own doc comments guarantee they carry no raw provider text or
// secrets. Every other cause — including TurnPanicError, whose Detail is an
// arbitrary recovered panic value with no such guarantee, and any raw
// provider/network error — is sanitized to a fixed, generic message. The raw
// cause is still attached as the Fault's local (unexported, never-serialized)
// diagnostic cause via errors.As/Unwrap: it is available for local logs, but
// protocol.ToWireError never sends it, and neither does this function's
// chosen Message text.
func sanitizedPromptFailure(cause error) *protocol.Fault {
	var empty *event.EmptyResponseError
	if errors.As(cause, &empty) {
		return protocol.InternalError("session/prompt: "+empty.Error(), cause)
	}
	var limit *event.ToolLimitError
	if errors.As(cause, &limit) {
		return protocol.InternalError("session/prompt: "+limit.Error(), cause)
	}
	// TurnPanicError.Detail is an arbitrary recovered panic value: unlike
	// EmptyResponseError/ToolLimitError, Harness makes no secret-free
	// guarantee about it, so it is deliberately NOT included in the message.
	var panicErr *event.TurnPanicError
	if errors.As(cause, &panicErr) {
		return protocol.InternalError("session/prompt: the turn ended due to an internal error", cause)
	}
	// Unknown/untyped cause (raw provider or network error, etc.): sanitize
	// to a fixed message. cause.Error() must never be interpolated here.
	return protocol.InternalError("session/prompt: the turn failed", cause)
}

// blocksFromPrompt converts an ACP PromptRequest's content blocks into the
// Harness content.Block sequence Submit needs. Only text blocks are
// translated today: image, audio, and embedded-resource blocks require
// prompt capabilities this facade does not yet advertise, and resource links
// have no corresponding content.Block variant to translate into without
// either fabricating one or lossily reinterpreting the link as plain text —
// either of which would misrepresent what the caller attached rather than
// fail closed. Rejecting them here (rather than silently dropping or
// reinterpreting) keeps the boundary honest; a later task may extend this
// alongside its own dedicated tests once the full inbound mapping is
// designed.
func blocksFromPrompt(blocks []protocol.ContentBlock) ([]content.Block, error) {
	out := make([]content.Block, 0, len(blocks))
	for i, b := range blocks {
		if b.Text == nil {
			return nil, unsupportedContentBlockError(i)
		}
		out = append(out, &content.TextBlock{Text: b.Text.Text})
	}
	return out, nil
}

func unsupportedContentBlockError(index int) error {
	return &UnsupportedContentBlockError{Index: index}
}

// UnsupportedContentBlockError reports that a session/prompt request
// contained a content block variant blocksFromPrompt cannot yet translate.
type UnsupportedContentBlockError struct {
	// Index is the position of the offending block within PromptRequest.Prompt.
	Index int
}

func (e *UnsupportedContentBlockError) Error() string {
	return "content block " + strconv.Itoa(e.Index) + ": unsupported block type (only text is currently supported)"
}

// handleSessionCancel answers the session/cancel notification. Per the
// design doc's "Cancellation and close": "session/cancel calls the live
// session's interrupt capability (session.Interrupt). The prompt handler
// continues draining the correlated turn until it observes its terminal,
// then returns stopReason: cancelled." This handler therefore does nothing
// beyond triggering the interrupt: it never short-circuits an in-flight
// handlePrompt call, and it never touches promptTracker — Interrupt is safe
// to call even when no prompt is in flight (a harmless, fail-quiet no-op per
// session.Session.Interrupt's own contract), so there is nothing to
// coordinate here beyond resolving the session.
//
// session/cancel is a notification (see protocol's NotifyFunc): it has no
// response channel, so a malformed request, an unknown session id, or an
// Interrupt failure all have nowhere to report to and are deliberately
// dropped after resolution fails closed (no live session -> nothing is
// interrupted, which is the safe outcome for an id that never named a
// session in the first place).
func (a *Agent) handleSessionCancel(ctx context.Context, _ string, params json.RawMessage) {
	var n protocol.CancelNotification
	if err := json.Unmarshal(params, &n); err != nil {
		return
	}
	live, err := a.resolveSession(n.SessionID)
	if err != nil {
		return
	}

	ictx, cancel := context.WithTimeout(ctx, cancelInterruptTimeout)
	defer cancel()
	_, _ = live.Interrupt(ictx)
}
