// gates.go implements the ACP permission-gate bridge: Task 2.6 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// prompt.go's drainToTerminal (Task 2.4) correlates a session/prompt to its
// exact turn and drains that turn's event stream to its terminal, forwarding
// every translatable Public event along the way as a session/update
// notification (Task 2.5). A turn can also PARK on an open gate mid-drain: a
// tool call awaiting human approval (gate.KindPermission) or an explicit
// user question (gate.KindAskUser) blocks the loop until it is answered.
// This file adds the handling drainToTerminal needs for that: when it
// observes a correlated, Public event.GateOpened for a translatable gate, it
// issues a session/request_permission call on the client connection, waits
// for the client's chosen option, validates that option was genuinely one
// that was offered, and answers the durable gate via LiveSession.RespondGate
// with the matching gate.GateResponse — all before drainToTerminal continues
// draining toward the turn's eventual terminal.
//
// # Only gate.KindPermission is translated
//
// The design doc ("Permissions and host-owned gates") describes the flow as
// applying to "Harness permission and ask-user gates" generically. Reading
// the real gate contract (harness/pkg/gate) shows the two kinds are not
// actually symmetric for this purpose:
//
//   - gate.KindPermission's answer is exactly one of the three
//     gate.ApprovalAction strings (Approve / Approve always for this
//     workspace / Deny — see gate.ApprovalControls, which documents these as
//     the gate's "exact, complete control set"). That is precisely what
//     ACP's session/request_permission was designed to carry: a fixed,
//     closed set of named options the user picks exactly one of, tagged with
//     a PermissionOptionKind hint whose four values (allow_once/
//     allow_always/reject_once/reject_always) already have obvious,
//     non-guessed counterparts for the three approval actions.
//
//   - gate.KindAskUser's answer is NOT an action pick: internal/loopruntime's
//     translateAskUserResponse (sessionruntime/gates.go) reads the answer
//     from response.Values["answer"] — arbitrary free text, or one value
//     from a bounded schema.Field's Options when the question offered fixed
//     choices (loopruntime's askUserFields). RequestPermissionResponse's
//     Outcome, however, carries only a discriminated Cancelled-or-Selected
//     choice with an OptionID — there is no field anywhere on the ACP
//     outcome for arbitrary answer text. Even in the bounded-choices case,
//     ACP's PermissionOptionKind is a closed, approve/deny-flavored enum
//     (see types_gen.go's UnmarshalJSON, which rejects anything else): there
//     is no non-fabricated kind that represents "the user picked one of N
//     arbitrary named choices" without guessing an approve/deny semantic
//     that the actual question never had (a "which color?" question tagged
//     allow_once/reject_once would misrepresent the choice to a client
//     rendering approval icons around it).
//
// This is the same category of gap already documented for elicitation in
// Task 1.7/1.8 (protocol/acp.go and internal/mockpeer/main.go): the pinned
// v1.20.0 schema has no wire shape that can carry this gate kind's answer
// faithfully, so — per the plan's own "pinned artifact wins" precedence rule
// — it is intentionally left untranslated here rather than inventing one.
// An ask-user GateOpened observed mid-drain therefore falls through exactly
// like any other untranslatable progress event (see drainToTerminal): the
// drain continues silently, and the gate stays open until something else
// (a future ACP capability, or a product-level ask-user answer path outside
// this facade) resolves it.
//
// # Host-owned gates (form, open-url) are never exposed here either
//
// gate.KindForm and gate.KindOpenURL are host-owned (gate.ResolverSession,
// never gate.ResolverLoop — see sessionruntime's hostOwnedGate), answered
// through session.GateHost's own AwaitGateAnswer path, not through a loop's
// RespondGate. This file's translation is scoped to gate.KindPermission with
// gate.ResolverLoop specifically (permissionOptionsFromGate checks both), so
// a host-owned gate is never even considered for request_permission — it is
// structurally excluded, not filtered out by a capability check. This
// matters because the design doc's own aspiration here ("session/elicitation
// ... when the connected client advertises elicitation") is doubly
// unrepresentable: the pinned schema has no elicitation method at all
// (confirmed absent in Task 1.7/1.8: no Elicit on ClientConn, no elicit
// entry in methods_gen.go) AND no client capability field for it either
// (protocol.ClientCapabilities has exactly Fs/Session/Terminal — see
// types_gen.go — nothing resembling elicitation or an open-URL interaction).
// There is therefore no capability to negotiate and nothing to gate a
// matrix test on beyond what gates_test.go already asserts: host-owned gate
// kinds are never flattened into request_permission, full stop, regardless
// of what a client advertises.
package agent

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/harness/pkg/gate"
)

// gatePermissionClient is the narrow client-conn capability the permission-
// gate round trip needs: exactly protocol.ClientConn's RequestPermission
// method, named here (like prompt.go's liveUpdateSender) so this file
// depends on the capability it actually uses rather than the concrete type.
type gatePermissionClient interface {
	RequestPermission(ctx context.Context, req protocol.RequestPermissionRequest) (*protocol.RequestPermissionResponse, error)
}

// liveClient is the full client-conn capability drainToTerminal needs over
// the course of one turn's drain: forwarding live session/update
// notifications (liveUpdateSender, Task 2.5) and running a permission-gate
// round trip (gatePermissionClient, this file). Production always supplies
// both from the same *protocol.ClientConn; the two are still named as
// separate embedded interfaces so a test fake that only exercises one half
// only needs to implement that half.
type liveClient interface {
	liveUpdateSender
	gatePermissionClient
}

// approvalOptionKinds maps each of the three exact gate.ApprovalAction
// values (see gate.ApprovalControls) to the ACP PermissionOptionKind hint
// with the equivalent, non-guessed semantic. This is the option-set fidelity
// this task requires: every permission gate offers exactly these three
// actions, and every one of them has an exact counterpart here.
var approvalOptionKinds = map[gate.ApprovalAction]protocol.PermissionOptionKind{
	gate.ApprovalApprove:                protocol.PermissionOptionKindAllowOnce,
	gate.ApprovalApproveAlwaysWorkspace: protocol.PermissionOptionKindAllowAlways,
	gate.ApprovalDeny:                   protocol.PermissionOptionKindRejectOnce,
}

// permissionOptionsFromGate reports the ACP PermissionOptions to offer for
// g, or ok=false when g is not translatable into a session/request_permission
// call at all.
//
// Translatable means: g.Kind is gate.KindPermission, g.Resolver is
// gate.ResolverLoop (excluding any host-owned gate structurally — see the
// package doc), and every one of g.Prompt.Controls parses as one of the
// three known gate.ApprovalAction strings. That last condition holds for
// every permission gate Harness actually opens today (gate.ApprovalControls
// is documented as "the exact, complete control set"), so this only ever
// reports false in practice for a non-permission gate kind; it exists as a
// fail-closed guard against ever fabricating an ACP option for a control
// this facade cannot map with a genuine, non-guessed PermissionOptionKind,
// rather than silently exposing a partial or invented option set.
//
// Each option's OptionID is the Harness Control.Action string verbatim: it
// is already the exact, stable machine token gate.GateResponse.Action
// expects back (see gate.ParseApprovalAction), so round-tripping a client's
// selection needs no separate lookup table — the chosen OptionID's string
// form already IS the answer to send.
func permissionOptionsFromGate(g gate.Gate) ([]protocol.PermissionOption, bool) {
	if g.Kind != gate.KindPermission || g.Resolver != gate.ResolverLoop {
		return nil, false
	}
	if len(g.Prompt.Controls) == 0 {
		return nil, false
	}
	opts := make([]protocol.PermissionOption, 0, len(g.Prompt.Controls))
	for _, c := range g.Prompt.Controls {
		action, ok := gate.ParseApprovalAction(c.Action)
		if !ok {
			return nil, false
		}
		kind, ok := approvalOptionKinds[action]
		if !ok {
			return nil, false
		}
		name := c.Label
		if name == "" {
			name = string(action)
		}
		opts = append(opts, protocol.PermissionOption{
			Kind:     kind,
			Name:     name,
			OptionID: protocol.PermissionOptionID(c.Action),
		})
	}
	return opts, true
}

// permissionToolCall builds the ToolCallUpdate a RequestPermissionRequest
// must carry ("Details about the tool call requiring permission") from the
// public fields a permission gate's envelope actually has: the tool call's
// identity (g.Subject.ToolExecutionID, via translate.go's toolCallID — the
// same derivation the live tool_call/tool_call_update notifications for the
// same call already use, so a client can correlate the two), the gate's
// Title, and its Body (the redacted capability description
// internal/loopruntime's renderApprovalBody already prepared for display).
// Kind/Locations/RawInput are left unset: the public Gate carries none of
// that structured data, and fabricating any of it would be guessing.
func permissionToolCall(g gate.Gate) protocol.ToolCallUpdate {
	upd := protocol.ToolCallUpdate{ToolCallID: toolCallID(g.Subject.ToolExecutionID)}
	if g.Prompt.Title != "" {
		title := g.Prompt.Title
		upd.Title = &title
	}
	if g.Prompt.Body != "" {
		upd.Content = []protocol.ToolCallContent{{
			Content: &protocol.Content{
				Content: protocol.ContentBlock{Text: &protocol.TextContent{Text: g.Prompt.Body}},
			},
		}}
	}
	return upd
}

// UnofferedPermissionOptionError reports that a client's
// session/request_permission response selected (or otherwise did not
// validly select) a PermissionOptionID this facade never offered for the
// named gate.
//
// All external input is untrusted, including — per the design doc's own
// framing of ACP as "a peer, not merely a client" — the client's chosen
// option: this error is deliberately raised BEFORE gate.GateResponse is ever
// built, so the gate is left open rather than answered on the strength of a
// selection that was never actually on offer. Task 2.7's session/close
// orchestration is expected to be the thing that eventually resolves a gate
// left open this way (deny), the same as any other outstanding permission
// request open when a session closes.
type UnofferedPermissionOptionError struct {
	GateID   gate.ID
	OptionID protocol.PermissionOptionID
}

func (e *UnofferedPermissionOptionError) Error() string {
	if e.OptionID == "" {
		return "agent: session/request_permission response did not select a valid offered option for gate " + e.GateID.String()
	}
	return "agent: client selected permission option " + strconv.Quote(string(e.OptionID)) + " which was not offered for gate " + e.GateID.String()
}

// gateFailClosedTimeout bounds the fail-closed RespondGate(Deny) delivery
// attempted by failGateClosed. It is independent of, and deliberately not
// derived from, the context the failed round trip was using — see
// failGateClosed.
const gateFailClosedTimeout = 10 * time.Second

// runPermissionGateRoundTrip runs one full session/request_permission round
// trip for a single gate.KindPermission gate, given the options
// permissionOptionsFromGate already derived for it. The caller
// (drainToTerminal) is responsible for having already confirmed g is
// translatable at all (permissionOptionsFromGate reported ok=true) and that
// the source event was Public — this function assumes both and focuses
// purely on the round trip.
//
// It returns nil on every outcome that resolves the gate one way or another
// (a valid client selection, a legitimate Cancelled outcome, or the
// fail-closed deny path below) and a non-nil *protocol.Fault only when the
// gate could not be resolved and was deliberately left open
// (UnofferedPermissionOptionError) — see the fail-closed rule in this
// package's tests for exactly which is which.
func runPermissionGateRoundTrip(ctx context.Context, client gatePermissionClient, live LiveSession, wireSessionID protocol.SessionID, g gate.Gate, opts []protocol.PermissionOption) *protocol.Fault {
	req := protocol.RequestPermissionRequest{
		SessionID: wireSessionID,
		Options:   opts,
		ToolCall:  permissionToolCall(g),
	}

	resp, err := client.RequestPermission(ctx, req)
	if err != nil {
		return failGateClosed(live, g.ID, err)
	}

	switch {
	case resp.Outcome.Selected != nil:
		return resolveSelectedOption(ctx, live, g.ID, opts, resp.Outcome.Selected.OptionID)
	case resp.Outcome.Cancelled != nil:
		// Per the pinned schema's own documented contract on
		// RequestPermissionOutcome.Cancelled: a client that sent
		// session/cancel "MUST respond to all pending
		// session/request_permission requests with this Cancelled
		// outcome." This is therefore an expected, legitimate resolution
		// (not a client error) — deny is the correct terminal action for a
		// gate whose surrounding prompt turn is being cancelled, and
		// drainToTerminal continues on to observe the turn's own
		// TurnInterrupted terminal exactly as it already does.
		if err := live.RespondGate(ctx, gate.GateResponse{
			GateID: g.ID,
			Action: string(gate.ApprovalDeny),
			Source: gate.ResponseSource{Kind: gate.ResponseFromPolicy, Reason: "prompt turn cancelled"},
		}); err != nil {
			return protocol.InternalError("session/prompt: RespondGate: "+err.Error(), err)
		}
		return nil
	default:
		// RequestPermissionOutcome.UnmarshalJSON already rejects any decoded
		// value without exactly one variant set (see types_gen.go), so this
		// is unreachable via the wire: a malformed outcome fails to decode
		// inside Conn.Call itself and is already handled by the err != nil
		// branch above. Defensive only — treat it exactly like an unoffered
		// selection (gate left open) rather than assuming either outcome.
		unoffered := &UnofferedPermissionOptionError{GateID: g.ID}
		return protocol.InternalError("session/prompt: "+unoffered.Error(), unoffered)
	}
}

// resolveSelectedOption validates that optionID was genuinely one of opts
// (the exact set this facade offered) before ever building a
// gate.GateResponse from it — untrusted client input must never reach
// RespondGate unchecked. A match answers the gate with that option's own
// Harness action string (see permissionOptionsFromGate: OptionID already IS
// that string). No match returns a typed *protocol.Fault wrapping
// UnofferedPermissionOptionError and leaves the gate open — RespondGate is
// never called on this path.
func resolveSelectedOption(ctx context.Context, live LiveSession, gateID gate.ID, opts []protocol.PermissionOption, optionID protocol.PermissionOptionID) *protocol.Fault {
	for _, o := range opts {
		if o.OptionID != optionID {
			continue
		}
		if err := live.RespondGate(ctx, gate.GateResponse{
			GateID: gateID,
			Action: string(optionID),
			Source: gate.ResponseSource{Kind: gate.ResponseFromUser},
		}); err != nil {
			return protocol.InternalError("session/prompt: RespondGate: "+err.Error(), err)
		}
		return nil
	}
	unoffered := &UnofferedPermissionOptionError{GateID: gateID, OptionID: optionID}
	return protocol.InternalError("session/prompt: "+unoffered.Error(), unoffered)
}

// failGateClosed is the fail-closed path for a permission round trip that
// could not be completed at all: client.RequestPermission itself returned
// an error, whether from a dead connection (*protocol.ConnClosedError), a
// context Task 2.7's session/close orchestration cancelled (see
// gateTracker), or any other transport failure.
//
// Per the plan's fail-closed rule ("an unresolvable/invalid client response,
// or a dead connection, must resolve the gate as deny/cancelled — NEVER left
// ambiguously open, NEVER defaulted to approve"), this always attempts to
// answer the gate Deny — using a context derived from context.Background(),
// deliberately NOT the failed ctx, since that ctx is frequently the exact
// reason this path was reached (a cancelled or dead connection's context),
// and the whole point is to deliver a deny answer to the live session
// regardless of what just happened to the ACP wire. If that deny attempt
// itself also fails, both errors are preserved (via errors.Join) as the
// returned Fault's local cause rather than silently discarding either.
func failGateClosed(live LiveSession, gateID gate.ID, cause error) *protocol.Fault {
	dctx, cancel := context.WithTimeout(context.Background(), gateFailClosedTimeout)
	defer cancel()
	denyErr := live.RespondGate(dctx, gate.GateResponse{
		GateID: gateID,
		Action: string(gate.ApprovalDeny),
		Source: gate.ResponseSource{Kind: gate.ResponseFromPolicy, Reason: "fail_closed: permission request could not be completed"},
	})
	if denyErr != nil {
		cause = errors.Join(cause, denyErr)
	}
	return protocol.InternalError("session/prompt: permission request could not be completed; denied the gate", cause)
}

// gateTracker tracks permission-gate round trips currently in flight, keyed
// by the ACP session they belong to. It exists purely as the integration
// point Task 2.7 needs: session/close's state machine step "outstanding
// permission requests resolved (denied)" is expected to call CancelSession
// for the closing session.
//
// It owns no resolution logic of its own — forcing an open round trip
// closed is done indirectly. begin derives a cancellable context from the
// caller's ctx and hands that (never the original) to the round trip, so
// CancelSession unblocks a pending client.RequestPermission call with
// ctx.Err() exactly the same way a real dead connection does, which
// runPermissionGateRoundTrip's failGateClosed path already answers Deny for.
// Task 2.7 therefore needs no separate deny-delivery code of its own: it
// only needs to call CancelSession and then await the prompt handler's own
// return, exactly as session/cancel already does today for an interrupted
// turn (see prompt.go's handleSessionCancel).
type gateTracker struct {
	mu   sync.Mutex
	open map[gate.ID]openGateEntry
}

// openGateEntry is one round trip's tracked state: which session it belongs
// to (so CancelSession can select the right ones) and the cancel func that
// unblocks it.
type openGateEntry struct {
	sessionID SessionID
	cancel    context.CancelFunc
}

// newGateTracker constructs an empty tracker.
func newGateTracker() *gateTracker {
	return &gateTracker{open: make(map[gate.ID]openGateEntry)}
}

// begin registers gateID as an open round trip for sessionID and returns a
// context derived from parent for the round trip to actually use, plus a
// release func the caller must call exactly once — typically deferred —
// when the round trip concludes by any path, to stop tracking it and free
// the derived context.
func (t *gateTracker) begin(parent context.Context, sessionID SessionID, gateID gate.ID) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	t.mu.Lock()
	t.open[gateID] = openGateEntry{sessionID: sessionID, cancel: cancel}
	t.mu.Unlock()
	return ctx, func() {
		t.mu.Lock()
		delete(t.open, gateID)
		t.mu.Unlock()
		cancel()
	}
}

// CancelSession cancels every gate round trip currently tracked as open for
// sessionID, without waiting for any of them to actually finish resolving —
// callers observe completion the same way they already observe an
// interrupted prompt's completion (draining to the prompt handler's own
// return), not through this call.
func (t *gateTracker) CancelSession(sessionID SessionID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, entry := range t.open {
		if entry.sessionID == sessionID {
			entry.cancel()
		}
	}
}
