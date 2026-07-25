// close.go implements the session/close orchestration state machine: Task
// 2.7 of harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// Per the design doc's "Cancellation and close" section, session/close is
// "an orchestrated lifecycle operation, not a direct registry delete":
//
//  1. Mark the ACP session closing and reject new prompts (promptTracker.
//     markClosing, reused from prompt.go's begin/end machinery — Task 2.4).
//  2. Cancel in-flight work with behavior equivalent to session/cancel
//     (LiveSession.Interrupt — the same call handleSessionCancel already
//     makes; not reimplemented here).
//  3. Resolve outstanding permission requests owned by the connection
//     (gateTracker.CancelSession, Task 2.6's own integration point: it
//     unblocks any pending client.RequestPermission call with ctx.Err(),
//     which drainToTerminal's failGateClosed path already turns into
//     RespondGate(Deny) automatically — no separate deny-delivery code is
//     needed here).
//  4. Wait for the in-flight prompt (if any) to actually finish draining —
//     not fire-and-forget: the channel promptTracker.markClosing returns
//     closes only once the drained handlePrompt call has returned.
//  5. Call the optional SessionCloser.Shutdown capability, bounded by
//     closeShutdownGrace.
//  6. Remove the session from the live registry — only now, after every
//     step above has completed.
//
// Durable history is never touched here: SessionDeleter (session/delete) is
// a completely separate optional capability this handler never calls.
package agent

import (
	"context"
	"encoding/json"
	"time"

	"github.com/looprig/acp/protocol"
)

// closeShutdownGrace bounds the optional SessionCloser.Shutdown call
// session/close makes once an in-flight prompt (if any) has finished
// draining. It is deliberately derived from context.Background(), not the
// inbound request ctx (see handleSessionClose): a client that cancels its
// own session/close call must not be able to abandon Shutdown mid-flight and
// leave the underlying resource half torn down. The bound mirrors the
// backend's own agentShutdownGrace = 5s (harness/internal/.../actor.go) for
// the same purpose — a bounded best-effort teardown call at a comparable
// layer of the stack.
const closeShutdownGrace = 5 * time.Second

// closeDrainTimeout bounds how long handleSessionClose waits for an
// in-flight prompt's drain to actually finish once cancellation has been
// requested (step 2 above) and any gate it was parked on has been forced
// closed (step 3). Unlike session/cancel (a notification with nowhere to
// report a timeout to), session/close's caller IS waiting on a response, so
// a wedged host implementation must not hang it forever: teardown still
// proceeds past this point even if the drain has not signaled completion,
// rather than blocking session/close indefinitely.
const closeDrainTimeout = 30 * time.Second

// handleSessionClose answers the session/close method: see this file's
// package doc for the full six-step state machine. It is registered
// unconditionally (see Register), matching session/new, session/prompt, and
// session/cancel: every product-facing agent needs Options.Host, and this
// handler has no capability gate of its own.
func (a *Agent) handleSessionClose(ctx context.Context, _ string, params json.RawMessage) (any, error) {
	var req protocol.CloseSessionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("session/close: decode params", err)
	}

	live, err := a.resolveSession(req.SessionID)
	if err != nil {
		return nil, err
	}
	sessionID := live.SessionID()

	// Step 1: mark closing. From this point on, every new session/prompt
	// for sessionID is rejected (promptBeginClosing) — see prompt.go's
	// begin. done is the channel that closes once the currently in-flight
	// prompt (if any) actually finishes; wasInFlight tells us whether one
	// exists to wait for at all.
	done, wasInFlight := a.prompts.markClosing(sessionID)

	// Step 2: cancel in-flight work with behavior equivalent to
	// session/cancel. Interrupt is safe to call even when nothing is
	// running (see handleSessionCancel's own doc), so this is unconditional
	// exactly like session/cancel's own call, not merely reserved for the
	// wasInFlight case.
	ictx, icancel := context.WithTimeout(ctx, cancelInterruptTimeout)
	_, _ = live.Interrupt(ictx)
	icancel()

	// Step 3: resolve outstanding permission requests. If the in-flight
	// prompt (if any) is currently parked on a permission gate,
	// CancelSession is what actually unblocks its drain — Interrupt alone
	// cannot, since the loop will not observe it until the gate resolves
	// one way or another. This must run before waiting on done below, or
	// that wait could block forever on a gate nothing will ever answer.
	a.gates.CancelSession(sessionID)

	// Step 4: wait for the drain to actually finish, bounded — never
	// fire-and-forget.
	if wasInFlight {
		select {
		case <-done:
		case <-time.After(closeDrainTimeout):
		}
	}

	// Step 5: optional bounded Shutdown.
	if closer, ok := live.(SessionCloser); ok {
		sctx, scancel := context.WithTimeout(context.Background(), closeShutdownGrace)
		_ = closer.Shutdown(sctx)
		scancel()
	}

	// Step 6: registry removal — only now that every step above has
	// completed. SessionDeleter is never consulted here: closing a session
	// must never delete its durable history.
	a.sessions.remove(sessionID)

	// Now that sessionID can never resolve again (resolveSession consults
	// a.sessions first, on every session-scoped call), the promptTracker
	// bookkeeping for it can be dropped entirely rather than held onto for
	// the rest of the process's lifetime — see promptTracker.forget's doc.
	a.prompts.forget(sessionID)

	return protocol.CloseSessionResponse{}, nil
}
