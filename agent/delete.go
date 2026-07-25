// delete.go implements the session/delete handler: Task 3.4 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md, the final task
// of Phase 3.
//
// session/delete is the deliberate converse of close.go's own invariant.
// close.go documents that "durable history is never touched" by
// session/close -- SessionDeleter is a completely separate optional
// capability that handler never calls. This file is the mirror image: it is
// the ONLY path in this package that may ever invoke SessionDeleter, and
// only for a sessionId that does NOT currently name a live, registered
// session (a.sessions.get, registry.go). Attempting to delete a session
// while it is still live is rejected outright, before the Deleter is ever
// consulted: durable history must never be deleted out from under a live
// session, so a client must session/close it first (see the design doc's
// "Cancellation and close": "Delete remains separate and is advertised only
// when a host supplies explicit storage and authorization semantics").
//
// # Wire error for "session still active"
//
// The pinned schema (protocol/schema/v1/schema.json) defines
// DeleteSessionRequest as exactly {sessionId, _meta} and DeleteSessionResponse
// as exactly {_meta} -- no dedicated field or error code anywhere in the
// schema names "the session is still active" as a distinguished condition,
// unlike (for example) ErrorCodeResourceNotFound or
// ErrorCodeAuthenticationRequired, which the schema's ErrorCode $def does
// single out. Absent a schema-documented specific code, this handler reports
// the rejection as protocol.InvalidRequest (-32600), reusing the exact
// precedent prompt.go's ErrSessionClosing and ErrPromptAlreadyInFlight
// already set for an analogous "invalid given current session state"
// condition (see prompt.go's handlePrompt) -- not a guessed or novel
// mapping.
package agent

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/looprig/acp/protocol"
)

// ErrSessionStillLive is the local cause behind the *protocol.Fault returned
// when session/delete is attempted on a sessionId that currently names a
// live, registered session (see this file's package doc). It never crosses
// the wire itself (only Message/Code/Data do -- see protocol.Fault); it
// exists so a local caller can errors.Is/As it, matching prompt.go's
// ErrSessionClosing/ErrPromptAlreadyInFlight sentinels.
var ErrSessionStillLive = errors.New("agent: session is still live; close it before deleting")

// handleSessionDelete answers the session/delete method. It is only ever
// registered when Options.Deleter is non-nil (see Register), matching
// capabilities.go's SessionCapabilities.Delete advertisement gate: a client
// is never told this capability is supported yet has the method rejected,
// and never told it is unsupported yet has it accepted.
//
// Order of checks, each failing closed before the next ever runs:
//  1. Decode params.
//  2. ParseSessionID -- a malformed wire sessionId is rejected before either
//     the registry or the Deleter is ever touched (all external input is
//     untrusted).
//  3. Liveness: if sessionID currently names a registered live session
//     (a.sessions.get), reject with InvalidRequest and ErrSessionStillLive
//     -- see this file's package doc for why this is the right wire
//     encoding. The Deleter is NEVER called on this path.
//  4. Only once sessionID is confirmed NOT live does Options.Deleter.
//     DeleteSession ever run, exactly like every other optional-capability
//     handler's Host-error convention: a returned *protocol.Fault passes
//     through unchanged, anything else is wrapped as InternalError.
func (a *Agent) handleSessionDelete(ctx context.Context, _ string, params json.RawMessage) (any, error) {
	var req protocol.DeleteSessionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("session/delete: decode params", err)
	}

	sessionID, err := ParseSessionID(req.SessionID)
	if err != nil {
		return nil, protocol.InvalidParams("sessionId: "+err.Error(), err)
	}

	if _, live := a.sessions.get(sessionID); live {
		return nil, protocol.InvalidRequest("session/delete: session is still active; close it first", ErrSessionStillLive)
	}

	if err := a.opts.Deleter.DeleteSession(ctx, sessionID); err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("session/delete: "+err.Error(), err)
	}

	return protocol.DeleteSessionResponse{}, nil
}
