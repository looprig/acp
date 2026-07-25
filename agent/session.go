// session.go implements ACP session identity mapping, the reusable
// session-scoped resolution helper, and the session/new handler: Task 2.3 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// Every session-scoped ACP method beyond session/new (session/prompt, the
// permission gates, session/cancel, session/close, and Phase 3's
// load/resume/list/delete) must resolve its wire sessionId through
// resolveSession before the id touches the host or any session state:
// resolveSession validates the string first (ParseSessionID) and only then
// consults the live-session registry, so a malformed id is rejected before a
// lookup — let alone a host call — is ever attempted.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/uuid"
)

// SessionIDReason classifies why a wire sessionId string failed
// ParseSessionID.
type SessionIDReason string

const (
	// SessionIDReasonEmpty: the wire sessionId was the empty string.
	SessionIDReasonEmpty SessionIDReason = "empty"
	// SessionIDReasonMalformed: the wire sessionId was not a structurally
	// valid 8-4-4-4-12 hyphenated UUID encoding (wrong length, a hyphen off
	// its fixed offset, or a non-hex digit).
	SessionIDReasonMalformed SessionIDReason = "malformed"
	// SessionIDReasonWrongVariant: the wire sessionId decoded to a
	// structurally valid UUID, but not the version-4/RFC-4122-variant stamp
	// every ACP session id carries (see ParseSessionID).
	SessionIDReasonWrongVariant SessionIDReason = "wrong_variant"
)

// SessionIDError reports that a wire sessionId string failed validation
// before ever reaching the live-session registry or the host boundary. All
// external input is untrusted, so every session-scoped method must reject a
// malformed id this way rather than let it flow deeper (see ParseSessionID).
type SessionIDError struct {
	Input  string
	Reason SessionIDReason
	cause  error
}

func (e *SessionIDError) Error() string {
	return "agent: invalid sessionId " + strconv.Quote(e.Input) + ": " + string(e.Reason)
}

func (e *SessionIDError) Unwrap() error { return e.cause }

// ParseSessionID validates and decodes an ACP wire sessionId into the
// SessionID (Harness UUID) it identifies. Every session-scoped handler must
// call this — directly, or through resolveSession — before the id touches
// any business logic or the host boundary.
//
// Beyond uuid.Parse's purely structural 8-4-4-4-12 hex check, ParseSessionID
// also requires the canonical version-4/variant-RFC4122 stamp uuid.New()
// always produces (see SessionID's doc in host.go: an ACP sessionId is
// always minted as "the Harness session UUID"). A structurally valid but
// differently-stamped 128-bit value — the nil UUID, a v1/v3/v5 UUID, or
// anything else this facade's own uuid.New() would never produce — is
// rejected here as wrong-variant, never silently accepted as if it could
// name a live session.
func ParseSessionID(id protocol.SessionID) (SessionID, error) {
	s := string(id)
	if s == "" {
		return SessionID{}, &SessionIDError{Input: s, Reason: SessionIDReasonEmpty}
	}
	parsed, err := uuid.Parse(s)
	if err != nil {
		return SessionID{}, &SessionIDError{Input: s, Reason: SessionIDReasonMalformed, cause: err}
	}
	if !isCanonicalSessionUUID(parsed) {
		return SessionID{}, &SessionIDError{Input: s, Reason: SessionIDReasonWrongVariant}
	}
	return parsed, nil
}

// isCanonicalSessionUUID reports whether u carries the version-4 and
// RFC-4122-variant stamp uuid.New() always writes: version nibble 4 in the
// high bits of byte 6, and variant bits 10 in the top two bits of byte 8.
func isCanonicalSessionUUID(u SessionID) bool {
	return u[6]>>4 == 0x4 && u[8]&0xc0 == 0x80
}

// resolveSession validates the wire sessionID string and looks it up in the
// facade's bounded live-session registry. It returns a *protocol.Fault
// directly (InvalidParams for a malformed id, ResourceNotFound for a
// well-formed id with no matching registered session) so callers can return
// the result unchanged as their handler's error.
func (a *Agent) resolveSession(id protocol.SessionID) (LiveSession, error) {
	sessionID, err := ParseSessionID(id)
	if err != nil {
		return nil, protocol.InvalidParams("sessionId: "+err.Error(), err)
	}
	live, ok := a.sessions.get(sessionID)
	if !ok {
		return nil, protocol.ResourceNotFound("sessionId: no such session", nil)
	}
	return live, nil
}

// handleSessionNew answers the session/new method. It fails closed if
// authentication is still required (AuthorizeSessionCreation — Task 2.2's
// gate, reused rather than reimplemented), fails closed if the live-session
// registry is already at MaxLiveSessions capacity, validates the incoming
// setup (NewSetup), creates the session through Host.NewSession, and
// registers the result under its own SessionID as the ACP wire sessionId —
// the Harness session UUID's string form (see SessionID's doc in host.go).
func (a *Agent) handleSessionNew(ctx context.Context, _ string, params json.RawMessage) (any, error) {
	if err := a.AuthorizeSessionCreation(); err != nil {
		return nil, err
	}

	var req protocol.NewSessionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("session/new: decode params", err)
	}

	// Advisory pre-check: avoid the cost (and, for a real host, the side
	// effect) of creating a session the registry cannot hold anyway. add's
	// own bounded check below is the authoritative enforcement point — see
	// sessionRegistry.atCapacity's doc.
	if a.sessions.atCapacity() {
		capErr := &TooManyLiveSessionsError{Max: MaxLiveSessions}
		return nil, protocol.InternalError("session/new: "+capErr.Error(), capErr)
	}

	caps := a.negotiatedClientCapabilities()
	// MCP composition has no Options capability yet (a later task's job per
	// the design doc's "MCP and external capabilities" section), so setup
	// fails closed and rejects any requested MCP servers rather than
	// silently accepting or dropping them.
	setup, err := NewSetup(req.Cwd, &caps, req.McpServers, false)
	if err != nil {
		return nil, protocol.InvalidParams("session/new: invalid setup: "+err.Error(), err)
	}

	live, err := a.opts.Host.NewSession(ctx, setup)
	if err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("session/new: "+err.Error(), err)
	}

	if err := a.sessions.add(live, setup.Cwd); err != nil {
		// The host has already created a live resource this facade can no
		// longer track: another concurrent session/new call won the
		// registry's last slot first (this branch is only reachable via the
		// race atCapacity's doc describes, not the common case). Rather than
		// silently abandoning it, make the same best-effort cleanup attempt
		// session/close makes on its own Shutdown step (close.go) — see
		// replay.go's shutdownOrphanedSession, shared with session/load's
		// identical orphan cleanup.
		shutdownOrphanedSession(live)
		return nil, protocol.InternalError("session/new: "+err.Error(), err)
	}

	configOptions, modes, err := a.initialConfigState(ctx, live.SessionID())
	if err != nil {
		// The live session is already registered above: this facade can no
		// longer track it once it stops being returned, so it gets the same
		// best-effort orphan cleanup as every other post-registration
		// failure in this handler (see shutdownOrphanedSession's doc).
		shutdownOrphanedSession(live)
		a.sessions.remove(live.SessionID())
		return nil, err
	}

	return protocol.NewSessionResponse{
		SessionID:     protocol.SessionID(live.SessionID().String()),
		ConfigOptions: configOptions,
		Modes:         modes,
	}, nil
}
