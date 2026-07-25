// resume.go implements the session/resume handler: Task 3.2 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// Unlike session/load (replay.go), Host.ResumeSession returns a plain
// LiveSession, not a LoadedSession (see host.go's SessionHost doc): there is
// no replay anchor and therefore no durable-history reconstruction to
// perform. The pinned schema documents session/resume as resuming a session
// "without returning previous messages (unlike session/load) ... for agents
// that can resume sessions but don't implement full session loading." This
// handler accordingly never calls a.client.SessionUpdate at all — the exact
// property this file's test proves — making it the simplest of the three
// session-establishment handlers: validate Setup, call Host.ResumeSession,
// register the result, and respond immediately.
package agent

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/looprig/acp/protocol"
)

// handleSessionResume answers the session/resume method. Unlike
// session/load, it has no independent capability gate of its own: every
// SessionHost implementation supplies ResumeSession as a required method
// (host.go), so it is registered unconditionally in Register, matching
// capabilities.go's unconditional SessionCapabilities.Resume advertisement.
//
// It fails closed exactly like handleSessionNew and handleSessionLoad: the
// auth gate (AuthorizeSessionCreation), the wire sessionId (ParseSessionID),
// the registry's advisory capacity pre-check, and Setup validation (NewSetup)
// all run before Host.ResumeSession is ever called. Any failure from
// Host.ResumeSession onward that leaves a live, host-backed session this
// facade can no longer track (an add-time registry-capacity race) gets the
// same best-effort orphan cleanup as session/new and session/load
// (shutdownOrphanedSession, replay.go) — reused, not reimplemented.
func (a *Agent) handleSessionResume(ctx context.Context, _ string, params json.RawMessage) (any, error) {
	if err := a.AuthorizeSessionCreation(); err != nil {
		return nil, err
	}

	var req protocol.ResumeSessionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("session/resume: decode params", err)
	}

	sessionID, err := ParseSessionID(req.SessionID)
	if err != nil {
		return nil, protocol.InvalidParams("sessionId: "+err.Error(), err)
	}

	// Advisory pre-check, mirroring handleSessionNew's and handleSessionLoad's:
	// avoid the cost (and, for a real host, the side effect) of resuming a
	// session the registry cannot hold anyway. sessions.add's own bounded
	// check below is still the authoritative enforcement point.
	if a.sessions.atCapacity() {
		capErr := &TooManyLiveSessionsError{Max: MaxLiveSessions}
		return nil, protocol.InternalError("session/resume: "+capErr.Error(), capErr)
	}

	caps := a.negotiatedClientCapabilities()
	// MCP composition has no Options capability yet (same restriction
	// handleSessionNew and handleSessionLoad apply — see their own docs), so
	// setup fails closed rather than silently accepting or dropping requested
	// MCP servers.
	setup, err := NewSetup(req.Cwd, &caps, req.McpServers, false)
	if err != nil {
		return nil, protocol.InvalidParams("session/resume: invalid setup: "+err.Error(), err)
	}

	live, err := a.opts.Host.ResumeSession(ctx, sessionID, setup)
	if err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("session/resume: "+err.Error(), err)
	}

	if err := a.sessions.add(live, setup.Cwd); err != nil {
		// The host has already created/restored a live resource this facade
		// can no longer track: another concurrent session-establishment call
		// won the registry's last slot first. Same best-effort cleanup as
		// handleSessionNew's and handleSessionLoad's identical race.
		shutdownOrphanedSession(live)
		return nil, protocol.InternalError("session/resume: "+err.Error(), err)
	}

	configOptions, modes, err := a.initialConfigState(ctx, live.SessionID())
	if err != nil {
		// The live session is already registered above: this facade can no
		// longer track it once it stops being returned, so it gets the same
		// best-effort orphan cleanup as every other post-registration
		// failure in this handler.
		shutdownOrphanedSession(live)
		a.sessions.remove(live.SessionID())
		return nil, err
	}

	// No replay: this response is sent having emitted zero session/update
	// notifications, unlike handleSessionLoad's replayHistory step.
	return protocol.ResumeSessionResponse{ConfigOptions: configOptions, Modes: modes}, nil
}
