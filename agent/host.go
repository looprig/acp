// Package agent is the Looprig-facing ACP agent facade: the only package in
// this module that may import Harness's or Core's public packages (see
// acp/CLAUDE.md and harness/docs/plans/2026-07-17-acp-bridge-design.md,
// "Agent-side host boundary").
//
// ACP setup cannot depend directly on serve.Rig: a Harness rig's option type
// is opaque to ACP, workspace placement is fixed when a rig is defined, and
// ACP setup carries product concerns (cwd, MCP servers, replay, catalogs,
// runtime configuration) that Harness itself does not know about. This file
// therefore defines small, consumer-owned host interfaces that a product
// (e.g. CodeRig) implements against its own composition root, instead of the
// facade touching rig.SessionOption or workspace placement directly.
package agent

import (
	"context"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/sessionstore"
)

// SessionID identifies a session. It reuses the Harness session UUID as its
// underlying representation rather than defining a second durable identity
// system: when a host creates the underlying session, that UUID is also used
// as the ACP session id string (see design doc "Session identity and
// authorization"). Load, resume, close, and list parse and validate the wire
// string into this identity before crossing the host boundary.
type SessionID = uuid.UUID

// SessionHost is the consumer-owned factory a product implements to create,
// restore, and resume Harness-backed sessions on ACP's behalf. It is the
// narrow substitute for touching rig.SessionOption or workspace placement
// directly: the facade only ever calls through this boundary with a
// validated Setup.
//
// Wire-exposure trust boundary: an error this interface returns is folded
// into a *protocol.Fault's Message field essentially verbatim (see
// session.go's handleSessionNew, which does exactly this for NewSession) and
// therefore reaches the ACP wire. This is a deliberate, narrower case than
// Harness's own TurnFailed causes, some of which Harness itself documents as
// able to carry arbitrary, unsafe content (e.g. TurnPanicError.Detail) and
// which the facade sanitizes before they ever reach a caller (see prompt.go's
// sanitizedPromptFailure): a SessionHost is product-owned, not an internal
// Harness turn-failure cause, so it is trusted here the same way any other
// consumer-supplied adapter is. Implementations must still not embed
// secrets, credentials, or other sensitive material in any error they
// return.
type SessionHost interface {
	NewSession(context.Context, Setup) (LiveSession, error)
	LoadSession(context.Context, SessionID, Setup) (LoadedSession, error)
	ResumeSession(context.Context, SessionID, Setup) (LiveSession, error)
}

// LiveSession is the narrow data plane the ACP prompt/gate/interrupt handlers
// need. A harness-backed host satisfies it with session.Session (SessionID,
// Submit, SubscribeEvents, RespondGate, Interrupt) — a strict subset of that
// interface's full method set, since session.Session also carries
// loop-addressed and compaction methods (ActiveLoop, Loop, SubmitToLoop,
// Compact, CompactToLoop) that ACP's data plane does not need directly;
// Compact is exposed separately as the optional Compactor capability.
//
// Wire-exposure trust boundary: same rule as SessionHost's doc comment above.
// An error Submit, SubscribeEvents, or RespondGate returns is folded into a
// *protocol.Fault's Message field essentially verbatim (see prompt.go's
// handlePrompt/drainToTerminal and gates.go's runPermissionGateRoundTrip/
// resolveSelectedOption) and therefore reaches the ACP wire; implementations
// must not embed secrets, credentials, or other sensitive material in any
// error they return.
type LiveSession interface {
	SessionID() uuid.UUID
	Submit(context.Context, []content.Block) (uuid.UUID, error)
	SubscribeEvents(event.EventFilter) (event.Subscription, error)
	RespondGate(context.Context, gate.GateResponse) error
	Interrupt(context.Context) (bool, error)
}

// LoadedSession is a LiveSession plus the replay anchor the session/load
// handler needs: the point up to which durable history was reconstructed
// before the live controller took over. Task 3.1 refines this anchor if
// replay needs more than one TurnIndex per loop.
type LoadedSession struct {
	Live LiveSession
	// ReplayedThrough is the highest turn index reconstructed from durable
	// history for the session's replayed loop.
	ReplayedThrough event.TurnIndex
}

// SessionCloser is the segregated shutdown capability behind session/close.
// Harness puts Shutdown on SessionController, not on Session, so the host
// adapter exposes it to agent as a distinct closer rather than widening
// LiveSession (see design doc "Agent-side host boundary").
type SessionCloser interface {
	Shutdown(context.Context) error
}

// EventReplayer is the optional capability to open a public-only durable
// event replayer for session/load. Its natural Harness realization is
// sessionstore.Store.OpenEventReplayer — never the privileged
// OpenInternalEventReplayer/OpenInternalRecordReplayer variants, which must
// never be wired into the ACP path (see design doc "Load replay versus live
// streaming"). Task 3.1 refines the request shape if replay needs more than a
// session identity.
type EventReplayer interface {
	OpenEventReplayer(SessionID) (journal.EventReplayer, error)
}

// SessionCatalog is the optional capability to list known sessions for ACP
// session/list, matching Harness's sessionstore.Catalog.ListSessions. The
// facade owns bounded page construction and opaque cursor validation over the
// returned metadata; callers cannot depend on the catalog's key layout.
type SessionCatalog interface {
	ListSessions(context.Context) ([]sessionstore.SessionMeta, error)
}

// RuntimeConfigOption is a placeholder projection of one configurable
// runtime option (mode, model, effort, or another product-defined category).
// Task 4 refines its shape to match ACP's config-option discriminated union.
type RuntimeConfigOption struct {
	ID    string
	Value string
}

// RuntimeConfigCatalog is the optional capability to enumerate the runtime
// configuration options currently available for a session. Concretely this
// is backed by Harness's loop.ModeCatalog plus a product's own model/effort
// catalogs (Harness deliberately has no model/effort catalog itself — see
// design doc "Session configuration").
type RuntimeConfigCatalog interface {
	RuntimeConfigOptions(context.Context, SessionID) ([]RuntimeConfigOption, error)
}

// RuntimeConfigController is the optional capability to apply a validated
// runtime configuration change and return the complete resulting option
// state so dependent choices stay coherent. Config writes are idempotent:
// setting an option to its current value must succeed without side effects.
// Concretely this is backed by Harness's loop.Controller (SetMode, Change)
// plus a product's own model/effort controllers.
type RuntimeConfigController interface {
	SetRuntimeConfigOption(context.Context, SessionID, RuntimeConfigOption) ([]RuntimeConfigOption, error)
}

// Compactor is the optional capability to trigger the session's focused/
// active-loop compaction, matching Harness's session.Session.Compact. It
// returns the command id used to correlate the compaction outcome exactly
// like Submit; the ACP prompt handler completes only once that outcome is
// observed (see design doc "Slash commands and compaction").
type Compactor interface {
	Compact(context.Context) (uuid.UUID, error)
}

// SessionDeleter is the optional capability to permanently delete a
// session's durable history. It is advertised only when the host supplies
// explicit storage and authorization semantics (see design doc "Cancellation
// and close").
type SessionDeleter interface {
	DeleteSession(context.Context, SessionID) error
}

// Authenticator is the optional capability to complete an ACP authenticate
// call for the method the client selected. The facade validates methodID
// against the advertised authMethods before calling this.
type Authenticator interface {
	Authenticate(context.Context, protocol.AuthMethodID) error
}

// LogoutHandler is the optional capability to clear an authenticated
// connection's credentials for ACP's logout method.
type LogoutHandler interface {
	Logout(context.Context) error
}
