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

// SessionCatalogEntry is one entry a SessionCatalog reports for
// session/list: a Harness catalog record plus this host's own knowledge (if
// any) of that session's actual working directory.
//
// Harness's sessionstore.SessionMeta carries no field for a session's live
// working-directory path at all — SessionMeta.CurrentWorkspace is a
// WorkspacePointer naming a content-addressed workspace-SNAPSHOT digest, not
// a filesystem path (see list.go's package doc for the full discrepancy).
// Cwd is this module's own consumer-owned overlay for that gap: it must be
// an absolute path when the host knows this session's actual working
// directory, and left empty when it does not — never a relative path, a
// placeholder, or a best-effort guess. handleSessionList (list.go) omits a
// session from the session/list response entirely rather than emit the
// pinned ACP schema's required-absolute-path SessionInfo.Cwd as an empty
// string, UNLESS the session is currently live in this facade's own bounded
// session registry, in which case its already-validated Setup.Cwd is used
// instead of Cwd here and this field is not consulted at all for that
// session id (see handleSessionList's overlay step).
//
// A durable fix — persisting a real per-session cwd inside Harness's own
// sessionstore.SessionMeta, so a host would not need to track this mapping
// itself — is a legitimate future Harness-side follow-up. It is out of
// scope here (Harness is read-only in this plan); this field is the
// narrower, consumer-side workaround until that lands, if it ever does.
type SessionCatalogEntry struct {
	// Meta is the underlying Harness catalog record for this session.
	Meta sessionstore.SessionMeta
	// Cwd is this session's absolute working directory, if this host knows
	// it; empty if unknown. See this type's doc for the full contract.
	Cwd string
}

// SessionCatalog is the optional capability to list known sessions for ACP
// session/list, matching Harness's sessionstore.Catalog.ListSessions plus
// this module's own cwd overlay (SessionCatalogEntry.Cwd — see its doc for
// the exact contract an implementation must uphold). The facade owns bounded
// page construction and opaque cursor validation over the returned metadata;
// callers cannot depend on the catalog's key layout.
type SessionCatalog interface {
	ListSessions(context.Context) ([]SessionCatalogEntry, error)
}

// RuntimeConfigValue is one selectable value of a RuntimeConfigOption: its
// stable wire identity, human-readable label, and optional description. It
// mirrors the pinned schema's SessionConfigSelectOption field-for-field so
// config.go's translation to the wire type is a straight copy, never a
// guess.
type RuntimeConfigValue struct {
	ID          protocol.SessionConfigValueID
	Name        string
	Description string
}

// RuntimeConfigOption is one configurable runtime option's complete current
// state: its identity, semantic category, human-readable label, the full set
// of values it currently offers, and which of those values is currently
// active. This is the discriminated union Task 4.1 was asked to refine
// RuntimeConfigOption's shape into — discriminated on Category, exactly
// mirroring the pinned schema's SessionConfigOptionCategory constants (mode,
// model, model_config, thought_level) plus any product-defined free-form
// category the schema reserves for values beginning with "_" (see
// protocol.SessionConfigOptionCategory's doc). Every RuntimeConfigOption
// config.go builds is projected onto the wire as a "select" variant
// (protocol.SessionConfigSelect): a dropdown over Values with CurrentValue
// marking the active one. This module has no need for the wire's "boolean"
// variant — every category this facade is asked to support (mode, model,
// thought level, and any further product-defined option) is naturally an
// enumerated choice, never a raw on/off toggle — so RuntimeConfigOption does
// not model one; a host that needs a boolean-shaped option is out of this
// task's scope.
//
// Category is protocol.SessionConfigOptionCategory directly rather than a
// second parallel host-side enum: this package already imports
// acp/protocol (see SessionID/Authenticator above), the wire category is
// exactly the semantic this field carries, and duplicating it would only
// invite the two to drift.
//
// Concretely, a RuntimeConfigCatalog implementation sources the mode
// category's Values/CurrentValue from Harness's loop.ModeCatalog.Modes()
// (translating each loop.ModeName into a RuntimeConfigValue) and every other
// category from the product's own model/effort/access catalogs — Harness
// deliberately has no model/effort/access catalog itself (see design doc
// "Session configuration"). acp/agent never imports pkg/loop to do this
// itself: LiveSession (this file) deliberately narrows session.Session down
// to the data-plane methods a prompt/gate/interrupt handler needs and omits
// ActiveLoop/Loop/SubmitToLoop, so a RuntimeConfigCatalog/
// RuntimeConfigController is where a host reaches into its own
// loop.ModeCatalog/loop.Controller instead — a consumer-owned adapter, not a
// Harness-native type, exactly like SessionHost and the rest of this file.
type RuntimeConfigOption struct {
	ID           protocol.SessionConfigID
	Category     protocol.SessionConfigOptionCategory
	Name         string
	Description  string
	Values       []RuntimeConfigValue
	CurrentValue protocol.SessionConfigValueID
}

// ModeConfigOptionID is the well-known RuntimeConfigOption.ID a
// RuntimeConfigCatalog/RuntimeConfigController implementation MUST use for
// the session-mode option (Category protocol.SessionConfigOptionCategoryMode,
// Values/CurrentValue sourced from loop.ModeCatalog.Modes() — see
// RuntimeConfigOption's doc). This is what keeps session/set_mode and
// session/set_config_option convergent: the pinned schema's
// SetSessionModeRequest carries only a bare SessionModeID, no configId, so
// config.go's handleSessionSetMode always targets this constant, translating
// the request into exactly the same call handleSessionSetConfigOption would
// make for configId=ModeConfigOptionID — both paths run through the single
// unexported applyConfigOption (config.go), never two independent
// implementations that could drift.
const ModeConfigOptionID protocol.SessionConfigID = "mode"

// RuntimeConfigCatalog is the optional capability to enumerate the runtime
// configuration options currently available for a session. Concretely this
// is backed by Harness's loop.ModeCatalog plus a product's own model/effort
// catalogs (Harness deliberately has no model/effort catalog itself — see
// design doc "Session configuration").
//
// config.go always fetches this catalog fresh, immediately before applying a
// requested change: this is the latest-snapshot validation the design
// requires (an option id or value id valid a moment ago may no longer be —
// a mode removed, a model retired — so the check must run against what is
// true right now, never a value cached from session/new or an earlier
// request).
type RuntimeConfigCatalog interface {
	RuntimeConfigOptions(context.Context, SessionID) ([]RuntimeConfigOption, error)
}

// RuntimeConfigChange is a validated request to set one RuntimeConfigOption
// (identified by OptionID) to one of the values it currently offers
// (ValueID). config.go constructs this only after checking both ids against
// a RuntimeConfigCatalog snapshot fetched in the same request.
type RuntimeConfigChange struct {
	OptionID protocol.SessionConfigID
	ValueID  protocol.SessionConfigValueID
}

// RuntimeConfigController is the optional capability to apply a validated
// runtime configuration change and return the complete resulting option
// state so dependent choices stay coherent. Concretely this is backed by
// Harness's loop.Controller (SetMode, Change) plus a product's own
// model/effort controllers — reached the same consumer-owned-adapter way
// RuntimeConfigOption's doc describes, since LiveSession does not expose
// Controller either.
//
// Config writes are idempotent: setting an option to its current value must
// succeed without side effects. config.go itself enforces this — it compares
// the requested ValueID against the latest catalog's CurrentValue for that
// option BEFORE ever calling SetRuntimeConfigOption, and short-circuits to a
// no-op success (no controller call, no config_option_update notification)
// when they already match. An implementation of this interface is therefore
// never asked to special-case a same-value request, and need not itself be
// idempotent for this contract to hold.
type RuntimeConfigController interface {
	SetRuntimeConfigOption(context.Context, SessionID, RuntimeConfigChange) ([]RuntimeConfigOption, error)
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
