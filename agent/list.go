// list.go implements the session/list handler and the session_info_update
// observation callback: Task 3.3 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// # The CurrentWorkspace/cwd discrepancy
//
// This task's design doc ("Session listing and metadata") says
// SessionMeta.CurrentWorkspace maps to ACP's cwd. It does not: reading the
// REAL harness/pkg/sessionstore/catalog.go, SessionMeta.CurrentWorkspace is a
// WorkspacePointer — Ref (a content-addressed workspace-SNAPSHOT digest,
// "v1:sha256:<64 hex>"), EventID, Seq, and Source (checkpoint vs restore).
// That identifies which immutable snapshot the session's workspace was last
// pointed at, not a live filesystem directory path; SessionMeta carries no
// field anywhere that represents a session's working-directory string at
// all. This is a real design-doc/reality gap, not a naming difference to
// paper over: mapping WorkspacePointer.Ref into ACP's Cwd string would be
// actively wrong (a client would try to treat a content hash as a path), so
// this file deliberately leaves SessionInfo.Cwd = "" for every entry instead.
// This narrows what a client can do with a listed session's cwd (and makes
// ListSessionsRequest.Cwd filtering meaningless — see handleSessionList's
// doc), and is flagged prominently in this task's report rather than worked
// around silently, per this plan's own talk-first rule for a real spec
// conflict. It is not this module's place to invent a change to Harness
// (harness is read-only here); if a real per-session cwd is ever needed from
// the catalog, that is a Harness-side addition to negotiate separately.
//
// # Pagination
//
// The facade paginates over the catalog's SessionMeta entries sorted
// ascending by SessionID bytes (sortedSessionMetas) — a stable, catalog-
// independent ordering that does not depend on ListSessions' own return
// order. A cursor is an opaque, HMAC-authenticated token naming the last
// SessionID included in the previous page (cursorPayload); it is validated
// (decodeListCursor) before ever being used to compute a page, and any
// structural or authentication failure is a typed *InvalidCursorError,
// mapped to InvalidParams — never silently treated as "start over" or
// accepted as some other position. The HMAC key (Agent.cursorKey) is
// generated fresh via crypto/rand at New() time, once per Agent instance:
// cursors are short-lived pagination tokens a client is expected to consume
// within one process's lifetime (they are never persisted or expected to
// survive a restart), so there is no reason to derive the key from anything
// stable across restarts, and every reason not to use a fixed or
// caller-influenced key.
//
// # session_info_update
//
// host.go's SessionCatalog interface exposes only a pull method
// (ListSessions): there is nothing already there a product could register a
// push callback against, and the direction of this signal is product-into-
// facade (the product's own catalog-observation mechanism learns of a
// change and tells the facade), not facade-into-product like every other
// host.go seam. ObserveSessionMeta is the narrow, facade-owned callback
// surface that fills that gap: a product calls it once per catalog
// observation (a KV watch, an event-driven fold — never a poll loop this
// facade runs itself), and it is the facade's OWN job — not the caller's —
// to decide whether that observation actually changed anything worth
// telling the client about (see ObserveSessionMeta's doc).
package agent

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/sessionstore"
)

// MaxPageSize bounds the number of SessionInfo entries session/list returns
// in one response. The facade owns this bound unconditionally: the pinned
// ListSessionsRequest schema carries no client-supplied page-size field, so
// every page is exactly MaxPageSize entries, or fewer only on the final page.
const MaxPageSize = 100

// cursorKeySize is the HMAC-SHA256 key length (32 bytes, matching SHA-256's
// block-aligned recommended key size) generated fresh for every Agent
// instance — see this file's package doc for why a per-instance, per-process
// key is the right choice here.
const cursorKeySize = 32

// --- cursor codec ------------------------------------------------------

// cursorPayload is the plaintext structure inside an opaque session/list
// pagination cursor: the sorted key (a catalog SessionID, string form) to
// resume after. It is never parsed as JSON until AFTER the accompanying HMAC
// tag has already been verified (decodeListCursor) — untrusted wire input is
// validated before it is used, per this module's fail-closed rule.
type cursorPayload struct {
	After string `json:"after"`
}

// CursorErrorReason classifies why a wire session/list cursor failed
// validation.
type CursorErrorReason string

const (
	// CursorReasonMalformed: the cursor string was not the "<payload>.<tag>"
	// shape at all, or either segment was not valid base64url.
	CursorReasonMalformed CursorErrorReason = "malformed"
	// CursorReasonTampered: the cursor decoded structurally, but its tag does
	// not authenticate against this Agent's cursor key — the payload was
	// altered, forged, or produced by a different Agent instance/key.
	CursorReasonTampered CursorErrorReason = "tampered"
	// CursorReasonInvalidPayload: the tag authenticated, but the payload
	// bytes are not a valid cursorPayload (bad JSON, unknown field, trailing
	// data, or an After value that does not parse as a session id).
	CursorReasonInvalidPayload CursorErrorReason = "invalid_payload"
)

// InvalidCursorError reports that a wire session/list cursor string failed
// validation before ever being used to compute a page. All three
// CursorErrorReason cases fail exactly the same way from the caller's
// perspective: session/list rejects the request outright rather than falling
// back to "start from the beginning" (which would silently and incorrectly
// reorder a client's in-progress pagination) or guessing at some other
// position.
type InvalidCursorError struct {
	Reason CursorErrorReason
	cause  error
}

func (e *InvalidCursorError) Error() string {
	msg := "agent: invalid session/list cursor: " + string(e.Reason)
	if e.cause != nil {
		return msg + ": " + e.cause.Error()
	}
	return msg
}

func (e *InvalidCursorError) Unwrap() error { return e.cause }

// cursorTag computes the HMAC-SHA256 authentication tag for payload under
// a's cursor key.
func (a *Agent) cursorTag(payload []byte) []byte {
	mac := hmac.New(sha256.New, a.cursorKey[:])
	mac.Write(payload)
	return mac.Sum(nil)
}

// encodeListCursor builds the opaque wire cursor naming after as the sorted
// key to resume from on the next call: base64url(payload) + "." +
// base64url(HMAC tag over payload).
func (a *Agent) encodeListCursor(after SessionID) (string, error) {
	payload, err := json.Marshal(cursorPayload{After: after.String()})
	if err != nil {
		// Unreachable: cursorPayload is a single plain string field with no
		// cyclic reference, channel, or function value to choke on.
		return "", err
	}
	tag := a.cursorTag(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(tag), nil
}

// decodeListCursor validates and decodes a wire session/list cursor string
// into the SessionID to resume after. Every external input the cursor
// carries is untrusted: the tag is checked with a constant-time comparison
// (hmac.Equal) before the payload is even parsed as JSON, and the JSON
// decode itself rejects unknown fields and trailing bytes (matching
// sessionstore's own decodeSessionMeta discipline) rather than tolerating a
// loosely-shaped payload.
func (a *Agent) decodeListCursor(token string) (SessionID, error) {
	segments := strings.Split(token, ".")
	if len(segments) != 2 {
		return SessionID{}, &InvalidCursorError{Reason: CursorReasonMalformed}
	}
	payload, err := base64.RawURLEncoding.DecodeString(segments[0])
	if err != nil {
		return SessionID{}, &InvalidCursorError{Reason: CursorReasonMalformed, cause: err}
	}
	tag, err := base64.RawURLEncoding.DecodeString(segments[1])
	if err != nil {
		return SessionID{}, &InvalidCursorError{Reason: CursorReasonMalformed, cause: err}
	}

	want := a.cursorTag(payload)
	if !hmac.Equal(tag, want) {
		return SessionID{}, &InvalidCursorError{Reason: CursorReasonTampered}
	}

	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	var p cursorPayload
	if err := dec.Decode(&p); err != nil {
		return SessionID{}, &InvalidCursorError{Reason: CursorReasonInvalidPayload, cause: err}
	}
	if _, err := dec.Token(); err != io.EOF {
		return SessionID{}, &InvalidCursorError{Reason: CursorReasonInvalidPayload}
	}

	id, err := uuid.Parse(p.After)
	if err != nil {
		return SessionID{}, &InvalidCursorError{Reason: CursorReasonInvalidPayload, cause: err}
	}
	return id, nil
}

// --- sorting and pagination ----------------------------------------------

// sortedSessionMetas returns metas sorted ascending by SessionID bytes — the
// stable, catalog-order-independent key session/list paginates over. It
// copies rather than mutating the caller's slice.
func sortedSessionMetas(metas []sessionstore.SessionMeta) []sessionstore.SessionMeta {
	sorted := append([]sessionstore.SessionMeta(nil), metas...)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].SessionID[:], sorted[j].SessionID[:]) < 0
	})
	return sorted
}

// paginateSessionMetas returns the page of sorted starting strictly after
// the after key (or from the start, when after is nil), bounded to at most
// MaxPageSize entries, plus the SessionID to resume after on a following
// call (nil once the page reaches the end of sorted).
func paginateSessionMetas(sorted []sessionstore.SessionMeta, after *SessionID) ([]sessionstore.SessionMeta, *SessionID) {
	start := 0
	if after != nil {
		start = sort.Search(len(sorted), func(i int) bool {
			return bytes.Compare(sorted[i].SessionID[:], (*after)[:]) > 0
		})
	}
	end := start + MaxPageSize
	if end > len(sorted) {
		end = len(sorted)
	}
	page := sorted[start:end]

	var next *SessionID
	if end < len(sorted) {
		id := page[len(page)-1].SessionID
		next = &id
	}
	return page, next
}

// --- SessionMeta -> SessionInfo/SessionInfoUpdate mapping -----------------

// optionalString returns nil for an empty string, else a pointer to s: ACP's
// optional string fields distinguish "absent" from "present but empty", and
// an unset SessionMeta.Title is absent, not an empty label.
func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// optionalTimestamp returns nil for a zero time.Time, else its RFC3339 (ISO
// 8601) rendering in UTC — matching SessionInfo.UpdatedAt's and
// SessionInfoUpdate.UpdatedAt's documented "ISO 8601 timestamp" shape.
func optionalTimestamp(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format(time.RFC3339)
	return &s
}

// sessionInfoFromMeta maps one Harness sessionstore.SessionMeta catalog
// entry onto the ACP SessionInfo session/list reports: {sessionId, cwd,
// title, updatedAt}, per this task's adapter contract. Cwd is always "" and
// AdditionalDirectories is always omitted — see this file's package doc for
// why (the CurrentWorkspace/cwd discrepancy).
func sessionInfoFromMeta(meta sessionstore.SessionMeta) protocol.SessionInfo {
	return protocol.SessionInfo{
		SessionID: protocol.SessionID(meta.SessionID.String()),
		Cwd:       "",
		Title:     optionalString(meta.Title),
		UpdatedAt: optionalTimestamp(meta.LastActiveAt),
	}
}

// --- handleSessionList -----------------------------------------------------

// handleSessionList answers the session/list method. It is only ever
// registered when Options.Catalog is non-nil (see Register), matching
// capabilities.go's SessionCapabilities.List advertisement gate.
//
// ListSessionsRequest.Cwd (a working-directory filter) is deliberately never
// consulted: it cannot be meaningfully honored given the CurrentWorkspace/cwd
// discrepancy this file's package doc describes — SessionMeta carries no
// per-session working-directory value to filter on. Silently returning zero
// results for a filtered request would be worse than returning the catalog
// unfiltered (it would look like "no matching sessions" rather than "this
// filter is not currently supported"), so every known session is always
// listed regardless of any requested Cwd. This is flagged in this task's
// report as a consequence of the underlying gap, not a considered filtering
// design.
func (a *Agent) handleSessionList(ctx context.Context, _ string, params json.RawMessage) (any, error) {
	var req protocol.ListSessionsRequest
	if len(params) > 0 {
		if err := json.Unmarshal(params, &req); err != nil {
			return nil, protocol.InvalidParams("session/list: decode params", err)
		}
	}

	var after *SessionID
	if req.Cursor != nil {
		id, err := a.decodeListCursor(*req.Cursor)
		if err != nil {
			return nil, protocol.InvalidParams("session/list: cursor: "+err.Error(), err)
		}
		after = &id
	}

	metas, err := a.opts.Catalog.ListSessions(ctx)
	if err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("session/list: "+err.Error(), err)
	}

	sorted := sortedSessionMetas(metas)
	page, next := paginateSessionMetas(sorted, after)

	infos := make([]protocol.SessionInfo, 0, len(page))
	for _, m := range page {
		infos = append(infos, sessionInfoFromMeta(m))
	}

	resp := protocol.ListSessionsResponse{Sessions: infos}
	if next != nil {
		cur, err := a.encodeListCursor(*next)
		if err != nil {
			return nil, protocol.InternalError("session/list: encode cursor: "+err.Error(), err)
		}
		resp.NextCursor = &cur
	}
	return resp, nil
}

// --- session_info_update: the product observation callback ------------------

// ErrAgentNotRegistered reports that ObserveSessionMeta was called before
// Register bound the facade to a live *protocol.Conn. There is no client to
// notify yet, so this fails closed rather than silently dropping the
// observation.
var ErrAgentNotRegistered = errors.New("agent: Register has not been called yet")

// SessionMetaObservationError reports that ObserveSessionMeta was called
// with a SessionMeta this facade cannot act on.
type SessionMetaObservationError struct {
	Reason string
}

func (e *SessionMetaObservationError) Error() string {
	return "agent: cannot observe session meta: " + e.Reason
}

// observedSessionInfo is the last (title, lastActiveAt) pair
// ObserveSessionMeta has sent to the client for one session id — the dedup
// baseline that keeps session_info_update emission strictly proportional to
// actual change, not to how often a product happens to call in.
type observedSessionInfo struct {
	title        string
	lastActiveAt time.Time
}

// ObserveSessionMeta is the narrow callback surface a SessionCatalog-owning
// product calls into whenever its OWN catalog-observation mechanism (a KV
// watch, an event-driven fold, or any other push-based signal — never a poll
// loop this facade runs) sees a session's catalog entry. It is the "product
// observation callback" this task's design calls for (see this file's
// package doc for why it lives here rather than as a new host.go interface).
//
// It compares meta's Title and LastActiveAt against the last values this
// method sent for meta.SessionID (an implicit zero baseline — empty title,
// zero time — for a session never observed before) and sends exactly one
// session_info_update over session/update if, and only if, at least one
// differs; the baseline is then updated to match. A product that calls this
// once per catalog write — even for a session whose title/activity did not
// actually change, or on its very first, still-uninitialized entry — never
// produces needless client-visible notification traffic as a result: this is
// what makes emission event-driven (proportional to actual catalog change)
// rather than "notify on every call."
//
// meta.SessionID must be non-zero (fails closed with a typed
// *SessionMetaObservationError otherwise); Register must have already run
// (fails closed with ErrAgentNotRegistered otherwise, since there is no
// client connection yet to notify).
func (a *Agent) ObserveSessionMeta(ctx context.Context, meta sessionstore.SessionMeta) error {
	if a.client == nil {
		return ErrAgentNotRegistered
	}
	if meta.SessionID.IsZero() {
		return &SessionMetaObservationError{Reason: "empty session id"}
	}

	next := observedSessionInfo{title: meta.Title, lastActiveAt: meta.LastActiveAt}

	a.sessionInfoMu.Lock()
	var baseline observedSessionInfo
	if prev, seen := a.sessionInfoObserved[meta.SessionID]; seen {
		baseline = prev
	}
	changed := next != baseline
	if changed {
		a.sessionInfoObserved[meta.SessionID] = next
	}
	a.sessionInfoMu.Unlock()

	if !changed {
		return nil
	}

	notification := protocol.SessionNotification{
		SessionID: protocol.SessionID(meta.SessionID.String()),
		Update: protocol.SessionUpdate{SessionInfoUpdate: &protocol.SessionInfoUpdate{
			Title:     optionalString(meta.Title),
			UpdatedAt: optionalTimestamp(meta.LastActiveAt),
		}},
	}
	return a.client.SessionUpdate(ctx, notification)
}
