package agent_test

// list_test.go tests session/list and the session_info_update observation
// callback: Task 3.3 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
//
// This file lives in package agent_test (black-box), matching
// capabilities_test.go/setup_test.go: every assertion here goes through
// exported surface (protocol.NewAgentConn(client).ListSessions,
// agent.Agent.ObserveSessionMeta) — none of it needs the white-box access
// resume_test.go/replay_test.go's "package agent" tests use.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	coreuuid "github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/sessionstore"
)

// --- listCatalogStub: a fixed, scriptable agent.SessionCatalog fake --------

type listCatalogStub struct {
	metas []sessionstore.SessionMeta
	err   error
}

func (s *listCatalogStub) ListSessions(context.Context) ([]sessionstore.SessionMeta, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.metas, nil
}

// listSessionID builds a deterministic, sortable UUID string for test
// fixtures: "00000000-0000-4000-8000-<12-digit i>". Zero-padded decimal
// keeps lexicographic (byte) order identical to numeric order of i, which is
// exactly the sorted key session/list paginates over.
func listSessionID(t *testing.T, i int) coreuuid.UUID {
	t.Helper()
	return coreuuid.MustParse(fmt.Sprintf("00000000-0000-4000-8000-%012d", i))
}

// listAgentSetup wires a fresh Agent (fakeHost from capabilities_test.go plus
// the given catalog) behind a registered pipe connection.
func listAgentSetup(t *testing.T, catalog agent.SessionCatalog) (a *agent.Agent, client *protocol.Conn) {
	t.Helper()
	opts := baseOptions()
	opts.Catalog = catalog
	var err error
	a, err = agent.New(opts)
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	var server *protocol.Conn
	client, server = pipeConns(t)
	a.Register(server)
	return a, client
}

// collectSessionUpdates registers a session/update notification handler on
// client, delivering each decoded notification on the returned channel.
// Mirrors replay_test.go's helper of the same name, duplicated here because
// that one lives in package agent (unreachable from this black-box file).
func collectSessionUpdates(t *testing.T, client *protocol.Conn) <-chan protocol.SessionNotification {
	t.Helper()
	ch := make(chan protocol.SessionNotification, 512)
	client.HandleNotify(string(protocol.MethodSessionUpdate), func(_ context.Context, _ string, params json.RawMessage) {
		var n protocol.SessionNotification
		if err := json.Unmarshal(params, &n); err != nil {
			t.Errorf("unmarshal session/update notification: %v", err)
			return
		}
		ch <- n
	})
	return ch
}

// --- mapping: SessionMeta -> SessionInfo -----------------------------------

// TestHandleSessionListMapsCatalogEntry asserts the {sessionId, cwd, title,
// updatedAt} mapping this task's adapter contract calls for, including the
// deliberate cwd="" (see this task's report: sessionstore.SessionMeta carries
// no field representing a session's live working-directory path;
// CurrentWorkspace is a content-addressed workspace-SNAPSHOT pointer, not a
// directory string).
func TestHandleSessionListMapsCatalogEntry(t *testing.T) {
	id := listSessionID(t, 1)
	lastActive := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	catalog := &listCatalogStub{metas: []sessionstore.SessionMeta{
		{SessionID: id, Title: "Fix the flaky test", LastActiveAt: lastActive},
	}}
	_, client := listAgentSetup(t, catalog)
	agentConn := protocol.NewAgentConn(client)

	resp, err := agentConn.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(resp.Sessions) != 1 {
		t.Fatalf("len(Sessions) = %d, want 1", len(resp.Sessions))
	}
	got := resp.Sessions[0]
	if got.SessionID != protocol.SessionID(id.String()) {
		t.Errorf("SessionID = %q, want %q", got.SessionID, id.String())
	}
	if got.Cwd != "" {
		t.Errorf("Cwd = %q, want \"\" (no field on SessionMeta represents a working directory)", got.Cwd)
	}
	if got.Title == nil || *got.Title != "Fix the flaky test" {
		t.Errorf("Title = %v, want \"Fix the flaky test\"", got.Title)
	}
	wantUpdatedAt := lastActive.Format(time.RFC3339)
	if got.UpdatedAt == nil || *got.UpdatedAt != wantUpdatedAt {
		t.Errorf("UpdatedAt = %v, want %q", got.UpdatedAt, wantUpdatedAt)
	}
	if resp.NextCursor != nil {
		t.Errorf("NextCursor = %v, want nil (single entry fits in one page)", resp.NextCursor)
	}
}

// TestHandleSessionListOmitsEmptyOptionalFields asserts a session with no
// title yet and a zero LastActiveAt reports both Title and UpdatedAt as nil,
// not empty-string pointers.
func TestHandleSessionListOmitsEmptyOptionalFields(t *testing.T) {
	id := listSessionID(t, 1)
	catalog := &listCatalogStub{metas: []sessionstore.SessionMeta{{SessionID: id}}}
	_, client := listAgentSetup(t, catalog)
	agentConn := protocol.NewAgentConn(client)

	resp, err := agentConn.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(resp.Sessions) != 1 {
		t.Fatalf("len(Sessions) = %d, want 1", len(resp.Sessions))
	}
	if resp.Sessions[0].Title != nil {
		t.Errorf("Title = %v, want nil", resp.Sessions[0].Title)
	}
	if resp.Sessions[0].UpdatedAt != nil {
		t.Errorf("UpdatedAt = %v, want nil", resp.Sessions[0].UpdatedAt)
	}
}

// --- pagination -------------------------------------------------------------

// buildFixedCatalog returns n deterministic SessionMeta entries with
// ascending-sorted SessionIDs (listSessionID), each carrying a distinct title
// so a test can identify exactly which entries landed on which page.
func buildFixedCatalog(t *testing.T, n int) []sessionstore.SessionMeta {
	t.Helper()
	metas := make([]sessionstore.SessionMeta, n)
	for i := 0; i < n; i++ {
		metas[i] = sessionstore.SessionMeta{SessionID: listSessionID(t, i), Title: fmt.Sprintf("session-%d", i)}
	}
	return metas
}

// TestHandleSessionListPaginationStableNoDuplicatesNoGaps walks a fixed
// 250-entry catalog to exhaustion via nextCursor and asserts: every page is
// bounded at MaxPageSize, the concatenation of all pages is exactly the
// sorted-by-SessionID original set with no duplicate and no gap, and the
// final page's nextCursor is nil.
func TestHandleSessionListPaginationStableNoDuplicatesNoGaps(t *testing.T) {
	const total = 250
	catalog := &listCatalogStub{metas: buildFixedCatalog(t, total)}
	_, client := listAgentSetup(t, catalog)
	agentConn := protocol.NewAgentConn(client)

	var allIDs []string
	var cursor *string
	pages := 0
	for {
		req := protocol.ListSessionsRequest{Cursor: cursor}
		resp, err := agentConn.ListSessions(context.Background(), req)
		if err != nil {
			t.Fatalf("ListSessions (page %d): %v", pages, err)
		}
		if len(resp.Sessions) > agent.MaxPageSize {
			t.Fatalf("page %d: len(Sessions) = %d, want <= MaxPageSize (%d)", pages, len(resp.Sessions), agent.MaxPageSize)
		}
		for _, s := range resp.Sessions {
			allIDs = append(allIDs, string(s.SessionID))
		}
		pages++
		if resp.NextCursor == nil {
			break
		}
		cursor = resp.NextCursor
		if pages > total { // guard against a runaway loop on a broken implementation
			t.Fatalf("pagination did not terminate after %d pages", pages)
		}
	}

	if len(allIDs) != total {
		t.Fatalf("total sessions observed across all pages = %d, want %d", len(allIDs), total)
	}
	seen := make(map[string]bool, total)
	for i, id := range allIDs {
		if seen[id] {
			t.Fatalf("duplicate SessionID %q observed at position %d", id, i)
		}
		seen[id] = true
		wantID := listSessionID(t, i).String()
		if id != wantID {
			t.Fatalf("position %d: SessionID = %q, want %q (gap or reorder)", i, id, wantID)
		}
	}
	wantPages := (total + agent.MaxPageSize - 1) / agent.MaxPageSize
	if pages != wantPages {
		t.Errorf("pages = %d, want %d", pages, wantPages)
	}
}

// TestHandleSessionListPageSizeBoundedAtMaxPageSize asserts the very first
// page of a catalog larger than MaxPageSize is exactly MaxPageSize entries,
// never more.
func TestHandleSessionListPageSizeBoundedAtMaxPageSize(t *testing.T) {
	catalog := &listCatalogStub{metas: buildFixedCatalog(t, agent.MaxPageSize+37)}
	_, client := listAgentSetup(t, catalog)
	agentConn := protocol.NewAgentConn(client)

	resp, err := agentConn.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(resp.Sessions) != agent.MaxPageSize {
		t.Fatalf("len(Sessions) = %d, want exactly MaxPageSize (%d)", len(resp.Sessions), agent.MaxPageSize)
	}
	if resp.NextCursor == nil {
		t.Fatal("NextCursor = nil, want non-nil (more entries remain)")
	}
}

// TestHandleSessionListSmallCatalogHasNoNextCursor asserts a catalog smaller
// than MaxPageSize reports every entry on one page with no nextCursor.
func TestHandleSessionListSmallCatalogHasNoNextCursor(t *testing.T) {
	catalog := &listCatalogStub{metas: buildFixedCatalog(t, 5)}
	_, client := listAgentSetup(t, catalog)
	agentConn := protocol.NewAgentConn(client)

	resp, err := agentConn.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(resp.Sessions) != 5 {
		t.Fatalf("len(Sessions) = %d, want 5", len(resp.Sessions))
	}
	if resp.NextCursor != nil {
		t.Errorf("NextCursor = %v, want nil", resp.NextCursor)
	}
}

// --- cursor validation: tamper and malformed input --------------------------

// TestHandleSessionListTamperedCursorRejected obtains a real nextCursor from
// a multi-page catalog, mutates one of its bytes, and asserts the mutated
// cursor is rejected with an InvalidParams fault — never silently treated as
// "start from the beginning" or accepted as some other valid position. The
// server-side typed *agent.InvalidCursorError this goes through internally
// is asserted directly in list_internal_test.go (package agent): per this
// module's wire-exposure trust boundary (protocol.Fault.Unwrap's doc,
// ToWireError), an internal cause never crosses the wire, so a client-side
// errors.As to it here would be asserting on something the design
// deliberately never sends.
func TestHandleSessionListTamperedCursorRejected(t *testing.T) {
	catalog := &listCatalogStub{metas: buildFixedCatalog(t, agent.MaxPageSize+10)}
	_, client := listAgentSetup(t, catalog)
	agentConn := protocol.NewAgentConn(client)

	resp, err := agentConn.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions (first page): %v", err)
	}
	if resp.NextCursor == nil {
		t.Fatal("NextCursor = nil, want a real cursor to tamper with")
	}
	original := *resp.NextCursor
	if len(original) == 0 {
		t.Fatal("cursor is empty, cannot tamper")
	}

	tampered := tamperLastByte(t, original)
	if tampered == original {
		t.Fatal("tamperLastByte produced no change")
	}

	_, err = agentConn.ListSessions(context.Background(), protocol.ListSessionsRequest{Cursor: &tampered})
	if err == nil {
		t.Fatal("ListSessions(tampered cursor): error = nil, want rejection")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
	}
}

// tamperLastByte flips the cursor's final character to a different valid
// base64url character, mutating the token's bytes while keeping it
// structurally decodable (so the test exercises the HMAC/content check, not
// merely a decode failure).
func tamperLastByte(t *testing.T, s string) string {
	t.Helper()
	runes := []rune(s)
	last := len(runes) - 1
	replacement := 'A'
	if runes[last] == 'A' {
		replacement = 'B'
	}
	runes[last] = replacement
	return string(runes)
}

// TestHandleSessionListMalformedCursorRejected asserts a structurally
// malformed cursor (not the "<payload>.<tag>" shape at all) is rejected the
// same way. The typed error's exact Reason (Malformed) is asserted directly
// in list_internal_test.go (package agent) — see
// TestHandleSessionListTamperedCursorRejected's doc for why that check
// cannot be made through a wire round trip.
func TestHandleSessionListMalformedCursorRejected(t *testing.T) {
	catalog := &listCatalogStub{metas: buildFixedCatalog(t, 5)}
	_, client := listAgentSetup(t, catalog)
	agentConn := protocol.NewAgentConn(client)

	garbage := "not-a-real-cursor-at-all"
	_, err := agentConn.ListSessions(context.Background(), protocol.ListSessionsRequest{Cursor: &garbage})
	if err == nil {
		t.Fatal("ListSessions(malformed cursor): error = nil, want rejection")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
	}
}

// TestHandleSessionListEmptyCursorStringRejected asserts an explicitly
// empty (but present) cursor string is rejected rather than silently
// treated as "no cursor" — a client that sends "" is not the same as a
// client that omits the field.
func TestHandleSessionListEmptyCursorStringRejected(t *testing.T) {
	catalog := &listCatalogStub{metas: buildFixedCatalog(t, 5)}
	_, client := listAgentSetup(t, catalog)
	agentConn := protocol.NewAgentConn(client)

	empty := ""
	_, err := agentConn.ListSessions(context.Background(), protocol.ListSessionsRequest{Cursor: &empty})
	if err == nil {
		t.Fatal("ListSessions(empty cursor string): error = nil, want rejection")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
	}
}

// --- catalog error passthrough ----------------------------------------------

// TestHandleSessionListCatalogTypedFaultPassesThrough asserts a
// *protocol.Fault returned by Catalog.ListSessions passes through unchanged,
// matching every other host-boundary handler's rule.
func TestHandleSessionListCatalogTypedFaultPassesThrough(t *testing.T) {
	wantFault := protocol.InternalError("catalog: backend unavailable", nil)
	catalog := &listCatalogStub{err: wantFault}
	_, client := listAgentSetup(t, catalog)
	agentConn := protocol.NewAgentConn(client)

	_, err := agentConn.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err == nil {
		t.Fatal("ListSessions: error = nil, want the catalog's Fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInternalError {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
	}
}

// TestHandleSessionListCatalogPlainErrorWrapped asserts a plain (non-Fault)
// error from Catalog.ListSessions is wrapped as InternalError, not passed
// through raw or dropped.
func TestHandleSessionListCatalogPlainErrorWrapped(t *testing.T) {
	catalog := &listCatalogStub{err: errors.New("boom")}
	_, client := listAgentSetup(t, catalog)
	agentConn := protocol.NewAgentConn(client)

	_, err := agentConn.ListSessions(context.Background(), protocol.ListSessionsRequest{})
	if err == nil {
		t.Fatal("ListSessions: error = nil, want InternalError fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInternalError {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
	}
}

// ============================================================================
// session_info_update: the product observation callback (ObserveSessionMeta)
// ============================================================================

// TestObserveSessionMetaEmitsUpdateOnMeaningfulFirstObservation asserts a
// session never seen before, whose meta carries a real title/activity, does
// produce exactly one session_info_update.
func TestObserveSessionMetaEmitsUpdateOnMeaningfulFirstObservation(t *testing.T) {
	a, client := listAgentSetup(t, &listCatalogStub{})
	updates := collectSessionUpdates(t, client)

	id := listSessionID(t, 1)
	lastActive := time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)
	err := a.ObserveSessionMeta(context.Background(), sessionstore.SessionMeta{
		SessionID: id, Title: "Investigate flaky test", LastActiveAt: lastActive,
	})
	if err != nil {
		t.Fatalf("ObserveSessionMeta: %v", err)
	}

	got := drainOneSessionUpdate(t, updates)
	if got.SessionID != protocol.SessionID(id.String()) {
		t.Errorf("SessionID = %q, want %q", got.SessionID, id.String())
	}
	if got.Update.SessionInfoUpdate == nil {
		t.Fatalf("Update.SessionInfoUpdate = nil, want non-nil")
	}
	if title := got.Update.SessionInfoUpdate.Title; title == nil || *title != "Investigate flaky test" {
		t.Errorf("SessionInfoUpdate.Title = %v, want \"Investigate flaky test\"", title)
	}
	assertNoMoreUpdates(t, updates)
}

// TestObserveSessionMetaNoOpWhenNothingMeaningfulEverObserved asserts a
// session whose FIRST observation carries no title and a zero LastActiveAt
// (nothing meaningful relative to the zero baseline) produces zero
// notifications — not a spurious empty session_info_update.
func TestObserveSessionMetaNoOpWhenNothingMeaningfulEverObserved(t *testing.T) {
	a, client := listAgentSetup(t, &listCatalogStub{})
	updates := collectSessionUpdates(t, client)

	err := a.ObserveSessionMeta(context.Background(), sessionstore.SessionMeta{SessionID: listSessionID(t, 1)})
	if err != nil {
		t.Fatalf("ObserveSessionMeta: %v", err)
	}
	assertNoMoreUpdates(t, updates)
}

// TestObserveSessionMetaDedupesIdenticalObservation asserts calling
// ObserveSessionMeta twice with an UNCHANGED title/activity produces exactly
// ONE notification (from the first call), proving the facade compares
// against its own last-sent state rather than re-sending on every call — the
// dedup that keeps this event-driven rather than "notify on every observation
// regardless of content."
func TestObserveSessionMetaDedupesIdenticalObservation(t *testing.T) {
	a, client := listAgentSetup(t, &listCatalogStub{})
	updates := collectSessionUpdates(t, client)

	meta := sessionstore.SessionMeta{
		SessionID: listSessionID(t, 1), Title: "Same title",
		LastActiveAt: time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC),
	}
	if err := a.ObserveSessionMeta(context.Background(), meta); err != nil {
		t.Fatalf("ObserveSessionMeta (1st): %v", err)
	}
	if err := a.ObserveSessionMeta(context.Background(), meta); err != nil {
		t.Fatalf("ObserveSessionMeta (2nd, unchanged): %v", err)
	}

	drainOneSessionUpdate(t, updates)
	assertNoMoreUpdates(t, updates)
}

// TestObserveSessionMetaEmitsOnTitleChange asserts a second observation whose
// ONLY difference is Title produces a second notification carrying the new
// title.
func TestObserveSessionMetaEmitsOnTitleChange(t *testing.T) {
	a, client := listAgentSetup(t, &listCatalogStub{})
	updates := collectSessionUpdates(t, client)

	id := listSessionID(t, 1)
	activeAt := time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)
	if err := a.ObserveSessionMeta(context.Background(), sessionstore.SessionMeta{
		SessionID: id, Title: "Original title", LastActiveAt: activeAt,
	}); err != nil {
		t.Fatalf("ObserveSessionMeta (1st): %v", err)
	}
	drainOneSessionUpdate(t, updates)

	if err := a.ObserveSessionMeta(context.Background(), sessionstore.SessionMeta{
		SessionID: id, Title: "Renamed title", LastActiveAt: activeAt,
	}); err != nil {
		t.Fatalf("ObserveSessionMeta (2nd, title changed): %v", err)
	}
	second := drainOneSessionUpdate(t, updates)
	if title := second.Update.SessionInfoUpdate.Title; title == nil || *title != "Renamed title" {
		t.Errorf("2nd update Title = %v, want \"Renamed title\"", title)
	}
	assertNoMoreUpdates(t, updates)
}

// TestObserveSessionMetaEmitsOnActivityChange asserts a second observation
// whose ONLY difference is LastActiveAt produces a second notification.
func TestObserveSessionMetaEmitsOnActivityChange(t *testing.T) {
	a, client := listAgentSetup(t, &listCatalogStub{})
	updates := collectSessionUpdates(t, client)

	id := listSessionID(t, 1)
	first := time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)
	second := time.Date(2026, 7, 24, 10, 30, 0, 0, time.UTC)
	if err := a.ObserveSessionMeta(context.Background(), sessionstore.SessionMeta{
		SessionID: id, Title: "Same title", LastActiveAt: first,
	}); err != nil {
		t.Fatalf("ObserveSessionMeta (1st): %v", err)
	}
	drainOneSessionUpdate(t, updates)

	if err := a.ObserveSessionMeta(context.Background(), sessionstore.SessionMeta{
		SessionID: id, Title: "Same title", LastActiveAt: second,
	}); err != nil {
		t.Fatalf("ObserveSessionMeta (2nd, activity changed): %v", err)
	}
	got := drainOneSessionUpdate(t, updates)
	wantUpdatedAt := second.Format(time.RFC3339)
	if updatedAt := got.Update.SessionInfoUpdate.UpdatedAt; updatedAt == nil || *updatedAt != wantUpdatedAt {
		t.Errorf("2nd update UpdatedAt = %v, want %q", updatedAt, wantUpdatedAt)
	}
	assertNoMoreUpdates(t, updates)
}

// TestObserveSessionMetaRejectsZeroSessionID asserts a zero SessionID is
// rejected with a typed error and produces no notification.
func TestObserveSessionMetaRejectsZeroSessionID(t *testing.T) {
	a, client := listAgentSetup(t, &listCatalogStub{})
	updates := collectSessionUpdates(t, client)

	err := a.ObserveSessionMeta(context.Background(), sessionstore.SessionMeta{Title: "no id"})
	if err == nil {
		t.Fatal("ObserveSessionMeta(zero SessionID): error = nil, want rejection")
	}
	assertNoMoreUpdates(t, updates)
}

// TestObserveSessionMetaRequiresRegister asserts ObserveSessionMeta fails
// closed with ErrAgentNotRegistered when called before Register has bound
// the facade to a connection (there is no client to notify).
func TestObserveSessionMetaRequiresRegister(t *testing.T) {
	opts := baseOptions()
	opts.Catalog = &listCatalogStub{}
	a, err := agent.New(opts)
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}

	err = a.ObserveSessionMeta(context.Background(), sessionstore.SessionMeta{
		SessionID: listSessionID(t, 1), Title: "unregistered",
	})
	if !errors.Is(err, agent.ErrAgentNotRegistered) {
		t.Fatalf("ObserveSessionMeta before Register: err = %v, want ErrAgentNotRegistered", err)
	}
}

// TestObserveSessionMetaIsEventDrivenNotPolling proves the mechanism is
// driven exclusively by ObserveSessionMeta calls: after one call produces its
// one notification, waiting with nothing further calling ObserveSessionMeta
// must never produce a second one — ruling out a hidden background poll loop
// re-checking (and re-emitting for) the catalog on its own.
func TestObserveSessionMetaIsEventDrivenNotPolling(t *testing.T) {
	a, client := listAgentSetup(t, &listCatalogStub{})
	updates := collectSessionUpdates(t, client)

	if err := a.ObserveSessionMeta(context.Background(), sessionstore.SessionMeta{
		SessionID: listSessionID(t, 1), Title: "one shot", LastActiveAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("ObserveSessionMeta: %v", err)
	}
	drainOneSessionUpdate(t, updates)

	select {
	case n := <-updates:
		t.Fatalf("unexpected additional session/update with no further ObserveSessionMeta call: %+v", n)
	case <-time.After(300 * time.Millisecond):
	}
}

// drainOneSessionUpdate reads exactly one notification from ch, bounded, or
// fails the test.
func drainOneSessionUpdate(t *testing.T, ch <-chan protocol.SessionNotification) protocol.SessionNotification {
	t.Helper()
	select {
	case n := <-ch:
		return n
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a session/update notification")
	}
	panic("unreachable")
}

// assertNoMoreUpdates asserts no further notification arrives within a
// generous bound.
func assertNoMoreUpdates(t *testing.T, ch <-chan protocol.SessionNotification) {
	t.Helper()
	select {
	case n := <-ch:
		t.Fatalf("unexpected extra session/update notification: %+v", n)
	case <-time.After(200 * time.Millisecond):
	}
}
