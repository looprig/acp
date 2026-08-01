package client

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

func chunkText(text string) protocol.SessionUpdate {
	return protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{Text: &protocol.TextContent{Text: text}}}}
}

// TestBufferedEarlyUpdatesAreNotDropped proves that session/update
// notifications delivered before a consumer ever reads from Updates() are
// still all eventually delivered, none lost — the "buffered-from-subscribe"
// contract Session.Updates documents.
func TestBufferedEarlyUpdatesAreNotDropped(t *testing.T) {
	sess := newSession(nil, "sess-buffered")
	defer sess.closeUpdates()

	const n = 50
	for i := 0; i < n; i++ {
		sess.deliver(Update{SessionUpdate: chunkText(string(rune('a' + i%26)))})
	}

	got := 0
	timeout := time.After(2 * time.Second)
	for got < n {
		select {
		case <-sess.Updates():
			got++
		case <-timeout:
			t.Fatalf("received %d/%d buffered updates before timing out", got, n)
		}
	}
}

// TestUpdatesClosesAfterDrainingQueuedUpdates proves that closing a
// session's update stream still delivers everything already queued before
// the channel closes.
func TestUpdatesClosesAfterDrainingQueuedUpdates(t *testing.T) {
	sess := newSession(nil, "sess-drain")
	sess.deliver(Update{SessionUpdate: chunkText("one")})
	sess.deliver(Update{SessionUpdate: chunkText("two")})
	sess.closeUpdates()

	var got []string
	for u := range sess.Updates() {
		got = append(got, u.SessionUpdate.AgentMessageChunk.Content.Text.Text)
	}
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("drained updates = %v, want [one two]", got)
	}
}

func TestCloseAllSessionsReleasesUndrainedPump(t *testing.T) {
	assertNoGoroutineLeak(t)
	sess := newSession(nil, "sess-close-undrained")
	sess.deliver(Update{SessionUpdate: chunkText("queued")})

	deadline := time.Now().Add(2 * time.Second)
	for {
		sess.mu.Lock()
		inFlight := sess.inFlight
		sess.mu.Unlock()
		if inFlight == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the update pump to block on the undrained Updates channel")
		}
		time.Sleep(time.Millisecond)
	}

	c := New(stdio.Command{}, Options{})
	c.sessionsMu.Lock()
	c.sessions[sess.ID()] = sess
	c.sessionsMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Client.Close() error = %v", err)
	}
	select {
	case _, open := <-sess.Updates():
		if open {
			t.Fatal("Updates() delivered a queued update after Client.Close(); want the close to release the undrained pump")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Updates() to close after client close")
	}
}

// --- _meta.eventId dedup ---

func TestDedupDropsDuplicateLiveUpdatesByEventID(t *testing.T) {
	sess := newSession(nil, "sess-dedup")
	sess.deliver(Update{SessionUpdate: chunkText("first"), Meta: UpdateMeta{EventID: "ev-1"}})
	sess.deliver(Update{SessionUpdate: chunkText("duplicate"), Meta: UpdateMeta{EventID: "ev-1"}})
	sess.deliver(Update{SessionUpdate: chunkText("second"), Meta: UpdateMeta{EventID: "ev-2"}})
	sess.closeUpdates()

	var got []string
	for u := range sess.Updates() {
		got = append(got, u.SessionUpdate.AgentMessageChunk.Content.Text.Text)
	}
	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("delivered updates = %v, want [first second] (duplicate ev-1 must be dropped)", got)
	}
}

// TestDedupExemptsReplayUpdates proves replay updates are never deduped
// against each other, or against a live update sharing the same eventId:
// dedup is a live-stream-only concern.
func TestDedupExemptsReplayUpdates(t *testing.T) {
	sess := newSession(nil, "sess-dedup-replay")
	sess.deliver(Update{SessionUpdate: chunkText("replay-1"), Meta: UpdateMeta{EventID: "ev-1", IsReplay: true}})
	sess.deliver(Update{SessionUpdate: chunkText("replay-1-again"), Meta: UpdateMeta{EventID: "ev-1", IsReplay: true}})
	sess.deliver(Update{SessionUpdate: chunkText("live-1"), Meta: UpdateMeta{EventID: "ev-1", IsReplay: false}})
	sess.closeUpdates()

	var got []string
	for u := range sess.Updates() {
		got = append(got, u.SessionUpdate.AgentMessageChunk.Content.Text.Text)
	}
	want := []string{"replay-1", "replay-1-again", "live-1"}
	if len(got) != len(want) {
		t.Fatalf("delivered updates = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("delivered[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestDedupIgnoresUpdatesWithNoEventID proves an update with an empty
// eventId is never treated as a duplicate of anything (there is nothing to
// key a highwater check on).
func TestDedupIgnoresUpdatesWithNoEventID(t *testing.T) {
	sess := newSession(nil, "sess-dedup-empty")
	sess.deliver(Update{SessionUpdate: chunkText("a")})
	sess.deliver(Update{SessionUpdate: chunkText("b")})
	sess.closeUpdates()

	var got []string
	for u := range sess.Updates() {
		got = append(got, u.SessionUpdate.AgentMessageChunk.Content.Text.Text)
	}
	if len(got) != 2 {
		t.Fatalf("delivered updates = %v, want 2 (no eventId means never deduped)", got)
	}
}

// --- update queue bound (Task 5.1 follow-up) ---

// TestUpdateQueueExactlyAtCapacityDropsNothing proves the boundary: exactly
// UpdateQueueDepth queued-and-undrained updates must all survive, with zero
// drops counted.
func TestUpdateQueueExactlyAtCapacityDropsNothing(t *testing.T) {
	sess := newSession(nil, "sess-queue-at-cap")

	for i := 0; i < UpdateQueueDepth; i++ {
		sess.deliver(Update{SessionUpdate: chunkText(strconv.Itoa(i))})
	}
	sess.closeUpdates()

	var got []string
	for u := range sess.Updates() {
		got = append(got, u.SessionUpdate.AgentMessageChunk.Content.Text.Text)
	}
	if len(got) != UpdateQueueDepth {
		t.Fatalf("drained %d updates, want exactly UpdateQueueDepth=%d", len(got), UpdateQueueDepth)
	}
	for i, text := range got {
		if text != strconv.Itoa(i) {
			t.Fatalf("drained[%d] = %q, want %q (no drops expected at exactly capacity)", i, text, strconv.Itoa(i))
		}
	}
	if dropped := sess.DroppedUpdates(); dropped != 0 {
		t.Fatalf("DroppedUpdates() = %d, want 0 at exactly capacity", dropped)
	}
}

// assertTailPresent fails t unless every index in [from, to) is present in
// got, as a string set. Used to check the guaranteed-retained "recent tail"
// of a queue-overflow scenario without pinning the exact identity of the
// dropped range, which is legitimately racy at its boundary: pump (see
// Session.inFlight's doc) may or may not win the race to pull item 0 out of
// the queue into "in flight" before the cap logic would otherwise have
// evicted it. If it does, item 0 escapes eviction entirely and the drop
// window shifts by exactly one position (dropping [1, extra] instead of
// [0, extra-1]) since the queue itself then only has room for cap-1 more
// items. Either way exactly `extra` items are dropped and the true newest
// ones always survive — but only indices from extra+1 onward are
// guaranteed present in BOTH scenarios; index `extra` itself is part of the
// racy boundary (present only in the no-prefetch scenario).
func assertTailPresent(t *testing.T, got []string, from, to int) {
	t.Helper()
	set := make(map[string]bool, len(got))
	for _, v := range got {
		set[v] = true
	}
	for i := from; i < to; i++ {
		if !set[strconv.Itoa(i)] {
			t.Fatalf("index %d missing from delivered updates, want every index in [%d,%d) retained (the guaranteed recent tail)", i, from, to)
		}
	}
}

// TestUpdateQueueOneOverCapacityDropsOldest proves the other side of the
// boundary: UpdateQueueDepth+1 queued-and-undrained updates drop exactly one
// update, always from the oldest end (index 0 or index 1 — see
// assertTailPresent's doc for why the single boundary survivor is racy but
// the aggregate is not), never the newest, and count exactly one drop.
func TestUpdateQueueOneOverCapacityDropsOldest(t *testing.T) {
	sess := newSession(nil, "sess-queue-over-cap-by-one")

	const n = UpdateQueueDepth + 1
	for i := 0; i < n; i++ {
		sess.deliver(Update{SessionUpdate: chunkText(strconv.Itoa(i))})
	}
	sess.closeUpdates()

	var got []string
	for u := range sess.Updates() {
		got = append(got, u.SessionUpdate.AgentMessageChunk.Content.Text.Text)
	}
	if len(got) != UpdateQueueDepth {
		t.Fatalf("drained %d updates, want exactly UpdateQueueDepth=%d", len(got), UpdateQueueDepth)
	}
	if dropped := sess.DroppedUpdates(); dropped != 1 {
		t.Fatalf("DroppedUpdates() = %d, want 1", dropped)
	}
	// Indices [2, n) must always survive regardless of the pump-prefetch
	// race (see assertTailPresent's doc): exactly one of index 0 or index 1
	// is the one dropped, everything from index 2 on is always retained.
	assertTailPresent(t, got, 2, n)
}

// TestUpdateQueueOverflowDropsOldestNotNewestAndCountsDrops mirrors
// protocol's TestConnBufferedNotificationsOverflowDropsOldest shape: a
// larger overflow (UpdateQueueDepth+20) still drops only the oldest 20,
// keeps the resident count capped, and the drop counter matches exactly.
func TestUpdateQueueOverflowDropsOldestNotNewestAndCountsDrops(t *testing.T) {
	sess := newSession(nil, "sess-queue-overflow")

	const extra = 20
	const n = UpdateQueueDepth + extra
	for i := 0; i < n; i++ {
		sess.deliver(Update{SessionUpdate: chunkText(strconv.Itoa(i))})
	}

	// Assert the internal queue plus whatever pump may be holding in
	// flight never exceeds UpdateQueueDepth resident updates — the precise
	// bound deliver enforces (see Session.inFlight's doc), not just that
	// draining eventually yields the right count.
	sess.mu.Lock()
	resident := len(sess.queue) + sess.inFlight
	sess.mu.Unlock()
	if resident != UpdateQueueDepth {
		t.Fatalf("resident count (queue+inFlight) = %d, want capped at UpdateQueueDepth=%d", resident, UpdateQueueDepth)
	}

	sess.closeUpdates()
	var got []string
	for u := range sess.Updates() {
		got = append(got, u.SessionUpdate.AgentMessageChunk.Content.Text.Text)
	}
	if len(got) != UpdateQueueDepth {
		t.Fatalf("drained %d updates, want exactly UpdateQueueDepth=%d", len(got), UpdateQueueDepth)
	}
	if dropped := sess.DroppedUpdates(); dropped != extra {
		t.Fatalf("DroppedUpdates() = %d, want %d", dropped, extra)
	}
	// Everything from index extra+1 through the newest must survive in
	// both possible drop-window placements (see assertTailPresent's doc);
	// index `extra` itself sits on the racy boundary and is deliberately
	// not checked here.
	assertTailPresent(t, got, extra+1, n)
}

// TestUpdateQueueActivelyDrainedSessionNeverDrops proves a session whose
// consumer keeps pace with delivery — the expected steady state, and the
// exact shape of Task 5.1's existing dedup/buffering tests — never sees a
// drop, even across total delivered volume that vastly exceeds
// UpdateQueueDepth. Producer and consumer are explicitly paced in lockstep
// (via receivedCh) rather than left as an unthrottled burst race: a tight
// producer loop can easily outrun even a consumer that WILL eventually
// drain everything, which would prove nothing about this specific
// "never drops while genuinely kept up with" claim. This is the regression
// guard: the bound must only bite a consumer that falls behind, never a
// normally-paced one.
func TestUpdateQueueActivelyDrainedSessionNeverDrops(t *testing.T) {
	sess := newSession(nil, "sess-queue-drained")

	const n = UpdateQueueDepth * 3
	receivedCh := make(chan string)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for u := range sess.Updates() {
			receivedCh <- u.SessionUpdate.AgentMessageChunk.Content.Text.Text
		}
	}()

	var got []string
	for i := 0; i < n; i++ {
		sess.deliver(Update{SessionUpdate: chunkText(strconv.Itoa(i))})
		select {
		case v := <-receivedCh:
			got = append(got, v)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting to receive update %d", i)
		}
	}
	sess.closeUpdates()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for drain to finish")
	}

	if len(got) != n {
		t.Fatalf("drained %d updates, want all %d (an actively-drained consumer must never lose any)", len(got), n)
	}
	for i, text := range got {
		if text != strconv.Itoa(i) {
			t.Fatalf("drained[%d] = %q, want %q", i, text, strconv.Itoa(i))
		}
	}
	if dropped := sess.DroppedUpdates(); dropped != 0 {
		t.Fatalf("DroppedUpdates() = %d, want 0 for an actively-drained session", dropped)
	}
}

// --- eventId dedup map bound (Task 5.1 follow-up) ---

// eventIDN produces a deterministic, distinct-looking eventId for index i.
// The real ids are random UUIDv4s (see harness/pkg/event/factory.go's
// uuid.New backing) with no ordering signal of their own; the dedup bound
// keys off DELIVERY order (see EventDedupWindowDepth's doc), not the string
// value, so a plain deterministic string is sufficient here.
func eventIDN(i int) string { return "ev-" + strconv.Itoa(i) }

// TestEventDedupMapExactlyAtCapacityRetainsAll proves the boundary: exactly
// EventDedupWindowDepth distinct live eventIds all get remembered, none
// evicted yet.
func TestEventDedupMapExactlyAtCapacityRetainsAll(t *testing.T) {
	sess := newSession(nil, "sess-dedup-at-cap")

	for i := 0; i < EventDedupWindowDepth; i++ {
		sess.deliver(Update{SessionUpdate: chunkText(eventIDN(i)), Meta: UpdateMeta{EventID: eventIDN(i)}})
	}

	sess.seenMu.Lock()
	seenLen := len(sess.seen)
	orderLen := len(sess.seenOrder)
	_, oldestStillPresent := sess.seen[eventIDN(0)]
	sess.seenMu.Unlock()

	if seenLen != EventDedupWindowDepth {
		t.Fatalf("len(seen) = %d, want exactly EventDedupWindowDepth=%d", seenLen, EventDedupWindowDepth)
	}
	if orderLen != EventDedupWindowDepth {
		t.Fatalf("len(seenOrder) = %d, want exactly EventDedupWindowDepth=%d", orderLen, EventDedupWindowDepth)
	}
	if !oldestStillPresent {
		t.Fatal("oldest eventId evicted before capacity was exceeded")
	}
	sess.closeUpdates()
	for range sess.Updates() {
	}
}

// TestEventDedupMapOneOverCapacityEvictsOldest proves the other side of the
// boundary: EventDedupWindowDepth+1 distinct ids keeps the map capped at
// EventDedupWindowDepth and evicts exactly the single oldest (index 0)
// entry, not an arbitrary one.
func TestEventDedupMapOneOverCapacityEvictsOldest(t *testing.T) {
	sess := newSession(nil, "sess-dedup-over-cap-by-one")

	for i := 0; i < EventDedupWindowDepth+1; i++ {
		sess.deliver(Update{SessionUpdate: chunkText(eventIDN(i)), Meta: UpdateMeta{EventID: eventIDN(i)}})
	}

	sess.seenMu.Lock()
	seenLen := len(sess.seen)
	_, oldestPresent := sess.seen[eventIDN(0)]
	_, secondOldestPresent := sess.seen[eventIDN(1)]
	_, newestPresent := sess.seen[eventIDN(EventDedupWindowDepth)]
	sess.seenMu.Unlock()

	if seenLen != EventDedupWindowDepth {
		t.Fatalf("len(seen) = %d, want capped at EventDedupWindowDepth=%d", seenLen, EventDedupWindowDepth)
	}
	if oldestPresent {
		t.Fatal("oldest eventId (index 0) still present, want evicted")
	}
	if !secondOldestPresent {
		t.Fatal("second-oldest eventId (index 1) evicted, want retained (only one entry should be evicted)")
	}
	if !newestPresent {
		t.Fatal("newest eventId evicted, want retained")
	}
	sess.closeUpdates()
	for range sess.Updates() {
	}
}

// TestEventDedupMapStaysBoundedAcrossManyMoreThanCapacity proves the map
// genuinely never grows past EventDedupWindowDepth, not merely off-by-one at
// the boundary: across many multiples of capacity, its length never exceeds
// the bound.
func TestEventDedupMapStaysBoundedAcrossManyMoreThanCapacity(t *testing.T) {
	sess := newSession(nil, "sess-dedup-many")

	const n = EventDedupWindowDepth * 10
	for i := 0; i < n; i++ {
		sess.deliver(Update{SessionUpdate: chunkText(eventIDN(i)), Meta: UpdateMeta{EventID: eventIDN(i)}})

		sess.seenMu.Lock()
		seenLen := len(sess.seen)
		orderLen := len(sess.seenOrder)
		sess.seenMu.Unlock()
		if seenLen > EventDedupWindowDepth {
			t.Fatalf("at i=%d: len(seen) = %d, exceeds EventDedupWindowDepth=%d", i, seenLen, EventDedupWindowDepth)
		}
		if orderLen > EventDedupWindowDepth {
			t.Fatalf("at i=%d: len(seenOrder) = %d, exceeds EventDedupWindowDepth=%d", i, orderLen, EventDedupWindowDepth)
		}
	}

	sess.closeUpdates()
	for range sess.Updates() {
	}
}

// TestEventDedupRecentDuplicateWithinWindowStillCaught proves the eviction
// strategy's core correctness claim: a duplicate of a RECENT id — one still
// inside the retained window — is caught exactly like Task 5.1's original
// TestDedupDropsDuplicateLiveUpdatesByEventID, unaffected by the map now
// being bounded.
func TestEventDedupRecentDuplicateWithinWindowStillCaught(t *testing.T) {
	sess := newSession(nil, "sess-dedup-recent-dup")

	for i := 0; i < EventDedupWindowDepth; i++ {
		sess.deliver(Update{SessionUpdate: chunkText(eventIDN(i)), Meta: UpdateMeta{EventID: eventIDN(i)}})
	}
	// Re-deliver the most recent id as a duplicate: still well within the
	// retained window, so it must be dropped.
	mostRecent := EventDedupWindowDepth - 1
	sess.deliver(Update{SessionUpdate: chunkText("duplicate-of-recent"), Meta: UpdateMeta{EventID: eventIDN(mostRecent)}})
	sess.closeUpdates()

	var got []string
	for u := range sess.Updates() {
		got = append(got, u.SessionUpdate.AgentMessageChunk.Content.Text.Text)
	}
	if len(got) != EventDedupWindowDepth {
		t.Fatalf("drained %d updates, want exactly EventDedupWindowDepth=%d (recent duplicate must be dropped)", len(got), EventDedupWindowDepth)
	}
	for _, text := range got {
		if text == "duplicate-of-recent" {
			t.Fatal("duplicate of a recent (in-window) eventId was delivered, want dropped")
		}
	}
}

// TestEventDedupEvictedIDNoLongerCaught documents the accepted tradeoff
// (see EventDedupWindowDepth's doc): once an id has aged out of the
// retained window, a later reappearance of that exact id is no longer
// recognized as a duplicate. This is deliberate bounded loss, not a bug —
// this test pins the documented behavior so a future change to the eviction
// strategy notices if it silently changes.
func TestEventDedupEvictedIDNoLongerCaught(t *testing.T) {
	sess := newSession(nil, "sess-dedup-evicted-dup")

	// Fill the window plus one: this evicts eventIDN(0).
	for i := 0; i < EventDedupWindowDepth+1; i++ {
		sess.deliver(Update{SessionUpdate: chunkText(eventIDN(i)), Meta: UpdateMeta{EventID: eventIDN(i)}})
	}
	// Re-deliver the now-evicted oldest id: outside the retained window, so
	// it is (by documented design) treated as new, not a duplicate.
	sess.deliver(Update{SessionUpdate: chunkText("replayed-evicted-id"), Meta: UpdateMeta{EventID: eventIDN(0)}})
	sess.closeUpdates()

	var got []string
	for u := range sess.Updates() {
		got = append(got, u.SessionUpdate.AgentMessageChunk.Content.Text.Text)
	}
	found := false
	for _, text := range got {
		if text == "replayed-evicted-id" {
			found = true
		}
	}
	if !found {
		t.Fatal("update keyed by an evicted eventId was dropped, want delivered (documented bounded-loss tradeoff)")
	}
}

// --- decodeUpdateMeta wire-shape fidelity ---

// TestDecodeUpdateMetaMatchesProducerWireShape decodes a hand-written _meta
// JSON literal shaped like acp/agent/translate.go's marshalUpdateMeta output,
// as a fast, dependency-free regression pin on this package's own decode
// logic. It does NOT call the real producer function, so it cannot by
// itself catch the two sides silently drifting — for that, see
// acp/agent/meta_roundtrip_test.go's TestMetaWireRoundTripAgentToClient,
// which calls the actual production marshalUpdateMeta (package agent) and
// feeds its raw output straight into this package's real DecodeUpdateMeta.
func TestDecodeUpdateMetaMatchesProducerWireShape(t *testing.T) {
	raw := []byte(`{"eventId":"55555555-5555-4555-8555-555555555555","promptId":"44444444-4444-4444-8444-444444444444"}`)
	got := DecodeUpdateMeta(raw)
	want := UpdateMeta{EventID: "55555555-5555-4555-8555-555555555555", PromptID: "44444444-4444-4444-8444-444444444444"}
	if got != want {
		t.Errorf("DecodeUpdateMeta() = %+v, want %+v", got, want)
	}
}

func TestDecodeUpdateMetaReplayShape(t *testing.T) {
	raw := []byte(`{"eventId":"11111111-1111-4111-8111-111111111111","isReplay":true}`)
	got := DecodeUpdateMeta(raw)
	if got.EventID != "11111111-1111-4111-8111-111111111111" || !got.IsReplay || got.PromptID != "" {
		t.Errorf("DecodeUpdateMeta() = %+v, want eventId set, isReplay true, promptId empty", got)
	}
}

func TestDecodeUpdateMetaDegradesOnAbsentOrMalformed(t *testing.T) {
	if got := DecodeUpdateMeta(nil); got != (UpdateMeta{}) {
		t.Errorf("DecodeUpdateMeta(nil) = %+v, want zero value", got)
	}
	if got := DecodeUpdateMeta([]byte(`not json`)); got != (UpdateMeta{}) {
		t.Errorf("DecodeUpdateMeta(malformed) = %+v, want zero value", got)
	}
}

// TestSessionUpdateNotificationRoutesThroughDedup drives the dedup path via
// the real handleSessionUpdateNotify dispatch entry point (not deliver
// directly), proving the wire-facing seam applies the same rule.
func TestSessionUpdateNotificationRoutesThroughDedup(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	sess := newSessionForTest(t, c, fa, "sess-wire-dedup")

	send := func(text, eventID string) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := fa.client.SessionUpdate(ctx, protocol.SessionNotification{
			SessionID: sess.ID(),
			Update:    chunkText(text),
			Meta:      []byte(`{"eventId":"` + eventID + `"}`),
		}); err != nil {
			t.Fatalf("SessionUpdate() error = %v", err)
		}
	}

	send("first", "ev-a")
	send("duplicate", "ev-a")
	send("second", "ev-b")

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case u := <-sess.Updates():
			got[u.SessionUpdate.AgentMessageChunk.Content.Text.Text] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for update %d/2", i+1)
		}
	}
	if !got["first"] || !got["second"] || got["duplicate"] {
		t.Errorf("got updates %v, want exactly {first, second}", got)
	}
}
