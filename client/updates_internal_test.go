package client

import (
	"context"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
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

// --- decodeUpdateMeta wire-shape fidelity ---

// TestDecodeUpdateMetaMatchesProducerWireShape decodes the exact _meta JSON
// acp/agent/translate.go's marshalUpdateMeta produces (hardcoded here, not
// imported — acp/client cannot import acp/agent, see acp/CLAUDE.md), proving
// this package's independently-owned UpdateMeta stays wire-compatible with
// the producer side's {eventId, promptId, isReplay} shape.
func TestDecodeUpdateMetaMatchesProducerWireShape(t *testing.T) {
	raw := []byte(`{"eventId":"55555555-5555-4555-8555-555555555555","promptId":"44444444-4444-4444-8444-444444444444"}`)
	got := decodeUpdateMeta(raw)
	want := UpdateMeta{EventID: "55555555-5555-4555-8555-555555555555", PromptID: "44444444-4444-4444-8444-444444444444"}
	if got != want {
		t.Errorf("decodeUpdateMeta() = %+v, want %+v", got, want)
	}
}

func TestDecodeUpdateMetaReplayShape(t *testing.T) {
	raw := []byte(`{"eventId":"11111111-1111-4111-8111-111111111111","isReplay":true}`)
	got := decodeUpdateMeta(raw)
	if got.EventID != "11111111-1111-4111-8111-111111111111" || !got.IsReplay || got.PromptID != "" {
		t.Errorf("decodeUpdateMeta() = %+v, want eventId set, isReplay true, promptId empty", got)
	}
}

func TestDecodeUpdateMetaDegradesOnAbsentOrMalformed(t *testing.T) {
	if got := decodeUpdateMeta(nil); got != (UpdateMeta{}) {
		t.Errorf("decodeUpdateMeta(nil) = %+v, want zero value", got)
	}
	if got := decodeUpdateMeta([]byte(`not json`)); got != (UpdateMeta{}) {
		t.Errorf("decodeUpdateMeta(malformed) = %+v, want zero value", got)
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
