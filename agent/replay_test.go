package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	coreuuid "github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
	"github.com/looprig/harness/pkg/identity"
	"github.com/looprig/harness/pkg/journal"
)

// --- fakeEventCursor: a scripted journal.EventCursor over a canned slice ---

// fakeEventCursor is the minimal journal.EventCursor every test in this file
// drives directly: Next walks a pre-built slice in order and reports io.EOF
// once exhausted, or a scripted error at a specific index (nextErrAt >= 0)
// instead of the event that would otherwise be at that position.
type fakeEventCursor struct {
	events    []event.Event
	i         int
	nextErrAt int // -1 disables; otherwise the index at which Next returns nextErr
	nextErr   error
}

func (c *fakeEventCursor) Next(context.Context) (event.Event, uint64, error) {
	if c.nextErrAt >= 0 && c.i == c.nextErrAt {
		return nil, 0, c.nextErr
	}
	if c.i >= len(c.events) {
		return nil, 0, io.EOF
	}
	ev := c.events[c.i]
	c.i++
	return ev, uint64(c.i), nil
}

func (c *fakeEventCursor) Close() error { return nil }

func newFakeEventCursor(events ...event.Event) *fakeEventCursor {
	return &fakeEventCursor{events: events, nextErrAt: -1}
}

// --- fixed, deterministic replay test coordinates --------------------------

var (
	replayTurn0ID = coreuuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	replayTurn1ID = coreuuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")

	replayTurnStartedEventID = coreuuid.MustParse("c0000000-0000-4000-8000-000000000001")
	replayStepAEventID       = coreuuid.MustParse("c0000000-0000-4000-8000-000000000002")
	replayStepBEventID       = coreuuid.MustParse("c0000000-0000-4000-8000-000000000003")
	replayContextEventID     = coreuuid.MustParse("c0000000-0000-4000-8000-000000000004")
	replayTurnDoneEventID    = coreuuid.MustParse("c0000000-0000-4000-8000-000000000005")
)

func replayHeader(turnID, eventID coreuuid.UUID, visibility event.EventVisibility) event.Header {
	return event.Header{
		Coordinates:     identity.Coordinates{SessionID: testSessionUUID, LoopID: testLoopID, TurnID: turnID},
		EventID:         eventID,
		EventVisibility: visibility,
	}
}

func textBlock(s string) *content.TextBlock { return &content.TextBlock{Text: s} }

func userMessage(text string) *content.UserMessage {
	return &content.UserMessage{Message: content.Message{Role: content.RoleUser, Blocks: []content.Block{textBlock(text)}}}
}

// --- golden test: exact ordered session/update JSON list -------------------

// TestBuildReplayNotificationsGoldenOrder pins the exact ordered
// session/update sequence session/load must produce for a canned, realistic
// turn: one user message, a two-step assistant response (thinking, then a
// tool call resolved mid-turn, then a final answer in a second step), and a
// context measurement — asserting the four-bucket grouping (user, then
// assistant, then tool calls, then metadata) called for by this task, not
// merely "some updates arrived".
func TestBuildReplayNotificationsGoldenOrder(t *testing.T) {
	events := []event.Event{
		event.TurnStarted{
			Header:    replayHeader(replayTurn0ID, replayTurnStartedEventID, event.Public),
			TurnIndex: 0,
			Message:   userMessage("Hello"),
		},
		event.StepDone{
			Header: replayHeader(replayTurn0ID, replayStepAEventID, event.Public),
			Messages: content.AgenticMessages{
				&content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{
					&content.ThinkingBlock{Thinking: "pondering"},
					textBlock("Let me check that file."),
					&content.ToolUseBlock{ID: "tu_1", Name: "read_file", Input: json.RawMessage(`{"path":"a.go"}`)},
				}}},
				&content.ToolResultMessage{
					Message:   content.Message{Role: content.RoleTool, Blocks: []content.Block{textBlock("file contents")}},
					ToolUseID: "tu_1",
				},
			},
		},
		event.StepDone{
			Header: replayHeader(replayTurn0ID, replayStepBEventID, event.Public),
			Messages: content.AgenticMessages{
				&content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{
					textBlock("Done, the file says X."),
				}}},
			},
		},
		event.ContextMeasured{
			Header:      replayHeader(replayTurn0ID, replayContextEventID, event.Public),
			Measurement: event.ContextMeasurement{InputTokens: 1234, InputLimit: 100000},
		},
		// TurnDone must produce NOTHING: its Message would duplicate the
		// StepDone-derived content already reconstructed above.
		event.TurnDone{
			Header:    replayHeader(replayTurn0ID, replayTurnDoneEventID, event.Public),
			TurnIndex: 0,
			Message:   &content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{textBlock("Done, the file says X.")}}},
		},
	}

	cur := newFakeEventCursor(events...)
	got, err := buildReplayNotifications(context.Background(), testWireSessionID, cur, 0)
	if err != nil {
		t.Fatalf("buildReplayNotifications: %v", err)
	}

	userMid := "msg:" + string(testWireSessionID) + ":" + testLoopID.String() + ":" + replayTurn0ID.String() + ":user"
	asstMid0 := "msg:" + string(testWireSessionID) + ":" + testLoopID.String() + ":" + replayTurn0ID.String() + ":0"
	asstMid1 := "msg:" + string(testWireSessionID) + ":" + testLoopID.String() + ":" + replayTurn0ID.String() + ":1"

	want := []string{
		// 1. user message
		`{"sessionId":"` + string(testWireSessionID) + `",` +
			`"update":{"content":{"text":"Hello","type":"text"},"messageId":"` + userMid + `","sessionUpdate":"user_message_chunk"},` +
			`"_meta":{"eventId":"` + replayTurnStartedEventID.String() + `","isReplay":true}}`,
		// 2. assistant thought (step A, seq 0)
		`{"sessionId":"` + string(testWireSessionID) + `",` +
			`"update":{"content":{"text":"pondering","type":"text"},"messageId":"` + asstMid0 + `","sessionUpdate":"agent_thought_chunk"},` +
			`"_meta":{"eventId":"` + replayStepAEventID.String() + `","isReplay":true}}`,
		// 3. assistant text (step A, kind switch -> seq 1)
		`{"sessionId":"` + string(testWireSessionID) + `",` +
			`"update":{"content":{"text":"Let me check that file.","type":"text"},"messageId":"` + asstMid1 + `","sessionUpdate":"agent_message_chunk"},` +
			`"_meta":{"eventId":"` + replayStepAEventID.String() + `","isReplay":true}}`,
		// 4. assistant text (step B, same kind as previous -> still seq 1)
		`{"sessionId":"` + string(testWireSessionID) + `",` +
			`"update":{"content":{"text":"Done, the file says X.","type":"text"},"messageId":"` + asstMid1 + `","sessionUpdate":"agent_message_chunk"},` +
			`"_meta":{"eventId":"` + replayStepBEventID.String() + `","isReplay":true}}`,
		// 5. completed tool call (resolved within step A)
		`{"sessionId":"` + string(testWireSessionID) + `",` +
			`"update":{"content":[{"content":{"text":"file contents","type":"text"},"type":"content"}],` +
			`"rawInput":{"path":"a.go"},"sessionUpdate":"tool_call","status":"completed","title":"read_file","toolCallId":"tu_1"},` +
			`"_meta":{"eventId":"` + replayStepAEventID.String() + `","isReplay":true}}`,
		// 6. current metadata: usage_update from the last ContextMeasured
		`{"sessionId":"` + string(testWireSessionID) + `",` +
			`"update":{"sessionUpdate":"usage_update","size":100000,"used":1234},` +
			`"_meta":{"eventId":"` + replayContextEventID.String() + `","isReplay":true}}`,
	}

	if len(got) != len(want) {
		t.Fatalf("len(notifications) = %d, want %d\ngot: %+v", len(got), len(want), got)
	}
	for i := range want {
		gotJSON := mustMarshal(t, got[i])
		if gotJSON != want[i] {
			t.Errorf("notification[%d] JSON =\n%s\nwant:\n%s", i, gotJSON, want[i])
		}
	}
}

// --- no-duplication property ------------------------------------------------

// TestBuildReplayNotificationsNoDuplication asserts the property this task
// requires in its own words: replay never emits an agent_message_chunk (or
// agent_thought_chunk) for content also delivered as a complete message.
// Since durable history carries no TokenDelta at all, the only way this
// could go wrong is if replay itself fragmented or re-emitted a block's full
// text — so this test pins that each distinct source block produces EXACTLY
// one chunk, no more, no less, and that content never reappears in a second
// notification.
func TestBuildReplayNotificationsNoDuplication(t *testing.T) {
	events := []event.Event{
		event.TurnStarted{Header: replayHeader(replayTurn0ID, replayTurnStartedEventID, event.Public), TurnIndex: 0, Message: userMessage("hi")},
		event.StepDone{
			Header: replayHeader(replayTurn0ID, replayStepAEventID, event.Public),
			Messages: content.AgenticMessages{
				&content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{
					&content.ThinkingBlock{Thinking: "thinking about it"},
					textBlock("the final answer"),
				}}},
			},
		},
	}

	cur := newFakeEventCursor(events...)
	got, err := buildReplayNotifications(context.Background(), testWireSessionID, cur, 0)
	if err != nil {
		t.Fatalf("buildReplayNotifications: %v", err)
	}

	var chunkTexts []string
	for _, n := range got {
		switch {
		case n.Update.AgentMessageChunk != nil:
			chunkTexts = append(chunkTexts, n.Update.AgentMessageChunk.Content.Text.Text)
		case n.Update.AgentThoughtChunk != nil:
			chunkTexts = append(chunkTexts, n.Update.AgentThoughtChunk.Content.Text.Text)
		}
	}

	wantTexts := []string{"thinking about it", "the final answer"}
	if len(chunkTexts) != len(wantTexts) {
		t.Fatalf("assistant/thought chunk count = %d (%v), want exactly %d (%v) — no fragmentation or duplication",
			len(chunkTexts), chunkTexts, len(wantTexts), wantTexts)
	}
	seen := map[string]int{}
	for _, text := range chunkTexts {
		seen[text]++
	}
	for _, want := range wantTexts {
		if seen[want] != 1 {
			t.Errorf("text %q appeared %d times, want exactly 1 (no duplication)", want, seen[want])
		}
	}
}

// TestBuildReplayNotificationsCarriesRefusalBlocks pins that a reloaded
// session still reports a declined turn. A RefusalBlock has no ACP content
// type, so it replays as an agent_message_chunk marked refusal in the chunk's
// own _meta; dropping it, as replay did before, left the client showing the
// turn as unanswered.
func TestBuildReplayNotificationsCarriesRefusalBlocks(t *testing.T) {
	events := []event.Event{
		event.TurnStarted{Header: replayHeader(replayTurn0ID, replayTurnStartedEventID, event.Public), TurnIndex: 0, Message: userMessage("do the thing")},
		event.StepDone{
			Header: replayHeader(replayTurn0ID, replayStepAEventID, event.Public),
			Messages: content.AgenticMessages{
				&content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{
					&content.RefusalBlock{Text: "I will not do that."},
				}}},
			},
		},
	}

	cur := newFakeEventCursor(events...)
	got, err := buildReplayNotifications(context.Background(), testWireSessionID, cur, 0)
	if err != nil {
		t.Fatalf("buildReplayNotifications: %v", err)
	}

	var refusals int
	for _, n := range got {
		chunk := n.Update.AgentMessageChunk
		if chunk == nil || chunk.Content.Text == nil || chunk.Content.Text.Text != "I will not do that." {
			continue
		}
		refusals++
		if string(chunk.Meta) != `{"refusal":true}` {
			t.Errorf("refusal chunk _meta = %s, want {\"refusal\":true}", chunk.Meta)
		}
	}
	if refusals != 1 {
		t.Fatalf("refusal chunks = %d, want exactly 1", refusals)
	}
}

// --- visibility defense in depth: a violating fake replayer ----------------

// TestBuildReplayNotificationsSkipsInternalEventsEvenFromAViolatingReplayer
// proves the re-check is real, not merely trusting the source: it feeds an
// event stamped EventVisibility: Internal in between two Public events —
// something the documented "public-only by construction"
// sessionstore.Store.OpenEventReplayer should never do, but a violating fake
// host implementation might — and asserts its content never reaches any
// emitted notification while the surrounding Public events are still
// processed normally (fail-safe skip, not an abort).
func TestBuildReplayNotificationsSkipsInternalEventsEvenFromAViolatingReplayer(t *testing.T) {
	const secretText = "INTERNAL-SECRET-SHOULD-NEVER-LEAK"
	const safeText = "safe text"

	events := []event.Event{
		event.TurnStarted{Header: replayHeader(replayTurn0ID, replayTurnStartedEventID, event.Public), TurnIndex: 0, Message: userMessage("hi")},
		// Violates the EventReplayer contract: this event is Internal, but a
		// misbehaving fake host hands it to the cursor anyway.
		event.StepDone{
			Header: replayHeader(replayTurn0ID, replayStepAEventID, event.Internal),
			Messages: content.AgenticMessages{
				&content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{textBlock(secretText)}}},
			},
		},
		event.StepDone{
			Header: replayHeader(replayTurn0ID, replayStepBEventID, event.Public),
			Messages: content.AgenticMessages{
				&content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{textBlock(safeText)}}},
			},
		},
		event.ContextMeasured{
			Header:      replayHeader(replayTurn0ID, replayContextEventID, event.Public),
			Measurement: event.ContextMeasurement{InputTokens: 1, InputLimit: 2},
		},
	}

	cur := newFakeEventCursor(events...)
	got, err := buildReplayNotifications(context.Background(), testWireSessionID, cur, 0)
	if err != nil {
		t.Fatalf("buildReplayNotifications: %v", err)
	}

	full, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("json.Marshal(got): %v", err)
	}
	if strings.Contains(string(full), secretText) {
		t.Fatalf("Internal event's content leaked into replayed output: %s", full)
	}
	if !strings.Contains(string(full), safeText) {
		t.Fatalf("surrounding Public event was NOT processed (fail-safe skip must continue the walk): %s", full)
	}
}

// --- ReplayedThrough boundary ------------------------------------------------

// TestBuildReplayNotificationsStopsAtReplayedThroughBoundary asserts a turn
// beyond LoadedSession.ReplayedThrough — and everything after it — is
// entirely excluded from the reconstruction, never partially represented.
func TestBuildReplayNotificationsStopsAtReplayedThroughBoundary(t *testing.T) {
	events := []event.Event{
		event.TurnStarted{Header: replayHeader(replayTurn0ID, replayTurnStartedEventID, event.Public), TurnIndex: 0, Message: userMessage("turn zero")},
		event.StepDone{
			Header: replayHeader(replayTurn0ID, replayStepAEventID, event.Public),
			Messages: content.AgenticMessages{
				&content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{textBlock("turn zero answer")}}},
			},
		},
		// Beyond the boundary (replayedThrough will be 0): must be fully excluded.
		event.TurnStarted{Header: replayHeader(replayTurn1ID, replayTurnDoneEventID, event.Public), TurnIndex: 1, Message: userMessage("turn one, out of scope")},
		event.StepDone{
			Header: replayHeader(replayTurn1ID, replayContextEventID, event.Public),
			Messages: content.AgenticMessages{
				&content.AIMessage{Message: content.Message{Role: content.RoleAssistant, Blocks: []content.Block{textBlock("turn one answer, out of scope")}}},
			},
		},
	}

	cur := newFakeEventCursor(events...)
	got, err := buildReplayNotifications(context.Background(), testWireSessionID, cur, 0)
	if err != nil {
		t.Fatalf("buildReplayNotifications: %v", err)
	}

	full, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("json.Marshal(got): %v", err)
	}
	if strings.Contains(string(full), "out of scope") {
		t.Fatalf("a turn beyond ReplayedThrough leaked into the reconstruction: %s", full)
	}
	if !strings.Contains(string(full), "turn zero") {
		t.Fatalf("the in-scope turn was not reconstructed: %s", full)
	}
}

// --- empty history and cursor error propagation -----------------------------

func TestBuildReplayNotificationsEmptyHistoryProducesNoNotifications(t *testing.T) {
	cur := newFakeEventCursor()
	got, err := buildReplayNotifications(context.Background(), testWireSessionID, cur, 0)
	if err != nil {
		t.Fatalf("buildReplayNotifications: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("len(notifications) = %d, want 0 for empty durable history", len(got))
	}
}

// TestBuildReplayNotificationsPropagatesNonEOFCursorError asserts a
// corrupt/unreadable durable record fails the whole reconstruction rather
// than silently truncating it into a partial replay that looks complete.
func TestBuildReplayNotificationsPropagatesNonEOFCursorError(t *testing.T) {
	wantErr := errors.New("boom: corrupt ledger record")
	cur := &fakeEventCursor{
		events:    []event.Event{event.TurnStarted{Header: replayHeader(replayTurn0ID, replayTurnStartedEventID, event.Public), TurnIndex: 0, Message: userMessage("hi")}},
		nextErrAt: 1,
		nextErr:   wantErr,
	}

	_, err := buildReplayNotifications(context.Background(), testWireSessionID, cur, 0)
	if !errors.Is(err, wantErr) {
		t.Fatalf("buildReplayNotifications error = %v, want %v", err, wantErr)
	}
}

// ============================================================================
// handleSessionLoad: handler-level tests
// ============================================================================

// replayLiveStub is a minimal LiveSession, optionally a SessionCloser, used
// by handleSessionLoad's tests.
type replayLiveStub struct {
	id coreuuid.UUID

	mu            sync.Mutex
	shutdownCalls int
}

func (s *replayLiveStub) SessionID() coreuuid.UUID { return s.id }
func (s *replayLiveStub) Submit(context.Context, []content.Block) (coreuuid.UUID, error) {
	return coreuuid.UUID{}, errors.New("replayLiveStub: Submit not implemented")
}
func (s *replayLiveStub) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errors.New("replayLiveStub: SubscribeEvents not implemented")
}
func (s *replayLiveStub) RespondGate(context.Context, gate.GateResponse) error {
	return errors.New("replayLiveStub: RespondGate not implemented")
}
func (s *replayLiveStub) Interrupt(context.Context) (bool, error) {
	return false, errors.New("replayLiveStub: Interrupt not implemented")
}
func (s *replayLiveStub) Shutdown(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdownCalls++
	return nil
}
func (s *replayLiveStub) shutdownCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shutdownCalls
}

func newReplayLiveStub(t *testing.T) *replayLiveStub {
	t.Helper()
	id, err := coreuuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	return &replayLiveStub{id: id}
}

// fakeJournalReplayer is the journal.EventReplayer a fakeReplayer.OpenEventReplayer
// hands back: each Open call builds a FRESH cursor over the same scripted
// events (matching the real sessionstore.eventReplayer's contract — "every
// Open builds an independent ledger cursor, so concurrent replays do not
// interfere", see harness/pkg/sessionstore/replay.go), or returns a scripted
// error.
type fakeJournalReplayer struct {
	events  []event.Event
	openErr error
}

func (r *fakeJournalReplayer) Open(context.Context, journal.ReplayRequest) (journal.EventCursor, error) {
	if r.openErr != nil {
		return nil, r.openErr
	}
	return newFakeEventCursor(r.events...), nil
}

func newFakeJournalReplayer(events ...event.Event) *fakeJournalReplayer {
	return &fakeJournalReplayer{events: events}
}

// fakeReplayer implements agent.EventReplayer (host.go), handing back a
// scripted journal.EventReplayer (or a scripted error from
// OpenEventReplayer itself).
type fakeReplayer struct {
	mu      sync.Mutex
	calls   int
	openErr error
	journal journal.EventReplayer
}

func (r *fakeReplayer) OpenEventReplayer(SessionID) (journal.EventReplayer, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	if r.openErr != nil {
		return nil, r.openErr
	}
	return r.journal, nil
}

// loadHostStub is a SessionHost whose LoadSession returns a scripted
// LoadedSession or error; NewSession/ResumeSession are not exercised by these
// tests.
type loadHostStub struct {
	mu      sync.Mutex
	calls   int
	loaded  LoadedSession
	loadErr error
}

func (h *loadHostStub) NewSession(context.Context, Setup) (LiveSession, error) {
	return nil, errors.New("loadHostStub: NewSession not implemented")
}
func (h *loadHostStub) LoadSession(_ context.Context, _ SessionID, _ Setup) (LoadedSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	if h.loadErr != nil {
		return LoadedSession{}, h.loadErr
	}
	return h.loaded, nil
}
func (h *loadHostStub) ResumeSession(context.Context, SessionID, Setup) (LiveSession, error) {
	return nil, errors.New("loadHostStub: ResumeSession not implemented")
}
func (h *loadHostStub) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// internalStubConfigCatalog is a minimal RuntimeConfigCatalog test double for
// white-box (package agent) tests that need Options.ConfigCatalog
// configured: session/load's and session/resume's initial config-state
// population tests share this rather than each defining their own (the
// black-box package-agent_test equivalent is config_test.go's
// stubConfigCatalog, unreachable from here).
type internalStubConfigCatalog struct {
	options []RuntimeConfigOption
	err     error
}

func (c *internalStubConfigCatalog) RuntimeConfigOptions(context.Context, SessionID) ([]RuntimeConfigOption, error) {
	if c.err != nil {
		return nil, c.err
	}
	return append([]RuntimeConfigOption(nil), c.options...), nil
}

// replayPipeConns wires two protocol.Conns together over a net.Pipe (a local
// equivalent of capabilities_test.go's pipeConns, unexported there and thus
// unreachable from this package-agent white-box test file).
func replayPipeConns(t *testing.T) (client, server *protocol.Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	client = protocol.NewConn(c1, c1, protocol.ConnOptions{})
	server = protocol.NewConn(c2, c2, protocol.ConnOptions{})
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

// collectSessionUpdates registers a session/update notification handler on
// client, delivering each decoded notification on the returned channel.
// Notification handlers run on a dedicated worker independent of the
// request/response path (see protocol's notifyWorker), so a notification
// written to the wire strictly before an RPC response can still be handled
// AFTER that response's caller unblocks; callers must drain this channel
// with a bounded wait (see drainNotifications), never assert on it
// immediately after an RPC call returns.
func collectSessionUpdates(t *testing.T, client *protocol.Conn) <-chan protocol.SessionNotification {
	t.Helper()
	ch := make(chan protocol.SessionNotification, 32)
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

// drainNotifications reads exactly want notifications from ch, bounded by
// timeout, failing the test if fewer arrive in time.
func drainNotifications(t *testing.T, ch <-chan protocol.SessionNotification, want int, timeout time.Duration) []protocol.SessionNotification {
	t.Helper()
	got := make([]protocol.SessionNotification, 0, want)
	deadline := time.After(timeout)
	for len(got) < want {
		select {
		case n := <-ch:
			got = append(got, n)
		case <-deadline:
			t.Fatalf("timed out waiting for session/update notifications: got %d, want %d", len(got), want)
		}
	}
	return got
}

func replayTestSetup(t *testing.T) (host *loadHostStub, live *replayLiveStub, replayer *fakeReplayer, a *Agent, client, server *protocol.Conn) {
	t.Helper()
	live = newReplayLiveStub(t)
	host = &loadHostStub{loaded: LoadedSession{Live: live, ReplayedThrough: 0}}
	replayer = &fakeReplayer{journal: newFakeJournalReplayer()}

	var err error
	a, err = New(Options{Host: host, Replayer: replayer})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server = replayPipeConns(t)
	a.Register(server)
	return host, live, replayer, a, client, server
}

// TestHandleSessionLoadNotRegisteredWithoutReplayer asserts the capability
// gate: with no Replayer configured, session/load is not wired at all (Conn's
// method-not-found fallback rejects it), matching capabilities.go never
// advertising loadSession in that configuration.
func TestHandleSessionLoadNotRegisteredWithoutReplayer(t *testing.T) {
	host := &loadHostStub{}
	a, err := New(Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := replayPipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	validID := coreuuid.MustParse("11111111-1111-4111-8111-111111111111")
	_, err = agentConn.LoadSession(context.Background(), protocol.LoadSessionRequest{
		SessionID: protocol.SessionID(validID.String()), Cwd: "/workspace",
	})
	if err == nil {
		t.Fatal("LoadSession without Replayer: error = nil, want MethodNotFound")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeMethodNotFound {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeMethodNotFound)
	}
	if host.callCount() != 0 {
		t.Errorf("Host.LoadSession calls = %d, want 0", host.callCount())
	}
}

// TestHandleSessionLoadHappyPath exercises the full path: Host.LoadSession is
// called, every reconstructed session/update is delivered to the client with
// isReplay:true, the live session is registered under its own SessionID, and
// the response arrives only after every notification has already reached the
// client (the "same correlation contract as prompt" property).
func TestHandleSessionLoadHappyPath(t *testing.T) {
	turn0 := coreuuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	evID := coreuuid.MustParse("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
	events := []event.Event{
		event.TurnStarted{
			Header:    event.Header{Coordinates: identity.Coordinates{LoopID: testLoopID, TurnID: turn0}, EventID: evID, EventVisibility: event.Public},
			TurnIndex: 0,
			Message:   userMessage("hello from history"),
		},
	}

	host, live, replayer, a, client, _ := replayTestSetup(t)
	replayer.journal = newFakeJournalReplayer(events...)

	updates := collectSessionUpdates(t, client)
	agentConn := protocol.NewAgentConn(client)

	req := protocol.LoadSessionRequest{SessionID: protocol.SessionID(live.SessionID().String()), Cwd: "/workspace"}
	resp, err := agentConn.LoadSession(context.Background(), req)
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if resp == nil {
		t.Fatal("LoadSession: resp = nil")
	}

	if host.callCount() != 1 {
		t.Fatalf("Host.LoadSession calls = %d, want 1", host.callCount())
	}
	if replayer.calls != 1 {
		t.Fatalf("Replayer.OpenEventReplayer calls = %d, want 1", replayer.calls)
	}

	got := drainNotifications(t, updates, 1, 5*time.Second)
	if got[0].Update.UserMessageChunk == nil {
		t.Fatalf("notification = %+v, want user_message_chunk", got[0].Update)
	}
	if got[0].Update.UserMessageChunk.Content.Text.Text != "hello from history" {
		t.Errorf("user message text = %q, want %q", got[0].Update.UserMessageChunk.Content.Text.Text, "hello from history")
	}
	var meta updateMeta
	if err := json.Unmarshal(got[0].Meta, &meta); err != nil {
		t.Fatalf("unmarshal _meta: %v", err)
	}
	if !meta.IsReplay {
		t.Error("_meta.isReplay = false, want true")
	}

	// The live session must now be registered under its own SessionID.
	registered, ok := a.sessions.get(live.SessionID())
	if !ok || registered != live {
		t.Errorf("sessions.get(%v) = (%v, %v), want (live, true)", live.SessionID(), registered, ok)
	}
}

// TestHandleSessionLoadPopulatesInitialConfigOptions is the Important fix
// from the Phase 4 follow-up review, checked against session/load: like
// session/new, the response must surface the loaded session's initial
// runtime configuration options and mode state when Options.ConfigCatalog
// is configured, via the exact same shared initialConfigState helper
// (config.go) — proving the gap did not exist only in session/new.
func TestHandleSessionLoadPopulatesInitialConfigOptions(t *testing.T) {
	live := newReplayLiveStub(t)
	host := &loadHostStub{loaded: LoadedSession{Live: live, ReplayedThrough: 0}}
	replayer := &fakeReplayer{journal: newFakeJournalReplayer()}
	catalog := &internalStubConfigCatalog{options: []RuntimeConfigOption{
		{
			ID:       ModeConfigOptionID,
			Category: protocol.SessionConfigOptionCategoryMode,
			Name:     "Mode",
			Values: []RuntimeConfigValue{
				{ID: "build", Name: "Build"},
				{ID: "plan", Name: "Plan"},
			},
			CurrentValue: "plan",
		},
	}}

	a, err := New(Options{Host: host, Replayer: replayer, ConfigCatalog: catalog})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := replayPipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	resp, err := agentConn.LoadSession(context.Background(), protocol.LoadSessionRequest{
		SessionID: protocol.SessionID(live.SessionID().String()), Cwd: "/workspace",
	})
	if err != nil {
		t.Fatalf("LoadSession: %v", err)
	}
	if resp == nil {
		t.Fatal("LoadSession: resp = nil")
	}
	if len(resp.ConfigOptions) != 1 {
		t.Fatalf("resp.ConfigOptions has %d entries, want 1", len(resp.ConfigOptions))
	}
	if resp.Modes == nil || resp.Modes.CurrentModeID != "plan" {
		t.Fatalf("resp.Modes = %+v, want a populated SessionModeState with CurrentModeID \"plan\"", resp.Modes)
	}
}

// TestHandleSessionLoadShutsDownOrphanWhenInitialConfigFetchFails asserts the
// initial-config-state fetch failure path gets the same best-effort
// SessionCloser.Shutdown cleanup session/load already applies to every other
// post-replay failure.
func TestHandleSessionLoadShutsDownOrphanWhenInitialConfigFetchFails(t *testing.T) {
	live := newReplayLiveStub(t)
	host := &loadHostStub{loaded: LoadedSession{Live: live, ReplayedThrough: 0}}
	replayer := &fakeReplayer{journal: newFakeJournalReplayer()}
	catalog := &internalStubConfigCatalog{err: errors.New("catalog unavailable")}

	a, err := New(Options{Host: host, Replayer: replayer, ConfigCatalog: catalog})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := replayPipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	_, err = agentConn.LoadSession(context.Background(), protocol.LoadSessionRequest{
		SessionID: protocol.SessionID(live.SessionID().String()), Cwd: "/workspace",
	})
	if err == nil {
		t.Fatal("LoadSession: error = nil, want InternalError when the initial config-options fetch fails")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInternalError {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
	}
	if got := live.shutdownCallCount(); got != 1 {
		t.Errorf("SessionCloser.Shutdown calls = %d, want exactly 1 (orphaned session must not be silently abandoned)", got)
	}
	if _, ok := a.sessions.get(live.SessionID()); ok {
		t.Error("session is registered after initial-config-fetch failure, want it NOT registered")
	}
}

// TestHandleSessionLoadCapacityRejected asserts the capacity pre-check
// short-circuits before the host is ever touched, matching handleSessionNew's
// own capacity behavior.
func TestHandleSessionLoadCapacityRejected(t *testing.T) {
	host, _, _, a, client, _ := replayTestSetup(t)

	// Directly fill the registry to capacity (white-box): avoids driving
	// MaxLiveSessions real session/new round trips just to set up this case.
	for i := 0; i < MaxLiveSessions; i++ {
		id, err := coreuuid.New()
		if err != nil {
			t.Fatalf("uuid.New: %v", err)
		}
		if err := a.sessions.add(&replayLiveStub{id: id}, "/test/cwd"); err != nil {
			t.Fatalf("sessions.add: %v", err)
		}
	}

	agentConn := protocol.NewAgentConn(client)
	validID := coreuuid.MustParse("11111111-1111-4111-8111-111111111111")
	_, err := agentConn.LoadSession(context.Background(), protocol.LoadSessionRequest{
		SessionID: protocol.SessionID(validID.String()), Cwd: "/workspace",
	})
	if err == nil {
		t.Fatal("LoadSession at capacity: error = nil, want rejection")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInternalError {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
	}
	if host.callCount() != 0 {
		t.Errorf("Host.LoadSession calls = %d, want 0 (capacity pre-check must short-circuit before touching host)", host.callCount())
	}
}

// TestHandleSessionLoadHostErrorPassesThroughTypedFault asserts a
// *protocol.Fault returned by Host.LoadSession is passed through unchanged,
// matching handleSessionNew's identical rule.
func TestHandleSessionLoadHostErrorPassesThroughTypedFault(t *testing.T) {
	wantFault := protocol.ResourceNotFound("session/load: no such durable session", nil)
	host := &loadHostStub{loadErr: wantFault}
	replayer := &fakeReplayer{journal: newFakeJournalReplayer()}
	a, err := New(Options{Host: host, Replayer: replayer})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := replayPipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	validID := coreuuid.MustParse("11111111-1111-4111-8111-111111111111")
	_, err = agentConn.LoadSession(context.Background(), protocol.LoadSessionRequest{
		SessionID: protocol.SessionID(validID.String()), Cwd: "/workspace",
	})
	if err == nil {
		t.Fatal("LoadSession: error = nil, want the host's Fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeResourceNotFound {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeResourceNotFound)
	}
}

// TestHandleSessionLoadShutsDownOrphanWhenReplayFails asserts that once
// Host.LoadSession has handed back a live, host-backed session, a failure
// anywhere in the replay step (here: opening the durable cursor) makes that
// session an orphan this facade can no longer track — and it must not be
// silently abandoned: SessionCloser.Shutdown is called on it, and it is
// never registered.
func TestHandleSessionLoadShutsDownOrphanWhenReplayFails(t *testing.T) {
	live := newReplayLiveStub(t)
	host := &loadHostStub{loaded: LoadedSession{Live: live, ReplayedThrough: 0}}
	replayer := &fakeReplayer{openErr: errors.New("open replayer: backend unavailable")}
	a, err := New(Options{Host: host, Replayer: replayer})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := replayPipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	req := protocol.LoadSessionRequest{SessionID: protocol.SessionID(live.SessionID().String()), Cwd: "/workspace"}
	_, err = agentConn.LoadSession(context.Background(), req)
	if err == nil {
		t.Fatal("LoadSession: error = nil, want the replayer-open failure")
	}

	if got := live.shutdownCallCount(); got != 1 {
		t.Errorf("Shutdown calls = %d, want exactly 1", got)
	}
	if _, ok := a.sessions.get(live.SessionID()); ok {
		t.Error("orphaned session must not be registered")
	}
}

// TestHandleSessionLoadShutsDownOrphanOnRegistryCapacityRace mirrors
// session.go's TestHandleSessionNewOrphanedSessionShutdownOnRegistryRace for
// session/load: two concurrent session/load calls both pass the advisory
// atCapacity pre-check and both genuinely obtain a real host-backed
// LoadedSession and complete a full (empty) replay before either's
// sessions.add call can run; only one can win the registry's last slot, and
// the loser's live session must get the same best-effort Shutdown cleanup,
// never be silently abandoned.
func TestHandleSessionLoadShutsDownOrphanOnRegistryCapacityRace(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})

	liveA := newReplayLiveStub(t)
	liveB := newReplayLiveStub(t)
	gatingHost := &gatingLoadHost{queue: []*replayLiveStub{liveA, liveB}, entered: entered, release: release}
	replayer := &fakeReplayer{journal: newFakeJournalReplayer()}
	a, err := New(Options{Host: gatingHost, Replayer: replayer})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := replayPipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	// Fill to MaxLiveSessions-1 directly (white-box), leaving exactly one slot.
	for i := 0; i < MaxLiveSessions-1; i++ {
		id, err := coreuuid.New()
		if err != nil {
			t.Fatalf("uuid.New: %v", err)
		}
		if err := a.sessions.add(&replayLiveStub{id: id}, "/test/cwd"); err != nil {
			t.Fatalf("sessions.add: %v", err)
		}
	}

	type result struct{ err error }
	results := make(chan result, 2)
	callLoad := func(id coreuuid.UUID) {
		_, err := agentConn.LoadSession(context.Background(), protocol.LoadSessionRequest{
			SessionID: protocol.SessionID(id.String()), Cwd: "/workspace",
		})
		results <- result{err: err}
	}
	go callLoad(liveA.id)
	go callLoad(liveB.id)

	<-entered
	<-entered
	close(release)

	r1 := <-results
	r2 := <-results

	winners, losers := 0, 0
	for _, r := range []result{r1, r2} {
		if r.err == nil {
			winners++
		} else {
			losers++
		}
	}
	if winners != 1 || losers != 1 {
		t.Fatalf("winners=%d losers=%d, want exactly 1 each (one registry slot available)", winners, losers)
	}

	// Exactly one of liveA/liveB must be registered; the other must have had
	// Shutdown called on it and must not be registered.
	_, aOK := a.sessions.get(liveA.id)
	_, bOK := a.sessions.get(liveB.id)
	if aOK == bOK {
		t.Fatalf("exactly one of liveA/liveB should be registered, got aOK=%v bOK=%v", aOK, bOK)
	}

	loser := liveA
	if aOK {
		loser = liveB
	}
	if got := loser.shutdownCallCount(); got != 1 {
		t.Errorf("loser Shutdown calls = %d, want exactly 1", got)
	}
}

// gatingLoadHost is a SessionHost whose LoadSession hands back the next
// queued live session, signaling entry on entered and blocking on release —
// the same barrier technique session_test.go's gatingHostStub uses for
// session/new — so both racing session/load calls genuinely complete
// Host.LoadSession (and, in this handler, the whole replay step) before
// either's sessions.add call can run.
type gatingLoadHost struct {
	mu      sync.Mutex
	queue   []*replayLiveStub
	entered chan struct{}
	release chan struct{}
}

func (h *gatingLoadHost) NewSession(context.Context, Setup) (LiveSession, error) {
	return nil, errors.New("gatingLoadHost: NewSession not implemented")
}

func (h *gatingLoadHost) LoadSession(_ context.Context, id SessionID, _ Setup) (LoadedSession, error) {
	h.mu.Lock()
	var chosen *replayLiveStub
	for i, live := range h.queue {
		if live.id == id {
			chosen = live
			h.queue = append(h.queue[:i], h.queue[i+1:]...)
			break
		}
	}
	h.mu.Unlock()
	if chosen == nil {
		return LoadedSession{}, errors.New("gatingLoadHost: unknown session id")
	}

	h.entered <- struct{}{}
	<-h.release

	return LoadedSession{Live: chosen, ReplayedThrough: 0}, nil
}

func (h *gatingLoadHost) ResumeSession(context.Context, SessionID, Setup) (LiveSession, error) {
	return nil, errors.New("gatingLoadHost: ResumeSession not implemented")
}
