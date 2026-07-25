package agent

import (
	"encoding/json"
	"testing"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/identity"
)

// Fixed, deterministic test coordinates. Golden JSON assertions need
// reproducible ids, never uuid.New()'s random output.
var (
	testSessionUUID = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	testLoopID      = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	testTurnID      = uuid.MustParse("33333333-3333-4333-8333-333333333333")
	testPromptID    = uuid.MustParse("44444444-4444-4444-8444-444444444444")
	testEventID     = uuid.MustParse("55555555-5555-4555-8555-555555555555")
	testToolExecID  = uuid.MustParse("66666666-6666-4666-8666-666666666666")

	testWireSessionID = protocol.SessionID(testSessionUUID.String())
)

func newTestTranslator() *liveTranslator {
	return newLiveTranslator(testWireSessionID, testLoopID, testTurnID, testPromptID)
}

func testHeader(visibility event.EventVisibility) event.Header {
	return event.Header{
		Coordinates:     identity.Coordinates{SessionID: testSessionUUID, LoopID: testLoopID, TurnID: testTurnID},
		EventID:         testEventID,
		EventVisibility: visibility,
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return string(raw)
}

const wantMeta = `"_meta":{"eventId":"55555555-5555-4555-8555-555555555555","promptId":"44444444-4444-4444-8444-444444444444"}`

// --- TokenDelta: text vs thinking classification ---------------------------

func TestTranslateTokenDeltaTextChunk(t *testing.T) {
	tr := newTestTranslator()
	ev := event.TokenDelta{
		Header: testHeader(event.Public),
		Chunk:  &content.TextChunk{Text: "hello"},
	}

	got, ok := tr.Translate(ev)
	if !ok {
		t.Fatal("Translate(text chunk): ok = false, want true")
	}

	want := `{"sessionId":"11111111-1111-4111-8111-111111111111",` +
		`"update":{"content":{"text":"hello","type":"text"},` +
		`"messageId":"msg:11111111-1111-4111-8111-111111111111:22222222-2222-4222-8222-222222222222:33333333-3333-4333-8333-333333333333:0",` +
		`"sessionUpdate":"agent_message_chunk"},` +
		wantMeta + `}`

	if gotJSON := mustMarshal(t, got); gotJSON != want {
		t.Errorf("Translate(text chunk) JSON =\n%s\nwant:\n%s", gotJSON, want)
	}
}

func TestTranslateTokenDeltaThinkingChunk(t *testing.T) {
	tr := newTestTranslator()
	ev := event.TokenDelta{
		Header: testHeader(event.Public),
		Chunk:  &content.ThinkingChunk{Thinking: "pondering"},
	}

	got, ok := tr.Translate(ev)
	if !ok {
		t.Fatal("Translate(thinking chunk): ok = false, want true")
	}

	want := `{"sessionId":"11111111-1111-4111-8111-111111111111",` +
		`"update":{"content":{"text":"pondering","type":"text"},` +
		`"messageId":"msg:11111111-1111-4111-8111-111111111111:22222222-2222-4222-8222-222222222222:33333333-3333-4333-8333-333333333333:0",` +
		`"sessionUpdate":"agent_thought_chunk"},` +
		wantMeta + `}`

	if gotJSON := mustMarshal(t, got); gotJSON != want {
		t.Errorf("Translate(thinking chunk) JSON =\n%s\nwant:\n%s", gotJSON, want)
	}
}

// ToolUseChunk is not called out by the design's mapping table (only text
// and thinking chunks are); ACP has no streaming tool-argument-delta update
// to represent it as, so it must be dropped, not guessed.
func TestTranslateTokenDeltaToolUseChunkDropped(t *testing.T) {
	tr := newTestTranslator()
	ev := event.TokenDelta{
		Header: testHeader(event.Public),
		Chunk:  &content.ToolUseChunk{Index: 0, ID: "tu_1", Name: "bash", InputJSON: `{"cmd":`},
	}

	if _, ok := tr.Translate(ev); ok {
		t.Fatal("Translate(tool use chunk): ok = true, want false (no representable ACP update)")
	}
}

// --- message id sequencing: deterministic, kind-transition based ----------

func TestMessageIDSameKindSharesID(t *testing.T) {
	tr := newTestTranslator()
	first, ok := tr.Translate(event.TokenDelta{Header: testHeader(event.Public), Chunk: &content.TextChunk{Text: "a"}})
	if !ok {
		t.Fatal("first Translate: ok = false")
	}
	second, ok := tr.Translate(event.TokenDelta{Header: testHeader(event.Public), Chunk: &content.TextChunk{Text: "b"}})
	if !ok {
		t.Fatal("second Translate: ok = false")
	}
	if first.Update.AgentMessageChunk.MessageID == nil || second.Update.AgentMessageChunk.MessageID == nil {
		t.Fatal("MessageID unexpectedly nil")
	}
	if *first.Update.AgentMessageChunk.MessageID != *second.Update.AgentMessageChunk.MessageID {
		t.Errorf("MessageID changed across two consecutive text chunks: %q != %q",
			*first.Update.AgentMessageChunk.MessageID, *second.Update.AgentMessageChunk.MessageID)
	}
}

func TestMessageIDChangesOnKindTransitionAndIsDeterministic(t *testing.T) {
	// text, text, thinking, text -> seq 0, 0, 1, 2. Two independent
	// translators fed the identical sequence must derive identical ids
	// (same input twice -> same id: this is the "genuinely deterministic"
	// self-review bar, not just "doesn't crash").
	run := func() []protocol.MessageID {
		tr := newTestTranslator()
		kinds := []content.Chunk{
			&content.TextChunk{Text: "1"},
			&content.TextChunk{Text: "2"},
			&content.ThinkingChunk{Thinking: "3"},
			&content.TextChunk{Text: "4"},
		}
		ids := make([]protocol.MessageID, 0, len(kinds))
		for _, c := range kinds {
			n, ok := tr.Translate(event.TokenDelta{Header: testHeader(event.Public), Chunk: c})
			if !ok {
				t.Fatalf("Translate: ok = false for chunk %#v", c)
			}
			switch {
			case n.Update.AgentMessageChunk != nil:
				ids = append(ids, *n.Update.AgentMessageChunk.MessageID)
			case n.Update.AgentThoughtChunk != nil:
				ids = append(ids, *n.Update.AgentThoughtChunk.MessageID)
			default:
				t.Fatal("neither AgentMessageChunk nor AgentThoughtChunk set")
			}
		}
		return ids
	}

	first := run()
	second := run()

	wantSeqSuffix := []string{":0", ":0", ":1", ":2"}
	for i, id := range first {
		if got := string(id); got[len(got)-2:] != wantSeqSuffix[i] {
			t.Errorf("id[%d] = %q, want suffix %q", i, got, wantSeqSuffix[i])
		}
	}
	if len(first) != len(second) {
		t.Fatalf("len(first)=%d != len(second)=%d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Errorf("id[%d] not deterministic across runs: %q != %q", i, first[i], second[i])
		}
	}
	// Distinct kinds within the run must never collide.
	if first[0] == first[2] {
		t.Errorf("text and thinking chunks produced the same message id: %q", first[0])
	}
}

// --- ToolCallStarted -> tool_call -------------------------------------------

func TestTranslateToolCallStarted(t *testing.T) {
	tr := newTestTranslator()
	ev := event.ToolCallStarted{
		Header:          testHeader(event.Public),
		ToolExecutionID: testToolExecID,
		ToolName:        "bash",
		Summary:         "Running ls -la",
	}

	got, ok := tr.Translate(ev)
	if !ok {
		t.Fatal("Translate(ToolCallStarted): ok = false, want true")
	}

	want := `{"sessionId":"11111111-1111-4111-8111-111111111111",` +
		`"update":{"sessionUpdate":"tool_call","status":"in_progress","title":"Running ls -la",` +
		`"toolCallId":"66666666-6666-4666-8666-666666666666"},` +
		wantMeta + `}`

	if gotJSON := mustMarshal(t, got); gotJSON != want {
		t.Errorf("Translate(ToolCallStarted) JSON =\n%s\nwant:\n%s", gotJSON, want)
	}
}

func TestTranslateToolCallStartedTitleFallsBackToToolName(t *testing.T) {
	tr := newTestTranslator()
	ev := event.ToolCallStarted{
		Header:          testHeader(event.Public),
		ToolExecutionID: testToolExecID,
		ToolName:        "bash",
		Summary:         "",
	}

	got, ok := tr.Translate(ev)
	if !ok {
		t.Fatal("Translate(ToolCallStarted): ok = false, want true")
	}
	if got.Update.ToolCall.Title != "bash" {
		t.Errorf("Title = %q, want fallback to ToolName %q", got.Update.ToolCall.Title, "bash")
	}
}

// --- ToolCallCompleted -> terminal tool_call_update -------------------------

func TestTranslateToolCallCompletedSuccess(t *testing.T) {
	tr := newTestTranslator()
	ev := event.ToolCallCompleted{
		Header:          testHeader(event.Public),
		ToolExecutionID: testToolExecID,
		IsError:         false,
	}

	got, ok := tr.Translate(ev)
	if !ok {
		t.Fatal("Translate(ToolCallCompleted): ok = false, want true")
	}

	want := `{"sessionId":"11111111-1111-4111-8111-111111111111",` +
		`"update":{"sessionUpdate":"tool_call_update","status":"completed",` +
		`"toolCallId":"66666666-6666-4666-8666-666666666666"},` +
		wantMeta + `}`

	if gotJSON := mustMarshal(t, got); gotJSON != want {
		t.Errorf("Translate(ToolCallCompleted success) JSON =\n%s\nwant:\n%s", gotJSON, want)
	}
}

func TestTranslateToolCallCompletedIsErrorMapsToFailedStatus(t *testing.T) {
	tr := newTestTranslator()
	ev := event.ToolCallCompleted{
		Header:          testHeader(event.Public),
		ToolExecutionID: testToolExecID,
		IsError:         true,
		ResultPreview:   "boom: exit status 1",
	}

	got, ok := tr.Translate(ev)
	if !ok {
		t.Fatal("Translate(ToolCallCompleted): ok = false, want true")
	}

	want := `{"sessionId":"11111111-1111-4111-8111-111111111111",` +
		`"update":{"content":[{"content":{"text":"boom: exit status 1","type":"text"},"type":"content"}],` +
		`"sessionUpdate":"tool_call_update","status":"failed",` +
		`"toolCallId":"66666666-6666-4666-8666-666666666666"},` +
		wantMeta + `}`

	if gotJSON := mustMarshal(t, got); gotJSON != want {
		t.Errorf("Translate(ToolCallCompleted failed) JSON =\n%s\nwant:\n%s", gotJSON, want)
	}
}

// --- deterministic tool call id derivation ----------------------------------

func TestToolCallIDIsToolExecutionIDString(t *testing.T) {
	tr := newTestTranslator()
	started, ok := tr.Translate(event.ToolCallStarted{Header: testHeader(event.Public), ToolExecutionID: testToolExecID, ToolName: "bash"})
	if !ok {
		t.Fatal("Translate(ToolCallStarted): ok = false")
	}
	completed, ok := tr.Translate(event.ToolCallCompleted{Header: testHeader(event.Public), ToolExecutionID: testToolExecID})
	if !ok {
		t.Fatal("Translate(ToolCallCompleted): ok = false")
	}

	want := protocol.ToolCallID(testToolExecID.String())
	if started.Update.ToolCall.ToolCallID != want {
		t.Errorf("ToolCall.ToolCallID = %q, want %q", started.Update.ToolCall.ToolCallID, want)
	}
	if completed.Update.ToolCallUpdate.ToolCallID != want {
		t.Errorf("ToolCallUpdate.ToolCallID = %q, want %q", completed.Update.ToolCallUpdate.ToolCallID, want)
	}
	if started.Update.ToolCall.ToolCallID != completed.Update.ToolCallUpdate.ToolCallID {
		t.Error("ToolCallStarted and ToolCallCompleted for the same execution must derive the same tool call id")
	}
}

// --- usage_update: ContextMeasured / ContextPressure ------------------------

func TestTranslateContextMeasured(t *testing.T) {
	tr := newTestTranslator()
	ev := event.ContextMeasured{
		Header: testHeader(event.Public),
		Measurement: event.ContextMeasurement{
			InputTokens: 12345,
			InputLimit:  200000,
		},
	}

	got, ok := tr.Translate(ev)
	if !ok {
		t.Fatal("Translate(ContextMeasured): ok = false, want true")
	}

	want := `{"sessionId":"11111111-1111-4111-8111-111111111111",` +
		`"update":{"sessionUpdate":"usage_update","size":200000,"used":12345},` +
		wantMeta + `}`

	if gotJSON := mustMarshal(t, got); gotJSON != want {
		t.Errorf("Translate(ContextMeasured) JSON =\n%s\nwant:\n%s", gotJSON, want)
	}
}

func TestTranslateContextPressure(t *testing.T) {
	tr := newTestTranslator()
	ev := event.ContextPressure{
		Header: testHeader(event.Public),
		Measurement: event.ContextMeasurement{
			InputTokens: 99,
			InputLimit:  1000,
		},
		Current: event.PressureCompact,
	}

	got, ok := tr.Translate(ev)
	if !ok {
		t.Fatal("Translate(ContextPressure): ok = false, want true")
	}

	want := `{"sessionId":"11111111-1111-4111-8111-111111111111",` +
		`"update":{"sessionUpdate":"usage_update","size":1000,"used":99},` +
		wantMeta + `}`

	if gotJSON := mustMarshal(t, got); gotJSON != want {
		t.Errorf("Translate(ContextPressure) JSON =\n%s\nwant:\n%s", gotJSON, want)
	}
}

// --- "drop, don't guess": TurnDone.Usage never produces a usage_update -----

// content.Usage carries token-spend totals, not a context-window size; there
// is no non-fabricated Size this translator could supply from it alone, so
// per "drop, don't guess" it must never be translated into a usage_update —
// even when Usage carries real, non-zero data.
func TestTranslateTurnDoneUsageNeverProducesUsageUpdate(t *testing.T) {
	tr := newTestTranslator()
	ev := event.TurnDone{
		Header: testHeader(event.Public),
		Usage: content.Usage{
			InputTokens:  500,
			OutputTokens: 250,
		},
	}

	if _, ok := tr.Translate(ev); ok {
		t.Fatal("Translate(TurnDone with non-zero Usage): ok = true, want false (drop, don't guess: no window size available)")
	}
}

// --- turn terminals are not live-translated (drainToTerminal's job) --------

func TestTranslateTurnTerminalsOtherThanUsageAreNotTranslated(t *testing.T) {
	tr := newTestTranslator()
	events := []event.Event{
		event.TurnFailed{Header: testHeader(event.Public)},
		event.TurnInterrupted{Header: testHeader(event.Public)},
		event.StepDone{Header: testHeader(event.Public)},
		event.TurnStarted{Header: testHeader(event.Public)},
	}
	for _, ev := range events {
		if _, ok := tr.Translate(ev); ok {
			t.Errorf("Translate(%T): ok = true, want false", ev)
		}
	}
}

// --- hard security boundary: Internal events are never translated ----------

func TestTranslateDropsInternalVisibilityEvents(t *testing.T) {
	tr := newTestTranslator()

	cases := []event.Event{
		event.TokenDelta{Header: testHeader(event.Internal), Chunk: &content.TextChunk{Text: "leaked?"}},
		event.ToolCallStarted{Header: testHeader(event.Internal), ToolExecutionID: testToolExecID, ToolName: "bash"},
		event.ToolCallCompleted{Header: testHeader(event.Internal), ToolExecutionID: testToolExecID},
		event.ContextMeasured{Header: testHeader(event.Internal), Measurement: event.ContextMeasurement{InputTokens: 1, InputLimit: 2}},
	}

	for _, ev := range cases {
		if _, ok := tr.Translate(ev); ok {
			t.Errorf("Translate(%T with Internal visibility): ok = true, want false (internal events must never cross the wire)", ev)
		}
	}
}

// Confirms the Public case is not simply "always true" by construction: the
// exact same event shape, differing only in Visibility, must translate.
func TestTranslatePublicVisibilityCounterpartStillTranslates(t *testing.T) {
	tr := newTestTranslator()
	ev := event.TokenDelta{Header: testHeader(event.Public), Chunk: &content.TextChunk{Text: "hi"}}
	if _, ok := tr.Translate(ev); !ok {
		t.Fatal("Translate(Public TokenDelta): ok = false, want true")
	}
}

// --- _meta stamping ----------------------------------------------------------

func TestMetaStampsEventIDAndPromptID(t *testing.T) {
	tr := newTestTranslator()
	got, ok := tr.Translate(event.TokenDelta{Header: testHeader(event.Public), Chunk: &content.TextChunk{Text: "x"}})
	if !ok {
		t.Fatal("Translate: ok = false")
	}

	var meta updateMeta
	if err := json.Unmarshal(got.Meta, &meta); err != nil {
		t.Fatalf("unmarshal _meta: %v", err)
	}
	if meta.EventID != testEventID.String() {
		t.Errorf("_meta.eventId = %q, want %q", meta.EventID, testEventID.String())
	}
	if meta.PromptID != testPromptID.String() {
		t.Errorf("_meta.promptId = %q, want %q", meta.PromptID, testPromptID.String())
	}
}

// Two different events (different EventID) in the same prompt must carry
// different eventId but the same promptId.
func TestMetaEventIDVariesPerEventPromptIDStaysFixed(t *testing.T) {
	tr := newTestTranslator()
	hdr1 := testHeader(event.Public)
	otherEventID := uuid.MustParse("77777777-7777-4777-8777-777777777777")
	hdr2 := testHeader(event.Public)
	hdr2.EventID = otherEventID

	n1, ok := tr.Translate(event.TokenDelta{Header: hdr1, Chunk: &content.TextChunk{Text: "x"}})
	if !ok {
		t.Fatal("Translate #1: ok = false")
	}
	n2, ok := tr.Translate(event.TokenDelta{Header: hdr2, Chunk: &content.TextChunk{Text: "y"}})
	if !ok {
		t.Fatal("Translate #2: ok = false")
	}

	var m1, m2 updateMeta
	if err := json.Unmarshal(n1.Meta, &m1); err != nil {
		t.Fatalf("unmarshal _meta #1: %v", err)
	}
	if err := json.Unmarshal(n2.Meta, &m2); err != nil {
		t.Fatalf("unmarshal _meta #2: %v", err)
	}
	if m1.EventID == m2.EventID {
		t.Error("eventId did not vary across two distinct events")
	}
	if m1.PromptID != m2.PromptID {
		t.Errorf("promptId varied within the same prompt: %q != %q", m1.PromptID, m2.PromptID)
	}
}
