package agent

import (
	"testing"

	"github.com/looprig/acp/client"
)

// TestMetaWireRoundTripAgentToClient proves the actual JSON `_meta` bytes
// this package's production marshalUpdateMeta emits are exactly what
// acp/client's production DecodeUpdateMeta expects to consume.
//
// The two sides are independently-owned wire twins (see this file's
// package's updateMeta doc above translate.go, and client/updates.go's
// UpdateMeta doc): acp/client must never import acp/agent (see
// acp/CLAUDE.md's layering rule), so there is no compile-time coupling
// between the field names/JSON tags on either side, only a shared
// convention documented in comments. A key rename on either side — say
// "eventId" drifting to "event_id" — would compile cleanly on both sides
// and only show up as silently-empty fields at runtime. This test is the
// guard: it calls the real producer function directly (marshalUpdateMeta,
// the same function translate.go and replay.go both call to build every
// session/update notification's `_meta` object) and feeds its raw output
// straight into the real consumer function (client.DecodeUpdateMeta, the
// same function dispatch.go calls to decode every inbound session/update
// notification's `_meta` object). This file lives in package agent (not
// agent_test) specifically so it can reach the unexported marshalUpdateMeta
// directly; it imports acp/client — the reverse direction is forbidden, not
// this one — solely to call the real DecodeUpdateMeta, never anything
// client imports back from agent.
func TestMetaWireRoundTripAgentToClient(t *testing.T) {
	tests := []struct {
		name              string
		eventID, promptID string
		isReplay          bool
	}{
		{
			name:     "live update carries eventId and promptId",
			eventID:  "55555555-5555-4555-8555-555555555555",
			promptID: "44444444-4444-4444-8444-444444444444",
			isReplay: false,
		},
		{
			name:     "replay update carries eventId, isReplay, no promptId",
			eventID:  "11111111-1111-4111-8111-111111111111",
			promptID: "",
			isReplay: true,
		},
		{
			name:     "live update with empty promptId still round-trips eventId",
			eventID:  "22222222-2222-4222-8222-222222222222",
			promptID: "",
			isReplay: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := marshalUpdateMeta(updateMeta{EventID: tt.eventID, PromptID: tt.promptID, IsReplay: tt.isReplay})

			got := client.DecodeUpdateMeta(raw)
			want := client.UpdateMeta{EventID: tt.eventID, PromptID: tt.promptID, IsReplay: tt.isReplay}
			if got != want {
				t.Errorf("round trip through raw wire bytes %s:\nDecodeUpdateMeta() = %+v\nwant                  %+v", raw, got, want)
			}
		})
	}
}
