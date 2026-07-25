package agent

// compact_internal_test.go white-box tests the pure helpers behind
// `/compact` (compact.go): isCompactSlashCommand's exact-match routing rule,
// and sanitizedCompactRejection's typed-cause-allowlist sanitization
// discipline (mirroring prompt.go's sanitizedPromptFailure — see
// prompt_internal_test.go, if it exists, for the analogous pattern on that
// side). Both are pure functions with no dependency on a live Agent, so
// package agent (white-box) is the right place to test them directly rather
// than only indirectly through a full wire round-trip (compact_test.go
// covers that separately, black-box, in package agent_test).

import (
	"strings"
	"testing"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/harness/pkg/event"
)

// --- isCompactSlashCommand ---------------------------------------------

func compactContentTextBlock(text string) protocol.ContentBlock {
	return protocol.ContentBlock{Text: &protocol.TextContent{Text: text}}
}

func TestIsCompactSlashCommand(t *testing.T) {
	uri := "https://example.invalid/x.png"
	tests := []struct {
		name   string
		blocks []protocol.ContentBlock
		want   bool
	}{
		{
			name:   "exact literal /compact matches",
			blocks: []protocol.ContentBlock{compactContentTextBlock("/compact")},
			want:   true,
		},
		{
			name:   "plain prose does not match",
			blocks: []protocol.ContentBlock{compactContentTextBlock("hello")},
			want:   false,
		},
		{
			name:   "different slash command does not match",
			blocks: []protocol.ContentBlock{compactContentTextBlock("/help")},
			want:   false,
		},
		{
			name:   "prefix-only text does not match",
			blocks: []protocol.ContentBlock{compactContentTextBlock("/compactness is nice")},
			want:   false,
		},
		{
			name:   "trailing whitespace does not match (must be EXACTLY /compact)",
			blocks: []protocol.ContentBlock{compactContentTextBlock("/compact ")},
			want:   false,
		},
		{
			name:   "leading whitespace does not match",
			blocks: []protocol.ContentBlock{compactContentTextBlock(" /compact")},
			want:   false,
		},
		{
			name:   "trailing newline does not match",
			blocks: []protocol.ContentBlock{compactContentTextBlock("/compact\n")},
			want:   false,
		},
		{
			name:   "extra text after /compact does not match",
			blocks: []protocol.ContentBlock{compactContentTextBlock("/compact please")},
			want:   false,
		},
		{
			name:   "empty prompt does not match",
			blocks: nil,
			want:   false,
		},
		{
			name:   "a second block alongside an exact /compact block does not match",
			blocks: []protocol.ContentBlock{compactContentTextBlock("/compact"), compactContentTextBlock("extra")},
			want:   false,
		},
		{
			name:   "a non-text block does not match, even if the only block",
			blocks: []protocol.ContentBlock{{Image: &protocol.ImageContent{Data: "", MimeType: "image/png", URI: &uri}}},
			want:   false,
		},
		{
			name:   "empty string does not match",
			blocks: []protocol.ContentBlock{compactContentTextBlock("")},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isCompactSlashCommand(tt.blocks); got != tt.want {
				t.Errorf("isCompactSlashCommand(%+v) = %v, want %v", tt.blocks, got, tt.want)
			}
		})
	}
}

// --- sanitizedCompactRejection -------------------------------------------

// TestSanitizedCompactRejectionAllowlist proves every valid
// event.CompactRejectReason maps to its own distinct, fixed message (the
// typed-cause allowlist must not collapse every reason into one generic
// string), and that an out-of-allowlist reason value falls back to the
// fixed generic message rather than leaking Go's default numeric formatting
// of the enum (the "canary" here: an invalid reason value chosen so that if
// this function ever regressed to fmt.Sprintf("%v", reason) or similar, the
// raw numeric tag would appear in the message and this test would catch it).
func TestSanitizedCompactRejectionAllowlist(t *testing.T) {
	validReasons := []event.CompactRejectReason{
		event.CompactRejectControlLaneFull,
		event.CompactRejectShuttingDown,
		event.CompactRejectInterrupted,
		event.CompactRejectCanceled,
		event.CompactRejectStaleBasis,
		event.CompactRejectProgressPublication,
		event.CompactRejectUnavailable,
		event.CompactRejectExecutionFailed,
		event.CompactRejectInvalidSummary,
		event.CompactRejectContextCountFailed,
		event.CompactRejectSummaryTooLarge,
		event.CompactRejectInternal,
		event.CompactRejectContextLimitUnknown,
	}

	seen := make(map[string]event.CompactRejectReason, len(validReasons))
	for _, r := range validReasons {
		if !r.Valid() {
			t.Fatalf("test setup: %v is not a Valid() CompactRejectReason", r)
		}
		f := sanitizedCompactRejection(r)
		if f == nil {
			t.Fatalf("sanitizedCompactRejection(%v) = nil, want a *protocol.Fault", r)
		}
		if f.Code != protocol.ErrorCodeInternalError {
			t.Errorf("sanitizedCompactRejection(%v).Code = %v, want %v", r, f.Code, protocol.ErrorCodeInternalError)
		}
		if other, dup := seen[f.Message]; dup {
			t.Errorf("sanitizedCompactRejection(%v) and sanitizedCompactRejection(%v) produced the identical message %q: the allowlist must not collapse distinct reasons into one string", r, other, f.Message)
		}
		seen[f.Message] = r
	}

	// Canary: a reason value well outside the valid range. If this function
	// ever regressed to formatting the raw enum value into the message, the
	// numeric tag "99" would appear in the wire text.
	const canaryReason = event.CompactRejectReason(99)
	if canaryReason.Valid() {
		t.Fatalf("test setup: canaryReason %d must not be Valid()", canaryReason)
	}
	f := sanitizedCompactRejection(canaryReason)
	if f == nil {
		t.Fatal("sanitizedCompactRejection(invalid reason) = nil, want a *protocol.Fault")
	}
	if strings.Contains(f.Message, "99") {
		t.Errorf("sanitizedCompactRejection(invalid reason).Message = %q, must never contain the raw numeric enum tag", f.Message)
	}
	for knownMsg := range seen {
		if f.Message == knownMsg {
			t.Errorf("sanitizedCompactRejection(invalid reason).Message = %q, must not collide with a real reason's message", f.Message)
		}
	}

	// CompactRejectUnspecified (the zero value) is itself outside Valid()'s
	// range (Valid requires >= CompactRejectControlLaneFull), so a validated
	// CompactWaiterRejected should never carry it — but this function must
	// still fail closed to the same generic fallback rather than panicking or
	// producing some other unlabeled behavior for the zero value specifically.
	if event.CompactRejectUnspecified.Valid() {
		t.Fatal("test setup: CompactRejectUnspecified must not be Valid()")
	}
	unspecified := sanitizedCompactRejection(event.CompactRejectUnspecified)
	if unspecified == nil {
		t.Fatal("sanitizedCompactRejection(CompactRejectUnspecified) = nil, want a *protocol.Fault")
	}
	if unspecified.Message != f.Message {
		t.Errorf("sanitizedCompactRejection(CompactRejectUnspecified).Message = %q, want the same generic fallback as any other invalid reason (%q)", unspecified.Message, f.Message)
	}
}
