package agent

// list_internal_test.go white-box tests the session/list cursor codec
// directly (package agent): decodeListCursor's typed *InvalidCursorError and
// its Reason classification never cross the wire (see this module's
// wire-exposure trust boundary — protocol.Fault.Unwrap's doc, ToWireError —
// an internal cause is dropped before a response is ever serialized), so
// asserting on the exact Reason requires calling the codec directly rather
// than round-tripping through a *protocol.Conn like list_test.go's
// black-box, wire-level tests do.

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	coreuuid "github.com/looprig/core/uuid"
)

// fakeMinimalHostForList is the smallest possible SessionHost: this file
// never touches it, but Options.Host is required by New.
type fakeMinimalHostForList struct{}

func (fakeMinimalHostForList) NewSession(context.Context, Setup) (LiveSession, error) {
	return nil, errors.New("fakeMinimalHostForList: NewSession not implemented")
}
func (fakeMinimalHostForList) LoadSession(context.Context, SessionID, Setup) (LoadedSession, error) {
	return LoadedSession{}, errors.New("fakeMinimalHostForList: LoadSession not implemented")
}
func (fakeMinimalHostForList) ResumeSession(context.Context, SessionID, Setup) (LiveSession, error) {
	return nil, errors.New("fakeMinimalHostForList: ResumeSession not implemented")
}

func newTestCursorAgent(t *testing.T) *Agent {
	t.Helper()
	a, err := New(Options{Host: fakeMinimalHostForList{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

// flipLastRune flips a token's final rune to a different valid base64url
// character, mutating its bytes while keeping the string the same length.
func flipLastRune(s string) string {
	runes := []rune(s)
	last := len(runes) - 1
	if runes[last] == 'A' {
		runes[last] = 'B'
	} else {
		runes[last] = 'A'
	}
	return string(runes)
}

// flipRuneJustBeforeDot flips the character immediately preceding token's
// "." separator — a byte inside the payload segment, leaving the tag
// segment (and therefore the original, now-stale HMAC tag) untouched.
func flipRuneJustBeforeDot(t *testing.T, token string) string {
	t.Helper()
	dot := -1
	runes := []rune(token)
	for i, r := range runes {
		if r == '.' {
			dot = i
			break
		}
	}
	if dot <= 0 {
		t.Fatalf("flipRuneJustBeforeDot: no '.' found in %q", token)
	}
	if runes[dot-1] == 'A' {
		runes[dot-1] = 'B'
	} else {
		runes[dot-1] = 'A'
	}
	return string(runes)
}

// TestListCursorRoundTrip asserts a cursor encoded by encodeListCursor
// decodes back to the exact same SessionID, unchanged.
func TestListCursorRoundTrip(t *testing.T) {
	a := newTestCursorAgent(t)
	id := coreuuid.MustParse("00000000-0000-4000-8000-000000000042")

	token, err := a.encodeListCursor(id)
	if err != nil {
		t.Fatalf("encodeListCursor: %v", err)
	}
	got, err := a.decodeListCursor(token)
	if err != nil {
		t.Fatalf("decodeListCursor: %v", err)
	}
	if got != id {
		t.Errorf("decodeListCursor = %v, want %v", got, id)
	}
}

// TestListCursorTamperedTagRejected flips the tag segment's last byte (the
// HMAC authentication tag), leaving the payload untouched, and asserts
// decodeListCursor rejects it as *InvalidCursorError{Reason:
// CursorReasonTampered}.
func TestListCursorTamperedTagRejected(t *testing.T) {
	a := newTestCursorAgent(t)
	id := coreuuid.MustParse("00000000-0000-4000-8000-000000000042")
	token, err := a.encodeListCursor(id)
	if err != nil {
		t.Fatalf("encodeListCursor: %v", err)
	}

	tampered := flipLastRune(token)
	if tampered == token {
		t.Fatal("flipLastRune produced no change")
	}

	_, err = a.decodeListCursor(tampered)
	if err == nil {
		t.Fatal("decodeListCursor(tampered): error = nil, want rejection")
	}
	var cursorErr *InvalidCursorError
	if !errors.As(err, &cursorErr) {
		t.Fatalf("error = %v (%T), want *InvalidCursorError", err, err)
	}
	if cursorErr.Reason != CursorReasonTampered {
		t.Errorf("Reason = %v, want %v", cursorErr.Reason, CursorReasonTampered)
	}
}

// TestListCursorTamperedPayloadRejected flips a byte inside the payload
// segment (changing which session id it names) while leaving the original
// tag untouched, and asserts decodeListCursor rejects it as Tampered — the
// classic "attacker edits the visible position, keeps the old signature"
// tamper shape.
func TestListCursorTamperedPayloadRejected(t *testing.T) {
	a := newTestCursorAgent(t)
	id := coreuuid.MustParse("00000000-0000-4000-8000-000000000042")
	token, err := a.encodeListCursor(id)
	if err != nil {
		t.Fatalf("encodeListCursor: %v", err)
	}

	tampered := flipRuneJustBeforeDot(t, token)
	if tampered == token {
		t.Fatal("flipRuneJustBeforeDot produced no change")
	}

	_, err = a.decodeListCursor(tampered)
	if err == nil {
		t.Fatal("decodeListCursor(tampered payload): error = nil, want rejection")
	}
	var cursorErr *InvalidCursorError
	if !errors.As(err, &cursorErr) {
		t.Fatalf("error = %v (%T), want *InvalidCursorError", err, err)
	}
	if cursorErr.Reason != CursorReasonTampered {
		t.Errorf("Reason = %v, want %v", cursorErr.Reason, CursorReasonTampered)
	}
}

// TestListCursorMalformedShapeRejected asserts a token with no "." separator
// at all is rejected as Malformed.
func TestListCursorMalformedShapeRejected(t *testing.T) {
	a := newTestCursorAgent(t)
	_, err := a.decodeListCursor("not-a-cursor-shape")
	if err == nil {
		t.Fatal("decodeListCursor: error = nil, want rejection")
	}
	var cursorErr *InvalidCursorError
	if !errors.As(err, &cursorErr) {
		t.Fatalf("error = %v (%T), want *InvalidCursorError", err, err)
	}
	if cursorErr.Reason != CursorReasonMalformed {
		t.Errorf("Reason = %v, want %v", cursorErr.Reason, CursorReasonMalformed)
	}
}

// TestListCursorMalformedBase64Rejected asserts a cursor whose payload
// segment is not valid base64url is rejected as Malformed.
func TestListCursorMalformedBase64Rejected(t *testing.T) {
	a := newTestCursorAgent(t)
	_, err := a.decodeListCursor("not valid base64!.also-not-valid!")
	if err == nil {
		t.Fatal("decodeListCursor: error = nil, want rejection")
	}
	var cursorErr *InvalidCursorError
	if !errors.As(err, &cursorErr) {
		t.Fatalf("error = %v (%T), want *InvalidCursorError", err, err)
	}
	if cursorErr.Reason != CursorReasonMalformed {
		t.Errorf("Reason = %v, want %v", cursorErr.Reason, CursorReasonMalformed)
	}
}

// TestListCursorDifferentAgentKeyRejected asserts a perfectly well-formed
// cursor minted by a DIFFERENT Agent instance (and therefore a different
// cursor key) is rejected as Tampered — proving the HMAC key is genuinely
// per-instance, not a fixed/shared value every Agent would accept.
func TestListCursorDifferentAgentKeyRejected(t *testing.T) {
	a1 := newTestCursorAgent(t)
	a2 := newTestCursorAgent(t)
	id := coreuuid.MustParse("00000000-0000-4000-8000-000000000042")

	token, err := a1.encodeListCursor(id)
	if err != nil {
		t.Fatalf("encodeListCursor: %v", err)
	}
	_, err = a2.decodeListCursor(token)
	if err == nil {
		t.Fatal("decodeListCursor (different agent's cursor): error = nil, want rejection")
	}
	var cursorErr *InvalidCursorError
	if !errors.As(err, &cursorErr) {
		t.Fatalf("error = %v (%T), want *InvalidCursorError", err, err)
	}
	if cursorErr.Reason != CursorReasonTampered {
		t.Errorf("Reason = %v, want %v", cursorErr.Reason, CursorReasonTampered)
	}
}

// TestListCursorUnknownFieldRejected asserts a well-formed, well-signed
// cursor whose payload carries an extra unknown field is rejected as
// InvalidPayload — decodeListCursor's JSON decode disallows unknown fields,
// matching sessionstore's own catalog-decode discipline.
func TestListCursorUnknownFieldRejected(t *testing.T) {
	a := newTestCursorAgent(t)
	payload := []byte(`{"after":"00000000-0000-4000-8000-000000000042","extra":"x"}`)
	tag := a.cursorTag(payload)
	token := base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(tag)

	_, err := a.decodeListCursor(token)
	if err == nil {
		t.Fatal("decodeListCursor: error = nil, want rejection")
	}
	var cursorErr *InvalidCursorError
	if !errors.As(err, &cursorErr) {
		t.Fatalf("error = %v (%T), want *InvalidCursorError", err, err)
	}
	if cursorErr.Reason != CursorReasonInvalidPayload {
		t.Errorf("Reason = %v, want %v", cursorErr.Reason, CursorReasonInvalidPayload)
	}
}
