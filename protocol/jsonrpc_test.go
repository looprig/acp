package protocol_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/looprig/acp/protocol"
)

func TestParseEnvelopeDecodesValidShapes(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want protocol.Kind
	}{
		{
			name: "request with object params",
			in:   `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"a":1}}`,
			want: protocol.KindRequest,
		},
		{
			name: "request with string id",
			in:   `{"jsonrpc":"2.0","id":"abc-123","method":"initialize"}`,
			want: protocol.KindRequest,
		},
		{
			name: "response with result",
			in:   `{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`,
			want: protocol.KindResponse,
		},
		{
			name: "response with error",
			in:   `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"nope"}}`,
			want: protocol.KindResponse,
		},
		{
			name: "notification with absent id",
			in:   `{"jsonrpc":"2.0","method":"session/update","params":{"a":1}}`,
			want: protocol.KindNotification,
		},
		{
			name: "notification via canonicalized empty (null) id",
			in:   `{"jsonrpc":"2.0","id":null,"method":"session/update"}`,
			want: protocol.KindNotification,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := protocol.ParseEnvelope([]byte(tt.in))
			if err != nil {
				t.Fatalf("ParseEnvelope() error = %v", err)
			}
			if got := env.Kind(); got != tt.want {
				t.Fatalf("Kind() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseEnvelopeRequestFields(t *testing.T) {
	env, err := protocol.ParseEnvelope([]byte(`{"jsonrpc":"2.0","id":42,"method":"initialize","params":{"a":1}}`))
	if err != nil {
		t.Fatalf("ParseEnvelope() error = %v", err)
	}
	req := env.Request
	if req == nil {
		t.Fatalf("Request = nil, want set")
	}
	if req.Method != "initialize" {
		t.Errorf("Method = %q, want initialize", req.Method)
	}
	n, ok := req.ID.Number()
	if !ok || n != 42 {
		t.Errorf("ID.Number() = (%d, %v), want (42, true)", n, ok)
	}
	if string(req.Params) != `{"a":1}` {
		t.Errorf("Params = %s, want {\"a\":1}", req.Params)
	}
}

func TestParseEnvelopeCanonicalizesEmptyIDAndNeverEmitsIt(t *testing.T) {
	env, err := protocol.ParseEnvelope([]byte(`{"jsonrpc":"2.0","id":null,"method":"session/update","params":{"x":1}}`))
	if err != nil {
		t.Fatalf("ParseEnvelope() error = %v", err)
	}
	if env.Kind() != protocol.KindNotification {
		t.Fatalf("Kind() = %v, want KindNotification", env.Kind())
	}
	out, err := json.Marshal(env.Notification)
	if err != nil {
		t.Fatalf("Marshal(Notification) error = %v", err)
	}
	if strings.Contains(string(out), `"id"`) {
		t.Fatalf("re-encoded notification must never emit an id field, got %s", out)
	}
}

func TestRequestResponseNotificationEncodeDecodeRoundTrip(t *testing.T) {
	req := &protocol.Request{ID: protocol.NewNumberID(7), Method: "initialize", Params: json.RawMessage(`{"a":1}`)}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("Marshal(Request) error = %v", err)
	}
	env, err := protocol.ParseEnvelope(data)
	if err != nil {
		t.Fatalf("ParseEnvelope(marshaled request) error = %v", err)
	}
	if env.Kind() != protocol.KindRequest || env.Request.Method != "initialize" {
		t.Fatalf("round trip request mismatch: %#v", env)
	}

	resp := &protocol.Response{ID: protocol.NewStringID("abc"), Result: json.RawMessage(`{"ok":true}`)}
	data, err = json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal(Response) error = %v", err)
	}
	env, err = protocol.ParseEnvelope(data)
	if err != nil {
		t.Fatalf("ParseEnvelope(marshaled response) error = %v", err)
	}
	if env.Kind() != protocol.KindResponse {
		t.Fatalf("round trip response mismatch: %#v", env)
	}

	notif := &protocol.Notification{Method: "session/update", Params: json.RawMessage(`{"x":1}`)}
	data, err = json.Marshal(notif)
	if err != nil {
		t.Fatalf("Marshal(Notification) error = %v", err)
	}
	env, err = protocol.ParseEnvelope(data)
	if err != nil {
		t.Fatalf("ParseEnvelope(marshaled notification) error = %v", err)
	}
	if env.Kind() != protocol.KindNotification {
		t.Fatalf("round trip notification mismatch: %#v", env)
	}
}

func TestParseEnvelopeRejects(t *testing.T) {
	oversizedParam := strings.Repeat("a", protocol.MaxMessageBytes)
	oversized := `{"jsonrpc":"2.0","method":"x","params":"` + oversizedParam + `"}`

	deepOpen := strings.Repeat(`{"a":`, protocol.MaxNestingDepth+1)
	deepClose := strings.Repeat(`}`, protocol.MaxNestingDepth+1)
	tooDeep := `{"jsonrpc":"2.0","id":1,"method":"x","params":` + deepOpen + `1` + deepClose + `}`

	tests := []struct {
		name string
		in   string
	}{
		{"wrong jsonrpc version", `{"jsonrpc":"1.0","id":1,"method":"x"}`},
		{"missing jsonrpc version", `{"id":1,"method":"x"}`},
		{"both result and error", `{"jsonrpc":"2.0","id":1,"result":1,"error":{"code":-32600,"message":"m"}}`},
		{"bool id", `{"jsonrpc":"2.0","id":true,"method":"x"}`},
		{"array id", `{"jsonrpc":"2.0","id":[1],"method":"x"}`},
		{"object id", `{"jsonrpc":"2.0","id":{},"method":"x"}`},
		{"fractional id", `{"jsonrpc":"2.0","id":1.5,"method":"x"}`},
		{"duplicate top-level field", `{"jsonrpc":"2.0","jsonrpc":"2.0","id":1,"method":"x"}`},
		{"unknown top-level field", `{"jsonrpc":"2.0","id":1,"method":"x","bogus":true}`},
		{"oversized payload", oversized},
		{"excessive nesting", tooDeep},
		{"response missing id", `{"jsonrpc":"2.0","result":1}`},
		{"response with null id", `{"jsonrpc":"2.0","id":null,"result":1}`},
		{"neither request/response/notification shape", `{"jsonrpc":"2.0","id":1}`},
		{"empty object", `{}`},
		{"top-level array", `[1,2,3]`},
		{"garbage json", `{"jsonrpc":`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := protocol.ParseEnvelope([]byte(tt.in))
			if err == nil {
				t.Fatalf("ParseEnvelope(%q) = %#v, nil, want error", tt.name, env)
			}
		})
	}
}

func TestParseEnvelopeDiagnosticsAreCompactAndNeverEchoPayload(t *testing.T) {
	const canary = "TOP-SECRET-PAYLOAD-MARKER-A1B2C3"
	in := `{"jsonrpc":"1.0","id":true,"result":1,"error":{"code":-32600,"message":"` + canary +
		`"},"bogus":"` + canary + `"}`

	_, err := protocol.ParseEnvelope([]byte(in))
	if err == nil {
		t.Fatalf("ParseEnvelope() succeeded, want error")
	}
	msg := err.Error()
	if strings.Contains(msg, canary) {
		t.Fatalf("diagnostic echoed payload content: %s", msg)
	}
	if !strings.Contains(msg, "issue") {
		t.Fatalf("diagnostic %q missing issue-count wording", msg)
	}
	wantKinds := []string{
		string(protocol.IssueWrongVersion),
		string(protocol.IssueInvalidIDType),
		string(protocol.IssueBothResultAndError),
		string(protocol.IssueUnknownField),
	}
	for _, k := range wantKinds {
		if !strings.Contains(msg, k) {
			t.Errorf("diagnostic %q missing issue kind %q", msg, k)
		}
	}
}

func TestValidationErrorAsFault(t *testing.T) {
	_, err := protocol.ParseEnvelope([]byte(`{"jsonrpc":`))
	var verr *protocol.ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("ParseEnvelope() error type = %T, want *protocol.ValidationError", err)
	}
	fault := verr.AsFault()
	if fault.Code != protocol.ErrorCodeParseError {
		t.Errorf("AsFault().Code = %d, want %d (ParseError) for malformed JSON", fault.Code, protocol.ErrorCodeParseError)
	}

	_, err = protocol.ParseEnvelope([]byte(`{"jsonrpc":"1.0","id":1,"method":"x"}`))
	verr = nil
	if !errors.As(err, &verr) {
		t.Fatalf("ParseEnvelope() error type = %T, want *protocol.ValidationError", err)
	}
	fault = verr.AsFault()
	if fault.Code != protocol.ErrorCodeInvalidRequest {
		t.Errorf("AsFault().Code = %d, want %d (InvalidRequest) for structural violation", fault.Code, protocol.ErrorCodeInvalidRequest)
	}
}
