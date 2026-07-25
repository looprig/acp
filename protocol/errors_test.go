package protocol_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/looprig/acp/protocol"
)

func TestErrorCodeTableConstructors(t *testing.T) {
	tests := []struct {
		name  string
		fault *protocol.Fault
		want  protocol.ErrorCode
	}{
		{"ParseError", protocol.ParseError("bad json", nil), protocol.ErrorCodeParseError},
		{"InvalidRequest", protocol.InvalidRequest("bad request", nil), protocol.ErrorCodeInvalidRequest},
		{"MethodNotFound", protocol.MethodNotFound("bad request", nil), protocol.ErrorCodeMethodNotFound},
		{"InvalidParams", protocol.InvalidParams("bad request", nil), protocol.ErrorCodeInvalidParams},
		{"InternalError", protocol.InternalError("bad request", nil), protocol.ErrorCodeInternalError},
		{"AuthRequired", protocol.AuthRequired("bad request", nil), protocol.ErrorCodeAuthenticationRequired},
		{"ResourceNotFound", protocol.ResourceNotFound("bad request", nil), protocol.ErrorCodeResourceNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.fault.Code != tt.want {
				t.Errorf("Code = %d, want %d", tt.fault.Code, tt.want)
			}
		})
	}
}

// TestErrorCodeExactValues pins the exact numeric codes from the pinned
// schema artifact (protocol/schema/v1/schema.json's ErrorCode $def, already
// generated into types_gen.go by Task 1.2). This module must not drift from
// that pinned artifact.
func TestErrorCodeExactValues(t *testing.T) {
	tests := []struct {
		code protocol.ErrorCode
		want int32
	}{
		{protocol.ErrorCodeParseError, -32700},
		{protocol.ErrorCodeInvalidRequest, -32600},
		{protocol.ErrorCodeMethodNotFound, -32601},
		{protocol.ErrorCodeInvalidParams, -32602},
		{protocol.ErrorCodeInternalError, -32603},
		{protocol.ErrorCodeAuthenticationRequired, -32000},
		{protocol.ErrorCodeResourceNotFound, -32002},
	}
	for _, tt := range tests {
		if int32(tt.code) != tt.want {
			t.Errorf("code = %d, want %d", tt.code, tt.want)
		}
	}
}

func TestFaultErrorMessageAndUnwrap(t *testing.T) {
	cause := errors.New("disk full: /var/data/session.json")
	f := protocol.InternalError("could not persist session", cause)

	if !errors.Is(f, cause) {
		t.Fatalf("errors.Is(f, cause) = false, want true (typed cause must be reachable via Unwrap)")
	}
	if got := f.Error(); !strings.Contains(got, "could not persist session") {
		t.Errorf("Error() = %q, want it to contain the fault message", got)
	}
}

func TestToWireErrorFromWireErrorRoundTrip(t *testing.T) {
	cause := errors.New("disk full: /var/data/session.json")
	f := protocol.InternalError("could not persist session", cause).
		WithData(map[string]string{"path": "/var/data/session.json"})

	wire := protocol.ToWireError(f)
	if wire == nil {
		t.Fatalf("ToWireError() = nil")
	}
	if wire.Code != f.Code {
		t.Fatalf("wire.Code = %d, want %d", wire.Code, f.Code)
	}
	if strings.Contains(string(wire.Data), "disk full") {
		t.Fatalf("wire.Data leaked internal cause: %s", wire.Data)
	}
	if !strings.Contains(string(wire.Data), "/var/data/session.json") {
		t.Fatalf("wire.Data missing whitelisted field: %s", wire.Data)
	}

	f2 := protocol.FromWireError(wire)
	if f2 == nil {
		t.Fatalf("FromWireError() = nil")
	}
	if f2.Code != f.Code {
		t.Errorf("round trip Code = %d, want %d", f2.Code, f.Code)
	}
	if string(f2.Data) != string(f.Data) {
		t.Errorf("round trip Data = %s, want %s", f2.Data, f.Data)
	}
	if errors.Unwrap(f2) != nil {
		t.Errorf("FromWireError must not fabricate a local cause, got %v", errors.Unwrap(f2))
	}
}

func TestWireErrorJSONShape(t *testing.T) {
	f := protocol.MethodNotFound("method not found: session/bogus", nil)
	wire := protocol.ToWireError(f)
	data, err := json.Marshal(wire)
	if err != nil {
		t.Fatalf("Marshal(wire error) error = %v", err)
	}
	var decoded struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal(wire error json) error = %v", err)
	}
	if decoded.Code != int(protocol.ErrorCodeMethodNotFound) {
		t.Errorf("decoded code = %d, want %d", decoded.Code, protocol.ErrorCodeMethodNotFound)
	}
	if decoded.Message == "" {
		t.Errorf("decoded message is empty")
	}
}
