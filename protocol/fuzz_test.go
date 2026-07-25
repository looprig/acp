package protocol_test

import (
	"testing"

	"github.com/looprig/acp/protocol"
)

// FuzzEnvelope feeds arbitrary bytes to ParseEnvelope. It must never panic,
// and every successfully parsed envelope must satisfy the invariants of its
// Kind (see acp/CLAUDE.md: all wire input is untrusted; fail closed rather
// than producing an inconsistent typed value).
func FuzzEnvelope(f *testing.F) {
	seeds := []string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","id":"x-1","method":"initialize","params":null}`,
		`{"jsonrpc":"2.0","id":1,"result":{}}`,
		`{"jsonrpc":"2.0","id":1,"result":null}`,
		`{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"m"}}`,
		`{"jsonrpc":"2.0","method":"session/update"}`,
		`{"jsonrpc":"2.0","id":null,"method":"session/update"}`,
		`{}`,
		`null`,
		`[]`,
		`"just a string"`,
		`1234`,
		`{"jsonrpc":"1.0"}`,
		`{"jsonrpc":"2.0","id":1.5,"method":"x"}`,
		`{"jsonrpc":"2.0","id":1,"result":1,"error":{"code":1,"message":"m"}}`,
		`{"jsonrpc":"2.0","id":1,"method":"x","bogus":1}`,
		`{"jsonrpc":"2.0",`,
		`{{{{{{{{{{{{{{{{{{{{`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, in string) {
		env, err := protocol.ParseEnvelope([]byte(in))
		if err != nil {
			if env != nil {
				t.Fatalf("ParseEnvelope(%q) returned non-nil envelope alongside error %v", in, err)
			}
			return
		}
		if env == nil {
			t.Fatalf("ParseEnvelope(%q) returned nil envelope with nil error", in)
		}
		switch env.Kind() {
		case protocol.KindRequest:
			if env.Request == nil || env.Request.Method == "" {
				t.Fatalf("ParseEnvelope(%q): invalid request envelope: %#v", in, env.Request)
			}
			if env.Response != nil || env.Notification != nil {
				t.Fatalf("ParseEnvelope(%q): request envelope has extra variants set: %#v", in, env)
			}
		case protocol.KindResponse:
			if env.Response == nil {
				t.Fatalf("ParseEnvelope(%q): invalid response envelope", in)
			}
			if (env.Response.Result == nil) == (env.Response.Error == nil) {
				t.Fatalf("ParseEnvelope(%q): response must have exactly one of result/error: %#v", in, env.Response)
			}
			if env.Request != nil || env.Notification != nil {
				t.Fatalf("ParseEnvelope(%q): response envelope has extra variants set: %#v", in, env)
			}
		case protocol.KindNotification:
			if env.Notification == nil || env.Notification.Method == "" {
				t.Fatalf("ParseEnvelope(%q): invalid notification envelope: %#v", in, env.Notification)
			}
			if env.Request != nil || env.Response != nil {
				t.Fatalf("ParseEnvelope(%q): notification envelope has extra variants set: %#v", in, env)
			}
		default:
			t.Fatalf("ParseEnvelope(%q): unknown Kind for successfully parsed envelope: %v", in, env.Kind())
		}
	})
}
