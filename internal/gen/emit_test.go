package main

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// mergedSchemaDoc combines the $defs of several fixture files into one
// schema document, so a single generated package can exercise fixtures that
// were written (and are unit-tested at the model level) independently.
func mergedSchemaDoc(t *testing.T, files ...string) []byte {
	t.Helper()
	defs := map[string]json.RawMessage{}
	for _, f := range files {
		var doc struct {
			Defs map[string]json.RawMessage `json:"$defs"`
		}
		if err := json.Unmarshal(readTestdata(t, f), &doc); err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for k, v := range doc.Defs {
			defs[k] = v
		}
	}
	out, err := json.Marshal(struct {
		Defs map[string]json.RawMessage `json:"$defs"`
	}{defs})
	if err != nil {
		t.Fatalf("marshal merged defs: %v", err)
	}
	return out
}

func TestEmitTypesIsSyntacticallyValidGo(t *testing.T) {
	for _, fixture := range []string{"nullable.json", "union_discriminated.json", "enum.json", "capabilities.json"} {
		fixture := fixture
		t.Run(fixture, func(t *testing.T) {
			schema := readTestdata(t, fixture)
			model, err := BuildModel(schema, []byte(`{"version":1}`))
			if err != nil {
				t.Fatalf("BuildModel: %v", err)
			}
			src, err := EmitTypes(model)
			if err != nil {
				t.Fatalf("EmitTypes: %v", err)
			}
			fset := token.NewFileSet()
			if _, err := parser.ParseFile(fset, fixture+".go", src, parser.AllErrors); err != nil {
				t.Fatalf("generated source does not parse: %v\n--- source ---\n%s", err, src)
			}
		})
	}
}

func TestEmitDeterministic(t *testing.T) {
	schema := mergedSchemaDoc(t, "nullable.json", "union_discriminated.json", "enum.json", "capabilities.json")
	meta := readTestdata(t, "method_table_meta.json")

	m1, err := BuildModel(schema, meta)
	if err != nil {
		t.Fatalf("BuildModel #1: %v", err)
	}
	types1, err := EmitTypes(m1)
	if err != nil {
		t.Fatalf("EmitTypes #1: %v", err)
	}
	methods1, err := EmitMethods(m1)
	if err != nil {
		t.Fatalf("EmitMethods #1: %v", err)
	}

	m2, err := BuildModel(schema, meta)
	if err != nil {
		t.Fatalf("BuildModel #2: %v", err)
	}
	types2, err := EmitTypes(m2)
	if err != nil {
		t.Fatalf("EmitTypes #2: %v", err)
	}
	methods2, err := EmitMethods(m2)
	if err != nil {
		t.Fatalf("EmitMethods #2: %v", err)
	}

	if string(types1) != string(types2) {
		t.Errorf("EmitTypes is not deterministic across identical runs")
	}
	if string(methods1) != string(methods2) {
		t.Errorf("EmitMethods is not deterministic across identical runs")
	}
}

// TestGeneratedCodeBehavior compiles the emitted code into a throwaway
// module and runs a hand-written test program against it, so the union
// Marshal/Unmarshal contract, enum strictness, capability defaults, and
// nullable field round-tripping are verified as real running behavior, not
// just inspected as generated source text.
func TestGeneratedCodeBehavior(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	schema := mergedSchemaDoc(t, "nullable.json", "union_discriminated.json", "enum.json", "capabilities.json")
	meta := []byte(`{"version":1,"agentMethods":{"session_new":"session/new"},"clientMethods":{},"protocolMethods":{}}`)

	model, err := BuildModel(schema, meta)
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}
	typesSrc, err := EmitTypes(model)
	if err != nil {
		t.Fatalf("EmitTypes: %v", err)
	}
	methodsSrc, err := EmitMethods(model)
	if err != nil {
		t.Fatalf("EmitMethods: %v", err)
	}

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "go.mod"), "module gentest\n\ngo 1.26\n")
	writeFile(t, filepath.Join(dir, "types_gen.go"), string(typesSrc))
	writeFile(t, filepath.Join(dir, "methods_gen.go"), string(methodsSrc))
	writeFile(t, filepath.Join(dir, "behavior_test.go"), behaviorTestSource)

	cmd := exec.Command("go", "test", "-race", "./...")
	cmd.Dir = dir
	cmd.Env = cleanGoEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generated code failed its behavior test:\n%s", out)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// cleanGoEnv strips any inherited GOFLAGS (e.g. -mod=vendor from this
// module's own Makefile-driven environment) so the throwaway module, which
// has no vendor directory, builds normally, and disables network module
// resolution since it only uses the standard library.
func cleanGoEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "GOFLAGS=") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, "GOFLAGS=", "GOPROXY=off")
	return env
}

const behaviorTestSource = `package protocol

import (
	"encoding/json"
	"testing"
)

func TestClosedEnumRejectsUnknown(t *testing.T) {
	var s Status
	if err := json.Unmarshal([]byte(` + "`" + `"pending"` + "`" + `), &s); err != nil {
		t.Fatalf("known value rejected: %v", err)
	}
	if s != StatusPending {
		t.Fatalf("got %v, want StatusPending", s)
	}
	if err := json.Unmarshal([]byte(` + "`" + `"bogus"` + "`" + `), &s); err == nil {
		t.Fatalf("unknown value accepted, want rejection")
	}
}

func TestOpenStringEnumAcceptsUnknown(t *testing.T) {
	var c Category
	if err := json.Unmarshal([]byte(` + "`" + `"mode"` + "`" + `), &c); err != nil {
		t.Fatalf("known value rejected: %v", err)
	}
	if c != CategoryMode {
		t.Fatalf("got %v, want CategoryMode", c)
	}
	if err := json.Unmarshal([]byte(` + "`" + `"whatever-future-value"` + "`" + `), &c); err != nil {
		t.Fatalf("open enum rejected an unrecognized value: %v", err)
	}
	if c != "whatever-future-value" {
		t.Fatalf("got %v, want the raw unrecognized value preserved", c)
	}
}

func TestOpenIntegerEnumAcceptsUnknown(t *testing.T) {
	var c Code
	if err := json.Unmarshal([]byte("-32700"), &c); err != nil {
		t.Fatalf("known value rejected: %v", err)
	}
	if c != CodeParseError {
		t.Fatalf("got %v, want CodeParseError", c)
	}
	if err := json.Unmarshal([]byte("12345"), &c); err != nil {
		t.Fatalf("open enum rejected an unrecognized value: %v", err)
	}
	if c != 12345 {
		t.Fatalf("got %v, want 12345", c)
	}
}

func TestDiscriminatedUnionNoFallback(t *testing.T) {
	circle := Shape{Circle: &Circle{Radius: 2}}
	data, err := json.Marshal(circle)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal check: %v", err)
	}
	if decoded["kind"] != "circle" {
		t.Fatalf("kind = %v, want circle", decoded["kind"])
	}

	var zero Shape
	if _, err := json.Marshal(zero); err == nil {
		t.Fatalf("marshal with zero variants set should fail")
	}

	both := Shape{Circle: &Circle{Radius: 1}, Square: &Square{Side: 1}}
	if _, err := json.Marshal(both); err == nil {
		t.Fatalf("marshal with multiple variants set should fail")
	}

	var reparsed Shape
	if err := json.Unmarshal(data, &reparsed); err != nil {
		t.Fatalf("unmarshal known tag: %v", err)
	}
	if reparsed.Circle == nil || reparsed.Circle.Radius != 2 {
		t.Fatalf("reparsed = %+v, want Circle{Radius:2}", reparsed)
	}

	var unknown Shape
	if err := json.Unmarshal([]byte(` + "`" + `{"kind":"triangle"}` + "`" + `), &unknown); err == nil {
		t.Fatalf("unrecognized discriminator accepted, want rejection")
	}

	var missing Shape
	if err := json.Unmarshal([]byte(` + "`" + `{}` + "`" + `), &missing); err == nil {
		t.Fatalf("missing discriminator accepted, want rejection")
	}
}

func TestDiscriminatedUnionFallback(t *testing.T) {
	var absent Transport
	if err := json.Unmarshal([]byte(` + "`" + `{"command":"ls"}` + "`" + `), &absent); err != nil {
		t.Fatalf("unmarshal with absent tag: %v", err)
	}
	if absent.Stdio == nil || absent.Stdio.Command != "ls" {
		t.Fatalf("absent-tag case = %+v, want fallback Stdio{Command:ls}", absent)
	}

	var unrecognized Transport
	if err := json.Unmarshal([]byte(` + "`" + `{"type":"carrier-pigeon","command":"ls"}` + "`" + `), &unrecognized); err != nil {
		t.Fatalf("unmarshal with unrecognized tag: %v", err)
	}
	if unrecognized.Stdio == nil || unrecognized.Stdio.Command != "ls" {
		t.Fatalf("unrecognized-tag case = %+v, want fallback Stdio{Command:ls}", unrecognized)
	}

	var known Transport
	if err := json.Unmarshal([]byte(` + "`" + `{"type":"http","url":"https://example"}` + "`" + `), &known); err != nil {
		t.Fatalf("unmarshal http: %v", err)
	}
	if known.HTTP == nil || known.HTTP.URL != "https://example" {
		t.Fatalf("known-tag case = %+v, want HTTP{URL:https://example}", known)
	}
}

func TestStructuralUnion(t *testing.T) {
	var text ResourcePayload
	if err := json.Unmarshal([]byte(` + "`" + `{"text":"hi","uri":"file:///x"}` + "`" + `), &text); err != nil {
		t.Fatalf("unmarshal text: %v", err)
	}
	if text.TextPayload == nil || text.TextPayload.Text != "hi" {
		t.Fatalf("text = %+v, want TextPayload{Text:hi}", text)
	}

	var blob ResourcePayload
	if err := json.Unmarshal([]byte(` + "`" + `{"blob":"YQ==","uri":"file:///x"}` + "`" + `), &blob); err != nil {
		t.Fatalf("unmarshal blob: %v", err)
	}
	if blob.BlobPayload == nil || blob.BlobPayload.Blob != "YQ==" {
		t.Fatalf("blob = %+v, want BlobPayload{Blob:YQ==}", blob)
	}

	var neither ResourcePayload
	if err := json.Unmarshal([]byte(` + "`" + `{"uri":"file:///x"}` + "`" + `), &neither); err == nil {
		t.Fatalf("neither variant's required fields present, want rejection")
	}

	both := ResourcePayload{TextPayload: &TextPayload{Text: "a", URI: "u"}, BlobPayload: &BlobPayload{Blob: "b", URI: "u"}}
	if _, err := json.Marshal(both); err == nil {
		t.Fatalf("marshal with multiple variants set should fail")
	}

	var zero ResourcePayload
	if _, err := json.Marshal(zero); err == nil {
		t.Fatalf("marshal with zero variants set should fail")
	}
}

func TestSingleBranchAlias(t *testing.T) {
	var s SingleBranch = OnlyPayload{Value: "z"}
	if s.Value != "z" {
		t.Fatalf("alias round trip failed: %+v", s)
	}
}

func TestCapabilityDefaults(t *testing.T) {
	feat := DefaultFeatureCapabilities()
	if feat.Widgets != false || feat.Gadgets != false {
		t.Fatalf("DefaultFeatureCapabilities = %+v, want all false", feat)
	}

	root := DefaultRootCapabilities()
	if root.LoadThing != false {
		t.Fatalf("root.LoadThing = %v, want false", root.LoadThing)
	}
	if root.Features == nil || *root.Features != feat {
		t.Fatalf("root.Features = %+v, want &%+v", root.Features, feat)
	}
}

func TestNullableFieldRoundTrip(t *testing.T) {
	w := Widget{Name: "n"}
	data, err := json.Marshal(w)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode check: %v", err)
	}
	if _, present := decoded["nickname"]; present {
		t.Fatalf("absent optional pointer field should be omitted, got %v", decoded)
	}

	nick := "nick"
	w2 := Widget{Name: "n", Nickname: &nick, Tags: []string{"a", "b"}}
	data2, err := json.Marshal(w2)
	if err != nil {
		t.Fatalf("marshal 2: %v", err)
	}
	var w3 Widget
	if err := json.Unmarshal(data2, &w3); err != nil {
		t.Fatalf("unmarshal 2: %v", err)
	}
	if w3.Nickname == nil || *w3.Nickname != "nick" {
		t.Fatalf("w3.Nickname = %v, want nick", w3.Nickname)
	}
	if len(w3.Tags) != 2 || w3.Tags[0] != "a" {
		t.Fatalf("w3.Tags = %v, want [a b]", w3.Tags)
	}
}

func TestMethodTableConstants(t *testing.T) {
	if MethodSessionNew != "session/new" {
		t.Fatalf("MethodSessionNew = %v, want session/new", MethodSessionNew)
	}
	if _, ok := AgentMethods[MethodSessionNew]; !ok {
		t.Fatalf("AgentMethods missing MethodSessionNew")
	}
	if CurrentProtocolVersion != 1 {
		t.Fatalf("CurrentProtocolVersion = %v, want 1", CurrentProtocolVersion)
	}
}
`
