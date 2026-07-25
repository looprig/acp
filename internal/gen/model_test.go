package main

import (
	"os"
	"testing"
)

func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read testdata/%s: %v", name, err)
	}
	return b
}

func findType(t *testing.T, m *Model, name string) TypeDecl {
	t.Helper()
	for _, d := range m.Types {
		if d.Name == name {
			return d
		}
	}
	t.Fatalf("type %q not found in model (have: %v)", name, typeNames(m))
	return TypeDecl{}
}

func typeNames(m *Model) []string {
	var names []string
	for _, d := range m.Types {
		names = append(names, d.Name)
	}
	return names
}

func findField(t *testing.T, d TypeDecl, jsonKey string) Field {
	t.Helper()
	for _, f := range d.Fields {
		if f.JSONKey == jsonKey {
			return f
		}
	}
	t.Fatalf("field with JSON key %q not found on %s (have: %+v)", jsonKey, d.Name, d.Fields)
	return Field{}
}

// --- Behavior 1: nullable normalization ---

func TestNullableNormalization(t *testing.T) {
	m, err := BuildModel(readTestdata(t, "nullable.json"), []byte(`{"version":1}`))
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}
	widget := findType(t, m, "Widget")

	name := findField(t, widget, "name")
	if name.GoType != "string" || !name.Required {
		t.Errorf("name: got GoType=%q Required=%v, want string/required (non-nullable required field is a plain value)", name.GoType, name.Required)
	}

	nickname := findField(t, widget, "nickname")
	if nickname.GoType != "*string" || nickname.Required {
		t.Errorf("nickname: got GoType=%q Required=%v, want *string/optional ({\"type\":[\"string\",\"null\"]} normalizes to a pointer)", nickname.GoType, nickname.Required)
	}

	owner := findField(t, widget, "owner")
	if owner.GoType != "*Owner" || owner.Required {
		t.Errorf("owner: got GoType=%q Required=%v, want *Owner/optional (anyOf-with-null normalizes to a pointer to the referenced type)", owner.GoType, owner.Required)
	}

	tags := findField(t, widget, "tags")
	if tags.GoType != "[]string" || tags.Required {
		t.Errorf("tags: got GoType=%q Required=%v, want []string/optional (nullable arrays stay plain slices, never double-pointered)", tags.GoType, tags.Required)
	}
}

// --- Behavior 2: discriminated union sum-type pattern ---

func TestDiscriminatedUnionNoFallback(t *testing.T) {
	m, err := BuildModel(readTestdata(t, "union_discriminated.json"), []byte(`{"version":1}`))
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}
	shape := findType(t, m, "Shape")

	if shape.UnionMode != UnionDiscriminated {
		t.Fatalf("Shape: UnionMode = %v, want UnionDiscriminated", shape.UnionMode)
	}
	if shape.DiscKey != "kind" {
		t.Fatalf("Shape: DiscKey = %q, want \"kind\"", shape.DiscKey)
	}
	if len(shape.Variants) != 2 {
		t.Fatalf("Shape: got %d variants, want 2", len(shape.Variants))
	}
	var sawCircle, sawSquare bool
	for _, v := range shape.Variants {
		if v.IsFallback {
			t.Errorf("Shape: variant %q unexpectedly marked fallback (every branch here carries a const tag)", v.GoName)
		}
		switch v.Tag {
		case "circle":
			sawCircle = true
			if v.Type != "Circle" {
				t.Errorf("circle variant type = %q, want Circle", v.Type)
			}
		case "square":
			sawSquare = true
			if v.Type != "Square" {
				t.Errorf("square variant type = %q, want Square", v.Type)
			}
		}
	}
	if !sawCircle || !sawSquare {
		t.Fatalf("Shape: missing expected variants, got %+v", shape.Variants)
	}
}

func TestDiscriminatedUnionWithFallback(t *testing.T) {
	m, err := BuildModel(readTestdata(t, "union_discriminated.json"), []byte(`{"version":1}`))
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}
	transport := findType(t, m, "Transport")
	if transport.UnionMode != UnionDiscriminated {
		t.Fatalf("Transport: UnionMode = %v, want UnionDiscriminated", transport.UnionMode)
	}
	fallbackCount := 0
	for _, v := range transport.Variants {
		if v.IsFallback {
			fallbackCount++
			if v.Type != "StdioTransport" {
				t.Errorf("fallback variant type = %q, want StdioTransport", v.Type)
			}
		}
	}
	if fallbackCount != 1 {
		t.Fatalf("Transport: got %d fallback variants, want exactly 1", fallbackCount)
	}
}

func TestStructuralUnion(t *testing.T) {
	m, err := BuildModel(readTestdata(t, "union_discriminated.json"), []byte(`{"version":1}`))
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}
	res := findType(t, m, "ResourcePayload")
	if res.UnionMode != UnionStructural {
		t.Fatalf("ResourcePayload: UnionMode = %v, want UnionStructural", res.UnionMode)
	}
	if res.DiscKey != "" {
		t.Errorf("ResourcePayload: DiscKey = %q, want empty (no tag field exists)", res.DiscKey)
	}
	if len(res.Variants) != 2 {
		t.Fatalf("ResourcePayload: got %d variants, want 2", len(res.Variants))
	}
}

func TestSingleBranchUnionIsAlias(t *testing.T) {
	m, err := BuildModel(readTestdata(t, "union_discriminated.json"), []byte(`{"version":1}`))
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}
	single := findType(t, m, "SingleBranch")
	if single.UnionMode != UnionAlias {
		t.Fatalf("SingleBranch: UnionMode = %v, want UnionAlias", single.UnionMode)
	}
	if single.AliasOf != "OnlyPayload" {
		t.Errorf("SingleBranch: AliasOf = %q, want OnlyPayload", single.AliasOf)
	}
}

// --- Behavior 3: required vs optional field mapping + doc comments ---

func TestRequiredOptionalAndDocs(t *testing.T) {
	m, err := BuildModel(readTestdata(t, "nullable.json"), []byte(`{"version":1}`))
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}
	widget := findType(t, m, "Widget")
	if widget.Doc == "" {
		t.Errorf("Widget: expected a doc comment carried from the schema description")
	}
	name := findField(t, widget, "name")
	if name.Doc == "" {
		t.Errorf("Widget.name: expected a doc comment carried from the field's description")
	}
}

// --- Behavior 4: method table ---

func TestMethodTable(t *testing.T) {
	m, err := BuildModel([]byte(`{"$defs":{}}`), readTestdata(t, "method_table_meta.json"))
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}
	if m.Methods.Version != 1 {
		t.Errorf("Version = %d, want 1", m.Methods.Version)
	}
	if len(m.Methods.Agent) != 2 {
		t.Fatalf("Agent methods = %+v, want 2 entries", m.Methods.Agent)
	}
	// Sorted by symbolic name: "initialize" < "session_new".
	if m.Methods.Agent[0].Symbol != "initialize" || m.Methods.Agent[1].Symbol != "session_new" {
		t.Errorf("Agent methods not sorted deterministically: %+v", m.Methods.Agent)
	}
	if m.Methods.Agent[1].Wire != "session/new" {
		t.Errorf("session_new wire = %q, want session/new", m.Methods.Agent[1].Wire)
	}
	if m.Methods.Agent[1].ConstName != "MethodSessionNew" {
		t.Errorf("session_new const name = %q, want MethodSessionNew", m.Methods.Agent[1].ConstName)
	}
	if len(m.Methods.Client) != 1 || m.Methods.Client[0].Wire != "session/update" {
		t.Errorf("Client methods = %+v", m.Methods.Client)
	}
	if len(m.Methods.Protocol) != 1 || m.Methods.Protocol[0].Wire != "$/cancel_request" {
		t.Errorf("Protocol methods = %+v", m.Methods.Protocol)
	}
}

// --- Behavior 5: capability defaults ---

func TestCapabilityDefaults(t *testing.T) {
	m, err := BuildModel(readTestdata(t, "capabilities.json"), []byte(`{"version":1}`))
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}
	feat := findType(t, m, "FeatureCapabilities")
	if !feat.HasDefaults() {
		t.Fatalf("FeatureCapabilities: HasDefaults() = false, want true")
	}
	widgets := findField(t, feat, "widgets")
	if widgets.DefaultLiteral != "false" {
		t.Errorf("widgets default literal = %q, want \"false\"", widgets.DefaultLiteral)
	}
	if widgets.GoType != "bool" {
		t.Errorf("widgets GoType = %q, want plain bool (optional-but-not-nullable booleans skip the pointer since false is already the zero value)", widgets.GoType)
	}

	root := findType(t, m, "RootCapabilities")
	if !root.HasDefaults() {
		t.Fatalf("RootCapabilities: HasDefaults() = false, want true")
	}
	features := findField(t, root, "features")
	if features.DefaultLiteral != "DefaultFeatureCapabilities()" {
		t.Errorf("features default literal = %q, want a call to the nested type's Default() constructor", features.DefaultLiteral)
	}
	if features.GoType != "*FeatureCapabilities" {
		t.Errorf("features GoType = %q, want *FeatureCapabilities (object-typed optional fields stay pointers)", features.GoType)
	}

	notDefaultable := findType(t, m, "NotDefaultable")
	if notDefaultable.HasDefaults() {
		t.Fatalf("NotDefaultable: HasDefaults() = true, want false (has a required field with no default)")
	}
}

// --- Enum strictness (open vs closed), part of the union behavior ---

func TestEnumStrictness(t *testing.T) {
	m, err := BuildModel(readTestdata(t, "enum.json"), []byte(`{"version":1}`))
	if err != nil {
		t.Fatalf("BuildModel: %v", err)
	}
	status := findType(t, m, "Status")
	if status.Kind != KindEnum || !status.Strict {
		t.Fatalf("Status: Kind=%v Strict=%v, want KindEnum/strict (every branch carries a const)", status.Kind, status.Strict)
	}
	if len(status.Members) != 2 {
		t.Fatalf("Status: got %d members, want 2", len(status.Members))
	}

	category := findType(t, m, "Category")
	if category.Kind != KindEnum || category.Strict {
		t.Fatalf("Category: Kind=%v Strict=%v, want KindEnum/open (one branch has no const)", category.Kind, category.Strict)
	}
	if len(category.Members) != 2 {
		t.Fatalf("Category: got %d members (the open branch should not produce a member), want 2", len(category.Members))
	}

	code := findType(t, m, "Code")
	if code.Kind != KindEnum || code.Strict {
		t.Fatalf("Code: Kind=%v Strict=%v, want KindEnum/open", code.Kind, code.Strict)
	}
	if code.Underlying != "int32" {
		t.Errorf("Code underlying = %q, want int32", code.Underlying)
	}
	if len(code.Members) != 1 || code.Members[0].Name != "CodeParseError" {
		t.Fatalf("Code members = %+v, want exactly [CodeParseError]", code.Members)
	}
}

// --- Determinism ---

func TestBuildModelDeterministic(t *testing.T) {
	schema := readTestdata(t, "union_discriminated.json")
	meta := []byte(`{"version":1}`)
	m1, err := BuildModel(schema, meta)
	if err != nil {
		t.Fatalf("BuildModel #1: %v", err)
	}
	m2, err := BuildModel(schema, meta)
	if err != nil {
		t.Fatalf("BuildModel #2: %v", err)
	}
	if len(m1.Types) != len(m2.Types) {
		t.Fatalf("type count differs between runs: %d vs %d", len(m1.Types), len(m2.Types))
	}
	for i := range m1.Types {
		if m1.Types[i].Name != m2.Types[i].Name {
			t.Fatalf("type order differs at index %d: %q vs %q", i, m1.Types[i].Name, m2.Types[i].Name)
		}
	}
}
