// Package gen builds a Go-facing type model from a pinned Agent Client
// Protocol JSON Schema (2020-12 subset) and its companion method-table JSON,
// then emits Go source. It understands exactly the schema shapes present in
// the pinned v1 artifact (see protocol/schema/v1/REVISION) and fails loudly on
// anything else rather than guessing at unsupported JSON Schema features.
package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// excludedDefs are $defs that model JSON-RPC envelope framing or open-ended
// vendor extension payloads rather than ACP domain types. They are handled by
// hand-written code in acp/protocol (jsonrpc.go, errors.go — see Task 1.3 of
// the ACP bridge implementation plan), not generated here:
//
//   - AgentRequest, AgentResponse, AgentNotification, ClientRequest,
//     ClientResponse, ClientNotification: the generic JSON-RPC dispatch-union
//     envelopes. Go handlers are registered per wire method string and decode
//     directly into the concrete per-method type below; a Rust-style
//     discriminated "any request" sum type serves no purpose in this design.
//   - RequestId: the JSON-RPC id scalar (null | integer | string) — part of
//     envelope framing, not a domain value.
//   - Error: the JSON-RPC error object shape — collides in spirit with Go's
//     builtin error and is owned by errors.go.
//   - ExtRequest, ExtResponse, ExtNotification: schema-less vendor extension
//     passthrough payloads (literally `{}` schemas); represented as
//     json.RawMessage at the transport layer, not as generated types.
//   - CancelRequestNotification: the protocol-level (x-side: protocol)
//     `$/cancel_request` notification — envelope-level cancellation, owned by
//     jsonrpc.go. (session/cancel's CancelNotification, x-side: agent, IS a
//     normal generated domain type.)
var excludedDefs = map[string]bool{
	"AgentRequest":              true,
	"AgentResponse":             true,
	"AgentNotification":         true,
	"ClientRequest":             true,
	"ClientResponse":            true,
	"ClientNotification":        true,
	"RequestId":                 true,
	"Error":                     true,
	"ExtRequest":                true,
	"ExtResponse":               true,
	"ExtNotification":           true,
	"CancelRequestNotification": true,
}

// Schema is the JSON Schema (2020-12) subset used by the pinned ACP artifact.
type Schema struct {
	Ref         string             `json:"$ref"`
	Type        StringOrSlice      `json:"type"`
	Const       json.RawMessage    `json:"const"`
	Title       string             `json:"title"`
	Description string             `json:"description"`
	Format      string             `json:"format"`
	Items       *Schema            `json:"items"`
	Properties  map[string]*Schema `json:"properties"`
	Required    []string           `json:"required"`
	AllOf       []*Schema          `json:"allOf"`
	AnyOf       []*Schema          `json:"anyOf"`
	OneOf       []*Schema          `json:"oneOf"`
	Default     json.RawMessage    `json:"default"`
	XMethod     string             `json:"x-method"`
	XSide       string             `json:"x-side"`
}

// StringOrSlice decodes a JSON Schema "type" keyword, which is either a bare
// string or an array of strings.
type StringOrSlice []string

func (s *StringOrSlice) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = []string{single}
		return nil
	}
	var multi []string
	if err := json.Unmarshal(data, &multi); err != nil {
		return fmt.Errorf("type: expected string or []string: %w", err)
	}
	*s = multi
	return nil
}

// SchemaDocument is the top-level shape of schema.json that this generator
// consumes: only $defs matters; the top-level anyOf message-dispatch union is
// envelope framing (see excludedDefs) and is not walked.
type SchemaDocument struct {
	Defs map[string]*Schema `json:"$defs"`
}

// MetaDocument is the top-level shape of meta.json.
type MetaDocument struct {
	Version         int               `json:"version"`
	AgentMethods    map[string]string `json:"agentMethods"`
	ClientMethods   map[string]string `json:"clientMethods"`
	ProtocolMethods map[string]string `json:"protocolMethods"`
}

// DeclKind classifies a generated top-level type declaration.
type DeclKind int

const (
	KindScalar DeclKind = iota
	KindEnum
	KindObject
)

// UnionMode classifies how an object-shaped declaration's variants (if any)
// are distinguished on the wire.
type UnionMode int

const (
	// UnionNone: a plain struct, no oneOf/anyOf of its own.
	UnionNone UnionMode = iota
	// UnionDiscriminated: variants are told apart by a shared tag field
	// carrying a distinct string const; at most one variant may lack the
	// tag; it is the default used when the tag is absent or unrecognized.
	UnionDiscriminated
	// UnionStructural: no tag field exists; variants are told apart by
	// which one's required JSON keys are present on the wire object.
	UnionStructural
	// UnionAlias: exactly one variant and no tag; the declaration is a
	// plain alias for that variant's type.
	UnionAlias
)

// EnumMember is one named value of a KindEnum declaration.
type EnumMember struct {
	Name  string // Go const identifier, e.g. ToolKindRead
	Value string // Go literal source for the const value, e.g. `"read"` or `-32700`
	Doc   string
}

// Field is one member of an object declaration's shared/base fields.
type Field struct {
	GoName   string
	JSONKey  string
	GoType   string
	Required bool
	Doc      string
	IsMeta   bool
	// DefaultLiteral is Go source for this field's schema default, or ""
	// when the field has no declared default. Only meaningful for structs
	// eligible for a generated Default() constructor (see TypeDecl.HasDefaults).
	DefaultLiteral string
}

// Variant is one alternative of an object declaration's union (if any).
type Variant struct {
	GoName     string // struct field name, e.g. "HTTP"
	Type       string // Go type name or literal type expression for the payload
	Tag        string // discriminator wire value; "" for the fallback variant
	IsFallback bool
	Doc        string
}

// TypeDecl is one generated top-level type.
type TypeDecl struct {
	Name       string
	Doc        string
	Kind       DeclKind
	Underlying string // KindScalar/KindEnum: Go base type
	Strict     bool   // KindEnum: reject unrecognized values on decode
	Members    []EnumMember
	Fields     []Field
	Variants   []Variant
	DiscKey    string // UnionDiscriminated: wire field name carrying the tag
	UnionMode  UnionMode
	AliasOf    string // UnionAlias: target type name
}

// HasDefaults reports whether this object declaration has at least one field
// with a schema-declared default and no required field lacking one, making a
// generated Default() constructor meaningful.
func (t TypeDecl) HasDefaults() bool {
	if t.Kind != KindObject || t.UnionMode != UnionNone {
		return false
	}
	any := false
	for _, f := range t.Fields {
		if f.Required && f.DefaultLiteral == "" && !f.IsMeta {
			return false
		}
		if f.DefaultLiteral != "" {
			any = true
		}
	}
	return any
}

// MethodEntry is one named wire method.
type MethodEntry struct {
	ConstName string // Go const identifier, e.g. MethodSessionNew
	Symbol    string // meta.json symbolic key, e.g. session_new
	Wire      string // wire method string, e.g. session/new
}

// MethodTable is the generated method-name vocabulary.
type MethodTable struct {
	Version  int
	Agent    []MethodEntry
	Client   []MethodEntry
	Protocol []MethodEntry
}

// Model is the fully resolved, Go-ready generator output.
type Model struct {
	Types   []TypeDecl
	Methods MethodTable
}

// BuildModel parses the pinned schema and meta JSON documents and resolves
// them into a deterministic Model. Output ordering never depends on Go map
// iteration order: $defs and meta.json method maps are sorted by key before
// emission; only JSON array order (oneOf/anyOf branches, required-field
// order as written) is preserved, which is itself fully determined by the
// pinned, committed input bytes.
func BuildModel(schemaJSON, metaJSON []byte) (*Model, error) {
	var doc SchemaDocument
	if err := json.Unmarshal(schemaJSON, &doc); err != nil {
		return nil, fmt.Errorf("gen: parse schema: %w", err)
	}
	var meta MetaDocument
	if err := json.Unmarshal(metaJSON, &meta); err != nil {
		return nil, fmt.Errorf("gen: parse meta: %w", err)
	}

	b := &builder{defs: doc.Defs}
	names := make([]string, 0, len(doc.Defs))
	for name := range doc.Defs {
		if excludedDefs[name] {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	types := make([]TypeDecl, 0, len(names))
	for _, name := range names {
		decl, err := b.buildDecl(name, doc.Defs[name])
		if err != nil {
			return nil, fmt.Errorf("gen: %s: %w", name, err)
		}
		types = append(types, decl)
	}

	methods := MethodTable{
		Version:  meta.Version,
		Agent:    buildMethodEntries(meta.AgentMethods),
		Client:   buildMethodEntries(meta.ClientMethods),
		Protocol: buildMethodEntries(meta.ProtocolMethods),
	}

	return &Model{Types: types, Methods: methods}, nil
}

func buildMethodEntries(m map[string]string) []MethodEntry {
	symbols := make([]string, 0, len(m))
	for sym := range m {
		symbols = append(symbols, sym)
	}
	sort.Strings(symbols)
	entries := make([]MethodEntry, 0, len(symbols))
	for _, sym := range symbols {
		entries = append(entries, MethodEntry{
			ConstName: "Method" + pascalFromSnake(sym),
			Symbol:    sym,
			Wire:      m[sym],
		})
	}
	return entries
}

// builder resolves $defs into TypeDecls, validating cross-references as it
// goes (an unresolved or excluded $ref target is a generator bug or a schema
// assumption violation; both fail the run rather than emit dubious code).
type builder struct {
	defs map[string]*Schema
}

func (b *builder) buildDecl(name string, s *Schema) (TypeDecl, error) {
	branches, key := s.OneOf, "oneOf"
	if branches == nil {
		branches, key = s.AnyOf, "anyOf"
	}

	if len(branches) == 0 {
		if len(s.Properties) > 0 {
			return b.buildObject(name, s)
		}
		return b.buildScalarAlias(name, s)
	}

	if allScalarBranches(branches) {
		return b.buildEnum(name, s, branches)
	}

	return b.buildUnionOrHybrid(name, s, branches, key)
}

func (b *builder) buildObject(name string, s *Schema) (TypeDecl, error) {
	fields, err := b.buildFields(name, s)
	if err != nil {
		return TypeDecl{}, err
	}
	return TypeDecl{
		Name:      goTypeName(name),
		Doc:       s.Description,
		Kind:      KindObject,
		Fields:    fields,
		UnionMode: UnionNone,
	}, nil
}

// buildFields resolves an object schema's own "properties"/"required" into
// deterministically ordered Fields: alphabetical by JSON key, with "_meta"
// (if present) always emitted last for readability.
func (b *builder) buildFields(name string, s *Schema) ([]Field, error) {
	required := map[string]bool{}
	for _, r := range s.Required {
		required[r] = true
	}
	keys := make([]string, 0, len(s.Properties))
	for k := range s.Properties {
		if k != "_meta" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	fields := make([]Field, 0, len(s.Properties))
	for _, k := range keys {
		f, err := b.resolveField(k, s.Properties[k], required[k])
		if err != nil {
			return nil, fmt.Errorf("field %s.%s: %w", name, k, err)
		}
		fields = append(fields, f)
	}
	if ms, ok := s.Properties["_meta"]; ok {
		fields = append(fields, Field{
			GoName:  "Meta",
			JSONKey: "_meta",
			GoType:  "json.RawMessage",
			IsMeta:  true,
			Doc:     ms.Description,
		})
	}
	return fields, nil
}

func (b *builder) resolveField(jsonKey string, ps *Schema, required bool) (Field, error) {
	baseType, nullable, err := b.resolveTypeExpr(ps)
	if err != nil {
		return Field{}, err
	}
	optional := !required || nullable
	goType := baseType
	// Optional (but not explicitly nullable) booleans skip the pointer: every
	// boolean default in the pinned schema is false, which is already Go's
	// zero value, so there is no "absent vs. explicit false" distinction
	// worth paying a pointer for. A field that is genuinely nullable
	// (explicit null in the schema) still gets a pointer even if boolean, to
	// keep null representable.
	skipPointerForBool := baseType == "bool" && !nullable
	if optional && needsPointer(baseType) && !skipPointerForBool {
		goType = "*" + baseType
	}
	defaultLit := ""
	if len(ps.Default) > 0 {
		defaultLit, err = b.defaultLiteral(baseType, ps.Default)
		if err != nil {
			return Field{}, fmt.Errorf("default: %w", err)
		}
	}
	return Field{
		GoName:         pascal(splitWords(jsonKey)),
		JSONKey:        jsonKey,
		GoType:         goType,
		Required:       required,
		Doc:            ps.Description,
		DefaultLiteral: defaultLit,
	}, nil
}

func needsPointer(goType string) bool {
	return !strings.HasPrefix(goType, "[]") && goType != "json.RawMessage"
}

// resolveTypeExpr resolves a property or array-item schema node to a Go type
// expression and whether the wire value may be null. It covers exactly the
// shapes present in the pinned schema: a $ref (bare, or the sole member of a
// single-element allOf), a nullable anyOf ([ref-or-type, null]), a nullable
// type array (["T","null"]), a plain array, a plain scalar, or (for fields
// like ToolCall.rawInput that carry no schema at all) an opaque JSON value.
func (b *builder) resolveTypeExpr(ps *Schema) (string, bool, error) {
	switch {
	case ps.Ref != "":
		t, err := b.resolveRef(ps.Ref)
		return t, false, err

	case len(ps.AllOf) == 1 && ps.AllOf[0].Ref != "":
		t, err := b.resolveRef(ps.AllOf[0].Ref)
		return t, false, err

	case len(ps.AnyOf) == 2 && hasNullBranch(ps.AnyOf):
		inner := nonNullBranch(ps.AnyOf)
		t, _, err := b.resolveTypeExpr(inner)
		return t, true, err

	case len(ps.Type) == 2 && containsNull(ps.Type):
		other := nonNullType(ps.Type)
		if other == "array" {
			item, _, err := b.resolveTypeExpr(ps.Items)
			return "[]" + item, true, err
		}
		t, _, err := b.resolveScalar(&Schema{Type: StringOrSlice{other}, Format: ps.Format})
		return t, true, err

	case len(ps.Type) == 1 && ps.Type[0] == "array":
		item, _, err := b.resolveTypeExpr(ps.Items)
		return "[]" + item, false, err

	case len(ps.Type) == 1:
		t, _, err := b.resolveScalar(ps)
		return t, false, err

	case ps.Ref == "" && len(ps.Type) == 0 && len(ps.AnyOf) == 0 && len(ps.AllOf) == 0 && len(ps.OneOf) == 0:
		return "json.RawMessage", false, nil

	default:
		return "", false, fmt.Errorf("unsupported field schema shape")
	}
}

func hasNullBranch(branches []*Schema) bool {
	n := 0
	for _, m := range branches {
		if len(m.Type) == 1 && m.Type[0] == "null" {
			n++
		}
	}
	return n == 1
}

func nonNullBranch(branches []*Schema) *Schema {
	for _, m := range branches {
		if !(len(m.Type) == 1 && m.Type[0] == "null") {
			return m
		}
	}
	return nil
}

func containsNull(types []string) bool {
	for _, t := range types {
		if t == "null" {
			return true
		}
	}
	return false
}

func nonNullType(types []string) string {
	for _, t := range types {
		if t != "null" {
			return t
		}
	}
	return ""
}

func (b *builder) resolveRef(ref string) (string, error) {
	const prefix = "#/$defs/"
	if !strings.HasPrefix(ref, prefix) {
		return "", fmt.Errorf("unsupported $ref %q", ref)
	}
	name := strings.TrimPrefix(ref, prefix)
	if _, ok := b.defs[name]; !ok {
		return "", fmt.Errorf("$ref to unknown def %q", name)
	}
	if excludedDefs[name] {
		return "", fmt.Errorf("$ref to excluded envelope def %q: domain types must not reference JSON-RPC envelope framing", name)
	}
	return goTypeName(name), nil
}

// defaultLiteral renders a schema "default" value as Go source for a
// Default() constructor field assignment. An empty return means the Go zero
// value already matches the default (booleans default false; the only
// observed non-object, non-boolean default in the pinned schema is an empty
// array, which is Go's nil slice).
func (b *builder) defaultLiteral(baseType string, raw json.RawMessage) (string, error) {
	trimmed := strings.TrimSpace(string(raw))
	switch trimmed {
	case "true", "false":
		return trimmed, nil
	case "[]", "null", "{}":
		// An empty-array/null/empty-object default all mean "the Go zero
		// value already matches": a nil slice, a nil pointer, or (for "{}")
		// a nested object whose own fields have no declared defaults of
		// their own, so it has no Default() constructor to call.
		return "", nil
	}
	if strings.HasPrefix(trimmed, "{") {
		return "Default" + baseType + "()", nil
	}
	return trimmed, nil
}

// buildUnionOrHybrid builds an object declaration whose type carries a
// oneOf/anyOf of its own. Three shapes are distinguished structurally:
//
//   - discriminated: at least one branch declares a property with a const
//     ("the tag"); every branch declaring the tag must use the same
//     property name. At most one branch may omit the tag — that branch is
//     the default used when the tag is absent from the wire object or set
//     to a value no branch declares (matches the pinned schema's own
//     "gracefully deserializes" language for McpServer/AuthMethod/
//     SetSessionConfigOptionRequest).
//   - structural: no branch anywhere declares a tag; branches are told
//     apart at decode time by which one's required JSON keys are present.
//   - alias: the structural case degenerates to exactly one branch — the
//     declaration is just a name for that branch's payload type.
//
// When the def also has its own "properties" (SessionConfigOption,
// SetSessionConfigOptionRequest), those become shared/base fields alongside
// the variants, flattened into one JSON object on the wire.
func (b *builder) buildUnionOrHybrid(name string, s *Schema, branches []*Schema, _ string) (TypeDecl, error) {
	baseFields, err := b.buildFields(name, s)
	if err != nil {
		return TypeDecl{}, err
	}

	discKey := ""
	for _, m := range branches {
		for pname, pschema := range m.Properties {
			if pschema.Const == nil {
				continue
			}
			if discKey == "" {
				discKey = pname
			} else if discKey != pname {
				return TypeDecl{}, fmt.Errorf("inconsistent discriminator property across branches: %q vs %q", discKey, pname)
			}
		}
	}

	if discKey == "" {
		if len(branches) == 1 {
			target, err := b.resolveVariantPayload(branches[0], "")
			if err != nil {
				return TypeDecl{}, fmt.Errorf("alias target: %w", err)
			}
			return TypeDecl{
				Name:      goTypeName(name),
				Doc:       s.Description,
				Kind:      KindObject,
				Fields:    baseFields,
				UnionMode: UnionAlias,
				AliasOf:   target,
			}, nil
		}

		variants := make([]Variant, 0, len(branches))
		for _, m := range branches {
			target, err := b.resolveVariantPayload(m, "")
			if err != nil {
				return TypeDecl{}, fmt.Errorf("structural variant: %w", err)
			}
			fname, err := variantFieldName("", m.Title)
			if err != nil {
				return TypeDecl{}, err
			}
			variants = append(variants, Variant{GoName: fname, Type: target, Doc: m.Description})
		}
		return TypeDecl{
			Name:      goTypeName(name),
			Doc:       s.Description,
			Kind:      KindObject,
			Fields:    baseFields,
			Variants:  variants,
			UnionMode: UnionStructural,
		}, nil
	}

	variants := make([]Variant, 0, len(branches))
	fallbackSeen := false
	for _, m := range branches {
		tag := ""
		hasTag := false
		if tagSchema, ok := m.Properties[discKey]; ok && tagSchema.Const != nil {
			if err := json.Unmarshal(tagSchema.Const, &tag); err != nil {
				return TypeDecl{}, fmt.Errorf("discriminator const: %w", err)
			}
			hasTag = true
		}
		target, err := b.resolveVariantPayload(m, discKey)
		if err != nil {
			return TypeDecl{}, fmt.Errorf("discriminated variant %q: %w", tag, err)
		}
		fname, err := variantFieldName(tag, m.Title)
		if err != nil {
			return TypeDecl{}, err
		}
		if !hasTag {
			if fallbackSeen {
				return TypeDecl{}, fmt.Errorf("more than one fallback (no-tag) branch")
			}
			fallbackSeen = true
		}
		variants = append(variants, Variant{
			GoName:     fname,
			Type:       target,
			Tag:        tag,
			IsFallback: !hasTag,
			Doc:        m.Description,
		})
	}
	return TypeDecl{
		Name:      goTypeName(name),
		Doc:       s.Description,
		Kind:      KindObject,
		Fields:    baseFields,
		Variants:  variants,
		DiscKey:   discKey,
		UnionMode: UnionDiscriminated,
	}, nil
}

// resolveVariantPayload resolves one union branch's payload type: a
// referenced named type (branch.allOf==[{$ref}] or a bare {$ref}), or — for
// branches with fields declared inline instead of via a $ref — the type of
// its single non-discriminator property. The pinned schema never declares
// more than one such inline field per branch; that shape is rejected rather
// than silently handled.
func (b *builder) resolveVariantPayload(branch *Schema, discKey string) (string, error) {
	switch {
	case len(branch.AllOf) == 1 && branch.AllOf[0].Ref != "":
		return b.resolveRef(branch.AllOf[0].Ref)
	case branch.Ref != "":
		return b.resolveRef(branch.Ref)
	case len(branch.Type) == 1 && branch.Type[0] == "array":
		// A branch that is itself an array (e.g. SessionConfigSelectOptions:
		// a flat list of options vs. a list of groups) — the payload is a
		// slice, not an object; emit.go's structural codec dispatches on
		// the first element's shape rather than the top-level object's keys.
		item, _, err := b.resolveTypeExpr(branch.Items)
		if err != nil {
			return "", err
		}
		return "[]" + item, nil
	default:
		var extra []string
		for pname := range branch.Properties {
			if pname == discKey {
				continue
			}
			extra = append(extra, pname)
		}
		sort.Strings(extra)
		switch len(extra) {
		case 0:
			return "struct{}", nil
		case 1:
			t, _, err := b.resolveTypeExpr(branch.Properties[extra[0]])
			return t, err
		default:
			return "", fmt.Errorf("inline union branch with multiple payload fields is unsupported")
		}
	}
}

func variantFieldName(tag, title string) (string, error) {
	if tag != "" {
		return pascal(splitWords(tag)), nil
	}
	if title != "" {
		return pascal(splitWords(title)), nil
	}
	return "", fmt.Errorf("union variant has neither a discriminator tag nor a title to name its field")
}

func allScalarBranches(branches []*Schema) bool {
	for _, m := range branches {
		if len(m.Type) != 1 {
			return false
		}
		switch m.Type[0] {
		case "string", "integer", "number", "boolean":
		default:
			return false
		}
	}
	return true
}

func (b *builder) buildScalarAlias(name string, s *Schema) (TypeDecl, error) {
	goType, _, err := b.resolveScalar(s)
	if err != nil {
		return TypeDecl{}, err
	}
	return TypeDecl{
		Name:       goTypeName(name),
		Doc:        s.Description,
		Kind:       KindScalar,
		Underlying: goType,
	}, nil
}

func (b *builder) resolveScalar(s *Schema) (goType string, nullable bool, err error) {
	if len(s.Type) == 0 {
		return "", false, fmt.Errorf("scalar def has no type")
	}
	if len(s.Type) == 2 {
		nullable = true
	}
	var t string
	for _, cand := range s.Type {
		if cand != "null" {
			t = cand
		}
	}
	switch t {
	case "string":
		return "string", nullable, nil
	case "boolean":
		return "bool", nullable, nil
	case "number":
		return "float64", nullable, nil
	case "integer":
		switch s.Format {
		case "uint16":
			return "uint16", nullable, nil
		case "uint32":
			return "uint32", nullable, nil
		case "uint64":
			return "uint64", nullable, nil
		case "int32":
			return "int32", nullable, nil
		case "int64", "":
			return "int64", nullable, nil
		default:
			return "", false, fmt.Errorf("integer: unsupported format %q", s.Format)
		}
	default:
		return "", false, fmt.Errorf("unsupported scalar type %q", t)
	}
}

// buildEnum handles both closed enums (every branch carries a const — reject
// unrecognized values on decode) and open enums (at least one branch has no
// const, meaning the wire value space is intentionally larger than the named
// set — e.g. ErrorCode's reserved application-error range, or
// SessionConfigOptionCategory's forward-compatible "other" category). Open
// enums get named constants for their known values but no validating
// UnmarshalJSON: any value of the underlying scalar type is accepted.
func (b *builder) buildEnum(name string, s *Schema, branches []*Schema) (TypeDecl, error) {
	underlying, _, err := b.resolveScalar(branches[0])
	if err != nil {
		return TypeDecl{}, fmt.Errorf("enum member: %w", err)
	}
	strict := true
	members := make([]EnumMember, 0, len(branches))
	for _, m := range branches {
		if m.Const == nil {
			strict = false
			continue
		}
		lit, err := constGoLiteral(underlying, m.Const)
		if err != nil {
			return TypeDecl{}, err
		}
		nameSource := m.Title
		if nameSource == "" {
			if underlying != "string" {
				return TypeDecl{}, fmt.Errorf("enum member has no title and a non-string const to derive a name from")
			}
			if err := json.Unmarshal(m.Const, &nameSource); err != nil {
				return TypeDecl{}, err
			}
		}
		members = append(members, EnumMember{
			Name:  goTypeName(name) + pascal(splitWords(nameSource)),
			Value: lit,
			Doc:   m.Description,
		})
	}
	return TypeDecl{
		Name:       goTypeName(name),
		Doc:        s.Description,
		Kind:       KindEnum,
		Underlying: underlying,
		Strict:     strict,
		Members:    members,
	}, nil
}

func constGoLiteral(underlying string, raw json.RawMessage) (string, error) {
	switch underlying {
	case "string":
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", err
		}
		return fmt.Sprintf("%q", s), nil
	default:
		var n int64
		if err := json.Unmarshal(raw, &n); err != nil {
			return "", err
		}
		return fmt.Sprintf("%d", n), nil
	}
}

func goTypeName(defName string) string {
	return pascal(splitWords(defName))
}

// splitWords tokenizes a JSON Schema identifier fragment written in any of
// camelCase, PascalCase, snake_case, or free-form Title text into lowercase
// words, so the same casing pipeline can turn "sessionId", "SessionId",
// "switch_mode", and "Parse error" all into consistent Go identifiers.
func splitWords(s string) []string {
	var words []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			words = append(words, strings.ToLower(cur.String()))
			cur.Reset()
		}
	}
	runes := []rune(s)
	for i, r := range runes {
		switch {
		case r == '_' || r == '-' || r == ' ' || r == '.' || r == '/':
			flush()
		case r >= 'A' && r <= 'Z':
			if i > 0 {
				prev := runes[i-1]
				prevLower := prev >= 'a' && prev <= 'z'
				nextLower := i+1 < len(runes) && runes[i+1] >= 'a' && runes[i+1] <= 'z'
				if prevLower || (cur.Len() > 0 && nextLower && isUpper(prev)) {
					flush()
				}
			}
			cur.WriteRune(r)
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	return words
}

func isUpper(r rune) bool { return r >= 'A' && r <= 'Z' }

// initialisms are the Go-conventional all-caps renderings for common
// acronyms that occur in ACP identifiers (matches staticcheck's ST1003 known
// initialism list for the subset actually used by this schema).
var initialisms = map[string]string{
	"id":   "ID",
	"url":  "URL",
	"uri":  "URI",
	"http": "HTTP",
	"api":  "API",
}

func pascal(words []string) string {
	var sb strings.Builder
	for _, w := range words {
		if up, ok := initialisms[w]; ok {
			sb.WriteString(up)
			continue
		}
		if w == "" {
			continue
		}
		sb.WriteString(strings.ToUpper(w[:1]))
		sb.WriteString(w[1:])
	}
	return sb.String()
}

func pascalFromSnake(s string) string { return pascal(splitWords(s)) }
