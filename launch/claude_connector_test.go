package launch

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/looprig/acp/protocol"
)

func categoryPtr(c protocol.SessionConfigOptionCategory) *protocol.SessionConfigOptionCategory {
	return &c
}

func modelOptionWithID(id protocol.SessionConfigID, values ...string) protocol.SessionConfigOption {
	opts := make([]protocol.SessionConfigSelectOption, len(values))
	for i, v := range values {
		opts[i] = protocol.SessionConfigSelectOption{Name: v, Value: protocol.SessionConfigValueID(v)}
	}
	return protocol.SessionConfigOption{
		ID:       id,
		Name:     "Model",
		Category: categoryPtr(protocol.SessionConfigOptionCategoryModel),
		Select: &protocol.SessionConfigSelect{
			CurrentValue: protocol.SessionConfigValueID(values[0]),
			Options:      protocol.SessionConfigSelectOptions{Ungrouped: opts},
		},
	}
}

// fakeSession implements sessionConfigurer (see claude_connector.go) so
// selectModel/applyPermissionMode -- the exact internal functions
// SelectDefaultModel/SelectSmallModel/ApplyPermissionMode delegate to --
// can be exercised directly against scripted config options/modes, without
// dialing a real ACP session at all. *client.Session satisfies the same
// interface structurally, so this proves the real production logic, not a
// parallel reimplementation of it.
type fakeSession struct {
	mu            sync.Mutex
	configOptions []protocol.SessionConfigOption
	modes         *protocol.SessionModeState

	setConfigCalls []setConfigCall
	setConfigErr   error
	setModeCalls   []protocol.SessionModeID
	setModeErr     error
}

type setConfigCall struct {
	ConfigID protocol.SessionConfigID
	ValueID  protocol.SessionConfigValueID
}

func (f *fakeSession) ConfigOptions() []protocol.SessionConfigOption { return f.configOptions }
func (f *fakeSession) Modes() *protocol.SessionModeState             { return f.modes }

func (f *fakeSession) SetConfigOption(ctx context.Context, configID protocol.SessionConfigID, valueID protocol.SessionConfigValueID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setConfigCalls = append(f.setConfigCalls, setConfigCall{ConfigID: configID, ValueID: valueID})
	return f.setConfigErr
}

func (f *fakeSession) SetMode(ctx context.Context, modeID protocol.SessionModeID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setModeCalls = append(f.setModeCalls, modeID)
	return f.setModeErr
}

// --- configOptionID: standard id vs legacy configId-in-_meta fallback ---

func TestConfigOptionIDPrefersStandardID(t *testing.T) {
	opt := protocol.SessionConfigOption{ID: "model"}
	id, ok := configOptionID(opt)
	if !ok || id != "model" {
		t.Fatalf("configOptionID() = (%q, %v), want (\"model\", true)", id, ok)
	}
}

func TestConfigOptionIDFallsBackToLegacyConfigIDInMeta(t *testing.T) {
	opt := protocol.SessionConfigOption{
		ID:   "", // spec field absent/empty: the legacy-adapter case
		Meta: json.RawMessage(`{"configId":"model"}`),
	}
	id, ok := configOptionID(opt)
	if !ok || id != "model" {
		t.Fatalf("configOptionID() = (%q, %v), want (\"model\", true) via the _meta.configId fallback", id, ok)
	}
}

func TestConfigOptionIDFailsWhenNeitherPresent(t *testing.T) {
	for name, opt := range map[string]protocol.SessionConfigOption{
		"no id, no meta":            {},
		"empty meta object":         {Meta: json.RawMessage(`{}`)},
		"meta present, no configId": {Meta: json.RawMessage(`{"other":"x"}`)},
		"configId is empty string":  {Meta: json.RawMessage(`{"configId":""}`)},
		"configId is not a string":  {Meta: json.RawMessage(`{"configId":42}`)},
		"meta is not an object":     {Meta: json.RawMessage(`[1,2,3]`)},
		"meta is malformed json":    {Meta: json.RawMessage(`{not-json`)},
	} {
		t.Run(name, func(t *testing.T) {
			if id, ok := configOptionID(opt); ok {
				t.Fatalf("configOptionID() = (%q, true), want ok=false", id)
			}
		})
	}
}

// --- select only category "model" ---

func TestFindModelOptionSelectsOnlyModelCategory(t *testing.T) {
	mode := protocol.SessionConfigOption{
		ID:       "mode",
		Category: categoryPtr(protocol.SessionConfigOptionCategoryMode),
		Select:   &protocol.SessionConfigSelect{Options: protocol.SessionConfigSelectOptions{Ungrouped: []protocol.SessionConfigSelectOption{{Value: "plan"}}}},
	}
	thoughtLevel := protocol.SessionConfigOption{
		ID:       "thought",
		Category: categoryPtr(protocol.SessionConfigOptionCategoryThoughtLevel),
		Select:   &protocol.SessionConfigSelect{Options: protocol.SessionConfigSelectOptions{Ungrouped: []protocol.SessionConfigSelectOption{{Value: "high"}}}},
	}
	uncategorized := protocol.SessionConfigOption{
		ID:     "misc",
		Select: &protocol.SessionConfigSelect{Options: protocol.SessionConfigSelectOptions{Ungrouped: []protocol.SessionConfigSelectOption{{Value: "x"}}}},
	}
	model := modelOptionWithID("model", "sonnet", "opus")

	opts := []protocol.SessionConfigOption{mode, thoughtLevel, uncategorized, model}

	got, id, ok := findModelOption(opts)
	if !ok {
		t.Fatal("findModelOption() ok = false, want true")
	}
	if id != "model" {
		t.Errorf("findModelOption() id = %q, want %q", id, "model")
	}
	if got.Category == nil || *got.Category != protocol.SessionConfigOptionCategoryModel {
		t.Errorf("findModelOption() returned option with category %v, want %q", got.Category, protocol.SessionConfigOptionCategoryModel)
	}
}

func TestFindModelOptionAbsentWithoutModelCategory(t *testing.T) {
	opts := []protocol.SessionConfigOption{
		{ID: "mode", Category: categoryPtr(protocol.SessionConfigOptionCategoryMode), Select: &protocol.SessionConfigSelect{}},
	}
	if _, _, ok := findModelOption(opts); ok {
		t.Fatal("findModelOption() ok = true, want false: no option advertises category model")
	}
}

func TestFindModelOptionIgnoresBooleanVariantEvenIfCategoryModel(t *testing.T) {
	boolOpt := protocol.SessionConfigOption{
		ID:       "model",
		Category: categoryPtr(protocol.SessionConfigOptionCategoryModel),
		Boolean:  &protocol.SessionConfigBoolean{CurrentValue: true},
	}
	if _, _, ok := findModelOption([]protocol.SessionConfigOption{boolOpt}); ok {
		t.Fatal("findModelOption() ok = true, want false: a boolean-variant option is never a model selector")
	}
}

// --- resolveModelSelection ---

func TestResolveModelSelectionMatchesAdvertisedValue(t *testing.T) {
	opts := []protocol.SessionConfigOption{modelOptionWithID("model", "sonnet", "opus")}

	configID, valueID, ok := resolveModelSelection(opts, "opus")
	if !ok {
		t.Fatal("resolveModelSelection() ok = false, want true")
	}
	if configID != "model" || valueID != "opus" {
		t.Errorf("resolveModelSelection() = (%q, %q), want (\"model\", \"opus\")", configID, valueID)
	}
}

func TestResolveModelSelectionMatchesGroupedValues(t *testing.T) {
	opt := protocol.SessionConfigOption{
		ID:       "model",
		Category: categoryPtr(protocol.SessionConfigOptionCategoryModel),
		Select: &protocol.SessionConfigSelect{
			Options: protocol.SessionConfigSelectOptions{
				Grouped: []protocol.SessionConfigSelectGroup{
					{Group: "anthropic", Name: "Anthropic", Options: []protocol.SessionConfigSelectOption{{Value: "opus"}, {Value: "sonnet"}}},
				},
			},
		},
	}
	_, valueID, ok := resolveModelSelection([]protocol.SessionConfigOption{opt}, "sonnet")
	if !ok || valueID != "sonnet" {
		t.Fatalf("resolveModelSelection() = (%q, %v), want (\"sonnet\", true)", valueID, ok)
	}
}

func TestResolveModelSelectionFailsForUnmatchedAlias(t *testing.T) {
	opts := []protocol.SessionConfigOption{modelOptionWithID("model", "sonnet", "opus")}

	if _, _, ok := resolveModelSelection(opts, "does-not-exist"); ok {
		t.Fatal("resolveModelSelection() ok = true, want false for an alias absent from the advertised values")
	}
}

func TestResolveModelSelectionFailsWithNoModelOptionAtAll(t *testing.T) {
	if _, _, ok := resolveModelSelection(nil, "anything"); ok {
		t.Fatal("resolveModelSelection() ok = true, want false with no config options at all")
	}
}

// --- selectModel: the real logic SelectDefaultModel/SelectSmallModel call ---

func TestSelectModelSendsResolvedConfigAndValueID(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "opus"})
	sess := &fakeSession{configOptions: []protocol.SessionConfigOption{modelOptionWithID("model", "sonnet", "opus")}}

	if err := c.selectModel(context.Background(), sess, "opus"); err != nil {
		t.Fatalf("selectModel() error = %v", err)
	}

	if len(sess.setConfigCalls) != 1 {
		t.Fatalf("SetConfigOption called %d times, want 1", len(sess.setConfigCalls))
	}
	got := sess.setConfigCalls[0]
	if got.ConfigID != "model" || got.ValueID != "opus" {
		t.Errorf("SetConfigOption(%q, %q), want (\"model\", \"opus\")", got.ConfigID, got.ValueID)
	}
}

func TestSelectModelReturnsTypedAliasMismatchWithoutCallingSetConfigOption(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "haiku"})
	sess := &fakeSession{configOptions: []protocol.SessionConfigOption{modelOptionWithID("model", "sonnet", "opus")}}

	err := c.selectModel(context.Background(), sess, "haiku")

	var aliasErr *ModelAliasError
	if !errors.As(err, &aliasErr) {
		t.Fatalf("selectModel() error = %v (%T), want *ModelAliasError", err, err)
	}
	if aliasErr.Alias != "haiku" {
		t.Errorf("ModelAliasError.Alias = %q, want %q", aliasErr.Alias, "haiku")
	}
	if len(sess.setConfigCalls) != 0 {
		t.Errorf("SetConfigOption called %d times, want 0: an unmatched alias must never reach the wire", len(sess.setConfigCalls))
	}
}

func TestSelectModelReturnsTypedAliasMismatchWhenNoModelOptionAdvertised(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "opus"})
	sess := &fakeSession{} // no config options at all

	err := c.selectModel(context.Background(), sess, "opus")
	var aliasErr *ModelAliasError
	if !errors.As(err, &aliasErr) {
		t.Fatalf("selectModel() error = %v (%T), want *ModelAliasError", err, err)
	}
}

func TestSelectDefaultAndSmallModelUseTheirOwnAlias(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "opus", Small: "haiku"})
	opts := []protocol.SessionConfigOption{modelOptionWithID("model", "haiku", "sonnet", "opus")}

	sessDefault := &fakeSession{configOptions: opts}
	if err := c.selectModel(context.Background(), sessDefault, c.Models.Default); err != nil {
		t.Fatalf("selectModel(Default) error = %v", err)
	}
	if got := sessDefault.setConfigCalls[0].ValueID; got != "opus" {
		t.Errorf("Default selection applied value %q, want %q", got, "opus")
	}

	sessSmall := &fakeSession{configOptions: opts}
	if err := c.selectModel(context.Background(), sessSmall, c.Models.Small); err != nil {
		t.Fatalf("selectModel(Small) error = %v", err)
	}
	if got := sessSmall.setConfigCalls[0].ValueID; got != "haiku" {
		t.Errorf("Small selection applied value %q, want %q", got, "haiku")
	}
}

func TestSelectModelPropagatesSetConfigOptionError(t *testing.T) {
	c := ClaudeCode(ClaudeModels{Default: "opus"})
	wantErr := errors.New("wire boom")
	sess := &fakeSession{
		configOptions: []protocol.SessionConfigOption{modelOptionWithID("model", "sonnet", "opus")},
		setConfigErr:  wantErr,
	}

	if err := c.selectModel(context.Background(), sess, "opus"); !errors.Is(err, wantErr) {
		t.Fatalf("selectModel() error = %v, want to wrap %v", err, wantErr)
	}
}

// --- applyPermissionMode: the real logic ApplyPermissionMode calls ---

func TestApplyPermissionModeSetsOnlyAdvertisedMode(t *testing.T) {
	sess := &fakeSession{modes: &protocol.SessionModeState{
		CurrentModeID:  "default",
		AvailableModes: []protocol.SessionMode{{ID: "default"}, {ID: "acceptEdits"}},
	}}

	if err := applyPermissionMode(context.Background(), sess, "acceptEdits"); err != nil {
		t.Fatalf("applyPermissionMode() error = %v", err)
	}
	if len(sess.setModeCalls) != 1 || sess.setModeCalls[0] != "acceptEdits" {
		t.Errorf("SetMode calls = %v, want exactly [\"acceptEdits\"]", sess.setModeCalls)
	}
}

func TestApplyPermissionModeSkipsUnadvertisedModeWithoutError(t *testing.T) {
	sess := &fakeSession{modes: &protocol.SessionModeState{
		CurrentModeID:  "default",
		AvailableModes: []protocol.SessionMode{{ID: "default"}},
	}}

	if err := applyPermissionMode(context.Background(), sess, "bypassPermissions"); err != nil {
		t.Fatalf("applyPermissionMode() error = %v, want nil (silent no-op for an unadvertised mode)", err)
	}
	if len(sess.setModeCalls) != 0 {
		t.Errorf("SetMode called %d times, want 0: an unadvertised mode must never reach the wire", len(sess.setModeCalls))
	}
}

func TestApplyPermissionModeNoOpsWithNoCachedModesAtAll(t *testing.T) {
	sess := &fakeSession{} // Modes() returns nil

	if err := applyPermissionMode(context.Background(), sess, "plan"); err != nil {
		t.Fatalf("applyPermissionMode() error = %v, want nil", err)
	}
	if len(sess.setModeCalls) != 0 {
		t.Errorf("SetMode called %d times, want 0", len(sess.setModeCalls))
	}
}

func TestApplyPermissionModePropagatesSetModeError(t *testing.T) {
	wantErr := errors.New("wire boom")
	sess := &fakeSession{
		modes:      &protocol.SessionModeState{AvailableModes: []protocol.SessionMode{{ID: "plan"}}},
		setModeErr: wantErr,
	}

	if err := applyPermissionMode(context.Background(), sess, "plan"); !errors.Is(err, wantErr) {
		t.Fatalf("applyPermissionMode() error = %v, want to wrap %v", err, wantErr)
	}
}
