// config_internal_test.go covers Task 9's session configuration surface:
// Session.ConfigOptions/Modes (defensive copies of session/new's response,
// replaced by SetConfigOption/SetMode's own effects), and the gated
// session/set_model extension (Client.ProveSetModelCapability +
// Session.SetModel).
package client

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

// roundTripJSON returns v after marshaling then unmarshaling it once,
// mirroring what a value actually looks like after crossing the wire
// (fakeAgent's handlers, like a real ACP peer, are reached through a real
// JSON-RPC round trip over net.Pipe — see dialTestClient). This matters for
// protocol.SessionConfigOption in particular: its custom MarshalJSON always
// emits an explicit `"_meta": null` when Meta is nil (unlike types relying
// on the default struct-tag `omitempty` encoding), so a nil-Meta value's
// shape after a real round trip differs from the value as directly
// constructed in Go — comparing a freshly-constructed "want" value against
// a Session's actually-round-tripped state would otherwise fail on that
// incidental difference rather than on anything this package is
// responsible for.
func roundTripJSON[T any](v T) T {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		panic(err)
	}
	return out
}

func sampleConfigOptions() []protocol.SessionConfigOption {
	return []protocol.SessionConfigOption{
		{
			ID:   "model",
			Name: "Model",
			Select: &protocol.SessionConfigSelect{
				CurrentValue: "sonnet",
				Options: protocol.SessionConfigSelectOptions{
					Ungrouped: []protocol.SessionConfigSelectOption{
						{Name: "Sonnet", Value: "sonnet"},
						{Name: "Opus", Value: "opus"},
					},
				},
			},
		},
	}
}

func sampleModeState() *protocol.SessionModeState {
	return &protocol.SessionModeState{
		CurrentModeID: "default",
		AvailableModes: []protocol.SessionMode{
			{ID: "default", Name: "Default"},
			{ID: "plan", Name: "Plan"},
		},
	}
}

// TestInitializeMetadataPreservesAbsentOptionalFields proves a successful
// initialize response with no optional agent information or raw metadata is
// represented by nil fields, rather than fabricated empty values.
func TestInitializeMetadataPreservesAbsentOptionalFields(t *testing.T) {
	client, _ := dialTestClient(t, Options{}, func(fa *fakeAgent) {
		fa.onInitialize = func(req protocol.InitializeRequest) (protocol.InitializeResponse, error) {
			return protocol.InitializeResponse{ProtocolVersion: protocol.CurrentProtocolVersion}, nil
		}
	})

	got, err := client.InitializeMetadata()
	if err != nil {
		t.Fatalf("InitializeMetadata() error = %v", err)
	}
	if got.AgentInfo != nil {
		t.Fatalf("InitializeMetadata().AgentInfo = %#v, want nil", got.AgentInfo)
	}
	if got.Meta != nil {
		t.Fatalf("InitializeMetadata().Meta = %#v, want nil", got.Meta)
	}
}

// TestNewSessionRetainsDefensiveCopyOfConfigOptionsAndModes proves NewSession
// stores its own copy of the session/new response's ConfigOptions/Modes
// (readable via ConfigOptions()/Modes()) and that mutating the response
// value, or the slice/pointer this test still holds, after the call returns
// never mutates the Session's internal state.
func TestNewSessionRetainsDefensiveCopyOfConfigOptionsAndModes(t *testing.T) {
	c, fa := dialTestClient(t, Options{})

	wantOptions := roundTripJSON(sampleConfigOptions())
	wantModes := sampleModeState()
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		return protocol.NewSessionResponse{
			SessionID:     "sess-cfg",
			ConfigOptions: sampleConfigOptions(),
			Modes:         sampleModeState(),
		}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := c.NewSession(ctx, NewSessionParams{Cwd: "/work"})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	gotOptions := sess.ConfigOptions()
	if !reflect.DeepEqual(gotOptions, wantOptions) {
		t.Errorf("ConfigOptions() = %#v, want %#v", gotOptions, wantOptions)
	}
	gotModes := sess.Modes()
	if !reflect.DeepEqual(gotModes, wantModes) {
		t.Errorf("Modes() = %#v, want %#v", gotModes, wantModes)
	}

	// Mutating the slice/pointer returned by the accessors must never affect
	// the Session's own state (defensive copy on read).
	gotOptions[0].Name = "mutated"
	_ = append(gotOptions, protocol.SessionConfigOption{ID: "extra"})
	gotModes.CurrentModeID = "mutated"
	gotModes.AvailableModes = append(gotModes.AvailableModes, protocol.SessionMode{ID: "extra"})

	againOptions := sess.ConfigOptions()
	if !reflect.DeepEqual(againOptions, wantOptions) {
		t.Errorf("ConfigOptions() after caller mutated a prior read = %#v, want unaffected %#v", againOptions, wantOptions)
	}
	againModes := sess.Modes()
	if !reflect.DeepEqual(againModes, wantModes) {
		t.Errorf("Modes() after caller mutated a prior read = %#v, want unaffected %#v", againModes, wantModes)
	}
}

// TestSessionConfigOptionsAndModesNilWithoutAgentSupport proves a Session
// whose session/new response carried no ConfigOptions/Modes at all exposes
// nil from both accessors, rather than an empty-but-non-nil slice/pointer
// that would misleadingly suggest the agent advertised (and returned) zero
// options.
func TestSessionConfigOptionsAndModesNilWithoutAgentSupport(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		return protocol.NewSessionResponse{SessionID: "sess-no-cfg"}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := c.NewSession(ctx, NewSessionParams{Cwd: "/work"})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	if got := sess.ConfigOptions(); got != nil {
		t.Errorf("ConfigOptions() = %#v, want nil", got)
	}
	if got := sess.Modes(); got != nil {
		t.Errorf("Modes() = %#v, want nil", got)
	}
}

// TestSetConfigOptionSendsExactRequestAndReplacesLocalState proves
// SetConfigOption sends the exact sessionId/configId/value discriminator
// over the wire (the value-id variant only: Boolean must be nil, ValueID
// must be set) and replaces this Session's cached ConfigOptions wholesale
// with the response's own set.
func TestSetConfigOptionSendsExactRequestAndReplacesLocalState(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		return protocol.NewSessionResponse{SessionID: "sess-set-cfg", ConfigOptions: sampleConfigOptions()}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := c.NewSession(ctx, NewSessionParams{Cwd: "/work"})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	updated := []protocol.SessionConfigOption{
		{
			ID:   "model",
			Name: "Model",
			Select: &protocol.SessionConfigSelect{
				CurrentValue: "opus",
				Options: protocol.SessionConfigSelectOptions{
					Ungrouped: []protocol.SessionConfigSelectOption{
						{Name: "Sonnet", Value: "sonnet"},
						{Name: "Opus", Value: "opus"},
					},
				},
			},
		},
	}

	wantUpdated := roundTripJSON(updated)

	var gotReq protocol.SetSessionConfigOptionRequest
	fa.onSetConfigOption = func(req protocol.SetSessionConfigOptionRequest) (protocol.SetSessionConfigOptionResponse, error) {
		gotReq = req
		return protocol.SetSessionConfigOptionResponse{ConfigOptions: updated}, nil
	}

	if err := sess.SetConfigOption(ctx, "model", "opus"); err != nil {
		t.Fatalf("SetConfigOption() error = %v", err)
	}

	if gotReq.SessionID != sess.ID() {
		t.Errorf("SetSessionConfigOptionRequest.SessionID = %q, want %q", gotReq.SessionID, sess.ID())
	}
	if gotReq.ConfigID != "model" {
		t.Errorf("SetSessionConfigOptionRequest.ConfigID = %q, want %q", gotReq.ConfigID, "model")
	}
	if gotReq.Boolean != nil {
		t.Errorf("SetSessionConfigOptionRequest.Boolean = %v, want nil (value-id variant only)", *gotReq.Boolean)
	}
	if gotReq.ValueID == nil || *gotReq.ValueID != "opus" {
		t.Errorf("SetSessionConfigOptionRequest.ValueID = %v, want *\"opus\"", gotReq.ValueID)
	}

	if got := sess.ConfigOptions(); !reflect.DeepEqual(got, wantUpdated) {
		t.Errorf("ConfigOptions() after SetConfigOption = %#v, want %#v", got, wantUpdated)
	}
}

// TestSetConfigOptionPropagatesAgentErrorAndLeavesLocalStateUnchanged proves
// a failed session/set_config_option call surfaces the agent's error and
// never overwrites the Session's previously cached ConfigOptions.
func TestSetConfigOptionPropagatesAgentErrorAndLeavesLocalStateUnchanged(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	original := sampleConfigOptions()
	wantOriginal := roundTripJSON(original)
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		return protocol.NewSessionResponse{SessionID: "sess-cfg-err", ConfigOptions: original}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := c.NewSession(ctx, NewSessionParams{Cwd: "/work"})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	fa.onSetConfigOption = func(req protocol.SetSessionConfigOptionRequest) (protocol.SetSessionConfigOptionResponse, error) {
		return protocol.SetSessionConfigOptionResponse{}, protocol.InvalidParams("bad configId", nil)
	}

	if err := sess.SetConfigOption(ctx, "model", "opus"); err == nil {
		t.Fatal("SetConfigOption() error = nil, want the agent's InvalidParams fault")
	}

	if got := sess.ConfigOptions(); !reflect.DeepEqual(got, wantOriginal) {
		t.Errorf("ConfigOptions() after a failed SetConfigOption = %#v, want unchanged %#v", got, wantOriginal)
	}
}

// TestSetModeSendsExactRequestAndUpdatesLocalCurrentMode proves SetMode
// sends the exact sessionId/modeId over the wire and, since
// session/set_mode's own response carries no state, locally replaces just
// CurrentModeID (preserving AvailableModes) on success.
func TestSetModeSendsExactRequestAndUpdatesLocalCurrentMode(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		return protocol.NewSessionResponse{SessionID: "sess-mode", Modes: sampleModeState()}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := c.NewSession(ctx, NewSessionParams{Cwd: "/work"})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	var gotReq protocol.SetSessionModeRequest
	fa.onSetMode = func(req protocol.SetSessionModeRequest) (protocol.SetSessionModeResponse, error) {
		gotReq = req
		return protocol.SetSessionModeResponse{}, nil
	}

	if err := sess.SetMode(ctx, "plan"); err != nil {
		t.Fatalf("SetMode() error = %v", err)
	}

	if gotReq.SessionID != sess.ID() {
		t.Errorf("SetSessionModeRequest.SessionID = %q, want %q", gotReq.SessionID, sess.ID())
	}
	if gotReq.ModeID != "plan" {
		t.Errorf("SetSessionModeRequest.ModeID = %q, want %q", gotReq.ModeID, "plan")
	}

	want := sampleModeState()
	want.CurrentModeID = "plan"
	if got := sess.Modes(); !reflect.DeepEqual(got, want) {
		t.Errorf("Modes() after SetMode = %#v, want %#v", got, want)
	}
}

// TestSetModeWithNoCachedModesRecordsMinimalState proves SetMode still
// records the confirmed mode change even when this Session had no cached
// mode state at all beforehand (e.g. a LoadSession/ResumeSession-created
// Session), rather than silently discarding it.
func TestSetModeWithNoCachedModesRecordsMinimalState(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	fa.onResume = func(req protocol.ResumeSessionRequest) (protocol.ResumeSessionResponse, error) {
		return protocol.ResumeSessionResponse{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := c.ResumeSession(ctx, ResumeSessionParams{SessionID: "sess-resume-mode", Cwd: "/work"})
	if err != nil {
		t.Fatalf("ResumeSession() error = %v", err)
	}
	if got := sess.Modes(); got != nil {
		t.Fatalf("Modes() before any SetMode = %#v, want nil", got)
	}

	if err := sess.SetMode(ctx, "plan"); err != nil {
		t.Fatalf("SetMode() error = %v", err)
	}

	want := &protocol.SessionModeState{CurrentModeID: "plan"}
	if got := sess.Modes(); !reflect.DeepEqual(got, want) {
		t.Errorf("Modes() after SetMode with no prior cache = %#v, want %#v", got, want)
	}
}

// TestSetModePropagatesAgentErrorAndLeavesLocalStateUnchanged proves a
// failed session/set_mode call surfaces the agent's error and never
// updates the Session's cached CurrentModeID.
func TestSetModePropagatesAgentErrorAndLeavesLocalStateUnchanged(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		return protocol.NewSessionResponse{SessionID: "sess-mode-err", Modes: sampleModeState()}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := c.NewSession(ctx, NewSessionParams{Cwd: "/work"})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}

	fa.onSetMode = func(req protocol.SetSessionModeRequest) (protocol.SetSessionModeResponse, error) {
		return protocol.SetSessionModeResponse{}, protocol.InvalidParams("bad modeId", nil)
	}

	if err := sess.SetMode(ctx, "plan"); err == nil {
		t.Fatal("SetMode() error = nil, want the agent's InvalidParams fault")
	}

	want := sampleModeState()
	if got := sess.Modes(); !reflect.DeepEqual(got, want) {
		t.Errorf("Modes() after a failed SetMode = %#v, want unchanged %#v", got, want)
	}
}

// TestSetConfigOptionAndSetModeAfterConnectionDeathFailTyped proves both new
// Session methods fail with a typed *ClosedError, like every other Client/
// Session operation, once the connection has died.
func TestSetConfigOptionAndSetModeAfterConnectionDeathFailTyped(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	sess := newSessionForTest(t, c, fa, "sess-dead")

	if err := fa.conn.Close(); err != nil {
		t.Fatalf("fa.conn.Close() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		errCfg := sess.SetConfigOption(ctx, "model", "opus")
		errMode := sess.SetMode(ctx, "plan")
		cancel()

		var closedCfg, closedMode *ClosedError
		cfgOK := errors.As(errCfg, &closedCfg)
		modeOK := errors.As(errMode, &closedMode)
		if cfgOK && modeOK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("SetConfigOption()/SetMode() after connection death errors = %v / %v, want *ClosedError for both", errCfg, errMode)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestProveSetModelCapability proves ProveSetModelCapability's grant is
// driven strictly by the real initialize response _meta this Client
// received: a key genuinely present and non-null grants proof, an absent or
// null key does not, and a Client that never dialed successfully never
// grants either.
func TestProveSetModelCapability(t *testing.T) {
	t.Run("key present and non-null grants proof", func(t *testing.T) {
		c, _ := dialTestClient(t, Options{}, func(fa *fakeAgent) {
			fa.onInitialize = func(req protocol.InitializeRequest) (protocol.InitializeResponse, error) {
				return protocol.InitializeResponse{
					ProtocolVersion: protocol.CurrentProtocolVersion,
					Meta:            json.RawMessage(`{"acp.dev/setModel":{}}`),
				}, nil
			}
		})

		proof, ok := c.ProveSetModelCapability("acp.dev/setModel")
		if !ok || !proof.granted {
			t.Fatalf("ProveSetModelCapability(present key) = (%#v, %v), want granted", proof, ok)
		}
	})

	t.Run("absent key refuses proof", func(t *testing.T) {
		c, _ := dialTestClient(t, Options{}, func(fa *fakeAgent) {
			fa.onInitialize = func(req protocol.InitializeRequest) (protocol.InitializeResponse, error) {
				return protocol.InitializeResponse{
					ProtocolVersion: protocol.CurrentProtocolVersion,
					Meta:            json.RawMessage(`{"acp.dev/setModel":{}}`),
				}, nil
			}
		})

		proof, ok := c.ProveSetModelCapability("acp.dev/other")
		if ok || proof.granted {
			t.Fatalf("ProveSetModelCapability(absent key) = (%#v, %v), want refused", proof, ok)
		}
	})

	t.Run("null key value refuses proof", func(t *testing.T) {
		c, _ := dialTestClient(t, Options{}, func(fa *fakeAgent) {
			fa.onInitialize = func(req protocol.InitializeRequest) (protocol.InitializeResponse, error) {
				return protocol.InitializeResponse{
					ProtocolVersion: protocol.CurrentProtocolVersion,
					Meta:            json.RawMessage(`{"acp.dev/setModel":null}`),
				}, nil
			}
		})

		proof, ok := c.ProveSetModelCapability("acp.dev/setModel")
		if ok || proof.granted {
			t.Fatalf("ProveSetModelCapability(null key) = (%#v, %v), want refused", proof, ok)
		}
	})

	t.Run("no _meta at all refuses proof", func(t *testing.T) {
		c, _ := dialTestClient(t, Options{})

		proof, ok := c.ProveSetModelCapability("acp.dev/setModel")
		if ok || proof.granted {
			t.Fatalf("ProveSetModelCapability(no _meta) = (%#v, %v), want refused", proof, ok)
		}
	})

	t.Run("never dialed refuses proof", func(t *testing.T) {
		c := New(stdio.Command{}, Options{})
		proof, ok := c.ProveSetModelCapability("acp.dev/setModel")
		if ok || proof.granted {
			t.Fatalf("ProveSetModelCapability(never dialed) = (%#v, %v), want refused", proof, ok)
		}
	})
}

// TestSetModelRefusesWithoutGrantedCapability proves Session.SetModel fails
// closed with *SetModelUnsupportedError, and never places a call on the
// wire at all, when given a zero-value SetModelCapability — this package
// must never speculatively probe an ACP peer for an undeclared method.
func TestSetModelRefusesWithoutGrantedCapability(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	sess := newSessionForTest(t, c, fa, "sess-set-model-refused")

	called := false
	fa.onSetModel = func(req setModelRequest) (setModelResponse, error) {
		called = true
		return setModelResponse{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := sess.SetModel(ctx, SetModelCapability{}, "claude-opus")

	var unsupported *SetModelUnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("SetModel() without proof error = %v (%T), want *SetModelUnsupportedError", err, err)
	}
	if called {
		t.Error("SetModel() without proof invoked session/set_model on the wire; want no wire traffic at all")
	}
}

// TestSetModelSucceedsWithGrantedCapability proves that once
// ProveSetModelCapability grants proof for the connected agent's advertised
// key, SetModel sends the exact sessionId/modelId over the wire and
// succeeds.
func TestSetModelSucceedsWithGrantedCapability(t *testing.T) {
	c, fa := dialTestClient(t, Options{}, func(fa *fakeAgent) {
		fa.onInitialize = func(req protocol.InitializeRequest) (protocol.InitializeResponse, error) {
			return protocol.InitializeResponse{
				ProtocolVersion: protocol.CurrentProtocolVersion,
				Meta:            json.RawMessage(`{"acp.dev/setModel":{}}`),
			}, nil
		}
	})
	sess := newSessionForTest(t, c, fa, "sess-set-model-ok")

	proof, ok := c.ProveSetModelCapability("acp.dev/setModel")
	if !ok {
		t.Fatal("ProveSetModelCapability() = false, want true")
	}

	var gotReq setModelRequest
	fa.onSetModel = func(req setModelRequest) (setModelResponse, error) {
		gotReq = req
		return setModelResponse{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sess.SetModel(ctx, proof, "claude-opus"); err != nil {
		t.Fatalf("SetModel() error = %v", err)
	}

	if gotReq.SessionID != sess.ID() {
		t.Errorf("setModelRequest.SessionID = %q, want %q", gotReq.SessionID, sess.ID())
	}
	if gotReq.ModelID != "claude-opus" {
		t.Errorf("setModelRequest.ModelID = %q, want %q", gotReq.ModelID, "claude-opus")
	}
}

// TestSetModelPropagatesAgentError proves a failed session/set_model call
// (once past the capability gate) surfaces the agent's error rather than
// being swallowed.
func TestSetModelPropagatesAgentError(t *testing.T) {
	c, fa := dialTestClient(t, Options{}, func(fa *fakeAgent) {
		fa.onInitialize = func(req protocol.InitializeRequest) (protocol.InitializeResponse, error) {
			return protocol.InitializeResponse{
				ProtocolVersion: protocol.CurrentProtocolVersion,
				Meta:            json.RawMessage(`{"acp.dev/setModel":{}}`),
			}, nil
		}
	})
	sess := newSessionForTest(t, c, fa, "sess-set-model-err")

	proof, ok := c.ProveSetModelCapability("acp.dev/setModel")
	if !ok {
		t.Fatal("ProveSetModelCapability() = false, want true")
	}

	fa.onSetModel = func(req setModelRequest) (setModelResponse, error) {
		return setModelResponse{}, protocol.InvalidParams("unknown modelId", nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sess.SetModel(ctx, proof, "does-not-exist"); err == nil {
		t.Fatal("SetModel() error = nil, want the agent's InvalidParams fault")
	}
}
