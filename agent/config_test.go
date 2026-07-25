package agent_test

// config_test.go tests the session/set_config_option and session/set_mode
// handlers: Task 4.1 of harness/docs/plans/2026-07-23-acp-bridge-implementation.md,
// the first task of Phase 4.
//
// Four behaviors, one test each, per the task:
//   - TestHandleSessionSetConfigOptionValidatesAgainstLatestCatalog: a
//     configId/valueId is validated against the CATALOG FETCHED FOR THIS
//     REQUEST, not a cached snapshot from an earlier call — the catalog
//     actually changes (a value is retired) between two requests, and the
//     second request for the now-stale value is rejected.
//   - TestHandleSessionSetConfigOptionIdempotentNoOp: setting an option to
//     its current value succeeds without ever calling the
//     RuntimeConfigController and without ever sending a config_option_update
//     notification.
//   - TestHandleSessionSetConfigOptionResponseReturnsCompleteState: the
//     response carries every option's current state, not just the one that
//     changed.
//   - TestHandleSessionSetModeConvergesWithConfigOption: session/set_mode
//     translates into exactly the same RuntimeConfigChange
//     (OptionID=agent.ModeConfigOptionID) handleSessionSetConfigOption would
//     use for the mode option, and exactly one config_option_update
//     notification is emitted — never two, proving the two wire methods
//     share one apply path rather than each independently notifying.
import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
)

// stubConfigCatalog is the RuntimeConfigCatalog test double every test in
// this file drives. Its options can be replaced between calls (setOptions),
// which is exactly how TestHandleSessionSetConfigOptionValidatesAgainstLatestCatalog
// proves validation runs against the latest snapshot rather than one cached
// from an earlier request: RuntimeConfigOptions defensively copies its
// current options on every call, so a caller mutating the returned slice can
// never perturb what a later call reports.
type stubConfigCatalog struct {
	mu      sync.Mutex
	calls   int
	options []agent.RuntimeConfigOption
	err     error
}

func (c *stubConfigCatalog) RuntimeConfigOptions(context.Context, agent.SessionID) ([]agent.RuntimeConfigOption, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	return append([]agent.RuntimeConfigOption(nil), c.options...), nil
}

func (c *stubConfigCatalog) setOptions(opts []agent.RuntimeConfigOption) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.options = opts
}

func (c *stubConfigCatalog) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// stubConfigController is the RuntimeConfigController test double every test
// in this file drives: it records every SetRuntimeConfigOption call (count
// and the exact RuntimeConfigChange requested) and returns a
// caller-configured resulting catalog (or error).
type stubConfigController struct {
	mu         sync.Mutex
	calls      int
	lastChange agent.RuntimeConfigChange
	result     []agent.RuntimeConfigOption
	err        error
}

func (c *stubConfigController) SetRuntimeConfigOption(_ context.Context, _ agent.SessionID, change agent.RuntimeConfigChange) ([]agent.RuntimeConfigOption, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.lastChange = change
	if c.err != nil {
		return nil, c.err
	}
	return c.result, nil
}

func (c *stubConfigController) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *stubConfigController) lastCalledChange() agent.RuntimeConfigChange {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastChange
}

// newConfigTestAgent wires a fresh agent.Agent around a live fakeLiveSession
// (reused from prompt_test.go) with catalog/controller configured as
// Options.ConfigCatalog/Options.ConfigController, registers it on a piped
// connection, subscribes to session/update notifications BEFORE creating the
// session (so no notification can be missed), and creates one session
// through the real session/new handshake — mirroring newDeleteTestAgent's
// established pattern (delete_test.go), parameterized on the config
// capabilities that pattern does not expose.
func newConfigTestAgent(t *testing.T, catalog agent.RuntimeConfigCatalog, controller agent.RuntimeConfigController) (agentConn *protocol.AgentConn, liveSessionID protocol.SessionID, updates chan protocol.SessionNotification) {
	t.Helper()
	fake := newFakeLiveSession(t)
	host := &promptHostStub{session: fake}
	a, err := agent.New(agent.Options{Host: host, ConfigCatalog: catalog, ConfigController: controller})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := pipeConns(t)
	a.Register(server)

	updates = make(chan protocol.SessionNotification, 8)
	client.HandleNotify(string(protocol.MethodSessionUpdate), func(_ context.Context, _ string, params json.RawMessage) {
		var n protocol.SessionNotification
		if err := json.Unmarshal(params, &n); err != nil {
			t.Errorf("unmarshal session/update notification: %v", err)
			return
		}
		updates <- n
	})

	agentConn = protocol.NewAgentConn(client)
	resp, err := agentConn.NewSession(context.Background(), protocol.NewSessionRequest{Cwd: "/workspace"})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	return agentConn, resp.SessionID, updates
}

// modelOption builds a small two-value "model" RuntimeConfigOption fixture
// shared by several tests below.
func modelOption(current protocol.SessionConfigValueID, values ...agent.RuntimeConfigValue) agent.RuntimeConfigOption {
	return agent.RuntimeConfigOption{
		ID:           "model",
		Category:     protocol.SessionConfigOptionCategoryModel,
		Name:         "Model",
		Values:       values,
		CurrentValue: current,
	}
}

// --- Behavior 1: latest-catalog validation --------------------------------

// TestHandleSessionSetConfigOptionValidatesAgainstLatestCatalog is this
// task's central "latest, not cached" assertion: a value valid under the
// catalog snapshot fetched for one request is rejected once the catalog
// itself changes (the value is retired) before a second request for that
// same value — proving each request re-validates against what is true right
// now, never a snapshot cached from session/new, an earlier request, or the
// session's own creation-time configuration.
func TestHandleSessionSetConfigOptionValidatesAgainstLatestCatalog(t *testing.T) {
	catalog := &stubConfigCatalog{options: []agent.RuntimeConfigOption{
		modelOption("fast",
			agent.RuntimeConfigValue{ID: "fast", Name: "Fast"},
			agent.RuntimeConfigValue{ID: "slow", Name: "Slow"},
		),
	}}
	controller := &stubConfigController{result: []agent.RuntimeConfigOption{
		modelOption("slow",
			agent.RuntimeConfigValue{ID: "fast", Name: "Fast"},
			agent.RuntimeConfigValue{ID: "slow", Name: "Slow"},
		),
	}}

	agentConn, sessionID, _ := newConfigTestAgent(t, catalog, controller)
	// session/new itself now fetches the catalog once, to populate its own
	// response's initial ConfigOptions (config.go's initialConfigState) —
	// baseline this call away so the assertion below counts only the two
	// explicit session/set_config_option requests this test actually makes.
	baseline := catalog.callCount()

	valueSlow := protocol.SessionConfigValueID("slow")
	if _, err := agentConn.SetConfigOption(context.Background(), protocol.SetSessionConfigOptionRequest{
		SessionID: sessionID, ConfigID: "model", ValueID: &valueSlow,
	}); err != nil {
		t.Fatalf("SetConfigOption(slow) against catalog A: %v", err)
	}
	if got := controller.callCount(); got != 1 {
		t.Fatalf("controller calls after first (valid) request = %d, want 1", got)
	}

	// The catalog changes: "slow" is retired in favor of "turbo". A second
	// request for the now-stale "slow" value must be validated against THIS
	// new snapshot.
	catalog.setOptions([]agent.RuntimeConfigOption{
		modelOption("fast",
			agent.RuntimeConfigValue{ID: "fast", Name: "Fast"},
			agent.RuntimeConfigValue{ID: "turbo", Name: "Turbo"},
		),
	})

	_, err := agentConn.SetConfigOption(context.Background(), protocol.SetSessionConfigOptionRequest{
		SessionID: sessionID, ConfigID: "model", ValueID: &valueSlow,
	})
	if err == nil {
		t.Fatal("SetConfigOption(slow) against catalog B: error = nil, want InvalidParams (value retired from the latest catalog)")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Fault.Code = %v, want %v (ErrorCodeInvalidParams)", f.Code, protocol.ErrorCodeInvalidParams)
	}
	if got := controller.callCount(); got != 1 {
		t.Errorf("controller calls after second (now-invalid) request = %d, want still 1 (must never apply an unvalidated change)", got)
	}
	if got := catalog.callCount() - baseline; got != 2 {
		t.Errorf("catalog fetches since session/new = %d, want 2 (one per session/set_config_option request — never reused across requests)", got)
	}
}

// --- Behavior 2: idempotent writes -----------------------------------------

// TestHandleSessionSetConfigOptionIdempotentNoOp asserts that setting an
// option to its OWN current value succeeds without any side effect: the
// RuntimeConfigController is never called, and — the part a weaker test
// could miss by only checking "no error" — no config_option_update
// notification is ever sent to the client.
func TestHandleSessionSetConfigOptionIdempotentNoOp(t *testing.T) {
	catalog := &stubConfigCatalog{options: []agent.RuntimeConfigOption{
		modelOption("fast",
			agent.RuntimeConfigValue{ID: "fast", Name: "Fast"},
			agent.RuntimeConfigValue{ID: "slow", Name: "Slow"},
		),
	}}
	controller := &stubConfigController{}

	agentConn, sessionID, updates := newConfigTestAgent(t, catalog, controller)

	valueFast := protocol.SessionConfigValueID("fast")
	resp, err := agentConn.SetConfigOption(context.Background(), protocol.SetSessionConfigOptionRequest{
		SessionID: sessionID, ConfigID: "model", ValueID: &valueFast,
	})
	if err != nil {
		t.Fatalf("SetConfigOption(current value): %v", err)
	}
	if resp == nil || len(resp.ConfigOptions) != 1 {
		t.Fatalf("resp.ConfigOptions = %+v, want the unchanged 1-option catalog", resp)
	}

	if got := controller.callCount(); got != 0 {
		t.Errorf("controller calls = %d, want 0 (idempotent no-op must never touch the controller)", got)
	}

	// No notification was ever sent by the server for this request, so its
	// absence is not a timing race to wait out — the channel can never
	// receive anything for this call, at any point in time.
	select {
	case n := <-updates:
		t.Fatalf("received unexpected config_option_update notification for a no-op write: %+v", n)
	default:
	}
}

// --- Behavior 3: complete resulting state ----------------------------------

// TestHandleSessionSetConfigOptionResponseReturnsCompleteState asserts the
// response returns EVERY option's current state, not just the one touched by
// this request — a second, untouched "access" option is present in both the
// catalog and the controller's result, and must appear unchanged in the
// response too.
func TestHandleSessionSetConfigOptionResponseReturnsCompleteState(t *testing.T) {
	accessOption := agent.RuntimeConfigOption{
		ID:       "access",
		Category: "_access",
		Name:     "Access",
		Values: []agent.RuntimeConfigValue{
			{ID: "readonly", Name: "Read-only"},
			{ID: "full", Name: "Full"},
		},
		CurrentValue: "full",
	}
	catalog := &stubConfigCatalog{options: []agent.RuntimeConfigOption{
		modelOption("fast",
			agent.RuntimeConfigValue{ID: "fast", Name: "Fast"},
			agent.RuntimeConfigValue{ID: "slow", Name: "Slow"},
		),
		accessOption,
	}}
	wantResult := []agent.RuntimeConfigOption{
		modelOption("slow",
			agent.RuntimeConfigValue{ID: "fast", Name: "Fast"},
			agent.RuntimeConfigValue{ID: "slow", Name: "Slow"},
		),
		accessOption,
	}
	controller := &stubConfigController{result: wantResult}

	agentConn, sessionID, updates := newConfigTestAgent(t, catalog, controller)

	valueSlow := protocol.SessionConfigValueID("slow")
	resp, err := agentConn.SetConfigOption(context.Background(), protocol.SetSessionConfigOptionRequest{
		SessionID: sessionID, ConfigID: "model", ValueID: &valueSlow,
	})
	if err != nil {
		t.Fatalf("SetConfigOption: %v", err)
	}
	if resp == nil {
		t.Fatal("SetConfigOption: resp = nil")
	}
	if got := controller.callCount(); got != 1 {
		t.Fatalf("controller calls = %d, want 1", got)
	}
	wantChange := agent.RuntimeConfigChange{OptionID: "model", ValueID: "slow"}
	if got := controller.lastCalledChange(); got != wantChange {
		t.Errorf("controller change = %+v, want %+v", got, wantChange)
	}

	if len(resp.ConfigOptions) != 2 {
		t.Fatalf("resp.ConfigOptions has %d entries, want 2 (the complete resulting state, not just the touched option)", len(resp.ConfigOptions))
	}
	byID := map[protocol.SessionConfigID]protocol.SessionConfigOption{}
	for _, o := range resp.ConfigOptions {
		byID[o.ID] = o
	}

	model, ok := byID["model"]
	if !ok {
		t.Fatal("resp.ConfigOptions: no \"model\" entry")
	}
	if model.Select == nil {
		t.Fatal("model option: Select = nil, want a select variant")
	}
	if model.Select.CurrentValue != "slow" {
		t.Errorf("model option CurrentValue = %q, want %q", model.Select.CurrentValue, "slow")
	}

	access, ok := byID["access"]
	if !ok {
		t.Fatal("resp.ConfigOptions: no \"access\" entry — the untouched option must still be reported")
	}
	if access.Select == nil || access.Select.CurrentValue != "full" {
		t.Errorf("access option = %+v, want CurrentValue \"full\" (unchanged)", access)
	}

	select {
	case n := <-updates:
		if n.Update.ConfigOptionUpdate == nil {
			t.Fatalf("notification = %+v, want a config_option_update", n)
		}
		if len(n.Update.ConfigOptionUpdate.ConfigOptions) != 2 {
			t.Errorf("notification configOptions has %d entries, want 2", len(n.Update.ConfigOptionUpdate.ConfigOptions))
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for config_option_update notification")
	}
}

// --- Behavior 4: session/set_mode convergence ------------------------------

// TestHandleSessionSetModeConvergesWithConfigOption asserts session/set_mode
// and session/set_config_option(category=mode) can never diverge: a
// session/set_mode request translates into exactly the RuntimeConfigChange
// handleSessionSetConfigOption would build for
// configId=agent.ModeConfigOptionID, and exactly ONE config_option_update
// notification is emitted for it — never two (which would be the observable
// symptom of two independent, potentially-drifting code paths instead of one
// shared one).
func TestHandleSessionSetModeConvergesWithConfigOption(t *testing.T) {
	catalog := &stubConfigCatalog{options: []agent.RuntimeConfigOption{
		{
			ID:       agent.ModeConfigOptionID,
			Category: protocol.SessionConfigOptionCategoryMode,
			Name:     "Mode",
			Values: []agent.RuntimeConfigValue{
				{ID: "build", Name: "Build"},
				{ID: "plan", Name: "Plan"},
			},
			CurrentValue: "build",
		},
	}}
	wantResult := []agent.RuntimeConfigOption{
		{
			ID:       agent.ModeConfigOptionID,
			Category: protocol.SessionConfigOptionCategoryMode,
			Name:     "Mode",
			Values: []agent.RuntimeConfigValue{
				{ID: "build", Name: "Build"},
				{ID: "plan", Name: "Plan"},
			},
			CurrentValue: "plan",
		},
	}
	controller := &stubConfigController{result: wantResult}

	agentConn, sessionID, updates := newConfigTestAgent(t, catalog, controller)

	if _, err := agentConn.SetMode(context.Background(), protocol.SetSessionModeRequest{
		SessionID: sessionID, ModeID: "plan",
	}); err != nil {
		t.Fatalf("SetMode: %v", err)
	}

	if got := controller.callCount(); got != 1 {
		t.Fatalf("controller calls = %d, want 1", got)
	}
	wantChange := agent.RuntimeConfigChange{OptionID: agent.ModeConfigOptionID, ValueID: "plan"}
	if got := controller.lastCalledChange(); got != wantChange {
		t.Errorf("controller change = %+v, want %+v (session/set_mode must target the well-known mode config option id)", got, wantChange)
	}

	select {
	case n := <-updates:
		if n.Update.ConfigOptionUpdate == nil {
			t.Fatalf("notification = %+v, want a config_option_update", n)
		}
		if len(n.Update.ConfigOptionUpdate.ConfigOptions) != 1 {
			t.Fatalf("notification configOptions = %+v, want 1 entry", n.Update.ConfigOptionUpdate.ConfigOptions)
		}
		if got := n.Update.ConfigOptionUpdate.ConfigOptions[0].Select; got == nil || got.CurrentValue != "plan" {
			t.Errorf("notification mode option = %+v, want CurrentValue \"plan\"", got)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for config_option_update notification from session/set_mode")
	}

	// Exactly one — never a second, independently-emitted notification.
	select {
	case n := <-updates:
		t.Fatalf("received a SECOND notification %+v — session/set_mode and session/set_config_option must converge on exactly one update", n)
	default:
	}
}

// TestHandleSessionSetConfigOptionAndSetModeNotRegisteredWithoutBothCapabilities
// asserts both wire methods are registered only when BOTH Options.ConfigCatalog
// and Options.ConfigController are supplied: a write path that could never
// validate against a live catalog (no ConfigCatalog) or never apply a change
// (no ConfigController) is not offered at all, matching every other optional
// capability's "never advertised, never accepted" rule.
func TestHandleSessionSetConfigOptionAndSetModeNotRegisteredWithoutBothCapabilities(t *testing.T) {
	cases := []struct {
		name       string
		catalog    agent.RuntimeConfigCatalog
		controller agent.RuntimeConfigController
	}{
		{name: "neither supplied", catalog: nil, controller: nil},
		{name: "only catalog supplied", catalog: &stubConfigCatalog{}, controller: nil},
		{name: "only controller supplied", catalog: nil, controller: &stubConfigController{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			agentConn, sessionID, _ := newConfigTestAgent(t, tc.catalog, tc.controller)

			valueID := protocol.SessionConfigValueID("x")
			_, err := agentConn.SetConfigOption(context.Background(), protocol.SetSessionConfigOptionRequest{
				SessionID: sessionID, ConfigID: "model", ValueID: &valueID,
			})
			assertMethodNotFoundErr(t, err, "session/set_config_option")

			_, err = agentConn.SetMode(context.Background(), protocol.SetSessionModeRequest{
				SessionID: sessionID, ModeID: "plan",
			})
			assertMethodNotFoundErr(t, err, "session/set_mode")
		})
	}
}

// TestHandleSessionSetModeFailsDistinctlyWhenCatalogLacksModeOption is Minor
// 2 of the Phase 4 follow-up review: ModeConfigOptionID's convergence
// contract ("a RuntimeConfigCatalog implementation MUST offer an option with
// this id" — host.go) is otherwise enforced only by doc comment/test
// convention, so a misconfigured catalog that omits it must fail LOUDLY and
// DIAGNOSABLY — a distinct internal error, not the ordinary "unknown
// configId" InvalidParams a real client gets for an arbitrary bad
// session/set_config_option request. This test asserts both that
// session/set_mode gets a different Code (InternalError, not InvalidParams)
// AND a different Message from that ordinary client-error case, proving the
// two are genuinely distinguishable, not just cosmetically different.
func TestHandleSessionSetModeFailsDistinctlyWhenCatalogLacksModeOption(t *testing.T) {
	catalog := &stubConfigCatalog{options: []agent.RuntimeConfigOption{
		modelOption("fast", agent.RuntimeConfigValue{ID: "fast", Name: "Fast"}),
	}}
	controller := &stubConfigController{}

	agentConn, sessionID, _ := newConfigTestAgent(t, catalog, controller)

	_, err := agentConn.SetMode(context.Background(), protocol.SetSessionModeRequest{
		SessionID: sessionID, ModeID: "plan",
	})
	if err == nil {
		t.Fatal("SetMode with no \"mode\" option in the catalog: error = nil, want a distinct internal (host misconfiguration) error")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInternalError {
		t.Errorf("Fault.Code = %v, want %v (InternalError: a misconfigured host, not a client mistake)", f.Code, protocol.ErrorCodeInternalError)
	}
	if got := controller.callCount(); got != 0 {
		t.Errorf("controller calls = %d, want 0 (must fail before ever touching the controller)", got)
	}

	// The ordinary client-error case: an arbitrary bad configId via
	// session/set_config_option. This must stay InvalidParams, and must be
	// genuinely distinguishable from session/set_mode's misconfiguration
	// error above (different Code AND different Message), not merely two
	// different strings that a client would otherwise have to sniff apart.
	valueID := protocol.SessionConfigValueID("x")
	_, ordinaryErr := agentConn.SetConfigOption(context.Background(), protocol.SetSessionConfigOptionRequest{
		SessionID: sessionID, ConfigID: "nonexistent", ValueID: &valueID,
	})
	var ordinaryFault *protocol.Fault
	if !errors.As(ordinaryErr, &ordinaryFault) {
		t.Fatalf("ordinary unknown configId error = %v (%T), want *protocol.Fault", ordinaryErr, ordinaryErr)
	}
	if ordinaryFault.Code != protocol.ErrorCodeInvalidParams {
		t.Fatalf("test setup sanity check: ordinary unknown configId Fault.Code = %v, want InvalidParams", ordinaryFault.Code)
	}
	if f.Code == ordinaryFault.Code {
		t.Error("session/set_mode's misconfiguration error must not share the ordinary client-error Code")
	}
	if f.Message == ordinaryFault.Message {
		t.Error("session/set_mode's misconfiguration error must not share the ordinary client-error Message")
	}
}

func assertMethodNotFoundErr(t *testing.T, err error, method string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: error = nil, want MethodNotFound fault", method)
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("%s: error = %v (%T), want *protocol.Fault", method, err, err)
	}
	if f.Code != protocol.ErrorCodeMethodNotFound {
		t.Errorf("%s: Fault.Code = %v, want %v (ErrorCodeMethodNotFound)", method, f.Code, protocol.ErrorCodeMethodNotFound)
	}
}
