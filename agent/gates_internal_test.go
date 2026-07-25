package agent

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
)

// --- fakes ---------------------------------------------------------------

// gateFakeClient is a minimal gatePermissionClient: it records every request
// it was given and returns exactly the scripted (resp, err) pair.
type gateFakeClient struct {
	resp *protocol.RequestPermissionResponse
	err  error

	mu   sync.Mutex
	reqs []protocol.RequestPermissionRequest
}

func (c *gateFakeClient) RequestPermission(_ context.Context, req protocol.RequestPermissionRequest) (*protocol.RequestPermissionResponse, error) {
	c.mu.Lock()
	c.reqs = append(c.reqs, req)
	c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	return c.resp, nil
}

// gateFakeSession is a minimal LiveSession whose RespondGate records every
// gate.GateResponse it receives (and returns a scripted error, if any). The
// rest of LiveSession's contract is unused boilerplate here (same rationale
// as registryFakeSession above): runPermissionGateRoundTrip only ever calls
// RespondGate.
type gateFakeSession struct {
	id uuid.UUID

	mu         sync.Mutex
	responses  []gate.GateResponse
	respondErr error
}

func newGateFakeSession(t *testing.T) *gateFakeSession {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	return &gateFakeSession{id: id}
}

func (s *gateFakeSession) SessionID() uuid.UUID { return s.id }

func (s *gateFakeSession) Submit(context.Context, []content.Block) (uuid.UUID, error) {
	return uuid.UUID{}, errors.New("gateFakeSession: Submit not implemented")
}

func (s *gateFakeSession) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errors.New("gateFakeSession: SubscribeEvents not implemented")
}

func (s *gateFakeSession) RespondGate(_ context.Context, resp gate.GateResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, resp)
	return s.respondErr
}

func (s *gateFakeSession) Interrupt(context.Context) (bool, error) {
	return false, errors.New("gateFakeSession: Interrupt not implemented")
}

func (s *gateFakeSession) gateResponses() []gate.GateResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]gate.GateResponse(nil), s.responses...)
}

// permissionGate builds a translatable gate.Gate exactly the shape
// internal/loopruntime's permissionGate (the real production constructor)
// produces: KindPermission, ResolverLoop, and the three exact
// gate.ApprovalControls.
func permissionGateForTest(t *testing.T) gate.Gate {
	t.Helper()
	gateID, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	toolExecID, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	return gate.Gate{
		ID:       gateID,
		Kind:     gate.KindPermission,
		Resolver: gate.ResolverLoop,
		Subject:  gate.Subject{ToolExecutionID: toolExecID},
		Prompt: gate.Prompt{
			Title:    "Approve tool call",
			Body:     "bash wants to run `rm -rf /tmp/scratch`",
			Controls: gate.ApprovalControls(),
		},
	}
}

// --- permissionOptionsFromGate: option-set fidelity, Harness -> ACP -------

func TestPermissionOptionsFromGateFidelity(t *testing.T) {
	g := permissionGateForTest(t)

	opts, ok := permissionOptionsFromGate(g)
	if !ok {
		t.Fatalf("permissionOptionsFromGate: ok = false, want true for a KindPermission/ResolverLoop gate")
	}
	if len(opts) != 3 {
		t.Fatalf("len(opts) = %d, want 3 (Approve, Approve always for this workspace, Deny)", len(opts))
	}

	want := []struct {
		action gate.ApprovalAction
		kind   protocol.PermissionOptionKind
	}{
		{gate.ApprovalApprove, protocol.PermissionOptionKindAllowOnce},
		{gate.ApprovalApproveAlwaysWorkspace, protocol.PermissionOptionKindAllowAlways},
		{gate.ApprovalDeny, protocol.PermissionOptionKindRejectOnce},
	}
	for i, w := range want {
		if protocol.PermissionOptionID(w.action) != opts[i].OptionID {
			t.Errorf("opts[%d].OptionID = %q, want %q", i, opts[i].OptionID, w.action)
		}
		if opts[i].Kind != w.kind {
			t.Errorf("opts[%d].Kind = %q, want %q", i, opts[i].Kind, w.kind)
		}
		if opts[i].Name == "" {
			t.Errorf("opts[%d].Name is empty, want a human-readable label", i)
		}
	}
}

// --- permissionOptionsFromGate: everything else is NOT translatable -------

func TestPermissionOptionsFromGateNotTranslatable(t *testing.T) {
	toolExecID, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	tests := []struct {
		name string
		gate gate.Gate
	}{
		{
			name: "ask-user gate: free-text/Values answer has no ACP outcome field to carry it",
			gate: gate.Gate{
				Kind:     gate.KindAskUser,
				Resolver: gate.ResolverLoop,
				Subject:  gate.Subject{ToolExecutionID: toolExecID},
				Prompt: gate.Prompt{
					Controls: []gate.Control{{Action: "answer", Label: "Answer"}},
				},
			},
		},
		{
			name: "host-owned form gate: never flattened into request_permission",
			gate: gate.Gate{
				Kind:     gate.KindForm,
				Resolver: gate.ResolverSession,
				Prompt: gate.Prompt{
					Controls: []gate.Control{
						{Action: "accept", Label: "Accept"},
						{Action: "decline", Label: "Decline"},
					},
				},
			},
		},
		{
			name: "host-owned open-url gate: never flattened into request_permission",
			gate: gate.Gate{
				Kind:     gate.KindOpenURL,
				Resolver: gate.ResolverSession,
				Prompt: gate.Prompt{
					Origin:   "https://example.invalid",
					Controls: []gate.Control{{Action: "accept", Label: "I opened it"}},
				},
			},
		},
		{
			name: "permission gate with an unrecognized control action: fail closed, never invent an option",
			gate: gate.Gate{
				Kind:     gate.KindPermission,
				Resolver: gate.ResolverLoop,
				Subject:  gate.Subject{ToolExecutionID: toolExecID},
				Prompt: gate.Prompt{
					Controls: []gate.Control{{Action: "yolo_approve", Label: "Sure, whatever"}},
				},
			},
		},
		{
			name: "permission gate with no controls at all",
			gate: gate.Gate{
				Kind:     gate.KindPermission,
				Resolver: gate.ResolverLoop,
				Subject:  gate.Subject{ToolExecutionID: toolExecID},
			},
		},
		{
			name: "permission-kind gate that is, hypothetically, host-owned",
			gate: gate.Gate{
				Kind:     gate.KindPermission,
				Resolver: gate.ResolverSession,
				Subject:  gate.Subject{ToolExecutionID: toolExecID},
				Prompt:   gate.Prompt{Controls: gate.ApprovalControls()},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts, ok := permissionOptionsFromGate(tt.gate)
			if ok {
				t.Fatalf("permissionOptionsFromGate: ok = true, opts = %+v, want false (not translatable)", opts)
			}
			if opts != nil {
				t.Errorf("opts = %+v, want nil when not translatable", opts)
			}
		})
	}
}

// --- runPermissionGateRoundTrip: the client's selection resolves the gate,
// both directions of fidelity (a selection round-trips back to the exact
// Harness action string) ---------------------------------------------------

func TestRunPermissionGateRoundTripSelectedOption(t *testing.T) {
	tests := []struct {
		name   string
		action gate.ApprovalAction
	}{
		{"approve", gate.ApprovalApprove},
		{"approve always for this workspace", gate.ApprovalApproveAlwaysWorkspace},
		{"deny", gate.ApprovalDeny},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			g := permissionGateForTest(t)
			opts, ok := permissionOptionsFromGate(g)
			if !ok {
				t.Fatalf("permissionOptionsFromGate: ok = false")
			}
			session := newGateFakeSession(t)
			client := &gateFakeClient{resp: &protocol.RequestPermissionResponse{
				Outcome: protocol.RequestPermissionOutcome{
					Selected: &protocol.SelectedPermissionOutcome{OptionID: protocol.PermissionOptionID(tt.action)},
				},
			}}

			fault := runPermissionGateRoundTrip(context.Background(), client, session, "session-1", g, opts)
			if fault != nil {
				t.Fatalf("runPermissionGateRoundTrip: unexpected fault: %v", fault)
			}

			responses := session.gateResponses()
			if len(responses) != 1 {
				t.Fatalf("RespondGate calls = %d, want 1", len(responses))
			}
			if responses[0].GateID != g.ID {
				t.Errorf("GateID = %v, want %v", responses[0].GateID, g.ID)
			}
			if responses[0].Action != string(tt.action) {
				t.Errorf("Action = %q, want %q", responses[0].Action, tt.action)
			}
			if responses[0].Source.Kind != gate.ResponseFromUser {
				t.Errorf("Source.Kind = %q, want %q", responses[0].Source.Kind, gate.ResponseFromUser)
			}
		})
	}
}

// --- runPermissionGateRoundTrip: an option the client was never offered is
// rejected, and the gate is left open (RespondGate is never called) --------

func TestRunPermissionGateRoundTripUnofferedOptionLeavesGateOpen(t *testing.T) {
	g := permissionGateForTest(t)
	opts, ok := permissionOptionsFromGate(g)
	if !ok {
		t.Fatalf("permissionOptionsFromGate: ok = false")
	}
	session := newGateFakeSession(t)
	client := &gateFakeClient{resp: &protocol.RequestPermissionResponse{
		Outcome: protocol.RequestPermissionOutcome{
			Selected: &protocol.SelectedPermissionOutcome{OptionID: "not-an-offered-option-id"},
		},
	}}

	fault := runPermissionGateRoundTrip(context.Background(), client, session, "session-1", g, opts)
	if fault == nil {
		t.Fatal("runPermissionGateRoundTrip: fault = nil, want a typed error for an unoffered option")
	}
	var unoffered *UnofferedPermissionOptionError
	if !errors.As(fault, &unoffered) {
		t.Fatalf("fault cause chain does not contain *UnofferedPermissionOptionError: %v", fault)
	}
	if unoffered.OptionID != "not-an-offered-option-id" {
		t.Errorf("OptionID = %q, want %q", unoffered.OptionID, "not-an-offered-option-id")
	}
	if len(session.gateResponses()) != 0 {
		t.Fatalf("RespondGate calls = %d, want 0 (gate must be left open, not answered)", len(session.gateResponses()))
	}
}

// --- runPermissionGateRoundTrip: a legitimate Cancelled outcome resolves the
// gate Deny (this is not a client error) ------------------------------------

func TestRunPermissionGateRoundTripCancelledOutcomeResolvesDeny(t *testing.T) {
	g := permissionGateForTest(t)
	opts, ok := permissionOptionsFromGate(g)
	if !ok {
		t.Fatalf("permissionOptionsFromGate: ok = false")
	}
	session := newGateFakeSession(t)
	client := &gateFakeClient{resp: &protocol.RequestPermissionResponse{
		Outcome: protocol.RequestPermissionOutcome{Cancelled: &struct{}{}},
	}}

	fault := runPermissionGateRoundTrip(context.Background(), client, session, "session-1", g, opts)
	if fault != nil {
		t.Fatalf("runPermissionGateRoundTrip: unexpected fault for a legitimate Cancelled outcome: %v", fault)
	}
	responses := session.gateResponses()
	if len(responses) != 1 || responses[0].Action != string(gate.ApprovalDeny) {
		t.Fatalf("gateResponses = %+v, want exactly one Deny response", responses)
	}
}

// --- fail-closed: RequestPermission itself failing (dead connection, a
// cancelled context, any transport error) still resolves the gate Deny -----

func TestRunPermissionGateRoundTripRequestPermissionErrorFailsClosed(t *testing.T) {
	g := permissionGateForTest(t)
	opts, ok := permissionOptionsFromGate(g)
	if !ok {
		t.Fatalf("permissionOptionsFromGate: ok = false")
	}
	session := newGateFakeSession(t)
	wireErr := &protocol.ConnClosedError{}
	client := &gateFakeClient{err: wireErr}

	fault := runPermissionGateRoundTrip(context.Background(), client, session, "session-1", g, opts)
	if fault == nil {
		t.Fatal("runPermissionGateRoundTrip: fault = nil, want a typed error when RequestPermission itself fails")
	}
	responses := session.gateResponses()
	if len(responses) != 1 {
		t.Fatalf("RespondGate calls = %d, want exactly 1 (fail-closed deny)", len(responses))
	}
	if responses[0].GateID != g.ID {
		t.Errorf("GateID = %v, want %v", responses[0].GateID, g.ID)
	}
	if responses[0].Action != string(gate.ApprovalDeny) {
		t.Errorf("Action = %q, want %q (fail closed, never approve)", responses[0].Action, gate.ApprovalDeny)
	}
	if !errors.Is(fault, wireErr) {
		t.Errorf("fault does not wrap the original RequestPermission error: %v", fault)
	}
}

// failGateClosed must never leave the gate open just because its own deny
// attempt also fails: both errors are preserved, but the caller still gets a
// typed failure back rather than a silent success.
func TestFailGateClosedJoinsOriginalAndDenyErrors(t *testing.T) {
	session := newGateFakeSession(t)
	session.respondErr = errors.New("harness: session gone")
	gateID, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	originalErr := errors.New("wire: connection reset")

	fault := failGateClosed(session, gateID, originalErr)
	if fault == nil {
		t.Fatal("failGateClosed: fault = nil, want a typed error")
	}
	if !errors.Is(fault, originalErr) {
		t.Error("fault does not preserve the original RequestPermission error")
	}
	if !errors.Is(fault, session.respondErr) {
		t.Error("fault does not preserve the RespondGate(deny) failure")
	}
	responses := session.gateResponses()
	if len(responses) != 1 || responses[0].Action != string(gate.ApprovalDeny) {
		t.Fatalf("gateResponses = %+v, want exactly one attempted Deny response", responses)
	}
}

// --- RespondGate itself failing after a VALID selection is a distinct,
// typed failure (not silently treated as success, not retried as deny) -----

func TestRunPermissionGateRoundTripRespondGateFailureAfterValidSelection(t *testing.T) {
	g := permissionGateForTest(t)
	opts, ok := permissionOptionsFromGate(g)
	if !ok {
		t.Fatalf("permissionOptionsFromGate: ok = false")
	}
	session := newGateFakeSession(t)
	session.respondErr = errors.New("harness: gate already closed")
	client := &gateFakeClient{resp: &protocol.RequestPermissionResponse{
		Outcome: protocol.RequestPermissionOutcome{
			Selected: &protocol.SelectedPermissionOutcome{OptionID: protocol.PermissionOptionID(gate.ApprovalApprove)},
		},
	}}

	fault := runPermissionGateRoundTrip(context.Background(), client, session, "session-1", g, opts)
	if fault == nil {
		t.Fatal("runPermissionGateRoundTrip: fault = nil, want a typed error when RespondGate itself fails")
	}
	if !errors.Is(fault, session.respondErr) {
		t.Errorf("fault does not wrap the RespondGate error: %v", fault)
	}
}

// --- gateTracker: the Task 2.7 integration point ---------------------------

func TestGateTrackerCancelSessionOnlyCancelsThatSession(t *testing.T) {
	tracker := newGateTracker()

	sessionA, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	sessionB, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	gateA, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	gateB, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	ctxA, releaseA := tracker.begin(context.Background(), sessionA, gateA)
	ctxB, releaseB := tracker.begin(context.Background(), sessionB, gateB)
	defer releaseA()
	defer releaseB()

	tracker.CancelSession(sessionA)

	select {
	case <-ctxA.Done():
	default:
		t.Fatal("ctxA is not Done after CancelSession(sessionA)")
	}
	select {
	case <-ctxB.Done():
		t.Fatal("ctxB was cancelled by CancelSession(sessionA); it belongs to a different session")
	default:
	}
}

func TestGateTrackerReleaseStopsTrackingAndCancels(t *testing.T) {
	tracker := newGateTracker()
	sessionID, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	gateID, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}

	ctx, release := tracker.begin(context.Background(), sessionID, gateID)
	if len(tracker.open) != 1 {
		t.Fatalf("tracker.open has %d entries, want 1 after begin", len(tracker.open))
	}
	release()
	if len(tracker.open) != 0 {
		t.Fatalf("tracker.open has %d entries, want 0 after release", len(tracker.open))
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("ctx is not Done after release")
	}

	// CancelSession after release must not panic even though the entry is
	// already gone (release, not CancelSession, is the normal end of every
	// round trip's tracked lifetime).
	tracker.CancelSession(sessionID)
}
