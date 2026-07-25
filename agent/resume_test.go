package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	coreuuid "github.com/looprig/core/uuid"
)

// resumeHostStub is a SessionHost whose ResumeSession returns a scripted
// LiveSession or error; NewSession/LoadSession are not exercised by these
// tests.
type resumeHostStub struct {
	mu        sync.Mutex
	calls     int
	lastID    SessionID
	lastSetup Setup
	live      LiveSession
	resumeErr error
}

func (h *resumeHostStub) NewSession(context.Context, Setup) (LiveSession, error) {
	return nil, errors.New("resumeHostStub: NewSession not implemented")
}

func (h *resumeHostStub) LoadSession(context.Context, SessionID, Setup) (LoadedSession, error) {
	return LoadedSession{}, errors.New("resumeHostStub: LoadSession not implemented")
}

func (h *resumeHostStub) ResumeSession(_ context.Context, id SessionID, setup Setup) (LiveSession, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	h.lastID = id
	h.lastSetup = setup
	if h.resumeErr != nil {
		return nil, h.resumeErr
	}
	return h.live, nil
}

func (h *resumeHostStub) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// resumeTestSetup wires a resumeHostStub (resuming to live) behind a
// registered Agent, reusing replay_test.go's replayLiveStub and
// replayPipeConns (both plain package-level helpers in this same white-box
// test package) rather than duplicating them.
func resumeTestSetup(t *testing.T) (host *resumeHostStub, live *replayLiveStub, a *Agent, client, server *protocol.Conn) {
	t.Helper()
	live = newReplayLiveStub(t)
	host = &resumeHostStub{live: live}

	var err error
	a, err = New(Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server = replayPipeConns(t)
	a.Register(server)
	return host, live, a, client, server
}

// TestHandleSessionResumeSendsNoNotificationsBeforeResponse is this task's
// central assertion: session/resume must respond having sent ZERO
// session/update notifications, unlike session/load's full replay.
//
// This does more than check "none observed by the time the test looked":
// client and server share one ordered net.Pipe stream (replayPipeConns), and
// the server always fully writes any notification it emits (Conn.Notify)
// before it writes the correlated response (Conn's dispatchRequest only
// builds and writes the response after the handler function returns — see
// conn.go). So if handleSessionResume had ever called a.client.SessionUpdate,
// that notification's frame would have been read off the wire and enqueued
// into the client's notify queue strictly before the response frame that
// unblocks this test's ResumeSession call (collectSessionUpdates' own doc
// explains why enqueue and delivery-to-this-channel are not the same
// instant). Draining with a generous bound AFTER the response has already
// returned therefore checks for a notification that, if it existed at all,
// is already sitting in that queue — not one that might still be racing to
// arrive. Observing nothing under that bound is proof of absence, not
// timing luck.
func TestHandleSessionResumeSendsNoNotificationsBeforeResponse(t *testing.T) {
	host, live, a, client, _ := resumeTestSetup(t)
	updates := collectSessionUpdates(t, client)
	agentConn := protocol.NewAgentConn(client)

	req := protocol.ResumeSessionRequest{SessionID: protocol.SessionID(live.SessionID().String()), Cwd: "/workspace"}
	resp, err := agentConn.ResumeSession(context.Background(), req)
	if err != nil {
		t.Fatalf("ResumeSession: %v", err)
	}
	if resp == nil {
		t.Fatal("ResumeSession: resp = nil")
	}
	if host.callCount() != 1 {
		t.Fatalf("Host.ResumeSession calls = %d, want 1", host.callCount())
	}

	select {
	case n := <-updates:
		t.Fatalf("session/update notification observed for session/resume: %+v (must send ZERO before responding)", n)
	case <-time.After(200 * time.Millisecond):
	}

	registered, ok := a.sessions.get(live.SessionID())
	if !ok || registered != live {
		t.Errorf("sessions.get(%v) = (%v, %v), want (live, true)", live.SessionID(), registered, ok)
	}
}

// TestHandleSessionResumeUsesHostSetupAndID asserts the request's sessionId
// and cwd reach Host.ResumeSession as the resolved SessionID and validated
// Setup, exactly like handleSessionLoad's identical contract.
func TestHandleSessionResumeUsesHostSetupAndID(t *testing.T) {
	host, live, _, client, _ := resumeTestSetup(t)
	agentConn := protocol.NewAgentConn(client)

	req := protocol.ResumeSessionRequest{SessionID: protocol.SessionID(live.SessionID().String()), Cwd: "/workspace"}
	if _, err := agentConn.ResumeSession(context.Background(), req); err != nil {
		t.Fatalf("ResumeSession: %v", err)
	}

	if host.lastID != live.SessionID() {
		t.Errorf("Host.ResumeSession id = %v, want %v", host.lastID, live.SessionID())
	}
	if host.lastSetup.Cwd != "/workspace" {
		t.Errorf("Host.ResumeSession Setup.Cwd = %q, want /workspace", host.lastSetup.Cwd)
	}
}

// TestHandleSessionResumeRejectsMalformedSessionID asserts a malformed wire
// sessionId is rejected by ParseSessionID before the host is ever touched,
// matching every other session-scoped handler's validation discipline.
func TestHandleSessionResumeRejectsMalformedSessionID(t *testing.T) {
	host, _, _, client, _ := resumeTestSetup(t)
	agentConn := protocol.NewAgentConn(client)

	_, err := agentConn.ResumeSession(context.Background(), protocol.ResumeSessionRequest{
		SessionID: "not-a-uuid", Cwd: "/workspace",
	})
	if err == nil {
		t.Fatal("ResumeSession(malformed sessionId): error = nil, want InvalidParams fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
	}
	if host.callCount() != 0 {
		t.Errorf("Host.ResumeSession calls = %d, want 0 (must fail closed before touching host)", host.callCount())
	}
}

// TestHandleSessionResumeRejectsInvalidSetup asserts a malformed Setup field
// (a non-absolute cwd) is rejected with InvalidParams before the host is
// ever called, reusing NewSetup's validation rather than duplicating it.
func TestHandleSessionResumeRejectsInvalidSetup(t *testing.T) {
	host, live, _, client, _ := resumeTestSetup(t)
	agentConn := protocol.NewAgentConn(client)

	_, err := agentConn.ResumeSession(context.Background(), protocol.ResumeSessionRequest{
		SessionID: protocol.SessionID(live.SessionID().String()), Cwd: "relative/path",
	})
	if err == nil {
		t.Fatal("ResumeSession(relative cwd): error = nil, want InvalidParams fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInvalidParams {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
	}
	if host.callCount() != 0 {
		t.Errorf("Host.ResumeSession calls = %d, want 0 (must fail closed before touching host)", host.callCount())
	}
}

// TestHandleSessionResumeRejectsWhenAuthRequired asserts session/resume
// reuses Task 2.2's AuthorizeSessionCreation gate rather than reimplementing
// it, matching session/new's and session/load's identical rule.
func TestHandleSessionResumeRejectsWhenAuthRequired(t *testing.T) {
	host := &resumeHostStub{}
	opts := Options{
		Host:          host,
		Authenticator: fakeAuthenticatorForResume{},
		AuthMethods:   []protocol.AuthMethod{{ID: "test-method", Name: "Test"}},
	}
	a, err := New(opts)
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := replayPipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	validID := coreuuid.MustParse("11111111-1111-4111-8111-111111111111")
	_, err = agentConn.ResumeSession(context.Background(), protocol.ResumeSessionRequest{
		SessionID: protocol.SessionID(validID.String()), Cwd: "/workspace",
	})
	if err == nil {
		t.Fatal("ResumeSession before authenticate: error = nil, want AuthenticationRequired fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeAuthenticationRequired {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeAuthenticationRequired)
	}
	if host.callCount() != 0 {
		t.Errorf("Host.ResumeSession calls = %d, want 0 (must fail closed before touching host)", host.callCount())
	}
}

// fakeAuthenticatorForResume is a minimal Authenticator that never
// succeeds, used only to prove AuthorizeSessionCreation's gate short-circuits
// session/resume before the Authenticator (or the host) is ever consulted.
type fakeAuthenticatorForResume struct{}

func (fakeAuthenticatorForResume) Authenticate(context.Context, protocol.AuthMethodID) error {
	return errors.New("fakeAuthenticatorForResume: Authenticate not implemented")
}

// TestHandleSessionResumeCapacityRejected asserts the capacity pre-check
// short-circuits before the host is ever touched, matching handleSessionNew's
// and handleSessionLoad's own capacity behavior.
func TestHandleSessionResumeCapacityRejected(t *testing.T) {
	host, _, a, client, _ := resumeTestSetup(t)

	for i := 0; i < MaxLiveSessions; i++ {
		id, err := coreuuid.New()
		if err != nil {
			t.Fatalf("uuid.New: %v", err)
		}
		if err := a.sessions.add(&replayLiveStub{id: id}); err != nil {
			t.Fatalf("sessions.add: %v", err)
		}
	}

	agentConn := protocol.NewAgentConn(client)
	validID := coreuuid.MustParse("11111111-1111-4111-8111-111111111111")
	_, err := agentConn.ResumeSession(context.Background(), protocol.ResumeSessionRequest{
		SessionID: protocol.SessionID(validID.String()), Cwd: "/workspace",
	})
	if err == nil {
		t.Fatal("ResumeSession at capacity: error = nil, want rejection")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeInternalError {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInternalError)
	}
	if host.callCount() != 0 {
		t.Errorf("Host.ResumeSession calls = %d, want 0 (capacity pre-check must short-circuit before touching host)", host.callCount())
	}
}

// TestHandleSessionResumeHostErrorPassesThroughTypedFault asserts a
// *protocol.Fault returned by Host.ResumeSession is passed through unchanged,
// matching handleSessionNew's and handleSessionLoad's identical rule.
func TestHandleSessionResumeHostErrorPassesThroughTypedFault(t *testing.T) {
	wantFault := protocol.ResourceNotFound("session/resume: no such durable session", nil)
	host := &resumeHostStub{resumeErr: wantFault}
	a, err := New(Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := replayPipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	validID := coreuuid.MustParse("11111111-1111-4111-8111-111111111111")
	_, err = agentConn.ResumeSession(context.Background(), protocol.ResumeSessionRequest{
		SessionID: protocol.SessionID(validID.String()), Cwd: "/workspace",
	})
	if err == nil {
		t.Fatal("ResumeSession: error = nil, want the host's Fault")
	}
	var f *protocol.Fault
	if !errors.As(err, &f) {
		t.Fatalf("error = %v (%T), want *protocol.Fault", err, err)
	}
	if f.Code != protocol.ErrorCodeResourceNotFound {
		t.Errorf("Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeResourceNotFound)
	}
}

// TestHandleSessionResumeRegisteredWithMinimalOptions asserts session/resume
// works with only the always-required Host set — no Replayer, Catalog, or
// any other optional capability field — proving it is NOT gated on any
// Options field the way session/load is gated on Replayer.
func TestHandleSessionResumeRegisteredWithMinimalOptions(t *testing.T) {
	live := newReplayLiveStub(t)
	host := &resumeHostStub{live: live}
	a, err := New(Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := replayPipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	resp, err := agentConn.ResumeSession(context.Background(), protocol.ResumeSessionRequest{
		SessionID: protocol.SessionID(live.SessionID().String()), Cwd: "/workspace",
	})
	if err != nil {
		t.Fatalf("ResumeSession with minimal Options: %v", err)
	}
	if resp == nil {
		t.Fatal("ResumeSession: resp = nil")
	}
}

// --- orphan cleanup on registry-capacity race --------------------------

// gatingResumeHost is a SessionHost whose ResumeSession hands back the next
// queued live session, signaling entry on entered and blocking on release —
// the same barrier technique replay_test.go's gatingLoadHost and
// session_test.go's gatingHostStub use — so both racing session/resume calls
// genuinely complete Host.ResumeSession before either's sessions.add call
// can run.
type gatingResumeHost struct {
	mu      sync.Mutex
	queue   []*replayLiveStub
	entered chan struct{}
	release chan struct{}
}

func (h *gatingResumeHost) NewSession(context.Context, Setup) (LiveSession, error) {
	return nil, errors.New("gatingResumeHost: NewSession not implemented")
}

func (h *gatingResumeHost) LoadSession(context.Context, SessionID, Setup) (LoadedSession, error) {
	return LoadedSession{}, errors.New("gatingResumeHost: LoadSession not implemented")
}

func (h *gatingResumeHost) ResumeSession(_ context.Context, id SessionID, _ Setup) (LiveSession, error) {
	h.mu.Lock()
	var chosen *replayLiveStub
	for i, live := range h.queue {
		if live.id == id {
			chosen = live
			h.queue = append(h.queue[:i], h.queue[i+1:]...)
			break
		}
	}
	h.mu.Unlock()
	if chosen == nil {
		return nil, errors.New("gatingResumeHost: unknown session id")
	}

	h.entered <- struct{}{}
	<-h.release

	return chosen, nil
}

// TestHandleSessionResumeShutsDownOrphanOnRegistryCapacityRace mirrors
// replay_test.go's TestHandleSessionLoadShutsDownOrphanOnRegistryCapacityRace
// (and session_test.go's identical session/new case) for session/resume: two
// concurrent session/resume calls both pass the advisory atCapacity
// pre-check and both genuinely obtain a real host-backed LiveSession before
// either's sessions.add call can run; only one can win the registry's last
// slot, and the loser's live session must get the same best-effort Shutdown
// cleanup, never be silently abandoned.
func TestHandleSessionResumeShutsDownOrphanOnRegistryCapacityRace(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})

	liveA := newReplayLiveStub(t)
	liveB := newReplayLiveStub(t)
	host := &gatingResumeHost{queue: []*replayLiveStub{liveA, liveB}, entered: entered, release: release}
	a, err := New(Options{Host: host})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	client, server := replayPipeConns(t)
	a.Register(server)
	agentConn := protocol.NewAgentConn(client)

	// Fill to MaxLiveSessions-1 directly (white-box), leaving exactly one slot.
	for i := 0; i < MaxLiveSessions-1; i++ {
		id, err := coreuuid.New()
		if err != nil {
			t.Fatalf("uuid.New: %v", err)
		}
		if err := a.sessions.add(&replayLiveStub{id: id}); err != nil {
			t.Fatalf("sessions.add: %v", err)
		}
	}

	type result struct{ err error }
	results := make(chan result, 2)
	callResume := func(id coreuuid.UUID) {
		_, err := agentConn.ResumeSession(context.Background(), protocol.ResumeSessionRequest{
			SessionID: protocol.SessionID(id.String()), Cwd: "/workspace",
		})
		results <- result{err: err}
	}
	go callResume(liveA.id)
	go callResume(liveB.id)

	<-entered
	<-entered
	close(release)

	r1 := <-results
	r2 := <-results

	winners, losers := 0, 0
	for _, r := range []result{r1, r2} {
		if r.err == nil {
			winners++
		} else {
			losers++
		}
	}
	if winners != 1 || losers != 1 {
		t.Fatalf("winners=%d losers=%d, want exactly 1 each (one registry slot available)", winners, losers)
	}

	_, aOK := a.sessions.get(liveA.id)
	_, bOK := a.sessions.get(liveB.id)
	if aOK == bOK {
		t.Fatalf("exactly one of liveA/liveB should be registered, got aOK=%v bOK=%v", aOK, bOK)
	}

	loser := liveA
	if aOK {
		loser = liveB
	}
	if got := loser.shutdownCallCount(); got != 1 {
		t.Errorf("loser Shutdown calls = %d, want exactly 1", got)
	}
}
