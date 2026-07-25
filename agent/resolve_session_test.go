package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/uuid"
)

// TestAgentResolveSessionValidatesBeforeLookup exercises resolveSession
// directly (package-internal, since it is unexported): the shared helper
// every session-scoped handler beyond session/new (Task 2.4's prompt, Task
// 2.6's gates, Task 2.7's cancel/close, and Phase 3's load/resume/list/
// delete) must call before touching the host or any session state. It must
// reject a malformed id without ever consulting the registry, find a
// registered session by its id, and report a well-formed-but-unregistered id
// as not found — never fabricate a result.
func TestAgentResolveSessionValidatesBeforeLookup(t *testing.T) {
	a, err := New(Options{Host: registryFakeHost{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	t.Run("malformed id rejected before any registry lookup", func(t *testing.T) {
		_, err := a.resolveSession(protocol.SessionID("not-a-uuid"))
		if err == nil {
			t.Fatal("resolveSession(malformed): error = nil, want *protocol.Fault")
		}
		var f *protocol.Fault
		if !errors.As(err, &f) {
			t.Fatalf("resolveSession(malformed): error = %v (%T), want *protocol.Fault", err, err)
		}
		if f.Code != protocol.ErrorCodeInvalidParams {
			t.Errorf("resolveSession(malformed): Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeInvalidParams)
		}
	})

	t.Run("well-formed but unregistered id reports not found", func(t *testing.T) {
		id, err := uuid.New()
		if err != nil {
			t.Fatalf("uuid.New: %v", err)
		}
		_, err = a.resolveSession(protocol.SessionID(id.String()))
		if err == nil {
			t.Fatal("resolveSession(unregistered): error = nil, want *protocol.Fault")
		}
		var f *protocol.Fault
		if !errors.As(err, &f) {
			t.Fatalf("resolveSession(unregistered): error = %v (%T), want *protocol.Fault", err, err)
		}
		if f.Code != protocol.ErrorCodeResourceNotFound {
			t.Errorf("resolveSession(unregistered): Fault.Code = %v, want %v", f.Code, protocol.ErrorCodeResourceNotFound)
		}
	})

	t.Run("registered id resolves to its live session", func(t *testing.T) {
		s := newRegistryFakeSession(t)
		if err := a.sessions.add(s); err != nil {
			t.Fatalf("sessions.add: %v", err)
		}
		got, err := a.resolveSession(protocol.SessionID(s.SessionID().String()))
		if err != nil {
			t.Fatalf("resolveSession(registered): unexpected error: %v", err)
		}
		if got.SessionID() != s.SessionID() {
			t.Errorf("resolveSession(registered): SessionID = %v, want %v", got.SessionID(), s.SessionID())
		}
	})
}

// registryFakeHost is a SessionHost whose methods are never expected to be
// called by these tests (resolveSession never touches the host).
type registryFakeHost struct{}

func (registryFakeHost) NewSession(context.Context, Setup) (LiveSession, error) {
	return nil, errors.New("registryFakeHost: NewSession not implemented")
}

func (registryFakeHost) LoadSession(context.Context, SessionID, Setup) (LoadedSession, error) {
	return LoadedSession{}, errors.New("registryFakeHost: LoadSession not implemented")
}

func (registryFakeHost) ResumeSession(context.Context, SessionID, Setup) (LiveSession, error) {
	return nil, errors.New("registryFakeHost: ResumeSession not implemented")
}
