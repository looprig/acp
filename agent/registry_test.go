package agent

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/looprig/core/content"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
)

// registryFakeSession is a minimal LiveSession stub for exercising
// sessionRegistry in isolation from any host or handler: only SessionID is
// ever read by the registry, so the rest of the interface is unused
// boilerplate to satisfy LiveSession's full contract (Liskov substitution:
// this fake must still honor every method, even though these tests never
// call them).
type registryFakeSession struct{ id uuid.UUID }

func (s registryFakeSession) SessionID() uuid.UUID { return s.id }

func (s registryFakeSession) Submit(context.Context, []content.Block) (uuid.UUID, error) {
	return uuid.UUID{}, errors.New("registryFakeSession: Submit not implemented")
}

func (s registryFakeSession) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errors.New("registryFakeSession: SubscribeEvents not implemented")
}

func (s registryFakeSession) RespondGate(context.Context, gate.GateResponse) error {
	return errors.New("registryFakeSession: RespondGate not implemented")
}

func (s registryFakeSession) Interrupt(context.Context) (bool, error) {
	return false, errors.New("registryFakeSession: Interrupt not implemented")
}

func newRegistryFakeSession(t *testing.T) registryFakeSession {
	t.Helper()
	id, err := uuid.New()
	if err != nil {
		t.Fatalf("uuid.New: %v", err)
	}
	return registryFakeSession{id: id}
}

// TestSessionRegistryBoundedAtMax pins the exact boundary semantics: a
// registry sized for max entries accepts exactly max additions and rejects
// the (max+1)th with a *TooManyLiveSessionsError, leaving the registry's
// contents unchanged by the rejected attempt.
func TestSessionRegistryBoundedAtMax(t *testing.T) {
	const max = 4
	r := newSessionRegistry(max)

	for i := 0; i < max; i++ {
		if err := r.add(newRegistryFakeSession(t)); err != nil {
			t.Fatalf("add #%d (within capacity): unexpected error: %v", i+1, err)
		}
	}
	if got := r.len(); got != max {
		t.Fatalf("len after filling to capacity = %d, want %d", got, max)
	}

	err := r.add(newRegistryFakeSession(t))
	if err == nil {
		t.Fatal("add beyond capacity: error = nil, want *TooManyLiveSessionsError")
	}
	var tooMany *TooManyLiveSessionsError
	if !errors.As(err, &tooMany) {
		t.Fatalf("add beyond capacity: error = %v (%T), want *TooManyLiveSessionsError", err, err)
	}
	if tooMany.Max != max {
		t.Errorf("TooManyLiveSessionsError.Max = %d, want %d", tooMany.Max, max)
	}
	if got := r.len(); got != max {
		t.Errorf("len after rejected add = %d, want unchanged %d", got, max)
	}
}

// TestSessionRegistryAtCapacity checks the advisory pre-check in isolation:
// false while under max, true once max is reached.
func TestSessionRegistryAtCapacity(t *testing.T) {
	const max = 2
	r := newSessionRegistry(max)
	if r.atCapacity() {
		t.Fatal("atCapacity() on empty registry = true, want false")
	}
	if err := r.add(newRegistryFakeSession(t)); err != nil {
		t.Fatalf("add: unexpected error: %v", err)
	}
	if r.atCapacity() {
		t.Fatal("atCapacity() at 1/2 = true, want false")
	}
	if err := r.add(newRegistryFakeSession(t)); err != nil {
		t.Fatalf("add: unexpected error: %v", err)
	}
	if !r.atCapacity() {
		t.Fatal("atCapacity() at 2/2 = false, want true")
	}
}

// TestSessionRegistryGetAndRemove covers the ordinary lookup/removal path:
// a registered session is found by its id, removal drops it, and looking up
// an id that was never registered (or already removed) reports not-found
// rather than panicking or fabricating a zero value.
func TestSessionRegistryGetAndRemove(t *testing.T) {
	r := newSessionRegistry(8)
	s := newRegistryFakeSession(t)

	if _, ok := r.get(s.SessionID()); ok {
		t.Fatal("get before add: ok = true, want false")
	}

	if err := r.add(s); err != nil {
		t.Fatalf("add: unexpected error: %v", err)
	}
	got, ok := r.get(s.SessionID())
	if !ok {
		t.Fatal("get after add: ok = false, want true")
	}
	if got.SessionID() != s.SessionID() {
		t.Errorf("get after add: SessionID = %v, want %v", got.SessionID(), s.SessionID())
	}

	r.remove(s.SessionID())
	if _, ok := r.get(s.SessionID()); ok {
		t.Fatal("get after remove: ok = true, want false")
	}

	// Removing an id that is not (or no longer) registered must be a no-op,
	// not a panic.
	r.remove(s.SessionID())
}

// TestSessionRegistryConcurrentAddGetRemove exercises add/get/remove from
// many goroutines at once (run with -race, per every task's ground rules)
// to confirm the registry's single mutex actually serializes access rather
// than merely looking correct in a single-threaded test.
func TestSessionRegistryConcurrentAddGetRemove(t *testing.T) {
	const n = 50
	r := newSessionRegistry(n)
	sessions := make([]registryFakeSession, n)
	for i := range sessions {
		sessions[i] = newRegistryFakeSession(t)
	}

	var wg sync.WaitGroup
	var addErrs int32
	for _, s := range sessions {
		wg.Add(1)
		go func(s registryFakeSession) {
			defer wg.Done()
			if err := r.add(s); err != nil {
				atomic.AddInt32(&addErrs, 1)
			}
		}(s)
	}
	wg.Wait()
	if addErrs != 0 {
		t.Fatalf("concurrent add errors = %d, want 0 (registry sized exactly to session count)", addErrs)
	}
	if got := r.len(); got != n {
		t.Fatalf("len after concurrent add = %d, want %d", got, n)
	}

	var missing int32
	for _, s := range sessions {
		wg.Add(1)
		go func(id uuid.UUID) {
			defer wg.Done()
			if _, ok := r.get(id); !ok {
				atomic.AddInt32(&missing, 1)
			}
			r.remove(id)
		}(s.SessionID())
	}
	wg.Wait()
	if missing != 0 {
		t.Errorf("concurrent get misses = %d, want 0", missing)
	}
	if got := r.len(); got != 0 {
		t.Errorf("len after concurrent remove = %d, want 0", got)
	}
}
