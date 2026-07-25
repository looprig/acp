// registry.go is the bounded, concurrency-safe registry of live ACP
// sessions the facade tracks between session/new and each later
// session-scoped method (Task 2.3 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md).
package agent

import (
	"strconv"
	"sync"
)

// MaxLiveSessions bounds how many ACP sessions this facade tracks
// concurrently as live, registered sessions. session/new fails closed with a
// *TooManyLiveSessionsError once the registry is already at this capacity,
// rather than growing without bound.
const MaxLiveSessions = 64

// TooManyLiveSessionsError reports that the live-session registry was
// already at capacity when a session tried to register.
type TooManyLiveSessionsError struct {
	// Max is the capacity that was reached (MaxLiveSessions in production;
	// a test may construct a registry with a smaller bound).
	Max int
}

func (e *TooManyLiveSessionsError) Error() string {
	return "agent: live session registry at capacity (" + strconv.Itoa(e.Max) + ")"
}

// sessionRegistry is a bounded map of live ACP sessions keyed by their
// Harness session UUID (SessionID). It is the facade's single source of
// truth for which session ids currently name a live session; resolveSession
// is its only reader outside this file.
type sessionRegistry struct {
	mu       sync.Mutex
	sessions map[SessionID]LiveSession
	max      int
}

// newSessionRegistry constructs an empty registry bounded at max entries.
func newSessionRegistry(max int) *sessionRegistry {
	return &sessionRegistry{sessions: make(map[SessionID]LiveSession), max: max}
}

// atCapacity reports whether the registry already holds max entries. It is
// an advisory pre-check a caller uses to avoid unnecessary host work (e.g.
// SessionHost.NewSession) when the registry cannot accept the result anyway.
// add's own bounded check is the actual point of enforcement: a caller must
// still call add and handle its error even after atCapacity returns false,
// since a concurrent registration can fill the last slot in between.
func (r *sessionRegistry) atCapacity() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sessions) >= r.max
}

// add registers s under its own SessionID(), failing closed with a
// *TooManyLiveSessionsError if the registry is already at capacity. This
// holds the lock for the full read-check-write, so it is correct even when
// multiple sessions race to register at once (unlike atCapacity followed by
// a separate, unsynchronized add call).
func (r *sessionRegistry) add(s LiveSession) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := s.SessionID()
	if _, exists := r.sessions[id]; !exists && len(r.sessions) >= r.max {
		return &TooManyLiveSessionsError{Max: r.max}
	}
	r.sessions[id] = s
	return nil
}

// get looks up a registered live session by its already-validated
// SessionID. Callers must validate the wire sessionId string (ParseSessionID)
// before calling get; get itself trusts its argument is already a
// well-formed SessionID and only reports whether it names a currently
// registered session.
func (r *sessionRegistry) get(id SessionID) (LiveSession, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[id]
	return s, ok
}

// remove drops id from the registry. It is a no-op for an id that is not
// (or is no longer) registered.
func (r *sessionRegistry) remove(id SessionID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, id)
}

// len reports the current number of registered sessions.
func (r *sessionRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sessions)
}
