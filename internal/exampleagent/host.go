package main

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/looprig/acp/agent"
	"github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/journal"
	"github.com/looprig/harness/pkg/sessionstore"
)

// sessionState is the durable, cross-instance state one ACP session's
// identity owns: its metadata (for session/list) and its append-only durable
// event log (for session/load's replay). It survives session/close — Host
// keeps it in memory for the lifetime of the process, exactly the way a real
// product's durable store would survive a process's own session/close, so
// session/load after a close can reconstruct real history rather than
// nothing at all. It is shared (by pointer) across every liveSession
// instance ever constructed for the same session id (session/new's original,
// and any later session/resume or session/load's fresh instance).
type sessionState struct {
	loopID uuid.UUID

	mu           sync.Mutex
	cwd          string
	title        string
	createdAt    time.Time
	lastActiveAt time.Time

	log *durableLog
}

// touch updates lastActiveAt and, the first time a non-empty title is
// supplied, records it — mirroring sessionstore's own "Title ... derived from
// the first turn's user message ... empty until a first TurnStarted is seen"
// contract (see harness/pkg/sessionstore/catalog.go's SessionMeta.Title doc).
func (s *sessionState) touch(now time.Time, title string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActiveAt = now
	if s.title == "" && title != "" {
		s.title = title
	}
}

func (s *sessionState) snapshot() (cwd, title string, createdAt, lastActiveAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cwd, s.title, s.createdAt, s.lastActiveAt
}

// durableLog is an in-memory, append-only record of one session's Enduring
// events: exactly the two kinds replay.go's buildReplayNotifications actually
// reconstructs from (event.TurnStarted, event.StepDone) — see agent/replay.go's
// package doc. It is safe for concurrent append and snapshot.
type durableLog struct {
	mu      sync.Mutex
	records []event.Event
}

func newDurableLog() *durableLog { return &durableLog{} }

func (d *durableLog) append(ev event.Event) {
	d.mu.Lock()
	d.records = append(d.records, ev)
	d.mu.Unlock()
}

// turnCount reports how many event.TurnStarted records have been durably
// appended so far — the next turn's 0-based event.TurnIndex.
func (d *durableLog) turnCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, ev := range d.records {
		if _, ok := ev.(event.TurnStarted); ok {
			n++
		}
	}
	return n
}

func (d *durableLog) snapshot() []event.Event {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]event.Event(nil), d.records...)
}

// eventCursor is a journal.EventCursor over one durableLog snapshot, take at
// Open time: it never observes appends made after it was opened, matching a
// real cold backlog cursor's contract (journal.ReplayRequest.Follow is never
// honored here — see eventReplayer.Open).
type eventCursor struct {
	records []event.Event
	pos     int
}

func (c *eventCursor) Next(ctx context.Context) (event.Event, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if c.pos >= len(c.records) {
		return nil, 0, io.EOF
	}
	ev := c.records[c.pos]
	c.pos++
	// Sequences are 1-based, matching journal.StartPos's documented
	// convention ("Log sequences are 1-based").
	// #nosec G115 -- c.pos is a slice index bounded by len(c.records) just
	// above (never negative, never anywhere near the int->uint64 boundary
	// for an in-memory test log), not externally controlled input.
	return ev, uint64(c.pos), nil
}

func (c *eventCursor) Close() error { return nil }

// errFollowUnsupported reports that a caller asked for a live-tailing replay
// (journal.ReplayRequest.Follow), which this in-memory example replayer never
// implements — the facade's own session/load path never sets Follow (see
// agent/replay.go's replayHistory: `journal.ReplayRequest{... From:
// journal.Beginning()}`, Follow left at its zero value), so this is a
// defensive fail-closed guard, not a path any of this module's real callers
// exercise.
var errFollowUnsupported = errors.New("exampleagent: follow (live-tailing) replay is not supported")

// eventReplayer implements journal.EventReplayer over one session's
// durableLog.
type eventReplayer struct{ log *durableLog }

func (r *eventReplayer) Open(ctx context.Context, req journal.ReplayRequest) (journal.EventCursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.Follow {
		return nil, errFollowUnsupported
	}
	return &eventCursor{records: r.log.snapshot()}, nil
}

// Host is exampleagent's SessionHost, agent.EventReplayer, and
// agent.SessionCatalog: a purely in-memory composition root with no product
// dependency of any kind. It is the concrete type main.go wires into
// agent.Options.
type Host struct {
	mu     sync.Mutex
	states map[uuid.UUID]*sessionState
}

// NewHost constructs an empty Host.
func NewHost() *Host {
	return &Host{states: make(map[uuid.UUID]*sessionState)}
}

// errUnknownSession reports that a caller-supplied ACP session id names no
// session this Host has ever created — never resumed, loaded, or listed.
var errUnknownSession = errors.New("exampleagent: unknown session id")

// NewSession implements agent.SessionHost: it mints a fresh session identity,
// a fresh loop id (loopID is fixed for the life of a session's identity, even
// across a later session/load or session/resume — see sessionState's doc),
// and an empty durable log, then returns the live controller bound to it.
func (h *Host) NewSession(_ context.Context, setup agent.Setup) (agent.LiveSession, error) {
	id, err := uuid.New()
	if err != nil {
		return nil, err
	}
	loopID, err := uuid.New()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	st := &sessionState{
		loopID:    loopID,
		cwd:       setup.Cwd,
		createdAt: now,
		log:       newDurableLog(),
	}

	h.mu.Lock()
	h.states[id] = st
	h.mu.Unlock()

	return newLiveSession(id, st), nil
}

// LoadSession implements agent.SessionHost: it looks up the existing
// sessionState for id (created by an earlier NewSession in this same
// process — exampleagent never restarts, so there is nothing to restore from
// disk) and hands back a fresh liveSession bound to the SAME durable log and
// metadata, plus the replay boundary (the last durably committed turn).
func (h *Host) LoadSession(_ context.Context, id agent.SessionID, setup agent.Setup) (agent.LoadedSession, error) {
	st, ok := h.lookup(id)
	if !ok {
		return agent.LoadedSession{}, errUnknownSession
	}
	st.mu.Lock()
	st.cwd = setup.Cwd
	st.mu.Unlock()

	replayedThrough := event.TurnIndex(st.log.turnCount() - 1)
	return agent.LoadedSession{Live: newLiveSession(id, st), ReplayedThrough: replayedThrough}, nil
}

// ResumeSession implements agent.SessionHost: like LoadSession, it resumes
// the existing sessionState under a fresh liveSession instance, but performs
// no replay (see agent/resume.go's doc: "no replay anchor and therefore no
// durable-history reconstruction to perform").
func (h *Host) ResumeSession(_ context.Context, id agent.SessionID, setup agent.Setup) (agent.LiveSession, error) {
	st, ok := h.lookup(id)
	if !ok {
		return nil, errUnknownSession
	}
	st.mu.Lock()
	st.cwd = setup.Cwd
	st.mu.Unlock()
	return newLiveSession(id, st), nil
}

func (h *Host) lookup(id uuid.UUID) (*sessionState, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st, ok := h.states[id]
	return st, ok
}

// OpenEventReplayer implements agent.EventReplayer.
func (h *Host) OpenEventReplayer(id agent.SessionID) (journal.EventReplayer, error) {
	st, ok := h.lookup(id)
	if !ok {
		return nil, errUnknownSession
	}
	return &eventReplayer{log: st.log}, nil
}

// ListSessions implements agent.SessionCatalog: every session this process
// has ever created, live or not, sorted by no particular order (handleSessionList
// sorts by SessionID itself — see agent/list.go).
func (h *Host) ListSessions(_ context.Context) ([]agent.SessionCatalogEntry, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	entries := make([]agent.SessionCatalogEntry, 0, len(h.states))
	for id, st := range h.states {
		cwd, title, createdAt, lastActiveAt := st.snapshot()
		entries = append(entries, agent.SessionCatalogEntry{
			Meta: sessionstore.SessionMeta{
				SessionID:    id,
				Title:        title,
				CreatedAt:    createdAt,
				LastActiveAt: lastActiveAt,
			},
			Cwd: cwd,
		})
	}
	return entries, nil
}
