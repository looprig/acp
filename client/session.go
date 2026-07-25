// session.go implements Session (one ACP session on a Client's connection)
// and Client's session-lifecycle methods: NewSession, LoadSession,
// ResumeSession. Prompt/Cancel live in prompt.go; inbound session/update
// routing and dedup live here (deliver), since they are intrinsic to a
// Session's identity and lifetime.
package client

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/looprig/acp/protocol"
)

// sessionUpdateQueueHint is an initial capacity hint for a Session's
// internal update queue slice. It bounds nothing by itself — it only avoids
// a few early reallocations for the common case — the actual bound is
// UpdateQueueDepth, enforced by deliver.
const sessionUpdateQueueHint = 8

// UpdateQueueDepth bounds how many not-yet-delivered updates a Session's
// internal queue holds before it starts dropping the OLDEST queued update to
// make room for the newest, mirroring protocol.Conn's NotifyBufferDepth
// (see Conn.DroppedNotifications) for the identical bounded-buffer,
// drop-oldest, observable-counter shape. A caller that calls Updates() and
// drains it promptly — the expected steady state, and the shape of every
// existing Task 5.1 test — never sees a drop: this bound only bites a
// consumer that falls behind delivery by more than UpdateQueueDepth updates,
// trading unbounded memory growth in a permanently-non-draining session for
// bounded, observable loss (see Session.DroppedUpdates). The oldest entry is
// dropped rather than the newest because a client actively draining
// Updates() cares about catching up to CURRENT state, not preserving
// ancient history it may never read anyway. 512 matches NotifyBufferDepth's
// existing precedent in this module rather than inventing a new value.
const UpdateQueueDepth = 512

// EventDedupWindowDepth bounds how many distinct live _meta.eventIds a
// Session remembers for dedup (see deliver). Harness event ids
// (event.Header.EventID, minted by event.Factory.Stamp via
// github.com/looprig/core/uuid.New, which reads crypto/rand) are random
// UUIDv4 values, so the id VALUE itself carries no ordering information a
// highwater mark could compare against.
//
// Delivery ORDER does, however, carry a real ordering guarantee: every
// session/update notification for one Client is drained by
// protocol.Conn's single notifyWorker goroutine strictly in wire order,
// completing each job before starting the next (see Conn.notifyWorker's
// doc), so successive calls to deliver for one session happen in true
// chronological order even though the ids themselves are unordered. A
// genuine duplicate (redelivery/retry) is therefore expected to reappear
// shortly after the original, not arbitrarily far in the future — so
// remembering only the most recently delivered EventDedupWindowDepth ids
// (an insertion-order window; the oldest is evicted first once the window
// is full) is enough to catch every realistic duplicate while keeping the
// dedup map's memory bounded across a session's full, potentially
// unbounded, lifetime. An id that reappears after it has aged out of the
// window is (by this deliberate tradeoff) no longer recognized as a
// duplicate and is delivered again — bounded, observable-in-principle loss
// traded for bounded memory, exactly as UpdateQueueDepth trades update loss
// for the same property. 512 matches UpdateQueueDepth/NotifyBufferDepth's
// existing precedent rather than inventing a new value.
const EventDedupWindowDepth = 512

// NewSessionParams are the caller-supplied parameters for Client.NewSession.
type NewSessionParams struct {
	// Cwd is the session's working directory. Must be an absolute path (ACP
	// requirement; enforced by the agent, not re-validated here).
	Cwd string
	// AdditionalDirectories are extra workspace roots. Nil/empty means none.
	AdditionalDirectories []string
	// McpServers are the MCP servers the agent should connect to for this
	// session. Nil is normalized to an empty (but present) list: ACP's
	// session/new request requires the field, never `null`.
	McpServers []protocol.McpServer
}

// LoadSessionParams are the caller-supplied parameters for Client.LoadSession.
type LoadSessionParams struct {
	SessionID             protocol.SessionID
	Cwd                   string
	AdditionalDirectories []string
	McpServers            []protocol.McpServer
}

// ResumeSessionParams are the caller-supplied parameters for
// Client.ResumeSession.
type ResumeSessionParams struct {
	SessionID             protocol.SessionID
	Cwd                   string
	AdditionalDirectories []string
	McpServers            []protocol.McpServer
}

// Session is one ACP session on a Client's connection: its id, its inbound
// session/update stream, and the one-prompt-in-flight gate (see prompt.go).
type Session struct {
	id     protocol.SessionID
	client *Client

	out chan Update

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []Update
	closed bool
	// inFlight is 1 while pump has dequeued an update and is blocked handing
	// it to Updates() (an unbuffered channel with no guaranteed reader), 0
	// otherwise. deliver counts it as still "resident" toward
	// UpdateQueueDepth (see deliver's doc) so the bound is exact regardless
	// of whether pump has had a chance to run: without this, an update
	// pump happens to dequeue early would silently escape the cap
	// accounting (the queue slice alone would look under-full while an
	// extra item sat parked on pump's stack), making both the true resident
	// count and DroppedUpdates undercount by however many times pump won
	// that race.
	inFlight       int
	droppedUpdates atomic.Uint64

	seenMu    sync.Mutex
	seen      map[string]struct{}
	seenOrder []string

	promptSem chan struct{}
}

// ID returns the ACP session id this Session was created or loaded with.
func (s *Session) ID() protocol.SessionID { return s.id }

// newSession constructs a Session and starts its update-delivery pump.
func newSession(client *Client, id protocol.SessionID) *Session {
	s := &Session{
		id:        id,
		client:    client,
		out:       make(chan Update),
		queue:     make([]Update, 0, sessionUpdateQueueHint),
		seen:      make(map[string]struct{}),
		promptSem: make(chan struct{}, 1),
	}
	s.cond = sync.NewCond(&s.mu)
	go s.pump()
	return s
}

// Updates returns the channel Session delivers session/update notifications
// on, typed and decoded. It is ready to receive from immediately: delivery
// begins the moment the Session is registered (at NewSession/LoadSession/
// ResumeSession time, before the caller could possibly have called Updates()
// yet), buffered internally by a queue (see deliver) so nothing arriving
// before the caller starts reading is dropped, up to UpdateQueueDepth. The
// channel is closed once the session is closed (Client.Close, connection
// death, or a future explicit session close) and every already-queued
// update has been delivered.
func (s *Session) Updates() <-chan Update { return s.out }

// DroppedUpdates reports how many queued session/update notifications have
// been dropped (oldest-first) for this Session because its internal queue
// exceeded UpdateQueueDepth — see deliver. This mirrors
// protocol.Conn.DroppedNotifications' shape (a diagnostic counter, never a
// silent failure with no way to observe it) and is distinct from
// Client.DroppedUpdates: the Client-level counter tracks updates that could
// not be routed to any session at all (unknown/unregistered sessionId),
// while this one tracks updates that WERE routed to this exact session but
// then evicted by its own queue bound because the consumer fell behind.
// Zero in the expected steady state of an actively-drained session.
func (s *Session) DroppedUpdates() uint64 { return s.droppedUpdates.Load() }

// deliver routes one decoded update into the session's queue, applying
// live-update dedup: a non-replay update whose _meta.eventId has already
// been seen for this session (within the retained EventDedupWindowDepth
// window — see recordSeenLocked) is dropped (never delivered twice), while
// a replay update (Meta.IsReplay) is exempt from dedup and always delivered
// — per the design doc, replay reconstruction and live streaming are
// tracked as separate concerns, and a replayed update's eventId
// legitimately reappearing (for example, if a client independently retains
// a replay's eventIds across a later live-observed duplicate is never
// expected in practice) must not be silently suppressed. An update with no
// eventId at all (Meta.EventID == "") is never deduplicated: there is
// nothing to key a highwater check on, so it is always delivered.
//
// The internal queue is bounded at UpdateQueueDepth (a growable slice
// guarded by mu, trimmed from the front on overflow), so a consumer that
// never calls Updates() (or falls far enough behind) cannot grow it without
// bound. This deliberately does NOT mirror protocol.Conn's own internal
// notifyQueue, which stays unbounded because it only ever holds already-
// dispatched handler jobs a single worker is actively draining as fast as
// each job completes (see Conn.enqueueNotifyJob's doc) — there is no
// equivalent "always being drained" guarantee here, since draining this
// queue is Updates(), and the whole point of this bound is that the caller
// might never call it. The shape instead mirrors Conn's OTHER bounded
// structure, notifyBuffers (see NotifyBufferDepth): a buffer that holds data
// for a reader who may not show up promptly, capped with the same
// drop-oldest-and-count discipline.
func (s *Session) deliver(u Update) {
	if !u.Meta.IsReplay && u.Meta.EventID != "" {
		s.seenMu.Lock()
		_, dup := s.seen[u.Meta.EventID]
		if !dup {
			s.recordSeenLocked(u.Meta.EventID)
		}
		s.seenMu.Unlock()
		if dup {
			return
		}
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.queue = append(s.queue, u)
	// Resident count includes the one update pump may already be holding
	// in flight (see inFlight's doc): that item is just as "not yet
	// delivered to the caller" as anything still in the slice, so it must
	// count against the same bound rather than escaping it for free.
	if drop := len(s.queue) + s.inFlight - UpdateQueueDepth; drop > 0 {
		if drop > len(s.queue) {
			// Can never actually happen at UpdateQueueDepth=512 (inFlight
			// is always 0 or 1), but guards the slice bound generically
			// rather than assuming today's constant forever.
			drop = len(s.queue)
		}
		// Zero the dropped entries' slots before advancing the slice's start
		// so their referenced content is released promptly rather than kept
		// alive by the backing array until a future reallocation.
		for i := 0; i < drop; i++ {
			s.queue[i] = Update{}
		}
		s.queue = s.queue[drop:]
		// drop is bounded above by len(s.queue) (an in-memory slice length,
		// never anywhere near the uint64 boundary), so this conversion
		// cannot wrap.
		s.droppedUpdates.Add(uint64(drop))
	}
	s.mu.Unlock()
	s.cond.Signal()
}

// recordSeenLocked records eventID as seen for dedup and evicts the oldest
// recorded id (by insertion/delivery order, not by id value — see
// EventDedupWindowDepth's doc) once the retained window would otherwise
// exceed EventDedupWindowDepth. Caller must hold seenMu.
func (s *Session) recordSeenLocked(eventID string) {
	s.seen[eventID] = struct{}{}
	s.seenOrder = append(s.seenOrder, eventID)
	if drop := len(s.seenOrder) - EventDedupWindowDepth; drop > 0 {
		for _, old := range s.seenOrder[:drop] {
			delete(s.seen, old)
		}
		s.seenOrder = s.seenOrder[drop:]
	}
}

// pump drains the internal queue in order onto the exposed Updates()
// channel, blocking (never dropping, from pump's own perspective) when the
// consumer is slower than delivery — deliver is what enforces
// UpdateQueueDepth on the producing side, by counting the one update pump
// may be sitting on (see inFlight) as still resident. Once closeUpdates has
// been called AND the queue has been fully drained, it closes out and
// returns: every update still resident at closing time is still delivered,
// matching Session's "nothing dropped once accepted" contract even at
// shutdown.
func (s *Session) pump() {
	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.closed {
			s.cond.Wait()
		}
		if len(s.queue) == 0 {
			s.mu.Unlock()
			close(s.out)
			return
		}
		u := s.queue[0]
		s.queue[0] = Update{}
		s.queue = s.queue[1:]
		s.inFlight = 1
		s.mu.Unlock()

		s.out <- u

		s.mu.Lock()
		s.inFlight = 0
		s.mu.Unlock()
	}
}

// closeUpdates marks the session closed, so pump exits (after draining
// whatever is already queued) instead of waiting for more.
func (s *Session) closeUpdates() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cond.Broadcast()
}

// registerSession constructs and tracks a new Session under id, so inbound
// session/update notifications for it (which may arrive concurrently with,
// or even before, the caller observes the *Session this returns — see
// LoadSession) are never dropped.
func (c *Client) registerSession(id protocol.SessionID) *Session {
	s := newSession(c, id)
	c.sessionsMu.Lock()
	c.sessions[id] = s
	c.sessionsMu.Unlock()
	return s
}

func (c *Client) unregisterSession(id protocol.SessionID) {
	c.sessionsMu.Lock()
	delete(c.sessions, id)
	c.sessionsMu.Unlock()
}

// closeAllSessions closes every currently-tracked Session's update stream
// and clears the registry. Called on Client.Close and on connection death
// (watchDeath): once the connection is gone, no more updates will ever
// arrive for any session, so every Updates() channel must close rather than
// hang forever.
func (c *Client) closeAllSessions() {
	c.sessionsMu.Lock()
	sessions := c.sessions
	c.sessions = make(map[protocol.SessionID]*Session)
	c.sessionsMu.Unlock()

	for _, s := range sessions {
		s.closeUpdates()
	}
}

// normalizeMcpServers returns servers unchanged if non-nil, or an empty
// (non-nil) slice otherwise: NewSessionRequest/LoadSessionRequest's
// mcpServers field has no `omitempty` in the pinned schema (see
// protocol/types_gen.go), so a nil slice would marshal as JSON `null`
// where the schema requires an array.
func normalizeMcpServers(servers []protocol.McpServer) []protocol.McpServer {
	if servers != nil {
		return servers
	}
	return []protocol.McpServer{}
}

// NewSession calls the agent's session/new method and returns the resulting
// Session, registered so its update stream begins delivering immediately.
func (c *Client) NewSession(ctx context.Context, p NewSessionParams) (*Session, error) {
	agent, err := c.currentAgent()
	if err != nil {
		return nil, err
	}

	resp, err := agent.NewSession(ctx, protocol.NewSessionRequest{
		Cwd:                   p.Cwd,
		AdditionalDirectories: p.AdditionalDirectories,
		McpServers:            normalizeMcpServers(p.McpServers),
	})
	if err != nil {
		return nil, wrapConnError(err)
	}
	return c.registerSession(resp.SessionID), nil
}

// errEmptySessionID reports that a caller-supplied SessionID was empty,
// caught before it ever reaches the wire or the session registry.
var errEmptySessionID = errors.New("acp/client: sessionId is required")

// LoadSession calls the agent's session/load method. The Session is
// registered under p.SessionID BEFORE the call is issued (unlike NewSession,
// the id is caller-supplied here, so this is possible and necessary): a
// foreign agent's session/load handler streams the session's full replay as
// session/update notifications before it ever returns its own response (see
// acp/agent/replay.go's handleSessionLoad), so the Session must already be
// listening the instant the call goes out, not only once it returns.
//
// The call itself is bounded by the Client's load timeout (LoadTimeout,
// overridable via Options.LoadTimeout): replay updates are consumed as they
// arrive regardless of how long the response itself takes, but a load that
// never resolves within the deadline fails with a typed *LoadTimeoutError
// rather than hanging forever or synthesizing a result.
func (c *Client) LoadSession(ctx context.Context, p LoadSessionParams) (*Session, error) {
	if p.SessionID == "" {
		return nil, errEmptySessionID
	}
	agent, err := c.currentAgent()
	if err != nil {
		return nil, err
	}

	sess := c.registerSession(p.SessionID)

	loadCtx, cancel := context.WithTimeout(ctx, c.loadTimeout())
	defer cancel()

	_, err = agent.LoadSession(loadCtx, protocol.LoadSessionRequest{
		SessionID:             p.SessionID,
		Cwd:                   p.Cwd,
		AdditionalDirectories: p.AdditionalDirectories,
		McpServers:            normalizeMcpServers(p.McpServers),
	})
	if err != nil {
		c.unregisterSession(p.SessionID)
		sess.closeUpdates()
		if loadCtx.Err() != nil && ctx.Err() == nil {
			// loadCtx's own deadline fired, not the caller's ctx: report the
			// bounded-wait failure as the typed timeout, not a raw
			// context.DeadlineExceeded from an internally-derived context the
			// caller never created.
			return nil, &LoadTimeoutError{SessionID: p.SessionID, Timeout: c.loadTimeout()}
		}
		return nil, wrapConnError(err)
	}
	return sess, nil
}

// ResumeSession calls the agent's session/resume method. Like LoadSession,
// the Session is registered under the caller-supplied id before the call is
// issued, so any updates the agent sends while resuming are never dropped.
func (c *Client) ResumeSession(ctx context.Context, p ResumeSessionParams) (*Session, error) {
	if p.SessionID == "" {
		return nil, errEmptySessionID
	}
	agent, err := c.currentAgent()
	if err != nil {
		return nil, err
	}

	sess := c.registerSession(p.SessionID)

	_, err = agent.ResumeSession(ctx, protocol.ResumeSessionRequest{
		SessionID:             p.SessionID,
		Cwd:                   p.Cwd,
		AdditionalDirectories: p.AdditionalDirectories,
		McpServers:            p.McpServers,
	})
	if err != nil {
		c.unregisterSession(p.SessionID)
		sess.closeUpdates()
		return nil, wrapConnError(err)
	}
	return sess, nil
}
