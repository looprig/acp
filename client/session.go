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

	"github.com/looprig/acp/protocol"
)

// sessionUpdateQueueHint is an initial capacity hint for a Session's
// internal update queue slice. It bounds nothing (the queue is never
// capped — see Session.deliver's doc) — it only avoids a few early
// reallocations for the common case.
const sessionUpdateQueueHint = 8

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

	seenMu sync.Mutex
	seen   map[string]struct{}

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
// yet), buffered internally by an unbounded queue (see deliver) so nothing
// arriving before the caller starts reading is ever dropped. The channel is
// closed once the session is closed (Client.Close, connection death, or a
// future explicit session close) and every already-queued update has been
// delivered.
func (s *Session) Updates() <-chan Update { return s.out }

// deliver routes one decoded update into the session's queue, applying
// live-update dedup: a non-replay update whose _meta.eventId has already
// been seen for this session is dropped (never delivered twice), while a
// replay update (Meta.IsReplay) is exempt from dedup and always delivered —
// per the design doc, replay reconstruction and live streaming are tracked
// as separate concerns, and a replayed update's eventId legitimately
// reappearing (for example, if a client independently retains a replay's
// eventIds across a later live-observed duplicate is never expected in
// practice) must not be silently suppressed. An update with no eventId at
// all (Meta.EventID == "") is never deduplicated: there is nothing to key a
// highwater check on, so it is always delivered.
//
// The internal queue is unbounded (a plain growable slice guarded by mu, not
// a fixed-capacity channel) so a slow consumer can never cause an update to
// be dropped — mirroring protocol.Conn's own notifyQueue/notifyWorker
// pattern for the identical "never drop, never block the sender" property.
func (s *Session) deliver(u Update) {
	if !u.Meta.IsReplay && u.Meta.EventID != "" {
		s.seenMu.Lock()
		_, dup := s.seen[u.Meta.EventID]
		if !dup {
			s.seen[u.Meta.EventID] = struct{}{}
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
	s.mu.Unlock()
	s.cond.Signal()
}

// pump drains the internal queue in order onto the exposed Updates()
// channel, blocking (never dropping) when the consumer is slower than
// delivery. Once closeUpdates has been called AND the queue has been fully
// drained, it closes out and returns: every update queued before closing is
// still delivered, matching Session's "nothing dropped" contract even at
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
		s.mu.Unlock()

		s.out <- u
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
