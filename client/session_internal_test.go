package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

type delayedCloseConn struct {
	net.Conn
	closeStarted chan struct{}
	release      chan struct{}
	closeOnce    sync.Once
	releaseOnce  sync.Once
}

func (c *delayedCloseConn) Close() error {
	c.closeOnce.Do(func() { close(c.closeStarted) })
	<-c.release
	return c.Conn.Close()
}

func (c *delayedCloseConn) releaseClose() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func TestNewSessionReturnsRegisteredSession(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		if req.Cwd != "/work" {
			t.Errorf("NewSessionRequest.Cwd = %q, want /work", req.Cwd)
		}
		if req.McpServers == nil {
			t.Error("NewSessionRequest.McpServers = nil, want a non-nil (possibly empty) slice")
		}
		return protocol.NewSessionResponse{SessionID: "sess-abc"}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := c.NewSession(ctx, NewSessionParams{Cwd: "/work"})
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	if sess.ID() != "sess-abc" {
		t.Errorf("Session.ID() = %q, want sess-abc", sess.ID())
	}

	c.sessionsMu.Lock()
	_, tracked := c.sessions[sess.ID()]
	c.sessionsMu.Unlock()
	if !tracked {
		t.Error("NewSession's returned Session is not tracked in the client's registry")
	}
}

func TestNewSessionPropagatesAgentError(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		return protocol.NewSessionResponse{}, protocol.InvalidParams("cwd is required", nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.NewSession(ctx, NewSessionParams{}); err == nil {
		t.Fatal("NewSession() error = nil, want the agent's InvalidParams fault")
	}
}

func TestNewSessionRejectsRegistrationAfterCloseSnapshot(t *testing.T) {
	assertNoGoroutineLeak(t)
	agentSide, clientSide := net.Pipe()
	clientConn := &delayedCloseConn{
		Conn:         clientSide,
		closeStarted: make(chan struct{}),
		release:      make(chan struct{}),
	}
	fa := newFakeAgent(protocol.NewConn(agentSide, agentSide, protocol.ConnOptions{}))
	c := New(stdio.Command{}, Options{})
	c.attemptConnect = func(ctx context.Context) error {
		return c.finishConnect(ctx, nil, clientConn, clientConn)
	}
	t.Cleanup(func() {
		clientConn.releaseClose()
		_ = agentSide.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Dial(ctx); err != nil {
		t.Fatalf("Dial() over delayed-close transport: %v", err)
	}

	rpcStarted := make(chan struct{})
	releaseRPC := make(chan struct{})
	var releaseRPCOnce sync.Once
	release := func() {
		releaseRPCOnce.Do(func() { close(releaseRPC) })
	}
	t.Cleanup(release)
	fa.onNewSession = func(req protocol.NewSessionRequest) (protocol.NewSessionResponse, error) {
		close(rpcStarted)
		<-releaseRPC
		return protocol.NewSessionResponse{SessionID: "sess-late-register"}, nil
	}

	newDone := make(chan struct{})
	var sess *Session
	var newErr error
	go func() {
		sess, newErr = c.NewSession(context.Background(), NewSessionParams{Cwd: "/work"})
		close(newDone)
	}()
	select {
	case <-rpcStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for session/new RPC")
	}

	closeDone := make(chan struct{})
	var closeErr error
	go func() {
		closeErr = c.Close(context.Background())
		close(closeDone)
	}()
	select {
	case <-clientConn.closeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Client.Close to snapshot sessions")
	}

	// Client.Close has snapshotted the registry and is blocked in transport
	// teardown. The response can now complete while registration races with
	// the already-closed registry.
	release()
	select {
	case <-newDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for NewSession after close snapshot")
	}

	var updatesClosed bool
	if sess != nil {
		select {
		case _, open := <-sess.Updates():
			updatesClosed = !open
		case <-time.After(2 * time.Second):
		}
	}
	clientConn.releaseClose()
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Client.Close")
	}
	if closeErr != nil {
		t.Fatalf("Client.Close() error = %v", closeErr)
	}

	var closedErr *ClosedError
	if !errors.As(newErr, &closedErr) {
		if sess != nil && !updatesClosed {
			t.Fatalf("late NewSession registration left Updates() open after Client.Close")
		}
		t.Fatalf("NewSession() error = %v (%T), want *ClosedError after close snapshot", newErr, newErr)
	}
	if sess != nil {
		t.Fatal("NewSession() returned a Session with *ClosedError")
	}

	c.sessionsMu.Lock()
	_, tracked := c.sessions["sess-late-register"]
	c.sessionsMu.Unlock()
	if tracked {
		t.Fatal("late NewSession session remained in the registry after Client.Close")
	}
}

func TestLoadSessionRegistersSessionBeforeCallResolves(t *testing.T) {
	c, fa := dialTestClient(t, Options{})

	reachedHandler := make(chan struct{})
	release := make(chan struct{})
	fa.onLoadSession = func(ctx context.Context, fa *fakeAgent, req protocol.LoadSessionRequest) (protocol.LoadSessionResponse, error) {
		// Stream one replay update before this handler ever returns, exactly
		// like acp/agent/replay.go's handleSessionLoad: the Session this
		// Client is about to register must already be listening for it.
		if err := fa.client.SessionUpdate(ctx, protocol.SessionNotification{
			SessionID: req.SessionID,
			Update:    protocol.SessionUpdate{AgentMessageChunk: &protocol.ContentChunk{Content: protocol.ContentBlock{Text: &protocol.TextContent{Text: "replayed"}}}},
			Meta:      []byte(`{"eventId":"ev-1","isReplay":true}`),
		}); err != nil {
			return protocol.LoadSessionResponse{}, err
		}
		close(reachedHandler)
		<-release
		return protocol.LoadSessionResponse{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	loadDone := make(chan struct{})
	var sess *Session
	var loadErr error
	go func() {
		sess, loadErr = c.LoadSession(ctx, LoadSessionParams{SessionID: "sess-to-load", Cwd: "/work"})
		close(loadDone)
	}()

	select {
	case <-reachedHandler:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for onLoadSession to stream its replay update")
	}

	// The session must already be tracked (and its replay update already
	// queued) even though session/load has not returned yet.
	c.sessionsMu.Lock()
	registered, ok := c.sessions["sess-to-load"]
	c.sessionsMu.Unlock()
	if !ok {
		t.Fatal("session not registered before session/load resolved")
	}

	select {
	case u := <-registered.Updates():
		if u.SessionUpdate.AgentMessageChunk == nil || u.SessionUpdate.AgentMessageChunk.Content.Text.Text != "replayed" {
			t.Errorf("got update %#v, want the replayed agent message chunk", u)
		}
		if !u.Meta.IsReplay {
			t.Error("Meta.IsReplay = false, want true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the pre-registered session's replay update")
	}

	close(release)
	select {
	case <-loadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for LoadSession to return")
	}
	if loadErr != nil {
		t.Fatalf("LoadSession() error = %v", loadErr)
	}
	if sess.ID() != "sess-to-load" {
		t.Errorf("Session.ID() = %q, want sess-to-load", sess.ID())
	}
}

func TestLoadSessionHangTimesOutTyped(t *testing.T) {
	c, fa := dialTestClient(t, Options{LoadTimeout: 50 * time.Millisecond})
	block := make(chan struct{})
	fa.onLoadSession = func(ctx context.Context, fa *fakeAgent, req protocol.LoadSessionRequest) (protocol.LoadSessionResponse, error) {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return protocol.LoadSessionResponse{}, ctx.Err()
	}
	defer close(block)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.LoadSession(ctx, LoadSessionParams{SessionID: "sess-hang", Cwd: "/work"})
	var timeoutErr *LoadTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("LoadSession() error = %v (%T), want *LoadTimeoutError", err, err)
	}

	c.sessionsMu.Lock()
	_, stillTracked := c.sessions["sess-hang"]
	c.sessionsMu.Unlock()
	if stillTracked {
		t.Error("session still tracked after a failed LoadSession; want it unregistered")
	}
}

func TestLoadSessionRejectsEmptySessionID(t *testing.T) {
	c, _ := dialTestClient(t, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.LoadSession(ctx, LoadSessionParams{Cwd: "/work"}); err == nil {
		t.Fatal("LoadSession() with empty SessionID error = nil, want an error")
	}
}

func TestResumeSessionReturnsRegisteredSession(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	fa.onResume = func(req protocol.ResumeSessionRequest) (protocol.ResumeSessionResponse, error) {
		if req.SessionID != "sess-resume" {
			t.Errorf("ResumeSessionRequest.SessionID = %q, want sess-resume", req.SessionID)
		}
		return protocol.ResumeSessionResponse{}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := c.ResumeSession(ctx, ResumeSessionParams{SessionID: "sess-resume", Cwd: "/work"})
	if err != nil {
		t.Fatalf("ResumeSession() error = %v", err)
	}
	if sess.ID() != "sess-resume" {
		t.Errorf("Session.ID() = %q, want sess-resume", sess.ID())
	}
}

func TestOperationsBeforeDialFailTyped(t *testing.T) {
	c := New(stdio.Command{}, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := c.NewSession(ctx, NewSessionParams{Cwd: "/work"})
	var notDialed *NotDialedError
	if !errors.As(err, &notDialed) {
		t.Fatalf("NewSession() before Dial error = %v (%T), want *NotDialedError", err, err)
	}
}
