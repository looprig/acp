package host_test

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/looprig/acp/agent"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	coreuuid "github.com/looprig/core/uuid"
	"github.com/looprig/harness/pkg/event"
	"github.com/looprig/harness/pkg/gate"
)

type host struct{ setup agent.Setup }

func (h *host) NewSession(_ context.Context, setup agent.Setup) (agent.LiveSession, error) {
	h.setup = setup
	return &liveSession{id: coreuuid.MustParse("123e4567-e89b-42d3-a456-426614174000")}, nil
}
func (*host) LoadSession(context.Context, agent.SessionID, agent.Setup) (agent.LoadedSession, error) {
	return agent.LoadedSession{}, errors.New("not implemented")
}
func (*host) ResumeSession(context.Context, agent.SessionID, agent.Setup) (agent.LiveSession, error) {
	return nil, errors.New("not implemented")
}

type liveSession struct{ id coreuuid.UUID }

func (s *liveSession) SessionID() coreuuid.UUID { return s.id }
func (*liveSession) Submit(context.Context, []content.Block) (coreuuid.UUID, error) {
	return coreuuid.UUID{}, errors.New("not implemented")
}
func (*liveSession) SubscribeEvents(event.EventFilter) (event.Subscription, error) {
	return nil, errors.New("not implemented")
}
func (*liveSession) RespondGate(context.Context, gate.GateResponse) error {
	return errors.New("not implemented")
}
func (*liveSession) Interrupt(context.Context) (bool, error) { return false, nil }

type authenticator struct{ called bool }

func (a *authenticator) Authenticate(_ context.Context, id protocol.AuthMethodID) error {
	a.called = id == "local"
	return nil
}

func Example_exposeHost() {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	h := &host{}
	auth := &authenticator{}
	facade, err := agent.New(agent.Options{Host: h, Authenticator: auth, AuthMethods: []protocol.AuthMethod{{ID: "local", Name: "Local fixture"}}})
	if err != nil {
		panic(err)
	}
	server := protocol.NewConn(right, right, protocol.ConnOptions{})
	defer server.Close()
	facade.Register(server)
	peer := protocol.NewConn(left, left, protocol.ConnOptions{})
	defer peer.Close()
	rpc := protocol.NewAgentConn(peer)
	ctx := context.Background()
	init, _ := rpc.Initialize(ctx, protocol.InitializeRequest{ProtocolVersion: protocol.CurrentProtocolVersion})
	_, _ = rpc.Authenticate(ctx, protocol.AuthenticateRequest{MethodID: init.AuthMethods[0].ID})
	session, _ := rpc.NewSession(ctx, protocol.NewSessionRequest{Cwd: "/workspace", McpServers: []protocol.McpServer{}})
	fmt.Printf("auth=%t cwd=%s session=%s\n", auth.called, h.setup.Cwd, session.SessionID)
	// Output: auth=true cwd=/workspace session=123e4567-e89b-42d3-a456-426614174000
}
