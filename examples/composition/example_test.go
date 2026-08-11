package composition_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/launch"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/acp/transport/stdio"
)

const helperEnv = "LOOPRIG_ACP_DOCS_HELPER"

func TestACPDocsHelper(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		return
	}
	conn := protocol.NewConn(os.Stdin, os.Stdout, protocol.ConnOptions{})
	conn.Handle(string(protocol.MethodInitialize), func(context.Context, string, json.RawMessage) (any, error) {
		caps := protocol.DefaultAgentCapabilities()
		return protocol.InitializeResponse{ProtocolVersion: protocol.CurrentProtocolVersion, AgentCapabilities: &caps, AgentInfo: &protocol.Implementation{Name: "docs-fake", Version: "1.0.0"}}, nil
	})
	conn.Handle(string(protocol.MethodSessionNew), func(context.Context, string, json.RawMessage) (any, error) {
		return protocol.NewSessionResponse{SessionID: "123e4567-e89b-42d3-a456-426614174000"}, nil
	})
	if err := stdio.Serve(context.Background(), os.Stdin, os.Stdout, conn); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func helperCommand() stdio.Command {
	path, err := os.Executable()
	if err != nil {
		panic(err)
	}
	return stdio.Command{Path: path, Args: []string{"-test.run=^TestACPDocsHelper$"}, Env: []string{helperEnv + "=1"}}
}

func Example_driveChild() {
	ctx := context.Background()
	c, err := client.Dial(ctx, helperCommand(), client.Options{})
	if err != nil {
		panic(err)
	}
	defer c.Close(ctx)
	meta, _ := c.InitializeMetadata()
	session, err := c.NewSession(ctx, client.NewSessionParams{Cwd: "/workspace"})
	if err != nil {
		panic(err)
	}
	fmt.Printf("agent=%s session=%s\n", meta.AgentInfo.Name, session.ID())
	// Output: agent=docs-fake session=123e4567-e89b-42d3-a456-426614174000
}

type proxy struct{ started, closed bool }

func (p *proxy) Start(context.Context) error   { p.started = true; return nil }
func (*proxy) Binding() (string, string, bool) { return "http://127.0.0.1:4141", "fixture-token", true }
func (p *proxy) Close(context.Context) error   { p.closed = true; return nil }

type adapter struct{}

func (adapter) Configure(cmd stdio.Command, binding launch.ProxyBinding) (stdio.Command, error) {
	if binding.BaseURL == "" || binding.Token == "" {
		return stdio.Command{}, errors.New("missing proxy binding")
	}
	cmd.Env = append(cmd.Env, "DOCS_PROXY_BOUND=1")
	return cmd, nil
}

func Example_proxyBacked() {
	ctx := context.Background()
	p := &proxy{}
	managed, err := launch.Dial(ctx, launch.Config{OwnedProxy: p, Harness: adapter{}, Command: helperCommand()})
	if err != nil {
		panic(err)
	}
	_ = managed.Close(ctx)
	fmt.Printf("proxy-started=%t proxy-closed=%t client=%t\n", p.started, p.closed, managed.Client() != nil)
	// Output: proxy-started=true proxy-closed=true client=true
}

func Example_adapterConfiguration() {
	base := stdio.Command{Path: "/opt/codex-acp", Env: []string{"PATH=/usr/bin"}}
	connector := launch.Codex("gpt-5.6-sol")
	gateway, _ := connector.Configure(base, launch.ProxyBinding{BaseURL: "http://127.0.0.1:4141", Token: "fixture-token"})
	native, _ := connector.ConfigureNative(base)
	fmt.Printf("gateway-provider=%t native-provider=%t\n", contains(gateway.Args, "model_provider=looprig"), contains(native.Args, "model_provider=looprig"))
	// Output: gateway-provider=true native-provider=false
}

func contains(values []string, target string) bool {
	return strings.Contains(strings.Join(values, "\n"), target)
}

func TestInstalledAdapterLive(t *testing.T) {
	path := os.Getenv("ACP_ADAPTER_PATH")
	if path == "" {
		t.Skip("set ACP_ADAPTER_PATH to a clean absolute installed adapter path")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	env := []string{"PATH=" + os.Getenv("PATH")}
	if home := os.Getenv("HOME"); home != "" {
		env = append(env, "HOME="+home)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, stdio.Command{Path: path, Env: env}, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if _, err := c.InitializeMetadata(); err != nil {
		t.Fatal(err)
	}
}
