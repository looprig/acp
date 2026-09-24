# acp

`github.com/looprig/acp` is a bidirectional [Agent Client Protocol](https://github.com/agentclientprotocol/agent-client-protocol)
(ACP) bridge for Looprig. It lets a Go program:

- **drive a foreign ACP agent** (for example `claude-agent-acp` or `codex-acp`) as a client, over the agent's stdin/stdout; and
- **expose a Harness-shaped session host as an ACP agent**, so an ACP client (such as an editor) can talk to it.

The wire layer is independent of Harness and usable against any ACP peer; only the `agent` package adapts it onto
Harness.

## Status

Released and in use by `foreignloops`. The wire types are generated from the pinned upstream ACP schema release
`schema-v1.20.0` (wire protocol version 1, stable surface only; see `protocol/schema/v1/REVISION`).

Known limits:

- Spawning a child process (`transport/stdio.Spawn`, and therefore `client` and `launch`) is supported on Linux
  (non-Android) and macOS only. Other platforms fail before any child is started.
- `launch.Gemini` is only a gateway environment adapter (`HarnessAdapter`), not a managed ACP connector. No Gemini
  CLI ACP adapter contract has been verified.
- The agent-side Zed checklist in `docs/interop/zed.md` is covered by automated subprocess tests, not by a live
  run under Zed.

## Install

```sh
go get github.com/looprig/acp@latest
```

## Packages

| Package | Purpose |
|---|---|
| `protocol` | JSON-RPC 2.0 framing (NDJSON), the `Conn` dispatcher, generated ACP types and method constants, and the typed `AgentConn`/`ClientConn` surfaces. |
| `transport/stdio` | Process-boundary transport: `Serve` runs a `Conn` over the current process's stdio; `Spawn` starts and supervises a child whose stdio carries the other end. |
| `client` | Drives a foreign ACP agent: `Dial`/`New`, sessions (`NewSession`, `LoadSession`, `ResumeSession`), prompts, cancellation, and optional permission, filesystem and terminal handlers. |
| `launch` | Supervises an ACP child together with an optional model proxy. Ships `ClaudeCode` and `Codex` connectors (with `ProbeCodexVersion` preflight), `DialNative` for adapters using their own auth, and the `Gemini` env adapter. |
| `agent` | The ACP agent facade over a consumer-supplied `SessionHost`/`LiveSession`: initialize, authenticate, session new/load/resume/list/close/delete, prompt, cancel, permission gates, config options and `/compact`. Optional capabilities are advertised only when their `Options` field is set. |

`protocol`, `transport/stdio`, `client` and `launch` never import `harness` or `core`; `agent` is the only
package that does.

## Usage

Drive an ACP agent as a child process (adapted from `examples/composition/example_test.go`):

```go
c, err := client.Dial(ctx, stdio.Command{Path: "/abs/path/to/agent", Env: env}, client.Options{})
if err != nil {
	return err
}
defer c.Close(ctx)

meta, _ := c.InitializeMetadata()
session, err := c.NewSession(ctx, client.NewSessionParams{Cwd: "/workspace"})
if err != nil {
	return err
}
fmt.Println(meta.AgentInfo.Name, session.ID())
```

Expose a session host as an ACP agent (see `agent/examples/host/example_test.go`):

```go
facade, err := agent.New(agent.Options{Host: myHost})
if err != nil {
	return err
}
conn := protocol.NewConn(os.Stdin, os.Stdout, protocol.ConnOptions{})
facade.Register(conn)
return stdio.Serve(ctx, os.Stdin, os.Stdout, conn)
```

More runnable examples: `Example_proxyBacked` and `Example_adapterConfiguration` in
`examples/composition/example_test.go`. Further documentation:

- `docs/connectors/inference-gateway.md`: gateway-backed connectors in `launch`.
- `docs/interop/client-interop.md`: client interop against a real ACP agent.
- `docs/interop/zed.md`: the agent-side Zed checklist.

## Where it sits

Tier 4 in the Looprig graph. Direct Looprig dependencies: `core` and `harness` (used only by `agent`).
Consumed by `foreignloops` and `carbon`.

## Development

Go 1.26.8 baseline.

```sh
GOWORK=off go test ./...
make test      # layering boundary check + go test -race ./...
make build     # boundary check + CGO_ENABLED=0 go build -trimpath ./...
make check     # fmt-check, vet, staticcheck, gosec, govulncheck, test, build
make fuzz      # prints the fuzz targets (FuzzEnvelope, FuzzFrameReader) and how to run them
```

`make boundary` enforces the layering rule above. The live interop probes are opt-in: set
`ACP_INTEROP_AGENT_PATH` for `client/interop_integration_test.go` (`-tags integration`) and `ACP_ADAPTER_PATH`
for `TestInstalledAdapterLive`. To regenerate the protocol types, run `go generate ./protocol`.

See `CONTRIBUTING.md` for contribution rules.

## License

Apache License 2.0. See `LICENSE`.
