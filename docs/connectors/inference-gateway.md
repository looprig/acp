# Gateway-backed ACP connectors (`acp/launch`)

This documents `acp/launch`: the package that supervises a foreign ACP
agent's process together with an inference model proxy standing in for a
real upstream provider. It covers the structural contracts the package
exposes, two runnable composition shapes, the Claude Code and Codex
connectors it ships, ownership/teardown rules, the absolute-path and
security posture every connector follows, and the deliberately deferred
Gemini ACP connector.

Ground truth for everything below is the current, committed source in
`acp/launch/*.go` (`contracts.go`, `managed.go`, `errors.go`, `env.go`,
`claudecode.go`, `claude_connector.go`, `codex.go`, `codex_connector.go`,
`version.go`, `gemini.go`) — read those files' own doc comments for the
authoritative behavior; this page is a guide on top of them, not a
replacement.

## Why `acp/launch` never imports `inference`

Per `acp/CLAUDE.md`, `acp/launch` sits in this module's pure wire layer
alongside `acp/client`: it imports `acp/client`, `acp/transport/stdio`, and
`acp/protocol`, but never `github.com/looprig/harness`, `github.com/looprig/core`,
or `github.com/looprig/inference`, directly or transitively.

That is possible because the package's model-proxy contract is structural,
not nominal:

```go
type ProxyBinding struct {
	BaseURL string
	Token   string
}

type ModelProxy interface {
	Start(context.Context) error
	Binding() (baseURL, token string, ready bool)
	Close(context.Context) error
}

type HarnessAdapter interface {
	Configure(stdio.Command, ProxyBinding) (stdio.Command, error)
}

type Config struct {
	OwnedProxy  ModelProxy
	SharedProxy *ProxyBinding
	Harness     HarnessAdapter
	Command     stdio.Command
	Client      client.Options
}

func Dial(context.Context, Config) (*ManagedClient, error)
```

`ModelProxy.Binding` deliberately returns a bare `(string, string, bool)`
tuple rather than a `*ProxyBinding` value. Go requires identical method
result types for structural interface satisfaction, and two independently
declared struct types (say, a `gateway.Binding` and this package's
`ProxyBinding`) are different named types even when their fields match
exactly. A primitive-tuple result is far more likely to already be exactly
what an independently built proxy server type naturally exposes than a
package-specific struct would be — so a `gateway.Server` built entirely
elsewhere (for example, in `inference/gateway`, which does not exist as a
dependency of this module and never will) satisfies `ModelProxy` with zero
code changes on either side, and zero compile-time coupling between the two
packages. `acp/launch` has no idea such a package exists; it only knows
about the three-method shape above.

The same idiom is used internally for testing seams (`connCloser` in
`managed.go`, `sessionConfigurer` in `claude_connector.go`): the real
`*client.Client` and `*client.Session` types satisfy them structurally, with
compile-time proofs (`var _ connCloser = (*client.Client)(nil)`, etc.)
pinning that down.

## Two runnable composition shapes

Both examples below are illustrative: they show the exact `launch.Config`
shape a caller builds, using a plausible `ModelProxy`-satisfying value
(`gatewayServer`, standing in for whatever local gateway type an embedding
application actually constructs — `acp/launch` has no opinion about it and
this module does not depend on one existing). Neither example needs a real
gateway package to compile; only the shapes matter.

### One shared proxy, two harness-facing aliases

A single gateway server can back more than one `Dial` call. Each borrower
gets its own alias exposed to its harness, but neither the process, port,
nor token differs between them. Set `Config.SharedProxy` rather than
`Config.OwnedProxy`, and never call `Start` or `Close` on the proxy from
`acp/launch` — the owning application does that once, before either `Dial`
call and after every borrower has finished with it.

```go
// gatewayServer is constructed once, by the embedding application, entirely
// outside acp/launch. It need only satisfy launch.ModelProxy structurally.
gatewayServer := newSharedGateway(gatewayConfig)

ctx := context.Background()
if err := gatewayServer.Start(ctx); err != nil {
	return err
}
// The application owns this Close; acp/launch never touches it because
// Config.SharedProxy carries no lifecycle methods, and neither ManagedClient
// below will ever call Start or Close on it.
defer gatewayServer.Close(ctx)

baseURL, token, ready := gatewayServer.Binding()
if !ready {
	return errors.New("gateway did not report ready after Start")
}
shared := &launch.ProxyBinding{BaseURL: baseURL, Token: token}

// Borrower 1: Claude Code, using the "primary" alias.
primary, err := launch.Dial(ctx, launch.Config{
	SharedProxy: shared,
	Harness: launch.ClaudeCode(launch.ClaudeModels{
		Default: "primary",
		Small:   "small",
	}),
	Command: stdio.Command{
		Path: "/opt/acp-adapters/claude-agent-acp",
		Dir:  workspaceA,
		Env:  allowlistedEnvironmentA,
	},
	Client: acpClientOptions,
})
if err != nil {
	return err
}
defer primary.Close(ctx)

// Borrower 2: a second Claude Code session, using a different alias
// ("reviewer") against the SAME gateway server and token.
reviewer, err := launch.Dial(ctx, launch.Config{
	SharedProxy: shared,
	Harness: launch.ClaudeCode(launch.ClaudeModels{
		Default: "reviewer",
		Small:   "small",
	}),
	Command: stdio.Command{
		Path: "/opt/acp-adapters/claude-agent-acp",
		Dir:  workspaceB,
		Env:  allowlistedEnvironmentB,
	},
	Client: acpClientOptions,
})
if err != nil {
	return err
}
defer reviewer.Close(ctx)
```

Closing `primary` or `reviewer` never starts or stops `gatewayServer`:
`ManagedClient.Close` closes only the ACP connection (and an owned proxy, if
any — see below), and a `SharedProxy` borrower has no owned proxy at all.
The `defer gatewayServer.Close(ctx)` above is the only place the shared
proxy is ever closed, and it runs only once every borrower using it is
already done.

### Two independent, owned gateway servers

Each `Dial` call may instead own a distinct proxy instance via
`Config.OwnedProxy`. `Dial` starts it before spawning the ACP child, and the
resulting `ManagedClient.Close` tears down both the ACP connection and that
specific proxy. Nothing here is shared or global: closing one
`ManagedClient` has zero effect on the other, because each owns a completely
independent proxy instance, listener, and token.

```go
serverA := newOwnedGateway(gatewayConfigA)
serverB := newOwnedGateway(gatewayConfigB)

managedA, err := launch.Dial(ctx, launch.Config{
	OwnedProxy: serverA,
	Harness:    launch.Codex("primary"),
	Command: stdio.Command{
		Path: "/opt/acp-adapters/codex-acp",
		Dir:  workspaceA,
		Env:  allowlistedEnvironmentA,
	},
	Client: acpClientOptions,
})
if err != nil {
	return err
}

managedB, err := launch.Dial(ctx, launch.Config{
	OwnedProxy: serverB,
	Harness:    launch.Codex("reviewer"),
	Command: stdio.Command{
		Path: "/opt/acp-adapters/codex-acp",
		Dir:  workspaceB,
		Env:  allowlistedEnvironmentB,
	},
	Client: acpClientOptions,
})
if err != nil {
	managedA.Close(ctx)
	return err
}

// Closing managedA closes serverA (and the codex-acp connection it owns).
// serverB, and managedB's own ACP connection, are entirely unaffected.
if err := managedA.Close(ctx); err != nil {
	log.Printf("closing managedA: %v", err)
}

// managedB and serverB continue to run independently.
if err := managedB.Close(ctx); err != nil {
	log.Printf("closing managedB: %v", err)
}
```

`Dial` validates that exactly one of `OwnedProxy`/`SharedProxy` is set
before starting anything (`*ConfigError` otherwise), and if any step after a
successful proxy start fails — configuring the command, spawning the ACP
child, or the ACP `initialize` handshake — that owned proxy is closed again
before `Dial` returns, so a failed `Dial` never leaks a running owned proxy.

## Claude Code connector

Construct with:

```go
connector := launch.ClaudeCode(launch.ClaudeModels{
	Default: "primary",
	Small:   "small",
})
```

`ClaudeModels.Default`/`Small` are harness-facing aliases resolved against
the connected `claude-agent-acp` adapter's advertised `"model"` select
config option — `SelectDefaultModel`/`SelectSmallModel` apply them via
`session/set_config_option`, once a `*client.Session` exists. An alias that
matches none of the adapter's advertised values fails with
`*launch.ModelAliasError`; there is no silent no-op.

`Configure` (the `HarnessAdapter` implementation, in `claudecode.go`)
requires `cmd.Path` to be a clean, absolute path to the `claude-agent-acp`
executable itself — this connector never performs a PATH lookup, never
invokes `npx`, and never installs anything. It sets exactly these
environment variables on top of the deep-copied `Command`:

- `ANTHROPIC_BASE_URL` — the proxy binding's base URL.
- `ANTHROPIC_AUTH_TOKEN` — the proxy binding's bearer token (the local
  gateway's own token, never a real Anthropic credential).
- `CLAUDE_CODE_EXECUTABLE` — only if `ClaudeConnector.CLIPath` is set, and
  only ever to a clean absolute path pinning the underlying `claude` CLI
  `claude-agent-acp` drives.

`CLAUDECODE` — `claude-agent-acp`'s own nested-session detection variable —
must never be present in the child's environment: the adapter misbehaves if
it is, on the assumption it is itself running nested inside another Claude
Code session. This connector never sets it, and if a caller-supplied
`stdio.Command.Env` already contains it, `Configure` rejects the whole
configuration with `*launch.ConflictingEnvError` rather than silently
stripping or overwriting it.

Some `claude-agent-acp` versions key a returned config option's identifier
with a legacy, request-shaped `configId` field inside `_meta` instead of the
spec's own top-level response field `id`. `configOptionID` in
`claude_connector.go` tolerates this by falling back to `_meta.configId`
only when the standard `id` field is empty — never the reverse, and never
by guessing at a different top-level field the generated decoder wouldn't
recognize anyway.

Permission modes (`ApplyPermissionMode`) are applied via `session/set_mode`,
but only if the requested mode actually appears in the session's currently
advertised `AvailableModes` — an unadvertised mode is a deliberate no-op,
not an error, since permission modes are optional by nature (unlike an
unmatched model alias).

This connector's session lifecycle uses `session/new` only. `acp/client`
itself keeps its own general-purpose `session/load` and `session/resume`
support, but nothing in `ClaudeConnector` calls, wraps, or depends on either
— a Claude Code session created through this connector is always a fresh
`session/new`, never a load or resume of a prior one.

## Codex connector

Construct with:

```go
connector := launch.Codex("primary")
connector.Posture = launch.CodexPosture{
	ApprovalPolicy:       "on-request", // default if left empty
	SandboxMode:          "workspace-write", // default if left empty
	SandboxNetworkAccess: false, // default; least-privilege
}
```

`Codex(model)` fixes the harness-facing model alias for the connector's
entire lifetime — there is no session-level model-switching RPC this
connector relies on (see below). `CodexPosture` is the caller-chosen
sandbox/approval posture; an empty `CodexPosture` resolves to sane,
least-privilege defaults (`on-request` / `workspace-write` /
`SandboxNetworkAccess: false`) rather than an invalid empty `-c` value.

`Configure` (in `codex.go`) requires `cmd.Path` to be a clean, absolute path
to the `codex-acp` executable, and `Model` to be non-empty (`*ConfigError`
otherwise). It replaces `cmd.Args` entirely with a fixed, ordered sequence
of nine `-c key=value` pairs — never merged with any caller-supplied
`Args`, since Codex's config surface is this connector's exclusive concern:

```
-c model=<alias>
-c model_provider=looprig
-c model_providers.looprig.base_url=<binding.BaseURL>/v1
-c model_providers.looprig.env_key=LOOPRIG_PROXY_TOKEN
-c model_providers.looprig.wire_api="responses"
-c model_providers.looprig.requires_openai_auth=false
-c approval_policy=<posture.ApprovalPolicy>
-c sandbox_mode=<posture.SandboxMode>
-c sandbox_workspace_write.network_access=<posture.SandboxNetworkAccess>
```

(`wire_api`'s value carries its own embedded double quotes because
`codex-acp` parses each `-c` value as a TOML expression, and a bare
`responses` would not parse as a TOML string; `requires_openai_auth` is the
opposite case — a bare, unquoted `false` so it parses as a TOML boolean
rather than the string `"false"`.)

The proxy binding's bearer token travels only in `LOOPRIG_PROXY_TOKEN`,
never as an argv value or in a generated config file. `CODEX_HOME` must
never be present in the child's environment — its presence would point
`codex-acp` at a real, persistent configuration directory this connector
must never generate, touch, or overwrite. Exactly like `CLAUDECODE` above,
a caller-supplied value already present for `CODEX_HOME` is rejected with
`*launch.ConflictingEnvError`, never silently stripped.

### Version probe is a caller-run preflight, not part of `Configure`

`Configure` itself never runs, spawns, or otherwise reaches for the
`codex-acp` binary to check its version — it only builds argv/env. Version
verification is `ProbeCodexVersion` (`version.go`), a distinct, explicit
step a caller runs itself, before ever constructing the `Config.Command`
whose `Path` `Configure` will validate:

```go
result, err := launch.ProbeCodexVersion(ctx, "/opt/acp-adapters/codex-acp", 0, nil)
if err != nil {
	// result.Class explains why: below-minimum, legacy-no-version,
	// unparseable, nonzero-exit, or timeout. Every non-modern class fails
	// closed identically from the caller's perspective — reject the
	// adapter — so branching on err == nil is sufficient; result.Class is
	// there for diagnostics only.
	return fmt.Errorf("codex-acp failed the version probe: %w", err)
}
// Only past this point should a Config referencing this same Path be built
// and passed to Dial.
```

`ProbeCodexVersion` is bounded (a caller-suppliable timeout, or
`DefaultCodexVersionProbeTimeout` — 5s — when none is given) and requires at
least `MinCodexVersion` (1.1.7); the legacy `@zed-industries/codex-acp`
0.16.x binary predates `--version` support entirely and is rejected. This
sequencing is deliberate and must stay explicit for integrators: nothing in
`Dial` or `CodexConnector.Configure` calls `ProbeCodexVersion` automatically
on a caller's behalf. An embedding application that skips this preflight
step gets no automatic protection against launching an outdated or
unrecognized `codex-acp` binary — probing is entirely the caller's own
responsibility, run once, ahead of `Dial`.

### Capability gating, not probing

`codex-acp` answers unknown/unimplemented extension methods with a bare
`{}` JSON-RPC success rather than the standard `-32601` "method not found"
error. That means probing for an optional extension by calling it and
inspecting whether it errors is unsafe here — a probe reads as success and
can silently drop real work. Any capability this connector (or a caller
layered on top of it) wants to use conditionally must be gated on the
adapter's own `initialize` response `_meta` explicitly advertising it, never
discovered by speculative calls.

### Model changes require a new connector and session

Current `codex-acp` adapters do not reliably support post-session model
switching. There is no `session/set_config_option`/`session/set_model` path
`CodexConnector` exposes for changing models on an existing session —
deliberately: `CodexConnector` defines no method that accepts a
`*client.Session` at all (unlike `ClaudeConnector`). To use a different
model, construct a new connector with `WithModel` and `Dial` an entirely new
ACP session/process:

```go
next := connector.WithModel("reviewer") // connector itself is left unchanged
managed, err := launch.Dial(ctx, launch.Config{
	OwnedProxy: newGatewayForThisSession(),
	Harness:    next,
	Command:    freshCommand,
	Client:     acpClientOptions,
})
```

## Ownership choices and teardown guarantees

Use `Config.OwnedProxy` when the `Dial` call itself should own the proxy's
entire lifecycle — one proxy instance per ACP session, started right before
the child spawns and closed exactly when that session ends (either
explicitly, via `ManagedClient.Close`, or implicitly, on unexpected child
death). Use `Config.SharedProxy` when an application-level proxy already
exists and multiple ACP sessions should reuse the same binding/token
without any of them managing that proxy's `Start`/`Close` — the application
that created it is solely responsible for closing it, once, after every
borrower is finished.

`ManagedClient.Close` always closes in reverse order relative to `Dial`'s
own startup sequence: the ACP connection first, then an owned proxy second
— never the other way around. If both closes fail, both errors are
preserved via `errors.Join` rather than either one being silently
discarded. Both steps are individually idempotent (each guarded by its own
`sync.Once`), so `Close` is safe to call more than once. A `SharedProxy`
borrower's `Close` never touches the shared binding at all — there is no
owned proxy for it to close.

If the ACP child dies unexpectedly — a crash, a transport failure, anything
that reaches `Client.Done()` on its own rather than through an explicit
`Close` — `ManagedClient` closes its owned proxy automatically, via a
background watcher goroutine started only when `OwnedProxy` was set. This
never happens for a `SharedProxy` borrower, since it has no owned proxy to
close and no watcher goroutine at all. Note this watches connection/child
death only, never request-level failures; a single failed inference request
through the proxy never triggers any of this.

## Absolute adapter paths and the installation boundary

Every connector's `Configure` validates that the executable path it is
given (`stdio.Command.Path`, and, for Claude Code, the optional
`CLIPath`/`CLAUDE_CODE_EXECUTABLE` value) is a clean, absolute path —
rejecting anything else with `*launch.PathError`. None of them perform a
`PATH` lookup, invoke `npx`, or install anything on the caller's behalf.
Locating, downloading, and installing `claude-agent-acp`, `codex-acp`, or
any other adapter binary is entirely the embedding application's own
responsibility; `acp/launch` only ever consumes an already-resolved
absolute path handed to it.

## Security posture

- **No ambient environment inheritance.** `stdio.Command.Env` is always
  treated as the caller's complete, already-allowlisted child environment.
  Every connector's `Configure` builds a fresh copy (`buildChildCommand` in
  `env.go`) and adds or replaces only its own documented variables; it never
  reads or forwards the ambient process environment.
- **No upstream provider credential forwarding.** The only credential any
  connector ever places in the child's environment is the local gateway's
  own bearer token from the `ProxyBinding` it was configured with — under
  whichever variable name that harness expects as its credential
  (`ANTHROPIC_AUTH_TOKEN` for Claude Code, `LOOPRIG_PROXY_TOKEN` for Codex,
  `GEMINI_API_KEY` for the Gemini adapter). No real upstream provider
  credential ever passes through this package.
- **Deep-copied, never mutated commands.** `buildChildCommand` always
  returns a fresh `stdio.Command` with independently copied `Env` and
  `Args` backing arrays; the caller's original `cmd` (and its slices) are
  never written to, so holding onto both the input and the returned value
  can never alias the same memory.
- **Conflicting security-sensitive values are rejected, not overwritten.**
  Before applying any override, `buildChildCommand` checks a `forbidden`
  list of variable names against the caller-supplied `Env` and fails with
  `*launch.ConflictingEnvError` if any of them is already present —
  regardless of its value. The concrete instances of this rule today are
  `CLAUDECODE` (Claude Code's Configure) and `CODEX_HOME` (Codex's
  Configure): both must be absent, and a caller-supplied value for either is
  a configuration bug to surface loudly, never a value to silently strip or
  let win.

## The deferred Gemini ACP connector

`launch.Gemini(model)` constructs a `GeminiAdapter` — a bare
`HarnessAdapter`, environment-variable construction only. `Configure` sets
exactly:

- `GOOGLE_GEMINI_BASE_URL` — the proxy binding's base URL.
- `GEMINI_API_KEY` — the proxy binding's bearer token.
- `GEMINI_MODEL` — the harness-facing model alias.

Nothing Vertex-AI- or Code-Assist-related is ever set or disabled; this is
Gemini API-key mode only.

There is deliberately no `GeminiConnector` type, and no ACP spawn contract
for a Gemini CLI ACP subprocess, in this package. None of the ACP hosts
surveyed while designing this package drive Gemini CLI over ACP, so there
is no proven adapter contract to pin yet. A Gemini ACP connector — one that
dials and manages a Gemini CLI ACP subprocess the way `ClaudeConnector` and
`CodexConnector` do for their harnesses — ships only once a real adapter
path has been verified against a live Gemini CLI release. Integrators
should not expect a Gemini ACP connector to exist yet; `GeminiAdapter`
alone is not one, and `gemini_test.go` carries a regression guard against
this package growing one prematurely.
