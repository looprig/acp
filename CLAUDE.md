# CLAUDE.md — Development Guidelines

## Design and dependency direction

Keep packages and types cohesive. Split code when responsibilities have different
owners, invariants, or reasons to change. Prefer composition for independent
capabilities and simple changes to existing types when behavior belongs there.

Every implementation of an interface must honor its complete contract. Keep
interfaces small and define them at the package that consumes them when a stable
boundary, substitution, or testing seam is needed. Depend on public contracts,
not concrete implementations; do not introduce an import from Harness's or
Core's internal packages into this module.

This module bridges the Agent Client Protocol to Harness. Its packages layer
strictly:

- `acp/protocol`, `acp/transport/stdio`, and `acp/client` are the pure wire
  layer: JSON-RPC framing, ACP message types, and the stdio transport. These
  packages must never import `github.com/looprig/harness` or
  `github.com/looprig/core`, directly or transitively. They exist independently
  of Harness and must stay usable against any ACP peer.
- `acp/agent` is the only PRODUCT-FACING package that may import Harness's
  public packages (and Core). It adapts the wire layer onto a Harness
  session; it is the seam where the protocol meets the agent runtime.
  `acp/internal/exampleagent` (Task 6.1's thin, test-only composition — the
  module still ships no product binary) is the one deliberate exception: it
  wires a minimal in-memory SessionHost/LiveSession implementation onto the
  real `acp/agent` facade for interop testing, so it necessarily depends on
  the same Harness/Core public packages a real product would (content, uuid,
  event, gate, journal, sessionstore) — never their internal/ packages,
  exactly like `acp/agent` itself. No other package may import Harness or
  Core.

A dependency-guard test enforces this layering (see Task 1.9 of the ACP bridge
implementation plan); until it lands, treat the rule above as binding anyway.

Prefer the standard library. External packages require explicit user approval
before they are added — do not `go get` anything not already listed below
without stopping to ask first. The only approved external packages are:

- `github.com/looprig/core` — shared content and UUID values, consumed by
  `acp/agent` and `acp/internal/exampleagent`
- `github.com/looprig/harness` — public foreign, loop, command, event, and
  identity contracts, consumed by `acp/agent` and `acp/internal/exampleagent`
- `github.com/securego/gosec/v2` — security static analysis (development tool only)
- `golang.org/x/vuln/cmd/govulncheck` — Go vulnerability scanner (development tool only)
- `honnef.co/go/tools/cmd/staticcheck` — extended static analysis (development tool only)

The development module resolves the untagged Harness and Core seams through the
sibling `../harness` and `../core` checkouts. A released `go.mod` must replace
those local development mappings with tagged versions and must not contain a
local `replace`.

## Validation and failures

Treat CLI arguments, environment variables, process output, protocol messages,
and filesystem content as untrusted. Validate at the boundary before values
enter transport, client, or agent logic. Reject unknown enum values, malformed
records, missing required fields, and unsafe paths.

Fail closed on error or ambiguity. Grant each component and child process only
the permissions, handles, paths, and environment values it needs. Never pass a
full configuration object where a narrow value or interface suffices.

Public failures that callers must classify, recover from, or inspect use typed or
sentinel errors and support `errors.Is` or `errors.As`. Wrap ordinary errors with
useful operation context; never discard errors or expose secrets in errors or
logs.

## Processes, environments, and paths

Invoke programs with `exec.CommandContext` and separate argument values. Never
build a shell command from external input. Every process operation must accept a
`context.Context`, honor cancellation and deadlines, close pipes, reap children,
and avoid goroutine or descriptor leaks.

Construct child environments from an explicit allowlist plus validated required
values. Do not inherit the parent environment wholesale, and do not forward
credentials unless the child contract explicitly requires them.

Clean external paths with `filepath.Clean`, reject absolute paths where a relative
path is required, and verify that resolved paths remain beneath the intended
root. Defend against `..` traversal and symlink escape before opening or writing.

All I/O workflows accept a context and have a finite deadline or caller-provided
bound. Check cancellation around blocking file, pipe, and stdio-transport
operations whose APIs do not take a context. Do not start unbounded network,
pipe, process, or filesystem work.

## Tests and secure builds

Build with `CGO_ENABLED=0 go build -trimpath ./...`. Keep the repository root free
of Go source files; packages live below it. All Go code must be `gofmt`-clean.

Run unit and integration tests with `-race`. Use focused tests for single
scenarios and table-driven tests for shared setup. Cover success, boundary,
invalid-input, cancellation, cleanup, and malformed-protocol-input paths. Code
that parses untrusted wire or transcript data needs a fuzz target; run fuzzing
with an explicit bound such as `-fuzztime=30s`.

**Dependencies are pinned, not vendored.** `go.mod` pins exact versions and
`go.sum` verifies their content hashes, which is what makes a build reproducible.
This module deliberately has no `vendor/`: a vendor tree is ignored under a
`go.work` but silently satisfies a `GOWORK=off` build, so a stale one lets
standalone verification pass against the vendored copy rather than the version
`go.mod` actually pins — defeating the purpose of verifying standalone. Run
`GOWORK=off go test ./...` to check this module against its real pinned
dependencies.

Run `make secure` before each commit. It enforces the root-package boundary,
formatting, `go vet`, staticcheck, gosec, module verification,
and govulncheck. Do not weaken or skip a failed check; diagnose the cause.
