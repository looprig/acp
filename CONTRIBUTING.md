# Contributing to looprig/acp

Thanks for considering a contribution. `acp` is part of a multi-module Go
ecosystem; this file is the short guide for working in *this* repository.

`acp` is a bidirectional Agent Client Protocol bridge: a pure wire layer
(`protocol`, `transport/stdio`, `client`) plus an `agent` package that is the
only place the wire layer meets Harness.

## Before you write code

1. Read [`CLAUDE.md`](CLAUDE.md) (a.k.a. `AGENTS.md`). It is the
   authoritative source for the design, security, dependency, build, and
   code rules this module follows. PRs that contradict it will be asked to
   change.
2. This repo has no `docs/` directory yet. See
   `harness/docs/plans/2026-07-23-acp-bridge-implementation.md` in the
   `harness` module for the phased implementation plan this module follows.
3. Open an issue for anything non-trivial so we can agree on direction
   before you spend the time.

## Design and security rules (the short version)

- **Depend on public contracts, not concrete implementations.** Never
  introduce an import from Harness's or Core's internal packages into this
  module; consume only their public contracts.
- **Layering is one-directional.** `protocol`, `transport/stdio`, and
  `client` are the pure wire layer and must never import
  `github.com/looprig/harness` or `github.com/looprig/core`. Only `agent`
  may import them.
- **Every interface implementation honors its full contract.** Keep
  interfaces small and defined at the package that consumes them when a
  stable boundary, substitution, or testing seam is needed.
- **All errors are typed.** Sentinel or typed errors for public failures
  callers classify with `errors.Is`/`errors.As`; wrapped ordinary errors for
  contextual failures. Never discard an error or expose secrets in an error
  or log message.
- **Treat all external input as untrusted.** CLI arguments, environment
  variables, process output, protocol messages, and filesystem content must
  be validated at the boundary before they reach transport, client, or agent
  logic. Reject unknown enum values, malformed records, missing required
  fields, and unsafe paths. Fail closed on error or ambiguity.
- **Processes are least-privilege and bounded.** Invoke child programs with
  `exec.CommandContext` and separate argument values — never build a shell
  command from external input. Every process operation accepts a
  `context.Context`, honors cancellation and deadlines, closes pipes, and
  reaps children. Construct child environments from an explicit allowlist
  plus validated required values; never inherit the parent environment
  wholesale.
- **Paths are cleaned and confined.** Clean external paths with
  `filepath.Clean`, reject absolute paths where a relative path is
  required, and verify resolved paths stay beneath the intended root.
  Defend against `..` traversal and symlink escape before opening or
  writing.
- **Prefer the standard library.** External packages require explicit user
  approval in the conversation that adds them. Once approved, the package
  is added to the approved list in `CLAUDE.md`. Never `go get` without that
  approval. See `CLAUDE.md` for the current approved list and the rationale
  behind each entry.

## Build, test, and secure

Run these before pushing. CI runs the same.

```sh
make fmt       # gofmt the whole module in place
make build     # boundary + CGO_ENABLED=0 go build -trimpath ./...
make test      # boundary + go test -race ./...           (always -race)
make secure    # lint + vuln:
               #   lint = boundary + fmt-check + go vet + staticcheck + gosec
               #   vuln = vendor-check + go mod verify + govulncheck
```

Two targets are specific to this repo beyond the usual `fmt`/`test`/`lint`/
`vuln`/`secure` set:

- `make root-check` — fails if any `.go` file (or symlink) lives directly in
  the repository root; packages must live below it. Run automatically as
  part of `make boundary`.
- `make boundary` — runs `root-check` and `vendor-check`, then runs the
  dependency/boundary test suite (`internal/boundary`, matching
  `Dependencies|Boundaries|Public|Scan`) that enforces the module's import
  and API-surface rules. As `protocol`, `transport/stdio`, `client`, and
  `agent` packages land, they join this target's package list. `build`,
  `test`, and `lint` all depend on it.

Build with `CGO_ENABLED=0 go build -trimpath ./...` so binaries never leak
local paths. Fuzz any code that parses untrusted wire or transcript data:
`go test -fuzz=FuzzXxx ./path/to/pkg -fuzztime=30s`.

The module **vendors** its dependency tree. Do not run `go get` casually;
`make vendor` refreshes `vendor/`, scrubs VCS metadata only from the
declared local Harness and Core replacements, and `make vendor-check`
rejects any remaining embedded `.git` metadata.

## Tests

- **Table-driven tests** when several cases share setup and assertion
  shape; focused tests for single scenarios. Cover success, boundary,
  invalid-input, cancellation, cleanup, and malformed-protocol-input paths.
- Run with `-race`: a test that passes without `-race` but not with it is
  not passing.
- Never assume a test framework or script beyond what's already wired. The
  `Makefile` is the source of truth; if you change how tests run, update it
  there (not in this doc).

## Pull requests

- Branch from `main`, name the branch something descriptive.
- One logical change per PR. If a change spans modules, open a PR per
  module; the `replace` directive lets this module build against local
  Harness and Core checkouts during development.
- Write a clear description: what, why, the design alternative you
  rejected, and how you verified. `make secure` output is welcome in the
  PR body.
- Don't force-push after review; add commits and let the reviewer squash.
- Don't commit secrets, tokens, or credentials. Don't add a new external
  dependency without prior approval (see `CLAUDE.md`).
- Don't update `CLAUDE.md`, `Makefile`, `go.mod`, or `scripts/` unless the
  change is the point of the PR.

## Code of conduct

Be excellent to each other. Discussions stay technical and respectful;
personal attacks, harassment, and discrimination are not welcome.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0, as described in [`LICENSE`](LICENSE).
