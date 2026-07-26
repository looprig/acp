# Real-agent client interop (Task 6.2)

This records Task 6.2 of `harness/docs/plans/2026-07-23-acp-bridge-implementation.md`:
"Client interop against one maintained ACP agent."

The golden probe lives at `acp/client/interop_integration_test.go`
(`TestClientInteropAgainstRealACPAgent`, `//go:build integration`). It is
env-gated: it drives a real, independently-maintained ACP agent binary
through `acp/client` only when a human points it at one via
`ACP_INTEROP_AGENT_PATH`; otherwise it skips cleanly (verified in this
session — see below).

## Which real agents this targets

This module's own implementation plan names three examples to pick from,
whichever is installed on the machine running the test — a Gemini CLI ACP
endpoint, a Claude Code ACP endpoint, or `cursor-agent acp`. These are drawn
from the plan text itself, not from anything embedded in the pinned
`agentclientprotocol/agent-client-protocol` schema/meta artifacts (checked:
neither `protocol/schema/v1/schema.json` nor `meta.json` names any specific
implementation — they define only the wire protocol, not an implementation
registry). The test is written generically against any ACP-conformant agent
in "agent" mode over stdio; it is not hardcoded to one.

**Honesty note:** this session had no such binary installed or reachable —
no `gemini`, no `cursor-agent`, and no ACP-serving flag on any available
`claude` CLI were found. The test has therefore never actually been run
against a real external agent; only its clean-skip behavior has been
verified here (see "Verification in this session" below). This is a real,
open gap pending a human with one of these agents (or any other
ACP-conformant one) installed.

## How to run it for real

```sh
cd acp

# Example: a hypothetical Gemini CLI ACP mode (flag name illustrative —
# consult that agent's own current documentation for the real one):
ACP_INTEROP_AGENT_PATH=gemini ACP_INTEROP_AGENT_ARGS="--experimental-acp" \
  go test -tags integration -race -v ./client/... -run TestClientInteropAgainstRealACPAgent

# Example: cursor-agent's ACP subcommand:
ACP_INTEROP_AGENT_PATH=cursor-agent ACP_INTEROP_AGENT_ARGS=acp \
  go test -tags integration -race -v ./client/... -run TestClientInteropAgainstRealACPAgent
```

`ACP_INTEROP_AGENT_PATH` may be a bare command name (resolved via `PATH`,
like a shell would) or an absolute/relative path. `ACP_INTEROP_AGENT_ARGS` is
an optional, whitespace-separated argument list appended after it. The test
forwards the current process's full environment to the child (a deliberate,
narrow exception to this module's usual "explicit allowlist only" rule for
child processes — see the test file's `interopEnv` doc for why: a
real, human-configured agent may need its own `PATH`, `HOME`, or credentials
that this test cannot enumerate in advance).

## What it asserts — protocol invariants only

Per the task's own spec, assertions are limited to wire-level protocol
invariants, never content:

- `Dial` (which performs `initialize` internally) succeeds.
- `session/new` returns a non-empty session id.
- `session/prompt`, given a deliberately inert prompt ("acknowledge only, do
  not run tools or make changes"), eventually returns one of the pinned
  schema's defined `StopReason` values — never an assertion about what the
  agent actually replied.
- `session/cancel` does not itself return an error.

## Verification in this session

Confirmed the test compiles and skips cleanly with no agent configured:

```
=== RUN   TestClientInteropAgainstRealACPAgent
    interop_integration_test.go:179: skipping: ACP_INTEROP_AGENT_PATH is not
    set. ...
--- SKIP: TestClientInteropAgainstRealACPAgent (0.00s)
PASS
```

Also confirmed that a deliberately invalid `ACP_INTEROP_AGENT_PATH` fails
loudly rather than silently skipping (a misconfiguration is not the same as
"no agent configured").
