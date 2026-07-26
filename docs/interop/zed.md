# Zed interop checklist (Task 6.1)

This records the status of Task 6.1 of
`harness/docs/plans/2026-07-23-acp-bridge-implementation.md`: "Zed interop
(agent side)". It covers the same checklist the task names — initialize,
session/new, session/prompt, session/cancel, permission, session/list,
session/load — against `acp/internal/exampleagent`, a thin, test-only
composition of the real `acp/agent` facade over a minimal in-memory
`SessionHost`/`LiveSession` (see that package's own doc comment for what it
is and is not).

## Honesty statement

**No live run under the real Zed editor has been performed for this
checklist.** The environment this work was done in has no GUI and no
scriptable Zed CLI/automation harness reachable from a shell — only the
`Zed.app` macOS application bundle exists on disk, with no headless or
CLI-driven way to point it at an external agent and drive it from an
automated test. Every row below that is marked "automated" was verified by a
real, passing Go integration test spawning `exampleagent` as a genuine OS
subprocess and driving it over the ACP wire with a hand-built minimal client
— not by Zed, and not simulated. Every row is additionally marked "pending
manual Zed verification": that is a real gap, not a formality, and it remains
open until a human with Zed installed completes it (see "How to run this
manually" below).

## Checklist

| # | Item | Automated coverage (this session) | Live Zed verification |
|---|------|-------------------------------------|------------------------|
| 1 | **initialize** | `TestExampleAgentInitializeAdvertisesLoadAndListCapabilities` (`acp/internal/exampleagent/exampleagent_integration_test.go`): spawns the real binary, sends `initialize`, asserts `protocolVersion` matches the pinned schema and `agentCapabilities.loadSession`/`sessionCapabilities.list` are advertised (since the Host supplies an `EventReplayer` and a `SessionCatalog`). | **Pending.** Not run under Zed in this session. |
| 2 | **session/new** | `TestExampleAgentNewSessionAndDefaultPromptCompletesWithToolCall`: calls `session/new`, asserts a non-empty, well-formed session id is returned. | **Pending.** Not run under Zed in this session. |
| 3 | **session/prompt** | Same test: submits a prompt, asserts the turn streams `agent_message_chunk`, `tool_call`, and a terminal `tool_call_update` (status `completed`), and the response's `stopReason` is `end_turn`. | **Pending.** Not run under Zed in this session. |
| 4 | **session/cancel** | `TestExampleAgentSessionCancelStopsAnInFlightPrompt`: starts a prompt that deliberately pauses mid-stream (the `trigger-cancel` marker — see `exampleagent`'s package doc), waits for real streamed output to confirm the turn is genuinely in flight, sends `session/cancel`, and asserts the prompt resolves with `stopReason: cancelled` and no error (cancellation-as-success). | **Pending.** Not run under Zed in this session. |
| 5 | **permission** | `TestExampleAgentPermissionRoundTrip` (table test, `approve` and `deny` cases): submits a prompt that opens a real `gate.KindPermission` gate (the `trigger-permission` marker), asserts the agent issues a real `session/request_permission` call with exactly the three expected options, answers it, and asserts the tool call runs only on approval. | **Pending.** Not run under Zed in this session. |
| 6 | **session/list** | `TestExampleAgentSessionListReportsSessionsWithTitleAndCwd`: creates two sessions (one prompted, one not), calls `session/list`, and asserts both are reported with the correct `cwd`, and that only the prompted one has a non-empty `title`. | **Pending.** Not run under Zed in this session. |
| 7 | **session/load** | `TestExampleAgentSessionLoadReplaysDurableHistoryAfterClose`: completes a prompt, closes the session, calls `session/load`, and asserts the replayed `session/update` notifications (`user_message_chunk`, `agent_message_chunk`, `tool_call`) carry `_meta.isReplay: true` and reproduce the original user text — then submits a further prompt on the reloaded session to prove the live controller (not just replay) was genuinely restored. | **Pending.** Not run under Zed in this session. |

All seven automated rows are real, currently-passing tests, run with
`-race`, spawning a genuinely built `exampleagent` binary as a subprocess —
not simulated and not asserted-but-unrun. Confirm with:

```sh
cd acp
go test -tags integration -race -v ./internal/exampleagent/...
```

## What this substitutes for, and what it does not

These tests exercise the identical protocol surface Zed's own external-agent
integration would exercise — the same `acp/agent` facade code, the same
`acp/protocol` wire types, the same `acp/transport/stdio` transport — so they
are real evidence the composition is wire-correct. They cannot, however,
prove that Zed's specific client implementation (its own request timing,
its own UI-driven permission flow, its own retry/reconnect behavior, any
Zed-specific extension fields) actually interoperates with this agent. Only
a live run under Zed can prove that.

## How to run this manually (for a human with Zed installed)

1. Build the binary:

   ```sh
   cd acp
   go build -o /tmp/exampleagent ./internal/exampleagent
   ```

2. In Zed's settings, add `/tmp/exampleagent` as an external agent (Zed's
   "Agent Servers" / external-agent configuration — consult Zed's current
   documentation for the exact settings key, as this is a Zed-side UI/config
   surface this module does not own or control). No arguments or special
   environment variables are required; `exampleagent` speaks ACP purely over
   stdio.
3. Start a conversation with it through Zed and walk the same seven-item
   checklist above by hand:
   - **initialize**: confirm Zed connects without a version-mismatch error.
   - **session/new**: confirm a new session opens.
   - **session/prompt**: send any message; confirm streamed text and a
     visible tool call (`read_file`) appear, ending normally.
   - **session/cancel**: send a message containing the literal substring
     `trigger-cancel` (e.g. "please trigger-cancel this one"), then cancel
     it from Zed's UI mid-stream; confirm it stops cleanly rather than
     erroring.
   - **permission**: send a message containing the literal substring
     `trigger-permission` (e.g. "trigger-permission please"); confirm Zed
     presents a real permission prompt with three choices (Approve / Approve
     always for this workspace / Deny) and that choosing each produces the
     expected outcome (tool runs on approval, does not run on denial).
   - **session/list**: confirm Zed's session picker (if it has one) shows
     previously created sessions with a title once at least one prompt has
     been sent.
   - **session/load**: close a session with some history, then reopen/load
     it from Zed's session picker; confirm the prior exchange is redisplayed.
4. Record the outcome (pass/fail per item, and any Zed-specific issue
   observed) by updating this file's table — replace the "Pending" cells
   above with the actual result and date once this has been done.

No step of this has been performed in this session. This file records that
honestly rather than asserting a live pass.
