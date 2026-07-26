// Package main builds exampleagent: a thin, test-only composition of the real
// acp/agent facade over a minimal, purely in-memory SessionHost/LiveSession
// implementation. It exists for Task 6.1 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md ("Zed interop
// (agent side)"): a genuinely runnable ACP agent binary — built on the real
// acp/agent, acp/protocol, and acp/transport/stdio code paths, exactly the
// way acp/internal/mockpeer (Task 1.8) is a real runnable binary — that a
// human with the Zed editor installed can point at as an external agent.
//
// exampleagent is NOT a product: it never imports any Looprig product code,
// and its "session" is entirely simulated in memory (no real LLM, no real
// tools, no durable storage). What IS real is everything downstream of that
// simulation: the ACP wire protocol, the facade's session lifecycle and
// prompt-correlation state machine, its live-event translation, its
// permission-gate round trip, and its durable-history replay path. Driving
// this binary over stdio exercises the identical code every real product
// composition (a future CodeRig-style consumer) would run.
//
// # Scripting a turn
//
// Because there is no real model behind this agent, session/prompt's
// behavior is selected by a literal marker substring in the submitted prompt
// text (see session.go's classifyPrompt), mirroring how acp/internal/mockpeer
// scripts its behavior via environment variables — except here the natural
// ACP entry point for a human (or a test) to control behavior is the prompt
// content itself, not the process environment:
//
//   - a prompt containing "trigger-permission" runs a turn that opens a
//     gate.KindPermission gate and blocks on session/request_permission
//     before completing (approved or denied, depending on the client's
//     choice);
//   - a prompt containing "trigger-cancel" runs a turn that pauses
//     mid-stream, giving a caller time to send session/cancel and observe
//     TurnInterrupted -> stopReason: cancelled;
//   - any other prompt runs a short default turn: a streamed text reply plus
//     one already-approved tool call, ending in stopReason: end_turn.
//
// Every completed turn's TurnStarted and StepDone are also appended to an
// in-memory durable log, so session/load (after a session/close) replays real
// reconstructed history, and session/list reports every session this process
// has ever created, live or not.
package main
