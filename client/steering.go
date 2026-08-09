package client

import (
	"context"
	"encoding/json"

	"github.com/looprig/acp/protocol"
)

// methodSessionSteering is the one fixed ACP extension method exposed by
// Session.Steer. It intentionally remains unexported: callers cannot turn
// this typed API into arbitrary method probing.
const methodSessionSteering = "_session/steering"

// SteerParams is the typed request shape for _session/steering. SessionID is
// overwritten with the receiver's ID by Session.Steer, so a caller cannot
// steer another Session through an existing Session value. Meta is optional
// ACP extension metadata and is passed through as caller-owned JSON bytes;
// this package does not interpret adapter capability or profile policy.
type SteerParams struct {
	SessionID protocol.SessionID      `json:"sessionId"`
	Prompt    []protocol.ContentBlock `json:"prompt"`
	Meta      json.RawMessage         `json:"_meta,omitempty"`
}

// SteerOutcome is the bounded normalized outcome vocabulary understood by the
// foreign driver. An empty or unknown value is preserved as an empty/unknown
// outcome so the driver can fail closed; the client never guesses policy from
// a method error or probes a second method.
type SteerOutcome string

const (
	SteerOutcomeInjected       SteerOutcome = "injected"
	SteerOutcomePromptRequired SteerOutcome = "promptRequired"
	SteerOutcomeStartedNewTurn SteerOutcome = "startedNewTurn"
	SteerOutcomeFailed         SteerOutcome = "failed"
)

// SteerResult is the typed, bounded result of Session.Steer. Outcome and
// Reason are the extension's normalized response facts; raw wire payloads
// are deliberately not exposed. Transport facts remain available even when
// err is non-nil, which lets a caller distinguish a proven pre-admission
// failure from an admitted but ambiguous/erroring call.
type SteerResult struct {
	Outcome SteerOutcome
	Reason  string

	WriteAdmitted    bool
	ReceiveSequence  uint64
	ResponseSequence uint64
}

const maxSteerReasonBytes = 1024

// Steer sends the fixed _session/steering request for this Session. It does
// not perform capability/profile allowlisting or an unknown-method probe;
// those policy decisions belong to the foreign driver.
func (s *Session) Steer(ctx context.Context, p SteerParams) (SteerResult, error) {
	agent, err := s.client.currentAgent()
	if err != nil {
		return SteerResult{}, err
	}

	p.SessionID = s.id
	var wire struct {
		Outcome string `json:"outcome"`
		Reason  string `json:"reason,omitempty"`
	}
	facts, err := agent.CallExtensionWithResult(ctx, methodSessionSteering, p, &wire)
	result := SteerResult{
		Outcome:          SteerOutcome(wire.Outcome),
		Reason:           boundSteerReason(wire.Reason),
		WriteAdmitted:    facts.WriteAdmitted,
		ReceiveSequence:  facts.ResponseSequence,
		ResponseSequence: facts.ResponseSequence,
	}
	if err != nil {
		return result, wrapConnError(err)
	}
	return result, nil
}

func boundSteerReason(reason string) string {
	if len(reason) <= maxSteerReasonBytes {
		return reason
	}
	return reason[:maxSteerReasonBytes]
}
