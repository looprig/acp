package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"

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

// maxSteeringErrorMessageBytes bounds peer-controlled diagnostic text kept by
// a SteeringError. Wire Data is intentionally discarded entirely; the driver
// receives only this bounded code/message pair and transport facts.
const maxSteeringErrorMessageBytes = 1024

// SteeringError is the bounded typed error returned by Session.Steer after a
// request reached the protocol layer. It never exposes the peer's raw error
// Data or an unbounded transport diagnostic. Code is the peer JSON-RPC code
// when one was received, or ErrorCodeInternalError for a local/transport
// failure.
type SteeringError struct {
	Code             protocol.ErrorCode
	Message          string
	WriteAdmitted    bool
	ReceiveSequence  uint64
	ResponseSequence uint64
	cause            error
}

func (e *SteeringError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return fmt.Sprintf("acp/client: steering failed (code %d)", e.Code)
	}
	return fmt.Sprintf("acp/client: steering failed (code %d): %s", e.Code, e.Message)
}

// Unwrap preserves cancellation and local transport classification without
// ever retaining a peer *protocol.Fault (whose Data may contain raw wire
// payload). Peer faults are represented only by SteeringError's bounded code
// and message.
func (e *SteeringError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

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
		return result, newSteeringError(err, facts)
	}
	return result, nil
}

func newSteeringError(err error, facts protocol.CallResult) error {
	if err == nil {
		return nil
	}
	steeringErr := &SteeringError{
		Code:             protocol.ErrorCodeInternalError,
		Message:          "steering request failed",
		WriteAdmitted:    facts.WriteAdmitted,
		ReceiveSequence:  facts.ResponseSequence,
		ResponseSequence: facts.ResponseSequence,
	}
	var fault *protocol.Fault
	if errors.As(err, &fault) && fault != nil {
		steeringErr.Code = fault.Code
		steeringErr.Message = boundSteeringMessage(fault.Message, maxSteeringErrorMessageBytes)
		return steeringErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		steeringErr.Code = protocol.ErrorCodeRequestCancelled
		steeringErr.Message = "steering request canceled"
		steeringErr.cause = err
	}
	return steeringErr
}

func boundSteerReason(reason string) string {
	return boundSteeringMessage(reason, maxSteerReasonBytes)
}

func boundSteeringMessage(message string, limit int) string {
	if len(message) <= limit && utf8.ValidString(message) {
		return sanitizeSteeringMessage(message)
	}
	if limit <= 0 {
		return ""
	}
	bounded := make([]byte, 0, minInt(len(message), limit))
	for len(message) > 0 && len(bounded) < limit {
		r, size := utf8.DecodeRuneInString(message)
		if size == 0 || len(bounded)+size > limit {
			break
		}
		if r < 0x20 && r != '\t' {
			r = ' '
		}
		bounded = utf8.AppendRune(bounded, r)
		message = message[size:]
	}
	return string(bounded)
}

func sanitizeSteeringMessage(message string) string {
	bounded := make([]byte, 0, len(message))
	for len(message) > 0 {
		r, size := utf8.DecodeRuneInString(message)
		if size == 0 {
			break
		}
		if r < 0x20 && r != '\t' {
			r = ' '
		}
		bounded = utf8.AppendRune(bounded, r)
		message = message[size:]
	}
	return string(bounded)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
