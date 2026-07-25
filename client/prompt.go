// prompt.go implements Session.Prompt and Session.Cancel: the one
// prompt-in-flight-per-session gate, and cancellation-as-success (a
// cancelled prompt resolves as StopReasonCancelled, never as an error — see
// the design doc's "Cancellation-as-success").
package client

import (
	"context"

	"github.com/looprig/acp/protocol"
)

// PromptResult is the outcome of one session/prompt turn.
type PromptResult struct {
	// StopReason is why the agent stopped processing the turn. A cancelled
	// turn (protocol.StopReasonCancelled) is reported here, in a successful
	// result — never as an error.
	StopReason protocol.StopReason
}

// Prompt sends blocks as one session/prompt turn and blocks until the agent
// responds. Only one Prompt call may be in flight per Session at a time
// (enforced by an internal semaphore): a concurrent caller blocks until the
// prior call completes, rather than racing two prompts onto the same
// session. ctx cancellation while waiting for the semaphore unblocks with
// ctx.Err() without disturbing a prompt already in flight.
func (s *Session) Prompt(ctx context.Context, blocks []protocol.ContentBlock) (*PromptResult, error) {
	select {
	case s.promptSem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-s.promptSem }()

	agent, err := s.client.currentAgent()
	if err != nil {
		return nil, err
	}

	resp, err := agent.Prompt(ctx, protocol.PromptRequest{SessionID: s.id, Prompt: blocks})
	if err != nil {
		return nil, wrapConnError(err)
	}
	return &PromptResult{StopReason: resp.StopReason}, nil
}

// Cancel sends the session/cancel notification for this session. It does
// not itself wait for the in-flight Prompt call to resolve: Prompt's own
// pending call resolves independently once the agent's response arrives
// (StopReasonCancelled, delivered as a successful *PromptResult per the
// design doc's cancellation-as-success rule), and this call simply requests
// that.
func (s *Session) Cancel(ctx context.Context) error {
	agent, err := s.client.currentAgent()
	if err != nil {
		return err
	}
	if err := agent.Cancel(ctx, protocol.CancelNotification{SessionID: s.id}); err != nil {
		return wrapConnError(err)
	}
	return nil
}
