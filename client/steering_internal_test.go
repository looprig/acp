package client

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/looprig/acp/protocol"
)

func TestOrderedCallPromptAndUpdateCarryMonotonicReceiveSequencesAndBarrier(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	sess := newSessionForTest(t, c, fa, "sess-ordered")
	fa.onPrompt = func(ctx context.Context, fa *fakeAgent, req protocol.PromptRequest) (protocol.PromptResponse, error) {
		if err := fa.client.SessionUpdate(ctx, protocol.SessionNotification{
			SessionID: req.SessionID,
			Update:    chunkText("before-completion"),
		}); err != nil {
			return protocol.PromptResponse{}, err
		}
		return protocol.PromptResponse{StopReason: protocol.StopReasonEndTurn}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := sess.Prompt(ctx, nil)
	if err != nil {
		t.Fatalf("Prompt() error = %v", err)
	}
	if result.ReceiveSequence == 0 {
		t.Fatal("PromptResult.ReceiveSequence = 0, want stamped response sequence")
	}

	updates := make(chan clientUpdateObservation, 1)
	go func() {
		update, open := <-sess.Updates()
		updates <- clientUpdateObservation{update: update, open: open}
	}()
	if err := sess.WaitForUpdatesThrough(ctx, result.ReceiveSequence); err != nil {
		t.Fatalf("WaitForUpdatesThrough() error = %v", err)
	}
	select {
	case observation := <-updates:
		if !observation.open {
			t.Fatal("session Updates channel closed before notification delivery")
		}
		update := observation.update
		if update.ReceiveSequence == 0 {
			t.Fatal("Update.ReceiveSequence = 0, want stamped notification sequence")
		}
		if update.ReceiveSequence >= result.ReceiveSequence {
			t.Fatalf("Update.ReceiveSequence = %d, PromptResult.ReceiveSequence = %d; want update first", update.ReceiveSequence, result.ReceiveSequence)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("notification barrier did not allow the session update to be delivered")
	}
}

type clientUpdateObservation struct {
	update Update
	open   bool
}

func TestExtensionSteerUsesFixedMethodAndTypedOutcome(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	sess := newSessionForTest(t, c, fa, "sess-steer")

	var got struct {
		SessionID protocol.SessionID      `json:"sessionId"`
		Prompt    []protocol.ContentBlock `json:"prompt"`
		Meta      json.RawMessage         `json:"_meta"`
	}
	fa.conn.Handle("_session/steering", func(_ context.Context, _ string, params json.RawMessage) (any, error) {
		if err := json.Unmarshal(params, &got); err != nil {
			return nil, protocol.InvalidParams("decode steering", err)
		}
		return map[string]any{
			"outcome": "injected",
			"reason":  "active turn accepted",
		}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := sess.Steer(ctx, SteerParams{
		Prompt: []protocol.ContentBlock{{Text: &protocol.TextContent{Text: "steer me"}}},
		Meta:   json.RawMessage(`{"steering":{"idleBehavior":"promptRequired"}}`),
	})
	if err != nil {
		t.Fatalf("Steer() error = %v", err)
	}
	if result.Outcome != SteerOutcomeInjected {
		t.Fatalf("Steer() outcome = %q, want %q", result.Outcome, SteerOutcomeInjected)
	}
	if !result.WriteAdmitted {
		t.Fatal("Steer() reported false write admission after successful response")
	}
	if result.ReceiveSequence == 0 {
		t.Fatal("SteerResult.ReceiveSequence = 0, want stamped response sequence")
	}
	if got.SessionID != sess.ID() {
		t.Fatalf("steering sessionId = %q, want %q", got.SessionID, sess.ID())
	}
	if len(got.Prompt) != 1 || got.Prompt[0].Text == nil || got.Prompt[0].Text.Text != "steer me" {
		t.Fatalf("steering prompt = %#v, want one text block", got.Prompt)
	}
	if string(got.Meta) != `{"steering":{"idleBehavior":"promptRequired"}}` {
		t.Fatalf("steering _meta = %s, want caller metadata", got.Meta)
	}
}

func TestExtensionSteerReturnsBoundedTypedPeerError(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	sess := newSessionForTest(t, c, fa, "sess-steer-error")
	fa.conn.Handle("_session/steering", func(context.Context, string, json.RawMessage) (any, error) {
		return nil, protocol.InvalidParams("steering rejected", nil).WithData(map[string]string{"secret": "must-not-leak"})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := sess.Steer(ctx, SteerParams{Prompt: nil})
	if err == nil {
		t.Fatal("Steer() error = nil, want peer error")
	}
	if result.WriteAdmitted == false {
		t.Fatal("Steer() reported false write admission after peer response")
	}
	var steeringErr *SteeringError
	if !errors.As(err, &steeringErr) {
		t.Fatalf("Steer() error = %v (%T), want *SteeringError", err, err)
	}
	if steeringErr.Code != protocol.ErrorCodeInvalidParams {
		t.Fatalf("SteeringError.Code = %d, want %d", steeringErr.Code, protocol.ErrorCodeInvalidParams)
	}
	if steeringErr.ReceiveSequence == 0 || !steeringErr.WriteAdmitted {
		t.Fatalf("SteeringError facts = %#v, want admitted response sequence", steeringErr)
	}
	if strings.Contains(steeringErr.Error(), "must-not-leak") {
		t.Fatalf("SteeringError leaked wire data: %q", steeringErr.Error())
	}
}

func TestSteeringErrorBoundsHostileFaultMessageAndData(t *testing.T) {
	const secret = "wire-secret-must-not-leak"
	fault := &protocol.Fault{
		Code:    protocol.ErrorCodeInvalidParams,
		Message: strings.Repeat("x", 4<<20) + secret,
		Data:    json.RawMessage(strings.Repeat("{\"secret\":\""+secret+"\"}", 4<<20/len(secret))),
	}
	err := newSteeringError(fault, protocol.CallResult{
		WriteAdmitted:    true,
		ResponseSequence: 42,
	})
	var steeringErr *SteeringError
	if !errors.As(err, &steeringErr) {
		t.Fatalf("newSteeringError() = %T, want *SteeringError", err)
	}
	if len(steeringErr.Message) > maxSteeringErrorMessageBytes {
		t.Fatalf("SteeringError.Message length = %d, want <= %d", len(steeringErr.Message), maxSteeringErrorMessageBytes)
	}
	if strings.Contains(steeringErr.Error(), secret) {
		t.Fatalf("SteeringError leaked hostile wire data")
	}
	if steeringErr.ResponseSequence != 42 || !steeringErr.WriteAdmitted {
		t.Fatalf("SteeringError facts = %#v, want admission and sequence", steeringErr)
	}
}

func TestSteeringErrorPreservesConnectionClosedClassification(t *testing.T) {
	original := &protocol.ConnClosedError{}
	err := newSteeringError(original, protocol.CallResult{WriteAdmitted: true})

	var got *protocol.ConnClosedError
	if !errors.As(err, &got) {
		t.Fatalf("newSteeringError() = %v, want a *protocol.ConnClosedError cause", err)
	}
	if !errors.Is(err, original) {
		t.Fatal("SteeringError lost errors.Is identity for ConnClosedError")
	}
	var closed *ClosedError
	if !errors.As(err, &closed) {
		t.Fatal("SteeringError lost client ClosedError classification")
	}
}

func TestSteeringErrorPreservesWriterClosedClassification(t *testing.T) {
	original := &protocol.WriterClosedError{}
	err := newSteeringError(original, protocol.CallResult{WriteAdmitted: false})

	var got *protocol.WriterClosedError
	if !errors.As(err, &got) {
		t.Fatalf("newSteeringError() = %v, want a *protocol.WriterClosedError cause", err)
	}
	if !errors.Is(err, original) {
		t.Fatal("SteeringError lost errors.Is identity for WriterClosedError")
	}
}

func TestBoundSteeringMessageInvalidUTF8RespectsByteLimit(t *testing.T) {
	for _, test := range []struct {
		name  string
		input []byte
		limit int
	}{
		{name: "one invalid byte below rune width", input: []byte{0xff}, limit: 1},
		{name: "two invalid bytes at replacement boundary", input: []byte{0xff, 0xfe}, limit: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := boundSteeringMessage(string(test.input), test.limit)
			if len(got) > test.limit {
				t.Fatalf("boundSteeringMessage() length = %d, want <= %d", len(got), test.limit)
			}
			if !utf8.ValidString(got) {
				t.Fatalf("boundSteeringMessage() returned invalid UTF-8: %x", []byte(got))
			}
		})
	}
}

func TestOrderedCallPromptErrorPreservesFactsBeforeExtensionReply(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	sess := newSessionForTest(t, c, fa, "sess-ordered-error")
	fa.onPrompt = func(ctx context.Context, fa *fakeAgent, req protocol.PromptRequest) (protocol.PromptResponse, error) {
		if err := fa.client.SessionUpdate(ctx, protocol.SessionNotification{
			SessionID: req.SessionID,
			Update:    chunkText("before-prompt-error"),
		}); err != nil {
			return protocol.PromptResponse{}, err
		}
		return protocol.PromptResponse{}, protocol.InvalidParams("prompt rejected", nil)
	}
	fa.conn.Handle("_session/steering", func(context.Context, string, json.RawMessage) (any, error) {
		return map[string]any{"outcome": "injected"}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	promptResult, promptErr := sess.Prompt(ctx, nil)
	if promptErr == nil {
		t.Fatal("Prompt() error = nil, want peer error")
	}
	if promptResult == nil {
		t.Fatal("Prompt() result = nil on peer error, want ordered transport facts")
	}
	if !promptResult.WriteAdmitted || promptResult.ReceiveSequence == 0 {
		t.Fatalf("Prompt() facts = %#v, want admitted response sequence", promptResult)
	}
	if err := sess.WaitForUpdatesThrough(ctx, promptResult.ReceiveSequence); err != nil {
		t.Fatalf("WaitForUpdatesThrough() error = %v", err)
	}
	update := <-sess.Updates()
	if update.ReceiveSequence == 0 || update.ReceiveSequence >= promptResult.ReceiveSequence {
		t.Fatalf("Update.ReceiveSequence = %d, PromptResult.ReceiveSequence = %d; want update first", update.ReceiveSequence, promptResult.ReceiveSequence)
	}

	steerResult, steerErr := sess.Steer(ctx, SteerParams{Prompt: nil})
	if steerErr != nil {
		t.Fatalf("Steer() error = %v", steerErr)
	}
	if steerResult.ReceiveSequence <= promptResult.ReceiveSequence {
		t.Fatalf("SteerResult.ReceiveSequence = %d, PromptResult.ReceiveSequence = %d; want extension reply later", steerResult.ReceiveSequence, promptResult.ReceiveSequence)
	}
}
