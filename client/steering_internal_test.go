package client

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
)

func TestPromptAndUpdateCarryMonotonicReceiveSequencesAndBarrier(t *testing.T) {
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

func TestSessionSteerUsesFixedMethodAndTypedOutcome(t *testing.T) {
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

func TestSessionSteerReturnsTypedPeerError(t *testing.T) {
	c, fa := dialTestClient(t, Options{})
	sess := newSessionForTest(t, c, fa, "sess-steer-error")
	fa.conn.Handle("_session/steering", func(context.Context, string, json.RawMessage) (any, error) {
		return nil, protocol.InvalidParams("steering rejected", nil)
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
	var fault *protocol.Fault
	if !errors.As(err, &fault) {
		t.Fatalf("Steer() error = %v (%T), want *protocol.Fault", err, err)
	}
}
