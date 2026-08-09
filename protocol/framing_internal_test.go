package protocol

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
)

type admissionGateWriter struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	writes  int
	once    sync.Once
}

func (w *admissionGateWriter) releaseWrite() {
	w.once.Do(func() { close(w.release) })
}

type admissionFrame struct {
	Worker int `json:"worker"`
	Seq    int `json:"seq"`
}

func newAdmissionGateWriter() *admissionGateWriter {
	return &admissionGateWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (w *admissionGateWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.writes++
	first := w.writes == 1
	w.mu.Unlock()
	if first {
		close(w.started)
	}
	<-w.release
	return len(p), nil
}

func (w *admissionGateWriter) writeCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes
}

type marshalGate struct {
	entered chan struct{}
	release chan struct{}
}

func (m *marshalGate) MarshalJSON() ([]byte, error) {
	close(m.entered)
	<-m.release
	return []byte(`{"ok":true}`), nil
}

func TestWriterCancellationDuringMarshalNeverAdmitsOrWrites(t *testing.T) {
	var sink bytes.Buffer
	w := NewWriter(&sink)
	defer w.Close()

	ctx, cancel := context.WithCancel(context.Background())
	gate := &marshalGate{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct {
		result WriteResult
		err    error
	}, 1)
	go func() {
		result, err := w.SendContextResult(ctx, gate)
		done <- struct {
			result WriteResult
			err    error
		}{result: result, err: err}
	}()
	<-gate.entered
	cancel()
	close(gate.release)

	outcome := <-done
	if !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("SendContextResult() error = %v, want context.Canceled", outcome.err)
	}
	if outcome.result.WriteAdmitted {
		t.Fatal("SendContextResult() admitted a frame after cancellation during marshal")
	}
	if sink.Len() != 0 {
		t.Fatalf("writer output length = %d, want zero", sink.Len())
	}
}

func TestWriterCancellationWhileQueueBlockedWinsAdmissionArbitration(t *testing.T) {
	sink := newAdmissionGateWriter()
	w := NewWriter(sink)
	defer func() {
		sink.releaseWrite()
		_ = w.Close()
	}()

	firstDone := make(chan error, 1)
	go func() { firstDone <- w.Send(admissionFrame{Worker: -1, Seq: -1}) }()
	<-sink.started

	// Keep the writer occupied in its first raw Write, then fill every queue
	// slot with admitted jobs. The queue length is read only by this white-box
	// test; no timing assumption is involved.
	for i := 0; i < SendQueueDepth; i++ {
		go func(i int) { _ = w.Send(admissionFrame{Worker: 1, Seq: i}) }(i)
	}
	for len(w.queue) != SendQueueDepth {
		runtime.Gosched()
	}

	ctx, cancel := context.WithCancel(context.Background())
	gate := &marshalGate{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct {
		result WriteResult
		err    error
	}, 1)
	go func() {
		result, err := w.SendContextResult(ctx, gate)
		done <- struct {
			result WriteResult
			err    error
		}{result: result, err: err}
	}()
	<-gate.entered
	cancel()
	close(gate.release)
	sink.releaseWrite()

	outcome := <-done
	if !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("SendContextResult() error = %v, want context.Canceled", outcome.err)
	}
	if outcome.result.WriteAdmitted {
		t.Fatal("SendContextResult() admitted a queued frame after cancellation won")
	}
	if got := sink.writeCount(); got > SendQueueDepth+1 {
		t.Fatalf("underlying writes = %d, want at most %d (target frame must not write)", got, SendQueueDepth+1)
	}
	if err := <-firstDone; err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
}

var _ io.Writer = (*admissionGateWriter)(nil)
