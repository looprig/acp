package protocol_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
)

// --- FrameReader ---

func TestFrameReaderSplitsOnNewline(t *testing.T) {
	r := protocol.NewFrameReader(bytes.NewReader([]byte("line one\nline two\nline three\n")))

	var got []string
	for {
		frame, err := r.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame() unexpected error = %v", err)
		}
		got = append(got, string(frame))
	}

	want := []string{"line one", "line two", "line three"}
	if len(got) != len(want) {
		t.Fatalf("got %d frames, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("frame[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestFrameReaderTrailingUnterminatedLineIsTruncated(t *testing.T) {
	// The stream ends without a final newline after a non-empty line: the
	// last "line" was never terminated, so it must be reported as a
	// truncated frame, not silently returned as a valid one.
	r := protocol.NewFrameReader(bytes.NewReader([]byte("complete\nincomplete-tail")))

	frame, err := r.ReadFrame()
	if err != nil {
		t.Fatalf("first ReadFrame() error = %v", err)
	}
	if string(frame) != "complete" {
		t.Fatalf("first frame = %q, want %q", frame, "complete")
	}

	_, err = r.ReadFrame()
	var trunc *protocol.TruncatedFrameError
	if !errors.As(err, &trunc) {
		t.Fatalf("second ReadFrame() error = %v (%T), want *TruncatedFrameError", err, err)
	}
	if trunc.Read != len("incomplete-tail") {
		t.Errorf("TruncatedFrameError.Read = %d, want %d", trunc.Read, len("incomplete-tail"))
	}
}

func TestFrameReaderCleanEOFAtFrameBoundaryIsNotTruncated(t *testing.T) {
	// Ending exactly on a newline is a clean end of stream: the next read
	// must be plain io.EOF, never TruncatedFrameError.
	r := protocol.NewFrameReader(bytes.NewReader([]byte("only\n")))

	frame, err := r.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}
	if string(frame) != "only" {
		t.Fatalf("frame = %q, want %q", frame, "only")
	}

	_, err = r.ReadFrame()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadFrame() at clean EOF = %v, want io.EOF", err)
	}
	var trunc *protocol.TruncatedFrameError
	if errors.As(err, &trunc) {
		t.Fatalf("ReadFrame() at clean EOF returned TruncatedFrameError: %v", trunc)
	}
}

func TestFrameReaderEmptyStreamIsCleanEOF(t *testing.T) {
	r := protocol.NewFrameReader(bytes.NewReader(nil))
	_, err := r.ReadFrame()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadFrame() on empty stream = %v, want io.EOF", err)
	}
}

func TestFrameReaderEnforcesMaxMessageBytes(t *testing.T) {
	oversized := bytes.Repeat([]byte("a"), protocol.MaxMessageBytes+1)
	oversized = append(oversized, '\n')
	r := protocol.NewFrameReader(bytes.NewReader(oversized))

	_, err := r.ReadFrame()
	var tooLarge *protocol.FrameTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("ReadFrame() error = %v (%T), want *FrameTooLargeError", err, err)
	}
	if tooLarge.Limit != protocol.MaxMessageBytes {
		t.Errorf("FrameTooLargeError.Limit = %d, want %d", tooLarge.Limit, protocol.MaxMessageBytes)
	}
}

func TestFrameReaderAcceptsExactlyMaxMessageBytes(t *testing.T) {
	exact := bytes.Repeat([]byte("a"), protocol.MaxMessageBytes)
	exact = append(exact, '\n')
	r := protocol.NewFrameReader(bytes.NewReader(exact))

	frame, err := r.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v, want nil", err)
	}
	if len(frame) != protocol.MaxMessageBytes {
		t.Errorf("len(frame) = %d, want %d", len(frame), protocol.MaxMessageBytes)
	}
}

func TestFrameReaderTolerantOfCRLF(t *testing.T) {
	r := protocol.NewFrameReader(bytes.NewReader([]byte("one\r\ntwo\r\n")))

	frame, err := r.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}
	if string(frame) != "one" {
		t.Errorf("frame = %q, want %q", frame, "one")
	}

	frame, err = r.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}
	if string(frame) != "two" {
		t.Errorf("frame = %q, want %q", frame, "two")
	}
}

func TestFrameReaderRejectsEmbeddedNUL(t *testing.T) {
	r := protocol.NewFrameReader(bytes.NewReader([]byte("ab\x00cd\n")))

	_, err := r.ReadFrame()
	var invalid *protocol.InvalidFrameError
	if !errors.As(err, &invalid) {
		t.Fatalf("ReadFrame() error = %v (%T), want *InvalidFrameError", err, err)
	}
}

func TestFrameReaderMultipleFramesAfterCRLFAndNUL(t *testing.T) {
	// Confirms the reader keeps making forward progress across a mix of
	// well-formed, CRLF-terminated, and rejected frames.
	r := protocol.NewFrameReader(bytes.NewReader([]byte("ok1\nok2\r\n")))
	for _, want := range []string{"ok1", "ok2"} {
		frame, err := r.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame() error = %v", err)
		}
		if string(frame) != want {
			t.Errorf("frame = %q, want %q", frame, want)
		}
	}
}

// --- Writer ---

func TestWriterSendProducesNewlineDelimitedJSON(t *testing.T) {
	var buf bytes.Buffer
	w := protocol.NewWriter(&buf)

	type msg struct {
		A int `json:"a"`
	}
	if err := w.Send(msg{A: 1}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if err := w.Send(msg{A: 2}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	want := "{\"a\":1}\n{\"a\":2}\n"
	if buf.String() != want {
		t.Fatalf("output = %q, want %q", buf.String(), want)
	}
}

func TestWriterSendRejectsOversizedMessage(t *testing.T) {
	var buf bytes.Buffer
	w := protocol.NewWriter(&buf)
	defer w.Close()

	huge := string(bytes.Repeat([]byte("a"), protocol.MaxMessageBytes+1))
	err := w.Send(huge)
	var tooLarge *protocol.FrameTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("Send() error = %v (%T), want *FrameTooLargeError", err, err)
	}
}

func TestWriterSendContextResultReportsPreAdmissionCancellationWithoutWrite(t *testing.T) {
	var buf bytes.Buffer
	w := protocol.NewWriter(&buf)
	defer w.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := w.SendContextResult(ctx, frame{Worker: 1, Seq: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SendContextResult() error = %v, want context.Canceled", err)
	}
	if result.WriteAdmitted {
		t.Fatal("SendContextResult() reported write admission after pre-admission cancellation")
	}
	if got := buf.String(); got != "" {
		t.Fatalf("writer output = %q, want no write", got)
	}
}

func TestWriterSendContextResultReportsAdmissionAfterFrameQueued(t *testing.T) {
	var buf bytes.Buffer
	w := protocol.NewWriter(&buf)

	result, err := w.SendContextResult(context.Background(), frame{Worker: 2, Seq: 3})
	if err != nil {
		t.Fatalf("SendContextResult() error = %v", err)
	}
	if !result.WriteAdmitted {
		t.Fatal("SendContextResult() reported false write admission after successful send")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := buf.String(); got == "" {
		t.Fatal("writer output is empty after successful admitted send")
	}
}

func TestWriterSendContextResultKeepsAdmissionAfterCancellationDuringWrite(t *testing.T) {
	sink := newGatedWriter()
	w := protocol.NewWriter(sink)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		result protocol.WriteResult
		err    error
	}, 1)
	go func() {
		result, err := w.SendContextResult(ctx, frame{Worker: 3, Seq: 4})
		done <- struct {
			result protocol.WriteResult
			err    error
		}{result: result, err: err}
	}()
	<-sink.started
	cancel()

	select {
	case outcome := <-done:
		if !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("SendContextResult() error = %v, want context.Canceled", outcome.err)
		}
		if !outcome.result.WriteAdmitted {
			t.Fatal("SendContextResult() reported false admission after underlying Write started")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendContextResult() remained blocked after cancellation during write")
	}

	close(sink.release)
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// frame is the payload the concurrent-writer test sends: each is uniquely
// identified by (Worker, Seq) so the test can assert every message was
// written exactly once with no corruption or loss.
type frame struct {
	Worker int `json:"worker"`
	Seq    int `json:"seq"`
}

func TestWriterSendIsSafeForConcurrentGoroutines(t *testing.T) {
	const workers = 100
	const perWorker = 100

	var buf bytes.Buffer
	w := protocol.NewWriter(&buf)

	var wg sync.WaitGroup
	for wk := 0; wk < workers; wk++ {
		wg.Add(1)
		go func(wk int) {
			defer wg.Done()
			for seq := 0; seq < perWorker; seq++ {
				if err := w.Send(frame{Worker: wk, Seq: seq}); err != nil {
					t.Errorf("Send() error = %v", err)
				}
			}
		}(wk)
	}
	wg.Wait()

	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	lines := bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n"))
	if len(lines) != workers*perWorker {
		t.Fatalf("got %d lines, want %d", len(lines), workers*perWorker)
	}

	seen := make(map[[2]int]bool, workers*perWorker)
	for _, line := range lines {
		var f frame
		if err := json.Unmarshal(line, &f); err != nil {
			t.Fatalf("line %q is not intact valid JSON: %v", line, err)
		}
		key := [2]int{f.Worker, f.Seq}
		if seen[key] {
			t.Fatalf("duplicate frame %v", key)
		}
		seen[key] = true
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("saw %d distinct frames, want %d", len(seen), workers*perWorker)
	}
}

// gatedWriter blocks the first Write until release is closed, letting the
// test deterministically build a backlog in the Writer's send queue before
// exercising Close, instead of relying on a sleep.
type gatedWriter struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatedWriter() *gatedWriter {
	return &gatedWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *gatedWriter) Write(p []byte) (int, error) {
	g.once.Do(func() { close(g.started) })
	<-g.release
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.Write(p)
}

func (g *gatedWriter) String() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.String()
}

func TestWriterCloseDrainsQueueThenRejectsNewSenders(t *testing.T) {
	sink := newGatedWriter()
	w := protocol.NewWriter(sink)

	// This Send is picked up by the writer's internal goroutine immediately
	// and blocks inside sink.Write until we release it below. sink.started
	// firing is a deterministic guarantee that this message has already been
	// pulled off the queue and is mid-write, so it MUST be drained (return
	// nil) rather than see the closed error, no matter when Close runs.
	blockedDone := make(chan error, 1)
	go func() { blockedDone <- w.Send(frame{Worker: -1, Seq: 0}) }()
	<-sink.started

	// These race with Close: each may or may not have been accepted onto the
	// bounded queue before Close begins shutting down, so either outcome
	// (drained, or rejected with the typed closed error) is acceptable. What
	// must never happen is a hang or any other error shape.
	const racing = 10
	racingDone := make([]chan error, racing)
	for i := 0; i < racing; i++ {
		ch := make(chan error, 1)
		racingDone[i] = ch
		go func(i int) { ch <- w.Send(frame{Worker: 0, Seq: i}) }(i)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- w.Close() }()

	// A send that arrives after Close has begun must fail fast with the
	// typed closed error rather than hang.
	overflowErrCh := make(chan error, 1)
	go func() { overflowErrCh <- w.Send(frame{Worker: 99, Seq: 0}) }()

	// Now let the blocked write (and anything queued behind it) proceed.
	close(sink.release)

	if err := <-blockedDone; err != nil {
		t.Fatalf("blocked Send() error = %v, want nil (should have been drained)", err)
	}

	assertNilOrClosed := func(label string, ch chan error) {
		select {
		case err := <-ch:
			var closedErr *protocol.WriterClosedError
			if err != nil && !errors.As(err, &closedErr) {
				t.Fatalf("%s Send() error = %v (%T), want nil or *WriterClosedError", label, err, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s Send() never returned", label)
		}
	}
	for i, ch := range racingDone {
		assertNilOrClosed(fmt.Sprintf("racing[%d]", i), ch)
	}
	assertNilOrClosed("overflow", overflowErrCh)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Close() never returned")
	}

	got := sink.String()
	trimmed := bytes.TrimRight([]byte(got), "\n")
	if len(trimmed) == 0 {
		t.Fatalf("no lines written, want at least the blocked frame")
	}
	lines := bytes.Split(trimmed, []byte("\n"))
	if len(lines) < 1 {
		t.Fatalf("got %d written lines, want at least 1", len(lines))
	}
	for _, line := range lines {
		var f frame
		if err := json.Unmarshal(line, &f); err != nil {
			t.Fatalf("line %q is not intact valid JSON: %v", line, err)
		}
	}
}

func TestWriterSendAfterCloseFailsFast(t *testing.T) {
	var buf bytes.Buffer
	w := protocol.NewWriter(&buf)
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Send(frame{Worker: 1, Seq: 1}) }()

	select {
	case err := <-done:
		var closedErr *protocol.WriterClosedError
		if !errors.As(err, &closedErr) {
			t.Fatalf("Send() after Close error = %v (%T), want *WriterClosedError", err, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Send() after Close never returned (did not fail fast)")
	}
}

func TestWriterCloseIsIdempotent(t *testing.T) {
	var buf bytes.Buffer
	w := protocol.NewWriter(&buf)
	if err := w.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

// FuzzFrameReader feeds arbitrary byte streams to FrameReader.ReadFrame in a
// loop. It must never panic, and every result must be either: a frame that
// is free of embedded NUL bytes and newlines and within MaxMessageBytes, or
// one of the reader's documented outcomes (io.EOF, *FrameTooLargeError,
// *TruncatedFrameError, *InvalidFrameError).
func FuzzFrameReader(f *testing.F) {
	seeds := []string{
		"",
		"\n",
		"{}\n",
		"{}\r\n",
		"a\nb\nc\n",
		"a\nb\nc",
		"a\x00b\n",
		"\x00\n",
		"\r\n\r\n",
		"\r\r\r\n",
		strings.Repeat("a", 8192) + "\n",
		strings.Repeat("\n", 50),
		"no newline at all",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, in string) {
		r := protocol.NewFrameReader(strings.NewReader(in))
		for {
			frameBytes, err := r.ReadFrame()
			if err != nil {
				var tooLarge *protocol.FrameTooLargeError
				var truncated *protocol.TruncatedFrameError
				var invalid *protocol.InvalidFrameError
				switch {
				case errors.Is(err, io.EOF):
				case errors.As(err, &tooLarge):
				case errors.As(err, &truncated):
				case errors.As(err, &invalid):
				default:
					t.Fatalf("ReadFrame(%q) returned unrecognized error: %v (%T)", in, err, err)
				}
				return
			}
			if bytes.IndexByte(frameBytes, 0) >= 0 {
				t.Fatalf("ReadFrame(%q) returned a frame with an embedded NUL byte: %q", in, frameBytes)
			}
			if bytes.IndexByte(frameBytes, '\n') >= 0 {
				t.Fatalf("ReadFrame(%q) returned a frame still containing a newline: %q", in, frameBytes)
			}
			if len(frameBytes) > protocol.MaxMessageBytes {
				t.Fatalf("ReadFrame(%q) returned a frame exceeding MaxMessageBytes: %d bytes", in, len(frameBytes))
			}
		}
	})
}

func ExampleWriter_marshalError() {
	var buf bytes.Buffer
	w := protocol.NewWriter(&buf)
	defer w.Close()

	// A value json.Marshal cannot encode (e.g. a channel) must surface a
	// plain encode error, not a hang or a typed framing error.
	err := w.Send(make(chan int))
	fmt.Println(err != nil)
	// Output: true
}
