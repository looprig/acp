// framing.go implements the raw newline-delimited (NDJSON) transport framing
// that sits one layer below the JSON-RPC envelope in jsonrpc.go: it knows
// only about lines and bytes, never about JSON-RPC shapes or ACP methods.
// FrameReader turns an io.Reader into a sequence of length-bounded frames;
// Writer turns concurrent Send calls into a single, serialized stream of
// newline-terminated frames on an io.Writer, so no two goroutines ever write
// to the underlying transport at once.
//
// All bytes read by FrameReader are untrusted wire input (see acp/CLAUDE.md:
// validate at the boundary, fail closed): size and content are checked
// before a frame is ever handed to a caller for JSON decoding.
package protocol

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// SendQueueDepth bounds how many frames Writer will buffer between a caller's
// Send call and the single internal goroutine that owns the underlying
// io.Writer. A Send that would exceed this depth blocks until room is
// available or the Writer is closed.
const SendQueueDepth = 256

// frameReadBufferSize is the chunk size FrameReader reads from the
// underlying io.Reader at a time. It only bounds per-read syscall size, not
// the maximum frame length (that is MaxMessageBytes).
const frameReadBufferSize = 4096

// FrameTooLargeError reports that a single line exceeded MaxMessageBytes
// before a terminating newline was found.
type FrameTooLargeError struct {
	// Limit is the byte limit that was exceeded (always MaxMessageBytes).
	Limit int
}

func (e *FrameTooLargeError) Error() string {
	return fmt.Sprintf("protocol: frame exceeds %d byte limit", e.Limit)
}

// TruncatedFrameError reports that the underlying reader reached EOF in the
// middle of a line: some bytes were read on the final line, but no
// terminating "\n" was ever found. A clean end of stream (EOF exactly at a
// frame boundary, with zero bytes read on the next attempt) is reported as
// plain io.EOF instead, never this type.
type TruncatedFrameError struct {
	// Read is the number of content bytes read on the truncated trailing
	// line before EOF.
	Read int
}

func (e *TruncatedFrameError) Error() string {
	return fmt.Sprintf("protocol: truncated frame after %d byte(s): unexpected EOF", e.Read)
}

// InvalidFrameError reports a frame that violated a structural framing rule
// other than size or truncation. Currently the only such rule is: a frame
// may never contain an embedded NUL byte, since one can never appear in
// valid JSON text and typically indicates a desynchronized or corrupted
// stream.
type InvalidFrameError struct {
	Reason string
}

func (e *InvalidFrameError) Error() string {
	return fmt.Sprintf("protocol: invalid frame: %s", e.Reason)
}

// FrameReader reads newline-delimited frames from an underlying io.Reader.
// Each frame is one line with its trailing "\n" (and an optional preceding
// "\r", tolerating CRLF line endings) removed. It is not safe for concurrent
// use: a FrameReader is meant to be owned by a single reader loop, mirroring
// how Writer is the single-writer counterpart for output.
type FrameReader struct {
	br *bufio.Reader
}

// NewFrameReader wraps r for frame-at-a-time reading.
func NewFrameReader(r io.Reader) *FrameReader {
	return &FrameReader{br: bufio.NewReaderSize(r, frameReadBufferSize)}
}

// ReadFrame returns the next frame's content, with its line terminator
// stripped. It returns io.EOF (via errors.Is) when the stream ends cleanly
// at a frame boundary, *TruncatedFrameError when the stream ends mid-line,
// *FrameTooLargeError when a line exceeds MaxMessageBytes before a
// terminator is found, and *InvalidFrameError when the frame contains an
// embedded NUL byte.
func (fr *FrameReader) ReadFrame() ([]byte, error) {
	var buf []byte
	for {
		chunk, err := fr.br.ReadSlice('\n')
		if len(chunk) > 0 {
			buf = append(buf, chunk...)
		}
		if err == nil {
			// Found the delimiter: buf ends with '\n'.
			break
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			if len(buf) > MaxMessageBytes {
				return nil, &FrameTooLargeError{Limit: MaxMessageBytes}
			}
			continue
		}
		if errors.Is(err, io.EOF) {
			if len(buf) == 0 {
				return nil, io.EOF
			}
			return nil, &TruncatedFrameError{Read: len(buf)}
		}
		return nil, err
	}

	buf = bytes.TrimSuffix(buf, []byte("\n"))
	buf = bytes.TrimSuffix(buf, []byte("\r"))

	if len(buf) > MaxMessageBytes {
		return nil, &FrameTooLargeError{Limit: MaxMessageBytes}
	}
	if bytes.IndexByte(buf, 0) >= 0 {
		return nil, &InvalidFrameError{Reason: "embedded NUL byte"}
	}
	return buf, nil
}

// WriterClosedError is returned by Send once the Writer has been (or is
// being) closed: it unblocks every sender that was not already drained, so
// none can hang waiting on a writer that will never make progress again.
type WriterClosedError struct {
	cause error
}

func (e *WriterClosedError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("protocol: writer closed: %v", e.cause)
	}
	return "protocol: writer closed"
}

// Unwrap exposes the cause that led to closing, if any (for example an error
// surfaced from the underlying io.Writer).
func (e *WriterClosedError) Unwrap() error { return e.cause }

// writeJob is one queued Send: the fully framed line to write, and the
// channel its result is delivered on.
type writeJob struct {
	ctx   context.Context
	line  []byte
	errCh chan error
}

// Writer serializes concurrent Send calls into a single stream of
// newline-terminated JSON frames written to an underlying io.Writer. Exactly
// one internal goroutine ever calls the underlying Write, so Writer is safe
// to share across arbitrarily many goroutines even when the underlying
// io.Writer is not itself concurrency-safe.
//
// Close coordinates with in-flight Send calls through an "admission" scheme
// rather than racing a done-channel against each Send's result wait: mu and
// closed gate entry so that every Send is classified, atomically, as either
// admitted (guaranteed a real write attempt and a result from errCh) or
// rejected outright (WriterClosedError, no job ever created). admitted
// tracks every currently-admitted Send so Close can wait for all of them to
// fully complete — with the writer goroutine still actively servicing the
// queue throughout — before it tells that goroutine no further work will
// ever arrive. This removes any window where a job could be silently
// dropped or where a sender whose write already succeeded could still
// observe a closed error.
type Writer struct {
	mu       sync.Mutex
	closed   bool
	admitted sync.WaitGroup

	queue    chan writeJob
	stopping chan struct{}
	runDone  chan struct{}
	once     sync.Once
}

// NewWriter starts the internal writer goroutine over w and returns a Writer
// ready for concurrent Send calls.
func NewWriter(w io.Writer) *Writer {
	wr := &Writer{
		queue:    make(chan writeJob, SendQueueDepth),
		stopping: make(chan struct{}),
		runDone:  make(chan struct{}),
	}
	go wr.run(w)
	return wr
}

func (wr *Writer) run(w io.Writer) {
	defer close(wr.runDone)
	for {
		select {
		case job := <-wr.queue:
			wr.runJob(w, job)
		case <-wr.stopping:
			// By the time stopping is closed, Close has already waited for
			// every admitted Send to finish enqueuing (see Close), so
			// draining whatever remains in the buffer is sufficient: no
			// further job can appear after this point.
			for {
				select {
				case job := <-wr.queue:
					wr.runJob(w, job)
				default:
					return
				}
			}
		}
	}
}

func (wr *Writer) runJob(w io.Writer, job writeJob) {
	defer wr.admitted.Done()
	select {
	case <-job.ctx.Done():
		job.errCh <- job.ctx.Err()
	default:
		_, err := w.Write(job.line)
		job.errCh <- err
	}
}

// Send marshals msg as JSON, appends a newline, and hands the resulting
// frame to the single internal writer goroutine, blocking until it has been
// written. Once the Writer is closed (or closing), Send fails fast with a
// *WriterClosedError instead of blocking. It is safe to call Send from any
// number of goroutines concurrently.
func (wr *Writer) Send(msg any) error {
	return wr.SendContext(context.Background(), msg)
}

// SendContext is Send's cancellation-aware form. If ctx is canceled while a
// job is queued, the writer skips the underlying write and returns ctx.Err().
// If the underlying Write is already blocked, SendContext returns ctx.Err()
// to its caller but keeps the admitted job tracked until the write completes;
// transport shutdown must still release that write before Writer.Close can
// finish. This preserves Writer.Close's no-lost-admitted-job accounting and
// keeps the writer goroutine owned by the Writer rather than by the caller.
func (wr *Writer) SendContext(ctx context.Context, msg any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("protocol: marshal frame for send: %w", err)
	}
	if len(data) > MaxMessageBytes {
		return &FrameTooLargeError{Limit: MaxMessageBytes}
	}

	wr.mu.Lock()
	if wr.closed {
		wr.mu.Unlock()
		return &WriterClosedError{}
	}
	wr.admitted.Add(1)
	wr.mu.Unlock()

	line := make([]byte, 0, len(data)+1)
	line = append(line, data...)
	line = append(line, '\n')

	errCh := make(chan error, 1)
	job := writeJob{ctx: ctx, line: line, errCh: errCh}
	// This send onto queue cannot block forever: it is only reached while
	// wr.closed is still false at the moment Close's Wait below could
	// observe this goroutine, which means the writer goroutine has not yet
	// been told to stop servicing wr.queue and so is guaranteed to keep
	// draining it (including growing room for this send) until every
	// admitted Send — this one included — completes.
	select {
	case wr.queue <- job:
	case <-ctx.Done():
		wr.admitted.Done()
		return ctx.Err()
	}

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops the Writer from accepting any further Send calls, waits for
// every already-admitted Send to be fully written, then stops the internal
// writer goroutine and waits for it to exit. Any Send that had not yet been
// admitted at the moment Close was called returns a *WriterClosedError
// immediately rather than blocking. Close is idempotent and safe to call
// concurrently with Send or with itself.
func (wr *Writer) Close() error {
	wr.once.Do(func() {
		wr.mu.Lock()
		wr.closed = true
		wr.mu.Unlock()

		wr.admitted.Wait()
		close(wr.stopping)
	})
	<-wr.runDone
	return nil
}
