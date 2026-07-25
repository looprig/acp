package stdio

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/looprig/acp/protocol"
)

const testTimeout = 5 * time.Second

// stdioPipes mimics a real agent process's standard streams as two
// independent io.Pipe pairs (distinct reader/writer objects, exactly like
// os.Stdin/os.Stdout), rather than a single rendezvous net.Conn.
type stdioPipes struct {
	agentR *io.PipeReader // agent's stdin
	agentW *io.PipeWriter // agent's stdout
	peerR  *io.PipeReader // peer's read end of agent's stdout
	peerW  *io.PipeWriter // peer's write end of agent's stdin
}

func newStdioPipes(t *testing.T) *stdioPipes {
	t.Helper()
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	p := &stdioPipes{agentR: stdinR, agentW: stdoutW, peerR: stdoutR, peerW: stdinW}
	t.Cleanup(func() {
		_ = p.agentR.Close()
		_ = p.agentW.Close()
		_ = p.peerR.Close()
		_ = p.peerW.Close()
	})
	return p
}

func TestServeReturnsCtxErrOnCancellation(t *testing.T) {
	p := newStdioPipes(t)
	conn := protocol.NewConn(p.agentR, p.agentW, protocol.ConnOptions{})
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, p.agentR, p.agentW, conn) }()

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve() error = %v, want context.Canceled", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Serve to return after ctx cancellation")
	}

	// Serve closing the agent's stdout must unblock the peer's read on it
	// with a clean EOF (Conn.Close closes both r and w since they differ).
	buf := make([]byte, 1)
	if _, err := p.peerR.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("peerR.Read() error = %v, want io.EOF", err)
	}

	// And the agent's stdin closing must unblock a peer write with a clear
	// closed-pipe error, never silently hang.
	writeDone := make(chan error, 1)
	go func() { _, err := p.peerW.Write([]byte("x")); writeDone <- err }()
	select {
	case err := <-writeDone:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("peerW.Write() error = %v, want io.ErrClosedPipe", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for peer write to unblock after agent stdin closed")
	}

	select {
	case <-conn.Done():
	default:
		t.Fatal("conn.Done() not closed after Serve returned on ctx cancellation")
	}
}

func TestServeReturnsNilWhenConnEndsOnItsOwn(t *testing.T) {
	p := newStdioPipes(t)
	conn := protocol.NewConn(p.agentR, p.agentW, protocol.ConnOptions{})
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, p.agentR, p.agentW, conn) }()

	// The peer closing its write end is a clean EOF for the agent's
	// FrameReader, which drives conn into shutdown entirely on its own, with
	// ctx never firing.
	_ = p.peerW.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve() error = %v, want nil", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for Serve to return after conn ended on its own")
	}
}

func TestServeReturnsClearErrorForNilConn(t *testing.T) {
	p := newStdioPipes(t)
	err := Serve(context.Background(), p.agentR, p.agentW, nil)
	if err == nil {
		t.Fatal("Serve() error = nil, want a clear error for a nil conn")
	}
}

// --- Command validation (no process boundary crossed: these never call
// exec.Start, so they belong in the unit suite, not the integration one). ---

func TestSpawnRejectsRelativePath(t *testing.T) {
	_, err := Spawn(context.Background(), Command{Path: "relative/path", Env: []string{}})
	assertCommandError(t, err, "Path")
}

func TestSpawnRejectsUncleanedPath(t *testing.T) {
	_, err := Spawn(context.Background(), Command{Path: "/usr/bin/../bin/true", Env: []string{}})
	assertCommandError(t, err, "Path")
}

func TestSpawnRejectsEmptyPath(t *testing.T) {
	_, err := Spawn(context.Background(), Command{Env: []string{}})
	assertCommandError(t, err, "Path")
}

func TestSpawnRejectsUncleanedDir(t *testing.T) {
	_, err := Spawn(context.Background(), Command{
		Path: "/bin/true",
		Dir:  "relative/dir",
		Env:  []string{},
	})
	assertCommandError(t, err, "Dir")
}

func TestSpawnRejectsAlreadyCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Spawn(ctx, Command{Path: "/bin/true", Env: []string{}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Spawn() error = %v, want context.Canceled", err)
	}
}

func assertCommandError(t *testing.T, err error, field string) {
	t.Helper()
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("Spawn() error = %#v, want *CommandError", err)
	}
	if cmdErr.Field != field {
		t.Fatalf("CommandError.Field = %q, want %q", cmdErr.Field, field)
	}
}

// --- stderr ring buffer: bounded to the last stderrRingCapacity bytes. ---

func TestStderrRingBoundsToCapacity(t *testing.T) {
	r := newStderrRing()
	const chunk = 1024
	const chunks = 16 // total 16 KiB written, twice stderrRingCapacity
	var reference []byte
	for i := 0; i < chunks; i++ {
		b := make([]byte, chunk)
		for j := range b {
			b[j] = byte(i) // distinct content per chunk, cheap to verify
		}
		reference = append(reference, b...)
		n, err := r.Write(b)
		if err != nil || n != chunk {
			t.Fatalf("Write() = (%d, %v), want (%d, nil)", n, err, chunk)
		}
	}
	got := r.Bytes()
	if len(got) != stderrRingCapacity {
		t.Fatalf("len(Bytes()) = %d, want %d", len(got), stderrRingCapacity)
	}
	want := reference[len(reference)-stderrRingCapacity:]
	if string(got) != string(want) {
		t.Fatal("Bytes() does not match the trailing stderrRingCapacity bytes written")
	}
}

func TestStderrRingUnderCapacityReturnsExactBytes(t *testing.T) {
	r := newStderrRing()
	if _, err := r.Write([]byte("boom")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := string(r.Bytes()); got != "boom" {
		t.Fatalf("Bytes() = %q, want %q", got, "boom")
	}
}

func TestExitErrorWrapsUnderlyingAndStderrTail(t *testing.T) {
	cause := errors.New("exit status 1")
	err := &ExitError{Err: cause, Stderr: []byte("bad things happened")}
	if !errors.Is(err, cause) {
		t.Fatal("errors.Is(err, cause) = false, want true")
	}
	if got := err.Error(); got == "" {
		t.Fatal("Error() = \"\", want non-empty message")
	}
}
