package client

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// assertNoGoroutineLeak captures the current goroutine count and registers a
// t.Cleanup that fails the test if the count has not returned to that
// baseline within 1s. Mirrors protocol package's own helper of the same
// name (protocol/conn_test.go) — call it before creating any Client under
// test.
func assertNoGoroutineLeak(t *testing.T) {
	t.Helper()
	runtime.Gosched()
	baseline := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(1 * time.Second)
		for {
			n := runtime.NumGoroutine()
			if n <= baseline {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("goroutine leak: NumGoroutine() = %d, want <= %d (baseline) within 1s", n, baseline)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
}

// TestCloseLeavesNoGoroutinesRunning proves that Dial followed by Close (and
// draining/using a Session in between) leaves no watchDeath or session pump
// goroutine running behind.
func TestCloseLeavesNoGoroutinesRunning(t *testing.T) {
	assertNoGoroutineLeak(t)

	c, fa := dialTestClient(t, Options{})
	_ = newSessionForTest(t, c, fa, "sess-leak")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
