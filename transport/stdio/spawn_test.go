package stdio

import (
	"testing"
	"time"
)

// TestStopTimerDoesNotDrainStoppedChannel covers Go 1.23+'s synchronous timer
// channel contract. A second Stop returns false, but there is no stale value to
// drain from C; attempting the pre-1.23 drain would block forever.
func TestStopTimerDoesNotDrainStoppedChannel(t *testing.T) {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		t.Fatal("initial timer stop returned false")
	}

	done := make(chan struct{})
	go func() {
		stopTimer(timer)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stopping an already-stopped timer blocked draining its channel")
	}
}
