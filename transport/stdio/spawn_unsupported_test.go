//go:build !darwin && (!linux || android)

package stdio

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
)

// TestSpawnFailsBeforeStartingAChildOnUnsupportedPlatforms mirrors
// foreignloops/driver/claude's unsupported-platform contract: a platform
// without the required process-group supervision must fail before any child
// is started, never fall back to weaker (unsupervised) spawning.
func TestSpawnFailsBeforeStartingAChildOnUnsupportedPlatforms(t *testing.T) {
	execPath, err := filepath.Abs(filepath.Join(t.TempDir(), "true"))
	if err != nil {
		t.Fatalf("absolute path: %v", err)
	}
	proc, err := Spawn(context.Background(), Command{Path: execPath, Env: []string{}})
	if proc != nil {
		t.Fatalf("Spawn() proc = %#v, want nil", proc)
	}
	var platformErr *PlatformError
	if !errors.As(err, &platformErr) || platformErr.GOOS != runtime.GOOS {
		t.Fatalf("Spawn() error = %T %v, want *PlatformError for %s", err, err, runtime.GOOS)
	}
}
