package protocol

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestGeneratedFilesAreNotStale regenerates types_gen.go and methods_gen.go
// into a temporary directory from the committed schema artifacts and
// byte-compares the result against what is committed in this package.
// Drift — a hand-edit, a schema bump without regenerating, or a generator
// change whose output was never regenerated — fails this test instead of
// silently diverging.
func TestGeneratedFilesAreNotStale(t *testing.T) {
	dir := t.TempDir()

	cmd := exec.Command("go", "run", "../internal/gen",
		"-schema", "schema/v1/schema.json",
		"-meta", "schema/v1/meta.json",
		"-out", dir,
	)
	cmd.Dir = "."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("regenerate failed: %v\n%s", err, out)
	}

	for _, name := range []string{"types_gen.go", "methods_gen.go"} {
		committed, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read committed %s: %v", name, err)
		}
		fresh, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("read regenerated %s: %v", name, err)
		}
		if string(committed) != string(fresh) {
			t.Errorf("%s is stale: committed output does not match a fresh run of the generator against the pinned schema artifacts. Run `go generate ./protocol/...` and commit the result.", name)
		}
	}
}
