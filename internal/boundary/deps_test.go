package boundary

import (
	"errors"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type boundaryViolationKind string

const (
	boundaryRootGoFile            boundaryViolationKind = "root Go file"
	boundaryHarnessInternalImport boundaryViolationKind = "Harness internal import"
	harnessInternalImportRoot                           = "github.com/looprig/harness/internal"
)

type boundaryViolation struct {
	Kind       boundaryViolationKind
	File       string
	ImportPath string
}

func TestScanModuleBoundariesRejectsNestedInternalImportAndRootGo(t *testing.T) {
	root := t.TempDir()
	writeBoundaryFixture(t, filepath.Join(root, "bad.go"), "package forbiddenroot\n")
	writeBoundaryFixture(t, filepath.Join(root, "agent", "nested", "bad.go"), `//go:build fixture_bad

package nested

import _ "github.com/looprig/harness/internal/sessionruntime"
`)

	violations, err := scanModuleBoundaries(root)
	if err != nil {
		t.Fatalf("scan synthetic module: %v", err)
	}
	if !hasBoundaryViolation(violations, boundaryRootGoFile, "bad.go", "") {
		t.Errorf("violations = %#v, want root Go-file rejection", violations)
	}
	if !hasBoundaryViolation(
		violations,
		boundaryHarnessInternalImport,
		filepath.Join("agent", "nested", "bad.go"),
		"github.com/looprig/harness/internal/sessionruntime",
	) {
		t.Errorf("violations = %#v, want nested Harness-internal import rejection", violations)
	}
	if len(violations) != 2 {
		t.Errorf("len(violations) = %d, want 2: %#v", len(violations), violations)
	}
}

func TestScanModuleBoundariesRejectsHarnessInternalImportsFromInactiveTests(t *testing.T) {
	root := t.TempDir()
	writeBoundaryFixture(t, filepath.Join(root, "agent", "nested", "bad_plan9_test.go"), `//go:build plan9

package nested

import _ "github.com/looprig/harness/internal/sessionruntime"
`)
	writeBoundaryFixture(t, filepath.Join(root, "nested-module", "go.mod"), "module example.com/nested\n")
	writeBoundaryFixture(t, filepath.Join(root, "nested-module", "ignored_test.go"), `package nested

import _ "github.com/looprig/harness/internal/sessionruntime"
`)
	writeBoundaryFixture(t, filepath.Join(root, "vendor", "ignored_test.go"), `package vendor

import _ "github.com/looprig/harness/internal/sessionruntime"
`)

	violations, err := scanModuleBoundaries(root)
	if err != nil {
		t.Fatalf("scan synthetic module: %v", err)
	}
	wantFile := filepath.Join("agent", "nested", "bad_plan9_test.go")
	if !hasBoundaryViolation(violations, boundaryHarnessInternalImport, wantFile, "github.com/looprig/harness/internal/sessionruntime") {
		t.Errorf("violations = %#v, want inactive test Harness-internal import rejection", violations)
	}
	if len(violations) != 1 {
		t.Errorf("len(violations) = %d, want 1: %#v", len(violations), violations)
	}
}

// TestModuleBoundaries scans the real module tree. Today the module has no
// packages beyond internal/boundary and internal/buildtest, so it trivially
// finds no violations; it starts enforcing the moment protocol, transport,
// client, or agent packages land. The finer-grained layering rule (protocol,
// transport/stdio, and client must not import harness or core at all; only
// agent may import harness) is a dependency-guard test added in Task 1.9.
func TestModuleBoundaries(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	violations, err := scanModuleBoundaries(root)
	if err != nil {
		t.Fatalf("scan module boundaries: %v", err)
	}
	for _, violation := range violations {
		switch violation.Kind {
		case boundaryRootGoFile:
			t.Errorf("forbidden root-level Go file: %s", violation.File)
		case boundaryHarnessInternalImport:
			t.Errorf("%s imports forbidden Harness internal package %q", violation.File, violation.ImportPath)
		default:
			t.Errorf("unknown module-boundary violation: %#v", violation)
		}
	}
}

func scanModuleBoundaries(root string) ([]boundaryViolation, error) {
	var violations []boundaryViolation
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path == root {
				return nil
			}
			if skipBoundaryDirectory(entry.Name()) {
				return fs.SkipDir
			}
			_, err := os.Stat(filepath.Join(path, "go.mod"))
			switch {
			case err == nil:
				return fs.SkipDir
			case !errors.Is(err, os.ErrNotExist):
				return err
			default:
				return nil
			}
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if filepath.Dir(rel) == "." {
			violations = append(violations, boundaryViolation{Kind: boundaryRootGoFile, File: rel})
			return nil
		}
		parsed, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range parsed.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			if importPath == harnessInternalImportRoot || strings.HasPrefix(importPath, harnessInternalImportRoot+"/") {
				violations = append(violations, boundaryViolation{
					Kind:       boundaryHarnessInternalImport,
					File:       rel,
					ImportPath: importPath,
				})
			}
		}
		return nil
	})
	return violations, err
}

func skipBoundaryDirectory(name string) bool {
	return name == "CVS" || name == "vendor" || name == "worktrees" || strings.HasPrefix(name, ".")
}

func hasBoundaryViolation(violations []boundaryViolation, kind boundaryViolationKind, file, importPath string) bool {
	for _, violation := range violations {
		if violation.Kind == kind && violation.File == file && violation.ImportPath == importPath {
			return true
		}
	}
	return false
}

func writeBoundaryFixture(t *testing.T, path, source string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatalf("write fixture %s: %v", path, err)
	}
}
