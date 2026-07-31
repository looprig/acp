package boundary

import (
	"errors"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Layering rule for this module (see acp/CLAUDE.md): acp/agent is the only
// PRODUCT-FACING package that may import Harness's or Core's *public*
// packages, and even it must not reach into their internal/ packages.
// internal/exampleagent (Task 6.1's thin, test-only composition — see its own
// package doc) is the one deliberate exception: it exists specifically to
// wire a minimal in-memory SessionHost/LiveSession implementation onto the
// real acp/agent facade, which unavoidably means depending on the same
// Harness/Core public packages a real product would (content, uuid, event,
// gate, journal, sessionstore) — never on their internal/ packages, exactly
// like acp/agent itself. Every OTHER package in this module (protocol,
// transport/stdio, client, mockpeer, and every other internal/* tooling
// package) must not import Harness or Core at all, directly or transitively
// through another local acp package. No package anywhere in the module may
// import github.com/looprig/foreignloops or github.com/looprig/inference:
// unlike Harness/Core, neither has a product-facing seam package in this
// module at all (see acp/launch's own package doc), so this ban applies
// even to acp/agent -- there is no carve-out for it the way
// mayImportHarnessOrCorePublic grants one for Harness/Core.
//
// scanModuleBoundaries enforces this by building the local import graph from
// source (go/parser, not go/packages or `go list`: see acp/CLAUDE.md on
// external dependencies) and computing, for every local package directory,
// the set of interesting external imports reachable from it — directly or
// through any chain of local acp imports. That reachability computation is
// what makes the guard automatically cover acp/agent and acp/client the
// moment those packages exist: nothing here hard-codes a package list.
type boundaryViolationKind string

const (
	boundaryRootGoFile          boundaryViolationKind = "root Go file"
	boundaryAgentInternalImport boundaryViolationKind = "agent package importing Harness/Core internal package"
	boundaryWireLayerImport     boundaryViolationKind = "non-agent package importing Harness or Core"
	boundaryForeignloopsImport  boundaryViolationKind = "package importing foreignloops"
	boundaryInferenceImport     boundaryViolationKind = "package importing inference"

	moduleImportRoot          = "github.com/looprig/acp"
	harnessImportRoot         = "github.com/looprig/harness"
	harnessInternalImportRoot = "github.com/looprig/harness/internal"
	coreImportRoot            = "github.com/looprig/core"
	coreInternalImportRoot    = "github.com/looprig/core/internal"
	foreignloopsImportRoot    = "github.com/looprig/foreignloops"
	inferenceImportRoot       = "github.com/looprig/inference"
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
		boundaryAgentInternalImport,
		filepath.Join("agent", "nested", "bad.go"),
		"github.com/looprig/harness/internal/sessionruntime",
	) {
		t.Errorf("violations = %#v, want nested agent Harness-internal import rejection", violations)
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
	if !hasBoundaryViolation(violations, boundaryAgentInternalImport, wantFile, "github.com/looprig/harness/internal/sessionruntime") {
		t.Errorf("violations = %#v, want inactive test agent Harness-internal import rejection", violations)
	}
	if len(violations) != 1 {
		t.Errorf("len(violations) = %d, want 1: %#v", len(violations), violations)
	}
}

func TestScanModuleBoundariesAllowsAgentHarnessAndCorePublicImports(t *testing.T) {
	root := t.TempDir()
	writeBoundaryFixture(t, filepath.Join(root, "agent", "ok.go"), `package agent

import (
	_ "github.com/looprig/harness/pkg/foreign"
	_ "github.com/looprig/core/content"
)
`)

	violations, err := scanModuleBoundaries(root)
	if err != nil {
		t.Fatalf("scan synthetic module: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("violations = %#v, want none: acp/agent may import Harness/Core public packages", violations)
	}
}

// TestScanModuleBoundariesAllowsExampleAgentHarnessAndCorePublicImports
// proves internal/exampleagent gets the same allowance as acp/agent itself
// (see mayImportHarnessOrCorePublic's doc): Task 6.1's composition needs
// Harness/Core public packages to implement SessionHost/LiveSession, exactly
// like a real product would.
func TestScanModuleBoundariesAllowsExampleAgentHarnessAndCorePublicImports(t *testing.T) {
	root := t.TempDir()
	writeBoundaryFixture(t, filepath.Join(root, "internal", "exampleagent", "ok.go"), `package main

import (
	_ "github.com/looprig/harness/pkg/foreign"
	_ "github.com/looprig/core/content"
)
`)

	violations, err := scanModuleBoundaries(root)
	if err != nil {
		t.Fatalf("scan synthetic module: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("violations = %#v, want none: internal/exampleagent may import Harness/Core public packages", violations)
	}
}

// TestScanModuleBoundariesRejectsExampleAgentHarnessInternalImport proves
// internal/exampleagent is held to the same "public only, never internal/"
// discipline as acp/agent — the allowance in
// TestScanModuleBoundariesAllowsExampleAgentHarnessAndCorePublicImports is
// not a blanket exemption.
func TestScanModuleBoundariesRejectsExampleAgentHarnessInternalImport(t *testing.T) {
	root := t.TempDir()
	writeBoundaryFixture(t, filepath.Join(root, "internal", "exampleagent", "bad.go"), `package main

import _ "github.com/looprig/harness/internal/sessionruntime"
`)

	violations, err := scanModuleBoundaries(root)
	if err != nil {
		t.Fatalf("scan synthetic module: %v", err)
	}
	wantFile := filepath.Join("internal", "exampleagent", "bad.go")
	if !hasBoundaryViolation(violations, boundaryAgentInternalImport, wantFile, "github.com/looprig/harness/internal/sessionruntime") {
		t.Errorf("violations = %#v, want exampleagent Harness-internal import rejection", violations)
	}
	if len(violations) != 1 {
		t.Errorf("len(violations) = %d, want 1: %#v", len(violations), violations)
	}
}

func TestScanModuleBoundariesRejectsWireLayerHarnessOrCoreImport(t *testing.T) {
	root := t.TempDir()
	writeBoundaryFixture(t, filepath.Join(root, "protocol", "bad.go"), `package protocol

import (
	_ "github.com/looprig/harness/pkg/foreign"
	_ "github.com/looprig/core/content"
)
`)

	violations, err := scanModuleBoundaries(root)
	if err != nil {
		t.Fatalf("scan synthetic module: %v", err)
	}
	wantFile := filepath.Join("protocol", "bad.go")
	for _, importPath := range []string{"github.com/looprig/harness/pkg/foreign", "github.com/looprig/core/content"} {
		if !hasBoundaryViolation(violations, boundaryWireLayerImport, wantFile, importPath) {
			t.Errorf("violations = %#v, want wire-layer rejection of %q", violations, importPath)
		}
	}
	if len(violations) != 2 {
		t.Errorf("len(violations) = %d, want 2: %#v", len(violations), violations)
	}
}

func TestScanModuleBoundariesRejectsForeignloopsImportEverywhere(t *testing.T) {
	root := t.TempDir()
	writeBoundaryFixture(t, filepath.Join(root, "protocol", "bad.go"), `package protocol

import _ "github.com/looprig/foreignloops/backend"
`)
	writeBoundaryFixture(t, filepath.Join(root, "agent", "bad.go"), `package agent

import _ "github.com/looprig/foreignloops"
`)

	violations, err := scanModuleBoundaries(root)
	if err != nil {
		t.Fatalf("scan synthetic module: %v", err)
	}
	if !hasBoundaryViolation(violations, boundaryForeignloopsImport, filepath.Join("protocol", "bad.go"), "github.com/looprig/foreignloops/backend") {
		t.Errorf("violations = %#v, want protocol foreignloops rejection", violations)
	}
	if !hasBoundaryViolation(violations, boundaryForeignloopsImport, filepath.Join("agent", "bad.go"), "github.com/looprig/foreignloops") {
		t.Errorf("violations = %#v, want agent foreignloops rejection (foreignloops is banned everywhere, not just outside agent)", violations)
	}
	if len(violations) != 2 {
		t.Errorf("len(violations) = %d, want 2: %#v", len(violations), violations)
	}
}

// TestScanModuleBoundariesRejectsInferenceImportEverywhere proves
// github.com/looprig/inference is banned from every package in this module,
// including acp/agent: unlike Harness/Core, inference has no product-facing
// seam package here at all (see acp/launch's own package doc on why it
// deliberately never imports inference), so mayImportHarnessOrCorePublic's
// carve-out must not extend to it.
func TestScanModuleBoundariesRejectsInferenceImportEverywhere(t *testing.T) {
	root := t.TempDir()
	writeBoundaryFixture(t, filepath.Join(root, "launch", "bad.go"), `package launch

import _ "github.com/looprig/inference/gateway"
`)
	writeBoundaryFixture(t, filepath.Join(root, "agent", "bad.go"), `package agent

import _ "github.com/looprig/inference"
`)

	violations, err := scanModuleBoundaries(root)
	if err != nil {
		t.Fatalf("scan synthetic module: %v", err)
	}
	if !hasBoundaryViolation(violations, boundaryInferenceImport, filepath.Join("launch", "bad.go"), "github.com/looprig/inference/gateway") {
		t.Errorf("violations = %#v, want launch inference rejection", violations)
	}
	if !hasBoundaryViolation(violations, boundaryInferenceImport, filepath.Join("agent", "bad.go"), "github.com/looprig/inference") {
		t.Errorf("violations = %#v, want agent inference rejection (inference is banned everywhere, not just outside agent)", violations)
	}
	if len(violations) != 2 {
		t.Errorf("len(violations) = %d, want 2: %#v", len(violations), violations)
	}
}

// TestScanModuleBoundariesRejectsTransitiveHarnessThroughLocalPackage proves
// the guard walks the local import graph rather than only inspecting direct
// imports: "client" never mentions Harness itself, but it imports "agent",
// which legitimately does, so client transitively depends on Harness and
// must be rejected exactly as if it had imported Harness directly.
func TestScanModuleBoundariesRejectsTransitiveHarnessThroughLocalPackage(t *testing.T) {
	root := t.TempDir()
	writeBoundaryFixture(t, filepath.Join(root, "agent", "agent.go"), `package agent

import _ "github.com/looprig/harness/pkg/foreign"
`)
	writeBoundaryFixture(t, filepath.Join(root, "client", "bad.go"), `package client

import _ "github.com/looprig/acp/agent"
`)

	violations, err := scanModuleBoundaries(root)
	if err != nil {
		t.Fatalf("scan synthetic module: %v", err)
	}
	wantFile := filepath.Join("client", "bad.go")
	if !hasBoundaryViolation(violations, boundaryWireLayerImport, wantFile, "github.com/looprig/harness/pkg/foreign") {
		t.Errorf("violations = %#v, want client rejected for transitively reaching Harness via acp/agent", violations)
	}
	if len(violations) != 1 {
		t.Errorf("len(violations) = %d, want 1: %#v", len(violations), violations)
	}
}

// TestModuleBoundaries scans the real module tree. It enforces every rule
// above against whatever packages currently exist below the module root, and
// starts enforcing against acp/agent and acp/client automatically the moment
// those packages land — nothing here needs updating when they do.
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
		case boundaryAgentInternalImport:
			t.Errorf("%s imports forbidden Harness/Core internal package %q (acp/agent may use Harness/Core public packages, never internal)", violation.File, violation.ImportPath)
		case boundaryWireLayerImport:
			t.Errorf("%s imports %q, which is forbidden outside acp/agent (directly or transitively)", violation.File, violation.ImportPath)
		case boundaryForeignloopsImport:
			t.Errorf("%s imports forbidden package %q: no package in this module may depend on foreignloops", violation.File, violation.ImportPath)
		default:
			t.Errorf("unknown module-boundary violation: %#v", violation)
		}
	}
}

// packageImports collects, per local package directory (relative to the
// module root, forward-slash separated), the information scanModuleBoundaries
// needs to compute layering violations.
type packageImports struct {
	// directExternal maps an interesting external import path (Harness, Core,
	// or foreignloops) to the first file in this directory that imports it.
	directExternal map[string]string
	// localEdges maps a local acp package directory imported from this
	// directory to the first file that imports it.
	localEdges map[string]string
}

func scanModuleBoundaries(root string) ([]boundaryViolation, error) {
	var violations []boundaryViolation
	packages := make(map[string]*packageImports)
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
		dir := filepath.Dir(rel)
		if dir == "." {
			violations = append(violations, boundaryViolation{Kind: boundaryRootGoFile, File: rel})
			return nil
		}
		dirSlash := filepath.ToSlash(dir)

		parsed, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range parsed.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			switch {
			case hasImportPrefix(importPath, harnessImportRoot),
				hasImportPrefix(importPath, coreImportRoot),
				hasImportPrefix(importPath, foreignloopsImportRoot),
				hasImportPrefix(importPath, inferenceImportRoot):
				pkg := packages[dirSlash]
				if pkg == nil {
					pkg = &packageImports{directExternal: map[string]string{}, localEdges: map[string]string{}}
					packages[dirSlash] = pkg
				}
				if _, seen := pkg.directExternal[importPath]; !seen {
					pkg.directExternal[importPath] = rel
				}
			case hasImportPrefix(importPath, moduleImportRoot):
				neighbor := strings.TrimPrefix(strings.TrimPrefix(importPath, moduleImportRoot), "/")
				if neighbor == "" || neighbor == dirSlash {
					continue
				}
				pkg := packages[dirSlash]
				if pkg == nil {
					pkg = &packageImports{directExternal: map[string]string{}, localEdges: map[string]string{}}
					packages[dirSlash] = pkg
				}
				if _, seen := pkg.localEdges[neighbor]; !seen {
					pkg.localEdges[neighbor] = rel
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	reach := transitiveExternalImports(packages)
	for _, dir := range sortedKeys(packages) {
		pkg := packages[dir]
		for _, importPath := range sortedSet(reach[dir]) {
			kind, ok := classifyBoundaryViolation(dir, importPath)
			if !ok {
				continue
			}
			file, ok := pkg.directExternal[importPath]
			if !ok {
				file = firstLocalEdgeFile(pkg)
			}
			violations = append(violations, boundaryViolation{Kind: kind, File: file, ImportPath: importPath})
		}
	}
	return violations, nil
}

// classifyBoundaryViolation decides, for a package directory and an external
// import path reachable from it (directly or transitively), whether that
// combination violates this module's layering rule.
func classifyBoundaryViolation(dir, importPath string) (boundaryViolationKind, bool) {
	if hasImportPrefix(importPath, foreignloopsImportRoot) {
		return boundaryForeignloopsImport, true
	}
	if hasImportPrefix(importPath, inferenceImportRoot) {
		return boundaryInferenceImport, true
	}
	if mayImportHarnessOrCorePublic(dir) {
		if hasImportPrefix(importPath, harnessInternalImportRoot) || hasImportPrefix(importPath, coreInternalImportRoot) {
			return boundaryAgentInternalImport, true
		}
		return "", false
	}
	if hasImportPrefix(importPath, harnessImportRoot) || hasImportPrefix(importPath, coreImportRoot) {
		return boundaryWireLayerImport, true
	}
	return "", false
}

// transitiveExternalImports computes, for every local package directory, the
// set of interesting external import paths reachable from it by following
// zero or more local acp import edges. It is a fixed-point computation over
// the (small, module-local) import graph, so it is correct even in the
// presence of import cycles between local packages.
func transitiveExternalImports(packages map[string]*packageImports) map[string]map[string]struct{} {
	reach := make(map[string]map[string]struct{}, len(packages))
	for dir, pkg := range packages {
		set := make(map[string]struct{}, len(pkg.directExternal))
		for importPath := range pkg.directExternal {
			set[importPath] = struct{}{}
		}
		reach[dir] = set
	}
	for changed := true; changed; {
		changed = false
		for dir, pkg := range packages {
			for neighbor := range pkg.localEdges {
				for importPath := range reach[neighbor] {
					if _, ok := reach[dir][importPath]; !ok {
						reach[dir][importPath] = struct{}{}
						changed = true
					}
				}
			}
		}
	}
	return reach
}

func firstLocalEdgeFile(pkg *packageImports) string {
	var best string
	for _, file := range pkg.localEdges {
		if best == "" || file < best {
			best = file
		}
	}
	return best
}

// mayImportHarnessOrCorePublic reports whether dir is one of the two
// package groups permitted to import Harness's or Core's public packages
// (never their internal/ packages — see classifyBoundaryViolation): the
// product-facing acp/agent facade itself, or internal/exampleagent, Task
// 6.1's test-only composition that wires a minimal in-memory host onto that
// same facade (see this file's package doc).
func mayImportHarnessOrCorePublic(dir string) bool {
	if dir == "agent" || strings.HasPrefix(dir, "agent/") {
		return true
	}
	return dir == "internal/exampleagent" || strings.HasPrefix(dir, "internal/exampleagent/")
}

func hasImportPrefix(importPath, root string) bool {
	return importPath == root || strings.HasPrefix(importPath, root+"/")
}

func sortedKeys(packages map[string]*packageImports) []string {
	keys := make([]string, 0, len(packages))
	for dir := range packages {
		keys = append(keys, dir)
	}
	sort.Strings(keys)
	return keys
}

func sortedSet(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
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
