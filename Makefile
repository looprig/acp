.PHONY: build test boundary fmt fmt-check root-check staticcheck lint vuln secure fuzz

GO ?= go

# Module's own package dirs (go list ./... stops at nested module boundaries).
# GO_DIRS scopes gosec, which takes package dirs. Never hand GO_DIRS to gofmt:
# gofmt recurses into directory operands, and for a module with a root package
# GO_DIRS contains the module root, so gofmt would walk the entire tree —
# including the nested .worktrees/ checkouts, which are separate modules. Use
# GO_FILES for gofmt: it expands to each package dir's own .go files (including
# platform-specific ones go list omits for the host) without descending.
GO_DIRS = $(shell go list -f '{{.Dir}}' ./...)
GO_FILES = $(foreach dir,$(GO_DIRS),$(wildcard $(dir)/*.go))

# This module does not vendor. go.mod pins exact versions and go.sum verifies
# their content hashes, which is what makes a build reproducible; a vendor tree
# adds only offline builds and source-level dependency diffs. It also actively
# misleads: a stale vendor/ is ignored under a go.work but silently satisfies a
# GOWORK=off build, so standalone verification tests the vendored copy rather
# than the version go.mod actually pins — which is precisely what standalone
# verification exists to check.

build: boundary
	CGO_ENABLED=0 go build -trimpath ./...

test: boundary
	go test -race ./...

# internal/boundary walks the whole module's import graph (see its deps_test.go),
# so it automatically covers acp/protocol, acp/transport/stdio, acp/client, and
# acp/agent the moment those packages exist; nothing here needs updating when
# they land.
boundary: root-check
	go test ./internal/boundary -run 'Dependencies|Boundaries|Public|Scan'

fmt:
	gofmt -w $(GO_FILES)

fmt-check:
	@unformatted=$$(gofmt -l $(GO_FILES)); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed (run 'make fmt'):"; echo "$$unformatted"; exit 1; \
	fi

root-check:
	@root_go_files=$$(find . -maxdepth 1 \( -type f -o -type l \) -name '*.go' -print); \
	if [ -n "$$root_go_files" ]; then \
		echo "forbidden Go files in repository root:"; echo "$$root_go_files"; exit 1; \
	fi

lint: boundary fmt-check
	go vet ./...
	$(MAKE) staticcheck
	go tool gosec $(GO_DIRS)

staticcheck:
	@GO="$(GO)" ./scripts/run-staticcheck.sh

vuln:
	go mod verify
	go tool govulncheck ./...

secure: lint vuln

fuzz:
	@echo "Usage: go test -fuzz=FuzzXxx ./path/to/pkg -fuzztime=30s"
	@echo "Current targets: FuzzEnvelope (./protocol), FuzzFrameReader (./protocol)"
