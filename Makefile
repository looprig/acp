.PHONY: build test boundary fmt fmt-check root-check vendor vendor-scrub vendor-check staticcheck lint vuln secure fuzz

GO ?= go

# Module package directories only. go list skips vendor and nested modules, so
# gofmt and gosec do not descend into copied dependencies or worktrees.
GO_DIRS = $(shell go list -f '{{.Dir}}' ./...)

VENDOR_DIR ?= vendor
LOCAL_REPLACE_VENDOR_DIRS := $(VENDOR_DIR)/github.com/looprig/harness $(VENDOR_DIR)/github.com/looprig/core

# Bootstrap without vendor, then force every build/check onto the auditable tree
# as soon as go mod vendor has produced it. This also overrides global GOFLAGS.
ifneq ($(wildcard $(VENDOR_DIR)/modules.txt),)
export GOFLAGS := -mod=vendor
endif

build: boundary
	CGO_ENABLED=0 go build -trimpath ./...

test: boundary
	go test -race ./...

# internal/boundary walks the whole module's import graph (see its deps_test.go),
# so it automatically covers acp/protocol, acp/transport/stdio, acp/client, and
# acp/agent the moment those packages exist; nothing here needs updating when
# they land.
boundary: root-check vendor-check
	go test ./internal/boundary -run 'Dependencies|Boundaries|Public|Scan'

fmt:
	gofmt -w $(GO_DIRS)

fmt-check:
	@unformatted=$$(gofmt -l $(GO_DIRS)); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed (run 'make fmt'):"; echo "$$unformatted"; exit 1; \
	fi

root-check:
	@root_go_files=$$(find . -maxdepth 1 \( -type f -o -type l \) -name '*.go' -print); \
	if [ -n "$$root_go_files" ]; then \
		echo "forbidden Go files in repository root:"; echo "$$root_go_files"; exit 1; \
	fi

vendor:
	go mod vendor
	$(MAKE) vendor-scrub
	$(MAKE) vendor-check

vendor-scrub:
	rm -rf $(addsuffix /.git,$(LOCAL_REPLACE_VENDOR_DIRS))

vendor-check:
	@test -f "$(VENDOR_DIR)/modules.txt" || { \
		echo "missing $(VENDOR_DIR)/modules.txt (run 'make vendor')"; exit 1; \
	}
	@metadata=$$(find "$(VENDOR_DIR)" -name .git -print); \
	if [ -n "$$metadata" ]; then \
		echo "forbidden VCS metadata in $(VENDOR_DIR):"; echo "$$metadata"; exit 1; \
	fi

lint: boundary fmt-check
	go vet ./...
	$(MAKE) staticcheck
	go tool gosec $(GO_DIRS)

staticcheck:
	@GO="$(GO)" ./scripts/run-staticcheck.sh

vuln: vendor-check
	go mod verify
	go tool govulncheck ./...

secure: lint vuln

fuzz:
	@echo "Usage: go test -fuzz=FuzzXxx ./path/to/pkg -fuzztime=30s"
	@echo "Current targets: FuzzEnvelope (./protocol), FuzzFrameReader (./protocol)"
