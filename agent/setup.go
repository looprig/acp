package agent

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/looprig/acp/protocol"
)

// Setup is the validated, ACP-facing negotiated setup data a SessionHost
// needs to create, load, or resume a session. It carries only negotiated
// setup values — never Harness rig options or other product configuration
// objects (see design doc "Agent-side host boundary"). Construct it with
// NewSetup; the zero value is not validated.
type Setup struct {
	// Cwd is the canonical absolute workspace root.
	Cwd string
	// ClientCapabilities is the negotiated client capability set, with every
	// schema-declared default applied to any subfield the client did not
	// advertise.
	ClientCapabilities protocol.ClientCapabilities
	// MCPServers is the set of MCP server descriptors requested for this
	// session. It is non-empty only when the host explicitly accepted MCP
	// setup (see NewSetup's acceptMCP parameter).
	MCPServers []protocol.McpServer
}

// CwdErrorReason classifies why a candidate cwd was rejected.
type CwdErrorReason string

const (
	// CwdReasonEmpty: cwd was the empty string.
	CwdReasonEmpty CwdErrorReason = "empty"
	// CwdReasonNotAbsolute: cwd was not an absolute path.
	CwdReasonNotAbsolute CwdErrorReason = "not_absolute"
	// CwdReasonTraversal: cwd contained a ".." path segment.
	CwdReasonTraversal CwdErrorReason = "traversal"
	// CwdReasonNotCanonical: cwd was absolute and traversal-free but not
	// already in filepath.Clean canonical form (e.g. a doubled separator, a
	// "." segment, or a trailing separator).
	CwdReasonNotCanonical CwdErrorReason = "not_canonical"
)

// CwdError reports that a candidate cwd failed canonical-absolute-path
// validation. All external input is untrusted, so NewSetup fails closed
// rather than silently canonicalizing a malformed path on the caller's
// behalf.
type CwdError struct {
	Cwd    string
	Reason CwdErrorReason
}

func (e *CwdError) Error() string {
	return "agent: invalid cwd " + strconv.Quote(e.Cwd) + ": " + string(e.Reason)
}

// MCPNotAcceptedError reports that Setup construction was asked to carry one
// or more MCP server descriptors, but the host has not advertised acceptance
// of MCP setup. ACP setup fails closed rather than silently dropping the
// requested servers (see design doc "MCP and external capabilities").
type MCPNotAcceptedError struct {
	Count int
}

func (e *MCPNotAcceptedError) Error() string {
	return "agent: " + strconv.Itoa(e.Count) + " MCP server descriptor(s) requested but host does not accept MCP setup"
}

// NewSetup validates cwd, defaults capabilities, and enforces MCP
// acceptance, returning a Setup ready to hand to a SessionHost.
//
// cwd must canonicalize to a clean absolute path (reject empty, relative,
// ".."-bearing, or otherwise non-canonical values); see CwdErrorReason for
// the exact failure classification.
//
// capabilities may be nil, in which case every schema-declared default
// applies wholesale (protocol.DefaultClientCapabilities). When non-nil, only
// the subfields the caller left unset (nil pointers) are filled from their
// own generated defaults; explicitly supplied values are preserved untouched.
//
// mcpServers is rejected with a *MCPNotAcceptedError unless acceptMCP is
// true, matching the host's advertised MCP-acceptance capability. An empty
// mcpServers is never rejected.
func NewSetup(cwd string, capabilities *protocol.ClientCapabilities, mcpServers []protocol.McpServer, acceptMCP bool) (Setup, error) {
	canonical, err := validateCwd(cwd)
	if err != nil {
		return Setup{}, err
	}
	if len(mcpServers) > 0 && !acceptMCP {
		return Setup{}, &MCPNotAcceptedError{Count: len(mcpServers)}
	}
	return Setup{
		Cwd:                canonical,
		ClientCapabilities: defaultedClientCapabilities(capabilities),
		MCPServers:         mcpServers,
	}, nil
}

// validateCwd canonicalizes and validates cwd, returning it unchanged when it
// is already a clean absolute path free of ".." segments.
func validateCwd(cwd string) (string, error) {
	if cwd == "" {
		return "", &CwdError{Cwd: cwd, Reason: CwdReasonEmpty}
	}
	if !filepath.IsAbs(cwd) {
		return "", &CwdError{Cwd: cwd, Reason: CwdReasonNotAbsolute}
	}
	if containsDotDotSegment(cwd) {
		return "", &CwdError{Cwd: cwd, Reason: CwdReasonTraversal}
	}
	if cleaned := filepath.Clean(cwd); cleaned != cwd {
		return "", &CwdError{Cwd: cwd, Reason: CwdReasonNotCanonical}
	}
	return cwd, nil
}

// containsDotDotSegment reports whether p has a literal ".." path segment.
func containsDotDotSegment(p string) bool {
	for _, segment := range strings.Split(p, "/") {
		if segment == ".." {
			return true
		}
	}
	return false
}

// defaultedClientCapabilities fills schema-declared defaults for every
// subfield the caller left unset, preserving explicitly supplied values.
func defaultedClientCapabilities(capabilities *protocol.ClientCapabilities) protocol.ClientCapabilities {
	if capabilities == nil {
		return protocol.DefaultClientCapabilities()
	}
	result := *capabilities
	if result.Fs == nil {
		fs := protocol.DefaultFileSystemCapabilities()
		result.Fs = &fs
	}
	return result
}
