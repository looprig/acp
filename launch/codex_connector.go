// codex_connector.go defines CodexConnector: the caller-facing type
// constructed via Codex and passed as Config.Harness to Dial. Its
// HarnessAdapter implementation (codex.go's Configure) is the ONLY wire
// this type has to a launched ACP session: unlike ClaudeConnector (see
// claude_connector.go), CodexConnector defines no method that accepts a
// *client.Session, calls session/set_config_option, or reaches for the
// unstable session/set_model extension -- current codex-acp adapters do
// not reliably support post-session model switching (see the design doc's
// Codex connector "Model selection"), so changing models always means
// constructing a new CodexConnector (see WithModel) and dialing an
// entirely new ACP session/process, never a wire call against an existing
// one. This is a deliberate, permanent omission, not a gap to fill in
// later: see codex_connector_test.go's method-set regression guard.
package launch

// CodexPosture is the caller-chosen sandbox/approval posture codex-acp is
// launched under: an application-level choice this package never
// hardcodes (see codex.go's Configure/codexConfigArgs).
type CodexPosture struct {
	// ApprovalPolicy is codex-acp's approval_policy `-c` override (e.g.
	// "untrusted", "on-failure", "on-request", "never"). Empty resolves
	// to defaultCodexApprovalPolicy.
	ApprovalPolicy string
	// SandboxMode is codex-acp's sandbox_mode `-c` override (e.g.
	// "read-only", "workspace-write", "danger-full-access"). Empty
	// resolves to defaultCodexSandboxMode.
	SandboxMode string
	// SandboxNetworkAccess is codex-acp's
	// sandbox_workspace_write.network_access `-c` override: required
	// when the child must reach loopback HTTP MCP servers or the
	// gateway from inside Codex's sandbox (see the design doc). Its zero
	// value (false, deny) is already the least-privilege default and is
	// never substituted for.
	SandboxNetworkAccess bool
}

const (
	// defaultCodexApprovalPolicy and defaultCodexSandboxMode are the
	// sane defaults CodexPosture.resolve substitutes for an empty
	// caller-supplied field -- a caller that only cares about the
	// model-provider override can leave Posture at its zero value and
	// still get a usable, working (if conservative) sandbox/approval
	// posture rather than an empty, invalid `-c` value.
	defaultCodexApprovalPolicy = "on-request"
	defaultCodexSandboxMode    = "workspace-write"
)

// resolve returns p with empty fields replaced by their documented sane
// defaults. It has a value receiver and returns a new value rather than
// mutating p in place, so calling it (as codex.go's Configure does, via
// c.Posture.resolve()) never changes the CodexConnector's own stored
// Posture field: defaults are applied fresh at every Configure call, never
// baked back into the connector.
func (p CodexPosture) resolve() CodexPosture {
	if p.ApprovalPolicy == "" {
		p.ApprovalPolicy = defaultCodexApprovalPolicy
	}
	if p.SandboxMode == "" {
		p.SandboxMode = defaultCodexSandboxMode
	}
	return p
}

// CodexConnector adapts a launched ACP session to codex-acp's specific
// conventions. Construct with Codex.
type CodexConnector struct {
	// Model is the harness-facing model alias embedded in the `-c
	// model=` override codex.go's Configure builds (see that file's
	// doc): codex-acp has no reliable post-session model-switching RPC,
	// so this value is fixed for this CodexConnector's entire lifetime.
	// Required by gateway Configure. Native no-proxy configuration may leave
	// it empty, in which case no model override is emitted.
	Model string
	// Effort is codex-acp's neutral reasoning-effort selector. It is applied
	// only by native launches, alongside Model, as a separate
	// model_reasoning_effort override. An empty value leaves effort selection
	// to Codex itself.
	Effort string
	// effortExplicit distinguishes the new paired selector API from the
	// legacy Codex(model) constructor, whose model-only native behavior is
	// retained for compatibility with existing callers.
	effortExplicit bool
	// Posture is the sandbox/approval posture Configure applies. The
	// zero value resolves to sane, least-privilege defaults (see
	// CodexPosture.resolve).
	Posture CodexPosture
}

// Codex constructs a CodexConnector for model, the harness-facing model
// alias codex-acp is launched with via the `-c model=` override. Posture
// may be set on the returned value before passing it to Dial as
// Config.Harness.
func Codex(model string) *CodexConnector {
	return &CodexConnector{Model: model}
}

// WithModel returns a new *CodexConnector identical to c except for its
// Model field (Posture is copied unchanged), for the caller to Dial as an
// entirely new ACP session/process when a different model is wanted --
// see this file's own doc: codex-acp's model is fixed at spawn time via
// an argv override, so "switching models" is always "launch a new
// codex-acp", never a session/set_config_option or session/set_model call
// against the existing one. c itself is never mutated, and the two
// resulting connectors never alias any state (CodexPosture has no pointer
// or slice fields, so the struct copy below is a fully independent
// value).
func (c *CodexConnector) WithModel(model string) *CodexConnector {
	clone := *c
	clone.Model = model
	return &clone
}

// WithModelEffort returns a new *CodexConnector identical to c except for its
// native model and reasoning-effort selectors. c itself is never mutated. A
// model and effort must either both be non-empty or both be empty when the
// resulting connector is configured for a native launch; ConfigureNative
// reports a typed ConfigError for a partial pair.
func (c *CodexConnector) WithModelEffort(model, effort string) *CodexConnector {
	clone := *c
	clone.Model = model
	clone.Effort = effort
	clone.effortExplicit = true
	return &clone
}

// Compile-time proof that CodexConnector actually satisfies HarnessAdapter
// (see codex.go's Configure).
var (
	_ HarnessAdapter       = (*CodexConnector)(nil)
	_ NativeHarnessAdapter = (*CodexConnector)(nil)
)
