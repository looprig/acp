// capabilities.go computes the AgentCapabilities advertised in the
// initialize response from an Agent's Options: the capability advertisement
// matrix described in Task 2.2 of
// harness/docs/plans/2026-07-23-acp-bridge-implementation.md.
package agent

import "github.com/looprig/acp/protocol"

// capabilities computes the AgentCapabilities to advertise in the initialize
// response from the currently configured Options. Each optional capability
// interface from host.go maps onto exactly the wire field the pinned schema
// defines for it, present only when the corresponding Options field is
// non-nil:
//
//   - EventReplayer   -> AgentCapabilities.LoadSession
//   - SessionCatalog  -> AgentCapabilities.SessionCapabilities.List
//   - SessionDeleter  -> AgentCapabilities.SessionCapabilities.Delete
//   - LogoutHandler   -> AgentCapabilities.Auth.Logout
//
// Authenticator has no field here: per the pinned schema, authentication
// support is signaled by a non-empty InitializeResponse.AuthMethods list
// (see handleInitialize), not a capability struct.
//
// RuntimeConfigCatalog, RuntimeConfigController, and Compactor have no
// initialize-level wire representation at all in the pinned schema: config
// options are surfaced per-session in the session/new response (Task 4.1)
// and `/compact` is advertised as a session-level available command (Task
// 4.2). Supplying or omitting them therefore has no effect on this response.
func (a *Agent) capabilities() protocol.AgentCapabilities {
	caps := protocol.AgentCapabilities{
		LoadSession: a.opts.Replayer != nil,
	}

	if a.opts.Logout != nil {
		caps.Auth = &protocol.AgentAuthCapabilities{
			Logout: &protocol.LogoutCapabilities{},
		}
	}

	var session protocol.SessionCapabilities
	haveSessionCapabilities := false
	if a.opts.Catalog != nil {
		session.List = &protocol.SessionListCapabilities{}
		haveSessionCapabilities = true
	}
	if a.opts.Deleter != nil {
		session.Delete = &protocol.SessionDeleteCapabilities{}
		haveSessionCapabilities = true
	}
	if haveSessionCapabilities {
		caps.SessionCapabilities = &session
	}

	return caps
}
