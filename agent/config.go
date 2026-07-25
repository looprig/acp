// config.go implements the session/set_config_option and session/set_mode
// handlers: Task 4.1 of harness/docs/plans/2026-07-23-acp-bridge-implementation.md,
// the first task of Phase 4.
//
// Both wire methods run through the single unexported applyConfigOption:
// handleSessionSetConfigOption calls it with the request's own configId/
// value, and handleSessionSetMode calls it with the well-known
// ModeConfigOptionID (host.go) and the requested mode id reinterpreted as a
// SessionConfigValueID. There is no second, independent "apply a mode
// change" implementation anywhere in this package — this is what keeps the
// two wire methods convergent rather than two behaviors a future change
// could accidentally let drift apart.
//
// applyConfigOption itself:
//
//  1. Fetches the LATEST RuntimeConfigCatalog snapshot for this request —
//     never one cached from session/new, an earlier request, or anywhere
//     else. All external input (configId/value) is untrusted, so it is
//     validated against what is true right now.
//  2. Rejects an unknown configId or an unknown value for that configId
//     (InvalidParams), fail closed, before the RuntimeConfigController is
//     ever consulted.
//  3. Short-circuits to a no-op success when the requested value already
//     equals the option's current value: config writes are idempotent (see
//     RuntimeConfigController's doc in host.go), and this facade enforces
//     that itself rather than trusting every controller implementation to
//     — the controller is not called, and no notification is sent.
//  4. Otherwise calls RuntimeConfigController.SetRuntimeConfigOption, then
//     sends exactly one config_option_update session/update notification
//     carrying the complete resulting option state, before returning that
//     same state to the caller.
//
// initialConfigState is the second entry point into this file's
// translation logic: session.go's handleSessionNew, replay.go's
// handleSessionLoad, and resume.go's handleSessionResume all call it to
// populate their response's ConfigOptions/Modes fields with the
// newly-established session's current RuntimeConfigCatalog snapshot, reusing
// translateRuntimeConfigOptions (the exact same helper applyConfigOption's
// own response uses) rather than a second, independent translation.
package agent

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/looprig/acp/protocol"
)

// ErrUnknownConfigOption is the local cause applyConfigOption attaches to the
// InvalidParams fault it returns when optionID names no option in the latest
// RuntimeConfigCatalog snapshot. It never crosses the wire itself (only
// Message/Code/Data do — see protocol.Fault) but lets a caller use errors.Is
// to distinguish this SPECIFIC condition from any other InvalidParams fault.
// handleSessionSetMode uses it this way: ModeConfigOptionID (host.go) is a
// package constant, never client-supplied, so if applyConfigOption ever
// fails with ErrUnknownConfigOption on that call path, the only possible
// cause is a misconfigured RuntimeConfigCatalog that omits the well-known
// "mode" option entirely — a host bug, not a client mistake — and
// handleSessionSetMode reports that as a distinct, louder diagnostic instead
// of silently reusing the ordinary "unknown configId" a real client would
// get for an arbitrary bad session/set_config_option request.
var ErrUnknownConfigOption = errors.New("agent: config option: unknown configId")

// handleSessionSetConfigOption answers the session/set_config_option method.
// It is only ever registered when both Options.ConfigCatalog and
// Options.ConfigController are non-nil (see Register): validating a change
// needs a live catalog, and applying one needs a controller, so a host that
// supplies only one of the two is treated the same as supplying neither.
func (a *Agent) handleSessionSetConfigOption(ctx context.Context, _ string, params json.RawMessage) (any, error) {
	var req protocol.SetSessionConfigOptionRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("session/set_config_option: decode params", err)
	}

	sessionID, err := ParseSessionID(req.SessionID)
	if err != nil {
		return nil, protocol.InvalidParams("sessionId: "+err.Error(), err)
	}

	valueID, err := requestedConfigValueID(req)
	if err != nil {
		return nil, err
	}

	options, err := a.applyConfigOption(ctx, sessionID, req.ConfigID, valueID)
	if err != nil {
		return nil, err
	}
	return protocol.SetSessionConfigOptionResponse{ConfigOptions: options}, nil
}

// requestedConfigValueID extracts the requested value from a
// SetSessionConfigOptionRequest's discriminated union. Only the ValueID
// (select) variant is supported: no RuntimeConfigOption this facade builds
// ever uses the wire's "boolean" variant (see RuntimeConfigOption's doc,
// host.go), so a Boolean-valued request can never correspond to any option
// this facade could validate. It fails closed rather than silently coercing
// the boolean into a string.
func requestedConfigValueID(req protocol.SetSessionConfigOptionRequest) (protocol.SessionConfigValueID, error) {
	if req.Boolean != nil {
		return "", protocol.InvalidParams("session/set_config_option: boolean-valued options are not supported", nil)
	}
	if req.ValueID == nil {
		return "", protocol.InvalidParams("session/set_config_option: missing value", nil)
	}
	return *req.ValueID, nil
}

// handleSessionSetMode answers the session/set_mode method. It is only ever
// registered alongside handleSessionSetConfigOption (see Register), under
// the identical Options.ConfigCatalog/Options.ConfigController gate.
//
// The pinned schema's SetSessionModeRequest carries only a bare
// SessionModeID — no configId — so this handler always targets
// ModeConfigOptionID (host.go), translating the request into exactly the
// call handleSessionSetConfigOption would make for
// configId=ModeConfigOptionID. See this file's package doc for why that is
// what keeps the two methods convergent.
func (a *Agent) handleSessionSetMode(ctx context.Context, _ string, params json.RawMessage) (any, error) {
	var req protocol.SetSessionModeRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, protocol.InvalidParams("session/set_mode: decode params", err)
	}

	sessionID, err := ParseSessionID(req.SessionID)
	if err != nil {
		return nil, protocol.InvalidParams("sessionId: "+err.Error(), err)
	}

	if _, err := a.applyConfigOption(ctx, sessionID, ModeConfigOptionID, protocol.SessionConfigValueID(req.ModeID)); err != nil {
		if errors.Is(err, ErrUnknownConfigOption) {
			// ModeConfigOptionID is a package constant, never client input:
			// this can only mean the configured RuntimeConfigCatalog itself
			// does not honor the documented mode<->"mode" convergence
			// contract (see ErrUnknownConfigOption's doc and host.go's
			// ModeConfigOptionID doc) — a host misconfiguration, not a
			// client mistake, so it is reported distinctly rather than
			// reusing applyConfigOption's ordinary "unknown configId"
			// InvalidParams a bad session/set_config_option request gets.
			return nil, protocol.InternalError(
				`session/set_mode: host misconfiguration: RuntimeConfigCatalog does not offer the well-known "mode" config option (see ModeConfigOptionID)`,
				err,
			)
		}
		return nil, err
	}
	return protocol.SetSessionModeResponse{}, nil
}

// applyConfigOption is the single shared apply path session/set_config_option
// and session/set_mode both run through — see this file's package doc for
// the full step-by-step contract.
func (a *Agent) applyConfigOption(ctx context.Context, sessionID SessionID, optionID protocol.SessionConfigID, valueID protocol.SessionConfigValueID) ([]protocol.SessionConfigOption, error) {
	latest, err := a.opts.ConfigCatalog.RuntimeConfigOptions(ctx, sessionID)
	if err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("config option: fetch latest catalog: "+err.Error(), err)
	}

	option, ok := findRuntimeConfigOption(latest, optionID)
	if !ok {
		return nil, protocol.InvalidParams("config option: unknown configId", ErrUnknownConfigOption)
	}
	if !runtimeConfigOptionHasValue(option, valueID) {
		return nil, protocol.InvalidParams("config option: unknown value for this configId", nil)
	}

	if option.CurrentValue == valueID {
		// Idempotent no-op: the controller is never consulted and no
		// notification is sent (see RuntimeConfigController's doc, host.go).
		return translateRuntimeConfigOptions(latest), nil
	}

	updated, err := a.opts.ConfigController.SetRuntimeConfigOption(ctx, sessionID, RuntimeConfigChange{OptionID: optionID, ValueID: valueID})
	if err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, f
		}
		return nil, protocol.InternalError("config option: apply change: "+err.Error(), err)
	}

	translated := translateRuntimeConfigOptions(updated)
	notification := protocol.SessionNotification{
		SessionID: protocol.SessionID(sessionID.String()),
		Update: protocol.SessionUpdate{
			ConfigOptionUpdate: &protocol.ConfigOptionUpdate{ConfigOptions: translated},
		},
	}
	if err := a.client.SessionUpdate(ctx, notification); err != nil {
		return nil, protocol.InternalError("config option: session/update: "+err.Error(), err)
	}

	return translated, nil
}

// initialConfigState builds the ConfigOptions/Modes payload shared by the
// session/new, session/load, and session/resume responses (types_gen.go's
// NewSessionResponse/LoadSessionResponse/ResumeSessionResponse all carry the
// identical pair of fields, documented as "Initial session configuration
// options/mode state if supported by the Agent"). It is a no-op (nil, nil,
// nil) when Options.ConfigCatalog is not configured: exactly like
// session/set_config_option and session/set_mode being unregistered
// entirely in that case, no capability means nothing to report, never an
// error.
//
// When a catalog IS configured, this fetches the latest RuntimeConfigOptions
// snapshot for sessionID — the same "always fresh, never cached" rule
// applyConfigOption follows — and translates it through
// translateRuntimeConfigOptions, the exact same helper applyConfigOption's
// own response uses, so a session's config surface is represented identically
// whether learned at establishment time or after a later
// session/set_config_option call. If the catalog's well-known
// ModeConfigOptionID entry (host.go) is present, its values are additionally
// projected onto the pinned schema's separate SessionModeState shape
// (protocol.SessionMode/protocol.SessionModeState); if it is absent, Modes is
// left nil rather than a zero-value SessionModeState — a legitimate catalog
// shape for a host that offers other options (model, thought level, ...) but
// no mode support at all.
func (a *Agent) initialConfigState(ctx context.Context, sessionID SessionID) ([]protocol.SessionConfigOption, *protocol.SessionModeState, error) {
	if a.opts.ConfigCatalog == nil {
		return nil, nil, nil
	}

	latest, err := a.opts.ConfigCatalog.RuntimeConfigOptions(ctx, sessionID)
	if err != nil {
		var f *protocol.Fault
		if errors.As(err, &f) {
			return nil, nil, f
		}
		return nil, nil, protocol.InternalError("config option: fetch initial catalog: "+err.Error(), err)
	}

	options := translateRuntimeConfigOptions(latest)

	var modes *protocol.SessionModeState
	if modeOption, ok := findRuntimeConfigOption(latest, ModeConfigOptionID); ok {
		modes = buildSessionModeState(modeOption)
	}

	return options, modes, nil
}

// buildSessionModeState projects one RuntimeConfigOption (assumed to be the
// well-known ModeConfigOptionID entry — see initialConfigState's doc) onto
// the pinned schema's SessionModeState: each RuntimeConfigValue becomes a
// protocol.SessionMode (ID/Name/optional Description, field-for-field —
// RuntimeConfigValue's own doc, host.go, says this mirroring is exactly why
// it exists), and CurrentValue becomes CurrentModeID.
func buildSessionModeState(modeOption RuntimeConfigOption) *protocol.SessionModeState {
	modes := make([]protocol.SessionMode, 0, len(modeOption.Values))
	for _, v := range modeOption.Values {
		m := protocol.SessionMode{ID: protocol.SessionModeID(v.ID), Name: v.Name}
		if v.Description != "" {
			d := v.Description
			m.Description = &d
		}
		modes = append(modes, m)
	}
	return &protocol.SessionModeState{
		AvailableModes: modes,
		CurrentModeID:  protocol.SessionModeID(modeOption.CurrentValue),
	}
}

// findRuntimeConfigOption looks up id in options by its ID field.
func findRuntimeConfigOption(options []RuntimeConfigOption, id protocol.SessionConfigID) (RuntimeConfigOption, bool) {
	for _, o := range options {
		if o.ID == id {
			return o, true
		}
	}
	return RuntimeConfigOption{}, false
}

// runtimeConfigOptionHasValue reports whether valueID names one of option's
// currently offered values.
func runtimeConfigOptionHasValue(option RuntimeConfigOption, valueID protocol.SessionConfigValueID) bool {
	for _, v := range option.Values {
		if v.ID == valueID {
			return true
		}
	}
	return false
}

// translateRuntimeConfigOptions projects a []RuntimeConfigOption (host.go)
// onto the wire's []protocol.SessionConfigOption.
func translateRuntimeConfigOptions(options []RuntimeConfigOption) []protocol.SessionConfigOption {
	out := make([]protocol.SessionConfigOption, 0, len(options))
	for _, o := range options {
		out = append(out, translateRuntimeConfigOption(o))
	}
	return out
}

// translateRuntimeConfigOption projects one RuntimeConfigOption onto the
// wire's SessionConfigOption, always as the "select" (dropdown) variant: see
// RuntimeConfigOption's doc (host.go) for why this facade never produces a
// "boolean" option.
func translateRuntimeConfigOption(o RuntimeConfigOption) protocol.SessionConfigOption {
	values := make([]protocol.SessionConfigSelectOption, 0, len(o.Values))
	for _, v := range o.Values {
		sv := protocol.SessionConfigSelectOption{Name: v.Name, Value: v.ID}
		if v.Description != "" {
			d := v.Description
			sv.Description = &d
		}
		values = append(values, sv)
	}

	opt := protocol.SessionConfigOption{
		ID:   o.ID,
		Name: o.Name,
		Select: &protocol.SessionConfigSelect{
			CurrentValue: o.CurrentValue,
			Options:      protocol.SessionConfigSelectOptions{Ungrouped: values},
		},
	}
	if o.Category != "" {
		cat := o.Category
		opt.Category = &cat
	}
	if o.Description != "" {
		d := o.Description
		opt.Description = &d
	}
	return opt
}
