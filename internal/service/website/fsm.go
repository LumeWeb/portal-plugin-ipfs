package website

import (
	"context"

	"github.com/looplab/fsm"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// Events that drive a website's lifecycle state machine. These are the only
// ways a Website may move between statuses; every status mutation in the code
// base should be routed through WebsiteStateMachine so illegal transitions are
// rejected instead of being silently written to the database.
const (
	// EventWebsiteCreate initializes a freshly created website to
	// pending_validation, overriding whatever status the caller may have
	// pre-set (a new website must pass DNS validation before it can be served).
	// NewWebsiteStateMachine keeps the empty state as-is so this is a real
	// transition; firing from an already-pending website is a guarded no-op.
	EventWebsiteCreate = "create"
	// EventWebsiteValidate activates a website whose DNS validation succeeded.
	// It is legal from pending_validation (first validation) and broken
	// (re-validation recovers a broken site). An already-active website needs
	// no transition (guard with Can()).
	EventWebsiteValidate = "validate"
	// EventWebsiteRevalidateOK is fired when the janitor confirms the target is
	// still pinned, recovering a broken website back to active. A website that
	// is already active and valid needs no transition (guard with Can()).
	EventWebsiteRevalidateOK = "revalidate_ok"
	// EventWebsiteCIDUnpinned is fired when an active website's target is no
	// longer pinned, flipping it to broken.
	EventWebsiteCIDUnpinned = "cid_unpinned"
	// EventWebsiteTargetChanged resets a website to pending_validation because
	// its target hash changed and must be re-validated. Only meaningful for
	// websites that were active/broken; an already-pending website is a no-op
	// (guard with Can()).
	EventWebsiteTargetChanged = "target_changed"
	// EventWebsiteBlock is an admin action that blocks serving the website.
	EventWebsiteBlock = "block"
	// EventWebsiteUnblock lifts an admin block, returning the website to
	// pending_validation so it must pass validation before being served again.
	EventWebsiteUnblock = "unblock"
)

// websiteStateMachineEvents is the authoritative transition table for website
// lifecycle statuses. Keep this in sync with the WebsiteStatus enum in
// internal/db/website.go.
// websiteStateMachineEvents is the authoritative transition table for website
// lifecycle statuses. Keep this in sync with the WebsiteStatus enum in
// internal/db/website.go.
//
// Note: looplab/fsm v1.0.3 treats a self-transition (Src == Dst) as an error
// (NoTransitionError). This table therefore contains only *real* transitions.
// Call sites that want a no-op "stay in the same state" outcome (e.g. the
// janitor confirming an already-active website, or forcing an already-pending
// website back to pending) must guard with Can() rather than firing the event.
var websiteStateMachineEvents = fsm.Events{
	// Fresh website (empty status) or one pre-set by the caller (e.g. active in
	// an API/test fixture) → pending_validation. A brand-new website must be
	// validated; CreateWebsite never inherits a prior/pre-set status. Firing
	// from an already-pending website is a self-transition and is guarded with
	// Can() by the caller, so it is omitted from the legal sources.
	{Name: EventWebsiteCreate, Src: []string{"", string(pluginDb.WebsiteStatusActive), string(pluginDb.WebsiteStatusBroken)}, Dst: string(pluginDb.WebsiteStatusPendingValidation)},
	// DNS validation succeeded (first validation or recovery of a broken site).
	{Name: EventWebsiteValidate, Src: []string{string(pluginDb.WebsiteStatusPendingValidation), string(pluginDb.WebsiteStatusBroken)}, Dst: string(pluginDb.WebsiteStatusActive)},
	// Janitor liveness check passed; recovers broken back to active.
	{Name: EventWebsiteRevalidateOK, Src: []string{string(pluginDb.WebsiteStatusBroken)}, Dst: string(pluginDb.WebsiteStatusActive)},
	// Janitor liveness check failed for an active website.
	{Name: EventWebsiteCIDUnpinned, Src: []string{string(pluginDb.WebsiteStatusActive)}, Dst: string(pluginDb.WebsiteStatusBroken)},
	// Target hash change invalidates a previously active/broken website.
	{Name: EventWebsiteTargetChanged, Src: []string{string(pluginDb.WebsiteStatusActive), string(pluginDb.WebsiteStatusBroken)}, Dst: string(pluginDb.WebsiteStatusPendingValidation)},
	// Admin blocking; not valid from an already-blocked website.
	{Name: EventWebsiteBlock, Src: []string{string(pluginDb.WebsiteStatusPendingValidation), string(pluginDb.WebsiteStatusActive), string(pluginDb.WebsiteStatusBroken)}, Dst: string(pluginDb.WebsiteStatusBlocked)},
	// Admin unblock returns the website to pending_validation.
	{Name: EventWebsiteUnblock, Src: []string{string(pluginDb.WebsiteStatusBlocked)}, Dst: string(pluginDb.WebsiteStatusPendingValidation)},
}

// WebsiteStateMachine wraps a looplab/fsm state machine bound to a single
// Website. It is constructed from the website's current status, fired to
// perform a legal transition (which updates the Website.Status field in place
// via the after_event callback), and the caller persists the change within the
// surrounding transaction.
type WebsiteStateMachine struct {
	fsm     *fsm.FSM
	website *pluginDb.Website
}

// NewWebsiteStateMachine builds a state machine initialized to the given
// website's current status. An empty status is kept as-is: it represents the
// pre-creation state from which the create event fires. This keeps create a
// real (non-self) transition under looplab/fsm.
func NewWebsiteStateMachine(website *pluginDb.Website) *WebsiteStateMachine {
	initial := website.Status

	m := &WebsiteStateMachine{
		website: website,
		fsm:     fsm.NewFSM(initial, websiteStateMachineEvents, fsm.Callbacks{
			// Persist the destination status back onto the website so callers
			// don't have to derive it from the event themselves.
			"after_event": func(_ context.Context, e *fsm.Event) {
				website.Status = e.Dst
			},
		}),
	}

	return m
}

// Fire performs a transition if it is legal for the website's current state.
// On success it mutates website.Status to the destination state. It returns an
// error (fsm.ErrNotInSource) when the transition is not allowed.
func (w *WebsiteStateMachine) Fire(ctx context.Context, event string) error {
	return w.fsm.Event(ctx, event)
}

// Can reports whether the given event is a legal transition from the website's
// current state. Use it to guard transitions that are only meaningful from a
// subset of states (e.g. a janitor marking an already-broken website broken).
func (w *WebsiteStateMachine) Can(event string) bool {
	return w.fsm.Can(event)
}

// Current returns the website's current status.
func (w *WebsiteStateMachine) Current() pluginDb.WebsiteStatus {
	return pluginDb.WebsiteStatus(w.fsm.Current())
}
