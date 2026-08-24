package website

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// emptyState represents the pre-creation status ("" in the DB).
const emptyState = pluginDb.WebsiteStatus("")

// legalTransitions is the expectation matrix for the website state machine:
// event → source statuses that may legally fire it, and the resulting status.
// create is only legal from the empty pre-creation state (initialEmpty).
var legalTransitions = []struct {
	name         string
	event        string
	sources      []pluginDb.WebsiteStatus
	dest         pluginDb.WebsiteStatus
	initialEmpty bool // allow firing from the empty (pre-creation) status
}{
	{name: "create", event: EventWebsiteCreate, sources: []pluginDb.WebsiteStatus{pluginDb.WebsiteStatusActive, pluginDb.WebsiteStatusBroken}, dest: pluginDb.WebsiteStatusPendingValidation, initialEmpty: true},
	{name: "validate", event: EventWebsiteValidate, sources: []pluginDb.WebsiteStatus{pluginDb.WebsiteStatusPendingValidation, pluginDb.WebsiteStatusBroken}, dest: pluginDb.WebsiteStatusActive},
	{name: "revalidate_ok", event: EventWebsiteRevalidateOK, sources: []pluginDb.WebsiteStatus{pluginDb.WebsiteStatusBroken}, dest: pluginDb.WebsiteStatusActive},
	{name: "cid_unpinned", event: EventWebsiteCIDUnpinned, sources: []pluginDb.WebsiteStatus{pluginDb.WebsiteStatusActive}, dest: pluginDb.WebsiteStatusBroken},
	{name: "target_changed", event: EventWebsiteTargetChanged, sources: []pluginDb.WebsiteStatus{pluginDb.WebsiteStatusActive, pluginDb.WebsiteStatusBroken}, dest: pluginDb.WebsiteStatusPendingValidation},
	{name: "block", event: EventWebsiteBlock, sources: []pluginDb.WebsiteStatus{pluginDb.WebsiteStatusPendingValidation, pluginDb.WebsiteStatusActive, pluginDb.WebsiteStatusBroken}, dest: pluginDb.WebsiteStatusBlocked},
	{name: "unblock", event: EventWebsiteUnblock, sources: []pluginDb.WebsiteStatus{pluginDb.WebsiteStatusBlocked}, dest: pluginDb.WebsiteStatusPendingValidation},
}

// allStatuses lists every known status, used to assert illegal transitions
// from every non-source state. The empty pre-creation state is included so the
// create event is asserted illegal from any real status.
var allStatuses = []pluginDb.WebsiteStatus{
	emptyState,
	pluginDb.WebsiteStatusPendingValidation,
	pluginDb.WebsiteStatusActive,
	pluginDb.WebsiteStatusBroken,
	pluginDb.WebsiteStatusBlocked,
}

func containsStatus(list []pluginDb.WebsiteStatus, s pluginDb.WebsiteStatus) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestWebsiteStateMachine_LegalTransitions(t *testing.T) {
	for _, tc := range legalTransitions {
		for _, src := range tc.sources {
			t.Run(tc.name+"_from_"+string(src), func(t *testing.T) {
				website := &pluginDb.Website{Status: string(src)}
				sm := NewWebsiteStateMachine(website)

				assert.True(t, sm.Can(tc.event), "expected %s to be legal from %s", tc.event, src)
				err := sm.Fire(context.Background(), tc.event)
				assert.NoError(t, err, "expected %s to succeed from %s", tc.event, src)
				assert.Equal(t, tc.dest, sm.Current(), "unexpected destination after %s", tc.event)
				assert.Equal(t, string(tc.dest), website.Status, "fire should update website.Status")
			})
		}

		if tc.initialEmpty {
			t.Run(tc.name+"_from_empty", func(t *testing.T) {
				website := &pluginDb.Website{Status: ""}
				sm := NewWebsiteStateMachine(website)
				assert.True(t, sm.Can(tc.event))
				assert.NoError(t, sm.Fire(context.Background(), tc.event))
				assert.Equal(t, tc.dest, sm.Current())
				assert.Equal(t, string(tc.dest), website.Status)
			})
		}
	}
}

func TestWebsiteStateMachine_IllegalTransitions(t *testing.T) {
	for _, tc := range legalTransitions {
		for _, src := range allStatuses {
			if containsStatus(tc.sources, src) || (src == emptyState && tc.initialEmpty) {
				continue
			}

			t.Run(tc.name+"_from_"+string(src)+"_rejected", func(t *testing.T) {
				website := &pluginDb.Website{Status: string(src)}
				sm := NewWebsiteStateMachine(website)

				assert.False(t, sm.Can(tc.event), "expected %s to be illegal from %s", tc.event, src)
				err := sm.Fire(context.Background(), tc.event)
				assert.Error(t, err, "expected %s to be rejected from %s", tc.event, src)
				// Status must be untouched on a rejected transition.
				assert.Equal(t, string(src), website.Status)
				assert.Equal(t, src, sm.Current())
			})
		}
	}
}

// TestWebsiteStateMachine_NoSelfTransitions documents that looplab/fsm v1.0.3
// rejects self-transitions (Src == Dst), so call sites keep a website in the
// same state via Can() guards rather than firing an event that would error.
func TestWebsiteStateMachine_NoSelfTransitions(t *testing.T) {
	// An already-active website is a no-op for revalidate_ok (no self-
	// transition); it stays active without firing an event.
	active := &pluginDb.Website{Status: string(pluginDb.WebsiteStatusActive)}
	smActive := NewWebsiteStateMachine(active)
	assert.False(t, smActive.Can(EventWebsiteRevalidateOK))
	// But target_changed is a real transition that forces re-validation.
	assert.True(t, smActive.Can(EventWebsiteTargetChanged))

	// An already-pending website is a no-op for create (only legal from "") and
	// target_changed (only legal from active/broken).
	pending := &pluginDb.Website{Status: string(pluginDb.WebsiteStatusPendingValidation)}
	smPending := NewWebsiteStateMachine(pending)
	assert.False(t, smPending.Can(EventWebsiteCreate))
	assert.False(t, smPending.Can(EventWebsiteTargetChanged))
	// But it can still be validated.
	assert.True(t, smPending.Can(EventWebsiteValidate))
}

func TestWebsiteStateMachine_BlockedIsTerminalForHealthEvents(t *testing.T) {
	// A blocked website must not be flipped to broken/active by janitor health
	// events, nor be re-validated directly.
	website := &pluginDb.Website{Status: string(pluginDb.WebsiteStatusBlocked)}
	sm := NewWebsiteStateMachine(website)

	for _, ev := range []string{EventWebsiteValidate, EventWebsiteRevalidateOK, EventWebsiteCIDUnpinned, EventWebsiteCreate, EventWebsiteBlock} {
		assert.False(t, sm.Can(ev), "expected %s to be illegal from blocked", ev)
	}

	assert.True(t, sm.Can(EventWebsiteUnblock))
	assert.NoError(t, sm.Fire(context.Background(), EventWebsiteUnblock))
	assert.Equal(t, pluginDb.WebsiteStatusPendingValidation, sm.Current())
}
