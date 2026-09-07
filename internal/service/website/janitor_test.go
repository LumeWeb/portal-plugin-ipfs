package website

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	domsvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/testopts"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

var JanitorTestOptions = coreTesting.CombineOptions(
	testopts.NewMockPluginBuilder().
		WithServiceConfig(pluginCore.WEBSITE_SERVICE, &pluginConfig.WebsiteConfig{
			NotificationsEnabled: false,
		}).
		WithMigrations(map[core.DBType]fs.FS{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
		}).BuilderOption(),
	util.GetProtocolMock(),
)

func TestWebsiteJanitorJob_NewWebsiteJanitorJob(t *testing.T) {
	job := NewWebsiteJanitorJob()

	assert.NotNil(t, job)
	assert.NotEmpty(t, job.ID())
	assert.Equal(t, core.JobOriginPlugin, job.Origin())
	assert.Equal(t, JanitorJobSourceID, job.SourceID())
	assert.Equal(t, "IPFS Website Janitor", job.DisplayName())
}

func TestWebsiteJanitorJob_Run_Disabled(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		job := NewWebsiteJanitorJob()

		websiteConfig := &pluginConfig.WebsiteConfig{
			JanitorEnabled: false,
		}

		janitorJob := job.(*WebsiteJanitorJob)
		janitorJob.config = websiteConfig
		janitorJob.db = ctx.DB()
		janitorJob.logger = ctx.Logger()

		err := job.Run(ctx, context.Background())

		require.NoError(tb, err)
	}, JanitorTestOptions)
}

func TestWebsiteJanitorJob_Run_NoWebsites(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		job := NewWebsiteJanitorJob()

		websiteConfig := &pluginConfig.WebsiteConfig{
			JanitorEnabled:     true,
			CheckInterval:      30 * time.Minute,
			JanitorWorkerCount: 2,
			JanitorBatchSize:   10,
		}

		janitorJob := job.(*WebsiteJanitorJob)
		janitorJob.config = websiteConfig
		janitorJob.db = ctx.DB()
		janitorJob.logger = ctx.Logger()

		err := job.Run(ctx, context.Background())

		require.NoError(tb, err)
	}, JanitorTestOptions)
}

func TestWebsiteJanitorJob_validateWebsite_SkipsPendingValidation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		job := NewWebsiteJanitorJob()

		websiteConfig := &pluginConfig.WebsiteConfig{
			JanitorEnabled:     true,
			CheckInterval:      30 * time.Minute,
			JanitorWorkerCount: 2,
			JanitorBatchSize:   10,
		}

		janitorJob := job.(*WebsiteJanitorJob)
		janitorJob.config = websiteConfig
		janitorJob.db = ctx.DB()
		janitorJob.logger = ctx.Logger()

		// Build an IPFS website in pending_validation state whose CID is NOT
		// pinned. Before the fix, the janitor would mark it broken.
		mhBytes, err := mh.Sum([]byte("unpinned-cid"), mh.SHA2_256, -1)
		require.NoError(tb, err)
		cidVersion := uint8(1)
		cidType := uint8(cid.Raw)

		website := &pluginDb.Website{
			TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
			TargetMultihash: mhBytes,
			CIDVersion:      &cidVersion,
			CIDType:         &cidType,
			Status:          string(pluginDb.WebsiteStatusPendingValidation),
			ValidationToken: "test-token",
		}
		require.NoError(tb, ctx.DB().Create(website).Error)
		require.Equal(tb, string(pluginDb.WebsiteStatusPendingValidation), website.Status)

		// Act: run the janitor validation over this website.
		require.NoError(tb, janitorJob.validateWebsite(context.Background(), website))

		// Assert: still pending_validation (not broken), and last_checked_at refreshed.
		var persisted pluginDb.Website
		require.NoError(tb, ctx.DB().First(&persisted, website.ID).Error)
		assert.Equal(tb, string(pluginDb.WebsiteStatusPendingValidation), persisted.Status)
		assert.NotNil(tb, persisted.LastCheckedAt)
	}, JanitorTestOptions)
}

func TestWebsiteJanitorJob_validateWebsite_GracePeriodDefersBroken(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		job := NewWebsiteJanitorJob()

		websiteConfig := &pluginConfig.WebsiteConfig{
			JanitorEnabled:     true,
			CheckInterval:      30 * time.Minute,
			JanitorWorkerCount: 2,
			JanitorBatchSize:   10,
			JanitorGracePeriod: 1 * time.Hour,
		}

		janitorJob := job.(*WebsiteJanitorJob)
		janitorJob.config = websiteConfig
		janitorJob.db = ctx.DB()
		janitorJob.logger = ctx.Logger()

		// An active website whose CID is NOT pinned, created just now (within
		// the grace period).
		mhBytes, err := mh.Sum([]byte("unpinned-within-grace"), mh.SHA2_256, -1)
		require.NoError(tb, err)
		cidVersion := uint8(1)
		cidType := uint8(cid.Raw)

		website := &pluginDb.Website{
			TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
			TargetMultihash: mhBytes,
			CIDVersion:      &cidVersion,
			CIDType:         &cidType,
			Status:          string(pluginDb.WebsiteStatusActive),
			ValidationToken: "test-token",
		}
		require.NoError(tb, ctx.DB().Create(website).Error)
		require.Equal(tb, string(pluginDb.WebsiteStatusActive), website.Status)

		// Act
		require.NoError(tb, janitorJob.validateWebsite(context.Background(), website))

		// Assert: not broken during the grace period, last_checked_at refreshed.
		var persisted pluginDb.Website
		require.NoError(tb, ctx.DB().First(&persisted, website.ID).Error)
		assert.Equal(tb, string(pluginDb.WebsiteStatusActive), persisted.Status)
		assert.NotNil(tb, persisted.LastCheckedAt)
	}, JanitorTestOptions)
}

func TestWebsiteJanitorJob_validateWebsite_GracePeriodDisabledMarksBroken(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		job := NewWebsiteJanitorJob()

		// Grace period explicitly disabled (0) preserves legacy behavior.
		websiteConfig := &pluginConfig.WebsiteConfig{
			JanitorEnabled:     true,
			CheckInterval:      30 * time.Minute,
			JanitorWorkerCount: 2,
			JanitorBatchSize:   10,
			JanitorGracePeriod: 0,
		}

		janitorJob := job.(*WebsiteJanitorJob)
		janitorJob.config = websiteConfig
		janitorJob.db = ctx.DB()
		janitorJob.logger = ctx.Logger()

		mhBytes, err := mh.Sum([]byte("unpinned-no-grace"), mh.SHA2_256, -1)
		require.NoError(tb, err)
		cidVersion := uint8(1)
		cidType := uint8(cid.Raw)

		website := &pluginDb.Website{
			TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
			TargetMultihash: mhBytes,
			CIDVersion:      &cidVersion,
			CIDType:         &cidType,
			Status:          string(pluginDb.WebsiteStatusActive),
			ValidationToken: "test-token",
		}
		require.NoError(tb, ctx.DB().Create(website).Error)

		// Act
		require.NoError(tb, janitorJob.validateWebsite(context.Background(), website))

		// Assert: marked broken when the grace period is disabled.
		var persisted pluginDb.Website
		require.NoError(tb, ctx.DB().First(&persisted, website.ID).Error)
		assert.Equal(tb, string(pluginDb.WebsiteStatusBroken), persisted.Status)
	}, JanitorTestOptions)
}

func TestWebsiteJanitorJob_validateWebsite_GracePeriodElapsedMarksBroken(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		job := NewWebsiteJanitorJob()

		websiteConfig := &pluginConfig.WebsiteConfig{
			JanitorEnabled:     true,
			CheckInterval:      30 * time.Minute,
			JanitorWorkerCount: 2,
			JanitorBatchSize:   10,
			JanitorGracePeriod: 1 * time.Hour,
		}

		janitorJob := job.(*WebsiteJanitorJob)
		janitorJob.config = websiteConfig
		janitorJob.db = ctx.DB()
		janitorJob.logger = ctx.Logger()

		mhBytes, err := mh.Sum([]byte("unpinned-grace-elapsed"), mh.SHA2_256, -1)
		require.NoError(tb, err)
		cidVersion := uint8(1)
		cidType := uint8(cid.Raw)

		// Website created well before the grace period.
		website := &pluginDb.Website{
			TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
			TargetMultihash: mhBytes,
			CIDVersion:      &cidVersion,
			CIDType:         &cidType,
			Status:          string(pluginDb.WebsiteStatusActive),
			ValidationToken: "test-token",
			CreatedAt:       time.Now().Add(-2 * time.Hour),
		}
		require.NoError(tb, ctx.DB().Create(website).Error)

		// Act
		require.NoError(tb, janitorJob.validateWebsite(context.Background(), website))

		// Assert: marked broken once the grace period has elapsed.
		var persisted pluginDb.Website
		require.NoError(tb, ctx.DB().First(&persisted, website.ID).Error)
		assert.Equal(tb, string(pluginDb.WebsiteStatusBroken), persisted.Status)
	}, JanitorTestOptions)
}

// TestWebsiteJanitorJob_validateWebsite_WarnOnlyKeepsActiveNoBrokenTransition
// verifies janitor warn-only mode: a failing target must NOT transition the
// website to broken (the site keeps serving); instead the website service is
// asked to warn the admin, once per run while the target is invalid.
func TestWebsiteJanitorJob_validateWebsite_WarnOnlyKeepsActiveNoBrokenTransition(t *testing.T) {
	for _, initialStatus := range []pluginDb.WebsiteStatus{pluginDb.WebsiteStatusActive, pluginDb.WebsiteStatusBroken} {
		t.Run(string(initialStatus), func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				job := NewWebsiteJanitorJob()

				websiteConfig := &pluginConfig.WebsiteConfig{
					JanitorEnabled:     true,
					CheckInterval:      30 * time.Minute,
					JanitorWorkerCount: 2,
					JanitorBatchSize:   10,
					JanitorGracePeriod: 0,
					JanitorWarnOnly:    true,
				}

				janitorJob := job.(*WebsiteJanitorJob)
				janitorJob.config = websiteConfig
				janitorJob.db = ctx.DB()
				janitorJob.logger = ctx.Logger()

				mhBytes, err := mh.Sum([]byte("unpinned-warn-only"), mh.SHA2_256, -1)
				require.NoError(tb, err)
				cidVersion := uint8(1)
				cidType := uint8(cid.Raw)

				website := &pluginDb.Website{
					TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
					TargetMultihash: mhBytes,
					CIDVersion:      &cidVersion,
					CIDType:         &cidType,
					Status:          string(initialStatus),
					ValidationToken: "test-token",
					CreatedAt:       time.Now().Add(-2 * time.Hour),
				}
				require.NoError(tb, ctx.DB().Create(website).Error)

				websiteSvc := mocks.NewMockWebsiteService(t)
				websiteSvc.EXPECT().
					NotifyAdminWebsiteBroken(mock.Anything, website.ID).
					Return(nil).Once()
				janitorJob.websiteSvc = websiteSvc

				// Act
				require.NoError(tb, janitorJob.validateWebsite(context.Background(), website))

				// Assert: status unchanged and last_checked_at refreshed.
				var persisted pluginDb.Website
				require.NoError(tb, ctx.DB().First(&persisted, website.ID).Error)
				assert.Equal(tb, string(initialStatus), persisted.Status)
				assert.NotNil(tb, persisted.LastCheckedAt)
			}, JanitorTestOptions)
		})
	}
}

func TestWebsiteJanitorJob_ID(t *testing.T) {
	job := NewWebsiteJanitorJob()
	jobID := job.ID()

	assert.NotEmpty(t, jobID)
	assert.IsType(t, jobID, jobID)
}

func TestWebsiteJanitorJob_DisplayName(t *testing.T) {
	job := NewWebsiteJanitorJob()
	assert.Equal(t, "IPFS Website Janitor", job.DisplayName())
}

func TestWebsiteJanitorJob_Origin(t *testing.T) {
	job := NewWebsiteJanitorJob()
	assert.Equal(t, core.JobOriginPlugin, job.Origin())
}

// recordingDelegatedDomainSvc records the statuses the janitor polls for
// pending delegations, so tests can assert exactly which lifecycle states are
// ever inspected (and that on-chain managed bindings are not).
type recordingDelegatedDomainSvc struct {
	statuses []pluginDb.DomainStatus
}

func (r *recordingDelegatedDomainSvc) UsesDelegationForOwnership(string) bool { return false }
func (r *recordingDelegatedDomainSvc) VerifyDomain(context.Context, *pluginDb.WebsiteDomain, ...domsvc.VerifyDomainOption) (domsvc.DelegationVerificationResult, error) {
	return domsvc.DelegationVerificationResult{State: domsvc.DelegationVerified}, nil
}
func (r *recordingDelegatedDomainSvc) GetNamespaceForDomain(string) (string, bool) { return "", false }
func (r *recordingDelegatedDomainSvc) ValidateOnChainTLSA(context.Context, *pluginDb.WebsiteDomain) (bool, string, string, string, error) {
	return true, "", "", "", nil
}
func (r *recordingDelegatedDomainSvc) GetWebsiteDomainByName(context.Context, string) (*pluginDb.WebsiteDomain, error) {
	return nil, gorm.ErrRecordNotFound
}
func (r *recordingDelegatedDomainSvc) DerivePolicyAxisColumns(context.Context, *pluginDb.WebsiteDomain, *pluginDb.Website) map[string]any {
	return nil
}

func (r *recordingDelegatedDomainSvc) DANEPublicationTargetFor(*pluginDb.WebsiteDomain) (domsvc.DANEPublicationTarget, bool) {
	return "", false
}
func (r *recordingDelegatedDomainSvc) CurrentBindingPlan(*pluginDb.WebsiteDomain, *pluginDb.Website) (domainpolicy.Plan, error) {
	return domainpolicy.Plan{}, errors.New("recordingDelegatedDomainSvc supplies no current-behavior plan")
}
func (r *recordingDelegatedDomainSvc) GetPendingWebsiteDomainsPaginated(_ context.Context, status pluginDb.DomainStatus, _, _ int) ([]pluginDb.WebsiteDomain, error) {
	r.statuses = append(r.statuses, status)
	return nil, nil
}

// driftReportingDelegatedDomainSvc yields a binding whose VerifyDomain
// reports a typed route-drift finding (HNS root persisted state vs. observed
// on-chain/eula.cross-chain route) — the janitor reconcile loop's
// worst case for the report-only guarantee.
type driftReportingDelegatedDomainSvc struct {
	recordingDelegatedDomainSvc
	wd pluginDb.WebsiteDomain
	// disableDrift makes VerifyDomain stop reporting drift — simulates an
	// operator-converted/resolved binding observed on a re-probe.
	disableDrift bool
	verified     int
	verifyArg    *pluginDb.WebsiteDomain
}

func (r *driftReportingDelegatedDomainSvc) GetPendingWebsiteDomainsPaginated(_ context.Context, status pluginDb.DomainStatus, _, _ int) ([]pluginDb.WebsiteDomain, error) {
	// The drifted binding lives in exactly one lifecycle pool; other polls
	// (records_generated) see nothing so VerifyDomain runs once.
	r.statuses = append(r.statuses, status)
	if status != pluginDb.DomainStatusWaitingDelegation {
		return nil, nil
	}
	return []pluginDb.WebsiteDomain{r.wd}, nil
}

func (r *driftReportingDelegatedDomainSvc) VerifyDomain(_ context.Context, wd *pluginDb.WebsiteDomain, _ ...domsvc.VerifyDomainOption) (domsvc.DelegationVerificationResult, error) {
	r.verified++
	r.verifyArg = wd
	res := domsvc.DelegationVerificationResult{State: domsvc.DelegationPending}
	if !r.disableDrift {
		res.RouteDrift = &domsvc.RouteDriftFinding{
			From:    domainpolicy.ResolutionRouteHNSRoot,
			To:      domainpolicy.ResolutionRouteCrossChain,
			Backend: domainpolicy.BackendEthereum,
		}
	}
	return res, nil
}

func TestWebsiteJanitorJob_verifyPendingDelegations_RouteDriftIsReportOnly(t *testing.T) {
	// The janitor is REPORT-ONLY for route
	// drift. Historically, this loop converted bindings as a side effect of
	// VerifyDomain; since VerifyDomain no longer converts, the janitor wiring
	// must also never enqueue, schedule, or perform conversion on its own. The
	// drift binding stays in the pending lifecycle pool (it is re-verified on
	// later runs) and the conversion decision is left entirely to the explicit
	// ConvertToOnChain command.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		job := NewWebsiteJanitorJob()
		janitorJob := job.(*WebsiteJanitorJob)
		janitorJob.db = ctx.DB()
		janitorJob.logger = ctx.Logger()
		fake := &driftReportingDelegatedDomainSvc{
			wd: pluginDb.WebsiteDomain{
				ID:        77,
				WebsiteID: 1,
				UserID:    1,
				Domain:    "janitor-drift.hns",
				Namespace: pluginDb.DomainNamespaceHNS,
				Status:    pluginDb.DomainStatusWaitingDelegation,
				ZoneID:    42,
			},
		}
		janitorJob.delegatedDomainSvc = fake

		require.NoError(tb, janitorJob.verifyPendingDelegations(context.Background()))

		// The drift binding was examined exactly once per polling pass — it
		// was neither dropped from the pool nor churned through retries.
		assert.Equal(tb, 1, fake.verified)
		require.NotNil(tb, fake.verifyArg)
		assert.Equal(tb, "janitor-drift.hns", fake.verifyArg.Domain)
		// The pending lifecycle pool is unchanged: a drifted binding remains
		// a waiting_delegation record that later runs may re-observe.
		assert.ElementsMatch(tb, []pluginDb.DomainStatus{
			pluginDb.DomainStatusWaitingDelegation,
			pluginDb.DomainStatusRecordsGenerated,
		}, fake.statuses)
	}, JanitorTestOptions)
}

// driftBackoffJanitor seeds a drifted binding in the database (so marker
// persistence can be observed) and wires a janitor job against the fake
// delegation service.
func driftBackoffJanitor(tb coreTesting.TB, ctx coreTesting.TestContext, fake *driftReportingDelegatedDomainSvc) *WebsiteJanitorJob {
	job := NewWebsiteJanitorJob()
	janitorJob := job.(*WebsiteJanitorJob)
	janitorJob.db = ctx.DB()
	janitorJob.logger = ctx.Logger()
	janitorJob.delegatedDomainSvc = fake
	require.NoError(tb, ctx.DB().Create(&fake.wd).Error)
	return janitorJob
}

func persistedDriftMarker(tb coreTesting.TB, ctx coreTesting.TestContext, id uint) *time.Time {
	var persisted pluginDb.WebsiteDomain
	require.NoError(tb, ctx.DB().Where("id = ?", id).First(&persisted).Error)
	return persisted.DriftDetectedAt
}

func TestWebsiteJanitorJob_verifyPendingDelegations_DriftBackoffSkipsReprobe(t *testing.T) {
	// A binding whose drift marker is still fresh must not be re-probed: the
	// janitor already reported the drift and nothing about the report-only
	// flow changes within the backoff window, so the external probes would
	// only repeat identical work every minute.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		now := time.Now()
		fake := &driftReportingDelegatedDomainSvc{
			wd: pluginDb.WebsiteDomain{
				ID:        101,
				WebsiteID: 1,
				UserID:    1,
				Domain:    "drift-backoff.hns",
				Namespace: pluginDb.DomainNamespaceHNS,
				Status:    pluginDb.DomainStatusWaitingDelegation,
				ZoneID:    43,
				// Distinguishable marker: a skip must leave it exactly as-is.
				DriftDetectedAt: &now,
			},
		}
		janitorJob := driftBackoffJanitor(tb, ctx, fake)

		require.NoError(tb, janitorJob.verifyPendingDelegations(context.Background()))

		assert.Zero(tb, fake.verified, "drifted binding must be skipped within the backoff window")
		marker := persistedDriftMarker(tb, ctx, fake.wd.ID)
		require.NotNil(tb, marker)
		assert.WithinDuration(tb, now, *marker, time.Second, "skip must not refresh the marker")
	}, JanitorTestOptions)
}

func TestWebsiteJanitorJob_verifyPendingDelegations_DriftReprobedAfterBackoff(t *testing.T) {
	// Once the backoff window elapses the janitor probes again and, because
	// the drift persists, refreshes the marker — keeping the degraded
	// (hourly-ish) reporting cadence instead of reverting to every minute.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		past := time.Now().Add(-2 * driftReprobeInterval)
		fake := &driftReportingDelegatedDomainSvc{
			wd: pluginDb.WebsiteDomain{
				ID:              102,
				WebsiteID:       1,
				UserID:          1,
				Domain:          "drift-reprobe.hns",
				Namespace:       pluginDb.DomainNamespaceHNS,
				Status:          pluginDb.DomainStatusWaitingDelegation,
				ZoneID:          44,
				DriftDetectedAt: &past,
			},
		}
		janitorJob := driftBackoffJanitor(tb, ctx, fake)

		require.NoError(tb, janitorJob.verifyPendingDelegations(context.Background()))

		assert.Equal(tb, 1, fake.verified, "drifted binding must be re-probed after the backoff window")
		require.NotNil(tb, fake.verifyArg)
		require.NotNil(tb, fake.verifyArg.DriftDetectedAt, "marker must be refreshed after a still-drifted re-probe")
		assert.True(tb, fake.verifyArg.DriftDetectedAt.After(past), "marker must be a fresh timestamp, not the old one")
		marker := persistedDriftMarker(tb, ctx, fake.wd.ID)
		require.NotNil(tb, marker)
		assert.True(tb, marker.After(past))
	}, JanitorTestOptions)
}

func TestWebsiteJanitorJob_verifyPendingDelegations_DriftMarkerClearedWhenResolved(t *testing.T) {
	// A re-probe after backoff expiry that no longer observes drift clears
	// the marker, so the binding returns to normal full-cadence verification.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		past := time.Now().Add(-2 * driftReprobeInterval)
		fake := &driftReportingDelegatedDomainSvc{
			// VerifyDomain reports no drift anymore (operator converted or
			// drift otherwise resolved).
			disableDrift: true,
			wd: pluginDb.WebsiteDomain{
				ID:              103,
				WebsiteID:       1,
				UserID:          1,
				Domain:          "drift-resolved.hns",
				Namespace:       pluginDb.DomainNamespaceHNS,
				Status:          pluginDb.DomainStatusWaitingDelegation,
				ZoneID:          45,
				DriftDetectedAt: &past,
			},
		}
		janitorJob := driftBackoffJanitor(tb, ctx, fake)

		require.NoError(tb, janitorJob.verifyPendingDelegations(context.Background()))

		assert.Equal(tb, 1, fake.verified)
		require.NotNil(tb, fake.verifyArg)
		assert.Nil(tb, fake.verifyArg.DriftDetectedAt, "in-memory marker must be cleared")
		assert.Nil(tb, persistedDriftMarker(tb, ctx, fake.wd.ID), "persisted marker must be cleared")
	}, JanitorTestOptions)
}

func TestWebsiteJanitorJob_verifyPendingDelegations_IgnoresOnchainManaged(t *testing.T) {
	// The janitor's delegation verification must only ever poll the delegation
	// lifecycle statuses (records_generated, waiting_delegation). On-chain
	// managed (HIP-5) bindings prove ownership via the TXT token flow and must
	// never be picked up for NS/DS delegation verification.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		job := NewWebsiteJanitorJob()
		janitorJob := job.(*WebsiteJanitorJob)
		janitorJob.db = ctx.DB()
		janitorJob.logger = ctx.Logger()
		fake := &recordingDelegatedDomainSvc{}
		janitorJob.delegatedDomainSvc = fake

		require.NoError(tb, janitorJob.verifyPendingDelegations(context.Background()))

		assert.ElementsMatch(tb, []pluginDb.DomainStatus{
			pluginDb.DomainStatusWaitingDelegation,
			pluginDb.DomainStatusRecordsGenerated,
		}, fake.statuses)
		assert.NotContains(tb, fake.statuses, pluginDb.DomainStatusOnchainManaged)
	}, JanitorTestOptions)
}
