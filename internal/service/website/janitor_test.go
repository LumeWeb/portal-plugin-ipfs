package website

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
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
func (r *recordingDelegatedDomainSvc) VerifyDomain(context.Context, *pluginDb.WebsiteDomain) (bool, error) {
	return true, nil
}
func (r *recordingDelegatedDomainSvc) GetNamespaceForDomain(string) (string, bool) { return "", false }
func (r *recordingDelegatedDomainSvc) GetWebsiteDomainByName(context.Context, string) (*pluginDb.WebsiteDomain, error) {
	return nil, gorm.ErrRecordNotFound
}
func (r *recordingDelegatedDomainSvc) GetPendingWebsiteDomainsPaginated(_ context.Context, status pluginDb.DomainStatus, _, _ int) ([]pluginDb.WebsiteDomain, error) {
	r.statuses = append(r.statuses, status)
	return nil, nil
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
