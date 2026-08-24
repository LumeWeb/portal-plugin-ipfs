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
		janitorJob.logger = nil

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
		janitorJob.logger = nil

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
		janitorJob.logger = nil

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
