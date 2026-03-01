package website

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

var JanitorTestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.WEBSITE_SERVICE, NewWebsiteService),
	coreTesting.WithMockServiceFactory(pluginCore.IPNS_KEY_SERVICE, mocks.NewMockIPNSKeyService),
	coreTesting.WithMockServiceFactory(pluginCore.IPNS_PUBLISHER_SERVICE, mocks.NewMockIPNSPublisherService),
	coreTesting.WithMockServiceFactory(pluginCore.PIN_SERVICE, mocks.NewMockIPFSPinService),
	coreTesting.WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService),
	util.GetProtocolMock(),
	// Disable notifications to avoid mailer mock issues in tests
	coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{
		Website: pluginConfig.WebsiteConfig{
			NotificationsEnabled: false,
		},
	}),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
)

func TestWebsiteJanitorJob_NewWebsiteJanitorJob(t *testing.T) {
	// Act
	job := NewWebsiteJanitorJob()

	// Assert
	assert.NotNil(t, job)
	assert.NotEmpty(t, job.ID())
	assert.Equal(t, core.JobOriginPlugin, job.Origin())
	assert.Equal(t, JanitorJobSourceID, job.SourceID())
	assert.Equal(t, "IPFS Website Janitor", job.DisplayName())
}

func TestWebsiteJanitorJob_Run_Disabled(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		job := NewWebsiteJanitorJob()

		// Configure janitor as disabled
		websiteConfig := &pluginConfig.WebsiteConfig{
			JanitorEnabled: false,
		}

		// Inject config into job
		janitorJob := job.(*WebsiteJanitorJob)
		janitorJob.config = websiteConfig
		janitorJob.db = ctx.DB()
		janitorJob.logger = nil

		// Act
		err := job.Run(ctx, context.Background())

		// Assert - Should not return error when disabled
		require.NoError(tb, err)
	}, JanitorTestOptions)
}

func TestWebsiteJanitorJob_Run_NoWebsites(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		job := NewWebsiteJanitorJob()

		// Configure janitor
		websiteConfig := &pluginConfig.WebsiteConfig{
			JanitorEnabled:      true,
			JanitorInterval:     30 * time.Minute,
			JanitorWorkerCount:  2,
			JanitorBatchSize:    10,
		}

		// Inject dependencies
		janitorJob := job.(*WebsiteJanitorJob)
		janitorJob.config = websiteConfig
		janitorJob.db = ctx.DB()
		janitorJob.logger = nil

		// Act
		err := job.Run(ctx, context.Background())

		// Assert
		require.NoError(tb, err)
	}, JanitorTestOptions)
}

func TestWebsiteJanitorJob_ID(t *testing.T) {
	// Arrange
	job := NewWebsiteJanitorJob()

	// Act
	jobID := job.ID()

	// Assert
	assert.NotEmpty(t, jobID)
	assert.IsType(t, jobID, jobID)
}

func TestWebsiteJanitorJob_DisplayName(t *testing.T) {
	// Arrange
	job := NewWebsiteJanitorJob()

	// Act
	jobName := job.DisplayName()

	// Assert
	assert.Equal(t, "IPFS Website Janitor", jobName)
}

func TestWebsiteJanitorJob_Origin(t *testing.T) {
	// Arrange
	job := NewWebsiteJanitorJob()

	// Act
	origin := job.Origin()

	// Assert
	assert.Equal(t, core.JobOriginPlugin, origin)
}

func TestWebsiteJanitorJob_SourceID(t *testing.T) {
	// Arrange
	job := NewWebsiteJanitorJob()

	// Act
	sourceID := job.SourceID()

	// Assert
	assert.Equal(t, "ipfs.website_janitor", sourceID)
}
