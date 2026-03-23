package website

import (
	"context"
	"io/fs"
	"testing"
	"time"

	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
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
	coreTesting.NewMockPluginBuilder(internal.ProtocolName).
		WithMockServiceFactory(pluginCore.WEBSITE_SERVICE, mocks.NewMockWebsiteService).
		WithServiceConfig(pluginCore.WEBSITE_SERVICE, &pluginConfig.WebsiteConfig{
			NotificationsEnabled: false,
		}).
		WithMockServiceFactory(pluginCore.IPNS_KEY_SERVICE, mocks.NewMockIPNSKeyService).
		WithMockServiceFactory(pluginCore.PIN_SERVICE, mocks.NewMockIPFSPinService).
		WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService).
		WithMockServiceFactory(pluginCore.DNS_SERVICE, mocks.NewMockDNSService).
		WithServiceConfig(pluginCore.DNS_SERVICE, &pluginConfig.DnsConfig{}).
		WithMigrations(map[core.DBType]fs.FS{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
		}).BuilderOption(),
	util.GetProtocolMock(),
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
			JanitorEnabled:     true,
			CheckInterval:      30 * time.Minute,
			JanitorWorkerCount: 2,
			JanitorBatchSize:   10,
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


func TestWebsiteJanitorJob_ValidateIPNSTarget_FullPath(t *testing.T) {
	// This test verifies that IPNS path prefixes are extracted correctly.
	// When an IPNS record returns a path like "/ipfs/bafy...", the janitor should
	// use internal.ExtractCIDFromPathLenient() to extract the CID.

	tests := []struct {
		name         string
		cid          string
	}{
		{
			name:         "Standard CID",
			cid:          "bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange - create path.Path from CID
			// Decode CID and create path from it (which represents /ipfs/{cid})
			targetCID, err := cid.Decode(tt.cid)
			require.NoError(t, err, "CID should be valid")
			targetPath := path.FromCid(targetCID)
			
			// Act - use the same helper as the production code
			// The helper extracts CID from the path by examining segments
			cidStr := internal.ExtractCIDFromPathLenient(targetPath)

			// Assert - verify the CID was extracted correctly
			assert.Equal(t, tt.cid, cidStr,
				"CID should be extracted correctly from path")
		})
	}
}
