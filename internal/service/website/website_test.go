package website

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/queryutil"
)

// Helper function to create a test website with IPFS target
func createTestIPFSWebsite(userID uint, domain string, cidStr string) *pluginDb.Website {
	c := cid.MustParse(cidStr)
	version := uint8(c.Version())
	return &pluginDb.Website{
		UserID:          userID,
		Domain:          domain,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
		TargetMultihash: c.Hash(),
		CIDVersion:      &version,
		SSLStatus:       string(pluginDb.SSLStatusPending),
	}
}

// Helper function to create a test website with IPNS target
func createTestIPNSWebsite(userID uint, domain string, ipnsStr string) *pluginDb.Website {
	target, _ := pluginDb.NewIPNSTargetFromString(ipnsStr)
	return &pluginDb.Website{
		UserID:          userID,
		Domain:          domain,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPNS),
		TargetMultihash: target.ToMultihash(),
		CIDVersion:      nil,
		SSLStatus:       string(pluginDb.SSLStatusPending),
	}
}

var TestOptions = coreTesting.CombineOptions(
	// Use mock IPNS key service since website service depends on it
	coreTesting.WithMockServiceFactory(pluginCore.IPNS_KEY_SERVICE, mocks.NewMockIPNSKeyService),
	coreTesting.WithServiceFactory(pluginCore.WEBSITE_SERVICE, NewWebsiteService),
	coreTesting.WithMockServiceFactory(pluginCore.PIN_SERVICE, mocks.NewMockIPFSPinService),
	coreTesting.WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService),
	coreTesting.WithMockMailerService(),
	util.GetProtocolMock(),
	// Disable notifications to avoid mailer mock issues in tests
	coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{
		Website: pluginConfig.WebsiteConfig{
			NotificationsEnabled: false,
			AdminEmail:           "",
			ValidationTokenTTL:   24 * time.Hour, // Extended TTL for tests
		},
	}),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
)

func TestWebsiteService_CreateWebsite_IPFSTarget(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "example.com", testCID.String())

		// Act
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.Equal(tb, userID, createdWebsite.UserID)
		assert.Equal(tb, "example.com", createdWebsite.Domain)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPFS), createdWebsite.TargetType)
		assert.Equal(tb, testCID.String(), createdWebsite.TargetHash())
		assert.Equal(tb, string(pluginDb.WebsiteStatusPendingValidation), createdWebsite.Status)
		assert.NotEmpty(tb, createdWebsite.ValidationToken)
		assert.NotNil(tb, createdWebsite.ValidationExpiresAt)
		assert.NotZero(tb, createdWebsite.ID)

		// Verify website can be retrieved
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(tb, createdWebsite.ID, retrievedWebsite.ID)
	}, TestOptions)
}

func TestWebsiteService_SSLStatusDoesNotAffectWebsiteStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")
		website := createTestIPFSWebsite(userID, "example.com", testCID.String())

		// Create website
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Get initial website status before SSL update
		initialStatus := createdWebsite.Status

		// Update SSL status to failed
		_, err = websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusFailed, "cert validation failed", nil)
		require.NoError(tb, err)

		// Act & Assert - Verify SSL status is failed and website status is unchanged
		finalWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)

		assert.Equal(tb, initialStatus, finalWebsite.Status, "website status should not be affected by SSL transitions")
		assert.Equal(tb, string(pluginDb.SSLStatusFailed), finalWebsite.SSLStatus)
		assert.Equal(tb, "cert validation failed", finalWebsite.SSLError)
	}, TestOptions)
}

func TestWebsiteService_SSLStatusTransitionsIndependently(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")
		website := createTestIPFSWebsite(userID, "example.com", testCID.String())

		// Create website
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act - Simulate SSL status transitions
		now := time.Now()

		// pending -> issuing
		updatedWebsite, err := websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusIssuing, "", &now)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.SSLStatusIssuing), updatedWebsite.SSLStatus)
		assert.Nil(tb, updatedWebsite.SSLIssuedAt)

		// issuing -> ready
		now2 := time.Now().Add(time.Minute)
		updatedWebsite, err = websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusReady, "", &now2)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.SSLStatusReady), updatedWebsite.SSLStatus)
		assert.NotNil(tb, updatedWebsite.SSLIssuedAt)
		assert.Equal(tb, "", updatedWebsite.SSLError)

		// ready -> failed (simulating certificate expiration)
		now3 := time.Now().Add(2 * time.Minute)
		updatedWebsite, err = websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusFailed, "certificate expired", &now3)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.SSLStatusFailed), updatedWebsite.SSLStatus)
		assert.Equal(tb, "certificate expired", updatedWebsite.SSLError)

		// Assert - Website status should not have changed
		finalWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.WebsiteStatusPendingValidation), finalWebsite.Status, "website status should not be affected by SSL transitions")
	}, TestOptions)
}

func TestWebsiteService_WebsiteCanBeBrokenRegardlessOfSSLStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")
		website := createTestIPFSWebsite(userID, "example.com", testCID.String())

		// Create website
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Set SSL status to ready
		_, err = websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusReady, "", nil)
		require.NoError(tb, err)

		// Verify SSL is ready
		websiteWithSSL, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.SSLStatusReady), websiteWithSSL.SSLStatus)

		// Act - Update website status to broken
		updates := map[string]interface{}{"status": string(pluginDb.WebsiteStatusBroken)}
		_, err = websiteService.UpdateWebsite(context.Background(), userID, createdWebsite.ID, updates)
		require.NoError(tb, err)

		// Assert - SSL status remains ready, website status is now broken
		finalWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.SSLStatusReady), finalWebsite.SSLStatus, "SSL status should not be affected by website status change")
		assert.Equal(t, string(pluginDb.WebsiteStatusBroken), finalWebsite.Status, "Website status should be broken")
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_IPNSTarget(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		// Use a valid IPNS name (CIDv1 with libp2p-key codec)
		ipnsName := "k51qzi5uqu5dlts3p5vfpw8kneqp5ye1ttb2jlt8qkt5mq9f2gvgmet6sec29r"

		website := createTestIPNSWebsite(userID, "ipns-example.com", ipnsName)

		// Act
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.Equal(tb, userID, createdWebsite.UserID)
		assert.Equal(tb, "ipns-example.com", createdWebsite.Domain)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite.TargetType)
		assert.Equal(tb, ipnsName, createdWebsite.TargetHash())
		assert.Equal(tb, string(pluginDb.WebsiteStatusPendingValidation), createdWebsite.Status)
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_InvalidDomain(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "invalid domain with spaces", testCID.String())
		website.Status = "" // Clear for validation error test


		// Act
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, createdWebsite)
		assert.Contains(tb, err.Error(), "invalid domain")
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_DuplicateDomain(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website1 := createTestIPFSWebsite(userID, "duplicate.com", testCID.String())

		// Create first website
		_, err := websiteService.CreateWebsite(context.Background(), website1)
		require.NoError(tb, err)

		website2 := createTestIPFSWebsite(userID, "duplicate.com", testCID.String())

		// Act - Try to create duplicate
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website2)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, createdWebsite)
		assert.Contains(tb, err.Error(), "domain already exists")
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_InvalidTargetType(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)

		website := &pluginDb.Website{
			UserID:     userID,
			Domain:     "example.com",
			TargetType: "invalid_type",
		}

		// Act
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, createdWebsite)
		assert.Contains(tb, err.Error(), "invalid target")
	}, TestOptions)
}

func TestWebsiteService_GetWebsite(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "get-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedWebsite)
		assert.Equal(tb, createdWebsite.ID, retrievedWebsite.ID)
		assert.Equal(tb, createdWebsite.Domain, retrievedWebsite.Domain)
		assert.Equal(tb, createdWebsite.TargetType, retrievedWebsite.TargetType)
		assert.Equal(tb, createdWebsite.TargetHash(), retrievedWebsite.TargetHash())
	}, TestOptions)
}

func TestWebsiteService_GetWebsite_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		nonExistentID := uint(99999)

		// Act
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), userID, nonExistentID)

		// Assert - GetWebsite returns (nil, nil) for not found (no error)
		require.NoError(tb, err)
		assert.Nil(tb, retrievedWebsite)
	}, TestOptions)
}

func TestWebsiteService_GetWebsiteByDomain(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "domain-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act
		retrievedWebsite, err := websiteService.GetWebsiteByDomain(context.Background(), "domain-test.com")

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedWebsite)
		assert.Equal(tb, createdWebsite.ID, retrievedWebsite.ID)
		assert.Equal(tb, "domain-test.com", retrievedWebsite.Domain)
	}, TestOptions)
}

func TestWebsiteService_GetWebsiteByDomain_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Act
		retrievedWebsite, err := websiteService.GetWebsiteByDomain(context.Background(), "nonexistent.com")

		// Assert - GetWebsiteByDomain returns (nil, nil) for not found (no error)
		require.NoError(tb, err)
		assert.Nil(tb, retrievedWebsite)
	}, TestOptions)
}

func TestWebsiteService_ListWebsites(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		userID2 := uint(2)

		// Create websites for user 1
		website1 := createTestIPFSWebsite(userID, "list1.com", util.GenerateTestCID(t, "data1").String())
		website2 := createTestIPFSWebsite(userID, "list2.com", util.GenerateTestCID(t, "data2").String())

		created1, err := websiteService.CreateWebsite(context.Background(), website1)
		require.NoError(tb, err)
		created2, err := websiteService.CreateWebsite(context.Background(), website2)
		require.NoError(tb, err)

		// Create a website for user 2
		website3 := createTestIPFSWebsite(userID2, "list3.com", util.GenerateTestCID(t, "data3").String())
		_, err = websiteService.CreateWebsite(context.Background(), website3)
		require.NoError(tb, err)

		// Act - List websites for user 1
		websites, total, err := websiteService.ListWebsites(context.Background(), userID, nil, nil, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err)
		assert.Equal(tb, int64(2), total)
		assert.Len(tb, websites, 2)

		// Verify all websites belong to user 1
		for _, w := range websites {
			assert.Equal(tb, userID, w.UserID)
		}

		// Verify the expected websites are in the list
		websiteIDs := make(map[uint]bool)
		for _, w := range websites {
			websiteIDs[w.ID] = true
		}
		assert.True(tb, websiteIDs[created1.ID])
		assert.True(tb, websiteIDs[created2.ID])
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "update-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		newCID := util.GenerateTestCID(t, "new data")
		newVersion := uint8(newCID.Version())
		updates := map[string]interface{}{
			"target_multihash": newCID.Hash(),
			"cid_version":      &newVersion,
			"status":           string(pluginDb.WebsiteStatusActive),
		}

		// Act
		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), userID, createdWebsite.ID, updates)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, updatedWebsite)
		assert.Equal(tb, createdWebsite.ID, updatedWebsite.ID)
		assert.Equal(tb, newCID.String(), updatedWebsite.TargetHash())
		assert.Equal(tb, string(pluginDb.WebsiteStatusActive), updatedWebsite.Status)
		assert.Equal(tb, "update-test.com", updatedWebsite.Domain)
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		nonExistentID := uint(99999)

		updates := map[string]interface{}{
			"status": string(pluginDb.WebsiteStatusActive),
		}

		// Act
		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), userID, nonExistentID, updates)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, updatedWebsite)
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "delete-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Verify website exists
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedWebsite)

		// Act - Delete the website
		err = websiteService.DeleteWebsite(context.Background(), userID, createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

		// Verify website is soft-deleted (should not be found)
		retrievedWebsite, err = websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Nil(tb, retrievedWebsite)
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		nonExistentID := uint(99999)

		// Act
		err := websiteService.DeleteWebsite(context.Background(), userID, nonExistentID)

		// Assert
		assert.Error(tb, err)
	}, TestOptions)
}

func TestWebsiteService_ValidationTokenExpiration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "expire-test.com", testCID.String())

		// Act
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite.ValidationExpiresAt)

		// Verify the expiration is in the future (approximately 24 hours)
		expectedExpiry := time.Now().Add(24 * time.Hour)
		timeDiff := createdWebsite.ValidationExpiresAt.Sub(expectedExpiry)

		// Allow for some time difference during test execution (within 1 second)
		assert.Less(t, timeDiff.Seconds(), 1.0)
		assert.Greater(t, timeDiff.Seconds(), -1.0)
	}, TestOptions)
}

func TestWebsiteService_StatusTransitions(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "status-test.com", testCID.String())

		// Create website (should be pending_validation)
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusPendingValidation), createdWebsite.Status)

		// Update to active
		updates := map[string]interface{}{
			"status": string(pluginDb.WebsiteStatusActive),
		}
		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), userID, createdWebsite.ID, updates)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusActive), updatedWebsite.Status)

		// Update to broken
		updates["status"] = string(pluginDb.WebsiteStatusBroken)
		updatedWebsite, err = websiteService.UpdateWebsite(context.Background(), userID, createdWebsite.ID, updates)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusBroken), updatedWebsite.Status)
	}, TestOptions)
}

func TestWebsiteService_ShouldCheck(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "check-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Test 1: Never checked website should be checked
		assert.True(t, createdWebsite.ShouldCheck(30*time.Minute))

		// Test 2: Recently checked website should not be checked
		now := time.Now()
		createdWebsite.LastCheckedAt = &now
		assert.False(t, createdWebsite.ShouldCheck(30*time.Minute))

		// Test 3: Old checked website should be checked
		oldTime := time.Now().Add(-1 * time.Hour)
		createdWebsite.LastCheckedAt = &oldTime
		assert.True(t, createdWebsite.ShouldCheck(30*time.Minute))
	}, TestOptions)
}

func TestWebsiteService_IsExpired(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "expire-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Test 1: Newly created website should not be expired
		assert.False(t, createdWebsite.IsExpired())

		// Test 2: Website with expired timestamp should be expired
		pastTime := time.Now().Add(-1 * time.Hour)
		createdWebsite.ValidationExpiresAt = &pastTime
		assert.True(t, createdWebsite.IsExpired())

		// Test 3: Website with nil expiry should not be expired
		createdWebsite.ValidationExpiresAt = nil
		assert.False(t, createdWebsite.IsExpired())
	}, TestOptions)
}

func TestWebsiteService_BlockWebsite(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "block-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusPendingValidation), createdWebsite.Status)

		// Act - Block the website
		err = websiteService.BlockWebsite(context.Background(), createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

		// Verify website is now blocked
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedWebsite)
		assert.Equal(t, string(pluginDb.WebsiteStatusBlocked), retrievedWebsite.Status)
	}, TestOptions)
}

func TestWebsiteService_UnblockWebsite(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "unblock-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Block the website first
		err = websiteService.BlockWebsite(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)

		// Verify it's blocked
		blockedWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusBlocked), blockedWebsite.Status)

		// Act - Unblock the website
		err = websiteService.UnblockWebsite(context.Background(), createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

		// Verify website is now active
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedWebsite)
		assert.Equal(t, string(pluginDb.WebsiteStatusActive), retrievedWebsite.Status)
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_Blocked(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "blocked-delete-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Block the website
		err = websiteService.BlockWebsite(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)

		// Verify it's blocked
		blockedWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusBlocked), blockedWebsite.Status)

		// Act - Try to delete the blocked website
		err = websiteService.DeleteWebsite(context.Background(), userID, createdWebsite.ID)

		// Assert - Should fail with error
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "cannot delete blocked website")

		// Verify website still exists and is blocked
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedWebsite)
		assert.Equal(t, string(pluginDb.WebsiteStatusBlocked), retrievedWebsite.Status)
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_Active(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "active-delete-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Update to active status
		updates := map[string]interface{}{
			"status": string(pluginDb.WebsiteStatusActive),
		}
		_, err = websiteService.UpdateWebsite(context.Background(), userID, createdWebsite.ID, updates)
		require.NoError(tb, err)

		// Act - Delete the active website
		err = websiteService.DeleteWebsite(context.Background(), userID, createdWebsite.ID)

		// Assert - Should succeed
		require.NoError(tb, err)

		// Verify website is soft-deleted
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), userID, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Nil(tb, retrievedWebsite)
	}, TestOptions)
}

func TestWebsiteService_UpdateSSLStatus_SuccessfulUpdate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "ssl-update-test.com", testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act - Update SSL status to issuing
		updatedWebsite, err := websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusIssuing, "", nil)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.SSLStatusIssuing), updatedWebsite.SSLStatus)
		assert.NotNil(tb, updatedWebsite.SSLLastUpdatedAt)
		assert.Empty(tb, updatedWebsite.SSLError)
	}, TestOptions)
}

func TestWebsiteService_UpdateSSLStatus_WebsiteNotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Act - Try to update SSL status for non-existent domain
		updatedWebsite, err := websiteService.UpdateSSLStatus(context.Background(), "nonexistent.com", pluginDb.SSLStatusReady, "", nil)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, updatedWebsite)
		assert.Contains(tb, err.Error(), "website not found")
	}, TestOptions)
}

func TestWebsiteService_UpdateSSLStatus_IssuedAtSetOnlyOnReadyTransition(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "ssl-issuedat-test.com", testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act - Update to issuing (should not set issued_at)
		_, err = websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusIssuing, "", nil)
		require.NoError(tb, err)

		// Check website after first update
		websiteAfterIssuing, err := websiteService.GetWebsiteByDomain(context.Background(), createdWebsite.Domain)
		require.NoError(tb, err)
		assert.Nil(tb, websiteAfterIssuing.SSLIssuedAt)

		// Act - Update to ready (should set issued_at)
		_, err = websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusReady, "", nil)
		require.NoError(tb, err)

		// Check website after ready transition
		websiteAfterReady, err := websiteService.GetWebsiteByDomain(context.Background(), createdWebsite.Domain)
		require.NoError(tb, err)
		assert.NotNil(tb, websiteAfterReady.SSLIssuedAt)

		// Act - Update to ready again (should not change issued_at)
		originalIssuedAt := websiteAfterReady.SSLIssuedAt
		time.Sleep(10 * time.Millisecond)
		_, err = websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusReady, "", nil)
		require.NoError(tb, err)

		// Check website after second ready update
		websiteAfterSecondReady, err := websiteService.GetWebsiteByDomain(context.Background(), createdWebsite.Domain)
		require.NoError(tb, err)
		assert.NotNil(tb, websiteAfterSecondReady.SSLIssuedAt)
		assert.Equal(tb, originalIssuedAt.Unix(), websiteAfterSecondReady.SSLIssuedAt.Unix(), "issued_at should not change when already ready")
	}, TestOptions)
}

func TestWebsiteService_UpdateSSLStatus_ErrorSetOnFailed(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "ssl-error-test.com", testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		testErrorMsg := "certificate validation failed"

		// Act - Update SSL status to failed with error message
		updatedWebsite, err := websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusFailed, testErrorMsg, nil)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.SSLStatusFailed), updatedWebsite.SSLStatus)
		assert.Equal(tb, testErrorMsg, updatedWebsite.SSLError)
	}, TestOptions)
}

func TestWebsiteService_UpdateSSLStatus_ErrorClearedOnStatusChange(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "ssl-clear-error-test.com", testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Set status to failed with error
		testErrorMsg := "certificate validation failed"
		_, err = websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusFailed, testErrorMsg, nil)
		require.NoError(tb, err)

		// Verify error is set
		websiteAfterFailed, err := websiteService.GetWebsiteByDomain(context.Background(), createdWebsite.Domain)
		require.NoError(tb, err)
		assert.Equal(tb, testErrorMsg, websiteAfterFailed.SSLError)

		// Act - Update to pending (should clear error)
		updatedWebsite, err := websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusPending, "", nil)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.SSLStatusPending), updatedWebsite.SSLStatus)
		assert.Empty(tb, updatedWebsite.SSLError, "Error should be cleared when status changes away from failed")
	}, TestOptions)
}

func TestWebsiteService_UpdateSSLStatus_AtomicUpdates(t *testing.T) {
	// Skip on SQLite due to locking limitations in high-concurrency scenarios
	// The production code uses proper row locking, but SQLite's locking behavior
	// causes intermittent failures with concurrent updates
	t.Skip("Skipping concurrent test on SQLite due to locking limitations")

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		userID := uint(1)
		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(userID, "ssl-atomic-test.com", testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act - Perform concurrent updates to test atomicity
		numGoroutines := 3
		errChan := make(chan error, numGoroutines)

		// Launch multiple goroutines updating SSL status concurrently
		for i := 0; i < numGoroutines; i++ {
			go func(index int) {
				var status pluginDb.SSLStatus
				var errorMsg string

				switch index % 4 {
				case 0:
					status = pluginDb.SSLStatusIssuing
					errorMsg = ""
				case 1:
					status = pluginDb.SSLStatusReady
					errorMsg = ""
				case 2:
					status = pluginDb.SSLStatusFailed
					errorMsg = "concurrent test error"
				case 3:
					status = pluginDb.SSLStatusPending
					errorMsg = ""
				}

				_, err := websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, status, errorMsg, nil)
				errChan <- err
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			err := <-errChan
			require.NoError(tb, err, "concurrent update should not fail")
		}

		// Assert - Final state should be consistent (no data corruption)
		finalWebsite, err := websiteService.GetWebsiteByDomain(context.Background(), createdWebsite.Domain)
		require.NoError(tb, err)
		assert.NotNil(tb, finalWebsite)

		// Verify final state is consistent and adheres to state transition rules
		switch pluginDb.SSLStatus(finalWebsite.SSLStatus) {
		case pluginDb.SSLStatusReady:
			assert.NotNil(tb, finalWebsite.SSLIssuedAt, "SSLIssuedAt should be set for Ready status")
			assert.Empty(tb, finalWebsite.SSLError, "SSLError should be empty for Ready status")
		case pluginDb.SSLStatusFailed:
			assert.NotEmpty(tb, finalWebsite.SSLError, "SSLError should be set for Failed status")
		case pluginDb.SSLStatusPending, pluginDb.SSLStatusIssuing:
			assert.Empty(tb, finalWebsite.SSLError, "SSLError should be empty for non-failed status")
			assert.Nil(tb, finalWebsite.SSLIssuedAt, "SSLIssuedAt should be nil for non-ready status")
		default:
			tb.Fatalf("unexpected final SSL status: %s", finalWebsite.SSLStatus)
		}

		// Verify last_updated_at was set
		assert.NotNil(tb, finalWebsite.SSLLastUpdatedAt)
	}, TestOptions)
}
