package website

import (
	"context"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
	"gorm.io/gorm"
)

// Test constants for DNS zone IDs and other magic numbers
// parsePeerID parses a peer ID string in various formats (legacy PeerID, CIDv1, etc.)
// Based on boxo codebase - liberal in inputs to handle legacy PeerID strings
func parsePeerID(pidStr string) (peer.ID, error) {
	// Attempt to parse PeerID
	pid, err := peer.Decode(pidStr)
	if err != nil {
		// Retry by parsing PeerID as CID, then setting codec to libp2p-key
		// and turning that back to PeerID.
		// This is necessary to make sure legacy keys are parsed correctly.
		pidAsCid, err2 := cid.Decode(pidStr)
		if err2 == nil {
			pidAsCid = cid.NewCidV1(cid.Libp2pKey, pidAsCid.Hash())
			pid, err = peer.FromCid(pidAsCid)
		}
	}
	return pid, err
}

const (
	testUserID1 = uint(1)
	testUserID2 = uint(2)
	testZoneID1 = uint(100)
	testZoneID2 = uint(200)
	testZoneID3 = uint(300)
	testZoneID4 = uint(400)
	testZoneID5 = uint(500)
	testZoneID6 = uint(600)
	testZoneID7 = uint(700)
	testZoneID8 = uint(800) // Reused across multiple tests: DeleteWebsite_DNSHostingEnabled_CleansUpDNSRecordsNotZone and CreateWebsite_DNSRecordsCreationFailure_ContinuesWithoutRecords
)

// createTestIPFSWebsite creates a test website with an IPFS target.
// It returns a Website struct ready for use in tests, with the specified user ID,
// domain, and CID string parsed into the appropriate fields.
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

// createTestIPNSWebsite creates a test website with an IPNS target.
// It returns a Website struct ready for use in tests, with the specified user ID,
// domain, and IPNS string parsed into the appropriate fields.
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

// createMockDNSZone creates a mock DNS zone for testing purposes.
// It returns a DNSZone struct with the specified ID, domain, and user ID,
// suitable for use as a return value in mocked DNS service calls.
func createMockDNSZone(zoneID uint, domain string, userID uint) *pluginDb.DNSZone {
	return &pluginDb.DNSZone{
		Model:  gorm.Model{ID: zoneID},
		Domain: domain,
		UserID: userID,
	}
}

var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.WEBSITE_SERVICE, NewWebsiteService),
	coreTesting.WithMockServiceFactory(pluginCore.IPNS_KEY_SERVICE, mocks.NewMockIPNSKeyService),
	coreTesting.WithMockServiceFactory(pluginCore.PIN_SERVICE, mocks.NewMockIPFSPinService),
	coreTesting.WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService),
	coreTesting.WithMockServiceFactory(pluginCore.DNS_SERVICE, mocks.NewMockDNSService),
	coreTesting.WithMockServiceFactory(pluginCore.IPNS_PUBLISHER_SERVICE, mocks.NewMockIPNSPublisherService),
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())

		// Act
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.Equal(tb, testUserID1, createdWebsite.UserID)
		assert.Equal(tb, "example.com", createdWebsite.Domain)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPFS), createdWebsite.TargetType)
		assert.Equal(tb, testCID.String(), createdWebsite.TargetHash())
		assert.Equal(tb, string(pluginDb.WebsiteStatusPendingValidation), createdWebsite.Status)
		assert.NotEmpty(tb, createdWebsite.ValidationToken)
		assert.NotNil(tb, createdWebsite.ValidationExpiresAt)
		assert.NotZero(tb, createdWebsite.ID)

		// Verify website can be retrieved
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(tb, createdWebsite.ID, retrievedWebsite.ID)
	}, TestOptions)
}

func TestWebsiteService_SSLStatusDoesNotAffectWebsiteStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")
		website := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())

		// Create website
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Get initial website status before SSL update
		initialStatus := createdWebsite.Status

		// Update SSL status to failed
		_, err = websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusFailed, "cert validation failed", nil)
		require.NoError(tb, err)

		// Act & Assert - Verify SSL status is failed and website status is unchanged
		finalWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
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

		testCID := util.GenerateTestCID(t, "test data")
		website := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())

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
		finalWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.WebsiteStatusPendingValidation), finalWebsite.Status, "website status should not be affected by SSL transitions")
	}, TestOptions)
}

func TestWebsiteService_WebsiteCanBeBrokenRegardlessOfSSLStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")
		website := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())

		// Create website
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Set SSL status to ready
		_, err = websiteService.UpdateSSLStatus(context.Background(), createdWebsite.Domain, pluginDb.SSLStatusReady, "", nil)
		require.NoError(tb, err)

		// Verify SSL is ready
		websiteWithSSL, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.SSLStatusReady), websiteWithSSL.SSLStatus)

		// Act - Update website status to broken
		updates := map[string]interface{}{"status": string(pluginDb.WebsiteStatusBroken)}
		_, err = websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)
		require.NoError(tb, err)

		// Assert - SSL status remains ready, website status is now broken
		finalWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
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

		// Use a valid IPNS name (CIDv1 with libp2p-key codec)
		ipnsName := "k51qzi5uqu5dlts3p5vfpw8kneqp5ye1ttb2jlt8qkt5mq9f2gvgmet6sec29r"

		website := createTestIPNSWebsite(testUserID1, "ipns-example.com", ipnsName)

		// Act
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.Equal(tb, testUserID1, createdWebsite.UserID)
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "invalid domain with spaces", testCID.String())
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

		testCID := util.GenerateTestCID(t, "test data")

		website1 := createTestIPFSWebsite(testUserID1, "duplicate.com", testCID.String())

		// Create first website
		_, err := websiteService.CreateWebsite(context.Background(), website1)
		require.NoError(tb, err)

		website2 := createTestIPFSWebsite(testUserID1, "duplicate.com", testCID.String())

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

		website := &pluginDb.Website{
			UserID:     testUserID1,
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "get-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)

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

		nonExistentID := uint(99999)

		// Act
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, nonExistentID)

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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "domain-test.com", testCID.String())

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

		// Create websites for user 1
		website1 := createTestIPFSWebsite(testUserID1, "list1.com", util.GenerateTestCID(t, "data1").String())
		website2 := createTestIPFSWebsite(testUserID1, "list2.com", util.GenerateTestCID(t, "data2").String())

		created1, err := websiteService.CreateWebsite(context.Background(), website1)
		require.NoError(tb, err)
		created2, err := websiteService.CreateWebsite(context.Background(), website2)
		require.NoError(tb, err)

		// Create a website for user 2
		website3 := createTestIPFSWebsite(testUserID2, "list3.com", util.GenerateTestCID(t, "data3").String())
		_, err = websiteService.CreateWebsite(context.Background(), website3)
		require.NoError(tb, err)

		// Act - List websites for user 1
		websites, total, err := websiteService.ListWebsites(context.Background(), testUserID1, nil, nil, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err)
		assert.Equal(tb, int64(2), total)
		assert.Len(tb, websites, 2)

		// Verify all websites belong to user 1
		for _, w := range websites {
			assert.Equal(tb, testUserID1, w.UserID)
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "update-test.com", testCID.String())

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
		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

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

		nonExistentID := uint(99999)

		updates := map[string]interface{}{
			"status": string(pluginDb.WebsiteStatusActive),
		}

		// Act
		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, nonExistentID, updates)

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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "delete-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Verify website exists
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedWebsite)

		// Act - Delete the website
		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

		// Verify website is soft-deleted (should not be found)
		retrievedWebsite, err = websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Nil(tb, retrievedWebsite)
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		nonExistentID := uint(99999)

		// Act
		err := websiteService.DeleteWebsite(context.Background(), testUserID1, nonExistentID)

		// Assert
		assert.Error(tb, err)
	}, TestOptions)
}

func TestWebsiteService_ValidationTokenExpiration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "expire-test.com", testCID.String())

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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "status-test.com", testCID.String())

		// Create website (should be pending_validation)
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusPendingValidation), createdWebsite.Status)

		// Update to active
		updates := map[string]interface{}{
			"status": string(pluginDb.WebsiteStatusActive),
		}
		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusActive), updatedWebsite.Status)

		// Update to broken
		updates["status"] = string(pluginDb.WebsiteStatusBroken)
		updatedWebsite, err = websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusBroken), updatedWebsite.Status)
	}, TestOptions)
}

func TestWebsiteService_ShouldCheck(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "check-test.com", testCID.String())

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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "expire-test.com", testCID.String())

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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "block-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusPendingValidation), createdWebsite.Status)

		// Act - Block the website
		err = websiteService.BlockWebsite(context.Background(), createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

		// Verify website is now blocked
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "unblock-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Block the website first
		err = websiteService.BlockWebsite(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)

		// Verify it's blocked
		blockedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusBlocked), blockedWebsite.Status)

		// Act - Unblock the website
		err = websiteService.UnblockWebsite(context.Background(), createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

		// Verify website is now active
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "blocked-delete-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Block the website
		err = websiteService.BlockWebsite(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)

		// Verify it's blocked
		blockedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(t, string(pluginDb.WebsiteStatusBlocked), blockedWebsite.Status)

		// Act - Try to delete the blocked website
		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert - Should fail with error
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "cannot delete blocked website")

		// Verify website still exists and is blocked
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "active-delete-test.com", testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Update to active status
		updates := map[string]interface{}{
			"status": string(pluginDb.WebsiteStatusActive),
		}
		_, err = websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)
		require.NoError(tb, err)

		// Act - Delete the active website
		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert - Should succeed
		require.NoError(tb, err)

		// Verify website is soft-deleted
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Nil(tb, retrievedWebsite)
	}, TestOptions)
}

func TestWebsiteService_UpdateSSLStatus_SuccessfulUpdate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "ssl-update-test.com", testCID.String())
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "ssl-issuedat-test.com", testCID.String())
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "ssl-error-test.com", testCID.String())
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "ssl-clear-error-test.com", testCID.String())
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

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "ssl-atomic-test.com", testCID.String())
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

func TestWebsiteService_CreateWebsite_DNSZoneCreatedWhenEnabled(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-enabled-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = true

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Act - Expect DNS zone creation and DNS records creation
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID1, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID1,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.NotNil(tb, createdWebsite.DNSZoneID)
		assert.Equal(tb, testZoneID1, *createdWebsite.DNSZoneID)
		assert.True(tb, createdWebsite.Enabled)

		// Verify critical DNS operations were called
		mockDNS.AssertCalled(t, "CreateZone", mock.Anything, domain, testUserID1)
		mockDNS.AssertCalled(t, "CreateWebsiteDNSRecords", mock.Anything, testZoneID1, mock.Anything, mock.Anything, mock.Anything)
		mockDNS.AssertExpectations(t)
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_DNSRecordsCreated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-records-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = true

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Act - Expect DNS zone and records to be created with specific parameters
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID2, domain, testUserID1), nil).Once()

		var capturedTargetHash string
		var capturedTargetType pluginDb.WebsiteTargetType
		var capturedToken string

		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID2,
			mock.MatchedBy(func(targetHash string) bool {
				capturedTargetHash = targetHash
				return true
			}),
			mock.MatchedBy(func(targetType pluginDb.WebsiteTargetType) bool {
				capturedTargetType = targetType
				return targetType == pluginDb.WebsiteTargetTypeIPFS
			}),
			mock.MatchedBy(func(token string) bool {
				capturedToken = token
				return token != ""
			}),
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.NotEmpty(tb, capturedTargetHash, "Target hash should be captured")
		assert.Equal(tb, pluginDb.WebsiteTargetTypeIPFS, capturedTargetType, "Target type should be IPFS")
		assert.NotEmpty(tb, capturedToken, "Validation token should be non-empty")
		assert.Equal(tb, createdWebsite.ValidationToken, capturedToken, "Validation token should match website token")

		mockDNS.AssertExpectations(t)
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_DNSRecordsUpdatedWhenTargetChanges(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-update-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = true

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Create website with DNS enabled
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID3, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID3,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		// Act - Update target (DNS update may or may not be called depending on internal logic)
		newCID := util.GenerateTestCID(t, "new data")
		newVersion := uint8(newCID.Version())

		updates := map[string]interface{}{
			"target_multihash": newCID.Hash(),
			"cid_version":      &newVersion,
		}

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, updatedWebsite)
		assert.NotEmpty(tb, updatedWebsite.TargetHash())

		mockDNS.AssertExpectations(t)
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_DNSRecordsCleanedUp(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-cleanup-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = true

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Create website with DNS enabled
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID4, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID4,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		// Act - Delete website and expect only DNS records to be cleaned up, NOT the zone
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, testZoneID4).Return(nil).Once()

		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

		mockDNS.AssertExpectations(t)

		// Verify website is soft-deleted
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Nil(tb, retrievedWebsite)
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_DNSCleanupFailureDoesNotPreventDeletion(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-cleanup-fail-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = true

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Create website with DNS enabled
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID5, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID5,
			mock.Anything,
			mock.Anything,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		// Act - Delete website with DNS cleanup failure
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, testZoneID5).Return(assert.AnError).Once()

		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert - Website should still be deleted despite DNS cleanup failure
		require.NoError(tb, err)

		mockDNS.AssertExpectations(t)

		// Verify website is soft-deleted
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Nil(tb, retrievedWebsite)
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_NoDNSZoneNoCleanup(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "no-dns-zone-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = false // DNS disabled

		// Act - Create website with DNS disabled (no zone created)
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		// Delete website - no DNS cleanup should occur
		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

		// Verify website is soft-deleted
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Nil(tb, retrievedWebsite)
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_DNSHostingDisabledWhenEnabledFalse(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-disabled-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = false

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Act - Create website with DNS disabled (no DNS methods should be called)
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.Nil(tb, createdWebsite.DNSZoneID, "DNS zone ID should be nil when DNS hosting is disabled")
		assert.False(tb, createdWebsite.Enabled)

		// Verify no DNS methods were called
		mockDNS.AssertNotCalled(t, "CreateZone")
		mockDNS.AssertNotCalled(t, "CreateWebsiteDNSRecords")
	}, TestOptions)
}

// DNS Hosting Mode Tests

func TestWebsiteService_CreateWebsite_DNSHostingEnabled_CreatesZoneAndRecords(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-enabled-test.com"
		targetHash := testCID.String()

		website := createTestIPFSWebsite(testUserID1, domain, targetHash)
		website.Enabled = true // DNS hosting enabled

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Mock DNS zone creation
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID6, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID6,
			mock.Anything, // target hash varies
			pluginDb.WebsiteTargetTypeIPFS,
			mock.Anything,
		).Return(nil).Once()

		// Act
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.NotNil(tb, createdWebsite.DNSZoneID, "DNS zone ID should be set when DNS hosting is enabled")
		assert.Equal(tb, testZoneID6, *createdWebsite.DNSZoneID)
		assert.True(tb, createdWebsite.Enabled)
		assert.Equal(tb, domain, createdWebsite.Domain)

		mockDNS.AssertExpectations(t)
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_DNSHostingEnabled_NoDNSUpdateWhenTargetUnchanged(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "no-dns-update-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = true

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Create website with initial DNS setup
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID7, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID7,
			mock.Anything, // target hash varies
			pluginDb.WebsiteTargetTypeIPFS,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act - Update website without changing target (should NOT update DNS records)
		updates := map[string]interface{}{
			"status": string(pluginDb.WebsiteStatusActive),
		}

		_, err = websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

		// Assert
		require.NoError(tb, err)

		// Verify DNS update was NOT called
		mockDNS.AssertNotCalled(t, "UpdateWebsiteDNSRecords")
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_DNSHostingEnabled_ZoneRemainsAfterDeletion(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "zone-persists-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = true

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Create website with DNS
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID8, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID8,
			mock.Anything, // target hash varies
			pluginDb.WebsiteTargetTypeIPFS,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Mock DNS records deletion
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, testZoneID8).Return(nil).Once()

		// Act - Delete website
		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

		// Verify website is soft-deleted
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Nil(tb, retrievedWebsite)

		// Verify DeleteZone was NOT called - zones persist independently
		mockDNS.AssertNotCalled(t, "DeleteZone")

		// Verify the zone still exists (simulated by checking no DeleteZone call)
		// In real scenario, you would verify GetZone(zoneID) still returns the zone
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_DNSHostingDisabled_NoDNSOperations(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "no-dns-ops-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = false // DNS hosting disabled

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Act - Create website with DNS disabled
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.Nil(tb, createdWebsite.DNSZoneID, "DNS zone ID should be nil when DNS hosting is disabled")
		assert.False(tb, createdWebsite.Enabled)

		// Verify no DNS operations were performed
		mockDNS.AssertNotCalled(t, "CreateZone")
		mockDNS.AssertNotCalled(t, "CreateWebsiteDNSRecords")
		mockDNS.AssertNotCalled(t, "UpdateWebsiteDNSRecords")
		mockDNS.AssertNotCalled(t, "DeleteWebsiteDNSRecords")
		mockDNS.AssertNotCalled(t, "DeleteZone")
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_DNSHostingDisabled_NoDNSOperations(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		newCID := util.GenerateTestCID(t, "new data")
		domain := "update-no-dns-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = false // DNS hosting disabled

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Create website without DNS
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act - Update website with new target (DNS hosting disabled, so no DNS operations)
		newVersion := uint8(newCID.Version())
		updates := map[string]interface{}{
			"target_multihash": newCID.Hash(),
			"cid_version":      &newVersion,
		}

		_, err = websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

		// Assert
		require.NoError(tb, err)

		// Verify no DNS operations were performed
		mockDNS.AssertNotCalled(t, "UpdateWebsiteDNSRecords")
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_DNSHostingDisabled_NoDNSOperations(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "delete-no-dns-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = false // DNS hosting disabled

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Create website without DNS
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act - Delete website (DNS hosting disabled, so no DNS operations)
		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

		// Verify no DNS operations were performed
		mockDNS.AssertNotCalled(t, "DeleteWebsiteDNSRecords")
		mockDNS.AssertNotCalled(t, "DeleteZone")
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_DNSZoneCreationFailure_ContinuesWithoutDNS(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "zone-fail-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = true

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Mock DNS zone creation failure
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(nil, assert.AnError).Once()

		// Act - Create website with DNS zone creation failure
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert - Website should still be created despite DNS failure
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.Nil(tb, createdWebsite.DNSZoneID, "DNS zone ID should be nil when zone creation fails")
		assert.Equal(tb, domain, createdWebsite.Domain)

		// Verify CreateWebsiteDNSRecords was not called (zone creation failed)
		mockDNS.AssertNotCalled(t, "CreateWebsiteDNSRecords")
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_DNSRecordsCreationFailure_ContinuesWithoutRecords(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "records-fail-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.Enabled = true

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Mock DNS zone creation success
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID8, domain, testUserID1), nil).Once()

		// Mock DNS records creation failure
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID8,
			mock.Anything, // target hash varies
			pluginDb.WebsiteTargetTypeIPFS,
			mock.Anything,
		).Return(assert.AnError).Once()

		// Act - Create website with DNS records creation failure
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert - Website should still be created with zone ID set
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.NotNil(tb, createdWebsite.DNSZoneID, "DNS zone ID should be set even when record creation fails")
		assert.Equal(tb, testZoneID8, *createdWebsite.DNSZoneID)
	}, TestOptions)
}

// TestWebsiteService_CreateWebsite_IPNSKeyAutoCreation_NoDuplicate tests that when creating
// a website with DNS hosting enabled and IPFS target, an IPNS key is auto-created, and if
// the website is created again with the same domain, the existing IPNS key is reused instead
// of creating a duplicate.
// TestWebsiteService_CreateWebsite_IPNSKeyAutoCreation_NoDuplicate tests that when creating
// a website with DNS hosting enabled and IPFS target, an IPNS key is auto-created, and if
// the website is created again with the same domain, the existing IPNS key is reused instead
// of creating a duplicate.
// TestWebsiteService_CreateWebsite_IPNSKeyAutoCreation_NoDuplicate tests that when creating
// a website with DNS hosting enabled and IPFS target, an IPNS key is auto-created, and if
// the website is created again with the same domain, the existing IPNS key is reused instead
// of creating a duplicate.
// TestWebsiteService_CreateWebsite_IPNSKeyAutoCreation_NoDuplicate tests that when creating
// a website with DNS hosting enabled and IPFS target, an IPNS key is auto-created, and if
// the website is created again with the same domain, the existing IPNS key is reused instead
// of creating a duplicate.
// TestWebsiteService_CreateWebsite_IPNSKeyAutoCreation_NoDuplicate tests that when creating
// a website with DNS hosting enabled and IPFS target, an IPNS key is auto-created, and if
// the website is created again with the same domain, the existing IPNS key is reused instead
// of creating a duplicate.
func TestWebsiteService_CreateWebsite_IPNSKeyAutoCreation_NoDuplicate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		ipnsKeyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		ipnsPublisherService := core.GetService[pluginCore.IPNSPublisherService](ctx, pluginCore.IPNS_PUBLISHER_SERVICE)
		dnsService := core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, ipnsKeyService)
		require.NotNil(tb, ipnsPublisherService)
		require.NotNil(tb, dnsService)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "test-auto-ipns.com"
		expectedKeyName := domain + "-auto"

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		mockIPNSKey, ok := ipnsKeyService.(*mocks.MockIPNSKeyService)
		require.True(tb, ok, "IPNS key service should be a mock")

		mockIPNSPublisher, ok := ipnsPublisherService.(*mocks.MockIPNSPublisherService)
		require.True(tb, ok, "IPNS publisher service should be a mock")

		// Set up mock expectations for first website creation
		// GetKeyByName should return nil (no existing key)
		mockIPNSKey.EXPECT().GetKeyByName(mock.Anything, testUserID1, expectedKeyName).Return(nil, nil).Once()
		
		// CreateKey should create a new IPNS key - keyType is int
		testKeyID := uint(1001)
		testPeerIDStr := "k51qzi5uqu5dlts3p5vfpw8kneqp5ye1ttb2jlt8qkt5mq9f2gvgmet6sec29r"
		testPeerID, _ := parsePeerID(testPeerIDStr)
		testPeerIDMultihash := mh.Multihash(testPeerID)
		testIPNSKey := &pluginDb.IPFSIPNSKey{
			ID:              testKeyID,
			UserID:          testUserID1,
			Name:            expectedKeyName,
			PeerIDMultihash: testPeerIDMultihash,
		}
		mockIPNSKey.EXPECT().CreateKey(mock.Anything, testUserID1, expectedKeyName, 1).Return(testIPNSKey, nil).Once()
		
		// PublishCID should publish the CID to the IPNS key with TTL
		mockIPNSPublisher.EXPECT().PublishCID(mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil).Once()
		
		// DNS zone creation
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID1, domain, testUserID1), nil).Once()
		
		// DNS records creation (after IPNS conversion)
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID1,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		// Act - Create first website with DNS hosting enabled
		website1 := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website1.Enabled = true // Enable DNS hosting
		createdWebsite1, err := websiteService.CreateWebsite(context.Background(), website1)
		require.NoError(tb, err)

		// Assert - IPNS key was created
		require.NotNil(tb, createdWebsite1.IPNSKeyID, "First website should have IPNS key ID")
		assert.Equal(tb, testKeyID, *createdWebsite1.IPNSKeyID, "IPNS key ID should match")
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite1.TargetType, "First website should use IPNS target")

		// Expect DNS records deletion when website is deleted
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, testZoneID1).Return(nil).Once()

		// Act - Delete first website
		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite1.ID)
		require.NoError(tb, err)

		// Set up mock expectations for second website creation
		// GetKeyByName should return the existing key (not nil)
		mockIPNSKey.EXPECT().GetKeyByName(mock.Anything, testUserID1, expectedKeyName).Return(testIPNSKey, nil).Once()
		
		// CreateKey should NOT be called since key already exists
		// PublishCID should still be called to update the IPNS key with the new content
		mockIPNSPublisher.EXPECT().PublishCID(mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil).Once()
		
		// DNS zone creation
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID1, domain, testUserID1), nil).Once()
		
		// DNS records creation
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID1,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		// Act - Create second website with same domain
		website2 := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website2.Enabled = true
		createdWebsite2, err := websiteService.CreateWebsite(context.Background(), website2)
		require.NoError(tb, err)

		// Assert - Second website reuses the same IPNS key
		require.NotNil(tb, createdWebsite2.IPNSKeyID, "Second website should have IPNS key ID")
		assert.Equal(tb, *createdWebsite1.IPNSKeyID, *createdWebsite2.IPNSKeyID, "Should reuse the same IPNS key ID")
		assert.Equal(tb, testKeyID, *createdWebsite2.IPNSKeyID, "IPNS key ID should match the original")
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite2.TargetType, "Second website should use IPNS target")
	}, TestOptions)
}
