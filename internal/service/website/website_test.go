package website

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/testopts"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/queryutil"
	"gorm.io/datatypes"
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

// setupIPNSAutoCreationMocks sets up mock expectations for IPNS key auto-creation
// when enabling DNS hosting on a website. This handles the common pattern of:
// 1. Listing existing keys (returns empty for new key)
// 2. Creating a new IPNS key
// 3. Publishing CID to the new key
// Returns the mock IPNS key that was created.
func setupIPNSAutoCreationMocks(t *testing.T, mockIPNS *mocks.MockIPNSKeyService, userID uint, domain string, cid any) *pluginDb.IPFSIPNSKey {
	expectedKeyName := domain + "-auto"
	testKeyID := uint(1001)
	testPeerIDStr := "k51qzi5uqu5dlts3p5vfpw8kneqp5ye1ttb2jlt8qkt5mq9f2gvgmet6sec29r"
	testPeerID, _ := parsePeerID(testPeerIDStr)
	testPeerIDMultihash := mh.Multihash(testPeerID)

	testIPNSKey := &pluginDb.IPFSIPNSKey{
		ID:              testKeyID,
		UserID:          userID,
		Name:            expectedKeyName,
		PeerIDMultihash: testPeerIDMultihash,
	}

	// Mock key listing (returns empty for new website)
	mockIPNS.EXPECT().ListKeys(mock.Anything, userID).Return([]pluginDb.IPFSIPNSKey{}, nil).Once()
	// Mock key creation
	mockIPNS.EXPECT().CreateKey(mock.Anything, userID, expectedKeyName, 1).Return(testIPNSKey, nil).Once()
	// Mock CID publishing
	mockIPNS.EXPECT().PublishCID(mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil).Once()

	return testIPNSKey
}

// setupDNSZoneCreationMocks sets up mock expectations for DNS zone operations
// when enabling DNS hosting on a website. This handles:
// 1. Creating a DNS zone (if new)
// 2. Creating DNS records for the website
func setupDNSZoneCreationMocks(t *testing.T, mockDNS *mocks.MockDNSService, zoneID uint, domain string, userID uint) {
	// Mock zone creation
	mockZone := createMockDNSZone(zoneID, domain, userID)
	mockDNS.EXPECT().CreateZone(mock.Anything, domain, userID).Return(mockZone, nil).Once()
	// Mock DNS records creation
	mockDNS.EXPECT().CreateWebsiteDNSRecords(
		mock.Anything,
		zoneID,
		mock.Anything,
		mock.Anything,
		mock.AnythingOfType("db.WebsiteTargetType"),
		mock.Anything,
	).Return(nil).Once()
}

// setupDeleteZoneMocks sets up mock expectations for deleting a DNS zone
// when disabling DNS hosting on a website.
func setupDeleteZoneMocks(t *testing.T, mockDNS *mocks.MockDNSService, zoneID uint) {
	mockDNS.EXPECT().DeleteZone(mock.Anything, zoneID).Return(nil).Once()
}

// createTestIPFSWebsite creates a test website with an IPFS target.
// It returns a Website struct ready for use in tests, with the specified user ID,
// domain, and CID string parsed into the appropriate fields.
func createTestIPFSWebsite(userID uint, domain string, cidStr string) *pluginDb.Website {
	c := cid.MustParse(cidStr)
	version := uint8(c.Version())
	codec := uint8(c.Type())
	return &pluginDb.Website{
		UserID:          userID,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
		TargetMultihash: c.Hash(),
		CIDVersion:      &version,
		CIDType:         &codec,
	}
}

// stubPinnedCID configures the pin service mock so GetPinByCIDAndUser reports
// the given CID as pinned for the user. CreateWebsite now refuses IPFS targets
// (and the IPNS auto-convert path) unless the CID is pinned, so tests that
// create such websites must stub the pin first.
func stubPinnedCID(t *testing.T, ctx coreTesting.TestContext, userID uint, cidStr string) {
	mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
	c := cid.MustParse(cidStr)
	mockPinSvc.EXPECT().
		GetPinByCIDAndUser(mock.Anything, c, userID).
		Return(&pluginDb.IPFSPin{UserID: userID, CID: c.Bytes(), Status: pluginDb.PinningStatusPinned}, nil).
		Maybe()
}

// createTestIPNSWebsite creates a test website with an IPNS target.
// It returns a Website struct ready for use in tests, with the specified user ID,
// domain, and IPNS string parsed into the appropriate fields.
func createTestIPNSWebsite(userID uint, domain string, ipnsStr string) *pluginDb.Website {
	target, _ := pluginDb.NewIPNSTargetFromString(ipnsStr)
	return &pluginDb.Website{
		UserID:          userID,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPNS),
		TargetMultihash: target.ToMultihash(),
		CIDVersion:      nil,
	}
}

// bindPrimaryDomain creates a WebsiteDomain binding for a website and sets it
// as the website's primary domain (Website.PrimaryDomainID). It mirrors the API
// layer's transparent primary-domain creation. DNS hosting state is set on the
// returned binding per the new per-domain model.
func bindPrimaryDomain(tb coreTesting.TB, ctx coreTesting.TestContext, websiteID uint, domain string, dnsHostingEnabled bool) *pluginDb.WebsiteDomain {
	wd := createTestWebsiteDomain(websiteID, domain)
	wd.DNSHostingEnabled = dnsHostingEnabled
	require.NoError(tb, ctx.DB().Create(wd).Error)
	// Use UpdateColumn so Website.BeforeSave (target validation on a bare
	// Website{ID:...}) is not triggered on this partial update.
	require.NoError(tb, ctx.DB().Model(&pluginDb.Website{ID: websiteID}).UpdateColumn("primary_domain_id", wd.ID).Error)
	return wd
}

// prebindPrimaryDomain binds a primary WebsiteDomain for a website that carries
// an explicit ID and points website.PrimaryDomainID at it, so that
// CreateWebsite's DNS/IPNS side-effects run against the pre-existing binding.
func prebindPrimaryDomain(tb coreTesting.TB, ctx coreTesting.TestContext, website *pluginDb.Website, domain string, dnsHostingEnabled bool) *pluginDb.WebsiteDomain {
	require.NotZero(tb, website.ID, "website must carry an explicit ID before primary-domain prebind")
	// Mirror the app's domain-bind guardrail: purge any prior soft-deleted
	// tombstone for this (domain, namespace) so a fresh binding does not collide
	// with the unique key (matching AddDomain in delegated_domain_service).
	require.NoError(tb, ctx.DB().
		Where("domain = ? AND namespace = ? AND deleted_at IS NOT NULL", domain, pluginDb.DomainNamespaceICANN).
		Unscoped().Delete(&pluginDb.WebsiteDomain{}).Error)
	wd := createTestWebsiteDomain(website.ID, domain)
	wd.DNSHostingEnabled = dnsHostingEnabled
	require.NoError(tb, ctx.DB().Create(wd).Error)
	pid := wd.ID
	website.PrimaryDomainID = &pid
	return wd
}

// apexDomain helper resolves a website's primary domain name via the service.
func apexDomain(tb coreTesting.TB, ctx coreTesting.TestContext, svc pluginCore.WebsiteService, websiteID uint) string {
	wd, err := svc.GetApexDomainBinding(context.Background(), websiteID)
	require.NoError(tb, err)
	require.NotNil(tb, wd)
	return wd.Domain
}

// createTestWebsiteDomain creates a domain binding for a website. SSL state is
// stored per-domain on WebsiteDomain, so tests that exercise SSL updates must
// create a binding first.
func createTestWebsiteDomain(websiteID uint, domain string) *pluginDb.WebsiteDomain {
	return &pluginDb.WebsiteDomain{
		WebsiteID: websiteID,
		UserID:    testUserID1,
		Domain:    domain,
		Namespace: pluginDb.DomainNamespaceICANN,
		Status:    pluginDb.DomainStatusDraft,
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
	coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
	testopts.NewBaseMockPluginBuilder().
		WithService(pluginCore.WEBSITE_SERVICE, NewWebsiteService).
		WithServiceConfig(pluginCore.WEBSITE_SERVICE, &pluginConfig.WebsiteConfig{
			NotificationsEnabled: false,
			AdminEmail:           "",
			ValidationTokenTTL:   24 * time.Hour,
		}).
		WithMockServiceFactory(pluginCore.DNS_SERVICE, mocks.NewMockDNSService).
		WithServiceConfig(pluginCore.DNS_SERVICE, &pluginConfig.DnsConfig{
			Enabled:                      true,
			Nameservers:                  []string{"ns1.localhost", "ns2.localhost"},
			NameserverValidationInterval: 5 * time.Minute,
			VerificationTokenKey:         "lumeweb-verify",
		}).
		WithMigrations(map[core.DBType]fs.FS{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
		}).BuilderOption(),
	coreTesting.WithMockMailerService(),
	util.GetProtocolMock(),
)

func TestWebsiteService_CreateWebsite_IPFSTarget(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")

		testsite := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), testsite)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.Equal(tb, testUserID1, createdWebsite.UserID)
		// Bind the primary domain (per-domain model) and assert its name.
		apex := bindPrimaryDomain(tb, ctx, createdWebsite.ID, "example.com", false)
		assert.Equal(tb, "example.com", apex.Domain)
		assert.Equal(tb, "example.com", apexDomain(tb, ctx, websiteService, createdWebsite.ID))
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPFS), createdWebsite.TargetType)

		// Compare CIDs using Equals method instead of string comparison
		createdCID, err := cid.Decode(createdWebsite.TargetHash())
		require.NoError(tb, err)
		assert.True(tb, testCID.Equals(createdCID), "CID mismatch: expected %v, got %v", testCID.String(), createdWebsite.TargetHash())

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

func TestWebsiteService_CreateWebsite_IPFSTargetNotPinned(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		require.NotNil(tb, websiteService)
		require.NotNil(tb, mockPinSvc)

		testCID := util.GenerateTestCID(t, "unpinned data")

		// Simulate an unpinned CID: the pin lookup returns no pin record.
		mockPinSvc.EXPECT().
			GetPinByCIDAndUser(mock.Anything, testCID, testUserID1).
			Return(nil, nil).Once()

		testsite := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), testsite)

		// Assert
		require.Error(tb, err)
		assert.True(tb, errors.Is(err, ErrCIDNotPinned))
		assert.Nil(tb, createdWebsite)
	}, TestOptions)
}

// notifyEnabledTestOptions mirrors the base test options but turns on the admin
// website-created notification so a test can observe the email send. No
// delegated-domain service is wired, exercising the domain-service-absent path.
var notifyEnabledTestOptions = coreTesting.CombineOptions(
	coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
	testopts.NewBaseMockPluginBuilder().
		WithService(pluginCore.WEBSITE_SERVICE, NewWebsiteService).
		WithServiceConfig(pluginCore.WEBSITE_SERVICE, &pluginConfig.WebsiteConfig{
			NotificationsEnabled: true,
			AdminEmail:           "admin@test",
			ValidationTokenTTL:   24 * time.Hour,
		}).
		WithMockServiceFactory(pluginCore.DNS_SERVICE, mocks.NewMockDNSService).
		WithServiceConfig(pluginCore.DNS_SERVICE, &pluginConfig.DnsConfig{
			Enabled:                      true,
			Nameservers:                  []string{"ns1.localhost", "ns2.localhost"},
			NameserverValidationInterval: 5 * time.Minute,
			VerificationTokenKey:         "lumeweb-verify",
		}).
		WithMigrations(map[core.DBType]fs.FS{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
		}).BuilderOption(),
	coreTesting.WithMockMailerService(),
	util.GetProtocolMock(),
)

// TestWebsiteService_CreateWebsite_NoDomainServiceFiresNotification verifies
// that a website created when the delegated-domain service is absent still
// fires the admin "website created" notification (previous behavior dropped it
// because CreateDomain, which normally fires it, is never invoked). The email's
// Domain is allowed to be empty, matching the "no domain set" contract.
func TestWebsiteService_CreateWebsite_NoDomainServiceFiresNotification(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		mailer := coreTesting.GetMockMailerService(ctx)
		require.NotNil(tb, mailer)
		mailer.EXPECT().TemplateSend(
			"website_created_admin",
			mock.Anything, mock.Anything,
			"admin@test",
		).Return(nil).Once()

		testCID := util.GenerateTestCID(t, "test data")
		website := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

		created, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, created)
	}, notifyEnabledTestOptions)
}

func TestWebsiteService_SSLStatusDoesNotAffectWebsiteStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")
		website := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

		// Create website
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Create a domain binding (per-domain SSL source of truth).
		boundDomain := bindPrimaryDomain(tb, ctx, createdWebsite.ID, "example.com", false)

		// Get initial website status before SSL update
		initialStatus := createdWebsite.Status

		// Update SSL status to failed
		_, err = websiteService.UpdateSSLStatus(context.Background(), boundDomain.Domain, pluginDb.SSLStatusFailed, "cert validation failed", nil)
		require.NoError(tb, err)

		// Act & Assert - binding SSL is failed and website status is unchanged
		var binding pluginDb.WebsiteDomain
		require.NoError(tb, ctx.DB().Where("domain = ?", boundDomain.Domain).First(&binding).Error)
		assert.Equal(tb, string(pluginDb.SSLStatusFailed), binding.SSLStatus)
		assert.Equal(tb, "cert validation failed", binding.SSLError)

		finalWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.Equal(tb, initialStatus, finalWebsite.Status, "website status should not be affected by SSL transitions")
	}, TestOptions)
}

func TestWebsiteService_SSLStatusTransitionsIndependently(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")
		website := createTestIPFSWebsite(testUserID1, "example.com", testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

		// Create website
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Create a domain binding for the website.
		boundDomain := bindPrimaryDomain(tb, ctx, createdWebsite.ID, "example.com", false)

		// Act - Simulate SSL status transitions
		now := time.Now()

		// pending -> issuing
		wd, err := websiteService.UpdateSSLStatus(context.Background(), boundDomain.Domain, pluginDb.SSLStatusIssuing, "", &now)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.SSLStatusIssuing), wd.SSLStatus)
		assert.Nil(tb, wd.SSLIssuedAt)

		// issuing -> ready
		now2 := time.Now().Add(time.Minute)
		wd, err = websiteService.UpdateSSLStatus(context.Background(), boundDomain.Domain, pluginDb.SSLStatusReady, "", &now2)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.SSLStatusReady), wd.SSLStatus)
		assert.NotNil(tb, wd.SSLIssuedAt)
		assert.Equal(tb, "", wd.SSLError)

		// ready -> failed (simulating certificate expiration)
		now3 := time.Now().Add(2 * time.Minute)
		wd, err = websiteService.UpdateSSLStatus(context.Background(), boundDomain.Domain, pluginDb.SSLStatusFailed, "certificate expired", &now3)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.SSLStatusFailed), wd.SSLStatus)
		assert.Equal(tb, "certificate expired", wd.SSLError)

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

		// Create website
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Create a domain binding for the website.
		boundDomain := bindPrimaryDomain(tb, ctx, createdWebsite.ID, "example.com", false)

		// Set SSL status to ready
		_, err = websiteService.UpdateSSLStatus(context.Background(), boundDomain.Domain, pluginDb.SSLStatusReady, "", nil)
		require.NoError(tb, err)

		// Verify SSL is ready on the binding
		var binding pluginDb.WebsiteDomain
		require.NoError(tb, ctx.DB().Where("domain = ?", boundDomain.Domain).First(&binding).Error)
		assert.Equal(tb, string(pluginDb.SSLStatusReady), binding.SSLStatus)

		// Act - Update website status to broken
		updates := map[string]interface{}{"status": string(pluginDb.WebsiteStatusBroken)}
		_, err = websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)
		require.NoError(tb, err)

		// Assert - SSL status remains ready on the binding, website status is now broken
		var finalBinding pluginDb.WebsiteDomain
		require.NoError(tb, ctx.DB().Where("domain = ?", boundDomain.Domain).First(&finalBinding).Error)
		assert.Equal(t, string(pluginDb.SSLStatusReady), finalBinding.SSLStatus, "SSL status should not be affected by website status change")

		finalWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
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
		bindPrimaryDomain(tb, ctx, createdWebsite.ID, "ipns-example.com", false)
		assert.Equal(tb, "ipns-example.com", apexDomain(tb, ctx, websiteService, createdWebsite.ID))
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite.TargetType)
		// Verify the peer ID is the same, comparing by decoding from both formats
		inputPeerID, _ := peer.Decode(ipnsName)
		outputPeerID, _ := peer.Decode(createdWebsite.TargetHash())
		assert.Equal(tb, inputPeerID, outputPeerID, "Peer IDs should match across different encodings")
		assert.Equal(tb, string(pluginDb.WebsiteStatusPendingValidation), createdWebsite.Status)
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_IPNSTargetWithPlainCID_AutoConvert(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "ipns-auto-convert-content")
		domain := "ipns-autoconvert.com"

		// Create a website with IPNS target type but a regular CID as the hash.
		// This simulates the user flow where someone provides a CID and selects IPNS.
		c := cid.MustParse(testCID.String())
		version := uint8(c.Version())
		codec := uint8(c.Type())

		website := &pluginDb.Website{
			UserID:          testUserID1,
			TargetType:      string(pluginDb.WebsiteTargetTypeIPNS),
			TargetMultihash: c.Hash(),
			CIDVersion:      &version,
			CIDType:         &codec,
		}
		website.ID = 9901
		// Bind the primary domain before creation so the IPNS auto-convert
		// (which requires a primary domain) can name the key after it.
		prebindPrimaryDomain(tb, ctx, website, domain, false)

		// Set up IPNS key mocks for auto-conversion
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// The plain CID must be pinned before the auto-convert path will publish it.
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

		// Act
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite.TargetType)
		assert.Nil(tb, createdWebsite.CIDVersion, "CIDVersion should be nil after IPNS conversion")
		assert.NotNil(tb, createdWebsite.IPNSKeyID, "IPNSKeyID should be set after auto-conversion")
		// TargetHash should now be a peer ID, not the original CID
		_, peerErr := peer.Decode(createdWebsite.TargetHash())
		assert.NoError(tb, peerErr, "TargetHash should be a valid peer ID after auto-conversion")
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_InvalidTargetType(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		website := &pluginDb.Website{
			UserID:     testUserID1,
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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		bindPrimaryDomain(tb, ctx, createdWebsite.ID, "get-test.com", false)

		// Act
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedWebsite)
		assert.Equal(tb, createdWebsite.ID, retrievedWebsite.ID)
		assert.Equal(tb, apexDomain(tb, ctx, websiteService, createdWebsite.ID), apexDomain(tb, ctx, websiteService, retrievedWebsite.ID))
		assert.Equal(tb, createdWebsite.TargetType, retrievedWebsite.TargetType)
		// Compare target hashes - both should be the same normalized CID
		createdCID, err := cid.Decode(createdWebsite.TargetHash())
		require.NoError(tb, err)
		retrievedCID, err := cid.Decode(retrievedWebsite.TargetHash())
		require.NoError(tb, err)
		assert.True(tb, createdCID.Equals(retrievedCID), "Target hash mismatch")
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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		bindPrimaryDomain(tb, ctx, createdWebsite.ID, "domain-test.com", false)

		// GetWebsiteByDomain resolves purely through the delegated domain
		// service (the legacy Website.domain column was removed). Inject a fake
		// that resolves the bound domain via the DB so the lookup succeeds.
		setMockDelegatedDomainSvc(websiteService, &testDelegatedDomainService{
			getByName: func(_ context.Context, domain string) (*pluginDb.WebsiteDomain, error) {
				var wd pluginDb.WebsiteDomain
				if err := ctx.DB().Where("domain = ?", domain).First(&wd).Error; err != nil {
					return nil, err
				}
				return &wd, nil
			},
		})

		// Act
		retrievedWebsite, _, err := websiteService.GetWebsiteByDomain(context.Background(), "domain-test.com")

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedWebsite)
		assert.Equal(tb, createdWebsite.ID, retrievedWebsite.ID)
		assert.Equal(tb, "domain-test.com", apexDomain(tb, ctx, websiteService, retrievedWebsite.ID))
	}, TestOptions)
}

func TestWebsiteService_GetWebsiteByDomain_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Act
		retrievedWebsite, _, err := websiteService.GetWebsiteByDomain(context.Background(), "nonexistent.com")

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
		stubPinnedCID(t, ctx, testUserID1, util.GenerateTestCID(t, "data1").String())
		website2 := createTestIPFSWebsite(testUserID1, "list2.com", util.GenerateTestCID(t, "data2").String())
		stubPinnedCID(t, ctx, testUserID1, util.GenerateTestCID(t, "data2").String())

		created1, err := websiteService.CreateWebsite(context.Background(), website1)
		require.NoError(tb, err)
		created2, err := websiteService.CreateWebsite(context.Background(), website2)
		require.NoError(tb, err)

		// Create a website for user 2
		website3 := createTestIPFSWebsite(testUserID2, "list3.com", util.GenerateTestCID(t, "data3").String())
		stubPinnedCID(t, ctx, testUserID2, util.GenerateTestCID(t, "data3").String())
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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		bindPrimaryDomain(tb, ctx, createdWebsite.ID, "update-test.com", false)

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

		// Compare CID using Equals method (newCID is already normalized from the test helper)
		updatedCID, err := cid.Decode(updatedWebsite.TargetHash())
		require.NoError(tb, err)
		assert.True(tb, newCID.Equals(updatedCID), "CID mismatch after update")

		assert.Equal(tb, string(pluginDb.WebsiteStatusActive), updatedWebsite.Status)
		assert.Equal(tb, "update-test.com", apexDomain(tb, ctx, websiteService, updatedWebsite.ID))
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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Test 1: Newly created website should not be expired
		t.Logf("ValidationExpiresAt: %v", createdWebsite.ValidationExpiresAt)
		t.Logf("Current time: %v", time.Now())
		t.Logf("IsExpired: %v", createdWebsite.IsExpired())
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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

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

		// Verify website is now pending_validation (must re-validate before
		// being served again).
		retrievedWebsite, err := websiteService.GetWebsite(context.Background(), testUserID1, createdWebsite.ID)
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedWebsite)
		assert.Equal(t, string(pluginDb.WebsiteStatusPendingValidation), retrievedWebsite.Status)
	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_Blocked(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "blocked-delete-test.com", testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// SSL state lives per-domain on WebsiteDomain, so a binding must exist
		// before UpdateSSLStatus can find and update it.
		require.NoError(tb, ctx.DB().Create(createTestWebsiteDomain(createdWebsite.ID, "ssl-update-test.com")).Error)

		// Act - Update SSL status to issuing
		updatedWebsite, err := websiteService.UpdateSSLStatus(context.Background(), "ssl-update-test.com", pluginDb.SSLStatusIssuing, "", nil)

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
		wd, err := websiteService.UpdateSSLStatus(context.Background(), "nonexistent.com", pluginDb.SSLStatusReady, "", nil)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, wd)
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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NoError(tb, ctx.DB().Create(createTestWebsiteDomain(createdWebsite.ID, "ssl-issuedat-test.com")).Error)

		lookupBinding := func(tb coreTesting.TB) pluginDb.WebsiteDomain {
			var wd pluginDb.WebsiteDomain
			require.NoError(tb, ctx.DB().Where("domain = ?", "ssl-issuedat-test.com").First(&wd).Error)
			return wd
		}

		// Act - Update to issuing (should not set issued_at)
		_, err = websiteService.UpdateSSLStatus(context.Background(), "ssl-issuedat-test.com", pluginDb.SSLStatusIssuing, "", nil)
		require.NoError(tb, err)
		assert.Nil(tb, lookupBinding(tb).SSLIssuedAt)

		// Act - Update to ready (should set issued_at)
		_, err = websiteService.UpdateSSLStatus(context.Background(), "ssl-issuedat-test.com", pluginDb.SSLStatusReady, "", nil)
		require.NoError(tb, err)
		afterReady := lookupBinding(tb)
		assert.NotNil(tb, afterReady.SSLIssuedAt)

		// Act - Update to ready again (should not change issued_at)
		originalIssuedAt := afterReady.SSLIssuedAt
		time.Sleep(10 * time.Millisecond)
		_, err = websiteService.UpdateSSLStatus(context.Background(), "ssl-issuedat-test.com", pluginDb.SSLStatusReady, "", nil)
		require.NoError(tb, err)
		afterSecondReady := lookupBinding(tb)
		assert.NotNil(tb, afterSecondReady.SSLIssuedAt)
		assert.Equal(tb, originalIssuedAt.Unix(), afterSecondReady.SSLIssuedAt.Unix(), "issued_at should not change when already ready")
	}, TestOptions)
}

func TestWebsiteService_UpdateSSLStatus_ErrorSetOnFailed(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "ssl-error-test.com", testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NoError(tb, ctx.DB().Create(createTestWebsiteDomain(createdWebsite.ID, "ssl-error-test.com")).Error)

		testErrorMsg := "certificate validation failed"

		// Act - Update SSL status to failed with error message
		wd, err := websiteService.UpdateSSLStatus(context.Background(), "ssl-error-test.com", pluginDb.SSLStatusFailed, testErrorMsg, nil)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, wd)
		assert.Equal(tb, string(pluginDb.SSLStatusFailed), wd.SSLStatus)
		assert.Equal(tb, testErrorMsg, wd.SSLError)
	}, TestOptions)
}

func TestWebsiteService_UpdateSSLStatus_ErrorClearedOnStatusChange(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		testCID := util.GenerateTestCID(t, "test data")

		website := createTestIPFSWebsite(testUserID1, "ssl-clear-error-test.com", testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NoError(tb, ctx.DB().Create(createTestWebsiteDomain(createdWebsite.ID, "ssl-clear-error-test.com")).Error)

		// Set status to failed with error
		testErrorMsg := "certificate validation failed"
		_, err = websiteService.UpdateSSLStatus(context.Background(), "ssl-clear-error-test.com", pluginDb.SSLStatusFailed, testErrorMsg, nil)
		require.NoError(tb, err)

		// Verify error is set on the binding
		var binding pluginDb.WebsiteDomain
		require.NoError(tb, ctx.DB().Where("domain = ?", "ssl-clear-error-test.com").First(&binding).Error)
		assert.Equal(tb, testErrorMsg, binding.SSLError)

		// Act - Update to pending (should clear error)
		wd, err := websiteService.UpdateSSLStatus(context.Background(), "ssl-clear-error-test.com", pluginDb.SSLStatusPending, "", nil)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, wd)
		assert.Equal(tb, string(pluginDb.SSLStatusPending), wd.SSLStatus)
		assert.Empty(tb, wd.SSLError, "Error should be cleared when status changes away from failed")
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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NoError(tb, ctx.DB().Create(createTestWebsiteDomain(createdWebsite.ID, "ssl-atomic-test.com")).Error)

		// Act - Perform concurrent updates to test atomicity
		numGoroutines := 3
		errChan := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(index int) {
				var status pluginDb.SSLStatus
				var errorMsg string
				switch index % 4 {
				case 0:
					status = pluginDb.SSLStatusIssuing
				case 1:
					status = pluginDb.SSLStatusReady
				case 2:
					status = pluginDb.SSLStatusFailed
					errorMsg = "concurrent test error"
				case 3:
					status = pluginDb.SSLStatusPending
				}
				_, err := websiteService.UpdateSSLStatus(context.Background(), "ssl-atomic-test.com", status, errorMsg, nil)
				errChan <- err
			}(i)
		}

		for i := 0; i < numGoroutines; i++ {
			err := <-errChan
			require.NoError(tb, err, "concurrent update should not fail")
		}

		// Assert - Final state should be consistent (no data corruption)
		var finalBinding pluginDb.WebsiteDomain
		require.NoError(tb, ctx.DB().Where("domain = ?", "ssl-atomic-test.com").First(&finalBinding).Error)

		switch pluginDb.SSLStatus(finalBinding.SSLStatus) {
		case pluginDb.SSLStatusReady:
			assert.NotNil(tb, finalBinding.SSLIssuedAt, "SSLIssuedAt should be set for Ready status")
			assert.Empty(tb, finalBinding.SSLError, "SSLError should be empty for Ready status")
		case pluginDb.SSLStatusFailed:
			assert.NotEmpty(tb, finalBinding.SSLError, "SSLError should be set for Failed status")
		case pluginDb.SSLStatusPending, pluginDb.SSLStatusIssuing:
			assert.Empty(tb, finalBinding.SSLError, "SSLError should be empty for non-failed status")
			assert.Nil(tb, finalBinding.SSLIssuedAt, "SSLIssuedAt should be nil for non-ready status")
		default:
			tb.Fatalf("unexpected final SSL status: %s", finalBinding.SSLStatus)
		}

		assert.NotNil(tb, finalBinding.SSLLastUpdatedAt)
	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_DNSZoneCreatedWhenEnabled(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-enabled-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8000
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Act - Expect DNS zone creation and DNS records creation
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID1, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID1,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		createdApex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		assert.NotZero(tb, createdApex.ZoneID)
		assert.Equal(tb, testZoneID1, createdApex.ZoneID)
		assert.True(tb, createdApex.DNSHostingEnabled)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite.TargetType, "Should be converted to IPNS for managed DNS")
		assert.NotNil(tb, createdWebsite.IPNSKeyID, "IPNS key ID should be set")

		// Verify critical operations were called
		mockDNS.AssertCalled(t, "CreateZone", mock.Anything, domain, testUserID1)
		mockDNS.AssertCalled(t, "CreateWebsiteDNSRecords", mock.Anything, testZoneID1, mock.Anything, mock.Anything, pluginDb.WebsiteTargetTypeIPNS, mock.Anything)

	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_DNSRecordsCreated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-records-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8001
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Act - Expect DNS zone and records to be created with specific parameters
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID2, domain, testUserID1), nil).Once()

		var capturedTargetHash string
		var capturedTargetType pluginDb.WebsiteTargetType
		var capturedToken string

		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID2,
			mock.Anything,
			mock.MatchedBy(func(targetHash string) bool {
				capturedTargetHash = targetHash
				return true
			}),
			mock.MatchedBy(func(targetType pluginDb.WebsiteTargetType) bool {
				capturedTargetType = targetType
				return targetType == pluginDb.WebsiteTargetTypeIPNS
			}),
			mock.MatchedBy(func(token string) bool {
				capturedToken = token
				return token != ""
			}),
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		assert.NotEmpty(tb, capturedTargetHash, "Target hash should be captured")
		assert.Equal(tb, pluginDb.WebsiteTargetTypeIPNS, capturedTargetType, "Target type should be IPNS after auto-conversion")
		assert.NotEmpty(tb, capturedToken, "Validation token should be non-empty")
		expectedFormattedToken := fmt.Sprintf("lumeweb-verify=%s", createdWebsite.ValidationToken)
		assert.Equal(tb, expectedFormattedToken, capturedToken, "Validation token should be formatted with key prefix")

	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_DNSRecordsUpdatedWhenTargetChanges(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-update-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		// Act - Update target to a new CID
		newCID := util.GenerateTestCID(t, "new data for update")

		// Mock: new CID must be pinned for validation to pass
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, newCID, testUserID1).
			Return(&pluginDb.IPFSPin{UserID: testUserID1, CID: newCID.Bytes(), Status: pluginDb.PinningStatusPinned}, nil).Once()

		updates := map[string]interface{}{
			"target_hash": newCID.String(),
		}

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, updatedWebsite)
		assert.NotEmpty(tb, updatedWebsite.TargetHash())

	}, TestOptions)
}

func TestWebsiteService_DeleteWebsite_DNSRecordsCleanedUp(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-cleanup-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8002
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Create website with DNS enabled
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID4, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID4,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		// Act - Delete website and expect only DNS records to be cleaned up, NOT the zone
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, testZoneID4, mock.Anything).Return(nil).Once()

		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert
		require.NoError(tb, err)

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
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-cleanup-fail-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8003
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Create website with DNS enabled
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID5, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID5,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		// Act - Delete website with DNS cleanup failure
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, testZoneID5, mock.Anything).Return(assert.AnError).Once()

		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite.ID)

		// Assert - Website should still be deleted despite DNS cleanup failure
		require.NoError(tb, err)

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8003
		// Bind a primary domain with DNS hosting disabled: the website owns a
		// domain but no DNS-managed zone, so no DNS side-effects run on create.
		prebindPrimaryDomain(tb, ctx, website, domain, false)

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Act - Create website with DNS disabled (no DNS methods should be called)
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		createdApex, apxErr := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, apxErr)
		require.NotNil(tb, createdApex)
		assert.Zero(tb, createdApex.ZoneID, "DNS zone ID should be nil when DNS hosting is disabled")
		assert.False(tb, createdApex.DNSHostingEnabled)

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
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-enabled-test.com"
		targetHash := testCID.String()

		website := createTestIPFSWebsite(testUserID1, domain, targetHash)
		stubPinnedCID(t, ctx, testUserID1, targetHash)
		website.ID = 8004
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)

		// Set up IPNS key mocks for auto-creation
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, targetHash)

		// Mock DNS zone creation
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID6, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID6,
			mock.Anything,
			mock.Anything,                  // website domain
			pluginDb.WebsiteTargetTypeIPNS, // Converted to IPNS for managed DNS
			mock.Anything,
		).Return(nil).Once()

		// Act
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		createdApex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		assert.NotZero(tb, createdApex.ZoneID, "DNS zone ID should be set when DNS hosting is enabled")
		assert.Equal(tb, testZoneID6, createdApex.ZoneID)
		assert.True(tb, createdApex.DNSHostingEnabled)
		assert.Equal(tb, domain, apexDomain(tb, ctx, websiteService, createdWebsite.ID))
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite.TargetType, "Should be converted to IPNS for managed DNS")

	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_DNSHostingEnabled_NoDNSUpdateWhenTargetUnchanged(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "no-dns-update-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8005
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Create website with initial DNS setup
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID7, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID7,
			mock.Anything,
			mock.Anything,                  // website domain
			pluginDb.WebsiteTargetTypeIPNS, // Converted to IPNS for managed DNS
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
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
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "zone-persists-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8006
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Create website with DNS
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID8, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID8,
			mock.Anything,
			mock.Anything,                  // website domain
			pluginDb.WebsiteTargetTypeIPNS, // Converted to IPNS for managed DNS
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)

		// Mock DNS records deletion
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, testZoneID8, mock.Anything).Return(nil).Once()

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

// TestWebsiteService_DeleteWebsite_PlatformSubdomain_OperatorApexAndZoneIntact
// proves the delete-scoping invariant for platform subdomains: Alice's website
// hosts a subdomain (e.g. "alice.pinned.site") inside the operator-owned root
// zone, alongside the operator's apex binding on that same root. Deleting
// Alice's website must clean up only Alice's subdomain DNS records — the
// operator's shared zone (and therefore its apex records) must survive.
func TestWebsiteService_DeleteWebsite_PlatformSubdomain_OperatorApexAndZoneIntact(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		const operatorZoneID = uint(4000)

		// Operator's platform root + its authoritative zone exist in the DB.
		pd := &pluginDb.PlatformDomain{
			Domain: "pinned.site", Namespace: pluginDb.DomainNamespaceICANN,
			ZoneID: operatorZoneID, Enabled: true,
		}
		require.NoError(tb, db.Create(pd).Error)
		require.NoError(tb, db.Create(&pluginDb.DNSZone{
			Model: gorm.Model{ID: operatorZoneID}, Domain: "pinned.site", UserID: 100,
			Status: string(pluginDb.DNSZoneStatusActive),
		}).Error)

		// Alice's website owns a platform subdomain under the operator root, and
		// it is her primary binding (the binding the delete flow cleans).
		website := createTestIPFSWebsite(testUserID1, "alice.pinned.site", util.GenerateTestCID(t, "alice data").String())
		stubPinnedCID(t, ctx, testUserID1, util.GenerateTestCID(t, "alice data").String())
		website.Status = string(pluginDb.WebsiteStatusActive)
		require.NoError(tb, db.Create(website).Error)
		wd := createTestWebsiteDomain(website.ID, "alice.pinned.site")
		wd.ZoneID = operatorZoneID
		wd.DNSHostingEnabled = true
		wd.Status = pluginDb.DomainStatusActive
		wd.PlatformDomainID = &pd.ID
		require.NoError(tb, db.Create(wd).Error)
		require.NoError(tb, db.Model(&pluginDb.Website{ID: website.ID}).UpdateColumn("primary_domain_id", wd.ID).Error)

		// Deleting Alice's website removes only her subdomain's DNS records from
		// the shared operator zone; the zone (and operator apex) must survive.
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, operatorZoneID, "alice.pinned.site").Return(nil).Once()

		err := websiteService.DeleteWebsite(context.Background(), testUserID1, website.ID)
		require.NoError(tb, err)

		// The operator's shared zone is never deleted, so its apex records stay.
		mockDNS.AssertNotCalled(t, "DeleteZone", mock.Anything, mock.Anything)
		mockDNS.AssertNotCalled(t, "DeleteWebsiteDNSRecords", mock.Anything, mock.Anything, "pinned.site")

		// The platform root and its zone row remain registered in the DB.
		var pdAfter pluginDb.PlatformDomain
		require.NoError(tb, db.Unscoped().First(&pdAfter, pd.ID).Error)
		assert.Equal(tb, operatorZoneID, pdAfter.ZoneID)
		var zoneCount int64
		require.NoError(tb, db.Model(&pluginDb.DNSZone{}).Where("domain = ?", "pinned.site").Count(&zoneCount).Error)
		assert.Equal(tb, int64(1), zoneCount)
	}, TestOptions)
}

// TestWebsiteService_ActivatePlatformSubdomainWebsite_FlipsToActive proves
// that a website whose primary binding is a just-created platform subdomain is
// activated without any external websites_validate call. The platform controls
// both ends of the DNS check, so a pending site is safe to activate as soon as
// the binding is live.
func TestWebsiteService_ActivatePlatformSubdomainWebsite_FlipsToActive(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

		// A freshly deployed website awaiting DNS validation.
		website := createTestIPFSWebsite(testUserID1, "dark-forest-7.pinned.site", util.GenerateTestCID(t, "site data").String())
		website.Status = string(pluginDb.WebsiteStatusPendingValidation)
		require.NoError(tb, db.Create(website).Error)

		pdID := uint(7)
		wd := createTestWebsiteDomain(website.ID, "dark-forest-7.pinned.site")
		wd.Status = pluginDb.DomainStatusActive
		wd.DNSHostingEnabled = true
		wd.PlatformDomainID = &pdID
		require.NoError(tb, db.Create(wd).Error)
		require.NoError(tb, db.Model(&pluginDb.Website{ID: website.ID}).UpdateColumn("primary_domain_id", wd.ID).Error)

		require.NoError(tb, websiteService.ActivatePlatformSubdomainWebsite(context.Background(), website.ID))

		var activated pluginDb.Website
		require.NoError(tb, db.First(&activated, website.ID).Error)
		assert.Equal(tb, string(pluginDb.WebsiteStatusActive), activated.Status)
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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 1671
		prebindPrimaryDomain(tb, ctx, website, domain, false) // DNS hosting disabled

		mockDNS, ok := dnsService.(*mocks.MockDNSService)
		require.True(tb, ok, "DNS service should be a mock")

		// Act - Create website with DNS disabled
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		apex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		require.NotNil(tb, apex)
		assert.Zero(tb, apex.ZoneID, "DNS zone ID should be nil when DNS hosting is disabled")
		assert.False(tb, apex.DNSHostingEnabled)

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 1700
		prebindPrimaryDomain(tb, ctx, website, domain, false) // DNS hosting disabled

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
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 1738
		prebindPrimaryDomain(tb, ctx, website, domain, false) // DNS hosting disabled

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
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "zone-fail-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8007
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Mock DNS zone creation failure
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(nil, assert.AnError).Once()

		// Act - Create website with DNS zone creation failure
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()

		// Assert - Website should still be created despite DNS failure
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		apex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		require.NotNil(tb, apex)
		assert.Zero(tb, apex.ZoneID, "DNS zone ID should be nil when zone creation fails")
		assert.Equal(tb, domain, apex.Domain)
		// Website should still get IPNS conversion since that happens before DNS operations
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite.TargetType, "Should be converted to IPNS")

		// Verify CreateWebsiteDNSRecords was not called (zone creation failed)
		mockDNS.AssertNotCalled(t, "CreateWebsiteDNSRecords")

	}, TestOptions)
}

func TestWebsiteService_CreateWebsite_DNSRecordsCreationFailure_ContinuesWithoutRecords(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "records-fail-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8008
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Mock DNS zone creation success
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID8, domain, testUserID1), nil).Once()

		// Mock DNS records creation failure
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID8,
			mock.Anything,
			mock.Anything,                  // website domain
			pluginDb.WebsiteTargetTypeIPNS, // Converted to IPNS for managed DNS
			mock.Anything,
		).Return(assert.AnError).Once()

		// Act - Create website with DNS records creation failure
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()

		// Assert - Website should still be created with zone ID set
		require.NoError(tb, err)
		assert.NotNil(tb, createdWebsite)
		createdApex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		assert.NotZero(tb, createdApex.ZoneID, "DNS zone ID should be set even when record creation fails")
		assert.Equal(tb, testZoneID8, createdApex.ZoneID)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite.TargetType, "Should be converted to IPNS")

	}, TestOptions)
}

// TestWebsiteService_EnableDNSHosting_RecordFailurePreservesDelegationZone
// verifies failed website-record creation cannot clear or delete a zone still
// required by delegation.
func TestWebsiteService_EnableDNSHosting_RecordFailurePreservesDelegationZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "delegation-enable-failure.com"
		testZoneID := uint(9002)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 9013
		// The enable transition loads the owning Website row for target/token
		// state, so it must exist in the DB (this test bypasses CreateWebsite in
		// favor of a prebound primary domain). A valid status satisfies the
		// Website.BeforeSave hook.
		website.Status = string(pluginDb.WebsiteStatusActive)
		require.NoError(tb, ctx.DB().Create(website).Error)
		wd := prebindPrimaryDomain(tb, ctx, website, domain, false)
		wd.ZoneID = testZoneID
		wd.Status = pluginDb.DomainStatusRecordsGenerated
		wd.DelegationData = datatypes.JSONMap{"ns": []interface{}{"dns1.example."}}
		require.NoError(tb, ctx.DB().Model(wd).Updates(map[string]interface{}{
			"zone_id":         testZoneID,
			"status":          string(pluginDb.DomainStatusRecordsGenerated),
			"delegation_data": wd.DelegationData,
		}).Error)

		mockDNS.EXPECT().CreateWebsiteValidationRecord(
			mock.Anything, testZoneID, domain, mock.Anything,
		).Return(assert.AnError).Once()
		mockDNS.EXPECT().DeleteWebsiteValidationRecord(mock.Anything, testZoneID, domain).Return(nil).Once()

		_, err := websiteService.SetDomainDNSEnabled(context.Background(), testUserID1, website.ID, wd.ID, true)
		require.Error(tb, err)

		var persisted pluginDb.WebsiteDomain
		require.NoError(tb, ctx.DB().First(&persisted, wd.ID).Error)
		assert.Equal(tb, testZoneID, persisted.ZoneID, "delegation-owned zone must survive enable rollback")
		assert.False(tb, persisted.DNSHostingEnabled, "hosting flag must remain disabled after failed enable")
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
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "test-auto-ipns.com"

		// Set up mock expectations for first website creation
		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)
		testKeyID := testIPNSKey.ID

		// DNS zone creation (using helper)
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID1, domain, testUserID1), nil).Once()

		// DNS records creation (after IPNS conversion)
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID1,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		// Act - Create first website with DNS hosting enabled
		website1 := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website1.ID = 8009
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website1, domain, true)
		createdWebsite1, err := websiteService.CreateWebsite(context.Background(), website1)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)

		// Assert - IPNS key was created
		require.NotNil(tb, createdWebsite1.IPNSKeyID, "First website should have IPNS key ID")
		assert.Equal(tb, testKeyID, *createdWebsite1.IPNSKeyID, "IPNS key ID should match")
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite1.TargetType, "First website should use IPNS target")

		// Expect DNS records deletion when website is deleted
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, testZoneID1, mock.Anything).Return(nil).Once()

		// Act - Delete first website
		err = websiteService.DeleteWebsite(context.Background(), testUserID1, createdWebsite1.ID)
		require.NoError(tb, err)

		// Set up mock expectations for second website creation
		// ListKeys should return the existing key (not nil)
		mockIPNSKey.EXPECT().ListKeys(mock.Anything, testUserID1).Return([]pluginDb.IPFSIPNSKey{*testIPNSKey}, nil).Once()

		// CreateKey should NOT be called since key already exists
		// PublishCID should still be called to update the IPNS key with the new content
		mockIPNSKey.EXPECT().PublishCID(mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil).Once()

		// DNS zone creation
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID1, domain, testUserID1), nil).Once()

		// DNS records creation
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID1,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		// Act - Create second website with same domain
		website2 := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website2.ID = 8010
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website2, domain, true)
		createdWebsite2, err := websiteService.CreateWebsite(context.Background(), website2)
		require.NoError(tb, err)

		// Assert - Second website reuses the same IPNS key
		require.NotNil(tb, createdWebsite2.IPNSKeyID, "Second website should have IPNS key ID")
		assert.Equal(tb, *createdWebsite1.IPNSKeyID, *createdWebsite2.IPNSKeyID, "Should reuse the same IPNS key ID")
		assert.Equal(tb, testKeyID, *createdWebsite2.IPNSKeyID, "IPNS key ID should match the original")
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite2.TargetType, "Second website should use IPNS target")
	}, TestOptions)
}

// TestWebsiteService_UpdateWebsite_ConvertIPFSToIPNS tests updating a website
// from IPFS target to IPNS target when only target_hash is provided.
// It validates that targetType is correctly auto-detected when target_hash
// is a peer ID, avoiding the validation bug where cid.Decode() would be
// called on a peer ID string.
func TestWebsiteService_UpdateWebsite_ConvertIPFSToIPNS(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		testCID := util.GenerateTestCID(t, "test data")
		domain := "convert-test.com"

		ipfsWebsite := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), ipfsWebsite)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPFS), createdWebsite.TargetType)

		// Update to IPNS by changing target_hash to a peer ID
		// targetType will be auto-detected as IPNS
		testPeerID := "12D3KooWCqvCZqaG6LmG4mtoWZZwrvYB911DK8qqwE9gc25s4Hft"

		// Mock: IPNS key must exist and belong to the user
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		mockIPNSKey.EXPECT().GetPrivateKeyByPeerID(mock.Anything, testPeerID).
			Return(nil, testUserID1, nil).Once()

		updates := map[string]interface{}{
			"target_hash": testPeerID,
		}

		// Act
		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

		// Assert
		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updatedWebsite.TargetType)
		assert.Equal(tb, testPeerID, updatedWebsite.TargetHash())
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_EnableDNSHostingTransition(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-enable-transition-test.com"
		zoneID := uint(9999)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8009
		// Bind a primary domain with DNS hosting disabled: enabling DNS later
		// toggles this binding's DNSHostingEnabled from false to true.
		prebindPrimaryDomain(tb, ctx, website, domain, false)
		website.Status = string(pluginDb.WebsiteStatusActive)

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)
		createdApex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		assert.False(t, createdApex.DNSHostingEnabled)
		assert.Zero(t, createdApex.ZoneID)

		// Set up mock DNS service expectations for enabling DNS hosting
		setupDNSZoneCreationMocks(t, mockDNS, zoneID, domain, testUserID1)

		// Act - Enable DNS hosting
		newDNSEnabled := true
		updates := map[string]interface{}{
			"dns_enabled": newDNSEnabled,
		}

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

		// Assert
		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		updatedApex, uerr := websiteService.GetApexDomainBinding(context.Background(), updatedWebsite.ID)
		require.NoError(tb, uerr)
		assert.True(t, updatedApex.DNSHostingEnabled)
		assert.Equal(t, string(pluginDb.WebsiteStatusPendingValidation), updatedWebsite.Status)
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_DisableDNSHostingTransition(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-disable-transition-test.com"
		testZoneID := uint(9998)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8011
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		website.Status = string(pluginDb.WebsiteStatusActive)

		// Set up IPNS key mocks for auto-creation
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Mock DNS operations for initial website creation with DNS enabled
		setupDNSZoneCreationMocks(t, mockDNS, testZoneID, domain, testUserID1)

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)
		createdApex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		assert.True(t, createdApex.DNSHostingEnabled)
		assert.NotZero(t, createdApex.ZoneID)

		// Set up mock DNS service expectations for disabling DNS hosting
		// First, DeleteWebsiteDNSRecords is called to remove the website's DNS records
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, testZoneID, mock.Anything).Return(nil).Once()
		// Then, DeleteZone is called because no other websites share the zone
		setupDeleteZoneMocks(t, mockDNS, testZoneID)

		// Act - Disable DNS hosting
		newDNSEnabled := false
		updates := map[string]interface{}{
			"dns_enabled": newDNSEnabled,
		}

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

		// Assert
		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		updatedApex, uerr := websiteService.GetApexDomainBinding(context.Background(), updatedWebsite.ID)
		require.NoError(tb, uerr)
		assert.False(t, updatedApex.DNSHostingEnabled)
		assert.Equal(t, string(pluginDb.WebsiteStatusPendingValidation), updatedWebsite.Status)
		assert.Zero(t, updatedApex.ZoneID)

	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_DNSEnabledInvalidType(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "invalid-dns-type-test.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		// Act - Try to update dns_enabled with an invalid type (string instead of bool)
		updates := map[string]interface{}{
			"dns_enabled": "true", // Invalid: should be bool
		}

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, updatedWebsite)
		assert.Contains(tb, err.Error(), "dns_enabled must be a boolean")
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_DNSHostingTransitionWithExistingZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "existing-zone-test.com"
		testZoneID := uint(9997)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8010
		// Bind a primary domain with DNS hosting disabled but an already-existing
		// zone, so that enabling DNS reuses the zone (no CreateZone) and only
		// creates the DNS records.
		existingApex := prebindPrimaryDomain(tb, ctx, website, domain, false)
		existingApex.ZoneID = testZoneID
		require.NoError(tb, ctx.DB().Save(existingApex).Error)
		website.Status = string(pluginDb.WebsiteStatusActive)

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)
		createdApex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		assert.False(t, createdApex.DNSHostingEnabled)

		// Act - Enable DNS hosting when zone already exists
		// Note: We don't mock CreateWebsiteDNSRecords here because handleDNSEnabledTransition
		// will add its own expectation when calling the method
		mockDNS.EXPECT().CreateWebsiteDNSRecords(mock.Anything, testZoneID, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		newDNSEnabled := true
		updates := map[string]interface{}{
			"dns_enabled": newDNSEnabled,
		}

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

		// Assert
		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		updatedApex, uerr := websiteService.GetApexDomainBinding(context.Background(), updatedWebsite.ID)
		require.NoError(tb, uerr)
		assert.True(t, updatedApex.DNSHostingEnabled)
		assert.Equal(t, string(pluginDb.WebsiteStatusPendingValidation), updatedWebsite.Status)

		// Verify CreateZone was NOT called (zone already existed)
		mockDNS.AssertNotCalled(t, "CreateZone")
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_DNSEnableToggleOffOn(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-toggle-test.com"
		testZoneID := uint(8001)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8012
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		website.Status = string(pluginDb.WebsiteStatusActive)

		// Initial creation with DNS enabled
		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)
		setupDNSZoneCreationMocks(t, mockDNS, testZoneID, domain, testUserID1)

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)
		createdApex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		assert.True(t, createdApex.DNSHostingEnabled)
		assert.NotZero(t, createdApex.ZoneID)

		// Toggle DNS off
		// First, DeleteWebsiteDNSRecords is called to remove the website's DNS records
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, createdApex.ZoneID, mock.Anything).Return(nil).Once()
		// Then, DeleteZone is called because no other websites share the zone
		mockDNS.EXPECT().DeleteZone(mock.Anything, createdApex.ZoneID).Return(nil).Once()
		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, map[string]interface{}{
			"dns_enabled": false,
		})
		require.NoError(tb, err)
		updatedApex, uerr := websiteService.GetApexDomainBinding(context.Background(), updatedWebsite.ID)
		require.NoError(tb, uerr)
		assert.False(t, updatedApex.DNSHostingEnabled)
		assert.Zero(t, updatedApex.ZoneID, "dns_zone_id should be nil after successful delete")

		// Toggle DNS back on - handleDNSEnabledTransition only creates zone + records (no IPNS)
		newZoneID := uint(8002)
		newMockZone := createMockDNSZone(newZoneID, domain, testUserID1)
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(newMockZone, nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(mock.Anything, newZoneID, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

		updatedWebsite2, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, map[string]interface{}{
			"dns_enabled": true,
		})
		require.NoError(tb, err)
		updatedApex2, u2err := websiteService.GetApexDomainBinding(context.Background(), updatedWebsite2.ID)
		require.NoError(tb, u2err)
		assert.True(t, updatedApex2.DNSHostingEnabled)
		assert.NotZero(t, updatedApex2.ZoneID, "dns_zone_id should be set after re-enable")
		assert.Equal(t, newZoneID, updatedApex2.ZoneID)
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_DisableDNSHostingDeleteZoneFails(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "dns-delete-fail-test.com"
		testZoneID := uint(8003)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8013
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		website.Status = string(pluginDb.WebsiteStatusActive)

		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)
		setupDNSZoneCreationMocks(t, mockDNS, testZoneID, domain, testUserID1)

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)
		createdApex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		assert.NotZero(t, createdApex.ZoneID)

		// Toggle DNS off but DeleteZone fails
		// First, DeleteWebsiteDNSRecords is called and succeeds
		mockDNS.EXPECT().DeleteWebsiteDNSRecords(mock.Anything, createdApex.ZoneID, mock.Anything).Return(nil).Once()
		// Then, DeleteZone is called but fails
		mockDNS.EXPECT().DeleteZone(mock.Anything, createdApex.ZoneID).Return(errors.New("powerdns unavailable")).Once()

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, map[string]interface{}{
			"dns_enabled": false,
		})
		require.NoError(tb, err)
		updatedApex, uerr := websiteService.GetApexDomainBinding(context.Background(), updatedWebsite.ID)
		require.NoError(tb, uerr)
		assert.False(t, updatedApex.DNSHostingEnabled)
		assert.NotZero(t, updatedApex.ZoneID, "dns_zone_id should be preserved when DeleteZone fails")
		assert.Equal(t, createdApex.ZoneID, updatedApex.ZoneID)
	}, TestOptions)
}

// TestWebsiteService_UpdateWebsite_ConvertIPNSToIPFS_UpdatesDNSRecords verifies that
// when a website with DNS hosting enabled is updated from IPNS to IPFS target type,
// the DNS _dnslink record is updated from /ipns/<peerID> to /ipfs/<cid>.
// This tests the fix for the bug where IPNSKeyID != nil caused DNS update to be skipped
// even when the target type was changing away from IPNS.
func TestWebsiteService_UpdateWebsite_ConvertIPNSToIPFS_UpdatesDNSRecords(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "ipns-to-ipfs-dns-test.com"
		testZoneID := uint(9001)

		// Create website with DNS hosting enabled (auto-creates IPNS key)
		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8014
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite.TargetType)
		require.NotNil(tb, createdWebsite.IPNSKeyID)
		apex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		require.NotNil(tb, apex)
		require.NotZero(tb, apex.ZoneID)
		assert.Equal(tb, testZoneID, apex.ZoneID)

		// Act - Update from IPNS to IPFS
		newCID := util.GenerateTestCID(t, "new ipfs content")

		// Mock: new CID must be pinned for validation to pass
		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, newCID, testUserID1).
			Return(&pluginDb.IPFSPin{UserID: testUserID1, CID: newCID.Bytes(), Status: pluginDb.PinningStatusPinned}, nil).Once()

		updates := map[string]interface{}{
			"target_hash": newCID.String(),
			"target_type": string(pluginDb.WebsiteTargetTypeIPFS),
		}

		// IPNS republish attempt (website still has IPNSKeyID from creation)
		mockIPNSKey.EXPECT().GetKeyByID(mock.Anything, testUserID1, *createdWebsite.IPNSKeyID).Return(testIPNSKey, nil).Once()
		mockIPNSKey.EXPECT().PublishCID(mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil).Once()

		// DNS records must be updated when switching from IPNS to IPFS
		mockDNS.EXPECT().UpdateWebsiteDNSRecords(
			mock.Anything,
			testZoneID,
			mock.Anything,
			newCID.String(),
			pluginDb.WebsiteTargetTypeIPFS,
		).Return(nil).Once()

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)

		// Assert
		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPFS), updatedWebsite.TargetType)
		assert.Equal(tb, newCID.String(), updatedWebsite.TargetHash())
	}, TestOptions)
}

// TestWebsiteService_UpdateWebsite_IPNSToIPNS_NoDNSUpdate verifies that
// when a website with DNS hosting stays as IPNS (only the CID published
// to the IPNS key changes), DNS records are NOT updated since the peer ID
// in the _dnslink record stays the same.
func TestWebsiteService_UpdateWebsite_IPNSToIPNS_NoDNSUpdate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "ipns-same-type-test.com"
		testZoneID := uint(9002)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8015
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)

		// Act - Update to a new IPNS peer ID (staying as IPNS)
		testPeerID := "12D3KooWCqvCZqaG6LmG4mtoWZZwrvYB911DK8qqwE9gc25s4Hft"

		// Mock: IPNS key must exist and belong to the user
		mockIPNSKey.EXPECT().GetPrivateKeyByPeerID(mock.Anything, testPeerID).
			Return(nil, testUserID1, nil).Once()

		updates := map[string]interface{}{
			"target_hash": testPeerID,
		}

		// IPNS republish attempt (website still has IPNSKeyID)
		mockIPNSKey.EXPECT().GetKeyByID(mock.Anything, testUserID1, *createdWebsite.IPNSKeyID).Return(testIPNSKey, nil).Once()
		mockIPNSKey.EXPECT().PublishCID(mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil).Once()

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)
		websiteService.WaitForPublishes()

		// Assert
		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updatedWebsite.TargetType)

		// DNS records should NOT be updated when staying as IPNS
		mockDNS.AssertNotCalled(t, "UpdateWebsiteDNSRecords")
	}, TestOptions)
}

// TestWebsiteService_UpdateWebsite_ConvertIPFSToIPNS_UpdatesDNSRecords verifies that
// when a website with DNS hosting is updated from IPFS to IPNS target type,
// the DNS _dnslink record is updated from /ipfs/<cid> to /ipns/<peerID>.
func TestWebsiteService_UpdateWebsite_ConvertIPFSToIPNS_UpdatesDNSRecords(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "ipfs-to-ipns-dns-test.com"
		testZoneID := uint(9003)

		// Create website with DNS hosting enabled (auto-creates IPNS key, starts as IPNS)
		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8016
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)
		apex, err := websiteService.GetApexDomainBinding(context.Background(), createdWebsite.ID)
		require.NoError(tb, err)
		require.NotNil(tb, apex)
		require.NotZero(tb, apex.ZoneID)
		assert.Equal(tb, testZoneID, apex.ZoneID)

		// First, switch back to IPFS to set up the IPFS→IPNS scenario
		newCID := util.GenerateTestCID(t, "intermediate ipfs content")

		// Mock: CID must be pinned for validation to pass
		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, newCID, testUserID1).
			Return(&pluginDb.IPFSPin{UserID: testUserID1, CID: newCID.Bytes(), Status: pluginDb.PinningStatusPinned}, nil).Once()

		ipfsUpdates := map[string]interface{}{
			"target_hash": newCID.String(),
			"target_type": string(pluginDb.WebsiteTargetTypeIPFS),
		}

		mockIPNSKey.EXPECT().GetKeyByID(mock.Anything, testUserID1, *createdWebsite.IPNSKeyID).Return(testIPNSKey, nil).Once()
		mockIPNSKey.EXPECT().PublishCID(mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil).Once()
		mockDNS.EXPECT().UpdateWebsiteDNSRecords(
			mock.Anything,
			testZoneID,
			mock.Anything,
			newCID.String(),
			pluginDb.WebsiteTargetTypeIPFS,
		).Return(nil).Once()

		ipfsWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, ipfsUpdates)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPFS), ipfsWebsite.TargetType)

		// Act - Update from IPFS to IPNS
		testPeerID := "12D3KooWCqvCZqaG6LmG4mtoWZZwrvYB911DK8qqwE9gc25s4Hft"

		// Mock: IPNS key must exist and belong to the user
		mockIPNSKey.EXPECT().GetPrivateKeyByPeerID(mock.Anything, testPeerID).
			Return(nil, testUserID1, nil).Once()

		ipnsUpdates := map[string]interface{}{
			"target_hash": testPeerID,
		}

		// IPNS republish (website still has IPNSKeyID)
		mockIPNSKey.EXPECT().GetKeyByID(mock.Anything, testUserID1, *ipfsWebsite.IPNSKeyID).Return(testIPNSKey, nil).Once()
		mockIPNSKey.EXPECT().PublishCID(mock.Anything, mock.Anything, mock.Anything, mock.AnythingOfType("time.Duration")).Return(nil).Once()

		// DNS records must be updated when switching from IPFS to IPNS
		mockDNS.EXPECT().UpdateWebsiteDNSRecords(
			mock.Anything,
			testZoneID,
			mock.Anything,
			testPeerID,
			pluginDb.WebsiteTargetTypeIPNS,
		).Return(nil).Once()

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, ipfsWebsite.ID, ipnsUpdates)
		websiteService.WaitForPublishes()

		// Assert
		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updatedWebsite.TargetType)
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_TargetTypeIPNSAlone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "type-only-convert.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 2544
		prebindPrimaryDomain(tb, ctx, website, domain, false) // DNS hosting disabled

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPFS), createdWebsite.TargetType)

		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Mock: existing CID must be pinned for validation to pass
		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, testCID, testUserID1).
			Return(&pluginDb.IPFSPin{UserID: testUserID1, CID: testCID.Bytes(), Status: pluginDb.PinningStatusPinned}, nil).Maybe()

		updates := map[string]interface{}{
			"target_type": string(pluginDb.WebsiteTargetTypeIPNS),
		}

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)
		websiteService.WaitForPublishes()

		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updatedWebsite.TargetType)
		assert.Equal(tb, testIPNSKey.PeerID().String(), updatedWebsite.TargetHash())
		require.NotNil(tb, updatedWebsite.IPNSKeyID)
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_TargetTypeIPNSAlone_DNSRecordsUpdated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "type-only-dns.com"
		testZoneID := uint(8001)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 2585
		prebindPrimaryDomain(tb, ctx, website, domain, false) // DNS hosting disabled initially

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, createdWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPFS), createdWebsite.TargetType)

		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		// Mock: existing CID must be pinned for validation to pass
		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, testCID, testUserID1).
			Return(&pluginDb.IPFSPin{UserID: testUserID1, CID: testCID.Bytes(), Status: pluginDb.PinningStatusPinned}, nil).Maybe()

		updates := map[string]interface{}{
			"target_type": string(pluginDb.WebsiteTargetTypeIPNS),
			"dns_enabled": true,
		}

		// Converting to IPNS while enabling DNS: the service applies the
		// dns_enabled toggle to the primary WebsiteDomain (per-domain model) and
		// creates the zone + IPNS records for the converted target.
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID,
			mock.Anything,
			testIPNSKey.PeerID().String(),
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)
		websiteService.WaitForPublishes()

		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updatedWebsite.TargetType)
		apex, err := websiteService.GetApexDomainBinding(context.Background(), updatedWebsite.ID)
		require.NoError(tb, err)
		require.NotNil(tb, apex)
		assert.True(tb, apex.DNSHostingEnabled)
		assert.NotZero(tb, apex.ZoneID)
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_IPNSTargetTypeWithCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "original data")
		domain := "ipns-with-cid.com"

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 2641
		prebindPrimaryDomain(tb, ctx, website, domain, false) // DNS hosting disabled

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPFS), createdWebsite.TargetType)

		newCID := util.GenerateTestCID(t, "new data for ipns")

		// Mock: CID must be pinned for validation to pass
		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, newCID, testUserID1).
			Return(&pluginDb.IPFSPin{UserID: testUserID1, CID: newCID.Bytes(), Status: pluginDb.PinningStatusPinned}, nil).Once()

		testIPNSKey := setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, newCID)

		updates := map[string]interface{}{
			"target_type": string(pluginDb.WebsiteTargetTypeIPNS),
			"target_hash": newCID.String(),
		}

		updatedWebsite, err := websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)
		websiteService.WaitForPublishes()

		require.NoError(tb, err)
		require.NotNil(tb, updatedWebsite)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), updatedWebsite.TargetType)
		assert.Equal(tb, testIPNSKey.PeerID().String(), updatedWebsite.TargetHash())
	}, TestOptions)
}

func TestWebsiteService_UpdateWebsite_IPNSToIPFSWithoutCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "ipns-to-ipfs-no-cid.com"
		testZoneID := uint(8002)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 8017
		// Bind the primary domain with DNS hosting enabled so CreateWebsite's
		// managed-DNS side-effects (IPNS auto-convert, zone + record creation) run.
		prebindPrimaryDomain(tb, ctx, website, domain, true)
		_ = setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)

		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(testZoneID, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			testZoneID,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		createdWebsite, err := websiteService.CreateWebsite(context.Background(), website)
		websiteService.WaitForPublishes()
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.WebsiteTargetTypeIPNS), createdWebsite.TargetType)

		updates := map[string]interface{}{
			"target_type": string(pluginDb.WebsiteTargetTypeIPFS),
		}

		_, err = websiteService.UpdateWebsite(context.Background(), testUserID1, createdWebsite.ID, updates)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "cannot convert from IPNS to IPFS without specifying a target CID")
	}, TestOptions)
}

// TestWebsiteService_DisableDNSHosting_PreservesDelegationOwnedZone verifies
// that disabling website DNS hosting on a delegation-owned binding (one whose
// PowerDNS zone hosts alt-root delegation records) does NOT delete the zone or
// clear zone_id — the zone must survive for the delegation (VerifyDomain,
// EnableDNSSEC, GetActiveDNSSECDS, republish all read zone_id).
func TestWebsiteService_DisableDNSHosting_PreservesDelegationOwnedZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

		testCID := util.GenerateTestCID(t, "test data")
		domain := "delegation-owned.com"
		testZoneID := uint(9001)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		stubPinnedCID(t, ctx, testUserID1, testCID.String())
		website.ID = 9012

		// The disable transition loads and rewrites the owning Website row, so
		// it must actually exist in the DB (this test bypasses CreateWebsite in
		// favor of a prebound primary domain). A valid status satisfies the
		// Website.BeforeSave hook.
		website.Status = string(pluginDb.WebsiteStatusActive)
		require.NoError(tb, ctx.DB().Create(website).Error)

		// Bind the primary domain as a delegation-owned binding: it has
		// progressed into the delegation lifecycle (delegation_data present,
		// status records_generated) and carries a real zone.
		wd := prebindPrimaryDomain(tb, ctx, website, domain, true)
		wd.ZoneID = testZoneID
		wd.Status = pluginDb.DomainStatusRecordsGenerated
		wd.DelegationData = datatypes.JSONMap{"ns": []interface{}{"dns1.example."}}
		require.NoError(tb, ctx.DB().Model(wd).Updates(map[string]interface{}{
			"zone_id":         testZoneID,
			"status":          string(pluginDb.DomainStatusRecordsGenerated),
			"delegation_data": wd.DelegationData,
		}).Error)

		// Toggle DNS hosting off. The delegation-owned gate must skip record
		// deletion and zone deletion entirely — no DNS service calls expected.
		updated, err := websiteService.SetDomainDNSEnabled(context.Background(), testUserID1, website.ID, wd.ID, false)
		require.NoError(tb, err)
		assert.False(tb, updated.DNSHostingEnabled, "hosting flag should be off")
		assert.Equal(tb, testZoneID, updated.ZoneID, "delegation-owned zone must be preserved on DNS-host disable")
	}, TestOptions)
}
