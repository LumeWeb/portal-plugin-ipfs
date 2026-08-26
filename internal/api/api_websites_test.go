package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pluginservice "go.lumeweb.com/portal-plugin-ipfs/internal/service/website"
	mocks "go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

// Helper function to create a mock IPFS website
func createMockIPFSWebsite(id, userID uint, domain string, testCID string, status pluginDb.WebsiteStatus, token string) *pluginDb.Website {
	c := cid.MustParse(testCID)
	version := uint8(c.Version())
	return &pluginDb.Website{
		ID:              id,
		UserID:          userID,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
		TargetMultihash: c.Hash(),
		CIDVersion:      &version,
		Status:          string(status),
		ValidationToken: token,
	}
}

// Helper function to create a mock IPNS website
func createMockIPNSWebsite(id, userID uint, domain string, peerIDStr string, status pluginDb.WebsiteStatus, token string) *pluginDb.Website {
	target, _ := pluginDb.NewIPNSTargetFromString(peerIDStr)
	return &pluginDb.Website{
		ID:              id,
		UserID:          userID,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPNS),
		TargetMultihash: target.ToMultihash(),
		CIDVersion:      nil,
		Status:          string(status),
		ValidationToken: token,
	}
}

// Website API Tests
//
// NOTE: AuthService mock configuration issues affecting all Phase 3 tests
// The MockAuthService in core/testing has expectations that are not being met
// across all Phase 3 test files. This is a pre-existing issue in the testing
// framework and is not specific to this implementation. The tests still pass
// because the mock is configured to allow unexpected calls with .Maybe().

func TestAPI_CreateWebsite(t *testing.T) {
	t.Run("success_ipfs_target", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			// The handler creates the primary WebsiteDomain through the real
			// DelegatedDomainService.CreateDomain, which looks the website up in
			// the DB and provisions a DNS zone. Persist a real website row and
			// stub the DNS zone/records calls.
			require.NoError(tb, ctx.DB().Create(createTestIPFSGatewayWebsite(1, userID, TestDomain, cid.MustParse(TestCID), pluginDb.WebsiteStatusActive)).Error)
			mockDNS := helper.SetupDNSServiceMocks()
			mockDNS.EXPECT().CreateZone(mock.Anything, TestDomain, userID).Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 1}, Domain: TestDomain}, nil)
			mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(1), mock.Anything, mock.Anything).Return(nil).Maybe()

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, pluginDb.WebsiteStatusPendingValidation, "test-token")

			mockWebsiteService.EXPECT().CreateWebsite(mock.Anything, mock.AnythingOfType("*db.Website")).Return(mockWebsite, nil)

			// Creating a website also creates its primary WebsiteDomain binding
			// and enables DNS hosting on it (default true); the handler resolves
			// the apex binding for the response and for the enable step.
			mockApex := &pluginDb.WebsiteDomain{
				ID:                1,
				WebsiteID:         1,
				UserID:            userID,
				Domain:            TestDomain,
				DNSHostingEnabled: true,
			}
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApex, nil)
			mockWebsiteService.EXPECT().SetDomainDNSEnabled(mock.Anything, userID, uint(1), uint(1), true).Return(mockApex, nil)
			mockWebsiteService.EXPECT().NotifyAdminWebsiteCreated(mock.Anything, uint(1)).Return(nil)

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusCreated, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.Equal(t, "ipfs", response.TargetType)
		}, TestOptions)
	})

	t.Run("success_ipns_target", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			// Setup IPNS service mocks for IPNS target type
			helper.SetupIPNSServiceMocks(userID)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			// CreateDomain via the real delegated domain service needs a real
			// website row and provisions a DNS zone.
			require.NoError(tb, ctx.DB().Create(createTestIPFSGatewayWebsite(1, userID, TestDomain, cid.MustParse(TestCID), pluginDb.WebsiteStatusActive)).Error)
			mockDNS := helper.SetupDNSServiceMocks()
			mockDNS.EXPECT().CreateZone(mock.Anything, TestDomain, userID).Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 1}, Domain: TestDomain}, nil)
			mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(1), mock.Anything, mock.Anything).Return(nil).Maybe()

			mockWebsite := createMockIPNSWebsite(1, userID, TestDomain, TestPeerID, pluginDb.WebsiteStatusPendingValidation, "test-token")

			mockWebsiteService.EXPECT().CreateWebsite(mock.Anything, mock.AnythingOfType("*db.Website")).Return(mockWebsite, nil)

			mockApex := &pluginDb.WebsiteDomain{
				ID:                1,
				WebsiteID:         1,
				UserID:            userID,
				Domain:            TestDomain,
				DNSHostingEnabled: true,
			}
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApex, nil)
			mockWebsiteService.EXPECT().SetDomainDNSEnabled(mock.Anything, userID, uint(1), uint(1), true).Return(mockApex, nil)
			mockWebsiteService.EXPECT().NotifyAdminWebsiteCreated(mock.Anything, uint(1)).Return(nil)

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipns","target_hash":"%s"}`, TestDomain, TestPeerID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusCreated, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.Equal(t, "ipns", response.TargetType)
		}, TestOptions)
	})

	t.Run("error_invalid_request_empty_domain", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_platform_root_domain_rejected", func(t *testing.T) {
		// Regression: a user must never be able to claim a platform root apex
		// (e.g. "pinned.site") as a custom domain for their website — the apex is
		// operator-owned. This is the exact leak that let the wizard create a
		// website with domain = the platform root and no subdomain name.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			// Register an enabled platform root (as the admin flow would).
			pd := &pluginDb.PlatformDomain{
				Domain:    "platform.test",
				Namespace: pluginDb.DomainNamespaceICANN,
				ZoneID:    1,
				Enabled:   true,
			}
			require.NoError(tb, ctx.DB().Create(pd).Error)

			// Creating a website that names the platform root as its primary
			// domain must be rejected up front (422), before any Website is
			// persisted.
			reqBody := fmt.Sprintf(`{"domain":"platform.test","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_platform_claim_rolls_back_website", func(t *testing.T) {
		// Regression: a platform-subdomain claim that fails at any point must
		// roll back the website row CreateWebsite already persisted so no orphan
		// website with no domain binding is left behind. Drive the pd==nil exit by
		// claiming a subdomain under a platform_domain that is not enabled.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, "", TestCID, pluginDb.WebsiteStatusPendingValidation, "test-token")
			mockWebsiteService.EXPECT().CreateWebsite(mock.Anything, mock.AnythingOfType("*db.Website")).Return(mockWebsite, nil)
			mockWebsiteService.EXPECT().DeleteWebsite(mock.Anything, userID, uint(1)).Return(nil)

			reqBody := fmt.Sprintf(`{"platform_domain":"%s","label":"foo","target_type":"ipfs","target_hash":"%s"}`, "no-such-domain.test", TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
			mockWebsiteService.AssertNumberOfCalls(tb, "DeleteWebsite", 1)
		}, TestOptions)
	})

	t.Run("error_mint_without_configured_platform_domain", func(t *testing.T) {
		// Regression: a `generate: true` mint (no platform_domain, no domain)
		// must be treated as a platform-subdomain claim and reach the claim
		// path — not be rejected with the misleading "domain is required". With
		// no enabled platform root configured, the claim resolves no root and
		// rolls back the website, so CreateWebsite runs and DeleteWebsite is
		// called exactly once.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, "", TestCID, pluginDb.WebsiteStatusPendingValidation, "test-token")
			mockWebsiteService.EXPECT().CreateWebsite(mock.Anything, mock.AnythingOfType("*db.Website")).Return(mockWebsite, nil)
			mockWebsiteService.EXPECT().DeleteWebsite(mock.Anything, userID, uint(1)).Return(nil)

			reqBody := fmt.Sprintf(`{"generate":true,"target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
			mockWebsiteService.AssertNumberOfCalls(tb, "CreateWebsite", 1)
			mockWebsiteService.AssertNumberOfCalls(tb, "DeleteWebsite", 1)
		}, TestOptions)
	})

	t.Run("error_domain_and_platform_claim_conflict", func(t *testing.T) {
		// A custom domain and a platform-subdomain claim are mutually exclusive
		// destinations. Supplying both is ambiguous and must be rejected up
		// front (422) without persisting a website.
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
			mockWebsiteService.AssertNotCalled(tb, "CreateWebsite", mock.Anything, mock.Anything)

			reqBody := fmt.Sprintf(`{"domain":"example.com","generate":true,"target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_target_type", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"invalid","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_cid", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"invalid-cid"}`, TestDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_peer_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipns","target_hash":"invalid-peer-id"}`, TestDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_creation_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().CreateWebsite(mock.Anything, mock.AnythingOfType("*db.Website")).Return(nil, errors.New("creation failed"))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_domain_owned_by_another_website_conflict", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			// A domain live-bound to a DIFFERENT website must be refused as an
			// ownership conflict (409) during create, not surface a raw MySQL
			// 1062 duplicate-key ("Duplicate entry 'example.org-icann' for key
			// 'website_domains.uk_domain_namespace'") as a 500. The guard runs
			// before the website is persisted, so CreateWebsite is not called,
			// leaving no dangling website row behind.
			require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
				ID:        1,
				WebsiteID: 99, // owned by another website
				UserID:    userID,
				Domain:    "example.org",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusActive,
			}).Error)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
			mockWebsiteService.AssertNotCalled(tb, "CreateWebsite", mock.Anything, mock.Anything)

			// Request the domain as a www.-prefixed, mixed-case variant: the
			// guard must normalize it to the stored apex ("example.org") before
			// the ownership lookup, still returning 409 rather than leaking a
			// raw 1062 duplicate-key 500 from CreateDomain.
			reqBody := fmt.Sprintf(`{"domain":"WWW.Example.org","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			// The normalized lookup finds a live binding owned by website 99 → 409, not 500.
			assert.Equal(t, http.StatusConflict, rec.Code)
		}, TestOptions)
	})

	t.Run("error_website_broken", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			require.NoError(tb, ctx.DB().Create(createTestIPFSGatewayWebsite(1, userID, TestDomain, cid.MustParse(TestCID), pluginDb.WebsiteStatusActive)).Error)
			mockDNS := helper.SetupDNSServiceMocks()
			mockDNS.EXPECT().CreateZone(mock.Anything, TestDomain, userID).Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 1}, Domain: TestDomain}, nil)
			mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(1), mock.Anything, mock.Anything).Return(nil).Maybe()

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, pluginDb.WebsiteStatusBroken, "test-token")

			mockWebsiteService.EXPECT().CreateWebsite(mock.Anything, mock.AnythingOfType("*db.Website")).Return(mockWebsite, nil)

			// Broken status returns 410 before the response is built, but the
			// handler still resolves (and finds absent) the primary binding
			// during domain enablement, and CreateDomain still fires the
			// service-layer created notification on the primary binding.
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(nil, nil)
			mockWebsiteService.EXPECT().NotifyAdminWebsiteCreated(mock.Anything, uint(1)).Return(nil)

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusGone, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			req := ctx.NewAPIRequest(http.MethodPost, "/api/websites", []byte(reqBody))
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

// Public GET SSL Status Query Tests
// These tests verify the public endpoint for users to query SSL status

func TestAPI_GetSSLStatus(t *testing.T) {
	t.Run("success_with_ready_status", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			userID := uint(1)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := &pluginDb.Website{
				ID:         1,
				UserID:     userID,
				TargetType: string(pluginDb.WebsiteTargetTypeIPFS),
				Status:     string(pluginDb.WebsiteStatusActive),
			}
			mockApex := &pluginDb.WebsiteDomain{
				ID:               1,
				WebsiteID:        1,
				Domain:           TestDomain,
				SSLStatus:        string(pluginDb.SSLStatusReady),
				SSLLastUpdatedAt: func() *time.Time { v := time.Now().UTC(); return &v }(),
			}

			mockWebsiteService.EXPECT().GetWebsiteByDomain(mock.Anything, TestDomain).Return(mockWebsite, pluginDb.DomainNamespaceICANN, nil)
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApex, nil)

			rec := helper.makeRequest(http.MethodGet, "/api/websites/"+TestDomain+"/ssl-status", nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.Equal(t, "ready", response.SSL.Status)
		}, TestOptions)
	})

	t.Run("success_with_pending_status", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			userID := uint(1)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := &pluginDb.Website{
				ID:         1,
				UserID:     userID,
				TargetType: string(pluginDb.WebsiteTargetTypeIPFS),
				Status:     string(pluginDb.WebsiteStatusActive),
			}
			mockApex := &pluginDb.WebsiteDomain{
				ID:               1,
				WebsiteID:        1,
				Domain:           TestDomain,
				SSLStatus:        string(pluginDb.SSLStatusPending),
				SSLLastUpdatedAt: func() *time.Time { v := time.Now().UTC(); return &v }(),
			}

			mockWebsiteService.EXPECT().GetWebsiteByDomain(mock.Anything, TestDomain).Return(mockWebsite, pluginDb.DomainNamespaceICANN, nil)
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApex, nil)

			rec := helper.makeRequest(http.MethodGet, "/api/websites/"+TestDomain+"/ssl-status", nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, "pending", response.SSL.Status)
		}, TestOptions)
	})

	t.Run("error_website_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().GetWebsiteByDomain(mock.Anything, TestDomain).Return(nil, pluginDb.DomainNamespaceICANN, nil)

			rec := helper.makeRequest(http.MethodGet, "/api/websites/"+TestDomain+"/ssl-status", nil)

			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("error_empty_domain", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			rec := helper.makeRequest(http.MethodGet, "/api/websites//ssl-status", nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_UpdateSSLStatus_Webhook(t *testing.T) {
	// setUpWebhookMocks sets up the UpdateSSLStatus / GetWebsite / GetApexDomainBinding
	// mocks shared by the success cases. It returns the website and apex binding used.
	setUpWebhookMocks := func(tb coreTesting.TB, mockWebsiteService *mocks.MockWebsiteService, status pluginDb.SSLStatus, sslError string, ts time.Time) (*pluginDb.Website, *pluginDb.WebsiteDomain) {
		mockWebsite := &pluginDb.Website{
			ID:         1,
			UserID:     uint(1),
			TargetType: string(pluginDb.WebsiteTargetTypeIPFS),
			Status:     string(pluginDb.WebsiteStatusActive),
		}
		mockApex := &pluginDb.WebsiteDomain{
			ID:               1,
			WebsiteID:        1,
			UserID:           1,
			Domain:           TestDomain,
			SSLStatus:        string(status),
			SSLError:         sslError,
			SSLLastUpdatedAt: &ts,
		}
		mockWebsiteService.EXPECT().UpdateSSLStatus(mock.Anything, TestDomain, status, sslError, mock.AnythingOfType("*time.Time")).Return(mockApex, nil)
		mockWebsiteService.EXPECT().GetWebsite(mock.Anything, uint(1), uint(1)).Return(mockWebsite, nil)
		mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApex, nil)
		return mockWebsite, mockApex
	}

	t.Run("success_status_ready", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			timestamp := time.Now().UTC()
			setUpWebhookMocks(tb, mockWebsiteService, pluginDb.SSLStatusReady, "", timestamp)

			reqBody := fmt.Sprintf(`{"status":"ready","timestamp":"%s"}`, timestamp.Format(time.RFC3339))
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.Equal(t, "ready", response.SSL.Status)
		}, TestOptions)
	})

	t.Run("success_status_failed_with_error", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			timestamp := time.Now().UTC()
			setUpWebhookMocks(tb, mockWebsiteService, pluginDb.SSLStatusFailed, "certificate validation failed", timestamp)

			reqBody := fmt.Sprintf(`{"status":"failed","error":"certificate validation failed","timestamp":"%s"}`, timestamp.Format(time.RFC3339))
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.Equal(t, "failed", response.SSL.Status)
			assert.Equal(t, "certificate validation failed", response.SSL.Error)
		}, TestOptions)
	})

	t.Run("success_status_pending", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			timestamp := time.Now().UTC()
			setUpWebhookMocks(tb, mockWebsiteService, pluginDb.SSLStatusPending, "", timestamp)

			reqBody := fmt.Sprintf(`{"status":"pending","timestamp":"%s"}`, timestamp.Format(time.RFC3339))
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.Equal(t, "pending", response.SSL.Status)
		}, TestOptions)
	})

	t.Run("success_status_issuing", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			timestamp := time.Now().UTC()
			setUpWebhookMocks(tb, mockWebsiteService, pluginDb.SSLStatusIssuing, "", timestamp)

			reqBody := fmt.Sprintf(`{"status":"issuing","timestamp":"%s"}`, timestamp.Format(time.RFC3339))
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.Equal(t, "issuing", response.SSL.Status)
		}, TestOptions)
	})

	t.Run("error_invalid_gateway_secret", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			reqBody := `{"status":"ready"}`
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", "wrong-secret", []byte(reqBody))

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})

	t.Run("error_missing_gateway_secret", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", []byte(`{"status":"ready"}`))
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_domain_empty", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			reqBody := `{"status":"ready"}`
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites//ssl-status", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_status_value", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			reqBody := `{"status":"invalid_status"}`
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_timestamp_format", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			reqBody := `{"status":"ready","timestamp":"invalid-timestamp"}`
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_website_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().UpdateSSLStatus(mock.Anything, TestDomain, pluginDb.SSLStatusReady, "", (*time.Time)(nil)).Return(nil, errors.New("website not found"))

			reqBody := `{"status":"ready"}`
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("error_update_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().UpdateSSLStatus(mock.Anything, TestDomain, pluginDb.SSLStatusReady, "", (*time.Time)(nil)).Return(nil, errors.New("database error"))

			reqBody := `{"status":"ready"}`
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("status_transition_pending_to_issuing_to_ready", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			userID := uint(1)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			timestamp1 := time.Now().UTC()
			timestamp2 := timestamp1.Add(time.Minute)
			timestamp3 := timestamp2.Add(time.Hour)

			mockWebsite := &pluginDb.Website{
				ID:         1,
				UserID:     userID,
				TargetType: string(pluginDb.WebsiteTargetTypeIPFS),
				Status:     string(pluginDb.WebsiteStatusActive),
			}

			mockApexPending := &pluginDb.WebsiteDomain{ID: 1, WebsiteID: 1, UserID: userID, Domain: TestDomain, SSLStatus: string(pluginDb.SSLStatusPending), SSLLastUpdatedAt: &timestamp1}
			mockApexIssuing := &pluginDb.WebsiteDomain{ID: 1, WebsiteID: 1, UserID: userID, Domain: TestDomain, SSLStatus: string(pluginDb.SSLStatusIssuing), SSLLastUpdatedAt: &timestamp2}
			mockApexReady := &pluginDb.WebsiteDomain{ID: 1, WebsiteID: 1, UserID: userID, Domain: TestDomain, SSLStatus: string(pluginDb.SSLStatusReady), SSLLastUpdatedAt: &timestamp3}

			mockWebsiteService.EXPECT().UpdateSSLStatus(mock.Anything, TestDomain, pluginDb.SSLStatusPending, "", mock.AnythingOfType("*time.Time")).Return(mockApexPending, nil)
			mockWebsiteService.EXPECT().UpdateSSLStatus(mock.Anything, TestDomain, pluginDb.SSLStatusIssuing, "", mock.AnythingOfType("*time.Time")).Return(mockApexIssuing, nil)
			mockWebsiteService.EXPECT().UpdateSSLStatus(mock.Anything, TestDomain, pluginDb.SSLStatusReady, "", mock.AnythingOfType("*time.Time")).Return(mockApexReady, nil)
			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(mockWebsite, nil).Times(3)
			// Each webhook request resolves the apex binding twice: once when
			// populating the primary-domain response fields and again inside
			// applyApexSSLStatus. Three requests => six GetApexDomainBinding
			// calls, preserved per-status in call order (P,P,I,I,R,R).
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApexPending, nil).Once()
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApexPending, nil).Once()
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApexIssuing, nil).Once()
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApexIssuing, nil).Once()
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApexReady, nil).Once()
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApexReady, nil).Once()

			reqBody1 := fmt.Sprintf(`{"status":"pending","timestamp":"%s"}`, timestamp1.Format(time.RFC3339))
			rec1 := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody1))
			assert.Equal(t, http.StatusOK, rec1.Code)
			var response1 dto.WebsiteResponse
			err := json.Unmarshal(rec1.Body.Bytes(), &response1)
			require.NoError(t, err)
			assert.Equal(t, "pending", response1.SSL.Status)

			reqBody2 := fmt.Sprintf(`{"status":"issuing","timestamp":"%s"}`, timestamp2.Format(time.RFC3339))
			rec2 := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody2))
			assert.Equal(t, http.StatusOK, rec2.Code)
			var response2 dto.WebsiteResponse
			err = json.Unmarshal(rec2.Body.Bytes(), &response2)
			require.NoError(t, err)
			assert.Equal(t, "issuing", response2.SSL.Status)

			reqBody3 := fmt.Sprintf(`{"status":"ready","timestamp":"%s"}`, timestamp3.Format(time.RFC3339))
			rec3 := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/websites/"+TestDomain+"/ssl-status", testGatewaySecret(), []byte(reqBody3))
			assert.Equal(t, http.StatusOK, rec3.Code)
			var response3 dto.WebsiteResponse
			err = json.Unmarshal(rec3.Body.Bytes(), &response3)
			require.NoError(t, err)
			assert.Equal(t, "ready", response3.SSL.Status)
		}, TestOptions)
	})
}

func TestAPI_ListWebsites(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsites := []*pluginDb.Website{
				createMockIPFSWebsite(1, userID, "example1.com", TestCID, pluginDb.WebsiteStatusActive, ""),
				createMockIPNSWebsite(2, userID, "example2.com", TestPeerID, pluginDb.WebsiteStatusPendingValidation, ""),
			}

			mockWebsiteService.EXPECT().ListWebsites(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return(mockWebsites, int64(2), nil)

			// Each listed website's primary (apex) binding is resolved for the
			// response; none of these test websites have one bound.
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(nil, nil)
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(2)).Return(nil, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, float64(2), response["total"])
			items, ok := response["data"].([]interface{})
			require.True(t, ok, "data should be a slice")
			assert.Len(t, items, 2)
		}, TestOptions)
	})

	t.Run("success_with_filters", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsites := []*pluginDb.Website{
				createMockIPFSWebsite(1, userID, TestDomain, TestCID, pluginDb.WebsiteStatusActive, ""),
			}

			mockWebsiteService.EXPECT().ListWebsites(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return(mockWebsites, int64(1), nil)

			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(nil, nil)

			// target_type contains an underscore, so the queryutil filter parser
			// needs explicit bracket notation; a plain target_type=ipfs would be
			// misread as field "target" operator "ype".
			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites?domain=example.com&filters[target_type][eq]=ipfs&status=active", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, float64(1), response["total"])
		}, TestOptions)
	})

	t.Run("empty_list", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().ListWebsites(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.Website{}, int64(0), nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, float64(0), response["total"])
			assert.Empty(t, response["items"])
		}, TestOptions)
	})

	t.Run("success_with_pagination", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			// Create 5 websites, but return only 2 for page 1
			mockWebsites := []*pluginDb.Website{
				createMockIPFSWebsite(1, userID, "example1.com", TestCID, pluginDb.WebsiteStatusActive, ""),
				createMockIPNSWebsite(2, userID, "example2.com", TestPeerID, pluginDb.WebsiteStatusPendingValidation, ""),
			}

			mockWebsiteService.EXPECT().ListWebsites(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return(mockWebsites, int64(5), nil)

			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(nil, nil)
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(2)).Return(nil, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites?_start=0&_end=2", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, float64(5), response["total"])
			// The library sets X-Total-Count to the number of items in this page
			// (2), not the total (5); the true total is in response["total"].
			assert.Equal(t, "2", rec.Header().Get("X-Total-Count"))
			items, ok := response["data"].([]interface{})
			require.True(t, ok, "data should be a slice")
			assert.Len(t, items, 2)
		}, TestOptions)
	})

	t.Run("error_list_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().ListWebsites(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return(nil, int64(0), errors.New("list failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodGet, "/api/websites", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_GetWebsite(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, pluginDb.WebsiteStatusActive, "")

			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(mockWebsite, nil)

			// The primary (apex) binding supplies the response's Domain/DNS fields.
			mockApex := &pluginDb.WebsiteDomain{
				ID:        1,
				WebsiteID: 1,
				UserID:    userID,
				Domain:    TestDomain,
			}
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApex, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/1", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
		}, TestOptions)
	})

	t.Run("error_invalid_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/invalid", token, nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(999)).Return(nil, gorm.ErrRecordNotFound)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/999", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_website_broken", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, pluginDb.WebsiteStatusBroken, "")

			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(mockWebsite, nil)

			// Broken: the isBroken path still resolves the apex binding for the
			// 410 response body.
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(nil, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/1", token, nil)

			assert.Equal(t, http.StatusGone, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodGet, "/api/websites/1", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_UpdateWebsite(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			// The handler runs the real DelegatedDomainService.CreateDomain when
			// changing the primary domain, which looks the website up in the DB
			// and persists a new binding there. Persist a real website row so
			// that lookup succeeds.
			require.NoError(tb, ctx.DB().Create(createTestIPFSGatewayWebsite(1, userID, "example.com", cid.MustParse(TestCID), pluginDb.WebsiteStatusActive)).Error)

			// CreateDomain for the ICANN primary domain creates a DNS zone and
			// DNSLink record through the DNS service; apex is skipped (no
			// gateway config in the harness).
			mockDNS := helper.SetupDNSServiceMocks()
			mockDNS.EXPECT().CreateZone(mock.Anything, "updated-example.com", userID).Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 1}, Domain: "updated-example.com"}, nil)
			mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(1), mock.Anything, mock.Anything).Return(nil).Maybe()

			mockWebsite := createMockIPFSWebsite(1, userID, "updated-example.com", TestCID, pluginDb.WebsiteStatusActive, "")

			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(1), mock.AnythingOfType("map[string]interface {}")).Return(mockWebsite, nil)

			// Changing the primary domain runs the real delegate domain service
			// (CreateDomain persists a binding in the test DB), then repoints the
			// website's primary via SetPrimaryDomain and resolves it back for the
			// response.
			mockApex := &pluginDb.WebsiteDomain{
				ID:        2,
				WebsiteID: 1,
				Domain:    "updated-example.com",
			}
			mockWebsiteService.EXPECT().SetPrimaryDomain(mock.Anything, userID, uint(1), mock.Anything).Return(mockApex, nil)
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApex, nil).Maybe()
			// Updating a website (even one that lacked a primary domain) must
			// NOT emit a "website created" notification — that is reserved for
			// genuine creations via the create handler.
			mockWebsiteService.AssertNotCalled(tb, "NotifyAdminWebsiteCreated", mock.Anything, mock.Anything)

			reqBody := fmt.Sprintf(`{"domain":"updated-example.com","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, "updated-example.com", response.Domain)
		}, TestOptions)
	})

	t.Run("error_platform_root_domain_rejected_on_update", func(t *testing.T) {
		// Regression: the update path (PUT /websites/:id with domain=...) must
		// reject a platform root apex just like createWebsite/createDomain.
		// Otherwise an end user could create a website, then re-point its
		// primary domain onto the operator-owned apex to bypass the guard.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			// Persist a real website row so the update handler reaches the
			// domain-change branch before the guard rejects it.
			require.NoError(tb, ctx.DB().Create(createTestIPFSGatewayWebsite(1, userID, "example.com", cid.MustParse(TestCID), pluginDb.WebsiteStatusActive)).Error)

			// Register an enabled platform root (as the admin flow would).
			require.NoError(tb, ctx.DB().Create(&pluginDb.PlatformDomain{
				Domain:    "platform.test",
				Namespace: pluginDb.DomainNamespaceICANN,
				ZoneID:    1,
				Enabled:   true,
			}).Error)

			reqBody := fmt.Sprintf(`{"domain":"platform.test","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("success_redeploy_existing_primary_reuses_binding", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			// Re-deploying to a domain that is already THIS website's primary
			// binding must reuse the existing binding (idempotent), not attempt
			// to create a second one (which would trip the unique key and 500).
			// Persist the website and its live (domain, namespace) binding so
			// the real DelegatedDomainService.GetWebsiteDomainByDomainAndNamespace
			// resolves it and the reuse path runs.
			require.NoError(tb, ctx.DB().Create(createTestIPFSGatewayWebsite(1, userID, "get.pinner.xyz", cid.MustParse(TestCID), pluginDb.WebsiteStatusActive)).Error)
			require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
				ID:        1,
				WebsiteID: 1,
				UserID:    userID,
				Domain:    "get.pinner.xyz",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusActive,
			}).Error)

			mockWebsite := createMockIPFSWebsite(1, userID, "get.pinner.xyz", TestCID, pluginDb.WebsiteStatusActive, "")
			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(1), mock.AnythingOfType("map[string]interface {}")).Return(mockWebsite, nil)

			// The existing binding is already primary for this website, so
			// SetPrimaryDomain is a no-op returning the same binding. The reuse
			// path preserves the binding's DNS hosting state (no DNS zone or
			// SetDomainDNSEnabled call without an explicit dns_hosting_enabled).
			existingBinding := &pluginDb.WebsiteDomain{
				ID:        1,
				WebsiteID: 1,
				UserID:    userID,
				Domain:    "get.pinner.xyz",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusActive,
			}
			mockWebsiteService.EXPECT().SetPrimaryDomain(mock.Anything, userID, uint(1), uint(1)).Return(existingBinding, nil)
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(existingBinding, nil).Maybe()

			reqBody := fmt.Sprintf(`{"domain":"get.pinner.xyz","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			// The reported bug returned 500 ("Duplicate entry ... for key
			// 'website_domains.uk_domain_namespace'"); the fix reuses the
			// existing binding and succeeds.
			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, "get.pinner.xyz", response.Domain)
		}, TestOptions)
	})

	t.Run("success_redeploy_preserves_self_hosted_dns_state", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			// Re-deploying (no dns_hosting_enabled in the request) to a primary
			// binding the user deliberately self-hosted (DNS hosting off, no
			// portal zone) must preserve that state, not silently re-provision
			// DNS hosting. Only an explicit dns_hosting_enabled override may
			// change it.
			require.NoError(tb, ctx.DB().Create(createTestIPFSGatewayWebsite(1, userID, "selfhost.xyz", cid.MustParse(TestCID), pluginDb.WebsiteStatusActive)).Error)
			require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
				ID:                1,
				WebsiteID:         1,
				UserID:            userID,
				Domain:            "selfhost.xyz",
				Namespace:         pluginDb.DomainNamespaceICANN,
				Status:            pluginDb.DomainStatusSelfHosted,
				DNSHostingEnabled: false,
				ZoneID:            0,
			}).Error)

			mockWebsite := createMockIPFSWebsite(1, userID, "selfhost.xyz", TestCID, pluginDb.WebsiteStatusActive, "")
			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(1), mock.AnythingOfType("map[string]interface {}")).Return(mockWebsite, nil)

			selfHostedBinding := &pluginDb.WebsiteDomain{
				ID:                1,
				WebsiteID:         1,
				UserID:            userID,
				Domain:            "selfhost.xyz",
				Namespace:         pluginDb.DomainNamespaceICANN,
				Status:            pluginDb.DomainStatusSelfHosted,
				DNSHostingEnabled: false,
				ZoneID:            0,
			}
			mockWebsiteService.EXPECT().SetPrimaryDomain(mock.Anything, userID, uint(1), uint(1)).Return(selfHostedBinding, nil)
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(selfHostedBinding, nil).Maybe()

			reqBody := fmt.Sprintf(`{"domain":"selfhost.xyz","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)
			// No silent DNS takeover: the self-hosted binding must not be
			// flipped to portal-managed DNS merely by a target re-deploy.
			mockWebsiteService.AssertNotCalled(tb, "SetDomainDNSEnabled", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		}, TestOptions)
	})

	t.Run("error_domain_owned_by_another_website_conflict", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			// A domain live-bound to a DIFFERENT website must be refused as an
			// ownership conflict (409), never re-bound to this website.
			require.NoError(tb, ctx.DB().Create(createTestIPFSGatewayWebsite(1, userID, "conflict.xyz", cid.MustParse(TestCID), pluginDb.WebsiteStatusActive)).Error)
			require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
				ID:        1,
				WebsiteID: 99, // owned by another website
				UserID:    userID,
				Domain:    "conflict.xyz",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusActive,
			}).Error)

			mockWebsite := createMockIPFSWebsite(1, userID, "conflict.xyz", TestCID, pluginDb.WebsiteStatusActive, "")
			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(1), mock.AnythingOfType("map[string]interface {}")).Return(mockWebsite, nil)

			reqBody := fmt.Sprintf(`{"domain":"conflict.xyz","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			// The lookup finds a live binding owned by website 99 → 409, not 500.
			assert.Equal(t, http.StatusConflict, rec.Code)
		}, TestOptions)
	})

	t.Run("success_dns_hosting_only", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, "example.com", TestCID, pluginDb.WebsiteStatusActive, "")

			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(1), mock.AnythingOfType("map[string]interface {}")).Return(mockWebsite, nil)

			// dns_hosting_enabled toggles DNS on the primary domain binding.
			mockApex := &pluginDb.WebsiteDomain{
				ID:                1,
				WebsiteID:         1,
				Domain:            "example.com",
				DNSHostingEnabled: true,
			}
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApex, nil)
			mockWebsiteService.EXPECT().SetDomainDNSEnabled(mock.Anything, userID, uint(1), uint(1), true).Return(mockApex, nil)

			reqBody := `{"dns_hosting_enabled":true}`
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
		}, TestOptions)
	})

	t.Run("error_no_fields", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := `{}`
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("success_target_type_ipns_without_target_hash", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, "example.com", TestCID, pluginDb.WebsiteStatusActive, "")

			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(1), mock.AnythingOfType("map[string]interface {}")).Return(mockWebsite, nil)

			// No domain/DNS fields in the update, so the handler falls back to
			// resolving the current apex binding for the response.
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(nil, nil)

			reqBody := `{"target_type":"ipns"}`
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)
		}, TestOptions)
	})

	t.Run("error_target_hash_without_target_type", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/invalid", token, []byte(reqBody))

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_request", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_target_type", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"invalid","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_cid", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"invalid-cid"}`, TestDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(999), mock.AnythingOfType("map[string]interface {}")).Return(nil, gorm.ErrRecordNotFound)

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/999", token, []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_update_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(1), mock.AnythingOfType("map[string]interface {}")).Return(nil, errors.New("update failed"))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_cid_not_pinned", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(1), mock.AnythingOfType("map[string]interface {}")).Return(nil, fmt.Errorf("CID validation failed: %w", pluginservice.ErrCIDNotPinned))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			errData, ok := body["error"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "CidNotPinned", errData["reason"])
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			req := ctx.NewAPIRequest(http.MethodPut, "/api/websites/1", []byte(reqBody))
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_DeleteWebsite(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().DeleteWebsite(mock.Anything, userID, uint(1)).Return(nil)

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/1", token, nil)

			assert.Equal(t, http.StatusNoContent, rec.Code)
			assert.Equal(t, 0, rec.Body.Len())
		}, TestOptions)
	})

	t.Run("error_invalid_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/invalid", token, nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().DeleteWebsite(mock.Anything, userID, uint(999)).Return(gorm.ErrRecordNotFound)

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/999", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_delete_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().DeleteWebsite(mock.Anything, userID, uint(1)).Return(errors.New("delete failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/1", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodDelete, "/api/websites/1", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_ValidateWebsiteDNS(t *testing.T) {
	t.Run("success_valid", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, pluginDb.WebsiteStatusActive, "")

			mockWebsiteService.EXPECT().ValidateDNS(mock.Anything, userID, uint(1)).Return(pluginCore.ValidateDNSResult{Valid: true, Message: "DNS validation successful for test.example.com", Reason: pluginCore.ValidationReasonValidated}, nil)
			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(mockWebsite, nil)

			// The response's Domain comes from the primary (apex) binding.
			mockApex := &pluginDb.WebsiteDomain{
				ID:        1,
				WebsiteID: 1,
				UserID:    userID,
				Domain:    TestDomain,
			}
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApex, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/1/validate", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteValidateResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.True(t, response.Valid)
			assert.Equal(t, "validated", response.Reason)
		}, TestOptions)
	})

	t.Run("success_invalid", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, pluginDb.WebsiteStatusPendingValidation, "")

			mockWebsiteService.EXPECT().ValidateDNS(mock.Anything, userID, uint(1)).Return(pluginCore.ValidateDNSResult{Valid: false, Message: "DNS validation failed: missing validation token for test.example.com", Reason: pluginCore.ValidationReasonTokenMissing}, nil)
			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(mockWebsite, nil)

			// The response's Domain comes from the primary (apex) binding.
			mockApex := &pluginDb.WebsiteDomain{
				ID:        1,
				WebsiteID: 1,
				UserID:    userID,
				Domain:    TestDomain,
			}
			mockWebsiteService.EXPECT().GetApexDomainBinding(mock.Anything, uint(1)).Return(mockApex, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/1/validate", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteValidateResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.False(t, response.Valid)
			assert.Equal(t, "token_missing", response.Reason)
		}, TestOptions)
	})

	t.Run("error_invalid_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/invalid/validate", token, nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("error_validation_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().ValidateDNS(mock.Anything, userID, uint(1)).Return(pluginCore.ValidateDNSResult{}, errors.New("validation failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/1/validate", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_dns_resolution_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			dnsErr := &net.DNSError{Err: "no such host", Name: "pinner-verify." + TestDomain, IsNotFound: true}
			mockWebsiteService.EXPECT().ValidateDNS(mock.Anything, userID, uint(1)).
				Return(pluginCore.ValidateDNSResult{}, fmt.Errorf("DNS TXT lookup failed for %s: %w", TestDomain, dnsErr))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/1/validate", token, nil)

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			errData, ok := body["error"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "DnsValidationFailed", errData["reason"])
		}, TestOptions)
	})

	t.Run("error_get_website_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().ValidateDNS(mock.Anything, userID, uint(1)).Return(pluginCore.ValidateDNSResult{Valid: true, Message: "DNS validation successful", Reason: pluginCore.ValidationReasonValidated}, nil)
			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(nil, errors.New("get failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/1/validate", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodPost, "/api/websites/1/validate", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestIsDuplicateKeyError(t *testing.T) {
	// GORM sentinel — only returned when gorm.Config{TranslateError:true} is set.
	assert.True(t, isDuplicateKeyError(gorm.ErrDuplicatedKey))

	// Raw MySQL 1062 — the actual production path when TranslateError is off.
	assert.True(t, isDuplicateKeyError(&mysql.MySQLError{Number: 1062}))
	assert.False(t, isDuplicateKeyError(&mysql.MySQLError{Number: 1146})) // table missing

	// Driver-agnostic string fallback (as surfaced on real MySQL / SQLite).
	assert.True(t, isDuplicateKeyError(errors.New("Error 1062 (23000): Duplicate entry 'test2.web3ready.org-icann' for key 'website_domains.uk_domain_namespace'")))
	assert.True(t, isDuplicateKeyError(errors.New("UNIQUE constraint failed: website_domains.domain, website_domains.namespace")))

	// Non-duplicate / nil errors are not flagged.
	assert.False(t, isDuplicateKeyError(errors.New("some other database error")))
	assert.False(t, isDuplicateKeyError(nil))
}
