package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/dane"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/datatypes"
)

func TestAPI_DeleteDomain(t *testing.T) {
	t.Run("hard_delete_allows_recreate_same_domain_namespace", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			// Create a website to attach the domain to.
			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			// Create a domain binding.
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID,
				Domain:    "example.com",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusDraft,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			// Delete it via the API.
			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/1/domains/1", token, nil)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// The record should be gone (hard-deleted, not soft-deleted).
			var count int64
			ctx.DB().Unscoped().Model(&pluginDb.WebsiteDomain{}).Where("domain = ? AND namespace = ?", "example.com", pluginDb.DomainNamespaceICANN).Count(&count)
			assert.Zero(t, count, "domain should be hard-deleted, not soft-deleted")

			// Re-create the same domain+namespace — should succeed (no unique collision).
			wd2 := &pluginDb.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID,
				Domain:    "example.com",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusDraft,
			}
			err := ctx.DB().Create(wd2).Error
			assert.NoError(t, err, "re-creating same domain+namespace after hard delete should succeed")
		}, TestOptions)
	})

	t.Run("delete_other_users_domain_returns_404", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			// Create a website owned by the authenticated user.
			website := createTestIPFSGatewayWebsite(1, userID, "other.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			// Create a domain binding owned by a different user.
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID + 100, // different user
				Domain:    "other.com",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusDraft,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			// Authenticated user tries to delete the other user's domain.
			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/1/domains/1", token, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_DomainDNSRequirements(t *testing.T) {
	t.Run("returns_delegation_for_bound_domain", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			// The DS is computed live from PowerDNS on demand, not stored. Stub
			// the current active signing key's DS so the handler injects it.
			mockDNS := helper.SetupDNSServiceMocks()
			mockDNS.EXPECT().GetActiveDNSSECDS(mock.Anything, uint(0)).Return(
				"60776 13 2 3b35deed97def5fbb5ce939cd5b9036f12db0ccc2e1cb40bb4c565c168c66116", nil,
			).Maybe()

			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			wd := &pluginDb.WebsiteDomain{
				WebsiteID:   1,
				UserID:      userID,
				Domain:      "lumeweb",
				Namespace:   pluginDb.DomainNamespaceHNS,
				Status:      pluginDb.DomainStatusRecordsGenerated,
				ZoneName:    "lumeweb.",
				GatewayHost: "gateway.lumeweb.com",
				DelegationData: datatypes.JSONMap{
					"mode": "delegated",
					"parent_records": []map[string]any{
						{"type": "NS", "value": "ns1.lumeweb,ns2.lumeweb"},
						// Stale stored DS — must be REPLACED by the live value.
						{"type": "DS", "value": "lumeweb. 3600 IN DS 12345 13 2 <digest>"},
					},
					"authoritative_records": []map[string]any{
						{"type": "NS", "value": "ns1.lumeweb\nns2.lumeweb"},
					},
				},
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/1/domains/1/dns-requirements", token, nil)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			var resp dto.DomainResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "lumeweb", resp.Domain)
			assert.Equal(t, "hns", resp.Namespace)
			require.NotNil(t, resp.Delegation)
			assert.Equal(t, "delegated", resp.Delegation.Mode)
			// No first-class DS field — the live DS is injected into
			// parent_records, replacing the stale stored DS entry.
			require.Len(t, resp.Delegation.ParentRecords, 2)
			assert.Equal(t, "NS", resp.Delegation.ParentRecords[0].Type)
			dsRec := resp.Delegation.ParentRecords[1]
			assert.Equal(t, "DS", dsRec.Type)
			assert.Equal(t, "60776 13 2 3b35deed97def5fbb5ce939cd5b9036f12db0ccc2e1cb40bb4c565c168c66116", dsRec.Value)
			// Active signing key means DNSSEC is explicitly reported as enabled,
			// so an enabled zone is never a silent gap.
			assert.Equal(t, "enabled", resp.Delegation.DNSSEC)
			assert.Empty(t, resp.Delegation.DNSSECError)
		}, TestOptions)
	})

	t.Run("ds_unresolvable_removes_stale_ds", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			// PowerDNS is unreachable / key rollover: GetActiveDNSSECDS errors.
			mockDNS := helper.SetupDNSServiceMocks()
			mockDNS.EXPECT().GetActiveDNSSECDS(mock.Anything, uint(0)).Return(
				"", errors.New("PowerDNS unreachable"),
			).Maybe()

			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			wd := &pluginDb.WebsiteDomain{
				WebsiteID:   1,
				UserID:      userID,
				Domain:      "lumeweb",
				Namespace:   pluginDb.DomainNamespaceHNS,
				Status:      pluginDb.DomainStatusRecordsGenerated,
				ZoneName:    "lumeweb.",
				GatewayHost: "gateway.lumeweb.com",
				DelegationData: datatypes.JSONMap{
					"mode": "delegated",
					"parent_records": []map[string]any{
						{"type": "NS", "value": "ns1.lumeweb,ns2.lumeweb"},
						// Stale stored DS — must be DROPPED, not presented as current.
						{"type": "DS", "value": "lumeweb. 3600 IN DS 12345 13 2 <digest>"},
					},
					"authoritative_records": []map[string]any{
						{"type": "NS", "value": "ns1.lumeweb\nns2.lumeweb"},
					},
				},
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/1/domains/1/dns-requirements", token, nil)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			var resp dto.DomainResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.NotNil(t, resp.Delegation)
			// Stale DS must be removed from parent_records so a value whose
			// correctness cannot be confirmed is never presented as current.
			require.Len(t, resp.Delegation.ParentRecords, 1)
			assert.Equal(t, "NS", resp.Delegation.ParentRecords[0].Type)
			// Resolution error is surfaced explicitly so the user can diagnose
			// (PowerDNS down / key rollover) rather than see a bare missing DS.
			assert.Equal(t, "error", resp.Delegation.DNSSEC)
			assert.Contains(t, resp.Delegation.DNSSECError, "PowerDNS unreachable")
		}, TestOptions)
	})

	t.Run("ds_empty_removes_stale_ds", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			// Zone has no active signing key (e.g. after key rotation): the
			// documented GetActiveDNSSECDS ("", nil) outcome.
			mockDNS := helper.SetupDNSServiceMocks()
			mockDNS.EXPECT().GetActiveDNSSECDS(mock.Anything, uint(0)).Return(
				"", nil,
			).Maybe()

			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			wd := &pluginDb.WebsiteDomain{
				WebsiteID:   1,
				UserID:      userID,
				Domain:      "lumeweb",
				Namespace:   pluginDb.DomainNamespaceHNS,
				Status:      pluginDb.DomainStatusRecordsGenerated,
				ZoneName:    "lumeweb.",
				GatewayHost: "gateway.lumeweb.com",
				DelegationData: datatypes.JSONMap{
					"mode": "delegated",
					"parent_records": []map[string]any{
						{"type": "NS", "value": "ns1.lumeweb,ns2.lumeweb"},
						// Stale stored DS — must be DROPPED when no live key exists.
						{"type": "DS", "value": "lumeweb. 3600 IN DS 12345 13 2 <digest>"},
					},
					"authoritative_records": []map[string]any{
						{"type": "NS", "value": "ns1.lumeweb\nns2.lumeweb"},
					},
				},
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/1/domains/1/dns-requirements", token, nil)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			var resp dto.DomainResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.NotNil(t, resp.Delegation)
			// No live signing key: stale DS must be removed, not presented.
			require.Len(t, resp.Delegation.ParentRecords, 1)
			assert.Equal(t, "NS", resp.Delegation.ParentRecords[0].Type)
			// "No active key" on a managed zone is surfaced as disabled (not
			// silent), telling the user DNSSEC isn't set up yet — the verify
			// self-heal will mint the key on the next run.
			assert.Equal(t, "disabled", resp.Delegation.DNSSEC)
			assert.Contains(t, resp.Delegation.DNSSECError, "no active signing key")
		}, TestOptions)
	})

	t.Run("missing_domain_returns_404", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _, _, _ := helper.SetupAuthenticatedTest()

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/1/domains/999/dns-requirements", token, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_DANERepublish(t *testing.T) {
	republishPath := func(websiteID, domainID int) string {
		return fmt.Sprintf("/api/websites/%d/domains/%d/dane/republish", websiteID, domainID)
	}
	seedWebsite := func(tb coreTesting.TB, ctx coreTesting.TestContext, userID uint, domain string) uint {
		testCID := util.GenerateTestCID(t, "test data")
		website := createTestIPFSGatewayWebsite(1, userID, domain, testCID, pluginDb.WebsiteStatusActive)
		require.NoError(t, ctx.DB().Create(website).Error)
		return website.ID
	}
	seedDomain := func(tb coreTesting.TB, ctx coreTesting.TestContext, websiteID, userID uint, domain string, ns pluginDb.DomainNamespace) uint {
		wd := &pluginDb.WebsiteDomain{
			WebsiteID: websiteID,
			UserID:    userID,
			Domain:    domain,
			Namespace: ns,
			Status:    pluginDb.DomainStatusDraft,
			ZoneID:    42,
		}
		require.NoError(t, ctx.DB().Create(wd).Error)
		return wd.ID
	}

	t.Run("missing_domain_returns_404", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _, _, _ := helper.SetupAuthenticatedTest()

			rec := helper.makeAuthenticatedRequest(http.MethodPost, republishPath(1, 999), token, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("other_users_domain_returns_404", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, _, _ := helper.SetupAuthenticatedTest()

			websiteID := seedWebsite(t, ctx, userID, "other.com")
			// Domain owned by a different user.
			seedDomain(t, ctx, websiteID, userID+100, "other.com", pluginDb.DomainNamespaceHNS)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, republishPath(int(websiteID), 1), token, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("icann_domain_rejected_409", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, _, _ := helper.SetupAuthenticatedTest()

			websiteID := seedWebsite(t, ctx, userID, "icann.test")
			domainID := seedDomain(t, ctx, websiteID, userID, "icann.test", pluginDb.DomainNamespaceICANN)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, republishPath(int(websiteID), int(domainID)), token, nil)
			assert.Equal(t, http.StatusConflict, rec.Code, "ICANN namespace has no managed-zone DANE TLSA")
		}, TestOptions)
	})

	t.Run("hns_no_stored_cert_returns_409", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, _, _ := helper.SetupAuthenticatedTest()

			websiteID := seedWebsite(t, ctx, userID, "hns.test")
			domainID := seedDomain(t, ctx, websiteID, userID, "hns.test", pluginDb.DomainNamespaceHNS)

			// No ProtocolData -> GetCertificateKey returns ErrRecordNotFound -> 409.
			rec := helper.makeAuthenticatedRequest(http.MethodPost, republishPath(int(websiteID), int(domainID)), token, nil)
			assert.Equal(t, http.StatusConflict, rec.Code, "HNS domain with no stored cert cannot be republished")
		}, TestOptions)
	})

	t.Run("hns_no_assigned_zone_returns_409", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, _, _ := helper.SetupAuthenticatedTest()

			websiteID := seedWebsite(t, ctx, userID, "hns-zone.test")
			// ZoneID 0 (no assigned managed zone) but no stored cert either; the
			// handler must reject on the missing zone BEFORE touching the cert.
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: websiteID,
				UserID:    userID,
				Domain:    "hns-zone.test",
				Namespace: pluginDb.DomainNamespaceHNS,
				Status:    pluginDb.DomainStatusDraft,
				ZoneID:    0,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, republishPath(int(websiteID), int(wd.ID)), token, nil)
			assert.Equal(t, http.StatusConflict, rec.Code, "HNS domain with no assigned managed zone cannot be republished")
		}, TestOptions)
	})

	t.Run("hns_with_stored_cert_republishes", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, _, _ := helper.SetupAuthenticatedTest()

			website := createTestIPFSGatewayWebsite(1, userID, "hns-repub.test", util.GenerateTestCID(t, "td"), pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)
			websiteID := website.ID

			// Seed an HNS domain with an assigned managed zone.
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: websiteID,
				UserID:    userID,
				Domain:    "hns-repub.test",
				Namespace: pluginDb.DomainNamespaceHNS,
				Status:    pluginDb.DomainStatusDraft,
				ZoneID:    42,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			// Populate the stored cert/key path by pushing a real cert through the
			// service (same path the cert webhook uses). This writes the encrypted
			// private key into ProtocolData, so GetCertificateKey can decrypt it.
			svc := core.GetService[*domain.DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)
			keyPEM := mustGenerateTestKey(t)
			certPEM := mustIssueTestCert(t, keyPEM, "hns-repub.test")
			mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, mockDNS)
			// The seeding push and the HTTP republish each call SetTLSARecord(zoneID=42).
			mockDNS.On("SetTLSARecord", mock.Anything, uint(42), mock.Anything).Return(nil).Times(2)
			_, _, err := svc.UpdateTLSAFromCert(ctx, "hns", wd.Domain, certPEM, keyPEM)
			require.NoError(t, err, "seeding stored cert via UpdateTLSAFromCert")

			mockDNS.AssertCalled(tb, "SetTLSARecord", mock.Anything, uint(42), mock.Anything)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, republishPath(int(websiteID), int(wd.ID)), token, nil)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			var resp dto.DomainDANERepublishResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "hns-repub.test", resp.Domain)
			assert.Equal(t, "hns", resp.Namespace)
			require.NotEmpty(t, resp.TLSARData)
			require.NotEmpty(t, resp.OwnerName)
			assert.Contains(t, resp.TLSARecord, "TLSA")

			// The DNS publish must have happened for the managed zone.
			mockDNS.AssertNumberOfCalls(tb, "SetTLSARecord", 2)
		}, daneRepublishTestOptions)
	})
}

func TestAPI_UpdateDomain(t *testing.T) {
	t.Run("toggle_dns_hosting_enabled", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			wd := &pluginDb.WebsiteDomain{
				WebsiteID:         1,
				UserID:            userID,
				Domain:            "example.com",
				Namespace:         pluginDb.DomainNamespaceICANN,
				Status:            pluginDb.DomainStatusDraft,
				DNSHostingEnabled: false,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
			updated := *wd
			updated.DNSHostingEnabled = true
			mockWebsiteService.EXPECT().SetDomainDNSEnabled(mock.Anything, userID, uint(1), uint(1), true).Return(&updated, nil)

			reqBody := `{"dns_hosting_enabled":true}`
			rec := helper.makeAuthenticatedRequest(http.MethodPatch, "/api/websites/1/domains/1", token, []byte(reqBody))
			assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			var resp dto.DomainResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, uint(1), resp.ID)
			assert.True(t, resp.DNSHostingEnabled)
		}, TestOptions)
	})

	t.Run("set_primary", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID,
				Domain:    "example.com",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusDraft,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
			mockWebsiteService.EXPECT().SetPrimaryDomain(mock.Anything, userID, uint(1), uint(1)).Return(wd, nil)

			reqBody := `{"primary":true}`
			rec := helper.makeAuthenticatedRequest(http.MethodPatch, "/api/websites/1/domains/1", token, []byte(reqBody))
			assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		}, TestOptions)
	})

	t.Run("toggle_dns_and_set_primary", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1,
				UserID:    userID,
				Domain:    "example.com",
				Namespace: pluginDb.DomainNamespaceICANN,
				Status:    pluginDb.DomainStatusDraft,
			}
			require.NoError(t, ctx.DB().Create(wd).Error)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
			updated := *wd
			updated.DNSHostingEnabled = true
			mockWebsiteService.EXPECT().SetPrimaryDomain(mock.Anything, userID, uint(1), uint(1)).Return(wd, nil)
			mockWebsiteService.EXPECT().SetDomainDNSEnabled(mock.Anything, userID, uint(1), uint(1), true).Return(&updated, nil)

			reqBody := `{"dns_hosting_enabled":true,"primary":true}`
			rec := helper.makeAuthenticatedRequest(http.MethodPatch, "/api/websites/1/domains/1", token, []byte(reqBody))
			assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		}, TestOptions)
	})

	t.Run("not_found", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _, _, _ := helper.SetupAuthenticatedTest()

			reqBody := `{"dns_hosting_enabled":true}`
			rec := helper.makeAuthenticatedRequest(http.MethodPatch, "/api/websites/1/domains/999", token, []byte(reqBody))
			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("no_updates_returns_422", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID, testCID, _ := helper.SetupAuthenticatedTest()

			website := createTestIPFSGatewayWebsite(1, userID, "example.com", testCID, pluginDb.WebsiteStatusActive)
			require.NoError(t, ctx.DB().Create(website).Error)

			reqBody := `{}`
			rec := helper.makeAuthenticatedRequest(http.MethodPatch, "/api/websites/1/domains/1", token, []byte(reqBody))
			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})
}

// testDANEKey is the fixed 32-byte AES-256 key (base64) used to encrypt the DANE
// private key at rest. It matches the domain package's testDANEKey and is an
// at-rest encryption key, not a secret literal.
const testDANEKey = "IUf7FMs69krvqJGFn7y8U2jfurNf8bxynXFQBGnP7cI="

// daneRepublishTestOptions wires the DnsConfig DANE key-encryption key so the
// republish handler's GetCertificateKey can decrypt the stored private key.
var daneRepublishTestOptions = coreTesting.CombineOptions(
	TestOptions,
	coreTesting.WithConfig("plugin.ipfs.service.dns.dane_key_encryption_key", testDANEKey),
)

func mustGenerateTestKey(t testing.TB) string {
	t.Helper()
	_, keyPEM, err := dane.GenerateSelfSignedECDSA([]string{"example"}, time.Now().AddDate(1, 0, 0))
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return keyPEM
}

func mustIssueTestCert(t testing.TB, _ string, domain string) string {
	t.Helper()
	certPEM, _, err := dane.GenerateSelfSignedECDSA([]string{domain}, time.Now().AddDate(1, 0, 0))
	if err != nil {
		t.Fatalf("issue cert: %v", err)
	}
	return certPEM
}
