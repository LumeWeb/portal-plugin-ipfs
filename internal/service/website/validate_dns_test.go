package website

import (
	"context"
	"fmt"
	"testing"
	"time"

	dnslink "github.com/dnslink-std/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func setMockResolver(ws pluginCore.WebsiteService, r DNSResolver) {
	svc, ok := ws.(*WebsiteServiceDefault)
	if !ok {
		panic("setMockResolver: service is not *WebsiteServiceDefault")
	}
	svc.resolver = r
}

type testDelegatedDomainService struct {
	uses      func(string) bool
	verify    func(context.Context, *pluginDb.WebsiteDomain) (bool, error)
	getNs     func(string) (string, bool)
	getByName func(context.Context, string) (*pluginDb.WebsiteDomain, error)
}

func (t *testDelegatedDomainService) UsesDelegationForOwnership(d string) bool {
	if t.uses != nil {
		return t.uses(d)
	}
	return false
}

func (t *testDelegatedDomainService) VerifyDomain(ctx context.Context, wd *pluginDb.WebsiteDomain) (bool, error) {
	if t.verify != nil {
		return t.verify(ctx, wd)
	}
	return true, nil
}

func (t *testDelegatedDomainService) GetNamespaceForDomain(d string) (string, bool) {
	if t.getNs != nil {
		return t.getNs(d)
	}
	// Default: for .hns tests, return hns if domain ends with .hns or contains hns
	if len(d) > 4 && d[len(d)-4:] == ".hns" {
		return string(pluginDb.DomainNamespaceHNS), true
	}
	return string(pluginDb.DomainNamespaceICANN), true
}

func (t *testDelegatedDomainService) GetWebsiteDomainByName(ctx context.Context, domain string) (*pluginDb.WebsiteDomain, error) {
	if t.getByName != nil {
		return t.getByName(ctx, domain)
	}
	return nil, gorm.ErrRecordNotFound
}

func (t *testDelegatedDomainService) GetPendingWebsiteDomainsPaginated(ctx context.Context, status pluginDb.DomainStatus, limit, offset int) ([]pluginDb.WebsiteDomain, error) {
	return nil, nil
}

func setMockDelegatedDomainSvc(ws pluginCore.WebsiteService, d delegatedDomainService) {
	svc, ok := ws.(*WebsiteServiceDefault)
	if !ok {
		panic("setMockDelegatedDomainSvc: service is not *WebsiteServiceDefault")
	}
	svc.delegatedDomainSvc = d
}

func TestValidateDNS_PendingValidation_ValidDNSLinkAndToken_ReturnsValidated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "validate-test")
		website := createTestIPFSWebsite(testUserID1, "validate-pending.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, created)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "validate-pending.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("validate-pending.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		mockResolver.EXPECT().LookupTXT(mock.Anything, "lumeweb-verify.validate-pending.com").Return([]string{
			fmt.Sprintf("lumeweb-verify=%s", created.ValidationToken),
		}, nil)
		setMockResolver(ws, mockResolver)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonValidated, result.Reason)

		final, err := ws.GetWebsite(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.WebsiteStatusActive), final.Status)
		assert.False(tb, final.IsExpired(), "validation expiry should be refreshed after successful validation")
	}, TestOptions)
}

func TestValidateDNS_PendingValidation_MissingDNSLink_ReturnsDNSMismatch(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "missing-dnslink")
		website := createTestIPFSWebsite(testUserID1, "missing-dnslink.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "missing-dnslink.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("missing-dnslink.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{},
		}, nil)
		setMockResolver(ws, mockResolver)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonDNSMismatch, result.Reason)
		assert.Contains(tb, result.Message, "missing or incorrect dnslink record")
	}, TestOptions)
}

func TestValidateDNS_PendingValidation_MissingToken_ReturnsTokenMissing(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "missing-token")
		website := createTestIPFSWebsite(testUserID1, "missing-token.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "missing-token.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("missing-token.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		mockResolver.EXPECT().LookupTXT(mock.Anything, "lumeweb-verify.missing-token.com").Return([]string{"some-other-txt-record=foo"}, nil)
		setMockResolver(ws, mockResolver)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonTokenMissing, result.Reason)
		assert.Contains(tb, result.Message, "missing validation token")
	}, TestOptions)
}

func TestValidateDNS_ActiveSite_ExpiredToken_SkipsTokenCheckAndValidates(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "active-expired-token")
		website := createTestIPFSWebsite(testUserID1, "active-expired.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		_, err = ws.UpdateWebsite(context.Background(), testUserID1, created.ID, map[string]interface{}{
			"status": string(pluginDb.WebsiteStatusActive),
		})
		require.NoError(tb, err)

		pastTime := time.Now().Add(-1 * time.Hour)
		_, err = ws.UpdateWebsite(context.Background(), testUserID1, created.ID, map[string]interface{}{
			"validation_expires_at": &pastTime,
		})
		require.NoError(tb, err)

		activeWebsite, err := ws.GetWebsite(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, activeWebsite.IsExpired())
		assert.Equal(tb, string(pluginDb.WebsiteStatusActive), activeWebsite.Status)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "active-expired.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("active-expired.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: activeWebsite.TargetHash()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonValidated, result.Reason)

		final, err := ws.GetWebsite(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.WebsiteStatusActive), final.Status)
		assert.False(tb, final.IsExpired(), "validation expiry should be refreshed after successful validation of expired active site")
	}, TestOptions)
}

func TestValidateDNS_BrokenSite_ExpiredToken_SkipsTokenCheckAndValidates(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "broken-expired-token")
		website := createTestIPFSWebsite(testUserID1, "broken-expired.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		_, err = ws.UpdateWebsite(context.Background(), testUserID1, created.ID, map[string]interface{}{
			"status": string(pluginDb.WebsiteStatusBroken),
		})
		require.NoError(tb, err)

		pastTime := time.Now().Add(-1 * time.Hour)
		_, err = ws.UpdateWebsite(context.Background(), testUserID1, created.ID, map[string]interface{}{
			"validation_expires_at": &pastTime,
		})
		require.NoError(tb, err)

		brokenWebsite, err := ws.GetWebsite(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, brokenWebsite.IsExpired())
		_ = bindPrimaryDomain(tb, ctx, created.ID, "broken-expired.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("broken-expired.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: brokenWebsite.TargetHash()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonValidated, result.Reason)

		final, err := ws.GetWebsite(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.WebsiteStatusActive), final.Status)
		assert.False(tb, final.IsExpired(), "validation expiry should be refreshed after successful validation of expired broken site")
	}, TestOptions)
}

func TestValidateDNS_PendingValidation_ExpiredToken_ReturnsTokenExpiredWithRegen(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "regen-token")
		website := createTestIPFSWebsite(testUserID1, "regen-token.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)

		pastTime := time.Now().Add(-1 * time.Hour)
		_, err = ws.UpdateWebsite(context.Background(), testUserID1, created.ID, map[string]interface{}{
			"validation_expires_at": &pastTime,
		})
		require.NoError(tb, err)

		expiredWebsite, err := ws.GetWebsite(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, expiredWebsite.IsExpired())
		_ = bindPrimaryDomain(tb, ctx, created.ID, "regen-token.com", false)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonTokenExpired, result.Reason)

		afterWebsite, err := ws.GetWebsite(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(tb, afterWebsite.IsExpired(), "token should be refreshed after regeneration")
	}, TestOptions)
}

func TestValidateDNS_NXDOMAIN_ReturnsDNSMissing(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "nxdomain")
		website := createTestIPFSWebsite(testUserID1, "nxdomain-test.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "nxdomain-test.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("nxdomain-test.com").Return(dnslink.Result{}, dnslink.DNSRCodeError{
			DNSRCode: 3,
			Code:     "RCODE_3",
			Name:     "NXDomain",
			Domain:   "nxdomain-test.com",
		})
		setMockResolver(ws, mockResolver)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonDNSMissing, result.Reason)
		assert.Contains(tb, result.Message, "No DNS records found")
	}, TestOptions)
}

func TestValidateDNS_DNSLinkLookupFailure_ReturnsError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "dns-fail")
		website := createTestIPFSWebsite(testUserID1, "dns-fail-test.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "dns-fail-test.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("dns-fail-test.com").Return(dnslink.Result{}, fmt.Errorf("network error"))
		setMockResolver(ws, mockResolver)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.Error(tb, err)
		assert.False(tb, result.Valid)
		assert.Contains(tb, err.Error(), "DNS lookup failed")
	}, TestOptions)
}

func TestValidateDNS_TxTTLookupFailure_ReturnsError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "txt-lookup-fail")
		website := createTestIPFSWebsite(testUserID1, "txt-fail-test.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "txt-fail-test.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("txt-fail-test.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		mockResolver.EXPECT().LookupTXT(mock.Anything, "lumeweb-verify.txt-fail-test.com").Return(nil, fmt.Errorf("TXT lookup timeout"))
		setMockResolver(ws, mockResolver)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.Error(tb, err)
		assert.False(tb, result.Valid)
		assert.Contains(tb, err.Error(), "DNS TXT lookup failed")
	}, TestOptions)
}

func TestValidateDNS_WrongDNSLink_ReturnsDNSMismatch(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "wrong-dnslink")
		website := createTestIPFSWebsite(testUserID1, "wrong-dnslink.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "wrong-dnslink.com", false)

		wrongCID := util.GenerateTestCID(t, "different content")
		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("wrong-dnslink.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: wrongCID.String()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonDNSMismatch, result.Reason)
		assert.Contains(tb, result.Message, "missing or incorrect dnslink record")
	}, TestOptions)
}

func TestValidateDNS_IPNSTarget_ValidDNSLinkAndToken(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		ipnsName := "k51qzi5uqu5dlts3p5vfpw8kneqp5ye1ttb2jlt8qkt5mq9f2gvgmet6sec29r"
		website := createTestIPNSWebsite(testUserID1, "ipns-validate.com", ipnsName)
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "ipns-validate.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("ipns-validate.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipns": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		mockResolver.EXPECT().LookupTXT(mock.Anything, "lumeweb-verify.ipns-validate.com").Return([]string{
			fmt.Sprintf("lumeweb-verify=%s", created.ValidationToken),
		}, nil)
		setMockResolver(ws, mockResolver)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonValidated, result.Reason)
	}, TestOptions)
}

func TestValidateDNS_PendingValidation_ExpiredToken_ManagedDNS_RegeneratesToken(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockIPNSKey := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, ws)
		require.NotNil(tb, mockDNS)

		testCID := util.GenerateTestCID(t, "managed-dns-regen")
		domain := "managed-dns-regen.com"
		zoneID := uint(5001)

		website := createTestIPFSWebsite(testUserID1, domain, testCID.String())
		website.ID = 5001
		prebindPrimaryDomain(tb, ctx, website, domain, true)

		setupIPNSAutoCreationMocks(t, mockIPNSKey, testUserID1, domain, testCID)
		mockDNS.EXPECT().CreateZone(mock.Anything, domain, testUserID1).Return(createMockDNSZone(zoneID, domain, testUserID1), nil).Once()
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			zoneID,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetTypeIPNS,
			mock.Anything,
		).Return(nil).Once()

		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		require.NotNil(tb, created)
		apex, err := ws.GetApexDomainBinding(context.Background(), created.ID)
		require.NoError(tb, err)
		require.NotNil(tb, apex)
		require.NotNil(tb, apex.DNSZoneID)

		pastTime := time.Now().Add(-1 * time.Hour)
		_, err = ws.UpdateWebsite(context.Background(), testUserID1, created.ID, map[string]interface{}{
			"validation_expires_at": &pastTime,
		})
		require.NoError(tb, err)

		expiredWebsite, err := ws.GetWebsite(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, expiredWebsite.IsExpired())

		var capturedToken string
		mockDNS.EXPECT().CreateWebsiteDNSRecords(
			mock.Anything,
			zoneID,
			mock.Anything,
			mock.Anything,
			pluginDb.WebsiteTargetType(expiredWebsite.TargetType),
			mock.MatchedBy(func(token string) bool {
				capturedToken = token
				return token != ""
			}),
		).Return(nil).Once()

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonTokenExpired, result.Reason)
		assert.Contains(tb, capturedToken, "lumeweb-verify=", "DNS token record should contain the verification key prefix")

		afterWebsite, err := ws.GetWebsite(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(tb, afterWebsite.IsExpired(), "token should be refreshed after regeneration")
	}, TestOptions)
}

func TestValidateDNS_WebsiteNotFound_ReturnsError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, 99999)
		require.Error(tb, err)
		assert.False(tb, result.Valid)
		assert.Contains(tb, err.Error(), "website not found")
	}, TestOptions)
}

func TestValidateDNS_PendingDelegated_SkipsTokenCheck(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "delegated-skip-token")
		website := createTestIPFSWebsite(testUserID1, "delegated-skip.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "delegated-skip.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		// Only DNSLink, no TXT lookup expected because token check skipped
		mockResolver.EXPECT().ResolveDNSLink("delegated-skip.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		// Delegated svc says this domain uses delegation for ownership → skip token
		mockDelegated := &testDelegatedDomainService{
			uses: func(d string) bool { return d == "delegated-skip.com" },
		}
		setMockDelegatedDomainSvc(ws, mockDelegated)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonValidated, result.Reason)
	}, TestOptions)
}

func TestValidateDNS_DelegatedAttached_Success(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "delegated-attached-ok")
		website := createTestIPFSWebsite(testUserID1, "attached-ok.hns", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "attached-ok.hns", false)

		// Insert an attached domain record (simulating delegated domain binding)
		svc := ws.(*WebsiteServiceDefault)
		if db := svc.DB(); db != nil {
			db.Create(&pluginDb.WebsiteDomain{
				WebsiteID:      created.ID,
				UserID:         testUserID1,
				Domain:         "attached-ok.hns",
				Namespace:      pluginDb.DomainNamespaceHNS,
				DelegationData: datatypes.JSONMap{},
			})
		}

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("attached-ok.hns").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		mockDelegated := &testDelegatedDomainService{
			uses: func(d string) bool { return true },
			verify: func(ctx context.Context, wd *pluginDb.WebsiteDomain) (bool, error) {
				return true, nil
			},
		}
		setMockDelegatedDomainSvc(ws, mockDelegated)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid)
	}, TestOptions)
}

func TestValidateDNS_DelegatedAttached_FailsVerification(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "delegated-attached-fail")
		website := createTestIPFSWebsite(testUserID1, "attached-fail.hns", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "attached-fail.hns", false)

		svc := ws.(*WebsiteServiceDefault)
		if db := svc.DB(); db != nil {
			db.Create(&pluginDb.WebsiteDomain{
				WebsiteID:      created.ID,
				UserID:         testUserID1,
				Domain:         "attached-fail.hns",
				Namespace:      pluginDb.DomainNamespaceHNS,
				DelegationData: datatypes.JSONMap{},
			})
		}

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("attached-fail.hns").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		mockDelegated := &testDelegatedDomainService{
			uses: func(d string) bool { return true },
			verify: func(ctx context.Context, wd *pluginDb.WebsiteDomain) (bool, error) {
				return false, nil
			},
		}
		setMockDelegatedDomainSvc(ws, mockDelegated)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonDelegationPending, result.Reason)
		assert.Contains(tb, result.Message, "Domain delegation not yet published")
	}, TestOptions)
}

func TestValidateDNS_DelegatedAttached_VerifyError_Fails(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "delegated-verify-err")
		website := createTestIPFSWebsite(testUserID1, "verify-err.hns", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "verify-err.hns", false)

		svc := ws.(*WebsiteServiceDefault)
		if db := svc.DB(); db != nil {
			db.Create(&pluginDb.WebsiteDomain{
				WebsiteID:      created.ID,
				UserID:         testUserID1,
				Domain:         "verify-err.hns",
				Namespace:      pluginDb.DomainNamespaceHNS,
				DelegationData: datatypes.JSONMap{},
			})
		}

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("verify-err.hns").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		mockDelegated := &testDelegatedDomainService{
			uses: func(d string) bool { return true },
			verify: func(ctx context.Context, wd *pluginDb.WebsiteDomain) (bool, error) {
				return false, fmt.Errorf("delegation verify internal error")
			},
		}
		setMockDelegatedDomainSvc(ws, mockDelegated)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.False(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonDelegationPending, result.Reason)
		assert.Contains(tb, result.Message, "Domain delegation not yet published")
	}, TestOptions)
}
