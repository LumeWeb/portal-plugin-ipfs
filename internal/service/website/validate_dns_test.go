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
		require.NotZero(tb, apex.ZoneID)

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

// TestValidateDNS_SelfHostedPrimary_DoesNotRequireDelegation verifies that a
// website whose primary binding is self-hosted (ZoneID == 0, no portal-managed
// zone) validates once its hosting DNS is correct. VerifyDomain no-ops with
// false for self-hosted bindings (there is no portal delegation to wait on),
// so the delegation gate must pass for them (their DNSLink + token were already
// validated by the earlier ValidateDNS steps), rather than leaving the site at
// "Domain delegation not yet published" forever.
func TestValidateDNS_SelfHostedPrimary_DoesNotRequireDelegation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "self-hosted-primary")
		website := createTestIPFSWebsite(testUserID1, "selfhosted.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		// bindPrimaryDomain with DNSHostingEnabled=false yields a zone-less
		// (ZoneID==0) self-hosted primary binding.
		_ = bindPrimaryDomain(tb, ctx, created.ID, "selfhosted.com", false)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("selfhosted.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		mockResolver.EXPECT().LookupTXT(mock.Anything, "lumeweb-verify.selfhosted.com").Return([]string{
			fmt.Sprintf("lumeweb-verify=%s", created.ValidationToken),
		}, nil)
		setMockResolver(ws, mockResolver)

		mockDelegated := &testDelegatedDomainService{
			uses: func(d string) bool { return false },
			// Mirror the real VerifyDomain: a self-hosted (zone-less) binding
			// returns (false, nil). checkDelegation must not fail on this.
			verify: func(ctx context.Context, wd *pluginDb.WebsiteDomain) (bool, error) {
				if wd.ZoneID == 0 {
					return false, nil
				}
				return true, nil
			},
		}
		setMockDelegatedDomainSvc(ws, mockDelegated)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid, "self-hosted primary must not be blocked at delegation pending")
		assert.Equal(tb, pluginCore.ValidationReasonValidated, result.Reason)
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

func TestValidateDNS_PlatformSubdomain_SkipsTokenCheck(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "platform-skip-token")
		website := createTestIPFSWebsite(testUserID1, "platform-skip.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		wd := bindPrimaryDomain(tb, ctx, created.ID, "platform-skip.com", true)

		// Mark the binding as a platform subdomain (ICANN namespace): the
		// platform controls both ends of the DNS check, so no user TXT token
		// exists and the token check must be skipped purely on this pointer.
		pdID := uint(7)
		wd.PlatformDomainID = &pdID
		require.NoError(tb, ctx.DB().Model(wd).Update("platform_domain_id", pdID).Error)

		mockResolver := mocks.NewMockDNSResolver(t)
		// Only DNSLink expected: the token check (TXT lookup) must NOT run.
		mockResolver.EXPECT().ResolveDNSLink("platform-skip.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		// UsesDelegationForOwnership returns false (ICANN), proving the skip is
		// triggered by the platform-subdomain path, not the delegation path.
		mockDelegated := &testDelegatedDomainService{
			uses: func(d string) bool { return false },
		}
		setMockDelegatedDomainSvc(ws, mockDelegated)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid)
		assert.Equal(tb, pluginCore.ValidationReasonValidated, result.Reason)

		final, err := ws.GetWebsite(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.Equal(tb, string(pluginDb.WebsiteStatusActive), final.Status)
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

// TestValidateDNS_SecondaryPending_DoesNotBlockPrimary validates the primary-only
// delegation gate: a website remains valid when its primary domain delegation
// verifies, even if a secondary attached domain's delegation is still pending.
// Secondaries own their own DNS and validate independently; they must not hold
// the whole site at "Domain delegation not yet published".
func TestValidateDNS_SecondaryPending_DoesNotBlockPrimary(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "secondary-pending")
		website := createTestIPFSWebsite(testUserID1, "primary.com", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		_ = bindPrimaryDomain(tb, ctx, created.ID, "primary.com", false)

		// Insert a secondary HNS binding whose delegation is pending (verifies
		// false). This must NOT fail the website's DNS validation.
		svc := ws.(*WebsiteServiceDefault)
		require.NotNil(tb, svc.DB())
		require.NoError(tb, svc.DB().Create(&pluginDb.WebsiteDomain{
			WebsiteID:      created.ID,
			UserID:         testUserID1,
			Domain:         "secondary.hns",
			Namespace:      pluginDb.DomainNamespaceHNS,
			DelegationData: datatypes.JSONMap{},
		}).Error)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("primary.com").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		setMockResolver(ws, mockResolver)

		mockDelegated := &testDelegatedDomainService{
			// Ownership proven via delegation for both bindings, so the token
			// lookup is skipped and only the delegation gate is exercised.
			uses: func(d string) bool { return true },
			// The primary verifies; the secondary would fail if ever checked.
			verify: func(ctx context.Context, wd *pluginDb.WebsiteDomain) (bool, error) {
				if wd.Domain == "secondary.hns" {
					return false, nil
				}
				return true, nil
			},
		}
		setMockDelegatedDomainSvc(ws, mockDelegated)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid, "secondary pending delegation must not block primary validation")
		assert.Equal(tb, pluginCore.ValidationReasonValidated, result.Reason)
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
		// Give the primary a portal-managed zone so checkDelegation actually
		// invokes VerifyDomain (a zone-less/self-hosted primary does not
		// require delegation and would pass the gate).
		require.NoError(tb, ctx.DB().Model(&pluginDb.WebsiteDomain{}).
			Where("website_id = ? AND domain = ?", created.ID, "attached-fail.hns").
			Update("zone_id", uint(42)).Error)

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

// TestValidateDNS_SelfHostedHNSPrimary_DoesNotDeadEnd guards against trapping a
// zone-less (self-hosted) HNS primary in an unreachable delegation-pending
// state. A self-hosted binding (ZoneID == 0) has no portal-managed PowerDNS
// zone, so VerifyDomain structurally cannot succeed for it (it no-ops with
// false). The delegation gate must pass for every zone-less primary once its
// hosting DNS validated, regardless of namespace: for HNS the DNSLink resolves
// from the owner-controlled HNS zone, which is itself the ownership proof.
func TestValidateDNS_SelfHostedHNSPrimary_DoesNotDeadEnd(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		ws := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, ws)

		testCID := util.GenerateTestCID(t, "self-hosted-hns-primary")
		website := createTestIPFSWebsite(testUserID1, "selfhosted.hns", testCID.String())
		created, err := ws.CreateWebsite(context.Background(), website)
		require.NoError(tb, err)
		// bindPrimaryDomain(false) yields a zone-less (ZoneID==0) primary; the
		// namespace is HNS, so it is a self-hosted binding in a delegated
		// namespace.
		_ = bindPrimaryDomain(tb, ctx, created.ID, "selfhosted.hns", false)
		require.NoError(tb, ctx.DB().Model(&pluginDb.WebsiteDomain{}).
			Where("website_id = ? AND domain = ?", created.ID, "selfhosted.hns").
			Update("namespace", string(pluginDb.DomainNamespaceHNS)).Error)

		mockResolver := mocks.NewMockDNSResolver(t)
		mockResolver.EXPECT().ResolveDNSLink("selfhosted.hns").Return(dnslink.Result{
			Links: map[string]dnslink.NamespaceEntries{
				"ipfs": {{Identifier: created.TargetHash()}},
			},
		}, nil)
		// No LookupTXT expectation: the token check is skipped for the
		// delegated (HNS) namespace; the DNSLink from the owner's HNS zone is
		// the ownership proof.
		setMockResolver(ws, mockResolver)

		mockDelegated := &testDelegatedDomainService{
			// HNS uses delegation for ownership, so the token check is skipped.
			uses: func(d string) bool { return d == "selfhosted.hns" },
			// Mirror the real VerifyDomain on a zone-less binding: it returns
			// (false, nil). checkDelegation must NOT route zone-less bindings
			// through it (that is an unreachable dead-end), so it must pass.
			verify: func(ctx context.Context, wd *pluginDb.WebsiteDomain) (bool, error) {
				if wd.ZoneID == 0 {
					return false, nil
				}
				return true, nil
			},
		}
		setMockDelegatedDomainSvc(ws, mockDelegated)

		result, err := ws.ValidateDNS(context.Background(), testUserID1, created.ID)
		require.NoError(tb, err)
		assert.True(tb, result.Valid, "self-hosted HNS primary must not dead-end at delegation pending")
		assert.Equal(tb, pluginCore.ValidationReasonValidated, result.Reason)
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
		// Give the primary a portal-managed zone so checkDelegation actually
		// invokes VerifyDomain (a zone-less/self-hosted primary does not
		// require delegation and would pass the gate).
		require.NoError(tb, ctx.DB().Model(&pluginDb.WebsiteDomain{}).
			Where("website_id = ? AND domain = ?", created.ID, "verify-err.hns").
			Update("zone_id", uint(42)).Error)

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
