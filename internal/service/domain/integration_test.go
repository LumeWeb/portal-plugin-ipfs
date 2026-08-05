package domain

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	dane "go.lumeweb.com/dane"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

func TestIntegration_CreateAndVerifyHNSDomain(t *testing.T) {
	certPEM, _, certErr := dane.GenerateSelfSignedECDSA([]string{"test.example"}, time.Now().AddDate(1, 0, 0))
	if certErr != nil {
		t.Skipf("dane.GenerateSelfSignedECDSA not usable: %v", certErr)
	}

	// Gateway IP is injected via env so the fixture does not hardcode a
	// production address; defaults to a loopback-safe value for CI.
	gatewayIP := os.Getenv("TEST_GATEWAY_IP")
	if gatewayIP == "" {
		gatewayIP = "127.0.0.1"
	}

	intOpts := coreTesting.CombineOptions(
		TestOptions,
		coreTesting.WithConfig("plugin.ipfs.service.dns.gateway_domain", "ipfs.pub"),
		coreTesting.WithConfig("plugin.ipfs.service.dns.gateway_ip", gatewayIP),
	)

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()

		// Create a website
		website := &pluginDb.Website{
			UserID:          1,
			Domain:          "example",
			TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
			TargetMultihash: []byte("test-hash"),
			Status:          string(pluginDb.WebsiteStatusPendingValidation),
			ValidationToken: os.Getenv("TEST_VALIDATION_TOKEN"),
			CIDVersion:      func() *uint8 { v := uint8(1); return &v }(),
			CIDType:         func() *uint8 { v := uint8(0x55); return &v }(),
		}
		require.NoError(tb, db.Create(website).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, mockDNS)

		// Seed the cert into the HNS provider's TLSASource before CreateDomain
		// Direct call to OnCertAvailable because the domain doesn't exist in the DB yet.
		// UpdateTLSAFromCert requires a persisted WebsiteDomain row.
		nsProvider := svc.registry.Get("hns")
		require.NotNil(tb, nsProvider, "HNS provider should be registered")
		require.NoError(tb, nsProvider.OnCertAvailable(context.Background(), "example", certPEM))

		mockDNS.EXPECT().CreateZone(mock.Anything, "example", uint(1)).
			Return(&pluginDb.DNSZone{Model: gorm.Model{ID: 1}, Domain: "example"}, nil).Once()
		mockDNS.EXPECT().CreateDNSLinkRecord(mock.Anything, uint(1), mock.Anything).Return(nil).Once()
		mockDNS.EXPECT().CreateApexRecord(mock.Anything, uint(1), pluginCore.RecordTypeA, gatewayIP).Return(nil).Once()
		mockDNS.EXPECT().EnableDNSSEC(mock.Anything, uint(1)).Return("257 3 13 dGVzdA==", nil).Maybe()

		// Create domain
		wd, err := svc.CreateDomain(context.Background(), "hns", "example", website.ID, 1, nil)
		require.NoError(tb, err)
		require.NotNil(tb, wd)
		assert.Equal(tb, pluginDb.DomainStatusRecordsGenerated, wd.Status)
	}, intOpts)
}
