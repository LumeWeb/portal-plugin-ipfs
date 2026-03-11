package api

import (
	"testing"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestMain(m *testing.M) {
	coreTesting.WithOptions(m,
		coreTesting.WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService),
		coreTesting.WithMockServiceFactory(pluginCore.PIN_SERVICE, mocks.NewMockIPFSPinService),
		coreTesting.WithMockServiceFactory(pluginCore.BLOCK_SERVICE, mocks.NewMockBlockService),
		coreTesting.WithMockServiceFactory(pluginCore.UPLOAD_SERVICE, mocks.NewMockUploadService),
		coreTesting.WithMockServiceFactory(pluginCore.WEBSITE_SERVICE, mocks.NewMockWebsiteService),
		coreTesting.WithAPI(internal.ProtocolName, NewAPI),
		coreTesting.WithAPIID(internal.ProtocolName),

		util.GetProtocolMock(),
		coreTesting.WithProtocolConfig(internal.ProtocolName, pluginConfig.ProtocolConfig{}),
	)
}

// TestOptions provides test configuration for API tests
var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService),
	coreTesting.WithMockServiceFactory(pluginCore.PIN_SERVICE, mocks.NewMockIPFSPinService),
	coreTesting.WithMockServiceFactory(pluginCore.BLOCK_SERVICE, mocks.NewMockBlockService),
	coreTesting.WithMockServiceFactory(pluginCore.UPLOAD_SERVICE, mocks.NewMockUploadService),
	coreTesting.WithMockServiceFactory(pluginCore.IPNS_KEY_SERVICE, mocks.NewMockIPNSKeyService),
	coreTesting.WithMockServiceFactory(pluginCore.WEBSITE_SERVICE, mocks.NewMockWebsiteService, &pluginConfig.WebsiteConfig{}),
	coreTesting.WithMockServiceFactory(pluginCore.DNS_SERVICE, mocks.NewMockDNSService, &pluginConfig.DnsConfig{}),
	coreTesting.WithHTTPService(),
	coreTesting.WithPlugins(),
	coreTesting.WithAPIConfig(internal.ProtocolName, &pluginConfig.APIConfig{
		GatewaySecret: "test-secret",
	}),
)
