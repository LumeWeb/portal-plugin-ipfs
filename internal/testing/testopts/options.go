package testopts

import (
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func NewMockPluginBuilder() *coreTesting.MockPluginBuilder {
	return NewBaseMockPluginBuilder().
		WithMockServiceFactory(pluginCore.WEBSITE_SERVICE, mocks.NewMockWebsiteService).
		WithMockServiceFactory(pluginCore.DNS_SERVICE, mocks.NewMockDNSService).
		WithServiceConfig(pluginCore.WEBSITE_SERVICE, &config.WebsiteConfig{}).
		WithServiceConfig(pluginCore.DNS_SERVICE, &config.DnsConfig{})
}

func NewBaseMockPluginBuilder() *coreTesting.MockPluginBuilder {
	return coreTesting.NewMockPluginBuilder(internal.ProtocolName).
		WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService).
		WithMockServiceFactory(pluginCore.PIN_SERVICE, mocks.NewMockIPFSPinService).
		WithMockServiceFactory(pluginCore.BLOCK_SERVICE, mocks.NewMockBlockService).
		WithMockServiceFactory(pluginCore.UPLOAD_SERVICE, mocks.NewMockUploadService).
		WithMockServiceFactory(pluginCore.IPNS_KEY_SERVICE, mocks.NewMockIPNSKeyService)
}
