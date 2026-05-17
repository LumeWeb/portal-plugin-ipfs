package api

import (
	"testing"

	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/testopts"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestMain(m *testing.M) {
	coreTesting.WithOptions(m,
		coreTesting.WithAPI(internal.ProtocolName, NewAPI),
		coreTesting.WithAPIID(internal.ProtocolName),
		util.GetProtocolMock(),
		coreTesting.WithProtocolConfig(internal.ProtocolName, pluginConfig.ProtocolConfig{}),
	)
}

var testPluginOptions = coreTesting.CombineOptions(
	testopts.NewMockPluginBuilder().BuilderOption(),
)

var TestOptions = coreTesting.CombineOptions(
	testPluginOptions,
	coreTesting.WithHTTPService(),
	coreTesting.WithPlugins(),
	coreTesting.WithAPIConfig(internal.ProtocolName, &pluginConfig.APIConfig{
		GatewaySecret: "test-secret",
	}),
)
