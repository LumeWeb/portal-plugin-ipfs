package util

import (
	"go.lumeweb.com/portal-plugin-ipfs/internal/plugin"
	coreTesting "go.lumeweb.com/portal/core/testing"
	serviceTesting "go.lumeweb.com/portal/service/testing"
)

func GetStandardTestOptions() []coreTesting.TestContextBuilderOption {
	return []coreTesting.TestContextBuilderOption{
		serviceTesting.PresetE2E(),
		coreTesting.WithConfig("core.mail.host", "localhost"),
		coreTesting.WithConfig("core.mail.port", 25),
		coreTesting.WithPlugins(plugin.GetPluginInfoWithTemplates(nil)),
	}
}
