package tests

import (
	"testing"

	"go.lumeweb.com/portal/config"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestDebugConfigDir(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		tb.Helper()
		
		// Debug: print the config directory
		cfg := ctx.Config()
		configDir := cfg.ConfigDir()
		
		t.Logf("ConfigDir: %s", configDir)
		t.Logf("Config type: %T", cfg)
		
		// Check if it's a mock or real config
		if mockCfg, ok := cfg.(*config.MockManager); ok {
			t.Logf("Mock config detected")
			t.Logf("Mock ConfigDir: %s", mockCfg.ConfigDir())
		} else if realCfg, ok := cfg.(*config.ManagerDefault); ok {
			t.Logf("Real config detected")
			t.Logf("Real ConfigDir: %s", realCfg.ConfigDir())
			// Try to get the config file
			t.Logf("ConfigFile: %s", realCfg.ConfigFile())
		} else {
			t.Logf("Unknown config type: %T", cfg)
		}
	})
}
