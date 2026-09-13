package workspace

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
)

func TestNewWorkspaceService_RegistersConfig(t *testing.T) {
	svc, _, err := NewWorkspaceService()
	require.NoError(t, err)
	assert.Equal(t, pluginCore.WORKSPACE_SERVICE, svc.ID())

	cfg, err := svc.(*WorkspaceService).GetConfig()
	require.NoError(t, err)
	_, ok := cfg.(*pluginConfig.WorkspaceConfig)
	require.True(t, ok, "GetConfig must return the workspace service config")
}

func TestWorkspaceService_DisabledConfigPassesStartupValidation(t *testing.T) {
	svc := &WorkspaceService{config: &pluginConfig.WorkspaceConfig{}}
	// Disabled config: validation passes trivially and no Coolify client is
	// built, so no context/network is required.
	require.NoError(t, svc.startupValidate(nil))
	assert.Nil(t, svc.Provider())
}

func TestWorkspaceService_InvalidEnabledConfigFailsBeforeNetwork(t *testing.T) {
	// An enabled but incomplete config must fail structural validation before
	// any Coolify client/network is touched (ctx stays nil). The workspace
	// service's own fields are valid so this exercises the independently-owned
	// Coolify child validator (not the parent WorkspaceConfig, which owns only
	// its own fields).
	svc := &WorkspaceService{config: &pluginConfig.WorkspaceConfig{
		Enabled:            true,
		ReconcileBatchSize: 50,
		RetryMaxAttempts:   3,
		Coolify: pluginConfig.WorkspaceCoolifyConfig{
			APIURL: "https://coolify.example.com",
		},
	}}
	err := svc.startupValidate(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "api_token")
	assert.Nil(t, svc.Provider())
}
