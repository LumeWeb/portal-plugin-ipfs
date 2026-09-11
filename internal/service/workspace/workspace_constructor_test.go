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
	// any Coolify client/network is touched (ctx stays nil).
	svc := &WorkspaceService{config: &pluginConfig.WorkspaceConfig{
		Enabled: true,
		Provider: pluginConfig.WorkspaceProviderConfig{
			APIURL: "https://coolify.example.com",
		},
	}}
	err := svc.startupValidate(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "api_token")
	assert.Nil(t, svc.Provider())
}
