package api

import (
	"crypto/rand"
	"encoding/hex"
	"io/fs"
	"os"
	"testing"

	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	domsvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/testopts"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

var _ = func() struct{} {
	if os.Getenv("GATEWAY_SECRET") == "" {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			panic("failed to generate gateway secret: " + err.Error())
		}
		os.Setenv("GATEWAY_SECRET", hex.EncodeToString(b))
	}
	// Wire the real DelegatedDomainService factory into testopts to avoid
	// an import cycle (testopts can't import domain because domain tests
	// import testopts).
	testopts.DelegatedDomainServiceFactory = domsvc.NewDelegatedDomainServiceFactory
	return struct{}{}
}()

func TestMain(m *testing.M) {
	coreTesting.WithOptions(m,
		coreTesting.WithAPI(internal.ProtocolName, NewAPI),
		coreTesting.WithAPIID(internal.ProtocolName),
		util.GetProtocolMock(),
		coreTesting.WithProtocolConfig(internal.ProtocolName, pluginConfig.ProtocolConfig{}),
	)
}

var testPluginOptions = coreTesting.CombineOptions(
	testopts.NewMockPluginBuilder().
		WithMigrations(map[core.DBType]fs.FS{
			core.DB_TYPE_SQLITE: migrations.GetSQLite(),
		}).BuilderOption(),
)

var TestOptions = coreTesting.CombineOptions(
	testPluginOptions,
	coreTesting.WithHTTPService(),
	coreTesting.WithPlugins(),
	coreTesting.WithAPIConfig(internal.ProtocolName, &pluginConfig.APIConfig{
		GatewaySecret: os.Getenv("GATEWAY_SECRET"),
	}),
)
