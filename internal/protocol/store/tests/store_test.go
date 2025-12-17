package tests

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

var (
	cfg            = coreTesting.NewConfigBuilder().Build()
	ipfsTestConfig = coreTesting.CombineOptions(

		coreTesting.WithSQLitePluginMigrations(internal.ProtocolName, migrations.GetSQLite()),
		util.GetProtocolMock(),
		coreTesting.WithProtocolConfig(internal.ProtocolName, cfg),
	)
)

func generateCid(t *testing.T, data string) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}
