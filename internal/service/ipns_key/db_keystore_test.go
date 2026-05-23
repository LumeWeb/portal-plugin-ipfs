package ipns_key

import (
	"context"
	"testing"

	"github.com/ipfs/boxo/keystore"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.uber.org/zap/zaptest"
)

var dbKeystoreTestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.IPNS_KEY_SERVICE, NewIPNSKeyService),
	coreTesting.WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService),
	util.GetProtocolMock(),
	coreTesting.WithProtocolConfig("ipfs", &pluginConfig.ProtocolConfig{}),
	coreTesting.WithSQLitePluginMigrations(
		"ipfs", migrations.GetSQLite(),
	),
)

func TestDBKeystore_Has(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		svc := keyService.(*IPNSKeyServiceDefault)
		ks := NewDBKeystore(svc.DB(), svc.decryptPrivateKey, zaptest.NewLogger(t))

		has, err := ks.Has("nonexistent")
		assert.NoError(tb, err)
		assert.False(tb, has)

		key, err := keyService.CreateKey(context.Background(), 1, "test-key", KeyType_Ed25519)
		require.NoError(tb, err)

		has, err = ks.Has(key.PeerID().String())
		assert.NoError(tb, err)
		assert.True(tb, has)
	}, dbKeystoreTestOptions)
}

func TestDBKeystore_Get(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		svc := keyService.(*IPNSKeyServiceDefault)
		ks := NewDBKeystore(svc.DB(), svc.decryptPrivateKey, zaptest.NewLogger(t))

		_, err := ks.Get("nonexistent")
		assert.ErrorIs(tb, err, keystore.ErrNoSuchKey)

		key, err := keyService.CreateKey(context.Background(), 1, "test-key", KeyType_Ed25519)
		require.NoError(tb, err)

		privKey, err := ks.Get(key.PeerID().String())
		assert.NoError(tb, err)
		assert.NotNil(tb, privKey)
	}, dbKeystoreTestOptions)
}

func TestDBKeystore_Put_Errors(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		svc := keyService.(*IPNSKeyServiceDefault)
		ks := NewDBKeystore(svc.DB(), svc.decryptPrivateKey, zaptest.NewLogger(t))

		privKey, _, err := ic.GenerateEd25519Key(nil)
		require.NoError(tb, err)

		err = ks.Put("test", privKey)
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "does not support Put")
	}, dbKeystoreTestOptions)
}

func TestDBKeystore_List(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		svc := keyService.(*IPNSKeyServiceDefault)
		ks := NewDBKeystore(svc.DB(), svc.decryptPrivateKey, zaptest.NewLogger(t))

		names, err := ks.List()
		assert.NoError(tb, err)
		assert.Empty(tb, names)

		key1, err := keyService.CreateKey(context.Background(), 1, "list-key1", KeyType_Ed25519)
		require.NoError(tb, err)
		key2, err := keyService.CreateKey(context.Background(), 1, "list-key2", KeyType_Ed25519)
		require.NoError(tb, err)

		names, err = ks.List()
		assert.NoError(tb, err)
		assert.Len(tb, names, 2)
		assert.Contains(tb, names, key1.PeerID().String())
		assert.Contains(tb, names, key2.PeerID().String())
	}, dbKeystoreTestOptions)
}

func TestDBKeystore_Delete(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		svc := keyService.(*IPNSKeyServiceDefault)
		ks := NewDBKeystore(svc.DB(), svc.decryptPrivateKey, zaptest.NewLogger(t))

		key, err := keyService.CreateKey(context.Background(), 1, "delete-key", KeyType_Ed25519)
		require.NoError(tb, err)

		has, err := ks.Has(key.PeerID().String())
		assert.NoError(tb, err)
		assert.True(tb, has)

		err = ks.Delete(key.PeerID().String())
		assert.NoError(tb, err)

		has, err = ks.Has(key.PeerID().String())
		assert.NoError(tb, err)
		assert.False(tb, has)
	}, dbKeystoreTestOptions)
}

func TestDBKeystore_ListKeysWithCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		svc := keyService.(*IPNSKeyServiceDefault)
		ks := NewDBKeystore(svc.DB(), svc.decryptPrivateKey, zaptest.NewLogger(t))

		keys, err := ks.ListKeysWithCID(context.Background())
		assert.NoError(tb, err)
		assert.Empty(tb, keys)

		key, err := keyService.CreateKey(context.Background(), 1, "cid-key", KeyType_Ed25519)
		require.NoError(tb, err)
		_ = key

		keys, err = ks.ListKeysWithCID(context.Background())
		assert.NoError(tb, err)
		assert.Empty(tb, keys, "Keys without LastPublishedCID should not be returned")
	}, dbKeystoreTestOptions)
}
