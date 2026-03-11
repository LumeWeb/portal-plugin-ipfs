package ipns_key

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/ipfs/boxo/keystore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.uber.org/zap/zaptest"
)

var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.IPNS_KEY_SERVICE, NewIPNSKeyService),
	coreTesting.WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService),
	util.GetProtocolMock(),
	coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
)

func TestIPNSKeyService_CreateKey(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		keyName := "test-key"

		// Act
		createdKey, err := keyService.CreateKey(context.Background(), userID, keyName, KeyType_Ed25519)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdKey)
		assert.Equal(tb, userID, createdKey.UserID)
		assert.Equal(tb, keyName, createdKey.Name)
		assert.NotEmpty(tb, createdKey.PeerID().String())
		assert.NotEmpty(tb, createdKey.PrivateKeyEncrypted)
		assert.NotZero(tb, createdKey.ID)

		// Verify peer ID is valid
		peerID, err := peer.Decode(createdKey.PeerID().String())
		require.NoError(tb, err)
		assert.NotEmpty(tb, peerID)

		// Verify key can be retrieved
		retrievedKey, err := keyService.GetKeyByID(context.Background(), userID, createdKey.ID)
		require.NoError(tb, err)
		assert.Equal(tb, createdKey.ID, retrievedKey.ID)
		assert.Equal(tb, createdKey.PeerID(), retrievedKey.PeerID())
	}, TestOptions)
}

func TestIPNSKeyService_CreateKey_RSA(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		keyName := "test-rsa-key"

		// Act
		createdKey, err := keyService.CreateKey(context.Background(), userID, keyName, KeyType_RSA)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdKey)
		assert.Equal(tb, userID, createdKey.UserID)
		assert.Equal(tb, keyName, createdKey.Name)
		assert.NotEmpty(tb, createdKey.PeerID().String())
		assert.NotEmpty(tb, createdKey.PrivateKeyEncrypted)
	}, TestOptions)
}

func TestIPNSKeyService_CreateKey_DefaultKeyType(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		keyName := "test-default-key"

		// Act - passing 0 should default to Ed25519
		createdKey, err := keyService.CreateKey(context.Background(), userID, keyName, 0)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, createdKey)
		assert.Equal(tb, userID, createdKey.UserID)
		assert.Equal(tb, keyName, createdKey.Name)
		assert.NotEmpty(tb, createdKey.PeerID().String())
	}, TestOptions)
}

func TestIPNSKeyService_ImportKey(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		keyName := "imported-key"

		// First, create a key to get a valid private key
		originalKey, err := keyService.CreateKey(context.Background(), userID, "original-key", KeyType_Ed25519)
		require.NoError(tb, err)

		// Export the private key
		exportedKey, err := keyService.ExportKey(context.Background(), userID, originalKey.ID)
		require.NoError(tb, err)
		assert.NotEmpty(tb, exportedKey)

		// Act - Import the key with a different name (should fail - same user, same peer ID)
		importedKey, err := keyService.ImportKey(context.Background(), userID, keyName, exportedKey)

		// Assert - Should fail because same user cannot have duplicate peer ID
		assert.Error(tb, err)
		assert.Nil(tb, importedKey)
		assert.Contains(tb, err.Error(), "already exists")
	}, TestOptions)
}

func TestIPNSKeyService_ImportKey_InvalidBase64(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		keyName := "invalid-key"
		invalidBase64 := "this is not valid base64!!!"

		// Act
		importedKey, err := keyService.ImportKey(context.Background(), userID, keyName, invalidBase64)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, importedKey)
		assert.Contains(tb, err.Error(), "failed to decode base64")
	}, TestOptions)
}

func TestIPNSKeyService_ImportKey_InvalidKeyFormat(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		keyName := "invalid-format-key"
		validBase64ButInvalidKey := "SGVsbG8gV29ybGQ=" // "Hello World" in base64, not a valid key

		// Act
		importedKey, err := keyService.ImportKey(context.Background(), userID, keyName, validBase64ButInvalidKey)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, importedKey)
		assert.Contains(tb, err.Error(), "failed to unmarshal private key")
	}, TestOptions)
}

func TestIPNSKeyService_ExportKey(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)

		// Create a key
		createdKey, err := keyService.CreateKey(context.Background(), userID, "test-export-key", KeyType_Ed25519)
		require.NoError(tb, err)

		// Act
		exportedKey, err := keyService.ExportKey(context.Background(), userID, createdKey.ID)

		// Assert
		require.NoError(tb, err)
		assert.NotEmpty(tb, exportedKey)

		// Verify the exported key can be decoded and unmarshaled
		// Decode base64
		keyBytes, err := base64.StdEncoding.DecodeString(exportedKey)
		require.NoError(tb, err)
		assert.NotEmpty(tb, keyBytes)

		// Verify the exported key can be unmarshaled
		privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
		require.NoError(tb, err)
		assert.NotNil(tb, privKey)
	}, TestOptions)
}

func TestIPNSKeyService_ExportKey_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		nonExistentKeyID := uint(99999)

		// Act
		exportedKey, err := keyService.ExportKey(context.Background(), userID, nonExistentKeyID)

		// Assert
		assert.Error(tb, err)
		assert.Empty(tb, exportedKey)
	}, TestOptions)
}

func TestIPNSKeyService_GetKeyByID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)

		// Create a key
		createdKey, err := keyService.CreateKey(context.Background(), userID, "test-get-key", KeyType_Ed25519)
		require.NoError(tb, err)

		// Act
		retrievedKey, err := keyService.GetKeyByID(context.Background(), userID, createdKey.ID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedKey)
		assert.Equal(tb, createdKey.ID, retrievedKey.ID)
		assert.Equal(tb, createdKey.Name, retrievedKey.Name)
		assert.Equal(tb, createdKey.PeerID(), retrievedKey.PeerID())
		assert.Equal(tb, createdKey.UserID, retrievedKey.UserID)
	}, TestOptions)
}

func TestIPNSKeyService_GetKeyByID_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		nonExistentKeyID := uint(99999)

		// Act
		retrievedKey, err := keyService.GetKeyByID(context.Background(), userID, nonExistentKeyID)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, retrievedKey)
	}, TestOptions)
}

func TestIPNSKeyService_ListKeys(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		userID2 := uint(2)

		// Create keys for user 1
		key1, err := keyService.CreateKey(context.Background(), userID, "user1-key1", KeyType_Ed25519)
		require.NoError(tb, err)
		key2, err := keyService.CreateKey(context.Background(), userID, "user1-key2", KeyType_Ed25519)
		require.NoError(tb, err)

		// Create a key for user 2
		key3, err := keyService.CreateKey(context.Background(), userID2, "user2-key1", KeyType_Ed25519)
		require.NoError(tb, err)

		// Act - List keys for user 1
		keys, err := keyService.ListKeys(context.Background(), userID)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, keys, 2)

		// Verify the keys belong to user 1
		keyIDs := make(map[uint]bool)
		for _, key := range keys {
			assert.Equal(tb, userID, key.UserID)
			keyIDs[key.ID] = true
		}
		assert.True(tb, keyIDs[key1.ID])
		assert.True(tb, keyIDs[key2.ID])
		assert.False(tb, keyIDs[key3.ID])
	}, TestOptions)
}

func TestIPNSKeyService_ListKeys_Empty(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(999)

		// Act
		keys, err := keyService.ListKeys(context.Background(), userID)

		// Assert
		require.NoError(tb, err)
		assert.Empty(tb, keys)
	}, TestOptions)
}

func TestIPNSKeyService_DeleteKey(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)

		// Create a key
		createdKey, err := keyService.CreateKey(context.Background(), userID, "test-delete-key", KeyType_Ed25519)
		require.NoError(tb, err)

		// Verify key exists
		retrievedKey, err := keyService.GetKeyByID(context.Background(), userID, createdKey.ID)
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedKey)

		// Act - Delete the key
		err = keyService.DeleteKey(context.Background(), userID, createdKey.ID)

		// Assert
		require.NoError(tb, err)

		// Verify key is soft-deleted (should not be found)
		_, err = keyService.GetKeyByID(context.Background(), userID, createdKey.ID)
		assert.Error(tb, err)
	}, TestOptions)
}

func TestIPNSKeyService_DeleteKey_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		nonExistentKeyID := uint(99999)

		// Act
		err := keyService.DeleteKey(context.Background(), userID, nonExistentKeyID)

		// Assert
		assert.Error(tb, err)
	}, TestOptions)
}

func TestIPNSKeyService_GetPrivateKey(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)

		// Create a key
		createdKey, err := keyService.CreateKey(context.Background(), userID, "test-private-key", KeyType_Ed25519)
		require.NoError(tb, err)

		// Act
		privKey, err := keyService.GetPrivateKey(context.Background(), userID, createdKey.ID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, privKey)

		// Verify the private key is valid and can derive the same peer ID
		pubKey := privKey.GetPublic()
		derivedPeerID, err := peer.IDFromPublicKey(pubKey)
		require.NoError(tb, err)
		assert.Equal(tb, createdKey.PeerID().String(), derivedPeerID.String())
	}, TestOptions)
}

func TestIPNSKeyService_GetPrivateKey_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)
		nonExistentKeyID := uint(99999)

		// Act
		privKey, err := keyService.GetPrivateKey(context.Background(), userID, nonExistentKeyID)

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, privKey)
	}, TestOptions)
}

func TestIPNSKeyService_GetPrivateKeyByPeerID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)

		// Create a key
		createdKey, err := keyService.CreateKey(context.Background(), userID, "test-peerid-key", KeyType_Ed25519)
		require.NoError(tb, err)

		// Act
		privKey, retrievedUserID, err := keyService.GetPrivateKeyByPeerID(context.Background(), createdKey.PeerID().String())

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, privKey)
		assert.Equal(tb, userID, retrievedUserID)

		// Verify the private key is valid
		pubKey := privKey.GetPublic()
		derivedPeerID, err := peer.IDFromPublicKey(pubKey)
		require.NoError(tb, err)
		assert.Equal(tb, createdKey.PeerID().String(), derivedPeerID.String())
	}, TestOptions)
}

func TestIPNSKeyService_GetPrivateKeyByPeerID_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		nonExistentPeerID := peer.ID("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG")

		// Act
		privKey, userID, err := keyService.GetPrivateKeyByPeerID(context.Background(), nonExistentPeerID.String())

		// Assert
		assert.Error(tb, err)
		assert.Nil(tb, privKey)
		assert.Zero(tb, userID)
	}, TestOptions)
}

func TestIPNSKeyService_SyncToBoxoKeystore(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)

		// Create multiple keys
		_, err := keyService.CreateKey(context.Background(), userID, "sync-key1", KeyType_Ed25519)
		require.NoError(tb, err)
		_, err = keyService.CreateKey(context.Background(), userID, "sync-key2", KeyType_Ed25519)
		require.NoError(tb, err)

		// Act - Sync keys to boxo keystore
		err = keyService.SyncToBoxoKeystore(context.Background())

		// Assert
		// The sync operation may fail if protocol doesn't implement IPNSBoxoServices
		// This is expected in test environment
		if err != nil {
			assert.Contains(tb, err.Error(), "IPFS protocol does not implement IPNSBoxoServices")
		}
	}, TestOptions)
}

func TestIPNSKeyService_UniquePeerIDPerUser(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID := uint(1)

		// Create a key
		createdKey, err := keyService.CreateKey(context.Background(), userID, "original-key", KeyType_Ed25519)
		require.NoError(tb, err)

		// Export the key
		exportedKey, err := keyService.ExportKey(context.Background(), userID, createdKey.ID)
		require.NoError(tb, err)

		// Act - Try to import the same key for the same user
		_, err = keyService.ImportKey(context.Background(), userID, "duplicate-key", exportedKey)

		// Assert - Should fail due to unique peer ID constraint
		assert.Error(tb, err)
		assert.Contains(tb, err.Error(), "already exists")
	}, TestOptions)
}

func TestIPNSKeyService_UniquePeerID_DifferentUsers(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		keyService := core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, keyService)

		userID1 := uint(1)
		userID2 := uint(2)

		// Create a key for user 1
		createdKey, err := keyService.CreateKey(context.Background(), userID1, "user1-key", KeyType_Ed25519)
		require.NoError(tb, err)

		// Export the key
		exportedKey, err := keyService.ExportKey(context.Background(), userID1, createdKey.ID)
		require.NoError(tb, err)

		// Act - Try to import the same key for a different user
		// The unique constraint is on (user_id, peer_id), so this should succeed
		// Different users can import the same peer ID with different names
		importedKey, err := keyService.ImportKey(context.Background(), userID2, "user2-imported-key", exportedKey)

		// Assert - Should succeed (different users can share the same IPNS key)
		require.NoError(tb, err)
		assert.NotNil(tb, importedKey)
		assert.Equal(tb, userID2, importedKey.UserID)
		assert.Equal(tb, "user2-imported-key", importedKey.Name)
		assert.Equal(tb, createdKey.PeerID(), importedKey.PeerID())
	}, TestOptions)
}

// SafeRepublisherKeystore tests

func TestSafeRepublisherKeystore_RejectsNilKeys(t *testing.T) {
	inner := keystore.NewMemKeystore()
	logger := &core.Logger{Logger: zaptest.NewLogger(t)}
	safeKS := NewSafeRepublisherKeystore(inner, logger)

	// Attempt to put a nil key
	err := safeKS.Put("test-key", nil)
	assert.Error(t, err, "Should reject nil key")
	assert.Contains(t, err.Error(), "cannot put nil key")
}

func TestSafeRepublisherKeystore_AcceptsValidKeys(t *testing.T) {
	inner := keystore.NewMemKeystore()
	logger := &core.Logger{Logger: zaptest.NewLogger(t)}
	safeKS := NewSafeRepublisherKeystore(inner, logger)

	// Generate a valid key
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 2048)
	require.NoError(t, err)

	// Put the valid key
	err = safeKS.Put("valid-key", privKey)
	assert.NoError(t, err, "Should accept valid key")

	// Verify key can be retrieved
	retrieved, err := safeKS.Get("valid-key")
	assert.NoError(t, err)
	assert.NotNil(t, retrieved, "Retrieved key should not be nil")
}

func TestSafeRepublisherKeystore_GetValidatesNonNil(t *testing.T) {
	// This test verifies the defensive check in Get() catches any edge cases
	// where nil keys might exist in the underlying keystore
	inner := keystore.NewMemKeystore()
	logger := &core.Logger{Logger: zaptest.NewLogger(t)}
	safeKS := NewSafeRepublisherKeystore(inner, logger)

	// Manually insert a nil key into the inner keystore (simulating corruption)
	// We need to use the inner keystore directly to bypass the safe wrapper
	_ = inner.Put("corrupted-key", nil)

	// Attempt to retrieve the corrupted key
	_, err := safeKS.Get("corrupted-key")
	assert.Error(t, err, "Should return error for nil key")
	assert.Contains(t, err.Error(), "is nil in keystore")
}

func TestSafeRepublisherKeystore_ListFiltersNilKeys(t *testing.T) {
	inner := keystore.NewMemKeystore()
	logger := &core.Logger{Logger: zaptest.NewLogger(t)}
	safeKS := NewSafeRepublisherKeystore(inner, logger)

	// Add a valid key
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 2048)
	require.NoError(t, err)
	err = safeKS.Put("valid-key", privKey)
	require.NoError(t, err)

	// Manually insert a nil key into the inner keystore (simulating corruption)
	_ = inner.Put("corrupted-key", nil)

	// List keys - should filter out the corrupted one
	names, err := safeKS.List()
	assert.NoError(t, err)
	assert.Len(t, names, 1, "Should only list the valid key")
	assert.Contains(t, names, "valid-key", "Should contain valid key")
	assert.NotContains(t, names, "corrupted-key", "Should not contain corrupted key")
}

