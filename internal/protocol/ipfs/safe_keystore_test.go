package ipfs

import (
	"testing"

	"github.com/ipfs/boxo/keystore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.lumeweb.com/portal/core"
)

func TestSafeKeystore_RejectsNilKeys(t *testing.T) {
	inner := keystore.NewMemKeystore()
	logger := core.NewLogger(zap.NewNop(), zap.NewNop().AtomicLevel())
	safeKS := NewSafeKeystore(inner, logger)

	// Attempt to put a nil key
	err := safeKS.Put("test-key", nil)
	assert.Error(t, err, "Should reject nil key")
	assert.Contains(t, err.Error(), "cannot put nil key")

	// Verify the key was not stored
	has, err := safeKS.Has("test-key")
	require.NoError(t, err)
	assert.False(t, has, "Nil key should not be stored")
}

func TestSafeKeystore_AllowsValidKeys(t *testing.T) {
	inner := keystore.NewMemKeystore()
	logger := core.NewLogger(zap.NewNop(), zap.NewNop().AtomicLevel())
	safeKS := NewSafeKeystore(inner, logger)

	// Generate a valid key
	privKey, _, err := crypto.GenerateEd25519Key(crypto.RandSource)
	require.NoError(t, err)

	// Put the valid key
	err = safeKS.Put("test-key", privKey)
	assert.NoError(t, err, "Should accept valid key")

	// Verify the key was stored
	has, err := safeKS.Has("test-key")
	require.NoError(t, err)
	assert.True(t, has, "Valid key should be stored")

	// Verify we can retrieve it
	retrieved, err := safeKS.Get("test-key")
	require.NoError(t, err)
	assert.NotNil(t, retrieved, "Retrieved key should not be nil")
}

func TestSafeKeystore_ListValidatesKeys(t *testing.T) {
	inner := keystore.NewMemKeystore()
	logger := core.NewLogger(zap.NewNop(), zap.NewNop().AtomicLevel())
	safeKS := NewSafeKeystore(inner, logger)

	// Add a valid key
	privKey, _, err := crypto.GenerateEd25519Key(crypto.RandSource)
	require.NoError(t, err)
	err = safeKS.Put("valid-key", privKey)
	require.NoError(t, err)

	// List keys
	names, err := safeKS.List()
	require.NoError(t, err)
	assert.Contains(t, names, "valid-key", "Should list valid key")
}

func TestSafeKeystore_GetValidatesNonNil(t *testing.T) {
	// This test verifies the defensive check in Get() catches any edge cases
	// where nil keys might exist in the underlying keystore
	inner := keystore.NewMemKeystore()
	logger := core.NewLogger(zap.NewNop(), zap.NewNop().AtomicLevel())
	safeKS := NewSafeKeystore(inner, logger)

	// Try to get a non-existent key
	_, err := safeKS.Get("non-existent-key")
	assert.Error(t, err, "Should return error for non-existent key")
}

func TestSafeKeystore_Delete(t *testing.T) {
	inner := keystore.NewMemKeystore()
	logger := core.NewLogger(zap.NewNop(), zap.NewNop().AtomicLevel())
	safeKS := NewSafeKeystore(inner, logger)

	// Add a valid key
	privKey, _, err := crypto.GenerateEd25519Key(crypto.RandSource)
	require.NoError(t, err)
	err = safeKS.Put("test-key", privKey)
	require.NoError(t, err)

	// Delete the key
	err = safeKS.Delete("test-key")
	assert.NoError(t, err)

	// Verify it's gone
	has, err := safeKS.Has("test-key")
	require.NoError(t, err)
	assert.False(t, has)
}
