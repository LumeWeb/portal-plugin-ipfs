package boxo

import (
	"testing"

	"github.com/ipfs/boxo/keystore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap/zaptest"
)

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
