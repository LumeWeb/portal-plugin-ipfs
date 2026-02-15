package ipfs

import (
	"errors"
	"fmt"

	"github.com/ipfs/boxo/keystore"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// SafeKeystore wraps a keystore.Keystore to prevent nil keys from being stored
// This is a defensive wrapper around the vendor's MemKeystore which doesn't validate nil keys
type SafeKeystore struct {
	inner keystore.Keystore
	log   *core.Logger
}

// NewSafeKeystore creates a new safe keystore wrapper
func NewSafeKeystore(inner keystore.Keystore, log *core.Logger) *SafeKeystore {
	return &SafeKeystore{
		inner: inner,
		log:   log,
	}
}

// Has returns whether or not a key exists in the Keystore
func (sk *SafeKeystore) Has(name string) (bool, error) {
	return sk.inner.Has(name)
}

// Put stores a key in the Keystore with nil validation
// Returns an error if the key is nil
func (sk *SafeKeystore) Put(name string, k ic.PrivKey) error {
	if k == nil {
		sk.log.Error("Refusing to put nil key into keystore",
			zap.String("key_name", name),
		)
		return errors.New("cannot put nil key into keystore")
	}
	return sk.inner.Put(name, k)
}

// Get retrieves a key from the Keystore
// Returns the key if it exists, and ErrNoSuchKey otherwise
func (sk *SafeKeystore) Get(name string) (ic.PrivKey, error) {
	key, err := sk.inner.Get(name)
	if err != nil {
		return nil, err
	}
	// Defensive check: even though we prevent nil keys from being stored,
	// validate on retrieval as well to catch any edge cases
	if key == nil {
		sk.log.Error("Retrieved nil key from keystore",
			zap.String("key_name", name),
		)
		return nil, fmt.Errorf("key %s exists but is nil in keystore", name)
	}
	return key, nil
}

// Delete removes a key from the Keystore
func (sk *SafeKeystore) Delete(name string) error {
	return sk.inner.Delete(name)
}

// List returns a list of key identifiers
func (sk *SafeKeystore) List() ([]string, error) {
	names, err := sk.inner.List()
	if err != nil {
		return nil, err
	}

	// Validate that all listed keys are non-nil
	// This is a defensive check to catch any corruption
	validNames := make([]string, 0, len(names))
	for _, name := range names {
		key, err := sk.Get(name)
		if err != nil {
			sk.log.Warn("Failed to get key during list validation",
				zap.String("key_name", name),
				zap.Error(err),
			)
			// Skip keys that can't be retrieved
			continue
		}
		if key == nil {
			sk.log.Error("Found nil key during list, attempting to delete",
				zap.String("key_name", name),
			)
			// Try to delete the corrupted entry
			_ = sk.Delete(name)
			continue
		}
		validNames = append(validNames, name)
	}

	return validNames, nil
}
