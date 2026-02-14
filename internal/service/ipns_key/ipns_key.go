package ipns_key

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/crypto/hkdf"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Key type constants from libp2p
const (
	KeyType_RSA       = 0
	KeyType_Ed25519   = 1
	KeyType_Secp256k1 = 2
	KeyType_ECDSA     = 3
)

// IPNSKeyServiceDefault implements the IPNS key management service
type IPNSKeyServiceDefault struct {
	*core.BaseComponent
	protocol protocol.ProtoNode
}

// Ensure IPNSKeyServiceDefault implements the interface
var _ pluginCore.IPNSKeyService = (*IPNSKeyServiceDefault)(nil)

// NewIPNSKeyService creates a new IPNS key service
func NewIPNSKeyService() (core.Service, []core.ContextBuilderOption, error) {
	svc := &IPNSKeyServiceDefault{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			proto := core.GetProtocol(internal.ProtocolName)
			ipfsProto, ok := proto.(protocol.ProtoNode)
			if !ok {
				return fmt.Errorf("protocol %s is not of type *protocol.Protocol", internal.ProtocolName)
			}
			svc.protocol = ipfsProto

			// Migrate encryption seed from placeholder to actual portal identity seed
			err := svc.MigrateEncryptionSeed(ctx)
			if err != nil {
				svc.Logger().Warn("IPNS key encryption seed migration encountered issues",
					zap.Error(err),
				)
				// Don't fail startup - log and continue
			}

			// Sync all IPNS keys to the boxo keystore during startup
			err = svc.SyncToBoxoKeystore(ctx)
			if err != nil {
				svc.Logger().Error("Failed to sync IPNS keys to boxo keystore during startup",
					zap.Error(err),
				)
				// Don't fail startup - log and continue
			}

			return nil
		}),
	)

	return svc, opts, nil
}

func (s *IPNSKeyServiceDefault) ID() string {
	return pluginCore.IPNS_KEY_SERVICE
}

// CreateKey creates a new IPNS key for the user
func (s *IPNSKeyServiceDefault) CreateKey(ctx context.Context, userID uint, name string, keyType int) (*pluginDb.IPFSIPNSKey, error) {
	ctx, span := core.TraceMethod(ctx, "IPNSKeyServiceDefault.CreateKey")
	defer span.End()

	// Default to Ed25519 if not specified
	if keyType == 0 {
		keyType = KeyType_Ed25519
	}

	privKey, pubKey, err := crypto.GenerateKeyPairWithReader(keyType, 2048, rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to derive peer ID: %w", err)
	}

	// Encrypt private key
	encryptedKey, err := s.encryptPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt private key: %w", err)
	}

	key := &pluginDb.IPFSIPNSKey{
		UserID:              userID,
		Name:                name,
		IPNSName:            peerID.String(),
		PeerID:              peerID.String(),
		PrivateKeyEncrypted: encryptedKey,
	}

	err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Create(key)
	})
	if err != nil {
		s.Logger().Error("Failed to create IPNS key", zap.Error(err), zap.Uint("user_id", userID), zap.String("name", name))
		return nil, fmt.Errorf("failed to create IPNS key: %w", err)
	}

	// Sync the new key to the boxo keystore (non-fatal)
	err = s.syncKeyToBoxoKeystore(ctx, key, privKey)
	if err != nil {
		s.Logger().Warn("Failed to sync new IPNS key to boxo keystore (key saved to database)",
			zap.Error(err),
			zap.Uint("key_id", key.ID),
			zap.String("peer_id", peerID.String()),
		)
		// Don't fail the operation - the key is saved in the database
	}

	s.Logger().Debug("Created IPNS key", zap.Uint("user_id", userID), zap.String("name", name), zap.String("peer_id", peerID.String()))
	return key, nil
}

// ImportKey imports an existing IPNS key
func (s *IPNSKeyServiceDefault) ImportKey(ctx context.Context, userID uint, name string, privateKeyBase64 string) (*pluginDb.IPFSIPNSKey, error) {
	ctx, span := core.TraceMethod(ctx, "IPNSKeyServiceDefault.ImportKey")
	defer span.End()

	// Decode base64
	keyBytes, err := base64.StdEncoding.DecodeString(privateKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 key: %w", err)
	}

	// Unmarshal private key
	privKey, err := crypto.UnmarshalPrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	pubKey := privKey.GetPublic()
	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to derive peer ID: %w", err)
	}

	// Check if peer ID already exists for this user
	var existingKey pluginDb.IPFSIPNSKey
	err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("user_id = ? AND peer_id = ?", userID, peerID.String()).First(&existingKey)
	})
	if err == nil {
		return nil, fmt.Errorf("IPNS key with peer ID %s already exists for this user", peerID.String())
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("failed to check existing key: %w", err)
	}

	// Encrypt private key
	encryptedKey, err := s.encryptPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt private key: %w", err)
	}

	key := &pluginDb.IPFSIPNSKey{
		UserID:              userID,
		Name:                name,
		IPNSName:            peerID.String(),
		PeerID:              peerID.String(),
		PrivateKeyEncrypted: encryptedKey,
	}

	err = db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Create(key)
	})
	if err != nil {
		s.Logger().Error("Failed to import IPNS key", zap.Error(err), zap.Uint("user_id", userID), zap.String("name", name))
		return nil, fmt.Errorf("failed to import IPNS key: %w", err)
	}

	// Sync the imported key to the boxo keystore (non-fatal)
	err = s.syncKeyToBoxoKeystore(ctx, key, privKey)
	if err != nil {
		s.Logger().Warn("Failed to sync imported IPNS key to boxo keystore (key saved to database)",
			zap.Error(err),
			zap.Uint("key_id", key.ID),
			zap.String("peer_id", peerID.String()),
		)
		// Don't fail the operation - the key is saved in the database
	}

	s.Logger().Debug("Imported IPNS key", zap.Uint("user_id", userID), zap.String("name", name), zap.String("peer_id", peerID.String()))
	return key, nil
}

// ExportKey exports a private key
func (s *IPNSKeyServiceDefault) ExportKey(ctx context.Context, userID uint, keyID uint) (string, error) {
	ctx, span := core.TraceMethod(ctx, "IPNSKeyServiceDefault.ExportKey")
	defer span.End()

	key, err := s.GetKeyByID(ctx, userID, keyID)
	if err != nil {
		return "", err
	}

	// Decrypt private key
	privKey, err := s.decryptPrivateKey(key.PrivateKeyEncrypted)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt private key: %w", err)
	}

	// Marshal to protobuf format
	keyBytes, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		return "", fmt.Errorf("failed to marshal private key: %w", err)
	}

	return base64.StdEncoding.EncodeToString(keyBytes), nil
}

// ListKeys lists all IPNS keys for a user
func (s *IPNSKeyServiceDefault) ListKeys(ctx context.Context, userID uint) ([]pluginDb.IPFSIPNSKey, error) {
	ctx, span := core.TraceMethod(ctx, "IPNSKeyServiceDefault.ListKeys")
	defer span.End()

	var keys []pluginDb.IPFSIPNSKey
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("user_id = ?", userID).Find(&keys)
	})
	if err != nil {
		s.Logger().Error("Failed to list IPNS keys", zap.Error(err), zap.Uint("user_id", userID))
		return nil, fmt.Errorf("failed to list IPNS keys: %w", err)
	}

	return keys, nil
}

// GetKeyByID retrieves a single IPNS key by ID
func (s *IPNSKeyServiceDefault) GetKeyByID(ctx context.Context, userID uint, keyID uint) (*pluginDb.IPFSIPNSKey, error) {
	ctx, span := core.TraceMethod(ctx, "IPNSKeyServiceDefault.GetKeyByID")
	defer span.End()

	var key pluginDb.IPFSIPNSKey
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("id = ? AND user_id = ?", keyID, userID).First(&key)
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("IPNS key not found")
		}
		s.Logger().Error("Failed to get IPNS key", zap.Error(err), zap.Uint("key_id", keyID), zap.Uint("user_id", userID))
		return nil, fmt.Errorf("failed to get IPNS key: %w", err)
	}

	return &key, nil
}

// DeleteKey deletes an IPNS key (soft delete)
func (s *IPNSKeyServiceDefault) DeleteKey(ctx context.Context, userID uint, keyID uint) error {
	ctx, span := core.TraceMethod(ctx, "IPNSKeyServiceDefault.DeleteKey")
	defer span.End()

	// Check if key exists
	var key pluginDb.IPFSIPNSKey
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("id = ? AND user_id = ? AND deleted_at IS NULL", keyID, userID).First(&key)
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("IPNS key not found")
		}
		s.Logger().Error("Failed to get IPNS key for deletion", zap.Error(err), zap.Uint("key_id", keyID), zap.Uint("user_id", userID))
		return fmt.Errorf("failed to get IPNS key: %w", err)
	}

	// Check if key is referenced by any active websites
	var websiteCount int64
	checkErr := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Table("ipfs_websites").
			Where("target_hash = ? AND target_type = ? AND status = ?", key.PeerID, "ipns", "active").
			Count(&websiteCount)
	})
	if checkErr != nil {
		return fmt.Errorf("failed to check website references: %w", checkErr)
	}

	if websiteCount > 0 {
		return fmt.Errorf("cannot delete IPNS key: referenced by %d active website(s)", websiteCount)
	}

	deleteErr := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Delete(&key)
	})
	if deleteErr != nil {
		s.Logger().Error("Failed to delete IPNS key", zap.Error(deleteErr), zap.Uint("key_id", keyID), zap.Uint("user_id", userID))
		return fmt.Errorf("failed to delete IPNS key: %w", deleteErr)
	}

	// Remove from boxo keystore
	proto := core.GetProtocol(internal.ProtocolName)
	if proto != nil {
		if ipnsNode, ok := proto.(pluginCore.IPNSBoxoServices); ok {
			node := ipnsNode.GetIPNSNode()
			if node != nil {
				boxoKS := node.GetKeystore()
				if boxoKS != nil {
					keyName := key.PeerID
					has, err := boxoKS.Has(keyName)
					if err != nil {
						s.Logger().Warn("Failed to check if key exists in boxo keystore",
							zap.Error(err),
							zap.String("peer_id", key.PeerID),
							zap.Uint("user_id", userID),
						)
					} else if has {
						err = boxoKS.Delete(keyName)
						if err != nil {
							s.Logger().Warn("Failed to delete key from boxo keystore",
								zap.Error(err),
								zap.String("peer_id", key.PeerID),
								zap.Uint("user_id", userID),
							)
						} else {
							s.Logger().Debug("Deleted IPNS key from boxo keystore",
								zap.String("peer_id", key.PeerID),
								zap.Uint("user_id", userID),
							)
						}
					}
				}
			}
		}
	}

	s.Logger().Debug("Deleted IPNS key", zap.Uint("key_id", keyID), zap.Uint("user_id", userID))
	return nil
}

// syncKeyToBoxoKeystore syncs a single IPNS key to the boxo keystore
// This is called after CreateKey and ImportKey operations
func (s *IPNSKeyServiceDefault) syncKeyToBoxoKeystore(ctx context.Context, key *pluginDb.IPFSIPNSKey, privKey crypto.PrivKey) error {
	ctx, span := core.TraceMethod(ctx, "IPNSKeyServiceDefault.syncKeyToBoxoKeystore")
	defer span.End()

	// Get the boxo keystore from the IPFS node
	proto := core.GetProtocol(internal.ProtocolName)
	if proto == nil {
		return fmt.Errorf("IPFS protocol not found")
	}

	ipnsNode, ok := proto.(pluginCore.IPNSBoxoServices)
	if !ok {
		return fmt.Errorf("IPFS protocol does not implement IPNSBoxoServices")
	}

	node := ipnsNode.GetIPNSNode()
	if node == nil {
		return fmt.Errorf("IPFS node not found")
	}

	boxoKS := node.GetKeystore()
	if boxoKS == nil {
		return fmt.Errorf("boxo keystore not found")
	}

	// Use peer ID as the key name in the keystore
	keyName := key.PeerID

	// Check if key already exists in keystore
	has, err := boxoKS.Has(keyName)
	if err != nil {
		return fmt.Errorf("failed to check if key exists in keystore: %w", err)
	}

	if has {
		// Key already exists in keystore, skip
		s.Logger().Debug("Key already exists in boxo keystore, skipping sync",
			zap.String("peer_id", key.PeerID),
		)
		return nil
	}

	// Import into boxo keystore
	err = boxoKS.Put(keyName, privKey)
	if err != nil {
		return fmt.Errorf("failed to import key into boxo keystore: %w", err)
	}

	s.Logger().Debug("Synced IPNS key to boxo keystore",
		zap.Uint("key_id", key.ID),
		zap.String("peer_id", key.PeerID),
		zap.String("name", key.Name),
	)

	return nil
}

// SyncToBoxoKeystore syncs all active IPNS keys from the database to the boxo keystore
// This should be called during plugin startup and after key creation/import
func (s *IPNSKeyServiceDefault) SyncToBoxoKeystore(ctx context.Context) error {
	ctx, span := core.TraceMethod(ctx, "IPNSKeyServiceDefault.SyncToBoxoKeystore")
	defer span.End()

	// Get the boxo keystore from the IPFS node
	proto := core.GetProtocol(internal.ProtocolName)
	if proto == nil {
		return fmt.Errorf("IPFS protocol not found")
	}

	ipnsNode, ok := proto.(pluginCore.IPNSBoxoServices)
	if !ok {
		return fmt.Errorf("IPFS protocol does not implement IPNSBoxoServices")
	}

	node := ipnsNode.GetIPNSNode()
	if node == nil {
		return fmt.Errorf("IPNS node not found")
	}

	boxoKS := node.GetKeystore()
	if boxoKS == nil {
		return fmt.Errorf("boxo keystore not found")
	}

	// Get all active IPNS keys from database
	var keys []pluginDb.IPFSIPNSKey
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Find(&keys)
	})
	if err != nil {
		s.Logger().Error("Failed to list IPNS keys for sync", zap.Error(err))
		return fmt.Errorf("failed to list IPNS keys: %w", err)
	}

	syncedCount := 0
	skippedCount := 0

	for _, key := range keys {
		// Use peer ID as the key name in the keystore
		keyName := key.PeerID

		// Check if key already exists in keystore
		has, err := boxoKS.Has(keyName)
		if err != nil {
			s.Logger().Warn("Failed to check if key exists in keystore",
				zap.Error(err),
				zap.String("peer_id", key.PeerID),
			)
			continue
		}

		if has {
			// Key already exists in keystore, skip
			skippedCount++
			continue
		}

		// Decrypt the private key
		privKey, err := s.decryptPrivateKey(key.PrivateKeyEncrypted)
		if err != nil {
			s.Logger().Error("Failed to decrypt private key for sync",
				zap.Error(err),
				zap.Uint("key_id", key.ID),
				zap.String("peer_id", key.PeerID),
			)
			continue
		}

		// Import into boxo keystore
		err = boxoKS.Put(keyName, privKey)
		if err != nil {
			s.Logger().Error("Failed to import key into boxo keystore",
				zap.Error(err),
				zap.Uint("key_id", key.ID),
				zap.String("peer_id", key.PeerID),
			)
			continue
		}

		syncedCount++
		s.Logger().Debug("Synced IPNS key to boxo keystore",
			zap.Uint("key_id", key.ID),
			zap.String("peer_id", key.PeerID),
			zap.String("name", key.Name),
		)
	}

	s.Logger().Info("Completed IPNS key sync to boxo keystore",
		zap.Int("synced", syncedCount),
		zap.Int("skipped", skippedCount),
		zap.Int("total", len(keys)),
	)

	return nil
}

// GetPrivateKeyByPeerID decrypts and returns the private key for a given peer ID
func (s *IPNSKeyServiceDefault) GetPrivateKeyByPeerID(ctx context.Context, peerIDStr string) (crypto.PrivKey, uint, error) {
	ctx, span := core.TraceMethod(ctx, "IPNSKeyServiceDefault.GetPrivateKeyByPeerID")
	defer span.End()

	var key pluginDb.IPFSIPNSKey
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("peer_id = ?", peerIDStr).First(&key)
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, 0, fmt.Errorf("IPNS key not found")
		}
		s.Logger().Error("Failed to get IPNS key by peer ID", zap.Error(err), zap.String("peer_id", peerIDStr))
		return nil, 0, fmt.Errorf("failed to get IPNS key: %w", err)
	}

	privKey, err := s.decryptPrivateKey(key.PrivateKeyEncrypted)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decrypt private key: %w", err)
	}

	return privKey, key.UserID, nil
}

// GetPrivateKey decrypts and returns the private key for a given key
func (s *IPNSKeyServiceDefault) GetPrivateKey(ctx context.Context, userID uint, keyID uint) (crypto.PrivKey, error) {
	key, err := s.GetKeyByID(ctx, userID, keyID)
	if err != nil {
		return nil, err
	}

	return s.decryptPrivateKey(key.PrivateKeyEncrypted)
}

// encryptPrivateKey encrypts a private key using the portal identity seed
func (s *IPNSKeyServiceDefault) encryptPrivateKey(privKey crypto.PrivKey) ([]byte, error) {
	keyBytes, err := crypto.MarshalPrivateKey(privKey)
	if err != nil {
		return nil, err
	}

	ctx := s.Context()
	portalSeed := ctx.Config().Config().Core.Identity.PrivateKey()

	hasher := hkdf.New(sha256.New, portalSeed, ctx.Config().Config().Core.NodeID.Bytes(), []byte("ipns-key-encryption"))
	derivedSeed := make([]byte, 32)

	if _, err := io.ReadFull(hasher, derivedSeed); err != nil {
		return nil, fmt.Errorf("failed to derive encryption seed: %w", err)
	}

	block, err := aes.NewCipher(derivedSeed)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := gcm.Seal(nonce, nonce, keyBytes, nil)
	return ciphertext, nil
}

// decryptPrivateKey decrypts an encrypted private key
func (s *IPNSKeyServiceDefault) decryptPrivateKey(encryptedKey []byte) (crypto.PrivKey, error) {
	ctx := s.Context()
	portalSeed := ctx.Config().Config().Core.Identity.PrivateKey()

	hasher := hkdf.New(sha256.New, portalSeed, ctx.Config().Config().Core.NodeID.Bytes(), []byte("ipns-key-encryption"))
	derivedSeed := make([]byte, 32)

	if _, err := io.ReadFull(hasher, derivedSeed); err != nil {
		return nil, fmt.Errorf("failed to derive decryption seed: %w", err)
	}

	block, err := aes.NewCipher(derivedSeed)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(encryptedKey) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := encryptedKey[:nonceSize], encryptedKey[nonceSize:]
	keyBytes, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return crypto.UnmarshalPrivateKey(keyBytes)
}

// decryptPrivateKeyWithSeed decrypts an encrypted private key using a specific seed
// This is used for migration from old seed to new seed
func (s *IPNSKeyServiceDefault) decryptPrivateKeyWithSeed(encryptedKey []byte, seed []byte) (crypto.PrivKey, error) {
	ctx := s.Context()
	hasher := hkdf.New(sha256.New, seed, ctx.Config().Config().Core.NodeID.Bytes(), []byte("ipns-key-encryption"))
	derivedSeed := make([]byte, 32)

	if _, err := io.ReadFull(hasher, derivedSeed); err != nil {
		return nil, fmt.Errorf("failed to derive decryption seed: %w", err)
	}

	block, err := aes.NewCipher(derivedSeed)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(encryptedKey) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := encryptedKey[:nonceSize], encryptedKey[nonceSize:]
	keyBytes, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return crypto.UnmarshalPrivateKey(keyBytes)
}

// MigrateEncryptionSeed migrates all IPNS keys from old placeholder seed to new portal identity seed
func (s *IPNSKeyServiceDefault) MigrateEncryptionSeed(ctx core.Context) error {
	s.Logger().Info("Starting IPNS key encryption seed migration")

	var keys []pluginDb.IPFSIPNSKey
	err := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
		return tx.Find(&keys)
	})
	if err != nil {
		s.Logger().Error("Failed to list IPNS keys for migration", zap.Error(err))
		return fmt.Errorf("failed to list IPNS keys: %w", err)
	}

	if len(keys) == 0 {
		s.Logger().Info("No IPNS keys to migrate")
		return nil
	}

	oldSeed := []byte("placeholder-portal-seed")
	migratedCount := 0
	failedCount := 0

	for _, key := range keys {
		privKey, err := s.decryptPrivateKeyWithSeed(key.PrivateKeyEncrypted, oldSeed)
		if err != nil {
			s.Logger().Warn("Failed to decrypt key with old seed during migration",
				zap.Error(err),
				zap.Uint("key_id", key.ID),
				zap.String("peer_id", key.PeerID),
			)
			failedCount++
			continue
		}

		newEncryptedKey, err := s.encryptPrivateKey(privKey)
		if err != nil {
			s.Logger().Error("Failed to encrypt key with new seed during migration",
				zap.Error(err),
				zap.Uint("key_id", key.ID),
				zap.String("peer_id", key.PeerID),
			)
			failedCount++
			continue
		}

		updateErr := db.RetryableComponentTransaction(s, ctx, func(tx *gorm.DB) *gorm.DB {
			return tx.Model(&pluginDb.IPFSIPNSKey{}).
				Where("id = ?", key.ID).
				Update("private_key_encrypted", newEncryptedKey)
		})
		if updateErr != nil {
			s.Logger().Error("Failed to update encrypted key in database",
				zap.Error(updateErr),
				zap.Uint("key_id", key.ID),
				zap.String("peer_id", key.PeerID),
			)
			failedCount++
			continue
		}

		migratedCount++
		s.Logger().Debug("Migrated IPNS key",
			zap.Uint("key_id", key.ID),
			zap.String("peer_id", key.PeerID),
			zap.String("name", key.Name),
		)
	}

	s.Logger().Info("Completed IPNS key encryption seed migration",
		zap.Int("migrated", migratedCount),
		zap.Int("failed", failedCount),
		zap.Int("total", len(keys)),
	)

	if failedCount > 0 {
		return fmt.Errorf("migration completed with %d failures out of %d keys", failedCount, len(keys))
	}

	return nil
}
