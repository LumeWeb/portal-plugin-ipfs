package core

import (
	"context"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/keystore"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p/core/crypto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
)

const (
	// IPNS_KEY_SERVICE is the service ID for the IPNS key management and publishing service
	IPNS_KEY_SERVICE = "ipfs.ipns.key"

	// Deprecated: IPNS_PUBLISHER_SERVICE is deprecated - use IPNS_KEY_SERVICE instead
	// This is kept for backward compatibility
	IPNS_PUBLISHER_SERVICE = IPNS_KEY_SERVICE
)

// IPNSNodeAccess provides access to IPFS node components needed for IPNS operations
type IPNSNodeAccess interface {
	GetPublisher() IPNSPublisher
	GetKeystore() keystore.Keystore
	GetDatastore() datastore.Datastore
	GetPrivateKey() crypto.PrivKey
}

// IPNSPublisher provides an interface for IPNS publishing operations
type IPNSPublisher interface {
	Publish(ctx context.Context, privKey crypto.PrivKey, ipnsPath path.Path, options ...namesys.PublishOption) error
	GetPublished(ctx context.Context, name ipns.Name, checkRouting bool) (*ipns.Record, error)
	ListPublished(ctx context.Context) (map[ipns.Name]*ipns.Record, error)
}

// IPNSBoxoServices provides access to Boxo IPNS services
type IPNSBoxoServices interface {
	GetIPNSNode() IPNSNodeAccess
}

// IPNSKeyService defines the interface for IPNS key management and publishing
type IPNSKeyService interface {
	core.Service

	// CreateKey creates a new IPNS key for the user
	CreateKey(ctx context.Context, userID uint, name string, keyType int) (*pluginDb.IPFSIPNSKey, error)

	// ImportKey imports an existing IPNS key
	ImportKey(ctx context.Context, userID uint, name string, privateKeyBase64 string) (*pluginDb.IPFSIPNSKey, error)

	// ExportKey exports a private key
	ExportKey(ctx context.Context, userID uint, keyID uint) (string, error)

	// ListKeys lists all IPNS keys for a user
	ListKeys(ctx context.Context, userID uint) ([]pluginDb.IPFSIPNSKey, error)

	// GetKeyByName retrieves a single IPNS key by name for a user
	GetKeyByName(ctx context.Context, userID uint, name string) (*pluginDb.IPFSIPNSKey, error)

	// GetKeyByID retrieves a single IPNS key by ID
	GetKeyByID(ctx context.Context, userID uint, keyID uint) (*pluginDb.IPFSIPNSKey, error)

	// DeleteKey deletes an IPNS key (soft delete)
	DeleteKey(ctx context.Context, userID uint, keyID uint) error

	// GetPrivateKey decrypts and returns the private key for a given key
	GetPrivateKey(ctx context.Context, userID uint, keyID uint) (crypto.PrivKey, error)

	// GetPrivateKeyByPeerID decrypts and returns the private key for a given peer ID
	GetPrivateKeyByPeerID(ctx context.Context, peerID string) (crypto.PrivKey, uint, error)

	// SyncToBoxoKeystore syncs all active IPNS keys from the database to the boxo keystore
	SyncToBoxoKeystore(ctx context.Context) error

	// PublishCID publishes a CID to an IPNS key using peer ID
	PublishCID(ctx context.Context, keyID string, cidStr string, ttl time.Duration) error

	// PublishWithKey publishes a CID using the provided private key
	PublishWithKey(ctx context.Context, privKey crypto.PrivKey, cidStr string, ttl time.Duration) error

	// GetPublished retrieves the latest published record for an IPNS name
	GetPublished(ctx context.Context, keyID string, checkRouting bool) (*ipns.Record, error)

	// ListPublished returns all IPNS records published by this node
	ListPublished(ctx context.Context) (map[ipns.Name]*ipns.Record, error)
}
