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
	"go.lumeweb.com/queryutil/filter"
)

const (
	IPNS_KEY_SERVICE    = "ipfs.ipns.key"
	IPNS_PUBLISHER_SERVICE = IPNS_KEY_SERVICE
)

type IPNSNodeAccess interface {
	GetPublisher() IPNSPublisher
	GetKeystore() keystore.Keystore
	GetDatastore() datastore.Datastore
	GetPrivateKey() crypto.PrivKey
}

type IPNSPublisher interface {
	Publish(ctx context.Context, privKey crypto.PrivKey, ipnsPath path.Path, options ...namesys.PublishOption) error
	GetPublished(ctx context.Context, name ipns.Name, checkRouting bool) (*ipns.Record, error)
	ListPublished(ctx context.Context) (map[ipns.Name]*ipns.Record, error)
}

type IPNSBoxoServices interface {
	GetIPNSNode() IPNSNodeAccess
}

type IPNSKeyService interface {
	core.Service

	CreateKey(ctx context.Context, userID uint, name string, keyType int) (*pluginDb.IPFSIPNSKey, error)
	ImportKey(ctx context.Context, userID uint, name string, privateKeyBase64 string) (*pluginDb.IPFSIPNSKey, error)
	ExportKey(ctx context.Context, userID uint, keyID uint) (string, error)
	ListKeys(ctx context.Context, userID uint) ([]pluginDb.IPFSIPNSKey, error)
	ListKeysWithFilters(ctx context.Context, userID uint, filters []filter.CrudFilter, sort []filter.Sort, pagination filter.Pagination) ([]*pluginDb.IPFSIPNSKey, int64, error)
	GetKeyByName(ctx context.Context, userID uint, name string) (*pluginDb.IPFSIPNSKey, error)
	GetKeyByID(ctx context.Context, userID uint, keyID uint) (*pluginDb.IPFSIPNSKey, error)
	DeleteKey(ctx context.Context, userID uint, keyID uint) error
	GetPrivateKey(ctx context.Context, userID uint, keyID uint) (crypto.PrivKey, error)
	GetPrivateKeyByPeerID(ctx context.Context, peerID string) (crypto.PrivKey, uint, error)
	PublishCID(ctx context.Context, keyID string, cidStr string, ttl time.Duration) error
	PublishWithKey(ctx context.Context, privKey crypto.PrivKey, cidStr string, ttl time.Duration) error
	GetPublished(ctx context.Context, keyID string, checkRouting bool) (*ipns.Record, error)
	ListPublished(ctx context.Context) (map[ipns.Name]*ipns.Record, error)
}
