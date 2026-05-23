package ipns_key

import (
	"context"
	"errors"

	"github.com/ipfs/boxo/keystore"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	portalDb "go.lumeweb.com/portal/db"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// DBKeystore implements keystore.Keystore backed by the ipfs_ipns_keys
// database table. Key creation goes through IPNSKeyService.CreateKey, so Put
// is not supported and returns an error. The republisher only calls List and
// Get, which is all this provides.
type DBKeystore struct {
	db      *gorm.DB
	decrypt func([]byte) (ic.PrivKey, error)
	log     *zap.Logger
}

var _ keystore.Keystore = (*DBKeystore)(nil)

func NewDBKeystore(db *gorm.DB, decrypt func([]byte) (ic.PrivKey, error), log *zap.Logger) *DBKeystore {
	return &DBKeystore{db: db, decrypt: decrypt, log: log}
}

// peerIDToMultihash converts a keystore name (peer ID string) to a multihash
// for querying the peer_id_multihash column.
func peerIDToMultihash(name string) (mh.Multihash, error) {
	pid, err := peer.Decode(name)
	if err != nil {
		return nil, keystore.ErrNoSuchKey
	}
	return mh.Multihash(pid), nil
}

func (d *DBKeystore) Has(name string) (bool, error) {
	mhVal, err := peerIDToMultihash(name)
	if err != nil {
		return false, nil
	}
	var count int64
	err = d.db.Model(&pluginDb.IPFSIPNSKey{}).
		Where("peer_id_multihash = ? AND deleted_at IS NULL", mhVal).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (d *DBKeystore) Put(name string, k ic.PrivKey) error {
	return errors.New("DBKeystore does not support Put — use IPNSKeyService.CreateKey")
}

func (d *DBKeystore) Get(name string) (ic.PrivKey, error) {
	mhVal, err := peerIDToMultihash(name)
	if err != nil {
		return nil, err
	}
	var key pluginDb.IPFSIPNSKey
	err = d.db.Where("peer_id_multihash = ? AND deleted_at IS NULL", mhVal).First(&key).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, keystore.ErrNoSuchKey
		}
		return nil, err
	}
	privKey, err := d.decrypt(key.PrivateKeyEncrypted)
	if err != nil {
		return nil, err
	}
	if privKey == nil {
		return nil, keystore.ErrNoSuchKey
	}
	return privKey, nil
}

func (d *DBKeystore) Delete(name string) error {
	mhVal, err := peerIDToMultihash(name)
	if err != nil {
		return err
	}
	return d.db.Where("peer_id_multihash = ? AND deleted_at IS NULL", mhVal).
		Delete(&pluginDb.IPFSIPNSKey{}).Error
}

func (d *DBKeystore) List() ([]string, error) {
	var keys []pluginDb.IPFSIPNSKey
	err := d.db.Where("deleted_at IS NULL").Find(&keys).Error
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		names = append(names, key.PeerID().String())
	}
	return names, nil
}

func (d *DBKeystore) ListKeysWithCID(ctx context.Context) ([]pluginDb.IPFSIPNSKey, error) {
	var keys []pluginDb.IPFSIPNSKey
	err := portalDb.RetryableTransaction(ctx, d.db, func(tx *gorm.DB) *gorm.DB {
		return tx.Where("last_published_cid != '' AND last_published_cid IS NOT NULL AND deleted_at IS NULL").Find(&keys)
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}
