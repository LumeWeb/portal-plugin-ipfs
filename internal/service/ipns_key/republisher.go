package ipns_key

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.uber.org/zap"
)

// Republisher broadcasts all IPNS keys with a known CID to the DHT on a
// regular interval. Unlike boxo's Republisher, it does not require a separate
// datastore — keys that exist in the database with a LastPublishedCID are
// considered published and will be rebroadcast.
type Republisher struct {
	publisher      namesys.Publisher
	keystore       *DBKeystore
	self           ic.PrivKey
	decrypt        func([]byte) (ic.PrivKey, error)
	recordLifetime time.Duration
	interval       time.Duration
	log            *zap.Logger
}

const (
	defaultRepublishInterval = time.Hour
	defaultRecordLifetime    = ipns.DefaultRecordLifetime
	initialRepublishDelay    = time.Minute
	failureRetryInterval     = 5 * time.Minute
)

func NewRepublisher(publisher namesys.Publisher, keystore *DBKeystore, self ic.PrivKey, decrypt func([]byte) (ic.PrivKey, error), log *zap.Logger) *Republisher {
	return &Republisher{
		publisher:      publisher,
		keystore:       keystore,
		self:           self,
		decrypt:        decrypt,
		recordLifetime: defaultRecordLifetime,
		interval:       defaultRepublishInterval,
		log:            log,
	}
}

func (r *Republisher) SetInterval(d time.Duration)       { r.interval = d }
func (r *Republisher) SetRecordLifetime(d time.Duration) { r.recordLifetime = d }

func (r *Republisher) Run() func() {
	ctx, cancel := context.WithCancel(context.Background())
	go r.run(ctx)
	return func() {
		r.log.Info("Stopping IPNS republisher")
		cancel()
	}
}

func (r *Republisher) run(ctx context.Context) {
	timer := time.NewTimer(initialRepublishDelay)
	defer timer.Stop()
	if r.interval < initialRepublishDelay {
		timer.Reset(r.interval)
	}

	for {
		select {
		case <-timer.C:
			timer.Reset(r.interval)
			err := r.republishAll(ctx)
			if err != nil {
				r.log.Error("Republisher failed", zap.Error(err))
				if failureRetryInterval < r.interval {
					timer.Reset(failureRetryInterval)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Republisher) republishAll(ctx context.Context) error {
	keys, err := r.keystore.ListKeysWithCID(ctx)
	if err != nil {
		return fmt.Errorf("list keys: %w", err)
	}

	published := 0
	failed := 0
	for _, key := range keys {
		if err := r.republishKey(ctx, key); err != nil {
			r.log.Error("Failed to republish IPNS key",
				zap.Error(err),
				zap.Stringer("peer_id", key.PeerID()),
				zap.String("cid", key.LastPublishedCID),
			)
			failed++
			continue
		}
		published++
	}

	r.log.Info("Republish cycle complete",
		zap.Int("published", published),
		zap.Int("failed", failed),
		zap.Int("total", len(keys)),
	)
	return nil
}

func (r *Republisher) republishKey(ctx context.Context, key pluginDb.IPFSIPNSKey) error {
	privKey, err := r.decrypt(key.PrivateKeyEncrypted)
	if err != nil {
		return fmt.Errorf("decrypt private key: %w", err)
	}
	if privKey == nil {
		return fmt.Errorf("decrypted private key is nil for peer %s", key.PeerID())
	}

	targetCid, err := cid.Decode(key.LastPublishedCID)
	if err != nil {
		return fmt.Errorf("parse CID %s: %w", key.LastPublishedCID, err)
	}

	eol := time.Now().Add(r.recordLifetime)
	err = r.publisher.Publish(ctx, privKey, path.FromCid(targetCid), namesys.PublishWithEOL(eol))
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}
