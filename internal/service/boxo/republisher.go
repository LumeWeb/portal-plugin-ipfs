package boxo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/keystore"
	boxoRepublisher "github.com/ipfs/boxo/namesys/republisher"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	"go.uber.org/zap"
)

// SafeRepublisherKeystore wraps a keystore with additional validation for the republisher
// It ensures that all keys returned to the republisher are non-nil
type SafeRepublisherKeystore struct {
	inner keystore.Keystore
	log   *core.Logger
}

// NewSafeRepublisherKeystore creates a new safe republisher keystore wrapper
func NewSafeRepublisherKeystore(inner keystore.Keystore, log *core.Logger) keystore.Keystore {
	return &SafeRepublisherKeystore{
		inner: inner,
		log:   log,
	}
}

// Has returns whether or not a key exists in the Keystore
func (srk *SafeRepublisherKeystore) Has(name string) (bool, error) {
	return srk.inner.Has(name)
}

// Put stores a key in the Keystore with nil validation
func (srk *SafeRepublisherKeystore) Put(name string, k ic.PrivKey) error {
	if k == nil {
		srk.log.Error("Refusing to put nil key into republisher keystore",
			zap.String("key_name", name),
		)
		return errors.New("cannot put nil key into keystore")
	}
	return srk.inner.Put(name, k)
}

// Get retrieves a key from the Keystore
// Returns the key if it exists, and ErrNoSuchKey otherwise
func (srk *SafeRepublisherKeystore) Get(name string) (ic.PrivKey, error) {
	key, err := srk.inner.Get(name)
	if err != nil {
		return nil, err
	}
	// Defensive check: validate on retrieval to catch any edge cases
	if key == nil {
		srk.log.Error("Retrieved nil key from republisher keystore",
			zap.String("key_name", name),
		)
		return nil, fmt.Errorf("key %s exists but is nil in keystore", name)
	}
	return key, nil
}

// Delete removes a key from the Keystore
func (srk *SafeRepublisherKeystore) Delete(name string) error {
	return srk.inner.Delete(name)
}

// List returns a list of key identifiers with validation
func (srk *SafeRepublisherKeystore) List() ([]string, error) {
	names, err := srk.inner.List()
	if err != nil {
		return nil, err
	}

	// Validate that all listed keys are non-nil and filter out corrupted entries
	validNames := ipfs.ValidateKeyList(names, srk.Get, srk.Delete, srk.log, "SafeRepublisherKeystore")

	return validNames, nil
}

// IPNSRepublisherService wraps boxo/namesys/republisher.Republisher for automatic IPNS republishing
type IPNSRepublisherService struct {
	*core.BaseComponent
	republisher *boxoRepublisher.Republisher
	stopFunc    context.CancelFunc
}

// NewIPNSRepublisherService creates a new IPNS republisher service
// The boxo Republisher automatically handles republishing of all keys in the keystore
// on a 4-hour interval. We just need to start it.
func NewIPNSRepublisherService() (core.Service, []core.ContextBuilderOption, error) {
	svc := &IPNSRepublisherService{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			// Get the IPFS protocol
			proto := core.GetProtocol(internal.ProtocolName)
			if proto == nil {
				return fmt.Errorf("IPFS protocol not found")
			}

			// Get the IPNS node access
			ipnsNode, ok := proto.(pluginCore.IPNSBoxoServices)
			if !ok {
				return fmt.Errorf("IPFS protocol does not implement IPNSBoxoServices")
			}

			node := ipnsNode.GetIPNSNode()
			if node == nil {
				return fmt.Errorf("IPNS node not found")
			}

			publisher := node.GetPublisher()
			if publisher == nil {
				return fmt.Errorf("IPNS publisher not found")
			}

			ds := node.GetDatastore()
			if ds == nil {
				return fmt.Errorf("IPFS datastore not found")
			}

			ks := node.GetKeystore()
			if ks == nil {
				return fmt.Errorf("IPFS keystore not found")
			}

			// Wrap the keystore with SafeRepublisherKeystore to filter out nil keys
			// This prevents the republisher from crashing on corrupted keystore entries
			safeKS := NewSafeRepublisherKeystore(ks, ctx.Logger())

			// Get the node's private key for the republisher's self key
			// The vendor code always tries to republish the self key first
			selfKey := node.GetPrivateKey()
			if selfKey == nil {
				return fmt.Errorf("node private key is nil")
			}

			// Create the boxo republisher with default settings
			// Pass the node's private key as the self key to satisfy vendor code requirements
			repub := boxoRepublisher.NewRepublisher(publisher, ds, selfKey, safeKS)

			// Configure with defaults (4-hour interval, 24-hour record lifetime)
			repub.Interval = boxoRepublisher.DefaultRebroadcastInterval
			repub.RecordLifetime = ipns.DefaultRecordLifetime

			svc.republisher = repub

			return svc.Start()
		}),
		core.ContextWithExitFunc(func(ctx core.Context) error {
			return svc.Stop()
		}),
	)

	return svc, opts, nil
}

func (s *IPNSRepublisherService) ID() string {
	return "ipns_republisher_service"
}

// Start starts the IPNS republisher if not already running
func (s *IPNSRepublisherService) Start() error {
	if s.stopFunc != nil {
		s.Logger().Warn("IPNS republisher is already running")
		return nil
	}

	s.Logger().Info("Starting IPNS republisher",
		zap.Duration("interval", s.republisher.Interval),
		zap.Duration("record_lifetime", s.republisher.RecordLifetime),
	)
	s.stopFunc = s.republisher.Run()
	return nil
}

// Stop stops the IPNS republisher if running
func (s *IPNSRepublisherService) Stop() error {
	if s.stopFunc == nil {
		s.Logger().Warn("IPNS republisher is not running")
		return nil
	}

	s.Logger().Info("Stopping IPNS republisher")
	s.stopFunc()
	s.stopFunc = nil
	return nil
}

// GetInterval returns the current republish interval
func (s *IPNSRepublisherService) GetInterval() time.Duration {
	return s.republisher.Interval
}

// SetInterval updates the republish interval
// Note: This doesn't affect a running republisher - it would need to be restarted
func (s *IPNSRepublisherService) SetInterval(interval time.Duration) {
	s.republisher.Interval = interval
	s.Logger().Info("IPNS republisher interval updated", zap.Duration("interval", interval))
}

// GetRecordLifetime returns the current record lifetime
func (s *IPNSRepublisherService) GetRecordLifetime() time.Duration {
	return s.republisher.RecordLifetime
}

// SetRecordLifetime updates the record lifetime
func (s *IPNSRepublisherService) SetRecordLifetime(lifetime time.Duration) {
	s.republisher.RecordLifetime = lifetime
	s.Logger().Info("IPNS record lifetime updated", zap.Duration("lifetime", lifetime))
}

// IsRunning returns whether the republisher is currently running
func (s *IPNSRepublisherService) IsRunning() bool {
	return s.stopFunc != nil
}
