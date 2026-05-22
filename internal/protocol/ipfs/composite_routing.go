package ipfs

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/routing"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// compositeValueStore publishes IPNS records to multiple routing backends
// and splits read/write paths for optimal DHT behavior.
//
// In FullRT mode, the primary (FullRT) is excellent for reads but a
// client-only DHT that does not reliably publish records to the network.
// The companion (server-mode IpfsDHT) handles PutValue so records are
// properly propagated. When no companion is provided, PutValue falls back
// to the primary (basic DHT or FullRT without companion).
type compositeValueStore struct {
	primary   routing.ValueStore
	companion routing.ValueStore // server-mode DHT for writes; nil falls back to primary
	secondary routing.ValueStore
	log       *core.Logger
}

var _ routing.ValueStore = (*compositeValueStore)(nil)

func newCompositeValueStore(primary, secondary routing.ValueStore, log *core.Logger) *compositeValueStore {
	return &compositeValueStore{primary: primary, secondary: secondary, log: log}
}

func newCompositeValueStoreWithCompanion(primary, companion, secondary routing.ValueStore, log *core.Logger) *compositeValueStore {
	return &compositeValueStore{primary: primary, companion: companion, secondary: secondary, log: log}
}

// writeTarget returns the routing.ValueStore that PutValue should write to.
// When a companion (server-mode DHT) is available, writes go there instead
// of the primary (which may be a client-only FullRT).
func (c *compositeValueStore) writeTarget() routing.ValueStore {
	if c.companion != nil {
		return c.companion
	}
	return c.primary
}

func (c *compositeValueStore) writeTargetName() string {
	if c.companion != nil {
		return "companion"
	}
	return "primary"
}

func (c *compositeValueStore) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	target := c.writeTarget()
	targetName := c.writeTargetName()

	type putResult struct {
		err error
	}
	dhtCh := make(chan putResult, 1)
	pubsubCh := make(chan putResult, 1)

	go func() {
		dhtCh <- putResult{err: target.PutValue(ctx, key, value, opts...)}
	}()

	go func() {
		pubsubCh <- putResult{err: c.secondary.PutValue(ctx, key, value, opts...)}
	}()

	dhtRes := <-dhtCh
	pubsubRes := <-pubsubCh

	if dhtRes.err != nil && pubsubRes.err != nil {
		c.debug("PutValue failed on both backends",
			zap.String("key", key),
			zap.String("write_target", targetName),
			zap.Error(dhtRes.err),
			zap.NamedError("pubsub_err", pubsubRes.err),
		)
		return fmt.Errorf("%s: %w; pubsub: %v", targetName, dhtRes.err, pubsubRes.err)
	}

	if dhtRes.err != nil {
		c.debug("PutValue failed on DHT backend",
			zap.String("key", key),
			zap.String("write_target", targetName),
			zap.Error(dhtRes.err),
		)
		return dhtRes.err
	}

	if pubsubRes.err != nil {
		c.debug("PutValue succeeded on DHT, failed on PubSub",
			zap.String("key", key),
			zap.String("write_target", targetName),
			zap.Error(pubsubRes.err),
		)
		return nil
	}

	c.debug("PutValue succeeded on both backends",
		zap.String("key", key),
		zap.String("write_target", targetName),
	)

	return nil
}

func (c *compositeValueStore) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	return c.primary.GetValue(ctx, key, opts...)
}

func (c *compositeValueStore) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	secCh, secErr := c.secondary.SearchValue(ctx, key, opts...)
	priCh, priErr := c.primary.SearchValue(ctx, key, opts...)

	if secErr != nil && priErr != nil {
		return nil, secErr
	}
	if secErr != nil {
		return priCh, priErr
	}
	if priErr != nil {
		return secCh, nil
	}

	merged := make(chan []byte)
	go func() {
		defer close(merged)
		openCount := 2
		for openCount > 0 {
			select {
			case v, ok := <-secCh:
				if !ok {
					secCh = nil
					openCount--
					continue
				}
				select {
				case merged <- v:
				case <-ctx.Done():
					return
				}
			case v, ok := <-priCh:
				if !ok {
					priCh = nil
					openCount--
					continue
				}
				select {
				case merged <- v:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return merged, nil
}

func (c *compositeValueStore) debug(msg string, fields ...zap.Field) {
	if c.log != nil {
		c.log.Debug(msg, fields...)
	}
}
