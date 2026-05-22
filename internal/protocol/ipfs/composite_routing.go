package ipfs

import (
	"context"

	"github.com/libp2p/go-libp2p/core/routing"
)

// compositeValueStore publishes IPNS records to multiple routing backends
// simultaneously (e.g. DHT + PubSub). Writes go to all stores; reads come
// from the primary store.
type compositeValueStore struct {
	primary   routing.ValueStore
	secondary routing.ValueStore
}

var _ routing.ValueStore = (*compositeValueStore)(nil)

func newCompositeValueStore(primary, secondary routing.ValueStore) *compositeValueStore {
	return &compositeValueStore{primary: primary, secondary: secondary}
}

func (c *compositeValueStore) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	errs := make(chan error, 2)

	go func() {
		errs <- c.primary.PutValue(ctx, key, value, opts...)
	}()

	go func() {
		errs <- c.secondary.PutValue(ctx, key, value, opts...)
	}()

	var firstErr error
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
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
