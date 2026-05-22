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
	ch, err := c.secondary.SearchValue(ctx, key, opts...)
	if err == nil {
		return ch, nil
	}
	return c.primary.SearchValue(ctx, key, opts...)
}
