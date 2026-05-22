package ipfs

import (
	"context"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/routing"
)

type mockValueStore struct {
	putValueFn    func(ctx context.Context, key string, value []byte, opts ...routing.Option) error
	getValueFn    func(ctx context.Context, key string, opts ...routing.Option) ([]byte, error)
	searchValueFn func(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error)
}

var _ routing.ValueStore = (*mockValueStore)(nil)

func (m *mockValueStore) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	if m.putValueFn != nil {
		return m.putValueFn(ctx, key, value, opts...)
	}
	return nil
}

func (m *mockValueStore) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	if m.getValueFn != nil {
		return m.getValueFn(ctx, key, opts...)
	}
	return nil, nil
}

func (m *mockValueStore) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	if m.searchValueFn != nil {
		return m.searchValueFn(ctx, key, opts...)
	}
	ch := make(chan []byte)
	close(ch)
	return ch, nil
}

func TestCompositeValueStore_PutValue_WritesToBoth(t *testing.T) {
	var primaryCalled, secondaryCalled bool

	primary := &mockValueStore{
		putValueFn: func(_ context.Context, key string, value []byte, _ ...routing.Option) error {
			primaryCalled = true
			if key != "test-key" {
				t.Errorf("primary got key %q, want %q", key, "test-key")
			}
			if string(value) != "test-value" {
				t.Errorf("primary got value %q, want %q", value, "test-value")
			}
			return nil
		},
	}

	secondary := &mockValueStore{
		putValueFn: func(_ context.Context, key string, value []byte, _ ...routing.Option) error {
			secondaryCalled = true
			if key != "test-key" {
				t.Errorf("secondary got key %q, want %q", key, "test-key")
			}
			if string(value) != "test-value" {
				t.Errorf("secondary got value %q, want %q", value, "test-value")
			}
			return nil
		},
	}

	c := newCompositeValueStore(primary, secondary)
	err := c.PutValue(context.Background(), "test-key", []byte("test-value"))
	if err != nil {
		t.Fatalf("PutValue returned error: %v", err)
	}

	if !primaryCalled {
		t.Error("primary PutValue was not called")
	}
	if !secondaryCalled {
		t.Error("secondary PutValue was not called")
	}
}

func TestCompositeValueStore_PutValue_PropagatesFirstError(t *testing.T) {
	testErr := errors.New("primary failed")

	primary := &mockValueStore{
		putValueFn: func(_ context.Context, _ string, _ []byte, _ ...routing.Option) error {
			return testErr
		},
	}

	secondary := &mockValueStore{}

	c := newCompositeValueStore(primary, secondary)
	err := c.PutValue(context.Background(), "key", []byte("value"))
	if err != testErr {
		t.Fatalf("got error %v, want %v", err, testErr)
	}
}

func TestCompositeValueStore_PutValue_SecondaryErrorOnly(t *testing.T) {
	testErr := errors.New("secondary failed")

	primary := &mockValueStore{}

	secondary := &mockValueStore{
		putValueFn: func(_ context.Context, _ string, _ []byte, _ ...routing.Option) error {
			return testErr
		},
	}

	c := newCompositeValueStore(primary, secondary)
	err := c.PutValue(context.Background(), "key", []byte("value"))
	if err != testErr {
		t.Fatalf("got error %v, want %v", err, testErr)
	}
}

func TestCompositeValueStore_PutValue_BothFail(t *testing.T) {
	err1 := errors.New("primary failed")
	err2 := errors.New("secondary failed")

	primary := &mockValueStore{
		putValueFn: func(_ context.Context, _ string, _ []byte, _ ...routing.Option) error {
			return err1
		},
	}

	secondary := &mockValueStore{
		putValueFn: func(_ context.Context, _ string, _ []byte, _ ...routing.Option) error {
			return err2
		},
	}

	c := newCompositeValueStore(primary, secondary)
	err := c.PutValue(context.Background(), "key", []byte("value"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err != err1 && err != err2 {
		t.Fatalf("got error %v, want either %v or %v", err, err1, err2)
	}
}

func TestCompositeValueStore_GetValue_UsesPrimary(t *testing.T) {
	primary := &mockValueStore{
		getValueFn: func(_ context.Context, key string, _ ...routing.Option) ([]byte, error) {
			return []byte("from-primary"), nil
		},
	}

	secondary := &mockValueStore{
		getValueFn: func(_ context.Context, key string, _ ...routing.Option) ([]byte, error) {
			return []byte("from-secondary"), nil
		},
	}

	c := newCompositeValueStore(primary, secondary)
	val, err := c.GetValue(context.Background(), "key")
	if err != nil {
		t.Fatalf("GetValue returned error: %v", err)
	}
	if string(val) != "from-primary" {
		t.Fatalf("got value %q, want %q", val, "from-primary")
	}
}

func TestCompositeValueStore_SearchValue_PrefersSecondary(t *testing.T) {
	secondaryCh := make(chan []byte, 1)
	secondaryCh <- []byte("from-secondary")

	secondary := &mockValueStore{
		searchValueFn: func(_ context.Context, _ string, _ ...routing.Option) (<-chan []byte, error) {
			return secondaryCh, nil
		},
	}

	primary := &mockValueStore{
		searchValueFn: func(_ context.Context, _ string, _ ...routing.Option) (<-chan []byte, error) {
			ch := make(chan []byte, 1)
			ch <- []byte("from-primary")
			return ch, nil
		},
	}

	c := newCompositeValueStore(primary, secondary)
	ch, err := c.SearchValue(context.Background(), "key")
	if err != nil {
		t.Fatalf("SearchValue returned error: %v", err)
	}
	val := <-ch
	if string(val) != "from-secondary" {
		t.Fatalf("got value %q, want %q", val, "from-secondary")
	}
}

func TestCompositeValueStore_SearchValue_FallsBackToPrimary(t *testing.T) {
	searchErr := errors.New("not found in pubsub")

	secondary := &mockValueStore{
		searchValueFn: func(_ context.Context, _ string, _ ...routing.Option) (<-chan []byte, error) {
			return nil, searchErr
		},
	}

	primaryCh := make(chan []byte, 1)
	primaryCh <- []byte("from-primary")

	primary := &mockValueStore{
		searchValueFn: func(_ context.Context, _ string, _ ...routing.Option) (<-chan []byte, error) {
			return primaryCh, nil
		},
	}

	c := newCompositeValueStore(primary, secondary)
	ch, err := c.SearchValue(context.Background(), "key")
	if err != nil {
		t.Fatalf("SearchValue returned error: %v", err)
	}
	val := <-ch
	if string(val) != "from-primary" {
		t.Fatalf("got value %q, want %q", val, "from-primary")
	}
}
