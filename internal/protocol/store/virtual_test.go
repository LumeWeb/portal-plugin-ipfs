package store

import (
	"errors"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"testing"
	"time"
)

var (
	cid1   = cid.MustParse("QmR56UJmAa4F1cUnQvejAQzFGD8Jpw9mKbxvjWvTVwcj6U")
	block1 = blocks.NewBlock([]byte("block1 data"))
)

func setupVirtualTest(tb coreTesting.TB, ctx coreTesting.TestContext, c cid.Cid, keys int) (*VirtualBlockStore, *mocks.MockMockBlockstore) {
	mockDirectBS := mocks.NewMockMockBlockstore(tb)
	virtualBS, err := NewVirtualBlockStore(ctx, mockDirectBS, blockstore.DefaultCacheOpts())
	require.NoError(tb, err)

	keysChan := make(chan cid.Cid, 1)
	keysChan <- c
	close(keysChan)
	mockDirectBS.EXPECT().AllKeysChan(mock.Anything).Return(keysChan, nil).Times(keys)

	tb.Cleanup(func() {
		time.Sleep(100 * time.Millisecond)
	})

	return virtualBS, mockDirectBS
}

func TestVirtualBlockStoreGetVirtualReadEnabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)

		// Use Any() to match any context type
		mockDirectBS.EXPECT().Get(mock.Anything, cid1).Return(block1, nil).Once()

		readCtx := VirtualReadOption(ctx, true)
		block, err := virtualBS.Get(readCtx, cid1)

		assert.NoError(tb, err)
		assert.Equal(tb, block1, block)
	})
}

func TestVirtualBlockStoreGetVirtualReadDisabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)

		mockDirectBS.EXPECT().Get(mock.Anything, cid1).Return(block1, nil).Once()

		block, err := virtualBS.Get(ctx, cid1)

		assert.NoError(tb, err)
		assert.Equal(tb, block1, block)
	})
}

func TestVirtualBlockStoreHasVirtualReadEnabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)

		mockDirectBS.EXPECT().Has(mock.Anything, cid1).Return(true, nil).Once()

		readCtx := VirtualReadOption(ctx, true)
		has, err := virtualBS.Has(readCtx, cid1)

		assert.NoError(tb, err)
		assert.True(tb, has)
	})
}

func TestVirtualBlockStoreHasVirtualReadDisabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)

		mockDirectBS.EXPECT().Has(mock.Anything, cid1).Return(true, nil).Once()

		has, err := virtualBS.Has(ctx, cid1)

		assert.NoError(tb, err)
		assert.True(tb, has)
	})
}

func TestVirtualBlockStorePutVirtualReadEnabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)
		mockDirectBS.EXPECT().AllKeysChan(mock.Anything).Return((<-chan cid.Cid)(make(chan cid.Cid)), nil).Maybe()

		mockDirectBS.EXPECT().Put(mock.Anything, block1).Return(nil).Once()

		readCtx := VirtualReadOption(ctx, true)
		err := virtualBS.Put(readCtx, block1)

		assert.NoError(tb, err)
	})
}

func TestVirtualBlockStorePutVirtualReadDisabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)
		mockDirectBS.EXPECT().AllKeysChan(mock.Anything).Return((<-chan cid.Cid)(make(chan cid.Cid)), nil).Maybe()

		mockDirectBS.EXPECT().Put(ctx, block1).Return(nil).Once()

		err := virtualBS.Put(ctx, block1)

		assert.NoError(tb, err)
	})
}

func TestVirtualBlockStoreDeleteBlockVirtualReadEnabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)

		mockDirectBS.EXPECT().DeleteBlock(mock.Anything, cid1).Return(nil).Once()

		readCtx := VirtualReadOption(ctx, true)
		err := virtualBS.DeleteBlock(readCtx, cid1)

		assert.NoError(tb, err)
	})
}

func TestVirtualBlockStoreDeleteBlockVirtualReadDisabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)

		mockDirectBS.EXPECT().DeleteBlock(ctx, cid1).Return(nil).Once()

		err := virtualBS.DeleteBlock(ctx, cid1)

		assert.NoError(tb, err)
	})
}

func TestVirtualBlockStoreGetSizeVirtualReadEnabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)

		mockDirectBS.EXPECT().GetSize(mock.Anything, cid1).Return(100, nil).Once()

		readCtx := VirtualReadOption(ctx, true)
		size, err := virtualBS.GetSize(readCtx, cid1)

		assert.NoError(tb, err)
		assert.Equal(tb, 100, size)
	})
}

func TestVirtualBlockStoreGetSizeVirtualReadDisabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)

		mockDirectBS.EXPECT().GetSize(ctx, cid1).Return(100, nil).Once()

		size, err := virtualBS.GetSize(ctx, cid1)

		assert.NoError(tb, err)
		assert.Equal(tb, 100, size)
	})
}

func TestVirtualBlockStorePutManyVirtualReadEnabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)
		mockDirectBS.EXPECT().AllKeysChan(mock.Anything).Return((<-chan cid.Cid)(make(chan cid.Cid)), nil).Maybe()

		_blocks := []blocks.Block{block1}
		mockDirectBS.EXPECT().PutMany(mock.Anything, _blocks).Return(nil).Once()

		readCtx := VirtualReadOption(ctx, true)
		err := virtualBS.PutMany(readCtx, _blocks)

		assert.NoError(tb, err)
	})
}

func TestVirtualBlockStorePutManyVirtualReadDisabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)
		mockDirectBS.EXPECT().AllKeysChan(mock.Anything).Return((<-chan cid.Cid)(make(chan cid.Cid)), nil).Maybe()

		_blocks := []blocks.Block{block1}
		mockDirectBS.EXPECT().PutMany(ctx, _blocks).Return(nil).Once()

		err := virtualBS.PutMany(ctx, _blocks)

		assert.NoError(tb, err)
	})
}

func TestVirtualBlockStoreAllKeysChanVirtualReadEnabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, _ := setupVirtualTest(tb, ctx, cid1, 2)

		readCtx := VirtualReadOption(ctx, true)
		ch, err := virtualBS.AllKeysChan(readCtx)

		assert.NoError(tb, err)
		assert.Equal(tb, cid1, <-ch)
	})
}

func TestVirtualBlockStoreAllKeysChanVirtualReadDisabled(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, _ := setupVirtualTest(tb, ctx, cid1, 0)

		ch, err := virtualBS.AllKeysChan(ctx)

		assert.NoError(tb, err)
		assert.Equal(tb, cid1, <-ch)
	})
}

func TestVirtualBlockStoreErrorFromDirectBlockstore(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 0)

		expectedErr := errors.New("direct blockstore error")
		mockDirectBS.EXPECT().Get(mock.Anything, cid1).Return(nil, expectedErr).Once()

		readCtx := VirtualReadOption(ctx, true)
		_, err := virtualBS.Get(readCtx, cid1)

		assert.Error(tb, err)
		assert.Equal(tb, expectedErr, err)
	})
}

func TestVirtualBlockStoreHashOnRead(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		virtualBS, mockDirectBS := setupVirtualTest(tb, ctx, cid1, 1)

		mockDirectBS.EXPECT().HashOnRead(true).Times(2)

		virtualBS.HashOnRead(true)
	})
}
