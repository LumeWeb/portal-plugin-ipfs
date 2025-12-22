package upload

import (
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
)

// DefaultInMemoryComponents creates default in-memory implementations for testing and development
func DefaultInMemoryComponents() (format.DAGService, blockstore.Blockstore) {
	// Create default in-memory implementations
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)

	// Create DAG service inline
	dagService := merkledag.NewDAGService(
		blockservice.New(bstore, offline.Exchange(bstore)),
	)

	return dagService, bstore
}
