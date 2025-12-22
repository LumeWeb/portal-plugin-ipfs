package common

import (
	"fmt"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"go.lumeweb.com/portal/core"
)

// ComponentFactory provides common IPFS component creation utilities
type ComponentFactory struct {
	logger *core.Logger
}

// NewComponentFactory creates a new component factory
func NewComponentFactory(logger *core.Logger) *ComponentFactory {
	return &ComponentFactory{
		logger: logger,
	}
}

// DefaultInMemoryComponents creates in-memory DAG service and blockstore
// This consolidates the duplicate component creation logic
func (cf *ComponentFactory) DefaultInMemoryComponents() (format.DAGService, blockstore.Blockstore) {
	// Create default in-memory implementations
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)

	// Create DAG service inline
	dagService := merkledag.NewDAGService(
		blockservice.New(bstore, offline.Exchange(bstore)),
	)

	return dagService, bstore
}

// DefaultInMemoryComponents is a convenience function that creates components without a factory
func DefaultInMemoryComponents() (format.DAGService, blockstore.Blockstore) {
	factory := NewComponentFactory(nil)
	return factory.DefaultInMemoryComponents()
}

// BaseServiceOptions holds common configuration options for upload services
type BaseServiceOptions struct {
	DAGService format.DAGService
	Blockstore blockstore.Blockstore
	Logger     *core.Logger
	MaxLinks   int
	ChunkSize  int64
}

// NewBaseServiceOptions creates default base service options
func NewBaseServiceOptions() *BaseServiceOptions {
	return &BaseServiceOptions{
		MaxLinks:  helpers.DefaultLinksPerBlock,
		ChunkSize: DefaultChunkSize,
	}
}

// DefaultChunkSize represents the default chunk size for file processing (256 KiB)
const DefaultChunkSize = 256 * 1024

// ComponentValidator provides validation utilities for service components
type ComponentValidator struct{}

// NewComponentValidator creates a new component validator
func NewComponentValidator() *ComponentValidator {
	return &ComponentValidator{}
}

// ValidateRequiredComponents checks that required components are present
func (cv *ComponentValidator) ValidateRequiredComponents(dagService format.DAGService, blockstore blockstore.Blockstore, logger *core.Logger) error {
	if dagService == nil {
		return fmt.Errorf("DAGService is required")
	}
	if blockstore == nil {
		return fmt.Errorf("Blockstore is required")
	}
	if logger == nil {
		return fmt.Errorf("Logger is required")
	}
	return nil
}