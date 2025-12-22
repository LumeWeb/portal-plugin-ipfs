package upload

import (
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
	"github.com/ipfs/boxo/blockstore"
	format "github.com/ipfs/go-ipld-format"
)

// DefaultInMemoryComponents delegates to the shared implementation in common package
func DefaultInMemoryComponents() (format.DAGService, blockstore.Blockstore) {
	return common.DefaultInMemoryComponents()
}
