package upload

import (
	"github.com/ipfs/boxo/blockstore"
	format "github.com/ipfs/go-ipld-format"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
)

// DefaultInMemoryComponents delegates to the shared implementation in common package
func DefaultInMemoryComponents() (format.DAGService, blockstore.Blockstore) {
	return common.DefaultInMemoryComponents()
}
