package encoding

import (
	blocks "github.com/ipfs/go-block-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	"github.com/ipld/go-ipld-prime"
)

// CBORNode is a wrapper around LegacyNode that uniquely identifies CBOR-encoded nodes
type CBORNode struct {
	legacy.LegacyNode
}

// IsCBORNode returns true if the node is a CBORNode wrapper
func IsCBORNode(node legacy.UniversalNode) bool {
	_, ok := node.(*CBORNode)
	return ok
}

// DagCborNodeConverter converts a go-ipld-prime node + block combination to a CBORNode
// that satisfies both current and legacy ipld formats for DAG-CBOR.
func DagCborNodeConverter(b blocks.Block, node ipld.Node) (legacy.UniversalNode, error) {
	return &CBORNode{legacy.LegacyNode{b, node}}, nil
}
