package encoding

import (
	"context"
	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

var encoderRegistry *legacy.Decoder

func init() {
	d := legacy.NewDecoder()
	d.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	d.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)
	encoderRegistry = d
}

func DecodeBlock(ctx context.Context, block blocks.Block) (format.Node, error) {
	return encoderRegistry.DecodeNode(ctx, block)
}

func ToV1(c cid.Cid) cid.Cid {
	switch c.Version() {
	case 0:
		dagPbCode := uint64(cid.DagProtobuf)
		newCid := cid.NewCidV1(dagPbCode, c.Hash())
		return newCid
	case 1:
		// If it's already version 1, return it as is
		return c
	default:
		return cid.Undef
	}
}

// Helper function to normalize CID
func NormalizeCid(c cid.Cid) cid.Cid {
	return ToV1(c)
}
