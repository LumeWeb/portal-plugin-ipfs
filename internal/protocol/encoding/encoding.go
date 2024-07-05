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
