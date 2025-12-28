package encoding

import (
	"context"

	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	dagpb "github.com/ipld/go-codec-dagpb"
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"go.lumeweb.com/portal/core"
)

// encoderRegistry holds the global decoder registry for IPLD formats
var encoderRegistry *legacy.Decoder

func init() {
	// Initialize a new decoder and register supported codecs
	d := legacy.NewDecoder()

	// Register protobuf codec (dag-pb) for IPLD nodes
	d.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)

	// Register raw codec for raw data blocks
	d.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)
	d.RegisterCodec(cid.DagCBOR, basicnode.Prototype.Any, DagCborNodeConverter)
	encoderRegistry = d
}

// DecodeBlock decodes an IPFS block into an IPLD node using the registered codecs
func DecodeBlock(ctx context.Context, block blocks.Block) (format.Node, error) {
	ctx, span := core.TraceMethod(ctx, "DecodeBlock")
	defer span.End()

	return encoderRegistry.DecodeNode(ctx, block)
}

// ToV1 converts a CID to version 1 format if it's version 0
// Returns CID unchanged if already version 1, or CID.Undef for unsupported versions
func ToV1(c cid.Cid) cid.Cid {
	switch c.Version() {
	case 0:
		newCid := cid.NewCidV1(c.Type(), c.Hash())
		return newCid
	case 1:
		// Already v1 - return as-is
		return c
	default:
		// Unsupported version
		return cid.Undef
	}
}

// NormalizeCid ensures a CID is in version 1 format
// This is used to maintain consistent CID representations across the system
func NormalizeCid(c cid.Cid) cid.Cid {
	return ToV1(c)
}
