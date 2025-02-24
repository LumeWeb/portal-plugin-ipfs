package internal

import (
	"context"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	pb "github.com/ipfs/boxo/ipld/unixfs/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	"github.com/samber/lo"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
)

const (
	// 240 KiB - minimum size to consider as potential chunk
	sizeThreshold = 240 * 1024
	// 256 KiB - standard IPFS chunk size
	typicalChunkSize = 256 * 1024
)

type NodeInfoType string

const (
	NodeTypeRaw      NodeInfoType = "raw"
	NodeTypeProtobuf NodeInfoType = "dag-pb"
	NodeTypeCBOR     NodeInfoType = "cbor"
	NodeTypeUnknown  NodeInfoType = "unknown"
)

type NodeInfo struct {
	Name             string           // Name of the node (e.g., filename within a directory)
	CID              cid.Cid          // CID of the block
	Type             NodeInfoType     // Type of the node (raw, protobuf, cbor, etc.)
	UnixFSType       pb.Data_DataType // UnixFS type (file, directory, symlink, etc.)
	Links            []*format.Link   // Links to child nodes
	IsUnixFS         bool             // Whether the node is a UnixFS node
	IsFileRoot       bool             // Whether the node is the root of a UnixFS file
	BlockSize        uint64           // Raw, encoded size of the block (disk usage)
	DataSize         uint64           // Size of the data within the node (e.g., file size, metadata size)
	UnixFSBlockSizes []uint64         // Block sizes for UnixFS files (chunk sizes)
}

func AnalyzeNode(ctx context.Context, block blocks.Block) (*NodeInfo, error) {
	node, err := encoding.DecodeBlock(ctx, block)
	if err != nil {
		return nil, err
	}

	links := lo.Map(node.Links(), func(link *format.Link, _ int) *format.Link {
		return &format.Link{
			Name: link.Name,
			Size: link.Size,
			Cid:  encoding.NormalizeCid(link.Cid),
		}
	})

	info := &NodeInfo{
		CID:       block.Cid(),
		Links:     links,
		BlockSize: uint64(len(block.RawData())),
	}

	switch n := node.(type) {
	case *merkledag.RawNode:
		info.Type = NodeTypeRaw
		info.DataSize = uint64(len(n.RawData()))
	case *merkledag.ProtoNode:
		info.Type = NodeTypeProtobuf
		data := n.Data()
		info.DataSize = uint64(len(data))

		if fsNode, err := unixfs.FSNodeFromBytes(data); err == nil {
			info.IsUnixFS = true
			info.UnixFSType = fsNode.Type()

			if fsNode.Type() == pb.Data_File {
				info.IsFileRoot = len(info.Links) > 0
				info.UnixFSBlockSizes = fsNode.BlockSizes()
			}
		}
	case *encoding.CBORNode:
		info.Type = NodeTypeCBOR
		info.DataSize = uint64(len(n.Block.RawData()))
	case *legacy.LegacyNode:
		info.Type = NodeTypeUnknown
		info.DataSize = uint64(len(n.Block.RawData()))
	default:
		info.Type = NodeTypeUnknown
	}

	return info, nil
}

// isLikelyChunk determines if a size is characteristic of an IPFS file chunk
func isLikelyChunk(size uint64) bool {
	// Check for sizes in chunking range (240KB <= size < 256KB)
	return size >= sizeThreshold && size < typicalChunkSize
}

func IsPartialFile(info *NodeInfo) bool {
	if info.IsUnixFS && info.UnixFSType == pb.Data_File && !info.IsFileRoot {
		// UnixFS files use the standard chunk size range (240KB <= size < 256KB)
		return isLikelyChunk(info.DataSize)
	}

	// Non-UnixFS raw data: 240KB <= size <= 256KB is considered partial
	if info.Type == NodeTypeRaw {
		return info.DataSize >= sizeThreshold && info.DataSize <= typicalChunkSize
	}

	// Non-UnixFS protobuf data: NEVER considered partial
	return false
}
func DetectPartialFile(ctx context.Context, block blocks.Block) (bool, error) {
	info, err := AnalyzeNode(ctx, block)
	if err != nil {
		return false, err
	}

	return IsPartialFile(info), nil
}
