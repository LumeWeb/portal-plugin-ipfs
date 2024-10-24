package internal

import (
	"context"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	pb "github.com/ipfs/boxo/ipld/unixfs/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/samber/lo"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
)

const typicalChunkSize = 256 * 1024
const sizeThreshold = 240 * 1024

type NodeInfoType string

const (
	NodeTypeRaw      NodeInfoType = "raw"
	NodeTypeProtobuf NodeInfoType = "protobuf"
	NodeTypeUnknown  NodeInfoType = "unknown"
)

type NodeInfo struct {
	Name             string
	CID              cid.Cid
	Type             NodeInfoType
	UnixFSType       pb.Data_DataType
	Size             uint64
	Links            []*format.Link
	IsUnixFS         bool
	IsFileRoot       bool
	DataSize         uint64
	UnixFSBlockSizes []uint64
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
		CID:   block.Cid(),
		Links: links,
	}

	switch n := node.(type) {
	case *merkledag.RawNode:
		info.Type = NodeTypeRaw
		info.Size = uint64(len(n.RawData()))
		info.DataSize = info.Size
	case *merkledag.ProtoNode:
		info.Type = NodeTypeProtobuf
		fsNode, err := unixfs.FSNodeFromBytes(n.Data())
		if err == nil {
			info.IsUnixFS = true
			info.UnixFSType = fsNode.Type()
			info.Size = fsNode.FileSize()
			info.DataSize = uint64(len(fsNode.Data()))
			info.IsFileRoot = fsNode.Type() == unixfs.TFile && len(info.Links) > 0
			info.UnixFSBlockSizes = fsNode.BlockSizes()
		} else {
			// Handle non-UnixFS ProtoNodes
			info.Size = uint64(len(n.Data()))
			info.DataSize = info.Size
		}
	default:
		info.Type = NodeTypeUnknown
	}

	return info, nil
}

func isLikelyChunk(size uint64) bool {
	return size >= sizeThreshold && size <= typicalChunkSize
}

func IsPartialFile(info *NodeInfo) bool {
	if info.IsUnixFS {
		if info.UnixFSType != pb.Data_File {
			return false // Only consider File type
		}
		if info.IsFileRoot {
			return false // File roots represent complete files, even if they have chunks
		}
		// For UnixFS file chunks, use size heuristic
		return isLikelyChunk(info.Size)
	}

	// For non-UnixFS nodes (including raw), use size heuristic
	return isLikelyChunk(info.Size)
}

func DetectPartialFile(ctx context.Context, block blocks.Block) (bool, error) {
	info, err := AnalyzeNode(ctx, block)
	if err != nil {
		return false, err
	}

	return IsPartialFile(info), nil
}
