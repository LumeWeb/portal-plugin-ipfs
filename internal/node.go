package internal

import (
	"context"
	"fmt"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	pb "github.com/ipfs/boxo/ipld/unixfs/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
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
	Name       string           // Name of node (e.g., filename within a directory)
	CIDBytes   []byte           // Binary CID representation (more memory efficient)
	Type       NodeInfoType     // Type of node (raw, protobuf, cbor, etc.)
	UnixFSType pb.Data_DataType // UnixFS type (file, directory, symlink, etc.)
	LinkCIDs   [][]byte         // Binary CIDs of child nodes (more compact than Link structs)
	LinkNames  []string         // Names of child links (parallel array with LinkCIDs)
	LinkSizes  []uint64         // Sizes of child links (parallel array with LinkCIDs)
	IsUnixFS   bool             // Whether the node is a UnixFS node
	IsFileRoot bool             // Whether the node is the root of a UnixFS file
	BlockSize  uint64           // Raw, encoded size of block (disk usage)
	DataSize   uint64           // Size of the data within the node (e.g., file size, metadata size)
	ChunkSizes []uint64         // Block sizes for UnixFS files (chunk sizes) - renamed for clarity
}

func AnalyzeNode(ctx context.Context, block blocks.Block) (*NodeInfo, error) {
	node, err := encoding.DecodeBlock(ctx, block)
	if err != nil {
		return nil, err
	}

	links := node.Links()
	linkCount := len(links)

	// Pre-allocate slices to avoid multiple allocations
	info := &NodeInfo{
		CIDBytes:  encoding.NormalizeCid(block.Cid()).Bytes(),
		LinkCIDs:  make([][]byte, 0, linkCount),
		LinkNames: make([]string, 0, linkCount),
		LinkSizes: make([]uint64, 0, linkCount),
		BlockSize: uint64(len(block.RawData())),
	}

	// Extract link data efficiently
	for _, link := range links {
		if link != nil {
			info.LinkCIDs = append(info.LinkCIDs, encoding.NormalizeCid(link.Cid).Bytes())
			info.LinkNames = append(info.LinkNames, link.Name)
			info.LinkSizes = append(info.LinkSizes, link.Size)
		}
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
				info.IsFileRoot = len(info.LinkCIDs) > 0
				blockSizes := fsNode.BlockSizes()
				if len(blockSizes) > 0 {
					info.ChunkSizes = make([]uint64, len(blockSizes))
					copy(info.ChunkSizes, blockSizes)
				}
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
		// UnixFS files use standard chunk size range (240KB <= size < 256KB)
		return isLikelyChunk(info.DataSize)
	}

	// Non-UnixFS raw data: 240KB <= size <= 256KB is considered partial
	if info.Type == NodeTypeRaw {
		return info.DataSize >= sizeThreshold && info.DataSize <= typicalChunkSize
	}

	// Non-UnixFS protobuf data: NEVER considered partial
	return false
}

// GetCID returns the CID from its binary representation
func (info *NodeInfo) GetCID() (cid.Cid, error) {
	return cid.Cast(info.CIDBytes)
}

// GetLinkAt returns the link at the specified index as a format.Link
func (info *NodeInfo) GetLinkAt(index int) (*format.Link, error) {
	if index < 0 || index >= len(info.LinkCIDs) {
		return nil, fmt.Errorf("link index out of bounds")
	}

	linkCID, err := cid.Cast(info.LinkCIDs[index])
	if err != nil {
		return nil, fmt.Errorf("invalid CID at index %d: %w", index, err)
	}

	return &format.Link{
		Name: info.LinkNames[index],
		Size: info.LinkSizes[index],
		Cid:  linkCID,
	}, nil
}

// LinkCount returns the number of links in this node
func (info *NodeInfo) LinkCount() int {
	return len(info.LinkCIDs)
}

func isPartialData(info *NodeInfo, sizeThreshold, typicalChunkSize uint64) bool {
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
