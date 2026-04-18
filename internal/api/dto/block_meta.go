package dto

import (
	"github.com/Oudwins/zog"
	"github.com/Oudwins/zog/pkgs/internals"
	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	"go.lumeweb.com/httputil"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
)

// UnixFSType represents the type of a UnixFS node (content type within an IPLD block).
//
// UnixFS is a data model used by IPFS to represent files and directories on top of IPLD.
// This enum describes the semantic type of content stored in a UnixFS node, which is
// independent from the block's encoding format (e.g., dag-pb, cbor, raw).
//
// Type Mappings:
//   - UnixFSTypeDirectory (1): Container nodes that hold links to child nodes
//   - UnixFSTypeFile (2): Regular file nodes (may have data or links to chunk blocks)
//   - UnixFSTypeSymlink (4): Symbolic link nodes containing a target path string
//   - UnixFSTypeHAMTShard (5): HAMT (Hash Array Mapped Trie) shard nodes for large directories
//
// Note: Values 3, 6+ are reserved or unused in the UnixFS specification.
//
// Related Types:
//   - NodeInfoType (internal/node.go): The encoding/codec type of the IPLD block
//   - UnixFSType should not be confused with block encoding types (raw, dag-pb, cbor)
type UnixFSType uint8

const (
	// UnixFSTypeDirectory represents a UnixFS directory node.
	//
	// Directory nodes contain only links to child nodes (subdirectories and files).
	// They do not contain inline data. The directory's "size" is typically 0 or reflects
	// only the size of the node structure itself, not the cumulative size of children.
	//
	// Value: 1
	UnixFSTypeDirectory UnixFSType = 1

	// UnixFSTypeFile represents a UnixFS regular file node.
	//
	// File nodes can either:
	//   - Contain inline data (small files that fit in one block)
	//   - Contain links to data chunk blocks (large files split across multiple blocks)
	//
	// For large files with children, the UnixFSTypeFile node contains block size information
	// in its BlockSize field that represents the cumulative size of all chunks.
	//
	// Value: 2
	UnixFSTypeFile UnixFSType = 2

	// UnixFSTypeSymlink represents a UnixFS symbolic link node.
	//
	// Symlink nodes contain a target path as inline data. The "data" field stores
	// the path that the symlink points to. Symlinks are resolved during file system
	// operations, not during IPFS retrieval.
	//
	// Value: 4
	UnixFSTypeSymlink UnixFSType = 4

	// UnixFSTypeHAMTShard represents a HAMT (Hash Array Mapped Trie) shard node.
	//
	// HAMT shards are used for extremely large directories that would exceed the block
	// size limit if stored as a single directory node. HAMT uses a trie structure to
	// efficiently distribute child entries across multiple blocks with O(log n) lookup.
	//
	// HAMT is implemented using UnixFS shard nodes and is typically transparent to users -
	// the directory appears as a normal directory to traversals.
	//
	// Value: 5
	UnixFSTypeHAMTShard UnixFSType = 5
)

// String returns a human-readable representation of the UnixFS type.
func (t UnixFSType) String() string {
	switch t {
	case UnixFSTypeDirectory:
		return "directory"
	case UnixFSTypeFile:
		return "file"
	case UnixFSTypeSymlink:
		return "symlink"
	case UnixFSTypeHAMTShard:
		return "hamt-shard"
	default:
		return "unknown"
	}
}

// IsDirectory returns true if the node is a directory or HAMT shard (both are directory-like).
func (t UnixFSType) IsDirectory() bool {
	return t == UnixFSTypeDirectory || t == UnixFSTypeHAMTShard
}

// IsFile returns true if the node is a regular file.
func (t UnixFSType) IsFile() bool {
	return t == UnixFSTypeFile
}

// IsSymlink returns true if the node is a symbolic link.
func (t UnixFSType) IsSymlink() bool {
	return t == UnixFSTypeSymlink
}

// IsValid returns true if the unixfs type is a known valid type.
func (t UnixFSType) IsValid() bool {
	switch t {
	case UnixFSTypeDirectory, UnixFSTypeFile, UnixFSTypeSymlink, UnixFSTypeHAMTShard:
		return true
	default:
		return false
	}
}

// ToUint8 returns the type as a uint8, suitable for database storage.
func (t UnixFSType) ToUint8() uint8 {
	return uint8(t)
}

// FromUint8 converts a uint8 from the database to a UnixFSType.
// Returns UnixFSType(value) even for invalid values - caller should check IsValid().
func FromUint8(value uint8) UnixFSType {
	return UnixFSType(value)
}

// GetBlockMetaBatchRequest and GetBlockMetaBatchResponse
var _ httputil.DTOValidator = (*GetBlockMetaBatchRequest)(nil)
var _ httputil.DTOValidator = (*GetBlockMetaRequest)(nil)
var _ httputil.DTORequest[*GetBlockMetaBatchParsedRequest] = (*GetBlockMetaBatchRequest)(nil)
var _ httputil.DTORequest[*GetBlockMetaParsedRequest] = (*GetBlockMetaRequest)(nil)
var _ httputil.DTORequest[*GetBlockMetaBatchParsedRequest] = (*GetBlockMetaBatchRequest)(nil)
var _ httputil.DTOResponse[map[string]*pluginDb.UnixFSNode] = (*GetBlockMetaBatchResponse)(nil)
var _ httputil.DTOResponse[*pluginDb.UnixFSNode] = (*BlockMetaResponse)(nil)

type BlockMap = map[string]*BlockMetaResponse

var (
	cidValidator = zog.String().TestFunc(func(val *string, ctx internals.Ctx) bool {
		_, err := cid.Parse(*val)
		if err != nil {
			return false
		}

		return true
	})
)

type GetBlockMetaRequest struct {
	CID string `json:"cid" param:"cid"`
}

func (g GetBlockMetaRequest) ToModel() (*GetBlockMetaParsedRequest, error) {
	_cid, err := cid.Parse(g.CID)
	if err != nil {
		return nil, err
	}

	return &GetBlockMetaParsedRequest{
		CID: _cid,
	}, nil
}

func (g GetBlockMetaRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"CID": cidValidator.Required(),
	})
}

type GetBlockMetaParsedRequest struct {
	CID cid.Cid `json:"cid"`
}

type GetBlockMetaBatchRequest struct {
	CID []string `json:"cid"`
}

type GetBlockMetaBatchParsedRequest struct {
	CID []cid.Cid `json:"cid"`
}

func (g GetBlockMetaBatchRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"CID": zog.Slice(cidValidator).Required(),
	})
}

func (g *GetBlockMetaBatchRequest) ToModel() (*GetBlockMetaBatchParsedRequest, error) {
	parsed := &GetBlockMetaBatchParsedRequest{}

	for _, item := range g.CID {
		_cid, err := cid.Parse(item)
		if err != nil {
			return nil, err
		}

		parsed.CID = append(parsed.CID, _cid)
	}

	return parsed, nil
}

type BlockMetaResponse struct {
	Name       string     `json:"name"`
	Type       UnixFSType `json:"type"`
	BlockSize  uint64     `json:"block_size"`  // Raw encoded block size (includes protobuf framing overhead)
	UnixFSSize int64      `json:"unixfs_size"` // Logical UnixFS file size (local size, original file size before chunking)
	ChildCID   []string   `json:"child_cid"`
}

func (b *BlockMetaResponse) FromModel(model *pluginDb.UnixFSNode) error {
	b.Name = model.Name
	b.Type = UnixFSType(model.Type) // Convert uint8 to UnixFSType enum
	b.BlockSize = model.Block.Size  // Raw encoded block size
	b.UnixFSSize = model.BlockSize  // Logical UnixFS file size
	b.ChildCID = lo.Map(model.ChildCID, func(c cid.Cid, _ int) string {
		return encoding.ToV1(c).String()
	})

	return nil
}

type GetBlockMetaBatchResponse map[string]*BlockMetaResponse

func (g *GetBlockMetaBatchResponse) FromModel(model map[string]*pluginDb.UnixFSNode) error {
	*g = make(GetBlockMetaBatchResponse)
	for _cid, node := range model {
		(*g)[_cid] = &BlockMetaResponse{
			Name:       node.Name,
			Type:       UnixFSType(node.Type), // Convert uint8 to UnixFSType enum
			BlockSize:  node.Block.Size,       // Raw encoded block size
			UnixFSSize: node.BlockSize,        // Logical UnixFS file size
			ChildCID: lo.Map(node.ChildCID, func(c cid.Cid, _ int) string {
				return encoding.ToV1(c).String()
			}),
		}
	}
	return nil
}
