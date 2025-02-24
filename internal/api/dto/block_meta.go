package dto

import (
	"github.com/Oudwins/zog"
	"github.com/Oudwins/zog/internals"
	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	"go.lumeweb.com/httputil"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
)

// GetBlockMetaBatchRequest and GetBlockMetaBatchResponse
var _ httputil.DTOValidator = (*GetBlockMetaBatchRequest)(nil)
var _ httputil.DTOValidator = (*GetBlockMetaRequest)(nil)
var _ httputil.DTORequest[*GetBlockMetaBatchParsedRequest] = (*GetBlockMetaBatchRequest)(nil)
var _ httputil.DTORequest[*GetBlockMetaParsedRequest] = (*GetBlockMetaRequest)(nil)
var _ httputil.DTORequest[*GetBlockMetaBatchParsedRequest] = (*GetBlockMetaBatchRequest)(nil)
var _ httputil.DTOResponse[map[string]*pluginDb.UnixFSNode] = (*GetBlockMetaBatchResponse)(nil)
var _ httputil.DTOResponse[*pluginDb.UnixFSNode] = (*BlockMetaResponse)(nil)

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
	Name      string   `json:"name"`
	Type      uint8    `json:"type"`
	BlockSize int64    `json:"block_size"`
	ChildCID  []string `json:"child_cid"`
}

func (b *BlockMetaResponse) FromModel(model *pluginDb.UnixFSNode) error {
	b.Name = model.Name
	b.Type = model.Type
	b.BlockSize = model.BlockSize
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
			Name:      node.Name,
			Type:      node.Type,
			BlockSize: node.BlockSize,
			ChildCID: lo.Map(node.ChildCID, func(c cid.Cid, _ int) string {
				return encoding.ToV1(c).String()
			}),
		}
	}
	return nil
}
