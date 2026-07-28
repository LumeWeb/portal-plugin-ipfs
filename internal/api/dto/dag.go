package dto

import (
	"github.com/Oudwins/zog"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/core"
)

var _ httputil.DTOValidator = (*GetDAGRequest)(nil)
var _ httputil.DTORequest[*GetDAGParsedRequest] = (*GetDAGRequest)(nil)
var _ httputil.DTOResponse[*DAGResolution] = (*DAGResponse)(nil)

type GetDAGRequest struct {
	CID string `json:"cid" param:"cid"`
}

func (g GetDAGRequest) ToModel() (*GetDAGParsedRequest, error) {
	_cid, err := cid.Parse(g.CID)
	if err != nil {
		return nil, err
	}

	return &GetDAGParsedRequest{
		CID: _cid,
	}, nil
}

func (g GetDAGRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"CID": cidValidator.Required(),
	})
}

type GetDAGParsedRequest struct {
	CID cid.Cid `json:"cid"`
}

// DAGResolution is the model returned by the protocol layer's ResolveDAG.
type DAGResolution struct {
	RootCID cid.Cid
	Nodes   []core.DAGBlockNode
}

// DAGBlockNodeResponse represents a single block in the resolved DAG.
type DAGBlockNodeResponse struct {
	CID      string   `json:"cid"`
	Size     uint64   `json:"size"`
	Children []string `json:"children"`
}

type DAGResponse struct {
	RootCID string                 `json:"root_cid"`
	Nodes   []DAGBlockNodeResponse `json:"nodes"`
}

func (r *DAGResponse) FromModel(model *DAGResolution) error {
	r.RootCID = encoding.ToV1(model.RootCID).String()
	r.Nodes = make([]DAGBlockNodeResponse, 0, len(model.Nodes))
	for _, node := range model.Nodes {
		children := make([]string, 0, len(node.Children))
		for _, child := range node.Children {
			children = append(children, encoding.ToV1(child).String())
		}
		r.Nodes = append(r.Nodes, DAGBlockNodeResponse{
			CID:      encoding.ToV1(node.CID).String(),
			Size:     node.Size,
			Children: children,
		})
	}
	return nil
}
