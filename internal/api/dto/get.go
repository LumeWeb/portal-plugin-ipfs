package dto

import (
	"fmt"
	"github.com/Oudwins/zog"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/httputil"
)

var _ httputil.DTOValidator = (*IPFSRequest)(nil)
var _ httputil.DTORequest[*IPFSRequestDecoded] = (*IPFSRequest)(nil)

// IPFSRequest represents the query parameters for IPFS requests
type IPFSRequest struct {
	CID         string `json:"cid" param:"cid"`
	Format      string `json:"format,omitempty" param:"format"`
	Filename    string `json:"filename,omitempty" param:"filename"`
	Download    bool   `json:"download,omitempty" param:"download"`
	DagScope    string `json:"dag-scope,omitempty" param:"dag-scope"`
	EntityBytes string `json:"entity-bytes,omitempty" param:"entity-bytes"`
	CarVersion  int    `json:"car-version,omitempty" param:"car-version"`
	CarOrder    string `json:"car-order,omitempty" param:"car-order"`
	CarDups     string `json:"car-dups,omitempty" param:"car-dups"`
}

func (I *IPFSRequest) ToModel() (*IPFSRequestDecoded, error) {
	parsedCID, err := cid.Parse(I.CID)
	if err != nil {
		return nil, fmt.Errorf("invalid CID: %w", err)
	}
	return &IPFSRequestDecoded{
		IPFSRequest: *I,
		CID:         parsedCID,
	}, nil
}

func (I IPFSRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"CID":         cidValidator,
		"Format":      zog.String().Default("raw"),
		"Filename":    zog.String(),
		"Download":    zog.Bool(),
		"DagScope":    zog.String().Default("all"),
		"EntityBytes": zog.String(),
		"CarVersion":  zog.Int(),
		"CarOrder":    zog.String(),
		"CarDups":     zog.String(),
	})
}

type IPFSRequestDecoded struct {
	IPFSRequest
	CID cid.Cid `json:"cid"`
}
