package dto

import (
	"encoding/json"
	"fmt"
	"github.com/Oudwins/zog"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"time"
)

// jsonToMap converts datatypes.JSON to map[string]string
func jsonToMap(jsonData []byte) (map[string]string, error) {
	var result map[string]string
	if len(jsonData) > 0 {
		err := json.Unmarshal(jsonData, &result)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal json: %w", err)
		}
	}
	return result, nil
}

// jsonToStringSlice converts datatypes.JSON to []string
func jsonToStringSlice(jsonData []byte) ([]string, error) {
	var result []string
	if len(jsonData) > 0 {
		err := json.Unmarshal(jsonData, &result)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal json: %w", err)
		}
	}
	return result, nil
}

// PinRequest and PinStatusResponse
var _ httputil.DTOValidator = (*PinRequest)(nil)
var _ httputil.DTORequest[*db.IPFSPin] = (*PinRequest)(nil)

type PinRequest struct {
	CID     string            `json:"cid"`
	Name    string            `json:"name,omitempty"`
	Origins []string          `json:"origins,omitempty"`
	Meta    map[string]string `json:"meta,omitempty"`
}

func (p PinRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"CID":     zog.String().Required(),
		"Name":    zog.String().Max(255),
		"Origins": zog.Slice(zog.String()),
		"Meta":    zog.Struct(zog.Shape{}),
	})
}

func (p PinRequest) ToModel() (*db.IPFSPin, error) {
	// Convert []string Origins to datatypes.JSON
	originsJSON, err := json.Marshal(p.Origins)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal origins: %w", err)
	}

	// Convert map[string]string Meta to datatypes.JSON
	metaJSON, err := json.Marshal(p.Meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal meta: %w", err)
	}

	_cid, err := cid.Parse(p.CID)

	if err != nil {
		return nil, fmt.Errorf("failed to parse CID: %w", err)
	}

	return &db.IPFSPin{
		CID:     _cid.Bytes(),
		Name:    p.Name,
		Origins: originsJSON,
		Meta:    metaJSON,
		Status:  db.PinningStatusQueued,
	}, nil
}

type PinStatusResponse struct {
	RequestID string            `json:"requestid"`
	Status    db.PinningStatus  `json:"status"`
	Created   time.Time         `json:"created"`
	Pin       PinRequest        `json:"pin"`
	Delegates []string          `json:"delegates"`
	Info      map[string]string `json:"info,omitempty"`
}

func (p *PinStatusResponse) FromModel(model *db.IPFSPin) error {
	_cid, err := cid.Parse(model.CID)
	if err != nil {
		return err
	}

	p.RequestID = model.RequestID.String()
	p.Status = model.Status
	p.Created = model.CreatedAt

	// Convert datatypes.JSON fields to appropriate types
	metaMap, err := jsonToMap(model.Meta)
	if err != nil {
		return fmt.Errorf("failed to convert meta: %w", err)
	}

	// Convert Delegates from JSON to []string
	delegates, err := jsonToStringSlice(model.Delegates)
	if err != nil {
		return fmt.Errorf("failed to convert delegates: %w", err)
	}

	infoMap, err := jsonToMap(model.Info)
	if err != nil {
		return fmt.Errorf("failed to convert info: %w", err)
	}

	// Convert Origins from JSON to []string
	origins, err := jsonToStringSlice(model.Origins)
	if err != nil {
		return fmt.Errorf("failed to convert origins: %w", err)
	}

	p.Pin = PinRequest{
		CID:     _cid.String(),
		Name:    model.Name,
		Origins: origins,
		Meta:    metaMap,
	}
	p.Delegates = delegates
	p.Info = infoMap
	return nil
}

type PinResultsResponse struct {
	Count   uint64              `json:"count"`
	Results []PinStatusResponse `json:"results"`
}

func (p *PinResultsResponse) FromModel(model []*db.IPFSPin) error {
	p.Results = make([]PinStatusResponse, len(model))
	for i, pin := range model {
		psr := PinStatusResponse{}
		if err := psr.FromModel(pin); err != nil {
			return err
		}
		p.Results[i] = psr
	}
	return nil
}

// PinStatusRequest and PinStatusResponse
var _ httputil.DTOValidator = (*PinStatusRequest)(nil)
var _ httputil.DTORequest[PinStatusRequest] = (*PinStatusRequest)(nil)

type PinStatusRequest struct {
	RequestID string `json:"requestid"`
}

func (p PinStatusRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"RequestID": zog.String().Required(),
	})
}

func (p PinStatusRequest) ToModel() (PinStatusRequest, error) {
	return p, nil
}
