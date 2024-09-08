package messages

import (
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"time"
)

type Pin struct {
	CID     string            `json:"cid"`
	Name    string            `json:"name,omitempty"`
	Origins []string          `json:"origins,omitempty"`
	Meta    map[string]string `json:"meta,omitempty"`
}

type PinStatus struct {
	RequestID string                 `json:"requestid"`
	Status    pluginDb.PinningStatus `json:"status"`
	Created   time.Time              `json:"created"`
	Pin       Pin                    `json:"pin"`
	Delegates []string               `json:"delegates"`
	Info      InfoObj                `json:"info,omitempty"`
}

type InfoObj map[string]string

type PinResults struct {
	Count   uint64       `json:"count"`
	Results []*PinStatus `json:"results"`
}

// GetPinsRequest represents the query parameters for listing pins
type GetPinsRequest struct {
	CID         []string          `json:"cid,omitempty"`
	Name        string            `json:"name,omitempty"`
	Match       string            `json:"match,omitempty"`
	Status      []string          `json:"status,omitempty"`
	Before      *time.Time        `json:"before,omitempty"`
	After       *time.Time        `json:"after,omitempty"`
	BeforeCount *int64            `json:"_before,omitempty"`
	AfterCount  *int64            `json:"_after,omitempty"`
	Limit       int               `json:"limit,omitempty"`
	Meta        map[string]string `json:"meta,omitempty"`
}

// AddPinRequest represents the body of a request to add a new pin
type AddPinRequest struct {
	Pin
}

// ReplacePinRequest represents the body of a request to replace an existing pin
type ReplacePinRequest struct {
	Pin
}

// Trustless Gateway Structs

// IPFSRequest represents the query parameters for IPFS requests
type IPFSRequest struct {
	Format      string `json:"format,omitempty"`
	Filename    string `json:"filename,omitempty"`
	Download    bool   `json:"download,omitempty"`
	DagScope    string `json:"dag-scope,omitempty"`
	EntityBytes string `json:"entity-bytes,omitempty"`
	CarVersion  int    `json:"car-version,omitempty"`
	CarOrder    string `json:"car-order,omitempty"`
	CarDups     string `json:"car-dups,omitempty"`
}

// IPNSRequest represents the query parameters for IPNS requests
type IPNSRequest struct {
	Format   string `json:"format,omitempty"`
	Filename string `json:"filename,omitempty"`
	Download bool   `json:"download,omitempty"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error struct {
		Reason  string `json:"reason"`
		Details string `json:"details,omitempty"`
	} `json:"error"`
}

type PostUploadResponse struct {
}

type BlockMetaResponse struct {
	Name      string   `json:"name"`
	Type      uint8    `json:"type"`
	BlockSize int64    `json:"block_size"`
	ChildCID  []string `json:"child_cid"`
}
type GetBlockMetaBatchRequest struct {
	CID []string `json:"cid"`
}

type GetBlockMetaBatchResponse = map[string]*BlockMetaResponse
