package dto

import (
	"time"

	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// GatewayWebsiteResponse contains website configuration for the gateway
type GatewayWebsiteResponse struct {
	Domain     string `json:"domain"`
	TargetType string `json:"target_type"` // "ipfs" or "ipns"
	TargetHash string `json:"target_hash"` // CID or IPNS name
	Status     string `json:"status"`      // pending_validation, active, broken
}

// GatewayWebsiteStatusResponse contains website status information
type GatewayWebsiteStatusResponse struct {
	Domain      string  `json:"domain"`
	Status      string  `json:"status"` // pending_validation, active, broken
	LastChecked *string `json:"last_checked,omitempty"`
	IsBroken    bool    `json:"is_broken"`
}

func (r *GatewayWebsiteResponse) FromModel(model *db.Website) error {
	r.Domain = model.Domain
	r.TargetType = model.TargetType
	r.TargetHash = model.TargetHash
	r.Status = model.Status
	return nil
}

func (r *GatewayWebsiteStatusResponse) FromModel(model *db.Website) error {
	r.Domain = model.Domain
	r.Status = model.Status
	r.IsBroken = model.Status == string(db.WebsiteStatusBroken)
	if model.LastCheckedAt != nil {
		formatted := model.LastCheckedAt.Format(time.RFC3339)
		r.LastChecked = &formatted
	}
	return nil
}
