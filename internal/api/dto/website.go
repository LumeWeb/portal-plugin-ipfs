package dto

import (
	"fmt"
	"time"

	"github.com/Oudwins/zog"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/config"
)

// IPNS Key DTOs

// IPNSKeyRequest represents a request to create or import an IPNS key
type IPNSKeyRequest struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"` // Base64-encoded private key (optional for import)
}

func (r IPNSKeyRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Name": zog.String().Required().Min(1).Max(255),
		"Key":  zog.String().Optional(),
	})
}

func (r *IPNSKeyRequest) ToModel() (*IPNSKeyRequest, error) {
	return r, nil
}

// IPNSKeyResponse represents an IPNS key response
type IPNSKeyResponse struct {
	ID       uint      `json:"id"`
	Name     string    `json:"name"`
	IPNSName string    `json:"ipns_name"`
	PeerID   string    `json:"peer_id"`
	Created  time.Time `json:"created"`
}

func (r *IPNSKeyResponse) FromModel(model *db.IPFSIPNSKey) error {
	r.ID = model.ID
	r.Name = model.Name
	r.IPNSName = model.IPNSName()
	r.PeerID = model.PeerID().String()
	r.Created = model.CreatedAt
	return nil
}

// IPNSKeyExportResponse represents an IPNS key export response
type IPNSKeyExportResponse struct {
	PrivateKey string `json:"private_key"` // Base64-encoded private key
}

// IPNSPublishRequest represents a request to publish a CID to IPNS
type IPNSPublishRequest struct {
	KeyID uint   `json:"key_id"`
	CID   string `json:"cid"`
	TTL   string `json:"ttl,omitempty"` // Duration string (e.g., "24h")
}

func (r IPNSPublishRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"KeyID": zog.UintLike[uint]().Required(),
		"CID":   zog.String().Required(),
		"TTL":   zog.String().Optional(),
	})
}

func (r *IPNSPublishRequest) ToModel() (*IPNSPublishRequest, error) {
	// Validate CID
	_, err := cid.Parse(r.CID)
	if err != nil {
		return nil, fmt.Errorf("invalid CID: %w", err)
	}
	return r, nil
}

// IPNSPublishResponse represents an IPNS publish response
type IPNSPublishResponse struct {
	Name      string    `json:"name"`      // IPNS name (peer ID)
	Value     string    `json:"value"`     // CID
	Sequence  uint64    `json:"sequence"`  // IPNS sequence number
	Validity  time.Time `json:"validity"`  // Valid until
	Published time.Time `json:"published"` // Published at
}

// IPNSResolveResponse represents an IPNS resolve response
type IPNSResolveResponse struct {
	Name     string    `json:"name"`     // IPNS name
	Value    string    `json:"value"`    // Resolved CID
	Sequence uint64    `json:"sequence"` // IPNS sequence number
	Path     string    `json:"path"`     // Full IPFS path
	Expired  bool      `json:"expired"`  // Whether the record is expired
	Expires  time.Time `json:"expires"`  // Expiration time
}

// IPNSRepublishResponse represents an IPNS republish response
type IPNSRepublishResponse struct {
	Count   int    `json:"count"`   // Number of records republished
	Message string `json:"message"` // Status message
}

// Website DTOs

// WebsiteRequest represents a request to create or update a website
type WebsiteRequest struct {
	Domain     string `json:"domain"`
	TargetType string `json:"target_type"` // db.WebsiteTargetTypeIPFS or db.WebsiteTargetTypeIPNS
	TargetHash string `json:"target_hash"` // CID or IPNS peer ID
}

func (r WebsiteRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain": zog.String().Required().Min(1).Max(255),
		"TargetType": config.ZogStringLike[db.WebsiteTargetType]().OneOf([]db.WebsiteTargetType{
			db.WebsiteTargetTypeIPFS,
			db.WebsiteTargetTypeIPNS,
		}).Required(),
		"TargetHash": zog.String().Required().Min(1).Max(255),
	})
}

func (r *WebsiteRequest) ToModel() (*db.Website, error) {
	// Validate target type
	if r.TargetType != string(db.WebsiteTargetTypeIPFS) && r.TargetType != string(db.WebsiteTargetTypeIPNS) {
		return nil, fmt.Errorf("invalid target type: must be '%s' or '%s'", db.WebsiteTargetTypeIPFS, db.WebsiteTargetTypeIPNS)
	}

	website := &db.Website{
		Domain:     r.Domain,
		TargetType: r.TargetType,
		Status:     string(db.WebsiteStatusPendingValidation),
	}

	// Validate and parse CID for IPFS targets
	if r.TargetType == string(db.WebsiteTargetTypeIPFS) {
		c, err := cid.Parse(r.TargetHash)
		if err != nil {
			return nil, fmt.Errorf("invalid CID: %w", err)
		}
		website.TargetMultihash = c.Hash()
		version := uint8(c.Version())
		website.CIDVersion = &version
	}

	// Validate peer ID for IPNS targets
	if r.TargetType == string(db.WebsiteTargetTypeIPNS) {
		target, err := db.NewIPNSTargetFromString(r.TargetHash)
		if err != nil {
			return nil, fmt.Errorf("invalid IPNS target: %w", err)
		}
		website.TargetMultihash = target.ToMultihash()
		website.CIDVersion = nil // NULL for IPNS
	}

	return website, nil
}

// SSLStatusInfo represents SSL certificate status information
type SSLStatusInfo struct {
	Status         string     `json:"status"`
	Error          string     `json:"error,omitempty"`
	IssuedAt       *time.Time `json:"issued_at,omitempty"`
	LastUpdatedAt  *time.Time `json:"last_updated_at,omitempty"`
}

// WebsiteResponse represents a website response
type WebsiteResponse struct {
	ID                  uint           `json:"id"`
	Domain              string         `json:"domain"`
	TargetType          string         `json:"target_type"`
	TargetHash          string         `json:"target_hash"`
	Status              string         `json:"status"`
	ValidationToken     string         `json:"validation_token"`
	ValidationExpiresAt *time.Time     `json:"validation_expires_at,omitempty"`
	LastCheckedAt       *time.Time     `json:"last_checked_at,omitempty"`
	Created             time.Time      `json:"created"`
	Updated             time.Time      `json:"updated"`
	Expired             bool            `json:"expired"` // Whether validation token has expired
	SSL                 *SSLStatusInfo  `json:"ssl,omitempty"`
}

func (r *WebsiteResponse) FromModel(model *db.Website) error {
	r.ID = model.ID
	r.Domain = model.Domain
	r.TargetType = model.TargetType
	r.TargetHash = model.TargetHash() // Use helper to generate string from multihash
	r.Status = model.Status
	r.ValidationToken = model.ValidationToken
	r.ValidationExpiresAt = model.ValidationExpiresAt
	r.LastCheckedAt = model.LastCheckedAt
	r.Created = model.CreatedAt
	r.Updated = model.UpdatedAt
	r.Expired = model.IsExpired()

	if model.SSLStatus != "" {
		var lastUpdated *time.Time
		if model.SSLLastUpdatedAt != nil {
			v := *model.SSLLastUpdatedAt
			lastUpdated = &v
		}
		var issuedAt *time.Time
		if model.SSLIssuedAt != nil {
			v := *model.SSLIssuedAt
			issuedAt = &v
		}
		r.SSL = &SSLStatusInfo{
			Status:        model.SSLStatus,
			Error:         model.SSLError,
			IssuedAt:      issuedAt,
			LastUpdatedAt: lastUpdated,
		}
	}

	return nil
}

// WebsiteValidateResponse represents a website validation response
type WebsiteValidateResponse struct {
	ID      uint   `json:"id"`
	Domain  string `json:"domain"`
	Valid   bool   `json:"valid"`
	Message string `json:"message"`
}

// WebsiteItem represents a website listing item (used in list responses)
type WebsiteItem WebsiteResponse

// WebsiteFilter represents filtering options for website listings
type WebsiteFilter struct {
	Domain     *string `json:"domain,omitempty" query:"domain"`
	TargetType *string `json:"target_type,omitempty" query:"target_type"`
	Status     *string `json:"status,omitempty" query:"status"`
}

func (f WebsiteFilter) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain": zog.Ptr(zog.String().Optional()),
		"TargetType": zog.Ptr(config.ZogStringLike[db.WebsiteTargetType]().OneOf([]db.WebsiteTargetType{
			db.WebsiteTargetTypeIPFS,
			db.WebsiteTargetTypeIPNS,
		}).Optional()),
		"Status": zog.Ptr(config.ZogStringLike[db.WebsiteStatus]().OneOf([]db.WebsiteStatus{
			db.WebsiteStatusPendingValidation,
			db.WebsiteStatusActive,
			db.WebsiteStatusBroken,
			db.WebsiteStatusBlocked,
		}).Optional()),
	})
}

func (f WebsiteFilter) ToModel() (WebsiteFilter, error) {
	return f, nil
}

// SSLStatusUpdateRequest represents a request to update SSL certificate status from Caddy webhook
type SSLStatusUpdateRequest struct {
	Status    db.SSLStatus `json:"status"`
	Error     string       `json:"error,omitempty"`
	Timestamp string       `json:"timestamp,omitempty"`
}

func (r SSLStatusUpdateRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Status": config.ZogStringLike[db.SSLStatus]().OneOf([]db.SSLStatus{
			db.SSLStatusPending,
			db.SSLStatusIssuing,
			db.SSLStatusReady,
			db.SSLStatusFailed,
		}).Required(),
		"Error":     zog.String().Optional().Max(1000),
		"Timestamp": zog.String().Optional().Transform(func(val *string, ctx zog.Ctx) error {
			if *val == "" {
				return fmt.Errorf("timestamp cannot be an empty string")
			}
			_, err := time.Parse(time.RFC3339, *val)
			if err != nil {
				return fmt.Errorf("timestamp must be in RFC3339 format: %w", err)
			}
			return nil
		}),
	})
}

func (r *SSLStatusUpdateRequest) ToModel() (*SSLStatusUpdateRequest, error) {
	return r, nil
}

// Ensure DTOs implement httputil interfaces
var _ httputil.DTOValidator = (*IPNSKeyRequest)(nil)
var _ httputil.DTOValidator = (*IPNSPublishRequest)(nil)
var _ httputil.DTORequest[*IPNSPublishRequest] = (*IPNSPublishRequest)(nil)
var _ httputil.DTOValidator = (*WebsiteRequest)(nil)
var _ httputil.DTORequest[*db.Website] = (*WebsiteRequest)(nil)
var _ httputil.DTOValidator = (*WebsiteFilter)(nil)
var _ httputil.DTORequest[WebsiteFilter] = (*WebsiteFilter)(nil)
var _ httputil.DTOValidator = (*SSLStatusUpdateRequest)(nil)
