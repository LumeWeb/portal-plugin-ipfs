package dto

import (
	"fmt"
	"time"

	"github.com/Oudwins/zog"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/config"
)

// Website Constants

// DefaultWebsiteEnabled is the default value for website DNS hosting enabled field
// Applications should use this constant to ensure consistency across the codebase
const DefaultWebsiteEnabled = true

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

func (r *IPNSKeyRequest) ToModel() (*db.IPFSIPNSKey, error) {
	if r.Name == "" {
		return nil, &httputil.ValidationError{
			FieldErrors: map[string]string{
				"name": "name is required",
			},
		}
	}
	return &db.IPFSIPNSKey{
		Name: r.Name,
	}, nil
}

// IPNSKeyResponse represents an IPNS key response
type IPNSKeyResponse struct {
	ID              uint       `json:"id"`
	Name            string     `json:"name"`
	IPNSName        string     `json:"ipns_name"`
	PeerID          string     `json:"peer_id"`
	Value           string     `json:"value,omitempty"`
	LastPublishedAt *time.Time `json:"last_published_at,omitempty"`
	Created         time.Time  `json:"created"`
}

func (r *IPNSKeyResponse) FromModel(model *db.IPFSIPNSKey) error {
	r.ID = model.ID
	r.Name = model.Name
	r.IPNSName = model.IPNSName()
	r.PeerID = model.PeerID().String()
	r.Value = model.LastPublishedCID
	r.LastPublishedAt = model.LastPublishedAt
	r.Created = model.CreatedAt
	return nil
}

// IPNSKeyListResponse represents an IPNS key in a list response
type IPNSKeyListResponse struct {
	ID              uint       `json:"id"`
	Name            string     `json:"name"`
	IPNSName        string     `json:"ipns_name"`
	PeerID          string     `json:"peer_id"`
	Value           string     `json:"value,omitempty"`
	LastPublishedAt *time.Time `json:"last_published_at,omitempty"`
	Created         time.Time  `json:"created"`
}

func (r *IPNSKeyListResponse) FromModel(model *db.IPFSIPNSKey) error {
	r.ID = model.ID
	r.Name = model.Name
	r.IPNSName = model.IPNSName()
	r.PeerID = model.PeerID().String()
	r.Value = model.LastPublishedCID
	r.LastPublishedAt = model.LastPublishedAt
	r.Created = model.CreatedAt
	return nil
}

// IPNSKeyListResponseResponse is a swagger-only DTO that represents the paginated response for IPNS keys.
// It merges the generic queryutil.Response[*dto.IPNSKeyListResponse] for OpenAPI documentation.
//
// This struct exists due to a TODO bug where queryutil.Response generics are not getting detected
// properly as an array type in the swagger documentation generation. By providing a concrete struct,
// we ensure the swagger docs correctly show the data field as an array of IPNSKeyListResponse items.
//
// Note: This struct is only used for swagger documentation, not for actual encoding.
type IPNSKeyListResponseResponse struct {
	Data  []IPNSKeyListResponse `json:"data"`
	Total int64                 `json:"total"`
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
		return nil, &httputil.ValidationError{
			FieldErrors: map[string]string{
				"cid": fmt.Sprintf("invalid CID: %v", err),
			},
		}
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

func (r *IPNSPublishResponse) FromModel(any) error {
	return nil
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

func (r *IPNSResolveResponse) FromModel(any) error {
	return nil
}

// IPNSRepublishResponse represents an IPNS republish response
type IPNSRepublishResponse struct {
	Count   int    `json:"count"`
	Message string `json:"message"`
}

func (r *IPNSRepublishResponse) FromModel(any) error {
	return nil
}

// Website DTOs

// WebsiteRequest represents a request to create a website.
//
// Its primary domain can be supplied in one of two mutually exclusive ways:
//   - A user-owned domain: set Domain (plus an optional Namespace).
//   - A platform subdomain (free subdomain under an operator-owned root): set
//     PlatformDomain (the root, e.g. "pinned.site") plus exactly one of Label
//     (an explicit subdomain label) or Generate (true — let the platform choose
//     a computed label). The namespace and DNS hosting are derived from the
//     platform root, mirroring the domain-bind flow.
type WebsiteRequest struct {
	Domain     string               `json:"domain,omitempty"`               // primary domain (transparently created as a WebsiteDomain binding); omitted when claiming a platform subdomain
	Namespace  *db.DomainNamespace  `json:"namespace,omitempty"`           // icann (default) or hns
	TargetType db.WebsiteTargetType `json:"target_type"`                   // db.WebsiteTargetTypeIPFS or db.WebsiteTargetTypeIPNS
	TargetHash string               `json:"target_hash"`                   // CID or IPNS peer ID
	DNSEnabled *bool                `json:"dns_hosting_enabled,omitempty"` // Whether DNS hosting is enabled for the primary domain (defaults to true if not specified)

	// Platform subdomain claim (mutually exclusive with Domain/Namespace).
	PlatformDomain string `json:"platform_domain,omitempty"`
	// PlatformNamespace optionally disambiguates which alt-root namespace to
	// claim under when the same root is registered under multiple namespaces.
	PlatformNamespace string `json:"platform_namespace,omitempty"`
	Label             string `json:"label,omitempty"`
	Generate          bool   `json:"generate,omitempty"`
}

// IsPlatformClaim reports whether this request claims a platform subdomain
// rather than a user-owned domain. A claim is expressed by naming a platform
// root (PlatformDomain), requesting an auto-generated subdomain (Generate), or
// supplying an explicit label (Label) — any of which selects the platform path.
func (r WebsiteRequest) IsPlatformClaim() bool {
	return r.PlatformDomain != "" || r.Generate || r.Label != ""
}

func (r WebsiteRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain": zog.String().Min(1).Max(255),
		"Namespace": zog.Ptr(config.ZogStringLike[db.DomainNamespace]().OneOf([]db.DomainNamespace{
			db.DomainNamespaceICANN,
			db.DomainNamespaceHNS,
		})),
		"TargetType": config.ZogStringLike[db.WebsiteTargetType]().OneOf([]db.WebsiteTargetType{
			db.WebsiteTargetTypeIPFS,
			db.WebsiteTargetTypeIPNS,
		}).Required(),
		"TargetHash":        zog.String().Required().Min(1).Max(255),
		"DNSEnabled":        zog.Ptr(zog.Bool()),
		"PlatformDomain":    zog.String().Min(1).Max(255),
		"PlatformNamespace": zog.String().OneOf([]string{string(db.DomainNamespaceICANN), string(db.DomainNamespaceHNS)}),
		"Label":             zog.String().Min(1).Max(63),
		"Generate":          zog.Bool(),
	})
}

func (r *WebsiteRequest) ToModel() (*db.Website, error) {
	// Validate target type
	if r.TargetType != db.WebsiteTargetTypeIPFS && r.TargetType != db.WebsiteTargetTypeIPNS {
		return nil, &httputil.ValidationError{
			FieldErrors: map[string]string{
				"target_type": fmt.Sprintf("must be '%s' or '%s'", db.WebsiteTargetTypeIPFS, db.WebsiteTargetTypeIPNS),
			},
		}
	}

	website := &db.Website{
		TargetType: string(r.TargetType),
		Status:     string(db.WebsiteStatusPendingValidation),
	}

	// Validate and parse CID for IPFS targets
	if r.TargetType == db.WebsiteTargetTypeIPFS {
		c, err := cid.Parse(r.TargetHash)
		if err != nil {
			return nil, &httputil.ValidationError{
				FieldErrors: map[string]string{
					"target_hash": fmt.Sprintf("invalid CID: %v", err),
				},
			}
		}

		// Normalize to v1 for consistent storage
		normalizedCid := encoding.NormalizeCid(c)
		website.TargetMultihash = normalizedCid.Hash()
		version := uint8(normalizedCid.Version())
		website.CIDVersion = &version
		codec := uint8(normalizedCid.Type())
		website.CIDType = &codec
	}

	// Validate peer ID for IPNS targets
	if r.TargetType == db.WebsiteTargetTypeIPNS {
		target, err := db.NewIPNSTargetFromString(r.TargetHash)
		if err == nil {
			// Valid IPNS target — store directly
			website.TargetMultihash = target.ToMultihash()
			website.CIDVersion = nil // NULL for IPNS
		} else {
			// Not a valid IPNS peer ID/libp2p-key — check if it's a valid IPFS CID.
			// The service layer will auto-create an IPNS key and publish this CID,
			// converting the target from a plain CID to an IPNS peer ID.
			c, cidErr := cid.Parse(r.TargetHash)
			if cidErr != nil {
				return nil, &httputil.ValidationError{
					FieldErrors: map[string]string{
						"target_hash": fmt.Sprintf("invalid IPNS target: %v", err),
					},
				}
			}
			// Store temporarily as IPFS-style fields; the service layer
			// will detect auto-conversion and replace with IPNS target.
			normalizedCid := encoding.NormalizeCid(c)
			website.TargetMultihash = normalizedCid.Hash()
			version := uint8(normalizedCid.Version())
			website.CIDVersion = &version
			codec := uint8(normalizedCid.Type())
			website.CIDType = &codec
		}
	}

	return website, nil
}

// WebsiteUpdateRequest represents a request to update a website.
// All fields are optional — only provided fields will be updated.
// DNS hosting state lives on the primary WebsiteDomain; the website-level
// dns_hosting_enabled field is accepted here for backward compatibility and
// applied only to the website's primary domain binding. Setting domain changes
// the website's primary (apex) domain.
type WebsiteUpdateRequest struct {
	TargetType *db.WebsiteTargetType `json:"target_type,omitempty"`
	TargetHash *string               `json:"target_hash,omitempty"`
	Domain     *string               `json:"domain,omitempty"`
	Namespace  *db.DomainNamespace   `json:"namespace,omitempty"`
	DNSEnabled *bool                 `json:"dns_hosting_enabled,omitempty"`
}

// HasUpdates returns true if at least one field is set
func (r *WebsiteUpdateRequest) HasUpdates() bool {
	return r.TargetType != nil || r.TargetHash != nil || r.Domain != nil || r.Namespace != nil || r.DNSEnabled != nil
}

func (r WebsiteUpdateRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"TargetType": zog.Ptr(config.ZogStringLike[db.WebsiteTargetType]().OneOf([]db.WebsiteTargetType{
			db.WebsiteTargetTypeIPFS,
			db.WebsiteTargetTypeIPNS,
		})),
		"TargetHash": zog.Ptr(zog.String().Required().Min(1).Max(255)),
		"Domain":     zog.Ptr(zog.String().Required().Min(1).Max(255)),
		"Namespace": zog.Ptr(config.ZogStringLike[db.DomainNamespace]().OneOf([]db.DomainNamespace{
			db.DomainNamespaceICANN,
			db.DomainNamespaceHNS,
		})),
		"DNSEnabled": zog.Ptr(zog.Bool()),
	})
}

func (r *WebsiteUpdateRequest) ToModel() (*db.Website, error) {
	website := &db.Website{}

	if r.TargetType != nil {
		website.TargetType = string(*r.TargetType)
	}

	if r.TargetHash != nil && r.TargetType != nil {
		if *r.TargetType == db.WebsiteTargetTypeIPFS {
			c, err := cid.Parse(*r.TargetHash)
			if err != nil {
				return nil, &httputil.ValidationError{
					FieldErrors: map[string]string{
						"target_hash": fmt.Sprintf("invalid CID: %v", err),
					},
				}
			}
			normalizedCid := encoding.NormalizeCid(c)
			website.TargetMultihash = normalizedCid.Hash()
			version := uint8(normalizedCid.Version())
			website.CIDVersion = &version
			codec := uint8(normalizedCid.Type())
			website.CIDType = &codec
		}

		if *r.TargetType == db.WebsiteTargetTypeIPNS {
			target, err := db.NewIPNSTargetFromString(*r.TargetHash)
			if err == nil {
				website.TargetMultihash = target.ToMultihash()
				website.CIDVersion = nil
			} else {
				// Not a valid IPNS peer ID — check if it's a CID for auto-conversion.
				c, cidErr := cid.Parse(*r.TargetHash)
				if cidErr != nil {
					return nil, &httputil.ValidationError{
						FieldErrors: map[string]string{
							"target_hash": fmt.Sprintf("invalid IPNS target: %v", err),
						},
					}
				}
				// Store temporarily as IPFS-style fields for service-layer auto-conversion.
				normalizedCid := encoding.NormalizeCid(c)
				website.TargetMultihash = normalizedCid.Hash()
				version := uint8(normalizedCid.Version())
				website.CIDVersion = &version
				codec := uint8(normalizedCid.Type())
				website.CIDType = &codec
			}
		}
	}

	return website, nil
}

// SSLStatusInfo represents SSL certificate status information
type SSLStatusInfo struct {
	Status        string     `json:"status"`
	Error         string     `json:"error,omitempty"`
	IssuedAt      *time.Time `json:"issued_at,omitempty"`
	LastUpdatedAt *time.Time `json:"last_updated_at,omitempty"`
}

// WebsiteResponse represents a website response
type WebsiteResponse struct {
	ID     uint   `json:"id"`
	Domain string `json:"domain"`
	// IPFS or IPNS
	TargetType string `json:"target_type" sort:"true"`
	// CID (IPFS) or peer ID (IPNS)
	// Not sortable: derived from target_multihash (no matching column).
	TargetHash string `json:"target_hash"`
	// FK to the linked IPNS key (set when DNS hosting auto-creates a key)
	IPNSKeyID *uint `json:"ipns_key_id,omitempty"`
	// The currently-published IPFS content CID (distinct from target_hash when target is IPNS)
	ActiveCID string `json:"active_cid,omitempty"`
	Status    string `json:"status" sort:"true"`
	// The full TXT record value (e.g. "lumeweb-verify=abc123...")
	ValidationToken string `json:"validation_token"`
	// The DNS hostname for the TXT record (e.g. "lumeweb-verify.example.com")
	ValidationRecordHost string     `json:"validation_record_host,omitempty"`
	ValidationExpiresAt  *time.Time `json:"validation_expires_at,omitempty"`
	LastCheckedAt        *time.Time `json:"last_checked_at,omitempty"`
	// Canonical PowerDNS zone hosting this website's records (0 = none)
	ZoneID *uint `json:"zone_id,omitempty"`
	// Whether DNS hosting (zone + records) is enabled for this website
	Enabled bool `json:"dns_hosting_enabled"`
	// True if domain is a subdomain of a shared DNS zone
	IsSubdomain bool `json:"is_subdomain"`
	// Gateway domain for constructing public URLs (e.g. ipfs.example.com)
	GatewayDomain string `json:"gateway_domain,omitempty"`
	// Created/Updated are not sortable: columns are created_at/updated_at.
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	// Whether validation token has expired
	Expired bool           `json:"expired"`
	SSL     *SSLStatusInfo `json:"ssl,omitempty"`
	// tokenKey is set by SetValidationRecordInfo; rawToken holds the raw
	// per-resource validation token set by FromModel. Backing ValidationToken
	// off rawToken makes the token format deterministic regardless of whether
	// SetValidationRecordInfo runs before or after FromModel.
	tokenKey string
	rawToken string
}

func (r *WebsiteResponse) FromModel(model *db.Website) error {
	r.ID = model.ID
	r.TargetType = model.TargetType
	r.TargetHash = model.TargetHash() // helper generates string from multihash
	r.Status = model.Status
	r.IPNSKeyID = model.IPNSKeyID
	r.ValidationExpiresAt = model.ValidationExpiresAt
	r.LastCheckedAt = model.LastCheckedAt
	r.Created = model.CreatedAt
	r.Updated = model.UpdatedAt
	r.Expired = model.IsExpired()
	// Keep the raw model token in rawToken so the format can be derived
	// consistently once tokenKey is known, independent of call order.
	r.rawToken = model.ValidationToken
	r.applyTokenFormat()

	return nil
}

// applyTokenFormat derives ValidationToken from the raw token and tokenKey.
// It is idempotent: repeated calls (e.g. EncodeResponse re-running FromModel)
// never double the prefix, and calling it with an unset tokenKey yields the
// raw token.
func (r *WebsiteResponse) applyTokenFormat() {
	if r.tokenKey != "" && r.rawToken != "" {
		r.ValidationToken = r.tokenKey + "=" + r.rawToken
	} else {
		r.ValidationToken = r.rawToken
	}
}

// SetPrimaryDomain populates the website-level domain/DNS fields from the
// website's primary (apex) WebsiteDomain binding, which now owns DNS hosting
// state. The Website record no longer carries a domain string. Callers resolve
// the primary binding (via the website service) and call this after FromModel.
func (r *WebsiteResponse) SetPrimaryDomain(primary *db.WebsiteDomain) {
	if primary == nil {
		r.Domain = ""
		r.ZoneID = nil
		r.Enabled = false
		return
	}
	r.Domain = primary.Domain
	// The zone is canonicalized on WebsiteDomain.ZoneID.
	if primary.ZoneID != 0 {
		r.ZoneID = &primary.ZoneID
	} else {
		r.ZoneID = nil
	}
	r.Enabled = primary.DNSHostingEnabled
	if r.tokenKey != "" && r.rawToken != "" {
		r.ValidationRecordHost = r.tokenKey + "." + primary.Domain
	}
}

// IPNSKeyCIDResolver resolves the last-published CID for an IPNS key.
type IPNSKeyCIDResolver interface {
	GetKeyLastPublishedCID(userID uint, keyID uint) string
}

// EnrichActiveCID populates the ActiveCID field by resolving the linked IPNS key's
// last-published CID from the resolver. This is separate from FromModel because
// it requires a service lookup beyond the DB model.
func (r *WebsiteResponse) EnrichActiveCID(resolver IPNSKeyCIDResolver, userID uint, model *db.Website) {
	if model.IPNSKeyID == nil {
		return
	}
	if cid := resolver.GetKeyLastPublishedCID(userID, *model.IPNSKeyID); cid != "" {
		r.ActiveCID = cid
	}
}

// SetSubdomainInfo sets the IsSubdomain flag based on the zone's domain.
// If zoneDomain is empty, the website is considered a top-level website.
func (r *WebsiteResponse) SetSubdomainInfo(zoneDomain string) {
	r.IsSubdomain = zoneDomain != "" && zoneDomain != r.Domain
}

func (r *WebsiteResponse) SetValidationRecordInfo(tokenKey string) {
	r.tokenKey = tokenKey
	// Re-derive the token format (and record host) so the result is correct
	// regardless of whether this runs before or after FromModel.
	r.applyTokenFormat()
	if r.Domain != "" && r.rawToken != "" {
		r.ValidationRecordHost = tokenKey + "." + r.Domain
	}
}

// WebsiteValidateResponse represents a website validation response
type WebsiteValidateResponse struct {
	ID      uint   `json:"id"`
	Domain  string `json:"domain"`
	Valid   bool   `json:"valid"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
}

// WebsiteItem represents a website listing item (used in list responses)
type WebsiteItem WebsiteResponse

// WebsiteItemResponse is a swagger-only DTO that represents the paginated response for websites.
// It merges the generic queryutil.Response[dto.WebsiteItem] for OpenAPI documentation.
//
// This struct exists due to a TODO bug where queryutil.Response generics are not getting detected
// properly as an array type in the swagger documentation generation. By providing a concrete struct,
// we ensure the swagger docs correctly show the data field as an array of WebsiteItem items.
//
// Note: This struct is only used for swagger documentation, not for actual encoding.
type WebsiteItemResponse struct {
	Data  []WebsiteItem `json:"data"`
	Total int64         `json:"total"`
}

// WebsiteFilter represents filtering options for website listings
type WebsiteFilter struct {
	Domain     *string               `json:"domain,omitempty" filter:"true"`
	TargetType *db.WebsiteTargetType `json:"target_type,omitempty" filter:"true"`
	Status     *db.WebsiteStatus     `json:"status,omitempty" filter:"true"`
}

func (f WebsiteFilter) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain": zog.Ptr(zog.String().Optional().Max(255)),
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

// WebsiteConfig holds website-related configuration
type WebsiteConfig struct {
	GatewayDomain string
	Nameservers   []string
}

// WebsiteConfigResponse returns website-related configuration for client use
type WebsiteConfigResponse struct {
	GatewayDomain string   `json:"gateway_domain,omitempty"`
	Nameservers   []string `json:"nameservers,omitempty"`
}

var _ httputil.DTOResponse[*WebsiteConfig] = (*WebsiteConfigResponse)(nil)

func (r *WebsiteConfigResponse) FromModel(cfg *WebsiteConfig) error {
	r.GatewayDomain = cfg.GatewayDomain
	r.Nameservers = cfg.Nameservers
	return nil
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
		"Error": zog.String().Optional().Max(1000),
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
var _ httputil.DTORequest[*db.IPFSIPNSKey] = (*IPNSKeyRequest)(nil)
var _ httputil.DTOResponse[*db.IPFSIPNSKey] = (*IPNSKeyResponse)(nil)
var _ httputil.DTOValidator = (*IPNSKeyRequest)(nil)
var _ httputil.DTOResponse[any] = (*IPNSPublishResponse)(nil)
var _ httputil.DTOResponse[any] = (*IPNSResolveResponse)(nil)
var _ httputil.DTOResponse[any] = (*IPNSRepublishResponse)(nil)
var _ httputil.DTORequest[*IPNSPublishRequest] = (*IPNSPublishRequest)(nil)
var _ httputil.DTOValidator = (*IPNSPublishRequest)(nil)
var _ httputil.DTORequest[*IPNSPublishRequest] = (*IPNSPublishRequest)(nil)
var _ httputil.DTOValidator = (*WebsiteRequest)(nil)
var _ httputil.DTORequest[*db.Website] = (*WebsiteRequest)(nil)
var _ httputil.DTOValidator = (*WebsiteUpdateRequest)(nil)
var _ httputil.DTORequest[*db.Website] = (*WebsiteUpdateRequest)(nil)
var _ httputil.DTOValidator = (*WebsiteFilter)(nil)
var _ httputil.DTORequest[WebsiteFilter] = (*WebsiteFilter)(nil)
var _ httputil.DTOValidator = (*SSLStatusUpdateRequest)(nil)
