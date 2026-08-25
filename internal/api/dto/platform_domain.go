package dto

import (
	"github.com/Oudwins/zog"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// PlatformDomainRequest registers a new platform-owned root (admin operation).
// The operator's zone is auto-created (idempotently) from the authenticated
// operator user; zone_id is no longer supplied by the client.
type PlatformDomainRequest struct {
	Domain    string  `json:"domain"`
	Namespace *string `json:"namespace,omitempty"` // hns (default) or icann
	Enabled   bool    `json:"enabled,omitempty"`
}

func (r PlatformDomainRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain":    zog.String().Required().Min(1).Max(255),
		"Namespace": zog.Ptr(zog.String().OneOf([]string{string(db.DomainNamespaceHNS), string(db.DomainNamespaceICANN)})),
		"Enabled":   zog.Bool(),
	})
}

func (r PlatformDomainRequest) ToModel() (*db.PlatformDomain, error) {
	namespace := string(db.DomainNamespaceHNS)
	if r.Namespace != nil {
		namespace = *r.Namespace
	}
	return &db.PlatformDomain{
		Domain:    r.Domain,
		Namespace: db.DomainNamespace(namespace),
		Enabled:   r.Enabled,
	}, nil
}

// PlatformDomainBindRequest binds an operator-owned website to the root apex
// of a platform domain (admin operation).
type PlatformDomainBindRequest struct {
	WebsiteID uint `json:"website_id"`
}

func (r PlatformDomainBindRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"WebsiteID": zog.Uint().Required().GT(0),
	})
}

func (r PlatformDomainBindRequest) ToModel() (*db.Website, error) {
	return &db.Website{ID: r.WebsiteID}, nil
}

// PlatformDomainUpdateRequest toggles registration state of a platform root.
type PlatformDomainUpdateRequest struct {
	Enabled *bool `json:"enabled"`
}

func (r PlatformDomainUpdateRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Enabled": zog.Ptr(zog.Bool()),
	})
}

func (r PlatformDomainUpdateRequest) ToModel() (*db.PlatformDomain, error) {
	enabled := false
	if r.Enabled != nil {
		enabled = *r.Enabled
	}
	return &db.PlatformDomain{Enabled: enabled}, nil
}

// PlatformDomainResponse is a registered platform root.
type PlatformDomainResponse struct {
	ID        uint   `json:"id"`
	Domain    string `json:"domain"`
	Namespace string `json:"namespace"`
	ZoneID    uint   `json:"zone_id"`
	Enabled   bool   `json:"enabled"`
}

func (r *PlatformDomainResponse) FromModel(m *db.PlatformDomain) error {
	r.ID = m.ID
	r.Domain = m.Domain
	r.Namespace = string(m.Namespace)
	r.ZoneID = m.ZoneID
	r.Enabled = m.Enabled
	return nil
}

// PlatformAvailabilityRequest is the query for checking whether a label is
// available across platform roots. Label is required.
type PlatformAvailabilityRequest struct {
	Label string `json:"label" query:"label"`
}

func (r PlatformAvailabilityRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Label": zog.String().Required().Min(1).Max(63),
	})
}

func (r PlatformAvailabilityRequest) ToModel() (PlatformAvailabilityRequest, error) {
	return r, nil
}

// PlatformAvailabilityResult is availability for a single platform root.
type PlatformAvailabilityResult struct {
	PlatformDomain string `json:"platform_domain"`
	Namespace      string `json:"namespace"`
	Available      bool   `json:"available"`
}

// PlatformAvailabilityResponse lists availability across all enabled roots.
type PlatformAvailabilityResponse struct {
	Label   string                       `json:"label"`
	Results []PlatformAvailabilityResult `json:"results"`
}

func (r *PlatformAvailabilityResponse) FromModel(m PlatformAvailabilityRequest) error {
	r.Label = m.Label
	return nil
}

// PlatformDomainListResponse is the swagger-only DTO that represents the
// paginated list response for platform roots.
type PlatformDomainListResponse struct {
	Data  []PlatformDomainResponse `json:"data"`
	Total int64                    `json:"total"`
}
