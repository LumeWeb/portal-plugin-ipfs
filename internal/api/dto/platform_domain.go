package dto

import (
	"github.com/Oudwins/zog"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// PlatformDomainRequest registers a new platform-owned root (admin operation).
type PlatformDomainRequest struct {
	Domain    string `json:"domain"`
	Namespace string `json:"namespace"` // icann | hns
	ZoneID    uint   `json:"zone_id"`   // operator-owned DNSZone for the root
	Enabled   bool   `json:"enabled,omitempty"`
}

func (r PlatformDomainRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Domain":    zog.String().Required().Min(1).Max(255),
		"Namespace": zog.String().Required().OneOf([]string{string(db.DomainNamespaceICANN), string(db.DomainNamespaceHNS)}),
		"ZoneID":    zog.Uint().Required().GT(0),
		"Enabled":   zog.Bool(),
	})
}

func (r PlatformDomainRequest) ToModel() (*db.PlatformDomain, error) {
	return &db.PlatformDomain{
		Domain:    r.Domain,
		Namespace: db.DomainNamespace(r.Namespace),
		ZoneID:    r.ZoneID,
		Enabled:   r.Enabled,
	}, nil
}

// PlatformDomainUpdateRequest toggles registration state of a platform root.
type PlatformDomainUpdateRequest struct {
	Enabled bool `json:"enabled"`
}

func (r PlatformDomainUpdateRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Enabled": zog.Bool().Required(),
	})
}

func (r PlatformDomainUpdateRequest) ToModel() (*db.PlatformDomain, error) {
	return &db.PlatformDomain{Enabled: r.Enabled}, nil
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
	Label string `json:"label"`
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
