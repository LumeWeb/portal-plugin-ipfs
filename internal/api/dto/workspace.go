package dto

import (
	"time"

	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// WorkspaceRequest carries the optional input for creating a workspace: it may
// optionally supply the ID of an existing website the authenticated user owns
// to attach the workspace to (the publish link). Building in a workspace is
// separate from publishing, so website_id is optional: omit it to create an
// unattached workspace that needs no Website record or domain.
type WorkspaceRequest struct {
	// WebsiteID is the optional website to attach the workspace to. Omit it to
	// create an unattached workspace.
	WebsiteID *uint `json:"website_id" zog:"optional"`
}

// ToModel satisfies httputil.DTORequest for the workspace request. The service
// creates the workspace model from the request, so the parsed request itself
// is returned.
func (r *WorkspaceRequest) ToModel() (*WorkspaceRequest, error) {
	return r, nil
}

// WorkspaceResponse is the owner-visible representation of a workspace. It
// deliberately does not include the proxy Basic Auth credentials, the database
// password, or the portal API key; those are only surfaced through the
// dedicated /access endpoint to the owner. WebsiteID is omitted when the
// workspace is unattached.
type WorkspaceResponse struct {
	ID        uint   `json:"id"`
	WebsiteID *uint  `json:"website_id,omitempty"`
	Label     string `json:"label"`
	// Domain is the workspace's authoring hostname (label + platform domain).
	Domain string `json:"domain"`
	// Status is one of provisioning, failed, ready, suspended, deleting.
	Status string `json:"status"`
	// Error is the last recorded (redacted, secret-safe) provisioning error.
	Error   string    `json:"error,omitempty"`
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
}

func (r *WorkspaceResponse) FromModel(model *pluginDb.Workspace) error {
	r.ID = model.ID
	r.WebsiteID = model.WebsiteID
	r.Label = model.Label
	r.Status = string(model.Status)
	r.Error = model.LastError
	r.Created = model.CreatedAt
	r.Updated = model.UpdatedAt
	// Hostname is only available once the platform domain is populated; when it
	// is not (lazy-loaded), the response omits the domain rather than panicking.
	if model.PlatformDomain.Domain != "" {
		r.Domain = model.Hostname()
	}
	return nil
}

// WorkspaceResolveWebsite is the minimal owner-visible Website representation
// returned by runtime self-resolution. It exposes only the publish
// relationship (id/target/status) — never validation tokens, DNS zone details,
// IPNS keys, or any secret.
type WorkspaceResolveWebsite struct {
	ID         uint   `json:"id"`
	TargetType string `json:"target_type"`
	TargetHash string `json:"target_hash"`
	Status     string `json:"status"`
}

// WorkspaceResolveResponse is the runtime-visible representation of a
// workspace resolved by its Coolify COOLIFY_RESOURCE_UUID plus its workspace
// portal API key. It mirrors WorkspaceResponse — never exposing the portal API
// key, database password, or proxy credentials — and adds the optional Website
// the workspace is attached to (the publish relationship).
type WorkspaceResolveResponse struct {
	ID        uint   `json:"id"`
	WebsiteID *uint  `json:"website_id,omitempty"`
	Label     string `json:"label"`
	// Domain is the workspace's authoring hostname (label + platform domain).
	Domain string `json:"domain"`
	// Status is one of provisioning, failed, ready, suspended, deleting.
	Status string `json:"status"`
	// Error is the last recorded (redacted, secret-safe) provisioning error.
	Error   string    `json:"error,omitempty"`
	Created time.Time `json:"created"`
	Updated time.Time `json:"updated"`
	// Website is the optional attached Website (publish relationship); omitted
	// when the workspace is unattached.
	Website *WorkspaceResolveWebsite `json:"website,omitempty"`
}

// FromResolve builds the resolve response from a workspace (with its platform
// domain loaded) and an optional owning Website.
func (r *WorkspaceResolveResponse) FromResolve(model *pluginDb.Workspace, website *pluginDb.Website) error {
	r.ID = model.ID
	r.WebsiteID = model.WebsiteID
	r.Label = model.Label
	r.Status = string(model.Status)
	r.Error = model.LastError
	r.Created = model.CreatedAt
	r.Updated = model.UpdatedAt
	// Hostname is only available once the platform domain is populated; when it
	// is not (lazy-loaded), the response omits the domain rather than panicking.
	if model.PlatformDomain.Domain != "" {
		r.Domain = model.Hostname()
	}
	if website != nil {
		r.Website = &WorkspaceResolveWebsite{
			ID:         website.ID,
			TargetType: website.TargetType,
			TargetHash: website.TargetHash(),
			Status:     website.Status,
		}
	}
	return nil
}

// WorkspaceAccessResponse is the owner-only access credential view. It returns
// only the proxy Basic Auth credential and never the portal API key or the
// database password.
type WorkspaceAccessResponse struct {
	// Username is the proxy Basic Auth username.
	Username string `json:"username"`
	// Password is the proxy Basic Auth password.
	Password string `json:"password"`
}

func (r *WorkspaceAccessResponse) FromModel(model *pluginDb.AccessCredentials) error {
	r.Username = model.Username
	r.Password = model.Password
	return nil
}
