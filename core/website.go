package core

import (
	"context"
	"time"

	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

const WEBSITE_SERVICE = "ipfs.website"

const DELEGATED_DOMAIN_SERVICE = "ipfs.delegated_domain"

type ValidationReason string

const (
	ValidationReasonValidated         ValidationReason = "validated"
	ValidationReasonTokenExpired      ValidationReason = "token_expired"
	ValidationReasonDNSMissing        ValidationReason = "dns_missing"
	ValidationReasonDNSMismatch       ValidationReason = "dns_mismatch"
	ValidationReasonTokenMissing      ValidationReason = "token_missing"
	ValidationReasonDelegationPending ValidationReason = "delegation_pending"
	ValidationReasonTLSAMissing       ValidationReason = "tlsa_missing"
	ValidationReasonTLSAMismatch      ValidationReason = "tlsa_mismatch"
)

// ValidationCheck gate names. Shared by website DNS validation and domain
// delegation verification so both surfaces report a stable, machine-readable
// gate identifier.
const (
	ValidationCheckDNSLink    = "dnslink"
	ValidationCheckToken      = "token"
	ValidationCheckDelegation = "delegation"
	ValidationCheckDNSSEC     = "dnssec"
	ValidationCheckOnChain    = "onchain"
	ValidationCheckPlatform   = "platform"
	ValidationCheckTLSA       = "tlsa"
)

// ValidationCheck reports the outcome of a single validation gate (website
// DNS validation or domain delegation verification) so a client can surface
// exactly which record/step failed and how to fix it, rather than relying on a
// single generic message. Name is one of the ValidationCheck* constants.
type ValidationCheck struct {
	// Name identifies the gate.
	Name string `json:"name"`
	// OK reports whether this gate passed. False gates carry the Message and,
	// where applicable, Expected/Found so a client can render targeted guidance.
	OK bool `json:"ok"`
	// Message is a human-readable detail for this gate.
	Message string `json:"message,omitempty"`
	// Expected holds what the gate required (e.g. the DNSLink the site must
	// serve). Multi-valued gates (e.g. nameserver sets) are comma-joined at the
	// API boundary.
	Expected string `json:"expected,omitempty"`
	// Found holds what the gate actually observed (e.g. the served DNSLink).
	// Multi-valued gates (e.g. nameserver sets) are comma-joined at the API
	// boundary.
	Found string `json:"found,omitempty"`
}

type ValidateDNSResult struct {
	Valid   bool
	Message string
	Reason  ValidationReason // machine-readable: "validated", "token_expired", "dns_missing", "dns_mismatch", "token_missing", "delegation_pending"
	// Checks enumerates each validation gate and its outcome, so clients can
	// render per-gate fix-up guidance instead of a single generic message.
	Checks []ValidationCheck
}

// WebsiteService defines the interface for managing website configurations
type WebsiteService interface {
	core.Service
	core.Configurable

	// CreateWebsite creates a new website configuration
	CreateWebsite(ctx context.Context, website *pluginDb.Website) (*pluginDb.Website, error)

	// GetWebsite retrieves a single website by ID
	GetWebsite(ctx context.Context, userID uint, websiteID uint) (*pluginDb.Website, error)

	// GetWebsiteByDomain retrieves a website by domain name, along with its
	// namespace (icann, hns). For legacy ipfs_websites.domain lookups the
	// namespace defaults to ICANN.
	GetWebsiteByDomain(ctx context.Context, domain string) (*pluginDb.Website, pluginDb.DomainNamespace, error)

	// ListWebsites retrieves a paginated and filtered list of websites
	ListWebsites(ctx context.Context, userID uint, filter []queryutil.CrudFilter, sort []filter.Sort, pagination queryutil.Pagination) ([]*pluginDb.Website, int64, error)

	// UpdateWebsite updates an existing website
	UpdateWebsite(ctx context.Context, userID uint, websiteID uint, updates map[string]interface{}) (*pluginDb.Website, error)

	// DeleteWebsite soft-deletes a website by ID
	DeleteWebsite(ctx context.Context, userID uint, websiteID uint) error

	// BlockWebsite blocks a website (admin operation)
	BlockWebsite(ctx context.Context, websiteID uint) error

	// UnblockWebsite unblocks a website (admin operation)
	UnblockWebsite(ctx context.Context, websiteID uint) error

	// ValidateDNS validates the DNS TXT record for a website domain
	ValidateDNS(ctx context.Context, userID uint, websiteID uint) (ValidateDNSResult, error)

	// CheckStatus checks the status of a website by validating its target
	CheckStatus(ctx context.Context, website *pluginDb.Website) (pluginDb.WebsiteStatus, error)

	// UpdateSSLStatus updates the SSL certificate status for a domain binding.
	// SSL state is a per-domain property, so it returns the updated
	// WebsiteDomain (the source of truth for certificate status).
	UpdateSSLStatus(ctx context.Context, domain string, status pluginDb.SSLStatus, sslError string, timestamp *time.Time) (*pluginDb.WebsiteDomain, error)

	// GetApexDomainBinding returns the website's primary/apex domain binding,
	// whose SSL state is presented at the website level for backward-compatible
	// site-level SSL synthesis.
	GetApexDomainBinding(ctx context.Context, websiteID uint) (*pluginDb.WebsiteDomain, error)

	// SetDomainDNSEnabled enables or disables DNS hosting for a specific domain
	// binding (the per-domain primitive). Enabling runs the DNS hosting
	// transition (zone/records/IPNS setup) for that domain; disabling tears it
	// down. Returns the updated binding.
	SetDomainDNSEnabled(ctx context.Context, userID, websiteID, domainID uint, enabled bool) (*pluginDb.WebsiteDomain, error)

	// SetPrimaryDomain repoints the website's primary (apex) domain binding to
	// the given WebsiteDomain. Returns the new primary binding.
	SetPrimaryDomain(ctx context.Context, userID, websiteID, domainID uint) (*pluginDb.WebsiteDomain, error)

	// NotifyAdminWebsiteCreated sends the admin "website created" notification
	// for the given website. The caller must have created the primary domain
	// binding first so the email's Domain field resolves. Resolves the
	// recipient user's email internally. No-op when notifications are disabled.
	NotifyAdminWebsiteCreated(ctx context.Context, websiteID uint) error

	// ActivatePlatformSubdomainWebsite activates a website whose primary domain
	// binding is a just-created platform subdomain. The platform controls both
	// ends of the DNS check for these, so the site requires no external
	// validation call: it transitions from pending_validation to active as soon
	// as the binding is live. It returns an error if the website cannot be
	// loaded or its primary binding is not a platform subdomain.
	ActivatePlatformSubdomainWebsite(ctx context.Context, websiteID uint) error

	// NotifyAdminWebsiteBroken sends the admin "target invalid / broken"
	// warning email for the given website. Intended for janitor warn-only
	// mode, where it is fired on every run while the target is invalid; no
	// deduplication is performed. No-op when notifications or the admin email
	// are not configured.
	NotifyAdminWebsiteBroken(ctx context.Context, websiteID uint) error

	// NotifyOwnerCIDUnpinned emails the owners of active websites whose
	// target was backed by the given CID at the time it was unpinned. A
	// website is affected when it directly targets the CID (IPFS target) or
	// when it targets an IPNS key whose last published CID is the unpinned
	// CID. Errors from individual emails are logged, not returned; a returned
	// error means the lookup itself failed.
	NotifyOwnerCIDUnpinned(ctx context.Context, cidStr string) error

	// WaitForPublishes blocks until all in-flight async publish operations complete
	WaitForPublishes()

	// WaitForValidations blocks until all in-flight background auto-validation
	// operations complete
	WaitForValidations()
}
