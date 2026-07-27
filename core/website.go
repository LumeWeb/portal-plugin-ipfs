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
)

type ValidateDNSResult struct {
	Valid   bool
	Message string
	Reason  ValidationReason // machine-readable: "validated", "token_expired", "dns_missing", "dns_mismatch", "token_missing", "delegation_pending"
}

// WebsiteService defines the interface for managing website configurations
type WebsiteService interface {
	core.Service
	core.Configurable

	// CreateWebsite creates a new website configuration
	CreateWebsite(ctx context.Context, website *pluginDb.Website) (*pluginDb.Website, error)

	// GetWebsite retrieves a single website by ID
	GetWebsite(ctx context.Context, userID uint, websiteID uint) (*pluginDb.Website, error)

	// GetWebsiteByDomain retrieves a website by domain name
	GetWebsiteByDomain(ctx context.Context, domain string) (*pluginDb.Website, error)

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

	// UpdateSSLStatus updates the SSL certificate status for a website domain
	UpdateSSLStatus(ctx context.Context, domain string, status pluginDb.SSLStatus, sslError string, timestamp *time.Time) (*pluginDb.Website, error)

	// WaitForPublishes blocks until all in-flight async publish operations complete
	WaitForPublishes()
}
