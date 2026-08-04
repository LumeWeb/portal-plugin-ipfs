package db

import "strings"

// NormalizeDomain lowercases, trims whitespace, and strips a leading "www."
// prefix so that a hostname always maps to its apex form. It is used as the
// single source of truth for domain normalization so website and website_domain
// records are always stored in canonical (apex) form regardless of whether an
// external caller passes a www.-prefixed hostname (e.g. a CDN or certificate
// hostname).
func NormalizeDomain(domain string) string {
	domain = strings.TrimSpace(strings.ToLower(domain))
	if strings.HasPrefix(domain, "www.") {
		return domain[4:]
	}
	return domain
}
