package domain

import (
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// NormalizeDomain lowercases, trims whitespace, and strips a leading "www."
// prefix. It delegates to db.NormalizeDomain, the single source of truth, so
// all callers share one normalization implementation.
func NormalizeDomain(domain string) string {
	return db.NormalizeDomain(domain)
}
