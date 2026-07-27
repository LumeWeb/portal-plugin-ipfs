package domain

import (
	"strings"
)

// NormalizeDomain lowercases, trims whitespace, strips www. prefix
func NormalizeDomain(domain string) string {
	domain = strings.TrimSpace(strings.ToLower(domain))
	if strings.HasPrefix(domain, "www.") {
		return domain[4:]
	}
	return domain
}
