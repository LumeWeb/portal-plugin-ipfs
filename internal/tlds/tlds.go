// Package tlds provides offline detection of ICANN-gTLD/ccTLD suffixes using
// the IANA "tlds-alpha-by-domain" root zone list, which is embedded at build
// time. It is the authoritative decision procedure for whether a domain name
// belongs to the ICANN namespace (its final label is an IANA-registered TLD)
// versus an alternate root such as HNS.
package tlds

import (
	_ "embed"
	"strings"
)

//go:embed tlds-alpha-by-domain.txt
var tldList []byte

// icann holds every IANA TLD lower-cased for O(1) membership checks.
var icann map[string]struct{}

func init() {
	icann = make(map[string]struct{}, 1400)
	for _, line := range strings.Split(string(tldList), "\n") {
		line = strings.TrimSpace(line)
		// Skip the format header and any blank lines.
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		icann[strings.ToLower(line)] = struct{}{}
	}
}

// IsICANNTld reports whether tld is a single label registered as an ICANN TLD
// in the IANA root zone list. Matching is case-insensitive.
func IsICANNTld(tld string) bool {
	_, ok := icann[strings.ToLower(strings.TrimSpace(tld))]
	return ok
}

// IsICANN reports whether domain's final label (its TLD) is an ICANN TLD. The
// domain is not otherwise validated; callers are expected to have normalized
// it already. Matching on the TLD is case-insensitive.
func IsICANN(domain string) bool {
	domain = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(domain)), ".")
	idx := strings.LastIndex(domain, ".")
	if idx < 0 || idx == len(domain)-1 {
		return false
	}
	_, ok := icann[domain[idx+1:]]
	return ok
}
