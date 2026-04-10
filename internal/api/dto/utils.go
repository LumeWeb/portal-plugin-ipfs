package dto

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/multiformats/go-multiaddr"
	"go.lumeweb.com/ipfs-content/paths"
)

// IPFSPath creates a properly formatted IPFS path from a CID string
func IPFSPath(cid string) string {
	return paths.IPFSPathPrefix + trimPath(cid)
}

// IPNSPath creates a properly formatted IPNS path from a peer ID string
func IPNSPath(peerID string) string {
	return paths.IPNSPathPrefix + trimPath(peerID)
}

// trimPath defensively trims leading and trailing slashes from path components
func trimPath(s string) string {
	return strings.Trim(s, "/")
}

// jsonToMap converts datatypes.JSON to map[string]string
func jsonToMap(jsonData []byte) (map[string]string, error) {
	var result map[string]string
	if len(jsonData) > 0 {
		err := json.Unmarshal(jsonData, &result)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal json: %w", err)
		}
	}
	return result, nil
}

// jsonToStringSlice converts datatypes.JSON to []string
func jsonToStringSlice(jsonData []byte) ([]string, error) {
	var result []string
	if len(jsonData) > 0 {
		err := json.Unmarshal(jsonData, &result)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal json: %w", err)
		}
	}
	return result, nil
}

// parseAndDeduplicateDelegates converts delegate strings to multiaddrs and removes duplicates
func parseAndDeduplicateDelegates(delegateStrings []string) []multiaddr.Multiaddr {
	// Parse delegates as multiaddrs and deduplicate
	parsedDelegates := make([]multiaddr.Multiaddr, 0, len(delegateStrings))
	for _, delegateStr := range delegateStrings {
		delegate, err := multiaddr.NewMultiaddr(delegateStr)
		if err != nil {
			continue // Skip invalid multiaddrs
		}
		parsedDelegates = append(parsedDelegates, delegate)
	}

	// Deduplicate
	uniqueDelegates := make([]multiaddr.Multiaddr, 0, len(parsedDelegates))
	seen := make(map[string]bool)

	for _, delegate := range parsedDelegates {
		delegateStr := delegate.String()
		if !seen[delegateStr] {
			seen[delegateStr] = true
			uniqueDelegates = append(uniqueDelegates, delegate)
		}
	}

	return uniqueDelegates
}