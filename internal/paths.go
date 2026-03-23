package internal

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/boxo/path"

	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
)

const (
	IPFSPathPrefix = "/ipfs/"
	IPNSPathPrefix = "/ipns/"
)

// extractCIDFromPath extracts a CID from an IPNS record path.
// The path should be formatted as /ipfs/{cid}, where the CID is at segment 1.
// Returns an error if the path format is invalid or doesn't start with "ipfs".
func extractCIDFromPath(valuePath path.Path) (string, error) {
	pathSegments := valuePath.Segments()
	if len(pathSegments) < 2 {
		return "", fmt.Errorf("invalid IPNS record path format: expected at least 2 segments, got %d", len(pathSegments))
	}
	if pathSegments[0] != "ipfs" {
		return "", fmt.Errorf("unexpected path protocol: %s", pathSegments[0])
	}
	return pathSegments[1], nil
}

// ExtractCIDFromPathLenient extracts a CID from a path, with lenient fallback.
// If the path is formatted as /ipfs/{cid}, returns the CID from the second segment.
// If the path does not have the /ipfs/ prefix, treats the entire path as a CID string.
// This is useful for validation where we want to be forgiving about path format.
func ExtractCIDFromPathLenient(valuePath path.Path) string {
	cid, err := extractCIDFromPath(valuePath)
	if err != nil {
		return valuePath.String()
	}
	return cid
}

// ExtractCIDFromPathStrict extracts a CID from a path, with strict validation.
// The path must be formatted as /ipfs/{cid}, where the CID is at segment 1.
// Returns an error if the path format is invalid or doesn't start with "ipfs".
// Use this when path format errors should be propagated.
func ExtractCIDFromPathStrict(valuePath path.Path) (string, error) {
	return extractCIDFromPath(valuePath)
}

// TryNormalizeCIDFromPath attempts to normalize a CID from a path.
// If the path contains a valid CID, it will be normalized to v1 format.
// If the path does not contain a valid CID, the original path string is returned unchanged.
// This is useful when you want to normalize CIDs when possible but handle non-CID paths gracefully.
func TryNormalizeCIDFromPath(valuePath path.Path) string {
	// Try to extract CID string from the path
	cidStr := ExtractCIDFromPathLenient(valuePath)
	
	// Try to parse it as a CID
	parsedCid, err := cid.Parse(cidStr)
	if err != nil {
		// Not a valid CID, return the original path unchanged
		return valuePath.String()
	}

	// Normalize the CID
	normalizedCid := encoding.NormalizeCid(parsedCid)
	if normalizedCid == cid.Undef {
		// Unsupported CID version, return original path unchanged
		return valuePath.String()
	}

	// Create a new path from the normalized CID
	return path.FromCid(normalizedCid).String()
}
