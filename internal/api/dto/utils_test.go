package dto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lumeweb.com/ipfs-content/paths"
)

// TestIPFSPath_NoDoublePrefix guards against the /ipfs/ipfs/<cid> doubling
// bug: resolve's normalized value already carries the /ipfs/ prefix
// (TryNormalizeCIDFromPath returns a full path), so IPFSPath must strip it
// before prepending, never produce a doubled prefix.
func TestIPFSPath_NoDoublePrefix(t *testing.T) {
	cid := "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"

	// Bare CID input: prefix added once (regression: was already correct).
	assert.Equal(t, paths.IPFSPathPrefix+cid, IPFSPath(cid))
	assert.Equal(t, paths.IPFSPathPrefix+cid, IPFSPath(paths.IPFSPathPrefix+cid))
	assert.Equal(t, paths.IPFSPathPrefix+cid, IPFSPath(paths.IPFSPathPrefix+"/"+cid))

	// A prefixed value with a sub-path is preserved intact after trimming.
	sub := paths.IPFSPathPrefix + cid + "/some/file.txt"
	assert.Equal(t, sub, IPFSPath(sub))
}

// TestIPNSPath_NoDoublePrefix mirrors the IPFSPath guard for IPNS peer IDs,
// which are always bare (no prefix expected) but must not double either.
func TestIPNSPath_NoDoublePrefix(t *testing.T) {
	peer := "12D3KooWJv8RMQd2Q2XrboA6XP2qKqJpRrYgX5e5y9QVA"
	assert.Equal(t, paths.IPNSPathPrefix+peer, IPNSPath(peer))
	assert.Equal(t, paths.IPNSPathPrefix+peer, IPNSPath(paths.IPNSPathPrefix+peer))
}
