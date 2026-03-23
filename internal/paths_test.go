package internal

import (
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"github.com/ipfs/boxo/path"
)

func TestTryNormalizeCIDFromPath(t *testing.T) {
	t.Run("v0_to_v1", func(t *testing.T) {
		// Create a v0 CID
		v0Cid, err := cid.Parse("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdD")
		require.NoError(t, err)
		
		// Create a path from the v0 CID
		valuePath := path.FromCid(v0Cid)
		
		// Normalize - should return v1 CID path
		normalized := TryNormalizeCIDFromPath(valuePath)
		
		// Check that it's a v1 CID
		parsedCid, err := cid.Parse(normalized)
		require.NoError(t, err, "Should be able to parse normalized CID")
		require.Equal(t, uint64(1), uint64(parsedCid.Version()), "Should be v1")
	})

	t.Run("v1_cid", func(t *testing.T) {
		// Create a v1 CID
		v1Cid, err := cid.Parse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
		require.NoError(t, err)
		
		// Create a path from the v1 CID
		valuePath := path.FromCid(v1Cid)
		
		// Normalize - should return the same v1 CID path
		normalized := TryNormalizeCIDFromPath(valuePath)
		
		// Check that it's a v1 CID
		parsedCid, err := cid.Parse(normalized)
		require.NoError(t, err, "Should be able to parse normalized CID")
		require.Equal(t, uint64(1), uint64(parsedCid.Version()), "Should be v1")
		
		// Check that it's the same CID
		require.True(t, v1Cid.Equals(parsedCid), "Should be the same CID")
	})
}
