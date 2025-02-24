package util

import (
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"testing"
)

func GenerateTestCID(t *testing.T, data string) cid.Cid {
	hash, err := multihash.Sum([]byte(data), multihash.SHA2_256, -1)
	require.NoError(t, err)
	testCID := cid.NewCidV1(cid.DagCBOR, hash)
	return testCID
}
