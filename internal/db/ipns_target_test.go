package db

import (
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIPNSTargetFromString_ValidPeerID(t *testing.T) {
	target, err := NewIPNSTargetFromString("12D3KooWRhWS6DXi1U1YnJ5r9E6KpSDHGbZAznXif4T9qDjHeEfE")
	require.NoError(t, err)
	assert.NotNil(t, target)
}

func TestNewIPNSTargetFromString_CIDv1Libp2pKey(t *testing.T) {
	target, err := NewIPNSTargetFromString("k51qzi5uqu5dlts3p5vfpw8kneqp5ye1ttb2jlt8qkt5mq9f2gvgmet6sec29r")
	require.NoError(t, err)
	assert.NotNil(t, target)
}

func TestNewIPNSTargetFromString_CIDv0_Rejected(t *testing.T) {
	// CIDv0 (Qm...) accidentally passes peer.Decode since both use base58btc
	// multihash encoding, but a content hash is not a peer ID.
	_, err := NewIPNSTargetFromString("QmWLqGsc1X914yZjFgqZ16uzPV69AZjrc4ioMemMhoHWee")
	require.Error(t, err, "CIDv0 should be rejected as IPNS target")
	assert.Contains(t, err.Error(), "not a peer ID")
}

func TestNewIPNSTargetFromString_CIDv1NonLibp2pKey_Rejected(t *testing.T) {
	hash := mustMultihash(t, "test-cidv1")
	c := cid.NewCidV1(cid.DagProtobuf, hash)
	_, err := NewIPNSTargetFromString(c.String())
	require.Error(t, err, "CIDv1 with non-libp2p-key codec should be rejected")
	assert.Contains(t, err.Error(), "not a peer ID")
}

func TestNewIPNSTargetFromString_InvalidString(t *testing.T) {
	_, err := NewIPNSTargetFromString("not-a-valid-peer-id")
	require.Error(t, err)
}

func mustMultihash(t *testing.T, data string) mh.Multihash {
	t.Helper()
	h, err := mh.Sum([]byte(data), mh.SHA2_256, -1)
	require.NoError(t, err)
	return h
}
