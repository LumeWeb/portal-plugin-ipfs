package encoding

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func TestToV1(t *testing.T) {
	// Test case 1: v0 CID to v1 CID conversion
	data := []byte("test data for v0 cid")
	hash, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("Failed to create multihash: %v", err)
	}
	v0Cid := cid.NewCidV0(hash)

	v1Cid := ToV1(v0Cid)
	if v1Cid.Version() != 1 {
		t.Errorf("Expected v1 CID, got version: %d", v1Cid.Version())
	}

	// Test case 2: v1 CID should remain unchanged
	data = []byte("hello world")
	hash, err = mh.Sum(data, mh.SHA3_256, -1)
	require.NoError(t, err, "Failed to create multihash")
	v1CidOriginal := cid.NewCidV1(cid.Raw, hash)

	v1CidResult := ToV1(v1CidOriginal)
	if v1CidResult != v1CidOriginal {
		t.Errorf("Expected v1 CID to remain unchanged, got: %v, expected: %v", v1CidResult, v1CidOriginal)
	}

	// Test case 3: Unsupported CID version
	unsupportedCid := cid.Undef
	resultCid := ToV1(unsupportedCid)
	if resultCid != cid.Undef {
		t.Errorf("Expected Undef CID for unsupported version, got: %v", resultCid)
	}
}

func TestNormalizeCid(t *testing.T) {
	// Test case 1: v0 CID normalization
	data := []byte("test data for v0 cid")
	hash, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		t.Fatalf("Failed to create multihash: %v", err)
	}
	v0Cid := cid.NewCidV0(hash)

	normalizedCid := NormalizeCid(v0Cid)
	if normalizedCid.Version() != 1 {
		t.Errorf("Expected v1 CID after normalization, got version: %d", normalizedCid.Version())
	}

	// Test case 2: v1 CID should remain unchanged after normalization
	data = []byte("hello world")
	hash, err = mh.Sum(data, mh.SHA3_256, -1)
	require.NoError(t, err)
	v1CidOriginal := cid.NewCidV1(cid.Raw, hash)

	normalizedCid = NormalizeCid(v1CidOriginal)
	if normalizedCid != v1CidOriginal {
		t.Errorf("Expected v1 CID to remain unchanged after normalization, got: %v, expected: %v", normalizedCid, v1CidOriginal)
	}
}

func computeCid(data []byte) (cid.Cid, error) {
	hash, err := mh.Sum(data, mh.SHA3_256, -1)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, hash), nil
}

func TestComputeCid(t *testing.T) {
	data := []byte("hello world")
	expectedCid, err := computeCid(data)
	if err != nil {
		t.Fatalf("Failed to compute CID: %v", err)
	}

	// You can add assertions here to validate the computed CID
	// For example, you can check its version, type, and multihash.
	if expectedCid.Version() != 1 {
		t.Errorf("Expected CID version 1, got: %d", expectedCid.Version())
	}

	if expectedCid.Type() != cid.Raw {
		t.Errorf("Expected CID type Raw, got: %d", expectedCid.Type())
	}

	expectedHash, _ := mh.Sum(data, mh.SHA3_256, -1)
	if !bytes.Equal(expectedCid.Hash(), expectedHash) {
		t.Errorf("Expected hash %x, got: %x", expectedHash, expectedCid.Hash())
	}
}
