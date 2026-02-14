package db

import (
	"fmt"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// IPFSTarget wraps a multihash with IPFS-specific helpers
type IPFSTarget struct {
	Multihash mh.Multihash
	Version   uint8 // 0 = CIDv0, 1 = CIDv1
}

// NewIPFSTargetFromCID creates an IPFSTarget from a CID
func NewIPFSTargetFromCID(c cid.Cid) (IPFSTarget, error) {
	if c == cid.Undef {
		return IPFSTarget{}, fmt.Errorf("invalid CID")
	}
	version := uint8(c.Version())
	return IPFSTarget{
		Multihash: c.Hash(),
		Version:   version,
	}, nil
}

// NewIPFSTargetFromString creates an IPFSTarget from a CID string
func NewIPFSTargetFromString(s string) (IPFSTarget, error) {
	c, err := cid.Decode(s)
	if err != nil {
		return IPFSTarget{}, fmt.Errorf("failed to decode CID: %w", err)
	}
	return NewIPFSTargetFromCID(c)
}

// NewIPFSTargetFromMultihash creates an IPFSTarget directly from a multihash and version
func NewIPFSTargetFromMultihash(m mh.Multihash, version uint8) (IPFSTarget, error) {
	if m == nil {
		return IPFSTarget{}, fmt.Errorf("multihash cannot be nil")
	}
	if version != 0 && version != 1 {
		return IPFSTarget{}, fmt.Errorf("CID version must be 0 or 1")
	}
	return IPFSTarget{
		Multihash: m,
		Version:   version,
	}, nil
}

// CID returns the full CID
func (t IPFSTarget) CID() cid.Cid {
	if t.Version == 0 {
		return cid.NewCidV0(t.Multihash)
	}
	// Default to raw codec for IPFS content
	return cid.NewCidV1(cid.Raw, t.Multihash)
}

// String returns the CID string representation
func (t IPFSTarget) String() string {
	return t.CID().String()
}

// ToMultihash returns the underlying multihash
func (t IPFSTarget) ToMultihash() mh.Multihash {
	return t.Multihash
}

// Bytes returns the raw multihash bytes
func (t IPFSTarget) Bytes() []byte {
	return []byte(t.Multihash)
}

// IsValid checks if the target is valid
func (t IPFSTarget) IsValid() bool {
	if t.Multihash == nil || len(t.Multihash) == 0 {
		return false
	}
	if t.Version != 0 && t.Version != 1 {
		return false
	}
	_, err := mh.Decode(t.Multihash)
	return err == nil
}
