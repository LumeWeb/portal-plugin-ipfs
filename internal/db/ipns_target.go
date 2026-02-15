package db

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
)

// IPNSTarget wraps a multihash with IPNS-specific helpers
type IPNSTarget mh.Multihash

// NewIPNSTargetFromPeerID creates an IPNSTarget from a peer.ID
func NewIPNSTargetFromPeerID(pid peer.ID) (IPNSTarget, error) {
	mh := mh.Multihash(pid)
	if mh == nil {
		return IPNSTarget{}, fmt.Errorf("invalid peer ID")
	}
	return IPNSTarget(mh), nil
}

// NewIPNSTargetFromString creates an IPNSTarget from a peer ID string or CIDv1 with libp2p-key codec
func NewIPNSTargetFromString(s string) (IPNSTarget, error) {
	// First try as peer ID string (base36)
	pid, err := peer.Decode(s)
	if err == nil {
		return IPNSTarget(mh.Multihash(pid)), nil
	}

	// Then try as CID (might be CIDv1 with libp2p-key)
	c, err := cid.Decode(s)
	if err == nil {
		if c.Type() == cid.Libp2pKey {
			return IPNSTarget(c.Hash()), nil
		}
		return IPNSTarget{}, fmt.Errorf("CID must be libp2p-key codec for IPNS target")
	}

	return IPNSTarget{}, fmt.Errorf("invalid IPNS target: %w", err)
}

// NewIPNSTargetFromMultihash creates an IPNSTarget directly from a multihash
func NewIPNSTargetFromMultihash(m mh.Multihash) (IPNSTarget, error) {
	if m == nil {
		return IPNSTarget{}, fmt.Errorf("multihash cannot be nil")
	}
	return IPNSTarget(m), nil
}

// PeerID returns the peer ID
func (t IPNSTarget) PeerID() peer.ID {
	return peer.ID(t)
}

// IPNSName returns the IPNS name (CIDv1 with libp2p-key codec)
func (t IPNSTarget) IPNSName() string {
	c := cid.NewCidV1(cid.Libp2pKey, mh.Multihash(t))
	return c.String()
}

// String returns the peer ID in base36 format
func (t IPNSTarget) String() string {
	return t.PeerID().String()
}

// ToMultihash returns the underlying multihash
func (t IPNSTarget) ToMultihash() mh.Multihash {
	return mh.Multihash(t)
}

// Bytes returns the raw multihash bytes
func (t IPNSTarget) Bytes() []byte {
	return []byte(t)
}

// IsValid checks if the target is valid
func (t IPNSTarget) IsValid() bool {
	if t == nil || len(t) == 0 {
		return false
	}
	_, err := mh.Decode(mh.Multihash(t))
	return err == nil
}
