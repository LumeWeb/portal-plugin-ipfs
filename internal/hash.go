package internal

import (
	"fmt"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"go.lumeweb.com/portal/core"
)

func NewIPFSHash(c cid.Cid) core.StorageHash {
	return core.NewStorageHashFromMultihash(c.Hash(), c.Type(), nil)
}

func CIDFromHash(hash []byte, cidType uint64) (cid.Cid, error) {
	encode, err := mh.Cast(hash)
	if err != nil {
		return cid.Cid{}, err
	}

	return cid.NewCidV1(cidType, encode), nil
}
func CIDFromStorageHash(hash core.StorageHash) (cid.Cid, error) {
	return CIDFromHash(hash.Multihash(), hash.CIDType())
}

func RegisterHashes() {
	hashAlgos := []struct {
		code     uint64
		priority int
	}{
		{mh.SHA2_256, 0},         // Default IPFS hash
		{mh.SHA1, 1},             // Legacy support
		{mh.BLAKE2B_MIN + 32, 0}, // Blake2b-256
		{mh.SHA3_256, 0},         // SHA3-256
	}

	for _, algo := range hashAlgos {
		err := core.GetHashRegistry().RegisterHashAlgorithm(core.HashAlgorithm{
			Type:        algo.code,
			Name:        mh.Codes[algo.code],
			Priority:    algo.priority,
			Protocol:    ProtocolName,
			NewVerifier: nil,
		})
		if err != nil {
			panic(fmt.Sprintf("failed to register %s hash algorithm (code %d): %v", mh.Codes[algo.code], algo.code, err))
		}
	}
}
