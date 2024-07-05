package internal

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/service"
)

func NewIPFSHash(c cid.Cid) core.StorageHash {
	return service.NewStorageHashFromMultihash(c.Hash(), nil)
}

func CIDFromHash(hash []byte) (cid.Cid, error) {
	encode, err := mh.Cast(hash)
	if err != nil {
		return cid.Cid{}, err
	}

	return cid.NewCidV1(cid.DagProtobuf, encode), nil
}
