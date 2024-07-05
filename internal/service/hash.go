package service

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"hash"
	"io"
)

type SHAReader struct {
	reader io.ReadCloser
	hash   hash.Hash
	size   int64
	cid    cid.Cid
}

func NewSHAReader(reader io.ReadCloser) *SHAReader {
	return &SHAReader{
		reader: reader,
		hash:   sha256.New(),
	}
}

func (r *SHAReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.hash.Write(p[:n])
		r.size += int64(n)
	}
	return n, err
}

func (r *SHAReader) CID() cid.Cid {
	if !r.cid.Defined() {
		h := r.hash.Sum(nil)
		mh, _ := multihash.Encode(h, multihash.SHA2_256)
		r.cid = cid.NewCidV1(cid.Raw, mh)
	}

	return r.cid
}

func (r *SHAReader) Close() error {
	return r.reader.Close()
}

func (r *SHAReader) Sum() string {
	return hex.EncodeToString(r.hash.Sum(nil))
}
