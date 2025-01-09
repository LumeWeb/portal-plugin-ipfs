package service

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"hash"
	"io"
	"sync"
)

// SHAReader provides a thread-safe wrapper around an io.ReadSeekCloser that computes
// a SHA256 hash of the content as it's read. It ensures thread-safe access to shared
// resources like the hash state and position tracking.
type SHAReader struct {
	reader io.ReadSeekCloser
	hash   hash.Hash
	size   int64

	mu       sync.Mutex
	cid      cid.Cid
	position int64
	hashDone bool
}

func NewSHAReader(reader io.ReadSeekCloser) (*SHAReader, error) {
	size, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to get size: %w", err)
	}

	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to reset position: %w", err)
	}

	return &SHAReader{
		reader:   reader,
		hash:     sha256.New(),
		size:     size,
		position: 0,
		hashDone: false,
	}, nil
}

func (r *SHAReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n, err := r.reader.Read(p)
	if n > 0 {
		if !r.hashDone {
			r.hash.Write(p[:n])
		}
		r.position += int64(n)
	}
	return n, err
}

func (r *SHAReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.hashDone {
		if err := r.computeFullHashLocked(); err != nil {
			return 0, fmt.Errorf("failed to compute hash: %w", err)
		}
	}

	newPos, err := r.reader.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	r.position = newPos
	return newPos, nil
}

// computeFullHashLocked reads the entire file to compute the hash.
// Caller must hold mu lock.
func (r *SHAReader) computeFullHashLocked() error {
	currentPos, err := r.reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// Ensure we attempt to restore position even if errors occur
	defer func() {
		_, seekErr := r.reader.Seek(currentPos, io.SeekStart)
		if seekErr != nil && err == nil {
			err = seekErr
		}
	}()

	_, err = r.reader.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	data, err := io.ReadAll(r.reader)
	if err != nil {
		return err
	}

	r.hash.Write(data)
	r.hashDone = true
	return nil
}

func (r *SHAReader) CID() (cid.Cid, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.cid.Defined() {
		if !r.hashDone {
			if err := r.computeFullHashLocked(); err != nil {
				return cid.Cid{}, fmt.Errorf("failed to compute hash: %w", err)
			}
		}
		h := r.hash.Sum(nil)
		mh, err := multihash.Encode(h, multihash.SHA2_256)
		if err != nil {
			return cid.Cid{}, fmt.Errorf("failed to encode multihash: %w", err)
		}
		r.cid = cid.NewCidV1(cid.Raw, mh)
	}
	return r.cid, nil
}

func (r *SHAReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reader.Close()
}

func (r *SHAReader) Sum() (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.hashDone {
		if err := r.computeFullHashLocked(); err != nil {
			return "", fmt.Errorf("failed to compute hash: %w", err)
		}
	}
	return hex.EncodeToString(r.hash.Sum(nil)), nil
}
