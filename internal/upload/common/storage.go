package common

import (
	"context"
	"fmt"
	"io"

	"go.lumeweb.com/portal/core"
)

// StorageHelper provides common file storage functionality for upload processors
type StorageHelper struct {
	storage core.StorageService
	ipfs    core.Protocol
}

// NewStorageHelper creates a new storage helper instance
func NewStorageHelper(storage core.StorageService, ipfs core.Protocol) *StorageHelper {
	return &StorageHelper{
		storage: storage,
		ipfs:    ipfs,
	}
}

// StoreFile stores a file using the storage service and returns the upload ID
// This method consolidates the identical storeFile logic from FileProcessor, CARProcessor, and ArchiveProcessor
func (sh *StorageHelper) StoreFile(ctx context.Context, reader io.ReadSeekCloser, size int64) (string, error) {
	// Reset the reader for storage (needed for some processors)
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return "", fmt.Errorf("failed to reset reader for storage: %w", err)
	}

	// Store the file using existing storage service
	uploadID, err := sh.storage.S3TemporaryUpload(ctx, reader, uint64(size), sh.ipfs.(core.StorageProtocol))
	if err != nil {
		return "", fmt.Errorf("failed to store file: %w", err)
	}

	return uploadID, nil
}

// PrepareReader gets the file size and resets the reader to the beginning
// This consolidates the prepareReader function from upload_processor.go
func PrepareReader(reader io.ReadSeeker) (int64, error) {
	return PrepareReaderWithPosition(reader, func() (int64, error) {
		return reader.Seek(0, io.SeekEnd)
	})
}