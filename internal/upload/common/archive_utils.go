package common

import (
	"context"
	"fmt"
	"io"

	"github.com/mholt/archives"
	"go.lumeweb.com/ipfs-content/archive"
	contentFs "go.lumeweb.com/ipfs-content/fs"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// GetArchiveDAGSize calculates the actual DAG block size for archives.
// This replaces GetArchiveRawDataSize for more accurate quota validation.
//
// Parameters:
//   - ctx: Context for cancellation
//   - extractor: ArchiveExtractor for the archive to analyze
//   - logger: Logger for error reporting
//
// Returns:
//   - uint64: Total size of all UnixFS blocks in the archive (actual IPFS storage size)
//   - error: Error if archive cannot be processed
func GetArchiveDAGSize(ctx context.Context, extractor archive.ArchiveExtractor, logger *core.Logger) (uint64, error) {
	// Get filesystem from archive extractor
	efs, err := extractor.Filesystem(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get filesystem from extractor: %w", err)
	}

	// Use ipfs-content's GetDAGSizeFromFS to calculate actual UnixFS DAG block size
	// wrapInDir=true for archives (matches UploadProcessor behavior)
	return contentFs.GetDAGSizeFromFS(ctx, efs, true)
}

// GetSingleFileDAGSize calculates the actual DAG block size for a single file.
// This accounts for UnixFS chunking overhead, not just raw file size.
//
// Parameters:
//   - ctx: Context for cancellation
//   - reader: Seekable reader for the file
//   - logger: Logger for error reporting
//
// Returns:
//   - uint64: Total size of all UnixFS blocks for the file (actual IPFS storage size)
//   - error: Error if file cannot be processed
func GetSingleFileDAGSize(ctx context.Context, reader io.ReadSeeker, logger *core.Logger) (uint64, error) {
	// Wrap the single file reader as a filesystem using ipfs-content's SingleFileFS
	fileFS := contentFs.NewSingleFileFSFromReader(reader, "file")

	// Use ipfs-content's GetDAGSizeFromFS to calculate actual UnixFS DAG block size
	// wrapInDir=false for single files (no directory wrapper)
	return contentFs.GetDAGSizeFromFS(ctx, fileFS, false)
}

// GetUploadDataSize calculates the appropriate data size for quota validation.
// For CAR files, it returns the DAG block size (raw block data).
// For archives, it returns the DAG block size (UnixFS chunked).
// For other files, it returns the DAG block size (UnixFS chunked).
//
// Parameters:
//   - ctx: Context for cancellation
//   - reader: Seekable reader for the upload
//   - format: File format detected by archive.DetectFormat
//   - logger: Logger for error reporting
//
// Returns:
//   - uint64: Appropriate data size for quota validation (actual DAG block size)
//   - error: Error if size calculation fails
func GetUploadDataSize(ctx context.Context, reader io.ReadSeeker, format archive.Format, logger *core.Logger) (uint64, error) {
	// Validate reader is seekable
	if _, ok := reader.(io.Seeker); !ok {
		return 0, fmt.Errorf("reader must be seekable (io.ReadSeeker required)")
	}

	// Ensure reader is at the beginning
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek to start: %w", err)
	}

	switch format {
	case archive.FormatCAR:
		// For CAR files, use DAG block size (raw block data)
		dagSize, err := GetCARBlockDAGSizeWithDefaultLimit(ctx, reader, logger)
		if err != nil {
			return 0, err
		}

		// Reset reader to beginning for subsequent operations
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return 0, fmt.Errorf("failed to reset CAR reader position: %w", err)
		}

		return dagSize, nil

	case archive.FormatFile:
		// For plain files, calculate actual DAG size with UnixFS chunking
		dagSize, err := GetSingleFileDAGSize(ctx, reader, logger)
		if err != nil {
			return 0, err
		}

		// Reset reader to beginning for subsequent operations
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return 0, fmt.Errorf("failed to reset file reader position: %w", err)
		}

		return dagSize, nil

	default:
		// For archive formats (ZIP, TAR, RAR, 7Z, etc.), get DAG block size
		if !format.IsArchiveFormat() {
			logger.Warn("Unknown format detected, processing as plain file",
				zap.String("format", format.String()))
			return GetUploadDataSize(ctx, reader, archive.FormatFile, logger)
		}

		seekableReader, ok := reader.(archives.ReaderAtSeeker)
		if !ok {
			return 0, fmt.Errorf("archive reader must implement archives.ReaderAtSeeker")
		}

		extractor, err := archive.CreateExtractor(seekableReader)
		if err != nil {
			logger.Error("Failed to create archive extractor",
				zap.Error(err))
			return 0, fmt.Errorf("failed to create extractor: %w", err)
		}
		defer func(extractor archive.ArchiveExtractor) {
			err := extractor.Close()
			if err != nil {
				logger.Warn("Failed to close archive extractor", zap.Error(err))
			}
		}(extractor)

		// Get DAG block size from archive (UnixFS chunked)
		dagSize, err := GetArchiveDAGSize(ctx, extractor, logger)
		if err != nil {
			return 0, fmt.Errorf("failed to get archive DAG size: %w", err)
		}

		return dagSize, nil
	}
}
