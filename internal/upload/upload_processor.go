package upload

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
	"go.lumeweb.com/portal/core"
	contentArchive "go.lumeweb.com/ipfs-content/archive"
	"go.uber.org/zap"
)

// validateQuotas validates both upload and storage quotas for a file size
func validateQuotas(ctx context.Context, portalCtx core.Context, userID uint, size uint64) error {
	if err := quota.ValidateUploadQuota(ctx, portalCtx, userID, size); err != nil {
		return fmt.Errorf("upload quota validation failed: %w", err)
	}
	if err := quota.ValidateStorageQuota(ctx, portalCtx, userID, size); err != nil {
		return fmt.Errorf("storage quota validation failed: %w", err)
	}
	return nil
}

// FileProcessor implements UploadProcessor for individual files, wrapping them in CAR format
type FileProcessor struct {
	storage       core.StorageService
	ipfs          core.Protocol
	carGenerator  CARGenerator
	storageHelper *common.StorageHelper
	portalCtx     core.Context
	userID        uint
}

// NewFileProcessor creates a new file processor
func NewFileProcessor(storage core.StorageService, ipfs core.Protocol, carGenerator CARGenerator, portalCtx core.Context, userID uint) *FileProcessor {
	return &FileProcessor{
		storage:       storage,
		ipfs:          ipfs,
		carGenerator:  carGenerator,
		storageHelper: common.NewStorageHelper(storage, ipfs),
		portalCtx:     portalCtx,
		userID:        userID,
	}
}

// Process converts a single file to CAR format and stores it
func (p *FileProcessor) Process(ctx context.Context, reader io.ReadSeekCloser) (cid.Cid, string, error) {
	ctx, span := core.TraceMethod(ctx, "FileProcessor.Process")
	defer span.End()

	car, c, err := p.carGenerator.FileToCAR(ctx, reader)
	if err != nil {
		return cid.Cid{}, "", err
	}

	// Get CAR file size for storage purposes
	carFileSize := uint64(car.Len())

	// Get actual DAG block size using ipfs-content ReadCAR API
	// This gives us the raw block data size, not the CAR file size (which includes headers)
	// We need to create a seekable reader from the buffer (bytes.Buffer is not seekable)
	carSeeker := bytes.NewReader(car.Bytes())
	dagSize, err := common.GetCARBlockDAGSizeWithDefaultLimit(ctx, carSeeker, p.portalCtx.Logger())
	if err != nil {
		return cid.Cid{}, "", err
	}

	// Validate quotas using actual DAG block size, not CAR file size
	if err := validateQuotas(ctx, p.portalCtx, p.userID, dagSize); err != nil {
		return cid.Cid{}, "", err
	}

	// Store the file using the CAR file size (for actual storage bytes)
	uploadID, err := p.storageHelper.StoreFile(ctx, NewUniversalReader(car), int64(carFileSize))
	if err != nil {
		return cid.Undef, "", err
	}

	return c, uploadID, nil
}

// CARProcessor implements UploadProcessor for CAR files
type CARProcessor struct {
	storage       core.StorageService
	ipfs          core.Protocol
	storageHelper *common.StorageHelper
	portalCtx     core.Context
	userID        uint
}

// NewCARProcessor creates a new CAR processor
func NewCARProcessor(storage core.StorageService, ipfs core.Protocol, portalCtx core.Context, userID uint) *CARProcessor {
	return &CARProcessor{
		storage:       storage,
		ipfs:          ipfs,
		storageHelper: common.NewStorageHelper(storage, ipfs),
		portalCtx:     portalCtx,
		userID:        userID,
	}
}

// Process validates and stores a CAR file directly
func (p *CARProcessor) Process(ctx context.Context, reader io.ReadSeekCloser) (cid.Cid, string, error) {
	ctx, span := core.TraceMethod(ctx, "CARProcessor.Process")
	defer span.End()

	// Get CAR file size for storage purposes
	fileSize, err := common.PrepareReader(reader)
	if err != nil {
		return cid.Undef, "", err
	}

	// Get actual DAG block size using ipfs-content ReadCAR API
	// This gives us the raw block data size, not the CAR file size (which includes headers)
	dagSize, err := common.GetCARBlockDAGSizeWithDefaultLimit(ctx, reader, p.portalCtx.Logger())
	if err != nil {
		return cid.Undef, "", err
	}

	// Validate quotas using actual DAG block size, not CAR file size
	if err := validateQuotas(ctx, p.portalCtx, p.userID, dagSize); err != nil {
		return cid.Undef, "", err
	}

	roots, err := GetCarRoots(reader, false)
	if err != nil {
		return cid.Undef, "", fmt.Errorf("failed to get CAR roots: %w", err)
	}

	// Reset reader position to start for full file upload
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return cid.Undef, "", fmt.Errorf("failed to reset reader position: %w", err)
	}

	if err := validateCARRoots(roots); err != nil {
		return cid.Undef, "", err
	}

	uploadID, err := p.storageHelper.StoreFile(ctx, reader, fileSize)
	if err != nil {
		return cid.Undef, "", err
	}

	return roots[0], uploadID, nil
}

// validateCARRoots ensures CAR files have exactly one root
func validateCARRoots(roots []cid.Cid) error {
	// CAR files with multiple roots are not currently supported
	if len(roots) > 1 {
		return fmt.Errorf("CAR file has multiple roots, only single root CARs are supported")
	}

	if len(roots) == 0 {
		return fmt.Errorf("CAR file has no roots")
	}

	return nil
}

// ArchiveProcessor implements UploadProcessor for archive formats (ZIP, TAR, etc.)
type ArchiveProcessor struct {
	format        contentArchive.Format
	storage       core.StorageService
	ipfs          core.Protocol
	carGenerator  CARGenerator
	storageHelper *common.StorageHelper
	logger        *core.Logger
	portalCtx     core.Context
	userID        uint
}

// NewArchiveProcessor creates a new archive processor
func NewArchiveProcessor(format contentArchive.Format, storage core.StorageService, ipfs core.Protocol, carGenerator CARGenerator, logger *core.Logger, portalCtx core.Context, userID uint) *ArchiveProcessor {
	return &ArchiveProcessor{
		format:        format,
		storage:       storage,
		ipfs:          ipfs,
		carGenerator:  carGenerator,
		storageHelper: common.NewStorageHelper(storage, ipfs),
		logger:        logger,
		portalCtx:     portalCtx,
		userID:        userID,
	}
}

// Process extracts archive contents and converts to CAR format
func (p *ArchiveProcessor) Process(ctx context.Context, reader io.ReadSeekCloser) (cid.Cid, string, error) {
	ctx, span := core.TraceMethod(ctx, "ArchiveProcessor.Process")
	defer span.End()

	uReader := NewUniversalReader(reader)
	defer common.SafeCloseFile(p.logger, uReader)

	// Get raw data size from archive before conversion
	// This prevents quota bypass by highly compressed archives
	_, err := uReader.Seek(0, io.SeekStart)
	if err != nil {
		return cid.Cid{}, "", fmt.Errorf("failed to seek reader: %w", err)
	}

	extractor, err := contentArchive.CreateExtractor(uReader)
	if err != nil {
		return cid.Cid{}, "", fmt.Errorf("failed to create extractor: %w", err)
	}

	// Get DAG block size for quota validation
	dagSize, err := common.GetArchiveDAGSize(ctx, extractor, p.portalCtx.Logger())
	if err != nil {
		extractor.Close()
		return cid.Cid{}, "", fmt.Errorf("failed to get archive DAG size: %w", err)
	}

	if err := extractor.Close(); err != nil {
		p.logger.Warn("Failed to close initial extractor", zap.Error(err))
	}

	// Validate quotas using DAG block size
	// This accurately reflects IPFS storage costs including UnixFS chunking overhead
	if err := validateQuotas(ctx, p.portalCtx, p.userID, dagSize); err != nil {
		return cid.Cid{}, "", err
	}

	// Reset reader for actual extraction
	_, err = uReader.Seek(0, io.SeekStart)
	if err != nil {
		return cid.Cid{}, "", fmt.Errorf("failed to reset reader: %w", err)
	}

	// Re-create extractor for processing
	extractor, err = contentArchive.CreateExtractor(uReader)
	if err != nil {
		return cid.Cid{}, "", fmt.Errorf("failed to recreate extractor: %w", err)
	}

	defer func() {
		if err := extractor.Close(); err != nil {
			p.logger.Warn("Failed to close extractor", zap.Error(err))
		}
	}()

	car, c, err := p.carGenerator.ArchiveToCAR(ctx, extractor)
	if err != nil {
		return cid.Cid{}, "", err
	}

	// Get CAR file size for storage purposes
	carFileSize := uint64(car.Len())

	// Get actual DAG block size using ipfs-content ReadCAR API
	// This gives us the raw block data size, not the CAR file size (which includes headers)
	// We need to create a seekable reader from the buffer (bytes.Buffer is not seekable)
	carSeeker := bytes.NewReader(car.Bytes())
	_, err = common.GetCARBlockDAGSizeWithDefaultLimit(ctx, carSeeker, p.logger)
	if err != nil {
		return cid.Cid{}, "", err
	}

	// Store the file using the CAR file size (for actual storage bytes)
	uploadID, err := p.storageHelper.StoreFile(ctx, NewUniversalReader(car), int64(carFileSize))
	if err != nil {
		return cid.Undef, "", err
	}

	return c, uploadID, nil
}

// UploadProcessorFactory implements UploadProcessorFactory with format-based routing
type DefaultUploadProcessorFactory struct {
	logger  *core.Logger
	storage core.StorageService
	ipfs    core.Protocol
}

// NewUploadProcessorFactory creates a new processor factory with storage and IPFS services
func NewUploadProcessorFactory(logger *core.Logger, storage core.StorageService, ipfs core.Protocol) *DefaultUploadProcessorFactory {
	return &DefaultUploadProcessorFactory{
		logger:  logger,
		storage: storage,
		ipfs:    ipfs,
	}
}

// CreateProcessor returns the appropriate processor based on file format
func (f *DefaultUploadProcessorFactory) CreateProcessor(format contentArchive.Format, mode ArchiveMode, portalCtx core.Context, userID uint) (UploadProcessor, error) {
	switch format {
	case contentArchive.FormatCAR:
		return NewCARProcessor(f.storage, f.ipfs, portalCtx, userID), nil
	default:
		gen := NewCARGeneratorWithDefaults(f.logger)

		if format.IsArchiveFormat() && mode == ArchiveConvert {
			return NewArchiveProcessor(format, f.storage, f.ipfs, gen, f.logger, portalCtx, userID), nil
		}
		// For non-archive formats, or ArchivePreserve, use FileProcessor which wraps files in CAR
		return NewFileProcessor(f.storage, f.ipfs, gen, portalCtx, userID), nil
	}
}
