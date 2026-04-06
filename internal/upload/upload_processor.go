package upload

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
	"go.lumeweb.com/portal/core"
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

	// Validate quotas using the CAR file size
	carSize := uint64(car.Len())

	if err := validateQuotas(ctx, p.portalCtx, p.userID, carSize); err != nil {
		return cid.Cid{}, "", err
	}

	uploadID, err := p.storageHelper.StoreFile(ctx, NewUniversalReader(car), int64(car.Len()))
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

	size, err := common.PrepareReader(reader)
	if err != nil {
		return cid.Undef, "", err
	}

	if err := validateQuotas(ctx, p.portalCtx, p.userID, uint64(size)); err != nil {
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

	uploadID, err := p.storageHelper.StoreFile(ctx, reader, size)
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
	format        Format
	storage       core.StorageService
	ipfs          core.Protocol
	carGenerator  CARGenerator
	storageHelper *common.StorageHelper
	logger        *core.Logger
	portalCtx     core.Context
	userID        uint
}

// NewArchiveProcessor creates a new archive processor
func NewArchiveProcessor(format Format, storage core.StorageService, ipfs core.Protocol, carGenerator CARGenerator, logger *core.Logger, portalCtx core.Context, userID uint) *ArchiveProcessor {
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

	extractor, err := CreateExtractor(uReader)
	if err != nil {
		return cid.Cid{}, "", err
	}

	car, c, err := p.carGenerator.ArchiveToCAR(ctx, extractor)
	if err != nil {
		return cid.Cid{}, "", err
	}

	// Validate quotas using the CAR file size
	carSize := uint64(car.Len())

	if err := validateQuotas(ctx, p.portalCtx, p.userID, carSize); err != nil {
		return cid.Cid{}, "", err
	}

	uploadID, err := p.storageHelper.StoreFile(ctx, NewUniversalReader(car), int64(car.Len()))
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
func (f *DefaultUploadProcessorFactory) CreateProcessor(format Format, mode ArchiveMode, portalCtx core.Context, userID uint) (UploadProcessor, error) {
	switch format {
	case FormatCAR:
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
