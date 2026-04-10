package protocol

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/mholt/archives"
	"github.com/samber/lo"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/core"
	contentArchive "go.lumeweb.com/ipfs-content/archive"
	contentCar "go.lumeweb.com/ipfs-content/car"
	contentFs "go.lumeweb.com/ipfs-content/fs"
	"go.uber.org/zap"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pluginUpload "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
)

// handleTUSUpload is the main handler for TUS upload operations
func handleTUSUpload(p core.Protocol) func(ctx context.Context, helper core.OperationHelper, request *models.Request, tsReq *models.TUSRequest) error {
	return func(ctx context.Context, helper core.OperationHelper, request *models.Request, tsReq *models.TUSRequest) error {
		ctx, span := core.TraceMethod(ctx, "IPFS.handleTUSUpload")
		defer span.End()

		// Validate user ID
		if err := validateUserID(request); err != nil {
			return err
		}

		// Get TUS handler
		tusHandler, proto, err := setupTUSHandler(p)
		if err != nil {
			return err
		}

		// Get upload size for quota check
		uploadSize, err := getUploadSize(ctx, tusHandler, proto, tsReq.TUSUploadID)
		if err != nil {
			return err
		}

		// Get upload reader for processing
		originalReader, reader, err := getUploadReader(ctx, tusHandler, proto, tsReq.TUSUploadID)
		if err != nil {
			return err
		}
		defer func() {
			if originalReader == nil {
				return
			}
			if err := originalReader.Close(); err != nil {
				helper.Logger().Error("Failed to close upload reader", zap.Error(err))
			}
		}()

		// Detect format
		uploadedFormat, err := detectUploadFormat(reader.(io.ReadSeeker))
		if err != nil {
			return err
		}

		// Calculate DAG size for quota checks
		dagSize, err := calculateDAGSize(ctx, uploadedFormat, uploadSize, reader.(io.ReadSeeker), helper.Logger())
		if err != nil {
			return err
		}

		// Validate quotas
		if err := validateUploadQuotas(helper.Context(), *request.UserID, dagSize); err != nil {
			return err
		}

		// Create processor
		protoNode := p.(ProtoNode)
		processor, err := createUploadProcessor(uploadedFormat, reader, protoNode, helper.Logger())
		if err != nil {
			return err
		}
		defer processor.Release()

		// Process blocks and create reservations
		allCids, rootCids, reservations, err := processUploadAndCreateReservations(ctx, helper, processor, protoNode, *request.UserID)
		if err != nil {
			return err
		}

		// Process with services
		metaStore := protoNode.GetMetadataStore()
		ipfsPin, err := processUploadWithServices(ctx, helper, p, allCids, rootCids, *request.UserID, reservations, request, metaStore)
		if err != nil {
			return err
		}

		// Update workflow data
		workflowData, err := updateWorkflowDataForTUSUpload(helper, request.ID, rootCids, ipfsPin)
		if err != nil {
			return err
		}

		// Validate DAG
		if err := validateDAGForTUSUpload(ctx, helper, request.ID, ipfsPin, workflowData); err != nil {
			return err
		}

		return nil
	}
}

// validateUserID validates that a user ID is present and valid
func validateUserID(request *models.Request) error {
	if request.UserID == nil || *request.UserID == 0 {
		return fmt.Errorf("user ID is required")
	}
	return nil
}

// setupTUSHandler sets up the TUS handler and storage protocol
func setupTUSHandler(p core.Protocol) (core.TusHandler, core.StorageProtocol, error) {
	apiName := p.Name()
	api := core.GetAPI(apiName)

	tusProto, ok := api.(core.APITusHandler)
	if !ok {
		return nil, nil, fmt.Errorf("API %T does not implement core.APITusHandler", api)
	}

	tusHandler := tusProto.GetTusHandler()
	proto := p.(core.StorageProtocol)

	return tusHandler, proto, nil
}

// getUploadSize retrieves the upload size for quota check
func getUploadSize(ctx context.Context, tusHandler core.TusHandler, proto core.StorageProtocol, uploadID string) (uint64, error) {
	size, err := tusHandler.UploadSize(ctx, proto, uploadID)
	if err != nil {
		return 0, fmt.Errorf("failed to get upload size: %w", err)
	}
	return size, nil
}

// getUploadReader retrieves the upload reader for processing
func getUploadReader(ctx context.Context, tusHandler core.TusHandler, proto core.StorageProtocol, uploadID string) (io.ReadCloser, io.ReadCloser, error) {
	reader, err := tusHandler.UploadReader(ctx, uploadID, proto, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get upload reader: %w", err)
	}

	universalReader := pluginUpload.NewUniversalReader(reader)
	return reader, universalReader, nil
}

// detectUploadFormat detects the format of an upload using the reader
func detectUploadFormat(reader io.ReadSeeker) (contentArchive.Format, error) {
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return contentArchive.FormatFile, fmt.Errorf("failed to seek to beginning for format detection: %w", err)
	}

	format, err := contentArchive.DetectFormat(reader)
	if err != nil {
		return contentArchive.FormatFile, fmt.Errorf("failed to detect upload format: %w", err)
	}

	return format, nil
}

// calculateDAGSize calculates the appropriate size for quota checks based on the format
func calculateDAGSize(ctx context.Context, format contentArchive.Format, uploadSize uint64, reader io.ReadSeeker, logger *core.Logger) (uint64, error) {
	if format == contentArchive.FormatCAR {
		return calculateCARSize(ctx, reader, logger)
	} else if format.IsArchiveFormat() {
		return calculateArchiveSize(ctx, reader, logger)
	} else {
		// For non-CAR, non-archive files, calculate actual DAG size
		// using ipfs-content's GetDAGSizeFromFS with SingleFileFS wrapper
		return calculateSingleFileSize(ctx, reader, logger)
	}
}

// calculateCARSize calculates the DAG block size for CAR files
func calculateCARSize(_ context.Context, reader io.ReadSeeker, logger *core.Logger) (uint64, error) {
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to beginning for quota calculation: %w", err)
	}

	// Read CAR and reconstruct tree structure using ipfs-content directly
	// ReadCAR will index blocks and calculate actual DAG block sizes with bounded memory
	summary, err := contentCar.ReadCAR(context.Background(), reader, contentCar.DefaultMemoryLimit)
	if err != nil {
		return 0, fmt.Errorf("failed to read CAR for quota check: %w", err)
	}

	// Reset reader position for subsequent processing
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("failed to reset reader position: %w", err)
	}

	return summary.TotalSize, nil
}

// calculateArchiveSize calculates the actual DAG block size for archives
func calculateArchiveSize(ctx context.Context, reader io.ReadSeeker, logger *core.Logger) (uint64, error) {
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to beginning for quota calculation: %w", err)
	}

	seekableReader, ok := reader.(archives.ReaderAtSeeker)
	if !ok {
		return 0, fmt.Errorf("archive reader must implement archives.ReaderAtSeeker for quota validation")
	}

	extractor, err := contentArchive.CreateExtractor(seekableReader)
	if err != nil {
		return 0, fmt.Errorf("failed to create archive extractor for quota check: %w", err)
	}

	// Get filesystem from archive extractor
	efs, err := extractor.Filesystem(ctx)
	if err != nil {
		extractor.Close()
		return 0, fmt.Errorf("failed to get filesystem from extractor: %w", err)
	}

	// Use ipfs-content's GetDAGSizeFromFS to calculate actual UnixFS DAG block size
	// This accounts for UnixFS chunking overhead, not just uncompressed file sizes
	dagSize, err := contentFs.GetDAGSizeFromFS(ctx, efs, true)
	if err != nil {
		extractor.Close()
		return 0, fmt.Errorf("failed to get archive DAG size for quota check: %w", err)
	}

	if err := extractor.Close(); err != nil {
		logger.Warn("Failed to close archive extractor", zap.Error(err))
	}

	// Reset reader position for subsequent processing
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("failed to reset reader position: %w", err)
	}

	return dagSize, nil
}

// calculateSingleFileSize calculates the actual DAG block size for a single file
// This accounts for UnixFS chunking overhead, not just raw file size
func calculateSingleFileSize(ctx context.Context, reader io.ReadSeeker, logger *core.Logger) (uint64, error) {
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to beginning for quota calculation: %w", err)
	}

	// Wrap the single file reader as a filesystem using ipfs-content's SingleFileFS
	fileFS := contentFs.NewSingleFileFSFromReader(reader, "file")

	// Use ipfs-content's GetDAGSizeFromFS to calculate actual UnixFS DAG block size
	// wrapInDir=false for single files (no directory wrapper)
	dagSize, err := contentFs.GetDAGSizeFromFS(ctx, fileFS, false)
	if err != nil {
		return 0, fmt.Errorf("failed to calculate single file DAG size for quota check: %w", err)
	}

	// Reset reader position for subsequent processing
	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("failed to reset reader position: %w", err)
	}

	return dagSize, nil
}

// validateUploadQuotas validates upload and storage quotas for the given size
func validateUploadQuotas(coreCtx core.Context, userID uint, dagSize uint64) error {
	if dagSize == 0 {
		return nil
	}

	ctx := context.Background()
	err := quota.ValidateUploadQuota(ctx, coreCtx, userID, dagSize)
	if err != nil {
		return fmt.Errorf("upload quota validation failed: %w", err)
	}

	err = quota.ValidateStorageQuota(ctx, coreCtx, userID, dagSize)
	if err != nil {
		return fmt.Errorf("storage quota validation failed: %w", err)
	}

	return nil
}

// createUploadProcessor creates the appropriate processor based on the upload format
func createUploadProcessor(format contentArchive.Format, reader io.ReadCloser, proto ProtoNode, logger *core.Logger) (BlockProcessor, error) {
	if format.IsUploadFormat() {
		// CAR format
		return NewCARBlockProcessor(reader)
	}

	// Single file format (archives treated as files, not extracted)
	return createFileProcessorForTUS(reader, proto, logger)
}

// processUploadAndCreateReservations processes the upload and creates block reservations
func processUploadAndCreateReservations(ctx context.Context, helper core.OperationHelper, processor BlockProcessor, proto ProtoNode, userID uint) ([]cid.Cid, []cid.Cid, map[cid.Cid]*quota.BlockReservations, error) {
	allCids, rootCids, err := ProcessBlocks(helper.Context(), processor)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to process upload: %w", err)
	}

	reservations, err := CreatePerBlockReservations(ctx, helper.Context(), proto, allCids, userID)
	if err != nil {
		return nil, nil, nil, err
	}

	return allCids, rootCids, reservations, nil
}

// processUploadWithServices processes the upload using upload and pin services
func processUploadWithServices(ctx context.Context, helper core.OperationHelper, p core.Protocol, allCids []cid.Cid, rootCids []cid.Cid, userID uint, reservations map[cid.Cid]*quota.BlockReservations, request *models.Request, metaStore pluginCore.MetadataStore) (*pluginDb.IPFSPin, error) {
	uploadSvc := core.GetService[pluginCore.UploadService](helper.Context(), pluginCore.UPLOAD_SERVICE)
	if uploadSvc == nil {
		helper.Logger().Error("Upload service not available")
		quota.ReleaseBlockReservationsMap(reservations)
		return nil, fmt.Errorf("upload service not available")
	}

	// Set client IP in context for quota tracking
	ctx = pc.ClientIPOption(ctx, request.SourceIP)

	err := uploadSvc.ProcessUpload(ctx, allCids, userID, reservations)
	if err != nil {
		quota.ReleaseBlockReservationsMap(reservations)
		return nil, fmt.Errorf("failed to process upload: %w", err)
	}

	// Fix any UnixFS metadata gaps before proceeding
	if metaStore != nil {
		err = metaStore.ProcessMissingUnixFSNames(ctx, allCids)
		if err != nil {
			helper.Logger().Warn("Failed to process missing UnixFS names", zap.Error(err))
		}
	}

	// Create IPFS pin record for the root CID
	ipfsPin, err := uploadSvc.CreateRootPin(ctx, rootCids[0], userID)
	if err != nil {
		return nil, fmt.Errorf("failed to create root pin: %w", err)
	}

	// Update pin status to pinned
	pinSvc := core.GetService[pluginCore.IPFSPinService](helper.Context(), pluginCore.PIN_SERVICE)
	if pinSvc == nil {
		return nil, fmt.Errorf("pin service not available: cannot update pin status")
	}

	err = pinSvc.UpdatePinStatus(ctx, ipfsPin.RequestID, pluginDb.PinningStatusPinned, nil)
	if err != nil {
		helper.Logger().Error("Failed to update pin status to pinned", zap.Error(err))
		// Don't fail the whole operation for this
	}

	return ipfsPin, nil
}

// updateWorkflowDataForTUSUpload updates workflow data for the upload
func updateWorkflowDataForTUSUpload(helper core.OperationHelper, requestID uint, rootCids []cid.Cid, ipfsPin *pluginDb.IPFSPin) (*PinWorkflowData, error) {
	workflowData := &PinWorkflowData{
		PinRequestID: ipfsPin.RequestID.ToUUID(),
		Cids:         cidSliceToStringSlice(rootCids),
	}

	err := helper.UpdateWorkflowDataStruct(requestID, workflowData)
	if err != nil {
		return nil, fmt.Errorf("failed to update workflow data: %w", err)
	}

	return workflowData, nil
}

// validateDAGForTUSUpload validates DAG completion and updates workflow data
func validateDAGForTUSUpload(ctx context.Context, helper core.OperationHelper, requestID uint, ipfsPin *pluginDb.IPFSPin, workflowData *PinWorkflowData) error {
	err := ValidateDAGCompletionAndUpdateWorkflow(ctx, helper, requestID, ipfsPin, workflowData)
	if err != nil {
		helper.Logger().Error("Failed to validate DAG completion and update workflow", zap.Error(err))
		// Don't fail the whole operation for DAG validation failure
		return nil
	}

	return err
}

// cidSliceToStringSlice converts a slice of CIDs to a slice of strings
func cidSliceToStringSlice(cids []cid.Cid) []string {
	return lo.Map(cids, func(item cid.Cid, _ int) string { return item.String() })
}
