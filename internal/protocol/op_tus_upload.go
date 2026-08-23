package protocol

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	contentArchive "go.lumeweb.com/ipfs-content/archive"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	pluginUpload "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	uploadCommon "go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
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
		dagSize, err := calculateDAGSize(ctx, uploadedFormat, reader.(io.ReadSeeker), helper.Logger())
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
			// processUploadWithServices releases reservations on all internal error paths.
			// Calling ReleaseBlockReservationsMap here would double-release.
			return err
		}
		// From this point on, reservations must be released on all paths
		// (including errors from SetHashById, updateWorkflowDataForTUSUpload,
		// and startTUSFilePathWorkflow) since processUploadWithServices
		// only releases on its own internal failures.
		defer quota.ReleaseBlockReservationsMap(reservations)

		if err := tusHandler.SetHashById(ctx, tsReq.TUSUploadID, internal.NewIPFSHash(rootCids[0])); err != nil {
			helper.Logger().Error("Failed to set upload hash", zap.Error(err))
		}

		// Update workflow data
		if _, err := updateWorkflowDataForTUSUpload(helper, request.ID, rootCids, ipfsPin); err != nil {
			return err
		}

		if err := startTUSFilePathWorkflow(ctx, helper, request, allCids, ipfsPin); err != nil {
			helper.Logger().Warn("Failed to start filepath workflow", zap.Error(err))
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

// getUploadReader retrieves the upload reader for processing
func getUploadReader(ctx context.Context, tusHandler core.TusHandler, proto core.StorageProtocol, uploadID string) (io.ReadCloser, io.ReadCloser, error) {
	reader, err := tusHandler.UploadReader(ctx, uploadID, proto, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get upload reader: %w", err)
	}

	// The TUS upload reader already implements io.ReadSeekCloser (via TUSUploadReader),
	// so wrapping it in UniversalReader would buffer the entire file into memory.
	// Use NewSeekableReader which detects seekable readers and passes them through.
	seekableReader := pluginUpload.NewSeekableReader(reader)
	return reader, seekableReader, nil
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
func calculateDAGSize(ctx context.Context, format contentArchive.Format, reader io.ReadSeeker, logger *core.Logger) (uint64, error) {
	return uploadCommon.GetUploadDataSize(ctx, reader, format, logger)
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

	if format.IsArchiveFormat() {
		return createArchiveProcessorForTUS(reader, format, proto, logger)
	}

	// Single file format
	return createFileProcessorForTUS(reader, proto, logger)
}

// processUploadAndCreateReservations processes the upload and creates block reservations
func processUploadAndCreateReservations(ctx context.Context, helper core.OperationHelper, processor BlockProcessor, proto ProtoNode, userID uint) ([]cid.Cid, []cid.Cid, map[cid.Cid]*quota.BlockReservations, error) {
	allCids, rootCids, err := ProcessBlocks(helper.Context(), ctx, processor, proto.GetBlockstoreFlusher())
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

	// Create IPFS pin record for the root CID
	ipfsPin, err := uploadSvc.CreateRootPin(ctx, rootCids[0], userID)
	if err != nil {
		quota.ReleaseBlockReservationsMap(reservations)
		return nil, fmt.Errorf("failed to create root pin: %w", err)
	}


	// Update pin status to pinned
	pinSvc := core.GetService[pluginCore.IPFSPinService](helper.Context(), pluginCore.PIN_SERVICE)
	if pinSvc == nil {
		quota.ReleaseBlockReservationsMap(reservations)
		return nil, fmt.Errorf("pin service not available: cannot update pin status")
	}

	err = pinSvc.UpdatePinStatus(ctx, ipfsPin.RequestID, pluginDb.PinningStatusPinned, nil)
	if err != nil {
		// Return the error so the workflow retries via RetryStep.
		// Swallowing it leaves the pin stuck at "queued" with no
		// reconciliation mechanism.
		return nil, fmt.Errorf("failed to update pin status to pinned: %w", err)
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

// cidSliceToStringSlice converts a slice of CIDs to a slice of strings
func cidSliceToStringSlice(cids []cid.Cid) []string {
	return lo.Map(cids, func(item cid.Cid, _ int) string { return item.String() })
}

// startTUSFilePathWorkflow starts the dedicated FILE_PATH_WORKFLOW with the
// uploaded CIDs and related CIDs as workflow data. The workflow runs
// asynchronously as a separate request.
func startTUSFilePathWorkflow(
	ctx context.Context,
	helper core.OperationHelper,
	request *models.Request,
	allCids []cid.Cid,
	ipfsPin *pluginDb.IPFSPin,
) error {
	userID := lo.FromPtrOr(request.UserID, 0)
	cids := lo.Map(allCids, func(c cid.Cid, _ int) string { return c.String() })

	if len(allCids) == 0 {
		return fmt.Errorf("no CIDs to process in filepath workflow")
	}

	var relatedCIDs []string
	if ipfsPin != nil {
		pinSvc := core.GetService[pluginCore.IPFSPinService](helper.Context(), pluginCore.PIN_SERVICE)
		if pinSvc != nil {
			related, err := pinSvc.ValidateDAGCompletion(ctx, ipfsPin)
			if err != nil {
				helper.Logger().Error("Failed to validate DAG completion for filepath workflow", zap.Error(err))
			} else {
				relatedCIDs = lo.FilterMap(related, func(b []byte, _ int) (string, bool) {
					c, err := cid.Cast(b)
					if err != nil {
						helper.Logger().Warn("Failed to cast related CID", zap.Binary("cid", b), zap.Error(err))
						return "", false
					}
					return c.String(), true
				})
			}
		}
	}

	_, err := helper.StartWorkflow(
		FILE_PATH_WORKFLOW,
		core.WithWorkflowStructData(FilePathWorkflowInputData{
			CIDs:        cids,
			RelatedCIDs: relatedCIDs,
			UserID:      userID,
		}, "json"),
		core.WithWorkflowUserID(userID),
	)
	return err
}
