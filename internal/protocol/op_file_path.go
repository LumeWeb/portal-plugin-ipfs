package protocol

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

const (
	FilePathPhaseStarting          = "starting"
	FilePathPhaseProcessingCIDs    = "processing_cids"
	FilePathPhaseComputingPaths    = "computing_paths"
	FilePathPhaseHandlingOrphans   = "handling_orphans"
	FilePathPhaseValidatingResults = "validating_results"
	FilePathPhaseCompleted         = "completed"
)

// FilePathWorkflowData represents the workflow data for file path operations
type FilePathWorkflowData struct {
	RequestID       string   `json:"request_id"`
	CIDs            []string `json:"cids"`
	UserID          uint     `json:"user_id"`
	RelatedCIDs     []string `json:"related_cids,omitempty"`
	CurrentPhase    string   `json:"current_phase"`
	CompletedPhases int      `json:"completed_phases"`
	TotalPhases     int      `json:"total_phases"`
	ProcessedCIDs   int      `json:"processed_cids"`
	TotalCIDs       int      `json:"total_cids"`
}

// FilePathOperationHandler handles file path computation and storage
type FilePathOperationHandler struct {
	core.OperationHelper
}

func (h *FilePathOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	if len(req.Hash) == 0 {
		var workflowData PinWorkflowData
		err := h.StructuredWorkflowData(req.ID, &workflowData)
		if err != nil || len(workflowData.Cids) == 0 {
			return fmt.Errorf("hash is required")
		}
	}
	return nil
}

func (h *FilePathOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	var pinWorkflowData PinWorkflowData
	err := h.StructuredWorkflowData(req.ID, &pinWorkflowData)
	if err != nil {
		return fmt.Errorf("failed to get pin workflow data: %w", err)
	}

	userID := lo.FromPtrOr(req.UserID, 0)

	// Validate that userID is non-nil and not zero
	if req.UserID == nil || userID == 0 {
		return fmt.Errorf("invalid or missing user ID")
	}

	// Initialize workflow data with progress tracking
	workflowData := FilePathWorkflowData{
		RequestID:       strconv.FormatUint(uint64(req.ID), 10),
		CIDs:            pinWorkflowData.Cids,
		UserID:          userID,
		CurrentPhase:    FilePathPhaseStarting,
		CompletedPhases: 0,
		TotalPhases:     5, // Total number of major phases
		ProcessedCIDs:   0,
		TotalCIDs:       len(pinWorkflowData.Cids),
	}

	err = h.UpdateWorkflowDataStruct(req.ID, workflowData)
	if err != nil {
		return fmt.Errorf("failed to initialize workflow data: %w", err)
	}

	fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)
	blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)

	// Phase 1: Processing CIDs
	h.updateWorkflowPhase(req.ID, &workflowData, FilePathPhaseProcessingCIDs, 1)

	// Prune existing file paths for related CIDs before recomputing
	if len(workflowData.RelatedCIDs) > 0 {
		err = h.pruneRelatedPaths(ctx, fileManagerSvc, userID, workflowData.RelatedCIDs)
		if err != nil {
			h.Logger().Error("Failed to prune related file paths", zap.Error(err))
			// Continue with path computation even if pruning fails
		}
	}

	// Process each CID in the workflow data
	for i, cidStr := range workflowData.CIDs {
		c, err := cid.Parse(cidStr)
		if err != nil {
			h.Logger().Error("Failed to parse CID", zap.String("cid", cidStr), zap.Error(err))
			continue
		}

		// Get block metadata to extract UnixFS info
		unixfsMeta, err := blockSvc.GetBlockMeta(ctx, c)
		if err != nil {
			// If we can't get block metadata, create an orphan entry
			h.createOrphanEntry(ctx, fileManagerSvc, userID, c, nil)
			h.Logger().Error("Failed to get block metadata", zap.Stringer("cid", c), zap.Error(err))
			continue
		}

		if unixfsMeta == nil || (unixfsMeta.Name == "" && len(unixfsMeta.ChildCID) == 0) {
			// Create orphan entry for incomplete metadata
			h.createOrphanEntry(ctx, fileManagerSvc, userID, c, nil)
			h.Logger().Warn("Incomplete UnixFS metadata, creating orphan entry", zap.Stringer("cid", c))
			continue
		}

		// Compute and store file paths
		err = h.computeAndStoreFilePaths(ctx, fileManagerSvc, unixfsMeta, userID, c)
		if err != nil {
			// Create orphan entry on path computation failure
			h.createOrphanEntry(ctx, fileManagerSvc, userID, c, nil)
			h.Logger().Error("Failed to compute and store file paths", zap.Stringer("cid", c), zap.Error(err))
			continue
		}

		// Update processed CIDs count
		workflowData.ProcessedCIDs = i + 1
		err = h.UpdateWorkflowDataStruct(req.ID, workflowData)
		if err != nil {
			h.Logger().Error("Failed to update workflow data with processed CID count", zap.Error(err))
		}
	}

	// Phase 2: Computing paths (this happens within computeAndStoreFilePaths)
	h.updateWorkflowPhase(req.ID, &workflowData, FilePathPhaseComputingPaths, 2)

	// Phase 3: Handling orphans (this happens automatically when metadata is incomplete)
	h.updateWorkflowPhase(req.ID, &workflowData, FilePathPhaseHandlingOrphans, 3)

	// Phase 4: Validating results
	h.updateWorkflowPhase(req.ID, &workflowData, FilePathPhaseValidatingResults, 4)

	// Validate that file paths were created successfully for the processed CIDs
	var filePathCount int64
	var expectedCIDs [][]byte

	// Convert processed CIDs to byte slices for database query
	for _, cidStr := range workflowData.CIDs {
		if cidStr != "" {
			c, err := cid.Parse(cidStr)
			if err == nil {
				expectedCIDs = append(expectedCIDs, c.Bytes())
			}
		}
	}

	if len(expectedCIDs) > 0 {
		err = h.Context().DB().WithContext(ctx).
			Model(&db.FilePath{}).
			Where("user_id = ? AND cid IN ?", userID, expectedCIDs).
			Count(&filePathCount).Error
		if err != nil {
			h.Logger().Error("Failed to validate file path results", zap.Error(err))
			return fmt.Errorf("failed to validate file path results: %w", err)
		}

		// Check if we have at least some file paths for the processed CIDs
		if filePathCount == 0 {
			h.Logger().Warn("No file paths created for processed CIDs",
				zap.Uint("user_id", userID),
				zap.Int("processed_cids", workflowData.ProcessedCIDs),
				zap.Strings("cids", workflowData.CIDs))
		}
	} else {
		// If no CIDs were processed successfully, just count total paths for user
		err = h.Context().DB().WithContext(ctx).
			Model(&db.FilePath{}).
			Where("user_id = ?", userID).
			Count(&filePathCount).Error
		if err != nil {
			h.Logger().Error("Failed to validate file path results", zap.Error(err))
			return fmt.Errorf("failed to validate file path results: %w", err)
		}
	}

	h.Logger().Debug("File path operation completed",
		zap.Uint("user_id", userID),
		zap.Int("processed_cids", workflowData.ProcessedCIDs),
		zap.Int("total_cids", workflowData.TotalCIDs),
		zap.Int64("file_paths_created", filePathCount))

	// Phase 5: Completed
	h.updateWorkflowPhase(req.ID, &workflowData, FilePathPhaseCompleted, 5)

	return nil
}

func (h *FilePathOperationHandler) computeAndStoreFilePaths(ctx context.Context, fileManagerSvc pluginCore.FileManagerService, unixfsMeta *db.UnixFSNode, userID uint, rootCID cid.Cid) error {
	// Create a map to track processed CIDs and avoid cycles
	processed := make(map[string]bool)

	// Start recursive path computation from the root
	size, err := h.ComputePathsRecursive(ctx, fileManagerSvc, unixfsMeta, userID, rootCID, "", 0, processed, false)
	if err != nil {
		return err
	}
	
	// Log the total size of the root
	h.Logger().Debug("Total size calculated for root CID",
		zap.Stringer("cid", rootCID),
		zap.Uint64("size", size))
	
	return nil
}

// createOrphanEntry creates a file path entry marked as orphan
func (h *FilePathOperationHandler) createOrphanEntry(ctx context.Context, fileManagerSvc pluginCore.FileManagerService, userID uint, c cid.Cid, unixfsMeta *db.UnixFSNode) {
	filePath := &db.FilePath{
		UserID:      userID,
		CID:         c.Bytes(),
		Path:        "/" + c.String(),
		Name:        "", // Don't set name if no metadata available
		Type:        0,  // Unknown type
		Size:        0,  // Will be updated from metadata if available
		IsDirectory: false,
		IsOrphan:    true,
		ParentPath:  "/", // Root-level orphans should also have parent_path = "/"
		Depth:       0,
	}

	// Use block metadata if available to set proper size and type
	if unixfsMeta != nil {
		filePath.Size = unixfsMeta.BlockSize
		filePath.Type = unixfsMeta.Type
		filePath.IsDirectory = unixfsMeta.Type == 1 // Type 1 = directory
		// Use name from metadata if available
		if unixfsMeta.Name != "" {
			filePath.Name = unixfsMeta.Name
			filePath.Path = "/" + unixfsMeta.Name
		}
	} else {
		// If no metadata provided, try to get basic block info from blockstore
		h.enrichOrphanEntryFromBlockstore(ctx, c, filePath)
	}

	// If we still don't have a name, try to get it from pin information
	if filePath.Name == "" {
		pinSvc := core.GetService[pluginCore.IPFSPinService](h.Context(), pluginCore.PIN_SERVICE)
		if pinSvc != nil {
			pin, err := pinSvc.GetPinByCIDAndUser(ctx, c, userID)
			if err == nil && pin != nil && pin.Name != "" {
				filePath.Name = pin.Name
				filePath.Path = "/" + pin.Name
			}
		}
	}

	// Store the orphan file path
	err := fileManagerSvc.CreateFilePath(ctx, filePath)
	if err != nil {
		h.Logger().Error("Failed to create orphan file path",
			zap.String("path", filePath.Path),
			zap.Stringer("cid", c),
			zap.Error(err))
	}
}

// CreateOrphanEntriesForPins creates orphan file path entries for pins that don't have proper UnixFS metadata
func (h *FilePathOperationHandler) CreateOrphanEntriesForPins(ctx context.Context, pins []*db.IPFSPin) error {
	fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)

	for _, pin := range pins {
		c, err := cid.Parse(pin.CID)
		if err != nil {
			h.Logger().Error("Failed to parse CID for orphan entry",
				zap.Binary("cid", pin.CID),
				zap.Error(err))
			continue
		}

		// Create minimal UnixFS metadata with pin name if available
		var unixfsMeta *db.UnixFSNode
		if pin.Name != "" {
			unixfsMeta = &db.UnixFSNode{
				Name: pin.Name,
			}
		}

		// Create orphan entry for this pin with available metadata
		h.createOrphanEntry(ctx, fileManagerSvc, pin.UserID, c, unixfsMeta)
		h.Logger().Debug("Created orphan entry for pin",
			zap.Stringer("cid", c),
			zap.Uint("user_id", pin.UserID),
			zap.String("pin_name", pin.Name))
	}

	return nil
}

func (h *FilePathOperationHandler) ComputePathsRecursive(ctx context.Context, fileManagerSvc pluginCore.FileManagerService, unixfsMeta *db.UnixFSNode, userID uint, currentCID cid.Cid, parentPath string, depth int, processed map[string]bool, isOrphan bool) (uint64, error) {
	cidStr := currentCID.String()

	// Check if we've already processed this CID to avoid cycles
	if processed[cidStr] {
		return 0, nil
	}
	processed[cidStr] = true

	// Determine the path for this node
	var currentPath string
	var effectiveParentPath string
	if parentPath == "" {
		// Root node - use the name from UnixFS or default to CID
		if unixfsMeta.Name != "" {
			currentPath = "/" + unixfsMeta.Name
		} else {
			currentPath = "/" + cidStr
		}
		// Root-level items should have parent_path = "/", not empty string
		effectiveParentPath = "/"
	} else {
		// Child node - append to parent path
		currentPath = parentPath + "/" + unixfsMeta.Name
		effectiveParentPath = parentPath
	}

	// Initialize size with this node's block size
	totalSize := uint64(unixfsMeta.BlockSize)

	// If this is a directory and has children, process them recursively
	if unixfsMeta.Type == 1 && len(unixfsMeta.ChildCID) > 0 {
		blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)

		for _, childCID := range unixfsMeta.ChildCID {
			// Get metadata for the child
			childMeta, err := blockSvc.GetBlockMeta(ctx, childCID)
			if err != nil {
				h.Logger().Error("Failed to get child block metadata",
					zap.Stringer("child_cid", childCID),
					zap.String("parent_path", currentPath),
					zap.Error(err))
				// Create orphan entry for child that failed to load
				h.createOrphanEntry(ctx, fileManagerSvc, userID, childCID, nil)
				continue
			}

			// Recursively process the child and accumulate size
			childSize, err := h.ComputePathsRecursive(ctx, fileManagerSvc, childMeta, userID, childCID, currentPath, depth+1, processed, false)
			if err != nil {
				h.Logger().Error("Failed to compute child paths",
					zap.Stringer("child_cid", childCID),
					zap.String("parent_path", currentPath),
					zap.Error(err))
				continue
			}
			
			// Add child size to total directory size
			totalSize += childSize
		}
	}

	// Create file path entry with calculated total size for directories
	filePath := &db.FilePath{
		UserID:      userID,
		CID:         currentCID.Bytes(),
		Path:        currentPath,
		Name:        unixfsMeta.Name,
		Type:        unixfsMeta.Type,
		Size:        int64(totalSize), // Use calculated size
		IsDirectory: unixfsMeta.Type == 1, // Type 1 = directory
		IsOrphan:    isOrphan,
		ParentPath:  effectiveParentPath,
		Depth:       depth,
	}

	// If this is a directory and name is empty, set name to CID string
	if filePath.IsDirectory && filePath.Name == "" {
		filePath.Name = currentCID.String()
	}

	// Store the file path
	err := fileManagerSvc.CreateFilePath(ctx, filePath)
	if err != nil {
		h.Logger().Error("Failed to create file path",
			zap.String("path", currentPath),
			zap.Stringer("cid", currentCID),
			zap.Error(err))
		return 0, fmt.Errorf("failed to create file path for %s: %w", currentPath, err)
	}

	h.Logger().Debug("Created file path entry",
		zap.String("path", currentPath),
		zap.String("name", unixfsMeta.Name),
		zap.Bool("is_directory", filePath.IsDirectory),
		zap.Bool("is_orphan", filePath.IsOrphan),
		zap.Int("depth", depth),
		zap.Uint64("size", totalSize))

	return totalSize, nil
}

// pruneRelatedPaths deletes existing file paths for related CIDs
func (h *FilePathOperationHandler) pruneRelatedPaths(ctx context.Context, fileManagerSvc pluginCore.FileManagerService, userID uint, relatedCIDs []string) error {
	for _, cidStr := range relatedCIDs {
		c, err := cid.Parse(cidStr)
		if err != nil {
			h.Logger().Warn("Failed to parse related CID for pruning", zap.String("cid", cidStr), zap.Error(err))
			continue
		}

		err = fileManagerSvc.DeleteFilePath(ctx, userID, c.Bytes())
		if err != nil {
			h.Logger().Error("Failed to delete file path for related CID",
				zap.String("cid", cidStr),
				zap.Error(err))
			// Continue with other CIDs even if one fails
		}
	}

	return nil
}

func (h *FilePathOperationHandler) GetStatus(ctx context.Context, req *models.Request) (*core.RequestStatus, error) {
	status := &core.RequestStatus{
		ProgressPercent: 0,
	}

	// Extract values from workflow data
	var workflowData FilePathWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflowData)
	if err != nil {
		return nil, err
	}

	currentPhase := workflowData.CurrentPhase
	completedPhases := float64(workflowData.CompletedPhases)
	totalPhases := float64(workflowData.TotalPhases)
	processedCIDs := float64(workflowData.ProcessedCIDs)
	totalCIDs := float64(workflowData.TotalCIDs)

	// Determine status based on request state
	status.State = req.Status
	switch req.Status {
	case models.RequestStatusPending:
		status.Message = "File path operation is queued"
		status.ProgressPercent = 0
	case models.RequestStatusProcessing:
		status.Message = "File path operation in progress: " + currentPhase

		// Compute progress based on both phases and CID processing
		phaseProgress := (completedPhases / totalPhases) * 0.7 // 70% weight for phases
		cidProgress := float64(0)
		if totalCIDs > 0 {
			cidProgress = (processedCIDs / totalCIDs) * 0.3 // 30% weight for CID processing
		}

		status.ProgressPercent = (phaseProgress + cidProgress) * 100

		// Ensure progress doesn't exceed 100%
		if status.ProgressPercent > 100 {
			status.ProgressPercent = 100
		}

		// Ensure progress is at least 1% if we're processing
		if status.ProgressPercent < 1 && completedPhases > 0 {
			status.ProgressPercent = 1
		}
	case models.RequestStatusCompleted:
		status.Message = "File paths computed and stored successfully"
		status.ProgressPercent = 100
	case models.RequestStatusFailed:
		status.Message = "File path operation failed"
		status.ProgressPercent = 100
	default:
		status.Message = "File path operation status unknown"
		status.ProgressPercent = 0
	}

	return status, nil
}

// updateWorkflowPhase updates workflow data with current phase and completed phases count
func (h *FilePathOperationHandler) updateWorkflowPhase(requestID uint, workflowData *FilePathWorkflowData, phase string, completedPhases int) {
	workflowData.CurrentPhase = phase
	workflowData.CompletedPhases = completedPhases
	err := h.UpdateWorkflowDataStruct(requestID, *workflowData)
	if err != nil {
		h.Logger().Error("Failed to update workflow data",
			zap.String("phase", phase),
			zap.Error(err))
	}
}

func (h *FilePathOperationHandler) Cleanup(_ context.Context, _ *models.Request) error {
	return nil
}

// enrichOrphanEntryFromBlockstore tries to get basic block info from blockstore
// when UnixFS metadata is not available, to set proper file size
func (h *FilePathOperationHandler) enrichOrphanEntryFromBlockstore(ctx context.Context, c cid.Cid, filePath *db.FilePath) {
	// Try to get basic block metadata from blockstore using metadata-only APIs
	proto := core.GetProtocol(internal.ProtocolName).(*Protocol)
	blockstore := proto.GetNode().GetBlockstore()

	// First check if the block exists
	has, err := blockstore.Has(ctx, c)
	if err != nil || !has {
		h.Logger().Debug("Block not found in blockstore for orphan enrichment",
			zap.Stringer("cid", c),
			zap.Error(err))
		return
	}

	// Get the size of the block without downloading its data
	size, err := blockstore.GetSize(ctx, c)
	if err != nil {
		h.Logger().Debug("Failed to get block size from blockstore for orphan enrichment",
			zap.Stringer("cid", c),
			zap.Error(err))
		return
	}

	// Update file path with basic block information
	filePath.Size = int64(size)
	filePath.Type = 0            // Default to file type since we don't have UnixFS info
	filePath.IsDirectory = false // Default to file since we don't have UnixFS info

	// Try to walk the DAG to get the total file size
	totalSize, err := h.walkDAGForTotalSize(ctx, c, make(map[string]bool))
	if err == nil && totalSize > 0 {
		filePath.Size = int64(totalSize)
		h.Logger().Debug("Successfully calculated total DAG size for orphan entry",
			zap.Stringer("cid", c),
			zap.Int64("total_size", filePath.Size))
	} else if err != nil {
		h.Logger().Warn("Failed to walk DAG for total size calculation",
			zap.Stringer("cid", c),
			zap.Error(err))
	}

	// If we still don't have a name, try to get it from pin information
	if filePath.Name == "" {
		pinSvc := core.GetService[pluginCore.IPFSPinService](h.Context(), pluginCore.PIN_SERVICE)
		if pinSvc != nil {
			pin, err := pinSvc.GetPinByCIDAndUser(ctx, c, filePath.UserID)
			if err == nil && pin != nil && pin.Name != "" {
				filePath.Name = pin.Name
				filePath.Path = "/" + pin.Name
			}
		}
	}

	h.Logger().Debug("Enriched orphan entry with blockstore data",
		zap.Stringer("cid", c),
		zap.Int64("size", filePath.Size),
		zap.Uint8("type", filePath.Type),
		zap.String("name", filePath.Name))
}

// walkDAGForTotalSize recursively walks through all child blocks in the DAG
// and sums up their sizes to get the total file size
func (h *FilePathOperationHandler) walkDAGForTotalSize(ctx context.Context, c cid.Cid, visited map[string]bool) (uint64, error) {
	// Handle cycles gracefully
	cidStr := c.String()
	if visited[cidStr] {
		return 0, nil
	}
	visited[cidStr] = true

	// Create a virtual read context to avoid downloading actual block data
	virtualCtx := store.VirtualReadOption(ctx, true)

	// Get metadata store to retrieve block information
	metadataStore := core.GetProtocol(internal.ProtocolName).(*Protocol).GetMetadataStore()
	if metadataStore == nil {
		h.Logger().Error("Metadata store not available")
		return 0, fmt.Errorf("metadata store not available")
	}

	// Get size of current block
	size, err := metadataStore.Size(c)
	if err != nil {
		// If we can't get the size from metadata store, try to get it from blockstore
		proto := core.GetProtocol(internal.ProtocolName).(*Protocol)
		blockstore := proto.GetNode().GetBlockstore()
		blockSize, blockErr := blockstore.GetSize(virtualCtx, c)
		if blockErr != nil {
			return 0, fmt.Errorf("failed to get size for CID %s from both metadata store and blockstore: %w", c.String(), err)
		}
		size = uint64(blockSize)
	}

	totalSize := size

	// Get children of current block
	children, err := metadataStore.BlockChildren(c, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get children for CID %s: %w", c.String(), err)
	}

	// Recursively walk children
	for _, child := range children {
		childSize, err := h.walkDAGForTotalSize(virtualCtx, child, visited)
		if err != nil {
			// Log error but continue with other children
			h.Logger().Warn("Failed to get size for child block in DAG walk",
				zap.Stringer("parent_cid", c),
				zap.Stringer("child_cid", child),
				zap.Error(err))
			continue
		}
		totalSize += childSize
	}

	return totalSize, nil
}

func NewFilePathOperation(ctx core.Context) core.Operation {
	return core.NewOperation(
		FilePathOperationName(),
		"", // No global type for file path
		&FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		},
	)
}

func FilePathOperationName() string {
	return FILE_PATH_WORKFLOW
}
