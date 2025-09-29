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
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

const (
	FilePathPhaseStarting            = "starting"
	FilePathPhaseProcessingCIDs      = "processing_cids"
	FilePathPhaseComputingPaths      = "computing_paths"
	FilePathPhaseHandlingOrphans     = "handling_orphans"
	FilePathPhaseValidatingResults  = "validating_results"
	FilePathPhaseCompleted          = "completed"
)

// FilePathWorkflowData represents the workflow data for file path operations
type FilePathWorkflowData struct {
	RequestID       string   `json:"request_id"`
	CIDs            []string `json:"cids"`
	UserID          uint     `json:"user_id"`
	CurrentPhase    string   `json:"current_phase"`
	CompletedPhases int      `json:"completed_phases"`
	TotalPhases     int      `json:"total_phases"`
	ProcessedCIDs   int      `json:"processed_cids"`
	TotalCIDs      int      `json:"total_cids"`
}

// FilePathOperationHandler handles file path computation and storage
type FilePathOperationHandler struct {
	core.OperationHelper
}

func (h *FilePathOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
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

	// Initialize workflow data with progress tracking
	workflowData := FilePathWorkflowData{
		RequestID:       strconv.FormatUint(uint64(req.ID), 10),
		CIDs:            pinWorkflowData.Cids,
		UserID:          userID,
		CurrentPhase:    FilePathPhaseStarting,
		CompletedPhases: 0,
		TotalPhases:     5, // Total number of major phases
		ProcessedCIDs:   0,
		TotalCIDs:      len(pinWorkflowData.Cids),
	}

	err = h.UpdateWorkflowDataStruct(req.ID, workflowData)
	if err != nil {
		return fmt.Errorf("failed to initialize workflow data: %w", err)
	}

	fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)
	blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)

	// Phase 1: Processing CIDs
	h.updateWorkflowPhase(req.ID, &workflowData, FilePathPhaseProcessingCIDs, 1)

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
			h.createOrphanEntry(ctx, fileManagerSvc, userID, c)
			h.Logger().Error("Failed to get block metadata", zap.Stringer("cid", c), zap.Error(err))
			continue
		}

		// Check if UnixFS metadata is complete
		if unixfsMeta == nil || (unixfsMeta.Name == "" && len(unixfsMeta.ChildCID) == 0) {
			// If metadata is incomplete, create an orphan entry
			h.createOrphanEntry(ctx, fileManagerSvc, userID, c)
			h.Logger().Warn("Incomplete UnixFS metadata, creating orphan entry", zap.Stringer("cid", c))
			continue
		}

		// Compute and store file paths
		err = h.computeAndStoreFilePaths(ctx, fileManagerSvc, unixfsMeta, userID, c)
		if err != nil {
			// If path computation fails, create an orphan entry
			h.createOrphanEntry(ctx, fileManagerSvc, userID, c)
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
	return h.ComputePathsRecursive(ctx, fileManagerSvc, unixfsMeta, userID, rootCID, "", 0, processed, false)
}

// createOrphanEntry creates a file path entry marked as orphan
func (h *FilePathOperationHandler) createOrphanEntry(ctx context.Context, fileManagerSvc pluginCore.FileManagerService, userID uint, c cid.Cid) {
	filePath := &db.FilePath{
		UserID:      userID,
		CID:         c.Bytes(),
		Path:        "/" + c.String(),
		Name:        c.String(),
		Type:        0, // Unknown type
		Size:        0, // Unknown size
		IsDirectory: false,
		IsOrphan:    true,
		ParentPath:  "",
		Depth:       0,
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

		// Create orphan entry for this pin
		h.createOrphanEntry(ctx, fileManagerSvc, pin.UserID, c)
		h.Logger().Debug("Created orphan entry for pin",
			zap.Stringer("cid", c),
			zap.Uint("user_id", pin.UserID))
	}

	return nil
}

func (h *FilePathOperationHandler) ComputePathsRecursive(ctx context.Context, fileManagerSvc pluginCore.FileManagerService, unixfsMeta *db.UnixFSNode, userID uint, currentCID cid.Cid, parentPath string, depth int, processed map[string]bool, isOrphan bool) error {
	cidStr := currentCID.String()

	// Check if we've already processed this CID to avoid cycles
	if processed[cidStr] {
		return nil
	}
	processed[cidStr] = true

	// Determine the path for this node
	var currentPath string
	if parentPath == "" {
		// Root node - use the name from UnixFS or default to CID
		if unixfsMeta.Name != "" {
			currentPath = "/" + unixfsMeta.Name
		} else {
			currentPath = "/" + cidStr
		}
	} else {
		// Child node - append to parent path
		currentPath = parentPath + "/" + unixfsMeta.Name
	}

	// Create file path entry
	filePath := &db.FilePath{
		UserID:      userID,
		CID:         currentCID.Bytes(),
		Path:        currentPath,
		Name:        unixfsMeta.Name,
		Type:        unixfsMeta.Type,
		Size:        unixfsMeta.BlockSize,
		IsDirectory: unixfsMeta.Type == 1, // Type 1 = directory
		IsOrphan:    isOrphan,
		ParentPath:  parentPath,
		Depth:       depth,
	}

	// Store the file path
	err := fileManagerSvc.CreateFilePath(ctx, filePath)
	if err != nil {
		h.Logger().Error("Failed to create file path",
			zap.String("path", currentPath),
			zap.Stringer("cid", currentCID),
			zap.Error(err))
		return fmt.Errorf("failed to create file path for %s: %w", currentPath, err)
	}

	h.Logger().Debug("Created file path entry",
		zap.String("path", currentPath),
		zap.String("name", unixfsMeta.Name),
		zap.Bool("is_directory", filePath.IsDirectory),
		zap.Bool("is_orphan", filePath.IsOrphan),
		zap.Int("depth", depth))

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
				h.createOrphanEntry(ctx, fileManagerSvc, userID, childCID)
				continue
			}

			// Recursively process the child
			err = h.ComputePathsRecursive(ctx, fileManagerSvc, childMeta, userID, childCID, currentPath, depth+1, processed, false)
			if err != nil {
				h.Logger().Error("Failed to compute child paths",
					zap.Stringer("child_cid", childCID),
					zap.String("parent_path", currentPath),
					zap.Error(err))
				continue
			}
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
	return "ipfs.file.path"
}
