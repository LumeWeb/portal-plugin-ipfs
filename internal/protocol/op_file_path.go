package protocol

import (
	"context"
	"errors"
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
	FilePathPhaseStarting   = "starting"
	FilePathPhaseProcessing = "processing"
	FilePathPhaseValidation = "validation"
	FilePathPhaseCompleted  = "completed"
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
	ctx, span := core.TraceMethod(ctx, "FilePathOperationHandler.Execute")
	defer span.End()

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

	allCIDs := pinWorkflowData.Cids

	// Initialize workflow data with progress tracking
	workflowData := FilePathWorkflowData{
		RequestID:       strconv.FormatUint(uint64(req.ID), 10),
		CIDs:            allCIDs,
		UserID:          userID,
		CurrentPhase:    FilePathPhaseStarting,
		CompletedPhases: 0,
		TotalPhases:     4, // Total number of major phases
		ProcessedCIDs:   0,
		TotalCIDs:       len(allCIDs),
	}

	err = h.UpdateWorkflowDataStruct(req.ID, workflowData)
	if err != nil {
		return fmt.Errorf("failed to initialize workflow data: %w", err)
	}

	// Initialize progress tracker with single step since all work is done together
	tracker, err := h.NewProgressTracker(req.ID, core.ProgressModeWeighted, func(cfg *core.ProgressTrackerConfig) {
		cfg.Steps = []core.ProgressStep{
			{
				Name:        FilePathPhaseProcessing,
				Description: "Processing CID metadata and computing file paths",
				Weight:      100,
			},
		}
		cfg.MessageProvider = h.NewDefaultProgressMessageProvider(core.OpTypeStore)
	})
	if err != nil {
		return fmt.Errorf("failed to initialize progress tracker: %w", err)
	}

	if err = tracker.Initialize(); err != nil {
		return fmt.Errorf("failed to initialize tracker: %w", err)
	}

	helper := core.NewProgressTrackerHelper(tracker, h.Context())

	fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)
	blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)

	// Phase 1: Processing
	h.updateWorkflowPhase(req.ID, &workflowData, FilePathPhaseProcessing, 1)

	if err = helper.RunStep(FilePathPhaseProcessing, 100, func() error {

		// Prune existing file paths for related CIDs before recomputing
		if len(workflowData.RelatedCIDs) > 0 {
			err = h.pruneRelatedPaths(ctx, fileManagerSvc, userID, workflowData.RelatedCIDs)
			if err != nil {
				h.Logger().Error("Failed to prune related file paths", zap.Error(err))
				// Continue with path computation even if pruning fails
			}
		}

		// Collect all UnixFS metadata first
		unixfsMetas := make(map[string]*db.UnixFSNode)
		failedCIDs := make(map[string]bool)
		orphanCIDs := make(map[string]bool)

		for i, cidStr := range workflowData.CIDs {
			c, err := cid.Parse(cidStr)
			if err != nil {
				h.Logger().Error("Failed to parse CID", zap.String("cid", cidStr), zap.Error(err))
				failedCIDs[cidStr] = true
				continue
			}

			// Get block metadata to extract UnixFS info
			unixfsMeta, err := blockSvc.GetBlockMeta(ctx, c)
			if err != nil {
				h.Logger().Error("Failed to get block metadata", zap.Stringer("cid", c), zap.Error(err))
				orphanCIDs[cidStr] = true
				continue
			}

			if unixfsMeta == nil || (unixfsMeta.Name == "" && len(unixfsMeta.ChildCID) == 0) {
				h.Logger().Warn("Incomplete UnixFS metadata", zap.Stringer("cid", c))
				orphanCIDs[cidStr] = true
				continue
			}

			unixfsMetas[cidStr] = unixfsMeta

			// Update processed CIDs count
			workflowData.ProcessedCIDs = i + 1
			err = h.UpdateWorkflowDataStruct(req.ID, workflowData)
			if err != nil {
				h.Logger().Error("Failed to update workflow data with processed CID count", zap.Error(err))
			}
		}

		// Create a set of all child CIDs to filter out non-root CIDs
		childCIDSet := make(map[string]bool)
		for _, meta := range unixfsMetas {
			for _, childCID := range meta.ChildCID {
				childCIDSet[childCID.String()] = true
			}
		}

		// Filter to only root CIDs (CIDs that are not children of other CIDs)
		rootCIDMetas := make(map[string]*db.UnixFSNode)
		for cidStr, meta := range unixfsMetas {
			if !childCIDSet[cidStr] {
				rootCIDMetas[cidStr] = meta
			}
		}

		// Create a map to track processed CIDs and avoid cycles
		processed := make(map[string]bool)

		// Process only root CID metadata
		for cidStr, unixfsMeta := range rootCIDMetas {
			c, err := cid.Parse(cidStr)
			if err != nil {
				h.Logger().Error("Failed to parse CID during processing", zap.String("cid", cidStr), zap.Error(err))
				orphanCIDs[cidStr] = true
				continue
			}

			// Compute and store file paths
			err = h.computeAndStoreFilePaths(ctx, fileManagerSvc, unixfsMeta, userID, c, processed)
			if err != nil {
				h.Logger().Error("Failed to compute and store file paths", zap.Stringer("cid", c), zap.Error(err))
				orphanCIDs[cidStr] = true
				continue
			}
		}

		// Phase 2: Process orphans
		h.updateWorkflowPhase(req.ID, &workflowData, FilePathPhaseValidation, 2)

		// Process orphan entries
		for cidStr := range orphanCIDs {
			c, err := cid.Parse(cidStr)
			if err != nil {
				h.Logger().Error("Failed to parse CID for orphan processing", zap.String("cid", cidStr), zap.Error(err))
				continue
			}

			// Create orphan entry
			h.createOrphanEntry(ctx, fileManagerSvc, userID, c, unixfsMetas[cidStr])
		}

		// Phase 3: Completed
		h.updateWorkflowPhase(req.ID, &workflowData, FilePathPhaseCompleted, 3)

		// Validate that file paths were created successfully
		var filePathCount int64
		var expectedCIDs [][]byte

		// Convert processed CIDs to byte slices for database query
		for cidStr := range processed {
			c, err := cid.Parse(cidStr)
			if err == nil {
				expectedCIDs = append(expectedCIDs, c.Bytes())
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
					zap.Int("processed_cids", len(processed)),
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
			zap.Int("processed_cids", len(processed)),
			zap.Int("total_cids", workflowData.TotalCIDs),
			zap.Int64("file_paths_created", filePathCount))

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (h *FilePathOperationHandler) computeAndStoreFilePaths(ctx context.Context, fileManagerSvc pluginCore.FileManagerService, unixfsMeta *db.UnixFSNode, userID uint, rootCID cid.Cid, processed map[string]bool) error {
	ctx, span := core.TraceMethod(ctx, "FilePathOperationHandler.computeAndStoreFilePaths")
	defer span.End()

	// Start recursive path computation from the root
	size, err := h.ComputePathsRecursive(ctx, fileManagerSvc, unixfsMeta, userID, rootCID, "/", 0, processed, false)
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
	ctx, span := core.TraceMethod(ctx, "FilePathOperationHandler.createOrphanEntry")
	defer span.End()

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
		filePath.IsDirectory = pluginCore.UnixFSType(unixfsMeta.Type).IsDirectory()
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
	if err != nil && !errors.Is(err, db.ErrDuplicateFilePath) {
		h.Logger().Error("Failed to create orphan file path",
			zap.String("path", filePath.Path),
			zap.Stringer("cid", c),
			zap.Error(err))
	}
}

// CreateOrphanEntriesForPins creates orphan file path entries for pins that don't have proper UnixFS metadata
func (h *FilePathOperationHandler) CreateOrphanEntriesForPins(ctx context.Context, pins []*db.IPFSPin) error {
	ctx, span := core.TraceMethod(ctx, "FilePathOperationHandler.CreateOrphanEntriesForPins")
	defer span.End()

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

func (h *FilePathOperationHandler) ComputePathsRecursive(ctx context.Context, fileManagerSvc pluginCore.FileManagerService, currentNodeMeta *db.UnixFSNode, userID uint, currentCID cid.Cid, parentPath string, depth int, processed map[string]bool, isOrphan bool) (uint64, error) {
	ctx, span := core.TraceMethod(ctx, "FilePathOperationHandler.ComputePathsRecursive")
	defer span.End()

	cidStr := currentCID.String()

	// Check if we've already processed this CID to avoid cycles
	if processed[cidStr] {
		return 0, nil
	}
	processed[cidStr] = true

	// Determine the path for this node
	var currentPath string
	var effectiveParentPath string

	// Compute fallback name before constructing paths
	nodeName := currentNodeMeta.Name
	if nodeName == "" {
		nodeName = cidStr
	}

	// For root nodes, parentPath is "/"
	if depth == 0 {
		currentPath = "/" + nodeName
		effectiveParentPath = "/"
	} else {
		// For child nodes, append to parent path
		if parentPath == "/" {
			currentPath = "/" + nodeName
		} else {
			currentPath = parentPath + "/" + nodeName
		}
		effectiveParentPath = parentPath
	}

	// Initialize size with this node's block size
	totalSize := uint64(currentNodeMeta.BlockSize)

	// Create file path entry for the current node first
	filePath := &db.FilePath{
		UserID:      userID,
		CID:         currentCID.Bytes(),
		Path:        currentPath,
		Name:        nodeName,
		Type:        currentNodeMeta.Type,
		Size:        int64(currentNodeMeta.BlockSize), // Start with just this block's size
		IsDirectory: pluginCore.UnixFSType(currentNodeMeta.Type).IsDirectory(),
		IsOrphan:    isOrphan,
		ParentPath:  effectiveParentPath,
		Depth:       depth,
	}

	// Store the file path for the current node
	err := fileManagerSvc.CreateFilePath(ctx, filePath)
	if err != nil {
		if errors.Is(err, db.ErrDuplicateFilePath) {
			// If the file path already exists, just log a debug message and continue
			h.Logger().Debug("File path already exists, skipping creation",
				zap.String("path", currentPath),
				zap.Stringer("cid", currentCID))
		} else {
			h.Logger().Error("Failed to create file path",
				zap.String("path", currentPath),
				zap.Stringer("cid", currentCID),
				zap.Error(err))
			return 0, fmt.Errorf("failed to create file path for %s: %w", currentPath, err)
		}
	}

	h.Logger().Debug("Created file path entry",
		zap.String("path", currentPath),
		zap.String("name", nodeName),
		zap.Bool("is_directory", filePath.IsDirectory),
		zap.Bool("is_orphan", filePath.IsOrphan),
		zap.Int("depth", depth),
		zap.Uint64("size", uint64(currentNodeMeta.BlockSize)))

	// If this is a directory and has children, process them recursively
	if pluginCore.UnixFSType(currentNodeMeta.Type).IsDirectory() && len(currentNodeMeta.ChildCID) > 0 {
		blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)

		for _, childCID := range currentNodeMeta.ChildCID {
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

		// Update the directory entry with the total accumulated size
		filePath.Size = int64(totalSize)
		err = fileManagerSvc.UpdateFilePath(ctx, filePath)
		if err != nil {
			h.Logger().Error("Failed to update directory size",
				zap.String("path", currentPath),
				zap.Stringer("cid", currentCID),
				zap.Uint64("total_size", totalSize),
				zap.Error(err))
		}
	}

	return totalSize, nil
}

// pruneRelatedPaths deletes existing file paths for related CIDs
func (h *FilePathOperationHandler) pruneRelatedPaths(ctx context.Context, fileManagerSvc pluginCore.FileManagerService, userID uint, relatedCIDs []string) error {
	ctx, span := core.TraceMethod(ctx, "FilePathOperationHandler.pruneRelatedPaths")
	defer span.End()

	var errors []error
	for _, cidStr := range relatedCIDs {
		c, err := cid.Parse(cidStr)
		if err != nil {
			h.Logger().Warn("Failed to parse related CID for pruning", zap.String("cid", cidStr), zap.Error(err))
			errors = append(errors, fmt.Errorf("failed to parse CID %s: %w", cidStr, err))
			continue
		}

		err = fileManagerSvc.DeleteFilePath(ctx, userID, c.Bytes())
		if err != nil {
			h.Logger().Error("Failed to delete file path for related CID",
				zap.String("cid", cidStr),
				zap.Error(err))
			errors = append(errors, fmt.Errorf("failed to delete file path for CID %s: %w", cidStr, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to prune related paths: %w", errors[0]) // Return first error for simplicity
	}

	return nil
}

func (h *FilePathOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	return h.GetStatusFromWorkflowData(req.ID, req)
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
	ctx, span := core.TraceMethod(ctx, "FilePathOperationHandler.enrichOrphanEntryFromBlockstore")
	defer span.End()

	// Try to get basic block metadata from blockstore using metadata-only APIs
	proto := core.GetProtocol(internal.ProtocolName).(ProtoNode)
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
	ctx, span := core.TraceMethod(ctx, "FilePathOperationHandler.walkDAGForTotalSize")
	defer span.End()

	// Handle cycles gracefully
	cidStr := c.String()
	if visited[cidStr] {
		return 0, nil
	}
	visited[cidStr] = true

	// Get metadata store to retrieve block information
	metadataStore := core.GetProtocol(internal.ProtocolName).(*Protocol).GetMetadataStore()
	if metadataStore == nil {
		h.Logger().Error("Metadata store not available")
		return 0, fmt.Errorf("metadata store not available")
	}

	// Get size of current block
	size, err := metadataStore.Size(ctx, c)
	if err != nil {
		// If we can't get the size from metadata store, try to get it from blockstore
		proto := core.GetProtocol(internal.ProtocolName).(*Protocol)
		blockstore := proto.GetNode().GetBlockstore()
		blockSize, blockErr := blockstore.GetSize(ctx, c)
		if blockErr != nil {
			return 0, fmt.Errorf("failed to get size for CID %s from both metadata store and blockstore: %w", c.String(), err)
		}
		size = uint64(blockSize)
	}

	totalSize := size

	// Get children of current block
	children, err := metadataStore.BlockChildren(ctx, c, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get children for CID %s: %w", c.String(), err)
	}

	// Recursively walk children
	for _, child := range children {
		childSize, err := h.walkDAGForTotalSize(ctx, child, visited)
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
	return core.NewNamedOperation(
		FilePathOperationName(),
		"", // No global type for file path
		&FilePathOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		},
		"Update File System",
	)
}

func FilePathOperationName() string {
	return FILE_PATH_WORKFLOW
}
