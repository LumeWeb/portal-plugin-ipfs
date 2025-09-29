package protocol

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	UnpinPhaseStarting                     = "starting"
	UnpinPhaseValidatingDAGBefore          = "validating_dag_before"
	UnpinPhaseAnalyzingDAGDependencies     = "analyzing_dag_dependencies"
	UnpinPhasePromotingOrphans             = "promoting_orphans"
	UnpinPhaseAnalyzingPathDependencies    = "analyzing_path_dependencies"
	UnpinPhaseHandlingPathCascadingEffects = "handling_path_cascading_effects"
	UnpinPhaseUnpinning                    = "unpinning"
	UnpinPhaseValidatingDAGAfter           = "validating_dag_after"
)

// UnpinWorkflowData represents the workflow data for unpin operations
type UnpinWorkflowData struct {
	PinRequestID    string `json:"pin_request_id"`
	CID             string `json:"cid"`
	UserID          uint   `json:"user_id"`
	CurrentPhase    string `json:"current_phase"`
	CompletedPhases int    `json:"completed_phases"`
	TotalPhases     int    `json:"total_phases"`
}

// UnpinOperationHandler handles unpinning operations with DAG dependency analysis
type UnpinOperationHandler struct {
	core.OperationHelper
}

func (h *UnpinOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}
	return nil
}

func (h *UnpinOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	// Parse the CID
	c, err := cid.Parse(req.Hash)
	if err != nil {
		return fmt.Errorf("failed to parse CID: %w", err)
	}

	userID := lo.FromPtrOr(req.UserID, 0)

	// Initialize workflow data with progress tracking
	workflowData := UnpinWorkflowData{
		PinRequestID:    strconv.FormatUint(uint64(req.ID), 10),
		CID:             c.String(),
		UserID:          userID,
		CurrentPhase:    UnpinPhaseStarting,
		CompletedPhases: 0,
		TotalPhases:     7, // Total number of major phases
	}

	err = h.UpdateWorkflowDataStruct(req.ID, workflowData)
	if err != nil {
		return fmt.Errorf("failed to initialize workflow data: %w", err)
	}

	// Start a transaction for the entire unpin operation
	_db := h.Context().DB()

	// Validate DAG integrity before unpinning
	h.updateWorkflowPhase(req.ID, &workflowData, UnpinPhaseValidatingDAGBefore, 1)

	if err := h.ValidateDAGIntegrityBeforeUnpin(ctx, c, userID); err != nil {
		h.Logger().Error("DAG integrity validation failed before unpin",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.Error(err))
		return fmt.Errorf("DAG integrity validation failed before unpin: %w", err)
	}

	txErr := db.RetryableTransaction(h.Context(), _db, func(tx *gorm.DB) *gorm.DB {
		// Analyze DAG dependencies before unpinning
		h.updateWorkflowPhase(req.ID, &workflowData, UnpinPhaseAnalyzingDAGDependencies, 2)

		analysis, err := h.AnalyzeDAGDependencies(ctx, tx, c, userID)
		if err != nil {
			h.Logger().Error("Failed to analyze DAG dependencies",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.Error(err))
			_ = tx.AddError(fmt.Errorf("failed to analyze DAG dependencies: %w", err))
			return tx
		}

		// Log the analysis results
		h.Logger().Info("DAG dependency analysis completed",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.Int("dependent_pins_count", len(analysis.DependentPins)),
			zap.Int("parent_blocks_count", len(analysis.ParentBlocks)),
			zap.Int("child_blocks_count", len(analysis.ChildBlocks)),
			zap.Bool("would_break_structure", analysis.WouldBreakStructure))

		// If this unpin would break the structure, promote dependent pins to orphan status
		if analysis.WouldBreakStructure {
			h.updateWorkflowPhase(req.ID, &workflowData, UnpinPhasePromotingOrphans, 3)

			h.Logger().Warn("Unpinning this CID would break DAG structure",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.Strings("dependent_pins", analysis.DependentPins))

			// Promote dependent pins to orphan status
			err = h.PromotePinsToOrphan(ctx, tx, analysis.DependentPins, userID)
			if err != nil {
				h.Logger().Error("Failed to promote pins to orphan status",
					zap.Stringer("cid", c),
					zap.Uint("user_id", userID),
					zap.Error(err))
				_ = tx.AddError(fmt.Errorf("failed to promote pins to orphan status: %w", err))
				return tx
			}

			// Validate orphan promotion results
			if err := h.ValidateOrphanPromotion(ctx, analysis.DependentPins, userID); err != nil {
				h.Logger().Error("Orphan promotion validation failed",
					zap.Stringer("cid", c),
					zap.Uint("user_id", userID),
					zap.Error(err))
				_ = tx.AddError(fmt.Errorf("orphan promotion validation failed: %w", err))
				return tx
			}
		}

		// Analyze file path dependencies
		h.updateWorkflowPhase(req.ID, &workflowData, UnpinPhaseAnalyzingPathDependencies, 4)

		pathAnalysis, err := h.AnalyzePathDependencies(ctx, tx, c, userID)
		if err != nil {
			h.Logger().Error("Failed to analyze path dependencies",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.Error(err))
			_ = tx.AddError(fmt.Errorf("failed to analyze path dependencies: %w", err))
			return tx
		}

		// Log path analysis results
		h.Logger().Info("Path dependency analysis completed",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.Int("affected_paths_count", len(pathAnalysis.AffectedPaths)),
			zap.Bool("would_break_paths", pathAnalysis.WouldBreakPaths))

		// Handle path dependencies if needed
		if pathAnalysis.WouldBreakPaths {
			h.updateWorkflowPhase(req.ID, &workflowData, UnpinPhaseHandlingPathCascadingEffects, 5)

			h.Logger().Warn("Unpinning this CID would affect shared path structures",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.Strings("affected_paths", pathAnalysis.AffectedPaths))

			// Handle cascading effects for shared directory structures
			err = h.HandlePathCascadingEffects(ctx, tx, c, userID, pathAnalysis)
			if err != nil {
				h.Logger().Error("Failed to handle path cascading effects",
					zap.Stringer("cid", c),
					zap.Uint("user_id", userID),
					zap.Error(err))
				_ = tx.AddError(fmt.Errorf("failed to handle path cascading effects: %w", err))
				return tx
			}
		}

		// Perform the actual unpin operation
		h.updateWorkflowPhase(req.ID, &workflowData, UnpinPhaseUnpinning, 6)

		pinSvc := core.GetService[core.PinService](h.Context(), core.PIN_SERVICE)
		if pinSvc == nil {
			_ = tx.AddError(fmt.Errorf("pin service not available"))
			return tx
		}

		err = pinSvc.DeletePinByHash(internal.NewIPFSHash(c), userID)
		if err != nil {
			h.Logger().Error("Failed to unpin CID",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.Error(err))
			_ = tx.AddError(fmt.Errorf("failed to unpin CID: %w", err))
			return tx
		}

		h.Logger().Debug("Unpinned CID successfully",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID))

		return tx
	})

	if txErr != nil {
		return txErr
	}

	// Validate DAG integrity after unpinning
	h.updateWorkflowPhase(req.ID, &workflowData, UnpinPhaseValidatingDAGAfter, 7)

	if err := h.ValidateDAGIntegrityAfterUnpin(ctx, c, userID); err != nil {
		h.Logger().Error("DAG integrity validation failed after unpin",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.Error(err))
		return fmt.Errorf("DAG integrity validation failed after unpin: %w", err)
	}

	// Final validation to ensure system consistency
	if err := h.ValidateSystemConsistency(ctx, c, userID); err != nil {
		h.Logger().Error("System consistency validation failed",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.Error(err))
		return fmt.Errorf("system consistency validation failed: %w", err)
	}

	h.Logger().Info("DAG integrity validation completed successfully",
		zap.Stringer("cid", c),
		zap.Uint("user_id", userID))

	return nil
}

// DAGDependencyAnalysis represents the results of dependency analysis
type DAGDependencyAnalysis struct {
	DependentPins       []string // CIDs of pins that depend on this block
	ParentBlocks        []string // CIDs of parent blocks in the DAG
	ChildBlocks         []string // CIDs of child blocks in the DAG
	WouldBreakStructure bool     // Whether unpinning would break the DAG structure
}

// PathDependencyAnalysis represents the results of file path dependency analysis
type PathDependencyAnalysis struct {
	AffectedPaths     []string // Paths that would be affected by unpinning
	WouldBreakPaths   bool     // Whether unpinning would break path structures
	SharedDirectories []string // Directories shared by multiple pins
	OrphanCandidates  []string // CIDs that could become orphans
}

// DAGValidationResult represents the results of DAG integrity validation
type DAGValidationResult struct {
	IsValid        bool     // Whether the DAG structure is valid
	MissingBlocks  []string // CIDs of blocks that are referenced but missing
	OrphanedBlocks []string // CIDs of blocks that are no longer referenced by any pin
	CycleDetected  bool     // Whether cycles were detected in the DAG structure
	ErrorMessage   string   // Detailed error message if validation failed
}

// AnalyzeDAGDependencies analyzes which other pins depend on the given CID
func (h *UnpinOperationHandler) AnalyzeDAGDependencies(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) (*DAGDependencyAnalysis, error) {
	analysis := &DAGDependencyAnalysis{
		DependentPins: make([]string, 0),
		ParentBlocks:  make([]string, 0),
		ChildBlocks:   make([]string, 0),
	}

	blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)
	if blockSvc == nil {
		return nil, fmt.Errorf("block service not available")
	}

	// Get all pins for this user to check dependencies
	allPins, err := h.GetAllUserPins(ctx, tx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user pins: %w", err)
	}

	// For each pin, check if it depends on the block we're about to unpin
	for _, pin := range allPins {
		// Skip the pin that we're currently unpinning
		pinCID, err := cid.Cast(pin.CID)
		if err != nil {
			h.Logger().Warn("Failed to cast pin CID during dependency analysis",
				zap.Binary("cid_bytes", pin.CID),
				zap.Error(err))
			continue
		}

		if pinCID.Equals(c) {
			continue
		}

		// Check if this pin depends on our target CID
		depends, err := h.DoesPinDependOnCID(ctx, blockSvc, pinCID, c)
		if err != nil {
			h.Logger().Error("Failed to check pin dependency",
				zap.Stringer("pin_cid", pinCID),
				zap.Stringer("target_cid", c),
				zap.Error(err))
			continue
		}

		if depends {
			analysis.DependentPins = append(analysis.DependentPins, pinCID.String())
		}
	}

	// Get parent and child relationships for the target CID
	parents, children, err := h.GetBlockRelationships(ctx, tx, blockSvc, c)
	if err != nil {
		return nil, fmt.Errorf("failed to get block relationships: %w", err)
	}

	analysis.ParentBlocks = parents
	analysis.ChildBlocks = children

	// Determine if unpinning would break the structure
	// If there are dependent pins, unpinning would break the structure
	analysis.WouldBreakStructure = len(analysis.DependentPins) > 0

	return analysis, nil
}

// GetAllUserPins retrieves all pins for a specific user
func (h *UnpinOperationHandler) GetAllUserPins(ctx context.Context, tx *gorm.DB, userID uint) ([]*pluginDb.IPFSPin, error) {
	var pins []*pluginDb.IPFSPin

	err := tx.WithContext(ctx).Where("user_id = ?", userID).Find(&pins).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get user pins: %w", err)
	}

	return pins, nil
}

// DoesPinDependOnCID checks if a pin depends on a specific CID in its DAG structure
func (h *UnpinOperationHandler) DoesPinDependOnCID(ctx context.Context, blockSvc pluginCore.BlockService, pinCID cid.Cid, targetCID cid.Cid) (bool, error) {
	// Get the metadata for the pin's root block
	rootMeta, err := blockSvc.GetBlockMeta(ctx, pinCID)
	if err != nil {
		return false, err
	}

	if rootMeta == nil {
		return false, nil
	}

	// Recursively check if any block in this pin's DAG references the target CID
	return h.CheckDAGForCID(ctx, blockSvc, pinCID, targetCID, make(map[string]bool))
}

// CheckDAGForCID recursively traverses a DAG to see if it contains a specific CID
func (h *UnpinOperationHandler) CheckDAGForCID(ctx context.Context, blockSvc pluginCore.BlockService, currentCID cid.Cid, targetCID cid.Cid, visited map[string]bool) (bool, error) {
	// If we've already visited this CID, skip to prevent infinite loops
	cidStr := currentCID.String()
	if visited[cidStr] {
		return false, nil
	}
	visited[cidStr] = true

	// If current CID matches target CID, we found a dependency
	if currentCID.Equals(targetCID) {
		return true, nil
	}

	// Get metadata for the current block
	meta, err := blockSvc.GetBlockMeta(ctx, currentCID)
	if err != nil {
		return false, err
	}

	if meta == nil || len(meta.ChildCID) == 0 {
		return false, nil
	}

	// Check each child
	for _, childCID := range meta.ChildCID {
		found, err := h.CheckDAGForCID(ctx, blockSvc, childCID, targetCID, visited)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}

	return false, nil
}

// GetBlockRelationships retrieves parent and child relationships for a CID
func (h *UnpinOperationHandler) GetBlockRelationships(ctx context.Context, tx *gorm.DB, blockSvc pluginCore.BlockService, c cid.Cid) (parents []string, children []string, err error) {
	// Get metadata for the block
	meta, err := blockSvc.GetBlockMeta(ctx, c)
	if err != nil {
		return nil, nil, err
	}

	if meta == nil {
		return make([]string, 0), make([]string, 0), nil
	}

	// Collect child CIDs
	children = make([]string, len(meta.ChildCID))
	for i, childCID := range meta.ChildCID {
		children[i] = childCID.String()
	}

	// To find parents, we need to query the linked blocks table
	var linkedBlocks []pluginDb.IPFSLinkedBlock

	err = tx.WithContext(ctx).
		Where("child_id = (SELECT id FROM ipfs_blocks WHERE cid = ?)", c.Bytes()).
		Find(&linkedBlocks).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, nil, err
	}

	// Collect parent CIDs
	parents = make([]string, 0, len(linkedBlocks))
	for _, link := range linkedBlocks {
		var parentBlock pluginDb.IPFSBlock
		err := tx.WithContext(ctx).Where("id = ?", link.ParentID).First(&parentBlock).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			h.Logger().Warn("Failed to get parent block", zap.Error(err))
			continue
		}
		parentCID, err := cid.Cast(parentBlock.CID)
		if err != nil {
			h.Logger().Warn("Failed to cast parent CID", zap.Error(err))
			continue
		}
		parents = append(parents, parentCID.String())
	}

	return parents, children, nil
}

// PromotePinsToOrphan updates file paths for dependent pins to mark them as orphans
func (h *UnpinOperationHandler) PromotePinsToOrphan(ctx context.Context, tx *gorm.DB, dependentPins []string, userID uint) error {
	fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)
	if fileManagerSvc == nil {
		return fmt.Errorf("file manager service not available")
	}

	for _, pinCIDStr := range dependentPins {
		pinCID, err := cid.Parse(pinCIDStr)
		if err != nil {
			h.Logger().Warn("Failed to parse dependent pin CID during orphan promotion",
				zap.String("cid", pinCIDStr),
				zap.Error(err))
			continue
		}

		// Update file paths to mark them as orphans
		err = h.UpdatePathsToOrphanWithTx(ctx, tx, pinCID, userID)
		if err != nil {
			h.Logger().Error("Failed to update paths to orphan status",
				zap.Stringer("cid", pinCID),
				zap.Uint("user_id", userID),
				zap.Error(err))
			return err
		}

		h.Logger().Info("Successfully promoted pin to orphan status",
			zap.Stringer("cid", pinCID),
			zap.Uint("user_id", userID))
	}

	return nil
}

// AnalyzePathDependencies analyzes file path dependencies for the given CID
func (h *UnpinOperationHandler) AnalyzePathDependencies(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) (*PathDependencyAnalysis, error) {
	analysis := &PathDependencyAnalysis{
		AffectedPaths:     make([]string, 0),
		SharedDirectories: make([]string, 0),
		OrphanCandidates:  make([]string, 0),
	}

	fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)
	if fileManagerSvc == nil {
		return nil, fmt.Errorf("file manager service not available")
	}

	// Get the file path for the CID being unpinned
	var targetPath pluginDb.FilePath

	err := tx.WithContext(ctx).Where("user_id = ? AND cid = ?", userID, c.Bytes()).First(&targetPath).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// If no path exists, there are no path dependencies
			analysis.WouldBreakPaths = false
			return analysis, nil
		}
		return nil, fmt.Errorf("failed to get target file path: %w", err)
	}

	// Check if this path is shared with other pins
	shared, err := h.IsPathShared(ctx, tx, c, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to check if path is shared: %w", err)
	}

	if shared {
		// If shared, we need to identify what would be affected
		analysis.WouldBreakPaths = true

		// Get all paths that would be affected by this unpin
		affectedPaths, err := h.GetAffectedPaths(ctx, tx, targetPath.Path, userID)
		if err != nil {
			return nil, fmt.Errorf("failed to get affected paths: %w", err)
		}
		analysis.AffectedPaths = affectedPaths

		// Identify shared directories in the path hierarchy
		sharedDirs, err := h.GetSharedDirectories(ctx, tx, targetPath.Path, userID)
		if err != nil {
			return nil, fmt.Errorf("failed to get shared directories: %w", err)
		}
		analysis.SharedDirectories = sharedDirs

		// Identify orphan candidates (child paths that might become orphans)
		orphanCandidates, err := h.GetOrphanCandidates(ctx, tx, targetPath.Path, userID)
		if err != nil {
			return nil, fmt.Errorf("failed to get orphan candidates: %w", err)
		}
		analysis.OrphanCandidates = orphanCandidates
	} else {
		// If not shared, check if removing it would break directory structures
		analysis.WouldBreakPaths = h.WouldBreakDirectoryStructure(targetPath)
		analysis.AffectedPaths = []string{targetPath.Path}
	}

	return analysis, nil
}

// IsPathShared checks if a path is shared by multiple pins for the same user
func (h *UnpinOperationHandler) IsPathShared(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) (bool, error) {
	// Get all pins for this user
	allPins, err := h.GetAllUserPins(ctx, tx, userID)
	if err != nil {
		return false, fmt.Errorf("failed to get user pins: %w", err)
	}

	// Count how many pins have the same CID as the target
	pinCount := 0
	for _, pin := range allPins {
		pinCID, err := cid.Cast(pin.CID)
		if err != nil {
			h.Logger().Warn("Failed to cast pin CID during shared path analysis",
				zap.Binary("cid_bytes", pin.CID),
				zap.Error(err))
			continue
		}

		// Count pins that match the target CID
		if pinCID.Equals(c) {
			pinCount++
		}
	}

	// If there's more than one pin with the same CID, the path is shared
	return pinCount > 1, nil
}

// GetAffectedPaths retrieves all file paths that would be affected by unpinning a specific path
func (h *UnpinOperationHandler) GetAffectedPaths(ctx context.Context, tx *gorm.DB, targetPath string, userID uint) ([]string, error) {
	var affectedPaths []string

	// Get all file paths for this user
	var allPaths []pluginDb.FilePath
	err := tx.WithContext(ctx).Where("user_id = ?", userID).Find(&allPaths).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, fmt.Errorf("failed to get all file paths: %w", err)
	}

	// Check each path to see if it would be affected
	for _, path := range allPaths {
		// A path is affected if:
		// 1. It's the target path itself
		// 2. It's a child of the target path
		// 3. It's a parent of the target path that becomes empty
		if path.Path == targetPath || strings.HasPrefix(path.Path, targetPath+"/") {
			affectedPaths = append(affectedPaths, path.Path)
		}
	}

	return affectedPaths, nil
}

// GetSharedDirectories identifies directories that are shared by multiple pins
func (h *UnpinOperationHandler) GetSharedDirectories(ctx context.Context, tx *gorm.DB, targetPath string, userID uint) ([]string, error) {
	sharedDirs := make([]string, 0)

	// Extract directory hierarchy from target path
	pathParts := strings.Split(strings.Trim(targetPath, "/"), "/")
	var dirs []string
	currentPath := ""

	for i, part := range pathParts {
		if part == "" {
			continue
		}
		if currentPath == "" {
			currentPath = "/" + part
		} else {
			currentPath += "/" + part
		}

		// Only consider directories (not the final file)
		if i < len(pathParts)-1 {
			dirs = append(dirs, currentPath)
		}
	}

	// For each directory, check if it's shared
	for _, dir := range dirs {
		shared, err := h.IsDirectoryShared(ctx, tx, dir, userID)
		if err != nil {
			h.Logger().Warn("Failed to check if directory is shared",
				zap.String("directory", dir),
				zap.Error(err))
			continue
		}

		if shared {
			sharedDirs = append(sharedDirs, dir)
		}
	}

	return sharedDirs, nil
}

// IsDirectoryShared checks if a directory path is shared by multiple pins
func (h *UnpinOperationHandler) IsDirectoryShared(ctx context.Context, tx *gorm.DB, dirPath string, userID uint) (bool, error) {

	// Count how many pins reference this directory path
	var pinCount int64
	err := tx.WithContext(ctx).
		Model(&pluginDb.IPFSPin{}).
		Joins("JOIN ipfs_file_paths ifp ON ipfs_pins.user_id = ifp.user_id AND ipfs_pins.cid = ifp.cid").
		Where("ipfs_pins.user_id = ? AND ifp.parent_path = ?", userID, dirPath).
		Count(&pinCount).Error

	if err != nil {
		return false, fmt.Errorf("failed to count pins referencing directory: %w", err)
	}

	

	// If more than one pin references this directory, it's shared
	return pinCount > 1, nil
}

// GetOrphanCandidates identifies CIDs that might become orphans when a path is removed
func (h *UnpinOperationHandler) GetOrphanCandidates(ctx context.Context, tx *gorm.DB, targetPath string, userID uint) ([]string, error) {
	var orphanCandidates []string

	// Get all child paths of the target path
	var childPaths []pluginDb.FilePath
	err := tx.WithContext(ctx).
		Where("user_id = ? AND path LIKE ?", userID, targetPath+"/%").
		Find(&childPaths).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, fmt.Errorf("failed to get child paths: %w", err)
	}

	// For each child path, check if its CID has any pins
	for _, childPath := range childPaths {
		c, err := cid.Cast(childPath.CID)
		if err != nil {
			h.Logger().Warn("Failed to cast child CID during orphan analysis",
				zap.Binary("cid_bytes", childPath.CID),
				zap.Error(err))
			continue
		}

		// Check if this CID has any pins
		var pinCount int64
		err = tx.WithContext(ctx).
			Model(&pluginDb.IPFSPin{}).
			Where("user_id = ? AND cid = ?", userID, c.Bytes()).
			Count(&pinCount).Error
		if err != nil {
			h.Logger().Warn("Failed to count pins for child CID",
				zap.Stringer("cid", c),
				zap.Error(err))
			continue
		}

		// If no pins exist for this CID, it's a candidate for orphaning
		if pinCount == 0 {
			orphanCandidates = append(orphanCandidates, c.String())
		}
	}

	return orphanCandidates, nil
}

// WouldBreakDirectoryStructure checks if removing a path would break a directory structure
func (h *UnpinOperationHandler) WouldBreakDirectoryStructure(path pluginDb.FilePath) bool {
	// If this is a directory, removing it would break the structure
	return path.IsDirectory
}

// HandlePathCascadingEffects manages the cascading effects of unpinning on file paths
func (h *UnpinOperationHandler) HandlePathCascadingEffects(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint, analysis *PathDependencyAnalysis) error {
	fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)
	if fileManagerSvc == nil {
		return fmt.Errorf("file manager service not available")
	}

	// Handle orphan candidates - promote them to orphan status
	if len(analysis.OrphanCandidates) > 0 {
		h.Logger().Info("Promoting orphan candidates to orphan status",
			zap.Int("count", len(analysis.OrphanCandidates)))

		for _, cidStr := range analysis.OrphanCandidates {
			orphanCID, err := cid.Parse(cidStr)
			if err != nil {
				h.Logger().Warn("Failed to parse orphan candidate CID",
					zap.String("cid", cidStr),
					zap.Error(err))
				continue
			}

			err = h.UpdatePathsToOrphanWithTx(ctx, tx, orphanCID, userID)
			if err != nil {
				h.Logger().Error("Failed to update paths to orphan status for candidate",
					zap.Stringer("cid", orphanCID),
					zap.Uint("user_id", userID),
					zap.Error(err))
				return err
			}
		}
	}

	// Handle shared directories - preserve them if they still have content
	if len(analysis.SharedDirectories) > 0 {
		h.Logger().Info("Preserving shared directories",
			zap.Int("count", len(analysis.SharedDirectories)))

		// In this implementation, we're not actually removing shared directories
		// since they might still be needed by other pins
		// The FileManagerService's DeleteFilePathSmart should handle this logic
	}

	return nil
}

// UpdatePathsToOrphan moves orphaned pins to root level path structure and marks them as orphans
func (h *UnpinOperationHandler) UpdatePathsToOrphan(ctx context.Context, fileManagerSvc pluginCore.FileManagerService, c cid.Cid, userID uint) error {
	// Get all file paths for this CID and user
	var paths []pluginDb.FilePath
	_db := h.Context().DB()

	err := _db.WithContext(ctx).
		Where("user_id = ? AND cid = ?", userID, c.Bytes()).
		Find(&paths).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return fmt.Errorf("failed to get file paths: %w", err)
	}

	// Update each path to be an orphan at root level
	for _, path := range paths {
		// Move to root level with CID as name
		rootPath := "/" + c.String()

		err := _db.WithContext(ctx).
			Model(&pluginDb.FilePath{}).
			Where("id = ?", path.ID).
			Updates(map[string]interface{}{
				"path":         rootPath,
				"parent_path":  "",
				"name":         c.String(),
				"depth":        0,
				"is_directory": false,
				"is_orphan":    true,
			}).Error
		if err != nil {
			h.Logger().Error("Failed to update file path to orphan status",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.String("path", path.Path),
				zap.Error(err))
			return err
		}

		h.Logger().Debug("Updated file path to orphan status",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.String("old_path", path.Path),
			zap.String("new_path", rootPath))
	}

	return nil
}

// UpdatePathsToOrphanWithTx moves orphaned pins to root level path structure and marks them as orphans using a transaction
func (h *UnpinOperationHandler) UpdatePathsToOrphanWithTx(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) error {
	// Get all file paths for this CID and user
	var paths []pluginDb.FilePath

	err := tx.WithContext(ctx).
		Where("user_id = ? AND cid = ?", userID, c.Bytes()).
		Find(&paths).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return fmt.Errorf("failed to get file paths: %w", err)
	}

	// Update each path to be an orphan at root level
	for _, path := range paths {
		// Move to root level with CID as name
		rootPath := "/" + c.String()

		err := tx.WithContext(ctx).
			Model(&pluginDb.FilePath{}).
			Where("id = ?", path.ID).
			Updates(map[string]interface{}{
				"path":         rootPath,
				"parent_path":  "",
				"name":         c.String(),
				"depth":        0,
				"is_directory": false,
				"is_orphan":    true,
			}).Error
		if err != nil {
			h.Logger().Error("Failed to update file path to orphan status",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.String("path", path.Path),
				zap.Error(err))
			return err
		}

		h.Logger().Debug("Updated file path to orphan status",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.String("old_path", path.Path),
			zap.String("new_path", rootPath))
	}

	return nil
}

func (h *UnpinOperationHandler) GetStatus(ctx context.Context, req *models.Request) (*core.RequestStatus, error) {
	status := &core.RequestStatus{
		ProgressPercent: 0,
	}

	// Extract values from workflow data
	var unpinData UnpinWorkflowData
	err := h.StructuredWorkflowData(req.ID, &unpinData)
	if err != nil {
		return nil, err
	}

	currentPhase := unpinData.CurrentPhase
	completedPhases := float64(unpinData.CompletedPhases)
	totalPhases := float64(unpinData.TotalPhases)

	// Determine status based on request state
	status.State = req.Status
	switch req.Status {
	case models.RequestStatusPending:
		status.Message = "Unpin operation is queued"
		status.ProgressPercent = 0
	case models.RequestStatusProcessing:
		status.Message = "Unpin operation in progress: " + currentPhase
		// Compute real progress based on completed phases
		if totalPhases > 0 {
			status.ProgressPercent = float64(int((completedPhases * 100) / totalPhases))
		} else {
			status.ProgressPercent = 0
		}
	case models.RequestStatusCompleted:
		status.Message = "Unpin operation completed successfully"
		status.ProgressPercent = 100
	case models.RequestStatusFailed:
		status.Message = "Unpin operation failed"
		status.ProgressPercent = 100
	default:
		status.Message = "Unpin operation status unknown"
		status.ProgressPercent = 0
	}

	return status, nil
}

// updateWorkflowPhase updates the workflow data with the current phase and completed phases count
func (h *UnpinOperationHandler) updateWorkflowPhase(requestID uint, workflowData *UnpinWorkflowData, phase string, completedPhases int) {
	workflowData.CurrentPhase = phase
	workflowData.CompletedPhases = completedPhases
	err := h.UpdateWorkflowDataStruct(requestID, workflowData)
	if err != nil {
		h.Logger().Error("Failed to update workflow data",
			zap.String("phase", phase),
			zap.Error(err))
	}
}

func (h *UnpinOperationHandler) Cleanup(_ context.Context, _ *models.Request) error {
	return nil
}

// ValidateDAGIntegrityBeforeUnpin validates the DAG structure before unpinning
func (h *UnpinOperationHandler) ValidateDAGIntegrityBeforeUnpin(ctx context.Context, c cid.Cid, userID uint) error {
	blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)
	if blockSvc == nil {
		return fmt.Errorf("block service not available")
	}

	// Check if the block exists
	_, err := blockSvc.GetBlockMeta(ctx, c)
	if err != nil {
		return fmt.Errorf("target block does not exist: %w", err)
	}

	// Validate the entire DAG structure for this user
	result, err := h.ValidateUserDAGStructure(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed to validate DAG structure: %w", err)
	}

	if !result.IsValid {
		return fmt.Errorf("DAG structure is invalid before unpin: %s", result.ErrorMessage)
	}

	h.Logger().Debug("DAG integrity validation passed before unpin",
		zap.Stringer("cid", c),
		zap.Uint("user_id", userID))

	return nil
}

// ValidateDAGIntegrityAfterUnpin validates the DAG structure after unpinning
func (h *UnpinOperationHandler) ValidateDAGIntegrityAfterUnpin(ctx context.Context, c cid.Cid, userID uint) error {
	// Validate the entire DAG structure for this user after unpinning
	result, err := h.ValidateUserDAGStructure(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed to validate DAG structure after unpin: %w", err)
	}

	if !result.IsValid {
		return fmt.Errorf("DAG structure is invalid after unpin: %s", result.ErrorMessage)
	}

	h.Logger().Debug("DAG integrity validation passed after unpin",
		zap.Stringer("cid", c),
		zap.Uint("user_id", userID))

	return nil
}

// ValidateUserDAGStructure validates the entire DAG structure for a user
func (h *UnpinOperationHandler) ValidateUserDAGStructure(ctx context.Context, userID uint) (*DAGValidationResult, error) {
	result := &DAGValidationResult{
		IsValid:        true,
		MissingBlocks:  make([]string, 0),
		OrphanedBlocks: make([]string, 0),
	}

	blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)
	if blockSvc == nil {
		return nil, fmt.Errorf("block service not available")
	}

	// Get all pins for this user
	pins, err := h.GetAllUserPins(ctx, h.Context().DB(), userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user pins: %w", err)
	}

	// Create a map of pinned CIDs for quick lookup
	pinnedCIDs := make(map[string]bool)
	for _, pin := range pins {
		pinCID, err := cid.Cast(pin.CID)
		if err != nil {
			h.Logger().Warn("Failed to cast pin CID during validation",
				zap.Binary("cid_bytes", pin.CID),
				zap.Error(err))
			continue
		}
		pinnedCIDs[pinCID.String()] = true
	}

	// Validate each pin's DAG structure
	processedBlocks := make(map[string]bool)
	for _, pin := range pins {
		pinCID, err := cid.Cast(pin.CID)
		if err != nil {
			h.Logger().Warn("Failed to cast pin CID during validation",
				zap.Binary("cid_bytes", pin.CID),
				zap.Error(err))
			continue
		}

		// Validate this pin's DAG
		missing, cycle, err := h.ValidateDAG(ctx, blockSvc, pinCID, pinnedCIDs, processedBlocks)
		if err != nil {
			result.IsValid = false
			result.ErrorMessage = fmt.Sprintf("failed to validate DAG for pin %s: %v", pinCID.String(), err)
			return result, nil
		}

		if cycle {
			result.IsValid = false
			result.CycleDetected = true
			result.ErrorMessage = fmt.Sprintf("cycle detected in DAG for pin %s", pinCID.String())
			return result, nil
		}

		result.MissingBlocks = append(result.MissingBlocks, missing...)
	}

	// If there are missing blocks, the DAG is invalid
	if len(result.MissingBlocks) > 0 {
		result.IsValid = false
		result.ErrorMessage = fmt.Sprintf("missing blocks in DAG: %v", result.MissingBlocks)
	}

	return result, nil
}

// ValidateDAG recursively validates a DAG structure
func (h *UnpinOperationHandler) ValidateDAG(ctx context.Context, blockSvc pluginCore.BlockService, currentCID cid.Cid, pinnedCIDs map[string]bool, processedBlocks map[string]bool) ([]string, bool, error) {
	cidStr := currentCID.String()

	// If we've already processed this block, skip to prevent infinite loops
	if processedBlocks[cidStr] {
		return nil, false, nil
	}
	processedBlocks[cidStr] = true

	// Check if the block exists
	meta, err := blockSvc.GetBlockMeta(ctx, currentCID)
	if err != nil {
		return []string{cidStr}, false, nil
	}

	if meta == nil {
		return []string{cidStr}, false, nil
	}

	// Check for cycles in the DAG
	var missingBlocks []string
	var cycleDetected bool

	// Validate each child block
	for _, childCID := range meta.ChildCID {
		childCIDStr := childCID.String()

		// Check if child block exists
		childMeta, err := blockSvc.GetBlockMeta(ctx, childCID)
		if err != nil || childMeta == nil {
			missingBlocks = append(missingBlocks, childCIDStr)
			continue
		}

		// Recursively validate child DAG
		missing, cycle, err := h.ValidateDAG(ctx, blockSvc, childCID, pinnedCIDs, processedBlocks)
		if err != nil {
			return nil, false, err
		}

		if cycle {
			cycleDetected = true
			break
		}

		missingBlocks = append(missingBlocks, missing...)
	}

	return missingBlocks, cycleDetected, nil
}

// ValidateOrphanPromotion validates that orphan promotion was successful
func (h *UnpinOperationHandler) ValidateOrphanPromotion(ctx context.Context, dependentPins []string, userID uint) error {
	__db := h.Context().DB()

	for _, pinCIDStr := range dependentPins {
		pinCID, err := cid.Parse(pinCIDStr)
		if err != nil {
			h.Logger().Warn("Failed to parse dependent pin CID during validation",
				zap.String("cid", pinCIDStr),
				zap.Error(err))
			continue
		}

		// Check if file paths exist for this pin and are marked as orphan
		var paths []pluginDb.FilePath
		err = __db.WithContext(ctx).
			Where("user_id = ? AND cid = ?", userID, pinCID.Bytes()).
			Find(&paths).Error
		if err != nil {
			return fmt.Errorf("failed to get file paths for pin %s: %w", pinCIDStr, err)
		}

		// Validate that all paths are marked as orphan
		for _, path := range paths {
			if !path.IsOrphan {
				return fmt.Errorf("file path %s for pin %s is not marked as orphan", path.Path, pinCIDStr)
			}
		}
	}

	return nil
}

// ValidateSystemConsistency performs final validation to ensure system consistency
func (h *UnpinOperationHandler) ValidateSystemConsistency(ctx context.Context, c cid.Cid, userID uint) error {
	__db := h.Context().DB()

	// Check that the pin record no longer exists
	var pinCount int64
	err := __db.WithContext(ctx).
		Model(&pluginDb.IPFSPin{}).
		Where("user_id = ? AND cid = ?", userID, c.Bytes()).
		Count(&pinCount).Error
	if err != nil {
		return fmt.Errorf("failed to check pin record existence: %w", err)
	}

	if pinCount > 0 {
		return fmt.Errorf("pin record still exists after unpin operation")
	}

	// Check that file paths have been properly handled
	var pathCount int64
	err = __db.WithContext(ctx).
		Model(&pluginDb.FilePath{}).
		Where("user_id = ? AND cid = ?", userID, c.Bytes()).
		Count(&pathCount).Error
	if err != nil {
		return fmt.Errorf("failed to check file path existence: %w", err)
	}

	// If there are still file paths, they should all be marked as orphan
	if pathCount > 0 {
		var nonOrphanPaths int64
		err = __db.WithContext(ctx).
			Model(&pluginDb.FilePath{}).
			Where("user_id = ? AND cid = ? AND is_orphan = ?", userID, c.Bytes(), false).
			Count(&nonOrphanPaths).Error
		if err != nil {
			return fmt.Errorf("failed to check non-orphan file paths: %w", err)
		}

		if nonOrphanPaths > 0 {
			return fmt.Errorf("found %d non-orphan file paths for unpinned CID", nonOrphanPaths)
		}
	}

	return nil
}

func NewUnpinOperation(ctx core.Context) core.Operation {
	return core.NewOperation(
		UnpinOperationName(),
		"", // No global type for unpin
		&UnpinOperationHandler{
			OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
		},
	)
}

func UnpinOperationName() string {
	return "ipfs.unpin"
}
