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
	UnpinPhaseStarting                        = "starting"
	UnpinPhaseValidatingDAGBefore             = "validating_dag_before"
	UnpinPhaseAnalyzingDAGDependencies        = "analyzing_dag_dependencies"
	UnpinPhasePromotingToRootLevelVisibility = "promoting_to_root_level_visibility"
	UnpinPhaseAnalyzingPathDependencies       = "analyzing_path_dependencies"
	UnpinPhaseHandlingPathCascadingEffects    = "handling_path_cascading_effects"
	UnpinPhaseUnpinning                       = "unpinning"
	UnpinPhaseValidatingDAGAfter              = "validating_dag_after"
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
	// Check for nil or empty hash
	if req == nil || req.Hash == nil || len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}

	// Check for valid user ID
	if req.UserID == nil || *req.UserID == 0 {
		return fmt.Errorf("user ID is required")
	}

	// Try to parse the CID to validate it
	_, err := cid.Parse(req.Hash)
	if err != nil {
		return fmt.Errorf("invalid cid")
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

	if err := h.ValidateDAGIntegrityBeforeUnpin(ctx, _db, c, userID); err != nil {
		h.Logger().Error("DAG integrity validation failed before unpin",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.Error(err))
		return fmt.Errorf("DAG integrity validation failed before unpin: %w", err)
	}

	txErr := db.RetryableTransaction(h.Context(), _db, func(tx *gorm.DB) *gorm.DB {
		// Analyze unpin impact before unpinning
		// This checks if unpinning this CID would create orphans in the file UI
		// We're specifically looking for child CIDs that are also pinned by the user
		// If found, these children need to be promoted to root-level paths to maintain file UI consistency
		h.updateWorkflowPhase(req.ID, &workflowData, UnpinPhaseAnalyzingDAGDependencies, 2)

		analysis, err := h.AnalyzeUnpinImpact(ctx, tx, c, userID)
		if err != nil {
			h.Logger().Error("Failed to analyze unpin impact",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.Error(err))
			_ = tx.AddError(fmt.Errorf("failed to analyze unpin impact: %w", err))
			return tx
		}

		// Log the analysis results
		h.Logger().Info("Unpin impact analysis completed",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.Bool("would_create_orphans", analysis.WouldCreateOrphans),
			zap.Int("orphan_candidates_count", len(analysis.RootLevelCandidates)),
			zap.Int("all_children_count", len(analysis.AllChildren)))

		// If this unpin would create orphans in the file UI, promote those children to root-level visibility
		// This ensures that even though the parent is being unpinned, users can still access the child files
		// in the root of their file UI rather than losing access to them entirely
		if analysis.WouldCreateOrphans {
			h.updateWorkflowPhase(req.ID, &workflowData, UnpinPhasePromotingToRootLevelVisibility, 3)

			h.Logger().Warn("Unpinning this CID would create orphans in file UI",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.Strings("root_level_candidates", analysis.RootLevelCandidates))

			// Promote dependent pins to root-level visibility in the file UI
			// The analysis.RootLevelCandidates list contains child CIDs that are pinned by the user
			// These children will become orphans when we unpin the parent, so we move them to root-level paths
			err = h.PromotePinsToRootLevelVisibility(ctx, tx, analysis.RootLevelCandidates, userID)
			if err != nil {
				h.Logger().Error("Failed to promote pins to root level visibility",
					zap.Stringer("cid", c),
					zap.Uint("user_id", userID),
					zap.Error(err))
				_ = tx.AddError(fmt.Errorf("failed to promote pins to root level visibility: %w", err))
				return tx
			}

			// Validate root level visibility promotion results
			if err := h.ValidateRootLevelVisibilityPromotion(ctx, analysis.RootLevelCandidates, userID); err != nil {
				h.Logger().Error("Root level visibility promotion validation failed",
					zap.Stringer("cid", c),
					zap.Uint("user_id", userID),
					zap.Error(err))
				_ = tx.AddError(fmt.Errorf("root level visibility promotion validation failed: %w", err))
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

		// Clean up file paths after unpinning
		fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)
		if fileManagerSvc != nil {
			// Use smart deletion to properly clean up file paths
			// This will only delete paths that are no longer referenced by other pins
			err = fileManagerSvc.DeleteFilePathSmart(ctx, userID, c.Bytes())
			if err != nil {
				h.Logger().Error("Failed to clean up file paths after unpin",
					zap.Stringer("cid", c),
					zap.Uint("user_id", userID),
					zap.Error(err))
				_ = tx.AddError(fmt.Errorf("failed to clean up file paths: %w", err))
				return tx
			}
		}

		// Final validation to ensure system consistency
		if err := h.ValidateSystemConsistency(ctx, c, userID); err != nil {
			h.Logger().Error("System consistency validation failed",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.Error(err))
			_ = tx.AddError(fmt.Errorf("system consistency validation failed: %w", err))
			return tx
		}

		return tx
	})

	if txErr != nil {
		return txErr
	}

	// Validate DAG integrity after unpinning
	h.updateWorkflowPhase(req.ID, &workflowData, UnpinPhaseValidatingDAGAfter, 7)

	if err := h.ValidateDAGIntegrityAfterUnpin(ctx, _db, c, userID); err != nil {
		h.Logger().Error("DAG integrity validation failed after unpin",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.Error(err))
		return fmt.Errorf("DAG integrity validation failed after unpin: %w", err)
	}

	h.Logger().Info("DAG integrity validation completed successfully",
		zap.Stringer("cid", c),
		zap.Uint("user_id", userID))

	return nil
}

// UnpinImpactAnalysis represents the results of unpin impact analysis
// This analysis is specifically concerned with maintaining file UI consistency when a CID is unpinned.
// We only care about children that need to be promoted to root-level visibility - parent dependencies
// don't matter for structure breaking. The goal is to identify which children need to be promoted
// to root-level paths in the file UI so users can still access them even after the parent is unpinned.
type UnpinImpactAnalysis struct {
	TargetCID             string   // The CID being analyzed for unpinning
	WouldCreateOrphans    bool     // True if unpinning this CID would require promoting child files to root-level visibility
	RootLevelCandidates   []string // Child CIDs that are pinned by the user and need to be promoted to root-level visibility
	AllChildren           []string // All child CIDs of the target CID (for informational purposes)
}

// PathDependencyAnalysis represents the results of file path dependency analysis
type PathDependencyAnalysis struct {
	AffectedPaths        []string // Paths that would be affected by unpinning
	WouldBreakPaths      bool     // Whether unpinning would break path structures
	SharedDirectories    []string // Directories shared by multiple pins
	RootLevelCandidates  []string // CIDs that need to be promoted to root-level visibility
}

// DAGValidationResult represents the results of DAG integrity validation
type DAGValidationResult struct {
	IsValid        bool     // Whether the DAG structure is valid
	MissingBlocks  []string // CIDs of blocks that are referenced but missing
	OrphanedBlocks []string // CIDs of blocks that are no longer referenced by any pin
	CycleDetected  bool     // Whether cycles were detected in the DAG structure
	ErrorMessage   string   // Detailed error message if validation failed
}

// AnalyzeUnpinImpact analyzes the impact of unpinning a CID, specifically looking for children that would become orphans in the file UI
// This method is concerned with maintaining file UI consistency, not DAG integrity. The step-by-step logic is:
// 1. Get all child relationships of the target CID being unpinned
// 2. Check if any of those child blocks are also pinned by the same user
// 3. If child blocks are pinned, they would become orphans when we unpin the parent
// 4. These orphaned children need to be promoted to root-level paths in the file UI so users can still access them
// We only care about children becoming orphans because parent dependencies don't affect file UI structure breaking.
func (h *UnpinOperationHandler) AnalyzeUnpinImpact(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) (*UnpinImpactAnalysis, error) {
	h.Logger().Debug("Starting unpin impact analysis",
		zap.Stringer("target_cid", c),
		zap.Uint("user_id", userID))

	analysis := &UnpinImpactAnalysis{
		TargetCID: c.String(),
	}

	// Get parent and child relationships for the target CID
	_, children, err := h.getBlockRelationshipsWithLogging(ctx, tx, c, userID)
	if err != nil {
		return nil, err
	}

	// Populate all children
	analysis.AllChildren = children

	// Check if any child blocks are pinned by this user
	// These would become orphans if we unpin the target CID
	pinnedDescendants, err := h.findPinnedChildBlocks(ctx, tx, c, userID, children)
	if err != nil {
		return nil, err
	}

	// Filter out the target CID itself from the candidates
	var rootLevelCandidates []string
	for _, descendant := range pinnedDescendants {
		if descendant != c.String() {
			rootLevelCandidates = append(rootLevelCandidates, descendant)
		}
	}
	analysis.RootLevelCandidates = rootLevelCandidates

	// Determine if unpinning would create orphans in the file UI
	analysis.WouldCreateOrphans = len(analysis.RootLevelCandidates) > 0

	h.Logger().Debug("Unpin impact analysis completed",
		zap.Stringer("cid", c),
		zap.Uint("user_id", userID),
		zap.Strings("root_level_candidates", analysis.RootLevelCandidates),
		zap.Strings("all_children", analysis.AllChildren),
		zap.Bool("would_create_orphans", analysis.WouldCreateOrphans))

	return analysis, nil
}

// getBlockRelationshipsWithLogging retrieves parent and child relationships for a CID with logging
func (h *UnpinOperationHandler) getBlockRelationshipsWithLogging(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) (parents []string, children []string, err error) {
	blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)
	if blockSvc == nil {
		h.Logger().Error("Block service not available during DAG dependency analysis")
		return nil, nil, fmt.Errorf("block service not available")
	}

	// Get parent and child relationships for the target CID
	parents, children, err = h.GetBlockRelationships(ctx, tx, blockSvc, c, userID)
	if err != nil {
		h.Logger().Error("Failed to get block relationships",
			zap.Stringer("cid", c),
			zap.Uint("user_id", userID),
			zap.Error(err))
		return nil, nil, fmt.Errorf("failed to get block relationships: %w", err)
	}

	h.Logger().Debug("Block relationships found",
		zap.Stringer("cid", c),
		zap.Int("parent_count", len(parents)),
		zap.Int("child_count", len(children)))

	return parents, children, nil
}

// findPinnedChildBlocks identifies all descendant blocks that are pinned by the user
func (h *UnpinOperationHandler) findPinnedChildBlocks(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint, children []string) ([]string, error) {
	blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)
	if blockSvc == nil {
		return nil, fmt.Errorf("block service not available")
	}

	// Use a map to track visited CIDs to prevent infinite loops
	visited := make(map[string]bool)
	
	// Find all pinned descendants recursively
	pinnedDescendants, err := h.findPinnedDescendants(ctx, tx, blockSvc, c, userID, visited)
	if err != nil {
		return nil, err
	}

	return pinnedDescendants, nil
}

// findPinnedDescendants recursively finds all descendant CIDs that are pinned by the user
func (h *UnpinOperationHandler) findPinnedDescendants(ctx context.Context, tx *gorm.DB, blockSvc pluginCore.BlockService, currentCID cid.Cid, userID uint, visited map[string]bool) ([]string, error) {
	cidStr := currentCID.String()
	
	// If we've already visited this CID, skip to prevent infinite loops
	if visited[cidStr] {
		return []string{}, nil
	}
	visited[cidStr] = true

	// Get metadata for the current block
	meta, err := blockSvc.GetBlockMeta(ctx, currentCID)
	if err != nil {
		h.Logger().Warn("Failed to get block metadata during descendant search",
			zap.Stringer("cid", currentCID),
			zap.Error(err))
		return []string{}, nil
	}

	if meta == nil {
		return []string{}, nil
	}

	var pinnedDescendants []string

	// Check if current CID is pinned by the user (but not the original target)
	// We don't want to include the target CID itself in the results
	var pinCount int64
	err = tx.WithContext(ctx).
		Model(&pluginDb.IPFSPin{}).
		Where("user_id = ? AND cid = ?", userID, currentCID.Bytes()).
		Count(&pinCount).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		h.Logger().Warn("Failed to check if CID is pinned during descendant search",
			zap.Stringer("cid", currentCID),
			zap.Error(err))
	} else if pinCount > 0 {
		// Only add to results if this isn't the original target (which is already tracked as visited)
		pinnedDescendants = append(pinnedDescendants, cidStr)
	}

	// Recursively check all children
	for _, childCID := range meta.ChildCID {
		childDescendants, err := h.findPinnedDescendants(ctx, tx, blockSvc, childCID, userID, visited)
		if err != nil {
			h.Logger().Warn("Failed to find pinned descendants for child",
				zap.Stringer("parent_cid", currentCID),
				zap.Stringer("child_cid", childCID),
				zap.Error(err))
			continue
		}
		pinnedDescendants = append(pinnedDescendants, childDescendants...)
	}

	return pinnedDescendants, nil
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
	h.Logger().Debug("Starting pin dependency check",
		zap.Stringer("pin_cid", pinCID),
		zap.Stringer("target_cid", targetCID))

	if blockSvc == nil {
		h.Logger().Error("Block service is nil during pin dependency check",
			zap.Stringer("pin_cid", pinCID),
			zap.Stringer("target_cid", targetCID))
		return false, fmt.Errorf("block service is nil")
	}

	// A pin cannot depend on itself
	if pinCID.Equals(targetCID) {
		h.Logger().Debug("Pin CID equals target CID, no dependency",
			zap.Stringer("pin_cid", pinCID))
		return false, nil
	}

	// Get the metadata for the pin's root block
	h.Logger().Debug("Getting root block metadata for pin dependency check",
		zap.Stringer("pin_cid", pinCID),
		zap.Stringer("target_cid", targetCID))

	rootMeta, err := blockSvc.GetBlockMeta(ctx, pinCID)
	if err != nil {
		h.Logger().Error("Failed to get root block metadata during pin dependency check",
			zap.Stringer("pin_cid", pinCID),
			zap.Stringer("target_cid", targetCID),
			zap.Error(err))
		return false, err
	}

	h.Logger().Debug("Got root block metadata",
		zap.Stringer("pin_cid", pinCID),
		zap.Stringer("target_cid", targetCID),
		zap.Bool("is_nil", rootMeta == nil))

	if rootMeta == nil {
		h.Logger().Debug("Root metadata is nil, no dependency",
			zap.Stringer("pin_cid", pinCID),
			zap.Stringer("target_cid", targetCID))
		return false, nil
	}

	// Recursively check if any block in this pin's DAG references the target CID
	h.Logger().Debug("Starting recursive DAG check for pin dependency",
		zap.Stringer("pin_cid", pinCID),
		zap.Stringer("target_cid", targetCID))

	result, err := h.CheckDAGForCID(ctx, blockSvc, pinCID, targetCID, make(map[string]bool))
	h.Logger().Debug("Recursive DAG check result",
		zap.Stringer("pin_cid", pinCID),
		zap.Stringer("target_cid", targetCID),
		zap.Bool("depends", result),
		zap.Error(err))

	return result, err
}

// CheckDAGForCID recursively traverses a DAG to see if it contains a specific CID
func (h *UnpinOperationHandler) CheckDAGForCID(ctx context.Context, blockSvc pluginCore.BlockService, currentCID cid.Cid, targetCID cid.Cid, visited map[string]bool) (bool, error) {
	h.Logger().Debug("Starting DAG traversal check",
		zap.Stringer("current_cid", currentCID),
		zap.Stringer("target_cid", targetCID),
		zap.Int("visited_count", len(visited)))

	if blockSvc == nil {
		h.Logger().Error("Block service is nil during DAG traversal check",
			zap.Stringer("current_cid", currentCID),
			zap.Stringer("target_cid", targetCID))
		return false, fmt.Errorf("block service is nil")
	}

	// If we've already visited this CID, skip to prevent infinite loops
	cidStr := currentCID.String()
	if visited[cidStr] {
		h.Logger().Debug("CID already visited, skipping",
			zap.String("cid", cidStr),
			zap.Stringer("target_cid", targetCID))
		return false, nil
	}
	visited[cidStr] = true

	h.Logger().Debug("Checking if current CID matches target CID",
		zap.Stringer("current_cid", currentCID),
		zap.Stringer("target_cid", targetCID),
		zap.Bool("equals", currentCID.Equals(targetCID)))

	// If current CID matches target CID, we found a dependency
	if currentCID.Equals(targetCID) {
		h.Logger().Debug("Found dependency - current CID matches target CID",
			zap.Stringer("current_cid", currentCID))
		return true, nil
	}

	// Get metadata for the current block
	h.Logger().Debug("Getting block metadata for DAG traversal",
		zap.Stringer("current_cid", currentCID),
		zap.Stringer("target_cid", targetCID))

	meta, err := blockSvc.GetBlockMeta(ctx, currentCID)
	if err != nil {
		h.Logger().Error("Failed to get block metadata during DAG traversal",
			zap.Stringer("current_cid", currentCID),
			zap.Stringer("target_cid", targetCID),
			zap.Error(err))
		return false, err
	}

	h.Logger().Debug("Got block metadata",
		zap.Stringer("current_cid", currentCID),
		zap.Stringer("target_cid", targetCID),
		zap.Bool("is_nil", meta == nil),
		zap.Int("child_count", len(lo.FromPtrOr(meta, pluginDb.UnixFSNode{}).ChildCID)))

	if meta == nil || len(meta.ChildCID) == 0 {
		h.Logger().Debug("No children found, no dependency",
			zap.Stringer("current_cid", currentCID),
			zap.Stringer("target_cid", targetCID))
		return false, nil
	}

	// Check each child
	h.Logger().Debug("Checking children for dependency",
		zap.Stringer("current_cid", currentCID),
		zap.Stringer("target_cid", targetCID),
		zap.Int("child_count", len(meta.ChildCID)))

	for _, childCID := range meta.ChildCID {
		h.Logger().Debug("Checking child CID for dependency",
			zap.Stringer("current_cid", currentCID),
			zap.Stringer("target_cid", targetCID),
			zap.Stringer("child_cid", childCID))

		found, err := h.CheckDAGForCID(ctx, blockSvc, childCID, targetCID, visited)
		if err != nil {
			h.Logger().Error("Error during recursive child check",
				zap.Stringer("current_cid", currentCID),
				zap.Stringer("target_cid", targetCID),
				zap.Stringer("child_cid", childCID),
				zap.Error(err))
			return false, err
		}

		h.Logger().Debug("Recursive child check result",
			zap.Stringer("current_cid", currentCID),
			zap.Stringer("target_cid", targetCID),
			zap.Stringer("child_cid", childCID),
			zap.Bool("found", found))

		if found {
			h.Logger().Debug("Dependency found in child",
				zap.Stringer("current_cid", currentCID),
				zap.Stringer("target_cid", targetCID),
				zap.Stringer("child_cid", childCID))
			return true, nil
		}
	}

	h.Logger().Debug("No dependency found in DAG traversal",
		zap.Stringer("current_cid", currentCID),
		zap.Stringer("target_cid", targetCID))

	return false, nil
}

// GetBlockRelationships retrieves parent and child relationships for a CID
func (h *UnpinOperationHandler) GetBlockRelationships(ctx context.Context, tx *gorm.DB, blockSvc pluginCore.BlockService, c cid.Cid, userID uint) (parents []string, children []string, err error) {
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

	// If no linked blocks found, that's okay - just return empty arrays
	if err == gorm.ErrRecordNotFound {
		return make([]string, 0), make([]string, 0), nil
	}

	// Collect parent CIDs
	var parentCIDs [][]byte
	for _, link := range linkedBlocks {
		var parentBlock pluginDb.IPFSBlock
		err := tx.WithContext(ctx).Where("id = ?", link.ParentID).First(&parentBlock).Error
		if err != nil {
			if err != gorm.ErrRecordNotFound {
				h.Logger().Warn("Failed to get parent block", zap.Error(err))
			}
			continue
		}
		parentCIDs = append(parentCIDs, parentBlock.CID)
	}

	// Batch check which parent CIDs are pinned by the user
	var pinnedParentCIDs [][]byte
	if len(parentCIDs) > 0 {
		err = tx.WithContext(ctx).
			Model(&pluginDb.IPFSPin{}).
			Where("user_id = ? AND cid IN ?", userID, parentCIDs).
			Pluck("cid", &pinnedParentCIDs).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return nil, nil, err
		}
	}

	// Convert pinned parent CIDs to strings
	parents = make([]string, 0, len(pinnedParentCIDs))
	for _, parentCIDBytes := range pinnedParentCIDs {
		parentCID, err := cid.Cast(parentCIDBytes)
		if err != nil {
			h.Logger().Warn("Failed to cast parent CID", zap.Error(err))
			continue
		}
		parents = append(parents, parentCID.String())
	}

	return parents, children, nil
}

// PromotePinsToRootLevelVisibility updates file paths for dependent pins to mark them as root level visible
func (h *UnpinOperationHandler) PromotePinsToRootLevelVisibility(ctx context.Context, tx *gorm.DB, dependentPins []string, userID uint) error {
	fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)
	if fileManagerSvc == nil {
		return fmt.Errorf("file manager service not available")
	}

	for _, pinCIDStr := range dependentPins {
		pinCID, err := cid.Parse(pinCIDStr)
		if err != nil {
			h.Logger().Warn("Failed to parse dependent pin CID during root level visibility promotion",
				zap.String("cid", pinCIDStr),
				zap.Error(err))
			continue
		}

		// Update file paths to mark them as root level visible
		err = h.UpdatePathsToRootLevelVisibilityWithTx(ctx, tx, pinCID, userID)
		if err != nil {
			h.Logger().Error("Failed to update paths to root level visibility",
				zap.Stringer("cid", pinCID),
				zap.Uint("user_id", userID),
				zap.Error(err))
			return err
		}

		h.Logger().Info("Successfully promoted pin to root level visibility",
			zap.Stringer("cid", pinCID),
			zap.Uint("user_id", userID))
	}

	return nil
}

// AnalyzePathDependencies analyzes file path dependencies for the given CID
func (h *UnpinOperationHandler) AnalyzePathDependencies(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) (*PathDependencyAnalysis, error) {
	analysis := &PathDependencyAnalysis{
		AffectedPaths:        make([]string, 0),
		SharedDirectories:    make([]string, 0),
		RootLevelCandidates: make([]string, 0),
	}

	// Validate input CID
	if c == cid.Undef {
		return nil, fmt.Errorf("CID is undefined")
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
	shared, err := h.IsCIDShared(ctx, tx, c, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to check if CID is shared: %w", err)
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
		analysis.RootLevelCandidates = orphanCandidates
	} else {
		// If not shared, check if removing it would break directory structures
		analysis.WouldBreakPaths = h.WouldBreakDirectoryStructure(targetPath)
		analysis.AffectedPaths = []string{targetPath.Path}
	}

	return analysis, nil
}

// IsCIDShared checks if the same CID is pinned multiple times by the same user
func (h *UnpinOperationHandler) IsCIDShared(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) (bool, error) {
	var pinCount int64
	err := tx.WithContext(ctx).
		Model(&pluginDb.IPFSPin{}).
		Where("user_id = ? AND cid = ?", userID, c.Bytes()).
		Count(&pinCount).Error
	if err != nil {
		return false, fmt.Errorf("failed to count pins for CID: %w", err)
	}

	// If there are multiple pins for the same CID, it's shared
	return pinCount > 1, nil
}

// IsDirectoryShared checks if a directory contains multiple different pins
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

// IsPathShared checks if a path is shared by multiple pins for the same user
func (h *UnpinOperationHandler) IsPathShared(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) (bool, error) {
	// First check if the CID itself is shared (duplicate pins)
	shared, err := h.IsCIDShared(ctx, tx, c, userID)
	if err != nil {
		return false, err
	}

	if shared {
		return true, nil
	}

	// If no duplicate pins, check if the directory structure is shared
	// Get the file path for this CID
	var targetPath pluginDb.FilePath
	err = tx.WithContext(ctx).Where("user_id = ? AND cid = ?", userID, c.Bytes()).First(&targetPath).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// If no file path exists for this CID, it's not shared
			return false, nil
		}
		return false, fmt.Errorf("failed to get target file path: %w", err)
	}

	// Check if the directory containing this file is shared
	return h.IsDirectoryShared(ctx, tx, targetPath.ParentPath, userID)
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
	if analysis == nil {
		return fmt.Errorf("path dependency analysis cannot be nil")
	}

	fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)
	if fileManagerSvc == nil {
		return fmt.Errorf("file manager service not available")
	}

	// Handle orphan candidates - promote them to root level visibility
	if len(analysis.RootLevelCandidates) > 0 {
		h.Logger().Info("Promoting orphan candidates to root level visibility",
			zap.Int("count", len(analysis.RootLevelCandidates)))

		for _, cidStr := range analysis.RootLevelCandidates {
			orphanCID, err := cid.Parse(cidStr)
			if err != nil {
				h.Logger().Warn("Failed to parse orphan candidate CID",
					zap.String("cid", cidStr),
					zap.Error(err))
				continue
			}

			err = h.UpdatePathsToRootLevelVisibilityWithTx(ctx, tx, orphanCID, userID)
			if err != nil {
				h.Logger().Error("Failed to update paths to root level visibility for candidate",
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

// UpdatePathsToRootLevelVisibility moves orphaned pins to root level path structure and marks them as root level visible
func (h *UnpinOperationHandler) UpdatePathsToRootLevelVisibility(ctx context.Context, c cid.Cid, userID uint) error {
	// Check for service availability before proceeding
	fileManagerSvc := core.GetService[pluginCore.FileManagerService](h.Context(), pluginCore.FILE_MANAGER_SERVICE)
	if fileManagerSvc == nil {
		return fmt.Errorf("file manager service not available")
	}

	return h.updatePathsToRootLevelVisibilityWithDB(ctx, h.Context().DB(), c, userID)
}

// UpdatePathsToRootLevelVisibilityWithTx moves orphaned pins to root level path structure and marks them as root level visible using a transaction
func (h *UnpinOperationHandler) UpdatePathsToRootLevelVisibilityWithTx(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) error {
	return h.updatePathsToRootLevelVisibilityWithDB(ctx, tx, c, userID)
}

// updatePathsToRootLevelVisibilityWithDB contains the shared logic for updating paths to root level visibility
func (h *UnpinOperationHandler) updatePathsToRootLevelVisibilityWithDB(ctx context.Context, db *gorm.DB, c cid.Cid, userID uint) error {
	// Get all file paths for this CID and user
	var paths []pluginDb.FilePath

	err := db.WithContext(ctx).
		Where("user_id = ? AND cid = ?", userID, c.Bytes()).
		Find(&paths).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return fmt.Errorf("failed to get file paths: %w", err)
	}

	// Update each path to be root level visible (orphaned) at root level
	for _, path := range paths {
		// Move to root level with CID as name
		rootPath := "/" + c.String()

		err := db.WithContext(ctx).
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
			h.Logger().Error("Failed to update file path to root level visibility",
				zap.Stringer("cid", c),
				zap.Uint("user_id", userID),
				zap.String("path", path.Path),
				zap.Error(err))
			return err
		}

		h.Logger().Debug("Updated file path to root level visibility",
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
func (h *UnpinOperationHandler) ValidateDAGIntegrityBeforeUnpin(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) error {
	blockSvc := core.GetService[pluginCore.BlockService](h.Context(), pluginCore.BLOCK_SERVICE)
	if blockSvc == nil {
		return fmt.Errorf("block service not available")
	}

	// Check if the block exists
	_, err := blockSvc.GetBlockMeta(ctx, c)
	if err != nil {
		return fmt.Errorf("target block does not exist: %w", err)
	}

	// Check if this CID is actually pinned by the user
	var pinCount int64
	err = tx.WithContext(ctx).
		Model(&pluginDb.IPFSPin{}).
		Where("user_id = ? AND cid = ?", userID, c.Bytes()).
		Count(&pinCount).Error
	if err != nil {
		return fmt.Errorf("failed to check pin existence: %w", err)
	}
	if pinCount == 0 {
		return fmt.Errorf("not found")
	}

	// Validate the entire DAG structure for this user
	result, err := h.ValidateUserDAGStructure(ctx, tx, userID)
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
func (h *UnpinOperationHandler) ValidateDAGIntegrityAfterUnpin(ctx context.Context, tx *gorm.DB, c cid.Cid, userID uint) error {
	// Validate the entire DAG structure for this user after unpinning
	result, err := h.ValidateUserDAGStructure(ctx, tx, userID)
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
func (h *UnpinOperationHandler) ValidateUserDAGStructure(ctx context.Context, tx *gorm.DB, userID uint) (*DAGValidationResult, error) {
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
	pins, err := h.GetAllUserPins(ctx, tx, userID)
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

	// Collect all pin CIDs for batch traversal
	var pinCIDs []cid.Cid
	for _, pin := range pins {
		pinCID, err := cid.Cast(pin.CID)
		if err != nil {
			h.Logger().Warn("Failed to cast pin CID during validation",
				zap.Binary("cid_bytes", pin.CID),
				zap.Error(err))
			continue
		}
		pinCIDs = append(pinCIDs, pinCID)
	}

	// Perform batched DAG validation
	missing, cycle, err := h.ValidateAllPins(ctx, blockSvc, pinCIDs, pinnedCIDs)
	if err != nil {
		result.IsValid = false
		result.ErrorMessage = fmt.Sprintf("failed to validate DAGs for pins: %v", err)
		return result, nil
	}

	if cycle {
		result.IsValid = false
		result.CycleDetected = true
		result.ErrorMessage = "cycle detected in DAG structure"
		return result, nil
	}

	result.MissingBlocks = missing

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

// ValidateAllPins performs batched validation of all pins for a user
func (h *UnpinOperationHandler) ValidateAllPins(ctx context.Context, blockSvc pluginCore.BlockService, pinCIDs []cid.Cid, pinnedCIDs map[string]bool) ([]string, bool, error) {
	processedBlocks := make(map[string]bool)
	var missingBlocks []string
	cycleDetected := false

	// Validate all pins in a single traversal
	for _, pinCID := range pinCIDs {
		// Validate this pin's DAG
		missing, cycle, err := h.ValidateDAG(ctx, blockSvc, pinCID, pinnedCIDs, processedBlocks)
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

// ValidateRootLevelVisibilityPromotion validates that root level visibility promotion was successful
func (h *UnpinOperationHandler) ValidateRootLevelVisibilityPromotion(ctx context.Context, dependentPins []string, userID uint) error {
	__db := h.Context().DB()

	for _, pinCIDStr := range dependentPins {
		pinCID, err := cid.Parse(pinCIDStr)
		if err != nil {
			h.Logger().Warn("Failed to parse dependent pin CID during validation",
				zap.String("cid", pinCIDStr),
				zap.Error(err))
			continue
		}

		// Check if file paths exist for this pin and are marked as root level visible
		var paths []pluginDb.FilePath
		err = __db.WithContext(ctx).
			Where("user_id = ? AND cid = ?", userID, pinCID.Bytes()).
			Find(&paths).Error
		if err != nil {
			return fmt.Errorf("failed to get file paths for pin %s: %w", pinCIDStr, err)
		}

		// Validate that all paths are marked as root level visible (orphaned)
		for _, path := range paths {
			if !path.IsOrphan {
				return fmt.Errorf("file path %s for pin %s is not marked as root level visible", path.Path, pinCIDStr)
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
