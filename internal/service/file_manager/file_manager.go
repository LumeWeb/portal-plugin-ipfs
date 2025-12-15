package filemanager

import (
	"context"
	"errors"
	"fmt"
	"strings"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// FileManagerServiceDefault implements the FileManagerService interface
type FileManagerServiceDefault struct {
	ctx    core.Context
	db     *gorm.DB
	logger *core.Logger
}

// Predefined errors
var (
	ErrDuplicateFilePath = errors.New("file path already exists")
)

// Ensure FileManagerServiceDefault implements the interface
var _ pluginCore.FileManagerService = (*FileManagerServiceDefault)(nil)

// NewFileManagerService creates a new file manager service
func NewFileManagerService() (core.Service, []core.ContextBuilderOption, error) {
	svc := &FileManagerServiceDefault{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			svc.ctx = ctx
			svc.logger = ctx.ServiceLogger(svc)
			svc.db = ctx.DB()
			return nil
		}),
	)

	return svc, opts, nil
}

func (s *FileManagerServiceDefault) ID() string {
	return pluginCore.FILE_MANAGER_SERVICE
}

// ListFiles retrieves a paginated and filtered list of files using the path table
func (s *FileManagerServiceDefault) ListFiles(ctx context.Context, userID uint, filters []queryutil.CrudFilter, sort []filter.Sort, pagination queryutil.Pagination) ([]*pluginDb.FilePath, int64, error) {
	var paths []*pluginDb.FilePath
	var total int64

	// Process filters to handle hierarchical parent_path filtering
	processedFilters := s.processHierarchicalFilters(filters)

	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		// Construct the query for file paths
		query := g.WithContext(ctx).Model(&pluginDb.FilePath{}).Where("user_id = ?", userID)

		// Apply filters using queryutil
		query = queryutil.ApplyFilters(query, processedFilters, nil)

		// Apply sort
		query = queryutil.ApplySort(query, sort)

		// Get total count
		if err := query.Count(&total).Error; err != nil {
			_ = g.AddError(fmt.Errorf("failed to count files: %w", err))
			return g
		}

		// Apply pagination
		query = queryutil.ApplyPagination(query, pagination)

		// Get the records
		if err := query.Find(&paths).Error; err != nil {
			_ = g.AddError(fmt.Errorf("failed to list files: %w", err))
			return g
		}

		return g
	})

	if err != nil {
		s.logger.Error("Failed to list files",
			zap.Error(err),
			zap.Uint("user_id", userID),
			zap.Any("filters", filters),
			zap.Any("pagination", pagination))
		return nil, 0, err
	}

	s.logger.Debug("Listed files",
		zap.Uint("user_id", userID),
		zap.Int("count", len(paths)),
		zap.Int64("total", total))
	return paths, total, nil
}

// ListDirectoryContents lists files and directories in a specific parent path
// Includes orphaned files only when parentPath is RootPath
func (s *FileManagerServiceDefault) ListDirectoryContents(ctx context.Context, userID uint, parentPath string) ([]*pluginDb.FilePath, error) {
	var paths []*pluginDb.FilePath

	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		query := g.WithContext(ctx).
			Where("user_id = ? AND parent_path = ?", userID, parentPath)

		// When listing root directory, include orphaned files
		// No additional filter needed as the base query already includes all entries for the parent path

		return query.Order("is_directory DESC, name ASC").Find(&paths)
	})

	if err != nil {
		s.logger.Error("Failed to list directory contents",
			zap.Error(err),
			zap.Uint("user_id", userID),
			zap.String("parent_path", parentPath))
		return nil, err
	}

	return paths, nil
}

// GetBreadcrumbs retrieves breadcrumb navigation for a given path
func (s *FileManagerServiceDefault) GetBreadcrumbs(ctx context.Context, userID uint, path string) ([]*pluginDb.FilePath, error) {
	// Validate the input path
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("path must start with '/'")
	}

	// Special case for root path - return empty breadcrumbs
	if path == pluginDb.RootPath {
		return []*pluginDb.FilePath{}, nil
	}

	// Check if path exists for the given user
	var existingPath pluginDb.FilePath
	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		return g.WithContext(ctx).
			Where("user_id = ? AND path = ?", userID, path).
			First(&existingPath)
	})

	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("path not found for user")
		}
		s.logger.Error("Failed to check if path exists",
			zap.Error(err),
			zap.Uint("user_id", userID),
			zap.String("path", path))
		return nil, err
	}

	var breadcrumbs []*pluginDb.FilePath

	// Build path hierarchy
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	var paths []string
	currentPath := ""

	for _, part := range pathParts {
		if part == "" {
			continue
		}
		if currentPath == "" {
			currentPath = "/" + part
		} else {
			currentPath += "/" + part
		}
		paths = append(paths, currentPath)
	}

	err = db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		return g.WithContext(ctx).
			Where("user_id = ? AND path IN ?", userID, paths).
			Order("depth ASC").
			Find(&breadcrumbs)
	})

	if err != nil {
		s.logger.Error("Failed to get breadcrumbs",
			zap.Error(err),
			zap.Uint("user_id", userID),
			zap.String("path", path))
		return nil, err
	}

	return breadcrumbs, nil
}

// CreateFilePath creates a new file path entry
func (s *FileManagerServiceDefault) CreateFilePath(ctx context.Context, path *pluginDb.FilePath) error {
	// Check if a file path entry already exists for the same user_id, cid, and path
	var existingPath pluginDb.FilePath
	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		return g.WithContext(ctx).
			Where("user_id = ? AND cid = ? AND path = ?", path.UserID, path.CID, path.Path).
			First(&existingPath)
	})

	// If an entry exists, return an error indicating duplicate path
	if err == nil {
		return ErrDuplicateFilePath
	}

	// If the error is not a "record not found" error, return the database error
	if err != gorm.ErrRecordNotFound {
		s.logger.Error("Failed to check for existing file path",
			zap.Error(err),
			zap.Any("path", path))
		return err
	}

	// If no entry exists, proceed with the normal creation logic
	err = db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		return g.WithContext(ctx).Create(path)
	})

	if err != nil {
		s.logger.Error("Failed to create file path",
			zap.Error(err),
			zap.Any("path", path))
		return err
	}

	return nil
}

// ValidatePathCompleteness checks if all pins have corresponding file paths
func (s *FileManagerServiceDefault) ValidatePathCompleteness(ctx context.Context) (bool, error) {
	incompletePins, err := s.GetIncompletePins(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get incomplete pins: %w", err)
	}

	if len(incompletePins) > 0 {
		s.logger.Warn("Found pins without file paths",
			zap.Int("count", len(incompletePins)))
		return false, nil
	}

	return true, nil
}

// GetIncompletePins returns pins that don't have corresponding file paths
func (s *FileManagerServiceDefault) GetIncompletePins(ctx context.Context) ([]*pluginDb.IPFSPin, error) {
	var incompletePins []*pluginDb.IPFSPin

	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		// Find pins that don't have corresponding file paths
		query := `
			SELECT ip.* 
			FROM ipfs_pins ip
			LEFT JOIN ipfs_file_paths ifp ON ip.user_id = ifp.user_id AND ip.cid = ifp.cid
			WHERE ifp.cid IS NULL
		`

		if err := g.WithContext(ctx).Raw(query).Scan(&incompletePins).Error; err != nil {
			_ = g.AddError(fmt.Errorf("failed to find incomplete pins: %w", err))
			return g
		}

		return g
	})

	if err != nil {
		s.logger.Error("Failed to get incomplete pins", zap.Error(err))
		return nil, err
	}

	return incompletePins, nil
}

// GetOrphanedPaths returns file paths that don't have corresponding pins
func (s *FileManagerServiceDefault) GetOrphanedPaths(ctx context.Context) ([]*pluginDb.FilePath, error) {
	var orphanedPaths []*pluginDb.FilePath

	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		// Find file paths that don't have corresponding pins
		query := `
			SELECT ifp.* 
			FROM ipfs_file_paths ifp
			LEFT JOIN ipfs_pins ip ON ip.user_id = ifp.user_id AND ip.cid = ifp.cid
			WHERE ip.cid IS NULL
		`

		if err := g.WithContext(ctx).Raw(query).Scan(&orphanedPaths).Error; err != nil {
			_ = g.AddError(fmt.Errorf("failed to find orphaned paths: %w", err))
			return g
		}

		return g
	})

	if err != nil {
		s.logger.Error("Failed to get orphaned paths", zap.Error(err))
		return nil, err
	}

	return orphanedPaths, nil
}

// processHierarchicalFilters handles special parent_path filtering logic
func (s *FileManagerServiceDefault) processHierarchicalFilters(filters []queryutil.CrudFilter) []queryutil.CrudFilter {
	// Find parent_path filter if it exists
	parentPathFilter := queryutil.DeepFindFilter(filters, "parent_path")
	if parentPathFilter == nil {
		return filters
	}

	// Get the parent path value
	parentPathValue, ok := parentPathFilter.GetValue().(string)
	if !ok {
		return filters
	}

	// Create new hierarchical filter based on the parent path value
	var hierarchicalFilter queryutil.CrudFilter
	if parentPathValue == pluginDb.RootPath {
		// For root path, match all direct children (parent_path = "/")
		hierarchicalFilter = queryutil.Equal("parent_path", pluginDb.RootPath)
	} else {
		// For other paths, match exact path or subpaths
		// Example: for "/documents", match:
		// 1. Files with parent_path="/documents" (direct children)
		// 2. Files with parent_path starting with "/documents/" (descendants)
		hierarchicalFilter = queryutil.Or(
			queryutil.Equal("parent_path", parentPathValue),
			queryutil.NewLogicalFilter("parent_path", queryutil.OpStartswiths, parentPathValue),
		)
	}

	// Remove the original parent_path filter and add the new hierarchical filter
	var processedFilters []queryutil.CrudFilter
	for _, f := range filters {
		if f.GetField() != "parent_path" {
			processedFilters = append(processedFilters, f)
		}
	}
	processedFilters = append(processedFilters, hierarchicalFilter)

	return processedFilters
}

// HealthCheck implements core.HealthChecker interface
func (s *FileManagerServiceDefault) HealthCheck(ctx context.Context) error {
	// Check database connectivity
	if err := s.db.WithContext(ctx).Exec("SELECT 1").Error; err != nil {
		return fmt.Errorf("database connectivity check failed: %w", err)
	}

	// Validate path completeness
	valid, err := s.ValidatePathCompleteness(ctx)
	if err != nil {
		return fmt.Errorf("path completeness validation failed: %w", err)
	}

	if !valid {
		// Log warning but don't fail health check - this is informational
		s.logger.Warn("File path completeness validation failed - some pins may not have file paths computed")
	}

	return nil
}

// DeleteFilePathSmart performs smart deletion of file paths, only removing paths
// that are not referenced by other pins for the same user
func (s *FileManagerServiceDefault) DeleteFilePathSmart(ctx context.Context, userID uint, cid []byte) error {
	// Get all file paths associated with this CID
	var pathsToDelete []*pluginDb.FilePath
	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		return g.WithContext(ctx).
			Where("user_id = ? AND cid = ?", userID, cid).
			Find(&pathsToDelete)
	})

	if err != nil {
		s.logger.Error("Failed to get file paths for deletion",
			zap.Error(err),
			zap.Uint("user_id", userID))
		return err
	}

	// For each path, check if other pins reference it
	for _, path := range pathsToDelete {
		// Count how many pins reference this path (by checking parent paths)
		var pinCount int64
		err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
			// Check if any other pins have this CID
			return g.WithContext(ctx).
				Model(&pluginDb.IPFSPin{}).
				Where("user_id = ? AND cid = ?", userID, cid).
				Count(&pinCount)
		})

		if err != nil {
			s.logger.Error("Failed to count pins referencing path",
				zap.Error(err),
				zap.Uint("user_id", userID),
				zap.String("path", path.Path))
			continue
		}

		// Only delete the path if no other pins reference it
		if pinCount == 0 {
			err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
				return g.WithContext(ctx).
					Where("id = ?", path.ID).
					Delete(&pluginDb.FilePath{})
			})

			if err != nil {
				s.logger.Error("Failed to delete file path",
					zap.Error(err),
					zap.Uint("user_id", userID),
					zap.String("path", path.Path))
				continue
			}

			s.logger.Debug("Deleted file path",
				zap.Uint("user_id", userID),
				zap.String("path", path.Path))
		} else {
			s.logger.Debug("Skipped deleting file path (referenced by other pins)",
				zap.Uint("user_id", userID),
				zap.String("path", path.Path),
				zap.Int64("pin_count", pinCount))
		}
	}

	return nil
}

// UpdateFilePath updates an existing file path entry
func (s *FileManagerServiceDefault) UpdateFilePath(ctx context.Context, path *pluginDb.FilePath) error {
	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		return g.WithContext(ctx).
			Model(&pluginDb.FilePath{}).
			Where("user_id = ? AND cid = ? AND path = ?", path.UserID, path.CID, path.Path).
			Updates(map[string]interface{}{
				"name":         path.Name,
				"type":         path.Type,
				"size":         path.Size,
				"is_directory": path.IsDirectory,
				"is_orphan":    path.IsOrphan,
				"parent_path":  path.ParentPath,
				"depth":        path.Depth,
			})
	})

	if err != nil {
		s.logger.Error("Failed to update file path",
			zap.Error(err),
			zap.Uint("user_id", path.UserID),
			zap.String("path", path.Path))
		return err
	}

	return nil
}

// DeleteFilePath deletes all file path entries for a specific CID and user
// This is a force delete that doesn't check for shared references
func (s *FileManagerServiceDefault) DeleteFilePath(ctx context.Context, userID uint, cid []byte) error {
	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		return g.WithContext(ctx).
			Where("user_id = ? AND cid = ?", userID, cid).
			Delete(&pluginDb.FilePath{})
	})

	if err != nil {
		s.logger.Error("Failed to delete file path",
			zap.Error(err),
			zap.Uint("user_id", userID))
		return err
	}

	return nil
}

// DeleteFilePathsByUserID deletes all file path entries for a user
func (s *FileManagerServiceDefault) DeleteFilePathsByUserID(ctx context.Context, userID uint) error {
	err := db.RetryableTransaction(s.ctx, s.db, func(g *gorm.DB) *gorm.DB {
		return g.WithContext(ctx).
			Where("user_id = ?", userID).
			Delete(&pluginDb.FilePath{})
	})

	if err != nil {
		s.logger.Error("Failed to delete file paths by user ID",
			zap.Error(err),
			zap.Uint("user_id", userID))
		return err
	}

	return nil
}
