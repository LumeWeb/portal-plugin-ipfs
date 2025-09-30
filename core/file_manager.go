package core

import (
	"context"

	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

const FILE_MANAGER_SERVICE = "ipfs.file_manager"

// FileManagerService provides file listing functionality for display purposes only
type FileManagerService interface {
	// ID returns the service identifier
	ID() string

	// ListFiles retrieves a paginated and filtered list of files for display
	ListFiles(ctx context.Context, userID uint, filters []queryutil.CrudFilter, sort []filter.Sort, pagination queryutil.Pagination) ([]*db.FilePath, int64, error)

	// ListDirectoryContents lists files and directories in a specific parent path
	ListDirectoryContents(ctx context.Context, userID uint, parentPath string) ([]*db.FilePath, error)

	// GetBreadcrumbs retrieves breadcrumb navigation for a given path
	GetBreadcrumbs(ctx context.Context, userID uint, path string) ([]*db.FilePath, error)

	// CreateFilePath creates a new file path entry
	CreateFilePath(ctx context.Context, path *db.FilePath) error

	// DeleteFilePathSmart performs smart deletion of file paths, only removing paths
	// that are not referenced by other pins for the same user
	DeleteFilePathSmart(ctx context.Context, userID uint, cid []byte) error

	// DeleteFilePath deletes a file path entry
	DeleteFilePath(ctx context.Context, userID uint, cid []byte) error

	// DeleteFilePathsByUserID deletes all file path entries for a user
	DeleteFilePathsByUserID(ctx context.Context, userID uint) error

	// ValidatePathCompleteness checks if all pins have corresponding file paths
	ValidatePathCompleteness(ctx context.Context) (bool, error)

	// GetIncompletePins retrieves all pins with status other than "pinned"
	GetIncompletePins(ctx context.Context) ([]*db.IPFSPin, error)

	// GetOrphanedPaths retrieves all file paths marked as orphaned
	GetOrphanedPaths(ctx context.Context) ([]*db.FilePath, error)
}
