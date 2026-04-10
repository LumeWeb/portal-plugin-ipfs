package common

import (
	"context"
	"fmt"
	"io/fs"

	"go.lumeweb.com/ipfs-content/archive"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// WalkArchiveEntry represents a single file or directory entry during a walk
type WalkArchiveEntry struct {
	Path     string            // Full path within the archive
	Name     string            // Base name of the entry
	Info     fs.FileInfo       // File info (size, mode, etc.)
	IsDir    bool              // Whether this entry is a directory
	ParentPath string           // Parent directory path
}

// ArchiveWalkCallback is called for each file and directory in an archive during a walk.
// The callback can return an error to abort the walk, or nil to continue.
// Returning nil means "continue processing", even for skipped entries.
//
// Parameters:
//   - entry: WalkArchiveEntry with information about the current entry
//   - efs: The filesystem interface for opening files if needed
//
// Returns:
//   - error: Error to abort the walk, or nil to continue
type ArchiveWalkCallback func(entry *WalkArchiveEntry, efs fs.FS) error

// WalkArchive walks an archive filesystem with consistent error handling and validation.
// This centralizes the common logic for walking archive entries, reducing code duplication.
//
// Common features:
//   - Context cancellation support
//   - Empty archive detection
//   - Path validation (prevents traversal attacks)
//   - Error handling with optional logging
//
// Parameters:
//   - ctx: Context for cancellation
//   - extractor: ArchiveExtractor providing the filesystem to walk
//   - logger: Logger for error reporting (optional, can be nil)
//   - skipRoot: Whether to skip calling the callback for the root path (default: true)
//   - callback: Function called for each file and directory entry
//
// Returns:
//   - error: Error if the walk fails or callback returns an error
func WalkArchive(ctx context.Context, extractor archive.ArchiveExtractor, logger *core.Logger, skipRoot bool, callback ArchiveWalkCallback) error {
	// Get filesystem from extractor
	efs, err := extractor.Filesystem(ctx)
	if err != nil {
		if logger != nil {
			logger.Error("Failed to get filesystem from archive extractor",
				zap.Error(err))
		}
		return fmt.Errorf("failed to get filesystem: %w", err)
	}

	// Walk filesystem and process entries
	err = fs.WalkDir(efs, ROOT, func(path string, d fs.DirEntry, err error) error {
		// Handle context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Handle walk errors
		if err != nil {
			// Check for empty archive conditions
			if path == ROOT && (IsNoSuchFileError(err) || IsPathError(err)) {
				return nil // Empty archive is a valid state
			}
			return fmt.Errorf("error accessing path %s: %w", path, err)
		}

		// Skip root if requested
		if skipRoot && path == ROOT {
			return nil
		}

		// Validate path to prevent traversal attacks
		if !IsValidPath(path) {
			return fmt.Errorf("invalid path detected: %s", path)
		}

		// Get file info
		info, err := d.Info()
		if err != nil {
			msg := "failed to get file info, skipping entry"
			if logger != nil {
				logger.Warn(msg,
					zap.String("path", path),
					zap.Error(err))
			}
			return nil // Continue processing other entries
		}

		// Create entry for callback
		entry := &WalkArchiveEntry{
			Path:      path,
			Name:      d.Name(),
			Info:      info,
			IsDir:     d.IsDir(),
			ParentPath: GetParentPath(path),
		}

		// Call the callback
		if err := callback(entry, efs); err != nil {
			return err // Abort walk on callback error
		}

		return nil
	})

	if err != nil {
		if logger != nil {
			logger.Error("Failed to walk archive filesystem",
				zap.Error(err))
		}
		return fmt.Errorf("filesystem walk failed: %w", err)
	}

	return nil
}
