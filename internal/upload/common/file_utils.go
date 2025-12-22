package common

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// FileHelper provides common file processing utilities
type FileHelper struct {
	logger *core.Logger
}

// NewFileHelper creates a new file helper instance
func NewFileHelper(logger *core.Logger) *FileHelper {
	return &FileHelper{
		logger: logger,
	}
}

// SafeFileOpen safely opens a file from a filesystem with proper error handling
func (fh *FileHelper) SafeFileOpen(efs fs.FS, path string) (io.ReadCloser, error) {
	file, err := efs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}
	return file, nil
}

// SafeClose safely closes a file with logging on error
func (fh *FileHelper) SafeClose(file io.Closer, path string) {
	if file == nil {
		return
	}
	if closeErr := file.Close(); closeErr != nil && fh.logger != nil {
		fh.logger.Warn("Failed to close file",
			zap.String("path", path),
			zap.Error(closeErr))
	}
}

// SafeCloseSilent closes a file without logging (for cases where close errors are expected)
func SafeCloseSilent(file io.Closer) {
	if file != nil {
		_ = file.Close()
	}
}

// SafeCloseFile is a convenience function that creates a FileHelper and closes a file safely
// This is a shorthand for: common.NewFileHelper(logger).SafeClose(closer, path)
// Path is optional - if not provided, no path will be logged
func SafeCloseFile(logger *core.Logger, closer io.Closer, path ...string) {
	var pathStr string
	if len(path) > 0 {
		pathStr = path[0]
	}
	NewFileHelper(logger).SafeClose(closer, pathStr)
}

// PrepareReaderWithPosition prepares a reader for operations by seeking to beginning and returns a position
// The position returned depends on the getPosition function provided, allowing for flexible position retrieval
func PrepareReaderWithPosition(seeker io.Seeker, getPosition func() (int64, error)) (int64, error) {
	// Get position using the provided function
	pos, err := getPosition()
	if err != nil {
		return 0, fmt.Errorf("failed to get reader position: %w", err)
	}

	// Seek to beginning for processing
	_, err = seeker.Seek(0, io.SeekStart)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to beginning: %w", err)
	}

	return pos, nil
}

// PrepareReaderPreservePos prepares a reader for operations while preserving the current position
// Returns the current position and ensures the reader is at the beginning
func PrepareReaderPreservePos(seeker io.Seeker) (int64, error) {
	return PrepareReaderWithPosition(seeker, func() (int64, error) {
		return seeker.Seek(0, io.SeekCurrent)
	})
}

// RestoreReaderPos restores a reader to its original position
func RestoreReaderPos(seeker io.Seeker, pos int64) error {
	_, err := seeker.Seek(pos, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to restore original reader position: %w", err)
	}
	return nil
}

// CheckContext checks for context cancellation
func CheckContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// ValidateReader validates that a reader is not nil
func ValidateReader(reader io.Reader) error {
	if reader == nil {
		return fmt.Errorf("reader cannot be nil")
	}
	return nil
}

// Constants for filesystem operations
const (
	ROOT = "."
)

// IsValidPath validates a file path to prevent traversal issues
func IsValidPath(currentPath string) bool {
	// Skip paths with directory traversal attempts or absolute paths
	if strings.Contains(currentPath, "..") || strings.HasPrefix(currentPath, "/") {
		return false
	}
	return true
}

// GetParentPath returns the parent directory path for a given path
func GetParentPath(currentPath string) string {
	if currentPath == ROOT {
		return ROOT
	}
	lastSlash := strings.LastIndex(currentPath, "/")
	if lastSlash == -1 {
		return ROOT
	}
	parent := currentPath[:lastSlash]
	if parent == "" {
		return ROOT
	}
	return parent
}

// IsNoSuchFileError checks if an error indicates a file doesn't exist
func IsNoSuchFileError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "no such file") ||
		strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "does not exist")
}

// IsPathError checks if an error is a filesystem path error
func IsPathError(err error) bool {
	if err == nil {
		return false
	}
	// Check for common path-related error patterns
	errStr := err.Error()
	return strings.Contains(errStr, "invalid path") ||
		strings.Contains(errStr, "path error") ||
		strings.Contains(errStr, "filesystem")
}