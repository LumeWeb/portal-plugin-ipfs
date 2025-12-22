package upload

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ValidateArchivePath validates that a file path is safe and doesn't contain path traversal attempts
// This is a shared validation function used by all archive extractors
func ValidateArchivePath(path string) error {
	// Check for suspicious patterns in the original path first
	suspiciousPatterns := []string{
		"\\", // Windows path separators
		"//", // Double slashes
		"./", // Current directory references
	}

	for _, pattern := range suspiciousPatterns {
		if strings.Contains(path, pattern) {
			return fmt.Errorf("path contains suspicious pattern: %s", pattern)
		}
	}

	// Clean the path for further validation
	cleanPath := filepath.Clean(path)

	// Check for path traversal attempts using root-boundary check
	root := "<archiveRoot>"
	joinedPath := filepath.Join(root, cleanPath)
	rel, err := filepath.Rel(root, joinedPath)
	if err != nil || strings.HasPrefix(rel, "..") || (rel == "." && joinedPath != root) {
		return fmt.Errorf("path escapes archive root boundary")
	}

	// Check for absolute paths (should be relative within the archive)
	if filepath.IsAbs(cleanPath) {
		return fmt.Errorf("absolute paths are not allowed")
	}

	return nil
}
