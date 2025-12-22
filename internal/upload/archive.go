package upload

import (
	"context"
	"io"
	"io/fs"
	"os"
	"time"
)

// ArchiveFileEntry represents a file entry with streaming content access
type ArchiveFileEntry struct {
	fileName   string            // Original file path within archive
	size       int64             // Uncompressed file size
	isDir      bool              // Whether this is a directory
	modified   time.Time         // Modification time
	mode       int64             // File permissions
	attributes map[string]string // Additional format-specific attributes

	// ContentReader provides direct access to file content
	contentReader io.ReadCloser
}

// Name returns the base name of the file
func (e *ArchiveFileEntry) Name() string { return e.fileName }

// Size returns the length in bytes for regular files; system-dependent for others
func (e *ArchiveFileEntry) Size() int64 { return e.size }

// Mode returns the file mode bits
func (e *ArchiveFileEntry) Mode() os.FileMode {
	if e.isDir {
		return os.ModeDir | os.FileMode(e.mode)
	}
	return os.FileMode(e.mode)
}

// ModTime returns modification time
func (e *ArchiveFileEntry) ModTime() time.Time { return e.modified }

// IsDir reports whether the entry is a directory
func (e *ArchiveFileEntry) IsDir() bool { return e.isDir }

// Sys returns underlying data source (can return nil)
func (e *ArchiveFileEntry) Sys() any { return nil }

// ContentReader returns the content reader for the file
func (e *ArchiveFileEntry) ContentReader() io.ReadCloser { return e.contentReader }

// SetContentReader sets the content reader for the file
func (e *ArchiveFileEntry) SetContentReader(reader io.ReadCloser) {
	e.contentReader = reader
}

// NewArchiveFileEntry creates a new ArchiveFileEntry with the given parameters
func NewArchiveFileEntry(name string, size int64, isDir bool, modified time.Time, mode int64, contentReader io.ReadCloser) *ArchiveFileEntry {
	return &ArchiveFileEntry{
		fileName:      name,
		size:          size,
		isDir:         isDir,
		modified:      modified,
		mode:          mode,
		attributes:    make(map[string]string),
		contentReader: contentReader,
	}
}

// Attributes returns the map of format-specific attributes
func (e *ArchiveFileEntry) Attributes() map[string]string { return e.attributes }

// SetAttribute sets a format-specific attribute
func (e *ArchiveFileEntry) SetAttribute(key, value string) {
	if e.attributes == nil {
		e.attributes = make(map[string]string)
	}
	e.attributes[key] = value
}

// SetAttributes sets multiple format-specific attributes
func (e *ArchiveFileEntry) SetAttributes(attrs map[string]string) {
	if e.attributes == nil {
		e.attributes = make(map[string]string)
	}
	for k, v := range attrs {
		e.attributes[k] = v
	}
}

// ArchiveExtractor defines the interface for extracting files from various archive formats
type ArchiveExtractor interface {
	// Format returns the archive format this extractor handles
	Format() Format

	// Filesystem returns a filesystem interface for the archive
	// Returns a filesystem that allows browsing the archive contents
	Filesystem(ctx context.Context) (fs.FS, error)

	// Close closes the extractor and releases any resources
	Close() error
}
