package upload

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/mholt/archives"
)

// RarArchiveExtractor implements ArchiveExtractor for RAR files using the unified driver
type RarArchiveExtractor struct {
	driver *ArchivesDriver
}

// NewRarArchiveExtractor creates a new RAR archive extractor using the unified driver
func NewRarArchiveExtractor(r archives.ReaderAtSeeker) (*RarArchiveExtractor, error) {
	driver := NewArchivesDriver(FormatRAR, r)

	if !driver.IsFormatSupported() {
		return nil, fmt.Errorf("RAR format not supported by driver")
	}

	return &RarArchiveExtractor{
		driver: driver,
	}, nil
}

// Format returns the archive format this extractor handles
func (r *RarArchiveExtractor) Format() Format {
	return FormatRAR
}

// Filesystem returns a filesystem interface for the archive
func (r *RarArchiveExtractor) Filesystem(ctx context.Context) (fs.FS, error) {
	return r.driver.Filesystem(ctx)
}

// Close closes the extractor and releases any resources
func (r *RarArchiveExtractor) Close() error {
	// The driver doesn't need explicit closing
	// Individual file readers are closed by the caller
	return nil
}

// RegisterRarExtractor registers the RAR extractor with the default registry
func RegisterRarExtractor() {
	RegisterExtractor(FormatRAR, func(reader archives.ReaderAtSeeker) (ArchiveExtractor, error) {
		return NewRarArchiveExtractor(reader)
	})
}
