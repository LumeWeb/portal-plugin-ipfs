package upload

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/mholt/archives"
)

// SevenZipArchiveExtractor implements ArchiveExtractor for 7Z files using the unified driver
type SevenZipArchiveExtractor struct {
	driver *ArchivesDriver
}

// NewSevenZipArchiveExtractor creates a new 7Z archive extractor using the unified driver
func NewSevenZipArchiveExtractor(r archives.ReaderAtSeeker) (*SevenZipArchiveExtractor, error) {
	driver := NewArchivesDriver(Format7Z, r)

	if !driver.IsFormatSupported() {
		return nil, fmt.Errorf("7Z format not supported by driver")
	}

	return &SevenZipArchiveExtractor{
		driver: driver,
	}, nil
}

// Format returns the archive format this extractor handles
func (s *SevenZipArchiveExtractor) Format() Format {
	return Format7Z
}

// Filesystem returns a filesystem interface for archive
func (s *SevenZipArchiveExtractor) Filesystem(ctx context.Context) (fs.FS, error) {
	return s.driver.Filesystem(ctx)
}

// Close closes the extractor and releases any resources
func (s *SevenZipArchiveExtractor) Close() error {
	// The driver doesn't need explicit closing
	// Individual file readers are closed by the caller
	return nil
}

// Register7ZipExtractor registers the 7Z extractor with the default registry
func Register7ZipExtractor() {
	RegisterExtractor(Format7Z, func(reader archives.ReaderAtSeeker) (ArchiveExtractor, error) {
		return NewSevenZipArchiveExtractor(reader)
	})
}
