package upload

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/mholt/archives"
)

// ZipArchiveExtractor implements ArchiveExtractor for ZIP files using the unified driver
type ZipArchiveExtractor struct {
	driver *ArchivesDriver
}

// NewZipArchiveExtractor creates a new ZIP archive extractor using the unified driver
func NewZipArchiveExtractor(r archives.ReaderAtSeeker) (*ZipArchiveExtractor, error) {
	driver := NewArchivesDriver(FormatZIP, r)

	if !driver.IsFormatSupported() {
		return nil, fmt.Errorf("ZIP format not supported by driver")
	}

	return &ZipArchiveExtractor{
		driver: driver,
	}, nil
}

// Format returns the archive format this extractor handles
func (z *ZipArchiveExtractor) Format() Format {
	return FormatZIP
}

// Filesystem returns a filesystem interface for the archive
func (z *ZipArchiveExtractor) Filesystem(ctx context.Context) (fs.FS, error) {
	return z.driver.Filesystem(ctx)
}

// Close closes the extractor and releases any resources
func (z *ZipArchiveExtractor) Close() error {
	// The driver doesn't need explicit closing
	// Individual file readers are closed by the caller
	return nil
}

// RegisterZipExtractor registers the ZIP extractor with the default registry
func RegisterZipExtractor() {
	RegisterExtractor(FormatZIP, func(reader archives.ReaderAtSeeker) (ArchiveExtractor, error) {
		return NewZipArchiveExtractor(reader)
	})
}
