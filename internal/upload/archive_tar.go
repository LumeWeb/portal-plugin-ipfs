package upload

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/mholt/archives"
)

// TarArchiveExtractor implements ArchiveExtractor for TAR files using the unified driver
type TarArchiveExtractor struct {
	driver *ArchivesDriver
}

// NewTarArchiveExtractor creates a new TAR archive extractor using the unified driver
func NewTarArchiveExtractor(r archives.ReaderAtSeeker) (*TarArchiveExtractor, error) {
	driver := NewArchivesDriver(FormatTAR, r)

	if !driver.IsFormatSupported() {
		return nil, fmt.Errorf("TAR format not supported by driver")
	}

	return &TarArchiveExtractor{
		driver: driver,
	}, nil
}

// Format returns the archive format this extractor handles
func (t *TarArchiveExtractor) Format() Format {
	return FormatTAR
}

// Filesystem returns a filesystem interface for the archive
func (t *TarArchiveExtractor) Filesystem(ctx context.Context) (fs.FS, error) {
	return t.driver.Filesystem(ctx)
}

// Close closes the extractor and releases any resources
func (t *TarArchiveExtractor) Close() error {
	// The driver doesn't need explicit closing
	// Individual file readers are closed by the caller
	return nil
}

// RegisterTarExtractor registers the TAR extractor with the default registry
func RegisterTarExtractor() {
	RegisterExtractor(FormatTAR, func(reader archives.ReaderAtSeeker) (ArchiveExtractor, error) {
		return NewTarArchiveExtractor(reader)
	})
}
